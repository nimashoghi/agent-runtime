"""Own a monitor process, retaining observations when the native session is closed."""

import asyncio
import os
import signal
import sys
import threading
import time
from collections.abc import Callable
from contextlib import suppress
from dataclasses import dataclass
from pathlib import Path
from typing import TypeVar, cast

import psutil

from agent_runtime import agy
from agent_runtime.authority import without_agy
from agent_runtime.claude import process_alive
from agent_runtime.monitors import Monitors
from agent_runtime.processes import agy_alive, owner_alive, start_token

# The final delivery pass after the command ends: two agentapi calls.
FINAL_DELIVERY = 2 * agy.AGENTAPI_TIMEOUT


async def exited(child: asyncio.subprocess.Process, timeout: float) -> bool:
    """Wait until the child exits, but not for its pipes to close.

    `Process.wait()` called before the child exits returns only after stdout
    and stderr close, and a pipe that no reader drains never closes.
    """
    deadline = time.monotonic() + timeout
    while child.returncode is None and time.monotonic() < deadline:
        await asyncio.sleep(0.05)
    return child.returncode is not None


async def run(directory: Path, identity: str) -> None:
    # An inherited SIG_IGN would let the kernel discard the command's exit status.
    signal.signal(signal.SIGCHLD, signal.SIG_DFL)
    monitors = Monitors(directory)
    # What readers judge the worker by; begin() fails a worker that they cannot judge.
    lifetime: dict[str, object] = {"pid": os.getpid()}
    process = None
    with suppress(psutil.Error, OSError):
        process = psutil.Process()
    if process is not None:
        with suppress(psutil.Error, OSError):
            lifetime["process_created_at"] = process.create_time()
        with suppress(psutil.Error, OSError):
            lifetime["start_token"] = start_token(process)
    if (value := monitors.begin(identity, **lifetime)) is None:
        return  # given up, or failed before this worker started
    assert process is not None  # begin() accepted an identity read from it
    stop = asyncio.Event()
    for signum in (signal.SIGTERM, signal.SIGINT):
        asyncio.get_running_loop().add_signal_handler(signum, stop.set)
    owner = value["owner"]
    finish = _Finish()
    delivery = None
    if "start_token" in owner:
        # The worker of an agy monitor delivers its own events with the
        # credentials of the command that started it; nothing else claims them.
        delivery = asyncio.create_task(_deliver(monitors, identity, owner, finish))

    async def watch_owner() -> None:
        # In its own task, because inspecting an agy owner's open files can be
        # slow, and supervision must go on meanwhile.
        while True:
            try:
                alive = await _in_thread(owner_alive, owner)
            except Exception as error:  # inconclusive: ask again
                print(f"monitor owner check: {type(error).__name__}: {error}", file=sys.stderr)
                alive = True
            if not alive:
                stop.set()
                return
            await asyncio.sleep(0.25)

    watcher = None if value["persistent"] else asyncio.create_task(watch_owner())
    child = None
    child_created = None
    descendants = {}
    started = time.monotonic()
    timed_out = False
    stopping = False  # supervision decided to stop the command
    readers = []
    try:
        # Stopping a command's descendants needs the process table.
        try:
            process.children(recursive=True)
        except (psutil.AccessDenied, PermissionError) as error:
            raise RuntimeError(
                "monitors need to list the worker's child processes, which this system denies"
            ) from error
        child = await asyncio.create_subprocess_exec(
            "/bin/bash",
            "-c",
            value["command"],
            cwd=value["cwd"],
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            # The monitored command is not the session.
            env=without_agy(dict(os.environ)),
            start_new_session=True,
        )
        with suppress(psutil.NoSuchProcess):
            child_created = psutil.Process(child.pid).create_time()

        async def read(stream, *, observations: bool):
            buffer = b""
            total = 0
            while block := await stream.read(4096):
                total += len(block)
                if total > 1024 * 1024:
                    raise RuntimeError(
                        "monitor output exceeded 1 MiB; restart with a tighter filter"
                    )
                buffer += block
                while b"\n" in buffer or len(buffer) >= 16384:
                    if b"\n" in buffer[:16384]:
                        line, buffer = buffer.split(b"\n", 1)
                    else:
                        line, buffer = buffer[:16384], buffer[16384:]
                    text = line.decode("utf-8", errors="replace")
                    if observations and text.strip():
                        monitors.append(identity, text)
                    elif not observations:
                        sys.stderr.write(text + "\n")
                        sys.stderr.flush()
            if buffer:
                text = buffer.decode("utf-8", errors="replace")
                if observations and text.strip():
                    monitors.append(identity, text)
                elif not observations:
                    sys.stderr.write(text + "\n")

        readers = [
            asyncio.create_task(read(child.stdout, observations=True)),
            asyncio.create_task(read(child.stderr, observations=False)),
        ]
        while child.returncode is None:
            if child_created is not None:
                try:
                    for descendant in psutil.Process(child.pid).children(recursive=True):
                        descendants[descendant.pid] = descendant.create_time()
                except psutil.NoSuchProcess:
                    pass
            state = monitors.get(identity)
            if not value["persistent"] and time.monotonic() - started >= value["timeout_ms"] / 1000:
                timed_out = True
                stop.set()
            if state.get("stop_requested"):
                stop.set()
            if stop.is_set():
                stopping = True
                if process_alive({"pid": child.pid, "process_created_at": child_created}):
                    os.killpg(child.pid, signal.SIGTERM)
                    if not await exited(child, 5) and process_alive(
                        {"pid": child.pid, "process_created_at": child_created}
                    ):
                        os.killpg(child.pid, signal.SIGKILL)
                break
            for reader in readers:
                if reader.done() and (error := reader.exception()) is not None:
                    raise error
            await asyncio.sleep(0.1)
        finish.deadline = time.monotonic() + FINAL_DELIVERY
        # Stop can win the race with a normal exit; record the exit code then too.
        await exited(child, 5)
        try:
            await asyncio.wait_for(asyncio.gather(*readers), timeout=5)
        except TimeoutError as error:
            raise RuntimeError(
                "monitor command exited while descendants kept its output open; "
                "keep the monitored command in the foreground"
            ) from error
        status = (
            "timed_out"
            if timed_out
            else "stopped"
            if stopping
            else "completed"
            if child.returncode == 0
            else "failed"
        )
        monitors.finish(
            identity,
            f"Monitor {identity} {status} (exit code {child.returncode}).",
            status=status,
            exit_code=child.returncode,
        )
    except Exception as error:
        if finish.deadline is None:
            finish.deadline = time.monotonic() + FINAL_DELIVERY
        monitors.finish(
            identity, f"Monitor {identity} failed: {error}", status="failed", error=str(error)
        )
        raise
    finally:
        if watcher is not None:
            watcher.cancel()
        for pid, created in descendants.items():
            if process_alive({"pid": pid, "process_created_at": created}):
                with suppress(ProcessLookupError):
                    os.kill(pid, signal.SIGKILL)
        if child is not None and child.returncode is None:
            if process_alive({"pid": child.pid, "process_created_at": child_created}):
                os.killpg(child.pid, signal.SIGKILL)
            await exited(child, 5)
        for reader in readers:
            reader.cancel()
        await asyncio.gather(*readers, return_exceptions=True)
        if delivery is not None:
            # A last pass for the final events. No send starts after the
            # deadline, which runs from the command's end, and none is cut off.
            if finish.deadline is None:
                finish.deadline = time.monotonic() + FINAL_DELIVERY
            finish.final = True
            # Bounded, so that a stuck inspection or store cannot keep the
            # worker, which holds the session's credentials: a send that
            # started before the deadline gets agentapi's own timeout.
            bound = finish.deadline - time.monotonic() + agy.AGENTAPI_TIMEOUT + 5
            with suppress(Exception):
                await asyncio.wait_for(delivery, timeout=max(bound, 0))


T = TypeVar("T")


async def _in_thread(function: Callable[..., T], *args: object) -> T:
    """Run a blocking call in a daemon thread. Unlike asyncio.to_thread, a call
    that never returns (an inspection of a stalled file system) cannot keep the
    worker from exiting."""
    loop = asyncio.get_running_loop()
    future: asyncio.Future[T] = loop.create_future()

    def settle(result: T | None, error: BaseException | None) -> None:
        if future.done():  # cancelled meanwhile
            return
        if error is not None:
            future.set_exception(error)
        else:
            future.set_result(cast(T, result))

    def target() -> None:
        try:
            result, error = function(*args), None
        except BaseException as caught:  # handed to the awaiting task
            result, error = None, caught
        with suppress(RuntimeError):  # the loop closed: nobody waits
            loop.call_soon_threadsafe(settle, result, error)

    threading.Thread(target=target, daemon=True).start()
    return await future


@dataclass
class _Finish:
    """When an agy worker's delivery ends."""

    deadline: float | None = None  # no send starts after it
    final: bool = False  # every event of the monitor is recorded


async def _deliver(monitors: Monitors, identity: str, owner: dict, finish: _Finish) -> None:
    """Notify the agy session of this monitor's events, until every event is
    recorded and none is left to send, or the deadline passed.

    A batch is sent only while the owner's lifetime is confirmed to hold the
    conversation and the deadline has not passed: before the claim, and again in
    the sending thread right before agentapi starts; otherwise the batch goes
    back to pending, for monitor_events. An owner that cannot be inspected is
    asked again. The store is used from a thread, so that a wait for it never
    holds up the worker's supervision. A claimed batch gets a receipt in one
    transaction, also when the send raised (uncertain), and a failed write is
    retried without sending again.
    """
    conversation = owner["conversation"]
    unsettled: list[tuple[list[int], str, str | None]] = []

    def owner_state() -> bool | None:
        return agy_alive(owner["pid"], owner["start_token"], conversation)

    def late() -> bool:
        return finish.deadline is not None and time.monotonic() > finish.deadline

    def sendable() -> bool:
        # The clock is read before the inspection, which can be slow, and after it.
        return not late() and owner_state() is True and not late()

    def drain() -> str:
        """Send the pending batches: "idle" when none is left, "gone" when the
        owner no longer holds the conversation, "closed" when the deadline
        passed, "retry" to try again later."""
        while unsettled:
            monitors.settle(*unsettled[0])
            unsettled.pop(0)
        while True:
            if late():  # before an inspection, which can be slow
                return "closed"
            state = owner_state()
            if state is False:
                return "gone"
            if late():
                return "closed"
            if state is None:
                return "retry"
            if not (events := monitors.claim_monitor(identity)):
                return "idle"
            try:
                text = agy.notice(conversation, events)
                status, detail = agy.send_notice(conversation, text, sendable)
            except Exception as error:  # agentapi may have started
                status, detail = "uncertain", f"delivery failed ({type(error).__name__})"
            unsettled.append(([event["id"] for event in events], status, detail))
            monitors.settle(*unsettled[0])
            unsettled.pop(0)
            if status == "pending":
                return "retry"

    while True:
        final = finish.final
        try:
            result = await _in_thread(drain)
        except Exception as error:  # a store error: report it and try again
            print(f"monitor delivery: {type(error).__name__}: {error}", file=sys.stderr)
            result = "retry"
        if result == "gone" or (final and (result != "retry" or late())):
            # Nothing can wake a conversation that its process left; the
            # events wait for monitor_events.
            return
        await asyncio.sleep(0.2 if result == "idle" else 1)


if __name__ == "__main__":
    os.umask(0o077)
    asyncio.run(run(Path(sys.argv[1]), sys.argv[2]))
