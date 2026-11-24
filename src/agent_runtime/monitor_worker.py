"""Own a monitor process, retaining observations when the native session is closed."""

import asyncio
import os
import signal
import sys
import time
from contextlib import suppress
from pathlib import Path

import psutil

from agent_runtime import agy
from agent_runtime.authority import without_agy
from agent_runtime.monitors import Monitors
from agent_runtime.processes import agy_alive, owner_alive, start_token


async def exited(child: asyncio.subprocess.Process, timeout: float) -> bool:
    """Wait until the child exits, but not for its pipes to close.

    `Process.wait()` called before the child exits returns only after stdout
    and stderr close, and a pipe that no reader drains never closes.
    """
    deadline = time.monotonic() + timeout
    while child.returncode is None and time.monotonic() < deadline:
        await asyncio.sleep(0.05)
    return child.returncode is not None


def _running(process: psutil.Process | None) -> bool:
    """Whether this exact process still runs: psutil's is_running() compares the
    lifetime it saw first, so a PID that changed hands does not count."""
    return (
        process is not None and process.is_running() and (process.status() != psutil.STATUS_ZOMBIE)
    )


async def run(directory: Path, identity: str) -> None:
    # An inherited SIG_IGN would let the kernel discard the command's exit status.
    signal.signal(signal.SIGCHLD, signal.SIG_DFL)
    monitors = Monitors(directory)
    value = monitors.get(identity)
    process = psutil.Process()
    lifetime = {"pid": process.pid, "process_created_at": process.create_time()}
    with suppress(OSError):  # without it, readers judge the worker by creation time
        lifetime["start_token"] = start_token(process)
    monitors.update(identity, status="running", **lifetime)
    stop = asyncio.Event()
    for signum in (signal.SIGTERM, signal.SIGINT):
        asyncio.get_running_loop().add_signal_handler(signum, stop.set)
    owner = value["owner"]
    finished = asyncio.Event()
    delivery = None
    if "start_token" in owner:
        # The worker of an agy monitor delivers its own events with the
        # credentials of the command that started it; nothing else claims them.
        delivery = asyncio.create_task(_deliver(monitors, identity, owner, finished))
    child = None
    child_process: psutil.Process | None = None
    descendants: list[psutil.Process] = []
    started = time.monotonic()
    timed_out = False
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
            child_process = psutil.Process(child.pid)

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
            if child_process is not None:
                try:
                    for descendant in child_process.children(recursive=True):
                        if descendant not in descendants:  # psutil compares lifetimes
                            descendants.append(descendant)
                except psutil.NoSuchProcess:
                    pass
            state = monitors.get(identity)
            if not value["persistent"] and time.monotonic() - started >= value["timeout_ms"] / 1000:
                timed_out = True
                stop.set()
            if state.get("stop_requested") or (not value["persistent"] and not owner_alive(owner)):
                stop.set()
            if stop.is_set():
                if _running(child_process):
                    os.killpg(child.pid, signal.SIGTERM)
                    if not await exited(child, 5) and _running(child_process):
                        os.killpg(child.pid, signal.SIGKILL)
                break
            for reader in readers:
                if reader.done() and (error := reader.exception()) is not None:
                    raise error
            await asyncio.sleep(0.1)
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
            if stop.is_set()
            else "completed"
            if child.returncode == 0
            else "failed"
        )
        monitors.update(identity, status=status, exit_code=child.returncode)
        monitors.append(identity, f"Monitor {identity} {status} (exit code {child.returncode}).")
    except Exception as error:
        monitors.update(identity, status="failed", error=str(error))
        monitors.append(identity, f"Monitor {identity} failed: {error}")
        raise
    finally:
        for descendant in descendants:
            # kill() checks that the PID still belongs to this lifetime.
            with suppress(psutil.NoSuchProcess, psutil.AccessDenied):
                descendant.kill()
        if child is not None and child.returncode is None:
            if _running(child_process):
                os.killpg(child.pid, signal.SIGKILL)
            await exited(child, 5)
        for reader in readers:
            reader.cancel()
        await asyncio.gather(*readers, return_exceptions=True)
        if delivery is not None:
            # A last pass for the final events. It stops between batches after
            # its deadline and never in the middle of a send.
            finished.set()
            with suppress(Exception):
                await delivery


async def _deliver(monitors: Monitors, identity: str, owner: dict, finished: asyncio.Event) -> None:
    """Notify the agy session of this monitor's events until the worker finishes.

    A batch is sent only while the owner's lifetime is confirmed to hold the
    conversation: before the claim, and again in the sending thread right before
    agentapi starts; otherwise the batch goes back to pending, for
    monitor_events in the session that resumes it. The store is used from
    threads, so that a wait for it never holds up the worker's supervision.
    """
    conversation = owner["conversation"]

    def confirmed() -> bool:
        return agy_alive(owner["pid"], owner["start_token"], conversation) is True

    def drain(deadline: float | None) -> bool:
        """Send the pending batches; False to retry later."""
        while confirmed() and (deadline is None or time.monotonic() < deadline):
            if not (events := monitors.claim_monitor(identity)):
                return True
            text = agy.notice(conversation, events)
            status, detail = agy.send_notice(conversation, text, confirmed)
            for event in events:
                monitors.delivered(event["id"], status, detail)
            if status == "pending":
                return False
        return True

    while not finished.is_set():
        try:
            retry = not await asyncio.to_thread(drain, None)
        except Exception as error:  # a store error: report it and try again
            print(f"monitor delivery: {type(error).__name__}: {error}", file=sys.stderr)
            retry = True
        await asyncio.sleep(1 if retry else 0.2)
    await asyncio.to_thread(drain, time.monotonic() + 2 * agy.AGENTAPI_TIMEOUT)


if __name__ == "__main__":
    os.umask(0o077)
    asyncio.run(run(Path(sys.argv[1]), sys.argv[2]))
