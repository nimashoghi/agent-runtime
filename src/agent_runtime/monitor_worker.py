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
from agent_runtime.claude import process_alive
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


async def run(directory: Path, identity: str) -> None:
    # An inherited SIG_IGN would let the kernel discard the command's exit status.
    signal.signal(signal.SIGCHLD, signal.SIG_DFL)
    monitors = Monitors(directory)
    value = monitors.get(identity)
    process = psutil.Process()
    monitors.update(
        identity,
        status="running",
        pid=process.pid,
        process_created_at=process.create_time(),
        start_token=start_token(process),
    )
    stop = asyncio.Event()
    for signum in (signal.SIGTERM, signal.SIGINT):
        asyncio.get_running_loop().add_signal_handler(signum, stop.set)
    owner = value["owner"]
    # The worker of an agy monitor delivers its own events with the credentials
    # of the command that started it; nothing else claims them.
    delivering = "start_token" in owner

    async def deliver() -> None:
        if not delivering:
            return
        if agy_alive(owner["pid"], owner["start_token"], owner["conversation"]) is False:
            return  # they wait for monitor_events in the session that resumes it
        while events := monitors.claim_monitor(identity):
            text = agy.notice(owner["conversation"], events)
            status, detail = await asyncio.to_thread(agy.send_notice, owner["conversation"], text)
            for event in events:
                monitors.delivered(event["id"], status, detail)
            if status != "transport-written":
                return

    delivered_at = 0.0
    child = None
    child_created = None
    descendants = {}
    started = time.monotonic()
    timed_out = False
    readers = []
    try:
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
            if state.get("stop_requested") or (
                not value["persistent"] and not owner_alive(value["owner"])
            ):
                stop.set()
            if stop.is_set():
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
            if time.monotonic() - delivered_at >= 0.2:
                await deliver()
                delivered_at = time.monotonic()
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
        await deliver()
    except Exception as error:
        monitors.update(identity, status="failed", error=str(error))
        monitors.append(identity, f"Monitor {identity} failed: {error}")
        with suppress(Exception):
            await deliver()
        raise
    finally:
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


if __name__ == "__main__":
    os.umask(0o077)
    asyncio.run(run(Path(sys.argv[1]), sys.argv[2]))
