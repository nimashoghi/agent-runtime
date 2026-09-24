"""Own a monitor process, retaining observations when the native session is closed."""

import asyncio
import os
import signal
import sys
import time
from contextlib import suppress
from pathlib import Path

import psutil

from agent_runtime.claude import process_alive
from agent_runtime.monitors import Monitors


async def run(directory: Path, identity: str) -> None:
    monitors = Monitors(directory)
    value = monitors.get(identity)
    process = psutil.Process()
    monitors.update(
        identity, status="running", pid=process.pid, process_created_at=process.create_time()
    )
    stop = asyncio.Event()
    for signum in (signal.SIGTERM, signal.SIGINT):
        asyncio.get_running_loop().add_signal_handler(signum, stop.set)
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
                not value["persistent"] and not process_alive(value["owner"])
            ):
                stop.set()
            if stop.is_set():
                if process_alive({"pid": child.pid, "process_created_at": child_created}):
                    os.killpg(child.pid, signal.SIGTERM)
                    try:
                        await asyncio.wait_for(child.wait(), timeout=5)
                    except TimeoutError:
                        if process_alive({"pid": child.pid, "process_created_at": child_created}):
                            os.killpg(child.pid, signal.SIGKILL)
                break
            for reader in readers:
                if reader.done() and (error := reader.exception()) is not None:
                    raise error
            await asyncio.sleep(0.1)
        await child.wait()
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
        for pid, created in descendants.items():
            if process_alive({"pid": pid, "process_created_at": created}):
                with suppress(ProcessLookupError):
                    os.kill(pid, signal.SIGKILL)
        if child is not None and child.returncode is None:
            if process_alive({"pid": child.pid, "process_created_at": child_created}):
                os.killpg(child.pid, signal.SIGKILL)
            await child.wait()
        for reader in readers:
            reader.cancel()
        await asyncio.gather(*readers, return_exceptions=True)


if __name__ == "__main__":
    os.umask(0o077)
    asyncio.run(run(Path(sys.argv[1]), sys.argv[2]))
