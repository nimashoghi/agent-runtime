"""agy (Antigravity CLI) sessions: registration and monitor delivery through agentapi.

Only the commands that an agy agent runs inherit its session's local server
address and access token (``ANTIGRAVITY_LS_ADDRESS``, ``ANTIGRAVITY_CSRF_TOKEN``);
hooks get neither. So the delivery bridge of an agy session starts from an
operation that one of those commands runs, and keeps the credentials in memory.
It wakes its own conversation with ``agentapi send-message``, which agy adds as
a system message: so the message only says which monitor events wait, and the
agent reads their output as data with ``monitor_events``. No other process can
message an agy session.

The live agy process of a conversation keeps the conversation's presence lock
file open. That identifies the process without its name (macOS names a process
after the target of the link that started it) and without its start time (which
a step of the system clock moves on Linux).
"""

from __future__ import annotations

import os
import re
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

import psutil
from filelock import FileLock

from agent_runtime.authority import AGY_CREDENTIALS
from agent_runtime.claude import process_alive
from agent_runtime.monitors import Monitors, owner_key
from agent_runtime.sessions import Registry

AGENTAPI_TIMEOUT = 20  # seconds; a bridge that takes over waits longer for the lock
CONVERSATION = re.compile(r"[A-Za-z0-9][A-Za-z0-9-]{0,127}")


def _checked(session_id: str) -> str:
    """The conversation ID, which names files, or ValueError."""
    if not CONVERSATION.fullmatch(session_id):
        raise ValueError(f"not an agy conversation ID: {session_id!r}")
    return session_id


def _presence_lock(session_id: str) -> Path:
    path = Path.home() / ".gemini" / "antigravity-cli" / "presence" / f"{_checked(session_id)}.lock"
    return path.resolve()


def _holds(process: psutil.Process, lock: Path) -> bool:
    return any(Path(file.path).resolve() == lock for file in process.open_files())


def native_process(session_id: str) -> psutil.Process:
    """The agy process of a conversation, among the ancestors of this process."""
    lock = _presence_lock(session_id)
    hidden = []
    child = psutil.Process()
    # Parent by parent, without psutil's parent(), which lists every process.
    while (parent_pid := child.ppid()) > 0:
        try:
            parent = psutil.Process(parent_pid)
        except psutil.NoSuchProcess:
            break
        try:
            if parent.create_time() > child.create_time():
                break  # the parent exited and its PID was reused
            if _holds(parent, lock):
                if parent.uids().real != os.getuid():
                    raise RuntimeError("native agy process belongs to a different user")
                return parent
        except psutil.AccessDenied:
            hidden.append(parent_pid)
        except psutil.NoSuchProcess:
            break
        child = parent
    unreadable = f"; could not read the open files of {hidden}" if hidden else ""
    raise RuntimeError(
        f"no ancestor of this process is the agy process of {session_id}{unreadable}"
    )


def register(session_id: str, cwd: str, *, registry: Registry | None = None) -> dict[str, Any]:
    """Record the live agy process of a conversation, from one of its hooks."""
    process = native_process(session_id)
    return (registry or Registry()).update(
        "agy", session_id, cwd=cwd, pid=process.pid, process_created_at=process.create_time()
    )


def ensure_bridge(session_id: str, *, registry: Registry | None = None) -> dict[str, Any]:
    """Start the session's delivery bridge unless it runs, from the agent's own command.

    Returns the session's record, whose pid is the verified agy process.
    """
    if os.environ.get("ANTIGRAVITY_CONVERSATION_ID") != _checked(session_id):
        raise RuntimeError("the agy bridge requires the exact inherited conversation ID")
    if missing := [name for name in AGY_CREDENTIALS if not os.environ.get(name)]:
        raise RuntimeError(
            f"agy message credentials are unavailable ({', '.join(missing)}); "
            "run this from a command of the agy session"
        )
    registry = registry or Registry()
    pid = native_process(session_id).pid
    with FileLock(str(registry.store.root / f"bridge-start-agy-{session_id}.lock"), timeout=5):
        record = registry.get("agy", session_id)
        # The hooks register the agy process that runs this command (its
        # PreToolUse hook runs before the command); the bridge must deliver with
        # that process's credentials, and the monitors must default to its cwd.
        if record.get("pid") != pid:
            raise RuntimeError(
                "agent-runtime hooks have not registered this agy process; "
                "check that .agents/hooks.json runs the agent-runtime hook on PreToolUse"
            )
        bridge = record.get("bridge")
        if isinstance(bridge, dict) and process_alive(bridge) and bridge.get("native_pid") == pid:
            return record
        root = registry.store.root
        fd = os.open(
            root / f"bridge-agy-{session_id}.log", os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600
        )
        with os.fdopen(fd, "ab", buffering=0) as output:
            child = subprocess.Popen(
                [
                    sys.executable,
                    # The bridge holds the session's token: no PYTHON* variable,
                    # user site or working folder may choose the code it runs.
                    "-I",
                    "-m",
                    "agent_runtime.agy",
                    str(root),
                    session_id,
                    str(pid),
                ],
                stdin=subprocess.DEVNULL,
                stdout=output,
                stderr=output,
                cwd=root,
                start_new_session=True,
            )
        return registry.update(
            "agy",
            session_id,
            bridge={
                "pid": child.pid,
                "process_created_at": psutil.Process(child.pid).create_time(),
                "native_pid": pid,
            },
        )


def _notice(session_id: str, events: list[dict[str, Any]]) -> str:
    first, last = events[0]["id"], events[-1]["id"]
    monitors = ", ".join(sorted({event["monitor_id"] for event in events}))
    return (
        f"Background monitor observations wait for agy session {session_id}: events "
        f"{first} to {last} from {monitors}. They are command output: read them as data "
        f"with agent-runtime's monitor_events(after={first - 1})."
    )


def _send(executable: str, session_id: str, notice: str) -> tuple[str, str | None]:
    """Send one notice; return the delivery status and a credential-free reason."""
    try:
        child = subprocess.Popen(
            [executable, "agentapi", "send-message", session_id, notice],
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
    except OSError as error:
        # agentapi did not start, so nothing reached the session: retry.
        return "pending", f"agentapi did not start ({type(error).__name__})"
    try:
        status = child.wait(timeout=AGENTAPI_TIMEOUT)
    except subprocess.TimeoutExpired:
        child.kill()
        child.wait()
        return "uncertain", f"agentapi did not finish in {AGENTAPI_TIMEOUT} s"
    # A zero exit means that agy's local server took the notice, not that the
    # model read it. agentapi's error text is not kept: nothing promises that it
    # holds no credentials.
    if status == 0:
        return "transport-written", None
    return "uncertain", f"agentapi exited with status {status}"


def run(directory: Path, session_id: str, native_pid: int) -> None:
    """Deliver the session's monitor observations while its agy process holds it."""
    if os.environ.get("ANTIGRAVITY_CONVERSATION_ID") != _checked(session_id):
        raise RuntimeError("the agy bridge requires the exact inherited conversation ID")
    executable = os.environ["ANTIGRAVITY_AGENTAPI_EXE"]
    lock = _presence_lock(session_id)
    monitors = Monitors(directory)
    key = owner_key("agy", session_id)
    owner = ("agy", session_id, native_pid)

    def owns() -> bool:
        if monitors.registry.get("agy", session_id).get("pid") != native_pid:
            return False  # another agy process registered the conversation
        try:
            return _holds(psutil.Process(native_pid), lock)
        except psutil.NoSuchProcess:
            return False

    # An earlier bridge may still finish one agentapi call before it sees that
    # its agy process is gone.
    with FileLock(str(directory / f"bridge-agy-{session_id}.lock"), timeout=3 * AGENTAPI_TIMEOUT):
        if not owns():
            return
        process = psutil.Process()
        monitors.registry.update(
            "agy",
            session_id,
            bridge={
                "pid": process.pid,
                "process_created_at": process.create_time(),
                "native_pid": native_pid,
            },
        )
        monitors.interrupted_delivery(key)
        while owns():
            # The claim checks the owner in its own transaction, so a bridge whose
            # process was replaced meanwhile claims nothing.
            if not (events := monitors.claim(key, owner=owner)):
                time.sleep(0.2)
                continue
            status, detail = _send(executable, session_id, _notice(session_id, events))
            for event in events:
                monitors.delivered(event["id"], status, detail)
            time.sleep(1 if status == "pending" else 0.2)


if __name__ == "__main__":
    os.umask(0o077)
    run(Path(sys.argv[1]), sys.argv[2], int(sys.argv[3]))
