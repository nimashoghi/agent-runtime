"""agy (Antigravity CLI) sessions: registration and monitor delivery through agentapi.

Only the commands that an agy agent runs inherit its session's local server
address and access token (``ANTIGRAVITY_LS_ADDRESS``, ``ANTIGRAVITY_CSRF_TOKEN``);
hooks get neither. So the delivery bridge of an agy session starts from an
operation that one of those commands runs, and keeps the credentials in memory.
It wakes its own conversation with ``agentapi send-message``, which agy adds as
a system message: so the message only says which monitor events wait, and the
agent reads their output as data with ``monitor_events``. No other process can
message an agy session.

One agy process lifetime is its PID with its start token (processes.start_token),
and it is live while it keeps the conversation's presence lock file open. The
session's record names that lifetime; every write that acts for the session
checks the record in the same transaction.
"""

from __future__ import annotations

import json
import os
import signal
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
from agent_runtime.processes import (
    agy_alive,
    agy_conversation,
    holds,
    presence_lock,
    start_token,
)
from agent_runtime.sessions import Registry

AGENTAPI_TIMEOUT = 20  # seconds; a bridge that takes over waits longer for the lock
Owner = tuple[int, str]  # the PID and start token of one agy process lifetime


def native_process(session_id: str) -> psutil.Process:
    """The agy process of a conversation, among the ancestors of this process."""
    lock = presence_lock(session_id)
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
            if holds(parent, lock):
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
    """Record the live agy process of a conversation, from one of its hooks.

    Only the process that holds the conversation's presence lock passes
    native_process, so a late hook of an exited process registers nothing.
    """
    process = native_process(session_id)
    return (registry or Registry()).update(
        "agy",
        session_id,
        cwd=cwd,
        pid=process.pid,
        process_created_at=process.create_time(),
        native_start=start_token(process),
    )


def _names(value: dict[str, Any], owner: Owner) -> bool:
    return (value.get("pid"), value.get("native_start")) == owner


def _update_owned(registry: Registry, session_id: str, owner: Owner, **fields: Any) -> bool:
    """Update the session's record only while it names owner."""
    with registry.store.connect() as db:
        db.execute("BEGIN IMMEDIATE")
        row = db.execute(
            "SELECT value FROM sessions WHERE provider='agy' AND id=?", (session_id,)
        ).fetchone()
        if row is None or not _names(value := json.loads(row[0]), owner):
            return False
        value.update(fields, updated_at=time.time())
        db.execute(
            "UPDATE sessions SET value=? WHERE provider='agy' AND id=?",
            (json.dumps(value), session_id),
        )
    return True


def live_sessions(registry: Registry | None = None) -> list[dict[str, Any]]:
    """The registered agy sessions whose agy process still holds the conversation."""
    with (registry or Registry()).store.connect() as db:
        rows = db.execute("SELECT value FROM sessions WHERE provider='agy'").fetchall()
    return [
        record
        for row in rows
        if agy_alive(
            (record := json.loads(row[0])).get("pid", 0),
            record.get("native_start", ""),
            record["session_id"],
        )
    ]


def ensure_bridge(session_id: str, *, registry: Registry | None = None) -> Owner:
    """Start the session's delivery bridge unless it runs, from the agent's own
    command; return the verified agy process lifetime."""
    if os.environ.get("ANTIGRAVITY_CONVERSATION_ID") != agy_conversation(session_id):
        raise RuntimeError("the agy bridge requires the exact inherited conversation ID")
    if missing := [name for name in AGY_CREDENTIALS if not os.environ.get(name)]:
        raise RuntimeError(
            f"agy message credentials are unavailable ({', '.join(missing)}); "
            "run this from a command of the agy session"
        )
    registry = registry or Registry()
    native = native_process(session_id)
    owner = (native.pid, start_token(native))
    with FileLock(str(registry.store.root / f"bridge-start-agy-{session_id}.lock"), timeout=5):
        record = registry.get("agy", session_id)
        # The hooks register the agy process that runs this command (its
        # PreToolUse hook runs before the command); the bridge must deliver with
        # that process's credentials, and the monitors must default to its cwd.
        if not _names(record, owner):
            raise RuntimeError(
                "agent-runtime hooks have not registered this agy process; "
                "check that .agents/hooks.json runs the agent-runtime hook on PreToolUse"
            )
        bridge = record.get("bridge")
        if (
            isinstance(bridge, dict)
            and process_alive(bridge)
            and (bridge.get("native_pid"), bridge.get("native_start")) == owner
        ):
            return owner
        root = registry.store.root
        log = root / f"bridge-agy-{session_id}.log"
        fd = os.open(log, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
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
                    str(owner[0]),
                    owner[1],
                ],
                stdin=subprocess.DEVNULL,
                stdout=output,
                stderr=output,
                cwd=root,
                start_new_session=True,
            )
        # A bridge that cannot import agent_runtime (an installation that needs
        # the user site, which -I leaves out) exits at once.
        try:
            status = child.wait(timeout=1)
        except subprocess.TimeoutExpired:
            pass
        else:
            tail = log.read_text(errors="replace").strip().splitlines()[-1:] or ["no output"]
            raise RuntimeError(f"the agy delivery bridge exited with status {status}: {tail[0]}")
        bridge = {
            "pid": child.pid,
            "process_created_at": psutil.Process(child.pid).create_time(),
            "native_pid": owner[0],
            "native_start": owner[1],
        }
        if not _update_owned(registry, session_id, owner, bridge=bridge):
            raise RuntimeError("the agy conversation moved to another agy process")
    return owner


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


def run(directory: Path, session_id: str, owner: Owner) -> None:
    """Deliver the session's monitor observations while its agy process holds it."""
    if os.environ.get("ANTIGRAVITY_CONVERSATION_ID") != agy_conversation(session_id):
        raise RuntimeError("the agy bridge requires the exact inherited conversation ID")
    # An inherited SIG_IGN would let the kernel discard agentapi's exit status,
    # which Python then reports as 0.
    signal.signal(signal.SIGCHLD, signal.SIG_DFL)
    executable = os.environ["ANTIGRAVITY_AGENTAPI_EXE"]
    monitors = Monitors(directory)
    registry = monitors.registry
    key = owner_key("agy", session_id)

    def owns() -> bool | None:
        """True while the record names this lifetime and it holds the conversation;
        None when that cannot be told now."""
        if not _names(registry.get("agy", session_id), owner):
            return False
        return agy_alive(owner[0], owner[1], session_id)

    # An earlier bridge may still finish one agentapi call before it sees that
    # its agy process is gone.
    with FileLock(str(directory / f"bridge-agy-{session_id}.lock"), timeout=3 * AGENTAPI_TIMEOUT):
        process = psutil.Process()
        bridge = {
            "pid": process.pid,
            "process_created_at": process.create_time(),
            "native_pid": owner[0],
            "native_start": owner[1],
        }
        if not _update_owned(registry, session_id, owner, bridge=bridge):
            return
        monitors.interrupted_delivery(key)
        while (state := owns()) is not False:
            if state is None:
                time.sleep(1)
                continue
            # The claim checks the record in its own transaction, so a bridge
            # whose process was replaced meanwhile claims nothing.
            if not (events := monitors.claim(key, owner=("agy", session_id, *owner))):
                time.sleep(0.2)
                continue
            if owns() is not True:
                # The conversation moved or cannot be checked: leave the events
                # to the bridge that delivers for it.
                for event in events:
                    monitors.delivered(event["id"], "pending")
                continue
            status, detail = _send(executable, session_id, _notice(session_id, events))
            for event in events:
                monitors.delivered(event["id"], status, detail)
            time.sleep(1 if status == "pending" else 0.2)


if __name__ == "__main__":
    os.umask(0o077)
    run(Path(sys.argv[1]), sys.argv[2], (int(sys.argv[3]), sys.argv[4]))
