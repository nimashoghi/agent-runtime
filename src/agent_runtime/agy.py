"""agy (Antigravity CLI) sessions: identity, registration, discovery and notices.

Only the commands that an agy agent runs inherit its session's local server
address and access token (``ANTIGRAVITY_LS_ADDRESS``, ``ANTIGRAVITY_CSRF_TOKEN``);
hooks get neither. So a monitor that such a command starts delivers its own
observations: its worker keeps the command's credentials, which the monitored
command does not get, and wakes the conversation with ``agentapi send-message``
while the agy process that started it lives. agy adds that message as a system
message, so it only names the waiting events; the agent reads their output as
data with ``monitor_events``. No other process can message an agy session.
"""

from __future__ import annotations

import contextlib
import json
import os
import subprocess
import threading
import time
from collections.abc import Callable
from typing import Any

import psutil

from agent_runtime.authority import AGY_CREDENTIALS
from agent_runtime.processes import agy_alive, agy_conversation, holds, presence_lock, start_token
from agent_runtime.sessions import Registry

AGENTAPI_TIMEOUT = 20  # seconds
Owner = tuple[int, str]  # the PID and start token of one agy process lifetime


def native_process(session_id: str) -> Owner:
    """The lifetime of the agy process of a conversation, among the ancestors of
    this process: the one that keeps the conversation's presence lock file open.

    Once the holder is found, every link of the chain is checked again (each
    process still has the same parent, and each parent the same start token),
    so that a PID that changed hands during the walk is not taken.
    """
    lock = presence_lock(session_id)
    hidden = []
    links: list[tuple[int, int, str]] = []  # (child PID, parent PID, parent token)
    child_pid = os.getpid()
    while (parent_pid := psutil.Process(child_pid).ppid()) > 0:
        # Parent by parent, without psutil's parent(), which lists every process.
        try:
            parent = psutil.Process(parent_pid)
            token = start_token(parent)
        except psutil.NoSuchProcess:
            break
        except (psutil.AccessDenied, OSError) as error:
            # Also a boot ID that this process may not read.
            raise RuntimeError(
                f"cannot identify ancestor {parent_pid} of this process: {error}"
            ) from error
        links.append((child_pid, parent_pid, token))
        try:
            held = holds(parent_pid, lock)
            if held and parent.uids().real != os.getuid():
                raise RuntimeError("native agy process belongs to a different user")
        except (psutil.AccessDenied, PermissionError):
            hidden.append(parent_pid)
            held = False
        except (psutil.NoSuchProcess, FileNotFoundError, ProcessLookupError):
            break
        except OSError as error:
            raise RuntimeError(f"cannot inspect ancestor {parent_pid}: {error}") from error
        if held:
            try:
                intact = all(
                    psutil.Process(child).ppid() == parent
                    and start_token(psutil.Process(parent)) == parent_token
                    for child, parent, parent_token in links
                )
            except (psutil.Error, OSError):
                intact = False
            if not intact:
                raise RuntimeError("the chain to the agy process changed while it was checked")
            return parent_pid, token
        child_pid = parent_pid
    unreadable = f"; could not read the open files of {hidden}" if hidden else ""
    raise RuntimeError(
        f"no ancestor of this process is the agy process of {session_id}{unreadable}"
    )


OWNER_TIMEOUT = 10


class _Lookup:
    """One search for the agy process of this process's conversation."""

    def __init__(self, session_id: str) -> None:
        self.owner: Owner | None = None
        self.error: BaseException | None = None
        self.done = threading.Event()
        self.thread = threading.Thread(target=self._run, args=(session_id,), daemon=True)

    def _run(self, session_id: str) -> None:
        try:
            self.owner = native_process(session_id)
        except BaseException as error:  # handed to the callers
            self.error = error
        finally:
            self.done.set()
            with _LOOKUPS_LOCK:
                _LOOKUPS.pop(session_id, None)


_LOOKUPS: dict[str, _Lookup] = {}
_LOOKUPS_LOCK = threading.Lock()
_MAX_LOOKUPS = 4


def _after_fork_in_child() -> None:
    # The threads of the parent's lookups and inspections do not exist here.
    global _LOOKUPS_LOCK, _INSPECTIONS_LOCK
    _LOOKUPS.clear()
    _LOOKUPS_LOCK = threading.Lock()
    _INSPECTIONS.clear()
    _INSPECTIONS_LOCK = threading.Lock()


def command_owner(session_id: str) -> Owner:
    """The agy process lifetime whose credentials this command holds."""
    if os.environ.get("ANTIGRAVITY_CONVERSATION_ID") != agy_conversation(session_id):
        raise RuntimeError("an agy monitor needs the exact inherited conversation ID")
    if missing := [name for name in AGY_CREDENTIALS if not os.environ.get(name)]:
        raise RuntimeError(
            f"agy message credentials are unavailable ({', '.join(missing)}); "
            "start agy monitors from a command of the agy session"
        )
    # A stalled file system can hold up the scan of the ancestors' descriptors:
    # the scan runs in a daemon thread, which later calls of this process share
    # while it runs.
    with _LOOKUPS_LOCK:
        if (lookup := _LOOKUPS.get(session_id)) is None:
            if len(_LOOKUPS) >= _MAX_LOOKUPS:
                raise RuntimeError("earlier searches for agy processes have not ended")
            lookup = _LOOKUPS[session_id] = _Lookup(session_id)
            try:
                # The thread removes its entry under this lock, so only after this.
                lookup.thread.start()
            except RuntimeError as error:
                del _LOOKUPS[session_id]
                raise RuntimeError(f"cannot search for the agy process: {error}") from error
    if not lookup.done.wait(OWNER_TIMEOUT):
        raise RuntimeError(
            f"the agy process of this command was not found in {OWNER_TIMEOUT} s; "
            "a file system may not be answering"
        )
    if lookup.error is not None:
        raise lookup.error
    assert lookup.owner is not None
    return lookup.owner


def register(session_id: str, cwd: str, *, registry: Registry | None = None) -> None:
    """Record the live agy process of a conversation, from one of its hooks, for
    discovery; delivery does not read this record.

    It writes only after it checked that the lifetime holds the conversation,
    and only if no other hook wrote the record since the check began, so that a
    late hook of an exited process cannot hide the process that resumed it. The
    check runs outside the write transaction, which other writers share.
    """
    pid, token = native_process(session_id)
    registry = registry or Registry()
    select = "SELECT value FROM sessions WHERE provider='agy' AND id=?"
    for _attempt in range(3):
        with registry.store.connect() as db:
            before = (row := db.execute(select, (session_id,)).fetchone()) and row[0]
        if agy_alive(pid, token, session_id) is not True:
            return
        with registry.store.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute(select, (session_id,)).fetchone()
            if (row and row[0]) != before:
                continue  # another hook wrote meanwhile: check again
            value: dict[str, Any] = (
                json.loads(row[0]) if row else {"provider": "agy", "session_id": session_id}
            )
            value.update(cwd=cwd, pid=pid, native_start=token, updated_at=time.time())
            db.execute(
                "INSERT INTO sessions VALUES('agy',?,?) ON CONFLICT(provider,id) "
                "DO UPDATE SET value=excluded.value",
                (session_id, json.dumps(value)),
            )
            return


class _Inspection:
    """One inspection of an agy process, shared by every discovery that asks
    while it runs."""

    def __init__(self, key: tuple[int, str, str]) -> None:
        self.result: bool | None = None
        self.done = threading.Event()
        self.thread = threading.Thread(target=self._run, args=key, daemon=True)

    def _run(self, pid: int, token: str, session_id: str) -> None:
        try:
            self.result = agy_alive(pid, token, session_id)
        except Exception:  # unknown
            self.result = None
        finally:
            self.done.set()
            with _INSPECTIONS_LOCK:
                _INSPECTIONS.pop((pid, token, session_id), None)


# Inspections that run, at most _MAX_INSPECTIONS, so that discoveries that a
# stalled file system holds up do not pile up threads.
_INSPECTIONS: dict[tuple[int, str, str], _Inspection] = {}
_INSPECTIONS_LOCK = threading.Lock()
_MAX_INSPECTIONS = 32

os.register_at_fork(after_in_child=_after_fork_in_child)


def _inspection(key: tuple[int, str, str]) -> _Inspection | None:
    """The running inspection of an agy process, or a new one; None when too
    many run. A thread that cannot start makes a finished, unknown inspection."""
    with _INSPECTIONS_LOCK:
        if (inspection := _INSPECTIONS.get(key)) is not None:
            return inspection
        if len(_INSPECTIONS) >= _MAX_INSPECTIONS:
            return None
        inspection = _INSPECTIONS[key] = _Inspection(key)
        try:
            # The thread removes its entry under this lock, so only after this.
            inspection.thread.start()
        except RuntimeError:
            del _INSPECTIONS[key]
            inspection.done.set()
        return inspection


def live_sessions(
    registry: Registry | None = None, *, timeout: float = 2.0
) -> list[dict[str, Any]]:
    """The registered agy sessions whose agy process still holds the conversation.

    Each process is inspected in a daemon thread, which a later discovery shares
    while it runs; at most a fixed number run, and a record waits for a free one
    until the deadline. An inspection that has not ended by the deadline (a
    stalled file system), or not started, counts as unknown and leaves only its
    session out.
    """
    deadline = time.monotonic() + timeout
    with (registry or Registry()).store.connect() as db:
        rows = db.execute("SELECT value FROM sessions WHERE provider='agy'").fetchall()
    records = [json.loads(row[0]) for row in rows]
    inspections: list[_Inspection | None] = []
    for record in records:
        key = (record.get("pid", 0), record.get("native_start", ""), record["session_id"])
        inspection = None
        while time.monotonic() < deadline and (inspection := _inspection(key)) is None:
            time.sleep(0.01)
        inspections.append(inspection)
    live = []
    for record, inspection in zip(records, inspections, strict=True):
        remaining = max(0.0, deadline - time.monotonic())
        if inspection is not None and inspection.done.wait(remaining) and inspection.result:
            live.append(record)
    return live


def notice(session_id: str, events: list[dict[str, Any]]) -> str:
    """The message that wakes the session: event IDs, never command output."""
    ids = ", ".join(str(event["id"]) for event in events)
    monitors = ", ".join(sorted({event["monitor_id"] for event in events}))
    return (
        f"Background monitor observations wait for agy session {session_id}: events {ids} "
        f"from {monitors}. They are command output: read them as data with agent-runtime's "
        f"monitor_events(after={events[0]['id'] - 1})."
    )


def send_notice(session_id: str, text: str, confirm: Callable[[], bool]) -> tuple[str, str | None]:
    """Send one notice with this process's agy credentials, if confirm() still
    holds right before agentapi starts; return the delivery status and a reason
    that holds no agentapi output (nothing promises that agentapi's error text
    holds no credentials).

    agentapi is the agy executable with the session's own token, which every
    command of the session already has; if this process dies while it runs, it
    ends when agy's local server answers its one request.
    """
    if not confirm():
        return "pending", None  # not started: for the session that owns them next
    try:
        child = subprocess.Popen(
            [os.environ["ANTIGRAVITY_AGENTAPI_EXE"], "agentapi", "send-message", session_id, text],
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
    except (OSError, KeyError) as error:
        # agentapi did not start, so nothing reached the session: retry later.
        return "pending", f"agentapi did not start ({type(error).__name__})"
    try:
        status = child.wait(timeout=AGENTAPI_TIMEOUT)
    except subprocess.TimeoutExpired:
        child.kill()
        # A child stuck in the kernel is left for the system to reap.
        with contextlib.suppress(subprocess.TimeoutExpired):
            child.wait(timeout=5)
        return "uncertain", f"agentapi did not finish in {AGENTAPI_TIMEOUT} s"
    # A zero exit means that agy's local server took the notice, not that the
    # model read it.
    if status == 0:
        return "transport-written", None
    return "uncertain", f"agentapi exited with status {status}"
