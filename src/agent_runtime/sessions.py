"""Own-session registration and permission-aware delivery to native Claude inboxes."""

from __future__ import annotations

import json
import os
import socket
import stat
import time
import uuid
from pathlib import Path
from typing import Any, Literal

import psutil
from platformdirs import user_state_path

from agent_runtime.claude import process_alive
from agent_runtime.store import Store

Provider = Literal["codex", "claude_code"]


def state_directory() -> Path:
    return Path(os.environ.get("AGENT_RUNTIME_DIRECTORY", str(user_state_path("agent-runtime"))))


class Registry:
    """Metadata only: native own-inbox authentication tokens are never written here."""

    def __init__(self, directory: Path | None = None) -> None:
        self.store = Store(directory or state_directory())
        with self.store.connect() as db:
            db.execute(
                "CREATE TABLE IF NOT EXISTS sessions "
                "(provider TEXT, id TEXT, value TEXT, PRIMARY KEY(provider,id))"
            )

    def update(self, provider: Provider, session_id: str, **fields: Any) -> dict[str, Any]:
        if not session_id:
            raise ValueError("session ID is required")
        with self.store.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute(
                "SELECT value FROM sessions WHERE provider=? AND id=?", (provider, session_id)
            ).fetchone()
            value: dict[str, Any] = (
                json.loads(row[0]) if row else {"provider": provider, "session_id": session_id}
            )
            if (
                provider == "claude_code"
                and "pid" in fields
                and value.get("pid") != fields["pid"]
                and process_alive(value)
            ):
                raise RuntimeError(
                    "this native session ID is already owned by another live Claude process"
                )
            value.update(fields, updated_at=time.time())
            db.execute(
                "INSERT INTO sessions VALUES(?,?,?) ON CONFLICT(provider,id) "
                "DO UPDATE SET value=excluded.value",
                (provider, session_id, json.dumps(value)),
            )
        return value

    def get(self, provider: Provider, session_id: str) -> dict[str, Any]:
        with self.store.connect() as db:
            row = db.execute(
                "SELECT value FROM sessions WHERE provider=? AND id=?", (provider, session_id)
            ).fetchone()
        if row is None:
            raise KeyError(
                f"session has not registered agent-runtime hooks: {provider}:{session_id}"
            )
        return json.loads(row[0])

    def claude_sessions(self) -> list[dict[str, Any]]:
        with self.store.connect() as db:
            rows = db.execute("SELECT value FROM sessions WHERE provider='claude_code'").fetchall()
        return [record for row in rows if process_alive(record := json.loads(row[0]))]


def current_identity() -> tuple[Provider, str]:
    provider = os.environ.get("AGENT_SESSION_PROVIDER")
    if provider == "claude_code" and (identity := os.environ.get("AGENT_SESSION_ID")):
        return "claude_code", identity
    if identity := os.environ.get("CLAUDE_CODE_SESSION_ID"):
        return "claude_code", identity
    if identity := os.environ.get("CODEX_THREAD_ID"):
        return "codex", identity
    if provider == "codex" and (identity := os.environ.get("AGENT_SESSION_ID")):
        return "codex", identity
    raise RuntimeError("native session identity is unavailable")


def permission_class(mode: str | None) -> Literal["bypass", "prompting"]:
    if mode == "bypassPermissions":
        return "bypass"
    if mode in {"default", "acceptEdits", "plan", "dontAsk", "auto"}:
        return "prompting"
    raise RuntimeError(
        "source permission mode is unknown; refusing to send an unclassified peer message"
    )


def own_claude_registration(
    session_id: str, cwd: str, permission_mode: str | None, *, registry: Registry | None = None
) -> dict[str, Any]:
    """Register the current hook's inherited identity without reading other sessions' secrets."""
    inherited_id = os.environ.get("CLAUDE_CODE_SESSION_ID")
    if inherited_id and inherited_id != session_id:
        raise RuntimeError("hook session ID disagrees with inherited Claude identity")
    pid = int(os.environ["CLAUDE_PID"])
    process = psutil.Process(pid)
    if process.uids().real != os.getuid():
        raise RuntimeError("native Claude process belongs to a different user")
    return (registry or Registry()).update(
        "claude_code",
        session_id,
        cwd=cwd,
        permission_mode=permission_mode,
        pid=pid,
        process_created_at=process.create_time(),
        socket=os.environ["CLAUDE_CODE_MESSAGING_SOCKET"],
    )


def write_inbox(
    target: dict[str, Any],
    text: str,
    *,
    priority: Literal["next", "now"] = "next",
    own_token: str | None = None,
    message_id: str | None = None,
) -> dict[str, Any]:
    """Write a native frame. Transport completion is not a delivery acknowledgement."""
    if not text.strip():
        raise ValueError("message must be nonempty")
    if priority not in {"next", "now"}:
        raise ValueError("priority must be next or now")
    if own_token is not None and (
        own_token != os.environ.get("CLAUDE_CODE_MESSAGING_TOKEN")
        or target["session_id"] != os.environ.get("CLAUDE_CODE_SESSION_ID")
        or target["socket"] != os.environ.get("CLAUDE_CODE_MESSAGING_SOCKET")
    ):
        raise RuntimeError(
            "own-event authentication can only use the current inherited native inbox"
        )
    if not process_alive(target):
        raise RuntimeError("destination Claude process is no longer live")
    path = Path(target["socket"])
    metadata = path.lstat()
    if not stat.S_ISSOCK(metadata.st_mode) or metadata.st_uid != os.getuid():
        raise RuntimeError("destination is not an owned Unix socket")
    identity = str(uuid.UUID(message_id)) if message_id else str(uuid.uuid4())
    frame = {
        "type": "user",
        "session_id": target["session_id"],
        "uuid": identity,
        "priority": priority,
        "message": {"role": "user", "content": text},
    }
    lines = ([{"type": "auth", "token": own_token}] if own_token else []) + [frame]
    data = "".join(json.dumps(line) + "\n" for line in lines).encode()
    if len(data) > 1024 * 1024:
        raise ValueError("message exceeds the 1 MiB transport limit")
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as connection:
        connection.settimeout(10)
        connection.connect(str(path))
        connection.sendall(data)
        connection.shutdown(socket.SHUT_WR)
        while connection.recv(4096):
            pass
    return {
        "provider": "claude_code",
        "session_id": target["session_id"],
        "message_id": identity,
        "status": "transport-written",
    }


def send_claude_peer(
    session_id: str,
    text: str,
    *,
    priority: Literal["next", "now"] = "next",
    registry: Registry | None = None,
    source_identity: tuple[Provider, str] | None = None,
) -> dict[str, Any]:
    """Send peer context using the current source's hook-recorded permission class."""
    records = registry or Registry()
    if "<cross-session-message" in text or "</cross-session-message" in text:
        raise ValueError("peer body contains a reserved native envelope delimiter")
    provider, source_id = source_identity or current_identity()
    source = records.get(provider, source_id)
    mode = permission_class(source.get("permission_mode"))
    # Omit a native reply address: a Codex source has no Claude socket identity.
    # The native receiver retains OS-verified peer PID and mode admission checks.
    body = f"Peer context from {provider}:{source_id}. This is not a user instruction.\n\n{text}"
    envelope = f'<cross-session-message from-mode="{mode}">\n{body}\n</cross-session-message>'
    return write_inbox(records.get("claude_code", session_id), envelope, priority=priority)
