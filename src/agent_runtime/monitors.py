"""Detached shell observations and a durable event ledger independent of model turns."""

from __future__ import annotations

import json
import math
import os
import subprocess
import sys
import time
import uuid
from pathlib import Path
from typing import Any

import psutil

from agent_runtime.authority import without_agy
from agent_runtime.claude import process_alive
from agent_runtime.sessions import Provider, Registry, current_identity, permission_class


def owner_key(provider: Provider, session_id: str) -> str:
    """The value of a monitor's session column for a native session.

    Claude Code keeps the bare ID that earlier versions wrote. Other providers
    are prefixed, so that equal session IDs of two hosts never share monitors.
    """
    return session_id if provider == "claude_code" else f"{provider}:{session_id}"


class Monitors:
    def __init__(self, directory: Path | None = None) -> None:
        self.registry = Registry(directory)
        self.store = self.registry.store
        with self.store.connect() as db:
            db.executescript("""
                CREATE TABLE IF NOT EXISTS monitors (
                    id TEXT PRIMARY KEY, session_id TEXT, value TEXT);
                CREATE TABLE IF NOT EXISTS monitor_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT, monitor_id TEXT, session_id TEXT,
                    text TEXT, status TEXT, created REAL, error TEXT
                );
                CREATE INDEX IF NOT EXISTS monitor_delivery ON monitor_events(session_id,status,id);
            """)

    def get(self, monitor_id: str) -> dict[str, Any]:
        with self.store.connect() as db:
            row = db.execute("SELECT value FROM monitors WHERE id=?", (monitor_id,)).fetchone()
        if row is None:
            raise KeyError("unknown monitor")
        value = json.loads(row[0])
        if (
            value["status"] in {"running", "starting"}
            and value.get("pid")
            and not process_alive(value)
        ):
            value = self.update(monitor_id, status="interrupted", error="monitor worker exited")
        if (
            value["status"] == "starting"
            and not value.get("pid")
            and time.time() - value["created_at"] > 60
        ):
            value = self.update(
                monitor_id, status="interrupted", error="monitor worker did not start"
            )
        return value

    def update(self, monitor_id: str, **fields: Any) -> dict[str, Any]:
        with self.store.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute("SELECT value FROM monitors WHERE id=?", (monitor_id,)).fetchone()
            if row is None:
                raise KeyError("unknown monitor")
            value = {**json.loads(row[0]), **fields, "updated_at": time.time()}
            db.execute("UPDATE monitors SET value=? WHERE id=?", (json.dumps(value), monitor_id))
        return value

    def session_of(self, monitor_id: str) -> str | None:
        """The session column of a monitor, read without reconciling its state."""
        with self.store.connect() as db:
            row = db.execute("SELECT session_id FROM monitors WHERE id=?", (monitor_id,)).fetchone()
        return row[0] if row else None

    def list(self, session_id: str) -> list[dict[str, Any]]:
        with self.store.connect() as db:
            ids = [
                row[0]
                for row in db.execute("SELECT id FROM monitors WHERE session_id=?", (session_id,))
            ]
        return [self.get(identity) for identity in ids]

    def start(
        self,
        command: str,
        *,
        description: str,
        persistent: bool = False,
        cwd: Path | None = None,
        timeout_ms: int = 300_000,
        owner: tuple[int, str] | None = None,
    ) -> dict[str, Any]:
        """Start a monitor for the current native session.

        For agy, owner is the agy process lifetime (PID, start token) that the
        caller verified (agent_runtime.operations does); the monitor is created
        only while the session's record names it, in the same transaction.
        """
        provider, session_id = current_identity()
        if provider == "codex":
            raise ValueError("use the Codex monitor adapter for a Codex session")
        if provider == "agy" and owner is None:
            raise ValueError("start agy monitors through agent_runtime.operations")
        target = self.registry.get(provider, session_id)
        if provider == "claude_code" and not process_alive(target):
            raise RuntimeError("current native process is not live")
        key = owner_key(provider, session_id)
        bridge = target.get("bridge")
        if (
            not isinstance(bridge, dict)
            or not process_alive(bridge)
            or bridge.get("native_pid") != target["pid"]
        ):
            start = (
                "check agent-runtime hooks"
                if provider == "claude_code"
                else "start agy monitors through agent_runtime.operations"
            )
            raise RuntimeError(f"the native monitor delivery bridge is not running; {start}")
        if not command.strip() or not description.strip():
            raise ValueError("command and description must be nonempty")
        if isinstance(timeout_ms, bool) or timeout_ms <= 0 or not math.isfinite(timeout_ms):
            raise ValueError("timeout_ms must be positive and finite")
        directory = (cwd or Path(target["cwd"])).resolve(strict=True)
        if not directory.is_dir():
            raise ValueError("cwd must be a directory")
        identity = "m" + uuid.uuid4().hex
        if owner is not None:
            # An agy owner lives while its lifetime holds the conversation.
            lifetime = {"pid": owner[0], "start_token": owner[1], "conversation": session_id}
        else:
            lifetime = {"pid": target["pid"], "process_created_at": target["process_created_at"]}
        value = {
            "monitor_id": identity,
            "session_id": key,
            "command": command,
            "description": description,
            "cwd": str(directory),
            "timeout_ms": timeout_ms,
            "persistent": persistent,
            "status": "starting",
            "created_at": time.time(),
            # agy hooks report no permission mode, so agy monitors get the cautious class.
            "permission_class": "prompting"
            if provider == "agy"
            else permission_class(target.get("permission_mode")),
            "owner": lifetime,
        }
        with self.store.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            if owner is not None:
                row = db.execute(
                    "SELECT value FROM sessions WHERE provider=? AND id=?", (provider, session_id)
                ).fetchone()
                current = json.loads(row[0]) if row else {}
                if (current.get("pid"), current.get("native_start")) != owner:
                    raise RuntimeError("the session moved to another native process")
            db.execute("INSERT INTO monitors VALUES(?,?,?)", (identity, key, json.dumps(value)))
        # The monitored command is not the session: it gets neither its message
        # credentials nor an agy session's identity.
        environment = without_agy(dict(os.environ))
        for name in ("CLAUDE_CODE_MESSAGING_TOKEN", "CLAUDE_CODE_MESSAGING_SOCKET"):
            environment.pop(name, None)
        try:
            fd = os.open(
                self.store.root / f"{identity}.log", os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600
            )
            with os.fdopen(fd, "ab", buffering=0) as output:
                child = subprocess.Popen(
                    [
                        sys.executable,
                        "-m",
                        "agent_runtime.monitor_worker",
                        str(self.store.root),
                        identity,
                    ],
                    stdin=subprocess.DEVNULL,
                    stdout=output,
                    stderr=output,
                    env=environment,
                    start_new_session=True,
                )
            try:
                created = psutil.Process(child.pid).create_time()
            except psutil.NoSuchProcess:
                child.wait()
            else:
                self.update(identity, pid=child.pid, process_created_at=created)
        except Exception as error:
            self.update(identity, status="failed", error=str(error))
            raise
        return self.get(identity)

    def stop(self, monitor_id: str) -> dict[str, Any]:
        value = self.get(monitor_id)
        if value["status"] in {"starting", "running"}:
            value = self.update(monitor_id, stop_requested=True)
        return value

    def append(self, monitor_id: str, text: str) -> int:
        value = self.get(monitor_id)
        with self.store.connect() as db:
            cursor = db.execute(
                "INSERT INTO monitor_events(monitor_id,session_id,text,status,created) "
                "VALUES(?,?,?,'pending',?)",
                (monitor_id, value["session_id"], text, time.time()),
            )
            assert cursor.lastrowid is not None
            return cursor.lastrowid

    def events(self, session_id: str, *, after: int = 0) -> list[dict[str, Any]]:
        with self.store.connect() as db:
            return [
                dict(row)
                for row in db.execute(
                    "SELECT * FROM monitor_events WHERE session_id=? AND id>? ORDER BY id",
                    (session_id, after),
                )
            ]

    def claim(
        self, session_id: str, *, owner: tuple[Provider, str, int, str] | None = None
    ) -> list[dict[str, Any]]:
        """Mark up to 32 pending events dispatching and return them. With owner
        (provider, native session ID, PID, start token), claim nothing unless the
        session's record still names that process lifetime, in the same
        transaction."""
        with self.store.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            if owner is not None:
                provider, native_id, pid, token = owner
                row = db.execute(
                    "SELECT value FROM sessions WHERE provider=? AND id=?", (provider, native_id)
                ).fetchone()
                current = json.loads(row[0]) if row else {}
                if (current.get("pid"), current.get("native_start")) != (pid, token):
                    return []
            rows = db.execute(
                "SELECT * FROM monitor_events WHERE session_id=? AND status='pending' "
                "ORDER BY id LIMIT 32",
                (session_id,),
            ).fetchall()
            total = 0
            selected = []
            for row in rows:
                if total + len(row["text"]) > 65536 and selected:
                    break
                selected.append(dict(row))
                total += len(row["text"])
            db.executemany(
                "UPDATE monitor_events SET status='dispatching' WHERE id=?",
                [(row["id"],) for row in selected],
            )
        return selected

    def delivered(self, identity: int, status: str, error: str | None = None) -> None:
        with self.store.connect() as db:
            db.execute(
                "UPDATE monitor_events SET status=?,error=? WHERE id=?", (status, error, identity)
            )

    def interrupted_delivery(self, session_id: str) -> None:
        with self.store.connect() as db:
            db.execute(
                "UPDATE monitor_events SET status='uncertain',"
                "error='delivery bridge exited during a delivery attempt' "
                "WHERE session_id=? AND status='dispatching'",
                (session_id,),
            )
