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

from agent_runtime import agy
from agent_runtime.authority import without_agy
from agent_runtime.claude import process_alive
from agent_runtime.processes import lifetime_alive
from agent_runtime.sessions import Provider, Registry, current_identity, permission_class


def owner_key(provider: Provider, session_id: str) -> str:
    """The value of a monitor's session column for a native session.

    Claude Code keeps the bare ID that earlier versions wrote. Other providers
    are prefixed, so that equal session IDs of two hosts never share monitors.
    """
    return session_id if provider == "claude_code" else f"{provider}:{session_id}"


def _worker_alive(value: dict[str, Any]) -> bool:
    """A worker that recorded its start token is judged by it, unchanged by clock steps."""
    if token := value.get("start_token"):
        return lifetime_alive(value["pid"], token) is not False
    return process_alive(value)


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
                CREATE INDEX IF NOT EXISTS monitor_claims ON monitor_events(monitor_id,status,id);
            """)

    def get(self, monitor_id: str) -> dict[str, Any]:
        with self.store.connect() as db:
            row = db.execute("SELECT value FROM monitors WHERE id=?", (monitor_id,)).fetchone()
        if row is None:
            raise KeyError("unknown monitor")
        value = json.loads(row[0])
        gone = bool(value.get("pid")) and not _worker_alive(value)
        if gone and value["status"] in {"running", "starting"}:
            value = self._interrupt(monitor_id, value["pid"])
        if gone and "start_token" in value.get("owner", {}):
            # An agy worker delivers its own events; one that died while sending
            # (also its final notice, after the command's outcome) leaves its
            # batch neither sent nor pending. Repeating this changes nothing.
            self.interrupted_delivery(value["session_id"], monitor_id=monitor_id)
        if (
            value["status"] == "starting"
            and not value.get("pid")
            and time.time() - value["created_at"] > 60
        ):
            value = self._interrupt(monitor_id, None, error="monitor worker did not start")
        return value

    def _interrupt(
        self, monitor_id: str, pid: int | None, *, error: str = "monitor worker exited"
    ) -> dict[str, Any]:
        """Record that the worker exited (or never started), unless it recorded a
        PID or an outcome meanwhile: the check is in the write transaction."""
        with self.store.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute("SELECT value FROM monitors WHERE id=?", (monitor_id,)).fetchone()
            value = json.loads(row[0])
            if value["status"] in {"running", "starting"} and value.get("pid") == pid:
                value.update(status="interrupted", error=error, updated_at=time.time())
                db.execute(
                    "UPDATE monitors SET value=? WHERE id=?", (json.dumps(value), monitor_id)
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
    ) -> dict[str, Any]:
        """Start a monitor for the current native session.

        An agy monitor must start from a command of the agy session: its worker
        keeps that command's credentials to deliver its own observations, and
        its owner is that command's agy process. Its cwd defaults to the
        command's.
        """
        provider, session_id = current_identity()
        if provider == "codex":
            raise ValueError("use the Codex monitor adapter for a Codex session")
        if not command.strip() or not description.strip():
            raise ValueError("command and description must be nonempty")
        if isinstance(timeout_ms, bool) or timeout_ms <= 0 or not math.isfinite(timeout_ms):
            raise ValueError("timeout_ms must be positive and finite")
        if provider == "agy":
            pid, token = agy.command_owner(session_id)
            owner = {"pid": pid, "start_token": token, "conversation": session_id}
            # agy hooks report no permission mode, so agy monitors get the cautious class.
            mode = "prompting"
            default_cwd = Path.cwd()
        else:
            target = self.registry.get(provider, session_id)
            if not process_alive(target):
                raise RuntimeError("current Claude process is not live")
            bridge = target.get("bridge")
            if (
                not isinstance(bridge, dict)
                or not process_alive(bridge)
                or bridge.get("native_pid") != target["pid"]
            ):
                raise RuntimeError(
                    "the native monitor delivery bridge is not running; check agent-runtime hooks"
                )
            owner = {"pid": target["pid"], "process_created_at": target["process_created_at"]}
            mode = permission_class(target.get("permission_mode"))
            default_cwd = Path(target["cwd"])
        directory = (cwd or default_cwd).resolve(strict=True)
        if not directory.is_dir():
            raise ValueError("cwd must be a directory")
        identity = "m" + uuid.uuid4().hex
        key = owner_key(provider, session_id)
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
            "permission_class": mode,
            "owner": owner,
        }
        with self.store.connect() as db:
            db.execute("INSERT INTO monitors VALUES(?,?,?)", (identity, key, json.dumps(value)))
        # The worker of an agy monitor keeps the command's agy credentials and
        # gives the monitored command none of them; a Claude monitor's worker
        # gets neither Claude's nor agy's.
        environment = dict(os.environ) if provider == "agy" else without_agy(dict(os.environ))
        for name in ("CLAUDE_CODE_MESSAGING_TOKEN", "CLAUDE_CODE_MESSAGING_SOCKET"):
            environment.pop(name, None)
        child = None
        try:
            fd = os.open(
                self.store.root / f"{identity}.log", os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600
            )
            with os.fdopen(fd, "ab", buffering=0) as output:
                child = subprocess.Popen(
                    [
                        sys.executable,
                        # An agy worker holds the session's token: no PYTHON*
                        # variable, user site or working folder may choose its code.
                        *(["-I"] if provider == "agy" else []),
                        "-m",
                        "agent_runtime.monitor_worker",
                        str(self.store.root),
                        identity,
                    ],
                    stdin=subprocess.DEVNULL,
                    stdout=output,
                    stderr=output,
                    env=environment,
                    cwd=self.store.root if provider == "agy" else None,
                    start_new_session=True,
                )
            try:
                created = psutil.Process(child.pid).create_time()
            except psutil.NoSuchProcess:
                child.wait()
            else:
                self.update(identity, pid=child.pid, process_created_at=created)
        except Exception as error:
            # A worker that started must not run on unseen: it stops its command
            # on SIGTERM.
            if child is not None and child.poll() is None:
                child.terminate()
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

    def claim(self, session_id: str) -> list[dict[str, Any]]:
        return self._claim("session_id", session_id)

    def claim_monitor(self, monitor_id: str) -> list[dict[str, Any]]:
        """Claim the pending events of one monitor; its agy worker is their only claimant."""
        return self._claim("monitor_id", monitor_id)

    def _claim(self, column: str, value: str) -> list[dict[str, Any]]:
        assert column in ("session_id", "monitor_id")
        with self.store.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            rows = db.execute(
                f"SELECT * FROM monitor_events WHERE {column}=? AND status='pending' "
                "ORDER BY id LIMIT 32",
                (value,),
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

    def settle(self, ids: list[int], status: str, error: str | None = None) -> None:
        """Write one batch's receipt in one transaction."""
        with self.store.connect() as db:
            db.executemany(
                "UPDATE monitor_events SET status=?,error=? WHERE id=?",
                [(status, error, identity) for identity in ids],
            )

    def delivered(self, identity: int, status: str, error: str | None = None) -> None:
        with self.store.connect() as db:
            db.execute(
                "UPDATE monitor_events SET status=?,error=? WHERE id=?", (status, error, identity)
            )

    def interrupted_delivery(self, session_id: str, *, monitor_id: str | None = None) -> None:
        """Mark the events that a delivery that ended left dispatching uncertain."""
        with self.store.connect() as db:
            db.execute(
                "UPDATE monitor_events SET status='uncertain',"
                "error='delivery exited during a delivery attempt' "
                "WHERE session_id=? AND status='dispatching' AND (? IS NULL OR monitor_id=?)",
                (session_id, monitor_id, monitor_id),
            )
