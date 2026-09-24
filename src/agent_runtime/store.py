"""Private durable execution state, input receipts, and ordered event cursors."""

from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import time
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any


class Store:
    def __init__(self, root: Path) -> None:
        self.root = root.expanduser().resolve()
        self.root.mkdir(parents=True, exist_ok=True, mode=0o700)
        os.chmod(self.root, 0o700)
        self.path = self.root / "runtime.sqlite3"
        with self.connect() as db:
            db.executescript(
                """
                CREATE TABLE IF NOT EXISTS executions (
                    id TEXT PRIMARY KEY, session_id TEXT NOT NULL, created REAL NOT NULL,
                    value TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS execution_sessions ON executions(session_id,created);
                CREATE TABLE IF NOT EXISTS inputs (
                    session_id TEXT NOT NULL, id TEXT NOT NULL, execution_id TEXT NOT NULL,
                    digest TEXT NOT NULL, text TEXT NOT NULL, status TEXT NOT NULL,
                    created REAL NOT NULL, error TEXT, native_uuid TEXT,
                    PRIMARY KEY(session_id,id)
                );
                CREATE TABLE IF NOT EXISTS events (
                    seq INTEGER PRIMARY KEY AUTOINCREMENT, execution_id TEXT NOT NULL,
                    session_id TEXT NOT NULL, value TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS event_sessions ON events(session_id,seq);
                CREATE TABLE IF NOT EXISTS interactions (
                    id TEXT PRIMARY KEY, execution_id TEXT NOT NULL,
                    request TEXT NOT NULL, answer TEXT
                );
                CREATE TABLE IF NOT EXISTS controls (
                    id INTEGER PRIMARY KEY AUTOINCREMENT, execution_id TEXT NOT NULL,
                    operation TEXT NOT NULL, payload TEXT NOT NULL, state TEXT NOT NULL
                );
                """
            )
        os.chmod(self.path, 0o600)

    @contextmanager
    def connect(self) -> Iterator[sqlite3.Connection]:
        db = sqlite3.connect(self.path, timeout=30)
        db.row_factory = sqlite3.Row
        try:
            with db:
                yield db
        finally:
            db.close()

    def create_execution(self, value: dict[str, Any]) -> bool:
        with self.connect() as db:
            cursor = db.execute(
                "INSERT OR IGNORE INTO executions VALUES(?,?,?,?)",
                (
                    value["execution_id"],
                    value["session_id"],
                    value["created_at"],
                    json.dumps(value),
                ),
            )
            return cursor.rowcount == 1

    def execution(self, identity: str) -> dict[str, Any]:
        with self.connect() as db:
            row = db.execute("SELECT value FROM executions WHERE id=?", (identity,)).fetchone()
        if row is None:
            raise KeyError(f"unknown execution: {identity}")
        return json.loads(row[0])

    def latest(self, session_id: str) -> dict[str, Any] | None:
        with self.connect() as db:
            row = db.execute(
                "SELECT value FROM executions WHERE session_id=? ORDER BY created DESC LIMIT 1",
                (session_id,),
            ).fetchone()
        return json.loads(row[0]) if row else None

    def update(self, identity: str, **fields: Any) -> dict[str, Any]:
        with self.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute("SELECT value FROM executions WHERE id=?", (identity,)).fetchone()
            if row is None:
                raise KeyError(identity)
            value = {**json.loads(row[0]), **fields}
            db.execute("UPDATE executions SET value=? WHERE id=?", (json.dumps(value), identity))
        return value

    def accept(
        self, session_id: str, execution_id: str, identity: str, text: str
    ) -> dict[str, Any]:
        if not identity or not text.strip():
            raise ValueError("message ID and text must be nonempty")
        digest = hashlib.sha256(text.encode()).hexdigest()
        with self.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute(
                "SELECT * FROM inputs WHERE session_id=? AND id=?", (session_id, identity)
            ).fetchone()
            if row is not None:
                if row["digest"] != digest:
                    raise ValueError("message ID was already used with different text")
            else:
                db.execute(
                    "INSERT INTO inputs(session_id,id,execution_id,digest,text,status,created) "
                    "VALUES(?,?,?,?,?,'accepted',?)",
                    (session_id, identity, execution_id, digest, text, time.time()),
                )
        receipt = self.receipt(session_id, identity)
        assert receipt is not None
        return receipt

    def receipt(self, session_id: str, identity: str) -> dict[str, Any] | None:
        with self.connect() as db:
            row = db.execute(
                "SELECT id AS message_id,execution_id,status,error,native_uuid,created "
                "FROM inputs WHERE session_id=? AND id=?",
                (session_id, identity),
            ).fetchone()
        return dict(row) if row else None

    def take_input(self, execution_id: str) -> dict[str, Any] | None:
        with self.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute(
                "SELECT * FROM inputs WHERE execution_id=? AND status='accepted' "
                "ORDER BY created LIMIT 1",
                (execution_id,),
            ).fetchone()
            if row is not None:
                db.execute(
                    "UPDATE inputs SET status='dispatching' WHERE session_id=? AND id=?",
                    (row["session_id"], row["id"]),
                )
        return dict(row) if row else None

    def input_status(
        self,
        session_id: str,
        identity: str,
        status: str,
        *,
        error: str | None = None,
        native_uuid: str | None = None,
    ) -> None:
        with self.connect() as db:
            db.execute(
                "UPDATE inputs SET status=?,error=?,native_uuid=COALESCE(?,native_uuid) "
                "WHERE session_id=? AND id=?",
                (status, error, native_uuid, session_id, identity),
            )

    def lose_dispatches(self, execution_id: str) -> None:
        with self.connect() as db:
            db.execute(
                "UPDATE inputs SET status='uncertain',"
                "error='worker exited before native admission acknowledgement' "
                "WHERE execution_id=? AND status='dispatching'",
                (execution_id,),
            )

    def reject_pending(self, execution_id: str, error: str) -> None:
        with self.connect() as db:
            db.execute(
                "UPDATE inputs SET status='rejected',error=? "
                "WHERE execution_id=? AND status='accepted'",
                (error, execution_id),
            )

    def admit(self, execution_id: str, native_uuid: str) -> str | None:
        with self.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute(
                "SELECT id FROM inputs "
                "WHERE execution_id=? AND native_uuid=? AND status='dispatching'",
                (execution_id, native_uuid),
            ).fetchone()
            if row is not None:
                db.execute(
                    "UPDATE inputs SET status='delivered',error=NULL "
                    "WHERE execution_id=? AND native_uuid=?",
                    (execution_id, native_uuid),
                )
        return row[0] if row else None

    def append(self, execution_id: str, session_id: str, value: dict[str, Any]) -> int:
        with self.connect() as db:
            cursor = db.execute(
                "INSERT INTO events(execution_id,session_id,value) VALUES(?,?,?)",
                (execution_id, session_id, json.dumps(value)),
            )
            assert cursor.lastrowid is not None
            return cursor.lastrowid

    def events(self, session_id: str, after: int = 0) -> list[dict[str, Any]]:
        with self.connect() as db:
            rows = db.execute(
                "SELECT * FROM events WHERE session_id=? AND seq>? ORDER BY seq",
                (session_id, after),
            ).fetchall()
        return [
            {
                "execution_id": row["execution_id"],
                "cursor": {"execution_id": row["execution_id"], "offset": row["seq"]},
                "event": json.loads(row["value"]),
            }
            for row in rows
        ]

    def request(self, execution_id: str, identity: str, value: dict[str, Any]) -> None:
        with self.connect() as db:
            db.execute(
                "INSERT OR IGNORE INTO interactions VALUES(?,?,?,NULL)",
                (identity, execution_id, json.dumps(value)),
            )

    def interactions(self, execution_id: str) -> list[dict[str, Any]]:
        with self.connect() as db:
            rows = db.execute(
                "SELECT request FROM interactions WHERE execution_id=? AND answer IS NULL",
                (execution_id,),
            ).fetchall()
        return [json.loads(row[0]) for row in rows]

    def answer(self, execution_id: str, identity: str, value: dict[str, Any]) -> None:
        encoded = json.dumps(value, sort_keys=True)
        with self.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute(
                "SELECT answer FROM interactions WHERE id=? AND execution_id=?",
                (identity, execution_id),
            ).fetchone()
            if row is None:
                raise KeyError("unknown interaction")
            if row[0] is not None and row[0] != encoded:
                raise ValueError("interaction already has a different answer")
            db.execute("UPDATE interactions SET answer=? WHERE id=?", (encoded, identity))

    def response(self, identity: str) -> dict[str, Any] | None:
        with self.connect() as db:
            row = db.execute("SELECT answer FROM interactions WHERE id=?", (identity,)).fetchone()
        return json.loads(row[0]) if row and row[0] else None

    def control(self, execution_id: str, operation: str, payload: dict[str, Any]) -> int:
        with self.connect() as db:
            cursor = db.execute(
                "INSERT INTO controls(execution_id,operation,payload,state) "
                "VALUES(?,?,?,'pending')",
                (execution_id, operation, json.dumps(payload)),
            )
            assert cursor.lastrowid is not None
            return cursor.lastrowid

    def take_controls(self, execution_id: str) -> list[dict[str, Any]]:
        with self.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            rows = db.execute(
                "SELECT * FROM controls WHERE execution_id=? AND state='pending' ORDER BY id",
                (execution_id,),
            ).fetchall()
            db.execute(
                "UPDATE controls SET state='dispatched' WHERE execution_id=? AND state='pending'",
                (execution_id,),
            )
        return [{**dict(row), "payload": json.loads(row["payload"])} for row in rows]
