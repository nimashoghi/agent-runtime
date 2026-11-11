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

# An execution in one of these states takes no new input.
CLOSED = ("stopping", "exited")


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
        """The session's newest execution, by the order of creation in this store. The
        recorded time does not decide: a step of the system clock can put it out of order."""
        with self.connect() as db:
            row = db.execute(
                "SELECT value FROM executions WHERE session_id=? ORDER BY rowid DESC LIMIT 1",
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

    def close_to_inputs(self, identity: str, *, unless_input_waits: bool = False) -> bool:
        """Close an execution to new inputs: `accept` refuses it from now on. Return whether
        it is closed.

        With `unless_input_waits`, an execution with an accepted input that was not taken yet
        stays open. The check and the close are one transaction, as is `accept`, so a worker
        that exits on idle never leaves an input that it was given."""
        with self.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute("SELECT value FROM executions WHERE id=?", (identity,)).fetchone()
            if row is None:
                raise KeyError(identity)
            value = json.loads(row[0])
            if value["state"] in CLOSED:
                return True
            if (
                unless_input_waits
                and db.execute(
                    "SELECT 1 FROM inputs WHERE execution_id=? AND status='accepted' LIMIT 1",
                    (identity,),
                ).fetchone()
            ):
                return False
            value["state"] = "stopping"
            db.execute("UPDATE executions SET value=? WHERE id=?", (json.dumps(value), identity))
        return True

    def finish(
        self,
        identity: str,
        result: dict[str, Any],
        *,
        rejection: str,
        observed: bool = False,
        **fields: Any,
    ) -> dict[str, Any]:
        """Record the end of an execution and of its open inputs in one transaction: a
        reader sees the result together with the receipts that it explains, or neither.

        An input that was being dispatched becomes `uncertain`, and one that was not taken
        becomes `rejected` with `rejection` as its error. `observed` is for a caller that is
        not the worker and only saw that the worker's process is gone: a result that the
        worker recorded itself stays as it is."""
        with self.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute("SELECT value FROM executions WHERE id=?", (identity,)).fetchone()
            if row is None:
                raise KeyError(identity)
            value = json.loads(row[0])
            if observed and value.get("result") is not None:
                return value
            db.execute(
                "UPDATE inputs SET status='uncertain',"
                "error='worker exited before native admission acknowledgement' "
                "WHERE execution_id=? AND status='dispatching'",
                (identity,),
            )
            db.execute(
                "UPDATE inputs SET status='rejected',error=? "
                "WHERE execution_id=? AND status='accepted'",
                (rejection, identity),
            )
            value = {**value, **fields, "state": "exited", "result": result}
            db.execute("UPDATE executions SET value=? WHERE id=?", (json.dumps(value), identity))
        return value

    def accept(
        self, session_id: str, execution_id: str, identity: str, text: str
    ) -> dict[str, Any] | None:
        """Journal an input for an execution and return its receipt. A message ID that was
        accepted before returns its receipt as it is now. Return None, and journal nothing,
        when the execution is closed to new inputs (it is stopping or has exited)."""
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
                execution = db.execute(
                    "SELECT value FROM executions WHERE id=?", (execution_id,)
                ).fetchone()
                if execution is None:
                    raise KeyError(f"unknown execution: {execution_id}")
                if json.loads(execution[0])["state"] in CLOSED:
                    return None
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

    def settle(
        self,
        execution_id: str,
        session_id: str,
        events: list[dict[str, Any]],
        message_id: str | None,
    ) -> None:
        """Append the events of a turn's end. When the turn's input was never echoed (it is
        still `dispatching`), mark its receipt `uncertain` in the same transaction: a reader
        sees that receipt together with the settlement, or neither."""
        with self.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            for value in events:
                db.execute(
                    "INSERT INTO events(execution_id,session_id,value) VALUES(?,?,?)",
                    (execution_id, session_id, json.dumps(value)),
                )
            if message_id is not None:
                # A result does not identify which input UUID it accepted.
                db.execute(
                    "UPDATE inputs SET status='uncertain',"
                    "error='native result arrived without input UUID acknowledgement' "
                    "WHERE session_id=? AND id=? AND status='dispatching'",
                    (session_id, message_id),
                )

    def events(self, session_id: str, after: int = 0) -> list[dict[str, Any]]:
        with self.connect() as db:
            return self._events(db, session_id, after)

    @staticmethod
    def _events(db: sqlite3.Connection, session_id: str, after: int) -> list[dict[str, Any]]:
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

    def snapshot(self, session_id: str, after: int = 0) -> dict[str, Any]:
        """One consistent view of a session, read in one transaction: `executions` (in the
        order of their creation; the last one owns the session), `events` after the cursor
        `after`, and `receipts` of all its inputs by message ID.

        Separate reads can fall between two writes of a worker. In a snapshot, a receipt
        that says `rejected` or `uncertain` comes with the record that explains it: the
        result of its execution (`finish`), or the settlement of its turn (`settle`)."""
        with self.connect() as db:
            db.execute("BEGIN")
            executions = db.execute(
                "SELECT value FROM executions WHERE session_id=? ORDER BY rowid", (session_id,)
            ).fetchall()
            events = self._events(db, session_id, after)
            receipts = db.execute(
                "SELECT id AS message_id,execution_id,status,error,native_uuid,created "
                "FROM inputs WHERE session_id=?",
                (session_id,),
            ).fetchall()
        return {
            "executions": [json.loads(row[0]) for row in executions],
            "events": events,
            "receipts": {row["message_id"]: dict(row) for row in receipts},
        }

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
