"""Reconnectable clients for privately owned, detached Claude SDK workers."""

from __future__ import annotations

import asyncio
import hashlib
import json
import math
import os
import shutil
import subprocess
import sys
import time
import uuid
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

import psutil
from filelock import FileLock

from agent_runtime.store import Store


@dataclass(frozen=True)
class LaunchOptions:
    cwd: Path
    environment: dict[str, str] = field(default_factory=dict)
    claude: Path | None = None
    instructions: str | None = None
    idle_timeout_seconds: float = 3600
    permission_mode: str = "default"
    allowed_tools: tuple[str, ...] = ()
    model: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    def configuration(self) -> dict[str, Any]:
        """Serialize launch policy without persisting the caller's environment."""
        if not math.isfinite(self.idle_timeout_seconds) or self.idle_timeout_seconds < 0:
            raise ValueError("idle timeout must be finite and nonnegative")
        if self.permission_mode not in {
            "default",
            "acceptEdits",
            "bypassPermissions",
            "plan",
            "dontAsk",
            "auto",
        }:
            raise ValueError("unknown Claude permission mode")
        cwd = self.cwd.expanduser().resolve(strict=True)
        if not cwd.is_dir():
            raise ValueError("cwd must be a directory")
        executable = str(self.claude) if self.claude else shutil.which("claude")
        if not executable:
            raise ValueError("Claude Code executable not found")
        value = asdict(self)
        value.pop("environment")
        value.update(cwd=str(cwd), claude=str(Path(executable).expanduser().resolve(strict=True)))
        return json.loads(json.dumps(value))


def process_alive(value: dict[str, Any]) -> bool:
    """PID reuse must never attach a client to an unrelated process."""
    if not value.get("pid") or not value.get("process_created_at"):
        return False
    try:
        process = psutil.Process(value["pid"])
        return (
            abs(process.create_time() - value["process_created_at"]) < 0.01
            and process.status() != psutil.STATUS_ZOMBIE
        )
    except psutil.Error:
        return False


class ClaudeRuntime:
    """The client owns no worker lifetime and can reconnect after a service restart."""

    def __init__(self, directory: Path) -> None:
        self.store = Store(directory)

    def inspect(self, execution_id: str) -> dict[str, Any]:
        value = self.store.execution(execution_id)
        if value.get("result") is not None or process_alive(value):
            return value
        if not value.get("pid") and time.time() - value["created_at"] < 60:
            return value
        self.store.lose_dispatches(execution_id)
        self.store.reject_pending(execution_id, "worker exited before dispatch")
        return self.store.update(
            execution_id,
            state="exited",
            result={"status": "failed", "error": "worker exited without a terminal record"},
        )

    async def start(
        self,
        text: str,
        *,
        message_id: str,
        options: LaunchOptions,
        launch_id: str,
        fork_session_id: str | None = None,
    ) -> dict[str, Any]:
        execution_id = str(uuid.uuid5(uuid.NAMESPACE_URL, "agent-runtime:claude:" + launch_id))
        session_id = str(uuid.uuid5(uuid.NAMESPACE_URL, "agent-runtime:session:" + launch_id))
        return self._launch(
            execution_id,
            session_id,
            text,
            message_id,
            options,
            resume=fork_session_id,
            fork=bool(fork_session_id),
        )

    def _launch(
        self,
        execution_id: str,
        session_id: str,
        text: str,
        message_id: str,
        options: LaunchOptions,
        *,
        resume: str | None,
        fork: bool,
    ) -> dict[str, Any]:
        configuration = options.configuration()
        with FileLock(str(self.store.root / f"launch-{session_id}.lock")):
            value = {
                "provider": "claude_code",
                "execution_id": execution_id,
                "session_id": session_id,
                "created_at": time.time(),
                "state": "starting",
                "result": None,
                "configuration": configuration,
                "resume": resume,
                "fork": fork,
                "initial_message_id": message_id,
                "initial_digest": hashlib.sha256(text.encode()).hexdigest(),
            }
            if not self.store.create_execution(value):
                existing = self.inspect(execution_id)
                for key in (
                    "initial_message_id",
                    "initial_digest",
                    "resume",
                    "fork",
                    "configuration",
                ):
                    if existing[key] != value[key]:
                        raise ValueError(f"launch ID already used with different {key}")
                return existing
            self.store.accept(session_id, execution_id, message_id, text)
            environment = {**os.environ, **options.environment}
            # A new native child establishes its own identity and messaging authority.
            for name in (
                "CLAUDECODE",
                "CLAUDE_CODE_SESSION_ID",
                "CLAUDE_CODE_MESSAGING_SOCKET",
                "CLAUDE_CODE_MESSAGING_TOKEN",
                "CLAUDE_PID",
                "CODEX_THREAD_ID",
                "CODEXR_SESSION_ROOT",
                "AGENT_SUBAGENT_ID",
            ):
                environment.pop(name, None)
            environment.update(AGENT_SESSION_PROVIDER="claude_code", AGENT_SESSION_ID=session_id)
            log = self.store.root / f"{execution_id}.log"
            fd = os.open(log, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
            try:
                with os.fdopen(fd, "ab", buffering=0) as output:
                    child = subprocess.Popen(
                        [
                            sys.executable,
                            "-m",
                            "agent_runtime.worker",
                            str(self.store.root),
                            execution_id,
                        ],
                        stdin=subprocess.DEVNULL,
                        stdout=output,
                        stderr=output,
                        cwd=configuration["cwd"],
                        env=environment,
                        start_new_session=True,
                    )
                self.store.update(
                    execution_id,
                    pid=child.pid,
                    process_created_at=psutil.Process(child.pid).create_time(),
                )
            except Exception as error:
                self.store.reject_pending(execution_id, "worker could not start")
                self.store.update(
                    execution_id, state="exited", result={"status": "failed", "error": str(error)}
                )
                raise
            return self.store.execution(execution_id)

    async def submit(
        self, session_id: str, text: str, *, message_id: str, options: LaunchOptions, resume: bool
    ) -> dict[str, Any]:
        # Admission is serialized with process exit/resume decisions. Each new
        # execution also obtains a lifetime lease before connecting the native CLI.
        with FileLock(str(self.store.root / f"submit-{session_id}.lock")):
            if saved := self.store.receipt(session_id, message_id):
                self.inspect(saved["execution_id"])
                return self.store.accept(session_id, saved["execution_id"], message_id, text)
            latest = self.store.latest(session_id)
            if latest is None:
                raise KeyError("unknown managed Claude session")
            latest = self.inspect(latest["execution_id"])
            if latest.get("result") is not None:
                if not resume:
                    return {
                        "message_id": message_id,
                        "execution_id": latest["execution_id"],
                        "status": "rejected",
                        "error": "session is stopped; explicit resume required",
                    }
                identity = str(
                    uuid.uuid5(uuid.NAMESPACE_URL, f"claude-resume:{session_id}:{message_id}")
                )
                self._launch(
                    identity, session_id, text, message_id, options, resume=session_id, fork=False
                )
            else:
                self.store.accept(session_id, latest["execution_id"], message_id, text)
            receipt = self.store.receipt(session_id, message_id)
            assert receipt is not None
            return receipt

    async def stop(self, execution_id: str) -> dict[str, Any]:
        value = self.inspect(execution_id)
        if value.get("result") is not None:
            return value
        self.store.control(execution_id, "stop", {})
        for _ in range(100):
            await asyncio.sleep(0.1)
            value = self.inspect(execution_id)
            if value.get("result") is not None:
                return value
        # Report pending shutdown; do not claim completion or kill a reused PID.
        return value

    def attach(self, execution_id: str) -> list[str]:
        value = self.inspect(execution_id)
        if value.get("result") is None:
            raise ValueError(
                "stop the managed worker before opening this session in interactive Claude"
            )
        return [value["configuration"]["claude"], "--resume", value["session_id"]]
