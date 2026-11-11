"""Reconnectable clients for privately owned, detached Claude SDK workers."""

from __future__ import annotations

import asyncio
import contextlib
import hashlib
import importlib.util
import json
import math
import os
import shutil
import signal
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
    effort: str | None = None
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
        if self.effort is not None and self.effort not in ("low", "medium", "high", "xhigh", "max"):
            raise ValueError("unknown Claude effort level")
        cwd = self.cwd.expanduser().resolve(strict=True)
        if not cwd.is_dir():
            raise ValueError("cwd must be a directory")
        executable = str(self.claude) if self.claude else shutil.which("claude")
        if not executable:
            raise ValueError("Claude Code executable not found")
        # The path as given, not its link target: a version manager's shim is a link to the
        # manager, which chooses the program by the name it was called with.
        claude = Path(executable).expanduser().absolute()
        if not claude.is_file():
            raise ValueError(f"Claude Code executable not found: {claude}")
        value = asdict(self)
        value.pop("environment")
        if self.effort is None:
            value.pop("effort")  # as a record of a version without the field
        value.update(cwd=str(cwd), claude=str(claude))
        return json.loads(json.dumps(value))


def launch_identity(configuration: dict[str, Any]) -> dict[str, Any]:
    """The part of a launch configuration that a repeated launch ID must repeat. The path of
    the Claude executable is not part of it: its spelling changes with the installation and
    between versions of this package (0.2.1 stored the link's target)."""
    return {key: value for key, value in configuration.items() if key != "claude"}


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


def worker_alive(value: dict[str, Any]) -> bool:
    """Whether the worker process of an execution runs.

    The worker is known by its command line, which names the execution. A reused PID cannot
    carry it, and it does not depend on the clock: a start time does (psutil on Linux
    computes it from the boot time, which a step of the system clock moves).

    Only proof makes the answer false: no such process, a zombie, or a process that is
    shown to be another one. A process that cannot be inspected counts as running. A
    caller records a worker as gone on a false answer, and that record must never be made
    for a live worker."""
    if not value.get("pid"):
        return False
    try:
        process = psutil.Process(value["pid"])
    except psutil.NoSuchProcess:
        return False
    except (psutil.Error, OSError, SystemError):
        return True  # not known
    try:
        if process.status() == psutil.STATUS_ZOMBIE:
            return False
        command = process.cmdline()
    except psutil.NoSuchProcess:
        return False
    except (psutil.Error, OSError, SystemError):
        # The command line cannot be read: on macOS, a process that is ending or one of
        # another user (psutil raises AccessDenied or SystemError).
        return _started_as_recorded(process, value)
    if not command:
        # No arguments yet (Linux, a process in the middle of its start).
        return _started_as_recorded(process, value)
    return "agent_runtime.worker" in command and value["execution_id"] in command


def _started_as_recorded(process: psutil.Process, value: dict[str, Any]) -> bool:
    """For a process whose command line says nothing: false only when its start time proves
    that it is not the recorded process."""
    try:
        started = process.create_time()
    except psutil.NoSuchProcess:
        return False
    except (psutil.Error, OSError, SystemError):
        return True  # not known
    if sys.platform == "linux" or not value.get("process_created_at"):
        # On Linux a start time moves with the system clock, so a difference is no proof.
        return True
    return abs(started - value["process_created_at"]) < 0.01


class ClaudeRuntime:
    """The client owns no worker lifetime and can reconnect after a service restart."""

    def __init__(self, directory: Path) -> None:
        if importlib.util.find_spec("claude_agent_sdk") is None:
            raise RuntimeError("ClaudeRuntime needs the claude extra: agent-runtime-py[claude]")
        self.store = Store(directory)

    def inspect(self, execution_id: str) -> dict[str, Any]:
        value = self.store.execution(execution_id)
        if value.get("result") is not None or worker_alive(value):
            return value
        if not value.get("pid") and time.time() - value["created_at"] < 60:
            return value
        # The worker can have recorded its own exit since the read above; that record stays.
        return self.store.finish(
            execution_id,
            {"status": "failed", "error": "worker exited without a terminal record"},
            rejection="worker exited before dispatch",
            observed=True,
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
                for key in ("initial_message_id", "initial_digest", "resume", "fork"):
                    if existing[key] != value[key]:
                        raise ValueError(f"launch ID already used with different {key}")
                if launch_identity(existing["configuration"]) != launch_identity(configuration):
                    raise ValueError("launch ID already used with different configuration")
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
            child: subprocess.Popen[bytes] | None = None
            try:
                with os.fdopen(fd, "ab", buffering=0) as output:
                    child = subprocess.Popen(
                        [
                            sys.executable,
                            # The worker's cwd is the session's folder, which the agent
                            # writes; it must not be on the worker's import path.
                            "-P",
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
                if child is not None:
                    # The worker started but has no usable record: it must not run. It is
                    # the leader of its own process group (`start_new_session`).
                    with contextlib.suppress(ProcessLookupError, PermissionError):
                        os.killpg(child.pid, signal.SIGKILL)
                    child.wait()
                self.store.finish(
                    execution_id,
                    {"status": "failed", "error": str(error)},
                    rejection="worker could not start",
                )
                raise
            return self.store.execution(execution_id)

    async def submit(
        self, session_id: str, text: str, *, message_id: str, options: LaunchOptions, resume: bool
    ) -> dict[str, Any]:
        # Submissions of one session are serialized. A worker closes its execution to new
        # inputs before it exits (`Store.close_to_inputs`), and `Store.accept` refuses a
        # closed one in the same transaction. The input then goes to a resumed execution,
        # whose worker waits for the session's lease until the old worker is gone.
        with FileLock(str(self.store.root / f"submit-{session_id}.lock")):
            if saved := self.store.receipt(session_id, message_id):
                self.inspect(saved["execution_id"])
                receipt = self.store.accept(session_id, saved["execution_id"], message_id, text)
                assert receipt is not None  # a message ID with a receipt returns it
                return receipt
            latest = self.store.latest(session_id)
            if latest is None:
                raise KeyError("unknown managed Claude session")
            latest = self.inspect(latest["execution_id"])
            if receipt := self.store.accept(session_id, latest["execution_id"], message_id, text):
                return receipt
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
        # Every worker of the session, not only this execution's: an earlier one can still
        # end (state `stopping`) after a later one has its result.
        for other in self.store.executions(value["session_id"]):
            if self.inspect(other["execution_id"]).get("result") is None:
                raise ValueError(
                    "stop the managed worker before opening this session in interactive Claude"
                )
        return [value["configuration"]["claude"], "--resume", value["session_id"]]
