"""One detached owner for one native Claude process and its durable event stream."""

from __future__ import annotations

import asyncio
import logging
import os
import signal
import sys
import time
import uuid
from dataclasses import asdict, is_dataclass
from pathlib import Path
from typing import Any

import psutil
from claude_agent_sdk import (
    TERMINAL_TASK_STATUSES,
    AssistantMessage,
    ClaudeAgentOptions,
    ClaudeSDKClient,
    PermissionResultAllow,
    PermissionResultDeny,
    ResultMessage,
    TextBlock,
    ToolPermissionContext,
    UserMessage,
)
from filelock import FileLock

from agent_runtime.monitors import Monitors
from agent_runtime.store import Store

log = logging.getLogger(__name__)


class Worker:
    def __init__(self, store: Store, execution_id: str) -> None:
        self.store = store
        self.execution_id = execution_id
        self.execution = store.execution(execution_id)
        self.session_id = self.execution["session_id"]
        self.stopping = asyncio.Event()
        self.active: dict[str, Any] | None = None
        self.last_activity = time.monotonic()
        self.background_tasks: set[str] = set()
        self.presentation_started = False

    def emit(self, kind: str, **fields: Any) -> None:
        self.store.append(self.execution_id, self.session_id, {"type": kind, **fields})

    async def permission(
        self, name: str, value: dict[str, Any], context: ToolPermissionContext
    ) -> Any:
        identity = "Q" + uuid.uuid4().hex[:12]
        self.store.request(
            self.execution_id,
            identity,
            {
                "id": identity,
                "provider": "claude_code",
                "method": "agent/question"
                if name == "AskUserQuestion"
                else "agent/requestApproval",
                "params": {
                    "tool": name,
                    "input": value,
                    "title": context.title,
                    "description": context.description,
                    "reason": context.decision_reason,
                    "tool_use_id": context.tool_use_id,
                    "agent_id": context.agent_id,
                },
            },
        )
        self.emit("interaction.requested", id=identity)
        while not self.stopping.is_set():
            answer = self.store.response(identity)
            if answer is not None:
                if name == "AskUserQuestion":
                    answers = answer.get("answers")
                    if not isinstance(answers, dict) or not all(
                        isinstance(k, str) and isinstance(v, str) for k, v in answers.items()
                    ):
                        return PermissionResultDeny(
                            message="invalid question response: expected a string-to-string mapping"
                        )
                    return PermissionResultAllow(updated_input={**value, "answers": answers})
                if answer.get("decision") == "allow":
                    return PermissionResultAllow(updated_input=value)
                return PermissionResultDeny(message="The user denied this operation.")
            await asyncio.sleep(0.1)
        return PermissionResultDeny(message="Worker is stopping.", interrupt=True)

    async def receive(self, client: ClaudeSDKClient) -> None:
        async for message in client.receive_messages():
            self.last_activity = time.monotonic()
            raw = asdict(message) if is_dataclass(message) else {"description": repr(message)}
            self.emit("claude.message", message_class=type(message).__name__, message=raw)
            if isinstance(message, UserMessage) and message.uuid:
                if admitted := self.store.admit(self.execution_id, message.uuid):
                    self.emit("input.delivered", message_id=admitted, native_uuid=message.uuid)
            elif isinstance(message, AssistantMessage) and message.parent_tool_use_id is None:
                if not self.presentation_started:
                    self.emit("turn.started", origin="native")
                    self.presentation_started = True
                text = "\n".join(
                    block.text for block in message.content if isinstance(block, TextBlock)
                )
                if text:
                    self.emit("agent.message", text=text, native_uuid=message.uuid)
            elif isinstance(message, ResultMessage):
                if message.session_id != self.session_id:
                    raise RuntimeError("native Claude returned a different session ID")
                human = message.origin is None or message.origin.get("kind") == "human"
                active = self.active if human else None
                status = "failed" if message.is_error else "completed"
                if message.terminal_reason in {"aborted_streaming", "aborted_tools"}:
                    status = "interrupted"
                self.emit(
                    "agent.turn.completed",
                    status=status,
                    origin=message.origin,
                    terminal_reason=message.terminal_reason,
                    usage=message.usage,
                )
                self.emit(
                    "work.settled",
                    status=status,
                    message=message.result or "",
                    message_ids=[active["id"]] if active else [],
                    origin=message.origin,
                    errors=message.errors,
                    error="; ".join(message.errors or []) or None,
                    last_turn_status=status,
                )
                self.presentation_started = False
                if active:
                    receipt = self.store.receipt(self.session_id, active["id"])
                    if receipt and receipt["status"] == "dispatching":
                        # A result does not identify which input UUID it accepted.
                        # Preserve uncertainty if the expected native echo was lost.
                        self.store.input_status(
                            self.session_id,
                            active["id"],
                            "uncertain",
                            error="native result arrived without input UUID acknowledgement",
                        )
                    self.active = None
            else:
                data = getattr(message, "data", {})
                subtype = getattr(message, "subtype", "")
                task_id = data.get("task_id") if isinstance(data, dict) else None
                if task_id and subtype == "task_started":
                    self.background_tasks.add(task_id)
                elif (task_id and subtype == "task_notification") or (
                    task_id
                    and subtype == "task_updated"
                    and data.get("patch", {}).get("status") in TERMINAL_TASK_STATUSES
                ):
                    self.background_tasks.discard(task_id)
        if not self.stopping.is_set():
            raise RuntimeError("native Claude event stream closed")

    async def run(self) -> None:
        policy = self.execution["configuration"]
        options = ClaudeAgentOptions(
            cwd=policy["cwd"],
            cli_path=policy["claude"],
            permission_mode=policy["permission_mode"],
            allowed_tools=policy["allowed_tools"],
            model=policy["model"],
            setting_sources=["user", "project", "local"],
            system_prompt={
                "type": "preset",
                "preset": "claude_code",
                "append": policy["instructions"] or "",
            },
            can_use_tool=self.permission,
            stderr=lambda line: log.debug("claude: %s", line),
            extra_args={"replay-user-messages": None},
            session_id=self.session_id
            if not self.execution["resume"] or self.execution["fork"]
            else None,
            resume=self.execution["resume"],
            fork_session=self.execution["fork"],
        )
        for signum in (signal.SIGTERM, signal.SIGINT):
            asyncio.get_running_loop().add_signal_handler(signum, self.stopping.set)
        client = ClaudeSDKClient(options)
        reader: asyncio.Task[None] | None = None
        result: dict[str, Any] = {"status": "stopped"}
        try:
            await client.connect()
            self.store.update(self.execution_id, state="running", ready_at=time.time())
            self.emit("worker.ready", provider="claude_code", session_id=self.session_id)
            reader = asyncio.create_task(self.receive(client))
            while not self.stopping.is_set():
                if reader.done():
                    await reader
                    break
                for control in self.store.take_controls(self.execution_id):
                    match control["operation"]:
                        case "stop":
                            self.stopping.set()
                        case "interrupt":
                            await client.interrupt()
                        case "set_model":
                            await client.set_model(control["payload"]["model"])
                        case _:
                            self.emit(
                                "control.failed", id=control["id"], error="unsupported operation"
                            )
                if self.stopping.is_set():
                    break
                if self.active is None:
                    self.active = self.store.take_input(self.execution_id)
                    if self.active:
                        native_uuid = str(uuid.uuid4())
                        self.store.input_status(
                            self.session_id,
                            self.active["id"],
                            "dispatching",
                            native_uuid=native_uuid,
                        )
                        text = self.active["text"]
                        input_id = self.active["id"]

                        async def prompt(native_uuid=native_uuid, text=text):
                            yield {
                                "type": "user",
                                "uuid": native_uuid,
                                "session_id": self.session_id,
                                "origin": {"kind": "human"},
                                "message": {"role": "user", "content": text},
                            }

                        self.emit("turn.started", message_id=input_id, native_uuid=native_uuid)
                        self.presentation_started = True
                        await client.query(prompt(), session_id=self.session_id)
                        self.last_activity = time.monotonic()
                timeout = policy["idle_timeout_seconds"]
                if (
                    timeout
                    and self.active is None
                    and not self.background_tasks
                    and time.monotonic() - self.last_activity >= timeout
                    and not any(
                        monitor["status"] in {"starting", "running"}
                        for monitor in Monitors().list(self.session_id)
                    )
                ):
                    result = {"status": "completed", "reason": "idle_timeout"}
                    break
                await asyncio.sleep(0.1)
        except Exception as error:
            log.exception("Claude worker failed")
            result = {"status": "failed", "error": str(error)}
        finally:
            self.stopping.set()
            try:
                await client.disconnect()
            except Exception:
                log.exception("Claude disconnect failed")
            if reader is not None:
                reader.cancel()
                await asyncio.gather(reader, return_exceptions=True)
            self.store.lose_dispatches(self.execution_id)
            self.store.reject_pending(self.execution_id, "worker stopped before dispatch")
            self.store.update(
                self.execution_id, state="exited", result=result, exited_at=time.time()
            )
            self.emit("worker.exited", **result)


def main() -> None:
    os.umask(0o077)
    logging.basicConfig(
        level=logging.DEBUG, format="%(asctime)s %(levelname)s %(name)s %(message)s"
    )
    store, identity = Store(Path(sys.argv[1])), sys.argv[2]
    value = store.execution(identity)
    with FileLock(str(store.root / f"lease-{value['session_id']}.lock"), timeout=0):
        process = psutil.Process()
        store.update(identity, pid=process.pid, process_created_at=process.create_time())
        asyncio.run(Worker(store, identity).run())


if __name__ == "__main__":
    main()
