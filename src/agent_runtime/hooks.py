"""Register live identities and maintain the native monitor-delivery child."""

import os
import subprocess
import sys

import psutil
from filelock import FileLock
from typed_agent_hooks import shared

from agent_runtime.claude import process_alive
from agent_runtime.sessions import Registry, own_claude_registration

app = shared.HookApp(name="agent-runtime")


def register(event) -> None:
    context = event.context
    if context.agent_id:
        return
    registry = Registry()
    provider = context.provider.value
    if provider == "codex":
        registry.update(
            provider, context.session_id, cwd=context.cwd, permission_mode=context.permission_mode
        )
        return
    with FileLock(str(registry.store.root / f"bridge-start-{context.session_id}.lock"), timeout=5):
        record = own_claude_registration(
            context.session_id, context.cwd, context.permission_mode, registry=registry
        )
        bridge = record.get("bridge")
        if (
            isinstance(bridge, dict)
            and process_alive(bridge)
            and bridge.get("native_pid") == record["pid"]
        ):
            return
        root = registry.store.root
        fd = os.open(
            root / f"bridge-{context.session_id}.log", os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600
        )
        with os.fdopen(fd, "ab", buffering=0) as output:
            child = subprocess.Popen(
                [sys.executable, "-m", "agent_runtime.bridge", str(root), context.session_id],
                stdin=subprocess.DEVNULL,
                stdout=output,
                stderr=output,
                start_new_session=True,
            )
        registry.update(
            "claude_code",
            context.session_id,
            bridge={
                "pid": child.pid,
                "process_created_at": psutil.Process(child.pid).create_time(),
                "native_pid": record["pid"],
            },
        )


@app.on(shared.events.SessionStarted, timeout=10)
def session_started(event: shared.events.SessionStarted) -> None:
    register(event)


@app.on(shared.events.PromptSubmitted, timeout=10)
def prompt_submitted(event: shared.events.PromptSubmitted) -> None:
    register(event)


@app.on(shared.events.ToolCallProposed, timeout=10)
def tool_call_proposed(event: shared.events.ToolCallProposed) -> None:
    register(event)


def main() -> None:
    app.main()


if __name__ == "__main__":
    main()
