"""The hooks against hand-written payloads in the native Codex and Claude shapes."""

import json
import socket
import subprocess
import sys
import tempfile
import time
import uuid
from pathlib import Path

import pytest

from agent_runtime.claude import process_alive
from agent_runtime.hooks import app
from agent_runtime.sessions import Registry, own_claude_registration


def wait_for(predicate, timeout=15):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if result := predicate():
            return result
        time.sleep(0.05)
    raise AssertionError("condition did not become true")


def codex_payload(event: str, session_id: str, **fields) -> dict:
    payload = {
        "session_id": session_id,
        "transcript_path": None,
        "cwd": "/repo",
        "hook_event_name": event,
        "model": "gpt-5",
        "permission_mode": "default",
    }
    if event == "SessionStart":
        payload["source"] = "startup"
    elif event == "UserPromptSubmit":
        payload.update(turn_id="t", prompt="hello")
    elif event == "PreToolUse":
        payload.update(
            turn_id="t", tool_name="Bash", tool_use_id="u", tool_input={"command": "echo ok"}
        )
    return {**payload, **fields}


@pytest.mark.parametrize("event", ["SessionStart", "UserPromptSubmit", "PreToolUse"])
def test_codex_events_register_the_session(tmp_path, monkeypatch, event):
    monkeypatch.setenv("AGENT_RUNTIME_DIRECTORY", str(tmp_path))
    session_id = str(uuid.uuid4())

    assert app.handle("codex", codex_payload(event, session_id)) is None

    record = Registry(tmp_path).get("codex", session_id)
    assert record["cwd"] == "/repo"
    assert record["permission_mode"] == "default"


@pytest.mark.parametrize("host", ["codex", "claude"])
def test_subagent_events_do_not_register(tmp_path, monkeypatch, host):
    monkeypatch.setenv("AGENT_RUNTIME_DIRECTORY", str(tmp_path))
    monkeypatch.delenv("CLAUDE_PID", raising=False)
    payload = codex_payload("PreToolUse", "parent", agent_id="a", agent_type="explore")
    if host == "claude":
        payload["transcript_path"] = str(tmp_path / "t.jsonl")

    assert app.handle(host, payload) is None

    for provider in ("codex", "claude_code"):
        with pytest.raises(KeyError):
            Registry(tmp_path).get(provider, "parent")


def test_hook_command_reads_host_argument_and_stdin(tmp_path):
    session_id = str(uuid.uuid4())
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            "from agent_runtime.hooks import app\nif __name__ == '__main__':\n    app.main()",
            "codex",
        ],
        input=json.dumps(codex_payload("SessionStart", session_id)),
        env={"AGENT_RUNTIME_DIRECTORY": str(tmp_path), "PATH": "/usr/bin:/bin"},
        capture_output=True,
        text=True,
        check=True,
    )

    assert (result.stdout, result.stderr) == ("", "")
    assert Registry(tmp_path).get("codex", session_id)["cwd"] == "/repo"


def test_claude_session_start_starts_one_delivery_bridge(tmp_path, monkeypatch):
    session_id = str(uuid.uuid4())
    payload = {
        "session_id": session_id,
        "transcript_path": str(tmp_path / "t.jsonl"),
        "cwd": "/repo",
        "hook_event_name": "SessionStart",
        "source": "startup",
        "model": "claude",
    }
    owner = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(300)"])
    try:
        with (
            tempfile.TemporaryDirectory(prefix="ar-hook-", dir="/tmp") as temporary,
            socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as inbox,
        ):
            inbox.bind(str(Path(temporary) / "inbox.sock"))
            inbox.listen()
            for name, value in {
                "AGENT_RUNTIME_DIRECTORY": str(tmp_path),
                "CLAUDE_CODE_SESSION_ID": session_id,
                "CLAUDE_PID": str(owner.pid),
                "CLAUDE_CODE_MESSAGING_SOCKET": str(Path(temporary) / "inbox.sock"),
                "CLAUDE_CODE_MESSAGING_TOKEN": "fixture-token",
            }.items():
                monkeypatch.setenv(name, value)

            assert app.handle("claude", payload) is None
            registry = Registry(tmp_path)
            # The hook records the bridge before it returns.
            bridge = registry.get("claude_code", session_id)["bridge"]
            assert bridge["native_pid"] == owner.pid
            assert process_alive(bridge)

            prompt = {
                **payload,
                "hook_event_name": "UserPromptSubmit",
                "prompt": "hi",
                "permission_mode": "default",
            }
            assert app.handle("claude", prompt) is None
            assert registry.get("claude_code", session_id)["bridge"] == bridge

            # SessionStart after compaction has no permission mode.
            assert app.handle("claude", {**payload, "source": "compact"}) is None
            assert registry.get("claude_code", session_id)["permission_mode"] == "default"
    finally:
        owner.terminate()
        owner.wait(timeout=5)
    # The bridge exits when its native owner exits.
    wait_for(lambda: not process_alive(bridge))


def test_permission_mode_belongs_to_the_native_process(tmp_path, monkeypatch):
    registry = Registry(tmp_path)
    monkeypatch.setenv("CLAUDE_CODE_MESSAGING_SOCKET", str(tmp_path / "inbox.sock"))
    monkeypatch.delenv("CLAUDE_CODE_SESSION_ID", raising=False)
    sleeper = [sys.executable, "-c", "import time; time.sleep(300)"]
    first = subprocess.Popen(sleeper)
    try:
        monkeypatch.setenv("CLAUDE_PID", str(first.pid))
        own_claude_registration("s", "/repo", "default", registry=registry)
        # SessionStart after compaction: same process, no mode in the payload.
        own_claude_registration("s", "/repo", None, registry=registry)
        assert registry.get("claude_code", "s")["permission_mode"] == "default"
    finally:
        first.terminate()
        first.wait(timeout=5)
    resumed = subprocess.Popen(sleeper)
    try:
        monkeypatch.setenv("CLAUDE_PID", str(resumed.pid))
        own_claude_registration("s", "/repo", None, registry=registry)
        assert registry.get("claude_code", "s")["permission_mode"] is None
    finally:
        resumed.terminate()
        resumed.wait(timeout=5)
