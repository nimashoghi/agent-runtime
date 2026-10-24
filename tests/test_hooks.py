"""The hook command against native Codex and Claude payloads."""

import json
import socket
import subprocess
import sys
import tempfile
import time
import uuid
from pathlib import Path

import pytest
from typed_agent_hooks import shared

from agent_runtime.claude import process_alive
from agent_runtime.hooks import app
from agent_runtime.sessions import Registry


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


def test_every_handled_event_has_a_native_name_in_both_hosts():
    assert set(app.handlers) == {
        shared.events.SessionStarted,
        shared.events.PromptSubmitted,
        shared.events.ToolCallProposed,
    }
    names = {
        host: {shared.native_event_name(event, host) for event in app.handlers}
        for host in ("codex", "claude")
    }
    assert names["codex"] == names["claude"] == {"SessionStart", "UserPromptSubmit", "PreToolUse"}


@pytest.mark.parametrize("event", ["SessionStart", "UserPromptSubmit", "PreToolUse"])
def test_codex_events_register_the_session(tmp_path, monkeypatch, event):
    monkeypatch.setenv("AGENT_RUNTIME_DIRECTORY", str(tmp_path))
    session_id = str(uuid.uuid4())

    assert app.handle("codex", codex_payload(event, session_id)) is None

    record = Registry(tmp_path).get("codex", session_id)
    assert record["cwd"] == "/repo"
    assert record["permission_mode"] == "default"


def test_subagent_events_do_not_register(tmp_path, monkeypatch):
    monkeypatch.setenv("AGENT_RUNTIME_DIRECTORY", str(tmp_path))
    payload = codex_payload("PreToolUse", "parent", agent_id="a", agent_type="explore")

    assert app.handle("codex", payload) is None

    with pytest.raises(KeyError):
        Registry(tmp_path).get("codex", "parent")


def test_hook_command_reads_host_argument_and_stdin(tmp_path):
    session_id = str(uuid.uuid4())
    result = subprocess.run(
        [sys.executable, "-m", "agent_runtime.hooks", "codex"],
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
    owner = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(300)"])
    with tempfile.TemporaryDirectory(prefix="ar-hook-", dir="/tmp") as temporary:
        inbox = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
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
        payload = {
            "session_id": session_id,
            "transcript_path": str(tmp_path / "t.jsonl"),
            "cwd": "/repo",
            "hook_event_name": "SessionStart",
            "source": "startup",
            "model": "claude",
        }
        try:
            assert app.handle("claude", payload) is None
            registry = Registry(tmp_path)
            bridge = wait_for(lambda: registry.get("claude_code", session_id).get("bridge"))
            assert bridge["native_pid"] == owner.pid
            assert process_alive(bridge)

            prompt = {**payload, "hook_event_name": "UserPromptSubmit", "prompt": "hi"}
            del prompt["source"], prompt["model"]
            assert app.handle("claude", prompt) is None
            assert registry.get("claude_code", session_id)["bridge"] == bridge
        finally:
            owner.terminate()
            owner.wait(timeout=5)
            inbox.close()
        # The bridge exits when its native owner exits.
        wait_for(lambda: not process_alive(bridge))
