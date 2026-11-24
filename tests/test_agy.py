"""agy sessions against a stand-in agy process and a recording agentapi."""

import json
import os
import subprocess
import sys
import time
import uuid

import pytest

from agent_runtime import operations
from agent_runtime.claude import process_alive
from agent_runtime.hooks import app
from agent_runtime.monitors import Monitors
from agent_runtime.sessions import Registry

# The live agy process of a conversation keeps its presence lock open and runs
# hooks and the agent's commands as its descendants.
FAKE_AGY = """\
import subprocess, sys, time

lock = open(sys.argv[1], "a")
subprocess.run(sys.argv[2:], check=True)
time.sleep(300)
"""

AGENTAPI = """\
import json, sys

with open(RECORD, "a") as record:
    record.write(json.dumps(sys.argv[1:]) + "\\n")
print(json.dumps({"response": {"sendMessage": {"recipientId": sys.argv[3]}}}))
"""

# A SessionStart hook and then a command of the agent, in one process.
AGENT = """\
import json, sys

from agent_runtime import operations
from agent_runtime.hooks import app

app.handle("agy", json.loads(sys.argv[1]))
command = 'printf "first\\n"; printf "%s\\n" "${ANTIGRAVITY_CSRF_TOKEN:-no-token}"'
monitor = operations.monitor_start(command, description="probe")
with open(sys.argv[2], "w") as result:
    json.dump(monitor, result)
"""


def wait_for(predicate, timeout=15):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if result := predicate():
            return result
        time.sleep(0.05)
    raise AssertionError("condition did not become true")


@pytest.fixture
def agy_session(tmp_path):
    session_id = str(uuid.uuid4())
    lock = tmp_path / "home/.gemini/antigravity-cli/presence" / f"{session_id}.lock"
    lock.parent.mkdir(parents=True)
    lock.touch()
    record = tmp_path / "agentapi.jsonl"
    agentapi = tmp_path / "agentapi-exe"
    agentapi.write_text(f"#!{sys.executable}\nRECORD = {str(record)!r}\n{AGENTAPI}")
    agentapi.chmod(0o755)
    env = {
        **{
            name: value
            for name, value in os.environ.items()
            if name not in {"CLAUDE_CODE_SESSION_ID", "CODEX_THREAD_ID", "AGENT_SESSION_PROVIDER"}
        },
        "HOME": str(tmp_path / "home"),
        "AGENT_RUNTIME_DIRECTORY": str(tmp_path / "state"),
        "ANTIGRAVITY_CONVERSATION_ID": session_id,
        "ANTIGRAVITY_LS_ADDRESS": "127.0.0.1:1",
        "ANTIGRAVITY_CSRF_TOKEN": "fixture-token",
        "ANTIGRAVITY_AGENTAPI_EXE": str(agentapi),
    }
    payload = {
        "conversationId": session_id,
        "transcriptPath": str(tmp_path / "transcript_full.jsonl"),
        "cwd": str(tmp_path),
        "hookEventName": "SessionStart",
        "modelName": "gemini",
    }
    return session_id, lock, record, env, payload


def test_agent_command_delivers_monitor_output_to_its_own_session(tmp_path, agy_session):
    session_id, lock, record, env, payload = agy_session
    result = tmp_path / "monitor.json"
    agent = [sys.executable, "-c", AGENT, json.dumps(payload), str(result)]
    fake_agy = subprocess.Popen([sys.executable, "-c", FAKE_AGY, str(lock), *agent], env=env)
    try:
        wait_for(result.exists)
        registry = Registry(tmp_path / "state")
        session = registry.get("agy", session_id)
        assert session["pid"] == fake_agy.pid
        assert session["cwd"] == str(tmp_path)
        assert json.loads(result.read_text())["permission_class"] == "prompting"

        sent = wait_for(
            lambda: (
                record.exists()
                and "no-token" in record.read_text()
                and [json.loads(line) for line in record.read_text().splitlines()]
            )
        )
        assert all(call[:3] == ["agentapi", "send-message", session_id] for call in sent)
        text = "\n".join(call[3] for call in sent)
        assert "first" in text
        # The monitored command does not inherit the session's credentials.
        assert "fixture-token" not in text
        monitors = Monitors(tmp_path / "state")
        # Two lines of output and the worker's completion notice.
        wait_for(
            lambda: (
                len(events := monitors.events(session_id)) == 3
                and all(event["status"] == "transport-written" for event in events)
            )
        )
        database = (tmp_path / "state/runtime.sqlite3").read_bytes()
        assert b"fixture-token" not in database
        bridge = registry.get("agy", session_id)["bridge"]
        assert bridge["native_pid"] == fake_agy.pid
    finally:
        fake_agy.terminate()
        fake_agy.wait(timeout=5)
    # The bridge exits with its agy process.
    wait_for(lambda: not process_alive(bridge))


def test_a_hook_outside_its_agy_process_does_not_register(tmp_path, agy_session, monkeypatch):
    session_id, _lock, _record, env, payload = agy_session
    for name in ("HOME", "AGENT_RUNTIME_DIRECTORY"):
        monkeypatch.setenv(name, env[name])

    with pytest.raises(RuntimeError, match="no ancestor"):
        app.handle("agy", payload)

    with pytest.raises(KeyError):
        Registry(tmp_path / "state").get("agy", session_id)


def test_agent_runtime_cannot_send_to_an_agy_session():
    with pytest.raises(ValueError, match="cannot send to it"):
        operations.send("agy", str(uuid.uuid4()), "hello")
