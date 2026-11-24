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
from agent_runtime.sessions import Registry, current_identity

# The live agy process of a conversation keeps its presence lock open and runs
# hooks and the agent's commands as its children, each with its own environment.
FAKE_AGY = """\
import json, subprocess, sys, time

lock = open(sys.argv[1], "a")
for step in json.loads(sys.argv[2]):
    subprocess.run(step["argv"], env=step["env"], check=False)
time.sleep(300)
"""

# Records each call and whether it got the session's credentials, never their values.
AGENTAPI = """\
import json, os, pathlib, sys

with open(RECORD, "a") as record:
    record.write(json.dumps({
        "argv": sys.argv[1:],
        "address": os.environ.get("ANTIGRAVITY_LS_ADDRESS") == "127.0.0.1:1",
        "token": os.environ.get("ANTIGRAVITY_CSRF_TOKEN") == "fixture-token",
    }) + "\\n")
if pathlib.Path(RECORD + ".fail").exists():
    print(json.dumps({"error": "rejected fixture-token"}))
    sys.exit(1)
print(json.dumps({"response": {"sendMessage": {"recipientId": sys.argv[3]}}}))
"""

HOOK = """\
import json, sys

from agent_runtime.hooks import app

app.handle("agy", json.loads(sys.argv[1]))
"""

AGENT = """\
import json, sys

from agent_runtime import operations

command = (
    'printf "first\\\\n"; printf "%s\\\\n" '
    '"${ANTIGRAVITY_CSRF_TOKEN:-no-token}" "${ANTIGRAVITY_LS_ADDRESS:-no-address}"'
)
try:
    result = operations.monitor_start(command, description="probe")
except Exception as error:
    result = {"error": str(error)}
with open(sys.argv[1], "w") as output:
    json.dump(result, output)
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
    outer = {"CLAUDE_CODE_SESSION_ID", "CODEX_THREAD_ID", "AGENT_SESSION_PROVIDER"}
    hook_env = {
        **{name: value for name, value in os.environ.items() if name not in outer},
        "HOME": str(tmp_path / "home"),
        "AGENT_RUNTIME_DIRECTORY": str(tmp_path / "state"),
        "ANTIGRAVITY_CONVERSATION_ID": session_id,
    }
    command_env = {
        **hook_env,
        "ANTIGRAVITY_AGENT": "1",
        "ANTIGRAVITY_LS_ADDRESS": "127.0.0.1:1",
        "ANTIGRAVITY_CSRF_TOKEN": "fixture-token",
        "ANTIGRAVITY_AGENTAPI_EXE": str(agentapi),
    }
    payload = {
        "conversationId": session_id,
        "transcriptPath": str(tmp_path / "transcript_full.jsonl"),
        "cwd": str(tmp_path),
        "hookEventName": "PreToolUse",
        "modelName": "gemini",
        "toolCall": {"id": "call_1", "name": "run_command", "args": {"CommandLine": "x"}},
    }
    started = []

    def start(*, hook=True):
        result = tmp_path / "monitor.json"
        steps = [{"argv": [sys.executable, "-c", HOOK, json.dumps(payload)], "env": hook_env}]
        steps = steps if hook else []
        steps.append({"argv": [sys.executable, "-c", AGENT, str(result)], "env": command_env})
        fake = subprocess.Popen([sys.executable, "-c", FAKE_AGY, str(lock), json.dumps(steps)])
        started.append(fake)
        wait_for(result.exists)
        return fake, json.loads(result.read_text())

    yield session_id, record, start
    for fake in started:
        fake.terminate()
        fake.wait(timeout=5)


def calls(record):
    return [json.loads(line) for line in record.read_text().splitlines()] if record.exists() else []


def test_agent_command_wakes_its_own_session_and_keeps_the_token_out(tmp_path, agy_session):
    session_id, record, start = agy_session
    fake, monitor = start()
    registry = Registry(tmp_path / "state")
    session = registry.get("agy", session_id)
    assert (session["pid"], session["cwd"]) == (fake.pid, str(tmp_path))
    assert monitor["session_id"] == f"agy:{session_id}"
    assert monitor["permission_class"] == "prompting"
    monitors = Monitors(tmp_path / "state")
    # The two output lines, the credentials' absence and the completion notice.
    events = wait_for(
        lambda: (
            len(found := monitors.events(f"agy:{session_id}")) == 4
            and all(event["status"] == "transport-written" for event in found)
            and found
        )
    )
    assert [event["text"] for event in events[:3]] == ["first", "no-token", "no-address"]
    sent = calls(record)
    assert sent and all(call["address"] and call["token"] for call in sent)
    assert all(call["argv"][:3] == ["agentapi", "send-message", session_id] for call in sent)
    # The notice names the events; the output itself stays out of agy's system role.
    assert "monitor_events(after=" in sent[0]["argv"][3]
    assert all("first" not in call["argv"][3] for call in sent)
    bridge = registry.get("agy", session_id)["bridge"]
    assert bridge["native_pid"] == fake.pid
    assert b"fixture-token" not in (tmp_path / "state/runtime.sqlite3").read_bytes()
    fake.terminate()
    fake.wait(timeout=5)
    # The bridge exits with its agy process.
    wait_for(lambda: not process_alive(bridge))


def test_a_failed_send_is_uncertain_and_records_no_token(tmp_path, agy_session):
    session_id, _record, start = agy_session
    (tmp_path / "agentapi.jsonl.fail").touch()
    start()
    monitors = Monitors(tmp_path / "state")
    events = wait_for(
        lambda: (
            (found := monitors.events(f"agy:{session_id}"))
            and all(event["status"] == "uncertain" for event in found)
            and found
        )
    )
    assert all(event["error"] == "agentapi exited with status 1" for event in events)
    assert b"fixture-token" not in (tmp_path / "state/runtime.sqlite3").read_bytes()


def test_a_command_of_an_unregistered_agy_process_starts_no_bridge(agy_session):
    _session_id, record, start = agy_session
    _fake, result = start(hook=False)
    assert "has not registered agent-runtime hooks" in result["error"]
    assert not calls(record)


def test_a_hook_outside_its_agy_process_does_not_register(tmp_path, monkeypatch):
    session_id = str(uuid.uuid4())
    monkeypatch.setenv("HOME", str(tmp_path / "home"))
    monkeypatch.setenv("AGENT_RUNTIME_DIRECTORY", str(tmp_path / "state"))
    payload = {
        "conversationId": session_id,
        "transcriptPath": str(tmp_path / "t.jsonl"),
        "cwd": str(tmp_path),
        "hookEventName": "SessionStart",
    }

    with pytest.raises(RuntimeError, match="no ancestor"):
        app.handle("agy", payload)

    with pytest.raises(KeyError):
        Registry(tmp_path / "state").get("agy", session_id)


def test_an_agy_conversation_inside_another_session_is_ambiguous(monkeypatch):
    monkeypatch.delenv("AGENT_SESSION_PROVIDER", raising=False)
    monkeypatch.delenv("CLAUDE_CODE_SESSION_ID", raising=False)
    monkeypatch.setenv("ANTIGRAVITY_CONVERSATION_ID", str(uuid.uuid4()))
    monkeypatch.setenv("CODEX_THREAD_ID", str(uuid.uuid4()))

    with pytest.raises(RuntimeError, match="ambiguous native identity"):
        current_identity()


def test_agent_runtime_cannot_send_to_an_agy_session():
    with pytest.raises(ValueError, match="cannot send to it"):
        operations.send("agy", str(uuid.uuid4()), "hello")
