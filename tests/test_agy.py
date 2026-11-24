"""agy sessions against a stand-in agy process and a recording agentapi."""

import itertools
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
from agent_runtime.sessions import (
    AmbiguousIdentity,
    IdentityUnavailable,
    Registry,
    current_identity,
)

# The live agy process of a conversation keeps its presence lock open and runs
# hooks and the agent's commands as its children, each with its own environment.
# The steps come on stdin, so that no environment appears in a command line.
FAKE_AGY = """\
import json, subprocess, sys, time

lock = open(sys.argv[1], "a")
for step in json.load(sys.stdin):
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
    print(json.dumps({"error": "rejected fixture-token at 127.0.0.1:1"}))
    sys.exit(1)
print(json.dumps({"response": {"sendMessage": {"recipientId": sys.argv[3]}}}))
"""

HOOK = """\
import json, sys

from agent_runtime.hooks import app

app.handle("agy", json.loads(sys.argv[1]))
"""

# One operation of agent-runtime, as a command of the agent runs it. The result
# file appears only when it is complete.
AGENT = """\
import json, os, sys

from agent_runtime import operations

op = json.loads(sys.argv[2])
try:
    if op["name"] == "start":
        result = operations.monitor_start(
            op["command"], description="probe", persistent=op.get("persistent", False)
        )
    elif op["name"] == "list":
        result = operations.monitor_list()
    else:
        result = operations.monitor_get(op["monitor_id"])
except Exception as error:
    result = {"error": str(error)}
with open(sys.argv[1] + ".partial", "w") as output:
    json.dump(result, output)
os.replace(sys.argv[1] + ".partial", sys.argv[1])
"""

PROBE = (
    'printf "first\\n"; printf "%s\\n" "${ANTIGRAVITY_CSRF_TOKEN:-no-token}" '
    '"${ANTIGRAVITY_LS_ADDRESS:-no-address}" "${ANTIGRAVITY_CONVERSATION_ID:-no-conversation}"'
)


def wait_for(predicate, timeout=15):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if result := predicate():
            return result
        time.sleep(0.05)
    raise AssertionError("condition did not become true")


class Agy:
    """A conversation whose stand-in agy processes run hooks and commands."""

    def __init__(self, tmp_path):
        self.tmp = tmp_path
        self.session_id = str(uuid.uuid4())
        self.lock = tmp_path / "home/.gemini/antigravity-cli/presence" / f"{self.session_id}.lock"
        self.lock.parent.mkdir(parents=True)
        self.lock.touch()
        self.record = tmp_path / "agentapi.jsonl"
        agentapi = tmp_path / "agentapi-exe"
        agentapi.write_text(f"#!{sys.executable}\nRECORD = {str(self.record)!r}\n{AGENTAPI}")
        agentapi.chmod(0o755)
        # Built from test values only, so that no caller's credentials leak in.
        self.hook_env = {
            "PATH": os.environ["PATH"],
            "HOME": str(tmp_path / "home"),
            "AGENT_RUNTIME_DIRECTORY": str(tmp_path / "state"),
            "ANTIGRAVITY_CONVERSATION_ID": self.session_id,
        }
        self.command_env = {
            **self.hook_env,
            "ANTIGRAVITY_AGENT": "1",
            "ANTIGRAVITY_LS_ADDRESS": "127.0.0.1:1",
            "ANTIGRAVITY_CSRF_TOKEN": "fixture-token",
            "ANTIGRAVITY_AGENTAPI_EXE": str(agentapi),
        }
        self.processes = []
        self.results = itertools.count()
        self.state = tmp_path / "state"

    def run(self, op, *, hook=True):
        """Start an agy process that runs a hook and then one command; return the
        process and the command's result."""
        result = self.tmp / f"result-{next(self.results)}.json"
        payload = {
            "conversationId": self.session_id,
            "transcriptPath": str(self.tmp / "transcript_full.jsonl"),
            "cwd": str(self.tmp),
            "hookEventName": "PreToolUse",
            "toolCall": {"id": "call_1", "name": "run_command", "args": {"CommandLine": "x"}},
        }
        steps = [{"argv": [sys.executable, "-c", HOOK, json.dumps(payload)], "env": self.hook_env}]
        steps = steps if hook else []
        command = [sys.executable, "-c", AGENT, str(result), json.dumps(op)]
        steps.append({"argv": command, "env": self.command_env})
        process = subprocess.Popen(
            [sys.executable, "-c", FAKE_AGY, str(self.lock)], stdin=subprocess.PIPE, text=True
        )
        process.stdin.write(json.dumps(steps))
        process.stdin.close()
        self.processes.append(process)
        wait_for(result.exists)
        return process, json.loads(result.read_text())

    def calls(self):
        if not self.record.exists():
            return []
        return [json.loads(line) for line in self.record.read_text().splitlines()]

    def events(self):
        return Monitors(self.state).events(f"agy:{self.session_id}")


@pytest.fixture
def conversation(tmp_path):
    agy = Agy(tmp_path)
    yield agy
    for process in agy.processes:
        process.terminate()
        process.wait(timeout=5)


def test_a_command_wakes_its_own_session_and_keeps_the_token_out(conversation):
    agy = conversation
    process, monitor = agy.run({"name": "start", "command": PROBE})
    registry = Registry(agy.state)
    session = registry.get("agy", agy.session_id)
    assert (session["pid"], session["cwd"]) == (process.pid, str(agy.tmp))
    assert monitor["session_id"] == f"agy:{agy.session_id}"
    assert monitor["permission_class"] == "prompting"
    # The output lines and the completion notice, each delivered.
    events = wait_for(
        lambda: (
            len(found := agy.events()) == 5
            and all(event["status"] == "transport-written" for event in found)
            and found
        )
    )
    # The monitored command has neither the credentials nor the conversation.
    texts = [event["text"] for event in events[:4]]
    assert texts == ["first", "no-token", "no-address", "no-conversation"]
    sent = agy.calls()
    assert sent and all(call["address"] and call["token"] for call in sent)
    assert all(call["argv"][:3] == ["agentapi", "send-message", agy.session_id] for call in sent)
    # The notice names the events; the output itself stays out of agy's system role.
    assert "monitor_events(after=" in sent[0]["argv"][3]
    assert all("first" not in call["argv"][3] for call in sent)
    bridge = registry.get("agy", agy.session_id)["bridge"]
    assert bridge["native_pid"] == process.pid
    assert b"fixture-token" not in (agy.state / "runtime.sqlite3").read_bytes()
    process.terminate()
    process.wait(timeout=5)
    # The bridge exits with its agy process.
    wait_for(lambda: not process_alive(bridge))


def test_a_resumed_session_gets_the_output_of_a_persistent_monitor(conversation):
    agy = conversation
    first, monitor = agy.run(
        {"name": "start", "command": "sleep 2; echo after-resume", "persistent": True}
    )
    assert "error" not in monitor
    first.terminate()
    first.wait(timeout=5)
    wait_for(
        lambda: any(e["text"] == "after-resume" and e["status"] == "pending" for e in agy.events())
    )
    assert not agy.calls()
    # A new agy process of the same conversation runs any operation.
    second, listed = agy.run({"name": "list"})
    assert [value["monitor_id"] for value in listed] == [monitor["monitor_id"]]
    wait_for(lambda: all(event["status"] == "transport-written" for event in agy.events()))
    assert Registry(agy.state).get("agy", agy.session_id)["bridge"]["native_pid"] == second.pid


def test_a_failed_send_is_uncertain_and_keeps_no_agentapi_text(conversation):
    agy = conversation
    (agy.tmp / "agentapi.jsonl.fail").touch()
    agy.run({"name": "start", "command": "echo one"})
    events = wait_for(
        lambda: (
            (found := agy.events())
            and all(event["status"] == "uncertain" for event in found)
            and found
        )
    )
    assert all(event["error"] == "agentapi exited with status 1" for event in events)
    assert b"fixture-token" not in (agy.state / "runtime.sqlite3").read_bytes()


def test_a_command_of_an_unregistered_agy_process_starts_no_bridge(conversation):
    agy = conversation
    _process, result = agy.run({"name": "start", "command": "echo one"}, hook=False)
    assert "has not registered agent-runtime hooks" in result["error"]
    assert not agy.calls()


def test_an_agy_session_does_not_see_a_claude_monitor_of_the_same_id(conversation):
    agy = conversation
    monitors = Monitors(agy.state)
    # A Claude monitor whose worker is gone, under the same session ID.
    claude = {
        "monitor_id": "mclaude",
        "session_id": agy.session_id,
        "status": "running",
        "pid": 999_999,
        "process_created_at": 1.0,
        "created_at": time.time(),
    }
    with monitors.store.connect() as db:
        db.execute(
            "INSERT INTO monitors VALUES(?,?,?)", ("mclaude", agy.session_id, json.dumps(claude))
        )
    _process, listed = agy.run({"name": "list"})
    assert listed == []
    _process, result = agy.run({"name": "get", "monitor_id": "mclaude"})
    assert result == {"error": "monitor belongs to another session"}
    with monitors.store.connect() as db:
        row = db.execute("SELECT value FROM monitors WHERE id='mclaude'").fetchone()
    assert json.loads(row[0])["status"] == "running"


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


@pytest.mark.parametrize(
    "outer",
    [
        {"CODEX_THREAD_ID": "codex-thread"},
        {"CLAUDE_CODE_SESSION_ID": "claude-session"},
        # A managed Claude worker's identity.
        {"AGENT_SESSION_PROVIDER": "claude_code", "AGENT_SESSION_ID": "claude-session"},
    ],
)
def test_an_agy_conversation_inside_another_session_is_ambiguous(monkeypatch, outer):
    for name in (
        "AGENT_SESSION_PROVIDER",
        "AGENT_SESSION_ID",
        "CLAUDE_CODE_SESSION_ID",
        "CODEX_THREAD_ID",
    ):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("ANTIGRAVITY_CONVERSATION_ID", str(uuid.uuid4()))
    for name, value in outer.items():
        monkeypatch.setenv(name, value)

    with pytest.raises(AmbiguousIdentity, match="ambiguous native identity"):
        current_identity()


def test_no_identity_is_distinct_from_an_ambiguous_one(monkeypatch):
    for name in (
        "AGENT_SESSION_PROVIDER",
        "AGENT_SESSION_ID",
        "CLAUDE_CODE_SESSION_ID",
        "CODEX_THREAD_ID",
        "ANTIGRAVITY_CONVERSATION_ID",
    ):
        monkeypatch.delenv(name, raising=False)
    with pytest.raises(IdentityUnavailable):
        current_identity()


def test_agent_runtime_cannot_send_to_an_agy_session():
    with pytest.raises(ValueError, match="cannot send to it"):
        operations.send("agy", str(uuid.uuid4()), "hello")
