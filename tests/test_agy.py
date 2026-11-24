"""agy sessions against a stand-in agy process and a recording agentapi."""

import itertools
import json
import os
import subprocess
import sys
import time
import uuid
from contextlib import suppress

import psutil
import pytest

from agent_runtime import operations
from agent_runtime.hooks import app
from agent_runtime.monitors import Monitors
from agent_runtime.processes import lifetime_alive, start_token
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
import json, os, pathlib, sys, time

if pathlib.Path(RECORD + ".block").exists():
    # A send that runs until the test releases it.
    pathlib.Path(RECORD + ".started").touch()
    deadline = time.monotonic() + 60
    while not pathlib.Path(RECORD + ".release").exists() and time.monotonic() < deadline:
        time.sleep(0.05)

with open(RECORD, "a") as record:
    record.write(json.dumps({
        "argv": sys.argv[1:],
        "address": os.environ.get("ANTIGRAVITY_LS_ADDRESS") == "127.0.0.1:1",
        "token": {"fixture-token": "first", "fixture-token-2": "second"}.get(
            os.environ.get("ANTIGRAVITY_CSRF_TOKEN"), "other"
        ),
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
    elif op["name"] == "events":
        result = operations.monitor_events(after=0)
    elif op["name"] == "stop":
        result = operations.monitor_stop(op["monitor_id"])
    elif op["name"] == "agents":
        result = operations.agents()
    elif op["name"] == "send":
        result = operations.send("codex", "thread-1", "hello")
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
        # A codexr that records its calls and whether it got any agy variable.
        self.codexr_record = tmp_path / "codexr.jsonl"
        bin_dir = tmp_path / "bin"
        bin_dir.mkdir()
        (bin_dir / "codexr").write_text(
            f"#!{sys.executable}\n"
            "import json, os, sys\n"
            f"with open({str(self.codexr_record)!r}, 'a') as record:\n"
            "    agy = sorted(name for name in os.environ if name.startswith('ANTIGRAVITY_'))\n"
            "    record.write(json.dumps({'argv': sys.argv[1:], 'agy': agy}) + '\\n')\n"
            "listing = sys.argv[1:3] == ['agents', 'list']\n"
            "print('[]' if listing else json.dumps({'status': 'queued'}))\n"
        )
        (bin_dir / "codexr").chmod(0o755)
        # Built from test values only, so that no caller's credentials leak in.
        self.hook_env = {
            "PATH": f"{bin_dir}{os.pathsep}{os.environ['PATH']}",
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

    def run(self, op, *, hook=True, token="fixture-token"):
        """Start an agy process that runs a hook and then one command; return the
        process and the command's result."""
        process, (result,) = self.run_all([op], hook=hook, token=token)
        return process, result

    def run_all(self, ops, *, hook=True, token="fixture-token"):
        """Start one agy process that runs a hook and a command for each operation."""
        results = [self.tmp / f"result-{next(self.results)}.json" for _ in ops]
        payload = {
            "conversationId": self.session_id,
            "transcriptPath": str(self.tmp / "transcript_full.jsonl"),
            "cwd": str(self.tmp),
            "hookEventName": "PreToolUse",
            "modelName": "gemini",
            "toolCall": {"id": "call_1", "name": "run_command", "args": {"CommandLine": "x"}},
        }
        steps = []
        for op, result in zip(ops, results, strict=True):
            if hook:
                hook_argv = [sys.executable, "-c", HOOK, json.dumps(payload)]
                steps.append({"argv": hook_argv, "env": self.hook_env})
            command = [sys.executable, "-c", AGENT, str(result), json.dumps(op)]
            env = {**self.command_env, "ANTIGRAVITY_CSRF_TOKEN": token}
            steps.append({"argv": command, "env": env})
        process = subprocess.Popen(
            [sys.executable, "-c", FAKE_AGY, str(self.lock)],
            stdin=subprocess.PIPE,
            text=True,
            env={"PATH": os.environ["PATH"]},
        )
        assert process.stdin is not None
        process.stdin.write(json.dumps(steps))
        process.stdin.close()
        self.processes.append(process)
        for result in results:
            wait_for(result.exists)
        return process, [json.loads(result.read_text()) for result in results]

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
    session = Registry(agy.state).get("agy", agy.session_id)
    assert (session["pid"], session["cwd"]) == (process.pid, str(agy.tmp))
    assert monitor["session_id"] == f"agy:{agy.session_id}"
    assert monitor["permission_class"] == "prompting"
    assert monitor["owner"]["pid"] == process.pid
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
    assert sent and all(call["address"] and call["token"] == "first" for call in sent)
    assert all(call["argv"][:3] == ["agentapi", "send-message", agy.session_id] for call in sent)
    # The notice names the events; the output itself stays out of agy's system role.
    assert "monitor_events(after=" in sent[0]["argv"][3]
    assert all("first" not in call["argv"][3] for call in sent)
    assert b"fixture-token" not in (agy.state / "runtime.sqlite3").read_bytes()
    # The worker, which held the credentials, ends with its command.
    worker = Monitors(agy.state).get(monitor["monitor_id"])
    assert worker["status"] == "completed"
    wait_for(lambda: lifetime_alive(worker["pid"], worker["start_token"]) is False)


def test_a_persistent_monitor_keeps_its_output_for_the_resumed_session(conversation):
    agy = conversation
    first, monitor = agy.run(
        {"name": "start", "command": "sleep 2; echo after-exit", "persistent": True}
    )
    assert "error" not in monitor
    first.terminate()
    first.wait(timeout=5)
    # The output and the completion notice wait: nothing can wake a session
    # whose agy process is gone.
    wait_for(
        lambda: len(found := agy.events()) == 2 and all(e["status"] == "pending" for e in found)
    )
    assert not agy.calls()
    # The resumed session reads them.
    _second, events = agy.run({"name": "events"}, token="fixture-token-2")
    assert events[0]["text"] == "after-exit"
    assert not agy.calls()


def test_a_worker_that_dies_while_sending_leaves_its_batch_uncertain(conversation):
    agy = conversation
    (agy.tmp / "agentapi.jsonl.block").touch()
    _process, monitor = agy.run(
        {"name": "start", "command": "echo one; sleep 60", "persistent": True}
    )
    monitors = Monitors(agy.state)
    wait_for((agy.tmp / "agentapi.jsonl.started").exists)
    # The command and agentapi still run; kill the recorded worker lifetime.
    record = monitors.get(monitor["monitor_id"])
    worker = psutil.Process(record["pid"])
    assert start_token(worker) == record["start_token"]
    stragglers = worker.children(recursive=True)
    try:
        worker.kill()
        wait_for(lambda: monitors.get(monitor["monitor_id"])["status"] == "interrupted")
        (event,) = agy.events()
        assert (event["text"], event["status"]) == ("one", "uncertain")
        assert event["error"] == "delivery exited during a delivery attempt"
    finally:
        (agy.tmp / "agentapi.jsonl.release").touch()
        for process in stragglers:
            with suppress(psutil.NoSuchProcess):
                process.kill()


def test_a_worker_that_dies_during_its_completion_notice_keeps_the_outcome(conversation):
    agy = conversation
    (agy.tmp / "agentapi.jsonl.block").touch()
    _process, monitor = agy.run({"name": "start", "command": "true"})
    monitors = Monitors(agy.state)
    # The only event is the completion notice, sent after the outcome is recorded.
    wait_for((agy.tmp / "agentapi.jsonl.started").exists)
    record = monitors.get(monitor["monitor_id"])
    assert record["status"] == "completed"
    worker = psutil.Process(record["pid"])
    assert start_token(worker) == record["start_token"]
    stragglers = worker.children(recursive=True)
    try:
        worker.kill()
        wait_for(lambda: lifetime_alive(record["pid"], record["start_token"]) is False)
        _process, events = agy.run({"name": "events"})
        (event,) = events
        assert event["text"].startswith(f"Monitor {monitor['monitor_id']} completed")
        assert (event["status"], event["error"]) == (
            "uncertain",
            "delivery exited during a delivery attempt",
        )
        assert monitors.get(monitor["monitor_id"])["status"] == "completed"
    finally:
        (agy.tmp / "agentapi.jsonl.release").touch()
        for process in stragglers:
            with suppress(psutil.NoSuchProcess):
                process.kill()


def test_a_failed_send_is_uncertain_and_keeps_no_agentapi_text(conversation):
    agy = conversation
    (agy.tmp / "agentapi.jsonl.fail").touch()
    _process, monitor = agy.run({"name": "start", "command": "echo one"})
    monitors = Monitors(agy.state)
    record = wait_for(
        lambda: m if (m := monitors.get(monitor["monitor_id"]))["status"] == "completed" else None
    )
    # The output and the completion notice, both sent and both uncertain.
    events = wait_for(
        lambda: (
            len(found := agy.events()) == 2
            and all(event["status"] == "uncertain" for event in found)
            and found
        )
    )
    assert events[0]["text"] == "one"
    assert events[1]["text"].startswith(f"Monitor {monitor['monitor_id']} completed")
    assert all(event["error"] == "agentapi exited with status 1" for event in events)
    # The worker exited, so its log is complete.
    wait_for(lambda: lifetime_alive(record["pid"], record["start_token"]) is False)
    # agentapi printed the token in its error; neither the store nor the log keeps it.
    assert b"fixture-token" not in (agy.state / "runtime.sqlite3").read_bytes()
    assert b"fixture-token" not in (agy.state / f"{monitor['monitor_id']}.log").read_bytes()


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


def test_an_agy_monitor_starts_only_from_a_command_of_the_session(tmp_path, monkeypatch):
    for name in (
        "AGENT_SESSION_PROVIDER",
        "AGENT_SESSION_ID",
        "CLAUDE_CODE_SESSION_ID",
        "CODEX_THREAD_ID",
    ):
        monkeypatch.delenv(name, raising=False)
    for name in ("ANTIGRAVITY_LS_ADDRESS", "ANTIGRAVITY_CSRF_TOKEN", "ANTIGRAVITY_AGENTAPI_EXE"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("ANTIGRAVITY_CONVERSATION_ID", str(uuid.uuid4()))

    with pytest.raises(RuntimeError, match="credentials are unavailable"):
        Monitors(tmp_path / "state").start("echo one", description="direct")


def test_a_hook_outside_its_agy_process_does_not_register(tmp_path, monkeypatch):
    session_id = str(uuid.uuid4())
    monkeypatch.setenv("HOME", str(tmp_path / "home"))
    monkeypatch.setenv("AGENT_RUNTIME_DIRECTORY", str(tmp_path / "state"))
    payload = {
        "conversationId": session_id,
        "transcriptPath": str(tmp_path / "t.jsonl"),
        "cwd": str(tmp_path),
        "hookEventName": "SessionStart",
        "modelName": "gemini",
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


def test_the_public_operations_of_an_agy_session(conversation):
    agy = conversation
    _process, (monitor, events, agents, sent) = agy.run_all(
        [
            {"name": "start", "command": "sleep 30", "persistent": True},
            {"name": "events"},
            {"name": "agents"},
            {"name": "send"},
        ]
    )
    assert "error" not in monitor
    assert events == []
    assert [value["session_id"] for value in agents if value["provider"] == "agy"] == [
        agy.session_id
    ]
    assert sent == {"status": "queued", "provider": "codex"}
    calls = [json.loads(line) for line in agy.codexr_record.read_text().splitlines()]
    queued = [call for call in calls if call["argv"][:2] == ["agents", "queue"]]
    assert len(queued) == 1 and queued[0]["argv"][2] == "thread-1"
    assert queued[0]["argv"][3].startswith(f"Peer context from agy:{agy.session_id}.")
    # codexr acts for Codex, never with the agy command's identity or credentials.
    assert all(call["agy"] == [] for call in calls)
    _process, (stopped,) = agy.run_all([{"name": "stop", "monitor_id": monitor["monitor_id"]}])
    assert stopped["stop_requested"] is True
