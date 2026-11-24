"""agy sessions against a stand-in agy process and a recording agentapi."""

import asyncio
import errno
import itertools
import json
import os
import shlex
import signal
import subprocess
import sys
import threading
import time
import uuid
from contextlib import suppress

import psutil
import pytest

from agent_runtime import agy as agy_module
from agent_runtime import monitor_worker, operations, processes
from agent_runtime import monitors as monitors_module
from agent_runtime.hooks import app
from agent_runtime.monitors import Monitors
from agent_runtime.processes import holds, lifetime_alive, start_token
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

# The names of the agy variables that the monitored command received, never values.
PROBE = 'printf "first\\n"; env | sed -n "s/^\\(ANTIGRAVITY_[A-Z_]*\\)=.*/\\1/p"; printf "end\\n"'


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
            "ANTIGRAVITY_TRAJECTORY_ID": "trajectory-1",
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
            len(found := agy.events()) == 3
            and all(event["status"] == "transport-written" for event in found)
            and found
        )
    )
    # The monitored command received no agy variable: between its two lines,
    # it printed the name of each one that it had.
    texts = [event["text"] for event in events[:2]]
    assert texts == ["first", "end"]
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
    gate = agy.tmp / "gate"
    command = f"while [ ! -e {shlex.quote(str(gate))} ]; do sleep 0.05; done; echo after-exit"
    try:
        first, monitor = agy.run({"name": "start", "command": command, "persistent": True})
        assert "error" not in monitor
        first.terminate()
        first.wait(timeout=5)
    finally:
        # The output comes only after the agy process is gone; a failure ends the command too.
        gate.touch()
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
    # The agy worker module, which agent-runtime 0.2 does not have.
    assert "agent_runtime.agy_monitor_worker" in worker.cmdline()
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


@pytest.mark.parametrize("when", ["before", "during"])
def test_an_old_process_does_not_hold_a_replaced_presence_lock(tmp_path, monkeypatch, when):
    lock = tmp_path / "conversation.lock"
    lock.touch()

    def replace():
        lock.unlink()
        lock.touch()

    with lock.open():
        assert holds(os.getpid(), lock)
        if when == "before":
            replace()
            assert not holds(os.getpid(), lock)
        else:
            # Replaced while the descriptors are read: the answer is unknown.
            listing = processes._descriptors

            def replacing(pid):
                found = listing(pid)
                replace()
                return found

            monkeypatch.setattr(processes, "_descriptors", replacing)
            with pytest.raises(OSError, match="changed during the scan"):
                holds(os.getpid(), lock)


def test_a_lock_renamed_over_during_the_scan_is_unknown(tmp_path, monkeypatch):
    # agy opens a new lock, renames it over the name and closes the old one,
    # while the descriptors are read: no descriptor matches, and yet it holds.
    lock = tmp_path / "conversation.lock"
    lock.touch()
    old = lock.open()
    new_name = tmp_path / "new.lock"
    new_name.touch()
    new = new_name.open()
    listing = processes._descriptors

    def renaming(pid):
        found = listing(pid)
        os.replace(new_name, lock)
        old.close()
        return found

    monkeypatch.setattr(processes, "_descriptors", renaming)
    try:
        with pytest.raises(OSError, match="changed during the scan"):
            holds(os.getpid(), lock)
    finally:
        old.close()
        new.close()


def test_a_zombie_is_gone_without_a_boot_id(monkeypatch):
    child = subprocess.Popen([sys.executable, "-c", "pass"])
    try:
        process = psutil.Process(child.pid)
        wait_for(lambda: process.status() == psutil.STATUS_ZOMBIE)

        def unreadable():
            raise OSError(errno.EPERM, "operation not permitted")

        monkeypatch.setattr(processes, "boot_id", unreadable)
        assert processes.lifetime_alive(child.pid, "any token") is False
    finally:
        child.wait()


def test_an_unreadable_presence_lock_leaves_the_owner_unknown(tmp_path, monkeypatch):
    monkeypatch.setenv("HOME", str(tmp_path))
    lock = processes.presence_lock("c1")
    lock.parent.mkdir(parents=True)
    lock.touch()
    token = start_token(psutil.Process())
    with lock.open():
        assert processes.agy_alive(os.getpid(), token, "c1") is True

        def unreadable(_path):
            raise OSError(errno.EIO, "input/output error")

        monkeypatch.setattr(processes, "_named_file", unreadable)
        assert processes.agy_alive(os.getpid(), token, "c1") is None


def test_a_monitor_is_not_given_up_after_a_step_of_the_calendar_clock(tmp_path):
    monitors = Monitors(tmp_path)
    record = {
        "monitor_id": "mstep",
        "session_id": "agy:c1",
        "status": "starting",
        # The calendar clock stepped 90 s forward right after the start.
        "created_at": time.time() - 90,
        "started": {"boot": processes.boot_id(), "monotonic": time.monotonic()},
        "owner": {"pid": 1, "start_token": "t", "conversation": "c1"},
    }
    with monitors.store.connect() as db:
        db.execute("INSERT INTO monitors VALUES(?,?,?)", ("mstep", "agy:c1", json.dumps(record)))
    assert monitors.get("mstep")["status"] == "starting"
    begun = monitors.begin("mstep", pid=os.getpid(), process_created_at=1.0, start_token="w")
    assert begun is not None and begun["status"] == "running"


def test_an_agy_worker_without_a_start_token_fails_before_it_runs(tmp_path, monkeypatch):
    monitors = Monitors(tmp_path)
    record = {
        "monitor_id": "magy",
        "session_id": "agy:c1",
        "status": "starting",
        "created_at": time.time(),
        "owner": {"pid": 1, "start_token": "t", "conversation": "c1"},
    }
    with monitors.store.connect() as db:
        db.execute("INSERT INTO monitors VALUES(?,?,?)", ("magy", "agy:c1", json.dumps(record)))

    def denied(_process):
        raise psutil.AccessDenied(os.getpid())

    monkeypatch.setattr(monitor_worker, "start_token", denied)

    # run() sets the process's SIGCHLD disposition; the test runner keeps its own.
    disposition = signal.getsignal(signal.SIGCHLD)
    try:
        asyncio.run(monitor_worker.run(tmp_path, "magy"))
    finally:
        signal.signal(signal.SIGCHLD, disposition)

    failed = monitors.get("magy")
    reason = "the monitor worker cannot read its own process identity"
    assert (failed["status"], failed["error"]) == ("failed", reason)
    (event,) = monitors.events("agy:c1")
    assert event["text"] == f"Monitor magy failed: {reason}"


def test_a_lock_released_before_the_answer_is_not_held(tmp_path, monkeypatch):
    lock = tmp_path / "conversation.lock"
    lock.touch()
    held = lock.open()
    named = processes._named_file
    calls = []

    def releasing(path):
        # agy releases the lock right after its descriptor matched; the name stays.
        calls.append(path)
        if len(calls) == 2:
            held.close()
        return named(path)

    monkeypatch.setattr(processes, "_named_file", releasing)
    try:
        assert not holds(os.getpid(), lock)
    finally:
        held.close()


def test_a_monitor_stopped_before_its_worker_started_never_runs(tmp_path):
    monitors = Monitors(tmp_path)
    record = {
        "monitor_id": "mstop",
        "session_id": "agy:c1",
        "status": "starting",
        "stop_requested": True,
        "created_at": time.time(),
        "started": {"boot": None, "monotonic": time.monotonic()},
        "owner": {"pid": 1, "start_token": "t", "conversation": "c1"},
    }
    with monitors.store.connect() as db:
        db.execute("INSERT INTO monitors VALUES(?,?,?)", ("mstop", "agy:c1", json.dumps(record)))

    begun = monitors.begin("mstop", pid=os.getpid(), process_created_at=1.0, start_token="w")

    # The worker gets the record, to deliver the stop notice, and runs no command.
    assert begun is not None and begun["status"] == "stopped"
    assert monitors.get("mstop")["status"] == "stopped"
    (event,) = monitors.events("agy:c1")
    assert event["text"] == "Monitor mstop stopped before its command started."


@pytest.mark.parametrize("reader_first", [True, False])
def test_a_stop_before_an_expired_start_ends_the_same_either_way(tmp_path, reader_first):
    monitors = Monitors(tmp_path)
    record = {
        "monitor_id": "mlate",
        "session_id": "agy:c1",
        "status": "starting",
        "stop_requested": True,
        "created_at": time.time() - 61,
        "started": {"boot": None, "monotonic": time.monotonic() - 61},
        "owner": {"pid": 1, "start_token": "t", "conversation": "c1"},
    }
    with monitors.store.connect() as db:
        db.execute("INSERT INTO monitors VALUES(?,?,?)", ("mlate", "agy:c1", json.dumps(record)))
    lifetime = {"pid": os.getpid(), "process_created_at": 1.0, "start_token": "w"}
    if reader_first:
        monitors.get("mlate")
        monitors.begin("mlate", **lifetime)
    else:
        monitors.begin("mlate", **lifetime)
        monitors.get("mlate")
    assert monitors.get("mlate")["status"] == "stopped"
    assert [event["text"] for event in monitors.events("agy:c1")] == [
        "Monitor mlate stopped before its command started."
    ]


def test_a_worker_stopped_before_its_start_only_delivers_the_notice(tmp_path, monkeypatch):
    monitors = Monitors(tmp_path)
    record = {
        "monitor_id": "mquiet",
        "session_id": "agy:c1",
        "command": "touch should-not-exist",
        "cwd": str(tmp_path),
        "persistent": False,
        "timeout_ms": 60_000,
        "status": "starting",
        "stop_requested": True,
        "created_at": time.time(),
        "started": {"boot": None, "monotonic": time.monotonic()},
        "owner": {"pid": 1, "start_token": "t", "conversation": "c1"},
    }
    with monitors.store.connect() as db:
        db.execute("INSERT INTO monitors VALUES(?,?,?)", ("mquiet", "agy:c1", json.dumps(record)))
    delivered = []

    async def deliver(_monitors, identity, _owner, finish):
        delivered.append((identity, finish.final))

    monkeypatch.setattr(monitor_worker, "_deliver", deliver)
    disposition = signal.getsignal(signal.SIGCHLD)
    try:
        asyncio.run(monitor_worker.run(tmp_path, "mquiet"))
    finally:
        signal.signal(signal.SIGCHLD, disposition)
    assert delivered == [("mquiet", True)]
    assert not (tmp_path / "should-not-exist").exists()
    assert monitors.get("mquiet")["status"] == "stopped"


def test_a_reader_without_the_boot_id_keeps_the_monotonic_grace(tmp_path, monkeypatch):
    monitors = Monitors(tmp_path)
    record = {
        "monitor_id": "mboot",
        "session_id": "agy:c1",
        "status": "starting",
        "created_at": time.time() - 90,  # a step of the calendar clock
        "started": {"boot": "an-earlier-read", "monotonic": time.monotonic()},
        "owner": {"pid": 1, "start_token": "t", "conversation": "c1"},
    }
    with monitors.store.connect() as db:
        db.execute("INSERT INTO monitors VALUES(?,?,?)", ("mboot", "agy:c1", json.dumps(record)))

    def unreadable():
        raise OSError(errno.EPERM, "operation not permitted")

    monkeypatch.setattr(monitors_module, "boot_id", unreadable)
    assert monitors.get("mboot")["status"] == "starting"


def _agy_records(registry, count):
    with registry.store.connect() as db:
        for index in range(count):
            value = {
                "provider": "agy",
                "session_id": f"c{index}",
                "pid": index,
                "native_start": "t",
            }
            db.execute("INSERT INTO sessions VALUES('agy',?,?)", (f"c{index}", json.dumps(value)))


def test_discovery_inspects_every_session_within_its_budget(tmp_path, monkeypatch):
    registry = Registry(tmp_path)
    _agy_records(registry, 40)

    def slow(_pid, _token, _session_id):
        time.sleep(0.1)
        return True

    monkeypatch.setattr(agy_module, "agy_alive", slow)
    found = agy_module.live_sessions(registry)
    assert sorted(record["session_id"] for record in found) == sorted(f"c{i}" for i in range(40))


def test_discovery_starts_no_inspection_after_its_deadline(tmp_path, monkeypatch):
    registry = Registry(tmp_path)
    _agy_records(registry, 5)
    calls = []

    def counted(_pid, _token, session_id):
        calls.append(session_id)
        return True

    monkeypatch.setattr(agy_module, "agy_alive", counted)
    assert agy_module.live_sessions(registry, timeout=0) == []
    assert calls == []


def test_an_owner_lookup_that_does_not_end_fails_monitor_start(monkeypatch):
    for name, value in {
        "ANTIGRAVITY_CONVERSATION_ID": "c1",
        "ANTIGRAVITY_LS_ADDRESS": "127.0.0.1:1",
        "ANTIGRAVITY_CSRF_TOKEN": "fixture-token",
        "ANTIGRAVITY_AGENTAPI_EXE": "/bin/false",
    }.items():
        monkeypatch.setenv(name, value)
    release = threading.Event()
    monkeypatch.setattr(agy_module, "native_process", lambda _session_id: release.wait(5))
    monkeypatch.setattr(agy_module, "OWNER_TIMEOUT", 0.1)
    try:
        with pytest.raises(RuntimeError, match=r"was not found in 0\.1 s"):
            agy_module.command_owner("c1")
    finally:
        release.set()


def test_a_thread_that_cannot_start_leaves_discovery_working(tmp_path, monkeypatch):
    registry = Registry(tmp_path)
    _agy_records(registry, 2)

    def refused(_thread):
        raise RuntimeError("can't start new thread")

    monkeypatch.setattr(threading.Thread, "start", refused)
    assert agy_module.live_sessions(registry, timeout=0.5) == []
    assert not agy_module._INSPECTIONS
