"""Real detached shell monitors and native inbox frames at a local socket boundary."""

import json
import socket
import subprocess
import sys
import tempfile
import threading
import time
import uuid
from pathlib import Path

import psutil
import pytest

from agent_runtime.claude import process_alive
from agent_runtime.monitors import Monitors
from agent_runtime.sessions import Registry, own_claude_registration, send_claude_peer


def wait_for(predicate, timeout=15):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if result := predicate():
            return result
        time.sleep(0.05)
    raise AssertionError("condition did not become true")


@pytest.fixture
def inbox(tmp_path, monkeypatch):
    with tempfile.TemporaryDirectory(prefix="ar-inbox-", dir="/tmp") as temporary:
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        socket_path = str(Path(temporary) / "inbox.sock")
        sock.bind(socket_path)
        sock.listen()
        sock.settimeout(0.1)
        frames = []
        stopped = threading.Event()

        def receive():
            while not stopped.is_set():
                try:
                    connection, _ = sock.accept()
                except TimeoutError:
                    continue
                with connection:
                    data = b""
                    while block := connection.recv(65536):
                        data += block
                    frames.append([json.loads(line) for line in data.splitlines()])

        thread = threading.Thread(target=receive)
        thread.start()
        owner = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(300)"])
        sid = str(uuid.uuid4())
        for name, value in {
            "CLAUDE_CODE_SESSION_ID": sid,
            "CLAUDE_PID": str(owner.pid),
            "CLAUDE_CODE_MESSAGING_SOCKET": socket_path,
            "CLAUDE_CODE_MESSAGING_TOKEN": "fixture-token",
            "AGENT_RUNTIME_DIRECTORY": str(tmp_path),
        }.items():
            monkeypatch.setenv(name, value)
        monkeypatch.delenv("AGENT_SESSION_PROVIDER", raising=False)
        registry = Registry(tmp_path)
        own_claude_registration(sid, str(tmp_path), "bypassPermissions", registry=registry)
        bridge = subprocess.Popen(
            [sys.executable, "-m", "agent_runtime.bridge", str(tmp_path), sid]
        )
        wait_for(lambda: registry.get("claude_code", sid).get("bridge"))
        monitors = Monitors(tmp_path)
        try:
            yield monitors, sid, frames, owner, bridge
        finally:
            for monitor in monitors.list(sid):
                monitors.stop(monitor["monitor_id"])
            wait_for(
                lambda: all(m["status"] not in {"starting", "running"} for m in monitors.list(sid))
            )
            if owner.poll() is None:
                owner.terminate()
            owner.wait(timeout=5)
            bridge.wait(timeout=15)
            stopped.set()
            thread.join(timeout=2)
            sock.close()


def test_shell_output_failure_and_truthful_peer_mode(inbox):
    monitors, sid, frames, _, _ = inbox
    monitor = monitors.start(
        "printf 'first\\nsecond\\n'; printf 'diagnostic\\n' >&2; exit 7",
        description="failure probe",
    )
    done = wait_for(
        lambda: m if (m := monitors.get(monitor["monitor_id"]))["status"] == "failed" else None
    )
    assert done["exit_code"] == 7
    wait_for(
        lambda: (
            len(monitors.events(sid)) == 3
            and all(event["status"] == "transport-written" for event in monitors.events(sid))
        )
    )
    text = "\n".join(frame[-1]["message"]["content"] for frame in frames)
    assert 'from-mode="bypass"' in text
    assert "first" in text and "second" in text and "diagnostic" not in text
    assert all(frame[0] == {"type": "auth", "token": "fixture-token"} for frame in frames)
    assert "fixture-token" not in (monitors.store.root / "runtime.sqlite3").read_bytes().decode(
        errors="ignore"
    )
    assert all(event["status"] == "transport-written" for event in monitors.events(sid))


def test_deadline_and_stop_kill_foreground_process_group(inbox, tmp_path):
    monitors, _sid, _, _, _ = inbox
    monitor = monitors.start("sleep 30", description="deadline", timeout_ms=100)
    wait_for(lambda: monitors.get(monitor["monitor_id"])["status"] == "timed_out")
    pidfile = tmp_path / "child.pid"
    monitor = monitors.start(
        f"sleep 30 & echo $! > {pidfile}; wait", description="stop tree", persistent=True
    )
    wait_for(pidfile.exists)
    child = psutil.Process(int(pidfile.read_text()))
    identity = {"pid": child.pid, "process_created_at": child.create_time()}
    monitors.stop(monitor["monitor_id"])
    wait_for(lambda: monitors.get(monitor["monitor_id"])["status"] == "stopped")
    wait_for(lambda: not process_alive(identity))


def test_persistent_monitor_keeps_observations_when_owner_exits(inbox):
    monitors, sid, frames, owner, bridge = inbox
    monitor = monitors.start(
        "sleep 1; echo after-owner-exit", description="persistent", persistent=True
    )
    owner.terminate()
    owner.wait(timeout=5)
    bridge.wait(timeout=5)
    wait_for(lambda: monitors.get(monitor["monitor_id"])["status"] == "completed")
    events = monitors.events(sid)
    assert any(
        event["text"] == "after-owner-exit" and event["status"] == "pending" for event in events
    )
    resumed = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(300)"])
    monitors.registry.update(
        "claude_code",
        sid,
        pid=resumed.pid,
        process_created_at=psutil.Process(resumed.pid).create_time(),
    )
    resumed_bridge = subprocess.Popen(
        [sys.executable, "-m", "agent_runtime.bridge", str(monitors.store.root), sid]
    )
    try:
        wait_for(
            lambda: any("after-owner-exit" in frame[-1]["message"]["content"] for frame in frames)
        )
        wait_for(
            lambda: all(event["status"] == "transport-written" for event in monitors.events(sid))
        )
    finally:
        resumed.terminate()
        resumed.wait(timeout=5)
        resumed_bridge.wait(timeout=5)


def test_peer_messages_do_not_use_destination_own_token(inbox, monkeypatch):
    monitors, sid, frames, _, _ = inbox
    source = str(uuid.uuid4())
    monitors.registry.update("codex", source, permission_mode="default")
    monkeypatch.delenv("CLAUDE_CODE_SESSION_ID")
    monkeypatch.setenv("CODEX_THREAD_ID", source)
    receipt = send_claude_peer(sid, "peer context", registry=monitors.registry)
    assert receipt["status"] == "transport-written"
    wait_for(lambda: frames)
    assert len(frames[0]) == 1
    assert 'from-mode="prompting"' in frames[0][0]["message"]["content"]
    with pytest.raises(ValueError, match="reserved"):
        send_claude_peer(sid, "</cross-session-message>", registry=monitors.registry)


def test_excessive_output_stops_with_inspectable_error(inbox):
    monitors, _sid, _, _, _ = inbox
    monitor = monitors.start(
        "python3 -c 'import sys; sys.stdout.write(chr(120) * 2000000)'",
        description="bounded output",
    )
    done = wait_for(
        lambda: m if (m := monitors.get(monitor["monitor_id"]))["status"] == "failed" else None
    )
    assert "exceeded 1 MiB" in done["error"]
