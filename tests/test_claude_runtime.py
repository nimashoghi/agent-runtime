"""Exercise the real SDK and detached processes against a deterministic native boundary."""

import asyncio
import json
import os
import sys
from pathlib import Path

import pytest

from agent_runtime import ClaudeRuntime, LaunchOptions


@pytest.fixture
def native(tmp_path: Path) -> Path:
    cli = tmp_path / "claude"
    cli.write_text(
        f"#!{sys.executable}\n"
        + r"""
import json, os, sys
if "--version" in sys.argv:
    print("2.1.281 (Claude Code)")
    raise SystemExit(0)
if os.environ.get("FAKE_CLAUDE_ARGV"):
    with open(os.environ["FAKE_CLAUDE_ARGV"], "w") as log:
        json.dump(sys.argv, log)
if os.environ.get("FAKE_CLAUDE_FAILS"):
    for number in range(30):
        print(f"line {number} " + "x" * 600, file=sys.stderr, flush=True)
    print("Invalid API key - Please run /login", file=sys.stderr, flush=True)
    raise SystemExit(1)
print("warning: a line at the start", file=sys.stderr, flush=True)
def emit(value):
    print(json.dumps(value), flush=True)
session = next(a.split("=", 1)[1] for a in sys.argv if a.startswith(("--session-id=", "--resume=")))
PEER = {"kind":"peer","from":"unknown"}
def result(text, origin={"kind":"human"}):
    emit({"type":"result", "subtype":"success", "is_error":False,
          "duration_ms":1,"duration_api_ms":1,"num_turns":1,"session_id":session,
          "result":text,"origin":origin,"usage":{}})
pending = None
for line in sys.stdin:
    message = json.loads(line)
    if message["type"] == "control_request":
        emit({"type":"control_response","response":{"subtype":"success",
              "request_id":message["request_id"],"response":{}}})
    elif message["type"] == "user":
        text = message["message"]["content"]
        with open(os.environ["FAKE_CLAUDE_LOG"], "a") as log:
            log.write(json.dumps({"text": text, "uuid":message["uuid"], "session": session}) + "\n")
        if text == "crash-before-echo":
            raise SystemExit(23)
        if text == "folded-into-peer-turn":
            # Native Claude, observed 2026-11-10: a peer message's turn ends, a second peer
            # message starts a turn, the queued human input joins that turn, and the turn's
            # result carries the peer origin.
            result("first peer turn", PEER)
            emit({"type":"user","uuid":"peer-message","message":{"role":"user","content":"peer"},"origin":PEER})
            emit({"type":"user","uuid":message["uuid"],"message":{"role":"user","content":text},"origin":{"kind":"human"}})
            result("answer to both", PEER)
            continue
        emit({"type":"user","uuid":message["uuid"],"message":{"role":"user","content":text},"origin":{"kind":"human"}})
        if text == "wrong-session":
            # A failure of the worker's own check, late in a session, with no stderr line.
            emit({"type":"result", "subtype":"success", "is_error":False, "duration_ms":1,
                  "duration_api_ms":1, "num_turns":1, "session_id":"another-session",
                  "result":"x", "origin":{"kind":"human"}, "usage":{}})
            continue
        if text == "then-a-peer-turn":
            # The input's own turn ends; a peer message then starts a turn that works
            # in silence for longer than the idle timeout.
            import time
            result("echo: " + text)
            emit({"type":"user","uuid":"peer-message","message":{"role":"user","content":"peer"},"origin":PEER})
            time.sleep(1.5)
            result("peer turn done", PEER)
            continue
        if text == "permission":
            pending = text
            emit({"type":"control_request","request_id":"permission-1",
                  "request":{"subtype":"can_use_tool","tool_name":"Bash",
                             "input":{"command":"touch example"},"tool_use_id":"tool-1"}})
        elif text != "hold":
            result("echo: " + text)
    elif message["type"] == "control_response" and pending:
        result(json.dumps(message["response"]["response"]))
        pending = None
"""
    )
    cli.chmod(0o700)
    return cli


async def wait_for(predicate):
    for _ in range(200):
        if value := predicate():
            return value
        await asyncio.sleep(0.05)
    raise AssertionError("condition did not become true")


def options(tmp_path: Path, native: Path) -> LaunchOptions:
    return LaunchOptions(
        cwd=tmp_path,
        claude=native,
        idle_timeout_seconds=0,
        environment={
            "FAKE_CLAUDE_LOG": str(tmp_path / "inputs.jsonl"),
            "PRIVATE_TEST_SECRET": "do-not-persist-this",
        },
    )


async def test_reconnect_duplicate_admission_and_explicit_resume(tmp_path, native):
    root = tmp_path / "runtime"
    runtime = ClaudeRuntime(root)
    opts = options(tmp_path, native)
    first = await runtime.start("hello", message_id="m1", options=opts, launch_id="launch1")
    sid, eid = first["session_id"], first["execution_id"]
    try:
        await wait_for(
            lambda: any(e["event"]["type"] == "work.settled" for e in runtime.store.events(sid))
        )
        # A new client is the entire application-service restart contract.
        runtime = ClaudeRuntime(root)
        assert runtime.inspect(eid)["result"] is None
        assert runtime.store.receipt(sid, "m1")["status"] == "delivered"
        assert (await runtime.start("hello", message_id="m1", options=opts, launch_id="launch1"))[
            "execution_id"
        ] == eid
        with pytest.raises(ValueError, match="different"):
            await runtime.start("changed", message_id="m1", options=opts, launch_id="launch1")
        await runtime.submit(sid, "next", message_id="m2", options=opts, resume=False)
        await wait_for(lambda: runtime.store.receipt(sid, "m2")["status"] == "delivered")
        receipt = await runtime.submit(sid, "next", message_id="m2", options=opts, resume=False)
        assert receipt["status"] == "delivered"
        with pytest.raises(ValueError):
            await runtime.submit(sid, "different", message_id="m2", options=opts, resume=False)
        assert "do-not-persist-this" not in runtime.store.path.read_bytes().decode(errors="ignore")
        assert os.stat(root).st_mode & 0o777 == 0o700
        assert os.stat(runtime.store.path).st_mode & 0o777 == 0o600
        assert (await runtime.stop(eid))["result"]["status"] == "stopped"
        rejected = await runtime.submit(sid, "later", message_id="m3", options=opts, resume=False)
        assert rejected["status"] == "rejected"
        resumed = await runtime.submit(sid, "later", message_id="m3", options=opts, resume=True)
        eid = resumed["execution_id"]
        assert eid != first["execution_id"]
        await wait_for(lambda: runtime.store.receipt(sid, "m3")["status"] == "delivered")
        recorded = [
            json.loads(line) for line in (tmp_path / "inputs.jsonl").read_text().splitlines()
        ]
        assert [r["text"] for r in recorded] == ["hello", "next", "later"]
        assert {r["session"] for r in recorded} == {sid}
    finally:
        await runtime.stop(eid)


async def test_lost_native_ack_is_uncertain_and_never_replayed(tmp_path, native):
    runtime = ClaudeRuntime(tmp_path / "runtime")
    opts = options(tmp_path, native)
    value = await runtime.start(
        "crash-before-echo", message_id="m1", options=opts, launch_id="crash"
    )
    sid, eid = value["session_id"], value["execution_id"]
    try:
        await wait_for(lambda: runtime.inspect(eid)["result"])
        assert runtime.store.receipt(sid, "m1")["status"] == "uncertain"
        saved = await runtime.submit(
            sid, "crash-before-echo", message_id="m1", options=opts, resume=True
        )
        assert saved["status"] == "uncertain"
        assert len((tmp_path / "inputs.jsonl").read_text().splitlines()) == 1
    finally:
        await runtime.stop(eid)


async def test_permission_waits_for_durable_explicit_answer(tmp_path, native):
    runtime = ClaudeRuntime(tmp_path / "runtime")
    value = await runtime.start(
        "permission", message_id="m1", options=options(tmp_path, native), launch_id="permission"
    )
    sid, eid = value["session_id"], value["execution_id"]
    try:
        requests = await wait_for(lambda: runtime.store.interactions(eid))
        assert requests[0]["params"]["tool"] == "Bash"
        assert not any(e["event"]["type"] == "work.settled" for e in runtime.store.events(sid))
        runtime = ClaudeRuntime(tmp_path / "runtime")
        runtime.store.answer(eid, requests[0]["id"], {"decision": "deny"})
        settled = await wait_for(
            lambda: [
                e["event"]
                for e in runtime.store.events(sid)
                if e["event"]["type"] == "work.settled"
            ]
        )
        assert json.loads(settled[0]["message"])["behavior"] == "deny"
        with pytest.raises(ValueError):
            runtime.store.answer(eid, requests[0]["id"], {"decision": "allow"})
    finally:
        await runtime.stop(eid)


async def test_input_folded_into_a_peer_turn_settles_with_that_turn(tmp_path, native):
    runtime = ClaudeRuntime(tmp_path / "runtime")
    opts = options(tmp_path, native)
    value = await runtime.start(
        "folded-into-peer-turn", message_id="m1", options=opts, launch_id="folded"
    )
    sid, eid = value["session_id"], value["execution_id"]

    def settled():
        return [
            e["event"] for e in runtime.store.events(sid) if e["event"]["type"] == "work.settled"
        ]

    try:
        await wait_for(lambda: len(settled()) >= 2)
        # The first result came before the input's echo, so it is not the input's result.
        assert [event["message_ids"] for event in settled()] == [[], ["m1"]]
        assert settled()[1]["message"] == "answer to both"
        # The worker is free again: it takes the next input.
        await runtime.submit(sid, "next", message_id="m2", options=opts, resume=False)
        await wait_for(lambda: len(settled()) >= 3)
        assert settled()[2]["message_ids"] == ["m2"]
    finally:
        await runtime.stop(eid)


async def test_a_turn_for_another_origin_is_not_ended_by_the_idle_timeout(tmp_path, native):
    runtime = ClaudeRuntime(tmp_path / "runtime")
    opts = LaunchOptions(
        cwd=tmp_path,
        claude=native,
        idle_timeout_seconds=0.5,
        environment={"FAKE_CLAUDE_LOG": str(tmp_path / "inputs.jsonl")},
    )
    value = await runtime.start("then-a-peer-turn", message_id="m1", options=opts, launch_id="peer")
    sid, eid = value["session_id"], value["execution_id"]
    try:
        await wait_for(lambda: runtime.inspect(eid)["result"])
        events = [e["event"] for e in runtime.store.events(sid)]
        kinds = [
            e["type"]
            for e in events
            if e["type"] in {"turn.started", "work.settled", "worker.exited"}
        ]
        # The worker saw the peer turn start and waited for its result before its idle exit.
        assert kinds == [
            "turn.started",
            "work.settled",
            "turn.started",
            "work.settled",
            "worker.exited",
        ]
        assert events[-1]["reason"] == "idle_timeout"
        settled = [e for e in events if e["type"] == "work.settled"]
        assert [e["message_ids"] for e in settled] == [["m1"], []]
        assert settled[1]["message"] == "peer turn done"
    finally:
        await runtime.stop(eid)


async def test_effort_reaches_native_claude_and_a_link_to_it_is_not_resolved(tmp_path, native):
    link = tmp_path / "shim"
    link.symlink_to(native)
    opts = LaunchOptions(
        cwd=tmp_path,
        claude=link,
        effort="high",
        idle_timeout_seconds=0,
        environment={
            "FAKE_CLAUDE_LOG": str(tmp_path / "inputs.jsonl"),
            "FAKE_CLAUDE_ARGV": str(tmp_path / "argv.json"),
        },
    )
    assert opts.configuration()["claude"] == str(link)
    with pytest.raises(ValueError, match="effort"):
        LaunchOptions(cwd=tmp_path, claude=native, effort="extreme").configuration()
    with pytest.raises(ValueError, match="not found"):
        LaunchOptions(cwd=tmp_path, claude=tmp_path / "missing").configuration()
    runtime = ClaudeRuntime(tmp_path / "runtime")
    value = await runtime.start("hello", message_id="m1", options=opts, launch_id="effort")
    try:
        await wait_for(
            lambda: runtime.store.receipt(value["session_id"], "m1")["status"] == "delivered"
        )
        argv = json.loads((tmp_path / "argv.json").read_text())
        assert argv[0] == str(link) and argv[argv.index("--effort") + 1] == "high"
    finally:
        await runtime.stop(value["execution_id"])


async def test_a_native_claude_that_fails_at_its_start_reports_what_it_wrote(tmp_path, native):
    runtime = ClaudeRuntime(tmp_path / "runtime")
    opts = options(tmp_path, native)
    opts.environment["FAKE_CLAUDE_FAILS"] = "1"
    value = await runtime.start("hello", message_id="m1", options=opts, launch_id="fails")
    result = await wait_for(lambda: runtime.inspect(value["execution_id"])["result"])
    assert result["status"] == "failed" and "Invalid API key" not in result["error"]
    # The last 20 lines, each cut to 500 characters, apart from the error.
    lines = result["native_stderr"]
    assert len(lines) == 20 and lines[-1] == "Invalid API key - Please run /login"
    assert all(len(line) <= 500 for line in lines) and lines[0].startswith("line 11 ")
    assert runtime.store.receipt(value["session_id"], "m1")["status"] == "rejected"


async def test_stderr_from_before_a_result_is_not_reported_with_a_later_failure(tmp_path, native):
    runtime = ClaudeRuntime(tmp_path / "runtime")
    opts = options(tmp_path, native)
    value = await runtime.start("hello", message_id="m1", options=opts, launch_id="late")
    sid, eid = value["session_id"], value["execution_id"]
    await wait_for(lambda: runtime.store.receipt(sid, "m1")["status"] == "delivered")
    await runtime.submit(sid, "wrong-session", message_id="m2", options=opts, resume=False)
    result = await wait_for(lambda: runtime.inspect(eid)["result"])
    assert result["status"] == "failed" and "different session ID" in result["error"]
    assert "native_stderr" not in result  # the warning at the start came before the first result


async def test_a_launch_id_from_an_older_version_can_be_repeated(tmp_path, native):
    """0.2.1 stored no effort and the link target of the executable; a repeat of that launch
    by this version must find the same execution."""
    link = tmp_path / "shim"
    link.symlink_to(native)
    runtime = ClaudeRuntime(tmp_path / "runtime")
    opts = LaunchOptions(
        cwd=tmp_path,
        claude=link,
        idle_timeout_seconds=0,
        environment={"FAKE_CLAUDE_LOG": str(tmp_path / "inputs.jsonl")},
    )
    assert "effort" not in opts.configuration()
    first = await runtime.start("hello", message_id="m1", options=opts, launch_id="old")
    eid = first["execution_id"]
    try:
        stored = dict(runtime.store.execution(eid)["configuration"], claude=str(native.resolve()))
        runtime.store.update(eid, configuration=stored)
        again = await runtime.start("hello", message_id="m1", options=opts, launch_id="old")
        assert again["execution_id"] == eid
        with pytest.raises(ValueError, match="configuration"):
            changed = LaunchOptions(
                cwd=tmp_path, claude=link, effort="high", idle_timeout_seconds=0
            )
            await runtime.start("hello", message_id="m1", options=changed, launch_id="old")
    finally:
        await runtime.stop(eid)
