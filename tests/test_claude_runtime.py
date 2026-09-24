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
def emit(value):
    print(json.dumps(value), flush=True)
session = next(a.split("=", 1)[1] for a in sys.argv if a.startswith(("--session-id=", "--resume=")))
def result(text):
    emit({"type":"result", "subtype":"success", "is_error":False,
          "duration_ms":1,"duration_api_ms":1,"num_turns":1,"session_id":session,
          "result":text,"origin":{"kind":"human"},"usage":{}})
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
        emit({"type":"user","uuid":message["uuid"],"message":{"role":"user","content":text},"origin":{"kind":"human"}})
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
