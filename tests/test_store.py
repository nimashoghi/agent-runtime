"""The store's transactions: what a reader can and cannot see between a worker's writes."""

import threading
import time

from agent_runtime.store import Store

FAILED = {"status": "failed", "error": "exit code 1", "native_stderr": ["Invalid API key"]}


def execution(store: Store, identity: str, session: str = "s", created_at: float | None = None):
    store.create_execution(
        {
            "provider": "claude_code",
            "execution_id": identity,
            "session_id": session,
            "created_at": time.time() if created_at is None else created_at,
            "state": "running",
            "result": None,
        }
    )


def test_the_end_of_an_execution_and_of_its_inputs_is_one_record(tmp_path):
    store = Store(tmp_path)
    execution(store, "e1")
    store.accept("s", "e1", "taken", "first")
    store.take_input("e1")  # dispatching
    store.accept("s", "e1", "waiting", "second")
    value = store.finish("e1", FAILED, rejection="worker stopped before dispatch", exited_at=1.0)
    assert (value["state"], value["result"], value["exited_at"]) == ("exited", FAILED, 1.0)
    view = store.snapshot("s")
    assert view["executions"] == [value]
    assert view["receipts"]["taken"]["status"] == "uncertain"
    assert view["receipts"]["waiting"]["status"] == "rejected"
    assert view["receipts"]["waiting"]["error"] == "worker stopped before dispatch"
    # An observer that only saw the process gone does not replace the worker's own record.
    seen = {"status": "failed", "error": "worker exited without a terminal record"}
    assert store.finish("e1", seen, rejection="x", observed=True) == value
    # Without a record of the worker, the observer's record is the result.
    execution(store, "e2")
    assert store.finish("e2", seen, rejection="x", observed=True)["result"] == seen


def test_a_snapshot_never_has_a_doubtful_receipt_without_what_explains_it(tmp_path):
    """A worker ends, or a turn ends without the echo of its input, while a reader reads as
    fast as it can: each snapshot has the receipt together with the result or settlement."""
    store = Store(tmp_path)
    for index in range(150):
        identity, message = f"e{index}", f"m{index}"
        execution(store, identity)
        store.accept("s", identity, message, "do it")
        if index % 2:
            store.take_input(identity)  # dispatching: the turn's end makes it uncertain
        go = threading.Event()

        def write(identity=identity, message=message, index=index, go=go):
            go.wait()
            if index % 2:
                settled = {"type": "work.settled", "status": "completed", "message_ids": [message]}
                store.settle(identity, "s", [settled], message)
            else:
                store.finish(identity, FAILED, rejection="worker stopped before dispatch")

        writer = threading.Thread(target=write)
        writer.start()
        go.set()
        deadline = time.monotonic() + 30
        while True:
            assert time.monotonic() < deadline, "the writer did not write"
            view = store.snapshot("s")
            receipt = view["receipts"][message]
            if receipt["status"] in {"rejected", "uncertain"}:
                ended = next(e for e in view["executions"] if e["execution_id"] == identity)
                settled = [
                    item["event"]
                    for item in view["events"]
                    if item["event"]["type"] == "work.settled"
                    and message in item["event"]["message_ids"]
                ]
                assert ended["result"] is not None or settled, (index, receipt)
                break
        writer.join()


def test_a_settlement_marks_only_an_input_whose_echo_was_lost(tmp_path):
    store = Store(tmp_path)
    execution(store, "e1")
    store.accept("s", "e1", "m1", "do it")
    taken = store.take_input("e1")
    assert taken is not None
    store.input_status("s", "m1", "dispatching", native_uuid="u1")
    assert store.admit("e1", "u1") == "m1"  # the echo: delivered
    store.settle("e1", "s", [{"type": "work.settled", "message_ids": ["m1"]}], "m1")
    assert store.receipt("s", "m1")["status"] == "delivered"
    assert [item["event"]["type"] for item in store.events("s")] == ["work.settled"]


def test_a_closed_execution_takes_no_input_and_an_idle_close_leaves_no_input(tmp_path):
    store = Store(tmp_path)
    execution(store, "e1")
    store.accept("s", "e1", "m1", "do it")
    # An input waits: the idle worker must not close; it takes the input instead.
    assert store.close_to_inputs("e1", unless_input_waits=True) is False
    assert store.execution("e1")["state"] == "running"
    assert store.take_input("e1") is not None
    assert store.close_to_inputs("e1", unless_input_waits=True) is True
    assert store.execution("e1")["state"] == "stopping"
    # Closed: a new input is refused and not journaled; a known message ID keeps its receipt.
    assert store.accept("s", "e1", "m2", "more") is None
    assert store.receipt("s", "m2") is None
    assert store.accept("s", "e1", "m1", "do it")["message_id"] == "m1"
    assert store.close_to_inputs("e1") is True  # again: no change
    store.finish("e1", {"status": "completed"}, rejection="x")
    assert store.accept("s", "e1", "m3", "more") is None


def test_the_newest_execution_is_the_one_created_last_whatever_the_clock_says(tmp_path):
    store = Store(tmp_path)
    execution(store, "first", created_at=2000.0)
    execution(store, "resumed", created_at=1000.0)  # the clock stepped back before the resume
    latest = store.latest("s")
    assert latest is not None and latest["execution_id"] == "resumed"
    assert [e["execution_id"] for e in store.snapshot("s")["executions"]] == ["first", "resumed"]
    assert store.executions("s") == store.snapshot("s")["executions"]
    assert store.executions("other") == []
    assert store.snapshot("other") == {"executions": [], "events": [], "receipts": {}}
