# Changelog

## 0.2.4

- Fix: a worker process that cannot be inspected counts as running. In 0.2.3, when the command line could not be read and the fallback to the start time also failed (psutil raised an error other than "no such process"), or the start time differed on Linux (a step of the system clock), the worker was taken for gone, and `inspect` recorded a live worker as failed. Only proof ends a worker's record now: no such process, a zombie, a command line of another process, or (not on Linux) another start time.

## 0.2.3

- `Store.snapshot(session_id, after=0)` reads a session's executions (in the order of their creation), its events after a cursor, and the receipts of all its inputs in one transaction. `Store.executions(session_id)` lists the executions alone.
- Fix: a worker that ends records its result and the receipts of its open inputs in one transaction (`Store.finish`), and a turn's settlement and the `uncertain` receipt of an input whose echo was lost likewise (`Store.settle`). Before, a reader could see a `rejected` or `uncertain` receipt without the result or the settlement that explains it. `Store.lose_dispatches` and `Store.reject_pending` are removed.
- Fix: `ClaudeRuntime.inspect` no longer replaces a result that the worker recorded itself in the moment before its process ended.
- Fix: a worker that exits (on idle, on a stop, after a failure) first closes its execution to new inputs (`Store.close_to_inputs`, state `stopping`), and an idle worker does not close while an accepted input waits. `submit` could give an input to a worker that was exiting on idle, which then rejected it; with `resume=True` it now starts a new execution. An input that is accepted in the moment before another exit is still rejected, together with the worker's result. `Store.accept` returns `None` for a closed execution and raises `KeyError` for an unknown one.
- A worker that gets the session's lease records the exit of earlier executions that have none (a worker that was killed while it was stopping), and `attach` refuses while any execution of the session has no result.
- Fix: a new worker waits up to 30 seconds for the session's lease, and records why (result and `worker.exited`) if it does not get it. A session that was resumed while its old worker still ended (a worker records its exit a moment before its process is gone) failed to start.
- Fix: a worker process is known by its command line, which names the execution, not by its start time. psutil on Linux computes the start time from the boot time, so a step of the system clock made every live worker look gone, and `inspect` recorded it as failed.
- Fix: `Store.latest` returns the execution that was created last in the store. It ordered by the recorded time, which a backward step of the clock puts out of order.
- Fix: a launch whose worker started but whose record could not be written kills that worker.

## 0.2.2

- Add `LaunchOptions.effort` (`low`, `medium`, `high`, `xhigh` or `max`), passed to native Claude.
- A worker that fails adds `native_stderr` to its result: the last lines (at most 20, each at most 500 characters) that native Claude wrote to stderr since its last result. They are often the reason of a failed start, where the SDK's error only says to check stderr. They are not redacted; a consumer decides whether to show them.
- Fix: `LaunchOptions.claude` and the `claude` found on PATH are used as given (made absolute), not as their link target. A version manager's shim (a link to the manager) was resolved to the manager, which then did not run Claude. A path that is not a file, also a directory, now raises `ValueError`; a missing path raised `FileNotFoundError` before.
- A repeated launch ID no longer compares the path of the Claude executable, so a launch that 0.2.1 recorded can be repeated.

## 0.2.1

- Fix: a Claude input that native Claude folded into a turn started by something else (a peer message, a background task) never settled, because that turn's result carries the other origin. The worker then took no further input and never exited on idle. A result now settles the active input when the input's echo arrived before it.
- Fix: a turn that native Claude runs for another origin (a peer message) counts as work: the worker reports `turn.started` when that message arrives, not at the first assistant text, and does not exit on idle while the turn runs.
- Fix: the Claude worker starts with `-P`, so the session's working folder is not on its import path.

## 0.2.0

- Breaking: requires typed-agent-hooks 0.3. Run the hooks with `agent_runtime.hooks.app.main()` and the host name (`codex` or `claude`) as the only argument; the host hook file sets the timeout. The `agent-runtime-hooks` console script is removed.
- Breaking: the Claude Agent SDK moves to the `claude` extra. `ClaudeRuntime` needs `agent-runtime-py[claude]`.
- Fix: a monitor worker whose command exceeded the output limit never exited and ignored SIGTERM.
- Fix: a Claude SessionStart, which carries no permission mode, kept the mode empty; it now keeps the mode that the same live Claude process reported.
