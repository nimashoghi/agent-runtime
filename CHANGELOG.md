# Changelog

## 0.3.0

- agy (Antigravity CLI) sessions, provider `"agy"`. The hooks accept the host argument `agy` (typed-agent-hooks 0.4) and record an agy conversation with its agy process for `agents()`. A monitor that a command of an agy session starts reaches that session: its worker keeps the command's credentials and wakes the conversation with `agentapi send-message` and a notice of the waiting events, which the agent reads with `monitor_events`. agy sessions appear in `agents()`, and an agy session can send peer context to Claude Code sessions (with the `prompting` class, because agy reports no permission mode) and to Codex sessions. `send("agy", ...)` raises: only the session's own commands reach its live process, and an agentapi call from another agy session writes into the stored conversation without waking it. The README section on agy gives the limits.
- agy monitors are stored under `agy:<session ID>`, so that a Claude Code and an agy session with the same ID never share monitors. Claude Code monitors keep the bare session ID; Codex monitors stay in codexr.
- The monitored command, a Claude worker, the Claude delivery child and `codexr` do not inherit an agy session's identity or credentials.
- `current_identity()` raises `AmbiguousIdentity` for an environment that has an agy conversation and a Claude, Codex or managed session, and `IdentityUnavailable` when there is none; both are `RuntimeError`s.
- `monitor_get` and `monitor_stop` reject another session's monitor before they read its state.
- `Monitors.claim_monitor(monitor_id)` claims one monitor's events. `agent_runtime.processes` holds a process identity that a step of the clock does not change (the boot ID on Linux or the boot session UUID on macOS, and psutil's own start of the process), which the monitor worker records for itself and `Monitors.get` uses. A monitor worker records its own process when it starts (`Monitors.begin`), and only while the monitor still waits for it; `Monitors.start` writes nothing after the worker starts and returns the new record, whose status is `starting`. An interruption is recorded only when, inside the write transaction, the record still names the same worker and that worker is confirmed gone: by its start token for a record that has one (every agy worker, and a Claude worker that can read its start), and otherwise by PID and creation time as before (where an inspection error or a step of the clock counts as gone): for records of earlier versions, and for a Claude monitor's worker that cannot read the boot ID or its own start. A monitor whose worker has not recorded itself 60 seconds after the start, measured on the monotonic clock (the calendar clock only for records of earlier versions), is interrupted ("monitor worker did not start"): by the next read, or by the late worker itself, which then exits without running the command. The worker records the monitor's outcome and its completion or failure event in one transaction (`Monitors.finish`). A monitor stopped before its worker started never runs its command; its worker still delivers the stop notice, also when a reader recorded the stop first. A launch that fails after a stop was recorded keeps the stop. A stop request comes before an expired startup grace, whichever of a reader and the worker records the outcome. The worker also resets an inherited `SIGCHLD` disposition, and fails at once with a reason where the system denies it the list of its child processes.
- An interrupted delivery records "delivery exited during a delivery attempt". `Monitors.reconcile_deliveries(session_id)` records it for the monitors of a session that have a batch being sent; agy's `monitor_events` calls it.
- Requires `typed-agent-hooks>=0.4,<0.5` and `psutil>=7.1,<8`.

## 0.2.4

- Fix: a worker process that cannot be inspected counts as running. In 0.2.3, when the command line could not be read and the fallback to the start time also failed (psutil raised an error other than "no such process"), or the start time differed on Linux (a step of the system clock), the worker was taken for gone, and `inspect` recorded a live worker as failed. Only proof ends a running worker's record now: no such process, a zombie, a command line of another process, or, for a process that cannot be inspected, a free session lease after the worker took it (the worker records `leased_at`; a killed worker's lease is free). A start time that differs is no proof on any platform: psutil computes it from the boot time, which a step of the clock moves. `worker_alive` takes the store's folder as a second argument.
- `Store.finish` appends the `worker.exited` event in the transaction of the result. A reader could see a worker's result without that event; an execution that an observer recorded as ended had no such event.

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
