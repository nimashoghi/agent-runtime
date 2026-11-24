# Agent runtime

`agent-runtime-py` provides detached Claude Code workers that applications can reconnect to after restarting. Import it as `agent_runtime`. The workers need the `claude` extra (`agent-runtime-py[claude]`), which installs the Claude Agent SDK; the hooks, monitors and peer messages do not. It uses the official Claude Agent SDK and your installed Claude Code executable, loading native user and project settings, hooks, skills, and MCP servers.

This repository is under active development. The current implementation supports macOS and Linux and has been exercised with Claude Code 2.1.281 and Claude Agent SDK 0.2.159. Windows is not supported.

```python
from pathlib import Path
from agent_runtime import ClaudeRuntime, LaunchOptions

runtime = ClaudeRuntime(Path.home() / ".local/state/example/claude")
execution = await runtime.start(
    "Inspect this project's tests.",
    message_id="request-123",
    launch_id="conversation-456:request-123",
    options=LaunchOptions(cwd=Path.cwd()),
)
```

The default permission mode is Claude's ordinary permission behavior. Pending approvals and questions are persisted in `runtime.store.interactions(execution_id)` and remain pending until the application supplies an explicit answer. Applications must present these requests to the user; this library does not auto-approve them.

## Lifetime and delivery

- Each execution is a detached Python worker owning one native Claude SDK connection. Destroying the application client does not stop the worker.
- A native session can have several executions after explicit stop and resume. A process lease prevents two managed workers from resuming it simultaneously: a new worker waits up to 30 seconds for the worker before it to be gone.
- Message IDs are idempotent within a session. Reusing an ID with different text is an error. A launch ID also fixes the initial prompt and launch configuration.
- `accepted` means privately journaled, `dispatching` means sent or being sent, and `delivered` requires Claude to echo the exact input UUID. A crash before that acknowledgement produces `uncertain`; the runtime never silently replays that input. A worker that ends records its result and the receipts of its open inputs (`uncertain`, or `rejected` for an input that it had not taken) in one transaction.
- A worker process is known by its PID and its command line, which names the execution; a step of the system clock does not make a live worker look gone, and neither does a process that cannot be inspected: only proof ends a running worker's record (no such process, a zombie, another process's command line, or, for a process that cannot be inspected, a free session lease after the worker took it). The proof is made on the machine that runs the worker; a store must not be shared between hosts or PID namespaces. In one rare case a dead worker counts as running until another process ends: it died before it took the session's lease (or was started by 0.2.3), and its PID now belongs to a process that cannot be inspected. `runtime.store.finish(execution_id, result, rejection=..., observed=True)` records such an execution as ended. Other processes (native sessions, monitors) are known by PID and process creation time. Restarting a dead worker requires an explicit resume request. A process restart does not imply resubmitting previously delivered work.
- Events have durable increasing cursors across executions. Native SDK messages are retained as `claude.message`; presentation events include `agent.message`, `agent.turn.completed`, and `work.settled`. `runtime.store.snapshot(session_id, after=cursor)` reads a session's executions, its new events and all its receipts in one transaction; separate reads can fall between two writes of a worker.
- A normal turn result leaves the worker available for more requests. The idle timeout applies only when there is no active request or tracked background task or running monitor. Set it to zero to keep the worker open until explicitly stopped. A worker that exits closes its execution to new inputs first (state `stopping`), and `submit` with `resume=True` then starts a new execution. A worker that exits on idle does not close while an accepted input waits, so it never leaves an input that it was given. On any other exit (a stop, a failure), every input that the worker had not taken yet is `rejected` together with the worker's result: the inputs that waited behind a running turn, and one that is accepted in the moment (up to about 0.1 s) before the worker closes. A reader of `snapshot` calls `inspect` on an execution without a result to learn whether its worker still runs.
- Private SQLite state and logs contain conversation content. Environment variables are passed to the child without being serialized into launch configuration.

`runtime.stop(execution_id)` requests shutdown. `runtime.attach(execution_id)` returns an interactive resume command only after the managed worker has stopped, preventing concurrent native ownership. It must be launched from the recorded working directory.

## Monitors and local peer messages

Run the hooks from an executable PEP 723 script with an exact `agent-runtime-py` pin and a `[tool.uv] exclude-newer` date, whose body is `from agent_runtime.hooks import app` and `if __name__ == "__main__": app.main()`. Register that script for SessionStart, UserPromptSubmit and PreToolUse in each host's hook file, with the host name (`codex`, `claude` or `agy`) as its only argument and a timeout of 10 seconds. agy has no UserPromptSubmit; register the script for its PreInvocation instead. End each command with `|| true`: uv exits nonzero when it cannot start the script (2, for example, when the package index is unreachable), and Codex and Claude Code read exit 2 on UserPromptSubmit and PreToolUse as a block. The hooks register native process identities and current permission modes. Claude hooks start a private delivery child whose inherited native inbox token remains in memory; it is never written to the registry or used for messages to another session.

`agent_runtime.operations` supplies `monitor_start`, `monitor_list`, `monitor_get`, `monitor_stop`, `monitor_events`, `agents`, and `send`. Codex operations delegate to the installed `codexr` CLI. Claude monitors run a detached foreground Bash command and retain stdout observations in SQLite; stderr goes to a private log. A 1 MiB output limit stops an overly verbose monitor. Delivery batches retain each monitor's launch permission class. Native Claude receives these as peer context and applies its own admission policy, including holding messages if permission modes require approval. `transport-written` confirms only a socket write, not admission or model processing. Interrupted writes remain uncertain and are not replayed automatically.

A normal Claude monitor stops when its owning native process exits or its deadline expires. A persistent monitor has no deadline and continues collecting output after CLI exit; resuming the exact native session starts a new delivery child for pending observations. This does not keep a plain `claude -p` process open or restart processes after a host reboot. Interactive sessions and managed SDK workers receive observations between turns. A managed worker's idle timeout does not end a running monitor.

Cross-session sends identify their source and never use the destination's own-event token. For a message to a Claude Code session, an unknown source permission mode fails closed, except for an agy source, which reports no mode and carries the `prompting` class. A message to Codex carries no permission class. Codex receipts preserve its native delivered/queued meanings; Claude socket receipts are transport-only. Native policy or a stopped process can prevent delivery.

### agy

An agy (Antigravity CLI) session takes messages only through `agentapi send-message` with its own credentials (`ANTIGRAVITY_LS_ADDRESS` and `ANTIGRAVITY_CSRF_TOKEN`). agy gives these to the commands that its agent runs, and not to its hooks. The hooks record the conversation with its agy process, which they find as the ancestor that holds the conversation's presence lock file. One agy process lifetime is its PID with a start token that a step of the clock does not change (psutil's own process identity, without its adjustment to the current clock), and it is live while it holds that file; registration, discovery, non-persistent monitors and the bridge all use that test, and a registration checks it in its write transaction. Each operation that a command of the session runs (the monitor operations and `agents`) checks that the hooks registered that same agy process, and starts the session's delivery bridge when it does not run. The bridge runs in isolated Python (`-I`), counts as started only once it holds its delivery lock and has recorded itself, keeps the credentials in memory and is bound to that agy process: it delivers only while that lifetime holds the presence lock and is the registered one, it claims events and writes its record only in transactions that check the registration, and it returns a claimed batch to pending when the registration changed before the send. It resets an inherited `SIGCHLD` disposition, so that agentapi's exit status is real.

agy adds a message to the conversation as a system message and starts a model run when the session is idle. So the bridge sends only a notice that names the waiting events, and the agent reads the command output as data with `monitor_events`. A zero exit of `agentapi` means only that agy's local server took the notice (`transport-written`). A failed call is `uncertain` with its exit status only (agentapi's own error text is not kept), and an `agentapi` that does not start leaves the events pending. The monitored command, a Claude worker, the Claude delivery child and `codexr` inherit neither the credentials nor the agy identity. agy hooks report no permission mode, so agy monitors record the `prompting` class, and a peer message from an agy session to a Claude session carries it.

Limits: `send` to an agy session raises, because no other process holds its credentials. (An `agentapi` call from another agy process writes into the stored conversation without waking the live session.) A resumed agy session gets the pending observations of a persistent monitor only after one of its commands runs an operation again. A command whose environment has both an agy conversation and a Claude, Codex or managed session (an agy started inside another session) gets an error instead of a guessed identity.

Goals use Claude's native `/goal`, including its evaluator, retry limits and resume behavior. There is no second goal store or emulated Codex goal API. An objective can survive native resume while usage baselines reset; unrecoverable errors can clear it. See [Claude goals](https://code.claude.com/docs/en/goal).

## Development

Run `uv run pytest -q`. The integration tests use the real Claude Agent SDK and detached worker processes against a deterministic fake native CLI. They cover application reconnection, stop/resume, duplicate admission, lost acknowledgement, explicit permission decisions, actual shell monitor failure, process-tree shutdown, deadlines, retained observations across native-owner exit/resume, and peer permission labels. They require no model account or API credentials.

Release publishing uses GitHub Actions trusted publishing. Configure a pending PyPI publisher with project `agent-runtime-py`, owner `nimashoghi`, repository `agent-runtime`, workflow `release.yml`, and environment `pypi`. Publishing a GitHub release whose tag matches `v<package-version>` builds and publishes the package.
