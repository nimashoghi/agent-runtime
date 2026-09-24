# Agent runtime

`agent-runtime-py` provides detached Claude Code workers that applications can reconnect to after restarting. Import it as `agent_runtime`. It uses the official Claude Agent SDK and your installed Claude Code executable, loading native user and project settings, hooks, skills, and MCP servers.

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
- A native session can have several executions after explicit stop and resume. A process lease prevents two managed workers from resuming it simultaneously.
- Message IDs are idempotent within a session. Reusing an ID with different text is an error. A launch ID also fixes the initial prompt and launch configuration.
- `accepted` means privately journaled, `dispatching` means sent or being sent, and `delivered` requires Claude to echo the exact input UUID. A crash before that acknowledgement produces `uncertain`; the runtime never silently replays that input.
- Process identity includes PID and process creation time. Restarting a dead worker requires an explicit resume request. A process restart does not imply resubmitting previously delivered work.
- Events have durable increasing cursors across executions. Native SDK messages are retained as `claude.message`; presentation events include `agent.message`, `agent.turn.completed`, and `work.settled`.
- A normal turn result leaves the worker available for more requests. The idle timeout applies only when there is no active request or tracked background task or running monitor. Set it to zero to keep the worker open until explicitly stopped.
- Private SQLite state and logs contain conversation content. Environment variables are passed to the child without being serialized into launch configuration.

`runtime.stop(execution_id)` requests shutdown. `runtime.attach(execution_id)` returns an interactive resume command only after the managed worker has stopped, preventing concurrent native ownership. It must be launched from the recorded working directory.

## Monitors and local peer messages

Install `agent-runtime-hooks` using a typed-agent-hooks `Collection` for both providers. The hooks register native process identities and current permission modes. Claude hooks start a private delivery child whose inherited native inbox token remains in memory; it is never written to the registry or used for messages to another session.

`agent_runtime.operations` supplies `monitor_start`, `monitor_list`, `monitor_get`, `monitor_stop`, `monitor_events`, `agents`, and `send`. Codex operations delegate to the installed `codexr` CLI. Claude monitors run a detached foreground Bash command and retain stdout observations in SQLite; stderr goes to a private log. A 1 MiB output limit stops an overly verbose monitor. Delivery batches retain each monitor's launch permission class. Native Claude receives these as peer context and applies its own admission policy, including holding messages if permission modes require approval. `transport-written` confirms only a socket write, not admission or model processing. Interrupted writes remain uncertain and are not replayed automatically.

A normal Claude monitor stops when its owning native process exits or its deadline expires. A persistent monitor has no deadline and continues collecting output after CLI exit; resuming the exact native session starts a new delivery child for pending observations. This does not keep a plain `claude -p` process open or restart processes after a host reboot. Interactive sessions and managed SDK workers receive observations between turns. A managed worker's idle timeout does not end a running monitor.

Cross-session sends identify their source and never use the destination's own-event token. Unknown source permission modes fail closed. Codex receipts preserve its native delivered/queued meanings; Claude socket receipts are transport-only. Native policy or a stopped process can prevent delivery.

Goals use Claude's native `/goal`, including its evaluator, retry limits and resume behavior. There is no second goal store or emulated Codex goal API. An objective can survive native resume while usage baselines reset; unrecoverable errors can clear it. See [Claude goals](https://code.claude.com/docs/en/goal).

## Development

Run `uv run pytest -q`. The integration tests use the real Claude Agent SDK and detached worker processes against a deterministic fake native CLI. They cover application reconnection, stop/resume, duplicate admission, lost acknowledgement, explicit permission decisions, actual shell monitor failure, process-tree shutdown, deadlines, retained observations across native-owner exit/resume, and peer permission labels. They require no model account or API credentials.

Release publishing uses GitHub Actions trusted publishing. Configure a pending PyPI publisher with project `agent-runtime-py`, owner `nimashoghi`, repository `agent-runtime`, workflow `release.yml`, and environment `pypi`. Publishing a GitHub release whose tag matches `v<package-version>` builds and publishes the package.
