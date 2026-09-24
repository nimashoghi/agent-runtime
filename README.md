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
- A normal turn result leaves the worker available for more requests. The idle timeout applies only when there is no active request or tracked background task. Set it to zero to keep the worker open until explicitly stopped.
- Private SQLite state and logs contain conversation content. Environment variables are passed to the child without being serialized into launch configuration.

`runtime.stop(execution_id)` requests shutdown. `runtime.attach(execution_id)` returns an interactive resume command only after the managed worker has stopped, preventing concurrent native ownership. It must be launched from the recorded working directory.

Monitoring, goal recovery, and cross-session messaging are still being implemented. This package does not yet promise recovery of a watch or goal across host restarts.

## Development

Run `uv run pytest -q`. The integration tests use the real Claude Agent SDK and detached worker processes against a deterministic fake native CLI. They cover application reconnection, stop/resume, duplicate admission, lost acknowledgement, and explicit permission decisions. They require no model account or API credentials.

Release publishing uses GitHub Actions trusted publishing. Configure a pending PyPI publisher with project `agent-runtime-py`, owner `nimashoghi`, repository `agent-runtime`, workflow `release.yml`, and environment `pypi`. Publishing a GitHub release whose tag matches `v<package-version>` builds and publishes the package.
