# Changelog

## 0.2.2

- Add `LaunchOptions.effort` (`low`, `medium`, `high`, `xhigh` or `max`), passed to native Claude.
- A worker whose native Claude fails reports the last lines that Claude wrote to stderr in its result. Before, the error only said to check stderr.
- Fix: `LaunchOptions.claude` and the `claude` found on PATH are used as given, not as their link target. A version manager's shim (a link to the manager) was resolved to the manager, which then did not run Claude.

## 0.2.1

- Fix: a Claude input that native Claude folded into a turn started by something else (a peer message, a background task) never settled, because that turn's result carries the other origin. The worker then took no further input and never exited on idle. A result now settles the active input when the input's echo arrived before it.
- Fix: a turn that native Claude runs for another origin (a peer message) counts as work: the worker reports `turn.started` when that message arrives, not at the first assistant text, and does not exit on idle while the turn runs.
- Fix: the Claude worker starts with `-P`, so the session's working folder is not on its import path.

## 0.2.0

- Breaking: requires typed-agent-hooks 0.3. Run the hooks with `agent_runtime.hooks.app.main()` and the host name (`codex` or `claude`) as the only argument; the host hook file sets the timeout. The `agent-runtime-hooks` console script is removed.
- Breaking: the Claude Agent SDK moves to the `claude` extra. `ClaudeRuntime` needs `agent-runtime-py[claude]`.
- Fix: a monitor worker whose command exceeded the output limit never exited and ignored SIGTERM.
- Fix: a Claude SessionStart, which carries no permission mode, kept the mode empty; it now keeps the mode that the same live Claude process reported.
