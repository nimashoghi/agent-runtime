# Changelog

## 0.2.1

- Fix: a Claude input that native Claude folded into a turn started by something else (a peer message, a background task) never settled, because that turn's result carries the other origin. The worker then took no further input and never exited on idle. A result now settles the active input when the input's echo arrived before it.
- Fix: the Claude worker starts with `-P`, so the session's working folder is not on its import path.

## 0.2.0

- Breaking: requires typed-agent-hooks 0.3. Run the hooks with `agent_runtime.hooks.app.main()` and the host name (`codex` or `claude`) as the only argument; the host hook file sets the timeout. The `agent-runtime-hooks` console script is removed.
- Breaking: the Claude Agent SDK moves to the `claude` extra. `ClaudeRuntime` needs `agent-runtime-py[claude]`.
- Fix: a monitor worker whose command exceeded the output limit never exited and ignored SIGTERM.
- Fix: a Claude SessionStart, which carries no permission mode, kept the mode empty; it now keeps the mode that the same live Claude process reported.
