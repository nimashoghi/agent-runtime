# Changelog

## 0.2.0

- Breaking: requires typed-agent-hooks 0.3. Run the hooks with `agent_runtime.hooks.app.main()` and the host name (`codex` or `claude`) as the only argument; the host hook file sets the timeout. The `agent-runtime-hooks` console script is removed.
- Breaking: the Claude Agent SDK moves to the `claude` extra. `ClaudeRuntime` needs `agent-runtime-py[claude]`.
- Fix: a monitor worker whose command exceeded the output limit never exited and ignored SIGTERM.
