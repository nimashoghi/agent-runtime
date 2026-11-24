"""Environment variables that carry a native session's identity or messaging authority."""

# An agy (Antigravity CLI) command's conversation, the address of the session's
# local server, its access token and the agy executable that sends with them.
AGY_IDENTITY = ("ANTIGRAVITY_CONVERSATION_ID", "ANTIGRAVITY_AGENT", "ANTIGRAVITY_TRAJECTORY_ID")
AGY_CREDENTIALS = ("ANTIGRAVITY_LS_ADDRESS", "ANTIGRAVITY_CSRF_TOKEN", "ANTIGRAVITY_AGENTAPI_EXE")


def without_agy(environment: dict[str, str]) -> dict[str, str]:
    """environment without an agy session's identity and credentials."""
    return {
        name: value
        for name, value in environment.items()
        if name not in AGY_IDENTITY + AGY_CREDENTIALS
    }
