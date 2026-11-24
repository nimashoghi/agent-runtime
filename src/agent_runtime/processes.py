"""Process identity that a step of the system clock does not change, and agy owners."""

from __future__ import annotations

import re
import sys
from pathlib import Path

import psutil

from agent_runtime.claude import process_alive

AGY_CONVERSATION = re.compile(r"[A-Za-z0-9][A-Za-z0-9-]{0,127}")


def start_token(process: psutil.Process) -> str:
    """When the process started, as a value that one process lifetime keeps.

    psutil's create_time on Linux adds the start's clock ticks to a boot time
    that it derives from the current clock, so a clock step changes it. The
    ticks themselves do not change. macOS records the start time once.
    """
    if sys.platform.startswith("linux"):
        with open(f"/proc/{process.pid}/stat") as stat:
            # Field 22, starttime; the command name in parentheses can hold spaces.
            return "ticks:" + stat.read().rsplit(")", 1)[1].split()[19]
    return f"time:{process.create_time()!r}"


def agy_conversation(session_id: str) -> str:
    """The agy conversation ID, which names files, or ValueError."""
    if not AGY_CONVERSATION.fullmatch(session_id):
        raise ValueError(f"not an agy conversation ID: {session_id!r}")
    return session_id


def presence_lock(session_id: str) -> Path:
    """The file that the live agy process of a conversation keeps open."""
    folder = Path.home() / ".gemini" / "antigravity-cli" / "presence"
    return (folder / f"{agy_conversation(session_id)}.lock").resolve()


def holds(process: psutil.Process, lock: Path) -> bool:
    return any(Path(file.path).resolve() == lock for file in process.open_files())


def agy_alive(pid: int, token: str, session_id: str) -> bool | None:
    """Whether the agy process of this lifetime still holds the conversation;
    None when this process may not inspect it."""
    try:
        process = psutil.Process(pid)
        if start_token(process) != token:
            return False  # the PID belongs to another process now
        return holds(process, presence_lock(session_id))
    except psutil.NoSuchProcess:
        return False
    except (psutil.AccessDenied, OSError):
        return None


def owner_alive(owner: dict) -> bool:
    """Whether a monitor's owner lives: an agy owner by its lifetime and
    conversation, another by PID and creation time."""
    if "start_token" in owner:
        # A process that this one may not inspect counts as alive.
        return agy_alive(owner["pid"], owner["start_token"], owner["conversation"]) is not False
    return process_alive(owner)
