"""Process identity that a step of the system clock does not change, and agy owners."""

from __future__ import annotations

import ctypes
import errno
import os
import re
import sys
from pathlib import Path

import psutil

from agent_runtime.claude import process_alive

AGY_CONVERSATION = re.compile(r"[A-Za-z0-9][A-Za-z0-9-]{0,127}")


def _boot() -> str:
    """This boot of the machine on Linux, so that a start token never repeats
    across restarts; elsewhere the start is calendar time, which does not. An
    unreadable boot ID raises, so that no token is made without it."""
    if not sys.platform.startswith("linux"):
        return ""
    if not (boot := Path("/proc/sys/kernel/random/boot_id").read_text().strip()):
        raise OSError("the boot ID is empty")
    return boot + ":"


def start_token(process: psutil.Process) -> str:
    """When the process started, as a value that one process lifetime keeps.

    This is psutil's own identity of a process lifetime (``Process._get_ident``,
    psutil 7.1 and newer): the start that the kernel recorded, without the
    adjustment to the current clock that the public create_time() makes, so a
    clock step leaves it alone.
    """
    platform_process = getattr(process, "_proc")  # noqa: B009 - psutil's private handle
    return _boot() + repr(platform_process.create_time(monotonic=True))


def lifetime_alive(pid: int, token: str) -> bool | None:
    """Whether the process of this lifetime runs; None when it cannot be inspected."""
    try:
        process = psutil.Process(pid)
        return start_token(process) == token and process.status() != psutil.STATUS_ZOMBIE
    except psutil.NoSuchProcess:
        return False
    except (psutil.AccessDenied, OSError):
        return None


def agy_conversation(session_id: str) -> str:
    """The agy conversation ID, which names files, or ValueError."""
    if not AGY_CONVERSATION.fullmatch(session_id):
        raise ValueError(f"not an agy conversation ID: {session_id!r}")
    return session_id


def presence_lock(session_id: str) -> Path:
    """The name of the file that the live agy process of a conversation keeps
    open. It is not resolved: holds() follows it when it looks."""
    folder = Path.home() / ".gemini" / "antigravity-cli" / "presence"
    return folder / f"{agy_conversation(session_id)}.lock"


if sys.platform == "darwin":
    # sys/proc_info.h: proc_pidfdinfo(PROC_PIDFDVNODEPATHINFO) gives the device
    # and inode of another process's open file, which psutil does not.
    class _VinfoStat(ctypes.Structure):
        _fields_ = [
            ("vst_dev", ctypes.c_uint32),
            ("vst_mode", ctypes.c_uint16),
            ("vst_nlink", ctypes.c_uint16),
            ("vst_ino", ctypes.c_uint64),
            ("vst_uid", ctypes.c_uint32),
            ("vst_gid", ctypes.c_uint32),
            ("vst_times", ctypes.c_int64 * 8),
            ("vst_size", ctypes.c_int64),
            ("vst_blocks", ctypes.c_int64),
            ("vst_blksize", ctypes.c_int32),
            ("vst_flags", ctypes.c_uint32),
            ("vst_gen", ctypes.c_uint32),
            ("vst_rdev", ctypes.c_uint32),
            ("vst_qspare", ctypes.c_int64 * 2),
        ]

    class _VnodeInfoPath(ctypes.Structure):
        _fields_ = [
            ("vi_stat", _VinfoStat),
            ("vi_type", ctypes.c_int),
            ("vi_pad", ctypes.c_int),
            ("vi_fsid", ctypes.c_int32 * 2),
            ("vip_path", ctypes.c_char * 1024),
        ]

    class _VnodeFdInfoWithPath(ctypes.Structure):
        _fields_ = [
            ("fi_openflags", ctypes.c_uint32),
            ("fi_status", ctypes.c_uint32),
            ("fi_offset", ctypes.c_int64),
            ("fi_type", ctypes.c_int32),
            ("fi_guardflags", ctypes.c_uint32),
            ("pvip", _VnodeInfoPath),
        ]

    _LIBPROC = ctypes.CDLL("/usr/lib/libproc.dylib", use_errno=True)
    _LIBPROC.proc_pidfdinfo.argtypes = [
        ctypes.c_int,
        ctypes.c_int,
        ctypes.c_int,
        ctypes.c_void_p,
        ctypes.c_int,
    ]
    _LIBPROC.proc_pidfdinfo.restype = ctypes.c_int
    _PROC_PIDFDVNODEPATHINFO = 2


def _descriptor_file(pid: int, fd: int) -> tuple[int, int] | None:
    """The device and inode of a process's open file, or None when the
    descriptor is closed."""
    if sys.platform.startswith("linux"):
        try:
            status = os.stat(f"/proc/{pid}/fd/{fd}")
        except FileNotFoundError:
            return None
        return status.st_dev, status.st_ino
    if sys.platform == "darwin":
        info = _VnodeFdInfoWithPath()
        size = ctypes.sizeof(info)
        if (
            _LIBPROC.proc_pidfdinfo(pid, fd, _PROC_PIDFDVNODEPATHINFO, ctypes.byref(info), size)
            != size
        ):
            if (error := ctypes.get_errno()) == errno.EBADF:
                return None
            raise OSError(error, os.strerror(error))
        return info.pvip.vi_stat.vst_dev, info.pvip.vi_stat.vst_ino
    raise NotImplementedError(f"agy sessions need Linux or macOS, not {sys.platform}")


def _named_file(path: Path) -> tuple[int, int] | None:
    """The device and inode of the file that a name identifies now, in the
    form of _descriptor_file, or None when there is none."""
    try:
        status = os.stat(path)
    except FileNotFoundError:
        return None
    # macOS reports the device of an open file as unsigned.
    device = status.st_dev & 0xFFFFFFFF if sys.platform == "darwin" else status.st_dev
    return device, status.st_ino


def holds(process: psutil.Process, lock: Path) -> bool:
    """Whether the process has the file that the lock's name identifies open.

    psutil names an open file that was replaced at its path, and that a process
    still holds open, by that path, so the open file must also be the same file
    (device and inode) as the one that the name identifies after the scan: a
    replacement before or during the scan does not count.
    """
    target = lock.resolve()
    opened = set()
    for file in process.open_files():
        if Path(file.path).resolve() == target and (
            identity := _descriptor_file(process.pid, file.fd)
        ):
            opened.add(identity)
    return bool(opened) and _named_file(lock) in opened


def agy_alive(pid: int, token: str, session_id: str) -> bool | None:
    """Whether the agy process of this lifetime still holds the conversation;
    None when this process may not inspect it. The lifetime is checked again
    after the open files, with a fresh look, so a PID that changed hands in
    between does not count."""
    if (first := lifetime_alive(pid, token)) is not True:
        return first
    try:
        held = holds(psutil.Process(pid), presence_lock(session_id))
    except psutil.NoSuchProcess:
        return False
    except (psutil.AccessDenied, OSError, RuntimeError):  # RuntimeError: a symlink loop
        return None
    if not held:
        return False
    return lifetime_alive(pid, token)


def owner_alive(owner: dict) -> bool:
    """Whether a monitor's owner lives: an agy owner by its lifetime and
    conversation, another by PID and creation time."""
    if "start_token" in owner:
        # A process that this one may not inspect counts as alive.
        return agy_alive(owner["pid"], owner["start_token"], owner["conversation"]) is not False
    return process_alive(owner)
