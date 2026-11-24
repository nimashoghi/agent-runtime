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


def boot_id() -> str:
    """This boot of the machine: the boot ID on Linux, the boot session UUID on
    macOS. Raises OSError when it cannot be read."""
    if sys.platform.startswith("linux"):
        boot = Path("/proc/sys/kernel/random/boot_id").read_text().strip()
    elif sys.platform == "darwin":
        boot = _sysctl_text(b"kern.bootsessionuuid")
    else:
        raise OSError(f"no boot ID on {sys.platform}")
    if not boot:
        raise OSError("the boot ID is empty")
    return boot


def start_token(process: psutil.Process, boot: str | None = None) -> str:
    """When the process started, as a value that one process lifetime keeps.

    This is psutil's own identity of a process lifetime (``Process._get_ident``,
    psutil 7.1 and newer): the start that the kernel recorded, without the
    adjustment to the current clock that the public create_time() makes, so a
    clock step leaves it alone. The boot (boot_id() unless given) comes first,
    so that it never repeats across restarts.
    """
    platform_process = getattr(process, "_proc")  # noqa: B009 - psutil's private handle
    prefix = boot if boot is not None else boot_id()
    return f"{prefix}:{platform_process.create_time(monotonic=True)!r}"


def lifetime_alive(pid: int, token: str) -> bool | None:
    """Whether the process of this lifetime runs; None when it cannot be inspected."""
    try:
        boot = boot_id()
    except OSError:  # without the boot ID, the process itself tells
        boot = None
    if boot is not None and token.rpartition(":")[0] != boot:
        return False  # a lifetime of another boot
    try:
        process = psutil.Process(pid)
        # A zombie has exited, whoever's it is; this needs no boot ID.
        if process.status() == psutil.STATUS_ZOMBIE:
            return False
        return start_token(process, boot) == token and process.status() != psutil.STATUS_ZOMBIE
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
    # sys/proc_info.h and libproc: the descriptors of another process and the
    # device and inode of each open file, which psutil does not give. ctypes
    # releases the GIL during each call, so a stalled file system holds up only
    # the thread that inspects.
    class _ProcFdInfo(ctypes.Structure):
        _fields_ = [("proc_fd", ctypes.c_int32), ("proc_fdtype", ctypes.c_uint32)]

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

    class _VnodeFdInfo(ctypes.Structure):
        # struct vnode_fdinfo: proc_fileinfo, then vnode_info.
        _fields_ = [
            ("fi_openflags", ctypes.c_uint32),
            ("fi_status", ctypes.c_uint32),
            ("fi_offset", ctypes.c_int64),
            ("fi_type", ctypes.c_int32),
            ("fi_guardflags", ctypes.c_uint32),
            ("vi_stat", _VinfoStat),
            ("vi_type", ctypes.c_int),
            ("vi_pad", ctypes.c_int),
            ("vi_fsid", ctypes.c_int32 * 2),
        ]

    _LIBPROC = ctypes.CDLL("/usr/lib/libproc.dylib", use_errno=True)
    _LIBPROC.proc_pidinfo.argtypes = [
        ctypes.c_int,
        ctypes.c_int,
        ctypes.c_uint64,
        ctypes.c_void_p,
        ctypes.c_int,
    ]
    _LIBPROC.proc_pidinfo.restype = ctypes.c_int
    _LIBPROC.proc_pidfdinfo.argtypes = [
        ctypes.c_int,
        ctypes.c_int,
        ctypes.c_int,
        ctypes.c_void_p,
        ctypes.c_int,
    ]
    _LIBPROC.proc_pidfdinfo.restype = ctypes.c_int
    _PROC_PIDLISTFDS = 1
    _PROX_FDTYPE_VNODE = 1
    # Without the path, so without the path's permission check.
    _PROC_PIDFDVNODEINFO = 1

    _LIBC = ctypes.CDLL(None, use_errno=True)
    _LIBC.sysctlbyname.argtypes = [
        ctypes.c_char_p,
        ctypes.c_void_p,
        ctypes.POINTER(ctypes.c_size_t),
        ctypes.c_void_p,
        ctypes.c_size_t,
    ]
    _LIBC.sysctlbyname.restype = ctypes.c_int

    def _failed(what: str) -> OSError:
        error = ctypes.get_errno()
        return OSError(error, f"{what}: {os.strerror(error)}")

    def _sysctl_text(name: bytes) -> str:
        size = ctypes.c_size_t(128)
        value = ctypes.create_string_buffer(size.value)
        if _LIBC.sysctlbyname(name, value, ctypes.byref(size), None, 0) != 0:
            raise _failed(f"sysctl {name.decode()}")
        return value.value.decode()

    def _darwin_descriptors(pid: int) -> list[int]:
        record = ctypes.sizeof(_ProcFdInfo)
        if (estimate := _LIBPROC.proc_pidinfo(pid, _PROC_PIDLISTFDS, 0, None, 0)) <= 0:
            raise _failed("proc_pidinfo")
        count = estimate // record + 32
        for _attempt in range(4):
            listing = (_ProcFdInfo * count)()
            size = _LIBPROC.proc_pidinfo(pid, _PROC_PIDLISTFDS, 0, listing, ctypes.sizeof(listing))
            if size <= 0:
                raise _failed("proc_pidinfo")
            if size < ctypes.sizeof(listing):  # a full buffer may have left some out
                return [
                    item.proc_fd
                    for item in listing[: size // record]
                    if item.proc_fdtype == _PROX_FDTYPE_VNODE
                ]
            count *= 2
        raise OSError(errno.EAGAIN, "the descriptor table kept growing")

    def _darwin_descriptor_file(pid: int, fd: int) -> tuple[int, int] | None:
        info = _VnodeFdInfo()
        size = ctypes.sizeof(info)
        if _LIBPROC.proc_pidfdinfo(pid, fd, _PROC_PIDFDVNODEINFO, ctypes.byref(info), size) != size:
            if ctypes.get_errno() == errno.EBADF:
                return None
            raise _failed("proc_pidfdinfo")
        return info.vi_stat.vst_dev, info.vi_stat.vst_ino


def _descriptors(pid: int) -> list[int]:
    """The open file descriptors of a process (on macOS, those of files)."""
    if sys.platform.startswith("linux"):
        return [int(name) for name in os.listdir(f"/proc/{pid}/fd")]
    if sys.platform == "darwin":
        return _darwin_descriptors(pid)
    raise OSError(f"agy sessions need Linux or macOS, not {sys.platform}")


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
        return _darwin_descriptor_file(pid, fd)
    raise OSError(f"agy sessions need Linux or macOS, not {sys.platform}")


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


class _LockChanged(OSError):
    def __init__(self) -> None:
        super().__init__(errno.EAGAIN, "the presence lock changed during the scan")


def holds(pid: int, lock: Path) -> bool:
    """Whether the process has the file that the lock's name identifies open.

    The open file must be the same file (device and inode), so that a lock that
    agy replaced does not count for an old process that still has the old file
    open. A descriptor that matches is checked again, with the name, right
    before the answer, so a lock that agy released or replaced meanwhile does
    not count; and it answers that the process does not hold the lock only if
    the name kept its file through the scan. A descriptor that cannot be
    inspected is passed over; when no other one matches, the answer is
    unknown. Raises OSError for unknown. Every call releases the GIL.
    """
    if (named := _named_file(lock)) is None:
        return False
    unknown: OSError | None = None
    for fd in _descriptors(pid):
        try:
            if _descriptor_file(pid, fd) != named:
                continue
            if _named_file(lock) != named:
                raise _LockChanged
            if _descriptor_file(pid, fd) == named:
                return True
        except _LockChanged:
            raise
        except OSError as error:  # this descriptor only: look at the others
            unknown = error
    # No: but only if the name kept its file, which agy may have replaced meanwhile.
    if _named_file(lock) != named:
        raise _LockChanged
    if unknown is not None:
        raise unknown
    return False


def agy_alive(pid: int, token: str, session_id: str) -> bool | None:
    """Whether the agy process of this lifetime still holds the conversation;
    None when this process cannot tell. The lifetime is checked again after the
    scan, so a PID that changed hands meanwhile does not count."""
    if (first := lifetime_alive(pid, token)) is not True:
        return first
    try:
        held: bool | None = holds(pid, presence_lock(session_id))
    except (OSError, RuntimeError):  # RuntimeError: a symlink loop
        held = None
    if (then := lifetime_alive(pid, token)) is not True:
        return then
    return held


def owner_alive(owner: dict) -> bool:
    """Whether a monitor's owner lives: an agy owner by its lifetime (whether
    its process still runs, which a scan of open files cannot get wrong),
    another by PID and creation time."""
    if "start_token" in owner:
        # A process that this one may not inspect counts as alive.
        return lifetime_alive(owner["pid"], owner["start_token"]) is not False
    return process_alive(owner)
