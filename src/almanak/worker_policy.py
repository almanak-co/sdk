"""Hard author-code limits, applied before source evaluation in every worker mode."""

import ctypes
import errno
import math
import os
import resource
import sys

MAX_BYTES = 8 * 1024 * 1024
MAX_FRAME_BYTES = 6 * MAX_BYTES + 4096
MEMORY_BYTES = 512 * 1024 * 1024
CPU_SECONDS = 20
WALL_SECONDS = 20
# A single-evaluation worker starts a fresh interpreter and imports the SDK and the
# request's connector before any author code runs. That trusted startup took 8-11s on a
# CPU-limited container. It has its own bound; the author's wall budget starts when
# the worker reports it is over.
STARTUP_WALL_SECONDS = 30
READY_FD_ENV = "ALMANAK_WORKER_READY_FD"


# The worker may start no thread or process (RLIMIT_NPROC 0); numeric libraries such as
# numpy's OpenBLAS otherwise try to start worker threads at import and abort.
SINGLE_THREADED = {"OPENBLAS_NUM_THREADS": "1", "OMP_NUM_THREADS": "1", "MKL_NUM_THREADS": "1"}


def bounded(kind, bound):
    # An unprivileged process cannot raise its hard limit; a host whose hard
    # limit already sits below the policy keeps the tighter value.
    _, hard = resource.getrlimit(kind)
    return bound if hard == resource.RLIM_INFINITY else min(bound, hard)


def spent_cpu_seconds() -> int:
    usage = resource.getrusage(resource.RUSAGE_SELF)
    return math.ceil(usage.ru_utime + usage.ru_stime)


def signal_ready():
    """Tell a single-evaluation parent that trusted startup is over.

    The descriptor is closed here, before any limit applies or author code loads, so the
    author can neither signal nor see it. Preloaded workers run without one.
    """
    descriptor = os.environ.pop(READY_FD_ENV, None)
    if descriptor is None:
        return
    try:
        os.write(int(descriptor), b"ready")
    finally:
        os.close(int(descriptor))


def restrict_author(*, cpu_seconds=CPU_SECONDS, file_bytes=MAX_BYTES):
    signal_ready()
    # Hard limits survive exec and cannot be raised by author code. Pipe output
    # additionally needs a bounded reader in the trusted parent.
    # RLIMIT_CPU counts the whole process, including the interpreter and SDK imports that
    # ran before this call; a limit below that is exceeded at once and SIGKILLs the worker.
    # The author's budget therefore starts from the CPU time already spent.
    for name, bound in (
        ("RLIMIT_CPU", spent_cpu_seconds() + cpu_seconds),
        ("RLIMIT_AS", MEMORY_BYTES),
        ("RLIMIT_FSIZE", file_bytes),
        ("RLIMIT_NOFILE", 64),
        ("RLIMIT_NPROC", 0),
    ):
        kind = getattr(resource, name)
        value = bounded(kind, bound)
        try:
            resource.setrlimit(kind, (value, value))
        except (ValueError, OSError) as error:
            raise RuntimeError(
                f"author limit {name} could not be applied: requested {value}, "
                f"current {resource.getrlimit(kind)}: {error}"
            ) from error
    if sys.platform == "linux":
        restrict_child()


def restrict_child():
    library = ctypes.CDLL("libseccomp.so.2", use_errno=True)
    library.seccomp_init.argtypes = [ctypes.c_uint32]
    library.seccomp_init.restype = ctypes.c_void_p
    library.seccomp_syscall_resolve_name.argtypes = [ctypes.c_char_p]
    library.seccomp_rule_add.argtypes = [
        ctypes.c_void_p,
        ctypes.c_uint32,
        ctypes.c_int,
        ctypes.c_uint,
    ]
    library.seccomp_load.argtypes = [ctypes.c_void_p]
    library.seccomp_release.argtypes = [ctypes.c_void_p]
    context = library.seccomp_init(0x7FFF0000)  # SCMP_ACT_ALLOW
    if not context:
        raise RuntimeError("child syscall isolation could not initialize")
    try:
        install_rules(library, context)
        if library.seccomp_load(context):
            raise RuntimeError("child syscall isolation could not activate")
    finally:
        library.seccomp_release(context)


def install_rules(library, context):
    denied = (
        "fork",
        "vfork",
        "clone",
        "clone3",
        "ptrace",
        "process_vm_readv",
        "process_vm_writev",
        "kill",
        "tkill",
        "tgkill",
        "rt_sigqueueinfo",
        "rt_tgsigqueueinfo",
        "pidfd_open",
        "pidfd_getfd",
        "pidfd_send_signal",
        "socket",
        "socketpair",
        "shmget",
        "shmat",
        "shmctl",
        "shmdt",
        "msgget",
        "msgsnd",
        "msgrcv",
        "msgctl",
        "semget",
        "semop",
        "semctl",
        "semtimedop",
        "ipc",
        "mq_open",
        "keyctl",
        "add_key",
        "request_key",
        "io_uring_setup",
        "bpf",
        "perf_event_open",
        "userfaultfd",
    )
    for name in denied:
        number = library.seccomp_syscall_resolve_name(name.encode())
        if number >= 0 and library.seccomp_rule_add(context, 0x50000 | errno.EPERM, number, 0):
            raise RuntimeError("child syscall isolation rule failed")
