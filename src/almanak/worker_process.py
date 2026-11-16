"""Bounded transport and cleanup for single-evaluation sandbox workers."""

import os
import select
import signal
import subprocess
import tempfile

from almanak import worker_policy
from almanak.sandbox import worker_command
from almanak.worker_policy import MAX_BYTES, READY_FD_ENV, SINGLE_THREADED, WALL_SECONDS


class WorkerWallTimeout(RuntimeError):
    """The author's wall budget ran out before the worker answered.

    Unlike a crash or the CPU limit, this can be caused by a starved host rather than the
    author's code, so the coordinator may evaluate again before treating it as fatal.
    """


class WorkerFailed(RuntimeError):
    """The worker exited unsuccessfully: `returncode` is its exit status, or minus the signal
    that ended it (a resource limit such as RLIMIT_CPU or RLIMIT_FSIZE)."""

    def __init__(self, message: str, returncode: int, errors: str):
        super().__init__(message)
        self.returncode = returncode
        self.errors = errors


class WorkerOutputLimit(ValueError):
    """The worker answered with more output than the caller accepts."""


class WorkerStartupTimeout(WorkerWallTimeout):
    """Trusted interpreter startup and SDK imports ran past their own bound.

    No author code has run yet, so only the host can be slow here: a starved node delays
    startup exactly as it delays an evaluation, and the coordinator retries both alike.
    """


def kill_worker(process):
    if process.poll() is None:
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        process.wait()


def with_ready_descriptor(command, descriptor):
    # bubblewrap clears the environment, so the sandboxed interpreter gets the variable
    # from the wrapper; the inherited descriptor itself stays open across it.
    if command and os.path.basename(command[0]) == "bwrap":
        split = command.index("--")
        return [*command[:split], "--setenv", READY_FD_ENV, str(descriptor), *command[split:]]
    return command


def await_startup(process, ready, seconds):
    """Wait until the worker finishes trusted startup, exits, or exceeds its startup bound."""
    readable, _, _ = select.select([ready], [], [], seconds)
    if not readable:
        raise WorkerStartupTimeout("strategy worker exceeded startup limit")
    # b"ready", or end of file when the worker exited first; communicate() reports that.
    os.read(ready, 16)


def run_worker(module, body, arguments=(), *, wall_seconds=WALL_SECONDS, output_bytes=MAX_BYTES):
    payload = body.encode()
    if len(payload) > MAX_BYTES:
        raise ValueError("strategy worker input exceeds its limit")
    ready, child = os.pipe()
    ready_child: int | None = child
    # Regular files keep communicate() from accumulating untrusted output in
    # parent memory. Workers set an immutable RLIMIT_FSIZE before loading source.
    # The request is a file too: the worker reads it during trusted startup, before ready.
    try:
        with (
            tempfile.TemporaryFile() as request,
            tempfile.TemporaryFile() as output,
            tempfile.TemporaryFile() as errors,
        ):
            request.write(payload)
            request.seek(0)
            with subprocess.Popen(
                [*with_ready_descriptor(worker_command(), child), "-m", module, *arguments],
                stdin=request,
                stdout=output,
                stderr=errors,
                cwd="/",
                env={
                    "PATH": os.defpath,
                    "PYTHONNOUSERSITE": "1",
                    "PYTHONDONTWRITEBYTECODE": "1",
                    **SINGLE_THREADED,
                    READY_FD_ENV: str(child),
                },
                pass_fds=(child,),
                start_new_session=True,
            ) as process:
                os.close(child)
                ready_child = None
                try:
                    # Interpreter startup and SDK imports, including the request's
                    # connector, are trusted and bounded on their own; the author's wall
                    # budget starts once the worker reports them done.
                    await_startup(process, ready, worker_policy.STARTUP_WALL_SECONDS)
                    process.communicate(timeout=wall_seconds)
                except subprocess.TimeoutExpired as exc:
                    raise WorkerWallTimeout("strategy worker exceeded wall-time limit") from exc
                finally:
                    kill_worker(process)
                if process.returncode:
                    errors.seek(0, os.SEEK_END)
                    errors.seek(max(0, errors.tell() - 2000))
                    tail = errors.read(2000).decode(errors="replace")
                    raise WorkerFailed("strategy worker failed: " + tail, process.returncode, tail)
            output.seek(0)
            result = output.read(output_bytes + 1)
            if len(result) > output_bytes:
                raise WorkerOutputLimit("strategy worker output exceeds its limit")
            return result.decode()
    finally:
        os.close(ready)
        if ready_child is not None:
            os.close(ready_child)
