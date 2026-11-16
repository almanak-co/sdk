"""Scoped Linux worker preloading with fresh child state for every request."""

import json
import os
import select
import subprocess
import sys
import tempfile
from contextlib import contextmanager
from contextvars import ContextVar

from almanak.sandbox import worker_command
from almanak.worker_policy import MAX_BYTES, MAX_FRAME_BYTES, SINGLE_THREADED
from almanak.worker_process import kill_worker

ACTIVE: ContextVar["WorkerSession | None"] = ContextVar("strategy_worker_session", default=None)
STARTED: ContextVar["WorkerSession | None"] = ContextVar("started_worker_session", default=None)


class WorkerSessionTimeout(RuntimeError):
    """The preload worker did not answer within its wall bound.

    Like a single worker's wall timeout this can be a starved host rather than the
    author's code, so a lifecycle failure caused by it is reported as `worker_timeout`.
    """

    kind = "worker_timeout"


def worker_timed_out(error: BaseException | None) -> bool:
    """Whether `error` or an exception it was raised from is a preload worker timeout."""
    while error is not None:
        if isinstance(error, WorkerSessionTimeout):
            return True
        error = error.__cause__
    return False


class WorkerSession:
    started = False

    def start(self):
        """Spawn the preload parent; `__enter__` waits for its readiness."""
        # Only the trusted preload parent and the sandbox wrapper write here: every
        # evaluation child replaces its descriptor 2 with that request's own pipe
        # before it loads source.
        self.errors = tempfile.TemporaryFile()
        try:
            self.process = subprocess.Popen(
                [*worker_command(), "-m", "almanak.worker_server"],
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=self.errors,
                text=True,
                start_new_session=True,
                cwd="/",
                env={
                    "PATH": os.defpath,
                    "PYTHONNOUSERSITE": "1",
                    "PYTHONDONTWRITEBYTECODE": "1",
                    **SINGLE_THREADED,
                },
            )
        except BaseException:
            self.errors.close()
            raise
        self.started = True
        return self

    def __enter__(self):
        if not self.started:
            self.start()
        try:
            if self.receive() != {"ready": True}:
                raise RuntimeError("sandboxed preload worker returned invalid readiness")
            return self
        except RuntimeError as error:
            # A sandbox that cannot start reports why on its own stderr, as a
            # single-evaluation worker's failure does.
            kill_worker(self.process)
            detail = self.startup_errors()
            self.__exit__()
            message = f"{error}: {detail}" if detail else str(error)
            raise type(error)(message) from error
        except BaseException:
            self.__exit__()
            raise

    def startup_errors(self):
        self.errors.seek(0, os.SEEK_END)
        self.errors.seek(max(0, self.errors.tell() - 2000))
        return self.errors.read(2000).decode(errors="replace").strip()

    def receive(self):
        stream = self.process.stdout
        assert stream is not None
        if not select.select([stream], [], [], 25)[0]:
            raise WorkerSessionTimeout("sandboxed preload worker timed out")
        line = stream.readline(MAX_FRAME_BYTES + 1)
        if len(line) > MAX_FRAME_BYTES or not line.endswith("\n"):
            raise RuntimeError("sandboxed preload worker ended or exceeded its output limit")
        return json.loads(line)

    def evaluate(self, request, arguments):
        return self.run("almanak.strategy_worker", request.model_dump_json(), arguments)

    def run(self, module, body, arguments):
        """One evaluation that `run_worker(module, body, arguments)` would otherwise start."""
        if len(body.encode()) > MAX_BYTES:
            raise ValueError("strategy worker input exceeds its limit")
        assert self.process.stdin is not None
        frame = {"module": module, "body": body, "arguments": list(arguments)}
        self.process.stdin.write(json.dumps(frame) + "\n")
        self.process.stdin.flush()
        response = self.receive()
        if "error" in response:
            raise RuntimeError(response["error"])
        return response["output"]

    def __exit__(self, *_):
        kill_worker(self.process)
        for stream in (self.process.stdin, self.process.stdout, self.errors):
            if stream is not None:
                stream.close()


@contextmanager
def started_workers():
    """Start the preload worker now so its startup overlaps the caller's other work.

    The next `preloaded_workers()` in this context claims it and waits for its
    readiness there. A worker that cannot even be spawned is left for that claim
    to start again, so a missing sandbox still fails where the caller reports it.
    """
    if sys.platform != "linux" or ACTIVE.get() is not None or STARTED.get() is not None:
        yield
        return
    try:
        session = WorkerSession().start()
    except (OSError, RuntimeError, ValueError):
        yield
        return
    token = STARTED.set(session)
    try:
        yield
    finally:
        STARTED.reset(token)
        session.__exit__()


@contextmanager
def preloaded_workers():
    if sys.platform != "linux" or ACTIVE.get() is not None:
        yield
        return
    session = STARTED.get()
    if session is not None:
        # One claim only: a later session in this context starts its own worker.
        STARTED.set(None)
    with session or WorkerSession() as active:
        token = ACTIVE.set(active)
        try:
            yield
        finally:
            ACTIVE.reset(token)
