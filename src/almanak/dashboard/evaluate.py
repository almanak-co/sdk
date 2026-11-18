"""Bounded sandbox evaluation, without a coordinator or gateway command channel."""

import contextlib
import errno
import json
import os
import signal
import sys
from typing import NoReturn

from pydantic import ValidationError

from almanak.dashboard.models import Context, Dashboard, Response, Snapshot
from almanak.dashboard.projection import project
from almanak.models import Model
from almanak.strategy import StrategyDefinition
from almanak.worker_policy import restrict_author
from almanak.worker_process import (
    WorkerFailed,
    WorkerOutputLimit,
    WorkerStartupTimeout,
    WorkerWallTimeout,
)
from almanak.worker_process import run_worker as run_isolated

MAX_BYTES = 1024 * 1024
MAX_INPUT_BYTES = 8 * MAX_BYTES
WALL_SECONDS = 5
CPU_SECONDS = 3
# The worker's exit status when the hook raised, or drew a document over MAX_BYTES; its last
# line of stderr then names the exception type or the size.
HOOK_RAISED = 3
TOO_LARGE = 4
FAILURE_PREFIX = "almanak-dashboard-failure: "


class Request(Model):
    strategy: StrategyDefinition
    context: Context


def evaluate(state, *, captured_at: int | None = None, strategy=None) -> Response:
    """`state`'s dashboard; `strategy` draws instead of the captured one without changing the
    snapshot, which keeps identifying the checkpoint's strategy and record."""
    context, snapshot = project(state, captured_at=captured_at)
    return respond(strategy or state.strategy, context, snapshot)


def evaluate_input(state, document: dict, strategy=None) -> Response:
    """The dashboard of a retained input (`dashboard/input.json` or `running.json`): `state`'s
    strategy over the input's context and snapshot, as the deployment would draw it then."""
    from almanak.dashboard.projection import asset_names, symbols

    raw = document.get("context")
    context = None if raw is None else Context.model_validate_json(json.dumps(raw))
    snapshot = Snapshot.model_validate_json(json.dumps(document["snapshot"]))
    # An input exported before every amount carried its symbol reads as the projection now would.
    if context is not None and (tables := asset_names(state, context.datasets)):
        context = context.model_copy(update={"datasets": {**context.datasets, **tables}})
    labels = symbols(state, snapshot.values)
    if labels:
        snapshot = snapshot.model_copy(update={"values": {**snapshot.values, **labels}})
        if context is not None:
            named = {k: v for k, v in labels.items() if k in context.values}
            context = context.model_copy(update={"values": {**context.values, **named}})
    return respond(strategy or state.strategy, context, snapshot)


def respond(strategy, context, snapshot) -> Response:
    if strategy is None or context is None:
        return Response(
            snapshot=snapshot,
            status="absent",
            document=None,
            diagnostic="This checkpoint has no captured strategy source.",
        )
    try:
        output = run_worker(Request(strategy=strategy, context=context))
        document = None if output.strip() == "null" else Dashboard.model_validate_json(output)
        return Response(
            snapshot=snapshot, status="ready" if document else "absent", document=document
        )
    except (RuntimeError, OSError, ValueError, ValidationError) as exc:
        return Response(
            snapshot=snapshot, status="unavailable", document=None, diagnostic=failure(exc)
        )


def _reported(errors: str) -> str:
    """The worker's own last line, `kind detail`, when it is one; author output is otherwise
    never read back."""
    lines = errors.strip().splitlines()
    last = lines[-1] if lines else ""
    return last.removeprefix(FAILURE_PREFIX) if last.startswith(FAILURE_PREFIX) else ""


def failure(exc: BaseException) -> str:
    """Why a draw failed, in terms the author can act on. Raw exception messages can contain
    author state, source or credentials, so only an exception's type and a size are named."""
    limit_kib = MAX_BYTES // 1024
    if isinstance(exc, WorkerStartupTimeout):
        return "The dashboard sandbox was too slow to start (host load); drawing again may work."
    if isinstance(exc, WorkerWallTimeout):
        return f"dashboard.py ran past its {WALL_SECONDS} s time limit."
    if isinstance(exc, WorkerOutputLimit):
        return f"The dashboard is larger than its {limit_kib} KiB limit."
    if isinstance(exc, WorkerFailed):
        return _failed(exc, limit_kib)
    if isinstance(exc, ValidationError):
        return "The drawn dashboard does not match the document contract."
    if isinstance(exc, ValueError):
        return "The dashboard input is larger than the sandbox accepts."
    if isinstance(exc, RuntimeError):
        return "No OS sandbox is available to run dashboard.py (Linux needs bubblewrap)."
    return f"Dashboard evaluation failed ({type(exc).__name__})."


def _failed(exc: WorkerFailed, limit_kib: int) -> str:
    kind, _, detail = _reported(exc.errors).partition(" ")
    if exc.returncode == TOO_LARGE and kind == "too_large" and detail.isdigit():
        return (
            f"The dashboard is too large ({detail} KiB; the limit is {limit_kib} KiB). "
            "Chart fewer series or keep tables short."
        )
    if exc.returncode == HOOK_RAISED and kind == "raised" and detail.isidentifier():
        return f"dashboard.py raised {detail}."
    # A signal reads as minus its number, or as 128 + its number through bubblewrap.
    killed_by = {-exc.returncode, exc.returncode - 128}
    if signal.SIGXFSZ in killed_by:
        return f"The dashboard is larger than its {limit_kib} KiB limit."
    if killed_by & {signal.SIGXCPU, signal.SIGKILL}:
        return f"dashboard.py ran out of its {CPU_SECONDS} s of CPU or its memory."
    return f"dashboard.py stopped unexpectedly (exit status {exc.returncode})."


def run_worker(request: Request) -> str:
    return run_isolated(
        "almanak.dashboard.worker",
        request.model_dump_json(),
        wall_seconds=WALL_SECONDS,
        output_bytes=MAX_BYTES,
    )


def execute(request: Request) -> Dashboard | None:
    from almanak.strategy import dashboard_namespace

    namespace = dashboard_namespace(request.strategy)
    hook = namespace.get("dashboard")
    if hook is None:
        return None
    if not callable(hook):
        raise TypeError("dashboard must be callable")
    from almanak.typed_parameters import typed_context

    result = hook(typed_context(namespace, request.context))
    if not isinstance(result, Dashboard):
        raise TypeError("dashboard must return Dashboard")
    return result


def _document(result: Dashboard | None, written: int) -> tuple[str | None, int]:
    """The bounded document to print and the size stdout would reach, or None for the document
    when that passes MAX_BYTES (with what the hook already wrote on stdout)."""
    from almanak.dashboard.bound import bounded

    document = "null" if result is None else bounded(result).model_dump_json()
    size = len(document.encode()) + written
    return (document if size <= MAX_BYTES - 1 else None), size


def _too_large(size: int) -> NoReturn:
    sys.stderr.write(f"{FAILURE_PREFIX}too_large {-(-size // 1024)}\n")
    sys.stderr.flush()
    # Skips flushing the stdout buffer again at exit, which would fail the same way.
    os._exit(TOO_LARGE)


def _emit(document: str | None, size: int):
    if document is None:
        _too_large(size)
    try:
        print(document, flush=True)
    except OSError:
        # The hook already wrote past the limit on stdout (RLIMIT_FSIZE).
        _too_large(size)


def _run_hook(payload: bytes) -> Dashboard | None:
    try:
        # stdout carries only the result: an author's print() goes to stderr.
        with contextlib.redirect_stdout(sys.stderr):
            return execute(Request.model_validate_json(payload))
    except Exception as exc:
        if isinstance(exc, OSError) and exc.errno == errno.EFBIG:
            # The hook itself wrote past the output limit on stdout.
            _too_large(os.fstat(sys.stdout.fileno()).st_size)
        from almanak.dashboard.author_errors import explain

        sys.stderr.write(explain(exc) + "\n")
        sys.stderr.write(f"{FAILURE_PREFIX}raised {type(exc).__name__}\n")
        sys.exit(HOOK_RAISED)


def worker_main():
    restrict_author(cpu_seconds=CPU_SECONDS, file_bytes=MAX_BYTES)
    payload = sys.stdin.buffer.read(MAX_INPUT_BYTES + 1)
    if len(payload) > MAX_INPUT_BYTES:
        raise ValueError("dashboard input exceeded its size limit")
    result = _run_hook(payload)
    # stdout is a regular file: what the hook wrote on it already counts too. Read before
    # printing, so a print the limit cuts short does not count the document twice.
    _emit(*_document(result, os.fstat(sys.stdout.fileno()).st_size))
