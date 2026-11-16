"""Tell an author's `validate_config` refusal apart from a crash, inside the worker.

A refusal is the author rejecting the parameters on purpose: the run reports its
message as a final `invalid_config`. Anything else is a crash, reported as the
traceback from the strategy's own first frame, without the SDK frames around it.
"""

import dis
import linecache
import traceback

from almanak.strategy_api import InvalidConfig

REFUSAL = "invalid_config"
MESSAGE_CHARS = 1000


def register_source(definition):
    """Let tracebacks quote the strategy's lines; the sandbox cannot read its file."""
    lines = definition.source.splitlines(keepends=True)
    linecache.cache[definition.filename] = (
        len(definition.source),
        None,
        lines,
        definition.filename,
    )


def refusal(exc, hook, filename):
    """The refusal document for `exc` raised by `hook`, or None when it is a crash.

    `InvalidConfig` is always a refusal. A plain `ValueError` is one only when a
    `raise` statement in `validate_config` itself raised it: an unpacking error, a
    failed conversion and a pydantic ValidationError are ValueErrors too, but they
    come from another instruction or another frame, so they stay crashes.
    """
    frames = innermost(exc.__traceback__)
    if isinstance(exc, InvalidConfig):
        own = [tb for tb in frames if tb.tb_frame.f_code.co_filename == filename]
        line = own[-1].tb_lineno if own else None
    elif type(exc) is ValueError and frames and raised_by_statement(frames[-1], hook):
        line = frames[-1].tb_lineno
    else:
        return None
    return refusal_document(str(exc), line)


def refusal_document(message, line=None, field=None):
    message = message.strip() or "validate_config refused the configuration"
    result = {"refusal": REFUSAL, "message": message[:MESSAGE_CHARS], "line": line}
    return result if field is None else {**result, "field": field}


def innermost(tb):
    frames = []
    while tb is not None:
        frames.append(tb)
        tb = tb.tb_next
    return frames


def raised_by_statement(tb, hook) -> bool:
    code = getattr(hook, "__code__", None)
    if code is None or tb.tb_frame.f_code is not code:
        return False
    instruction = next((i for i in dis.get_instructions(code) if i.offset == tb.tb_lasti), None)
    return instruction is not None and instruction.opname == "RAISE_VARARGS"


def author_traceback(exc, filename) -> str:
    """The crash from the first strategy frame on, for each exception in its chain.

    An exception without a strategy frame, such as an SDK contract error, keeps its stack.
    """
    report = traceback.TracebackException.from_exception(exc)
    pending = [report]
    while pending:
        current = pending.pop()
        frames = [f.filename for f in current.stack]
        start = frames.index(filename) if filename in frames else 0
        current.stack = traceback.StackSummary.from_list(current.stack[start:])
        pending += [c for c in (current.__cause__, current.__context__) if c is not None]
    return "".join(report.format())
