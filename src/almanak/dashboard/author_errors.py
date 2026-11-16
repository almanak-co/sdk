"""One-paragraph explanations of dashboard failures, for the author instead of a traceback."""

import traceback

AUTHOR_FILES = ("dashboard.py", "parameters.py", "strategy.py")
HINT = (
    "Compare with the closest reference's dashboard.py"
    " (`almanak support --references --connector <id> --json`)."
)


def _where(exc: BaseException) -> str:
    # Author code runs compiled under its bare file name, so an SDK file never matches.
    if isinstance(exc, SyntaxError) and exc.filename in AUTHOR_FILES:
        return f"{exc.filename} line {exc.lineno}: "
    frames = [f for f in traceback.extract_tb(exc.__traceback__) if f.filename in AUTHOR_FILES]
    if not frames:
        return ""
    last = frames[-1]
    return f"{last.filename} line {last.lineno} ({last.name}): "


def _what(exc: BaseException) -> str:
    errors = getattr(exc, "errors", None)
    if callable(errors) and type(exc).__name__ == "ValidationError":
        model = getattr(exc, "title", "value")
        parts = []
        for error in errors()[:3]:
            loc = ".".join(str(p) for p in error.get("loc", ())) or "value"
            got = error.get("input")
            shown = "" if isinstance(got, dict | list | tuple) else f" (got {got!r})"
            parts.append(f"{model}.{loc}: {error.get('msg', '')}{shown}")
        return "; ".join(parts)
    return f"{type(exc).__name__}: {exc}"


def explain(exc: BaseException) -> str:
    """`dashboard.py line 55 (dashboard): Transition.tone: ... (got 'x')`, plus a pointer."""
    return f"{_where(exc)}{_what(exc)}\n{HINT}"
