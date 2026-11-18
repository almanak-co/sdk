"""Bundled JSON schemas for authored files, served by ``almanak support --schema``.

The schema files are the ones editors use from ``docs/schemas``; the wheel carries a copy so an
installed SDK serves exactly the schema of its own version without a source checkout.
The dashboard kind is generated from the response model instead: it describes what the
evaluator emits, not what an author writes.
"""

import json
from pathlib import Path

ROOTS = {
    "strategy": "strategy.schema.json",
    "backtest": "backtest.schema.json",
    "dashboard": "dashboard.schema.json",
}


def directory() -> Path:
    bundled = Path(__file__).parent / "files"
    if bundled.is_dir():
        return bundled
    return Path(__file__).parents[3] / "docs" / "schemas"


def bundle(kind: str) -> dict:
    if kind not in ROOTS:
        raise ValueError("schema kind must be strategy, backtest or dashboard")
    if kind == "dashboard":
        from almanak.dashboard.preview import schema

        return {"root": ROOTS[kind], "files": {ROOTS[kind]: schema()}}
    base = directory()
    files = {ROOTS[kind]: json.loads((base / ROOTS[kind]).read_text())}
    for path in sorted(base.rglob("*.schema.json")):
        files.setdefault(path.relative_to(base).as_posix(), json.loads(path.read_text()))
    return {"root": ROOTS[kind], "files": files}
