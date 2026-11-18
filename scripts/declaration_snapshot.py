"""Regenerate or check the retained connector declaration snapshot.

The snapshot records the trusted public declarations from `reports.support()`
together with the source head and a scope sentence describing the promotion
history. Promotions regenerate it in the same change as the declaration edit:

    uv run python -m scripts.declaration_snapshot --head <sha> --scope "<text>"

`--check` compares the rendering with the committed file and exits 1 on any
byte difference. Without `--head` or `--scope`, the committed values are kept,
so check mode verifies only that the entries still match the declarations.
"""

import argparse
import json
import sys
from pathlib import Path
from typing import Any

from almanak import reports

ROOT = Path(__file__).resolve().parents[1]
SNAPSHOT = ROOT / "tests/fixtures/connector-chain-declarations.json"


def render(head: str, scope: str) -> str:
    """Return the snapshot text for the installed declarations."""
    entries = [
        {"connector": connector, **entry}
        for connector, rows in sorted(reports.support().items())
        for entry in rows
    ]
    document: dict[str, Any] = {"head": head, "scope": scope, "entries": entries}
    return json.dumps(document, indent=1) + "\n"


def arguments(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Regenerate or check the retained connector declaration snapshot."
    )
    parser.add_argument("--check", action="store_true", help="exit 1 if the file would change")
    parser.add_argument("--head", help="source commit recorded in the snapshot")
    parser.add_argument("--scope", help="scope sentence recorded in the snapshot")
    parser.add_argument("--path", type=Path, default=SNAPSHOT, help=argparse.SUPPRESS)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = arguments(argv)
    current = args.path.read_text(encoding="utf-8")
    existing = json.loads(current)
    text = render(args.head or existing["head"], args.scope or existing["scope"])
    if args.check:
        if text == current:
            return 0
        print(f"{args.path} is out of date with reports.support()", file=sys.stderr)
        return 1
    args.path.write_text(text, encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
