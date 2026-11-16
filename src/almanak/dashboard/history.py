"""Readings at each decision, retained beside the dashboard input for charts.

The checkpoint keeps observations only around transactions, so a position that holds for days
leaves no trace of how its market moved. The runtime hands each decision's observation to the
export, which keeps a bounded series here. Display only: execution never reads it.
"""

import json
import re
from decimal import Decimal, localcontext
from pathlib import Path

from almanak.dashboard.models import HistoryRow

HISTORY_FILE = "history.json"
MAX_ROWS = 500
PLACES = Decimal(10) ** -18


def decimal_text(value: Decimal) -> str:
    """A reading as plain decimal digits: 18 places at most, no exponent, no trailing zeros. A
    full-range position's upper edge is a price with dozens of digits, so it is rounded at a
    precision that holds them."""
    with localcontext() as context:
        context.prec = 100
        text = format(value.quantize(PLACES), "f")
    return text.rstrip("0").rstrip(".") if "." in text else text


def prefixes(allocations) -> dict[str, str]:
    """A history-key prefix for each allocation name: characters a key cannot hold become `_`,
    and names that would collide get a numbered suffix, so every prefix is distinct."""
    taken: dict[str, str] = {}
    for name in dict.fromkeys(allocations):
        base = re.sub(r"[^a-zA-Z0-9_-]", "_", name)[:80] or "allocation"
        prefix, n = base, 2
        while prefix in taken.values():
            prefix, n = f"{base}_{n}", n + 1
        taken[name] = prefix
    return taken


def load(directory: Path) -> tuple[HistoryRow, ...]:
    try:
        rows = json.loads((directory / HISTORY_FILE).read_text())
        return tuple(HistoryRow.model_validate_json(json.dumps(row)) for row in rows)
    except (OSError, ValueError):
        return ()


def append(directory: Path, row: HistoryRow | None) -> bool:
    """Keep `row` unless the last kept row read the same block; True when it was kept."""
    if row is None:
        return False
    rows = list(load(directory))
    if rows and rows[-1].block == row.block and rows[-1].fields == row.fields:
        return False
    rows = [*rows, row][-MAX_ROWS:]
    from almanak.dashboard.export import write_atomic

    write_atomic(directory / HISTORY_FILE, [r.model_dump(mode="json") for r in rows])
    return True
