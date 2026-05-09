"""Generate post-T2 expected_baseline.sqlite + expected_cells.json (VIB-4162 / T2 commit).

Run AFTER the T2 production code is in place. Produces:

* ``tests/fixtures/accounting/lp/expected_baseline.sqlite``
* ``tests/fixtures/accounting/looping/expected_baseline.sqlite``
* ``tests/fixtures/accounting/perp/expected_baseline.sqlite``
* ``tests/fixtures/accounting/lp/expected_cells.json``
* ``tests/fixtures/accounting/looping/expected_cells.json``
* ``tests/fixtures/accounting/perp/expected_cells.json``

Each ``expected_cells.json`` is shaped::

    {
        "matching_policy_version": <int>,
        "ledger_row_count": <int>,
        "accounting_events_row_count": <int>,
        "cells": {"G1": "PASS", "G2": "PASS", ...}
    }
"""

from __future__ import annotations

import json
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from almanak.framework.accounting.accountant_test import run_against_sqlite  # noqa: E402
from almanak.framework.accounting.payload_schemas import MATCHING_POLICY_VERSIONS  # noqa: E402
from almanak.framework.primitives.types import Primitive  # noqa: E402
from tests.fixtures.accounting._generate_baselines import (  # noqa: E402
    generate_lp_fixture,
    generate_looping_fixture,
    generate_perp_fixture,
)

_PRIMITIVE_VERSION_MAP = {
    "lp": Primitive.LP,
    "looping": Primitive.LENDING,
    "perp": Primitive.PERP,
}

_LEDGER_ROW_COUNT = {"lp": 4, "looping": 6, "perp": 4}


def _emit(primitive: str, generator) -> None:
    base = Path(__file__).parent
    out_dir = base / primitive
    out_dir.mkdir(parents=True, exist_ok=True)
    db_path = out_dir / "expected_baseline.sqlite"
    generator(db_path)

    # Read row counts directly from the DB.
    conn = sqlite3.connect(str(db_path))
    try:
        ae_count = conn.execute("SELECT COUNT(*) FROM accounting_events").fetchone()[0]
    finally:
        conn.close()

    report = run_against_sqlite(db_path, primitive=primitive, strict_lifecycle=True)  # type: ignore[arg-type]
    cells = {c.cell_id: c.status for c in report.cells}

    expected = {
        "matching_policy_version": MATCHING_POLICY_VERSIONS[_PRIMITIVE_VERSION_MAP[primitive]],
        "ledger_row_count": _LEDGER_ROW_COUNT[primitive],
        "accounting_events_row_count": ae_count,
        "cells": cells,
    }
    (out_dir / "expected_cells.json").write_text(
        json.dumps(expected, indent=2, sort_keys=True) + "\n"
    )


def main() -> None:
    _emit("lp", generate_lp_fixture)
    _emit("looping", generate_looping_fixture)
    _emit("perp", generate_perp_fixture)


if __name__ == "__main__":
    main()
