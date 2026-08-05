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
from scripts.ci.check_accounting_ratchet import _FIXTURE_SCORING_PROFILE  # noqa: E402
from tests.fixtures.accounting._generate_baselines import (  # noqa: E402
    generate_looping_debt_open_fixture,
    generate_looping_fixture,
    generate_lp_fixture,
    generate_perp_fixture,
    generate_settlement_fixture,
)
from tests.fixtures.accounting._stamp_cell_metrics import extract_cell_metrics  # noqa: E402

_PRIMITIVE_VERSION_MAP = {
    "lp": Primitive.LP,
    "looping": Primitive.LENDING,
    "looping_debt_open": Primitive.LENDING,
    "perp": Primitive.PERP,
    "settlement": Primitive.SETTLEMENT,
}

_LEDGER_ROW_COUNT = {"lp": 4, "looping": 6, "looping_debt_open": 4, "perp": 4, "settlement": 4}


def _emit(primitive: str, generator) -> None:
    """Regenerate one fixture directory + its manifest.

    A fixture DIRECTORY is not always its scoring profile: ``looping_debt_open``
    (VIB-6560) is a second lending STIMULUS, not a second scorecard, so it reuses
    the ``looping`` cell pack and epsilon. That mapping is READ from
    ``check_accounting_ratchet._FIXTURE_SCORING_PROFILE`` rather than duplicated
    here, so this script and the ratchet gate cannot disagree about which profile
    a fixture is scored under — a disagreement would mean the manifest was frozen
    against a different cell pack than the gate later checks it with. Same import
    and same ``.get(primitive, primitive)`` default as ``_stamp_cell_metrics.py``.
    """
    profile = _FIXTURE_SCORING_PROFILE.get(primitive, primitive)
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

    report = run_against_sqlite(db_path, primitive=profile, strict_lifecycle=True)  # type: ignore[arg-type]
    cells = {c.cell_id: c.status for c in report.cells}

    expected = {
        "matching_policy_version": MATCHING_POLICY_VERSIONS[_PRIMITIVE_VERSION_MAP[primitive]],
        "ledger_row_count": _LEDGER_ROW_COUNT[primitive],
        "accounting_events_row_count": ae_count,
        "cells": cells,
        # VIB-4226 §1a: the numeric floors travel WITH the manifest. This file
        # rewrites expected_cells.json wholesale, so omitting them here would
        # silently strip every floor the moment anyone regenerated a baseline —
        # and the ratchet's own failure message points operators at this script.
        #
        # KNOWN LIMITATION (VIB-6414): this is the SECOND unconditional writer of
        # cell_metrics (the first is _stamp_cell_metrics.py, which carries the same
        # note). The ratchet's remedy text names both scripts, so re-generating
        # after a regression lowers the floor by exactly the same route. Marking
        # one entrypoint of a two-entrypoint rule would leave the other silent.
        "cell_metrics": extract_cell_metrics(report),
    }
    (out_dir / "expected_cells.json").write_text(
        json.dumps(expected, indent=2, sort_keys=True, ensure_ascii=False) + "\n"
    )


def main() -> None:
    _emit("lp", generate_lp_fixture)
    _emit("looping", generate_looping_fixture)
    _emit("looping_debt_open", generate_looping_debt_open_fixture)
    _emit("perp", generate_perp_fixture)
    _emit("settlement", generate_settlement_fixture)


if __name__ == "__main__":
    main()
