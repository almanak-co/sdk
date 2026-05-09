"""Freeze the pre-T2 Accountant Test cell-status baselines (VIB-4162 precursor).

Run ONCE on the precursor commit (pre-T2 source code) to freeze the
baseline cell statuses for each primitive. The post-T2 ``test_no_scoring_drift``
test loads these JSON files and asserts no cell regressed (PASS > XFAIL >
SKIP > FAIL — see ``test_no_scoring_drift.py`` for the rank).

Three artifacts are produced (one per primitive):

* ``tests/fixtures/accounting/lp/baseline_pre_T2.json``
* ``tests/fixtures/accounting/looping/baseline_pre_T2.json``
* ``tests/fixtures/accounting/perp/baseline_pre_T2.json``

Each is shaped ``{cell_id: status}`` covering the ~21 cells the Accountant
Test scores. At precursor time the writer's ``MATCHING_POLICY_VERSION`` is
the global v3 — the fixture generator stamps v3 globally on every accounting
event, mirroring pre-T2 production behaviour.
"""

from __future__ import annotations

import json
import sys
import tempfile
from pathlib import Path

# Allow running this script directly without installing tests as a package.
sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from almanak.framework.accounting.accountant_test import run_against_sqlite  # noqa: E402
from tests.fixtures.accounting._generate_baselines import (  # noqa: E402
    generate_lp_fixture,
    generate_looping_fixture,
    generate_perp_fixture,
)


def _freeze_one(primitive: str, generator) -> dict[str, str]:
    """Generate a synthetic fixture, run the Accountant Test, return cell statuses."""
    with tempfile.TemporaryDirectory() as td:
        db_path = Path(td) / f"{primitive}.sqlite"
        generator(db_path)
        report = run_against_sqlite(db_path, primitive=primitive)
        return {c.cell_id: c.status for c in report.cells}


def main() -> None:
    base = Path(__file__).parent
    pairs = (
        ("lp", generate_lp_fixture),
        ("looping", generate_looping_fixture),
        ("perp", generate_perp_fixture),
    )
    for primitive, generator in pairs:
        out_dir = base / primitive
        out_dir.mkdir(parents=True, exist_ok=True)
        cells = _freeze_one(primitive, generator)
        out_path = out_dir / "baseline_pre_T2.json"
        out_path.write_text(json.dumps(cells, indent=2, sort_keys=True) + "\n")


if __name__ == "__main__":
    main()
