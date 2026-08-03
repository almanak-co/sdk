"""Stamp numeric floors into the frozen Accountant-Test manifests (VIB-4226 §1a).

The status ratchet in ``scripts/ci/check_accounting_ratchet.py`` compares an enum
and nothing else. G6 — the reconciliation cell — sits at ``FAIL`` on most frozen
fixtures, and ``FAIL`` is the bottom of the partial order, so a reconciliation gap
can grow without bound and the gate stays green. This script records the CURRENT
gap for each floored cell into that fixture's ``expected_cells.json`` under
``cell_metrics``, giving the gate a magnitude to defend::

    "cell_metrics": {"G6": {"gap_usd": "10.0"}}

Unlike ``_generate_post_t2_baselines.py`` this script **never rewrites a fixture
DB**. Several fixtures (pendle_*, lp_curve*) are captured from real forked-mainnet
runs and are not reproducible from code; re-deriving them would destroy evidence.
Stamping reads the frozen DB and edits only the manifest JSON.

Usage::

    uv run python tests/fixtures/accounting/_stamp_cell_metrics.py           # write
    uv run python tests/fixtures/accounting/_stamp_cell_metrics.py --dry-run # show
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(_REPO_ROOT))

from scripts.ci.check_accounting_ratchet import (  # noqa: E402
    _FIXTURE_SCORING_PROFILE,
    PRIMITIVES,
)

_FIXTURE_BASE = _REPO_ROOT / "tests" / "fixtures" / "accounting"

# Which (cell, metric) pairs carry a numeric floor. Deliberately narrow: a live
# decomposition holds dozens of diagnostic numbers, and flooring all of them
# would turn every incidental diagnostic change into a gate failure. Every key
# here must also be declared in ``_METRIC_DIRECTION`` in the gate, which fails
# closed on an undeclared metric.
_FLOORED: dict[str, tuple[str, ...]] = {
    "G6": ("gap_usd",),
}


def extract_cell_metrics(report) -> dict[str, dict[str, str]]:
    """Build the ``cell_metrics`` block from an already-scored report.

    Public because ``_generate_post_t2_baselines.py`` calls it too: that script
    rewrites ``expected_cells.json`` wholesale, so without sharing this it would
    silently DROP every numeric floor the next time anyone regenerated the
    baselines — and the gate's own remedy message tells operators to run it.
    One definition of "which metrics are floored", used by both writers.
    """
    out: dict[str, dict[str, str]] = {}
    for cell in report.cells:
        wanted = _FLOORED.get(cell.cell_id)
        if not wanted:
            continue
        decomp = cell.decomposition or {}
        floors = {}
        for metric in wanted:
            raw = decomp.get(metric)
            if raw is None or raw == "":
                # Empty != Zero: a cell that did not report the metric gets NO
                # floor rather than a fabricated 0, which would be an
                # unsatisfiable floor the very next run.
                continue
            floors[metric] = str(raw)
        if floors:
            out[cell.cell_id] = floors
    return out


def _stamp(primitive: str) -> dict[str, dict[str, str]]:
    """Score one frozen fixture and return its ``cell_metrics`` block."""
    from almanak.framework.accounting.accountant_test import run_against_sqlite

    db_path = _FIXTURE_BASE / primitive / "expected_baseline.sqlite"
    if not db_path.is_file():
        raise SystemExit(f"missing frozen fixture DB: {db_path}")
    profile = _FIXTURE_SCORING_PROFILE.get(primitive, primitive)
    report = run_against_sqlite(db_path, primitive=profile, strict_lifecycle=True)
    return extract_cell_metrics(report)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="stamp_cell_metrics")
    parser.add_argument("--dry-run", action="store_true", help="Print, do not write.")
    parser.add_argument(
        "--primitive", choices=PRIMITIVES, action="append", help="Limit to primitive(s). Repeatable."
    )
    args = parser.parse_args(argv)
    primitives = tuple(args.primitive) if args.primitive else PRIMITIVES

    for primitive in primitives:
        manifest_path = _FIXTURE_BASE / primitive / "expected_cells.json"
        if not manifest_path.is_file():
            raise SystemExit(f"missing manifest: {manifest_path}")
        doc = json.loads(manifest_path.read_text(encoding="utf-8"))
        metrics = _stamp(primitive)
        before = doc.get("cell_metrics")
        # KNOWN LIMITATION (VIB-6414): this write is unconditional, so re-stamping
        # after a regression replaces a good floor with the worse live value and the
        # gate then reports EXPECTED. The regression is still caught on the run that
        # introduces it — escaping needs a deliberate re-stamp plus a commit of a
        # visibly worse number — but there is no forward-only guard yet. The status
        # ratchet has only a PARTIAL analogue (test_no_scoring_drift.py covers 3 of
        # the 9 fixtures — the only ones with a baseline_pre_T2.json — and compares
        # against a static VIB-4162 precursor, not the previously committed value),
        # so metrics cannot simply copy it: all 9 fixtures now carry a floor. That
        # needs a committed reference floor, which is a design decision rather than
        # a line of code. Tracked in VIB-6414.
        doc["cell_metrics"] = metrics
        rendered = ", ".join(f"{c}.{m}={v}" for c, mv in metrics.items() for m, v in mv.items()) or "(none)"
        change = "" if before == metrics else "  <-- CHANGED"
        print(f"  {primitive:<20s} {rendered}{change}")
        if not args.dry_run:
            # Preserve the file's existing key order and its non-ASCII text.
            # ``sort_keys`` would reorder keys these manifests carry in a
            # hand-maintained order, and ``ensure_ascii`` would rewrite the
            # em-dashes in ``_notes`` — both are churn in someone else's
            # evidence, unrelated to adding a floor.
            manifest_path.write_text(
                json.dumps(doc, indent=2, ensure_ascii=False) + "\n", encoding="utf-8"
            )

    if args.dry_run:
        print("\n(dry run — nothing written)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
