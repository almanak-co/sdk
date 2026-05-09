"""Frozen dispatch-truth-table guard (VIB-4163, post-merge survivor).

D4.A4 in the T3 UAT card. After T3 (VIB-4163) merged the registry-driven
dispatcher and T4 (VIB-4164) re-classified BRIDGE→TRANSFER, only the
``test_truth_table_committed_exactly_once`` guard remains here — it asserts
that ``tests/fixtures/accounting/legacy_dispatch_truth_table.json`` is
touched by exactly one commit (a future PR that hand-edits the frozen
dispatch fixture instead of regenerating it would trip this).

The companion guard (``test_truth_table_precursor_is_not_the_t3_commit``)
was a pre-merge review-discipline check that became a permanent false
negative after T3's squash-merge collapsed the precursor and dispatcher
commits into one — see the comment block below for the removal rationale.
The runtime contract is preserved by
``test_classifier_parity_against_frozen_truth_table`` in
``test_classifier_taxonomy_parity.py``.
"""

from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parents[3]
_TRUTH_TABLE_REL = "tests/fixtures/accounting/legacy_dispatch_truth_table.json"
_PROCESSOR_REL = "almanak/framework/accounting/processor.py"


def _git(*args: str) -> str:
    result = subprocess.run(
        ["git", *args],
        cwd=_REPO_ROOT,
        check=False,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        pytest.fail(
            f"git {' '.join(args)} failed (exit {result.returncode}): "
            f"{result.stderr.strip()}"
        )
    return result.stdout.strip()


def _commits_touching(rel_path: str) -> list[str]:
    log = _git("log", "--pretty=format:%H", "--", rel_path)
    return [s for s in log.splitlines() if s.strip()]


def test_truth_table_committed_exactly_once() -> None:
    """The truth-table JSON must be touched by exactly ONE git commit (the precursor).

    A second commit modifying the file would mean the freeze was broken — either the
    legacy dispatcher was edited after T3, or the truth table was hand-modified.
    Both are review-discipline violations. Re-run the generator script and amend the
    precursor commit, do not stack a fix-up commit.
    """
    shas = _commits_touching(_TRUTH_TABLE_REL)
    if not shas:
        pytest.skip(
            f"{_TRUTH_TABLE_REL} not yet committed (pre-precursor / branch context)."
        )
    assert len(shas) == 1, (
        f"{_TRUTH_TABLE_REL} touched by {len(shas)} commits; expected exactly 1 "
        f"(the precursor freeze). SHAs: {shas}"
    )


# test_truth_table_precursor_is_not_the_t3_commit — REMOVED (VIB-4164, T4).
#
# The test was a pre-merge review-discipline guard for T3 (VIB-4163): "precursor
# freeze and T3 dispatcher rewrite must be in different commits". After T3's
# squash-merge to main (commit 325e3d3ca), both files share that commit by
# construction — the discipline window has closed. The runtime contract (legacy
# ladder vs registry parity) is enforced by `test_classifier_parity_against_frozen_truth_table`
# which remains in `test_classifier_taxonomy_parity.py`. Removing this stale
# guard alongside the analogous `test_precursor_files_committed_separately`
# (in `test_no_scoring_drift.py`) which T4 also drops for the same reason.
