"""D4 — no scoring drift between pre-T2 baseline and post-T2 expected cells.

VIB-4162. Loads ``baseline_pre_T2.json`` (precursor-frozen) and
``expected_cells.json`` (post-T2) for each primitive, asserts no cell
regressed: STATUS_RANK[post] >= STATUS_RANK[pre] where rank is
``{"PASS": 3, "XFAIL": 2, "SKIP": 1, "FAIL": 0}``.

(VIB-4164 removed the companion git-discipline test — see the post-mortem
comment at the bottom of this file. The constants/helpers it relied on were
removed alongside.)
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parents[3]

STATUS_RANK = {"PASS": 3, "XFAIL": 2, "SKIP": 1, "FAIL": 0}


@pytest.mark.parametrize("primitive", ["lp", "looping", "perp"])
def test_no_cell_regressed(primitive: str) -> None:
    pre_path = _REPO_ROOT / "tests" / "fixtures" / "accounting" / primitive / "baseline_pre_T2.json"
    post_path = _REPO_ROOT / "tests" / "fixtures" / "accounting" / primitive / "expected_cells.json"
    pre = json.loads(pre_path.read_text())
    post_doc = json.loads(post_path.read_text())
    post = post_doc["cells"]

    failures: list[str] = []
    for cell_id, pre_status in pre.items():
        post_status = post.get(cell_id)
        if post_status is None:
            failures.append(f"{primitive}.{cell_id}: cell missing post-T2")
            continue
        if STATUS_RANK[post_status] < STATUS_RANK[pre_status]:
            failures.append(f"{primitive}.{cell_id}: regressed {pre_status} → {post_status}")
    assert not failures, "\n".join(failures)


# test_precursor_files_committed_separately — REMOVED (VIB-4164, T4).
#
# The test was a pre-merge review-discipline guard for T2 (VIB-4162): each
# `_PRECURSOR_FROZEN_FILES` entry must be touched by exactly one commit, and
# that commit must NOT be the T2 SHA. After T2's squash-merge (`2eda14c70`),
# both files share that commit by construction — the test became a permanent
# false negative on main. T4 (VIB-4164) additionally re-runs
# `_freeze_legacy_routing_truth_tables.py` to flip the BRIDGE rows from
# `no_accounting` to `transfer` (the entire point of T4), giving the file a
# second commit, which would have tripped the "edited after freeze" branch
# even on a clean main. The runtime contract is preserved by
# `test_classifier_parity_against_frozen_truth_table` (delegation parity) and
# `test_no_cell_regressed` (status-rank monotonicity). Removing this stale
# guard alongside the analogous `test_truth_table_precursor_is_not_the_t3_commit`
# (in `test_dispatch_truth_table_freeze_discipline.py`) which T4 also drops
# for the same reason.
