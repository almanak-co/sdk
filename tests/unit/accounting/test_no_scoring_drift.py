"""D4 — no scoring drift between pre-T2 baseline and post-T2 expected cells.

VIB-4162. Loads ``baseline_pre_T2.json`` (precursor-frozen) and
``expected_cells.json`` (post-T2) for each primitive, asserts:

* No cell regressed: STATUS_RANK[post] >= STATUS_RANK[pre] where rank is
  ``{"PASS": 3, "XFAIL": 2, "SKIP": 1, "FAIL": 0}``.
* Each precursor-frozen file has been touched by EXACTLY ONE git commit,
  and that SHA is NOT the T2 SHA (skipped when no T2 SHA exists).
"""

from __future__ import annotations

import json
import subprocess
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parents[3]

STATUS_RANK = {"PASS": 3, "XFAIL": 2, "SKIP": 1, "FAIL": 0}

_PRECURSOR_FROZEN_FILES = [
    *[f"tests/fixtures/accounting/{p}/baseline_pre_T2.json" for p in ("lp", "looping", "perp")],
    "tests/fixtures/accounting/legacy_classifier_truth_table.json",
    "tests/fixtures/accounting/legacy_position_type_truth_table.json",
    "tests/fixtures/accounting/legacy_lifecycle_truth_table.json",
]


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


def _find_t2_sha() -> str | None:
    """Find the T2 commit by file-touch signature, not subject heuristics.

    See ``test_same_commit_discipline._find_t2_sha`` — same rationale: the
    branch carries two VIB-4162 commits and T2 is the one that touches
    ``accounting/classifier.py``.
    """
    log = _git("log", "--pretty=%H %s")
    for line in log.splitlines():
        if not line.strip():
            continue
        sha, subject = line.split(" ", 1)
        if "VIB-4162" not in subject:
            continue
        files = _git("show", "--name-only", "--pretty=", sha).splitlines()
        if "almanak/framework/accounting/classifier.py" in files:
            return sha
    return None


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


def test_precursor_files_committed_separately() -> None:
    t2_sha = _find_t2_sha()
    if not t2_sha:
        pytest.skip("T2 commit not found in git log (pre-merge / branch context).")

    failures: list[str] = []
    for rel in _PRECURSOR_FROZEN_FILES:
        sha_log = _git("log", "--pretty=format:%H", "--", rel)
        shas = [s for s in sha_log.splitlines() if s.strip()]
        if not shas:
            failures.append(f"{rel}: no commits touched the file")
            continue
        if len(shas) != 1:
            failures.append(f"{rel}: edited after freeze ({len(shas)} commits)")
            continue
        if shas[0] == t2_sha:
            failures.append(f"{rel}: co-committed with T2 SHA {t2_sha} (must be precursor)")
    assert not failures, "\n".join(failures)
