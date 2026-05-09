"""SAME-COMMIT discipline (Hard Ratification Condition #2).

VIB-4162 (T2 D4). The 10 listed paths MUST be touched by the SAME git
commit (the "T2 commit"); the precursor-frozen baseline_pre_T2.json files
must be touched by a DIFFERENT (earlier) commit.

This test detects the T2 commit by searching ``git log --pretty=%s`` for
a conventional-commit subject containing "VIB-4162". When the test runs
during PR development (pre-merge) the T2 commit may not exist yet — the
test skips with a clear reason in that case.
"""

from __future__ import annotations

import json
import subprocess
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parents[3]
_T2_FILES = [
    "almanak/framework/accounting/classifier.py",
    "almanak/framework/observability/position_events.py",
    "almanak/framework/accounting/position_state.py",
    "almanak/framework/teardown/models.py",
    "almanak/framework/accounting/accountant_test.py",
    "almanak/framework/accounting/payload_schemas.py",
    "almanak/framework/accounting/writer.py",
    "tests/fixtures/accounting/lp/expected_cells.json",
    "tests/fixtures/accounting/looping/expected_cells.json",
    "tests/fixtures/accounting/perp/expected_cells.json",
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
    """Find the T2 commit by mechanical signature, not subject heuristics.

    The branch carries TWO VIB-4162 commits — the precursor (chore freeze)
    and T2 itself (the rename). Subject-string matching alone can't tell
    them apart. T2 is uniquely identified by touching
    ``accounting/classifier.py`` (the legacy-frozenset deletion site); the
    precursor does not. Walk the most-recent-first log and pick the first
    VIB-4162 commit that touches that file.
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


def _last_touching_sha(rel_path: str) -> str:
    return _git("log", "-1", "--pretty=format:%H", "--", rel_path)


@pytest.fixture(scope="module")
def t2_sha() -> str:
    sha = _find_t2_sha()
    if not sha:
        pytest.skip(
            "T2 commit not found in git log (pre-merge / branch context). "
            "This test gates the post-merge audit; rerun on main."
        )
    return sha


def test_t2_commit_atomically_touched_all_listed_files(t2_sha: str) -> None:
    """The T2 commit MUST have touched every listed T2 path atomically.

    This is the historical-atomicity property: at T2_SHA, the rename and
    Accountant Test re-baseline landed in the same commit. The earlier
    contract used ``_last_touching_sha`` which forbade ANY later edit to
    these files — including legitimate post-merge correctness fixes
    (CodeRabbit findings, lint cleanup, etc.). That conflated process
    discipline with code-correctness discipline.

    The substantive guarantees — that the T2 commit deletes the legacy
    authoritative sets, that the post-T2 ``expected_cells.json`` files
    carry the per-primitive matching_policy_version field, and that the
    current code state matches the post-T2 contract — are exercised by
    the other tests in this module and by the parity / anti-bypass /
    no-scoring-drift tests. This test gates only the historical
    atomicity at the moment T2 was made.
    """
    files_in_t2 = set(_git("show", "--name-only", "--pretty=", t2_sha).splitlines())
    failures: list[str] = []
    for rel in _T2_FILES:
        if rel not in files_in_t2:
            failures.append(f"{rel} was NOT touched by T2 commit {t2_sha}")
    assert not failures, "\n".join(failures)


def test_t2_commit_deletes_legacy_frozensets(t2_sha: str) -> None:
    diff = _git("show", t2_sha, "--", "almanak/framework/accounting/classifier.py")
    legacy_tokens = (
        "_LP_TYPES",
        "_LENDING_TYPES",
        "_PERP_TYPES",
        "_VAULT_TYPES",
        "_PREDICTION_TYPES",
        "_NO_ACCOUNTING_TYPES",
    )
    failures = []
    for token in legacy_tokens:
        if not any(line.startswith(f"-{token}") for line in diff.splitlines()):
            failures.append(f"T2 commit does not delete legacy set {token}")
    assert not failures, "\n".join(failures)


def test_t2_commit_deletes_intent_to_position_type(t2_sha: str) -> None:
    diff = _git("show", t2_sha, "--", "almanak/framework/observability/position_events.py")
    deletion_present = any(
        line.startswith("-INTENT_TO_POSITION_TYPE") for line in diff.splitlines()
    )
    assert deletion_present, "T2 commit must delete INTENT_TO_POSITION_TYPE"


def test_expected_cells_carry_per_primitive_version(t2_sha: str) -> None:
    """Each post-T2 expected_cells.json declares matching_policy_version."""
    from almanak.framework.accounting.payload_schemas import MATCHING_POLICY_VERSIONS
    from almanak.framework.primitives.types import Primitive

    primitive_map = {"lp": Primitive.LP, "looping": Primitive.LENDING, "perp": Primitive.PERP}
    failures: list[str] = []
    for primitive, member in primitive_map.items():
        path = _REPO_ROOT / "tests" / "fixtures" / "accounting" / primitive / "expected_cells.json"
        data = json.loads(path.read_text())
        actual = data.get("matching_policy_version")
        expected = MATCHING_POLICY_VERSIONS[member]
        if actual != expected:
            failures.append(f"{path}: matching_policy_version={actual}, expected {expected}")
    assert not failures, "\n".join(failures)
