"""FROZEN-PRE-T3 truth-table commit discipline (VIB-4163).

D4.A4 in the UAT card. The frozen dispatch truth table at
``tests/fixtures/accounting/legacy_dispatch_truth_table.json`` MUST be touched by
exactly ONE git commit (the precursor commit), and that commit must NOT be the same
as the one that touches ``almanak/framework/accounting/processor.py`` to introduce
the registry-driven dispatch (the T3 commit).

The two-commit shape mirrors T2's ``test_same_commit_discipline.py``: the truth
table is captured against the LEGACY if-ladder before T3 changes anything, then
the T3 commit rewrites the dispatcher and adds a parity test that compares the
new dispatch against the frozen JSON.

This test skips on branches where the precursor commit doesn't exist yet (e.g.
during PR development before the precursor lands).
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


def test_truth_table_precursor_is_not_the_t3_commit() -> None:
    """The truth-table commit must precede the T3 dispatcher rewrite.

    Identifying the T3 commit by the same mechanical signature used in T2's
    ``test_same_commit_discipline._find_t2_sha``: walk the log most-recent-first,
    pick the first commit whose subject contains "VIB-4163" and which touched
    ``processor.py``. If none, skip (pre-T3 / branch context).
    """
    truth_shas = _commits_touching(_TRUTH_TABLE_REL)
    if not truth_shas:
        pytest.skip(f"{_TRUTH_TABLE_REL} not yet committed.")
    truth_sha = truth_shas[0]

    log = _git("log", "--pretty=%H %s")
    t3_sha = None
    for line in log.splitlines():
        if not line.strip():
            continue
        sha, subject = line.split(" ", 1)
        if "VIB-4163" not in subject:
            continue
        files = _git("show", "--name-only", "--pretty=", sha).splitlines()
        if _PROCESSOR_REL in files:
            t3_sha = sha
            break

    if t3_sha is None:
        pytest.skip(
            "T3 commit (touching processor.py with VIB-4163 subject) not yet on this branch."
        )

    assert truth_sha != t3_sha, (
        f"truth table and T3 dispatcher rewrite share commit {t3_sha}; "
        "the precursor freeze MUST land in a separate earlier commit."
    )
