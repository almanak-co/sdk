"""Tests for nightly-test-builds/direct_runner.py — harness status determination.

VIB-3755: the harness wall-clock timeout previously short-circuited every run
that hadn't returned within the budget to ``FAIL / TIMEOUT``, even when the
strategy had successfully submitted on-chain transactions and only the
``iteration_summary`` log line failed to drain before the kill. This produced
false-negative regressions for slow-but-successful runs in the QA April 29
batches.

These tests pin the new behaviour: with ``timed_out=True`` we trust ``tx_hashes``
as a signal of success, while still respecting any explicit error status the
strategy logged before the timeout fired.
"""

from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest


def _load_direct_runner_module():
    repo_root = Path(__file__).resolve().parents[2]
    script_path = repo_root / "nightly-test-builds" / "direct_runner.py"
    spec = importlib.util.spec_from_file_location("direct_runner_module", script_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Failed to load direct_runner from {script_path}")  # noqa: TRY003
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="module")
def direct_runner():
    return _load_direct_runner_module()


# A canonical-looking 32-byte hex transaction hash for fixture data.
_TX_HASH_OK = "0x" + "ab" * 32


# ──────────────────────────────────────────────────────────────────────────
# Pre-VIB-3755 invariants — must keep working
# ──────────────────────────────────────────────────────────────────────────


def test_clean_success_status_line_returns_executed(direct_runner):
    status, outcome = direct_runner.determine_status(
        exit_code=0,
        tx_hashes=[_TX_HASH_OK],
        stdout="Status: SUCCESS | Intent: SWAP | Duration: 1234ms\n",
        stderr="",
        timed_out=False,
    )
    assert (status, outcome) == ("PASS", "EXECUTED")


def test_clean_hold_status_line_returns_hold(direct_runner):
    status, outcome = direct_runner.determine_status(
        exit_code=0,
        tx_hashes=[],
        stdout="Status: HOLD | Intent: HOLD | Duration: 80ms\n",
        stderr="",
        timed_out=False,
    )
    assert (status, outcome) == ("PASS", "HOLD")


def test_execution_failed_returns_error(direct_runner):
    status, outcome = direct_runner.determine_status(
        exit_code=1,
        tx_hashes=[],
        stdout="Status: EXECUTION_FAILED | Intent: SWAP | Error: revert | Duration: 40ms\n",
        stderr="",
        timed_out=False,
    )
    assert (status, outcome) == ("FAIL", "ERROR")


def test_non_zero_exit_with_blocked_pattern(direct_runner):
    status, outcome = direct_runner.determine_status(
        exit_code=2,
        tx_hashes=[],
        stdout="",
        stderr="RiskGuard blocked by leverage cap",
        timed_out=False,
    )
    assert (status, outcome) == ("FAIL", "BLOCKED")


# ──────────────────────────────────────────────────────────────────────────
# VIB-3755 — tx_hash heuristic on timeout
# ──────────────────────────────────────────────────────────────────────────


def test_timeout_without_tx_hashes_is_still_failure(direct_runner):
    """No tx_hashes + timeout = real failure; do not silently flip to PASS."""
    status, outcome = direct_runner.determine_status(
        exit_code=None,
        tx_hashes=[],
        stdout="",
        stderr="",
        timed_out=True,
    )
    assert (status, outcome) == ("FAIL", "TIMEOUT")


def test_timeout_with_tx_hashes_is_executed(direct_runner):
    """VIB-3755: tx_hash present at timeout = the strategy executed."""
    status, outcome = direct_runner.determine_status(
        exit_code=None,
        tx_hashes=[_TX_HASH_OK],
        stdout="tx_hash=" + _TX_HASH_OK[2:] + "\n",
        stderr="",
        timed_out=True,
    )
    assert (status, outcome) == ("PASS", "EXECUTED")


def test_timeout_with_tx_hashes_but_explicit_failure_status_wins(direct_runner):
    """If the strategy logged an explicit failure status before the timeout,
    we trust that signal even when tx_hashes were emitted earlier in the run."""
    status, outcome = direct_runner.determine_status(
        exit_code=None,
        tx_hashes=[_TX_HASH_OK],
        stdout=(
            "tx_hash=" + _TX_HASH_OK[2:] + "\n"
            "Status: EXECUTION_FAILED | Intent: SWAP | Error: simulator timeout | Duration: 40ms\n"
        ),
        stderr="",
        timed_out=True,
    )
    assert (status, outcome) == ("FAIL", "ERROR")


def test_timeout_with_compilation_failure_is_error(direct_runner):
    status, outcome = direct_runner.determine_status(
        exit_code=None,
        tx_hashes=[],
        stdout="Status: COMPILATION_FAILED | Intent: SWAP | Error: bad pool | Duration: 12ms\n",
        stderr="",
        timed_out=True,
    )
    assert (status, outcome) == ("FAIL", "ERROR")


def test_timeout_with_multiple_tx_hashes_is_executed(direct_runner):
    """Lifecycle strategies submit multiple txs; any tx_hash + timeout = PASS."""
    status, outcome = direct_runner.determine_status(
        exit_code=None,
        tx_hashes=[_TX_HASH_OK, "0x" + "cd" * 32],
        stdout="",
        stderr="",
        timed_out=True,
    )
    assert (status, outcome) == ("PASS", "EXECUTED")


# ──────────────────────────────────────────────────────────────────────────
# VIB-3755 — failure-precedence on timeout (Codex P2 + pr-auditor Important #1)
# ──────────────────────────────────────────────────────────────────────────


def test_timeout_with_tx_hash_then_revert_log_is_failure(direct_runner):
    """Submitted-then-reverted: tx_hash present + ``Transaction reverted: tx_hash=``
    log line in stdout must classify FAIL, not silently flip to PASS.

    Without this guard, a strategy whose tx submits successfully but reverts
    on-chain (then spins/retries until wall-clock timeout) would be marked
    PASS/EXECUTED — hiding a real regression in nightly runs.
    """
    status, outcome = direct_runner.determine_status(
        exit_code=None,
        tx_hashes=[_TX_HASH_OK],
        stdout=(
            f"Transaction submitted: tx_hash={_TX_HASH_OK[2:]}, latency=1500ms\n"
            f"Transaction reverted: tx_hash={_TX_HASH_OK[2:]}, reason=ERC20: insufficient allowance\n"
        ),
        stderr="",
        timed_out=True,
    )
    assert (status, outcome) == ("FAIL", "ERROR")


def test_timeout_with_transaction_not_found_log_is_failure(direct_runner):
    """``Transaction not found: tx_hash=`` is a hard failure signal even with
    other tx_hashes in the run (the submitter logs this when receipt
    polling exhausts retries)."""
    status, outcome = direct_runner.determine_status(
        exit_code=None,
        tx_hashes=[_TX_HASH_OK],
        stdout="",
        stderr=f"Transaction not found: tx_hash={_TX_HASH_OK[2:]}\n",
        timed_out=True,
    )
    assert (status, outcome) == ("FAIL", "ERROR")


def test_timeout_with_transaction_reverted_error_traceback_is_failure(direct_runner):
    """Tracebacks containing ``TransactionRevertedError`` (the exception class
    name) also count as a hard failure signal, since the exception fires
    when the submitter raises after a confirmed revert."""
    status, outcome = direct_runner.determine_status(
        exit_code=None,
        tx_hashes=[_TX_HASH_OK],
        stdout="",
        stderr=(
            "Traceback (most recent call last):\n"
            "  File \"submitter/public.py\", line 1018, in _confirm\n"
            "    raise TransactionRevertedError(tx_hash=...)\n"
            "TransactionRevertedError: tx reverted on chain\n"
        ),
        timed_out=True,
    )
    assert (status, outcome) == ("FAIL", "ERROR")


def test_timeout_with_iteration_failed_log_is_failure(direct_runner):
    """``Iteration failed: ...`` is parsed by ``_parse_status_line`` as an
    error message even when no formal ``Status:`` line was emitted. The
    timeout heuristic must honor it (Codex P2 finding)."""
    status, outcome = direct_runner.determine_status(
        exit_code=None,
        tx_hashes=[_TX_HASH_OK],
        stdout=(
            f"Transaction submitted: tx_hash={_TX_HASH_OK[2:]}, latency=900ms\n"
            "Iteration failed: state machine wedged in COMPILE\n"
        ),
        stderr="",
        timed_out=True,
    )
    assert (status, outcome) == ("FAIL", "ERROR")


def test_tx_failure_log_markers_are_documented_and_complete(direct_runner):
    """Pin the exact set of failure-log markers the heuristic recognises so
    a future edit cannot accidentally drop one. If you add a new submitter
    error log line, update both the constant and this test."""
    assert direct_runner._TX_FAILURE_LOG_MARKERS == (
        "Transaction reverted: tx_hash=",
        "Transaction not found: tx_hash=",
        "TransactionRevertedError",
    )


# ──────────────────────────────────────────────────────────────────────────
# tx_hash extraction (sanity)
# ──────────────────────────────────────────────────────────────────────────


def test_extract_tx_hashes_normalizes_prefix(direct_runner):
    raw = "submitted tx_hash=" + ("ab" * 32) + " ok"
    hashes = direct_runner.extract_tx_hashes(raw)
    assert hashes == ["0x" + "ab" * 32]


def test_extract_tx_hashes_dedupes_in_order(direct_runner):
    h1 = "ab" * 32
    h2 = "cd" * 32
    raw = f"tx_hash={h1} ... tx_hash={h2} ... tx_hash={h1}"
    hashes = direct_runner.extract_tx_hashes(raw)
    assert hashes == ["0x" + h1, "0x" + h2]
