"""VIB-3888 — symmetric ``post_state.captured_at`` writer.

Pre-VIB-3888 the runner's ledger row consistently had a populated
``pre_state.captured_at`` and an empty ``post_state.captured_at``.
The asymmetry was a write-side bug: the reconciliation report only
serialized ``post_balances`` without the timestamp. Per-intent G6
reconciliation (Accountant Test) needs both timestamps to bracket
the price observation window, and PRD-1 (block-anchored balance
reads, VIB-3350) is harder without it.

These tests fence:
1. ``ReconciliationReport.to_dict()`` propagates pre/post timestamps.
2. ``_build_post_state_for_ledger`` always emits a non-empty
   ``captured_at`` — either from the recon, or via the fallback.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from decimal import Decimal

from almanak.framework.runner.reconciliation import (
    BalanceSnapshot,
    ReconciliationReport,
    build_reconciliation_report,
)
from almanak.framework.runner.strategy_runner import _build_post_state_for_ledger


# ──────────────────────────────────────────────────────────────────────────
# ReconciliationReport.to_dict — timestamps propagate
# ──────────────────────────────────────────────────────────────────────────


def test_to_dict_includes_post_timestamp():
    pre_ts = datetime(2026, 5, 2, 11, 9, 19, tzinfo=UTC)
    post_ts = datetime(2026, 5, 2, 11, 9, 21, tzinfo=UTC)
    report = ReconciliationReport(
        tokens_checked=["USDC"],
        pre_balances={"USDC": Decimal("100")},
        post_balances={"USDC": Decimal("99")},
        actual_deltas={"USDC": Decimal("-1")},
        expected_ranges={},
        mismatches=[],
        warnings=[],
        incident=False,
        enforced=False,
        pre_timestamp=pre_ts,
        post_timestamp=post_ts,
    )
    d = report.to_dict()
    assert d["pre_timestamp"] == pre_ts.isoformat()
    assert d["post_timestamp"] == post_ts.isoformat()


def test_to_dict_emits_empty_strings_when_timestamps_none():
    """Defensive: missing timestamps serialize as empty strings, not crash."""
    report = ReconciliationReport(
        tokens_checked=[],
        pre_balances={},
        post_balances={},
        actual_deltas={},
        expected_ranges={},
        mismatches=[],
        warnings=[],
        incident=False,
        enforced=False,
    )
    d = report.to_dict()
    assert d["pre_timestamp"] == ""
    assert d["post_timestamp"] == ""


def test_build_reconciliation_report_propagates_timestamps():
    """Integration: the build helper inherits timestamps from the snapshots."""
    pre_ts = datetime(2026, 5, 2, 11, 9, 19, tzinfo=UTC)
    post_ts = datetime(2026, 5, 2, 11, 9, 21, tzinfo=UTC)
    pre = BalanceSnapshot(timestamp=pre_ts, balances={"USDC": Decimal("100")})
    post = BalanceSnapshot(timestamp=post_ts, balances={"USDC": Decimal("99")})

    report = build_reconciliation_report(
        pre=pre, post=post, intent=None, execution_result=None
    )
    assert report.pre_timestamp == pre_ts
    assert report.post_timestamp == post_ts


# ──────────────────────────────────────────────────────────────────────────
# _build_post_state_for_ledger — captured_at always populated
# ──────────────────────────────────────────────────────────────────────────


def test_post_state_captured_at_uses_recon_timestamp():
    """Happy path: recon supplies post_timestamp, builder propagates it."""
    expected = "2026-05-02T11:09:21+00:00"
    recon = {
        "post_balances": {"USDC": "99"},
        "post_timestamp": expected,
        "incident": False,
    }
    state = _build_post_state_for_ledger(recon)
    assert state is not None
    assert state["captured_at"] == expected


def test_post_state_captured_at_falls_back_to_now_when_missing():
    """Legacy recon (pre-VIB-3888) → builder stamps now() rather than empty."""
    recon = {
        "post_balances": {"USDC": "99"},
        # post_timestamp deliberately omitted
        "incident": False,
    }
    before = datetime.now(UTC)
    state = _build_post_state_for_ledger(recon)
    after = datetime.now(UTC)

    assert state is not None
    assert state["captured_at"], (
        "VIB-3888: post_state.captured_at must NEVER be empty. The "
        "fallback stamp datetime.now(UTC) is closer to truth than NULL."
    )
    # Sanity: stamped time falls inside the test's measurement window.
    stamped = datetime.fromisoformat(state["captured_at"])
    assert before <= stamped <= after


def test_post_state_returns_none_without_post_balances():
    """No balance query result → no post_state. Honest absence."""
    recon = {"post_timestamp": "2026-05-02T11:09:21+00:00", "incident": False}
    assert _build_post_state_for_ledger(recon) is None


def test_post_state_returns_none_when_recon_empty():
    assert _build_post_state_for_ledger(None) is None
    assert _build_post_state_for_ledger({}) is None


def test_post_state_serialises_to_json_with_captured_at():
    """End-to-end: the dict round-trips through JSON cleanly (it lands on
    transaction_ledger.post_state_json as a JSON string)."""
    recon = {
        "post_balances": {"USDC": "99", "WETH": "0.5"},
        "post_timestamp": "2026-05-02T11:09:21+00:00",
        "incident": False,
    }
    state = _build_post_state_for_ledger(recon)
    encoded = json.dumps(state)
    decoded = json.loads(encoded)
    assert decoded["captured_at"] == "2026-05-02T11:09:21+00:00"
    assert decoded["wallet_balances"] == {"USDC": "99", "WETH": "0.5"}
