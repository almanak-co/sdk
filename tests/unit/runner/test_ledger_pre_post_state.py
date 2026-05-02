"""Tests for the pre_state / post_state ledger wiring
(Accounting-AttemptNo17 §A4 — VIB-3480 columns finally populated).

Until this landed, ``transaction_ledger.pre_state_json`` and
``post_state_json`` were NULL on every ledger row even though the
runner had pre/post wallet balance observations in scope.
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from types import SimpleNamespace

from almanak.framework.runner.strategy_runner import (
    _build_post_state_for_ledger,
    _build_pre_state_for_ledger,
)


def _balance_snapshot(balances: dict[str, Decimal] | None = None) -> SimpleNamespace:
    return SimpleNamespace(
        balances=balances or {},
        timestamp=datetime(2026, 5, 1, 12, 0, 0, tzinfo=UTC),
    )


def test_pre_state_returns_none_when_snapshot_missing():
    assert _build_pre_state_for_ledger(None) is None


def test_pre_state_returns_none_when_balances_empty():
    assert _build_pre_state_for_ledger(_balance_snapshot({})) is None


def test_pre_state_serialises_balances_as_strings():
    snap = _balance_snapshot({"USDC": Decimal("19.0"), "WETH": Decimal("0.001234")})
    out = _build_pre_state_for_ledger(snap)
    assert out is not None
    assert out["wallet_balances"] == {"USDC": "19.0", "WETH": "0.001234"}
    assert out["captured_at"] == "2026-05-01T12:00:00+00:00"
    assert out["source"] == "balance_provider"


def test_post_state_returns_none_when_recon_missing():
    assert _build_post_state_for_ledger(None) is None


def test_post_state_returns_none_when_no_post_balances():
    recon = {"tokens_checked": ["USDC"], "warnings": []}  # no post_balances
    assert _build_post_state_for_ledger(recon) is None


def test_post_state_pulls_from_recon():
    recon = {
        "tokens_checked": ["USDC", "WETH"],
        "post_balances": {"USDC": "20.5", "WETH": "0.001"},
        "post_timestamp": "2026-05-01T12:00:30+00:00",
        "incident": False,
    }
    out = _build_post_state_for_ledger(recon)
    assert out is not None
    assert out["wallet_balances"] == {"USDC": "20.5", "WETH": "0.001"}
    assert out["captured_at"] == "2026-05-01T12:00:30+00:00"
    assert out["source"] == "balance_provider"
    assert out["incident"] is False


def test_post_state_threads_incident_flag_for_recon_failure_path():
    # VIB-3480 use case: an incident row should still carry post-state so
    # auditors can see the on-chain state at the time of the breach.
    recon = {
        "post_balances": {"USDC": "0"},
        "incident": True,
        "mismatches": ["something"],
    }
    out = _build_post_state_for_ledger(recon)
    assert out is not None
    assert out["incident"] is True


def test_post_state_handles_missing_post_timestamp():
    recon = {"post_balances": {"USDC": "20.5"}}
    out = _build_post_state_for_ledger(recon)
    assert out is not None
    assert out["captured_at"] == ""
