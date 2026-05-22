"""Tests for ``_amount_in_usd`` scoping fix (VIB-4778 / W1-3).

Verifies that:
- SWAP rows with a stable token_out → return the stable amount as Decimal.
- LP_OPEN rows with USDC amount_out are skipped → return None (NOT 1585552).
- LP_CLOSE rows with a stable token → return None.
- SWAP rows with no stable leg → return None (existing behaviour).
- End-to-end: a mixed ledger of 1 SWAP ($4.50) + 3 LP_OPEN raw-wei rows
  yields ``avg_trade_size_usd < $100``.
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import pytest

from almanak.framework.cli.strat_pnl import _amount_in_usd, compute_pnl_breakdown
from almanak.framework.observability.ledger import LedgerEntry
from almanak.framework.portfolio.models import PortfolioMetrics


# ---------------------------------------------------------------------------
# Minimal stub for a LedgerEntry-shaped object without a full DB round-trip.
# ---------------------------------------------------------------------------


def _make_entry(
    intent_type: str,
    *,
    token_in: str,
    amount_in: str,
    token_out: str,
    amount_out: str,
    chain: str = "arbitrum",
) -> LedgerEntry:
    return LedgerEntry(
        strategy_id="test",
        deployment_id="test:abc123",
        timestamp=datetime.now(UTC),
        intent_type=intent_type,
        token_in=token_in,
        amount_in=amount_in,
        token_out=token_out,
        amount_out=amount_out,
        chain=chain,
        success=True,
    )


# ---------------------------------------------------------------------------
# Unit tests for _amount_in_usd
# ---------------------------------------------------------------------------


def test_swap_with_stable_token_out_returns_amount() -> None:
    """SWAP row with USDC token_out → amount_out returned as Decimal."""
    entry = _make_entry(
        "SWAP",
        token_in="WETH",
        amount_in="0.002125",
        token_out="USDC",
        amount_out="4.50",
    )
    result = _amount_in_usd(entry)
    assert result is not None
    assert result == Decimal("4.50")


def test_swap_with_stable_token_in_returns_amount() -> None:
    """SWAP row with USDC token_in → amount_in returned as Decimal."""
    entry = _make_entry(
        "SWAP",
        token_in="USDC",
        amount_in="100.00",
        token_out="WETH",
        amount_out="0.047",
    )
    result = _amount_in_usd(entry)
    assert result is not None
    assert result == Decimal("100.00")


def test_lp_open_with_stable_amount_out_returns_none() -> None:
    """LP_OPEN row with USDC amount_out=1585552 (raw 6dp) → None.

    This is the core W1-3 regression: the stablecoin heuristic must NOT be
    applied to LP_OPEN rows because amount_in carries raw-wei WETH and
    amount_out carries raw-6dp USDC, not human-form USD values.
    """
    entry = _make_entry(
        "LP_OPEN",
        token_in="WETH",
        amount_in="701279299182337",  # raw wei
        token_out="USDC",
        amount_out="1585552",  # raw 6dp — looks like $1.58M to the old heuristic
    )
    result = _amount_in_usd(entry)
    # Must be None — not Decimal("1585552")
    assert result is None


def test_lp_close_with_stable_token_returns_none() -> None:
    """LP_CLOSE row with stable token → None (not stable amount)."""
    entry = _make_entry(
        "LP_CLOSE",
        token_in="WETH",
        amount_in="500000000000000",  # raw wei
        token_out="USDC",
        amount_out="1810369",  # raw 6dp
    )
    result = _amount_in_usd(entry)
    assert result is None


def test_swap_with_no_stable_leg_returns_none() -> None:
    """SWAP with no stable leg (e.g. WETH → ARB) → None (existing behaviour)."""
    entry = _make_entry(
        "SWAP",
        token_in="WETH",
        amount_in="0.1",
        token_out="ARB",
        amount_out="450",
    )
    result = _amount_in_usd(entry)
    assert result is None


def test_supply_returns_none() -> None:
    """SUPPLY (lending) rows → None."""
    entry = _make_entry(
        "SUPPLY",
        token_in="USDC",
        amount_in="10.00",
        token_out="",
        amount_out="",
    )
    result = _amount_in_usd(entry)
    assert result is None


def test_borrow_returns_none() -> None:
    """BORROW rows → None."""
    entry = _make_entry(
        "BORROW",
        token_in="",
        amount_in="",
        token_out="USDT",
        amount_out="3.11",
    )
    result = _amount_in_usd(entry)
    assert result is None


# ---------------------------------------------------------------------------
# End-to-end: avg_trade_size_usd regression
# ---------------------------------------------------------------------------


def _make_metrics(initial: str = "9.00", total: str = "9.00") -> PortfolioMetrics:
    return PortfolioMetrics(
        strategy_id="test:abc123",
        deployment_id="test:abc123",
        timestamp=datetime.now(UTC),
        total_value_usd=Decimal(total),
        initial_value_usd=Decimal(initial),
        gas_spent_usd=Decimal("0"),
    )


def test_avg_trade_size_usd_not_inflated_by_lp_rows() -> None:
    """Mixed ledger: 1 SWAP @$4.50 + 3 LP_OPEN raw-wei rows → avg < $100.

    Before W1-3 the three LP_OPEN rows contributed $1,585,552 / $1,654,257 /
    $1,810,369 each (raw-6dp USDC amounts misread as human USD), making the
    average $1,262,545.  After the fix all LP rows are skipped and the average
    equals the single SWAP notional of $4.50.
    """
    swap = _make_entry(
        "SWAP",
        token_in="USDC",
        amount_in="4.50",
        token_out="WETH",
        amount_out="0.002125",
    )
    lp1 = _make_entry(
        "LP_OPEN",
        token_in="WETH",
        amount_in="701279299182337",
        token_out="USDC",
        amount_out="1585552",
    )
    lp2 = _make_entry(
        "LP_OPEN",
        token_in="WETH",
        amount_in="731000000000000",
        token_out="USDC",
        amount_out="1654257",
    )
    lp3 = _make_entry(
        "LP_OPEN",
        token_in="WETH",
        amount_in="800000000000000",
        token_out="USDC",
        amount_out="1810369",
    )

    breakdown = compute_pnl_breakdown(
        deployment_id="test:abc123",
        metrics=_make_metrics(),
        ledger_entries=[swap, lp1, lp2, lp3],
        position_events=[],
        snapshot=None,
    )

    assert breakdown.avg_trade_size_usd is not None
    assert breakdown.avg_trade_size_usd < Decimal("100"), (
        f"avg_trade_size_usd={breakdown.avg_trade_size_usd} is unexpectedly large "
        f"— LP raw-wei amounts are leaking into the notional calculation"
    )
    # Specifically: only the SWAP row ($4.50) should contribute
    assert breakdown.avg_trade_size_usd == Decimal("4.50")
