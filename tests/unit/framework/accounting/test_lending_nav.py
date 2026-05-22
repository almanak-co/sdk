"""Unit tests for the lending NAV helper (W1-1, VIB-4776).

Coverage:
  1. PRD acceptance test: SUPPLY+BORROW+LP snapshot → correct net/carry.
  2. Empty snapshot (None) → all-zeros LendingNAVSummary.
  3. Only SUPPLY positions → borrow fields zero.
  4. Only BORROW positions → supply fields zero.
  5. value_usd=None on one leg → that leg skipped gracefully.
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

import pytest

from almanak.framework.accounting.lending_nav import LendingNAVSummary, compute_lending_nav
from almanak.framework.portfolio.models import PortfolioSnapshot, PositionValue, ValueConfidence
from almanak.framework.teardown.models import PositionType


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _snapshot(*positions: PositionValue) -> PortfolioSnapshot:
    """Build a minimal PortfolioSnapshot with the given positions."""
    return PortfolioSnapshot(
        timestamp=datetime.now(UTC),
        strategy_id="test:nav",
        total_value_usd=Decimal("0"),
        available_cash_usd=Decimal("0"),
        value_confidence=ValueConfidence.HIGH,
        positions=list(positions),
    )


def _supply(value_usd: str, unrealized: str = "0") -> PositionValue:
    return PositionValue(
        position_type=PositionType.SUPPLY,
        protocol="aave_v3",
        chain="arbitrum",
        value_usd=Decimal(value_usd),
        label="USDC Supply",
        unrealized_pnl_usd=Decimal(unrealized),
    )


def _borrow(value_usd: str, unrealized: str = "0") -> PositionValue:
    """BORROW positions carry negative value_usd (liability convention)."""
    return PositionValue(
        position_type=PositionType.BORROW,
        protocol="aave_v3",
        chain="arbitrum",
        value_usd=Decimal(value_usd),
        label="USDT Borrow",
        unrealized_pnl_usd=Decimal(unrealized),
    )


def _lp_pos() -> PositionValue:
    """LP position — must be ignored by compute_lending_nav."""
    return PositionValue(
        position_type=PositionType.LP,
        protocol="uniswap_v3",
        chain="arbitrum",
        value_usd=Decimal("99.99"),
        label="WETH/USDC LP",
        unrealized_pnl_usd=Decimal("5.00"),
    )


# ---------------------------------------------------------------------------
# Test 1: PRD acceptance test (App. A §Tests #1)
# ---------------------------------------------------------------------------


class TestPrdAcceptanceCase:
    """Snapshot with SUPPLY +10.37 / BORROW -3.11 / LP → net ≈ 7.26, carry ≈ +0.000332."""

    def test_net_lending_value_usd(self):
        snap = _snapshot(
            _supply("10.3746640", unrealized="0.0005280"),
            _borrow("-3.1124370", unrealized="-0.0001960"),
            _lp_pos(),
        )
        nav = compute_lending_nav(snap)
        # net = 10.3746640 − 3.1124370 = 7.2622270
        assert nav.net_lending_value_usd == pytest.approx(Decimal("7.2622270"), rel=Decimal("1e-6"))

    def test_gross_supply(self):
        snap = _snapshot(
            _supply("10.3746640", unrealized="0.0005280"),
            _borrow("-3.1124370", unrealized="-0.0001960"),
        )
        nav = compute_lending_nav(snap)
        assert nav.gross_supply_value_usd == Decimal("10.3746640")

    def test_gross_debt_is_abs_of_borrow_value(self):
        snap = _snapshot(
            _supply("10.3746640", unrealized="0.0005280"),
            _borrow("-3.1124370", unrealized="-0.0001960"),
        )
        nav = compute_lending_nav(snap)
        assert nav.gross_debt_value_usd == Decimal("3.1124370")

    def test_net_unrealized_carry(self):
        snap = _snapshot(
            _supply("10.3746640", unrealized="0.0005280"),
            _borrow("-3.1124370", unrealized="-0.0001960"),
        )
        nav = compute_lending_nav(snap)
        # 0.0005280 + (-0.0001960) = 0.0003320
        assert nav.net_unrealized_carry_usd == pytest.approx(
            Decimal("0.0003320"), rel=Decimal("1e-6")
        )

    def test_lp_position_ignored(self):
        # LP positions must not influence lending NAV counts or values.
        snap_with_lp = _snapshot(
            _supply("10.3746640", unrealized="0.0005280"),
            _borrow("-3.1124370", unrealized="-0.0001960"),
            _lp_pos(),
        )
        snap_without_lp = _snapshot(
            _supply("10.3746640", unrealized="0.0005280"),
            _borrow("-3.1124370", unrealized="-0.0001960"),
        )
        nav_with = compute_lending_nav(snap_with_lp)
        nav_without = compute_lending_nav(snap_without_lp)
        assert nav_with.net_lending_value_usd == nav_without.net_lending_value_usd
        assert nav_with.supply_positions == nav_without.supply_positions
        assert nav_with.borrow_positions == nav_without.borrow_positions

    def test_position_counts(self):
        snap = _snapshot(
            _supply("10.3746640", unrealized="0.0005280"),
            _borrow("-3.1124370", unrealized="-0.0001960"),
            _lp_pos(),
        )
        nav = compute_lending_nav(snap)
        assert nav.supply_positions == 1
        assert nav.borrow_positions == 1


# ---------------------------------------------------------------------------
# Test 2: None snapshot → all-zeros
# ---------------------------------------------------------------------------


class TestNoneSnapshot:
    def test_returns_zero_summary(self):
        nav = compute_lending_nav(None)
        assert isinstance(nav, LendingNAVSummary)
        assert nav.gross_supply_value_usd == Decimal("0")
        assert nav.gross_debt_value_usd == Decimal("0")
        assert nav.net_lending_value_usd == Decimal("0")
        assert nav.supply_unrealized_pnl_usd == Decimal("0")
        assert nav.borrow_unrealized_pnl_usd == Decimal("0")
        assert nav.net_unrealized_carry_usd == Decimal("0")
        assert nav.supply_positions == 0
        assert nav.borrow_positions == 0


# ---------------------------------------------------------------------------
# Test 3: Only SUPPLY positions
# ---------------------------------------------------------------------------


class TestOnlySupply:
    def test_borrow_fields_are_zero(self):
        snap = _snapshot(_supply("5.00", unrealized="0.001"))
        nav = compute_lending_nav(snap)
        assert nav.gross_supply_value_usd == Decimal("5.00")
        assert nav.gross_debt_value_usd == Decimal("0")
        assert nav.net_lending_value_usd == Decimal("5.00")
        assert nav.borrow_unrealized_pnl_usd == Decimal("0")
        assert nav.borrow_positions == 0
        assert nav.supply_positions == 1

    def test_supply_unrealized_accumulated(self):
        snap = _snapshot(
            _supply("5.00", unrealized="0.001"),
            _supply("3.00", unrealized="0.002"),
        )
        nav = compute_lending_nav(snap)
        assert nav.supply_unrealized_pnl_usd == Decimal("0.003")
        assert nav.supply_positions == 2


# ---------------------------------------------------------------------------
# Test 4: Only BORROW positions
# ---------------------------------------------------------------------------


class TestOnlyBorrow:
    def test_supply_fields_are_zero(self):
        snap = _snapshot(_borrow("-2.00", unrealized="-0.0005"))
        nav = compute_lending_nav(snap)
        assert nav.gross_supply_value_usd == Decimal("0")
        assert nav.gross_debt_value_usd == Decimal("2.00")
        assert nav.net_lending_value_usd == Decimal("-2.00")
        assert nav.supply_unrealized_pnl_usd == Decimal("0")
        assert nav.supply_positions == 0
        assert nav.borrow_positions == 1

    def test_borrow_unrealized_is_negative(self):
        snap = _snapshot(_borrow("-2.00", unrealized="-0.0005"))
        nav = compute_lending_nav(snap)
        assert nav.borrow_unrealized_pnl_usd == Decimal("-0.0005")
        assert nav.net_unrealized_carry_usd == Decimal("-0.0005")


# ---------------------------------------------------------------------------
# Test 5: value_usd=None on one leg (legacy/partial data)
# ---------------------------------------------------------------------------


class TestValueUsdNoneOnOneLeg:
    """Defensive: if a PositionValue arrives with value_usd=None (possible for
    legacy or partially-constructed payloads), skip that leg's value contribution
    but still count the position and sum its unrealized_pnl_usd."""

    def test_none_supply_value_skipped_from_gross(self):
        # Build a SUPPLY position with value_usd=None by patching post-init.
        supply = _supply("10.00", unrealized="0.001")
        object.__setattr__(supply, "value_usd", None)  # bypass frozen — PositionValue not frozen

        snap = _snapshot(supply, _borrow("-3.00", unrealized="-0.0002"))
        nav = compute_lending_nav(snap)
        # Supply value skipped; debt still counted.
        assert nav.gross_supply_value_usd == Decimal("0")
        assert nav.gross_debt_value_usd == Decimal("3.00")
        assert nav.net_lending_value_usd == Decimal("-3.00")
        # Counts still include the position.
        assert nav.supply_positions == 1
        # Unrealized still summed.
        assert nav.supply_unrealized_pnl_usd == Decimal("0.001")

    def test_none_borrow_value_skipped_from_gross(self):
        borrow = _borrow("-3.00", unrealized="-0.0002")
        object.__setattr__(borrow, "value_usd", None)

        snap = _snapshot(_supply("10.00", unrealized="0.001"), borrow)
        nav = compute_lending_nav(snap)
        assert nav.gross_debt_value_usd == Decimal("0")
        assert nav.net_lending_value_usd == Decimal("10.00")
        assert nav.borrow_positions == 1
        assert nav.borrow_unrealized_pnl_usd == Decimal("-0.0002")


# ---------------------------------------------------------------------------
# Test 6: Empty snapshot (no positions)
# ---------------------------------------------------------------------------


class TestEmptySnapshot:
    def test_snapshot_with_no_positions_returns_zeros(self):
        snap = _snapshot()
        nav = compute_lending_nav(snap)
        assert nav.supply_positions == 0
        assert nav.borrow_positions == 0
        assert nav.net_lending_value_usd == Decimal("0")
        assert nav.net_unrealized_carry_usd == Decimal("0")
