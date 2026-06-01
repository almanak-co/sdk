"""Unit tests for the lending NAV helper (W1-1, VIB-4776).

Coverage:
  1. PRD acceptance test: SUPPLY+BORROW+LP snapshot → correct net/carry.
  2. Empty snapshot (None) → all-zeros LendingNAVSummary.
  3. Only SUPPLY positions → borrow fields zero.
  4. Only BORROW positions → supply fields zero.
  5. value_usd=None on one leg → that leg skipped gracefully.
  6. unrealized_pnl_usd=None on one leg → skipped from carry sum + INFO log
     (Empty != Zero discipline, AGENTS.md §Accounting).
  7. Multi-protocol robustness — Aave V3 + Morpho Blue aggregates correctly.
  8. Multi-chain robustness — Arbitrum + Base aggregates correctly.
"""

from __future__ import annotations

import logging
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
        deployment_id="test:nav",
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


# ---------------------------------------------------------------------------
# Test 7: unrealized_pnl_usd=None → Empty != Zero discipline
# ---------------------------------------------------------------------------


class TestUnrealizedNoneEmptyNotZero:
    """AGENTS.md §Accounting "Empty != Zero": ``None`` ``unrealized_pnl_usd``
    represents "not measured" and must not crash the aggregator or silently
    be coerced to zero in a mixed (measured + unmeasured) snapshot.

    The dataclass default is ``Decimal("0")`` (a measured zero by
    construction), so ``None`` only arrives via legacy / partially-
    constructed payloads — patched in via ``object.__setattr__`` here.
    """

    def _patch_unrealized_none(self, pos: PositionValue) -> PositionValue:
        object.__setattr__(pos, "unrealized_pnl_usd", None)
        return pos

    def test_all_unmeasured_aggregate_is_zero(self):
        """All-None unrealized inputs → net_unrealized_carry_usd == 0 (the
        typed contract).  Counts still record the positions."""
        s = self._patch_unrealized_none(_supply("10.00"))
        b = self._patch_unrealized_none(_borrow("-3.00"))
        snap = _snapshot(s, b)
        nav = compute_lending_nav(snap)
        assert nav.supply_unrealized_pnl_usd == Decimal("0")
        assert nav.borrow_unrealized_pnl_usd == Decimal("0")
        assert nav.net_unrealized_carry_usd == Decimal("0")
        # Counts still recorded.
        assert nav.supply_positions == 1
        assert nav.borrow_positions == 1
        # Value side untouched by unrealized = None.
        assert nav.gross_supply_value_usd == Decimal("10.00")
        assert nav.gross_debt_value_usd == Decimal("3.00")

    def test_mixed_measured_and_unmeasured_returns_measured_sum(self):
        """One measured + one unmeasured SUPPLY → aggregate = measured value
        (Decimal("0") for a measured-zero leg, not None)."""
        measured = _supply("10.00", unrealized="0")  # measured zero
        unmeasured = self._patch_unrealized_none(_supply("5.00"))
        snap = _snapshot(measured, unmeasured)
        nav = compute_lending_nav(snap)
        # Aggregate equals Decimal("0") — the measured one.  Critically NOT
        # None and NOT the unmeasured's silently-coerced default.
        assert nav.supply_unrealized_pnl_usd == Decimal("0")
        assert nav.supply_positions == 2

    def test_mixed_measured_and_unmeasured_supply_positive(self):
        """One measured-positive + one unmeasured → aggregate equals the
        measured positive value, not zero."""
        measured = _supply("10.00", unrealized="0.0005280")
        unmeasured = self._patch_unrealized_none(_supply("5.00"))
        snap = _snapshot(measured, unmeasured)
        nav = compute_lending_nav(snap)
        assert nav.supply_unrealized_pnl_usd == Decimal("0.0005280")

    def test_unmeasured_logs_info(self, caplog):
        """At least one unmeasured leg → INFO log identifying the count
        (never WARN — this is a normal legacy-payload signal).  Operators
        rely on this log to know the carry sum is measured-only."""
        unmeasured = self._patch_unrealized_none(_supply("5.00"))
        snap = _snapshot(unmeasured)
        with caplog.at_level(logging.INFO, logger="almanak.framework.accounting.lending_nav"):
            compute_lending_nav(snap)
        info_records = [
            r for r in caplog.records
            if r.levelno == logging.INFO and "unrealized_pnl_usd=None" in r.getMessage()
        ]
        assert len(info_records) == 1, (
            f"expected exactly 1 INFO record naming the unmeasured leg(s); "
            f"got {[r.getMessage() for r in caplog.records]}"
        )
        # The log must NOT have been WARN — that's an Empty != Zero anti-
        # pattern (treats a normal degrade as an error).
        warn_records = [r for r in caplog.records if r.levelno >= logging.WARNING]
        assert warn_records == []

    def test_no_unmeasured_no_info_log(self, caplog):
        """Snapshot with only measured legs → no INFO log about unmeasured
        positions.  Guards against log noise on the common-case path."""
        snap = _snapshot(_supply("10.00", unrealized="0.0005280"))
        with caplog.at_level(logging.INFO, logger="almanak.framework.accounting.lending_nav"):
            compute_lending_nav(snap)
        info_records = [
            r for r in caplog.records
            if "unrealized_pnl_usd=None" in r.getMessage()
        ]
        assert info_records == []

    def test_unmeasured_does_not_crash_on_decimal_arithmetic(self):
        """Regression: prior aggregator did ``supply_unrealized += pos.unrealized_pnl_usd``
        directly, which would TypeError on ``Decimal + None`` for legacy
        payloads.  Hardening must avoid that."""
        unmeasured = self._patch_unrealized_none(_supply("5.00"))
        snap = _snapshot(unmeasured)
        # Must NOT raise.
        nav = compute_lending_nav(snap)
        assert isinstance(nav, LendingNAVSummary)


# ---------------------------------------------------------------------------
# Test 8: multi-protocol robustness (Aave V3 + Morpho Blue)
# ---------------------------------------------------------------------------


def _morpho_supply(value_usd: str, unrealized: str = "0", chain: str = "ethereum") -> PositionValue:
    """Morpho Blue SUPPLY position fixture.

    Morpho Blue uses ``protocol="morpho_blue"`` per the connector registry
    (`almanak/connectors/morpho_blue/compiler.py`).
    """
    return PositionValue(
        position_type=PositionType.SUPPLY,
        protocol="morpho_blue",
        chain=chain,
        value_usd=Decimal(value_usd),
        label="USDC Morpho Supply",
        unrealized_pnl_usd=Decimal(unrealized),
    )


def _morpho_borrow(value_usd: str, unrealized: str = "0", chain: str = "ethereum") -> PositionValue:
    return PositionValue(
        position_type=PositionType.BORROW,
        protocol="morpho_blue",
        chain=chain,
        value_usd=Decimal(value_usd),
        label="WETH Morpho Borrow",
        unrealized_pnl_usd=Decimal(unrealized),
    )


class TestMultiProtocol:
    """The helper is protocol-agnostic: any lending position type (SUPPLY /
    BORROW) is aggregated regardless of which connector emitted it.  This
    guards against a single-protocol regression in the dispatch.
    """

    def test_aave_plus_morpho_supply_aggregates(self):
        """Aave V3 SUPPLY $10 + Morpho Blue SUPPLY $5 → gross_supply = $15."""
        snap = _snapshot(
            _supply("10.00", unrealized="0.0010"),  # aave_v3 (default in helper)
            _morpho_supply("5.00", unrealized="0.0003"),
        )
        nav = compute_lending_nav(snap)
        assert nav.gross_supply_value_usd == Decimal("15.00")
        assert nav.supply_unrealized_pnl_usd == Decimal("0.0013")
        assert nav.supply_positions == 2
        assert nav.gross_debt_value_usd == Decimal("0")

    def test_aave_supply_plus_morpho_borrow_nets_correctly(self):
        """Cross-protocol leveraged position: Aave supply + Morpho borrow.
        Net = supply (positive) − abs(borrow) (positive) regardless of
        which protocol holds which side."""
        snap = _snapshot(
            _supply("10.3746640", unrealized="0.0005280"),  # aave_v3
            _morpho_borrow("-3.1124370", unrealized="-0.0001960"),
        )
        nav = compute_lending_nav(snap)
        assert nav.net_lending_value_usd == pytest.approx(
            Decimal("7.2622270"), rel=Decimal("1e-6")
        )
        assert nav.net_unrealized_carry_usd == pytest.approx(
            Decimal("0.0003320"), rel=Decimal("1e-6")
        )
        assert nav.supply_positions == 1
        assert nav.borrow_positions == 1

    def test_three_protocols_in_one_snapshot(self):
        """Aave V3 SUPPLY + Morpho Blue SUPPLY + Aave V3 BORROW. Validates
        the helper does not key off (protocol, chain) — it only filters by
        position_type."""
        snap = _snapshot(
            _supply("8.00", unrealized="0.0002"),       # aave_v3 SUPPLY
            _morpho_supply("4.00", unrealized="0.0001"),  # morpho_blue SUPPLY
            _borrow("-3.00", unrealized="-0.00015"),    # aave_v3 BORROW
        )
        nav = compute_lending_nav(snap)
        assert nav.gross_supply_value_usd == Decimal("12.00")
        assert nav.gross_debt_value_usd == Decimal("3.00")
        assert nav.net_lending_value_usd == Decimal("9.00")
        # 0.0002 + 0.0001 = 0.0003 supply, -0.00015 borrow, net 0.00015.
        assert nav.supply_unrealized_pnl_usd == Decimal("0.0003")
        assert nav.borrow_unrealized_pnl_usd == Decimal("-0.00015")
        assert nav.net_unrealized_carry_usd == Decimal("0.00015")
        assert nav.supply_positions == 2
        assert nav.borrow_positions == 1


# ---------------------------------------------------------------------------
# Test 9: multi-chain robustness (Arbitrum + Base)
# ---------------------------------------------------------------------------


def _supply_on_chain(chain: str, value_usd: str, unrealized: str = "0") -> PositionValue:
    return PositionValue(
        position_type=PositionType.SUPPLY,
        protocol="aave_v3",
        chain=chain,
        value_usd=Decimal(value_usd),
        label=f"USDC Supply ({chain})",
        unrealized_pnl_usd=Decimal(unrealized),
    )


def _borrow_on_chain(chain: str, value_usd: str, unrealized: str = "0") -> PositionValue:
    return PositionValue(
        position_type=PositionType.BORROW,
        protocol="aave_v3",
        chain=chain,
        value_usd=Decimal(value_usd),
        label=f"USDT Borrow ({chain})",
        unrealized_pnl_usd=Decimal(unrealized),
    )


class TestMultiChain:
    """The helper aggregates across chains in a single PortfolioSnapshot.
    A cross-chain lending strategy (e.g. Aave on Arbitrum + Aave on Base)
    must have its NAV summed correctly without chain-filtering."""

    def test_arbitrum_plus_base_supply_sums(self):
        snap = _snapshot(
            _supply_on_chain("arbitrum", "10.00", unrealized="0.0010"),
            _supply_on_chain("base", "7.50", unrealized="0.0005"),
        )
        nav = compute_lending_nav(snap)
        assert nav.gross_supply_value_usd == Decimal("17.50")
        assert nav.supply_unrealized_pnl_usd == Decimal("0.0015")
        assert nav.supply_positions == 2

    def test_arbitrum_supply_plus_base_borrow_nets(self):
        """Cross-chain leveraged position: supply on one chain, borrow on
        another (e.g. via bridge).  Net NAV crosses the chain boundary."""
        snap = _snapshot(
            _supply_on_chain("arbitrum", "10.00", unrealized="0.0008"),
            _borrow_on_chain("base", "-3.00", unrealized="-0.0002"),
        )
        nav = compute_lending_nav(snap)
        assert nav.net_lending_value_usd == Decimal("7.00")
        assert nav.net_unrealized_carry_usd == Decimal("0.0006")
        assert nav.supply_positions == 1
        assert nav.borrow_positions == 1

    def test_three_chains_with_mixed_sides(self):
        """Arbitrum SUPPLY + Base SUPPLY + Ethereum BORROW.  Mirrors a
        cross-chain delta-neutral lending strategy.  Helper aggregates
        without losing legs to a chain-filter bug."""
        snap = _snapshot(
            _supply_on_chain("arbitrum", "5.00", unrealized="0.0001"),
            _supply_on_chain("base", "4.00", unrealized="0.0002"),
            _borrow_on_chain("ethereum", "-2.00", unrealized="-0.0001"),
        )
        nav = compute_lending_nav(snap)
        assert nav.gross_supply_value_usd == Decimal("9.00")
        assert nav.gross_debt_value_usd == Decimal("2.00")
        assert nav.net_lending_value_usd == Decimal("7.00")
        assert nav.supply_positions == 2
        assert nav.borrow_positions == 1
        assert nav.net_unrealized_carry_usd == Decimal("0.0002")
