"""Tests for unrealized lending carry in the strat-pnl renderer (W1-2, VIB-4777).

Coverage:
  1. Per-position ``Unrealized carry:`` line appears when snapshot data present.
  2. Net carry footer (``Net lending value:`` + ``Net unrealized:``) appears when
     at least one position has unrealized carry.
  3. No carry line / footer when snapshot is absent (backward-compat).
  4. No carry line / footer when unrealized_pnl_usd is None on the position.
  5. ``build_lending_report`` populates snapshot fields correctly.
  6. Negative carry (borrow-only) renders with minus sign.
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

from almanak.framework.accounting.models import (
    AccountingConfidence,
    AccountingIdentity,
    LendingAccountingEvent,
    LendingEventType,
)
from almanak.framework.accounting.reporting.lending_report import (
    LendingPositionSummary,
    LendingSection,
    build_lending_report,
)
from almanak.framework.accounting.reporting.loader import AccountingData, _detect_strategy_classes
from almanak.framework.accounting.reporting.render_text import render_lending_section
from almanak.framework.portfolio.models import PortfolioSnapshot, PositionValue, ValueConfidence
from almanak.framework.teardown.models import PositionType

DEPLOYMENT_ID = "test:carry-render"


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _identity() -> AccountingIdentity:
    return AccountingIdentity(
        id="evt-001",
        deployment_id=DEPLOYMENT_ID,
        cycle_id="cycle-001",
        execution_mode="live",
        timestamp=datetime.now(UTC),
        chain="arbitrum",
        protocol="aave_v3",
        wallet_address="0xdeadbeef",
        tx_hash="0xabc123",
        ledger_entry_id="led-001",
    )


def _supply_event(asset: str = "USDC") -> LendingAccountingEvent:
    return LendingAccountingEvent(
        identity=_identity(),
        event_type=LendingEventType.SUPPLY,
        position_key=f"aave_v3:SUPPLY:{asset}:arbitrum",
        market_id="0xaave_market",
        asset=asset,
        collateral_value_before_usd=Decimal("0"),
        collateral_value_after_usd=Decimal("10.3746640"),
        debt_value_before_usd=Decimal("0"),
        debt_value_after_usd=Decimal("0"),
        net_equity_before_usd=Decimal("0"),
        net_equity_after_usd=Decimal("10.3746640"),
        health_factor_before=None,
        health_factor_after=Decimal("999"),  # max HF when no debt
        liquidation_threshold=None,
        lltv=None,
        supply_apr_bps=None,
        borrow_apr_bps=None,
        principal_delta_usd=Decimal("10.00"),
        interest_delta_usd=None,
        gas_usd=Decimal("0.02"),
        confidence=AccountingConfidence.HIGH,
    )


def _supply_snapshot_position(
    protocol: str = "aave_v3",
    chain: str = "arbitrum",
    asset: str = "USDC",
    value_usd: str = "10.3746640",
    unrealized: str = "0.0005280",
) -> PositionValue:
    return PositionValue(
        position_type=PositionType.SUPPLY,
        protocol=protocol,
        chain=chain,
        value_usd=Decimal(value_usd),
        label=f"{protocol} SUPPLY",
        tokens=[asset],
        details={"asset": asset, "amount": "10.374664"},
        unrealized_pnl_usd=Decimal(unrealized),
    )


def _borrow_snapshot_position(
    protocol: str = "aave_v3",
    chain: str = "arbitrum",
    asset: str = "USDT",
    value_usd: str = "-3.1124370",
    unrealized: str = "-0.0001960",
) -> PositionValue:
    return PositionValue(
        position_type=PositionType.BORROW,
        protocol=protocol,
        chain=chain,
        value_usd=Decimal(value_usd),
        label=f"{protocol} BORROW",
        tokens=[asset],
        details={"asset": asset, "amount": "3.112437"},
        unrealized_pnl_usd=Decimal(unrealized),
    )


def _snapshot(*positions: PositionValue) -> PortfolioSnapshot:
    return PortfolioSnapshot(
        timestamp=datetime.now(UTC),
        deployment_id=DEPLOYMENT_ID,
        total_value_usd=Decimal("10.3746640"),
        available_cash_usd=Decimal("0"),
        value_confidence=ValueConfidence.HIGH,
        positions=list(positions),
    )


def _make_data(
    lending_events: list[LendingAccountingEvent],
    snapshot: PortfolioSnapshot | None = None,
) -> AccountingData:
    classes = _detect_strategy_classes(lending_events, [], [], [])
    return AccountingData(
        deployment_id=DEPLOYMENT_ID,
        metrics=None,
        ledger_entries=[],
        position_events=[],
        snapshot=snapshot,
        lending_events=lending_events,
        pendle_events=[],
        unavailable_records=[],
        strategy_classes=classes,
    )


# ---------------------------------------------------------------------------
# Test 1 + 2: position carry line + net footer present with snapshot data
# ---------------------------------------------------------------------------


class TestCarryLineAndFooter:
    def test_unrealized_carry_line_present_for_supply_position(self):
        snap = _snapshot(_supply_snapshot_position())
        data = _make_data([_supply_event()], snapshot=snap)
        section = build_lending_report(data)
        out = render_lending_section(section, snapshot=snap)
        assert "Unrealized carry:" in out

    def test_unrealized_carry_value_is_positive_for_supply(self):
        snap = _snapshot(_supply_snapshot_position(unrealized="0.0005280"))
        data = _make_data([_supply_event()], snapshot=snap)
        section = build_lending_report(data)
        out = render_lending_section(section, snapshot=snap)
        # Signed format: +$0.000528
        assert "+$" in out

    def test_net_lending_value_footer_present(self):
        snap = _snapshot(_supply_snapshot_position())
        data = _make_data([_supply_event()], snapshot=snap)
        section = build_lending_report(data)
        out = render_lending_section(section, snapshot=snap)
        assert "Net lending value:" in out

    def test_net_unrealized_footer_present(self):
        snap = _snapshot(_supply_snapshot_position())
        data = _make_data([_supply_event()], snapshot=snap)
        section = build_lending_report(data)
        out = render_lending_section(section, snapshot=snap)
        assert "Net unrealized:" in out


# ---------------------------------------------------------------------------
# Test 3: no carry without snapshot
# ---------------------------------------------------------------------------


class TestNoCarryWithoutSnapshot:
    def test_no_carry_line_when_snapshot_absent(self):
        data = _make_data([_supply_event()], snapshot=None)
        section = build_lending_report(data)
        out = render_lending_section(section, snapshot=None)
        assert "Unrealized carry:" not in out
        assert "Net lending value:" not in out
        assert "Net unrealized:" not in out

    def test_no_footer_when_no_snapshot_arg(self):
        """render_lending_section called without snapshot keyword stays backward-compat."""
        pos = LendingPositionSummary(
            position_key="key",
            protocol="aave_v3",
            chain="arbitrum",
            asset="USDC",
            market_id="0xm",
        )
        # unrealized_pnl_usd not set → None
        section = LendingSection(positions=[pos])
        out = render_lending_section(section)
        assert "Unrealized carry:" not in out
        assert "Net lending value:" not in out


# ---------------------------------------------------------------------------
# Test 4: no carry line when unrealized_pnl_usd is None
# ---------------------------------------------------------------------------


class TestNoCarryWhenNone:
    def test_no_carry_line_when_unrealized_none(self):
        """LendingPositionSummary with unrealized_pnl_usd=None → no carry line."""
        pos = LendingPositionSummary(
            position_key="key",
            protocol="aave_v3",
            chain="arbitrum",
            asset="USDC",
            market_id="0xm",
            unrealized_pnl_usd=None,
        )
        section = LendingSection(positions=[pos])
        snap = _snapshot(_supply_snapshot_position())
        out = render_lending_section(section, snapshot=snap)
        assert "Unrealized carry:" not in out
        assert "Net lending value:" not in out
        assert "Net unrealized:" not in out


# ---------------------------------------------------------------------------
# Test 5: build_lending_report populates snapshot fields
# ---------------------------------------------------------------------------


class TestBuildLendingReportEnrichment:
    def test_supply_unrealized_populated_from_snapshot(self):
        snap = _snapshot(_supply_snapshot_position(unrealized="0.0005280"))
        data = _make_data([_supply_event()], snapshot=snap)
        section = build_lending_report(data)
        assert len(section.positions) == 1
        pos = section.positions[0]
        assert pos.unrealized_pnl_usd == Decimal("0.0005280")

    def test_supply_balance_usd_populated_from_snapshot(self):
        snap = _snapshot(_supply_snapshot_position(value_usd="10.3746640"))
        data = _make_data([_supply_event()], snapshot=snap)
        section = build_lending_report(data)
        pos = section.positions[0]
        assert pos.supply_balance_usd == Decimal("10.3746640")

    def test_no_enrichment_when_snapshot_none(self):
        data = _make_data([_supply_event()], snapshot=None)
        section = build_lending_report(data)
        pos = section.positions[0]
        assert pos.unrealized_pnl_usd is None
        assert pos.supply_balance_usd is None
        assert pos.borrow_balance_usd is None

    def test_no_enrichment_when_no_matching_snapshot_position(self):
        # Snapshot has a USDC position on a different protocol.
        snap = _snapshot(_supply_snapshot_position(protocol="compound_v3"))
        data = _make_data([_supply_event()], snapshot=snap)  # event is aave_v3
        section = build_lending_report(data)
        pos = section.positions[0]
        assert pos.unrealized_pnl_usd is None

    def test_supply_summary_rejects_borrow_snapshot_row(self):
        """W1-2 side-filter (gemini review): a SUPPLY-only summary must
        NOT inherit BORROW snapshot data for the same asset."""
        snap = _snapshot(_borrow_snapshot_position(asset="USDC"))
        data = _make_data([_supply_event(asset="USDC")], snapshot=snap)
        section = build_lending_report(data)
        pos = section.positions[0]
        assert pos.position_type == "SUPPLY"
        assert pos.unrealized_pnl_usd is None
        assert pos.supply_balance_usd is None
        assert pos.borrow_balance_usd is None

    def test_borrow_summary_rejects_supply_snapshot_row(self):
        """Mirror: a BORROW-only summary must NOT inherit SUPPLY snapshot
        data for the same asset."""
        snap = _snapshot(_supply_snapshot_position(asset="USDT"))
        data = _make_data([_borrow_event(asset="USDT")], snapshot=snap)
        section = build_lending_report(data)
        pos = section.positions[0]
        assert pos.position_type == "BORROW"
        assert pos.unrealized_pnl_usd is None
        assert pos.supply_balance_usd is None
        assert pos.borrow_balance_usd is None


# ---------------------------------------------------------------------------
# Test 6: negative carry (borrow-side)
# ---------------------------------------------------------------------------


class TestNegativeCarry:
    def test_negative_unrealized_carry_has_minus_sign(self):
        """A BORROW-side carry should render with a leading '-'."""
        pos = LendingPositionSummary(
            position_key="key",
            protocol="aave_v3",
            chain="arbitrum",
            asset="USDT",
            market_id="0xm",
            unrealized_pnl_usd=Decimal("-0.0001960"),
        )
        snap = _snapshot(_borrow_snapshot_position())
        section = LendingSection(positions=[pos])
        out = render_lending_section(section, snapshot=snap)
        assert "Unrealized carry:" in out
        assert "-$" in out


# ---------------------------------------------------------------------------
# W1-6 (T3a, VIB-4781) — infer closed state from snapshot when no CLOSE event
# ---------------------------------------------------------------------------


def _withdraw_event(asset: str = "USDC") -> LendingAccountingEvent:
    return LendingAccountingEvent(
        identity=_identity(),
        event_type=LendingEventType.WITHDRAW,
        position_key=f"aave_v3:SUPPLY:{asset}:arbitrum",
        market_id="0xaave_market",
        asset=asset,
        collateral_value_before_usd=Decimal("10.3746640"),
        collateral_value_after_usd=Decimal("0"),
        debt_value_before_usd=Decimal("0"),
        debt_value_after_usd=Decimal("0"),
        net_equity_before_usd=Decimal("10.3746640"),
        net_equity_after_usd=Decimal("0"),
        health_factor_before=Decimal("999"),
        health_factor_after=None,
        liquidation_threshold=None,
        lltv=None,
        supply_apr_bps=None,
        borrow_apr_bps=None,
        principal_delta_usd=Decimal("-10.00"),
        interest_delta_usd=Decimal("0.0006340"),
        gas_usd=Decimal("0.02"),
        confidence=AccountingConfidence.HIGH,
    )


def _borrow_event(asset: str = "USDT") -> LendingAccountingEvent:
    return LendingAccountingEvent(
        identity=_identity(),
        event_type=LendingEventType.BORROW,
        position_key=f"aave_v3:BORROW:{asset}:arbitrum",
        market_id="0xaave_market",
        asset=asset,
        collateral_value_before_usd=Decimal("10.0"),
        collateral_value_after_usd=Decimal("10.0"),
        debt_value_before_usd=Decimal("0"),
        debt_value_after_usd=Decimal("3.0"),
        net_equity_before_usd=Decimal("10.0"),
        net_equity_after_usd=Decimal("7.0"),
        health_factor_before=None,
        health_factor_after=Decimal("3.3"),
        liquidation_threshold=None,
        lltv=None,
        supply_apr_bps=None,
        borrow_apr_bps=None,
        principal_delta_usd=Decimal("3.0"),
        interest_delta_usd=None,
        gas_usd=Decimal("0.02"),
        confidence=AccountingConfidence.HIGH,
    )


def _repay_event(asset: str = "USDT") -> LendingAccountingEvent:
    return LendingAccountingEvent(
        identity=_identity(),
        event_type=LendingEventType.REPAY,
        position_key=f"aave_v3:BORROW:{asset}:arbitrum",
        market_id="0xaave_market",
        asset=asset,
        collateral_value_before_usd=Decimal("10.0"),
        collateral_value_after_usd=Decimal("10.0"),
        debt_value_before_usd=Decimal("3.0"),
        debt_value_after_usd=Decimal("0"),
        net_equity_before_usd=Decimal("7.0"),
        net_equity_after_usd=Decimal("10.0"),
        health_factor_before=Decimal("3.3"),
        health_factor_after=None,
        liquidation_threshold=None,
        lltv=None,
        supply_apr_bps=None,
        borrow_apr_bps=None,
        principal_delta_usd=Decimal("-3.0"),
        interest_delta_usd=Decimal("0.0002370"),
        gas_usd=Decimal("0.02"),
        confidence=AccountingConfidence.HIGH,
    )


def _empty_snapshot() -> PortfolioSnapshot:
    return PortfolioSnapshot(
        timestamp=datetime.now(UTC),
        deployment_id=DEPLOYMENT_ID,
        total_value_usd=Decimal("0"),
        available_cash_usd=Decimal("0"),
        value_confidence=ValueConfidence.HIGH,
        positions=[],
    )


class TestInferredClosedState:
    """W1-6 (T3a): infer is_closed from snapshot when no CLOSE event was written."""

    def test_supply_then_withdraw_with_empty_snapshot_marks_closed(self):
        """Last event = WITHDRAW + snapshot has no matching SUPPLY → closed.

        ``data.lending_events`` arrives newest-first from the DB query
        (``ORDER BY timestamp DESC``); ``build_lending_report`` reverses
        it for chronological state accumulation.  Test inputs follow the
        same convention.
        """
        data = _make_data([_withdraw_event(), _supply_event()], snapshot=_empty_snapshot())
        section = build_lending_report(data)
        assert len(section.positions) == 1
        assert section.positions[0].is_closed is True

    def test_borrow_then_repay_with_empty_snapshot_marks_closed(self):
        """Last event = REPAY + snapshot has no matching BORROW → closed."""
        data = _make_data([_repay_event(), _borrow_event()], snapshot=_empty_snapshot())
        section = build_lending_report(data)
        assert len(section.positions) == 1
        assert section.positions[0].is_closed is True

    def test_withdraw_with_matching_snapshot_position_stays_open(self):
        """Last event = WITHDRAW but snapshot still has a matching SUPPLY → partial.

        A partial WITHDRAW leaves a live position in the snapshot; the
        summary should not be auto-closed.
        """
        snap = _snapshot(_supply_snapshot_position())
        data = _make_data([_withdraw_event(), _supply_event()], snapshot=snap)
        section = build_lending_report(data)
        assert section.positions[0].is_closed is False

    def test_supply_only_with_empty_snapshot_stays_open(self):
        """Last event = SUPPLY (opening) → should never auto-close even if snapshot empty."""
        data = _make_data([_supply_event()], snapshot=_empty_snapshot())
        section = build_lending_report(data)
        assert section.positions[0].is_closed is False

    def test_no_snapshot_keeps_open_default(self):
        """No snapshot available → cannot infer closure; summary stays open."""
        data = _make_data([_withdraw_event(), _supply_event()], snapshot=None)
        section = build_lending_report(data)
        assert section.positions[0].is_closed is False

    def test_supply_summary_closed_when_only_borrow_snapshot_remains(self):
        """W1-6 side-filter: a SUPPLY summary must not stay OPEN because an
        unrelated BORROW snapshot row exists for the same asset."""
        snap = _snapshot(_borrow_snapshot_position(asset="USDC"))
        data = _make_data([_withdraw_event(asset="USDC"), _supply_event(asset="USDC")], snapshot=snap)
        section = build_lending_report(data)
        assert section.positions[0].position_type == "SUPPLY"
        assert section.positions[0].is_closed is True

    def test_borrow_summary_closed_when_only_supply_snapshot_remains(self):
        """Mirror: a BORROW summary must not stay OPEN because an unrelated
        SUPPLY snapshot row exists for the same asset."""
        snap = _snapshot(_supply_snapshot_position(asset="USDT"))
        data = _make_data([_repay_event(asset="USDT"), _borrow_event(asset="USDT")], snapshot=snap)
        section = build_lending_report(data)
        assert section.positions[0].position_type == "BORROW"
        assert section.positions[0].is_closed is True

    def test_close_event_still_overrides_inference(self):
        """An explicit LendingEventType.CLOSE event always wins, even with empty snapshot."""
        close_ev = LendingAccountingEvent(
            identity=_identity(),
            event_type=LendingEventType.CLOSE,
            position_key="aave_v3:SUPPLY:USDC:arbitrum",
            market_id="0xaave_market",
            asset="USDC",
            collateral_value_before_usd=Decimal("0"),
            collateral_value_after_usd=Decimal("0"),
            debt_value_before_usd=Decimal("0"),
            debt_value_after_usd=Decimal("0"),
            net_equity_before_usd=Decimal("0"),
            net_equity_after_usd=Decimal("0"),
            health_factor_before=None,
            health_factor_after=None,
            liquidation_threshold=None,
            lltv=None,
            supply_apr_bps=None,
            borrow_apr_bps=None,
            principal_delta_usd=Decimal("0"),
            interest_delta_usd=None,
            gas_usd=Decimal("0"),
            confidence=AccountingConfidence.HIGH,
        )
        data = _make_data([close_ev, _supply_event()], snapshot=_empty_snapshot())
        section = build_lending_report(data)
        assert section.positions[0].is_closed is True


# ---------------------------------------------------------------------------
# W1-6 (T3b, VIB-4781) — realized interest rendering with precision
# ---------------------------------------------------------------------------


class TestRealizedInterestRendering:
    """W1-6 (T3b): sub-cent realized interest must be visible in the renderer."""

    def test_subcent_realized_interest_renders_with_six_decimals(self):
        pos = LendingPositionSummary(
            position_key="key",
            protocol="aave_v3",
            chain="arbitrum",
            asset="USDC",
            market_id="0xm",
            total_interest_delta_usd=Decimal("0.000634"),
        )
        section = LendingSection(positions=[pos])
        out = render_lending_section(section)
        # Old _m() formatter would have shown "$0.00".  _m_signed preserves
        # the 6th decimal so the value is honest.
        assert "Realized interest:" in out
        assert "+$0.000634" in out

    def test_negative_realized_interest_for_borrow_side(self):
        pos = LendingPositionSummary(
            position_key="key",
            protocol="aave_v3",
            chain="arbitrum",
            asset="USDT",
            market_id="0xm",
            total_interest_delta_usd=Decimal("-0.000237"),
        )
        section = LendingSection(positions=[pos])
        out = render_lending_section(section)
        assert "Realized interest:" in out
        assert "-$0.000237" in out

    def test_zero_interest_line_suppressed(self):
        pos = LendingPositionSummary(
            position_key="key",
            protocol="aave_v3",
            chain="arbitrum",
            asset="USDC",
            market_id="0xm",
            total_interest_delta_usd=Decimal("0"),
        )
        section = LendingSection(positions=[pos])
        out = render_lending_section(section)
        assert "Realized interest:" not in out
