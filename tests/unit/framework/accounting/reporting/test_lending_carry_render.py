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
    classes = _detect_strategy_classes(lending_events, [], [])
    return AccountingData(
        deployment_id=DEPLOYMENT_ID,
        metrics=None,
        ledger_entries=[],
        position_events=[],
        snapshot=snapshot,
        lending_events=lending_events,
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
    """W1-6 (T3b) + VIB-4974: sub-cent realized interest must be visible in
    the renderer AND signed/labelled by side (paid vs earned)."""

    def test_subcent_supply_yield_renders_earned_positive(self):
        # VIB-4974: supply-side yield → "Interest earned: +$…".
        pos = LendingPositionSummary(
            position_key="key",
            protocol="aave_v3",
            chain="arbitrum",
            asset="USDC",
            market_id="0xm",
            total_interest_earned_usd=Decimal("0.000634"),
            total_interest_delta_usd=Decimal("0.000634"),
        )
        section = LendingSection(positions=[pos])
        out = render_lending_section(section)
        # Old _m() formatter would have shown "$0.00".  _m_signed preserves
        # the 6th decimal so the value is honest.
        assert "Interest earned:" in out
        assert "+$0.000634" in out
        assert "Interest paid:" not in out

    def test_subcent_borrow_cost_renders_paid_negative(self):
        # VIB-4974 core fix: borrow interest paid is a COST.  Pre-fix this
        # printed "+$0.000237" (a phantom gain); it must now render under an
        # "Interest paid" label with a leading minus.
        pos = LendingPositionSummary(
            position_key="key",
            protocol="aave_v3",
            chain="arbitrum",
            asset="USDT",
            market_id="0xm",
            total_interest_paid_usd=Decimal("0.000237"),
            total_interest_delta_usd=Decimal("-0.000237"),
        )
        section = LendingSection(positions=[pos])
        out = render_lending_section(section)
        assert "Interest paid:" in out
        assert "-$0.000237" in out
        assert "Interest earned:" not in out
        assert "+$0.000237" not in out  # never a gain

    def test_zero_interest_line_suppressed(self):
        pos = LendingPositionSummary(
            position_key="key",
            protocol="aave_v3",
            chain="arbitrum",
            asset="USDC",
            market_id="0xm",
        )
        section = LendingSection(positions=[pos])
        out = render_lending_section(section)
        assert "Interest paid:" not in out
        assert "Interest earned:" not in out
        assert "Net interest:" not in out

    def test_mixed_same_asset_shows_both_gross_components(self):
        # VIB-4974: a same-asset supply+borrow position sharing one key must
        # show BOTH gross legs plus a net line — never collapse the paid
        # borrow cost into a single netted figure.
        pos = LendingPositionSummary(
            position_key="key",
            protocol="aave_v3",
            chain="arbitrum",
            asset="USDC",
            market_id="0xm",
            position_type="MIXED",
            total_interest_paid_usd=Decimal("0.30"),
            total_interest_earned_usd=Decimal("0.50"),
            total_interest_delta_usd=Decimal("0.20"),
        )
        section = LendingSection(positions=[pos])
        out = render_lending_section(section)
        assert "Interest paid:    -$0.300000" in out
        assert "Interest earned:  +$0.500000" in out
        assert "Net interest:     +$0.200000" in out


class TestRealizedInterestSigningFromEvents:
    """VIB-4974: ``build_lending_report`` must SIGN realized interest by the
    event side.  ``interest_delta_usd`` is a positive magnitude on the event
    for both debt- and supply-side closes; the read-side report applies the
    sign (debt = cost = negative; supply = yield = positive)."""

    def test_repay_interest_aggregates_negative(self):
        # Borrow leg: BORROW then REPAY carrying +0.000237 interest magnitude.
        repay = _repay_event(asset="USDT")
        repay.interest_delta_usd = Decimal("0.000237")
        data = _make_data([repay, _borrow_event(asset="USDT")], snapshot=None)
        pos = build_lending_report(data).positions[0]
        assert pos.position_type == "BORROW"
        assert pos.total_interest_paid_usd == Decimal("0.000237")
        assert pos.total_interest_earned_usd == Decimal("0")
        # Net is a COST → negative.
        assert pos.total_interest_delta_usd == Decimal("-0.000237")

    def test_deleverage_interest_aggregates_negative(self):
        # DELEVERAGE routes through match_repay like REPAY → debt-side cost.
        delev = _repay_event(asset="USDT")
        delev.event_type = LendingEventType.DELEVERAGE
        delev.interest_delta_usd = Decimal("0.001500")
        data = _make_data([delev, _borrow_event(asset="USDT")], snapshot=None)
        pos = build_lending_report(data).positions[0]
        assert pos.total_interest_paid_usd == Decimal("0.001500")
        assert pos.total_interest_delta_usd == Decimal("-0.001500")
        assert pos.deleverage_count == 1

    def test_withdraw_interest_aggregates_positive(self):
        # Supply leg: SUPPLY then WITHDRAW carrying +0.000634 yield magnitude.
        data = _make_data([_withdraw_event(asset="USDC"), _supply_event(asset="USDC")], snapshot=None)
        pos = build_lending_report(data).positions[0]
        assert pos.position_type == "SUPPLY"
        assert pos.total_interest_earned_usd == Decimal("0.0006340")
        assert pos.total_interest_paid_usd == Decimal("0")
        # Net is a YIELD → positive.
        assert pos.total_interest_delta_usd == Decimal("0.0006340")


# ---------------------------------------------------------------------------
# Test 8: sub-cent unrealized carry must be visible in render output
# ---------------------------------------------------------------------------


class TestSubCentUnrealizedCarryRendering:
    """W1-2 (VIB-4777): unrealized carry on a leveraged-lending strategy is
    often sub-cent per snapshot (e.g. $0.000528 supply yield, -$0.000196
    borrow cost over ~30 minutes).  The renderer must surface these at >=4dp
    precision; the older two-decimal formatter that collapsed them to
    "$0.00" is the bug §A.6.3 in AccountingLastFixesMay22 describes.
    """

    def test_subcent_supply_unrealized_renders_with_six_decimals(self):
        """The canonical Looping signal from §A.6.3: +$0.000528 supply
        accrual.  Must appear character-for-character in the output."""
        snap = _snapshot(_supply_snapshot_position(unrealized="0.0005280"))
        data = _make_data([_supply_event()], snapshot=snap)
        section = build_lending_report(data)
        out = render_lending_section(section, snapshot=snap)
        assert "Unrealized carry:" in out
        assert "+$0.000528" in out, f"sub-cent supply unrealized must render with 6dp precision; got:\n{out}"
        # Critically not collapsed to "$0.00" — the bug the PRD §A.6.3 names.
        assert "Unrealized carry: +$0.00\n" not in out
        assert "Unrealized carry:  $0.00\n" not in out

    def test_subcent_borrow_unrealized_renders_negative_with_six_decimals(self):
        """The canonical Looping signal: -$0.000196 borrow cost."""
        snap = _snapshot(
            _borrow_snapshot_position(unrealized="-0.0001960"),
        )
        # Borrow-only summary requires a BORROW-side lending event so the
        # report picks up position_type="BORROW" and enrich works.
        borrow_event = LendingAccountingEvent(
            identity=_identity(),
            event_type=LendingEventType.BORROW,
            position_key="aave_v3:BORROW:USDT:arbitrum",
            market_id="0xaave_market",
            asset="USDT",
            collateral_value_before_usd=Decimal("0"),
            collateral_value_after_usd=Decimal("0"),
            debt_value_before_usd=Decimal("0"),
            debt_value_after_usd=Decimal("3.1124370"),
            net_equity_before_usd=Decimal("0"),
            net_equity_after_usd=Decimal("-3.1124370"),
            health_factor_before=None,
            health_factor_after=Decimal("2.5"),
            liquidation_threshold=None,
            lltv=None,
            supply_apr_bps=None,
            borrow_apr_bps=None,
            principal_delta_usd=Decimal("3.00"),
            interest_delta_usd=None,
            gas_usd=Decimal("0.02"),
            confidence=AccountingConfidence.HIGH,
        )
        data = _make_data([borrow_event], snapshot=snap)
        section = build_lending_report(data)
        out = render_lending_section(section, snapshot=snap)
        assert "Unrealized carry:" in out
        assert "-$0.000196" in out, (
            f"sub-cent borrow unrealized must render with 6dp precision and minus sign; got:\n{out}"
        )

    def test_subcent_net_carry_footer_renders_with_six_decimals(self):
        """Net carry footer: supply +$0.000528 + borrow -$0.000196 =
        +$0.000332.  This is the operator-visible aggregate from §A.6.3."""
        snap = _snapshot(
            _supply_snapshot_position(unrealized="0.0005280"),
            _borrow_snapshot_position(unrealized="-0.0001960"),
        )
        # Need both SUPPLY and BORROW events so both summaries render.
        borrow_event = LendingAccountingEvent(
            identity=_identity(),
            event_type=LendingEventType.BORROW,
            position_key="aave_v3:BORROW:USDT:arbitrum",
            market_id="0xaave_market",
            asset="USDT",
            collateral_value_before_usd=Decimal("0"),
            collateral_value_after_usd=Decimal("10.3746640"),
            debt_value_before_usd=Decimal("0"),
            debt_value_after_usd=Decimal("3.1124370"),
            net_equity_before_usd=Decimal("0"),
            net_equity_after_usd=Decimal("7.2622270"),
            health_factor_before=None,
            health_factor_after=Decimal("2.5"),
            liquidation_threshold=None,
            lltv=None,
            supply_apr_bps=None,
            borrow_apr_bps=None,
            principal_delta_usd=Decimal("3.00"),
            interest_delta_usd=None,
            gas_usd=Decimal("0.02"),
            confidence=AccountingConfidence.HIGH,
        )
        data = _make_data([_supply_event(), borrow_event], snapshot=snap)
        section = build_lending_report(data)
        out = render_lending_section(section, snapshot=snap)
        assert "Net unrealized:" in out
        assert "+$0.000332" in out, f"net carry footer must render the sub-cent aggregate; got:\n{out}"


# ---------------------------------------------------------------------------
# Test 9: multi-protocol render — Aave V3 + Morpho Blue in one report
# ---------------------------------------------------------------------------


def _morpho_supply_event(asset: str = "USDC") -> LendingAccountingEvent:
    """Morpho Blue SUPPLY accounting event (protocol="morpho_blue").

    Used by the multi-protocol render test to verify both Aave V3 and
    Morpho Blue positions surface their unrealized carry in the same
    report.  Both protocols write SUPPLY events with the same shape, so
    the renderer should treat them identically.
    """
    return LendingAccountingEvent(
        identity=AccountingIdentity(
            id="evt-mpho-001",
            deployment_id=DEPLOYMENT_ID,
            cycle_id="cycle-001",
            execution_mode="live",
            timestamp=datetime.now(UTC),
            chain="ethereum",
            protocol="morpho_blue",
            wallet_address="0xdeadbeef",
            tx_hash="0xmpho123",
            ledger_entry_id="led-mpho-001",
        ),
        event_type=LendingEventType.SUPPLY,
        position_key=f"morpho_blue:SUPPLY:{asset}:ethereum",
        market_id="0xmorpho_market",
        asset=asset,
        collateral_value_before_usd=Decimal("0"),
        collateral_value_after_usd=Decimal("5.00"),
        debt_value_before_usd=Decimal("0"),
        debt_value_after_usd=Decimal("0"),
        net_equity_before_usd=Decimal("0"),
        net_equity_after_usd=Decimal("5.00"),
        health_factor_before=None,
        health_factor_after=Decimal("999"),
        liquidation_threshold=None,
        lltv=None,
        supply_apr_bps=None,
        borrow_apr_bps=None,
        principal_delta_usd=Decimal("5.00"),
        interest_delta_usd=None,
        gas_usd=Decimal("0.03"),
        confidence=AccountingConfidence.HIGH,
    )


class TestMultiProtocolRender:
    """The renderer is protocol-agnostic: an Aave V3 SUPPLY and a Morpho
    Blue SUPPLY in the same snapshot must both render with their own
    per-position carry line.  This proves the design scales to additional
    lending protocols (Compound, Spark, Fluid, etc.) without needing
    per-protocol rendering branches.
    """

    def test_aave_and_morpho_both_render_carry(self):
        aave_pos = _supply_snapshot_position(unrealized="0.0005280")
        morpho_pos = PositionValue(
            position_type=PositionType.SUPPLY,
            protocol="morpho_blue",
            chain="ethereum",
            value_usd=Decimal("5.00"),
            label="USDC Morpho Supply",
            tokens=["USDC"],
            details={"asset": "USDC"},
            unrealized_pnl_usd=Decimal("0.0003"),
        )
        snap = _snapshot(aave_pos, morpho_pos)
        data = _make_data([_supply_event(), _morpho_supply_event()], snapshot=snap)
        section = build_lending_report(data)
        out = render_lending_section(section, snapshot=snap)
        # Both protocols appear in the header line of their respective
        # position blocks.
        assert "[aave_v3 / arbitrum]" in out
        assert "[morpho_blue / ethereum]" in out
        # Both carry lines render at 6dp precision.
        assert "+$0.000528" in out
        assert "+$0.000300" in out
        # Net footer combines them: 0.0005280 + 0.0003 = 0.000828.
        assert "+$0.000828" in out, f"multi-protocol Net unrealized footer must sum both legs; got:\n{out}"

    def test_multi_chain_carry_renders_both_protocols(self):
        """Aave V3 supply on Arbitrum + Aave V3 supply on Base — same
        protocol, different chains.  Both carry lines must render."""
        arb_pos = _supply_snapshot_position(chain="arbitrum", unrealized="0.0005280")
        base_pos = _supply_snapshot_position(chain="base", value_usd="7.50", unrealized="0.0002500")
        # Make the base event match its position so build_lending_report
        # finds it and enrichment links the snapshot row.
        base_identity = AccountingIdentity(
            id="evt-base-001",
            deployment_id=DEPLOYMENT_ID,
            cycle_id="cycle-001",
            execution_mode="live",
            timestamp=datetime.now(UTC),
            chain="base",
            protocol="aave_v3",
            wallet_address="0xdeadbeef",
            tx_hash="0xbase123",
            ledger_entry_id="led-base-001",
        )
        base_event = LendingAccountingEvent(
            identity=base_identity,
            event_type=LendingEventType.SUPPLY,
            position_key="aave_v3:SUPPLY:USDC:base",
            market_id="0xaave_market_base",
            asset="USDC",
            collateral_value_before_usd=Decimal("0"),
            collateral_value_after_usd=Decimal("7.50"),
            debt_value_before_usd=Decimal("0"),
            debt_value_after_usd=Decimal("0"),
            net_equity_before_usd=Decimal("0"),
            net_equity_after_usd=Decimal("7.50"),
            health_factor_before=None,
            health_factor_after=Decimal("999"),
            liquidation_threshold=None,
            lltv=None,
            supply_apr_bps=None,
            borrow_apr_bps=None,
            principal_delta_usd=Decimal("7.50"),
            interest_delta_usd=None,
            gas_usd=Decimal("0.01"),
            confidence=AccountingConfidence.HIGH,
        )
        snap = _snapshot(arb_pos, base_pos)
        data = _make_data([_supply_event(), base_event], snapshot=snap)
        section = build_lending_report(data)
        out = render_lending_section(section, snapshot=snap)
        assert "[aave_v3 / arbitrum]" in out
        assert "[aave_v3 / base]" in out
        assert "+$0.000528" in out
        assert "+$0.000250" in out


# ---------------------------------------------------------------------------
# VIB-4792 — collateral/debt post-state scoped to the side the event mutated
# ---------------------------------------------------------------------------


class TestSideScopedPostState:
    """VIB-4792: a one-sided summary must not inherit the opposite side's value.

    ``collateral_value_after_usd`` / ``debt_value_after_usd`` carry the WHOLE
    position's state on every event.  ``_borrow_event`` / ``_repay_event``
    therefore both carry ``collateral_value_after_usd=10.0`` even though they
    key a USDT-side (BORROW) summary — pre-fix that leaked $10 of USDC
    collateral onto the USDT summary.  Symmetrically a SUPPLY/WITHDRAW event
    carrying a debt post-state would leak debt onto a collateral-only summary.
    """

    def test_borrow_only_summary_does_not_inherit_collateral(self):
        # newest-first (DB order); build_lending_report reverses for accumulation
        data = _make_data([_repay_event(), _borrow_event()], snapshot=None)
        section = build_lending_report(data)
        assert len(section.positions) == 1
        pos = section.positions[0]
        assert pos.position_type == "BORROW"
        # collateral never written from a borrow-side event → stays unmeasured
        assert pos.collateral_usd is None
        # debt is the latest borrow-side post-state (REPAY → measured zero)
        assert pos.debt_usd == Decimal("0")

    def test_single_borrow_sets_debt_not_collateral(self):
        pos = build_lending_report(_make_data([_borrow_event()], snapshot=None)).positions[0]
        assert pos.collateral_usd is None
        assert pos.debt_usd == Decimal("3.0")

    def test_supply_only_summary_does_not_inherit_debt(self):
        # A supply-side event that (incorrectly) carries the whole-position debt
        # must not surface that debt on the collateral-only summary.
        leaky_supply = _supply_event()
        leaky_supply.debt_value_after_usd = Decimal("3.0")
        pos = build_lending_report(_make_data([leaky_supply], snapshot=None)).positions[0]
        assert pos.position_type == "SUPPLY"
        assert pos.collateral_usd == Decimal("10.3746640")
        assert pos.debt_usd is None

    def test_deleverage_updates_both_sides(self):
        # Whole-position events (DELEVERAGE) still set BOTH collateral and debt.
        delev = _borrow_event()
        delev.event_type = LendingEventType.DELEVERAGE
        delev.collateral_value_after_usd = Decimal("8.0")
        delev.debt_value_after_usd = Decimal("2.0")
        pos = build_lending_report(_make_data([delev], snapshot=None)).positions[0]
        assert pos.collateral_usd == Decimal("8.0")
        assert pos.debt_usd == Decimal("2.0")

    def test_close_updates_both_sides_to_settled(self):
        # CLOSE is a whole-position settle: both sides go to its post-state (0).
        close_ev = _borrow_event()
        close_ev.event_type = LendingEventType.CLOSE
        close_ev.collateral_value_after_usd = Decimal("0")
        close_ev.debt_value_after_usd = Decimal("0")
        pos = build_lending_report(_make_data([close_ev], snapshot=None)).positions[0]
        assert pos.collateral_usd == Decimal("0")
        assert pos.debt_usd == Decimal("0")

    def test_liquidation_risk_update_updates_both_sides(self):
        # LIQUIDATION_RISK_UPDATE is a whole-position risk snapshot — it is
        # neither supply-side nor borrow-side, so it updates BOTH collateral
        # and debt (mirrors DELEVERAGE/CLOSE).
        liq_ev = _borrow_event()
        liq_ev.event_type = LendingEventType.LIQUIDATION_RISK_UPDATE
        liq_ev.collateral_value_after_usd = Decimal("9.5")
        liq_ev.debt_value_after_usd = Decimal("2.5")
        pos = build_lending_report(_make_data([liq_ev], snapshot=None)).positions[0]
        assert pos.collateral_usd == Decimal("9.5")
        assert pos.debt_usd == Decimal("2.5")
