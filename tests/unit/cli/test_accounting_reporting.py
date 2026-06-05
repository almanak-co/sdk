"""Tests for the accounting reporting module (VIB-3427).

Tests the strategy-class detection, per-section builders, and text/JSON
renderers using in-memory fixtures — no SQLite required.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from decimal import Decimal

from almanak.connectors.pendle.reporting import (
    build_pendle_report,
    pendle_section_to_dict,
    render_pendle_section,
)
from almanak.framework.accounting.models import (
    AccountingConfidence,
    AccountingIdentity,
    LendingAccountingEvent,
    LendingEventType,
    PendleAccountingEvent,
    PendleEventType,
)
from almanak.framework.accounting.reporting.data_quality import (
    build_data_quality,
)
from almanak.framework.accounting.reporting.lending_report import (
    build_lending_report,
)
from almanak.framework.accounting.reporting.loader import (
    AccountingData,
    StrategyClass,
    _deserialize_events,
    _detect_strategy_classes,
)
from almanak.framework.accounting.reporting.lp_report import build_lp_report
from almanak.framework.accounting.reporting.render_json import (
    data_quality_to_dict,
    lending_section_to_dict,
    lp_section_to_dict,
)
from almanak.framework.accounting.reporting.render_text import (
    render_data_quality_section,
    render_lending_section,
    render_lp_section,
)

DEPLOYMENT_ID = "test:deploy01"


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _identity(protocol: str = "aave", chain: str = "arbitrum") -> AccountingIdentity:
    return AccountingIdentity(
        id="evt-001",
        deployment_id=DEPLOYMENT_ID,
        cycle_id="cycle-001",
        execution_mode="live",
        timestamp=datetime.now(UTC),
        chain=chain,
        protocol=protocol,
        wallet_address="0xdeadbeef",
        tx_hash="0xabc123",
        ledger_entry_id="led-001",
    )


def _lending_event(
    event_type: LendingEventType = LendingEventType.SUPPLY,
    position_key: str = "aave:USDC",
    collateral_after: str = "10000",
    debt_after: str = "5000",
    hf_after: str = "2.5",
    supply_apr_bps: int = 350,
    borrow_apr_bps: int = 450,
    confidence: AccountingConfidence = AccountingConfidence.HIGH,
) -> LendingAccountingEvent:
    return LendingAccountingEvent(
        identity=_identity(),
        event_type=event_type,
        position_key=position_key,
        market_id="0xaave_market",
        asset="USDC",
        collateral_value_before_usd=Decimal("9000"),
        collateral_value_after_usd=Decimal(collateral_after),
        debt_value_before_usd=Decimal("4000"),
        debt_value_after_usd=Decimal(debt_after),
        net_equity_before_usd=Decimal("5000"),
        net_equity_after_usd=Decimal(collateral_after) - Decimal(debt_after),
        health_factor_before=Decimal("2.3"),
        health_factor_after=Decimal(hf_after),
        liquidation_threshold=Decimal("0.8"),
        lltv=Decimal("0.85"),
        supply_apr_bps=supply_apr_bps,
        borrow_apr_bps=borrow_apr_bps,
        principal_delta_usd=Decimal("1000"),
        interest_delta_usd=Decimal("10"),
        gas_usd=Decimal("2.50"),
        confidence=confidence,
    )


def _pendle_event(
    event_type: PendleEventType = PendleEventType.PT_BUY,
    position_key: str = "pendle:wstETH",
    pt_amount: str = "10",
    implied_apr_bps: int = 450,
    realized_yield: str = "0",
    confidence: AccountingConfidence = AccountingConfidence.HIGH,
) -> PendleAccountingEvent:
    return PendleAccountingEvent(
        identity=_identity(protocol="pendle"),
        event_type=event_type,
        position_key=position_key,
        market_id="0xpendle_market",
        pt_token="PT-wstETH-25JUN2026",
        maturity_timestamp=datetime(2026, 6, 25, tzinfo=UTC),
        pt_amount=Decimal(pt_amount),
        sy_amount=Decimal("10"),
        pt_price=Decimal("0.95"),
        implied_apr_bps=implied_apr_bps,
        days_to_maturity=60,
        realized_yield_usd=Decimal(realized_yield),
        confidence=confidence,
    )


def _lp_event(
    event_type: str = "OPEN",
    position_id: str = "pos-001",
    value_usd: str = "5000",
    fees_token0: str = "0",
    fees_token1: str = "0",
    attribution_json: str = "{}",
) -> dict:
    return {
        "position_id": position_id,
        "position_type": "LP",
        "event_type": event_type,
        "protocol": "uniswap_v3",
        "chain": "arbitrum",
        "token0": "USDC",
        "token1": "ETH",
        "value_usd": value_usd,
        "fees_token0": fees_token0,
        "fees_token1": fees_token1,
        "protocol_fees_usd": "0",
        "gas_usd": "1.50",
        "in_range": True,
        "attribution_json": attribution_json,
    }


def _make_data(
    lending: list | None = None,
    pendle: list | None = None,
    position_events: list | None = None,
    ledger_entries: list | None = None,
    unavailable: list | None = None,
) -> AccountingData:
    lending_events = lending or []
    pendle_events = pendle or []
    pos_evts = position_events or []
    led_entries = ledger_entries or []
    connector_events = {"pendle": pendle_events} if pendle_events else {}

    classes = _detect_strategy_classes(
        lending_events,
        pos_evts,
        led_entries,
        connector_events=connector_events,
    )
    return AccountingData(
        deployment_id=DEPLOYMENT_ID,
        metrics=None,
        ledger_entries=led_entries,
        position_events=pos_evts,
        snapshot=None,
        lending_events=lending_events,
        connector_events=connector_events,
        unavailable_records=unavailable or [],
        strategy_classes=classes,
    )


# ---------------------------------------------------------------------------
# Strategy class detection
# ---------------------------------------------------------------------------


def test_detect_lending():
    data = _make_data(lending=[_lending_event()])
    assert StrategyClass.LENDING in data.strategy_classes
    assert data.has_strategy_class("lending")


def test_detect_pendle():
    data = _make_data(pendle=[_pendle_event()])
    assert "pendle" in data.strategy_classes
    assert data.has_strategy_class("pendle")


def test_detect_lp():
    data = _make_data(position_events=[_lp_event()])
    assert StrategyClass.LP in data.strategy_classes


def test_detect_mixed():
    data = _make_data(lending=[_lending_event()], pendle=[_pendle_event()])
    assert StrategyClass.LENDING in data.strategy_classes
    assert "pendle" in data.strategy_classes
    assert StrategyClass.UNKNOWN not in data.strategy_classes


def test_detect_unknown_when_empty():
    data = _make_data()
    assert StrategyClass.UNKNOWN in data.strategy_classes


# ---------------------------------------------------------------------------
# Lending section
# ---------------------------------------------------------------------------


def test_lending_section_empty_when_no_events():
    data = _make_data()
    section = build_lending_report(data)
    assert section.is_empty


def test_lending_section_groups_by_position_key():
    ev1 = _lending_event(position_key="aave:USDC")
    ev2 = _lending_event(position_key="aave:ETH", event_type=LendingEventType.BORROW)
    data = _make_data(lending=[ev1, ev2])
    section = build_lending_report(data)
    assert len(section.positions) == 2
    keys = {p.position_key for p in section.positions}
    assert keys == {"aave:USDC", "aave:ETH"}


def test_lending_section_latest_state():
    # Two events on the same position — latest state should win
    ev1 = _lending_event(collateral_after="10000", hf_after="2.5")
    ev2 = _lending_event(collateral_after="12000", hf_after="2.8")
    # Reversed order in the list (loader reverses incoming newest-first)
    data = _make_data(lending=[ev2, ev1])
    section = build_lending_report(data)
    assert len(section.positions) == 1
    pos = section.positions[0]
    # ev1 is processed first (chronological after reverse), ev2 overwrites
    assert pos.collateral_usd == Decimal("12000")
    assert pos.health_factor == Decimal("2.8")


def test_lending_section_accumulates_gas():
    ev1 = _lending_event()
    ev2 = _lending_event()
    data = _make_data(lending=[ev1, ev2])
    section = build_lending_report(data)
    pos = section.positions[0]
    assert pos.total_gas_usd == Decimal("5.00")  # 2.50 * 2


def test_lending_section_deleverage_counted():
    ev = _lending_event(event_type=LendingEventType.DELEVERAGE)
    data = _make_data(lending=[ev])
    section = build_lending_report(data)
    assert section.positions[0].deleverage_count == 1


def test_lending_section_close_marks_closed():
    ev = _lending_event(event_type=LendingEventType.CLOSE)
    data = _make_data(lending=[ev])
    section = build_lending_report(data)
    assert section.positions[0].is_closed is True


# ---------------------------------------------------------------------------
# Pendle section
# ---------------------------------------------------------------------------


def test_pendle_section_empty_when_no_events():
    data = _make_data()
    section = build_pendle_report(data)
    assert section.is_empty


def test_pendle_section_captures_entry_apr():
    buy = _pendle_event(event_type=PendleEventType.PT_BUY, implied_apr_bps=450)
    data = _make_data(pendle=[buy])
    section = build_pendle_report(data)
    assert len(section.positions) == 1
    pos = section.positions[0]
    assert pos.implied_apr_pct_at_entry == Decimal("4.50")


def test_pendle_section_accumulates_yield():
    redeem = _pendle_event(
        event_type=PendleEventType.PT_REDEEM,
        realized_yield="150",
    )
    data = _make_data(pendle=[redeem])
    section = build_pendle_report(data)
    assert section.positions[0].realized_yield_usd == Decimal("150")
    assert section.positions[0].is_redeemed is True


def test_pendle_section_groups_by_position_key():
    ev1 = _pendle_event(position_key="pendle:wstETH")
    ev2 = _pendle_event(position_key="pendle:sUSDe")
    data = _make_data(pendle=[ev1, ev2])
    section = build_pendle_report(data)
    assert len(section.positions) == 2


# ---------------------------------------------------------------------------
# LP section
# ---------------------------------------------------------------------------


def test_lp_section_empty_when_no_lp_events():
    data = _make_data()
    section = build_lp_report(data)
    assert section.is_empty


def test_lp_section_open_close():
    open_ev = _lp_event(event_type="OPEN", value_usd="5000")
    close_ev = _lp_event(
        event_type="CLOSE",
        value_usd="5200",
        attribution_json=json.dumps({"net_pnl_usd": "200", "impermanent_loss_usd": "-50"}),
    )
    data = _make_data(position_events=[open_ev, close_ev])
    section = build_lp_report(data)
    assert len(section.positions) == 1
    pos = section.positions[0]
    assert pos.entry_value_usd == Decimal("5000")
    assert pos.exit_value_usd == Decimal("5200")
    assert pos.net_pnl_usd == Decimal("200")
    assert pos.il_usd == Decimal("-50")
    assert pos.is_closed is True


def test_lp_section_accumulates_gas():
    ev1 = _lp_event(event_type="OPEN")
    ev2 = _lp_event(event_type="SNAPSHOT")
    data = _make_data(position_events=[ev1, ev2])
    section = build_lp_report(data)
    assert section.positions[0].total_gas_usd == Decimal("3.00")  # 1.50 * 2


def test_lp_section_total_net_pnl_none_when_no_attribution():
    ev = _lp_event(event_type="OPEN")
    data = _make_data(position_events=[ev])
    section = build_lp_report(data)
    assert section.total_net_pnl_usd is None


def test_lp_section_ignores_non_lp_events():
    perp_ev = {
        "position_id": "pos-perp",
        "position_type": "PERP",
        "event_type": "OPEN",
        "protocol": "gmx",
        "chain": "arbitrum",
    }
    data = _make_data(position_events=[perp_ev])
    section = build_lp_report(data)
    assert section.is_empty


# ---------------------------------------------------------------------------
# Data quality section
# ---------------------------------------------------------------------------


def test_data_quality_empty_when_no_unavailable():
    data = _make_data()
    section = build_data_quality(data)
    assert section.is_empty


def test_data_quality_surfaces_unavailable_records():
    unavailable = [
        {
            "id": "u-001",
            "event_type": "SUPPLY",
            "position_key": "aave:USDC",
            "timestamp": "2026-04-26T10:00:00",
            "confidence": "UNAVAILABLE",
            "protocol": "aave",
            "chain": "arbitrum",
            "payload_json": json.dumps({"unavailable_reason": "price oracle failed"}),
        }
    ]
    data = _make_data(unavailable=unavailable)
    section = build_data_quality(data)
    assert not section.is_empty
    assert len(section.issues) == 1
    assert section.issues[0].reason == "price oracle failed"


# ---------------------------------------------------------------------------
# Event deserialization
# ---------------------------------------------------------------------------


def test_deserialize_lending_from_raw():
    ev = _lending_event()
    payload = ev.to_payload_json()
    raw = [
        {
            "id": "evt-001",
            "deployment_id": DEPLOYMENT_ID,
            "cycle_id": "c1",
            "execution_mode": "live",
            "timestamp": "2026-04-26T10:00:00",
            "chain": "arbitrum",
            "protocol": "aave",
            "wallet_address": "0xdeadbeef",
            "tx_hash": "0xabc",
            "ledger_entry_id": "led-1",
            "event_type": LendingEventType.SUPPLY.value,
            "confidence": AccountingConfidence.HIGH.value,
            "payload_json": payload,
            "position_key": "aave:USDC",
        }
    ]
    lending, connector_events, unavailable, parse_errors = _deserialize_events(raw)
    assert len(lending) == 1
    assert connector_events == {}
    assert len(unavailable) == 0
    assert parse_errors == 0
    assert lending[0].event_type == LendingEventType.SUPPLY


def test_deserialize_pendle_from_connector_report_provider():
    ev = _pendle_event()
    payload = ev.to_payload_json()
    raw = [
        {
            "id": "evt-pendle-001",
            "deployment_id": DEPLOYMENT_ID,
            "cycle_id": "c1",
            "execution_mode": "live",
            "timestamp": "2026-04-26T10:00:00",
            "chain": "arbitrum",
            "protocol": "pendle",
            "wallet_address": "0xdeadbeef",
            "tx_hash": "0xabc",
            "ledger_entry_id": "led-1",
            "event_type": PendleEventType.PT_BUY.value,
            "confidence": AccountingConfidence.HIGH.value,
            "payload_json": payload,
            "position_key": "pendle:wstETH",
        }
    ]
    lending, connector_events, unavailable, parse_errors = _deserialize_events(raw)
    assert len(lending) == 0
    assert len(connector_events["pendle"]) == 1
    assert len(unavailable) == 0
    assert parse_errors == 0
    assert connector_events["pendle"][0].event_type == PendleEventType.PT_BUY


def test_deserialize_unavailable_flagged():
    ev = _lending_event(confidence=AccountingConfidence.UNAVAILABLE)
    payload = ev.to_payload_json()
    raw = [
        {
            "id": "evt-002",
            "deployment_id": DEPLOYMENT_ID,
            "cycle_id": "c1",
            "execution_mode": "live",
            "timestamp": "2026-04-26T10:00:00",
            "chain": "arbitrum",
            "protocol": "aave",
            "wallet_address": "0xdeadbeef",
            "tx_hash": "0xabc",
            "ledger_entry_id": "led-1",
            "event_type": LendingEventType.SUPPLY.value,
            "confidence": AccountingConfidence.UNAVAILABLE.value,
            "payload_json": payload,
            "position_key": "aave:USDC",
        }
    ]
    _lending, connector_events, unavailable, parse_errors = _deserialize_events(raw)
    assert len(unavailable) == 1
    assert len(_lending) == 0  # UNAVAILABLE must not also be in typed list
    assert connector_events == {}
    assert parse_errors == 0


def test_deserialize_ignores_malformed_payload():
    raw = [
        {
            "id": "evt-003",
            "deployment_id": DEPLOYMENT_ID,
            "cycle_id": "c1",
            "execution_mode": "live",
            "timestamp": "2026-04-26T10:00:00",
            "chain": "arbitrum",
            "protocol": "aave",
            "wallet_address": "0x",
            "tx_hash": "",
            "ledger_entry_id": "",
            "event_type": "SUPPLY",
            "confidence": "HIGH",
            "payload_json": "not valid json",
            "position_key": "bad",
        }
    ]
    lending, connector_events, unavailable, parse_errors = _deserialize_events(raw)
    # Malformed payloads are counted but not silently dropped without notice
    assert len(lending) == 0
    assert connector_events == {}
    assert parse_errors == 1


# ---------------------------------------------------------------------------
# Text rendering (smoke tests — format correctness)
# ---------------------------------------------------------------------------


def test_render_lending_section_not_empty():
    data = _make_data(lending=[_lending_event()])
    section = build_lending_report(data)
    text = render_lending_section(section)
    assert "Lending Positions" in text
    assert "USDC" in text
    assert "aave" in text


def test_render_pendle_section_not_empty():
    data = _make_data(pendle=[_pendle_event()])
    section = build_pendle_report(data)
    text = render_pendle_section(section)
    assert "Pendle Positions" in text
    assert "PT-wstETH" in text


def test_render_lp_section_not_empty():
    data = _make_data(position_events=[_lp_event()])
    section = build_lp_report(data)
    text = render_lp_section(section)
    assert "LP Positions" in text
    assert "USDC/ETH" in text


def test_render_data_quality_not_empty():
    unavailable = [
        {
            "event_type": "SUPPLY",
            "position_key": "aave:USDC",
            "timestamp": "2026-04-26T10:00:00",
            "protocol": "aave",
            "chain": "arbitrum",
            "payload_json": json.dumps({"unavailable_reason": "stale price"}),
        }
    ]
    data = _make_data(unavailable=unavailable)
    section = build_data_quality(data)
    text = render_data_quality_section(section)
    assert "Data Quality" in text
    assert "UNAVAILABLE" in text


def test_render_empty_sections_return_empty_string():
    data = _make_data()
    assert render_lending_section(build_lending_report(data)) == ""
    assert render_pendle_section(build_pendle_report(data)) == ""
    assert render_lp_section(build_lp_report(data)) == ""
    assert render_data_quality_section(build_data_quality(data)) == ""


# ---------------------------------------------------------------------------
# JSON rendering
# ---------------------------------------------------------------------------


def test_lp_section_to_dict():
    data = _make_data(position_events=[_lp_event()])
    section = build_lp_report(data)
    d = lp_section_to_dict(section)
    assert "positions" in d
    assert len(d["positions"]) == 1
    pos = d["positions"][0]
    assert pos["token0"] == "USDC"
    assert pos["token1"] == "ETH"


def test_lending_section_to_dict():
    data = _make_data(lending=[_lending_event()])
    section = build_lending_report(data)
    d = lending_section_to_dict(section)
    assert "positions" in d
    pos = d["positions"][0]
    assert pos["asset"] == "USDC"
    assert Decimal(pos["supply_apr_pct"]) == Decimal("3.50")


def test_pendle_section_to_dict():
    data = _make_data(pendle=[_pendle_event()])
    section = build_pendle_report(data)
    d = pendle_section_to_dict(section)
    pos = d["positions"][0]
    assert pos["pt_token"] == "PT-wstETH-25JUN2026"
    assert Decimal(pos["implied_apr_pct_at_entry"]) == Decimal("4.50")


def test_data_quality_to_dict():
    unavailable = [
        {
            "event_type": "SUPPLY",
            "position_key": "aave:USDC",
            "timestamp": "2026-04-26T10:00:00",
            "protocol": "aave",
            "chain": "arbitrum",
            "payload_json": json.dumps({"unavailable_reason": "oracle down"}),
        }
    ]
    data = _make_data(unavailable=unavailable)
    section = build_data_quality(data)
    d = data_quality_to_dict(section)
    assert d["unavailable_count"] == 1
    assert d["issues"][0]["reason"] == "oracle down"


# ---------------------------------------------------------------------------
# Regression tests for auditor findings
# ---------------------------------------------------------------------------


def test_lending_withdraw_does_not_close_position():
    """WITHDRAW event must not flip is_closed — only CLOSE does."""
    supply = _lending_event(event_type=LendingEventType.SUPPLY)
    withdraw = _lending_event(event_type=LendingEventType.WITHDRAW)
    data = _make_data(lending=[supply, withdraw])
    section = build_lending_report(data)
    assert section.positions[0].is_closed is False


def test_unavailable_events_not_in_typed_lists():
    """UNAVAILABLE confidence rows must only appear in unavailable_records, not typed lists."""
    ev = _lending_event(confidence=AccountingConfidence.UNAVAILABLE)
    payload = ev.to_payload_json()
    raw = [
        {
            "id": "evt-unavail",
            "deployment_id": DEPLOYMENT_ID,
            "cycle_id": "c1",
            "execution_mode": "live",
            "timestamp": "2026-04-26T10:00:00",
            "chain": "arbitrum",
            "protocol": "aave",
            "wallet_address": "0xdeadbeef",
            "tx_hash": "0xabc",
            "ledger_entry_id": "led-1",
            "event_type": LendingEventType.SUPPLY.value,
            "confidence": AccountingConfidence.UNAVAILABLE.value,
            "payload_json": payload,
            "position_key": "aave:USDC",
        }
    ]
    lending, connector_events, unavailable, parse_errors = _deserialize_events(raw)
    assert len(unavailable) == 1
    assert len(lending) == 0  # must NOT also appear in lending list
    assert connector_events == {}
    assert parse_errors == 0


def test_il_positive_renders_without_forced_negative():
    """Positive IL (favorable price move) must not render with a forced '-' prefix."""
    lp_ev = _lp_event(
        event_type="CLOSE",
        attribution_json=json.dumps({"net_pnl_usd": "100", "impermanent_loss_usd": "50"}),
    )
    data = _make_data(position_events=[lp_ev])
    section = build_lp_report(data)
    text = render_lp_section(section)
    # IL is positive so should NOT appear as a double-negative
    assert "IL:         -" not in text  # old bug: was always prefixing '-'


def test_parse_errors_surfaced_in_data_quality():
    """Deserialization failures must appear in the Data Quality section."""
    data = _make_data()
    # Inject parse_errors directly onto the bundle
    object.__setattr__(data, "parse_errors", 3) if False else None
    data_with_errors = AccountingData(
        deployment_id=DEPLOYMENT_ID,
        metrics=None,
        ledger_entries=[],
        position_events=[],
        snapshot=None,
        parse_errors=3,
        strategy_classes=frozenset(),
    )
    section = build_data_quality(data_with_errors)
    assert not section.is_empty
    assert section.parse_errors == 3
    text = render_data_quality_section(section)
    assert "failed to parse" in text
