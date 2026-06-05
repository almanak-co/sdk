"""Tests for Pendle connector-owned accounting reporting provider."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

from almanak.connectors.pendle.reporting import (
    PendleAccountingReportConnector,
    PendlePositionSummary,
    PendleSection,
    render_pendle_section,
)
from almanak.framework.accounting.models import (
    AccountingConfidence,
    AccountingIdentity,
    PendleAccountingEvent,
    PendleEventType,
)
from almanak.framework.accounting.reporting.loader import AccountingData


def _identity() -> AccountingIdentity:
    return AccountingIdentity(
        id="evt-1",
        deployment_id="dep",
        cycle_id="cycle",
        execution_mode="live",
        timestamp=datetime(2026, 1, 2, tzinfo=UTC),
        chain="arbitrum",
        protocol="pendle",
        wallet_address="0xwallet",
        tx_hash="0xtx",
        ledger_entry_id="led-1",
    )


def _event() -> PendleAccountingEvent:
    return PendleAccountingEvent(
        identity=_identity(),
        event_type=PendleEventType.PT_BUY,
        position_key="pendle_pt:arbitrum:0xwallet:0xmarket",
        market_id="0xmarket",
        pt_token="PT-wstETH-25JUN2026",
        maturity_timestamp=datetime(2026, 6, 25, tzinfo=UTC),
        pt_amount=Decimal("1"),
        sy_amount=Decimal("0.95"),
        pt_price=Decimal("0.95"),
        implied_apr_bps=450,
        days_to_maturity=60,
        realized_yield_usd=None,
        confidence=AccountingConfidence.HIGH,
    )


def test_pendle_reporting_provider_claims_pendle_event_types() -> None:
    connector = PendleAccountingReportConnector()

    assert connector.key == "pendle"
    assert connector.strategy_class == "pendle"
    assert connector.event_types == frozenset(event_type.value for event_type in PendleEventType)
    assert connector.section_key == "pendle"
    assert connector.section_type is PendleSection
    assert connector.section_order == 300


def test_pendle_reporting_provider_deserializes_payload() -> None:
    connector = PendleAccountingReportConnector()
    event = _event()

    deserialized = connector.deserialize_event(_identity(), event.to_payload_json())

    assert deserialized.event_type is PendleEventType.PT_BUY
    assert deserialized.pt_token == "PT-wstETH-25JUN2026"
    assert deserialized.implied_apr_bps == 450


def test_pendle_reporting_provider_builds_connector_report_section() -> None:
    connector = PendleAccountingReportConnector()
    event = _event()
    data = AccountingData(
        deployment_id="dep",
        metrics=None,
        ledger_entries=[],
        position_events=[],
        snapshot=None,
        connector_events={"pendle": [event]},
    )

    section = connector.build_section(data)

    assert not section.is_empty
    assert section.positions[0].pt_token == "PT-wstETH-25JUN2026"
    assert "Pendle Positions" in connector.render_text(section, data)
    assert connector.to_json(section)["positions"][0]["pt_token"] == "PT-wstETH-25JUN2026"


def test_render_pendle_section_does_not_truncate_short_market_id() -> None:
    section = PendleSection(
        positions=[
            PendlePositionSummary(
                position_key="pendle_pt:arbitrum:0xwallet:0xmarket",
                market_id="0xmarket",
                pt_token="PT-wstETH-25JUN2026",
                protocol="pendle",
                chain="arbitrum",
                event_count=1,
            )
        ]
    )

    text = render_pendle_section(section)

    assert "    Market:        0xmarket\n" in text
    assert "0xmarket…" not in text


def test_render_pendle_section_truncates_long_market_id() -> None:
    section = PendleSection(
        positions=[
            PendlePositionSummary(
                position_key="pendle_pt:arbitrum:0xwallet:0x1234567890abcdef12",
                market_id="0x1234567890abcdef12",
                pt_token="PT-wstETH-25JUN2026",
                protocol="pendle",
                chain="arbitrum",
                event_count=1,
            )
        ]
    )

    text = render_pendle_section(section)

    assert "    Market:        0x1234567890abcd…\n" in text
