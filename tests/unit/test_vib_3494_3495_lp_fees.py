"""Tests for VIB-3494 (LP_COLLECT_FEES events) and VIB-3495 (protocol fee parser coverage).

VIB-3494: COLLECT_FEES events are enriched with fee amounts from lp_close_data.
VIB-3495: LP parsers (TJ V2, Aerodrome, Curve) explicitly emit ProtocolFees with
          unavailable_reason rather than returning None from extract_protocol_fees,
          so downstream attribution records "known-unknown" not "parser absent".
"""

from datetime import UTC, datetime
from decimal import Decimal


class TestTraderJoeV2ParserEmitsProtocolFeesUnavailable:
    """VIB-3495: TraderJoe V2 parser must emit ProtocolFees with unavailable_reason."""

    def test_extract_protocol_fees_returns_protocol_fees_object(self):
        """Parser returns a ProtocolFees instance, not None."""
        from almanak.framework.connectors.traderjoe_v2.receipt_parser import TraderJoeV2ReceiptParser
        from almanak.framework.execution.extracted_data import ProtocolFees

        parser = TraderJoeV2ReceiptParser(chain="avalanche")
        result = parser.extract_protocol_fees({})

        assert result is not None, "Expected ProtocolFees, got None"
        assert isinstance(result, ProtocolFees), f"Expected ProtocolFees, got {type(result)}"

    def test_extract_protocol_fees_has_unavailable_reason(self):
        """Parser emits explicit unavailable_reason (not just None)."""
        from almanak.framework.connectors.traderjoe_v2.receipt_parser import TraderJoeV2ReceiptParser

        parser = TraderJoeV2ReceiptParser(chain="avalanche")
        result = parser.extract_protocol_fees({})

        assert result.unavailable_reason is not None, "unavailable_reason must be set"
        assert result.unavailable_reason == "protocol_fee_not_emitted_in_receipt"

    def test_extract_protocol_fees_total_usd_is_none(self):
        """total_usd must be None when unavailable_reason is set."""
        from almanak.framework.connectors.traderjoe_v2.receipt_parser import TraderJoeV2ReceiptParser

        parser = TraderJoeV2ReceiptParser(chain="avalanche")
        result = parser.extract_protocol_fees({})

        assert result.total_usd is None, "total_usd must be None for unavailable fees"

    def test_extract_protocol_fees_is_unavailable_property(self):
        """is_unavailable property returns True."""
        from almanak.framework.connectors.traderjoe_v2.receipt_parser import TraderJoeV2ReceiptParser

        parser = TraderJoeV2ReceiptParser(chain="avalanche")
        result = parser.extract_protocol_fees({})

        assert result.is_unavailable is True


class TestAerodromeParserEmitsProtocolFeesUnavailable:
    """VIB-3495: Aerodrome parser must emit ProtocolFees with unavailable_reason."""

    def test_extract_protocol_fees_returns_protocol_fees_object(self):
        """Parser returns a ProtocolFees instance, not None."""
        from almanak.framework.connectors.aerodrome.receipt_parser import AerodromeReceiptParser
        from almanak.framework.execution.extracted_data import ProtocolFees

        parser = AerodromeReceiptParser(chain="base")
        result = parser.extract_protocol_fees({})

        assert result is not None, "Expected ProtocolFees, got None"
        assert isinstance(result, ProtocolFees), f"Expected ProtocolFees, got {type(result)}"

    def test_extract_protocol_fees_has_unavailable_reason(self):
        """Parser emits explicit unavailable_reason."""
        from almanak.framework.connectors.aerodrome.receipt_parser import AerodromeReceiptParser

        parser = AerodromeReceiptParser(chain="base")
        result = parser.extract_protocol_fees({})

        assert result.unavailable_reason == "protocol_fee_not_emitted_in_receipt"
        assert result.total_usd is None
        assert result.is_unavailable is True

    def test_slipstream_parser_inherits_unavailable(self):
        """AerodromeSlipstreamReceiptParser inherits the same extract_protocol_fees."""
        from almanak.framework.connectors.aerodrome.receipt_parser import AerodromeSlipstreamReceiptParser

        parser = AerodromeSlipstreamReceiptParser(chain="base")
        result = parser.extract_protocol_fees({})

        assert result is not None
        assert result.is_unavailable is True


class TestCurveParserEmitsProtocolFeesUnavailable:
    """VIB-3495: Curve parser must emit ProtocolFees with unavailable_reason."""

    def test_extract_protocol_fees_returns_protocol_fees_object(self):
        """Parser returns a ProtocolFees instance, not None."""
        from almanak.framework.connectors.curve.receipt_parser import CurveReceiptParser
        from almanak.framework.execution.extracted_data import ProtocolFees

        parser = CurveReceiptParser(chain="ethereum")
        result = parser.extract_protocol_fees({})

        assert result is not None, "Expected ProtocolFees, got None"
        assert isinstance(result, ProtocolFees), f"Expected ProtocolFees, got {type(result)}"

    def test_extract_protocol_fees_has_unavailable_reason(self):
        """Parser emits explicit unavailable_reason."""
        from almanak.framework.connectors.curve.receipt_parser import CurveReceiptParser

        parser = CurveReceiptParser(chain="ethereum")
        result = parser.extract_protocol_fees({})

        assert result.unavailable_reason == "protocol_fee_not_emitted_in_receipt"
        assert result.total_usd is None
        assert result.is_unavailable is True


class TestProtocolFeesUnavailableSemantics:
    """Unit tests for the ProtocolFees dataclass unavailable_reason semantics (VIB-3495)."""

    def test_protocol_fees_unavailable_construction(self):
        """Can construct ProtocolFees with unavailable_reason and None total_usd."""
        from almanak.framework.execution.extracted_data import ProtocolFees

        pf = ProtocolFees(total_usd=None, unavailable_reason="protocol_fee_not_emitted_in_receipt")
        assert pf.is_unavailable is True
        assert pf.total_usd is None
        assert pf.unavailable_reason == "protocol_fee_not_emitted_in_receipt"

    def test_protocol_fees_unavailable_rejects_total_usd(self):
        """When unavailable_reason is set, total_usd must be None."""
        import pytest
        from almanak.framework.execution.extracted_data import ProtocolFees

        with pytest.raises(ValueError, match="unavailable_reason is set"):
            ProtocolFees(total_usd=Decimal("0"), unavailable_reason="some_reason")

    def test_protocol_fees_unavailable_rejects_components(self):
        """When unavailable_reason is set, no component fields may be populated."""
        import pytest
        from almanak.framework.execution.extracted_data import ProtocolFees

        with pytest.raises(ValueError, match="unavailable_reason is set"):
            ProtocolFees(
                total_usd=None,
                swap_fee_usd=Decimal("1"),
                unavailable_reason="some_reason",
            )

    def test_protocol_fees_apply_phase_leaves_field_unknown(self):
        """_apply_protocol_fees leaves protocol_fees_usd='' for unavailable ProtocolFees."""
        from almanak.framework.execution.extracted_data import ProtocolFees
        from almanak.framework.observability.position_events import (
            IntentEventContext,
            PositionEvent,
            _apply_protocol_fees,
        )

        unavailable_fees = ProtocolFees(
            total_usd=None,
            unavailable_reason="protocol_fee_not_emitted_in_receipt",
        )

        event = PositionEvent(protocol_fees_usd="")

        class _FakeIntent:
            intent_type = "LP_CLOSE"
            protocol = "aerodrome"

        ctx = IntentEventContext(
            intent=_FakeIntent(),
            result=None,
            extracted={"protocol_fees": unavailable_fees},
            deployment_id="test",
            chain="base",
            ledger_entry_id="",
        )

        _apply_protocol_fees(event, ctx)

        # Field must remain "" (unknown), NOT "None" or "0"
        assert event.protocol_fees_usd == "", (
            f"Expected '' (unknown), got {event.protocol_fees_usd!r}"
        )


class TestLPCollectFeesEventFields:
    """VIB-3494: LP_COLLECT_FEES events are enriched with fee amounts."""

    def _make_lp_close_data(self, amount0_collected: int, amount1_collected: int, fees0: int = 0, fees1: int = 0):
        from almanak.framework.execution.extracted_data import LPCloseData

        return LPCloseData(
            amount0_collected=amount0_collected,
            amount1_collected=amount1_collected,
            fees0=fees0,
            fees1=fees1,
        )

    def test_collect_fees_event_populates_amount0_amount1(self):
        """COLLECT_FEES event picks up amount0/amount1 from lp_close_data."""
        from almanak.framework.observability.position_events import (
            IntentEventContext,
            PositionEvent,
            _apply_collect_fees,
        )

        lp_close = self._make_lp_close_data(
            amount0_collected=1_000_000,  # 1 USDC (raw)
            amount1_collected=500_000_000_000_000_000,  # 0.5 WETH (raw)
        )

        event = PositionEvent(event_type="COLLECT_FEES")

        class _FakeIntent:
            intent_type = "LP_COLLECT_FEES"
            protocol = "uniswap_v3"

        ctx = IntentEventContext(
            intent=_FakeIntent(),
            result=None,
            extracted={"lp_close_data": lp_close},
            deployment_id="test",
            chain="arbitrum",
            ledger_entry_id="",
        )

        _apply_collect_fees(event, ctx)

        assert event.amount0 == "1000000", f"Got amount0={event.amount0!r}"
        assert event.amount1 == "500000000000000000", f"Got amount1={event.amount1!r}"

    def test_collect_fees_event_populates_fee_fields(self):
        """COLLECT_FEES event picks up fees_token0/fees_token1 from lp_close_data."""
        from almanak.framework.observability.position_events import (
            IntentEventContext,
            PositionEvent,
            _apply_collect_fees,
        )

        lp_close = self._make_lp_close_data(
            amount0_collected=1_000_000,
            amount1_collected=500_000_000_000_000_000,
            fees0=5000,    # fee in token0 raw
            fees1=1000000000000000,  # fee in token1 raw
        )

        event = PositionEvent(event_type="COLLECT_FEES")

        class _FakeIntent:
            intent_type = "LP_COLLECT_FEES"
            protocol = "uniswap_v3"

        ctx = IntentEventContext(
            intent=_FakeIntent(),
            result=None,
            extracted={"lp_close_data": lp_close},
            deployment_id="test",
            chain="arbitrum",
            ledger_entry_id="",
        )

        _apply_collect_fees(event, ctx)

        assert event.fees_token0 == "5000", f"Got fees_token0={event.fees_token0!r}"
        assert event.fees_token1 == "1000000000000000", f"Got fees_token1={event.fees_token1!r}"

    def test_collect_fees_phase_skipped_for_non_collect_events(self):
        """_apply_collect_fees is a no-op for LP_CLOSE events."""
        from almanak.framework.execution.extracted_data import LPCloseData
        from almanak.framework.observability.position_events import (
            IntentEventContext,
            PositionEvent,
            _apply_collect_fees,
        )

        lp_close = LPCloseData(
            amount0_collected=1_000_000,
            amount1_collected=500_000_000_000_000_000,
        )

        event = PositionEvent(event_type="CLOSE", amount0="", amount1="")

        class _FakeIntent:
            intent_type = "LP_CLOSE"
            protocol = "uniswap_v3"

        ctx = IntentEventContext(
            intent=_FakeIntent(),
            result=None,
            extracted={"lp_close_data": lp_close},
            deployment_id="test",
            chain="arbitrum",
            ledger_entry_id="",
        )

        _apply_collect_fees(event, ctx)

        # CLOSE events must not be modified by the collect-fees phase
        assert event.amount0 == "", "CLOSE event amount0 must not be modified by collect-fees phase"
        assert event.amount1 == "", "CLOSE event amount1 must not be modified by collect-fees phase"

    def test_build_position_event_collect_fees_wired(self):
        """build_position_event_from_intent wires _apply_collect_fees for LP_COLLECT_FEES."""
        from unittest.mock import MagicMock

        from almanak.framework.execution.extracted_data import LPCloseData
        from almanak.framework.observability.position_events import build_position_event_from_intent

        lp_close = LPCloseData(
            amount0_collected=2_000_000,
            amount1_collected=3_000_000_000_000_000_000,
            fees0=10_000,
            fees1=5_000_000_000_000_000,
        )

        class _FakeIntent:
            intent_type = MagicMock()
            protocol = "traderjoe_v2"
            position_id = "pos-123"

        _FakeIntent.intent_type.value = "LP_COLLECT_FEES"

        class _FakeResult:
            position_id = "pos-123"
            transaction_results = []
            gas_cost_usd = None
            extracted_data = {"lp_close_data": lp_close}

        event = build_position_event_from_intent(
            deployment_id="test-deploy",
            intent=_FakeIntent(),
            result=_FakeResult(),
            ledger_entry_id="ledger-1",
            chain="avalanche",
        )

        assert event is not None, "build_position_event_from_intent returned None"
        assert event.event_type == "COLLECT_FEES"
        assert event.amount0 == "2000000"
        assert event.amount1 == "3000000000000000000"
        assert event.fees_token0 == "10000"
        assert event.fees_token1 == "5000000000000000"


class TestFeeApyComputation:
    """VIB-3494: compute_fee_apy produces correct APY from collect events."""

    def test_fee_apy_basic(self):
        """30-day hold with known principal and fees produces correct APY."""
        from almanak.framework.observability.pnl_attributor import compute_fee_apy

        open_ts = datetime(2026, 1, 1, 0, 0, 0, tzinfo=UTC)
        collect_ts = datetime(2026, 1, 31, 0, 0, 0, tzinfo=UTC)  # 30 days later

        open_event = {
            "event_type": "OPEN",
            "value_usd": "10000",  # $10,000 principal
            "timestamp": open_ts.isoformat(),
        }

        # Two fee collections: $50 after 15 days and $50 at 30 days
        mid_ts = datetime(2026, 1, 16, 0, 0, 0, tzinfo=UTC)
        collect_events = [
            {
                "event_type": "COLLECT_FEES",
                "value_usd": "50",  # $50 fees
                "timestamp": mid_ts.isoformat(),
            },
            {
                "event_type": "COLLECT_FEES",
                "value_usd": "50",  # $50 fees
                "timestamp": collect_ts.isoformat(),
            },
        ]

        apy = compute_fee_apy(open_event, collect_events)

        assert apy is not None, "APY should not be None"
        # total_fees = $100, hold_days = 30, principal = $10,000
        # apy = (100 / 10000) / 30 * 365 = 0.01 / 30 * 365 ≈ 0.1217
        expected = Decimal("100") / Decimal("10000") / Decimal("30") * Decimal("365")
        assert abs(apy - expected) < Decimal("0.0001"), f"APY {apy} too far from expected {expected}"

    def test_fee_apy_zero_fees_returns_none(self):
        """Zero fee collections return None (no meaningful APY)."""
        from almanak.framework.observability.pnl_attributor import compute_fee_apy

        open_ts = datetime(2026, 1, 1, tzinfo=UTC)
        collect_ts = datetime(2026, 2, 1, tzinfo=UTC)

        open_event = {
            "value_usd": "10000",
            "timestamp": open_ts.isoformat(),
        }
        collect_events = [
            {
                "value_usd": "0",
                "timestamp": collect_ts.isoformat(),
            }
        ]

        apy = compute_fee_apy(open_event, collect_events)
        assert apy is None

    def test_fee_apy_no_collect_events_returns_none(self):
        """No collect events returns None."""
        from almanak.framework.observability.pnl_attributor import compute_fee_apy

        open_event = {
            "value_usd": "10000",
            "timestamp": datetime(2026, 1, 1, tzinfo=UTC).isoformat(),
        }

        apy = compute_fee_apy(open_event, collect_events=[])
        assert apy is None

    def test_fee_apy_zero_principal_returns_none(self):
        """Zero principal returns None (division by zero guard)."""
        from almanak.framework.observability.pnl_attributor import compute_fee_apy

        open_event = {
            "value_usd": "0",
            "timestamp": datetime(2026, 1, 1, tzinfo=UTC).isoformat(),
        }
        collect_events = [
            {
                "value_usd": "100",
                "timestamp": datetime(2026, 2, 1, tzinfo=UTC).isoformat(),
            }
        ]

        apy = compute_fee_apy(open_event, collect_events)
        assert apy is None

    def test_fee_apy_single_day_clamped(self):
        """Hold duration < 1 day is clamped to 1 day to avoid extreme APY."""
        from almanak.framework.observability.pnl_attributor import compute_fee_apy

        open_ts = datetime(2026, 1, 1, 0, 0, 0, tzinfo=UTC)
        collect_ts = datetime(2026, 1, 1, 1, 0, 0, tzinfo=UTC)  # 1 hour later

        open_event = {
            "value_usd": "10000",
            "timestamp": open_ts.isoformat(),
        }
        collect_events = [
            {
                "value_usd": "100",
                "timestamp": collect_ts.isoformat(),
            }
        ]

        apy = compute_fee_apy(open_event, collect_events)

        assert apy is not None
        # With 1-day clamp: (100 / 10000) / 1 * 365 = 3.65
        expected = Decimal("100") / Decimal("10000") / Decimal("1") * Decimal("365")
        assert abs(apy - expected) < Decimal("0.001"), f"APY {apy} too far from expected {expected}"
