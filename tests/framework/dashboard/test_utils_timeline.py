"""Tests for timeline formatting utilities."""

from almanak.framework.dashboard.models import TimelineEventType
from almanak.framework.dashboard.utils import format_timeline_summary, get_event_type_category


def test_format_timeline_summary_swap_includes_protocol_and_slippage():
    summary = format_timeline_summary(
        TimelineEventType.SWAP,
        "swap",
        {
            "token_in": "USDC",
            "token_out": "ETH",
            "amount_in": "1000",
            "amount_out": "0.52",
            "protocol": "Uniswap V3",
            "slippage": "0.3%",
        },
    )
    assert summary == "Swapped 1000 USDC -&gt; 0.52 ETH on Uniswap V3 (slippage: 0.3%)"


def test_format_timeline_summary_lp_open_includes_position_and_liquidity():
    summary = format_timeline_summary(
        TimelineEventType.LP_OPEN,
        "lp open",
        {
            "pool": "WETH/USDC 0.30%",
            "position_id": 12345,
            "liquidity_usd": "5000",
        },
    )
    assert summary == "Opened LP position #12345 in WETH/USDC 0.30% ($5000)"


def test_format_timeline_summary_risk_guard_uses_reason():
    summary = format_timeline_summary(
        TimelineEventType.RISK_GUARD_TRIGGERED,
        "risk guard blocked",
        {"reason": "Daily loss limit reached"},
    )
    assert summary == "Daily loss limit reached"


def test_get_event_type_category_recognizes_transaction_reverted_as_error():
    category = get_event_type_category(TimelineEventType.TRANSACTION_REVERTED)
    assert category == "error"


def test_format_timeline_summary_escapes_html():
    summary = format_timeline_summary(
        TimelineEventType.OPERATOR_ACTION_EXECUTED,
        "operator action",
        {"action": "<script>alert(1)</script>", "actor": "dashboard"},
    )
    assert "<script>" not in summary
    assert "&lt;script&gt;" in summary


def test_get_event_type_category_lp_close_is_warning():
    category = get_event_type_category(TimelineEventType.LP_CLOSE)
    assert category == "warning"


def test_get_event_type_category_circuit_breaker_is_error():
    category = get_event_type_category(TimelineEventType.CIRCUIT_BREAKER_TRIGGERED)
    assert category == "error"
