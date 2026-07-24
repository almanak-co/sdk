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


def test_format_timeline_summary_trade_without_amounts_uses_token_pair_only():
    summary = format_timeline_summary(
        TimelineEventType.TRADE,
        "trade",
        {"token_in": "USDC", "token_out": "WETH"},
    )
    assert summary == "Swapped USDC -&gt; WETH"


def test_format_timeline_summary_swap_with_protocol_but_no_slippage():
    summary = format_timeline_summary(
        TimelineEventType.SWAP,
        "swap",
        {
            "token_in": "USDC",
            "token_out": "WETH",
            "amount_in": "10",
            "amount_out": "0.004",
            "protocol": "Aerodrome",
        },
    )
    assert summary == "Swapped 10 USDC -&gt; 0.004 WETH on Aerodrome"


def test_format_timeline_summary_swap_with_slippage_but_no_protocol():
    summary = format_timeline_summary(
        TimelineEventType.SWAP,
        "swap",
        {"token_in": "USDC", "token_out": "WETH", "slippage": "0.1%"},
    )
    assert summary == "Swapped USDC -&gt; WETH (slippage: 0.1%)"


def test_format_timeline_summary_swap_missing_token_falls_back_to_description():
    summary = format_timeline_summary(
        TimelineEventType.SWAP,
        "swap happened",
        {"token_in": "USDC"},  # no token_out
    )
    assert summary == "swap happened"


def test_format_timeline_summary_lp_open_pool_and_position_without_liquidity():
    summary = format_timeline_summary(
        TimelineEventType.LP_OPEN,
        "lp open",
        {"pool": "WETH/USDC 0.05%", "position_id": 7},
    )
    assert summary == "Opened LP position #7 in WETH/USDC 0.05%"


def test_format_timeline_summary_lp_open_pool_only():
    summary = format_timeline_summary(
        TimelineEventType.LP_OPEN,
        "lp open",
        {"pool": "WETH/USDC 0.05%"},
    )
    assert summary == "Opened LP position in WETH/USDC 0.05%"


def test_format_timeline_summary_lp_open_without_pool_falls_back_to_description():
    summary = format_timeline_summary(TimelineEventType.LP_OPEN, "opened something", {})
    assert summary == "opened something"


def test_format_timeline_summary_transaction_confirmed_with_block_and_gas():
    summary = format_timeline_summary(
        TimelineEventType.TRANSACTION_CONFIRMED,
        "tx confirmed",
        {"block_number": 123456, "gas_used": 21000},
    )
    assert summary == "Transaction confirmed in block 123456 (gas: 21000)"


def test_format_timeline_summary_transaction_confirmed_without_block_uses_generic():
    summary = format_timeline_summary(
        TimelineEventType.TRANSACTION_CONFIRMED,
        "tx confirmed",
        {"gas_used": 21000},  # no block_number
    )
    assert summary == "Transaction confirmed"


def test_format_timeline_summary_transaction_submitted():
    summary = format_timeline_summary(TimelineEventType.TRANSACTION_SUBMITTED, "sent", {})
    assert summary == "Transaction submitted"


def test_format_timeline_summary_strategy_paused_with_reason():
    summary = format_timeline_summary(
        TimelineEventType.STRATEGY_PAUSED,
        "Strategy paused",
        {"pause_reason": "operator request"},
    )
    assert summary == "Strategy paused (operator request)"


def test_format_timeline_summary_strategy_resumed_without_reason_uses_description():
    summary = format_timeline_summary(TimelineEventType.STRATEGY_RESUMED, "Strategy resumed", {})
    assert summary == "Strategy resumed"


def test_format_timeline_summary_operator_action_without_actor():
    summary = format_timeline_summary(
        TimelineEventType.OPERATOR_ACTION_EXECUTED,
        "operator action",
        {"action": "pause"},
    )
    assert summary == "Operator action: pause"


def test_format_timeline_summary_operator_action_without_action_uses_description():
    summary = format_timeline_summary(
        TimelineEventType.OPERATOR_ACTION_EXECUTED,
        "an operator did a thing",
        {"actor": "dashboard"},
    )
    assert summary == "an operator did a thing"


def test_format_timeline_summary_circuit_breaker_uses_message_fallback():
    summary = format_timeline_summary(
        TimelineEventType.CIRCUIT_BREAKER_TRIGGERED,
        "circuit breaker",
        {"message": "3 consecutive failures"},
    )
    assert summary == "3 consecutive failures"


def test_format_timeline_summary_risk_guard_without_reason_uses_description():
    summary = format_timeline_summary(
        TimelineEventType.RISK_GUARD_TRIGGERED,
        "risk guard tripped",
        {},
    )
    assert summary == "risk guard tripped"


def test_format_timeline_summary_unhandled_event_type_uses_description():
    summary = format_timeline_summary(
        TimelineEventType.DEPOSIT,
        "deposited 5 <ETH>",
        {"amount": "5"},
    )
    assert summary == "deposited 5 &lt;ETH&gt;"
