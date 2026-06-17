"""Unit coverage for OperatorCardGenerator reason detection."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

import pytest

from almanak.framework.models import StuckReason
from almanak.framework.services.operator_card_generator import (
    ErrorContext,
    OperatorCardGenerator,
    StrategyState,
)


@pytest.fixture
def generator() -> OperatorCardGenerator:
    return OperatorCardGenerator()


def _state(**overrides: Any) -> StrategyState:
    values = {
        "deployment_id": "test_strategy",
        "status": "stuck",
        "total_value_usd": Decimal("1000"),
        "available_balance_usd": Decimal("500"),
    }
    values.update(overrides)
    return StrategyState(**values)


def test_reason_detection_prefers_exact_error_type(
    generator: OperatorCardGenerator,
) -> None:
    error = ErrorContext(
        error_type="TransactionTimeout",
        error_message="slippage exceeded and allowance missing",
        allowance=Decimal("0"),
        required_allowance=Decimal("10"),
    )

    card = generator.generate_card(_state(), error)

    assert card.reason == StuckReason.NOT_INCLUDED_TIMEOUT


def test_reason_detection_exact_error_type_beats_state_clues(
    generator: OperatorCardGenerator,
) -> None:
    error = ErrorContext(
        error_type="TransactionTimeout",
        error_message="unclassified failure",
    )

    card = generator.generate_card(
        _state(
            pending_tx_hash="0xabc",
            pending_tx_gas_price=10,
            current_gas_price=20,
            pool_liquidity_usd=Decimal("1"),
        ),
        error,
    )

    assert card.reason == StuckReason.NOT_INCLUDED_TIMEOUT


def test_reason_detection_preserves_keyword_order(
    generator: OperatorCardGenerator,
) -> None:
    error = ErrorContext(
        error_type="UnknownError",
        error_message="execution reverted: insufficient liquidity",
    )

    card = generator.generate_card(_state(), error)

    assert card.reason == StuckReason.TRANSACTION_REVERTED


def test_reason_detection_keyword_beats_context_fields(
    generator: OperatorCardGenerator,
) -> None:
    error = ErrorContext(
        error_type="UnknownError",
        error_message="nonce conflict with insufficient balance",
        balance=Decimal("1"),
        required_balance=Decimal("2"),
    )

    card = generator.generate_card(_state(), error)

    assert card.reason == StuckReason.NONCE_CONFLICT


def test_reason_detection_context_field_priority(
    generator: OperatorCardGenerator,
) -> None:
    error = ErrorContext(
        error_type="UnknownError",
        error_message="unclassified failure",
        allowance=Decimal("1"),
        required_allowance=Decimal("2"),
        balance=Decimal("1"),
        required_balance=Decimal("2"),
    )

    card = generator.generate_card(_state(), error)

    assert card.reason == StuckReason.ALLOWANCE_MISSING


def test_reason_detection_stale_oracle_beats_rpc_error(
    generator: OperatorCardGenerator,
) -> None:
    error = ErrorContext(
        error_type="UnknownError",
        error_message="unclassified failure",
        oracle_timestamp=datetime.now(UTC) - timedelta(hours=2),
        rpc_error="connection refused",
    )

    card = generator.generate_card(_state(), error)

    assert card.reason == StuckReason.ORACLE_STALE


def test_reason_detection_rpc_error_beats_paused_and_revert(
    generator: OperatorCardGenerator,
) -> None:
    error = ErrorContext(
        error_type="UnknownError",
        error_message="unclassified failure",
        rpc_error="connection refused",
        protocol_status="paused",
        revert_reason="custom error",
    )

    card = generator.generate_card(_state(), error)

    assert card.reason == StuckReason.RPC_FAILURE


@pytest.mark.parametrize(
    ("error_kwargs", "expected"),
    [
        (
            {"allowance": Decimal("1"), "required_allowance": Decimal("2")},
            StuckReason.ALLOWANCE_MISSING,
        ),
        (
            {"balance": Decimal("1"), "required_balance": Decimal("2")},
            StuckReason.INSUFFICIENT_BALANCE,
        ),
        (
            {"slippage_actual": Decimal("0.05"), "slippage_max": Decimal("0.01")},
            StuckReason.SLIPPAGE_EXCEEDED,
        ),
        (
            {"oracle_timestamp": datetime.now(UTC) - timedelta(hours=2)},
            StuckReason.ORACLE_STALE,
        ),
        (
            {"rpc_error": "connection refused"},
            StuckReason.RPC_FAILURE,
        ),
        (
            {"protocol_status": "paused"},
            StuckReason.PROTOCOL_PAUSED,
        ),
        (
            {"revert_reason": "custom error"},
            StuckReason.TRANSACTION_REVERTED,
        ),
    ],
)
def test_reason_detection_from_error_context_fields(
    generator: OperatorCardGenerator,
    error_kwargs: dict[str, Any],
    expected: StuckReason,
) -> None:
    error = ErrorContext(
        error_type="UnknownError",
        error_message="unclassified failure",
        **error_kwargs,
    )

    card = generator.generate_card(_state(), error)

    assert card.reason == expected


def test_reason_detection_from_pending_gas_state(
    generator: OperatorCardGenerator,
) -> None:
    card = generator.generate_card(
        _state(
            pending_tx_hash="0xabc",
            pending_tx_gas_price=10,
            current_gas_price=20,
        )
    )

    assert card.reason == StuckReason.GAS_PRICE_BLOCKED


def test_reason_detection_state_fallback_after_unmatched_error_context(
    generator: OperatorCardGenerator,
) -> None:
    error = ErrorContext(
        error_type="UnknownError",
        error_message="unclassified failure",
    )

    card = generator.generate_card(
        _state(
            pending_tx_hash="0xabc",
            pending_tx_gas_price=10,
            current_gas_price=20,
        ),
        error,
    )

    assert card.reason == StuckReason.GAS_PRICE_BLOCKED


def test_reason_detection_pending_gas_beats_low_liquidity(
    generator: OperatorCardGenerator,
) -> None:
    card = generator.generate_card(
        _state(
            pending_tx_hash="0xabc",
            pending_tx_gas_price=10,
            current_gas_price=20,
            pool_liquidity_usd=Decimal("1"),
        )
    )

    assert card.reason == StuckReason.GAS_PRICE_BLOCKED


def test_reason_detection_zero_gas_values_do_not_match(
    generator: OperatorCardGenerator,
) -> None:
    card = generator.generate_card(
        _state(
            pending_tx_hash="0xabc",
            pending_tx_gas_price=0,
            current_gas_price=20,
        )
    )

    assert card.reason == StuckReason.UNKNOWN


def test_reason_detection_from_low_pool_liquidity(
    generator: OperatorCardGenerator,
) -> None:
    card = generator.generate_card(_state(pool_liquidity_usd=Decimal("999.99")))

    assert card.reason == StuckReason.POOL_LIQUIDITY_LOW


def test_reason_detection_pool_liquidity_boundary(
    generator: OperatorCardGenerator,
) -> None:
    card = generator.generate_card(_state(pool_liquidity_usd=Decimal("1000")))

    assert card.reason == StuckReason.UNKNOWN


def test_reason_detection_defaults_to_unknown(
    generator: OperatorCardGenerator,
) -> None:
    card = generator.generate_card(_state())

    assert card.reason == StuckReason.UNKNOWN
