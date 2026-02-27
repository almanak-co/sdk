"""Teardown tests for Aerodrome LP demo strategy."""

from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import patch

from strategies.demo.aerodrome_lp import AerodromeLPStrategy


def create_strategy() -> AerodromeLPStrategy:
    """Create AerodromeLPStrategy with minimal test attributes."""
    with patch.object(AerodromeLPStrategy, "__init__", lambda _self, *_args, **_kwargs: None):
        strategy = AerodromeLPStrategy.__new__(AerodromeLPStrategy)

    strategy._strategy_id = "test-aerodrome-lp"
    strategy._chain = "base"
    strategy.pool = "WETH/USDC"
    strategy.stable = False
    strategy.token0_symbol = "WETH"
    strategy.token1_symbol = "USDC"
    strategy.amount0 = Decimal("0.001")
    strategy.amount1 = Decimal("3")
    strategy._has_position = False
    strategy._lp_token_balance = Decimal("0")

    return strategy


def test_get_open_positions_returns_valid_summary_without_position() -> None:
    """Method should return a valid summary object even with no position."""
    strategy = create_strategy()

    summary = strategy.get_open_positions()

    assert summary.strategy_id == "test-aerodrome-lp"
    assert len(summary.positions) == 0
    assert summary.total_value_usd == Decimal("0")


def test_get_open_positions_returns_valid_summary_with_position() -> None:
    """Method should return LP position summary without constructor field errors."""
    strategy = create_strategy()
    strategy._has_position = True
    strategy._lp_token_balance = Decimal("1.25")

    summary = strategy.get_open_positions()

    assert summary.strategy_id == "test-aerodrome-lp"
    assert len(summary.positions) == 1
    assert summary.positions[0].protocol == "aerodrome"
    assert summary.total_value_usd > Decimal("0")


def test_get_persistent_state_includes_lp_fields() -> None:
    """Persisted state should include LP position flags."""
    strategy = create_strategy()
    strategy._has_position = True
    strategy._lp_token_balance = Decimal("1.25")

    state = strategy.get_persistent_state()

    assert state["has_position"] is True
    assert state["lp_token_balance"] == "1.25"


def test_load_persistent_state_restores_lp_fields() -> None:
    """Persisted state should restore in-memory LP fields."""
    strategy = create_strategy()
    strategy.load_persistent_state({"has_position": True, "lp_token_balance": "2.50"})

    assert strategy._has_position is True
    assert strategy._lp_token_balance == Decimal("2.50")


def test_load_persistent_state_invalid_balance_defaults_to_zero() -> None:
    """Invalid persisted balances should fail closed to zero."""
    strategy = create_strategy()
    strategy._lp_token_balance = Decimal("9")

    strategy.load_persistent_state({"has_position": "true", "lp_token_balance": "not-a-decimal"})

    assert strategy._has_position is True
    assert strategy._lp_token_balance == Decimal("0")


def test_get_open_positions_uses_positive_balance_marker() -> None:
    """Positive LP balance marker should count as an open position."""
    strategy = create_strategy()
    strategy._has_position = False
    strategy._lp_token_balance = Decimal("0.75")

    summary = strategy.get_open_positions()

    assert len(summary.positions) == 1
    assert summary.positions[0].protocol == "aerodrome"


def test_get_persistent_state_marks_position_open_for_positive_balance() -> None:
    """Persisted has_position should follow tracked balance marker."""
    strategy = create_strategy()
    strategy._has_position = False
    strategy._lp_token_balance = Decimal("0.75")

    state = strategy.get_persistent_state()

    assert state["has_position"] is True


def test_load_persistent_state_positive_balance_sets_has_position() -> None:
    """Positive persisted LP balance should normalize to has_position=True."""
    strategy = create_strategy()
    strategy.load_persistent_state({"has_position": False, "lp_token_balance": "0.2"})

    assert strategy._has_position is True
    assert strategy._lp_token_balance == Decimal("0.2")


def test_generate_teardown_intents_uses_positive_balance_marker() -> None:
    """Teardown intents should be generated when tracked marker indicates open position."""
    strategy = create_strategy()
    strategy._has_position = False
    strategy._lp_token_balance = Decimal("0.3")

    intents = strategy.generate_teardown_intents(SimpleNamespace(value="graceful"))

    assert len(intents) == 1
    assert intents[0].intent_type.value == "LP_CLOSE"
