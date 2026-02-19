"""Teardown tests for Aerodrome LP demo strategy."""

from decimal import Decimal
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
