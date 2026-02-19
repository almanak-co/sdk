"""Teardown tests for TraderJoe V2 LP demo strategy."""

from decimal import Decimal
from unittest.mock import patch

from strategies.demo.traderjoe_lp import TraderJoeLPStrategy


def _create_strategy() -> TraderJoeLPStrategy:
    """Create TraderJoeLPStrategy with minimal test attributes."""
    with patch.object(TraderJoeLPStrategy, "__init__", lambda self, *args, **kwargs: None):
        strategy = TraderJoeLPStrategy.__new__(TraderJoeLPStrategy)

    strategy._strategy_id = "test-traderjoe-lp"
    strategy._chain = "avalanche"
    strategy.pool = "WAVAX/USDC/20"
    strategy.bin_step = 20
    strategy.token_x_symbol = "WAVAX"
    strategy.token_y_symbol = "USDC"
    strategy.amount_x = Decimal("1.0")
    strategy.amount_y = Decimal("30")
    strategy._position_bin_ids = []

    return strategy


def test_get_open_positions_returns_valid_summary_without_position() -> None:
    """Method should return a valid summary object even with no position."""
    strategy = _create_strategy()

    summary = strategy.get_open_positions()

    assert summary.strategy_id == "test-traderjoe-lp"
    assert len(summary.positions) == 0
    assert summary.total_value_usd == Decimal("0")


def test_get_open_positions_returns_valid_summary_with_position() -> None:
    """Method should return LP position summary without constructor field errors."""
    strategy = _create_strategy()
    strategy._position_bin_ids = [8388608, 8388609, 8388610]

    summary = strategy.get_open_positions()

    assert summary.strategy_id == "test-traderjoe-lp"
    assert len(summary.positions) == 1
    pos = summary.positions[0]
    assert pos.protocol == "traderjoe_v2"
    assert pos.position_id == "traderjoe-lp-WAVAX/USDC/20-avalanche"
    assert pos.chain == "avalanche"
    assert pos.details["num_bins"] == 3
    assert pos.details["bin_ids"] == [8388608, 8388609, 8388610]
    assert summary.total_value_usd > Decimal("0")
