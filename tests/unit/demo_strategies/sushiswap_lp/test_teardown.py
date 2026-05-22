"""Teardown tests for SushiSwap V3 LP demo strategy."""

from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import patch

from almanak.demo_strategies.sushiswap_lp import SushiSwapLPStrategy


def _create_strategy() -> SushiSwapLPStrategy:
    """Create SushiSwapLPStrategy with minimal test attributes."""
    with patch.object(SushiSwapLPStrategy, "__init__", lambda self, *args, **kwargs: None):
        strategy = SushiSwapLPStrategy.__new__(SushiSwapLPStrategy)

    strategy._deployment_id = "test-sushiswap-lp"
    strategy._chain = "arbitrum"
    strategy.pool = "WETH/USDC/3000"
    strategy.fee_tier = 3000
    strategy.token0_symbol = "WETH"
    strategy.token1_symbol = "USDC"
    strategy.amount0 = Decimal("0.001")
    strategy.amount1 = Decimal("3")
    strategy._position_id = None
    strategy._liquidity = None
    strategy._tick_lower = None
    strategy._tick_upper = None

    return strategy


def test_get_open_positions_returns_valid_summary_without_position() -> None:
    """Method should return a valid summary object even with no position."""
    strategy = _create_strategy()

    summary = strategy.get_open_positions()

    assert summary.deployment_id == "test-sushiswap-lp"
    assert len(summary.positions) == 0
    assert summary.total_value_usd == Decimal("0")


def test_get_open_positions_returns_valid_summary_with_position() -> None:
    """Method should return LP position summary without constructor field errors."""
    strategy = _create_strategy()
    strategy._position_id = 12345
    strategy._liquidity = 50000
    strategy._tick_lower = -887220
    strategy._tick_upper = 887220

    snapshot = SimpleNamespace(price=lambda symbol: Decimal("3000") if symbol == "WETH" else Decimal("1"))
    with patch.object(SushiSwapLPStrategy, "create_market_snapshot", return_value=snapshot):
        summary = strategy.get_open_positions()

    assert summary.deployment_id == "test-sushiswap-lp"
    assert len(summary.positions) == 1
    pos = summary.positions[0]
    assert pos.protocol == "sushiswap_v3"
    assert pos.position_id == "sushiswap-lp-12345-arbitrum"
    assert pos.chain == "arbitrum"
    assert pos.details["nft_position_id"] == 12345
    # WETH 0.001 @ 3000 + USDC 3 @ 1 == 6 USD; deterministic snapshot pricing.
    assert summary.total_value_usd == Decimal("6")
    assert pos.value_usd == Decimal("6")
