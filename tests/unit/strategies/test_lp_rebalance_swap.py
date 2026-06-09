"""Unit coverage for the LP swap-to-ratio rebalance branch.

After a drift-close the wallet holds a skewed inventory; the strategy must swap
the heavy side toward ~50/50 (emitting a SWAP) before reopening, and stay put
when inventory is already balanced within the tolerance band. Exercised on the
reference demo (``uniswap_lp``); the same helper shape is replicated across the
other LP demos.
"""

from __future__ import annotations

from decimal import Decimal

import pytest


@pytest.fixture
def strategy():
    from almanak.demo_strategies.uniswap_lp.strategy import UniswapLPStrategy

    strat = UniswapLPStrategy.__new__(UniswapLPStrategy)
    strat.token0_symbol = "WETH"
    strat.token1_symbol = "USDC"
    return strat


class TestRebalanceSwapToRatio:
    def test_token0_heavy_swaps_token0_to_token1(self, strategy):
        # 80/20 split of $100 -> $30 over the $50 half, beyond the $10 band.
        intent = strategy._rebalance_swap_intent(Decimal("80"), Decimal("20"), Decimal("100"))
        assert intent is not None
        assert intent.intent_type.value == "SWAP"
        assert intent.from_token == "WETH"
        assert intent.to_token == "USDC"
        assert intent.amount_usd == Decimal("30")

    def test_token1_heavy_swaps_token1_to_token0(self, strategy):
        intent = strategy._rebalance_swap_intent(Decimal("20"), Decimal("80"), Decimal("100"))
        assert intent is not None
        assert intent.intent_type.value == "SWAP"
        assert intent.from_token == "USDC"
        assert intent.to_token == "WETH"
        assert intent.amount_usd == Decimal("30")

    def test_balanced_inventory_returns_none(self, strategy):
        # 52/48 of $100 -> only $2 off, inside the $10 tolerance band.
        assert strategy._rebalance_swap_intent(Decimal("52"), Decimal("48"), Decimal("100")) is None

    def test_exactly_at_tolerance_does_not_swap(self, strategy):
        # 60/40 of $100 -> exactly $10 over half; tolerance is strict (>), so hold.
        assert strategy._rebalance_swap_intent(Decimal("60"), Decimal("40"), Decimal("100")) is None
