"""Tests for the Aave V3 Sweep Lending Arbitrum demo strategy.

Validates the Arbitrum default path after the VIB-3294 collateral switch from
WETH to wstETH.
"""

from __future__ import annotations

from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest


@pytest.fixture
def strategy():
    from almanak.demo_strategies.aave_sweep_lending.strategy import AaveSweepLendingStrategy

    with patch(
        "almanak.framework.strategies.intent_strategy.IntentStrategy.__init__",
        return_value=None,
    ):
        strat = AaveSweepLendingStrategy.__new__(AaveSweepLendingStrategy)
        strat._config = {}
        strat._chain = "arbitrum"
        strat._wallet_address = "0x" + "0" * 40
        strat._deployment_id = "test-aave-sweep-arbitrum"
        strat.get_config = lambda key, default=None: strat._config.get(key, default)
        AaveSweepLendingStrategy.__init__(strat)
        return strat


def _mock_market(wsteth_price: float = 3400.0, usdc_price: float = 1.0) -> MagicMock:
    market = MagicMock()

    def price_fn(token):
        if token == "wstETH":
            return Decimal(str(wsteth_price))
        if token == "USDC":
            return Decimal(str(usdc_price))
        raise ValueError(f"Unexpected token: {token}")

    market.price = MagicMock(side_effect=price_fn)
    return market


def test_init_defaults_supply_token_to_wsteth(strategy):
    """Arbitrum sweep demo should default to wstETH after VIB-3294."""
    assert strategy.supply_token == "wstETH"


def test_first_tick_supplies_wsteth_by_default(strategy):
    """Default config path should emit a wstETH SUPPLY intent on first tick."""
    market = _mock_market()

    intent = strategy.decide(market)

    assert intent.intent_type.value == "SUPPLY"
    assert intent.token == "wstETH"
    assert intent.protocol == "aave_v3"
