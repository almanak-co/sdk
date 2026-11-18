"""A separate trading asset cannot make reference teardown consume existing fund cash."""

from pathlib import Path

import pytest

from almanak.runtime import validate_strategy_config
from almanak.strategy import StrategyDefinition
from tests.composed_public import resolved_parameters

REFERENCE = Path(__file__).parents[1] / "strategies/avalanche_aave_v4_borrow_and_trade"


@pytest.mark.parametrize("quantity", (0, 1))
def test_distinct_counter_asset_initial_cash_is_checked_inside_sandbox(quantity):
    _, author = resolved_parameters(REFERENCE.name)
    counter = author.target("trade").pool.currency0
    assert counter != author.target("collateral").asset
    inventory = tuple(
        balance.model_copy(update={"amount": quantity}) if balance.key.asset == counter else balance
        for balance in author.inventory
    )
    parameters = author.model_copy(update={"inventory": inventory})
    strategy = StrategyDefinition.read(REFERENCE / "strategy.py")
    if quantity == 0:
        validate_strategy_config(strategy, parameters)
    else:
        with pytest.raises(RuntimeError, match="counter-asset must start with no fund cash"):
            validate_strategy_config(strategy, parameters)
