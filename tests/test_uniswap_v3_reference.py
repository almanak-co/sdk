"""Reusable V3 author hooks execute in isolation against retained captured observations."""

from pathlib import Path

import pytest

from almanak.configuration.backtest_inputs import read_backtest_inputs
from almanak.configuration.unresolved import read_inputs
from almanak.connectors.uniswap_v3.state import DecisionInput
from almanak.runtime import strategy_capabilities, strategy_decision, validate_strategy_config
from almanak.strategy import StrategyDefinition
from almanak.strategy_api import Memory
from almanak.triggers import DecisionSchedule
from tests.test_uniswap_v3_validation import state_for

ROOT = Path(__file__).parents[1]


@pytest.mark.parametrize("chain,index", [("ethereum", 0), ("base", 1)])
@pytest.mark.parametrize("style", ["trading", "lp"])
def test_reference_config_and_first_decision_execute_in_sandbox(chain, index, style):
    directory = ROOT / f"strategies/uniswap_v3_{chain}_{style}"
    inputs = read_inputs(directory / "config.yaml").inputs
    settings = read_backtest_inputs(directory / "backtest-economic.yaml").inputs
    assert settings.data["source"] == "uniswap_v3_archive"
    assert "decision_interval_override_seconds" not in settings.schedule()
    definition = StrategyDefinition(
        filename="strategy.py", source=(directory / "strategy.py").read_text()
    )
    state, observed, _ = state_for(chain=index)
    # Retained fork measurements exercise the same source hooks with their exact target.
    allocation = state.config.allocations[0]
    if style == "trading":
        allocation = allocation.model_copy(update={"range": None})
    config = state.config.model_copy(update={"allocations": (allocation,)})
    target = config.target(allocation.name)
    owned = observed.owned.model_copy(update={"target": target})
    quote = observed.entry_quote.model_copy(update={"source": target})
    observed = observed.model_copy(update={"target": target, "owned": owned, "entry_quote": quote})
    validate_strategy_config(definition, config)
    market = inputs.markets[inputs.strategy.allocations["position"].market]
    assert market.chain == allocation.pool.chain.chain_id
    assert market.pool == allocation.pool.pool
    capabilities = strategy_capabilities(definition, config)
    context = DecisionInput(
        config=config,
        memory=Memory(),
        owned=(owned,),
        observations=(observed,),
        trigger=DecisionSchedule().event(observed.effective_at),
    )
    result = strategy_decision(definition, context)
    assert result.intent.kind == "swap_exact_input"
    assert result.intent.target == target
    assert capabilities.requirements[0].target == target
