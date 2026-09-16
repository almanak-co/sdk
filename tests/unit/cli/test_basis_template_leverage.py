"""Generated basis strategies expose and consistently size perp leverage."""

import importlib.util
import json
import sys
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from click.testing import CliRunner

from almanak.framework.cli.new_strategy import new_strategy


@pytest.fixture
def generated(tmp_path):
    name = "basis_leverage_regression"
    target = tmp_path / name
    result = CliRunner().invoke(
        new_strategy,
        ["--template", "basis_trade", "--name", name, "--chain", "arbitrum", "--output-dir", str(target)],
        env={"CI": ""},
    )
    assert result.exit_code == 0, result.output
    spec = importlib.util.spec_from_file_location(name, target / "strategy.py")
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    cls = next(
        c for c in vars(module).values() if isinstance(c, type) and c.__module__ == name and hasattr(c, "decide")
    )
    config = json.loads((target / "config.json").read_text())
    yield cls, module, config
    sys.modules.pop(name, None)


def _market(price="2", balance="100000"):
    market = MagicMock()
    market.price.return_value = Decimal(price)
    market.balance.return_value = SimpleNamespace(balance=Decimal(balance))
    market.funding_rate.return_value = SimpleNamespace(rate_hourly=Decimal("0.001"))
    return market


@pytest.mark.parametrize("leverage,ratio", [("2", "0.5"), ("10", "1"), ("5", "1.5")])
def test_collateral_uses_notional_leverage_and_measured_token_price(generated, leverage, ratio):
    cls, module, config = generated
    assert config["perp_leverage"] == "10"
    config.update(perp_leverage=leverage, hedge_ratio=ratio)
    strategy = cls(config=config, chain="arbitrum", wallet_address="0x" + "1" * 40)
    strategy._trade_state = module.BasisTradeState.SPOT_BOUGHT
    intent = strategy.decide(_market())
    assert intent.leverage == Decimal(leverage)
    assert intent.size_usd == Decimal(config["spot_size_usd"]) * Decimal(ratio)
    assert intent.collateral_amount * Decimal("2") * intent.leverage == intent.size_usd


@pytest.mark.parametrize("leverage", ["0", "-1", "0.5", "101", "NaN", "Infinity"])
def test_invalid_or_unsupported_leverage_rejected_at_construction(generated, leverage):
    cls, _, config = generated
    config["perp_leverage"] = leverage
    with pytest.raises(ValueError):
        cls(config=config, chain="arbitrum", wallet_address="0x" + "1" * 40)


def test_spot_entry_reserves_perp_collateral(generated):
    cls, _, config = generated
    strategy = cls(config=config, chain="arbitrum", wallet_address="0x" + "1" * 40)
    assert strategy.decide(_market(price="1", balance="10000")).intent_type.value == "HOLD"
    assert strategy.decide(_market(price="1", balance="11000")).intent_type.value == "SWAP"


def test_unavailable_balance_after_spot_fill_preserves_hedge_for_retry(generated):
    cls, module, config = generated
    strategy = cls(config=config, chain="arbitrum", wallet_address="0x" + "1" * 40)
    spot = strategy.decide(_market())
    strategy.on_intent_executed(spot, True, None)
    assert strategy._trade_state == module.BasisTradeState.SPOT_BOUGHT
    market = _market()
    market.balance.side_effect = ValueError("Balance unavailable")
    assert strategy.decide(market).intent_type.value == "HOLD"
    assert strategy._trade_state == module.BasisTradeState.SPOT_BOUGHT
    assert strategy.decide(_market()).intent_type.value == "PERP_OPEN"


@pytest.mark.parametrize("price", ["0", "-1", "NaN"])
def test_invalid_collateral_price_refuses_hedge(generated, price):
    cls, module, config = generated
    strategy = cls(config=config, chain="arbitrum", wallet_address="0x" + "1" * 40)
    strategy._trade_state = module.BasisTradeState.SPOT_BOUGHT
    assert strategy.decide(_market(price=price)).intent_type.value == "HOLD"


def test_entry_collateral_guard_does_not_block_perp_close(generated):
    cls, module, config = generated
    strategy = cls(config=config, chain="arbitrum", wallet_address="0x" + "1" * 40)
    strategy._trade_state = module.BasisTradeState.HEDGED
    market = _market()
    market.price.side_effect = lambda token: Decimal("3000") if token == "WETH" else Decimal("0")
    market.funding_rate.return_value.rate_hourly = Decimal("-0.001")
    assert strategy.decide(market).intent_type.value == "PERP_CLOSE"


@pytest.mark.parametrize(
    "state_name,expected,failed_token",
    [("IDLE", "SWAP", "WETH"), ("IDLE", "SWAP", "USDC"), ("SPOT_BOUGHT", "PERP_OPEN", "USDC")],
)
def test_unavailable_price_preserves_phase_for_retry(generated, state_name, expected, failed_token):
    cls, module, config = generated
    strategy = cls(config=config, chain="arbitrum", wallet_address="0x" + "1" * 40)
    state = getattr(module.BasisTradeState, state_name)
    strategy._trade_state = state
    market = _market()

    def price(token):
        if token == failed_token:
            raise ValueError("Price unavailable")
        return Decimal("2")

    market.price.side_effect = price
    assert strategy.decide(market).intent_type.value == "HOLD"
    assert strategy._trade_state == state
    assert strategy.decide(_market()).intent_type.value == expected


@pytest.mark.asyncio
@pytest.mark.parametrize("message,is_data_failure", [("RPC timeout", True), ("unsupported price feed", False)])
@pytest.mark.parametrize("failed_token", ["WETH", "USDC"])
async def test_price_failure_keeps_runner_data_classification(generated, message, is_data_failure, failed_token):
    from almanak.framework.market.builders import MarketSnapshotBuilder
    from almanak.framework.runner.failure_kind import kind_for_status
    from almanak.framework.runner.strategy_runner import IterationStatus
    from tests.unit.runner.test_run_iteration_steps import _make_runner, _make_state

    cls, module, config = generated
    strategy = cls(config=config, chain="arbitrum", wallet_address="0x" + "1" * 40)
    expected_state = module.BasisTradeState.IDLE if failed_token == "WETH" else module.BasisTradeState.SPOT_BOUGHT
    strategy._trade_state = expected_state

    def oracle(token, quote):
        if token == failed_token:
            raise ValueError(message)
        return Decimal("2")

    strategy._price_oracle = oracle
    state = _make_state(strategy)
    state.market = MarketSnapshotBuilder.for_strategy_runner(strategy=strategy, runtime_surface="unit_test")
    runner = _make_runner()
    assert await runner._step_decide(state) is None
    result = runner._step_extract_intents(state)
    assert result.status == IterationStatus.DATA_ERROR
    assert kind_for_status(result.status, result.error).is_data_class is is_data_failure
    assert strategy._trade_state == expected_state


@pytest.mark.parametrize("state_name,expected", [("SPOT_BOUGHT", "PERP_OPEN"), ("HEDGED", "PERP_CLOSE")])
def test_entry_price_is_not_required_to_hedge_or_close(generated, state_name, expected):
    cls, module, config = generated
    strategy = cls(config=config, chain="arbitrum", wallet_address="0x" + "1" * 40)
    strategy._trade_state = getattr(module.BasisTradeState, state_name)
    market = _market()

    def price(token):
        if token == strategy.base_token:
            raise ValueError("Base price unavailable")
        return Decimal("2")

    market.price.side_effect = price
    market.funding_rate.return_value.rate_hourly = Decimal("-0.001")
    assert strategy.decide(market).intent_type.value == expected
    assert all(call.args[0] != strategy.base_token for call in market.price.call_args_list)
