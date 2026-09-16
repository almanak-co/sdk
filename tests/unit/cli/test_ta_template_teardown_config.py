"""Generated TA entries and exits consume the configured tolerance and route."""

import importlib.util
import json
import sys
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from click.testing import CliRunner

from almanak.framework.cli.new_strategy import new_strategy
from almanak.framework.teardown import TeardownMode


@pytest.fixture
def generated(tmp_path):
    name = "ta_teardown_regression"
    target = tmp_path / name
    result = CliRunner().invoke(
        new_strategy,
        ["--template", "ta_swap", "--name", name, "--chain", "arbitrum", "--output-dir", str(target)],
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


@pytest.mark.parametrize("normal,hard", [(17, 125), (75, 450)])
def test_teardown_reuses_slippage_and_pinned_route(generated, normal, hard):
    cls, _, config = generated
    route = {"pool": "0xc6962004f452be9203591991d15f6b388e09e8d0", "fee_tier": 500}
    config.update(
        max_slippage_bps=normal, hard_teardown_max_slippage_bps=hard, protocol="uniswap_v3", swap_params=route
    )
    strategy = cls(config=config, chain="arbitrum", wallet_address="0x" + "1" * 40)
    market = MagicMock()
    market.balance.return_value = SimpleNamespace(balance=Decimal("1"), balance_usd=Decimal("3000"))
    market.price.return_value = Decimal("3000")
    for mode, expected in [(TeardownMode.SOFT, normal), (TeardownMode.HARD, hard)]:
        intent = strategy.generate_teardown_intents(mode=mode, market=market)[0]
        assert intent.max_slippage == Decimal(expected) / 10000
        assert intent.chain == "arbitrum"
        assert intent.protocol == "uniswap_v3"
        assert intent.swap_params == route


def test_automatic_route_remains_automatic(generated):
    cls, _, config = generated
    assert config["hard_teardown_max_slippage_bps"] == 300
    assert "protocol" not in config
    assert "swap_params" not in config
    strategy = cls(config=config, chain="arbitrum", wallet_address="0x" + "1" * 40)
    market = MagicMock()
    market.balance.return_value = SimpleNamespace(balance=Decimal("1"), balance_usd=Decimal("3000"))
    market.price.return_value = Decimal("3000")
    intent = strategy.generate_teardown_intents(mode=TeardownMode.SOFT, market=market)[0]
    assert intent.protocol is None and intent.swap_params is None


@pytest.mark.parametrize("field", ["max_slippage_bps", "hard_teardown_max_slippage_bps"])
@pytest.mark.parametrize("value", [-1, 10000, "NaN", "Infinity"])
def test_invalid_tolerance_rejected_before_running(generated, field, value):
    cls, _, config = generated
    config[field] = value
    with pytest.raises(ValueError):
        cls(config=config, chain="arbitrum", wallet_address="0x" + "1" * 40)


def test_buy_sell_and_exit_use_the_same_route(generated):
    cls, _, config = generated
    route = {"fee_tier": 500}
    config.update(protocol="uniswap_v3", swap_params=route)
    strategy = cls(config=config, chain="arbitrum", wallet_address="0x" + "1" * 40)
    market = MagicMock()
    market.chain = "arbitrum"
    market.price.return_value = Decimal("3000")
    market.rsi.return_value = SimpleNamespace(value=Decimal("20"))
    market.balance.side_effect = lambda token: SimpleNamespace(
        balance=Decimal("0") if token == "WETH" else Decimal("10000"), balance_usd=Decimal("10000")
    )
    buy = strategy.decide(market)
    assert buy.intent_type.value == "SWAP"
    assert buy.protocol == "uniswap_v3" and buy.swap_params == route
    market.balance.side_effect = None
    market.balance.return_value = SimpleNamespace(balance=Decimal("1"), balance_usd=Decimal("3000"))
    market.rsi.return_value = SimpleNamespace(value=Decimal("80"))
    sell = strategy.decide(market)
    assert sell.intent_type.value == "SWAP"
    assert (sell.from_token, sell.to_token) == ("WETH", "USDC")
    assert sell.protocol == "uniswap_v3" and sell.swap_params == route
    exit_intent = strategy.generate_teardown_intents(mode=TeardownMode.SOFT, market=market)[0]
    assert (exit_intent.protocol, exit_intent.swap_params) == (buy.protocol, buy.swap_params)
