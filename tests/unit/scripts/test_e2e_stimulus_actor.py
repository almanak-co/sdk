"""The external stimulus is bounded independently of subject decisions."""

import json
from dataclasses import asdict, replace
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from web3.providers import BaseProvider

from almanak.framework.anvil.accounts import anvil_default_address
from qa_lab.strategies.lp_stimulus.strategy import PoolStimulusStrategy, StimulusConfig


@pytest.fixture
def config():
    return StimulusConfig(
        "test-instance", 100, "0x" + "aa" * 32, 1000000, 1000000, anvil_default_address(0), 5, "0.001", 3, 6, 18
    )


@pytest.fixture
def actor(config, monkeypatch):
    strategy = PoolStimulusStrategy(config=config, chain="arbitrum", wallet_address=anvil_default_address(1))
    client = SimpleNamespace(eth=SimpleNamespace(get_transaction_count=Mock(return_value=5)))
    monkeypatch.setattr(strategy, "_client", lambda: client)
    return strategy, client


def test_actor_emits_one_exact_fee_pinned_swap_then_holds(actor):
    strategy, client = actor
    intent = strategy.decide(None)
    assert intent.from_token == "USDC" and intent.to_token == "WETH"
    assert intent.amount == Decimal(1)
    assert intent.max_slippage == Decimal("0.001")
    assert intent.swap_params == {"fee_tier": 500}
    assert strategy.decide(None).intent_type.value == "HOLD"
    client.eth.get_transaction_count.assert_called_once_with(anvil_default_address(1), "pending")


def test_actor_accepts_configuration_loaded_by_the_sdk_cli(config, tmp_path):
    from almanak.framework.cli._strategy_config import coerce_strategy_config
    from almanak.framework.cli.run import load_strategy_config

    path = tmp_path / "config.json"
    path.write_text(json.dumps({**asdict(config), "chain": "arbitrum"}, default=str))
    loaded = load_strategy_config("qa_lp_stimulus", config_file=str(path))
    typed = coerce_strategy_config(PoolStimulusStrategy, loaded, echo=False)
    assert typed.max_slippage == Decimal("0.001")
    actor = PoolStimulusStrategy(config=typed, chain="arbitrum", wallet_address=anvil_default_address(1))
    assert actor.config.max_slippage == Decimal("0.001")


def test_actor_refuses_replay_when_local_attempt_state_was_lost(actor):
    strategy, client = actor
    client.eth.get_transaction_count.return_value = 6
    with pytest.raises(ValueError, match="ambiguous retry"):
        strategy.decide(None)


@pytest.mark.parametrize(
    "change",
    [
        {"amount_usdc_raw": 1000001},
        {"amount_usdc_raw": True},
        {"fork_hash": "0x123"},
        {"max_slippage": "0.006"},
        {"max_slippage": "NaN"},
        {"max_slippage": "invalid"},
        {"max_slippage": 0.001},
        {"max_swaps": 0},
        {"usdc_decimals": 18},
        {"weth_decimals": 6},
    ],
)
def test_actor_rejects_invalid_input_before_rpc(config, change):
    with pytest.raises(ValueError, match="bounded Anvil"):
        PoolStimulusStrategy(
            config=replace(config, **change), chain="arbitrum", wallet_address=anvil_default_address(1)
        )


def test_actor_cannot_use_subject_wallet(config):
    with pytest.raises(ValueError, match="bounded Anvil"):
        PoolStimulusStrategy(
            config=replace(config, subject_wallet=anvil_default_address(1)),
            chain="arbitrum",
            wallet_address=anvil_default_address(1),
        )


def test_actor_cleanup_uses_measured_weth_inventory(actor, monkeypatch):
    strategy, _ = actor
    monkeypatch.setattr(strategy, "_weth_balance", lambda: Decimal("0.123456789"))
    intents = strategy.generate_teardown_intents(None)
    assert len(intents) == 1
    assert intents[0].amount == Decimal("0.123456789")
    assert intents[0].max_slippage == Decimal("0.001")
    assert intents[0].from_token == "WETH" and intents[0].to_token == "USDC"
    monkeypatch.setattr(strategy, "_weth_balance", lambda: Decimal(0))
    assert strategy.generate_teardown_intents(None) == []


@pytest.mark.parametrize("failure", [None, "mainnet", "instance", "chain", "origin"])
def test_actor_gateway_identity_is_checked_before_dispatch(config, monkeypatch, failure):
    calls = []

    class Provider(BaseProvider):
        def make_request(self, method, params):
            calls.append(method)
            if method == "anvil_metadata":
                if failure == "mainnet":
                    return {"jsonrpc": "2.0", "id": 1, "error": {"code": -32601, "message": "not allowed"}}
                result = {
                    "instanceId": "wrong" if failure == "instance" else "test-instance",
                    "forkedNetwork": {"forkBlockNumber": 100},
                }
            elif method == "eth_chainId":
                result = hex(1 if failure == "chain" else 42161)
            elif method == "eth_getBlockByNumber":
                result = {"number": "0x64", "hash": "0x" + ("bb" if failure == "origin" else "aa") * 32}
            elif method == "eth_getTransactionCount":
                result = "0x5"
            else:
                raise AssertionError(method)
            return {"jsonrpc": "2.0", "id": 1, "result": result}

    monkeypatch.setattr("qa_lab.strategies.lp_stimulus.strategy.GatewayWeb3Provider", lambda *args: Provider())
    strategy = PoolStimulusStrategy(config=config, chain="arbitrum", wallet_address=anvil_default_address(1))
    strategy._gateway_client = object()
    if failure:
        with pytest.raises(ValueError, match="bound Anvil|origin changed"):
            strategy.decide(None)
        assert "eth_getTransactionCount" not in calls
    else:
        assert strategy.decide(None).intent_type.value == "SWAP"


def test_persisted_attempt_prevents_repeat_after_restart(actor, config, monkeypatch):
    strategy, client = actor
    strategy.decide(None)
    restored = PoolStimulusStrategy(config=config, chain="arbitrum", wallet_address=anvil_default_address(1))
    monkeypatch.setattr(restored, "_client", lambda: client)
    restored.load_persistent_state(strategy.get_persistent_state())
    assert restored.decide(None).intent_type.value == "HOLD"
