"""Cost-policy presence survives the managed runner and standalone teardown lanes."""

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from almanak.config.cli_runtime import gas_cost_overrides
from almanak.config.env import gateway_config_from_env
from almanak.config.runtime import ConfigurationError
from almanak.framework.cli import _run_components as run
from almanak.framework.cli import teardown_helpers
from almanak.framework.cli._run_context import ComponentBundle
from almanak.framework.execution.config import GatewayRuntimeConfig, LocalRuntimeConfig
from almanak.framework.execution.multichain import MultiChainOrchestrator
from almanak.framework.strategies.metadata import StrategyDataRequirements

WALLET = "0x1234567890abcdef1234567890abcdef12345678"
FIELDS = ("max_gas_cost_native", "max_gas_cost_usd")


@pytest.fixture(autouse=True)
def isolated_cost_environment(monkeypatch):
    for field in FIELDS:
        for prefix in ("", "ALMANAK_", "ALMANAK_GATEWAY_"):
            monkeypatch.delenv(prefix + field.upper(), raising=False)


@pytest.mark.parametrize("raw, expected", [(None, None), ("0", 0.0), ("0.025", 0.025)])
@pytest.mark.parametrize("prefix", ["ALMANAK_", ""])
def test_cli_and_gateway_resolve_same_explicit_policy(monkeypatch, raw, expected, prefix):
    if raw is not None:
        for field in FIELDS:
            monkeypatch.setenv(prefix + field.upper(), raw)
    overrides = gas_cost_overrides()
    gateway = gateway_config_from_env()
    for field in FIELDS:
        assert overrides.get(field) == expected
        assert (field in overrides) is (raw is not None)
        assert getattr(gateway, field) == expected


@pytest.mark.parametrize("raw", ["", "bad", "-1", "NaN", "inf", "-inf"])
@pytest.mark.parametrize("field", FIELDS)
def test_invalid_cost_policy_fails_at_configuration_boundary(monkeypatch, raw, field):
    monkeypatch.setenv("ALMANAK_" + field.upper(), raw)
    with pytest.raises(ConfigurationError, match="finite nonnegative"):
        gas_cost_overrides()
    with pytest.raises(ConfigurationError, match="finite nonnegative"):
        gateway_config_from_env()


def test_gateway_explicit_policy_takes_precedence_over_legacy(monkeypatch):
    for field in FIELDS:
        monkeypatch.setenv("ALMANAK_GATEWAY_" + field.upper(), "0")
        monkeypatch.setenv("ALMANAK_" + field.upper(), "invalid")
    gateway = gateway_config_from_env()
    assert gateway.max_gas_cost_native == 0
    assert gateway.max_gas_cost_usd == 0


def test_prefixed_policy_wins_over_legacy_and_runtime(monkeypatch):
    monkeypatch.setenv("ALMANAK_MAX_GAS_COST_USD", "0")
    monkeypatch.setenv("MAX_GAS_COST_USD", "9")
    runtime = SimpleNamespace(max_gas_cost_native=0.02, max_gas_cost_usd=4)
    assert gas_cost_overrides(runtime) == {"max_gas_cost_native": 0.02, "max_gas_cost_usd": 0}


def _factories():
    return SimpleNamespace(
        price_oracle=MagicMock(),
        balance_provider=MagicMock(),
        multi_chain_orchestrator=MultiChainOrchestrator,
    )


def _isolate_market_data(monkeypatch):
    monkeypatch.setattr(run, "_wire_market_data_services", MagicMock())
    monkeypatch.setattr(run, "_wire_optional_data_services", MagicMock())
    monkeypatch.setattr(run, "_maybe_start_solana_fork", MagicMock())


@pytest.mark.parametrize("surface", ["local", "hosted", "multichain", "teardown"])
@pytest.mark.parametrize("caps", [None, (0.0, 0.0), (0.02, 4.0)])
def test_actual_cli_factories_preserve_cost_policy(monkeypatch, surface, caps):
    if caps is not None:
        for field, value in zip(FIELDS, caps, strict=True):
            monkeypatch.setenv("ALMANAK_" + field.upper(), str(value))
    _isolate_market_data(monkeypatch)
    client = MagicMock()
    common = {
        "strategy_config": {},
        "gateway_client": client,
        "chain_wallets": {},
        "strategy_instance": SimpleNamespace(),
        "requirements": StrategyDataRequirements(),
        "factories": _factories(),
    }
    if surface == "multichain":
        runtime = SimpleNamespace(
            execution_address=WALLET,
            max_gas_price_gwei=50,
            simulation_enabled=True,
            rpc_urls={},
            max_gas_cost_native=0,
            max_gas_cost_usd=0,
        )
        built = run._build_multi_chain_providers(
            runtime_config=runtime,
            strategy_chains=["bsc", "arbitrum"],
            **common,
        )
        orchestrators = [built.execution_orchestrator._get_gateway_orchestrator(c) for c in ("bsc", "arbitrum")]
    elif surface == "teardown":
        from almanak.framework.intents import compiler
        from almanak.framework.teardown import state_manager

        monkeypatch.setattr(compiler, "IntentCompiler", MagicMock())
        monkeypatch.setattr(state_manager, "TeardownStateAdapter", MagicMock())
        machinery = teardown_helpers.build_teardown_machinery(
            gateway_client=client,
            chain="bsc",
            wallet_address=WALLET,
            price_oracle=MagicMock(),
            no_accounting=False,
            network="mainnet",
        )
        orchestrators = [machinery.orchestrator]
    else:
        runtime = (
            LocalRuntimeConfig(chain="bsc", rpc_url="http://unused.invalid", private_key="0x" + "01" * 32)
            if surface == "local"
            else GatewayRuntimeConfig(chain="bsc", wallet_address=WALLET, is_safe=True)
        )
        built = run._build_single_chain_providers(
            runtime_config=runtime,
            resolved_network="mainnet",
            components=ComponentBundle(),
            **common,
        )
        orchestrators = [built.execution_orchestrator]
    for orchestrator in orchestrators:
        assert orchestrator._max_gas_cost_native == (caps[0] if caps else None)
        assert orchestrator._max_gas_cost_usd == (caps[1] if caps else None)
    client.execution.Execute.assert_not_called()
    client.execution.ExecuteWithGasPolicy.assert_not_called()


@pytest.mark.parametrize("caps", [(None, None), (0.0, 0.0), (0.02, 4.0), (None, 4.0)])
def test_programmatic_multichain_policy_applies_to_each_chain(caps):
    orchestrator = MultiChainOrchestrator.from_gateway(
        gateway_client=MagicMock(),
        chains=["bsc", "base"],
        wallet_address=WALLET,
        max_gas_cost_native=caps[0],
        max_gas_cost_usd=caps[1],
    )
    for chain in ("bsc", "base"):
        child = orchestrator._get_gateway_orchestrator(chain)
        assert child._max_gas_cost_native == caps[0]
        assert child._max_gas_cost_usd == caps[1]


def test_local_factory_retains_programmatic_runtime_caps(monkeypatch):
    _isolate_market_data(monkeypatch)
    runtime = LocalRuntimeConfig(
        chain="bsc",
        rpc_url="http://unused.invalid",
        private_key="0x" + "01" * 32,
        max_gas_cost_native=0.02,
        max_gas_cost_usd=4,
    )
    built = run._build_single_chain_providers(
        runtime_config=runtime,
        resolved_network="mainnet",
        components=ComponentBundle(),
        strategy_config={},
        gateway_client=MagicMock(),
        chain_wallets={},
        strategy_instance=SimpleNamespace(),
        requirements=StrategyDataRequirements(),
        factories=_factories(),
    )
    assert built.execution_orchestrator._max_gas_cost_native == 0.02
    assert built.execution_orchestrator._max_gas_cost_usd == 4
