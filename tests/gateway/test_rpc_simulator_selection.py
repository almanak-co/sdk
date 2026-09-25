"""Gateway simulator selection: explicit node backend, or the node when it is the only one covering a live chain."""

from unittest.mock import MagicMock, patch

import pytest
from pydantic import ValidationError

from almanak.config.simulation import SimulationConfig as TypedSimulationConfig
from almanak.config.simulation import simulation_config_from_env
from almanak.core.chains import ChainRegistry
from almanak.core.enums import ChainFamily
from almanak.core.rpc_network import Network
from almanak.framework.execution.simulator import create_simulator
from almanak.framework.execution.simulator.config import SimulationConfig
from almanak.gateway.services.rpc_simulator import GatewayRpcSimulator, create_gateway_simulator

CHAINS = [chain.name for chain in ChainRegistry.all() if chain.family is ChainFamily.EVM]
NODE_ONLY_CHAINS = [
    chain.name
    for chain in ChainRegistry.all()
    if chain.family is ChainFamily.EVM
    and chain.simulation.node_simulate_v1
    and not (chain.simulation.tenderly_supported or chain.simulation.alchemy_network)
]
FALLBACK_CHAINS = [chain for chain in CHAINS if chain not in NODE_ONLY_CHAINS]


def test_robinhood_is_simulated_by_its_own_node():
    assert "robinhood" in NODE_ONLY_CHAINS
    assert "hyperevm" in FALLBACK_CHAINS


@pytest.mark.parametrize("chain", FALLBACK_CHAINS)
@pytest.mark.parametrize("network", [Network.MAINNET, Network.ANVIL])
def test_default_backend_preserves_existing_selection_for_other_evm_chains(chain, network):
    config = SimulationConfig(enabled=True)
    with patch("almanak.framework.execution.simulator.create_simulator") as original:
        selected = create_gateway_simulator(
            config=config, rpc_url="https://rpc.example.invalid", chain=chain, network=network
        )
    assert selected is original.return_value
    original.assert_called_once_with(config=config, rpc_url="https://rpc.example.invalid")


@pytest.mark.parametrize("chain", NODE_ONLY_CHAINS)
def test_default_backend_uses_the_node_on_a_live_chain_no_vendor_covers(chain):
    config = SimulationConfig(enabled=True)
    with (
        patch("almanak.gateway.services.rpc_simulator.get_cached_web3", return_value=MagicMock()),
        patch("almanak.framework.execution.simulator.create_simulator") as original,
    ):
        selected = create_gateway_simulator(
            config=config, rpc_url="https://rpc.example.invalid", chain=chain, network=Network.MAINNET
        )
    assert isinstance(selected, GatewayRpcSimulator)
    assert selected.supports_chain(chain)
    original.assert_not_called()


@pytest.mark.parametrize("chain", NODE_ONLY_CHAINS)
def test_default_backend_keeps_the_snapshotting_simulator_on_a_managed_fork(chain):
    config = SimulationConfig(enabled=True)
    with patch("almanak.framework.execution.simulator.create_simulator") as original:
        selected = create_gateway_simulator(
            config=config, rpc_url="http://127.0.0.1:8545", chain=chain, network=Network.ANVIL
        )
    assert selected is original.return_value


@pytest.mark.parametrize("chain", CHAINS)
def test_explicit_node_backend_is_bound_to_the_selected_chain(chain):
    config = SimulationConfig(enabled=True, backend="rpc", timeout_seconds=40)
    with (
        patch("almanak.gateway.services.rpc_simulator.get_cached_web3", return_value=MagicMock()) as provider,
        patch("almanak.framework.execution.simulator.create_simulator") as original,
    ):
        selected = create_gateway_simulator(
            config=config, rpc_url="https://rpc.example.invalid", chain=chain, network=Network.MAINNET
        )
    assert isinstance(selected, GatewayRpcSimulator)
    assert selected.supports_chain(chain)
    assert not selected.supports_chain("not_the_configured_chain")
    provider.assert_called_once_with(chain, Network.MAINNET)
    original.assert_not_called()


def test_disabled_simulation_does_not_create_a_node_backend():
    with (
        patch("almanak.gateway.services.rpc_simulator.get_cached_web3") as provider,
        patch("almanak.framework.execution.simulator.create_simulator") as original,
    ):
        config = SimulationConfig(enabled=False, backend="rpc")
        assert (
            create_gateway_simulator(
                config=config, rpc_url="https://rpc.example.invalid", chain="robinhood", network=Network.MAINNET
            )
            is original.return_value
        )
    provider.assert_not_called()


def test_typed_backend_is_explicit_and_unknown_values_refuse(monkeypatch):
    monkeypatch.setattr("almanak.config.simulation._load_dotenv_once", lambda _: None)
    monkeypatch.delenv("ALMANAK_SIMULATION_BACKEND", raising=False)
    assert simulation_config_from_env().backend == "auto"
    monkeypatch.setenv("ALMANAK_SIMULATION_BACKEND", "rpc")
    assert simulation_config_from_env().backend == "rpc"
    with pytest.raises(ValidationError):
        TypedSimulationConfig(backend="rpcc")


@pytest.mark.parametrize("rpc_url", [None, "http://localhost:8545", "https://rpc.example.invalid"])
def test_framework_factory_cannot_silently_ignore_explicit_rpc_backend(rpc_url):
    with pytest.raises(ValueError, match="chain-bound simulator factory"):
        create_simulator(config=SimulationConfig(enabled=True, backend="rpc"), rpc_url=rpc_url)


def test_explicit_rpc_configuration_does_not_require_vendor_credentials():
    config = SimulationConfig(enabled=True, backend="rpc")
    assert config.should_simulate()
    assert all(config.can_simulate_chain(chain) for chain in CHAINS)
    assert not config.can_simulate_chain("solana")
    assert not config.can_simulate_chain("unknown")
    config.enabled = False
    assert not config.should_simulate()
