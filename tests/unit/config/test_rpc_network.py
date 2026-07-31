"""Closed-vocabulary coverage for RPC-managed runtime networks (ALM-3082)."""

from unittest.mock import patch

import pytest
from pydantic import ValidationError

from almanak.config.runtime import ConfigurationError, RuntimeConfig, runtime_config_from_env
from almanak.core.rpc_network import NETWORK_PROFILES, Network
from almanak.framework.execution.config import MultiChainRuntimeConfig
from almanak.gateway.core.settings import GatewaySettings
from almanak.gateway.utils.rpc_provider import NodeProvider, get_rpc_url, get_rpc_url_cached

_TEST_PRIVATE_KEY = "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80"


def test_every_declared_network_has_explicit_rpc_routing_semantics() -> None:
    """A new enum member cannot ship without updating the extension table."""
    assert set(NETWORK_PROFILES) == set(Network)


@pytest.mark.parametrize(
    ("network", "expected_url"),
    [
        (Network.MAINNET, "https://eth-mainnet.g.alchemy.com/v2/test-key"),
        (Network.TESTNET, "https://eth-sepolia.g.alchemy.com/v2/test-key"),
        (Network.SEPOLIA, "https://eth-sepolia.g.alchemy.com/v2/test-key"),
        (Network.ANVIL, "http://127.0.0.1:8549"),
    ],
)
def test_every_declared_network_has_rpc_lookup_coverage(network: Network, expected_url: str) -> None:
    """Every enum member must resolve through its declared routing profile."""
    with patch.dict("os.environ", {"ALCHEMY_API_KEY": "test-key"}, clear=True):
        assert get_rpc_url("ethereum", network=network, provider=NodeProvider.ALCHEMY) == expected_url


def test_rpc_cache_keys_are_canonical_networks() -> None:
    """Equivalent string and enum inputs must share one RPC cache entry."""
    get_rpc_url_cached.cache_clear()
    try:
        with patch(
            "almanak.gateway.utils.rpc_provider.get_rpc_url",
            return_value="https://rpc.example",
        ) as lookup:
            assert get_rpc_url_cached("arbitrum", network=" MAINNET ") == "https://rpc.example"
            assert get_rpc_url_cached("arbitrum", network=Network.MAINNET) == "https://rpc.example"

        lookup.assert_called_once_with("arbitrum", Network.MAINNET)
    finally:
        get_rpc_url_cached.cache_clear()


@pytest.mark.parametrize("network", list(Network))
def test_gateway_settings_parse_and_serialize_each_network(network: Network) -> None:
    """Gateway settings must carry enums internally and strings on the wire."""
    settings = GatewaySettings(network=network.value, polymarket_network=network.value)

    assert settings.network is network
    assert settings.polymarket_network is network
    assert type(settings.model_dump()["network"]) is str
    assert settings.model_dump()["network"] == network.value
    assert type(settings.model_dump()["polymarket_network"]) is str
    assert settings.model_dump()["polymarket_network"] == network.value


@pytest.mark.parametrize("field", ["network", "polymarket_network"])
def test_gateway_settings_reject_invalid_network_at_parse_time(field: str) -> None:
    """Both gateway-owned network fields reject open-ended values."""
    with pytest.raises(ValidationError, match=r"Unknown network 'devnet'.*mainnet.*anvil"):
        GatewaySettings.model_validate({field: "devnet"})


def test_runtime_factory_rejects_invalid_network_before_rpc_resolution() -> None:
    """Runtime construction must reject invalid networks before any RPC lookup."""
    with patch("almanak.gateway.utils.rpc_provider.get_rpc_url") as get_rpc_url_mock:
        with pytest.raises(ConfigurationError, match=r"'network'.*Unknown network 'devnet'") as exc_info:
            runtime_config_from_env(chain="arbitrum", network="devnet")

    assert exc_info.value.field == "network"
    get_rpc_url_mock.assert_not_called()


def test_runtime_model_preserves_wire_value_while_carrying_enum() -> None:
    """RuntimeConfig must normalize ingress without changing serialized shape."""
    runtime = RuntimeConfig(single_chain=True, chain="arbitrum", network=" SEPOLIA ")

    assert runtime.network is Network.SEPOLIA
    assert type(runtime.model_dump()["network"]) is str
    assert runtime.model_dump()["network"] == "sepolia"


def test_execution_config_normalizes_direct_construction_boundary() -> None:
    """Legacy direct construction must still enter the canonical enum domain."""
    with patch.object(MultiChainRuntimeConfig, "_load_rpc_urls"):
        config = MultiChainRuntimeConfig(
            chains=["arbitrum"],
            protocols={"arbitrum": ["uniswap_v3"]},
            private_key=_TEST_PRIVATE_KEY,
            network=" ANVIL ",
        )

    assert config.network is Network.ANVIL


def test_execution_config_routes_each_chain_through_anvil_network() -> None:
    """Typed Anvil config must select each chain's configured local RPC port."""
    env = {
        "ANVIL_ARBITRUM_PORT": "18545",
        "ANVIL_BASE_PORT": "28545",
    }
    with patch.dict("os.environ", env, clear=True), patch("almanak.config.runtime._load_dotenv_once"):
        config = MultiChainRuntimeConfig(
            chains=["arbitrum", "base"],
            protocols={"arbitrum": ["uniswap_v3"], "base": ["uniswap_v3"]},
            private_key=_TEST_PRIVATE_KEY,
            network=" AnViL ",
        )

    assert config.network is Network.ANVIL
    assert config.rpc_urls == {
        "arbitrum": "http://127.0.0.1:18545",
        "base": "http://127.0.0.1:28545",
    }
