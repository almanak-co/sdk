"""Tests for multi-chain sidecar deployment mode.

Covers the fix that allows multi-chain strategies to run in sidecar mode
(--no-gateway without ALMANAK_PRIVATE_KEY) when ALMANAK_GATEWAY_WALLETS is set.

Three aspects tested:
1. MultiChainRuntimeConfig.from_env() accepts ALMANAK_GATEWAY_WALLETS without private key
2. _load_rpc_urls() skips RPC loading in gateway wallets mode
3. _load_rpc_urls() still loads RPCs when private key is present
"""

import os
from unittest.mock import patch

import pytest

from almanak.framework.execution.config import (
    MissingEnvironmentVariableError,
    MultiChainRuntimeConfig,
)

# Deterministic test key (Anvil default #0)
TEST_PRIVATE_KEY = "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80"
TEST_DERIVED_EOA = "0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266"

# Minimal gateway wallets JSON for two chains
GATEWAY_WALLETS_JSON = (
    '{"base":{"type":"direct","wallet_address":"0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266"},'
    '"arbitrum":{"type":"direct","wallet_address":"0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266"}}'
)


@pytest.fixture(autouse=True)
def _no_dotenv():
    """Prevent load_dotenv from loading .env file during tests."""
    with patch("almanak.framework.execution.config.load_dotenv"):
        yield


class TestMultiChainSidecarConfig:
    """Test MultiChainRuntimeConfig with gateway wallets (no private key)."""

    def test_from_env_accepts_gateway_wallets_without_private_key(self):
        """ALMANAK_GATEWAY_WALLETS set + no ALMANAK_PRIVATE_KEY should succeed."""
        env = {
            "ALMANAK_GATEWAY_WALLETS": GATEWAY_WALLETS_JSON,
        }

        with patch.dict(os.environ, env, clear=True):
            config = MultiChainRuntimeConfig.from_env(
                chains=["base", "arbitrum"],
                protocols={"base": ["uniswap_v3"], "arbitrum": ["uniswap_v3"]},
                network="mainnet",
            )
            # Private key should be empty (gateway handles signing)
            assert config.private_key == ""
            # Wallet address is placeholder (resolved later by register_chains)
            assert config.wallet_address == ""
            # Chains should be set correctly
            assert config.chains == ["base", "arbitrum"]
            # RPC URLs should be empty (gateway handles RPC)
            assert config.rpc_urls == {}

    def test_from_env_without_gateway_wallets_or_private_key_raises(self):
        """No ALMANAK_GATEWAY_WALLETS and no ALMANAK_PRIVATE_KEY should raise."""
        with patch.dict(os.environ, {}, clear=True):
            with pytest.raises(MissingEnvironmentVariableError):
                MultiChainRuntimeConfig.from_env(
                    chains=["base", "arbitrum"],
                    protocols={"base": ["uniswap_v3"], "arbitrum": ["uniswap_v3"]},
                    network="mainnet",
                )

    def test_from_env_with_private_key_still_loads_rpc_urls(self):
        """When private key is present, RPC URLs should still be loaded."""
        env = {
            "ALMANAK_PRIVATE_KEY": TEST_PRIVATE_KEY,
        }

        with patch.dict(os.environ, env, clear=True):
            config = MultiChainRuntimeConfig.from_env(
                chains=["arbitrum"],
                protocols={"arbitrum": ["uniswap_v3"]},
                network="mainnet",
            )
            assert config.wallet_address == TEST_DERIVED_EOA
            # RPC URLs should be populated (built dynamically from public RPCs)
            assert "arbitrum" in config.rpc_urls
            assert config.rpc_urls["arbitrum"] != ""


class TestLoadRpcUrlsGatewayWallets:
    """Test _load_rpc_urls skips loading when gateway wallets are configured."""

    def test_rpc_urls_empty_in_gateway_wallets_mode(self):
        """Gateway wallets mode should skip RPC URL loading entirely."""
        env = {
            "ALMANAK_GATEWAY_WALLETS": GATEWAY_WALLETS_JSON,
        }

        with patch.dict(os.environ, env, clear=True):
            config = MultiChainRuntimeConfig.from_env(
                chains=["base", "arbitrum"],
                protocols={"base": ["uniswap_v3"], "arbitrum": ["uniswap_v3"]},
                network="mainnet",
            )
            # No RPC URLs loaded - gateway handles all RPC access
            assert config.rpc_urls == {}

    def test_rpc_urls_loaded_with_private_key_no_gateway_wallets(self):
        """When private key is present and no gateway wallets, RPC URLs should load."""
        env = {
            "ALMANAK_PRIVATE_KEY": TEST_PRIVATE_KEY,
        }

        with patch.dict(os.environ, env, clear=True):
            config = MultiChainRuntimeConfig.from_env(
                chains=["arbitrum"],
                protocols={"arbitrum": ["uniswap_v3"]},
                network="mainnet",
            )
            # RPC URLs should be populated
            assert "arbitrum" in config.rpc_urls

    def test_safe_signer_skipped_in_gateway_wallets_mode(self):
        """Gateway wallets mode should skip local Safe signer creation."""
        env = {
            "ALMANAK_GATEWAY_WALLETS": GATEWAY_WALLETS_JSON,
            "ALMANAK_EXECUTION_MODE": "safe_zodiac",
        }

        with patch.dict(os.environ, env, clear=True):
            config = MultiChainRuntimeConfig.from_env(
                chains=["base", "arbitrum"],
                protocols={"base": ["uniswap_v3"], "arbitrum": ["uniswap_v3"]},
                network="mainnet",
            )
            # Safe signer should NOT be created (gateway handles it)
            assert config.safe_signer is None
