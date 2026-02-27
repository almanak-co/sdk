"""Tests for IntentCompiler._get_chain_rpc_url() Anvil fork detection.

When a managed gateway starts an Anvil fork, it sets ANVIL_{CHAIN}_PORT env var.
The compiler must use this to route RPC calls to the fork, not mainnet.
This is critical for LP_CLOSE compilation which queries on-chain position state.

Fixes VIB-233.
"""

import os
from unittest.mock import MagicMock, patch

import pytest

from almanak.framework.intents.compiler import IntentCompiler, IntentCompilerConfig


@pytest.fixture
def compiler_no_rpc():
    """Create a compiler without an explicit rpc_url (gateway mode)."""
    return IntentCompiler(
        chain="avalanche",
        wallet_address="0x1234567890abcdef1234567890abcdef12345678",
        rpc_url=None,
        config=IntentCompilerConfig(allow_placeholder_prices=True),
    )


@pytest.fixture
def compiler_with_rpc():
    """Create a compiler with an explicit rpc_url."""
    return IntentCompiler(
        chain="avalanche",
        wallet_address="0x1234567890abcdef1234567890abcdef12345678",
        rpc_url="https://explicit-rpc.example.com",
        config=IntentCompilerConfig(allow_placeholder_prices=True),
    )


class TestGetChainRpcUrlAnvilDetection:
    """Test that _get_chain_rpc_url() detects running Anvil forks."""

    def test_explicit_rpc_url_takes_precedence(self, compiler_with_rpc):
        """When rpc_url is set on compiler, it takes precedence over everything."""
        with patch.dict(os.environ, {"ANVIL_AVALANCHE_PORT": "9999"}):
            url = compiler_with_rpc._get_chain_rpc_url()
        assert url == "https://explicit-rpc.example.com"

    def test_anvil_port_env_var_returns_localhost(self, compiler_no_rpc):
        """When ANVIL_{CHAIN}_PORT is set, return localhost URL with that port."""
        with patch.dict(os.environ, {"ANVIL_AVALANCHE_PORT": "8547"}):
            url = compiler_no_rpc._get_chain_rpc_url()
        assert url == "http://127.0.0.1:8547"

    def test_anvil_port_takes_priority_over_mainnet(self, compiler_no_rpc):
        """Anvil fork URL must take priority over mainnet RPC resolution."""
        with (
            patch.dict(os.environ, {"ANVIL_AVALANCHE_PORT": "8547", "ALCHEMY_API_KEY": "test-key"}),
            patch("almanak.gateway.utils.get_rpc_url") as mock_get_rpc,
        ):
            mock_get_rpc.return_value = "https://avax-mainnet.g.alchemy.com/v2/test-key"
            url = compiler_no_rpc._get_chain_rpc_url()

        # Should use Anvil, NOT call get_rpc_url at all
        assert url == "http://127.0.0.1:8547"
        mock_get_rpc.assert_not_called()

    def test_no_anvil_port_falls_through_to_mainnet(self, compiler_no_rpc):
        """Without ANVIL_{CHAIN}_PORT, fall through to mainnet RPC resolution."""
        clean_env = {k: v for k, v in os.environ.items() if not k.startswith("ANVIL_")}
        with (
            patch.dict(os.environ, clean_env, clear=True),
            patch("almanak.gateway.utils.get_rpc_url", return_value="https://mainnet.example.com") as mock_get_rpc,
        ):
            url = compiler_no_rpc._get_chain_rpc_url()

        assert url == "https://mainnet.example.com"
        mock_get_rpc.assert_called_once_with("avalanche")

    def test_anvil_port_different_chains(self):
        """Each chain's compiler uses its own ANVIL_{CHAIN}_PORT."""
        for chain, port in [("base", "8548"), ("arbitrum", "8549"), ("ethereum", "8550")]:
            compiler = IntentCompiler(
                chain=chain,
                wallet_address="0x1234567890abcdef1234567890abcdef12345678",
                rpc_url=None,
                config=IntentCompilerConfig(allow_placeholder_prices=True),
            )
            env_var = f"ANVIL_{chain.upper()}_PORT"
            with patch.dict(os.environ, {env_var: port}):
                url = compiler._get_chain_rpc_url()
            assert url == f"http://127.0.0.1:{port}", f"Failed for chain={chain}"

    def test_anvil_port_not_set_for_other_chains(self, compiler_no_rpc):
        """ANVIL_BASE_PORT should not affect an avalanche compiler."""
        clean_env = {k: v for k, v in os.environ.items() if not k.startswith("ANVIL_")}
        with (
            patch.dict(os.environ, {**clean_env, "ANVIL_BASE_PORT": "8548"}, clear=True),
            patch("almanak.gateway.utils.get_rpc_url", return_value="https://mainnet.example.com"),
        ):
            url = compiler_no_rpc._get_chain_rpc_url()
        # avalanche compiler should NOT use base's anvil port
        assert url == "https://mainnet.example.com"
