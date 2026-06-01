"""Unit tests for Monad chain support in the Morpho Blue connector.

These tests guard the Monad-specific wiring added to the Morpho Blue connector.
Unlike Ethereum/Base/Arbitrum, Monad's Morpho Blue is deployed at a non-universal
address (chain-specific deployer pattern), so a regression to the old singleton
assumption would silently route transactions to an EOA. The tests below exist to
fail loudly if that happens.

Lives in its own file rather than `test_adapter.py` because that file has
pre-existing broken imports on `main` (imports symbols that no longer exist in the
adapter). Fixing it is out of scope for the Monad enablement work.
"""

from __future__ import annotations

from unittest.mock import patch

import pytest

from almanak.connectors.morpho_blue.addresses import MORPHO_BLUE, MORPHO_BLUE_ADDRESS, MORPHO_BLUE_TOKENS
from almanak.connectors.morpho_blue.adapter import (
    MORPHO_BLUE_ADDRESSES,
    MORPHO_BUNDLER_ADDRESSES,
    MORPHO_MARKETS,
    MorphoBlueAdapter,
    MorphoBlueConfig,
)
from almanak.connectors.morpho_blue.sdk import (
    MORPHO_DEPLOYMENT_BLOCKS,
    SUPPORTED_CHAINS,
    MorphoBlueSDK,
)

MONAD_MORPHO_ADDRESS = "0xD5D960E8C380B724a48AC59E2DfF1b2CB4a1eAee"
MONAD_BUNDLER_ADDRESS = "0x82b684483e844422FD339df0b67b3B111F02c66E"


class TestMonadRegistry:
    """Monad must be wired into the central contract registry."""

    def test_monad_in_morpho_blue_registry(self) -> None:
        assert "monad" in MORPHO_BLUE
        assert MORPHO_BLUE["monad"]["morpho"] == MONAD_MORPHO_ADDRESS
        assert MORPHO_BLUE["monad"]["bundler"] == MONAD_BUNDLER_ADDRESS

    def test_monad_uses_chain_specific_morpho_address(self) -> None:
        """Guard against silent regression to the universal-address assumption.

        Checks the central registry *and* the adapter's derived address dict, because
        the adapter uses the latter for transaction routing. A regression in either
        surface would quietly send Monad txs to the universal EOA.
        """
        assert MORPHO_BLUE["monad"]["morpho"] != MORPHO_BLUE_ADDRESS
        assert MORPHO_BLUE_ADDRESSES["monad"] != MORPHO_BLUE_ADDRESS
        assert MORPHO_BLUE_ADDRESSES["monad"] == MONAD_MORPHO_ADDRESS

    def test_monad_tokens_registered(self) -> None:
        """Core tokens for the two pre-configured Monad markets must be registered."""
        assert "monad" in MORPHO_BLUE_TOKENS
        monad_tokens = MORPHO_BLUE_TOKENS["monad"]
        for symbol in ("WETH", "wstETH", "WBTC", "AUSD", "USDC"):
            assert symbol in monad_tokens, f"Monad token registry missing {symbol}"


class TestMonadAdapter:
    """Adapter must initialise correctly for Monad and expose the chain-specific address."""

    @pytest.fixture
    def monad_adapter(self) -> MorphoBlueAdapter:
        config = MorphoBlueConfig(
            chain="monad",
            wallet_address="0x1234567890123456789012345678901234567890",
            # SDK init is lazy — explicit for clarity in offline unit tests.
            enable_sdk=False,
        )
        return MorphoBlueAdapter(config)

    def test_monad_chain_is_accepted(self, monad_adapter: MorphoBlueAdapter) -> None:
        assert monad_adapter.chain == "monad"

    def test_monad_adapter_uses_chain_specific_morpho_address(self, monad_adapter: MorphoBlueAdapter) -> None:
        assert monad_adapter.morpho_address == MONAD_MORPHO_ADDRESS
        assert monad_adapter.morpho_address != MORPHO_BLUE_ADDRESS

    def test_monad_adapter_has_preconfigured_markets(self, monad_adapter: MorphoBlueAdapter) -> None:
        assert len(monad_adapter.markets) >= 2, (
            "Expected at least 2 pre-configured Monad markets (wstETH/WETH, WBTC/AUSD)"
        )

    def test_monad_bundler_address_present(self) -> None:
        assert MORPHO_BUNDLER_ADDRESSES["monad"] == MONAD_BUNDLER_ADDRESS

    def test_monad_markets_have_required_fields(self) -> None:
        """Every Monad market must carry the full parameter set the compiler expects."""
        required_fields = {
            "name",
            "loan_token",
            "loan_token_address",
            "collateral_token",
            "collateral_token_address",
            "oracle",
            "irm",
            "lltv",
        }
        for market_id, info in MORPHO_MARKETS["monad"].items():
            missing = required_fields - set(info)
            assert not missing, f"Monad market {market_id} missing fields: {missing}"
            assert info["lltv"] > 0, f"Monad market {market_id} has non-positive LLTV"


class TestMonadSdkWiring:
    """SDK-level chain enablement."""

    def test_monad_in_supported_chains(self) -> None:
        assert "monad" in SUPPORTED_CHAINS

    def test_monad_deployment_block_registered(self) -> None:
        assert "monad" in MORPHO_DEPLOYMENT_BLOCKS
        assert MORPHO_DEPLOYMENT_BLOCKS["monad"] > 0

    def test_morpho_blue_sdk_init_uses_chain_specific_morpho_address_for_monad(self) -> None:
        """Constructor-level guard: instantiating MorphoBlueSDK(chain='monad') must resolve
        .morpho_address to the Monad-specific deployment, not the universal address.

        Exercises the real MorphoBlueSDK.__init__ path (not just static metadata) so a
        regression that re-introduced the `MORPHO_BLUE_ADDRESS` singleton fallback would
        be caught here. Web3 connection is mocked to avoid network.
        """
        with patch("almanak.connectors.morpho_blue.sdk.Web3") as mock_web3_cls:
            from web3 import Web3 as RealWeb3

            # Preserve real address checksumming (the code under test does this).
            mock_web3_cls.to_checksum_address.side_effect = RealWeb3.to_checksum_address
            mock_instance = mock_web3_cls.return_value
            mock_instance.is_connected.return_value = True

            sdk = MorphoBlueSDK(chain="monad", rpc_url="http://mocked")

            assert sdk.chain == "monad"
            assert sdk.morpho_address == MONAD_MORPHO_ADDRESS
            assert sdk.morpho_address != MORPHO_BLUE_ADDRESS
