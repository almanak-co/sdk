"""Unit tests for Arbitrum chain support in the Morpho Blue connector.

These tests guard against regression to the pre-VIB-2969 state, where the Arbitrum
entry in MORPHO_BLUE pointed at the universal vanity address
(0xBBBB...FFCb). That address has 0 bytes of code on Arbitrum, so any Morpho Blue
compile call on Arbitrum would silently route to an EOA and fail with a misleading
"Unknown market" error (documented in iter-173).

Lives in its own file rather than `test_adapter.py` because that file has
pre-existing broken imports on `main`.
"""

from __future__ import annotations

from unittest.mock import patch

import pytest
from eth_abi import encode
from web3 import Web3

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

ARBITRUM_MORPHO_ADDRESS = "0x6c247b1F6182318877311737BaC0844bAa518F5e"
ARBITRUM_BUNDLER_ADDRESS = "0x1FA4431bC113D308beE1d46B0e98Cb805FB48C13"
ARBITRUM_IRM_ADDRESS = "0x66F30587FB8D4206918deb78ecA7d5eBbafD06DA"


class TestArbitrumRegistry:
    """Arbitrum must be wired into the central contract registry with the real address."""

    def test_arbitrum_in_morpho_blue_registry(self) -> None:
        assert "arbitrum" in MORPHO_BLUE
        assert MORPHO_BLUE["arbitrum"]["morpho"] == ARBITRUM_MORPHO_ADDRESS
        assert MORPHO_BLUE["arbitrum"]["bundler"] == ARBITRUM_BUNDLER_ADDRESS

    def test_arbitrum_uses_chain_specific_morpho_address(self) -> None:
        """Guard against regression to the universal-address assumption.

        Before VIB-2969, Arbitrum was registered at the universal 0xBBBB...FFCb
        address, which has 0 bytes of code on Arbitrum. Any transaction routed
        there would have failed with a misleading error.
        """
        assert MORPHO_BLUE["arbitrum"]["morpho"] != MORPHO_BLUE_ADDRESS
        assert MORPHO_BLUE_ADDRESSES["arbitrum"] != MORPHO_BLUE_ADDRESS
        assert MORPHO_BLUE_ADDRESSES["arbitrum"] == ARBITRUM_MORPHO_ADDRESS

    def test_arbitrum_tokens_registered(self) -> None:
        """Core tokens for the pre-configured Arbitrum markets must be registered."""
        assert "arbitrum" in MORPHO_BLUE_TOKENS
        arbitrum_tokens = MORPHO_BLUE_TOKENS["arbitrum"]
        for symbol in ("USDC", "WETH", "wstETH", "WBTC"):
            assert symbol in arbitrum_tokens, f"Arbitrum token registry missing {symbol}"


class TestArbitrumAdapter:
    """Adapter must initialise correctly for Arbitrum and expose the chain-specific address."""

    @pytest.fixture
    def arbitrum_adapter(self) -> MorphoBlueAdapter:
        config = MorphoBlueConfig(
            chain="arbitrum",
            wallet_address="0x1234567890123456789012345678901234567890",
            enable_sdk=False,
        )
        return MorphoBlueAdapter(config)

    def test_arbitrum_chain_is_accepted(self, arbitrum_adapter: MorphoBlueAdapter) -> None:
        assert arbitrum_adapter.chain == "arbitrum"

    def test_arbitrum_adapter_uses_chain_specific_morpho_address(self, arbitrum_adapter: MorphoBlueAdapter) -> None:
        assert arbitrum_adapter.morpho_address == ARBITRUM_MORPHO_ADDRESS
        assert arbitrum_adapter.morpho_address != MORPHO_BLUE_ADDRESS

    def test_arbitrum_adapter_has_preconfigured_markets(self, arbitrum_adapter: MorphoBlueAdapter) -> None:
        assert len(arbitrum_adapter.markets) >= 2, (
            "Expected at least 2 pre-configured Arbitrum markets (wstETH/USDC, WBTC/USDC)"
        )

    def test_arbitrum_bundler_address_present(self) -> None:
        assert MORPHO_BUNDLER_ADDRESSES["arbitrum"] == ARBITRUM_BUNDLER_ADDRESS

    def test_arbitrum_markets_have_required_fields(self) -> None:
        """Every Arbitrum market must carry the full parameter set the compiler expects."""
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
        for market_id, info in MORPHO_MARKETS["arbitrum"].items():
            missing = required_fields - set(info)
            assert not missing, f"Arbitrum market {market_id} missing fields: {missing}"
            assert info["lltv"] > 0, f"Arbitrum market {market_id} has non-positive LLTV"

    def test_arbitrum_markets_use_chain_specific_irm(self) -> None:
        """Arbitrum markets must use the Arbitrum AdaptiveCurveIRM, not Ethereum's."""
        for market_id, info in MORPHO_MARKETS["arbitrum"].items():
            assert info["irm"] == ARBITRUM_IRM_ADDRESS, (
                f"Arbitrum market {market_id} has wrong IRM: {info['irm']}. "
                f"Expected Arbitrum IRM {ARBITRUM_IRM_ADDRESS} — a regression to the "
                "Ethereum IRM would route to an address without code on Arbitrum."
            )

    def test_arbitrum_market_ids_match_hashed_params(self) -> None:
        """Each preconfigured market_id must equal keccak256(abi.encode(MarketParams)).

        Morpho Blue derives market_id = keccak256(abi.encode(loanToken, collateralToken,
        oracle, irm, lltv)) on-chain. If any of those fields are edited without also
        updating the dict key, the SDK would build calldata for a market the contract
        doesn't recognize. This test recomputes the hash from the five fields and
        asserts it matches the key — catches typos at CI time, on every chain.
        """
        for market_id, info in MORPHO_MARKETS["arbitrum"].items():
            encoded = encode(
                ["(address,address,address,address,uint256)"],
                [
                    (
                        Web3.to_checksum_address(info["loan_token_address"]),
                        Web3.to_checksum_address(info["collateral_token_address"]),
                        Web3.to_checksum_address(info["oracle"]),
                        Web3.to_checksum_address(info["irm"]),
                        int(info["lltv"]),
                    )
                ],
            )
            computed = "0x" + Web3.keccak(encoded).hex()
            assert computed.lower() == market_id.lower(), (
                f"Market '{info['name']}' has inconsistent params: "
                f"registered id={market_id}, computed id={computed}. "
                "If you just edited the market entry, recompute the id via "
                "keccak256(abi.encode(loan, collateral, oracle, irm, lltv))."
            )


class TestArbitrumSdkWiring:
    """SDK-level chain enablement."""

    def test_arbitrum_in_supported_chains(self) -> None:
        assert "arbitrum" in SUPPORTED_CHAINS

    def test_arbitrum_deployment_block_registered(self) -> None:
        assert "arbitrum" in MORPHO_DEPLOYMENT_BLOCKS
        assert MORPHO_DEPLOYMENT_BLOCKS["arbitrum"] > 0

    def test_morpho_blue_sdk_init_uses_chain_specific_morpho_address_for_arbitrum(self) -> None:
        """Constructor-level guard: instantiating MorphoBlueSDK(chain='arbitrum') must
        resolve .morpho_address to the Arbitrum-specific deployment (0x6c24...), not
        the universal address (0xBBBB...).

        Exercises the real MorphoBlueSDK.__init__ path (not just static metadata) so a
        regression that re-introduced the `MORPHO_BLUE_ADDRESS` singleton fallback
        would be caught here. Web3 connection is mocked to avoid network.
        """
        with patch("almanak.connectors.morpho_blue.sdk.Web3") as mock_web3_cls:
            from web3 import Web3 as RealWeb3

            mock_web3_cls.to_checksum_address.side_effect = RealWeb3.to_checksum_address
            mock_instance = mock_web3_cls.return_value
            mock_instance.is_connected.return_value = True

            sdk = MorphoBlueSDK(chain="arbitrum", rpc_url="http://mocked")

            assert sdk.chain == "arbitrum"
            assert sdk.morpho_address == ARBITRUM_MORPHO_ADDRESS
            assert sdk.morpho_address != MORPHO_BLUE_ADDRESS
