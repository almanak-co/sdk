"""Unit tests for Robinhood Chain (4663) support in the Morpho Blue connector.

Robinhood is an Arbitrum Orbit L2 where Morpho powers ~73% of TVL (the Earn
product). Like Monad/Arbitrum/Polygon, its Morpho Blue singleton is NON-vanity:
the universal ``0xBBBB…FFCb`` address has ZERO code here, so a regression to the
old singleton assumption would silently route transactions to an EOA. Every
market on 4663 uses USDG (Global Dollar) as the loan asset and the single
chain-specific AdaptiveCurveIRM. These tests fail loudly if any of that wiring
regresses.

Bundler3 is deliberately absent from the registry for robinhood (Morpho does not
publish it on 4663 and no supply/borrow/repay/withdraw path uses it), so — unlike
the Monad tests — there is no bundler assertion here.
"""

from __future__ import annotations

from unittest.mock import patch

import pytest

from almanak.connectors.morpho_blue.adapter import (
    MORPHO_BLUE_ADDRESSES,
    MORPHO_BUNDLER_ADDRESSES,
    MORPHO_MARKETS,
    MorphoBlueAdapter,
    MorphoBlueConfig,
)
from almanak.connectors.morpho_blue.addresses import MORPHO_BLUE, MORPHO_BLUE_ADDRESS, MORPHO_BLUE_TOKENS
from almanak.connectors.morpho_blue.sdk import (
    MORPHO_DEPLOYMENT_BLOCKS,
    SUPPORTED_CHAINS,
    MorphoBlueSDK,
)

ROBINHOOD_MORPHO_ADDRESS = "0x9D53d5E3bd5E8d4Cbfa6DB1ca238AEA02E651010"
ROBINHOOD_IRM = "0x2BD3d5965B26B51814AC95127B2b80dD6CcC0fa1"
ROBINHOOD_TARGET_ORACLE = "0xE64849bd4AD03DfaBbe02bb521de19997a19055f"
ROBINHOOD_USDG = "0x5fc5360D0400a0Fd4f2af552ADD042D716F1d168"
ROBINHOOD_USDE = "0x5d3a1Ff2b6BAb83b63cd9AD0787074081a52ef34"
TARGET_MARKET_ID = "0xc845da65a020ddca5f132efa8fea79676d8edfdea504226a4c01e7a9e34cddd6"


class TestRobinhoodRegistry:
    """Robinhood must be wired into the central contract registry."""

    def test_robinhood_in_morpho_blue_registry(self) -> None:
        assert "robinhood" in MORPHO_BLUE
        assert MORPHO_BLUE["robinhood"]["morpho"] == ROBINHOOD_MORPHO_ADDRESS

    def test_robinhood_uses_chain_specific_morpho_address(self) -> None:
        """Guard against silent regression to the universal-address assumption.

        The vanity ``0xBBBB…FFCb`` address has no code on 4663; routing there
        would send every tx to an EOA.
        """
        assert MORPHO_BLUE["robinhood"]["morpho"] != MORPHO_BLUE_ADDRESS
        assert MORPHO_BLUE_ADDRESSES["robinhood"] != MORPHO_BLUE_ADDRESS
        assert MORPHO_BLUE_ADDRESSES["robinhood"] == ROBINHOOD_MORPHO_ADDRESS

    def test_robinhood_bundler_deliberately_absent(self) -> None:
        """Bundler3 is unresolved on 4663 and not on any lending path — the
        registry must omit it (not fabricate an address), and the derived
        adapter map must tolerate the absence rather than KeyError at import.
        """
        assert "bundler" not in MORPHO_BLUE["robinhood"]
        assert "robinhood" not in MORPHO_BUNDLER_ADDRESSES

    def test_robinhood_tokens_registered(self) -> None:
        """Tokens for the pre-configured Robinhood markets must be registered."""
        assert "robinhood" in MORPHO_BLUE_TOKENS
        rh_tokens = MORPHO_BLUE_TOKENS["robinhood"]
        for symbol in ("WETH", "USDG", "USDe", "syrupUSDG"):
            assert symbol in rh_tokens, f"Robinhood token registry missing {symbol}"
        assert rh_tokens["USDG"] == ROBINHOOD_USDG
        assert rh_tokens["USDe"] == ROBINHOOD_USDE

    def test_robinhood_target_market_params(self) -> None:
        """The deep USDe/USDG market must carry the exact verified parameters."""
        markets = MORPHO_MARKETS["robinhood"]
        assert TARGET_MARKET_ID in markets, "USDe/USDG target market missing"
        target = markets[TARGET_MARKET_ID]
        assert target["loan_token"] == "USDG"
        assert target["loan_token_address"] == ROBINHOOD_USDG
        assert target["collateral_token"] == "USDe"
        assert target["collateral_token_address"] == ROBINHOOD_USDE
        assert target["oracle"] == ROBINHOOD_TARGET_ORACLE
        assert target["irm"] == ROBINHOOD_IRM
        assert target["lltv"] == 915000000000000000  # 91.5%
        # The target market is first so permission-hint discovery + the demo
        # resolve it via ``next(iter(...))``.
        assert next(iter(markets)) == TARGET_MARKET_ID


class TestRobinhoodAdapter:
    """Adapter must initialise for Robinhood and expose the chain-specific address."""

    @pytest.fixture
    def robinhood_adapter(self) -> MorphoBlueAdapter:
        config = MorphoBlueConfig(
            chain="robinhood",
            wallet_address="0x1234567890123456789012345678901234567890",
            enable_sdk=False,
        )
        return MorphoBlueAdapter(config)

    def test_robinhood_chain_is_accepted(self, robinhood_adapter: MorphoBlueAdapter) -> None:
        assert robinhood_adapter.chain == "robinhood"

    def test_robinhood_adapter_uses_chain_specific_morpho_address(
        self, robinhood_adapter: MorphoBlueAdapter
    ) -> None:
        assert robinhood_adapter.morpho_address == ROBINHOOD_MORPHO_ADDRESS
        assert robinhood_adapter.morpho_address != MORPHO_BLUE_ADDRESS

    def test_robinhood_adapter_bundler_is_none(self, robinhood_adapter: MorphoBlueAdapter) -> None:
        """Absent bundler resolves to None, never a stray/other-chain address."""
        assert robinhood_adapter.bundler_address is None

    def test_robinhood_markets_have_required_fields(self) -> None:
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
        for market_id, info in MORPHO_MARKETS["robinhood"].items():
            missing = required_fields - set(info)
            assert not missing, f"Robinhood market {market_id} missing fields: {missing}"
            assert info["lltv"] > 0, f"Robinhood market {market_id} has non-positive LLTV"
            # Every 4663 market is USDG-loan (chain-canonical stable).
            assert info["loan_token"] == "USDG"


class TestRobinhoodSdkWiring:
    """SDK-level chain enablement."""

    def test_robinhood_in_supported_chains(self) -> None:
        assert "robinhood" in SUPPORTED_CHAINS

    def test_robinhood_deployment_block_registered(self) -> None:
        # Required so event-scan discovery doesn't fall back to from_block=0 and
        # scan the whole chain. Block verified by binary-searching eth_getCode.
        assert "robinhood" in MORPHO_DEPLOYMENT_BLOCKS
        assert MORPHO_DEPLOYMENT_BLOCKS["robinhood"] == 3967111

    def test_morpho_blue_sdk_init_uses_chain_specific_morpho_address_for_robinhood(self) -> None:
        """Constructor-level guard: MorphoBlueSDK(chain='robinhood') must resolve
        .morpho_address to the Robinhood-specific deployment, not the universal
        address. Web3 connection is mocked to avoid network.
        """
        with patch("almanak.connectors.morpho_blue.sdk.Web3") as mock_web3_cls:
            from web3 import Web3 as RealWeb3

            mock_web3_cls.to_checksum_address.side_effect = RealWeb3.to_checksum_address
            mock_instance = mock_web3_cls.return_value
            mock_instance.is_connected.return_value = True

            sdk = MorphoBlueSDK(chain="robinhood", rpc_url="http://mocked")

            assert sdk.chain == "robinhood"
            assert sdk.morpho_address == ROBINHOOD_MORPHO_ADDRESS
            assert sdk.morpho_address != MORPHO_BLUE_ADDRESS
