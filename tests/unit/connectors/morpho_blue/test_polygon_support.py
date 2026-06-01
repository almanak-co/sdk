"""Unit tests for Polygon chain support in the Morpho Blue connector.

These tests guard against the same class of failure VIB-2969 fixed on Arbitrum:
the universal 0xBBBB...FFCb vanity address has 0 bytes of code on Polygon, so a
registry entry pointing there would silently route every Morpho Blue compile call
to an EOA and fail with misleading "Unknown market" errors.

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

POLYGON_MORPHO_ADDRESS = "0x1bF0c2541F820E775182832f06c0B7Fc27A25f67"
POLYGON_BUNDLER_ADDRESS = "0x2d9C3A9E67c966C711208cc78b34fB9E9f8db589"
POLYGON_IRM_ADDRESS = "0xe675A2161D4a6E2de2eeD70ac98EEBf257FBF0B0"
POLYGON_DEPLOYMENT_BLOCK = 66931042


class TestPolygonRegistry:
    """Polygon must be wired into the central contract registry with the real address."""

    def test_polygon_in_morpho_blue_registry(self) -> None:
        assert "polygon" in MORPHO_BLUE
        assert MORPHO_BLUE["polygon"]["morpho"] == POLYGON_MORPHO_ADDRESS
        assert MORPHO_BLUE["polygon"]["bundler"] == POLYGON_BUNDLER_ADDRESS

    def test_polygon_uses_chain_specific_morpho_address(self) -> None:
        """Guard against regression to the universal-address assumption.

        The universal 0xBBBB...FFCb vanity address has 0 bytes of code on Polygon
        (verified 2026-04-17 via Morpho GraphQL + on-chain eth_getCode). A registry
        entry pointing there would fail with misleading errors — the same latent
        bug VIB-2969 exposed on Arbitrum.
        """
        assert MORPHO_BLUE["polygon"]["morpho"] != MORPHO_BLUE_ADDRESS
        assert MORPHO_BLUE_ADDRESSES["polygon"] != MORPHO_BLUE_ADDRESS
        assert MORPHO_BLUE_ADDRESSES["polygon"] == POLYGON_MORPHO_ADDRESS

    def test_polygon_tokens_registered(self) -> None:
        """Core tokens for the pre-configured Polygon markets must be registered."""
        assert "polygon" in MORPHO_BLUE_TOKENS
        polygon_tokens = MORPHO_BLUE_TOKENS["polygon"]
        for symbol in ("USDC", "WETH", "WBTC", "WPOL", "wstETH"):
            assert symbol in polygon_tokens, f"Polygon token registry missing {symbol}"

    def test_polygon_usdc_is_native_not_bridged(self) -> None:
        """Polygon has TWO USDCs: native Circle (0x3c49...) and bridged USDC.e (0x2791...).

        Morpho markets created after Circle's native USDC launch quote against
        the native one. Using USDC.e as the "USDC" address would route compile
        calls to a token the market doesn't recognize.
        """
        assert MORPHO_BLUE_TOKENS["polygon"]["USDC"] == "0x3c499c542cEF5E3811e1192ce70d8cC03d5c3359"


class TestPolygonAdapter:
    """Adapter must initialise correctly for Polygon and expose the chain-specific address."""

    @pytest.fixture
    def polygon_adapter(self) -> MorphoBlueAdapter:
        config = MorphoBlueConfig(
            chain="polygon",
            wallet_address="0x1234567890123456789012345678901234567890",
            enable_sdk=False,
        )
        return MorphoBlueAdapter(config)

    def test_polygon_chain_is_accepted(self, polygon_adapter: MorphoBlueAdapter) -> None:
        assert polygon_adapter.chain == "polygon"

    def test_polygon_adapter_uses_chain_specific_morpho_address(self, polygon_adapter: MorphoBlueAdapter) -> None:
        assert polygon_adapter.morpho_address == POLYGON_MORPHO_ADDRESS
        assert polygon_adapter.morpho_address != MORPHO_BLUE_ADDRESS

    def test_polygon_adapter_has_preconfigured_markets(self, polygon_adapter: MorphoBlueAdapter) -> None:
        # Tightened to `== 3` (not `>= 2`) so accidental deletion of a preconfigured
        # market is surfaced at CI time. Update alongside the dict when adding new markets.
        assert len(polygon_adapter.markets) == 3, (
            "Expected 3 pre-configured Polygon markets (WBTC/WPOL, WBTC/USDC, wstETH/WETH)"
        )

    def test_polygon_bundler_address_present(self) -> None:
        assert MORPHO_BUNDLER_ADDRESSES["polygon"] == POLYGON_BUNDLER_ADDRESS

    def test_polygon_markets_have_required_fields(self) -> None:
        """Every Polygon market must carry the full parameter set the compiler expects."""
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
        for market_id, info in MORPHO_MARKETS["polygon"].items():
            missing = required_fields - set(info)
            assert not missing, f"Polygon market {market_id} missing fields: {missing}"
            assert info["lltv"] > 0, f"Polygon market {market_id} has non-positive LLTV"

    def test_polygon_markets_use_chain_specific_irm(self) -> None:
        """Polygon markets must use the Polygon AdaptiveCurveIRM, not Ethereum's.

        Ethereum's IRM address 0x870a...00BC has no code on Polygon; routing a
        market through the wrong IRM would revert at compile time or worse,
        silently succeed against a different implementation.
        """
        for market_id, info in MORPHO_MARKETS["polygon"].items():
            assert info["irm"] == POLYGON_IRM_ADDRESS, (
                f"Polygon market {market_id} has wrong IRM: {info['irm']}. Expected Polygon IRM {POLYGON_IRM_ADDRESS}."
            )

    def test_polygon_market_ids_match_hashed_params(self) -> None:
        """Each preconfigured market_id must equal keccak256(abi.encode(MarketParams)).

        Morpho Blue derives market_id = keccak256(abi.encode(loanToken, collateralToken,
        oracle, irm, lltv)) on-chain. If any of those fields are edited without also
        updating the dict key, the SDK would build calldata for a market the contract
        doesn't recognize. This test recomputes the hash from the five fields and
        asserts it matches the key — catches typos at CI time, on every chain.
        """
        for market_id, info in MORPHO_MARKETS["polygon"].items():
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


class TestPolygonTokenResolver:
    """Every preconfigured market's loan/collateral symbol must resolve.

    The adapter reads decimals via `get_token_resolver().get_decimals()` during
    supply/borrow/repay. If a symbol in MORPHO_MARKETS["polygon"] is unknown to
    the resolver (e.g., WPOL after the Sep-2024 rebrand), the entire market is
    silently unusable even though the market_id and addresses are correct. This
    test exercises the resolution path for each wired market — regression guard
    flagged by two independent auditors on this PR.
    """

    def test_every_polygon_market_token_symbol_resolves(self) -> None:
        from almanak.framework.data.tokens import get_token_resolver

        resolver = get_token_resolver()
        for _market_id, info in MORPHO_MARKETS["polygon"].items():
            for role in ("loan_token", "collateral_token"):
                symbol = info[role]
                resolved = resolver.resolve(symbol, "polygon")
                assert resolved.address.lower() == info[f"{role}_address"].lower(), (
                    f"Market '{info['name']}' {role}='{symbol}' resolves to "
                    f"{resolved.address}, but market entry points at {info[f'{role}_address']}. "
                    "This mismatch would route supply/borrow/repay calls to the wrong token."
                )
                assert resolved.decimals > 0, (
                    f"Market '{info['name']}' {role}='{symbol}' resolved with non-positive decimals"
                )


class TestPolygonSdkWiring:
    """SDK-level chain enablement."""

    def test_polygon_in_supported_chains(self) -> None:
        assert "polygon" in SUPPORTED_CHAINS

    def test_polygon_deployment_block_registered(self) -> None:
        assert "polygon" in MORPHO_DEPLOYMENT_BLOCKS
        assert MORPHO_DEPLOYMENT_BLOCKS["polygon"] == POLYGON_DEPLOYMENT_BLOCK

    def test_morpho_blue_sdk_init_uses_chain_specific_morpho_address_for_polygon(self) -> None:
        """Constructor-level guard: instantiating MorphoBlueSDK(chain='polygon') must
        resolve .morpho_address to the Polygon-specific deployment (0x1bF0...), not
        the universal address (0xBBBB...).

        Exercises the real MorphoBlueSDK.__init__ path (not just static metadata) so a
        regression that re-introduced the `MORPHO_BLUE_ADDRESS` singleton fallback
        would be caught here. Web3 connection is mocked to avoid network.
        """
        with patch("almanak.connectors.morpho_blue.sdk.Web3") as mock_web3_cls:
            # `Web3` is patched inside the sdk module; reuse the real class only for
            # checksum address conversion, which doesn't hit the network. Import kept
            # local because the patch context shadows the module-level `Web3` binding.
            from web3 import Web3 as RealWeb3

            mock_web3_cls.to_checksum_address.side_effect = RealWeb3.to_checksum_address
            mock_instance = mock_web3_cls.return_value
            mock_instance.is_connected.return_value = True

            sdk = MorphoBlueSDK(chain="polygon", rpc_url="http://mocked")

            assert sdk.chain == "polygon"
            assert sdk.morpho_address == POLYGON_MORPHO_ADDRESS
            assert sdk.morpho_address != MORPHO_BLUE_ADDRESS
