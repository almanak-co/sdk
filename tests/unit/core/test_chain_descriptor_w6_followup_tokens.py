"""W6-followup (VIB-4872): coverage for ``ChainDescriptor.tokens``.

VIB-4872 migrated the legacy ``CHAIN_TOKENS`` dict in
``framework/intents/compiler_constants.py`` onto a new
``ChainDescriptor.tokens`` field (``Optional[Mapping[str, str]]``,
lowercase-symbol keyed). The derived legacy view is built at module-
import time by iterating ``ChainRegistry.all()`` and collecting the
non-``None`` tokens map per descriptor.

These tests pin:

* The shape contract — every chain that had a legacy ``CHAIN_TOKENS``
  entry has the same map back through the descriptor.
* The immutability contract — the field is frozen via
  ``MappingProxyType`` so descriptor-as-source-of-truth holds even when
  the per-chain module passed a mutable dict literal.
* The derived-view identity — the legacy
  ``compiler_constants.CHAIN_TOKENS`` dict matches the registry's
  contribution exactly, chain-by-chain.
"""

from __future__ import annotations

from types import MappingProxyType

import pytest

from almanak.core.chains import ChainRegistry


# =============================================================================
# Frozen historical snapshot — the legacy CHAIN_TOKENS values the
# descriptors now mirror. Kept inline so a future regression diff against
# ``main`` is obvious.
# =============================================================================


HISTORICAL_CHAIN_TOKENS: dict[str, dict[str, str]] = {
    "ethereum": {
        "usdc": "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
        "usdt": "0xdAC17F958D2ee523a2206206994597C13D831ec7",
        "weth": "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2",
        "wbtc": "0x2260FAC5E5542a773Aa44fBCfeDf7C193bc2C599",
        "dai": "0x6B175474E89094C44Da98b954EedeAC495271d0F",
    },
    "arbitrum": {
        "usdc": "0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
        "usdc_bridged": "0xFF970A61A04b1cA14834A43f5dE4533eBDDB5CC8",
        "usdt": "0xFd086bC7CD5C481DCC9C85ebE478A1C0b69FCbb9",
        "weth": "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1",
        "wbtc": "0x2f2a2543B76A4166549F7aaB2e75Bef0aefC5B0f",
    },
    "optimism": {
        "usdc": "0x0b2C639c533813f4Aa9D7837CAf62653d097Ff85",
        "usdt": "0x94b008aA00579c1307B0EF2c499aD98a8ce58e58",
        "weth": "0x4200000000000000000000000000000000000006",
    },
    "polygon": {
        "usdc": "0x3c499c542cEF5E3811e1192ce70d8cC03d5c3359",
        "usdt": "0xc2132D05D31c914a87C6611C10748AEb04B58e8F",
        "weth": "0x7ceB23fD6bC0adD59E62ac25578270cFf1b9f619",
    },
    "base": {
        "usdc": "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913",
        "weth": "0x4200000000000000000000000000000000000006",
    },
    "avalanche": {
        "usdc": "0xB97EF9Ef8734C71904D8002F8b6Bc66Dd9c48a6E",
        "usdt": "0x9702230A8Ea53601f5cD2dc00fDBc13d4dF4A8c7",
        "wavax": "0xB31f66AA3C1e785363F0875A1B74E27b85FD66c7",
    },
    "bsc": {
        "usdc": "0x8AC76a51cc950d9822D68b83fE1Ad97B32Cd580d",
        "usdt": "0x55d398326f99059fF775485246999027B3197955",
        "wbnb": "0xbb4CdB9CBd36B01bD1cBaEBF2De08d9173bc095c",
        "weth": "0x2170Ed0880ac9A755fd29B2688956BD959F933F8",
    },
    "linea": {
        "usdc": "0x176211869cA2b568f2A7D4EE941E073a821EE1ff",
        "usdt": "0xA219439258ca9da29E9Cc4cE5596924745e12B93",
        "weth": "0xe5D7C2a44FfDDf6b295A15c148167daaAf5Cf34f",
    },
    "sonic": {
        "usdc": "0x29219dd400f2Bf60E5a23d13Be72B486D4038894",
        "weth": "0x50c42dEAcD8Fc9773493ED674b675bE577f2634b",
        "ws": "0x039e2fB66102314Ce7b64Ce5Ce3E5183bc94aD38",
    },
    "mantle": {
        "usdc": "0x09Bc4E0D864854c6aFB6eB9A9cdF58aC190D0dF9",
        "usdt": "0x201EBa5CC46D216Ce6DC03F6a759e8E766e956aE",
        "weth": "0xdEAddEaDdeadDEadDEADDEAddEADDEAddead1111",
        "wmnt": "0x78c1b0C915c4FAA5FffA6CAbf0219DA63d7f4cb8",
    },
    "xlayer": {
        "usdc": "0x74b7F16337b8972027F6196A17a631aC6dE26d22",
        "usdt": "0x779Ded0c9e1022225f8E0630b35a9b54bE713736",
        "weth": "0x5A77f1443D16ee5761d310e38b62f77f726bC71c",
        "wokb": "0xe538905cf8410324e03A5A23C1c177a474D59b2b",
        "xeth": "0xE7B000003A45145decf8a28FC755aD5eC5EA025A",
        "xbtc": "0xb7C00000bcDEeF966b20B3D884B98E64d2b06b4f",
        "usdg": "0x4ae46a509F6b1D9056937BA4500cb143933D2dc8",
        "usdt0": "0x779Ded0c9e1022225f8E0630b35a9b54bE713736",
    },
    "monad": {
        "usdc": "0x754704Bc059F8C67012fEd69BC8A327a5aafb603",
        "weth": "0xEE8c0E9f1BFFb4Eb878d8f15f368A02a35481242",
        "wmon": "0x3bd359C1119dA7Da1D913D1C4D2B7c461115433A",
        "wbtc": "0x0555E30da8f98308EdB960aa94C0Db47230d2B9c",
    },
}


class TestChainDescriptorTokensField:
    """Cover the new ``ChainDescriptor.tokens`` field shape + identity."""

    @pytest.mark.parametrize("chain_name", sorted(HISTORICAL_CHAIN_TOKENS))
    def test_descriptor_tokens_matches_historical_snapshot(self, chain_name: str) -> None:
        descriptor = ChainRegistry.try_resolve(chain_name)
        assert descriptor is not None, f"chain {chain_name!r} not registered"
        assert descriptor.tokens is not None, (
            f"chain {chain_name!r} descriptor has no tokens map; historical "
            f"CHAIN_TOKENS expected: {HISTORICAL_CHAIN_TOKENS[chain_name]}"
        )
        # Compare lowercased — the descriptor frozen-set normalises symbol
        # keys to lowercase; addresses preserve their original case for
        # downstream consumers that care about checksum format.
        expected_lower = {k.lower(): v for k, v in HISTORICAL_CHAIN_TOKENS[chain_name].items()}
        assert dict(descriptor.tokens) == expected_lower

    def test_tokens_field_is_frozen(self) -> None:
        """``tokens`` is wrapped in MappingProxyType — mutation must raise."""
        descriptor = ChainRegistry.try_resolve("ethereum")
        assert descriptor is not None
        assert descriptor.tokens is not None
        assert isinstance(descriptor.tokens, MappingProxyType)
        with pytest.raises(TypeError):
            descriptor.tokens["new"] = "0xdeadbeef"  # type: ignore[index]

    def test_chains_without_tokens_field_return_none(self) -> None:
        """Solana / Plasma / Berachain / Blast etc. have no tokens catalogue
        today; the field stays ``None`` (vs an empty dict) so the consumer's
        ``CHAIN_TOKENS.get(chain, {})`` semantics keep working."""
        for unsupported_chain in ("solana", "berachain", "blast", "plasma"):
            descriptor = ChainRegistry.try_resolve(unsupported_chain)
            if descriptor is None:
                # Not registered at all on some branches — skip
                continue
            assert descriptor.tokens is None, (
                f"chain {unsupported_chain!r} unexpectedly has a tokens map: "
                f"{descriptor.tokens}"
            )


class TestCompilerConstantsDerivedView:
    """The derived ``CHAIN_TOKENS`` view matches the registry contribution."""

    def test_derived_chain_tokens_matches_registry(self) -> None:
        from almanak.framework.intents.compiler_constants import CHAIN_TOKENS

        # Build the expected aggregation directly from the registry —
        # every descriptor with a non-None tokens field contributes one
        # entry, keyed by canonical chain name.
        expected: dict[str, dict[str, str]] = {}
        for descriptor in ChainRegistry.all():
            if descriptor.tokens is not None:
                expected[descriptor.name] = dict(descriptor.tokens)

        assert CHAIN_TOKENS == expected

    def test_derived_view_includes_every_historical_chain(self) -> None:
        from almanak.framework.intents.compiler_constants import CHAIN_TOKENS

        # Every chain in the pre-refactor snapshot must round-trip.
        for chain_name, historical in HISTORICAL_CHAIN_TOKENS.items():
            assert chain_name in CHAIN_TOKENS, f"missing chain {chain_name!r} in derived view"
            expected_lower = {k.lower(): v for k, v in historical.items()}
            assert CHAIN_TOKENS[chain_name] == expected_lower
