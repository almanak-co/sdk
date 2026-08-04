"""``GatewayPriceIdCapability`` contract tests (VIB-4811 / Phase 3).

CoinGecko + DexScreener now build their lookup dicts from
``GATEWAY_REGISTRY.capability_providers(GatewayPriceIdCapability)``.
Tests pin:

* ``isinstance(connector, GatewayPriceIdCapability)`` is True iff the
  connector defines ``coingecko_ids`` and ``dexscreener_ids``.
* The post-refactor ``GLOBAL_TOKEN_IDS`` preserves the legacy hardcoded merge,
  except for explicitly pinned verified asset-ID corrections.
* The post-refactor DexScreener ``_KNOWN_TOKEN_ADDRESSES`` is
  byte-identical to the legacy Solana address dict.
* Collisions (two providers publishing different slugs for the same
  symbol) raise a loud ``RuntimeError`` at assembly time.
"""

from __future__ import annotations

from typing import ClassVar

import pytest

from almanak.connectors._base.gateway_capabilities import (
    GatewayPriceIdCapability,
)
from almanak.connectors._base.gateway_connector import GatewayConnector
from almanak.connectors._base.types import ProtocolKind, ProtocolName


class _PriceIdImpl(GatewayConnector):
    protocol: ClassVar[ProtocolName] = ProtocolName("price_id_demo")
    kind: ClassVar[ProtocolKind] = ProtocolKind.LP

    def coingecko_ids(self) -> dict[str, str]:
        return {"DEMO": "demo-coin"}

    def dexscreener_ids(self) -> dict[str, dict[str, str]]:
        return {"solana": {"DEMO": "DemoAddr11111111111111111111111111111111111"}}


class _BareConnector(GatewayConnector):
    protocol: ClassVar[ProtocolName] = ProtocolName("bare_price_id_demo")
    kind: ClassVar[ProtocolKind] = ProtocolKind.LP


def test_price_id_capability_runtime_isinstance() -> None:
    assert isinstance(_PriceIdImpl(), GatewayPriceIdCapability)
    assert not isinstance(_BareConnector(), GatewayPriceIdCapability)


def test_registered_connectors_advertise_capability() -> None:
    """Every Phase-3 PriceId connector exposes the capability."""
    from almanak.connectors._gateway_registry import GATEWAY_REGISTRY

    providers = GATEWAY_REGISTRY.capability_providers(GatewayPriceIdCapability)
    protocols = {p.protocol for p in providers}
    expected = {
        ProtocolName("aave_v3"),
        ProtocolName("aerodrome"),
        ProtocolName("benqi"),
        ProtocolName("ethena"),
        ProtocolName("gmx_v2"),
        ProtocolName("jupiter"),
        ProtocolName("lido"),
        ProtocolName("orca"),
        ProtocolName("pancakeswap_v3"),
        ProtocolName("pendle"),
        ProtocolName("raydium"),
        ProtocolName("traderjoe_v2"),
        ProtocolName("uniswap_v3"),
    }
    assert expected.issubset(protocols)


def test_coingecko_global_token_ids_matches_reviewed_merge() -> None:
    """``GLOBAL_TOKEN_IDS`` preserves legacy rows plus reviewed corrections.

    Every symbol+slug pair the hardcoded merge produced still resolves
    the same way after the registry-driven re-assembly unless a distinct
    wrapped-asset CoinGecko record was explicitly verified.
    """
    from almanak.gateway.data.price.coingecko import GLOBAL_TOKEN_IDS

    legacy = {
        # ARBITRUM_TOKEN_IDS
        "ETH": "ethereum",
        "WETH": "weth",
        "USDC": "usd-coin",
        "USDC.E": "usd-coin",
        "ARB": "arbitrum",
        "WBTC": "wrapped-bitcoin",
        "USDT": "tether",
        "DAI": "dai",
        "LINK": "chainlink",
        "UNI": "uniswap",
        "GMX": "gmx",
        "PENDLE": "pendle",
        "RDNT": "radiant-capital",
        "SOL": "solana",
        "JOE": "trader-joe",
        "LDO": "lido-dao",
        "BTC": "bitcoin",
        "STETH": "lido-dao-wrapped-staked-eth",
        "WSTETH": "wrapped-steth",
        "CBETH": "coinbase-wrapped-staked-eth",
        "USDE": "ethena-usde",
        "SUSDE": "ethena-staked-usde",
        # AVALANCHE_TOKEN_IDS
        "AVAX": "avalanche-2",
        "WAVAX": "avalanche-2",
        "USDT.E": "tether",
        "DAI.E": "dai",
        "WETH.E": "weth",
        "WBTC.E": "wrapped-bitcoin",
        "PNG": "pangolin",
        "QI": "benqi",
        "AAVE": "aave",
        "BTC.B": "bitcoin",
        # BASE_TOKEN_IDS
        "USDBC": "usd-coin",
        "AERO": "aerodrome-finance",
        "BASE": "base-protocol",
        "DEGEN": "degen-base",
        "BRETT": "brett",
        # BSC_TOKEN_IDS
        "BNB": "binancecoin",
        "WBNB": "binancecoin",
        "BTCB": "bitcoin",
        "CAKE": "pancakeswap-token",
        "BUSD": "binance-usd",
        # MANTLE_TOKEN_IDS
        "MNT": "mantle",
        "WMNT": "mantle",
        # XLAYER_TOKEN_IDS
        "OKB": "okb",
        "WOKB": "okb",
        "USDT0": "tether",
        "USDG": "usd-coin",
        # SOLANA_TOKEN_IDS
        "WSOL": "solana",
        "JUP": "jupiter-exchange-solana",
        "RAY": "raydium",
        "ORCA": "orca",
        "BONK": "bonk",
        "WIF": "dogwifcoin",
        "JTO": "jito-governance-token",
        "PYTH": "pyth-network",
        "MSOL": "msol",
        "JITOSOL": "jito-staked-sol",
        # MONAD_TOKEN_IDS
        "MON": "monad",
        "WMON": "monad",
        "EZETH": "renzo-restaked-eth",
        # ETHEREUM_TOKEN_IDS
        "CRV": "curve-dao-token",
        "CVX": "convex-finance",
        "COMP": "compound-governance-token",
        "MKR": "maker",
        "SNX": "havven",
        "RPL": "rocket-pool",
        "ENS": "ethereum-name-service",
        "GHO": "gho",
        "CRVUSD": "crvusd",
        "RETH": "rocket-pool-eth",
        "WEETH": "wrapped-eeth",
        "PUFETH": "pufeth",
    }
    verified_corrections = {
        "WAVAX": "wrapped-avax",
        "WMNT": "wrapped-mantle",
        "WMATIC": "wmatic",
    }
    expected = {**legacy, **verified_corrections}
    for symbol, expected_slug in expected.items():
        assert GLOBAL_TOKEN_IDS.get(symbol) == expected_slug, (
            f"{symbol}: expected {expected_slug}, got {GLOBAL_TOKEN_IDS.get(symbol)}"
        )


def test_coingecko_id_collision_raises() -> None:
    """Two providers publishing different slugs for the same symbol raises."""
    import almanak.connectors._gateway_registry as registry_mod
    from almanak.gateway.data.price.coingecko import _build_registry_price_ids

    class _ProviderA:
        def coingecko_ids(self) -> dict[str, str]:
            return {"DEMO": "demo-a"}

        def dexscreener_ids(self) -> dict[str, dict[str, str]]:
            return {}

    class _ProviderB:
        def coingecko_ids(self) -> dict[str, str]:
            return {"DEMO": "demo-b"}

        def dexscreener_ids(self) -> dict[str, dict[str, str]]:
            return {}

    class _FakeRegistry:
        def capability_providers(self, _cap: object) -> tuple[object, ...]:
            return (_ProviderA(), _ProviderB())

    original = registry_mod.GATEWAY_REGISTRY
    registry_mod.GATEWAY_REGISTRY = _FakeRegistry()  # type: ignore[assignment]
    try:
        with pytest.raises(RuntimeError, match="slug collision"):
            _build_registry_price_ids()
    finally:
        registry_mod.GATEWAY_REGISTRY = original


def test_dexscreener_known_addresses_matches_legacy() -> None:
    """DexScreener ``_KNOWN_TOKEN_ADDRESSES`` is byte-identical to the legacy dict."""
    from almanak.gateway.data.price.dexscreener import _KNOWN_TOKEN_ADDRESSES

    legacy = {
        "solana": {
            "SOL": "So11111111111111111111111111111111111111112",
            "WSOL": "So11111111111111111111111111111111111111112",
            "USDC": "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
            "USDT": "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB",
            "JUP": "JUPyiwrYJFskUPiHa7hkeR8VUtAeFoSYbKedZNsDvCN",
            "RAY": "4k3Dyjzvzp8eMZWUXbBCjEvwSkkk59S5iCNLY3QrkX6R",
            "BONK": "DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263",
            "WIF": "EKpQGSJtjMFqKZ9KQanSqYXRcF8fBopzLHYxdM65zcjm",
            "JTO": "jtojtomepa8beP8AuQc6eXt5FriJwfFMwQx2v2f9mCL",
            "ORCA": "orcaEKTdK7LKz57vaAYr9QeNsVEPfiu6QeMU1kektZE",
            "MSOL": "mSoLzYCxHdYgdzU16g5QSh3i5K3z3KZK7ytfqcJm7So",
            "JITOSOL": "J1toso1uCk3RLmjorhTtrVwY9HJ7X8V9yYac6Y7kGCPn",
            "PYTH": "HZ1JovNiVvGrGNiiYvEozEVgZ58xaU3RKwX8eACQBCt3",
        },
    }
    assert _KNOWN_TOKEN_ADDRESSES == legacy


@pytest.mark.parametrize("operation", ["copy", "union", "update", "pop", "popitem", "setdefault"])
def test_coingecko_lazy_mapping_builds_before_copy_and_mutation(monkeypatch, operation: str) -> None:
    import almanak.integrations.coingecko.gateway.price_source as module

    monkeypatch.setattr(module, "_CHAIN_TABLE_TOKEN_IDS", {"KNOWN": "known-id"})
    monkeypatch.setattr(module, "_build_registry_price_ids", lambda: {})
    mapping = module._LazyGlobalTokenIds()
    assert not mapping._built

    if operation == "copy":
        assert mapping.copy()["KNOWN"] == "known-id"
    elif operation == "union":
        assert (mapping | {"NEW": "new-id"})["KNOWN"] == "known-id"
    elif operation == "update":
        mapping.update({"NEW": "new-id"})
        assert mapping["KNOWN"] == "known-id"
    elif operation == "pop":
        assert mapping.pop("KNOWN") == "known-id"
    elif operation == "popitem":
        assert mapping.popitem() == ("KNOWN", "known-id")
    else:
        assert mapping.setdefault("KNOWN", "replacement") == "known-id"

    assert mapping._built


@pytest.mark.parametrize("operation", ["copy", "union", "update", "pop", "popitem", "setdefault"])
def test_dexscreener_lazy_mapping_builds_before_copy_and_mutation(monkeypatch, operation: str) -> None:
    import almanak.integrations.dexscreener.gateway.price_source as module

    known = {"KNOWN": "0xknown"}
    monkeypatch.setattr(module, "_build_registry_known_addresses", lambda: {"test-chain": known})
    mapping = module._LazyKnownTokenAddresses()
    assert not mapping._built

    if operation == "copy":
        assert mapping.copy()["test-chain"] == known
    elif operation == "union":
        assert (mapping | {"new-chain": {}})["test-chain"] == known
    elif operation == "update":
        mapping.update({"new-chain": {}})
        assert mapping["test-chain"] == known
    elif operation == "pop":
        assert mapping.pop("test-chain") == known
    elif operation == "popitem":
        assert mapping.popitem() == ("test-chain", known)
    else:
        assert mapping.setdefault("test-chain", {}) == known

    assert mapping._built


def test_dexscreener_address_collision_raises() -> None:
    """Two providers publishing different addresses for (chain, symbol) raises."""
    import almanak.connectors._gateway_registry as registry_mod
    from almanak.gateway.data.price.dexscreener import (
        _build_registry_known_addresses,
    )

    class _ProviderA:
        def coingecko_ids(self) -> dict[str, str]:
            return {}

        def dexscreener_ids(self) -> dict[str, dict[str, str]]:
            return {"solana": {"DEMO": "AAA"}}

    class _ProviderB:
        def coingecko_ids(self) -> dict[str, str]:
            return {}

        def dexscreener_ids(self) -> dict[str, dict[str, str]]:
            return {"solana": {"DEMO": "BBB"}}

    class _FakeRegistry:
        def capability_providers(self, _cap: object) -> tuple[object, ...]:
            return (_ProviderA(), _ProviderB())

    original = registry_mod.GATEWAY_REGISTRY
    registry_mod.GATEWAY_REGISTRY = _FakeRegistry()  # type: ignore[assignment]
    try:
        with pytest.raises(RuntimeError, match="address collision"):
            _build_registry_known_addresses()
    finally:
        registry_mod.GATEWAY_REGISTRY = original
