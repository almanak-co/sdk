"""Tests for chain-scoped native-symbol detection in MarketService (VIB-3137).

The legacy code matched native gas tokens by symbol ONLY:

    if token.upper() in ("ETH", "AVAX", "MATIC", "POL", "BNB", "SOL", "MNT"):
        result = await provider.get_native_balance()

This meant `GetBalance(token="POL", chain="ethereum")` would return the wallet's
ETH balance — wrong asset, wrong USD valuation downstream. The fix scopes the
native check to (symbol, chain) pairs via ``_is_native_symbol``.

These unit tests pin the behaviour so future symbol additions cannot regress
it back to a global allowlist.
"""

from __future__ import annotations

import pytest

from almanak.gateway.services.market_service import (
    NATIVE_SYMBOLS_BY_CHAIN,
    _is_native_symbol,
)


class TestNativeSymbolPolygon:
    """POL and MATIC are both native on Polygon (1:1 Sep-2024 rename)."""

    @pytest.mark.parametrize("symbol", ["POL", "MATIC", "pol", "matic"])
    def test_native_on_polygon(self, symbol):
        assert _is_native_symbol(symbol, "polygon") is True

    @pytest.mark.parametrize("chain", ["ethereum", "arbitrum", "base", "bsc", "avalanche"])
    def test_pol_not_native_off_polygon(self, chain):
        # The whole point of the fix: POL must NOT short-circuit to
        # get_native_balance() on chains where it isn't the gas coin.
        assert _is_native_symbol("POL", chain) is False
        assert _is_native_symbol("MATIC", chain) is False


class TestNativeSymbolPerChain:
    """Each chain only treats its own gas coin as native."""

    @pytest.mark.parametrize(
        ("symbol", "chain"),
        [
            ("ETH", "ethereum"),
            ("ETH", "arbitrum"),
            ("ETH", "base"),
            ("ETH", "optimism"),
            ("AVAX", "avalanche"),
            ("BNB", "bsc"),
            ("MNT", "mantle"),
            ("S", "sonic"),
            ("SOL", "solana"),
        ],
    )
    def test_native_for_own_chain(self, symbol, chain):
        assert _is_native_symbol(symbol, chain) is True

    @pytest.mark.parametrize(
        ("symbol", "chain"),
        [
            ("AVAX", "ethereum"),
            ("BNB", "ethereum"),
            ("ETH", "avalanche"),
            ("ETH", "polygon"),
            ("ETH", "bsc"),
            ("SOL", "ethereum"),
            ("MNT", "arbitrum"),
        ],
    )
    def test_not_native_off_chain(self, symbol, chain):
        # Cross-chain leakage prevention.
        assert _is_native_symbol(symbol, chain) is False


class TestUnknownChainFailsClosed:
    """Unknown chains return False for EVERY symbol (fail-closed).

    Rationale: if a chain isn't in the map, returning True for `ETH` would
    short-circuit to `get_native_balance()` on a chain whose native coin
    isn't actually ETH (e.g. a future zkSync-style L2 with a different gas
    token), recreating the very bug VIB-3137 fixes. Fail-closed: the
    request falls through to the ERC-20 path which will fail loudly if
    the token isn't a real contract on that chain.
    """

    @pytest.mark.parametrize(
        "symbol",
        ["ETH", "AVAX", "MATIC", "POL", "BNB", "SOL", "MNT", "USDC", "WETH"],
    )
    def test_unknown_chain_returns_false_for_any_symbol(self, symbol):
        assert _is_native_symbol(symbol, "some-future-l2") is False
        assert _is_native_symbol(symbol, "ZORA") is False
        assert _is_native_symbol(symbol, "") is False


class TestNativeSymbolMapInvariants:
    """Lock in the shape of NATIVE_SYMBOLS_BY_CHAIN to catch typos."""

    def test_polygon_has_both_matic_and_pol(self):
        assert "MATIC" in NATIVE_SYMBOLS_BY_CHAIN["polygon"]
        assert "POL" in NATIVE_SYMBOLS_BY_CHAIN["polygon"]

    def test_no_chain_lists_eth_alongside_other_natives(self):
        # ETH chains MUST list ONLY ETH (otherwise we'd accept e.g. BNB on
        # Ethereum). This catches accidental over-broad symbol additions.
        eth_chains = ["ethereum", "arbitrum", "optimism", "base", "linea", "blast"]
        for chain in eth_chains:
            if chain in NATIVE_SYMBOLS_BY_CHAIN:
                assert NATIVE_SYMBOLS_BY_CHAIN[chain] == frozenset({"ETH"}), (
                    f"{chain} should only have ETH as native, got {NATIVE_SYMBOLS_BY_CHAIN[chain]}"
                )
