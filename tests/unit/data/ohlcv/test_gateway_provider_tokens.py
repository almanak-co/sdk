"""Tests for gateway OHLCV provider token symbol mappings."""

from unittest.mock import MagicMock

import pytest

from almanak.framework.data.models import CEX_SYMBOL_MAP
from almanak.framework.data.ohlcv.gateway_provider import (
    TOKEN_TO_BINANCE_SYMBOL,
    GatewayOHLCVProvider,
)


class TestTokenToBinanceSymbol:
    """Verify that all common tokens have Binance symbol mappings."""

    @pytest.mark.parametrize(
        "token,expected_symbol",
        [
            ("ETH", "ETHUSDT"),
            ("WETH", "ETHUSDT"),
            ("BTC", "BTCUSDT"),
            ("WBTC", "BTCUSDT"),
            ("BNB", "BNBUSDT"),  # VIB-1442: was missing
            ("WBNB", "BNBUSDT"),  # VIB-1442: was missing
            ("AVAX", "AVAXUSDT"),
            ("WAVAX", "AVAXUSDT"),
            ("MATIC", "POLUSDT"),  # MATIC->POL rebrand: POLUSDT is the live pair
            ("WMATIC", "POLUSDT"),
            ("POL", "POLUSDT"),
            ("WPOL", "POLUSDT"),
            ("SOL", "SOLUSDT"),
            ("ARB", "ARBUSDT"),
            ("OP", "OPUSDT"),
        ],
    )
    def test_token_maps_to_binance_symbol(self, token, expected_symbol):
        assert token in TOKEN_TO_BINANCE_SYMBOL, f"Token '{token}' missing from TOKEN_TO_BINANCE_SYMBOL"
        assert TOKEN_TO_BINANCE_SYMBOL[token] == expected_symbol


class TestResolveBinanceSymbol:
    """``_resolve_binance_symbol`` consults the canonical ``CEX_SYMBOL_MAP``
    first, then the connector-local table — closing the drift that left
    CBBTC/DAI/GMX/PENDLE/BTCB unresolvable (Binance silently skipped, vol broke)."""

    @staticmethod
    def _provider() -> GatewayOHLCVProvider:
        # The resolver does not touch the gateway client.
        return GatewayOHLCVProvider(gateway_client=MagicMock())

    @pytest.mark.parametrize(
        "token,expected",
        [
            # Previously MISSING from the local table -> resolver returned None ->
            # Binance skipped -> sparse CoinGecko fallback -> realized-vol failed.
            ("CBBTC", "BTCUSDT"),
            ("DAI", "DAIUSDT"),
            ("GMX", "GMXUSDT"),
            ("PENDLE", "PENDLEUSDT"),
            ("BTCB", "BTCUSDT"),
            # Tokens already in the local table still resolve.
            ("WETH", "ETHUSDT"),
            ("WBTC", "BTCUSDT"),
            ("MATIC", "POLUSDT"),  # only in the local table -> exercises the fallback
        ],
    )
    def test_resolves_via_canonical_map_then_local_fallback(self, token, expected):
        assert self._provider()._resolve_binance_symbol(token) == expected

    def test_case_insensitive(self):
        assert self._provider()._resolve_binance_symbol("cbbtc") == "BTCUSDT"

    def test_unknown_token_returns_none(self):
        assert self._provider()._resolve_binance_symbol("NOTATOKEN") is None

    def test_every_binance_base_in_canonical_map_is_resolvable(self):
        """Drift guard: every Binance base in ``CEX_SYMBOL_MAP`` must resolve, so
        the provider can never silently diverge from it again (the CBBTC bug)."""
        provider = self._provider()
        binance_bases = {base for (exch, base, _quote) in CEX_SYMBOL_MAP if exch == "binance"}
        unresolved = sorted(b for b in binance_bases if provider._resolve_binance_symbol(b) is None)
        assert unresolved == [], f"Binance bases not resolvable by provider: {unresolved}"
