"""Tests for BinanceOHLCVProvider symbol resolution."""

import pytest

from almanak.framework.data.ohlcv.binance_provider import (
    BINANCE_SYMBOL_MAP,
    BinanceOHLCVProvider,
)


class TestBinanceSymbolMap:
    """Test that BINANCE_SYMBOL_MAP contains all expected token mappings."""

    @pytest.fixture()
    def provider(self):
        return BinanceOHLCVProvider()

    @pytest.mark.parametrize(
        ("token", "expected_pair"),
        [
            # Wrapped native tokens -- the bug fix
            ("WBNB", "BNBUSDT"),
            ("BNB", "BNBUSDT"),
            ("WAVAX", "AVAXUSDT"),
            ("WMATIC", "MATICUSDT"),
            ("S", "SUSDT"),
            ("WS", "SUSDT"),
            # Pre-existing mappings (regression guard)
            ("WETH", "ETHUSDT"),
            ("ETH", "ETHUSDT"),
            ("AVAX", "AVAXUSDT"),
            ("MATIC", "MATICUSDT"),
            ("BTC", "BTCUSDT"),
            ("WBTC", "BTCUSDT"),
        ],
    )
    def test_symbol_map_contains_token(self, token: str, expected_pair: str):
        """Each supported token must resolve to the correct Binance pair."""
        assert BINANCE_SYMBOL_MAP.get(token) == expected_pair

    @pytest.mark.parametrize(
        ("token", "expected_pair"),
        [
            ("wbnb", "BNBUSDT"),
            ("Wbnb", "BNBUSDT"),
            ("bnb", "BNBUSDT"),
            ("wavax", "AVAXUSDT"),
        ],
    )
    def test_resolve_symbol_case_insensitive(self, provider: BinanceOHLCVProvider, token: str, expected_pair: str):
        """_resolve_symbol should be case-insensitive."""
        assert provider._resolve_symbol(token) == expected_pair

    def test_resolve_symbol_returns_none_for_unknown(self, provider: BinanceOHLCVProvider):
        """Unknown tokens should return None, not raise."""
        assert provider._resolve_symbol("NONEXISTENT_TOKEN_XYZ") is None

    def test_wrapped_native_consistency(self):
        """Every wrapped native and its unwrapped form should map to the same pair."""
        pairs = [
            ("WETH", "ETH"),
            ("WBNB", "BNB"),
            ("WAVAX", "AVAX"),
            ("WMATIC", "MATIC"),
            ("WS", "S"),
        ]
        for wrapped, unwrapped in pairs:
            assert BINANCE_SYMBOL_MAP[wrapped] == BINANCE_SYMBOL_MAP[unwrapped], (
                f"{wrapped} and {unwrapped} should map to the same Binance pair"
            )
