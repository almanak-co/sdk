"""Tests for gateway OHLCV provider token symbol mappings."""

import pytest

from almanak.framework.data.ohlcv.gateway_provider import TOKEN_TO_BINANCE_SYMBOL


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
            ("MATIC", "MATICUSDT"),
            ("WMATIC", "MATICUSDT"),
            ("SOL", "SOLUSDT"),
            ("ARB", "ARBUSDT"),
            ("OP", "OPUSDT"),
        ],
    )
    def test_token_maps_to_binance_symbol(self, token, expected_symbol):
        assert token in TOKEN_TO_BINANCE_SYMBOL, (
            f"Token '{token}' missing from TOKEN_TO_BINANCE_SYMBOL"
        )
        assert TOKEN_TO_BINANCE_SYMBOL[token] == expected_symbol
