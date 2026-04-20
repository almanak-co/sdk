"""Tests for _format_token_amount helper in Aave V3 receipt parser."""

from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest

from almanak.framework.connectors.aave_v3.receipt_parser import _format_token_amount


class TestFormatTokenAmount:
    """Tests for human-readable token amount formatting."""

    @patch("almanak.framework.data.tokens.get_token_resolver")
    def test_usdc_6_decimals(self, mock_get_resolver):
        """USDC with 6 decimals: 1500000000 -> '1,500'."""
        mock_resolver = MagicMock()
        mock_resolver.resolve.return_value = MagicMock(decimals=6)
        mock_get_resolver.return_value = mock_resolver

        result = _format_token_amount(Decimal("1500000000"), "0xaf88d065e77c8cC2239327C5EDb3A432268e5831", "arbitrum")

        assert result == "1,500"
        mock_resolver.resolve.assert_called_once_with(
            "0xaf88d065e77c8cC2239327C5EDb3A432268e5831", "arbitrum"
        )

    @patch("almanak.framework.data.tokens.get_token_resolver")
    def test_weth_18_decimals(self, mock_get_resolver):
        """WETH with 18 decimals: 2700000000000000 -> '0.0027'."""
        mock_resolver = MagicMock()
        mock_resolver.resolve.return_value = MagicMock(decimals=18)
        mock_get_resolver.return_value = mock_resolver

        result = _format_token_amount(Decimal("2700000000000000"), "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1", "arbitrum")

        assert result == "0.0027"

    @patch("almanak.framework.data.tokens.get_token_resolver")
    def test_wbtc_8_decimals(self, mock_get_resolver):
        """WBTC with 8 decimals: 150000000 -> '1.5'."""
        mock_resolver = MagicMock()
        mock_resolver.resolve.return_value = MagicMock(decimals=8)
        mock_get_resolver.return_value = mock_resolver

        result = _format_token_amount(Decimal("150000000"), "0x2f2a2543B76A4166549F7aaB2e75Bef0aefC5B0f", "arbitrum")

        assert result == "1.5"

    @patch("almanak.framework.data.tokens.get_token_resolver")
    def test_zero_amount(self, mock_get_resolver):
        """Zero amount returns '0'."""
        mock_resolver = MagicMock()
        mock_resolver.resolve.return_value = MagicMock(decimals=6)
        mock_get_resolver.return_value = mock_resolver

        result = _format_token_amount(Decimal("0"), "0xaddr", "ethereum")

        assert result == "0"

    @patch("almanak.framework.data.tokens.get_token_resolver")
    def test_large_amount_with_commas(self, mock_get_resolver):
        """Large amounts get comma formatting: 1000000000000 USDC -> '1,000,000'."""
        mock_resolver = MagicMock()
        mock_resolver.resolve.return_value = MagicMock(decimals=6)
        mock_get_resolver.return_value = mock_resolver

        result = _format_token_amount(Decimal("1000000000000"), "0xaddr", "ethereum")

        assert result == "1,000,000"

    @patch("almanak.framework.data.tokens.get_token_resolver")
    def test_trailing_zeros_stripped(self, mock_get_resolver):
        """Trailing zeros after decimal point are stripped."""
        mock_resolver = MagicMock()
        mock_resolver.resolve.return_value = MagicMock(decimals=6)
        mock_get_resolver.return_value = mock_resolver

        # 1.5 USDC = 1500000
        result = _format_token_amount(Decimal("1500000"), "0xaddr", "ethereum")

        assert result == "1.5"

    @patch("almanak.framework.data.tokens.get_token_resolver")
    def test_resolver_failure_returns_raw(self, mock_get_resolver):
        """When resolver raises, returns raw amount with '(raw)' suffix."""
        mock_get_resolver.side_effect = Exception("Token not found")

        result = _format_token_amount(Decimal("2700000000000000"), "0xunknown", "arbitrum")

        assert result == "2,700,000,000,000,000 (raw)"

    @patch("almanak.framework.data.tokens.get_token_resolver")
    def test_resolver_resolve_failure_returns_raw(self, mock_get_resolver):
        """When resolver.resolve() raises, returns raw amount with '(raw)' suffix."""
        mock_resolver = MagicMock()
        mock_resolver.resolve.side_effect = Exception("Unknown token")
        mock_get_resolver.return_value = mock_resolver

        result = _format_token_amount(Decimal("1000000"), "0xunknown", "ethereum")

        assert result == "1,000,000 (raw)"

    @patch("almanak.framework.data.tokens.get_token_resolver")
    def test_chain_passed_to_resolver(self, mock_get_resolver):
        """Chain parameter is forwarded to the token resolver."""
        mock_resolver = MagicMock()
        mock_resolver.resolve.return_value = MagicMock(decimals=18)
        mock_get_resolver.return_value = mock_resolver

        _format_token_amount(Decimal("1000000000000000000"), "0xaddr", "base")

        mock_resolver.resolve.assert_called_once_with("0xaddr", "base")
