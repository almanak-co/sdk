"""Unit tests for FluidRatesProvider.

Tests caching, pool enumeration, and rate data retrieval with mocked Web3.

To run:
    uv run pytest tests/unit/connectors/test_fluid_rates_provider.py -v
"""

import time
from unittest.mock import MagicMock, patch

import pytest

from almanak.framework.connectors.fluid.rates_provider import (
    RATES_CACHE_TTL,
    FluidPoolRate,
    FluidRatesProvider,
)
from almanak.framework.connectors.fluid.sdk import DexPoolData, FluidSDKError


class TestFluidRatesProvider:
    """Tests for the FluidRatesProvider."""

    def _make_provider(self, mock_sdk=None):
        """Create a FluidRatesProvider with mocked SDK."""
        provider = FluidRatesProvider(
            chain="arbitrum",
            rpc_url="http://localhost:8545",
            cache_ttl=60,
        )
        if mock_sdk:
            provider._sdk = mock_sdk
        return provider

    def _make_pool_data(self, address="0xPool1", fee=30, smart_col=False, smart_debt=False):
        return DexPoolData(
            dex_address=address,
            token0="0xWETH",
            token1="0xUSDC",
            fee_bps=fee,
            is_smart_collateral=smart_col,
            is_smart_debt=smart_debt,
        )

    def test_get_all_pool_rates(self):
        """Fetches rates for all registered pools."""
        mock_sdk = MagicMock()
        mock_sdk.get_all_dex_addresses.return_value = ["0xPool1", "0xPool2"]
        mock_sdk.get_dex_data.side_effect = [
            self._make_pool_data("0xPool1"),
            self._make_pool_data("0xPool2", fee=50),
        ]

        provider = self._make_provider(mock_sdk)
        rates = provider.get_all_pool_rates()

        assert len(rates) == 2
        assert rates[0].dex_address == "0xPool1"
        assert rates[0].fee_bps == 30
        assert rates[1].fee_bps == 50

    def test_caching(self):
        """Second call returns cached data without querying SDK."""
        mock_sdk = MagicMock()
        mock_sdk.get_all_dex_addresses.return_value = ["0xPool1"]
        mock_sdk.get_dex_data.return_value = self._make_pool_data()

        provider = self._make_provider(mock_sdk)

        rates1 = provider.get_all_pool_rates()
        rates2 = provider.get_all_pool_rates()

        assert rates1 == rates2
        # SDK called only once (second call is cached)
        assert mock_sdk.get_all_dex_addresses.call_count == 1

    def test_skips_failed_pools(self):
        """Pools that fail to resolve are skipped, not fatal."""
        mock_sdk = MagicMock()
        mock_sdk.get_all_dex_addresses.return_value = ["0xGood", "0xBad"]
        mock_sdk.get_dex_data.side_effect = [
            self._make_pool_data("0xGood"),
            FluidSDKError("Pool reverted"),
        ]

        provider = self._make_provider(mock_sdk)
        rates = provider.get_all_pool_rates()

        assert len(rates) == 1
        assert rates[0].dex_address == "0xGood"

    def test_get_pool_rate(self):
        """Looks up rate for a specific pool."""
        mock_sdk = MagicMock()
        mock_sdk.get_all_dex_addresses.return_value = ["0xPool1", "0xTarget"]
        mock_sdk.get_dex_data.side_effect = [
            self._make_pool_data("0xPool1"),
            self._make_pool_data("0xTarget", fee=100),
        ]

        provider = self._make_provider(mock_sdk)
        rate = provider.get_pool_rate("0xTarget")

        assert rate is not None
        assert rate.fee_bps == 100

    def test_get_pool_rate_not_found(self):
        """Returns None for unknown pool."""
        mock_sdk = MagicMock()
        mock_sdk.get_all_dex_addresses.return_value = ["0xPool1"]
        mock_sdk.get_dex_data.return_value = self._make_pool_data()

        provider = self._make_provider(mock_sdk)
        rate = provider.get_pool_rate("0xNonexistent")

        assert rate is None

    def test_is_available(self):
        """is_available returns True for supported chains."""
        assert FluidRatesProvider.is_available("arbitrum") is True
        assert FluidRatesProvider.is_available("ethereum") is False
        assert FluidRatesProvider.is_available("base") is False

    def test_no_rpc_url_raises(self):
        """Provider raises if rpc_url not provided when SDK is needed."""
        provider = FluidRatesProvider(chain="arbitrum", rpc_url=None)
        with pytest.raises(FluidSDKError, match="rpc_url"):
            provider.get_all_pool_rates()

    def test_pool_rate_frozen(self):
        """FluidPoolRate is frozen (immutable)."""
        rate = FluidPoolRate(
            dex_address="0x1",
            token0="0xA",
            token1="0xB",
            fee_bps=30,
            is_smart_collateral=False,
            is_smart_debt=False,
        )
        with pytest.raises(AttributeError):
            rate.fee_bps = 50
