"""Unit tests for DriftDataClient — REST API client.

All tests use mocked HTTP responses. No network access required.
"""

from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest
import requests

from almanak.framework.connectors.drift.client import DriftDataClient
from almanak.framework.connectors.drift.exceptions import DriftAPIError


class TestDriftDataClientInit:
    def test_default_init(self):
        client = DriftDataClient()
        assert "data.api.drift.trade" in client.base_url

    def test_custom_url(self):
        client = DriftDataClient(base_url="https://custom-api.test")
        assert client.base_url == "https://custom-api.test"


class TestGetPerpMarkets:
    def setup_method(self):
        self.client = DriftDataClient()

    @patch.object(DriftDataClient, "_make_request")
    def test_returns_markets(self, mock_request):
        mock_request.return_value = [
            {
                "marketType": "perp",
                "marketIndex": 0,
                "symbol": "SOL-PERP",
                "oraclePrice": "150.0",
            },
            {
                "marketType": "perp",
                "marketIndex": 1,
                "symbol": "BTC-PERP",
                "oraclePrice": "50000.0",
            },
        ]
        markets = self.client.get_perp_markets()
        assert len(markets) == 2
        assert markets[0].symbol == "SOL-PERP"
        assert markets[1].symbol == "BTC-PERP"

    @patch.object(DriftDataClient, "_make_request", side_effect=DriftAPIError("fail", 500))
    def test_returns_static_on_error(self, mock_request):
        markets = self.client.get_perp_markets()
        # Should fall back to static market list
        assert len(markets) > 0
        assert markets[0].symbol == "SOL-PERP"


class TestGetOraclePrices:
    def setup_method(self):
        self.client = DriftDataClient()

    @patch.object(DriftDataClient, "_make_request")
    def test_returns_prices(self, mock_request):
        mock_request.return_value = [
            {"marketIndex": 0, "oraclePrice": 150_000_000},
            {"marketIndex": 1, "oraclePrice": 50_000_000_000},
        ]
        prices = self.client.get_oracle_prices()
        assert 0 in prices
        assert 1 in prices
        assert prices[0] == Decimal("150")
        assert prices[1] == Decimal("50000")

    @patch.object(DriftDataClient, "_make_request", side_effect=DriftAPIError("fail", 500))
    def test_returns_empty_on_error(self, mock_request):
        prices = self.client.get_oracle_prices()
        assert prices == {}


class TestGetOraclePrice:
    def setup_method(self):
        self.client = DriftDataClient()

    @patch.object(DriftDataClient, "get_oracle_prices")
    def test_returns_price(self, mock_prices):
        mock_prices.return_value = {0: Decimal("150"), 2: Decimal("3500")}
        price = self.client.get_oracle_price(0)
        assert price == Decimal("150")

    @patch.object(DriftDataClient, "get_oracle_prices")
    def test_returns_none_for_missing(self, mock_prices):
        mock_prices.return_value = {0: Decimal("150")}
        price = self.client.get_oracle_price(99)
        assert price is None


class TestGetFundingRates:
    def setup_method(self):
        self.client = DriftDataClient()

    @patch.object(DriftDataClient, "_make_request")
    def test_returns_rates(self, mock_request):
        mock_request.return_value = [
            {"ts": 1700000000, "fundingRate": "0.0001", "marketIndex": 0},
            {"ts": 1700003600, "fundingRate": "-0.0002", "marketIndex": 0},
        ]
        rates = self.client.get_funding_rates(0)
        assert len(rates) == 2
        assert rates[0].funding_rate == Decimal("0.0001")
        assert rates[1].funding_rate == Decimal("-0.0002")

    @patch.object(DriftDataClient, "_make_request", side_effect=DriftAPIError("fail", 500))
    def test_returns_empty_on_error(self, mock_request):
        rates = self.client.get_funding_rates(0)
        assert rates == []


class TestMakeRequest:
    def setup_method(self):
        self.client = DriftDataClient()

    @patch("almanak.framework.connectors.drift.client.requests.Session")
    def test_http_error_raises_drift_api_error(self, mock_session_cls):
        mock_session = MagicMock()
        mock_response = MagicMock()
        mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError(
            response=MagicMock(status_code=404)
        )
        mock_session.request.return_value = mock_response
        self.client.session = mock_session

        with pytest.raises(DriftAPIError):
            self.client._make_request("GET", "/test")
