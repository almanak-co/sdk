"""Tests for known-unpriceable token log level downgrade (VIB-1370).

Validates that price lookup failures for derivative tokens (PT, YT, LP, etc.)
are logged at WARNING level instead of ERROR, since these tokens are not listed
on standard price feeds.
"""

import logging
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from almanak.framework.data.interfaces import AllDataSourcesFailed, DataSourceUnavailable
from almanak.gateway.data.price.aggregator import (
    KNOWN_UNPRICEABLE_PREFIXES,
    _is_known_unpriceable,
)


class TestIsKnownUnpriceable:
    """Test the _is_known_unpriceable helper function."""

    @pytest.mark.parametrize(
        "token",
        [
            "PT-wstETH",
            "PT-sUSDe",
            "YT-wstETH",
            "LP-ETH-USDC",
            "SY-wstETH",
            "aToken-WETH",
            "vToken-USDC",
            "sToken-WETH",
        ],
    )
    def test_derivative_tokens_detected(self, token):
        assert _is_known_unpriceable(token) is True

    @pytest.mark.parametrize(
        "token",
        [
            "WETH",
            "USDC",
            "BTC",
            "ETH",
            "wstETH",
            "AAVE",
            "UNI",
        ],
    )
    def test_normal_tokens_not_flagged(self, token):
        assert _is_known_unpriceable(token) is False

    def test_case_insensitive(self):
        assert _is_known_unpriceable("pt-wstETH") is True
        assert _is_known_unpriceable("PT-WSTETH") is True

    def test_prefixes_constant_not_empty(self):
        assert len(KNOWN_UNPRICEABLE_PREFIXES) > 0


class TestGetPriceLogLevel:
    """Test that GetPrice logs at the correct level based on exception type and token."""

    @pytest.mark.asyncio
    async def test_market_service_warns_for_unpriceable_token_all_sources_failed(self, caplog):
        """AllDataSourcesFailed + unpriceable token -> WARNING."""
        from almanak.gateway.services.market_service import MarketServiceServicer

        svc = MarketServiceServicer.__new__(MarketServiceServicer)
        svc._initialized = True
        svc._price_aggregator = AsyncMock()
        svc._price_aggregator.get_aggregated_price.side_effect = AllDataSourcesFailed(
            errors={"coingecko": "not found"}
        )

        request = MagicMock()
        request.token = "PT-wstETH"
        request.quote = "USD"
        context = MagicMock()

        with caplog.at_level(logging.WARNING, logger="almanak.gateway.services.market_service"):
            await svc.GetPrice(request, context)

        warning_records = [r for r in caplog.records if r.levelno == logging.WARNING and "PT-wstETH" in r.message]
        error_records = [r for r in caplog.records if r.levelno == logging.ERROR and "PT-wstETH" in r.message]
        assert len(warning_records) == 1
        assert len(error_records) == 0

    @pytest.mark.asyncio
    async def test_market_service_errors_for_normal_token_all_sources_failed(self, caplog):
        """AllDataSourcesFailed + normal token -> ERROR."""
        from almanak.gateway.services.market_service import MarketServiceServicer

        svc = MarketServiceServicer.__new__(MarketServiceServicer)
        svc._initialized = True
        svc._price_aggregator = AsyncMock()
        svc._price_aggregator.get_aggregated_price.side_effect = AllDataSourcesFailed(
            errors={"coingecko": "not found"}
        )

        request = MagicMock()
        request.token = "WETH"
        request.quote = "USD"
        context = MagicMock()

        with caplog.at_level(logging.WARNING, logger="almanak.gateway.services.market_service"):
            await svc.GetPrice(request, context)

        error_records = [r for r in caplog.records if r.levelno == logging.ERROR and "WETH" in r.message]
        assert len(error_records) == 1

    @pytest.mark.asyncio
    async def test_market_service_errors_for_unpriceable_token_other_exception(self, caplog):
        """Non-AllDataSourcesFailed exception + unpriceable token -> ERROR (not downgraded)."""
        from almanak.gateway.services.market_service import MarketServiceServicer

        svc = MarketServiceServicer.__new__(MarketServiceServicer)
        svc._initialized = True
        svc._price_aggregator = AsyncMock()
        svc._price_aggregator.get_aggregated_price.side_effect = RuntimeError("unexpected failure")

        request = MagicMock()
        request.token = "PT-wstETH"
        request.quote = "USD"
        context = MagicMock()

        with caplog.at_level(logging.WARNING, logger="almanak.gateway.services.market_service"):
            await svc.GetPrice(request, context)

        error_records = [r for r in caplog.records if r.levelno == logging.ERROR and "PT-wstETH" in r.message]
        assert len(error_records) == 1

    @pytest.mark.asyncio
    async def test_gateway_oracle_warns_for_unpriceable_token(self, caplog):
        """GatewayPriceOracle logs WARNING for unpriceable tokens."""
        from almanak.framework.data.price.gateway_oracle import GatewayPriceOracle

        mock_client = MagicMock()
        oracle = GatewayPriceOracle(mock_client)

        with (
            patch("asyncio.to_thread", side_effect=Exception("All data sources failed: coingecko: not found")),
            caplog.at_level(logging.WARNING, logger="almanak.framework.data.price.gateway_oracle"),
            pytest.raises(AllDataSourcesFailed),
        ):
            await oracle.get_aggregated_price("PT-wstETH", "USD")

        warning_records = [r for r in caplog.records if r.levelno == logging.WARNING and "PT-wstETH" in r.message]
        assert len(warning_records) == 1

    @pytest.mark.asyncio
    async def test_gateway_oracle_errors_for_normal_token(self, caplog):
        """GatewayPriceOracle logs ERROR for normal tokens."""
        from almanak.framework.data.price.gateway_oracle import GatewayPriceOracle

        mock_client = MagicMock()
        oracle = GatewayPriceOracle(mock_client)

        with (
            patch("asyncio.to_thread", side_effect=Exception("All data sources failed: coingecko: not found")),
            caplog.at_level(logging.WARNING, logger="almanak.framework.data.price.gateway_oracle"),
            pytest.raises(AllDataSourcesFailed),
        ):
            await oracle.get_aggregated_price("WETH", "USD")

        error_records = [r for r in caplog.records if r.levelno == logging.ERROR and "WETH" in r.message]
        assert len(error_records) == 1

    @pytest.mark.asyncio
    async def test_gateway_oracle_errors_for_infra_failure(self, caplog):
        """GatewayPriceOracle logs ERROR for UNAVAILABLE, even for unpriceable tokens."""
        from almanak.framework.data.price.gateway_oracle import GatewayPriceOracle

        mock_client = MagicMock()
        oracle = GatewayPriceOracle(mock_client)

        with (
            patch("asyncio.to_thread", side_effect=Exception("UNAVAILABLE: gateway down")),
            caplog.at_level(logging.WARNING, logger="almanak.framework.data.price.gateway_oracle"),
            pytest.raises(DataSourceUnavailable),
        ):
            await oracle.get_aggregated_price("PT-wstETH", "USD")

        error_records = [r for r in caplog.records if r.levelno == logging.ERROR and "PT-wstETH" in r.message]
        assert len(error_records) == 1
