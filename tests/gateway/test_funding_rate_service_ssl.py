"""Tests for FundingRateService SSL context initialization."""

import ssl
from unittest.mock import AsyncMock, MagicMock, patch

import pytest


@pytest.fixture
def settings():
    mock = MagicMock()
    mock.network = "mainnet"
    return mock


@pytest.mark.asyncio
async def test_get_web3_uses_ssl_context(settings):
    """_get_web3 initializes AsyncHTTPProvider with build_ssl_context kwargs."""
    from almanak.gateway.services.funding_rate_service import FundingRateServiceServicer

    fake_ctx = MagicMock(spec=ssl.SSLContext)
    service = FundingRateServiceServicer(settings=settings)

    with patch(
        "almanak.gateway.services.funding_rate_service.build_ssl_context",
        return_value=fake_ctx,
    ) as mock_build:
        with patch(
            "almanak.gateway.services.funding_rate_service.get_rpc_url",
            return_value="https://test.rpc.example.com",
        ):
            with patch("almanak.gateway.services.funding_rate_service.AsyncHTTPProvider") as mock_provider:
                with patch("almanak.gateway.services.funding_rate_service.AsyncWeb3"):
                    await service._get_web3("arbitrum")

    mock_build.assert_called_once()
    mock_provider.assert_called_once_with(
        "https://test.rpc.example.com",
        request_kwargs={"ssl": fake_ctx},
    )


@pytest.mark.asyncio
async def test_get_web3_returns_none_on_invalid_rpc(settings):
    """_get_web3 returns None when RPC URL resolution fails."""
    from almanak.gateway.services.funding_rate_service import FundingRateServiceServicer

    service = FundingRateServiceServicer(settings=settings)

    with patch(
        "almanak.gateway.services.funding_rate_service.get_rpc_url",
        side_effect=ValueError("no rpc"),
    ):
        result = await service._get_web3("unknownchain")

    assert result is None
