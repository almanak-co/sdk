"""Gateway-owned CoinGecko contract-resolution cache and error semantics."""

from unittest.mock import AsyncMock

import pytest

from almanak.integrations._base.gateway.base import IntegrationError
from almanak.integrations.coingecko.gateway.client import CoinGeckoIntegration

_BASE_USDC = "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913"


@pytest.mark.asyncio
async def test_cold_base_usdc_resolution_is_cache_fronted_gateway_http() -> None:
    integration = CoinGeckoIntegration(api_key="gateway-only-key")
    integration._fetch = AsyncMock(return_value={"id": "usd-coin"})  # type: ignore[method-assign]

    cold = await integration.resolve_contract(asset_platform="base", contract_address=_BASE_USDC.upper())
    warm = await integration.resolve_contract(asset_platform="base", contract_address=_BASE_USDC)

    assert cold == {"coin_id": "usd-coin", "found": True, "source": "coingecko_api"}
    assert warm == {"coin_id": "usd-coin", "found": True, "source": "gateway_cache"}
    integration._fetch.assert_awaited_once_with(f"/coins/base/contract/{_BASE_USDC}")


@pytest.mark.asyncio
async def test_explicit_404_is_short_cached_as_honest_miss() -> None:
    integration = CoinGeckoIntegration(api_key="gateway-only-key")
    integration._fetch = AsyncMock(  # type: ignore[method-assign]
        side_effect=IntegrationError("coingecko", "HTTP 404: not found", code="HTTP_404")
    )

    cold = await integration.resolve_contract(asset_platform="base", contract_address=_BASE_USDC)
    warm = await integration.resolve_contract(asset_platform="base", contract_address=_BASE_USDC)

    assert cold == {"coin_id": "", "found": False, "source": "coingecko_api"}
    assert warm == {"coin_id": "", "found": False, "source": "gateway_cache"}
    integration._fetch.assert_awaited_once()


@pytest.mark.asyncio
async def test_transient_upstream_error_is_never_cached_as_not_found() -> None:
    integration = CoinGeckoIntegration(api_key="gateway-only-key")
    integration._fetch = AsyncMock(  # type: ignore[method-assign]
        side_effect=IntegrationError("coingecko", "network down", code="NETWORK_ERROR")
    )

    with pytest.raises(IntegrationError, match="network down"):
        await integration.resolve_contract(asset_platform="base", contract_address=_BASE_USDC)
    with pytest.raises(IntegrationError, match="network down"):
        await integration.resolve_contract(asset_platform="base", contract_address=_BASE_USDC)

    assert integration._fetch.await_count == 2


@pytest.mark.asyncio
async def test_malformed_success_is_typed_unavailable_not_an_honest_miss() -> None:
    integration = CoinGeckoIntegration(api_key="gateway-only-key")
    integration._fetch = AsyncMock(return_value={})  # type: ignore[method-assign]

    with pytest.raises(IntegrationError) as exc_info:
        await integration.resolve_contract(asset_platform="base", contract_address=_BASE_USDC)

    assert exc_info.value.code == "INVALID_RESPONSE"
