from __future__ import annotations

from typing import Any

from almanak.integrations._base import PriceSourceScope


class CoinGeckoPriceSourceFactory:
    name = "coingecko"
    scope = PriceSourceScope.SHARED
    order = 40

    def supports(self, chain: str | None) -> bool:
        return True

    def build(self, *, chain: str | None, settings: Any) -> Any:
        from .price_source import CoinGeckoPriceSource

        return CoinGeckoPriceSource(api_key=settings.coingecko_api_key or "", cache_ttl=30)


class CoinGeckoClientFactory:
    name = "coingecko"

    def build(self, *, settings: Any) -> Any:
        from .client import CoinGeckoIntegration

        return CoinGeckoIntegration(api_key=settings.coingecko_api_key)
