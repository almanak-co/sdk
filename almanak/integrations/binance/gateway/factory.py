from __future__ import annotations

from typing import Any

from almanak.integrations._base import PriceSourceScope


class BinancePriceSourceFactory:
    name = "binance"
    scope = PriceSourceScope.SHARED
    order = 20

    def supports(self, chain: str | None) -> bool:
        if not chain:
            return False
        from almanak.gateway.validation import is_solana_chain

        return not is_solana_chain(chain)

    def build(self, *, chain: str | None, settings: Any) -> Any:
        from .price_source import BinancePriceSource

        return BinancePriceSource(cache_ttl=30, request_timeout=5.0)


class BinanceClientFactory:
    name = "binance"

    def build(self, *, settings: Any) -> Any:
        from .client import BinanceIntegration

        return BinanceIntegration()
