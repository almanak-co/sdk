from __future__ import annotations

from typing import Any

from almanak.integrations._base import PriceSourceScope


class PythPriceSourceFactory:
    name = "pyth"
    scope = PriceSourceScope.SHARED
    order = 10

    def supports(self, chain: str | None) -> bool:
        if not chain:
            return False
        from almanak.gateway.validation import is_solana_chain

        return is_solana_chain(chain)

    def build(self, *, chain: str | None, settings: Any) -> Any:
        from .price_source import PythPriceSource

        return PythPriceSource(cache_ttl=15)
