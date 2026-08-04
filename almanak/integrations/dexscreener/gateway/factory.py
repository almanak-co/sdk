from __future__ import annotations

from typing import Any

from almanak.integrations._base import PriceSourceScope


class DexScreenerPriceSourceFactory:
    name = "dexscreener"
    scope = PriceSourceScope.CHAIN
    order = 30

    def supports(self, chain: str | None) -> bool:
        return bool(chain)

    def build(self, *, chain: str | None, settings: Any) -> Any:
        if chain is None:
            raise ValueError("DexScreener price source requires a chain")
        from almanak.framework.data.tokens import get_token_resolver

        from .price_source import DexScreenerPriceSource

        return DexScreenerPriceSource(
            default_chain_id=chain.lower(),
            cache_ttl=30,
            token_resolver=get_token_resolver(),
        )
