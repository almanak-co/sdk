from __future__ import annotations

from typing import Any


class ZerionClientFactory:
    name = "zerion"

    def build(self, *, settings: Any) -> Any:
        from .client import ZerionIntegration

        return ZerionIntegration(
            api_key=settings.portfolio_api_key,
            cache_ttl=settings.portfolio_api_cache_ttl,
        )


class ZerionPortfolioProviderFactory:
    name = "zerion"
    requires_api_key = True

    def build(self, *, api_key: str | None, cache_ttl: int, settings: Any | None = None) -> Any:
        from .client import ZerionIntegration

        return ZerionIntegration(api_key=api_key, cache_ttl=cache_ttl)
