from __future__ import annotations

from typing import Any


class OkxPortfolioProviderFactory:
    name = "okx"
    requires_api_key = False

    def build(self, *, api_key: str | None, cache_ttl: int, settings: Any | None = None) -> Any:
        from .client import OkxIntegration

        return OkxIntegration(api_key=api_key, cache_ttl=cache_ttl)
