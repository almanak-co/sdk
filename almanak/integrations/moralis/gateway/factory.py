from __future__ import annotations

from typing import Any


class MoralisPortfolioProviderFactory:
    name = "moralis"
    requires_api_key = True

    def build(self, *, api_key: str | None, cache_ttl: int, settings: Any | None = None) -> Any:
        from .client import MoralisIntegration

        return MoralisIntegration(api_key=api_key, cache_ttl=cache_ttl)
