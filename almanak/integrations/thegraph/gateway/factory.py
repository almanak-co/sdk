from __future__ import annotations

from typing import Any


class TheGraphClientFactory:
    name = "thegraph"

    def build(self, *, settings: Any) -> Any:
        from .client import TheGraphIntegration

        return TheGraphIntegration(api_key=settings.thegraph_api_key)
