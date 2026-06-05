"""Pendle-owned swap route inference."""

from __future__ import annotations

from typing import Any, ClassVar

from almanak.connectors._base.types import ProtocolKind, ProtocolName
from almanak.connectors._strategy_base.swap_route_inference_registry import (
    SwapRouteInferenceCapability,
    SwapRouteInferenceConnector,
)

_PENDLE_SWAP_TOKEN_PREFIXES = ("PT-", "YT-")


class PendleSwapRouteInferenceConnector(SwapRouteInferenceConnector, SwapRouteInferenceCapability):
    """Claim protocol-less swaps involving Pendle PT or YT symbols."""

    protocol: ClassVar[ProtocolName] = ProtocolName("pendle")
    kind: ClassVar[ProtocolKind] = ProtocolKind.YIELD_TRADING

    def claims_swap_route(self, intent: Any) -> bool:
        from_token = str(getattr(intent, "from_token", "") or "").upper()
        to_token = str(getattr(intent, "to_token", "") or "").upper()
        return from_token.startswith(_PENDLE_SWAP_TOKEN_PREFIXES) or to_token.startswith(_PENDLE_SWAP_TOKEN_PREFIXES)


__all__ = ["PendleSwapRouteInferenceConnector"]
