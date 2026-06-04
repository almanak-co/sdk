"""Strategy-runner hooks for the Uniswap V3 connector."""

from __future__ import annotations

from typing import Any, ClassVar

from almanak.connectors._base.types import ProtocolKind, ProtocolName
from almanak.connectors._strategy_base.runner_hook_registry import (
    RunnerHookConnector,
    RunnerLPReceiptTopicCapability,
    RunnerResultEnrichmentCapability,
)


class UniswapV3RunnerHookConnector(
    RunnerHookConnector,
    RunnerLPReceiptTopicCapability,
    RunnerResultEnrichmentCapability,
):
    """Runner hooks for Uniswap V3 receipt selection and slot0 enrichment."""

    protocol: ClassVar[ProtocolName] = ProtocolName("uniswap_v3")
    kind: ClassVar[ProtocolKind] = ProtocolKind.LP

    def lp_receipt_topics(self) -> frozenset[str]:
        """Return LP event topics used to pick the bundle receipt to parse."""
        from almanak.connectors.uniswap_v3.receipt_parser import EVENT_TOPICS

        return frozenset(
            {
                EVENT_TOPICS["IncreaseLiquidity"],
                EVENT_TOPICS["DecreaseLiquidity"],
            }
        )

    def enrich_result(self, result: Any, *, gateway_client: Any, chain: str) -> None:
        """Fill missing LP current_tick fields from pool slot0 when possible."""
        extracted = getattr(result, "extracted_data", None)
        if not isinstance(extracted, dict):
            return

        from almanak.connectors.uniswap_v3.slot0_fallback import (
            enrich_lp_close_with_slot0,
            enrich_lp_open_with_slot0,
        )

        lp_open = extracted.get("lp_open_data")
        if lp_open is not None:
            enriched = enrich_lp_open_with_slot0(lp_open, gateway_client=gateway_client, chain=chain)
            if enriched is not lp_open:
                extracted["lp_open_data"] = enriched

        lp_close = extracted.get("lp_close_data")
        if lp_close is not None:
            enriched = enrich_lp_close_with_slot0(lp_close, gateway_client=gateway_client, chain=chain)
            if enriched is not lp_close:
                extracted["lp_close_data"] = enriched


__all__ = ["UniswapV3RunnerHookConnector"]
