"""Strategy-runner hooks for the Hyperliquid connector (VIB-5595).

The CoreWriter submit receipt carries no fill economics (off-EVM settlement), so
the perp accounting event would otherwise record ``None`` fee / realized-PnL /
funding. This hook runs post-receipt, reads HyperCore ``userFills`` /
``userFunding`` through the gateway, correlates the fills to the executed intent
by the deterministic ``cloid`` the order carried, and stamps a measured
``PerpData`` (+ a ``ProtocolFees`` fee) onto ``result.extracted_data`` so the
shared perp accounting handler emits a ``PerpAccountingEvent`` with real
economics.

Best-effort and honest (Empty ≠ Zero): if the gateway read fails or no fill has
settled yet, nothing is stamped and the perp event keeps its honest
ESTIMATED / None. The registry wraps this hook fail-open.
"""

from __future__ import annotations

import logging
from typing import Any, ClassVar

from almanak.connectors._base.types import ProtocolKind, ProtocolName
from almanak.connectors._strategy_base.runner_hook_registry import (
    RunnerHookConnector,
    RunnerResultEnrichmentCapability,
)

logger = logging.getLogger(__name__)


class HyperliquidRunnerHookConnector(
    RunnerHookConnector,
    RunnerResultEnrichmentCapability,
):
    """Runner hook: reconstruct HL perp fill economics from HyperCore fills."""

    protocol: ClassVar[ProtocolName] = ProtocolName("hyperliquid")
    kind: ClassVar[ProtocolKind] = ProtocolKind.PERP

    def enrich_result(self, result: Any, *, gateway_client: Any, chain: str, wallet_address: str = "") -> None:
        """Stamp measured fill economics onto ``result.extracted_data``.

        No-op unless the result carries a decodable HL CoreWriter limit-order
        submission (so this is inert for every non-HL result). Never overwrites a
        ``perp_data`` already present. Never fabricates a value — a failed gateway
        read or an unsettled fill leaves the perp event honest (None).
        """
        if chain != "hyperevm":
            return
        extracted = getattr(result, "extracted_data", None)
        if not isinstance(extracted, dict):
            return
        if extracted.get("perp_data") is not None:
            return  # already enriched (idempotent)

        from almanak.connectors.hyperliquid.fill_accounting import build_perp_data_from_fills

        is_open = self._is_open_result(result, extracted)

        bundle = build_perp_data_from_fills(
            result,
            gateway_client=gateway_client,
            wallet_address=wallet_address,
            is_open=is_open,
        )
        if bundle is None:
            return

        extracted["perp_data"] = bundle.perp

        # Surface the measured USD fee. ``PerpData`` has no USD-fee field and the
        # ``PerpAccountingEvent`` has no fee slot either; perp fees flow through
        # ``result.protocol_fees`` (ProtocolFees.perp_fee_usd) into PnL
        # attribution. Only stamp when we MEASURED a fee (Empty ≠ Zero) and the
        # result does not already carry protocol fees.
        self._maybe_stamp_fee(result, bundle.fee_usd)

    @staticmethod
    def _is_open_result(result: Any, extracted: dict[str, Any]) -> bool:
        """Best-effort open/close discrimination for the executed perp intent.

        The runner-hook seam does not carry the intent, so infer from the decoded
        order's ``reduce_only`` flag (a PERP_CLOSE compiles reduce-only=True; a
        PERP_OPEN reduce-only=False). Falls back to treating an unknown as an
        open (conservative: opens don't book realized PnL / funding).
        """
        from almanak.connectors.hyperliquid.fill_accounting import _decode_submitted_order

        order = _decode_submitted_order(result)
        if order is not None:
            return not order.reduce_only
        return True

    @staticmethod
    def _maybe_stamp_fee(result: Any, fee_usd: Any) -> None:
        """Attach the measured perp fee as ``ProtocolFees`` when not already set."""
        if fee_usd is None:
            return
        if getattr(result, "protocol_fees", None) is not None:
            return
        from almanak.framework.execution.extracted_data import ProtocolFees

        try:
            fees = ProtocolFees(total_usd=fee_usd, perp_fee_usd=fee_usd)
        except (ValueError, TypeError):
            logger.debug("HL fill accounting: could not build ProtocolFees for fee=%s", fee_usd, exc_info=True)
            return
        try:
            result.protocol_fees = fees
        except Exception:  # noqa: BLE001 — result may be frozen/immutable in odd paths
            logger.debug("HL fill accounting: could not attach protocol_fees", exc_info=True)
        # Mirror into extracted_data so the ledger serializer persists it even if
        # the top-level slot is not read downstream.
        extracted = getattr(result, "extracted_data", None)
        if isinstance(extracted, dict) and "protocol_fees" not in extracted:
            extracted["protocol_fees"] = fees


__all__ = ["HyperliquidRunnerHookConnector"]
