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
from dataclasses import dataclass
from typing import Any, ClassVar

from almanak.connectors._base.types import ProtocolKind, ProtocolName
from almanak.connectors._strategy_base.runner_hook_registry import (
    FillReconciliationVerdict,
    RunnerFillReconciliationCapability,
    RunnerHookConnector,
    RunnerResultEnrichmentCapability,
)

logger = logging.getLogger(__name__)

_PROTOCOL = ProtocolName("hyperliquid")

# FillStatus string values (StrEnum) the framework treats as TERMINAL — a
# confirmed fill or reject resolves the submission. Defined connector-side (the
# framework stays vocabulary-free; it only reads ``FillReconciliationVerdict.terminal``).
_TERMINAL_FILL_STATUSES = frozenset({"filled", "partially_filled", "rejected"})


@dataclass(frozen=True)
class PendingFillHandle:
    """Serializable correlation handle for a pending PERP_OPEN (VIB-5614).

    Distilled from the submit result at execute time so the runner can re-read the
    fill signal on later ticks without holding the (gone) result. Carries the
    owning ``protocol`` (routes ``resolve_fill_status`` back here) and
    ``intent_type`` (passed to ``strategy.reconcile_fill``), plus the venue
    correlation keys: the deterministic ``cloid`` the order carried and the coin.
    """

    protocol: str
    intent_type: str
    cloid_hex: str
    coin: str


class HyperliquidRunnerHookConnector(
    RunnerHookConnector,
    RunnerResultEnrichmentCapability,
    RunnerFillReconciliationCapability,
):
    """Runner hook: reconstruct HL perp fill economics from HyperCore fills."""

    protocol: ClassVar[ProtocolName] = _PROTOCOL
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

    # ------------------------------------------------------------------ #
    # RunnerFillReconciliationCapability (VIB-5614)
    # ------------------------------------------------------------------ #

    def extract_pending_fill_handle(self, result: Any) -> PendingFillHandle | None:
        """Distil a just-executed PERP_OPEN result into a pending-fill handle.

        Returns ``None`` for anything that is not a HL open submission — a
        reduce-only CLOSE (no fill to await; the position is already managed) or a
        non-HL result. Only OPENs enter PENDING, so only opens get a handle.
        """
        from almanak.connectors.hyperliquid.fill_accounting import (
            _coin_for_asset_index,
            _decode_submitted_order,
        )

        order = _decode_submitted_order(result)
        if order is None or order.reduce_only:
            # Not a HL limit-order submission, or a reduce-only close (not a
            # pending open). Nothing to reconcile.
            return None
        return PendingFillHandle(
            protocol=str(_PROTOCOL),
            intent_type="PERP_OPEN",
            cloid_hex=order.cloid_hex,
            coin=_coin_for_asset_index(order.asset_index),
        )

    def resolve_fill_status(
        self,
        *,
        gateway_client: Any,
        wallet_address: str,
        handle: Any,
    ) -> FillReconciliationVerdict | None:
        """Read HyperCore fills + orderStatus for the pending open and return the verdict.

        Two signals, in strict FILLED-precedence order (egress stays gateway-side):

        1. ``userFills`` by the submitted order's ``cloid`` — a matching fill →
           FILLED (terminal). A fill ALWAYS wins over a stale reject.
        2. When no fill matched yet, ``orderStatus`` by ``cloid`` (VIB-5616) — this
           is the reject-detection signal: ``userFills`` alone cannot distinguish
           "not filled yet" (async settlement lag) from "rejected" (both leave the
           fills book empty), which would strand a genuinely-rejected open in
           PENDING forever. A positively-measured REJECTED → REJECTED (terminal),
           so the runner clears the handle and the strategy re-opens. A measured
           FILLED / PARTIALLY_FILLED there also promotes (fill wins).

        Empty ≠ Zero throughout: a failed/unavailable read on EITHER signal, or an
        orderStatus that is merely RESTING / UNMEASURED, → UNMEASURED (NON-terminal)
        — the strategy stays PENDING and the runner re-pumps next tick. This path
        never fabricates a terminal verdict from an unmeasured read.
        """
        if not isinstance(handle, PendingFillHandle):
            return None
        if not wallet_address:
            return self._unmeasured("no wallet_address")

        from almanak.connectors.hyperliquid.fill_accounting import (
            _aggregate_matching_fills,
            _read_user_fills,
        )
        from almanak.connectors.hyperliquid.fill_reconciliation import FillStatus

        fills = _read_user_fills(gateway_client, wallet_address=wallet_address, coin=handle.coin)
        if fills is None:
            # Gateway read failed / unavailable → UNMEASURED. Never assume flat.
            return self._unmeasured("userFills read unavailable")

        agg = _aggregate_matching_fills(list(fills), handle.cloid_hex)
        if agg.matched_fill_count > 0:
            logger.info(
                "HL fill reconciliation: cloid=%s coin=%s matched=%d → FILLED",
                handle.cloid_hex,
                handle.coin or "?",
                agg.matched_fill_count,
            )
            return self._terminal(FillStatus.FILLED)

        # No fill matched yet — consult orderStatus to tell "not filled yet" from
        # "rejected". FILLED precedence is preserved: we only reach here when the
        # fills book showed no match.
        return self._resolve_from_order_status(gateway_client, wallet_address, handle)

    def _resolve_from_order_status(
        self,
        gateway_client: Any,
        wallet_address: str,
        handle: PendingFillHandle,
    ) -> FillReconciliationVerdict:
        """Reject-detection via ``orderStatus`` (VIB-5616), after fills showed no match.

        A positively-measured terminal status (REJECTED, or a late FILLED /
        PARTIALLY_FILLED the fills book hasn't surfaced yet) → terminal verdict.
        Anything unmeasured / resting → non-terminal UNMEASURED (stay PENDING).
        """
        from almanak.connectors.hyperliquid.fill_accounting import read_order_status
        from almanak.connectors.hyperliquid.fill_reconciliation import FillStatus

        response = read_order_status(gateway_client, wallet_address=wallet_address, cloid_hex=handle.cloid_hex)
        if response is None:
            # orderStatus read unmeasured — no reject signal. Stay PENDING (fills
            # showed no match either; this is the async-lag case).
            return self._unmeasured(f"no fill matched cloid {handle.cloid_hex} yet; orderStatus unmeasured")

        status = str(getattr(response, "status", "") or "")
        if status not in _TERMINAL_FILL_STATUSES:
            # RESTING / UNMEASURED / unrecognised — not a confirmed terminal state.
            return self._unmeasured(
                f"no fill matched cloid {handle.cloid_hex} yet; orderStatus={status or '?'} (non-terminal)"
            )

        logger.info(
            "HL fill reconciliation: cloid=%s coin=%s orderStatus=%s → terminal",
            handle.cloid_hex,
            handle.coin or "?",
            status,
        )
        return self._terminal(FillStatus(status))

    @staticmethod
    def _terminal(status: Any) -> FillReconciliationVerdict:
        """A terminal verdict for a positively-measured fill / reject."""
        return FillReconciliationVerdict(status=status, terminal=str(status) in _TERMINAL_FILL_STATUSES)

    @staticmethod
    def _unmeasured(reason: str) -> FillReconciliationVerdict:
        """A NON-terminal UNMEASURED verdict (Empty ≠ Zero — stays PENDING)."""
        from almanak.connectors.hyperliquid.fill_reconciliation import FillStatus

        logger.debug("HL fill reconciliation UNMEASURED: %s", reason)
        return FillReconciliationVerdict(status=FillStatus.UNMEASURED, terminal=False)


__all__ = ["HyperliquidRunnerHookConnector", "PendingFillHandle"]
