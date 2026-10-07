"""Runner hook: turn an Aster Pro fill into the perp accounting payload.

The execution handler publishes the venue's own fill record under
``extracted_data["aster_order"]``. This hook projects it onto ``PerpData`` and
``ProtocolFees`` so the shared perp accounting handler records measured
economics. Fields the venue did not report stay ``None`` (Empty ≠ Zero).
"""

from __future__ import annotations

import logging
from decimal import Decimal, InvalidOperation
from typing import Any, ClassVar

from almanak.connectors._base.types import ProtocolKind, ProtocolName
from almanak.connectors._strategy_base.runner_hook_registry import (
    RunnerHookConnector,
    RunnerResultEnrichmentCapability,
)

logger = logging.getLogger(__name__)

_ORDER_KEY = "aster_order"
# Fees and realized PnL are charged in the margin asset; only a USD-pegged
# margin asset converts to USD at par.
_USD_PAR_ASSETS = frozenset({"USDT", "USDC", "USD1"})


def _decimal(value: Any) -> Decimal | None:
    if value in (None, ""):
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        return None


class AsterPerpsRunnerHookConnector(RunnerHookConnector, RunnerResultEnrichmentCapability):
    protocol: ClassVar[ProtocolName] = ProtocolName("aster_perps")
    kind: ClassVar[ProtocolKind] = ProtocolKind.PERP

    def enrich_result(self, result: Any, *, gateway_client: Any, chain: str, wallet_address: str = "") -> None:
        extracted = getattr(result, "extracted_data", None)
        if not isinstance(extracted, dict):
            return
        order = extracted.get(_ORDER_KEY)
        if not isinstance(order, dict) or extracted.get("perp_data") is not None:
            return

        from almanak.framework.execution.extracted_data import PerpData, ProtocolFees

        is_open = not order.get("reduce_only")
        avg_price = _decimal(order.get("avg_price"))
        usd_denominated = str(order.get("fee_asset", "")).upper() in _USD_PAR_ASSETS
        realized = _decimal(order.get("realized_pnl")) if usd_denominated and not is_open else None
        requested = _decimal(order.get("leverage_requested"))
        # position_id stays unset: the framework derives the canonical perp
        # identity from the intent, and a venue-local id would contradict it.
        # The perp accounting handler reads ``size_delta`` as USD notional; the
        # venue's executed quote amount is the measured size, not the request.
        extracted["perp_data"] = PerpData(
            size_delta=_decimal(order.get("cum_quote")),
            entry_price=avg_price if is_open else None,
            exit_price=avg_price if not is_open else None,
            realized_pnl=realized,
            leverage_requested=requested,
        )
        fee = _decimal(order.get("fee")) if usd_denominated else None
        if fee is not None and getattr(result, "protocol_fees", None) is None:
            fees = ProtocolFees(total_usd=fee, perp_fee_usd=fee)
            try:
                result.protocol_fees = fees
            except Exception:  # noqa: BLE001 — frozen result objects keep the extracted copy only
                logger.debug("Aster: could not attach protocol_fees to result", exc_info=True)
            extracted.setdefault("protocol_fees", fees)


__all__ = ["AsterPerpsRunnerHookConnector"]
