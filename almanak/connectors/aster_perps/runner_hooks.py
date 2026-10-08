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
        result = Decimal(str(value))
        return result if result.is_finite() else None
    except (InvalidOperation, ValueError, TypeError):
        return None


def _withdrawal_cash_transfer(withdrawal: dict[str, Any]) -> dict[str, Any]:
    amount = _decimal(withdrawal.get("amount"))
    fee = _decimal(withdrawal.get("fee"))
    net_amount = amount - fee if amount is not None and fee is not None and amount >= fee >= 0 else None
    return {
        "schema_version": 1,
        "type": "WITHDRAW",
        "transfer_id": withdrawal.get("withdraw_id"),
        "asset": withdrawal.get("asset"),
        "gross_amount": None if amount is None else str(amount),
        "fee_amount": None if fee is None else str(fee),
        "net_amount": None if net_amount is None else str(net_amount),
        "fee_usd": str(fee)
        if net_amount is not None and str(withdrawal.get("asset", "")).upper() in _USD_PAR_ASSETS
        else None,
        "receiver": withdrawal.get("receiver"),
    }


def _enrich_venue_receipt(
    extracted: dict[str, Any], withdrawal: Any, order: Any, *, chain: str, wallet_address: str
) -> None:
    evidence = withdrawal if isinstance(withdrawal, dict) else order
    if isinstance(evidence, dict):
        extracted["venue_receipt"] = {
            "schema_version": 1,
            "protocol": "aster_perps",
            "chain": chain,
            "wallet_address": wallet_address,
            "kind": "WITHDRAWAL_ACCEPTED" if evidence is withdrawal else "ORDER",
            "execution_id": str(evidence.get("withdraw_id") if evidence is withdrawal else evidence.get("order_id")),
            "request_id": evidence.get("client_request_id")
            if evidence is withdrawal
            else evidence.get("client_order_id"),
        }


def _observed_leverage(
    order: dict[str, Any], *, gateway_client: Any, wallet_address: str, is_open: bool
) -> Decimal | None:
    leverage = None
    if is_open and gateway_client is not None:
        from almanak.connectors.aster_perps.gateway_client import AsterGatewayError, GatewayAsterPerpsClient

        try:
            positions = GatewayAsterPerpsClient(gateway_client).get_positions(wallet_address=wallet_address)
            matches = [
                p
                for p in positions
                if p.symbol == order.get("symbol")
                and p.position_amt != 0
                and (p.position_amt > 0) == order.get("is_long")
            ]
            if len(matches) == 1:
                leverage = matches[0].leverage
        except AsterGatewayError:
            logger.warning("Aster fill leverage is unmeasured: position read failed")
    return leverage


def _perp_data(order: dict[str, Any], *, gateway_client: Any, wallet_address: str) -> Any:
    from almanak.framework.execution.extracted_data import PerpData

    is_open = not order.get("reduce_only")
    avg_price = _decimal(order.get("avg_price"))
    usd_denominated = str(order.get("fee_asset", "")).upper() in _USD_PAR_ASSETS
    realized = _decimal(order.get("realized_pnl")) if usd_denominated and not is_open else None
    requested = _decimal(order.get("leverage_requested"))
    leverage = _observed_leverage(order, gateway_client=gateway_client, wallet_address=wallet_address, is_open=is_open)
    # position_id stays unset: the framework derives the canonical perp
    # identity from the intent, and a venue-local id would contradict it.
    # The perp accounting handler reads ``size_delta`` as USD notional; the
    # venue's executed quote amount is the measured size, not the request.
    return PerpData(
        is_long=order.get("is_long") if isinstance(order.get("is_long"), bool) else None,
        size_delta=_decimal(order.get("cum_quote")),
        entry_price=avg_price if is_open else None,
        exit_price=avg_price if not is_open else None,
        realized_pnl=realized,
        leverage_requested=requested,
        leverage=leverage,
        venue_leverage=leverage,
    )


def _attach_protocol_fees(result: Any, extracted: dict[str, Any], order: dict[str, Any]) -> None:
    from almanak.framework.execution.extracted_data import ProtocolFees

    usd_denominated = str(order.get("fee_asset", "")).upper() in _USD_PAR_ASSETS
    fee = _decimal(order.get("fee")) if usd_denominated else None
    if fee is not None and getattr(result, "protocol_fees", None) is None:
        fees = ProtocolFees(total_usd=fee, perp_fee_usd=fee)
        try:
            result.protocol_fees = fees
        except Exception:  # noqa: BLE001 — frozen result objects keep the extracted copy only
            logger.debug("Aster: could not attach protocol_fees to result", exc_info=True)
        extracted.setdefault("protocol_fees", fees)


class AsterPerpsRunnerHookConnector(RunnerHookConnector, RunnerResultEnrichmentCapability):
    protocol: ClassVar[ProtocolName] = ProtocolName("aster_perps")
    kind: ClassVar[ProtocolKind] = ProtocolKind.PERP

    def enrich_result(self, result: Any, *, gateway_client: Any, chain: str, wallet_address: str = "") -> None:
        extracted = getattr(result, "extracted_data", None)
        if not isinstance(extracted, dict):
            return
        withdrawal = extracted.get("aster_withdraw")
        if isinstance(withdrawal, dict):
            extracted["venue_cash_transfer"] = _withdrawal_cash_transfer(withdrawal)
        order = extracted.get(_ORDER_KEY)
        _enrich_venue_receipt(extracted, withdrawal, order, chain=chain, wallet_address=wallet_address)
        if not isinstance(order, dict) or extracted.get("perp_data") is not None:
            return
        extracted["perp_data"] = _perp_data(order, gateway_client=gateway_client, wallet_address=wallet_address)
        _attach_protocol_fees(result, extracted, order)


__all__ = ["AsterPerpsRunnerHookConnector"]
