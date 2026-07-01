"""GMX V2 teardown residual discovery — pending unfilled orders (VIB-5116).

Discovers this deployment's **pending (unfilled) GMX V2 orders** directly from
chain, so teardown can surface the collateral they hold in the OrderVault even
when the strategy's ``get_open_positions()`` reports nothing (the order is not a
position and was never written to the ``position_registry``; the
enumeration-blindness root cause of VIB-5116).

Published on the connector manifest as ``teardown_residual_discovery`` and run by
the framework (``almanak.framework.teardown.residual_discovery``) over the
deployment's own wallet on GMX's chains. Gateway-routed only; never raises —
returns ``ok=False`` on any unmeasured read so a strand can never be silently
missed (Empty ≠ Zero).
"""

from __future__ import annotations

from decimal import Decimal
from typing import Any

from almanak.connectors._strategy_base.teardown_residual_discovery import (
    PendingResidual,
    ResidualDiscoveryResult,
)
from almanak.connectors.gmx_v2.orders_read import GMX_USD_DECIMALS
from almanak.connectors.gmx_v2.teardown_reads import read_pending_orders

_USD_DIVISOR = Decimal(10**GMX_USD_DECIMALS)


def gmx_v2_teardown_residual_discovery(
    wallet_address: str,
    chain: str,
    gateway_client: Any | None = None,
    rpc_url: str | None = None,  # noqa: ARG001 — protocol parity; framework crosses the gateway only
    block: int | str | None = None,
) -> ResidualDiscoveryResult:
    """Discover the wallet's pending GMX V2 orders on ``chain``.

    Each pending order becomes a :class:`PendingResidual` keyed by its on-chain
    order key, carrying the market / collateral token / committed collateral
    amount / order type so the operator sees exactly what is stranded and the
    teardown post-condition can re-verify it on-chain. A measured empty order
    book yields no residuals; an unmeasured read yields ``ok=False`` (fail-closed).
    """
    result = read_pending_orders(gateway_client, chain, wallet_address, block=block)
    if not result.ok:
        # UNMEASURED read (gateway/RPC error, decode fault, partial data): report
        # ok=False so the framework surfaces its loud closure-failing sentinel. We
        # do NOT swallow this as "no orders" — that fail-quiet-read-as-zero is the
        # exact VIB-5116 bug (Empty != Zero, fail-closed LOUD). Safe under
        # teardown's inverted failure semantics: loud, never blocks risk reduction.
        return ResidualDiscoveryResult(ok=False, error=result.error)

    residuals: list[PendingResidual] = []
    for idx, order in enumerate(result.orders):
        order_key = order.order_key or (result.order_keys[idx] if idx < len(result.order_keys) else "")
        identifier = order_key or f"gmx-order-{chain}-{idx}"
        details: dict[str, Any] = {
            "kind": "pending_order",
            "order_key": order_key,
            "venue": "gmx_v2",
        }
        # Detail fields are best-effort (order_type == -1 marks a key-only stub
        # where the Order.Props struct decode was unavailable — detection still
        # holds via the key). Only surface fields we actually measured.
        if order.order_type >= 0:
            details["order_type"] = order.order_type
            details["is_long"] = order.is_long
        if order.market:
            details["market"] = order.market
        if order.initial_collateral_token:
            details["collateral_token"] = order.initial_collateral_token
        if order.initial_collateral_delta_amount:
            details["collateral_amount_raw"] = str(order.initial_collateral_delta_amount)
        if order.size_delta_usd:
            details["size_delta_usd"] = str(Decimal(order.size_delta_usd) / _USD_DIVISOR)
        if result.error:
            # A non-fatal note (e.g. detail decode drifted) — surface it so the
            # operator knows detection held via the key list even if detail didn't.
            details["detail_note"] = result.error
        residuals.append(
            PendingResidual(
                protocol="gmx_v2",
                chain=str(chain or "").lower(),
                identifier=identifier,
                position_type="PERP",
                value_usd=Decimal("0"),
                details=details,
            )
        )
    return ResidualDiscoveryResult(residuals=residuals, ok=True)


__all__ = ["gmx_v2_teardown_residual_discovery"]
