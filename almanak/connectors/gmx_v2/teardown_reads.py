"""GMX V2 teardown on-chain reads — gateway-routed orchestration (VIB-5116).

Thin gateway-routed wrappers over the pure calldata/decode helpers in
``orders_read.py``. Both the teardown residual discovery
(``teardown_residual_discovery.py``) and the teardown post-condition
(``teardown_post_condition.py``) call :func:`read_pending_orders` so the strand
detection lives in one place.

Every on-chain read goes through the supplied ``gateway_client`` (the
``GatewayClient.eth_call`` RPC proxy) — no provider is opened here
(gateway-boundary rule). NEVER raises: any failure returns an UNMEASURED
``PendingOrdersResult(ok=False, ...)`` so an unreadable account can never be
mistaken for "no pending orders" (Empty ≠ Zero — the exact VIB-5116 bug).
"""

from __future__ import annotations

import logging
from typing import Any

from almanak.connectors.gmx_v2.addresses import GMX_V2
from almanak.connectors.gmx_v2.orders_read import (
    MAX_ORDER_RANGE,
    PendingOrder,
    PendingOrdersResult,
    build_account_orders_calldata,
    build_order_count_calldata,
    build_order_keys_calldata,
    decode_account_orders,
    decode_bytes32_array,
    decode_uint,
)

logger = logging.getLogger(__name__)

# Canonical Multicall3 (same address on Arbitrum / Avalanche / most EVM chains).
# ``getCurrentBlockTimestamp()`` (selector 0x0f28c97d) returns ``block.timestamp``
# via the existing gateway ``eth_call`` — no new gateway capability, no provider
# opened here (gateway-boundary rule). Used to age-gate account-initiated order
# cancellation (VIB-5568): GMX rejects a cancel until the order is old enough.
_MULTICALL3 = "0xcA11bde05977b3631167028862bE2a173976CA11"
_GET_CURRENT_BLOCK_TIMESTAMP_CALLDATA = "0x0f28c97d"


def read_chain_timestamp(gateway_client: Any, chain: str, *, block: int | str | None = None) -> int | None:
    """Read ``block.timestamp`` on ``chain`` via Multicall3 (gateway-routed).

    Returns ``None`` on any unmeasured read (no gateway, eth_call error, or
    undecodable result) — callers fail-closed (defer cancellation) rather than
    guess the current time.
    """
    if gateway_client is None:
        return None
    try:
        blob = gateway_client.eth_call(
            chain=chain, to=_MULTICALL3, data=_GET_CURRENT_BLOCK_TIMESTAMP_CALLDATA, block=block
        )
        # decode_uint stays INSIDE the try: a malformed/empty blob must degrade to
        # None (fail-closed), never propagate. (Gemini review.)
        ts = decode_uint(blob)
        return ts if ts and ts > 0 else None
    except Exception:  # noqa: BLE001 — unmeasured ⇒ None (fail-closed at the caller)
        logger.debug("Multicall3 getCurrentBlockTimestamp eth_call raised on %s", chain, exc_info=True)
        return None


def resolve_gmx_contracts(chain: str) -> tuple[str | None, str | None, str | None]:
    """Resolve ``(reader, data_store, order_vault)`` for ``chain``; ``None`` per role when absent."""
    table = GMX_V2.get(str(chain or "").lower(), {})
    return table.get("reader"), table.get("data_store"), table.get("order_vault")


def read_open_positions(
    gateway_client: Any,
    chain: str,
    account: str,
    *,
    block: int | str | None = None,
) -> Any:
    """Read ``account``'s active GMX V2 positions on ``chain`` via the gateway.

    Reuses the connector's drift-safe ``perps_read`` plan/decode (the VIB-5289
    ``Position.Props`` struct is maintained in exactly one place) and returns its
    :class:`PerpsReadResult` (``.positions`` active-only, ``.ok`` Empty ≠ Zero: a
    failed read is ``ok=False``, a measured-empty book is ``ok=True`` + no
    positions).
    """
    from almanak.connectors._strategy_base.perps_read_base import PerpsPositionQuery, PerpsReadResult
    from almanak.connectors.gmx_v2 import perps_read as _pr

    reader, data_store, _order_vault = resolve_gmx_contracts(chain)
    if not reader or not data_store:
        return PerpsReadResult(positions=(), ok=False)
    query = PerpsPositionQuery(
        chain=str(chain or "").lower(),
        wallet_address=account,
        targets={"reader": reader, "data_store": data_store},
    )
    calls = _pr._build_gmx_calls(query)
    if not calls:
        return PerpsReadResult(positions=(), ok=False)
    try:
        blob = gateway_client.eth_call(chain=chain, to=calls[0].to, data=calls[0].data, block=block)
    except Exception:  # noqa: BLE001 — fail-closed (unmeasured)
        logger.debug("GMX getAccountPositions eth_call raised", exc_info=True)
        return PerpsReadResult(positions=(), ok=False)
    return _pr._reduce_gmx_positions(query, [blob])


def read_pending_orders(
    gateway_client: Any,
    chain: str,
    account: str,
    *,
    block: int | str | None = None,
) -> PendingOrdersResult:
    """Read ``account``'s pending GMX V2 orders on ``chain`` via the gateway.

    Detection is anchored on the drift-proof ``DataStore`` order-list count/keys;
    the ``Reader.getAccountOrders`` struct decode is best-effort detail on top. A
    ``count == 0`` is a MEASURED empty book (``ok=True``, no orders); any read that
    cannot be measured returns ``ok=False`` (fail-closed).
    """
    if gateway_client is None:
        return PendingOrdersResult(ok=False, error="no gateway client to read pending orders")
    if not str(account or "").strip():
        return PendingOrdersResult(ok=False, error="no account address to read pending orders")

    reader, data_store, _order_vault = resolve_gmx_contracts(chain)
    if not data_store:
        return PendingOrdersResult(ok=False, error=f"no GMX DataStore registered for chain {chain!r}")

    # 1) Order COUNT (stable ABI) — the fail-closed detection anchor.
    try:
        count_blob = gateway_client.eth_call(
            chain=chain, to=data_store, data=build_order_count_calldata(account), block=block
        )
    except Exception as exc:  # noqa: BLE001 — fail-closed
        return PendingOrdersResult(ok=False, error=f"getBytes32Count eth_call raised: {exc}")
    count = decode_uint(count_blob)
    if count is None:
        # No EVIDENCE (measured_count=None): the count read itself was unmeasured,
        # a broad gateway failure — NOT positive proof this wallet has GMX orders.
        # The caller must not fail-closed a non-GMX deployment on this.
        return PendingOrdersResult(ok=False, error="getBytes32Count returned unmeasured/undecodable data")
    if count == 0:
        return PendingOrdersResult(orders=[], order_keys=[], ok=True, measured_count=0)  # measured empty book

    truncated = count > MAX_ORDER_RANGE
    if truncated:
        logger.warning(
            "GMX pending-order read: account has %d pending orders on %s but only the first %d are "
            "enumerated in one window — the aggregate strand still fires on count>0, but the key set "
            "is PARTIAL (a not-found key is not treated as closed)",
            count,
            chain,
            MAX_ORDER_RANGE,
        )
    end = min(count, MAX_ORDER_RANGE)

    # 2) Order KEYS (stable ABI) — the drift-proof identity surface.
    try:
        keys_blob = gateway_client.eth_call(
            chain=chain, to=data_store, data=build_order_keys_calldata(account, 0, end), block=block
        )
    except Exception as exc:  # noqa: BLE001 — fail-closed WITH evidence (count>0)
        return PendingOrdersResult(ok=False, measured_count=count, error=f"getBytes32ValuesAt eth_call raised: {exc}")
    keys = decode_bytes32_array(keys_blob)
    if keys is None:
        # POSITIVE evidence (measured_count=count>0) but could not read the keys —
        # UNMEASURED identity, fail-closed (we KNOW a strand exists but cannot
        # enumerate it). The evidence scopes the fail-closed to this real GMX user.
        return PendingOrdersResult(
            ok=False, measured_count=count, error=f"order count={count} but getBytes32ValuesAt was unmeasured"
        )

    # 3) Order DETAIL (versioned struct) — best-effort on top of detection.
    orders: list[PendingOrder] | None = None
    detail_note: str | None = None
    if reader:
        try:
            detail_blob = gateway_client.eth_call(
                chain=chain,
                to=reader,
                data=build_account_orders_calldata(data_store, account, 0, end),
                block=block,
            )
            orders = decode_account_orders(detail_blob, order_keys=keys)
        except Exception as exc:  # noqa: BLE001 — detail is best-effort; detection holds via keys
            logger.debug("GMX getAccountOrders detail read failed: %s", exc, exc_info=True)
            orders = None
            detail_note = f"detail read raised: {exc}"
    else:
        detail_note = f"no GMX Reader registered for chain {chain!r}; detail unavailable"

    if not orders:
        # Detail unavailable/empty but keys prove pending orders exist: surface
        # key-only stubs (order_type=-1 marks "detail unmeasured") so detection is
        # never lost to struct drift.
        orders = [
            PendingOrder(
                market="",
                initial_collateral_token="",
                initial_collateral_delta_amount=0,
                size_delta_usd=0,
                order_type=-1,
                execution_fee=0,
                is_long=False,
                order_key=key,
            )
            for key in keys
        ]
        detail_note = detail_note or "getAccountOrders detail undecodable (struct drift?); detection via key list"

    return PendingOrdersResult(
        orders=orders, order_keys=keys, ok=True, measured_count=count, truncated=truncated, error=detail_note
    )


__all__ = ["read_chain_timestamp", "read_pending_orders", "resolve_gmx_contracts"]
