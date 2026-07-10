"""Fill-economics accounting for Hyperliquid CoreWriter perps (VIB-5595).

The CoreWriter submit receipt proves *submission*, not *fill*: the order settles
off the EVM on HyperCore, so entry/exit price, fee, realized PnL and funding are
absent from the receipt (``receipt_parser.py`` honestly returns ``None`` for all
of them). This module reconstructs those fill economics **after** execution by
reading HyperCore's ``userFills`` / ``userFunding`` through the gateway
``PerpFillService`` and correlating them to the intent that produced them via the
deterministic ``cloid`` the CoreWriter order carried (``compiler._cloid`` =
``keccak(intent_id)[:16]``).

The result is a :class:`PerpData` stamped onto ``result.extracted_data`` — the
exact shape the shared perp accounting handler
(``framework/accounting/category_handlers/perp_handler.py``) reads to emit a
``PerpAccountingEvent`` with measured ``fee`` / ``realized_pnl`` / ``funding``.
No writes happen here; the runner's outbox → AccountingProcessor → AccountingWriter
pipeline persists the event (AccountingWriter is the only save path).

Empty ≠ Zero throughout: a fill field the venue did not report stays ``None``
(unmeasured); a measured zero is a real ``Decimal("0")``. If the gateway read
fails, nothing is stamped (the perp event keeps its honest ESTIMATED / None).

Gateway boundary: this module runs in the strategy container and performs NO
egress. The userFills / userFunding HTTP calls live in the gateway
(``almanak/gateway/services/perp_fill_service.py`` + the connector's gateway
provider). Here we only speak gRPC through ``gateway_client.perp_fill``.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any

logger = logging.getLogger(__name__)

# Direction labels HyperCore reports on a CLOSING fill (``dir`` field). Only
# closing fills carry a non-zero ``closedPnl``; opening fills carry ``"0.0"``.
_CLOSE_DIR_HINTS = ("close", "long > short", "short > long")

# VIB-5724 — leverage is an integer on-venue (``uint32``); a sub-0.01 gap is only
# a Decimal-representation artefact of the requested value, not a real divergence.
_LEVERAGE_DIVERGENCE_TOLERANCE = Decimal("0.01")


def _to_decimal(value: Any) -> Decimal | None:
    """Parse a wire string to Decimal, Empty ≠ Zero.

    ``""`` / ``None`` → ``None`` (unmeasured). A parseable value (including
    ``"0"``) → its Decimal (measured). Non-finite / unparseable → ``None``.
    """
    if value is None or value == "":
        return None
    try:
        d = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None
    return d if d.is_finite() else None


def _normalize_cloid(cloid: str) -> str:
    """Normalize a cloid hex string for equality (lowercase, strip 0x, unpad).

    Our submitted cloid is ``hex(uint128)`` (``compiler._cloid`` → ``sdk`` →
    ``receipt_parser.extract_position_id``). HyperCore may echo the cloid with a
    different zero-padding, so compare on the unpadded lowercase hex value.
    """
    if not cloid:
        return ""
    s = str(cloid).strip().lower()
    if s.startswith("0x"):
        s = s[2:]
    return s.lstrip("0") or "0"


@dataclass
class _SubmittedOrder:
    """The order we submitted, decoded from the CoreWriter RawAction receipt."""

    cloid_hex: str
    asset_index: int
    reduce_only: bool


def _decode_submitted_order(result: Any) -> _SubmittedOrder | None:
    """Decode the CoreWriter limit order from ``result``'s receipt.

    Returns ``None`` when the result carries no decodable HL limit-order
    submission (not a HL perp result, or a cancel / transfer action).
    """
    from almanak.connectors.hyperliquid.receipt_parser import HyperliquidReceiptParser

    parser = HyperliquidReceiptParser()
    for tr in getattr(result, "transaction_results", None) or []:
        receipt = getattr(tr, "receipt", None)
        if receipt is None:
            continue
        receipt_dict = receipt.to_dict() if hasattr(receipt, "to_dict") else receipt
        if not isinstance(receipt_dict, dict):
            continue
        parsed = parser.parse_receipt(receipt_dict)
        if parsed.limit_orders:
            order = parsed.limit_orders[0]
            return _SubmittedOrder(
                cloid_hex=hex(order.cloid),
                asset_index=int(order.asset),
                reduce_only=bool(order.reduce_only),
            )
    return None


def _coin_for_asset_index(asset_index: int) -> str:
    """Resolve a HyperCore asset index → base coin symbol (best-effort).

    Uses the connector's static market seed. Returns ``""`` when the index is
    outside the seed (the gateway then reads fills for ALL coins and we still
    correlate by cloid, so an unseeded coin is not a hard failure).
    """
    from almanak.connectors.hyperliquid.markets import _SEED_MARKETS

    for market in _SEED_MARKETS:
        if market.asset_index == asset_index:
            return market.symbol
    return ""


@dataclass
class _AggregatedFills:
    """Fill economics aggregated across every fill matching one cloid."""

    fee_usd: Decimal | None
    realized_pnl_usd: Decimal | None
    avg_price: Decimal | None
    total_size: Decimal | None
    matched_fill_count: int


def _aggregate_matching_fills(fills: list[Any], cloid_hex: str) -> _AggregatedFills:
    """Aggregate every gateway fill whose cloid matches our submitted order.

    - ``fee_usd`` — sum of per-fill fees. Measured iff at least one matched fill
      reported a fee; a matched fill with an empty fee contributes nothing but
      does not taint the sum (Empty ≠ Zero: the aggregate stays ``None`` only
      when NO matched fill reported a fee).
    - ``realized_pnl_usd`` — sum of per-fill ``closedPnl`` (non-zero on closes).
    - ``avg_price`` — size-weighted average fill price.
    - ``total_size`` — sum of absolute fill sizes.
    """
    want = _normalize_cloid(cloid_hex)

    fee_sum: Decimal | None = None
    pnl_sum: Decimal | None = None
    notional_sum = Decimal("0")
    size_sum = Decimal("0")
    matched = 0

    for fill in fills:
        if _normalize_cloid(getattr(fill, "cloid", "")) != want:
            continue
        matched += 1

        fee = _to_decimal(getattr(fill, "fee", ""))
        if fee is not None:
            fee_sum = fee if fee_sum is None else fee_sum + fee

        pnl = _to_decimal(getattr(fill, "closed_pnl", ""))
        if pnl is not None:
            pnl_sum = pnl if pnl_sum is None else pnl_sum + pnl

        px = _to_decimal(getattr(fill, "px", ""))
        sz = _to_decimal(getattr(fill, "sz", ""))
        if px is not None and sz is not None:
            abs_sz = abs(sz)
            notional_sum += px * abs_sz
            size_sum += abs_sz

    avg_price = (notional_sum / size_sum) if size_sum > 0 else None
    total_size = size_sum if size_sum > 0 else None
    return _AggregatedFills(
        fee_usd=fee_sum,
        realized_pnl_usd=pnl_sum,
        avg_price=avg_price,
        total_size=total_size,
        matched_fill_count=matched,
    )


def _sum_funding(deltas: list[Any]) -> Decimal | None:
    """Sum funding deltas (USDC, signed). ``None`` when none were measured.

    Returns the net funding across the supplied deltas. A missing / empty ``usdc``
    field on a delta contributes nothing; the aggregate is ``None`` only when NO
    delta reported a usdc amount (Empty ≠ Zero).
    """
    total: Decimal | None = None
    for delta in deltas:
        usdc = _to_decimal(getattr(delta, "usdc", ""))
        if usdc is not None:
            total = usdc if total is None else total + usdc
    return total


def read_venue_leverage(
    gateway_client: Any,
    *,
    wallet_address: str,
    asset_index: int,
    chain: str,
) -> tuple[Decimal | None, str | None]:
    """Read the VENUE-observed leverage + margin mode for an open position (VIB-5724).

    CoreWriter has no set-leverage / margin-mode action (the action set is IDs
    1-13,15,16 — ``updateLeverage`` lives only on the L1 EIP-712 exchange
    endpoint), so a submitted ``leverage`` is NOT applied on-venue: the position
    opens at the account's existing per-asset leverage and margin mode. This
    reads the ``0x0800`` position precompile — where the venue leverage
    (``uint32``) and ``isIsolated`` (``bool``) are decoded — so accounting can
    record the truth the venue used rather than the value the intent requested.

    Returns ``(venue_leverage, venue_margin_mode)`` where ``venue_margin_mode`` is
    ``"isolated"`` / ``"cross"``. Empty ≠ Zero: a failed/unavailable read, a flat
    read (``szi == 0`` — no position to describe), or a malformed
    ``leverage == 0`` all yield ``(None, None)`` — NEVER a fabricated ``0`` and
    NEVER the requested value. The egress is the gateway ``eth_call`` (the same
    channel ``perps_read`` uses); this stays a pure caller of that boundary.
    """
    from .addresses import PRECOMPILE_POSITION
    from .sdk import decode_position, encode_position_query

    if not wallet_address or not chain:
        return (None, None)
    try:
        eth_call = gateway_client.eth_call
    except AttributeError:
        logger.debug("HL venue leverage: gateway has no eth_call; unmeasured", exc_info=True)
        return (None, None)

    try:
        data = "0x" + encode_position_query(wallet_address, int(asset_index)).hex()
        raw = eth_call(chain, PRECOMPILE_POSITION, data)
    except Exception:  # noqa: BLE001 — any gateway/encode fault → UNMEASURED
        logger.debug("HL venue leverage: position eth_call failed", exc_info=True)
        return (None, None)

    if not isinstance(raw, str) or not raw:
        # Empty≠Zero: an unmeasured / non-hex read is not a position at 0x leverage.
        return (None, None)

    try:
        pos = decode_position(raw)
        # A flat read (no position) or a malformed leverage carries no venue
        # truth. Property access + Decimal coercion stay INSIDE the try: this
        # helper promises never to raise into the fill-accounting path, so a
        # malformed decode result (missing attribute, None leverage) must
        # degrade to unmeasured, not propagate.
        if not pos.is_open or pos.leverage is None or pos.leverage <= 0:
            return (None, None)
        venue_margin_mode = "isolated" if pos.is_isolated else "cross"
        return (Decimal(pos.leverage), venue_margin_mode)
    except Exception:  # noqa: BLE001 — a bad blob is unmeasured, not a fabricated read
        logger.debug("HL venue leverage: could not decode position blob", exc_info=True)
        return (None, None)


def _warn_on_leverage_divergence(
    *,
    venue_leverage: Decimal | None,
    venue_margin_mode: str | None,
    leverage_requested: Decimal | None,
    position_id: str,
    coin: str,
) -> None:
    """Log a loud WARNING when the venue truth diverges from the intent request (VIB-5724).

    The compile-time warning already flags that leverage cannot be set on-venue;
    this is the *observed* confirmation that it wasn't. Never raises / halts (the
    position is already open) — it only surfaces the divergence with BOTH values
    and the position identity so an operator can reconcile the on-venue risk.
    """
    if venue_leverage is None or leverage_requested is None:
        return  # cannot compare — at least one side unmeasured (Empty≠Zero)
    if abs(venue_leverage - leverage_requested) <= _LEVERAGE_DIVERGENCE_TOLERANCE:
        return  # venue honoured the request (within rounding) — nothing to flag
    logger.warning(
        "HL leverage divergence: intent requested leverage=%s but venue opened "
        "leverage=%s margin_mode=%s (cloid=%s coin=%s). CoreWriter cannot set "
        "leverage/margin-mode; the position uses the account's per-asset default. "
        "Accounting records the venue truth (venue_leverage/venue_margin_mode); "
        "the requested value is kept as metadata only.",
        leverage_requested,
        venue_leverage,
        venue_margin_mode or "?",
        position_id,
        coin or "?",
    )


def build_perp_data_from_fills(
    result: Any,
    *,
    gateway_client: Any,
    wallet_address: str,
    is_open: bool,
    chain: str = "",
    leverage_requested: Decimal | None = None,
) -> Any | None:
    """Build a :class:`PerpData` from HyperCore fills, correlated by cloid.

    Reads ``userFills`` (and, for a close, ``userFunding``) through the gateway,
    matches by the submitted order's cloid, and returns a ``PerpData`` carrying
    the measured fill economics — or ``None`` when nothing could be measured (no
    HL order in the receipt, gateway unavailable, or no matching fill yet). The
    caller stamps the return onto ``result.extracted_data['perp_data']``.

    ``is_open`` selects which fields we populate: opens carry ``entry_price`` +
    ``fee``; closes carry ``exit_price`` + ``realized_pnl`` + ``fee`` +
    ``funding_fee_usd``. Both are best-effort and honest (Empty ≠ Zero).

    VIB-5724: on an OPEN, once the fill is confirmed, also read the VENUE-observed
    leverage + margin mode from the position precompile and stamp them on the
    ``PerpData`` (``venue_leverage`` / ``venue_margin_mode``), setting the
    canonical ``leverage`` field to the venue truth too. ``leverage_requested``
    (the intent's requested value) is carried as metadata only, and a divergence
    between the two is logged loudly. A failed venue read leaves the venue fields
    ``None`` (Empty ≠ Zero) — never defaulted to the requested value.
    """
    from almanak.framework.execution.extracted_data import PerpData

    if not wallet_address:
        logger.debug("HL fill accounting: no wallet_address; skipping")
        return None

    order = _decode_submitted_order(result)
    if order is None:
        return None  # not a HL limit-order submission

    coin = _coin_for_asset_index(order.asset_index)

    fills_result = _read_user_fills(gateway_client, wallet_address=wallet_address, coin=coin)
    if fills_result is None:
        # Gateway read failed / unavailable → UNMEASURED. Do not fabricate.
        return None

    agg = _aggregate_matching_fills(list(fills_result), order.cloid_hex)
    if agg.matched_fill_count == 0:
        # Settlement not yet observable (async). Honest: leave perp event as-is.
        logger.debug(
            "HL fill accounting: no fills matched cloid %s for %s yet",
            order.cloid_hex,
            coin or "?",
        )
        return None

    funding_usd: Decimal | None = None
    if not is_open:
        funding_deltas = _read_user_funding(gateway_client, wallet_address=wallet_address, coin=coin)
        if funding_deltas is not None:
            funding_usd = _sum_funding(list(funding_deltas))

    # VIB-5724 — on an OPEN, propagate the VENUE-observed leverage + margin mode.
    # A close flattens the position, so its post-close leverage carries no truth;
    # only opens read the venue leverage. Empty≠Zero: an unmeasured read stays None.
    venue_leverage: Decimal | None = None
    venue_margin_mode: str | None = None
    if is_open:
        venue_leverage, venue_margin_mode = read_venue_leverage(
            gateway_client,
            wallet_address=wallet_address,
            asset_index=order.asset_index,
            chain=chain,
        )
        _warn_on_leverage_divergence(
            venue_leverage=venue_leverage,
            venue_margin_mode=venue_margin_mode,
            leverage_requested=leverage_requested,
            position_id=order.cloid_hex,
            coin=coin,
        )

    perp = PerpData(
        position_id=order.cloid_hex,
        entry_price=agg.avg_price if is_open else None,
        exit_price=agg.avg_price if not is_open else None,
        realized_pnl=agg.realized_pnl_usd if not is_open else None,
        fees_paid=None,  # fees carried in USD below; raw-int fees_paid stays unmeasured
        funding_fee_usd=funding_usd,
        # Venue truth (Empty≠Zero when unread). The canonical ``leverage`` field
        # carries the venue value — never the requested one — so any reader of
        # ``leverage`` sees on-venue reality.
        leverage=venue_leverage,
        venue_leverage=venue_leverage,
        venue_margin_mode=venue_margin_mode,
        leverage_requested=leverage_requested,
    )
    # ``PerpData`` has no dedicated USD-fee field; the perp handler reads
    # ``fee``-style economics through ``funding_fee_usd`` / ``realized_pnl`` /
    # price. Surface the measured fee via the top-level extracted key the
    # ResultEnricher perp spec already declares (``fees_paid`` is raw-int only).
    logger.info(
        "HL fill accounting: cloid=%s coin=%s matched=%d fee=%s realized_pnl=%s funding=%s",
        order.cloid_hex,
        coin or "?",
        agg.matched_fill_count,
        agg.fee_usd,
        agg.realized_pnl_usd,
        funding_usd,
    )
    return _PerpFillBundle(perp=perp, fee_usd=agg.fee_usd)


@dataclass
class _PerpFillBundle:
    """PerpData plus the measured USD fee (kept separate — PerpData has no USD fee field)."""

    perp: Any
    fee_usd: Decimal | None


def _read_user_fills(gateway_client: Any, *, wallet_address: str, coin: str) -> list[Any] | None:
    """Read ``userFills`` via the gateway. ``None`` on failure (UNMEASURED)."""
    from almanak.gateway.proto import gateway_pb2

    try:
        stub = gateway_client.perp_fill
    except Exception:  # noqa: BLE001 — no stub / not connected
        logger.debug("HL fill accounting: perp_fill stub unavailable", exc_info=True)
        return None

    request = gateway_pb2.UserFillsRequest(
        venue="hyperliquid",
        wallet_address=wallet_address,
        coin=coin,
    )
    try:
        response = stub.GetUserFills(request, timeout=10.0)
    except Exception:  # noqa: BLE001 — gateway/network fault → UNMEASURED
        logger.debug("HL fill accounting: GetUserFills failed", exc_info=True)
        return None

    if not getattr(response, "success", False):
        logger.debug("HL fill accounting: GetUserFills success=false (%s)", getattr(response, "error", ""))
        return None
    return list(response.fills)


def _read_user_funding(gateway_client: Any, *, wallet_address: str, coin: str) -> list[Any] | None:
    """Read ``userFunding`` via the gateway. ``None`` on failure (UNMEASURED)."""
    from almanak.gateway.proto import gateway_pb2

    try:
        stub = gateway_client.perp_fill
    except Exception:  # noqa: BLE001
        return None

    request = gateway_pb2.UserFundingRequest(
        venue="hyperliquid",
        wallet_address=wallet_address,
        coin=coin,
    )
    try:
        response = stub.GetUserFunding(request, timeout=10.0)
    except Exception:  # noqa: BLE001
        logger.debug("HL fill accounting: GetUserFunding failed", exc_info=True)
        return None

    if not getattr(response, "success", False):
        return None
    return list(response.deltas)


def read_order_status(gateway_client: Any, *, wallet_address: str, cloid_hex: str) -> Any | None:
    """Read a single order's ``orderStatus`` by ``cloid`` via the gateway (VIB-5616).

    Returns the ``OrderStatusResponse`` on a measured read (``success=True``), or
    ``None`` when the read could not be measured (no stub, transport fault, or
    ``success=False``). Empty ≠ Zero: an unmeasured read is ``None``, never a
    fabricated verdict — the caller keeps the position PENDING.

    ``cloid_hex`` is the submitted order's ``hex(uint128)`` cloid; the RPC wire
    field is the decimal-string form of that uint128 (proto has no uint128).
    """
    from almanak.gateway.proto import gateway_pb2

    try:
        cloid_int = int(cloid_hex, 16)
    except (TypeError, ValueError):
        logger.debug("HL fill reconciliation: un-parseable cloid_hex %r", cloid_hex)
        return None

    try:
        stub = gateway_client.perp_fill
    except Exception:  # noqa: BLE001 — no stub / not connected
        logger.debug("HL fill reconciliation: perp_fill stub unavailable", exc_info=True)
        return None

    request = gateway_pb2.OrderStatusRequest(
        venue="hyperliquid",
        wallet_address=wallet_address,
        cloid=str(cloid_int),
        chain="hyperevm",
    )
    try:
        response = stub.GetOrderStatus(request, timeout=10.0)
    except Exception:  # noqa: BLE001 — gateway/network fault → UNMEASURED
        logger.debug("HL fill reconciliation: GetOrderStatus failed", exc_info=True)
        return None

    if not getattr(response, "success", False):
        logger.debug("HL fill reconciliation: GetOrderStatus success=false (%s)", getattr(response, "error", ""))
        return None
    return response


__all__ = ["build_perp_data_from_fills", "read_order_status", "read_venue_leverage"]
