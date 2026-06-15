"""Shared V3-fork LP registry-payload helpers.

Single source of truth for the LP_CLOSE registry-payload composition,
audit-M1 cross-check, and OPEN-field merge semantics shared by the
Uniswap V3 fork family (uniswap_v3, sushiswap_v3, pancakeswap_v3,
aerodrome Slipstream). Bodies relocated verbatim from
``UniswapV3ReceiptParser`` (see blueprint 19); the parser keeps
delegating wrappers for backward compatibility.

This module must not import any concrete connector (foundation rule -
AGENTS.md §Connector additions, blueprint 22).
"""

from __future__ import annotations

from typing import Any


def open_payload_token_id_int(open_payload: dict[str, Any]) -> int | None:
    """Coerce ``open_payload['token_id']`` to ``int`` or ``None``.

    Returns ``None`` for missing / empty / non-integer values. Pulled
    out of the close-payload extractor's cross-check so the coercion
    path stays trivially testable on its own.
    """
    raw = open_payload.get("token_id")
    if raw is None or raw == "":
        return None
    try:
        return int(raw)
    except (TypeError, ValueError):
        return None


def open_payload_disagrees(
    *,
    open_payload: dict[str, Any] | None,
    token_id: int,
    pool_address: str,
) -> bool:
    """Audit M1 cross-check.

    Return ``True`` iff ``open_payload`` is non-None AND its identity
    anchors disagree with the close receipt's anchors. The caller
    treats a True here as "wrong OPEN row threaded - refuse the
    close" and returns ``None`` so the registry doesn't overwrite the
    close-receipt anchors with stale data.
    ``open_payload=None`` (legacy / orphan close) returns ``False``
    - there's nothing to disagree with.
    """
    if open_payload is None:
        return False
    open_token_id = open_payload_token_id_int(open_payload)
    if open_token_id is not None and open_token_id != token_id:
        return True
    open_pool = str(open_payload.get("pool_address") or "").lower()
    return bool(open_pool and open_pool != pool_address)


def build_close_receipt_payload(
    *,
    token_id: int,
    pool_address: str,
    lp_close: Any,
    nft_manager_addr: str,
) -> dict[str, Any]:
    """Compose the receipt-only portion of the LP_CLOSE registry payload.

    T08 golden contract: ``amount0_close`` / ``amount1_close`` equal
    ``LPCloseData.amount{0,1}_collected`` AS-EMITTED by the parser
    (the NPM Collect -> user totals), NOT a derived principal-only
    figure. ``fee_owed_{0,1}`` carry the parser's fees emission
    alongside. Round-3 attempted to subtract fees out; that
    contradicted the T08 goldens - see audit m8 in the L2 contract
    test for the full rationale.

    VIB-4470 - when ``lp_close.fees{0,1}`` is ``None`` (unmeasured per
    Empty != Zero) emit JSON ``null`` rather than the literal string
    ``"None"``. Downstream registry consumers distinguish unmeasured
    from measured-zero via ``null`` vs ``"0"``.
    """
    payload: dict[str, Any] = {
        "token_id": str(token_id),
        "pool_address": pool_address,
        # VIB-5117 — Empty ≠ Zero on the principal legs: emit JSON ``null`` for
        # an unmeasured leg (a V4 native principal the receipt could not
        # observe), never the literal string ``"None"``. Symmetric with the
        # fee_owed guards below. V3 parsers never produce ``None`` here, but the
        # field type is now ``int | None`` so the guard is correct-by-construction.
        "amount0_close": (str(lp_close.amount0_collected) if lp_close.amount0_collected is not None else None),
        "amount1_close": (str(lp_close.amount1_collected) if lp_close.amount1_collected is not None else None),
        "fee_owed_0": str(lp_close.fees0) if lp_close.fees0 is not None else None,
        "fee_owed_1": str(lp_close.fees1) if lp_close.fees1 is not None else None,
        "nft_manager_addr": nft_manager_addr,
    }
    if lp_close.liquidity_removed is not None:
        payload["liquidity"] = str(lp_close.liquidity_removed)
    return payload


def merge_open_payload_fields(
    payload: dict[str, Any],
    open_payload: dict[str, Any] | None,
) -> None:
    """Merge OPEN-time fields onto a close ``payload`` in place.

    The close receipt cannot re-derive ticks, OPEN-time amounts, the
    original mint liquidity, the fee tier, or the token labels -
    those come from the OPEN-side registry row threaded through by
    the strategy author / runner. When ``open_payload`` is ``None``
    (legacy / orphan close), this is a no-op; the registry's
    ON CONFLICT clause preserves the existing OPEN-side values via
    ``COALESCE``-style merges (blueprint 28 §4.3 / sqlite store
    contract).

    OPEN-time liquidity wins for the registry row's ``liquidity``
    payload field - matches the goldens which preserve the original
    mint amount, not the burned amount.
    """
    if open_payload is None:
        return
    for key in ("tick_lower", "tick_upper"):
        if open_payload.get(key) is not None and key not in payload:
            payload[key] = open_payload[key]
    if "amount0" in open_payload and open_payload["amount0"] is not None:
        payload.setdefault("amount0_open", open_payload["amount0"])
    if "amount1" in open_payload and open_payload["amount1"] is not None:
        payload.setdefault("amount1_open", open_payload["amount1"])
    if "liquidity" in open_payload and open_payload["liquidity"] is not None:
        # OPEN-time liquidity wins (see docstring).
        payload["liquidity"] = open_payload["liquidity"]
    if "fee_tier" in open_payload and open_payload["fee_tier"] is not None:
        payload.setdefault("fee_tier", open_payload["fee_tier"])
    for label in ("_token0_label", "_token1_label"):
        if open_payload.get(label):
            payload.setdefault(label, open_payload[label])


__all__ = [
    "build_close_receipt_payload",
    "merge_open_payload_fields",
    "open_payload_disagrees",
    "open_payload_token_id_int",
]
