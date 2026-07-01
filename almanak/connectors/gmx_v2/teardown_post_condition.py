"""GMX V2 teardown on-chain closure verifier (VIB-5116).

Before this hook, GMX perp had NO per-position on-chain closure authority in the
teardown post-teardown verify (S6): ``plan_a_reconciliation._reconcile_one``
returned ``UNVERIFIABLE`` and no post-condition was registered, so a residual
perp position — or a pending (unfilled) order still holding collateral in the
OrderVault — was reported optimistically as closed (the VIB-5116 silent-success
strand, the perp analogue of the VIB-3741/3742 LP $1.16-leak). This hook reads
on-chain truth via the gateway and fails the teardown closed on ANY residual:

* **Pending order** (``details['kind'] == 'pending_order'`` — surfaced by the
  teardown residual discovery, ``teardown_residual_discovery.py``): closed iff the
  order key is no longer in the wallet's on-chain pending-order set. This is the
  strand VIB-5116 targets — a cancelled/executed order leaves the set; a still
  unfilled one does not.
* **Unverified residual** (``details['kind'] == 'residual_unverified'`` — the
  framework's fail-closed sentinel for an UNMEASURED discovery read): closed iff
  the wallet now has ZERO pending orders on-chain (a clean re-read clears it).
* **Open perp position** (any other ``gmx_v2`` position, e.g. a registry
  perp-cutover row): closed iff the wallet has no active position for that market
  (and side, when known) — ``sizeInUsd == 0``.

Closure rule is **exact**: any pending order / any matching active position ⇒ NOT
closed. Fail-closed is the safe error direction (teardown's inverted failure
semantics make a FAILED verification loud-but-non-blocking); the opposite error —
reporting a still-funded order/position as closed — is the leak we must never
make. Empty ≠ Zero: an unmeasured read (gateway/RPC error) is NEVER "closed".

Gateway boundary: every read goes through the supplied ``gateway_client``. No
direct egress; ``rpc_url`` is accepted for protocol parity but not consumed.
NEVER raises.
"""

from __future__ import annotations

from typing import Any

from almanak.connectors._strategy_base.teardown_post_condition import ClosureCheckResult
from almanak.connectors.gmx_v2.teardown_reads import read_open_positions, read_pending_orders

_PENDING_KINDS = frozenset({"pending_order", "residual_unverified"})


def _result(closed: bool, position_id: str, **extra: Any) -> ClosureCheckResult:
    residual = {k: v for k, v in extra.items() if k not in ("error",)}
    return ClosureCheckResult(
        closed=closed,
        protocol="gmx_v2",
        position_id=position_id,
        residual=residual,
        error=extra.get("error"),
    )


def _verify_pending_order(
    position: Any, wallet_address: str, gateway_client: Any, block: int | str | None
) -> ClosureCheckResult:
    """Closed iff the order key is gone from the wallet's on-chain pending set."""
    chain = getattr(position, "chain", None) or ""
    position_id = str(getattr(position, "position_id", "") or "")
    details = getattr(position, "details", None) or {}
    kind = str(details.get("kind") or "").lower()

    result = read_pending_orders(gateway_client, chain, wallet_address, block=block)
    if not result.ok:
        return _result(
            False,
            position_id,
            error=(
                f"GMX pending-order closure UNVERIFIED for {position_id}: {result.error} — "
                "fail-closed (an unmeasured order read is never 'closed')"
            ),
        )

    on_chain_keys = {str(k).lower() for k in result.order_keys}
    on_chain_keys.update(str(o.order_key).lower() for o in result.orders if o.order_key)

    # The unverified sentinel has no specific key: it clears only when the wallet
    # has NO pending orders at all (a clean, measured, empty re-read).
    order_key = str(details.get("order_key") or "").lower()
    if kind == "residual_unverified" or not order_key:
        if not result.orders and not result.order_keys:
            return _result(True, position_id)
        return _result(
            False,
            position_id,
            pending_order_count=len(result.order_keys) or len(result.orders),
            order_keys=sorted(on_chain_keys),
        )

    if order_key in on_chain_keys:
        return _result(False, position_id, order_key=order_key, kind="pending_order")
    # Not found in the on-chain key set. If the read was TRUNCATED (more pending
    # orders than one window), this key may lie beyond the window — do NOT read it
    # as closed (that would be the silent-strand class VIB-5116 fixes); fail-closed.
    if getattr(result, "truncated", False):
        return _result(
            False,
            position_id,
            order_key=order_key,
            error=(
                f"GMX pending-order closure UNVERIFIED for {position_id}: on-chain order set was "
                "TRUNCATED (more orders than one read window); key not in the partial set — fail-closed"
            ),
        )
    return _result(True, position_id)


def _verify_open_position(
    position: Any, wallet_address: str, gateway_client: Any, block: int | str | None
) -> ClosureCheckResult:
    """Closed iff the deployment wallet has ZERO active GMX positions on-chain.

    Under 1 gateway : 1 strategy (blueprint 20 §1) the wallet is the deployment's
    own, so EVERY active GMX position it holds is this deployment's residual perp
    risk. That makes the account-level "any active position ⇒ not closed" the
    correct, robust closure rule — and it deliberately avoids matching a registry
    position by its ``market`` field, which may be a symbol while
    ``getAccountPositions`` returns market ADDRESSES (a symbol-vs-address mismatch
    would false-report a still-open position as closed, the exact leak this seam
    prevents). A deployment closing several markets reports every position
    not-closed until the whole book is flat, which is the honest state.
    """
    chain = getattr(position, "chain", None) or ""
    position_id = str(getattr(position, "position_id", "") or "")

    pr = read_open_positions(gateway_client, chain, wallet_address, block=block)
    if not getattr(pr, "ok", False):
        return _result(
            False,
            position_id,
            error=(
                f"GMX position closure UNVERIFIED for {position_id}: getAccountPositions "
                "read was unmeasured — fail-closed"
            ),
        )

    active = [p for p in pr.positions if getattr(p, "is_active", False)]
    if not active:
        return _result(True, position_id)
    return _result(
        False,
        position_id,
        active_positions=len(active),
        markets=sorted({str(getattr(p, "market", "") or "").lower() for p in active}),
    )


def gmx_v2_teardown_post_condition(
    position: Any,
    wallet_address: str,
    gateway_client: Any | None = None,
    rpc_url: str | None = None,  # noqa: ARG001 — protocol parity; framework crosses the gateway only
    block: int | str | None = None,
) -> ClosureCheckResult:
    """Verify a GMX V2 residual is closed on-chain (pending order or open position)."""
    position_id = str(getattr(position, "position_id", "") or "")
    chain = getattr(position, "chain", None) or ""
    if not chain:
        return _result(False, position_id, error="GMX post-condition needs position.chain; none found")
    if gateway_client is None:
        return _result(
            False,
            position_id,
            error=(
                "GMX post-condition requires a gateway_client to read on-chain truth "
                "(pending orders / getAccountPositions). None supplied — verification cannot proceed."
            ),
        )
    if not str(wallet_address or "").strip():
        return _result(False, position_id, error="GMX post-condition needs a wallet address; none found")

    details = getattr(position, "details", None) or {}
    kind = str(details.get("kind") or "").lower()
    if kind in _PENDING_KINDS:
        return _verify_pending_order(position, wallet_address, gateway_client, block)
    return _verify_open_position(position, wallet_address, gateway_client, block)


__all__ = ["gmx_v2_teardown_post_condition"]
