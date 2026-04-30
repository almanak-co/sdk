"""Teardown post-conditions: protocol-specific on-chain closure verification.

VIB-3742 — Framework hardening for graceful teardown.

Background
----------
``TeardownManager._verify_closure`` historically only re-read
``strategy.get_open_positions()`` — an in-memory call that returns 0
immediately after ``on_intent_executed`` clears the strategy's tracked
``_position_id``. Result: the framework reported teardown success while
liquidity remained on-chain (the $1.16-leak scenario behind VIB-3741 / 3742).

Design
------
``TeardownPostCondition`` is a small Protocol that protocol owners implement
to assert "this position is closed on-chain." The teardown manager iterates
the positions that existed *before* execution started (via the snapshot it
already took for ``starting_value_usd``) and dispatches each to the
post-condition registered for the position's protocol.

The registry is open: connectors can register their own post-condition by
calling ``register_teardown_post_condition(protocol, hook)`` at import time.
We ship a default for TraderJoe V2 because that's the protocol the bug was
filed against; future CL connectors (Aerodrome Slipstream, Uniswap V4,
Orca/Raydium CLMM) plug in via the same hook.

Hard constraints
----------------
- All on-chain reads MUST go through the gateway in production. The TJ V2
  default uses ``TraderJoeV2Adapter`` which already accepts a
  ``GatewayClient`` and routes RPC through it. Callers that test locally
  may pass ``rpc_url`` for direct anvil access; this is the same dual-path
  the compiler uses.
- Failures in a hook return ``ClosureCheckResult(closed=False)`` with an
  error message rather than raising — verification is informational and a
  hook crash must not silently pass the teardown.
- No emojis. No Postgres DDL.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, Protocol

logger = logging.getLogger(__name__)


@dataclass
class ClosureCheckResult:
    """Outcome of an on-chain closure verification for a single position.

    Attributes:
        closed: True iff the post-condition determined the position is fully
            closed on-chain. False means residual liquidity / debt was
            detected, OR the check itself errored out (fail-closed).
        protocol: Protocol the result is for (informational, for logs).
        position_id: Position identifier checked.
        residual: Protocol-specific residual data (e.g.
            ``{"bin_balances": {123: 4567}, "total_lb_tokens": 4567}`` for
            TraderJoe V2). Empty when ``closed=True``.
        error: Set when the check itself failed (RPC error, missing
            dependency, etc.). Treated as ``closed=False`` for the
            teardown verifier — fail-closed.
    """

    closed: bool
    protocol: str = ""
    position_id: str = ""
    residual: dict[str, Any] = field(default_factory=dict)
    error: str | None = None


class TeardownPostCondition(Protocol):
    """Protocol-specific on-chain closure check.

    Implementations must be side-effect free — only read state. They run
    AFTER the teardown intents have completed; the teardown manager uses
    the result to decide whether to mark the teardown ``success=False``.

    Implementations should:
    - Return ``ClosureCheckResult(closed=True)`` only when ALL liquidity /
      debt for the given position is provably gone on-chain.
    - Return ``ClosureCheckResult(closed=False, residual=...)`` with a
      detailed residual map when liquidity remains. The residual goes into
      the teardown ``error`` so operators can see exactly which bins / NFTs
      / accounts still have value.
    - Return ``ClosureCheckResult(closed=False, error=...)`` on internal
      failure. The teardown verifier treats this as fail-closed.
    """

    def __call__(
        self,
        position: Any,
        wallet_address: str,
        gateway_client: Any | None = None,
        rpc_url: str | None = None,
    ) -> ClosureCheckResult: ...


# =============================================================================
# Registry
# =============================================================================

_REGISTRY: dict[str, TeardownPostCondition] = {}


def register_teardown_post_condition(protocol: str, hook: TeardownPostCondition) -> None:
    """Register a post-condition for a protocol.

    Idempotent: re-registering the same hook is fine. Replacing an existing
    hook logs a warning so accidental shadowing is visible in logs.
    """
    key = protocol.lower()
    existing = _REGISTRY.get(key)
    if existing is not None and existing is not hook:
        logger.warning(
            "Replacing existing teardown post-condition for protocol %r",
            protocol,
        )
    _REGISTRY[key] = hook


def get_teardown_post_condition(protocol: str) -> TeardownPostCondition | None:
    """Look up a registered post-condition. Returns ``None`` when none."""
    return _REGISTRY.get(protocol.lower())


def has_teardown_post_condition(protocol: str) -> bool:
    """``True`` iff a post-condition is registered for ``protocol``."""
    return protocol.lower() in _REGISTRY


# =============================================================================
# TraderJoe V2 default post-condition
# =============================================================================


def _traderjoe_v2_post_condition(
    position: Any,
    wallet_address: str,
    gateway_client: Any | None = None,
    rpc_url: str | None = None,
) -> ClosureCheckResult:
    """Verify a TraderJoe V2 LP position has zero residual LB token balance.

    Uses the SDK's ``balanceOfBatch`` over the position's known bin_ids when
    they're present in ``position.details``; otherwise falls back to the
    same ±50 bin scan the compiler uses (with the same caveats). When the
    fallback path runs we mark the result with ``residual['fallback_scan']``
    so operators can see the scan was inherently incomplete and treat
    ``closed=True`` from the fallback with appropriate skepticism.
    """
    protocol = "traderjoe_v2"
    position_id = getattr(position, "position_id", "") or ""

    # Pull pool + bin metadata. ``position`` is a teardown ``PositionInfo``
    # (from ``strategy.get_open_positions()``), or anything else with a
    # ``.details`` mapping.
    details = getattr(position, "details", None) or {}
    pool_address = details.get("pool_address") or details.get("pool") or details.get("pool_addr")
    if not pool_address:
        return ClosureCheckResult(
            closed=False,
            protocol=protocol,
            position_id=position_id,
            error=(
                "TraderJoe V2 post-condition needs position.details['pool_address'] "
                "(or 'pool', 'pool_addr'); none found"
            ),
        )

    try:
        from almanak.framework.connectors.traderjoe_v2 import (
            TraderJoeV2Adapter,
            TraderJoeV2Config,
        )
    except Exception as exc:  # noqa: BLE001 — defensive
        return ClosureCheckResult(
            closed=False,
            protocol=protocol,
            position_id=position_id,
            error=f"TraderJoe V2 connector unavailable: {exc}",
        )

    chain = getattr(position, "chain", None) or "avalanche"

    config = TraderJoeV2Config(
        chain=chain,
        wallet_address=wallet_address,
        rpc_url=rpc_url if gateway_client is None else None,
        gateway_client=gateway_client,
    )

    try:
        adapter = TraderJoeV2Adapter(config)
        sdk = adapter.sdk
    except Exception as exc:  # noqa: BLE001
        return ClosureCheckResult(
            closed=False,
            protocol=protocol,
            position_id=position_id,
            error=f"TraderJoe V2 SDK init failed: {exc}",
        )

    # Prefer the explicit bin_ids list — that's the only guaranteed-complete
    # check. If we have it, we're done with one balanceOfBatch round-trip.
    bin_ids_raw = details.get("bin_ids") or []
    try:
        known_bin_ids = [int(b) for b in bin_ids_raw]
    except (TypeError, ValueError):
        known_bin_ids = []

    used_fallback = False
    try:
        if known_bin_ids:
            balances = sdk.get_position_balances_for_ids(pool_address, wallet_address, known_bin_ids)
        else:
            used_fallback = True
            # The active-bin ±50 fallback. Mark as incomplete in residual so
            # the verifier surfaces "scan may have missed bins" to operators.
            balances = sdk.get_position_balances(pool_address, wallet_address)
    except Exception as exc:  # noqa: BLE001
        return ClosureCheckResult(
            closed=False,
            protocol=protocol,
            position_id=position_id,
            error=f"TraderJoe V2 balanceOf query failed: {exc}",
        )

    residual: dict[str, Any] = {}
    if balances:
        residual["bin_balances"] = {int(b): int(v) for b, v in balances.items()}
        residual["total_lb_tokens"] = int(sum(balances.values()))
        residual["pool_address"] = pool_address
        if used_fallback:
            residual["fallback_scan"] = (
                "Used active-id ±50 heuristic (bin_ids unavailable in position.details). "
                "Bins outside that window were NOT checked."
            )

    if balances:
        return ClosureCheckResult(
            closed=False,
            protocol=protocol,
            position_id=position_id,
            residual=residual,
        )

    if used_fallback:
        # Closed-with-asterisk: heuristic returned empty, but it only checked
        # ±50 bins. Surface the caveat so operators know the verifier ran in
        # weak-mode for this position. We still return closed=True because
        # marking otherwise would block every TJ V2 teardown that did not
        # round-trip bin_ids — defeating the purpose of best-effort
        # verification. The compiler-level WARNING (item 1) catches the
        # silent-leak case at a different seam.
        return ClosureCheckResult(
            closed=True,
            protocol=protocol,
            position_id=position_id,
            residual={
                "fallback_scan": (
                    "TraderJoe V2 post-condition used active-id ±50 fallback "
                    "(no bin_ids in position.details). No residual liquidity "
                    "found in the scanned window, but bins outside it were "
                    "NOT checked. To get strong verification, ensure your "
                    "strategy attaches bin_ids to the PositionInfo.details "
                    "for teardown."
                ),
            },
        )

    return ClosureCheckResult(
        closed=True,
        protocol=protocol,
        position_id=position_id,
    )


register_teardown_post_condition("traderjoe_v2", _traderjoe_v2_post_condition)


__all__ = [
    "ClosureCheckResult",
    "TeardownPostCondition",
    "get_teardown_post_condition",
    "has_teardown_post_condition",
    "register_teardown_post_condition",
]
