"""Trader Joe V2 teardown post-condition."""

from __future__ import annotations

from typing import Any

from almanak.connectors._strategy_base.teardown_post_condition import ClosureCheckResult


def _resolve_tj_v2_target(
    position: Any,
    gateway_client: Any | None,
    protocol: str,
    position_id: str,
) -> ClosureCheckResult | tuple[str, str]:
    """Run the fail-closed input guards and resolve the on-chain LB-pair target.

    Returns a short-circuit ``ClosureCheckResult`` — a non-LP scope skip
    (``closed=True``) or a fail-closed error (``closed=False``) for a
    missing/invalid pool address, a missing chain, or a hosted-mode call with
    no ``gateway_client`` — or the validated ``(pool_address, chain)`` pair when
    the position is eligible for an on-chain balance check. Never raises: an
    unverifiable position must surface as ``closed=False``, not an exception.
    """
    # ``protocol="traderjoe_v2"`` is shared between LP positions and TOKEN
    # positions reported by swap-only strategies. The LB-pair-shaped check must
    # not run on TOKEN positions because those positions have no pool address.
    position_type_raw = getattr(position, "position_type", None)
    position_type_value = (getattr(position_type_raw, "value", None) or str(position_type_raw or "")).upper()
    if position_type_value and position_type_value != "LP":
        return ClosureCheckResult(
            closed=True,
            protocol=protocol,
            position_id=position_id,
            residual={
                "skipped_reason": (
                    f"TraderJoe V2 post-condition only verifies LP LB-pair positions; "
                    f"position_type={position_type_value!r} is outside scope"
                ),
            },
        )

    details = getattr(position, "details", None) or {}
    pool_address = details.get("pool_address") or details.get("pool_addr") or details.get("pool")
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

    if not (isinstance(pool_address, str) and pool_address.startswith("0x") and len(pool_address) == 42):
        return ClosureCheckResult(
            closed=False,
            protocol=protocol,
            position_id=position_id,
            error=(
                f"TraderJoe V2 post-condition: position.details pool_address must be a "
                f"42-char hex address (got {pool_address!r}). Strategies must populate "
                "details['pool_address'] with the LB pair contract address, not a symbol."
            ),
        )

    # Fail closed on a missing chain rather than guessing "avalanche": the LB
    # pair address is chain-scoped, so verifying against the wrong chain would
    # report incorrect closure state. Mirrors the Uniswap V3 sibling hook.
    chain = getattr(position, "chain", None) or ""
    if not chain:
        return ClosureCheckResult(
            closed=False,
            protocol=protocol,
            position_id=position_id,
            error="TraderJoe V2 post-condition needs position.chain; none found",
        )

    # Gateway-boundary guard: the strategy container has no outbound network
    # access except the gateway channel. A direct ``rpc_url`` read is permitted
    # only as a local/test convenience (the same dual path the compiler uses).
    # In hosted mode a missing ``gateway_client`` must fail closed rather than
    # fall back to direct RPC egress.
    from almanak.framework.deployment import is_hosted

    if gateway_client is None and is_hosted():
        return ClosureCheckResult(
            closed=False,
            protocol=protocol,
            position_id=position_id,
            error=(
                "TraderJoe V2 post-condition requires a gateway_client in hosted "
                "mode; direct rpc_url fallback is local/test only"
            ),
        )

    return pool_address, chain


def traderjoe_v2_post_condition(
    position: Any,
    wallet_address: str,
    gateway_client: Any | None = None,
    rpc_url: str | None = None,
) -> ClosureCheckResult:
    """Verify a TraderJoe V2 LP position has zero residual LB token balance.

    Uses the SDK's ``balanceOfBatch`` over the position's known bin_ids when
    they're present in ``position.details``. Otherwise falls back to the same
    +/-50 bin scan the compiler uses, marking the result with
    ``residual["fallback_scan"]`` so operators can see the scan was incomplete.
    """
    protocol = "traderjoe_v2"
    position_id = getattr(position, "position_id", "") or ""

    target = _resolve_tj_v2_target(position, gateway_client, protocol, position_id)
    if isinstance(target, ClosureCheckResult):
        return target
    pool_address, chain = target

    try:
        from almanak.connectors.traderjoe_v2 import (
            TraderJoeV2Adapter,
            TraderJoeV2Config,
        )
    except Exception as exc:  # noqa: BLE001 - defensive
        return ClosureCheckResult(
            closed=False,
            protocol=protocol,
            position_id=position_id,
            error=f"TraderJoe V2 connector unavailable: {exc}",
        )

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

    details = getattr(position, "details", None) or {}
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
                "Used active-id +/-50 heuristic (bin_ids unavailable in position.details). "
                "Bins outside that window were not checked."
            )

    if balances:
        return ClosureCheckResult(
            closed=False,
            protocol=protocol,
            position_id=position_id,
            residual=residual,
        )

    if used_fallback:
        return ClosureCheckResult(
            closed=True,
            protocol=protocol,
            position_id=position_id,
            residual={
                "fallback_scan": (
                    "TraderJoe V2 post-condition used active-id +/-50 fallback "
                    "(no bin_ids in position.details). No residual liquidity "
                    "found in the scanned window, but bins outside it were not "
                    "checked. To get strong verification, ensure your strategy "
                    "attaches bin_ids to PositionInfo.details for teardown."
                ),
            },
        )

    return ClosureCheckResult(
        closed=True,
        protocol=protocol,
        position_id=position_id,
    )


__all__ = ["traderjoe_v2_post_condition"]
