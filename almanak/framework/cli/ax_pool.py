"""Combine verified pool identity with exact-address state reads."""

from collections.abc import Callable
from typing import Any

from almanak.framework.agent_tools.errors import AgentErrorCode, ToolErrorPayload, get_error_category
from almanak.framework.agent_tools.schemas import ToolResponse, ToolResponseStatus


def enrich_pool_identity(
    identity: ToolResponse,
    *,
    address: str,
    chain: str,
    run_tool: Callable[[str, dict[str, Any]], ToolResponse],
) -> ToolResponse:
    if identity.status != ToolResponseStatus.SUCCESS or not isinstance(identity.data, dict):
        return identity
    data = dict(identity.data)
    address = address.lower()
    reason = _state_unavailability(data)
    if reason:
        return identity.model_copy(update={"data": {**data, "state_status": "unavailable", "state_reason": reason}})

    coins = data.get("coins") or []
    token0 = data.get("token0") or (coins[0] if len(coins) >= 2 else None)
    token1 = data.get("token1") or (coins[1] if len(coins) >= 2 else None)
    if not token0 or not token1:
        return identity.model_copy(
            update={"data": {**data, "state_status": "unavailable", "state_reason": "Pool tokens were not identified"}}
        )
    state = run_tool(
        "get_pool_state",
        {
            "chain": chain,
            "protocol": data["protocol"],
            "pool_address": address,
            "token_a": token0,
            "token_b": token1,
            "fee_tier": data.get("fee_tier"),
        },
    )
    if state.status != ToolResponseStatus.SUCCESS:
        reason = state.error.message if state.error else "Pool state read failed"
        return state.model_copy(update={"data": {**data, "state_status": "unavailable", "state_reason": reason}})

    values = dict(state.data or {})
    if str(values.get("pool_address", "")).lower() != address.lower():
        code = AgentErrorCode.VALIDATION_ERROR
        return ToolResponse(
            status=ToolResponseStatus.ERROR,
            data={**data, "state_status": "unavailable", "state_reason": "State reader returned a different pool"},
            error=ToolErrorPayload(
                error_code=code,
                message="State reader returned a different pool",
                recoverable=False,
                error_category=get_error_category(code),
            ),
        )
    # Identity retains contract addresses; state readers may return display symbols.
    for key in ("token0", "token1"):
        if key in values and values[key] != data.get(key):
            values[f"{key}_symbol"] = values.pop(key)
    merged = {**values, **data, "state_status": "available"}
    if data.get("fee_tier") is not None:
        merged["fee_tier_source"] = "pool_identity"
    missing = [key for key in ("tvl_usd", "volume_24h_usd", "fee_apr") if merged.get(key) in (None, "")]
    if missing:
        merged["analytics_unavailable_fields"] = missing
        merged["analytics_unavailable_reason"] = "State reader did not return these analytics"
    return identity.model_copy(update={"data": merged})


def _state_unavailability(data: dict[str, Any]) -> str | None:
    if data.get("kind") == "pool_id":
        return "Pool-id identity does not include the token pair required by the state reader"
    if data.get("kind") != "pool":
        return f"Identified contract kind {data.get('kind', 'unknown')} is not a pool"
    if data.get("factory_verified") != "verified":
        return "Pool identity is not factory verified"
    if not data.get("protocol"):
        return "Pool protocol is not identified"
    return None
