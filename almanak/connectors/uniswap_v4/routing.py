"""Connector-owned swap selection; discovery ends before an executable quote begins."""

from __future__ import annotations

import re
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any

from almanak.connectors._strategy_base.v4_pool_abi import V4_ZERO_ADDRESS, resolve_v4_tick_spacing

from .pool_key import PoolKey

_POOL_ID = re.compile(r"0x[0-9a-fA-F]{64}\Z")
_HEX_BYTES = re.compile(r"0x(?:[0-9a-fA-F]{2})*\Z")
_KEYS = frozenset({"pool_key", "pool_id", "fee_tier", "tick_spacing", "hooks", "hook_data"})


@dataclass(frozen=True, slots=True)
class SwapSelection:
    key: PoolKey
    hook_data: bytes | None


def _validate_explicit_pins(params: Mapping[str, Any], key: PoolKey) -> None:
    for supplied, field in (("fee_tier", "fee"), ("tick_spacing", "tick_spacing"), ("hooks", "hooks")):
        if supplied not in params:
            continue
        value = params[supplied]
        if field == "hooks" and isinstance(value, str):
            value = value.lower()
        if value != getattr(key, field):
            raise ValueError(f"{supplied} conflicts with the selected PoolKey")


def resolve_swap_selection(
    params: Mapping[str, Any] | None,
    *,
    token_in: str,
    token_out: str,
    default_fee: int,
    lookup: Callable[[str], PoolKey | None] | None = None,
) -> SwapSelection:
    params = {} if params is None else params
    unknown = set(params) - _KEYS
    if unknown:
        raise ValueError(f"Unsupported V4 swap parameters: {sorted(unknown)}")
    raw_data = params.get("hook_data")
    if "hook_data" in params and (type(raw_data) is not str or not _HEX_BYTES.fullmatch(raw_data)):
        raise ValueError("hook_data must be explicit 0x-prefixed, even-length hex bytes")
    data = None if raw_data is None else bytes.fromhex(raw_data[2:])
    key = PoolKey.from_wire(params["pool_key"]) if "pool_key" in params else None
    if "pool_id" in params:
        pool_id = params["pool_id"]
        if type(pool_id) is not str or not _POOL_ID.fullmatch(pool_id):
            raise ValueError("pool_id must be a 0x-prefixed bytes32 V4 pool ID")
        if key is None:
            if lookup is None:
                raise ValueError("Resolving pool_id requires a connected gateway or an explicit full pool_key")
            key = lookup(pool_id)
            if key is None:
                raise ValueError(
                    "PoolKey discovery unavailable; supply a full key from verified initialization evidence"
                )
        if key.pool_id != pool_id.lower():
            raise ValueError("Resolved PoolKey hash does not match the requested pool_id")
    if key is None:
        fee = params.get("fee_tier", default_fee)
        key = PoolKey(
            token_in,
            token_out,
            fee,
            resolve_v4_tick_spacing(fee, params.get("tick_spacing")),
            params.get("hooks", V4_ZERO_ADDRESS),
        )
    else:
        _validate_explicit_pins(params, key)
    key.direction(token_in, token_out)
    if key.hooks == V4_ZERO_ADDRESS:
        if data not in (None, b""):
            raise ValueError("A no-hook pool only accepts empty hook_data")
        data = b""
    elif data is None:
        raise ValueError("Hook data is absent; provide explicit empty or profile-validated bytes")
    return SwapSelection(key=key, hook_data=data)
