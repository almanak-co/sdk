"""Protocol identity for V4; mutable LP fees are never part of this value."""

from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from almanak.connectors._strategy_base.v4_pool_abi import (
    V4_ZERO_ADDRESS,
    V4PoolKeyError,
    compute_v4_pool_id,
    is_v4_dynamic_fee,
    resolve_v4_tick_spacing,
    validate_v4_fee_field,
)

_ADDRESS = re.compile(r"0x[0-9a-fA-F]{40}\Z")


@dataclass(frozen=True, slots=True)
class PoolKey:
    """Canonical immutable currencies, raw fee field, independent spacing and hook."""

    currency0: str
    currency1: str
    fee: int
    tick_spacing: int
    hooks: str = V4_ZERO_ADDRESS

    def __post_init__(self) -> None:
        for name in ("currency0", "currency1", "hooks"):
            address = getattr(self, name)
            if type(address) is not str or not _ADDRESS.fullmatch(address):
                raise V4PoolKeyError(f"{name} must be a 20-byte hex address")
            object.__setattr__(self, name, address.lower())
        if self.currency0 == self.currency1:
            raise V4PoolKeyError("V4 pool currencies must be distinct")
        if self.currency0 > self.currency1:
            first, second = self.currency1, self.currency0
            object.__setattr__(self, "currency0", first)
            object.__setattr__(self, "currency1", second)
        validate_v4_fee_field(self.fee)
        resolve_v4_tick_spacing(self.fee, self.tick_spacing)
        flags = int(self.hooks, 16) & ((1 << 14) - 1)
        if self.hooks == V4_ZERO_ADDRESS:
            if self.is_dynamic:
                raise V4PoolKeyError("A dynamic-fee pool requires a nonzero hook")
        elif not flags and not self.is_dynamic:
            raise V4PoolKeyError("A static-fee hook must enable at least one callback")
        for delta_bit, callback_bit in ((3, 7), (2, 6), (1, 10), (0, 8)):
            if flags & (1 << delta_bit) and not flags & (1 << callback_bit):
                raise V4PoolKeyError("A hook return-delta flag requires its corresponding callback flag")

    @property
    def is_dynamic(self) -> bool:
        return is_v4_dynamic_fee(self.fee)

    @property
    def pool_id(self) -> str:
        return compute_v4_pool_id(self.currency0, self.currency1, self.fee, self.tick_spacing, self.hooks)

    def to_wire(self) -> dict[str, Any]:
        return {name: getattr(self, name) for name in ("currency0", "currency1", "fee", "tick_spacing", "hooks")}

    @classmethod
    def from_wire(cls, value: Mapping[str, Any]) -> PoolKey:
        expected = {"currency0", "currency1", "fee", "tick_spacing", "hooks"}
        if not isinstance(value, Mapping) or set(value) != expected:
            raise V4PoolKeyError(f"pool_key requires exactly {sorted(expected)}")
        key = cls(**dict(value))
        if value["currency0"].lower() != key.currency0:
            raise V4PoolKeyError("Explicit pool_key currencies must be in canonical order")
        return key

    def direction(self, token_in: str, token_out: str) -> bool:
        if (token_in.lower(), token_out.lower()) == (self.currency0, self.currency1):
            return True
        if (token_in.lower(), token_out.lower()) == (self.currency1, self.currency0):
            return False
        raise V4PoolKeyError("Swap assets do not match the selected V4 pool; native and wrapped assets are distinct")
