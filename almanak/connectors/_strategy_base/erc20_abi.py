"""Strategy-side ERC-20 ABI surface and allowance planning cache.

Pure ABI primitives are implemented in :mod:`almanak.connectors._base.erc20_abi`
because gateway code also needs them.  This module re-exports that surface and
adds the stateful cache used only while strategy-side transaction bundles are
being compiled.

Cache policy
------------
Confirmed values come from an explicit caller seed or a successful on-chain
read.  Planned values are optimistic: they describe an approval transaction
emitted into the current bundle, not chain state.  A planned value may suppress
duplicate approvals only within that planning lifetime and must be cleared at
every execution boundary.  Clearing planned values reveals any older confirmed
value; clearing the cache invalidates both kinds.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from almanak.connectors._base.erc20_abi import (
    ERC20_ALLOWANCE_SELECTOR,
    ERC20_APPROVE_SELECTOR,
    ERC20_BALANCE_OF_SELECTOR,
    ERC20_TRANSFER_TOPIC,
    MAX_UINT256,
    encode_allowance,
    encode_approve,
    encode_balance_of,
    pad_address,
    pad_uint256,
)

AllowanceKey = tuple[str, str, str]


def _canonical_address(address: str) -> str:
    """Lowercase, ``0x``-prefixed address validated by the ABI encoder."""
    return "0x" + pad_address(address)[-40:]


@dataclass
class AllowanceCache:
    """Owner-bound cache that separates confirmed and planned allowances."""

    owner: str
    _confirmed: dict[AllowanceKey, int] = field(default_factory=dict, init=False, repr=False)
    _planned: dict[AllowanceKey, int] = field(default_factory=dict, init=False, repr=False)

    def __post_init__(self) -> None:
        self.owner = _canonical_address(self.owner)

    def _key(self, token: str, spender: str) -> AllowanceKey:
        return (self.owner, _canonical_address(token), _canonical_address(spender))

    def get(self, token: str, spender: str) -> int | None:
        """Return the planned value when present, else the confirmed value."""
        key = self._key(token, spender)
        if key in self._planned:
            return self._planned[key]
        return self._confirmed.get(key)

    def get_planned(self, token: str, spender: str) -> int | None:
        """Return the optimistic value emitted into the current bundle, if any."""
        return self._planned.get(self._key(token, spender))

    def is_sufficient(self, token: str, spender: str, amount: int) -> bool:
        """Whether the best cached value positively covers ``amount``."""
        pad_uint256(amount)
        cached = self.get(token, spender)
        return cached is not None and cached >= amount

    def record_confirmed(self, token: str, spender: str, amount: int) -> None:
        """Record an explicitly seeded or positively read on-chain value."""
        pad_uint256(amount)
        key = self._key(token, spender)
        self._confirmed[key] = amount
        self._planned.pop(key, None)

    def record_planned(self, token: str, spender: str, amount: int) -> None:
        """Record an approval emitted into the current unexecuted bundle."""
        pad_uint256(amount)
        self._planned[self._key(token, spender)] = amount

    def invalidate(self, token: str, spender: str) -> None:
        """Invalidate both confirmed and planned state for one allowance."""
        key = self._key(token, spender)
        self._confirmed.pop(key, None)
        self._planned.pop(key, None)

    def clear_planned(self) -> None:
        """Invalidate every optimistic value at an execution boundary."""
        self._planned.clear()

    def clear(self) -> None:
        """Invalidate all confirmed and planned values."""
        self._confirmed.clear()
        self._planned.clear()

    def __bool__(self) -> bool:
        return bool(self._confirmed or self._planned)


__all__ = [
    "AllowanceCache",
    "ERC20_ALLOWANCE_SELECTOR",
    "ERC20_APPROVE_SELECTOR",
    "ERC20_BALANCE_OF_SELECTOR",
    "ERC20_TRANSFER_TOPIC",
    "MAX_UINT256",
    "encode_allowance",
    "encode_approve",
    "encode_balance_of",
    "pad_address",
    "pad_uint256",
]
