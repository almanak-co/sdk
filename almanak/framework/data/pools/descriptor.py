"""Canonical immutable identity for a resolved liquidity pool."""

from __future__ import annotations

import re
from dataclasses import dataclass
from decimal import Decimal

from almanak.framework.intents.lp_fees import fee_rate_from_units

_EVM_ADDRESS_RE = re.compile(r"^0x[a-f0-9]{40}$")


def _is_evm_address(value: str) -> bool:
    """Validate normalized EVM address shape without adding a data-layer dependency."""
    return _EVM_ADDRESS_RE.fullmatch(value) is not None


@dataclass(frozen=True, slots=True)
class PoolDescriptor:
    """Address-bound pool identity shared across execution and data planes.

    Fee identity stays in raw factory units when the venue exposes an immutable
    factory discriminator.  Fungible/dynamic-fee venues may leave it
    unmeasured; economic code must then use its existing venue-specific fee
    fallback rather than fabricating a V3 tier.
    """

    chain: str
    protocol: str
    address: str
    token0: str
    token1: str
    token0_decimals: int
    token1_decimals: int
    fee_tier_units: int | None
    provenance: str
    factory: str | None = None

    def __post_init__(self) -> None:
        required_text = (self.chain, self.protocol, self.address, self.token0, self.token1, self.provenance)
        if not all(isinstance(value, str) for value in required_text):
            raise ValueError("pool descriptor identity and provenance fields must be strings")
        if self.factory is not None and not isinstance(self.factory, str):
            raise ValueError("pool descriptor factory must be a string address when provided")
        chain = self.chain.strip().lower()
        protocol = self.protocol.strip().lower().replace("-", "_")
        address = self.address.strip().lower()
        token0 = self.token0.strip().lower()
        token1 = self.token1.strip().lower()
        provenance = self.provenance.strip()
        factory = self.factory.strip().lower() if self.factory is not None else None
        if not chain or not protocol or not provenance:
            raise ValueError("pool descriptor requires chain, protocol, and provenance")
        if not all(_is_evm_address(value) for value in (address, token0, token1)) or token0 == token1:
            raise ValueError("pool descriptor requires a valid pool address and two distinct token addresses")
        if factory is not None and not _is_evm_address(factory):
            raise ValueError("pool descriptor factory must be a valid address when provided")
        if not 0 <= self.token0_decimals <= 36 or not 0 <= self.token1_decimals <= 36:
            raise ValueError("pool descriptor token decimals must be in the interval [0, 36]")
        if self.fee_tier_units is not None:
            fee_rate_from_units(self.fee_tier_units)
        object.__setattr__(self, "chain", chain)
        object.__setattr__(self, "protocol", protocol)
        object.__setattr__(self, "address", address)
        object.__setattr__(self, "token0", token0)
        object.__setattr__(self, "token1", token1)
        object.__setattr__(self, "provenance", provenance)
        object.__setattr__(self, "factory", factory)

    @property
    def fee_rate(self) -> Decimal | None:
        """Return the normalized immutable fee, or ``None`` when unmeasured."""
        return fee_rate_from_units(self.fee_tier_units) if self.fee_tier_units is not None else None

    @property
    def key(self) -> tuple[str, str, str]:
        """Return the full execution identity; addresses alone are not globally unique."""
        return self.chain, self.protocol, self.address


__all__ = ["PoolDescriptor"]
