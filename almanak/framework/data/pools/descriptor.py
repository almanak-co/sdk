"""Canonical immutable identity for a resolved liquidity pool."""

from __future__ import annotations

import re
from dataclasses import dataclass
from decimal import Decimal
from typing import Any

from almanak.core.constants import canonical_chain_name
from almanak.framework.intents.lp_fees import fee_rate_from_units

_EVM_ADDRESS_RE = re.compile(r"^0x[a-f0-9]{40}$")


def _is_evm_address(value: str) -> bool:
    """Validate normalized EVM address shape without adding a data-layer dependency."""
    return _EVM_ADDRESS_RE.fullmatch(value) is not None


_DISCRIMINATOR_KINDS = frozenset({"fee_tier", "tick_spacing", "stable_flag", "none"})


def _require_text_types(descriptor: ResolvedPoolDescriptor) -> None:
    """Reject non-text identity fields before applying string normalization."""
    required_text = (
        descriptor.chain,
        descriptor.protocol,
        descriptor.address,
        descriptor.token0,
        descriptor.token1,
        descriptor.provenance,
    )
    if not all(isinstance(value, str) for value in required_text):
        raise ValueError("pool descriptor identity and provenance fields must be strings")
    if descriptor.factory is not None and not isinstance(descriptor.factory, str):
        raise ValueError("pool descriptor factory must be a string address when provided")
    if descriptor.discriminator_kind is not None and not isinstance(descriptor.discriminator_kind, str):
        raise ValueError("pool descriptor discriminator_kind must be a string when provided")


def _normalize_text_fields(
    descriptor: ResolvedPoolDescriptor,
) -> tuple[str, str, str, str, str, str, str | None]:
    """Return the canonical text portion of a descriptor identity."""
    return (
        canonical_chain_name(descriptor.chain.strip()).strip().lower(),
        descriptor.protocol.strip().lower().replace("-", "_"),
        descriptor.address.strip().lower(),
        descriptor.token0.strip().lower(),
        descriptor.token1.strip().lower(),
        descriptor.provenance.strip(),
        descriptor.factory.strip().lower() if descriptor.factory is not None else None,
    )


def _normalize_discriminator(
    discriminator_kind: str | None,
    discriminator: int | bool | None,
    fee_tier_units: int | None,
) -> tuple[str, int | bool | None]:
    """Apply persisted-descriptor defaults to the factory discriminator."""
    if discriminator_kind is None:
        kind = "fee_tier" if fee_tier_units is not None else "none"
    else:
        kind = discriminator_kind.strip().lower()
    if discriminator is None and kind == "fee_tier":
        discriminator = fee_tier_units
    return kind, discriminator


def _validate_required_identity(chain: str, protocol: str, provenance: str) -> None:
    """Require the non-address identity fields after normalization."""
    if not chain or not protocol or not provenance:
        raise ValueError("pool descriptor requires chain, protocol, and provenance")


def _validate_pool_addresses(address: str, token0: str, token1: str) -> None:
    """Require one pool and two distinct normalized EVM token addresses."""
    if not all(_is_evm_address(value) for value in (address, token0, token1)):
        raise ValueError("pool descriptor requires a valid pool address and two distinct token addresses")
    if token0 == token1:
        raise ValueError("pool descriptor requires a valid pool address and two distinct token addresses")


def _validate_factory(factory: str | None) -> None:
    """Validate a connector factory address when one is pinned."""
    if factory is not None and not _is_evm_address(factory):
        raise ValueError("pool descriptor factory must be a valid address when provided")


def _validate_token_decimals(token0_decimals: int, token1_decimals: int) -> None:
    """Keep token decimal precision within the EVM contract boundary."""
    if type(token0_decimals) is not int or not 0 <= token0_decimals <= 36:
        raise ValueError("pool descriptor token decimals must be in the interval [0, 36]")
    if type(token1_decimals) is not int or not 0 <= token1_decimals <= 36:
        raise ValueError("pool descriptor token decimals must be in the interval [0, 36]")


def _validate_none_discriminator(discriminator: int | bool | None) -> None:
    """Require a keyless descriptor to omit its discriminator value."""
    if discriminator is not None:
        raise ValueError("pool descriptor discriminator must be None when discriminator_kind='none'")


def _validate_stable_discriminator(discriminator: int | bool | None) -> None:
    """Require stable-pool identity to retain its boolean ABI type."""
    if type(discriminator) is not bool:
        raise ValueError("pool descriptor stable_flag discriminator must be bool")


def _validate_integer_discriminator(discriminator_kind: str, discriminator: int | bool | None) -> None:
    """Require a positive, non-boolean integer pool key."""
    if type(discriminator) is not int or discriminator <= 0:
        raise ValueError(f"pool descriptor {discriminator_kind} discriminator must be a positive integer")


def _validate_discriminator(
    discriminator_kind: str,
    discriminator: int | bool | None,
    fee_tier_units: int | None,
) -> None:
    """Validate the discriminator type and its relationship to fee identity."""
    if discriminator_kind not in _DISCRIMINATOR_KINDS:
        raise ValueError(f"pool descriptor discriminator_kind must be one of {sorted(_DISCRIMINATOR_KINDS)!r}")
    if discriminator_kind == "none":
        _validate_none_discriminator(discriminator)
    elif discriminator_kind == "stable_flag":
        _validate_stable_discriminator(discriminator)
    else:
        _validate_integer_discriminator(discriminator_kind, discriminator)
    if discriminator_kind == "fee_tier" and discriminator != fee_tier_units:
        raise ValueError("pool descriptor fee_tier discriminator must equal fee_tier_units")


def _validate_deployment_block(deployment_block: int | None) -> None:
    """Validate the optional first-code block used for historical admission."""
    if deployment_block is not None and (type(deployment_block) is not int or deployment_block < 0):
        raise ValueError("pool descriptor deployment_block must be a non-negative integer when provided")


@dataclass(frozen=True, slots=True)
class ResolvedPoolDescriptor:
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
    discriminator_kind: str | None = None
    discriminator: int | bool | None = None
    deployment_block: int | None = None

    def __post_init__(self) -> None:
        _require_text_types(self)
        chain, protocol, address, token0, token1, provenance, factory = _normalize_text_fields(self)
        discriminator_kind, discriminator = _normalize_discriminator(
            self.discriminator_kind,
            self.discriminator,
            self.fee_tier_units,
        )
        _validate_required_identity(chain, protocol, provenance)
        _validate_pool_addresses(address, token0, token1)
        _validate_factory(factory)
        _validate_token_decimals(self.token0_decimals, self.token1_decimals)
        if self.fee_tier_units is not None:
            fee_rate_from_units(self.fee_tier_units)
        _validate_discriminator(discriminator_kind, discriminator, self.fee_tier_units)
        _validate_deployment_block(self.deployment_block)
        object.__setattr__(self, "chain", chain)
        object.__setattr__(self, "protocol", protocol)
        object.__setattr__(self, "address", address)
        object.__setattr__(self, "token0", token0)
        object.__setattr__(self, "token1", token1)
        object.__setattr__(self, "provenance", provenance)
        object.__setattr__(self, "factory", factory)
        object.__setattr__(self, "discriminator_kind", discriminator_kind)
        object.__setattr__(self, "discriminator", discriminator)

    @property
    def fee_rate(self) -> Decimal | None:
        """Return the normalized immutable fee, or ``None`` when unmeasured."""
        return fee_rate_from_units(self.fee_tier_units) if self.fee_tier_units is not None else None

    @property
    def key(self) -> tuple[str, str, str]:
        """Return the full execution identity; addresses alone are not globally unique."""
        return self.chain, self.protocol, self.address

    @property
    def manifest_key(self) -> str:
        """Stable string identity used by config/result manifests."""
        return ":".join(self.key)

    def to_dict(self) -> dict[str, Any]:
        """Return the canonical JSON-safe representation pinned into a run."""
        return {
            "chain": self.chain,
            "protocol": self.protocol,
            "address": self.address,
            "token0": self.token0,
            "token1": self.token1,
            "discriminator_kind": self.discriminator_kind,
            "discriminator": self.discriminator,
            "token0_decimals": self.token0_decimals,
            "token1_decimals": self.token1_decimals,
            "fee_tier_units": self.fee_tier_units,
            "factory": self.factory,
            "deployment_block": self.deployment_block,
            "provenance": self.provenance,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> ResolvedPoolDescriptor:
        """Rehydrate a pinned descriptor, including legacy fee-only values."""
        return cls(
            chain=data["chain"],
            protocol=data["protocol"],
            address=data["address"],
            token0=data["token0"],
            token1=data["token1"],
            token0_decimals=int(data["token0_decimals"]),
            token1_decimals=int(data["token1_decimals"]),
            fee_tier_units=(int(data["fee_tier_units"]) if data.get("fee_tier_units") is not None else None),
            provenance=data.get("provenance", "pinned:legacy"),
            factory=data.get("factory"),
            discriminator_kind=data.get("discriminator_kind"),
            discriminator=data.get("discriminator"),
            deployment_block=(int(data["deployment_block"]) if data.get("deployment_block") is not None else None),
        )


# Compatibility name retained for the execution/data-plane code that adopted
# this value before it became part of the public persisted backtest contract.
PoolDescriptor = ResolvedPoolDescriptor


__all__ = ["PoolDescriptor", "ResolvedPoolDescriptor"]
