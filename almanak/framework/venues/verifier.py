"""Gateway-mediated connector verifier interface for exact venues."""

from __future__ import annotations

import re
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Protocol

from almanak.core.asset_identity import AssetIdentity
from almanak.core.chains import ChainRegistry
from almanak.framework.primitives.types import Primitive

from .types import VenueBindingComponent, VenueTargetRef, VenueTargetRole, VenueVerificationResult

_IDENTITY_ROLES = frozenset(
    {VenueTargetRole.POOL, VenueTargetRole.MARKET, VenueTargetRole.VAULT, VenueTargetRole.PROGRAM}
)


class VenueVerificationGateway(Protocol):
    """Minimum gateway-routed read seam available to connector verifiers."""

    def read(
        self,
        *,
        chain: str,
        target: VenueTargetRef,
        payload: bytes,
        block_number: int | None = None,
    ) -> bytes:
        """Execute one chain-specific read through the gateway gRPC client."""

    def block_hash(self, *, chain: str, block_number: int) -> str:
        """Return the canonical hash for a gateway-observed block."""


@dataclass(frozen=True, slots=True)
class VenueVerificationRequest:
    """Immutable expected venue facts passed to one exact protocol verifier."""

    chain: str
    protocol: str
    primitive: Primitive
    requested_refs: tuple[VenueTargetRef, ...]
    ordered_assets: tuple[AssetIdentity, ...]
    binding_components: tuple[VenueBindingComponent, ...]
    binding_policy_version: int

    def __post_init__(self) -> None:
        if type(self.chain) is not str:
            raise TypeError("VenueVerificationRequest.chain must be a string")
        descriptor = ChainRegistry.resolve(self.chain)
        if self.chain != descriptor.name:
            raise ValueError("VenueVerificationRequest.chain must use its canonical chain name")
        if type(self.protocol) is not str or not re.fullmatch(r"[a-z][a-z0-9_]*", self.protocol):
            raise ValueError("VenueVerificationRequest.protocol must be a canonical protocol key")
        if type(self.primitive) is not Primitive:
            raise TypeError("VenueVerificationRequest.primitive must be an exact Primitive")
        _validate_ordered_tuple(self.requested_refs, VenueTargetRef, "requested_refs", allow_empty=False)
        if any(reference.role not in _IDENTITY_ROLES for reference in self.requested_refs):
            raise ValueError("VenueVerificationRequest.requested_refs may contain only physical venue roles")
        for reference in self.requested_refs:
            reference.validate_chain(self.chain)
        _validate_ordered_tuple(self.binding_components, VenueBindingComponent, "binding_components", allow_empty=True)
        if len({component.name for component in self.binding_components}) != len(self.binding_components):
            raise ValueError("VenueVerificationRequest.binding_components contains duplicate names")
        if type(self.ordered_assets) is not tuple or not self.ordered_assets:
            raise ValueError("VenueVerificationRequest.ordered_assets must be a non-empty tuple")
        if any(type(asset) is not AssetIdentity for asset in self.ordered_assets):
            raise TypeError("VenueVerificationRequest.ordered_assets must contain exact AssetIdentity values")
        if any(asset.chain != self.chain for asset in self.ordered_assets):
            raise ValueError("VenueVerificationRequest.ordered_assets contains a cross-chain asset")
        if len(set(self.ordered_assets)) != len(self.ordered_assets):
            raise ValueError("VenueVerificationRequest.ordered_assets contains duplicate assets")
        if type(self.binding_policy_version) is not int or self.binding_policy_version <= 0:
            raise ValueError("VenueVerificationRequest.binding_policy_version must be a positive integer")


def _validate_ordered_tuple(value: object, item_type: type, field_name: str, *, allow_empty: bool) -> None:
    if type(value) is not tuple:
        raise TypeError(f"VenueVerificationRequest.{field_name} must be a tuple")
    if not allow_empty and not value:
        raise ValueError(f"VenueVerificationRequest.{field_name} must be non-empty")
    if any(type(item) is not item_type for item in value):
        raise TypeError(f"VenueVerificationRequest.{field_name} contains an invalid value")
    expected = tuple(sorted(value, key=lambda item: item.sort_key))
    if value != expected:
        raise ValueError(f"VenueVerificationRequest.{field_name} must be canonically ordered")
    if len(set(value)) != len(value):
        raise ValueError(f"VenueVerificationRequest.{field_name} contains duplicate values")


class BaseVenueVerifier(ABC):
    """Connector-owned verifier that may read chain state only via ``gateway``."""

    @abstractmethod
    def verify_venue(
        self,
        request: VenueVerificationRequest,
        gateway: VenueVerificationGateway,
        *,
        block_number: int | None = None,
    ) -> VenueVerificationResult:
        """Return verified success or a closed failure without fallback."""


__all__ = ["BaseVenueVerifier", "VenueVerificationGateway", "VenueVerificationRequest"]
