"""Network-free Data QA contracts for authoritative resource identity.

Provider catalogues are declarations, not evidence that an address measures
the pair written beside it.  This module compares those declarations with an
independently captured authority record and validates the dimensional identity
of direct and composed price routes.

The types are provider-neutral.  Chainlink is the first adapter, but no field
or validator assumes a Chainlink ABI and no validation opens a socket.
"""

from __future__ import annotations

from collections.abc import Collection, Mapping, Sequence
from dataclasses import dataclass
from enum import StrEnum


class PriceRouteKind(StrEnum):
    """How a requested output pair is produced."""

    DIRECT = "direct"
    DERIVED_PRODUCT = "derived_product"


class DataCapability(StrEnum):
    """Exact data capabilities a generated strategy may require."""

    REFERENCE_PRICE = "reference_price"
    MARKET_CALENDAR = "market_calendar"
    BAND_DEPTH = "band_depth"


@dataclass(frozen=True)
class FeedIdentity:
    """Identity of one provider resource, independent of a requested route."""

    provider: str
    chain: str
    address: str
    pair: str
    decimals: int
    kind: str

    def __post_init__(self) -> None:
        provider = self.provider.strip().lower()
        chain = self.chain.strip().lower()
        pair = self.pair.strip().upper()
        kind = self.kind.strip().lower()
        if not provider or not chain:
            raise ValueError("FeedIdentity provider and chain are required")
        if pair.count("/") != 1 or any(not side for side in pair.split("/")):
            raise ValueError(f"FeedIdentity.pair must be BASE/QUOTE, got {self.pair!r}")
        address = self.address.strip().lower()
        if not address.startswith("0x") or len(address) != 42:
            raise ValueError(f"FeedIdentity.address must be a 20-byte hex address, got {self.address!r}")
        try:
            int(address[2:], 16)
        except ValueError as exc:
            raise ValueError(f"FeedIdentity.address is not hexadecimal: {self.address!r}") from exc
        if self.decimals < 0:
            raise ValueError("FeedIdentity.decimals cannot be negative")
        object.__setattr__(self, "provider", provider)
        object.__setattr__(self, "chain", chain)
        object.__setattr__(self, "address", address)
        object.__setattr__(self, "pair", pair)
        object.__setattr__(self, "kind", kind)

    @property
    def resource_id(self) -> str:
        return f"{self.provider}:{self.chain}:{self.address}"


@dataclass(frozen=True)
class AuthoritativeFeed:
    """Externally anchored identity for one provider resource."""

    identity: FeedIdentity
    authority_uri: str

    def __post_init__(self) -> None:
        if not self.authority_uri.startswith("https://"):
            raise ValueError("AuthoritativeFeed.authority_uri must be HTTPS")


@dataclass(frozen=True)
class PriceRoute:
    """A direct or dimensionally composed output-pair route."""

    chain: str
    output_pair: str
    kind: PriceRouteKind
    component_resource_ids: tuple[str, ...]

    def __post_init__(self) -> None:
        object.__setattr__(self, "chain", self.chain.strip().lower())
        object.__setattr__(self, "output_pair", self.output_pair.strip().upper())
        if self.output_pair.count("/") != 1:
            raise ValueError("PriceRoute.output_pair must be BASE/QUOTE")


@dataclass(frozen=True)
class DataRequirement:
    """One exact consumer requirement evaluated against available resources."""

    requirement_id: str
    chain: str
    pair: str
    capabilities: frozenset[DataCapability]

    def __post_init__(self) -> None:
        object.__setattr__(self, "chain", self.chain.strip().lower())
        object.__setattr__(self, "pair", self.pair.strip().upper())
        if not self.requirement_id.strip() or self.pair.count("/") != 1:
            raise ValueError("DataRequirement requires an id and BASE/QUOTE pair")
        if not self.capabilities:
            raise ValueError("DataRequirement.capabilities cannot be empty")


@dataclass(frozen=True)
class DataQAResult:
    """Machine-readable result; empty reason codes means exact satisfaction."""

    passed: bool
    reason_codes: tuple[str, ...]


_IDENTITY_FIELDS = ("provider", "chain", "address", "pair", "decimals", "kind")


def discriminate_feed_identity(declared: FeedIdentity, authority: AuthoritativeFeed) -> DataQAResult:
    """Compare a producer declaration to an independent authority record."""
    expected = authority.identity
    reasons = tuple(
        f"feed_{field}_mismatch" for field in _IDENTITY_FIELDS if getattr(declared, field) != getattr(expected, field)
    )
    return DataQAResult(passed=not reasons, reason_codes=reasons)


def validate_price_route(route: PriceRoute, resources: Mapping[str, FeedIdentity]) -> DataQAResult:
    """Prove route identity, including denomination cancellation for products."""
    reasons: list[str] = []
    components: list[FeedIdentity] = []
    for resource_id in route.component_resource_ids:
        component = resources.get(resource_id)
        if component is None:
            reasons.append("route_component_missing")
        else:
            components.append(component)
            if component.chain != route.chain:
                reasons.append("route_component_chain_mismatch")

    expected_count = 1 if route.kind is PriceRouteKind.DIRECT else 2
    if len(route.component_resource_ids) != expected_count:
        reasons.append("route_component_count_invalid")
    if len(components) == expected_count:
        if route.kind is PriceRouteKind.DIRECT:
            if components[0].pair != route.output_pair:
                reasons.append("direct_route_pair_mismatch")
        else:
            left_base, left_quote = components[0].pair.split("/")
            right_base, right_quote = components[1].pair.split("/")
            if left_quote != right_base:
                reasons.append("derived_route_denominator_mismatch")
            elif f"{left_base}/{right_quote}" != route.output_pair:
                reasons.append("derived_route_output_mismatch")

    unique_reasons = tuple(dict.fromkeys(reasons))
    return DataQAResult(passed=not unique_reasons, reason_codes=unique_reasons)


def evaluate_data_requirement(
    requirement: DataRequirement,
    *,
    routes: Sequence[PriceRoute],
    calendar_pairs: Collection[tuple[str, str]],
    band_depth_pairs: Collection[tuple[str, str]],
) -> DataQAResult:
    """Fail closed when a required pair-level capability is absent."""
    key = (requirement.chain, requirement.pair)
    reasons: list[str] = []
    if DataCapability.REFERENCE_PRICE in requirement.capabilities and not any(
        (route.chain, route.output_pair) == key for route in routes
    ):
        reasons.append("required_price_route_missing")
    if DataCapability.MARKET_CALENDAR in requirement.capabilities and key not in calendar_pairs:
        reasons.append("required_market_calendar_missing")
    if DataCapability.BAND_DEPTH in requirement.capabilities and key not in band_depth_pairs:
        reasons.append("required_band_depth_missing")
    return DataQAResult(passed=not reasons, reason_codes=tuple(reasons))


__all__ = [
    "AuthoritativeFeed",
    "DataCapability",
    "DataQAResult",
    "DataRequirement",
    "FeedIdentity",
    "PriceRoute",
    "PriceRouteKind",
    "discriminate_feed_identity",
    "evaluate_data_requirement",
    "validate_price_route",
]
