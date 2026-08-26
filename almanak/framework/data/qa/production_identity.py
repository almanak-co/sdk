"""QA-only contracts projected from production resource registries.

This module deliberately owns no resource catalogue.  Requirements are derived
from the token, Chainlink, and connector-owned pool registries that production
already consumes.  Observations are inert value objects suitable for a later
gateway-backed collector; evaluation itself is deterministic and network-free.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from typing import Protocol

from almanak.core.chains import ChainRegistry
from almanak.core.enums import ChainFamily
from almanak.framework.data.tokens.defaults import NATIVE_SENTINEL
from almanak.framework.data.tokens.models import Token, normalize_token_address_for_chain
from almanak.integrations.chainlink.models import FeedSpec


class ResourceKind(StrEnum):
    """Resource families covered by the first production-derived QA slice."""

    TOKEN = "token"
    DIRECT_CHAINLINK_FEED = "direct_chainlink_feed"
    V3_POOL = "v3_pool"


class IdentityVerdict(StrEnum):
    """Identity verdict independent of availability and freshness."""

    PASS = "PASS"
    FAIL = "FAIL"
    UNMEASURED = "UNMEASURED"


class IdentityRequirement(Protocol):
    """Common structural contract for projected requirements."""

    requirement_id: str
    kind: ResourceKind

    def to_canonical_dict(self) -> dict[str, object]: ...


def _canonical_address(chain: str, address: str) -> str:
    return normalize_token_address_for_chain(address, chain.strip().lower())


def _resource_id(kind: ResourceKind, *parts: object) -> str:
    return ":".join((kind.value, *(str(part).strip().lower() for part in parts)))


@dataclass(frozen=True)
class TokenRequirement:
    """Exact chain/address identity and amount metadata from the token registry."""

    chain: str
    address: str
    decimals: int
    symbols: tuple[str, ...] = ()
    kind: ResourceKind = ResourceKind.TOKEN

    def __post_init__(self) -> None:
        chain = self.chain.strip().lower()
        if not chain or self.decimals < 0:
            raise ValueError("TokenRequirement requires a chain and non-negative decimals")
        object.__setattr__(self, "chain", chain)
        object.__setattr__(self, "address", _canonical_address(chain, self.address))
        object.__setattr__(
            self, "symbols", tuple(sorted({symbol.strip().upper() for symbol in self.symbols if symbol}))
        )

    @property
    def requirement_id(self) -> str:
        return _resource_id(self.kind, self.chain, self.address)

    def to_canonical_dict(self) -> dict[str, object]:
        return {
            "kind": self.kind.value,
            "requirement_id": self.requirement_id,
            "chain": self.chain,
            "address": self.address,
            "decimals": self.decimals,
            "symbols": list(self.symbols),
        }


@dataclass(frozen=True)
class DirectChainlinkFeedRequirement:
    """One direct feed declaration projected from ``ChainlinkCatalog``."""

    chain: str
    address: str
    pair: str
    decimals: int
    feed_kind: str
    provider: str = "chainlink"
    kind: ResourceKind = ResourceKind.DIRECT_CHAINLINK_FEED

    def __post_init__(self) -> None:
        chain = self.chain.strip().lower()
        pair = self.pair.strip().upper()
        if not chain or pair.count("/") != 1 or self.decimals < 0:
            raise ValueError("DirectChainlinkFeedRequirement requires chain, BASE/QUOTE, and non-negative decimals")
        object.__setattr__(self, "chain", chain)
        object.__setattr__(self, "address", _canonical_address(chain, self.address))
        object.__setattr__(self, "pair", pair)
        object.__setattr__(self, "feed_kind", self.feed_kind.strip().lower())
        object.__setattr__(self, "provider", self.provider.strip().lower())

    @property
    def requirement_id(self) -> str:
        # A provider may intentionally publish multiple logical pair routes
        # through one proxy (for example native/wrapped aliases).  Pair is part
        # of the requirement identity so those declarations do not overwrite
        # one another in the derived inventory.
        return _resource_id(self.kind, self.provider, self.chain, self.address, self.pair)

    def to_canonical_dict(self) -> dict[str, object]:
        return {
            "kind": self.kind.value,
            "requirement_id": self.requirement_id,
            "provider": self.provider,
            "chain": self.chain,
            "address": self.address,
            "pair": self.pair,
            "decimals": self.decimals,
            "feed_kind": self.feed_kind,
        }


@dataclass(frozen=True)
class V3PoolRequirement:
    """Exact binding of a connector-owned known V3 pool."""

    protocol: str
    chain: str
    address: str
    token_pair: tuple[str, str]
    fee_tier: int
    kind: ResourceKind = ResourceKind.V3_POOL

    def __post_init__(self) -> None:
        protocol = self.protocol.strip().lower()
        chain = self.chain.strip().lower()
        if not protocol or not chain or self.fee_tier <= 0 or len(self.token_pair) != 2:
            raise ValueError("V3PoolRequirement requires protocol, chain, token pair, and positive fee tier")
        # Preserve token0/token1 order exactly.  Address sorting would erase the
        # class of production drift this contract exists to discriminate.
        pair = tuple(_canonical_address(chain, address) for address in self.token_pair)
        if pair[0] == pair[1]:
            raise ValueError("V3PoolRequirement token pair must contain two distinct tokens")
        object.__setattr__(self, "protocol", protocol)
        object.__setattr__(self, "chain", chain)
        object.__setattr__(self, "address", _canonical_address(chain, self.address))
        object.__setattr__(self, "token_pair", pair)

    @property
    def requirement_id(self) -> str:
        return _resource_id(self.kind, self.protocol, self.chain, self.address)

    def to_canonical_dict(self) -> dict[str, object]:
        return {
            "kind": self.kind.value,
            "requirement_id": self.requirement_id,
            "protocol": self.protocol,
            "chain": self.chain,
            "address": self.address,
            "token_pair": list(self.token_pair),
            "fee_tier": self.fee_tier,
        }


type Requirement = TokenRequirement | DirectChainlinkFeedRequirement | V3PoolRequirement


@dataclass(frozen=True)
class ObservationProvenance:
    """Independent capture anchor; a URL or label alone is not evidence."""

    collector: str
    captured_at: datetime
    block_number: int
    block_hash: str
    artifact_sha256: str

    def __post_init__(self) -> None:
        collector = self.collector.strip().lower()
        if collector != "gateway_rpc":
            raise ValueError("ObservationProvenance.collector must be gateway_rpc")
        if self.captured_at.tzinfo is None or self.captured_at.utcoffset() is None:
            raise ValueError("ObservationProvenance.captured_at must be timezone-aware")
        if self.block_number < 0:
            raise ValueError("ObservationProvenance.block_number cannot be negative")
        block_hash = self.block_hash.strip().lower()
        if re.fullmatch(r"0x[0-9a-f]{64}", block_hash) is None:
            raise ValueError("ObservationProvenance.block_hash must be a 32-byte hex value")
        artifact_sha256 = self.artifact_sha256.strip().lower()
        if re.fullmatch(r"[0-9a-f]{64}", artifact_sha256) is None:
            raise ValueError("ObservationProvenance.artifact_sha256 must be a SHA-256 hex digest")
        object.__setattr__(self, "collector", collector)
        object.__setattr__(self, "captured_at", self.captured_at.astimezone(UTC))
        object.__setattr__(self, "block_hash", block_hash)
        object.__setattr__(self, "artifact_sha256", artifact_sha256)

    def to_canonical_dict(self) -> dict[str, object]:
        return {
            "collector": self.collector,
            "captured_at": self.captured_at.isoformat().replace("+00:00", "Z"),
            "block_number": self.block_number,
            "block_hash": self.block_hash,
            "artifact_sha256": self.artifact_sha256,
        }


@dataclass(frozen=True)
class TokenObservation:
    requirement_id: str
    chain: str
    address: str
    decimals: int
    provenance: ObservationProvenance
    kind: ResourceKind = ResourceKind.TOKEN

    def __post_init__(self) -> None:
        chain = self.chain.strip().lower()
        object.__setattr__(self, "chain", chain)
        object.__setattr__(self, "address", _canonical_address(chain, self.address))

    def to_canonical_dict(self) -> dict[str, object]:
        return {
            "kind": self.kind.value,
            "requirement_id": self.requirement_id,
            "chain": self.chain,
            "address": self.address,
            "decimals": self.decimals,
            "provenance": self.provenance.to_canonical_dict(),
        }


@dataclass(frozen=True)
class DirectChainlinkFeedObservation:
    requirement_id: str
    chain: str
    address: str
    pair: str
    decimals: int
    feed_kind: str
    provenance: ObservationProvenance
    provider: str = "chainlink"
    kind: ResourceKind = ResourceKind.DIRECT_CHAINLINK_FEED

    def __post_init__(self) -> None:
        chain = self.chain.strip().lower()
        object.__setattr__(self, "chain", chain)
        object.__setattr__(self, "address", _canonical_address(chain, self.address))
        object.__setattr__(self, "pair", self.pair.strip().upper())
        object.__setattr__(self, "feed_kind", self.feed_kind.strip().lower())
        object.__setattr__(self, "provider", self.provider.strip().lower())

    def to_canonical_dict(self) -> dict[str, object]:
        return {
            "kind": self.kind.value,
            "requirement_id": self.requirement_id,
            "provider": self.provider,
            "chain": self.chain,
            "address": self.address,
            "pair": self.pair,
            "decimals": self.decimals,
            "feed_kind": self.feed_kind,
            "provenance": self.provenance.to_canonical_dict(),
        }


@dataclass(frozen=True)
class V3PoolObservation:
    requirement_id: str
    protocol: str
    chain: str
    address: str
    token_pair: tuple[str, str]
    fee_tier: int
    provenance: ObservationProvenance
    kind: ResourceKind = ResourceKind.V3_POOL

    def __post_init__(self) -> None:
        chain = self.chain.strip().lower()
        object.__setattr__(self, "protocol", self.protocol.strip().lower())
        object.__setattr__(self, "chain", chain)
        object.__setattr__(self, "address", _canonical_address(chain, self.address))
        object.__setattr__(self, "token_pair", tuple(_canonical_address(chain, address) for address in self.token_pair))

    def to_canonical_dict(self) -> dict[str, object]:
        return {
            "kind": self.kind.value,
            "requirement_id": self.requirement_id,
            "protocol": self.protocol,
            "chain": self.chain,
            "address": self.address,
            "token_pair": list(self.token_pair),
            "fee_tier": self.fee_tier,
            "provenance": self.provenance.to_canonical_dict(),
        }


type Observation = TokenObservation | DirectChainlinkFeedObservation | V3PoolObservation


@dataclass(frozen=True)
class IdentityEvaluation:
    requirement_id: str
    verdict: IdentityVerdict
    reason_codes: tuple[str, ...] = ()


@dataclass(frozen=True)
class ObservationSetEvaluation:
    results: tuple[IdentityEvaluation, ...]
    unexpected_observation_ids: tuple[str, ...] = ()

    @property
    def passed(self) -> bool:
        return not self.unexpected_observation_ids and all(
            result.verdict is IdentityVerdict.PASS for result in self.results
        )


def derive_token_requirements(tokens: Iterable[Token] | None = None) -> tuple[TokenRequirement, ...]:
    """Project chain-scoped identities from the production token registry."""
    if tokens is None:
        from almanak.framework.data.tokens.defaults import DEFAULT_TOKENS

        tokens = DEFAULT_TOKENS
    projected: dict[str, TokenRequirement] = {}
    for token in tokens:
        for chain in token.chains:
            address = token.get_address(chain)
            if address is None:
                continue
            descriptor = ChainRegistry.try_resolve(chain)
            if descriptor is None or descriptor.family is not ChainFamily.EVM:
                continue
            if address.lower() == NATIVE_SENTINEL.lower():
                # Native assets have no ERC-20 decimals() contract. They need
                # their own chain-metadata proof family, not a fabricated token
                # observation that happens to share the registry shape.
                continue
            candidate = TokenRequirement(
                chain=chain,
                address=address,
                decimals=token.get_decimals(chain),
                symbols=(token.symbol,),
            )
            existing = projected.get(candidate.requirement_id)
            if existing is not None:
                if existing.decimals != candidate.decimals:
                    raise ValueError(f"Conflicting token decimals for {candidate.requirement_id}")
                candidate = TokenRequirement(
                    chain=chain,
                    address=address,
                    decimals=candidate.decimals,
                    symbols=(*existing.symbols, token.symbol),
                )
            projected[candidate.requirement_id] = candidate
    return tuple(projected[key] for key in sorted(projected))


def derive_chainlink_requirements(catalog: object | None = None) -> tuple[DirectChainlinkFeedRequirement, ...]:
    """Project every direct resource from the public Chainlink catalogue API."""
    if catalog is None:
        from almanak.integrations.chainlink.catalog import CATALOG

        catalog = CATALOG
    projected: list[DirectChainlinkFeedRequirement] = []
    for chain in catalog.chains:  # type: ignore[attr-defined]
        for spec in catalog.feeds(chain).values():  # type: ignore[attr-defined]
            if not isinstance(spec, FeedSpec):
                raise TypeError("Chainlink catalog feeds() must contain FeedSpec values")
            projected.append(
                DirectChainlinkFeedRequirement(
                    chain=spec.chain,
                    address=spec.address,
                    pair=spec.pair,
                    decimals=spec.decimals,
                    feed_kind=spec.kind.value,
                )
            )
    return _unique_requirements(projected)


def derive_v3_pool_requirements(pool_specs: Iterable[object] | None = None) -> tuple[V3PoolRequirement, ...]:
    """Project known fee-tier V3 pools from connector-owned reader specs."""
    from almanak.connectors._strategy_base.pool_reader import PoolDiscriminatorKind, PoolReaderSpec

    if pool_specs is None:
        from almanak.connectors._strategy_pool_reader_registry import POOL_READER_REGISTRY

        pool_specs = POOL_READER_REGISTRY.all()
    projected: list[V3PoolRequirement] = []
    for raw_spec in pool_specs:
        if not isinstance(raw_spec, PoolReaderSpec):
            raise TypeError("Pool registry must contain PoolReaderSpec values")
        if raw_spec.discriminator_kind is not PoolDiscriminatorKind.FEE_TIER:
            continue
        for chain, pools in raw_spec.known_pools.items():
            for (token0, token1, fee_tier), address in pools.items():
                projected.append(
                    V3PoolRequirement(
                        protocol=raw_spec.protocol,
                        chain=chain,
                        address=address,
                        token_pair=(token0, token1),
                        fee_tier=fee_tier,
                    )
                )
    return _unique_requirements(projected)


def derive_production_requirements() -> tuple[Requirement, ...]:
    """Build the complete first-slice inventory without owning parallel data."""
    return _unique_requirements(
        (*derive_token_requirements(), *derive_chainlink_requirements(), *derive_v3_pool_requirements())
    )


def _unique_requirements(requirements: Iterable[Requirement]) -> tuple[Requirement, ...]:
    indexed: dict[str, Requirement] = {}
    for requirement in requirements:
        existing = indexed.get(requirement.requirement_id)
        if existing is not None and existing != requirement:
            raise ValueError(f"Conflicting requirements share id {requirement.requirement_id}")
        indexed[requirement.requirement_id] = requirement
    return tuple(indexed[key] for key in sorted(indexed))


def requirements_digest(requirements: Iterable[Requirement]) -> str:
    """Hash a canonical, order-independent projection of exact requirements."""
    unique = _unique_requirements(requirements)
    payload = {
        "schema_version": 1,
        "requirements": [requirement.to_canonical_dict() for requirement in unique],
    }
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode()
    return hashlib.sha256(encoded).hexdigest()


def requirement_from_dict(raw: dict[str, object]) -> Requirement:
    """Parse a requirement bundle row into its exact typed contract."""
    kind = ResourceKind(str(raw.get("kind") or ""))
    if kind is ResourceKind.TOKEN:
        return TokenRequirement(
            chain=str(raw["chain"]),
            address=str(raw["address"]),
            decimals=int(raw["decimals"]),
            symbols=tuple(str(value) for value in raw.get("symbols", [])),  # type: ignore[arg-type]
        )
    if kind is ResourceKind.DIRECT_CHAINLINK_FEED:
        return DirectChainlinkFeedRequirement(
            provider=str(raw["provider"]),
            chain=str(raw["chain"]),
            address=str(raw["address"]),
            pair=str(raw["pair"]),
            decimals=int(raw["decimals"]),
            feed_kind=str(raw["feed_kind"]),
        )
    pair = raw["token_pair"]
    if not isinstance(pair, list) or len(pair) != 2:
        raise ValueError("V3 pool requirement token_pair must contain two addresses")
    return V3PoolRequirement(
        protocol=str(raw["protocol"]),
        chain=str(raw["chain"]),
        address=str(raw["address"]),
        token_pair=(str(pair[0]), str(pair[1])),
        fee_tier=int(raw["fee_tier"]),
    )


def observation_from_dict(raw: dict[str, object]) -> Observation:
    """Parse one independently captured observation bundle row."""
    provenance_raw = raw.get("provenance")
    if not isinstance(provenance_raw, dict):
        raise ValueError("Observation requires provenance")
    captured_at = datetime.fromisoformat(str(provenance_raw["captured_at"]).replace("Z", "+00:00"))
    provenance = ObservationProvenance(
        collector=str(provenance_raw["collector"]),
        captured_at=captured_at,
        block_number=int(provenance_raw["block_number"]),
        block_hash=str(provenance_raw["block_hash"]),
        artifact_sha256=str(provenance_raw["artifact_sha256"]),
    )
    kind = ResourceKind(str(raw.get("kind") or ""))
    common = {
        "requirement_id": str(raw["requirement_id"]),
        "chain": str(raw["chain"]),
        "address": str(raw["address"]),
        "provenance": provenance,
    }
    if kind is ResourceKind.TOKEN:
        return TokenObservation(decimals=int(raw["decimals"]), **common)
    if kind is ResourceKind.DIRECT_CHAINLINK_FEED:
        return DirectChainlinkFeedObservation(
            provider=str(raw["provider"]),
            pair=str(raw["pair"]),
            decimals=int(raw["decimals"]),
            feed_kind=str(raw["feed_kind"]),
            **common,
        )
    pair = raw["token_pair"]
    if not isinstance(pair, list) or len(pair) != 2:
        raise ValueError("V3 pool observation token_pair must contain two addresses")
    return V3PoolObservation(
        protocol=str(raw["protocol"]),
        token_pair=(str(pair[0]), str(pair[1])),
        fee_tier=int(raw["fee_tier"]),
        **common,
    )


def evaluate_identity(requirement: Requirement, observation: Observation | None) -> IdentityEvaluation:
    """Compare one independent observation with one projected declaration."""
    if observation is None:
        return IdentityEvaluation(requirement.requirement_id, IdentityVerdict.UNMEASURED, ("observation_missing",))
    reasons: list[str] = []
    if observation.requirement_id != requirement.requirement_id:
        reasons.append("requirement_id_mismatch")
    if observation.kind is not requirement.kind:
        reasons.append("resource_kind_mismatch")
    elif isinstance(requirement, TokenRequirement) and isinstance(observation, TokenObservation):
        _compare(reasons, "token_chain_mismatch", requirement.chain, observation.chain)
        _compare(reasons, "token_address_mismatch", requirement.address, observation.address)
        _compare(reasons, "token_decimals_mismatch", requirement.decimals, observation.decimals)
    elif isinstance(requirement, DirectChainlinkFeedRequirement) and isinstance(
        observation, DirectChainlinkFeedObservation
    ):
        for reason, expected, observed in (
            ("feed_provider_mismatch", requirement.provider, observation.provider),
            ("feed_chain_mismatch", requirement.chain, observation.chain),
            ("feed_address_mismatch", requirement.address, observation.address),
            ("feed_pair_mismatch", requirement.pair, observation.pair),
            ("feed_decimals_mismatch", requirement.decimals, observation.decimals),
            ("feed_kind_mismatch", requirement.feed_kind, observation.feed_kind),
        ):
            _compare(reasons, reason, expected, observed)
    elif isinstance(requirement, V3PoolRequirement) and isinstance(observation, V3PoolObservation):
        for reason, expected, observed in (
            ("pool_protocol_mismatch", requirement.protocol, observation.protocol),
            ("pool_chain_mismatch", requirement.chain, observation.chain),
            ("pool_address_mismatch", requirement.address, observation.address),
            ("pool_token_pair_mismatch", requirement.token_pair, observation.token_pair),
            ("pool_fee_tier_mismatch", requirement.fee_tier, observation.fee_tier),
        ):
            _compare(reasons, reason, expected, observed)
    else:
        reasons.append("observation_type_mismatch")
    verdict = IdentityVerdict.FAIL if reasons else IdentityVerdict.PASS
    return IdentityEvaluation(requirement.requirement_id, verdict, tuple(reasons))


def _compare(reasons: list[str], reason: str, expected: object, observed: object) -> None:
    if expected != observed:
        reasons.append(reason)


def evaluate_observation_set(
    requirements: Sequence[Requirement], observations: Sequence[Observation]
) -> ObservationSetEvaluation:
    """Evaluate exact coverage, rejecting ambiguous duplicate evidence."""
    expected = {requirement.requirement_id: requirement for requirement in _unique_requirements(requirements)}
    observed: dict[str, Observation] = {}
    for observation in observations:
        if observation.requirement_id in observed:
            raise ValueError(f"Duplicate observation for {observation.requirement_id}")
        observed[observation.requirement_id] = observation
    results = tuple(evaluate_identity(requirement, observed.get(key)) for key, requirement in sorted(expected.items()))
    unexpected = tuple(sorted(set(observed) - set(expected)))
    return ObservationSetEvaluation(results=results, unexpected_observation_ids=unexpected)


__all__ = [
    "DirectChainlinkFeedObservation",
    "DirectChainlinkFeedRequirement",
    "IdentityEvaluation",
    "IdentityVerdict",
    "ObservationProvenance",
    "ObservationSetEvaluation",
    "ResourceKind",
    "TokenObservation",
    "TokenRequirement",
    "V3PoolObservation",
    "V3PoolRequirement",
    "derive_chainlink_requirements",
    "derive_production_requirements",
    "derive_token_requirements",
    "derive_v3_pool_requirements",
    "evaluate_identity",
    "evaluate_observation_set",
    "requirements_digest",
    "requirement_from_dict",
    "observation_from_dict",
]
