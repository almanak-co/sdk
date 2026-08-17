"""Writer-safe, chain-scoped fungible asset identity.

``AssetIdentity`` is the dependency-light value object shared by resolver,
compiler, receipt, accounting, valuation, and lifecycle code.  It contains no
display or amount metadata: symbols, decimals, aliases, and provenance describe
an asset but do not identify it.

The canonical external representation is CAIP-19.  The canonical persisted
representation is the versioned mapping returned by :meth:`to_wire`.
"""

from __future__ import annotations

import json
import re
from collections.abc import Mapping
from dataclasses import dataclass
from enum import StrEnum
from typing import Any

from almanak.core.addresses import normalize_address
from almanak.core.chains import ChainRegistry, parse_caip2
from almanak.core.enums import ChainFamily

ASSET_IDENTITY_SCHEMA_VERSION = 1

_EVM_ADDRESS_RE = re.compile(r"^0[xX][0-9a-fA-F]{40}$")
_SOLANA_MINT_RE = re.compile(r"^[1-9A-HJ-NP-Za-km-z]{32,44}$")
_BASE58_ALPHABET = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"
_BASE58_INDEX = {character: index for index, character in enumerate(_BASE58_ALPHABET)}
_ASSET_NAMESPACE_RE = re.compile(r"^[-a-z0-9]{3,8}$")
_ASSET_REFERENCE_RE = re.compile(r"^[-.%a-zA-Z0-9]{1,128}$")
_WIRE_KEYS = frozenset({"schemaVersion", "chain", "assetNamespace", "assetReference"})


def _base58_decoded_length(value: str) -> int:
    """Return decoded byte length without introducing a protocol-SDK dependency."""
    number = 0
    for character in value:
        number = number * 58 + _BASE58_INDEX[character]
    significant_bytes = (number.bit_length() + 7) // 8
    leading_zero_bytes = len(value) - len(value.lstrip("1"))
    return leading_zero_bytes + significant_bytes


class AssetNamespace(StrEnum):
    """Closed CAIP-19 namespaces supported by writer-safe fungible identity."""

    ERC20 = "erc20"
    NATIVE = "slip44"
    TOKEN = "token"


class UnmeasuredDecimalsReason(StrEnum):
    """Closed reasons why amount-scale metadata is not measured."""

    NOT_RESOLVED = "not_resolved"
    PROVIDER_UNAVAILABLE = "provider_unavailable"
    UNSUPPORTED_ASSET = "unsupported_asset"


class NativeIdentityUnavailableReason(StrEnum):
    """Closed reasons a canonical native identity cannot be emitted."""

    MISSING_SLIP44 = "missing_slip44"


@dataclass(frozen=True, slots=True)
class NativeIdentityUnavailable:
    """Typed refusal to guess a native identity for an incomplete chain."""

    chain: str
    reason_code: NativeIdentityUnavailableReason

    def __post_init__(self) -> None:
        if type(self.chain) is not str:
            raise TypeError(f"NativeIdentityUnavailable.chain must be a string, got {type(self.chain).__name__}")
        if type(self.reason_code) is not NativeIdentityUnavailableReason:
            raise TypeError(
                "NativeIdentityUnavailable.reason_code must be a NativeIdentityUnavailableReason; "
                f"got {type(self.reason_code).__name__}"
            )
        descriptor = ChainRegistry.resolve(self.chain)
        if descriptor.native.slip44 is not None:
            raise ValueError(f"Chain {descriptor.name!r} has a registered SLIP-44 native identity")
        object.__setattr__(self, "chain", descriptor.name)


@dataclass(frozen=True, slots=True)
class KnownDecimals:
    """Measured decimals safe to use in amount conversion."""

    value: int

    def __post_init__(self) -> None:
        if type(self.value) is not int or not 0 <= self.value <= 77:
            raise ValueError(f"KnownDecimals.value must be an integer from 0 through 77, got {self.value!r}")


@dataclass(frozen=True, slots=True)
class UnmeasuredDecimals:
    """Explicit absence of decimals; deliberately has no numeric fallback."""

    reason_code: UnmeasuredDecimalsReason

    def __post_init__(self) -> None:
        if type(self.reason_code) is not UnmeasuredDecimalsReason:
            raise TypeError(
                "UnmeasuredDecimals.reason_code must be an UnmeasuredDecimalsReason; "
                f"got {type(self.reason_code).__name__}"
            )


AssetDecimals = KnownDecimals | UnmeasuredDecimals


@dataclass(frozen=True, slots=True)
class ParsedAsset:
    """Grammar-validated CAIP-19 components without registry resolution."""

    caip2: str
    asset_namespace: str
    asset_reference: str


def parse_caip19(value: str) -> ParsedAsset:
    """Parse CAIP-19 syntax without asserting registered-chain semantics."""
    if type(value) is not str:
        raise TypeError(f"CAIP-19 asset id must be a string, got {type(value).__name__}")
    normalized = value.strip()

    chain_part, sep, asset_part = normalized.partition("/")
    if not sep:
        raise ValueError(
            f"Malformed CAIP-19 asset id: {value!r} (expected '<caip2>/<asset_namespace>:<asset_reference>')"
        )
    try:
        parse_caip2(chain_part)
    except ValueError as exc:
        raise ValueError(f"Malformed CAIP-19 asset id: {value!r} (invalid CAIP-2 chain part {chain_part!r})") from exc

    namespace, ns_sep, reference = asset_part.partition(":")
    if not ns_sep or not _ASSET_NAMESPACE_RE.fullmatch(namespace) or not _ASSET_REFERENCE_RE.fullmatch(reference):
        raise ValueError(
            f"Malformed CAIP-19 asset id: {value!r} (expected '<caip2>/<asset_namespace>:<asset_reference>')"
        )
    return ParsedAsset(chain_part, namespace, reference)


@dataclass(frozen=True, slots=True)
class AssetIdentity:
    """Canonical identity of one fungible or native asset on one chain."""

    chain: str
    asset_namespace: AssetNamespace
    asset_reference: str

    def __post_init__(self) -> None:
        if type(self.chain) is not str:
            raise TypeError(f"AssetIdentity.chain must be a string, got {type(self.chain).__name__}")
        if type(self.asset_namespace) is not AssetNamespace:
            raise TypeError(
                f"AssetIdentity.asset_namespace must be an AssetNamespace; got {type(self.asset_namespace).__name__}"
            )
        if type(self.asset_reference) is not str:
            raise TypeError(
                f"AssetIdentity.asset_reference must be a string; got {type(self.asset_reference).__name__}"
            )
        if not self.asset_reference or self.asset_reference != self.asset_reference.strip():
            raise ValueError("AssetIdentity.asset_reference must be non-empty and whitespace-trimmed")

        descriptor = ChainRegistry.resolve(self.chain)
        reference = self.asset_reference

        if self.asset_namespace is AssetNamespace.NATIVE:
            slip44 = descriptor.native.slip44
            if slip44 is None:
                raise ValueError(
                    f"Chain {descriptor.name!r} has no SLIP-44 coin type; native asset identity cannot be constructed"
                )
            expected = str(slip44)
            if reference != expected:
                raise ValueError(
                    f"Native asset reference for {descriptor.name!r} must be SLIP-44 {expected!r}, got {reference!r}"
                )
        elif descriptor.family is ChainFamily.EVM:
            if self.asset_namespace is not AssetNamespace.ERC20:
                raise ValueError(
                    f"EVM chain {descriptor.name!r} requires namespace {AssetNamespace.ERC20.value!r} "
                    f"for non-native assets, got {self.asset_namespace.value!r}"
                )
            if not _EVM_ADDRESS_RE.fullmatch(reference):
                raise ValueError(
                    f"ERC-20 asset reference on {descriptor.name!r} must be '0x' plus 40 hexadecimal characters"
                )
            reference = normalize_address(reference, descriptor.name)
        elif descriptor.family is ChainFamily.SOLANA:
            if self.asset_namespace is not AssetNamespace.TOKEN:
                raise ValueError(
                    f"Solana chain {descriptor.name!r} requires namespace {AssetNamespace.TOKEN.value!r} "
                    f"for non-native assets, got {self.asset_namespace.value!r}"
                )
            if not _SOLANA_MINT_RE.fullmatch(reference):
                raise ValueError(f"Solana asset reference on {descriptor.name!r} must be a 32-44 character base58 mint")
            if _base58_decoded_length(reference) != 32:
                raise ValueError(f"Solana asset reference on {descriptor.name!r} must decode to exactly 32 bytes")
            reference = normalize_address(reference, descriptor.name)
        else:  # pragma: no cover - registry additions must extend this contract first
            raise ValueError(f"Unsupported chain family for asset identity: {descriptor.family.value}")

        object.__setattr__(self, "chain", descriptor.name)
        object.__setattr__(self, "asset_reference", reference)

    @classmethod
    def native(cls, chain: str) -> AssetIdentity:
        """Construct the registered native identity without guessing SLIP-44."""
        result = resolve_native_asset_identity(chain)
        if isinstance(result, NativeIdentityUnavailable):
            raise ValueError(
                f"Chain {result.chain!r} has no SLIP-44 coin type; native asset identity cannot be constructed"
            )
        return result

    @classmethod
    def from_caip19(cls, value: str) -> AssetIdentity:
        """Resolve and validate one canonical CAIP-19 identity."""
        parsed = parse_caip19(value)
        descriptor = ChainRegistry.by_caip2(parsed.caip2)
        try:
            namespace = AssetNamespace(parsed.asset_namespace)
        except ValueError as exc:
            raise ValueError(f"Unsupported fungible asset namespace: {parsed.asset_namespace!r}") from exc
        return cls(descriptor.name, namespace, parsed.asset_reference)

    @classmethod
    def from_wire(cls, data: Mapping[str, Any]) -> AssetIdentity:
        """Load the exact version-1 persisted shape, rejecting drift."""
        if not isinstance(data, Mapping):
            raise TypeError(f"AssetIdentity wire value must be a mapping, got {type(data).__name__}")
        keys = frozenset(data)
        if keys != _WIRE_KEYS:
            missing = sorted(_WIRE_KEYS - keys)
            extra = sorted(keys - _WIRE_KEYS)
            raise ValueError(f"AssetIdentity wire keys mismatch: missing={missing}, extra={extra}")
        version = data["schemaVersion"]
        if type(version) is not int or version != ASSET_IDENTITY_SCHEMA_VERSION:
            raise ValueError(
                f"Unsupported AssetIdentity schemaVersion {version!r}; expected {ASSET_IDENTITY_SCHEMA_VERSION}"
            )
        namespace_value = data["assetNamespace"]
        if type(namespace_value) is not str:
            raise TypeError("AssetIdentity wire assetNamespace must be a string")
        try:
            namespace = AssetNamespace(namespace_value)
        except ValueError as exc:
            raise ValueError(f"Unsupported AssetIdentity namespace: {namespace_value!r}") from exc
        return cls(
            chain=data["chain"],
            asset_namespace=namespace,
            asset_reference=data["assetReference"],
        )

    @classmethod
    def from_json(cls, value: str) -> AssetIdentity:
        """Load canonical JSON while preserving the strict wire validator."""
        if type(value) is not str:
            raise TypeError(f"AssetIdentity JSON must be a string, got {type(value).__name__}")
        try:
            data = json.loads(value)
        except json.JSONDecodeError as exc:
            raise ValueError("AssetIdentity JSON is malformed") from exc
        if not isinstance(data, dict):
            raise ValueError("AssetIdentity JSON must contain an object")
        return cls.from_wire(data)

    @property
    def caip19(self) -> str:
        """Return the canonical CAIP-19 external identity."""
        descriptor = ChainRegistry.get(self.chain)
        return f"{descriptor.caip2}/{self.asset_namespace.value}:{self.asset_reference}"

    @property
    def identity_key(self) -> tuple[str, AssetNamespace, str]:
        """Return the canonical equality/persistence key."""
        return (self.chain, self.asset_namespace, self.asset_reference)

    def to_wire(self) -> dict[str, str | int]:
        """Serialize the exact version-1 persisted shape."""
        return {
            "schemaVersion": ASSET_IDENTITY_SCHEMA_VERSION,
            "chain": self.chain,
            "assetNamespace": self.asset_namespace.value,
            "assetReference": self.asset_reference,
        }

    def to_json(self) -> str:
        """Serialize to byte-stable canonical JSON without environment data."""
        return json.dumps(self.to_wire(), sort_keys=True, separators=(",", ":"), ensure_ascii=True)


NativeAssetIdentityResult = AssetIdentity | NativeIdentityUnavailable


def resolve_native_asset_identity(chain: str) -> NativeAssetIdentityResult:
    """Return canonical native identity or a typed, non-serializable refusal."""
    descriptor = ChainRegistry.resolve(chain)
    if descriptor.native.slip44 is None:
        return NativeIdentityUnavailable(
            chain=descriptor.name,
            reason_code=NativeIdentityUnavailableReason.MISSING_SLIP44,
        )
    return AssetIdentity(
        chain=descriptor.name,
        asset_namespace=AssetNamespace.NATIVE,
        asset_reference=str(descriptor.native.slip44),
    )


__all__ = [
    "ASSET_IDENTITY_SCHEMA_VERSION",
    "AssetIdentity",
    "AssetDecimals",
    "AssetNamespace",
    "KnownDecimals",
    "NativeAssetIdentityResult",
    "NativeIdentityUnavailable",
    "NativeIdentityUnavailableReason",
    "ParsedAsset",
    "UnmeasuredDecimals",
    "UnmeasuredDecimalsReason",
    "parse_caip19",
    "resolve_native_asset_identity",
]
