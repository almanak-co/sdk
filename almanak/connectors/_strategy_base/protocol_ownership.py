"""Manifest specs for connector-owned protocol metadata.

Capability ownership remains a lazy module reference. Chain support is small,
pure connector metadata and is declared inline so every consumer reads the
same end-to-end support truth.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from types import MappingProxyType

from almanak.core.chains import ChainRegistry

__all__ = [
    "CapabilitiesSpec",
    "SupportedChainsSpec",
]


def _validate_keys_and_module(spec_name: str, keys: tuple[str, ...], module: str) -> None:
    """Validate one ownership spec's protocol keys and module path."""
    if not isinstance(keys, tuple) or not keys:
        raise ValueError(f"{spec_name}.keys must be a non-empty tuple[str, ...], got {keys!r}")
    bad_keys = [key for key in keys if not isinstance(key, str) or not key.strip()]
    if bad_keys:
        raise ValueError(f"{spec_name}.keys must contain only non-empty strings, got {bad_keys!r}")
    if len(set(keys)) != len(keys):
        raise ValueError(f"{spec_name}.keys contains duplicates: {keys!r}")
    # Registry lookups lower-case the requested protocol, so an upper- or
    # mixed-case key would be silently unreachable.
    non_lowercase = [key for key in keys if key != key.lower()]
    if non_lowercase:
        raise ValueError(f"{spec_name}.keys must be lowercase, got {non_lowercase!r}")
    if not isinstance(module, str) or not module.strip():
        raise ValueError(f"{spec_name}.module must be a non-empty module path, got {module!r}")
    if module.startswith("."):
        raise ValueError(f"{spec_name}.module must be an absolute module path, got {module!r}")


@dataclass(frozen=True)
class CapabilitiesSpec:
    """Which protocol identifiers a connector's ``capabilities.py`` owns.

    ``module`` must export a module-level ``PROTOCOL_CAPABILITIES`` dict
    containing every identifier in ``keys``.
    """

    keys: tuple[str, ...]
    module: str

    def __post_init__(self) -> None:
        """Validate the declared keys and module path."""
        _validate_keys_and_module("CapabilitiesSpec", self.keys, self.module)


@dataclass(frozen=True)
class SupportedChainsSpec:
    """End-to-end strategy chain support declared inline by a connector.

    ``chains`` applies to every declared strategy intent unless an entry in
    ``intent_overrides`` replaces it. ``protocol_overrides`` gives an owned
    protocol alias a different chain set (for example Agni Finance on Mantle).
    ``chains=None`` is an explicit off-chain declaration.
    """

    chains: tuple[str, ...] | None
    intent_overrides: Mapping[str, tuple[str, ...]] = field(default_factory=dict)
    protocol_overrides: Mapping[str, tuple[str, ...]] = field(default_factory=dict)

    def __post_init__(self) -> None:
        """Validate and freeze canonical chain declarations."""
        canonical_chains = self._canonicalize_chains("chains", self.chains, allow_none=True)
        canonical_intents = self._canonicalize_overrides("intent_overrides", self.intent_overrides, intent_keys=True)
        canonical_protocols = self._canonicalize_overrides(
            "protocol_overrides",
            self.protocol_overrides,
            intent_keys=False,
        )
        if canonical_chains is None and (canonical_intents or canonical_protocols):
            raise ValueError("off-chain SupportedChainsSpec cannot declare intent or protocol overrides")
        object.__setattr__(self, "chains", canonical_chains)
        object.__setattr__(self, "intent_overrides", MappingProxyType(canonical_intents))
        object.__setattr__(self, "protocol_overrides", MappingProxyType(canonical_protocols))

    @staticmethod
    def _canonicalize_chains(
        field_name: str,
        chains: tuple[str, ...] | None,
        *,
        allow_none: bool,
    ) -> tuple[str, ...] | None:
        if chains is None:
            if allow_none:
                return None
            raise ValueError(f"SupportedChainsSpec.{field_name} must be a non-empty tuple[str, ...]")
        if not isinstance(chains, tuple) or not chains:
            raise ValueError(f"SupportedChainsSpec.{field_name} must be a non-empty tuple[str, ...], got {chains!r}")
        bad = [chain for chain in chains if not isinstance(chain, str) or not chain.strip()]
        if bad:
            raise ValueError(f"SupportedChainsSpec.{field_name} contains invalid chain values: {bad!r}")
        canonical: list[str] = []
        for chain in chains:
            descriptor = ChainRegistry.try_resolve(chain)
            if descriptor is None:
                raise ValueError(f"SupportedChainsSpec.{field_name} contains unknown chain {chain!r}")
            canonical.append(descriptor.name)
        if len(set(canonical)) != len(canonical):
            raise ValueError(f"SupportedChainsSpec.{field_name} contains duplicate canonical chains: {canonical!r}")
        return tuple(canonical)

    @classmethod
    def _canonicalize_overrides(
        cls,
        field_name: str,
        overrides: Mapping[str, tuple[str, ...]],
        *,
        intent_keys: bool,
    ) -> dict[str, tuple[str, ...]]:
        if not isinstance(overrides, Mapping):
            raise ValueError(f"SupportedChainsSpec.{field_name} must be a mapping, got {overrides!r}")
        canonical: dict[str, tuple[str, ...]] = {}
        for raw_key, chains in overrides.items():
            if not isinstance(raw_key, str) or not raw_key.strip():
                raise ValueError(f"SupportedChainsSpec.{field_name} contains invalid key {raw_key!r}")
            # Strip BEFORE normalising. Lookup keys are built from
            # ``getattr(intent, "name", intent)`` and from ``protocol.lower()``,
            # neither of which ever carries stray whitespace — so a key like
            # ``" SWAP"`` would validate, store, and then never match anything.
            # The override would silently be dead weight: no exception, no test
            # failure, just missing coverage for the cell it was meant to pin.
            stripped = raw_key.strip()
            key = stripped.upper() if intent_keys else stripped.lower().replace("-", "_")
            if key in canonical:
                raise ValueError(f"SupportedChainsSpec.{field_name} contains duplicate key {key!r}")
            resolved = cls._canonicalize_chains(f"{field_name}[{raw_key!r}]", chains, allow_none=False)
            assert resolved is not None
            canonical[key] = resolved
        return canonical

    @property
    def is_offchain(self) -> bool:
        """Whether this declaration represents an off-chain venue."""
        return self.chains is None

    def chains_for_intent(self, intent: object) -> tuple[str, ...] | None:
        """Return the exact chain set for one intent name or enum value."""
        raw = getattr(intent, "name", intent)
        key = str(raw).upper()
        return self.intent_overrides.get(key, self.chains)

    def default_chains_union(self) -> tuple[str, ...]:
        """Return the stable union for the connector's canonical protocol."""
        ordered: dict[str, None] = {}
        for chain in self.chains or ():
            ordered.setdefault(chain, None)
        for chains in self.intent_overrides.values():
            for chain in chains:
                ordered.setdefault(chain, None)
        return tuple(ordered)

    def all_chains(self) -> tuple[str, ...]:
        """Return the stable union across canonical and alias coverage."""
        ordered = dict.fromkeys(self.default_chains_union())
        for chains in self.protocol_overrides.values():
            for chain in chains:
                ordered.setdefault(chain, None)
        return tuple(ordered)

    def chains_for_protocol(self, protocol: str) -> tuple[str, ...]:
        """Return a protocol alias override or the canonical connector union."""
        key = protocol.lower().replace("-", "_")
        return self.protocol_overrides.get(key, self.default_chains_union())

    def chains_for(
        self,
        *,
        protocol: str | None = None,
        intent: object | None = None,
    ) -> tuple[str, ...] | None:
        """Return exact coverage for an optional protocol alias and intent.

        A protocol override replaces the connector default for that owned
        alias. Otherwise an intent override replaces the default chain set.
        """
        if protocol is not None:
            key = protocol.lower().replace("-", "_")
            if key in self.protocol_overrides:
                return self.protocol_overrides[key]
            if intent is None:
                return self.default_chains_union()
        if intent is not None:
            return self.chains_for_intent(intent)
        return self.all_chains()

    def supports(
        self,
        *,
        chain: str,
        protocol: str | None = None,
        intent: object | None = None,
    ) -> bool:
        """Return whether this declaration supports an exact support query."""
        descriptor = ChainRegistry.try_resolve(chain)
        if descriptor is None:
            return False
        chains = self.chains_for(protocol=protocol, intent=intent) or ()
        return descriptor.name in chains
