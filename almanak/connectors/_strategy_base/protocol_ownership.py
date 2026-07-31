"""Manifest specs for connector-owned protocol metadata.

Capability ownership remains a lazy module reference. Chain support is small,
pure connector metadata and is declared inline so every consumer reads the
same end-to-end support truth.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from types import MappingProxyType

from almanak.core.chains import ChainDescriptor, ChainRegistry

__all__ = [
    "CapabilitiesSpec",
    "SupportedChainsSpec",
]

_EMPTY_CHAIN_OVERRIDES: Mapping[str, tuple[ChainDescriptor, ...]] = MappingProxyType({})


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


@dataclass(frozen=True, init=False)
class SupportedChainsSpec:
    """End-to-end strategy chain support declared inline by a connector.

    Declarations use the registered :class:`ChainDescriptor` singleton exported
    by each module under :mod:`almanak.core.chains`. This makes a misspelled
    chain an import/type-check failure instead of a string that poisons
    connector discovery at runtime.

    ``chains`` applies to every declared strategy intent unless an entry in
    ``intent_overrides`` replaces it. ``protocol_overrides`` gives an owned
    protocol alias a different chain set (for example Agni Finance on Mantle).
    ``chains=None`` is an explicit off-chain declaration. Construction projects
    descriptors to canonical string names immediately so the existing metadata,
    repr, equality, CLI, config, and serialization boundaries remain stable.
    """

    chains: tuple[str, ...] | None
    intent_overrides: Mapping[str, tuple[str, ...]]
    protocol_overrides: Mapping[str, tuple[str, ...]]

    def __init__(
        self,
        *,
        chains: tuple[ChainDescriptor, ...] | None,
        intent_overrides: Mapping[str, tuple[ChainDescriptor, ...]] = _EMPTY_CHAIN_OVERRIDES,
        protocol_overrides: Mapping[str, tuple[ChainDescriptor, ...]] = _EMPTY_CHAIN_OVERRIDES,
    ) -> None:
        """Validate descriptor inputs and freeze their canonical names."""
        validated_chains = self._validate_chain_refs("chains", chains, allow_none=True)
        validated_intents = self._validate_overrides("intent_overrides", intent_overrides, intent_keys=True)
        validated_protocols = self._validate_overrides(
            "protocol_overrides",
            protocol_overrides,
            intent_keys=False,
        )
        if validated_chains is None and (validated_intents or validated_protocols):
            raise ValueError("off-chain SupportedChainsSpec cannot declare intent or protocol overrides")
        object.__setattr__(self, "chains", validated_chains)
        object.__setattr__(self, "intent_overrides", MappingProxyType(validated_intents))
        object.__setattr__(self, "protocol_overrides", MappingProxyType(validated_protocols))

    @staticmethod
    def _validate_chain_refs(
        field_name: str,
        chains: tuple[ChainDescriptor, ...] | None,
        *,
        allow_none: bool,
    ) -> tuple[str, ...] | None:
        if chains is None:
            if allow_none:
                return None
            raise ValueError(f"SupportedChainsSpec.{field_name} must be a non-empty tuple[ChainDescriptor, ...]")
        if not isinstance(chains, tuple) or not chains:
            raise ValueError(
                f"SupportedChainsSpec.{field_name} must be a non-empty tuple[ChainDescriptor, ...], got {chains!r}"
            )
        bad = [chain for chain in chains if not isinstance(chain, ChainDescriptor)]
        if bad:
            raise TypeError(
                f"SupportedChainsSpec.{field_name} must contain registered ChainDescriptor references, got {bad!r}"
            )
        canonical_names: list[str] = []
        for chain in chains:
            registered = ChainRegistry.try_resolve(chain.name)
            if registered is not chain:
                raise ValueError(
                    f"SupportedChainsSpec.{field_name} contains unregistered "
                    f"ChainDescriptor {chain.name!r}; import the descriptor singleton "
                    f"from almanak.core.chains.{chain.name}"
                )
            canonical_names.append(chain.name)
        if len(set(canonical_names)) != len(canonical_names):
            raise ValueError(
                f"SupportedChainsSpec.{field_name} contains duplicate canonical chains: {canonical_names!r}"
            )
        return tuple(canonical_names)

    @classmethod
    def _validate_overrides(
        cls,
        field_name: str,
        overrides: Mapping[str, tuple[ChainDescriptor, ...]],
        *,
        intent_keys: bool,
    ) -> dict[str, tuple[str, ...]]:
        if not isinstance(overrides, Mapping):
            raise ValueError(f"SupportedChainsSpec.{field_name} must be a mapping, got {overrides!r}")
        validated: dict[str, tuple[str, ...]] = {}
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
            if key in validated:
                raise ValueError(f"SupportedChainsSpec.{field_name} contains duplicate key {key!r}")
            names = cls._validate_chain_refs(f"{field_name}[{raw_key!r}]", chains, allow_none=False)
            assert names is not None
            validated[key] = names
        return validated

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
