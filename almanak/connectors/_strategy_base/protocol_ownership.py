"""Manifest specs declaring connector ownership of protocol-keyed metadata modules.

A connector that ships a protocol-keyed metadata module (``capabilities.py``
exporting ``PROTOCOL_CAPABILITIES``, ``supported_chains.py`` exporting
``SUPPORTED_CHAINS_BY_PROTOCOL``) declares which protocol identifiers that
module owns on its ``CONNECTOR`` manifest. The keys live on the manifest — not
only inside the metadata module — so registries can answer ``has(protocol)``
and route per-key lookups WITHOUT importing the metadata module. That keys
duplication is deliberate: it is what preserves the per-key lazy-import
contract (a broken sibling connector cannot poison an unrelated lookup), and
the registries raise on lookup when a manifest claims a key its module does
not declare, so the two lists cannot drift silently.

``CapabilitiesSpec`` and ``SupportedChainsSpec`` are distinct types (rather
than one shared spec) so a manifest author cannot swap them across fields
unnoticed.
"""

from __future__ import annotations

from dataclasses import dataclass

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
    """Which protocol identifiers a connector's ``supported_chains.py`` owns.

    ``module`` must export a module-level ``SUPPORTED_CHAINS_BY_PROTOCOL``
    dict containing every identifier in ``keys``. This is execution-side chain
    coverage ("where is the connector alive on-chain"), distinct from the
    manifest's ``strategy_chains`` venue list — the two are different facts
    with different vocabularies today (see VIB-4851).
    """

    keys: tuple[str, ...]
    module: str

    def __post_init__(self) -> None:
        """Validate the declared keys and module path."""
        _validate_keys_and_module("SupportedChainsSpec", self.keys, self.module)
