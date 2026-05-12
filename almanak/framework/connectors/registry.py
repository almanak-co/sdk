"""Connector registration & registry.

Every connector under :mod:`almanak.framework.connectors` declares which
intent verbs it implements and which chains those implementations are alive
on. Strategies route intents to connectors via the compiler; this registry
makes the *universe* of (connector, intent, chain) triples machine-readable
so that downstream tooling — coverage gates, docs generation, demo gating,
agent-tool exposure — does not need to hand-maintain a parallel list.

The shape:

* Each connector dir contains an ``__init__.py`` that calls
  :func:`register_connector` exactly once at module level. ``__init__.py``
  is used because it is the only file structure uniform across all
  connectors (adapter classes vary: ``Adapter``, ``SDK``, ``Client``,
  ``ctf_sdk``, ...).
* Calls run as a side effect of importing the connector package. Validation
  happens at decoration time, so bad input (typo'd chain, empty intents,
  duplicate registration) raises immediately at import with a clean
  traceback pointing at the call site.
* :class:`ConnectorRegistry` is a module-level singleton populated by those
  calls. :func:`_import_all_connectors` is the CI-only hook that ensures
  every subpackage is imported before the gate queries the registry.

Enforcement that every non-excluded connector dir actually calls
:func:`register_connector` lives at
``scripts/ci/check_connector_registry.py`` and is wired into ``make lint``.
"""

from __future__ import annotations

import importlib
import pkgutil
from dataclasses import dataclass

from almanak.framework.intents.vocabulary import IntentType

# Canonical venue identifiers a connector may declare. EVM chains use the
# normalized form already established by ``almanak.core.constants`` (``bnb``
# not ``bsc``); Solana protocols use ``solana``; non-EVM L1s with their own
# chain-like semantics (Hyperliquid) live here as first-class venues.
# Off-chain venues (centralized exchanges like Kraken) do NOT appear in
# this set — they register with ``chains=None`` instead.
KNOWN_VENUES: frozenset[str] = frozenset(
    {
        "ethereum",
        "arbitrum",
        "base",
        "optimism",
        "polygon",
        "bnb",
        "avalanche",
        "mantle",
        "xlayer",
        "monad",
        "zerog",
        "solana",
        "hyperliquid",
    }
)


@dataclass(frozen=True)
class ConnectorManifest:
    """A connector's self-declared (intent, chain) coverage.

    Fields:

    * ``name`` — connector identifier; must equal the connector's directory
      name under ``almanak/framework/connectors/`` and be unique across the
      registry.
    * ``intents`` — the :class:`IntentType` verbs this connector
      implements. Non-empty, no duplicates.
    * ``chains`` — either a non-empty tuple of strings from
      :data:`KNOWN_VENUES` (no duplicates), or ``None`` for off-chain
      venues (centralized exchanges, etc.). An empty tuple is rejected as
      ambiguous between "no chains" and "not filled in yet".

    Validation runs in ``__post_init__`` so a manifest cannot exist in an
    invalid state — every error fires at construction with a message that
    names the offending field and value.
    """

    name: str
    intents: tuple[IntentType, ...]
    chains: tuple[str, ...] | None

    def __post_init__(self) -> None:
        if not isinstance(self.name, str) or not self.name.strip():
            raise ValueError(f"ConnectorManifest.name must be a non-empty string, got {self.name!r}")

        if not isinstance(self.intents, tuple) or not self.intents:
            raise ValueError(f"ConnectorManifest.intents must be a non-empty tuple of IntentType, got {self.intents!r}")
        bad_intent_types = [i for i in self.intents if not isinstance(i, IntentType)]
        if bad_intent_types:
            raise ValueError(
                f"ConnectorManifest.intents must contain only IntentType members; "
                f"got non-IntentType values {bad_intent_types!r}"
            )
        if len(set(self.intents)) != len(self.intents):
            raise ValueError(f"ConnectorManifest.intents contains duplicates: {self.intents!r}")

        if self.chains is not None:
            if not isinstance(self.chains, tuple) or not self.chains:
                raise ValueError(
                    f"ConnectorManifest.chains must be None or a non-empty tuple; "
                    f"got {self.chains!r}. Use chains=None for off-chain venues "
                    f"(e.g. Kraken). An empty tuple is rejected as ambiguous."
                )
            bad_chain_types = [c for c in self.chains if not isinstance(c, str)]
            if bad_chain_types:
                raise ValueError(
                    f"ConnectorManifest.chains must contain only strings; got non-string values {bad_chain_types!r}"
                )
            unknown = set(self.chains) - KNOWN_VENUES
            if unknown:
                raise ValueError(
                    f"ConnectorManifest.chains contains values not in KNOWN_VENUES: "
                    f"{sorted(unknown)!r}. Allowed: {sorted(KNOWN_VENUES)!r}."
                )
            if len(set(self.chains)) != len(self.chains):
                raise ValueError(f"ConnectorManifest.chains contains duplicates: {self.chains!r}")


class ConnectorRegistry:
    """Module-level singleton populated at import time.

    Production code does not call methods on this class directly — it is
    consumed by CI tooling (``scripts/ci/check_connector_registry.py``) and
    by future tooling (coverage gate, docs generator). The registry exists
    because the universe of (connector, intent, chain) triples is otherwise
    only knowable by reading 21k lines of compiler dispatch code.
    """

    _entries: dict[str, ConnectorManifest] = {}

    @classmethod
    def register(cls, manifest: ConnectorManifest) -> None:
        """Register a connector. Raises if ``manifest.name`` is already registered."""
        if manifest.name in cls._entries:
            raise ValueError(
                f"Connector {manifest.name!r} is already registered. "
                f"Each connector must call register_connector exactly once. "
                f"Existing manifest: {cls._entries[manifest.name]!r}"
            )
        cls._entries[manifest.name] = manifest

    @classmethod
    def all(cls) -> tuple[ConnectorManifest, ...]:
        """Return every registered manifest, sorted by name for determinism."""
        return tuple(cls._entries[name] for name in sorted(cls._entries))

    @classmethod
    def get(cls, name: str) -> ConnectorManifest | None:
        return cls._entries.get(name)

    @classmethod
    def names(cls) -> frozenset[str]:
        return frozenset(cls._entries)

    @classmethod
    def _clear(cls) -> None:
        """Reset the registry. Test fixture only — never call from production."""
        cls._entries.clear()


def register_connector(
    *,
    name: str,
    intents: tuple[IntentType, ...],
    chains: tuple[str, ...] | None,
) -> None:
    """Imperative call placed at module level in each connector's ``__init__.py``.

    Keyword-only — positional args are rejected to keep call sites
    self-documenting at the back-fill scale (~42 connectors).

    The function constructs a :class:`ConnectorManifest` (which validates the
    arguments) and registers it with :class:`ConnectorRegistry`. Both steps
    can raise ``ValueError`` and will surface at import time with a traceback
    pointing at the connector's ``__init__.py`` line.
    """
    ConnectorRegistry.register(ConnectorManifest(name=name, intents=intents, chains=chains))


def _import_all_connectors() -> None:
    """Import every subpackage of ``almanak.framework.connectors``.

    Used only by the CI gate. Production code does not need this — strategies
    import individual connectors on demand, and loading every adapter at
    startup of unrelated CLI commands would be wasteful.

    Errors are collected across all subpackages and raised together so the
    gate operator sees every broken connector in one pass, not a whack-a-mole
    sequence of "fix one, re-run, find the next".
    """
    import almanak.framework.connectors as pkg

    errors: list[str] = []
    for info in pkgutil.iter_modules(pkg.__path__):
        if not info.ispkg:
            continue
        try:
            importlib.import_module(f"{pkg.__name__}.{info.name}")
        except Exception as exc:  # noqa: BLE001
            errors.append(f"{info.name} ({type(exc).__name__}: {exc})")

    if errors:
        raise RuntimeError("Failed to import connector subpackages:\n  " + "\n  ".join(errors))
