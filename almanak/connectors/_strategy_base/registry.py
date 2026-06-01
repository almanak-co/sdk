"""Connector registration & registry.

Every connector under :mod:`almanak.connectors` declares which
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
from dataclasses import dataclass, field

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
        "linea",
        "mantle",
        "xlayer",
        "monad",
        "zerog",
        "solana",
        "hyperliquid",
    }
)


# Canonical display order for the ``almanak info matrix`` CLI (VIB-4856 / W4).
#
# Lives here (and not in ``almanak.framework.cli.support_matrix``) because
# the framework / CLI roots are scanned by ``scripts/ci/scan_chain_protocol_coupling.py``;
# enumerating chain canonical names from a CLI module trips the
# CHAIN_STRING category. ``almanak/connectors/`` is the scan's
# canonical-home exclusion, so the per-chain data legitimately sits here
# next to ``KNOWN_VENUES``.
#
# The tuple is broader than ``KNOWN_VENUES`` (which whitelists the chains
# a connector may register as ``ConnectorManifest.chains``): matrix
# display covers chains that appear via compiler routing tables
# (``PROTOCOL_ROUTERS`` / ``LP_POSITION_MANAGERS``) too — ``bsc``,
# ``blast``, ``sonic``, ``plasma``, ``berachain`` historically render in
# the table even though no connector declares them in its manifest.
# Chains not in this list fall through to alphabetical ordering by
# ``support_matrix._sort_chains`` (forward-compatible default for new
# chains).
MATRIX_CHAIN_DISPLAY_ORDER: tuple[str, ...] = (
    "ethereum",
    "arbitrum",
    "optimism",
    "base",
    "polygon",
    "avalanche",
    "bsc",
    "mantle",
    "linea",
    "blast",
    "sonic",
    "plasma",
    "berachain",
    "monad",
    "solana",
    "hyperliquid",
)


@dataclass(frozen=True)
class MatrixEntry:
    """One ``almanak info matrix`` row this connector contributes (VIB-4856).

    Lives strategy-side because the matrix CLI module under
    ``almanak/framework/cli/`` is a strategy-container module and the
    strategy-side import boundary
    (``tests/static/test_strategy_import_boundary.py``) forbids it from
    reading anything under ``almanak.connectors._base.gateway_*``.
    ``support_matrix.py`` consumes ``ConnectorManifest.matrix_entries``
    directly.

    Fields:

    * ``matrix_name`` — protocol name as rendered in the matrix. May
      differ from the connector's directory name when one connector emits
      multiple rows (e.g. Aerodrome emits both ``"aerodrome"`` and
      ``"aerodrome_slipstream"``).
    * ``category`` — matrix action category (``"swap"``, ``"lp"``,
      ``"lending"``, ``"perps"``, ``"yield"``, ``"prediction"``,
      ``"flash_loan"``, ``"aggregator"``, ``"bridge"``). The connector
      declares this directly so ``support_matrix.py`` does not need a
      hardcoded intent → category dispatch.
    * ``chains`` — frozenset of chain canonical names where this
      ``(matrix_name, category)`` row is live. Uses the matrix's
      canonical chain names (``"bsc"`` not ``"bnb"``; the strategy
      manifest's ``chains`` field uses ``"bnb"`` for its own contracts
      but matrix rendering normalises to ``"bsc"``).
    """

    matrix_name: str
    category: str
    chains: frozenset[str]


def _validate_matrix_entry_fields(entry: MatrixEntry) -> None:
    """Validate a single ``MatrixEntry``'s field contents.

    Catches the same shape of mistakes that other ``ConnectorManifest``
    fields catch (empty string / wrong container / blank chain strings).
    Extracted from ``ConnectorManifest._validate_matrix_entries`` so the
    parent method stays under the CRAP complexity gate.
    """
    if not isinstance(entry.matrix_name, str) or not entry.matrix_name.strip():
        raise ValueError(f"MatrixEntry.matrix_name must be a non-empty string, got {entry.matrix_name!r}")
    if not isinstance(entry.category, str) or not entry.category.strip():
        raise ValueError(f"MatrixEntry.category must be a non-empty string, got {entry.category!r}")
    if not isinstance(entry.chains, frozenset) or not entry.chains:
        raise ValueError(f"MatrixEntry.chains must be a non-empty frozenset[str], got {entry.chains!r}")
    bad_chain_values = [c for c in entry.chains if not isinstance(c, str) or not c.strip()]
    if bad_chain_values:
        raise ValueError(
            f"MatrixEntry.chains must contain only non-empty strings; got invalid values {bad_chain_values!r}"
        )


@dataclass(frozen=True)
class ConnectorManifest:
    """A connector's self-declared (intent, chain) coverage.

    Fields:

    * ``name`` — connector identifier; must equal the connector's directory
      name under ``almanak/connectors/`` and be unique across the
      registry.
    * ``intents`` — the :class:`IntentType` verbs this connector
      implements. Non-empty, no duplicates.
    * ``chains`` — either a non-empty tuple of strings from
      :data:`KNOWN_VENUES` (no duplicates), or ``None`` for off-chain
      venues (centralized exchanges, etc.). An empty tuple is rejected as
      ambiguous between "no chains" and "not filled in yet".
    * ``matrix_entries`` — optional explicit ``MatrixEntry`` tuple
      describing every ``(matrix_name, category, chains)`` row the
      connector emits into ``almanak info matrix``. When ``None`` (the
      default), ``support_matrix.py`` derives the entries from
      ``intents`` + ``chains`` using a small intent → category dispatch.
      Override when the derivation can't produce the right matrix shape:
      multi-row connectors (Aerodrome's slipstream alias), aggregator
      overrides (Enso/LiFi/1inch's ``SWAP`` intent maps to
      ``aggregator``, not ``swap``), and connectors whose matrix chain
      coverage differs from the strategy-side ``chains`` field (e.g. a
      Uniswap V3 fork live on chains where the strategy-side adapter
      doesn't yet declare support).

    Validation runs in ``__post_init__`` so a manifest cannot exist in an
    invalid state — every error fires at construction with a message that
    names the offending field and value.
    """

    name: str
    intents: tuple[IntentType, ...]
    chains: tuple[str, ...] | None
    matrix_entries: tuple[MatrixEntry, ...] | None = field(default=None)

    def __post_init__(self) -> None:
        if not isinstance(self.name, str) or not self.name.strip():
            raise ValueError(f"ConnectorManifest.name must be a non-empty string, got {self.name!r}")

        self._validate_intents()
        self._validate_chains()
        self._validate_matrix_entries()

    def _validate_intents(self) -> None:
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

    def _validate_chains(self) -> None:
        if self.chains is None:
            return
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

    def _validate_matrix_entries(self) -> None:
        """Validate ``matrix_entries`` shape + per-entry field contents.

        ``MatrixEntry`` is a frozen dataclass without its own validation,
        so bad values (empty matrix_name, blank chain strings, non-
        frozenset chain container) would otherwise propagate into matrix
        assembly and surface as confusing ``KeyError`` / ``TypeError``
        downstream. Catch them at registration time where the call site
        is in the traceback. (CodeRabbit review on PR 2469.)

        ``matrix_entries=()`` (zero entries) IS legal — it signals "this
        connector intentionally publishes nothing into the matrix"
        (suppresses the intent → category derivation that would
        otherwise fire for ``matrix_entries=None``); per-entry
        non-emptiness checks therefore only run when entries exist.
        """
        if self.matrix_entries is None:
            return
        if not isinstance(self.matrix_entries, tuple):
            raise ValueError(
                f"ConnectorManifest.matrix_entries must be a tuple of MatrixEntry, "
                f"got {type(self.matrix_entries).__qualname__}"
            )
        bad_entry_types = [e for e in self.matrix_entries if not isinstance(e, MatrixEntry)]
        if bad_entry_types:
            raise ValueError(
                f"ConnectorManifest.matrix_entries must contain only MatrixEntry; "
                f"got non-MatrixEntry values {bad_entry_types!r}"
            )
        for entry in self.matrix_entries:
            _validate_matrix_entry_fields(entry)
        # Same (matrix_name, category) cannot appear twice — declarative
        # overrides must dedupe at the call site, not silently overwrite
        # each other. The matrix renderer treats (name, category) as the
        # entry key.
        keys = [(e.matrix_name, e.category) for e in self.matrix_entries]
        if len(set(keys)) != len(keys):
            raise ValueError(f"ConnectorManifest.matrix_entries has duplicate (matrix_name, category) keys: {keys!r}")


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
        """Reset the registry. Test fixture only — never call from production.

        Also resets the module-level ``_registered`` flag on every loaded
        lazy-connector subpackage so a subsequent ``_register_once()``
        actually re-fires. Without this, the autouse-fixture pattern in
        ``tests/unit/connectors/registry/conftest.py`` leaves connectors
        wedged in a "module says registered, registry says empty" state
        that breaks any downstream consumer
        (``support_matrix._build_matrix``, the coverage gate, …) that
        runs after the registry tests in the same pytest session.
        """
        import sys

        cls._entries.clear()
        for mod_name, mod in list(sys.modules.items()):
            # Only touch connector subpackages — narrow predicate avoids
            # accidentally clobbering an unrelated module that happens to
            # carry a ``_registered`` attribute.
            if (
                mod_name.startswith("almanak.connectors.")
                and mod_name.count(".") == 2
                and getattr(mod, "_registered", None) is True
            ):
                # ``setattr`` (vs ``mod._registered = False``) keeps mypy
                # quiet — ``mod`` is typed as ``ModuleType`` and connector
                # ``_registered`` flags are a connector-convention attribute,
                # not a declared property on ``ModuleType``.
                setattr(mod, "_registered", False)  # noqa: B010


def register_connector(
    *,
    name: str,
    intents: tuple[IntentType, ...],
    chains: tuple[str, ...] | None,
    matrix_entries: tuple[MatrixEntry, ...] | None = None,
) -> None:
    """Imperative call placed at module level in each connector's ``__init__.py``.

    Keyword-only — positional args are rejected to keep call sites
    self-documenting at the back-fill scale (~42 connectors).

    ``matrix_entries`` is optional declarative override for the
    ``almanak info matrix`` CLI (VIB-4856 / W4). When set, the connector
    publishes its own ``MatrixEntry`` rows verbatim and the matrix
    builder's intent → category derivation is skipped for this
    connector. When ``None``, the matrix builder derives entries from
    ``intents`` + ``chains``. See :class:`MatrixEntry` for the field
    semantics.

    The function constructs a :class:`ConnectorManifest` (which validates the
    arguments) and registers it with :class:`ConnectorRegistry`. Both steps
    can raise ``ValueError`` and will surface at import time with a traceback
    pointing at the connector's ``__init__.py`` line.
    """
    ConnectorRegistry.register(
        ConnectorManifest(
            name=name,
            intents=intents,
            chains=chains,
            matrix_entries=matrix_entries,
        )
    )


def _is_protocol_leaf(info: pkgutil.ModuleInfo) -> bool:
    """A protocol leaf is a non-underscored subpackage of ``almanak.connectors``.

    Underscore-prefixed packages (``_base``, ``_strategy_base``,
    ``_gateway_registry``) are foundation, not protocol leaves.
    """
    return info.ispkg and not info.name.startswith("_")


def _import_one_connector(package_name: str, subpackage_name: str) -> list[str]:
    """Import one connector subpackage and fire its lazy ``_register_once`` if present.

    Returns a list of error strings — empty on success, one entry for an
    import failure, one entry for a ``_register_once`` failure. The two
    failure modes are reported separately so the gate operator can tell
    them apart.

    Most protocol connectors are PEP 562 lazy (VIB-4835 cleanup): their
    ``register_connector(...)`` call lives inside a ``_register_once()``
    helper that fires on first strategy-side attribute access, NOT at
    package init. Eager-registering connectors continue to work because
    their registration already ran during the bare import and the helper
    is absent.
    """
    errors: list[str] = []
    try:
        mod = importlib.import_module(f"{package_name}.{subpackage_name}")
    except Exception as exc:  # noqa: BLE001
        errors.append(f"{subpackage_name} ({type(exc).__name__}: {exc})")
        return errors

    register_fn = getattr(mod, "_register_once", None)
    if callable(register_fn):
        try:
            register_fn()
        except Exception as exc:  # noqa: BLE001
            errors.append(f"{subpackage_name}._register_once ({type(exc).__name__}: {exc})")
    return errors


def _import_all_connectors() -> None:
    """Import every protocol-leaf subpackage of ``almanak.connectors``.

    Used only by the CI gate. Production code does not need this — strategies
    import individual connectors on demand, and loading every adapter at
    startup of unrelated CLI commands would be wasteful.

    Errors are collected across all subpackages and raised together so the
    gate operator sees every broken connector in one pass, not a whack-a-mole
    sequence of "fix one, re-run, find the next".
    """
    import almanak.connectors as pkg

    errors: list[str] = []
    for info in pkgutil.iter_modules(pkg.__path__):
        if _is_protocol_leaf(info):
            errors.extend(_import_one_connector(pkg.__name__, info.name))

    if errors:
        raise RuntimeError("Failed to import connector subpackages:\n  " + "\n  ".join(errors))
