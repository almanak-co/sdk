"""Connector strategy-registration registry.

Every connector under :mod:`almanak.connectors` declares which intent verbs it
implements and which chains those implementations are alive on. Strategies
route intents to connectors via the compiler; this registry makes the
*universe* of (connector, intent, chain) triples machine-readable so downstream
tooling - coverage gates, docs generation, demo gating, agent-tool exposure -
does not need to hand-maintain a parallel list.

Connector-owned ``CONNECTOR`` manifests in
``almanak/connectors/<name>/connector.py`` are the source of truth. Descriptor
discovery can load strategy support without importing connector packages or
framework intent vocabulary, then this module converts that metadata into
:class:`ConnectorManifest` values for downstream consumers.

:func:`_import_all_connectors` is the CI/tooling sweep that hydrates the
registry from descriptors and then imports remaining protocol packages to catch
package import errors.
"""

from __future__ import annotations

import importlib
import pkgutil
from dataclasses import dataclass, field, replace

from almanak.connectors._connector import CONNECTOR_REGISTRY as CONNECTOR_DESCRIPTOR_REGISTRY
from almanak.connectors._connector import Connector as ConnectorDescriptor
from almanak.connectors._connector import StrategyIntentChainExclusion, StrategyMatrixEntry
from almanak.core.constants import canonical_chain_name
from almanak.framework.intents.vocabulary import IntentType

# Strategy registry venue identifiers a connector may declare, keyed by the
# ChainRegistry CANONICAL lowercase name (``bsc``, not the historical ``bnb``
# alias — VIB-5293). ``ConnectorManifest``
# canonicalizes declared chains through ``canonical_chain_name`` BEFORE this
# set is checked, so a connector declaring a registered alias still validates
# and downstream consumers always observe canonical names. Solana protocols
# use ``solana``; non-EVM L1s with their own chain-like semantics
# (Hyperliquid) live here as first-class venues (not ChainRegistry entries —
# canonicalization passes them through verbatim).
# Off-chain venues (centralized exchanges like Kraken) do NOT appear in
# this set — they register with ``chains=None`` instead.
KNOWN_VENUES: frozenset[str] = frozenset(
    {
        "ethereum",
        "arbitrum",
        "base",
        "optimism",
        "polygon",
        "bsc",
        "avalanche",
        "linea",
        "mantle",
        "xlayer",
        "monad",
        "zerog",
        "robinhood",
        "solana",
        "hyperliquid",
        "hyperevm",
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
    "hyperevm",
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
      ``(matrix_name, category)`` row is live. Uses ChainRegistry
      canonical chain names (``"bsc"`` not ``"bnb"``) — the same
      vocabulary the strategy manifest's ``chains`` field canonicalizes
      to; matrix rendering additionally alias-normalises as a tolerant
      backstop.
    """

    matrix_name: str
    category: str
    chains: frozenset[str]


@dataclass(frozen=True)
class IntentChainExclusion:
    """One ``(intent, chains)`` pair the connector does NOT support (VIB-6111).

    The registry-side twin of
    :class:`~almanak.connectors._connector_descriptor.StrategyIntentChainExclusion`,
    typed with :class:`IntentType` the same way ``ConnectorManifest.intents``
    is, so consumers never re-parse intent strings.

    Semantics are **narrowing-only**: an exclusion subtracts from the declared
    ``intents`` x ``chains`` cross-product and can never widen it. That keeps
    the "the advertised matrix cannot outrun the declaration" invariant true by
    construction, and means a chain added to ``chains`` later is automatically
    supported for every intent unless someone deliberately excludes it.

    Fields:

    * ``intent`` — the verb being narrowed. Must appear in ``intents``.
    * ``chains`` — the chains this verb is NOT supported on. Non-empty, a
      strict subset of ``chains`` (excluding every chain would mean the verb
      is not supported at all — drop it from ``intents`` instead). Values are
      canonicalized through :func:`canonical_chain_name` at construction, same
      as ``ConnectorManifest.chains``.
    * ``reason`` — non-empty human-readable why. This surface exists to
      document truth, so a silent haircut is not allowed.
    * ``ticket`` — non-empty tracking ticket (e.g. ``"VIB-6111"``).
    """

    intent: IntentType
    chains: frozenset[str]
    reason: str
    ticket: str


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
      ambiguous between "no chains" and "not filled in yet". Values are
      canonicalized through :func:`canonical_chain_name` at construction
      (``"bnb"`` → ``"bsc"``), so consumers always read ChainRegistry
      canonical names.
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
    * ``intent_chain_exclusions`` — optional narrowing-only
      :class:`IntentChainExclusion` tuple: the ``(intent, chain)`` cells
      the cross-product implies but the connector does NOT support
      (VIB-6111). Consumers must ask :meth:`chains_for_intent` /
      :meth:`intents_for_chain` rather than reading ``intents`` x
      ``chains`` raw — with the deliberate exception of fail-closed
      safety sweeps (teardown residual discovery), which must keep
      seeing the widest possible scope.

    Validation runs in ``__post_init__`` so a manifest cannot exist in an
    invalid state — every error fires at construction with a message that
    names the offending field and value.
    """

    name: str
    intents: tuple[IntentType, ...]
    chains: tuple[str, ...] | None
    matrix_entries: tuple[MatrixEntry, ...] | None = field(default=None)
    intent_chain_exclusions: tuple[IntentChainExclusion, ...] | None = field(default=None)

    def __post_init__(self) -> None:
        if not isinstance(self.name, str) or not self.name.strip():
            raise ValueError(f"ConnectorManifest.name must be a non-empty string, got {self.name!r}")

        self._canonicalize_chains()
        self._canonicalize_exclusion_chains()
        self._validate_intents()
        self._validate_chains()
        self._validate_matrix_entries()
        self._validate_intent_chain_exclusions()

    def _canonicalize_chains(self) -> None:
        """Rewrite ``chains`` to ChainRegistry canonical names (VIB-5293 root).

        The registry boundary is the single seam where connector-declared
        chain vocabulary becomes the machine-readable (connector, intent,
        chain) universe, so canonicalization happens HERE — before
        validation — rather than in every consumer. A registered alias
        (``"bnb"``) both validates against the canonical
        :data:`KNOWN_VENUES` vocabulary and never leaks downstream; venues
        the chain registry does not model (``"hyperliquid"``) and
        non-string junk pass through verbatim for :meth:`_validate_chains`
        to report. Runs on the frozen dataclass via ``object.__setattr__``
        because it is part of construction, not later mutation.
        """
        if not isinstance(self.chains, tuple):
            return
        object.__setattr__(
            self,
            "chains",
            tuple(canonical_chain_name(chain) if isinstance(chain, str) else chain for chain in self.chains),
        )

    def _canonicalize_exclusion_chains(self) -> None:
        """Rewrite every exclusion's ``chains`` to canonical names (VIB-6111).

        Runs BEFORE validation for the same reason ``_canonicalize_chains``
        does: the subset check below compares against the already-canonicalized
        ``chains``, so an exclusion declared with a registered alias
        (``"bnb"``) must fold to ``"bsc"`` first or it would spuriously read as
        "not in strategy_chains". Non-string junk passes through verbatim for
        :meth:`_validate_intent_chain_exclusions` to report.
        """
        if not isinstance(self.intent_chain_exclusions, tuple):
            return
        canonicalized = tuple(
            replace(
                exclusion,
                chains=frozenset(
                    canonical_chain_name(chain) if isinstance(chain, str) else chain for chain in exclusion.chains
                ),
            )
            if isinstance(exclusion, IntentChainExclusion) and isinstance(exclusion.chains, frozenset)
            else exclusion
            for exclusion in self.intent_chain_exclusions
        )
        object.__setattr__(self, "intent_chain_exclusions", canonicalized)

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

    def _validate_intent_chain_exclusions(self) -> None:
        """Validate ``intent_chain_exclusions`` shape + per-entry contents (VIB-6111)."""
        if self.intent_chain_exclusions is None:
            return
        if not isinstance(self.intent_chain_exclusions, tuple):
            raise ValueError(
                f"ConnectorManifest.intent_chain_exclusions must be a tuple of IntentChainExclusion, "
                f"got {type(self.intent_chain_exclusions).__qualname__}"
            )
        bad_entry_types = [e for e in self.intent_chain_exclusions if not isinstance(e, IntentChainExclusion)]
        if bad_entry_types:
            raise ValueError(
                f"ConnectorManifest.intent_chain_exclusions must contain only IntentChainExclusion; "
                f"got non-IntentChainExclusion values {bad_entry_types!r}"
            )
        for exclusion in self.intent_chain_exclusions:
            self._validate_intent_chain_exclusion_fields(exclusion)
        intents = [e.intent for e in self.intent_chain_exclusions]
        if len(set(intents)) != len(intents):
            raise ValueError(
                f"ConnectorManifest.intent_chain_exclusions has duplicate intent keys: {intents!r}. "
                "Declare one exclusion per intent listing every excluded chain."
            )
        # The DUAL of the per-entry "excludes every declared chain" rule
        # (VIB-6111). That rule says: an intent excluded everywhere should be
        # dropped from ``intents``. This one says: a chain on which EVERY intent
        # is excluded should be dropped from ``chains``. Without it the
        # connector still claims the chain while supporting nothing on it —
        # ``intents_for_chain`` returns ``()`` and the docs generators render the
        # connector under that chain with a BLANK intent cell, contradicting the
        # support matrix (which drops the row) with no reason surfaced anywhere.
        if self.chains:
            for chain in self.chains:
                if not any(chain not in self.excluded_chains(intent) for intent in self.intents):
                    raise ValueError(
                        f"ConnectorManifest {self.name!r} excludes EVERY declared intent on chain "
                        f"{chain!r}, so the connector claims a chain it supports nothing on. "
                        "Remove the chain from strategy_chains instead of excluding every intent on it."
                    )
        if self.matrix_entries is not None:
            # ``matrix_entries`` is a verbatim override: the matrix builder
            # publishes those rows as-is and skips intent derivation entirely,
            # so it never consults ``chains_for_intent``. Declaring both would
            # let the rendered matrix advertise a cell the exclusions removed —
            # the displayed matrix outrunning the declaration, which is the one
            # invariant this field exists to protect. Rejected at construction
            # rather than silently resolved, because either resolution order
            # ("override wins" / "exclusion wins") would surprise half of
            # readers (VIB-6111).
            raise ValueError(
                f"ConnectorManifest {self.name!r} declares BOTH matrix_entries and "
                "intent_chain_exclusions. matrix_entries is published verbatim and bypasses "
                "per-intent narrowing, so the two cannot be combined without the matrix "
                "outrunning the declaration. Use intent_chain_exclusions alone (rows are then "
                "derived and narrowed), or encode the narrowing directly in matrix_entries."
            )

    def _validate_intent_chain_exclusion_fields(self, exclusion: IntentChainExclusion) -> None:
        """Validate one exclusion against the declared intents/chains."""
        if not isinstance(exclusion.intent, IntentType):
            raise ValueError(f"IntentChainExclusion.intent must be an IntentType member, got {exclusion.intent!r}")
        if exclusion.intent not in self.intents:
            raise ValueError(
                f"IntentChainExclusion.intent {exclusion.intent!r} is not declared in "
                f"ConnectorManifest.intents {self.intents!r} — an exclusion may only narrow the "
                "declared cross-product, never introduce a new intent."
            )
        if not isinstance(exclusion.chains, frozenset) or not exclusion.chains:
            raise ValueError(
                f"IntentChainExclusion.chains must be a non-empty frozenset[str], got {exclusion.chains!r}"
            )
        bad_chains = [c for c in exclusion.chains if not isinstance(c, str) or not c.strip()]
        if bad_chains:
            raise ValueError(f"IntentChainExclusion.chains must contain only non-empty strings, got {bad_chains!r}")
        if self.chains is None:
            raise ValueError(
                "ConnectorManifest.intent_chain_exclusions may not be set when chains is None — "
                "an off-chain venue has no chains to exclude."
            )
        unknown = set(exclusion.chains) - set(self.chains)
        if unknown:
            raise ValueError(
                f"IntentChainExclusion.chains for intent {exclusion.intent!r} contains chains not in "
                f"ConnectorManifest.chains: {sorted(unknown)!r}. Allowed: {sorted(self.chains)!r}."
            )
        if set(exclusion.chains) == set(self.chains):
            raise ValueError(
                f"IntentChainExclusion for intent {exclusion.intent!r} excludes every declared chain "
                f"{sorted(self.chains)!r} — drop the intent from strategy_intents instead."
            )
        if not isinstance(exclusion.reason, str) or not exclusion.reason.strip():
            raise ValueError(f"IntentChainExclusion.reason must be a non-empty string, got {exclusion.reason!r}")
        if not isinstance(exclusion.ticket, str) or not exclusion.ticket.strip():
            raise ValueError(f"IntentChainExclusion.ticket must be a non-empty string, got {exclusion.ticket!r}")

    # ------------------------------------------------------------------
    # Narrowed reads (VIB-6111) — the single way consumers should ask
    # "is this (intent, chain) cell actually supported?".
    # ------------------------------------------------------------------

    def excluded_chains(self, intent: IntentType) -> frozenset[str]:
        """Canonical chain names ``intent`` is explicitly NOT supported on."""
        for exclusion in self.intent_chain_exclusions or ():
            if exclusion.intent == intent:
                return exclusion.chains
        return frozenset()

    def exclusion_for(self, intent: IntentType, chain: str) -> IntentChainExclusion | None:
        """The exclusion covering ``(intent, chain)``, or ``None`` when none does.

        Accepts canonical or alias chain names. Returning the exclusion (not a
        bool) lets renderers surface the ``reason`` / ``ticket`` instead of
        showing an unexplained blank cell.

        ``None`` means "no exclusion covers this cell" — it does NOT mean
        "supported". An intent or chain the connector never declared also has no
        exclusion. Ask :meth:`supports` for the membership question and use this
        only to render *why* a declared cell was narrowed away.
        """
        canonical = canonical_chain_name(chain) if isinstance(chain, str) else chain
        for exclusion in self.intent_chain_exclusions or ():
            if exclusion.intent == intent and canonical in exclusion.chains:
                return exclusion
        return None

    def chains_for_intent(self, intent: IntentType) -> tuple[str, ...]:
        """``chains`` minus the chains excluded for ``intent``.

        Returns ``()`` for off-chain venues (``chains is None``), matching the
        "the matrix is on-chain only" convention downstream consumers already
        use. Declaration order is preserved.

        An intent the connector does not declare yields ``()`` — the exact dual
        of :meth:`intents_for_chain` returning ``()`` for an undeclared chain.
        Without this check the method would answer "supported on every chain"
        for an intent that is not in the product at all: ``aave_v3`` would report
        ``chains_for_intent(SWAP)`` as all ten chains. That is a WIDENING, and
        these methods are documented as the single way to ask "is this
        ``(intent, chain)`` cell supported?" — so a consumer writing
        ``chain in m.chains_for_intent(intent)`` would get a false positive for
        every undeclared verb.
        """
        if self.chains is None:
            return ()
        if intent not in self.intents:
            return ()
        excluded = self.excluded_chains(intent)
        return tuple(chain for chain in self.chains if chain not in excluded)

    def supports(self, intent: IntentType, chain: str) -> bool:
        """Whether ``(intent, chain)`` is a declared, non-excluded cell.

        The unambiguous membership question. :meth:`exclusion_for` answers a
        narrower one — "which exclusion covers this cell" — and its ``None``
        means "no exclusion covers it", NOT "supported": an undeclared intent
        or chain has no exclusion either. Callers that only special-case
        ``exclusion_for(...) is not None`` would read an out-of-product cell as
        ordinary supported, so they should ask this instead and use
        ``exclusion_for`` only to render the reason.

        Returns ``False`` for an OFF-CHAIN venue (``chains is None``, e.g.
        Kraken), matching ``chains_for_intent`` / ``intents_for_chain``'s
        "the matrix is on-chain only" convention. A caller using this as a
        general execution gate would therefore exclude every off-chain venue —
        it answers "is this an on-chain cell this connector supports?", not
        "can this connector do this at all".
        """
        if self.chains is None:
            return False
        canonical = canonical_chain_name(chain) if isinstance(chain, str) else chain
        return canonical in self.chains_for_intent(intent)

    def intents_for_chain(self, chain: str) -> tuple[IntentType, ...]:
        """The intents supported on ``chain`` — declaration order preserved.

        Accepts canonical or alias chain names. A chain the connector does not
        declare at all yields ``()``: an exclusion narrows, it never widens.
        """
        if self.chains is None:
            return ()
        canonical = canonical_chain_name(chain) if isinstance(chain, str) else chain
        if canonical not in self.chains:
            return ()
        return tuple(intent for intent in self.intents if canonical not in self.excluded_chains(intent))


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
    intent_chain_exclusions: tuple[IntentChainExclusion, ...] | None = None,
) -> None:
    """Register a validated connector strategy manifest.

    Keyword-only - positional args are rejected to keep call sites
    self-documenting.

    ``matrix_entries`` is optional declarative override for the
    ``almanak info matrix`` CLI (VIB-4856 / W4). When set, the connector
    publishes its own ``MatrixEntry`` rows verbatim and the matrix
    builder's intent-to-category derivation is skipped for this
    connector. When ``None``, the matrix builder derives entries from
    ``intents`` + ``chains``. See :class:`MatrixEntry` for the field
    semantics.

    ``intent_chain_exclusions`` is the optional narrowing-only list of
    ``(intent, chains)`` cells the cross-product implies but the connector
    does not support (VIB-6111). See :class:`IntentChainExclusion`.

    Connector authors should declare ``strategy_intents`` and related fields on
    ``CONNECTOR`` in ``connector.py`` rather than calling this helper from a
    package ``__init__.py``.
    """
    ConnectorRegistry.register(
        ConnectorManifest(
            name=name,
            intents=intents,
            chains=chains,
            matrix_entries=matrix_entries,
            intent_chain_exclusions=intent_chain_exclusions,
        )
    )


def _intent_from_descriptor(connector_name: str, intent_value: str) -> IntentType:
    """Convert one descriptor-owned intent string into ``IntentType``."""
    try:
        return IntentType[intent_value]
    except KeyError:
        try:
            return IntentType(intent_value)
        except ValueError as exc:
            raise ValueError(
                f"Connector {connector_name!r} strategy_intents contains unknown intent "
                f"{intent_value!r}; expected one of {sorted(IntentType.__members__)}"
            ) from exc


def _matrix_entry_from_descriptor(entry: StrategyMatrixEntry) -> MatrixEntry:
    """Convert descriptor strategy-matrix metadata into the registry type."""
    return MatrixEntry(
        matrix_name=entry.matrix_name,
        category=entry.category,
        chains=entry.chains,
    )


def _exclusion_from_descriptor(connector_name: str, entry: StrategyIntentChainExclusion) -> IntentChainExclusion:
    """Convert descriptor per-(intent, chain) exclusion metadata into the registry type."""
    return IntentChainExclusion(
        intent=_intent_from_descriptor(connector_name, entry.intent),
        chains=entry.chains,
        reason=entry.reason,
        ticket=entry.ticket,
    )


def _manifest_from_descriptor(connector: ConnectorDescriptor) -> ConnectorManifest:
    """Build a strategy registry manifest from connector-owned metadata."""
    if connector.strategy_intents is None:
        raise ValueError(f"Connector {connector.name!r} does not declare strategy_intents")
    matrix_entries = (
        None
        if connector.strategy_matrix_entries is None
        else tuple(_matrix_entry_from_descriptor(entry) for entry in connector.strategy_matrix_entries)
    )
    exclusions = (
        None
        if connector.strategy_intent_chain_exclusions is None
        else tuple(
            _exclusion_from_descriptor(connector.name, entry) for entry in connector.strategy_intent_chain_exclusions
        )
    )
    return ConnectorManifest(
        name=connector.name,
        intents=tuple(_intent_from_descriptor(connector.name, intent) for intent in connector.strategy_intents),
        chains=connector.strategy_chains,
        matrix_entries=matrix_entries,
        intent_chain_exclusions=exclusions,
    )


def _register_descriptor_connectors() -> frozenset[str]:
    """Register strategy manifests declared by connector descriptors."""
    registered: set[str] = set()
    for connector in CONNECTOR_DESCRIPTOR_REGISTRY.with_strategy_support():
        manifest = _manifest_from_descriptor(connector)
        existing = ConnectorRegistry.get(manifest.name)
        if existing is None:
            ConnectorRegistry.register(manifest)
        elif existing != manifest:
            raise ValueError(
                f"Connector {manifest.name!r} has conflicting descriptor strategy registrations. "
                f"Descriptor manifest: {manifest!r}; existing manifest: {existing!r}"
            )
        registered.add(manifest.name)
    return frozenset(registered)


def _is_protocol_leaf(info: pkgutil.ModuleInfo) -> bool:
    """A protocol leaf is a non-underscored subpackage of ``almanak.connectors``.

    Underscore-prefixed packages (``_base``, ``_strategy_base``,
    ``_gateway_registry``) are foundation, not protocol leaves.
    """
    return info.ispkg and not info.name.startswith("_")


def _import_one_connector(package_name: str, subpackage_name: str) -> list[str]:
    """Import one connector subpackage and fire its lazy ``_register_once`` if present.

    Returns a list of error strings - empty on success, one entry for an
    import failure, one entry for a ``_register_once`` failure. The two
    failure modes are reported separately so the gate operator can tell
    them apart.

    Protocol connectors are PEP 562 lazy (VIB-4835 cleanup). The optional
    ``_register_once()`` hook is retained only as an idempotent compatibility
    no-op for migrated packages; strategy registration itself is descriptor-
    owned.
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

    _register_descriptor_connectors()
    errors: list[str] = []
    for info in pkgutil.iter_modules(pkg.__path__):
        if _is_protocol_leaf(info):
            errors.extend(_import_one_connector(pkg.__name__, info.name))

    if errors:
        raise RuntimeError("Failed to import connector subpackages:\n  " + "\n  ".join(errors))
