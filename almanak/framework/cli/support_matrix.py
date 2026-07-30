"""CLI command to display the chains x protocols support matrix.

Dynamically derived from the SDK's actual data structures — the matrix
is composed from two strategy-side sources:

1. **Strategy-side** :class:`~almanak.connectors._strategy_base.registry.ConnectorRegistry`
   — every ``register_connector`` call contributes a manifest. The
   manifest's optional ``matrix_entries`` field is consumed verbatim
   when declared; otherwise entries are derived from ``intents`` +
   ``chains`` via :func:`_derive_entries_from_intents`.
2. **Compiler routing tables** — last-resort fallback for protocols
   that have no connector folder (``uniswap_v2`` / ``pancakeswap_v2`` /
   ``quickswap`` / ``sushiswap`` / ``velodrome`` / ``1inch``). Read as
   data (dict iteration), so no protocol-name string literals leak into
   this file.

The gateway-side ``SupportedActionsCapability`` was an early design
candidate (Source 3 in the VIB-4856 spec) but the strategy-side import
boundary forbids reading gateway-only modules from a strategy-container
CLI module. ``ConnectorManifest.matrix_entries`` is the equivalent
declarative override on the strategy side — every connector that needs
matrix coverage beyond the intent → category default declares it there
in its ``__init__.py``.

Adding a new connector folder under ``almanak/connectors/<protocol>/``
causes it to appear in the matrix automatically — no central edit. See
VIB-4856 (epic VIB-4851) for the rationale, and
``blueprints/22-connector-self-containment.md`` for the architectural
context.

**Schema v2 (``--json``)** adds ``chainsByIntent`` + ``intentsKnown`` per row
and a provenance envelope, leaving every v1 field byte-identical. The rendering
``category`` collapses a connector's exact verbs into one label — SUPPLY /
BORROW / REPAY / WITHDRAW all become ``lending``, and ``yield`` covers STAKE,
VAULT_DEPOSIT and Pendle's LP/SWAP surface alike — so a consumer reading rows
alone can ask "is this protocol on this chain?" but never "can it do this
operation here?". v2 publishes the second answer from the manifest accessor
that already knows it. Contract and invariants:
``blueprints/05-connectors.md`` §2a Support matrix JSON contract.
"""

from __future__ import annotations

import json as json_module
from collections.abc import Callable, Mapping
from datetime import UTC, datetime
from functools import cache
from pathlib import Path
from types import MappingProxyType
from typing import Any

import click

from almanak.core.chains import ChainRegistry

# ---------------------------------------------------------------------------
# Action categories for organizing protocols
# ---------------------------------------------------------------------------
ACTION_SWAP = "swap"
ACTION_LP = "lp"
ACTION_LENDING = "lending"
ACTION_PERPS = "perps"
ACTION_FLASH_LOAN = "flash_loan"
ACTION_YIELD = "yield"
ACTION_AGGREGATOR = "aggregator"
ACTION_PREDICTION = "prediction"
ACTION_BRIDGE = "bridge"

# Ordered list of all supported categories. Used both to emit protocols in a
# stable order inside `_build_matrix()` and to derive the CLI --category help
# text, so adding a new ACTION_* constant above only requires adding it here
# once to flow through to the rendered table and CLI help.
#
# NOTE: `ACTION_FLASH_LOAN` is DISABLED and withheld from the support matrix
# (`almanak info matrix` — table AND --json — plus the rendered docs). A flash
# loan is only useful as *flash loan + atomic action* (borrow → act → repay in
# one transaction), which requires an on-chain receiver contract implementing
# the provider callback (Balancer `receiveFlashLoan` / Aave `executeOperation`);
# an EOA wallet cannot receive one. Until a receiver-contract-backed demo exists
# the capability is not shippable, so it is not advertised as supported. The
# reference demo is parked at `strategies/internal/demo_catalog/balancer_flash_arb`.
# `ACTION_PREDICTION` is likewise omitted pending further testing. The connectors
# stay registered and the intents still compile at the framework level — they are
# simply not advertised as supported. Re-enable a capability by adding its
# constant back here (flash loans: only once a receiver contract ships).
SUPPORTED_CATEGORIES: tuple[str, ...] = (
    ACTION_SWAP,
    ACTION_LP,
    ACTION_LENDING,
    ACTION_PERPS,
    ACTION_YIELD,
    ACTION_AGGREGATOR,
    ACTION_BRIDGE,
)

# Categories deliberately withheld from the advertised matrix (see the note on
# SUPPORTED_CATEGORIES for why). Intents mapping to one of these are withheld
# from ``chainsByIntent`` as well: the withholding is a PRODUCT decision, not a
# rendering one, so a consumer that keys execution readiness on per-intent
# coverage must not see them either. Without this, schema v2 would re-advertise
# through the intent map exactly what the category filter withholds — e.g.
# ``morpho_blue`` declares FLASH_LOAN and renders only a ``lending`` row.
#
# Every category in the intent -> category map must appear in exactly one of
# SUPPORTED_CATEGORIES or WITHHELD_CATEGORIES; ``test_support_matrix.py``
# enforces that so adding a category to neither list cannot silently create a
# third state where a row is hidden but its intents are published.
WITHHELD_CATEGORIES: tuple[str, ...] = (ACTION_FLASH_LOAN, ACTION_PREDICTION)

# Intents with NO rendering category, listed EXPLICITLY so the omission is a
# decision rather than an oversight. The partition that matters is over INTENTS,
# not categories: an intent absent from the category map yields ``None``, which
# is not in WITHHELD_CATEGORIES, so it was published into ``chainsByIntent``
# without ever passing the product/withheld decision (observed: hyperliquid
# published PERP_WITHDRAW). Adding a new IntentType now fails
# ``test_support_matrix.py`` until someone classifies it.
#
# These ARE published: they are genuine strategy-authorable capabilities that
# the rendering vocabulary simply cannot draw a row for. Withholding them would
# repeat the collapse schema v2 exists to undo. Verbs that must NOT be
# advertised belong in WITHHELD_CATEGORIES via the category map instead.
UNCATEGORISED_INTENTS: frozenset[str] = frozenset(
    {
        "CLOSE_CDP",
        "DELEVERAGE",
        "ENSURE_BALANCE",
        "HOLD",
        "LIQUIDATE",
        "MINT_STABLE",
        "OPEN_CDP",
        "PERP_CANCEL_ORDER",
        "PERP_WITHDRAW",
        "REPAY_STABLE",
        "UNWRAP_NATIVE",
        "VAULT_MANAGE",
        "VAULT_REALLOCATE",
        "WRAP_NATIVE",
    }
)

# Schema version for ``almanak info matrix --json``. v1 was the implicit,
# unversioned ``{chains, protocols}`` shape; v2 adds ``chainsByIntent`` /
# ``intentsKnown`` per row plus the provenance envelope, and leaves every v1
# field byte-identical. Bump only on a breaking change to the published shape.
SCHEMA_VERSION = 2


# ---------------------------------------------------------------------------
# Connector-driven matrix derivation
# ---------------------------------------------------------------------------


def _matrix_chain(chain: str) -> str:
    """Normalise a manifest chain name to the matrix's canonical form.

    The strategy registry canonicalizes ``ConnectorManifest.chains``
    at construction (``"bnb"`` → ``"bsc"``), so manifest-derived rows arrive
    already canonical and this is a no-op for them. It stays as the tolerant
    rendering backstop for declarative ``matrix_entries`` (which are not
    registry-canonicalized) and for any alias that slips in via routing
    tables, so downstream Edge / CI consumers always match on the canonical
    string. Normalising via :class:`~almanak.core.chains.ChainRegistry`
    ensures new aliases flow automatically. Unknown chains pass through.
    """
    descriptor = ChainRegistry.try_resolve(chain)
    return descriptor.name if descriptor is not None else chain


def _normalize_entry_chains(chains: frozenset[str]) -> frozenset[str]:
    """Apply ``_matrix_chain`` to every chain in a ``MatrixEntry``."""
    return frozenset(_matrix_chain(c) for c in chains)


def _constant_chains(chains: tuple[str, ...]) -> Callable[[Any], tuple[str, ...]]:
    """A narrowing read that returns ``chains`` for every intent.

    Used to derive the PRE-exclusion rows so they can be diffed against the
    narrowed ones (VIB-6111). A closure factory rather than an inline lambda:
    an inline `lambda _intent: declared` inside the connector loop trips ruff
    B023 (late binding), and the `lambda _intent, _c=…: _c` form that avoids
    B023 defeats mypy's inference against the `Callable` parameter type. Binding
    the value through a function argument satisfies both.
    """

    def _read(_intent: Any) -> tuple[str, ...]:
        return chains

    return _read


@cache
def _intent_category_map() -> Mapping[Any, str]:
    """Intent class -> rendering category.

    Centralising the dispatch here means connectors don't have to repeat it;
    the matrix is the only place that knows about the rendering category
    vocabulary. Module-level and cached (rather than rebuilt inside
    :func:`_derive_entries_from_intents`) because schema v2's per-intent
    coverage needs the SAME map to decide which intents are withheld —
    two copies would drift, and a drifted copy publishes an intent whose
    category the renderer suppresses.

    Built lazily: ``IntentType`` pulls in the intent vocabulary, and this
    module is imported by the CLI's top-level ``info`` group. Returned as a
    read-only view because the result is cached and shared — a caller mutating
    it would poison every later build in the process.
    """
    from almanak.framework.intents.vocabulary import IntentType

    return MappingProxyType(
        {
            IntentType.SWAP: ACTION_SWAP,
            IntentType.LP_OPEN: ACTION_LP,
            IntentType.LP_CLOSE: ACTION_LP,
            IntentType.LP_COLLECT_FEES: ACTION_LP,
            IntentType.SUPPLY: ACTION_LENDING,
            IntentType.BORROW: ACTION_LENDING,
            IntentType.REPAY: ACTION_LENDING,
            IntentType.WITHDRAW: ACTION_LENDING,
            IntentType.PERP_OPEN: ACTION_PERPS,
            IntentType.PERP_CLOSE: ACTION_PERPS,
            IntentType.STAKE: ACTION_YIELD,
            IntentType.UNSTAKE: ACTION_YIELD,
            IntentType.VAULT_DEPOSIT: ACTION_YIELD,
            IntentType.VAULT_REDEEM: ACTION_YIELD,
            IntentType.PREDICTION_BUY: ACTION_PREDICTION,
            IntentType.PREDICTION_SELL: ACTION_PREDICTION,
            IntentType.PREDICTION_REDEEM: ACTION_PREDICTION,
            IntentType.BRIDGE: ACTION_BRIDGE,
            IntentType.FLASH_LOAN: ACTION_FLASH_LOAN,
        }
    )


def _connector_intent_coverage(manifest: Any) -> dict[str, list[str]]:
    """Per-intent chain coverage for one connector — the schema v2 payload.

    ``{intent_name: [chain, ...]}`` built from
    :meth:`ConnectorManifest.chains_for_intent`, which is the accessor the
    manifest's own docstring tells consumers to ask instead of reading
    ``intents`` x ``chains`` raw. That is the whole point of the field: the
    category rows collapse a connector's exact verbs into one rendering label
    (SUPPLY / BORROW / REPAY / WITHDRAW all become ``lending``), so a consumer
    reading rows alone can ask "is this protocol on this chain?" but never
    "can it do this operation here?".

    Deliberately NOT intersected with the row's chains. A connector can emit
    several rows with different chain sets, and intersecting would make the
    same intent report different coverage depending on which of its rows you
    read — a consumer looking at one row would silently get a narrowed answer.
    The map is a property of the CONNECTOR and is published identically on
    every row it emits; the row's ``chains`` stays the rendering union.

    Intents whose category is withheld (:data:`WITHHELD_CATEGORIES`) are
    dropped. An intent with no surviving chain contributes nothing rather than
    an empty list — an empty claim is not a claim.
    """
    category_of = _intent_category_map()
    coverage: dict[str, list[str]] = {}
    for intent in manifest.intents:
        if intent.name in UNCATEGORISED_INTENTS:
            category: str | None = None
        elif intent in category_of:
            category = category_of[intent]
        else:
            # An intent in neither the category map nor UNCATEGORISED_INTENTS is
            # an unclassified new verb. Fail loudly rather than publish it
            # without a product decision — but with a message an operator or
            # agent can act on, since this path is also reached by the
            # LLM-facing check_protocol_support tool. A bare KeyError naming
            # only the enum member does not say what to do about it.
            raise RuntimeError(
                f"IntentType.{intent.name} is not classified for the support "
                f"matrix. Add it to the intent -> category map in "
                f"support_matrix.py to render a row for it, or to "
                f"UNCATEGORISED_INTENTS to publish per-intent coverage with no "
                f"row. Leaving a verb unclassified would publish it to Edge and "
                f"the Platform without a product decision."
            )
        if category in WITHHELD_CATEGORIES:
            continue
        chains = sorted(_matrix_chain(c) for c in manifest.chains_for_intent(intent))
        if chains:
            coverage[intent.name] = chains
    return coverage


def _published_matrix_names(manifest: Any) -> set[str]:
    """The matrix row names this connector publishes under.

    Usually the connector's own name, but a declared ``matrix_entries`` may
    rename the row (``balancer_v2`` publishes as ``balancer``) or emit several.
    A manifest with ``matrix_entries=()`` publishes nothing and yields the empty
    set — the deliberate full-suppression case, which must not acquire a
    ``chainsByIntent`` entry through the back door.
    """
    if manifest.matrix_entries is not None:
        return {entry.matrix_name for entry in manifest.matrix_entries}
    return {manifest.name}


def _derive_entries_from_intents(
    name: str,
    intents: tuple[Any, ...],
    chains: tuple[str, ...] | None,
    chains_for_intent: Callable[[Any], tuple[str, ...]],
) -> tuple[tuple[str, str, frozenset[str]], ...]:
    """Derive ``(matrix_name, category, chains)`` rows from a manifest's intents.

    Used as the fallback when a :class:`~almanak.connectors._strategy_base.registry.ConnectorManifest`
    has no explicit ``matrix_entries`` field. The intent → category map
    lives here (not on each connector) so the strategy registry stays
    schema-clean — connectors that need anything beyond this default
    declare it explicitly via ``matrix_entries``.

    A connector with multiple intent classes (e.g. Curve has SWAP +
    LP_OPEN + LP_CLOSE) emits multiple rows, one per category. Returns
    an empty tuple for connectors with off-chain venues (``chains is
    None``) since the matrix is on-chain only.

    ``chains_for_intent`` is the manifest's narrowed per-intent chain read
    (``ConnectorManifest.chains_for_intent`` — VIB-6111). A category's chain
    set is the UNION of its intents' narrowed chain sets, so a category
    survives on a chain as long as at least one of its verbs does (Aave V3 on
    mantle keeps its ``lending`` row because SUPPLY / WITHDRAW / REPAY still
    work there, even though BORROW is excluded). A category disappears from a
    chain only when EVERY intent in it is excluded there.

    It is REQUIRED, deliberately (VIB-6111). Making it optional with an
    un-narrowed default meant a future caller that forgot the argument silently
    got pre-exclusion semantics — a quiet widening in the one function whose job
    is truthful narrowing. A missing argument must raise TypeError, not return
    the wrong answer.
    """
    if chains is None:
        return ()

    narrowed: Callable[[Any], tuple[str, ...]] = chains_for_intent

    intent_category = _intent_category_map()

    # Per-category chain union over that category's intents. An intent whose
    # narrowed chain set is empty contributes nothing; a category with no
    # surviving chain at all emits no row (it would be an empty claim).
    category_chains: dict[str, set[str]] = {}
    for intent in intents:
        cat = intent_category.get(intent)
        if cat is None:
            continue
        category_chains.setdefault(cat, set()).update(_matrix_chain(c) for c in narrowed(intent))

    return tuple((name, cat, frozenset(chain_set)) for cat, chain_set in category_chains.items() if chain_set)


def _collect_from_connector_registry(
    entries: dict[tuple[str, str], set[str]],
    authoritative: set[tuple[str, str]],
    narrowed_out: dict[tuple[str, str], set[str]],
    intent_coverage: dict[str, dict[str, list[str]]],
) -> None:
    """Phase A — derive rows from the strategy-side ``ConnectorRegistry``.

    Every connector that calls ``register_connector(...)`` contributes
    rows here. When the manifest declares ``matrix_entries``, those are
    consumed verbatim and the resulting keys are marked
    ``authoritative`` (no compiler-table widening); otherwise rows are
    derived from ``(intents, chains)`` via
    :func:`_derive_entries_from_intents` and the keys stay non-
    authoritative (Phase B can still union compiler-table chains in,
    since derivation only knows what the manifest declared).

    A manifest with ``matrix_entries=()`` (zero declared entries) is
    treated as authoritative-empty: the connector explicitly publishes
    nothing into the matrix. Skipping intent derivation in that case is
    the mechanism connectors use to suppress matrix surfacing for an
    intent they implement but don't want rendered (e.g. Aave's
    flash-loan capability today).
    """
    from almanak.connectors._strategy_base.registry import (
        ConnectorRegistry,
        _import_all_connectors,
    )

    # Strategy-side connectors register lazily on first attribute access;
    # the matrix builder forces a full sweep so every connector folder
    # is represented regardless of whether the rest of the CLI session
    # has touched it. (CI gates the same way.)
    _import_all_connectors()

    for manifest in ConnectorRegistry.all():
        # Schema v2 — attach the connector's per-intent chain coverage to every
        # matrix_name it publishes under. Registered for BOTH branches below,
        # and BEFORE the ``continue``, so a connector that overrides its
        # rendering rows still publishes truthful per-intent coverage: Pendle
        # renders as ``yield`` yet does SWAP / LP_OPEN / LP_CLOSE / WITHDRAW,
        # which is exactly the collapse this field exists to undo. Registered
        # even when empty, so ``intentsKnown`` can distinguish "manifest says
        # nothing is published here" from "no manifest backs this row at all".
        # An OFF-CHAIN venue (``chains is None``) has a structurally empty
        # coverage map, so registering it would publish
        # ``intentsKnown: true, chainsByIntent: {}`` — read by a fail-closed
        # consumer as "supports nothing".
        #
        # Scoped to the COVERAGE registration only. An earlier form used
        # ``continue`` here, which also skipped the row emission and the
        # ``authoritative`` marker below: the venue then published no row at all,
        # which ``executor.py`` reports as "not integrated in the SDK" — a worse
        # demotion than the one being fixed — and Phase B could union
        # compiler-table chains into a key the connector had frozen.
        #
        # Leaving it unregistered yields ``intentsKnown: false``, which is the
        # honest answer: a manifest describes this row, but per-intent coverage
        # is not expressible for an off-chain venue. That IS "unknown".
        if manifest.chains is not None:
            coverage = _connector_intent_coverage(manifest)
            for matrix_name in _published_matrix_names(manifest):
                intent_coverage.setdefault(matrix_name, {}).update(coverage)

        if manifest.matrix_entries is not None:
            for entry in manifest.matrix_entries:
                key = (entry.matrix_name, entry.category)
                entries.setdefault(key, set()).update(_normalize_entry_chains(entry.chains))
                authoritative.add(key)
            # Mark the connector's own name as authoritative — even with
            # zero declared entries the connector has spoken, so Phase B
            # must not widen a key keyed by this connector's name. The
            # marker key uses ``("", manifest.name)`` so it doesn't
            # collide with real (matrix_name, category) entries; Phase B
            # consults the marker directly.
            authoritative.add(("", manifest.name))
            continue
        derived = _derive_entries_from_intents(
            manifest.name,
            manifest.intents,
            manifest.chains,
            manifest.chains_for_intent,
        )
        # Record EXACTLY WHICH CHAINS the exclusions removed, per row (VIB-6111).
        #
        # Phase B unions compiler-table routes into any key it does not consider
        # authoritative, which would silently undo the narrowing. Freezing the
        # whole key stops that, but it over-corrects twice over: first at
        # connector granularity (an exclusion on a LENDING verb froze the SWAP
        # row), and then still at row granularity — a frozen key also loses
        # compiler-table chains the exclusion never mentioned. Both directions
        # under-advertise real support.
        #
        # A per-chain denylist is the precise instrument: let Phase B union
        # whatever it wants, then subtract only the chains the connector
        # actually disclaimed. Computed by diffing the raw derivation against
        # the narrowed one, so there is no intent→category duplication and it
        # stays correct if the category mapping changes.
        raw = _derive_entries_from_intents(
            manifest.name,
            manifest.intents,
            manifest.chains,
            _constant_chains(tuple(manifest.chains or ())),
        )
        raw_by_key = {(n, c): chains for n, c, chains in raw}
        derived_by_key = {(n, c): chains for n, c, chains in derived}
        for key, chain_set in derived_by_key.items():
            entries.setdefault(key, set()).update(chain_set)
        # Iterate the RAW keys, not the derived ones. A category whose every
        # chain is narrowed away emits no derived row at all
        # (``_derive_entries_from_intents`` drops empty categories), so keying
        # off ``derived`` would record no denylist for it and Phase B could
        # recreate the whole row from the routing tables. Defaulting the
        # narrowed set to empty covers both the surviving and the
        # fully-removed case with one expression.
        #
        # NOTE: unreachable under today's validators — a per-entry exclusion
        # covering every declared chain is rejected, so every intent keeps at
        # least one chain and a category union can never be empty. Kept as
        # belt-and-braces because the cost is one dict lookup and the failure
        # mode if a validator ever relaxes is silent re-advertisement. Do NOT
        # add a test that fabricates the shape by bypassing validation.
        for key, raw_chain_set in raw_by_key.items():
            dropped = set(raw_chain_set) - set(derived_by_key.get(key, frozenset()))
            if dropped:
                narrowed_out.setdefault(key, set()).update(dropped)


def _collect_from_compiler_tables(
    entries: dict[tuple[str, str], set[str]],
    authoritative: set[tuple[str, str]],
) -> None:
    """Phase B — compiler-only routes without a dedicated connector folder.

    A handful of legacy DEXes (Uniswap V2, PancakeSwap V2, QuickSwap,
    SushiSwap, Velodrome) and aggregators (1inch) ship in
    ``PROTOCOL_ROUTERS`` but have no connector folder under
    ``almanak/connectors/``. They cannot publish through the registry,
    so the matrix iterates the routing tables as a fallback.

    For ``(matrix_name, category)`` pairs in ``authoritative`` (declared
    by a strategy-side ``matrix_entries`` field), Phase B must NOT widen
    the chain set — the connector's own declaration wins. For non-
    authoritative pairs (those derived from a strategy manifest's
    ``intents`` + ``chains`` only, or absent entirely), Phase B unions
    the compiler-table chains in: the manifest typically declares the
    chains a strategy can use end-to-end, and the routing tables add
    chains that a swap router covers but a strategy lifecycle doesn't
    yet — keeping the union gives the matrix its historical "wherever
    the protocol is routable" view.

    Reads from compiler tables as **data** (dict iteration). The
    protocol names come from the table keys, not from literal strings,
    so no protocol-name string literal leaks into this file.
    """
    from almanak.framework.intents.compiler_constants import (
        BALANCER_VAULT_ADDRESSES,
        LP_POSITION_MANAGERS,
        PROTOCOL_ROUTERS,
    )

    # Aggregator protocol names are emitted in their own category, not
    # ``swap``. The list lives in the compiler routing tables (the only
    # place aggregators appear) so the disambiguation is data-driven.
    aggregator_names = frozenset({"1inch"})

    def _maybe_add(key: tuple[str, str], chain: str) -> None:
        # ``authoritative`` keys (declared via strategy ``matrix_entries``)
        # are never widened — see the docstring. Other keys union the
        # compiler-table chains in. We also block widening when the
        # connector whose name matches the matrix_name has spoken (even
        # with an empty matrix_entries tuple): the
        # ``("", connector_name)`` marker key is consulted directly.
        if key in authoritative:
            return
        if ("", key[0]) in authoritative:
            return
        entries.setdefault(key, set()).add(chain)

    for chain, protos in PROTOCOL_ROUTERS.items():
        matrix_chain = _matrix_chain(chain)
        for proto in protos:
            category = ACTION_AGGREGATOR if proto in aggregator_names else ACTION_SWAP
            _maybe_add((proto, category), matrix_chain)

    for chain, protos in LP_POSITION_MANAGERS.items():
        matrix_chain = _matrix_chain(chain)
        for proto in protos:
            _maybe_add((proto, ACTION_LP), matrix_chain)

    # Balancer V2's vault is the canonical flash-loan venue across every
    # chain it's deployed on. The matrix has historically rendered the
    # row under the bare ``"balancer"`` name (not ``"balancer_v2"``);
    # preserve that. Sourced from compiler_constants as data, not from a
    # literal protocol name in this file.
    balancer_matrix_name = "balancer"  # matrix-display alias; cf. balancer_v2 connector
    for chain in BALANCER_VAULT_ADDRESSES:
        _maybe_add((balancer_matrix_name, ACTION_FLASH_LOAN), _matrix_chain(chain))


def _sort_chains(all_chains: set[str]) -> list[str]:
    """Sort chains in canonical CLI display order, appending unknowns alphabetically.

    The canonical order is owned by
    :data:`~almanak.connectors._strategy_base.registry.MATRIX_CHAIN_DISPLAY_ORDER`
    — the chain-name string literals live under ``almanak/connectors/``
    (the coupling scanner's canonical-home exclusion) so this module
    stays free of per-chain literals. Unknown chains (newly registered
    after the display order was last updated) fall through to
    alphabetical sorting at the tail — forward-compatible default.
    """
    from almanak.connectors._strategy_base.registry import MATRIX_CHAIN_DISPLAY_ORDER

    sorted_chains = [c for c in MATRIX_CHAIN_DISPLAY_ORDER if c in all_chains]
    # Add any chains not in our predefined order
    sorted_chains.extend(sorted(all_chains - set(MATRIX_CHAIN_DISPLAY_ORDER)))
    return sorted_chains


def _category_is_served_by_coverage(category: str, coverage: Mapping[str, Any]) -> bool:
    """Whether any intent in ``coverage`` belongs to ``category``.

    Derived by inverting the same intent -> category map the rest of this module
    uses, so there is no second hand-maintained table to drift.
    """
    category_by_intent_name = {intent.name: cat for intent, cat in _intent_category_map().items()}
    return any(category_by_intent_name.get(intent_name) == category for intent_name in coverage)


def _is_suppressed_phase_b_row(
    key: tuple[str, str],
    authoritative: set[tuple[str, str]],
    intent_coverage: dict[str, dict[str, Any]],
) -> bool:
    """Whether a Phase-B row advertises a category its own declaration cannot serve.

    VIB-6231. ``camelot`` declares ``SWAP`` only, but its address in
    ``LP_POSITION_MANAGERS`` minted an ``lp`` row — so the matrix advertised an LP
    venue whose compiler answers "CamelotCompiler does not support intent type
    IntentType.LP_OPEN". ``chainsByIntent`` was already honest (``{"SWAP": [...]}``
    on both rows), so only a v1 consumer reading ``category`` was misled.

    Two deliberate carve-outs, both of which keep the row:

    * **Declared (`authoritative`) rows.** A ``matrix_entries`` row is a rendering
      override whose category need not match its intents' categories (``enso``
      renders SWAP as ``aggregator``, ``pendle`` renders LP/SWAP as ``yield``).
      Suppressing those deletes them outright.
    * **Connectors with no categorised intent at all.** If every declared intent is
      in ``UNCATEGORISED_INTENTS`` the connector maps to no category, and "serves
      nothing" is indistinguishable from "we cannot tell". The surrounding code
      works hard to keep absent != unsupported, so this fails OPEN. Latent today
      (no connector is purely uncategorised) and kept that way on purpose.
    """
    name, category = key
    coverage = intent_coverage.get(name)
    if not coverage or key in authoritative:
        return False
    category_by_intent_name = {intent.name: cat for intent, cat in _intent_category_map().items()}
    categorised = {category_by_intent_name[i] for i in coverage if i in category_by_intent_name}
    if not categorised:
        return False  # fail open — see docstring
    return category not in categorised


def _build_matrix() -> dict:
    """Build the chains x protocols support matrix from SDK data structures.

    Composition order:

    1. Strategy-side ``ConnectorRegistry`` manifests (uses
       ``matrix_entries`` when declared, derived from intents +
       chains otherwise). Manifests with ``matrix_entries=()``
       intentionally publish nothing.
    2. Compiler routing tables (fallback for protocols without a
       connector folder; only fills ``(matrix_name, category)`` keys no
       upstream phase touched).

    Returns a dict with:
        chains: list of chain names (canonical order, matrix form)
        protocols: list of {name, category, chains, chainsByIntent, intentsKnown}

    ``chains`` and ``chainsByIntent`` answer DIFFERENT questions and are
    deliberately not constrained to match:

    * ``chains`` — the rendering row. May be WIDER than the manifest (Phase B
      unions compiler-routable chains a connector never declared) or NARROWER
      (a declared ``matrix_entries`` row scopes it).
    * ``chainsByIntent`` — the execution truth, from ``chains_for_intent``.
      Published per CONNECTOR and repeated identically on every row that
      connector emits, so reading any single row gives the same answer.

    A chain in ``chains`` that no intent covers therefore means "routable but
    not strategy-verified" — a consumer gating execution must treat it as
    UNKNOWN, not as supported. ``intentsKnown=False`` marks rows no manifest
    describes at all, where an absent intent likewise means unknown rather than
    unsupported.

    Each ``(name, category)`` pair appears at most once; chains union
    across phases so a connector visible to both registries doesn't
    double-up.
    """
    entries: dict[tuple[str, str], set[str]] = {}
    # Keys declared explicitly by a connector (via a strategy-side
    # ``matrix_entries`` field). Phase B must not widen these — the
    # connector's view wins. Other keys are open for compiler-table
    # union. See ``_collect_from_compiler_tables`` for the rationale.
    authoritative: set[tuple[str, str]] = set()
    # (matrix_name, category) -> chains a connector explicitly disclaimed via
    # ``intent_chain_exclusions``. Subtracted AFTER Phase B so a compiler-table
    # route can never re-advertise a cell the connector declared unsupported,
    # while chains the exclusion never mentioned still union normally (VIB-6111).
    narrowed_out: dict[tuple[str, str], set[str]] = {}
    # matrix_name -> {intent_name: [chain, ...]} for every manifest-backed row
    # (schema v2). Rows with no entry here are compiler-table-only or alias rows
    # that no connector manifest describes; they publish ``intentsKnown: false``.
    intent_coverage: dict[str, dict[str, list[str]]] = {}

    _collect_from_connector_registry(entries, authoritative, narrowed_out, intent_coverage)
    _collect_from_compiler_tables(entries, authoritative)

    for key, dropped in narrowed_out.items():
        if key in entries:
            entries[key] -= dropped

    # Drop empty-chain rows — a connector that declared the capability
    # without any chain coverage shouldn't surface as a no-op row.
    entries = {key: chains for key, chains in entries.items() if chains}

    # VIB-6231: drop Phase-B rows whose category the connector's own declaration
    # cannot serve, BEFORE the chain axis is unioned below. Suppressing later (in
    # the row loop) left a suppressed row's chains in ``chains``, so the first
    # suppression of the only row carrying some chain would publish a chain with
    # no protocol row — the phantom-chain class this same change removes.
    entries = {
        key: chains
        for key, chains in entries.items()
        if not _is_suppressed_phase_b_row(key, authoritative, intent_coverage)
    }

    all_chains: set[str] = set()
    for chain_set in entries.values():
        all_chains.update(chain_set)

    # Group entries by category (preserve canonical category order from
    # SUPPORTED_CATEGORIES), then sort alphabetically by name within
    # each category. This is the same order the previous hand-coded
    # build produced.
    protocols: list[dict[str, Any]] = []
    for category in SUPPORTED_CATEGORIES:
        category_entries = [(name, chain_set) for (name, cat), chain_set in entries.items() if cat == category]
        # Sort by ``name`` only — explicit key avoids relying on tuple
        # comparison, which would fall through to ``set`` comparison
        # (uncomparable in Python 3) on duplicate names. Names are
        # de-duplicated upstream (the dict key includes the category)
        # so this guard is belt-and-braces, but it documents intent
        # and removes a fragile implicit contract. (Gemini code review
        # on PR 2469.)
        for name, chain_set in sorted(category_entries, key=lambda pair: pair[0]):
            coverage = intent_coverage.get(name)
            protocols.append(
                {
                    "name": name,
                    "category": category,
                    "chains": sorted(chain_set & all_chains),
                    # Schema v2. See ``_connector_intent_coverage`` for why this
                    # is NOT intersected with ``chains`` above: the two answer
                    # different questions. ``chains`` is the rendering row (it
                    # can be WIDER — Phase B unions compiler-routable chains the
                    # manifest never claimed — or NARROWER when a declared
                    # matrix_entries row scopes it). ``chainsByIntent`` is the
                    # execution truth from ``chains_for_intent``. A chain in
                    # ``chains`` that no intent covers means "routable, not
                    # strategy-verified" — treat it as UNKNOWN, not supported.
                    "chainsByIntent": dict(sorted(coverage.items())) if coverage else {},
                    # False = no connector manifest describes this row at all
                    # (compiler-table-only DEXes and alias rows), so an absent
                    # intent means "unknown", not "unsupported". Distinguishing
                    # these is what stops a fail-closed consumer demoting real
                    # venues it simply has no manifest for.
                    "intentsKnown": name in intent_coverage,
                }
            )

    return {
        "chains": _sort_chains(all_chains),
        "protocols": protocols,
    }


def _sdk_module_suffix() -> Path | None:
    """This module's path relative to an SDK checkout root, or ``None``.

    ``almanak/framework/cli/support_matrix.py``, derived from ``__name__`` so it
    cannot drift if the module moves.

    ``None`` when there is no package path to anchor against — i.e. the module
    was executed directly. An earlier form RAISED here, which propagated straight
    out of the CLI: :func:`_is_sdk_checkout` catches only ``OSError`` and
    :func:`_source_commit` only ``(OSError, UnicodeDecodeError)``, so the raise
    escaped both. That is the same shape as the two exception-tuple bugs already
    fixed on this branch, and a direct contradiction of "provenance degrades,
    never fails".
    """
    if "." not in __name__:  # pragma: no cover - only if run as a script
        return None
    return Path(*__name__.split(".")).with_suffix(".py")


def _is_sdk_checkout(root: Path) -> bool:
    """Whether ``root`` is the checkout THIS module was loaded from.

    The upward walk finds the first ``.git`` above the module file. In the
    standard local-SDK layout the SDK is installed into a user's strategy repo
    (``<user-repo>/.venv/lib/pythonX/site-packages/almanak/...``), so that first
    ``.git`` is the USER'S repository — and provenance would confidently report
    a 40-hex SHA describing an entirely unrelated project, with ``sourceDirty``
    reporting their working tree and ``git status`` running inside their repo.

    False provenance is worse than none: a drift check would compare two
    unrelated repositories and report match or mismatch with equal
    meaninglessness. So a candidate is accepted only when it actually contains
    this module at its package path.

    Known, accepted false negative: a NON-editable (copy) install into a venv
    that happens to sit inside the SDK checkout loses provenance, because the
    installed copy's path is not the source path it is compared against. That
    degrades to ``None``, never to wrong data, and "an installed copy is a build,
    not a checkout" is arguably the correct answer. Editable installs (PEP 660,
    legacy egg-link, and ``uv sync``'s workspace editable) all report real source
    paths and resolve normally.
    """
    suffix = _sdk_module_suffix()
    if suffix is None:
        return False
    try:
        return (root / suffix).resolve() == Path(__file__).resolve()
    except OSError:
        return False


def _checkout_root(start: Path | None = None) -> Path | None:
    """The directory CONTAINING ``.git`` — i.e. the working tree root.

    Distinct from :func:`_git_dir`, and the distinction is load-bearing for a
    linked worktree: there ``.git`` is a FILE and the git dir resolves to
    ``<main>/.git/worktrees/<name>``, whose parent is inside ``.git`` rather
    than a checkout. Deriving a working-tree path from the git dir is therefore
    wrong exactly where this repo does much of its work.

    When ``start`` is omitted (the production path) the result must be THIS
    SDK's checkout — see :func:`_is_sdk_checkout`. An explicit ``start`` skips
    that anchor so tests can exercise the walk against fixture trees.
    """
    for parent in (start or Path(__file__)).resolve().parents:
        candidate = parent / ".git"
        if candidate.is_dir() or candidate.is_file():
            if start is None and not _is_sdk_checkout(parent):
                return None
            return parent
    return None


def _git_dir(start: Path | None = None) -> Path | None:
    """Locate this checkout's git directory, or ``None`` when there isn't one.

    ``start`` is a FILE path to walk up from; it defaults to this module and
    exists so the search is testable against a fixture tree rather than only
    against whatever checkout the tests happen to run in.
    """
    for parent in (start or Path(__file__)).resolve().parents:
        candidate = parent / ".git"
        if (candidate.is_dir() or candidate.is_file()) and start is None and not _is_sdk_checkout(parent):
            # Found a repository, but not the one this module lives in — see
            # _is_sdk_checkout. Refuse rather than describe someone else's repo.
            return None
        if candidate.is_dir():
            return candidate
        if candidate.is_file():
            # Linked worktree / submodule: the file points at the real git dir.
            try:
                text = candidate.read_text(encoding="utf-8").strip()
            except (OSError, UnicodeDecodeError):
                # Callers other than _source_commit (e.g. _worktree_dirty) reach
                # this without a try, so degrade here rather than propagate.
                return None
            if not text.startswith("gitdir:"):
                return None
            resolved = Path(text.split(":", 1)[1].strip())
            if not resolved.is_absolute():
                resolved = (parent / resolved).resolve()
            return resolved if resolved.is_dir() else None
    return None


def _ref_search_dirs(git_dir: Path) -> list[Path]:
    """``git_dir`` plus the shared common dir, when it is a linked worktree.

    A worktree's own git dir holds HEAD but not ``refs/heads`` — those live in
    the main checkout's git dir, named by the ``commondir`` file. Without this,
    provenance would silently degrade to ``None`` for every agent worktree,
    which is where a good deal of this repo's work actually happens.
    """
    dirs = [git_dir]
    commondir = git_dir / "commondir"
    if commondir.is_file():
        shared = Path(commondir.read_text(encoding="utf-8").strip())
        if not shared.is_absolute():
            shared = (git_dir / shared).resolve()
        if shared.is_dir():
            dirs.append(shared)
    return dirs


def _resolve_ref(git_dir: Path, ref: str) -> str | None:
    """Resolve ``refs/heads/x`` to a SHA via loose refs, then ``packed-refs``.

    The ref is taken from ``.git/HEAD``, and ``pathlib``'s ``/`` lets an
    ABSOLUTE right operand replace the left entirely — so ``ref: /etc/passwd``
    would read that path, and ``..`` segments would escape the git dir.
    ``_valid_sha`` means nothing but 40/64-hex is ever published, so there is no
    disclosure channel, and writing ``.git/HEAD`` already implies control of the
    checkout; this is hardening, not a patch for a live hole.
    """
    if not ref.startswith("refs/") or ".." in Path(ref).parts or Path(ref).is_absolute():
        return None
    for search_dir in _ref_search_dirs(git_dir):
        loose = search_dir / ref
        if loose.is_file():
            try:
                return loose.read_text(encoding="utf-8").strip() or None
            except UnicodeDecodeError:
                return None
        packed = search_dir / "packed-refs"
        if not packed.is_file():
            continue
        try:
            packed_lines = packed.read_text(encoding="utf-8").splitlines()
        except UnicodeDecodeError:
            return None
        for line in packed_lines:
            if line.startswith(("#", "^")):
                continue
            sha, _, name = line.partition(" ")
            if name.strip() == ref:
                return sha.strip() or None
    return None


def _source_commit(start: Path | None = None) -> str | None:
    """The commit this matrix was generated from, or ``None`` if unresolvable.

    Provenance for consumers that vendor the artifact — Edge has had a vendored
    copy drift from the SDK it claimed to describe, and ``sdkVersion`` alone
    cannot separate two builds of the same rc.

    Read straight from ``.git`` with plain file I/O — no subprocess and no
    egress on THIS path. (Its sibling :func:`_worktree_dirty` does shell out to
    a local ``git``; that is a deliberate, documented exception confined to the
    dirty-check, not a property of provenance reads generally.) Degrades to
    ``None`` in an installed wheel (no ``.git``), where ``sdkVersion`` is the
    authoritative identifier anyway. ``None`` always means "not resolvable
    here" — never a guess, and never a stale value from another checkout.
    """
    try:
        git_dir = _git_dir(start)
        if git_dir is None:
            return None
        head = (git_dir / "HEAD").read_text(encoding="utf-8").strip()
        if head.startswith("ref:"):
            return _valid_sha(_resolve_ref(git_dir, head.split(":", 1)[1].strip()))
        return _valid_sha(head)
    except (OSError, UnicodeDecodeError):
        # UnicodeDecodeError is NOT an OSError subclass, so catching OSError
        # alone let a HEAD / ref / packed-refs file with malformed encoding
        # crash `almanak info matrix --json` outright — the opposite of the
        # "provenance degrades, never fails" contract this function documents.
        return None


def _valid_sha(value: str | None) -> str | None:
    """``value`` if it is a git object id, else ``None``.

    Provenance must be correct or explicitly absent — never a guess. Without
    this, any non-empty text in ``HEAD``, a loose ref, or ``packed-refs``
    (a truncated write, an editor's stray newline, ``ref: refs/heads/x`` written
    to a ref file by mistake) would be published verbatim as ``sourceCommit``.
    A consumer diffing a vendored copy against the SDK it claims to describe
    would then be comparing against a value that identifies no build at all,
    which is worse than reporting nothing.

    Accepts SHA-1 (40 hex) and SHA-256 (64 hex) object ids, so a repository
    using the newer hash keeps working.
    """
    if value is None:
        return None
    candidate = value.strip()
    if len(candidate) in (40, 64) and all(c in "0123456789abcdef" for c in candidate.lower()):
        return candidate
    return None


def _worktree_dirty() -> bool | None:
    """Whether the checkout has uncommitted changes; ``None`` if undeterminable.

    ``sourceCommit`` alone over-claims: a matrix generated from a dirty checkout
    reflects uncommitted connector or registry edits, but still reports HEAD — so
    a vendored artifact would name a commit that cannot reproduce its contents,
    which is exactly the drift the provenance envelope exists to expose.

    Published as a separate flag rather than by nulling ``sourceCommit``, because
    the base commit is still useful information and nulling would erase it — and
    because almost every dev checkout carries incidental dirt (a lockfile, a
    scratch doc) that says nothing about whether the matrix itself is
    reproducible. A consumer diffing a vendored copy reads ``sourceCommit`` for
    the base and ``sourceDirty`` to know whether exact reproduction is expected.

    Uses ``git status --porcelain`` with no shell and a short timeout. That is a
    LOCAL read — not egress — and this module is an operator-facing CLI, not a
    strategy-container hot path. Any failure (no git, not a checkout, timeout,
    installed wheel) yields ``None``: "not determinable", never a guess.
    """
    import subprocess  # noqa: PLC0415 - local, CLI-only, deliberately not top-level

    root = _checkout_root()
    if root is None:
        return None
    try:
        completed = subprocess.run(  # noqa: S603 - fixed argv, no shell, no user input
            # --no-optional-locks: `git status` otherwise refreshes the index and
            # takes index.lock. A read-only annotation must not write to .git,
            # and under concurrent git activity the contention would degrade this
            # to None — safe, but silently losing the signal.
            ["git", "--no-optional-locks", "status", "--porcelain"],  # noqa: S607
            cwd=str(root),
            capture_output=True,
            text=True,
            timeout=10,
            check=False,
        )
    except (OSError, subprocess.SubprocessError):
        return None
    if completed.returncode != 0:
        return None
    return bool(completed.stdout.strip())


def _sdk_version() -> str | None:
    """The installed SDK version, or ``None`` if it cannot be determined."""
    try:
        from almanak import __version__

        return __version__
    except Exception:  # pragma: no cover - defensive; __version__ is generated
        return None


def _render_table(data: dict) -> str:
    """Render the matrix as an ASCII table."""
    chains = data["chains"]
    protocols = data["protocols"]

    # Column widths
    name_width = max(len(p["name"]) for p in protocols) + 2
    cat_width = max(len(p["category"]) for p in protocols) + 2
    chain_width = max(max(len(c) for c in chains), 4) + 1

    # Header
    lines: list[str] = []
    header = f"{'Protocol':<{name_width}} {'Category':<{cat_width}} "
    header += " ".join(f"{c:^{chain_width}}" for c in chains)
    lines.append(header)
    lines.append("-" * len(header))

    # Group by category
    current_cat = ""
    for proto in protocols:
        if proto["category"] != current_cat:
            current_cat = proto["category"]
            if lines[-1] != "-" * len(header):
                lines.append("")

        row = f"{proto['name']:<{name_width}} {proto['category']:<{cat_width}} "
        cells = []
        for chain in chains:
            supported = chain in proto["chains"]
            cells.append(f"{'  Y':^{chain_width}}" if supported else f"{'  -':^{chain_width}}")
        row += " ".join(cells)
        lines.append(row)

    # Summary
    lines.append("")
    lines.append(
        f"Chains: {len(chains)}  |  Protocols: {len(protocols)}  |  "
        f"Supported pairs: {sum(len(p['chains']) for p in protocols)}"
    )

    return "\n".join(lines)


@click.command("matrix")
@click.option("--json", "as_json", is_flag=True, default=False, help="Output as JSON (for programmatic consumption).")
@click.option(
    "--category",
    "-c",
    type=str,
    default=None,
    help=f"Filter by category ({', '.join(SUPPORTED_CATEGORIES)}).",
)
@click.option("--chain", type=str, default=None, help="Filter by chain name.")
@click.option("--protocol", "-p", type=str, default=None, help="Filter by protocol name (partial match).")
def support_matrix(as_json: bool, category: str | None, chain: str | None, protocol: str | None) -> None:
    """Show the chains x protocols support matrix.

    Dynamically derived from the SDK's registries and compiler routing
    tables. Always reflects the current state of the codebase — adding a
    new connector folder under ``almanak/connectors/`` makes it appear
    here automatically.

    Examples:

    \b
        almanak info matrix                  # Pretty table
        almanak info matrix --json           # JSON for Edge/CI
        almanak info matrix -c swap          # Only swap protocols
        almanak info matrix --chain arbitrum  # Only Arbitrum support
        almanak info matrix -p uniswap       # Only Uniswap protocols
    """
    data = _build_matrix()

    # Apply filters
    if category:
        data["protocols"] = [p for p in data["protocols"] if p["category"] == category.lower()]
    if chain:
        # Canonicalize the user's spelling: rows are written canonically by
        # _matrix_chain, so a registered alias (`bnb` -> `bsc`) previously
        # reported "no support" for a fully supported chain. Every other chain
        # surface in the SDK accepts aliases.
        chain_lower = _matrix_chain(chain.strip().lower())
        data["protocols"] = [p for p in data["protocols"] if chain_lower in p["chains"]]
        # Trim each protocol's chains list to only the filtered chain
        for p in data["protocols"]:
            p["chains"] = [c for c in p["chains"] if c == chain_lower]
            # Keep the v2 map consistent with the trimmed row. An intent with no
            # coverage on the filtered chain drops out entirely rather than
            # rendering as an empty list — an empty claim is not a claim, and a
            # row can survive this filter on a chain that is routable but that
            # no intent covers, which must read as {} (unknown), not as support.
            p["chainsByIntent"] = {
                intent: [c for c in chains if c == chain_lower]
                for intent, chains in p["chainsByIntent"].items()
                if chain_lower in chains
            }
        data["chains"] = [c for c in data["chains"] if c == chain_lower]
    if protocol:
        protocol_lower = protocol.lower()
        data["protocols"] = [p for p in data["protocols"] if protocol_lower in p["name"].lower()]

    if not data["protocols"] and not as_json:
        # Prose belongs on the table path only. Under --json, emitting nothing
        # gave zero bytes on stdout with exit 0, so `json.loads` / `jq` failed
        # and could not tell "no matches" from "the command is broken".
        click.echo("No protocols match the given filters.", err=True)
        return

    if as_json:
        # Clean output for programmatic consumption. The provenance envelope is
        # added HERE rather than in ``_build_matrix`` so that builder stays
        # deterministic and pure for its other consumers (the table renderer,
        # the agent tool, and the tests) — ``generatedAt`` would otherwise make
        # every one of them non-reproducible.
        source_commit = _source_commit()
        output = {
            "schemaVersion": SCHEMA_VERSION,
            "sdkVersion": _sdk_version(),
            "sourceCommit": source_commit,
            # True = the checkout had uncommitted changes, so this artifact is
            # NOT reproducible from sourceCommit alone. None = undeterminable.
            # Gated on sourceCommit: the flag QUALIFIES that commit ("is this
            # artifact reproducible from it?"), so publishing it without one
            # says nothing. Reachable via a valid .git pointer with an
            # unreadable HEAD, where _git_dir degrades but git status succeeds.
            "sourceDirty": _worktree_dirty() if source_commit is not None else None,
            "generatedAt": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
            "chains": data["chains"],
            "protocols": data["protocols"],
        }
        click.echo(json_module.dumps(output, indent=2))
    else:
        click.echo(_render_table(data))
