"""Manifest-derived **capability matrix** — advisory computed view (VIB-5112).

This module is **phase 1** of G1/D3 in the Demo-E2E plan
(``docs/internal/qa/ToDo-DemoE2E-June27.md`` §D3/§G1): a *read-only view* over
the capability declarations the connector manifest already carries. It is
**advisory only** — nothing here fails a build, marks a valuer ``no_path``, or
gates the runner. Those are later phases (consumers, then a CI ``unknown``
gate). See ``docs/internal/qa/capability-matrix-g1-phase1.md`` for the phasing.

Where the existing ``almanak info matrix`` (``support_matrix.py``) answers
"which (protocol, category, chain) triples are routable?", this view answers the
*deeper* question the plan's §D symptoms all share: for each declared
``(protocol, chain, intent)`` cell, which **supporting capabilities** —
rate, valuation, accounting, safety-floor, demo-coverage — are actually
**declared**, and which are silently **unknown** (the "discovered-at-runtime
through a raise or a \\$0" failure mode).

Derivation sources (all strategy-safe manifest fields on
:class:`~almanak.connectors._connector.Connector`; no ``ImportRef.load()`` is
called, so no gateway-only module is imported):

============== ==========================================================
capability     derived from
============== ==========================================================
compile        ``strategy_intents`` × ``strategy_chains`` defines the
               universe; every declared cell compiles.
execute        co-declared with ``compile`` in phase 1 (the manifest has no
               separate execute decl); a later phase binds this to demo /
               intent-test evidence.
rate           ``lending_read.rate_history_chains`` (strategy-side mirror of
               the gateway ``GatewayLendingRateHistoryCapability``).
valuation      ``lending_read`` presence (lending). LP / vault / perp have no
               ``position_read`` decl yet → ``unknown`` (this gap is the
               whole point of the view).
accounting     presence of a money-leg / accounting decl
               (``receipt_parser_connector`` / ``lending_read`` /
               ``accounting_treatment`` / ``primitive``).
safety-floor   best-effort: swap min-out via ``swap_quote_connector``;
               LP min-lp is not derivable from the manifest → ``unknown``.
demo-coverage  a demo in the ``DemoCatalog`` covering this protocol+chain
               (``quarantined`` if the only covering demo is quarantined).
============== ==========================================================

States (advisory; none fails anything in phase 1):

* ``supported`` — a relevant decl is present.
* ``unsupported-explicit`` — declared off-chain / not-applicable venue.
* ``quarantined`` — covered only by a quarantined demo (with ``until`` expiry).
* ``unknown`` — **no declaration either way.** In phase 1 this is *reported*,
  not failed. The phase-4 CI gate is what would later flip reachable
  ``unknown`` cells to red — that is out of scope here.

The ``token-class`` and ``primitive`` dimensions from the full §D3 spec are a
documented TODO; phase 1 keys on ``protocol × chain × intent × capability``.
"""

from __future__ import annotations

import json as json_module
from collections.abc import Iterable
from dataclasses import dataclass

import click

from almanak.core.chains import ChainRegistry

# ---------------------------------------------------------------------------
# Capability + state vocabulary
# ---------------------------------------------------------------------------
CAP_COMPILE = "compile"
CAP_EXECUTE = "execute"
CAP_RATE = "rate"
CAP_VALUATION = "valuation"
CAP_ACCOUNTING = "accounting"
CAP_SAFETY_FLOOR = "safety-floor"
CAP_DEMO_COVERAGE = "demo-coverage"

# Canonical capability display order (matches the §D3 spec ordering).
CAPABILITIES: tuple[str, ...] = (
    CAP_COMPILE,
    CAP_EXECUTE,
    CAP_RATE,
    CAP_VALUATION,
    CAP_ACCOUNTING,
    CAP_SAFETY_FLOOR,
    CAP_DEMO_COVERAGE,
)

STATE_SUPPORTED = "supported"
STATE_UNSUPPORTED = "unsupported-explicit"
STATE_QUARANTINED = "quarantined"
STATE_UNKNOWN = "unknown"

# Intent-category buckets — the capability applicability map keys on these
# rather than on individual intent verbs so a new lending/LP/vault verb inherits
# the right applicability automatically.
_CAT_LENDING = "lending"
_CAT_LP = "lp"
_CAT_YIELD = "yield"
_CAT_PERP = "perp"
_CAT_SWAP = "swap"
_CAT_OTHER = "other"

# Intent verb (string, as declared in ``strategy_intents``) → category.
# Mirrors the dispatch in ``support_matrix._derive_entries_from_intents`` but
# keyed by the raw manifest string so this module never imports the framework
# intent vocabulary (keeps the derivation strategy-safe + cheap).
_INTENT_CATEGORY: dict[str, str] = {
    "SUPPLY": _CAT_LENDING,
    "BORROW": _CAT_LENDING,
    "REPAY": _CAT_LENDING,
    "WITHDRAW": _CAT_LENDING,
    "LP_OPEN": _CAT_LP,
    "LP_CLOSE": _CAT_LP,
    "LP_COLLECT_FEES": _CAT_LP,
    "STAKE": _CAT_YIELD,
    "UNSTAKE": _CAT_YIELD,
    "VAULT_DEPOSIT": _CAT_YIELD,
    "VAULT_REDEEM": _CAT_YIELD,
    "PERP_OPEN": _CAT_PERP,
    "PERP_CLOSE": _CAT_PERP,
    "SWAP": _CAT_SWAP,
}

# Which capabilities are *applicable* to which intent categories. A cell is only
# emitted for applicable (intent, capability) pairs — a SWAP has no meaningful
# "rate" cell, so we don't drown the view in meaningless ``unknown`` rows.
# compile / execute / accounting / demo-coverage apply to every intent.
_RATE_CATEGORIES = frozenset({_CAT_LENDING, _CAT_YIELD})
_VALUATION_CATEGORIES = frozenset({_CAT_LENDING, _CAT_LP, _CAT_YIELD, _CAT_PERP})
_SAFETY_FLOOR_CATEGORIES = frozenset({_CAT_SWAP, _CAT_LP})


def _intent_category(intent: str) -> str:
    return _INTENT_CATEGORY.get(intent, _CAT_OTHER)


def _capability_applies(capability: str, category: str) -> bool:
    """Whether ``capability`` is meaningful for an intent in ``category``."""
    if capability in (CAP_COMPILE, CAP_EXECUTE, CAP_ACCOUNTING, CAP_DEMO_COVERAGE):
        return True
    if capability == CAP_RATE:
        return category in _RATE_CATEGORIES
    if capability == CAP_VALUATION:
        return category in _VALUATION_CATEGORIES
    if capability == CAP_SAFETY_FLOOR:
        return category in _SAFETY_FLOOR_CATEGORIES
    return False


# ---------------------------------------------------------------------------
# Result shapes
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class CapabilityCell:
    """One ``protocol × chain × intent × capability`` derivation result.

    ``state`` is one of the four advisory states; ``reason`` is a short,
    human-readable note naming the manifest decl the state was derived from.
    """

    protocol: str
    chain: str
    intent: str
    capability: str
    state: str
    reason: str

    def to_dict(self) -> dict[str, str]:
        return {
            "protocol": self.protocol,
            "chain": self.chain,
            "intent": self.intent,
            "capability": self.capability,
            "state": self.state,
            "reason": self.reason,
        }


@dataclass(frozen=True)
class CapabilityMatrix:
    """Computed capability matrix — an immutable list of cells."""

    cells: tuple[CapabilityCell, ...]

    def counts_by_state(self) -> dict[str, int]:
        """Tally cells per state (for the advisory summary line)."""
        counts: dict[str, int] = {
            STATE_SUPPORTED: 0,
            STATE_UNSUPPORTED: 0,
            STATE_QUARANTINED: 0,
            STATE_UNKNOWN: 0,
        }
        for cell in self.cells:
            counts[cell.state] = counts.get(cell.state, 0) + 1
        return counts

    def to_dict(self) -> dict[str, object]:
        return {
            "capabilities": list(CAPABILITIES),
            "states": [STATE_SUPPORTED, STATE_UNSUPPORTED, STATE_QUARANTINED, STATE_UNKNOWN],
            "cells": [c.to_dict() for c in self.cells],
            "summary": self.counts_by_state(),
        }


# ---------------------------------------------------------------------------
# Chain normalisation (shared convention with support_matrix.py)
# ---------------------------------------------------------------------------
def _matrix_chain(chain: str) -> str:
    """Normalise a manifest chain name to the matrix's canonical form.

    The strategy-side manifest uses ``"bnb"`` for BNB Chain; the matrix renders
    ``"bsc"``. Resolving via :class:`~almanak.core.chains.ChainRegistry` keeps
    both surfaces consistent. Unknown chains pass through unchanged.
    """
    descriptor = ChainRegistry.try_resolve(chain)
    return descriptor.name if descriptor is not None else chain


# ---------------------------------------------------------------------------
# Demo coverage index
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class _DemoCoverage:
    """Pre-computed ``(protocol, chain) -> covering demo names`` index.

    Separated from the connector manifest so the per-capability derivation can
    be unit-tested with a hand-built index (no filesystem walk required).
    """

    # (protocol_lower, chain_norm) -> tuple of demo directory names
    by_protocol_chain: dict[tuple[str, str], tuple[str, ...]]
    # demo name -> frozenset of normalised chains the demo is quarantined on.
    # The ``scripts/ci/demo-quarantine.yml`` schema requires a chain per entry
    # (``Quarantine.load`` rejects a chain-less entry), so quarantine is always
    # per-(demo, chain) — there is no "all chains" sentinel.
    quarantined: dict[str, frozenset[str]]

    def covering_demos(self, protocols: Iterable[str], chain: str) -> tuple[str, ...]:
        """Demos covering any of ``protocols`` on ``chain``."""
        found: list[str] = []
        for protocol in protocols:
            found.extend(self.by_protocol_chain.get((protocol.lower(), chain), ()))
        # De-dup while preserving order.
        return tuple(dict.fromkeys(found))

    def is_quarantined(self, demo: str, chain: str) -> bool:
        return chain in self.quarantined.get(demo, frozenset())


def _build_demo_coverage(demo_catalog: object, quarantine: object) -> _DemoCoverage:
    """Index a ``DemoCatalog`` + ``Quarantine`` into a ``_DemoCoverage``.

    Typed as ``object`` to keep this module import-light; the only attributes
    consumed are ``DemoCatalog.specs`` (each ``DemoSpec`` exposes ``name``,
    ``supported_protocols``, ``supported_chains``) and ``Quarantine.find``.

    Quarantine is matched **per (demo, chain)** using the demo's *declared*
    chain string (the same vocabulary ``demo-quarantine.yml`` keys on), then
    stored under the matrix-normalised chain so ``is_quarantined`` lines up with
    the rendered rows.
    """
    by_protocol_chain: dict[tuple[str, str], list[str]] = {}
    quarantined: dict[str, set[str]] = {}
    find = getattr(quarantine, "find", None)

    for spec in getattr(demo_catalog, "specs", []) or []:
        name = getattr(spec, "name", "")
        declared_chains = getattr(spec, "supported_chains", ()) or ()
        for protocol in getattr(spec, "supported_protocols", ()) or ():
            for declared in declared_chains:
                by_protocol_chain.setdefault((protocol.lower(), _matrix_chain(declared)), []).append(name)
        if callable(find):
            for declared in declared_chains:
                if find(name, declared) is not None:
                    quarantined.setdefault(name, set()).add(_matrix_chain(declared))

    return _DemoCoverage(
        by_protocol_chain={k: tuple(v) for k, v in by_protocol_chain.items()},
        quarantined={k: frozenset(v) for k, v in quarantined.items()},
    )


# ---------------------------------------------------------------------------
# Per-capability derivation (pure — the unit-test surface)
# ---------------------------------------------------------------------------
def _rate_state(connector: object, chain: str) -> tuple[str, str]:
    lending_read = getattr(connector, "lending_read", None)
    if lending_read is None:
        return STATE_UNKNOWN, "no lending_read decl (no GatewayLendingRateHistoryCapability mirror)"
    rate_chains = {_matrix_chain(c) for c in (getattr(lending_read, "rate_history_chains", ()) or ())}
    if chain in rate_chains:
        return STATE_SUPPORTED, "lending_read.rate_history_chains declares this chain"
    return STATE_UNKNOWN, "lending_read present but rate_history_chains omits this chain"


def _valuation_state(connector: object, category: str) -> tuple[str, str]:
    if category == _CAT_LENDING:
        if getattr(connector, "lending_read", None) is not None:
            return STATE_SUPPORTED, "lending_read decl present"
        return STATE_UNKNOWN, "lending intent but no lending_read decl"
    # LP / vault / perp: no position_read decl exists in the manifest yet.
    return STATE_UNKNOWN, "no position_read decl exists yet (phase-1 expected gap)"


def _accounting_state(connector: object) -> tuple[str, str]:
    if getattr(connector, "receipt_parser_connector", None) is not None:
        return STATE_SUPPORTED, "receipt_parser_connector decl present (money-leg extraction)"
    if getattr(connector, "lending_read", None) is not None:
        return STATE_SUPPORTED, "lending_read decl present (shared lending money legs)"
    if getattr(connector, "accounting_treatment", None) is not None:
        return STATE_SUPPORTED, "accounting_treatment decl present"
    if getattr(connector, "primitive", None) is not None:
        return STATE_SUPPORTED, "primitive decl present"
    return STATE_UNKNOWN, "no receipt_parser / lending_read / accounting_treatment / primitive decl"


def _safety_floor_state(connector: object, category: str) -> tuple[str, str]:
    if category == _CAT_SWAP:
        if getattr(connector, "swap_quote_connector", None) is not None:
            return STATE_SUPPORTED, "swap_quote_connector decl present (min-out quote source)"
        return STATE_UNKNOWN, "swap intent but no swap_quote_connector decl (best-effort)"
    # LP min-lp floors are not derivable from a manifest field today.
    return STATE_UNKNOWN, "no min-lp floor decl derivable from manifest (best-effort)"


def _demo_coverage_state(
    protocol_keys: Iterable[str],
    chain: str,
    coverage: _DemoCoverage,
) -> tuple[str, str]:
    demos = coverage.covering_demos(protocol_keys, chain)
    if not demos:
        return STATE_UNKNOWN, "no demo covers this protocol/chain"
    live = [d for d in demos if not coverage.is_quarantined(d, chain)]
    if live:
        return STATE_SUPPORTED, f"covered by demo(s): {', '.join(live)}"
    return STATE_QUARANTINED, f"only quarantined demo(s) cover this: {', '.join(demos)}"


def _capability_state(
    capability: str,
    connector: object,
    chain: str,
    category: str,
    protocol_keys: Iterable[str],
    coverage: _DemoCoverage,
) -> tuple[str, str]:
    """Dispatch one capability to its derivation. Returns ``(state, reason)``."""
    if capability == CAP_COMPILE:
        return STATE_SUPPORTED, "declared in strategy_intents × strategy_chains"
    if capability == CAP_EXECUTE:
        return STATE_SUPPORTED, "co-declared with compile (phase-1: no separate execute evidence)"
    if capability == CAP_RATE:
        return _rate_state(connector, chain)
    if capability == CAP_VALUATION:
        return _valuation_state(connector, category)
    if capability == CAP_ACCOUNTING:
        return _accounting_state(connector)
    if capability == CAP_SAFETY_FLOOR:
        return _safety_floor_state(connector, category)
    if capability == CAP_DEMO_COVERAGE:
        return _demo_coverage_state(protocol_keys, chain, coverage)
    raise ValueError(f"unknown capability {capability!r}")


def _connector_protocol_keys(connector: object) -> tuple[str, ...]:
    """Names a demo might use to reference this connector (name + aliases)."""
    name = getattr(connector, "name", None) or ""
    aliases = getattr(connector, "aliases", None) or ()
    return (name, *aliases)


def _cells_for_connector(connector: object, coverage: _DemoCoverage) -> list[CapabilityCell]:
    """Every applicable capability cell this connector contributes."""
    intents = getattr(connector, "strategy_intents", None)
    chains = getattr(connector, "strategy_chains", None)
    name = getattr(connector, "name", None) or ""
    if not intents:
        return []
    protocol_keys = _connector_protocol_keys(connector)

    cells: list[CapabilityCell] = []
    if chains is None:
        # Off-chain venue (e.g. Kraken): on-chain capabilities are explicitly
        # not applicable. Surface one row per intent so the venue is visible.
        cells.extend(_offchain_cells(name, intents))
        return cells

    norm_chains = [_matrix_chain(c) for c in chains]
    for intent in intents:
        category = _intent_category(intent)
        for chain in norm_chains:
            for capability in CAPABILITIES:
                if not _capability_applies(capability, category):
                    continue
                state, reason = _capability_state(capability, connector, chain, category, protocol_keys, coverage)
                cells.append(
                    CapabilityCell(
                        protocol=name,
                        chain=chain,
                        intent=intent,
                        capability=capability,
                        state=state,
                        reason=reason,
                    )
                )
    return cells


def _offchain_cells(name: str, intents: Iterable[str]) -> list[CapabilityCell]:
    """Capability rows for an off-chain venue (``strategy_chains is None``)."""
    cells: list[CapabilityCell] = []
    for intent in intents:
        for capability in CAPABILITIES:
            cells.append(
                CapabilityCell(
                    protocol=name,
                    chain="(off-chain)",
                    intent=intent,
                    capability=capability,
                    state=STATE_UNSUPPORTED,
                    reason="off-chain venue (strategy_chains=None)",
                )
            )
    return cells


def _discover_connectors() -> tuple[object, ...]:
    """Discover connectors with strategy support from the descriptor registry."""
    from almanak.connectors._connector import CONNECTOR_DESCRIPTOR_REGISTRY

    return CONNECTOR_DESCRIPTOR_REGISTRY.with_strategy_support()


def _discover_demo_coverage() -> _DemoCoverage:
    """Build the demo-coverage index from the on-disk demo catalog."""
    from almanak.framework.demos.quarantine import Quarantine
    from almanak.framework.demos.spec import DemoCatalog

    catalog = DemoCatalog.discover()
    try:
        quarantine: object | None = Quarantine.load_default()
    except (FileNotFoundError, ValueError, OSError):
        quarantine = None
    return _build_demo_coverage(catalog, quarantine)


def build_capability_matrix(
    connectors: Iterable[object] | None = None,
    demo_coverage: _DemoCoverage | None = None,
) -> CapabilityMatrix:
    """Compute the advisory capability matrix.

    The derivation itself is pure (no writes, no network, deterministic from its
    inputs). With both arguments injected it touches nothing else — that is the
    unit-test surface. When omitted, discovery has the import cost the SDK's
    registries already pay: ``_discover_connectors`` imports connector manifests
    and ``_discover_demo_coverage`` walks the demo catalog (which imports each
    demo's ``strategy.py`` for its ``@almanak_strategy`` metadata — the sanctioned
    metadata source per AGENTS.md; read-only, no chain/gateway calls). Both are
    injectable so callers (and tests) can supply pre-built inputs and avoid that
    import cost entirely.
    """
    if connectors is None:
        connectors = _discover_connectors()
    if demo_coverage is None:
        demo_coverage = _discover_demo_coverage()

    cells: list[CapabilityCell] = []
    for connector in connectors:
        cells.extend(_cells_for_connector(connector, demo_coverage))

    cells.sort(key=lambda c: (c.protocol, c.chain, c.intent, CAPABILITIES.index(c.capability)))
    return CapabilityMatrix(cells=tuple(cells))


# ---------------------------------------------------------------------------
# Rendering
# ---------------------------------------------------------------------------
_STATE_GLYPH = {
    STATE_SUPPORTED: "Y",
    STATE_UNSUPPORTED: "x",
    STATE_QUARANTINED: "Q",
    STATE_UNKNOWN: "?",
}


def _render_table(matrix: CapabilityMatrix) -> str:
    """Render the matrix as one row per (protocol, chain, intent), capabilities as columns."""
    if not matrix.cells:
        return "No capability cells (no connectors with strategy support)."

    # Group cells by (protocol, chain, intent) row key.
    rows: dict[tuple[str, str, str], dict[str, str]] = {}
    for cell in matrix.cells:
        rows.setdefault((cell.protocol, cell.chain, cell.intent), {})[cell.capability] = cell.state

    row_keys = sorted(rows)
    proto_w = max(len(k[0]) for k in row_keys) + 2
    chain_w = max(max(len(k[1]) for k in row_keys), 5) + 1
    intent_w = max(len(k[2]) for k in row_keys) + 1
    # Abbreviated capability headers keep the table narrow.
    headers = [(_cap_header(c), c) for c in CAPABILITIES]
    cap_w = max(len(h) for h, _ in headers) + 1

    lines: list[str] = []
    header = f"{'Protocol':<{proto_w}} {'Chain':<{chain_w}} {'Intent':<{intent_w}} "
    header += " ".join(f"{h:^{cap_w}}" for h, _ in headers)
    lines.append(header)
    lines.append("-" * len(header))

    for proto, chain, intent in row_keys:
        states = rows[(proto, chain, intent)]
        row = f"{proto:<{proto_w}} {chain:<{chain_w}} {intent:<{intent_w}} "
        cells_render = []
        for _, capability in headers:
            state = states.get(capability)
            glyph = _STATE_GLYPH.get(state, " ") if state is not None else "."
            cells_render.append(f"{glyph:^{cap_w}}")
        row += " ".join(cells_render)
        lines.append(row)

    summary = matrix.counts_by_state()
    lines.append("")
    lines.append("Legend: Y=supported  x=unsupported-explicit  Q=quarantined  ?=unknown  .=not-applicable")
    lines.append(
        f"Cells: {len(matrix.cells)}  |  supported={summary[STATE_SUPPORTED]}  "
        f"unsupported-explicit={summary[STATE_UNSUPPORTED]}  "
        f"quarantined={summary[STATE_QUARANTINED]}  unknown={summary[STATE_UNKNOWN]}"
    )
    lines.append("ADVISORY VIEW (VIB-5112 phase 1) — 'unknown' is reported, never fails a build.")
    return "\n".join(lines)


def _cap_header(capability: str) -> str:
    """Short column header for a capability."""
    return {
        CAP_COMPILE: "cmpl",
        CAP_EXECUTE: "exec",
        CAP_RATE: "rate",
        CAP_VALUATION: "val",
        CAP_ACCOUNTING: "acct",
        CAP_SAFETY_FLOOR: "floor",
        CAP_DEMO_COVERAGE: "demo",
    }.get(capability, capability)


def _filter_cells(
    matrix: CapabilityMatrix,
    *,
    protocol: str | None,
    chain: str | None,
    intent: str | None,
    capability: str | None,
    state: str | None,
) -> CapabilityMatrix:
    """Apply CLI filters (all case-insensitive substring / exact matches)."""
    cells = matrix.cells
    if protocol:
        needle = protocol.lower()
        cells = tuple(c for c in cells if needle in c.protocol.lower())
    if chain:
        # Normalise the requested chain the same way rows are normalised
        # (``_matrix_chain``), so a ``--chain bnb`` request matches the rendered
        # ``bsc`` rows. Off-chain rows render as "(off-chain)" and pass through.
        needle = _matrix_chain(chain.lower())
        cells = tuple(c for c in cells if c.chain == needle)
    if intent:
        needle = intent.upper()
        cells = tuple(c for c in cells if c.intent.upper() == needle)
    if capability:
        needle = capability.lower()
        cells = tuple(c for c in cells if c.capability == needle)
    if state:
        needle = state.lower()
        cells = tuple(c for c in cells if c.state == needle)
    return CapabilityMatrix(cells=cells)


@click.command("capabilities")
@click.option("--json", "as_json", is_flag=True, default=False, help="Output as JSON (for programmatic consumption).")
@click.option("--protocol", "-p", type=str, default=None, help="Filter by protocol name (partial match).")
@click.option("--chain", type=str, default=None, help="Filter by chain name (exact).")
@click.option("--intent", "-i", type=str, default=None, help="Filter by intent verb (exact, e.g. SUPPLY).")
@click.option(
    "--capability",
    "-c",
    type=click.Choice(CAPABILITIES),
    default=None,
    help="Filter by capability.",
)
@click.option(
    "--state",
    "-s",
    type=click.Choice([STATE_SUPPORTED, STATE_UNSUPPORTED, STATE_QUARANTINED, STATE_UNKNOWN]),
    default=None,
    help="Filter by state (e.g. unknown to list the gaps).",
)
def capability_matrix_command(
    as_json: bool,
    protocol: str | None,
    chain: str | None,
    intent: str | None,
    capability: str | None,
    state: str | None,
) -> None:
    """Show the manifest-derived **capability matrix** (advisory — VIB-5112 phase 1).

    A read-only view over the capability declarations the connector manifest
    already carries: per declared ``(protocol, chain, intent)`` cell, which of
    ``compile / execute / rate / valuation / accounting / safety-floor /
    demo-coverage`` are declared (``supported``) vs silently ``unknown``.

    This is **advisory only** — ``unknown`` is reported, it does not fail
    anything. The CI ``unknown`` gate is a later phase (see
    ``docs/internal/qa/capability-matrix-g1-phase1.md``).

    Examples:

    \b
        almanak info capabilities                  # Full table
        almanak info capabilities --json           # JSON for CI / tooling
        almanak info capabilities -s unknown       # Just the coverage gaps
        almanak info capabilities -p aave_v3       # One protocol
        almanak info capabilities -c rate          # One capability column
    """
    matrix = build_capability_matrix()
    matrix = _filter_cells(
        matrix,
        protocol=protocol,
        chain=chain,
        intent=intent,
        capability=capability,
        state=state,
    )

    # --json must always emit valid JSON, even on an empty result set — a
    # programmatic consumer that pipes to a parser must not receive a
    # human-readable error on stdout. The empty-match notice is plain-text only.
    if as_json:
        click.echo(json_module.dumps(matrix.to_dict(), indent=2))
        return

    if not matrix.cells:
        click.echo("No capability cells match the given filters.", err=True)
        return

    click.echo(_render_table(matrix))
