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
"""

from __future__ import annotations

import json as json_module
from typing import Any

import click

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
SUPPORTED_CATEGORIES: tuple[str, ...] = (
    ACTION_SWAP,
    ACTION_LP,
    ACTION_LENDING,
    ACTION_PERPS,
    ACTION_YIELD,
    ACTION_PREDICTION,
    ACTION_FLASH_LOAN,
    ACTION_AGGREGATOR,
    ACTION_BRIDGE,
)


# ---------------------------------------------------------------------------
# Connector-driven matrix derivation
# ---------------------------------------------------------------------------


def _matrix_chain(chain: str) -> str:
    """Normalise a manifest chain name to the matrix's canonical form.

    The strategy-side :data:`KNOWN_VENUES` uses ``"bnb"`` (the
    ``resolve_chain_name`` canonical form for BNB Chain), but the matrix
    has rendered ``"bsc"`` since inception so downstream Edge / CI
    consumers match on that string. Normalising at the matrix boundary
    keeps both surfaces consistent without changing the strategy-side
    contract.
    """
    return "bsc" if chain == "bnb" else chain


def _normalize_entry_chains(chains: frozenset[str]) -> frozenset[str]:
    """Apply ``_matrix_chain`` to every chain in a ``MatrixEntry``."""
    return frozenset(_matrix_chain(c) for c in chains)


def _derive_entries_from_intents(
    name: str,
    intents: tuple[Any, ...],
    chains: tuple[str, ...] | None,
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
    """
    from almanak.framework.intents.vocabulary import IntentType

    if chains is None:
        return ()

    matrix_chains = frozenset(_matrix_chain(c) for c in chains)

    # Intent class → category. Centralising the dispatch here means
    # connectors don't have to repeat it; the matrix is the only place
    # that knows about the rendering category vocabulary.
    intent_category: dict[IntentType, str] = {
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

    categories: set[str] = set()
    for intent in intents:
        cat = intent_category.get(intent)
        if cat is not None:
            categories.add(cat)

    return tuple((name, cat, matrix_chains) for cat in categories)


def _collect_from_connector_registry(
    entries: dict[tuple[str, str], set[str]],
    authoritative: set[tuple[str, str]],
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
        )
        for matrix_name, category, chain_set in derived:
            key = (matrix_name, category)
            entries.setdefault(key, set()).update(chain_set)


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
        protocols: list of {name, category, chains: [chain_names]}

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

    _collect_from_connector_registry(entries, authoritative)
    _collect_from_compiler_tables(entries, authoritative)

    # Drop empty-chain rows — a connector that declared the capability
    # without any chain coverage shouldn't surface as a no-op row.
    entries = {key: chains for key, chains in entries.items() if chains}

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
            protocols.append(
                {
                    "name": name,
                    "category": category,
                    "chains": sorted(chain_set & all_chains),
                }
            )

    return {
        "chains": _sort_chains(all_chains),
        "protocols": protocols,
    }


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
        chain_lower = chain.lower()
        data["protocols"] = [p for p in data["protocols"] if chain_lower in p["chains"]]
        # Trim each protocol's chains list to only the filtered chain
        for p in data["protocols"]:
            p["chains"] = [c for c in p["chains"] if c == chain_lower]
        data["chains"] = [c for c in data["chains"] if c == chain_lower]
    if protocol:
        protocol_lower = protocol.lower()
        data["protocols"] = [p for p in data["protocols"] if protocol_lower in p["name"].lower()]

    if not data["protocols"]:
        click.echo("No protocols match the given filters.", err=True)
        return

    if as_json:
        # Clean output for programmatic consumption
        output = {
            "chains": data["chains"],
            "protocols": data["protocols"],
        }
        click.echo(json_module.dumps(output, indent=2))
    else:
        click.echo(_render_table(data))
