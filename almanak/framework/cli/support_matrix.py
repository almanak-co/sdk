"""CLI command to display the chains x protocols support matrix.

Dynamically derives the matrix from the SDK's actual data structures
(compiler routing tables, contract registries, connector availability).
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
# Per-classifier collectors. Each populates one or more {protocol: chains} maps
# from a specific data source (compiler routing table, contracts registry, or
# connector import-probe). Helpers mutate the shared `all_chains` set so the
# orchestrator only owns assembly + sort.
# ---------------------------------------------------------------------------


def _collect_swap_and_split_oneinch(all_chains: set[str]) -> tuple[dict[str, set[str]], set[str]]:
    """Split PROTOCOL_ROUTERS into (swap_protocols, oneinch_chains)."""
    from almanak.framework.intents.compiler import PROTOCOL_ROUTERS

    swap_protocols: dict[str, set[str]] = {}
    oneinch_chains: set[str] = set()
    for chain, protos in PROTOCOL_ROUTERS.items():
        all_chains.add(chain)
        for proto in protos:
            # Track aggregators separately — they get their own category
            if proto in ("1inch",):
                oneinch_chains.add(chain)
                continue
            swap_protocols.setdefault(proto, set()).add(chain)
    return swap_protocols, oneinch_chains


def _collect_lp_position_managers(all_chains: set[str]) -> dict[str, set[str]]:
    """Collect LP protocols from compiler's LP_POSITION_MANAGERS table."""
    from almanak.framework.intents.compiler import LP_POSITION_MANAGERS

    lp_protocols: dict[str, set[str]] = {}
    for chain, protos in LP_POSITION_MANAGERS.items():
        all_chains.add(chain)
        for proto in protos:
            lp_protocols.setdefault(proto, set()).add(chain)
    return lp_protocols


def _collect_registry_lending(all_chains: set[str]) -> dict[str, set[str]]:
    """Collect Aave V3 and Morpho Blue lending from contracts registry."""
    from almanak.core.contracts import AAVE_V3, MORPHO_BLUE

    lending: dict[str, set[str]] = {}
    for chain in AAVE_V3:
        all_chains.add(chain)
        lending.setdefault("aave_v3", set()).add(chain)
    for chain in MORPHO_BLUE:
        all_chains.add(chain)
        lending.setdefault("morpho_blue", set()).add(chain)
    return lending


def _add_compound_v3(lending: dict[str, set[str]], all_chains: set[str]) -> None:
    """Compound V3 — derive chains from adapter's COMET_ADDRESSES (single source of truth)."""
    try:
        from almanak.framework.connectors.compound_v3 import COMPOUND_V3_COMET_ADDRESSES

        compound_chains = list(COMPOUND_V3_COMET_ADDRESSES.keys())
        lending.setdefault("compound_v3", set()).update(compound_chains)
        all_chains.update(compound_chains)
    except ImportError:
        pass


def _add_euler_v2(lending: dict[str, set[str]], all_chains: set[str]) -> None:
    """Euler V2 — lending on Avalanche + Ethereum (derived from adapter's CHAIN_ADDRESSES)."""
    try:
        from almanak.framework.connectors.euler_v2.adapter import CHAIN_ADDRESSES as EULER_V2_CHAINS

        euler_v2_chains = set(EULER_V2_CHAINS.keys())
        lending.setdefault("euler_v2", set()).update(euler_v2_chains)
        all_chains.update(euler_v2_chains)
    except ImportError:
        pass


def _add_curvance(lending: dict[str, set[str]], all_chains: set[str]) -> None:
    """Curvance — Monad isolated leveraged lending markets (VIB-2861)."""
    try:
        from almanak.framework.connectors.curvance.constants import SUPPORTED_CHAINS as CURVANCE_CHAINS

        lending.setdefault("curvance", set()).update(CURVANCE_CHAINS)
        all_chains.update(CURVANCE_CHAINS)
    except ImportError:
        pass


def _add_singleton_lending(
    lending: dict[str, set[str]],
    all_chains: set[str],
    proto: str,
    chain: str,
    module_path: str,
    attr: str,
) -> None:
    """Add a single-chain lending connector if its adapter module is importable."""
    try:
        __import__(module_path, fromlist=[attr])
    except ImportError:
        return
    lending.setdefault(proto, set()).add(chain)
    all_chains.add(chain)


def _collect_lending_protocols(all_chains: set[str]) -> dict[str, set[str]]:
    """Collect lending protocols from registries + connector import-probes."""
    lending = _collect_registry_lending(all_chains)
    _add_compound_v3(lending, all_chains)
    # BenQi — Avalanche lending
    _add_singleton_lending(
        lending, all_chains, "benqi", "avalanche", "almanak.framework.connectors.benqi", "BenqiAdapter"
    )
    # Spark — Ethereum lending
    _add_singleton_lending(
        lending, all_chains, "spark", "ethereum", "almanak.framework.connectors.spark", "SparkAdapter"
    )
    # Silo V2 — Avalanche isolated lending (ERC-4626 vault pairs)
    _add_singleton_lending(
        lending, all_chains, "silo_v2", "avalanche", "almanak.framework.connectors.silo_v2.adapter", "SiloV2Adapter"
    )
    _add_euler_v2(lending, all_chains)
    # Joe Lend (Banker Joe) — DORMANT (governance wound down the protocol; VIB-3960).
    # Connector kept in-tree only for historical receipt parsing; full removal in July.
    # Jupiter Lend — intentionally NOT emitted. The compiler_solana.py routing
    # is unexercised (no demo, no incubating, no on-chain intent test). The
    # connector is also omitted from ConnectorRegistry. Re-add this collector
    # once a Solana lending demo + intent test cover the full lifecycle.
    _add_curvance(lending, all_chains)
    return lending


def _collect_perps_protocols(all_chains: set[str]) -> dict[str, set[str]]:
    """Collect perps protocols from contracts registry + connector import-probes."""
    from almanak.core.contracts import GMX_V2

    perps: dict[str, set[str]] = {}
    for chain in GMX_V2:
        all_chains.add(chain)
        perps.setdefault("gmx_v2", set()).add(chain)

    # Drift — Solana perps
    try:
        from almanak.framework.connectors.drift import DriftAdapter  # noqa: F401

        perps.setdefault("drift", set()).add("solana")
        all_chains.add("solana")
    except ImportError:
        pass

    # Hyperliquid — intentionally NOT emitted. Production PERP execution has
    # not shipped (VIB-4774) and zero strategies in the repo route through the
    # adapter. The connector is also omitted from ConnectorRegistry. Re-add
    # this import-probe once VIB-4774 lands and an on-chain intent test
    # covers PERP_OPEN / PERP_CLOSE end-to-end.

    # Aster (ApolloX rebrand) — BSC perps (Diamond proxy, broker_id 0).
    # PancakeSwap Perps is a shim over aster_perps (broker_id 2), so it ships
    # whenever the Aster adapter is importable.
    try:
        from almanak.framework.connectors.aster_perps.adapter import AsterPerpsAdapter  # noqa: F401

        perps.setdefault("aster_perps", set()).add("bsc")
        perps.setdefault("pancakeswap_perps", set()).add("bsc")
        all_chains.add("bsc")
    except ImportError:
        pass

    return perps


def _collect_yield_protocols(all_chains: set[str]) -> dict[str, set[str]]:
    """Collect yield protocols from contracts registry + connector import-probes."""
    from almanak.core.contracts import PENDLE

    yields: dict[str, set[str]] = {}
    for chain in PENDLE:
        all_chains.add(chain)
        yields.setdefault("pendle", set()).add(chain)

    # Ethena
    try:
        from almanak.framework.connectors.ethena import EthenaAdapter  # noqa: F401

        yields.setdefault("ethena", set()).add("ethereum")
        all_chains.add("ethereum")
    except ImportError:
        pass

    # Lido staking
    try:
        from almanak.framework.connectors.lido import LidoAdapter  # noqa: F401

        yields.setdefault("lido", set()).add("ethereum")
        all_chains.add("ethereum")
    except ImportError:
        pass

    # MetaMorpho vaults
    try:
        from almanak.framework.connectors.morpho_vault import MetaMorphoAdapter  # noqa: F401

        morpho_vault_chains = ["ethereum", "base"]
        yields.setdefault("morpho_vault", set()).update(morpho_vault_chains)
        all_chains.update(morpho_vault_chains)
    except ImportError:
        pass

    # Gimo — liquid staking on 0G Chain (StaFi EVM LSD Stack)
    try:
        from almanak.framework.connectors.gimo import GimoAdapter  # noqa: F401

        yields.setdefault("gimo", set()).add("zerog")
        all_chains.add("zerog")
    except ImportError:
        pass

    return yields


def _collect_prediction_protocols(all_chains: set[str]) -> dict[str, set[str]]:
    """Collect prediction-market protocols.

    Polymarket — prediction markets on Polygon (off-chain CLOB + on-chain CTF).
    Edge/agent-side compatibility filters consult this matrix — dropping
    Polymarket from "yield" into a dedicated "prediction" category avoids
    surfacing it as a yield opportunity (VIB-3139).
    """
    prediction: dict[str, set[str]] = {}
    try:
        from almanak.framework.connectors.polymarket.adapter import PolymarketAdapter  # noqa: F401

        prediction.setdefault("polymarket", set()).add("polygon")
        all_chains.add("polygon")
    except ImportError:
        pass
    return prediction


def _collect_bridge_protocols(all_chains: set[str]) -> dict[str, set[str]]:
    """Collect bridge protocols (Across, Stargate) intersected with SDK Chain enum."""
    bridges: dict[str, set[str]] = {}

    # Across — fast intent-based bridge (uses spoke pools, no slippage on supported tokens)
    # zksync is in Across's registry but not in the SDK's Chain enum — excluded.
    try:
        from almanak.framework.connectors.across.adapter import ACROSS_CHAIN_IDS

        across_sdk_chains = {"ethereum", "arbitrum", "optimism", "base", "polygon", "linea"}
        bridges.setdefault("across", set()).update(c for c in ACROSS_CHAIN_IDS if c in across_sdk_chains)
        all_chains.update(c for c in ACROSS_CHAIN_IDS if c in across_sdk_chains)
    except ImportError:
        pass

    # Stargate — LayerZero-based bridge (native asset + stablecoin transfers)
    try:
        from almanak.framework.connectors.stargate.adapter import STARGATE_CHAIN_IDS

        stargate_sdk_chains = {"ethereum", "arbitrum", "optimism", "base", "polygon", "avalanche", "bsc"}
        bridges.setdefault("stargate", set()).update(c for c in STARGATE_CHAIN_IDS if c in stargate_sdk_chains)
        all_chains.update(c for c in STARGATE_CHAIN_IDS if c in stargate_sdk_chains)
    except ImportError:
        pass

    return bridges


def _collect_flash_protocols(all_chains: set[str]) -> dict[str, set[str]]:
    """Collect flash-loan protocols from BALANCER_VAULT_ADDRESSES."""
    from almanak.framework.intents.compiler import BALANCER_VAULT_ADDRESSES

    flash: dict[str, set[str]] = {}
    for chain in BALANCER_VAULT_ADDRESSES:
        all_chains.add(chain)
        flash.setdefault("balancer", set()).add(chain)
    return flash


def _collect_aggregator_protocols(all_chains: set[str], oneinch_chains: set[str]) -> dict[str, set[str]]:
    """Collect aggregator protocols (Enso, LiFi) and merge in 1inch chains tracked from PROTOCOL_ROUTERS."""
    aggs: dict[str, set[str]] = {}
    try:
        from almanak.framework.connectors.enso.client import CHAIN_MAPPING as ENSO_CHAIN_MAPPING

        enso_chains = set(ENSO_CHAIN_MAPPING.keys())
        aggs["enso"] = enso_chains
        all_chains.update(enso_chains)
    except ImportError:
        pass
    try:
        from almanak.framework.connectors.lifi.client import CHAIN_MAPPING as LIFI_CHAIN_MAPPING

        lifi_chains = set(LIFI_CHAIN_MAPPING.keys())
        aggs["lifi"] = lifi_chains
        all_chains.update(lifi_chains)
    except ImportError:
        pass
    # 1inch — tracked from PROTOCOL_ROUTERS above
    if oneinch_chains:
        aggs["1inch"] = oneinch_chains
        all_chains.update(oneinch_chains)
    return aggs


def _augment_with_curve(
    swap_protocols: dict[str, set[str]],
    lp_protocols: dict[str, set[str]],
    all_chains: set[str],
) -> None:
    """Curve (from connector adapter, supports swap + LP)."""
    try:
        from almanak.framework.connectors.curve.adapter import CURVE_ADDRESSES

        for chain in CURVE_ADDRESSES:
            all_chains.add(chain)
            swap_protocols.setdefault("curve", set()).add(chain)
            lp_protocols.setdefault("curve", set()).add(chain)
    except ImportError:
        pass


def _augment_with_dex_extras(
    swap_protocols: dict[str, set[str]],
    lp_protocols: dict[str, set[str]],
    all_chains: set[str],
) -> None:
    """DEX-specific additions from contracts registry + Fluid import-probe."""
    from almanak.core.contracts import AERODROME, AGNI_FINANCE, TRADERJOE_V2, UNISWAP_V4

    # Aerodrome/Velodrome
    for chain in AERODROME:
        all_chains.add(chain)
        # Already in swap_protocols via PROTOCOL_ROUTERS, but ensure LP
        lp_protocols.setdefault("aerodrome", set()).add(chain)

    # TraderJoe V2 — LP + swap (VIB-1928 compiler path) on all TJ V2 chains
    for chain in TRADERJOE_V2:
        all_chains.add(chain)
        lp_protocols.setdefault("traderjoe_v2", set()).add(chain)
        swap_protocols.setdefault("traderjoe_v2", set()).add(chain)

    # Uniswap V4 — swap via Universal Router (LP already populated by
    # LP_POSITION_MANAGERS via PROTOCOL_ROUTERS in compiler_constants)
    for chain in UNISWAP_V4:
        all_chains.add(chain)
        swap_protocols.setdefault("uniswap_v4", set()).add(chain)

    # Fluid DEX — swap on Arbitrum (LP already populated via LP_POSITION_MANAGERS)
    try:
        from almanak.framework.connectors.fluid.adapter import FluidAdapter  # noqa: F401

        swap_protocols.setdefault("fluid", set()).add("arbitrum")
        all_chains.add("arbitrum")
    except ImportError:
        pass

    # Agni Finance (Uniswap V3 fork on Mantle)
    for chain in AGNI_FINANCE:
        all_chains.add(chain)
        swap_protocols.setdefault("agni_finance", set()).add(chain)
        lp_protocols.setdefault("agni_finance", set()).add(chain)


def _try_import_solana_proto(module_path: str, attr: str) -> bool:
    """Return True if a Solana connector module is importable."""
    try:
        __import__(module_path, fromlist=[attr])
    except ImportError:
        return False
    return True


def _augment_with_solana_protocols(
    swap_protocols: dict[str, set[str]],
    lp_protocols: dict[str, set[str]],
    lending_protocols: dict[str, set[str]],
    all_chains: set[str],
) -> None:
    """Probe Solana connectors and merge them into the swap/lp/lending dicts."""
    solana_swap: list[str] = []
    solana_lp: list[str] = []
    solana_lending: list[str] = []

    if _try_import_solana_proto("almanak.framework.connectors.jupiter", "JupiterAdapter"):
        solana_swap.append("jupiter")
    if _try_import_solana_proto("almanak.framework.connectors.raydium", "RaydiumAdapter"):
        solana_lp.append("raydium")
    if _try_import_solana_proto("almanak.framework.connectors.orca", "OrcaAdapter"):
        solana_lp.append("orca")
    if _try_import_solana_proto("almanak.framework.connectors.meteora", "MeteoraAdapter"):
        solana_lp.append("meteora")
    if _try_import_solana_proto("almanak.framework.connectors.kamino", "KaminoAdapter"):
        solana_lending.append("kamino")

    if solana_swap or solana_lp or solana_lending:
        all_chains.add("solana")

    for proto in solana_swap:
        swap_protocols.setdefault(proto, set()).add("solana")
    for proto in solana_lp:
        lp_protocols.setdefault(proto, set()).add("solana")
    for proto in solana_lending:
        lending_protocols.setdefault(proto, set()).add("solana")


def _assemble_protocol_list(
    all_chains: set[str],
    swap_protocols: dict[str, set[str]],
    lp_protocols: dict[str, set[str]],
    lending_protocols: dict[str, set[str]],
    perps_protocols: dict[str, set[str]],
    yield_protocols: dict[str, set[str]],
    prediction_protocols: dict[str, set[str]],
    flash_protocols: dict[str, set[str]],
    agg_protocols: dict[str, set[str]],
    bridge_protocols: dict[str, set[str]],
) -> list[dict[str, Any]]:
    """Build the unified protocol list in the canonical category order."""
    protocols: list[dict[str, Any]] = []

    def _add(name: str, category: str, chains: set[str]) -> None:
        protocols.append(
            {
                "name": name,
                "category": category,
                "chains": sorted(chains & all_chains),
            }
        )

    # Order matters — assertions in tests/CLI rendering rely on it.
    sections: tuple[tuple[dict[str, set[str]], str], ...] = (
        (swap_protocols, ACTION_SWAP),
        # Always add LP entry even when protocol also appears in swap
        (lp_protocols, ACTION_LP),
        (lending_protocols, ACTION_LENDING),
        (perps_protocols, ACTION_PERPS),
        (yield_protocols, ACTION_YIELD),
        (prediction_protocols, ACTION_PREDICTION),
        (flash_protocols, ACTION_FLASH_LOAN),
        (agg_protocols, ACTION_AGGREGATOR),
        (bridge_protocols, ACTION_BRIDGE),
    )
    for source, category in sections:
        for name, chains in sorted(source.items()):
            _add(name, category, chains)
    return protocols


def _sort_chains(all_chains: set[str]) -> list[str]:
    """Sort chains in canonical CLI display order, appending unknowns alphabetically."""
    chain_order = [
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
    ]
    sorted_chains = [c for c in chain_order if c in all_chains]
    # Add any chains not in our predefined order
    sorted_chains.extend(sorted(all_chains - set(chain_order)))
    return sorted_chains


def _build_matrix() -> dict:
    """Build the chains x protocols support matrix from SDK data structures.

    Returns a dict with:
        chains: list of chain names
        protocols: list of {name, category, chains: [chain_names]}
    """
    all_chains: set[str] = set()

    swap_protocols, oneinch_chains = _collect_swap_and_split_oneinch(all_chains)
    lp_protocols = _collect_lp_position_managers(all_chains)
    lending_protocols = _collect_lending_protocols(all_chains)
    perps_protocols = _collect_perps_protocols(all_chains)
    yield_protocols = _collect_yield_protocols(all_chains)
    prediction_protocols = _collect_prediction_protocols(all_chains)
    bridge_protocols = _collect_bridge_protocols(all_chains)
    flash_protocols = _collect_flash_protocols(all_chains)
    agg_protocols = _collect_aggregator_protocols(all_chains, oneinch_chains)

    _augment_with_curve(swap_protocols, lp_protocols, all_chains)
    _augment_with_dex_extras(swap_protocols, lp_protocols, all_chains)
    _augment_with_solana_protocols(swap_protocols, lp_protocols, lending_protocols, all_chains)

    protocols = _assemble_protocol_list(
        all_chains,
        swap_protocols,
        lp_protocols,
        lending_protocols,
        perps_protocols,
        yield_protocols,
        prediction_protocols,
        flash_protocols,
        agg_protocols,
        bridge_protocols,
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

    Dynamically derived from the SDK's routing tables and contract registries.
    Always reflects the current state of the codebase.

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
