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


def _build_matrix() -> dict:
    """Build the chains x protocols support matrix from SDK data structures.

    Returns a dict with:
        chains: list of chain names
        protocols: list of {name, category, chains: [chain_names]}
    """
    from almanak.core.contracts import (
        AAVE_V3,
        AERODROME,
        GMX_V2,
        MORPHO_BLUE,
        PENDLE,
        TRADERJOE_V2,
    )
    from almanak.framework.intents.compiler import (
        BALANCER_VAULT_ADDRESSES,
        LP_POSITION_MANAGERS,
        PROTOCOL_ROUTERS,
    )

    # Collect all chains from all sources
    all_chains: set[str] = set()

    # --- Swap protocols (from PROTOCOL_ROUTERS) ---
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

    # --- LP protocols (from LP_POSITION_MANAGERS) ---
    lp_protocols: dict[str, set[str]] = {}
    for chain, protos in LP_POSITION_MANAGERS.items():
        all_chains.add(chain)
        for proto in protos:
            lp_protocols.setdefault(proto, set()).add(chain)

    # --- Lending protocols (from contracts.py registries) ---
    lending_protocols: dict[str, set[str]] = {}
    for chain in AAVE_V3:
        all_chains.add(chain)
        lending_protocols.setdefault("aave_v3", set()).add(chain)
    for chain in MORPHO_BLUE:
        all_chains.add(chain)
        lending_protocols.setdefault("morpho_blue", set()).add(chain)

    # Compound V3 — check connector existence
    try:
        from almanak.framework.connectors.compound_v3 import CompoundV3Adapter  # noqa: F401

        # Compound V3 supports these chains (from adapter)
        compound_chains = ["ethereum", "arbitrum", "base", "polygon"]
        lending_protocols.setdefault("compound_v3", set()).update(compound_chains)
        all_chains.update(compound_chains)
    except ImportError:
        pass

    # BenQi — Avalanche lending
    try:
        from almanak.framework.connectors.benqi import BenqiAdapter  # noqa: F401

        lending_protocols.setdefault("benqi", set()).add("avalanche")
        all_chains.add("avalanche")
    except ImportError:
        pass

    # Spark — Ethereum lending
    try:
        from almanak.framework.connectors.spark import SparkAdapter  # noqa: F401

        lending_protocols.setdefault("spark", set()).add("ethereum")
        all_chains.add("ethereum")
    except ImportError:
        pass

    # --- Perps protocols ---
    perps_protocols: dict[str, set[str]] = {}
    for chain in GMX_V2:
        all_chains.add(chain)
        perps_protocols.setdefault("gmx_v2", set()).add(chain)

    # Drift — Solana perps
    try:
        from almanak.framework.connectors.drift import DriftAdapter  # noqa: F401

        perps_protocols.setdefault("drift", set()).add("solana")
        all_chains.add("solana")
    except ImportError:
        pass

    # Hyperliquid
    try:
        from almanak.framework.connectors.hyperliquid import HyperliquidAdapter  # noqa: F401

        perps_protocols.setdefault("hyperliquid", set()).add("hyperliquid")
        all_chains.add("hyperliquid")
    except ImportError:
        pass

    # --- Yield protocols ---
    yield_protocols: dict[str, set[str]] = {}
    for chain in PENDLE:
        all_chains.add(chain)
        yield_protocols.setdefault("pendle", set()).add(chain)

    # Ethena
    try:
        from almanak.framework.connectors.ethena import EthenaAdapter  # noqa: F401

        yield_protocols.setdefault("ethena", set()).add("ethereum")
        all_chains.add("ethereum")
    except ImportError:
        pass

    # Lido staking
    try:
        from almanak.framework.connectors.lido import LidoAdapter  # noqa: F401

        yield_protocols.setdefault("lido", set()).add("ethereum")
        all_chains.add("ethereum")
    except ImportError:
        pass

    # MetaMorpho vaults
    try:
        from almanak.framework.connectors.morpho_vault import MetaMorphoAdapter  # noqa: F401

        morpho_vault_chains = ["ethereum", "base"]
        yield_protocols.setdefault("morpho_vault", set()).update(morpho_vault_chains)
        all_chains.update(morpho_vault_chains)
    except ImportError:
        pass

    # --- Flash loan ---
    flash_protocols: dict[str, set[str]] = {}
    for chain in BALANCER_VAULT_ADDRESSES:
        all_chains.add(chain)
        flash_protocols.setdefault("balancer", set()).add(chain)

    # --- Aggregators (derived from connector CHAIN_MAPPING) ---
    agg_protocols: dict[str, set[str]] = {}
    try:
        from almanak.framework.connectors.enso.client import CHAIN_MAPPING as ENSO_CHAIN_MAPPING

        enso_chains = set(ENSO_CHAIN_MAPPING.keys())
        agg_protocols["enso"] = enso_chains
        all_chains.update(enso_chains)
    except ImportError:
        pass
    try:
        from almanak.framework.connectors.lifi.client import CHAIN_MAPPING as LIFI_CHAIN_MAPPING

        lifi_chains = set(LIFI_CHAIN_MAPPING.keys())
        agg_protocols["lifi"] = lifi_chains
        all_chains.update(lifi_chains)
    except ImportError:
        pass
    # 1inch — tracked from PROTOCOL_ROUTERS above
    if oneinch_chains:
        agg_protocols["1inch"] = oneinch_chains
        all_chains.update(oneinch_chains)

    # --- DEX-specific additions from contracts ---
    # Aerodrome/Velodrome
    for chain in AERODROME:
        all_chains.add(chain)
        # Already in swap_protocols via PROTOCOL_ROUTERS, but ensure LP
        lp_protocols.setdefault("aerodrome", set()).add(chain)

    # TraderJoe V2
    for chain in TRADERJOE_V2:
        all_chains.add(chain)
        lp_protocols.setdefault("traderjoe_v2", set()).add(chain)

    # --- Solana protocols ---
    solana_swap: list[str] = []
    solana_lp: list[str] = []
    solana_lending: list[str] = []

    try:
        from almanak.framework.connectors.jupiter import JupiterAdapter  # noqa: F401

        solana_swap.append("jupiter")
    except ImportError:
        pass

    try:
        from almanak.framework.connectors.raydium import RaydiumAdapter  # noqa: F401

        solana_lp.append("raydium")
    except ImportError:
        pass

    try:
        from almanak.framework.connectors.orca import OrcaAdapter  # noqa: F401

        solana_lp.append("orca")
    except ImportError:
        pass

    try:
        from almanak.framework.connectors.meteora import MeteoraAdapter  # noqa: F401

        solana_lp.append("meteora")
    except ImportError:
        pass

    try:
        from almanak.framework.connectors.kamino import KaminoAdapter  # noqa: F401

        solana_lending.append("kamino")
    except ImportError:
        pass

    if solana_swap or solana_lp or solana_lending:
        all_chains.add("solana")

    for proto in solana_swap:
        swap_protocols.setdefault(proto, set()).add("solana")
    for proto in solana_lp:
        lp_protocols.setdefault(proto, set()).add("solana")
    for proto in solana_lending:
        lending_protocols.setdefault(proto, set()).add("solana")

    # --- Build unified protocol list ---
    protocols: list[dict[str, Any]] = []

    def _add(name: str, category: str, chains: set[str]) -> None:
        protocols.append(
            {
                "name": name,
                "category": category,
                "chains": sorted(chains & all_chains),
            }
        )

    for name, chains in sorted(swap_protocols.items()):
        _add(name, ACTION_SWAP, chains)
    for name, chains in sorted(lp_protocols.items()):
        # Always add LP entry even when protocol also appears in swap
        _add(name, ACTION_LP, chains)
    for name, chains in sorted(lending_protocols.items()):
        _add(name, ACTION_LENDING, chains)
    for name, chains in sorted(perps_protocols.items()):
        _add(name, ACTION_PERPS, chains)
    for name, chains in sorted(yield_protocols.items()):
        _add(name, ACTION_YIELD, chains)
    for name, chains in sorted(flash_protocols.items()):
        _add(name, ACTION_FLASH_LOAN, chains)
    for name, chains in sorted(agg_protocols.items()):
        _add(name, ACTION_AGGREGATOR, chains)

    # Sort chains in a sensible order
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

    return {
        "chains": sorted_chains,
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
    help="Filter by category (swap, lp, lending, perps, yield, flash_loan, aggregator).",
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
