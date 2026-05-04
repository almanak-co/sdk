"""CLI command for generating Zodiac Roles permission manifests.

Usage:
    almanak strat permissions                          # from strategy directory
    almanak strat permissions -d almanak/demo_strategies/uniswap_rsi
    almanak strat permissions --chain arbitrum          # override chain
    almanak strat permissions --output manifest.json   # write to file
    almanak strat permissions --rpc-url https://...    # enable on-chain discovery
    ALCHEMY_API_KEY=xyz almanak strat permissions      # auto-resolve RPC from env
"""

from __future__ import annotations

import json
import logging
import os
import sys
from pathlib import Path

import click

from .intent_debug import load_strategy_from_file

logger = logging.getLogger(__name__)

# RPC URL templates for auto-resolution from ALCHEMY_API_KEY env var.
_CHAIN_RPC_TEMPLATES: dict[str, str] = {
    "base": "https://base-mainnet.g.alchemy.com/v2/{key}",
    "arbitrum": "https://arb-mainnet.g.alchemy.com/v2/{key}",
    "ethereum": "https://eth-mainnet.g.alchemy.com/v2/{key}",
    "avalanche": "https://avax-mainnet.g.alchemy.com/v2/{key}",
    "mantle": "https://mantle-mainnet.g.alchemy.com/v2/{key}",
    "bsc": "https://bnb-mainnet.g.alchemy.com/v2/{key}",
    "optimism": "https://opt-mainnet.g.alchemy.com/v2/{key}",
    "polygon": "https://polygon-mainnet.g.alchemy.com/v2/{key}",
}


def _load_dotenv(working_path: Path) -> None:
    """Load .env from the working directory into os.environ (without overwriting)."""
    env_file = working_path / ".env"
    if not env_file.exists():
        return
    try:
        for line in env_file.read_text().splitlines():
            stripped = line.strip()
            if not stripped or stripped.startswith("#"):
                continue
            eq = stripped.find("=")
            if eq < 0:
                continue
            key = stripped[:eq].strip()
            val = stripped[eq + 1 :].strip().strip("'\"")
            if key not in os.environ:
                os.environ[key] = val
    except (OSError, UnicodeDecodeError) as exc:
        logger.debug("Failed to load .env from %s: %s", env_file, exc)


def _resolve_rpc_url(explicit_url: str | None, chain: str) -> str | None:
    """Resolve RPC URL from explicit flag or ALCHEMY_API_KEY environment variable."""
    if explicit_url:
        return explicit_url
    alchemy_key = os.environ.get("ALCHEMY_API_KEY")
    if not alchemy_key:
        return None
    template = _CHAIN_RPC_TEMPLATES.get(chain.lower())
    if not template:
        return None
    return template.replace("{key}", alchemy_key)


@click.command("permissions")
@click.option(
    "--working-dir",
    "-d",
    type=click.Path(exists=True),
    default=".",
    help="Working directory containing the strategy files.",
)
@click.option(
    "--chain",
    type=str,
    default=None,
    help="Override the target chain (default: from strategy metadata).",
)
@click.option(
    "--output",
    "-o",
    type=click.Path(),
    default=None,
    help="Write manifest to file instead of stdout.",
)
@click.option(
    "--format",
    "output_format",
    type=click.Choice(["manifest", "zodiac"]),
    default="zodiac",
    help="Output format: 'zodiac' (Zodiac Roles Target[], default) or 'manifest' (SDK format).",
)
@click.option(
    "--rpc-url",
    type=str,
    default=None,
    help="RPC URL for on-chain discovery (e.g. Aerodrome pool addresses). "
    "Auto-resolved from ALCHEMY_API_KEY env if not provided.",
)
def permissions(  # noqa: C901
    working_dir: str, chain: str | None, output: str | None, output_format: str, rpc_url: str | None
) -> None:
    """Generate a Zodiac Roles permission manifest for a strategy.

    Automatically discovers required contract permissions by compiling
    synthetic intents with the strategy's declared protocols and intent types.
    """
    working_path = Path(working_dir).resolve()

    # Load .env from the strategy directory so ALCHEMY_API_KEY (and other
    # env vars) are available for RPC auto-resolution without the user
    # having to export them manually.
    _load_dotenv(working_path)

    strategy_file = working_path / "strategy.py"

    if not strategy_file.exists():
        click.echo(f"Error: No strategy.py found in {working_path}", err=True)
        sys.exit(1)

    # Load strategy class
    strategy_class, error = load_strategy_from_file(strategy_file)
    if error or strategy_class is None:
        click.echo(f"Error loading strategy: {error}", err=True)
        sys.exit(1)

    # Read metadata from decorator
    metadata = getattr(strategy_class, "STRATEGY_METADATA", None)
    if metadata is None:
        click.echo(
            "Error: Strategy has no STRATEGY_METADATA. Add @almanak_strategy(...) decorator to your strategy class.",
            err=True,
        )
        sys.exit(1)

    strategy_name = metadata.name or strategy_class.__name__
    protocols = list(metadata.supported_protocols) if metadata.supported_protocols else []
    intent_types = list(metadata.intent_types) if metadata.intent_types else []

    if not protocols:
        click.echo("Warning: No supported_protocols in strategy metadata.", err=True)
    if not intent_types:
        click.echo("Warning: No intent_types in strategy metadata.", err=True)

    # Determine chain(s)
    if chain:
        chains = [chain]
    elif metadata.supported_chains:
        chains = list(metadata.supported_chains)
    elif metadata.default_chain:
        chains = [metadata.default_chain]
    else:
        chains = ["arbitrum"]

    # Load config.json for token extraction
    from ..permissions.generator import discover_teardown_protocols, load_strategy_config

    config_path = working_path / "config.json"
    config = load_strategy_config(config_path)

    # Teardown protocol discovery is done per-chain inside the manifest loop
    # to avoid granting chain-specific protocols (e.g. Enso on Base) to all chains.
    declared_protocols_lower = {p.lower() for p in protocols}

    # Generate manifest for each chain
    from ..permissions.generator import generate_manifest

    # Filter out non-EVM chains for zodiac format (Safe/Zodiac is EVM-only)
    # This must happen before the compiler logger mutation so the early exit
    # doesn't leave the logger stuck at CRITICAL.
    if output_format == "zodiac":
        from almanak.core.enums import CHAIN_FAMILY_MAP, Chain, ChainFamily

        evm_chains = []
        for c in chains:
            try:
                chain_enum = Chain(c.upper())
                if CHAIN_FAMILY_MAP.get(chain_enum) == ChainFamily.EVM:
                    evm_chains.append(c)
                else:
                    click.echo(f"  Skipping {c} (non-EVM, Zodiac not applicable)", err=True)
            except ValueError:
                # Unknown chain -- fail closed to match PermissionManifest.is_evm_chain
                click.echo(f"  Skipping {c} (unknown chain, cannot verify EVM)", err=True)
        chains = evm_chains

    if not chains:
        click.echo("No EVM chains to generate permissions for.", err=True)
        if output:
            Path(output).write_text("[]")
            click.echo(f"Empty zodiac targets written to {output}", err=True)
        else:
            click.echo("[]")
        return

    # Block --rpc-url with multiple chains — a single URL can only serve one chain.
    # Use ALCHEMY_API_KEY for automatic multi-chain resolution instead.
    if rpc_url and len(chains) > 1:
        click.echo(
            f"Error: --rpc-url cannot be used with multiple chains ({', '.join(chains)}). "
            "Set ALCHEMY_API_KEY in .env for automatic per-chain RPC resolution.",
            err=True,
        )
        sys.exit(1)

    # Suppress noisy compiler warnings during permission discovery
    # (e.g., Enso API key errors, placeholder price warnings produce tracebacks)
    compiler_logger = logging.getLogger("almanak.framework.intents.compiler")
    original_level = compiler_logger.level
    compiler_logger.setLevel(logging.CRITICAL)

    manifests = []
    try:
        for target_chain in chains:
            # Per-chain teardown protocol discovery
            td_protocols, td_warnings = discover_teardown_protocols(strategy_class, target_chain, config=config)
            for w in td_warnings:
                click.echo(f"  Warning: {w}", err=True)
            chain_extra = td_protocols - declared_protocols_lower
            chain_protocols = protocols if not chain_extra else list(set(protocols) | chain_extra)

            if chain_extra:
                missing_str = ", ".join(sorted(chain_extra))
                click.echo(
                    f"  Teardown on {target_chain} uses protocols not in supported_protocols: [{missing_str}]",
                    err=True,
                )

            # Resolve RPC URL for this chain (explicit flag > ALCHEMY_API_KEY env)
            chain_rpc_url = _resolve_rpc_url(rpc_url, target_chain)
            if chain_rpc_url:
                click.echo(f"  Using RPC for on-chain discovery on {target_chain}", err=True)

            click.echo(f"Generating permissions for {strategy_name} on {target_chain}...", err=True)
            manifest = generate_manifest(
                strategy_name=strategy_name,
                chain=target_chain,
                supported_protocols=chain_protocols,
                intent_types=intent_types,
                config=config,
                rpc_url=chain_rpc_url,
            )
            manifests.append(manifest)

            # Print warnings
            for warning in manifest.warnings:
                click.echo(f"  Warning: {warning}", err=True)

            click.echo(
                f"  Found {len(manifest.permissions)} contract permissions "
                f"with {sum(len(p.function_selectors) for p in manifest.permissions)} selectors",
                err=True,
            )
    finally:
        compiler_logger.setLevel(original_level)

    # Output
    output_data: object
    if output_format == "zodiac":
        if len(manifests) == 1:
            output_data = manifests[0].to_zodiac_targets()
        else:
            output_data = {m.chain: m.to_zodiac_targets() for m in manifests}
    else:
        output_data = manifests[0].to_dict() if len(manifests) == 1 else [m.to_dict() for m in manifests]

    json_output = json.dumps(output_data, indent=2)

    if output:
        output_path = Path(output)
        output_path.write_text(json_output)
        click.echo(
            f"{'Zodiac targets' if output_format == 'zodiac' else 'Manifest'} written to {output_path}", err=True
        )
    else:
        click.echo(json_output)
