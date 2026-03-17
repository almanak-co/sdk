"""CLI command for generating Zodiac Roles permission manifests.

Usage:
    almanak strat permissions                          # from strategy directory
    almanak strat permissions -d strategies/demo/uniswap_rsi
    almanak strat permissions --chain arbitrum          # override chain
    almanak strat permissions --output manifest.json   # write to file
"""

from __future__ import annotations

import json
import logging
import sys
from pathlib import Path

import click

from .intent_debug import load_strategy_from_file

logger = logging.getLogger(__name__)


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
def permissions(working_dir: str, chain: str | None, output: str | None, output_format: str) -> None:
    """Generate a Zodiac Roles permission manifest for a strategy.

    Automatically discovers required contract permissions by compiling
    synthetic intents with the strategy's declared protocols and intent types.
    """
    working_path = Path(working_dir).resolve()
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
    from ..permissions.generator import load_strategy_config

    config_path = working_path / "config.json"
    config = load_strategy_config(config_path)

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

    # Suppress noisy compiler warnings during permission discovery
    # (e.g., Enso API key errors, placeholder price warnings produce tracebacks)
    compiler_logger = logging.getLogger("almanak.framework.intents.compiler")
    original_level = compiler_logger.level
    compiler_logger.setLevel(logging.CRITICAL)

    manifests = []
    try:
        for target_chain in chains:
            click.echo(f"Generating permissions for {strategy_name} on {target_chain}...", err=True)
            manifest = generate_manifest(
                strategy_name=strategy_name,
                chain=target_chain,
                supported_protocols=protocols,
                intent_types=intent_types,
                config=config,
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
