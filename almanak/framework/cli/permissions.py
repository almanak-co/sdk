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
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any

import click

from almanak.config import load_config
from almanak.core.chains import DEFAULT_CHAIN
from almanak.core.chains._helpers import alchemy_rpc_url_template_for

from .intent_debug import load_strategy_from_file

if TYPE_CHECKING:
    from ..permissions.models import PermissionManifest
    from ..strategies.intent_strategy import IntentStrategy
    from ..strategies.metadata import StrategyMetadata

logger = logging.getLogger(__name__)


def _load_dotenv(working_path: Path) -> None:
    """Load .env from the working directory through the config-service boundary.

    Routes through :func:`almanak.config.env._load_dotenv_once` — the single
    process-wide dotenv ingest. ``_load_dotenv_once`` honours the typical
    "no overwrite" semantic via dotenv's default ``override=False`` (so existing
    env values win), matching the legacy hand-rolled parser this helper replaced.
    """
    from almanak.config.env import _load_dotenv_once

    env_file = working_path / ".env"
    if not env_file.exists():
        return
    _load_dotenv_once(str(env_file))


def _resolve_rpc_url(explicit_url: str | None, chain: str) -> str | None:
    """Resolve RPC URL from explicit flag or typed gateway config.

    The Alchemy key is the canonical home of the gateway-tier env read
    (:attr:`GatewayConfig.alchemy_api_key`); this helper consumes the
    typed value rather than re-reading ``ALCHEMY_API_KEY`` directly.
    """
    if explicit_url:
        return explicit_url
    alchemy_key = load_config().gateway.alchemy_api_key
    if not alchemy_key:
        return None
    template = alchemy_rpc_url_template_for(chain)
    if not template:
        return None
    return template.replace("{key}", alchemy_key)


@dataclass(frozen=True)
class _PermissionOptions:
    working_path: Path
    chain: str | None
    output: str | None
    output_format: str
    rpc_url: str | None


@dataclass(frozen=True)
class _StrategyInputs:
    strategy_class: type[IntentStrategy[Any]]
    strategy_name: str
    protocols: list[str]
    declared_protocols_lower: set[str]
    intent_types: list[str]
    chains: list[str]
    config: dict[str, Any]


def _resolve_cli_inputs(
    working_dir: str,
    chain: str | None,
    output: str | None,
    output_format: str,
    rpc_url: str | None,
) -> _PermissionOptions:
    """Resolve CLI inputs before strategy imports or permission discovery."""
    working_path = Path(working_dir).resolve()
    _load_dotenv(working_path)
    return _PermissionOptions(working_path, chain, output, output_format, rpc_url)


def _load_strategy_inputs(working_path: Path, explicit_chain: str | None) -> _StrategyInputs:
    """Load strategy metadata and config in the CLI's established failure order."""
    strategy_file = working_path / "strategy.py"
    if not strategy_file.exists():
        click.echo(f"Error: No strategy.py found in {working_path}", err=True)
        sys.exit(1)

    strategy_class, error = load_strategy_from_file(strategy_file)
    if error or strategy_class is None:
        click.echo(f"Error loading strategy: {error}", err=True)
        sys.exit(1)

    metadata = getattr(strategy_class, "STRATEGY_METADATA", None)
    if metadata is None:
        click.echo(
            "Error: Strategy has no STRATEGY_METADATA. Add @almanak_strategy(...) decorator to your strategy class.",
            err=True,
        )
        sys.exit(1)

    strategy_name = metadata.name or strategy_class.__name__
    protocols = list(metadata.supported_protocols) if metadata.supported_protocols else []
    intent_types = [member.value for member in metadata.intent_types]
    if not protocols:
        click.echo("Warning: No supported_protocols in strategy metadata.", err=True)
    if not intent_types:
        click.echo("Warning: No intent_types in strategy metadata.", err=True)

    chains = _resolve_strategy_chains(explicit_chain, metadata)

    from ..permissions.generator import load_strategy_config

    config = load_strategy_config(working_path / "config.json")
    declared_protocols_lower = {protocol.lower() for protocol in protocols}
    return _StrategyInputs(
        strategy_class,
        strategy_name,
        protocols,
        declared_protocols_lower,
        intent_types,
        chains,
        config,
    )


def _resolve_strategy_chains(explicit_chain: str | None, metadata: StrategyMetadata) -> list[str]:
    """Apply the CLI chain precedence without normalizing user or metadata values."""
    if explicit_chain:
        return [explicit_chain]
    if metadata.supported_chains:
        return list(metadata.supported_chains)
    if metadata.default_chain:
        return [metadata.default_chain]
    return [DEFAULT_CHAIN]


def _select_output_chains(chains: list[str], output_format: str) -> list[str]:
    """Fail closed on unknown and non-EVM chains for Zodiac output only."""
    if output_format != "zodiac":
        return chains

    from almanak.core.chains import ChainRegistry
    from almanak.core.enums import ChainFamily

    evm_chains = []
    for chain in chains:
        family = ChainRegistry.family_of(chain)
        if family is ChainFamily.EVM:
            evm_chains.append(chain)
        elif family is None:
            click.echo(f"  Skipping {chain} (unknown chain, cannot verify EVM)", err=True)
        else:
            click.echo(f"  Skipping {chain} (non-EVM, Zodiac not applicable)", err=True)
    return evm_chains


def _render_empty_zodiac(output: str | None) -> None:
    """Render the successful empty result used when no Zodiac chain is eligible."""
    click.echo("No EVM chains to generate permissions for.", err=True)
    if output:
        Path(output).write_text("[]")
        click.echo(f"Empty zodiac targets written to {output}", err=True)
    else:
        click.echo("[]")


def _validate_rpc_scope(rpc_url: str | None, chains: list[str]) -> None:
    """Reject one explicit RPC URL when discovery still targets multiple chains."""
    if rpc_url and len(chains) > 1:
        click.echo(
            f"Error: --rpc-url cannot be used with multiple chains ({', '.join(chains)}). "
            "Set ALCHEMY_API_KEY in .env for automatic per-chain RPC resolution.",
            err=True,
        )
        sys.exit(1)


def _discover_manifest_for_chain(
    inputs: _StrategyInputs,
    target_chain: str,
    rpc_url: str | None,
) -> PermissionManifest:
    """Discover one chain's teardown-aware manifest and translate security errors.

    Generation and binding failures already carry an exact remedy. Broken hints
    imports need wider context because discovery loads every connector's hints,
    not only those declared by this strategy. Other exceptions propagate.
    """
    from ..permissions.generator import PermissionGenerationError, discover_teardown_protocols, generate_manifest
    from ..permissions.hints import PermissionBindingError, PermissionHintsError

    td_protocols, td_warnings = discover_teardown_protocols(inputs.strategy_class, target_chain, config=inputs.config)
    for warning in td_warnings:
        click.echo(f"  Warning: {warning}", err=True)
    chain_extra = td_protocols - inputs.declared_protocols_lower
    chain_protocols = inputs.protocols if not chain_extra else list(set(inputs.protocols) | chain_extra)

    if chain_extra:
        missing_str = ", ".join(sorted(chain_extra))
        click.echo(
            f"  Teardown on {target_chain} uses protocols not in supported_protocols: [{missing_str}]",
            err=True,
        )

    chain_rpc_url = _resolve_rpc_url(rpc_url, target_chain)
    if chain_rpc_url:
        click.echo(f"  Using RPC for on-chain discovery on {target_chain}", err=True)

    click.echo(f"Generating permissions for {inputs.strategy_name} on {target_chain}...", err=True)
    try:
        manifest = generate_manifest(
            strategy_name=inputs.strategy_name,
            chain=target_chain,
            supported_protocols=chain_protocols,
            intent_types=inputs.intent_types,
            config=inputs.config,
            rpc_url=chain_rpc_url,
        )
    except (PermissionGenerationError, PermissionBindingError) as exc:
        raise click.ClickException(str(exc)) from exc
    except (PermissionHintsError, ImportError) as exc:
        raise click.ClickException(
            f"{exc}\n\n"
            "Permission manifest generation failed while importing. Most often this is a "
            "connector's permission_hints module missing an export or carrying a broken "
            "import — and discovery reads EVERY connector's hints, so one bad module blocks "
            "generation for all of them; the fault need not be in the connector you asked "
            "for. Inspect the error above for the module that actually failed. Do not "
            "hand-write a manifest to work around this: an incomplete one reverts "
            "unauthorized under Safe."
        ) from exc

    for warning in manifest.warnings:
        click.echo(f"  Warning: {warning}", err=True)
    click.echo(
        f"  Found {len(manifest.permissions)} contract permissions "
        f"with {sum(len(permission.function_selectors) for permission in manifest.permissions)} selectors",
        err=True,
    )
    return manifest


def _discover_manifests(inputs: _StrategyInputs, chains: list[str], rpc_url: str | None) -> list[PermissionManifest]:
    """Run discovery with compiler noise suppressed and always restore logging."""
    compiler_logger = logging.getLogger("almanak.framework.intents.compiler")
    original_level = compiler_logger.level
    compiler_logger.setLevel(logging.CRITICAL)
    try:
        return [_discover_manifest_for_chain(inputs, target_chain, rpc_url) for target_chain in chains]
    finally:
        compiler_logger.setLevel(original_level)


def _render_manifests(manifests: list[PermissionManifest], output_format: str, output: str | None) -> None:
    """Serialize manifests in the requested schema and route output to file or stdout."""
    output_data: object
    if output_format == "zodiac":
        if len(manifests) == 1:
            output_data = manifests[0].to_zodiac_targets()
        else:
            output_data = {manifest.chain: manifest.to_zodiac_targets() for manifest in manifests}
    else:
        output_data = manifests[0].to_dict() if len(manifests) == 1 else [manifest.to_dict() for manifest in manifests]

    json_output = json.dumps(output_data, indent=2)
    if output:
        output_path = Path(output)
        output_path.write_text(json_output)
        click.echo(
            f"{'Zodiac targets' if output_format == 'zodiac' else 'Manifest'} written to {output_path}", err=True
        )
    else:
        click.echo(json_output)


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
def permissions(
    working_dir: str, chain: str | None, output: str | None, output_format: str, rpc_url: str | None
) -> None:
    """Generate a Zodiac Roles permission manifest for a strategy.

    Automatically discovers required contract permissions by compiling
    synthetic intents with the strategy's declared protocols and intent types.
    """
    options = _resolve_cli_inputs(working_dir, chain, output, output_format, rpc_url)
    inputs = _load_strategy_inputs(options.working_path, options.chain)
    chains = _select_output_chains(inputs.chains, options.output_format)
    if not chains:
        _render_empty_zodiac(options.output)
        return

    _validate_rpc_scope(options.rpc_url, chains)
    manifests = _discover_manifests(inputs, chains, options.rpc_url)
    _render_manifests(manifests, options.output_format, options.output)
