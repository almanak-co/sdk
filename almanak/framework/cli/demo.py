"""CLI command: almanak strat demo - browse and copy a demo strategy."""

from __future__ import annotations

import json
import shutil
import sys
from pathlib import Path

import click


def _build_menu_entries(strategies: list[dict]) -> list[str]:
    """Build display strings for the interactive menu."""
    # Find the longest name for alignment
    max_name = max(len(s["name"]) for s in strategies)
    entries = []
    for s in strategies:
        chain = s["chain"]
        desc = s["description"] or "(no description)"
        entries.append(f"{s['name']:<{max_name}}  [{chain}]  {desc}")
    return entries


@click.command("demo")
@click.option(
    "--name",
    "-n",
    type=str,
    default=None,
    help="Demo strategy name (skip interactive selection).",
)
@click.option(
    "--output-dir",
    "-o",
    type=click.Path(),
    default=".",
    help="Parent directory for the copied strategy folder.",
)
@click.option(
    "--list",
    "list_only",
    is_flag=True,
    default=False,
    help="List available demo strategies and exit.",
)
def demo(name: str | None, output_dir: str, list_only: bool) -> None:
    """Browse and copy a demo strategy to get started quickly.

    Shows an interactive menu to pick from 16 working demo strategies,
    then copies the selected strategy into your working directory.

    \b
    Examples:
        almanak strat demo                       # Interactive selection
        almanak strat demo --name uniswap_rsi    # Copy directly
        almanak strat demo --list                 # List available demos
        almanak strat demo -n aave_borrow -o .   # Copy to current dir
    """
    from almanak.demo_strategies import (
        DEMO_STRATEGY_NAMES,
        get_demo_strategy_path,
        list_demo_strategies,
    )

    try:
        strategies = list_demo_strategies()
    except ValueError as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)

    if not strategies:
        click.echo("Error: no demo strategies found in package.", err=True)
        sys.exit(1)

    # --list: print table and exit
    if list_only:
        _print_strategy_table(strategies)
        return

    # Resolve selection
    if name is not None:
        # Validate the provided name
        if name not in DEMO_STRATEGY_NAMES:
            click.echo(f"Error: unknown demo strategy '{name}'.", err=True)
            click.echo(f"Available: {', '.join(DEMO_STRATEGY_NAMES)}", err=True)
            sys.exit(1)
        selected = name
    elif not sys.stdin.isatty():
        # Non-interactive: list and exit (not an error, just informational)
        click.echo("No --name provided and stdin is not a TTY. Available demo strategies:\n", err=True)
        _print_strategy_table(strategies)
        sys.exit(0)
    else:
        choice = _interactive_select(strategies)
        if choice is None:
            # User cancelled
            return
        selected = choice

    # Copy strategy
    source = get_demo_strategy_path(selected)
    output_root = Path(output_dir)
    if output_root.exists() and not output_root.is_dir():
        click.echo(f"Error: output path is not a directory: {output_root}", err=True)
        sys.exit(1)

    target = output_root / selected
    if target.exists():
        click.echo(f"Error: directory already exists: {target}", err=True)
        sys.exit(1)

    shutil.copytree(source, target)

    # Rewrite strategy_id and strategy_name in copied config.json
    _rewrite_config(target, selected)

    # Remove run_anvil.py from the copy (internal dev script)
    run_anvil = target / "run_anvil.py"
    if run_anvil.exists():
        run_anvil.unlink()

    # Remove tests/ subdirectory from the copy (internal tests)
    tests_dir = target / "tests"
    if tests_dir.is_dir():
        shutil.rmtree(tests_dir)

    click.echo(f"\nCopied demo strategy '{selected}' to {target}/")
    click.echo("\nNext steps:")
    click.echo(f"  cd {target}")
    click.echo("  almanak strat run --network anvil --once")


def _print_strategy_table(strategies: list[dict]) -> None:
    """Print a formatted table of strategies."""
    max_name = max(len(s["name"]) for s in strategies)
    max_chain = max(len(s["chain"]) for s in strategies)
    for s in strategies:
        desc = s["description"] or "(no description)"
        click.echo(f"  {s['name']:<{max_name}}  {s['chain']:<{max_chain}}  {desc}")


def _interactive_select(strategies: list[dict]) -> str | None:
    """Show interactive arrow-key menu. Returns strategy name or None if cancelled."""
    try:
        from simple_term_menu import TerminalMenu
    except ImportError:
        click.echo(
            "Error: interactive selection requires 'simple-term-menu'. Install it with: pip install simple-term-menu",
            err=True,
        )
        click.echo("Alternatively, use: almanak strat demo --name <strategy>", err=True)
        sys.exit(1)

    entries = _build_menu_entries(strategies)

    click.echo("Select a demo strategy:\n")
    menu = TerminalMenu(
        entries,
        title="",
        menu_cursor_style=("fg_cyan", "bold"),
        menu_highlight_style=("bg_gray", "fg_cyan", "bold"),
    )
    index = menu.show()

    if index is None:
        click.echo("Cancelled.")
        return None

    return strategies[index]["name"]


def _rewrite_config(target: Path, strategy_name: str) -> None:
    """Clean up the copied config.json.

    Structural metadata (strategy_id, strategy_name, chain, etc.) now lives
    in the @almanak_strategy decorator, so this only strips leftover metadata
    fields for a clean user experience.
    """
    config_path = target / "config.json"
    if not config_path.is_file():
        return

    with open(config_path, encoding="utf-8") as f:
        try:
            config = json.load(f)
        except json.JSONDecodeError as e:
            click.echo(f"Warning: could not parse config.json ({e}); skipping config rewrite.", err=True)
            return

    # Remove any leftover metadata fields (these belong in the decorator now)
    metadata_keys = {"strategy_id", "strategy_name", "description", "protocol", "network", "chain"}
    removed = {k for k in metadata_keys if k in config}
    for k in removed:
        del config[k]

    if removed:
        with open(config_path, "w", encoding="utf-8") as f:
            json.dump(config, f, indent=4)
            f.write("\n")
