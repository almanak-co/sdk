"""CLI command for the LP_CLOSE teardown-bug repair (VIB-4896).

Provides ``almanak strat repair-teardown-lp-close --db <path>`` — an offline
operator command that backfills ``position_events`` LP_CLOSE rows broken by
the pre-VIB-4839 silent-cache bug (``token0``/``token1``/``value_usd`` all
empty string) in an existing SQLite state DB.

No gateway call is made — this is a local-only DML repair against a stopped
strategy's state DB. The engine lives in
``almanak.framework.accounting.repair.lp_close_repair``; this module is the
thin click wrapper (arg parsing, DB resolution, diff/summary printing).

Usage:
    almanak strat repair-teardown-lp-close --db ./almanak_state.db
    almanak strat repair-teardown-lp-close --db ./state.db --dry-run
    almanak strat repair-teardown-lp-close --db ./state.db -s MyStrat:abc123
    almanak strat repair-teardown-lp-close --db ./state.db --prices-source ./prices.json
"""

from __future__ import annotations

import sys
from pathlib import Path

import click

from almanak.framework.accounting.repair import (
    LpCloseRepairResult,
    repair_teardown_lp_close,
)


def _default_db_path() -> str:
    """Resolve the canonical local DB path (mirrors ``strat pnl``)."""
    from almanak.framework.local_paths import LocalPathError, local_db_path

    try:
        return str(local_db_path())
    except LocalPathError:
        return ":hosted-mode-no-sqlite-path:"


def _print_diff(result: LpCloseRepairResult) -> None:
    """Print a per-row diff of the repair (used in both modes)."""
    if not result.rows:
        click.secho("No broken LP_CLOSE rows detected. Nothing to repair.", fg="green")
        return
    verb = "Would repair" if result.dry_run else "Repaired"
    click.secho(
        f"Detected {result.detected} broken LP_CLOSE row(s):",
        fg="yellow",
        bold=True,
    )
    for row in result.rows:
        click.echo("")
        click.echo(f"  event_id={row.event_id}")
        click.echo(f"  position_id={row.position_id} chain={row.chain or '—'}")
        if row.skip_reason is not None:
            click.secho(f"  SKIPPED ({row.skip_reason})", fg="red")
            # Carried bracket cols (where knowable) are still applied.
            if row.token0 or row.token1 or row.tick_lower is not None:
                click.echo(
                    f"    carried bracket: token0={row.token0 or '—'} "
                    f"token1={row.token1 or '—'} "
                    f"tick_lower={row.tick_lower if row.tick_lower is not None else '—'} "
                    f"tick_upper={row.tick_upper if row.tick_upper is not None else '—'} "
                    f"liquidity={row.liquidity or '—'}"
                )
            click.echo("    value_usd left '' (Empty ≠ Zero — never fabricated)")
        else:
            click.secho(f"  {verb}:", fg="green")
            click.echo(
                f"    token0={row.token0} token1={row.token1} "
                f"tick_lower={row.tick_lower} tick_upper={row.tick_upper} "
                f"liquidity={row.liquidity}"
            )
            click.echo(
                f"    value_usd={row.value_usd} "
                f"principal_recovered_usd={row.principal_recovered_usd} "
                f"(prices: {row.price_provenance})"
            )


def _print_summary(result: LpCloseRepairResult) -> None:
    click.echo("")
    click.secho("Summary", bold=True)
    click.echo(f"  detected: {result.detected}")
    click.echo(f"  repaired: {result.repaired}")
    click.echo(f"  skipped:  {result.skipped}")
    if result.dry_run:
        click.secho("  DRY RUN — no changes written, no backup created.", fg="cyan")
    else:
        click.echo(f"  written:  {result.written}")
        if result.backup_path:
            click.echo(f"  backup:   {result.backup_path}")


@click.command("repair-teardown-lp-close")
@click.option(
    "--db",
    "db_path",
    default=None,
    help="SQLite state DB path (default: the canonical local DB path).",
)
@click.option(
    "--deployment-id",
    "-s",
    "deployment_id",
    default=None,
    help="Restrict the repair to a single deployment id (default: all).",
)
@click.option(
    "--dry-run",
    is_flag=True,
    help="Print the per-row diff and write NOTHING (no backup created).",
)
@click.option(
    "--prices-source",
    "prices_source",
    default=None,
    help=(
        "Optional JSON price override (keyed by UPPER-cased symbol -> "
        "{'price_usd': '<decimal>'}). Used only when the ledger's "
        "price_inputs_json is missing/empty (degraded case)."
    ),
)
def repair_teardown_lp_close_cmd(
    db_path: str | None,
    deployment_id: str | None,
    dry_run: bool,
    prices_source: str | None,
) -> None:
    """Backfill LP_CLOSE rows broken by the pre-VIB-4839 silent-cache bug.

    Detects LP CLOSE rows where token0='' AND token1='' AND value_usd=''
    (the parser-didn't-emit shape; NEVER NULL or '0'), carries the immutable
    bracket forward from the matching OPEN, recomputes value_usd from the
    received amounts × execution-time prices (transaction_ledger
    price_inputs_json), and backfills attribution_json.principal_recovered_usd.

    Stop the strategy before running (1 strategy : 1 DB). Default mode backs up
    the DB to ``<db>.bak-<UTC-ts>`` before the first write and applies all
    updates in a single transaction. Idempotent: a second run repairs 0 rows.

    \b
    Examples:
        almanak strat repair-teardown-lp-close --db ./almanak_state.db
        almanak strat repair-teardown-lp-close --db ./state.db --dry-run
        almanak strat repair-teardown-lp-close --db ./state.db -s MyStrat:abc123
    """
    resolved_db = db_path or _default_db_path()
    if not Path(resolved_db).is_file():
        click.secho(
            f"State DB not found at {resolved_db}. Pass --db with the strategy's "
            "SQLite path (and stop the strategy first).",
            fg="red",
            err=True,
        )
        sys.exit(1)

    try:
        result = repair_teardown_lp_close(
            resolved_db,
            deployment_id=deployment_id,
            dry_run=dry_run,
            prices_source=prices_source,
        )
    except (FileNotFoundError, ValueError) as exc:
        click.secho(f"Repair failed: {exc}", fg="red", err=True)
        sys.exit(1)
    except Exception as exc:  # noqa: BLE001 — surface any unexpected DB error
        click.secho(f"Repair failed (unexpected): {exc}", fg="red", err=True)
        sys.exit(1)

    _print_diff(result)
    _print_summary(result)
