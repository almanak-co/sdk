"""CLI command for the frozen-book ``position_reference`` repair (VIB-6552).

Provides ``almanak strat repair-position-references --db <path>`` — an offline
operator command that re-points ``accounting_events.position_reference`` rows
left at ``source="legacy"`` by the pre-VIB-6346 write ordering (GMX V2's venue
positionKey is unknowable at Phase-1; the registry row lands at keeper
settlement; nothing revisited the Phase-1 rows). The write-path half shipped
with VIB-6346 and fires only on a registry write — which a ``closed`` (terminal)
registry row never receives again — so existing frozen books need this explicit
entry point. Moves Accountant Test cell L5_22 FAIL → PASS on such books.

No gateway call is made — this is a local-only DML repair against a stopped
strategy's state DB. The engine is
:meth:`almanak.framework.state.backends.sqlite.SQLiteStore.repair_frozen_position_references`
— the same join predicate, ambiguity safeguards, and single-key
``restamp_position_reference`` writer as the in-process repair, so the two
halves cannot drift. This module is the thin click wrapper (arg parsing, DB
resolution, summary printing).

Also runnable as a module (the UAT card's ``MIGRATION_CMD`` slot invokes it
this way): ``python -m almanak.framework.cli.repair_position_references --db <path>``.

Usage:
    almanak strat repair-position-references --db ./almanak_state.db
    almanak strat repair-position-references --db ./state.db --dry-run
    almanak strat repair-position-references --db ./state.db -s deployment:abc123def456
"""

from __future__ import annotations

import logging
import sys

import click


def _default_db_path() -> str:
    """Resolve the canonical local DB path (mirrors ``repair-teardown-lp-close``)."""
    from almanak.framework.local_paths import LocalPathError, local_db_path

    try:
        return str(local_db_path())
    except LocalPathError:
        return ":hosted-mode-no-sqlite-path:"


def _print_report(result) -> None:  # noqa: ANN001 — FrozenBookRepairResult, imported lazily
    if result.registry_absent:
        click.secho(
            "No position_registry table in this DB — a legacy book has nothing to join against. Nothing to repair.",
            fg="green",
        )
        return
    if result.deployment_id and result.registry_rows == 0:
        # `-s` takes a deployment id (`deployment:<12-hex>`), and elsewhere in
        # the `strat backtest` group the same short flag means a strategy
        # name — a mixed-up value silently scopes the repair to nothing and
        # would otherwise read as a clean no-op (CodeRabbit, PR #3615).
        click.secho(
            f"WARNING: no position_registry rows match deployment id "
            f"'{result.deployment_id}'. If you meant to repair this book, check "
            "the id — it is the runner's 'deployment:<hash>' string (from the "
            "boot log or strategy_state.deployment_id), NOT the strategy name.",
            fg="yellow",
        )
    click.echo(f"Registry rows examined: {result.registry_rows}")
    for anomaly in result.anomalies:
        click.secho(f"  INTEGRITY ANOMALY: {anomaly}", fg="red")
    for skip in result.skips:
        click.secho(
            f"  SKIPPED phid={skip.physical_identity_hash} (deployment={skip.deployment_id} chain={skip.chain}): "
            f"{skip.reason} — {skip.detail}",
            fg="red",
        )
    click.echo("")
    click.secho("Summary", bold=True)
    click.echo(f"  registry rows: {result.registry_rows}")
    click.echo(f"  events re-pointed: {result.repaired_events}")
    click.echo(f"  rows skipped: {result.skipped_rows}")
    if result.dry_run:
        click.secho("  DRY RUN — no changes written, no backup created.", fg="cyan")
    elif not result.written:
        click.secho("  Nothing needed re-pointing. No write, no backup.", fg="green")
    else:
        click.echo(f"  backup:  {result.backup_path}")
        click.secho("  Committed.", fg="green")


@click.command("repair-position-references")
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
    help="Run the real repair in a rolled-back transaction and report what it would write.",
)
def repair_position_references_cmd(
    db_path: str | None,
    deployment_id: str | None,
    dry_run: bool,
) -> None:
    """Re-point legacy position_reference rows at their settled registry rows.

    Repairs books written before the VIB-6346 write-ordering fix: PERP_OPEN /
    PERP_CLOSE accounting rows stuck at source="legacy" while their
    position_registry row is closed (Accountant Test cell L5_22 "inverse
    orphan"). Only the position_reference key is touched — never any other
    payload key, and never the registry.

    Stop the strategy before running (1 strategy : 1 DB). Default mode backs up
    the DB to ``<db>.bak-<UTC-ts>`` before writing and applies all updates in a
    single transaction. Idempotent: a second run repairs 0 rows. Rows with
    integrity anomalies are skipped loudly, never overwritten — a skip is a
    reported outcome, not an error, so the exit code stays 0.

    \b
    Examples:
        almanak strat repair-position-references --db ./almanak_state.db
        almanak strat repair-position-references --db ./state.db --dry-run
        almanak strat repair-position-references --db ./state.db -s deployment:abc123def456
    """
    from pathlib import Path

    from almanak.framework.local_paths import LocalDbLockError
    from almanak.framework.state.backends.sqlite import SQLiteConfig, SQLiteStore

    resolved_db = db_path or _default_db_path()
    if not Path(resolved_db).is_file():
        click.secho(
            f"State DB not found at {resolved_db}. Pass --db with the strategy's "
            "SQLite path (and stop the strategy first).",
            fg="red",
            err=True,
        )
        sys.exit(1)

    store = SQLiteStore(SQLiteConfig(db_path=resolved_db))
    try:
        result = store.repair_frozen_position_references(
            deployment_id=deployment_id,
            dry_run=dry_run,
        )
    except LocalDbLockError as exc:
        click.secho(
            f"Repair refused: {exc}\nA running gateway/strategy owns this DB — "
            "stop it before repairing (1 strategy : 1 DB).",
            fg="red",
            err=True,
        )
        sys.exit(1)
    except (FileNotFoundError, ValueError) as exc:
        click.secho(f"Repair failed: {exc}", fg="red", err=True)
        sys.exit(1)
    except Exception as exc:  # noqa: BLE001 — surface any unexpected DB error
        click.secho(f"Repair failed (unexpected): {type(exc).__name__}: {exc}", fg="red", err=True)
        logging.getLogger(__name__).error("repair-position-references failed unexpectedly", exc_info=True)
        sys.exit(1)

    _print_report(result)


if __name__ == "__main__":
    repair_position_references_cmd()
