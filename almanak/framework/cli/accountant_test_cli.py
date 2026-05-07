"""`almanak strat accountant-test` — run the Accountant Test against a strategy DB.

This is the runnable form of D1 in `docs/internal/Accounting-AttemptNo17.md`.

Usage::

    python -m almanak.framework.cli.accountant_test_cli \
        -d strategies/accounting/lp \
        --primitive lp \
        --report-out docs/internal/AccountantTest-lp-anvil.md

When no ``--db`` and no ``-d`` is given, falls back to the cwd-detected
strategy folder per VIB-3835 / VIB-3761.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from almanak.framework.accounting.accountant_test import run_against_sqlite


# crap-allowlist: Phase 5e (#2097) swaps four direct ``os.environ.<get|pop|set>``
# calls (the prior save / restore-or-pop pattern around ALMANAK_STRATEGY_FOLDER)
# for the typed :func:`push_strategy_folder` / :func:`pop_strategy_folder` helpers
# in ``almanak.framework.local_paths``. The function's CC (9) is structural to the
# resolution / validation ladder (explicit-db branch + folder-scoped branch + four
# distinct file-shape error paths); targeted unit coverage is owned by the
# accountant-test smoke harness rather than an isolated test class.
def _resolve_db_path(strategy_folder: str | None, explicit_db: str | None) -> Path:
    """Resolve the sqlite DB path with the same rules as VIB-3835.

    Defers to :func:`almanak.framework.local_paths.local_strategy_db_path`
    (the canonical SQLite-resolution helper) when no explicit ``--db`` is
    given, so this CLI stays in lockstep with the rest of the framework.
    The legacy cwd-relative ``./almanak_state.db`` default was removed for
    exactly this reason — duplicating the path rule here is how April 29's
    silent-failure shape recurred in helper code.
    """
    if explicit_db:
        p = Path(explicit_db).expanduser().resolve()
        if not p.is_file():
            # ``exists()`` would also accept a directory and defer the failure
            # to ``sqlite3.connect`` with a less actionable message; require a
            # regular file up front.
            if p.exists():
                raise SystemExit(f"--db must point at a regular file, got: {p}")
            raise SystemExit(f"DB file does not exist: {p}")
        return p

    from almanak.framework.local_paths import (
        LocalPathError,
        local_strategy_db_path,
        pop_strategy_folder,
        push_strategy_folder,
        strategy_folder_env,
    )

    # ``local_strategy_db_path`` honours ALMANAK_STATE_DB / ALMANAK_STRATEGY_FOLDER
    # before falling back. When the caller passes -d, scope it via the env var
    # so the helper resolves to <folder>/almanak_state.db.
    if strategy_folder:
        prior = push_strategy_folder(Path(strategy_folder).expanduser().resolve())
    else:
        prior = strategy_folder_env()
    try:
        try:
            p = local_strategy_db_path()
        except LocalPathError as e:
            raise SystemExit(
                f"Cannot resolve strategy DB path: {e}\n"
                f"Pass --db <path>, set ALMANAK_STATE_DB, or use -d/--working-dir."
            ) from None
    finally:
        if strategy_folder:
            pop_strategy_folder(prior)

    if not p.is_file():
        # ``exists()`` would also accept a directory and defer the failure
        # to ``sqlite3.connect`` with an unhelpful error; symmetric with
        # the explicit ``--db`` branch above.
        if p.exists():
            raise SystemExit(
                f"Folder-scoped DB path is not a regular file: {p}\n"
                f"Pass --db <path> or run from inside the strategy folder, or use -d/--working-dir."
            )
        raise SystemExit(
            f"DB file does not exist at folder-scoped path: {p}\n"
            f"Pass --db <path> or run from inside the strategy folder, or use -d/--working-dir."
        )
    return p


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(
        prog="almanak-accountant-test",
        description="Run the Accountant Test (Accounting-AttemptNo17 §1) against a strategy DB.",
    )
    p.add_argument(
        "-d",
        "--working-dir",
        dest="working_dir",
        default=None,
        help="Strategy folder. Falls back to cwd. Folder must contain almanak_state.db.",
    )
    p.add_argument("--db", default=None, help="Explicit path to a sqlite DB file (overrides -d).")
    p.add_argument(
        "--primitive",
        choices=["lp", "looping", "perp"],
        required=True,
        help="Which primitive's cell matrix to evaluate.",
    )
    p.add_argument(
        "--report-out",
        default=None,
        help="Write the markdown report to this file (defaults to stdout).",
    )
    # VIB-3870: gating modes. The default behaviour (exit 0 unless any cell
    # FAILs) is the *progress scorecard* mode — fine for daily Anvil
    # smokes. CI / production deploys should use --strict (any non-PASS
    # status fails) or --require-cells (specific cells must PASS).
    p.add_argument(
        "--strict",
        action="store_true",
        help="Strict mode: exit non-zero on any non-PASS cell (FAIL, XFAIL, or SKIP). "
        "Use this for CI / production-readiness gating — XFAIL on a Track-C-dependent "
        "cell will fail the gate, which is the correct behaviour for ship signals.",
    )
    p.add_argument(
        "--require-cells",
        default=None,
        help="Comma-separated cell IDs that must PASS (e.g. 'G2,G6,G12,LP1,LP3'). "
        "Exits non-zero if any listed cell is not PASS. Mutually exclusive with --strict; "
        "use this when only a subset of cells is meaningful for a given deploy gate.",
    )
    args = p.parse_args(argv)

    if args.strict and args.require_cells:
        sys.stderr.write("error: --strict and --require-cells are mutually exclusive — pick one\n")
        return 2

    db_path = _resolve_db_path(args.working_dir, args.db)
    report = run_against_sqlite(db_path, primitive=args.primitive)
    md = report.format_markdown()

    if args.report_out:
        out_path = Path(args.report_out).expanduser().resolve()
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(md, encoding="utf-8")
        sys.stderr.write(f"Wrote report to {out_path}\n")
    else:
        sys.stdout.write(md + "\n")

    # Default: exit non-zero only on FAIL. XFAIL/SKIP are deferred-tracking
    # statuses per AttemptNo17 §6 and don't gate the progress scorecard.
    if args.require_cells:
        required_ids = {x.strip() for x in args.require_cells.split(",") if x.strip()}
        cells_by_id = {c.cell_id: c for c in report.cells}
        unknown = required_ids - set(cells_by_id.keys())
        if unknown:
            sys.stderr.write(f"error: --require-cells references unknown cell IDs: {sorted(unknown)}\n")
            return 2
        not_passing = [cid for cid in sorted(required_ids) if cells_by_id[cid].status != "PASS"]
        if not_passing:
            sys.stderr.write(
                f"--require-cells gate: {len(not_passing)} of {len(required_ids)} required "
                f"cells did not PASS: {not_passing}\n"
            )
            return 1
        return 0
    if args.strict:
        non_pass = [c for c in report.cells if c.status != "PASS"]
        if non_pass:
            summary = ", ".join(f"{c.cell_id}={c.status}" for c in non_pass[:10])
            sys.stderr.write(
                f"--strict gate: {len(non_pass)} of {report.total_cells} cells did not PASS (e.g. {summary})\n"
            )
            return 1
        return 0
    return 1 if report.failed > 0 else 0


if __name__ == "__main__":
    raise SystemExit(main())
