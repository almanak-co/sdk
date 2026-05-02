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
import os
import sys
from pathlib import Path

from almanak.framework.accounting.accountant_test import run_against_sqlite


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

    from almanak.framework.local_paths import LocalPathError, local_strategy_db_path

    # ``local_strategy_db_path`` honours ALMANAK_STATE_DB / ALMANAK_STRATEGY_FOLDER
    # before falling back. When the caller passes -d, scope it via the env var
    # so the helper resolves to <folder>/almanak_state.db.
    prior = os.environ.get("ALMANAK_STRATEGY_FOLDER")
    try:
        if strategy_folder:
            os.environ["ALMANAK_STRATEGY_FOLDER"] = str(Path(strategy_folder).expanduser().resolve())
        try:
            p = local_strategy_db_path()
        except LocalPathError as e:
            raise SystemExit(
                f"Cannot resolve strategy DB path: {e}\n"
                f"Pass --db <path>, set ALMANAK_STATE_DB, or use -d/--working-dir."
            ) from None
    finally:
        if strategy_folder:
            if prior is None:
                os.environ.pop("ALMANAK_STRATEGY_FOLDER", None)
            else:
                os.environ["ALMANAK_STRATEGY_FOLDER"] = prior

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
        help="Strategy folder. Falls back to cwd. Folder must contain almanak_state.db (VIB-3761).",
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
    args = p.parse_args(argv)

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

    # Exit non-zero when at least one cell FAILed (XFAIL is fine — those are
    # tracked-as-deferred per the §6 Track structure). SKIP is fine.
    return 1 if report.failed > 0 else 0


if __name__ == "__main__":
    raise SystemExit(main())
