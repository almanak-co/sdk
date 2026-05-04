"""Filtered Accountant Test reporting (VIB-3870).

This module is the **reporting primitive** half of the
test-harness-vs-reporting split called out in VIB-3870 / Codex's review of
PR #1997. The :mod:`accountant_test` module stays as the test harness:
small fixture, full ``SELECT *``, designed to run against a strategy
folder's SQLite DB after a real round-trip.

What lives *here*:

* :func:`accountant_report_from_db` — typed, filtered query over the
  same SDK-owned accounting tables. Filters by ``strategy_id``,
  ``deployment_id``, ``cycle_ids``, ``since`` / ``until`` time window, or
  a tax-period label that resolves to a calendar window. Re-uses the
  cell evaluators from :mod:`accountant_test` so the matrix concept
  extends transparently to long-lived hosted strategies.
* :class:`AccountingReportFilter` — typed filter spec for callers that
  prefer a value object over kwargs.
* :class:`TaxPeriod` — calendar resolution helper for ``"FY2026"`` /
  ``"Q1-2026"`` / ``"Q2-2026"`` / etc.

Both SQLite (local) and Postgres (hosted) are usable as backends — the
caller passes any object that quacks like ``sqlite3.Connection`` for the
``Connection`` parameter (i.e. exposes ``cursor()`` returning a cursor
with ``execute(sql, params)`` and ``fetchall()``). For Postgres the
caller is responsible for translating ``?`` placeholders to ``%s`` if
needed; the helper functions here use the SQLite ``?`` style.

Per the gateway-boundary contract in ``CLAUDE.md``, this module does NOT
open its own DB connections — callers pass connections in. That keeps
the module testable against in-memory SQLite without leaking egress
boundaries.
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from almanak.framework.accounting.accountant_test import (
    AccountantReport,
    Primitive,
    evaluate_cells,
)

# ─── Tax period resolution ───────────────────────────────────────────────


@dataclass(frozen=True)
class TaxPeriod:
    """A calendar window expressed as ISO-8601 timestamps.

    Use :meth:`from_label` to resolve a human label like ``"FY2026"`` or
    ``"Q2-2026"`` to start/end timestamps. The resolved values bound the
    accounting query inclusively at the start and exclusively at the end —
    matches typical ``where timestamp >= since AND timestamp < until``
    semantics for half-open intervals.
    """

    label: str
    since: datetime
    until: datetime

    @classmethod
    def from_label(cls, label: str) -> TaxPeriod:
        """Resolve a tax-period label to a calendar window.

        Supported forms:

        * ``"FYYYYY"`` — full fiscal year (Jan 1 → Jan 1 next year)
        * ``"Qn-YYYY"`` — calendar quarter; ``n`` ∈ ``{1, 2, 3, 4}``

        Raises :class:`ValueError` for unrecognised labels — silently
        returning an empty window would be a reporting hazard (the cell
        matrix would evaluate against zero rows and look healthy).
        """
        s = label.strip().upper()
        if s.startswith("FY") and s[2:].isdigit():
            year = int(s[2:])
            return cls(
                label=label,
                since=datetime(year, 1, 1, tzinfo=UTC),
                until=datetime(year + 1, 1, 1, tzinfo=UTC),
            )
        if len(s) == 7 and s[0] == "Q" and s[1] in "1234" and s[2] == "-" and s[3:].isdigit():
            quarter = int(s[1])
            year = int(s[3:])
            start_month = (quarter - 1) * 3 + 1
            end_month = start_month + 3
            since = datetime(year, start_month, 1, tzinfo=UTC)
            if end_month > 12:
                until = datetime(year + 1, 1, 1, tzinfo=UTC)
            else:
                until = datetime(year, end_month, 1, tzinfo=UTC)
            return cls(label=label, since=since, until=until)
        raise ValueError(
            f"Unrecognised tax_period label {label!r} — expected 'FYYYYY' (e.g. 'FY2026') or 'Qn-YYYY' (e.g. 'Q2-2026')"
        )


# ─── Filter spec ─────────────────────────────────────────────────────────


@dataclass(frozen=True)
class AccountingReportFilter:
    """Filter spec for :func:`accountant_report_from_db`.

    All fields default to ``None`` — that means "no filter, include all
    rows". Combine arbitrarily; filters AND together at the SQL layer.
    """

    strategy_id: str | None = None
    deployment_id: str | None = None
    # ``deployment_ids`` accepts a multi-value IN-clause for the
    # multi-deployment fallback path (CodeRabbit round 5, 2026-05-02). When
    # set, the filter applies ``deployment_id IN (deployment_ids)`` on every
    # table that carries a deployment_id column. ``deployment_id`` (single
    # value) and ``deployment_ids`` (multi-value) are mutually exclusive at
    # the call site — the public entrypoint accepts the singular form and
    # internally builds the plural for the position_events fallback.
    deployment_ids: tuple[str, ...] | None = None
    cycle_ids: tuple[str, ...] | None = None
    since: datetime | None = None
    until: datetime | None = None
    tax_period: str | None = None

    def resolved_window(self) -> tuple[datetime | None, datetime | None]:
        """Return the effective (since, until) after resolving ``tax_period``.

        ``tax_period`` and explicit ``since`` / ``until`` cannot both be
        set — the caller must pick one. Mixing is a reporting-correctness
        hazard (which one wins is non-obvious) so we raise instead of
        silently picking.
        """
        if self.tax_period and (self.since or self.until):
            raise ValueError(
                "tax_period is mutually exclusive with explicit since/until — set one or the other, not both"
            )
        if self.tax_period:
            tp = TaxPeriod.from_label(self.tax_period)
            return tp.since, tp.until
        return self.since, self.until


# ─── Query helpers ───────────────────────────────────────────────────────


# Per-table list of filter columns. The "core" filters (strategy_id,
# deployment_id, timestamp window) apply to every table; cycle_ids
# restricts at the row level the same way. We hold the column list per
# table because portfolio_metrics has only `strategy_id` (no cycle_id /
# timestamp on the canonical row).
_TABLE_FILTER_COLUMNS: dict[str, dict[str, str]] = {
    "transaction_ledger": {
        "strategy_id": "strategy_id",
        "deployment_id": "deployment_id",
        "cycle_id": "cycle_id",
        "timestamp": "timestamp",
    },
    "position_events": {
        "deployment_id": "deployment_id",
        "cycle_id": "cycle_id",
        "timestamp": "timestamp",
    },
    "accounting_events": {
        "strategy_id": "strategy_id",
        "deployment_id": "deployment_id",
        "cycle_id": "cycle_id",
        "timestamp": "timestamp",
    },
    "portfolio_snapshots": {
        "strategy_id": "strategy_id",
        "deployment_id": "deployment_id",
        "cycle_id": "cycle_id",
        "timestamp": "timestamp",
    },
    "portfolio_metrics": {
        "strategy_id": "strategy_id",
        "deployment_id": "deployment_id",
    },
    "position_state_snapshots": {
        "strategy_id": "strategy_id",
        "deployment_id": "deployment_id",
        "cycle_id": "cycle_id",
        "timestamp": "captured_at",
    },
}


def _table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    """Return the set of column names present on ``table``.

    Lets us downgrade filters gracefully when the live schema is older
    than the canonical column set (e.g. a strategy DB that predates
    ``deployment_id``). A query that hard-codes a missing column would
    raise ``OperationalError`` and cause a confusing crash for the
    reporting caller — silently dropping the filter for an absent column
    is the right behaviour because the column-not-present case is
    semantically equivalent to "no rows match the filter on that
    dimension".
    """
    cur = conn.cursor()
    try:
        cur.execute(f"PRAGMA table_info({table})")  # noqa: S608 — whitelisted
    except sqlite3.OperationalError:
        return set()
    return {row[1] for row in cur.fetchall()}


def _filtered_rows(  # noqa: C901
    conn: sqlite3.Connection,
    table: str,
    filt: AccountingReportFilter,
) -> list[dict[str, Any]]:
    """Read filtered rows from one of the SDK accounting tables.

    Hand-rolled SQL because :mod:`accountant_test` reads the same tables
    permissively (``SELECT *``) for the test-harness path — splitting the
    filter logic keeps the harness code simple and the reporting code
    explicit about which dimensions it can filter.

    Parameters bind via ``?`` placeholders. Identifiers (table + column
    names) come from a whitelist (``_TABLE_FILTER_COLUMNS``), not user
    input, so identifier interpolation is safe here.
    """
    if table not in _TABLE_FILTER_COLUMNS:
        raise ValueError(f"_filtered_rows: unknown table {table!r}")
    cols = _TABLE_FILTER_COLUMNS[table]
    live_cols = _table_columns(conn, table)
    # If the table doesn't exist at all, return empty. Matches the
    # existing _table_rows behaviour in accountant_test.py.
    if not live_cols:
        return []

    where: list[str] = []
    params: list[Any] = []

    # CodeRabbit (2026-05-02): when the caller asks for a filter dimension
    # that the live table doesn't carry, silently dropping the predicate
    # would widen the read past the requested scope and contaminate the
    # cell matrix. Return ``[]`` instead — "no rows match a filter we
    # can't enforce" is the only safe answer.
    if filt.strategy_id and "strategy_id" in cols:
        if cols["strategy_id"] not in live_cols:
            return []
        where.append(f"{cols['strategy_id']} = ?")
        params.append(filt.strategy_id)
    if filt.deployment_id and "deployment_id" in cols:
        if cols["deployment_id"] not in live_cols:
            return []
        where.append(f"{cols['deployment_id']} = ?")
        params.append(filt.deployment_id)
    # CodeRabbit (round 5, 2026-05-02): multi-value deployment_ids — used by
    # the multi-deployment fallback in accountant_report_from_db when a
    # strategy redeployed across multiple deployment_ids. Position_events
    # already carries a deployment_id column so we use it directly instead
    # of falling back to cycle_ids; safer because cycle_ids could overlap
    # across deployments.
    if filt.deployment_ids is not None and "deployment_id" in cols:
        if cols["deployment_id"] not in live_cols:
            return []
        # Empty tuple → "match nothing", not "no filter" (CodeRabbit r5).
        if len(filt.deployment_ids) == 0:
            return []
        placeholders = ",".join("?" for _ in filt.deployment_ids)
        where.append(f"{cols['deployment_id']} IN ({placeholders})")
        params.extend(filt.deployment_ids)
    # CodeRabbit (round 5, 2026-05-02): explicit empty cycle_ids = ()
    # MUST be treated as "match nothing", not "no filter". The previous
    # truthy check dropped the predicate entirely on an empty tuple,
    # widening the query back to all rows. Use ``is not None`` to keep
    # the predicate active even when the caller deliberately passed [].
    if filt.cycle_ids is not None and "cycle_id" in cols:
        if cols["cycle_id"] not in live_cols:
            return []
        if len(filt.cycle_ids) == 0:
            return []
        # SQLite has a default 999 host-parameter limit; chunking would be
        # over-engineering for "list of cycle_ids" sized inputs (typically
        # tens to hundreds). If a caller hits this, they're probably
        # better-served by a `since`/`until` filter anyway.
        placeholders = ",".join("?" for _ in filt.cycle_ids)
        where.append(f"{cols['cycle_id']} IN ({placeholders})")
        params.extend(filt.cycle_ids)

    since, until = filt.resolved_window()
    if (since or until) and "timestamp" in cols:
        if cols["timestamp"] not in live_cols:
            # CodeRabbit (2026-05-02 round 4): caller requested a time
            # window but the live table lacks a timestamp column. Return
            # [] for symmetry with the strategy_id / deployment_id /
            # cycle_ids paths above — silently widening to all rows would
            # contaminate tax-period reports on older schemas.
            return []
        ts_col = cols["timestamp"]
        if since:
            where.append(f"{ts_col} >= ?")
            params.append(since.isoformat())
        if until:
            where.append(f"{ts_col} < ?")
            params.append(until.isoformat())

    sql = f"SELECT * FROM {table}"  # noqa: S608 — whitelisted identifier
    if where:
        sql += " WHERE " + " AND ".join(where)

    # Force `Row` factory so we can `dict(r)` consistently.
    prior_factory = conn.row_factory
    conn.row_factory = sqlite3.Row
    try:
        cur = conn.cursor()
        cur.execute(sql, params)
        return [dict(r) for r in cur.fetchall()]
    finally:
        conn.row_factory = prior_factory


# ─── Public entrypoint ───────────────────────────────────────────────────


def accountant_report_from_db(
    conn_or_path: sqlite3.Connection | str | Path,
    *,
    primitive: Primitive,
    strategy_id: str | None = None,
    deployment_id: str | None = None,
    cycle_ids: list[str] | tuple[str, ...] | None = None,
    since: datetime | None = None,
    until: datetime | None = None,
    tax_period: str | None = None,
) -> AccountantReport:
    """Run the Accountant Test cell matrix over a *filtered* view of the DB.

    ``conn_or_path`` is either a live ``sqlite3.Connection`` (caller owns
    lifecycle) or a path to a SQLite file (this function opens + closes).
    Identical cell evaluators run against the filtered rows — a green
    matrix here is the same "ship signal" as a green
    :func:`run_against_sqlite`, just over a slice of the strategy's
    history rather than the whole history.

    Filters AND together. ``tax_period`` and ``since/until`` are mutually
    exclusive (see :class:`AccountingReportFilter` for rationale).

    Parameters
    ----------
    conn_or_path
        SQLite connection or path. When a path is passed, the connection
        is opened in read-only mode-equivalent style (no writes are
        attempted) and closed before returning.
    primitive
        ``"lp"``, ``"looping"``, or ``"perp"`` — selects the
        primitive-specific cells appended to the 15 generic cells.
    strategy_id, deployment_id, cycle_ids, since, until, tax_period
        Filter dimensions. All optional. ``None`` means "no filter on
        this dimension". ``cycle_ids`` accepts list or tuple.
    """
    filt = AccountingReportFilter(
        strategy_id=strategy_id,
        deployment_id=deployment_id,
        cycle_ids=tuple(cycle_ids) if cycle_ids is not None else None,
        since=since,
        until=until,
        tax_period=tax_period,
    )

    conn: sqlite3.Connection
    own_connection = isinstance(conn_or_path, str | Path)
    if isinstance(conn_or_path, str | Path):
        # Claude pr-auditor finding #1 (2026-05-02): refuse to silently create
        # an empty SQLite file when the path doesn't exist. A typo in --db
        # would otherwise produce a "0 PASS / N FAIL" report against an empty
        # DB and look like a strategy regression. Mirror the actionable
        # message from accountant_test_cli._resolve_db_path.
        path_obj = Path(str(conn_or_path))
        if not path_obj.is_file():
            raise FileNotFoundError(f"DB file does not exist: {path_obj}")
        conn = sqlite3.connect(str(conn_or_path))
    else:
        conn = conn_or_path

    try:
        ledger = _filtered_rows(conn, "transaction_ledger", filt)
        # Codex P2 / CodeRabbit (2026-05-02): position_events has no
        # strategy_id column. A bare strategy_id filter would silently leak
        # cross-strategy rows on a shared/hosted DB, contaminating LP1/LP3/
        # G7 cells. When strategy_id is requested but deployment_id is not
        # supplied directly, derive matching deployment_ids from the
        # filtered ledger (which DOES carry strategy_id) and use them to
        # narrow position_events. If the ledger has zero rows for this
        # strategy in the requested window, return zero position_events
        # rather than ALL position_events.
        if filt.strategy_id and not filt.deployment_id:
            ledger_deployment_ids = sorted(
                {
                    str(r.get("deployment_id"))
                    for r in ledger
                    if r.get("deployment_id") is not None and str(r.get("deployment_id")) != ""
                }
            )
            if not ledger_deployment_ids:
                # No ledger rows for this strategy → no position_events to
                # report on either. Skip the unscoped read entirely.
                pos_events: list[dict[str, Any]] = []
            elif len(ledger_deployment_ids) == 1:
                # Exactly one deployment_id — narrow position_events with it.
                derived = AccountingReportFilter(
                    strategy_id=filt.strategy_id,
                    deployment_id=ledger_deployment_ids[0],
                    cycle_ids=filt.cycle_ids,
                    since=filt.since,
                    until=filt.until,
                    tax_period=filt.tax_period,
                )
                pos_events = _filtered_rows(conn, "position_events", derived)
            else:
                # Multiple deployment_ids for the same strategy_id is
                # legal (re-deploy). CodeRabbit (round 5, 2026-05-02): use
                # ``deployment_id IN (ledger_deployment_ids)`` directly —
                # cycle_ids could overlap across deployments and the
                # cycle_id-only fallback would leak cross-deployment rows.
                derived = AccountingReportFilter(
                    strategy_id=filt.strategy_id,
                    deployment_id=None,
                    deployment_ids=tuple(ledger_deployment_ids),
                    cycle_ids=filt.cycle_ids,
                    since=filt.since,
                    until=filt.until,
                    tax_period=filt.tax_period,
                )
                pos_events = _filtered_rows(conn, "position_events", derived)
        else:
            pos_events = _filtered_rows(conn, "position_events", filt)
        acct_events = _filtered_rows(conn, "accounting_events", filt)
        snapshots = _filtered_rows(conn, "portfolio_snapshots", filt)
        metrics = _filtered_rows(conn, "portfolio_metrics", filt)
        position_state_rows = _filtered_rows(conn, "position_state_snapshots", filt)
    finally:
        if own_connection:
            conn.close()

    db_path: str | None = None
    if own_connection:
        db_path = f"{conn_or_path} (filtered: {filt})"

    return evaluate_cells(
        ledger=ledger,
        pos_events=pos_events,
        acct_events=acct_events,
        snapshots=snapshots,
        metrics=metrics,
        position_state_rows=position_state_rows,
        primitive=primitive,
        db_dump_path=db_path,
    )


__all__ = [
    "AccountingReportFilter",
    "TaxPeriod",
    "accountant_report_from_db",
]
