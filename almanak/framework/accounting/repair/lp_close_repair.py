"""LP_CLOSE teardown-bug repair engine (VIB-4896).

Backfills ``position_events`` LP_CLOSE rows broken by the pre-VIB-4839
silent-cache bug: when the runner's in-memory ``_recent_open_events`` cache
was cold (hosted GSM, cross-process signal-driven teardown), the teardown
LP_CLOSE landed with ``token0=''``, ``token1=''`` and ``value_usd=''`` — and
its ``attribution_json.principal_recovered_usd`` consequently computed to 0.
VIB-4839 fixed the live path; this engine repairs *existing* DBs that were
written before that fix and cannot be re-derived on-chain.

Design (blueprint 27 §14.1; APPROVED VIB-4896):

* **Detection predicate (Empty ≠ Zero, exact):** an LP CLOSE row with
  ``token0='' AND token1='' AND value_usd=''`` (the parser-didn't-emit
  shape). NEVER NULL (unmeasured) and NEVER ``'0'`` (measured zero).
* **OPEN selection:** ``select_open_for_lp_close`` (shared with the runner's
  durable-hydration fallback), bounded by the CLOSE row's timestamp so a
  later re-open of the same ``position_id`` does not leak its bracket.
* **Price source:** ``transaction_ledger.price_inputs_json`` joined via
  ``position_events.ledger_entry_id`` (VIB-3480 home of execution-time,
  symbol-keyed ``{"price_usd": ...}`` prices). ``--prices-source`` is an
  opt-in override for the degraded case only.
* **value_usd math:** ``compute_lp_close_value_usd`` (shared with the live
  enricher). Fail-closed: leave ``value_usd=''`` and mark the row skipped
  (reason ``price_unavailable``) — NEVER write 0.
* **principal_recovered_usd:** backfilled directly (= recomputed value_usd),
  merged into the existing ``attribution_json`` (preserving other keys), with
  ``attribution_version = CURRENT_VERSION``. We do NOT rely on
  ``recompute_attribution`` (it skips already-current rows). The
  matching-policy version is unchanged (this is a data backfill, not an
  algorithm change).

This is local SQLite DML (CLAUDE.md §Database schema ownership: local SQLite
is SDK-owned). No gateway egress, no schema change.
"""

from __future__ import annotations

import json
import logging
import sqlite3
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

from almanak.framework.observability.pnl_attributor import (
    CURRENT_VERSION,
    select_open_for_lp_close,
)
from almanak.framework.observability.position_events import compute_lp_close_value_usd

logger = logging.getLogger(__name__)


# Skip reasons (stable strings — referenced by tests and operator-facing
# diff output). A row carrying a skip reason was NOT fully repaired:
# value_usd stays '' (Empty ≠ Zero), though immutable bracket columns are
# still carried forward where knowable.
SKIP_NO_MATCHING_OPEN = "no_matching_open"
SKIP_MISSING_AMOUNTS = "missing_amounts"
SKIP_PRICE_UNAVAILABLE = "price_unavailable"

# Provenance tags stamped onto a repaired row's attribution_json so the
# source of the backfilled prices is auditable after the fact.
PRICE_PROVENANCE_LEDGER = "ledger_price_inputs_json"
PRICE_PROVENANCE_OVERRIDE = "prices_source_override"


@dataclass
class RepairedRow:
    """Per-row outcome of a repair pass (one broken LP_CLOSE row)."""

    event_id: str
    position_id: str
    chain: str
    # Carried-forward immutable bracket (None when no OPEN matched).
    token0: str = ""
    token1: str = ""
    tick_lower: int | None = None
    tick_upper: int | None = None
    liquidity: str = ""
    # Recomputed close value (empty string = fail-closed, never 0).
    value_usd: str = ""
    principal_recovered_usd: str = ""
    price_provenance: str = ""
    # None = fully repaired; otherwise one of the SKIP_* sentinels.
    skip_reason: str | None = None
    # True when this row's UPDATE was actually written (False in dry-run or
    # when nothing changed).
    written: bool = False

    @property
    def repaired(self) -> bool:
        """Whether value_usd was recomputed (the headline success metric)."""
        return self.skip_reason is None and bool(self.value_usd)


@dataclass
class LpCloseRepairResult:
    """Aggregate outcome of a repair pass."""

    db_path: str
    dry_run: bool
    deployment_id: str | None = None
    backup_path: str | None = None
    rows: list[RepairedRow] = field(default_factory=list)

    @property
    def detected(self) -> int:
        return len(self.rows)

    @property
    def repaired(self) -> int:
        return sum(1 for r in self.rows if r.repaired)

    @property
    def written(self) -> int:
        return sum(1 for r in self.rows if r.written)

    @property
    def skipped(self) -> int:
        return sum(1 for r in self.rows if r.skip_reason is not None)


# Detection predicate (Empty ≠ Zero — exact three-empty-string shape). Bound
# parameters are unnecessary because the literals are constant; kept inline
# for clarity. LP position_type only.
_DETECT_SQL = """
    SELECT id, deployment_id, position_id, position_type, event_type,
           timestamp, chain, amount0, amount1, ledger_entry_id, attribution_json
    FROM position_events
    WHERE position_type = 'LP'
      AND event_type = 'CLOSE'
      AND token0 = ''
      AND token1 = ''
      AND value_usd = ''
"""


def _load_price_override(prices_source: str | None) -> dict[str, Any]:
    """Load the optional ``--prices-source`` JSON override.

    Expected shape mirrors ``price_inputs_json``: a dict keyed by UPPER-cased
    token symbol -> ``{"price_usd": "<decimal>"}`` or a bare scalar. Returns
    an empty dict when no override is given. Raises on a malformed file so the
    operator gets a loud failure rather than a silent price miss.
    """
    if not prices_source:
        return {}
    path = Path(prices_source)
    if not path.is_file():
        raise FileNotFoundError(f"--prices-source not found: {prices_source}")
    data = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise ValueError(f"--prices-source must be a JSON object keyed by token symbol; got {type(data).__name__}")
    return data


def _ledger_price_inputs(conn: sqlite3.Connection, ledger_entry_id: str) -> dict[str, Any]:
    """Read ``transaction_ledger.price_inputs_json`` for one ledger row.

    Returns an empty dict when the id is missing/empty, the row is absent, or
    the JSON is empty/malformed (the degraded case the override exists for).
    """
    if not ledger_entry_id:
        return {}
    cur = conn.execute(
        "SELECT price_inputs_json FROM transaction_ledger WHERE id = ?",
        (ledger_entry_id,),
    )
    row = cur.fetchone()
    if row is None:
        return {}
    raw = row[0] or ""
    if not raw or raw == "{}":
        return {}
    try:
        parsed = json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _merge_principal(
    attribution_json: str,
    principal_recovered_usd: str,
    *,
    price_provenance: str = "",
) -> str:
    """Merge ``principal_recovered_usd`` (and repair provenance) into attribution_json.

    Preserves every other key (``current_prices``, etc.) and re-serialises.
    Tolerates malformed / empty JSON by starting from ``{}``. When
    ``price_provenance`` is given, the repair provenance tags are stamped in
    the same single parse/serialise pass (no second round-trip).
    """
    try:
        existing = json.loads(attribution_json or "{}")
    except (json.JSONDecodeError, TypeError):
        existing = {}
    if not isinstance(existing, dict):
        existing = {}
    existing["principal_recovered_usd"] = principal_recovered_usd
    if price_provenance:
        existing["repair_price_provenance"] = price_provenance
        existing["repair_ticket"] = "VIB-4896"
    return json.dumps(existing)


def _carry_bracket(open_row: dict, repaired: RepairedRow) -> None:
    """Carry the immutable bracket columns from the OPEN onto ``repaired``.

    Mirrors ``_apply_lp_close_columns``' ``if not event.x and cached_x``
    guards: only populated OPEN fields are carried (partial OPEN data leaves
    the corresponding repaired field at its default).
    """
    t0 = open_row.get("token0")
    t1 = open_row.get("token1")
    if t0:
        repaired.token0 = str(t0)
    if t1:
        repaired.token1 = str(t1)
    tl = open_row.get("tick_lower")
    tu = open_row.get("tick_upper")
    if isinstance(tl, int):
        repaired.tick_lower = tl
    if isinstance(tu, int):
        repaired.tick_upper = tu
    liq = open_row.get("liquidity")
    if liq:
        repaired.liquidity = str(liq)


def _position_history(conn: sqlite3.Connection, deployment_id: str, position_id: str) -> list[dict]:
    """Return all events for one position_id, oldest-first (ASC by timestamp).

    Mirrors ``SQLiteStore.get_position_history`` so ``select_open_for_lp_close``
    sees the same row shape and ordering as the runner.
    """
    cur = conn.execute(
        """
        SELECT id, deployment_id, position_id, position_type, event_type,
               timestamp, token0, token1, amount0, amount1, value_usd,
               tick_lower, tick_upper, liquidity, ledger_entry_id,
               attribution_json, attribution_version
        FROM position_events
        WHERE deployment_id = ? AND position_id = ?
        ORDER BY timestamp ASC
        """,
        (deployment_id, position_id),
    )
    cols = [
        "id",
        "deployment_id",
        "position_id",
        "position_type",
        "event_type",
        "timestamp",
        "token0",
        "token1",
        "amount0",
        "amount1",
        "value_usd",
        "tick_lower",
        "tick_upper",
        "liquidity",
        "ledger_entry_id",
        "attribution_json",
        "attribution_version",
    ]
    return [dict(zip(cols, row, strict=True)) for row in cur.fetchall()]


def _repair_one_row(
    conn: sqlite3.Connection,
    broken: dict,
    price_override: dict[str, Any],
) -> RepairedRow:
    """Compute the repair for a single broken LP_CLOSE row (no writes).

    Returns a :class:`RepairedRow` describing what (if anything) can be
    backfilled. ``written`` is left ``False`` — the caller decides whether to
    persist.
    """
    event_id = str(broken["id"])
    deployment_id = str(broken["deployment_id"] or "")
    position_id = str(broken["position_id"] or "")
    chain = str(broken["chain"] or "")
    close_timestamp = str(broken["timestamp"] or "")
    repaired = RepairedRow(
        event_id=event_id,
        position_id=position_id,
        chain=chain,
        principal_recovered_usd="",
    )

    history = _position_history(conn, deployment_id, position_id)
    # Exclude the CLOSE row being repaired from the walk: its timestamp equals
    # ``close_timestamp`` and ``select_open_for_lp_close`` treats any CLOSE as
    # invalidating the prior OPEN, which would always yield no_matching_open.
    history = [h for h in history if str(h.get("id") or "") != event_id]
    open_row = select_open_for_lp_close(history, close_timestamp=close_timestamp)
    if open_row is None:
        repaired.skip_reason = SKIP_NO_MATCHING_OPEN
        return repaired

    # Carry the immutable bracket forward regardless of whether value_usd can
    # be recomputed (knowable cols are always salvageable).
    _carry_bracket(open_row, repaired)

    amount0 = broken.get("amount0")
    amount1 = broken.get("amount1")
    # Empty ≠ Zero: '' / None amount is unmeasured — cannot recompute value.
    if not amount0 or not amount1:
        repaired.skip_reason = SKIP_MISSING_AMOUNTS
        return repaired
    if not (repaired.token0 and repaired.token1):
        # No tokens carried (partial OPEN) → cannot price.
        repaired.skip_reason = SKIP_MISSING_AMOUNTS
        return repaired

    # Price ladder: ledger price_inputs_json → --prices-source override.
    price_oracle = _ledger_price_inputs(conn, str(broken.get("ledger_entry_id") or ""))
    provenance = PRICE_PROVENANCE_LEDGER
    if not price_oracle and price_override:
        price_oracle = price_override
        provenance = PRICE_PROVENANCE_OVERRIDE
    if not price_oracle:
        repaired.skip_reason = SKIP_PRICE_UNAVAILABLE
        return repaired

    result = compute_lp_close_value_usd(
        repaired.token0,
        repaired.token1,
        amount0,
        amount1,
        price_oracle,
        chain=chain,
        position_id=position_id,
    )
    if not result.value_usd:
        # Fail-closed: leave value_usd='' (NEVER 0). Reason is price-side
        # (the ledger / override didn't carry both prices) — surface it as
        # price_unavailable for the operator.
        repaired.skip_reason = SKIP_PRICE_UNAVAILABLE
        return repaired

    repaired.value_usd = result.value_usd
    repaired.principal_recovered_usd = result.value_usd
    repaired.price_provenance = provenance
    return repaired


def _apply_row(conn: sqlite3.Connection, broken: dict, repaired: RepairedRow) -> None:
    """Persist one repaired row via the dedicated LP_CLOSE-columns UPDATE.

    Carries every knowable column (token0/1, ticks, liquidity, value_usd) plus
    the merged attribution_json (principal_recovered_usd backfilled) and
    ``attribution_version = CURRENT_VERSION``. Skipped rows still carry their
    immutable bracket where knowable, but value_usd stays '' (Empty ≠ Zero).
    """
    merged_attr = (
        _merge_principal(
            str(broken.get("attribution_json") or "{}"),
            repaired.principal_recovered_usd or "",
            price_provenance=repaired.price_provenance,
        )
        if repaired.principal_recovered_usd
        else str(broken.get("attribution_json") or "{}")
    )
    conn.execute(
        """
        UPDATE position_events
        SET token0 = ?, token1 = ?, tick_lower = ?, tick_upper = ?,
            liquidity = ?, value_usd = ?,
            attribution_json = ?, attribution_version = ?
        WHERE id = ?
        """,
        (
            repaired.token0,
            repaired.token1,
            repaired.tick_lower,
            repaired.tick_upper,
            repaired.liquidity,
            repaired.value_usd,
            merged_attr,
            CURRENT_VERSION,
            repaired.event_id,
        ),
    )
    repaired.written = True


def _looks_actively_written(db_path: Path) -> bool:
    """Heuristic: is the DB likely owned by a running strategy right now?

    The 1-strat:1-DB model uses an OS flock; a running strategy also leaves
    SQLite WAL/SHM sidecar files. We treat the presence of a non-empty ``-wal``
    sidecar as the signal. This is advisory only — the operator must stop the
    strategy before repairing (CLAUDE.md §1 Gateway : 1 Strategy).
    """
    wal = db_path.with_name(db_path.name + "-wal")
    try:
        return wal.is_file() and wal.stat().st_size > 0
    except OSError:
        return False


def repair_teardown_lp_close(
    db_path: str,
    *,
    deployment_id: str | None = None,
    dry_run: bool = False,
    prices_source: str | None = None,
) -> LpCloseRepairResult:
    """Repair LP_CLOSE rows broken by the pre-VIB-4839 silent-cache bug.

    Args:
        db_path: Path to the SQLite state DB.
        deployment_id: When given, restrict the repair to this deployment.
        dry_run: When True, compute the diff but write NOTHING (no backup).
        prices_source: Optional JSON price override for the degraded case
            (missing/empty ``ledger_entry_id`` or pre-VIB-3480 empty
            ``price_inputs_json``).

    Returns:
        :class:`LpCloseRepairResult` describing detected / repaired / skipped
        rows. Idempotent: a second pass over a repaired DB detects 0 rows.
    """
    path = Path(db_path)
    if not path.is_file():
        raise FileNotFoundError(f"State DB not found: {db_path}")

    price_override = _load_price_override(prices_source)

    if not dry_run and _looks_actively_written(path):
        logger.warning(
            "repair_teardown_lp_close: %s looks actively written (non-empty WAL "
            "sidecar). The 1-strategy:1-DB model requires the strategy to be "
            "stopped before repair — proceeding, but stop the strategy first if "
            "this is unexpected.",
            db_path,
        )

    result = LpCloseRepairResult(db_path=db_path, dry_run=dry_run, deployment_id=deployment_id)

    conn = sqlite3.connect(str(path))
    try:
        # Back up BEFORE the first write. Strategy DBs run in WAL mode, so a
        # plain file copy of the main ``.db`` can miss committed frames still
        # in the ``-wal`` file and yield an inconsistent backup. SQLite's
        # native online-backup API copies a consistent snapshot of both,
        # reusing the connection we already hold open.
        if not dry_run:
            ts = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
            backup = path.with_name(f"{path.name}.bak-{ts}")
            backup_conn = sqlite3.connect(str(backup))
            try:
                conn.backup(backup_conn)
            finally:
                backup_conn.close()
            result.backup_path = str(backup)

        sql = _DETECT_SQL
        params: tuple = ()
        if deployment_id:
            sql = sql + " AND deployment_id = ?"
            params = (deployment_id,)
        cols = [
            "id",
            "deployment_id",
            "position_id",
            "position_type",
            "event_type",
            "timestamp",
            "chain",
            "amount0",
            "amount1",
            "ledger_entry_id",
            "attribution_json",
        ]
        broken_rows = [dict(zip(cols, r, strict=True)) for r in conn.execute(sql, params).fetchall()]

        # Single transaction for all UPDATEs.
        for broken in broken_rows:
            repaired = _repair_one_row(conn, broken, price_override)
            if not dry_run:
                _apply_row(conn, broken, repaired)
            result.rows.append(repaired)

        if not dry_run:
            conn.commit()
    finally:
        conn.close()

    logger.info(
        "repair_teardown_lp_close: db=%s dry_run=%s detected=%d repaired=%d skipped=%d written=%d backup=%s",
        db_path,
        dry_run,
        result.detected,
        result.repaired,
        result.skipped,
        result.written,
        result.backup_path,
    )
    return result


def _value_decimal(value_usd: str) -> Decimal | None:
    """Parse a value_usd string to Decimal (None on empty/malformed)."""
    if not value_usd:
        return None
    try:
        return Decimal(value_usd)
    except (ValueError, ArithmeticError):
        return None
