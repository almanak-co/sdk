"""The Accountant Test (AttemptNo17 §1) — runnable, scriptable, CI-able.

This is the "answer the senior DeFi quant's questions" contract. A
strategy passes the Accountant Test when every applicable cell can be
answered using only persisted DB state — no re-reading the chain, no
recomputing from logs, no manual derivation.

The test is structured as 33 cells (15 generic + 6 LP + 6 lending + 6
perp). Each cell is a `(question_id, predicate, decomposition_emitter)`
that the runner evaluates against a SQLite DB dump and produces a typed
``CellResult``.

The output is a markdown report per AttemptNo17 §6.A — diff-able across
runs so a reviewer can compare iterations of a strategy or compare the
same strategy across PRs.

## Usage

>>> from almanak.framework.accounting.accountant_test import AccountantTest, run_against_sqlite
>>> result = run_against_sqlite("strategies/accounting/lp/almanak_state.db", primitive="lp")
>>> print(result.format_markdown())
"""

from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Literal

from almanak.framework.accounting.inventory_revaluation import (
    compute_inventory_revaluation,
)
from almanak.framework.accounting.payload_schemas import (
    FORMULA_VERSION,
    MATCHING_POLICY_VERSION,
    SCHEMA_VERSION,
    is_v1_event_type,
    validate_payload,
)
from almanak.framework.accounting.scorecard_profiles import (
    G6Bases,
    ScorecardCtx,
    ScorecardProfile,
)
from almanak.framework.primitives.taxonomy import (
    TAXONOMY,
    UnknownIntentTypeError,
    materializer_primitive_for,
    primitive_for,
    record_for,
)
from almanak.framework.primitives.taxonomy import (
    Primitive as _TaxonomyPrimitive,
)
from almanak.framework.primitives.types import EventKind

# VIB-4201 (T15): close-event allow-list for cell #22.
# Materialized once at module import from the canonical taxonomy.
# A unit test (`test_cell22_sql_close_list_equals_taxonomy`) asserts
# this tuple stays in lock-step with the SQL CTE in cell #22's predicate
# so a future taxonomy addition is loud, not silently under-counting.
CLOSE_EVENT_TYPES: tuple[str, ...] = tuple(
    sorted(intent for intent, rec in TAXONOMY.items() if rec.event_kind == EventKind.CLOSE)
)

# The scorecard-profile string contract: the stable keys used by the ratchet,
# matrix YAML, CLI, fixture directories, and the accounting unit tests. This is
# NOT the canonical ``Primitive`` enum — ``"looping"`` is a leverage-loop
# *lending* scorecard with no enum twin. Each profile carries its canonical
# ``Primitive`` via ``SCORECARD_PROFILES`` (assembled below). ``Primitive`` is
# kept as a back-compat alias: it is exported in ``__all__`` and referenced by
# annotations throughout this module.
ProfileName = Literal["lp", "looping", "perp", "pendle_pt", "pendle_lp"]
Primitive = ProfileName
CellStatus = Literal["PASS", "FAIL", "XFAIL", "SKIP"]


# VIB-4162 (T2): canonical lifecycle expectations per primitive now live on each
# ``ScorecardProfile.required_lifecycle`` (see ``SCORECARD_PROFILES`` below). The
# lifecycle harness reads them through the registry; a unit test asserts each
# tuple equals the taxonomy lifecycle constant for the profile's canonical
# primitive, so the explicit tuple cannot silently drift from the taxonomy.


class FixtureLifecycleError(AssertionError):
    """Raised when an Accountant Test fixture is missing required lifecycle steps.

    VIB-4162 (T2): a synthetic fixture (or a real strategy DB used as one)
    must exercise the canonical lifecycle for its primitive (LP: OPEN +
    CLOSE; Looping: SUPPLY + BORROW + REPAY + WITHDRAW; Perp: OPEN + CLOSE)
    so the cell predicates can be evaluated against the same shape they
    would see on a real round-trip. A fixture that lands LP_OPEN but skips
    LP_CLOSE produces nominally-passing G1/G7 results that mask a missing
    half of the test surface — this assertion fails loudly instead.
    """


def _assert_fixture_lifecycle(
    conn: sqlite3.Connection,
    primitive: Primitive,
    *,
    deployment_id: str | None = None,
) -> None:
    """Read transaction_ledger.intent_type for success=1 rows and assert
    every canonical lifecycle step is present. Extra steps are allowed.

    VIB-4540 (audit PR #2343): when ``deployment_id`` is supplied, the
    lifecycle query is scoped to that deployment — otherwise a fixture
    DB containing multiple deployments could falsely pass a target
    deployment that is missing a step because another unrelated
    deployment supplied it (or falsely fail based on rows from a
    different deployment).

    Raises :class:`FixtureLifecycleError` with a structured diagnostic that
    names the missing step(s) AND the steps that were observed.
    """
    profile = _profile_for(primitive)
    expected = set(profile.required_lifecycle)
    if not expected:
        # An atomic primitive (e.g. pendle_pt: SWAP has no multi-step
        # lifecycle constant) declares an empty required_lifecycle — its
        # round-trip is enforced by its cell pack, not this guard. Unknown
        # profiles never reach here: _profile_for raises above.
        return
    if deployment_id is None:
        cur = conn.execute("SELECT DISTINCT intent_type FROM transaction_ledger WHERE success=1")
    else:
        cur = conn.execute(
            "SELECT DISTINCT intent_type FROM transaction_ledger WHERE success=1 AND deployment_id = ?",
            (deployment_id,),
        )
    actual = {row[0] for row in cur.fetchall() if row[0]}
    missing = expected - actual
    if missing:
        raise FixtureLifecycleError(
            f"primitive={primitive} fixture missing lifecycle steps: {sorted(missing)}; got: {sorted(actual)}"
        )


@dataclass
class CellResult:
    """One row in the audit report."""

    cell_id: str
    description: str
    status: CellStatus
    diagnostic: str = ""
    decomposition: dict[str, Any] = field(default_factory=dict)
    primitive: str = ""

    def is_pass(self) -> bool:
        return self.status == "PASS"


@dataclass
class AccountantReport:
    """The full audit report for one DB dump + one primitive."""

    primitive: Primitive
    network: str
    deployment_id: str
    schema_version: int = SCHEMA_VERSION
    formula_version: int = FORMULA_VERSION
    matching_policy_version: int = MATCHING_POLICY_VERSION
    timestamp: datetime = field(default_factory=lambda: datetime.now(UTC))
    cells: list[CellResult] = field(default_factory=list)
    on_chain_footprint: list[dict[str, Any]] = field(default_factory=list)
    g6_decomposition: dict[str, Any] = field(default_factory=dict)
    db_dump_path: str | None = None
    # VIB-3868: every accounting_events row whose payload failed Pydantic
    # validation against payload_schemas.py. Cells that read this row's
    # payload FAIL with the captured error — the permissive `_json` helper
    # used to silently substitute `{}` for malformed payloads, hiding the
    # contract drift.
    payload_validation_errors: list[dict[str, Any]] = field(default_factory=list)
    # VIB-3868: list of cells that flipped to FAIL specifically because of
    # an upstream payload validation error. Lets reviewers triage cell-status
    # changes between runs without re-deriving the propagation by hand.
    cells_blocked_by_payload_errors: list[str] = field(default_factory=list)

    @property
    def total_cells(self) -> int:
        return len(self.cells)

    @property
    def passed(self) -> int:
        return sum(1 for c in self.cells if c.status == "PASS")

    @property
    def failed(self) -> int:
        return sum(1 for c in self.cells if c.status == "FAIL")

    @property
    def xfailed(self) -> int:
        return sum(1 for c in self.cells if c.status == "XFAIL")

    def to_json(self) -> dict[str, Any]:
        """JSON-serializable dict for the matrix runner and downstream consumers.

        The flat ``cells: {id -> status}`` shape is a superset-compatible
        extension of ``tests/fixtures/accounting/<primitive>/expected_cells.json``
        so existing baselines remain comparable. ``cell_details`` carries the
        richer per-cell payload (description, diagnostic) for triage.
        """
        return {
            "primitive": self.primitive,
            "network": self.network,
            "deployment_id": self.deployment_id,
            "schema_version": self.schema_version,
            "formula_version": self.formula_version,
            "matching_policy_version": self.matching_policy_version,
            "timestamp": self.timestamp.isoformat(),
            "cells": {c.cell_id: c.status for c in self.cells},
            "cell_details": [
                {
                    "id": c.cell_id,
                    "description": c.description,
                    "status": c.status,
                    "diagnostic": c.diagnostic,
                    "primitive": c.primitive,
                }
                for c in self.cells
            ],
            "scores": {
                "passed": self.passed,
                "failed": self.failed,
                "xfailed": self.xfailed,
                "total": self.total_cells,
            },
            "payload_validation_errors": list(self.payload_validation_errors),
            "cells_blocked_by_payload_errors": list(self.cells_blocked_by_payload_errors),
            "g6_decomposition": dict(self.g6_decomposition),
            "on_chain_footprint": list(self.on_chain_footprint),
            "db_dump_path": self.db_dump_path,
        }

    def format_markdown(self) -> str:
        lines = []
        lines.append(f"# Accountant Test — {self.primitive} — {self.timestamp.isoformat()}")
        lines.append("")
        lines.append("## Run metadata")
        lines.append(f"- Primitive: **{self.primitive}**")
        lines.append(f"- Network: {self.network}")
        lines.append(f"- Strategy: `{self.deployment_id}`")
        lines.append(
            f"- schema_version / formula_version / matching_policy_version: "
            f"{self.schema_version} / {self.formula_version} / {self.matching_policy_version}"
        )
        if self.db_dump_path:
            lines.append(f"- DB: `{self.db_dump_path}`")
        lines.append("")
        # Score
        generic = [c for c in self.cells if c.cell_id.startswith("G")]
        prim = [c for c in self.cells if not c.cell_id.startswith("G")]

        def _score(rs: list[CellResult]) -> str:
            p = sum(1 for r in rs if r.status == "PASS")
            f = sum(1 for r in rs if r.status == "FAIL")
            x = sum(1 for r in rs if r.status == "XFAIL")
            s = sum(1 for r in rs if r.status == "SKIP")
            return f"{p} PASS, {f} FAIL, {x} XFAIL, {s} SKIP (of {len(rs)})"

        lines.append("## Score")
        lines.append(f"- Generic 15: {_score(generic)}")
        lines.append(f"- Primitive {len(prim)}: {_score(prim)}")
        lines.append(f"- Total: {self.passed}/{self.total_cells} PASS, {self.failed} FAIL, {self.xfailed} XFAIL")
        # VIB-4201 (T15): cell L5_22 is informational only — not in the
        # ≥16/21 gating sum. The gating line below partitions the original
        # 21 cells from cell #22 explicitly so a FAIL on #22 stays visible
        # but does not degrade gating arithmetic. If L5_22 is absent for
        # any reason (legacy back-compat caller, primitive that does not
        # produce a 22nd cell), the gating line still renders against the
        # 21 cells with status="absent".
        gated_cells = [c for c in self.cells if c.cell_id != "L5_22"]
        cell22 = next((c for c in self.cells if c.cell_id == "L5_22"), None)
        gated_pass = sum(1 for c in gated_cells if c.status == "PASS")
        cell22_status = cell22.status if cell22 is not None else "absent"
        lines.append(
            f"- Gating: {gated_pass}/{len(gated_cells)} PASS (≥16/21 required); "
            f"cell L5_22 informational only this cycle (status: {cell22_status})"
        )
        lines.append("")
        lines.append("## Cells")
        # MD058: blank line between heading and table.
        lines.append("")
        lines.append("| ID | Description | Status | Diagnostic |")
        lines.append("|---|---|---|---|")
        for cell in self.cells:
            diag = cell.diagnostic.replace("|", "\\|").replace("\n", " ")
            lines.append(f"| {cell.cell_id} | {cell.description} | **{cell.status}** | {diag} |")
        lines.append("")
        if self.g6_decomposition:
            lines.append("## G6 decomposition (always emitted)")
            for k, v in self.g6_decomposition.items():
                lines.append(f"- {k}: {v}")
            lines.append("")
        if self.payload_validation_errors:
            # VIB-3868: surface schema mismatches at top level so reviewers
            # can triage them without combing through cell diagnostics.
            lines.append("## Payload validation errors")
            lines.append("")
            for rec in self.payload_validation_errors:
                lines.append(
                    f"- row_id=`{rec.get('row_id')}` event_type=`{rec.get('event_type')}` error={rec.get('error')}"
                )
            if self.cells_blocked_by_payload_errors:
                lines.append("")
                lines.append(f"_Cells blocked by validation errors: {', '.join(self.cells_blocked_by_payload_errors)}_")
            lines.append("")
        if self.on_chain_footprint:
            lines.append("## On-chain footprint")
            for tx in self.on_chain_footprint:
                lines.append(
                    f"- tx_hash: `{tx.get('tx_hash')}` | intent: {tx.get('intent_type')} "
                    f"| chain: {tx.get('chain')} | gas_used: {tx.get('gas_used')} "
                    f"| success: {tx.get('success')}"
                )
            lines.append("")
        return "\n".join(lines)


# ─── DB read helpers ─────────────────────────────────────────────────────


def _connect(db_path: str | Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    return conn


_ALLOWED_READ_TABLES: frozenset[str] = frozenset(
    {
        "transaction_ledger",
        "position_events",
        "accounting_events",
        "portfolio_snapshots",
        "portfolio_metrics",
        "position_state_snapshots",
        # VIB-4201 (T15): cell #22 reads position_registry.
        # The table may be absent on pre-T11 fixtures; ``_table_rows`` returns
        # ``[]`` on a missing table (sqlite3.OperationalError caught), which
        # routes the cell to its "registry-absent" branch.
        "position_registry",
    }
)


def _table_rows(
    conn: sqlite3.Connection,
    table: str,
    *,
    deployment_id: str | None = None,
) -> list[dict[str, Any]]:
    """Read rows from one of the SDK's read-only accounting tables.

    The table name is interpolated into the SQL string because sqlite3 does
    not parameterize identifiers. The whitelist below makes that safe — only
    the small set of SDK-owned accounting tables this module ever needs to
    read are permitted, and any other input raises ``ValueError`` rather
    than silently issuing a query against an attacker-controlled identifier.

    VIB-4540: when ``deployment_id`` is supplied, rows are scoped via
    ``WHERE deployment_id = ?``. Without that filter, a folder-scoped DB
    accumulated across multiple deployments would leak older rows into the
    current run's cell scores (caught when L3 reported ``min(HF) = 0.997``
    from a prior deployment instead of from this run's ``HF = 1.71``).
    Back-compat: passing ``None`` preserves the original unfiltered shape
    for any callers that pre-date the scoping flag.
    """
    if table not in _ALLOWED_READ_TABLES:
        raise ValueError(
            f"_table_rows: refusing to read unknown table {table!r}; allowed tables: {sorted(_ALLOWED_READ_TABLES)}"
        )
    cur = conn.cursor()
    try:
        if deployment_id is None:
            cur.execute(f"SELECT * FROM {table}")  # noqa: S608 — whitelisted identifier
        else:
            cur.execute(
                f"SELECT * FROM {table} WHERE deployment_id = ?",  # noqa: S608 — whitelisted identifier
                (deployment_id,),
            )
    except sqlite3.OperationalError:
        # Table missing or (when ``deployment_id`` is supplied) the column
        # is absent on an older schema. Either case collapses to "no rows"
        # so callers get the same back-compat shape they had before VIB-4540.
        return []
    return [dict(r) for r in cur.fetchall()]


# VIB-4540: the small subset of tables whose ``deployment_id`` column is
# the canonical identity key — querying any one of them is enough to
# enumerate the deployments present in a folder DB. ``SELECT DISTINCT
# deployment_id`` on these tables performs a full table scan on an
# unindexed column, so the cost is O(rows), not O(deployments). For the
# CLI's interactive case that's fine (one call per invocation); a future
# hot-path caller should add an index or maintain a separate
# ``deployments`` table (gemini review on PR #2343).
_DEPLOYMENT_SCAN_TABLES: tuple[str, ...] = (
    "transaction_ledger",
    "accounting_events",
    "portfolio_snapshots",
)


class MultipleDeploymentsError(RuntimeError):
    """Raised when ``run_against_sqlite`` sees >1 deployment in the DB and
    the caller did not supply ``deployment_id``.

    Auto-picking would silently contaminate the score with rows from an
    unrelated deployment (the bug VIB-4540 fixes); raising forces the
    caller to choose explicitly. The candidate deployment ids are
    exposed on ``deployment_ids`` so a CLI / UI can render its own
    selection prompt without re-parsing the error string.
    """

    def __init__(self, deployment_ids: list[str]) -> None:
        self.deployment_ids = list(deployment_ids)
        super().__init__(
            "Multiple deployments present in this DB; the Accountant Test must "
            "score against one. Pass deployment_id explicitly. Candidates: "
            f"{sorted(self.deployment_ids)}"
        )


def _deployment_exists(conn: sqlite3.Connection, deployment_id: str) -> bool:
    """Return True iff ``deployment_id`` appears in at least one of the
    canonical accounting tables. Used to validate an explicit ``--deployment-id``
    flag before scoping reads — without this, a typo would fall through to
    empty filtered reads and produce a misleading FAIL/XFAIL report instead
    of a config error (audit PR #2343 finding)."""
    for table in _DEPLOYMENT_SCAN_TABLES:
        try:
            cur = conn.execute(
                f"SELECT 1 FROM {table} WHERE deployment_id = ? LIMIT 1",  # noqa: S608 — whitelisted identifier
                (deployment_id,),
            )
            if cur.fetchone() is not None:
                return True
        except sqlite3.OperationalError:
            continue
    return False


def _resolve_singleton_deployment_id(conn: sqlite3.Connection) -> str | None:
    """Discover the deployment to score against when the caller didn't pick one.

    Returns:
      * ``None`` when no deployment is found (empty DB, or a fixture predating
        the canonical ``deployment_id`` column). Callers proceed unfiltered —
        same back-compat shape as before VIB-4540.
      * The singleton id when exactly one deployment is present (the
        matrix-runner case — every fixture DB carries one deployment).

    Raises:
      :class:`MultipleDeploymentsError` when the DB has >1 deployment. Silent
      auto-pick of "first" or "latest" would re-introduce the contamination
      this helper exists to prevent; raising forces the caller to choose.
    """
    deployments: set[str] = set()
    for table in _DEPLOYMENT_SCAN_TABLES:
        try:
            cur = conn.execute(
                f"SELECT DISTINCT deployment_id FROM {table} "  # noqa: S608 — whitelisted identifier
                "WHERE deployment_id IS NOT NULL AND deployment_id != ''"
            )
            deployments.update(row[0] for row in cur.fetchall() if row[0])
        except sqlite3.OperationalError:
            # Table or column missing — partial schema is fine, keep scanning.
            continue
    if not deployments:
        return None
    if len(deployments) == 1:
        return next(iter(deployments))
    raise MultipleDeploymentsError(sorted(deployments))


def _dec(v: Any) -> Decimal | None:
    if v is None or v == "":
        return None
    try:
        return Decimal(str(v))
    except (InvalidOperation, TypeError):
        return None


def _snapshot_equity(s: dict[str, Any]) -> Decimal | None:
    """Strategy equity at a snapshot = ``total_value_usd + available_cash_usd``.

    VIB-3614 split deployed (positions) from cash (uninvested wallet) into
    separate columns. The Senior DeFi Quant's equity curve / PnL view is the
    SUM. A post-teardown snapshot with ``total_value_usd=0`` is *not* a
    missing measurement — every position closed cleanly and equity collapsed
    into ``available_cash_usd``. Treating that as null double-counts a
    successful unwind as an accounting failure (G8 false positive seen on
    looping mainnet, 2026-05-01).

    Returns ``None`` ONLY when both columns are unmeasured. A pure-cash
    snapshot or a pure-deployed snapshot is a valid equity point.
    """
    deployed = _dec(s.get("total_value_usd"))
    cash = _dec(s.get("available_cash_usd"))
    if deployed is None and cash is None:
        return None
    return (deployed or Decimal("0")) + (cash or Decimal("0"))


def _json(s: Any) -> dict[str, Any]:
    if s is None or s == "":
        return {}
    if isinstance(s, dict):
        return s
    try:
        d = json.loads(s)
        return d if isinstance(d, dict) else {}
    except (json.JSONDecodeError, TypeError):
        return {}


# ─── VIB-3868: typed payload reads ───────────────────────────────────────


def _project_payload_for_v1_validation(payload: dict[str, Any], row: dict[str, Any]) -> dict[str, Any]:
    """Map writer output → v1 spec shape before Pydantic validation.

    The typed event writers (``LPAccountingEvent``, ``LendingAccountingEvent``,
    ``PerpAccountingEvent``) emit names that pre-date the AttemptNo17 §1.2 spec
    (``amount_token``/``supply_apr_bps``/``borrow_apr_bps`` instead of
    ``amount``/``supply_apr_pct``/``borrow_apr_pct``; LP omits ``protocol``
    because the position_key carries it). Without this projection step,
    every real-run row fails validation against the v1 schemas — Codex P1
    audit finding (2026-05-02).

    The projection is read-only on the writer's persisted shape (we never
    mutate the row); we just return a dict suitable for the schema. Aliases
    populate spec names only when the spec name is missing — never overwrite
    a writer that already emits the canonical name.
    """
    et = (payload.get("event_type") or "").upper()
    out = dict(payload)

    # Inject ``protocol`` from the row's protocol column when the payload
    # itself doesn't carry it (LP/Perp writers don't emit it; the row
    # column is the canonical source).
    if not out.get("protocol"):
        row_protocol = (row.get("protocol") or "").strip()
        if row_protocol:
            out["protocol"] = row_protocol

    # Lending: amount_token → amount (SUPPLY/REPAY/WITHDRAW) or borrowed_amount (BORROW)
    #
    # VIB-4539: forward ``amount_token`` even when it is None **for WITHDRAW
    # only**. ``WithdrawEventPayload.amount`` is widened to ``Decimal | None``
    # per the AGENTS.md Empty ≠ Zero rule because the Morpho receipt parser
    # cannot always resolve assets amount on shares-mode withdraws / unresolved
    # decimals. SUPPLY / REPAY / DELEVERAGE / BORROW schemas remain strictly
    # ``Decimal`` — keep the prior "only forward when non-None" behaviour for
    # them so an unmeasured row fails Pydantic loudly with "field required"
    # instead of silently aliasing None onto a still-required field (audit
    # PR #2343 Claude finding 3).
    if et == "WITHDRAW" and "amount" not in out and "amount_token" in out:
        out["amount"] = out["amount_token"]
    if et in {"SUPPLY", "REPAY", "DELEVERAGE"} and "amount" not in out:
        if out.get("amount_token") is not None:
            out["amount"] = out["amount_token"]
    if et == "BORROW" and "borrowed_amount" not in out:
        if out.get("amount_token") is not None:
            out["borrowed_amount"] = out["amount_token"]

    # APR bps → pct projection (10000 bps = 100%, so bps / 100 = pct).
    # Gemini (2026-05-02): narrow the except to the only error classes
    # ``Decimal(str(bps))`` and division can raise; let unexpected ones
    # propagate so refactor regressions surface loudly.
    from decimal import Decimal as _Dec
    from decimal import InvalidOperation as _InvalidOp

    if et in {"SUPPLY", "WITHDRAW"} and "supply_apr_pct" not in out:
        bps = out.get("supply_apr_bps")
        if bps is not None:
            try:
                out["supply_apr_pct"] = _Dec(str(bps)) / _Dec("100")
            except (_InvalidOp, TypeError, ValueError):
                pass
    if et in {"BORROW", "REPAY", "DELEVERAGE"} and "borrow_apr_pct" not in out:
        bps = out.get("borrow_apr_bps")
        if bps is not None:
            try:
                out["borrow_apr_pct"] = _Dec(str(bps)) / _Dec("100")
            except (_InvalidOp, TypeError, ValueError):
                pass

    return out


def _typed_acct_payloads(
    acct_events: list[dict[str, Any]],
) -> tuple[dict[Any, dict[str, Any]], dict[Any, str], list[dict[str, Any]]]:
    """Decode + Pydantic-validate every ``accounting_events.payload_json``.

    Returns three values (VIB-3868):

    - ``payloads_by_id`` — maps each row's ``id`` to a *validated* dict.
      For event_types in the v1 surface (``payload_schemas._PAYLOAD_MODELS``),
      the dict is the result of ``model.model_dump()``. For non-v1 types
      (PENDLE, POLYMARKET, …) the raw decoded dict pass-through is preserved.
      On validation failure the entry is ``{}`` so downstream cells see
      "no data" rather than malformed data — and the cell that *cares* about
      this row's event_type can FAIL via the ``errors_by_id`` lookup.
    - ``errors_by_id`` — maps row ``id`` → human-readable error message for
      every row whose payload failed Pydantic validation.
    - ``error_records`` — public-facing list with `{row_id, event_type,
      error}` entries; surfaced on the report so reviewers can diff
      validation drift across runs.

    Why "validated then dumped" instead of returning the model instance?
    Cells today read payloads as plain dicts (``p.get("foo")``); preserving
    that read shape keeps the diff small and avoids accidentally typing
    every cell against the model class. The validation step still happens —
    schema-incompatible payloads land in ``errors_by_id`` and never reach
    the cell.

    Codex P1 (2026-05-02): payloads are projected from the writer's persisted
    shape onto the v1 spec shape via ``_project_payload_for_v1_validation``
    before validation. The schemas use ``extra="ignore"`` so writer-only
    fields (``lp_token_amount``, ``fees0_collected``, etc.) are silently
    dropped — the validation still fires on missing/wrong-typed required
    fields.
    """
    payloads_by_id: dict[Any, dict[str, Any]] = {}
    errors_by_id: dict[Any, str] = {}
    error_records: list[dict[str, Any]] = []
    for r in acct_events:
        row_id = r.get("id")
        et = r.get("event_type") or ""
        decoded = _json(r.get("payload_json"))
        if not is_v1_event_type(et):
            # Out of v1 scope — preserve the decoded dict but do NOT validate.
            # AttemptNo17 §8.5 explicitly tracks PENDLE / POLYMARKET / etc.
            # under v2 placeholder tickets; surfacing v1 schema mismatches on
            # them would be noise.
            payloads_by_id[row_id] = decoded
            continue
        # Project writer output → spec shape before validation (Codex P1).
        projected = _project_payload_for_v1_validation(decoded, r)
        try:
            validated = validate_payload(et, projected)
            payloads_by_id[row_id] = validated.model_dump() if validated is not None else projected
        except ValueError as e:
            errors_by_id[row_id] = str(e)
            error_records.append({"row_id": row_id, "event_type": et, "error": str(e)})
            payloads_by_id[row_id] = {}
    return payloads_by_id, errors_by_id, error_records


def _payload_block_cell(
    cell_id: str,
    description: str,
    rows: list[dict[str, Any]],
    errors_by_id: dict[Any, str],
) -> CellResult | None:
    """Return a FAIL ``CellResult`` if any of ``rows`` had a payload validation
    error; otherwise return ``None`` so the caller can run its real predicate.

    A cell's data is unusable when the upstream payload didn't match the
    frozen Pydantic schema. Today's cells used to silently treat that as
    "field absent" and drop into XFAIL/SKIP — VIB-3868's correctness
    contract: surface the error here so the diagnostic carries the schema
    mismatch reason and the cell flips to FAIL, not XFAIL.
    """
    if not errors_by_id:
        return None
    blocking = [(r.get("id"), errors_by_id[r.get("id")]) for r in rows if r.get("id") in errors_by_id]
    if not blocking:
        return None
    sample = blocking[:3]
    return CellResult(
        cell_id,
        description,
        "FAIL",
        f"{len(blocking)} payload(s) failed Pydantic validation; cell data unusable. e.g. {sample!r}",
    )


# ─── Cell predicates ─────────────────────────────────────────────────────


def _cell_g1_money_trail(
    rows: list[dict[str, Any]],
    acct_events: list[dict[str, Any]],
    acct_payloads: dict[Any, dict[str, Any]],
) -> CellResult:
    """G1 — Money trail (every credit/debit → tx_hash + USD@block).

    VIB-3868 (B): G1 fails strictly when any successful SWAP ledger row is
    missing token amounts (``amount_in``/``amount_out``) **or** when the
    paired ``accounting_events`` row lacks both USD valuations
    (``amount_in_usd`` / ``amount_out_usd``). The previous implementation
    counted missing token amounts but never enforced the USD pillar, so a
    swap that landed on-chain without USD values silently passed — exactly
    the false positive the cell name claims to prevent.

    The pairing rule is ledger.id == accounting_events.ledger_entry_id (the
    foreign key wired by ``AccountingWriter``). When a SWAP ledger row has
    no matching accounting_events row at all, that's also a money-trail
    failure — the typed payload is the *only* place USD valuations live.
    """
    if not rows:
        return CellResult(
            "G1",
            "Money trail (every credit/debit → tx_hash + USD@block)",
            "FAIL",
            "transaction_ledger empty",
        )
    # Successful intents only — failed intents are evaluated under G11
    # (gas-only money trail). Mixing the two would FAIL G1 for a reverted
    # SWAP that legitimately has no amount_out, masking the actual gap.
    successful = [r for r in rows if r.get("success")]
    missing_hash = [r for r in successful if not r.get("tx_hash")]
    missing_token_amounts = [
        r for r in successful if r.get("intent_type") == "SWAP" and not (r.get("amount_in") and r.get("amount_out"))
    ]
    # Cross-table USD pillar: every successful SWAP ledger row must have a
    # matching accounting_events row whose validated SwapEventPayload
    # populates BOTH amount_in_usd and amount_out_usd. ``acct_payloads`` is
    # the validated map from ``_typed_acct_payloads`` — a row whose payload
    # failed Pydantic validation lands as ``{}`` here, which counts as
    # missing USD (and the matching cell-level error is also surfaced via
    # the report's ``payload_validation_errors`` list).
    swap_acct_by_ledger_id: dict[Any, dict[str, Any]] = {}
    for ae in acct_events:
        if ae.get("event_type") != "SWAP":
            continue
        leg = ae.get("ledger_entry_id")
        if leg is None:
            continue
        swap_acct_by_ledger_id[leg] = acct_payloads.get(ae.get("id"), {})

    missing_swap_usd: list[tuple[Any, str]] = []
    for r in successful:
        if r.get("intent_type") != "SWAP":
            continue
        ledger_id = r.get("id")
        payload = swap_acct_by_ledger_id.get(ledger_id)
        if payload is None:
            missing_swap_usd.append((ledger_id, "no SwapEventPayload row"))
            continue
        in_usd = payload.get("amount_in_usd")
        out_usd = payload.get("amount_out_usd")
        if in_usd in (None, "") or out_usd in (None, ""):
            missing_swap_usd.append((ledger_id, f"amount_in_usd={in_usd!r} amount_out_usd={out_usd!r}"))

    if missing_hash:
        return CellResult(
            "G1",
            "Money trail",
            "FAIL",
            f"{len(missing_hash)} successful ledger rows missing tx_hash",
        )
    if missing_token_amounts:
        sample = [r.get("id") for r in missing_token_amounts[:3]]
        return CellResult(
            "G1",
            "Money trail",
            "FAIL",
            f"{len(missing_token_amounts)} SWAP rows missing amount_in/amount_out (e.g. {sample!r})",
        )
    if missing_swap_usd:
        sample_usd = missing_swap_usd[:3]
        return CellResult(
            "G1",
            "Money trail",
            "FAIL",
            f"{len(missing_swap_usd)} SWAP rows missing USD valuation in SwapEventPayload (e.g. {sample_usd!r})",
        )
    swap_count = sum(1 for r in successful if r.get("intent_type") == "SWAP")
    return CellResult(
        "G1",
        "Money trail",
        "PASS",
        f"{len(rows)} ledger rows ({len(successful)} successful, {swap_count} SWAP); "
        "all tx_hashes present; SWAP rows carry token amounts AND USD valuations",
    )


def _cell_g2_cost_ledger(rows: list[dict[str, Any]]) -> CellResult:
    if not rows:
        return CellResult("G2", "Cost ledger (gas_usd separable)", "FAIL", "no ledger rows")
    missing = [r for r in rows if not r.get("gas_usd") and (r.get("gas_used") or 0) > 0]
    if missing:
        return CellResult(
            "G2",
            "Cost ledger",
            "FAIL",
            f"gas_usd empty on {len(missing)}/{len(rows)} ledger rows (intent_types: "
            f"{','.join(sorted({r.get('intent_type', '?') for r in missing}))})",
        )
    return CellResult(
        "G2",
        "Cost ledger",
        "PASS",
        f"gas_usd populated on {len(rows)}/{len(rows)} ledger rows",
    )


def _cell_g3_yield_ledger(pos_events: list[dict[str, Any]], acct_events: list[dict[str, Any]]) -> CellResult:
    if not pos_events and not acct_events:
        return CellResult("G3", "Yield ledger", "XFAIL", "no position_events nor accounting_events")
    # Diagnostic list — heterogeneous tuples (3-4 fields) are intentional;
    # downstream we only count `len(yields)`. Annotate as tuple-of-Any so
    # mypy doesn't pin the element shape to whichever append it sees first.
    yields: list[tuple[Any, ...]] = []
    for r in pos_events:
        if r.get("fees_token0") or r.get("fees_token1"):
            yields.append(("fees", r.get("event_type"), r.get("fees_token0"), r.get("fees_token1")))
    for r in acct_events:
        p = _json(r.get("payload_json"))
        # VIB-4905 (F1): for SWAP, prefer ``realized_pnl_usd_matched``
        # (matched-portion PnL, populated on partial matches under v2 contract)
        # over legacy ``realized_pnl_usd`` (null on partial under v1).  Other
        # event types still read ``realized_pnl_usd`` per their own contracts.
        # Keeps this diagnostic in lockstep with dashboard +
        # ``_cell_g6_reconciliation``'s SWAP-bucket precedence.
        rpnl_for_yield = (
            p.get("realized_pnl_usd_matched")
            if r.get("event_type") == "SWAP" and p.get("realized_pnl_usd_matched") is not None
            else p.get("realized_pnl_usd")
        )
        if rpnl_for_yield:
            yields.append(("realized_pnl", r.get("event_type"), rpnl_for_yield))
        # ``augment_accounting_payload`` projects lending events onto the
        # AttemptNo17 spec field names (``interest_paid_usd`` for REPAY,
        # ``interest_accrued_usd`` for WITHDRAW). Counting only the legacy
        # ``interest_paid`` here would silently mark spec-shaped lending
        # rows as "no interest" and false-fail G3 once Track A's projection
        # fully replaces the legacy keys.
        if p.get("interest_paid") or p.get("interest_paid_usd") or p.get("interest_accrued_usd"):
            yields.append(
                (
                    "interest",
                    r.get("event_type"),
                    p.get("interest_paid") or p.get("interest_paid_usd") or p.get("interest_accrued_usd"),
                )
            )
        if p.get("fees0_collected") or p.get("fees1_collected"):
            yields.append(("lp_fees", r.get("event_type"), p.get("fees0_collected"), p.get("fees1_collected")))
    if not yields:
        return CellResult(
            "G3",
            "Yield ledger",
            "FAIL",
            "no realized yield / fees / interest captured on any event",
        )
    return CellResult("G3", "Yield ledger", "PASS", f"{len(yields)} yield-emitting events found")


def _cell_g4_capital_deployed(snapshots: list[dict[str, Any]]) -> CellResult:
    """G4 — Capital deployed right now (positions + cash) reconciles.

    Per the VIB-3614 column split (see ``_snapshot_equity`` docstring above):

    * ``total_value_usd`` — the deployed (positions) side of strategy value.
    * ``available_cash_usd`` — the uninvested cash side.
    * Strategy equity = ``total_value_usd + available_cash_usd``.

    Earlier revisions of this cell tried to derive ``deployed`` as
    ``total - cash``, which inverted the semantics and produced negative
    deployed values for cash-heavy or fully-teardown snapshots. The honest
    predicate is just "both columns are measured and both are non-negative;
    equity sums" — i.e. the snapshotter persisted a coherent snapshot.

    The legacy ``deployed_capital_usd`` column is left in the schema but
    populated as ``"0"`` for every real run today; do NOT read it.
    """
    if not snapshots:
        return CellResult("G4", "Capital deployed right now", "FAIL", "no portfolio_snapshots")
    last = snapshots[-1]
    deployed = _dec(last.get("total_value_usd"))
    cash = _dec(last.get("available_cash_usd"))
    if deployed is None or cash is None:
        return CellResult(
            "G4",
            "Capital deployed right now",
            "FAIL",
            f"snapshot fields null: deployed={deployed} cash={cash}",
        )
    if deployed < 0 or cash < 0:
        return CellResult(
            "G4",
            "Capital deployed right now",
            "FAIL",
            f"negative side: deployed=${deployed} cash=${cash}",
        )
    equity = deployed + cash
    return CellResult(
        "G4",
        "Capital deployed right now",
        "PASS",
        f"deployed=${deployed} cash=${cash} equity=${equity}",
    )


def _cell_g5_initial_vs_current(metrics: list[dict[str, Any]], snapshots: list[dict[str, Any]]) -> CellResult:
    if not metrics:
        return CellResult("G5", "Initial vs current", "FAIL", "no portfolio_metrics row")
    m = metrics[-1]
    initial = _dec(m.get("initial_value_usd"))
    if initial is None:
        return CellResult("G5", "Initial vs current", "FAIL", "initial_value_usd null")
    if not snapshots:
        return CellResult(
            "G5",
            "Initial vs current",
            "FAIL",
            f"initial=${initial} but no snapshots for current",
        )
    current = _snapshot_equity(snapshots[-1])
    if current is None:
        return CellResult("G5", "Initial vs current", "FAIL", f"initial=${initial} but current null")
    delta = current - initial
    return CellResult(
        "G5",
        "Initial vs current",
        "PASS",
        f"initial=${initial} current=${current} delta=${delta}",
    )


def _cell_g6_reconciliation(  # noqa: C901
    snapshots: list[dict[str, Any]],
    ledger: list[dict[str, Any]],
    pos_events: list[dict[str, Any]],
    acct_events: list[dict[str, Any]],
    primitive: Primitive,
    acct_payloads: dict[Any, dict[str, Any]],
    payload_errors: dict[Any, str],
) -> tuple[CellResult, dict[str, Any]]:
    """G6 reconciliation: wallet ≡ component within ε, decomposition ALWAYS emitted.

    IL is NOT a reconciliation term — recovered LP principal already reflects
    post-IL outcome. IL is a decomposition of the LP open→close delta, lives
    in LP4/LP5 attribution only.

    Ambient inventory revaluation IS a reconciliation term (blueprint 27 §11.5).
    The wallet (equity) method moves when an UNTRADED token the strategy merely
    holds changes price between the two endpoint snapshots — idle WETH, the
    native-gas remainder, the unspent half of a single-sided swap, an open swap
    lot's residual mark-to-market. None of that lands in a typed component
    bucket, so without the ``Σ_inventory_reval_usd`` term the component sum is
    structurally short and G6 reports a spurious gap. The term
    (``qty_idle × Δmark`` for ambient tokens + ``remaining × mark − basis`` for
    open lots) is read off the SAME priced[0]/priced[-1] snapshot rows that
    produced ``wallet_pnl`` — same marks, no skew, no new reads — and collapses
    to exactly zero when there is no untraded inventory at either endpoint. An
    unmeasured term (a held token with no persisted mark, or an open lot with no
    basis) is a NULL input (Empty≠Zero) and FAILs the cell with a diagnostic,
    never a silent zero.
    """
    # VIB-3868: any acct_event with a malformed payload would silently
    # contribute zero to the component PnL through ``_json`` returning ``{}``
    # — exactly the false-positive shape Codex flagged. Pre-empt the whole
    # cell with a FAIL when validation drift exists; surfacing the typed
    # error here keeps the diagnostic actionable.
    blocked = _payload_block_cell(
        "G6",
        "Reconciliation (wallet ≡ component)",
        acct_events,
        payload_errors,
    )
    if blocked is not None:
        return blocked, {}
    # Wallet method: equity_final − equity_initial across all priced
    # snapshots. ``_snapshot_equity`` sums total_value_usd (deployed) +
    # available_cash_usd (uninvested wallet) — a post-teardown snapshot
    # with all-cash equity is a valid endpoint, not a measurement gap.
    priced = [s for s in snapshots if _snapshot_equity(s) is not None]
    if len(priced) < 2:
        return (
            CellResult(
                "G6",
                "Reconciliation (wallet ≡ component)",
                "FAIL",
                f"need ≥2 snapshots with measured equity (have {len(priced)} of {len(snapshots)})",
            ),
            {},
        )
    initial = _snapshot_equity(priced[0])
    final = _snapshot_equity(priced[-1])
    if initial is None or final is None:
        return (
            CellResult(
                "G6",
                "Reconciliation",
                "FAIL",
                f"snapshot equity null (initial={initial} final={final})",
            ),
            {},
        )
    wallet_pnl = final - initial

    # Component method: sum the typed columns + payload reads.
    # Each bucket attributes to a distinct economic source so the
    # reconciliation diagnostic can pin which primitive's accounting drifted
    # if wallet_pnl ≠ component_pnl. PERP_CLOSE realized_pnl gets its own
    # bucket (sum_perp) — VIB-3865 fixed it accumulating into sum_lp.
    sum_swap = Decimal(0)
    sum_lp = Decimal(0)
    sum_perp = Decimal(0)
    sum_fees = Decimal(0)
    sum_funding = Decimal(0)
    sum_interest = Decimal(0)
    sum_gas = Decimal(0)
    il_diagnostic = Decimal(0)

    # VIB-3869 (A): per-bucket null counts.
    # The bug the cell hides today: `if rpnl is not None: sum_swap += rpnl`
    # silently treats a null `realized_pnl_usd` on a SWAP payload as zero.
    # On a hosted run where every SWAP payload had `realized_pnl_usd=null`,
    # `Σ_swaps_usd = 0` would reconcile against a wallet PnL that's also
    # zero — a false positive. Counting the nulls separately surfaces this
    # as "the inputs to the reconciliation are NULL, not measured zero".
    null_swap_rpnl = 0
    # VIB-4394: a SWAP whose realized_pnl is None because there was no prior
    # FIFO lot to realize against (an opening / acquiring swap, or the first
    # disposal of pre-existing wallet inventory) is a LEGITIMATE measured state,
    # NOT a measurement gap. It is counted separately and reported for forensics,
    # but — unlike null_swap_rpnl — it does NOT trip has_nulls / fail G6. Only an
    # UNMEASURED-amount SWAP (amount_in_usd is None) increments null_swap_rpnl.
    no_prior_basis_swap = 0
    null_lp_close_rpnl = 0
    null_lp_fees = 0
    null_perp_rpnl = 0
    null_perp_funding = 0
    null_withdraw_interest = 0
    null_repay_interest = 0

    # VIB-3869 (B): notional accumulators for primitive-aware tolerance.
    notional_traded = Decimal(0)  # LP / Spot scaling base
    debt_outstanding = Decimal(0)  # Looping running debt
    max_debt = Decimal(0)  # Looping scaling base
    max_perp_notional = Decimal(0)  # Perp scaling base

    for r in ledger:
        gas = _dec(r.get("gas_usd"))
        if gas is not None:
            sum_gas += gas

    # Time-ordered iteration so debt_outstanding tracks the actual running
    # liability through BORROW → REPAY pairs. ``acct_events`` was already
    # sorted by timestamp in ``run_against_sqlite``.
    for r in acct_events:
        p = acct_payloads.get(r.get("id"), {})
        et = r.get("event_type")
        rpnl = _dec(p.get("realized_pnl_usd"))
        if et == "SWAP":
            # VIB-4905 (F1): SWAP bucket prefers ``realized_pnl_usd_matched``
            # (matched-portion PnL, populated on partial matches too) over
            # legacy ``realized_pnl_usd`` (null on partial under the v1
            # contract).  Keeps the G6 SWAP bucket in lockstep with the
            # dashboard's ``compute_cost_stack`` precedence — same precedence,
            # same number on the same DB.  Pre-v2 payloads only carry the
            # legacy key; the ``is not None`` fall-through handles both.
            matched = _dec(p.get("realized_pnl_usd_matched"))
            rpnl_swap = matched if matched is not None else rpnl
            amt_in_usd = _dec(p.get("amount_in_usd"))
            if rpnl_swap is not None:
                sum_swap += rpnl_swap
            elif amt_in_usd is not None:
                # VIB-4394: measured amounts but no prior FIFO basis to realize
                # against — an opening / acquiring swap, or the first disposal of
                # pre-existing wallet inventory. A legitimate measured state, NOT
                # a measurement gap. Surfaced for forensics; does NOT fail G6.
                no_prior_basis_swap += 1
            else:
                # Unmeasured amounts: the receipt parser could not resolve a USD
                # value the SWAP path should have emitted. A genuine gap — the
                # reconciliation runs on null, not a real signal. FAIL G6.
                null_swap_rpnl += 1
            if amt_in_usd is not None:
                notional_traded += abs(amt_in_usd)
        if et in ("LP_OPEN", "LP_CLOSE"):
            il = _dec(p.get("il_usd"))
            if il is not None:
                il_diagnostic += il
            if et == "LP_CLOSE":
                if rpnl is None:
                    null_lp_close_rpnl += 1
                else:
                    sum_lp += rpnl
            fees_usd = _dec(p.get("fees_total_usd"))
            if fees_usd is None and et == "LP_CLOSE":
                # Only LP_CLOSE is expected to emit `fees_total_usd`;
                # LP_OPEN doesn't have realized fees yet.
                null_lp_fees += 1
            elif fees_usd is not None:
                sum_fees += fees_usd
            amt0_usd = _dec(p.get("amount0_usd"))
            amt1_usd = _dec(p.get("amount1_usd"))
            if amt0_usd is not None:
                notional_traded += abs(amt0_usd)
            if amt1_usd is not None:
                notional_traded += abs(amt1_usd)
        if et == "BORROW":
            borrowed = _dec(p.get("borrowed_amount_usd"))
            if borrowed is not None:
                debt_outstanding += borrowed
                if debt_outstanding > max_debt:
                    max_debt = debt_outstanding
                notional_traded += abs(borrowed)
        if et == "WITHDRAW":
            interest = _dec(p.get("interest_accrued_usd"))
            if interest is None:
                null_withdraw_interest += 1
            else:
                sum_interest += interest
            amt_usd = _dec(p.get("amount_usd"))
            if amt_usd is not None:
                notional_traded += abs(amt_usd)
        if et in ("REPAY", "DELEVERAGE"):
            interest = _dec(p.get("interest_paid_usd"))
            if interest is None:
                null_repay_interest += 1
            else:
                sum_interest -= interest
            principal = _dec(p.get("principal_repaid_usd"))
            if principal is not None:
                debt_outstanding -= principal
                # Clamp at zero — partial-repay accounting noise can drive
                # the running tally slightly negative without affecting the
                # high-water mark.
                if debt_outstanding < Decimal(0):
                    debt_outstanding = Decimal(0)
            amt_usd = _dec(p.get("amount_usd"))
            if amt_usd is not None:
                notional_traded += abs(amt_usd)
        if et == "PERP_CLOSE":
            funding_p = _dec(p.get("funding_paid_usd"))
            funding_r = _dec(p.get("funding_received_usd"))
            # Funding is "all-or-nothing" per row: a payload that emitted
            # neither is unmeasured. Both being zero is a measured zero
            # (no funding accrued) and is fine.
            if funding_p is None and funding_r is None:
                null_perp_funding += 1
            if funding_p is not None:
                sum_funding -= funding_p
            if funding_r is not None:
                sum_funding += funding_r
            if rpnl is None:
                null_perp_rpnl += 1
            else:
                sum_perp += rpnl
            size = _dec(p.get("size"))
            exit_price = _dec(p.get("exit_price"))
            if size is not None and exit_price is not None:
                notional = abs(size) * abs(exit_price)
                if notional > max_perp_notional:
                    max_perp_notional = notional
        if et == "PERP_OPEN":
            size = _dec(p.get("size"))
            entry_price = _dec(p.get("entry_price"))
            if size is not None and entry_price is not None:
                notional = abs(size) * abs(entry_price)
                if notional > max_perp_notional:
                    max_perp_notional = notional

    # Ambient inventory revaluation term (blueprint 27 §11.5): the component
    # sum above only carries the PnL of tokens the strategy TRADED. The wallet
    # (equity) method also moves when an UNTRADED token the strategy is merely
    # holding changes price between the two endpoint snapshots (idle WETH, the
    # native-gas remainder, the unspent half of a single-sided swap, an open
    # swap lot's residual MTM). That revaluation is a real reconciliation term,
    # not a measurement gap. It is read off the SAME priced[0]/priced[-1] rows
    # that produced wallet_pnl — same snapshot rows ⇒ same marks ⇒ no skew, and
    # no new on-chain / price reads. The term collapses to exactly zero when no
    # untraded inventory exists at either endpoint.
    inv_deployment_id = ""
    for s in (priced[0], priced[-1]):
        did = s.get("deployment_id")
        if did:
            inv_deployment_id = str(did)
            break
    inv = compute_inventory_revaluation(
        snapshot_initial=priced[0],
        snapshot_final=priced[-1],
        accounting_events=acct_events,
        deployment_id=inv_deployment_id,
    )
    # Empty ≠ Zero: a None total means a held token's mark (or an open lot's
    # basis) was unmeasured at an endpoint. Surface it as a null bucket so G6
    # FAILs with a diagnostic instead of folding an unmeasured term in as zero.
    sum_inventory_reval = inv.total_usd if inv.total_usd is not None else Decimal(0)
    null_inventory_reval = 0 if inv.total_usd is not None else 1

    component_pnl = sum_swap + sum_lp + sum_perp + sum_fees + sum_funding + sum_interest - sum_gas
    component_pnl += sum_inventory_reval

    # VIB-3869 (B): primitive-aware notional-scaled tolerance.
    # Replaces the prior `max($0.50, eps_pct × capital)` rule, which on a
    # $5 validation run gave a $0.50 floor — i.e. 10% of capital — masking
    # real reconciliation errors. The new floor is $0.10 (rounding /
    # oracle-noise floor) and the percent is scaled against the right
    # *notional* base for the primitive:
    #   - LP/Spot: 0.25% × notional_traded (sum of swap + LP open/close USD)
    #   - Looping: 0.10% × max(notional_traded, max_debt_outstanding)
    #   - Perp:    0.05% × max_notional_exposure
    floor = Decimal("0.10")
    eps_floor_label = "$0.10"
    # Per-primitive tolerance lives on the ``ScorecardProfile`` (G-A): a flat
    # ``eps_pct`` plus a selector over the three notional bases computed above.
    # ``eps_pct`` / ``scaling_base`` / ``scaling_label`` / ``eps`` are identical
    # to the former if/elif/else ladder for lp/looping/perp.
    _profile = _profile_for(primitive)
    eps_pct = _profile.eps_pct
    scaling_base, scaling_label = _profile.eps_scaling(
        G6Bases(
            notional_traded=notional_traded,
            max_debt=max_debt,
            max_perp_notional=max_perp_notional,
        )
    )
    eps = max(floor, eps_pct * scaling_base)
    capital = max(abs(initial), abs(final))
    gap = abs(wallet_pnl - component_pnl)

    null_breakdown = {
        "Σ_swaps_usd_null_count": null_swap_rpnl,
        "Σ_lp_usd_null_count": null_lp_close_rpnl,
        "Σ_lp_fees_null_count": null_lp_fees,
        "Σ_perp_usd_null_count": null_perp_rpnl,
        "Σ_funding_usd_null_count": null_perp_funding,
        "Σ_interest_supply_null_count": null_withdraw_interest,
        "Σ_interest_borrow_null_count": null_repay_interest,
        # An unmeasured ambient-revaluation term (a held token with no mark, or
        # an open lot with no basis) is a null input to the reconciliation, not
        # a measured zero — FAIL the cell rather than fold it in as zero.
        "Σ_inventory_reval_usd_null_count": null_inventory_reval,
    }
    has_nulls = any(v > 0 for v in null_breakdown.values())

    decomp = {
        "wallet_pnl_usd": str(wallet_pnl),
        "component_pnl_usd": str(component_pnl),
        "Σ_swaps_usd": str(sum_swap),
        # VIB-4394: SWAPs with measured amounts but no prior FIFO basis to
        # realize against (opening / acquiring swaps, first disposal of
        # pre-existing inventory). A legitimate measured state — surfaced for
        # forensics but deliberately NOT in null_breakdown, so it never fails G6.
        "Σ_swaps_no_prior_basis_count": str(no_prior_basis_swap),
        "Σ_lp_usd": str(sum_lp),
        "Σ_perp_usd": str(sum_perp),
        "Σ_fees_usd": str(sum_fees),
        "Σ_funding_usd": str(sum_funding),
        "Σ_interest_usd": str(sum_interest),
        "Σ_gas_usd": str(-sum_gas),
        # Blueprint 27 §11.5 — ambient inventory revaluation (untraded
        # qty_idle × Δmark + open swap-lot residual MTM). "" when unmeasured.
        "Σ_inventory_reval_usd": ("" if inv.total_usd is None else str(sum_inventory_reval)),
        "inventory_reval_confidence": inv.confidence,
        "inventory_reval_per_token": inv.per_token,
        "inventory_reval_excluded_tokens": inv.excluded_tokens,
        "gap_usd": str(gap),
        "ε_threshold_usd": str(eps),
        "ε_pct": str(eps_pct),
        "ε_floor_usd": eps_floor_label,
        "ε_scaling_base_usd": str(scaling_base),
        "ε_scaling_base_label": scaling_label,
        "capital_usd": str(capital),
        "il_diagnostic_usd_NOT_in_PnL": str(il_diagnostic),
        **{k: str(v) for k, v in null_breakdown.items()},
    }

    # VIB-3869 (A): any null in a bucket where the row's intent_type would
    # normally emit a value FAILs G6 — the reconciliation result is
    # otherwise running on unmeasured zero, not a real signal.
    if has_nulls:
        nonzero = {k: v for k, v in null_breakdown.items() if v > 0}
        return (
            CellResult(
                "G6",
                "Reconciliation",
                "FAIL",
                f"component buckets contain unmeasured nulls: {nonzero}; "
                f"wallet=${wallet_pnl} component=${component_pnl} gap=${gap} "
                "(reconciliation result is not trustworthy until inputs are populated)",
                decomposition=decomp,
            ),
            decomp,
        )
    if gap <= eps:
        return (
            CellResult(
                "G6",
                "Reconciliation",
                "PASS",
                f"wallet=${wallet_pnl} component=${component_pnl} gap=${gap} "
                f"(ε=${eps} = {eps_pct} × {scaling_label}=${scaling_base}, floor={eps_floor_label})",
                decomposition=decomp,
            ),
            decomp,
        )
    return (
        CellResult(
            "G6",
            "Reconciliation",
            "FAIL",
            f"wallet=${wallet_pnl} component=${component_pnl} gap=${gap} > ε=${eps} "
            f"({eps_pct} × {scaling_label}=${scaling_base}, floor={eps_floor_label})",
            decomposition=decomp,
        ),
        decomp,
    )


def _cell_g7_attribution(
    ledger: list[dict[str, Any]], pos_events: list[dict[str, Any]], acct_events: list[dict[str, Any]]
) -> CellResult:
    missing = []
    for table_name, rows in (
        ("transaction_ledger", ledger),
        ("position_events", pos_events),
        ("accounting_events", acct_events),
    ):
        for r in rows:
            if not r.get("cycle_id"):
                missing.append((table_name, r.get("id")))
    if missing:
        return CellResult(
            "G7",
            "Attribution (cycle_id everywhere)",
            "FAIL",
            f"{len(missing)} rows missing cycle_id (e.g. {missing[:3]})",
        )
    return CellResult(
        "G7",
        "Attribution (cycle_id everywhere)",
        "PASS",
        f"all rows tagged: ledger={len(ledger)} pos={len(pos_events)} acct={len(acct_events)}",
    )


def _cell_g8_time_series(snapshots: list[dict[str, Any]]) -> CellResult:
    """G8 — strategy equity over time.

    "Equity" here = ``total_value_usd + available_cash_usd``. VIB-3614 split
    deployed (positions) from cash (uninvested wallet) into separate columns;
    the equity curve a Senior DeFi Quant cares about is the SUM. A
    post-teardown snapshot with ``total_value_usd=0`` is *not* a missing
    measurement — every position closed cleanly and the equity collapsed
    into ``available_cash_usd``. Treating that as null double-counts
    teardown success as an accounting failure.

    The cell now fails only when **equity itself** is missing — i.e. both
    columns are unmeasured. Pure cash-only is a valid equity curve point.
    """
    if not snapshots:
        return CellResult("G8", "Time-series (equity curve)", "FAIL", "no snapshots")

    unmeasured = [s for s in snapshots if _snapshot_equity(s) is None]
    if unmeasured:
        return CellResult(
            "G8",
            "Time-series",
            "FAIL",
            f"{len(unmeasured)}/{len(snapshots)} snapshots have unmeasured equity "
            "(both total_value_usd AND available_cash_usd are null)",
        )
    return CellResult(
        "G8",
        "Time-series",
        "PASS",
        f"{len(snapshots)} snapshots with measured equity (positions + cash)",
    )


def _cell_g9_confidence(snapshots: list[dict[str, Any]], acct_events: list[dict[str, Any]]) -> CellResult:
    bad = []
    for s in snapshots:
        # G8 redefined "equity" as ``total_value_usd + available_cash_usd``
        # to handle post-teardown snapshots where ``total_value_usd`` collapses
        # to 0 and the equity is entirely in ``available_cash_usd``. G9 must
        # mirror that — a cash-only snapshot still bears USD value and still
        # requires a confidence stamp; previously it was waved through.
        equity = _snapshot_equity(s)
        if equity is not None and equity != 0 and not s.get("value_confidence"):
            bad.append(("snapshot", s.get("id")))
    for r in acct_events:
        if not r.get("confidence"):
            bad.append(("acct_event", r.get("id")))
    if bad:
        return CellResult(
            "G9",
            "Confidence on every USD",
            "FAIL",
            f"{len(bad)} rows have non-zero USD but no confidence (e.g. {bad[:3]})",
        )
    return CellResult(
        "G9",
        "Confidence on every USD",
        "PASS",
        f"all USD-bearing rows have confidence ({len(snapshots)} snapshots, {len(acct_events)} acct events)",
    )


def _cell_g10_multi_tx_atomicity(
    ledger: list[dict[str, Any]],
    pos_events: list[dict[str, Any]],
    acct_events: list[dict[str, Any]],
) -> CellResult:
    """G10 — Multi-tx atomicity.

    Two distinct contracts (VIB-3868 (C) tightening):

    1. **No double-writes**: a successful intent must produce exactly ONE
       ledger row regardless of how many on-chain transactions it took to
       land (APPROVE+SUPPLY, NPM.multicall LP_CLOSE, …). The cell detects
       "same intent recorded N times" by collapsing on
       ``(cycle_id, intent_type, tx_hash)`` — sharing those three fields is
       what makes two rows "the same intent". Grouping by ``id`` (the PK)
       would be a tautology because every row has a unique PK.

    2. **Cycle-level atomicity**: rows that share a ``cycle_id`` must agree
       on outcome — every dispatched intent within the cycle either
       succeeded or every dispatched intent reverted. A cycle that had
       APPROVE succeed and SUPPLY revert is the failure mode this cell
       must catch. Pre-VIB-3868 G10 grouped only by intent identity, so
       mixed-status cycles silently passed — exactly the false positive
       Codex flagged in PR #1997 review.

    A cycle with a single landed row is uniform-by-construction (no mixed
    status possible) and contributes nothing to either check.
    """
    # ── Contract 1: no double-writes ────────────────────────────────────
    by_intent: dict[Any, int] = {}
    for r in ledger:
        # Skip teardown rows whose tx_hash may be NULL until the intent
        # confirms; G10 evaluates only landed intents (success/fail with a
        # dispatched TX). A None tx_hash on an "in-flight" row would otherwise
        # collide with other in-flight rows in the same cycle.
        tx_hash = r.get("tx_hash")
        if not tx_hash:
            continue
        k = (r.get("cycle_id"), r.get("intent_type"), tx_hash)
        by_intent[k] = by_intent.get(k, 0) + 1
    dups = {k: v for k, v in by_intent.items() if v > 1}
    if dups:
        sample = next(iter(dups))
        return CellResult(
            "G10",
            "Multi-tx atomicity",
            "FAIL",
            f"{len(dups)} ledger entries duplicated for the same intent (e.g. {sample!r} ×{dups[sample]})",
        )

    # ── Contract 2: cycle-level uniform status ─────────────────────────
    # Group rows that landed (have tx_hash) by cycle_id. A cycle is "mixed"
    # when at least one row succeeded and at least one row failed — that's
    # the partial-unwind / partial-supply / leaked-state bug that breaks
    # accounting recoverability.
    cycles: dict[Any, list[dict[str, Any]]] = {}
    for r in ledger:
        if not r.get("tx_hash"):
            continue
        cyc = r.get("cycle_id")
        if cyc is None or cyc == "":
            continue
        cycles.setdefault(cyc, []).append(r)

    mixed: list[tuple[Any, int, int]] = []  # (cycle_id, success_count, fail_count)
    for cyc, rs in cycles.items():
        if len(rs) < 2:
            continue
        successes = sum(1 for r in rs if r.get("success"))
        fails = len(rs) - successes
        if successes > 0 and fails > 0:
            mixed.append((cyc, successes, fails))
    if mixed:
        sample = mixed[:3]
        return CellResult(
            "G10",
            "Multi-tx atomicity",
            "FAIL",
            f"{len(mixed)} cycles have mixed-status ledger rows (some succeeded, some reverted) — e.g. {sample!r}",
        )

    multi_row_cycles = sum(1 for rs in cycles.values() if len(rs) > 1)
    return CellResult(
        "G10",
        "Multi-tx atomicity",
        "PASS",
        f"{len(ledger)} ledger rows; no duplicates; "
        f"{multi_row_cycles}/{len(cycles)} cycles span multiple intents and all are uniform-status",
    )


def _cell_g11_failed_intents(ledger: list[dict[str, Any]]) -> CellResult:
    failed = [r for r in ledger if not r.get("success")]
    if not failed:
        return CellResult(
            "G11",
            "Failed intents",
            "SKIP",
            "no failed intents in this run — cell is N/A but writer contract was unexercised",
        )
    bad = [r for r in failed if not r.get("gas_usd") and (r.get("gas_used") or 0) > 0]
    if bad:
        return CellResult(
            "G11",
            "Failed intents",
            "FAIL",
            f"{len(bad)} failed intents have no gas_usd despite gas_used>0",
        )
    return CellResult("G11", "Failed intents", "PASS", f"{len(failed)} failed intents accounted for")


def _cell_g12_oracle_consistency(ledger: list[dict[str, Any]]) -> CellResult:
    if not ledger:
        return CellResult("G12", "Oracle consistency + source identity", "FAIL", "no ledger rows")
    empty = [r for r in ledger if not r.get("price_inputs_json")]
    if empty:
        return CellResult(
            "G12",
            "Oracle consistency",
            "FAIL",
            f"{len(empty)}/{len(ledger)} ledger rows have empty price_inputs_json",
        )
    # Catch "non-empty but not a JSON object" rows separately from empty rows
    # so a writer that produced ``"[]"`` or ``"42"`` doesn't slip through G12
    # by way of ``_json`` collapsing the bad payload to ``{}`` (which the
    # shape loop below would silently accept).
    malformed_root: list[Any] = []
    for r in ledger:
        raw = r.get("price_inputs_json")
        if not raw:
            continue
        try:
            decoded = json.loads(raw) if isinstance(raw, str) else raw
        except (json.JSONDecodeError, TypeError):
            malformed_root.append((r.get("id"), "decode_error"))
            continue
        if not isinstance(decoded, dict):
            malformed_root.append((r.get("id"), type(decoded).__name__))
    if malformed_root:
        return CellResult(
            "G12",
            "Oracle consistency",
            "FAIL",
            f"{len(malformed_root)} rows have non-object price_inputs_json (e.g. {malformed_root[:3]!r})",
        )
    # Validate shape: should be {symbol_or_addr: {price_usd, oracle_source, ...}}
    bad_shape = []
    for r in ledger:
        d = _json(r.get("price_inputs_json"))
        for sym, entry in d.items():
            if not isinstance(entry, dict) or "price_usd" not in entry or "oracle_source" not in entry:
                bad_shape.append((r.get("id"), sym))
                break
    if bad_shape:
        return CellResult(
            "G12",
            "Oracle consistency",
            "FAIL",
            f"{len(bad_shape)} rows have malformed price_inputs (missing price_usd or oracle_source)",
        )
    return CellResult(
        "G12",
        "Oracle consistency",
        "PASS",
        f"all {len(ledger)} ledger rows have shaped price_inputs_json",
    )


def _coerce_version(raw: Any) -> int | None:
    if raw is None or raw == "":
        return None
    try:
        return int(raw)
    except (ValueError, TypeError):
        return None


def _g13_collect_versions(
    ledger: list[dict[str, Any]],
    acct_events: list[dict[str, Any]],
    acct_payloads: dict[Any, dict[str, Any]],
) -> tuple[dict[_TaxonomyPrimitive, set[int]], list[Any]]:
    """Group matching_policy_version values by primitive.

    Ledger rows carry intent_type rather than event_type, but the keys
    are 1:1 in the taxonomy. Ledger versions fold into UTILITY (lowest-
    volume bucket so it doesn't mask drift). Accounting-events resolve
    primitive via taxonomy lookup; unknown event_types are skipped.
    """
    per_primitive: dict[_TaxonomyPrimitive, set[int]] = {}
    bad_rows: list[Any] = []

    for r in ledger:
        v = _coerce_version(r.get("matching_policy_version"))
        if v is not None:
            per_primitive.setdefault(_TaxonomyPrimitive.UTILITY, set()).add(v)
        elif r.get("matching_policy_version") not in (None, ""):
            bad_rows.append(("ledger", r.get("id")))

    for r in acct_events:
        p = acct_payloads.get(r.get("id"), {})
        v = _coerce_version(p.get("matching_policy_version"))
        if v is None:
            if p.get("matching_policy_version") not in (None, ""):
                bad_rows.append(("acct_event", r.get("id")))
            continue
        et = r.get("event_type") or p.get("event_type")
        if not isinstance(et, str) or not et:
            continue
        # VIB-4477: protocol-aware bucket resolution. ``record_for(et)`` returns
        # ``Primitive.LP`` for every LP event_type; ``primitive_for(et, proto)``
        # overrides LP → LP_V4 when ``payload.protocol`` is ``uniswap_v4`` so V3
        # and V4 rows land in distinct G13 buckets. Falls back to the plain
        # ``record_for`` lookup (no override) when the event_type is unknown.
        try:
            record_for(et)
        except UnknownIntentTypeError:
            continue
        proto = r.get("protocol") or p.get("protocol") or ""
        primitive = primitive_for(et, proto if isinstance(proto, str) else "")
        per_primitive.setdefault(primitive, set()).add(v)

    return per_primitive, bad_rows


def _cell_g13_lot_matching(
    ledger: list[dict[str, Any]],
    acct_events: list[dict[str, Any]],
    acct_payloads: dict[Any, dict[str, Any]],
    payload_errors: dict[Any, str],
) -> CellResult:
    """G13 — Lot-matching policy declared + versioned (per-primitive).

    VIB-4162 (T2): each primitive's events must carry a SINGLE
    matching_policy_version (per-primitive uniqueness). LP can advance
    to v4 while Lending stays at v3 and Perp stays at v1 without
    breaking G13 — drift is only flagged WITHIN a primitive bucket.
    """
    blocked = _payload_block_cell("G13", "Lot-matching policy declared + versioned", acct_events, payload_errors)
    if blocked is not None:
        return blocked

    per_primitive, bad_rows = _g13_collect_versions(ledger, acct_events, acct_payloads)

    if bad_rows:
        return CellResult(
            "G13",
            "Lot-matching policy",
            "FAIL",
            f"{len(bad_rows)} rows have non-integer matching_policy_version (e.g. {bad_rows[:3]!r})",
        )
    if not per_primitive:
        return CellResult(
            "G13",
            "Lot-matching policy declared + versioned",
            "FAIL",
            "no row carries matching_policy_version",
        )

    for primitive, versions in per_primitive.items():
        if len(versions) > 1:
            return CellResult(
                "G13",
                "Lot-matching policy",
                "FAIL",
                f"multiple matching_policy_version values for primitive={primitive.value}: {sorted(versions)}",
            )

    summary = {p.value: next(iter(v)) for p, v in per_primitive.items()}
    return CellResult(
        "G13",
        "Lot-matching policy",
        "PASS",
        f"per-primitive: {summary}",
    )


def _cell_g14_sdk_eq_onchain(
    snapshots: list[dict[str, Any]],
    position_state_rows: list[dict[str, Any]],
) -> CellResult:
    """G14: SDK position state ≡ on-chain state ± 1 bp dust per snapshot.

    Mirrors G15's gate-on-table-absence shape: when ``position_state_snapshots``
    rows are missing, return XFAIL pointing at the missing Track C surface for
    this run. Local SQLite has a Track C caller; hosted mode is still gated, and
    local runs can still have zero rows when the snapshot had no recognizable
    open positions. Once rows exist, the cell must evaluate
    ``delta_vs_protocol_pct`` per row and flip to PASS/FAIL.
    Returning unconditional XFAIL would mean the cell can never advance even
    after the materializer lands — a violation of the matrix's "must move
    forward" contract.
    """
    if not position_state_rows:
        return CellResult(
            "G14",
            "SDK ≡ on-chain reconciliation",
            "XFAIL",
            "no position_state_snapshots rows for this run (Track C absent, hosted-gated, "
            "or no recognizable open positions); cell is xfail by design until rows exist",
        )

    # Track C is wired: evaluate the 1-bp tolerance.
    eps_pct = Decimal("0.0001")  # 1 bp
    bad: list[tuple[Any, Decimal]] = []
    for row in position_state_rows:
        raw = row.get("delta_vs_protocol_pct")
        if raw is None:
            continue
        try:
            delta = Decimal(str(raw))
        except (InvalidOperation, ValueError, TypeError):
            continue
        if abs(delta) > eps_pct:
            bad.append((row.get("position_key") or row.get("id"), delta))
    if bad:
        sample = bad[:3]
        return CellResult(
            "G14",
            "SDK ≡ on-chain reconciliation",
            "FAIL",
            f"{len(bad)} position_state rows exceed 1bp delta vs on-chain (e.g. {sample!r})",
        )
    return CellResult(
        "G14",
        "SDK ≡ on-chain reconciliation",
        "PASS",
        f"all {len(position_state_rows)} position_state rows within 1bp of on-chain state",
    )


# The four top-level primitives whose positions the Track-C materializer
# (``accounting.position_state._classify_position``) actually re-reads on
# chain. Stored as ``Primitive`` enum-value strings so the eligibility check
# below can also recognise a ``position_type`` that is itself one of these
# enum-value labels (e.g. the literal ``"LP_V4"``), which the registry-driven
# ``materializer_primitive_for`` does NOT map (it answers protocol-name
# aliases like ``"UNISWAP_V4"`` and the generic ``"LP"`` label, not the
# enum-value string ``"LP_V4"``).
_TRACK_C_PRIMITIVE_VALUES: frozenset[str] = frozenset(
    {
        _TaxonomyPrimitive.LP.value,
        _TaxonomyPrimitive.LP_V4.value,
        _TaxonomyPrimitive.LENDING.value,
        _TaxonomyPrimitive.PERP.value,
    }
)


def _is_track_c_eligible_position(pos: dict[str, Any]) -> bool:
    """True when a ``positions_json`` entry would produce a Track C
    (``position_state_snapshots``) row.

    Track C is a periodic on-chain re-read of *protocol* positions — LP /
    lending / perp — materialised by
    :func:`almanak.framework.accounting.position_state.materialise_position_state`
    via ``_classify_position`` (which accepts only ``lp`` / ``lp_v4`` /
    ``lending`` / ``perp`` and returns ``None`` for everything else). Wallet
    token inventory — including the VIB-5057 ``swap_inventory_lots``
    pseudo-position (``position_type="TOKEN"``, ``protocol="wallet"``) that
    NAV counts as deployed capital — is intentionally NOT a Track C row
    (``TOKEN`` → ``Primitive.UTILITY`` → ``None``; blueprint 27 §Track C vs
    §7 swap-inventory). G15 must therefore count only Track-C-eligible
    positions when checking per-snapshot coverage, or a clean round-trip that
    ends holding cash-as-deployed-inventory false-fails the cell.

    Resolution is two-pronged so a V4 LP is counted whichever label the
    snapshot carries (VIB-4483):

    * ``materializer_primitive_for`` resolves generic labels (``"LP"``) and
      connector protocol-name aliases (``"UNISWAP_V4"`` → ``Primitive.LP_V4``)
      — exactly what ``_classify_position`` keys off.
    * the ``Primitive`` enum-value strings themselves are recognised directly
      via a case-insensitive match against ``_TRACK_C_PRIMITIVE_VALUES`` (the
      actual enum values are lowercase — ``"lp_v4"`` / ``"lp"`` / ``"lending"``
      / ``"perp"`` — and the input is ``.lower()``-normalised, so a label like
      ``"LP_V4"`` matches). This direct prong is needed because the
      registry-driven materializer maps protocol aliases — not the bare
      enum-value label ``"LP_V4"`` — so a snapshot written with
      ``position_type="LP_V4"`` would otherwise be silently dropped from the
      G15 expected count and a real V4 LP MtM gap would pass unnoticed.

    Neither prong invents a new primitive rule; both fold into the SAME
    Track-C primitive set the materializer honours.
    """
    pt = str(pos.get("position_type") or pos.get("type") or "")
    primitive = materializer_primitive_for(pt)
    if primitive is not None and primitive.value in _TRACK_C_PRIMITIVE_VALUES:
        return True
    # Fallback: the position_type is itself a Track-C primitive enum-value
    # string (e.g. "LP_V4") that the materializer's alias registry does not map.
    return pt.strip().lower() in _TRACK_C_PRIMITIVE_VALUES


def _cell_g15_multi_period_self_consistency(
    snapshots: list[dict[str, Any]], position_state_rows: list[dict[str, Any]]
) -> CellResult:
    """G15: Multi-period MtM self-consistency.

    The honest predicate (post-Track-C wiring, VIB-3891): every snapshot
    that *had open positions* must have a corresponding set of
    ``position_state_snapshots`` rows — one per position. A snapshot
    where the strategy held 3 LP positions but only 2 Track C rows
    landed is a coverage gap that would silently skew the time-series
    the cell is supposed to validate.

    Pre-VIB-3865 this cell was a telescoping tautology
    (``Σ(s[i+1] - s[i]) ≡ s[-1] - s[0]`` for any monotonic measured series)
    and was masquerading as a PASS. The fix replaces that with a
    coverage check that actually depends on Track C inputs — and the
    cell stays XFAIL when no Track C rows exist anywhere, because
    "no rows at all" means Track C is absent for this run (hosted-gated,
    unsupported backend, no recognizable open positions), not a coverage
    mismatch between a parent snapshot and child rows.
    """
    if not position_state_rows:
        return CellResult(
            "G15",
            "Multi-period MtM self-consistency",
            "XFAIL",
            "no position_state_snapshots rows for this run (Track C absent, hosted-gated, "
            "or no recognizable open positions); cell is xfail by design until rows exist",
        )

    # Coverage check: every snapshot that reported open positions must
    # have at least one Track C row tied to it. A row count below the
    # snapshot's open-position count is a partial-write — surface it
    # rather than silently masking with the telescope identity.
    snapshot_position_counts: dict[Any, int] = {}
    unreadable_snapshots: list[Any] = []
    for s in snapshots:
        positions_json = s.get("positions_json")
        if not positions_json or positions_json == "[]":
            continue
        try:
            parsed = json.loads(positions_json)
        except (json.JSONDecodeError, TypeError):
            # Unreadable JSON is NOT "no positions" — surface as coverage
            # failure rather than silently passing G15 as cash-only (VIB-3891).
            unreadable_snapshots.append(s.get("id"))
            continue
        # VIB-4489: accept two writer shapes. Legacy plain list and the
        # versioned-envelope dict the writer ships today
        # ({"schema_version": 1, "positions": [...], "metadata": {...}}).
        # The accept rule is structural: a list at the root, OR a dict
        # whose `positions` key holds a list. Any other shape is malformed.
        if isinstance(parsed, list):
            positions = parsed
        elif isinstance(parsed, dict) and isinstance(parsed.get("positions"), list):
            positions = parsed["positions"]
        else:
            unreadable_snapshots.append(s.get("id"))
            continue
        # Count only Track-C-eligible (protocol LP/lending/perp) positions —
        # the Track C materializer excludes wallet/TOKEN inventory (incl. the
        # VIB-5057 swap_inventory_lots pseudo-position), so counting them here
        # would demand Track C rows that correctly never get written and
        # false-fail a clean round-trip that ends holding deployed cash.
        track_c_positions = [p for p in positions if isinstance(p, dict) and _is_track_c_eligible_position(p)]
        if track_c_positions:
            snapshot_position_counts[s.get("id")] = len(track_c_positions)

    if unreadable_snapshots:
        sample = unreadable_snapshots[:3]
        return CellResult(
            "G15",
            "Multi-period MtM self-consistency",
            "FAIL",
            f"{len(unreadable_snapshots)} snapshot(s) have malformed positions_json "
            f"(JSONDecodeError/TypeError); e.g. {sample!r} — coverage check cannot "
            "reliably classify those snapshots as position-bearing or cash-only",
        )

    if not snapshot_position_counts:
        # Track C rows exist but the strategy never reported open
        # positions on any snapshot — nothing to reconcile against.
        # Treat as PASS: the materializer is wired and chose to write
        # nothing useful (e.g. a strategy that holds only cash). A FAIL
        # here would penalise the strategy for being position-less.
        return CellResult(
            "G15",
            "Multi-period MtM self-consistency",
            "PASS",
            f"{len(position_state_rows)} Track C rows present; no snapshots reported "
            "open positions (cash-only strategy or pre-deploy snapshot)",
        )

    track_c_by_snapshot: dict[Any, int] = {}
    for r in position_state_rows:
        sid = r.get("snapshot_id")
        if sid is None:
            continue
        track_c_by_snapshot[sid] = track_c_by_snapshot.get(sid, 0) + 1

    gaps: list[tuple[Any, int, int]] = []
    for sid, expected in snapshot_position_counts.items():
        actual = track_c_by_snapshot.get(sid, 0)
        # CodeRabbit (2026-05-02): also fail on over-coverage. The DDL has no
        # uniqueness constraint so a retry / double-call could insert the same
        # position twice; ``actual > expected`` is just as much a coverage
        # contract violation as ``actual < expected`` and silently passing
        # an over-counted snapshot would mask the duplication regression.
        if actual != expected:
            gaps.append((sid, expected, actual))
    if gaps:
        sample = gaps[:3]
        return CellResult(
            "G15",
            "Multi-period MtM self-consistency",
            "FAIL",
            f"{len(gaps)} snapshot(s) with mismatched Track C coverage "
            f"(expected = open positions, actual = position_state rows; "
            f"either under- or over-counted); e.g. {sample!r}",
        )
    return CellResult(
        "G15",
        "Multi-period MtM self-consistency",
        "PASS",
        f"every snapshot with open positions has Track C coverage "
        f"({len(snapshot_position_counts)} snapshots, "
        f"{sum(track_c_by_snapshot.values())} Track C rows)",
    )


# ─── VIB-4201 (T15): cell #22 — registry coherence ───────────────────────


def _cell22_position_reference_phid(payload_str: Any) -> str | None:
    """Extract ``physical_identity_hash`` from an ``accounting_events.position_reference`` JSON.

    Returns ``None`` for any of: NULL column, malformed JSON,
    non-dict root, or missing key. The cell's preflight separately
    fails on malformed JSON before this helper is consulted, so reaching
    here for a malformed payload would be an implementation bug — the
    helper is defensive in case a future caller skips the preflight.
    """
    if payload_str is None or payload_str == "":
        return None
    try:
        decoded = json.loads(payload_str) if isinstance(payload_str, str) else payload_str
    except (json.JSONDecodeError, TypeError):
        return None
    if not isinstance(decoded, dict):
        return None
    phid = decoded.get("physical_identity_hash")
    return phid if isinstance(phid, str) and phid else None


def _cell22_registry_coherence(  # noqa: C901
    acct_events: list[dict[str, Any]],
    registry_rows: list[dict[str, Any]],
    *,
    position_reference_column_present: bool,
    position_registry_table_present: bool,
    malformed_position_reference_row_ids: list[Any],
) -> CellResult:
    """L5 cell #22 — bidirectional ``accounting_events`` ↔ ``position_registry`` close coherence.

    The cell's contract (UAT card VIB-4201, ratified by codex SPEC_OK):

    1. Forward direction — every ``accounting_events`` row whose
       ``event_type`` is in :data:`CLOSE_EVENT_TYPES` AND whose
       ``position_reference.physical_identity_hash`` is non-null MUST
       have a matching ``position_registry`` row at ``status='closed'``
       with the same hash.
    2. Inverse direction — every ``position_registry`` row at
       ``status='closed'`` MUST have at least one matching CLOSE
       accounting event whose ``position_reference.physical_identity_hash``
       equals the registry row's hash.

    Verdicts (per the card's "Verdict mapping" table):

    - **XFAIL (F9)** if ``accounting_events.position_reference`` column
      is absent — pre-T10 schemas cannot be evaluated.
    - **FAIL (F10)** if any ``position_reference`` row carries malformed
      JSON. The corrupt payload contaminates the audit trail; surfacing
      it as a row-level skip would silently hide a persistence regression.
    - **FAIL (F6)** if the registry table is absent / empty BUT at least
      one CLOSE event has a non-null hash — the events claim hashes the
      registry never witnessed.
    - **XFAIL (F7)** if the registry is absent / empty AND every CLOSE
      event has a null hash (Day-1 legacy ``source="legacy"`` per
      :mod:`position_reference`).
    - **XFAIL (F8)** if no CLOSE events exist AND no closed registry
      rows exist — the lifecycle wasn't exercised in this run.
    - **FAIL** when forward orphans, inverse orphans, or both exist.
    - **PASS** when both directions agree AND at least one side has data.

    Cell #22 is INFORMATIONAL — it is rendered alongside the original 21
    cells but does NOT contribute to the ≥16/21 gating sum. See
    :func:`AccountantReport.format_markdown` for the gating-line rendering
    contract.
    """
    cell_id = "L5_22"
    description = "Registry coherence (accounting_events ↔ position_registry, bidirectional)"

    # Preflight P1: column exists?
    if not position_reference_column_present:
        return CellResult(
            cell_id,
            description,
            "XFAIL",
            "accounting_events.position_reference column missing (pre-T10 DB); cell cannot evaluate",
        )

    # Preflight P3: malformed JSON?
    if malformed_position_reference_row_ids:
        sample = malformed_position_reference_row_ids[:5]
        return CellResult(
            cell_id,
            description,
            "FAIL",
            f"{len(malformed_position_reference_row_ids)} accounting_events row(s) carry malformed "
            f"position_reference JSON (e.g. ids={sample!r}); corrupt payloads contaminate the audit trail",
        )

    # CLOSE event census (independent of registry presence — the F6/F7
    # boundary needs this number whether the registry is there or not).
    # Hoist `set(CLOSE_EVENT_TYPES)` out of the comprehension so the lookup
    # cost stays O(1) per row instead of rebuilding the set every iteration
    # (gemini-code-assist 2026-05-10).
    _close_event_types = set(CLOSE_EVENT_TYPES)
    close_events = [r for r in acct_events if r.get("event_type") in _close_event_types]
    close_event_phids: set[str] = set()
    close_events_with_hash = 0
    close_events_legacy_null_hash = 0
    for r in close_events:
        phid = _cell22_position_reference_phid(r.get("position_reference"))
        if phid is None:
            close_events_legacy_null_hash += 1
        else:
            close_events_with_hash += 1
            close_event_phids.add(phid)

    # Preflight P2: registry table present?
    # Note: the registry-row sort + set construction below is gated on
    # ``position_registry_table_present`` so pre-T11 fixtures don't pay
    # for work that the registry-absent branches never read
    # (gemini-code-assist 2026-05-10).
    if not position_registry_table_present:
        if close_events_with_hash > 0:
            return CellResult(
                cell_id,
                description,
                "FAIL",
                f"position_registry table absent but {close_events_with_hash} CLOSE accounting "
                f"event(s) carry non-null physical_identity_hash — events claim hashes the "
                f"registry never witnessed",
            )
        if not close_events:
            return CellResult(
                cell_id,
                description,
                "XFAIL",
                "no CLOSE accounting events and no position_registry table — lifecycle not exercised in this run",
            )
        # Registry absent + every CLOSE event has null hash → legacy.
        return CellResult(
            cell_id,
            description,
            "XFAIL",
            f"position_registry table absent and {close_events_legacy_null_hash} CLOSE event(s) "
            f"carry only legacy position_reference (physical_identity_hash=null); registry mode "
            f"not yet on for any primitive in this run",
        )

    # Registry table present — compute closed-row census now (deferred from
    # before the P2 gate so the sort doesn't fire on pre-T11 fixtures).
    # Sort by physical_identity_hash so the FAIL diagnostic sample is
    # deterministic across SQLite versions / file orderings — the cell's
    # idempotency contract (UAT card §D3 F5) requires identical
    # ``(status, diagnostic)`` tuples on repeat runs.
    closed_registry_rows = sorted(
        (r for r in registry_rows if r.get("status") == "closed"),
        key=lambda r: r.get("physical_identity_hash") or "",
    )
    closed_registry_phids: set[str] = {
        r["physical_identity_hash"] for r in closed_registry_rows if r.get("physical_identity_hash")
    }

    # Compute the bidirectional orphan sets. Carry the extracted hash on
    # each forward-orphan tuple so the diagnostic sample doesn't re-parse
    # ``position_reference`` JSON (gemini-code-assist 2026-05-10).
    forward_orphans: list[tuple[dict[str, Any], str]] = [
        (r, phid)
        for r in close_events
        if (phid := _cell22_position_reference_phid(r.get("position_reference"))) is not None
        and phid not in closed_registry_phids
    ]
    inverse_orphans = [r for r in closed_registry_rows if r.get("physical_identity_hash") not in close_event_phids]

    if forward_orphans or inverse_orphans:
        diag_parts: list[str] = []
        if forward_orphans:
            sample_fwd = [
                {
                    "acct_event_id": r.get("id"),
                    "event_type": r.get("event_type"),
                    "phid": phid,
                }
                for r, phid in forward_orphans[:3]
            ]
            diag_parts.append(
                f"{len(forward_orphans)} forward orphan(s) — CLOSE event with hash but no closed "
                f"registry row (e.g. {sample_fwd!r})"
            )
        if inverse_orphans:
            sample_inv = [
                {
                    "phid": r.get("physical_identity_hash"),
                    "primitive": r.get("primitive"),
                    "closed_tx": r.get("closed_tx"),
                }
                for r in inverse_orphans[:3]
            ]
            diag_parts.append(
                f"{len(inverse_orphans)} inverse orphan(s) — closed registry row with no matching "
                f"CLOSE event (e.g. {sample_inv!r})"
            )
        return CellResult(
            cell_id,
            description,
            "FAIL",
            "; ".join(diag_parts),
        )

    # No orphans on either side. Determine if work was actually exercised.
    if close_events_with_hash == 0 and not closed_registry_phids:
        # Registry table is present but empty AND no CLOSE events with
        # hashes were emitted. Either pre-cutover for every primitive in
        # this run, or no close lifecycle exercised. Either way, the
        # cell did not have the inputs to make a meaningful claim.
        if not close_events:
            return CellResult(
                cell_id,
                description,
                "XFAIL",
                "no CLOSE accounting events and no closed position_registry rows — lifecycle not exercised in this run",
            )
        return CellResult(
            cell_id,
            description,
            "XFAIL",
            f"position_registry present but empty (0 closed rows) and {close_events_legacy_null_hash} CLOSE "
            f"event(s) carry only legacy position_reference (null hash); registry mode not yet on",
        )
    return CellResult(
        cell_id,
        description,
        "PASS",
        f"bidirectional coherence holds: {close_events_with_hash} CLOSE event(s) with hash, "
        f"{len(closed_registry_phids)} closed registry row(s); zero orphans on either side",
    )


# ─── Primitive-specific cells ────────────────────────────────────────────


# IL sanity factor for LP4 — ``|il_usd|`` must not exceed
# ``_LP4_IL_SANITY_FACTOR × max(|cost_basis_usd|, |hodl_value_usd|)``.
# Factor 2.0 accommodates legitimate large-IL positions while still
# catching "IL = entire position value" pathology (lp-close-may20.md §6.5).
_LP4_IL_SANITY_FACTOR = Decimal("2.0")


def _lp4_insanity_signature(row: dict[str, Any], payload: dict[str, Any]) -> dict[str, Any] | None:
    """Return a diagnostic dict if ``payload.il_usd`` violates the LP4 sanity
    bound on this LP_CLOSE row, else ``None``.

    See ``_cells_lp`` LP4 block for context. Presence-only check was the
    safety-net gap that let the lp-close-may20.md principal-as-fees bug land
    green: ``il_usd = −hodl_value_usd`` is not "PASS, il_usd exists" — it is
    "FAIL, il_usd is economically impossible".
    """
    il_raw = payload.get("il_usd")
    if il_raw is None or row.get("event_type") != "LP_CLOSE":
        return None
    try:
        il = abs(Decimal(str(il_raw)))
    except (InvalidOperation, ValueError, TypeError):
        return None  # malformed numeric — handled by the payload_block path
    cost_basis_raw = payload.get("cost_basis_usd")
    hodl_raw = payload.get("hodl_value_usd")
    references: list[Decimal] = []
    for ref in (cost_basis_raw, hodl_raw):
        if ref is None:
            continue
        try:
            references.append(abs(Decimal(str(ref))))
        except (InvalidOperation, ValueError, TypeError):
            continue
    if not references:
        return None  # no reference scale to compare against
    reference_max = max(references)
    if reference_max == 0:
        return (
            {
                "id": row.get("id"),
                "il_usd": il_raw,
                "cost_basis_usd": cost_basis_raw,
                "hodl_value_usd": hodl_raw,
            }
            if il > 0
            else None
        )
    if il > _LP4_IL_SANITY_FACTOR * reference_max:
        return {
            "id": row.get("id"),
            "il_usd": il_raw,
            "cost_basis_usd": cost_basis_raw,
            "hodl_value_usd": hodl_raw,
            "factor": float(il / reference_max),
        }
    return None


def _lp4_il_sanity_cell(
    lp_acct: list[dict[str, Any]],
    acct_payloads: dict[Any, dict[str, Any]],
) -> CellResult:
    """Build the LP4 cell with the sanity bound applied.

    PASS — at least one LP_CLOSE payload carries ``il_usd`` and every
    LP_CLOSE row whose ``il_usd`` is set has it within
    ``_LP4_IL_SANITY_FACTOR × max(|cost_basis|, |hodl|)``.
    FAIL — any LP_CLOSE row violates the sanity bound (lp-close-may20.md).
    XFAIL — no payload carries ``il_usd`` (handler hasn't started emitting it).
    """
    has_il = False
    for row in lp_acct:
        payload = acct_payloads.get(row.get("id"), {})
        if payload.get("il_usd") is None:
            continue
        has_il = True
        insane = _lp4_insanity_signature(row, payload)
        if insane is not None:
            return CellResult(
                "LP4",
                "Impermanent loss (diagnostic, NOT in net PnL)",
                "FAIL",
                (
                    f"il_usd magnitude exceeds {_LP4_IL_SANITY_FACTOR}× max(|cost_basis|,|hodl|) "
                    f"on LP_CLOSE row {insane.get('id')}: {insane} — "
                    "see lp-close-may20.md (principal-as-fees signature)."
                ),
            )
    return CellResult(
        "LP4",
        "Impermanent loss (diagnostic, NOT in net PnL)",
        "PASS" if has_il else "XFAIL",
        "il_usd in LP_CLOSE payload within sanity bound" if has_il else "il_usd not yet emitted by LP close handler",
    )


# The decomposition legs ``attribute_lp`` writes onto the CLOSE
# ``attribution_json`` (pnl_attributor.py). LP5 PASSes only when a CLOSE event
# carries the LP marker plus all four — anything short means attribution did
# not run end-to-end.
_LP5_REQUIRED_FIELDS = (
    "net_pnl_usd",
    "principal_deposited_usd",
    "principal_recovered_usd",
    "price_pnl_usd",
)


def _lp5_decomposition_cell(pos_events: list[dict[str, Any]]) -> CellResult:
    """LP5 (VIB-4263): open→close delta decomposition present on a CLOSE event.

    Data-presence predicate over each CLOSE position_event's
    ``attribution_json``, mirroring LP2 / LP6. PASS when some CLOSE event's
    decomposition has ``position_type == "LP"`` and every field in
    ``_LP5_REQUIRED_FIELDS`` is present and non-empty (Empty != zero — an
    empty-string leg is "not computed", not measured zero). Otherwise XFAIL
    with the original diagnostic (no regression to the prior verdict when
    attribution has not run).
    """
    for r in pos_events:
        if r.get("event_type") != "CLOSE":
            continue
        decomp = _json(r.get("attribution_json"))
        if decomp.get("position_type") != "LP":
            continue
        if all(decomp.get(f) not in (None, "") for f in _LP5_REQUIRED_FIELDS):
            present = ", ".join(_LP5_REQUIRED_FIELDS)
            return CellResult(
                "LP5",
                "LP open→close delta decomposition",
                "PASS",
                f"CLOSE attribution_json carries LP decomposition ({present})",
            )
    return CellResult(
        "LP5",
        "LP open→close delta decomposition",
        "XFAIL",
        "attribution_json LP decomposition not yet computed",
    )


def _cells_lp(
    pos_events: list[dict[str, Any]],
    acct_events: list[dict[str, Any]],
    snapshots: list[dict[str, Any]],
    acct_payloads: dict[Any, dict[str, Any]],
    payload_errors: dict[Any, str],
    position_state_rows: list[dict[str, Any]] | None = None,
) -> list[CellResult]:
    position_state_rows = position_state_rows or []
    lp_state_rows = [r for r in position_state_rows if r.get("position_type") == "LP"]
    out: list[CellResult] = []
    # LP1: range exposure
    has_ticks = any(r.get("tick_lower") is not None and r.get("tick_upper") is not None for r in pos_events)
    out.append(
        CellResult(
            "LP1",
            "Range exposure (tick_lower/upper/current_tick at every snapshot)",
            "PASS" if has_ticks else "FAIL",
            "found tick_lower/upper on position_events" if has_ticks else "no position_events row carries ticks",
        )
    )
    # LP2: in-range time fraction (Track C)
    if lp_state_rows:
        in_range_rows = [r for r in lp_state_rows if r.get("in_range") is not None]
        if in_range_rows:
            in_range_count = sum(1 for r in in_range_rows if r.get("in_range"))
            fraction = in_range_count / len(in_range_rows)
            out.append(
                CellResult(
                    "LP2",
                    "In-range time (fraction over hold)",
                    "PASS",
                    f"{in_range_count}/{len(in_range_rows)} samples in-range ({fraction:.2%}); track-c rows present",
                )
            )
        else:
            out.append(
                CellResult(
                    "LP2",
                    "In-range time (fraction over hold)",
                    "FAIL",
                    f"{len(lp_state_rows)} LP track-c rows but none has in_range populated — "
                    "LP observer is not emitting in_range",
                )
            )
    else:
        out.append(
            CellResult(
                "LP2",
                "In-range time (fraction over hold)",
                "XFAIL",
                "no LP rows in position_state_snapshots (no LP observers wired or no LP positions)",
            )
        )
    # LP3: fees per position
    fees_seen = any(r.get("fees_token0") or r.get("fees_token1") for r in pos_events)
    out.append(
        CellResult(
            "LP3",
            "Fees earned per position",
            "PASS" if fees_seen else "FAIL",
            "position_events.fees_token0/1 populated" if fees_seen else "no fees_token0/1 on any position_event",
        )
    )
    # LP4: IL diagnostic — VIB-3868 / lp-close-may20.md §6.5. Sanity-bound
    # check (not presence-only): see ``_lp4_il_sanity_cell``.
    lp_acct = [r for r in acct_events if r.get("event_type") in ("LP_OPEN", "LP_CLOSE")]
    blocked = _payload_block_cell("LP4", "Impermanent loss (diagnostic, NOT in net PnL)", lp_acct, payload_errors)
    if blocked is not None:
        out.append(blocked)
    else:
        out.append(_lp4_il_sanity_cell(lp_acct, acct_payloads))
    # LP5: open→close delta decomposition (VIB-4263).
    #
    # Gate on data presence in the CLOSE position_event's ``attribution_json``,
    # mirroring the conditional-XFAIL shape of LP2 / LP6 (which gate on Track-C
    # row presence). Before VIB-4263 this cell was an UNCONDITIONAL XFAIL with
    # no PASS branch, so it could never validate what it claims even once the
    # upstream attribution (VIB-3954, computed by ``attribute_lp`` →
    # ``run_attribution_on_close``) lands. The required decomposition fields are
    # exactly the ones ``attribute_lp`` emits (pnl_attributor.py): the marker
    # ``position_type == "LP"`` plus the four core USD legs. A field present but
    # empty-string is treated as "not computed" — Empty != zero per CLAUDE.md;
    # the genuine values are always non-empty ``str(Decimal(...))`` when
    # attribution ran.
    out.append(_lp5_decomposition_cell(pos_events))
    # LP6: liquidity over time (Track C)
    if lp_state_rows:
        # CodeRabbit (2026-05-02): position_state.py materialises liquidity as
        # an integer column, so SQLite reads it back as int 0 — not the
        # string "0". Include numeric 0 in the empty-set check so LP6
        # doesn't pass on rows that genuinely have zero liquidity.
        liq_rows = [r for r in lp_state_rows if r.get("liquidity") not in (None, "", "0", 0)]
        if liq_rows:
            out.append(
                CellResult(
                    "LP6",
                    "Liquidity over time",
                    "PASS",
                    f"{len(liq_rows)}/{len(lp_state_rows)} LP rows carry non-zero liquidity",
                )
            )
        else:
            out.append(
                CellResult(
                    "LP6",
                    "Liquidity over time",
                    "FAIL",
                    f"{len(lp_state_rows)} LP track-c rows but none has non-zero liquidity — "
                    "LP observer is not reading pool liquidity",
                )
            )
    else:
        out.append(
            CellResult(
                "LP6",
                "Liquidity over time",
                "XFAIL",
                "no LP rows in position_state_snapshots",
            )
        )
    return out


def _cells_lending(  # noqa: C901
    acct_events: list[dict[str, Any]],
    snapshots: list[dict[str, Any]],
    acct_payloads: dict[Any, dict[str, Any]],
    payload_errors: dict[Any, str],
    position_state_rows: list[dict[str, Any]] | None = None,
) -> list[CellResult]:
    position_state_rows = position_state_rows or []
    lending_state_rows = [r for r in position_state_rows if r.get("position_type") == "LENDING"]
    out: list[CellResult] = []
    # L1: net carry — VIB-3868 validated reads. A WITHDRAW/REPAY payload
    # that fails Pydantic validation FAILs L1 with the schema-mismatch
    # message instead of silently summing zero interest.
    lending_acct = [r for r in acct_events if r.get("event_type") in ("WITHDRAW", "REPAY", "DELEVERAGE")]
    blocked = _payload_block_cell("L1", "Net carry (supply_int − borrow_int)", lending_acct, payload_errors)
    # CodeRabbit (2026-05-02): a malformed payload only invalidates the
    # payload-driven cells (L1 / L4 / L6). L2 / L3 / L5 read Track-C
    # ``position_state_rows`` and are independent of the payload schema —
    # do NOT short-circuit them on a payload validation failure.
    payload_blocked = blocked is not None
    if blocked is not None:
        out.append(blocked)
        out.append(
            CellResult(
                "L4",
                "Principal vs interest at REPAY",
                "FAIL",
                "lending payload(s) failed Pydantic validation; cell data unusable",
            )
        )
        out.append(
            CellResult(
                "L6",
                "Loop-leg attribution",
                "FAIL",
                "lending payload(s) failed Pydantic validation; cell data unusable",
            )
        )
    if not payload_blocked:
        # CodeRabbit (2026-05-02): truthiness checks collapse Decimal("0") /
        # "0" into "missing", which downgrades a measured-zero-carry run to
        # XFAIL (Empty ≠ zero per CLAUDE.md). Use explicit ``not in (None, "")``
        # to preserve measured zero. Also include DELEVERAGE — the rest of
        # this file treats it as a REPAY-class event; missing it under-counts
        # borrow interest on deleveraging loops.
        interest_supply = Decimal(0)
        interest_borrow = Decimal(0)
        for r in acct_events:
            p = acct_payloads.get(r.get("id"), {})
            accrued = p.get("interest_accrued_usd")
            if r.get("event_type") == "WITHDRAW" and accrued not in (None, ""):
                interest_supply += _dec(accrued) or Decimal(0)
            paid = p.get("interest_paid_usd")
            if r.get("event_type") in ("REPAY", "DELEVERAGE") and paid not in (None, ""):
                interest_borrow += _dec(paid) or Decimal(0)
        if interest_supply or interest_borrow:
            out.append(
                CellResult(
                    "L1",
                    "Net carry (supply_int − borrow_int)",
                    "PASS",
                    f"supply=${interest_supply} borrow=${interest_borrow} net=${interest_supply - interest_borrow}",
                )
            )
        else:
            out.append(
                CellResult(
                    "L1",
                    "Net carry",
                    "XFAIL",
                    "no interest_*_usd captured (needs Track C materializer for accrual or REPAY/WITHDRAW with interest split)",
                )
            )
    # L2: HF/LTV trajectory (Track C)
    if lending_state_rows:
        hf_rows = [r for r in lending_state_rows if r.get("health_factor") not in (None, "")]
        if hf_rows:
            out.append(
                CellResult(
                    "L2",
                    "HF / LTV trajectory",
                    "PASS",
                    f"{len(hf_rows)}/{len(lending_state_rows)} lending track-c rows carry health_factor",
                )
            )
        else:
            out.append(
                CellResult(
                    "L2",
                    "HF / LTV trajectory",
                    "FAIL",
                    f"{len(lending_state_rows)} lending track-c rows but none has health_factor — "
                    "lending observer is not reading HF (depends on lending pre/post-state pipeline)",
                )
            )
    else:
        out.append(
            CellResult(
                "L2",
                "HF / LTV trajectory",
                "XFAIL",
                "no LENDING rows in position_state_snapshots",
            )
        )
    # L3: liquidation buffer (Track C). Reuses HF samples; the buffer is
    # min(HF) > 1.0 across the trajectory.
    if lending_state_rows:
        hf_decimals: list[Decimal] = []
        for r in lending_state_rows:
            try:
                hf = Decimal(str(r.get("health_factor")))
                hf_decimals.append(hf)
            except (InvalidOperation, ValueError, TypeError):
                continue
        if hf_decimals:
            min_hf = min(hf_decimals)
            if min_hf > Decimal("1.0"):
                out.append(
                    CellResult(
                        "L3",
                        "Liquidation buffer",
                        "PASS",
                        f"min(HF) = {min_hf} across {len(hf_decimals)} samples (> 1.0)",
                    )
                )
            else:
                out.append(
                    CellResult(
                        "L3",
                        "Liquidation buffer",
                        "FAIL",
                        f"min(HF) = {min_hf} ≤ 1.0 — strategy entered liquidation territory",
                    )
                )
        else:
            out.append(
                CellResult(
                    "L3",
                    "Liquidation buffer",
                    "FAIL",
                    f"{len(lending_state_rows)} lending rows but no parseable health_factor",
                )
            )
    else:
        out.append(
            CellResult(
                "L3",
                "Liquidation buffer",
                "XFAIL",
                "no LENDING rows in position_state_snapshots",
            )
        )
    # L4: principal vs interest at REPAY (skip when payload validation blocked above).
    # Both spec names (``principal_repaid_usd`` / ``interest_paid_usd``) and
    # legacy ``*_delta_usd`` names are accepted — the writer projects from
    # the legacy fields to the spec names (see writer._project_lending_aliases).
    # ``interest_paid_usd`` may be None in cases where there were no matching
    # BORROW lots (FIFO miss) — that's UNAVAILABLE rather than a fail. The
    # cell looks for AT LEAST ONE REPAY row where the split was emittable.
    if not payload_blocked:
        has_split = False
        repay_rows = 0
        for r in acct_events:
            if r.get("event_type") in ("REPAY", "DELEVERAGE"):
                repay_rows += 1
                p = acct_payloads.get(r.get("id"), {})
                principal = p.get("principal_repaid_usd")
                if principal is None:
                    principal = p.get("principal_delta_usd")
                interest = p.get("interest_paid_usd")
                if interest is None:
                    interest = p.get("interest_delta_usd")
                if principal is not None and interest is not None:
                    has_split = True
                    break
        if has_split:
            out.append(
                CellResult(
                    "L4",
                    "Principal vs interest at REPAY",
                    "PASS",
                    f"REPAY payload has principal/interest split ({repay_rows} REPAY-class rows)",
                )
            )
        elif repay_rows == 0:
            out.append(
                CellResult(
                    "L4",
                    "Principal vs interest at REPAY",
                    "SKIP",
                    "no REPAY rows in this run — split contract is unexercised",
                )
            )
        else:
            out.append(
                CellResult(
                    "L4",
                    "Principal vs interest at REPAY",
                    "FAIL",
                    f"{repay_rows} REPAY rows but principal/interest split missing — "
                    "FIFO basis store may not have a matching BORROW lot",
                )
            )
    # L5: APR/APY snapshot (Track C)
    if lending_state_rows:
        apr_rows = [
            r
            for r in lending_state_rows
            if r.get("supply_apy_pct") not in (None, "") or r.get("borrow_apy_pct") not in (None, "")
        ]
        if apr_rows:
            out.append(
                CellResult(
                    "L5",
                    "APR / APY snapshot",
                    "PASS",
                    f"{len(apr_rows)}/{len(lending_state_rows)} lending track-c rows carry "
                    "supply_apy_pct and/or borrow_apy_pct",
                )
            )
        else:
            out.append(
                CellResult(
                    "L5",
                    "APR / APY snapshot",
                    "FAIL",
                    f"{len(lending_state_rows)} lending rows but none has APR/APY — "
                    "lending observer is not reading rates",
                )
            )
    else:
        out.append(
            CellResult(
                "L5",
                "APR / APY snapshot",
                "XFAIL",
                "no LENDING rows in position_state_snapshots",
            )
        )
    # L6: loop-leg attribution (VIB-3964).
    # The basis store now mints swap-key acquisition lots on BORROW / WITHDRAW
    # and consumes them on SUPPLY / REPAY, so a SWAP that disposes the borrowed
    # token reports a non-null ``realized_pnl_usd``. The cell PASSes when the
    # accounting events tell a coherent loop story:
    #   1. At least one BORROW and one REPAY (loop is structurally complete).
    #   2. At least one SWAP whose ``token_in`` matches a borrowed asset
    #      (the borrow→swap leg actually executed).
    #   3. Every SWAP carries a non-null ``realized_pnl_usd`` (basis was
    #      attributed end-to-end — same invariant G6 enforces, repeated here
    #      because L6 should fail loudly for the loop primitive even if a
    #      future G6 tolerance change masks it).
    if not payload_blocked:
        # CodeRabbit 2026-05-04: L6 also reads ``BORROW.asset`` and
        # ``SWAP.token_in`` / ``SWAP.realized_pnl_usd`` — so a payload
        # validation error on a BORROW or SWAP row would otherwise hand L6
        # an empty dict and the cell would misclassify as "loop incomplete"
        # or "null PnL" instead of surfacing the schema mismatch. The
        # earlier ``payload_blocked`` check covers WITHDRAW/REPAY/DELEVERAGE
        # only (it gates L1); BORROW+SWAP need their own block here.
        l6_borrow_swap_rows = [r for r in acct_events if r.get("event_type") in ("BORROW", "SWAP")]
        l6_blocked = _payload_block_cell("L6", "Loop-leg attribution", l6_borrow_swap_rows, payload_errors)
        if l6_blocked is not None:
            out.append(l6_blocked)
            return out

        borrow_assets: set[str] = set()
        repay_count = 0
        for r in acct_events:
            et = r.get("event_type")
            p = acct_payloads.get(r.get("id"), {}) or {}
            asset = (p.get("asset") or "").upper()
            if et == "BORROW" and asset:
                borrow_assets.add(asset)
            elif et in ("REPAY", "DELEVERAGE") and asset:
                repay_count += 1

        swap_payloads = [acct_payloads.get(r.get("id"), {}) or {} for r in acct_events if r.get("event_type") == "SWAP"]
        # CodeRabbit 2026-05-04: L6 is "loop-leg attribution" — a non-loop
        # SWAP (e.g. a side spot trade in the same strategy) carrying a null
        # realized_pnl_usd shouldn't FAIL the loop-leg cell. Filter to swaps
        # whose token_in matches a borrowed asset before checking nulls.
        loop_leg_payloads = [p for p in swap_payloads if (p.get("token_in") or "").upper() in borrow_assets]
        # VIB-4905 (F1) — INTENTIONAL DIVERGENCE from the G6/G3 precedence
        # walk: L6 stays on legacy ``realized_pnl_usd`` deliberately.  L6's
        # invariant is stricter than G6's reconciliation — every loop-leg
        # SWAP MUST fully match against accumulated BORROW/WITHDRAW basis
        # credits because the wallet-basis store (VIB-3964 v3) mints
        # acquisition lots on BORROW and consumes them on REPAY/SUPPLY.  A
        # partial-match loop-leg SWAP signals that the basis store missed a
        # credit somewhere — exactly the failure mode L6 must surface.
        # Migrating to the matched-portion precedence walk would mask that
        # by counting the partial-match as a successful loop leg.  Pre-V2
        # payloads only carry ``realized_pnl_usd`` anyway, so the legacy
        # field is the safer signal here.
        null_loop_leg_pnl = sum(1 for p in loop_leg_payloads if p.get("realized_pnl_usd") is None)

        if not borrow_assets or repay_count == 0:
            out.append(
                CellResult(
                    "L6",
                    "Loop-leg attribution",
                    "XFAIL",
                    f"loop incomplete (borrows={len(borrow_assets)}, repays={repay_count}) — "
                    "cell only applies when both legs executed",
                )
            )
        elif not loop_leg_payloads:
            out.append(
                CellResult(
                    "L6",
                    "Loop-leg attribution",
                    "FAIL",
                    f"borrow asset(s) {sorted(borrow_assets)} never appeared as SWAP.token_in — no observable loop leg",
                )
            )
        elif null_loop_leg_pnl:
            out.append(
                CellResult(
                    "L6",
                    "Loop-leg attribution",
                    "FAIL",
                    f"{null_loop_leg_pnl}/{len(loop_leg_payloads)} loop-leg SWAPs have realized_pnl_usd=null — "
                    "wallet basis store missed a BORROW/WITHDRAW credit",
                )
            )
        else:
            out.append(
                CellResult(
                    "L6",
                    "Loop-leg attribution",
                    "PASS",
                    f"{len(loop_leg_payloads)} loop-leg SWAP(s) dispose borrow asset(s) "
                    f"{sorted(borrow_assets)}; all carry realized_pnl_usd",
                )
            )
    return out


def _cells_perp(
    acct_events: list[dict[str, Any]],
    pos_events: list[dict[str, Any]],
    acct_payloads: dict[Any, dict[str, Any]],
    payload_errors: dict[Any, str],
) -> list[CellResult]:
    out: list[CellResult] = []
    has_open = any(r.get("event_type") == "PERP_OPEN" for r in pos_events)
    has_close = any(r.get("event_type") == "PERP_CLOSE" for r in pos_events)
    out.append(
        CellResult(
            "P1",
            "Position lifecycle (size, leverage, direction, entry/exit price)",
            "PASS" if (has_open or has_close) else "XFAIL",
            f"OPEN={has_open} CLOSE={has_close} on position_events",
        )
    )
    out.append(
        CellResult(
            "P2",
            "Cumulative funding paid/received during hold",
            "XFAIL",
            "needs position_state_snapshots (Track C)",
        )
    )
    perp_acct = [r for r in acct_events if r.get("event_type") in ("PERP_OPEN", "PERP_CLOSE")]
    blocked_p3 = _payload_block_cell("P3", "Open + close fees + price impact (separable)", perp_acct, payload_errors)
    if blocked_p3 is not None:
        out.append(blocked_p3)
    else:
        has_fee_split = False
        for r in perp_acct:
            p = acct_payloads.get(r.get("id"), {})
            if p.get("open_fee_usd") is not None or p.get("close_fee_usd") is not None:
                has_fee_split = True
                break
        out.append(
            CellResult(
                "P3",
                "Open + close fees + price impact (separable)",
                "PASS" if has_fee_split else "XFAIL",
                "fee fields in PERP_*_PAYLOAD" if has_fee_split else "fee fields not yet populated",
            )
        )
    out.append(CellResult("P4", "Liquidation buffer over time", "XFAIL", "Track C"))
    perp_close_acct = [r for r in acct_events if r.get("event_type") == "PERP_CLOSE"]
    blocked_p5 = _payload_block_cell(
        "P5", "Realised PnL with funding/fees decomposition", perp_close_acct, payload_errors
    )
    if blocked_p5 is not None:
        out.append(blocked_p5)
    else:
        has_realized = False
        for r in perp_close_acct:
            p = acct_payloads.get(r.get("id"), {})
            if p.get("realized_pnl_usd") is not None:
                has_realized = True
                break
        out.append(
            CellResult(
                "P5",
                "Realised PnL with funding/fees decomposition",
                "PASS" if has_realized else "XFAIL",
                "PERP_CLOSE.realized_pnl_usd present" if has_realized else "realized_pnl_usd null/missing",
            )
        )
    out.append(CellResult("P6", "Margin utilisation over time", "XFAIL", "Track C"))
    return out


def _cells_pendle_pt(
    acct_events: list[dict[str, Any]],
    pos_events: list[dict[str, Any]],
    acct_payloads: dict[Any, dict[str, Any]],
    payload_errors: dict[Any, str],
) -> list[CellResult]:
    """PEN1–PEN6 — the Pendle Principal Token (PT) scorecard.

    A PT is economically a fixed-yield position (buy at a discount to par,
    redeem/sell to realise yield), even though its events ride the SWAP
    primitive in the taxonomy (``taxonomy.py`` ``PENDLE_PT → Primitive.SWAP``).
    These cells score the PT-specific economics off the ``PendleAccountingEvent``
    payload (``pt_amount``/``sy_amount``/``pt_price``/``implied_apr_bps``/
    ``realized_yield_usd``) and the ``PENDLE_PT`` position_events lifecycle.
    """
    out: list[CellResult] = []
    pt_buys = [r for r in acct_events if r.get("event_type") == "PT_BUY"]
    pt_disposals = [r for r in acct_events if r.get("event_type") in ("PT_SELL", "PT_REDEEM")]

    # PEN1 — acquisition cost basis: a PT buy books principal (pt_amount) AND its
    # SY cost (sy_amount), the two inputs every downstream PnL number needs.
    blocked = _payload_block_cell(
        "PEN1", "PT acquisition cost basis (principal + SY cost booked)", pt_buys, payload_errors
    )
    if blocked is not None:
        out.append(blocked)
    else:
        ok = False
        for r in pt_buys:
            p = acct_payloads.get(r.get("id"), {})
            if p.get("pt_amount") is not None and p.get("sy_amount") is not None:
                ok = True
                break
        out.append(
            CellResult(
                "PEN1",
                "PT acquisition cost basis (principal + SY cost booked)",
                "PASS" if ok else "XFAIL",
                "PT_BUY books pt_amount + sy_amount" if ok else "no PT_BUY carrying pt_amount + sy_amount",
            )
        )

    # PEN2 — entry economics: discount-to-par (pt_price) and implied fixed APY
    # (implied_apr_bps) are persisted at entry so the thesis is auditable.
    blocked = _payload_block_cell("PEN2", "Discount-to-par + implied fixed APY at entry", pt_buys, payload_errors)
    if blocked is not None:
        out.append(blocked)
    else:
        ok = False
        for r in pt_buys:
            p = acct_payloads.get(r.get("id"), {})
            if p.get("pt_price") is not None and p.get("implied_apr_bps") is not None:
                ok = True
                break
        out.append(
            CellResult(
                "PEN2",
                "Discount-to-par + implied fixed APY at entry",
                "PASS" if ok else "XFAIL",
                "PT_BUY carries pt_price + implied_apr_bps" if ok else "pt_price / implied_apr_bps not populated",
            )
        )

    # PEN3 — open-PT mark-to-market (unrealised discount accretion). Requires the
    # portfolio valuer to price an open PT (G-PT2) which needs a gateway PT price
    # path (VIB-5276); until then an open PT carries no cost_basis_usd /
    # unrealized_pnl_usd. XFAIL, not FAIL — the capability is absent, not wrong.
    out.append(
        CellResult(
            "PEN3",
            "Open-PT mark-to-market (unrealised discount accretion)",
            "XFAIL",
            "open-PT valuer unwired (G-PT2 / VIB-5276 gateway PT price)",
        )
    )

    # PEN4 — realised fixed yield: a PT sell/redeem books realized_yield_usd
    # (sy_received − matched-lot cost) against the FIFO buy lot. This is the
    # strategy's entire payoff.
    blocked = _payload_block_cell(
        "PEN4", "Realised fixed yield on sell/redeem (FIFO-matched)", pt_disposals, payload_errors
    )
    if blocked is not None:
        out.append(blocked)
    else:
        ok = False
        for r in pt_disposals:
            p = acct_payloads.get(r.get("id"), {})
            if p.get("realized_yield_usd") is not None:
                ok = True
                break
        out.append(
            CellResult(
                "PEN4",
                "Realised fixed yield on sell/redeem (FIFO-matched)",
                "PASS" if ok else "XFAIL",
                "PT_SELL/PT_REDEEM books realized_yield_usd" if ok else "realized_yield_usd null/missing",
            )
        )

    # PEN5 — lifecycle continuity: a PT buy seeds a PENDLE_PT OPEN and a
    # sell/redeem a CLOSE on the SAME position_id, so the dashboard renders one
    # position. (position_events ↔ accounting_events join key is byte-identical,
    # asserted in the connector/observability unit suites.)
    pt_pos = [r for r in pos_events if r.get("position_type") == "PENDLE_PT"]
    opens = [r for r in pt_pos if r.get("event_type") == "OPEN"]
    closes = [r for r in pt_pos if r.get("event_type") == "CLOSE"]
    has_open = bool(opens)
    has_close = bool(closes)
    same_id = bool(
        has_open and has_close and {r.get("position_id") for r in opens} & {r.get("position_id") for r in closes}
    )
    out.append(
        CellResult(
            "PEN5",
            "Position lifecycle continuity (PENDLE_PT OPEN→CLOSE, one position)",
            "PASS" if same_id else "XFAIL",
            f"OPEN={has_open} CLOSE={has_close} shared_position_id={same_id}",
        )
    )

    # PEN6 — PT-quantity conservation: the principal acquired must equal the
    # principal disposed (sold + redeemed) within tolerance — the conservation
    # invariant the lane-symmetry contract rests on. Full money-leg (USD) lane
    # symmetry is the PrimitiveMoneyLegs work (G-PT4); this is the unit-level
    # conservation it presupposes, checkable from the existing payloads.
    def _sum_pt(rows: list[dict[str, Any]]) -> tuple[Decimal, bool]:
        total = Decimal(0)
        seen = False
        for r in rows:
            v = acct_payloads.get(r.get("id"), {}).get("pt_amount")
            if v is None:
                continue
            try:
                total += Decimal(str(v))
                seen = True
            except (ArithmeticError, ValueError):
                continue
        return total, seen

    bought, have_buy = _sum_pt(pt_buys)
    disposed, have_disp = _sum_pt(pt_disposals)
    if have_buy and have_disp and bought > 0:
        rel = abs(bought - disposed) / bought
        pen6_pass = rel <= Decimal("0.01")
        pen6_detail = f"PT bought={bought} disposed={disposed} rel_diff={rel:.6f}"
    else:
        pen6_pass = False
        pen6_detail = f"incomplete round-trip (have_buy={have_buy} have_disposal={have_disp})"
    out.append(
        CellResult(
            "PEN6",
            "PT-quantity conservation (principal acquired == disposed)",
            "PASS" if pen6_pass else "XFAIL",
            pen6_detail,
        )
    )
    return out


def _cells_pendle_lp(
    acct_events: list[dict[str, Any]],
    pos_events: list[dict[str, Any]],
    acct_payloads: dict[Any, dict[str, Any]],
    payload_errors: dict[Any, str],
) -> list[CellResult]:
    """PLP1–PLP6 — the Pendle LP scorecard (VIB-5320 PR A).

    Pendle LP is a *fungible* LP surface: a single-sided deposit (wstETH ->
    SY) mints an LP token whose contract is the market address (no NFT, no
    tick range). It rides the LP primitive in the taxonomy
    (``taxonomy.py`` ``PENDLE_LP_OPEN/CLOSE -> Primitive.LP``) but its typed
    events are ``PendleAccountingEvent`` (``handle_pendle_lp``), NOT the generic
    ``LPOpenEventPayload`` / ``LPCloseEventPayload`` — so the generic LP cell
    pack (``_cells_lp``, which asserts ticks / fees_token0-1 / IL / Track-C)
    would FAIL on data Pendle LP never emits. This pack scores only what Pendle
    LP actually books off the ``PENDLE_LP_OPEN`` / ``PENDLE_LP_CLOSE`` payload
    (``sy_amount`` / ``pt_amount``, both human-units, ``confidence=ESTIMATED``).

    This pins the CURRENT HONEST FLOOR, not a clean-green target. Pendle LP
    events are USD-less by design today (limitation B1), unpriced by the
    portfolio valuer (limitation B2 — same root cause as ``pendle_pt`` G6=FAIL),
    and out of the v1 payload-validation surface (limitation B3 —
    ``PENDLE_LP_OPEN/CLOSE`` absent from ``_PAYLOAD_MODELS``, so they decode but
    are not Pydantic-validated; ``payload_errors`` is empty for them). PLP3/PLP4
    are XFAIL because the capability is absent, not wrong; the USD-pricing fix
    (VIB-5276 gateway PT price + valuer wiring) is a separate follow-up PR.
    """
    out: list[CellResult] = []
    lp_opens = [r for r in acct_events if r.get("event_type") == "PENDLE_LP_OPEN"]
    lp_closes = [r for r in acct_events if r.get("event_type") == "PENDLE_LP_CLOSE"]

    # PLP1 — open books both legs: a Pendle LP_OPEN books sy_amount (SY supplied)
    # AND pt_amount (PT supplied), the two quantity inputs every downstream PnL
    # number needs. (B3: these rows are out of the v1 surface, so _payload_block
    # returns None and we read the decoded payload directly — but we still call
    # it so a future v1 wiring of PENDLE_LP would surface validation errors here.)
    blocked = _payload_block_cell("PLP1", "LP_OPEN books both legs (sy_amount + pt_amount)", lp_opens, payload_errors)
    if blocked is not None:
        out.append(blocked)
    else:
        ok = False
        for r in lp_opens:
            p = acct_payloads.get(r.get("id"), {})
            if p.get("sy_amount") is not None and p.get("pt_amount") is not None:
                ok = True
                break
        out.append(
            CellResult(
                "PLP1",
                "LP_OPEN books both legs (sy_amount + pt_amount)",
                "PASS" if ok else "XFAIL",
                "PENDLE_LP_OPEN books sy_amount + pt_amount"
                if ok
                else "no PENDLE_LP_OPEN carrying sy_amount + pt_amount",
            )
        )

    # PLP2 — close books the collected legs: a Pendle LP_CLOSE books sy_amount
    # AND pt_amount drained on burn (amount0_collected / amount1_collected scaled
    # into human units by handle_pendle_lp). These are the realized proceeds the
    # conservation invariant (PLP6) rests on.
    blocked = _payload_block_cell(
        "PLP2", "LP_CLOSE books collected legs (sy_amount + pt_amount)", lp_closes, payload_errors
    )
    if blocked is not None:
        out.append(blocked)
    else:
        ok = False
        for r in lp_closes:
            p = acct_payloads.get(r.get("id"), {})
            if p.get("sy_amount") is not None and p.get("pt_amount") is not None:
                ok = True
                break
        out.append(
            CellResult(
                "PLP2",
                "LP_CLOSE books collected legs (sy_amount + pt_amount)",
                "PASS" if ok else "XFAIL",
                "PENDLE_LP_CLOSE books sy_amount + pt_amount"
                if ok
                else "no PENDLE_LP_CLOSE carrying sy_amount + pt_amount",
            )
        )

    # PLP3 — open-LP mark-to-market. Requires the portfolio valuer to price an
    # open Pendle LP (value_pendle_lp / value_principal_token_lp_from_components),
    # which is NOT wired into portfolio_valuer.py (limitation B2) — so an open
    # Pendle LP marks to ~0 and carries no cost_basis_usd / unrealized_pnl_usd.
    # XFAIL, not FAIL: the capability is absent, not wrong. Unblocked by the
    # VIB-5276 gateway PT-price path + valuer wiring (separate follow-up PR).
    out.append(
        CellResult(
            "PLP3",
            "Open-LP mark-to-market (cost basis + unrealised PnL)",
            "XFAIL",
            "Pendle-LP valuer unwired (B2: value_pendle_lp not in portfolio_valuer; VIB-5276 gateway PT price)",
        )
    )

    # PLP4 — realised PnL / fees in USD on close. Pendle LP events are USD-less
    # by design today (limitation B1: pt_price=None, realized_yield_usd=None, no
    # *_usd fields on the LP payload model), so a close cannot attribute realised
    # PnL or fees in USD. XFAIL until the USD-pricing follow-up lands.
    out.append(
        CellResult(
            "PLP4",
            "Realised PnL / fees in USD on close",
            "XFAIL",
            "Pendle LP events USD-less by design (B1: realized_yield_usd / *_usd absent on LP payload)",
        )
    )

    # PLP5 — lifecycle continuity: an LP_OPEN seeds an LP position_events OPEN and
    # an LP_CLOSE a CLOSE on the SAME position_id, so the dashboard renders one
    # position. NOTE Pendle LP's conftest position-key special-case yields an
    # EMPTY position_key / market_id on the accounting event (a real contract
    # divergence vs Uniswap V3 LP — see tests/intents/arbitrum/test_pendle_lp.py),
    # so the join key lives on position_events, not the accounting payload. We
    # assert what is actually true: if position_events carry a shared OPEN/CLOSE
    # position_id, PASS; if Pendle LP does not seed position_events with a usable
    # shared id (the present floor), XFAIL with the observed shape.
    lp_pos = [r for r in pos_events if r.get("position_type") in ("LP", "PENDLE_LP")]
    opens = [r for r in lp_pos if r.get("event_type") == "OPEN"]
    closes = [r for r in lp_pos if r.get("event_type") == "CLOSE"]
    has_open = bool(opens)
    has_close = bool(closes)
    # Filter falsy/empty position_ids so {None}&{None} (or {""}&{""}) cannot
    # spuriously read as a shared id (CodeRabbit/Gemini).
    same_id = bool(
        has_open
        and has_close
        and {r.get("position_id") for r in opens if r.get("position_id")}
        & {r.get("position_id") for r in closes if r.get("position_id")}
    )
    out.append(
        CellResult(
            "PLP5",
            "Position lifecycle continuity (LP OPEN->CLOSE, one position)",
            "PASS" if same_id else "XFAIL",
            f"OPEN={has_open} CLOSE={has_close} shared_position_id={same_id}",
        )
    )

    # PLP6 — quantity conservation: the SY+PT supplied on open must equal the
    # SY+PT drained on close within tolerance (single round-trip, no MEV on a
    # fork). This is the unit-level conservation the lane-symmetry contract rests
    # on; the USD lane symmetry is deferred to the B1/B2 USD-pricing follow-up.
    def _sum_legs(rows: list[dict[str, Any]]) -> tuple[Decimal, bool]:
        total = Decimal(0)
        seen = False
        for r in rows:
            p = acct_payloads.get(r.get("id"), {})
            for field_name in ("sy_amount", "pt_amount"):
                v = p.get(field_name)
                if v is None:
                    continue
                try:
                    total += Decimal(str(v))
                    seen = True
                except (ArithmeticError, ValueError):
                    continue
        return total, seen

    supplied, have_open = _sum_legs(lp_opens)
    drained, have_close = _sum_legs(lp_closes)
    if have_open and have_close and supplied > 0:
        rel = abs(supplied - drained) / supplied
        plp6_pass = rel <= Decimal("0.01")
        plp6_detail = f"SY+PT supplied={supplied} drained={drained} rel_diff={rel:.6f}"
    else:
        plp6_pass = False
        plp6_detail = f"incomplete round-trip (have_open={have_open} have_close={have_close})"
    out.append(
        CellResult(
            "PLP6",
            "Quantity conservation (SY+PT supplied == drained)",
            "PASS" if plp6_pass else "XFAIL",
            plp6_detail,
        )
    )
    return out


# ─── Scorecard profile registry (G-A foundation) ─────────────────────────
#
# One declarative table replaces the former per-primitive if/elif ladders (the
# lifecycle map, the G6 ε selector, and the cell-pack dispatch). Each profile
# carries its canonical taxonomy ``Primitive`` (Blueprint 27 §2.4) and an adapter
# that calls the existing cell pack with its current signature — so adding a
# primitive is one entry, not three new branches. Assembled here (not in
# ``scorecard_profiles.py``) because the cell-pack callables live in this module;
# the dataclass tier is neutral so there is no import cycle (§2.1 layering).
#
# ``_TaxonomyPrimitive`` is the canonical enum (imported as such because the
# module-local ``Primitive`` name is the ``ProfileName`` string alias).
SCORECARD_PROFILES: dict[str, ScorecardProfile] = {
    "lp": ScorecardProfile(
        name="lp",
        canonical_primitive=_TaxonomyPrimitive.LP,
        required_lifecycle=("LP_OPEN", "LP_CLOSE"),
        eps_pct=Decimal("0.0025"),
        eps_scaling=lambda b: (b.notional_traded, "notional_traded"),
        cells=lambda ctx: _cells_lp(
            ctx.pos_events,
            ctx.acct_events,
            ctx.snapshots,
            ctx.acct_payloads,
            ctx.payload_errors,
            ctx.position_state_rows,
        ),
    ),
    "looping": ScorecardProfile(
        name="looping",
        canonical_primitive=_TaxonomyPrimitive.LENDING,
        required_lifecycle=("SUPPLY", "BORROW", "REPAY", "WITHDRAW"),
        eps_pct=Decimal("0.0010"),
        eps_scaling=lambda b: (
            max(b.notional_traded, b.max_debt),
            "max(notional_traded, max_debt_outstanding)",
        ),
        cells=lambda ctx: _cells_lending(
            ctx.acct_events,
            ctx.snapshots,
            ctx.acct_payloads,
            ctx.payload_errors,
            ctx.position_state_rows,
        ),
    ),
    "perp": ScorecardProfile(
        name="perp",
        canonical_primitive=_TaxonomyPrimitive.PERP,
        required_lifecycle=("PERP_OPEN", "PERP_CLOSE"),
        eps_pct=Decimal("0.0005"),
        eps_scaling=lambda b: (b.max_perp_notional, "max_perp_notional"),
        cells=lambda ctx: _cells_perp(
            ctx.acct_events,
            ctx.pos_events,
            ctx.acct_payloads,
            ctx.payload_errors,
        ),
    ),
    # Pendle PT rides the SWAP primitive in the taxonomy (taxonomy.py
    # ``PENDLE_PT → Primitive.SWAP``): a PT buy/sell IS a swap, and both legs land
    # as ``SWAP`` intent_type in the ledger — so the canonical lifecycle is SWAP's
    # (atomic, empty). The buy→sell round-trip is asserted by the PEN cell pack
    # (PEN1/PEN4/PEN5), not the coarse intent_type lifecycle guard, which cannot
    # tell a PT buy from a PT sell (both are ``SWAP``).
    "pendle_pt": ScorecardProfile(
        name="pendle_pt",
        canonical_primitive=_TaxonomyPrimitive.SWAP,
        required_lifecycle=(),
        eps_pct=Decimal("0.0025"),
        eps_scaling=lambda b: (b.notional_traded, "notional_traded"),
        cells=lambda ctx: _cells_pendle_pt(
            ctx.acct_events,
            ctx.pos_events,
            ctx.acct_payloads,
            ctx.payload_errors,
        ),
    ),
    # Pendle LP rides the LP primitive in the taxonomy (taxonomy.py
    # ``PENDLE_LP_OPEN/CLOSE -> Primitive.LP`` with ``_LP_LIFECYCLE``): the
    # ledger intent_type IS ``LP_OPEN`` / ``LP_CLOSE``, so the canonical
    # lifecycle is LP's and the FixtureLifecycleError guard verifies the
    # round-trip from the ledger. The cell pack scores the *Pendle* economics
    # off the PendleAccountingEvent payload (sy_amount/pt_amount), NOT the
    # generic LP shape (ticks/IL/Track-C) which Pendle LP never emits — using
    # ``_cells_lp`` here would FAIL, not XFAIL. LP's ε (0.0025 on
    # notional_traded) is reused. VIB-5320 PR A.
    "pendle_lp": ScorecardProfile(
        name="pendle_lp",
        canonical_primitive=_TaxonomyPrimitive.LP,
        required_lifecycle=("LP_OPEN", "LP_CLOSE"),
        eps_pct=Decimal("0.0025"),
        eps_scaling=lambda b: (b.notional_traded, "notional_traded"),
        cells=lambda ctx: _cells_pendle_lp(
            ctx.acct_events,
            ctx.pos_events,
            ctx.acct_payloads,
            ctx.payload_errors,
        ),
    ),
}


def _profile_for(name: str) -> ScorecardProfile:
    """Resolve a scorecard profile by its string key, failing loud on an unknown
    profile.

    The former G6 ``else`` branch silently scored an unknown primitive with
    perp's ε — a latent mis-scoring. Every caller is constrained to the
    registered keys (the ratchet tuple, regression-assert ``choices``, the matrix
    runner, the CLI, and ``accountant_query``'s documented contract), so raising
    here changes no real output and turns a typo into a clear error instead of a
    wrong number.
    """
    try:
        return SCORECARD_PROFILES[name]
    except KeyError:
        raise ValueError(f"unknown scorecard profile {name!r}; known: {sorted(SCORECARD_PROFILES)}") from None


# ─── Top-level runner ────────────────────────────────────────────────────


def evaluate_cells(
    *,
    ledger: list[dict[str, Any]],
    pos_events: list[dict[str, Any]],
    acct_events: list[dict[str, Any]],
    snapshots: list[dict[str, Any]],
    metrics: list[dict[str, Any]],
    position_state_rows: list[dict[str, Any]],
    primitive: Primitive,
    db_dump_path: str | None = None,
    # VIB-4201 (T15): cell #22 inputs. Defaults preserve back-compat for
    # callers that pre-date cell #22 — they get an XFAIL on cell #22 with
    # a "preflight not run" diagnostic rather than a crash. Production
    # callers (run_against_sqlite, accountant_query) supply real values.
    position_registry_rows: list[dict[str, Any]] | None = None,
    position_reference_column_present: bool | None = None,
    position_registry_table_present: bool | None = None,
    malformed_position_reference_row_ids: list[Any] | None = None,
) -> AccountantReport:
    """Evaluate the cell matrix against pre-fetched rows.

    Decoupled from sqlite I/O so callers like the filtered reporting API
    (VIB-3870) can pass pre-filtered rows (by deployment_id, cycle_ids, time
    window, …) without rewriting the cell predicates.

    Sorts the input lists in-place by timestamp / iteration_number — cells
    assume time-ordered rows for running aggregations (see the BORROW →
    REPAY tracker in G6).
    """
    snapshots.sort(key=lambda r: (r.get("iteration_number") or 0, r.get("timestamp") or ""))
    ledger.sort(key=lambda r: r.get("timestamp") or "")
    pos_events.sort(key=lambda r: r.get("timestamp") or "")
    acct_events.sort(key=lambda r: r.get("timestamp") or "")

    deployment_id = ""
    network = ""
    if metrics:
        deployment_id = metrics[0].get("deployment_id") or ""
    if ledger:
        network = ledger[0].get("chain") or ""

    # VIB-3868: typed payload reads. Every cell that reads a payload field
    # goes through this validated map; rows whose payload failed Pydantic
    # validation are surfaced via ``payload_errors`` and FAIL the cells
    # downstream of them.
    acct_payloads, payload_errors, payload_error_records = _typed_acct_payloads(acct_events)

    cells: list[CellResult] = []
    cells.append(_cell_g1_money_trail(ledger, acct_events, acct_payloads))
    cells.append(_cell_g2_cost_ledger(ledger))
    cells.append(_cell_g3_yield_ledger(pos_events, acct_events))
    cells.append(_cell_g4_capital_deployed(snapshots))
    cells.append(_cell_g5_initial_vs_current(metrics, snapshots))
    g6, decomp = _cell_g6_reconciliation(
        snapshots, ledger, pos_events, acct_events, primitive, acct_payloads, payload_errors
    )
    cells.append(g6)
    cells.append(_cell_g7_attribution(ledger, pos_events, acct_events))
    cells.append(_cell_g8_time_series(snapshots))
    cells.append(_cell_g9_confidence(snapshots, acct_events))
    cells.append(_cell_g10_multi_tx_atomicity(ledger, pos_events, acct_events))
    cells.append(_cell_g11_failed_intents(ledger))
    cells.append(_cell_g12_oracle_consistency(ledger))
    cells.append(_cell_g13_lot_matching(ledger, acct_events, acct_payloads, payload_errors))
    cells.append(_cell_g14_sdk_eq_onchain(snapshots, position_state_rows))
    cells.append(_cell_g15_multi_period_self_consistency(snapshots, position_state_rows))

    # Primitive-specific cell pack (G-A): one registry lookup replaces the
    # former if/elif ladder. An unrecognised profile extends nothing — exactly
    # the old fall-through (there was no ``else`` branch).
    _profile = SCORECARD_PROFILES.get(primitive)
    if _profile is not None:
        cells.extend(
            _profile.cells(
                ScorecardCtx(
                    pos_events=pos_events,
                    acct_events=acct_events,
                    snapshots=snapshots,
                    acct_payloads=acct_payloads,
                    payload_errors=payload_errors,
                    position_state_rows=position_state_rows,
                )
            )
        )

    # VIB-4201 (T15): cell #22 — registry coherence. Appended after the
    # 15 generic + 6 primitive-specific cells. NOT in the ≥16/21 gating
    # sum (see ``format_markdown``); informational on every primitive.
    if position_reference_column_present is None:
        # Caller did not run preflight (pre-T15 caller, or back-compat
        # path). Mark as XFAIL with an explicit "preflight not run"
        # diagnostic rather than crashing. New production callers
        # (``run_against_sqlite``) always provide the flags.
        cells.append(
            CellResult(
                "L5_22",
                "Registry coherence (accounting_events ↔ position_registry, bidirectional)",
                "XFAIL",
                "cell #22 preflight not run (caller supplied no registry inputs); cell cannot evaluate",
            )
        )
    else:
        cells.append(
            _cell22_registry_coherence(
                acct_events,
                position_registry_rows or [],
                position_reference_column_present=position_reference_column_present,
                position_registry_table_present=bool(position_registry_table_present),
                malformed_position_reference_row_ids=list(malformed_position_reference_row_ids or []),
            )
        )

    # Track which cells flipped to FAIL specifically because of payload
    # validation drift. Lets reviewers diff cell-status changes between
    # runs without re-deriving propagation by hand.
    cells_blocked: list[str] = []
    if payload_errors:
        for c in cells:
            if (
                c.status == "FAIL"
                and "payload" in c.diagnostic.lower()
                and ("validation" in c.diagnostic.lower() or "pydantic" in c.diagnostic.lower())
            ):
                cells_blocked.append(c.cell_id)

    footprint = [
        {
            "tx_hash": r.get("tx_hash"),
            "intent_type": r.get("intent_type"),
            "chain": r.get("chain"),
            "gas_used": r.get("gas_used"),
            "success": bool(r.get("success")),
        }
        for r in ledger
    ]

    return AccountantReport(
        primitive=primitive,
        network=network,
        deployment_id=deployment_id,
        cells=cells,
        on_chain_footprint=footprint,
        g6_decomposition=decomp,
        db_dump_path=db_dump_path,
        payload_validation_errors=payload_error_records,
        cells_blocked_by_payload_errors=cells_blocked,
    )


def run_against_sqlite(
    db_path: str | Path,
    *,
    primitive: Primitive,
    strict_lifecycle: bool = False,
    deployment_id: str | None = None,
) -> AccountantReport:
    """Run the Accountant Test against a SQLite DB file.

    Thin shim around :func:`evaluate_cells` — fetches the canonical row
    set from the strategy's local DB. For filtered queries (by date,
    cycle, deployment, …) use
    :func:`almanak.framework.accounting.reporting.accountant_query.accountant_report_from_db`
    instead.

    VIB-4162 (T2): when ``strict_lifecycle=True``, the harness asserts the
    fixture's ``transaction_ledger`` exercises every canonical lifecycle
    step for the chosen primitive (LP / Looping / Perp). Missing steps
    raise :class:`FixtureLifecycleError` BEFORE any cell is evaluated, so
    a half-built fixture cannot produce a partial-pass report. The default
    is ``False`` to preserve back-compat for production callers (running
    against real DBs that may exercise only part of a lifecycle); the
    Accountant Test test-suite (``test_accountant_test_baseline.py``)
    opts in.

    VIB-4540: when ``deployment_id`` is supplied, every row read is
    scoped to that deployment. When unspecified, the helper auto-picks
    the singleton if exactly one deployment is present (preserves the
    matrix-runner contract — every fixture DB is single-deployment) and
    raises :class:`MultipleDeploymentsError` otherwise. Silent contamination
    across deployments was the original bug; auto-picking "first" or
    "latest" would just hide it.
    """
    conn = _connect(db_path)
    try:
        # VIB-4540 (audit PR #2343): resolve deployment_id BEFORE the
        # lifecycle check so strict mode evaluates the same scoped row
        # set as the cells, and validate an explicit id so a typo
        # surfaces as a config error instead of an empty-filter FAIL.
        if deployment_id is None:
            deployment_id = _resolve_singleton_deployment_id(conn)
        elif not _deployment_exists(conn, deployment_id):
            raise ValueError(
                f"Unknown deployment_id: {deployment_id!r}. "
                "No rows for this id were found in any of the canonical "
                "accounting tables; check for a typo or pass --deployment-id "
                "with one of the candidates surfaced by MultipleDeploymentsError."
            )
        if strict_lifecycle:
            _assert_fixture_lifecycle(conn, primitive, deployment_id=deployment_id)
        ledger = _table_rows(conn, "transaction_ledger", deployment_id=deployment_id)
        pos_events = _table_rows(conn, "position_events", deployment_id=deployment_id)
        acct_events = _table_rows(conn, "accounting_events", deployment_id=deployment_id)
        snapshots = _table_rows(conn, "portfolio_snapshots", deployment_id=deployment_id)
        metrics = _table_rows(conn, "portfolio_metrics", deployment_id=deployment_id)
        # Track C surface — empty list when the materializer hasn't been
        # wired (current state on this branch). Both G14 and G15 stay
        # XFAIL in that case by design.
        position_state_rows = _table_rows(conn, "position_state_snapshots", deployment_id=deployment_id)
        # VIB-4201 (T15): cell #22 preflight + reads.
        position_registry_rows = _table_rows(conn, "position_registry", deployment_id=deployment_id)
        (
            position_reference_column_present,
            position_registry_table_present,
            malformed_position_reference_row_ids,
        ) = _cell22_preflight(conn, deployment_id=deployment_id)
    finally:
        conn.close()
    return evaluate_cells(
        ledger=ledger,
        pos_events=pos_events,
        acct_events=acct_events,
        snapshots=snapshots,
        metrics=metrics,
        position_state_rows=position_state_rows,
        primitive=primitive,
        db_dump_path=str(db_path),
        position_registry_rows=position_registry_rows,
        position_reference_column_present=position_reference_column_present,
        position_registry_table_present=position_registry_table_present,
        malformed_position_reference_row_ids=malformed_position_reference_row_ids,
    )


def _cell22_preflight(
    conn: sqlite3.Connection,
    *,
    deployment_id: str | None = None,
) -> tuple[bool, bool, list[Any]]:
    """Run the cell #22 preflight checks against an open SQLite connection.

    Returns ``(position_reference_column_present, position_registry_table_present,
    malformed_position_reference_row_ids)`` — see the UAT card §4 D1
    Preflight for the contract. Each query is wrapped in its own
    ``try/except`` so a missing table / column fails to ``False`` / ``[]``
    rather than raising into the caller; the cell predicate's branches
    interpret the flags into PASS / FAIL / XFAIL verdicts.

    VIB-4540 (audit PR #2343): when ``deployment_id`` is supplied, the
    malformed-JSON scan is scoped to that deployment. Without scoping, a
    bad row in an older/unrelated deployment would cause L5_22 to FAIL
    for an otherwise clean target deployment — re-introducing the cross-
    deployment contamination this fix is supposed to prevent. P1 (column
    existence) and P2 (table existence) are schema-level checks that
    apply DB-wide and don't need scoping.
    """
    # P1: position_reference column exists?
    try:
        cur = conn.execute(
            "SELECT count(*) FROM pragma_table_info('accounting_events') WHERE name = 'position_reference'"
        )
        position_reference_column_present = (cur.fetchone()[0] or 0) > 0
    except sqlite3.OperationalError:
        position_reference_column_present = False

    # P2: position_registry table exists?
    try:
        cur = conn.execute("SELECT count(*) FROM sqlite_master WHERE type='table' AND name='position_registry'")
        position_registry_table_present = (cur.fetchone()[0] or 0) > 0
    except sqlite3.OperationalError:
        position_registry_table_present = False

    # P3: malformed position_reference JSON? Skip if column missing.
    malformed_ids: list[Any] = []
    if position_reference_column_present:
        try:
            if deployment_id is None:
                cur = conn.execute(
                    "SELECT id FROM accounting_events "
                    "WHERE position_reference IS NOT NULL AND json_valid(position_reference) = 0"
                )
            else:
                cur = conn.execute(
                    "SELECT id FROM accounting_events "
                    "WHERE deployment_id = ? "
                    "AND position_reference IS NOT NULL AND json_valid(position_reference) = 0",
                    (deployment_id,),
                )
            malformed_ids = [row[0] for row in cur.fetchall()]
        except sqlite3.OperationalError:
            # ``json_valid`` is missing on ancient SQLite builds (<3.38;
            # Python 3.10+ bundles 3.40+, so this branch only fires on
            # exotic system-SQLite installs). The Python-side orphan
            # walker (``_cell22_position_reference_phid``) handles
            # malformed JSON safely by returning ``None``, so a corrupt
            # row collapses into the "legacy null hash" census bucket.
            # That's a degraded F10 surface — corrupt payloads no longer
            # produce a loud FAIL — but the cell remains crash-free.
            # Track in VIB-4201 follow-up if the Python target ever
            # regresses to <3.10.
            malformed_ids = []

    return position_reference_column_present, position_registry_table_present, malformed_ids


__all__ = [
    "SCORECARD_PROFILES",
    "AccountantReport",
    "CellResult",
    "FixtureLifecycleError",
    "MultipleDeploymentsError",
    "Primitive",
    "evaluate_cells",
    "run_against_sqlite",
]
