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
import logging
import sqlite3
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Literal

from almanak.framework.accounting.gas_pricing import native_token_for_chain
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
from almanak.framework.accounting.receipt_set import evaluate_landed_receipt_sets
from almanak.framework.accounting.scorecard_profiles import (
    G6Bases,
    ScorecardCtx,
    ScorecardProfile,
)
from almanak.framework.observability.position_events import (
    PositionEventType,
    PositionType,
)
from almanak.framework.primitives.taxonomy import (
    _SETTLEMENT_LIFECYCLE,
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
from almanak.framework.valuation.net_debt import (
    net_debt_from_positions_json,
    parse_positions_payload,
    read_position_decimal,
    wallet_nav_usd,
)

logger = logging.getLogger(__name__)

# Derived from the taxonomy so registry-coherence checks cover every close event.
# Keep the corresponding SQL CTE in lockstep; a unit test enforces this.
CLOSE_EVENT_TYPES: tuple[str, ...] = tuple(
    sorted(intent for intent, rec in TAXONOMY.items() if rec.event_kind == EventKind.CLOSE)
)

# Profile keys are a stable external contract and are distinct from taxonomy primitives;
# for example, ``looping`` is a lending scorecard with no primitive enum twin.
ProfileName = Literal[
    "spot",
    "lp",
    "looping",
    "lending_lifecycle",
    "perp",
    "pendle_pt",
    "pendle_lp",
    "curve_lp",
    "settlement",
]
Primitive = ProfileName
CellStatus = Literal["PASS", "FAIL", "XFAIL", "SKIP"]


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
        # Atomic primitive round trips are enforced by their cell packs.
        return
    # Lifecycle records chain-landed steps even when their accounting verdict is degraded;
    # otherwise the audit would abort before cells can report the accounting defect.
    if deployment_id is None:
        cur = conn.execute(
            f"SELECT DISTINCT intent_type FROM transaction_ledger WHERE {_landed_sql()}",
            dict(_LANDED_PARAMS),
        )
    else:
        cur = conn.execute(
            "SELECT DISTINCT intent_type FROM transaction_ledger "
            f"WHERE {_landed_sql()} AND deployment_id = :deployment_id",
            {"deployment_id": deployment_id, **_LANDED_PARAMS},
        )
    actual = {row[0] for row in cur.fetchall() if row[0]}
    missing = expected - actual
    if missing:
        raise FixtureLifecycleError(
            f"primitive={primitive} fixture missing lifecycle steps: {sorted(missing)}; got: {sorted(actual)}"
        )


# These cells remain visible and strict but are excluded from the historical
# 21-cell denominator so scores remain comparable across runs.
_INFORMATIONAL_CELL_IDS = frozenset({"L5_22", "G16", "G17"})


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
    payload_validation_errors: list[dict[str, Any]] = field(default_factory=list)
    cells_blocked_by_payload_errors: list[str] = field(default_factory=list)
    # N-leg findings are diagnostic-only and do not change cell status.
    nleg_invariant_findings: list[str] = field(default_factory=list)

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
            "nleg_invariant_findings": list(self.nleg_invariant_findings),
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
        generic = [c for c in self.cells if c.cell_id.startswith("G") and c.cell_id not in _INFORMATIONAL_CELL_IDS]
        prim = [c for c in self.cells if not c.cell_id.startswith("G") and c.cell_id not in _INFORMATIONAL_CELL_IDS]
        informational = [c for c in self.cells if c.cell_id in _INFORMATIONAL_CELL_IDS]

        def _score(rs: list[CellResult]) -> str:
            p = sum(1 for r in rs if r.status == "PASS")
            f = sum(1 for r in rs if r.status == "FAIL")
            x = sum(1 for r in rs if r.status == "XFAIL")
            s = sum(1 for r in rs if r.status == "SKIP")
            return f"{p} PASS, {f} FAIL, {x} XFAIL, {s} SKIP (of {len(rs)})"

        lines.append("## Score")
        lines.append(f"- Generic {len(generic)}: {_score(generic)}")
        lines.append(f"- Primitive {len(prim)}: {_score(prim)}")
        if informational:
            lines.append(f"- Informational {len(informational)}: {_score(informational)}")
        lines.append(f"- Total: {self.passed}/{self.total_cells} PASS, {self.failed} FAIL, {self.xfailed} XFAIL")
        # Informational cells render normally but do not change the historical denominator.
        gated_cells = [c for c in self.cells if c.cell_id not in _INFORMATIONAL_CELL_IDS]
        cell22 = next((c for c in self.cells if c.cell_id == "L5_22"), None)
        g16 = next((c for c in self.cells if c.cell_id == "G16"), None)
        g17 = next((c for c in self.cells if c.cell_id == "G17"), None)
        gated_pass = sum(1 for c in gated_cells if c.status == "PASS")
        cell22_status = cell22.status if cell22 is not None else "absent"
        g16_status = g16.status if g16 is not None else "absent"
        g17_status = g17.status if g17 is not None else "absent"
        lines.append(
            f"- Gating: {gated_pass}/{len(gated_cells)} PASS (≥16/21 required); "
            f"cell L5_22 informational only this cycle (status: {cell22_status}); "
            f"cell G16 informational only this cycle (status: {g16_status}); "
            f"cell G17 informational only this cycle (status: {g17_status})"
        )
        lines.append("")
        lines.append("## Cells")
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
        if self.nleg_invariant_findings:
            lines.append("## N-leg reconciliation diagnostics (VIB-5540)")
            lines.append("")
            for finding in self.nleg_invariant_findings:
                lines.append(f"- {finding}")
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
        # Missing tables or deployment columns represent an unavailable read surface.
        return []
    return [dict(r) for r in cur.fetchall()]


# These scans are O(rows); add an index or deployment table before using them on a hot path.
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
        value = Decimal(str(v))
    except (InvalidOperation, TypeError):
        return None
    # NaN is unmeasured; ordered Decimal comparisons against it raise.
    if value.is_nan():
        return None
    return value


def _snapshot_equity(s: dict[str, Any]) -> Decimal | None:
    """Strategy equity at a snapshot = ``total_value_usd − debt_mark +
    available_cash_usd`` (the canonical NAV, blueprint 27 §7.11).

    VIB-3614 split deployed (positions) from cash (uninvested wallet) into
    separate columns. The Senior DeFi Quant's equity curve / PnL view is the
    SUM of the two — but ``total_value_usd`` is Σ POSITIVE ``value_usd`` (the
    negative debt leg is DROPPED, VIB-3614), so for a leveraged position it is
    *gross collateral*, not equity. VIB-5857: summing it with cash overstated
    mid-run equity by the entire outstanding debt. The debt is re-netted here
    exactly once via the canonical read path (``net_debt_from_positions_json``
    + ``wallet_nav_usd`` — never a local re-derivation). Round-trip fixtures
    are unaffected (debt is zero at both endpoints and the bias cancelled);
    only a run with debt open at an endpoint moves, which is what the
    ``looping_debt_open`` fixture (VIB-6560) exists to measure.

    A post-teardown snapshot with ``total_value_usd=0`` is *not* a
    missing measurement — every position closed cleanly and equity collapsed
    into ``available_cash_usd``. Treating that as null double-counts a
    successful unwind as an accounting failure (G8 false positive seen on
    looping mainnet, 2026-05-01).

    An ``UNAVAILABLE`` row is the runner's diagnostic failure contract. Its
    persisted ``0 + 0`` placeholders are not measurements and must never enter
    G6's ``priced`` bracket as a zero-valued endpoint. Returns ``None`` for
    that confidence, when both columns are unmeasured, or when the row carries
    a measured debt leg while ``total_value_usd`` is unmeasured — a real
    liability with an unmeasured asset side is not an equity point, and
    coercing the missing deployed column to ``0`` would produce the
    wrong-signed ``0 − debt + cash`` (Empty ≠ Zero; ``wallet_nav_usd``'s
    contract forbids the caller coercing an unmeasured term). For every other
    shape the netting changes a row's VALUE but never its membership in the
    ``priced`` bracket. A measured pure-cash or pure-deployed snapshot remains
    a valid equity point.

    Historical rows: the netting subtracts the debt via
    :func:`_snapshot_debt_mark`, which excludes debt already counted inside a
    LEGACY net-shaped SUPPLY leg (pre-VIB-5857, no ``supply_leg_convention``
    marker) so those rows are branch-read, not reinterpreted.
    """
    if s.get("value_confidence") == "UNAVAILABLE":
        return None
    deployed = _dec(s.get("total_value_usd"))
    cash = _dec(s.get("available_cash_usd"))
    if deployed is None and cash is None:
        return None
    positions_json = s.get("positions_json")
    _count, raw_debt_mark, _debt_cost = net_debt_from_positions_json(positions_json)
    # Use raw debt here so legacy correction cannot turn an unmeasured asset side into zero.
    if deployed is None and raw_debt_mark != 0:
        return None
    debt_mark = _corrected_debt_mark(positions_json, raw_debt_mark)
    return wallet_nav_usd(deployed or Decimal("0"), debt_mark, cash or Decimal("0"))


def _snapshot_debt_mark(positions_json: Any) -> Decimal:
    """The debt a NAV consumer must subtract from this row's ``total_value_usd``.

    ``net_debt_from_positions_json`` gives the canonical Σ|negative legs|;
    :func:`_legacy_net_supply_debt` then excludes the portion a LEGACY
    net-shaped SUPPLY leg already netted internally — paired per reserve, so
    the correction can only ever cancel the same reserve's own BORROW legs.

    KNOWN LIMITATION (VIB-6703): an absent or unparsable ``positions_json``
    parses to ``[]`` and yields a MEASURED ``debt_mark == 0``, so a leveraged
    row that lost its legs scores gross-of-debt with no diagnostic. Kept
    deliberately — refusing such rows here would move them in and out of
    G5/G6's ``priced`` bracket on payload health; the follow-up surfaces a
    diagnostic count in the cell detail instead.
    """
    _count, debt_mark, _debt_cost = net_debt_from_positions_json(positions_json)
    return _corrected_debt_mark(positions_json, debt_mark)


def _corrected_debt_mark(positions_json: Any, raw_debt_mark: Decimal) -> Decimal:
    """Apply the legacy net-shaped-leg exclusion to a raw ``debt_mark``."""
    if raw_debt_mark == 0:
        return raw_debt_mark
    return max(Decimal("0"), raw_debt_mark - _legacy_net_supply_debt(positions_json))


def _missing_debt_leg_rows(snapshots: list[dict[str, Any]]) -> int:
    """Rows where a marked-gross SUPPLY leg declares reserve debt but NO
    same-reserve BORROW leg survives in the snapshot (VIB-6699 loudness).

    Under the gross convention (VIB-5857) the debt lives ONLY in the signed
    BORROW leg; blueprint 27 §7.11 names the structural precondition that the
    leg be present, and the partial-discovery failure that loses the debt side
    while the collateral side reprices would otherwise read as a silently
    plausible (overstated) equity. A marked-gross leg is self-describing —
    its enriched ``details.debt_value_usd`` says whether its reserve carries
    debt — so the inconsistency is detectable and counted here. Bounds, stated:
    account-state-protocol legs (no enriched details, VIB-6701) and legacy
    unmarked legs cannot be checked this way; and this is diagnostic-only for
    now (promotion to a verdict input follows the same corpus discipline as
    ``_unreadable_payload_rows``).
    """
    n = 0
    for s in snapshots:
        legs = parse_positions_payload(s.get("positions_json"))
        if not legs:
            continue
        borrow_keys: set[tuple[str, str, str]] = set()
        for leg in legs:
            v = read_position_decimal(leg, "value_usd")
            if v is None or v >= 0:
                continue
            details = leg.get("details") if isinstance(leg, dict) else getattr(leg, "details", None)
            key = _leg_reserve_key(leg, details if isinstance(details, dict) else None)
            if key is not None:
                borrow_keys.add(key)
        for leg in legs:
            details = leg.get("details") if isinstance(leg, dict) else getattr(leg, "details", None)
            if not isinstance(details, dict):
                continue
            if details.get("supply_leg_convention") != "gross":
                continue
            debt = _dec(details.get("debt_value_usd"))
            if debt is None or debt <= 0:
                continue
            v = read_position_decimal(leg, "value_usd")
            if v is None or v <= 0:
                continue
            key = _leg_reserve_key(leg, details)
            if key is None or key not in borrow_keys:
                n += 1
                break
    return n


def _unreadable_payload_rows(snapshots: list[dict[str, Any]]) -> int:
    """Rows whose deployed column claims positive value while ``positions_json``
    yields no legs (VIB-6703 diagnostic).

    ``total_value_usd`` is Σ positive leg ``value_usd`` (VIB-3614), so a
    measured-positive total beside an absent / unparsable / empty payload is
    incoherent by construction — and on a leveraged row it silently scores
    gross-of-debt because ``debt_mark`` reads the (lost) legs. This count makes
    that shape LOUD in the G4/G6 output instead of indistinguishable from a
    genuinely debt-free row. Diagnostic-only for now: promoting it to a verdict
    input requires sweeping the historical corpus for legacy position-blind
    rows first (the plain ``looping`` fixture is one), which is a corpus
    question tracked on VIB-6703 — the same promotion discipline the perp-fee
    unmeasured counters follow.
    """
    n = 0
    for s in snapshots:
        deployed = _dec(s.get("total_value_usd"))
        if deployed is None or deployed <= 0:
            continue
        if not parse_positions_payload(s.get("positions_json")):
            n += 1
    return n


def _leg_reserve_key(leg: Any, details: dict[str, Any] | None) -> tuple[str, str, str] | None:
    """Reserve identity for pairing a legacy SUPPLY leg with its BORROW
    sibling: (protocol, chain, asset), case-folded. FAILS CLOSED: ``None``
    when any component is missing/empty — an unidentifiable reserve must
    neither feed the sibling pool nor receive a correction, because two
    reserves collapsing into one empty-keyed bucket would let one reserve's
    correction un-net another's real debt (NAV overstatement). The enriched
    writer stamps protocol/chain unconditionally but ``details.asset`` only
    when a symbol resolved, so the missing-asset shape is producible; an
    unpairable legacy leg then simply stays double-subtracted (understated —
    the safe direction, same as the stripped-details detection bound)."""

    def _read(obj: Any, key: str) -> str:
        raw = obj.get(key) if isinstance(obj, dict) else getattr(obj, key, None)
        return str(raw).lower() if raw is not None else ""

    asset = _read(details, "asset") if details is not None else ""
    key = (_read(leg, "protocol"), _read(leg, "chain"), asset)
    if not all(key):
        return None
    return key


def _legacy_net_supply_debt(positions_json: Any) -> Decimal:
    """Debt double-counted between a LEGACY net-shaped SUPPLY leg and its
    same-reserve BORROW sibling (VIB-5857).

    Before VIB-5857 the valuer's single-reserve enriched path persisted
    ``value_usd = net_value_usd`` on a SUPPLY leg while a signed BORROW leg
    still carried the same reserve's debt — so subtracting the full
    ``debt_mark`` from a row written by that code double-subtracts. Those legs
    are identified by the exact shape only that writer produced: enriched
    ``details`` carrying ``net_value_usd`` and a strictly positive
    ``debt_value_usd``, NO ``supply_leg_convention`` marker, and
    ``value_usd == net_value_usd`` on the positive side. The equality cannot
    hold coincidentally under the gross convention (gross == net only when
    the reserve debt is zero, which the ``debt > 0`` guard excludes), and
    account-state-protocol legs (morpho_blue / benqi / compound_v3 — VIB-6701)
    never carry ``net_value_usd``/``debt_value_usd`` keys, so they are never
    flagged and their (gross, strategy-reported) values keep netting normally.

    **Paired per reserve, never global**: the exclusion for each legacy leg is
    ``min(details.debt_value_usd, remaining same-reserve BORROW pool)``. The
    double-count exists only where a BORROW sibling of the SAME reserve is
    actually in ``debt_mark``; a legacy net leg with no sibling (its row's
    total is already net for that reserve, debt_mark carries nothing to
    cancel) contributes zero, so the correction can never un-net a DIFFERENT
    reserve's real debt — that would overstate NAV, the dangerous direction.
    The pool contains BORROW legs only and is CONSUMED as legs claim from it
    (two legacy legs on one reserve cannot both cancel the same sibling), and
    reserve identity fails closed (:func:`_leg_reserve_key` returns ``None``
    on a missing component; unpairable legs are skipped on both sides and
    simply stay double-subtracted — understated, the safe direction).

    NaN discipline: any of the three money fields parsing to NaN disqualifies
    the leg (ordered comparisons on NaN raise ``InvalidOperation``, which
    would take the whole cell to CRASH rather than FAIL).

    Detection bound, stated rather than implied: a legacy net-shaped leg whose
    details were stripped of the enriched keys cannot be disambiguated and
    will still double-subtract; `test_looping_same_reserve_vib5857.py` pins
    both the correction firing and this bound with negative controls.
    """
    legs = parse_positions_payload(positions_json)

    def _leg_field(leg: Any, key: str) -> Any:
        return leg.get(key) if isinstance(leg, dict) else getattr(leg, key, None)

    # Only same-reserve BORROW debt may be cancelled, and each amount is consumed once.
    borrow_by_reserve: dict[tuple[str, str, str], Decimal] = {}
    for leg in legs:
        if str(_leg_field(leg, "position_type") or "").upper() != "BORROW":
            continue
        value = read_position_decimal(leg, "value_usd")
        if value is None or value.is_nan() or value >= 0:
            continue
        details = _leg_field(leg, "details")
        key = _leg_reserve_key(leg, details if isinstance(details, dict) else None)
        if key is None:
            continue
        borrow_by_reserve[key] = borrow_by_reserve.get(key, Decimal("0")) + (-value)

    total = Decimal("0")
    for leg in legs:
        details = _leg_field(leg, "details")
        if not isinstance(details, dict):
            continue
        if details.get("supply_leg_convention") == "gross":
            continue
        value = read_position_decimal(leg, "value_usd")
        if value is None or value.is_nan() or value <= 0:
            continue
        net = _dec(details.get("net_value_usd"))
        debt = _dec(details.get("debt_value_usd"))
        if net is None or debt is None or net.is_nan() or debt.is_nan() or debt <= 0:
            continue
        if value != net:
            continue
        key = _leg_reserve_key(leg, details)
        if key is None:
            continue
        take = min(debt, borrow_by_reserve.get(key, Decimal("0")))
        if take > 0:
            borrow_by_reserve[key] -= take
            total += take
    return total


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


def _json_list(s: Any) -> list[Any]:
    """Decode a JSON-array column (e.g. ``wallet_balances_json`` /
    ``positions_json``) to a list, tolerating ``None`` / ``""`` / malformed
    input (→ ``[]``) and an already-decoded list. Never raises."""
    if s is None or s == "":
        return []
    if isinstance(s, list):
        return s
    try:
        d = json.loads(s)
        return d if isinstance(d, list) else []
    except (json.JSONDecodeError, TypeError):
        return []


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

    if not out.get("protocol"):
        row_protocol = (row.get("protocol") or "").strip()
        if row_protocol:
            out["protocol"] = row_protocol

    # WITHDRAW permits an unmeasured amount; other lending schemas require a Decimal
    # and must fail validation rather than receiving an aliased None.
    if et == "WITHDRAW" and "amount" not in out and "amount_token" in out:
        out["amount"] = out["amount_token"]
    if et in {"SUPPLY", "REPAY", "DELEVERAGE"} and "amount" not in out:
        if out.get("amount_token") is not None:
            out["amount"] = out["amount_token"]
    if et == "BORROW" and "borrowed_amount" not in out:
        if out.get("amount_token") is not None:
            out["borrowed_amount"] = out["amount_token"]

    # 10,000 bps = 100%, so percentage units are bps / 100.
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
            # Non-v1 payloads intentionally pass through without schema validation.
            payloads_by_id[row_id] = decoded
            continue
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


# Pendle PT ledger rows use SWAP, but their USD proof is ``sy_amount * sy_price``
# on the typed Pendle event rather than a generic swap payload.
_PENDLE_PT_EVENT_TYPES = ("PT_BUY", "PT_SELL", "PT_REDEEM")
_PENDLE_PT_DISPOSAL_TYPES = ("PT_SELL", "PT_REDEEM")


def _g1_classify_swap_usd_legs(
    successful: list[dict[str, Any]],
    acct_events: list[dict[str, Any]],
    acct_payloads: dict[Any, dict[str, Any]],
) -> tuple[list[tuple[Any, str]], list[tuple[Any, str]]]:
    """Classify each successful SWAP ledger row's USD money-trail proof.

    Returns ``(missing_swap_usd, pt_unmeasured_disposals)``:

    * ``missing_swap_usd`` — SWAP rows with a genuinely missing USD pillar (no
      paired SwapEventPayload, a null SwapEventPayload USD, or an acquiring
      PT_BUY with no USD). These FAIL G1.
    * ``pt_unmeasured_disposals`` — PT disposal rows (PT_SELL / PT_REDEEM) whose
      sell-side SY price is unmeasured (sy_price=None — VIB-5276). The caller
      routes these to XFAIL (ticketed gap) or FAIL per the profile.

    VIB-5319: a SWAP ledger row backed by a typed Pendle PT event proves its
    money trail through the Pendle payload (``sy_amount × sy_price``), not a
    SwapEventPayload, so it is checked against the Pendle event here.
    """
    swap_acct_by_ledger_id: dict[Any, dict[str, Any]] = {}
    pt_acct_by_ledger_id: dict[Any, tuple[str, dict[str, Any]]] = {}
    for ae in acct_events:
        leg = ae.get("ledger_entry_id")
        if leg is None:
            continue
        et = ae.get("event_type")
        if et == "SWAP":
            swap_acct_by_ledger_id[leg] = acct_payloads.get(ae.get("id"), {})
        elif et in _PENDLE_PT_EVENT_TYPES:
            pt_acct_by_ledger_id[leg] = (et, acct_payloads.get(ae.get("id"), {}))

    missing_swap_usd: list[tuple[Any, str]] = []
    pt_unmeasured_disposals: list[tuple[Any, str]] = []
    for r in successful:
        if r.get("intent_type") != "SWAP":
            continue
        ledger_id = r.get("id")
        pt_entry = pt_acct_by_ledger_id.get(ledger_id)
        if pt_entry is not None:
            et, pt_payload = pt_entry
            sy_amount = pt_payload.get("sy_amount")
            sy_price = pt_payload.get("sy_price")
            if sy_amount not in (None, "") and sy_price not in (None, ""):
                continue
            detail = f"sy_amount={sy_amount!r} sy_price={sy_price!r}"
            # The waiver applies only to measured disposal amounts with an unavailable
            # gateway price (None). Missing amounts and parser-omitted prices ("") fail.
            amount_measured = sy_amount not in (None, "")
            price_is_gateway_gap = sy_price is None
            if et in _PENDLE_PT_DISPOSAL_TYPES and amount_measured and price_is_gateway_gap:
                pt_unmeasured_disposals.append((ledger_id, detail))
            elif et in _PENDLE_PT_DISPOSAL_TYPES:
                missing_swap_usd.append((ledger_id, f"{et} {detail}"))
            else:
                missing_swap_usd.append((ledger_id, f"PT_BUY {detail}"))
            continue
        payload = swap_acct_by_ledger_id.get(ledger_id)
        if payload is None:
            missing_swap_usd.append((ledger_id, "no SwapEventPayload row"))
            continue
        in_usd = payload.get("amount_in_usd")
        out_usd = payload.get("amount_out_usd")
        if in_usd in (None, "") or out_usd in (None, ""):
            missing_swap_usd.append((ledger_id, f"amount_in_usd={in_usd!r} amount_out_usd={out_usd!r}"))
    return missing_swap_usd, pt_unmeasured_disposals


# The scorecard imports the write-path landed/degraded predicates to prevent drift.
from almanak.framework.accounting.ledger_guard import LANDED_PARAMS as _LANDED_PARAMS
from almanak.framework.accounting.ledger_guard import degraded as _degraded_rule
from almanak.framework.accounting.ledger_guard import landed as _landed_rule
from almanak.framework.accounting.ledger_guard import landed_sql as _landed_sql


def _row_landed(row: dict[str, Any]) -> bool:
    """Whether a ledger row represents a transaction that actually LANDED.

    ``transaction_ledger.success`` is the *framework verdict*, not chain
    reality: the slippage circuit-breaker, the reconciliation finalizer and
    the Empty != Zero guard (VIB-6043 leg 2) all write ``success=False`` on
    transactions that landed on-chain.

    **Every cell that asks "did this transaction execute?" must use this, not
    ``row["success"]``.** Cells that ask "were the books clean?" should keep
    using ``success`` directly. Getting that distinction wrong in either
    direction produces a scorecard that reports something it has not
    verified — G1, G10 and G11 have each been bitten by it.
    """
    return _landed_rule(row.get("success"), row.get("error"), row.get("tx_hash"))


def _cell_g1_money_trail(
    rows: list[dict[str, Any]],
    acct_events: list[dict[str, Any]],
    acct_payloads: dict[Any, dict[str, Any]],
    primitive: str,
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

    VIB-5319: Pendle PT legs ride the SWAP intent_type but book typed
    ``PendleAccountingEvent`` rows whose USD lives in ``sy_amount × sy_price``.
    A PT-backed SWAP ledger row is checked against the Pendle payload, not a
    SwapEventPayload. A PT disposal whose sell-side SY price is unmeasured
    (sy_price=None — VIB-5276 gateway PT price) has a genuinely unmeasured
    money-trail USD leg; for a profile that opts in
    (``disposal_usd_unmeasured_is_xfail``) that is a ticketed measurement gap
    (XFAIL), not a books error (FAIL), and never a fabricated PASS.
    """
    if not rows:
        return CellResult(
            "G1",
            "Money trail (every credit/debit → tx_hash + USD@block)",
            "FAIL",
            "transaction_ledger empty",
        )
    # Reverted intents legitimately lack output amounts and are evaluated by G11.
    successful = [r for r in rows if r.get("success")]
    # Degraded rows must remain in G1 even though the write guard sets success=False.
    # Do not filter them by tx_hash: unmeasured rows without a captured hash must fail too.
    degraded = [r for r in rows if _degraded_rule(r.get("error"))]
    # Keep degraded rows in the hash check if verdict branches are reordered.
    missing_hash = [r for r in (*successful, *degraded) if not r.get("tx_hash")]
    missing_token_amounts = [
        r for r in successful if r.get("intent_type") == "SWAP" and not (r.get("amount_in") and r.get("amount_out"))
    ]
    missing_swap_usd, pt_unmeasured_disposals = _g1_classify_swap_usd_legs(successful, acct_events, acct_payloads)

    if degraded:
        sample_tx = degraded[0].get("tx_hash") or "<no tx_hash>"
        return CellResult(
            "G1",
            "Money trail",
            "FAIL",
            f"{len(degraded)} ledger rows are accounting-degraded "
            f"(money moved, amounts unmeasured) — e.g. tx {sample_tx}",
        )

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
    if pt_unmeasured_disposals:
        profile = _profile_for(primitive)
        pt_sample = pt_unmeasured_disposals[:3]
        if profile.disposal_usd_unmeasured_is_xfail:
            return CellResult(
                "G1",
                "Money trail",
                "XFAIL",
                f"{len(pt_unmeasured_disposals)} PT disposal row(s) carry no USD valuation: "
                f"sy_price=None (VIB-5276 gateway PT/SY price) — measured-but-blocked, "
                f"not a books error (e.g. {pt_sample!r}). Flips to PASS once the SY price lands.",
            )
        return CellResult(
            "G1",
            "Money trail",
            "FAIL",
            f"{len(pt_unmeasured_disposals)} PT disposal row(s) missing USD valuation (e.g. {pt_sample!r})",
        )
    swap_count = sum(1 for r in successful if r.get("intent_type") == "SWAP")
    return CellResult(
        "G1",
        "Money trail",
        "PASS",
        f"{len(rows)} ledger rows ({len(successful)} successful, {swap_count} SWAP); "
        "all tx_hashes present; SWAP/PT rows carry token amounts AND USD valuations",
    )


# Native-token residual floor, above Decimal round-trip noise but below a typical keeper fee.
_G16_ABS_EPSILON_NATIVE = Decimal("0.000005")
# Residuals below 5% of attributed native cost are treated as rounding.
_G16_REL_TOLERANCE = Decimal("0.05")


def _g16_native_balance(snapshot: dict[str, Any], symbol: str) -> Decimal | None:
    """The wallet's native-token balance at one snapshot, or None if unmeasured."""
    for wb in _json_list(snapshot.get("wallet_balances_json")):
        if not isinstance(wb, dict):
            continue
        if str(wb.get("symbol") or "").upper() != symbol.upper():
            continue
        raw = wb.get("balance")
        if raw is None:
            return None
        try:
            parsed = Decimal(str(raw))
        except (InvalidOperation, ValueError, TypeError):
            return None
        return parsed if parsed.is_finite() else None
    return None


@dataclass(frozen=True)
class _G16SettlementFee:
    """One settled order's native execution-fee economics (VIB-6061)."""

    submitted_at: str
    settled_at: str
    fee_native: Decimal | None
    escrow_native: Decimal | None


def _g16_settlement_fees(
    acct_events: list[dict[str, Any]],
    ledger: list[dict[str, Any]],
) -> list[_G16SettlementFee]:
    """Per-settlement native fee + escrow, with the window each escrow was in flight.

    ``submitted_at`` is the SUBMISSION ledger row's timestamp, recovered by joining
    ``submission_ledger_entry_id`` against ``transaction_ledger.id`` — not the
    settlement's own timestamp, and not an accounting-event id (that join looks
    plausible and silently matches nothing, collapsing every in-flight window to
    zero). The escrow leaves the wallet at submission and returns at settlement, so
    those are the two edges of the window; using the settlement timestamp for both
    would reintroduce the endpoint contamination this exists to remove.
    """
    by_ledger_id = {
        str(r.get("id") or ""): str(r.get("timestamp") or "") for r in ledger if isinstance(r, dict) and r.get("id")
    }
    out: list[_G16SettlementFee] = []
    for event in acct_events:
        if not isinstance(event, dict):
            continue
        if str(event.get("event_type") or "").upper() != "PERP_SETTLEMENT":
            continue
        payload = _json(event.get("payload_json")) or {}
        fee_wei = payload.get("keeper_execution_fee_wei")
        refund_wei = payload.get("execution_fee_refund_wei")
        if not isinstance(fee_wei, int) or isinstance(fee_wei, bool):
            continue
        fee_native = Decimal(fee_wei) / Decimal(10**18)
        escrow_native = (
            (Decimal(fee_wei) + Decimal(refund_wei)) / Decimal(10**18)
            if isinstance(refund_wei, int) and not isinstance(refund_wei, bool)
            else None
        )
        settled_at = str(event.get("timestamp") or "")
        submitted_at = by_ledger_id.get(str(payload.get("submission_ledger_entry_id") or ""), settled_at)
        out.append(_G16SettlementFee(submitted_at, settled_at, fee_native, escrow_native))
    return out


def _g16_escrow_outstanding(fees: list[_G16SettlementFee], at: str) -> Decimal:
    """Native escrowed with the venue but not yet settled at instant ``at``.

    Escrowed native is out of the wallet without being a cost, so a snapshot taken
    mid-flight understates the balance by exactly this amount. Adding it back makes
    the two endpoints comparable. An escrow we could not measure contributes
    nothing — it cannot be conjured — which is why an unmeasured settlement shows up
    as residual rather than being silently smoothed away.
    """
    total = Decimal("0")
    for fee in fees:
        if fee.escrow_native is None:
            continue
        if fee.submitted_at <= at < fee.settled_at:
            total += fee.escrow_native
    return total


# ``settled_at`` is booking time, not keeper block time. A snapshot in that gap sees
# the refund in-wallet while escrow still appears in flight, so endpoint windows with
# outstanding escrow must be refused. Do not widen tolerance to hide this ambiguity.


def _g16_gas_native(row: dict[str, Any], symbol: str) -> Decimal | None:
    """Recover one ledger row's gas cost in NATIVE units (Empty != Zero).

    ``gas_usd`` was written as ``gas_cost_wei / 1e18 * native_price``, and the
    price that produced it is persisted on the SAME row as ``price_inputs_json``.
    Dividing it back out therefore recovers the native amount exactly rather than
    approximately, and — crucially — it cannot drift with the market: valuing the
    whole run at one late price would inject a price move into a residual that is
    supposed to contain only unattributed COST.

    ``None`` when the row carries no gas_usd or no usable native price; the caller
    treats that as unmeasured and refuses to score, never as a zero-cost row.
    """
    gas_usd = _dec(row.get("gas_usd"))
    if gas_usd is None:
        return None
    prices = _json(row.get("price_inputs_json")) or {}
    entry = prices.get(symbol) or prices.get(symbol.upper()) or prices.get(symbol.lower())
    price = _dec(entry.get("price_usd")) if isinstance(entry, dict) else _dec(entry)
    if price is None or price <= 0:
        return None
    return gas_usd / price


def _cell_g16_native_lane(
    snapshots: list[dict[str, Any]],
    ledger: list[dict[str, Any]],
    acct_events: list[dict[str, Any]],
) -> CellResult:
    """G16 — every native token that left the wallet is on a Cost Stack line.

    THE INVARIANT VIB-6061 EXISTS FOR. The Cost Stack counted transaction gas and
    nothing else, so the GMX keeper execution fee — 86% of native spend on the
    sealed ``20260726-0035-gmxdca-arb`` run — was invisible in every bucket. A
    number can be fixed and silently regress; this cell is what makes the regression
    loud, by refusing to let any native outflow go unnamed.

    The identity, in NATIVE units (never USD — see ``_g16_gas_native``):

        native(t0) - native(t1) == gas + venue_execution_fee   (+/- epsilon)

    ESCROW IS THE SUBTLETY. A GMX order posts its execution fee as ``msg.value`` at
    submission and the unused part comes back only when the keeper settles. A
    snapshot taken between those two moments sees the whole escrow gone from the
    wallet, and on the arb bundle above the very first snapshot lands ONE SECOND
    after a submission — so a naive endpoint difference reads a ~0.001 ETH escrow as
    if it were a cost, and the cell would fail forever on runs that are perfectly
    reconciled. ``_g16_escrow_outstanding`` adds back the escrow in flight at each
    endpoint, which is what makes a green here achievable and therefore meaningful.

    Scoring is deliberately two-sided. An UNDER-attributed residual is the VIB-6061
    defect (native left and no line claims it). An OVER-attributed one is the
    mirror defect that booking the escrow rather than the keeper's cut would have
    produced. Both FAIL, and the diagnostic reports the signed residual so the
    direction is legible.

    NA, never a silent pass, when the run cannot support the check: fewer than two
    snapshots carrying a native balance, or any ledger row whose native gas cannot
    be recovered. "We could not measure this" must never render as "this passed" —
    a cell that greens on missing data is the vacuity this suite has been bitten by.
    """
    chain = (ledger[0].get("chain") if ledger else "") or ""
    symbol = native_token_for_chain(chain)

    # SQLite row order is not chronological; refuse unorderable endpoints rather than guess.
    ordered = _snapshots_in_time_order(snapshots)
    if ordered is None:
        return CellResult(
            "G16",
            "Native lane (cost stack == native balance delta)",
            "SKIP",
            "snapshots cannot be ordered by time (a timestamp is unmeasured or unparseable), "
            "so the window endpoints would be chosen by SQLite row order",
        )
    priced = [s for s in ordered if _g16_native_balance(s, symbol) is not None]
    if len(priced) < 2:
        return CellResult(
            "G16",
            "Native lane (cost stack == native balance delta)",
            "SKIP",
            f"need >=2 snapshots carrying a {symbol} wallet balance (have {len(priced)} of {len(snapshots)})",
        )

    # Native outflow includes every chain-landed row, including rows with a failed books verdict.
    settled = [r for r in ledger if _row_landed(r)]
    if not settled:
        return CellResult(
            "G16",
            "Native lane (cost stack == native balance delta)",
            "SKIP",
            "no successful on-chain ledger rows — no native cost to attribute",
        )

    unrecoverable = [r for r in settled if _g16_gas_native(r, symbol) is None]
    if unrecoverable:
        return CellResult(
            "G16",
            "Native lane (cost stack == native balance delta)",
            "SKIP",
            f"{len(unrecoverable)}/{len(settled)} successful ledger rows carry no recoverable native gas "
            f"(missing gas_usd or no {symbol} price in price_inputs_json) — the native lane is unmeasured, "
            "which is NOT the same as reconciled",
        )

    first, last = priced[0], priced[-1]
    t0 = str(first.get("timestamp") or "")
    t1 = str(last.get("timestamp") or "")

    fees = _g16_settlement_fees(acct_events, ledger)
    # Re-read rather than coerce None to zero; an unmeasured endpoint is not zero balance.
    start_balance = _g16_native_balance(first, symbol)
    end_balance = _g16_native_balance(last, symbol)
    if start_balance is None or end_balance is None:  # pragma: no cover — gated above
        return CellResult(
            "G16",
            "Native lane (cost stack == native balance delta)",
            "SKIP",
            f"the window endpoints stopped carrying a measured {symbol} balance",
        )
    # Booking time cannot resolve an endpoint with in-flight escrow; refuse to score
    # until the payload carries the keeper transaction's block timestamp.
    escrow_t0 = _g16_escrow_outstanding(fees, t0)
    escrow_t1 = _g16_escrow_outstanding(fees, t1)
    if escrow_t0 or escrow_t1:
        return CellResult(
            "G16",
            "Native lane (cost stack == native balance delta)",
            "SKIP",
            f"a venue execution-fee escrow is outstanding at a window endpoint "
            f"(t0={escrow_t0}, t1={escrow_t1} {symbol}) as of the settlement BOOKING time, which "
            "is not when the keeper settled on-chain; the endpoint balance cannot be corrected "
            "without the keeper block timestamp, so this window is unscoreable",
        )
    start = start_balance
    end = end_balance
    observed_outflow = start - end

    in_window_gas: list[Decimal] = []
    for r in settled:
        if not (t0 < str(r.get("timestamp") or "") <= t1):
            continue
        gas = _g16_gas_native(r, symbol)
        if gas is not None:
            in_window_gas.append(gas)
    gas_native = sum(in_window_gas, Decimal("0"))
    fee_native = sum(
        (f.fee_native for f in fees if f.fee_native is not None and t0 < f.settled_at <= t1),
        Decimal("0"),
    )
    attributed = gas_native + fee_native
    residual = observed_outflow - attributed
    tolerance = max(_G16_ABS_EPSILON_NATIVE, abs(attributed) * _G16_REL_TOLERANCE)

    decomposition = {
        "native_symbol": symbol,
        "window": f"{t0} .. {t1}",
        "observed_outflow_native": str(observed_outflow),
        "attributed_native": str(attributed),
        "gas_native": str(gas_native),
        "venue_execution_fee_native": str(fee_native),
        "residual_native": str(residual),
        "tolerance_native": str(tolerance),
    }

    if abs(residual) <= tolerance:
        return CellResult(
            "G16",
            "Native lane (cost stack == native balance delta)",
            "PASS",
            f"{symbol} outflow {observed_outflow} == gas {gas_native} + venue fee {fee_native} "
            f"(residual {residual}, tolerance {tolerance})",
            decomposition=decomposition,
        )

    if residual > 0:
        headline = (
            f"UNATTRIBUTED {symbol}: the wallet gave up {observed_outflow} but the Cost Stack accounts "
            f"for only {attributed} (gas {gas_native} + venue execution fee {fee_native}). "
            f"{residual} of native left the wallet with no cost line naming it — this is the VIB-6061 shape."
        )
    else:
        headline = (
            f"OVER-ATTRIBUTED {symbol}: the Cost Stack claims {attributed} of native cost "
            f"(gas {gas_native} + venue execution fee {fee_native}) but the wallet only gave up "
            f"{observed_outflow}, leaving {-residual} of native unexplained INTO the wallet. "
            "Booking an escrow rather than the keeper's cut of it produces exactly this; so does an "
            "unbooked refund or an external top-up mid-run, which this cell cannot tell apart — "
            "check for a native transfer before treating it as a booking defect."
        )
    return CellResult(
        "G16",
        "Native lane (cost stack == native balance delta)",
        "FAIL",
        f"{headline} Residual {residual} exceeds tolerance {tolerance}.",
        decomposition=decomposition,
    )


def _cell_g17_receipt_set(rows: list[dict[str, Any]]) -> CellResult:
    """G17 — persisted landed rows carry an internally complete receipt set.

    This is intentionally narrower than chain completeness.  It detects a
    missing or contradictory leg inside a row the ledger persisted; it cannot
    detect a whole attempt absent from the DB (VIB-6303), which requires an
    independent nonce-window reconciliation (VIB-6368).
    """
    evaluation = evaluate_landed_receipt_sets(rows)
    description = "Receipt-set integrity (typed legs, identity, status, exact execution resource)"
    if evaluation.landed_rows == 0:
        return CellResult(
            "G17",
            description,
            "SKIP",
            "no landed ledger rows; receipt-set integrity asserted nothing",
        )
    if evaluation.findings:
        rendered = "; ".join(
            f"{finding.row_id}:{finding.code}: {finding.detail}" for finding in evaluation.findings[:5]
        )
        remainder = len(evaluation.findings) - 5
        if remainder > 0:
            rendered = f"{rendered}; +{remainder} more finding(s)"
        return CellResult(
            "G17",
            description,
            "FAIL",
            f"{len(evaluation.findings)} violation(s) across {evaluation.landed_rows} landed row(s): {rendered}",
            decomposition={
                "landed_rows": evaluation.landed_rows,
                "sub_transactions": evaluation.sub_transactions,
                "finding_codes": [finding.code for finding in evaluation.findings],
                "scope": "persisted landed rows only; nonce completeness not asserted",
            },
        )
    return CellResult(
        "G17",
        description,
        "PASS",
        f"{evaluation.landed_rows} landed row(s), {evaluation.sub_transactions} unique successful "
        "sub-transaction(s): parent ACTION identity and exact aggregate gas verified; "
        "nonce completeness not asserted",
        decomposition={
            "landed_rows": evaluation.landed_rows,
            "sub_transactions": evaluation.sub_transactions,
            "scope": "persisted landed rows only; nonce completeness not asserted",
        },
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
    # Entries are heterogeneous because only their count is consumed.
    yields: list[tuple[Any, ...]] = []
    for r in pos_events:
        if r.get("fees_token0") or r.get("fees_token1"):
            yields.append(("fees", r.get("event_type"), r.get("fees_token0"), r.get("fees_token1")))
    for r in acct_events:
        p = _json(r.get("payload_json"))
        # Partial SWAP matches use matched-portion PnL; other events use their own PnL field.
        rpnl_for_yield = (
            p.get("realized_pnl_usd_matched")
            if r.get("event_type") == "SWAP" and p.get("realized_pnl_usd_matched") is not None
            else p.get("realized_pnl_usd")
        )
        if rpnl_for_yield:
            yields.append(("realized_pnl", r.get("event_type"), rpnl_for_yield))
        # Pendle disposals book PnL as realized_yield_usd. Normalize strings so
        # measured zero is not misclassified as a yield-emitting event.
        pendle_yield = _dec(p.get("realized_yield_usd"))
        if pendle_yield is not None and pendle_yield != Decimal("0"):
            yields.append(("pendle_yield", r.get("event_type"), p.get("realized_yield_usd")))
        # Accept both legacy and projected lending interest fields.
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

    * ``total_value_usd`` — the deployed (positions) side of strategy value,
      Σ POSITIVE ``value_usd`` (gross of debt, VIB-3614).
    * ``available_cash_usd`` — the uninvested cash side.
    * Strategy equity = ``total_value_usd − debt_mark + available_cash_usd``
      (VIB-5857 — the reported equity nets the debt legs).

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
    # Restarts reset iteration numbers, so only timestamp order can identify "right now".
    ordered = _snapshots_in_time_order(snapshots)
    if ordered is None:
        return CellResult(
            "G4",
            "Capital deployed right now",
            "FAIL",
            "cannot order snapshots by time — at least one timestamp is unmeasured or "
            "unparseable, so the terminal ('right now') snapshot cannot be identified "
            "and row order must not elect it (VIB-6545)",
        )
    last = ordered[-1]
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
    # Report canonical net equity even though this cell predicates only measuredness and sign.
    debt_mark = _snapshot_debt_mark(last.get("positions_json"))
    equity = wallet_nav_usd(deployed, debt_mark, cash)
    unreadable = _unreadable_payload_rows(ordered)
    missing_debt = _missing_debt_leg_rows(ordered)
    return CellResult(
        "G4",
        "Capital deployed right now",
        "PASS",
        f"deployed=${deployed} cash=${cash} debt_mark=${debt_mark} equity=${equity}"
        f" unreadable_payload_rows={unreadable} missing_debt_leg_rows={missing_debt}",
    )


def _g5_parse_baseline_provenance(
    metrics_row: dict[str, Any],
    initial: Decimal,
) -> tuple[Any | None, CellResult | None]:
    """Parse G5's atomic authority and reject corrupt/contradictory records."""
    from almanak.framework.portfolio.models import BaselineProvenanceError, decode_baseline_provenance

    try:
        provenance = decode_baseline_provenance(metrics_row.get("positions_json") or "[]")
    except BaselineProvenanceError as exc:
        return None, CellResult(
            "G5",
            "Initial vs current",
            "FAIL",
            f"baseline provenance is invalid: {exc}",
        )
    if provenance is not None and provenance.initial_value_usd != initial:
        return None, CellResult(
            "G5",
            "Initial vs current",
            "FAIL",
            f"baseline provenance contradicts portfolio_metrics: provenance initial="
            f"${provenance.initial_value_usd} metrics initial=${initial} source={provenance.source}",
        )
    return provenance, None


def _g5_baseline_source_failure(
    *,
    provenance: Any | None,
    initial: Decimal,
    opening_deployed: Decimal | None,
    opening_cash: Decimal | None,
    opening: Decimal,
    detail: str,
) -> CellResult | None:
    """Return a fail-closed G5 result when source evidence contradicts the opening."""
    if provenance is not None and provenance.source == "snapshot_total_value_usd" and initial != opening_deployed:
        return CellResult(
            "G5",
            "Initial vs current",
            "FAIL",
            f"baseline provenance source contradicts snapshot-0: source=snapshot_total_value_usd "
            f"initial=${initial} snapshot-0 deployed=${opening_deployed}; {detail}",
        )
    if provenance is not None and provenance.source == "snapshot_available_cash_usd" and initial != opening_cash:
        return CellResult(
            "G5",
            "Initial vs current",
            "FAIL",
            f"baseline provenance source contradicts snapshot-0: source=snapshot_available_cash_usd "
            f"initial=${initial} snapshot-0 cash=${opening_cash}; {detail}",
        )
    cash_drop = (
        opening_deployed is not None
        and opening_cash is not None
        and initial == opening_deployed
        and opening_cash > _G5_CASH_DROP_FLOOR_USD
    )
    if not cash_drop or (provenance is not None and provenance.source == "strategy_allocation_usd"):
        return None
    if provenance is None:
        return CellResult(
            "G5",
            "Initial vs current",
            "FAIL",
            f"baseline excludes the cash leg: initial_value_usd=${initial} equals snapshot-0 "
            f"deployed=${opening_deployed} while snapshot-0 held cash=${opening_cash}; "
            f"opening equity was ${opening}, so any PnL measured against this baseline is "
            f"offset by ${opening_cash}. TWO legacy writers produce this shape and the row "
            f"has no immutable provenance: (a) the VIB-6349 defect — "
            f"'total_value_usd or available_cash_usd' silently discarding cash when a "
            f"position is already open at the first snapshot; (b) the VIB-3882 contract — "
            f"a strategy declaring allocation_usd that coincidentally equals its deployed "
            f"value, where excluding unrelated wallet cash is INTENDED. Legacy ambiguity "
            f"cannot be resolved retroactively. {detail}",
        )
    if provenance.source == "snapshot_available_cash_usd":
        return CellResult(
            "G5",
            "Initial vs current",
            "FAIL",
            f"baseline provenance proves the deployed leg was excluded: source="
            f"snapshot_available_cash_usd initial=${initial} snapshot-0 deployed=${opening_deployed}; "
            f"opening equity=${opening}, so PnL against the persisted baseline is offset "
            f"by ${opening_deployed}. {detail}",
        )
    return CellResult(
        "G5",
        "Initial vs current",
        "FAIL",
        f"baseline provenance proves the cash leg was discarded: source="
        f"{provenance.source} initial=${initial} snapshot-0 cash=${opening_cash}; "
        f"opening equity=${opening}, so PnL against the persisted baseline is offset "
        f"by ${opening_cash}. {detail}",
    )


def _cell_g5_initial_vs_current(
    metrics: list[dict[str, Any]],
    snapshots: list[dict[str, Any]],
    ledger: list[dict[str, Any]] | None = None,
) -> CellResult:
    """G5 — "Initial vs current equity is consistent across snapshots".

    Blueprint 27 specifies a *consistency* assertion. Pre-VIB-6345 this cell
    asserted nothing: it formatted ``current - initial`` and returned PASS
    whenever both sides parsed. On mainnet R1 it reported ``+$5.709`` on a run
    that lost ``$0.61`` — and passed. A wrong-signed PnL behind a green cell is
    worse than a null, because the green is what certifies the books.

    Two independent errors produced that number; both are addressed here.

    **1. Denomination.** Legacy ``initial_value_usd`` rows were written from
    deployed value when a position was already open at the first snapshot,
    else cash. The current side is
    ``_snapshot_equity`` = deployed + cash. Subtracting one from the other
    mixes bases, which is how the sign flipped. The reported delta is now
    equity-vs-equity, both sides from ``_snapshot_equity``, so a denomination
    mismatch can no longer reach the number.

    **2. The baseline itself.** New rows carry an immutable, same-row source
    record. A declared ``strategy_allocation_usd`` may intentionally exclude
    unrelated wallet cash (VIB-3882); a ``snapshot_total_value_usd`` source
    with a measured cash leg proves the fallback defect. Legacy rows retain
    the conservative ambiguous FAIL because their origin cannot be inferred
    retroactively. Malformed, duplicate, unknown-version, or contradictory
    provenance fails closed.

    Baseline *placement* is reported, never silently folded in: a first
    snapshot that post-dates the first ledger row cannot see that
    transaction's cost, so the delta is known to understate (R1: ``-$0.375``
    measured against ``-$0.61`` on-chain). Correcting the placement is its own
    change; G5 names it so the number is never read as exact.
    """
    if not metrics:
        return CellResult("G5", "Initial vs current", "FAIL", "no portfolio_metrics row")
    m = metrics[-1]
    initial = _dec(m.get("initial_value_usd"))
    if initial is None:
        return CellResult("G5", "Initial vs current", "FAIL", "initial_value_usd null")
    provenance, provenance_failure = _g5_parse_baseline_provenance(m, initial)
    if provenance_failure is not None:
        return provenance_failure
    if not snapshots:
        return CellResult(
            "G5",
            "Initial vs current",
            "FAIL",
            f"initial=${initial} but no snapshots for current",
        )
    # PnL endpoints must be chronological; SQLite row order is not an oracle.
    ordered = _snapshots_in_time_order(snapshots)
    if ordered is None:
        return CellResult(
            "G5",
            "Initial vs current",
            "FAIL",
            "cannot order snapshots by time — at least one timestamp is unmeasured or "
            "unparseable, and portfolio_snapshots is read without ORDER BY, so the PnL "
            "endpoints would be SQLite row order rather than first/last",
        )

    # UNAVAILABLE snapshots contain placeholder zeros and cannot anchor PnL.
    usable = [s for s in ordered if _snapshot_confidence_can_anchor_pnl(s)]
    refused = len(ordered) - len(usable)
    if len(usable) < 2:
        why = (
            f"; {refused} refused as unmeasured/UNAVAILABLE"
            if refused
            else " (none were refused — the run has too few snapshots)"
        )
        return CellResult(
            "G5",
            "Initial vs current",
            "FAIL",
            f"need >=2 snapshots whose confidence can anchor a PnL endpoint; have {len(usable)} of {len(ordered)}{why}",
        )
    # If the terminal row is refused, the diagnostic must not imply full-run coverage.
    trailing_refused = not _snapshot_confidence_can_anchor_pnl(ordered[-1])
    ordered = usable

    current = _snapshot_equity(ordered[-1])
    if current is None:
        return CellResult("G5", "Initial vs current", "FAIL", f"initial=${initial} but current null")

    # Compare equity endpoints; initial_value_usd is the value under test.
    opening = _snapshot_equity(ordered[0])
    if opening is None:
        return CellResult(
            "G5",
            "Initial vs current",
            "FAIL",
            f"first snapshot has no measured equity (initial=${initial} current=${current})",
        )
    delta = current - opening

    detail = f"opening_equity=${opening} current=${current} delta=${delta}"
    if provenance is not None:
        detail += f"; baseline_source={provenance.source} schema_version={provenance.schema_version}"
    else:
        detail += "; baseline_source=legacy_unmeasured"
    if trailing_refused:
        detail += (
            f"; NOTE current is NOT the run's last snapshot — {refused} trailing/other row(s) "
            f"were refused as unmeasured, so this delta stops at the last measured point"
        )

    # Both opening columns must be measured before diagnosing a discarded cash leg.
    opening_deployed = _dec(ordered[0].get("total_value_usd"))
    opening_cash = _dec(ordered[0].get("available_cash_usd"))
    source_failure = _g5_baseline_source_failure(
        provenance=provenance,
        initial=initial,
        opening_deployed=opening_deployed,
        opening_cash=opening_cash,
        opening=opening,
        detail=detail,
    )
    if source_failure is not None:
        return source_failure

    # A late baseline yields a real but understated delta, so report the placement.
    late_by = _baseline_window_coverage(ordered[0] if ordered else None, ledger).late_by
    if late_by is not None:
        detail += (
            f"; NOTE baseline is LATE — first snapshot post-dates the first ledger row "
            f"by {late_by}, so pre-baseline cost is invisible and the delta understates"
        )

    return CellResult("G5", "Initial vs current", "PASS", detail)


# Measured-zero cash is valid; ignore sub-cent dust when detecting a discarded cash leg.
_G5_CASH_DROP_FLOOR_USD = Decimal("0.01")


def _snapshot_confidence_can_anchor_pnl(snapshot: dict[str, Any]) -> bool:
    """Whether this snapshot claims a measurement good enough to be a PnL endpoint.

    ``UNAVAILABLE`` is the runner's *failure* contract, not a measurement:
    ``runner_state._make_unavailable_snapshot`` stamps ``total_value_usd=0``
    and ``available_cash_usd=0`` purely so the equity curve has no holes, and
    that row reaches ``portfolio_snapshots`` even though it never establishes
    ``initial_value_usd``. Reading its ``0 + 0`` as an opening equity reports
    the entire final balance as profit.

    A row whose confidence is missing or outside the vocabulary is also
    refused. This is NOT "the writer always stamps it":
    ``PortfolioSnapshot.value_confidence`` defaults to ``None`` and serialises
    to ``""``, which happens exactly when the valuer returned no confidence —
    i.e. precisely the rows ``PortfolioSnapshot.is_valid`` already calls
    invalid. Refusing them is therefore agreeing with the framework, not
    guessing. Measured across the nine committed fixtures, zero rows carry a
    null or empty stamp, so the refusal costs nothing on real data.

    ``STALE`` anchors: the value was genuinely measured, just old. That is a
    freshness question, not a "this number is fabricated" question, and G9
    owns freshness.
    """
    from almanak.framework.portfolio.models import ValueConfidence

    raw = snapshot.get("value_confidence")
    if raw is None:
        return False
    try:
        parsed = ValueConfidence.parse(raw)
    except (ValueError, TypeError):
        # Do not normalize unknown confidence values into trusted measurements.
        return False
    return parsed.value in _G5_ANCHORING_CONFIDENCES


# This closed classification must be updated explicitly when ValueConfidence grows.
_G5_ANCHORING_CONFIDENCES = frozenset({"HIGH", "ESTIMATED", "STALE"})

_G5_REFUSED_CONFIDENCES = frozenset({"UNAVAILABLE"})


def _snapshots_in_time_order(snapshots: list[dict[str, Any]]) -> list[dict[str, Any]] | None:
    """Snapshots oldest-first by ``(timestamp, id)``, or ``None`` when that is impossible.

    This is the ONE ordering authority for ``portfolio_snapshots`` rows
    (VIB-6545). ``iteration_number`` must never participate in ordering: it is
    process-local and a mid-run restart resets it to 1, so a
    ``(iteration_number, timestamp)`` key files the terminal teardown snapshot
    in the MIDDLE of the series — on the sealed bundle ``20260804-2310-gmxrt``
    that made every cell reading ``snapshots[-1]`` measure a window ending
    before the close, and G6 reported ``+$0.0149`` on a run that lost $0.52.

    Returns ``None`` if ANY row's timestamp is unmeasured or unparseable. The
    earlier revision fell back to the caller's order in that case, which was
    the wrong failure direction: ``_table_rows`` issues no ``ORDER BY``, so the
    "preserved historical behaviour" was SQLite row order, and a cell whose job
    is to stop a green wrong PnL would have gone on to PASS with endpoints
    chosen by accident. Refusing to order is information the caller must act
    on, not a detail to swallow.

    The ``id`` tie-break makes the order a function of the DATA rather than of
    the caller's row order: both snapshot writers stamp whole seconds, so two
    rows can carry an equal timestamp, and a bare stable sort would then keep
    whatever order the rows arrived in — deterministic per call, but different
    for two callers passing the same rows differently ordered. ``id`` is the
    SQLite ``INTEGER PRIMARY KEY AUTOINCREMENT``, i.e. persistence order — the
    best same-second approximation of event order the schema records. A row
    without an integer-coercible ``id`` falls back to input position, ranked
    after every id-bearing row so the two key shapes never interleave.
    """
    pairs: list[tuple[tuple[datetime, tuple[int, int]], dict[str, Any]]] = []
    for index, snapshot in enumerate(snapshots):
        parsed = _parse_ts(snapshot.get("timestamp"))
        if parsed is None:
            return None
        try:
            tiebreak = (0, int(snapshot.get("id")))  # type: ignore[arg-type]
        except (TypeError, ValueError):
            tiebreak = (1, index)
        pairs.append(((parsed, tiebreak), snapshot))
    return [s for _, s in sorted(pairs, key=lambda pair: pair[0])]


def _parse_ts(value: Any) -> datetime | None:
    """Best-effort timestamp read, ``None`` when unmeasured or unparseable.

    Mirrors the normalisation ``basis.py`` already applies to event
    timestamps: ``Z`` suffix accepted, naive values read as UTC. Ledger rows
    reach us as ISO strings from SQLite but as int epochs when the gateway
    serialised them, so both are handled.
    """
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=UTC)
    if isinstance(value, int | float):
        try:
            return datetime.fromtimestamp(float(value), tz=UTC)
        except (ValueError, OverflowError, OSError):
            return None
    if not isinstance(value, str):
        return None
    try:
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except (ValueError, TypeError):
        return None
    return dt if dt.tzinfo else dt.replace(tzinfo=UTC)


@dataclass(frozen=True)
class WindowCoverage:
    """Whether a snapshot endpoint brackets every money-moving row before it.

    A snapshot-derived delta whose baseline is captured *after* the first
    money-moving transaction can never see that transaction's cost, so the
    error is directional: it only ever understates. R1 measured ``-$0.375``
    against an on-chain ``-$0.6145`` with the baseline 3s late (VIB-6345).

    ``measurable`` is False when the endpoint carries no parseable timestamp —
    Empty != Zero, so ``covers`` is not a claim in that case and
    ``gas_before_usd`` is ``None`` (unmeasured) rather than ``Decimal(0)``
    (measured zero). Callers must branch on ``measurable`` before reading a
    verdict off ``covers``.
    """

    measurable: bool
    covers: bool
    late_by: str | None
    rows_before: int
    gas_before_usd: Decimal | None
    gas_before_unmeasured_rows: int
    gas_before_complete: bool
    rows_without_timestamp: int
    earliest_before_ts: str

    @property
    def gas_before_measured(self) -> Decimal | None:
        """The pre-window gas aggregate, or ``None`` when it is not a total.

        Empty != Zero applies to the AGGREGATE, not just the terms: if any
        pre-window row's ``gas_usd`` was unmeasured, the sum of the rest is a
        subtotal, not "the gas spent before the baseline". Reporting that
        subtotal as a measured magnitude would let the ratchet lock a floor
        below the true value, and would let the verdict below call a gap
        "explained" on partial evidence.
        """
        if self.gas_before_usd is None or not self.gas_before_complete:
            return None
        return self.gas_before_usd


def _baseline_window_coverage(
    endpoint: dict[str, Any] | None,
    ledger: list[dict[str, Any]] | None,
) -> WindowCoverage:
    """Measure the ledger rows that predate a wallet-method endpoint.

    Shared by G5 (diagnostic note on an otherwise-consistent baseline) and G6
    (gating verdict — the gap is *arithmetically* wrong when the brackets
    differ). It reports lateness rather than repairing it: the ratified fix is
    producer-side, a pre-trade boot snapshot (VIB-5854).

    A ledger row with no parseable timestamp is UNMEASURED, not "before": it is
    counted in ``rows_without_timestamp`` and never trips the invariant. The
    surrounding sort key ``r.get("timestamp") or ""`` sorts such a row *first*
    (VIB-6348), which is exactly the mis-election this measurement must not
    inherit.

    KNOWN LIMITATION (VIB-6429): ``covers=True`` is weaker than it reads, in two
    ways the panel identified. (a) Both writers serialise whole seconds —
    ``int(...timestamp())`` at ``gateway_state_manager.py:316`` and ``:412`` — so a
    transaction executed just *before* the endpoint within the same second stores
    an EQUAL timestamp and is counted as covered by the ``<`` below. Two committed
    fixtures (``lp_curve``, ``lp_curve_tricrypto``) show exactly 0s between their
    first ledger row and their first priced snapshot, which is the signature of
    that truncation rather than of simultaneity. (b) A row whose timestamp is
    unparseable has an unknowable position, yet ``covers`` is still reported as a
    measurement. Neither is a regression — both cases reach the same verdict as
    they do without this guard, because the guard simply does not fire — so this
    is a detector that is less sensitive than the data ideally allows, not a false
    certification introduced here. Closing it needs an ordering source with
    sub-second resolution (a cycle/sequence marker), which is a producer-side
    change of the same family as the boot snapshot itself.
    """
    unmeasured = WindowCoverage(
        measurable=False,
        covers=False,
        late_by=None,
        rows_before=0,
        gas_before_usd=None,
        gas_before_unmeasured_rows=0,
        gas_before_complete=False,
        rows_without_timestamp=0,
        earliest_before_ts="",
    )
    if endpoint is None:
        return unmeasured
    endpoint_ts = _parse_ts(endpoint.get("timestamp"))
    if endpoint_ts is None:
        return unmeasured
    rows = ledger or []
    untimed = 0
    before: list[tuple[datetime, dict[str, Any]]] = []
    for r in rows:
        ts = _parse_ts(r.get("timestamp"))
        if ts is None:
            untimed += 1
        elif ts < endpoint_ts:
            before.append((ts, r))

    gas_before = Decimal(0)
    gas_unmeasured = 0
    for _, r in before:
        gas = _dec(r.get("gas_usd"))
        if gas is None:
            gas_unmeasured += 1
        else:
            gas_before += gas

    earliest = min((ts for ts, _ in before), default=None)
    return WindowCoverage(
        measurable=True,
        covers=not before,
        late_by=(None if earliest is None else f"{(endpoint_ts - earliest).total_seconds():.0f}s"),
        rows_before=len(before),
        gas_before_usd=gas_before,
        gas_before_unmeasured_rows=gas_unmeasured,
        gas_before_complete=(gas_unmeasured == 0),
        rows_without_timestamp=untimed,
        earliest_before_ts=("" if earliest is None else earliest.isoformat()),
    )


# Keep settlement supersession here in lockstep with dashboard reconciliation.


def _g6_settlement_by_link(
    acct_events: list[dict[str, Any]], acct_payloads: dict[Any, dict[str, Any]]
) -> dict[str, dict[str, Any]]:
    """Map submission ledger id → EXECUTED ``PERP_SETTLEMENT`` payload.

    Only ``EXECUTED`` settlements carry measured fill economics — ``CANCELLED`` /
    ``FROZEN`` / ``NOT_FOUND_UNCORRELATED`` / ``UNMEASURED`` must NOT supersede the
    submission estimate, or an unfilled order would overwrite a measured estimate
    with a fabricated zero. Mirrors ``_executed_settlements_by_link``.
    """
    by_link: dict[str, dict[str, Any]] = {}
    for r in acct_events:
        if str(r.get("event_type") or "").upper() != "PERP_SETTLEMENT":
            continue
        p = acct_payloads.get(r.get("id"), {})
        if p.get("settlement_state") != "EXECUTED":
            continue
        link = str(p.get("submission_ledger_entry_id") or r.get("ledger_entry_id") or "")
        if link:
            by_link[link] = p
    return by_link


def _g6_perp_estimate_links(acct_events: list[dict[str, Any]]) -> set[str]:
    """Ledger ids of the ESTIMATED ``PERP_OPEN`` / ``PERP_CLOSE`` submission rows.

    A settlement whose link is in this set was already merged by the PERP_CLOSE
    branch; only an ORPHAN settlement folds its own economics. Mirrors
    ``_perp_estimate_links``.
    """
    links: set[str] = set()
    for r in acct_events:
        if str(r.get("event_type") or "").upper() in ("PERP_OPEN", "PERP_CLOSE"):
            link = str(r.get("ledger_entry_id") or "")
            if link:
                links.add(link)
    return links


def _g6_settlement_cost_usd(p: dict[str, Any]) -> tuple[Decimal | None, Decimal | None]:
    """``(trading_fee, keeper_execution_fee)`` for one settlement payload, in USD.

    ``trading_fee`` is ``position_fee + borrowing_fee``. Empty≠Zero on BOTH
    components: ``None`` when either is unmeasured, because a partial that silently
    drops a component is a wrong number rather than a smaller one. Mirrors
    ``_settlement_fee_usd`` + ``_fold_venue_execution_fee``.
    """
    position_fee = _dec(p.get("position_fee_usd"))
    borrowing_fee = _dec(p.get("borrowing_fee_usd"))
    trading = None if (position_fee is None or borrowing_fee is None) else position_fee + borrowing_fee
    return trading, _dec(p.get("keeper_execution_fee_usd"))


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
    # Invalid payloads must fail before missing fields can collapse into zero contribution.
    blocked = _payload_block_cell(
        "G6",
        "Reconciliation (wallet ≡ component)",
        acct_events,
        payload_errors,
    )
    if blocked is not None:
        return blocked, {}
    # Wallet PnL requires chronological endpoints; process-local iteration numbers reset.
    ordered_by_time = _snapshots_in_time_order(snapshots)
    if ordered_by_time is None:
        return (
            CellResult(
                "G6",
                "Reconciliation (wallet ≡ component)",
                "FAIL",
                "cannot order snapshots by time — at least one timestamp is unmeasured or "
                "unparseable, so the wallet-method endpoints would be row order rather "
                "than chronological first/last (VIB-6545)",
            ),
            {},
        )
    priced = [s for s in ordered_by_time if _snapshot_confidence_can_anchor_pnl(s) and _snapshot_equity(s) is not None]
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

    sum_swap = Decimal(0)
    sum_lp = Decimal(0)
    sum_perp = Decimal(0)
    sum_fees = Decimal(0)
    sum_funding = Decimal(0)
    sum_interest = Decimal(0)
    sum_gas = Decimal(0)
    il_diagnostic = Decimal(0)

    # Null bucket counts prevent unmeasured component PnL from becoming measured zero.
    null_swap_rpnl = 0
    # A swap with measured amounts but no prior FIFO basis is not an unmeasured PnL input.
    no_prior_basis_swap = 0
    null_lp_close_rpnl = 0
    null_lp_fees = 0
    null_perp_rpnl = 0
    null_perp_funding = 0
    null_withdraw_interest = 0
    null_repay_interest = 0
    # Pendle disposal PnL is realized_yield_usd, not gross proceeds. Only a matched
    # lot with an unavailable USD projection is waiver-eligible; missing amounts fail.
    # A measured disposal with no prior lot is diagnostic-only, as for generic swaps.
    null_pt_realized_usd = 0
    null_pt_amount = 0
    no_prior_basis_pt = 0

    notional_traded = Decimal(0)
    debt_outstanding = Decimal(0)
    max_debt = Decimal(0)
    max_perp_notional = Decimal(0)

    # Settlement fees are positive cost subtotals and are negated into the net fee bucket.
    sum_perp_trading_fee = Decimal(0)
    sum_perp_keeper_fee = Decimal(0)
    perp_settlement_fee_unmeasured = 0
    perp_keeper_fee_unmeasured = 0
    settlement_by_link = _g6_settlement_by_link(acct_events, acct_payloads)
    estimate_links = _g6_perp_estimate_links(acct_events)

    for r in ledger:
        gas = _dec(r.get("gas_usd"))
        if gas is not None:
            sum_gas += gas

    # Event order is required for the running BORROW-to-REPAY debt balance.
    for r in acct_events:
        p = acct_payloads.get(r.get("id"), {})
        et = r.get("event_type")
        rpnl = _dec(p.get("realized_pnl_usd"))
        if et == "SWAP":
            # Prefer matched-portion PnL for partial swaps; legacy payloads fall back.
            matched = _dec(p.get("realized_pnl_usd_matched"))
            rpnl_swap = matched if matched is not None else rpnl
            amt_in_usd = _dec(p.get("amount_in_usd"))
            if rpnl_swap is not None:
                sum_swap += rpnl_swap
            elif amt_in_usd is not None:
                # Measured amount with no prior lot is a valid no-prior-basis state.
                no_prior_basis_swap += 1
            else:
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
                null_lp_fees += 1
            elif fees_usd is not None:
                sum_fees += fees_usd
            amt0_usd = _dec(p.get("amount0_usd"))
            amt1_usd = _dec(p.get("amount1_usd"))
            if amt0_usd is not None:
                notional_traded += abs(amt0_usd)
            if amt1_usd is not None:
                notional_traded += abs(amt1_usd)
            # N-coin LPs expose aggregate cost basis instead of two token notionals.
            # Fall back only when both token amounts are absent to avoid double counting.
            if amt0_usd is None and amt1_usd is None:
                lp_cost_basis = _dec(p.get("cost_basis_usd"))
                if lp_cost_basis is not None:
                    notional_traded += abs(lp_cost_basis)
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
                # Partial-repay rounding must not create negative outstanding debt.
                if debt_outstanding < Decimal(0):
                    debt_outstanding = Decimal(0)
            amt_usd = _dec(p.get("amount_usd"))
            if amt_usd is not None:
                notional_traded += abs(amt_usd)
        if et == "PERP_CLOSE":
            # Measured settlement components supersede submission estimates. A None
            # settlement field never replaces a measured estimate with zero.
            s = settlement_by_link.get(str(r.get("ledger_entry_id") or ""))
            s_funding = _dec(s.get("funding_fee_usd")) if s else None
            if s_funding is not None:
                # funding_fee_usd is signed and paid-positive; PnL contribution is opposite.
                sum_funding -= s_funding
            else:
                funding_p = _dec(p.get("funding_paid_usd"))
                funding_r = _dec(p.get("funding_received_usd"))
                # Neither funding field means unmeasured; two zeros are measured zero.
                if funding_p is None and funding_r is None:
                    null_perp_funding += 1
                if funding_p is not None:
                    sum_funding -= funding_p
                if funding_r is not None:
                    sum_funding += funding_r
            s_rpnl = _dec(s.get("realized_pnl_usd")) if s else None
            if s_rpnl is not None:
                sum_perp += s_rpnl
            elif rpnl is None:
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
        if et == "PERP_SETTLEMENT":
            # Keeper and trading costs exist only on the settlement row and must be
            # included in the same wallet window without submission supersession.
            trading_fee, keeper_fee = _g6_settlement_cost_usd(p)
            # Trading fees require a fill; keeper fees consume escrow for every outcome.
            if p.get("settlement_state") == "EXECUTED":
                if trading_fee is None:
                    perp_settlement_fee_unmeasured += 1
                else:
                    sum_perp_trading_fee += trading_fee
            if keeper_fee is None:
                perp_keeper_fee_unmeasured += 1
            else:
                sum_perp_keeper_fee += keeper_fee
            # Orphan settlements contribute economics directly; linked ones were merged above.
            link = str(p.get("submission_ledger_entry_id") or r.get("ledger_entry_id") or "")
            if p.get("settlement_state") == "EXECUTED" and link not in estimate_links:
                orphan_rpnl = _dec(p.get("realized_pnl_usd"))
                if orphan_rpnl is not None:
                    sum_perp += orphan_rpnl
                orphan_funding = _dec(p.get("funding_fee_usd"))
                if orphan_funding is not None:
                    sum_funding -= orphan_funding
        # PT buys establish basis but no PnL; disposals contribute realized yield,
        # never gross proceeds. Missing USD price is distinct from missing quantities,
        # and measured no-prior-basis disposals remain non-failing diagnostics.
        if et in ("PT_SELL", "PT_REDEEM"):
            rpnl_pt = _dec(p.get("realized_yield_usd"))
            ryield_sy = _dec(p.get("realized_yield_sy"))
            sy_amount = _dec(p.get("sy_amount"))
            sy_price = _dec(p.get("sy_price"))
            if rpnl_pt is not None:
                sum_swap += rpnl_pt
            elif ryield_sy is not None and sy_price is None:
                null_pt_realized_usd += 1
            elif ryield_sy is not None:
                null_pt_amount += 1
            elif sy_amount is not None:
                no_prior_basis_pt += 1
            else:
                null_pt_amount += 1
            if sy_amount is not None and sy_price is not None:
                notional_traded += abs(sy_amount * sy_price)
        if et == "PT_BUY":
            sy_amount = _dec(p.get("sy_amount"))
            sy_price = _dec(p.get("sy_price"))
            if sy_amount is not None and sy_price is not None:
                notional_traded += abs(sy_amount * sy_price)

    # Untraded inventory revaluation is a component term and must use the same
    # persisted endpoint marks as wallet PnL to avoid price-time skew.
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
    # Preserve an unmeasured inventory total as a failing null bucket, not measured zero.
    sum_inventory_reval = inv.total_usd if inv.total_usd is not None else Decimal(0)
    null_inventory_reval = 0 if inv.total_usd is not None else 1

    # Perp fees are costs; keep their positive subtotals separate but negate them into net fees.
    sum_fees -= sum_perp_trading_fee
    sum_fees -= sum_perp_keeper_fee
    # G6 does not yet refuse wallet endpoints during in-flight native escrow, so
    # such a window can report a loud residual even when these cost terms are correct.

    component_pnl = sum_swap + sum_lp + sum_perp + sum_fees + sum_funding + sum_interest - sum_gas
    component_pnl += sum_inventory_reval

    # Tolerance is the greater of a $0.10 noise floor and a primitive-specific
    # percentage of traded notional, debt high-water mark, or perp exposure.
    floor = Decimal("0.10")
    eps_floor_label = "$0.10"
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

    # A scaled tolerance at least as large as capital makes the cell unfalsifiable.
    # Test zero-capital runs too; only the diagnostic ratio requires a nonzero divisor.
    eps_scaled = eps_pct * scaling_base
    eps_vacuous = eps_scaled > 0 and eps_scaled >= capital
    eps_over_capital = (eps_scaled / capital) if capital > 0 else None

    null_breakdown = {
        "Σ_swaps_usd_null_count": null_swap_rpnl,
        "Σ_lp_usd_null_count": null_lp_close_rpnl,
        "Σ_lp_fees_null_count": null_lp_fees,
        "Σ_perp_usd_null_count": null_perp_rpnl,
        "Σ_funding_usd_null_count": null_perp_funding,
        "Σ_interest_supply_null_count": null_withdraw_interest,
        "Σ_interest_borrow_null_count": null_repay_interest,
        # Missing marks or basis make inventory revaluation unmeasured, not zero.
        "Σ_inventory_reval_usd_null_count": null_inventory_reval,
        # A matched PT lot with no USD price is the profile's narrow XFAIL case.
        "Σ_pt_realized_usd_null_count": null_pt_realized_usd,
        # Missing PT amounts are data loss and never qualify for the price-gap waiver.
        "Σ_pt_amount_null_count": null_pt_amount,
    }
    has_nulls = any(v > 0 for v in null_breakdown.values())

    # Wallet and component methods measure different intervals if ledger activity
    # predates the first priced snapshot.
    coverage = _baseline_window_coverage(priced[0], ledger)
    window_uncovered = coverage.measurable and not coverage.covers
    # Subtract pre-window gas from the signed discrepancy; using abs(gap) first
    # loses direction and can falsely explain an opposite-signed books error.
    # This does not account for pre-window realized economics, so an unexplained
    # result may remain conservative. A vacuous epsilon cannot authorize a waiver.
    window_gas = coverage.gas_before_measured
    signed_delta = wallet_pnl - component_pnl
    window_residual = None if window_gas is None else abs(signed_delta - window_gas)
    window_explained = not eps_vacuous and window_residual is not None and window_residual <= eps

    decomp = {
        "wallet_pnl_usd": str(wallet_pnl),
        "component_pnl_usd": str(component_pnl),
        "Σ_swaps_usd": str(sum_swap),
        # Measured swaps without prior basis are diagnostic, not null inputs.
        "Σ_swaps_no_prior_basis_count": str(no_prior_basis_swap),
        # Measured PT disposals without prior basis follow the same rule.
        "Σ_pt_no_prior_basis_count": str(no_prior_basis_pt),
        "Σ_lp_usd": str(sum_lp),
        "Σ_perp_usd": str(sum_perp),
        "Σ_fees_usd": str(sum_fees),
        # These subtotals are positive costs; Σ_fees_usd carries them negated.
        "Σ_perp_trading_fee_usd": str(sum_perp_trading_fee),
        "Σ_perp_keeper_fee_usd": str(sum_perp_keeper_fee),
        # Legacy settlements omit these fields, so missing-fee counts remain forensic
        # rather than failing until the persisted corpus can support strictness.
        "Σ_perp_trading_fee_unmeasured_count": str(perp_settlement_fee_unmeasured),
        "Σ_perp_keeper_fee_unmeasured_count": str(perp_keeper_fee_unmeasured),
        "Σ_funding_usd": str(sum_funding),
        "Σ_interest_usd": str(sum_interest),
        "Σ_gas_usd": str(-sum_gas),
        # Ambient inventory revaluation is empty, not zero, when unmeasured.
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
        # Empty ratio means no nonzero capital denominator was available.
        "ε_vacuous": str(eps_vacuous),
        "ε_scaled_over_capital": ("" if eps_over_capital is None else str(eps_over_capital)),
        "il_diagnostic_usd_NOT_in_PnL": str(il_diagnostic),
        # Empty coverage means endpoint chronology was unmeasured.
        "initial_endpoint_covers_run": ("" if not coverage.measurable else str(coverage.covers)),
        "initial_snapshot_cycle_id": priced[0].get("cycle_id") or "",
        "ledger_rows_before_initial_endpoint": str(coverage.rows_before),
        # Empty gas total means chronology or at least one pre-window gas value was unmeasured.
        "gas_usd_before_initial_endpoint": ("" if window_gas is None else str(window_gas)),
        "gas_usd_before_initial_endpoint_unmeasured_count": str(coverage.gas_before_unmeasured_rows),
        # Empty residual means the explained portion could not be computed.
        "window_residual_usd": ("" if window_residual is None else str(window_residual)),
        "ledger_rows_without_timestamp": str(coverage.rows_without_timestamp),
        # Positive deployed value without readable legs remains diagnostic-only.
        "unreadable_payload_rows": str(_unreadable_payload_rows(priced)),
        # Missing same-reserve debt siblings expose partially discovered leveraged positions.
        "missing_debt_leg_rows": str(_missing_debt_leg_rows(priced)),
        **{k: str(v) for k, v in null_breakdown.items()},
    }

    # Any required null makes reconciliation operate on unmeasured zero.
    if has_nulls:
        nonzero = {k: v for k, v in null_breakdown.items() if v > 0}
        # The profile may waive only an unavailable PT USD projection; every other
        # null bucket, including missing PT quantities, remains a failure.
        if (
            _profile.disposal_usd_unmeasured_is_xfail
            and null_pt_realized_usd > 0
            and set(nonzero) == {"Σ_pt_realized_usd_null_count"}
        ):
            return (
                CellResult(
                    "G6",
                    "Reconciliation",
                    "XFAIL",
                    f"PT disposal realized-PnL USD unmeasured: sy_price=None on "
                    f"{null_pt_realized_usd} PT_SELL/PT_REDEEM row(s) (VIB-5276 gateway "
                    f"PT/SY price); wallet=${wallet_pnl} component=${component_pnl} "
                    f"gap=${gap} — measured-but-blocked, not a books error. "
                    "Flips to PASS once the sell-side SY price lands.",
                    decomposition=decomp,
                ),
                decomp,
            )
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
    # Prefer the null diagnosis, then block only the otherwise-PASS vacuous case.
    if eps_vacuous and gap <= eps:
        return (
            CellResult(
                "G6",
                "Reconciliation",
                "FAIL",
                f"tolerance is vacuous: notional-scaled ε=${eps_scaled} >= capital=${capital} "
                f"({'ratio=' + str(eps_over_capital) + 'x' if eps_over_capital is not None else 'capital is zero — ANY positive ε is vacuous'})"
                f" — G6 cannot fail for ANY input at this ε, so a PASS "
                f"would verify nothing. Root-cause the scaling base "
                f"(${scaling_base} via {scaling_label}) before trusting this cell; a notional that "
                f"exceeds capital usually means mis-scaled leg amounts, not real volume. "
                f"wallet=${wallet_pnl} component=${component_pnl} gap=${gap}",
                decomposition=decomp,
            ),
            decomp,
        )
    # An uncovered wallet window cannot support PASS. Waive only when measured
    # pre-window spend explains the discrepancy; unrelated residue must still fail.
    # Do not window gas alone because component economics would remain unwindowed.
    if window_uncovered and window_explained:
        return (
            CellResult(
                "G6",
                "Reconciliation",
                "XFAIL",
                f"wallet window does not cover the run: {coverage.rows_before} ledger row(s) "
                f"predate the initial endpoint (earliest {coverage.earliest_before_ts}, "
                f"baseline late by {coverage.late_by}), carrying ${window_gas} "
                f"of gas the wallet method cannot see — which accounts for the gap to within "
                f"${window_residual} (ε=${eps}). wallet=${wallet_pnl} "
                f"component=${component_pnl} gap=${gap} — the endpoints measure different "
                "intervals, so this gap is not a books error. Needs the producer-side "
                "pre-trade boot snapshot (VIB-5854); do NOT window Σ_gas to match.",
                decomposition=decomp,
            ),
            decomp,
        )
    if window_uncovered:
        # Residual is a signed-discrepancy calculation, not a portion of gap or spend.
        # Distinguish an unfalsifiable threshold, a measured excess, and unmeasured gas.
        if eps_vacuous and window_residual is not None and window_residual <= eps:
            # A residual beyond even a vacuous epsilon is still a definite failure.
            unexplained = (
                f"whether that spend explains the discrepancy cannot be tested: the "
                f"tolerance is vacuous (notional-scaled ε=${eps_scaled} >= capital="
                f"${capital}), so residual=${window_residual} against ε=${eps} compares "
                "nothing"
            )
            headline = "This cell cannot be decided."
            closing = (
                f"Root-cause the ε scaling base (${scaling_base} via {scaling_label}) "
                "first — while ε is vacuous neither the residue nor the gap can be judged."
            )
        elif window_residual is not None:
            unexplained = f"the discrepancy is not explained by that spend (residual=${window_residual} > ε=${eps})"
            headline = "TWO defects on one run."
            closing = "Fixing the baseline alone will NOT close this cell — reconcile the residue as an ordinary gap."
        else:
            unexplained = (
                f"the pre-baseline gas is unmeasured on "
                f"{coverage.gas_before_unmeasured_rows} row(s), so how much of the gap it "
                "explains cannot be established"
            )
            headline = "This cell cannot be decided."
            closing = (
                f"Populate gas_usd on the {coverage.gas_before_unmeasured_rows} pre-baseline "
                "row(s) before reading anything into this gap."
            )
        return (
            CellResult(
                "G6",
                "Reconciliation",
                "FAIL",
                f"{headline} (1) The wallet window does not cover the run: "
                f"{coverage.rows_before} ledger row(s) predate the initial endpoint "
                f"(earliest {coverage.earliest_before_ts}, baseline late by "
                f"{coverage.late_by}), carrying "
                f"{'$' + str(window_gas) if window_gas is not None else 'an unmeasured amount'} "
                f"of gas the wallet method cannot see — needs the producer-side pre-trade "
                f"boot snapshot (VIB-5854). (2) {unexplained}: wallet=${wallet_pnl} "
                f"component=${component_pnl} gap=${gap} (ε=${eps}). {closing}",
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

    "Equity" here = ``_snapshot_equity`` = ``total_value_usd − debt_mark +
    available_cash_usd`` (VIB-5857 netted the debt term; VIB-3614 split
    deployed from cash into separate columns). A
    post-teardown snapshot with ``total_value_usd=0`` is *not* a missing
    measurement — every position closed cleanly and the equity collapsed
    into ``available_cash_usd``. Treating that as null double-counts
    teardown success as an accounting failure.

    The cell fails when **equity itself** is missing — both columns are
    unmeasured — or explicitly ``UNAVAILABLE``. Pure cash-only is a valid
    measured equity curve point.
    """
    if not snapshots:
        return CellResult("G8", "Time-series (equity curve)", "FAIL", "no snapshots")

    confidence_unmeasured = sum(not _snapshot_confidence_can_anchor_pnl(s) for s in snapshots)
    equity_unmeasured = sum(_snapshot_equity(s) is None for s in snapshots)
    unmeasured = sum(not _snapshot_confidence_can_anchor_pnl(s) or _snapshot_equity(s) is None for s in snapshots)
    if unmeasured:
        return CellResult(
            "G8",
            "Time-series",
            "FAIL",
            f"{unmeasured}/{len(snapshots)} snapshots have unmeasured equity "
            f"({confidence_unmeasured} with missing/invalid confidence; "
            f"{equity_unmeasured} with null/invalid equity columns)",
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
        # Use raw assets and debt: zero net equity can still contain measured USD exposure.
        deployed = _dec(s.get("total_value_usd"))
        cash = _dec(s.get("available_cash_usd"))
        bears_usd = (
            (deployed is not None and deployed != 0)
            or (cash is not None and cash != 0)
            # Confidence on a liability must not depend on legacy net-shape correction.
            or net_debt_from_positions_json(s.get("positions_json"))[1] != 0
        )
        if bears_usd and not s.get("value_confidence"):
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
    by_intent: dict[Any, int] = {}
    for r in ledger:
        # In-flight rows have no stable transaction identity and must not collide.
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

    # A dispatched cycle must not mix landed and reverted transactions.
    cycles: dict[Any, list[dict[str, Any]]] = {}
    for r in ledger:
        if not r.get("tx_hash"):
            continue
        cyc = r.get("cycle_id")
        if cyc is None or cyc == "":
            continue
        cycles.setdefault(cyc, []).append(r)

    # Use chain outcome, not the books verdict; accounting-degraded rows still landed.
    mixed: list[tuple[Any, int, int]] = []  # (cycle_id, landed_count, reverted_count)
    for cyc, rs in cycles.items():
        if len(rs) < 2:
            continue
        successes = sum(1 for r in rs if _row_landed(r))
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
    # Accounting-degraded rows landed and do not exercise the failed-intent contract.
    failed = [r for r in ledger if not _row_landed(r)]
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
    # Reject non-object roots before _json can collapse them to an empty mapping.
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
        # Protocol-aware resolution keeps LP v3 and v4 matching versions independent.
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

    # Count null, omitted, and invalid deltas separately; only compared rows can pass.
    # The threshold assumes the producer emits a fraction, not percentage points.
    eps_pct = Decimal("0.0001")  # 1 bp as a fraction
    bad: list[tuple[Any, Decimal]] = []
    compared = 0
    null_rows = 0
    empty_rows = 0
    unparseable_rows = 0
    for row in position_state_rows:
        raw = row.get("delta_vs_protocol_pct")
        if raw is None:
            null_rows += 1
            continue
        if isinstance(raw, str) and raw.strip() == "":
            empty_rows += 1
            continue
        try:
            delta = Decimal(str(raw))
        except (InvalidOperation, ValueError, TypeError):
            unparseable_rows += 1
            continue
        # NaN is unreadable; infinity remains comparable and must fail the bound.
        if delta.is_nan():
            unparseable_rows += 1
            continue
        compared += 1
        if abs(delta) > eps_pct:
            bad.append((row.get("position_key") or row.get("id"), delta))
    unread = f"unmeasured={null_rows} not-emitted={empty_rows} unparseable={unparseable_rows}"
    decomp = {
        "rows_present": str(len(position_state_rows)),
        "rows_compared": str(compared),
        "rows_null": str(null_rows),
        "rows_empty": str(empty_rows),
        "rows_unparseable": str(unparseable_rows),
    }
    if bad:
        sample = bad[:3]
        return CellResult(
            "G14",
            "SDK ≡ on-chain reconciliation",
            "FAIL",
            f"{len(bad)} of {compared} compared position_state rows exceed 1bp delta "
            f"vs on-chain (e.g. {sample!r}); {unread}",
            decomposition=decomp,
        )
    if compared == 0:
        # Present but wholly unreadable rows are unmeasurable, not a successful comparison.
        return CellResult(
            "G14",
            "SDK ≡ on-chain reconciliation",
            "XFAIL",
            f"{len(position_state_rows)} position_state row(s) present but 0 carried a "
            f"comparable delta_vs_protocol_pct ({unread}) — nothing was compared against "
            "on-chain state, so this cell asserts nothing. Needs a producer that computes "
            "the delta (declared position_state.py:87, written only gateway-side today).",
            decomposition=decomp,
        )
    return CellResult(
        "G14",
        "SDK ≡ on-chain reconciliation",
        "PASS",
        f"{compared} of {len(position_state_rows)} position_state rows compared and within "
        f"1bp of on-chain state; {unread}",
        decomposition=decomp,
    )


# Track C re-reads protocol positions, not wallet token inventory. Direct enum
# values are included because the alias resolver handles protocol names instead.
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
    # Accept a direct primitive enum value when no protocol alias resolves.
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

    snapshot_position_counts: dict[Any, int] = {}
    unreadable_snapshots: list[Any] = []
    for s in snapshots:
        positions_json = s.get("positions_json")
        if not positions_json or positions_json == "[]":
            continue
        try:
            parsed = json.loads(positions_json)
        except (json.JSONDecodeError, TypeError):
            # Unreadable position data cannot prove a cash-only snapshot.
            unreadable_snapshots.append(s.get("id"))
            continue
        # Accept the legacy list and versioned-envelope writer shapes.
        if isinstance(parsed, list):
            positions = parsed
        elif isinstance(parsed, dict) and isinstance(parsed.get("positions"), list):
            positions = parsed["positions"]
        else:
            unreadable_snapshots.append(s.get("id"))
            continue
        # Wallet inventory is not a Track-C protocol position.
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
        # Track C is present but there are no eligible open positions to reconcile.
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
        # Over-coverage can indicate a duplicate write and fails like under-coverage.
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

    if not position_reference_column_present:
        return CellResult(
            cell_id,
            description,
            "XFAIL",
            "accounting_events.position_reference column missing (pre-T10 DB); cell cannot evaluate",
        )

    if malformed_position_reference_row_ids:
        sample = malformed_position_reference_row_ids[:5]
        return CellResult(
            cell_id,
            description,
            "FAIL",
            f"{len(malformed_position_reference_row_ids)} accounting_events row(s) carry malformed "
            f"position_reference JSON (e.g. ids={sample!r}); corrupt payloads contaminate the audit trail",
        )

    # The close-event census is required even when the registry is absent.
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
        return CellResult(
            cell_id,
            description,
            "XFAIL",
            f"position_registry table absent and {close_events_legacy_null_hash} CLOSE event(s) "
            f"carry only legacy position_reference (physical_identity_hash=null); registry mode "
            f"not yet on for any primitive in this run",
        )

    # Sort by identity hash for deterministic diagnostics.
    closed_registry_rows = sorted(
        (r for r in registry_rows if r.get("status") == "closed"),
        key=lambda r: r.get("physical_identity_hash") or "",
    )
    closed_registry_phids: set[str] = {
        r["physical_identity_hash"] for r in closed_registry_rows if r.get("physical_identity_hash")
    }

    # Carry the parsed hash so diagnostics do not reparse position_reference.
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

    if close_events_with_hash == 0 and not closed_registry_phids:
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


# Allow large IL while rejecting values beyond twice the position reference scale.
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


# LP5 requires every close-attribution leg emitted by attribute_lp.
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
    has_ticks = any(r.get("tick_lower") is not None and r.get("tick_upper") is not None for r in pos_events)
    out.append(
        CellResult(
            "LP1",
            "Range exposure (tick_lower/upper/current_tick at every snapshot)",
            "PASS" if has_ticks else "FAIL",
            "found tick_lower/upper on position_events" if has_ticks else "no position_events row carries ticks",
        )
    )
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
    fees_seen = any(r.get("fees_token0") or r.get("fees_token1") for r in pos_events)
    out.append(
        CellResult(
            "LP3",
            "Fees earned per position",
            "PASS" if fees_seen else "FAIL",
            "position_events.fees_token0/1 populated" if fees_seen else "no fees_token0/1 on any position_event",
        )
    )
    lp_acct = [r for r in acct_events if r.get("event_type") in ("LP_OPEN", "LP_CLOSE")]
    blocked = _payload_block_cell("LP4", "Impermanent loss (diagnostic, NOT in net PnL)", lp_acct, payload_errors)
    if blocked is not None:
        out.append(blocked)
    else:
        out.append(_lp4_il_sanity_cell(lp_acct, acct_payloads))
    out.append(_lp5_decomposition_cell(pos_events))
    if lp_state_rows:
        # SQLite can return liquidity zero as either text or integer.
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
    *,
    require_loop_leg_attribution: bool = True,
) -> list[CellResult]:
    position_state_rows = position_state_rows or []
    lending_state_rows = [r for r in position_state_rows if r.get("position_type") == "LENDING"]
    out: list[CellResult] = []
    lending_acct = [r for r in acct_events if r.get("event_type") in ("WITHDRAW", "REPAY", "DELEVERAGE")]
    blocked = _payload_block_cell("L1", "Net carry (supply_int − borrow_int)", lending_acct, payload_errors)
    # Payload errors do not invalidate independent Track-C cells.
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
        if require_loop_leg_attribution:
            out.append(
                CellResult(
                    "L6",
                    "Loop-leg attribution",
                    "FAIL",
                    "lending payload(s) failed Pydantic validation; cell data unusable",
                )
            )
    if not payload_blocked:
        # Preserve measured zero interest and treat DELEVERAGE as REPAY-class.
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
    # Accept projected and legacy principal/interest names. At least one REPAY
    # must carry both because a row without a matching BORROW lot may be unavailable.
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
    # Applicability comes from the profile, but an observed borrow-to-swap leg is
    # always scored so a misdeclared profile cannot hide missing attribution.
    observed_borrow_assets = {
        str((acct_payloads.get(r.get("id"), {}) or {}).get("asset") or "").upper()
        for r in acct_events
        if r.get("event_type") == "BORROW"
    }
    observed_borrow_assets.discard("")
    observed_matching_swap = any(
        r.get("event_type") == "SWAP"
        and str((acct_payloads.get(r.get("id"), {}) or {}).get("token_in") or "").upper() in observed_borrow_assets
        for r in acct_events
    )
    if not require_loop_leg_attribution and not observed_matching_swap:
        out.append(
            CellResult(
                "L6",
                "Loop-leg attribution",
                "SKIP",
                "not applicable to the explicit lending-lifecycle profile (no borrow→swap leg in its contract)",
            )
        )
        return out

    # A complete loop needs BORROW, matching asset disposal, REPAY, and fully
    # attributed PnL on every matching disposal.
    if not payload_blocked:
        # Validate BORROW and SWAP payloads before classifying loop completeness.
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
        # Side trades are outside the loop-leg PnL invariant.
        loop_leg_payloads = [p for p in swap_payloads if (p.get("token_in") or "").upper() in borrow_assets]
        # L6 intentionally requires full realized_pnl_usd, unlike G6's matched-portion
        # precedence: a partial loop match signals a missing basis credit.
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
    # Position events use OPEN/CLOSE plus position_type; accounting events use PERP_*.
    perp_pos = [r for r in pos_events if r.get("position_type") == PositionType.PERP]
    has_open = any(r.get("event_type") == PositionEventType.OPEN for r in perp_pos)
    has_close = any(r.get("event_type") == PositionEventType.CLOSE for r in perp_pos)
    out.append(
        CellResult(
            "P1",
            "Position lifecycle (size, leverage, direction, entry/exit price)",
            # OPEN carries the entry economics; CLOSE-only cannot prove the lifecycle.
            "PASS" if has_open else "XFAIL",
            f"OPEN={has_open} CLOSE={has_close} on {len(perp_pos)} PERP position_events row(s)",
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
    # Async fees and realized PnL are measured only after execution. Join an EXECUTED
    # settlement to its submission by ledger_entry_id and prefer its economics.
    settlement_by_link: dict[str, dict[str, Any]] = {}
    for r in acct_events:
        if r.get("event_type") != "PERP_SETTLEMENT":
            continue
        sp = acct_payloads.get(r.get("id"), {})
        if sp.get("settlement_state") != "EXECUTED":
            continue
        link = sp.get("submission_ledger_entry_id") or r.get("ledger_entry_id")
        if link:
            settlement_by_link[str(link)] = sp

    def _linked_settlement(row: dict[str, Any]) -> dict[str, Any]:
        return settlement_by_link.get(str(row.get("ledger_entry_id") or ""), {})

    perp_acct = [r for r in acct_events if r.get("event_type") in ("PERP_OPEN", "PERP_CLOSE")]
    blocked_p3 = _payload_block_cell("P3", "Open + close fees + price impact (separable)", perp_acct, payload_errors)
    if blocked_p3 is not None:
        out.append(blocked_p3)
    else:
        has_fee_split = False
        via_settlement = False
        for r in perp_acct:
            p = acct_payloads.get(r.get("id"), {})
            s = _linked_settlement(r)
            # Settlement economics supersede stale inline submission values.
            if s.get("position_fee_usd") is not None:
                has_fee_split = True
                via_settlement = True
                break
            if p.get("open_fee_usd") is not None or p.get("close_fee_usd") is not None:
                has_fee_split = True
                break
        out.append(
            CellResult(
                "P3",
                "Open + close fees + price impact (separable)",
                "PASS" if has_fee_split else "XFAIL",
                (
                    "measured fee in PERP_SETTLEMENT.position_fee_usd"
                    if via_settlement
                    else "fee fields in PERP_*_PAYLOAD"
                )
                if has_fee_split
                else "fee fields not yet populated (no inline fee, no EXECUTED settlement)",
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
        via_settlement = False
        for r in perp_close_acct:
            p = acct_payloads.get(r.get("id"), {})
            s = _linked_settlement(r)
            # Settlement economics supersede stale inline submission values.
            if s.get("realized_pnl_usd") is not None:
                has_realized = True
                via_settlement = True
                break
            if p.get("realized_pnl_usd") is not None:
                has_realized = True
                break
        out.append(
            CellResult(
                "P5",
                "Realised PnL with funding/fees decomposition",
                "PASS" if has_realized else "XFAIL",
                (
                    "measured PERP_SETTLEMENT.realized_pnl_usd"
                    if via_settlement
                    else "PERP_CLOSE.realized_pnl_usd present"
                )
                if has_realized
                else "realized_pnl_usd null/missing (no inline PnL, no EXECUTED settlement)",
            )
        )
    out.append(CellResult("P6", "Margin utilisation over time", "XFAIL", "Track C"))
    return out


def _open_pt_inventory_rows(snapshots: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], bool, bool]:
    """Open held-PT inventory rows across all snapshots, time-ordered.

    The portfolio valuer surfaces an open Pendle PT as a synthetic
    ``positions_json`` row tagged ``details.source == "pt_inventory_lots"``
    (``_classify_pt_inventory`` / ``_pt_unmeasured_row``, VIB-5316). PEN3 reads
    those rows to score open-PT mark-to-market — a real measurement now that the
    gateway PT implied-price path is wired (VIB-5276), not a hardcoded XFAIL.

    Returns ``(rows, any_unreadable, unorderable)``. ``rows`` are the matching
    position dicts ordered oldest→newest through the shared chronological
    authority ``_snapshots_in_time_order`` (VIB-6545: the former
    ``(iteration_number, timestamp)`` key mis-elects "latest" on any restarted
    run, because a restart resets ``iteration_number``) so the caller can take
    the LATEST snapshot's mark with ``rows[-1]``. ``any_unreadable`` is True when
    ≥1 snapshot's ``positions_json`` is malformed — surfaced as a diagnostic note
    (Empty ≠ Zero / VIB-3891: unreadable JSON is NOT "no PT", it is simply
    unknown here; the hard coverage failure for it lives in G15, not PEN3).
    ``unorderable`` is True when the snapshots cannot be time-ordered at all —
    then ``rows`` is empty and the caller must refuse to claim a "latest" mark
    rather than electing one by row order.

    Parses ``positions_json`` with the same accept-two-shapes discipline as the
    Track-C cells (legacy plain list OR versioned envelope ``{"positions": [...]}``).

    The ``"pt_inventory_lots"`` marker is the data-shape contract (VIB-4636
    discipline — detect by marker, never by a protocol-name string); the accounting
    layer matches it as a literal so it does not import the valuer (mirrors the
    ``swap_inventory_lots`` literal used by the Track-C cells above). Source of
    truth: ``portfolio_valuer._PT_INVENTORY_SOURCE``.
    """
    out: list[dict[str, Any]] = []
    any_unreadable = False
    ordered = _snapshots_in_time_order(snapshots)
    if ordered is None:
        # any_unreadable is undefined here; the caller must branch on unorderable first.
        return [], False, True
    for s in ordered:
        positions_json = s.get("positions_json")
        if not positions_json or positions_json == "[]":
            continue
        try:
            parsed = json.loads(positions_json)
        except (json.JSONDecodeError, TypeError):
            any_unreadable = True
            continue
        if isinstance(parsed, list):
            positions = parsed
        elif isinstance(parsed, dict) and isinstance(parsed.get("positions"), list):
            positions = parsed["positions"]
        else:
            any_unreadable = True
            continue
        for p in positions:
            if not isinstance(p, dict):
                continue
            details = p.get("details")
            if isinstance(details, dict) and details.get("source") == "pt_inventory_lots":
                out.append(p)
    return out, any_unreadable, False


def _pen3_open_pt_cell(snapshots: list[dict[str, Any]]) -> CellResult:
    """PEN3 — open-PT mark-to-market (unrealised discount accretion).

    The gateway PT implied-price path (VIB-5276:
    ``PT/USD = pt_to_asset_rate × underlying/USD``) is wired through
    ``MarketSnapshot.pt_price`` into the portfolio valuer (VIB-5313 reprice +
    VIB-5316 FIFO-inventory consumer), which surfaces an open PT as a
    ``pt_inventory_lots`` row in ``portfolio_snapshots.positions_json``. This cell
    reads those rows — a real measurement, no longer a hardcoded XFAIL.

    Empty ≠ Zero (blueprint 27 §L2 mark-to-market, line ~700; CLAUDE.md
    §Accounting spine §3.3): the measured/unmeasured discriminators are the
    ``details.*_unmeasured`` flags the valuer sets — NOT ``price_confidence``
    (STALE / ESTIMATED still carry a real ``value_usd`` mark). The cell's claim is
    the *unrealised discount accretion*, so it PASSes only when ALL THREE of the
    mark, the cost basis, and the unrealised PnL are measured. ``_classify_pt_inventory``
    can mark the row priced (``mark_unmeasured`` absent) yet flag
    ``cost_basis_unmeasured`` / ``unrealized_pnl_unmeasured`` when the buy-time USD
    cost leg is missing — in that case the persisted ``cost_basis_usd`` /
    ``unrealized_pnl_usd`` are placeholder ``0`` (Empty ≠ Zero), so a PASS here
    would publish a fabricated zero unrealised PnL. PEN3 XFAILs instead.

    * measured open-PT row (USD mark AND cost_basis AND unrealized PnL) → **PASS**;
    * row priced but cost / unrealised-PnL unmeasured (buy-time USD leg missing) →
      **XFAIL** (the discount-accretion claim is unmeasured, not a fabricated 0);
    * honest-unmeasured row (gateway price UNAVAILABLE on this fork) → **XFAIL**
      citing the row's ``unavailable_reason`` (capability present, not priceable
      here — absent, not wrong);
    * no open-PT inventory row in any snapshot → **XFAIL** (nothing to mark).

    There is NO FAIL branch: an unmeasured / absent mark is Empty ≠ Zero, never a
    failure. The decision is taken on the LATEST snapshot bearing a PT row (rows
    are time-ordered, VIB-6545) — PT inventory exists only while a lot is open, so
    the most recent such row is the freshest unrealised-discount mark.
    """
    name = "Open-PT mark-to-market (unrealised discount accretion)"
    pt_rows, pt_unreadable, pt_unorderable = _open_pt_inventory_rows(snapshots)
    if pt_unorderable:
        # Latest is a chronological claim; unorderable marks are unmeasurable.
        return CellResult(
            "PEN3",
            name,
            "XFAIL",
            "snapshots cannot be ordered by time (a timestamp is unmeasured or "
            "unparseable), so the latest open-PT mark cannot be identified (VIB-6545)",
        )
    if not pt_rows:
        note = "; NOTE: malformed positions_json on ≥1 snapshot" if pt_unreadable else ""
        return CellResult(
            "PEN3",
            name,
            "XFAIL",
            f"no open-PT inventory row in any snapshot (pt_inventory_lots) — nothing to mark-to-market{note}",
        )
    latest = pt_rows[-1]
    d = latest.get("details") or {}
    sym = d.get("pt_symbol") or d.get("asset") or "PT"
    # Placeholder zeros paired with *_unmeasured flags cannot satisfy the claim.
    unmeasured = d.get("mark_unmeasured") or d.get("cost_basis_unmeasured") or d.get("unrealized_pnl_unmeasured")
    if not unmeasured:
        return CellResult(
            "PEN3",
            name,
            "PASS",
            f"open-PT {sym} marked value_usd={latest.get('value_usd')} "
            f"cost_basis_usd={latest.get('cost_basis_usd')} "
            f"unrealized_pnl_usd={latest.get('unrealized_pnl_usd')} "
            f"(confidence={d.get('price_confidence', '?')}, source={d.get('price_source', '?')})",
        )
    reason = d.get("unavailable_reason") or (
        "price_unmeasured" if d.get("mark_unmeasured") else "cost_basis_unmeasured"
    )
    return CellResult(
        "PEN3",
        name,
        "XFAIL",
        f"open-PT {sym} mark/cost unmeasured ({reason}) — Empty ≠ Zero: capability wired "
        "(VIB-5276) but the gateway price or buy-time cost basis is not measured on this run",
    )


def _cells_pendle_pt(
    acct_events: list[dict[str, Any]],
    pos_events: list[dict[str, Any]],
    snapshots: list[dict[str, Any]],
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

    out.append(_pen3_open_pt_cell(snapshots))

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

    # Keep the payload guard even though Pendle LP currently bypasses v1 validation.
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

    # Open Pendle LP valuation is unavailable, so absence is XFAIL rather than FAIL.
    out.append(
        CellResult(
            "PLP3",
            "Open-LP mark-to-market (cost basis + unrealised PnL)",
            "XFAIL",
            "Pendle-LP valuer unwired (B2: value_pendle_lp not wired into portfolio_valuer)",
        )
    )

    out.append(
        CellResult(
            "PLP4",
            "Realised PnL / fees in USD on close",
            "XFAIL",
            "Pendle LP events USD-less by design (B1: realized_yield_usd / *_usd absent on LP payload)",
        )
    )

    # Pendle LP lifecycle identity lives on position_events, not its accounting payload.
    lp_pos = [r for r in pos_events if r.get("position_type") in ("LP", "PENDLE_LP")]
    opens = [r for r in lp_pos if r.get("event_type") == "OPEN"]
    closes = [r for r in lp_pos if r.get("event_type") == "CLOSE"]
    has_open = bool(opens)
    has_close = bool(closes)
    # Empty IDs cannot establish shared lifecycle identity.
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


# Curve is an N-coin fungible LP without NFT ranges or ticks, so it uses six
# Curve-specific cells rather than concentrated-liquidity assertions. Legacy
# fixtures expose amount0/amount1 while newer payloads may expose richer coin legs.
# Measured zero is a booked leg; only None or empty text is unmeasured.
_CURVE_COIN_AMOUNT_KEYS = ("amount0", "amount1")


def _curve_funded_legs(values: dict[str, Any]) -> list[str]:
    """Names of the coin-amount legs an event/position row booked.

    Reads the generic ``amount0`` / ``amount1`` legs the frozen fixture carries.
    Empty != Zero: a key present and not ``None`` / ``""`` counts (measured zero
    included); unmeasured legs do not.
    """
    return [k for k in _CURVE_COIN_AMOUNT_KEYS if values.get(k) not in (None, "")]


def _curve1_open_legs(
    lp_opens: list[dict[str, Any]],
    acct_payloads: dict[Any, dict[str, Any]],
    payload_errors: dict[Any, str],
) -> CellResult:
    """CURVE1 — LP_OPEN books at least one funded coin leg (replaces LP1 ticks)."""
    desc = "LP_OPEN books funded per-coin legs (N-coin deposit, no tick range)"
    blocked = _payload_block_cell("CURVE1", desc, lp_opens, payload_errors)
    if blocked is not None:
        return blocked
    for r in lp_opens:
        legs = _curve_funded_legs(acct_payloads.get(r.get("id"), {}))
        if legs:
            return CellResult("CURVE1", desc, "PASS", f"LP_OPEN books funded coin legs {legs}")
    return CellResult("CURVE1", desc, "XFAIL", "no LP_OPEN carrying a funded coin leg (amount0/amount1)")


def _curve2_close_legs(
    lp_closes: list[dict[str, Any]],
    acct_payloads: dict[Any, dict[str, Any]],
    pos_events: list[dict[str, Any]],
    payload_errors: dict[Any, str],
) -> CellResult:
    """CURVE2 — LP_CLOSE books the collected coin legs (replaces LP2 in-range).

    Proportional close returns ALL N coins (no token direction); a one-coin /
    imbalanced close returns a subset. The realized collected coins land on the
    typed payload (post-#3109 N-coin legs) or, on the frozen fixture, the CLOSE
    ``position_event`` ``amount0`` / ``amount1`` — either booking the legs PASSes.
    """
    desc = "LP_CLOSE books collected coin legs (proportional / one-coin / imbalanced)"
    blocked = _payload_block_cell("CURVE2", desc, lp_closes, payload_errors)
    if blocked is not None:
        return blocked
    for r in lp_closes:
        legs = _curve_funded_legs(acct_payloads.get(r.get("id"), {}))
        if legs:
            return CellResult("CURVE2", desc, "PASS", f"LP_CLOSE books collected coin legs {legs}")
    close_pe = [r for r in pos_events if r.get("position_type") == "LP" and r.get("event_type") == "CLOSE"]
    for r in close_pe:
        legs = _curve_funded_legs(r)
        if legs:
            return CellResult("CURVE2", desc, "PASS", f"LP_CLOSE collected coin legs booked on position_event {legs}")
    return CellResult("CURVE2", desc, "XFAIL", "no LP_CLOSE collected coin legs booked")


def _curve3_fees(
    lp_events: list[dict[str, Any]],
    acct_payloads: dict[Any, dict[str, Any]],
    pos_events: list[dict[str, Any]],
    payload_errors: dict[Any, str],
) -> CellResult:
    """CURVE3 — fees measured OR explicitly UNAVAILABLE-with-reason.

    Curve fee USD valuation is unavailable by design today (no per-pool fee
    accrual feed wired). The honest bar: a measured fee leg (Empty != Zero — a
    no-time round-trip's measured-zero fee counts) PASSes; absent that, an
    explicit ``unavailable_reason`` on the payload is an honest known-unknown and
    also PASSes; only a silent gap (no fee, no reason) is XFAIL.
    """
    desc = "Fees measured OR explicitly unavailable-with-reason (Curve fee USD unavailable by design)"
    # Validate before interpreting absent fee fields as an availability state.
    blocked = _payload_block_cell("CURVE3", desc, lp_events, payload_errors)
    if blocked is not None:
        return blocked
    for r in lp_events:
        p = acct_payloads.get(r.get("id"), {})
        for k in ("fees0_collected", "fees1_collected", "fees_total_usd"):
            if p.get(k) not in (None, ""):
                return CellResult("CURVE3", desc, "PASS", f"fees measured on payload ({k})")
    for r in (r for r in pos_events if r.get("position_type") == "LP"):
        for k in ("fees_token0", "fees_token1"):
            if r.get(k) not in (None, ""):
                return CellResult("CURVE3", desc, "PASS", f"fees measured on position_event ({k}; Empty != Zero)")
    for r in lp_events:
        reason = acct_payloads.get(r.get("id"), {}).get("unavailable_reason")
        if reason:
            return CellResult(
                "CURVE3", desc, "PASS", "Curve fee USD valuation unavailable by design — explicit reason recorded"
            )
    return CellResult("CURVE3", desc, "XFAIL", "no fees measured and no unavailable_reason recorded")


def _curve4_il(
    lp_opens: list[dict[str, Any]],
    lp_closes: list[dict[str, Any]],
    acct_payloads: dict[Any, dict[str, Any]],
    payload_errors: dict[Any, str],
) -> CellResult:
    """CURVE4 — IL diagnostic when 2-sided; N/A (XFAIL) for a single-sided deposit.

    IL is only defined against a paired (>=2-coin) deposit; a single-sided Curve
    deposit has no HODL counterfactual, so IL is undefined — XFAIL, not FAIL.
    When the open booked >=2 funded coin legs, reuse the generic LP4 sanity-bound
    check (same ``il_usd`` magnitude predicate) under the CURVE4 id.
    """
    desc = "Impermanent loss (diagnostic, NOT in net PnL) — N/A for single-sided"
    lp_acct = lp_opens + lp_closes
    # Validate before inferring single-sidedness from absent legs.
    blocked = _payload_block_cell("CURVE4", desc, lp_acct, payload_errors)
    if blocked is not None:
        return blocked
    two_sided = any(len(_curve_funded_legs(acct_payloads.get(r.get("id"), {}))) >= 2 for r in lp_opens)
    if not two_sided:
        return CellResult(
            "CURVE4", desc, "XFAIL", "single-sided deposit — IL undefined (no paired HODL counterfactual)"
        )
    inner = _lp4_il_sanity_cell(lp_acct, acct_payloads)
    return CellResult("CURVE4", desc, inner.status, inner.diagnostic)


def _curve6_liquidity(lp_state_rows: list[dict[str, Any]]) -> CellResult:
    """CURVE6 — LP-token balance over time IS the liquidity measure (replaces LP6).

    Fungible Curve LP has no concentrated-liquidity ticks: the LP-token balance
    over the hold IS the position's liquidity. Non-zero LP balance on the Track-C
    ``position_state_snapshots`` PASSes. (Empty != Zero: numeric / string ``0`` is
    a genuinely-zero balance, not a measured liquidity.)
    """
    desc = "LP-token balance over time (fungible: LP balance IS the liquidity measure)"
    if not lp_state_rows:
        return CellResult("CURVE6", desc, "XFAIL", "no LP rows in position_state_snapshots")
    liq_rows = [r for r in lp_state_rows if r.get("liquidity") not in (None, "", "0", 0)]
    if liq_rows:
        return CellResult(
            "CURVE6", desc, "PASS", f"{len(liq_rows)}/{len(lp_state_rows)} LP rows carry non-zero LP-token balance"
        )
    return CellResult(
        "CURVE6",
        desc,
        "FAIL",
        f"{len(lp_state_rows)} LP track-c rows but none has non-zero LP-token balance",
    )


def _cells_curve_lp(
    acct_events: list[dict[str, Any]],
    pos_events: list[dict[str, Any]],
    acct_payloads: dict[Any, dict[str, Any]],
    payload_errors: dict[Any, str],
    position_state_rows: list[dict[str, Any]] | None = None,
) -> list[CellResult]:
    """CURVE1–CURVE6 — the bespoke Curve LP scorecard (VIB-5430).

    See the module comment above this function for the design (rangeless,
    multi-coin, fungible; tick cells replaced not dropped). Pins the CURRENT
    HONEST FLOOR of the frozen ``lp_curve`` fixture, not a clean-green target:
    CURVE4 stays XFAIL on a single-sided deposit (IL undefined), and the generic
    G6 USD reconciliation stays FAIL on a pre-existing wallet-side gap (M1-5,
    VIB-5427 cluster) — neither is masked here.
    """
    position_state_rows = position_state_rows or []
    lp_state_rows = [r for r in position_state_rows if r.get("position_type") == "LP"]
    lp_opens = [r for r in acct_events if r.get("event_type") == "LP_OPEN"]
    lp_closes = [r for r in acct_events if r.get("event_type") == "LP_CLOSE"]

    out: list[CellResult] = [
        _curve1_open_legs(lp_opens, acct_payloads, payload_errors),
        _curve2_close_legs(lp_closes, acct_payloads, pos_events, payload_errors),
        _curve3_fees(lp_opens + lp_closes, acct_payloads, pos_events, payload_errors),
        _curve4_il(lp_opens, lp_closes, acct_payloads, payload_errors),
    ]
    lp5 = _lp5_decomposition_cell(pos_events)
    out.append(
        CellResult(
            "CURVE5", "LP open->close delta decomposition (net_pnl + principal legs)", lp5.status, lp5.diagnostic
        )
    )
    out.append(_curve6_liquidity(lp_state_rows))
    return out


# Vault settlement changes depositor capital and shares, not strategy return.
# Keep its two legs distinct and reject fields that would leak capital into PnL.
_SETTLEMENT_PNL_LEAK_KEYS: frozenset[str] = frozenset(
    {"realized_pnl_usd", "principal_delta_usd", "cost_basis_usd", "unrealized_pnl", "net_pnl_usd"}
)


def _settlement_leg_cell(
    cell_id: str,
    event_type: str,
    acct_events: list[dict[str, Any]],
    acct_payloads: dict[Any, dict[str, Any]],
    payload_errors: dict[Any, str],
) -> CellResult:
    """Score one settlement leg (``SETTLE_DEPOSIT`` / ``SETTLE_REDEEM``).

    PASS requires ALL of, on at least one event of ``event_type``:

    1. the typed event exists;
    2. ``assets_delta`` AND ``shares_delta`` are MEASURED (Empty ≠ Zero: an
       unmeasured ``None`` never satisfies) and finite — and non-zero, since a
       real settlement moved capital and issued/burned shares;
    3. ``schema_version`` AND ``primitive_version`` version stamps present;
    4. PnL-inert — no capital/return leak key in the payload;
    5. ledger linkage present — ``tx_hash`` AND ``ledger_entry_id`` non-empty.

    Any single failing check yields FAIL with a diagnostic naming the first
    non-conformant reason across the candidate events.
    """
    desc = f"{event_type} typed capital event (measured, version-stamped, PnL-inert, ledger-linked)"
    rows = [r for r in acct_events if r.get("event_type") == event_type]
    if not rows:
        return CellResult(cell_id, desc, "FAIL", f"no {event_type} accounting event")
    blocked = _payload_block_cell(cell_id, desc, rows, payload_errors)
    if blocked is not None:
        return blocked

    reasons: list[str] = []
    for r in rows:
        p = acct_payloads.get(r.get("id")) or {}
        assets = _dec(p.get("assets_delta"))
        shares = _dec(p.get("shares_delta"))
        if assets is None or shares is None:
            reasons.append("assets_delta/shares_delta unmeasured (None — Empty ≠ Zero)")
            continue
        if not (assets.is_finite() and shares.is_finite()):
            reasons.append("assets_delta/shares_delta non-finite")
            continue
        if assets <= 0 or shares <= 0:
            reasons.append(f"non-positive capital magnitude (assets={assets}, shares={shares})")
            continue
        if p.get("schema_version") is None or p.get("primitive_version") is None:
            reasons.append("missing schema_version / primitive_version stamp")
            continue
        pnl_leak = sorted(k for k in _SETTLEMENT_PNL_LEAK_KEYS if p.get(k) is not None)
        if pnl_leak:
            reasons.append(f"PnL-leak key(s) present: {pnl_leak}")
            continue
        if not (r.get("tx_hash") or "") or not (r.get("ledger_entry_id") or ""):
            reasons.append("missing ledger linkage (tx_hash / ledger_entry_id)")
            continue
        return CellResult(
            cell_id,
            desc,
            "PASS",
            f"{event_type}: assets_delta={assets} shares_delta={shares} "
            f"stamps(schema={p.get('schema_version')},primitive={p.get('primitive_version')}) "
            "PnL-inert, ledger-linked",
        )
    return CellResult(cell_id, desc, "FAIL", "; ".join(reasons) or f"{event_type} event not conformant")


def _cells_settlement(
    acct_events: list[dict[str, Any]],
    acct_payloads: dict[Any, dict[str, Any]],
    payload_errors: dict[Any, str],
) -> list[CellResult]:
    """SETTLE_DEPOSIT + SETTLE_REDEEM — the vault-settlement scorecard (VIB-5682).

    Two cells, one per capital-moving leg. Reads the raw (non-v1) settlement
    payload pass-through from ``acct_payloads`` (``SettlementAccountingEvent`` is
    not in the v1 Pydantic surface, so the decoded dict is preserved verbatim).
    """
    return [
        _settlement_leg_cell("SETTLE_DEPOSIT", "SETTLE_DEPOSIT", acct_events, acct_payloads, payload_errors),
        _settlement_leg_cell("SETTLE_REDEEM", "SETTLE_REDEEM", acct_events, acct_payloads, payload_errors),
    ]


def _spot_swap_payloads(
    acct_events: list[dict[str, Any]],
    acct_payloads: dict[Any, dict[str, Any]],
) -> list[tuple[dict[str, Any], dict[str, Any]]]:
    return [(row, acct_payloads.get(row.get("id"), {})) for row in acct_events if row.get("event_type") == "SWAP"]


def _spot_replay_lots(
    swaps: list[tuple[dict[str, Any], dict[str, Any]]],
) -> tuple[dict[str, list[list[Decimal]]], list[str]]:
    """Replay the persisted SWAP payloads through a minimal FIFO ledger.

    The replay is deliberately independent of the production ``FIFOBasisStore``:
    S2 is meant to catch a defect in that implementation, so importing it here
    would make the Accountant repeat the same bug rather than audit it.
    """
    lots: dict[str, list[list[Decimal]]] = {}
    errors: list[str] = []
    matched_disposals = 0
    for row, payload in swaps:
        row_id = str(row.get("id") or "?")
        token_in = str(payload.get("token_in") or "").upper()
        token_out = str(payload.get("token_out") or "").upper()
        amount_in = _dec(payload.get("amount_in"))
        amount_out = _dec(payload.get("amount_out"))
        amount_in_usd = _dec(payload.get("amount_in_usd"))
        amount_out_usd = _dec(payload.get("amount_out_usd"))
        if not token_in or not token_out or amount_in is None or amount_out is None:
            errors.append(f"row {row_id}: token/amount evidence is unmeasured")
            continue

        remaining = amount_in
        basis_consumed = Decimal("0")
        for lot in lots.get(token_in, []):
            if remaining <= 0:
                break
            lot_amount, lot_cost = lot
            if lot_amount <= 0:
                continue
            consumed = min(remaining, lot_amount)
            consumed_cost = lot_cost * (consumed / lot_amount)
            lot[0] -= consumed
            lot[1] -= consumed_cost
            remaining -= consumed
            basis_consumed += consumed_cost

        persisted_unmatched = _dec(payload.get("unmatched_amount_in"))
        if persisted_unmatched is None or persisted_unmatched != remaining:
            errors.append(
                f"row {row_id}: unmatched_amount_in={persisted_unmatched} does not equal FIFO replay {remaining}"
            )
        matched = amount_in - remaining
        if matched > 0:
            matched_disposals += 1
            if amount_in_usd is None:
                errors.append(f"row {row_id}: matched disposal has no amount_in_usd")
            else:
                matched_proceeds = amount_in_usd * (matched / amount_in)
                expected_pnl = matched_proceeds - basis_consumed
                actual_pnl = _dec(payload.get("realized_pnl_usd_matched"))
                if actual_pnl is None or actual_pnl != expected_pnl:
                    errors.append(f"row {row_id}: realized_pnl_usd_matched={actual_pnl} != FIFO replay {expected_pnl}")

        if amount_out > 0:
            if amount_out_usd is None:
                errors.append(f"row {row_id}: acquired {token_out} without measured USD basis")
            else:
                lots.setdefault(token_out, []).append([amount_out, amount_out_usd])

    if matched_disposals == 0:
        errors.append("no SWAP disposal matched a previously acquired FIFO lot")
    return lots, errors


def _spot_snapshot_inventory(snapshot: dict[str, Any]) -> dict[str, dict[str, Any]]:
    positions = _json(snapshot.get("positions_json"))
    metadata_value = positions.get("metadata")
    metadata: dict[str, Any] = metadata_value if isinstance(metadata_value, dict) else {}
    swap_inventory_value = metadata.get("swap_inventory")
    swap_inventory: dict[str, Any] = swap_inventory_value if isinstance(swap_inventory_value, dict) else {}
    tokens = swap_inventory.get("tokens")
    return tokens if isinstance(tokens, dict) else {}


def _spot_mark_cell(snapshots: list[dict[str, Any]]) -> CellResult:
    marked_snapshots = 0
    errors: list[str] = []
    for snapshot in snapshots:
        inventory = _spot_snapshot_inventory(snapshot)
        if not inventory:
            continue
        marked_snapshots += 1
        wallet_rows = _json_list(snapshot.get("wallet_balances_json"))
        wallet = {str(row.get("symbol") or "").lower(): row for row in wallet_rows}
        for token, mark in inventory.items():
            wallet_row = wallet.get(str(token).lower())
            if wallet_row is None:
                errors.append(f"snapshot {snapshot.get('id')}: {token} absent from wallet balances")
                continue
            if _dec(mark.get("quantity")) != _dec(wallet_row.get("balance")):
                errors.append(f"snapshot {snapshot.get('id')}: {token} quantity != wallet balance")
            if _dec(mark.get("value_usd")) != _dec(wallet_row.get("value_usd")):
                errors.append(f"snapshot {snapshot.get('id')}: {token} inventory mark != wallet mark")
            balance = _dec(wallet_row.get("balance"))
            price = _dec(wallet_row.get("price_usd"))
            wallet_value = _dec(wallet_row.get("value_usd"))
            if balance is None or price is None or wallet_value is None or balance * price != wallet_value:
                errors.append(f"snapshot {snapshot.get('id')}: {token} wallet mark != balance × price")
    if marked_snapshots == 0:
        errors.append("no snapshot published an open swap_inventory mark")
    return CellResult(
        "S3",
        "Open-inventory mark equals wallet balance × price",
        "PASS" if not errors else "FAIL",
        f"{marked_snapshots} open-inventory snapshots match wallet quantity and USD mark"
        if not errors
        else "; ".join(errors),
    )


def _spot_basis_cell(swaps: list[tuple[dict[str, Any], dict[str, Any]]], snapshots: list[dict[str, Any]]) -> CellResult:
    errors: list[str] = []
    checked_basis = 0
    for snapshot in snapshots:
        inventory = _spot_snapshot_inventory(snapshot)
        if not inventory:
            continue
        snapshot_ts = str(snapshot.get("timestamp") or "")
        prefix = [(row, payload) for row, payload in swaps if str(row.get("timestamp") or "") <= snapshot_ts]
        lots, replay_errors = _spot_replay_lots(prefix)
        errors.extend(error for error in replay_errors if "no SWAP disposal matched" not in error)
        for token, mark in inventory.items():
            checked_basis += 1
            replay_basis = sum((lot[1] for lot in lots.get(str(token).upper(), []) if lot[0] > 0), Decimal("0"))
            if _dec(mark.get("cost_usd")) != replay_basis:
                errors.append(
                    f"snapshot {snapshot.get('id')}: {token} cost_usd={mark.get('cost_usd')} != replay {replay_basis}"
                )
    if checked_basis == 0:
        errors.append("no open inventory basis was available to audit")
    return CellResult(
        "S4",
        "Acquired basis equals persisted open-lot basis",
        "PASS" if not errors else "FAIL",
        f"{checked_basis} held-token basis marks equal independent acquisition replay"
        if not errors
        else "; ".join(errors),
    )


def _cells_spot(
    acct_events: list[dict[str, Any]],
    snapshots: list[dict[str, Any]],
    acct_payloads: dict[Any, dict[str, Any]],
    payload_errors: dict[Any, str],
) -> list[CellResult]:
    """S1–S4: persisted BUY→SELL SWAP round-trip contract (VIB-4203)."""
    swaps = _spot_swap_payloads(acct_events, acct_payloads)
    malformed = [str(row.get("id")) for row, _ in swaps if row.get("id") in payload_errors]
    if malformed:
        diagnostic = f"SWAP payload validation failed for rows {malformed}"
        return [
            CellResult(f"S{i}", title, "FAIL", diagnostic)
            for i, title in enumerate(
                (
                    "BUY-leg cost basis recorded",
                    "SELL-leg realized PnL reconciles to FIFO replay",
                    "Open-inventory mark equals wallet balance × price",
                    "Acquired basis equals persisted open-lot basis",
                ),
                start=1,
            )
        ]

    # S1 — a closed pair with a measured acquisition lot.
    round_trip = False
    if len(swaps) >= 2:
        first = swaps[0][1]
        last = swaps[-1][1]
        round_trip = (
            str(first.get("token_in") or "").upper() == str(last.get("token_out") or "").upper()
            and str(first.get("token_out") or "").upper() == str(last.get("token_in") or "").upper()
            and first.get("cost_basis_recorded") is True
            and all(
                first.get(key) not in (None, "")
                for key in ("amount_in", "amount_out", "amount_in_usd", "amount_out_usd")
            )
            and all(
                (_dec(first.get(key)) or Decimal("0")) > 0
                for key in ("amount_in", "amount_out", "amount_in_usd", "amount_out_usd")
            )
        )
    s1 = CellResult(
        "S1",
        "BUY-leg cost basis recorded",
        "PASS" if round_trip else "FAIL",
        "opening SWAP recorded measured basis and terminal SWAP closes the token pair"
        if round_trip
        else "need a measured SWAP→SWAP-back pair whose BUY records acquisition basis",
    )

    _, replay_errors = _spot_replay_lots(swaps)
    s2 = CellResult(
        "S2",
        "SELL-leg realized PnL reconciles to FIFO replay",
        "PASS" if not replay_errors else "FAIL",
        "independent FIFO replay matches persisted matched PnL" if not replay_errors else "; ".join(replay_errors),
    )

    return [s1, s2, _spot_mark_cell(snapshots), _spot_basis_cell(swaps, snapshots)]


# Profiles centralize lifecycle, tolerance, and cell dispatch. They live here
# because the cell-pack callables are module-local; _TaxonomyPrimitive avoids
# colliding with the module's ProfileName alias.
SCORECARD_PROFILES: dict[str, ScorecardProfile] = {
    "spot": ScorecardProfile(
        name="spot",
        canonical_primitive=_TaxonomyPrimitive.SWAP,
        # The spot cell pack proves the round trip; SWAP itself is atomic.
        required_lifecycle=(),
        eps_pct=Decimal("0.0025"),
        eps_scaling=lambda b: (b.notional_traded, "notional_traded"),
        cells=lambda ctx: _cells_spot(
            ctx.acct_events,
            ctx.snapshots,
            ctx.acct_payloads,
            ctx.payload_errors,
        ),
    ),
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
    # Pure lending has no borrow-to-swap leg, unlike the looping profile.
    "lending_lifecycle": ScorecardProfile(
        name="lending_lifecycle",
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
            require_loop_leg_attribution=False,
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
    # PT buy and sell both map to atomic SWAP; PEN cells prove the round trip.
    "pendle_pt": ScorecardProfile(
        name="pendle_pt",
        canonical_primitive=_TaxonomyPrimitive.SWAP,
        required_lifecycle=(),
        eps_pct=Decimal("0.0025"),
        eps_scaling=lambda b: (b.notional_traded, "notional_traded"),
        cells=lambda ctx: _cells_pendle_pt(
            ctx.acct_events,
            ctx.pos_events,
            ctx.snapshots,
            ctx.acct_payloads,
            ctx.payload_errors,
        ),
        # A measured PT disposal with unavailable gateway price is blocked, not wrong.
        disposal_usd_unmeasured_is_xfail=True,
    ),
    # Pendle LP shares LP lifecycle and tolerance but has fungible Pendle economics.
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
    # Curve shares LP lifecycle, tolerance, and matching version, but its
    # rangeless N-coin economics require a distinct cell pack.
    "curve_lp": ScorecardProfile(
        name="curve_lp",
        canonical_primitive=_TaxonomyPrimitive.LP,
        required_lifecycle=("LP_OPEN", "LP_CLOSE"),
        eps_pct=Decimal("0.0025"),
        eps_scaling=lambda b: (b.notional_traded, "notional_traded"),
        cells=lambda ctx: _cells_curve_lp(
            ctx.acct_events,
            ctx.pos_events,
            ctx.acct_payloads,
            ctx.payload_errors,
            ctx.position_state_rows,
        ),
    ),
    # Settlement lifecycle includes only capital-moving settle legs, not proposals.
    "settlement": ScorecardProfile(
        name="settlement",
        canonical_primitive=_TaxonomyPrimitive.SETTLEMENT,
        required_lifecycle=_SETTLEMENT_LIFECYCLE,
        eps_pct=Decimal("0.0025"),
        eps_scaling=lambda b: (b.notional_traded, "notional_traded"),
        cells=lambda ctx: _cells_settlement(
            ctx.acct_events,
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
    # Missing registry preflight inputs produce XFAIL instead of crashing older callers.
    position_registry_rows: list[dict[str, Any]] | None = None,
    position_reference_column_present: bool | None = None,
    position_registry_table_present: bool | None = None,
    malformed_position_reference_row_ids: list[Any] | None = None,
) -> AccountantReport:
    """Evaluate the cell matrix against pre-fetched rows.

    Decoupled from sqlite I/O so callers like the filtered reporting API
    (VIB-3870) can pass pre-filtered rows (by deployment_id, cycle_ids, time
    window, …) without rewriting the cell predicates.

    Sorts the input lists in-place by timestamp — cells assume time-ordered
    rows for running aggregations (see the BORROW → REPAY tracker in G6).
    """
    # Snapshot chronology cannot depend on process-local iteration numbers.
    _ordered_snapshots = _snapshots_in_time_order(snapshots)
    if _ordered_snapshots is not None:
        snapshots[:] = _ordered_snapshots
    else:
        # Preserve deterministic iteration, but endpoint cells re-derive and refuse chronology.
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

    # Payload-reading cells consume this validated map and fail on schema errors.
    acct_payloads, payload_errors, payload_error_records = _typed_acct_payloads(acct_events)

    cells: list[CellResult] = []
    cells.append(_cell_g1_money_trail(ledger, acct_events, acct_payloads, primitive))
    cells.append(_cell_g2_cost_ledger(ledger))
    cells.append(_cell_g3_yield_ledger(pos_events, acct_events))
    cells.append(_cell_g4_capital_deployed(snapshots))
    cells.append(_cell_g5_initial_vs_current(metrics, snapshots, ledger))
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

    cells.append(_cell_g16_native_lane(snapshots, ledger, acct_events))

    cells.append(_cell_g17_receipt_set(ledger))

    if position_reference_column_present is None:
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

    # Expose which cell failures propagated from payload validation.
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


# N-leg reconciliation findings are primitive-agnostic diagnostics and do not
# alter the historical cell denominator. Fixtures without coin_symbols are out of scope.


def _acct_event_coin_symbols(acct_events: list[dict[str, Any]]) -> set[str]:
    """Union of the pool-coin-ordered ``coin_symbols`` across all accounting
    events that stamped one (the shared N-coin carrier). Case-folded to upper.
    """
    out: set[str] = set()
    for ae in acct_events:
        payload = _json(ae.get("payload_json"))
        coins = payload.get("coin_symbols")
        if isinstance(coins, list | tuple):
            for c in coins:
                if c:
                    out.add(str(c).upper())
    return out


def _snapshot_covered_symbols(snapshots: list[dict[str, Any]]) -> set[str]:
    """Union across ALL snapshots of every symbol that was priced into the
    equity universe — wallet-balance rows plus per-position token identities.

    Taking the union across snapshots (not just the final one) keeps the
    invariant robust to teardown consolidation: a returned coin that was priced
    into a close-time snapshot and later swapped to the target token still
    counts as "was in the equity universe", so the check flags only the true
    defect (a returned coin that NEVER entered equity), never a legitimately
    consolidated coin.
    """
    covered: set[str] = set()
    for s in snapshots:
        for wb in _json_list(s.get("wallet_balances_json")):
            sym = wb.get("symbol") if isinstance(wb, dict) else None
            if sym:
                covered.add(str(sym).upper())
        for pos in _json_list(s.get("positions_json")):
            if not isinstance(pos, dict):
                continue
            for key in ("symbol", "token0", "token1"):
                sym = pos.get(key)
                if sym:
                    covered.add(str(sym).upper())
            details = pos.get("details")
            details = details if isinstance(details, dict) else _json(details)
            coins = details.get("coin_symbols") if isinstance(details, dict) else None
            if isinstance(coins, list | tuple):
                for c in coins:
                    if c:
                        covered.add(str(c).upper())
    return covered


def check_snapshot_covers_position_coins(
    snapshots: list[dict[str, Any]],
    acct_events: list[dict[str, Any]],
) -> list[str]:
    """VIB-5540 (Seam A) standing invariant — the snapshot equity universe must
    cover every coin any position touched.

    Returns a list of violation messages (empty ⇒ invariant holds). A non-empty
    result means a coin the strategy deposited into / recovered from an N-coin
    position (its ``coin_symbols``) never appeared in ANY portfolio snapshot's
    priced token universe — exactly the "a returned coin fell out of equity"
    defect that makes the wallet-method PnL short and G6 report a spurious gap.
    Primitive-agnostic: keyed only on the shared ``coin_symbols`` carrier.

    Correlation note (CodeRabbit): this is an order-independent, position-AGNOSTIC
    UNION check — it asserts ``⋃ snapshot covered symbols ⊇ ⋃ every position's
    coin_symbols`` (a set-superset), NOT a per-position comparison, so it needs no
    position-identity correlation (unlike the LP5 principal↔cost_basis companion,
    which does correlate by ``ledger_entry_id``). Cross-contamination is
    structurally impossible here: adding position B's coins to ``required`` can
    only make the superset test STRICTER, never mask a coin missing for position
    A — every coin any position touched must be covered.
    """
    required = _acct_event_coin_symbols(acct_events)
    if not required:
        return []
    covered = _snapshot_covered_symbols(snapshots)
    missing = sorted(required - covered)
    if missing:
        return [
            f"position coin(s) {missing} never entered the snapshot equity universe "
            f"(covered={sorted(covered)}); wallet-method PnL understated → G6 gap"
        ]
    return []


def check_lp5_principal_matches_cost_basis(
    pos_events: list[dict[str, Any]],
    acct_events: list[dict[str, Any]],
    *,
    rel_eps: Decimal = Decimal("0.005"),
    abs_floor_usd: Decimal = Decimal("1.00"),
) -> list[str]:
    """VIB-5612 (Seam B) LP5 companion — a fungible LP close's
    ``principal_recovered_usd`` (position-event attribution) must reconcile with
    the N-complete ``cost_basis_usd`` the typed accounting layer measured for the
    same close.

    Guards Seam B against regressing to the 2-coin ``position_events.value_usd``
    that left ``principal_recovered_usd`` at zero for a fungible Curve close.
    Returns violation messages (empty ⇒ holds / not applicable). Empty ≠ Zero:
    an unmeasured cost_basis (``None`` / missing) is skipped, never compared as
    zero.

    Tolerance = ``max(rel_eps × |cost_basis|, abs_floor_usd)``. Both sides value
    the IDENTICAL close-time coin-leg amounts, so on a shared price snapshot they
    agree to within oracle/rounding noise; ``rel_eps=0.5%`` is 10x tighter than
    the original 2% the audit flagged (2% permitted ~$1k silent divergence on a
    $50k position) and well below the depeg / price-impact guards (100-500 bps),
    so a real Seam-B drift on a volatile N-coin close surfaces. The $1 absolute
    floor prevents false positives on sub-few-hundred-dollar positions where
    sub-cent rounding on 18-dec legs would otherwise exceed a pure-relative bound.

    **Position-identity correlation (CodeRabbit Major):** on a multi-LP strategy
    the two streams carry DIFFERENT identity schemes (accounting_events key on a
    composite ``position_key`` = ``lp:proto:chain:wallet:pool``; position_events
    key on ``position_id`` = the pool/LP-token address), so principal and
    cost_basis are matched by the field BOTH close rows share for the same close
    intent — ``ledger_entry_id`` (the per-intent ledger row that produced both),
    falling back to ``tx_hash``. Each position's principal is compared against
    ITS OWN close cost_basis; a close with one side measured but not the other
    (Empty≠Zero) is skipped, never cross-matched to a different position.
    """
    # Correlate only N-coin closes with measured basis.
    cost_basis_by_key: dict[str, Decimal] = {}
    for ae in acct_events:
        if (ae.get("event_type") or "").upper() != "LP_CLOSE":
            continue
        payload = _json(ae.get("payload_json"))
        if not payload.get("coin_symbols"):
            continue
        cb = _dec(payload.get("cost_basis_usd"))
        if cb is None:
            continue
        key = _lp_close_correlation_key(ae)
        if key:
            cost_basis_by_key[key] = cb
    if not cost_basis_by_key:
        return []
    # Never compare principal and basis from different close intents.
    findings: list[str] = []
    for pe in pos_events:
        if (pe.get("event_type") or "").upper() != "CLOSE":
            continue
        key = _lp_close_correlation_key(pe)
        if not key or key not in cost_basis_by_key:
            continue
        attr = _json(pe.get("attribution_json"))
        principal = _dec(attr.get("principal_recovered_usd"))
        if principal is None:
            continue
        cb = cost_basis_by_key[key]
        tolerance = max(rel_eps * abs(cb), abs_floor_usd)
        if abs(principal - cb) > tolerance:
            findings.append(
                f"position (key={key}): principal_recovered_usd={principal} diverges "
                f"from measured accounting cost_basis_usd={cb} beyond tolerance "
                f"{tolerance} (max of {rel_eps:%} × cost_basis and ${abs_floor_usd} floor) "
                "(Seam B N-complete principal regressed)"
            )
    return findings


def _lp_close_correlation_key(event: dict[str, Any]) -> str | None:
    """The field that correlates an accounting LP_CLOSE with its position-event
    CLOSE for the SAME close intent — ``ledger_entry_id`` (both rows are derived
    from the one per-intent ledger row), falling back to ``tx_hash``. Returns
    ``None`` when neither is present so an un-correlatable row is skipped rather
    than cross-matched to a different position."""
    return event.get("ledger_entry_id") or event.get("tx_hash") or None


def evaluate_nleg_invariants(
    snapshots: list[dict[str, Any]],
    pos_events: list[dict[str, Any]],
    acct_events: list[dict[str, Any]],
) -> list[str]:
    """Run both VIB-5540 N-leg reconciliation invariants over a real DB's rows
    and return the union of findings (empty ⇒ all hold / not applicable).

    Called by ``run_against_sqlite`` so the accounting-matrix path and every
    real-fork DB carry these diagnostics. Fail-safe: never raises (a decode
    failure inside a helper degrades to "no findings" rather than crashing the
    report), and yields nothing on a fixture with no ``coin_symbols``.
    """
    findings: list[str] = []
    try:
        findings.extend(check_snapshot_covers_position_coins(snapshots, acct_events))
    except Exception:  # noqa: BLE001 — diagnostic must never crash the report
        logger.debug("check_snapshot_covers_position_coins failed", exc_info=True)
    try:
        findings.extend(check_lp5_principal_matches_cost_basis(pos_events, acct_events))
    except Exception:  # noqa: BLE001 — diagnostic must never crash the report
        logger.debug("check_lp5_principal_matches_cost_basis failed", exc_info=True)
    return findings


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
        # Resolve deployment before lifecycle checks so every read uses one scope.
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
        position_state_rows = _table_rows(conn, "position_state_snapshots", deployment_id=deployment_id)
        position_registry_rows = _table_rows(conn, "position_registry", deployment_id=deployment_id)
        (
            position_reference_column_present,
            position_registry_table_present,
            malformed_position_reference_row_ids,
        ) = _cell22_preflight(conn, deployment_id=deployment_id)
    finally:
        conn.close()
    report = evaluate_cells(
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
    # Run diagnostics over the same scoped rows as the scorecard.
    report.nleg_invariant_findings = evaluate_nleg_invariants(snapshots, pos_events, acct_events)
    return report


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
    try:
        cur = conn.execute(
            "SELECT count(*) FROM pragma_table_info('accounting_events') WHERE name = 'position_reference'"
        )
        position_reference_column_present = (cur.fetchone()[0] or 0) > 0
    except sqlite3.OperationalError:
        position_reference_column_present = False

    try:
        cur = conn.execute("SELECT count(*) FROM sqlite_master WHERE type='table' AND name='position_registry'")
        position_registry_table_present = (cur.fetchone()[0] or 0) > 0
    except sqlite3.OperationalError:
        position_registry_table_present = False

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
            # Without SQLite json_valid(), malformed references degrade to null hashes.
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
