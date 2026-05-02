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

from almanak.framework.accounting.payload_schemas import (
    FORMULA_VERSION,
    MATCHING_POLICY_VERSION,
    SCHEMA_VERSION,
)

Primitive = Literal["lp", "looping", "perp"]
CellStatus = Literal["PASS", "FAIL", "XFAIL", "SKIP"]


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
    strategy_id: str
    schema_version: int = SCHEMA_VERSION
    formula_version: int = FORMULA_VERSION
    matching_policy_version: int = MATCHING_POLICY_VERSION
    timestamp: datetime = field(default_factory=lambda: datetime.now(UTC))
    cells: list[CellResult] = field(default_factory=list)
    on_chain_footprint: list[dict[str, Any]] = field(default_factory=list)
    g6_decomposition: dict[str, Any] = field(default_factory=dict)
    db_dump_path: str | None = None

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

    def format_markdown(self) -> str:
        lines = []
        lines.append(f"# Accountant Test — {self.primitive} — {self.timestamp.isoformat()}")
        lines.append("")
        lines.append("## Run metadata")
        lines.append(f"- Primitive: **{self.primitive}**")
        lines.append(f"- Network: {self.network}")
        lines.append(f"- Strategy: `{self.strategy_id}`")
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
    }
)


def _table_rows(conn: sqlite3.Connection, table: str) -> list[dict[str, Any]]:
    """Read all rows from one of the SDK's read-only accounting tables.

    The table name is interpolated into the SQL string because sqlite3 does
    not parameterize identifiers. The whitelist below makes that safe — only
    the small set of SDK-owned accounting tables this module ever needs to
    read are permitted, and any other input raises ``ValueError`` rather
    than silently issuing a query against an attacker-controlled identifier.
    """
    if table not in _ALLOWED_READ_TABLES:
        raise ValueError(
            f"_table_rows: refusing to read unknown table {table!r}; allowed tables: {sorted(_ALLOWED_READ_TABLES)}"
        )
    cur = conn.cursor()
    try:
        cur.execute(f"SELECT * FROM {table}")  # noqa: S608 — whitelisted identifier
    except sqlite3.OperationalError:
        return []
    return [dict(r) for r in cur.fetchall()]


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


# ─── Cell predicates ─────────────────────────────────────────────────────


def _cell_g1_money_trail(rows: list[dict[str, Any]]) -> CellResult:
    if not rows:
        return CellResult(
            "G1",
            "Money trail (every credit/debit → tx_hash + USD@block)",
            "FAIL",
            "transaction_ledger empty",
        )
    missing_hash = [r for r in rows if not r.get("tx_hash")]
    # Enforce both pillars of the money-trail contract: every successful
    # ledger row must carry a tx_hash AND every SWAP row must record both
    # amount_in and amount_out. ``missing_usd`` was previously computed but
    # not enforced — a swap that landed on-chain without amounts in the
    # ledger silently passed G1, leaving the money trail half-broken.
    missing_usd = [
        r for r in rows if r.get("intent_type") == "SWAP" and not (r.get("amount_in") and r.get("amount_out"))
    ]
    if missing_hash:
        return CellResult(
            "G1",
            "Money trail",
            "FAIL",
            f"{len(missing_hash)} ledger rows missing tx_hash",
        )
    if missing_usd:
        sample = [r.get("id") for r in missing_usd[:3]]
        return CellResult(
            "G1",
            "Money trail",
            "FAIL",
            f"{len(missing_usd)} SWAP rows missing amount_in/amount_out (e.g. {sample!r})",
        )
    return CellResult(
        "G1",
        "Money trail",
        "PASS",
        f"{len(rows)} ledger rows; all tx_hashes present; all SWAP rows carry amounts",
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
        if p.get("realized_pnl_usd"):
            yields.append(("realized_pnl", r.get("event_type"), p.get("realized_pnl_usd")))
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


def _cell_g6_reconciliation(
    snapshots: list[dict[str, Any]],
    ledger: list[dict[str, Any]],
    pos_events: list[dict[str, Any]],
    acct_events: list[dict[str, Any]],
    primitive: Primitive,
) -> tuple[CellResult, dict[str, Any]]:
    """G6 reconciliation: wallet ≡ component within ε, decomposition ALWAYS emitted.

    IL is NOT a reconciliation term — recovered LP principal already reflects
    post-IL outcome. IL is a decomposition of the LP open→close delta, lives
    in LP4/LP5 attribution only.
    """
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

    for r in ledger:
        gas = _dec(r.get("gas_usd"))
        if gas is not None:
            sum_gas += gas

    for r in acct_events:
        p = _json(r.get("payload_json"))
        et = r.get("event_type")
        rpnl = _dec(p.get("realized_pnl_usd"))
        if et == "SWAP" and rpnl is not None:
            sum_swap += rpnl
        if et in ("LP_OPEN", "LP_CLOSE"):
            il = _dec(p.get("il_usd"))
            if il is not None:
                il_diagnostic += il
            if et == "LP_CLOSE" and rpnl is not None:
                sum_lp += rpnl
            fees_usd = _dec(p.get("fees_total_usd"))
            if fees_usd is not None:
                sum_fees += fees_usd
        if et == "WITHDRAW":
            interest = _dec(p.get("interest_accrued_usd"))
            if interest is not None:
                sum_interest += interest
        if et == "REPAY":
            interest = _dec(p.get("interest_paid_usd"))
            if interest is not None:
                sum_interest -= interest
        if et == "PERP_CLOSE":
            funding_p = _dec(p.get("funding_paid_usd"))
            funding_r = _dec(p.get("funding_received_usd"))
            if funding_p is not None:
                sum_funding -= funding_p
            if funding_r is not None:
                sum_funding += funding_r
            if rpnl is not None:
                sum_perp += rpnl

    component_pnl = sum_swap + sum_lp + sum_perp + sum_fees + sum_funding + sum_interest - sum_gas

    eps_pct = Decimal("0.0025") if primitive in ("lp", "looping") else Decimal("0.01")
    capital = max(abs(initial), abs(final))
    eps = max(Decimal("0.5"), eps_pct * capital)
    gap = abs(wallet_pnl - component_pnl)

    decomp = {
        "wallet_pnl_usd": str(wallet_pnl),
        "component_pnl_usd": str(component_pnl),
        "Σ_swaps_usd": str(sum_swap),
        "Σ_lp_usd": str(sum_lp),
        "Σ_perp_usd": str(sum_perp),
        "Σ_fees_usd": str(sum_fees),
        "Σ_funding_usd": str(sum_funding),
        "Σ_interest_usd": str(sum_interest),
        "Σ_gas_usd": str(-sum_gas),
        "gap_usd": str(gap),
        "ε_threshold_usd": str(eps),
        "ε_pct": str(eps_pct),
        "il_diagnostic_usd_NOT_in_PnL": str(il_diagnostic),
    }
    if gap <= eps:
        return (
            CellResult(
                "G6",
                "Reconciliation",
                "PASS",
                f"wallet=${wallet_pnl} component=${component_pnl} gap=${gap} (ε=${eps})",
                decomposition=decomp,
            ),
            decomp,
        )
    return (
        CellResult(
            "G6",
            "Reconciliation",
            "FAIL",
            f"wallet=${wallet_pnl} component=${component_pnl} gap=${gap} > ε=${eps}",
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

    A successful intent must produce exactly ONE ledger row regardless of
    how many on-chain transactions it took to land (e.g. APPROVE+SUPPLY,
    NPM.multicall LP_CLOSE). The cell detects "same intent recorded N times"
    by collapsing on the intent's natural identity within a cycle:
    ``(cycle_id, intent_type, tx_hash)``. ``tx_hash`` is what makes two rows
    "the same intent" — the framework writes one row per dispatched intent,
    so two rows sharing all three fields are a duplicate write.

    Including ``id`` (the ledger row PK) here would make this cell a
    tautology — every row has a unique PK, so dups would always be empty.
    """
    by_intent: dict[Any, int] = {}
    for r in ledger:
        # Skip teardown rows whose tx_hash may be NULL until the intent
        # confirms; G10 evaluates only landed intents (success/fail with a
        # dispatched TX). A None tx_hash on a "in-flight" row would otherwise
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
    return CellResult(
        "G10",
        "Multi-tx atomicity",
        "PASS",
        f"{len(ledger)} ledger rows: 1:1 with intents",
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


def _cell_g13_lot_matching(ledger: list[dict[str, Any]], acct_events: list[dict[str, Any]]) -> CellResult:
    versions: set[int] = set()
    bad_rows: list[Any] = []

    def _coerce(raw: Any) -> int | None:
        if raw is None or raw == "":
            return None
        try:
            return int(raw)
        except (ValueError, TypeError):
            return None

    for r in ledger:
        v = _coerce(r.get("matching_policy_version"))
        if v is not None:
            versions.add(v)
        elif r.get("matching_policy_version") not in (None, ""):
            bad_rows.append(("ledger", r.get("id")))
    for r in acct_events:
        p = _json(r.get("payload_json"))
        v = _coerce(p.get("matching_policy_version"))
        if v is not None:
            versions.add(v)
        elif p.get("matching_policy_version") not in (None, ""):
            bad_rows.append(("acct_event", r.get("id")))
    if bad_rows:
        return CellResult(
            "G13",
            "Lot-matching policy",
            "FAIL",
            f"{len(bad_rows)} rows have non-integer matching_policy_version (e.g. {bad_rows[:3]!r})",
        )
    if not versions:
        return CellResult(
            "G13",
            "Lot-matching policy declared + versioned",
            "FAIL",
            "no row carries matching_policy_version",
        )
    if len(versions) > 1:
        return CellResult(
            "G13",
            "Lot-matching policy",
            "FAIL",
            f"multiple matching_policy_version values in one run: {sorted(versions)}",
        )
    return CellResult(
        "G13",
        "Lot-matching policy",
        "PASS",
        f"matching_policy_version={next(iter(versions))} (FIFO in v1)",
    )


def _cell_g14_sdk_eq_onchain(
    snapshots: list[dict[str, Any]],
    position_state_rows: list[dict[str, Any]],
) -> CellResult:
    """G14: SDK position state ≡ on-chain state ± 1 bp dust per snapshot.

    Mirrors G15's gate-on-table-absence shape: when ``position_state_snapshots``
    rows are missing (Track C library-only on this branch — VIB-3866),
    return XFAIL pointing at the dependency. Once Track C is wired, the cell
    must evaluate ``delta_vs_protocol_pct`` per row and flip to PASS/FAIL.
    Returning unconditional XFAIL would mean the cell can never advance even
    after the materializer lands — a violation of the matrix's "must move
    forward" contract.
    """
    if not position_state_rows:
        return CellResult(
            "G14",
            "SDK ≡ on-chain reconciliation",
            "XFAIL",
            "position_state_snapshots not yet wired (Track C); cell is xfail by design "
            "until materializer lands — VIB-3866 truth correction",
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


def _cell_g15_multi_period_self_consistency(
    snapshots: list[dict[str, Any]], position_state_rows: list[dict[str, Any]]
) -> CellResult:
    """G15: Multi-period MtM self-consistency.

    The honest predicate: at every iteration, the SDK's MtM (sum of
    ``position_state_snapshots.value_usd`` across open positions, plus
    available cash) must equal the on-chain position state ± dust within
    a per-iteration tolerance, AND consecutive iterations must telescope
    to the endpoint delta of *priced* equity changes (i.e. P&L attributable
    to time / market moves, separate from the same-day open→close pairs
    handled by G6).

    That predicate requires :class:`position_state_snapshots` rows — Track C
    in `Accounting-AttemptNo17.md`. Track C is library code only on this
    branch (no production callers — see VIB-3866 for the truth-table
    correction). Without those rows, every implementation reduces to a
    telescoping sum over `portfolio_snapshots.equity` which is a tautology
    (Σ(s[i+1] - s[i]) ≡ s[-1] - s[0] for any monotonic measured series)
    and was masquerading as a PASS in earlier iterations of this test —
    flagged in Codex's PR-review (VIB-3865).

    Until Track C lands, this cell is **XFAIL by design**. A passing G15
    would be a regression — it would re-introduce a false positive that
    drives mainnet decisions on an arithmetic identity rather than a
    real reconciliation.
    """
    if not position_state_rows:
        return CellResult(
            "G15",
            "Multi-period MtM self-consistency",
            "XFAIL",
            "position_state_snapshots not yet wired (Track C); cell is xfail by design "
            "until materializer lands — VIB-3866 truth correction",
        )

    # When Track C lands, replace this branch with the real per-iteration
    # SDK-vs-on-chain reconciliation against position_state_snapshots.
    # Until then, we deliberately do not score against portfolio_snapshots
    # alone — see docstring.
    return CellResult(
        "G15",
        "Multi-period MtM self-consistency",
        "XFAIL",
        f"position_state_snapshots present ({len(position_state_rows)} rows) but the "
        "Track-C-aware reconciliation predicate is not yet implemented — VIB-3866",
    )


# ─── Primitive-specific cells ────────────────────────────────────────────


def _cells_lp(
    pos_events: list[dict[str, Any]], acct_events: list[dict[str, Any]], snapshots: list[dict[str, Any]]
) -> list[CellResult]:
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
    # LP2: in-range time
    out.append(
        CellResult(
            "LP2",
            "In-range time (fraction over hold)",
            "XFAIL",
            "position_state_snapshots not wired (Track C); deferred",
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
    # LP4: IL diagnostic
    has_il = False
    for r in acct_events:
        p = _json(r.get("payload_json"))
        if p.get("il_usd") is not None:
            has_il = True
            break
    out.append(
        CellResult(
            "LP4",
            "Impermanent loss (diagnostic, NOT in net PnL)",
            "PASS" if has_il else "XFAIL",
            "il_usd in LP_CLOSE payload" if has_il else "il_usd not yet emitted by LP close handler",
        )
    )
    # LP5: open→close delta decomposition
    out.append(
        CellResult(
            "LP5",
            "LP open→close delta decomposition",
            "XFAIL",
            "attribution_json LP decomposition not yet computed",
        )
    )
    # LP6: liquidity over time
    out.append(
        CellResult(
            "LP6",
            "Liquidity over time",
            "XFAIL",
            "needs position_state_snapshots (Track C)",
        )
    )
    return out


def _cells_lending(acct_events: list[dict[str, Any]], snapshots: list[dict[str, Any]]) -> list[CellResult]:
    out: list[CellResult] = []
    # L1: net carry
    interest_supply = Decimal(0)
    interest_borrow = Decimal(0)
    for r in acct_events:
        p = _json(r.get("payload_json"))
        if r.get("event_type") == "WITHDRAW" and p.get("interest_accrued_usd"):
            interest_supply += _dec(p.get("interest_accrued_usd")) or Decimal(0)
        if r.get("event_type") == "REPAY" and p.get("interest_paid_usd"):
            interest_borrow += _dec(p.get("interest_paid_usd")) or Decimal(0)
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
    # L2: HF/LTV trajectory
    out.append(
        CellResult(
            "L2",
            "HF / LTV trajectory",
            "XFAIL",
            "needs position_state_snapshots (Track C)",
        )
    )
    # L3: liquidation buffer
    out.append(
        CellResult(
            "L3",
            "Liquidation buffer",
            "XFAIL",
            "needs L2 + per-asset thresholds (Track C)",
        )
    )
    # L4: principal vs interest at REPAY.
    # Both spec names (``principal_repaid_usd`` / ``interest_paid_usd``) and
    # legacy ``*_delta_usd`` names are accepted — the writer projects from
    # the legacy fields to the spec names (see writer._project_lending_aliases).
    # ``interest_paid_usd`` may be None in cases where there were no matching
    # BORROW lots (FIFO miss) — that's UNAVAILABLE rather than a fail. The
    # cell looks for AT LEAST ONE REPAY row where the split was emittable.
    has_split = False
    repay_rows = 0
    for r in acct_events:
        if r.get("event_type") in ("REPAY", "DELEVERAGE"):
            repay_rows += 1
            p = _json(r.get("payload_json"))
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
    # L5: APR/APY snapshot
    out.append(
        CellResult(
            "L5",
            "APR / APY snapshot",
            "XFAIL",
            "needs position_state_snapshots (Track C) capturing supply_apr/borrow_apr per iteration",
        )
    )
    # L6: loop-leg attribution
    out.append(
        CellResult(
            "L6",
            "Loop-leg attribution",
            "XFAIL",
            "loop-leg attribution not in v1 of typed-column writer",
        )
    )
    return out


def _cells_perp(acct_events: list[dict[str, Any]], pos_events: list[dict[str, Any]]) -> list[CellResult]:
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
    has_fee_split = False
    for r in acct_events:
        p = _json(r.get("payload_json"))
        if r.get("event_type") in ("PERP_OPEN", "PERP_CLOSE") and (
            p.get("open_fee_usd") is not None or p.get("close_fee_usd") is not None
        ):
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
    has_realized = False
    for r in acct_events:
        if r.get("event_type") == "PERP_CLOSE":
            p = _json(r.get("payload_json"))
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


# ─── Top-level runner ────────────────────────────────────────────────────


def run_against_sqlite(db_path: str | Path, *, primitive: Primitive) -> AccountantReport:
    """Run the Accountant Test against a SQLite DB file."""
    conn = _connect(db_path)
    try:
        ledger = _table_rows(conn, "transaction_ledger")
        pos_events = _table_rows(conn, "position_events")
        acct_events = _table_rows(conn, "accounting_events")
        snapshots = _table_rows(conn, "portfolio_snapshots")
        metrics = _table_rows(conn, "portfolio_metrics")
        # Track C surface — empty list when the materializer hasn't been
        # wired (current state on this branch). Both G14 and G15 stay
        # XFAIL in that case by design.
        position_state_rows = _table_rows(conn, "position_state_snapshots")
    finally:
        conn.close()

    snapshots.sort(key=lambda r: (r.get("iteration_number") or 0, r.get("timestamp") or ""))
    ledger.sort(key=lambda r: r.get("timestamp") or "")
    pos_events.sort(key=lambda r: r.get("timestamp") or "")
    acct_events.sort(key=lambda r: r.get("timestamp") or "")

    strategy_id = ""
    network = ""
    if metrics:
        strategy_id = metrics[0].get("strategy_id") or ""
    if ledger:
        network = ledger[0].get("chain") or ""

    cells: list[CellResult] = []
    cells.append(_cell_g1_money_trail(ledger))
    cells.append(_cell_g2_cost_ledger(ledger))
    cells.append(_cell_g3_yield_ledger(pos_events, acct_events))
    cells.append(_cell_g4_capital_deployed(snapshots))
    cells.append(_cell_g5_initial_vs_current(metrics, snapshots))
    g6, decomp = _cell_g6_reconciliation(snapshots, ledger, pos_events, acct_events, primitive)
    cells.append(g6)
    cells.append(_cell_g7_attribution(ledger, pos_events, acct_events))
    cells.append(_cell_g8_time_series(snapshots))
    cells.append(_cell_g9_confidence(snapshots, acct_events))
    cells.append(_cell_g10_multi_tx_atomicity(ledger, pos_events, acct_events))
    cells.append(_cell_g11_failed_intents(ledger))
    cells.append(_cell_g12_oracle_consistency(ledger))
    cells.append(_cell_g13_lot_matching(ledger, acct_events))
    cells.append(_cell_g14_sdk_eq_onchain(snapshots, position_state_rows))
    cells.append(_cell_g15_multi_period_self_consistency(snapshots, position_state_rows))

    if primitive == "lp":
        cells.extend(_cells_lp(pos_events, acct_events, snapshots))
    elif primitive == "looping":
        cells.extend(_cells_lending(acct_events, snapshots))
    elif primitive == "perp":
        cells.extend(_cells_perp(acct_events, pos_events))

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
        strategy_id=strategy_id,
        cells=cells,
        on_chain_footprint=footprint,
        g6_decomposition=decomp,
        db_dump_path=str(db_path),
    )


__all__ = [
    "AccountantReport",
    "CellResult",
    "Primitive",
    "run_against_sqlite",
]
