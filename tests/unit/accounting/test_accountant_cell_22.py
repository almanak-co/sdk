"""VIB-4201 (T15) — Accountant Test cell #22 (registry coherence) unit tests.

Cell #22 is the bidirectional ``accounting_events`` ↔ ``position_registry``
close-coherence check. Forward direction: every CLOSE event with a non-null
``position_reference.physical_identity_hash`` must have a matching
``status='closed'`` registry row at the same hash. Inverse direction: every
``status='closed'`` registry row must have at least one matching CLOSE
accounting event whose ``position_reference.physical_identity_hash`` equals
the registry row's hash.

These tests cover the failure modes F1-F10 enumerated on the UAT card
(``docs/internal/uat-cards/VIB-4201.md``) plus per-primitive coverage
across every CLOSE-event-kind row in the canonical taxonomy.

Test DBs are constructed at runtime under ``tmp_path`` — no mocking, no
patching. Each test exercises one cell-evaluation path end-to-end through
:func:`run_against_sqlite`.
"""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any

import pytest

from almanak.framework.accounting.accountant_test import (
    CLOSE_EVENT_TYPES,
    Primitive,
    run_against_sqlite,
)
from almanak.framework.primitives.taxonomy import TAXONOMY
from almanak.framework.primitives.types import EventKind

# Minimal SQLite schema needed for cell #22 to evaluate. Mirrors the
# columns the Accountant Test reads — extra columns are unused.
_SCHEMA_SQL = """
CREATE TABLE accounting_events (
    id TEXT PRIMARY KEY,
    deployment_id TEXT NOT NULL,
    strategy_id TEXT NOT NULL,
    cycle_id TEXT NOT NULL,
    execution_mode TEXT NOT NULL,
    timestamp TEXT NOT NULL,
    chain TEXT NOT NULL,
    protocol TEXT NOT NULL,
    wallet_address TEXT NOT NULL,
    event_type TEXT NOT NULL,
    position_key TEXT NOT NULL,
    ledger_entry_id TEXT,
    tx_hash TEXT,
    confidence TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    schema_version INTEGER NOT NULL DEFAULT 1,
    position_reference TEXT
);

CREATE TABLE position_registry (
    deployment_id TEXT NOT NULL,
    chain TEXT NOT NULL,
    primitive TEXT NOT NULL,
    accounting_category TEXT NOT NULL,
    physical_identity_hash TEXT NOT NULL,
    semantic_grouping_key TEXT NOT NULL,
    grouping_policy_version TEXT NOT NULL,
    handle TEXT,
    status TEXT NOT NULL CHECK (status IN ('open', 'closed', 'reorg_invalidated')),
    payload TEXT NOT NULL,
    opened_at_block INTEGER,
    opened_tx TEXT,
    closed_at_block INTEGER,
    closed_tx TEXT,
    last_reconciled_at_block INTEGER,
    matching_policy_version INTEGER NOT NULL,
    PRIMARY KEY (deployment_id, chain, primitive, physical_identity_hash)
);

-- Tables consumed by the other 21 cells. The cell-22 tests don't care
-- about their content — empty tables are fine — but the cell harness
-- would otherwise emit warnings.
CREATE TABLE transaction_ledger (
    id TEXT PRIMARY KEY, cycle_id TEXT, strategy_id TEXT, deployment_id TEXT,
    execution_mode TEXT, timestamp TEXT, intent_type TEXT, token_in TEXT,
    amount_in TEXT, token_out TEXT, amount_out TEXT, effective_price TEXT,
    slippage_bps INTEGER, gas_used INTEGER, gas_usd TEXT, tx_hash TEXT,
    chain TEXT, protocol TEXT, success INTEGER, error TEXT,
    extracted_data_json TEXT, price_inputs_json TEXT, pre_state_json TEXT,
    post_state_json TEXT, matching_policy_version INTEGER
);
CREATE TABLE position_events (id TEXT, timestamp TEXT, event_type TEXT, cycle_id TEXT);
CREATE TABLE portfolio_snapshots (id INTEGER PRIMARY KEY, timestamp TEXT, iteration_number INTEGER, total_value_usd TEXT, available_cash_usd TEXT, positions_json TEXT, value_confidence TEXT);
CREATE TABLE portfolio_metrics (strategy_id TEXT, initial_value_usd TEXT, deployment_id TEXT);
CREATE TABLE position_state_snapshots (id INTEGER PRIMARY KEY, position_type TEXT);
"""


def _make_position_reference(phid: str | None) -> str:
    """Build the JSON sub-document the writer puts in
    ``accounting_events.position_reference``."""
    return json.dumps(
        {
            "source": "receipt" if phid is not None else "legacy",
            "primitive": "lp",
            "accounting_category": "lp",
            "physical_identity_hash": phid,
            "semantic_grouping_key": None,
            "registry_handle": None,
            "grouping_policy_version": None,
            "matching_policy_version": None,
        },
        sort_keys=True,
    )


def _new_db(tmp_path: Path) -> Path:
    """Create a fresh DB file with the cell-22 schema applied."""
    db = tmp_path / "test.db"
    conn = sqlite3.connect(str(db))
    try:
        conn.executescript(_SCHEMA_SQL)
        conn.commit()
    finally:
        conn.close()
    return db


def _insert_acct_event(
    conn: sqlite3.Connection,
    *,
    row_id: str,
    event_type: str,
    position_reference: str | None,
    payload_json: str = "{}",
) -> None:
    conn.execute(
        """
        INSERT INTO accounting_events
        (id, deployment_id, strategy_id, cycle_id, execution_mode, timestamp,
         chain, protocol, wallet_address, event_type, position_key, ledger_entry_id,
         tx_hash, confidence, payload_json, position_reference)
        VALUES (?, 'dep-1', 'strat-1', 'cyc-1', 'live', '2026-05-10T00:00:00+00:00',
                'arbitrum', 'uniswap_v3', '0xwallet', ?, 'pos:lp:1', 'led-1',
                '0xdead', 'high', ?, ?)
        """,
        (row_id, event_type, payload_json, position_reference),
    )


def _insert_registry_row(
    conn: sqlite3.Connection,
    *,
    phid: str,
    status: str = "closed",
    primitive: str = "lp",
    category: str = "lp",
) -> None:
    conn.execute(
        """
        INSERT INTO position_registry
        (deployment_id, chain, primitive, accounting_category, physical_identity_hash,
         semantic_grouping_key, grouping_policy_version, status, payload,
         closed_tx, matching_policy_version)
        VALUES ('dep-1', 'arbitrum', ?, ?, ?, 'sgk', 'gpv', ?, '{}', '0xclosed', 1)
        """,
        (primitive, category, phid, status),
    )


def _run(db: Path, primitive: Primitive = "lp") -> Any:
    return run_against_sqlite(db, primitive=primitive, strict_lifecycle=False)


def _cell22(report: Any) -> Any:
    cells = [c for c in report.cells if c.cell_id == "L5_22"]
    assert len(cells) == 1, f"expected exactly one L5_22 cell, got {len(cells)}"
    return cells[0]


# ─── F0: PASS path ────────────────────────────────────────────────────────


def test_cell22_pass_lp_close_matches_registry_close(tmp_path: Path) -> None:
    """F0 — happy path: 1 LP_CLOSE event with hash + 1 closed registry row."""
    db = _new_db(tmp_path)
    conn = sqlite3.connect(str(db))
    try:
        _insert_acct_event(
            conn,
            row_id="ae-1",
            event_type="LP_CLOSE",
            position_reference=_make_position_reference("0xaaaa"),
        )
        _insert_registry_row(conn, phid="0xaaaa", status="closed")
        conn.commit()
    finally:
        conn.close()

    cell = _cell22(_run(db))
    assert cell.status == "PASS"
    assert "bidirectional coherence holds" in cell.diagnostic


# ─── F1 / F1' / F4: forward-orphan branches ──────────────────────────────


def test_cell22_fail_close_event_hash_not_in_registry(tmp_path: Path) -> None:
    """F1' — CLOSE event with hash but registry has no row for that hash."""
    db = _new_db(tmp_path)
    conn = sqlite3.connect(str(db))
    try:
        _insert_acct_event(
            conn,
            row_id="ae-1",
            event_type="LP_CLOSE",
            position_reference=_make_position_reference("0xaaaa"),
        )
        # No registry row for hash 0xaaaa.
        _insert_registry_row(conn, phid="0xbbbb", status="open")
        conn.commit()
    finally:
        conn.close()

    cell = _cell22(_run(db))
    assert cell.status == "FAIL"
    assert "forward orphan" in cell.diagnostic
    assert "ae-1" in cell.diagnostic


def test_cell22_fail_registry_open_paired_with_close_event(tmp_path: Path) -> None:
    """F1 / F4 — registry row for the hash exists BUT status='open'."""
    db = _new_db(tmp_path)
    conn = sqlite3.connect(str(db))
    try:
        _insert_acct_event(
            conn,
            row_id="ae-1",
            event_type="LP_CLOSE",
            position_reference=_make_position_reference("0xaaaa"),
        )
        _insert_registry_row(conn, phid="0xaaaa", status="open")
        conn.commit()
    finally:
        conn.close()

    cell = _cell22(_run(db))
    assert cell.status == "FAIL"
    assert "forward orphan" in cell.diagnostic


def test_cell22_fail_reorg_invalidated_paired_with_close_event(tmp_path: Path) -> None:
    """F4 variant — registry row at status='reorg_invalidated' is NOT closed."""
    db = _new_db(tmp_path)
    conn = sqlite3.connect(str(db))
    try:
        _insert_acct_event(
            conn,
            row_id="ae-1",
            event_type="LP_CLOSE",
            position_reference=_make_position_reference("0xaaaa"),
        )
        _insert_registry_row(conn, phid="0xaaaa", status="reorg_invalidated")
        conn.commit()
    finally:
        conn.close()

    cell = _cell22(_run(db))
    assert cell.status == "FAIL"
    assert "forward orphan" in cell.diagnostic


# ─── F2: inverse-orphan branch ───────────────────────────────────────────


def test_cell22_fail_inverse_orphan_registry_closed_no_event(tmp_path: Path) -> None:
    """F2 — registry row at status='closed' but no matching CLOSE event."""
    db = _new_db(tmp_path)
    conn = sqlite3.connect(str(db))
    try:
        _insert_registry_row(conn, phid="0xbbbb", status="closed")
        conn.commit()
    finally:
        conn.close()

    cell = _cell22(_run(db))
    assert cell.status == "FAIL"
    assert "inverse orphan" in cell.diagnostic
    assert "0xbbbb" in cell.diagnostic


# ─── F3: hash mismatch (both directions) ──────────────────────────────────


def test_cell22_fail_hash_mismatch_both_directions(tmp_path: Path) -> None:
    """F3 — CLOSE event hash differs from registry closed hash."""
    db = _new_db(tmp_path)
    conn = sqlite3.connect(str(db))
    try:
        _insert_acct_event(
            conn,
            row_id="ae-1",
            event_type="LP_CLOSE",
            position_reference=_make_position_reference("0xaaaa"),
        )
        _insert_registry_row(conn, phid="0xcccc", status="closed")
        conn.commit()
    finally:
        conn.close()

    cell = _cell22(_run(db))
    assert cell.status == "FAIL"
    # Both directions surface the disagreement.
    assert "forward orphan" in cell.diagnostic
    assert "inverse orphan" in cell.diagnostic


# ─── F5: idempotency ──────────────────────────────────────────────────────


def test_cell22_idempotent_repeat_run_same_db(tmp_path: Path) -> None:
    """F5 — running the cell twice on the same DB produces identical (status, diagnostic) tuples."""
    db = _new_db(tmp_path)
    conn = sqlite3.connect(str(db))
    try:
        _insert_acct_event(
            conn,
            row_id="ae-1",
            event_type="LP_CLOSE",
            position_reference=_make_position_reference("0xaaaa"),
        )
        # Deliberately leave the registry empty so the cell FAILs (the
        # idempotency contract is just as critical for FAIL as for PASS).
        conn.commit()
    finally:
        conn.close()

    cell_a = _cell22(_run(db))
    cell_b = _cell22(_run(db))
    assert (cell_a.status, cell_a.diagnostic) == (cell_b.status, cell_b.diagnostic)


# ─── F6 / F7 / F8: registry-absent / no-lifecycle branches ────────────────


def test_cell22_fail_registry_empty_close_events_have_hashes(tmp_path: Path) -> None:
    """F6 — registry table is present but EMPTY, AND a CLOSE event carries a non-null hash."""
    db = _new_db(tmp_path)
    conn = sqlite3.connect(str(db))
    try:
        _insert_acct_event(
            conn,
            row_id="ae-1",
            event_type="LP_CLOSE",
            position_reference=_make_position_reference("0xaaaa"),
        )
        # No registry rows.
        conn.commit()
    finally:
        conn.close()

    cell = _cell22(_run(db))
    # Registry-present-but-empty + non-null hash → forward orphan FAIL.
    assert cell.status == "FAIL"
    assert "forward orphan" in cell.diagnostic


def test_cell22_fail_registry_table_absent_close_events_have_hashes(tmp_path: Path) -> None:
    """F6 — registry TABLE absent + at least one CLOSE event has hash → FAIL."""
    db = _new_db(tmp_path)
    conn = sqlite3.connect(str(db))
    try:
        conn.executescript("DROP TABLE position_registry")
        _insert_acct_event(
            conn,
            row_id="ae-1",
            event_type="LP_CLOSE",
            position_reference=_make_position_reference("0xaaaa"),
        )
        conn.commit()
    finally:
        conn.close()

    cell = _cell22(_run(db))
    assert cell.status == "FAIL"
    assert "position_registry table absent" in cell.diagnostic
    assert "claim hashes" in cell.diagnostic


def test_cell22_xfail_legacy_no_registry_all_null_hashes(tmp_path: Path) -> None:
    """F7 — registry absent AND every CLOSE event has null hash (legacy)."""
    db = _new_db(tmp_path)
    conn = sqlite3.connect(str(db))
    try:
        conn.executescript("DROP TABLE position_registry")
        _insert_acct_event(
            conn,
            row_id="ae-1",
            event_type="LP_CLOSE",
            position_reference=_make_position_reference(None),
        )
        conn.commit()
    finally:
        conn.close()

    cell = _cell22(_run(db))
    assert cell.status == "XFAIL"
    assert "registry mode not yet on" in cell.diagnostic


def test_cell22_xfail_no_close_events_at_all(tmp_path: Path) -> None:
    """F8 — no CLOSE events AND no closed registry rows."""
    db = _new_db(tmp_path)
    # Keep the registry table; just leave both sides empty.
    cell = _cell22(_run(db))
    assert cell.status == "XFAIL"
    assert "lifecycle not exercised" in cell.diagnostic.lower() or "registry mode not yet on" in cell.diagnostic


# ─── F9: position_reference column missing ──────────────────────────────


def test_cell22_xfail_position_reference_column_missing(tmp_path: Path) -> None:
    """F9 — pre-T10 DB without position_reference column → XFAIL."""
    db = tmp_path / "old.db"
    conn = sqlite3.connect(str(db))
    try:
        # Ancient schema: no position_reference column on accounting_events.
        # Drop the column line entirely (the schema string declares the
        # column on its own indented line — match that shape exactly so
        # the replace doesn't silently no-op).
        ancient_schema = _SCHEMA_SQL.replace(",\n    position_reference TEXT", "")
        assert "position_reference TEXT" not in ancient_schema, (
            "schema replacement failed; cell would still see the column"
        )
        conn.executescript(ancient_schema)
        # Insert a CLOSE event row so we don't fall into the "no CLOSE
        # events" XFAIL branch — the F9 branch must short-circuit BEFORE
        # the no-lifecycle branch.
        conn.execute(
            """
            INSERT INTO accounting_events
            (id, deployment_id, strategy_id, cycle_id, execution_mode, timestamp,
             chain, protocol, wallet_address, event_type, position_key, ledger_entry_id,
             tx_hash, confidence, payload_json)
            VALUES ('ae-1', 'dep-1', 'strat-1', 'cyc-1', 'live', '2026-05-10T00:00:00+00:00',
                    'arbitrum', 'uniswap_v3', '0xwallet', 'LP_CLOSE', 'pos:lp:1', 'led-1',
                    '0xdead', 'high', '{}')
            """
        )
        conn.commit()
    finally:
        conn.close()

    cell = _cell22(_run(db))
    assert cell.status == "XFAIL"
    assert "position_reference column missing" in cell.diagnostic


# ─── F10: malformed JSON ─────────────────────────────────────────────────


def test_cell22_fails_malformed_position_reference_json(tmp_path: Path) -> None:
    """F10 — malformed JSON in position_reference must FAIL with offending row id, NOT silently skip."""
    db = _new_db(tmp_path)
    conn = sqlite3.connect(str(db))
    try:
        # Bypass _insert_acct_event to land deliberately bad JSON.
        conn.execute(
            """
            INSERT INTO accounting_events
            (id, deployment_id, strategy_id, cycle_id, execution_mode, timestamp,
             chain, protocol, wallet_address, event_type, position_key, ledger_entry_id,
             tx_hash, confidence, payload_json, position_reference)
            VALUES ('ae-bad', 'dep-1', 'strat-1', 'cyc-1', 'live', '2026-05-10T00:00:00+00:00',
                    'arbitrum', 'uniswap_v3', '0xwallet', 'LP_CLOSE', 'pos:lp:1', 'led-1',
                    '0xdead', 'high', '{}', '{not a valid json')
            """
        )
        conn.commit()
    finally:
        conn.close()

    cell = _cell22(_run(db))
    assert cell.status == "FAIL"
    assert "malformed" in cell.diagnostic.lower()
    assert "ae-bad" in cell.diagnostic


# ─── Structural: open registry rows are ignored ─────────────────────────


def test_cell22_does_not_count_open_registry_rows(tmp_path: Path) -> None:
    """A registry with status='open' rows but no closed → cell evaluates only closed."""
    db = _new_db(tmp_path)
    conn = sqlite3.connect(str(db))
    try:
        # Several open rows; no CLOSE events, no closed rows.
        _insert_registry_row(conn, phid="0xopen1", status="open")
        _insert_registry_row(conn, phid="0xopen2", status="open")
        conn.commit()
    finally:
        conn.close()

    cell = _cell22(_run(db))
    # No closed rows + no CLOSE events with hash → no orphans on either
    # side → XFAIL (no work exercised).
    assert cell.status == "XFAIL"


# ─── Lending lifecycle coverage ─────────────────────────────────────────


def test_cell22_lending_multiple_repays_one_close(tmp_path: Path) -> None:
    """Lending: many REPAY events with the same hash + 1 closed registry row → PASS."""
    db = _new_db(tmp_path)
    conn = sqlite3.connect(str(db))
    try:
        for i in range(3):
            _insert_acct_event(
                conn,
                row_id=f"ae-rep-{i}",
                event_type="REPAY",
                position_reference=_make_position_reference("0xloop"),
            )
        _insert_registry_row(conn, phid="0xloop", status="closed", primitive="lending", category="lending")
        conn.commit()
    finally:
        conn.close()

    cell = _cell22(_run(db, primitive="looping"))
    assert cell.status == "PASS"


@pytest.mark.parametrize(
    "event_type",
    ["WITHDRAW", "DELEVERAGE", "CLOSE"],
)
def test_cell22_lending_close_event_types_recognized(tmp_path: Path, event_type: str) -> None:
    """Per-event-type coverage: each lending CLOSE event_type pairs with a closed registry row → PASS."""
    db = _new_db(tmp_path)
    conn = sqlite3.connect(str(db))
    try:
        _insert_acct_event(
            conn,
            row_id="ae-1",
            event_type=event_type,
            position_reference=_make_position_reference("0xland"),
        )
        _insert_registry_row(conn, phid="0xland", status="closed", primitive="lending", category="lending")
        conn.commit()
    finally:
        conn.close()

    cell = _cell22(_run(db, primitive="looping"))
    assert cell.status == "PASS", f"event_type={event_type} got {cell.status}: {cell.diagnostic}"


# ─── LP / Pendle LP coverage ────────────────────────────────────────────


def test_cell22_lp_uniswap_close(tmp_path: Path) -> None:
    """LP_CLOSE event + closed registry → PASS (UniV3 cutover post-T12)."""
    db = _new_db(tmp_path)
    conn = sqlite3.connect(str(db))
    try:
        _insert_acct_event(
            conn,
            row_id="ae-lp",
            event_type="LP_CLOSE",
            position_reference=_make_position_reference("0xlp"),
        )
        _insert_registry_row(conn, phid="0xlp", status="closed", primitive="lp", category="lp")
        conn.commit()
    finally:
        conn.close()

    cell = _cell22(_run(db, primitive="lp"))
    assert cell.status == "PASS"


def test_cell22_pendle_lp_close_recognized(tmp_path: Path) -> None:
    """PENDLE_LP_CLOSE event + closed registry → PASS (proves taxonomy-driven enumeration)."""
    db = _new_db(tmp_path)
    conn = sqlite3.connect(str(db))
    try:
        _insert_acct_event(
            conn,
            row_id="ae-pendle",
            event_type="PENDLE_LP_CLOSE",
            position_reference=_make_position_reference("0xpendle"),
        )
        _insert_registry_row(conn, phid="0xpendle", status="closed", primitive="lp", category="pendle_lp")
        conn.commit()
    finally:
        conn.close()

    cell = _cell22(_run(db, primitive="lp"))
    assert cell.status == "PASS"


# ─── Perp coverage ──────────────────────────────────────────────────────


@pytest.mark.parametrize("event_type", ["PERP_CLOSE", "PERP_LIQUIDATE"])
def test_cell22_perp_close_event_types_recognized(tmp_path: Path, event_type: str) -> None:
    db = _new_db(tmp_path)
    conn = sqlite3.connect(str(db))
    try:
        _insert_acct_event(
            conn,
            row_id="ae-1",
            event_type=event_type,
            position_reference=_make_position_reference("0xperp"),
        )
        _insert_registry_row(conn, phid="0xperp", status="closed", primitive="perp", category="perp")
        conn.commit()
    finally:
        conn.close()

    cell = _cell22(_run(db, primitive="perp"))
    assert cell.status == "PASS", f"event_type={event_type} got {cell.status}: {cell.diagnostic}"


# ─── Vault / staking / prediction / Pendle PT coverage ──────────────────


@pytest.mark.parametrize(
    "event_type,primitive_str,category",
    [
        ("VAULT_REDEEM", "vault", "vault"),
        ("UNSTAKE", "staking", "no_accounting"),
        ("PREDICTION_SELL", "prediction", "prediction"),
        ("PREDICTION_REDEEM", "prediction", "prediction"),
        ("PREDICTION_CLOSE", "prediction", "prediction"),
        ("PT_SELL", "swap", "pendle_pt"),
        ("PT_REDEEM", "swap", "pendle_pt"),
    ],
)
def test_cell22_other_close_primitives_recognized(
    tmp_path: Path, event_type: str, primitive_str: str, category: str
) -> None:
    """Every CLOSE event_type from the canonical taxonomy passes when paired with a closed registry row."""
    db = _new_db(tmp_path)
    conn = sqlite3.connect(str(db))
    try:
        _insert_acct_event(
            conn,
            row_id="ae-1",
            event_type=event_type,
            position_reference=_make_position_reference("0xphid"),
        )
        _insert_registry_row(conn, phid="0xphid", status="closed", primitive=primitive_str, category=category)
        conn.commit()
    finally:
        conn.close()

    # The Accountant Test only knows three primitive labels (lp, looping,
    # perp). For other primitives the cell still scores correctly; we run
    # against ``lp`` arbitrarily since the cell predicate is primitive-
    # agnostic (it scans the entire DB, not a primitive sub-slice).
    cell = _cell22(_run(db, primitive="lp"))
    assert cell.status == "PASS", f"event_type={event_type} got {cell.status}: {cell.diagnostic}"


# ─── Taxonomy / SQL parity invariant ─────────────────────────────────────


def test_cell22_sql_close_list_equals_taxonomy() -> None:
    """The SQL CTE list embedded in the UAT card MUST stay in lock-step with
    the taxonomy-derived CLOSE_EVENT_TYPES tuple. If a taxonomy addition
    lands without updating the card's CTE, this test fails — preventing
    silent under-coverage."""
    taxonomy_closes = tuple(sorted(it for it, rec in TAXONOMY.items() if rec.event_kind == EventKind.CLOSE))
    assert CLOSE_EVENT_TYPES == taxonomy_closes, (
        f"CLOSE_EVENT_TYPES drift. Module: {CLOSE_EVENT_TYPES}. Taxonomy now: {taxonomy_closes}."
    )

    # Card's hand-pasted CTE list — keep in lock-step with the SQL block
    # in `docs/internal/uat-cards/VIB-4201.md`. If this assertion fails,
    # update BOTH the card and this list.
    card_cte_list = (
        "CLOSE",
        "DELEVERAGE",
        "LP_CLOSE",
        "PENDLE_LP_CLOSE",
        "PERP_CLOSE",
        "PERP_LIQUIDATE",
        "PREDICTION_CLOSE",
        "PREDICTION_REDEEM",
        "PREDICTION_SELL",
        "PT_REDEEM",
        "PT_SELL",
        "REPAY",
        "UNSTAKE",
        "VAULT_REDEEM",
        "WITHDRAW",
    )
    assert CLOSE_EVENT_TYPES == card_cte_list, (
        "Drift between Python CLOSE_EVENT_TYPES and the UAT card's SQL CTE list. "
        "Update docs/internal/uat-cards/VIB-4201.md AND the card_cte_list literal in this test."
    )


# ─── Score / report rendering ───────────────────────────────────────────


def test_cell22_does_not_break_existing_21_cells(tmp_path: Path) -> None:
    """The cell list still contains the 21 original cells; cell #22 is appended."""
    db = _new_db(tmp_path)
    report = _run(db, primitive="lp")
    cell_ids = [c.cell_id for c in report.cells]
    assert "L5_22" in cell_ids
    # 15 generic + 6 LP + 1 cell-22 = 22 total.
    assert len(cell_ids) == 22, f"expected 22 cells, got {len(cell_ids)}: {cell_ids}"
    # Generic cell ordering preserved.
    expected_generic_prefix = [
        "G1",
        "G2",
        "G3",
        "G4",
        "G5",
        "G6",
        "G7",
        "G8",
        "G9",
        "G10",
        "G11",
        "G12",
        "G13",
        "G14",
        "G15",
    ]
    assert cell_ids[:15] == expected_generic_prefix


def test_cell22_gating_line_excludes_l5_22(tmp_path: Path) -> None:
    """Gating line is computed against the 21 non-L5_22 cells only."""
    db = _new_db(tmp_path)
    conn = sqlite3.connect(str(db))
    try:
        _insert_acct_event(
            conn,
            row_id="ae-1",
            event_type="LP_CLOSE",
            position_reference=_make_position_reference("0xaaaa"),
        )
        _insert_registry_row(conn, phid="0xaaaa", status="closed", primitive="lp", category="lp")
        conn.commit()
    finally:
        conn.close()

    report = _run(db, primitive="lp")
    md = report.format_markdown()
    # Cell #22 should be PASS in this fixture.
    assert "**PASS**" in md
    # Gating line shape: explicit /21 denominator (NOT /22) and L5_22 status.
    assert "Gating: " in md
    assert "/21 PASS" in md
    assert "cell L5_22 informational only this cycle" in md
    # PASS status flagged on the gating line.
    assert "(status: PASS)" in md


def test_cell22_fail_renders_in_cells_table(tmp_path: Path) -> None:
    """A FAIL on cell #22 renders as a regular FAIL row in the cells table AND
    appears in the gating line's status suffix."""
    db = _new_db(tmp_path)
    conn = sqlite3.connect(str(db))
    try:
        _insert_acct_event(
            conn,
            row_id="ae-1",
            event_type="LP_CLOSE",
            position_reference=_make_position_reference("0xaaaa"),
        )
        # No registry row — forward orphan → FAIL.
        conn.commit()
    finally:
        conn.close()

    report = _run(db, primitive="lp")
    md = report.format_markdown()
    assert "L5_22" in md
    # The cells table contains the FAIL row.
    cell_22_row = [line for line in md.splitlines() if "| L5_22 |" in line]
    assert cell_22_row, "L5_22 row missing from cells markdown table"
    assert "**FAIL**" in cell_22_row[0]
    # Gating line surfaces the FAIL status.
    assert "(status: FAIL)" in md


def test_cell22_xfail_renders_in_cells_table(tmp_path: Path) -> None:
    """An XFAIL on cell #22 renders cleanly with the XFAIL status visible on the gating line."""
    db = _new_db(tmp_path)
    # Both tables empty → no CLOSE events, no closed rows → XFAIL.
    report = _run(db, primitive="lp")
    md = report.format_markdown()
    assert "(status: XFAIL)" in md
    assert "/21 PASS" in md
