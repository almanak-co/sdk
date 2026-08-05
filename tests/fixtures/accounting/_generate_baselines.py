"""Synthetic accounting fixture generator (VIB-4162 / T2 of Primitives Refactor).

Builds three deterministic SQLite fixtures — LP, Looping, Perp — that cover
one full lifecycle of each primitive. The fixtures are read by:

* :func:`almanak.framework.accounting.accountant_test.run_against_sqlite` —
  the 21-cell Accountant Test scoring matrix.
* :mod:`tests.unit.accounting.test_per_primitive_matching_version` — the
  per-primitive ``MATCHING_POLICY_VERSIONS`` isolation proof.
* :mod:`tests.unit.accounting.test_no_scoring_drift` — the pre-T2 vs post-T2
  cell-status anchor.

Determinism is required: running the public API multiple times produces
byte-identical SQLite files. Every UUID, timestamp and Decimal is seeded
from a fixed clock + counter — no ``datetime.now()``, no ``uuid.uuid4()``.

Per-primitive ``matching_policy_version`` stamping
--------------------------------------------------

The generator imports ``MATCHING_POLICY_VERSIONS`` from
``almanak.framework.accounting.payload_schemas`` when it exists (post-T2).
At precursor time the symbol does NOT exist and the global v3
``MATCHING_POLICY_VERSION`` is used instead, matching the writer behaviour
the freezer ran against. This dual-mode lookup is the contract that lets
the same generator script produce both the pre-T2 baseline (global v3) and
the post-T2 baseline (per-primitive) without any conditional branches in
the consumer test code.
"""

from __future__ import annotations

import json
import sqlite3
from decimal import Decimal
from pathlib import Path

from almanak.framework.accounting.payload_schemas import (
    FORMULA_VERSION,
    MATCHING_POLICY_VERSION as _GLOBAL_MATCHING_POLICY_VERSION,
    PRIMITIVE_VERSIONS,
    SCHEMA_VERSION,
)
from almanak.framework.primitives.types import Primitive

# Per-primitive map only exists post-T2; fall back to the global value at
# precursor time so the same generator can produce both baselines.
try:
    from almanak.framework.accounting.payload_schemas import MATCHING_POLICY_VERSIONS as _PER_PRIMITIVE_VERSIONS  # type: ignore[attr-defined]
except ImportError:  # pragma: no cover — precursor path
    _PER_PRIMITIVE_VERSIONS = None

try:
    from almanak.framework.primitives.types import Primitive as _Primitive
except ImportError:  # pragma: no cover — defensive
    _Primitive = None


# ─── Fixed determinism inputs ────────────────────────────────────────────
_BASE_TIMESTAMP = "2026-05-09T00:00:00+00:00"
_DEPLOYMENT_ID = "AccountantBaseline:fixture"
_WALLET = "0x0000000000000000000000000000000000000abc"
_EXECUTION_MODE = "paper"


def _ts(offset_seconds: int) -> str:
    """Deterministic timestamp = base + offset (no clock reads)."""
    from datetime import datetime, timedelta

    base = datetime.fromisoformat(_BASE_TIMESTAMP)
    return (base + timedelta(seconds=offset_seconds)).isoformat()


def _stable_id(prefix: str, n: int) -> str:
    """UUID-shaped deterministic id: prefix-NNN…NNN (no randomness)."""
    return f"{prefix}-{n:032d}"


def _matching_version_for(primitive_name: str) -> int:
    """Look up the matching_policy_version for a primitive.

    Pre-T2: returns global v3. Post-T2: returns the per-primitive value.
    Names accepted: 'lp', 'lending', 'perp', 'utility', 'swap'.
    """
    if _PER_PRIMITIVE_VERSIONS is None or _Primitive is None:
        return _GLOBAL_MATCHING_POLICY_VERSION
    enum_member = getattr(_Primitive, primitive_name.upper(), None)
    if enum_member is None:
        return _GLOBAL_MATCHING_POLICY_VERSION
    return _PER_PRIMITIVE_VERSIONS.get(enum_member, _GLOBAL_MATCHING_POLICY_VERSION)


# ─── Schema (the 6 SDK accounting tables we actually need) ───────────────
# Kept in this module rather than importing from the live store so the
# generator cannot drift from the columns the cells read. CREATE TABLE
# bodies match the live `tests/fixtures/accounting/baseline/lp.db` and
# the production `state/backends/sqlite.py` schema; only `position_state_snapshots`
# is added here because the existing baseline DB lacks it (Track C is
# library-only at present).
_DDL: tuple[str, ...] = (
    """
    CREATE TABLE transaction_ledger (
        id TEXT PRIMARY KEY,
        cycle_id TEXT NOT NULL,
        deployment_id TEXT NOT NULL,
        execution_mode TEXT DEFAULT '',
        timestamp TEXT NOT NULL,
        intent_type TEXT NOT NULL,
        token_in TEXT,
        amount_in TEXT,
        token_out TEXT,
        amount_out TEXT,
        effective_price TEXT,
        slippage_bps REAL,
        gas_used INTEGER,
        gas_usd TEXT,
        tx_hash TEXT,
        chain TEXT,
        protocol TEXT,
        success BOOLEAN NOT NULL DEFAULT 1,
        error TEXT,
        extracted_data_json TEXT DEFAULT '',
        price_inputs_json TEXT DEFAULT '',
        pre_state_json TEXT DEFAULT '',
        post_state_json TEXT DEFAULT ''
    )
    """,
    """
    CREATE TABLE accounting_events (
        id TEXT PRIMARY KEY,
        deployment_id TEXT NOT NULL,
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
        schema_version INTEGER NOT NULL DEFAULT 1
    )
    """,
    """
    CREATE TABLE portfolio_snapshots (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        deployment_id TEXT NOT NULL,
        cycle_id TEXT DEFAULT '',
        execution_mode TEXT DEFAULT '',
        timestamp TEXT NOT NULL,
        iteration_number INTEGER DEFAULT 0,
        total_value_usd TEXT NOT NULL,
        available_cash_usd TEXT NOT NULL,
        deployed_capital_usd TEXT DEFAULT '0',
        wallet_total_value_usd TEXT DEFAULT '0',
        value_confidence TEXT DEFAULT 'HIGH',
        positions_json TEXT NOT NULL,
        token_prices_json TEXT DEFAULT '{}',
        wallet_balances_json TEXT DEFAULT '[]',
        chain TEXT,
        created_at TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE position_events (
        id TEXT PRIMARY KEY,
        deployment_id TEXT NOT NULL,
        cycle_id TEXT DEFAULT '',
        execution_mode TEXT DEFAULT '',
        position_id TEXT NOT NULL,
        position_type TEXT NOT NULL,
        event_type TEXT NOT NULL,
        timestamp TEXT NOT NULL,
        protocol TEXT,
        chain TEXT,
        token0 TEXT,
        token1 TEXT,
        amount0 TEXT,
        amount1 TEXT,
        value_usd TEXT,
        tick_lower INTEGER,
        tick_upper INTEGER,
        liquidity TEXT,
        in_range BOOLEAN,
        fees_token0 TEXT,
        fees_token1 TEXT,
        leverage TEXT,
        entry_price TEXT,
        mark_price TEXT,
        unrealized_pnl TEXT,
        is_long BOOLEAN,
        tx_hash TEXT,
        gas_usd TEXT,
        ledger_entry_id TEXT,
        protocol_fees_usd TEXT DEFAULT '',
        attribution_json TEXT DEFAULT '{}',
        attribution_version INTEGER DEFAULT 0
    )
    """,
    """
    CREATE TABLE portfolio_metrics (
        deployment_id TEXT PRIMARY KEY,
        initial_value_usd TEXT NOT NULL,
        initial_timestamp TEXT NOT NULL,
        deposits_usd TEXT DEFAULT '0',
        withdrawals_usd TEXT DEFAULT '0',
        gas_spent_usd TEXT DEFAULT '0',
        total_value_usd TEXT DEFAULT '0',
        positions_json TEXT DEFAULT '[]',
        cycle_id TEXT,
        execution_mode TEXT DEFAULT '',
        is_complete BOOLEAN DEFAULT 1,
        updated_at TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE position_state_snapshots (
        snapshot_id INTEGER,
        deployment_id TEXT,
        cycle_id TEXT,
        timestamp TEXT,
        position_id TEXT,
        position_type TEXT,
        current_tick INTEGER,
        in_range BOOLEAN,
        liquidity TEXT,
        sqrt_price_x96 TEXT,
        supply_balance TEXT,
        borrow_balance TEXT,
        health_factor TEXT,
        supply_apy_pct TEXT,
        borrow_apy_pct TEXT,
        interest_accrued_since_last TEXT,
        mark_price TEXT,
        unrealized_pnl TEXT,
        funding_accrued_since_last TEXT,
        liquidation_price TEXT,
        margin_utilisation_pct TEXT,
        value_confidence TEXT,
        delta_vs_protocol_pct TEXT,
        schema_version INTEGER,
        formula_version INTEGER,
        matching_policy_version INTEGER
    )
    """,
)


def _connect(db_path: Path) -> sqlite3.Connection:
    """Create a fresh DB and apply the DDL."""
    if db_path.exists():
        db_path.unlink()
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path))
    for stmt in _DDL:
        conn.execute(stmt)
    return conn


def _stamp_payload(payload: dict, primitive_name: str) -> dict:
    """Stamp the version triple onto a payload dict.

    Mirrors :func:`almanak.framework.accounting.writer.augment_accounting_payload`
    on the version-fields side; the alias-projection side is irrelevant
    for the synthetic fixtures (we author the payloads directly with the
    spec field names).
    """
    payload = dict(payload)
    payload["schema_version"] = SCHEMA_VERSION
    payload["formula_version"] = FORMULA_VERSION
    payload["matching_policy_version"] = _matching_version_for(primitive_name)
    return payload


def _insert_ledger(
    conn: sqlite3.Connection,
    *,
    row_id: str,
    cycle_id: str,
    timestamp: str,
    intent_type: str,
    token_in: str,
    amount_in: str,
    token_out: str,
    amount_out: str,
    chain: str,
    protocol: str,
    tx_hash: str,
    extracted: dict | None = None,
    pre_state: dict | None = None,
    post_state: dict | None = None,
    price_inputs: dict | None = None,
    gas_usd: str = "0.5",
) -> None:
    # ``gas_usd`` defaults to the historical hard-coded "0.5" so every fixture
    # authored before VIB-6560 regenerates byte-identically. The debt-open
    # lending fixture overrides it per row because its G6 reconciliation ties
    # EXACTLY (Σ gas is the only component term), and a flat $0.50 on a $5
    # portfolio would swamp the debt term this fixture exists to expose.
    conn.execute(
        """
        INSERT INTO transaction_ledger
        (id, cycle_id, deployment_id, execution_mode, timestamp,
         intent_type, token_in, amount_in, token_out, amount_out,
         effective_price, slippage_bps, gas_used, gas_usd, tx_hash, chain, protocol,
         success, error, extracted_data_json, price_inputs_json,
         pre_state_json, post_state_json)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, '', ?, ?, ?, ?)
        """,
        (
            row_id,
            cycle_id,
            _DEPLOYMENT_ID,
            _EXECUTION_MODE,
            timestamp,
            intent_type,
            token_in,
            amount_in,
            token_out,
            amount_out,
            "1.0",
            5.0,
            150_000,
            gas_usd,
            tx_hash,
            chain,
            protocol,
            json.dumps(extracted or {}),
            json.dumps(price_inputs or {}),
            json.dumps(pre_state or {}),
            json.dumps(post_state or {}),
        ),
    )


def _insert_acct_event(
    conn: sqlite3.Connection,
    *,
    row_id: str,
    cycle_id: str,
    timestamp: str,
    chain: str,
    protocol: str,
    event_type: str,
    position_key: str,
    ledger_entry_id: str,
    tx_hash: str,
    payload: dict,
    primitive_name: str,
    stamp: bool = True,
) -> None:
    # ``stamp=False`` inserts the payload verbatim — used by the settlement
    # fixture, whose ``SettlementAccountingEvent`` carries ``schema_version`` +
    # ``primitive_version`` but (as a capital event, no lot matching) NEITHER
    # ``formula_version`` NOR ``matching_policy_version``. ``_stamp_payload`` would
    # inject those two and mis-shape the row vs. the real event
    # (settlement_accounting.py::to_payload_json).
    stamped = _stamp_payload(payload, primitive_name) if stamp else dict(payload)
    conn.execute(
        """
        INSERT INTO accounting_events
        (id, deployment_id, cycle_id, execution_mode, timestamp,
         chain, protocol, wallet_address, event_type, position_key,
         ledger_entry_id, tx_hash, confidence, payload_json, schema_version)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            row_id,
            _DEPLOYMENT_ID,
            cycle_id,
            _EXECUTION_MODE,
            timestamp,
            chain,
            protocol,
            _WALLET,
            event_type,
            position_key,
            ledger_entry_id,
            tx_hash,
            stamped.get("confidence", "HIGH"),
            json.dumps(stamped, default=str),
            SCHEMA_VERSION,
        ),
    )


def _insert_position_event(
    conn: sqlite3.Connection,
    *,
    row_id: str,
    cycle_id: str,
    timestamp: str,
    position_id: str,
    position_type: str,
    event_type: str,
    chain: str,
    protocol: str,
    token0: str,
    token1: str,
    amount0: str,
    amount1: str,
    value_usd: str,
    tx_hash: str,
    ledger_entry_id: str,
    tick_lower: int | None = None,
    tick_upper: int | None = None,
    liquidity: str = "",
    in_range: bool | None = None,
    fees_token0: str = "",
    fees_token1: str = "",
    gas_usd: str = "0.5",
) -> None:
    conn.execute(
        """
        INSERT INTO position_events
        (id, deployment_id, cycle_id, execution_mode, position_id, position_type,
         event_type, timestamp, protocol, chain, token0, token1, amount0, amount1,
         value_usd, tick_lower, tick_upper, liquidity, in_range, fees_token0, fees_token1,
         tx_hash, gas_usd, ledger_entry_id, protocol_fees_usd,
         attribution_json, attribution_version)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            row_id,
            _DEPLOYMENT_ID,
            cycle_id,
            _EXECUTION_MODE,
            position_id,
            position_type,
            event_type,
            timestamp,
            protocol,
            chain,
            token0,
            token1,
            amount0,
            amount1,
            value_usd,
            tick_lower,
            tick_upper,
            liquidity,
            in_range,
            fees_token0,
            fees_token1,
            tx_hash,
            gas_usd,
            ledger_entry_id,
            "0",
            "{}",
            0,
        ),
    )


def _insert_portfolio_snapshot(
    conn: sqlite3.Connection,
    *,
    cycle_id: str,
    iteration_number: int,
    timestamp: str,
    total_value_usd: str,
    available_cash_usd: str,
    deployed_capital_usd: str,
    chain: str,
    positions_json: str = "[]",
    token_prices_json: str = "{}",
    wallet_balances_json: str = "[]",
) -> None:
    # The three JSON columns default to the historical empty literals so every
    # fixture authored before VIB-6560 regenerates byte-identically. Those
    # defaults are exactly the structural blindness VIB-6560 measured on the
    # ``looping`` fixture (1 distinct positions_json = ``[]``): a NAV fold over
    # positions reads the same on every row, so no position-sourced defect can
    # move a cell. ``looping_debt_open`` populates all three.
    conn.execute(
        """
        INSERT INTO portfolio_snapshots
        (deployment_id, cycle_id, execution_mode, timestamp,
         iteration_number, total_value_usd, available_cash_usd, deployed_capital_usd,
         wallet_total_value_usd, value_confidence, positions_json, token_prices_json,
         wallet_balances_json, chain, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            _DEPLOYMENT_ID,
            cycle_id,
            _EXECUTION_MODE,
            timestamp,
            iteration_number,
            total_value_usd,
            available_cash_usd,
            deployed_capital_usd,
            total_value_usd,
            "HIGH",
            positions_json,
            token_prices_json,
            wallet_balances_json,
            chain,
            timestamp,
        ),
    )


def _insert_portfolio_metrics(
    conn: sqlite3.Connection,
    *,
    initial_value_usd: str = "1000.00",
    gas_spent_usd: str = "2.0",
    total_value_usd: str = "1010.00",
) -> None:
    conn.execute(
        """
        INSERT INTO portfolio_metrics
        (deployment_id, initial_value_usd, initial_timestamp, deposits_usd,
         withdrawals_usd, gas_spent_usd, total_value_usd, positions_json,
         cycle_id, execution_mode, is_complete, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            _DEPLOYMENT_ID,
            initial_value_usd,
            _ts(0),
            "0",
            "0",
            gas_spent_usd,
            total_value_usd,
            "[]",
            "cycle-001",
            _EXECUTION_MODE,
            1,
            _ts(3600),
        ),
    )


# ─── LP fixture (Uniswap V3 lifecycle: SWAP → LP_OPEN → LP_COLLECT_FEES → LP_CLOSE) ───
def generate_lp_fixture(db_path: str | Path) -> None:
    """Generate the canonical LP fixture: 4 ledger rows, 3 position events, 3 acct events."""
    db_path = Path(db_path)
    conn = _connect(db_path)
    try:
        cycle = "cycle-lp-001"
        chain = "arbitrum"
        protocol = "uniswap_v3"
        position_key = "lp:arbitrum:uniswap_v3:wallet:WETH-USDC-3000"

        # T0: SWAP USDC → WETH (entry)
        ledger_swap = _stable_id("tl-lp", 1)
        _insert_ledger(
            conn,
            row_id=ledger_swap,
            cycle_id=cycle,
            timestamp=_ts(0),
            intent_type="SWAP",
            token_in="USDC",
            amount_in="500.0",
            token_out="WETH",
            amount_out="0.2",
            chain=chain,
            protocol=protocol,
            tx_hash="0x1111",
        )
        _insert_acct_event(
            conn,
            row_id=_stable_id("ae-lp", 1),
            cycle_id=cycle,
            timestamp=_ts(0),
            chain=chain,
            protocol=protocol,
            event_type="SWAP",
            position_key="swap:WETH-USDC",
            ledger_entry_id=ledger_swap,
            tx_hash="0x1111",
            payload={
                "event_type": "SWAP",
                "protocol": protocol,
                "token_in": "USDC",
                "token_out": "WETH",
                "amount_in": "500.0",
                "amount_out": "0.2",
                "amount_in_usd": "500.0",
                "amount_out_usd": "500.0",
                "effective_price": "2500.0",
                "slippage_bps": "5",
                "realized_pnl_usd": None,
                "cost_basis_recorded": True,
                "gas_usd": "0.5",
                "confidence": "HIGH",
            },
            primitive_name="swap",
        )

        # T1: LP_OPEN
        ledger_open = _stable_id("tl-lp", 2)
        _insert_ledger(
            conn,
            row_id=ledger_open,
            cycle_id=cycle,
            timestamp=_ts(60),
            intent_type="LP_OPEN",
            token_in="WETH",
            amount_in="0.2",
            token_out="LP-NFT",
            amount_out="1",
            chain=chain,
            protocol=protocol,
            tx_hash="0x2222",
            pre_state={"reserves": "1000", "tick": -200000},
            post_state={"liquidity": "1234567890", "tick": -200000},
        )
        _insert_acct_event(
            conn,
            row_id=_stable_id("ae-lp", 2),
            cycle_id=cycle,
            timestamp=_ts(60),
            chain=chain,
            protocol=protocol,
            event_type="LP_OPEN",
            position_key=position_key,
            ledger_entry_id=ledger_open,
            tx_hash="0x2222",
            payload={
                "event_type": "LP_OPEN",
                "protocol": protocol,
                "position_key": position_key,
                "pool_address": "0xpool",
                "token0": "WETH",
                "token1": "USDC",
                "amount0": "0.2",
                "amount1": "500.0",
                "amount0_usd": "500.0",
                "amount1_usd": "500.0",
                "cost_basis_usd": "1000.0",
                "tick_lower": -201000,
                "tick_upper": -199000,
                "liquidity": 1234567890,
                "current_tick": -200000,
                "in_range": True,
                "confidence": "HIGH",
            },
            primitive_name="lp",
        )
        _insert_position_event(
            conn,
            row_id=_stable_id("pe-lp", 1),
            cycle_id=cycle,
            timestamp=_ts(60),
            position_id=position_key,
            position_type="LP",
            event_type="OPEN",
            chain=chain,
            protocol=protocol,
            token0="WETH",
            token1="USDC",
            amount0="0.2",
            amount1="500.0",
            value_usd="1000.0",
            tx_hash="0x2222",
            ledger_entry_id=ledger_open,
            tick_lower=-201000,
            tick_upper=-199000,
            liquidity="1234567890",
            in_range=True,
        )

        # T2: LP_COLLECT_FEES
        ledger_collect = _stable_id("tl-lp", 3)
        _insert_ledger(
            conn,
            row_id=ledger_collect,
            cycle_id=cycle,
            timestamp=_ts(120),
            intent_type="LP_COLLECT_FEES",
            token_in="LP-NFT",
            amount_in="1",
            token_out="USDC",
            amount_out="3.0",
            chain=chain,
            protocol=protocol,
            tx_hash="0x3333",
        )
        _insert_position_event(
            conn,
            row_id=_stable_id("pe-lp", 2),
            cycle_id=cycle,
            timestamp=_ts(120),
            position_id=position_key,
            position_type="LP",
            event_type="COLLECT_FEES",
            chain=chain,
            protocol=protocol,
            token0="WETH",
            token1="USDC",
            amount0="0.001",
            amount1="2.0",
            value_usd="5.5",
            tx_hash="0x3333",
            ledger_entry_id=ledger_collect,
            tick_lower=-201000,
            tick_upper=-199000,
            liquidity="1234567890",
            in_range=True,
            fees_token0="0.001",
            fees_token1="2.0",
        )

        # T3: LP_CLOSE
        ledger_close = _stable_id("tl-lp", 4)
        _insert_ledger(
            conn,
            row_id=ledger_close,
            cycle_id=cycle,
            timestamp=_ts(180),
            intent_type="LP_CLOSE",
            token_in="LP-NFT",
            amount_in="1",
            token_out="WETH",
            amount_out="0.21",
            chain=chain,
            protocol=protocol,
            tx_hash="0x4444",
            pre_state={"liquidity": "1234567890"},
            post_state={"liquidity": "0"},
        )
        _insert_acct_event(
            conn,
            row_id=_stable_id("ae-lp", 3),
            cycle_id=cycle,
            timestamp=_ts(180),
            chain=chain,
            protocol=protocol,
            event_type="LP_CLOSE",
            position_key=position_key,
            ledger_entry_id=ledger_close,
            tx_hash="0x4444",
            payload={
                "event_type": "LP_CLOSE",
                "protocol": protocol,
                "position_key": position_key,
                "pool_address": "0xpool",
                "token0": "WETH",
                "token1": "USDC",
                "amount0": "0.21",
                "amount1": "525.0",
                "amount0_usd": "525.0",
                "amount1_usd": "525.0",
                "fees0_collected": "0.001",
                "fees1_collected": "2.5",
                "fees_total_usd": "5.0",
                "realized_pnl_usd": "10.0",
                "il_usd": "0.0",
                "hodl_value_usd": "1000.0",
                "confidence": "HIGH",
            },
            primitive_name="lp",
        )
        _insert_position_event(
            conn,
            row_id=_stable_id("pe-lp", 3),
            cycle_id=cycle,
            timestamp=_ts(180),
            position_id=position_key,
            position_type="LP",
            event_type="CLOSE",
            chain=chain,
            protocol=protocol,
            token0="WETH",
            token1="USDC",
            amount0="0.21",
            amount1="525.0",
            value_usd="1050.0",
            tx_hash="0x4444",
            ledger_entry_id=ledger_close,
            tick_lower=-201000,
            tick_upper=-199000,
            liquidity="0",
            in_range=False,
            fees_token0="0.001",
            fees_token1="2.5",
        )

        # Snapshots — one pre and post per ledger row would be 8; keep 4 to
        # match "ledger_row_count"; G4/G5/G8 just need >0 rows.
        for i, offset in enumerate((0, 60, 120, 180), start=1):
            _insert_portfolio_snapshot(
                conn,
                cycle_id=cycle,
                iteration_number=i,
                timestamp=_ts(offset),
                total_value_usd=str(Decimal("1000") + Decimal(i)),
                available_cash_usd="500.0",
                deployed_capital_usd="500.0",
                chain=chain,
            )

        _insert_portfolio_metrics(conn)
        conn.commit()
    finally:
        conn.close()


# ─── Looping fixture (Aave V3 lifecycle) ─────────────────────────────────
def generate_looping_fixture(db_path: str | Path) -> None:
    """Generate the canonical Looping fixture: 6 ledger rows, 6 position events, 6 acct events."""
    db_path = Path(db_path)
    conn = _connect(db_path)
    try:
        cycle = "cycle-loop-001"
        chain = "arbitrum"
        protocol = "aave_v3"
        coll_key = "lending:arbitrum:aave_v3:wallet:USDC"
        debt_key = "lending:arbitrum:aave_v3:wallet:WETH"

        steps = [
            ("SUPPLY", "USDC", "1000.0", "aUSDC", "1000.0", "0xa1", coll_key, "LENDING_COLLATERAL", "OPEN", "lending"),
            ("BORROW", "WETH", "0.2", "WETH", "0.2", "0xa2", debt_key, "LENDING_DEBT", "OPEN", "lending"),
            ("BORROW", "WETH", "0.05", "WETH", "0.05", "0xa3", debt_key, "LENDING_DEBT", "INCREASE", "lending"),
            ("REPAY", "WETH", "0.1", "WETH", "0.1", "0xa4", debt_key, "LENDING_DEBT", "DECREASE", "lending"),
            ("REPAY", "WETH", "0.15", "WETH", "0.15", "0xa5", debt_key, "LENDING_DEBT", "CLOSE", "lending"),
            ("WITHDRAW", "aUSDC", "1000.0", "USDC", "1000.0", "0xa6", coll_key, "LENDING_COLLATERAL", "CLOSE", "lending"),
        ]

        for idx, (
            intent_type,
            tin,
            ain,
            tout,
            aout,
            tx,
            pos_key,
            pos_type,
            ev_type,
            primitive,
        ) in enumerate(steps, start=1):
            ts = _ts(60 * idx)
            ledger_id = _stable_id("tl-loop", idx)
            _insert_ledger(
                conn,
                row_id=ledger_id,
                cycle_id=cycle,
                timestamp=ts,
                intent_type=intent_type,
                token_in=tin,
                amount_in=ain,
                token_out=tout,
                amount_out=aout,
                chain=chain,
                protocol=protocol,
                tx_hash=tx,
                post_state={
                    "collateral_value_usd": "1000.0" if pos_type == "LENDING_COLLATERAL" and ev_type != "CLOSE" else "0",
                    "debt_value_usd": "500.0" if pos_type == "LENDING_DEBT" and ev_type not in ("CLOSE",) else "0",
                    "health_factor": "2.0",
                },
            )

            payload = _build_lending_payload(intent_type, pos_key, ain)
            _insert_acct_event(
                conn,
                row_id=_stable_id("ae-loop", idx),
                cycle_id=cycle,
                timestamp=ts,
                chain=chain,
                protocol=protocol,
                event_type=intent_type,
                position_key=pos_key,
                ledger_entry_id=ledger_id,
                tx_hash=tx,
                payload=payload,
                primitive_name=primitive,
            )
            _insert_position_event(
                conn,
                row_id=_stable_id("pe-loop", idx),
                cycle_id=cycle,
                timestamp=ts,
                position_id=pos_key,
                position_type=pos_type,
                event_type=ev_type,
                chain=chain,
                protocol=protocol,
                token0=tin,
                token1="",
                amount0=ain,
                amount1="",
                value_usd=ain,
                tx_hash=tx,
                ledger_entry_id=ledger_id,
            )

        for i, offset in enumerate((60, 120, 180, 240, 300, 360), start=1):
            _insert_portfolio_snapshot(
                conn,
                cycle_id=cycle,
                iteration_number=i,
                timestamp=_ts(offset),
                total_value_usd=str(Decimal("1000") + Decimal(i)),
                available_cash_usd="0",
                deployed_capital_usd="1000.0",
                chain=chain,
            )

        _insert_portfolio_metrics(conn)
        conn.commit()
    finally:
        conn.close()


def _build_lending_payload(intent_type: str, position_key: str, amount: str) -> dict:
    base = {
        "event_type": intent_type,
        "protocol": "aave_v3",
        "asset": "USDC" if intent_type in ("SUPPLY", "WITHDRAW") else "WETH",
        "amount": amount,
        "amount_usd": amount,
        "confidence": "HIGH",
        "position_key": position_key,
    }
    if intent_type == "SUPPLY":
        base["supply_apr_pct"] = "3.0"
        base["health_factor_after"] = "2.0"
        base["cost_basis_usd"] = amount
    elif intent_type == "WITHDRAW":
        base["interest_accrued_usd"] = "0.0"
        base["interest_delta_usd"] = "0.0"
        base["realized_pnl_usd"] = "0.0"
        base["health_factor_after"] = "2.0"
    elif intent_type == "BORROW":
        base["borrowed_amount"] = amount
        base["borrowed_amount_usd"] = amount
        base["borrow_apr_pct"] = "4.5"
        base["health_factor_after"] = "2.0"
        base.pop("amount", None)
        base.pop("amount_usd", None)
    elif intent_type in ("REPAY", "DELEVERAGE"):
        base["principal_repaid"] = amount
        base["interest_paid"] = "0.001"
        base["principal_repaid_usd"] = amount
        base["interest_paid_usd"] = "0.001"
        base["principal_delta_usd"] = amount
        base["interest_delta_usd"] = "0.001"
        base["health_factor_after"] = "2.0"
    return base


# ─── Looping "debt open at the endpoint" fixture (VIB-6560) ───────────────
#
# WHY THIS FIXTURE EXISTS
# -----------------------
# ``generate_looping_fixture`` above is structurally blind. Measured 2026-08-05:
# 6 snapshots, 1 distinct ``available_cash_usd`` (= 0), 1 distinct
# ``positions_json`` (= ``[]``). No position ever appears, so ANY cell folding
# NAV from positions reads identically on every row regardless of the events —
# which is why VIB-5857 (``_snapshot_equity`` measures equity GROSS of debt)
# moved 0 of 308 cells across the whole committed corpus.
#
# The load-bearing property is NOT "leveraged" — the flat fixture already emits
# BORROW events (2) and REPAY events (2). It is **debt still open at the
# endpoint the wallet method brackets on**. A completed run tears down, so its
# open debt lives in the middle and its final snapshot is clean; only a
# pre-teardown capture carries a live debt leg at the last snapshot.
#
# PROVENANCE — see ``looping_debt_open/capture_provenance.json``. The leg shape,
# the field sets inside ``details``, the envelope form of ``positions_json`` and
# the endpoint USD marks are taken from a REAL pre-teardown capture (Aave V3 on
# BSC, run ``20260803-0430-noneth8-pcs-carry-bsc``), not authored from intuition.
# Disjoint reserves only: collateral is WBNB, debt is USDC. Same-reserve
# SUPPLY+BORROW is the VIB-5857 landmine and belongs in a later, explicit
# fixture — encoding an invented same-reserve payload in the first "can fail"
# corpus would risk freezing the landmine as truth.
#
# WHAT MAKES IT NON-VACUOUS (the arithmetic, so a reader can re-derive it)
# ------------------------------------------------------------------------
# ``total_value_usd`` is Σ POSITIVE ``value_usd`` — the debt leg is dropped
# (VIB-3614). So for any snapshot::
#
#     equity_gross = total_value_usd + available_cash_usd
#                  = equity_net + debt_mark
#
# Every flow in the ladder below is NAV-conservative (SUPPLY moves cash into a
# collateral leg; BORROW mints a debt leg AND the matching token leg; REPAY and
# WITHDRAW reverse them), interest is a MEASURED zero at every leg, and every
# token price is flat across the whole run. So the ONLY thing that moves true
# NAV is gas, and the component method sums exactly that::
#
#     wallet_pnl_net   = -Σ gas          component_pnl = -Σ gas   → gap = 0
#     wallet_pnl_gross = -Σ gas + D      component_pnl = -Σ gas   → gap = D
#
# where ``D`` is ``debt_mark`` at the final snapshot (debt_mark at the first
# snapshot is zero — it pre-dates every transaction). G6's ``gap_usd`` therefore
# moves by EXACTLY ``D`` between the gross and net equity projections, which is
# what ``test_looping_debt_open_vib6560.py`` asserts.
#
# Interest is a measured ``"0"`` (Decimal zero), never ``None`` and never ``""``
# — Empty != Zero. A same-block borrow/repay on a forked chain genuinely accrues
# zero at 18dp, and pinning it to zero keeps the fixture's ONE load-bearing
# property (open debt at the endpoint) un-confounded by an accrual term the
# component method books at REPAY while the wallet method books continuously.

# Flat marks. A constant price across the bracket collapses G6's ambient
# inventory-revaluation term (blueprint 27 §11.5) to exactly zero, so no price
# noise competes with the debt term under test.
_LDO_PRICE_BNB = Decimal("584.67")
_LDO_PRICE_USDC = Decimal("1.00")

# Endpoint USD marks, verbatim from the capture's final snapshot.
_LDO_SUPPLY_END_USD = Decimal("2.923350182028013944740031133")
_LDO_DEBT_END_USD = Decimal("0.87001967417207508600")
# Pre-deleverage magnitudes: the run withdrew $1.00 of collateral and repaid
# $0.50 of principal, so it entered the bracket this much larger.
_LDO_WITHDRAW_USD = Decimal("1.00")
_LDO_REPAY_PRINCIPAL_USD = Decimal("0.50")
_LDO_SUPPLY_OPEN_USD = _LDO_SUPPLY_END_USD + _LDO_WITHDRAW_USD
_LDO_DEBT_OPEN_USD = _LDO_DEBT_END_USD + _LDO_REPAY_PRINCIPAL_USD

# Native-gas ladder, in BNB. 0.002 BNB is the capture's actual FUND_PLAN.
_LDO_BNB_START = Decimal("0.002")
_LDO_GAS_BNB = (
    Decimal("0.000021"),
    Decimal("0.0000185"),
    Decimal("0.000016"),
    Decimal("0.0000142"),
)

_LDO_CHAIN = "bsc"
_LDO_PROTOCOL = "aave_v3"
_LDO_COLL_KEY = "lending:bsc:aave_v3:wallet:WBNB"
_LDO_DEBT_KEY = "lending:bsc:aave_v3:wallet:USDC"
_LDO_WBNB_ADDR = "0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c"
_LDO_USDC_ADDR = "0x8ac76a51cc950d9822d68b83fe1ad97b32cd580d"
_LDO_HEALTH_FACTOR = "2.519169603883311198"


def _ldo_units(value_usd: Decimal, price: Decimal) -> str:
    """Token units backing ``value_usd`` at ``price``, quantised to 18dp.

    The USD marks are the authoritative (capture-derived) money; the ``details``
    balances are DERIVED from them so the two can never disagree by more than
    one wei-equivalent. Production does the reverse (units are read on-chain and
    priced), and the capture shows the two are not exactly reciprocal there
    either — its display price is ``584.67`` while the valuer used a
    higher-precision mark. Nothing the Accountant Test reads depends on this
    field; it exists so the fixture is inspectable, not so it is authoritative.
    """
    return str((value_usd / price).quantize(Decimal("1E-18")))


def _ldo_supply_leg(value_usd: Decimal, ts: str, ledger_entry_id: str) -> dict:
    """An Aave V3 SUPPLY (collateral) leg, field-for-field from the capture.

    ``cost_basis_usd == value_usd`` (so ``unrealized_pnl_usd`` is a measured
    zero): the fixture pins zero accrual, see the module comment above.
    """
    units = _ldo_units(value_usd, _LDO_PRICE_BNB)
    return {
        "position_type": "SUPPLY",
        "protocol": _LDO_PROTOCOL,
        "chain": _LDO_CHAIN,
        "value_usd": str(value_usd),
        "label": "aave_v3 SUPPLY",
        "tokens": [],
        "details": {
            "amount": units,
            "asset": "WBNB",
            "asset_address": _LDO_WBNB_ADDR,
            "borrow_balance": "0",
            "collateral_enabled": True,
            "debt_value_usd": "0E-13",
            "health_factor": _LDO_HEALTH_FACTOR,
            "net_value_usd": str(value_usd),
            "stable_debt_balance": "0",
            "supply_apy_pct": "0.010846239415788261974020200",
            "supply_balance": units,
            "supply_value_usd": str(value_usd),
            "valuation_source": "on_chain",
            "variable_debt_balance": "0",
            "wallet_address": _WALLET,
        },
        "cost_basis_usd": str(value_usd),
        "unrealized_pnl_usd": "0",
        "entry_timestamp": _ts(60),
        "last_update_timestamp": ts,
        "ledger_entry_id": ledger_entry_id,
    }


def _ldo_borrow_leg(debt_usd: Decimal, ts: str, ledger_entry_id: str) -> dict:
    """An Aave V3 BORROW (debt) leg — the SIGNED-NEGATIVE leg this fixture exists
    to put at the endpoint. ``value_usd`` is negative per the canonical money
    representation (blueprint 27 §7.11); ``net_debt_from_positions_json`` reads
    exactly this sign to produce ``debt_mark``.
    """
    units = _ldo_units(debt_usd, _LDO_PRICE_USDC)
    return {
        "position_type": "BORROW",
        "protocol": _LDO_PROTOCOL,
        "chain": _LDO_CHAIN,
        "value_usd": str(-debt_usd),
        "label": "aave_v3 BORROW",
        "tokens": [],
        "details": {
            "amount": units,
            "asset": "USDC",
            "asset_address": _LDO_USDC_ADDR,
            "borrow_balance": units,
            "collateral_enabled": False,
            "debt_value_usd": str(debt_usd),
            "health_factor": _LDO_HEALTH_FACTOR,
            "net_value_usd": str(-debt_usd),
            "stable_debt_balance": "0",
            "supply_apy_pct": "3.184757303562403849828165700",
            "supply_balance": "0",
            "supply_value_usd": "0.00",
            "valuation_source": "on_chain",
            "variable_debt_balance": units,
            "wallet_address": _WALLET,
        },
        "cost_basis_usd": str(debt_usd),
        "unrealized_pnl_usd": "0",
        "entry_timestamp": _ts(120),
        "last_update_timestamp": ts,
        "ledger_entry_id": ledger_entry_id,
    }


def _ldo_token_leg(symbol: str, value_usd: Decimal, price: Decimal, ts: str, ledger_entry_id: str) -> dict:
    """A plain ERC-20 holding.

    The capture keeps ERC-20 balances as TOKEN *positions* and reserves
    ``wallet_balances_json`` / ``available_cash_usd`` for the NATIVE gas token —
    so this fixture does the same rather than inventing a different convention.
    """
    return {
        "position_type": "TOKEN",
        "protocol": "wallet",
        "chain": _LDO_CHAIN,
        "value_usd": str(value_usd),
        "label": f"wallet {symbol}",
        "tokens": [],
        "details": {
            "amount": _ldo_units(value_usd, price),
            "asset": symbol,
            "asset_address": _LDO_WBNB_ADDR if symbol == "WBNB" else _LDO_USDC_ADDR,
            "cost_basis_source": "swap_inventory_lots",
            "price_usd": str(price),
            "spot_amount": _ldo_units(value_usd, price),
            "valuation_source": "on_chain",
            "wallet_address": _WALLET,
        },
        "cost_basis_usd": str(value_usd),
        "unrealized_pnl_usd": "0",
        "entry_timestamp": _ts(0),
        "last_update_timestamp": ts,
        "ledger_entry_id": ledger_entry_id,
    }


def _ldo_positions_json(legs: list[dict]) -> str:
    """Wrap legs in the VIB-3923 envelope the capture emits."""
    return json.dumps({"schema_version": 1, "positions": legs}, default=str)


def _ldo_wallet_balances_json(bnb_balance: Decimal) -> str:
    """``wallet_balances_json`` carrying ONLY the native gas token, as the
    capture does. G6's ambient revaluation term reads this; the price is flat
    across the run so the term is a measured exact zero."""
    return json.dumps(
        [
            {
                "symbol": "BNB",
                "balance": str(bnb_balance),
                "value_usd": str(bnb_balance * _LDO_PRICE_BNB),
                "address": "",
                "price_usd": str(_LDO_PRICE_BNB),
            }
        ]
    )


_LDO_TOKEN_PRICES_JSON = json.dumps(
    {
        f"bsc:{_LDO_USDC_ADDR}": {"decimals": 18, "price_usd": "1.00", "symbol": "USDC"},
        f"bsc:{_LDO_WBNB_ADDR}": {"decimals": 18, "price_usd": "584.67", "symbol": "WBNB"},
    }
)


def _ldo_lending_payload(event_type: str, position_key: str, amount_usd: Decimal, units: str) -> dict:
    """Lending payloads for the four lifecycle legs.

    Interest is a MEASURED ``"0"`` on both carry legs (Empty != Zero: this is
    ``Decimal("0")``, not ``None`` and not ``""``). G6 reads
    ``WITHDRAW.interest_accrued_usd`` and ``REPAY.interest_paid_usd``; a missing
    key would land in ``null_withdraw_interest`` / ``null_repay_interest`` and
    FAIL the cell on unmeasured inputs rather than on the debt term.
    """
    # KNOWN LIMITATION (VIB-6571): no ``amount_token``. ``FIFOBasisStore`` treats a
    # lending event without it as policy-v1 legacy, skips it, and reconstructs no
    # debt lots — so basis/restart-dependent cells scored against this fixture
    # exercise the legacy path while the row carries the CURRENT matching-policy
    # stamp. Deliberately matched to the rest of the corpus rather than fixed here:
    # the pre-existing ``looping`` fixture has the identical gap (0 of 6 lending
    # events carry it), and fixing one fixture alone would leave the corpus split
    # between two schemas. The fix regenerates fixture content and must re-measure
    # every affected cell and floor, which is its own change — see VIB-6571, and
    # coordinate with VIB-6563's regeneration decision.
    base = {
        "event_type": event_type,
        "protocol": _LDO_PROTOCOL,
        "asset": "WBNB" if event_type in ("SUPPLY", "WITHDRAW") else "USDC",
        "amount": units,
        "amount_usd": str(amount_usd),
        "confidence": "HIGH",
        "position_key": position_key,
    }
    if event_type == "SUPPLY":
        base["supply_apr_pct"] = "0.0108"
        base["health_factor_after"] = _LDO_HEALTH_FACTOR
        base["cost_basis_usd"] = str(amount_usd)
    elif event_type == "WITHDRAW":
        base["interest_accrued_usd"] = "0"
        base["interest_delta_usd"] = "0"
        base["realized_pnl_usd"] = "0"
        base["health_factor_after"] = _LDO_HEALTH_FACTOR
    elif event_type == "BORROW":
        base["borrowed_amount"] = units
        base["borrowed_amount_usd"] = str(amount_usd)
        base["borrow_apr_pct"] = "3.1848"
        base["health_factor_after"] = _LDO_HEALTH_FACTOR
        base.pop("amount", None)
        base.pop("amount_usd", None)
    elif event_type == "REPAY":
        base["principal_repaid"] = units
        base["principal_repaid_usd"] = str(amount_usd)
        base["interest_paid"] = "0"
        base["interest_paid_usd"] = "0"
        base["principal_delta_usd"] = str(amount_usd)
        base["interest_delta_usd"] = "0"
        base["health_factor_after"] = _LDO_HEALTH_FACTOR
    return base


def generate_looping_debt_open_fixture(db_path: str | Path) -> None:
    """Generate the debt-open-at-the-endpoint lending fixture (VIB-6560).

    4 ledger rows (SUPPLY → BORROW → REPAY → WITHDRAW, so the ``looping``
    profile's ``required_lifecycle`` guard is satisfied), 4 accounting events,
    4 position events, 7 portfolio snapshots, 1 portfolio_metrics row.

    The deleverage is PARTIAL: a collateral leg and a debt leg are both still
    live at snapshot 7. That is the whole point — a fixture that fully unwinds
    ends flat and is inert for exactly the same reason every completed run in
    the 193-DB corpus is.

    Scored under the EXISTING ``looping`` ScorecardProfile (registered in
    ``check_accounting_ratchet._FIXTURE_SCORING_PROFILE``). The lending cell pack
    and its epsilon are correct here; only the STIMULUS changes, so a new profile
    would fork scoring semantics for no reason.
    """
    db_path = Path(db_path)
    conn = _connect(db_path)
    try:
        cycle = "cycle-loop-debt-open-001"

        # ── Native-gas ladder ────────────────────────────────────────────
        bnb = [_LDO_BNB_START]
        for g in _LDO_GAS_BNB:
            bnb.append(bnb[-1] - g)
        gas_usd = [g * _LDO_PRICE_BNB for g in _LDO_GAS_BNB]

        # ── Four landed transactions ─────────────────────────────────────
        # (event_type, position_key, amount_usd, price, position_type, event)
        steps = (
            ("SUPPLY", _LDO_COLL_KEY, _LDO_SUPPLY_OPEN_USD, _LDO_PRICE_BNB, "LENDING_COLLATERAL", "OPEN"),
            ("BORROW", _LDO_DEBT_KEY, _LDO_DEBT_OPEN_USD, _LDO_PRICE_USDC, "LENDING_DEBT", "OPEN"),
            ("REPAY", _LDO_DEBT_KEY, _LDO_REPAY_PRINCIPAL_USD, _LDO_PRICE_USDC, "LENDING_DEBT", "DECREASE"),
            ("WITHDRAW", _LDO_COLL_KEY, _LDO_WITHDRAW_USD, _LDO_PRICE_BNB, "LENDING_COLLATERAL", "DECREASE"),
        )
        ledger_ids: list[str] = []
        for idx, (event_type, pos_key, amount_usd, price, pos_type, ev_type) in enumerate(steps, start=1):
            ts = _ts(60 * idx)
            ledger_id = _stable_id("tl-ldo", idx)
            ledger_ids.append(ledger_id)
            units = _ldo_units(amount_usd, price)
            asset = "WBNB" if event_type in ("SUPPLY", "WITHDRAW") else "USDC"
            _insert_ledger(
                conn,
                row_id=ledger_id,
                cycle_id=cycle,
                timestamp=ts,
                intent_type=event_type,
                token_in=asset,
                amount_in=units,
                token_out=asset,
                amount_out=units,
                chain=_LDO_CHAIN,
                protocol=_LDO_PROTOCOL,
                tx_hash=f"0xldo{idx}",
                gas_usd=str(gas_usd[idx - 1]),
                post_state={"health_factor": _LDO_HEALTH_FACTOR},
            )
            _insert_acct_event(
                conn,
                row_id=_stable_id("ae-ldo", idx),
                cycle_id=cycle,
                timestamp=ts,
                chain=_LDO_CHAIN,
                protocol=_LDO_PROTOCOL,
                event_type=event_type,
                position_key=pos_key,
                ledger_entry_id=ledger_id,
                tx_hash=f"0xldo{idx}",
                payload=_ldo_lending_payload(event_type, pos_key, amount_usd, units),
                primitive_name="lending",
            )
            _insert_position_event(
                conn,
                row_id=_stable_id("pe-ldo", idx),
                cycle_id=cycle,
                timestamp=ts,
                position_id=pos_key,
                position_type=pos_type,
                event_type=ev_type,
                chain=_LDO_CHAIN,
                protocol=_LDO_PROTOCOL,
                token0=asset,
                token1="",
                amount0=units,
                amount1="",
                value_usd=str(amount_usd),
                tx_hash=f"0xldo{idx}",
                ledger_entry_id=ledger_id,
                gas_usd=str(gas_usd[idx - 1]),
            )

        # ── Seven snapshots ──────────────────────────────────────────────
        # Snapshot 1 PRE-DATES every ledger row, so G6's VIB-5854 window
        # coverage reports covers=True and no gas hides outside the bracket
        # (``gas_usd_before_initial_endpoint`` == 0). Each later snapshot is
        # taken 10s after the transaction it reflects.
        tl_supply, tl_borrow, tl_repay, tl_withdraw = ledger_ids
        supply_open, supply_end = _LDO_SUPPLY_OPEN_USD, _LDO_SUPPLY_END_USD
        debt_open, debt_end = _LDO_DEBT_OPEN_USD, _LDO_DEBT_END_USD

        def _held_wbnb(v: Decimal, ts: str, src: str) -> dict:
            return _ldo_token_leg("WBNB", v, _LDO_PRICE_BNB, ts, src)

        def _held_usdc(v: Decimal, ts: str, src: str) -> dict:
            return _ldo_token_leg("USDC", v, _LDO_PRICE_USDC, ts, src)

        # (offset, bnb_index, legs)
        ladder: tuple[tuple[int, int, list[dict]], ...] = (
            # 1 — pre-trade: holds the WBNB it is about to supply.
            (0, 0, [_held_wbnb(supply_open, _ts(0), "")]),
            # 2 — after SUPPLY: the WBNB became collateral.
            (70, 1, [_ldo_supply_leg(supply_open, _ts(70), tl_supply)]),
            # 3 — after BORROW: debt leg live, borrowed USDC in hand.
            (
                130,
                2,
                [
                    _ldo_supply_leg(supply_open, _ts(130), tl_supply),
                    _ldo_borrow_leg(debt_open, _ts(130), tl_borrow),
                    _held_usdc(debt_open, _ts(130), tl_borrow),
                ],
            ),
            # 4 — idle hold, no transaction. Same legs, later marks.
            (
                160,
                2,
                [
                    _ldo_supply_leg(supply_open, _ts(160), tl_supply),
                    _ldo_borrow_leg(debt_open, _ts(160), tl_borrow),
                    _held_usdc(debt_open, _ts(160), tl_borrow),
                ],
            ),
            # 5 — after partial REPAY: debt shrinks, USDC leaves the wallet.
            (
                190,
                3,
                [
                    _ldo_supply_leg(supply_open, _ts(190), tl_supply),
                    _ldo_borrow_leg(debt_end, _ts(190), tl_repay),
                    _held_usdc(debt_end, _ts(190), tl_borrow),
                ],
            ),
            # 6 — after partial WITHDRAW: collateral shrinks, WBNB returns.
            (
                250,
                4,
                [
                    _ldo_supply_leg(supply_end, _ts(250), tl_withdraw),
                    _ldo_borrow_leg(debt_end, _ts(250), tl_repay),
                    _held_usdc(debt_end, _ts(250), tl_borrow),
                    _held_wbnb(_LDO_WITHDRAW_USD, _ts(250), tl_withdraw),
                ],
            ),
            # 7 — THE ENDPOINT. Still leveraged: a live collateral leg AND a
            #     live debt leg. This is the row the whole ticket is about.
            (
                310,
                4,
                [
                    _ldo_supply_leg(supply_end, _ts(310), tl_withdraw),
                    _ldo_borrow_leg(debt_end, _ts(310), tl_repay),
                    _held_usdc(debt_end, _ts(310), tl_borrow),
                    _held_wbnb(_LDO_WITHDRAW_USD, _ts(310), tl_withdraw),
                ],
            ),
        )

        first_equity: Decimal | None = None
        last_total: Decimal | None = None
        for i, (offset, bnb_idx, legs) in enumerate(ladder, start=1):
            values = [Decimal(leg["value_usd"]) for leg in legs]
            # VIB-3614: the deployed column is Σ POSITIVE value_usd — the debt
            # leg is DROPPED here and must be re-subtracted by any NAV consumer.
            total = sum((v for v in values if v > 0), Decimal("0"))
            # deployed_capital_usd is GROSS Σ|cost_basis| (blueprint 27 §7.11).
            deployed = sum((abs(Decimal(leg["cost_basis_usd"])) for leg in legs), Decimal("0"))
            cash = bnb[bnb_idx] * _LDO_PRICE_BNB
            if first_equity is None:
                first_equity = total + cash
            last_total = total
            _insert_portfolio_snapshot(
                conn,
                cycle_id=cycle,
                iteration_number=i,
                timestamp=_ts(offset),
                total_value_usd=str(total),
                available_cash_usd=str(cash),
                deployed_capital_usd=str(deployed),
                chain=_LDO_CHAIN,
                positions_json=_ldo_positions_json(legs),
                token_prices_json=_LDO_TOKEN_PRICES_JSON,
                wallet_balances_json=_ldo_wallet_balances_json(bnb[bnb_idx]),
            )

        assert first_equity is not None and last_total is not None
        _insert_portfolio_metrics(
            conn,
            # The strategy's opening EQUITY (deployed + cash), not the deployed
            # column alone. Writing the deployed column here is the VIB-6349
            # ``or``-drop defect, and G5 FAILs on exactly that signature; this
            # fixture isolates ONE defect (VIB-5857), so it does not encode a
            # second one.
            initial_value_usd=str(first_equity),
            gas_spent_usd=str(sum(gas_usd, Decimal("0"))),
            total_value_usd=str(last_total),
        )
        conn.commit()
    finally:
        conn.close()


# ─── Perp fixture (GMX V2 lifecycle) ──────────────────────────────────────
def generate_perp_fixture(db_path: str | Path) -> None:
    """Generate the canonical Perp fixture: 4 ledger rows, 2 position events, 2 acct events."""
    db_path = Path(db_path)
    conn = _connect(db_path)
    try:
        cycle = "cycle-perp-001"
        chain = "arbitrum"
        protocol = "gmx_v2"
        position_key = "perp:arbitrum:gmx_v2:wallet:ETH-USDC"

        # T0: SWAP USDC → collateral (entry)
        ledger_swap_in = _stable_id("tl-perp", 1)
        _insert_ledger(
            conn,
            row_id=ledger_swap_in,
            cycle_id=cycle,
            timestamp=_ts(0),
            intent_type="SWAP",
            token_in="USDC",
            amount_in="100.0",
            token_out="USDC",
            amount_out="100.0",
            chain=chain,
            protocol=protocol,
            tx_hash="0xp1",
        )
        _insert_acct_event(
            conn,
            row_id=_stable_id("ae-perp", 0),
            cycle_id=cycle,
            timestamp=_ts(0),
            chain=chain,
            protocol=protocol,
            event_type="SWAP",
            position_key="swap:USDC-USDC",
            ledger_entry_id=ledger_swap_in,
            tx_hash="0xp1",
            payload={
                "event_type": "SWAP",
                "protocol": protocol,
                "token_in": "USDC",
                "token_out": "USDC",
                "amount_in": "100.0",
                "amount_out": "100.0",
                "amount_in_usd": "100.0",
                "amount_out_usd": "100.0",
                "effective_price": "1.0",
                "slippage_bps": "0",
                "realized_pnl_usd": None,
                "cost_basis_recorded": True,
                "gas_usd": "0.5",
                "confidence": "HIGH",
            },
            primitive_name="swap",
        )

        # T1: PERP_OPEN
        ledger_open = _stable_id("tl-perp", 2)
        _insert_ledger(
            conn,
            row_id=ledger_open,
            cycle_id=cycle,
            timestamp=_ts(60),
            intent_type="PERP_OPEN",
            token_in="USDC",
            amount_in="100.0",
            token_out="ETH-PERP",
            amount_out="0.04",
            chain=chain,
            protocol=protocol,
            tx_hash="0xp2",
        )
        _insert_acct_event(
            conn,
            row_id=_stable_id("ae-perp", 1),
            cycle_id=cycle,
            timestamp=_ts(60),
            chain=chain,
            protocol=protocol,
            event_type="PERP_OPEN",
            position_key=position_key,
            ledger_entry_id=ledger_open,
            tx_hash="0xp2",
            payload={
                "event_type": "PERP_OPEN",
                "protocol": protocol,
                "position_key": position_key,
                "market": "ETH-USDC",
                "is_long": True,
                "size": "0.04",
                "leverage": "2.0",
                "entry_price": "2500.0",
                "open_fee_usd": "0.5",
                "price_impact_usd": "0.1",
                "cost_basis_usd": "100.0",
                "confidence": "HIGH",
            },
            primitive_name="perp",
        )
        _insert_position_event(
            conn,
            row_id=_stable_id("pe-perp", 1),
            cycle_id=cycle,
            timestamp=_ts(60),
            position_id=position_key,
            position_type="PERP",
            event_type="OPEN",
            chain=chain,
            protocol=protocol,
            token0="ETH",
            token1="USDC",
            amount0="0.04",
            amount1="100.0",
            value_usd="100.0",
            tx_hash="0xp2",
            ledger_entry_id=ledger_open,
        )

        # T2: PERP_CLOSE
        ledger_close = _stable_id("tl-perp", 3)
        _insert_ledger(
            conn,
            row_id=ledger_close,
            cycle_id=cycle,
            timestamp=_ts(120),
            intent_type="PERP_CLOSE",
            token_in="ETH-PERP",
            amount_in="0.04",
            token_out="USDC",
            amount_out="105.0",
            chain=chain,
            protocol=protocol,
            tx_hash="0xp3",
        )
        _insert_acct_event(
            conn,
            row_id=_stable_id("ae-perp", 2),
            cycle_id=cycle,
            timestamp=_ts(120),
            chain=chain,
            protocol=protocol,
            event_type="PERP_CLOSE",
            position_key=position_key,
            ledger_entry_id=ledger_close,
            tx_hash="0xp3",
            payload={
                "event_type": "PERP_CLOSE",
                "protocol": protocol,
                "position_key": position_key,
                "market": "ETH-USDC",
                "is_long": True,
                "size": "0.04",
                "exit_price": "2625.0",
                "close_fee_usd": "0.5",
                "price_impact_usd": "0.1",
                "funding_paid_usd": "0.05",
                "funding_received_usd": "0",
                "realized_pnl_usd": "5.0",
                "confidence": "HIGH",
            },
            primitive_name="perp",
        )
        _insert_position_event(
            conn,
            row_id=_stable_id("pe-perp", 2),
            cycle_id=cycle,
            timestamp=_ts(120),
            position_id=position_key,
            position_type="PERP",
            event_type="CLOSE",
            chain=chain,
            protocol=protocol,
            token0="ETH",
            token1="USDC",
            amount0="0.04",
            amount1="105.0",
            value_usd="105.0",
            tx_hash="0xp3",
            ledger_entry_id=ledger_close,
        )

        # T3: SWAP collateral out
        ledger_swap_out = _stable_id("tl-perp", 4)
        _insert_ledger(
            conn,
            row_id=ledger_swap_out,
            cycle_id=cycle,
            timestamp=_ts(180),
            intent_type="SWAP",
            token_in="USDC",
            amount_in="105.0",
            token_out="USDC",
            amount_out="105.0",
            chain=chain,
            protocol=protocol,
            tx_hash="0xp4",
        )
        _insert_acct_event(
            conn,
            row_id=_stable_id("ae-perp", 3),
            cycle_id=cycle,
            timestamp=_ts(180),
            chain=chain,
            protocol=protocol,
            event_type="SWAP",
            position_key="swap:USDC-USDC",
            ledger_entry_id=ledger_swap_out,
            tx_hash="0xp4",
            payload={
                "event_type": "SWAP",
                "protocol": protocol,
                "token_in": "USDC",
                "token_out": "USDC",
                "amount_in": "105.0",
                "amount_out": "105.0",
                "amount_in_usd": "105.0",
                "amount_out_usd": "105.0",
                "effective_price": "1.0",
                "slippage_bps": "0",
                "realized_pnl_usd": None,
                "cost_basis_recorded": True,
                "gas_usd": "0.5",
                "confidence": "HIGH",
            },
            primitive_name="swap",
        )

        for i, offset in enumerate((0, 60, 120, 180), start=1):
            _insert_portfolio_snapshot(
                conn,
                cycle_id=cycle,
                iteration_number=i,
                timestamp=_ts(offset),
                total_value_usd=str(Decimal("100") + Decimal(i)),
                available_cash_usd="100.0",
                deployed_capital_usd="100.0",
                chain=chain,
            )

        _insert_portfolio_metrics(conn)
        conn.commit()
    finally:
        conn.close()


# ─── Settlement fixture (Lagoon ERC-7540 operator side) — VIB-5682 ────────
def generate_settlement_fixture(db_path: str | Path) -> None:
    """Generate the canonical vault-SETTLEMENT fixture.

    Shape-faithful to the VIB-5666 real-fork settlement proof
    (``tests/reports/vib-5666-realfork-settlement-proof.md``) and to what
    ``runner/settlement_commit.py`` + ``category_handlers/settlement_handler.py``
    actually produce:

    * 4 ``transaction_ledger`` rows — a ``SETTLE_PROPOSE`` (NO_ACCOUNTING gas/tx
      row, no typed event), a ``SETTLE_DEPOSIT`` (ledger + typed event), a second
      ``SETTLE_PROPOSE`` (redeem valuation, NO_ACCOUNTING), and a ``SETTLE_REDEEM``
      (ledger + typed event). The propose legs carry the settlement outputs on
      NEITHER leg — they move no capital.
    * 2 ``accounting_events`` — the ``SETTLE_DEPOSIT`` / ``SETTLE_REDEEM``
      ``SettlementAccountingEvent`` rows: receipt-measured ``assets_delta`` /
      ``shares_delta`` (positive magnitudes), post-settle ``new_total_assets``,
      ``assets_usd``, version stamps (``schema_version`` + ``primitive_version``,
      NO ``formula_version`` / ``matching_policy_version`` — settlement does no lot
      matching), and NO PnL keys (capital-event discipline).

    The redeem leg is constructed synthetically (the share-backed guard blocks the
    epoch-2 redeem on the live demo today), but its row shape is derived from the
    commit-pipeline code, not imagination.
    """
    db_path = Path(db_path)
    conn = _connect(db_path)
    try:
        cycle_dep = "settlement-1"
        cycle_red = "settlement-2"
        chain = "base"
        protocol = "lagoon"
        vault = "0x6c347d32ef555f034ff22ce7a84fc8019dcbfb67"
        position_key = f"settlement:{chain}:{protocol}:{_WALLET}:{vault}"

        def _settlement_payload(event_type: str, assets: str, shares: str, nta: str, usd: str, epoch: int) -> dict:
            # Verbatim SettlementAccountingEvent.to_payload_json shape (capital
            # event: schema_version + primitive_version, no lot-matching stamps,
            # no PnL keys).
            return {
                "event_type": event_type,
                "position_key": position_key,
                "vault_address": vault,
                "asset_token": "USDC",
                "assets_delta": assets,
                "shares_delta": shares,
                "new_total_assets": nta,
                "fee_shares": None,
                "assets_usd": usd,
                "epoch_id": epoch,
                "confidence": "HIGH",
                "unavailable_reason": "",
                "schema_version": SCHEMA_VERSION,
                "primitive_version": PRIMITIVE_VERSIONS[Primitive.SETTLEMENT],
            }

        # T0: SETTLE_PROPOSE (updateNewTotalAssets, deposit valuation) — no capital,
        # NO_ACCOUNTING ledger-only row (gas/tx visibility so the books tie).
        _insert_ledger(
            conn,
            row_id=_stable_id("tl-settle", 1),
            cycle_id=cycle_dep,
            timestamp=_ts(0),
            intent_type="SETTLE_PROPOSE",
            token_in="",
            amount_in="",
            token_out="",
            amount_out="",
            chain=chain,
            protocol=protocol,
            tx_hash="0xs1",
        )

        # T1: SETTLE_DEPOSIT — assets flowed IN (1000 USDC), shares minted (1000).
        ledger_deposit = _stable_id("tl-settle", 2)
        _insert_ledger(
            conn,
            row_id=ledger_deposit,
            cycle_id=cycle_dep,
            timestamp=_ts(30),
            intent_type="SETTLE_DEPOSIT",
            token_in="USDC",
            amount_in="1000",
            token_out="",
            amount_out="",
            chain=chain,
            protocol=protocol,
            tx_hash="0xs2",
            extracted={
                "settlement": {
                    "leg": "deposit",
                    "assets": "1000",
                    "shares": "1000",
                    "new_total_assets": "0",
                    "fee_shares": None,
                    "assets_usd": "1000.00",
                    "epoch_id": 1,
                }
            },
        )
        _insert_acct_event(
            conn,
            row_id=_stable_id("ae-settle", 1),
            cycle_id=cycle_dep,
            timestamp=_ts(30),
            chain=chain,
            protocol=protocol,
            event_type="SETTLE_DEPOSIT",
            position_key=position_key,
            ledger_entry_id=ledger_deposit,
            tx_hash="0xs2",
            payload=_settlement_payload("SETTLE_DEPOSIT", "1000", "1000", "0", "1000.00", 1),
            primitive_name="settlement",
            stamp=False,
        )

        # T2: SETTLE_PROPOSE #2 (redeem valuation) — Lagoon v0.5.0 needs a fresh
        # proposal per settleRedeem. NO_ACCOUNTING ledger-only row.
        _insert_ledger(
            conn,
            row_id=_stable_id("tl-settle", 3),
            cycle_id=cycle_red,
            timestamp=_ts(60),
            intent_type="SETTLE_PROPOSE",
            token_in="",
            amount_in="",
            token_out="",
            amount_out="",
            chain=chain,
            protocol=protocol,
            tx_hash="0xs3",
        )

        # T3: SETTLE_REDEEM — assets flowed OUT (500 USDC), shares burned (500).
        ledger_redeem = _stable_id("tl-settle", 4)
        _insert_ledger(
            conn,
            row_id=ledger_redeem,
            cycle_id=cycle_red,
            timestamp=_ts(90),
            intent_type="SETTLE_REDEEM",
            token_in="USDC",
            amount_in="500",
            token_out="",
            amount_out="",
            chain=chain,
            protocol=protocol,
            tx_hash="0xs4",
            extracted={
                "settlement": {
                    "leg": "redeem",
                    "assets": "500",
                    "shares": "500",
                    "new_total_assets": "500",
                    "fee_shares": None,
                    "assets_usd": "500.00",
                    "epoch_id": 2,
                }
            },
        )
        _insert_acct_event(
            conn,
            row_id=_stable_id("ae-settle", 2),
            cycle_id=cycle_red,
            timestamp=_ts(90),
            chain=chain,
            protocol=protocol,
            event_type="SETTLE_REDEEM",
            position_key=position_key,
            ledger_entry_id=ledger_redeem,
            tx_hash="0xs4",
            payload=_settlement_payload("SETTLE_REDEEM", "500", "500", "500", "500.00", 2),
            primitive_name="settlement",
            stamp=False,
        )

        # Snapshots + metrics so the generic G4/G5/G8 cells have >0 rows.
        for i, (cyc, offset) in enumerate(((cycle_dep, 0), (cycle_dep, 30), (cycle_red, 60), (cycle_red, 90)), start=1):
            _insert_portfolio_snapshot(
                conn,
                cycle_id=cyc,
                iteration_number=i,
                timestamp=_ts(offset),
                total_value_usd=str(Decimal("1000") + Decimal(i)),
                available_cash_usd="1000.0",
                deployed_capital_usd="0",
                chain=chain,
            )

        _insert_portfolio_metrics(conn)
        conn.commit()
    finally:
        conn.close()


def main() -> None:
    """Generate all fixtures + their expected_cells.json into the
    canonical directory layout. Used both by ``_freeze_pre_t2_baseline.py``
    (precursor) and by the T2 commit author when re-baselining.
    """
    base = Path(__file__).parent
    generate_lp_fixture(base / "lp" / "expected_baseline.sqlite")
    generate_looping_fixture(base / "looping" / "expected_baseline.sqlite")
    generate_looping_debt_open_fixture(base / "looping_debt_open" / "expected_baseline.sqlite")
    generate_perp_fixture(base / "perp" / "expected_baseline.sqlite")
    generate_settlement_fixture(base / "settlement" / "expected_baseline.sqlite")


if __name__ == "__main__":
    main()
