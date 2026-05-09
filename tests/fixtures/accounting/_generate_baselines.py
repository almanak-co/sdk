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
    SCHEMA_VERSION,
)

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
_STRATEGY_ID = "AccountantBaseline:fixture"
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
        strategy_id TEXT NOT NULL,
        deployment_id TEXT DEFAULT '',
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
        schema_version INTEGER NOT NULL DEFAULT 1
    )
    """,
    """
    CREATE TABLE portfolio_snapshots (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        strategy_id TEXT NOT NULL,
        deployment_id TEXT DEFAULT '',
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
        strategy_id TEXT PRIMARY KEY,
        initial_value_usd TEXT NOT NULL,
        initial_timestamp TEXT NOT NULL,
        deposits_usd TEXT DEFAULT '0',
        withdrawals_usd TEXT DEFAULT '0',
        gas_spent_usd TEXT DEFAULT '0',
        total_value_usd TEXT DEFAULT '0',
        positions_json TEXT DEFAULT '[]',
        cycle_id TEXT,
        deployment_id TEXT DEFAULT '',
        execution_mode TEXT DEFAULT '',
        is_complete BOOLEAN DEFAULT 1,
        updated_at TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE position_state_snapshots (
        snapshot_id INTEGER,
        strategy_id TEXT,
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
) -> None:
    conn.execute(
        """
        INSERT INTO transaction_ledger
        (id, cycle_id, strategy_id, deployment_id, execution_mode, timestamp,
         intent_type, token_in, amount_in, token_out, amount_out,
         effective_price, slippage_bps, gas_used, gas_usd, tx_hash, chain, protocol,
         success, error, extracted_data_json, price_inputs_json,
         pre_state_json, post_state_json)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, '', ?, ?, ?, ?)
        """,
        (
            row_id,
            cycle_id,
            _STRATEGY_ID,
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
            "0.5",
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
) -> None:
    stamped = _stamp_payload(payload, primitive_name)
    conn.execute(
        """
        INSERT INTO accounting_events
        (id, deployment_id, strategy_id, cycle_id, execution_mode, timestamp,
         chain, protocol, wallet_address, event_type, position_key,
         ledger_entry_id, tx_hash, confidence, payload_json, schema_version)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            row_id,
            _DEPLOYMENT_ID,
            _STRATEGY_ID,
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
            "0.5",
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
) -> None:
    conn.execute(
        """
        INSERT INTO portfolio_snapshots
        (strategy_id, deployment_id, cycle_id, execution_mode, timestamp,
         iteration_number, total_value_usd, available_cash_usd, deployed_capital_usd,
         wallet_total_value_usd, value_confidence, positions_json, token_prices_json,
         wallet_balances_json, chain, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            _STRATEGY_ID,
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
            "[]",
            "{}",
            "[]",
            chain,
            timestamp,
        ),
    )


def _insert_portfolio_metrics(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        INSERT INTO portfolio_metrics
        (strategy_id, initial_value_usd, initial_timestamp, deposits_usd,
         withdrawals_usd, gas_spent_usd, total_value_usd, positions_json,
         cycle_id, deployment_id, execution_mode, is_complete, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            _STRATEGY_ID,
            "1000.00",
            _ts(0),
            "0",
            "0",
            "2.0",
            "1010.00",
            "[]",
            "cycle-001",
            _DEPLOYMENT_ID,
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


def main() -> None:
    """Generate all three fixtures + their expected_cells.json into the
    canonical directory layout. Used both by ``_freeze_pre_t2_baseline.py``
    (precursor) and by the T2 commit author when re-baselining.
    """
    base = Path(__file__).parent
    generate_lp_fixture(base / "lp" / "expected_baseline.sqlite")
    generate_looping_fixture(base / "looping" / "expected_baseline.sqlite")
    generate_perp_fixture(base / "perp" / "expected_baseline.sqlite")


if __name__ == "__main__":
    main()
