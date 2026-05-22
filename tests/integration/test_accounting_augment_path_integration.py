"""Integration test: simulate the gateway-routed accounting save path
end-to-end and prove the augmentation fires.

Mainnet probe (2026-05-01) couldn't get the augmentation to fire on actual
runs. Root cause: pip-editable install pointed `almanak` at the main repo
not the worktree, so the CLI loaded pre-fix code. This test runs the same
augmentation chain through pytest (which always loads the worktree) and
shows every layer of the chain (writer → SQLite save → augmentation)
correctly stamps versions and projects lending aliases on every event
type.

Scope: demonstrates G13 + L1/L4 augmentation work end-to-end. Does NOT
require any real network, RPC, or wallet — purely in-memory.
"""

from __future__ import annotations

import json
import sqlite3
import tempfile
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest

from almanak.framework.accounting.models import (
    AccountingConfidence,
    AccountingIdentity,
    LendingAccountingEvent,
    LendingEventType,
    SwapAccountingEvent,
    SwapEventType,
)
from almanak.framework.accounting.payload_schemas import (
    FORMULA_VERSION,
    MATCHING_POLICY_VERSION,
    SCHEMA_VERSION,
)
from almanak.framework.accounting.writer import (
    AccountingWriter,
    augment_accounting_payload,
)
from almanak.framework.state.backends.sqlite import SQLiteConfig, SQLiteStore


def _identity(execution_mode: str = "live") -> AccountingIdentity:
    return AccountingIdentity(
        id="test-augment-path-1",
        deployment_id="strat-1",
        cycle_id="cyc-1",
        execution_mode=execution_mode,
        timestamp=datetime.now(UTC),
        chain="arbitrum",
        protocol="uniswap_v3",
        wallet_address="0xWALLET",
        tx_hash="0xtest",
        ledger_entry_id="le-1",
    )


def _swap_event() -> SwapAccountingEvent:
    return SwapAccountingEvent(
        identity=_identity(),
        event_type=SwapEventType.SWAP,
        protocol="uniswap_v3",
        token_in="USDC",
        token_out="WETH",
        amount_in="2.0",
        amount_out="0.000866",
        amount_in_usd=None,
        amount_out_usd=None,
        effective_price="0.000433",
        slippage_bps=3,
        realized_pnl_usd=None,
        gas_usd=None,
        confidence=AccountingConfidence.ESTIMATED,
        unavailable_reason="missing prices",
        cost_basis_recorded=True,
        swap_position_key="swap:arbitrum:0xWALLET",
    )


def _lending_event(event_type: LendingEventType) -> LendingAccountingEvent:
    return LendingAccountingEvent(
        identity=_identity(),
        event_type=event_type,
        position_key="lending:arbitrum:0xWALLET",
        market_id="aave_v3",
        asset="USDC",
        collateral_value_before_usd=None,
        collateral_value_after_usd=None,
        debt_value_before_usd=None,
        debt_value_after_usd=None,
        net_equity_before_usd=None,
        net_equity_after_usd=None,
        health_factor_before=None,
        health_factor_after=None,
        liquidation_threshold=None,
        lltv=None,
        supply_apr_bps=None,
        borrow_apr_bps=None,
        principal_delta_usd="100.0",
        interest_delta_usd="1.5",
        gas_usd=None,
        confidence=AccountingConfidence.HIGH,
    )


@pytest.mark.asyncio
async def test_full_chain_swap_event_lands_versioned():
    """Simulate the gateway-routed save chain for a SWAP event.

    AccountingWriter.write → SQLiteStore.save_accounting_event →
    augment_accounting_payload. The saved row's payload_json must carry
    schema_version, formula_version, matching_policy_version, AND keep the
    original SwapAccountingEvent payload fields (no data loss).
    """
    db_path = Path(tempfile.mktemp(suffix="-augment-path.db"))
    store = SQLiteStore(SQLiteConfig(db_path=str(db_path)))
    await store.initialize()

    writer = AccountingWriter(store)
    event = _swap_event()
    ok = await writer.write(event)
    assert ok is True

    conn = sqlite3.connect(db_path)
    rows = list(conn.execute("SELECT payload_json FROM accounting_events"))
    assert len(rows) == 1
    payload = json.loads(rows[0][0])

    # G13: every version stamped.
    assert payload["schema_version"] == SCHEMA_VERSION
    assert payload["formula_version"] == FORMULA_VERSION
    assert payload["matching_policy_version"] == MATCHING_POLICY_VERSION

    # Original fields preserved.
    assert payload["event_type"] == "SWAP"
    assert payload["token_in"] == "USDC"
    assert payload["token_out"] == "WETH"


@pytest.mark.asyncio
async def test_full_chain_repay_event_aliases_projected():
    """L4 spec contract: REPAY rows must carry both ``principal_delta_usd``
    (legacy) AND ``principal_repaid_usd`` (spec name).
    """
    db_path = Path(tempfile.mktemp(suffix="-augment-path-repay.db"))
    store = SQLiteStore(SQLiteConfig(db_path=str(db_path)))
    await store.initialize()

    writer = AccountingWriter(store)
    event = _lending_event(LendingEventType.REPAY)
    ok = await writer.write(event)
    assert ok is True

    conn = sqlite3.connect(db_path)
    rows = list(conn.execute("SELECT payload_json FROM accounting_events"))
    payload = json.loads(rows[0][0])

    # G13.
    assert payload["matching_policy_version"] == MATCHING_POLICY_VERSION

    # L4 alias projection.
    assert payload["principal_repaid_usd"] == "100.0"
    assert payload["interest_paid_usd"] == "1.5"

    # Legacy fields still present for backward compatibility.
    assert payload["principal_delta_usd"] == "100.0"
    assert payload["interest_delta_usd"] == "1.5"


@pytest.mark.asyncio
async def test_full_chain_withdraw_event_emits_accrued():
    """L1 spec contract: WITHDRAW rows must carry ``interest_accrued_usd``."""
    db_path = Path(tempfile.mktemp(suffix="-augment-path-withdraw.db"))
    store = SQLiteStore(SQLiteConfig(db_path=str(db_path)))
    await store.initialize()

    writer = AccountingWriter(store)
    event = _lending_event(LendingEventType.WITHDRAW)
    ok = await writer.write(event)
    assert ok is True

    conn = sqlite3.connect(db_path)
    rows = list(conn.execute("SELECT payload_json FROM accounting_events"))
    payload = json.loads(rows[0][0])

    # L1 alias projection (supply-side interest).
    assert payload["interest_accrued_usd"] == "1.5"

    # WITHDRAW has no principal_repaid_usd concept (that's a borrow-side term).
    assert "principal_repaid_usd" not in payload


def test_augment_idempotent_round_trip():
    """Running augmentation twice on the same payload yields the same result —
    the second invocation is a no-op. This is the property that lets us stamp
    at multiple layers of the save chain without inconsistency.
    """
    payload = json.dumps({"event_type": "SWAP", "x": 1})
    once = augment_accounting_payload(payload, is_live=True)
    twice = augment_accounting_payload(once, is_live=True)
    once_d = json.loads(once)
    twice_d = json.loads(twice)
    assert once_d == twice_d
