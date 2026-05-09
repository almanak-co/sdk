"""Unit tests for AccountingProcessor (VIB-3467).

Tests drain_one, drain_pending, idempotency, and FIFO lot management.
Uses in-process mocks — no SQLite, no gateway, no network.
"""

from __future__ import annotations

import asyncio
import json
import uuid
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from almanak.framework.accounting.basis import FIFOBasisStore
from almanak.framework.accounting.processor import AccountingProcessor, write_outbox_entry


# ──────────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────────


def _make_outbox_row(
    ledger_entry_id: str,
    intent_type: str = "SUPPLY",
    status: str = "pending",
    attempts: int = 0,
    wallet_address: str = "0xabc",
    position_key: str = "lending:arbitrum:aave_v3:0xabc:usdc",
    market_id: str = "",
) -> dict[str, Any]:
    return {
        "id": str(uuid.uuid4()),
        "ledger_entry_id": ledger_entry_id,
        "deployment_id": "dep-1",
        "strategy_id": "strat-1",
        "cycle_id": "cycle-1",
        "intent_type": intent_type,
        "wallet_address": wallet_address,
        "position_key": position_key,
        "market_id": market_id,
        "status": status,
        "attempts": attempts,
        "error": "",
        "created_at": datetime.now(UTC).isoformat(),
        "updated_at": datetime.now(UTC).isoformat(),
    }


def _make_ledger_row(
    ledger_entry_id: str,
    intent_type: str = "SUPPLY",
    protocol: str = "aave_v3",
    chain: str = "arbitrum",
    extracted_data_json: str = "",
    price_inputs_json: str = "",
    post_state_json: str = "",
    tx_hash: str = "0xdeadbeef",
) -> dict[str, Any]:
    return {
        "id": ledger_entry_id,
        "strategy_id": "strat-1",
        "deployment_id": "dep-1",
        "cycle_id": "cycle-1",
        "execution_mode": "live",
        "timestamp": datetime.now(UTC).isoformat(),
        "intent_type": intent_type,
        "token_in": "USDC",
        "amount_in": "100",
        "token_out": "",
        "amount_out": "",
        "effective_price": "",
        "slippage_bps": None,
        "gas_used": 0,
        "gas_usd": "0.01",
        "tx_hash": tx_hash,
        "chain": chain,
        "protocol": protocol,
        "success": True,
        "error": "",
        "extracted_data_json": extracted_data_json,
        "price_inputs_json": price_inputs_json,
        "pre_state_json": "",
        "post_state_json": post_state_json,
    }


def _make_mock_store(
    outbox_row: dict | None = None,
    ledger_row: dict | None = None,
    already_written: bool = False,
) -> MagicMock:
    """Build a mock state_manager with controllable outbox/ledger responses."""
    store = MagicMock()
    store.get_outbox_by_ledger_id = MagicMock(return_value=outbox_row)
    store.get_outbox_pending = MagicMock(return_value=[outbox_row] if outbox_row else [])
    store.update_outbox_entry = MagicMock()
    store.has_accounting_events_for_ledger = MagicMock(return_value=already_written)
    store.get_ledger_entry_by_id = MagicMock(return_value=ledger_row)
    store.save_accounting_event = AsyncMock(return_value=True)
    return store


# ──────────────────────────────────────────────────────────────────────────────
# drain_one
# ──────────────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_drain_one_no_outbox_row() -> None:
    store = _make_mock_store(outbox_row=None)
    proc = AccountingProcessor(state_manager=store, basis_store=FIFOBasisStore(), deployment_id="dep-1")

    result = await proc.drain_one("nonexistent-id")

    assert result is False


@pytest.mark.asyncio
async def test_drain_one_already_processed() -> None:
    led_id = str(uuid.uuid4())
    outbox_row = _make_outbox_row(led_id, status="processed")
    store = _make_mock_store(outbox_row=outbox_row)
    proc = AccountingProcessor(state_manager=store, basis_store=FIFOBasisStore(), deployment_id="dep-1")

    result = await proc.drain_one(led_id)

    assert result is True
    store.update_outbox_entry.assert_not_called()


@pytest.mark.asyncio
async def test_drain_one_idempotent_when_event_already_written() -> None:
    """If accounting_events already has a row for this ledger_entry_id, mark processed and skip."""
    led_id = str(uuid.uuid4())
    outbox_row = _make_outbox_row(led_id, status="pending")
    ledger_row = _make_ledger_row(led_id, intent_type="SUPPLY")
    store = _make_mock_store(outbox_row=outbox_row, ledger_row=ledger_row, already_written=True)
    proc = AccountingProcessor(state_manager=store, basis_store=FIFOBasisStore(), deployment_id="dep-1")

    result = await proc.drain_one(led_id)

    assert result is True
    store.save_accounting_event.assert_not_called()
    # Must have been marked processed
    calls = [c.args for c in store.update_outbox_entry.call_args_list]
    assert any(c[1] == "processed" for c in calls)


@pytest.mark.asyncio
async def test_drain_one_no_accounting_intent() -> None:
    """HOLD intent → no_accounting → no accounting_events row written, but outbox marked processed.

    Uses HOLD as the canonical NO_ACCOUNTING intent. VIB-4164 (T4) reclassified
    BRIDGE from NO_ACCOUNTING to TRANSFER, so BRIDGE no longer satisfies this
    fixture's "no accounting event written" precondition. HOLD remains
    NO_ACCOUNTING (utility intent with no financial event to record).
    """
    led_id = str(uuid.uuid4())
    outbox_row = _make_outbox_row(led_id, intent_type="HOLD")
    ledger_row = _make_ledger_row(led_id, intent_type="HOLD")
    store = _make_mock_store(outbox_row=outbox_row, ledger_row=ledger_row, already_written=False)
    proc = AccountingProcessor(state_manager=store, basis_store=FIFOBasisStore(), deployment_id="dep-1")

    result = await proc.drain_one(led_id)

    assert result is True
    store.save_accounting_event.assert_not_called()
    calls = [c.args for c in store.update_outbox_entry.call_args_list]
    assert any(c[1] == "processed" for c in calls)


@pytest.mark.asyncio
async def test_drain_one_failed_row_too_many_retries() -> None:
    led_id = str(uuid.uuid4())
    outbox_row = _make_outbox_row(led_id, status="failed", attempts=3)
    store = _make_mock_store(outbox_row=outbox_row)
    proc = AccountingProcessor(state_manager=store, basis_store=FIFOBasisStore(), deployment_id="dep-1")

    result = await proc.drain_one(led_id)

    assert result is False
    store.update_outbox_entry.assert_not_called()


@pytest.mark.asyncio
async def test_drain_one_handler_exception_marks_failed() -> None:
    led_id = str(uuid.uuid4())
    outbox_row = _make_outbox_row(led_id, intent_type="SUPPLY")
    ledger_row = _make_ledger_row(led_id, intent_type="SUPPLY")
    store = _make_mock_store(outbox_row=outbox_row, ledger_row=ledger_row, already_written=False)
    proc = AccountingProcessor(state_manager=store, basis_store=FIFOBasisStore(), deployment_id="dep-1")

    # Make the writer raise to simulate a failure
    store.save_accounting_event = AsyncMock(side_effect=RuntimeError("db down"))

    with patch(
        "almanak.framework.accounting.category_handlers.lending_handler.handle_lending",
        return_value=MagicMock(
            identity=MagicMock(execution_mode="live"),
            event_type="SUPPLY",
        ),
    ):
        result = await proc.drain_one(led_id)

    assert result is False
    calls = [c.args for c in store.update_outbox_entry.call_args_list]
    assert any(c[1] == "failed" for c in calls)


# ──────────────────────────────────────────────────────────────────────────────
# drain_pending
# ──────────────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_drain_pending_empty() -> None:
    store = _make_mock_store(outbox_row=None)
    proc = AccountingProcessor(state_manager=store, basis_store=FIFOBasisStore(), deployment_id="dep-1")

    count = await proc.drain_pending()

    assert count == 0


@pytest.mark.asyncio
async def test_drain_pending_processes_pending_rows() -> None:
    led_id = str(uuid.uuid4())
    outbox_row = _make_outbox_row(led_id, intent_type="BRIDGE")
    ledger_row = _make_ledger_row(led_id, intent_type="BRIDGE")
    store = _make_mock_store(outbox_row=outbox_row, ledger_row=ledger_row, already_written=False)
    store.get_outbox_pending = MagicMock(return_value=[outbox_row])
    proc = AccountingProcessor(state_manager=store, basis_store=FIFOBasisStore(), deployment_id="dep-1")

    count = await proc.drain_pending()

    assert count == 1


@pytest.mark.asyncio
async def test_drain_pending_skips_rows_without_ledger_entry_id() -> None:
    bad_row = {"id": "x", "ledger_entry_id": "", "status": "pending", "attempts": 0}
    store = MagicMock()
    store.get_outbox_pending = MagicMock(return_value=[bad_row])
    proc = AccountingProcessor(state_manager=store, basis_store=FIFOBasisStore(), deployment_id="dep-1")

    count = await proc.drain_pending()

    assert count == 0


# ──────────────────────────────────────────────────────────────────────────────
# write_outbox_entry helper
# ──────────────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_write_outbox_entry_returns_id() -> None:
    store = MagicMock()
    store.save_outbox_entry = MagicMock(return_value=None)

    outbox_id = await write_outbox_entry(
        store,
        deployment_id="dep-1",
        strategy_id="strat-1",
        cycle_id="cycle-1",
        ledger_entry_id="ledger-1",
        intent_type="SUPPLY",
        wallet_address="0xabc",
        position_key="lending:arbitrum:aave_v3:0xabc:usdc",
    )

    assert outbox_id is not None
    store.save_outbox_entry.assert_called_once()


@pytest.mark.asyncio
async def test_write_outbox_entry_no_state_manager() -> None:
    outbox_id = await write_outbox_entry(
        None,
        deployment_id="dep-1",
        strategy_id="strat-1",
        cycle_id="cycle-1",
        ledger_entry_id="ledger-1",
        intent_type="SUPPLY",
        wallet_address="0xabc",
    )

    assert outbox_id is None


@pytest.mark.asyncio
async def test_write_outbox_entry_store_exception_returns_none() -> None:
    store = MagicMock()
    store.save_outbox_entry = MagicMock(side_effect=RuntimeError("io error"))

    outbox_id = await write_outbox_entry(
        store,
        deployment_id="dep-1",
        strategy_id="strat-1",
        cycle_id="cycle-1",
        ledger_entry_id="ledger-1",
        intent_type="SUPPLY",
        wallet_address="0xabc",
    )

    assert outbox_id is None


# ──────────────────────────────────────────────────────────────────────────────
# Lending FIFO lot management via drain_one
# ──────────────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_drain_one_lending_borrow_records_fifo_lot(monkeypatch: pytest.MonkeyPatch) -> None:
    """BORROW drain_one should add a lot to the FIFO store."""
    led_id = str(uuid.uuid4())
    outbox_row = _make_outbox_row(
        led_id,
        intent_type="BORROW",
        wallet_address="0xwallet",
        position_key="lending:arbitrum:aave_v3:0xwallet:usdc",
    )
    # Fake extracted_data_json with borrow_amount
    extracted = json.dumps({"borrow_amount": 1000_000_000})  # 1000 USDC (6 dec)
    price_inputs = json.dumps({"USDC": "1.0"})
    ledger_row = _make_ledger_row(
        led_id,
        intent_type="BORROW",
        protocol="aave_v3",
        extracted_data_json=extracted,
        price_inputs_json=price_inputs,
    )

    store = _make_mock_store(outbox_row=outbox_row, ledger_row=ledger_row, already_written=False)
    basis = FIFOBasisStore()
    proc = AccountingProcessor(state_manager=store, basis_store=basis, deployment_id="dep-1")

    # Patch token resolver at the source module — the handler imports lazily.
    mock_token_info = MagicMock()
    mock_token_info.decimals = 6
    mock_resolver = MagicMock(resolve=MagicMock(return_value=mock_token_info))

    with patch("almanak.framework.data.tokens.resolver.get_token_resolver", return_value=mock_resolver):
        result = await proc.drain_one(led_id)

    assert result is True, "drain_one must return True for a successful BORROW"
    # Accounting event must have been written via the writer
    store.save_accounting_event.assert_awaited_once()
    # FIFO lot must be recorded so future REPAY can match interest.
    # FIFOBasisStore._key lowercases the token, so "USDC" → "usdc".
    position_key = "lending:arbitrum:aave_v3:0xwallet:usdc"
    key = f"dep-1:{position_key}:usdc"
    lots = basis._lots.get(key, [])
    assert len(lots) == 1, f"Expected 1 BORROW lot in store, found {len(lots)} (keys={list(basis._lots)})"
    assert lots[0]["remaining"] > 0


# ──────────────────────────────────────────────────────────────────────────────
# initialize_run_loop drain_pending integration
# ──────────────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_initialize_run_loop_drains_pending_outbox() -> None:
    """drain_pending() is called in initialize_run_loop with deployment_id set first."""
    from almanak.framework.runner._run_loop_helpers import initialize_run_loop

    strategy = MagicMock()
    strategy.deployment_id = "dep-1"
    strategy._wallet_activity_provider = None
    strategy_id = "strat-1"

    runner = MagicMock()
    runner.config.enable_state_persistence = True
    runner._is_live_mode.return_value = False
    runner._lending_basis_store = FIFOBasisStore()

    state_manager = MagicMock()
    state_manager.initialize = AsyncMock()
    state_manager.get_accounting_events_sync = MagicMock(return_value=[])
    state_manager.load_state = AsyncMock(return_value=None)
    runner.state_manager = state_manager

    processor = MagicMock()
    processor._deployment_id = ""

    # Use side_effect to assert deployment_id is already set at call time, not
    # just after initialize_run_loop returns — catches regressions where
    # drain_pending fires before _deployment_id is assigned.
    def _drain_pending_probe() -> int:
        assert processor._deployment_id == "dep-1", (
            f"deployment_id must be set before drain_pending is called, got {processor._deployment_id!r}"
        )
        return 3

    processor.drain_pending = AsyncMock(side_effect=_drain_pending_probe)
    runner._accounting_processor = processor

    runner._recover_incomplete_sessions = AsyncMock(return_value=0)
    runner._register_with_gateway = MagicMock()
    runner._lifecycle_write_state = MagicMock()
    runner._get_gateway_client = MagicMock(return_value=None)
    runner._shutdown_requested = False
    runner._signal_received = False
    runner._terminal_lifecycle_state = None
    runner._terminal_lifecycle_error_message = None

    with patch("almanak.framework.runner._run_loop_helpers.add_event"):
        await initialize_run_loop(runner, strategy, strategy_id, interval=60)

    processor.drain_pending.assert_awaited_once()


@pytest.mark.asyncio
async def test_initialize_run_loop_drain_pending_raises_in_live_mode() -> None:
    """drain_pending exception raises RuntimeError in live mode."""
    from almanak.framework.runner._run_loop_helpers import initialize_run_loop

    strategy = MagicMock()
    strategy.deployment_id = "dep-1"
    strategy._wallet_activity_provider = None
    strategy_id = "strat-1"

    runner = MagicMock()
    runner.config.enable_state_persistence = True
    runner._is_live_mode.return_value = True
    runner._lending_basis_store = FIFOBasisStore()

    state_manager = MagicMock()
    state_manager.initialize = AsyncMock()
    state_manager.get_accounting_events_sync = MagicMock(return_value=[])
    runner.state_manager = state_manager

    processor = MagicMock()
    processor._deployment_id = ""
    processor.drain_pending = AsyncMock(side_effect=RuntimeError("db down"))
    runner._accounting_processor = processor

    runner._recover_incomplete_sessions = AsyncMock(return_value=0)
    runner._register_with_gateway = MagicMock()
    runner._lifecycle_write_state = MagicMock()
    runner._get_gateway_client = MagicMock(return_value=None)
    runner._shutdown_requested = False
    runner._signal_received = False
    runner._terminal_lifecycle_state = None
    runner._terminal_lifecycle_error_message = None

    with patch("almanak.framework.runner._run_loop_helpers.add_event"):
        with pytest.raises(RuntimeError, match=r"AccountingProcessor\.drain_pending failed"):
            await initialize_run_loop(runner, strategy, strategy_id, interval=60)
