"""Unit tests for AccountingProcessor (VIB-3467).

Tests drain_one, drain_pending, idempotency, and FIFO lot management.
Uses in-process mocks — no SQLite, no gateway, no network.
"""

from __future__ import annotations

import json
import uuid
from datetime import UTC, datetime
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from almanak.framework.accounting.basis import FIFOBasisStore
from almanak.framework.accounting.processor import AccountingProcessor, write_outbox_entry
from almanak.framework.models.run_mode import RunMode
from almanak.framework.state.exceptions import AccountingPersistenceError, AccountingWriteKind
from tests.unit.runner._boot_snapshot import measured_boot_snapshot

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
    execution_mode: str = "live",
) -> dict[str, Any]:
    return {
        "id": ledger_entry_id,
        "deployment_id": "dep-1",
        "cycle_id": "cycle-1",
        "execution_mode": execution_mode,
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
async def test_drain_one_explicit_no_accounting_marks_processed() -> None:
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
    assert [c[1] for c in calls] == ["processing", "processed"]


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
async def test_drain_one_live_writer_exception_marks_failed_and_raises() -> None:
    led_id = str(uuid.uuid4())
    outbox_row = _make_outbox_row(led_id, intent_type="SUPPLY")
    ledger_row = _make_ledger_row(led_id, intent_type="SUPPLY")
    store = _make_mock_store(outbox_row=outbox_row, ledger_row=ledger_row, already_written=False)
    proc = AccountingProcessor(state_manager=store, basis_store=FIFOBasisStore(), deployment_id="dep-1")

    # Make the durable store raise after dispatch produced a valid live event.
    store.save_accounting_event = AsyncMock(side_effect=RuntimeError("db down"))
    event = MagicMock(identity=MagicMock(execution_mode="live", deployment_id="dep-1"))
    proc._dispatch = MagicMock(return_value=event)

    with pytest.raises(AccountingPersistenceError, match="db down"):
        await proc.drain_one(led_id)

    calls = [c.args for c in store.update_outbox_entry.call_args_list]
    assert any(c[1] == "failed" for c in calls)
    assert not any(c[1] == "processed" for c in calls)


@pytest.mark.asyncio
async def test_drain_one_live_dispatch_none_marks_failed_and_raises(caplog: pytest.LogCaptureFixture) -> None:
    """A claimed live accounting path may not silently book ``None`` as processed."""
    led_id = str(uuid.uuid4())
    outbox_row = _make_outbox_row(led_id, intent_type="LP_COLLECT_FEES")
    ledger_row = _make_ledger_row(led_id, intent_type="LP_COLLECT_FEES", protocol="curve")
    store = _make_mock_store(outbox_row=outbox_row, ledger_row=ledger_row)
    proc = AccountingProcessor(state_manager=store, basis_store=FIFOBasisStore(), deployment_id="dep-1")
    proc._dispatch = MagicMock(return_value=None)

    with (
        caplog.at_level("ERROR"),
        pytest.raises(
            AccountingPersistenceError,
            match="classified accounting dispatch produced no event",
        ),
    ):
        await proc.drain_one(led_id)

    store.save_accounting_event.assert_not_called()
    calls = [c.args for c in store.update_outbox_entry.call_args_list]
    assert [c[1] for c in calls] == ["processing", "failed"]
    assert "produced no event" in calls[-1][2]
    assert calls[-1][3] == 1
    assert "produced no event" in caplog.text


@pytest.mark.asyncio
async def test_drain_one_live_dispatch_exception_marks_failed_and_raises() -> None:
    """A handler crash is the same mandatory-event failure class as bare ``None``."""
    led_id = str(uuid.uuid4())
    outbox_row = _make_outbox_row(led_id, intent_type="LP_COLLECT_FEES")
    ledger_row = _make_ledger_row(led_id, intent_type="LP_COLLECT_FEES", protocol="curve")
    store = _make_mock_store(outbox_row=outbox_row, ledger_row=ledger_row)
    proc = AccountingProcessor(state_manager=store, basis_store=FIFOBasisStore(), deployment_id="dep-1")
    proc._dispatch = MagicMock(side_effect=RuntimeError("handler broke"))

    with pytest.raises(AccountingPersistenceError, match="accounting dispatch raised") as raised:
        await proc.drain_one(led_id)

    assert isinstance(raised.value.cause, RuntimeError)
    calls = [c.args for c in store.update_outbox_entry.call_args_list]
    assert [c[1] for c in calls] == ["processing", "failed"]


@pytest.mark.asyncio
async def test_drain_one_paper_dispatch_none_marks_failed_and_returns_false(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Non-live drops remain retryable and visible without halting the loop."""
    led_id = str(uuid.uuid4())
    outbox_row = _make_outbox_row(led_id, intent_type="LP_COLLECT_FEES")
    ledger_row = _make_ledger_row(
        led_id,
        intent_type="LP_COLLECT_FEES",
        protocol="curve",
        execution_mode="paper",
    )
    store = _make_mock_store(outbox_row=outbox_row, ledger_row=ledger_row)
    proc = AccountingProcessor(state_manager=store, basis_store=FIFOBasisStore(), deployment_id="dep-1")
    proc._dispatch = MagicMock(return_value=None)

    with caplog.at_level("ERROR"):
        result = await proc.drain_one(led_id)

    assert result is False
    store.save_accounting_event.assert_not_called()
    calls = [c.args for c in store.update_outbox_entry.call_args_list]
    assert [c[1] for c in calls] == ["processing", "failed"]
    assert "produced no event" in caplog.text


@pytest.mark.asyncio
async def test_drain_one_real_lp_handler_missing_pool_fails_closed_in_live() -> None:
    """The real LP handler's pool-resolution ``None`` is a mandatory live drop."""
    led_id = str(uuid.uuid4())
    outbox_row = _make_outbox_row(
        led_id,
        intent_type="LP_COLLECT_FEES",
        position_key="",
        market_id="",
    )
    ledger_row = _make_ledger_row(led_id, intent_type="LP_COLLECT_FEES", protocol="curve")
    store = _make_mock_store(outbox_row=outbox_row, ledger_row=ledger_row)
    proc = AccountingProcessor(state_manager=store, basis_store=FIFOBasisStore(), deployment_id="dep-1")

    with pytest.raises(AccountingPersistenceError, match="classified accounting dispatch produced no event"):
        await proc.drain_one(led_id)

    store.save_accounting_event.assert_not_called()
    assert [c.args[1] for c in store.update_outbox_entry.call_args_list] == ["processing", "failed"]


@pytest.mark.asyncio
async def test_drain_one_real_lp_handler_missing_pool_degrades_in_paper() -> None:
    """The same real-handler drop remains failed/retryable without halting paper."""
    led_id = str(uuid.uuid4())
    outbox_row = _make_outbox_row(
        led_id,
        intent_type="LP_COLLECT_FEES",
        position_key="",
        market_id="",
    )
    ledger_row = _make_ledger_row(
        led_id,
        intent_type="LP_COLLECT_FEES",
        protocol="curve",
        execution_mode="paper",
    )
    store = _make_mock_store(outbox_row=outbox_row, ledger_row=ledger_row)
    proc = AccountingProcessor(state_manager=store, basis_store=FIFOBasisStore(), deployment_id="dep-1")

    assert await proc.drain_one(led_id) is False

    store.save_accounting_event.assert_not_called()
    assert [c.args[1] for c in store.update_outbox_entry.call_args_list] == ["processing", "failed"]


@pytest.mark.asyncio
async def test_drain_one_unstamped_row_uses_fail_closed_live_processor_mode() -> None:
    """A legacy empty stamp cannot downgrade a live runner's mandatory drop."""
    led_id = str(uuid.uuid4())
    outbox_row = _make_outbox_row(led_id, intent_type="LP_COLLECT_FEES")
    ledger_row = _make_ledger_row(
        led_id,
        intent_type="LP_COLLECT_FEES",
        protocol="curve",
        execution_mode="",
    )
    store = _make_mock_store(outbox_row=outbox_row, ledger_row=ledger_row)
    proc = AccountingProcessor(state_manager=store, basis_store=FIFOBasisStore(), deployment_id="dep-1")
    proc._dispatch = MagicMock(return_value=None)

    with pytest.raises(AccountingPersistenceError, match="classified accounting dispatch produced no event"):
        await proc.drain_one(led_id)

    assert proc._dispatch.call_args.args[1]["execution_mode"] is RunMode.LIVE


@pytest.mark.asyncio
async def test_drain_one_unstamped_row_uses_configured_paper_processor_mode() -> None:
    """Legacy rows inherit a known paper runner mode rather than assuming live."""
    led_id = str(uuid.uuid4())
    outbox_row = _make_outbox_row(led_id, intent_type="LP_COLLECT_FEES")
    ledger_row = _make_ledger_row(
        led_id,
        intent_type="LP_COLLECT_FEES",
        protocol="curve",
        execution_mode="",
    )
    store = _make_mock_store(outbox_row=outbox_row, ledger_row=ledger_row)
    proc = AccountingProcessor(
        state_manager=store,
        basis_store=FIFOBasisStore(),
        deployment_id="dep-1",
        run_mode=RunMode.PAPER,
    )
    proc._dispatch = MagicMock(return_value=None)

    assert await proc.drain_one(led_id) is False
    assert proc._dispatch.call_args.args[1]["execution_mode"] is RunMode.PAPER


@pytest.mark.asyncio
async def test_drain_one_invalid_mode_does_not_inherit_configured_processor_mode() -> None:
    """An explicit corrupt stamp fails its row instead of being reinterpreted."""
    led_id = str(uuid.uuid4())
    outbox_row = _make_outbox_row(led_id, intent_type="SUPPLY")
    ledger_row = _make_ledger_row(led_id, intent_type="SUPPLY", execution_mode="livve")
    store = _make_mock_store(outbox_row=outbox_row, ledger_row=ledger_row)
    proc = AccountingProcessor(
        state_manager=store,
        basis_store=FIFOBasisStore(),
        deployment_id="dep-1",
        run_mode=RunMode.PAPER,
    )
    proc._dispatch = MagicMock()

    assert await proc.drain_one(led_id) is False
    proc._dispatch.assert_not_called()
    assert store.update_outbox_entry.call_args_list[-1].args[1] == "failed"


@pytest.mark.asyncio
async def test_drain_one_paper_writer_false_marks_failed_not_processed() -> None:
    """A non-live writer decline is a failed write, never processed success."""
    led_id = str(uuid.uuid4())
    outbox_row = _make_outbox_row(led_id, intent_type="SUPPLY")
    ledger_row = _make_ledger_row(led_id, intent_type="SUPPLY", execution_mode="paper")
    store = _make_mock_store(outbox_row=outbox_row, ledger_row=ledger_row)
    proc = AccountingProcessor(state_manager=store, basis_store=FIFOBasisStore(), deployment_id="dep-1")
    proc._dispatch = MagicMock(return_value=MagicMock())
    proc._writer.write = AsyncMock(return_value=False)

    result = await proc.drain_one(led_id)

    assert result is False
    calls = [c.args for c in store.update_outbox_entry.call_args_list]
    assert [c[1] for c in calls] == ["processing", "failed"]


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


@pytest.mark.asyncio
async def test_drain_pending_propagates_live_accounting_failure() -> None:
    """Startup must not turn a typed live accounting failure into successful boot."""
    led_id = str(uuid.uuid4())
    outbox_row = _make_outbox_row(led_id)
    store = _make_mock_store(outbox_row=outbox_row)
    proc = AccountingProcessor(state_manager=store, basis_store=FIFOBasisStore(), deployment_id="dep-1")
    failure = AccountingPersistenceError(
        AccountingWriteKind.ACCOUNTING,
        deployment_id="dep-1",
        message="mandatory event missing",
    )
    proc.drain_one = AsyncMock(side_effect=failure)

    with pytest.raises(AccountingPersistenceError, match="mandatory event missing"):
        await proc.drain_pending()


@pytest.mark.asyncio
async def test_drain_pending_continues_after_live_failure_then_reraises() -> None:
    """One poisoned startup row must not block recovery of independent rows."""
    failed_id = str(uuid.uuid4())
    healthy_id = str(uuid.uuid4())
    rows = [_make_outbox_row(failed_id), _make_outbox_row(healthy_id)]
    store = _make_mock_store()
    store.get_outbox_pending = MagicMock(return_value=rows)
    proc = AccountingProcessor(state_manager=store, basis_store=FIFOBasisStore(), deployment_id="dep-1")
    failure = AccountingPersistenceError(
        AccountingWriteKind.ACCOUNTING,
        deployment_id="dep-1",
        message="first row poisoned",
    )
    proc.drain_one = AsyncMock(side_effect=[failure, True])

    with pytest.raises(AccountingPersistenceError, match="first row poisoned"):
        await proc.drain_pending()

    assert [call.args[0] for call in proc.drain_one.await_args_list] == [failed_id, healthy_id]


# ──────────────────────────────────────────────────────────────────────────────
# write_outbox_entry helper
# ──────────────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_write_outbox_entry_returns_id() -> None:
    store = MagicMock()
    store.save_outbox_entry = MagicMock(return_value=None)

    outbox_id = await write_outbox_entry(
        store,
        deployment_id="strat-1",
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
        deployment_id="strat-1",
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
        deployment_id="strat-1",
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
    """drain_pending() uses the immutable boot deployment_id before it runs."""
    from almanak.framework.migration import CutoverStorageNotSupported
    from almanak.framework.runner._run_loop_helpers import initialize_run_loop

    strategy = MagicMock()
    strategy.deployment_id = "dep-1"
    strategy._wallet_activity_provider = None
    deployment_id = "strat-1"

    runner = MagicMock()
    runner.config.enable_state_persistence = True
    runner._is_live_mode.return_value = False
    runner._lending_basis_store = FIFOBasisStore()

    state_manager = MagicMock()
    state_manager.initialize = AsyncMock()
    # This integration test characterizes outbox recovery after startup.  A
    # prior snapshot makes that post-boot state explicit; fresh boot capture is
    # covered by the dedicated VIB-5854 runner tests.
    state_manager.get_latest_snapshot = AsyncMock(return_value=measured_boot_snapshot("strat-1"))
    state_manager.get_accounting_events_sync = MagicMock(return_value=[])
    state_manager.load_state = AsyncMock(return_value=None)
    # Audit F4 (T12 cutover): the boot guard added by VIB-4198 awaits
    # ``upsert_migration_state`` on the state manager. A bare MagicMock
    # returns a non-awaitable MagicMock for this attr. Wire AsyncMock
    # stubs that raise ``CutoverStorageNotSupported`` — that's the
    # canonical "this backend doesn't support cutover storage" signal
    # the boot guard catches and degrades on (controlled-degrade path).
    state_manager.upsert_migration_state = AsyncMock(
        side_effect=CutoverStorageNotSupported("test stub: cutover storage not implemented")
    )
    state_manager.get_migration_state = AsyncMock(
        side_effect=CutoverStorageNotSupported("test stub: cutover storage not implemented")
    )
    runner.state_manager = state_manager

    processor = MagicMock()
    processor._deployment_id = ""

    # The strategy attribute is deliberately different: the runner-resolved boot
    # identity is canonical and must not be replaced during recovery.
    def _drain_pending_probe() -> int:
        assert processor._deployment_id == deployment_id, (
            f"boot deployment_id must be set before drain_pending is called, got {processor._deployment_id!r}"
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
        await initialize_run_loop(runner, strategy, deployment_id, interval=60)

    processor.drain_pending.assert_awaited_once()


@pytest.mark.asyncio
async def test_initialize_run_loop_drain_pending_raises_in_live_mode() -> None:
    """drain_pending exception raises RuntimeError in live mode."""
    from almanak.framework.migration import CutoverStorageNotSupported
    from almanak.framework.runner._run_loop_helpers import initialize_run_loop

    strategy = MagicMock()
    strategy.deployment_id = "dep-1"
    strategy._wallet_activity_provider = None
    deployment_id = "strat-1"

    runner = MagicMock()
    runner.config.enable_state_persistence = True
    runner._is_live_mode.return_value = True
    runner._lending_basis_store = FIFOBasisStore()

    state_manager = MagicMock()
    state_manager.initialize = AsyncMock()
    # See the post-boot fixture rationale in the sibling test above.
    state_manager.get_latest_snapshot = AsyncMock(return_value=measured_boot_snapshot("strat-1"))
    state_manager.get_accounting_events_sync = MagicMock(return_value=[])
    # See test_initialize_run_loop_drains_pending_outbox above for the
    # rationale on these two stubs (audit F4).
    state_manager.upsert_migration_state = AsyncMock(
        side_effect=CutoverStorageNotSupported("test stub: cutover storage not implemented")
    )
    state_manager.get_migration_state = AsyncMock(
        side_effect=CutoverStorageNotSupported("test stub: cutover storage not implemented")
    )
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
            await initialize_run_loop(runner, strategy, deployment_id, interval=60)
