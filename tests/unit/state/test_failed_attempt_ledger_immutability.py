"""A confirmed attempt retains its first durable valuation across replay and races."""

import asyncio
import json
from copy import deepcopy
from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock, Mock

import pytest

from almanak.framework.models.run_mode import RunMode
from almanak.framework.observability.ledger import LedgerEntry
from almanak.framework.state.backends.sqlite import SQLiteConfig, SQLiteStore
from almanak.gateway.proto import gateway_pb2
from almanak.gateway.services.state_service import StateServiceServicer


def make_entry():
    row = LedgerEntry(
        id="a5d0a14d-aebc-4794-96b5-3b9d07438412",
        deployment_id="deployment:abc123abc123",
        cycle_id="original-cycle",
        execution_mode=RunMode.LIVE,
        timestamp=datetime(2026, 9, 8, tzinfo=UTC),
        intent_type="LP_CLOSE",
        chain="base",
        protocol="uniswap_v4",
        success=False,
        tx_hash="0x" + "ab" * 32,
        gas_used=123,
        gas_usd="0.01",
        price_inputs_json='{"ETH":{"price_usd":"2500"}}',
    )
    row.extracted_data_json = json.dumps(
        {
            "failed_attempt": {
                "schema_version": 1,
                "ledger_entry_id": row.id,
                "receipts": [{"transaction_hash": row.tx_hash, "status": 0, "gas_used": 123}],
                "total_gas_used": 123,
                "measured_gas_cost_wei": "4000000000",
            },
            "compiler_evidence": {"quote_block": 100},
            "execution_intent_id": "original-execution-intent",
        }
    )
    return row


@pytest.fixture
def entry():
    return make_entry()


def replay(row):
    changed = deepcopy(row)
    changed.timestamp += timedelta(days=1)
    changed.cycle_id = "later-cycle"
    changed.gas_usd = "0.04"
    changed.price_inputs_json = '{"ETH":{"price_usd":"10000"}}'
    return changed


def mutation(row, kind):
    changed = replay(row)
    data = json.loads(changed.extracted_data_json)
    if kind == "remove":
        data.pop("failed_attempt")
    elif kind == "null":
        data["failed_attempt"] = None
    elif kind == "receipt":
        data["failed_attempt"]["receipts"][0]["gas_used"] += 1
    elif kind == "compiler":
        data["compiler_evidence"]["quote_block"] += 1
    elif kind == "lineage_changed":
        data["execution_intent_id"] = "different-execution-intent"
    elif kind == "lineage_removed":
        data.pop("execution_intent_id")
    elif kind == "success":
        changed.success = True
        changed.amount_in = "1"
        changed.amount_out = "1"
    elif kind == "identity":
        changed.protocol = "uniswap_v3"
    changed.extracted_data_json = json.dumps(data)
    return changed


@pytest.mark.asyncio
async def test_sqlite_replay_preserves_original_and_normal_rows_still_update(tmp_path, entry):
    store = SQLiteStore(SQLiteConfig(db_path=str(tmp_path / "state.db")))
    await store.initialize()
    try:
        await store.save_ledger_entry(entry)
        original = await store.get_ledger_entry_by_id(entry.id)
        await store.save_ledger_entry(replay(entry))
        assert await store.get_ledger_entry_by_id(entry.id) == original
        ordinary = deepcopy(entry)
        ordinary.id = "ordinary"
        ordinary.extracted_data_json = "{}"
        await store.save_ledger_entry(ordinary)
        changed = replay(ordinary)
        await store.save_ledger_entry(changed)
        saved = await store.get_ledger_entry_by_id(ordinary.id)
        assert saved["gas_usd"] == changed.gas_usd
        assert saved["cycle_id"] == changed.cycle_id
    finally:
        await store.close()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "kind", ["remove", "null", "receipt", "compiler", "lineage_changed", "lineage_removed", "success", "identity"]
)
async def test_sqlite_conflicting_or_unmarked_writer_cannot_replace_attempt(tmp_path, entry, kind):
    store = SQLiteStore(SQLiteConfig(db_path=str(tmp_path / "state.db")))
    await store.initialize()
    try:
        await store.save_ledger_entry(entry)
        original = await store.get_ledger_entry_by_id(entry.id)
        with pytest.raises(ValueError):
            await store.save_ledger_entry(mutation(entry, kind))
        assert await store.get_ledger_entry_by_id(entry.id) == original
        await store.save_ledger_entry(replay(entry))
        assert await store.get_ledger_entry_by_id(entry.id) == original
    finally:
        await store.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("conflict", [False, True])
async def test_sqlite_two_connections_race_without_overwriting_first_commit(tmp_path, entry, conflict):
    stores = [SQLiteStore(SQLiteConfig(db_path=str(tmp_path / "state.db"))) for _ in range(2)]
    for store in stores:
        await store.initialize()
    try:
        second = mutation(entry, "compiler") if conflict else replay(entry)
        results = await asyncio.gather(
            stores[0].save_ledger_entry(entry), stores[1].save_ledger_entry(second), return_exceptions=True
        )
        assert all(result is None or isinstance(result, ValueError) for result in results)
        assert sum(isinstance(result, ValueError) for result in results) == int(conflict)
        saved = await stores[0].get_ledger_entry_by_id(entry.id)
        candidates = (entry, second)
        retained = [candidate for candidate in candidates if saved == candidate.to_dict()]
        assert len(retained) == 1
        winner = retained[0]
        assert results[candidates.index(winner)] is None
        first = dict(saved)
        later_attempts = [replay(winner) for _ in stores]
        for index, attempt in enumerate(later_attempts):
            attempt.cycle_id = f"post-race-replay-{index}"
        await asyncio.gather(
            *(store.save_ledger_entry(attempt) for store, attempt in zip(stores, later_attempts, strict=True))
        )
        assert await stores[1].get_ledger_entry_by_id(entry.id) == first
    finally:
        for store in stores:
            await store.close()


def request(row):
    return gateway_pb2.SaveLedgerEntryRequest(
        id=row.id,
        deployment_id=row.deployment_id,
        cycle_id=row.cycle_id,
        execution_mode="live",
        timestamp=int(row.timestamp.timestamp()),
        intent_type=row.intent_type,
        chain=row.chain,
        protocol=row.protocol,
        success=row.success,
        tx_hash=row.tx_hash,
        gas_used=row.gas_used,
        gas_usd=row.gas_usd,
        amount_in=row.amount_in,
        amount_out=row.amount_out,
        extracted_data_json=row.extracted_data_json.encode(),
        price_inputs_json=row.price_inputs_json.encode(),
    )


def hosted(existing, command="INSERT 0 0"):
    service = StateServiceServicer.__new__(StateServiceServicer)
    service._snapshot_pool = object()
    service._ensure_snapshot_pool = AsyncMock()
    service._snapshot_execute = AsyncMock(return_value=command)
    service._snapshot_fetchrow = AsyncMock(return_value=existing)
    return service


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "kind",
    ["identical", "remove", "null", "receipt", "compiler", "lineage_changed", "lineage_removed", "success", "identity"],
)
async def test_hosted_conflict_response_verifies_without_rewriting(entry, kind):
    original = entry.to_dict()
    original["extracted_data_json"] = json.loads(original["extracted_data_json"])
    service = hosted(original)
    incoming = replay(entry) if kind == "identical" else mutation(entry, kind)
    response = await service.SaveLedgerEntry(request(incoming), Mock())
    assert response.success == (kind == "identical")
    assert original["gas_usd"] == "0.01"
    assert service._snapshot_execute.await_count <= 1
    if service._snapshot_execute.await_count:
        query = service._snapshot_execute.await_args.args[0]
        # Both sides must protect the conflict: an ordinary writer cannot erase
        # an existing marker, nor can a marked writer replace an ordinary row.
        assert "WHERE NOT" in query
        assert "transaction_ledger.extracted_data_json ? 'failed_attempt'" in query
        assert "EXCLUDED.extracted_data_json ? 'failed_attempt'" in query
        assert "DO UPDATE" in query


@pytest.mark.asyncio
async def test_hosted_noop_without_durable_matching_row_is_failure(entry):
    service = hosted(None)
    response = await service.SaveLedgerEntry(request(entry), Mock())
    assert not response.success


@pytest.mark.asyncio
async def test_hosted_new_attempt_and_ordinary_upsert_need_no_replay_read(entry):
    service = hosted(None, "INSERT 0 1")
    assert (await service.SaveLedgerEntry(request(entry), Mock())).success
    ordinary = replay(entry)
    ordinary.extracted_data_json = "{}"
    assert (await service.SaveLedgerEntry(request(ordinary), Mock())).success
    service._snapshot_fetchrow.assert_not_awaited()


@pytest.mark.asyncio
async def test_server_sqlite_lookup_requires_strict_backend_read():
    from types import SimpleNamespace

    warm = SimpleNamespace(get_ledger_entry_by_id=AsyncMock(side_effect=RuntimeError("read failed")))
    service = StateServiceServicer.__new__(StateServiceServicer)
    service._ensure_initialized = AsyncMock()
    service._state_manager = SimpleNamespace(warm_backend=warm)
    with pytest.raises(RuntimeError, match="read failed"):
        await service._get_ledger_entry_sqlite_response("some-id")
    warm.get_ledger_entry_by_id.assert_awaited_once_with("some-id", strict=True)
    service._state_manager.warm_backend = None
    with pytest.raises(RuntimeError, match="unavailable"):
        await service._get_ledger_entry_sqlite_response("some-id")


@pytest.mark.asyncio
@pytest.mark.parametrize("strict", [False, True])
async def test_facade_initializes_before_reading_existing_ledger_identity(tmp_path, entry, strict):
    from almanak.framework.state.state_manager import (
        SQLiteConfigLight,
        StateManager,
        StateManagerConfig,
        WarmBackendType,
    )

    path = str(tmp_path / "ledger.db")
    store = SQLiteStore(SQLiteConfig(db_path=path))
    await store.initialize()
    await store.save_ledger_entry(entry)
    await store.close()
    manager = StateManager(
        StateManagerConfig(
            warm_backend=WarmBackendType.SQLITE,
            sqlite_config=SQLiteConfigLight(db_path=path),
            load_state_on_startup=False,
        )
    )
    assert manager._warm is None
    try:
        saved = await manager.get_ledger_entry_by_id(entry.id, strict=strict)
        assert saved["id"] == entry.id
        assert saved["extracted_data_json"] == entry.extracted_data_json
    finally:
        await manager.close()


@pytest.mark.asyncio
async def test_hosted_conflict_is_precondition_and_database_failure_is_internal(entry):
    import grpc

    context = Mock()
    service = hosted(entry.to_dict())
    response = await service.SaveLedgerEntry(request(mutation(entry, "compiler")), context)
    assert not response.success
    context.set_code.assert_called_once_with(grpc.StatusCode.FAILED_PRECONDITION)

    context = Mock()
    service._snapshot_execute.side_effect = ValueError("private database diagnostic")
    response = await service.SaveLedgerEntry(request(entry), context)
    assert not response.success and response.error == "internal server error"
    context.set_code.assert_called_once_with(grpc.StatusCode.INTERNAL)
