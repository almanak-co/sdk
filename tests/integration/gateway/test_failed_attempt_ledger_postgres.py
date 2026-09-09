"""Real PostgreSQL conflict arbitration for immutable failed-attempt ledger rows.

ALMANAK_TEST_POSTGRES_DSN must name a dedicated loopback test database. Each test
creates and drops only its own schema; no deployed schema or migration is used.
"""

import asyncio
import os
from copy import deepcopy
from unittest.mock import AsyncMock, Mock
from urllib.parse import urlparse
from uuid import uuid4

import pytest
import pytest_asyncio

from almanak.gateway.services.state_service import StateServiceServicer
from tests.unit.state.test_failed_attempt_ledger_immutability import make_entry, mutation, replay, request


@pytest.fixture
def entry():
    return make_entry()


@pytest_asyncio.fixture
async def service():
    dsn = os.environ.get("ALMANAK_TEST_POSTGRES_DSN")
    if not dsn:
        pytest.skip("Dedicated local PostgreSQL test DSN not supplied")
    parsed = urlparse(dsn)
    if parsed.hostname not in {"localhost", "127.0.0.1", "::1"} or not parsed.path.startswith(
        "/v4_failed_attempt_test"
    ):
        pytest.fail("Refusing a non-test or non-loopback PostgreSQL database")
    asyncpg = pytest.importorskip("asyncpg")
    schema = "failed_attempt_" + uuid4().hex
    control = await asyncpg.connect(dsn)
    pool = None
    try:
        await control.execute(f'CREATE SCHEMA "{schema}"')
        await control.execute(f'''CREATE TABLE "{schema}".transaction_ledger (
            id TEXT PRIMARY KEY, cycle_id TEXT, deployment_id TEXT, execution_mode TEXT,
            timestamp TIMESTAMPTZ, intent_type TEXT, token_in TEXT, amount_in TEXT,
            token_out TEXT, amount_out TEXT, effective_price TEXT, slippage_bps REAL,
            gas_used BIGINT, gas_usd TEXT, tx_hash TEXT, chain TEXT, protocol TEXT,
            success BOOLEAN, error TEXT, extracted_data_json JSONB, price_inputs_json JSONB,
            pre_state_json JSONB, post_state_json JSONB
        )''')
        pool = await asyncpg.create_pool(dsn, min_size=2, max_size=8, server_settings={"search_path": schema})
        server = StateServiceServicer.__new__(StateServiceServicer)
        server._snapshot_pool = pool
        server._snapshot_schema = None
        server._ensure_snapshot_pool = AsyncMock()
        yield server
    finally:
        if pool is not None:
            await pool.close()
        await control.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await control.close()


async def row(service, identifier):
    return dict(await service._snapshot_fetchrow("SELECT * FROM transaction_ledger WHERE id=$1", identifier))


@pytest.mark.asyncio
async def test_postgres_concurrent_identical_attempts_keep_first_price(service, entry):
    candidates = [deepcopy(entry) for _ in range(8)]
    for index, candidate in enumerate(candidates):
        candidate.cycle_id = f"cycle-{index}"
        candidate.gas_usd = str(index + 1)
    results = await asyncio.gather(*(service.SaveLedgerEntry(request(candidate), Mock()) for candidate in candidates))
    assert all(result.success for result in results)
    original = await row(service, entry.id)
    assert original["gas_usd"] == str(int(original["cycle_id"].split("-")[1]) + 1)
    await asyncio.gather(*(service.SaveLedgerEntry(request(replay(entry)), Mock()) for _ in range(8)))
    assert await row(service, entry.id) == original


@pytest.mark.asyncio
async def test_postgres_conflicting_first_insert_race_has_one_winner(service, entry):
    changed = mutation(entry, "compiler")
    results = await asyncio.gather(
        service.SaveLedgerEntry(request(entry), Mock()), service.SaveLedgerEntry(request(changed), Mock())
    )
    assert sum(result.success for result in results) == 1
    winner = entry if results[0].success else changed
    original = await row(service, entry.id)
    assert original["cycle_id"] == winner.cycle_id
    assert (await service.SaveLedgerEntry(request(replay(winner)), Mock())).success
    assert await row(service, entry.id) == original


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "kind",
    ["remove", "null", "receipt", "compiler", "success", "identity", "lineage_changed", "lineage_removed"],
)
async def test_postgres_marked_row_rejects_changes_and_marker_removal(service, entry, kind):
    assert (await service.SaveLedgerEntry(request(entry), Mock())).success
    original = await row(service, entry.id)
    assert not (await service.SaveLedgerEntry(request(mutation(entry, kind)), Mock())).success
    assert await row(service, entry.id) == original


@pytest.mark.asyncio
async def test_postgres_ordinary_upsert_unchanged_and_cannot_be_retyped(service, entry):
    ordinary = deepcopy(entry)
    ordinary.extracted_data_json = "{}"
    assert (await service.SaveLedgerEntry(request(ordinary), Mock())).success
    assert (await service.SaveLedgerEntry(request(replay(ordinary)), Mock())).success
    original = await row(service, entry.id)
    assert original["gas_usd"] == replay(ordinary).gas_usd
    assert not (await service.SaveLedgerEntry(request(entry), Mock())).success
    assert await row(service, entry.id) == original
