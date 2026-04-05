"""Tests for transaction ledger SQLite persistence."""

from datetime import UTC, datetime

import pytest
import pytest_asyncio

from almanak.framework.observability.ledger import LedgerEntry
from almanak.framework.state.backends.sqlite import SQLiteConfig, SQLiteStore


@pytest_asyncio.fixture
async def sqlite_store():
    """Create an in-memory SQLite store."""
    store = SQLiteStore(SQLiteConfig(db_path=":memory:"))
    await store.initialize()
    return store


@pytest.mark.asyncio
class TestLedgerSQLitePersistence:
    """Tests for ledger save/query in SQLite."""

    async def test_save_and_query_ledger_entry(self, sqlite_store):
        entry = LedgerEntry(
            id="entry-1",
            cycle_id="cycle-1",
            strategy_id="strat-1",
            timestamp=datetime(2026, 4, 5, 12, 0, 0, tzinfo=UTC),
            intent_type="SWAP",
            token_in="USDC",
            amount_in="1000",
            token_out="ETH",
            amount_out="0.5",
            effective_price="2000",
            slippage_bps=5.0,
            gas_used=150000,
            gas_usd="0.50",
            tx_hash="0xabc",
            chain="arbitrum",
            protocol="uniswap_v3",
            success=True,
        )

        await sqlite_store.save_ledger_entry(entry)
        entries = await sqlite_store.get_ledger_entries("strat-1")

        assert len(entries) == 1
        assert entries[0].id == "entry-1"
        assert entries[0].token_in == "USDC"
        assert entries[0].token_out == "ETH"
        assert entries[0].effective_price == "2000"
        assert entries[0].success is True

    async def test_query_with_intent_type_filter(self, sqlite_store):
        for i, intent_type in enumerate(["SWAP", "SUPPLY", "SWAP"]):
            await sqlite_store.save_ledger_entry(
                LedgerEntry(
                    id=f"entry-{i}",
                    cycle_id=f"cycle-{i}",
                    strategy_id="strat-1",
                    intent_type=intent_type,
                )
            )

        swaps = await sqlite_store.get_ledger_entries("strat-1", intent_type="SWAP")
        assert len(swaps) == 2

        supplies = await sqlite_store.get_ledger_entries("strat-1", intent_type="SUPPLY")
        assert len(supplies) == 1

    async def test_query_with_since_filter(self, sqlite_store):
        old = LedgerEntry(
            id="old",
            strategy_id="strat-1",
            timestamp=datetime(2026, 4, 1, tzinfo=UTC),
            intent_type="SWAP",
        )
        new = LedgerEntry(
            id="new",
            strategy_id="strat-1",
            timestamp=datetime(2026, 4, 5, tzinfo=UTC),
            intent_type="SWAP",
        )
        await sqlite_store.save_ledger_entry(old)
        await sqlite_store.save_ledger_entry(new)

        since = datetime(2026, 4, 3, tzinfo=UTC)
        entries = await sqlite_store.get_ledger_entries("strat-1", since=since)
        assert len(entries) == 1
        assert entries[0].id == "new"

    async def test_query_respects_limit(self, sqlite_store):
        for i in range(10):
            await sqlite_store.save_ledger_entry(
                LedgerEntry(id=f"e-{i}", strategy_id="strat-1", intent_type="SWAP")
            )

        entries = await sqlite_store.get_ledger_entries("strat-1", limit=3)
        assert len(entries) == 3

    async def test_query_different_strategy_ids_isolated(self, sqlite_store):
        await sqlite_store.save_ledger_entry(
            LedgerEntry(id="a", strategy_id="strat-a", intent_type="SWAP")
        )
        await sqlite_store.save_ledger_entry(
            LedgerEntry(id="b", strategy_id="strat-b", intent_type="BORROW")
        )

        a_entries = await sqlite_store.get_ledger_entries("strat-a")
        b_entries = await sqlite_store.get_ledger_entries("strat-b")
        assert len(a_entries) == 1
        assert len(b_entries) == 1
        assert a_entries[0].id == "a"
        assert b_entries[0].id == "b"
