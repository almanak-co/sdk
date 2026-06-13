"""VIB-5057 — accounting-event replay must be deterministic on identical timestamps.

The swap-inventory classifier replays accounting events through FIFOBasisStore,
which assumes a BUY lot precedes the SELL that consumes it. Two events that
share an identical ISO timestamp string must therefore replay in insertion
order, not in whatever order the SQLite scan happens to return — otherwise a
SELL could match before its BUY and skew ``remaining`` / cost. The
``get_accounting_events_sync`` query pins this with an ``ORDER BY timestamp ASC,
rowid ASC`` tiebreak; this test fails if the tiebreak is dropped.
"""

from __future__ import annotations

import os
import tempfile

import pytest
import pytest_asyncio

from almanak.framework.state.backends.sqlite import SQLiteConfig, SQLiteStore

_DEP = "FifoOrderStrat:abc123"
_TS = "2026-06-01T00:00:00+00:00"  # identical across all rows on purpose


def _insert_event(store: SQLiteStore, event_id: str, event_type: str) -> None:
    store._conn.execute(  # type: ignore[union-attr]
        """
        INSERT INTO accounting_events (
            id, deployment_id, cycle_id, execution_mode, timestamp,
            chain, protocol, wallet_address, event_type, position_key,
            confidence, payload_json
        ) VALUES (?, ?, 'cycle-1', 'paper', ?, 'arbitrum', 'uniswap_v3',
                  '0xwallet', ?, 'swap:arbitrum:0xwallet', 'HIGH', '{}')
        """,
        (event_id, _DEP, _TS, event_type),
    )
    store._conn.commit()  # type: ignore[union-attr]


@pytest_asyncio.fixture
async def store():
    with tempfile.TemporaryDirectory() as tmpdir:
        s = SQLiteStore(SQLiteConfig(db_path=os.path.join(tmpdir, "fifo.db")))
        await s.initialize()
        try:
            yield s
        finally:
            await s.close()


@pytest.mark.asyncio
async def test_identical_timestamp_events_replay_in_insertion_order(store: SQLiteStore):
    # Insert BUY then SELL with the SAME timestamp; UUID-like ids sort the
    # OPPOSITE way to insertion order so a naive id-only or scan-order result
    # would surface the SELL first.
    _insert_event(store, "ffff-buy", "SWAP_BUY")
    _insert_event(store, "0000-sell", "SWAP_SELL")

    events = store.get_accounting_events_sync(_DEP)

    assert [e["event_type"] for e in events] == ["SWAP_BUY", "SWAP_SELL"], (
        "identical-timestamp events must replay in rowid (insertion) order so "
        "FIFO lot matching sees BUY before SELL"
    )
