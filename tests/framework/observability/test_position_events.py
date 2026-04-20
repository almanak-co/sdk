"""Tests for position lifecycle events (Phase 2, VIB-2774/2775).

Validates:
- PositionEvent creation from LP/perp intents
- SWAP/SUPPLY intents produce no position events
- SQLite persistence and querying
- Position history (chronological lifecycle)
"""

import asyncio
from datetime import UTC, datetime

import pytest

from almanak.framework.observability.position_events import (
    INTENT_TO_EVENT_TYPE,
    PositionEvent,
    PositionEventType,
    PositionType,
    build_position_event_from_intent,
)
from almanak.framework.state.backends.sqlite import SQLiteConfig, SQLiteStore


# --- Mock intent/result helpers ---


class MockIntent:
    def __init__(self, intent_type: str, protocol: str = "uniswap_v3", position_id: str = ""):
        self.intent_type = type("IT", (), {"value": intent_type})()
        self.protocol = protocol
        self.position_id = position_id


class MockTxResult:
    def __init__(self, tx_hash: str = "0xabc"):
        self.tx_hash = tx_hash
        self.gas_used = 200000
        self.success = True


class MockResult:
    def __init__(self, position_id: str = "", tx_hash: str = "0xabc"):
        self.position_id = position_id
        self.transaction_results = [MockTxResult(tx_hash)]
        self.gas_cost_usd = "2.50"
        self.extracted_data = {}


class TestBuildPositionEvent:
    """Test building position events from intents."""

    def test_lp_open_produces_open_event(self):
        intent = MockIntent("LP_OPEN")
        result = MockResult(position_id="12345")
        event = build_position_event_from_intent(
            deployment_id="strat:abc",
            intent=intent,
            result=result,
            chain="arbitrum",
        )
        assert event is not None
        assert event.event_type == "OPEN"
        assert event.position_type == "LP"
        assert event.position_id == "12345"
        assert event.deployment_id == "strat:abc"
        assert event.chain == "arbitrum"

    def test_lp_close_produces_close_event(self):
        intent = MockIntent("LP_CLOSE", position_id="12345")
        result = MockResult()
        event = build_position_event_from_intent(
            deployment_id="strat:abc",
            intent=intent,
            result=result,
        )
        assert event is not None
        assert event.event_type == "CLOSE"
        assert event.position_type == "LP"

    def test_perp_open_produces_open_event(self):
        intent = MockIntent("PERP_OPEN", protocol="gmx_v2")
        result = MockResult(position_id="perp-001")
        event = build_position_event_from_intent(
            deployment_id="strat:abc",
            intent=intent,
            result=result,
        )
        assert event is not None
        assert event.event_type == "OPEN"
        assert event.position_type == "PERP"
        assert event.protocol == "gmx_v2"

    def test_swap_produces_no_event(self):
        intent = MockIntent("SWAP")
        result = MockResult()
        event = build_position_event_from_intent(
            deployment_id="strat:abc",
            intent=intent,
            result=result,
        )
        assert event is None

    def test_supply_produces_no_event(self):
        intent = MockIntent("SUPPLY")
        result = MockResult()
        event = build_position_event_from_intent(
            deployment_id="strat:abc",
            intent=intent,
            result=result,
        )
        assert event is None

    def test_borrow_produces_no_event(self):
        intent = MockIntent("BORROW")
        event = build_position_event_from_intent(
            deployment_id="strat:abc",
            intent=intent,
            result=MockResult(),
        )
        assert event is None

    def test_no_event_when_position_id_empty(self):
        """LP_OPEN with no position_id resolved returns None (guard)."""
        intent = MockIntent("LP_OPEN")
        result = MockResult(position_id="")  # No position_id resolved
        event = build_position_event_from_intent(
            deployment_id="strat:abc",
            intent=intent,
            result=result,
        )
        assert event is None

    def test_tx_hash_and_gas_captured(self):
        intent = MockIntent("LP_OPEN")
        result = MockResult(position_id="12345", tx_hash="0xdeadbeef")
        event = build_position_event_from_intent(
            deployment_id="strat:abc",
            intent=intent,
            result=result,
        )
        assert event is not None
        assert event.tx_hash == "0xdeadbeef"
        assert event.gas_usd == "2.50"


class TestIntentToEventMapping:
    """Verify the intent->event mapping covers LP and perps only."""

    def test_lp_intents_mapped(self):
        assert "LP_OPEN" in INTENT_TO_EVENT_TYPE
        assert "LP_CLOSE" in INTENT_TO_EVENT_TYPE
        assert "LP_COLLECT_FEES" in INTENT_TO_EVENT_TYPE

    def test_perp_intents_mapped(self):
        assert "PERP_OPEN" in INTENT_TO_EVENT_TYPE
        assert "PERP_CLOSE" in INTENT_TO_EVENT_TYPE

    def test_fungible_intents_not_mapped(self):
        for intent_type in ("SWAP", "SUPPLY", "WITHDRAW", "BORROW", "REPAY", "STAKE", "UNSTAKE", "HOLD"):
            assert intent_type not in INTENT_TO_EVENT_TYPE


# --- SQLite persistence tests ---


@pytest.fixture
def store(tmp_path):
    db_path = str(tmp_path / "test.db")
    config = SQLiteConfig(db_path=db_path)
    s = SQLiteStore(config)
    asyncio.get_event_loop().run_until_complete(s.initialize())
    yield s
    asyncio.get_event_loop().run_until_complete(s.close())


class TestPositionEventPersistence:
    """Test save and query of position events in SQLite."""

    def test_save_and_retrieve(self, store):
        event = PositionEvent(
            deployment_id="strat:abc",
            position_id="12345",
            position_type="LP",
            event_type="OPEN",
            protocol="uniswap_v3",
            chain="arbitrum",
            tick_lower=-1000,
            tick_upper=1000,
        )
        ok = asyncio.get_event_loop().run_until_complete(store.save_position_event(event))
        assert ok

        events = asyncio.get_event_loop().run_until_complete(
            store.get_position_events("strat:abc")
        )
        assert len(events) == 1
        assert events[0]["position_id"] == "12345"
        assert events[0]["event_type"] == "OPEN"
        assert events[0]["tick_lower"] == -1000

    def test_filter_by_position_id(self, store):
        for pid in ("100", "200"):
            event = PositionEvent(
                deployment_id="strat:abc",
                position_id=pid,
                position_type="LP",
                event_type="OPEN",
            )
            asyncio.get_event_loop().run_until_complete(store.save_position_event(event))

        events = asyncio.get_event_loop().run_until_complete(
            store.get_position_events("strat:abc", position_id="100")
        )
        assert len(events) == 1
        assert events[0]["position_id"] == "100"

    def test_filter_by_event_type(self, store):
        for etype in ("OPEN", "SNAPSHOT", "CLOSE"):
            event = PositionEvent(
                deployment_id="strat:abc",
                position_id="100",
                position_type="LP",
                event_type=etype,
            )
            asyncio.get_event_loop().run_until_complete(store.save_position_event(event))

        events = asyncio.get_event_loop().run_until_complete(
            store.get_position_events("strat:abc", event_type="SNAPSHOT")
        )
        assert len(events) == 1

    def test_position_history_chronological(self, store):
        for i, etype in enumerate(["OPEN", "SNAPSHOT", "CLOSE"]):
            event = PositionEvent(
                deployment_id="strat:abc",
                position_id="100",
                position_type="LP",
                event_type=etype,
                timestamp=datetime(2026, 1, 1 + i, tzinfo=UTC),
            )
            asyncio.get_event_loop().run_until_complete(store.save_position_event(event))

        history = asyncio.get_event_loop().run_until_complete(
            store.get_position_history("strat:abc", "100")
        )
        assert len(history) == 3
        assert history[0]["event_type"] == "OPEN"
        assert history[1]["event_type"] == "SNAPSHOT"
        assert history[2]["event_type"] == "CLOSE"
