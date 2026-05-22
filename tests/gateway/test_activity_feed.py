"""Tests for `DashboardService.GetActivityFeed` (VIB-4042 / PR3).

The activity feed is the dashboard's render path — gateway-owned merge of
`timeline_events` (UX cards) and `transaction_ledger` (financial truth).
The compositor contract this file verifies:

  1. Both streams are present in timestamp-DESC order.
  2. A TIMELINE_EVENT whose `related_ledger_entry_id` references a
     LEDGER_ENTRY in the same window is dropped (truth wins, dedup-by-id).
  3. `before_timestamp` cursor pagination is honest:
       - Items returned are strictly older than the cursor.
       - `has_more` is True only when the merged stream actually has more.
       - `next_before_timestamp` is the timestamp of the last returned item.
  4. The page never exceeds the requested limit, and the server cap (200)
     applies even when the request asks for more.
  5. Validation rejects bad deployment_ids before any backend hit.
  6. Default kind / unknown payloads degrade gracefully on the client side.

Failure of any of these would mean the dashboard either drifts from the
ledger or papers over the producer-side guardrail PR4 enforces — which is
exactly what PRD-TimelineEvents §6.1 / §9 forbids.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import grpc
import pytest

from almanak.gateway.core.settings import GatewaySettings
from almanak.gateway.proto import gateway_pb2
from almanak.gateway.services.dashboard_service import DashboardServiceServicer
from almanak.gateway.timeline.store import TimelineEvent, get_timeline_store, reset_timeline_store


@dataclass
class _LedgerRow:
    """Minimal stand-in for the StateManager ledger row used by the compositor."""

    id: str
    cycle_id: str
    deployment_id: str
    timestamp: datetime
    intent_type: str = "SWAP"
    token_in: str = "USDC"
    amount_in: str = "100"
    token_out: str = "USDT"
    amount_out: str = "99"
    effective_price: str = "0.99"
    slippage_bps: float = 0.0
    gas_used: int = 100_000
    gas_usd: str = "0.50"
    tx_hash: str = "0xabc"
    chain: str = "arbitrum"
    protocol: str = "uniswap_v3"
    success: bool = True
    error: str = ""


@pytest.fixture(autouse=True)
def _reset_timeline_store():
    reset_timeline_store()
    yield
    reset_timeline_store()


@pytest.fixture
def dashboard_service():
    settings = GatewaySettings()
    service = DashboardServiceServicer(settings)
    service._initialized = True
    service._state_manager = MagicMock()
    service._state_manager.get_ledger_entries = AsyncMock(return_value=[])
    return service


@pytest.fixture
def mock_context():
    return MagicMock(spec=grpc.aio.ServicerContext)


def _add_timeline(deployment_id: str, ts: datetime, **kwargs) -> TimelineEvent:
    event = TimelineEvent(
        event_id=str(uuid4()),
        deployment_id=deployment_id,
        timestamp=ts,
        event_type=kwargs.pop("event_type", "STATE_CHANGE"),
        description=kwargs.pop("description", "test"),
        cycle_id=kwargs.pop("cycle_id", ""),
        phase=kwargs.pop("phase", ""),
        related_ledger_entry_id=kwargs.pop("related_ledger_entry_id", ""),
        **kwargs,
    )
    get_timeline_store().add_event(event)
    return event


class TestMergeAndOrder:
    @pytest.mark.asyncio
    async def test_merges_timeline_and_ledger_in_descending_order(self, dashboard_service, mock_context):
        sid = "test_strategy"
        now = datetime.now(UTC)

        # Three events spanning 4 minutes
        _add_timeline(sid, now - timedelta(minutes=1), description="newest UX")
        _add_timeline(sid, now - timedelta(minutes=3), description="oldest UX")

        ledger = [
            _LedgerRow(id="lg-1", cycle_id="cyc-1", deployment_id=sid, timestamp=now - timedelta(minutes=2)),
        ]
        dashboard_service._state_manager.get_ledger_entries = AsyncMock(return_value=ledger)

        response = await dashboard_service.GetActivityFeed(
            gateway_pb2.GetActivityFeedRequest(deployment_id=sid, limit=10),
            mock_context,
        )

        assert len(response.items) == 3
        # Strict timestamp-DESC ordering across the merged stream.
        timestamps = [i.timestamp for i in response.items]
        assert timestamps == sorted(timestamps, reverse=True)

        kinds = [i.kind for i in response.items]
        assert gateway_pb2.ActivityFeedItem.Kind.TIMELINE_EVENT in kinds
        assert gateway_pb2.ActivityFeedItem.Kind.LEDGER_ENTRY in kinds

    @pytest.mark.asyncio
    async def test_timeline_event_with_related_ledger_in_window_is_deduped(self, dashboard_service, mock_context):
        sid = "test_strategy"
        now = datetime.now(UTC)

        # The ledger row IS the truth.
        ledger = [_LedgerRow(id="lg-7", cycle_id="cyc-7", deployment_id=sid, timestamp=now)]
        dashboard_service._state_manager.get_ledger_entries = AsyncMock(return_value=ledger)

        # Timeline duplicate references that ledger row → must be suppressed.
        _add_timeline(
            sid,
            now,
            event_type="POSITION_OPENED",
            description="LP_OPEN landed",
            related_ledger_entry_id="lg-7",
        )
        # A pure UX event — not transaction-derived — must NOT be dropped.
        _add_timeline(
            sid,
            now - timedelta(seconds=30),
            event_type="STATE_CHANGE",
            description="decide() started",
        )

        response = await dashboard_service.GetActivityFeed(
            gateway_pb2.GetActivityFeedRequest(deployment_id=sid, limit=10),
            mock_context,
        )

        kinds = [i.kind for i in response.items]
        # Exactly: 1 ledger row + 1 STATE_CHANGE; the dup POSITION_OPENED is dropped.
        assert kinds.count(gateway_pb2.ActivityFeedItem.Kind.TIMELINE_EVENT) == 1
        assert kinds.count(gateway_pb2.ActivityFeedItem.Kind.LEDGER_ENTRY) == 1

        timeline_items = [i for i in response.items if i.kind == gateway_pb2.ActivityFeedItem.Kind.TIMELINE_EVENT]
        assert timeline_items[0].timeline_event.event_type == "STATE_CHANGE"

    @pytest.mark.asyncio
    async def test_timeline_event_without_related_ledger_is_kept(self, dashboard_service, mock_context):
        sid = "test_strategy"
        now = datetime.now(UTC)
        # related_ledger_entry_id pointing at a row NOT in this window — keep the event.
        _add_timeline(sid, now, related_ledger_entry_id="lg-not-in-window")
        dashboard_service._state_manager.get_ledger_entries = AsyncMock(return_value=[])

        response = await dashboard_service.GetActivityFeed(
            gateway_pb2.GetActivityFeedRequest(deployment_id=sid, limit=10),
            mock_context,
        )
        assert len(response.items) == 1
        assert response.items[0].kind == gateway_pb2.ActivityFeedItem.Kind.TIMELINE_EVENT


class TestPagination:
    @pytest.mark.asyncio
    async def test_before_timestamp_filters_strictly_older(self, dashboard_service, mock_context):
        sid = "test_strategy"
        now = datetime.now(UTC)

        for i in range(5):
            _add_timeline(sid, now - timedelta(minutes=i), description=f"e{i}")

        # Cursor at minute -2 → only items strictly older (minutes -3, -4 from now)
        cursor_dt = now - timedelta(minutes=2)

        response = await dashboard_service.GetActivityFeed(
            gateway_pb2.GetActivityFeedRequest(
                deployment_id=sid,
                limit=10,
                before_timestamp=int(cursor_dt.timestamp()),
            ),
            mock_context,
        )

        cursor_ts = int(cursor_dt.timestamp())
        for item in response.items:
            assert item.timestamp < cursor_ts

    @pytest.mark.asyncio
    async def test_has_more_and_next_cursor_when_overflow(self, dashboard_service, mock_context):
        sid = "test_strategy"
        now = datetime.now(UTC)
        for i in range(8):
            _add_timeline(sid, now - timedelta(seconds=i), description=f"e{i}")

        response = await dashboard_service.GetActivityFeed(
            gateway_pb2.GetActivityFeedRequest(deployment_id=sid, limit=3),
            mock_context,
        )

        assert len(response.items) == 3
        assert response.has_more is True
        # The next cursor must equal the timestamp of the last returned item
        # so the next call resumes strictly older.
        assert response.next_before_timestamp == response.items[-1].timestamp

    @pytest.mark.asyncio
    async def test_no_more_emits_zero_cursor(self, dashboard_service, mock_context):
        sid = "test_strategy"
        now = datetime.now(UTC)
        _add_timeline(sid, now)

        response = await dashboard_service.GetActivityFeed(
            gateway_pb2.GetActivityFeedRequest(deployment_id=sid, limit=10),
            mock_context,
        )

        assert response.has_more is False
        assert response.next_before_timestamp == 0

    @pytest.mark.asyncio
    async def test_server_cap_overrides_oversized_request(self, dashboard_service, mock_context):
        sid = "test_strategy"
        now = datetime.now(UTC)
        # Request 9999 — cap is 200; response must NOT exceed that even
        # if the underlying stream is huge.
        for i in range(250):
            _add_timeline(sid, now - timedelta(seconds=i), description=f"e{i}")

        response = await dashboard_service.GetActivityFeed(
            gateway_pb2.GetActivityFeedRequest(deployment_id=sid, limit=9999),
            mock_context,
        )
        assert len(response.items) <= 200


class TestPushdownAndCursor:
    """Gemini review (PR #2116):

    1. ``before_timestamp`` MUST be pushed into each backend, not post-fetched
       in the compositor. If the compositor receives the most recent ``limit+1``
       items and post-filters by the cursor, a paginated caller can hit an
       empty page even when the database still has many older items.

    2. The cursor MUST include a tie-breaker so two items at the same
       boundary timestamp are paginated deterministically — neither dropped
       nor returned twice.
    """

    @pytest.mark.asyncio
    async def test_before_pushed_into_ledger_query(self, dashboard_service, mock_context):
        """The ``before`` cursor must hit the ledger's SQL filter."""
        sid = "test_strategy"
        now = datetime.now(UTC)
        cursor_dt = now - timedelta(minutes=5)

        spy = AsyncMock(return_value=[])
        dashboard_service._state_manager.get_ledger_entries = spy

        await dashboard_service.GetActivityFeed(
            gateway_pb2.GetActivityFeedRequest(
                deployment_id=sid,
                limit=10,
                before_timestamp=int(cursor_dt.timestamp()),
            ),
            mock_context,
        )

        spy.assert_awaited_once()
        kwargs = spy.await_args.kwargs
        assert kwargs.get("before") is not None, "before must be pushed down to the ledger query"
        assert int(kwargs["before"].timestamp()) == int(cursor_dt.timestamp())

    @pytest.mark.asyncio
    async def test_before_pushed_into_timeline_store(self, dashboard_service, mock_context, monkeypatch):
        """The ``before`` cursor must hit ``TimelineStore.get_events``, not be post-filtered."""
        sid = "test_strategy"
        now = datetime.now(UTC)
        cursor_dt = now - timedelta(minutes=5)

        captured: dict = {}

        from almanak.gateway.timeline import store as store_mod

        original = store_mod.TimelineStore.get_events

        def _spy(self, **kwargs):
            captured["before"] = kwargs.get("before")
            return original(self, **kwargs)

        monkeypatch.setattr(store_mod.TimelineStore, "get_events", _spy)

        await dashboard_service.GetActivityFeed(
            gateway_pb2.GetActivityFeedRequest(
                deployment_id=sid,
                limit=10,
                before_timestamp=int(cursor_dt.timestamp()),
            ),
            mock_context,
        )

        assert captured.get("before") is not None
        assert int(captured["before"].timestamp()) == int(cursor_dt.timestamp())

    @pytest.mark.asyncio
    async def test_pagination_does_not_lose_items_with_dense_recent_activity(self, dashboard_service, mock_context):
        """Reproduces Gemini's high-priority concern.

        20 events, 10 of them clustered at ``now`` (newer than the cursor)
        and 10 spread between cursor-1m and cursor-30m (older than the
        cursor). With a limit of 5 + a cursor at ``now - 5m``, the 10 newest
        events would saturate any over-fetch buffer. If ``before`` is post-
        filtered (the bug), the page comes back empty. Push-down must yield
        exactly the 5 most-recent items strictly older than the cursor.
        """
        sid = "test_strategy"
        now = datetime.now(UTC)
        # 10 newer-than-cursor events
        for i in range(10):
            _add_timeline(sid, now - timedelta(seconds=i), description=f"new{i}")
        # 10 older-than-cursor events
        for i in range(10):
            _add_timeline(sid, now - timedelta(minutes=10 + i), description=f"old{i}")

        cursor_dt = now - timedelta(minutes=5)

        response = await dashboard_service.GetActivityFeed(
            gateway_pb2.GetActivityFeedRequest(
                deployment_id=sid,
                limit=5,
                before_timestamp=int(cursor_dt.timestamp()),
            ),
            mock_context,
        )

        # Without push-down this returns 0; with push-down it returns 5.
        assert len(response.items) == 5
        for item in response.items:
            assert item.timestamp < int(cursor_dt.timestamp())

    @pytest.mark.asyncio
    async def test_tie_breaker_cursor_does_not_skip_or_duplicate(self, dashboard_service, mock_context):
        """Three timeline events share the SAME timestamp.

        Page 1: limit=2 — must return 2 of them and emit a tie-breaker cursor.
        Page 2: limit=2 with the previous cursor — must return the third item
        only, never duplicating page-1 items, never skipping.
        """
        sid = "test_strategy"
        now = datetime.now(UTC)
        for i in range(3):
            _add_timeline(sid, now, description=f"tied{i}")

        # Page 1
        page1 = await dashboard_service.GetActivityFeed(
            gateway_pb2.GetActivityFeedRequest(deployment_id=sid, limit=2),
            mock_context,
        )
        assert len(page1.items) == 2
        assert page1.has_more is True
        assert page1.next_before_timestamp == int(now.timestamp())
        assert page1.next_before_id != ""

        # Page 2 with the tie-breaker cursor — must return the third item.
        page2 = await dashboard_service.GetActivityFeed(
            gateway_pb2.GetActivityFeedRequest(
                deployment_id=sid,
                limit=2,
                before_timestamp=page1.next_before_timestamp,
                before_id=page1.next_before_id,
            ),
            mock_context,
        )

        page1_descs = {i.timeline_event.description for i in page1.items}
        page2_descs = {i.timeline_event.description for i in page2.items}
        assert len(page2_descs) == 1, "page 2 must return exactly the remaining tied item"
        assert page1_descs.isdisjoint(page2_descs), "no item may appear on both pages"
        assert page1_descs | page2_descs == {"tied0", "tied1", "tied2"}, "no item may be skipped"

    @pytest.mark.asyncio
    async def test_response_emits_next_before_id_when_has_more(self, dashboard_service, mock_context):
        sid = "test_strategy"
        now = datetime.now(UTC)
        for i in range(5):
            _add_timeline(sid, now - timedelta(seconds=i), description=f"e{i}")

        response = await dashboard_service.GetActivityFeed(
            gateway_pb2.GetActivityFeedRequest(deployment_id=sid, limit=2),
            mock_context,
        )
        assert response.has_more is True
        assert response.next_before_id != ""
        # The cursor key encodes "<priority>:<kind>:<id>". A timeline-only
        # stream must produce a TIMELINE-priority cursor.
        assert response.next_before_id.startswith("0:T:")

    @pytest.mark.asyncio
    async def test_response_clears_next_before_id_when_no_more(self, dashboard_service, mock_context):
        sid = "test_strategy"
        now = datetime.now(UTC)
        _add_timeline(sid, now)

        response = await dashboard_service.GetActivityFeed(
            gateway_pb2.GetActivityFeedRequest(deployment_id=sid, limit=10),
            mock_context,
        )
        assert response.has_more is False
        assert response.next_before_id == ""

    @pytest.mark.asyncio
    async def test_ledger_backend_without_before_kwarg_falls_back_to_post_filter(self, dashboard_service, mock_context):
        """A backend that doesn't accept ``before=`` (test mock or older SDK)
        must NOT crash the compositor. ``_load_ledger_fallback_no_before``
        catches the TypeError, retries without the kwarg, and post-filters in
        Python so the cursor still produces strictly-older items.
        """
        sid = "test_strategy"
        now = datetime.now(UTC)
        cursor_dt = now - timedelta(minutes=5)

        all_entries = [
            _LedgerRow(id=f"lg-{i}", cycle_id=f"cyc-{i}", deployment_id=sid, timestamp=now - timedelta(minutes=i))
            for i in range(10)
        ]

        call_count = {"n": 0}

        async def picky_backend(*args, **kwargs):
            call_count["n"] += 1
            if "before" in kwargs:
                # Simulate an older backend that doesn't know about `before`.
                raise TypeError("get_ledger_entries() got an unexpected keyword argument 'before'")
            return all_entries

        dashboard_service._state_manager.get_ledger_entries = picky_backend

        response = await dashboard_service.GetActivityFeed(
            gateway_pb2.GetActivityFeedRequest(
                deployment_id=sid,
                limit=10,
                before_timestamp=int(cursor_dt.timestamp()),
            ),
            mock_context,
        )

        # The compositor retried (so 2 calls), then post-filtered strictly.
        assert call_count["n"] == 2
        cursor_ts = int(cursor_dt.timestamp())
        for item in response.items:
            assert item.timestamp < cursor_ts


class TestValidation:
    @pytest.mark.asyncio
    async def test_invalid_deployment_id_rejected(self, dashboard_service, mock_context):
        response = await dashboard_service.GetActivityFeed(
            gateway_pb2.GetActivityFeedRequest(deployment_id="", limit=10),
            mock_context,
        )
        # Empty response, INVALID_ARGUMENT set on context
        assert len(response.items) == 0
        mock_context.set_code.assert_called_with(grpc.StatusCode.INVALID_ARGUMENT)

    @pytest.mark.asyncio
    async def test_before_id_without_before_timestamp_rejected(self, dashboard_service, mock_context):
        """CR review: composite cursor must be validated as a pair."""
        response = await dashboard_service.GetActivityFeed(
            gateway_pb2.GetActivityFeedRequest(deployment_id="s", limit=10, before_id="0:T:abc"),
            mock_context,
        )
        assert len(response.items) == 0
        mock_context.set_code.assert_called_with(grpc.StatusCode.INVALID_ARGUMENT)

    @pytest.mark.asyncio
    async def test_malformed_before_id_shape_rejected(self, dashboard_service, mock_context):
        bad_cursors = [
            "T:abc",  # missing priority
            "0:T",  # missing item_id
            "abc",  # no colons
            "2:T:abc",  # invalid priority
            "0:X:abc",  # invalid kind
            "1:T:abc",  # priority/kind mismatch (priority 1 = LEDGER, kind T = TIMELINE)
            "0:L:abc",  # priority/kind mismatch (priority 0 = TIMELINE, kind L = LEDGER)
        ]
        for bad in bad_cursors:
            mock_context.reset_mock()
            response = await dashboard_service.GetActivityFeed(
                gateway_pb2.GetActivityFeedRequest(
                    deployment_id="s", limit=10, before_timestamp=1700000000, before_id=bad
                ),
                mock_context,
            )
            assert len(response.items) == 0, f"cursor {bad!r} should be rejected"
            mock_context.set_code.assert_called_with(grpc.StatusCode.INVALID_ARGUMENT), bad

    @pytest.mark.asyncio
    async def test_well_formed_before_id_accepted(self, dashboard_service, mock_context):
        """A valid composite cursor must NOT trip the validator."""
        sid = "test_strategy"
        await dashboard_service.GetActivityFeed(
            gateway_pb2.GetActivityFeedRequest(
                deployment_id=sid,
                limit=10,
                before_timestamp=1700000000,
                before_id="1:L:lg-7",
            ),
            mock_context,
        )
        # Empty result is fine — what matters is no INVALID_ARGUMENT was raised.
        for call in mock_context.set_code.call_args_list:
            assert call.args[0] != grpc.StatusCode.INVALID_ARGUMENT

    @pytest.mark.asyncio
    async def test_overflow_before_timestamp_rejected_with_invalid_argument(self, dashboard_service, mock_context):
        """CodeRabbit: an out-of-range int64 ``before_timestamp`` must surface as
        INVALID_ARGUMENT, not INTERNAL.

        Without the boundary guard, a value past ``datetime``'s upper bound
        raises ``OverflowError`` from ``datetime.fromtimestamp`` and the gRPC
        layer would translate that to INTERNAL — leaking implementation
        details and giving the caller no actionable signal.
        """
        # Max int64; well past datetime year 9999 (≈ 253402300799).
        overflow_ts = 9223372036854775807
        response = await dashboard_service.GetActivityFeed(
            gateway_pb2.GetActivityFeedRequest(deployment_id="s", limit=10, before_timestamp=overflow_ts),
            mock_context,
        )
        assert len(response.items) == 0
        mock_context.set_code.assert_called_with(grpc.StatusCode.INVALID_ARGUMENT)

    @pytest.mark.asyncio
    async def test_overflow_before_timestamp_with_tie_breaker_also_rejected(self, dashboard_service, mock_context):
        """The same overflow must be rejected when a tie-breaker is present."""
        response = await dashboard_service.GetActivityFeed(
            gateway_pb2.GetActivityFeedRequest(
                deployment_id="s",
                limit=10,
                before_timestamp=9223372036854775807,
                before_id="0:T:evt-7",
            ),
            mock_context,
        )
        assert len(response.items) == 0
        mock_context.set_code.assert_called_with(grpc.StatusCode.INVALID_ARGUMENT)


class TestFieldPropagation:
    @pytest.mark.asyncio
    async def test_timeline_fields_round_trip_through_feed(self, dashboard_service, mock_context):
        sid = "test_strategy"
        now = datetime.now(UTC)
        _add_timeline(
            sid,
            now,
            event_type="STATE_CHANGE",
            description="d",
            cycle_id="cyc-99",
            phase="EXECUTE",
            related_ledger_entry_id="",
        )
        response = await dashboard_service.GetActivityFeed(
            gateway_pb2.GetActivityFeedRequest(deployment_id=sid, limit=10),
            mock_context,
        )
        item = response.items[0]
        assert item.cycle_id == "cyc-99"
        assert item.timeline_event.cycle_id == "cyc-99"
        assert item.timeline_event.phase == "EXECUTE"

    @pytest.mark.asyncio
    async def test_ledger_fields_round_trip_through_feed(self, dashboard_service, mock_context):
        sid = "test_strategy"
        now = datetime.now(UTC)
        ledger = [
            _LedgerRow(
                id="lg-1",
                cycle_id="cyc-1",
                deployment_id=sid,
                timestamp=now,
                intent_type="SWAP",
                token_in="USDC",
                amount_in="500",
                token_out="USDT",
                amount_out="499",
                effective_price="0.998",
                gas_usd="0.42",
            )
        ]
        dashboard_service._state_manager.get_ledger_entries = AsyncMock(return_value=ledger)

        response = await dashboard_service.GetActivityFeed(
            gateway_pb2.GetActivityFeedRequest(deployment_id=sid, limit=10),
            mock_context,
        )
        item = response.items[0]
        assert item.kind == gateway_pb2.ActivityFeedItem.Kind.LEDGER_ENTRY
        entry = item.ledger_entry
        assert entry.id == "lg-1"
        assert entry.cycle_id == "cyc-1"
        assert entry.intent_type == "SWAP"
        assert entry.amount_in == "500"
        assert entry.gas_usd == "0.42"


class TestIncrementalDedup:
    """CodeRabbit review on PR #2116: a ledger row in the over-fetch tail
    must NOT suppress its timeline duplicate from the current page.

    Earlier code built the dedup set from ALL fetched ledger entries
    (including limit+1 over-fetch tail), then dropped any timeline event
    referencing one of those ids. If the matching ledger row sorted into
    the over-fetch tail (position > limit), the user would see neither the
    timeline event NOR the ledger row on the current page — silent gap.

    The fix: defer dedup to a page-incremental walk. A ledger row is only
    "seen" when it actually lands on the page, so a tail-position ledger
    cannot suppress an earlier timeline event.
    """

    @pytest.mark.asyncio
    async def test_ledger_in_over_fetch_tail_does_not_suppress_timeline(self, dashboard_service, mock_context):
        sid = "test_strategy"
        now = datetime.now(UTC)

        # Build a candidate stream where the ledger row referenced by an
        # early timeline event sits BEYOND the page boundary.
        # Limit will be 2, so we craft 5 timeline events at minute 0..4 and
        # 1 ledger row at minute 6 (older than all timelines, in over-fetch).
        # The earliest timeline ref'ing "lg-tail" was previously deduped
        # because the dedup set included the over-fetch tail.
        for i in range(5):
            _add_timeline(
                sid,
                now - timedelta(minutes=i),
                description=f"e{i}",
                related_ledger_entry_id="lg-tail" if i == 0 else "",
            )
        ledger = [
            _LedgerRow(id="lg-tail", cycle_id="cyc-tail", deployment_id=sid, timestamp=now - timedelta(minutes=6)),
        ]
        dashboard_service._state_manager.get_ledger_entries = AsyncMock(return_value=ledger)

        response = await dashboard_service.GetActivityFeed(
            gateway_pb2.GetActivityFeedRequest(deployment_id=sid, limit=2),
            mock_context,
        )

        # The earliest timeline (referencing lg-tail) must NOT be deduped:
        # lg-tail does not appear on this page (it's older than all 5
        # timelines). The page should be the 2 newest timelines: e0, e1.
        assert len(response.items) == 2
        descriptions = [i.timeline_event.description for i in response.items]
        assert descriptions == ["e0", "e1"], f"got: {descriptions}"

    @pytest.mark.asyncio
    async def test_dedup_fires_when_timeline_sorts_before_its_ledger(self, dashboard_service, mock_context):
        """CodeRabbit on PR #2117: dedup must NOT depend on the ledger
        sorting before the timeline.

        In production the runner emits a ``TimelineEvent`` AFTER the ledger
        write returns its id, so the timeline's ``datetime.now()`` is a
        tick newer than the ledger row's. When that 1-second skew lands at
        a wall-clock second boundary, the timeline sorts FIRST in DESC order
        and the ledger sorts second. The earlier single-pass walk would
        emit both rows. The two-phase fix replaces the pending timeline
        with the ledger when the ledger arrives later in the walk.
        """
        sid = "test_strategy"
        # Anchor at a fixed timestamp so the test is stable across re-runs.
        anchor = datetime(2026, 5, 1, 12, 0, 0, tzinfo=UTC)
        # Ledger row at t=0.
        ledger = [_LedgerRow(id="lg-7", cycle_id="cyc-7", deployment_id=sid, timestamp=anchor)]
        dashboard_service._state_manager.get_ledger_entries = AsyncMock(return_value=ledger)
        # Timeline event at t+1s, referencing the ledger. Sort DESC: timeline first.
        _add_timeline(
            sid,
            anchor + timedelta(seconds=1),
            description="dup_after_ledger_write",
            related_ledger_entry_id="lg-7",
        )
        # Another standalone timeline at t+2s to fill the page.
        _add_timeline(sid, anchor + timedelta(seconds=2), description="standalone")

        response = await dashboard_service.GetActivityFeed(
            gateway_pb2.GetActivityFeedRequest(deployment_id=sid, limit=10),
            mock_context,
        )
        kinds = [i.kind for i in response.items]
        descriptions = [i.timeline_event.description for i in response.items if i.timeline_event.description]
        ledger_ids = [i.ledger_entry.id for i in response.items if i.ledger_entry.id]

        # Page must contain: standalone timeline + lg-7 ledger row. The
        # timeline that referenced lg-7 is dropped (the ledger row is the
        # truth), even though the timeline sorted FIRST in DESC order.
        assert "dup_after_ledger_write" not in descriptions, (
            "Timeline event whose ref'd ledger appears later on the page "
            f"must be dropped (PRD §6.1 ledger-wins). Got: {descriptions}"
        )
        assert "standalone" in descriptions
        assert "lg-7" in ledger_ids
        assert kinds.count(gateway_pb2.ActivityFeedItem.Kind.TIMELINE_EVENT) == 1
        assert kinds.count(gateway_pb2.ActivityFeedItem.Kind.LEDGER_ENTRY) == 1

    @pytest.mark.asyncio
    async def test_ledger_on_page_still_dedups_timeline(self, dashboard_service, mock_context):
        """Sanity: when the ledger row IS on the page, dedup still happens."""
        sid = "test_strategy"
        now = datetime.now(UTC)

        # Both at `now`: ledger sorts before timeline at ties (priority "1" > "0").
        ledger = [_LedgerRow(id="lg-7", cycle_id="cyc-7", deployment_id=sid, timestamp=now)]
        dashboard_service._state_manager.get_ledger_entries = AsyncMock(return_value=ledger)
        _add_timeline(sid, now, description="dup", related_ledger_entry_id="lg-7")
        _add_timeline(sid, now - timedelta(seconds=10), description="other")

        response = await dashboard_service.GetActivityFeed(
            gateway_pb2.GetActivityFeedRequest(deployment_id=sid, limit=10),
            mock_context,
        )
        kinds = [i.kind for i in response.items]
        # ledger is on page → timeline dup MUST be dropped → 1 ledger + 1 "other"
        assert kinds.count(gateway_pb2.ActivityFeedItem.Kind.TIMELINE_EVENT) == 1
        assert kinds.count(gateway_pb2.ActivityFeedItem.Kind.LEDGER_ENTRY) == 1
        descriptions = [i.timeline_event.description for i in response.items if i.timeline_event.description]
        assert "other" in descriptions
        assert "dup" not in descriptions

    @pytest.mark.asyncio
    async def test_dedup_drops_all_timeline_refs_to_same_ledger_id(self, dashboard_service, mock_context):
        """CodeRabbit on PR #2117 round 5: the dedup map ``pending_timeline_refs``
        previously stored one index per ``related_ledger_entry_id``. If two
        (or more) timeline rows referenced the SAME ledger row and both
        sorted BEFORE the ledger in DESC order, only the latest assignment
        survived in the map — the earlier one leaked into the response when
        the ledger arrived. That's a silent violation of the
        dedup-by-`related_ledger_entry_id` contract (PRD §6.1).

        This pathology is plausible in production: a single execution can
        produce multiple UX cards (e.g. "Position opened" + "Fee tier set"
        + "Range configured") that all narrate the same on-chain tx and
        therefore all carry the same ``related_ledger_entry_id``. After the
        fix all of them must collapse onto the single ledger row.
        """
        sid = "test_strategy"
        anchor = datetime(2026, 5, 1, 12, 0, 0, tzinfo=UTC)
        ledger = [_LedgerRow(id="lg-multi", cycle_id="cyc-1", deployment_id=sid, timestamp=anchor)]
        dashboard_service._state_manager.get_ledger_entries = AsyncMock(return_value=ledger)
        # Three UX rows, all referencing lg-multi, all sort BEFORE lg-multi
        # in DESC (newer ts).
        _add_timeline(
            sid,
            anchor + timedelta(seconds=3),
            description="ux_card_a",
            related_ledger_entry_id="lg-multi",
        )
        _add_timeline(
            sid,
            anchor + timedelta(seconds=2),
            description="ux_card_b",
            related_ledger_entry_id="lg-multi",
        )
        _add_timeline(
            sid,
            anchor + timedelta(seconds=1),
            description="ux_card_c",
            related_ledger_entry_id="lg-multi",
        )

        response = await dashboard_service.GetActivityFeed(
            gateway_pb2.GetActivityFeedRequest(deployment_id=sid, limit=10),
            mock_context,
        )
        kinds = [i.kind for i in response.items]
        descriptions = [i.timeline_event.description for i in response.items if i.timeline_event.description]
        ledger_ids = [i.ledger_entry.id for i in response.items if i.ledger_entry.id]

        assert kinds.count(gateway_pb2.ActivityFeedItem.Kind.LEDGER_ENTRY) == 1, "exactly one ledger row must survive"
        assert kinds.count(gateway_pb2.ActivityFeedItem.Kind.TIMELINE_EVENT) == 0, (
            f"all three timeline rows referencing lg-multi must collapse onto "
            f"the ledger row (PR #2117 round 5 — CR's multi-pending bug). "
            f"Got descriptions: {descriptions!r}"
        )
        assert "lg-multi" in ledger_ids
        for leaked in ("ux_card_a", "ux_card_b", "ux_card_c"):
            assert leaked not in descriptions, f"{leaked} leaked through the dedup gate — multi-pending bug regression"


class TestBackfillLoop:
    """CodeRabbit (heavy lift): the compositor used to over-fetch ``limit + 1``
    per stream. Combined with ``_apply_boundary_filter`` and
    ``_select_page_with_incremental_dedup`` (which can each drop more than one
    candidate), this could return short pages while older rows remained in the
    stores — a paginated client would silently lose data.

    The fix is a bounded backfill loop in
    ``_gather_activity_feed_page``. Each iteration over-fetches
    ``limit * OVER_FETCH_FACTOR + 1`` per stream and advances per-stream
    cursors strictly past each batch's oldest item. Loop stops when the page
    fills, both streams exhaust, or ``MAX_BACKFILL_ATTEMPTS`` is reached.

    These tests pin the contract on three concrete pathologies and the
    bounded-RPC guarantee.
    """

    @pytest.mark.asyncio
    async def test_dedup_heavy_window_fills_page_via_backfill(self, dashboard_service, mock_context):
        """The pathology that motivated this fix.

        One ledger row at the top of the merged stream + many timeline events
        referencing it = the dedup walks past all the timeline events without
        adding them, so the page would have come back with just the ledger row.
        With backfill, the page fills with the older unreferenced timeline
        events that remained in the store.
        """
        sid = "test_strategy"
        now = datetime.now(UTC)
        # Single ledger row at t=now (top of stream).
        ledger = [
            _LedgerRow(
                id="lg-top",
                cycle_id="cyc-top",
                deployment_id=sid,
                timestamp=now,
            )
        ]
        dashboard_service._state_manager.get_ledger_entries = AsyncMock(return_value=ledger)

        # 6 timeline events referencing lg-top, then 5 unreferenced timeline
        # events older still. With limit=5 and OVER_FETCH_FACTOR=3 this would
        # also fail under a naive ``limit + 1`` over-fetch (6 fetched, all
        # referencing lg-top → 0 timeline survive dedup → page would be 1).
        for i in range(6):
            _add_timeline(
                sid,
                now - timedelta(seconds=i + 1),
                description=f"dup{i}",
                related_ledger_entry_id="lg-top",
            )
        for i in range(5):
            _add_timeline(
                sid,
                now - timedelta(seconds=i + 100),
                description=f"keep{i}",
            )

        response = await dashboard_service.GetActivityFeed(
            gateway_pb2.GetActivityFeedRequest(deployment_id=sid, limit=5),
            mock_context,
        )

        # Page must be full. Item 0 = lg-top (ledger sorts first). Items 1..4
        # = the unreferenced timeline events (the duplicates were correctly
        # dropped by dedup against the on-page ledger row).
        assert len(response.items) == 5, (
            "Backfill must surface the unreferenced timeline events that the "
            "old ``limit + 1`` over-fetch left stranded behind the dedup curtain."
        )
        assert response.items[0].kind == gateway_pb2.ActivityFeedItem.Kind.LEDGER_ENTRY
        assert response.items[0].ledger_entry.id == "lg-top"
        # Remaining 4 items must be the unreferenced timelines (keep0..keep3,
        # ordered by timestamp DESC; keep4 is on the next page).
        descriptions = [i.timeline_event.description for i in response.items[1:]]
        assert descriptions == ["keep0", "keep1", "keep2", "keep3"], f"backfill returned wrong items: {descriptions}"

    @pytest.mark.asyncio
    async def test_both_streams_exhaust_returns_short_page_with_has_more_false(self, dashboard_service, mock_context):
        """When the cumulative buffer is genuinely smaller than ``limit`` and
        both streams have run dry, return what we have and say so.

        ``has_more=True`` here would lie to the client — they'd paginate
        forever, calling the gateway with a cursor that produces zero rows.
        """
        sid = "test_strategy"
        now = datetime.now(UTC)
        # Only 3 items total, limit=10 — cannot fill, must report no more.
        _add_timeline(sid, now, description="t0")
        ledger = [
            _LedgerRow(
                id="lg-1",
                cycle_id="cyc-1",
                deployment_id=sid,
                timestamp=now - timedelta(seconds=1),
            ),
            _LedgerRow(
                id="lg-2",
                cycle_id="cyc-2",
                deployment_id=sid,
                timestamp=now - timedelta(seconds=2),
            ),
        ]
        dashboard_service._state_manager.get_ledger_entries = AsyncMock(return_value=ledger)

        response = await dashboard_service.GetActivityFeed(
            gateway_pb2.GetActivityFeedRequest(deployment_id=sid, limit=10),
            mock_context,
        )
        assert len(response.items) == 3
        assert response.has_more is False, "exhausted streams must report no more"
        assert response.next_before_timestamp == 0
        assert response.next_before_id == ""

    @pytest.mark.asyncio
    async def test_backfill_advances_per_stream_cursor_strictly(self, dashboard_service, mock_context, monkeypatch):
        """The backfill loop must advance per-stream cursors strictly between
        iterations so the gateway never re-reads the same window.

        CodeRabbit on PR #2117: the prior version of this test exited after
        a single fetch (the merged dataset fit under ``over_fetch``), so it
        never demonstrated cursor advancement. This version
          1. shrinks ``OVER_FETCH_FACTOR`` so the page-fill threshold is
             reachable with a small fixture,
          2. constructs a duplicate-heavy timeline that forces the inner
             page-select to leave the page short (so the outer loop must
             backfill from the timeline stream multiple times),
          3. spies on the timeline store's ``get_events`` to record each
             ``before`` cursor and asserts they descend strictly.
        """
        sid = "test_strategy"
        anchor = datetime(2026, 5, 1, 12, 0, 0, tzinfo=UTC)

        # Shrink the over-fetch so each store call reads a small slice.
        dashboard_service._ACTIVITY_FEED_OVER_FETCH_FACTOR = 1

        # ``lg-target`` is the dedup anchor — it must sort INTO the window
        # (i.e., be newer than the dups) so the inner select drops every dup
        # against it. Place lg-target at anchor and dups at anchor-Δ so the
        # ledger sits at sort position 1 and the dups follow in positions
        # 2..N.
        ledger = [
            _LedgerRow(
                id="lg-target",
                cycle_id="c",
                deployment_id=sid,
                timestamp=anchor,
            ),
        ]
        dashboard_service._state_manager.get_ledger_entries = AsyncMock(return_value=ledger)

        # 12 timeline events all referencing ``lg-target``, all OLDER than
        # the ledger so the ledger lands in the inner select's window. Inner
        # select keeps lg-target and drops every dup → page returns just
        # ``[lg-target]`` from each iteration → outer loop must keep
        # backfilling the timeline stream.
        for i in range(12):
            _add_timeline(
                sid,
                anchor - timedelta(seconds=i + 1),
                description=f"dup{i}",
                related_ledger_entry_id="lg-target",
            )

        # Spy on the timeline store. The store is a global singleton; this
        # replaces ``get_events`` with a recorder that delegates to the real
        # implementation.
        store = get_timeline_store()
        real_get_events = store.get_events
        timeline_cursors_seen: list[datetime | None] = []

        def _spy_get_events(*args, **kwargs):
            timeline_cursors_seen.append(kwargs.get("before"))
            return real_get_events(*args, **kwargs)

        monkeypatch.setattr(store, "get_events", _spy_get_events)

        await dashboard_service.GetActivityFeed(
            gateway_pb2.GetActivityFeedRequest(deployment_id=sid, limit=3),
            mock_context,
        )

        # Multiple timeline fetches observed (≥ 2 = at least one backfill iter).
        assert len(timeline_cursors_seen) >= 2, (
            "Expected the backfill loop to invoke the timeline store ≥ 2 times "
            f"for this duplicate-heavy scenario. cursors_seen={timeline_cursors_seen}"
        )
        # Each subsequent ``before`` cursor must descend strictly. The first
        # call's cursor (``None``) has no predecessor; subsequent calls must
        # be strictly older. None after a real cursor would mean a regression
        # (we'd re-read the same window), so it's never permitted past the
        # first call.
        for prior, current in zip(timeline_cursors_seen, timeline_cursors_seen[1:], strict=False):
            assert current is not None, (
                f"Cursor regressed to ``None`` after advancement. cursors_seen={timeline_cursors_seen}"
            )
            prior_ts = prior.timestamp() if prior is not None else float("inf")
            assert current.timestamp() < prior_ts, (
                "Per-stream cursor must descend strictly between iterations "
                f"to bound RPC cost. cursors_seen={timeline_cursors_seen}"
            )

    @pytest.mark.asyncio
    async def test_backfill_bounded_by_max_attempts(self, dashboard_service, mock_context, monkeypatch):
        """``MAX_BACKFILL_ATTEMPTS`` caps total backend RPC calls.

        A pathologically saturated tie-second (more items at one timestamp
        than the over-fetch can drain) must NOT cause unbounded fan-out.
        """
        sid = "test_strategy"
        now = datetime.now(UTC)

        # Build a ledger that returns ``over_fetch`` rows at the same
        # timestamp every call — saturated tie-second. The cursor advances by
        # 0 seconds each iteration, so every attempt returns the SAME rows.
        ledger_rows = [
            _LedgerRow(
                id=f"lg-{i}",
                cycle_id="c",
                deployment_id=sid,
                timestamp=now,
            )
            for i in range(31)
        ]

        call_count = [0]

        async def _spy(_deployment_id, *, since=None, intent_type=None, limit=None, before=None):
            call_count[0] += 1
            return ledger_rows[:limit] if limit else ledger_rows

        dashboard_service._state_manager.get_ledger_entries = AsyncMock(side_effect=_spy)

        await dashboard_service.GetActivityFeed(
            gateway_pb2.GetActivityFeedRequest(deployment_id=sid, limit=10),
            mock_context,
        )

        # Expected: at most MAX_BACKFILL_ATTEMPTS calls. The constant is
        # intentionally tested via the live class attribute so the bound is
        # enforced even if we tune the factor later.
        assert call_count[0] <= dashboard_service._ACTIVITY_FEED_MAX_BACKFILL_ATTEMPTS, (
            f"backfill loop fanned out unbounded: {call_count[0]} calls "
            f"(cap = {dashboard_service._ACTIVITY_FEED_MAX_BACKFILL_ATTEMPTS})"
        )

    @pytest.mark.asyncio
    async def test_backfill_truncation_signal_set_when_max_attempts_reached(
        self, dashboard_service, mock_context, monkeypatch
    ):
        """CodeRabbit on PR #2117: when the loop hits MAX_ATTEMPTS without
        filling the page AND at least one stream still has rows, the wire
        flag ``backfill_truncated`` MUST be True. This is the operator-visible
        signal that distinguishes "end of feed" from "tail of a tie-second
        was dropped".

        Setup: single ledger row (lg-0) at the NEWEST timestamp, and a
        timeline stream that NEVER exhausts (returns ``over_fetch`` items
        per call, all referencing lg-0). The dedup walks the window —
        [lg-0, dup, dup, ...] — keeps lg-0 and drops every dup, so the
        inner page-select returns ``[lg-0]`` from each iteration. The
        outer backfill loop continues because ``timeline_exhausted`` is
        False, hits MAX_ATTEMPTS, and sets the truncation flag.
        """
        sid = "test_strategy"
        anchor = datetime(2026, 5, 1, 12, 0, 0, tzinfo=UTC)

        # Ledger has only 1 item (lg-0) at the newest second — gets
        # exhausted on iter 1.
        ledger = [
            _LedgerRow(
                id="lg-0",
                cycle_id="c",
                deployment_id=sid,
                timestamp=anchor,
            )
        ]
        dashboard_service._state_manager.get_ledger_entries = AsyncMock(return_value=ledger)

        # Stub the timeline loader to NEVER exhaust — it always returns
        # ``over_fetch`` fresh dups, all ref'd to lg-0. The decreasing
        # timestamps come from a counter so per-stream cursor advance is
        # honest (strictly older each call).
        call_count = [0]

        def _fake_load_timeline(_resolved_id, limit_plus_one, _filter, store_before):
            call_count[0] += 1
            # Each call yields over_fetch fresh dups one second older than the
            # last batch's oldest row, all referencing lg-0.
            base_offset = (call_count[0] - 1) * limit_plus_one
            return [
                TimelineEvent(
                    event_id=f"dup-{base_offset + i}",
                    deployment_id=sid,
                    timestamp=anchor - timedelta(seconds=base_offset + i + 1),
                    event_type="STATE_CHANGE",
                    description=f"dup{base_offset + i}",
                    related_ledger_entry_id="lg-0",
                )
                for i in range(limit_plus_one)
            ]

        monkeypatch.setattr(dashboard_service, "_load_timeline_for_feed", _fake_load_timeline)

        response = await dashboard_service.GetActivityFeed(
            gateway_pb2.GetActivityFeedRequest(deployment_id=sid, limit=10),
            mock_context,
        )

        # The page must be SHORT — only lg-0 survives dedup each iter.
        assert len(response.items) == 1, (
            f"Expected page=[lg-0] only (every dup gets dedup'd against lg-0); got {len(response.items)} items"
        )
        # And the truncation flag must be set: loop hit MAX_ATTEMPTS,
        # timeline_exhausted is False (the stub never returns < over_fetch),
        # ledger_exhausted is True. The condition for truncation = (max
        # attempts hit) AND (NOT both exhausted) AND (page < limit) — all
        # three hold here.
        assert response.backfill_truncated is True, (
            "Wire-level truncation signal MUST be True when MAX_ATTEMPTS "
            "fires without filling AND at least one stream has more rows."
        )
        # Sanity: the loop ran exactly MAX_BACKFILL_ATTEMPTS times.
        assert call_count[0] == dashboard_service._ACTIVITY_FEED_MAX_BACKFILL_ATTEMPTS

    @pytest.mark.asyncio
    async def test_backfill_truncation_false_on_clean_end_of_feed(self, dashboard_service, mock_context):
        """``backfill_truncated`` MUST stay False when the page is just
        short because both streams genuinely exhausted — that's "end of
        feed", not the saturation pathology.
        """
        sid = "test_strategy"
        now = datetime.now(UTC)
        _add_timeline(sid, now, description="t0")
        dashboard_service._state_manager.get_ledger_entries = AsyncMock(return_value=[])

        response = await dashboard_service.GetActivityFeed(
            gateway_pb2.GetActivityFeedRequest(deployment_id=sid, limit=10),
            mock_context,
        )
        assert len(response.items) == 1
        assert response.has_more is False
        assert response.backfill_truncated is False, "End-of-feed (both streams exhausted) is NOT truncation."


class TestPayloadOneofInvariant:
    """``ActivityFeedItem.payload`` is a protobuf ``oneof`` (CodeRabbit review).

    Schema-level enforcement that exactly one of ``timeline_event`` /
    ``ledger_entry`` is populated. Without ``oneof`` the schema permits invalid
    payloads (both set, or kind/payload desync); with ``oneof`` the gateway
    can assert ``WhichOneof("payload")`` matches ``kind``.
    """

    @pytest.mark.asyncio
    async def test_compositor_emits_oneof_payload(self, dashboard_service, mock_context):
        """Every emitted item must have exactly one of the two payloads set."""
        sid = "test_strategy"
        now = datetime.now(UTC)
        _add_timeline(sid, now, description="ux event")
        ledger = [_LedgerRow(id="lg-1", cycle_id="cyc-1", deployment_id=sid, timestamp=now - timedelta(seconds=1))]
        dashboard_service._state_manager.get_ledger_entries = AsyncMock(return_value=ledger)

        response = await dashboard_service.GetActivityFeed(
            gateway_pb2.GetActivityFeedRequest(deployment_id=sid, limit=10),
            mock_context,
        )
        for item in response.items:
            which = item.WhichOneof("payload")
            assert which is not None, "every item must have a payload set"
            if item.kind == gateway_pb2.ActivityFeedItem.Kind.TIMELINE_EVENT:
                assert which == "timeline_event"
            elif item.kind == gateway_pb2.ActivityFeedItem.Kind.LEDGER_ENTRY:
                assert which == "ledger_entry"
            else:
                pytest.fail(f"unexpected kind: {item.kind}")

    def test_setting_one_payload_clears_the_other(self):
        """oneof semantics: assigning ledger_entry clears any prior timeline_event."""
        item = gateway_pb2.ActivityFeedItem(kind=gateway_pb2.ActivityFeedItem.Kind.TIMELINE_EVENT)
        item.timeline_event.event_type = "STATE_CHANGE"
        assert item.WhichOneof("payload") == "timeline_event"
        # Switch to ledger_entry — oneof must clear timeline_event.
        item.ledger_entry.id = "lg-1"
        assert item.WhichOneof("payload") == "ledger_entry"
        assert item.timeline_event.event_type == "", "timeline_event must be cleared by oneof"


class TestClientConversion:
    """Verify the dashboard client converts the feed cleanly."""

    def test_client_kind_dispatch(self):
        from almanak.framework.dashboard.gateway_client import (
            ActivityFeedItem as ClientItem,
        )
        from almanak.framework.dashboard.gateway_client import (
            GatewayDashboardClient,
        )

        # Build a fake gRPC response with 2 items: one timeline, one ledger.
        ts = int(datetime.now(UTC).timestamp())
        proto_response = gateway_pb2.GetActivityFeedResponse(
            items=[
                gateway_pb2.ActivityFeedItem(
                    kind=gateway_pb2.ActivityFeedItem.Kind.TIMELINE_EVENT,
                    timestamp=ts,
                    cycle_id="cyc-1",
                    timeline_event=gateway_pb2.TimelineEventInfo(
                        timestamp=ts,
                        event_type="STATE_CHANGE",
                        description="d",
                        cycle_id="cyc-1",
                    ),
                ),
                gateway_pb2.ActivityFeedItem(
                    kind=gateway_pb2.ActivityFeedItem.Kind.LEDGER_ENTRY,
                    timestamp=ts - 1,
                    cycle_id="cyc-2",
                    ledger_entry=gateway_pb2.LedgerEntryInfo(
                        id="lg-1",
                        cycle_id="cyc-2",
                        deployment_id="s",
                        timestamp=ts - 1,
                        intent_type="SWAP",
                        amount_in="1",
                        amount_out="1",
                        success=True,
                    ),
                ),
            ],
            has_more=False,
            next_before_timestamp=0,
        )

        # Stub-out the client's gRPC layer with a mock that returns the response.
        client = GatewayDashboardClient.__new__(GatewayDashboardClient)
        client._client = MagicMock()
        client._client.dashboard.GetActivityFeed = MagicMock(return_value=proto_response)
        client._ensure_connected = lambda: client._client  # type: ignore[method-assign]

        result = client.get_activity_feed("s", limit=10)

        assert len(result.items) == 2
        assert result.has_more is False
        assert result.items[0].kind == "TIMELINE_EVENT"
        assert isinstance(result.items[0], ClientItem)
        assert result.items[0].timeline_event.event_type == "STATE_CHANGE"
        assert result.items[0].ledger_entry is None

        assert result.items[1].kind == "LEDGER_ENTRY"
        assert result.items[1].ledger_entry.id == "lg-1"
        assert result.items[1].timeline_event is None
