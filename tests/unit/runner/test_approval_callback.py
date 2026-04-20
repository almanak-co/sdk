"""Tests for the runner's polling approval callback (VIB-2927).

Covers every response path the EscalatingSlippageManager can hit:
- approve (happy path)
- wait_and_escalate (operator asks to advance to next level)
- cancel (operator aborts)
- timeout (no response in window → auto-escalate)
- malformed JSON (defensive parsing → safe default, never cancel)
- unknown action (whitelist enforcement → safe default)
- missing expires_at on request (fallback deadline, no crash)
"""

from __future__ import annotations

import asyncio
import json
import threading
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from almanak.framework.runner.runner_teardown import (
    _APPROVAL_POLL_INTERVAL_S,
    _make_approval_callback,
)
from almanak.framework.teardown.models import (
    ApprovalRequest,
    EscalationLevel,
)
from almanak.framework.teardown.state_manager import TeardownStateAdapter


@pytest.fixture
def adapter(tmp_path: Path) -> TeardownStateAdapter:
    return TeardownStateAdapter(db_path=tmp_path / "state.db")


@pytest.fixture(autouse=True)
def _fast_poll(monkeypatch: pytest.MonkeyPatch) -> None:
    """Speed up the callback's sleep so tests finish quickly."""
    monkeypatch.setattr(
        "almanak.framework.runner.runner_teardown._APPROVAL_POLL_INTERVAL_S",
        0.01,
    )


def _make_request(expires_in: timedelta | None = timedelta(seconds=2)) -> ApprovalRequest:
    return ApprovalRequest(
        teardown_id="td_1",
        strategy_id="strat_1",
        current_level=EscalationLevel.LEVEL_3,
        current_slippage=Decimal("0.05"),
        estimated_loss_usd=Decimal("50"),
        position_value_usd=Decimal("1000"),
        reason="test",
        options=["approve", "wait_and_escalate", "cancel"],
        requested_at=datetime.now(UTC),
        expires_at=datetime.now(UTC) + expires_in if expires_in else None,
    )


def _make_runner(alert_raises: bool = False) -> MagicMock:
    runner = MagicMock()
    if alert_raises:
        runner.alert_manager = MagicMock()

        async def _raise(*_a, **_k):
            raise RuntimeError("alert channel down")

        runner.alert_manager.send_approval_needed = _raise
    else:
        runner.alert_manager = None
    return runner


async def _respond_after(
    adapter: TeardownStateAdapter,
    delay_s: float,
    response: dict,
) -> None:
    """Write an approval response to the SQLite channel after ``delay_s``."""
    await asyncio.sleep(delay_s)
    adapter.write_approval_response(
        teardown_id="td_1",
        level=EscalationLevel.LEVEL_3,
        response_json=json.dumps(response),
    )


class TestCallbackHappyPaths:
    @pytest.mark.asyncio
    async def test_approve_returns_approved_response(self, adapter: TeardownStateAdapter) -> None:
        callback = _make_approval_callback(_make_runner(), adapter)
        request = _make_request()

        writer = asyncio.create_task(
            _respond_after(adapter, 0.05, {"approved": True, "action": "approve"})
        )
        response = await callback(request)
        await writer

        assert response.approved is True
        assert response.action == "approve"

    @pytest.mark.asyncio
    async def test_wait_and_escalate_returns_unapproved_escalate(
        self, adapter: TeardownStateAdapter
    ) -> None:
        callback = _make_approval_callback(_make_runner(), adapter)
        request = _make_request()

        writer = asyncio.create_task(
            _respond_after(adapter, 0.05, {"approved": False, "action": "wait_and_escalate"})
        )
        response = await callback(request)
        await writer

        assert response.approved is False
        assert response.action == "wait_and_escalate"

    @pytest.mark.asyncio
    async def test_cancel_returns_unapproved_cancel(self, adapter: TeardownStateAdapter) -> None:
        callback = _make_approval_callback(_make_runner(), adapter)
        request = _make_request()

        writer = asyncio.create_task(
            _respond_after(adapter, 0.05, {"approved": False, "action": "cancel"})
        )
        response = await callback(request)
        await writer

        assert response.approved is False
        assert response.action == "cancel"


class TestCallbackDefensive:
    @pytest.mark.asyncio
    async def test_timeout_auto_escalates_not_cancels(self, adapter: TeardownStateAdapter) -> None:
        """With no operator response, timeout must auto-escalate, never cancel.

        This is the safety-critical guarantee: an operator who's asleep must
        not accidentally cancel a teardown that's hitting the approval gate.
        """
        callback = _make_approval_callback(_make_runner(), adapter)
        # Expire almost immediately so the callback times out on the first poll.
        request = _make_request(expires_in=timedelta(milliseconds=50))

        response = await callback(request)

        assert response.approved is False
        assert response.action == "wait_and_escalate"  # safe default, NOT cancel

    @pytest.mark.asyncio
    async def test_malformed_json_falls_back_to_safe_default(
        self, adapter: TeardownStateAdapter
    ) -> None:
        callback = _make_approval_callback(_make_runner(), adapter)
        request = _make_request()

        async def _write_garbage() -> None:
            await asyncio.sleep(0.05)
            adapter.write_approval_response(
                teardown_id="td_1",
                level=EscalationLevel.LEVEL_3,
                response_json="this is not JSON {",
            )

        writer = asyncio.create_task(_write_garbage())
        response = await callback(request)
        await writer

        assert response.action == "wait_and_escalate"  # not cancel, not crash

    @pytest.mark.asyncio
    async def test_unknown_action_falls_back_to_safe_default(
        self, adapter: TeardownStateAdapter
    ) -> None:
        callback = _make_approval_callback(_make_runner(), adapter)
        request = _make_request()

        writer = asyncio.create_task(
            _respond_after(adapter, 0.05, {"action": "eject_pilot"})
        )
        response = await callback(request)
        await writer

        assert response.action == "wait_and_escalate"

    @pytest.mark.asyncio
    async def test_non_object_json_falls_back(self, adapter: TeardownStateAdapter) -> None:
        callback = _make_approval_callback(_make_runner(), adapter)
        request = _make_request()

        async def _write_array() -> None:
            await asyncio.sleep(0.05)
            adapter.write_approval_response(
                teardown_id="td_1",
                level=EscalationLevel.LEVEL_3,
                response_json=json.dumps(["not", "an", "object"]),
            )

        writer = asyncio.create_task(_write_array())
        response = await callback(request)
        await writer

        assert response.action == "wait_and_escalate"

    @pytest.mark.asyncio
    async def test_missing_expires_at_uses_fallback_deadline(
        self, adapter: TeardownStateAdapter
    ) -> None:
        """expires_at is typed Optional — callback must not crash when None."""
        callback = _make_approval_callback(_make_runner(), adapter)
        request = _make_request(expires_in=None)

        writer = asyncio.create_task(
            _respond_after(adapter, 0.05, {"approved": True, "action": "approve"})
        )
        response = await callback(request)
        await writer

        assert response.action == "approve"

    @pytest.mark.asyncio
    async def test_alert_failure_does_not_abort_callback(
        self, adapter: TeardownStateAdapter
    ) -> None:
        """If the alert channel is down, we still fall back to polling — an
        operator watching the dashboard can still respond through the API."""
        runner = _make_runner(alert_raises=True)
        callback = _make_approval_callback(runner, adapter)
        request = _make_request()

        writer = asyncio.create_task(
            _respond_after(adapter, 0.05, {"approved": True, "action": "approve"})
        )
        response = await callback(request)
        await writer

        assert response.action == "approve"


class TestApprovedFieldParsing:
    """Regression tests for CodeRabbit round 4: payload must not collapse
    `approved` strings to True via bool(), and an invalid approved_slippage
    must fall back to the safe default instead of raising."""

    @pytest.mark.asyncio
    async def test_string_false_is_not_true(self, adapter: TeardownStateAdapter) -> None:
        """`{"approved": "false"}` must parse as approved=False, not True."""
        callback = _make_approval_callback(_make_runner(), adapter)
        request = _make_request()

        writer = asyncio.create_task(
            _respond_after(adapter, 0.05, {"approved": "false", "action": "wait_and_escalate"})
        )
        response = await callback(request)
        await writer

        assert response.approved is False
        assert response.action == "wait_and_escalate"

    @pytest.mark.asyncio
    async def test_string_true_is_true(self, adapter: TeardownStateAdapter) -> None:
        callback = _make_approval_callback(_make_runner(), adapter)
        request = _make_request()

        writer = asyncio.create_task(
            _respond_after(adapter, 0.05, {"approved": "true", "action": "approve"})
        )
        response = await callback(request)
        await writer

        assert response.approved is True

    @pytest.mark.asyncio
    async def test_invalid_approved_slippage_falls_back_safely(
        self, adapter: TeardownStateAdapter
    ) -> None:
        """A non-numeric approved_slippage must not crash the callback."""
        callback = _make_approval_callback(_make_runner(), adapter)
        request = _make_request()

        writer = asyncio.create_task(
            _respond_after(
                adapter,
                0.05,
                {"approved": True, "action": "approve", "approved_slippage": "not_a_number"},
            )
        )
        response = await callback(request)
        await writer

        # Safe-default path — we do not execute at a slippage we can't parse.
        assert response.action == "wait_and_escalate"
        assert response.approved is False
        assert response.approved_slippage is None


class TestLateResponseBeatsTimeoutWrite:
    """Regression test for CodeRabbit round 4: a response landing in the final
    sleep gap (after the last poll but before the timeout write) must not be
    dropped in favour of auto-escalation."""

    @pytest.mark.asyncio
    async def test_race_response_honoured(
        self, adapter: TeardownStateAdapter, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        callback = _make_approval_callback(_make_runner(), adapter)
        request = _make_request(expires_in=timedelta(milliseconds=50))

        # Simulate the race: the operator writes *before* the callback
        # attempts its timeout-write. The WHERE response_json IS NULL guard
        # on write_approval_response means our timeout write returns False,
        # and the callback should then re-read and honour the real response.
        original_write = adapter.write_approval_response

        def write_then_maybe_race(teardown_id, level, response_json):
            # If this is the timeout-write attempt, first slip the operator
            # response in so the subsequent UPDATE matches zero rows.
            data = json.loads(response_json)
            if data.get("timeout"):
                original_write(
                    teardown_id,
                    level,
                    json.dumps({"approved": True, "action": "approve"}),
                )
            return original_write(teardown_id, level, response_json)

        monkeypatch.setattr(adapter, "write_approval_response", write_then_maybe_race)

        response = await callback(request)

        assert response.approved is True
        assert response.action == "approve"


class TestCreateRequestPreservesResponse:
    """Regression test for CodeRabbit round 4: INSERT ... ON CONFLICT DO UPDATE
    must preserve an already-landed operator response, so a runner restart or
    retry that re-emits the same (teardown_id, level) doesn't wipe approvals."""

    def test_reemit_after_response_does_not_clobber(self, adapter: TeardownStateAdapter) -> None:
        expires = (datetime.now(UTC) + timedelta(minutes=30)).isoformat()
        adapter.create_approval_request(
            teardown_id="td_1",
            strategy_id="strat_1",
            level=EscalationLevel.LEVEL_3,
            request_json=json.dumps({"attempt": 1}),
            expires_at=expires,
        )

        # Operator responds.
        adapter.write_approval_response(
            teardown_id="td_1",
            level=EscalationLevel.LEVEL_3,
            response_json=json.dumps({"approved": True, "action": "approve"}),
        )

        # Runner restarts and re-emits the same request. Must NOT clobber.
        adapter.create_approval_request(
            teardown_id="td_1",
            strategy_id="strat_1",
            level=EscalationLevel.LEVEL_3,
            request_json=json.dumps({"attempt": 2}),
            expires_at=expires,
        )

        body = adapter.get_approval_response("td_1", EscalationLevel.LEVEL_3)
        assert body is not None
        assert json.loads(body)["approved"] is True

    def test_reemit_without_prior_response_updates_fields(
        self, adapter: TeardownStateAdapter
    ) -> None:
        """Re-emit when no response exists should still refresh expires_at/request."""
        expires_old = (datetime.now(UTC) + timedelta(minutes=5)).isoformat()
        adapter.create_approval_request(
            teardown_id="td_1",
            strategy_id="strat_1",
            level=EscalationLevel.LEVEL_3,
            request_json=json.dumps({"attempt": 1}),
            expires_at=expires_old,
        )
        expires_new = (datetime.now(UTC) + timedelta(minutes=30)).isoformat()
        adapter.create_approval_request(
            teardown_id="td_1",
            strategy_id="strat_1",
            level=EscalationLevel.LEVEL_3,
            request_json=json.dumps({"attempt": 2}),
            expires_at=expires_new,
        )

        pending = adapter.get_latest_pending_approval("strat_1")
        assert pending is not None
        assert json.loads(pending["request_json"])["attempt"] == 2


class TestCallbackApprovedSlippage:
    @pytest.mark.asyncio
    async def test_approved_slippage_is_propagated(self, adapter: TeardownStateAdapter) -> None:
        """When operator approves with a specific slippage, it must flow through."""
        callback = _make_approval_callback(_make_runner(), adapter)
        request = _make_request()

        writer = asyncio.create_task(
            _respond_after(
                adapter,
                0.05,
                {"approved": True, "action": "approve", "approved_slippage": "0.06"},
            )
        )
        response = await callback(request)
        await writer

        assert response.approved is True
        assert response.approved_slippage == Decimal("0.06")


class TestTimeoutClearsApprovalRow:
    """Regression test for the round-2 finding that both Claude and Codex flagged.

    If a level-3 approval times out and the escalation advances to level 4, a
    later API response written via `write_approval_response_by_strategy`
    targets the OLDEST pending row — which would be the stale level-3 row if
    we don't mark it resolved on timeout. The operator's decision would then
    silently land on the wrong level.
    """

    @pytest.mark.asyncio
    async def test_timed_out_row_is_marked_resolved(self, adapter: TeardownStateAdapter) -> None:
        callback = _make_approval_callback(_make_runner(), adapter)
        # Expire almost immediately.
        request = _make_request(expires_in=timedelta(milliseconds=50))

        response = await callback(request)

        assert response.action == "wait_and_escalate"

        # The level-3 row must now be resolved (no longer picked up by
        # get_latest_pending_approval) so a later API response lands on the
        # NEXT level's row instead.
        pending = adapter.get_latest_pending_approval("strat_1")
        assert pending is None, (
            "Timed-out approval row was not marked resolved — a late API "
            "response would land on this stale row instead of the current level."
        )

    @pytest.mark.asyncio
    async def test_late_api_response_after_timeout_does_not_clobber_next_level(
        self, adapter: TeardownStateAdapter
    ) -> None:
        """Full round-trip: level 3 times out → escalation creates level 4 →
        a late operator response from the API lands on level 4, not level 3."""
        callback = _make_approval_callback(_make_runner(), adapter)
        level_3_request = _make_request(expires_in=timedelta(milliseconds=50))

        # Level 3 times out.
        await callback(level_3_request)

        # Simulate EscalatingSlippageManager advancing to level 4 — a new
        # approval request is created.
        adapter.create_approval_request(
            teardown_id="td_1",
            strategy_id="strat_1",
            level=EscalationLevel.LEVEL_4,
            request_json=json.dumps({"level": "LEVEL_4"}),
            expires_at=(datetime.now(UTC) + timedelta(minutes=30)).isoformat(),
        )

        # Operator approves via the API (strategy_id-based lookup).
        ok = adapter.write_approval_response_by_strategy(
            strategy_id="strat_1",
            response_json=json.dumps({"approved": True, "action": "approve"}),
        )

        assert ok is True
        # The approval must land on the LIVE level (4), not the stale level (3).
        level_4_body = adapter.get_approval_response("td_1", EscalationLevel.LEVEL_4)
        assert level_4_body is not None
        assert json.loads(level_4_body)["approved"] is True


class TestCallbackPersistsRequest:
    @pytest.mark.asyncio
    async def test_request_row_is_created_for_operator(
        self, adapter: TeardownStateAdapter
    ) -> None:
        """The callback must write a pending approval row before polling so the
        API can discover it via get_latest_pending_approval."""
        callback = _make_approval_callback(_make_runner(), adapter)
        request = _make_request()

        async def _check_then_respond() -> None:
            await asyncio.sleep(0.02)
            pending = adapter.get_latest_pending_approval("strat_1")
            assert pending is not None, "approval request not persisted before polling started"
            assert pending["level"] == EscalationLevel.LEVEL_3.value
            payload = json.loads(pending["request_json"])
            assert payload["requested_at"]  # included for operator context
            assert payload["expires_at"]
            adapter.write_approval_response(
                teardown_id="td_1",
                level=EscalationLevel.LEVEL_3,
                response_json=json.dumps({"approved": True, "action": "approve"}),
            )

        writer = asyncio.create_task(_check_then_respond())
        response = await callback(request)
        await writer

        assert response.action == "approve"
