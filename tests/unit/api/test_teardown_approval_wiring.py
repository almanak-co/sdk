"""Tests that the teardown API's approval endpoint writes to the shared SQLite
channel the runner polls from.

This is the regression test for the VIB-2927 architectural bug where the API
and runner used disjoint channels (_teardown_state dict vs the SQLite
teardown_approvals table), so operator approvals never reached the runner.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from fastapi import HTTPException

from almanak.framework.api import teardown as teardown_api
from almanak.framework.teardown.models import EscalationLevel
from almanak.framework.teardown.state_manager import TeardownStateAdapter


@pytest.fixture
def tmp_adapter(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """Replace the API module's adapter singleton with one backed by tmp_path."""
    adapter = TeardownStateAdapter(db_path=tmp_path / "state.db")
    monkeypatch.setattr(teardown_api, "_teardown_adapter", adapter)

    # Force _get_teardown_adapter to return our tmp-backed instance by clearing
    # the global and letting the getter set it from our monkeypatch — safer
    # than relying on internal state.
    monkeypatch.setattr(teardown_api, "_get_teardown_adapter", lambda: adapter)
    return adapter


def _seed_pending_approval(adapter: TeardownStateAdapter, deployment_id: str) -> str:
    """Seed a pending approval as if a runner callback had written one."""
    teardown_id = "td_approve_test"
    adapter.create_approval_request(
        teardown_id=teardown_id,
        deployment_id=deployment_id,
        level=EscalationLevel.LEVEL_3,
        request_json=json.dumps({"level": "LEVEL_3", "current_slippage": "0.05"}),
        expires_at=(datetime.now(UTC) + timedelta(minutes=30)).isoformat(),
    )
    return teardown_id


def _seed_in_memory_teardown(
    deployment_id: str,
    *,
    status: str = "paused",
    approval_needed: dict | None = None,
) -> dict:
    teardown = {
        "teardown_id": f"td_{deployment_id}",
        "deployment_id": deployment_id,
        "status": status,
        "approval_needed": {"level": "LEVEL_3"} if approval_needed is None else approval_needed,
    }
    teardown_api._teardown_state.set_teardown(deployment_id, teardown)
    return teardown


class TestApproveEscalationWritesToSqlite:
    @pytest.mark.asyncio
    async def test_approve_action_writes_sqlite_response(self, tmp_adapter: TeardownStateAdapter) -> None:
        """approve_escalation must land in the SQLite channel the runner polls."""
        deployment_id = "runner_initiated_strat"
        teardown_id = _seed_pending_approval(tmp_adapter, deployment_id)
        # Runner-initiated: no in-memory dict entry exists.
        teardown_api._teardown_state.remove_teardown(deployment_id)

        request = teardown_api.EscalationApprovalRequest(action="approve")
        response = await teardown_api.approve_escalation(
            deployment_id=deployment_id,
            request=request,
            api_key="test-key",
        )

        assert response.success is True
        # Runner polling for this level must see the approval.
        body = tmp_adapter.get_approval_response(teardown_id, EscalationLevel.LEVEL_3)
        assert body is not None
        payload = json.loads(body)
        assert payload["approved"] is True
        assert payload["action"] == "approve"

    @pytest.mark.asyncio
    async def test_wait_and_escalate_action_writes_sqlite_response(self, tmp_adapter: TeardownStateAdapter) -> None:
        deployment_id = "runner_strat_wait"
        teardown_id = _seed_pending_approval(tmp_adapter, deployment_id)
        teardown_api._teardown_state.remove_teardown(deployment_id)

        response = await teardown_api.approve_escalation(
            deployment_id=deployment_id,
            request=teardown_api.EscalationApprovalRequest(action="wait_and_escalate"),
            api_key="test-key",
        )

        assert response.success is True
        body = tmp_adapter.get_approval_response(teardown_id, EscalationLevel.LEVEL_3)
        assert body is not None
        assert json.loads(body)["action"] == "wait_and_escalate"

    @pytest.mark.asyncio
    async def test_cancel_action_writes_sqlite_response(self, tmp_adapter: TeardownStateAdapter) -> None:
        deployment_id = "runner_strat_cancel"
        teardown_id = _seed_pending_approval(tmp_adapter, deployment_id)
        teardown_api._teardown_state.remove_teardown(deployment_id)

        response = await teardown_api.approve_escalation(
            deployment_id=deployment_id,
            request=teardown_api.EscalationApprovalRequest(action="cancel"),
            api_key="test-key",
        )

        assert response.success is True
        body = tmp_adapter.get_approval_response(teardown_id, EscalationLevel.LEVEL_3)
        assert body is not None
        assert json.loads(body)["action"] == "cancel"

    @pytest.mark.asyncio
    async def test_404_when_no_pending_approval_on_either_channel(self, tmp_adapter: TeardownStateAdapter) -> None:
        """404 if NEITHER in-memory nor SQLite has a pending approval."""
        teardown_api._teardown_state.remove_teardown("ghost_strategy")

        with pytest.raises(HTTPException) as exc_info:
            await teardown_api.approve_escalation(
                deployment_id="ghost_strategy",
                request=teardown_api.EscalationApprovalRequest(action="approve"),
                api_key="test-key",
            )
        assert exc_info.value.status_code == 404

    @pytest.mark.asyncio
    async def test_409_when_sqlite_approval_resolved_mid_request(
        self, tmp_adapter: TeardownStateAdapter, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Simulate a race: get_latest_pending_approval succeeds but the row is
        gone by the time write_approval_response_by_strategy runs. Surface as
        409 rather than silently dropping the operator's decision."""
        deployment_id = "race_strat"
        teardown_api._teardown_state.remove_teardown(deployment_id)

        # Pretend there was a pending approval at lookup time.
        monkeypatch.setattr(
            tmp_adapter,
            "get_latest_pending_approval",
            lambda sid: {
                "teardown_id": "td_race",
                "level": "LEVEL_3",
                "deployment_id": deployment_id,
                "request_json": "{}",
                "created_at": "",
                "expires_at": "",
            },
        )
        # ...but by the time we try to write, the row doesn't exist anymore.
        monkeypatch.setattr(
            tmp_adapter,
            "write_approval_response_by_strategy",
            lambda **_kw: False,
        )

        with pytest.raises(HTTPException) as exc_info:
            await teardown_api.approve_escalation(
                deployment_id=deployment_id,
                request=teardown_api.EscalationApprovalRequest(action="approve"),
                api_key="test-key",
            )
        assert exc_info.value.status_code == 409

    @pytest.mark.asyncio
    async def test_rejects_in_memory_teardown_that_is_not_paused(self, tmp_adapter: TeardownStateAdapter) -> None:
        deployment_id = "executing_strat"
        _seed_in_memory_teardown(deployment_id, status="executing")

        with pytest.raises(HTTPException) as exc_info:
            await teardown_api.approve_escalation(
                deployment_id=deployment_id,
                request=teardown_api.EscalationApprovalRequest(action="approve"),
                api_key="test-key",
            )

        assert exc_info.value.status_code == 400
        assert exc_info.value.detail == "Teardown is not paused (status: executing)"

    @pytest.mark.asyncio
    async def test_rejects_in_memory_teardown_without_pending_request(self, tmp_adapter: TeardownStateAdapter) -> None:
        deployment_id = "paused_without_request"
        _seed_in_memory_teardown(deployment_id, approval_needed={})

        with pytest.raises(HTTPException) as exc_info:
            await teardown_api.approve_escalation(
                deployment_id=deployment_id,
                request=teardown_api.EscalationApprovalRequest(action="approve"),
                api_key="test-key",
            )

        assert exc_info.value.status_code == 400
        assert exc_info.value.detail == "No approval request pending"

    @pytest.mark.parametrize(
        ("action", "approved_slippage", "expected_status", "expected_message"),
        [
            ("approve", 0.06, "executing", "Slippage approved. Continuing teardown."),
            (
                "wait_and_escalate",
                None,
                "waiting_retry",
                "Operator declined current level; advancing to next escalation level.",
            ),
            ("cancel", None, "cancelled", "Teardown cancelled by operator."),
        ],
    )
    @pytest.mark.asyncio
    async def test_in_memory_actions_preserve_lifecycle_and_audit_contract(
        self,
        tmp_adapter: TeardownStateAdapter,
        monkeypatch: pytest.MonkeyPatch,
        action: str,
        approved_slippage: float | None,
        expected_status: str,
        expected_message: str,
    ) -> None:
        deployment_id = f"api_{action}"
        teardown = _seed_in_memory_teardown(deployment_id)
        audit = MagicMock()
        monkeypatch.setattr(teardown_api, "emit_audit_event", audit)

        response = await teardown_api.approve_escalation(
            deployment_id=deployment_id,
            request=teardown_api.EscalationApprovalRequest(
                action=action,
                approved_slippage=approved_slippage,
            ),
            api_key="test-key",
        )

        assert response.model_dump() == {
            "success": True,
            "message": expected_message,
            "teardown_id": teardown["teardown_id"],
            "new_status": expected_status,
        }
        stored = teardown_api._teardown_state.get_teardown(deployment_id)
        assert stored is teardown
        assert stored["status"] == expected_status
        if action in {"approve", "cancel"}:
            assert stored["approval_needed"] is None
        else:
            assert stored["approval_needed"] == {"level": "LEVEL_3"}
        if approved_slippage is not None:
            assert stored["approved_slippage"] == approved_slippage
        audit.assert_called_once_with(
            deployment_id=deployment_id,
            action="TEARDOWN_ESCALATION_RESPONSE",
            details={
                "teardown_id": teardown["teardown_id"],
                "action": action,
                "approved_slippage": approved_slippage,
                "channel": "in_memory",
            },
            api_key="test-key",
        )
