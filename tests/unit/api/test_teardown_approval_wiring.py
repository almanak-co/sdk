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

import pytest

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
    async def test_wait_and_escalate_action_writes_sqlite_response(
        self, tmp_adapter: TeardownStateAdapter
    ) -> None:
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
    async def test_404_when_no_pending_approval_on_either_channel(
        self, tmp_adapter: TeardownStateAdapter
    ) -> None:
        """404 if NEITHER in-memory nor SQLite has a pending approval."""
        from fastapi import HTTPException

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
        from fastapi import HTTPException

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
