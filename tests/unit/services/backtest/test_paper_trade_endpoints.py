"""Tests for BacktestService paper trading endpoints."""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest

from almanak.services.backtest.models import PaperTradeLiveMetrics, PaperTradeSessionStatus
from almanak.services.backtest.services.paper_trade_manager import PaperTradeManager


VALID_PAPER_TRADE_REQUEST = {
    "strategy_spec": {
        "protocol": "uniswap_v3",
        "chain": "arbitrum",
        "action": "swap",
        "parameters": {
            "from_token": "USDC",
            "to_token": "WETH",
            "amount_usd": "1000",
        },
    },
    "chain": "arbitrum",
    "duration_hours": 24,
    "initial_capital_usd": 10000,
    "tick_interval_seconds": 60,
}


# ---------------------------------------------------------------------------
# Start endpoint
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_start_paper_trade_returns_202(client):
    """POST /paper-trade returns 202 with a session_id."""
    resp = await client.post("/api/v1/paper-trade", json=VALID_PAPER_TRADE_REQUEST)
    assert resp.status_code == 202
    data = resp.json()
    assert "session_id" in data
    assert data["session_id"].startswith("pt_")
    assert data["status"] == "starting"


@pytest.mark.asyncio
async def test_start_paper_trade_invalid_request(client):
    """POST /paper-trade with missing fields returns 422."""
    resp = await client.post("/api/v1/paper-trade", json={"strategy_spec": {}})
    assert resp.status_code == 422


@pytest.mark.asyncio
async def test_start_paper_trade_has_created_at(client):
    """Start response includes created_at timestamp."""
    resp = await client.post("/api/v1/paper-trade", json=VALID_PAPER_TRADE_REQUEST)
    data = resp.json()
    assert "created_at" in data
    assert data["created_at"] is not None


# ---------------------------------------------------------------------------
# Poll endpoint
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_poll_unknown_session_returns_404(client):
    """GET /paper-trade/{id} for unknown session returns 404."""
    resp = await client.get("/api/v1/paper-trade/pt_nonexistent")
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_poll_paper_session(client):
    """Submit then poll — session should exist with progress and metrics."""
    with patch(
        "almanak.services.backtest.routers.paper_trade._run_paper_session",
        new_callable=AsyncMock,
    ):
        submit_resp = await client.post("/api/v1/paper-trade", json=VALID_PAPER_TRADE_REQUEST)
        session_id = submit_resp.json()["session_id"]

        poll_resp = await client.get(f"/api/v1/paper-trade/{session_id}")
        assert poll_resp.status_code == 200
        data = poll_resp.json()
        assert data["session_id"] == session_id
        assert data["status"] in ("starting", "running")
        assert "progress" in data
        assert "metrics" in data


@pytest.mark.asyncio
async def test_poll_session_has_live_metrics_fields(client):
    """Poll response metrics include pnl_usd, total_trades, gas_cost_usd."""
    with patch(
        "almanak.services.backtest.routers.paper_trade._run_paper_session",
        new_callable=AsyncMock,
    ):
        submit_resp = await client.post("/api/v1/paper-trade", json=VALID_PAPER_TRADE_REQUEST)
        session_id = submit_resp.json()["session_id"]

        poll_resp = await client.get(f"/api/v1/paper-trade/{session_id}")
        metrics = poll_resp.json()["metrics"]
        assert "pnl_usd" in metrics
        assert "total_trades" in metrics
        assert "gas_cost_usd" in metrics


# ---------------------------------------------------------------------------
# Stop endpoint
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_stop_paper_session(client):
    """DELETE /paper-trade/{id} stops a starting/running session."""
    with patch(
        "almanak.services.backtest.routers.paper_trade._run_paper_session",
        new_callable=AsyncMock,
    ):
        submit_resp = await client.post("/api/v1/paper-trade", json=VALID_PAPER_TRADE_REQUEST)
        session_id = submit_resp.json()["session_id"]

        stop_resp = await client.delete(f"/api/v1/paper-trade/{session_id}")
        assert stop_resp.status_code == 200
        data = stop_resp.json()
        assert data["status"] == "stopped"
        assert data["stopped_at"] is not None


@pytest.mark.asyncio
async def test_stop_already_stopped_returns_409(client):
    """DELETE on already-stopped session returns 409."""
    with patch(
        "almanak.services.backtest.routers.paper_trade._run_paper_session",
        new_callable=AsyncMock,
    ):
        submit_resp = await client.post("/api/v1/paper-trade", json=VALID_PAPER_TRADE_REQUEST)
        session_id = submit_resp.json()["session_id"]

        await client.delete(f"/api/v1/paper-trade/{session_id}")
        resp = await client.delete(f"/api/v1/paper-trade/{session_id}")
        assert resp.status_code == 409


@pytest.mark.asyncio
async def test_delete_unknown_session_returns_404(client):
    """DELETE /paper-trade/{id} for unknown session returns 404."""
    resp = await client.delete("/api/v1/paper-trade/pt_nonexistent")
    assert resp.status_code == 404


# ---------------------------------------------------------------------------
# Capacity / throttling
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_paper_trade_capacity_limit(client):
    """Submitting more than max_concurrent_paper_sessions returns 429."""
    with patch(
        "almanak.services.backtest.routers.paper_trade._run_paper_session",
        new_callable=AsyncMock,
    ):
        # Default max is 2
        for _ in range(2):
            resp = await client.post("/api/v1/paper-trade", json=VALID_PAPER_TRADE_REQUEST)
            assert resp.status_code == 202

        resp = await client.post("/api/v1/paper-trade", json=VALID_PAPER_TRADE_REQUEST)
        assert resp.status_code == 429


# ---------------------------------------------------------------------------
# OpenAPI schema
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_paper_trade_endpoints_in_openapi(client):
    """Paper trade endpoints appear in OpenAPI schema."""
    resp = await client.get("/openapi.json")
    assert resp.status_code == 200
    schema = resp.json()
    paths = schema["paths"]
    assert "/api/v1/paper-trade" in paths
    assert "/api/v1/paper-trade/{session_id}" in paths


# ---------------------------------------------------------------------------
# PaperTradeManager unit tests
# ---------------------------------------------------------------------------


class TestPaperTradeManager:
    """Unit tests for the in-memory paper trading session tracker."""

    def test_create_and_get_session(self, tmp_path):
        mgr = PaperTradeManager(max_sessions=4, state_base_dir=str(tmp_path))
        session_id = mgr.create_session("test_strategy", "arbitrum")
        assert session_id.startswith("pt_")
        session = mgr.get_session(session_id)
        assert session is not None
        assert session.status == PaperTradeSessionStatus.STARTING
        assert session.strategy_id == "test_strategy"
        assert session.chain == "arbitrum"

    def test_lifecycle_starting_running_stopped(self, tmp_path):
        mgr = PaperTradeManager(max_sessions=4, state_base_dir=str(tmp_path))
        sid = mgr.create_session("test", "ethereum")

        mgr.mark_running(sid, pid=12345)
        session = mgr.get_session(sid)
        assert session.status == PaperTradeSessionStatus.RUNNING
        assert session.pid == 12345

        mgr.mark_stopped(sid)
        session = mgr.get_session(sid)
        assert session.status == PaperTradeSessionStatus.STOPPED
        assert session.stopped_at is not None

    def test_lifecycle_starting_failed(self, tmp_path):
        mgr = PaperTradeManager(max_sessions=4, state_base_dir=str(tmp_path))
        sid = mgr.create_session("test", "base")

        mgr.mark_failed(sid, "Something went wrong")
        session = mgr.get_session(sid)
        assert session.status == PaperTradeSessionStatus.FAILED
        assert session.error == "Something went wrong"
        assert session.stopped_at is not None

    def test_max_sessions_enforced(self, tmp_path):
        mgr = PaperTradeManager(max_sessions=2, state_base_dir=str(tmp_path))
        mgr.create_session("s1", "arbitrum")
        mgr.create_session("s2", "arbitrum")
        with pytest.raises(RuntimeError, match="Max concurrent"):
            mgr.create_session("s3", "arbitrum")

    def test_stopped_sessions_dont_count_as_active(self, tmp_path):
        mgr = PaperTradeManager(max_sessions=2, state_base_dir=str(tmp_path))
        s1 = mgr.create_session("s1", "arbitrum")
        mgr.create_session("s2", "arbitrum")

        mgr.mark_stopped(s1)
        # Should succeed — s1 freed a slot
        s3 = mgr.create_session("s3", "arbitrum")
        assert s3.startswith("pt_")

    def test_active_count(self, tmp_path):
        mgr = PaperTradeManager(max_sessions=4, state_base_dir=str(tmp_path))
        assert mgr.active_count == 0
        s1 = mgr.create_session("test", "arbitrum")
        assert mgr.active_count == 1
        mgr.mark_running(s1, pid=100)
        assert mgr.active_count == 1
        mgr.mark_stopped(s1)
        assert mgr.active_count == 0

    def test_update_progress(self, tmp_path):
        mgr = PaperTradeManager(max_sessions=4, state_base_dir=str(tmp_path))
        sid = mgr.create_session("test", "arbitrum")
        mgr.update_progress(sid, 42.0, "Tick 42/100", eta_seconds=58)

        session = mgr.get_session(sid)
        assert session.progress.percent == 42.0
        assert session.progress.current_step == "Tick 42/100"
        assert session.progress.eta_seconds == 58

    def test_update_metrics(self, tmp_path):
        mgr = PaperTradeManager(max_sessions=4, state_base_dir=str(tmp_path))
        sid = mgr.create_session("test", "arbitrum")
        metrics = PaperTradeLiveMetrics(pnl_usd="150.50", total_trades=5, gas_cost_usd="2.30")
        mgr.update_metrics(sid, metrics)

        session = mgr.get_session(sid)
        assert session.metrics.pnl_usd == "150.50"
        assert session.metrics.total_trades == 5
        assert session.metrics.gas_cost_usd == "2.30"

    def test_get_nonexistent_session_returns_none(self, tmp_path):
        mgr = PaperTradeManager(max_sessions=4, state_base_dir=str(tmp_path))
        assert mgr.get_session("pt_does_not_exist") is None

    def test_eviction_of_stopped_sessions(self, tmp_path):
        mgr = PaperTradeManager(max_sessions=10, max_total=3, state_base_dir=str(tmp_path))
        ids = []
        for _ in range(3):
            sid = mgr.create_session("test", "arbitrum")
            mgr.mark_stopped(sid)
            ids.append(sid)

        # All 3 exist
        assert all(mgr.get_session(sid) is not None for sid in ids)

        # Creating a 4th triggers eviction of the oldest
        s4 = mgr.create_session("test", "arbitrum")
        assert mgr.get_session(s4) is not None
        assert mgr.get_session(ids[0]) is None  # oldest evicted
