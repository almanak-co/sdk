"""Tests for BacktestService paper trading endpoints."""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest


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
async def test_poll_unknown_session_returns_404(client):
    """GET /paper-trade/{id} for unknown session returns 404."""
    resp = await client.get("/api/v1/paper-trade/pt_nonexistent")
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_poll_paper_session(client):
    """Submit then poll — session should exist."""
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

        # Stop it
        await client.delete(f"/api/v1/paper-trade/{session_id}")
        # Try again
        resp = await client.delete(f"/api/v1/paper-trade/{session_id}")
        assert resp.status_code == 409


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


@pytest.mark.asyncio
async def test_delete_unknown_session_returns_404(client):
    """DELETE /paper-trade/{id} for unknown session returns 404."""
    resp = await client.delete("/api/v1/paper-trade/pt_nonexistent")
    assert resp.status_code == 404
