"""Tests for BacktestService backtest endpoints."""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest


VALID_BACKTEST_REQUEST = {
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
    "timeframe": {
        "start": "2025-01-01",
        "end": "2025-01-08",
    },
}


# ---------------------------------------------------------------------------
# Submit endpoint
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_submit_backtest_returns_202(client):
    """POST /backtest returns 202 with a job_id."""
    resp = await client.post("/api/v1/backtest", json=VALID_BACKTEST_REQUEST)
    assert resp.status_code == 202
    data = resp.json()
    assert "job_id" in data
    assert data["status"] == "pending"
    assert data["job_id"].startswith("bt_")


@pytest.mark.asyncio
async def test_submit_backtest_invalid_spec(client):
    """POST /backtest with missing fields returns 422."""
    resp = await client.post("/api/v1/backtest", json={"strategy_spec": {}})
    assert resp.status_code == 422


@pytest.mark.asyncio
async def test_submit_backtest_no_strategy(client):
    """POST /backtest with neither strategy_name nor strategy_spec returns 422."""
    resp = await client.post(
        "/api/v1/backtest",
        json={"timeframe": {"start": "2025-01-01", "end": "2025-01-08"}},
    )
    assert resp.status_code == 422


@pytest.mark.asyncio
async def test_submit_backtest_created_at_present(client):
    """Submit response includes created_at timestamp."""
    resp = await client.post("/api/v1/backtest", json=VALID_BACKTEST_REQUEST)
    data = resp.json()
    assert "created_at" in data
    assert data["created_at"] is not None


# ---------------------------------------------------------------------------
# Poll endpoint
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_poll_unknown_job_returns_404(client):
    """GET /backtest/{id} for unknown job returns 404."""
    resp = await client.get("/api/v1/backtest/bt_nonexistent")
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_poll_pending_job(client):
    """Submit then immediately poll — job should be pending (runner is mocked)."""
    with patch(
        "almanak.services.backtest.routers.backtest.run_backtest_job",
        new_callable=AsyncMock,
    ):
        submit_resp = await client.post("/api/v1/backtest", json=VALID_BACKTEST_REQUEST)
        job_id = submit_resp.json()["job_id"]

        poll_resp = await client.get(f"/api/v1/backtest/{job_id}")
        assert poll_resp.status_code == 200
        data = poll_resp.json()
        assert data["job_id"] == job_id
        assert data["status"] in ("pending", "running")
        assert "progress" in data


@pytest.mark.asyncio
async def test_poll_has_structured_progress(client):
    """Poll response includes percent, current_step, eta_seconds."""
    submit_resp = await client.post("/api/v1/backtest", json=VALID_BACKTEST_REQUEST)
    job_id = submit_resp.json()["job_id"]

    poll_resp = await client.get(f"/api/v1/backtest/{job_id}")
    progress = poll_resp.json()["progress"]
    assert "percent" in progress
    assert "current_step" in progress
    assert "eta_seconds" in progress


# ---------------------------------------------------------------------------
# Capacity / throttling
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_job_manager_capacity_limit(client):
    """Submitting more than max_concurrent_jobs returns 429."""
    # Mock runner so jobs stay pending (don't fail and free their slots)
    with patch(
        "almanak.services.backtest.routers.backtest.run_backtest_job",
        new_callable=AsyncMock,
    ):
        # Default max is 4 — submit 5
        for _ in range(4):
            resp = await client.post("/api/v1/backtest", json=VALID_BACKTEST_REQUEST)
            assert resp.status_code == 202

        resp = await client.post("/api/v1/backtest", json=VALID_BACKTEST_REQUEST)
        assert resp.status_code == 429


# ---------------------------------------------------------------------------
# Quick backtest
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_quick_backtest_validation_error(client):
    """POST /backtest/quick with missing fields returns 422."""
    resp = await client.post("/api/v1/backtest/quick", json={"strategy_spec": {}})
    assert resp.status_code == 422


@pytest.mark.asyncio
async def test_quick_backtest_named_strategy_requires_chain(client):
    """POST /backtest/quick with strategy_name but no chain returns 422."""
    resp = await client.post(
        "/api/v1/backtest/quick",
        json={"strategy_name": "demo_uniswap_rsi", "tokens": ["WETH", "USDC"]},
    )
    assert resp.status_code == 422


# ---------------------------------------------------------------------------
# List strategies endpoint
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_list_strategies_returns_list(client):
    """GET /strategies returns a list with a count."""
    resp = await client.get("/api/v1/strategies")
    assert resp.status_code == 200
    data = resp.json()
    assert "strategies" in data
    assert "count" in data
    assert isinstance(data["strategies"], list)
    assert data["count"] == len(data["strategies"])


@pytest.mark.asyncio
async def test_list_strategies_sorted(client):
    """GET /strategies returns strategies in sorted order."""
    resp = await client.get("/api/v1/strategies")
    data = resp.json()
    strategies = data["strategies"]
    assert strategies == sorted(strategies)


# ---------------------------------------------------------------------------
# OpenAPI schema
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_backtest_endpoints_in_openapi(client):
    """All backtest endpoints appear in OpenAPI schema."""
    resp = await client.get("/openapi.json")
    assert resp.status_code == 200
    schema = resp.json()
    paths = schema["paths"]
    assert "/api/v1/backtest" in paths
    assert "/api/v1/backtest/{job_id}" in paths
    assert "/api/v1/backtest/quick" in paths
    assert "/api/v1/strategies" in paths
