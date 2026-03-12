"""Tests for BacktestService health endpoint."""

from __future__ import annotations

import pytest


@pytest.mark.asyncio
async def test_health_returns_ok(client):
    resp = await client.get("/api/v1/health")
    assert resp.status_code == 200
    data = resp.json()
    assert data["status"] == "ok"
    assert data["version"] == "0.1.0"
    assert "active_backtest_jobs" in data
    assert "active_paper_sessions" in data
    assert "uptime_seconds" in data


@pytest.mark.asyncio
async def test_health_shows_zero_jobs_initially(client):
    resp = await client.get("/api/v1/health")
    data = resp.json()
    assert data["active_backtest_jobs"] == 0
    assert data["active_paper_sessions"] == 0


@pytest.mark.asyncio
async def test_openapi_docs_available(client):
    resp = await client.get("/docs")
    assert resp.status_code == 200


@pytest.mark.asyncio
async def test_openapi_json_available(client):
    resp = await client.get("/openapi.json")
    assert resp.status_code == 200
    schema = resp.json()
    assert schema["info"]["title"] == "Almanak BacktestService"
    assert "/api/v1/health" in schema["paths"]
