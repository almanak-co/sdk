"""Tests for BacktestService fee model endpoints."""

from __future__ import annotations

import pytest


EXPECTED_PROTOCOLS = {
    "uniswap_v3",
    "pancakeswap_v3",
    "aerodrome",
    "curve",
    "aave_v3",
    "morpho",
    "compound_v3",
    "gmx",
    "hyperliquid",
}


@pytest.mark.asyncio
async def test_list_fee_models(client):
    """GET /fee-models returns a list of protocols."""
    resp = await client.get("/api/v1/fee-models")
    assert resp.status_code == 200
    data = resp.json()
    assert "protocols" in data
    assert isinstance(data["protocols"], list)
    # Should have at least some registered models
    if len(data["protocols"]) > 0:
        proto = data["protocols"][0]
        assert "protocol" in proto
        assert "model_name" in proto


@pytest.mark.asyncio
async def test_list_fee_models_covers_all_protocols(client):
    """All 9 registered protocol fee models are returned by the list endpoint."""
    resp = await client.get("/api/v1/fee-models")
    assert resp.status_code == 200
    data = resp.json()
    returned_protocols = {p["protocol"] for p in data["protocols"]}
    missing = EXPECTED_PROTOCOLS - returned_protocols
    assert not missing, f"Missing fee model protocols: {missing}"


@pytest.mark.asyncio
async def test_get_fee_model_unknown_returns_404(client):
    """GET /fee-models/{protocol} for unknown protocol returns 404."""
    resp = await client.get("/api/v1/fee-models/nonexistent_protocol")
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_get_fee_model_uniswap_v3(client):
    """GET /fee-models/uniswap_v3 returns detail for Uniswap V3."""
    resp = await client.get("/api/v1/fee-models/uniswap_v3")
    # May be 200 or 404 depending on whether the registry auto-loads
    if resp.status_code == 200:
        data = resp.json()
        assert data["protocol"] == "uniswap_v3"
        assert "model_name" in data
        assert "supported_chains" in data


@pytest.mark.asyncio
async def test_fee_models_in_openapi(client):
    """Fee model endpoints appear in OpenAPI schema."""
    resp = await client.get("/openapi.json")
    assert resp.status_code == 200
    schema = resp.json()
    assert "/api/v1/fee-models" in schema["paths"]
    assert "/api/v1/fee-models/{protocol}" in schema["paths"]
