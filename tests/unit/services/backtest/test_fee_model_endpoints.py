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


# ---------------------------------------------------------------------------
# List endpoint
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_list_fee_models(client):
    """GET /fee-models returns a list of protocols."""
    resp = await client.get("/api/v1/fee-models")
    assert resp.status_code == 200
    data = resp.json()
    assert "protocols" in data
    assert isinstance(data["protocols"], list)
    assert len(data["protocols"]) >= 9


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
async def test_list_fee_models_summaries_have_required_fields(client):
    """Each summary has protocol, model_name, and supported_chains."""
    resp = await client.get("/api/v1/fee-models")
    assert resp.status_code == 200
    for proto in resp.json()["protocols"]:
        assert "protocol" in proto
        assert "model_name" in proto
        assert "supported_chains" in proto
        assert isinstance(proto["supported_chains"], list)
        assert len(proto["supported_chains"]) > 0 or proto["protocol"] == "hyperliquid"


@pytest.mark.asyncio
async def test_list_fee_models_model_names_are_class_names(client):
    """model_name should be the Python class name, not the registry key."""
    resp = await client.get("/api/v1/fee-models")
    assert resp.status_code == 200
    name_map = {p["protocol"]: p["model_name"] for p in resp.json()["protocols"]}
    assert name_map["uniswap_v3"] == "UniswapV3FeeModel"
    assert name_map["aave_v3"] == "AaveV3FeeModel"
    assert name_map["gmx"] == "GMXFeeModel"
    assert name_map["hyperliquid"] == "HyperliquidFeeModel"


# ---------------------------------------------------------------------------
# Detail endpoint — 404
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_fee_model_unknown_returns_404(client):
    """GET /fee-models/{protocol} for unknown protocol returns 404."""
    resp = await client.get("/api/v1/fee-models/nonexistent_protocol")
    assert resp.status_code == 404


# ---------------------------------------------------------------------------
# Detail endpoint — per-protocol
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_fee_model_uniswap_v3(client):
    """Uniswap V3 detail has fee tiers and slippage model."""
    resp = await client.get("/api/v1/fee-models/uniswap_v3")
    assert resp.status_code == 200
    data = resp.json()
    assert data["protocol"] == "uniswap_v3"
    assert data["model_name"] == "UniswapV3FeeModel"
    assert data["fee_tiers"] == [0.0001, 0.0005, 0.003, 0.01]
    assert data["default_fee"] == 0.003
    assert data["slippage_model"] == "sqrt_impact"
    for intent_type in ["SWAP", "LP_OPEN", "LP_CLOSE"]:
        assert intent_type in data["supported_intent_types"]
    assert "ethereum" in data["supported_chains"]
    assert "raw_config" in data


@pytest.mark.asyncio
async def test_get_fee_model_pancakeswap_v3(client):
    """PancakeSwap V3 detail has correct default fee (0.25%, not 0.3%)."""
    resp = await client.get("/api/v1/fee-models/pancakeswap_v3")
    assert resp.status_code == 200
    data = resp.json()
    assert data["protocol"] == "pancakeswap_v3"
    assert data["default_fee"] == 0.0025
    assert 0.0025 in data["fee_tiers"]


@pytest.mark.asyncio
async def test_get_fee_model_aerodrome(client):
    """Aerodrome detail has stable/volatile fee tiers."""
    resp = await client.get("/api/v1/fee-models/aerodrome")
    assert resp.status_code == 200
    data = resp.json()
    assert data["protocol"] == "aerodrome"
    assert 0.0001 in data["fee_tiers"]  # stable
    assert 0.003 in data["fee_tiers"]  # volatile
    assert "base" in data["supported_chains"]


@pytest.mark.asyncio
async def test_get_fee_model_curve(client):
    """Curve detail has stableswap slippage model."""
    resp = await client.get("/api/v1/fee-models/curve")
    assert resp.status_code == 200
    data = resp.json()
    assert data["slippage_model"] == "stableswap"


@pytest.mark.asyncio
async def test_get_fee_model_aave_v3(client):
    """Aave V3 detail has lending intent types."""
    resp = await client.get("/api/v1/fee-models/aave_v3")
    assert resp.status_code == 200
    data = resp.json()
    assert data["protocol"] == "aave_v3"
    assert data["slippage_model"] == "none"
    for intent_type in ["SUPPLY", "BORROW", "WITHDRAW", "REPAY"]:
        assert intent_type in data["supported_intent_types"]
    assert data["default_fee"] == 0.0001


@pytest.mark.asyncio
async def test_get_fee_model_morpho(client):
    """Morpho detail reflects fee-free operations."""
    resp = await client.get("/api/v1/fee-models/morpho")
    assert resp.status_code == 200
    data = resp.json()
    assert data["protocol"] == "morpho"
    assert data["default_fee"] == 0.0
    assert data["fee_tiers"] == []


@pytest.mark.asyncio
async def test_get_fee_model_compound_v3(client):
    """Compound V3 detail reflects fee-free operations."""
    resp = await client.get("/api/v1/fee-models/compound_v3")
    assert resp.status_code == 200
    data = resp.json()
    assert data["protocol"] == "compound_v3"
    assert data["default_fee"] == 0.0
    assert "SUPPLY" in data["supported_intent_types"]


@pytest.mark.asyncio
async def test_get_fee_model_gmx(client):
    """GMX detail has perp intent types."""
    resp = await client.get("/api/v1/fee-models/gmx")
    assert resp.status_code == 200
    data = resp.json()
    assert data["protocol"] == "gmx"
    assert "PERP_OPEN" in data["supported_intent_types"]
    assert "PERP_CLOSE" in data["supported_intent_types"]
    assert data["slippage_model"] == "price_impact"


@pytest.mark.asyncio
async def test_get_fee_model_hyperliquid(client):
    """Hyperliquid detail has volume-based fee tiers."""
    resp = await client.get("/api/v1/fee-models/hyperliquid")
    assert resp.status_code == 200
    data = resp.json()
    assert data["protocol"] == "hyperliquid"
    assert data["slippage_model"] == "orderbook"
    assert len(data["fee_tiers"]) >= 7  # 7 VIP tiers


@pytest.mark.asyncio
async def test_get_fee_model_raw_config_present(client):
    """Detail endpoint includes raw_config from model's to_dict()."""
    resp = await client.get("/api/v1/fee-models/uniswap_v3")
    assert resp.status_code == 200
    data = resp.json()
    assert "raw_config" in data
    assert isinstance(data["raw_config"], dict)
    assert data["raw_config"]["model_name"] == "uniswap_v3"


# ---------------------------------------------------------------------------
# Alias resolution
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_alias_resolution(client):
    """Aliases (e.g., 'uniswap', 'aave') resolve to the primary protocol."""
    for alias, expected in [("uniswap", "uniswap_v3"), ("aave", "aave_v3"), ("compound", "compound_v3")]:
        resp = await client.get(f"/api/v1/fee-models/{alias}")
        assert resp.status_code == 200, f"Alias '{alias}' should resolve"
        data = resp.json()
        assert data["protocol"] == expected, f"Alias '{alias}' -> '{expected}'"


# ---------------------------------------------------------------------------
# OpenAPI
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_fee_models_in_openapi(client):
    """Fee model endpoints appear in OpenAPI schema."""
    resp = await client.get("/openapi.json")
    assert resp.status_code == 200
    schema = resp.json()
    assert "/api/v1/fee-models" in schema["paths"]
    assert "/api/v1/fee-models/{protocol}" in schema["paths"]
