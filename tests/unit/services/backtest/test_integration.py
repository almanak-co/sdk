"""Integration tests: full submit -> poll -> result lifecycle with mocked backtester.

These tests exercise the real HTTP endpoints and the real backtest_runner code,
with only the PnLBacktester.backtest() call mocked to return a realistic
BacktestResult. This validates the entire data flow from HTTP request to
serialized JSON response.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from almanak.framework.backtesting.models import (
    BacktestEngine,
    BacktestMetrics,
    BacktestResult,
    EquityPoint,
    TradeRecord,
)


def _make_backtest_result(
    *,
    net_pnl: str = "234.56",
    sharpe: str = "1.82",
    trades_count: int = 12,
) -> BacktestResult:
    """Create a realistic BacktestResult for test assertions."""
    metrics = BacktestMetrics(
        total_pnl_usd=Decimal("250.00"),
        net_pnl_usd=Decimal(net_pnl),
        sharpe_ratio=Decimal(sharpe),
        max_drawdown_pct=Decimal("0.043"),
        win_rate=Decimal("0.583"),
        total_trades=trades_count,
        winning_trades=7,
        losing_trades=5,
        profit_factor=Decimal("2.1"),
        total_return_pct=Decimal("0.0235"),
        annualized_return_pct=Decimal("0.42"),
        total_fees_usd=Decimal("15.44"),
        total_slippage_usd=Decimal("3.20"),
        total_gas_usd=Decimal("1.80"),
        volatility=Decimal("0.18"),
        sortino_ratio=Decimal("2.3"),
        calmar_ratio=Decimal("5.5"),
        avg_trade_pnl_usd=Decimal("19.55"),
        largest_win_usd=Decimal("80.00"),
        largest_loss_usd=Decimal("-25.00"),
        avg_win_usd=Decimal("35.00"),
        avg_loss_usd=Decimal("-10.00"),
        total_fees_earned_usd=Decimal("0"),
        total_funding_paid=Decimal("0"),
        total_funding_received=Decimal("0"),
        total_interest_earned=Decimal("0"),
        total_interest_paid=Decimal("0"),
        realized_pnl=Decimal("234.56"),
        unrealized_pnl=Decimal("0"),
        pnl_by_protocol={"uniswap_v3": Decimal("234.56")},
        pnl_by_intent_type={"SWAP": Decimal("234.56")},
        pnl_by_asset={"WETH": Decimal("234.56")},
    )
    trades = [
        TradeRecord(
            timestamp=datetime(2025, 1, 2, tzinfo=UTC),
            intent_type="SWAP",
            executed_price=Decimal("2500.00"),
            amount_usd=Decimal("1000"),
            fee_usd=Decimal("3.00"),
            slippage_usd=Decimal("0.50"),
            gas_cost_usd=Decimal("0.30"),
            pnl_usd=Decimal("50.00"),
            success=True,
        ),
    ]
    equity_curve = [
        EquityPoint(timestamp=datetime(2025, 1, 1, tzinfo=UTC), value_usd=Decimal("10000")),
        EquityPoint(timestamp=datetime(2025, 1, 8, tzinfo=UTC), value_usd=Decimal("10234.56")),
    ]
    return BacktestResult(
        engine=BacktestEngine.PNL,
        strategy_id="spec_uniswap_v3_swap_arbitrum",
        backtest_id="test-integration-001",
        start_time=datetime(2025, 1, 1, tzinfo=UTC),
        end_time=datetime(2025, 1, 8, tzinfo=UTC),
        run_started_at=datetime(2025, 1, 1, tzinfo=UTC),
        run_ended_at=datetime(2025, 1, 1, tzinfo=UTC),
        run_duration_seconds=2.5,
        metrics=metrics,
        trades=trades,
        equity_curve=equity_curve,
        initial_capital_usd=Decimal("10000"),
        final_capital_usd=Decimal("10234.56"),
        chain="arbitrum",
        config={},
        data_quality=None,
        preflight_report=None,
        config_hash=None,
        parameter_sources=None,
        accuracy_estimate=None,
        data_coverage_metrics=None,
        monte_carlo_results=None,
        walk_forward_results=None,
        crisis_results=None,
        gas_price_summary=None,
    )


VALID_SWAP_REQUEST = {
    "strategy_spec": {
        "protocol": "uniswap_v3",
        "chain": "arbitrum",
        "action": "swap",
        "parameters": {"from_token": "USDC", "to_token": "WETH", "amount_usd": "1000"},
    },
    "timeframe": {"start": "2025-01-01", "end": "2025-01-08"},
}

VALID_LP_REQUEST = {
    "strategy_spec": {
        "protocol": "uniswap_v3",
        "chain": "arbitrum",
        "action": "provide_liquidity",
        "parameters": {
            "pool": "WETH/USDC",
            "amount0": "1",
            "amount1": "2000",
            "range_lower": "1800",
            "range_upper": "2200",
        },
    },
    "timeframe": {"start": "2025-01-01", "end": "2025-02-01"},
}

VALID_LEND_REQUEST = {
    "strategy_spec": {
        "protocol": "aave_v3",
        "chain": "ethereum",
        "action": "lend",
        "parameters": {"token": "USDC", "amount": "5000"},
    },
    "timeframe": {"start": "2025-01-01", "end": "2025-03-01"},
}


def _mock_backtester(result: BacktestResult):
    """Create a mock PnLBacktester that returns the given result."""
    mock_bt = MagicMock()
    mock_bt.backtest = AsyncMock(return_value=result)
    mock_bt.close = AsyncMock()
    return mock_bt


# ---------------------------------------------------------------------------
# Submit -> poll -> complete lifecycle
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_full_lifecycle_swap(client):
    """Submit a swap backtest, wait for background task, poll complete result."""
    result = _make_backtest_result()

    with patch(
        "almanak.services.backtest.services.backtest_runner.create_backtester",
        return_value=_mock_backtester(result),
    ):
        # Submit
        submit_resp = await client.post("/api/v1/backtest", json=VALID_SWAP_REQUEST)
        assert submit_resp.status_code == 202
        job_id = submit_resp.json()["job_id"]

        # Give the background task time to complete
        await asyncio.sleep(0.5)

        # Poll
        poll_resp = await client.get(f"/api/v1/backtest/{job_id}")
        assert poll_resp.status_code == 200
        data = poll_resp.json()

        assert data["status"] == "complete"
        assert data["result"] is not None
        assert data["completed_at"] is not None

        # Verify full metrics
        m = data["result"]["metrics"]
        assert m["net_pnl_usd"] == "234.56"
        assert m["sharpe_ratio"] == "1.82"
        assert m["total_trades"] == 12
        assert m["winning_trades"] == 7
        assert m["losing_trades"] == 5
        assert m["total_fees_usd"] == "15.44"
        assert m["total_slippage_usd"] == "3.20"
        assert m["total_gas_usd"] == "1.80"
        assert m["volatility"] == "0.18"
        assert m["realized_pnl"] == "234.56"
        assert m["pnl_by_protocol"] == {"uniswap_v3": "234.56"}
        assert m["pnl_by_intent_type"] == {"SWAP": "234.56"}
        assert m["pnl_by_asset"] == {"WETH": "234.56"}

        # Verify equity curve
        assert len(data["result"]["equity_curve"]) == 2
        assert data["result"]["equity_curve"][0]["value_usd"] == "10000"

        # Verify trades
        assert len(data["result"]["trades"]) == 1
        assert data["result"]["trades"][0]["intent_type"] == "SWAP"
        assert data["result"]["trades"][0]["fee_usd"] == "3.00"

        # Verify duration
        assert data["result"]["duration_seconds"] == 2.5


@pytest.mark.asyncio
async def test_full_lifecycle_lp(client):
    """Submit an LP backtest, verify it completes with correct strategy spec."""
    result = _make_backtest_result(net_pnl="100.00", trades_count=5)

    with patch(
        "almanak.services.backtest.services.backtest_runner.create_backtester",
        return_value=_mock_backtester(result),
    ):
        submit_resp = await client.post("/api/v1/backtest", json=VALID_LP_REQUEST)
        assert submit_resp.status_code == 202
        job_id = submit_resp.json()["job_id"]

        await asyncio.sleep(0.5)

        poll_resp = await client.get(f"/api/v1/backtest/{job_id}")
        data = poll_resp.json()
        assert data["status"] == "complete"
        assert data["result"]["metrics"]["net_pnl_usd"] == "100.00"
        assert data["result"]["metrics"]["total_trades"] == 5


@pytest.mark.asyncio
async def test_full_lifecycle_lend(client):
    """Submit a lending backtest, verify it completes."""
    result = _make_backtest_result(net_pnl="50.00", sharpe="0.9", trades_count=2)

    with patch(
        "almanak.services.backtest.services.backtest_runner.create_backtester",
        return_value=_mock_backtester(result),
    ):
        submit_resp = await client.post("/api/v1/backtest", json=VALID_LEND_REQUEST)
        assert submit_resp.status_code == 202
        job_id = submit_resp.json()["job_id"]

        await asyncio.sleep(0.5)

        poll_resp = await client.get(f"/api/v1/backtest/{job_id}")
        data = poll_resp.json()
        assert data["status"] == "complete"
        assert data["result"]["metrics"]["net_pnl_usd"] == "50.00"


# ---------------------------------------------------------------------------
# Quick backtest lifecycle
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_quick_backtest_eligible(client):
    """Quick backtest with positive Sharpe and low drawdown -> eligible."""
    result = _make_backtest_result(sharpe="1.5")

    with patch(
        "almanak.services.backtest.routers.backtest.create_backtester",
        return_value=_mock_backtester(result),
    ):
        resp = await client.post(
            "/api/v1/backtest/quick",
            json={
                "strategy_spec": {
                    "protocol": "uniswap_v3",
                    "chain": "arbitrum",
                    "action": "swap",
                    "parameters": {"from_token": "USDC", "to_token": "WETH"},
                },
            },
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["eligible"] is True
        assert data["metrics"]["sharpe_ratio"] == "1.5"
        assert data["duration_seconds"] >= 0


@pytest.mark.asyncio
async def test_quick_backtest_not_eligible(client):
    """Quick backtest with negative Sharpe -> not eligible."""
    result = _make_backtest_result(sharpe="-0.5")

    with patch(
        "almanak.services.backtest.routers.backtest.create_backtester",
        return_value=_mock_backtester(result),
    ):
        resp = await client.post(
            "/api/v1/backtest/quick",
            json={
                "strategy_spec": {
                    "protocol": "uniswap_v3",
                    "chain": "arbitrum",
                    "action": "swap",
                    "parameters": {},
                },
            },
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["eligible"] is False


@pytest.mark.asyncio
async def test_quick_backtest_with_custom_timeframe(client):
    """Quick backtest with explicit timeframe."""
    result = _make_backtest_result()

    with patch(
        "almanak.services.backtest.routers.backtest.create_backtester",
        return_value=_mock_backtester(result),
    ):
        resp = await client.post(
            "/api/v1/backtest/quick",
            json={
                "strategy_spec": {
                    "protocol": "aave_v3",
                    "chain": "ethereum",
                    "action": "lend",
                    "parameters": {"token": "DAI"},
                },
                "timeframe": {"start": "2025-02-01", "end": "2025-02-08"},
            },
        )
        assert resp.status_code == 200
        assert resp.json()["eligible"] is True


# ---------------------------------------------------------------------------
# Error handling in lifecycle
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_backtest_failure_propagates(client):
    """When PnLBacktester raises, the job should be marked failed."""
    mock_bt = MagicMock()
    mock_bt.backtest = AsyncMock(side_effect=RuntimeError("Data provider connection failed"))
    mock_bt.close = AsyncMock()

    with patch(
        "almanak.services.backtest.services.backtest_runner.create_backtester",
        return_value=mock_bt,
    ):
        submit_resp = await client.post("/api/v1/backtest", json=VALID_SWAP_REQUEST)
        job_id = submit_resp.json()["job_id"]

        await asyncio.sleep(0.5)

        poll_resp = await client.get(f"/api/v1/backtest/{job_id}")
        data = poll_resp.json()
        assert data["status"] == "failed"
        assert data["error"] is not None
        assert "failed" in data["error"].lower()


# ---------------------------------------------------------------------------
# Health endpoint with resource reporting
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_health_includes_resource_reporting(client):
    """Health endpoint should include memory_mb and cpu_percent fields."""
    resp = await client.get("/api/v1/health")
    assert resp.status_code == 200
    data = resp.json()
    assert "peak_memory_mb" in data
    assert "cpu_percent" in data
    assert isinstance(data["peak_memory_mb"], (int, float))
    assert data["uptime_seconds"] >= 0


@pytest.mark.asyncio
async def test_health_active_jobs_increments(client):
    """Health should reflect active job count after submission."""
    # Mock the runner so jobs stay in pending/running
    with patch(
        "almanak.services.backtest.routers.backtest.run_backtest_job",
        new_callable=AsyncMock,
    ):
        # Check baseline
        resp = await client.get("/api/v1/health")
        initial_jobs = resp.json()["active_backtest_jobs"]

        # Submit a job
        await client.post("/api/v1/backtest", json=VALID_SWAP_REQUEST)

        # Check incremented
        resp = await client.get("/api/v1/health")
        assert resp.json()["active_backtest_jobs"] == initial_jobs + 1


# ---------------------------------------------------------------------------
# Strategy discovery
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_list_strategies_returns_sorted_list(client):
    """GET /strategies should return a sorted list of registered strategies."""
    resp = await client.get("/api/v1/strategies")
    assert resp.status_code == 200
    data = resp.json()
    assert "strategies" in data
    assert "count" in data
    assert isinstance(data["strategies"], list)
    assert data["count"] == len(data["strategies"])
    # Verify sorted
    assert data["strategies"] == sorted(data["strategies"])


# ---------------------------------------------------------------------------
# Fee model integration
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_fee_models_list_returns_known_protocols(client):
    """Fee models list should include major protocols."""
    resp = await client.get("/api/v1/fee-models")
    assert resp.status_code == 200
    data = resp.json()
    protocol_names = [p["protocol"] for p in data["protocols"]]
    # At minimum these should be registered
    for expected in ["uniswap_v3", "aave_v3"]:
        assert expected in protocol_names, f"{expected} not in fee model list"


@pytest.mark.asyncio
async def test_fee_model_detail_has_structure(client):
    """Fee model detail should have expected fields."""
    resp = await client.get("/api/v1/fee-models/uniswap_v3")
    assert resp.status_code == 200
    data = resp.json()
    assert data["protocol"] == "uniswap_v3"
    assert data["model_name"] != ""
    assert isinstance(data["supported_chains"], list)


# ---------------------------------------------------------------------------
# OpenAPI schema validation
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_openapi_schema_includes_all_endpoints(client):
    """OpenAPI schema should document all 9 endpoints from the PRD."""
    resp = await client.get("/openapi.json")
    assert resp.status_code == 200
    paths = resp.json()["paths"]

    expected_paths = [
        "/api/v1/health",
        "/api/v1/backtest",
        "/api/v1/backtest/{job_id}",
        "/api/v1/backtest/quick",
        "/api/v1/paper-trade",
        "/api/v1/paper-trade/{session_id}",
        "/api/v1/fee-models",
        "/api/v1/fee-models/{protocol}",
    ]
    for path in expected_paths:
        assert path in paths, f"Missing endpoint in OpenAPI: {path}"
