"""Integration-style tests for the backtest lifecycle: submit -> poll -> complete/fail."""

from __future__ import annotations

from decimal import Decimal
from unittest.mock import AsyncMock, patch

import pytest

from almanak.services.backtest.services.backtest_runner import (
    SpecBacktestStrategy,
    _extract_tokens,
    build_backtest_config,
    build_quick_timeframe,
    serialize_result,
)
from almanak.services.backtest.services.job_manager import JobManager


# ---------------------------------------------------------------------------
# Unit tests for backtest_runner components
# ---------------------------------------------------------------------------


class TestSpecBacktestStrategy:
    """Tests for the StrategySpec -> BacktestableStrategy adapter."""

    def test_strategy_id_from_spec(self):
        from almanak.services.backtest.models import StrategySpec

        spec = StrategySpec(protocol="uniswap_v3", chain="arbitrum", action="swap")
        strategy = SpecBacktestStrategy(spec)
        assert strategy.strategy_id == "spec_uniswap_v3_swap_arbitrum"

    def test_decide_swap_returns_swap_intent(self):
        from almanak.services.backtest.models import StrategySpec

        spec = StrategySpec(
            protocol="uniswap_v3",
            chain="arbitrum",
            action="swap",
            parameters={"from_token": "USDC", "to_token": "WETH", "amount_usd": "500"},
        )
        strategy = SpecBacktestStrategy(spec)
        intent = strategy.decide(None)  # market unused for swap
        assert intent.from_token == "USDC"
        assert intent.to_token == "WETH"
        assert intent.amount_usd == Decimal("500")

    def test_decide_lend_returns_supply_intent(self):
        from almanak.framework.intents.vocabulary import SupplyIntent
        from almanak.services.backtest.models import StrategySpec

        spec = StrategySpec(protocol="aave_v3", chain="ethereum", action="lend")
        strategy = SpecBacktestStrategy(spec)
        intent = strategy.decide(None)
        assert isinstance(intent, SupplyIntent)
        assert intent.protocol == "aave_v3"
        assert intent.token == "USDC"  # default

    def test_decide_lp_returns_lp_open_intent(self):
        from almanak.framework.intents.vocabulary import LPOpenIntent
        from almanak.services.backtest.models import StrategySpec

        spec = StrategySpec(
            protocol="uniswap_v3",
            chain="arbitrum",
            action="provide_liquidity",
            parameters={"pool": "WETH/USDC", "amount0": "2", "amount1": "5000"},
        )
        strategy = SpecBacktestStrategy(spec)
        intent = strategy.decide(None)
        assert isinstance(intent, LPOpenIntent)
        assert intent.pool == "WETH/USDC"
        assert intent.amount0 == Decimal("2")
        assert intent.amount1 == Decimal("5000")

    def test_decide_borrow_returns_borrow_intent(self):
        from almanak.framework.intents.vocabulary import BorrowIntent
        from almanak.services.backtest.models import StrategySpec

        spec = StrategySpec(
            protocol="aave_v3",
            chain="ethereum",
            action="borrow",
            parameters={
                "collateral_token": "WETH",
                "collateral_amount": "5",
                "borrow_token": "USDC",
                "borrow_amount": "3000",
            },
        )
        strategy = SpecBacktestStrategy(spec)
        intent = strategy.decide(None)
        assert isinstance(intent, BorrowIntent)
        assert intent.collateral_token == "WETH"
        assert intent.borrow_amount == Decimal("3000")

    def test_second_tick_returns_hold(self):
        from almanak.framework.intents.vocabulary import HoldIntent
        from almanak.services.backtest.models import StrategySpec

        spec = StrategySpec(
            protocol="uniswap_v3",
            chain="arbitrum",
            action="swap",
            parameters={"from_token": "USDC", "to_token": "WETH", "amount_usd": "500"},
        )
        strategy = SpecBacktestStrategy(spec)
        intent1 = strategy.decide(None)
        assert intent1.from_token == "USDC"  # First tick: SwapIntent

        intent2 = strategy.decide(None)
        assert isinstance(intent2, HoldIntent)  # Second tick: Hold

    def test_unknown_action_raises_value_error(self):
        from almanak.services.backtest.models import StrategySpec

        spec = StrategySpec(protocol="some_protocol", chain="ethereum", action="unknown_action")
        with pytest.raises(ValueError, match="Unknown action 'unknown_action'"):
            SpecBacktestStrategy(spec)


class TestExtractTokens:
    """Tests for _extract_tokens helper."""

    def test_swap_tokens(self):
        from almanak.services.backtest.models import StrategySpec

        spec = StrategySpec(
            protocol="uniswap_v3", chain="arbitrum", action="swap",
            parameters={"from_token": "DAI", "to_token": "WETH"},
        )
        assert _extract_tokens(spec) == ["DAI", "WETH"]

    def test_lp_tokens(self):
        from almanak.services.backtest.models import StrategySpec

        spec = StrategySpec(
            protocol="uniswap_v3", chain="arbitrum", action="provide_liquidity",
            parameters={"token0": "WBTC", "token1": "USDC"},
        )
        assert _extract_tokens(spec) == ["WBTC", "USDC"]

    def test_lend_single_token(self):
        from almanak.services.backtest.models import StrategySpec

        spec = StrategySpec(
            protocol="aave_v3", chain="ethereum", action="lend",
            parameters={"token": "DAI"},
        )
        assert _extract_tokens(spec) == ["DAI"]

    def test_borrow_tokens(self):
        from almanak.services.backtest.models import StrategySpec

        spec = StrategySpec(
            protocol="aave_v3", chain="ethereum", action="borrow",
            parameters={"collateral_token": "WETH", "borrow_token": "USDC"},
        )
        assert _extract_tokens(spec) == ["WETH", "USDC"]

    def test_explicit_tokens_list_overrides(self):
        from almanak.services.backtest.models import StrategySpec

        spec = StrategySpec(
            protocol="uniswap_v3", chain="arbitrum", action="swap",
            parameters={"tokens": ["LINK", "DAI"], "from_token": "USDC"},
        )
        assert _extract_tokens(spec) == ["LINK", "DAI"]


class TestBuildBacktestConfig:
    """Tests for build_backtest_config."""

    def test_builds_config_from_spec(self):
        from almanak.services.backtest.models import StrategySpec, TimeframeSpec

        spec = StrategySpec(
            protocol="uniswap_v3",
            chain="arbitrum",
            action="swap",
            parameters={"amount_usd": "5000", "tokens": ["WETH", "USDC"]},
        )
        timeframe = TimeframeSpec(start="2025-01-01", end="2025-01-08")
        config = build_backtest_config(spec, timeframe)

        assert config.chain == "arbitrum"
        assert config.initial_capital_usd == Decimal("5000")
        assert config.interval_seconds == 3600  # full mode = 1h
        assert config.include_gas_costs is True

    def test_quick_mode_daily_interval(self):
        from almanak.services.backtest.models import StrategySpec, TimeframeSpec

        spec = StrategySpec(protocol="uniswap_v3", chain="arbitrum", action="swap")
        timeframe = TimeframeSpec(start="2025-01-01", end="2025-01-08")
        config = build_backtest_config(spec, timeframe, quick=True)

        assert config.interval_seconds == 86400  # quick mode = 1d
        assert config.include_gas_costs is False


class TestBuildBacktestConfigNamedStrategy:
    """Tests for build_backtest_config with named strategies."""

    def test_named_strategy_requires_chain(self):
        from almanak.services.backtest.models import TimeframeSpec

        timeframe = TimeframeSpec(start="2025-01-01", end="2025-01-08")
        with pytest.raises(ValueError, match="chain is required"):
            build_backtest_config(spec=None, timeframe=timeframe)

    def test_named_strategy_requires_tokens(self):
        from almanak.services.backtest.models import TimeframeSpec

        timeframe = TimeframeSpec(start="2025-01-01", end="2025-01-08")
        with pytest.raises(ValueError, match="tokens is required"):
            build_backtest_config(spec=None, timeframe=timeframe, chain="arbitrum")

    def test_named_strategy_builds_config(self):
        from almanak.services.backtest.models import TimeframeSpec

        timeframe = TimeframeSpec(start="2025-01-01", end="2025-01-08")
        config = build_backtest_config(
            spec=None,
            timeframe=timeframe,
            chain="arbitrum",
            tokens=["WETH", "USDC"],
            initial_capital_usd=Decimal("5000"),
        )
        assert config.chain == "arbitrum"
        assert config.tokens == ["WETH", "USDC"]
        assert config.initial_capital_usd == Decimal("5000")
        assert config.fee_model == "realistic"


class TestBuildQuickTimeframe:
    """Tests for build_quick_timeframe."""

    def test_returns_7_day_window(self):
        tf = build_quick_timeframe()
        delta = tf.end - tf.start
        assert delta.days == 7


# ---------------------------------------------------------------------------
# JobManager unit tests
# ---------------------------------------------------------------------------


class TestJobManager:
    """Tests for in-memory job tracking."""

    def test_create_and_get_job(self):
        jm = JobManager(max_concurrent=4)
        job_id = jm.create_job()
        assert job_id.startswith("bt_")
        job = jm.get_job(job_id)
        assert job is not None
        assert job.status.value == "pending"

    def test_lifecycle_pending_running_complete(self):
        jm = JobManager(max_concurrent=4)
        job_id = jm.create_job()

        jm.mark_running(job_id)
        assert jm.get_job(job_id).status.value == "running"

        jm.complete_job(job_id, {"metrics": {}})
        job = jm.get_job(job_id)
        assert job.status.value == "complete"
        assert job.result == {"metrics": {}}
        assert job.completed_at is not None

    def test_lifecycle_pending_running_failed(self):
        jm = JobManager(max_concurrent=4)
        job_id = jm.create_job()

        jm.mark_running(job_id)
        jm.fail_job(job_id, "something broke")

        job = jm.get_job(job_id)
        assert job.status.value == "failed"
        assert job.error == "something broke"

    def test_max_concurrent_enforced(self):
        jm = JobManager(max_concurrent=2)
        jm.create_job()
        jm.create_job()
        with pytest.raises(RuntimeError, match="Max concurrent"):
            jm.create_job()

    def test_completed_jobs_dont_count_as_active(self):
        jm = JobManager(max_concurrent=2)
        j1 = jm.create_job()
        jm.create_job()

        # Complete first job — should free a slot
        jm.mark_running(j1)
        jm.complete_job(j1, {})

        # Should succeed now
        j3 = jm.create_job()
        assert j3.startswith("bt_")

    def test_active_count(self):
        jm = JobManager(max_concurrent=4)
        assert jm.active_count == 0
        j1 = jm.create_job()
        assert jm.active_count == 1
        jm.mark_running(j1)
        assert jm.active_count == 1
        jm.complete_job(j1, {})
        assert jm.active_count == 0

    def test_progress_update(self):
        jm = JobManager(max_concurrent=4)
        job_id = jm.create_job()
        jm.mark_running(job_id)
        jm.update_progress(job_id, 50.0, "Halfway there", eta_seconds=30)

        job = jm.get_job(job_id)
        assert job.progress.percent == 50.0
        assert job.progress.current_step == "Halfway there"
        assert job.progress.eta_seconds == 30

    def test_eviction_of_completed_jobs(self):
        """Completed jobs are evicted when max_total is exceeded."""
        jm = JobManager(max_concurrent=10, max_total=3)
        ids = []
        for _ in range(3):
            jid = jm.create_job()
            jm.mark_running(jid)
            jm.complete_job(jid, {})
            ids.append(jid)

        # All 3 exist
        assert all(jm.get_job(jid) is not None for jid in ids)

        # Creating a 4th triggers eviction of the oldest completed
        j4 = jm.create_job()
        assert jm.get_job(j4) is not None
        assert jm.get_job(ids[0]) is None  # oldest evicted

    def test_get_nonexistent_job_returns_none(self):
        jm = JobManager(max_concurrent=4)
        assert jm.get_job("bt_does_not_exist") is None

    def test_complete_sets_progress_to_100(self):
        jm = JobManager(max_concurrent=4)
        job_id = jm.create_job()
        jm.mark_running(job_id)
        jm.update_progress(job_id, 50.0, "Halfway")
        jm.complete_job(job_id, {"metrics": {}})
        job = jm.get_job(job_id)
        assert job.progress.percent == 100.0
        assert job.progress.current_step == "Done"


# ---------------------------------------------------------------------------
# Full lifecycle integration test (HTTP level)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_submit_poll_complete_lifecycle(client):
    """Full lifecycle: submit -> poll (pending) -> mock complete -> poll (complete)."""
    from almanak.services.backtest.routers import backtest as backtest_router

    # Submit a job
    resp = await client.post(
        "/api/v1/backtest",
        json={
            "strategy_spec": {
                "protocol": "uniswap_v3",
                "chain": "arbitrum",
                "action": "swap",
                "parameters": {"from_token": "USDC", "to_token": "WETH"},
            },
            "timeframe": {"start": "2025-01-01", "end": "2025-01-08"},
        },
    )
    assert resp.status_code == 202
    job_id = resp.json()["job_id"]

    # Manually complete the job via job manager (simulating background task)
    jm = backtest_router._job_manager
    jm.mark_running(job_id)
    jm.update_progress(job_id, 50.0, "Running simulation...")
    jm.complete_job(
        job_id,
        {
            "metrics": {
                "net_pnl_usd": "150.00",
                "total_return_pct": "1.5",
                "sharpe_ratio": "1.2",
                "max_drawdown_pct": "0.05",
                "win_rate": "0.6",
                "total_trades": 10,
                "total_fees_usd": "5.00",
                "sortino_ratio": "1.5",
                "calmar_ratio": "30.0",
                "profit_factor": "2.5",
            },
            "equity_curve": [],
            "trades": [],
            "duration_seconds": 2.5,
        },
    )

    # Poll — should be complete with results
    poll_resp = await client.get(f"/api/v1/backtest/{job_id}")
    assert poll_resp.status_code == 200
    data = poll_resp.json()
    assert data["status"] == "complete"
    assert data["result"] is not None
    assert data["result"]["metrics"]["net_pnl_usd"] == "150.00"
    assert data["result"]["metrics"]["total_trades"] == 10
    assert data["result"]["duration_seconds"] == 2.5
    assert data["completed_at"] is not None


@pytest.mark.asyncio
async def test_submit_poll_failed_lifecycle(client):
    """Full lifecycle: submit -> mock fail -> poll (failed with error)."""
    from almanak.services.backtest.routers import backtest as backtest_router

    resp = await client.post(
        "/api/v1/backtest",
        json={
            "strategy_spec": {
                "protocol": "uniswap_v3",
                "chain": "arbitrum",
                "action": "swap",
                "parameters": {},
            },
            "timeframe": {"start": "2025-01-01", "end": "2025-01-08"},
        },
    )
    job_id = resp.json()["job_id"]

    # Simulate failure
    jm = backtest_router._job_manager
    jm.mark_running(job_id)
    jm.fail_job(job_id, "PnLBacktester initialization failed")

    poll_resp = await client.get(f"/api/v1/backtest/{job_id}")
    data = poll_resp.json()
    assert data["status"] == "failed"
    assert "PnLBacktester" in data["error"]
    assert data["result"] is None
