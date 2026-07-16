"""Integration-style tests for the backtest lifecycle: submit -> poll -> complete/fail."""

from __future__ import annotations
from tests.backtesting_funding import pnl_token_funding as _pnl_token_funding

import asyncio
from datetime import UTC, datetime
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from almanak.core.models.quote_asset import QuoteAsset
from almanak.framework.backtesting.pnl.config import PnLBacktestConfig
from almanak.services.backtest.models import BacktestRequest, StrategySpec, TimeframeSpec
from almanak.services.backtest.services.backtest_runner import (


    SpecBacktestStrategy,
    _extract_tokens,
    build_backtest_config,
    build_backtest_token_address_map,
    build_quick_timeframe,
    collect_backtest_token_refs,
    create_backtester,
    normalize_backtest_token_refs,
    run_backtest_job,
    serialize_result,
)
from almanak.services.backtest.services.job_manager import JobManager

BASE_CBBTC = "0xcbb7c0000ab88b473b1f5afd9ef808440eed33bf"
BASE_USDC = "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913"
BASE_WETH = "0x4200000000000000000000000000000000000006"
UNKNOWN_MIXED_CASE_ADDRESS = "0xAaAaAaAaAaAaAaAaAaAaAaAaAaAaAaAaAaAaAaAa"


# ---------------------------------------------------------------------------
# Unit tests for backtest_runner components
# ---------------------------------------------------------------------------


class TestSpecBacktestStrategy:
    """Tests for the StrategySpec -> BacktestableStrategy adapter."""

    def test_deployment_id_from_spec(self):
        from almanak.services.backtest.models import StrategySpec

        spec = StrategySpec(protocol="uniswap_v3", chain="arbitrum", action="swap")
        strategy = SpecBacktestStrategy(spec)
        assert strategy.deployment_id == "spec_uniswap_v3_swap_arbitrum"

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
        token_funding = _pnl_token_funding(Decimal("5000"))
        config = build_backtest_config(spec, timeframe, token_funding=token_funding)

        assert config.chain == "arbitrum"
        assert config.token_funding == token_funding
        assert config.interval_seconds == 3600  # full mode = 1h
        assert config.include_gas_costs is True

    def test_quick_mode_daily_interval(self):
        from almanak.services.backtest.models import StrategySpec, TimeframeSpec

        spec = StrategySpec(protocol="uniswap_v3", chain="arbitrum", action="swap")
        timeframe = TimeframeSpec(start="2025-01-01", end="2025-01-08")
        config = build_backtest_config(spec, timeframe, quick=True, token_funding=_pnl_token_funding(Decimal("5000")))

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
            token_funding=_pnl_token_funding(Decimal("5000")),
        )
        assert config.chain == "arbitrum"
        assert "WETH" in config.tokens
        assert "USDC" in config.tokens
        assert config.token_funding == _pnl_token_funding(Decimal("5000"))
        assert config.fee_model == "realistic"


class TestBacktestTokenRefs:
    """Tests for address-native token coverage in backtest runners."""

    def test_collect_backtest_token_refs_recurses_and_threads_all_sources(self):
        class Metadata:
            quote_asset = QuoteAsset.token(8453, BASE_CBBTC)

        class Strategy:
            STRATEGY_METADATA = Metadata()
            quote_asset = QuoteAsset.token(8453, BASE_USDC)
            _spec = SimpleNamespace(parameters={"legs": [{"borrow_token": "WETH"}]})

        refs = collect_backtest_token_refs(
            chain="base",
            strategy_config={
                "tokens": ["USDC"],
                "routes": [
                    {
                        "base_token_address": BASE_CBBTC,
                        "legs": [{"quote_token": "cbBTC"}],
                    }
                ],
                "risk": {"entry_token_address": BASE_WETH},
            },
            strategy=Strategy(),
            strategy_class=Strategy,
            extra_refs=["DAI"],
        )

        assert "USDC" in refs
        assert "WETH" in refs
        assert "cbBTC" in refs
        assert "DAI" in refs
        assert BASE_CBBTC in refs
        assert BASE_USDC in refs
        assert BASE_WETH in refs

    def test_normalize_backtest_token_refs_dedupes_unresolved_address_case(self):
        assert normalize_backtest_token_refs(
            [UNKNOWN_MIXED_CASE_ADDRESS, UNKNOWN_MIXED_CASE_ADDRESS.lower(), BASE_CBBTC],
            "base",
        ) == [UNKNOWN_MIXED_CASE_ADDRESS.lower(), "CBBTC"]

    def test_build_backtest_token_address_map_skips_native_but_keeps_wrapped_native(self):
        config = PnLBacktestConfig(
            start_time=datetime(2024, 1, 1, tzinfo=UTC),
            end_time=datetime(2024, 1, 2, tzinfo=UTC),
            token_funding=_pnl_token_funding(Decimal("10000"), chain="base"),
            chain="base",
            tokens=["ETH", "WETH", "USDC"],
        )

        token_addresses = build_backtest_token_address_map(
            config,
            extra_refs=[BASE_CBBTC, BASE_WETH],
        )

        assert "ETH" not in token_addresses
        assert token_addresses["WETH"] == ("base", BASE_WETH)
        assert token_addresses["USDC"] == ("base", BASE_USDC)
        assert token_addresses["CBBTC"] == ("base", BASE_CBBTC)

    def test_create_backtester_threads_token_addresses_into_provider_cache_keys(self):
        backtester = create_backtester(token_addresses={"CBBTC": ("base", BASE_CBBTC)})
        try:
            cache_key = backtester.data_provider._market_cache_key("CBBTC", "base")
            assert cache_key == ("base", BASE_CBBTC)
        finally:
            asyncio.run(backtester.close())

    def test_create_backtester_coerces_json_decimal_overrides(self):
        # Platform BACKTEST_CONFIG delivers JSON numbers as float/str; the
        # Decimal-typed data-config fields must not reach Decimal arithmetic raw.
        backtester = create_backtester(
            data_config_overrides={
                "allow_volume_fallback": True,
                "explicit_pool_volume_usd_daily": 5000000.0,
                "explicit_pool_liquidity_usd": "8800000",
            }
        )
        try:
            config = backtester.data_config
            assert config.explicit_pool_volume_usd_daily == Decimal("5000000")
            assert isinstance(config.explicit_pool_volume_usd_daily, Decimal)
            assert config.explicit_pool_liquidity_usd == Decimal("8800000")
            assert isinstance(config.explicit_pool_liquidity_usd, Decimal)
            assert config.allow_volume_fallback is True
        finally:
            asyncio.run(backtester.close())

    def test_create_backtester_rejects_non_numeric_decimal_override(self):
        with pytest.raises(ValueError, match="not a valid number"):
            create_backtester(data_config_overrides={"explicit_pool_liquidity_usd": "not-a-number"})

    @pytest.mark.asyncio
    async def test_run_backtest_job_threads_strategy_token_addresses(self):
        captured: list[dict[str, tuple[str, str]] | None] = []

        class Backtester:
            async def backtest(self, strategy: object, config: PnLBacktestConfig) -> object:
                return object()

            async def close(self) -> None:
                return None

        def fake_create_backtester(*, token_addresses: dict[str, tuple[str, str]] | None = None) -> Backtester:
            captured.append(token_addresses)
            return Backtester()

        request = BacktestRequest(
            strategy_spec=StrategySpec(
                protocol="uniswap_v3",
                chain="base",
                action="swap",
                parameters={
                    "from_token": "USDC",
                    "to_token_address": BASE_WETH,
                    "routes": [{"base_token_address": BASE_CBBTC}],
                },
            ),
            timeframe=TimeframeSpec(start="2024-01-01", end="2024-01-02"),
            token_funding=_pnl_token_funding(Decimal("5000"), chain="base"),
        )
        job_manager = JobManager()
        job_id = job_manager.create_job()

        with (
            patch(
                "almanak.services.backtest.services.backtest_runner.create_backtester",
                side_effect=fake_create_backtester,
            ),
            patch(
                "almanak.services.backtest.services.backtest_runner.serialize_result",
                return_value={"metrics": {}, "trades": []},
            ),
        ):
            await run_backtest_job(job_id, request, job_manager)

        assert captured == [
            {
                "CBBTC": ("base", BASE_CBBTC),
                "USDC": ("base", BASE_USDC),
                "WETH": ("base", BASE_WETH),
            }
        ]
        job = job_manager.get_job(job_id)
        assert job is not None
        assert job.status.value == "complete"


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


class TestSerializeResult:
    """serialize_result trade serialization, including unrealized PnL (VIB-5083)."""

    def _result(self, trades):
        from datetime import UTC, datetime

        from almanak.framework.backtesting.models import (
            BacktestEngine,
            BacktestMetrics,
            BacktestResult,
            EquityPoint,
        )

        return BacktestResult(
            engine=BacktestEngine.PNL,
            deployment_id="svc-test",
            start_time=datetime(2025, 11, 1, tzinfo=UTC),
            end_time=datetime(2025, 11, 2, tzinfo=UTC),
            metrics=BacktestMetrics(total_trades=len(trades)),
            trades=trades,
            equity_curve=[
                EquityPoint(timestamp=datetime(2025, 11, 1, tzinfo=UTC), value_usd=Decimal("10000")),
            ],
        )

    def test_realized_and_unrealized_pnl_serialize(self):
        """A closing trade serializes its realized PnL; an opening trade serializes null."""
        from datetime import UTC, datetime

        from almanak.framework.backtesting.models import IntentType, TradeRecord

        opening = TradeRecord(
            timestamp=datetime(2025, 11, 1, 1, tzinfo=UTC),
            intent_type=IntentType.SWAP,
            executed_price=Decimal("2000"),
            fee_usd=Decimal("0"),
            slippage_usd=Decimal("0"),
            gas_cost_usd=Decimal("0"),
            pnl_usd=None,  # opening / inventory-building trade
            success=True,
            amount_usd=Decimal("5000"),
        )
        closing = TradeRecord(
            timestamp=datetime(2025, 11, 1, 2, tzinfo=UTC),
            intent_type=IntentType.SWAP,
            executed_price=Decimal("2500"),
            fee_usd=Decimal("0"),
            slippage_usd=Decimal("0"),
            gas_cost_usd=Decimal("0"),
            pnl_usd=Decimal("1250"),
            success=True,
            amount_usd=Decimal("6250"),
        )

        out = serialize_result(self._result([opening, closing]))
        serialized = out["trades"]
        assert serialized[0]["pnl_usd"] is None  # opening trade -> JSON null, not "None"
        assert serialized[1]["pnl_usd"] == "1250"  # closing trade -> realized gain

    def test_rejected_trades_carry_status_and_reason(self):
        """Rejected intents serialize as status=rejected with the portfolio's reason (ALM-2936)."""
        from datetime import UTC, datetime

        from almanak.framework.backtesting.models import IntentType, TradeRecord

        filled = TradeRecord(
            timestamp=datetime(2025, 11, 1, 1, tzinfo=UTC),
            intent_type=IntentType.SWAP,
            executed_price=Decimal("2000"),
            fee_usd=Decimal("1"),
            slippage_usd=Decimal("0"),
            gas_cost_usd=Decimal("0"),
            pnl_usd=None,
            success=True,
            amount_usd=Decimal("5000"),
        )
        rejected = TradeRecord(
            timestamp=datetime(2025, 11, 1, 2, tzinfo=UTC),
            intent_type=IntentType.SWAP,
            executed_price=Decimal("0"),
            fee_usd=Decimal("0"),
            slippage_usd=Decimal("0"),
            gas_cost_usd=Decimal("0"),
            pnl_usd=None,
            success=False,
            amount_usd=Decimal("0"),
            error="insufficient cash for fill: required 1, cash-like 0",
        )

        serialized = serialize_result(self._result([filled, rejected]))["trades"]
        assert serialized[0]["status"] == "filled"
        assert serialized[0]["rejection_reason"] is None
        assert serialized[1]["status"] == "rejected"
        assert serialized[1]["rejection_reason"].startswith("insufficient cash for fill")

    def test_usd_result_emits_no_numeraire_or_price_keys(self):
        """A fiat_usd result payload stays free of numeraire / price-series keys."""
        out = serialize_result(self._result([]))
        assert "numeraire" not in out
        assert "initial_capital_numeraire" not in out
        assert "final_capital_numeraire" not in out
        assert "price_series" not in out
        assert "price_series_display_labels" not in out
        assert all("numeraire_price_usd" not in pt and "value_numeraire" not in pt for pt in out["equity_curve"])

    def test_numeraire_and_price_series_pass_through(self):
        """The service serializer must not strip the numeraire projection (VIB-5127).

        Regression: the old serializer emitted only ``{timestamp, value_usd}``
        per equity point, dropping ``numeraire_price_usd`` and the top-level
        numeraire descriptors that the SDK result carried.
        """
        from datetime import UTC, datetime

        from almanak.framework.backtesting.models import EquityPoint, PricePoint

        result = self._result([])
        result.numeraire = "WETH"
        result.initial_capital_numeraire = Decimal("5")
        result.final_capital_numeraire = Decimal("5.5")
        ts = datetime(2025, 11, 1, tzinfo=UTC)
        result.equity_curve = [
            EquityPoint(timestamp=ts, value_usd=Decimal("10000"), numeraire_price_usd=Decimal("2000")),
        ]
        result.price_series = [
            PricePoint(timestamp=ts, prices={"arbitrum:0xweth": Decimal("2000"), "USDC": Decimal("1")}),
        ]
        result.price_series_display_labels = {"arbitrum:0xweth": "WETH", "USDC": "USDC"}

        out = serialize_result(result)
        assert out["numeraire"] == "WETH"
        assert out["initial_capital_numeraire"] == "5"
        assert out["final_capital_numeraire"] == "5.5"
        point = out["equity_curve"][0]
        assert point["numeraire_price_usd"] == "2000"
        assert point["value_numeraire"] == "5"  # 10000 / 2000
        price_point = out["price_series"][0]
        assert price_point["prices"] == {"arbitrum:0xweth": "2000", "USDC": "1"}
        assert out["price_series_display_labels"]["arbitrum:0xweth"] == "WETH"
