"""Snapshot decision-input honesty in the pnl backtest (ALM-2951).

Covers: the completed indicator set (close-derived) + timeframe honoring,
on-demand providers, real 24h price_data enrichment, the engine-modeled gas
view, unavailable-accessor recording, and the run-level
``decision_input_failures`` report with hollow-run detection.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

import pytest

from almanak.framework.backtesting.pnl.config import PnLBacktestConfig
from almanak.framework.backtesting.pnl.data_provider import MarketState
from almanak.framework.backtesting.pnl.engine import (
    DefaultFeeModel,
    DefaultSlippageModel,
    PnLBacktester,
)
from almanak.framework.backtesting.pnl.indicator_engine import BacktestIndicatorEngine
from almanak.framework.market import MarketSnapshot
from tests.backtesting_funding import pnl_token_funding as _pnl_token_funding


def _engine_with_series(token: str = "WETH", n: int = 120) -> BacktestIndicatorEngine:
    engine = BacktestIndicatorEngine()
    for i in range(n):
        # Gently oscillating series so no indicator degenerates.
        engine.append_price(token, Decimal("3000") + Decimal(i % 10) * 3 - Decimal(i % 7))
    return engine


def _snapshot() -> MarketSnapshot:
    # VIB-4062 §5.7: tests build through the sanctioned builder, not the
    # raw constructor (which is allowlist-gated).
    from almanak.framework.market.builders import MarketSnapshotBuilder

    return MarketSnapshotBuilder.seeded(chain="arbitrum", wallet_address="0x" + "0" * 40)


class TestCompletedIndicatorSet:
    def test_new_indicators_served_on_demand(self):
        # Lazy by default (perf gate): the providers compute when read.
        engine = _engine_with_series()
        snapshot = _snapshot()
        rsi_provider, indicator_provider = engine.snapshot_providers({}, 3600)
        snapshot._rsi_provider = rsi_provider
        snapshot._indicator_provider = indicator_provider

        assert snapshot.sma("WETH", timeframe="1h").ma_type == "SMA"
        assert snapshot.stochastic("WETH", timeframe="1h").k_value >= 0
        assert snapshot.adx("WETH", timeframe="1h").adx >= 0
        assert snapshot.cci("WETH", timeframe="1h").period == 20
        assert snapshot.ichimoku("WETH", timeframe="1h").kijun_sen > 0

    def test_new_indicators_prepopulate_on_required_declaration(self):
        # Declaring required_indicators opts into the eager fast path.
        engine = BacktestIndicatorEngine(required_indicators={"stochastic", "adx", "cci", "ichimoku", "sma"})
        for i in range(120):
            engine.append_price("WETH", Decimal("3000") + Decimal(i % 10) * 3 - Decimal(i % 7))
        snapshot = _snapshot()
        engine.populate_snapshot(snapshot, {}, timeframe="1h")

        assert snapshot.stochastic("WETH", timeframe="1h").k_value >= 0
        assert snapshot.ichimoku("WETH", timeframe="1h").kijun_sen > 0

    def test_obv_stays_honest_unavailable(self):
        engine = _engine_with_series()
        snapshot = _snapshot()
        rsi_provider, indicator_provider = engine.snapshot_providers({}, 3600)
        snapshot._rsi_provider = rsi_provider
        snapshot._indicator_provider = indicator_provider
        engine.populate_snapshot(snapshot, {}, timeframe="1h")

        with pytest.raises(ValueError, match="OBV data not available"):
            snapshot.obv("WETH", timeframe="1h")
        # The provider's honest reason travels into the failure ledger.
        detail = next(v for k, v in snapshot._critical_data_failures.items() if k[0] == "obv")
        assert "volume history" in detail

    def test_prepopulated_values_stamped_with_tick_timeframe(self):
        engine = _engine_with_series()
        snapshot = _snapshot()
        engine.populate_snapshot(snapshot, {}, timeframe="1h")

        # No provider wired: a mismatched-timeframe request must NOT silently
        # serve the 1h value (the pre-ALM-2951 behavior).
        with pytest.raises(ValueError):
            snapshot.rsi("WETH", period=14, timeframe="4h")


class TestOnDemandProviders:
    def test_resamples_whole_multiples(self):
        engine = _engine_with_series(n=200)
        rsi_provider, _ = engine.snapshot_providers({}, 3600)

        four_hour = rsi_provider("WETH", 14, timeframe="4h")
        one_hour = rsi_provider("WETH", 14, timeframe="1h")
        assert four_hour.period == 14
        # 4h series is every 4th close — a genuinely different series.
        assert four_hour.value != one_hour.value

    def test_resamples_arbitrary_multiples(self):
        # "2h" isn't a canonical label — the parser must handle any <n><unit>.
        engine = _engine_with_series(n=200)
        rsi_provider, _ = engine.snapshot_providers({}, 3600)

        assert rsi_provider("WETH", 14, timeframe="2h").period == 14

    def test_rejects_non_derivable_timeframe(self):
        engine = _engine_with_series()
        rsi_provider, _ = engine.snapshot_providers({}, 3600)

        with pytest.raises(ValueError, match="not derivable"):
            rsi_provider("WETH", 14, timeframe="15m")

    def test_serves_non_config_periods(self):
        engine = _engine_with_series()
        _, provider = engine.snapshot_providers({}, 3600)

        assert provider.sma("WETH", 33, timeframe="1h").period == 33

    def test_snapshot_falls_through_to_provider_on_period_mismatch(self):
        engine = _engine_with_series()
        snapshot = _snapshot()
        rsi_provider, indicator_provider = engine.snapshot_providers({}, 3600)
        snapshot._rsi_provider = rsi_provider
        snapshot._indicator_provider = indicator_provider
        engine.populate_snapshot(snapshot, {}, timeframe="1h")

        # period=21 isn't pre-populated (config default is 14) — the provider
        # computes it on demand instead of raising.
        assert snapshot.rsi("WETH", period=21, timeframe="1h").period == 21
        assert snapshot.rsi("WETH", period=21, timeframe="4h").period == 21


class TestPriceDataEnrichment:
    def test_real_24h_fields_after_window(self):
        engine = BacktestIndicatorEngine()
        for i in range(30):
            engine.append_price("WETH", Decimal(3000 + i))
        snapshot = _snapshot()
        snapshot.set_price("WETH", Decimal("3029"))
        engine.enrich_price_data(snapshot, 3600)

        data = snapshot.price_data("WETH")
        assert data.price_24h_ago == Decimal("3005")  # 25 ticks back (24h window + 1)
        assert data.high_24h == Decimal("3029")
        assert data.low_24h == Decimal("3005")
        assert data.change_24h_pct > 0
        assert data.source == "backtest_price_series"

    def test_untouched_during_warmup(self):
        engine = BacktestIndicatorEngine()
        for i in range(5):
            engine.append_price("WETH", Decimal(3000 + i))
        snapshot = _snapshot()
        snapshot.set_price("WETH", Decimal("3004"))
        engine.enrich_price_data(snapshot, 3600)

        # Not enough history: the enricher must not fabricate 24h fields.
        assert snapshot.price_data("WETH").price_24h_ago == Decimal("0")


class TestRecordingGap:
    @pytest.mark.parametrize(
        ("call", "source"),
        [
            (lambda s: s.twap("WETH/USDC"), "twap"),
            (lambda s: s.lending_rate("aave_v3", "USDC"), "lending_rate"),
            (lambda s: s.pool_price("0x" + "1" * 40), "pool_price"),
            (lambda s: s.liquidity_depth("0x" + "1" * 40), "liquidity_depth"),
            (lambda s: s.gas_price(), "gas_price"),
            (lambda s: s.price_across_dexs("USDC", "WETH", Decimal("1")), "price_across_dexs"),
        ],
    )
    def test_unavailable_raise_is_recorded(self, call, source):
        snapshot = _snapshot()
        with pytest.raises((ValueError, NotImplementedError)):
            call(snapshot)
        assert any(key[0] == source for key in snapshot._critical_data_failures), (
            source,
            dict(snapshot._critical_data_failures),
        )

    def test_multi_dex_error_satisfies_both_contracts(self):
        from almanak.framework.market.snapshot import MultiDexUnavailableError

        snapshot = _snapshot()
        with pytest.raises(MultiDexUnavailableError) as excinfo:
            snapshot.best_dex_price("USDC", "WETH", Decimal("1"))
        assert isinstance(excinfo.value, ValueError)
        assert isinstance(excinfo.value, NotImplementedError)


class _TickingProvider:
    provider_name = "mock_ticking"

    def __init__(self, num_ticks: int = 40) -> None:
        self.num_ticks = num_ticks

    async def iterate(self, config: Any):
        start = datetime(2024, 1, 1, tzinfo=UTC)
        for i in range(self.num_ticks):
            timestamp = start + timedelta(hours=i)
            price = Decimal("3000") + Decimal(i % 10)
            yield (
                timestamp,
                MarketState(
                    timestamp=timestamp,
                    prices={
                        "WETH": price,
                        "ETH": price,
                        "USDC": Decimal("1"),
                        ("arbitrum", "0xaf88d065e77c8cc2239327c5edb3a432268e5831"): Decimal("1"),
                    },
                    chain="arbitrum",
                    block_number=1000 + i,
                ),
            )


class _TwapReadingStrategy:
    """Holds every tick after asking for an input the backtest can't serve."""

    def __init__(self) -> None:
        self._deployment_id = "twap_reader"

    @property
    def deployment_id(self) -> str:
        return self._deployment_id

    def decide(self, market: Any) -> Any:
        try:
            market.twap("WETH/USDC")
        except ValueError:
            pass
        return None


class _GasReadingStrategy:
    def __init__(self) -> None:
        self._deployment_id = "gas_reader"
        self.gas_costs: list[Decimal] = []
        self.worthwhile: list[bool] = []

    @property
    def deployment_id(self) -> str:
        return self._deployment_id

    def decide(self, market: Any) -> Any:
        self.gas_costs.append(market.estimate_swap_gas_cost_usd())
        self.worthwhile.append(market.is_trade_worthwhile(Decimal("10000")))
        return None


def _config(num_hours: int) -> PnLBacktestConfig:
    start = datetime(2024, 1, 1, tzinfo=UTC)
    return PnLBacktestConfig(
        start_time=start,
        end_time=start + timedelta(hours=num_hours),
        token_funding=_pnl_token_funding(Decimal("10000"), chain="arbitrum"),
        tokens=["WETH", "USDC"],
        preflight_validation=False,
        fail_on_preflight_error=True,
        inclusion_delay_blocks=0,
    )


def _backtester(num_ticks: int) -> PnLBacktester:
    return PnLBacktester(
        data_provider=_TickingProvider(num_ticks=num_ticks),
        fee_models={"default": DefaultFeeModel()},
        slippage_models={"default": DefaultSlippageModel()},
    )


class TestSimulatedGasViewErrors:
    def test_unbound_view_raises_gas_unavailable_not_valueerror_subclass_confusion(self):
        from almanak.framework.backtesting.pnl.engine import SimulatedGasView
        from almanak.framework.market.errors import GasUnavailableError

        view = SimulatedGasView(backtester=None, config=None)
        with pytest.raises(GasUnavailableError):
            view.get_gas_price("ethereum")

    def test_missing_config_gwei_raises_gas_unavailable(self):
        from types import SimpleNamespace

        from almanak.framework.backtesting.pnl.engine import SimulatedGasView
        from almanak.framework.market.errors import GasUnavailableError

        view = SimulatedGasView(backtester=None, config=SimpleNamespace(gas_price_gwei=None))
        view.bind(market_state=object(), timestamp=datetime(2024, 1, 1, tzinfo=UTC))
        with pytest.raises(GasUnavailableError):
            view.get_gas_price("ethereum")


class TestResultRoundTrip:
    def test_to_dict_from_dict_preserve_failure_report(self):
        from almanak.framework.backtesting.models import BacktestResult

        report = [{"source": "twap", "key": "unconfigured", "ticks": 6, "detail": "no provider"}]
        result = _minimal_result(decision_input_failures=report)
        data = result.to_dict()
        assert data["decision_input_failures"] == report
        assert BacktestResult.from_dict(data).decision_input_failures == report


def _minimal_result(**overrides: Any):
    from almanak.framework.backtesting.models import BacktestEngine, BacktestMetrics, BacktestResult

    kwargs: dict[str, Any] = {
        "engine": BacktestEngine.PNL,
        "deployment_id": "roundtrip",
        "start_time": datetime(2024, 1, 1, tzinfo=UTC),
        "end_time": datetime(2024, 1, 2, tzinfo=UTC),
        "metrics": BacktestMetrics(),
        "trades": [],
        "equity_curve": [],
        "initial_portfolio_value_usd": Decimal("100"),
        "final_capital_usd": Decimal("100"),
        "chain": "arbitrum",
    }
    kwargs.update(overrides)
    return BacktestResult(**kwargs)


class _PriceDataReadingStrategy:
    def __init__(self) -> None:
        self._deployment_id = "price_data_reader"
        self.day_ago_values: list[Decimal] = []

    @property
    def deployment_id(self) -> str:
        return self._deployment_id

    def decide(self, market: Any) -> Any:
        self.day_ago_values.append(market.price_data("WETH").price_24h_ago)
        return None


class TestRunLevelReport:
    @pytest.mark.asyncio
    async def test_hollow_run_reports_decision_input_failures(self, caplog):
        backtester = _backtester(num_ticks=6)
        with caplog.at_level("WARNING"):
            result = await backtester.backtest(_TwapReadingStrategy(), _config(6))

        assert result.decision_input_failures
        twap_entries = [f for f in result.decision_input_failures if f["source"] == "twap"]
        assert twap_entries and twap_entries[0]["ticks"] == 6
        assert any("HOLLOW BACKTEST" in r.message for r in caplog.records)

    @pytest.mark.asyncio
    async def test_clean_run_has_no_report(self):
        class _Holder:
            deployment_id = "clean_holder"

            def decide(self, market: Any) -> Any:
                return None

        backtester = _backtester(num_ticks=4)
        result = await backtester.backtest(_Holder(), _config(4))
        assert result.decision_input_failures is None

    @pytest.mark.asyncio
    async def test_gas_helpers_served_by_engine_model(self):
        strategy = _GasReadingStrategy()
        backtester = _backtester(num_ticks=4)
        result = await backtester.backtest(strategy, _config(4))

        assert strategy.gas_costs and all(cost > 0 for cost in strategy.gas_costs)
        assert strategy.worthwhile and all(strategy.worthwhile)
        # Served gas must not appear in the failure report.
        assert not any(f["source"].startswith("gas") for f in (result.decision_input_failures or []))

    @pytest.mark.asyncio
    async def test_price_data_24h_enriched_in_loop(self):
        strategy = _PriceDataReadingStrategy()
        backtester = _backtester(num_ticks=30)
        await backtester.backtest(strategy, _config(30))

        # Warm-up ticks (< 24h of history) stay honest-zero; later ticks
        # carry the real 24h-ago close from the run's own series.
        assert strategy.day_ago_values[0] == Decimal("0")
        assert strategy.day_ago_values[-1] > 0

    @pytest.mark.asyncio
    async def test_serialized_result_carries_report(self):
        from almanak.services.backtest.services.backtest_runner import serialize_result

        backtester = _backtester(num_ticks=4)
        result = await backtester.backtest(_TwapReadingStrategy(), _config(4))
        payload = serialize_result(result)
        assert payload["decision_input_failures"]
        assert payload["decision_input_failures"][0]["source"] == "twap"


class TestIdentityRegistrationSeam:
    """ALM-2960 (#3310 review round): the iteration loop must register the
    run's token identities on the portfolio, and credits must then land on
    the address-native plane."""

    @pytest.mark.asyncio
    async def test_loop_registers_identities_and_credits_land_address_keyed(self, monkeypatch):
        from decimal import Decimal as D

        from almanak.framework.backtesting.models import IntentType
        from almanak.framework.backtesting.pnl.portfolio import SimulatedFill, SimulatedPortfolio

        captured: list[tuple[SimulatedPortfolio, dict]] = []
        orig = SimulatedPortfolio.register_token_identities

        def spy(self, token_addresses):
            captured.append((self, dict(token_addresses or {})))
            return orig(self, token_addresses)

        monkeypatch.setattr(SimulatedPortfolio, "register_token_identities", spy)

        weth_key = ("arbitrum", "0x82af49447d8a07e3bd95bd0d56f35241523fbab1")
        backtester = _backtester(num_ticks=8)
        backtester.data_provider._token_addresses = {"WETH": weth_key}
        strategy = _TwapReadingStrategy()

        await backtester.backtest(strategy, _config(num_hours=8))

        # The loop registered the provider's identity map on the run's portfolio...
        assert captured, "execute_iteration_loop never registered token identities"
        portfolio, registered_map = captured[-1]
        assert registered_map.get("WETH") == weth_key

        # ...and a symbol-shaped credit on that SAME portfolio lands on the
        # address-native key (the ALM-2960 fix, engine seam included).
        fill = SimulatedFill(
            timestamp=datetime(2024, 1, 1, 6, tzinfo=UTC),
            intent_type=IntentType.LP_CLOSE,
            protocol="uniswap_v3",
            tokens=["WETH"],
            executed_price=D("3000"),
            amount_usd=D("0"),
            fee_usd=D("0"),
            slippage_usd=D("0"),
            gas_cost_usd=D("0"),
            tokens_in={"WETH": D("0.5")},
            tokens_out={},
        )
        assert portfolio.apply_fill(fill) is True
        assert portfolio.tokens.get(weth_key) == D("0.5")
        assert "WETH" not in portfolio.tokens


class _RsiReadingStrategy:
    """Reads 1h RSI every tick and remembers whether it was served or refused."""

    def __init__(self) -> None:
        self._deployment_id = "rsi_reader"
        self.served: list[bool] = []

    @property
    def deployment_id(self) -> str:
        return self._deployment_id

    def decide(self, market: Any) -> Any:
        try:
            market.rsi("WETH", period=5, timeframe="1h")
            self.served.append(True)
        except ValueError:
            self.served.append(False)
        return None


class TestGranularityHandoff:
    """ALM-2957 (#3311 review round): the loop's first tick must thread the
    provider's MEASURED data granularity into the indicator engine — a daily
    plane under hourly ticks makes 1h indicator reads refuse-and-record
    instead of serving saturated values."""

    @pytest.mark.asyncio
    async def test_first_tick_threads_measured_granularity(self):
        backtester = _backtester(num_ticks=40)
        backtester.data_provider.measured_granularity_seconds = 86400
        strategy = _RsiReadingStrategy()

        result = await backtester.backtest(strategy, _config(num_hours=40))

        # Every read refused (never served a degenerate 1h RSI)...
        assert strategy.served and not any(strategy.served)
        # ...and the refusals are on the decision-input ledger.
        rsi_entries = [f for f in result.decision_input_failures if f.get("source") == "rsi"]
        assert rsi_entries, result.decision_input_failures
        assert any("resolution" in str(f.get("detail")) for f in rsi_entries)

    @pytest.mark.asyncio
    async def test_matching_granularity_serves_after_warmup(self):
        backtester = _backtester(num_ticks=40)
        backtester.data_provider.measured_granularity_seconds = 3600
        strategy = _RsiReadingStrategy()

        await backtester.backtest(strategy, _config(num_hours=40))

        assert any(strategy.served)  # warm-up refuses, then 1h RSI serves
