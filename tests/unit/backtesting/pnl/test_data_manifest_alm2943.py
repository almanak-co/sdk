"""RunDataManifest + BacktestDataBroker lifecycle skeleton (ALM-2943).

Covers: manifest aggregation/serialization semantics, the configurable
source ladder recorded per-serve, broker provider coalescing (pool-history
singleton routing + funding-provider memo), the contextvar seam, and a
small synthetic backtest whose result carries a manifest with entries from
at least two lanes (price + ohlcv) that round-trips through JSON.
"""

from __future__ import annotations

import json
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from typing import Any

import pytest

from almanak.framework.backtesting.pnl.config import PnLBacktestConfig
from almanak.framework.backtesting.pnl.data_broker import (
    BacktestDataBroker,
    active_data_broker,
    data_broker_scope,
    pool_history_provider,
    record_data_serve,
)
from almanak.framework.backtesting.pnl.data_manifest import (
    DEFAULT_SOURCE_LADDER,
    LANE_FUNDING,
    LANE_OHLCV,
    LANE_POOL_VOLUME,
    LANE_PRICE,
    OUTCOME_DEGRADED,
    OUTCOME_SERVED,
    RunDataManifest,
)
from almanak.framework.backtesting.pnl.data_provider import MarketState
from almanak.framework.backtesting.pnl.engine import (
    DefaultFeeModel,
    DefaultSlippageModel,
    PnLBacktester,
)
from tests.backtesting_funding import pnl_token_funding as _pnl_token_funding


class TestRunDataManifest:
    def test_aggregates_identical_serve_shapes(self):
        manifest = RunDataManifest()
        for hour in range(5):
            manifest.record(
                lane=LANE_PRICE,
                key="WETH",
                source="coingecko",
                outcome=OUTCOME_SERVED,
                at=datetime(2024, 1, 1, hour, tzinfo=UTC),
            )
        entries = manifest.entries()
        assert len(entries) == 1
        entry = entries[0]
        assert entry["count"] == 5
        assert entry["first"] == datetime(2024, 1, 1, 0, tzinfo=UTC).isoformat()
        assert entry["last"] == datetime(2024, 1, 1, 4, tzinfo=UTC).isoformat()

    def test_distinct_outcomes_are_distinct_rows(self):
        manifest = RunDataManifest()
        manifest.record(lane=LANE_PRICE, key="WETH", source="coingecko", outcome=OUTCOME_SERVED)
        manifest.record(lane=LANE_PRICE, key="WETH", source="", outcome=OUTCOME_DEGRADED, detail="miss")
        entries = manifest.entries()
        assert len(entries) == 2
        assert {e["outcome"] for e in entries} == {OUTCOME_SERVED, OUTCOME_DEGRADED}

    def test_date_range_serves_use_start_end(self):
        manifest = RunDataManifest()
        manifest.record(
            lane=LANE_POOL_VOLUME,
            key="base:0xpool",
            source="gateway_pool_history:defillama",
            outcome=OUTCOME_SERVED,
            start=date(2024, 1, 1),
            end=date(2024, 1, 3),
        )
        (entry,) = manifest.entries()
        assert entry["first"] == "2024-01-01"
        assert entry["last"] == "2024-01-03"

    def test_ladder_recorded_per_serve_with_configurable_default(self):
        manifest = RunDataManifest(source_ladder=("a", "b"))
        manifest.record(lane=LANE_FUNDING, key="ETH-USD", source="historical:gmx", outcome=OUTCOME_SERVED)
        manifest.record(
            lane=LANE_FUNDING,
            key="ETH-USD",
            source="historical:gmx",
            outcome=OUTCOME_SERVED,
            ladder=("b", "a"),
        )
        entries = manifest.entries()
        # A different per-serve ladder order is a distinct manifest row.
        assert len(entries) == 2
        assert {tuple(e["ladder"]) for e in entries} == {("a", "b"), ("b", "a")}
        assert manifest.to_dict()["source_ladder"] == ["a", "b"]

    def test_default_ladder_is_subgraph_first(self):
        assert DEFAULT_SOURCE_LADDER == ("subgraph", "coingecko-onchain", "multiplier")
        assert RunDataManifest().to_dict()["source_ladder"] == list(DEFAULT_SOURCE_LADDER)

    def test_to_dict_is_json_serializable(self):
        manifest = RunDataManifest()
        manifest.record(
            lane=LANE_OHLCV,
            key="WETH/USD@1h",
            source="backtest_price_series:close_only",
            outcome=OUTCOME_SERVED,
            at=datetime(2024, 1, 1, tzinfo=UTC),
        )
        payload = manifest.to_dict()
        round_tripped = json.loads(json.dumps(payload))
        assert round_tripped["schema_version"] == 1
        assert round_tripped["entries"][0]["lane"] == LANE_OHLCV


class TestBacktestDataBroker:
    def test_pool_history_routes_to_process_singleton(self):
        from almanak.framework.backtesting.pnl.providers.pool_history_fallback import get_pool_history_fallback

        broker = BacktestDataBroker()
        assert broker.pool_history() is get_pool_history_fallback()
        # And so does the module-level seam, with or without an active broker.
        assert pool_history_provider() is get_pool_history_fallback()
        with data_broker_scope(broker):
            assert pool_history_provider() is get_pool_history_fallback()

    def test_funding_provider_construction_is_coalesced(self):
        broker = BacktestDataBroker()
        calls: list[str] = []

        def build() -> object:
            calls.append("built")
            return object()

        first = broker.funding_provider(("funding", "gmx", "arbitrum"), build)
        second = broker.funding_provider(("funding", "gmx", "arbitrum"), build)
        assert first is second
        assert calls == ["built"]
        other = broker.funding_provider(("funding", "gmx", "avalanche"), build)
        assert other is not first
        assert calls == ["built", "built"]

    def test_contextvar_scope_and_noop_recording(self):
        assert active_data_broker() is None
        # No active broker: recording must be a silent no-op.
        record_data_serve(lane=LANE_PRICE, key="WETH", source="x", outcome=OUTCOME_SERVED)
        broker = BacktestDataBroker()
        with data_broker_scope(broker):
            assert active_data_broker() is broker
            record_data_serve(lane=LANE_PRICE, key="WETH", source="x", outcome=OUTCOME_SERVED)
        assert active_data_broker() is None
        (entry,) = broker.manifest.entries()
        assert entry["count"] == 1


class _TickingProvider:
    provider_name = "mock_ticking"

    def __init__(self, num_ticks: int = 8) -> None:
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
                    prices={"WETH": price, "ETH": price, "USDC": Decimal("1")},
                    chain="arbitrum",
                    block_number=1000 + i,
                ),
            )


class _CandleReadingStrategy:
    """Reads market.ohlcv() so the run serves the price AND ohlcv lanes."""

    deployment_id = "manifest_probe"

    def decide(self, market: Any) -> Any:
        try:
            market.ohlcv("WETH", timeframe="1h", limit=4)
        except ValueError:
            pass
        return None


class _RefusedCandleStrategy:
    """Asks for candles the run has no series for: an ohlcv-lane refusal."""

    deployment_id = "refused_candle_probe"

    def decide(self, market: Any) -> Any:
        try:
            market.ohlcv("DOGE", timeframe="1h", limit=4)
        except ValueError:
            pass
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


class TestManifestOnBacktestResult:
    @pytest.mark.asyncio
    async def test_synthetic_run_collects_at_least_two_lanes(self):
        backtester = _backtester(num_ticks=8)
        result = await backtester.backtest(_CandleReadingStrategy(), _config(8))

        assert result.error is None
        manifest = result.data_manifest
        assert manifest is not None
        assert manifest["schema_version"] == 1
        assert manifest["source_ladder"] == list(DEFAULT_SOURCE_LADDER)
        lanes = {entry["lane"] for entry in manifest["entries"]}
        assert LANE_PRICE in lanes
        assert LANE_OHLCV in lanes

        price_entries = [e for e in manifest["entries"] if e["lane"] == LANE_PRICE and e["outcome"] == OUTCOME_SERVED]
        assert price_entries
        # Serves aggregate per key: 8 ticks x served WETH -> one row, count 8.
        weth = next(e for e in price_entries if e["key"] == "WETH")
        assert weth["count"] == 8
        assert weth["source"] == "mock_ticking"
        assert weth["first"] < weth["last"]

    @pytest.mark.asyncio
    async def test_manifest_serializes_and_round_trips(self):
        from almanak.framework.backtesting.models import BacktestResult

        backtester = _backtester(num_ticks=4)
        result = await backtester.backtest(_CandleReadingStrategy(), _config(4))

        payload = result.to_dict()
        assert payload["data_manifest"]["entries"]
        json.dumps(payload["data_manifest"])  # JSON-safe end to end
        assert BacktestResult.from_dict(payload).data_manifest == result.data_manifest

    @pytest.mark.asyncio
    async def test_broker_deactivated_after_run(self):
        backtester = _backtester(num_ticks=4)
        await backtester.backtest(_CandleReadingStrategy(), _config(4))
        assert active_data_broker() is None

    @pytest.mark.asyncio
    async def test_refused_ohlcv_request_produces_refused_row(self):
        """Refusals stamp too — the manifest must answer 'why did this run degrade'."""
        backtester = _backtester(num_ticks=4)
        result = await backtester.backtest(_RefusedCandleStrategy(), _config(4))

        assert result.data_manifest is not None
        refused = [
            e for e in result.data_manifest["entries"] if e["lane"] == LANE_OHLCV and e["outcome"] == "refused"
        ]
        assert refused, result.data_manifest["entries"]
        (row,) = refused
        assert row["key"] == "DOGE@1h"
        assert row["count"] == 4  # one refusal per tick, aggregated
        assert "no backtest price series" in row["detail"]
        # And no served ohlcv rows: the only candle asked for was refused.
        assert not any(
            e["lane"] == LANE_OHLCV and e["outcome"] == OUTCOME_SERVED for e in result.data_manifest["entries"]
        )


class TestMissingPriceStamps:
    def test_strict_missing_price_stamps_refused(self):
        from almanak.framework.backtesting.pnl.portfolio import SimulatedPortfolio

        portfolio = SimulatedPortfolio(
            initial_capital_usd=Decimal("0"), cash_usd=Decimal("0"), chain="arbitrum"
        )
        broker = BacktestDataBroker()
        with data_broker_scope(broker):
            with pytest.raises(ValueError, match="strict_price_mode"):
                portfolio._handle_missing_price(
                    "WETH",
                    chain_id=42161,
                    data_tracker=None,
                    simulation_timestamp=datetime(2024, 1, 1, tzinfo=UTC),
                    strict_price_mode=True,
                    context="valuation",
                )
        (row,) = broker.manifest.entries()
        assert row["lane"] == LANE_PRICE
        assert row["outcome"] == "refused"
        assert row["key"] == "WETH"

    def test_nonstrict_missing_price_stamps_degraded(self):
        from almanak.framework.backtesting.pnl.portfolio import SimulatedPortfolio

        portfolio = SimulatedPortfolio(
            initial_capital_usd=Decimal("0"), cash_usd=Decimal("0"), chain="arbitrum"
        )
        broker = BacktestDataBroker()
        with data_broker_scope(broker):
            portfolio._handle_missing_price(
                "WETH",
                chain_id=42161,
                data_tracker=None,
                simulation_timestamp=datetime(2024, 1, 1, tzinfo=UTC),
                strict_price_mode=False,
                context="valuation",
            )
        (row,) = broker.manifest.entries()
        assert row["outcome"] == OUTCOME_DEGRADED
        assert "missing price during valuation" in row["detail"]


class TestSerializeResult:
    @pytest.mark.asyncio
    async def test_serialize_result_carries_manifest(self):
        from almanak.services.backtest.services.backtest_runner import serialize_result

        backtester = _backtester(num_ticks=4)
        result = await backtester.backtest(_CandleReadingStrategy(), _config(4))
        payload = serialize_result(result)
        assert payload["data_manifest"] == result.data_manifest
        assert payload["data_manifest"]["entries"]

    def test_absent_manifest_stays_absent(self):
        from almanak.framework.backtesting.models import BacktestEngine, BacktestMetrics, BacktestResult
        from almanak.services.backtest.services.backtest_runner import serialize_result

        result = BacktestResult(
            engine=BacktestEngine.PNL,
            deployment_id="no_manifest",
            start_time=datetime(2024, 1, 1, tzinfo=UTC),
            end_time=datetime(2024, 1, 2, tzinfo=UTC),
            metrics=BacktestMetrics(),
            chain="arbitrum",
        )
        assert result.data_manifest is None
        assert "data_manifest" not in serialize_result(result)
        assert "data_manifest" not in result.to_dict()
