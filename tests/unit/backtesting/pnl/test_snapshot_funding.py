"""Strategy-facing funding lane for PnL-backtest snapshots (unit).

Guards ``SnapshotFundingRateSource`` / ``SnapshotFundingRateView``
(``pnl/providers/perp/snapshot_funding.py``) and their wiring through
``create_market_snapshot_from_state``: before that lane existed the engine
handed ``decide()`` snapshots with no ``funding_rate_provider``, so every
``market.funding_rate(...)`` read raised "No funding rate provider configured
for MarketSnapshot" and funding-gated perp strategies produced 0-trade
backtests over any window.

The engine-loop proof (a funding-gated strategy actually enters) lives in the
Trust Matrix cell ``perp:funding_gated_entry``
(``tests/validation/backtesting/``); this module pins the lane's semantics:
config gating, no look-ahead, per-hour caching, strict-mode honesty, and the
exact demo-strategy read from the regression report.
"""

from __future__ import annotations

import asyncio
import math
from datetime import UTC, datetime, timedelta, timezone
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from almanak.framework.backtesting.config import BacktestDataConfig
from almanak.framework.backtesting.pnl.data_manifest import (
    CONSUMER_STRATEGY_DECISION,
    LANE_FUNDING,
    OUTCOME_DEGRADED,
    OUTCOME_REFUSED,
    OUTCOME_SERVED,
    RunDataManifest,
)
from almanak.framework.backtesting.pnl.data_provider import MarketState
from almanak.framework.backtesting.pnl.engine import create_market_snapshot_from_state
from almanak.framework.backtesting.pnl.providers.perp._gateway_history import (
    MAX_WINDOW_SECONDS,
    FundingHistoryPoint,
)
from almanak.framework.backtesting.pnl.providers.perp.snapshot_funding import (
    DEFAULT_FALLBACK_RATE,
    SnapshotFundingRateSource,
)
from almanak.framework.data.funding import FundingRateUnavailableError, Venue

TICK = datetime(2024, 1, 15, 12, 30, tzinfo=UTC)
TICK_HOUR = datetime(2024, 1, 15, 12, 0, tzinfo=UTC)


def _market_state(timestamp: datetime = TICK) -> MarketState:
    return MarketState(timestamp=timestamp, prices={"WETH": Decimal("2000")}, chain="arbitrum")


def _get_rate(source: SnapshotFundingRateSource, venue: str = "gmx_v2", timestamp: datetime = TICK):
    return asyncio.run(source.view_at(timestamp).get_funding_rate(Venue(venue), "ETH-USD"))


@pytest.fixture
def no_gateway(monkeypatch: pytest.MonkeyPatch):
    """Fail loudly if the lane opens a gateway round-trip."""

    def _explode(**_kwargs):
        raise AssertionError("gateway funding fetch must not be reached")

    monkeypatch.setattr(
        "almanak.framework.backtesting.pnl.providers.perp.snapshot_funding.fetch_funding_points",
        _explode,
    )


# =============================================================================
# Fixed lane (use_historical_funding off / no data_config)
# =============================================================================


def test_fixed_lane_serves_configured_fallback_rate_without_network(no_gateway) -> None:
    fallback = Decimal("0.0002")
    manifest = RunDataManifest()
    source = SnapshotFundingRateSource(
        chain="arbitrum",
        start_time=TICK_HOUR,
        end_time=TICK_HOUR + timedelta(hours=2),
        data_config=BacktestDataConfig(use_historical_funding=False, funding_fallback_rate=fallback),
        manifest=manifest,
    )

    rate = _get_rate(source)
    _get_rate(source, timestamp=TICK + timedelta(minutes=15))

    assert rate.rate_hourly == fallback
    assert rate.rate_8h == fallback * 8
    assert rate.rate_annualized == fallback * 8760
    assert rate.is_live_data is False
    assert rate.timestamp == TICK_HOUR
    assert rate.venue == "gmx_v2"
    assert rate.market == "ETH-USD"
    (entry,) = manifest.entries()
    assert entry["consumer"] == CONSUMER_STRATEGY_DECISION
    assert entry["source"] == "fixed:configured"
    assert entry["outcome"] == OUTCOME_DEGRADED
    assert entry["count"] == 2
    assert f"funding_rate_hourly={fallback}" in entry["detail"]


def test_no_data_config_serves_default_rate(no_gateway) -> None:
    source = SnapshotFundingRateSource(
        chain="arbitrum",
        start_time=TICK_HOUR,
        end_time=TICK_HOUR + timedelta(hours=2),
        data_config=None,
    )

    assert _get_rate(source).rate_hourly == DEFAULT_FALLBACK_RATE


def test_spread_view_is_timestamp_bound_and_labelled(no_gateway) -> None:
    source = SnapshotFundingRateSource(
        chain="arbitrum",
        start_time=TICK_HOUR,
        end_time=TICK_HOUR + timedelta(hours=2),
        data_config=None,
    )

    spread = asyncio.run(source.view_at(TICK).get_funding_rate_spread("ETH-USD", Venue.GMX_V2, Venue.HYPERLIQUID))

    assert spread.venue_a == "gmx_v2"
    assert spread.venue_b == "hyperliquid"
    assert spread.spread_8h == Decimal("0")
    assert spread.timestamp == TICK_HOUR


# =============================================================================
# Historical lane (gateway-backed, no look-ahead)
# =============================================================================


def _historical_source(
    *,
    strict: bool = False,
    chain: str = "arbitrum",
    start_time: datetime = TICK_HOUR,
    end_time: datetime = TICK_HOUR + timedelta(hours=2),
    manifest: RunDataManifest | None = None,
) -> SnapshotFundingRateSource:
    return SnapshotFundingRateSource(
        chain=chain,
        start_time=start_time,
        end_time=end_time,
        data_config=BacktestDataConfig(
            use_historical_funding=True,
            strict_historical_mode=strict,
            funding_fallback_rate=Decimal("0.0007"),
        ),
        manifest=manifest,
    )


def test_historical_lane_resolves_latest_point_at_or_before_tick(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[dict] = []
    manifest = RunDataManifest()

    def _fetch(**kwargs):
        calls.append(kwargs)
        tick_ts = int(TICK_HOUR.timestamp())
        return [
            FundingHistoryPoint(timestamp=tick_ts - 7200, rate_hourly=Decimal("0.0003")),
            FundingHistoryPoint(timestamp=tick_ts - 60, rate_hourly=Decimal("0.0004")),
            FundingHistoryPoint(timestamp=kwargs["end_ts"] - 60, rate_hourly=Decimal("0.9999")),
        ]

    monkeypatch.setattr(
        "almanak.framework.backtesting.pnl.providers.perp.snapshot_funding.fetch_funding_points",
        _fetch,
    )
    source = _historical_source(manifest=manifest)

    rate = _get_rate(source)

    # Latest measured point at or before the (hour-normalized) tick wins.
    assert rate.rate_hourly == Decimal("0.0004")
    # The source materializes the whole run, including a 24h lead-in. Point
    # resolution still ignores observations after each simulated tick.
    assert calls[0]["start_ts"] == int((TICK_HOUR - timedelta(hours=24)).timestamp())
    assert calls[0]["end_ts"] == int((TICK_HOUR + timedelta(hours=2)).timestamp())

    # Same and later hours are served from the same run-wide series.
    _get_rate(source, timestamp=TICK + timedelta(minutes=15))
    assert len(calls) == 1
    _get_rate(source, timestamp=TICK + timedelta(hours=1))
    assert len(calls) == 1
    (entry,) = manifest.entries()
    assert entry["source"] == "historical:gateway"
    assert entry["outcome"] == OUTCOME_SERVED
    assert entry["count"] == 3


def test_hour_normalization_floors_aware_offsets_in_utc(monkeypatch: pytest.MonkeyPatch) -> None:
    """A +05:30 tick at 07:00 UTC must query through 07:00 UTC, not 06:30.

    Flooring in the value's own offset would end the fetch window 30 minutes
    early for odd-offset zones and miss the latest measured point.
    """
    calls: list[dict] = []

    def _fetch(**kwargs):
        calls.append(kwargs)
        return [FundingHistoryPoint(timestamp=kwargs["end_ts"] - 60, rate_hourly=Decimal("0.0004"))]

    monkeypatch.setattr(
        "almanak.framework.backtesting.pnl.providers.perp.snapshot_funding.fetch_funding_points",
        _fetch,
    )
    ist = timezone(timedelta(hours=5, minutes=30))
    tick_utc7 = datetime(2024, 1, 15, 12, 30, tzinfo=ist)  # == 07:00 UTC
    expected_hour = datetime(2024, 1, 15, 7, 0, tzinfo=UTC)

    rate = _get_rate(
        _historical_source(start_time=expected_hour, end_time=expected_hour),
        timestamp=tick_utc7,
    )

    assert rate.timestamp == expected_hour
    assert calls[0]["end_ts"] == int(expected_hour.timestamp())
    assert rate.rate_hourly == Decimal("0.0004")


def test_historical_unmeasured_hour_falls_back_to_configured_rate(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "almanak.framework.backtesting.pnl.providers.perp.snapshot_funding.fetch_funding_points",
        lambda **_kwargs: [],
    )
    manifest = RunDataManifest()
    source = _historical_source(manifest=manifest)

    # The engine-configured fallback governs, not the provider module default.
    assert _get_rate(source).rate_hourly == Decimal("0.0007")
    (entry,) = manifest.entries()
    assert entry["lane"] == LANE_FUNDING
    assert entry["consumer"] == CONSUMER_STRATEGY_DECISION
    assert entry["source"] == "fallback:no_data"
    assert entry["outcome"] == OUTCOME_DEGRADED
    assert "funding_rate_hourly=0.0007" in entry["detail"]


def test_historical_unmeasured_hour_raises_in_strict_mode(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[dict] = []
    manifest = RunDataManifest()

    def _fetch(**kwargs):
        calls.append(kwargs)
        return []

    monkeypatch.setattr(
        "almanak.framework.backtesting.pnl.providers.perp.snapshot_funding.fetch_funding_points",
        _fetch,
    )
    source = _historical_source(strict=True, manifest=manifest)

    with pytest.raises(FundingRateUnavailableError):
        _get_rate(source)

    # Strict unavailability is sticky for the run.
    with pytest.raises(FundingRateUnavailableError):
        _get_rate(source, timestamp=TICK + timedelta(minutes=15))
    assert len(calls) == 1
    with pytest.raises(FundingRateUnavailableError):
        _get_rate(source, timestamp=TICK + timedelta(hours=1))
    assert len(calls) == 1
    (entry,) = manifest.entries()
    assert entry["consumer"] == CONSUMER_STRATEGY_DECISION
    assert entry["source"] == ""
    assert entry["outcome"] == OUTCOME_REFUSED
    assert entry["count"] == 3


def test_unexpected_history_load_failure_propagates_and_is_not_cached(monkeypatch: pytest.MonkeyPatch) -> None:
    calls = 0

    def _fetch(**_kwargs):
        nonlocal calls
        calls += 1
        if calls == 1:
            raise RuntimeError("unexpected decoder failure")
        return [
            FundingHistoryPoint(
                timestamp=int(TICK_HOUR.timestamp()) - 60,
                rate_hourly=Decimal("0.00042"),
            )
        ]

    monkeypatch.setattr(
        "almanak.framework.backtesting.pnl.providers.perp.snapshot_funding.fetch_funding_points",
        _fetch,
    )
    source = _historical_source()

    with pytest.raises(RuntimeError, match="unexpected decoder failure"):
        asyncio.run(source.materialize_history("gmx_v2", "ETH-USD"))

    assert asyncio.run(source.materialize_history("gmx_v2", "ETH-USD")) == 1
    assert calls == 2


def test_snapshot_and_accrual_observation_share_one_series(monkeypatch: pytest.MonkeyPatch) -> None:
    calls = 0

    def _fetch(**_kwargs):
        nonlocal calls
        calls += 1
        return [
            FundingHistoryPoint(
                timestamp=int(TICK_HOUR.timestamp()) - 60,
                rate_hourly=Decimal("0.00042"),
            )
        ]

    monkeypatch.setattr(
        "almanak.framework.backtesting.pnl.providers.perp.snapshot_funding.fetch_funding_points",
        _fetch,
    )
    source = _historical_source()

    snapshot_rate = _get_rate(source)
    accrual = source.observation_at("gmx_v2", "ETH-USD", TICK)

    assert snapshot_rate.rate_hourly == accrual.rate == Decimal("0.00042")
    assert accrual.source == "historical:gateway"
    assert accrual.confidence == "high"
    assert calls == 1


def test_served_observation_does_not_reuse_stale_degradation_reason(monkeypatch: pytest.MonkeyPatch) -> None:
    def _fetch(**_kwargs):
        return [
            FundingHistoryPoint(
                timestamp=int(TICK_HOUR.timestamp()) - 60,
                rate_hourly=Decimal("0.00042"),
            )
        ]

    monkeypatch.setattr(
        "almanak.framework.backtesting.pnl.providers.perp.snapshot_funding.fetch_funding_points",
        _fetch,
    )
    source = _historical_source()

    before_materialization = source.observation_at("gmx_v2", "ETH-USD", TICK)
    assert before_materialization.degraded is True
    assert before_materialization.reason

    assert asyncio.run(source.materialize_history("gmx_v2", "ETH-USD")) == 1
    served = source.observation_at("gmx_v2", "ETH-USD", TICK)

    assert served.degraded is False
    assert served.reason == ""
    assert served.rate == Decimal("0.00042")


def test_upstream_calls_scale_with_history_chunks_not_ticks(monkeypatch: pytest.MonkeyPatch) -> None:
    """A 3000-tick run performs one bounded series load, not 3000 RPCs."""
    tick_count = 3000
    start = datetime(2024, 1, 1, tzinfo=UTC)
    end = start + timedelta(hours=tick_count - 1)
    requests: list[object] = []

    class _Request:
        def __init__(self, **kwargs):
            self.__dict__.update(kwargs)

    def _rpc(request):
        requests.append(request)
        points = [
            SimpleNamespace(timestamp=timestamp, rate_hourly="0.0001")
            for timestamp in range(request.start_ts, request.end_ts + 1, 3600)
        ]
        return SimpleNamespace(success=True, source="hyperliquid", error="", points=points)

    client = SimpleNamespace(
        rate_history=SimpleNamespace(GetFundingRateHistory=_rpc),
        is_connected=True,
    )
    pb2 = SimpleNamespace(GetFundingRateHistoryRequest=_Request)
    monkeypatch.setattr(
        "almanak.framework.backtesting.pnl.providers.perp._gateway_history.get_connected_gateway_client",
        lambda: (client, pb2),
    )
    source = _historical_source(start_time=start, end_time=end)

    async def _read_every_tick() -> None:
        for offset in range(tick_count):
            await source.funding_rate_at("gmx_v2", "ETH-USD", start + timedelta(hours=offset))

    asyncio.run(_read_every_tick())

    history_start_ts = int((start - timedelta(hours=24)).timestamp())
    history_end_ts = int(end.timestamp())
    expected_chunks = math.ceil((history_end_ts - history_start_ts + 1) / MAX_WINDOW_SECONDS)
    assert len(requests) == expected_chunks
    assert len(requests) < tick_count


def test_on_chain_venue_with_undeclared_chain_degrades_without_gateway(no_gateway) -> None:
    # gmx_v2 declares arbitrum/avalanche only: an ethereum run must not ask
    # the gateway for wrong-chain data — it degrades to the fallback rate.
    source = _historical_source(chain="ethereum")

    assert _get_rate(source).rate_hourly == Decimal("0.0007")

    with pytest.raises(FundingRateUnavailableError):
        _get_rate(_historical_source(strict=True, chain="ethereum"))


# =============================================================================
# Snapshot factory wiring + the demo-strategy read from the regression report
# =============================================================================


def test_snapshot_factory_binds_view_to_tick_timestamp(no_gateway) -> None:
    source = SnapshotFundingRateSource(
        chain="arbitrum",
        start_time=TICK_HOUR,
        end_time=TICK_HOUR + timedelta(hours=2),
        data_config=None,
    )

    snapshot = create_market_snapshot_from_state(_market_state(), chain="arbitrum", funding_rate_source=source)
    rate = snapshot.funding_rate("gmx_v2", "ETH-USD")

    assert rate.rate_hourly == DEFAULT_FALLBACK_RATE
    assert rate.timestamp == TICK_HOUR


def test_snapshot_without_source_still_raises() -> None:
    snapshot = create_market_snapshot_from_state(_market_state(), chain="arbitrum")

    with pytest.raises(ValueError, match="No funding rate provider configured"):
        snapshot.funding_rate("gmx_v2", "ETH-USD")


def test_demo_perp_strategy_funding_gate_receives_rate(no_gateway) -> None:
    """The exact read from the report: gmx_v2_directional_perp._funding_hourly.

    Unwired, it logged "Funding rate unavailable for ETH-USD: No funding rate
    provider configured for MarketSnapshot" and returned None on every tick,
    so the entry gate never passed. Against an engine-built snapshot it must
    return the served rate.
    """
    from almanak.demo_strategies.gmx_v2_directional_perp.strategy import GmxV2DirectionalPerp
    from tests.unit.connectors.gmx_v2.market_fixtures import market_address

    # Address-first market contract: __init__ requires config `market_address`
    # (the audited arbitrum ETH/USD market-token address); every other knob
    # keeps its default. `_funding_hourly` reads funding by this address.
    config = {"market_address": market_address("arbitrum", "ETH/USD")}
    with patch(
        "almanak.framework.strategies.intent_strategy.IntentStrategy.__init__",
        return_value=None,
    ):
        strategy = GmxV2DirectionalPerp.__new__(GmxV2DirectionalPerp)
        strategy._config = config
        strategy.get_config = lambda key, default=None: config.get(key, default)
        GmxV2DirectionalPerp.__init__(strategy)

    snapshot = create_market_snapshot_from_state(
        _market_state(),
        chain="arbitrum",
        funding_rate_source=SnapshotFundingRateSource(
            chain="arbitrum",
            start_time=TICK_HOUR,
            end_time=TICK_HOUR + timedelta(hours=2),
            data_config=None,
        ),
    )

    assert strategy._funding_hourly(snapshot) == DEFAULT_FALLBACK_RATE
