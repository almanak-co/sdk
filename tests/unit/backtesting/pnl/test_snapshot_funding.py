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
from datetime import UTC, datetime, timedelta, timezone
from decimal import Decimal
from unittest.mock import patch

import pytest

from almanak.framework.backtesting.config import BacktestDataConfig
from almanak.framework.backtesting.pnl.data_provider import MarketState
from almanak.framework.backtesting.pnl.engine import create_market_snapshot_from_state
from almanak.framework.backtesting.pnl.providers.perp._gateway_history import FundingHistoryPoint
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
        "almanak.framework.backtesting.pnl.providers.funding_rates.fetch_funding_points",
        _explode,
    )


# =============================================================================
# Fixed lane (use_historical_funding off / no data_config)
# =============================================================================


def test_fixed_lane_serves_configured_fallback_rate_without_network(no_gateway) -> None:
    fallback = Decimal("0.0002")
    source = SnapshotFundingRateSource(
        chain="arbitrum",
        data_config=BacktestDataConfig(use_historical_funding=False, funding_fallback_rate=fallback),
    )

    rate = _get_rate(source)

    assert rate.rate_hourly == fallback
    assert rate.rate_8h == fallback * 8
    assert rate.rate_annualized == fallback * 8760
    assert rate.is_live_data is False
    assert rate.timestamp == TICK_HOUR
    assert rate.venue == "gmx_v2"
    assert rate.market == "ETH-USD"


def test_no_data_config_serves_default_rate(no_gateway) -> None:
    source = SnapshotFundingRateSource(chain="arbitrum", data_config=None)

    assert _get_rate(source).rate_hourly == DEFAULT_FALLBACK_RATE


def test_spread_view_is_timestamp_bound_and_labelled(no_gateway) -> None:
    source = SnapshotFundingRateSource(chain="arbitrum", data_config=None)

    spread = asyncio.run(source.view_at(TICK).get_funding_rate_spread("ETH-USD", Venue.GMX_V2, Venue.HYPERLIQUID))

    assert spread.venue_a == "gmx_v2"
    assert spread.venue_b == "hyperliquid"
    assert spread.spread_8h == Decimal("0")
    assert spread.timestamp == TICK_HOUR


# =============================================================================
# Historical lane (gateway-backed, no look-ahead)
# =============================================================================


def _historical_source(*, strict: bool = False, chain: str = "arbitrum") -> SnapshotFundingRateSource:
    return SnapshotFundingRateSource(
        chain=chain,
        data_config=BacktestDataConfig(
            use_historical_funding=True,
            strict_historical_mode=strict,
            funding_fallback_rate=Decimal("0.0007"),
        ),
    )


def test_historical_lane_resolves_latest_point_at_or_before_tick(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[dict] = []

    def _fetch(**kwargs):
        calls.append(kwargs)
        end_ts = kwargs["end_ts"]
        return [
            FundingHistoryPoint(timestamp=end_ts - 7200, rate_hourly=Decimal("0.0003")),
            FundingHistoryPoint(timestamp=end_ts - 60, rate_hourly=Decimal("0.0004")),
        ]

    monkeypatch.setattr(
        "almanak.framework.backtesting.pnl.providers.funding_rates.fetch_funding_points",
        _fetch,
    )
    source = _historical_source()

    rate = _get_rate(source)

    # Latest measured point at or before the (hour-normalized) tick wins.
    assert rate.rate_hourly == Decimal("0.0004")
    # The fetch window must never extend past the tick — no look-ahead.
    assert calls[0]["end_ts"] == int(TICK_HOUR.timestamp())
    assert calls[0]["start_ts"] < calls[0]["end_ts"]

    # Same hour is served from the per-hour cache (one fetch), a later tick
    # hour issues a fresh window bound to ITS timestamp.
    _get_rate(source, timestamp=TICK + timedelta(minutes=15))
    assert len(calls) == 1
    _get_rate(source, timestamp=TICK + timedelta(hours=1))
    assert len(calls) == 2
    assert calls[1]["end_ts"] == int((TICK_HOUR + timedelta(hours=1)).timestamp())


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
        "almanak.framework.backtesting.pnl.providers.funding_rates.fetch_funding_points",
        _fetch,
    )
    ist = timezone(timedelta(hours=5, minutes=30))
    tick_utc7 = datetime(2024, 1, 15, 12, 30, tzinfo=ist)  # == 07:00 UTC

    rate = _get_rate(_historical_source(), timestamp=tick_utc7)

    expected_hour = datetime(2024, 1, 15, 7, 0, tzinfo=UTC)
    assert rate.timestamp == expected_hour
    assert calls[0]["end_ts"] == int(expected_hour.timestamp())
    assert rate.rate_hourly == Decimal("0.0004")


def test_historical_unmeasured_hour_falls_back_to_configured_rate(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "almanak.framework.backtesting.pnl.providers.funding_rates.fetch_funding_points",
        lambda **_kwargs: [],
    )
    source = _historical_source()

    # The engine-configured fallback governs, not the provider module default.
    assert _get_rate(source).rate_hourly == Decimal("0.0007")


def test_historical_unmeasured_hour_raises_in_strict_mode(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[dict] = []

    def _fetch(**kwargs):
        calls.append(kwargs)
        return []

    monkeypatch.setattr(
        "almanak.framework.backtesting.pnl.providers.funding_rates.fetch_funding_points",
        _fetch,
    )
    source = _historical_source(strict=True)

    with pytest.raises(FundingRateUnavailableError):
        _get_rate(source)

    # Strict unavailability is cached per hour: a same-hour re-read raises
    # again WITHOUT a fresh gateway attempt; the next hour retries once.
    with pytest.raises(FundingRateUnavailableError):
        _get_rate(source, timestamp=TICK + timedelta(minutes=15))
    assert len(calls) == 1
    with pytest.raises(FundingRateUnavailableError):
        _get_rate(source, timestamp=TICK + timedelta(hours=1))
    assert len(calls) == 2


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
    source = SnapshotFundingRateSource(chain="arbitrum", data_config=None)

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

    with patch(
        "almanak.framework.strategies.intent_strategy.IntentStrategy.__init__",
        return_value=None,
    ):
        strategy = GmxV2DirectionalPerp.__new__(GmxV2DirectionalPerp)
        strategy._config = {}
        strategy.get_config = lambda key, default=None: default
        GmxV2DirectionalPerp.__init__(strategy)

    snapshot = create_market_snapshot_from_state(
        _market_state(),
        chain="arbitrum",
        funding_rate_source=SnapshotFundingRateSource(chain="arbitrum", data_config=None),
    )

    assert strategy._funding_hourly(snapshot) == DEFAULT_FALLBACK_RATE
