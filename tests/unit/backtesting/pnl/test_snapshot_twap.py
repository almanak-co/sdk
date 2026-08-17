"""Run-scoped exact-pool TWAP declaration, provenance, and refusal tests."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from decimal import Decimal
from types import SimpleNamespace

import pytest

from almanak.core.finality import DataFinality
from almanak.framework.backtesting.pnl.data_manifest import LANE_TWAP, RunDataManifest
from almanak.framework.backtesting.pnl.providers.snapshot_twap import (
    HistoricalTWAPTarget,
    SnapshotTWAPSource,
    declared_historical_twap_targets,
)
from almanak.framework.backtesting.pnl.providers.twap import HistoricalTWAPPoint
from almanak.framework.data.models import DataClassification
from almanak.framework.market.errors import PoolPriceUnavailableError

POOL = "0xc756bba710d45647715079ce50aa16aab36ded42"
START = datetime.fromtimestamp(1_000, UTC)
END = datetime.fromtimestamp(4_600, UTC)
TARGET = HistoricalTWAPTarget("ethereum", "uniswap_v3", POOL, 1_800)
GENERATED_TARGET = HistoricalTWAPTarget("bsc", "pancakeswap_v3", POOL, 300)


def _fetcher(**kwargs):
    assert kwargs["interval_secs"] == 3_600
    assert kwargs["window_secs"] == 1_800
    assert kwargs["pool_address"] == POOL
    return [
        HistoricalTWAPPoint(1_000, Decimal("1.002"), 2, "on_chain_archive", 100),
        HistoricalTWAPPoint(4_590, Decimal("1.003"), 2, "on_chain_archive", 460),
    ]


def test_typed_declaration_takes_precedence() -> None:
    custom_target = HistoricalTWAPTarget("ethereum", "custom_pool_provider", POOL, 1_800)
    strategy = SimpleNamespace(
        backtest_twap_targets=[custom_target],
        pool="0x0000000000000000000000000000000000000001",
    )

    assert declared_historical_twap_targets(strategy, {}, default_chain="ethereum") == (custom_target,)


def test_narrow_legacy_decoder_accepts_only_matching_generated_shape() -> None:
    config = {"swap_pool": POOL, "protocol": "uniswap_v3", "twap_window_seconds": 1_800}
    strategy = SimpleNamespace(
        pool=POOL,
        protocol="uniswap_v3",
        twap_window_seconds=1_800,
        STRATEGY_METADATA=SimpleNamespace(supported_protocols=["uniswap_v3"]),
    )
    assert declared_historical_twap_targets(strategy, config, default_chain="ethereum") == (TARGET,)

    strategy.pool = "0x0000000000000000000000000000000000000001"
    assert declared_historical_twap_targets(strategy, config, default_chain="ethereum") == ()


def test_decoder_accepts_current_generated_pool_and_window_names() -> None:
    config = {
        "swap_pool": POOL,
        "protocol": "pancakeswap_v3",
        "pool_twap_window_seconds": 300,
    }
    strategy = SimpleNamespace(
        swap_pool=POOL,
        protocol="pancakeswap_v3",
        STRATEGY_METADATA=SimpleNamespace(supported_protocols=["pancakeswap_v3"]),
    )

    assert declared_historical_twap_targets(strategy, config, default_chain="bsc") == (GENERATED_TARGET,)

    # The generated strategy passes ``window_seconds=300`` literally.  A
    # same-named instance attribute is optional, but when present it remains a
    # consistency assertion against the config declaration.
    strategy.pool_twap_window_seconds = 301
    assert declared_historical_twap_targets(strategy, config, default_chain="bsc") == ()


def test_decoder_accepts_generated_minute_window_with_seconds_attribute() -> None:
    config = {
        "swap_pool": POOL,
        "protocol": "pancakeswap_v3",
        "twap_window_minutes": 5,
    }
    strategy = SimpleNamespace(
        swap_pool=POOL,
        protocol="pancakeswap_v3",
        twap_window_seconds=300,
        STRATEGY_METADATA=SimpleNamespace(supported_protocols=["pancakeswap_v3"]),
    )

    assert declared_historical_twap_targets(strategy, config, default_chain="bsc") == (GENERATED_TARGET,)

    strategy.twap_window_seconds = 301
    assert declared_historical_twap_targets(strategy, config, default_chain="bsc") == ()


def test_decoder_fails_preflight_for_curve_twap() -> None:
    config = {
        "swap_pool": POOL,
        "protocol": "curve",
        "pool_twap_window_seconds": 300,
    }
    strategy = SimpleNamespace(
        swap_pool=POOL,
        protocol="curve",
        STRATEGY_METADATA=SimpleNamespace(supported_protocols=["curve"]),
    )

    with pytest.raises(ValueError, match="Curve pools do not expose one uniform native TWAP"):
        declared_historical_twap_targets(strategy, config, default_chain="ethereum")


def test_snapshot_view_serves_exact_archived_pool_observation_with_provenance() -> None:
    manifest = RunDataManifest()
    source = SnapshotTWAPSource(
        start_time=START,
        end_time=END,
        sample_interval_seconds=3_600,
        manifest=manifest,
        fetcher=_fetcher,
    )
    assert asyncio.run(source.materialize_history(TARGET)) == 2

    envelope = source.view_at(END).twap(
        pool_address=POOL,
        chain="ethereum",
        window_seconds=1_800,
        protocol="uniswap_v3",
    )

    assert envelope.value.price == Decimal("1.003")
    assert envelope.value.method == "twap"
    assert envelope.value.window_seconds == 1_800
    assert envelope.value.block_range == (460, 460)
    assert envelope.value.sources[0].pool_address == POOL
    assert envelope.classification is DataClassification.EXECUTION_GRADE
    assert envelope.meta.source == "historical:on_chain_archive"
    assert envelope.meta.observed_at == datetime.fromtimestamp(4_590, UTC)
    assert envelope.meta.block_number == 460
    assert envelope.meta.finality is DataFinality.LATEST
    assert envelope.meta.staleness_ms == 10_000
    assert envelope.is_fresh
    entry = manifest.entries()[0]
    assert entry["lane"] == LANE_TWAP
    assert entry["ladder"] == ["archive_observe"]
    assert "window_seconds=1800" in entry["detail"]
    assert "sample_interval_seconds=3600" in entry["detail"]


@pytest.mark.parametrize(
    "kwargs",
    [
        {"pool_address": "0x0000000000000000000000000000000000000001"},
        {"pool_address": "PAXG/XAUT"},
        {"window_seconds": 300},
        {"window_seconds": 0},
        {"protocol": "sushiswap_v3"},
    ],
)
def test_snapshot_view_refuses_undeclared_identity(kwargs) -> None:
    manifest = RunDataManifest()
    source = SnapshotTWAPSource(
        start_time=START,
        end_time=END,
        sample_interval_seconds=3_600,
        manifest=manifest,
        fetcher=_fetcher,
    )
    asyncio.run(source.materialize_history(TARGET))
    call = {
        "pool_address": POOL,
        "chain": "ethereum",
        "window_seconds": 1_800,
        "protocol": "uniswap_v3",
    }
    call.update(kwargs)
    with pytest.raises(PoolPriceUnavailableError, match="not declared and prewarmed"):
        source.view_at(END).twap(**call)
    assert manifest.entries()[-1]["outcome"] == "refused"


def test_materialize_rejects_stale_grid_point() -> None:
    def stale_fetcher(**_kwargs):
        return [
            HistoricalTWAPPoint(1_000, Decimal("1"), 2, "archive", 100),
            HistoricalTWAPPoint(999, Decimal("1"), 2, "archive", 100),
        ]

    source = SnapshotTWAPSource(
        start_time=START,
        end_time=END,
        sample_interval_seconds=3_600,
        fetcher=stale_fetcher,
    )
    with pytest.raises(ValueError, match="stale by"):
        asyncio.run(source.materialize_history(TARGET))
