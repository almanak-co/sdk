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


def _fetcher(**kwargs):
    assert kwargs["interval_secs"] == 3_600
    assert kwargs["window_secs"] == 1_800
    assert kwargs["pool_address"] == POOL
    return [
        HistoricalTWAPPoint(1_000, Decimal("1.002"), 2, "on_chain_archive", 100),
        HistoricalTWAPPoint(4_590, Decimal("1.003"), 2, "on_chain_archive", 460),
    ]


def test_typed_declaration_is_the_only_declaration() -> None:
    custom_target = HistoricalTWAPTarget("ethereum", "custom_pool_provider", POOL, 1_800)
    strategy = SimpleNamespace(
        backtest_twap_targets=[custom_target],
        pool="0x0000000000000000000000000000000000000001",
    )

    assert declared_historical_twap_targets(strategy, {}, default_chain="ethereum") == (custom_target,)


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
        # A different protocol on the same pool is no longer a refusal: it is a
        # distinct identity fetched at first use, and this fixture's fetcher
        # serves it (see test_snapshot_view_serves_undeclared_pool_at_first_use).
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
    # An undeclared identity is fetched at first use; this fixture's fetcher
    # only serves TARGET, so the archive "cannot serve" it and the read is
    # refused fail-closed (an invalid identity never reaches the fetcher).
    with pytest.raises(
        PoolPriceUnavailableError,
        match="first-use historical TWAP fetch failed|not a valid historical TWAP target",
    ):
        source.view_at(END).twap(**call)
    assert manifest.entries()[-1]["outcome"] == "refused"


def test_snapshot_view_serves_undeclared_pool_at_first_use() -> None:
    """A decide()-time TWAP read that names an undeclared pool/window fetches it inline, once."""
    calls: list[dict] = []

    def permissive_fetcher(**kwargs):
        calls.append(kwargs)
        return _fetcher(**{**kwargs, "pool_address": POOL, "window_secs": 1_800})

    manifest = RunDataManifest()
    source = SnapshotTWAPSource(
        start_time=START,
        end_time=END,
        sample_interval_seconds=3_600,
        manifest=manifest,
        fetcher=permissive_fetcher,
    )
    # Nothing declared, nothing prewarmed.
    assert not source._series

    first = source.view_at(END).twap(pool_address=POOL, chain="ethereum", protocol="uniswap_v3", window_seconds=300)
    second = source.view_at(END).twap(pool_address=POOL, chain="ethereum", protocol="uniswap_v3", window_seconds=300)

    assert len(calls) == 1
    assert calls[0]["pool_address"] == POOL and calls[0]["window_secs"] == 300
    assert first.value.price == second.value.price > 0
    assert first.classification is DataClassification.EXECUTION_GRADE
    assert manifest.entries()[-1]["outcome"] == "served"


def test_snapshot_view_remembers_first_use_fetch_failure() -> None:
    """An archive that cannot serve the pool is asked once; every later read refuses instantly."""
    calls: list[dict] = []

    def failing_fetcher(**kwargs):
        calls.append(kwargs)
        raise ValueError("archive RPC unavailable")

    source = SnapshotTWAPSource(start_time=START, end_time=END, sample_interval_seconds=3_600, fetcher=failing_fetcher)
    for _ in range(2):
        with pytest.raises(PoolPriceUnavailableError, match="first-use historical TWAP fetch failed"):
            source.view_at(END).twap(pool_address=POOL, chain="ethereum", protocol="uniswap_v3", window_seconds=300)
    assert len(calls) == 1


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
