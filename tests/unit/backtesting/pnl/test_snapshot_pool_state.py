"""Exact-address historical pool-state declaration and snapshot tests."""

from __future__ import annotations

import asyncio
from dataclasses import replace
from datetime import UTC, datetime
from types import SimpleNamespace

import pytest

from almanak.framework.backtesting.pnl.data_manifest import LANE_POOL_STATE, RunDataManifest
from almanak.framework.backtesting.pnl.providers.snapshot_pool_state import (
    HistoricalPoolStatePoint,
    HistoricalPoolStateTarget,
    SnapshotPoolStateSource,
    declared_historical_pool_state_targets,
    fetch_historical_pool_state_points,
)
from almanak.framework.data.interfaces import DataSourceUnavailable
from almanak.framework.data.models import DataClassification
from almanak.framework.market.errors import PoolPriceUnavailableError

POOL = "0x9b08288c3be4f62bbf8d1c20ac9c5e6f9467d8b7"
TOKEN0 = "0x0d500b1d8e8ef31e21c99d1db9a6444d3adf1270"
TOKEN1 = "0xc2132d05d31c914a87c6611c10748aeb04b58e8f"
START = datetime.fromtimestamp(1_000, UTC)
END = datetime.fromtimestamp(4_600, UTC)
TARGET = HistoricalPoolStateTarget("polygon", "uniswap_v3", POOL, (TOKEN0, TOKEN1), 500)


def _fetcher(**kwargs):
    assert kwargs["pool_address"] == POOL
    return [
        HistoricalPoolStatePoint(
            1_000, 100, 2**96, 0, 9_000, TOKEN0, TOKEN1, 18, 6, 500, 2 * 10**18, 4 * 10**6, "on_chain_archive"
        ),
        HistoricalPoolStatePoint(
            4_590, 460, 2**97, 1, 8_000, TOKEN0, TOKEN1, 18, 6, 500, 3 * 10**18, 5 * 10**6, "on_chain_archive"
        ),
    ]


def _gateway_point(timestamp: int, block_number: int) -> SimpleNamespace:
    return SimpleNamespace(
        timestamp=timestamp,
        block_number=block_number,
        sqrt_price_x96=2**96,
        tick=0,
        liquidity=9_000,
        token0=TOKEN0,
        token1=TOKEN1,
        token0_decimals=18,
        token1_decimals=6,
        fee_tier=500,
        reserve0_raw=2 * 10**18,
        reserve1_raw=4 * 10**6,
    )


def test_legacy_generated_pool_binding_is_decoded_address_first() -> None:
    config = {
        "pool": POOL,
        "protocol": "uniswap_v3",
        "fee_tier": 500,
        "base_token": {"symbol": "WMATIC", "address": TOKEN0},
        "quote_token": {"symbol": "USDT", "address": TOKEN1},
    }
    strategy = SimpleNamespace(
        pool=POOL,
        protocol="uniswap_v3",
        STRATEGY_METADATA=SimpleNamespace(supported_protocols=["uniswap_v3"]),
    )

    assert declared_historical_pool_state_targets(strategy, config, default_chain="polygon") == (TARGET,)

    strategy.pool = "0x0000000000000000000000000000000000000001"
    assert declared_historical_pool_state_targets(strategy, config, default_chain="polygon") == ()


def test_snapshot_serves_execution_grade_pool_price_and_reserves() -> None:
    manifest = RunDataManifest()
    source = SnapshotPoolStateSource(
        start_time=START,
        end_time=END,
        sample_interval_seconds=3_600,
        manifest=manifest,
        fetcher=_fetcher,
    )
    assert asyncio.run(source.materialize_history(TARGET)) == 2
    view = source.view_at(END)

    price = view.read_pool_price(POOL, "polygon")
    reserves = asyncio.run(view.get_pool_reserves(POOL, "polygon"))

    assert price.classification is DataClassification.EXECUTION_GRADE
    assert price.value.price == 4_000_000_000_000
    assert price.value.tick == 1
    assert price.value.liquidity == 8_000
    assert price.meta.block_number == 460
    assert reserves.pool_address == POOL
    assert reserves.token0.address == TOKEN0
    assert reserves.token1.address == TOKEN1
    assert reserves.reserve0 == 3
    assert reserves.reserve1 == 5
    assert reserves.fee_tier == 500
    assert reserves.sqrt_price_x96 == 2**97
    assert view.resolve_pool_address(TOKEN1, TOKEN0, "POLYGON", 500) == POOL
    assert view.resolve_pool_address(TOKEN0, TOKEN1, "polygon", 3000) is None
    untiered = HistoricalPoolStateTarget("polygon", "uniswap_v3", POOL, (TOKEN0, TOKEN1))
    untiered_source = SnapshotPoolStateSource(
        start_time=START,
        end_time=END,
        sample_interval_seconds=3_600,
        fetcher=_fetcher,
    )
    asyncio.run(untiered_source.materialize_history(untiered))
    assert untiered_source.view_at(END).resolve_pool_address(TOKEN0, TOKEN1, "polygon", 500) == POOL
    assert untiered_source.view_at(END).resolve_pool_address(TOKEN0, TOKEN1, "polygon", 3000) is None
    entries = manifest.entries()
    assert len(entries) == 1
    assert entries[0]["lane"] == LANE_POOL_STATE
    assert entries[0]["count"] == 2


def test_gateway_fetch_validates_and_maps_exact_pool_state(monkeypatch: pytest.MonkeyPatch) -> None:
    from almanak.framework.backtesting.pnl.providers import twap

    response = SimpleNamespace(
        success=True,
        error="",
        source="on_chain_archive",
        dex="uniswap_v3",
        chain="polygon",
        pool_address=POOL,
        points=[_gateway_point(1_000, 100), _gateway_point(4_590, 460)],
    )
    rate_history = SimpleNamespace(GetDexPoolStateSeries=lambda _request, timeout: response)
    client = SimpleNamespace(rate_history=rate_history, config=SimpleNamespace(timeout=30))
    gateway_pb2 = SimpleNamespace(GetDexPoolStateSeriesRequest=lambda **values: SimpleNamespace(**values))
    monkeypatch.setattr(twap, "_twap_get_connected_gateway_client", lambda: (client, gateway_pb2))

    points = fetch_historical_pool_state_points(
        protocol=" Uniswap_V3 ",
        chain="POLYGON",
        pool_address=POOL.upper(),
        start_ts=1_000,
        end_ts=4_600,
        interval_secs=3_600,
    )

    assert [(point.timestamp, point.block_number) for point in points] == [(1_000, 100), (4_590, 460)]
    assert all(point.source == "on_chain_archive" for point in points)


def test_gateway_fetch_refuses_malformed_or_unproven_responses(monkeypatch: pytest.MonkeyPatch) -> None:
    from almanak.framework.backtesting.pnl.providers import twap

    responses = iter(
        (
            SimpleNamespace(
                success=True,
                error="",
                source="",
                dex="uniswap_v3",
                chain="polygon",
                pool_address=POOL,
                points=[_gateway_point(1_000, 100)],
            ),
            SimpleNamespace(
                success=True,
                error="",
                source="on_chain_archive",
                dex="uniswap_v3",
                chain="polygon",
                pool_address=POOL,
                points=[_gateway_point(1_001, 100)],
            ),
        )
    )
    rate_history = SimpleNamespace(GetDexPoolStateSeries=lambda _request, timeout: next(responses))
    client = SimpleNamespace(rate_history=rate_history, config=SimpleNamespace(timeout=30))
    gateway_pb2 = SimpleNamespace(GetDexPoolStateSeriesRequest=lambda **values: SimpleNamespace(**values))
    monkeypatch.setattr(twap, "_twap_get_connected_gateway_client", lambda: (client, gateway_pb2))
    kwargs = {
        "protocol": "uniswap_v3",
        "chain": "polygon",
        "pool_address": POOL,
        "start_ts": 1_000,
        "end_ts": 1_000,
        "interval_secs": 3_600,
    }

    with pytest.raises(DataSourceUnavailable, match="omitted provenance"):
        fetch_historical_pool_state_points(**kwargs)
    with pytest.raises(DataSourceUnavailable, match="no-lookahead"):
        fetch_historical_pool_state_points(**kwargs)


def test_materialization_rejects_fee_tier_mismatch() -> None:
    def wrong_fetcher(**_kwargs):
        point = _fetcher(pool_address=POOL)[0]
        wrong_fee = replace(point, fee_tier=3000)
        return [wrong_fee, replace(wrong_fee, timestamp=4_590)]

    source = SnapshotPoolStateSource(
        start_time=START,
        end_time=END,
        sample_interval_seconds=3_600,
        fetcher=wrong_fetcher,
    )
    with pytest.raises(ValueError, match="pool fee mismatch"):
        asyncio.run(source.materialize_history(TARGET))


def test_materialization_rejects_pool_fee_identity_drift() -> None:
    def drifting_fetcher(**_kwargs):
        point = _fetcher(pool_address=POOL)[0]
        return [point, replace(point, timestamp=4_590, fee_tier=3000)]

    source = SnapshotPoolStateSource(
        start_time=START,
        end_time=END,
        sample_interval_seconds=3_600,
        fetcher=drifting_fetcher,
    )
    with pytest.raises(ValueError, match="pool fee identity drift"):
        asyncio.run(source.materialize_history(HistoricalPoolStateTarget("polygon", "uniswap_v3", POOL)))


def test_materialization_rejects_pool_token_identity_mismatch() -> None:
    other_token = "0x1111111111111111111111111111111111111111"

    def wrong_fetcher(**_kwargs):
        point = _fetcher(pool_address=POOL)[0]
        return [point, replace(point, timestamp=4_590, token1=other_token)]

    source = SnapshotPoolStateSource(
        start_time=START,
        end_time=END,
        sample_interval_seconds=3_600,
        fetcher=wrong_fetcher,
    )
    with pytest.raises(ValueError, match="pool token identity mismatch"):
        asyncio.run(source.materialize_history(TARGET))


def test_undeclared_pool_refuses_instead_of_using_token_ratio() -> None:
    source = SnapshotPoolStateSource(
        start_time=START,
        end_time=END,
        sample_interval_seconds=3_600,
        fetcher=_fetcher,
    )
    asyncio.run(source.materialize_history(TARGET))
    with pytest.raises(PoolPriceUnavailableError, match="not declared and prewarmed"):
        source.view_at(END).read_pool_price("0x0000000000000000000000000000000000000001", "polygon")
