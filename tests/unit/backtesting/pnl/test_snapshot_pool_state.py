"""Exact-address historical pool-state declaration and snapshot tests."""

from __future__ import annotations

import asyncio
from dataclasses import replace
from datetime import UTC, datetime
from decimal import Decimal
from types import SimpleNamespace

import grpc
import pytest

from almanak.framework.backtesting.pnl import _engine_helpers
from almanak.framework.backtesting.pnl.config import PnLBacktestConfig
from almanak.framework.backtesting.pnl.data_manifest import LANE_POOL_STATE, LANE_POOL_TVL, RunDataManifest
from almanak.framework.backtesting.pnl.data_provider import HistoricalPriceObservation, MarketState
from almanak.framework.backtesting.pnl.providers.snapshot_pool_state import (
    HistoricalPoolStatePoint,
    HistoricalPoolStateTarget,
    SnapshotPoolStateSource,
    declared_historical_pool_state_targets,
    fetch_historical_pool_state_points,
)
from almanak.framework.backtesting.pnl.types import DataConfidence
from almanak.framework.data.interfaces import DataSourceTimeout, DataSourceUnavailable
from almanak.framework.data.models import DataClassification
from almanak.framework.market.errors import PoolPriceUnavailableError

POOL = "0x9b08288c3be4f62bbf8d1c20ac9c5e6f9467d8b7"
TOKEN0 = "0x0d500b1d8e8ef31e21c99d1db9a6444d3adf1270"
TOKEN1 = "0xc2132d05d31c914a87c6611c10748aeb04b58e8f"
START = datetime.fromtimestamp(1_000, UTC)
END = datetime.fromtimestamp(4_600, UTC)
TARGET = HistoricalPoolStateTarget("polygon", "uniswap_v3", POOL, (TOKEN0, TOKEN1), 500)
ADDRESS_ONLY_TARGET = HistoricalPoolStateTarget("bsc", "pancakeswap_v3", POOL)


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


def test_target_and_source_share_canonical_chain_identity() -> None:
    target = HistoricalPoolStateTarget("arb", "uniswap-v3", POOL)
    assert target.chain == "arbitrum"
    assert target.key == ("arbitrum", "uniswap_v3", POOL)


def test_async_materialization_is_idempotent_across_chain_aliases() -> None:
    fetches = 0

    def counting_fetcher(**kwargs):
        nonlocal fetches
        fetches += 1
        return _fetcher(**kwargs)

    source = SnapshotPoolStateSource(
        start_time=START,
        end_time=END,
        sample_interval_seconds=3_600,
        fetcher=counting_fetcher,
    )
    target = HistoricalPoolStateTarget("matic", "uniswap_v3", POOL, (TOKEN0, TOKEN1), 500)

    asyncio.run(source.materialize_history(target))
    asyncio.run(
        source.materialize_history(HistoricalPoolStateTarget("polygon", "uniswap_v3", POOL, (TOKEN0, TOKEN1), 500))
    )
    with pytest.raises(ValueError, match="pool fee mismatch"):
        asyncio.run(source.materialize_history(HistoricalPoolStateTarget("polygon", "uniswap_v3", POOL, fee_tier=3000)))

    assert fetches == 1
    assert source._resolve("matic", "uniswap_v3", POOL, 1_000)[0].chain == "polygon"


@pytest.mark.asyncio
async def test_declared_prewarm_accepts_an_equivalent_run_chain_alias(monkeypatch: pytest.MonkeyPatch) -> None:
    strategy = SimpleNamespace(backtest_pool_state_targets=[TARGET])
    config = PnLBacktestConfig(
        start_time=START,
        end_time=END,
        interval_seconds=3_600,
        chain="matic",
    )
    monkeypatch.setattr(
        "almanak.framework.backtesting.pnl.providers.snapshot_pool_state.fetch_historical_pool_state_points",
        _fetcher,
    )

    source = await _engine_helpers._prepare_declared_historical_pool_state(strategy, {}, config, None)

    assert source is not None
    assert source.pool_descriptor("matic", "uniswap_v3", POOL) is not None


def test_typed_pool_state_declaration_is_the_only_declaration() -> None:
    custom_target = HistoricalPoolStateTarget("ethereum", "custom_pool_provider", POOL)
    strategy = SimpleNamespace(
        backtest_pool_state_targets=[custom_target],
        swap_pool="0x0000000000000000000000000000000000000001",
    )

    assert declared_historical_pool_state_targets(strategy, {}, default_chain="bsc") == (custom_target,)


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
    descriptor = source.pool_descriptor("POLYGON", "Uniswap-V3", POOL.upper())
    assert descriptor is not None
    assert (descriptor.token0, descriptor.token1) == (TOKEN0, TOKEN1)
    assert descriptor.fee_tier_units == 500
    assert descriptor.fee_rate == Decimal("0.0005")
    assert descriptor.provenance == "historical:on_chain_archive"
    assert descriptor.factory == "0x1f98431c8ad98523631ae4a59f267346ea31f984"
    assert source.descriptors() == (descriptor,)
    assert source.verification_block("POLYGON", "Uniswap-V3", POOL.upper()) == 100
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
    assert view.protocols_for_chain("matic") == ["uniswap_v3"]
    assert view.resolve_pool_address(TOKEN1, TOKEN0, "matic", 500) == POOL
    assert view.resolve_pool_address(TOKEN1, TOKEN0, "POLYGON", 500) == POOL
    assert view.resolve_pool_address(TOKEN0, TOKEN1, "polygon", 3000) is None
    untiered = HistoricalPoolStateTarget("polygon", "uniswap_v3", POOL)
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


def test_snapshot_values_exact_pool_balances_with_tick_prices() -> None:
    manifest = RunDataManifest()
    source = SnapshotPoolStateSource(
        start_time=START,
        end_time=END,
        sample_interval_seconds=3_600,
        manifest=manifest,
        fetcher=_fetcher,
    )
    asyncio.run(source.materialize_history(TARGET))
    state = MarketState(
        timestamp=END,
        prices={("polygon", TOKEN0): Decimal("2"), ("polygon", TOKEN1): Decimal("1")},
        price_observations={
            ("polygon", TOKEN0): HistoricalPriceObservation(
                price=Decimal("2"), timestamp=END, source="coingecko", confidence=DataConfidence.MEDIUM
            ),
            ("polygon", TOKEN1): HistoricalPriceObservation(
                price=Decimal("1"), timestamp=END, source="coingecko", confidence=DataConfidence.MEDIUM
            ),
        },
        chain="polygon",
    )

    envelope = source.view_at(END).read_pool_tvl_usd(POOL, "polygon", "uniswap_v3", state)

    assert envelope.value.tvl_usd == Decimal("11")
    assert envelope.value.token0_value_usd == Decimal("6")
    assert envelope.value.token1_value_usd == Decimal("5")
    assert envelope.meta.block_number == 460
    assert envelope.meta.staleness_ms == 10_000
    assert envelope.is_fresh
    assert "historical:on_chain_archive" in envelope.meta.source
    assert any(entry["lane"] == LANE_POOL_TVL for entry in manifest.entries())


def test_snapshot_ignores_scalar_only_leg_and_values_it_through_exact_pool_spot() -> None:
    def balanced_fetcher(**_kwargs):
        return [
            HistoricalPoolStatePoint(
                1_000,
                100,
                2**96,
                0,
                9_000,
                TOKEN0,
                TOKEN1,
                18,
                18,
                500,
                2 * 10**18,
                4 * 10**18,
                "on_chain_archive",
            ),
            HistoricalPoolStatePoint(
                4_590,
                460,
                2**96,
                0,
                9_000,
                TOKEN0,
                TOKEN1,
                18,
                18,
                500,
                2 * 10**18,
                4 * 10**18,
                "on_chain_archive",
            ),
        ]

    source = SnapshotPoolStateSource(
        start_time=START,
        end_time=END,
        sample_interval_seconds=3_600,
        fetcher=balanced_fetcher,
    )
    asyncio.run(source.materialize_history(TARGET))
    state = MarketState(
        timestamp=END,
        prices={("polygon", TOKEN0): Decimal("10"), ("polygon", TOKEN1): Decimal("999999")},
        price_observations={
            ("polygon", TOKEN0): HistoricalPriceObservation(
                price=Decimal("10"), timestamp=END, source="coingecko", confidence=DataConfidence.MEDIUM
            )
        },
        chain="polygon",
    )

    envelope = source.view_at(END).read_pool_tvl_usd(POOL, "polygon", "uniswap_v3", state)

    assert envelope.value.tvl_usd == Decimal("60")
    assert envelope.meta.confidence == pytest.approx(0.95)


def test_snapshot_tvl_refuses_scalar_prices_without_measured_observations() -> None:
    source = SnapshotPoolStateSource(
        start_time=START,
        end_time=END,
        sample_interval_seconds=3_600,
        fetcher=_fetcher,
    )
    asyncio.run(source.materialize_history(TARGET))
    state = MarketState(
        timestamp=END,
        prices={("polygon", TOKEN0): Decimal("2"), ("polygon", TOKEN1): Decimal("1")},
        chain="polygon",
    )

    with pytest.raises(ValueError, match="no historical USD price for either pool token"):
        source.view_at(END).read_pool_tvl_usd(POOL, "polygon", "uniswap_v3", state)


def test_snapshot_tvl_refuses_when_neither_pool_token_has_a_price() -> None:
    source = SnapshotPoolStateSource(
        start_time=START,
        end_time=END,
        sample_interval_seconds=3_600,
        fetcher=_fetcher,
    )
    asyncio.run(source.materialize_history(TARGET))
    state = MarketState(timestamp=END, prices={}, chain="polygon")

    with pytest.raises(ValueError, match="no historical USD price for either pool token"):
        source.view_at(END).read_pool_tvl_usd(POOL, "polygon", "uniswap_v3", state)


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


def test_gateway_fetch_pages_long_windows_below_the_gateway_deadline(monkeypatch: pytest.MonkeyPatch) -> None:
    from almanak.framework.backtesting.pnl.providers import twap

    requests = []

    def get_series(request, timeout):
        assert timeout == 30
        requests.append(request)
        samples = range(request.start_ts, request.end_ts + 1, request.interval_secs)
        return SimpleNamespace(
            success=True,
            error="",
            source="on_chain_archive",
            dex="uniswap_v3",
            chain="polygon",
            pool_address=POOL,
            points=[_gateway_point(sample, sample) for sample in samples],
        )

    client = SimpleNamespace(
        rate_history=SimpleNamespace(GetDexPoolStateSeries=get_series),
        config=SimpleNamespace(timeout=30),
    )
    gateway_pb2 = SimpleNamespace(GetDexPoolStateSeriesRequest=lambda **values: SimpleNamespace(**values))
    monkeypatch.setattr(twap, "_twap_get_connected_gateway_client", lambda: (client, gateway_pb2))

    points = fetch_historical_pool_state_points(
        protocol="uniswap_v3",
        chain="polygon",
        pool_address=POOL,
        start_ts=1_000,
        end_ts=1_000 + 128 * 3_600,
        interval_secs=3_600,
    )

    assert len(points) == 129
    assert [(request.end_ts - request.start_ts) // request.interval_secs + 1 for request in requests] == [128, 1]


def test_gateway_fetch_splits_a_page_that_hits_the_deadline(monkeypatch: pytest.MonkeyPatch) -> None:
    """Archive latency varies per page; one slow page must not fail the run."""
    from almanak.framework.backtesting.pnl.providers import twap

    class DeadlineExceeded(grpc.RpcError):
        def code(self):
            return grpc.StatusCode.DEADLINE_EXCEEDED

    sizes = []

    def get_series(request, timeout):
        samples = list(range(request.start_ts, request.end_ts + 1, request.interval_secs))
        sizes.append(len(samples))
        if len(samples) > 64:
            raise DeadlineExceeded()
        return SimpleNamespace(
            success=True,
            error="",
            source="on_chain_archive",
            dex="uniswap_v3",
            chain="polygon",
            pool_address=POOL,
            points=[_gateway_point(sample, sample) for sample in samples],
        )

    client = SimpleNamespace(
        rate_history=SimpleNamespace(GetDexPoolStateSeries=get_series),
        config=SimpleNamespace(timeout=30),
    )
    gateway_pb2 = SimpleNamespace(GetDexPoolStateSeriesRequest=lambda **values: SimpleNamespace(**values))
    monkeypatch.setattr(twap, "_twap_get_connected_gateway_client", lambda: (client, gateway_pb2))

    points = fetch_historical_pool_state_points(
        protocol="uniswap_v3",
        chain="polygon",
        pool_address=POOL,
        start_ts=1_000,
        end_ts=1_000 + 128 * 3_600,
        interval_secs=3_600,
    )

    assert [point.timestamp for point in points] == list(range(1_000, 1_000 + 129 * 3_600, 3_600))
    assert sizes == [128, 64, 64, 1]


def test_gateway_fetch_refuses_when_the_floor_page_still_times_out(monkeypatch: pytest.MonkeyPatch) -> None:
    from almanak.framework.backtesting.pnl.providers import twap

    class DeadlineExceeded(grpc.RpcError):
        def code(self):
            return grpc.StatusCode.DEADLINE_EXCEEDED

    sizes = []

    def get_series(request, timeout):
        sizes.append((request.end_ts - request.start_ts) // request.interval_secs + 1)
        raise DeadlineExceeded()

    client = SimpleNamespace(
        rate_history=SimpleNamespace(GetDexPoolStateSeries=get_series),
        config=SimpleNamespace(timeout=30),
    )
    gateway_pb2 = SimpleNamespace(GetDexPoolStateSeriesRequest=lambda **values: SimpleNamespace(**values))
    monkeypatch.setattr(twap, "_twap_get_connected_gateway_client", lambda: (client, gateway_pb2))

    with pytest.raises(DataSourceTimeout, match="timed out after 30.0s"):
        fetch_historical_pool_state_points(
            protocol="uniswap_v3",
            chain="polygon",
            pool_address=POOL,
            start_ts=1_000,
            end_ts=1_000 + 127 * 3_600,
            interval_secs=3_600,
        )

    assert sizes == [128, 64, 32, 16, 8]


@pytest.mark.parametrize(
    ("points", "sizes"),
    [
        pytest.param(9, [9], id="just-above-floor-is-not-split"),
        pytest.param(15, [15], id="below-two-floor-pages-is-not-split"),
        pytest.param(16, [16, 8], id="two-floor-pages-split-once"),
    ],
)
def test_gateway_fetch_never_retries_a_page_below_the_floor(
    monkeypatch: pytest.MonkeyPatch, points: int, sizes: list[int]
) -> None:
    from almanak.framework.backtesting.pnl.providers import twap

    class DeadlineExceeded(grpc.RpcError):
        def code(self):
            return grpc.StatusCode.DEADLINE_EXCEEDED

    seen = []

    def get_series(request, timeout):
        seen.append((request.end_ts - request.start_ts) // request.interval_secs + 1)
        raise DeadlineExceeded()

    client = SimpleNamespace(
        rate_history=SimpleNamespace(GetDexPoolStateSeries=get_series),
        config=SimpleNamespace(timeout=30),
    )
    gateway_pb2 = SimpleNamespace(GetDexPoolStateSeriesRequest=lambda **values: SimpleNamespace(**values))
    monkeypatch.setattr(twap, "_twap_get_connected_gateway_client", lambda: (client, gateway_pb2))

    with pytest.raises(DataSourceTimeout):
        fetch_historical_pool_state_points(
            protocol="uniswap_v3",
            chain="polygon",
            pool_address=POOL,
            start_ts=1_000,
            end_ts=1_000 + (points - 1) * 3_600,
            interval_secs=3_600,
        )

    assert seen == sizes
    assert all(size >= 8 for size in seen)


def test_gateway_fetch_maps_client_deadline_to_retryable_timeout(monkeypatch: pytest.MonkeyPatch) -> None:
    from almanak.framework.backtesting.pnl.providers import twap

    class DeadlineExceeded(grpc.RpcError):
        def code(self):
            return grpc.StatusCode.DEADLINE_EXCEEDED

    def get_series(_request, timeout):
        assert timeout == 30
        raise DeadlineExceeded()

    client = SimpleNamespace(
        rate_history=SimpleNamespace(GetDexPoolStateSeries=get_series),
        config=SimpleNamespace(timeout=30),
    )
    gateway_pb2 = SimpleNamespace(GetDexPoolStateSeriesRequest=lambda **values: SimpleNamespace(**values))
    monkeypatch.setattr(twap, "_twap_get_connected_gateway_client", lambda: (client, gateway_pb2))

    with pytest.raises(DataSourceTimeout, match="timed out after 30.0s"):
        fetch_historical_pool_state_points(
            protocol="uniswap_v3",
            chain="polygon",
            pool_address=POOL,
            start_ts=1_000,
            end_ts=1_000,
            interval_secs=3_600,
        )


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


def test_materialization_rejects_derived_pool_token_metadata_drift() -> None:
    def drifting_fetcher(**_kwargs):
        point = _fetcher(pool_address=POOL)[0]
        return [point, replace(point, timestamp=4_590, token1_decimals=8)]

    source = SnapshotPoolStateSource(
        start_time=START,
        end_time=END,
        sample_interval_seconds=3_600,
        fetcher=drifting_fetcher,
    )
    with pytest.raises(ValueError, match="pool token metadata drift"):
        asyncio.run(source.materialize_history(HistoricalPoolStateTarget("polygon", "uniswap_v3", POOL)))


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


def test_pool_descriptor_factory_is_named_only_when_one_generation_can_own_the_pool(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Several reviewed factory generations leave the descriptor's factory unmeasured, never guessed."""
    from almanak.connectors import _strategy_pool_reader_registry as registry_module

    source = SnapshotPoolStateSource(
        start_time=START,
        end_time=END,
        sample_interval_seconds=3_600,
        manifest=RunDataManifest(),
        fetcher=_fetcher,
    )
    assert asyncio.run(source.materialize_history(TARGET)) == 2
    cases = (
        ((), None),
        (("0x1f98431c8ad98523631ae4a59f267346ea31f984",), "0x1f98431c8ad98523631ae4a59f267346ea31f984"),
        (("0x" + "aa" * 20, "0x" + "bb" * 20), None),
    )
    for factories, expected in cases:
        monkeypatch.setattr(
            registry_module.POOL_READER_REGISTRY,
            "lookup",
            lambda _protocol, found=factories: SimpleNamespace(factories_for=lambda _chain: found),
        )
        descriptor = source.pool_descriptor("POLYGON", "Uniswap-V3", POOL.upper())
        assert descriptor is not None
        assert descriptor.factory == expected
        assert (descriptor.token0, descriptor.token1) == (TOKEN0, TOKEN1)


def _tvl_market_state() -> MarketState:
    return MarketState(
        timestamp=END,
        prices={("polygon", TOKEN0): Decimal("2"), ("polygon", TOKEN1): Decimal("1")},
        price_observations={
            ("polygon", TOKEN0): HistoricalPriceObservation(
                price=Decimal("2"), timestamp=END, source="coingecko", confidence=DataConfidence.MEDIUM
            ),
            ("polygon", TOKEN1): HistoricalPriceObservation(
                price=Decimal("1"), timestamp=END, source="coingecko", confidence=DataConfidence.MEDIUM
            ),
        },
        chain="polygon",
    )


def test_tvl_read_materializes_undeclared_exact_pool_at_first_use() -> None:
    """A protocol-scoped TVL read on an undeclared pool fetches its archive state inline, once."""
    calls: list[dict] = []
    events: list[str] = []

    def counting_fetcher(**kwargs):
        events.append("fetch")
        calls.append(kwargs)
        return _fetcher(**kwargs)

    source = SnapshotPoolStateSource(
        start_time=START,
        end_time=END,
        sample_interval_seconds=3_600,
        fetcher=counting_fetcher,
        first_use_feasibility=lambda: events.append("feasibility"),
    )
    assert source.is_empty

    first = source.view_at(END).read_pool_tvl_usd(POOL, "polygon", "uniswap_v3", _tvl_market_state())
    second = source.view_at(END).read_pool_tvl_usd(POOL, "polygon", "uniswap_v3", _tvl_market_state())

    assert len(calls) == 1 and calls[0]["pool_address"] == POOL
    assert events == ["feasibility", "fetch"]
    assert first.value.tvl_usd == second.value.tvl_usd == Decimal("11")
    assert not source.is_empty
    assert source.pool_descriptor("polygon", "uniswap_v3", POOL) is not None


def test_tvl_read_remembers_first_use_fetch_failure() -> None:
    """An archive that cannot serve the pool is asked once; later reads refuse without refetching."""
    calls: list[dict] = []

    def failing_fetcher(**kwargs):
        calls.append(kwargs)
        raise ValueError("archive RPC unavailable")

    source = SnapshotPoolStateSource(
        start_time=START, end_time=END, sample_interval_seconds=3_600, fetcher=failing_fetcher
    )
    for _ in range(2):
        with pytest.raises(PoolPriceUnavailableError, match="first-use exact-pool state fetch failed"):
            source.view_at(END).read_pool_tvl_usd(POOL, "polygon", "uniswap_v3", _tvl_market_state())
    assert len(calls) == 1
    assert source.is_empty


def test_pool_price_read_without_protocol_keeps_fallback_semantics() -> None:
    """``pool_price`` carries no protocol, so it cannot authenticate a pool: unchanged refusal."""
    source = SnapshotPoolStateSource(start_time=START, end_time=END, sample_interval_seconds=3_600, fetcher=_fetcher)
    with pytest.raises(PoolPriceUnavailableError, match="not declared and prewarmed"):
        source.view_at(END).read_pool_price(POOL, "polygon")
