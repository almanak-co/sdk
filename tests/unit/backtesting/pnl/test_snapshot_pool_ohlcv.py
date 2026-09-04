from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

import pytest

import almanak.framework.backtesting.pnl.providers.snapshot_pool_ohlcv as exact_ohlcv_module
from almanak.core.asset_identity import AssetIdentity, AssetNamespace
from almanak.framework.backtesting.pnl.data_manifest import LANE_OHLCV, RunDataManifest
from almanak.framework.backtesting.pnl.data_provider import MarketState
from almanak.framework.backtesting.pnl.engine import BacktestOHLCVView, create_market_snapshot_from_state
from almanak.framework.backtesting.pnl.indicator_engine import BacktestIndicatorEngine
from almanak.framework.backtesting.pnl.providers import HistoricalPoolOHLCVTarget, SnapshotExactPoolOHLCVSource
from almanak.framework.backtesting.pnl.providers.snapshot_pool_ohlcv import declared_historical_pool_ohlcv_targets
from almanak.framework.data.interfaces import OHLCVCandle
from almanak.framework.data.pools.descriptor import PoolDescriptor
from almanak.framework.data.timeframes import OHLCVTimeframe
from almanak.framework.primitives.types import Primitive
from almanak.framework.venues import (
    ExactVenueObservation,
    VenueBindingComponent,
    VenueDataFailure,
    VenueDataFailureReason,
    VenueDataFailureState,
    VenueDataProvenance,
    VenueObservationAnchor,
    VenueObservedFact,
    VenueReferenceNamespace,
    VenueTargetRef,
    VenueTargetRole,
    VenueVerificationEvidence,
    build_verified_venue_binding,
)

POOL = "0x89001d846f7ca36ee089f73eefc25657e1798144"
TOKEN0 = "0x3f53de71c126bdabae20f9cd64848d317f6c3238"
TOKEN1 = "0x55d398326f99059ff775485246999027b3197955"
ROUTER = "0x1111111111111111111111111111111111111111"
START_BLOCK = 113_161_920
TICK = datetime(2026, 8, 2, 12, tzinfo=UTC)
END = datetime(2026, 8, 2, 13, tzinfo=UTC)

DESCRIPTOR = PoolDescriptor(
    chain="bsc",
    protocol="pancakeswap_v3",
    address=POOL,
    token0=TOKEN0,
    token1=TOKEN1,
    token0_decimals=18,
    token1_decimals=18,
    fee_tier_units=100,
    provenance="historical:on_chain_archive",
)


class _PoolStateSource:
    def descriptors(self) -> tuple[PoolDescriptor, ...]:
        return (DESCRIPTOR,)

    def verification_block(self, chain: str, protocol: str, pool_address: str) -> int:
        assert (chain, protocol, pool_address) == DESCRIPTOR.key
        return START_BLOCK


def _new_source(*, start: datetime = TICK, end: datetime = END) -> SnapshotExactPoolOHLCVSource:
    return SnapshotExactPoolOHLCVSource(
        _PoolStateSource(),
        start_time=start,
        end_time=end,
        token_addresses={"GOOGLB": ("bsc", TOKEN0), "BSC-USD": ("bsc", TOKEN1)},
    )


def _verified():
    pool = VenueTargetRef(VenueTargetRole.POOL, VenueReferenceNamespace.EVM_ADDRESS, POOL)
    router = VenueTargetRef(VenueTargetRole.ROUTER, VenueReferenceNamespace.EVM_ADDRESS, ROUTER)
    return build_verified_venue_binding(
        chain="bsc",
        protocol="pancakeswap_v3",
        primitive=Primitive.SWAP,
        identity_refs=(pool,),
        binding_components=(VenueBindingComponent("fee", "100"),),
        ordered_assets=(
            AssetIdentity("bsc", AssetNamespace.ERC20, TOKEN0),
            AssetIdentity("bsc", AssetNamespace.ERC20, TOKEN1),
        ),
        binding_policy_version=1,
        operational_refs=(router,),
        evidence=VenueVerificationEvidence(
            chain="bsc",
            verifier_ref="almanak.connectors._strategy_base.v3_venue_verifier:V3VenueVerifier",
            verifier_contract_version="v3_exact_pool.v1",
            block_number=START_BLOCK,
            block_hash="0x" + "44" * 32,
            observed_facts=(VenueObservedFact("router", router.reference, router),),
        ),
    )


@pytest.fixture
def exact_source(monkeypatch: pytest.MonkeyPatch):
    requests = []
    verified = _verified()
    client = object()

    def verify(descriptor: PoolDescriptor, block_number: int):
        assert descriptor == DESCRIPTOR
        assert block_number == START_BLOCK
        return verified, client

    def observe(request: Any, gateway_client: Any):
        assert gateway_client is client
        requests.append(request)
        parameters = request.parameters
        candles = tuple(
            OHLCVCandle(
                timestamp=datetime.fromtimestamp(timestamp, tz=UTC),
                open=Decimal("2"),
                high=Decimal("3"),
                low=Decimal("1"),
                close=Decimal("2.5"),
                volume=Decimal("10"),
            )
            for timestamp in range(
                int(parameters.start_at.timestamp()),
                int(parameters.end_at.timestamp()),
                parameters.timeframe.seconds,
            )
        )
        return ExactVenueObservation.from_request(
            request=request,
            value=candles,
            anchor=VenueObservationAnchor(observed_at=END, block_number=None, block_hash=None),
            provenance=VenueDataProvenance(
                provider_ref="tests.unit.backtesting.pnl.test_snapshot_pool_ohlcv:FakeProvider",
                provider_contract_version="v1",
                source="coingecko_onchain.exact_pool",
            ),
        )

    monkeypatch.setattr(exact_ohlcv_module, "_verify_descriptor", verify)
    monkeypatch.setattr(exact_ohlcv_module, "observe_exact_venue_data", observe)
    source = _new_source()
    asyncio.run(
        source.materialize_history(
            HistoricalPoolOHLCVTarget(
                "bsc",
                "pancakeswap_v3",
                POOL,
                TOKEN0,
                TOKEN1,
                OHLCVTimeframe.FIFTEEN_MINUTES,
                2,
            )
        )
    )
    return source, requests


def test_exact_pool_candles_preserve_identity_and_hide_future_buckets(exact_source) -> None:
    source, requests = exact_source

    frame = source.get_pool_ohlcv(
        pool_address=POOL,
        chain="bsc",
        timestamp=TICK,
        timeframe="15m",
        limit=2,
        requested_symbol="GOOGLB/BSC-USD",
    )

    assert list(frame["timestamp"]) == [
        datetime(2026, 8, 2, 11, 30, tzinfo=UTC),
        datetime(2026, 8, 2, 11, 45, tzinfo=UTC),
    ]
    assert frame.attrs["pool_address"] == POOL
    assert frame.attrs["protocol"] == "pancakeswap_v3"
    assert frame.attrs["base_asset"] == TOKEN0
    assert frame.attrs["quote_asset"] == TOKEN1
    assert frame.attrs["source"] == "coingecko_onchain.exact_pool"
    assert requests[0].parameters.start_at == datetime(2026, 8, 2, 11, 30, tzinfo=UTC)
    assert requests[0].parameters.end_at == END

    source.get_pool_ohlcv(
        pool_address=POOL,
        chain="bsc",
        timestamp=TICK,
        timeframe="15m",
        limit=2,
        requested_symbol="GOOGLB/BSC-USD",
    )
    assert len(requests) == 1

    later = source.get_pool_ohlcv(
        pool_address=POOL,
        chain="bsc",
        timestamp=datetime(2026, 8, 2, 12, 15, tzinfo=UTC),
        timeframe="15m",
        limit=2,
        requested_symbol=TOKEN0,
    )
    assert later["timestamp"].iloc[-1] == datetime(2026, 8, 2, 12, tzinfo=UTC)
    assert len(requests) == 1


def test_materialized_range_serves_more_than_thirty_ticks_with_one_request(exact_source) -> None:
    _source, requests = exact_source
    start = TICK
    end = start + timedelta(minutes=15 * 64)
    source = _new_source(start=start, end=end)
    target = HistoricalPoolOHLCVTarget(
        "bsc",
        "pancakeswap_v3",
        POOL,
        TOKEN0,
        TOKEN1,
        OHLCVTimeframe.FIFTEEN_MINUTES,
        2,
    )
    before = len(requests)

    asyncio.run(source.materialize_history(target))
    for index in range(65):
        tick = start + timedelta(minutes=15 * index)
        frame = source.get_pool_ohlcv(
            pool_address=POOL,
            chain="bsc",
            timestamp=tick,
            timeframe="15m",
            limit=2,
            requested_symbol=TOKEN0,
        )
        assert frame["timestamp"].iloc[-1] < tick

    assert len(requests) - before == 1
    assert requests[-1].parameters.end_at == end


def test_materialization_uses_bounded_provider_pages(exact_source) -> None:
    _source, requests = exact_source
    end = TICK + timedelta(minutes=15 * 1_050)
    source = _new_source(start=TICK, end=end)
    target = HistoricalPoolOHLCVTarget(
        "bsc",
        "pancakeswap_v3",
        POOL,
        TOKEN0,
        TOKEN1,
        OHLCVTimeframe.FIFTEEN_MINUTES,
        2,
    )
    before = len(requests)

    asyncio.run(source.materialize_history(target))

    pages = requests[before:]
    assert len(pages) == 2
    assert [request.parameters.end_at for request in pages] == [
        TICK - timedelta(minutes=30) + timedelta(minutes=15 * 1_000),
        end,
    ]


def test_materialization_refuses_a_gap_without_committing_partial_cache(
    exact_source,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _source, requests = exact_source
    source = _new_source(end=TICK + timedelta(minutes=15 * 1_050))
    target = HistoricalPoolOHLCVTarget(
        "bsc",
        "pancakeswap_v3",
        POOL,
        TOKEN0,
        TOKEN1,
        OHLCVTimeframe.FIFTEEN_MINUTES,
        2,
    )

    def observe_with_gap(request: Any, _gateway_client: Any) -> ExactVenueObservation:
        requests.append(request)
        parameters = request.parameters
        timestamps = tuple(
            range(
                int(parameters.start_at.timestamp()),
                int(parameters.end_at.timestamp()),
                parameters.timeframe.seconds,
            )
        )
        candles = tuple(
            OHLCVCandle(
                timestamp=datetime.fromtimestamp(timestamp, tz=UTC),
                open=Decimal("2"),
                high=Decimal("3"),
                low=Decimal("1"),
                close=Decimal("2.5"),
                volume=Decimal("10"),
            )
            for timestamp in timestamps[1:]
        )
        return ExactVenueObservation.from_request(
            request=request,
            value=candles,
            anchor=VenueObservationAnchor(observed_at=END, block_number=None, block_hash=None),
            provenance=VenueDataProvenance(
                provider_ref="tests.unit.backtesting.pnl.test_snapshot_pool_ohlcv:FakeProvider",
                provider_contract_version="v1",
                source="coingecko_onchain.exact_pool",
            ),
        )

    monkeypatch.setattr(exact_ohlcv_module, "observe_exact_venue_data", observe_with_gap)
    before = len(requests)

    with pytest.raises(ValueError, match="complete interval coverage"):
        asyncio.run(source.materialize_history(target))
    assert len(requests) - before == 2
    with pytest.raises(ValueError, match="not declared and prewarmed"):
        source.get_pool_ohlcv(
            pool_address=POOL,
            chain="bsc",
            timestamp=TICK,
            timeframe="15m",
            limit=2,
            requested_symbol=TOKEN0,
        )


def test_requested_reverse_orientation_is_bound_into_the_exact_request(exact_source) -> None:
    source, requests = exact_source

    asyncio.run(
        source.materialize_history(
            HistoricalPoolOHLCVTarget(
                "bsc",
                "pancakeswap_v3",
                POOL,
                TOKEN1,
                TOKEN0,
                OHLCVTimeframe.FIFTEEN_MINUTES,
                2,
            )
        )
    )

    frame = source.get_pool_ohlcv(
        pool_address=POOL,
        chain="bsc",
        timestamp=TICK,
        timeframe="15m",
        limit=2,
        requested_symbol="BSC-USD/GOOGLB",
    )

    assert (requests[-1].parameters.base_asset_index, requests[-1].parameters.quote_asset_index) == (1, 0)
    assert frame.attrs["base_asset"] == TOKEN1
    assert frame.attrs["quote_asset"] == TOKEN0


def test_generated_exact_pool_shape_declares_default_lookback() -> None:
    class Strategy:
        swap_pool = POOL
        protocol = "pancakeswap_v3"
        STRATEGY_METADATA = type("Metadata", (), {"supported_protocols": ("pancakeswap_v3",)})()

    targets = declared_historical_pool_ohlcv_targets(
        Strategy(),
        {
            "swap_pool": POOL,
            "protocol": "pancakeswap_v3",
            "base_token": {"address": TOKEN0},
            "quote_token": {"address": TOKEN1},
            "data_granularity": "15m",
        },
        default_chain="bsc",
    )

    assert targets == (
        HistoricalPoolOHLCVTarget(
            "bsc",
            "pancakeswap_v3",
            POOL,
            TOKEN0,
            TOKEN1,
            OHLCVTimeframe.FIFTEEN_MINUTES,
            100,
        ),
    )


def test_undeclared_pool_and_mismatched_asset_refuse_without_provider_access(exact_source) -> None:
    source, requests = exact_source
    before = len(requests)
    kwargs = {
        "chain": "bsc",
        "timestamp": TICK,
        "timeframe": "15m",
        "limit": 2,
    }

    with pytest.raises(ValueError, match="not declared and prewarmed"):
        source.get_pool_ohlcv(pool_address="0x" + "9" * 40, requested_symbol=TOKEN0, **kwargs)
    with pytest.raises(ValueError, match="is not in exact pool"):
        source.get_pool_ohlcv(pool_address=POOL, requested_symbol="0x" + "8" * 40, **kwargs)
    with pytest.raises(ValueError, match="not declared and prewarmed"):
        source.get_pool_ohlcv(
            pool_address="0x" + "9" * 40,
            requested_symbol=TOKEN0,
            **{**kwargs, "limit": 0},
        )
    with pytest.raises(ValueError, match="explicit quote"):
        source.get_pool_ohlcv(
            pool_address=POOL,
            requested_symbol=TOKEN0,
            requested_quote="0x" + "8" * 40,
            **{**kwargs, "limit": 0},
        )
    assert len(requests) == before


def test_valid_nonpositive_limit_keeps_authenticated_exact_pool_provenance(exact_source) -> None:
    _source, requests = exact_source
    source = _new_source()
    before = len(requests)

    frame = source.get_pool_ohlcv(
        pool_address=POOL,
        chain="bsc",
        timestamp=TICK,
        timeframe="15m",
        limit=0,
        requested_symbol=TOKEN0,
        requested_quote=TOKEN1,
    )

    assert frame.empty
    assert frame.attrs["pool_address"] == POOL
    assert frame.attrs["base_asset"] == TOKEN0
    assert frame.attrs["quote_asset"] == TOKEN1
    assert frame.attrs["source"] == ""
    assert frame.attrs["confidence"] == "identity_only"
    assert frame.attrs["binding_hash"] == _verified().binding.binding_hash
    assert len(requests) == before


def test_backtest_view_records_exact_pool_serve_and_never_uses_pair_proxy(exact_source) -> None:
    source, _requests = exact_source
    manifest = RunDataManifest()
    view = BacktestOHLCVView(
        BacktestIndicatorEngine(required_indicators=set()),
        900,
        manifest=manifest,
        chain="bsc",
        pool_ohlcv_source=source,
    )
    view.bind(TICK)

    frame = view.get_pool_ohlcv(POOL, "bsc", "15m", 2, requested_symbol=TOKEN0)

    assert frame.attrs["source"] == "coingecko_onchain.exact_pool"
    assert manifest.entries() == [
        {
            "lane": LANE_OHLCV,
            "consumer": "",
            "key": f"pool:bsc:{POOL}:{TOKEN0}/{TOKEN1}@15m",
            "source": "coingecko_onchain.exact_pool",
            "outcome": "served",
            "ladder": ["coingecko_onchain.exact_pool"],
            "count": 1,
            "first": TICK.isoformat(),
            "last": TICK.isoformat(),
            "detail": "",
        }
    ]


def test_backtest_view_does_not_record_nonpositive_limit_as_a_data_serve(exact_source) -> None:
    source, requests = exact_source
    before = len(requests)
    manifest = RunDataManifest()
    view = BacktestOHLCVView(
        BacktestIndicatorEngine(required_indicators=set()),
        900,
        manifest=manifest,
        chain="bsc",
        pool_ohlcv_source=source,
    )
    view.bind(TICK)

    frame = view.get_pool_ohlcv(
        POOL,
        "bsc",
        "15m",
        0,
        requested_symbol=TOKEN0,
        requested_quote=TOKEN1,
    )

    assert frame.empty
    assert frame.attrs["binding_hash"] == _verified().binding.binding_hash
    assert manifest.entries() == []
    assert len(requests) == before


def test_market_snapshot_threads_explicit_pool_quote_and_keeps_failures_lane_scoped(exact_source) -> None:
    source, requests = exact_source
    view = BacktestOHLCVView(
        BacktestIndicatorEngine(required_indicators=set()),
        900,
        chain="bsc",
        pool_ohlcv_source=source,
    )
    view.bind(TICK)
    snapshot = create_market_snapshot_from_state(
        market_state=MarketState(timestamp=TICK, prices={}, chain="bsc"),
        chain="bsc",
        ohlcv_module=view,
    )
    wrong_quote = "0x" + "8" * 40

    default_quote = snapshot.ohlcv(TOKEN0, timeframe="15m", limit=2, pool_address=POOL)
    assert default_quote.attrs["quote_asset"] == TOKEN1

    with pytest.raises(ValueError, match="explicit quote"):
        snapshot.ohlcv(TOKEN0, timeframe="15m", limit=2, quote=wrong_quote, pool_address=POOL)

    failed_key = f"pool:bsc:{POOL}:{TOKEN0}/{wrong_quote}@15m:pool_scoped"
    assert ("ohlcv", failed_key) in snapshot._critical_data_failures
    frame = snapshot.ohlcv(TOKEN0, timeframe="15m", limit=2, quote=TOKEN1, pool_address=POOL)
    assert frame.attrs["quote_asset"] == TOKEN1
    assert ("ohlcv", failed_key) in snapshot._critical_data_failures
    assert requests


def test_nonpositive_limit_does_not_clear_a_prior_exact_pool_failure(exact_source) -> None:
    source, requests = exact_source
    before = len(requests)
    view = BacktestOHLCVView(
        BacktestIndicatorEngine(required_indicators=set()),
        900,
        chain="bsc",
        pool_ohlcv_source=source,
    )
    view.bind(TICK)
    snapshot = create_market_snapshot_from_state(
        market_state=MarketState(timestamp=TICK, prices={}, chain="bsc"),
        chain="bsc",
        ohlcv_module=view,
    )

    with pytest.raises(ValueError, match="exceeds prewarmed coverage"):
        snapshot.ohlcv(TOKEN0, timeframe="15m", limit=3, quote=TOKEN1, pool_address=POOL)

    failure_key = f"pool:bsc:{POOL}:{TOKEN0}/{TOKEN1}@15m:pool_scoped"
    assert ("ohlcv", failure_key) in snapshot._critical_data_failures
    frame = snapshot.ohlcv(TOKEN0, timeframe="15m", limit=0, quote=TOKEN1, pool_address=POOL)
    assert frame.empty
    assert ("ohlcv", failure_key) in snapshot._critical_data_failures
    assert len(requests) == before


def test_backtest_view_refuses_registry_known_pool_without_exact_source() -> None:
    view = BacktestOHLCVView(BacktestIndicatorEngine(required_indicators=set()), 3600, chain="base")
    view.bind(TICK)

    with pytest.raises(ValueError, match="not declared and prewarmed"):
        view.get_pool_ohlcv("0xd0b53d9277642d899df5c87a3966a349a798f224", "base", "1h", 2)


def test_exact_provider_failure_is_refused_without_fallback(
    monkeypatch: pytest.MonkeyPatch,
    exact_source,
) -> None:
    _source, _requests = exact_source
    source = _new_source()

    def refuse(request: Any, _gateway_client: Any) -> VenueDataFailure:
        return VenueDataFailure(
            request=request,
            state=VenueDataFailureState.UNAVAILABLE,
            reason_code=VenueDataFailureReason.TRANSPORT_UNAVAILABLE,
            detail="exact upstream unavailable",
        )

    monkeypatch.setattr(exact_ohlcv_module, "observe_exact_venue_data", refuse)
    with pytest.raises(ValueError, match="transport_unavailable: exact upstream unavailable"):
        asyncio.run(
            source.materialize_history(
                HistoricalPoolOHLCVTarget(
                    "bsc",
                    "pancakeswap_v3",
                    POOL,
                    TOKEN0,
                    TOKEN1,
                    OHLCVTimeframe.FIFTEEN_MINUTES,
                    2,
                )
            )
        )
