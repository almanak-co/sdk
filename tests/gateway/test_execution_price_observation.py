"""Execution freshness uses actual aggregate contributors and chain provisioning."""

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from almanak.framework.data.interfaces import AllDataSourcesFailed, PriceResult
from almanak.gateway.core.settings import GatewaySettings
from almanak.gateway.data.price.aggregator import PriceAggregator
from almanak.gateway.services.execution_service import ExecutionServiceServicer
from almanak.gateway.services.market_service import MarketServiceServicer


def _source(name, price, timestamp):
    return SimpleNamespace(
        source_name=name,
        get_price=AsyncMock(
            return_value=PriceResult(
                price=Decimal(str(price)),
                source=name,
                timestamp=timestamp,
                confidence=1,
                stale=False,
            )
        ),
    )


def _service(aggregator):
    service = ExecutionServiceServicer(GatewaySettings())
    market = MarketServiceServicer(GatewaySettings())
    market._ensure_initialized = AsyncMock()
    market._auto_reinitialize_unconfigured_chains = AsyncMock()
    market._chain_configuration_error = MagicMock(return_value=None)
    market._price_aggregators = {"bsc": aggregator}
    service.market_servicer = market
    return service


@pytest.mark.asyncio
async def test_real_aggregation_does_not_make_old_chainlink_observation_fresh():
    old = datetime.now(UTC) - timedelta(minutes=20)
    aggregator = PriceAggregator([_source("chainlink", 600, old)])
    aggregate = await aggregator.get_aggregated_price("BNB")
    assert aggregate.timestamp > old
    assert aggregate.stale is False
    assert aggregate.source_details["contributing_observations"][0]["timestamp"] == old.isoformat()
    with pytest.raises(AllDataSourcesFailed):
        await _service(aggregator)._gas_policy_native_price("bsc")


@pytest.mark.asyncio
@pytest.mark.parametrize("old_price", [600, 1000])
async def test_execution_aggregates_only_fresh_contributors(old_price):
    now = datetime.now(UTC)
    old = now - timedelta(minutes=20)
    first = now - timedelta(seconds=20)
    aggregator = PriceAggregator(
        [
            _source("old", old_price, old),
            _source("fresh_first", 600, first),
            _source("fresh_second", 602, now),
        ]
    )
    price, timestamp = await _service(aggregator)._gas_policy_native_price("bsc")
    assert price == 601
    assert timestamp == first


@pytest.mark.asyncio
async def test_returned_observations_cannot_be_replaced_by_later_aggregate_diagnostics():
    first_time = datetime.now(UTC) - timedelta(seconds=10)
    source = _source("chainlink", 600, first_time)
    aggregator = PriceAggregator([source])
    first = await aggregator.get_aggregated_price("BNB")
    source.get_price.return_value = PriceResult(
        price=Decimal(601),
        source="chainlink",
        timestamp=datetime.now(UTC),
        confidence=1,
    )
    await aggregator.get_aggregated_price("BNB")
    assert ExecutionServiceServicer._gas_policy_observation_timestamp(first) == first_time


@pytest.mark.asyncio
@pytest.mark.parametrize("timestamp", [None, datetime(2026, 1, 1)])
async def test_missing_or_naive_source_timestamp_is_not_replaced_with_aggregation_time(timestamp):
    aggregator = PriceAggregator([_source("chainlink", 600, timestamp)])
    with pytest.raises(AllDataSourcesFailed):
        await _service(aggregator)._gas_policy_native_price("bsc")


@pytest.mark.asyncio
async def test_legacy_aggregate_without_contributor_provenance_is_refused():
    aggregate = PriceResult(price=Decimal(600), source="aggregated", timestamp=datetime.now(UTC), confidence=1)
    service = _service(SimpleNamespace(get_aggregated_price=AsyncMock(return_value=aggregate)))
    with pytest.raises(ValueError, match="measured contributing"):
        await service._gas_policy_native_price("bsc")


@pytest.mark.asyncio
@pytest.mark.parametrize("configured", [False, True])
async def test_native_price_obeys_real_on_demand_and_configured_chain_gate(configured):
    settings = GatewaySettings(chains=["ethereum"] if configured else [])
    market = MarketServiceServicer(settings)
    market._initialized = True
    market._price_aggregators = {}
    aggregator = PriceAggregator([_source("chainlink", 600, datetime.now(UTC))])

    def initialize_sources():
        market._price_aggregators = dict.fromkeys(market._on_demand_chains, aggregator)

    market._do_initialize = MagicMock(side_effect=initialize_sources)
    service = ExecutionServiceServicer(settings)
    service.market_servicer = market
    if configured:
        with pytest.raises(ValueError, match="not configured"):
            await service._gas_policy_native_price("bsc")
        market._do_initialize.assert_not_called()
        assert settings.chains == ["ethereum"]
    else:
        price, _ = await service._gas_policy_native_price("bsc")
        assert price == 600
        assert market._on_demand_chains == ["bsc"]
        assert settings.chains == []
        market._do_initialize.assert_called_once_with()


@pytest.mark.asyncio
async def test_stale_majority_cannot_exclude_the_only_fresh_execution_observation():
    now = datetime.now(UTC)
    old = now - timedelta(minutes=20)
    aggregator = PriceAggregator(
        [
            _source("old_one", 500, old),
            _source("old_two", 501, old),
            _source("fresh", 600, now),
        ]
    )
    market_price = await aggregator.get_aggregated_price("BNB")
    assert market_price.price == Decimal("500.5")
    price, timestamp = await _service(aggregator)._gas_policy_native_price("bsc")
    assert price == 600
    assert timestamp == now


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["stale", "future", "synthetic"])
async def test_execution_filter_excludes_ineligible_sources_before_consensus(kind):
    now = datetime.now(UTC)
    source = _source("ineligible", 500, now)
    result = source.get_price.return_value
    source.get_price.return_value = PriceResult(
        price=result.price,
        source=result.source,
        timestamp=now + timedelta(seconds=10) if kind == "future" else now,
        confidence=1,
        stale=kind == "stale",
        peg_tokens=("bsc:synthetic",) if kind == "synthetic" else (),
    )
    aggregator = PriceAggregator([source, _source("fresh", 600, now)])
    price, timestamp = await _service(aggregator)._gas_policy_native_price("bsc")
    assert price == 600
    assert timestamp == now


@pytest.mark.asyncio
@pytest.mark.parametrize("eligible", [False, True])
async def test_execution_filter_does_not_publish_failures_into_market_diagnostics(eligible):
    now = datetime.now(UTC)
    sources = [_source("publication", 600, now - timedelta(minutes=20))]
    if eligible:
        sources.append(_source("fresh", 601, now))
    aggregator = PriceAggregator(sources)
    await aggregator.get_aggregated_price("BNB")
    ordinary_details = aggregator.get_last_details("BNB", "USD")
    assert ordinary_details["sources_failed"] == {}
    if eligible:
        await aggregator.get_aggregated_price("BNB", max_observation_age_seconds=60)
    else:
        with pytest.raises(AllDataSourcesFailed):
            await aggregator.get_aggregated_price("BNB", max_observation_age_seconds=60)
    assert aggregator.get_last_details("BNB", "USD") == ordinary_details
