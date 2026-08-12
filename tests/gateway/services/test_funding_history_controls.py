"""Gateway funding-history cache, throttle, and 429 retry controls."""

from __future__ import annotations

import asyncio
from decimal import Decimal
from typing import Any

import grpc
import pytest

from almanak.connectors._base.gateway_capabilities import FundingHistorySource
from almanak.gateway.core.settings import GatewaySettings
from almanak.gateway.proto import gateway_pb2
from almanak.gateway.services.rate_history_service import (
    FundingRatePoint,
    RateHistoryRateLimited,
    RateHistoryServiceServicer,
)


class _Provider:
    def __init__(
        self,
        venue: str,
        *,
        failures: int = 0,
        wait: asyncio.Event | None = None,
        source_key: str = "hyperliquid_info",
    ) -> None:
        self._venue = venue
        self._failures = failures
        self._wait = wait
        self.calls = 0
        self._source_key = source_key

    def funding_venue(self) -> str:
        return self._venue

    def funding_supported_markets(self) -> frozenset[str]:
        return frozenset({"ETH-USD"})

    def funding_history_source(self, chain: str) -> FundingHistorySource:
        return FundingHistorySource(
            key=self._source_key,
            scope="",
            requests_per_minute=30,
            burst_size=6,
        )

    async def fetch_funding_history(self, servicer: Any, **kwargs: Any) -> list[FundingRatePoint]:
        self.calls += 1
        if self._wait is not None:
            await self._wait.wait()
        if self.calls <= self._failures:
            raise RateHistoryRateLimited("hyperliquid", "HTTP 429", retry_after=2.0)
        return [FundingRatePoint(timestamp=kwargs["end_ts"], rate_hourly=Decimal("0.0001"))]


class _Context:
    def __init__(self) -> None:
        self.code: grpc.StatusCode | None = None
        self.details = ""
        self.trailing_metadata: tuple[tuple[str, bytes], ...] = ()

    def set_code(self, code: grpc.StatusCode) -> None:
        self.code = code

    def set_details(self, details: str) -> None:
        self.details = details

    def set_trailing_metadata(self, metadata: tuple[tuple[str, bytes], ...]) -> None:
        self.trailing_metadata = metadata


@pytest.fixture
def servicer() -> RateHistoryServiceServicer:
    return RateHistoryServiceServicer(GatewaySettings())


def test_gmx_and_hyperliquid_have_independent_native_budgets(servicer: RateHistoryServiceServicer) -> None:
    gmx = servicer._funding_providers["gmx_v2"]
    hyperliquid = servicer._funding_providers["hyperliquid"]

    assert gmx.funding_history_source("arbitrum") != hyperliquid.funding_history_source("")


@pytest.mark.asyncio
async def test_distinct_native_sources_do_not_share_cache(servicer: RateHistoryServiceServicer) -> None:
    gmx = _Provider("gmx_v2", source_key="gmx_synthetics_subsquid")
    hyperliquid = _Provider("hyperliquid")
    kwargs = {
        "market": "ETH-USD",
        "market_address": "",
        "chain": "arbitrum",
        "start_ts": 1_700_000_000,
        "end_ts": 1_700_003_600,
    }

    gmx_result = await servicer._cached_funding_history(gmx, **kwargs)
    hyperliquid_result = await servicer._cached_funding_history(hyperliquid, **kwargs)

    assert gmx.calls == 1
    assert hyperliquid.calls == 1
    assert gmx_result.points == hyperliquid_result.points


@pytest.mark.asyncio
async def test_exact_address_cache_still_partitions_by_declared_market(servicer: RateHistoryServiceServicer) -> None:
    provider = _Provider("gmx_v2", source_key="gmx_synthetics_subsquid")
    shared = {
        "market_address": "0x7c54d547fad72f8afbf6e5b04403a0168b654c6f",
        "chain": "arbitrum",
        "start_ts": 1_700_000_000,
        "end_ts": 1_700_003_600,
    }

    await servicer._cached_funding_history(provider, market="XMR-USD", **shared)
    await servicer._cached_funding_history(provider, market="BTC-USD", **shared)

    assert provider.calls == 2


@pytest.mark.asyncio
async def test_concurrent_cold_reads_share_one_upstream_fetch(servicer: RateHistoryServiceServicer) -> None:
    release = asyncio.Event()
    provider = _Provider("hyperliquid", wait=release)
    kwargs = {
        "market": "ETH-USD",
        "market_address": "",
        "chain": "",
        "start_ts": 1_700_000_000,
        "end_ts": 1_700_003_600,
    }

    tasks = [asyncio.create_task(servicer._cached_funding_history(provider, **kwargs)) for _ in range(20)]
    await asyncio.sleep(0)
    release.set()
    results = await asyncio.gather(*tasks)

    assert provider.calls == 1
    assert all(result.points == results[0].points for result in results)
    assert servicer._funding_history_cache.stats()["inflight_dedup_hits"] == 19


@pytest.mark.asyncio
async def test_429_retry_honors_retry_after(
    servicer: RateHistoryServiceServicer,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    provider = _Provider("hyperliquid", failures=2)
    sleeps: list[tuple[int, float | None]] = []

    async def _record_sleep(attempt: int, retry_after: float | None) -> None:
        sleeps.append((attempt, retry_after))

    monkeypatch.setattr(servicer, "_funding_retry_sleep", _record_sleep)
    result = await servicer._cached_funding_history(
        provider,
        market="ETH-USD",
        market_address="",
        chain="",
        start_ts=1_700_000_000,
        end_ts=1_700_003_600,
    )

    assert len(result.points) == 1
    assert provider.calls == 3
    assert sleeps == [(1, 2.0), (2, 2.0)]


@pytest.mark.asyncio
async def test_exhausted_429_is_typed_resource_exhausted(
    servicer: RateHistoryServiceServicer,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    provider = _Provider("hyperliquid", failures=3)
    servicer._funding_providers["hyperliquid"] = provider

    async def _no_sleep(attempt: int, retry_after: float | None) -> None:
        return None

    monkeypatch.setattr(servicer, "_funding_retry_sleep", _no_sleep)
    context = _Context()
    request = gateway_pb2.GetFundingRateHistoryRequest(
        venue="hyperliquid",
        market="ETH-USD",
        start_ts=1_700_000_000,
        end_ts=1_700_003_600,
    )

    response = await servicer.GetFundingRateHistory(request, context)  # type: ignore[arg-type]

    assert response.success is False
    assert provider.calls == 3
    assert context.code == grpc.StatusCode.RESOURCE_EXHAUSTED
    assert context.trailing_metadata
