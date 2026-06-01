"""Active unit tests for the W7 gateway-backed TWAP fetch path.

These complement ``test_twap_provider.py`` (skipped pending the VIB-4869
caller-migration rewrite). They lock in the VIB-4859 re-review fix that the
``DexTwapPoint.tick_observation_count`` ring-buffer counter is surfaced under
its honest name and is NOT mislabeled as a computed tick TWAP.
"""

from __future__ import annotations

from decimal import Decimal
from types import SimpleNamespace
from typing import Any

import pytest

from almanak.framework.backtesting.pnl.providers import twap as twap_mod
from almanak.framework.backtesting.pnl.providers.twap import (
    TWAPDataProvider,
    TWAPResult,
)


class _FakePoint:
    def __init__(self, price: str, tick_observation_count: int) -> None:
        self.price = price
        self.tick_observation_count = tick_observation_count


class _FakeResponse:
    def __init__(self, point: _FakePoint) -> None:
        self.success = True
        self.source = "gateway"
        self.error = ""
        self.point = point


class _FakeRateHistory:
    def __init__(self, response: _FakeResponse) -> None:
        self._response = response

    def GetDexTwap(self, request: Any) -> _FakeResponse:  # noqa: N802 (gRPC stub name)
        return self._response


class _FakeClient:
    def __init__(self, response: _FakeResponse) -> None:
        self.rate_history = _FakeRateHistory(response)


class _FakePb2:
    @staticmethod
    def GetDexTwapRequest(**kwargs: Any) -> SimpleNamespace:  # noqa: N802 (proto ctor name)
        return SimpleNamespace(**kwargs)


@pytest.fixture
def _patch_gateway(monkeypatch: pytest.MonkeyPatch):
    """Return a factory that patches the gateway client with a canned point."""

    def _install(price: str, tick_observation_count: int) -> None:
        response = _FakeResponse(_FakePoint(price, tick_observation_count))
        monkeypatch.setattr(
            twap_mod,
            "_twap_get_connected_gateway_client",
            lambda: (_FakeClient(response), _FakePb2()),
        )

    return _install


@pytest.mark.asyncio
async def test_fetch_twap_via_gateway_surfaces_observation_count(_patch_gateway):
    """The returned dict uses the honest ``tick_observation_count`` key.

    Regression guard: the gateway TWAP point carries no computed tick, only
    the ring-buffer observation counter. Mapping that counter to a ``"tick"``
    key (the old behaviour) silently mislabeled a sanity-check value as the
    arithmetic-mean tick.
    """
    _patch_gateway(price="3000.50", tick_observation_count=42)
    provider = TWAPDataProvider(chain="arbitrum")

    point = await provider._fetch_twap_via_gateway("0xpool")

    assert point == {
        "price": Decimal("3000.50"),
        "tick_observation_count": 42,
    }
    assert "tick" not in point


@pytest.mark.asyncio
async def test_cached_twap_result_carries_observation_count(_patch_gateway):
    """``get_latest_price`` stamps the counter into ``TWAPResult``."""
    _patch_gateway(price="2500.00", tick_observation_count=7)
    provider = TWAPDataProvider(chain="arbitrum")

    # ARB is paired with WETH in the pool table, but we test the direct
    # cache stamping via the dedicated helper to avoid two-hop coupling.
    provider._cache_twap_price(
        "WETH",
        price=Decimal("2500.00"),
        tick_observation_count=7,
        pool_address="0xpool",
    )

    cached = provider._cache["WETH"]
    assert isinstance(cached.result, TWAPResult)
    assert cached.result.tick_observation_count == 7
    assert not hasattr(cached.result, "tick_twap")
