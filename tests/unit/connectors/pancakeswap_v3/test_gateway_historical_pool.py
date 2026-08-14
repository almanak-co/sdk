"""PancakeSwap V3 historical exact-pool gateway capability tests."""

from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pytest

from almanak.connectors._base import v3_gateway_twap
from almanak.connectors._base.gateway_capabilities import GatewayDexPoolStateCapability
from almanak.connectors.pancakeswap_v3.addresses import PANCAKESWAP_V3
from almanak.connectors.pancakeswap_v3.gateway.provider import PancakeSwapV3GatewayConnector
from almanak.gateway.services.rate_history_service import RateHistoryUnavailable

POOL = "0x1111111111111111111111111111111111111111"


def test_connector_publishes_bsc_pool_state_capability() -> None:
    connector = PancakeSwapV3GatewayConnector()

    assert isinstance(connector, GatewayDexPoolStateCapability)
    assert "bsc" in connector.pool_state_supported_chains()
    assert "bsc" in connector.twap_supported_chains()


def test_historical_twap_series_uses_shared_v3_reader(monkeypatch: pytest.MonkeyPatch) -> None:
    expected = [SimpleNamespace(price="1.23")]
    received: dict[str, object] = {}

    async def fake_fetch(servicer, **kwargs):
        received["servicer"] = servicer
        received.update(kwargs)
        return expected

    monkeypatch.setattr(v3_gateway_twap, "fetch_v3_twap_series", fake_fetch)
    servicer = SimpleNamespace()
    result = asyncio.run(
        PancakeSwapV3GatewayConnector().fetch_twap_series(
            servicer,
            chain="bsc",
            pool_address=POOL,
            start_ts=1_000,
            end_ts=1_600,
            interval_secs=300,
            window_secs=300,
        )
    )

    assert result is expected
    assert received == {
        "servicer": servicer,
        "chain": "bsc",
        "pool_address": POOL,
        "start_ts": 1_000,
        "end_ts": 1_600,
        "interval_secs": 300,
        "window_secs": 300,
        "protocol": "pancakeswap_v3",
    }


def test_historical_pool_state_uses_connector_factory(monkeypatch: pytest.MonkeyPatch) -> None:
    expected = [SimpleNamespace(fee_tier=2_500)]
    received: dict[str, object] = {}

    async def fake_fetch(servicer, **kwargs):
        received["servicer"] = servicer
        received.update(kwargs)
        return expected

    monkeypatch.setattr(v3_gateway_twap, "fetch_v3_pool_state_series", fake_fetch)
    servicer = SimpleNamespace()
    result = asyncio.run(
        PancakeSwapV3GatewayConnector().fetch_pool_state_series(
            servicer,
            chain="BSC",
            pool_address=POOL,
            start_ts=1_000,
            end_ts=1_600,
            interval_secs=300,
        )
    )

    assert result is expected
    assert received == {
        "servicer": servicer,
        "chain": "BSC",
        "pool_address": POOL,
        "start_ts": 1_000,
        "end_ts": 1_600,
        "interval_secs": 300,
        "protocol": "pancakeswap_v3",
        "factory_address": PANCAKESWAP_V3["bsc"]["factory"],
    }


def test_historical_pool_state_rejects_chain_without_factory() -> None:
    with pytest.raises(RateHistoryUnavailable, match="no authenticated PancakeSwap V3 factory"):
        asyncio.run(
            PancakeSwapV3GatewayConnector().fetch_pool_state_series(
                SimpleNamespace(),
                chain="unsupported",
                pool_address=POOL,
                start_ts=1_000,
                end_ts=1_600,
                interval_secs=300,
            )
        )
