from __future__ import annotations

import asyncio
from datetime import datetime, timedelta
from decimal import Decimal

from almanak.framework.backtesting.pnl.data_provider import HistoricalDataConfig
from scripts.ci.run_benchmarks import FastMockDataProvider


def test_fast_mock_data_provider_keeps_stablecoin_prices_pegged() -> None:
    start = datetime(2024, 1, 1)
    later = start + timedelta(hours=123)
    provider = FastMockDataProvider(
        {"USDC": Decimal("1"), "USDT": Decimal("1"), "WETH": Decimal("2000")},
        start,
        volatility=Decimal("0.5"),
    )

    assert asyncio.run(provider.get_price("USDC", later)) == Decimal("1")
    assert asyncio.run(provider.get_price("USDT", later)) == Decimal("1")


def test_fast_mock_data_provider_iterate_keeps_stablecoin_prices_pegged() -> None:
    start = datetime(2024, 1, 1)
    provider = FastMockDataProvider(
        {"USDC": Decimal("1"), "USDT": Decimal("1"), "WETH": Decimal("2000")},
        start,
        volatility=Decimal("0.5"),
    )

    async def first_market_prices() -> dict[object, Decimal]:
        config = HistoricalDataConfig(
            start_time=start,
            end_time=start + timedelta(hours=1),
            tokens=["USDC", "USDT"],
            chains=["arbitrum"],
        )
        async for _timestamp, market_state in provider.iterate(config):
            return market_state.prices
        raise AssertionError("provider did not emit market data")

    prices = asyncio.run(first_market_prices())

    assert prices == {"USDC": Decimal("1"), "USDT": Decimal("1")}
