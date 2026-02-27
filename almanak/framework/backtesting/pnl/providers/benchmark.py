"""Benchmark data provider for strategy performance comparison.

This module provides functions and classes for fetching benchmark returns
to compare strategy performance against passive investment alternatives.

Supported Benchmarks:
    - ETH_HOLD: Buy and hold ETH/Ethereum
    - BTC_HOLD: Buy and hold BTC/Bitcoin
    - DEFI_INDEX: DeFi index (weighted basket of DeFi tokens)

Example:
    from almanak.framework.backtesting.pnl.providers.benchmark import (
        Benchmark,
        get_benchmark_returns,
        get_benchmark_price_series,
    )
    from datetime import datetime

    # Get daily returns for ETH
    returns = await get_benchmark_returns(
        benchmark=Benchmark.ETH_HOLD,
        start=datetime(2024, 1, 1),
        end=datetime(2024, 6, 1),
    )

    # Get full price series
    prices = await get_benchmark_price_series(
        benchmark=Benchmark.BTC_HOLD,
        start=datetime(2024, 1, 1),
        end=datetime(2024, 6, 1),
        interval_seconds=3600,  # hourly
    )
"""

import logging
import os
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from enum import StrEnum
from typing import Any

import aiohttp

logger = logging.getLogger(__name__)

# CoinGecko API base URLs (same as CoinGeckoDataProvider)
_FREE_API_BASE = "https://api.coingecko.com/api/v3"
_PRO_API_BASE = "https://pro-api.coingecko.com/api/v3"


class Benchmark(StrEnum):
    """Supported benchmark types for performance comparison.

    Attributes:
        ETH_HOLD: Buy and hold Ethereum - most common DeFi benchmark
        BTC_HOLD: Buy and hold Bitcoin - traditional crypto benchmark
        DEFI_INDEX: Weighted basket of major DeFi tokens (UNI, AAVE, LINK, etc.)
    """

    ETH_HOLD = "eth_hold"
    BTC_HOLD = "btc_hold"
    DEFI_INDEX = "defi_index"

    @classmethod
    def from_string(cls, value: str) -> "Benchmark":
        """Parse benchmark from string, case-insensitive.

        Args:
            value: String representation of benchmark (e.g., "eth_hold", "ETH", "btc")

        Returns:
            Matching Benchmark enum value

        Raises:
            ValueError: If benchmark string is not recognized
        """
        value_lower = value.lower().strip()

        # Direct enum value match
        for member in cls:
            if member.value == value_lower:
                return member

        # Common aliases
        aliases = {
            "eth": cls.ETH_HOLD,
            "ethereum": cls.ETH_HOLD,
            "weth": cls.ETH_HOLD,
            "btc": cls.BTC_HOLD,
            "bitcoin": cls.BTC_HOLD,
            "wbtc": cls.BTC_HOLD,
            "defi": cls.DEFI_INDEX,
            "index": cls.DEFI_INDEX,
        }

        if value_lower in aliases:
            return aliases[value_lower]

        valid_values = [m.value for m in cls] + list(aliases.keys())
        raise ValueError(f"Unknown benchmark '{value}'. Valid options: {', '.join(sorted(set(valid_values)))}")


# CoinGecko token IDs for benchmark tokens
BENCHMARK_TOKEN_IDS: dict[str, str] = {
    "ETH": "ethereum",
    "BTC": "bitcoin",
    # DeFi index components
    "UNI": "uniswap",
    "AAVE": "aave",
    "LINK": "chainlink",
    "MKR": "maker",
    "COMP": "compound-governance-token",
    "CRV": "curve-dao-token",
    "LDO": "lido-dao",
    "SNX": "havven",
}

# DeFi index weights (approximate market-cap weights, sum to 1.0)
DEFI_INDEX_WEIGHTS: dict[str, Decimal] = {
    "UNI": Decimal("0.25"),  # Uniswap
    "AAVE": Decimal("0.20"),  # Aave
    "LINK": Decimal("0.20"),  # Chainlink
    "MKR": Decimal("0.10"),  # Maker
    "LDO": Decimal("0.10"),  # Lido
    "COMP": Decimal("0.05"),  # Compound
    "CRV": Decimal("0.05"),  # Curve
    "SNX": Decimal("0.05"),  # Synthetix
}


@dataclass
class BenchmarkPricePoint:
    """A single price point in a benchmark price series.

    Attributes:
        timestamp: Time of this price point
        price: Price in USD
    """

    timestamp: datetime
    price: Decimal


@dataclass
class BenchmarkReturn:
    """A single return data point.

    Attributes:
        timestamp: End of the return period
        return_value: Return as decimal (0.01 = 1%)
    """

    timestamp: datetime
    return_value: Decimal


async def get_benchmark_price_series(
    benchmark: Benchmark,
    start: datetime,
    end: datetime,
    interval_seconds: int = 86400,  # Default: daily
) -> list[BenchmarkPricePoint]:
    """Fetch historical price series for a benchmark.

    Args:
        benchmark: The benchmark to fetch (ETH_HOLD, BTC_HOLD, DEFI_INDEX)
        start: Start datetime (UTC)
        end: End datetime (UTC)
        interval_seconds: Interval between data points (default: 86400 = daily)

    Returns:
        List of BenchmarkPricePoint with timestamp and price

    Raises:
        ValueError: If date range is invalid
        aiohttp.ClientError: If API request fails
    """
    if start >= end:
        raise ValueError(f"Start date {start} must be before end date {end}")

    if benchmark == Benchmark.DEFI_INDEX:
        return await _get_defi_index_prices(start, end, interval_seconds)

    # Single token benchmarks (ETH, BTC)
    token = "ETH" if benchmark == Benchmark.ETH_HOLD else "BTC"
    return await _get_single_token_prices(token, start, end, interval_seconds)


async def get_benchmark_returns(
    benchmark: Benchmark,
    start: datetime,
    end: datetime,
    interval_seconds: int = 86400,  # Default: daily
) -> list[Decimal]:
    """Calculate periodic returns for a benchmark.

    Returns are calculated as: (price_t / price_{t-1}) - 1

    Args:
        benchmark: The benchmark to fetch
        start: Start datetime (UTC)
        end: End datetime (UTC)
        interval_seconds: Interval between return periods (default: daily)

    Returns:
        List of returns as Decimals (0.01 = 1% return)
        Length will be (number of price points - 1)

    Example:
        returns = await get_benchmark_returns(
            benchmark=Benchmark.ETH_HOLD,
            start=datetime(2024, 1, 1),
            end=datetime(2024, 1, 31),
        )
        # returns will have ~29 daily returns
    """
    prices = await get_benchmark_price_series(benchmark, start, end, interval_seconds)

    if len(prices) < 2:
        logger.warning(
            f"Insufficient price data for benchmark {benchmark.value}: got {len(prices)} points, need at least 2"
        )
        return []

    returns: list[Decimal] = []
    for i in range(1, len(prices)):
        prev_price = prices[i - 1].price
        curr_price = prices[i].price

        if prev_price == 0:
            logger.warning(f"Zero price at {prices[i - 1].timestamp} for {benchmark.value}, skipping")
            continue

        period_return = (curr_price - prev_price) / prev_price
        returns.append(period_return)

    return returns


async def get_benchmark_total_return(
    benchmark: Benchmark,
    start: datetime,
    end: datetime,
) -> Decimal:
    """Calculate total return for a benchmark over a period.

    Args:
        benchmark: The benchmark to calculate return for
        start: Start datetime (UTC)
        end: End datetime (UTC)

    Returns:
        Total return as decimal (0.15 = 15% total return)
        Returns Decimal("0") if insufficient data
    """
    prices = await get_benchmark_price_series(benchmark, start, end)

    if len(prices) < 2:
        logger.warning(f"Insufficient price data for total return: {len(prices)} points")
        return Decimal("0")

    start_price = prices[0].price
    end_price = prices[-1].price

    if start_price == 0:
        logger.warning(f"Zero start price for {benchmark.value}")
        return Decimal("0")

    return (end_price - start_price) / start_price


async def _get_single_token_prices(
    token: str,
    start: datetime,
    end: datetime,
    interval_seconds: int,
) -> list[BenchmarkPricePoint]:
    """Fetch price series for a single token from CoinGecko.

    Args:
        token: Token symbol (ETH, BTC, etc.)
        start: Start datetime
        end: End datetime
        interval_seconds: Interval between data points

    Returns:
        List of BenchmarkPricePoint
    """
    token_id = BENCHMARK_TOKEN_IDS.get(token.upper())
    if not token_id:
        raise ValueError(f"Unknown token for benchmark: {token}")

    # Convert to Unix timestamps
    start_ts = int(start.timestamp())
    end_ts = int(end.timestamp())

    # Use Pro API if API key is available (matches CoinGeckoDataProvider pattern)
    api_key = os.environ.get("COINGECKO_API_KEY", "")
    api_base = _PRO_API_BASE if api_key else _FREE_API_BASE

    url = f"{api_base}/coins/{token_id}/market_chart/range"
    params: dict[str, str | int] = {
        "vs_currency": "usd",
        "from": start_ts,
        "to": end_ts,
    }

    headers: dict[str, str] = {}
    if api_key:
        headers["x-cg-pro-api-key"] = api_key

    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, params=params, headers=headers) as response:
                if response.status == 429:
                    logger.warning("CoinGecko rate limit hit for benchmark %s, returning empty prices", token)
                    return []

                if response.status != 200:
                    logger.error("CoinGecko API error %d for benchmark %s", response.status, token)
                    return []

                data = await response.json()

    except aiohttp.ClientError as e:
        logger.error("Network error fetching benchmark %s prices: %s", token, e)
        return []

    return _parse_coingecko_prices(data, interval_seconds)


async def _get_defi_index_prices(
    start: datetime,
    end: datetime,
    interval_seconds: int,
) -> list[BenchmarkPricePoint]:
    """Calculate DeFi index prices as weighted basket.

    Fetches prices for all index components and calculates weighted average.

    Args:
        start: Start datetime
        end: End datetime
        interval_seconds: Interval between data points

    Returns:
        List of BenchmarkPricePoint for the index
    """
    # Fetch all component prices
    component_prices: dict[str, list[BenchmarkPricePoint]] = {}

    for token in DEFI_INDEX_WEIGHTS:
        prices = await _get_single_token_prices(token, start, end, interval_seconds)
        if prices:
            component_prices[token] = prices

    if not component_prices:
        logger.warning("No component prices available for DeFi index")
        return []

    # Normalize to a common index starting at 100
    # Find the reference token with most data points
    ref_token = max(component_prices.keys(), key=lambda t: len(component_prices[t]))
    ref_prices = component_prices[ref_token]

    # Calculate weighted index at each timestamp
    index_prices: list[BenchmarkPricePoint] = []

    for i, ref_point in enumerate(ref_prices):
        weighted_return = Decimal("0")
        total_weight = Decimal("0")

        for token, weight in DEFI_INDEX_WEIGHTS.items():
            if token not in component_prices:
                continue

            token_prices = component_prices[token]
            if i >= len(token_prices):
                continue

            # Calculate normalized price (relative to first price)
            first_price = token_prices[0].price
            if first_price == 0:
                continue

            current_price = token_prices[i].price
            normalized_return = current_price / first_price

            weighted_return += weight * normalized_return
            total_weight += weight

        if total_weight == 0:
            continue

        # Normalize by actual weights used
        index_value = (weighted_return / total_weight) * Decimal("100")

        index_prices.append(
            BenchmarkPricePoint(
                timestamp=ref_point.timestamp,
                price=index_value,
            )
        )

    return index_prices


def _parse_coingecko_prices(
    data: dict[str, Any],
    interval_seconds: int,
) -> list[BenchmarkPricePoint]:
    """Parse CoinGecko API response into price points.

    CoinGecko returns data as:
    {
        "prices": [[timestamp_ms, price], ...],
        "market_caps": [...],
        "total_volumes": [...]
    }

    Args:
        data: CoinGecko API response
        interval_seconds: Desired interval for filtering

    Returns:
        List of BenchmarkPricePoint
    """
    prices_raw = data.get("prices", [])
    if not prices_raw:
        return []

    prices: list[BenchmarkPricePoint] = []
    last_timestamp: float | None = None

    for item in prices_raw:
        if len(item) < 2:
            continue

        timestamp_ms, price = item[0], item[1]
        timestamp = datetime.fromtimestamp(timestamp_ms / 1000, tz=UTC)

        # Filter to desired interval
        if last_timestamp is not None:
            elapsed = timestamp_ms / 1000 - last_timestamp
            if elapsed < interval_seconds * 0.9:  # Allow 10% tolerance
                continue

        prices.append(
            BenchmarkPricePoint(
                timestamp=timestamp,
                price=Decimal(str(price)),
            )
        )
        last_timestamp = timestamp_ms / 1000

    return prices


# Default benchmark for CLI
DEFAULT_BENCHMARK = Benchmark.ETH_HOLD


__all__ = [
    "Benchmark",
    "BenchmarkPricePoint",
    "BenchmarkReturn",
    "DEFAULT_BENCHMARK",
    "DEFI_INDEX_WEIGHTS",
    "get_benchmark_price_series",
    "get_benchmark_returns",
    "get_benchmark_total_return",
]
