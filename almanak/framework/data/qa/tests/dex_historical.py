"""DEX Historical Price Test for QA Framework.

This module provides validation for DEX (Decentralized Exchange) historical
price data with WETH denomination. It fetches token/USD and ETH/USD OHLCV data
from CoinGecko and derives WETH-denominated prices.

Example:
    from almanak.framework.data.qa.tests.dex_historical import DEXHistoricalTest
    from almanak.framework.data.qa.config import load_config

    config = load_config()
    test = DEXHistoricalTest(config)
    results = await test.run()

    for result in results:
        print(f"{result.token}: {result.total_points} points - {'PASS' if result.passed else 'FAIL'}")
"""

import logging
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Any

from almanak.framework.data.indicators.rsi import CoinGeckoOHLCVProvider
from almanak.framework.data.interfaces import DataSourceUnavailable, OHLCVCandle
from almanak.framework.data.qa.config import QAConfig

logger = logging.getLogger(__name__)


@dataclass
class WETHPricePoint:
    """A single WETH-denominated price point.

    Attributes:
        timestamp: Time of the price point
        price_weth: Token price in WETH
    """

    timestamp: datetime
    price_weth: Decimal

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "timestamp": self.timestamp.isoformat(),
            "price_weth": str(self.price_weth),
        }


@dataclass
class DEXHistoricalResult:
    """Result of a DEX historical price test for a single token.

    Attributes:
        token: Token symbol (e.g., "ETH", "WBTC")
        weth_prices: List of WETH-denominated price points
        total_points: Number of price points generated
        passed: Whether all validation checks passed
        error: Error message if the test failed, None otherwise
        note: Additional information about the data source
    """

    token: str
    weth_prices: list[WETHPricePoint]
    total_points: int
    passed: bool
    error: str | None
    note: str = "Derived from CEX data with WETH conversion"

    def to_dict(self) -> dict[str, Any]:
        """Convert result to dictionary for serialization."""
        return {
            "token": self.token,
            "weth_prices": [p.to_dict() for p in self.weth_prices],
            "total_points": self.total_points,
            "passed": self.passed,
            "error": self.error,
            "note": self.note,
        }


class DEXHistoricalTest:
    """Test for validating DEX historical WETH-denominated prices.

    This test validates that:
    1. Historical OHLCV data can be fetched for all configured tokens
    2. ETH/USD data can be fetched for WETH conversion
    3. WETH prices are correctly derived (token_usd / eth_usd)
    4. Price data is valid (positive values)

    Attributes:
        config: QA configuration with token lists and thresholds
        ohlcv_provider: CoinGecko OHLCV provider instance

    Example:
        config = load_config()
        test = DEXHistoricalTest(config)
        results = await test.run()

        passed = all(r.passed for r in results)
        print(f"DEX Historical Test: {'PASSED' if passed else 'FAILED'}")
    """

    def __init__(
        self,
        config: QAConfig,
        ohlcv_provider: CoinGeckoOHLCVProvider | None = None,
    ) -> None:
        """Initialize the DEX historical price test.

        Args:
            config: QA configuration with token lists and thresholds
            ohlcv_provider: Optional CoinGeckoOHLCVProvider instance. If None,
                           a default instance will be created.
        """
        self.config = config
        self.ohlcv_provider = ohlcv_provider or CoinGeckoOHLCVProvider()

    async def run(self) -> list[DEXHistoricalResult]:
        """Run the DEX historical price test for all configured tokens.

        Returns:
            List of DEXHistoricalResult for each token tested.
        """
        results: list[DEXHistoricalResult] = []

        # Use dex_tokens if available, otherwise use all_tokens
        tokens = self.config.dex_tokens if self.config.dex_tokens else self.config.all_tokens

        if not tokens:
            logger.info("No tokens configured for DEX historical test")
            return results

        # Skip ETH as we can't get WETH price for ETH itself
        tokens = [t for t in tokens if t != "ETH"]

        logger.info(
            "Running DEX historical test for %d tokens (timeframe=%s, days=%d)",
            len(tokens),
            self.config.timeframe,
            self.config.historical_days,
        )

        # First, fetch ETH/USD candles for conversion
        eth_candles = await self._fetch_eth_candles()
        if eth_candles is None:
            # If ETH data unavailable, all tokens fail
            for token in tokens:
                results.append(
                    DEXHistoricalResult(
                        token=token,
                        weth_prices=[],
                        total_points=0,
                        passed=False,
                        error="ETH/USD data unavailable for WETH conversion",
                    )
                )
            return results

        # Build ETH price lookup by timestamp
        eth_prices = self._build_price_lookup(eth_candles)

        for token in tokens:
            result = await self._test_token(token, eth_prices)
            results.append(result)

        # Log summary
        passed_count = sum(1 for r in results if r.passed)
        logger.info(
            "DEX historical test complete: %d/%d passed",
            passed_count,
            len(results),
        )

        return results

    def _calculate_expected_candles(self) -> int:
        """Calculate expected number of candles based on timeframe and days.

        Returns:
            Expected number of candles
        """
        # Mapping of timeframe to seconds
        timeframe_seconds: dict[str, int] = {
            "1m": 60,
            "5m": 300,
            "15m": 900,
            "1h": 3600,
            "4h": 14400,
            "1d": 86400,
        }
        tf_seconds = timeframe_seconds.get(self.config.timeframe, 14400)
        total_seconds = self.config.historical_days * 24 * 3600
        return total_seconds // tf_seconds

    async def _fetch_eth_candles(self) -> list[OHLCVCandle] | None:
        """Fetch ETH/USD candles for WETH conversion.

        Returns:
            List of ETH candles or None if unavailable
        """
        try:
            limit = self._calculate_expected_candles()
            candles = await self.ohlcv_provider.get_ohlcv(
                token="ETH",
                quote="USD",
                timeframe=self.config.timeframe,
                limit=limit,
            )
            if not candles:
                logger.warning("No ETH/USD OHLCV data returned")
                return None
            return candles
        except DataSourceUnavailable as e:
            logger.warning("ETH/USD data unavailable: %s", str(e))
            return None
        except Exception as e:
            logger.error("Unexpected error fetching ETH/USD data: %s", str(e))
            return None

    def _build_price_lookup(self, candles: list[OHLCVCandle]) -> dict[datetime, Decimal]:
        """Build a lookup dictionary from timestamp to close price.

        Args:
            candles: List of OHLCV candles

        Returns:
            Dictionary mapping timestamp to close price
        """
        return {c.timestamp: c.close for c in candles}

    def _find_matching_eth_price(
        self,
        timestamp: datetime,
        eth_prices: dict[datetime, Decimal],
    ) -> Decimal | None:
        """Find matching ETH price for a given timestamp.

        First tries exact match, then finds closest timestamp within tolerance.

        Args:
            timestamp: Target timestamp
            eth_prices: ETH price lookup dictionary

        Returns:
            ETH price or None if no match found
        """
        # Try exact match first
        if timestamp in eth_prices:
            return eth_prices[timestamp]

        # Find closest timestamp within 1 hour tolerance
        tolerance_seconds = 3600  # 1 hour

        closest_ts = None
        min_diff = float("inf")

        for ts in eth_prices:
            diff = abs((ts - timestamp).total_seconds())
            if diff < min_diff and diff <= tolerance_seconds:
                min_diff = diff
                closest_ts = ts

        if closest_ts is not None:
            return eth_prices[closest_ts]

        return None

    async def _test_token(
        self,
        token: str,
        eth_prices: dict[datetime, Decimal],
    ) -> DEXHistoricalResult:
        """Test historical WETH-denominated prices for a single token.

        Args:
            token: Token symbol to test
            eth_prices: ETH/USD price lookup by timestamp

        Returns:
            DEXHistoricalResult with validation results
        """
        try:
            # Fetch token/USD candles
            limit = self._calculate_expected_candles()
            token_candles = await self.ohlcv_provider.get_ohlcv(
                token=token,
                quote="USD",
                timeframe=self.config.timeframe,
                limit=limit,
            )

            if not token_candles:
                logger.warning("No OHLCV data returned for %s", token)
                return DEXHistoricalResult(
                    token=token,
                    weth_prices=[],
                    total_points=0,
                    passed=False,
                    error="No OHLCV data returned",
                )

            # Sort candles by timestamp
            token_candles = sorted(token_candles, key=lambda c: c.timestamp)

            # Calculate WETH prices
            weth_prices: list[WETHPricePoint] = []
            conversion_errors = 0

            for candle in token_candles:
                eth_price = self._find_matching_eth_price(candle.timestamp, eth_prices)

                if eth_price is None or eth_price <= 0:
                    conversion_errors += 1
                    continue

                # WETH price = token_usd / eth_usd
                weth_price = candle.close / eth_price

                if weth_price <= 0:
                    conversion_errors += 1
                    continue

                weth_prices.append(
                    WETHPricePoint(
                        timestamp=candle.timestamp,
                        price_weth=weth_price,
                    )
                )

            # Validate results
            if not weth_prices:
                return DEXHistoricalResult(
                    token=token,
                    weth_prices=[],
                    total_points=0,
                    passed=False,
                    error="No WETH prices could be derived (all conversions failed)",
                )

            # Pass if we have at least 50% of expected data points
            min_points = len(token_candles) // 2
            passed = len(weth_prices) >= min_points and len(weth_prices) > 0

            error = None
            if not passed:
                error = f"Insufficient WETH prices: {len(weth_prices)}/{len(token_candles)} converted"

            logger.debug(
                "DEX historical for %s: %d WETH prices (errors=%d, passed=%s)",
                token,
                len(weth_prices),
                conversion_errors,
                passed,
            )

            return DEXHistoricalResult(
                token=token,
                weth_prices=weth_prices,
                total_points=len(weth_prices),
                passed=passed,
                error=error,
            )

        except DataSourceUnavailable as e:
            logger.warning(
                "DEX historical data unavailable for %s: %s",
                token,
                str(e),
            )
            return DEXHistoricalResult(
                token=token,
                weth_prices=[],
                total_points=0,
                passed=False,
                error=f"Data source unavailable: {e.reason}",
            )

        except Exception as e:
            logger.error(
                "Unexpected error fetching DEX historical data for %s: %s",
                token,
                str(e),
            )
            return DEXHistoricalResult(
                token=token,
                weth_prices=[],
                total_points=0,
                passed=False,
                error=f"Unexpected error: {str(e)}",
            )

    async def close(self) -> None:
        """Close resources (HTTP sessions, etc.)."""
        await self.ohlcv_provider.close()

    async def __aenter__(self) -> "DEXHistoricalTest":
        """Async context manager entry."""
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Async context manager exit."""
        await self.close()


__all__ = [
    "DEXHistoricalResult",
    "DEXHistoricalTest",
    "WETHPricePoint",
]
