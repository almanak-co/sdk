"""CEX Spot Price Test for QA Framework.

This module provides validation for CEX (Centralized Exchange) spot prices
using CoinGecko as the data source. It validates price freshness, confidence,
and data availability.

Example:
    from almanak.framework.data.qa.tests.cex_spot import CEXSpotPriceTest
    from almanak.framework.data.qa.config import load_config

    config = load_config()
    test = CEXSpotPriceTest(config)
    results = await test.run()

    for result in results:
        print(f"{result.token}: ${result.price_usd} - {'PASS' if result.passed else 'FAIL'}")
"""

import logging
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal

from almanak.framework.data.interfaces import DataSourceUnavailable
from almanak.framework.data.qa.config import QAConfig
from almanak.gateway.data.price import CoinGeckoPriceSource

logger = logging.getLogger(__name__)


@dataclass
class CEXSpotResult:
    """Result of a CEX spot price test for a single token.

    Attributes:
        token: Token symbol (e.g., "ETH", "WBTC")
        price_usd: Current USD price as Decimal, None if unavailable
        confidence: Confidence score from the data source (0.0-1.0)
        timestamp: When the price was fetched
        is_fresh: Whether the data is within the max_stale_seconds threshold
        passed: Whether all validation checks passed
        error: Error message if the test failed, None otherwise
    """

    token: str
    price_usd: Decimal | None
    confidence: float | None
    timestamp: datetime | None
    is_fresh: bool
    passed: bool
    error: str | None

    def to_dict(self) -> dict:
        """Convert result to dictionary for serialization."""
        return {
            "token": self.token,
            "price_usd": str(self.price_usd) if self.price_usd is not None else None,
            "confidence": self.confidence,
            "timestamp": self.timestamp.isoformat() if self.timestamp else None,
            "is_fresh": self.is_fresh,
            "passed": self.passed,
            "error": self.error,
        }


class CEXSpotPriceTest:
    """Test for validating CEX spot prices from CoinGecko.

    This test validates that:
    1. Prices can be fetched for all configured tokens
    2. Prices are positive
    3. Confidence meets the minimum threshold
    4. Data is fresh (within max_stale_seconds)

    Attributes:
        config: QA configuration with token lists and thresholds
        price_source: CoinGecko price source instance

    Example:
        config = load_config()
        test = CEXSpotPriceTest(config)
        results = await test.run()

        passed = all(r.passed for r in results)
        print(f"CEX Spot Test: {'PASSED' if passed else 'FAILED'}")
    """

    def __init__(
        self,
        config: QAConfig,
        price_source: CoinGeckoPriceSource | None = None,
    ) -> None:
        """Initialize the CEX spot price test.

        Args:
            config: QA configuration with token lists and thresholds
            price_source: Optional CoinGeckoPriceSource instance. If None,
                         a default instance will be created.
        """
        self.config = config
        self.price_source = price_source or CoinGeckoPriceSource()

    async def run(self) -> list[CEXSpotResult]:
        """Run the CEX spot price test for all configured tokens.

        Returns:
            List of CEXSpotResult for each token tested.
        """
        results: list[CEXSpotResult] = []

        # Test all tokens (popular + additional)
        tokens = self.config.all_tokens

        logger.info(
            "Running CEX spot price test for %d tokens",
            len(tokens),
        )

        for token in tokens:
            result = await self._test_token(token)
            results.append(result)

        # Log summary
        passed_count = sum(1 for r in results if r.passed)
        logger.info(
            "CEX spot price test complete: %d/%d passed",
            passed_count,
            len(results),
        )

        return results

    async def _test_token(self, token: str) -> CEXSpotResult:
        """Test a single token's CEX spot price.

        Args:
            token: Token symbol to test

        Returns:
            CEXSpotResult with validation results
        """
        try:
            # Fetch price from CoinGecko
            price_result = await self.price_source.get_price(token, "USD")

            # Calculate freshness
            age_seconds = price_result.age_seconds
            is_fresh = age_seconds < self.config.thresholds.max_stale_seconds

            # Validate price
            price_valid = price_result.price > 0

            # Validate confidence
            confidence_valid = price_result.confidence >= self.config.thresholds.min_confidence

            # Determine if test passed
            passed = price_valid and confidence_valid and is_fresh

            # Build error message if failed
            error = None
            if not passed:
                errors = []
                if not price_valid:
                    errors.append(f"Invalid price: {price_result.price}")
                if not confidence_valid:
                    errors.append(
                        f"Low confidence: {price_result.confidence:.2f} (min: {self.config.thresholds.min_confidence})"
                    )
                if not is_fresh:
                    errors.append(
                        f"Stale data: {age_seconds:.1f}s old (max: {self.config.thresholds.max_stale_seconds}s)"
                    )
                error = "; ".join(errors)

            logger.debug(
                "CEX spot price for %s: $%s (confidence=%.2f, fresh=%s, passed=%s)",
                token,
                price_result.price,
                price_result.confidence,
                is_fresh,
                passed,
            )

            return CEXSpotResult(
                token=token,
                price_usd=price_result.price,
                confidence=price_result.confidence,
                timestamp=price_result.timestamp,
                is_fresh=is_fresh,
                passed=passed,
                error=error,
            )

        except DataSourceUnavailable as e:
            logger.warning(
                "CEX spot price unavailable for %s: %s",
                token,
                str(e),
            )
            return CEXSpotResult(
                token=token,
                price_usd=None,
                confidence=None,
                timestamp=None,
                is_fresh=False,
                passed=False,
                error=f"Data source unavailable: {e.reason}",
            )

        except Exception as e:
            logger.error(
                "Unexpected error fetching CEX spot price for %s: %s",
                token,
                str(e),
            )
            return CEXSpotResult(
                token=token,
                price_usd=None,
                confidence=None,
                timestamp=None,
                is_fresh=False,
                passed=False,
                error=f"Unexpected error: {str(e)}",
            )

    async def close(self) -> None:
        """Close resources (HTTP sessions, etc.)."""
        await self.price_source.close()

    async def __aenter__(self) -> "CEXSpotPriceTest":
        """Async context manager entry."""
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        """Async context manager exit."""
        await self.close()


__all__ = [
    "CEXSpotResult",
    "CEXSpotPriceTest",
]
