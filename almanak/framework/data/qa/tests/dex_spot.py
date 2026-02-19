"""DEX Spot Price Test for QA Framework.

This module provides validation for DEX (Decentralized Exchange) spot prices
using the MultiDexPriceService. It validates on-chain price discovery with
WETH-quoted prices.

Example:
    from almanak.framework.data.qa.tests.dex_spot import DEXSpotPriceTest
    from almanak.framework.data.qa.config import load_config

    config = load_config()
    test = DEXSpotPriceTest(config)
    results = await test.run()

    for result in results:
        print(f"{result.token}: {result.price_weth} WETH via {result.best_dex}")
"""

import logging
from dataclasses import dataclass
from decimal import Decimal

from almanak.framework.data.qa.config import QAConfig
from almanak.gateway.data.price import (
    MultiDexPriceService,
    QuoteUnavailableError,
)

logger = logging.getLogger(__name__)

# Default trade size: $1000 equivalent in WETH (assuming ~$2500/ETH)
DEFAULT_TRADE_SIZE_WETH = Decimal("0.4")


@dataclass
class DEXSpotResult:
    """Result of a DEX spot price test for a single token.

    Attributes:
        token: Token symbol (e.g., "USDC", "LINK")
        best_dex: The DEX offering the best quote (e.g., "uniswap_v3", "enso")
        price_weth: Price in WETH (how much WETH you get per token), None if unavailable
        amount_out: Amount of output token received for the trade, None if unavailable
        price_impact_bps: Price impact in basis points (100 bps = 1%)
        passed: Whether all validation checks passed
        error: Error message if the test failed, None otherwise
    """

    token: str
    best_dex: str | None
    price_weth: Decimal | None
    amount_out: Decimal | None
    price_impact_bps: int | None
    passed: bool
    error: str | None

    def to_dict(self) -> dict:
        """Convert result to dictionary for serialization."""
        return {
            "token": self.token,
            "best_dex": self.best_dex,
            "price_weth": str(self.price_weth) if self.price_weth is not None else None,
            "amount_out": str(self.amount_out) if self.amount_out is not None else None,
            "price_impact_bps": self.price_impact_bps,
            "passed": self.passed,
            "error": self.error,
        }


class DEXSpotPriceTest:
    """Test for validating DEX spot prices with WETH quotes.

    This test validates that:
    1. DEX quotes can be fetched for all configured dex_tokens
    2. Prices are positive
    3. Price impact is below the threshold

    Attributes:
        config: QA configuration with token lists and thresholds
        dex_service: MultiDexPriceService instance
        trade_size: Trade size in WETH for quotes

    Example:
        config = load_config()
        test = DEXSpotPriceTest(config)
        results = await test.run()

        passed = all(r.passed for r in results)
        print(f"DEX Spot Test: {'PASSED' if passed else 'FAILED'}")
    """

    def __init__(
        self,
        config: QAConfig,
        dex_service: MultiDexPriceService | None = None,
        trade_size: Decimal = DEFAULT_TRADE_SIZE_WETH,
    ) -> None:
        """Initialize the DEX spot price test.

        Args:
            config: QA configuration with token lists and thresholds
            dex_service: Optional MultiDexPriceService instance. If None,
                        a default instance will be created.
            trade_size: Trade size in WETH for fetching quotes (default: 0.4 WETH ~ $1000)
        """
        self.config = config
        self.dex_service = dex_service or MultiDexPriceService(chain=config.chain)
        self.trade_size = trade_size

    async def run(self) -> list[DEXSpotResult]:
        """Run the DEX spot price test for all configured dex_tokens.

        Returns:
            List of DEXSpotResult for each token tested.
        """
        results: list[DEXSpotResult] = []

        # Test dex_tokens (tokens with sufficient DEX liquidity)
        tokens = self.config.dex_tokens

        logger.info(
            "Running DEX spot price test for %d tokens with trade size %s WETH",
            len(tokens),
            self.trade_size,
        )

        for token in tokens:
            result = await self._test_token(token)
            results.append(result)

        # Log summary
        passed_count = sum(1 for r in results if r.passed)
        logger.info(
            "DEX spot price test complete: %d/%d passed",
            passed_count,
            len(results),
        )

        return results

    async def _test_token(self, token: str) -> DEXSpotResult:
        """Test a single token's DEX spot price.

        Args:
            token: Token symbol to test

        Returns:
            DEXSpotResult with validation results
        """
        try:
            # Fetch best DEX price: token -> WETH
            # We want to know how much WETH we get for selling `trade_size` worth of token
            # But since we're quoting token->WETH, we need to figure out amount_in
            # For simplicity, we'll use WETH->token to get price, then invert
            # Actually, the PRD says "get_best_dex_price(token, 'WETH', amount)"
            # which means token_in=token, token_out=WETH
            best_result = await self.dex_service.get_best_dex_price(
                token_in=token,
                token_out="WETH",
                amount_in=self.trade_size,
            )

            # Check if we got any quotes
            if best_result.best_quote is None:
                return DEXSpotResult(
                    token=token,
                    best_dex=None,
                    price_weth=None,
                    amount_out=None,
                    price_impact_bps=None,
                    passed=False,
                    error="No DEX quotes available",
                )

            quote = best_result.best_quote

            # Validate price
            price_valid = quote.price > 0

            # Validate price impact
            price_impact_valid = quote.price_impact_bps <= self.config.thresholds.max_price_impact_bps

            # Determine if test passed
            passed = price_valid and price_impact_valid

            # Build error message if failed
            error = None
            if not passed:
                errors = []
                if not price_valid:
                    errors.append(f"Invalid price: {quote.price}")
                if not price_impact_valid:
                    errors.append(
                        f"High price impact: {quote.price_impact_bps} bps "
                        f"(max: {self.config.thresholds.max_price_impact_bps} bps)"
                    )
                error = "; ".join(errors)

            logger.debug(
                "DEX spot price for %s: %s WETH via %s (impact=%d bps, passed=%s)",
                token,
                quote.price,
                quote.dex,
                quote.price_impact_bps,
                passed,
            )

            return DEXSpotResult(
                token=token,
                best_dex=quote.dex,
                price_weth=quote.price,
                amount_out=quote.amount_out,
                price_impact_bps=quote.price_impact_bps,
                passed=passed,
                error=error,
            )

        except QuoteUnavailableError as e:
            logger.warning(
                "DEX spot price unavailable for %s: %s",
                token,
                str(e),
            )
            return DEXSpotResult(
                token=token,
                best_dex=None,
                price_weth=None,
                amount_out=None,
                price_impact_bps=None,
                passed=False,
                error=f"Quote unavailable: {e.reason}",
            )

        except Exception as e:
            logger.error(
                "Unexpected error fetching DEX spot price for %s: %s",
                token,
                str(e),
            )
            return DEXSpotResult(
                token=token,
                best_dex=None,
                price_weth=None,
                amount_out=None,
                price_impact_bps=None,
                passed=False,
                error=f"Unexpected error: {str(e)}",
            )

    async def close(self) -> None:
        """Close resources (clear caches, etc.)."""
        self.dex_service.clear_cache()

    async def __aenter__(self) -> "DEXSpotPriceTest":
        """Async context manager entry."""
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        """Async context manager exit."""
        await self.close()


__all__ = [
    "DEXSpotResult",
    "DEXSpotPriceTest",
    "DEFAULT_TRADE_SIZE_WETH",
]
