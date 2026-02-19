"""
===============================================================================
TUTORIAL: Polymarket Arbitrage Strategy
===============================================================================

This is a tutorial strategy demonstrating how to detect and trade price
discrepancies between related prediction markets on Polymarket. It's designed
to teach you about cross-market arbitrage opportunities.

WHAT THIS STRATEGY DOES:
------------------------
1. Monitors a pair of related markets (e.g., mutually exclusive outcomes)
2. Detects when prices are mispriced relative to each other
3. When YES prices sum > $1.00: Sells the overpriced outcome
4. When YES prices sum < $1.00: Buys the underpriced outcome
5. When prices are fair: Holds, no action

ARBITRAGE EXPLAINED:
--------------------
On Polymarket, some markets have mutually exclusive outcomes that together
should equal 100% probability. For example:

- "Will Bitcoin hit $100k in January?" YES = 30%
- "Will Bitcoin hit $100k in February?" YES = 25%
- "Will Bitcoin hit $100k in March?" YES = 20%
- "Will Bitcoin NOT hit $100k in Q1?" YES = ???

If the first three sum to 75%, the fourth should be 25% (0.25). If it's
trading at 0.30, there's an arbitrage opportunity!

TYPES OF ARBITRAGE:
-------------------
1. **Sum-to-one arbitrage**: Mutually exclusive outcomes should sum to 1
2. **Cross-market arbitrage**: Same event priced differently across markets
3. **Time-decay arbitrage**: Markets that resolve to the same outcome

USAGE:
------
    # Run once in dry-run mode (no real trades)
    python -m src.cli.run --strategy demo_polymarket_arbitrage --once --dry-run

    # Test with mocked prices
    python strategies/demo/polymarket_arbitrage/run_anvil.py

===============================================================================
"""

# =============================================================================
# IMPORTS
# =============================================================================

import logging
from decimal import Decimal
from typing import TYPE_CHECKING, Any

# Intent is what your strategy returns
from almanak.framework.intents import Intent

# Core strategy framework imports
from almanak.framework.strategies import (
    IntentStrategy,
    MarketSnapshot,
    almanak_strategy,
)

# Logging utilities
from almanak.framework.utils.log_formatters import format_usd

# Type hints for teardown (imported at runtime inside methods)
if TYPE_CHECKING:
    from almanak.framework.teardown import TeardownMode, TeardownPositionSummary

# Logger for debugging and monitoring
logger = logging.getLogger(__name__)


# =============================================================================
# STRATEGY METADATA
# =============================================================================


@almanak_strategy(
    # Unique identifier
    name="demo_polymarket_arbitrage",
    # Human-readable description
    description="Tutorial arbitrage strategy - detects and trades mispriced prediction markets",
    # Semantic versioning
    version="1.0.0",
    # Author information
    author="Almanak",
    # Tags for categorization
    tags=["demo", "tutorial", "prediction", "arbitrage", "polymarket"],
    # Polymarket is on Polygon
    supported_chains=["polygon"],
    # Which protocols this strategy interacts with
    supported_protocols=["polymarket"],
    # What types of intents this strategy may return
    intent_types=["PREDICTION_BUY", "PREDICTION_SELL", "HOLD"],
)
class PolymarketArbitrageStrategy(IntentStrategy):
    """
    A simple arbitrage strategy for prediction markets.

    This strategy demonstrates:
    - How to monitor multiple related markets
    - How to detect pricing discrepancies
    - How to calculate arbitrage opportunities
    - How to execute arbitrage trades

    Configuration Parameters (from config.json):
    --------------------------------------------
    - market_pair: List of market IDs that should sum to 1.0
    - min_arb_pct: Minimum arbitrage percentage to trade (default: 0.02 = 2%)
    - trade_size_usd: How much to trade per opportunity (default: 10)
    - order_type: "market" or "limit" (default: "market")
    - max_exposure_usd: Maximum total exposure across all positions

    Example Config:
    ---------------
    {
        "market_pair": [
            "will-bitcoin-hit-100k-jan",
            "will-bitcoin-not-hit-100k-jan"
        ],
        "min_arb_pct": 0.02,
        "trade_size_usd": 10,
        "order_type": "market"
    }
    """

    # =========================================================================
    # INITIALIZATION
    # =========================================================================

    def __init__(self, *args, **kwargs):
        """Initialize the strategy with configuration."""
        super().__init__(*args, **kwargs)

        def get_config(key: str, default: Any) -> Any:
            if isinstance(self.config, dict):
                return self.config.get(key, default)
            else:
                return getattr(self.config, key, default)

        # Markets to monitor (should be mutually exclusive)
        self.market_pair = get_config("market_pair", [])

        # Trading parameters
        self.min_arb_pct = Decimal(str(get_config("min_arb_pct", "0.02")))
        self.trade_size_usd = Decimal(str(get_config("trade_size_usd", "10")))
        self.max_exposure_usd = Decimal(str(get_config("max_exposure_usd", "100")))

        # Order configuration
        self.order_type = get_config("order_type", "market")

        # Track state
        self._consecutive_holds = 0
        self._total_exposure = Decimal("0")

        # Validate configuration
        if len(self.market_pair) < 2:
            logger.warning("Market pair not configured - need at least 2 markets")

        logger.info(
            f"PolymarketArbitrageStrategy initialized: "
            f"markets={self.market_pair}, "
            f"min_arb={self.min_arb_pct:.1%}, "
            f"trade_size=${self.trade_size_usd}"
        )

    # =========================================================================
    # MAIN DECISION LOGIC
    # =========================================================================

    def decide(self, market: MarketSnapshot) -> Intent | None:
        """
        Detect and trade arbitrage opportunities.

        Decision Flow:
            1. Get prices for all markets in the pair
            2. Calculate sum of YES prices
            3. If sum > 1: Market is overpriced, opportunity to sell
            4. If sum < 1: Market is underpriced, opportunity to buy
            5. Trade the most mispriced market
        """

        try:
            # =================================================================
            # STEP 0: Validate configuration
            # =================================================================
            if len(self.market_pair) < 2:
                return Intent.hold(reason="Market pair not configured")

            # =================================================================
            # STEP 1: Get prices for all markets
            # =================================================================
            prices: dict[str, Decimal] = {}

            for market_id in self.market_pair:
                try:
                    yes_price = market.prediction_price(market_id, "YES")
                    if yes_price is None:
                        yes_price = self._get_market_price(market_id)

                    if yes_price is None:
                        return Intent.hold(reason=f"Could not get price for {market_id}")

                    prices[market_id] = yes_price

                except (ValueError, KeyError) as e:
                    logger.warning(f"Could not get price for {market_id}: {e}")
                    return Intent.hold(reason=f"Price unavailable for {market_id}")

            logger.debug(f"Market prices: {prices}")

            # =================================================================
            # STEP 2: Calculate sum and detect arbitrage
            # =================================================================
            price_sum = sum(prices.values())
            fair_sum = Decimal("1.00")

            # Arbitrage = deviation from fair sum
            arb_amount = price_sum - fair_sum
            arb_pct = abs(arb_amount)

            logger.debug(f"Price sum: {price_sum:.4f}, Fair sum: {fair_sum:.4f}, Arb: {arb_pct:.2%}")

            # =================================================================
            # STEP 3: Check if arbitrage is profitable
            # =================================================================
            if arb_pct < self.min_arb_pct:
                self._consecutive_holds += 1
                return Intent.hold(
                    reason=f"No arbitrage opportunity "
                    f"(sum={price_sum:.4f}, arb={arb_pct:.2%} < {self.min_arb_pct:.1%}) "
                    f"(hold #{self._consecutive_holds})"
                )

            # =================================================================
            # STEP 4: Check exposure limits
            # =================================================================
            if self._total_exposure >= self.max_exposure_usd:
                return Intent.hold(reason=f"Max exposure reached (${self._total_exposure} >= ${self.max_exposure_usd})")

            # =================================================================
            # STEP 5: Check wallet balance
            # =================================================================
            try:
                usdc_balance = market.balance("USDC")
                if usdc_balance.balance_usd < self.trade_size_usd:
                    return Intent.hold(
                        reason=f"Insufficient USDC (${usdc_balance.balance_usd:.2f} < ${self.trade_size_usd})"
                    )
            except (ValueError, KeyError):
                logger.warning("Could not get USDC balance, proceeding anyway")

            # =================================================================
            # STEP 6: Find the most mispriced market
            # =================================================================
            # Reset hold counter
            self._consecutive_holds = 0

            # Find the market furthest from fair price
            # For a pair summing to 1, fair price is (1 - other_price)
            best_market_id = None
            best_mispricing = Decimal("0")
            is_overpriced = False

            for market_id, price in prices.items():
                # Fair price = 1 - sum of other prices
                other_sum = sum(p for m, p in prices.items() if m != market_id)
                fair_price = Decimal("1") - other_sum

                mispricing = price - fair_price

                if abs(mispricing) > abs(best_mispricing):
                    best_market_id = market_id
                    best_mispricing = mispricing
                    is_overpriced = mispricing > 0

            if best_market_id is None:
                return Intent.hold(reason="Could not determine mispriced market")

            # =================================================================
            # STEP 7: Generate trading intent
            # =================================================================

            # -----------------------------------------------------------------
            # CASE 1: Market is OVERPRICED -> SELL or SHORT
            # -----------------------------------------------------------------
            if is_overpriced:
                # If price sum > 1, we can profit by selling the overpriced market
                # In practice, this means buying NO (which is selling YES)
                current_price = prices[best_market_id]
                no_price = Decimal("1") - current_price

                logger.info(
                    f"ARBITRAGE SELL: {best_market_id} overpriced by {best_mispricing:.2%} "
                    f"(YES=${current_price:.3f}, sum={price_sum:.4f}) "
                    f"| Buying NO shares worth {format_usd(self.trade_size_usd)}"
                )

                # Update exposure tracking
                self._total_exposure += self.trade_size_usd

                # Buy NO shares (equivalent to shorting YES)
                max_price = None
                if self.order_type == "limit":
                    max_price = min(no_price + Decimal("0.02"), Decimal("0.99"))

                return Intent.prediction_buy(
                    market_id=best_market_id,
                    outcome="NO",
                    amount_usd=self.trade_size_usd,
                    max_price=max_price,
                    order_type=self.order_type,
                )

            # -----------------------------------------------------------------
            # CASE 2: Market is UNDERPRICED -> BUY
            # -----------------------------------------------------------------
            else:
                # If price sum < 1, we can profit by buying the underpriced market
                current_price = prices[best_market_id]

                logger.info(
                    f"ARBITRAGE BUY: {best_market_id} underpriced by {abs(best_mispricing):.2%} "
                    f"(YES=${current_price:.3f}, sum={price_sum:.4f}) "
                    f"| Buying YES shares worth {format_usd(self.trade_size_usd)}"
                )

                # Update exposure tracking
                self._total_exposure += self.trade_size_usd

                # Buy YES shares
                max_price = None
                if self.order_type == "limit":
                    max_price = min(current_price + Decimal("0.02"), Decimal("0.99"))

                return Intent.prediction_buy(
                    market_id=best_market_id,
                    outcome="YES",
                    amount_usd=self.trade_size_usd,
                    max_price=max_price,
                    order_type=self.order_type,
                )

        except Exception as e:
            # =================================================================
            # ERROR HANDLING
            # =================================================================
            logger.exception(f"Error in decide(): {e}")
            return Intent.hold(reason=f"Error: {str(e)}")

    # =========================================================================
    # HELPER METHODS
    # =========================================================================

    def _get_market_price(self, market_id: str) -> Decimal | None:
        """Get current YES price for a market (fallback method)."""
        # In production, this would call the PredictionMarketDataProvider
        return None

    # =========================================================================
    # STATUS REPORTING
    # =========================================================================

    def get_status(self) -> dict[str, Any]:
        """Get current strategy status for monitoring/dashboards."""
        return {
            "strategy": "demo_polymarket_arbitrage",
            "chain": self.chain,
            "wallet": self.wallet_address[:10] + "..." if self.wallet_address else None,
            "config": {
                "market_pair": self.market_pair,
                "min_arb_pct": str(self.min_arb_pct),
                "trade_size_usd": str(self.trade_size_usd),
                "max_exposure_usd": str(self.max_exposure_usd),
                "order_type": self.order_type,
            },
            "state": {
                "consecutive_holds": self._consecutive_holds,
                "total_exposure": str(self._total_exposure),
            },
        }

    # =========================================================================
    # TEARDOWN SUPPORT
    # =========================================================================

    def supports_teardown(self) -> bool:
        """Indicate this strategy supports safe teardown."""
        return True

    def get_open_positions(self) -> "TeardownPositionSummary":
        """Get summary of open positions for teardown preview."""
        from datetime import UTC, datetime

        from almanak.framework.teardown import (
            PositionInfo,
            PositionType,
            TeardownPositionSummary,
        )

        positions: list[PositionInfo] = []

        for market_id in self.market_pair:
            positions.append(
                PositionInfo(
                    position_type=PositionType.PREDICTION,
                    position_id=f"polymarket_{market_id}",
                    chain=self.chain,
                    protocol="polymarket",
                    value_usd=self._total_exposure / len(self.market_pair) if self.market_pair else Decimal("0"),
                    details={
                        "market_id": market_id,
                    },
                )
            )

        return TeardownPositionSummary(
            strategy_id=getattr(self, "strategy_id", "demo_polymarket_arbitrage"),
            timestamp=datetime.now(UTC),
            positions=positions,
        )

    def generate_teardown_intents(self, mode: "TeardownMode", market=None) -> list[Intent]:
        """Generate intents to close all positions."""
        intents: list[Intent] = []

        for market_id in self.market_pair:
            # Sell all YES and NO positions
            for outcome in ["YES", "NO"]:
                intents.append(
                    Intent.prediction_sell(
                        market_id=market_id,
                        outcome=outcome,
                        shares="all",
                        order_type="market",
                    )
                )

        return intents


# =============================================================================
# TESTING
# =============================================================================

if __name__ == "__main__":
    print("=" * 60)
    print("PolymarketArbitrageStrategy - Demo Strategy")
    print("=" * 60)
    print(f"\nStrategy Name: {PolymarketArbitrageStrategy.STRATEGY_NAME}")
    print(f"Version: {PolymarketArbitrageStrategy.STRATEGY_METADATA.get('version', 'N/A')}")
    print(f"Supported Chains: {PolymarketArbitrageStrategy.SUPPORTED_CHAINS}")
    print(f"Supported Protocols: {PolymarketArbitrageStrategy.SUPPORTED_PROTOCOLS}")
    print(f"Intent Types: {PolymarketArbitrageStrategy.INTENT_TYPES}")
    print(f"\nDescription: {PolymarketArbitrageStrategy.STRATEGY_METADATA.get('description', 'N/A')}")
    print("\nTo run this strategy:")
    print("  python -m src.cli.run --strategy demo_polymarket_arbitrage --once --dry-run")
    print("\nTo test with mocked prices:")
    print("  python strategies/demo/polymarket_arbitrage/run_anvil.py")
