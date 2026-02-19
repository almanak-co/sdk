"""
===============================================================================
TUTORIAL: Polymarket Signal Trader Strategy
===============================================================================

This is a tutorial strategy demonstrating how to build a signal-based trading
strategy for prediction markets on Polymarket. It's designed to teach you the
fundamentals of using external signals with the Almanak strategy framework.

WHAT THIS STRATEGY DOES:
------------------------
1. Monitors external signals for a specific prediction market
2. When signal is BULLISH with high confidence: Buys YES shares
3. When signal is BEARISH with high confidence: Buys NO shares (or sells YES)
4. When signal is NEUTRAL or low confidence: Holds, no action

STRATEGY PATTERN:
-----------------
Every Almanak prediction strategy follows this pattern:
1. Inherit from IntentStrategy
2. Use @almanak_strategy decorator for metadata
3. Implement decide(market) method that returns a prediction Intent
4. The framework handles compilation and execution via CLOB API

FILE STRUCTURE:
---------------
strategies/demo/polymarket_signal_trader/
    __init__.py      - Package exports
    strategy.py      - This file (main strategy logic)
    config.json      - Default configuration
    run_anvil.py     - Test script for running with mock signals
    README.md        - Documentation

USAGE:
------
    # Run once in dry-run mode (no real trades)
    python -m src.cli.run --strategy demo_polymarket_signal_trader --once --dry-run

    # Test with mocked signals
    python strategies/demo/polymarket_signal_trader/run_anvil.py

===============================================================================
"""

# =============================================================================
# IMPORTS
# =============================================================================
#
# These are the core imports you'll need for Polymarket strategies.

import logging
from decimal import Decimal
from typing import TYPE_CHECKING, Any

# Signal framework for external signal integration
from almanak.framework.connectors.polymarket.signals import (
    SignalDirection,
    SignalResult,
)

# Intent is what your strategy returns - a high-level action description
from almanak.framework.intents import Intent

# Exit conditions for automatic position monitoring
from almanak.framework.services.prediction_monitor import PredictionExitConditions

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
# STRATEGY METADATA (via decorator)
# =============================================================================


@almanak_strategy(
    # Unique identifier - used to run the strategy via CLI
    name="demo_polymarket_signal_trader",
    # Human-readable description for documentation
    description="Tutorial signal-based strategy - trades prediction markets based on external signals",
    # Semantic versioning for tracking changes
    version="1.0.0",
    # Author information
    author="Almanak",
    # Tags for categorization and search
    tags=["demo", "tutorial", "prediction", "signals", "polymarket"],
    # Which blockchains this strategy supports (Polymarket is on Polygon)
    supported_chains=["polygon"],
    # Which protocols this strategy interacts with
    supported_protocols=["polymarket"],
    # What types of intents this strategy may return
    intent_types=["PREDICTION_BUY", "PREDICTION_SELL", "HOLD"],
)
class PolymarketSignalTraderStrategy(IntentStrategy):
    """
    A simple signal-based prediction market strategy for educational purposes.

    This strategy demonstrates:
    - How to read market data for prediction markets
    - How to integrate external signals
    - How to return Prediction Intents for execution
    - How to set up exit conditions (stop-loss, take-profit)
    - How to handle edge cases and errors

    Configuration Parameters (from config.json):
    --------------------------------------------
    - market_id: Polymarket market ID or slug to trade
    - trade_size_usd: How much to trade per signal (default: 10)
    - min_confidence: Minimum signal confidence to trade (default: 0.6)
    - min_edge: Minimum edge vs market price to trade (default: 0.05 = 5%)
    - order_type: "market" or "limit" (default: "market")
    - stop_loss_pct: Stop-loss percentage (default: 0.20 = 20%)
    - take_profit_pct: Take-profit percentage (default: 0.30 = 30%)
    - exit_before_resolution_hours: Exit this many hours before resolution
    - use_signals: List of signal sources to use

    Example Config:
    ---------------
    {
        "market_id": "will-bitcoin-exceed-100000-by-2025",
        "trade_size_usd": 10,
        "min_confidence": 0.6,
        "min_edge": 0.05,
        "order_type": "market",
        "stop_loss_pct": 0.20,
        "take_profit_pct": 0.30
    }
    """

    # =========================================================================
    # INITIALIZATION
    # =========================================================================

    def __init__(self, *args, **kwargs):
        """
        Initialize the strategy with configuration.

        The base class (IntentStrategy) handles:
        - self.config: Strategy configuration (dict or dataclass)
        - self.chain: The blockchain to operate on (should be "polygon")
        - self.wallet_address: The wallet executing trades

        Here we extract our strategy-specific parameters from config.
        """
        # Always call parent __init__ first
        super().__init__(*args, **kwargs)

        # =====================================================================
        # Extract configuration with safe defaults
        # =====================================================================

        def get_config(key: str, default: Any) -> Any:
            if isinstance(self.config, dict):
                return self.config.get(key, default)
            else:
                return getattr(self.config, key, default)

        # Market to trade
        self.market_id = get_config("market_id", "")

        # Trading parameters
        self.trade_size_usd = Decimal(str(get_config("trade_size_usd", "10")))

        # Signal thresholds
        self.min_confidence = Decimal(str(get_config("min_confidence", "0.6")))
        self.min_edge = Decimal(str(get_config("min_edge", "0.05")))

        # Order configuration
        self.order_type = get_config("order_type", "market")
        self.time_in_force = get_config("time_in_force", "GTC")

        # Risk management - exit conditions
        self.stop_loss_pct = Decimal(str(get_config("stop_loss_pct", "0.20")))
        self.take_profit_pct = Decimal(str(get_config("take_profit_pct", "0.30")))
        self.exit_before_resolution_hours = get_config("exit_before_resolution_hours", 24)

        # Signal providers to use (would be configured in production)
        self.use_signals = get_config("use_signals", ["mock"])

        # Track state
        self._consecutive_holds = 0
        self._last_signal: SignalResult | None = None

        # Validate configuration
        if not self.market_id:
            logger.warning("No market_id configured - strategy will hold until market is set")

        logger.info(
            f"PolymarketSignalTraderStrategy initialized: "
            f"market={self.market_id}, "
            f"trade_size=${self.trade_size_usd}, "
            f"min_confidence={self.min_confidence}, "
            f"min_edge={self.min_edge}"
        )

    # =========================================================================
    # MAIN DECISION LOGIC
    # =========================================================================

    def decide(self, market: MarketSnapshot) -> Intent | None:
        """
        Make a trading decision based on external signals and market conditions.

        This is the CORE method of any strategy. It's called by the framework
        on each iteration with fresh market data.

        Parameters:
            market: MarketSnapshot containing:
                - market.prediction_price(market_id, outcome): Get current price
                - market.prediction_position(market_id): Get current position
                - market.balance(token): Get wallet balance

        Returns:
            Intent: What action to take
                - Intent.prediction_buy(...): Buy outcome shares
                - Intent.prediction_sell(...): Sell outcome shares
                - Intent.hold(...): Do nothing
                - None: Also means hold

        Decision Flow:
            1. Get external signals for the market
            2. Get current market price
            3. Calculate edge (signal vs market)
            4. Check confidence threshold
            5. Return appropriate Intent
        """

        try:
            # =================================================================
            # STEP 0: Validate configuration
            # =================================================================
            if not self.market_id:
                return Intent.hold(reason="No market_id configured")

            # =================================================================
            # STEP 1: Get external signal
            # =================================================================
            signal = self._get_aggregated_signal(self.market_id)
            self._last_signal = signal

            logger.debug(f"Signal for {self.market_id}: {signal.direction.value}, confidence={signal.confidence:.2f}")

            # =================================================================
            # STEP 2: Get current market price
            # =================================================================
            try:
                # Get YES price (market's implied probability)
                yes_price = market.prediction_price(self.market_id, "YES")
                if yes_price is None:
                    # Try to fetch from provider if not in snapshot
                    yes_price = self._get_market_price(self.market_id)

                if yes_price is None:
                    return Intent.hold(reason="Could not get market price")

                logger.debug(f"Market YES price: ${yes_price:.3f}")

            except (ValueError, KeyError) as e:
                logger.warning(f"Could not get market price: {e}")
                return Intent.hold(reason="Market price unavailable")

            # =================================================================
            # STEP 3: Calculate edge
            # =================================================================
            # Convert signal to implied probability
            signal_prob = self._signal_to_probability(signal)

            # Edge = how much signal probability differs from market price
            edge = abs(signal_prob - yes_price)

            logger.debug(f"Signal probability: {signal_prob:.3f}, Market price: {yes_price:.3f}, Edge: {edge:.3f}")

            # =================================================================
            # STEP 4: Check trading conditions
            # =================================================================

            # Check confidence threshold
            confidence_decimal = Decimal(str(signal.confidence))
            if confidence_decimal < self.min_confidence:
                self._consecutive_holds += 1
                return Intent.hold(
                    reason=f"Low confidence ({signal.confidence:.2f} < {self.min_confidence}) "
                    f"(hold #{self._consecutive_holds})"
                )

            # Check edge threshold
            if edge < self.min_edge:
                self._consecutive_holds += 1
                return Intent.hold(
                    reason=f"Insufficient edge ({edge:.2f} < {self.min_edge}) (hold #{self._consecutive_holds})"
                )

            # Check signal direction
            if signal.direction == SignalDirection.NEUTRAL:
                self._consecutive_holds += 1
                return Intent.hold(reason=f"Neutral signal (hold #{self._consecutive_holds})")

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
            # STEP 6: Generate trading intent
            # =================================================================

            # Reset hold counter
            self._consecutive_holds = 0

            # Set up exit conditions for position monitoring
            exit_conditions = self._create_exit_conditions(yes_price)

            # -----------------------------------------------------------------
            # CASE 1: BULLISH -> Buy YES shares
            # -----------------------------------------------------------------
            if signal.direction == SignalDirection.BULLISH:
                logger.info(
                    f"BUY YES: signal={signal.direction.value}, "
                    f"confidence={signal.confidence:.2f}, edge={edge:.2f} "
                    f"| Buying {format_usd(self.trade_size_usd)} of YES shares"
                )

                # For limit orders, set max_price slightly above current
                max_price = None
                if self.order_type == "limit":
                    max_price = min(yes_price + Decimal("0.02"), Decimal("0.99"))

                return Intent.prediction_buy(
                    market_id=self.market_id,
                    outcome="YES",
                    amount_usd=self.trade_size_usd,
                    max_price=max_price,
                    order_type=self.order_type,
                    time_in_force=self.time_in_force,
                    exit_conditions=exit_conditions,
                )

            # -----------------------------------------------------------------
            # CASE 2: BEARISH -> Buy NO shares (equivalent to shorting YES)
            # -----------------------------------------------------------------
            elif signal.direction == SignalDirection.BEARISH:
                # NO price is 1 - YES price
                no_price = Decimal("1") - yes_price

                logger.info(
                    f"BUY NO: signal={signal.direction.value}, "
                    f"confidence={signal.confidence:.2f}, edge={edge:.2f} "
                    f"| Buying {format_usd(self.trade_size_usd)} of NO shares"
                )

                max_price = None
                if self.order_type == "limit":
                    max_price = min(no_price + Decimal("0.02"), Decimal("0.99"))

                return Intent.prediction_buy(
                    market_id=self.market_id,
                    outcome="NO",
                    amount_usd=self.trade_size_usd,
                    max_price=max_price,
                    order_type=self.order_type,
                    time_in_force=self.time_in_force,
                    exit_conditions=exit_conditions,
                )

            # Should not reach here
            return Intent.hold(reason="Unknown signal direction")

        except Exception as e:
            # =================================================================
            # ERROR HANDLING
            # =================================================================
            logger.exception(f"Error in decide(): {e}")
            return Intent.hold(reason=f"Error: {str(e)}")

    # =========================================================================
    # HELPER METHODS
    # =========================================================================

    def _get_aggregated_signal(self, market_id: str) -> SignalResult:
        """
        Get aggregated signal from all configured signal providers.

        In production, this would call real signal providers. For the demo,
        we return a mock signal.
        """
        # For demo purposes, return a neutral signal
        # In production, you would instantiate real providers:
        #
        # from almanak.framework.connectors.polymarket.signals import (
        #     NewsAPISignalProvider, SocialSentimentProvider
        # )
        # signals = []
        # for provider in self.signal_providers:
        #     signals.append(provider.get_signal(market_id))
        # return aggregate_signals(signals)

        return SignalResult(
            direction=SignalDirection.NEUTRAL,
            confidence=0.5,
            source="mock",
            metadata={"demo": True},
        )

    def _get_market_price(self, market_id: str) -> Decimal | None:
        """Get current YES price for a market (fallback method)."""
        # In production, this would call the PredictionMarketDataProvider
        # For demo, return None to let the strategy hold
        return None

    def _signal_to_probability(self, signal: SignalResult) -> Decimal:
        """Convert signal to implied probability.

        BULLISH with high confidence -> high probability (close to 1)
        BEARISH with high confidence -> low probability (close to 0)
        NEUTRAL -> 0.5
        """
        confidence = Decimal(str(signal.confidence))

        if signal.direction == SignalDirection.BULLISH:
            # Map confidence [0, 1] to probability [0.5, 1]
            return Decimal("0.5") + (confidence * Decimal("0.5"))
        elif signal.direction == SignalDirection.BEARISH:
            # Map confidence [0, 1] to probability [0, 0.5]
            return Decimal("0.5") - (confidence * Decimal("0.5"))
        else:
            return Decimal("0.5")

    def _create_exit_conditions(self, entry_price: Decimal) -> PredictionExitConditions:
        """Create exit conditions for position monitoring.

        Args:
            entry_price: Price at which we're buying

        Returns:
            PredictionExitConditions with stop-loss and take-profit
        """
        # Calculate stop-loss and take-profit prices
        stop_loss = max(entry_price - (entry_price * self.stop_loss_pct), Decimal("0.01"))
        take_profit = min(entry_price + (entry_price * self.take_profit_pct), Decimal("0.99"))

        return PredictionExitConditions(
            stop_loss_price=stop_loss,
            take_profit_price=take_profit,
            exit_before_resolution_hours=self.exit_before_resolution_hours,
        )

    # =========================================================================
    # STATUS REPORTING
    # =========================================================================

    def get_status(self) -> dict[str, Any]:
        """Get current strategy status for monitoring/dashboards."""
        return {
            "strategy": "demo_polymarket_signal_trader",
            "chain": self.chain,
            "wallet": self.wallet_address[:10] + "..." if self.wallet_address else None,
            "config": {
                "market_id": self.market_id,
                "trade_size_usd": str(self.trade_size_usd),
                "min_confidence": str(self.min_confidence),
                "min_edge": str(self.min_edge),
                "order_type": self.order_type,
                "stop_loss_pct": str(self.stop_loss_pct),
                "take_profit_pct": str(self.take_profit_pct),
            },
            "state": {
                "consecutive_holds": self._consecutive_holds,
                "last_signal": self._last_signal.serialize() if self._last_signal else None,
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

        if self.market_id:
            positions.append(
                PositionInfo(
                    position_type=PositionType.PREDICTION,
                    position_id=f"polymarket_{self.market_id}",
                    chain=self.chain,
                    protocol="polymarket",
                    value_usd=self.trade_size_usd,
                    details={
                        "market_id": self.market_id,
                        "consecutive_holds": self._consecutive_holds,
                    },
                )
            )

        return TeardownPositionSummary(
            strategy_id=getattr(self, "strategy_id", "demo_polymarket_signal_trader"),
            timestamp=datetime.now(UTC),
            positions=positions,
        )

    def generate_teardown_intents(self, mode: "TeardownMode", market=None) -> list[Intent]:
        """Generate intents to close all positions."""
        intents: list[Intent] = []

        if self.market_id:
            # Sell all YES and NO positions
            for outcome in ["YES", "NO"]:
                intents.append(
                    Intent.prediction_sell(
                        market_id=self.market_id,
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
    print("PolymarketSignalTraderStrategy - Demo Strategy")
    print("=" * 60)
    print(f"\nStrategy Name: {PolymarketSignalTraderStrategy.STRATEGY_NAME}")
    print(f"Version: {PolymarketSignalTraderStrategy.STRATEGY_METADATA.get('version', 'N/A')}")
    print(f"Supported Chains: {PolymarketSignalTraderStrategy.SUPPORTED_CHAINS}")
    print(f"Supported Protocols: {PolymarketSignalTraderStrategy.SUPPORTED_PROTOCOLS}")
    print(f"Intent Types: {PolymarketSignalTraderStrategy.INTENT_TYPES}")
    print(f"\nDescription: {PolymarketSignalTraderStrategy.STRATEGY_METADATA.get('description', 'N/A')}")
    print("\nTo run this strategy:")
    print("  python -m src.cli.run --strategy demo_polymarket_signal_trader --once --dry-run")
    print("\nTo test with mocked signals:")
    print("  python strategies/demo/polymarket_signal_trader/run_anvil.py")
