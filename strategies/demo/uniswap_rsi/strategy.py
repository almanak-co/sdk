"""
===============================================================================
TUTORIAL: Uniswap RSI Strategy
===============================================================================

This is a tutorial strategy demonstrating how to build an RSI-based trading
strategy that executes swaps on Uniswap V3. It's designed to teach you the
fundamentals of the Almanak strategy framework.

WHAT THIS STRATEGY DOES:
------------------------
1. Monitors the RSI (Relative Strength Index) of (W)ETH
2. When RSI < 30 (oversold): Buys (W)ETH with USDC
3. When RSI > 70 (overbought): Sells (W)ETH for USDC
4. When RSI is between 30-70 (neutral): Holds, no action

RSI EXPLAINED:
--------------
RSI is a momentum indicator that measures the speed and magnitude of price
changes. It oscillates between 0 and 100:
- RSI < 30: Asset is "oversold" - may be undervalued (buy signal)
- RSI > 70: Asset is "overbought" - may be overvalued (sell signal)
- RSI 30-70: Neutral territory (hold)

STRATEGY PATTERN:
-----------------
Every Almanak strategy follows this pattern:
1. Inherit from IntentStrategy
2. Use @almanak_strategy decorator for metadata
3. Implement decide(market) method that returns an Intent
4. The framework handles compilation and execution of the Intent

FILE STRUCTURE:
---------------
strategies/demo/uniswap_rsi/
    __init__.py      - Package exports
    strategy.py      - This file (main strategy logic)
    config.json      - Default configuration
    run_anvil.py     - Test script for running on Anvil fork
    README.md        - Documentation

USAGE:
------
    # Run once in dry-run mode (no real transactions)
    python -m src.cli.run --strategy demo_uniswap_rsi --once --dry-run

    # Run continuously every 60 seconds
    python -m src.cli.run --strategy demo_uniswap_rsi --interval 60

    # Test on Anvil (local fork)
    python strategies/demo/uniswap_rsi/run_anvil.py

===============================================================================
"""

# =============================================================================
# IMPORTS
# =============================================================================
#
# These are the core imports you'll need for most strategies.
# The framework provides clean abstractions so you focus on strategy logic.

import logging
from datetime import UTC
from decimal import Decimal
from typing import Any

# Intent is what your strategy returns - a high-level action description
from almanak.framework.intents import Intent

# Core strategy framework imports
from almanak.framework.strategies import (
    IntentStrategy,  # Base class for all strategies
    MarketSnapshot,  # Contains market data (prices, RSI, balances)
    almanak_strategy,  # Decorator for strategy registration
)

# Logging utilities for user-friendly output
from almanak.framework.utils.log_formatters import format_usd

# Logger for debugging and monitoring
logger = logging.getLogger(__name__)


# =============================================================================
# STRATEGY METADATA (via decorator)
# =============================================================================
#
# The @almanak_strategy decorator registers your strategy with the framework
# and provides important metadata for:
# - Discovery: Strategy can be found by name
# - Documentation: Description, author, version
# - Runtime: What chains and protocols are supported
# - Validation: What intent types the strategy may emit


@almanak_strategy(
    # Unique identifier - used to run the strategy via CLI
    # Example: python -m src.cli.run --strategy demo_uniswap_rsi
    name="demo_uniswap_rsi",
    # Human-readable description for documentation
    description="Tutorial RSI strategy - buys when oversold, sells when overbought on Uniswap V3",
    # Semantic versioning for tracking changes
    version="1.0.0",
    # Author information
    author="Almanak",
    # Tags for categorization and search
    # Use descriptive tags that help users find relevant strategies
    tags=["demo", "tutorial", "trading", "rsi", "mean-reversion", "uniswap"],
    # Which blockchains this strategy supports
    # The strategy can be deployed on any of these chains
    supported_chains=["arbitrum", "ethereum", "base", "optimism"],
    # Which protocols this strategy interacts with
    # This helps with intent compilation and validation
    supported_protocols=["uniswap_v3"],
    # What types of intents this strategy may return
    # SWAP: Exchange one token for another
    # HOLD: No action (wait for better conditions)
    intent_types=["SWAP", "HOLD"],
    default_chain="ethereum",
)
class UniswapRSIStrategy(IntentStrategy):
    """
    A simple RSI-based mean reversion strategy for educational purposes.

    This strategy demonstrates:
    - How to read market data (prices, RSI, balances)
    - How to implement trading logic
    - How to return Intents for execution
    - How to handle edge cases and errors

    Configuration Parameters (from config.json):
    --------------------------------------------
    - trade_size_usd: How much to trade per signal (default: 100)
    - rsi_period: Number of periods for RSI calculation (default: 14)
    - rsi_oversold: RSI level that triggers buy (default: 30)
    - rsi_overbought: RSI level that triggers sell (default: 70)
    - max_slippage_bps: Maximum allowed slippage in basis points (default: 50 = 0.5%)
    - base_token: Token to trade (default: "WETH")
    - quote_token: Token to use as quote (default: "USDC")

    Example Config:
    ---------------
    {
        "trade_size_usd": 100,
        "rsi_period": 14,
        "rsi_oversold": 30,
        "rsi_overbought": 70,
        "max_slippage_bps": 50,
        "base_token": "WETH",
        "quote_token": "USDC"
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
        - self.chain: The blockchain to operate on
        - self.wallet_address: The wallet executing trades

        Here we extract our strategy-specific parameters from config.
        We use .get() with defaults to make the strategy work without config.

        Parameters:
            *args: Positional arguments passed to base class
            **kwargs: Keyword arguments including config, chain, wallet_address
        """
        # Always call parent __init__ first
        super().__init__(*args, **kwargs)

        # =====================================================================
        # Extract configuration with safe defaults
        # =====================================================================
        # config can be:
        # - A dict (from JSON config file)
        # - A HotReloadableConfig (from runtime/test scripts)
        # - A custom dataclass
        # We handle all cases here for flexibility

        # Trading parameters
        self.trade_size_usd = Decimal(str(self.get_config("trade_size_usd", "10")))

        # RSI parameters
        # - rsi_period: How many candles to use for RSI calculation
        # - rsi_oversold: RSI below this = buy signal
        # - rsi_overbought: RSI above this = sell signal
        self.rsi_period = int(self.get_config("rsi_period", 14))
        self.rsi_oversold = Decimal(str(self.get_config("rsi_oversold", "30")))
        self.rsi_overbought = Decimal(str(self.get_config("rsi_overbought", "70")))

        # Slippage protection
        # 50 bps = 0.5% slippage tolerance
        self.max_slippage_bps = int(self.get_config("max_slippage_bps", 50))

        # Token configuration
        # WETH/USDC is the most liquid pair on Uniswap
        self.base_token = self.get_config("base_token", "WETH")
        self.quote_token = self.get_config("quote_token", "USDC")

        # =====================================================================
        # Internal state tracking (optional but useful)
        # =====================================================================
        # Track how many times we've held in a row
        # This can be useful for logging/debugging
        self._consecutive_holds = 0

        # Log initialization for debugging
        logger.info(
            f"UniswapRSIStrategy initialized: "
            f"trade_size=${self.trade_size_usd}, "
            f"RSI period={self.rsi_period}, "
            f"oversold={self.rsi_oversold}, "
            f"overbought={self.rsi_overbought}, "
            f"pair={self.base_token}/{self.quote_token}"
        )

    # =========================================================================
    # MAIN DECISION LOGIC
    # =========================================================================

    def decide(self, market: MarketSnapshot) -> Intent | None:
        """
        Make a trading decision based on current market conditions.

        This is the CORE method of any strategy. It's called by the framework
        on each iteration (e.g., every 60 seconds) with fresh market data.

        Parameters:
            market: MarketSnapshot containing:
                - market.price(token): Get current price in USD
                - market.rsi(token, period): Get RSI indicator
                - market.balance(token): Get wallet balance
                - market.chain: Current chain
                - market.wallet_address: Current wallet

        Returns:
            Intent: What action to take
                - Intent.swap(...): Execute a swap
                - Intent.hold(...): Do nothing
                - None: Also means hold (but prefer Intent.hold for clarity)

        Decision Flow:
            1. Get current market data (price, RSI)
            2. Check RSI against thresholds
            3. Check we have sufficient balance
            4. Return appropriate Intent

        Error Handling:
            Catch specific exceptions (e.g., ValueError) where recovery is possible.
            Let unexpected errors propagate to the framework's STRATEGY_ERROR handler.
        """

        # =================================================================
        # STEP 1: Get current market price
        # =================================================================
        # We need the price to:
        # - Calculate how much ETH to sell for our USD trade size
        # - Log what's happening for debugging

        base_price = market.price(self.base_token)
        logger.debug(f"Current {self.base_token} price: ${base_price:,.2f}")

        # =================================================================
        # STEP 2: Get RSI indicator
        # =================================================================
        # RSI is our primary signal. The market.rsi() method returns
        # an RSI object with a .value property.
        #
        # If RSI data isn't available (e.g., not enough historical data),
        # we should hold and wait.

        try:
            rsi = market.rsi(self.base_token, period=self.rsi_period)
            logger.debug(f"{self.base_token} RSI({self.rsi_period}): {rsi.value:.2f}")
        except ValueError as e:
            # RSI calculation failed - data might not be available
            logger.warning(f"Could not get RSI: {e}")
            return Intent.hold(reason="RSI data unavailable")

        # =================================================================
        # STEP 3: Get wallet balances
        # =================================================================
        # Before deciding to trade, check we have sufficient funds.
        # The balance() method returns a Balance object with:
        # - .balance: Raw token amount (e.g., 1.5 WETH)
        # - .balance_usd: Value in USD (e.g., $5100)

        try:
            quote_balance = market.balance(self.quote_token)  # USDC for buying
            base_balance = market.balance(self.base_token)  # WETH for selling

            logger.debug(
                f"Balances - {self.quote_token}: ${quote_balance.balance_usd:,.2f}, "
                f"{self.base_token}: {base_balance.balance} (${base_balance.balance_usd:,.2f})"
            )
        except ValueError as e:
            logger.warning(f"Could not get balances: {e}")
            return Intent.hold(reason="Balance data unavailable")

        # =================================================================
        # STEP 4: Trading decision logic
        # =================================================================
        # This is where the actual strategy logic lives.
        # We check RSI against our thresholds and decide what to do.

        # -----------------------------------------------------------------
        # CASE 1: OVERSOLD (RSI < 30) -> BUY
        # -----------------------------------------------------------------
        # The asset appears undervalued. We want to buy.

        if rsi.value <= self.rsi_oversold:
            # First, check we have enough quote token (USDC) to buy
            if quote_balance.balance_usd < self.trade_size_usd:
                return Intent.hold(
                    reason=f"Oversold (RSI={rsi.value:.1f}) but insufficient {self.quote_token} "
                    f"(${quote_balance.balance_usd:.2f} < ${self.trade_size_usd})"
                )

            # We have funds! Log the buy signal with formatted amounts
            logger.info(
                f"📈 BUY SIGNAL: RSI={rsi.value:.2f} < {self.rsi_oversold} (oversold) "
                f"| Buying {format_usd(self.trade_size_usd)} of {self.base_token}"
            )

            # Reset our hold counter
            self._consecutive_holds = 0

            # Return a SWAP intent: USDC -> WETH
            return Intent.swap(
                from_token=self.quote_token,  # Selling USDC
                to_token=self.base_token,  # Buying WETH
                amount_usd=self.trade_size_usd,
                max_slippage=Decimal(str(self.max_slippage_bps)) / Decimal("10000"),  # Convert bps to decimal
                protocol="uniswap_v3",  # Explicit protocol (optional but recommended)
            )

        # -----------------------------------------------------------------
        # CASE 2: OVERBOUGHT (RSI > 70) -> SELL
        # -----------------------------------------------------------------
        # The asset appears overvalued. We want to sell.

        elif rsi.value >= self.rsi_overbought:
            # Calculate how much base token we need to sell for our trade size
            min_base_to_sell = self.trade_size_usd / base_price

            # Check we have enough base token (WETH) to sell
            if base_balance.balance < min_base_to_sell:
                return Intent.hold(
                    reason=f"Overbought (RSI={rsi.value:.1f}) but insufficient {self.base_token} "
                    f"({base_balance.balance:.4f} < {min_base_to_sell:.4f})"
                )

            # We have funds! Log the sell signal with formatted amounts
            logger.info(
                f"📉 SELL SIGNAL: RSI={rsi.value:.2f} > {self.rsi_overbought} (overbought) "
                f"| Selling {format_usd(self.trade_size_usd)} of {self.base_token}"
            )

            # Reset our hold counter
            self._consecutive_holds = 0

            # Return a SWAP intent: WETH -> USDC
            return Intent.swap(
                from_token=self.base_token,  # Selling WETH
                to_token=self.quote_token,  # Buying USDC
                amount_usd=self.trade_size_usd,
                max_slippage=Decimal(str(self.max_slippage_bps)) / Decimal("10000"),  # Convert bps to decimal
                protocol="uniswap_v3",
            )

        # -----------------------------------------------------------------
        # CASE 3: NEUTRAL (30 < RSI < 70) -> HOLD
        # -----------------------------------------------------------------
        # No clear signal. Stay on the sidelines.

        else:
            self._consecutive_holds += 1

            return Intent.hold(
                reason=f"RSI={rsi.value:.2f} in neutral zone "
                f"[{self.rsi_oversold}-{self.rsi_overbought}] "
                f"(hold #{self._consecutive_holds})"
            )

    # =========================================================================
    # OPTIONAL: STATUS REPORTING
    # =========================================================================

    def get_status(self) -> dict[str, Any]:
        """
        Get current strategy status for monitoring/dashboards.

        This is optional but useful for:
        - Debugging
        - Dashboard displays
        - Logging

        Returns:
            Dictionary with strategy status information
        """
        return {
            "strategy": "demo_uniswap_rsi",
            "chain": self.chain,
            "wallet": self.wallet_address[:10] + "...",
            "config": {
                "trade_size_usd": str(self.trade_size_usd),
                "rsi_period": self.rsi_period,
                "rsi_oversold": str(self.rsi_oversold),
                "rsi_overbought": str(self.rsi_overbought),
                "max_slippage_bps": self.max_slippage_bps,
                "pair": f"{self.base_token}/{self.quote_token}",
            },
            "state": {
                "consecutive_holds": self._consecutive_holds,
            },
        }

    # =========================================================================
    # TEARDOWN SUPPORT
    # =========================================================================

    def supports_teardown(self) -> bool:
        """Indicate this strategy supports safe teardown.

        Swap-based strategies have simple teardown:
        - Convert any base token holdings back to quote token (stable)

        Returns:
            True - this strategy can be safely torn down
        """
        return True

    def get_open_positions(self) -> "TeardownPositionSummary":
        """Get summary of open positions for teardown preview.

        For swap strategies, "positions" are token holdings:
        - If holding base token (WETH), that's the position to close
        - Quote token (USDC) is the target, no action needed

        Returns:
            TeardownPositionSummary with token position details
        """
        from datetime import datetime

        from almanak.framework.teardown import (
            PositionInfo,
            PositionType,
            TeardownPositionSummary,
        )

        positions: list[PositionInfo] = []

        # For swap strategies, we track base token as the "position"
        # The value would come from actual balance queries in production
        # TODO: That's not true - what if you buy several times? Need to check on-chain balance here.
        estimated_value = self.trade_size_usd

        positions.append(
            PositionInfo(
                position_type=PositionType.TOKEN,
                position_id="uniswap_rsi_token_0",
                chain=self.chain,
                protocol="uniswap_v3",
                value_usd=estimated_value,
                details={
                    "asset": self.base_token,
                    "base_token": self.base_token,
                    "quote_token": self.quote_token,
                    "consecutive_holds": self._consecutive_holds,
                },
            )
        )

        return TeardownPositionSummary(
            strategy_id=getattr(self, "strategy_id", "demo_uniswap_rsi"),
            timestamp=datetime.now(UTC),
            positions=positions,
        )

    def generate_teardown_intents(self, mode: "TeardownMode", market=None) -> list[Intent]:
        """Generate intents to close all positions.

        For swap strategies, teardown means:
        - Swap any base token holdings back to quote token (stable)

        Args:
            mode: TeardownMode (SOFT or HARD) - affects slippage tolerance

        Returns:
            List of SWAP intents to convert to stable
        """
        from almanak.framework.teardown import TeardownMode

        intents: list[Intent] = []

        # Determine slippage based on mode
        if mode == TeardownMode.HARD:
            # Emergency: higher slippage tolerance for faster exit
            max_slippage = Decimal("0.03")  # 3%
        else:
            # Graceful: use configured slippage
            max_slippage = Decimal(str(self.max_slippage_bps)) / Decimal("10000")

        logger.info(
            f"Generating teardown intent: swap {self.base_token} -> "
            f"{self.quote_token} (mode={mode.value}, slippage={max_slippage})"
        )

        # Swap all base token back to quote token
        intents.append(
            Intent.swap(
                from_token=self.base_token,
                to_token=self.quote_token,
                amount="all",  # Swap entire balance
                max_slippage=max_slippage,
                protocol="uniswap_v3",
            )
        )

        return intents


# =============================================================================
# TESTING
# =============================================================================
# This block runs when you execute this file directly:
#   python strategies/demo/uniswap_rsi/strategy.py

if __name__ == "__main__":
    print("=" * 60)
    print("UniswapRSIStrategy - Demo Strategy")
    print("=" * 60)
    print(f"\nStrategy Name: {UniswapRSIStrategy.STRATEGY_NAME}")
    print(f"Version: {UniswapRSIStrategy.STRATEGY_METADATA.version}")
    print(f"Supported Chains: {UniswapRSIStrategy.SUPPORTED_CHAINS}")
    print(f"Supported Protocols: {UniswapRSIStrategy.SUPPORTED_PROTOCOLS}")
    print(f"Intent Types: {UniswapRSIStrategy.INTENT_TYPES}")
    print(f"\nDescription: {UniswapRSIStrategy.STRATEGY_METADATA.description}")
    print("\nTo run this strategy:")
    print("  python -m src.cli.run --strategy demo_uniswap_rsi --once --dry-run")
    print("\nTo test on Anvil:")
    print("  python strategies/demo/uniswap_rsi/run_anvil.py")
