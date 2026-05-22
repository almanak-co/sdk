"""Config-driven RSI swap strategy demo.

This demo keeps the historical ``demo_uniswap_rsi`` name because it is widely
referenced by docs, CI reports, and examples, but the strategy itself is no
longer tied to Uniswap. The DEX protocol, chain, and token pair are config
fields, so the same ``strategy.py`` can run as Uniswap V3 WETH/USDC on
Arbitrum or TraderJoe V2 WAVAX/USDC on Avalanche.

WHAT THIS STRATEGY DOES:
------------------------
1. Monitors the RSI (Relative Strength Index) of the configured base token
2. When RSI < oversold threshold: Buys base token with quote token
3. When RSI > overbought threshold: Sells base token for quote token
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
from typing import TYPE_CHECKING, Any

# Intent is what your strategy returns - a high-level action description
from almanak.framework.intents import Intent

# Core strategy framework imports
from almanak.framework.market import MarketSnapshot
from almanak.framework.strategies import ConfigValidationError, IntentStrategy, almanak_strategy

# Logging utilities for user-friendly output
from almanak.framework.utils.log_formatters import format_usd

# Logger for debugging and monitoring
logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from almanak.framework.teardown import TeardownMode, TeardownPositionSummary

SUPPORTED_PROTOCOL_CHAINS: dict[str, tuple[str, ...]] = {
    "uniswap_v3": ("ethereum", "arbitrum", "optimism", "polygon", "base", "avalanche", "bnb", "monad"),
    "traderjoe_v2": ("avalanche", "arbitrum", "bnb", "ethereum"),
    "aerodrome": ("base", "optimism"),
    "pancakeswap_v3": ("bnb", "ethereum", "arbitrum", "base"),
    "sushiswap_v3": ("ethereum", "arbitrum", "base", "optimism", "polygon", "bnb"),
}

SUPPORTED_SWAP_PROTOCOLS = tuple(SUPPORTED_PROTOCOL_CHAINS)
SUPPORTED_SWAP_CHAINS = tuple(
    sorted({chain for chains in SUPPORTED_PROTOCOL_CHAINS.values() for chain in chains})
)
DEFAULT_PROTOCOL = "uniswap_v3"


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
    description="Config-driven RSI swap strategy - buys when oversold, sells when overbought",
    # Semantic versioning for tracking changes
    version="1.1.0",
    # Author information
    author="Almanak",
    # Tags for categorization and search
    # Use descriptive tags that help users find relevant strategies
    tags=["demo", "tutorial", "trading", "ta", "rsi", "mean-reversion", "swap", "config-driven"],
    # Which blockchains this strategy supports
    # The strategy can be deployed on any of these chains
    supported_chains=list(SUPPORTED_SWAP_CHAINS),
    # Which protocols this strategy interacts with
    # This helps with intent compilation and validation
    supported_protocols=list(SUPPORTED_SWAP_PROTOCOLS),
    # What types of intents this strategy may return
    # SWAP: Exchange one token for another
    # HOLD: No action (wait for better conditions)
    intent_types=["SWAP", "HOLD"],
    default_chain="ethereum",
)
class UniswapRSIStrategy(IntentStrategy):
    """
    A config-driven RSI-based mean reversion swap strategy.

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
    - protocol: Swap connector to route through (default: "uniswap_v3")
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
        "protocol": "uniswap_v3",
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

        # Routing and token configuration. The default preserves the historical
        # demo behavior; override protocol/chain/tokens together for other DEXs.
        self.protocol = str(self.get_config("protocol", DEFAULT_PROTOCOL)).strip()
        self.base_token = self.get_config("base_token", "WETH")
        self.quote_token = self.get_config("quote_token", "USDC")

        # =====================================================================
        # Internal state tracking (optional but useful)
        # =====================================================================
        # Track how many times we've held in a row
        # This can be useful for logging/debugging
        self._consecutive_holds = 0
        self._last_rsi_signal = "NEUTRAL"

        # Log initialization for debugging
        logger.info(
            f"UniswapRSIStrategy initialized: "
            f"trade_size=${self.trade_size_usd}, "
            f"RSI period={self.rsi_period}, "
            f"oversold={self.rsi_oversold}, "
            f"overbought={self.rsi_overbought}, "
            f"protocol={self.protocol}, "
            f"chain={self.chain}, "
            f"pair={self.base_token}/{self.quote_token}"
        )

    # =========================================================================
    # CONFIG VALIDATION (preflight hook)
    # =========================================================================
    #
    # validate_config() is called from IntentStrategy.__init__ AFTER the config
    # is loaded and BEFORE any other setup. Override it to enforce invariants
    # on your config so tooling like the Portfolio Manager's `strat check`
    # preflight can catch misconfigurations at construction time — not at the
    # first decide() call in production.
    #
    # Raise ConfigValidationError(message, field=...) on failure. ``field``
    # should be the offending config key when applicable; omit it for
    # cross-field invariants.

    def validate_config(self) -> None:
        """Enforce RSI strategy config invariants.

        Raises:
            ConfigValidationError: If RSI thresholds or routing config are invalid.
        """
        protocol = str(self.get_config("protocol", DEFAULT_PROTOCOL)).strip()
        if not protocol:
            raise ConfigValidationError("protocol must be a non-empty connector name", field="protocol")
        if protocol not in SUPPORTED_PROTOCOL_CHAINS:
            raise ConfigValidationError(
                f"protocol {protocol!r} is not supported by this demo; "
                f"supported protocols: {', '.join(SUPPORTED_SWAP_PROTOCOLS)}",
                field="protocol",
            )
        if self.chain not in SUPPORTED_PROTOCOL_CHAINS[protocol]:
            supported = ", ".join(SUPPORTED_PROTOCOL_CHAINS[protocol])
            raise ConfigValidationError(
                f"protocol {protocol!r} does not support chain {self.chain!r}; supported chains: {supported}",
                field="chain",
            )

        oversold = Decimal(str(self.get_config("rsi_oversold", "30")))
        overbought = Decimal(str(self.get_config("rsi_overbought", "70")))
        if oversold >= overbought:
            raise ConfigValidationError(
                f"rsi_oversold ({oversold}) must be strictly less than rsi_overbought ({overbought})",
                field="rsi_oversold",
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

        current_signal = self._classify_rsi_signal(rsi.value)

        # -----------------------------------------------------------------
        # CASE 1: OVERSOLD (RSI < threshold) -> BUY on signal transition
        # -----------------------------------------------------------------
        # The asset appears undervalued. We buy only when entering the
        # oversold zone. While RSI remains oversold, hold until it resets
        # through neutral. This prevents buy sprees across continuous ticks
        # and across restarts because _last_rsi_signal is persisted.

        if current_signal == "OVERSOLD":
            if self._last_rsi_signal == "OVERSOLD":
                self._consecutive_holds += 1
                return Intent.hold(
                    reason=f"RSI={rsi.value:.2f} remains oversold; waiting for neutral reset "
                    f"(hold #{self._consecutive_holds})"
                )

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

            # Reset our hold counter. The ``_last_rsi_signal`` latch is only
            # updated in ``on_intent_executed`` so a failed swap (transient
            # RPC / compile error) does not lock us into the "remains oversold"
            # HOLD path on the next iteration.
            self._consecutive_holds = 0

            # Return a SWAP intent: quote token -> base token
            return Intent.swap(
                from_token=self.quote_token,
                to_token=self.base_token,
                amount_usd=self.trade_size_usd,
                max_slippage=Decimal(str(self.max_slippage_bps)) / Decimal("10000"),  # Convert bps to decimal
                protocol=self.protocol,
            )

        # -----------------------------------------------------------------
        # CASE 2: OVERBOUGHT (RSI > threshold) -> SELL on signal transition
        # -----------------------------------------------------------------
        # The asset appears overvalued. We want to sell.

        elif current_signal == "OVERBOUGHT":
            if self._last_rsi_signal == "OVERBOUGHT":
                self._consecutive_holds += 1
                return Intent.hold(
                    reason=f"RSI={rsi.value:.2f} remains overbought; waiting for neutral reset "
                    f"(hold #{self._consecutive_holds})"
                )

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

            # Reset our hold counter. See note on the OVERSOLD branch above —
            # ``_last_rsi_signal`` is updated only after the framework confirms
            # the swap, so a failed execution does not falsely persist the
            # latch.
            self._consecutive_holds = 0

            # Return a SWAP intent: base token -> quote token
            return Intent.swap(
                from_token=self.base_token,
                to_token=self.quote_token,
                amount_usd=self.trade_size_usd,
                max_slippage=Decimal(str(self.max_slippage_bps)) / Decimal("10000"),  # Convert bps to decimal
                protocol=self.protocol,
            )

        # -----------------------------------------------------------------
        # CASE 3: NEUTRAL (30 < RSI < 70) -> HOLD
        # -----------------------------------------------------------------
        # No clear signal. Stay on the sidelines.

        else:
            self._consecutive_holds += 1
            self._last_rsi_signal = "NEUTRAL"

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
                "protocol": self.protocol,
                "pair": f"{self.base_token}/{self.quote_token}",
            },
            "state": {
                "consecutive_holds": self._consecutive_holds,
                "last_rsi_signal": self._last_rsi_signal,
            },
        }

    def _classify_rsi_signal(self, rsi_value: Decimal) -> str:
        if rsi_value <= self.rsi_oversold:
            return "OVERSOLD"
        if rsi_value >= self.rsi_overbought:
            return "OVERBOUGHT"
        return "NEUTRAL"

    def on_intent_executed(self, intent: Any, success: bool, result: Any) -> None:
        """Latch ``_last_rsi_signal`` only on a confirmed successful swap.

        Setting the latch inline in ``decide()`` (before the runner reports
        success) would mean a transient compile/RPC failure persists OVERSOLD
        across iterations and silently locks the strategy into the
        "remains oversold; waiting for neutral reset" HOLD branch instead of
        retrying. Direction is inferred from the swap intent's tokens.
        """
        if not success:
            return
        from_token = getattr(intent, "from_token", None)
        to_token = getattr(intent, "to_token", None)
        if from_token == self.quote_token and to_token == self.base_token:
            self._last_rsi_signal = "OVERSOLD"
        elif from_token == self.base_token and to_token == self.quote_token:
            self._last_rsi_signal = "OVERBOUGHT"

    def get_persistent_state(self) -> dict[str, Any]:
        return {
            "consecutive_holds": self._consecutive_holds,
            "last_rsi_signal": self._last_rsi_signal,
        }

    def load_persistent_state(self, state: dict[str, Any]) -> None:
        self._consecutive_holds = int(state.get("consecutive_holds", 0) or 0)
        self._last_rsi_signal = str(state.get("last_rsi_signal", "NEUTRAL") or "NEUTRAL")

    # =========================================================================
    # TEARDOWN SUPPORT
    # =========================================================================

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

        # Query on-chain balance instead of unconditionally reporting a position
        try:
            market = self.create_market_snapshot()
            base_balance = market.balance(self.base_token)
            if base_balance.balance > 0:
                positions.append(
                    PositionInfo(
                        position_type=PositionType.TOKEN,
                        position_id=f"{self.protocol}_{self.base_token.lower()}_position",
                        chain=self.chain,
                        protocol=self.protocol,
                        value_usd=base_balance.balance_usd,
                        details={
                            "asset": self.base_token,
                            "balance": str(base_balance.balance),
                            "protocol": self.protocol,
                            "base_token": self.base_token,
                            "quote_token": self.quote_token,
                        },
                    )
                )
        except Exception:
            logger.warning("Failed to query balance for teardown; reporting no positions")

        return TeardownPositionSummary(
            deployment_id=self.deployment_id,
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
                protocol=self.protocol,
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
    print(f"Supported Chains: {UniswapRSIStrategy.STRATEGY_METADATA.supported_chains}")
    print(f"Supported Protocols: {UniswapRSIStrategy.STRATEGY_METADATA.supported_protocols}")
    print(f"Intent Types: {UniswapRSIStrategy.STRATEGY_METADATA.intent_types}")
    print(f"\nDescription: {UniswapRSIStrategy.STRATEGY_METADATA.description}")
    print("\nTo run this strategy:")
    print("  python -m src.cli.run --strategy demo_uniswap_rsi --once --dry-run")
    print("\nTo test on Anvil:")
    print("  python strategies/demo/uniswap_rsi/run_anvil.py")
