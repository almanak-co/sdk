"""GMX Perpetual Futures Demo Strategy (Tutorial).

==============================================================================
WHAT ARE PERPETUAL FUTURES?
==============================================================================

Perpetual futures (perps) are derivative contracts that:
1. Track the price of an underlying asset (e.g., ETH, BTC)
2. Allow you to trade with leverage (1.1x to 100x on GMX)
3. Never expire (unlike traditional futures)
4. Require collateral to open positions

KEY CONCEPTS:
- LONG: Profit when price goes UP
- SHORT: Profit when price goes DOWN
- COLLATERAL: The tokens you deposit to back your position
- LEVERAGE: Multiplier for your position size (higher = more risk/reward)
- LIQUIDATION: Forced position closure if losses exceed collateral

==============================================================================
GMX V2 SPECIFICS
==============================================================================

GMX V2 is a decentralized perpetual exchange on Arbitrum and Avalanche.

Markets: ETH/USD, BTC/USD, LINK/USD, ARB/USD, SOL/USD, etc.
Leverage: 1.1x to 100x (varies by market)
Collateral: WETH, USDC, USDC.e, USDT, DAI, WBTC, etc.

HOW IT WORKS:
1. You deposit collateral (e.g., 0.1 WETH)
2. Specify position size in USD terms (e.g., $7,000)
3. GMX opens a leveraged position using synthetic exposure
4. You pay an execution fee in ETH (~0.0005 ETH)
5. Close position later to realize profit/loss

==============================================================================
THIS STRATEGY
==============================================================================

A simple trend-following strategy that:
1. Opens a LONG position when there's no open position
2. Holds the position for a configurable period
3. Closes the position after the hold period
4. Repeats the cycle

This is for demonstration - real perps strategies would:
- Use technical analysis for entry/exit timing
- Implement stop-loss and take-profit levels
- Monitor funding rates and open interest
- Adjust leverage based on market volatility

==============================================================================
"""

import logging
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

from almanak.framework.intents import Intent
from almanak.framework.strategies import IntentStrategy, MarketSnapshot, almanak_strategy

# Logging utilities for user-friendly output
from almanak.framework.utils.log_formatters import format_token_amount_human, format_usd

# =============================================================================
# LOGGING SETUP
# =============================================================================
# Logging is essential for debugging and monitoring your strategy.
# We use Python's built-in logging module to track what the strategy is doing.

logger = logging.getLogger(__name__)


# =============================================================================
# STRATEGY REGISTRATION
# =============================================================================
# The @almanak_strategy decorator registers this strategy with the Almanak stack.
# This allows automatic discovery when you run:
#   python -m src.cli.run --strategy demo_gmx_perps
#
# IMPORTANT: The name must be unique and match the pattern "demo_<folder_name>".


@almanak_strategy(
    # UNIQUE NAME: Used to run the strategy via CLI
    name="demo_gmx_perps",
    # DESCRIPTION: Brief explanation of what the strategy does
    description="Tutorial: Perpetual futures trading on GMX V2",
    # VERSION: Semantic versioning for tracking changes
    version="1.0.0",
    # AUTHOR: Who wrote this strategy
    author="Almanak",
    # TAGS: Keywords for categorizing and searching
    tags=["perpetuals", "gmx", "leverage", "demo", "tutorial"],
    # SUPPORTED_CHAINS: Which blockchains this strategy works on
    # GMX V2 is available on Arbitrum and Avalanche
    supported_chains=["arbitrum", "avalanche"],
    # SUPPORTED_PROTOCOLS: Which DeFi protocols this strategy uses
    supported_protocols=["gmx_v2"],
    # INTENT_TYPES: What types of intents this strategy can emit
    # PERP_OPEN: Open a leveraged position
    # PERP_CLOSE: Close an existing position
    # HOLD: Do nothing and wait
    intent_types=["PERP_OPEN", "PERP_CLOSE", "HOLD"],
)
class GMXPerpsStrategy(IntentStrategy):
    """Tutorial strategy demonstrating GMX V2 perpetual futures.

    This strategy shows how to:
    1. Open long/short perpetual positions
    2. Calculate position sizes based on leverage
    3. Close positions after a holding period
    4. Use the PERP_OPEN and PERP_CLOSE intent types

    CONFIGURATION (from config.json):
        market (str): GMX market to trade (e.g., "ETH/USD")
        collateral_token (str): Token for collateral (e.g., "WETH")
        collateral_amount (str): Amount of collateral per position
        leverage (str): Target leverage multiplier (e.g., "2.0")
        is_long (bool): True for long, False for short
        hold_minutes (int): Minutes to hold each position
        max_slippage_pct (float): Maximum slippage percentage (e.g., 1.0 = 1%)
        force_action (str): Force specific action for testing

    INTERNAL STATE:
        The strategy tracks whether a position is open and when it was opened.
        This is in-memory only - real strategies should persist state.

    EXAMPLE:
        # Via CLI
        python -m src.cli.run --strategy demo_gmx_perps --once

        # Via Anvil test
        python strategies/demo/gmx_perps/run_anvil.py
    """

    # =========================================================================
    # INITIALIZATION
    # =========================================================================

    def __init__(self, *args, **kwargs):
        """Initialize the strategy with configuration.

        The parent class (IntentStrategy) handles:
        - Loading configuration from config.json
        - Setting up chain and wallet information
        - Initializing the state machine

        We add:
        - Parsing our specific configuration values
        - Internal state tracking for positions
        """
        # Call parent constructor first
        # This loads self.config, self.chain, self.wallet_address, etc.
        super().__init__(*args, **kwargs)

        # =====================================================================
        # CONFIGURATION PARSING
        # =====================================================================
        # self.config is either a dict (from JSON), a DictConfigWrapper, or a dataclass.
        # We handle all cases using duck typing.

        # Use .get() if available (dict or DictConfigWrapper), otherwise empty dict
        if hasattr(self.config, "get"):
            config_dict = self.config
        elif isinstance(self.config, dict):
            config_dict = self.config
        else:
            config_dict = {}

        # MARKET CONFIGURATION
        # ---------------------
        # market: The GMX market to trade (e.g., "ETH/USD", "BTC/USD")
        self.market = config_dict.get("market", "ETH/USD")

        # collateral_token: What token to use as collateral
        # Common options: WETH, USDC, USDC.e, USDT, DAI
        self.collateral_token = config_dict.get("collateral_token", "WETH")

        # collateral_amount: How much collateral per position
        # Using Decimal for precise financial calculations
        self.collateral_amount = Decimal(str(config_dict.get("collateral_amount", "0.1")))

        # leverage: Position multiplier (1.1 to 100 on GMX)
        # Higher leverage = more profit/loss per price movement
        # Example: 2x leverage means 1% price move = 2% position change
        self.leverage = Decimal(str(config_dict.get("leverage", "2.0")))

        # is_long: Direction of the position
        # True = profit when price goes UP
        # False = profit when price goes DOWN
        self.is_long = config_dict.get("is_long", True)

        # TIMING CONFIGURATION
        # ---------------------
        # hold_minutes: How long to hold each position before closing
        self.hold_minutes = int(config_dict.get("hold_minutes", 60))

        # max_slippage_pct: Maximum acceptable price slippage (percentage)
        # 1.0 = 1% maximum deviation from expected price
        self.max_slippage_pct = float(config_dict.get("max_slippage_pct", 1.0))

        # force_action: Override normal logic for testing
        # Values: "open", "close", or None
        self.force_action = config_dict.get("force_action", None)

        # =====================================================================
        # INTERNAL STATE
        # =====================================================================
        # Track position state in memory.
        # NOTE: In production, you'd want to persist this to handle restarts.

        self._has_position = False
        self._position_opened_at: datetime | None = None
        self._position_size_usd = Decimal("0")
        self._positions_opened = 0
        self._positions_closed = 0

        # Log initialization
        logger.info(
            f"GMXPerpsStrategy initialized: "
            f"market={self.market}, "
            f"collateral={self.collateral_amount} {self.collateral_token}, "
            f"leverage={self.leverage}x, "
            f"direction={'LONG' if self.is_long else 'SHORT'}, "
            f"hold={self.hold_minutes}min"
        )

    # =========================================================================
    # MAIN DECISION LOGIC
    # =========================================================================

    def decide(self, market: MarketSnapshot) -> Intent | None:
        """Make a trading decision based on current market state.

        This is the core method that the execution engine calls periodically.
        It receives a MarketSnapshot and returns an Intent.

        DECISION FLOW:
        1. Get current price for position sizing
        2. Check if we should force a specific action (for testing)
        3. If no position: open a new one
        4. If position exists and hold time exceeded: close it
        5. Otherwise: hold

        Args:
            market: Current market snapshot with prices, balances, etc.
                - market.price("ETH") -> current ETH price in USD
                - market.balance("WETH") -> wallet's WETH balance

        Returns:
            Intent to execute:
            - Intent.perp_open(...) to open a position
            - Intent.perp_close(...) to close a position
            - Intent.hold(...) to do nothing
            - None to skip this cycle (same as hold)
        """
        try:
            # =================================================================
            # STEP 1: GET CURRENT PRICE
            # =================================================================
            # Extract the index token from the market (e.g., "ETH" from "ETH/USD")
            index_token = self.market.split("/")[0]

            try:
                current_price = market.price(index_token)
                logger.debug(f"Current {index_token} price: ${current_price:,.2f}")
            except ValueError:
                # Price not available - use a reasonable estimate
                # In production, you might want to fail rather than estimate
                default_prices = {"ETH": Decimal("3500"), "BTC": Decimal("95000")}
                current_price = default_prices.get(index_token, Decimal("100"))
                logger.warning(f"Price for {index_token} unavailable, using ${current_price}")

            # =================================================================
            # STEP 2: HANDLE FORCED ACTIONS (FOR TESTING)
            # =================================================================
            # The force_action config allows us to test specific intents
            if self.force_action:
                logger.info(f"Force action requested: {self.force_action}")

                if self.force_action == "open":
                    return self._create_open_intent(current_price)
                elif self.force_action == "close":
                    return self._create_close_intent()
                else:
                    logger.warning(f"Unknown force_action: {self.force_action}")

            # =================================================================
            # STEP 3: NORMAL TRADING LOGIC
            # =================================================================
            now = datetime.now(UTC)

            # STATE A: No position -> Open new position
            if not self._has_position:
                logger.info("No open position - opening new position")
                return self._create_open_intent(current_price)

            # STATE B: Position exists -> Check if we should close
            if self._position_opened_at:
                time_held = now - self._position_opened_at
                hold_duration = timedelta(minutes=self.hold_minutes)
                time_remaining = hold_duration - time_held

                if time_held >= hold_duration:
                    logger.info(f"Hold time exceeded ({time_held} >= {hold_duration}) - closing position")
                    return self._create_close_intent()

                # Still holding
                logger.debug(f"Holding position: {time_remaining} remaining (held for {time_held})")
                return Intent.hold(
                    reason=f"Holding {self.market} position - {int(time_remaining.total_seconds())}s until close"
                )

            # Fallback: unclear state
            return Intent.hold(reason="Position state unclear - holding")

        except Exception as e:
            logger.exception(f"Error in decide(): {e}")
            return Intent.hold(reason=f"Error: {str(e)}")

    # =========================================================================
    # INTENT CREATION METHODS
    # =========================================================================

    def _create_open_intent(self, current_price: Decimal) -> Intent:
        """Create an intent to open a perpetual position.

        This method:
        1. Calculates the position size in USD based on collateral and leverage
        2. Converts slippage from percentage to decimal
        3. Creates a PERP_OPEN intent with all parameters
        4. Updates internal state

        POSITION SIZE CALCULATION:
            collateral_value = collateral_amount * current_price
            position_size = collateral_value * leverage

            Example: 0.1 ETH at $3,500 with 2x leverage
            collateral_value = 0.1 * 3500 = $350
            position_size = 350 * 2 = $700

        Args:
            current_price: Current price of the index token in USD

        Returns:
            PerpOpenIntent to open the position
        """
        # Calculate position size
        collateral_value_usd = self.collateral_amount * current_price
        position_size_usd = collateral_value_usd * self.leverage

        # Convert slippage from percentage to decimal
        # 1% = 0.01
        max_slippage = Decimal(str(self.max_slippage_pct)) / Decimal("100")

        direction = "📈 LONG" if self.is_long else "📉 SHORT"
        logger.info(
            f"{direction}: {format_token_amount_human(self.collateral_amount, self.collateral_token)} "
            f"({format_usd(collateral_value_usd)}) → {format_usd(position_size_usd)} position "
            f"@ {self.leverage}x leverage, slippage={self.max_slippage_pct}%"
        )

        # Update internal state
        # NOTE: This is optimistic - we update before confirmation
        # In production, you'd wait for confirmation
        self._has_position = True
        self._position_opened_at = datetime.now(UTC)
        self._position_size_usd = position_size_usd
        self._positions_opened += 1

        # Create the PERP_OPEN intent
        # This will be compiled to GMX V2 transaction calldata
        return Intent.perp_open(
            # REQUIRED PARAMETERS
            market=self.market,  # e.g., "ETH/USD"
            collateral_token=self.collateral_token,  # e.g., "WETH"
            collateral_amount=self.collateral_amount,  # e.g., Decimal("0.1")
            size_usd=position_size_usd,  # Total position size in USD
            is_long=self.is_long,  # True for long, False for short
            # OPTIONAL PARAMETERS
            leverage=self.leverage,  # Leverage multiplier
            max_slippage=max_slippage,  # Maximum acceptable slippage
            protocol="gmx_v2",  # Protocol to use
        )

    def _create_close_intent(self) -> Intent:
        """Create an intent to close the current position.

        This method:
        1. Converts slippage from percentage to decimal
        2. Creates a PERP_CLOSE intent
        3. Updates internal state

        CLOSE PARAMETERS:
        - size_usd: Amount to close in USD (None = close full position)
        - For partial closes, specify a specific USD amount

        Returns:
            PerpCloseIntent to close the position
        """
        # Convert slippage
        max_slippage = Decimal(str(self.max_slippage_pct)) / Decimal("100")

        direction = "📈 LONG" if self.is_long else "📉 SHORT"
        logger.info(f"🔒 Closing {direction} position: {self.market}, size={format_usd(self._position_size_usd)}")

        # Update internal state
        self._has_position = False
        self._position_opened_at = None
        self._positions_closed += 1

        # Create the PERP_CLOSE intent
        return Intent.perp_close(
            market=self.market,  # Same market as open
            collateral_token=self.collateral_token,  # Same collateral
            is_long=self.is_long,  # Same direction
            size_usd=self._position_size_usd,  # Full position size to close
            max_slippage=max_slippage,  # Maximum acceptable slippage
            protocol="gmx_v2",  # Protocol to use
        )

    # =========================================================================
    # STATUS AND MONITORING
    # =========================================================================

    def get_status(self) -> dict[str, Any]:
        """Get current strategy status for monitoring.

        This method is called by the CLI and monitoring tools to
        display strategy state.

        Returns:
            Dictionary with current strategy status
        """
        now = datetime.now(timezone.utc)
        time_held = None
        time_remaining = None

        if self._position_opened_at:
            time_held = now - self._position_opened_at
            hold_duration = timedelta(minutes=self.hold_minutes)
            time_remaining = max(hold_duration - time_held, timedelta(0))

        return {
            "strategy": "demo_gmx_perps",
            "chain": self.chain,
            "wallet": self.wallet_address[:10] + "...",
            "config": {
                "market": self.market,
                "collateral_token": self.collateral_token,
                "collateral_amount": str(self.collateral_amount),
                "leverage": str(self.leverage),
                "is_long": self.is_long,
                "hold_minutes": self.hold_minutes,
                "max_slippage_pct": self.max_slippage_pct,
            },
            "state": {
                "has_position": self._has_position,
                "position_size_usd": str(self._position_size_usd),
                "position_opened_at": (self._position_opened_at.isoformat() if self._position_opened_at else None),
                "time_held_seconds": (time_held.total_seconds() if time_held else None),
                "time_remaining_seconds": (time_remaining.total_seconds() if time_remaining else None),
                "positions_opened": self._positions_opened,
                "positions_closed": self._positions_closed,
            },
        }

    # =========================================================================
    # UTILITY METHODS
    # =========================================================================

    def reset_state(self) -> None:
        """Reset internal position state.

        Useful for testing or recovering from errors where
        the internal state has gotten out of sync.
        """
        logger.warning("Resetting position state")
        self._has_position = False
        self._position_opened_at = None
        self._position_size_usd = Decimal("0")

    # =========================================================================
    # TEARDOWN INTERFACE
    # =========================================================================
    # These methods enable safe strategy teardown (closing all positions).

    def supports_teardown(self) -> bool:
        """This strategy supports the teardown system."""
        return True

    def get_open_positions(self) -> "TeardownPositionSummary":
        """Get all open positions for teardown.

        Returns:
            TeardownPositionSummary with current perp position (if any)
        """

        from almanak.framework.teardown import PositionInfo, PositionType, TeardownPositionSummary

        positions = []

        # Check if we have an open perp position
        if self._has_position and self._position_size_usd > 0:
            positions.append(
                PositionInfo(
                    position_type=PositionType.PERP,
                    position_id=f"gmx-{self.market}-{self.chain}",
                    chain=self.chain,
                    protocol="gmx_v2",
                    value_usd=self._position_size_usd,
                    liquidation_risk=False,  # Would check health factor in production
                    details={
                        "market": self.market,
                        "is_long": self.is_long,
                        "leverage": str(self.leverage),
                        "collateral_token": self.collateral_token,
                        "opened_at": self._position_opened_at.isoformat() if self._position_opened_at else None,
                    },
                )
            )

        return TeardownPositionSummary(
            strategy_id=self.STRATEGY_NAME,
            timestamp=datetime.now(UTC),
            positions=positions,
        )

    def generate_teardown_intents(self, mode: "TeardownMode", market=None) -> list[Intent]:
        """Generate intents to close all positions.

        For perps, we close the position and swap collateral to USDC.

        Args:
            mode: TeardownMode.SOFT (graceful) or TeardownMode.HARD (emergency)

        Returns:
            List of intents to execute in order
        """
        from almanak.framework.teardown import TeardownMode

        intents = []

        # Close perp position if we have one
        if self._has_position:
            # In emergency mode, accept higher slippage
            slippage = 0.03 if mode == TeardownMode.HARD else 0.01

            intents.append(
                Intent.perp_close(
                    market=self.market,
                    size_usd=self._position_size_usd,
                    collateral_token=self.collateral_token,
                    slippage=Decimal(str(slippage)),
                    protocol="gmx_v2",
                )
            )

            # Swap collateral to USDC
            intents.append(
                Intent.swap(
                    from_token=self.collateral_token,
                    to_token="USDC",
                    amount="all",  # Swap entire balance
                )
            )

        return intents

    def on_teardown_started(self, mode: "TeardownMode") -> None:
        """Called when teardown starts."""
        from almanak.framework.teardown import TeardownMode

        mode_name = "graceful" if mode == TeardownMode.SOFT else "emergency"
        logger.info(f"Teardown started in {mode_name} mode for GMX Perps strategy")

    def on_teardown_completed(self, success: bool, recovered_usd: Decimal) -> None:
        """Called when teardown completes."""
        if success:
            logger.info(f"Teardown completed. Recovered ${recovered_usd:,.2f}")
            self.reset_state()
        else:
            logger.error("Teardown failed - manual intervention may be required")

    def to_dict(self) -> dict[str, Any]:
        """Serialize strategy state to dictionary.

        Override parent to handle dict config (instead of dataclass).

        Returns:
            Dictionary representation of strategy state
        """
        metadata = self.get_metadata()

        # Handle both dict and dataclass config
        if isinstance(self.config, dict):
            config_dict = self.config
        elif hasattr(self.config, "to_dict"):
            config_dict = self.config.to_dict()
        else:
            config_dict = {}

        return {
            "strategy_name": self.__class__.STRATEGY_NAME,
            "chain": self.chain,
            "wallet_address": self.wallet_address,
            "config": config_dict,
            "config_version": self.get_current_config_version(),
            "current_intent": self._current_intent.serialize() if self._current_intent else None,
            "metadata": metadata.to_dict() if metadata else None,
        }


# =============================================================================
# MODULE TESTING
# =============================================================================

if __name__ == "__main__":
    print("GMXPerpsStrategy loaded successfully!")
    print(f"Metadata: {GMXPerpsStrategy.STRATEGY_METADATA}")
