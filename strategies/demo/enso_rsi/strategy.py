"""Enso RSI Demo Strategy (Tutorial).

==============================================================================
WHAT IS ENSO?
==============================================================================

Enso is a DEX aggregator that finds the best swap route across multiple DEXs:

BENEFITS OVER DIRECT DEX EXECUTION:
1. Better prices via multi-DEX routing (Uniswap, SushiSwap, Camelot, etc.)
2. Automatic slippage protection with `safeRouteSingle`
3. Cross-chain swaps via bridge aggregation (Stargate, LayerZero)
4. Single API for all DeFi operations

HOW IT WORKS:
1. You specify: token in, token out, amount, slippage
2. Enso finds optimal route across all available DEXs
3. Returns ready-to-execute transaction calldata
4. Same interface whether single-DEX or multi-hop route

==============================================================================
THIS STRATEGY
==============================================================================

An RSI-based trading strategy that uses Enso for execution:
1. Monitors the RSI (Relative Strength Index) of a target token
2. When RSI < oversold threshold: Buys using Enso aggregator
3. When RSI > overbought threshold: Sells using Enso aggregator
4. Otherwise: Holds

The key difference from demo_uniswap_rsi is:
- Uses `protocol="enso"` in the swap intent
- Gets potentially better prices via multi-DEX routing
- Same strategy logic, different execution path

==============================================================================
RSI REMINDER
==============================================================================

RSI oscillates between 0 and 100:
- RSI < 30: "Oversold" - potential buy signal
- RSI > 70: "Overbought" - potential sell signal
- RSI 30-70: Neutral - hold

==============================================================================
"""

import logging
from datetime import UTC
from decimal import Decimal
from typing import Any

from almanak.framework.intents import Intent
from almanak.framework.strategies import IntentStrategy, MarketSnapshot, almanak_strategy

# Logging utilities for user-friendly output
from almanak.framework.utils.log_formatters import format_usd

# =============================================================================
# LOGGING SETUP
# =============================================================================

logger = logging.getLogger(__name__)


# =============================================================================
# STRATEGY REGISTRATION
# =============================================================================


@almanak_strategy(
    # UNIQUE NAME for CLI invocation
    name="demo_enso_rsi",
    # DESCRIPTION of what this strategy does
    description="Tutorial: RSI-based trading using Enso DEX aggregator",
    # VERSION tracking
    version="1.0.0",
    # AUTHOR
    author="Almanak",
    # TAGS for categorization
    tags=["demo", "tutorial", "rsi", "enso", "aggregator", "trading"],
    # SUPPORTED CHAINS - Enso supports multiple chains
    # https://api.enso.finance/api/v1/metadata/chains
    supported_chains=["arbitrum", "ethereum", "base", "optimism", "polygon"],
    # SUPPORTED PROTOCOLS - using Enso aggregator
    supported_protocols=["enso"],
    # INTENT TYPES this strategy may emit
    intent_types=["SWAP", "HOLD"],
)
class EnsoRSIStrategy(IntentStrategy):
    """Tutorial strategy demonstrating RSI trading via Enso aggregator.

    This strategy shows how to:
    1. Use RSI for entry/exit signals
    2. Execute swaps via Enso DEX aggregator
    3. Handle forced actions for testing
    4. Override to_dict for dict configs

    CONFIGURATION (from config.json):
        trade_size_usd (str): Amount to trade per signal
        rsi_oversold (int): RSI level that triggers buy
        rsi_overbought (int): RSI level that triggers sell
        max_slippage_pct (float): Maximum slippage percentage
        base_token (str): Token to trade (e.g., "WETH")
        quote_token (str): Quote token (e.g., "USDC")
        force_action (str): Force "buy" or "sell" for testing

    ENSO vs UNISWAP:
        The only difference from demo_uniswap_rsi is:
        `protocol="enso"` in the Intent.swap() call

        This routes the swap through Enso's aggregator instead of
        directly to Uniswap V3. Enso may find a better price by
        splitting across multiple DEXs.
    """

    # =========================================================================
    # INITIALIZATION
    # =========================================================================

    def __init__(self, *args, **kwargs):
        """Initialize the strategy with configuration."""
        super().__init__(*args, **kwargs)

        # =====================================================================
        # CONFIGURATION PARSING
        # =====================================================================

        # Trading parameters
        self.trade_size_usd = Decimal(str(self.get_config("trade_size_usd", "100")))

        # RSI thresholds
        self.rsi_oversold = int(self.get_config("rsi_oversold", 30))
        self.rsi_overbought = int(self.get_config("rsi_overbought", 70))

        # Slippage (as percentage, e.g., 0.5 = 0.5%)
        self.max_slippage_pct = float(self.get_config("max_slippage_pct", 0.5))

        # Token configuration
        self.base_token = self.get_config("base_token", "WETH")
        self.quote_token = self.get_config("quote_token", "USDC")

        # Force action for testing
        self.force_action = self.get_config("force_action", None)

        # Internal state
        self._trades_executed = 0

        logger.info(
            f"EnsoRSIStrategy initialized: "
            f"trade_size=${self.trade_size_usd}, "
            f"rsi_oversold={self.rsi_oversold}, "
            f"rsi_overbought={self.rsi_overbought}, "
            f"slippage={self.max_slippage_pct}%, "
            f"pair={self.base_token}/{self.quote_token}"
        )

    # =========================================================================
    # MAIN DECISION LOGIC
    # =========================================================================

    def decide(self, market: MarketSnapshot) -> Intent | None:
        """Make a trading decision based on RSI.

        DECISION FLOW:
        1. Check for forced action (for testing)
        2. Get current RSI for base token
        3. If RSI < oversold: BUY base token with quote token via Enso
        4. If RSI > overbought: SELL base token for quote token via Enso
        5. Otherwise: HOLD

        Args:
            market: Current market snapshot with prices, RSI, balances

        Returns:
            Intent to execute (SWAP via Enso or HOLD)
        """
        # =================================================================
        # STEP 1: HANDLE FORCED ACTIONS (FOR TESTING)
        # =================================================================
        if self.force_action:
            logger.info(f"Force action requested: {self.force_action}")

            if self.force_action == "buy":
                return self._create_buy_intent()
            elif self.force_action == "sell":
                return self._create_sell_intent()
            else:
                logger.warning(f"Unknown force_action: {self.force_action}")

        # =================================================================
        # STEP 2: GET RSI VALUE
        # =================================================================
        try:
            rsi_data = market.rsi(self.base_token)
            current_rsi = float(rsi_data.value)
            logger.debug(f"Current RSI for {self.base_token}: {current_rsi:.2f}")
        except ValueError:
            # RSI not available - use default for testing
            current_rsi = 50.0
            logger.warning(f"RSI unavailable for {self.base_token}, using {current_rsi}")

        # =================================================================
        # STEP 3: MAKE TRADING DECISION
        # =================================================================

        # OVERSOLD: RSI < threshold -> BUY
        if current_rsi < self.rsi_oversold:
            logger.info(
                f"📈 BUY SIGNAL: RSI={current_rsi:.2f} < {self.rsi_oversold} (oversold) "
                f"| Buying {format_usd(self.trade_size_usd)} of {self.base_token} via Enso"
            )
            return self._create_buy_intent()

        # OVERBOUGHT: RSI > threshold -> SELL
        elif current_rsi > self.rsi_overbought:
            logger.info(
                f"📉 SELL SIGNAL: RSI={current_rsi:.2f} > {self.rsi_overbought} (overbought) "
                f"| Selling {format_usd(self.trade_size_usd)} of {self.base_token} via Enso"
            )
            return self._create_sell_intent()

        # NEUTRAL: HOLD
        else:
            logger.debug(
                f"RSI {current_rsi:.2f} in neutral zone [{self.rsi_oversold}-{self.rsi_overbought}] -> HOLD"
            )
            return Intent.hold(reason=f"RSI {current_rsi:.2f} in neutral zone")

    # =========================================================================
    # INTENT CREATION METHODS
    # =========================================================================

    def _create_buy_intent(self) -> Intent:
        """Create a buy intent using Enso aggregator.

        BUY: Convert quote token (USDC) to base token (WETH)

        KEY DIFFERENCE FROM UNISWAP:
        - `protocol="enso"` routes through Enso aggregator
        - Enso may split across multiple DEXs for better price
        - Otherwise identical interface

        Returns:
            SwapIntent configured for Enso execution
        """
        # Convert slippage from percentage to decimal
        max_slippage = Decimal(str(self.max_slippage_pct)) / Decimal("100")

        logger.info(
            f"🔄 BUY via Enso: {format_usd(self.trade_size_usd)} {self.quote_token} → {self.base_token}, "
            f"slippage={self.max_slippage_pct}%"
        )

        self._trades_executed += 1

        # Create swap intent with Enso protocol
        # This is the key difference from uniswap_rsi
        return Intent.swap(
            from_token=self.quote_token,  # e.g., "USDC"
            to_token=self.base_token,  # e.g., "WETH"
            amount_usd=self.trade_size_usd,  # e.g., Decimal("100")
            max_slippage=max_slippage,  # e.g., Decimal("0.005")
            protocol="enso",  # USE ENSO AGGREGATOR
            # chain is set automatically from strategy config
        )

    def _create_sell_intent(self) -> Intent:
        """Create a sell intent using Enso aggregator.

        SELL: Convert base token (WETH) to quote token (USDC)

        Returns:
            SwapIntent configured for Enso execution
        """
        max_slippage = Decimal(str(self.max_slippage_pct)) / Decimal("100")

        logger.info(
            f"Creating SELL intent via Enso: {self.base_token} -> {self.quote_token}, slippage={self.max_slippage_pct}%"
        )

        self._trades_executed += 1

        return Intent.swap(
            from_token=self.base_token,  # e.g., "WETH"
            to_token=self.quote_token,  # e.g., "USDC"
            amount_usd=self.trade_size_usd,  # Trade this USD worth
            max_slippage=max_slippage,
            protocol="enso",  # USE ENSO AGGREGATOR
        )

    # =========================================================================
    # STATUS AND MONITORING
    # =========================================================================

    def get_status(self) -> dict[str, Any]:
        """Get current strategy status for monitoring."""
        return {
            "strategy": "demo_enso_rsi",
            "chain": self.chain,
            "wallet": self.wallet_address[:10] + "...",
            "config": {
                "trade_size_usd": str(self.trade_size_usd),
                "rsi_oversold": self.rsi_oversold,
                "rsi_overbought": self.rsi_overbought,
                "max_slippage_pct": self.max_slippage_pct,
                "base_token": self.base_token,
                "quote_token": self.quote_token,
            },
            "state": {
                "trades_executed": self._trades_executed,
            },
        }

    def to_dict(self) -> dict[str, Any]:
        """Serialize strategy state to dictionary.

        Override parent to handle dict config.
        """
        metadata = self.get_metadata()

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
        # Here we estimate based on trade size
        estimated_value = self.trade_size_usd

        positions.append(
            PositionInfo(
                position_type=PositionType.TOKEN,
                position_id="enso_rsi_token_0",
                chain=self.chain,
                protocol="enso",
                value_usd=estimated_value,
                details={
                    "asset": self.base_token,
                    "base_token": self.base_token,
                    "quote_token": self.quote_token,
                    "trades_executed": self._trades_executed,
                },
            )
        )

        return TeardownPositionSummary(
            strategy_id=getattr(self, "strategy_id", "demo_enso_rsi"),
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
            max_slippage = Decimal(str(self.max_slippage_pct)) / Decimal("100")

        logger.info(
            f"Generating teardown intent: swap {self.base_token} -> "
            f"{self.quote_token} (mode={mode.value}, slippage={max_slippage})"
        )

        # Swap all base token back to quote token
        # Using amount="all" to swap entire balance
        intents.append(
            Intent.swap(
                from_token=self.base_token,
                to_token=self.quote_token,
                amount="all",  # Swap entire balance
                max_slippage=max_slippage,
                protocol="enso",
            )
        )

        return intents


# =============================================================================
# MODULE TESTING
# =============================================================================

if __name__ == "__main__":
    print("EnsoRSIStrategy loaded successfully!")
    print(f"Metadata: {EnsoRSIStrategy.STRATEGY_METADATA}")
