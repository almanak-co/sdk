"""Enso-Uniswap Arbitrage Demo Strategy (Tutorial).

===============================================================================
CROSS-PROTOCOL ARBITRAGE CONCEPT
===============================================================================

This strategy demonstrates how to use two different protocols in sequence:
1. BUY via Enso DEX aggregator (finds best route across DEXs)
2. SELL via Uniswap V3 directly

ARBITRAGE THEORY:
-----------------
DEX aggregators like Enso find optimal routes across multiple DEXs. Sometimes
the aggregated price is better than any single DEX price. This creates an
arbitrage opportunity:

1. If Enso finds a better BUY price than Uniswap: Buy via Enso, sell on Uniswap
2. If Uniswap has a better BUY price than Enso: Buy on Uniswap, sell via Enso

This demo shows pattern #1: Buy via aggregator, sell on single DEX.

REAL-WORLD CONSIDERATIONS:
--------------------------
In practice, this arbitrage is hard to profit from because:
- Gas costs eat into small spreads
- Price impact on both legs
- MEV bots front-run obvious arbitrage
- Non-atomic execution (two separate txs)

For educational purposes, we force the execution to demonstrate the pattern.

===============================================================================
INTENT SEQUENCE PATTERN
===============================================================================

This strategy uses Intent.sequence() to chain dependent actions:

    Intent.sequence([
        Intent.swap(..., protocol="enso"),      # Step 1: Buy via Enso
        Intent.swap(..., protocol="uniswap_v3"), # Step 2: Sell on Uniswap
    ])

The framework executes these in order, waiting for each to complete.
The amount="all" pattern can be used to pass output from step 1 to step 2.

===============================================================================
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
    name="demo_enso_uniswap_arbitrage",
    # DESCRIPTION of what this strategy does
    description="Tutorial: Cross-protocol arbitrage buying via Enso and selling via Uniswap V3",
    # VERSION tracking
    version="1.0.0",
    # AUTHOR
    author="Almanak",
    # TAGS for categorization
    tags=["demo", "tutorial", "arbitrage", "enso", "uniswap", "cross-protocol"],
    # SUPPORTED CHAINS - where this can run
    supported_chains=["arbitrum", "ethereum", "base", "optimism", "polygon"],
    # SUPPORTED PROTOCOLS - using both Enso and Uniswap
    supported_protocols=["enso", "uniswap_v3"],
    # INTENT TYPES this strategy may emit
    intent_types=["SWAP", "HOLD"],
)
class EnsoUniswapArbitrageStrategy(IntentStrategy):
    """Tutorial strategy demonstrating cross-protocol arbitrage.

    This strategy shows how to:
    1. Use Intent.sequence() for multi-step operations
    2. Combine different protocols in one trade
    3. Buy via DEX aggregator, sell on single DEX
    4. Handle forced actions for testing

    STRATEGY FLOW:
    1. Buy base token (WETH) with quote token (USDC) via Enso
    2. Sell base token (WETH) for quote token (USDC) via Uniswap V3
    3. Net result: Profit if Enso buy price < Uniswap sell price

    CONFIGURATION (from config.json):
        trade_size_usd (str): Amount to trade per arbitrage
        max_slippage_pct (float): Maximum slippage percentage
        base_token (str): Token to arbitrage (e.g., "WETH")
        quote_token (str): Quote token (e.g., "USDC")
        mode (str): "buy_enso_sell_uniswap" or "buy_uniswap_sell_enso"

    SEQUENCE PATTERN:
        This strategy returns an IntentSequence, not a single Intent.
        The framework handles:
        - Sequential execution (step 1 completes before step 2)
        - Amount chaining (use amount="all" to use output of previous step)
        - Error handling (sequence aborts on failure)
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

        config_dict = self.config if isinstance(self.config, dict) else {}

        # Handle DictConfigWrapper (from CLI)
        if hasattr(self.config, "get"):
            config_dict = {k: getattr(self.config, k) for k in dir(self.config) if not k.startswith("_")}

        # Trading parameters
        self.trade_size_usd = Decimal(str(config_dict.get("trade_size_usd", "100")))

        # Slippage (as percentage, e.g., 0.5 = 0.5%)
        self.max_slippage_pct = float(config_dict.get("max_slippage_pct", 0.5))

        # Token configuration
        self.base_token = config_dict.get("base_token", "WETH")
        self.quote_token = config_dict.get("quote_token", "USDC")

        # Arbitrage mode
        # "buy_enso_sell_uniswap": Buy via Enso, sell on Uniswap
        # "buy_uniswap_sell_enso": Buy on Uniswap, sell via Enso
        self.mode = config_dict.get("mode", "buy_enso_sell_uniswap")

        # Internal state
        self._arbitrages_executed = 0

        logger.info(
            f"EnsoUniswapArbitrageStrategy initialized: "
            f"trade_size=${self.trade_size_usd}, "
            f"slippage={self.max_slippage_pct}%, "
            f"pair={self.base_token}/{self.quote_token}, "
            f"mode={self.mode}"
        )

    # =========================================================================
    # MAIN DECISION LOGIC
    # =========================================================================

    def decide(self, market: MarketSnapshot) -> Intent | None:
        """Execute cross-protocol arbitrage.

        DECISION FLOW:
        1. Check market conditions (in real strategy, check for price diff)
        2. Create two-step sequence: buy on one protocol, sell on another
        3. Return sequence for framework to execute

        For this demo, we always execute the arbitrage when decide() is called.
        In production, you would:
        - Query prices from both protocols
        - Calculate expected profit after gas and slippage
        - Only execute if profitable

        Args:
            market: Current market snapshot with prices, balances

        Returns:
            IntentSequence with buy and sell intents
        """
        try:
            # =================================================================
            # ARBITRAGE EXECUTION
            # =================================================================
            # For demo purposes, we always execute the arbitrage
            # Real strategy would check for price differential first

            logger.info(
                f"Executing {self.mode} arbitrage: {self.quote_token} -> {self.base_token} -> {self.quote_token}"
            )

            if self.mode == "buy_enso_sell_uniswap":
                return self._create_buy_enso_sell_uniswap_sequence()
            elif self.mode == "buy_uniswap_sell_enso":
                return self._create_buy_uniswap_sell_enso_sequence()
            else:
                logger.warning(f"Unknown mode: {self.mode}")
                return Intent.hold(reason=f"Unknown mode: {self.mode}")

        except Exception as e:
            logger.exception(f"Error in decide(): {e}")
            return Intent.hold(reason=f"Error: {str(e)}")

    # =========================================================================
    # ARBITRAGE SEQUENCE CREATION
    # =========================================================================

    def _create_buy_enso_sell_uniswap_sequence(self) -> Intent:
        """Create arbitrage sequence: Buy via Enso, Sell on Uniswap.

        This is the primary arbitrage pattern:
        1. Enso finds optimal route to buy base token
        2. Sell on Uniswap V3 directly

        WHY THIS MIGHT BE PROFITABLE:
        - Enso may find a better price by splitting across DEXs
        - You sell at Uniswap's price, which might be higher

        Returns:
            IntentSequence with two swap intents
        """
        max_slippage = Decimal(str(self.max_slippage_pct)) / Decimal("100")

        logger.info(
            f"🔄 ARB SEQUENCE: Buy {format_usd(self.trade_size_usd)} {self.base_token} via Enso → Sell on Uniswap V3"
        )

        self._arbitrages_executed += 1

        # =====================================================================
        # BUILD THE SEQUENCE
        # =====================================================================
        # Intent.sequence() ensures these execute in order
        # The second swap waits for the first to complete

        return Intent.sequence(
            [
                # ---------------------------------------------------------
                # STEP 1: Buy base token via Enso aggregator
                # ---------------------------------------------------------
                # Enso finds the optimal route across all DEXs.
                # May split the order across Uniswap, SushiSwap, Camelot, etc.
                Intent.swap(
                    from_token=self.quote_token,  # USDC
                    to_token=self.base_token,  # WETH
                    amount_usd=self.trade_size_usd,  # e.g., $100
                    max_slippage=max_slippage,
                    protocol="enso",  # Buy via aggregator
                ),
                # ---------------------------------------------------------
                # STEP 2: Sell base token on Uniswap V3
                # ---------------------------------------------------------
                # Sell directly on Uniswap to close the arbitrage.
                # Use amount="all" to sell everything we just bought.
                #
                # NOTE: amount="all" means:
                # "Use the actual amount received from the previous step"
                # This accounts for slippage and fees from step 1.
                Intent.swap(
                    from_token=self.base_token,  # WETH
                    to_token=self.quote_token,  # USDC
                    amount="all",  # Sell everything from step 1
                    max_slippage=max_slippage,
                    protocol="uniswap_v3",  # Sell on Uniswap directly
                ),
            ],
            description=f"Enso->Uniswap arbitrage: {self.quote_token}->{self.base_token}->{self.quote_token}",
        )

    def _create_buy_uniswap_sell_enso_sequence(self) -> Intent:
        """Create arbitrage sequence: Buy on Uniswap, Sell via Enso.

        Alternative arbitrage pattern:
        1. Buy on Uniswap V3 directly
        2. Enso finds optimal route to sell base token

        WHY THIS MIGHT BE PROFITABLE:
        - Uniswap might have a better buy price for specific pairs
        - Enso may find a better sell route across DEXs

        Returns:
            IntentSequence with two swap intents
        """
        max_slippage = Decimal(str(self.max_slippage_pct)) / Decimal("100")

        logger.info(
            f"🔄 ARB SEQUENCE: Buy {format_usd(self.trade_size_usd)} {self.base_token} on Uniswap → Sell via Enso"
        )

        self._arbitrages_executed += 1

        return Intent.sequence(
            [
                # STEP 1: Buy on Uniswap V3
                Intent.swap(
                    from_token=self.quote_token,  # USDC
                    to_token=self.base_token,  # WETH
                    amount_usd=self.trade_size_usd,
                    max_slippage=max_slippage,
                    protocol="uniswap_v3",  # Buy on Uniswap
                ),
                # STEP 2: Sell via Enso
                Intent.swap(
                    from_token=self.base_token,  # WETH
                    to_token=self.quote_token,  # USDC
                    amount="all",  # Sell everything
                    max_slippage=max_slippage,
                    protocol="enso",  # Sell via aggregator
                ),
            ],
            description=f"Uniswap->Enso arbitrage: {self.quote_token}->{self.base_token}->{self.quote_token}",
        )

    # =========================================================================
    # STATUS AND MONITORING
    # =========================================================================

    def get_status(self) -> dict[str, Any]:
        """Get current strategy status for monitoring."""
        return {
            "strategy": "demo_enso_uniswap_arbitrage",
            "chain": self.chain,
            "wallet": self.wallet_address[:10] + "...",
            "config": {
                "trade_size_usd": str(self.trade_size_usd),
                "max_slippage_pct": self.max_slippage_pct,
                "base_token": self.base_token,
                "quote_token": self.quote_token,
                "mode": self.mode,
            },
            "state": {
                "arbitrages_executed": self._arbitrages_executed,
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

        Arbitrage strategies should end in quote token (stable).
        Teardown converts any intermediate holdings back to stable.

        Returns:
            True - this strategy can be safely torn down
        """
        return True

    def get_open_positions(self) -> "TeardownPositionSummary":
        """Get summary of open positions for teardown preview.

        For arbitrage strategies, positions are any intermediate token holdings.
        Since arbitrage is round-trip (USDC -> WETH -> USDC), we may have
        base token holdings if stopped mid-execution.

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

        # Track potential intermediate holdings
        # In production, would query actual balances
        estimated_value = self.trade_size_usd

        positions.append(
            PositionInfo(
                position_type=PositionType.TOKEN,
                position_id="enso_uniswap_arb_token_0",
                chain=self.chain,
                protocol="enso",  # Could be either protocol
                value_usd=estimated_value,
                details={
                    "asset": self.base_token,
                    "base_token": self.base_token,
                    "quote_token": self.quote_token,
                    "mode": self.mode,
                    "arbitrages_executed": self._arbitrages_executed,
                },
            )
        )

        return TeardownPositionSummary(
            strategy_id=getattr(self, "strategy_id", "demo_enso_uniswap_arbitrage"),
            timestamp=datetime.now(UTC),
            positions=positions,
        )

    def generate_teardown_intents(self, mode: "TeardownMode", market=None) -> list[Intent]:
        """Generate intents to close all positions.

        For arbitrage strategies, teardown means:
        - Convert any base token holdings back to quote token (stable)

        Since arbitrage is round-trip, we may be holding base token
        if execution was interrupted.

        Args:
            mode: TeardownMode (SOFT or HARD) - affects slippage tolerance

        Returns:
            List of SWAP intents to convert to stable
        """
        from almanak.framework.teardown import TeardownMode

        intents: list[Intent] = []

        # Determine slippage based on mode
        if mode == TeardownMode.HARD:
            max_slippage = Decimal("0.03")  # 3% for emergency
        else:
            max_slippage = Decimal(str(self.max_slippage_pct)) / Decimal("100")

        logger.info(
            f"Generating teardown intent: swap {self.base_token} -> "
            f"{self.quote_token} (mode={mode.value}, slippage={max_slippage})"
        )

        # Swap all base token back to quote token via Enso (best routing)
        intents.append(
            Intent.swap(
                from_token=self.base_token,
                to_token=self.quote_token,
                amount="all",
                max_slippage=max_slippage,
                protocol="enso",  # Use aggregator for best price
            )
        )

        return intents


# =============================================================================
# MODULE TESTING
# =============================================================================

if __name__ == "__main__":
    print("EnsoUniswapArbitrageStrategy loaded successfully!")
    print(f"Metadata: {EnsoUniswapArbitrageStrategy.STRATEGY_METADATA}")
