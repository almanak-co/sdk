"""
===============================================================================
TUTORIAL: Pendle Basics Strategy
===============================================================================

This is a demo strategy showing how to interact with Pendle Protocol,
a permissionless yield-trading protocol.

WHAT THIS STRATEGY DOES:
------------------------
1. Monitors wstETH balance and market conditions
2. When conditions are favorable: Swaps wstETH for PT (Principal Token)
3. PT can be held until maturity for guaranteed yield
4. Demonstrates the basic flow of yield tokenization

PENDLE CONCEPTS:
----------------
- SY (Standardized Yield): Wrapped yield-bearing tokens (e.g., wstETH -> SY-wstETH)
- PT (Principal Token): Represents the principal, redeemable 1:1 at maturity
- YT (Yield Token): Represents the yield until maturity
- Market: AMM pool for trading PT against SY

STRATEGY PATTERN:
-----------------
This strategy uses a simple decision flow:
1. Check if we have wstETH to invest
2. If yes, swap to PT for fixed yield exposure
3. Hold PT until maturity

USAGE:
------
    # Start gateway first
    almanak gateway --network anvil

    # Run the strategy (from repo root)
    almanak strat run -d strategies/demo/pendle_basics --once

===============================================================================
"""

import logging
from datetime import UTC
from decimal import Decimal
from typing import TYPE_CHECKING, Any

from almanak.framework.intents import Intent

if TYPE_CHECKING:
    from almanak.framework.teardown import TeardownMode, TeardownPositionSummary
from almanak.framework.strategies import (
    IntentStrategy,
    MarketSnapshot,
    almanak_strategy,
)
from almanak.framework.utils.log_formatters import format_usd

logger = logging.getLogger(__name__)


# Pendle market addresses by chain
PENDLE_MARKETS = {
    # Arbitrum markets (active)
    "PT-wstETH-25JUN2026": "0xf78452e0f5C0B95fc5dC8353B8CD1e06E53fa25B",
    # Arbitrum markets (expired)
    "PT-wstETH-26JUN2025": "0x08a152834de126d2ef83D612ff36e4523FD0017F",
    "PT-wstETH-26DEC2024": "0xf769035a247af48bf55BaA82d8b5e14E02E49A25",
    "PT-eETH-26DEC2024": "0x952083cde7aaa11AB8449057F7de23A970AA8472",
    # Plasma markets (expired 2026-02-26)
    "PT-fUSDT0-26FEB2026": "0x0cb289E9df2d0dCFe13732638C89655fb80C2bE2",
}


@almanak_strategy(
    name="demo_pendle_basics",
    description="Demo strategy for Pendle Protocol - shows PT trading basics",
    version="1.0.0",
    author="Almanak",
    tags=["demo", "tutorial", "pendle", "yield", "pt", "fixed-yield"],
    supported_chains=["arbitrum"],
    supported_protocols=["pendle"],
    intent_types=["SWAP", "HOLD"],
    default_chain="arbitrum",
)
class PendleBasicsStrategy(IntentStrategy):
    """
    A simple Pendle demonstration strategy.

    This strategy shows the basics of interacting with Pendle:
    - Checking balances
    - Swapping tokens to PT for fixed yield
    - Understanding the yield tokenization flow

    Configuration Parameters (from config.json):
    --------------------------------------------
    - market: Pendle market address to trade on
    - market_name: Human-readable market name
    - trade_size_token: Amount to trade in tokens (e.g., 0.001 for 0.001 wstETH)
    - trade_size_usd: Amount to trade in USD (used if trade_size_token not set)
    - max_slippage_bps: Maximum slippage in basis points
    - base_token: Token to trade (WSTETH - must be valid for market)
    - pt_token: PT token to receive

    Example Config:
    ---------------
    {
        "market": "0xf78452e0f5c0b95fc5dc8353b8cd1e06e53fa25b",
        "market_name": "PT-wstETH-25JUN2026",
        "trade_size_token": 0.001,
        "max_slippage_bps": 100,
        "base_token": "WSTETH",
        "pt_token": "PT-wstETH"
    }
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        """Initialize the strategy with configuration."""
        super().__init__(*args, **kwargs)

        # Market configuration
        self.market = self.get_config("market", PENDLE_MARKETS["PT-wstETH-25JUN2026"])
        self.market_name = self.get_config("market_name", "PT-wstETH-25JUN2026")

        # Trading parameters - support both token-based and USD-based amounts
        self.trade_size_token = self.get_config("trade_size_token", None)
        if self.trade_size_token is not None:
            self.trade_size_token = Decimal(str(self.trade_size_token))
        self.trade_size_usd = Decimal(str(self.get_config("trade_size_usd", "10")))
        self.max_slippage_bps = int(self.get_config("max_slippage_bps", 100))

        # Token configuration - support both symbols and addresses
        self.base_token = self.get_config("base_token", "WSTETH")
        self.base_token_symbol = self.get_config("base_token_symbol", self.base_token)
        self.base_token_decimals = int(self.get_config("base_token_decimals", 18))
        self.pt_token = self.get_config("pt_token", "PT-wstETH")
        self.pt_token_symbol = self.get_config("pt_token_symbol", self.pt_token)

        # Track state
        self._has_entered_position = False
        self._consecutive_holds = 0

        trade_size_display = (
            f"{self.trade_size_token} {self.base_token_symbol}"
            if self.trade_size_token
            else f"${self.trade_size_usd}"
        )
        logger.info(
            f"PendleBasicsStrategy initialized: "
            f"market={self.market_name}, "
            f"trade_size={trade_size_display}, "
            f"slippage={self.max_slippage_bps}bps"
        )

    def decide(self, market: MarketSnapshot) -> Intent | None:
        """
        Make a trading decision based on current market conditions.

        This is the core method that returns an Intent for execution.

        Args:
            market: MarketSnapshot with current prices and balances

        Returns:
            Intent: What action to take (SWAP or HOLD)
        """
        # =================================================================
        # STEP 1: Get current market data
        # =================================================================
        # For stablecoins (USDT0, USDC, etc), assume $1 if price not available
        stablecoins = {"USDT0", "USDC", "USDT", "DAI", "FUSDT0"}
        # Known stablecoin addresses (Plasma)
        stablecoin_addresses = {
            "0xb8ce59fc3717ada4c02eadf9682a9e934f625ebb",  # USDT0
            "0x1dd4b13fcae900c60a350589be8052959d2ed27b",  # fUSDT0
        }
        try:
            base_price = market.price(self.base_token)
            logger.debug(f"Current {self.base_token_symbol} price: ${base_price:,.2f}")
        except ValueError:
            is_stablecoin = (
                self.base_token_symbol.upper() in stablecoins
                or self.base_token.lower() in stablecoin_addresses
            )
            if is_stablecoin:
                base_price = 1.0
                logger.debug(f"Using $1.00 for stablecoin {self.base_token_symbol}")
            else:
                raise

        # =================================================================
        # STEP 2: Check balances
        # =================================================================
        try:
            base_balance = market.balance(self.base_token)
            logger.debug(
                f"Balance - {self.base_token}: {base_balance.balance:.4f} "
                f"(${base_balance.balance_usd:,.2f})"
            )
        except ValueError as e:
            logger.warning(f"Could not get balance: {e}")
            return Intent.hold(reason="Balance data unavailable")

        # =================================================================
        # STEP 3: Decision Logic
        # =================================================================

        # Check if we have enough balance to trade
        if self.trade_size_token:
            # Token-based check
            if base_balance.balance < self.trade_size_token:
                self._consecutive_holds += 1
                return Intent.hold(
                    reason=f"Insufficient {self.base_token} balance "
                    f"({base_balance.balance:.6f} < {self.trade_size_token})"
                )
        elif base_balance.balance_usd < self.trade_size_usd:
            self._consecutive_holds += 1
            return Intent.hold(
                reason=f"Insufficient {self.base_token} balance "
                f"(${base_balance.balance_usd:.2f} < ${self.trade_size_usd})"
            )

        # If we haven't entered a position yet, buy PT
        if not self._has_entered_position:
            if self.trade_size_token:
                logger.info(
                    f"Entering Pendle position: Swapping {self.trade_size_token} "
                    f"{self.base_token} for {self.pt_token}"
                )
            else:
                logger.info(
                    f"Entering Pendle position: Swapping {format_usd(self.trade_size_usd)} "
                    f"{self.base_token} for {self.pt_token}"
                )

            self._has_entered_position = True
            self._consecutive_holds = 0

            # Use the standard swap intent with Pendle protocol
            # The framework will route this through the Pendle adapter
            if self.trade_size_token:
                return Intent.swap(
                    from_token=self.base_token,
                    to_token=self.pt_token,
                    amount=self.trade_size_token,
                    max_slippage=Decimal(str(self.max_slippage_bps)) / Decimal("10000"),
                    protocol="pendle",
                )
            else:
                return Intent.swap(
                    from_token=self.base_token,
                    to_token=self.pt_token,
                    amount_usd=self.trade_size_usd,
                    max_slippage=Decimal(str(self.max_slippage_bps)) / Decimal("10000"),
                    protocol="pendle",
                )

        # Already in position, hold
        self._consecutive_holds += 1
        return Intent.hold(
            reason=f"Already holding {self.pt_token} position "
            f"(hold #{self._consecutive_holds})"
        )

    def _get_tracked_tokens(self) -> list[str]:
        """Get list of tokens to track for wallet balance.

        Override default to return only tokens used by this strategy.
        """
        return [self.base_token_symbol, self.pt_token_symbol]

    def get_status(self) -> dict[str, Any]:
        """Get current strategy status for monitoring."""
        return {
            "strategy": "demo_pendle_basics",
            "chain": self.chain,
            "wallet": self.wallet_address[:10] + "..." if self.wallet_address else "N/A",
            "config": {
                "market": self.market_name,
                "trade_size_usd": str(self.trade_size_usd),
                "max_slippage_bps": self.max_slippage_bps,
                "tokens": f"{self.base_token} -> {self.pt_token}",
            },
            "state": {
                "has_position": self._has_entered_position,
                "consecutive_holds": self._consecutive_holds,
            },
        }

    def get_open_positions(self) -> "TeardownPositionSummary":
        """Get summary of open positions for teardown."""
        from datetime import datetime

        from almanak.framework.teardown import (
            PositionInfo,
            PositionType,
            TeardownPositionSummary,
        )

        positions: list[PositionInfo] = []

        if self._has_entered_position:
            positions.append(
                PositionInfo(
                    position_type=PositionType.TOKEN,
                    position_id="pendle_pt_0",
                    chain=self.chain,
                    protocol="pendle",
                    value_usd=self.trade_size_usd,
                    details={
                        "market": self.market_name,
                        "pt_token": self.pt_token,
                        "base_token": self.base_token,
                    },
                )
            )

        return TeardownPositionSummary(
            strategy_id=getattr(self, "strategy_id", "demo_pendle_basics"),
            timestamp=datetime.now(UTC),
            positions=positions,
        )

    def generate_teardown_intents(self, mode: "TeardownMode", market=None) -> list[Intent]:
        """Generate intents to close all positions."""
        from almanak.framework.teardown import TeardownMode

        intents: list[Intent] = []

        if not self._has_entered_position:
            return intents

        # Determine slippage based on mode
        if mode == TeardownMode.HARD:
            max_slippage = Decimal("0.05")  # 5% for emergency
        else:
            max_slippage = Decimal(str(self.max_slippage_bps)) / Decimal("10000")

        logger.info(
            f"Generating teardown: swap {self.pt_token} -> {self.base_token} "
            f"(mode={mode.value}, slippage={max_slippage})"
        )

        # Swap PT back to base token
        intents.append(
            Intent.swap(
                from_token=self.pt_token,
                to_token=self.base_token,
                amount="all",
                max_slippage=max_slippage,
                protocol="pendle",
            )
        )

        return intents


# =============================================================================
# Entry Point
# =============================================================================

if __name__ == "__main__":
    print("=" * 60)
    print("PendleBasicsStrategy - Demo Strategy")
    print("=" * 60)
    print(f"\nStrategy Name: {PendleBasicsStrategy.STRATEGY_NAME}")
    print(f"Version: {PendleBasicsStrategy.STRATEGY_METADATA.version}")
    print(f"Supported Chains: {PendleBasicsStrategy.SUPPORTED_CHAINS}")
    print(f"Supported Protocols: {PendleBasicsStrategy.SUPPORTED_PROTOCOLS}")
    print(f"Intent Types: {PendleBasicsStrategy.INTENT_TYPES}")
    print(f"\nDescription: {PendleBasicsStrategy.STRATEGY_METADATA.description}")
    print("\nTo run this strategy:")
    print("  1. Start gateway: almanak gateway --network anvil")
    print("  2. Run strategy: almanak strat run -d strategies/demo/pendle_basics --once")
