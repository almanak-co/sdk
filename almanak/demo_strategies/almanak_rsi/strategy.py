"""
===============================================================================
ALMANAK RSI Strategy
===============================================================================

An RSI-based mean reversion strategy for trading ALMANAK/USDC on Uniswap V3
on the Base chain. Uses CoinGecko DEX (GeckoTerminal) OHLCV data for 15-minute
candles to calculate RSI.

WHAT THIS STRATEGY DOES:
------------------------
1. On first run: Buys ALMANAK with half of initial USDC capital (initialization)
2. Monitors RSI(14) using 15-minute candles from GeckoTerminal
3. When RSI < 30 (oversold): Buys ALMANAK with all available USDC
4. When RSI > 70 (overbought): Sells all ALMANAK for USDC
5. Enforces 1-hour cooldown between trades

TRADING PAIR:
-------------
- Base Token: ALMANAK (0xdefa1d21c5f1cbeac00eeb54b44c7d86467cc3a3)
- Quote Token: USDC (0x833589fcd6edb6e08f4c7c32d4f71b54bda02913)
- Pool: 0xbDbC38652D78AF0383322bBc823E06FA108d0874
- Fee Tier: 3000 (0.3%)
- Chain: Base

USAGE:
------
    # Run in dry-run mode
    almanak strat run -d strategies/demo/almanak_rsi --once --dry-run

    # Run on Anvil fork
    almanak strat run -d strategies/demo/almanak_rsi --network anvil --once

    # Run live (requires wallet config)
    almanak strat run -d strategies/demo/almanak_rsi --once

===============================================================================
"""

import logging
from datetime import UTC, datetime, timedelta
from decimal import ROUND_DOWN, Decimal
from typing import Any

from almanak.framework.intents import Intent
from almanak.framework.strategies import (
    IntentStrategy,
    MarketSnapshot,
    almanak_strategy,
)
from almanak.framework.utils.log_formatters import format_usd

logger = logging.getLogger(__name__)


# =============================================================================
# CONSTANTS
# =============================================================================

# Token addresses on Base
ALMANAK_ADDRESS = "0xdefa1d21c5f1cbeac00eeb54b44c7d86467cc3a3"
USDC_ADDRESS = "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913"
POOL_ADDRESS = "0xbDbC38652D78AF0383322bBc823E06FA108d0874"


# =============================================================================
# STRATEGY
# =============================================================================


@almanak_strategy(
    name="almanak_rsi",
    description="RSI mean reversion strategy for ALMANAK/USDC on Base (Uniswap V3)",
    version="1.0.0",
    author="Almanak",
    tags=["demo", "rsi", "mean-reversion", "uniswap", "base", "almanak"],
    supported_chains=["base"],
    supported_protocols=["uniswap_v3"],
    intent_types=["SWAP", "HOLD"],
)
class AlmanakRSIStrategy(IntentStrategy):
    """RSI-based mean reversion strategy for ALMANAK/USDC.

    This strategy demonstrates:
    - Trading a custom token by address
    - Using GeckoTerminal for DEX-native OHLCV data
    - Initialization phase (buying initial position)
    - Cooldown enforcement between trades
    - Comprehensive metrics tracking

    Configuration Parameters (from config.json):
    --------------------------------------------
    - initial_capital_usdc: Starting USDC capital (default: 20)
    - rsi_period: RSI calculation period (default: 14)
    - rsi_oversold: Buy signal threshold (default: 30)
    - rsi_overbought: Sell signal threshold (default: 70)
    - cooldown_hours: Hours between trades (default: 1)
    - max_slippage_pct: Max slippage as percentage (default: 1.0)
    """

    # =========================================================================
    # INITIALIZATION
    # =========================================================================

    def __init__(self, *args, **kwargs):
        """Initialize the strategy with configuration."""
        super().__init__(*args, **kwargs)

        # Helper to get config value
        def get_config(key: str, default: Any) -> Any:
            if isinstance(self.config, dict):
                return self.config.get(key, default)
            return getattr(self.config, key, default)

        # Token configuration
        self.base_token = get_config("base_token", "ALMANAK")
        self.base_token_address = get_config("base_token_address", ALMANAK_ADDRESS)
        self.quote_token = get_config("quote_token", "USDC")
        self.quote_token_address = get_config("quote_token_address", USDC_ADDRESS)
        self.pool_address = get_config("pool_address", POOL_ADDRESS)
        self.fee_tier = int(get_config("fee_tier", 3000))

        # RSI configuration
        self.rsi_period = int(get_config("rsi_period", 14))
        self.rsi_oversold = Decimal(str(get_config("rsi_oversold", 30)))
        self.rsi_overbought = Decimal(str(get_config("rsi_overbought", 70)))
        self.data_granularity = get_config("data_granularity", "15m")

        # Execution configuration
        self.initial_capital_usdc = Decimal(str(get_config("initial_capital_usdc", 20)))
        self.position_size_pct = int(get_config("position_size_pct", 100))
        self.cooldown_hours = int(get_config("cooldown_hours", 1))
        self.max_slippage_pct = Decimal(str(get_config("max_slippage_pct", 1.0)))

        # =====================================================================
        # State tracking
        # =====================================================================
        # Initialize from persistent state if available
        persistent = getattr(self, "persistent_state", {})

        self._initialized = persistent.get("initialized", False)
        self._last_trade_time: datetime | None = None
        last_trade_str = persistent.get("last_trade_time")
        if last_trade_str:
            try:
                self._last_trade_time = datetime.fromisoformat(last_trade_str)
            except (ValueError, TypeError):
                pass

        # Metrics tracking
        self._trade_count = persistent.get("trade_count", 0)
        self._initial_value_usd = Decimal(str(persistent.get("initial_value_usd", 0)))
        self._price_history: list[dict[str, Any]] = persistent.get("price_history", [])
        self._signal_history: list[dict[str, Any]] = persistent.get("signal_history", [])
        self._consecutive_holds = 0

        logger.info(
            f"AlmanakRSIStrategy initialized: "
            f"pair={self.base_token}/{self.quote_token}, "
            f"chain=base, pool={self.pool_address[:10]}..., "
            f"RSI period={self.rsi_period}, "
            f"oversold={self.rsi_oversold}, overbought={self.rsi_overbought}, "
            f"cooldown={self.cooldown_hours}h, "
            f"initial_capital=${self.initial_capital_usdc}"
        )

    # =========================================================================
    # MAIN DECISION LOGIC
    # =========================================================================

    def decide(self, market: MarketSnapshot) -> Intent | None:
        """Make a trading decision based on RSI.

        Decision flow:
        1. Check if strategy is initialized (initial buy done)
        2. If not initialized, buy half of capital in ALMANAK
        3. Get current RSI value
        4. Check cooldown period
        5. Execute trade based on RSI signal

        Parameters:
            market: MarketSnapshot with prices, RSI, balances

        Returns:
            Intent to execute (SWAP or HOLD)
        """
        try:
            # =================================================================
            # STEP 1: INITIALIZATION PHASE
            # =================================================================
            # On first run, buy ALMANAK for half of initial capital
            if not self._initialized:
                return self._handle_initialization(market)

            # =================================================================
            # STEP 2: GET CURRENT RSI
            # =================================================================
            try:
                # Try to get RSI for ALMANAK
                # Note: For new tokens, RSI might not be available via standard methods
                # We attempt to fetch it, but handle the case where it's unavailable
                rsi = market.rsi(self.base_token, period=self.rsi_period)
                current_rsi = rsi.value
                logger.debug(f"Current RSI({self.rsi_period}): {current_rsi:.2f}")
            except ValueError as e:
                logger.warning(f"Could not get RSI for {self.base_token}: {e}")
                return Intent.hold(reason=f"RSI data unavailable: {e}")

            # =================================================================
            # STEP 3: RECORD PRICE AND RSI FOR CHARTING
            # =================================================================
            self._record_price_data(market, current_rsi)

            # =================================================================
            # STEP 4: CHECK COOLDOWN
            # =================================================================
            if not self._can_trade():
                cooldown_remaining = self._get_cooldown_remaining()
                self._consecutive_holds += 1
                return Intent.hold(
                    reason=f"Cooldown active ({cooldown_remaining:.0f}m remaining), "
                    f"RSI={current_rsi:.2f} (hold #{self._consecutive_holds})"
                )

            # =================================================================
            # STEP 5: GET BALANCES
            # =================================================================
            try:
                quote_balance = market.balance(self.quote_token)
                base_balance = market.balance(self.base_token)
                logger.debug(
                    f"Balances - {self.quote_token}: ${quote_balance.balance_usd:.2f}, "
                    f"{self.base_token}: {base_balance.balance}"
                )
            except ValueError as e:
                logger.warning(f"Could not get balances: {e}")
                return Intent.hold(reason=f"Balance data unavailable: {e}")

            # =================================================================
            # STEP 6: TRADING DECISION
            # =================================================================

            # OVERSOLD: RSI < 30 -> BUY ALMANAK
            if current_rsi <= self.rsi_oversold:
                # Check if we have USDC to buy with
                if quote_balance.balance <= Decimal("0.01"):
                    return Intent.hold(
                        reason=f"Oversold (RSI={current_rsi:.1f}) but no {self.quote_token} to buy with"
                    )

                logger.info(
                    f"BUY SIGNAL: RSI={current_rsi:.2f} < {self.rsi_oversold} (oversold) | "
                    f"Buying {self.base_token} with all {self.quote_token}"
                )

                self._record_signal("BUY", current_rsi)
                self._consecutive_holds = 0

                return Intent.swap(
                    from_token=self.quote_token_address,
                    to_token=self.base_token_address,
                    amount="all",
                    max_slippage=self.max_slippage_pct / Decimal("100"),
                    protocol="uniswap_v3",
                )

            # OVERBOUGHT: RSI > 70 -> SELL ALMANAK
            elif current_rsi >= self.rsi_overbought:
                # Check if we have ALMANAK to sell (dust threshold)
                if base_balance.balance <= Decimal("0.0001"):
                    return Intent.hold(
                        reason=f"Overbought (RSI={current_rsi:.1f}) but no {self.base_token} to sell"
                    )

                logger.info(
                    f"SELL SIGNAL: RSI={current_rsi:.2f} > {self.rsi_overbought} (overbought) | "
                    f"Selling all {self.base_token} for {self.quote_token}"
                )

                self._record_signal("SELL", current_rsi)
                self._consecutive_holds = 0

                return Intent.swap(
                    from_token=self.base_token_address,
                    to_token=self.quote_token_address,
                    amount="all",
                    max_slippage=self.max_slippage_pct / Decimal("100"),
                    protocol="uniswap_v3",
                )

            # NEUTRAL: HOLD
            else:
                self._consecutive_holds += 1
                return Intent.hold(
                    reason=f"RSI={current_rsi:.2f} in neutral zone "
                    f"[{self.rsi_oversold}-{self.rsi_overbought}] "
                    f"(hold #{self._consecutive_holds})"
                )

        except Exception as e:
            logger.exception(f"Error in decide(): {e}")
            return Intent.hold(reason=f"Error: {str(e)}")

    # =========================================================================
    # INITIALIZATION HANDLER
    # =========================================================================

    def _handle_initialization(self, market: MarketSnapshot) -> Intent:
        """Handle first-run initialization: buy initial position.

        Buys ALMANAK for exactly half of initial USDC capital, regardless
        of market conditions.

        Args:
            market: MarketSnapshot for balance checks

        Returns:
            SwapIntent to buy initial ALMANAK position
        """
        # Populate price cache so IntentCompiler can calculate slippage protection.
        # Since the compiler fails closed on missing prices, we must HOLD if prices
        # are unavailable -- otherwise compilation will always fail.
        try:
            market.price(self.quote_token)
            market.price(self.base_token)
        except ValueError as e:
            logger.warning(f"Could not pre-populate price data for initialization: {e}")
            return Intent.hold(reason=f"Price data unavailable for init swap: {e}")

        # Round down to USDC precision (6 decimals) to avoid overspending
        initial_buy_amount = (self.initial_capital_usdc / Decimal("2")).quantize(
            Decimal("0.000001"), rounding=ROUND_DOWN
        )

        logger.info(
            f"INITIALIZATION: First run - buying {self.base_token} "
            f"for {format_usd(initial_buy_amount)} (half of initial capital)"
        )

        # Record initial value (state mutation happens in on_intent_executed)
        self._initial_value_usd = self.initial_capital_usdc

        return Intent.swap(
            from_token=self.quote_token_address,
            to_token=self.base_token_address,
            amount=initial_buy_amount,
            max_slippage=self.max_slippage_pct / Decimal("100"),
            protocol="uniswap_v3",
        )

    # =========================================================================
    # COOLDOWN MANAGEMENT
    # =========================================================================

    def _can_trade(self) -> bool:
        """Check if cooldown period has passed since last trade."""
        if self._last_trade_time is None:
            return True

        cooldown = timedelta(hours=self.cooldown_hours)
        now = datetime.now(UTC)

        # Handle timezone-naive datetime
        last_trade = self._last_trade_time
        if last_trade.tzinfo is None:
            last_trade = last_trade.replace(tzinfo=UTC)

        return now >= last_trade + cooldown

    def _get_cooldown_remaining(self) -> float:
        """Get remaining cooldown time in minutes."""
        if self._last_trade_time is None:
            return 0.0

        cooldown = timedelta(hours=self.cooldown_hours)
        now = datetime.now(UTC)

        last_trade = self._last_trade_time
        if last_trade.tzinfo is None:
            last_trade = last_trade.replace(tzinfo=UTC)

        end_time = last_trade + cooldown
        if now >= end_time:
            return 0.0

        remaining = end_time - now
        return remaining.total_seconds() / 60.0

    # =========================================================================
    # METRICS AND HISTORY
    # =========================================================================

    def _record_price_data(self, market: MarketSnapshot, rsi_value: Decimal) -> None:
        """Record price and RSI for charting."""
        try:
            price = market.price(self.base_token)
            self._price_history.append({
                "timestamp": datetime.now(UTC).isoformat(),
                "price": float(price),
                "rsi": float(rsi_value),
            })
            # Keep last 1000 data points
            if len(self._price_history) > 1000:
                self._price_history = self._price_history[-1000:]
        except Exception:
            pass  # Price data recording is best-effort

    def _record_signal(self, signal_type: str, rsi_value: Decimal) -> None:
        """Record a buy/sell signal for charting."""
        try:
            self._signal_history.append({
                "timestamp": datetime.now(UTC).isoformat(),
                "signal": signal_type,
                "rsi": float(rsi_value),
            })
            # Keep last 100 signals
            if len(self._signal_history) > 100:
                self._signal_history = self._signal_history[-100:]
        except Exception:
            pass

    def _save_state(self) -> None:
        """Save current state to persistent storage."""
        self.persistent_state.update({
            "initialized": self._initialized,
            "last_trade_time": self._last_trade_time.isoformat() if self._last_trade_time else None,
            "trade_count": self._trade_count,
            "initial_value_usd": str(self._initial_value_usd),
            "price_history": self._price_history,
            "signal_history": self._signal_history,
        })

    # =========================================================================
    # LIFECYCLE CALLBACKS
    # =========================================================================

    def on_intent_executed(self, intent: Intent, success: bool, result: Any) -> None:
        """Called after an intent is executed.

        Updates state tracking after successful trades. Also handles
        initialization: marks the strategy as initialized only after the
        first swap succeeds, so a failed init swap will be retried.
        """
        if success:
            if not self._initialized:
                self._initialized = True
                logger.info("Initialization swap succeeded - strategy is now initialized")
            self._last_trade_time = datetime.now(UTC)
            self._trade_count += 1
            self._save_state()
            logger.info(f"Trade executed successfully (total trades: {self._trade_count})")

    # =========================================================================
    # STATUS REPORTING
    # =========================================================================

    def get_status(self) -> dict[str, Any]:
        """Get current strategy status for monitoring/dashboards."""
        return {
            "strategy": "almanak_rsi",
            "chain": "base",
            "wallet": self.wallet_address[:10] + "..." if self.wallet_address else "N/A",
            "config": {
                "pair": f"{self.base_token}/{self.quote_token}",
                "pool": self.pool_address,
                "rsi_period": self.rsi_period,
                "rsi_oversold": str(self.rsi_oversold),
                "rsi_overbought": str(self.rsi_overbought),
                "cooldown_hours": self.cooldown_hours,
                "initial_capital_usdc": str(self.initial_capital_usdc),
            },
            "state": {
                "initialized": self._initialized,
                "trade_count": self._trade_count,
                "last_trade_time": self._last_trade_time.isoformat() if self._last_trade_time else None,
                "can_trade": self._can_trade(),
                "cooldown_remaining_min": self._get_cooldown_remaining(),
                "consecutive_holds": self._consecutive_holds,
            },
            "metrics": {
                "price_history_count": len(self._price_history),
                "signal_history_count": len(self._signal_history),
            },
        }

    # =========================================================================
    # TEARDOWN SUPPORT
    # =========================================================================

    def supports_teardown(self) -> bool:
        """Indicate this strategy supports safe teardown."""
        return True

    def get_open_positions(self):
        """Get summary of open positions for teardown preview."""
        from almanak.framework.teardown import (
            PositionInfo,
            PositionType,
            TeardownPositionSummary,
        )

        positions: list[PositionInfo] = []

        # Token position
        positions.append(
            PositionInfo(
                position_type=PositionType.TOKEN,
                position_id="almanak_rsi_token_0",
                chain="base",
                protocol="uniswap_v3",
                value_usd=self.initial_capital_usdc / Decimal("2"),  # Estimate
                details={
                    "asset": self.base_token,
                    "base_token": self.base_token,
                    "quote_token": self.quote_token,
                    "pool": self.pool_address,
                    "trade_count": self._trade_count,
                },
            )
        )

        return TeardownPositionSummary(
            strategy_id=getattr(self, "strategy_id", "almanak_rsi"),
            timestamp=datetime.now(UTC),
            positions=positions,
        )

    def generate_teardown_intents(self, mode) -> list[Intent]:
        """Generate intents to close all positions.

        Sells all ALMANAK back to USDC.

        Args:
            mode: TeardownMode (SOFT or HARD) - affects slippage tolerance

        Returns:
            List of SWAP intents to convert to USDC
        """
        from almanak.framework.teardown import TeardownMode

        intents: list[Intent] = []

        # Determine slippage based on mode
        if mode == TeardownMode.HARD:
            max_slippage = Decimal("0.05")  # 5% for emergency exit
        else:
            max_slippage = self.max_slippage_pct / Decimal("100")

        logger.info(
            f"Generating teardown: swap all {self.base_token} -> {self.quote_token} "
            f"(mode={mode.value}, slippage={max_slippage})"
        )

        intents.append(
            Intent.swap(
                from_token=self.base_token_address,
                to_token=self.quote_token_address,
                amount="all",
                max_slippage=max_slippage,
                protocol="uniswap_v3",
            )
        )

        return intents


# =============================================================================
# TESTING
# =============================================================================

if __name__ == "__main__":
    metadata = AlmanakRSIStrategy.STRATEGY_METADATA
    print("=" * 60)
    print("ALMANAK RSI Strategy")
    print("=" * 60)
    print(f"\nStrategy Name: {AlmanakRSIStrategy.STRATEGY_NAME}")
    print(f"Version: {metadata.version}")
    print(f"Supported Chains: {metadata.supported_chains}")
    print(f"Supported Protocols: {metadata.supported_protocols}")
    print(f"\nDescription: {metadata.description}")
    print("\nTo run this strategy:")
    print("  almanak strat run -d strategies/demo/almanak_rsi --once --dry-run")
