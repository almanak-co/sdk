"""
MACD Crossover Strategy - Test Strategy for TA Test Suite

A MACD-based trend following strategy:
- Buys when MACD line crosses above signal line (bullish crossover)
- Sells when MACD line crosses below signal line (bearish crossover)
- Holds when no crossover detected

Trades ARB/USDC on Arbitrum using Uniswap V3.
"""

import logging
from decimal import Decimal
from typing import Any

from almanak.framework.intents import Intent
from almanak.framework.strategies import (
    IntentStrategy,
    MarketSnapshot,
    almanak_strategy,
)

logger = logging.getLogger(__name__)


@almanak_strategy(
    name="test_macd_crossover",
    description="MACD crossover strategy - buys on bullish crossover, sells on bearish",
    version="1.0.0",
    author="Almanak Test Suite",
    tags=["test", "macd", "trend-following", "uniswap"],
    supported_chains=["arbitrum"],
    supported_protocols=["uniswap_v3"],
    intent_types=["SWAP", "HOLD"],
)
class MACDCrossoverStrategy(IntentStrategy):
    """MACD-based trend following strategy for testing."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        def get_config(key: str, default: Any) -> Any:
            if isinstance(self.config, dict):
                return self.config.get(key, default)
            return getattr(self.config, key, default)

        self.trade_size_usd = Decimal(str(get_config("trade_size_usd", "5")))
        self.macd_fast = int(get_config("macd_fast", 12))
        self.macd_slow = int(get_config("macd_slow", 26))
        self.macd_signal = int(get_config("macd_signal", 9))
        self.max_slippage_bps = int(get_config("max_slippage_bps", 100))
        self.base_token = get_config("base_token", "ARB")
        self.quote_token = get_config("quote_token", "USDC")
        self.protocol = get_config("protocol", "uniswap_v3")
        self.force_action = get_config("force_action", None)

        logger.info(
            f"MACDCrossoverStrategy initialized: "
            f"trade_size=${self.trade_size_usd}, "
            f"MACD fast={self.macd_fast}, slow={self.macd_slow}, signal={self.macd_signal}"
        )

    def decide(self, market: MarketSnapshot) -> Intent | None:
        """Make trading decision based on MACD crossover."""
        try:
            base_price = market.price(self.base_token)

            # Check for force_action override (for testing)
            if self.force_action == "buy":
                logger.info(f"FORCE BUY: Buying ${self.trade_size_usd} of {self.base_token}")
                return Intent.swap(
                    from_token=self.quote_token,
                    to_token=self.base_token,
                    amount_usd=self.trade_size_usd,
                    max_slippage=Decimal(str(self.max_slippage_bps)) / Decimal("10000"),
                    protocol=self.protocol,
                )
            elif self.force_action == "sell":
                logger.info(f"FORCE SELL: Selling ${self.trade_size_usd} of {self.base_token}")
                return Intent.swap(
                    from_token=self.base_token,
                    to_token=self.quote_token,
                    amount_usd=self.trade_size_usd,
                    max_slippage=Decimal(str(self.max_slippage_bps)) / Decimal("10000"),
                    protocol=self.protocol,
                )

            # Get MACD data from market snapshot
            try:
                macd = market.macd(
                    self.base_token,
                    fast_period=self.macd_fast,
                    slow_period=self.macd_slow,
                    signal_period=self.macd_signal,
                )
            except ValueError as e:
                logger.warning(f"MACD data unavailable: {e}")
                return Intent.hold(reason="MACD data unavailable")

            logger.info(
                f"MACD({self.macd_fast},{self.macd_slow},{self.macd_signal}): "
                f"MACD={macd.macd_line:.6f}, Signal={macd.signal_line:.6f}, "
                f"Histogram={macd.histogram:.6f}"
            )

            # Get balances
            try:
                quote_balance = market.balance(self.quote_token)
                base_balance = market.balance(self.base_token)
            except ValueError as e:
                logger.warning(f"Could not get balances: {e}")
                return Intent.hold(reason="Balance data unavailable")

            # Trading logic: MACD crossover
            # Bullish crossover: MACD crosses above signal (histogram turns positive)
            if macd.is_bullish_crossover:
                if quote_balance.balance_usd < self.trade_size_usd:
                    return Intent.hold(
                        reason=f"Bullish crossover (histogram={macd.histogram:.4f}) but insufficient {self.quote_token}"
                    )
                logger.info(
                    f"BUY SIGNAL: MACD({macd.macd_line:.4f}) > Signal({macd.signal_line:.4f}) - bullish crossover"
                )
                return Intent.swap(
                    from_token=self.quote_token,
                    to_token=self.base_token,
                    amount_usd=self.trade_size_usd,
                    max_slippage=Decimal(str(self.max_slippage_bps)) / Decimal("10000"),
                    protocol=self.protocol,
                )

            # Bearish crossover: MACD crosses below signal (histogram turns negative)
            elif macd.is_bearish_crossover:
                min_base_to_sell = self.trade_size_usd / base_price
                if base_balance.balance < min_base_to_sell:
                    return Intent.hold(
                        reason=f"Bearish crossover (histogram={macd.histogram:.4f}) but insufficient {self.base_token}"
                    )
                logger.info(
                    f"SELL SIGNAL: MACD({macd.macd_line:.4f}) < Signal({macd.signal_line:.4f}) - bearish crossover"
                )
                return Intent.swap(
                    from_token=self.base_token,
                    to_token=self.quote_token,
                    amount_usd=self.trade_size_usd,
                    max_slippage=Decimal(str(self.max_slippage_bps)) / Decimal("10000"),
                    protocol=self.protocol,
                )

            else:
                return Intent.hold(reason="MACD histogram at zero - no clear signal")

        except Exception as e:
            logger.exception(f"Error in decide(): {e}")
            return Intent.hold(reason=f"Error: {e!s}")
