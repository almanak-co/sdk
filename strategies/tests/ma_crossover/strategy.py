"""
MA Crossover Strategy - Test Strategy for TA Test Suite

A Simple Moving Average crossover strategy:
- Buys when short MA crosses above long MA (Golden Cross)
- Sells when short MA crosses below long MA (Death Cross)
- Holds when no crossover detected

Trades LINK/USDC on Arbitrum using Uniswap V3.
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
    name="test_ma_crossover",
    description="MA crossover strategy - buys on golden cross, sells on death cross",
    version="1.0.0",
    author="Almanak Test Suite",
    tags=["test", "ma", "sma", "crossover", "trend-following", "uniswap"],
    supported_chains=["arbitrum"],
    supported_protocols=["uniswap_v3"],
    intent_types=["SWAP", "HOLD"],
)
class MACrossoverStrategy(IntentStrategy):
    """Moving Average crossover strategy for testing."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        def get_config(key: str, default: Any) -> Any:
            if isinstance(self.config, dict):
                return self.config.get(key, default)
            return getattr(self.config, key, default)

        self.trade_size_usd = Decimal(str(get_config("trade_size_usd", "5")))
        self.sma_short = int(get_config("sma_short", 9))
        self.sma_long = int(get_config("sma_long", 21))
        self.max_slippage_bps = int(get_config("max_slippage_bps", 100))
        self.base_token = get_config("base_token", "LINK")
        self.quote_token = get_config("quote_token", "USDC")
        self.protocol = get_config("protocol", "uniswap_v3")
        self.force_action = get_config("force_action", None)

        logger.info(
            f"MACrossoverStrategy initialized: trade_size=${self.trade_size_usd}, SMA short={self.sma_short}, long={self.sma_long}"
        )

    def decide(self, market: MarketSnapshot) -> Intent | None:
        """Make trading decision based on MA crossover."""
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

            # Get MA data from market snapshot using the new API
            try:
                short_ma_data = market.sma(self.base_token, period=self.sma_short)
                long_ma_data = market.sma(self.base_token, period=self.sma_long)
            except ValueError as e:
                logger.warning(f"MA data unavailable: {e}")
                return Intent.hold(reason="MA data unavailable")

            short_ma = float(short_ma_data.value)
            long_ma = float(long_ma_data.value)

            # Get previous MA values (stored separately with prev_ prefix periods)
            try:
                prev_short_ma_data = market.sma(
                    self.base_token, period=self.sma_short + 1000
                )  # Using offset period as workaround
                prev_long_ma_data = market.sma(self.base_token, period=self.sma_long + 1000)
                prev_short_ma = float(prev_short_ma_data.value)
                prev_long_ma = float(prev_long_ma_data.value)
            except ValueError:
                # If previous values not available, use current values (no crossover detected)
                prev_short_ma = short_ma
                prev_long_ma = long_ma

            logger.info(
                f"MA({self.sma_short},{self.sma_long}): Short MA={short_ma:.6f}, Long MA={long_ma:.6f}, Prev Short={prev_short_ma:.6f}, Prev Long={prev_long_ma:.6f}"
            )

            # Get balances
            try:
                quote_balance = market.balance(self.quote_token)
                base_balance = market.balance(self.base_token)
            except ValueError as e:
                logger.warning(f"Could not get balances: {e}")
                return Intent.hold(reason="Balance data unavailable")

            # Trading logic: MA crossover
            # Golden Cross: Short MA crosses above Long MA
            was_below = prev_short_ma < prev_long_ma
            is_above = short_ma > long_ma

            # Death Cross: Short MA crosses below Long MA
            was_above = prev_short_ma > prev_long_ma
            is_below = short_ma < long_ma

            if was_below and is_above:
                # Golden Cross - BUY
                if quote_balance.balance_usd < self.trade_size_usd:
                    return Intent.hold(reason=f"Golden Cross detected but insufficient {self.quote_token}")
                logger.info(f"BUY SIGNAL: Golden Cross - Short MA({short_ma:.4f}) > Long MA({long_ma:.4f})")
                return Intent.swap(
                    from_token=self.quote_token,
                    to_token=self.base_token,
                    amount_usd=self.trade_size_usd,
                    max_slippage=Decimal(str(self.max_slippage_bps)) / Decimal("10000"),
                    protocol=self.protocol,
                )

            elif was_above and is_below:
                # Death Cross - SELL
                min_base_to_sell = self.trade_size_usd / base_price
                if base_balance.balance < min_base_to_sell:
                    return Intent.hold(reason=f"Death Cross detected but insufficient {self.base_token}")
                logger.info(f"SELL SIGNAL: Death Cross - Short MA({short_ma:.4f}) < Long MA({long_ma:.4f})")
                return Intent.swap(
                    from_token=self.base_token,
                    to_token=self.quote_token,
                    amount_usd=self.trade_size_usd,
                    max_slippage=Decimal(str(self.max_slippage_bps)) / Decimal("10000"),
                    protocol=self.protocol,
                )

            else:
                return Intent.hold(reason=f"No crossover: Short MA={short_ma:.4f}, Long MA={long_ma:.4f}")

        except Exception as e:
            logger.exception(f"Error in decide(): {e}")
            return Intent.hold(reason=f"Error: {e!s}")
