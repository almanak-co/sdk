"""
RSI Reversion Strategy - Test Strategy for TA Test Suite

A simple RSI-based mean reversion strategy:
- Buys when RSI < oversold threshold (default 30)
- Sells when RSI > overbought threshold (default 70)
- Holds in neutral zone

Trades WETH/USDC on Arbitrum using Uniswap V3.
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
    name="test_rsi_reversion",
    description="RSI mean reversion strategy - buys oversold, sells overbought",
    version="1.0.0",
    author="Almanak Test Suite",
    tags=["test", "rsi", "mean-reversion", "uniswap"],
    supported_chains=["arbitrum"],
    supported_protocols=["uniswap_v3"],
    intent_types=["SWAP", "HOLD"],
)
class RSIReversionStrategy(IntentStrategy):
    """RSI-based mean reversion strategy for testing."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        def get_config(key: str, default: Any) -> Any:
            if isinstance(self.config, dict):
                return self.config.get(key, default)
            return getattr(self.config, key, default)

        self.trade_size_usd = Decimal(str(get_config("trade_size_usd", "5")))
        self.rsi_period = int(get_config("rsi_period", 14))
        self.rsi_oversold = Decimal(str(get_config("rsi_oversold", "30")))
        self.rsi_overbought = Decimal(str(get_config("rsi_overbought", "70")))
        self.max_slippage_bps = int(get_config("max_slippage_bps", 100))
        self.base_token = get_config("base_token", "WETH")
        self.quote_token = get_config("quote_token", "USDC")
        self.protocol = get_config("protocol", "uniswap_v3")
        self.force_action = get_config("force_action", None)

        logger.info(
            f"RSIReversionStrategy initialized: "
            f"trade_size=${self.trade_size_usd}, "
            f"RSI period={self.rsi_period}, "
            f"oversold={self.rsi_oversold}, "
            f"overbought={self.rsi_overbought}"
        )

    def decide(self, market: MarketSnapshot) -> Intent | None:
        """Make trading decision based on RSI."""
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

            # Get RSI indicator
            try:
                rsi = market.rsi(self.base_token, period=self.rsi_period)
                rsi_value = rsi.value
                logger.info(f"RSI({self.rsi_period}) = {rsi_value:.2f}")
            except ValueError as e:
                logger.warning(f"Could not get RSI: {e}")
                return Intent.hold(reason="RSI data unavailable")

            # Get balances
            try:
                quote_balance = market.balance(self.quote_token)
                base_balance = market.balance(self.base_token)
            except ValueError as e:
                logger.warning(f"Could not get balances: {e}")
                return Intent.hold(reason="Balance data unavailable")

            # Trading logic
            if rsi_value <= self.rsi_oversold:
                if quote_balance.balance_usd < self.trade_size_usd:
                    return Intent.hold(reason=f"Oversold (RSI={rsi_value:.1f}) but insufficient {self.quote_token}")
                logger.info(f"BUY SIGNAL: RSI={rsi_value:.2f} < {self.rsi_oversold} (oversold)")
                return Intent.swap(
                    from_token=self.quote_token,
                    to_token=self.base_token,
                    amount_usd=self.trade_size_usd,
                    max_slippage=Decimal(str(self.max_slippage_bps)) / Decimal("10000"),
                    protocol=self.protocol,
                )

            elif rsi_value >= self.rsi_overbought:
                min_base_to_sell = self.trade_size_usd / base_price
                if base_balance.balance < min_base_to_sell:
                    return Intent.hold(reason=f"Overbought (RSI={rsi_value:.1f}) but insufficient {self.base_token}")
                logger.info(f"SELL SIGNAL: RSI={rsi_value:.2f} > {self.rsi_overbought} (overbought)")
                return Intent.swap(
                    from_token=self.base_token,
                    to_token=self.quote_token,
                    amount_usd=self.trade_size_usd,
                    max_slippage=Decimal(str(self.max_slippage_bps)) / Decimal("10000"),
                    protocol=self.protocol,
                )

            else:
                return Intent.hold(
                    reason=f"RSI={rsi_value:.2f} in neutral zone [{self.rsi_oversold}-{self.rsi_overbought}]"
                )

        except Exception as e:
            logger.exception(f"Error in decide(): {e}")
            return Intent.hold(reason=f"Error: {e!s}")
