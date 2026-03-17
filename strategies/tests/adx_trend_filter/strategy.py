"""
ADX Trend Filter Strategy - Test Strategy for TA Test Suite

An ADX-based trend strength strategy with directional indicators:
- Buys when ADX > threshold (strong trend) AND +DI > -DI (bullish direction)
- Sells when ADX > threshold (strong trend) AND -DI > +DI (bearish direction)
- Holds when ADX < threshold (weak/no trend)

Trades LINK/USDC on Arbitrum using Uniswap V3.
"""

import logging
from decimal import Decimal
from typing import Any

from almanak.framework.intents import Intent
from almanak.framework.strategies import (
    StatelessStrategy,
    MarketSnapshot,
    almanak_strategy,
)

logger = logging.getLogger(__name__)


@almanak_strategy(
    name="test_adx_trend_filter",
    description="ADX trend strength strategy - trades only in strong trends",
    version="1.0.0",
    author="Almanak Test Suite",
    tags=["test", "adx", "trend", "momentum", "uniswap"],
    supported_chains=["arbitrum"],
    supported_protocols=["uniswap_v3"],
    intent_types=["SWAP", "HOLD"],
)
class ADXTrendFilterStrategy(StatelessStrategy):
    """ADX trend strength filter strategy for testing."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        def get_config(key: str, default: Any) -> Any:
            if isinstance(self.config, dict):
                return self.config.get(key, default)
            return getattr(self.config, key, default)

        self.trade_size_usd = Decimal(str(get_config("trade_size_usd", "5")))
        self.adx_period = int(get_config("adx_period", 14))
        self.trend_threshold = float(get_config("trend_threshold", 25))
        self.max_slippage_bps = int(get_config("max_slippage_bps", 100))
        self.base_token = get_config("base_token", "LINK")
        self.quote_token = get_config("quote_token", "USDC")
        self.protocol = get_config("protocol", "uniswap_v3")
        self.force_action = get_config("force_action", None)

        logger.info(
            f"ADXTrendFilterStrategy initialized: "
            f"trade_size=${self.trade_size_usd}, "
            f"period={self.adx_period}, threshold={self.trend_threshold}"
        )

    def decide(self, market: MarketSnapshot) -> Intent | None:
        """Make trading decision based on ADX trend strength and direction."""
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

            # Get ADX data from market snapshot
            try:
                adx = market.adx(self.base_token, period=self.adx_period)
            except ValueError as e:
                logger.warning(f"ADX data unavailable: {e}")
                return Intent.hold(reason="ADX data unavailable")

            logger.info(
                f"ADX({self.adx_period}): ADX={adx.adx:.2f}, +DI={adx.plus_di:.2f}, -DI={adx.minus_di:.2f}, "
                f"threshold={self.trend_threshold}"
            )

            # Get balances
            try:
                quote_balance = market.balance(self.quote_token)
                base_balance = market.balance(self.base_token)
            except ValueError as e:
                logger.warning(f"Could not get balances: {e}")
                return Intent.hold(reason="Balance data unavailable")

            # Trading logic: Only trade in strong trends (ADX > threshold)
            # Buy when strong uptrend (+DI > -DI)
            # Sell when strong downtrend (-DI > +DI)

            if not adx.is_strong_trend:
                return Intent.hold(reason=f"ADX={adx.adx:.2f} below threshold ({self.trend_threshold}) - weak trend")

            # Strong trend detected
            if adx.is_uptrend:
                # Bullish direction - buy
                if quote_balance.balance_usd < self.trade_size_usd:
                    return Intent.hold(reason=f"Buy signal but insufficient {self.quote_token}")
                logger.info(f"BUY SIGNAL: ADX={adx.adx:.2f} (strong), +DI({adx.plus_di:.2f}) > -DI({adx.minus_di:.2f})")
                return Intent.swap(
                    from_token=self.quote_token,
                    to_token=self.base_token,
                    amount_usd=self.trade_size_usd,
                    max_slippage=Decimal(str(self.max_slippage_bps)) / Decimal("10000"),
                    protocol=self.protocol,
                )

            elif adx.is_downtrend:
                # Bearish direction - sell
                min_base_to_sell = self.trade_size_usd / base_price
                if base_balance.balance < min_base_to_sell:
                    return Intent.hold(reason=f"Sell signal but insufficient {self.base_token}")
                logger.info(
                    f"SELL SIGNAL: ADX={adx.adx:.2f} (strong), -DI({adx.minus_di:.2f}) > +DI({adx.plus_di:.2f})"
                )
                return Intent.swap(
                    from_token=self.base_token,
                    to_token=self.quote_token,
                    amount_usd=self.trade_size_usd,
                    max_slippage=Decimal(str(self.max_slippage_bps)) / Decimal("10000"),
                    protocol=self.protocol,
                )

            return Intent.hold(reason=f"ADX={adx.adx:.2f}, +DI={adx.plus_di:.2f}, -DI={adx.minus_di:.2f} - neutral")

        except Exception as e:
            logger.exception(f"Error in decide(): {e}")
            return Intent.hold(reason=f"Error: {e!s}")
