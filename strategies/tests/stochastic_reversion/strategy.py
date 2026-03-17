"""
Stochastic Reversion Strategy - Test Strategy for TA Test Suite

A Stochastic oscillator strategy with %K/%D crossovers:
- Buys when %K crosses above %D in oversold zone (below 20)
- Sells when %K crosses below %D in overbought zone (above 80)
- Holds when conditions aren't met

Trades ARB/USDC on Arbitrum using Uniswap V3.
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
    name="test_stochastic_reversion",
    description="Stochastic oscillator strategy - buys in oversold, sells in overbought",
    version="1.0.0",
    author="Almanak Test Suite",
    tags=["test", "stochastic", "mean-reversion", "momentum", "uniswap"],
    supported_chains=["arbitrum"],
    supported_protocols=["uniswap_v3"],
    intent_types=["SWAP", "HOLD"],
)
class StochasticReversionStrategy(StatelessStrategy):
    """Stochastic oscillator mean reversion strategy for testing."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        def get_config(key: str, default: Any) -> Any:
            if isinstance(self.config, dict):
                return self.config.get(key, default)
            return getattr(self.config, key, default)

        self.trade_size_usd = Decimal(str(get_config("trade_size_usd", "5")))
        self.stoch_fast_k = int(get_config("stoch_fast_k", 14))
        self.stoch_slow_k = int(get_config("stoch_slow_k", 3))
        self.stoch_slow_d = int(get_config("stoch_slow_d", 3))
        self.overbought = float(get_config("overbought", 80))
        self.oversold = float(get_config("oversold", 20))
        self.max_slippage_bps = int(get_config("max_slippage_bps", 100))
        self.base_token = get_config("base_token", "ARB")
        self.quote_token = get_config("quote_token", "USDC")
        self.protocol = get_config("protocol", "uniswap_v3")
        self.force_action = get_config("force_action", None)

        logger.info(
            f"StochasticReversionStrategy initialized: "
            f"trade_size=${self.trade_size_usd}, "
            f"fastK={self.stoch_fast_k}, slowK={self.stoch_slow_k}, slowD={self.stoch_slow_d}, "
            f"overbought={self.overbought}, oversold={self.oversold}"
        )

    def decide(self, market: MarketSnapshot) -> Intent | None:
        """Make trading decision based on Stochastic oscillator."""
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

            # Get Stochastic data from market snapshot
            try:
                stoch = market.stochastic(self.base_token, k_period=self.stoch_fast_k, d_period=self.stoch_slow_d)
            except ValueError as e:
                logger.warning(f"Stochastic data unavailable: {e}")
                return Intent.hold(reason="Stochastic data unavailable")

            percent_k = float(stoch.k_value)
            percent_d = float(stoch.d_value)

            logger.info(
                f"Stochastic({self.stoch_fast_k},{self.stoch_slow_k},{self.stoch_slow_d}): "
                f"%K={percent_k:.2f}, %D={percent_d:.2f}, "
                f"overbought={self.overbought}, oversold={self.oversold}"
            )

            # Get balances
            try:
                quote_balance = market.balance(self.quote_token)
                base_balance = market.balance(self.base_token)
            except ValueError as e:
                logger.warning(f"Could not get balances: {e}")
                return Intent.hold(reason="Balance data unavailable")

            # Trading logic: %K/%D relationship in oversold/overbought zones
            # Buy signal: %K above %D while in oversold zone (bullish momentum)
            k_above_d = percent_k > percent_d
            in_oversold = percent_k < self.oversold or percent_d < self.oversold

            if k_above_d and in_oversold:
                if quote_balance.balance_usd < self.trade_size_usd:
                    return Intent.hold(reason=f"Buy signal but insufficient {self.quote_token}")
                logger.info(f"BUY SIGNAL: %K({percent_k:.2f}) > %D({percent_d:.2f}) in oversold zone")
                return Intent.swap(
                    from_token=self.quote_token,
                    to_token=self.base_token,
                    amount_usd=self.trade_size_usd,
                    max_slippage=Decimal(str(self.max_slippage_bps)) / Decimal("10000"),
                    protocol=self.protocol,
                )

            # Sell signal: %K below %D while in overbought zone (bearish momentum)
            k_below_d = percent_k < percent_d
            in_overbought = percent_k > self.overbought or percent_d > self.overbought

            if k_below_d and in_overbought:
                min_base_to_sell = self.trade_size_usd / base_price
                if base_balance.balance < min_base_to_sell:
                    return Intent.hold(reason=f"Sell signal but insufficient {self.base_token}")
                logger.info(f"SELL SIGNAL: %K({percent_k:.2f}) < %D({percent_d:.2f}) in overbought zone")
                return Intent.swap(
                    from_token=self.base_token,
                    to_token=self.quote_token,
                    amount_usd=self.trade_size_usd,
                    max_slippage=Decimal(str(self.max_slippage_bps)) / Decimal("10000"),
                    protocol=self.protocol,
                )

            return Intent.hold(reason=f"%K={percent_k:.2f}, %D={percent_d:.2f} - no signal")

        except Exception as e:
            logger.exception(f"Error in decide(): {e}")
            return Intent.hold(reason=f"Error: {e!s}")
