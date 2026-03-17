"""
Bollinger Bands Reversion Strategy - Test Strategy for TA Test Suite

A Bollinger Bands mean reversion strategy:
- Buys when price touches or goes below lower band (oversold)
- Sells when price touches or goes above upper band (overbought)
- Holds when price is within the bands

Trades WETH/USDC on Arbitrum using Uniswap V3.
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
    name="test_bollinger_reversion",
    description="Bollinger Bands mean reversion strategy - buys at lower band, sells at upper band",
    version="1.0.0",
    author="Almanak Test Suite",
    tags=["test", "bollinger", "mean-reversion", "volatility", "uniswap"],
    supported_chains=["arbitrum"],
    supported_protocols=["uniswap_v3"],
    intent_types=["SWAP", "HOLD"],
)
class BollingerReversionStrategy(StatelessStrategy):
    """Bollinger Bands mean reversion strategy for testing."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        def get_config(key: str, default: Any) -> Any:
            if isinstance(self.config, dict):
                return self.config.get(key, default)
            return getattr(self.config, key, default)

        self.trade_size_usd = Decimal(str(get_config("trade_size_usd", "5")))
        self.bb_period = int(get_config("bb_period", 20))
        self.bb_std_dev = float(get_config("bb_std_dev", 2.0))
        self.max_slippage_bps = int(get_config("max_slippage_bps", 100))
        self.base_token = get_config("base_token", "WETH")
        self.quote_token = get_config("quote_token", "USDC")
        self.protocol = get_config("protocol", "uniswap_v3")
        self.force_action = get_config("force_action", None)

        logger.info(
            f"BollingerReversionStrategy initialized: "
            f"trade_size=${self.trade_size_usd}, "
            f"BB period={self.bb_period}, "
            f"std_dev={self.bb_std_dev}"
        )

    def decide(self, market: MarketSnapshot) -> Intent | None:
        """Make trading decision based on Bollinger Bands."""
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

            # Get Bollinger Bands data from market snapshot
            try:
                bb = market.bollinger_bands(self.base_token, period=self.bb_period, std_dev=self.bb_std_dev)
            except ValueError as e:
                logger.warning(f"Bollinger Bands data unavailable: {e}")
                return Intent.hold(reason="Bollinger Bands data unavailable")

            current_price = float(base_price)

            logger.info(
                f"Bollinger Bands({self.bb_period}, {self.bb_std_dev}): "
                f"Upper={bb.upper_band:.2f}, Middle={bb.middle_band:.2f}, "
                f"Lower={bb.lower_band:.2f}, Price={current_price:.2f}"
            )

            # Get balances
            try:
                quote_balance = market.balance(self.quote_token)
                base_balance = market.balance(self.base_token)
            except ValueError as e:
                logger.warning(f"Could not get balances: {e}")
                return Intent.hold(reason="Balance data unavailable")

            # Trading logic: Mean reversion at bands
            # Buy when price at or below lower band (oversold)
            if bb.is_oversold or current_price <= float(bb.lower_band):
                if quote_balance.balance_usd < self.trade_size_usd:
                    return Intent.hold(reason=f"Price at lower band but insufficient {self.quote_token}")
                logger.info(f"BUY SIGNAL: Price({current_price:.2f}) <= Lower Band({bb.lower_band:.2f})")
                return Intent.swap(
                    from_token=self.quote_token,
                    to_token=self.base_token,
                    amount_usd=self.trade_size_usd,
                    max_slippage=Decimal(str(self.max_slippage_bps)) / Decimal("10000"),
                    protocol=self.protocol,
                )

            # Sell when price at or above upper band (overbought)
            elif bb.is_overbought or current_price >= float(bb.upper_band):
                min_base_to_sell = self.trade_size_usd / base_price
                if base_balance.balance < min_base_to_sell:
                    return Intent.hold(reason=f"Price at upper band but insufficient {self.base_token}")
                logger.info(f"SELL SIGNAL: Price({current_price:.2f}) >= Upper Band({bb.upper_band:.2f})")
                return Intent.swap(
                    from_token=self.base_token,
                    to_token=self.quote_token,
                    amount_usd=self.trade_size_usd,
                    max_slippage=Decimal(str(self.max_slippage_bps)) / Decimal("10000"),
                    protocol=self.protocol,
                )

            else:
                return Intent.hold(
                    reason=f"Price({current_price:.2f}) within bands [{bb.lower_band:.2f}-{bb.upper_band:.2f}]"
                )

        except Exception as e:
            logger.exception(f"Error in decide(): {e}")
            return Intent.hold(reason=f"Error: {e!s}")
