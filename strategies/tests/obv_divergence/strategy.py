"""
OBV Divergence Strategy - Test Strategy for TA Test Suite

An On-Balance Volume (OBV) divergence strategy:
- Buys when OBV crosses above its signal line (bullish momentum)
- Sells when OBV crosses below its signal line (bearish momentum)
- Holds when no crossover detected

OBV = cumulative volume where:
- Add volume if close > previous close
- Subtract volume if close < previous close
Signal = SMA of OBV over signal_period

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
    name="test_obv_divergence",
    description="OBV divergence strategy - buys when OBV > signal, sells when OBV < signal",
    version="1.0.0",
    author="Almanak Test Suite",
    tags=["test", "obv", "volume", "divergence", "uniswap"],
    supported_chains=["arbitrum"],
    supported_protocols=["uniswap_v3"],
    intent_types=["SWAP", "HOLD"],
)
class OBVDivergenceStrategy(IntentStrategy):
    """OBV-based divergence strategy for testing."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        def get_config(key: str, default: Any) -> Any:
            if isinstance(self.config, dict):
                return self.config.get(key, default)
            return getattr(self.config, key, default)

        self.trade_size_usd = Decimal(str(get_config("trade_size_usd", "5")))
        self.obv_signal_period = int(get_config("obv_signal_period", 21))
        self.max_slippage_bps = int(get_config("max_slippage_bps", 100))
        self.base_token = get_config("base_token", "WETH")
        self.quote_token = get_config("quote_token", "USDC")
        self.protocol = get_config("protocol", "uniswap_v3")
        self.force_action = get_config("force_action", None)

        logger.info(
            f"OBVDivergenceStrategy initialized: "
            f"trade_size=${self.trade_size_usd}, "
            f"OBV signal_period={self.obv_signal_period}"
        )

    def decide(self, market: MarketSnapshot) -> Intent | None:
        """Make trading decision based on OBV divergence."""
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

            # Get OBV data from market snapshot
            try:
                obv_indicator = market.obv(self.base_token)
            except ValueError as e:
                logger.warning(f"OBV data unavailable: {e}")
                return Intent.hold(reason="OBV data unavailable")

            obv = obv_indicator.obv
            signal = obv_indicator.signal_line

            logger.info(f"OBV({self.obv_signal_period}): OBV={obv:,.0f}, Signal={signal:,.0f}")

            # Get balances
            try:
                quote_balance = market.balance(self.quote_token)
                base_balance = market.balance(self.base_token)
            except ValueError as e:
                logger.warning(f"Could not get balances: {e}")
                return Intent.hold(reason="Balance data unavailable")

            # Trading logic: OBV vs Signal line
            # Bullish: OBV above signal line indicates buying pressure
            if obv > signal:
                if quote_balance.balance_usd < self.trade_size_usd:
                    return Intent.hold(reason=f"Bullish (OBV > Signal) but insufficient {self.quote_token}")
                logger.info(f"BUY SIGNAL: OBV({obv:,.0f}) > Signal({signal:,.0f}) - bullish momentum")
                return Intent.swap(
                    from_token=self.quote_token,
                    to_token=self.base_token,
                    amount_usd=self.trade_size_usd,
                    max_slippage=Decimal(str(self.max_slippage_bps)) / Decimal("10000"),
                    protocol=self.protocol,
                )

            # Bearish: OBV below signal line indicates selling pressure
            elif obv < signal:
                min_base_to_sell = self.trade_size_usd / base_price
                if base_balance.balance < min_base_to_sell:
                    return Intent.hold(reason=f"Bearish (OBV < Signal) but insufficient {self.base_token}")
                logger.info(f"SELL SIGNAL: OBV({obv:,.0f}) < Signal({signal:,.0f}) - bearish momentum")
                return Intent.swap(
                    from_token=self.base_token,
                    to_token=self.quote_token,
                    amount_usd=self.trade_size_usd,
                    max_slippage=Decimal(str(self.max_slippage_bps)) / Decimal("10000"),
                    protocol=self.protocol,
                )

            else:
                return Intent.hold(reason="OBV at signal line - no clear signal")

        except Exception as e:
            logger.exception(f"Error in decide(): {e}")
            return Intent.hold(reason=f"Error: {e!s}")
