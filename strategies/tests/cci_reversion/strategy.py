"""
CCI Reversion Strategy - Test Strategy for TA Test Suite

A Commodity Channel Index (CCI) mean reversion strategy:
- Buys when CCI falls below lower_level (-100 by default, oversold)
- Sells when CCI rises above upper_level (+100 by default, overbought)
- Holds when CCI is within normal range

CCI = (Typical Price - SMA) / (0.015 * Mean Deviation)
Typical Price = (High + Low + Close) / 3

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
    name="test_cci_reversion",
    description="CCI mean reversion strategy - buys at oversold, sells at overbought",
    version="1.0.0",
    author="Almanak Test Suite",
    tags=["test", "cci", "mean-reversion", "oscillator", "uniswap"],
    supported_chains=["arbitrum"],
    supported_protocols=["uniswap_v3"],
    intent_types=["SWAP", "HOLD"],
)
class CCIReversionStrategy(StatelessStrategy):
    """CCI-based mean reversion strategy for testing."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        def get_config(key: str, default: Any) -> Any:
            if isinstance(self.config, dict):
                return self.config.get(key, default)
            return getattr(self.config, key, default)

        self.trade_size_usd = Decimal(str(get_config("trade_size_usd", "5")))
        self.cci_period = int(get_config("cci_period", 20))
        self.cci_upper_level = float(get_config("cci_upper_level", 100))
        self.cci_lower_level = float(get_config("cci_lower_level", -100))
        self.max_slippage_bps = int(get_config("max_slippage_bps", 100))
        self.base_token = get_config("base_token", "ARB")
        self.quote_token = get_config("quote_token", "USDC")
        self.protocol = get_config("protocol", "uniswap_v3")
        self.force_action = get_config("force_action", None)

        logger.info(
            f"CCIReversionStrategy initialized: "
            f"trade_size=${self.trade_size_usd}, "
            f"CCI period={self.cci_period}, "
            f"levels=({self.cci_lower_level}, {self.cci_upper_level})"
        )

    def decide(self, market: MarketSnapshot) -> Intent | None:
        """Make trading decision based on CCI levels."""
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

            # Get CCI data from market snapshot
            try:
                cci = market.cci(self.base_token, period=self.cci_period)
            except ValueError as e:
                logger.warning(f"CCI data unavailable: {e}")
                return Intent.hold(reason="CCI data unavailable")

            cci_value = float(cci.value)

            logger.info(
                f"CCI({self.cci_period}): {cci_value:.2f} "
                f"(Lower: {self.cci_lower_level}, Upper: {self.cci_upper_level})"
            )

            # Get balances
            try:
                quote_balance = market.balance(self.quote_token)
                base_balance = market.balance(self.base_token)
            except ValueError as e:
                logger.warning(f"Could not get balances: {e}")
                return Intent.hold(reason="Balance data unavailable")

            # Trading logic: Mean reversion at extreme CCI levels
            # Oversold: CCI below lower level - buy opportunity
            if cci_value < self.cci_lower_level:
                if quote_balance.balance_usd < self.trade_size_usd:
                    return Intent.hold(
                        reason=f"Oversold (CCI < {self.cci_lower_level}) but insufficient {self.quote_token}"
                    )
                logger.info(f"BUY SIGNAL: CCI({cci_value:.2f}) < {self.cci_lower_level} - oversold")
                return Intent.swap(
                    from_token=self.quote_token,
                    to_token=self.base_token,
                    amount_usd=self.trade_size_usd,
                    max_slippage=Decimal(str(self.max_slippage_bps)) / Decimal("10000"),
                    protocol=self.protocol,
                )

            # Overbought: CCI above upper level - sell opportunity
            elif cci_value > self.cci_upper_level:
                min_base_to_sell = self.trade_size_usd / base_price
                if base_balance.balance < min_base_to_sell:
                    return Intent.hold(
                        reason=f"Overbought (CCI > {self.cci_upper_level}) but insufficient {self.base_token}"
                    )
                logger.info(f"SELL SIGNAL: CCI({cci_value:.2f}) > {self.cci_upper_level} - overbought")
                return Intent.swap(
                    from_token=self.base_token,
                    to_token=self.quote_token,
                    amount_usd=self.trade_size_usd,
                    max_slippage=Decimal(str(self.max_slippage_bps)) / Decimal("10000"),
                    protocol=self.protocol,
                )

            else:
                return Intent.hold(reason=f"CCI({cci_value:.2f}) within normal range")

        except Exception as e:
            logger.exception(f"Error in decide(): {e}")
            return Intent.hold(reason=f"Error: {e!s}")
