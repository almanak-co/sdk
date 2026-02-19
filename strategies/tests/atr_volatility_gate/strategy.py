"""
ATR Volatility Gate Strategy - Test Strategy for TA Test Suite

An ATR-based volatility filter strategy:
- Only trades when ATR (as percentage of price) is below threshold
- High volatility = stay out (risk management)
- Low volatility = safe to trade
- Uses ATR period=14 and max_volatility_threshold=5% by default

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
    name="test_atr_volatility_gate",
    description="ATR volatility filter strategy - trades only when volatility is below threshold",
    version="1.0.0",
    author="Almanak Test Suite",
    tags=["test", "atr", "volatility", "risk-management", "uniswap"],
    supported_chains=["arbitrum"],
    supported_protocols=["uniswap_v3"],
    intent_types=["SWAP", "HOLD"],
)
class ATRVolatilityGateStrategy(IntentStrategy):
    """ATR-based volatility gate strategy for testing."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        def get_config(key: str, default: Any) -> Any:
            if isinstance(self.config, dict):
                return self.config.get(key, default)
            return getattr(self.config, key, default)

        self.trade_size_usd = Decimal(str(get_config("trade_size_usd", "5")))
        self.atr_period = int(get_config("atr_period", 14))
        self.max_volatility_threshold = Decimal(str(get_config("max_volatility_threshold", "5")))
        self.max_slippage_bps = int(get_config("max_slippage_bps", 100))
        self.base_token = get_config("base_token", "WETH")
        self.quote_token = get_config("quote_token", "USDC")
        self.protocol = get_config("protocol", "uniswap_v3")
        self.force_action = get_config("force_action", None)

        logger.info(
            f"ATRVolatilityGateStrategy initialized: trade_size=${self.trade_size_usd}, ATR period={self.atr_period}, max_volatility={self.max_volatility_threshold}%"
        )

    def decide(self, market: MarketSnapshot) -> Intent | None:
        """Make trading decision based on ATR volatility gate."""
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

            # Get ATR data from market snapshot
            try:
                atr = market.atr(self.base_token, period=self.atr_period)
            except ValueError as e:
                logger.warning(f"ATR data unavailable: {e}")
                return Intent.hold(reason="ATR data unavailable")

            logger.info(f"ATR({self.atr_period}): {atr.value:.2f}, ATR%: {atr.value_percent:.2f}%")

            # Get balances
            try:
                quote_balance = market.balance(self.quote_token)
                base_balance = market.balance(self.base_token)
            except ValueError as e:
                logger.warning(f"Could not get balances: {e}")
                return Intent.hold(reason="Balance data unavailable")

            # Trading logic: Only trade when volatility is below threshold
            if atr.is_high_volatility:
                logger.info(
                    f"VOLATILITY TOO HIGH: ATR%={atr.value_percent:.2f}% > {self.max_volatility_threshold}% threshold"
                )
                return Intent.hold(
                    reason=f"Volatility too high: ATR%={atr.value_percent:.2f}% > {self.max_volatility_threshold}%"
                )

            # Volatility is acceptable - default to buying if we have quote balance
            if quote_balance.balance_usd >= self.trade_size_usd:
                logger.info(
                    f"LOW VOLATILITY BUY: ATR%={atr.value_percent:.2f}% < {self.max_volatility_threshold}% threshold"
                )
                return Intent.swap(
                    from_token=self.quote_token,
                    to_token=self.base_token,
                    amount_usd=self.trade_size_usd,
                    max_slippage=Decimal(str(self.max_slippage_bps)) / Decimal("10000"),
                    protocol=self.protocol,
                )

            # No quote balance, try to sell base if we have it
            min_base_to_sell = self.trade_size_usd / base_price
            if base_balance.balance >= min_base_to_sell:
                logger.info(
                    f"LOW VOLATILITY SELL: ATR%={atr.value_percent:.2f}% < {self.max_volatility_threshold}% threshold"
                )
                return Intent.swap(
                    from_token=self.base_token,
                    to_token=self.quote_token,
                    amount_usd=self.trade_size_usd,
                    max_slippage=Decimal(str(self.max_slippage_bps)) / Decimal("10000"),
                    protocol=self.protocol,
                )

            return Intent.hold(
                reason=f"Low volatility (ATR%={atr.value_percent:.2f}%) but insufficient balance to trade"
            )

        except Exception as e:
            logger.exception(f"Error in decide(): {e}")
            return Intent.hold(reason=f"Error: {e!s}")
