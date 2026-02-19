"""
Ichimoku Crossover Strategy - Test Strategy for TA Test Suite

An Ichimoku Cloud crossover strategy:
- Buys when Tenkan-sen crosses above Kijun-sen (bullish crossover)
- Sells when Tenkan-sen crosses below Kijun-sen (bearish crossover)
- Uses cloud (Senkou Span A/B) for trend confirmation
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
    name="test_ichimoku_crossover",
    description="Ichimoku Cloud crossover strategy - buys on Tenkan/Kijun bullish crossover",
    version="1.0.0",
    author="Almanak Test Suite",
    tags=["test", "ichimoku", "cloud", "crossover", "trend-following", "uniswap"],
    supported_chains=["arbitrum"],
    supported_protocols=["uniswap_v3"],
    intent_types=["SWAP", "HOLD"],
)
class IchimokuCrossoverStrategy(IntentStrategy):
    """Ichimoku Cloud crossover strategy for testing."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        def get_config(key: str, default: Any) -> Any:
            if isinstance(self.config, dict):
                return self.config.get(key, default)
            return getattr(self.config, key, default)

        self.trade_size_usd = Decimal(str(get_config("trade_size_usd", "5")))
        self.tenkan_period = int(get_config("tenkan_period", 9))
        self.kijun_period = int(get_config("kijun_period", 26))
        self.senkou_b_period = int(get_config("senkou_b_period", 52))
        self.max_slippage_bps = int(get_config("max_slippage_bps", 100))
        self.base_token = get_config("base_token", "LINK")
        self.quote_token = get_config("quote_token", "USDC")
        self.protocol = get_config("protocol", "uniswap_v3")
        self.force_action = get_config("force_action", None)

        logger.info(
            f"IchimokuCrossoverStrategy initialized: trade_size=${self.trade_size_usd}, Tenkan={self.tenkan_period}, Kijun={self.kijun_period}, Senkou B={self.senkou_b_period}"
        )

    def decide(self, market: MarketSnapshot) -> Intent | None:
        """Make trading decision based on Ichimoku crossover."""
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

            # Get Ichimoku data from market snapshot
            try:
                ichimoku = market.ichimoku(self.base_token)
            except ValueError as e:
                logger.warning(f"Ichimoku data unavailable: {e}")
                return Intent.hold(reason="Ichimoku data unavailable")

            tenkan = ichimoku.tenkan_sen
            kijun = ichimoku.kijun_sen
            senkou_a = ichimoku.senkou_span_a
            senkou_b = ichimoku.senkou_span_b

            logger.info(
                f"Ichimoku: Tenkan={tenkan:.6f}, Kijun={kijun:.6f}, Senkou A={senkou_a:.6f}, Senkou B={senkou_b:.6f}"
            )

            # Get balances
            try:
                quote_balance = market.balance(self.quote_token)
                base_balance = market.balance(self.base_token)
            except ValueError as e:
                logger.warning(f"Could not get balances: {e}")
                return Intent.hold(reason="Balance data unavailable")

            # Cloud confirmation: price above cloud = bullish, below = bearish
            above_cloud = ichimoku.is_above_cloud
            below_cloud = ichimoku.is_below_cloud

            if ichimoku.is_bullish_crossover:
                # Bullish Tenkan/Kijun crossover - BUY
                if quote_balance.balance_usd < self.trade_size_usd:
                    return Intent.hold(reason=f"Bullish crossover detected but insufficient {self.quote_token}")
                cloud_status = "above cloud" if above_cloud else ("in cloud" if not below_cloud else "below cloud")
                logger.info(f"BUY SIGNAL: Tenkan({tenkan:.4f}) > Kijun({kijun:.4f}), price {cloud_status}")
                return Intent.swap(
                    from_token=self.quote_token,
                    to_token=self.base_token,
                    amount_usd=self.trade_size_usd,
                    max_slippage=Decimal(str(self.max_slippage_bps)) / Decimal("10000"),
                    protocol=self.protocol,
                )

            elif ichimoku.is_bearish_crossover:
                # Bearish Tenkan/Kijun crossover - SELL
                min_base_to_sell = self.trade_size_usd / base_price
                if base_balance.balance < min_base_to_sell:
                    return Intent.hold(reason=f"Bearish crossover detected but insufficient {self.base_token}")
                cloud_status = "above cloud" if above_cloud else ("in cloud" if not below_cloud else "below cloud")
                logger.info(f"SELL SIGNAL: Tenkan({tenkan:.4f}) < Kijun({kijun:.4f}), price {cloud_status}")
                return Intent.swap(
                    from_token=self.base_token,
                    to_token=self.quote_token,
                    amount_usd=self.trade_size_usd,
                    max_slippage=Decimal(str(self.max_slippage_bps)) / Decimal("10000"),
                    protocol=self.protocol,
                )

            else:
                return Intent.hold(reason=f"No crossover: Tenkan={tenkan:.4f}, Kijun={kijun:.4f}")

        except Exception as e:
            logger.exception(f"Error in decide(): {e}")
            return Intent.hold(reason=f"Error: {e!s}")
