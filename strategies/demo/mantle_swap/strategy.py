"""Mantle Swap Demo Strategy.

A simple RSI-based swap strategy running on Mantle (L2). Demonstrates how to
build and run a strategy on the Mantle chain using Uniswap V3.

What this strategy does:
    1. Monitors RSI of WETH on Mantle
    2. RSI < oversold  -> buys WETH with USDT via Uniswap V3
    3. RSI > overbought -> sells WETH for USDT via Uniswap V3
    4. Otherwise -> holds

Mantle specifics:
    - Native gas token is MNT (not ETH)
    - WETH is bridged at 0xdEAddEaDdeadDEadDEADDEAddEADDEAddead1111
    - USDT is the primary stablecoin with deep liquidity
    - Low gas fees (OP Stack L2)

Usage:
    almanak strat run -d strategies/demo/mantle_swap --network anvil --once
"""

import logging
from datetime import UTC
from decimal import Decimal
from typing import Any

from almanak.framework.intents import Intent
from almanak.framework.strategies import IntentStrategy, MarketSnapshot, almanak_strategy
from almanak.framework.utils.log_formatters import format_usd

logger = logging.getLogger(__name__)


@almanak_strategy(
    name="demo_mantle_swap",
    description="Simple RSI swap strategy on Mantle L2 via Uniswap V3",
    version="1.0.0",
    author="Almanak",
    tags=["demo", "mantle", "swap", "rsi", "uniswap"],
    supported_chains=["mantle"],
    supported_protocols=["uniswap_v3"],
    intent_types=["SWAP", "HOLD"],
)
class MantleSwapStrategy(IntentStrategy):
    """RSI mean-reversion swap strategy on Mantle.

    Configuration (config.json):
        trade_size_usd: Amount per trade in USD (default: 5)
        rsi_period: RSI lookback period (default: 14)
        rsi_oversold: Buy threshold (default: 35)
        rsi_overbought: Sell threshold (default: 65)
        max_slippage_bps: Max slippage in basis points (default: 100)
        base_token: Token to trade (default: WETH)
        quote_token: Stablecoin (default: USDT)
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.trade_size_usd = Decimal(str(self.get_config("trade_size_usd", "5")))
        self.rsi_period = int(self.get_config("rsi_period", 14))
        self.rsi_oversold = Decimal(str(self.get_config("rsi_oversold", "35")))
        self.rsi_overbought = Decimal(str(self.get_config("rsi_overbought", "65")))
        self.max_slippage_bps = int(self.get_config("max_slippage_bps", 100))
        self.base_token = self.get_config("base_token", "WETH")
        self.quote_token = self.get_config("quote_token", "USDT")

        self._consecutive_holds = 0

        logger.info(
            f"MantleSwapStrategy initialized: "
            f"trade_size=${self.trade_size_usd}, "
            f"RSI({self.rsi_period}) oversold={self.rsi_oversold}/overbought={self.rsi_overbought}, "
            f"pair={self.base_token}/{self.quote_token} on Mantle"
        )

    def decide(self, market: MarketSnapshot) -> Intent | None:
        try:
            base_price = market.price(self.base_token)

            try:
                rsi = market.rsi(self.base_token, period=self.rsi_period)
            except ValueError as e:
                logger.warning(f"RSI unavailable: {e}")
                return Intent.hold(reason="RSI data unavailable")

            try:
                quote_balance = market.balance(self.quote_token)
                base_balance = market.balance(self.base_token)
            except ValueError as e:
                logger.warning(f"Balance unavailable: {e}")
                return Intent.hold(reason="Balance data unavailable")

            max_slippage = Decimal(str(self.max_slippage_bps)) / Decimal("10000")

            # BUY: RSI oversold
            if rsi.value <= self.rsi_oversold:
                if quote_balance.balance_usd < self.trade_size_usd:
                    return Intent.hold(
                        reason=f"Oversold RSI={rsi.value:.1f} but low {self.quote_token} balance"
                    )

                logger.info(
                    f"BUY: RSI={rsi.value:.2f} < {self.rsi_oversold} | "
                    f"Buying {format_usd(self.trade_size_usd)} of {self.base_token}"
                )
                self._consecutive_holds = 0

                return Intent.swap(
                    from_token=self.quote_token,
                    to_token=self.base_token,
                    amount_usd=self.trade_size_usd,
                    max_slippage=max_slippage,
                    protocol="uniswap_v3",
                )

            # SELL: RSI overbought
            elif rsi.value >= self.rsi_overbought:
                min_base = self.trade_size_usd / base_price
                if base_balance.balance < min_base:
                    return Intent.hold(
                        reason=f"Overbought RSI={rsi.value:.1f} but low {self.base_token} balance"
                    )

                logger.info(
                    f"SELL: RSI={rsi.value:.2f} > {self.rsi_overbought} | "
                    f"Selling {format_usd(self.trade_size_usd)} of {self.base_token}"
                )
                self._consecutive_holds = 0

                return Intent.swap(
                    from_token=self.base_token,
                    to_token=self.quote_token,
                    amount_usd=self.trade_size_usd,
                    max_slippage=max_slippage,
                    protocol="uniswap_v3",
                )

            # HOLD: neutral zone
            else:
                self._consecutive_holds += 1
                return Intent.hold(
                    reason=f"RSI={rsi.value:.2f} neutral [{self.rsi_oversold}-{self.rsi_overbought}] "
                    f"(hold #{self._consecutive_holds})"
                )

        except Exception as e:
            logger.exception(f"Error in decide(): {e}")
            return Intent.hold(reason=f"Error: {e}")

    # -- Teardown --

    def get_open_positions(self):
        from datetime import datetime

        from almanak.framework.teardown import PositionInfo, PositionType, TeardownPositionSummary

        return TeardownPositionSummary(
            strategy_id=getattr(self, "strategy_id", "demo_mantle_swap"),
            timestamp=datetime.now(UTC),
            positions=[
                PositionInfo(
                    position_type=PositionType.TOKEN,
                    position_id="mantle_swap_token_0",
                    chain=self.chain,
                    protocol="uniswap_v3",
                    value_usd=self.trade_size_usd,
                    details={"asset": self.base_token},
                )
            ],
        )

    def generate_teardown_intents(self, mode, market=None):
        from almanak.framework.teardown import TeardownMode

        max_slippage = (
            Decimal("0.03")
            if mode == TeardownMode.HARD
            else Decimal(str(self.max_slippage_bps)) / Decimal("10000")
        )
        return [
            Intent.swap(
                from_token=self.base_token,
                to_token=self.quote_token,
                amount="all",
                max_slippage=max_slippage,
                protocol="uniswap_v3",
            )
        ]

    def get_status(self) -> dict[str, Any]:
        return {
            "strategy": "demo_mantle_swap",
            "chain": self.chain,
            "pair": f"{self.base_token}/{self.quote_token}",
            "consecutive_holds": self._consecutive_holds,
        }
