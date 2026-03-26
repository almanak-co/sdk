"""
===============================================================================
DEMO: Aerodrome RSI Strategy — Swap-Based RSI on Base
===============================================================================

RSI-based mean reversion strategy that executes swaps via Aerodrome on Base.
Buys WETH when oversold, sells when overbought. Designed as the vehicle for
PnL backtesting on Base chain with a non-Uniswap DEX.

USAGE:
------
    # PnL backtest (primary use case)
    almanak strat backtest pnl -d strategies/demo/aerodrome_rsi

    # Run on Anvil (single iteration)
    almanak strat run -d strategies/demo/aerodrome_rsi --network anvil --once

    # Dry run
    almanak strat run -d strategies/demo/aerodrome_rsi --once --dry-run
===============================================================================
"""

import logging
from datetime import UTC, datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Any

from almanak.framework.intents import Intent
from almanak.framework.strategies import (
    IntentStrategy,
    MarketSnapshot,
    almanak_strategy,
)
from almanak.framework.utils.log_formatters import format_usd

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from almanak.framework.teardown import TeardownMode, TeardownPositionSummary


@almanak_strategy(
    name="demo_aerodrome_rsi",
    description="RSI swap strategy on Base via Aerodrome — PnL backtest vehicle",
    version="1.0.0",
    author="Almanak",
    tags=["demo", "trading", "rsi", "mean-reversion", "aerodrome", "base", "backtesting"],
    supported_chains=["base"],
    supported_protocols=["aerodrome"],
    intent_types=["SWAP", "HOLD"],
    default_chain="base",
)
class AerodromeRSIStrategy(IntentStrategy):
    """RSI-based swap strategy using Aerodrome on Base.

    Configuration (config.json):
        trade_size_usd: Trade size in USD per signal (default: 3)
        rsi_period: RSI lookback period (default: 14)
        rsi_oversold: Buy threshold (default: 40)
        rsi_overbought: Sell threshold (default: 70)
        max_slippage_bps: Max slippage in basis points (default: 100)
        base_token: Token to trade (default: WETH)
        quote_token: Quote token (default: USDC)
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.trade_size_usd = Decimal(str(self.get_config("trade_size_usd", "3")))
        self.rsi_period = int(self.get_config("rsi_period", 14))
        self.rsi_oversold = Decimal(str(self.get_config("rsi_oversold", "40")))
        self.rsi_overbought = Decimal(str(self.get_config("rsi_overbought", "70")))
        self.max_slippage_bps = int(self.get_config("max_slippage_bps", 100))
        self.base_token = self.get_config("base_token", "WETH")
        self.quote_token = self.get_config("quote_token", "USDC")

        self._consecutive_holds = 0

        logger.info(
            f"AerodromeRSI initialized: trade_size=${self.trade_size_usd}, "
            f"RSI({self.rsi_period}) [{self.rsi_oversold},{self.rsi_overbought}], "
            f"pair={self.base_token}/{self.quote_token}"
        )

    def decide(self, market: MarketSnapshot) -> Intent | None:
        """RSI-based buy/sell/hold decision using Aerodrome swaps."""
        try:
            base_price = market.price(self.base_token)
        except ValueError as e:
            logger.warning(f"Price unavailable: {e}")
            return Intent.hold(reason="Price data unavailable")

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

        # OVERSOLD -> BUY
        if rsi.value <= self.rsi_oversold:
            if quote_balance.balance_usd < self.trade_size_usd:
                return Intent.hold(
                    reason=f"Oversold (RSI={rsi.value:.1f}) but insufficient "
                    f"{self.quote_token} ({format_usd(quote_balance.balance_usd)})"
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
                protocol="aerodrome",
            )

        # OVERBOUGHT -> SELL
        elif rsi.value >= self.rsi_overbought:
            min_base_to_sell = self.trade_size_usd / base_price
            if base_balance.balance < min_base_to_sell:
                return Intent.hold(
                    reason=f"Overbought (RSI={rsi.value:.1f}) but insufficient "
                    f"{self.base_token} ({base_balance.balance:.4f})"
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
                protocol="aerodrome",
            )

        # NEUTRAL -> HOLD
        else:
            self._consecutive_holds += 1
            return Intent.hold(
                reason=f"RSI={rsi.value:.2f} neutral [{self.rsi_oversold}-{self.rsi_overbought}] "
                f"(hold #{self._consecutive_holds})"
            )

    # =========================================================================
    # TEARDOWN
    # =========================================================================

    def supports_teardown(self) -> bool:
        return True

    def get_open_positions(self) -> "TeardownPositionSummary":
        from almanak.framework.teardown import PositionInfo, PositionType, TeardownPositionSummary

        positions: list[PositionInfo] = []

        # Query on-chain balance instead of unconditionally reporting a position
        try:
            market = self.create_market_snapshot()
            base_balance = market.balance(self.base_token)
            if base_balance.balance > 0:
                positions.append(
                    PositionInfo(
                        position_type=PositionType.TOKEN,
                        position_id="aerodrome_rsi_base_token",
                        chain=self.chain,
                        protocol="aerodrome",
                        value_usd=base_balance.balance_usd,
                        details={
                            "asset": self.base_token,
                            "balance": str(base_balance.balance),
                            "base_token": self.base_token,
                            "quote_token": self.quote_token,
                        },
                    )
                )
        except Exception:
            logger.warning("Failed to query balance for teardown; reporting no positions")

        return TeardownPositionSummary(
            strategy_id=getattr(self, "strategy_id", "demo_aerodrome_rsi"),
            timestamp=datetime.now(UTC),
            positions=positions,
        )

    def generate_teardown_intents(self, mode: "TeardownMode", market=None) -> list[Intent]:
        from almanak.framework.teardown import TeardownMode

        max_slippage = Decimal("0.03") if mode == TeardownMode.HARD else Decimal(str(self.max_slippage_bps)) / Decimal("10000")
        return [
            Intent.swap(
                from_token=self.base_token,
                to_token=self.quote_token,
                amount="all",
                max_slippage=max_slippage,
                protocol="aerodrome",
            )
        ]

    # =========================================================================
    # STATUS
    # =========================================================================

    def get_status(self) -> dict[str, Any]:
        return {
            "strategy": "demo_aerodrome_rsi",
            "chain": self.chain,
            "config": {
                "trade_size_usd": str(self.trade_size_usd),
                "rsi_period": self.rsi_period,
                "pair": f"{self.base_token}/{self.quote_token}",
            },
            "state": {"consecutive_holds": self._consecutive_holds},
        }
