"""Uniswap V3 RSI Swap Strategy — Parameter Sweep Demo on Arbitrum.

A sweep-optimized variant of the uniswap_rsi demo for parameter sweep
backtesting on Arbitrum. Same logic as the tutorial strategy but configured
for grid search over RSI parameters.

Sweepable parameters:
    - rsi_period: Number of candles for RSI calculation (e.g., 10, 14, 20)
    - rsi_oversold: RSI buy threshold (e.g., 20, 25, 30, 35)
    - rsi_overbought: RSI sell threshold (e.g., 65, 70, 75, 80)
    - trade_size_usd: Trade size per signal (e.g., 3, 5, 10)

Run sweep:
    almanak strat backtest sweep -s demo_uniswap_rsi_sweep \\
        --start 2025-01-01 --end 2025-02-01 \\
        --chain arbitrum --tokens WETH,USDC \\
        --param "rsi_period:10,14,20" \\
        --param "rsi_oversold:20,25,30,35" \\
        --param "rsi_overbought:65,70,75,80"
"""

import logging
from datetime import UTC
from decimal import Decimal
from typing import Any

from almanak.framework.intents import Intent
from almanak.framework.strategies import (
    IntentStrategy,
    MarketSnapshot,
    almanak_strategy,
)
from almanak.framework.utils.log_formatters import format_usd

logger = logging.getLogger(__name__)


@almanak_strategy(
    name="demo_uniswap_rsi_sweep",
    description="Uniswap V3 RSI sweep demo — parameter optimization on Arbitrum",
    version="1.0.0",
    author="Almanak",
    tags=["demo", "sweep", "backtest", "rsi", "mean-reversion", "uniswap", "arbitrum"],
    supported_chains=["arbitrum"],
    supported_protocols=["uniswap_v3"],
    intent_types=["SWAP", "HOLD"],
    default_chain="arbitrum",
)
class UniswapRSISweepStrategy(IntentStrategy):
    """RSI-based mean reversion strategy optimized for parameter sweep backtesting.

    The sweep engine overrides config values before each backtest run. This
    strategy reads its parameters from config so sweep overrides take effect
    without code changes.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.trade_size_usd = Decimal(str(self.get_config("trade_size_usd", "3")))
        self.rsi_period = int(self.get_config("rsi_period", 14))
        self.rsi_oversold = Decimal(str(self.get_config("rsi_oversold", "30")))
        self.rsi_overbought = Decimal(str(self.get_config("rsi_overbought", "70")))
        self.max_slippage_bps = int(self.get_config("max_slippage_bps", 100))
        self.base_token = self.get_config("base_token", "WETH")
        self.quote_token = self.get_config("quote_token", "USDC")

        self._consecutive_holds = 0
        self._total_trades = 0

        logger.info(
            f"UniswapRSISweepStrategy initialized: "
            f"trade_size=${self.trade_size_usd}, "
            f"RSI({self.rsi_period}), "
            f"oversold={self.rsi_oversold}, overbought={self.rsi_overbought}"
        )

    def decide(self, market: MarketSnapshot) -> Intent | None:
        """Make a trading decision based on RSI."""
        try:
            rsi = market.rsi(self.base_token, period=self.rsi_period)
        except ValueError as e:
            logger.warning(f"Could not get RSI: {e}")
            return Intent.hold(reason="RSI data unavailable")

        try:
            quote_balance = market.balance(self.quote_token)
            base_balance = market.balance(self.base_token)
        except ValueError as e:
            logger.warning(f"Could not get balances: {e}")
            return Intent.hold(reason="Balance data unavailable")

        # BUY when oversold
        if rsi.value <= self.rsi_oversold:
            if quote_balance.balance_usd < self.trade_size_usd:
                return Intent.hold(
                    reason=f"Oversold (RSI={rsi.value:.1f}) but insufficient {self.quote_token}"
                )

            logger.info(
                f"BUY SIGNAL: RSI={rsi.value:.2f} < {self.rsi_oversold} "
                f"| Buying {format_usd(self.trade_size_usd)} of {self.base_token}"
            )
            self._consecutive_holds = 0
            self._total_trades += 1

            return Intent.swap(
                from_token=self.quote_token,
                to_token=self.base_token,
                amount_usd=self.trade_size_usd,
                max_slippage=Decimal(str(self.max_slippage_bps)) / Decimal("10000"),
                protocol="uniswap_v3",
            )

        # SELL when overbought
        elif rsi.value >= self.rsi_overbought:
            try:
                base_price = market.price(self.base_token)
            except (ValueError, KeyError) as e:
                logger.warning(f"Could not get price for {self.base_token}: {e}")
                return Intent.hold(reason="Price data unavailable")
            min_base_to_sell = self.trade_size_usd / base_price
            if base_balance.balance < min_base_to_sell:
                return Intent.hold(
                    reason=f"Overbought (RSI={rsi.value:.1f}) but insufficient {self.base_token}"
                )

            logger.info(
                f"SELL SIGNAL: RSI={rsi.value:.2f} > {self.rsi_overbought} "
                f"| Selling {format_usd(self.trade_size_usd)} of {self.base_token}"
            )
            self._consecutive_holds = 0
            self._total_trades += 1

            return Intent.swap(
                from_token=self.base_token,
                to_token=self.quote_token,
                amount_usd=self.trade_size_usd,
                max_slippage=Decimal(str(self.max_slippage_bps)) / Decimal("10000"),
                protocol="uniswap_v3",
            )

        # HOLD in neutral zone
        else:
            self._consecutive_holds += 1
            return Intent.hold(
                reason=f"RSI={rsi.value:.2f} in neutral zone "
                f"[{self.rsi_oversold}-{self.rsi_overbought}]"
            )

    def get_status(self) -> dict[str, Any]:
        return {
            "strategy": "demo_uniswap_rsi_sweep",
            "chain": self.chain,
            "config": {
                "rsi_period": self.rsi_period,
                "rsi_oversold": str(self.rsi_oversold),
                "rsi_overbought": str(self.rsi_overbought),
                "trade_size_usd": str(self.trade_size_usd),
            },
            "state": {
                "consecutive_holds": self._consecutive_holds,
                "total_trades": self._total_trades,
            },
        }

    # ── Teardown ──

    def supports_teardown(self) -> bool:
        return True

    def get_open_positions(self) -> "TeardownPositionSummary":
        from datetime import datetime

        from almanak.framework.teardown import (
            PositionInfo,
            PositionType,
            TeardownPositionSummary,
        )

        positions: list[PositionInfo] = []

        try:
            market = self.create_market_snapshot()
            base_balance = market.balance(self.base_token)
            if base_balance.balance > 0:
                positions.append(
                    PositionInfo(
                        position_type=PositionType.TOKEN,
                        position_id="uniswap_rsi_sweep_token_0",
                        chain=self.chain,
                        protocol="uniswap_v3",
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
            strategy_id=getattr(self, "strategy_id", "demo_uniswap_rsi_sweep"),
            timestamp=datetime.now(UTC),
            positions=positions,
        )

    def generate_teardown_intents(self, mode: "TeardownMode", market=None) -> list[Intent]:
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
