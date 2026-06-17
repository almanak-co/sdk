"""Uniswap V3 RSI Parameter Sweep -- Arbitrum with Production-Scale Trades.

This strategy validates the parameter sweep backtester on Arbitrum with
production-scale trade sizes (500-2000 USD). The existing uniswap_rsi_sweep
demo uses $3 trades -- this version exercises the sweep engine with realistic
capital deployment and Arbitrum-specific gas/price feeds.

PURPOSE:
--------
1. First parameter sweep with production-scale trade sizes on Arbitrum.
2. Validates sweep engine parameter override mechanics at larger scale.
3. Tests Arbitrum gas estimation in sweep context.

SWEEP PARAMETERS (from ticket VIB-1916):
-----------------------------------------
  - rsi_period: [7, 14, 21]
  - rsi_oversold: [25, 30, 35]
  - rsi_overbought: [65, 70, 75]
  - trade_size_usd: [500, 1000, 2000]

USAGE:
------
    almanak strat backtest sweep -s demo_uniswap_v3_rsi_sweep_arbitrum \\
        --start 2025-01-01 --end 2025-02-01 \\
        --chain arbitrum --tokens WETH,USDC \\
        --param "rsi_period:7,14,21" \\
        --param "rsi_oversold:25,30,35" \\
        --param "rsi_overbought:65,70,75" \\
        --param "trade_size_usd:500,1000,2000"
"""

import logging
from datetime import UTC
from decimal import ROUND_DOWN, Decimal
from typing import TYPE_CHECKING, Any

from almanak.framework.intents import Intent
from almanak.framework.market import MarketSnapshot
from almanak.framework.strategies import IntentStrategy, almanak_strategy
from almanak.framework.utils.log_formatters import format_usd

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from almanak.framework.teardown import TeardownMode, TeardownPositionSummary


@almanak_strategy(
    name="demo_uniswap_v3_rsi_sweep_arbitrum",
    description="Uniswap V3 RSI parameter sweep on Arbitrum -- production-scale trade sizes",
    version="1.0.0",
    author="Almanak",
    tags=["demo", "sweep", "backtest", "rsi", "uniswap-v3", "arbitrum"],
    supported_chains=["arbitrum"],
    supported_protocols=["uniswap_v3"],
    intent_types=["SWAP", "HOLD"],
    default_chain="arbitrum",
    quote_asset="USD",
)
class UniswapV3RSISweepArbitrumStrategy(IntentStrategy):
    """RSI-based mean reversion strategy for parameter sweep on Arbitrum.

    The sweep engine overrides config values before each backtest run. This
    strategy reads parameters from config so sweep overrides take effect.

    Configuration (config.json):
        base_token: Token to trade (default: "WETH")
        quote_token: Quote token (default: "USDC")
        trade_size_usd: USD amount per trade (default: 1000)
        rsi_period: RSI lookback period (default: 14)
        rsi_oversold: RSI buy threshold (default: 30)
        rsi_overbought: RSI sell threshold (default: 70)
        max_slippage_bps: Max slippage in basis points (default: 50)
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.base_token = str(self.get_config("base_token", "WETH"))
        self.quote_token = str(self.get_config("quote_token", "USDC"))
        self.trade_size_usd = Decimal(str(self.get_config("trade_size_usd", "1000")))
        self.rsi_period = int(self.get_config("rsi_period", 14))
        self.rsi_oversold = Decimal(str(self.get_config("rsi_oversold", "30")))
        self.rsi_overbought = Decimal(str(self.get_config("rsi_overbought", "70")))
        self.max_slippage_bps = int(self.get_config("max_slippage_bps", 50))

        self._consecutive_holds = 0
        self._total_trades = 0
        self._total_buys = 0
        self._total_sells = 0
        # Neutral-rearm latch: only act on a signal transition, re-arm via neutral.
        self._last_signal = "neutral"

        logger.info(
            f"UniswapV3RSISweepArbitrum initialized: "
            f"trade_size={format_usd(self.trade_size_usd)}, "
            f"RSI({self.rsi_period}), "
            f"oversold={self.rsi_oversold}, overbought={self.rsi_overbought}, "
            f"slippage={self.max_slippage_bps}bps"
        )

    def decide(self, market: MarketSnapshot) -> Intent | None:
        """RSI-gated swap: buy oversold, sell overbought."""
        try:
            rsi = market.rsi(self.base_token, period=self.rsi_period)
        except (ValueError, KeyError, AttributeError) as e:
            logger.warning(f"RSI data unavailable: {e}")
            return Intent.hold(reason=f"RSI data unavailable: {e}")

        try:
            quote_balance = market.balance(self.quote_token)
            base_balance = market.balance(self.base_token)
        except (ValueError, KeyError) as e:
            logger.warning(f"Balance data unavailable: {e}")
            return Intent.hold(reason=f"Balance data unavailable: {e}")

        max_slippage = Decimal(str(self.max_slippage_bps)) / Decimal("10000")

        # Neutral re-arm: act only on a transition into a signal zone, not every
        # tick RSI stays extreme. Re-arm when RSI returns to neutral; the buy/sell
        # latch is set in on_intent_executed on a SUCCESSFUL swap, so a held-back
        # (insufficient balance) or failed swap never locks out the next attempt.
        current_signal = (
            "buy" if rsi.value < self.rsi_oversold
            else "sell" if rsi.value > self.rsi_overbought
            else "neutral"
        )
        if current_signal == "neutral":
            self._last_signal = "neutral"
            self._consecutive_holds += 1
            return Intent.hold(
                reason=f"RSI={rsi.value:.2f} in neutral zone "
                f"[{self.rsi_oversold}, {self.rsi_overbought}]"
            )
        if current_signal == self._last_signal:
            return Intent.hold(
                reason=f"RSI={rsi.value:.2f} still {current_signal}; awaiting neutral reset"
            )

        # BUY when oversold
        if current_signal == "buy":
            if quote_balance.balance_usd < self.trade_size_usd:
                return Intent.hold(
                    reason=f"Oversold (RSI={rsi.value:.1f}) but insufficient {self.quote_token} "
                    f"({format_usd(quote_balance.balance_usd)} < {format_usd(self.trade_size_usd)})"
                )

            logger.info(
                f"BUY SIGNAL: RSI={rsi.value:.2f} < {self.rsi_oversold} "
                f"| Buying {format_usd(self.trade_size_usd)} of {self.base_token}"
            )
            self._consecutive_holds = 0
            # Trade counters are incremented in on_intent_executed on a
            # SUCCESSFUL swap, not here — a decision is not an execution (ALM-2808).

            return Intent.swap(
                from_token=self.quote_token,
                to_token=self.base_token,
                amount_usd=self.trade_size_usd,
                max_slippage=max_slippage,
                protocol="uniswap_v3",
                chain=self.chain,
            )

        # SELL when overbought
        if current_signal == "sell":
            try:
                base_price = market.price(self.base_token)
            except (ValueError, KeyError) as e:
                return Intent.hold(reason=f"Price unavailable for {self.base_token}: {e}")

            if base_price <= 0:
                return Intent.hold(reason=f"Invalid price for {self.base_token}: {base_price}")

            min_base_to_sell = (self.trade_size_usd / base_price).quantize(Decimal("0.0001"), rounding=ROUND_DOWN)
            if base_balance.balance < min_base_to_sell:
                return Intent.hold(
                    reason=f"Overbought (RSI={rsi.value:.1f}) but insufficient {self.base_token} "
                    f"(have {base_balance.balance} < need {min_base_to_sell})"
                )

            logger.info(
                f"SELL SIGNAL: RSI={rsi.value:.2f} > {self.rsi_overbought} "
                f"| Selling {min_base_to_sell} {self.base_token}"
            )
            self._consecutive_holds = 0
            # Trade counters are incremented in on_intent_executed on a
            # SUCCESSFUL swap, not here — a decision is not an execution (ALM-2808).

            return Intent.swap(
                from_token=self.base_token,
                to_token=self.quote_token,
                amount=min_base_to_sell,
                max_slippage=max_slippage,
                protocol="uniswap_v3",
                chain=self.chain,
            )

        return Intent.hold(reason=f"RSI={rsi.value:.2f}; no actionable signal")

    def on_intent_executed(self, intent: Intent, success: bool, result: Any) -> None:
        """Latch the acted signal and count the trade only on a successful swap.

        Trade-side state (executed-trade counters and the buy/sell re-arm latch)
        is mutated here, post-execution — never in decide(), where the swap has
        not yet landed (ALM-2808). _consecutive_holds stays in decide() because
        it counts HOLD *decisions*, not executions.
        """
        if not success:
            return
        intent_type = getattr(intent, "intent_type", None)
        if not intent_type or intent_type.value != "SWAP":
            return
        if getattr(intent, "to_token", None) == self.base_token:
            self._last_signal = "buy"
            self._total_trades += 1
            self._total_buys += 1
        elif getattr(intent, "from_token", None) == self.base_token:
            self._last_signal = "sell"
            self._total_trades += 1
            self._total_sells += 1

    def get_persistent_state(self) -> dict[str, Any]:
        return {
            "last_signal": self._last_signal,
            "consecutive_holds": self._consecutive_holds,
            "total_trades": self._total_trades,
            "total_buys": self._total_buys,
            "total_sells": self._total_sells,
        }

    def load_persistent_state(self, state: dict[str, Any]) -> None:
        if state:
            self._last_signal = state.get("last_signal", "neutral")
            self._consecutive_holds = int(state.get("consecutive_holds", 0))
            self._total_trades = int(state.get("total_trades", 0))
            self._total_buys = int(state.get("total_buys", 0))
            self._total_sells = int(state.get("total_sells", 0))

    def get_status(self) -> dict[str, Any]:
        return {
            "strategy": "demo_uniswap_v3_rsi_sweep_arbitrum",
            "chain": self.chain,
            "config": {
                "rsi_period": self.rsi_period,
                "rsi_oversold": str(self.rsi_oversold),
                "rsi_overbought": str(self.rsi_overbought),
                "trade_size_usd": str(self.trade_size_usd),
                "max_slippage_bps": self.max_slippage_bps,
            },
            "state": {
                "consecutive_holds": self._consecutive_holds,
                "total_trades": self._total_trades,
                "total_buys": self._total_buys,
                "total_sells": self._total_sells,
            },
        }

    # -- Teardown --

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
                        position_id="uniswap_rsi_sweep_arb_token_0",
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
            deployment_id=getattr(self, "deployment_id", "demo_uniswap_v3_rsi_sweep_arbitrum"),
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
                chain=self.chain,
            )
        ]
