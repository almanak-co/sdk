"""Mantle MNT Accumulator Strategy.

Multi-signal accumulation strategy that builds a WMNT position on Mantle
using Uniswap V3 for swap execution.

Thesis:
    MNT is Mantle's native token. This strategy accumulates WMNT from USDT
    using three entry signals at varying conviction levels, plus a
    profit-taking mechanism to harvest gains into stables.

Signals:
    1. RSI oversold (< 30)    -> standard dip buy (25% of stables)
    2. RSI deeply oversold (<20) -> heavy dip buy (40% of stables)
    3. Regular accumulation    -> small periodic buy (10% of stables)
    4. RSI overbought (> 70)  -> partial profit-take (15% of WMNT)

State machine:
    accumulating -> (dip_buy | heavy_dip | profit_take | regular_buy) -> accumulating

Usage:
    almanak strat run -d strategies/demo/mantle_mnt_accumulator --network anvil --once
"""

import logging
from datetime import UTC, datetime, timedelta
from decimal import ROUND_DOWN, Decimal
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from almanak.framework.teardown.models import TeardownMode, TeardownPositionSummary

from almanak.framework.intents import Intent
from almanak.framework.strategies import IntentStrategy, MarketSnapshot, almanak_strategy

logger = logging.getLogger(__name__)


def _cfg(config, key: str, default: Any = None) -> Any:
    if isinstance(config, dict):
        return config.get(key, default)
    return getattr(config, key, default)


@almanak_strategy(
    name="mantle_mnt_accumulator",
    description="Multi-signal MNT accumulation with Uniswap V3 on Mantle",
    version="1.0.0",
    author="Almanak",
    tags=["mantle", "accumulation", "rsi", "dip-buying", "uniswap_v3"],
    supported_chains=["mantle"],
    supported_protocols=["uniswap_v3"],
    intent_types=["SWAP", "HOLD"],
)
class MantleMntAccumulator(IntentStrategy):
    """Accumulates WMNT on Mantle via multi-signal RSI entries and Uniswap V3.

    Three buy tiers based on RSI conviction, plus periodic small buys and
    profit-taking on overbought conditions. Position size is capped to avoid
    over-concentration.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        c = self.config

        # Tokens
        self.target_token: str = _cfg(c, "target_token", "WMNT")
        self.stable_token: str = _cfg(c, "stable_token", "USDT")
        self.protocol: str = _cfg(c, "protocol", "uniswap_v3")

        # RSI
        self.rsi_period: int = int(_cfg(c, "rsi_period", 14))
        self.rsi_oversold: int = int(_cfg(c, "rsi_oversold_threshold", 30))
        self.rsi_overbought: int = int(_cfg(c, "rsi_overbought_threshold", 70))
        self.heavy_dip_rsi: int = int(_cfg(c, "heavy_dip_rsi_threshold", 20))

        # Buy/sell sizing (% of available balance)
        self.base_buy_pct = Decimal(str(_cfg(c, "base_buy_pct", 10))) / Decimal("100")
        self.dip_buy_pct = Decimal(str(_cfg(c, "dip_buy_pct", 25))) / Decimal("100")
        self.heavy_dip_buy_pct = Decimal(str(_cfg(c, "heavy_dip_buy_pct", 40))) / Decimal("100")
        self.profit_take_pct = Decimal(str(_cfg(c, "profit_take_pct", 15))) / Decimal("100")

        # Risk limits
        self.max_position_pct = Decimal(str(_cfg(c, "max_position_pct", 80))) / Decimal("100")
        self.max_slippage = Decimal(str(_cfg(c, "max_slippage_bps", 150))) / Decimal("10000")
        self.cooldown = timedelta(minutes=int(_cfg(c, "cooldown_minutes", 30)))

        # Internal state
        self._last_trade_time: datetime | None = None
        self._last_rsi_signal: str = "NEUTRAL"
        self._total_buys: int = 0
        self._total_sells: int = 0

        logger.info(
            f"MantleMntAccumulator initialized: "
            f"{self.target_token}/{self.stable_token} via {self.protocol}, "
            f"RSI({self.rsi_period}) oversold<{self.rsi_oversold} heavy<{self.heavy_dip_rsi} "
            f"overbought>{self.rsi_overbought}, "
            f"buy tiers: {self.base_buy_pct*100:.0f}%/{self.dip_buy_pct*100:.0f}%/"
            f"{self.heavy_dip_buy_pct*100:.0f}%, "
            f"profit take: {self.profit_take_pct*100:.0f}%"
        )

    # --------------------------------------------------------------------- #
    # decide()
    # --------------------------------------------------------------------- #

    def decide(self, market: MarketSnapshot) -> Intent | None:
        try:
            return self._decide_internal(market)
        except Exception as e:
            logger.exception("Error in decide()")
            return Intent.hold(reason=f"Error: {e}")

    def _decide_internal(self, market: MarketSnapshot) -> Intent:
        # Read market data
        try:
            rsi = market.rsi(self.target_token, period=self.rsi_period)
        except Exception:
            return Intent.hold(reason="RSI data unavailable for MNT")

        stable_bal = market.balance(self.stable_token)
        target_bal = market.balance(self.target_token)
        total_usd = stable_bal.balance_usd + target_bal.balance_usd

        # Current position ratio
        position_pct = target_bal.balance_usd / total_usd if total_usd > 0 else Decimal("0")

        rsi_val = rsi.value
        current_signal = self._classify_signal(rsi_val)

        logger.debug(
            f"RSI={rsi_val:.1f} signal={current_signal} "
            f"position={position_pct*100:.1f}% "
            f"stables=${stable_bal.balance_usd:.2f} "
            f"target=${target_bal.balance_usd:.2f}"
        )

        # Cooldown check
        if not self._cooldown_passed():
            return Intent.hold(
                reason=f"Cooldown active (last trade {self._last_trade_time})"
            )

        # PROFIT TAKE: overbought + significant position
        if current_signal == "OVERBOUGHT" and position_pct > Decimal("0.2"):
            sell_amount = self._round_amount(
                target_bal.balance * self.profit_take_pct
            )
            if sell_amount > 0:
                logger.info(
                    f"PROFIT TAKE: RSI={rsi_val:.1f} > {self.rsi_overbought} | "
                    f"Selling {sell_amount:.4f} {self.target_token} "
                    f"({self.profit_take_pct*100:.0f}% of position)"
                )
                return Intent.swap(
                    from_token=self.target_token,
                    to_token=self.stable_token,
                    amount=sell_amount,
                    max_slippage=self.max_slippage,
                    protocol=self.protocol,
                )

        # Position cap check - don't buy if already heavily positioned
        if position_pct >= self.max_position_pct:
            return Intent.hold(
                reason=f"Position at {position_pct*100:.1f}% >= cap {self.max_position_pct*100:.0f}%"
            )

        # HEAVY DIP BUY: deeply oversold
        if current_signal == "HEAVY_DIP" and stable_bal.balance_usd > Decimal("10"):
            buy_amount = self._round_amount(
                stable_bal.balance * self.heavy_dip_buy_pct
            )
            if buy_amount > 0:
                logger.info(
                    f"HEAVY DIP BUY: RSI={rsi_val:.1f} < {self.heavy_dip_rsi} | "
                    f"Buying with {buy_amount:.2f} {self.stable_token} "
                    f"({self.heavy_dip_buy_pct*100:.0f}% of stables)"
                )
                return Intent.swap(
                    from_token=self.stable_token,
                    to_token=self.target_token,
                    amount=buy_amount,
                    max_slippage=self.max_slippage,
                    protocol=self.protocol,
                )

        # DIP BUY: oversold (signal change detection)
        if current_signal == "OVERSOLD" and self._last_rsi_signal != "OVERSOLD":
            if stable_bal.balance_usd > Decimal("10"):
                buy_amount = self._round_amount(
                    stable_bal.balance * self.dip_buy_pct
                )
                if buy_amount > 0:
                    logger.info(
                        f"DIP BUY: RSI={rsi_val:.1f} < {self.rsi_oversold} | "
                        f"Signal change {self._last_rsi_signal}->OVERSOLD | "
                        f"Buying with {buy_amount:.2f} {self.stable_token}"
                    )
                    self._last_rsi_signal = current_signal
                    return Intent.swap(
                        from_token=self.stable_token,
                        to_token=self.target_token,
                        amount=buy_amount,
                        max_slippage=self.max_slippage,
                        protocol=self.protocol,
                    )

        # REGULAR BUY: neutral zone, small periodic accumulation
        if current_signal == "NEUTRAL" and stable_bal.balance_usd > Decimal("50"):
            buy_amount = self._round_amount(
                stable_bal.balance * self.base_buy_pct
            )
            if buy_amount > 0:
                logger.info(
                    f"REGULAR BUY: RSI={rsi_val:.1f} neutral | "
                    f"Accumulating {buy_amount:.2f} {self.stable_token} worth of {self.target_token}"
                )
                self._last_rsi_signal = current_signal
                return Intent.swap(
                    from_token=self.stable_token,
                    to_token=self.target_token,
                    amount=buy_amount,
                    max_slippage=self.max_slippage,
                    protocol=self.protocol,
                )

        self._last_rsi_signal = current_signal
        return Intent.hold(
            reason=f"RSI={rsi_val:.1f} signal={current_signal} "
            f"pos={position_pct*100:.1f}% stables=${stable_bal.balance_usd:.2f}"
        )

    # --------------------------------------------------------------------- #
    # Helpers
    # --------------------------------------------------------------------- #

    def _classify_signal(self, rsi_val: Decimal) -> str:
        if rsi_val < self.heavy_dip_rsi:
            return "HEAVY_DIP"
        if rsi_val < self.rsi_oversold:
            return "OVERSOLD"
        if rsi_val > self.rsi_overbought:
            return "OVERBOUGHT"
        return "NEUTRAL"

    def _cooldown_passed(self) -> bool:
        if not self._last_trade_time:
            return True
        return datetime.now(UTC) - self._last_trade_time >= self.cooldown

    def _round_amount(self, amount: Decimal) -> Decimal:
        return amount.quantize(Decimal("0.000001"), rounding=ROUND_DOWN)

    # --------------------------------------------------------------------- #
    # Lifecycle hooks
    # --------------------------------------------------------------------- #

    def on_intent_executed(self, _intent: Any, success: bool, result: Any) -> None:
        if not success:
            logger.warning("Trade failed")
            return

        self._last_trade_time = datetime.now(UTC)

        # Detect buy vs sell from the intent
        if hasattr(_intent, "from_token"):
            if _intent.from_token == self.stable_token:
                self._total_buys += 1
                logger.info(f"Buy #{self._total_buys} executed successfully")
            else:
                self._total_sells += 1
                logger.info(f"Sell #{self._total_sells} executed (profit take)")

    # --------------------------------------------------------------------- #
    # State persistence
    # --------------------------------------------------------------------- #

    def get_persistent_state(self) -> dict[str, Any]:
        return {
            "last_trade_time": self._last_trade_time.isoformat() if self._last_trade_time else None,
            "last_rsi_signal": self._last_rsi_signal,
            "total_buys": self._total_buys,
            "total_sells": self._total_sells,
        }

    def load_persistent_state(self, state: dict[str, Any]) -> None:
        ltt = state.get("last_trade_time")
        self._last_trade_time = datetime.fromisoformat(ltt) if ltt else None
        self._last_rsi_signal = state.get("last_rsi_signal", "NEUTRAL")
        self._total_buys = state.get("total_buys", 0)
        self._total_sells = state.get("total_sells", 0)
        logger.info(
            f"Loaded state: buys={self._total_buys}, sells={self._total_sells}, "
            f"last_signal={self._last_rsi_signal}"
        )

    # --------------------------------------------------------------------- #
    # Status & teardown
    # --------------------------------------------------------------------- #

    def get_status(self) -> dict[str, Any]:
        return {
            "strategy": "mantle_mnt_accumulator",
            "chain": self.chain,
            "target": self.target_token,
            "stable": self.stable_token,
            "protocol": self.protocol,
            "total_buys": self._total_buys,
            "total_sells": self._total_sells,
            "last_signal": self._last_rsi_signal,
        }

    def get_open_positions(self) -> "TeardownPositionSummary":
        from almanak.framework.teardown import PositionInfo, PositionType, TeardownPositionSummary

        value_usd = Decimal("0")
        try:
            market = self.create_market_snapshot()
            target_bal = market.balance(self.target_token)
            try:
                price = market.price(self.target_token)
                value_usd = target_bal.balance * price
            except Exception:
                pass
        except Exception:
            pass

        return TeardownPositionSummary(
            strategy_id=getattr(self, "strategy_id", "mantle_mnt_accumulator"),
            timestamp=datetime.now(UTC),
            positions=[
                PositionInfo(
                    position_type=PositionType.TOKEN,
                    position_id="mnt_accumulator_position",
                    chain=self.chain,
                    protocol=self.protocol,
                    value_usd=value_usd,
                    details={"asset": self.target_token},
                )
            ],
        )

    def generate_teardown_intents(self, mode: "TeardownMode", market=None) -> list[Intent]:
        from almanak.framework.teardown import TeardownMode

        max_slippage = Decimal("0.03") if mode == TeardownMode.HARD else self.max_slippage

        # On teardown, sell all target token back to stables
        return [
            Intent.swap(
                from_token=self.target_token,
                to_token=self.stable_token,
                amount="all",
                max_slippage=max_slippage,
                protocol=self.protocol,
            )
        ]
