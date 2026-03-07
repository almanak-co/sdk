"""
RSI + MACD Confluence LP Strategy on Arbitrum.

First kitchenloop strategy combining two technical indicators simultaneously.
Opens Uniswap V3 LP only when BOTH RSI and MACD confirm (conjunction entry).
Closes when EITHER signal turns bearish (disjunction exit).

This pattern is foundational for real quant strategies where signal confluence
reduces false positives dramatically.
"""

import logging
from datetime import UTC, datetime
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
    name="demo_rsi_macd_lp",
    description="RSI + MACD confluence LP entry on Uniswap V3 (Arbitrum)",
    version="1.0.0",
    author="Almanak",
    tags=["demo", "lp", "rsi", "macd", "confluence", "uniswap-v3", "arbitrum"],
    supported_chains=["arbitrum"],
    supported_protocols=["uniswap_v3"],
    intent_types=["LP_OPEN", "LP_CLOSE", "HOLD"],
)
class RSIMACDLPStrategy(IntentStrategy):
    """Uniswap V3 LP with dual-signal confluence entry (RSI + MACD)."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # Pool config
        self.pool = self.get_config("pool", "WETH/USDC/500")
        pool_parts = self.pool.split("/")
        if len(pool_parts) != 3:
            raise ValueError(
                f"Invalid pool format: '{self.pool}'. Expected 'TOKEN0/TOKEN1/FEE'."
            )
        self.token0_symbol = pool_parts[0]
        self.token1_symbol = pool_parts[1]

        # RSI parameters
        self.rsi_oversold = float(self.get_config("rsi_oversold", "35"))
        self.rsi_overbought = float(self.get_config("rsi_overbought", "65"))

        # MACD parameters
        self.macd_fast = int(self.get_config("macd_fast", "12"))
        self.macd_slow = int(self.get_config("macd_slow", "26"))
        self.macd_signal = int(self.get_config("macd_signal", "9"))

        # LP parameters
        self.lp_range_pct = Decimal(str(self.get_config("lp_range_pct", "0.15")))
        self.amount0 = Decimal(str(self.get_config("amount0", "0.001")))
        self.amount1 = Decimal(str(self.get_config("amount1", "3")))

        # State
        self._current_position_id: str | None = None
        self._prev_macd_histogram: float | None = None
        self._signal_log: list[dict] = []

        logger.info(
            f"RSIMACDLPStrategy initialized: pool={self.pool}, "
            f"RSI [{self.rsi_oversold}/{self.rsi_overbought}], "
            f"MACD [{self.macd_fast}/{self.macd_slow}/{self.macd_signal}]"
        )

    def decide(self, market: MarketSnapshot) -> Intent | None:
        """Dual-signal confluence decision: AND for entry, OR for exit."""
        # Read both indicators
        try:
            rsi = market.rsi(self.token0_symbol, period=14)
        except (ValueError, KeyError) as e:
            logger.warning(f"RSI unavailable: {e}")
            return Intent.hold(reason="RSI data unavailable")

        try:
            macd = market.macd(
                self.token0_symbol,
                fast_period=self.macd_fast,
                slow_period=self.macd_slow,
                signal_period=self.macd_signal,
            )
        except (ValueError, KeyError) as e:
            logger.warning(f"MACD unavailable: {e}")
            return Intent.hold(reason="MACD data unavailable")

        # Determine MACD trend direction
        histogram = macd.histogram
        prev_histogram = self._prev_macd_histogram
        self._prev_macd_histogram = histogram

        macd_bullish = histogram > 0 and (prev_histogram is None or histogram > prev_histogram)
        macd_bearish = histogram < 0 and (prev_histogram is not None and histogram < prev_histogram)

        # Log signal for analysis
        self._signal_log.append({
            "timestamp": datetime.now(UTC).isoformat(),
            "rsi": round(rsi, 2),
            "macd_histogram": round(histogram, 6),
            "macd_bullish": macd_bullish,
            "macd_bearish": macd_bearish,
        })
        # Keep last 50 entries
        if len(self._signal_log) > 50:
            self._signal_log = self._signal_log[-50:]

        logger.info(
            f"Signals: RSI={rsi:.1f}, MACD histogram={histogram:.6f}, "
            f"bullish={macd_bullish}, bearish={macd_bearish}"
        )

        # === ENTRY LOGIC (conjunction: both must confirm) ===
        if not self._current_position_id:
            if rsi < self.rsi_oversold and macd_bullish:
                # Price lookup only needed for LP range sizing
                try:
                    token0_price = market.price(self.token0_symbol)
                    token1_price = market.price(self.token1_symbol)
                    current_price = token0_price / token1_price
                except (ValueError, KeyError) as e:
                    logger.warning(f"Price data unavailable: {e}")
                    return Intent.hold(reason="Price data unavailable for LP range calculation")

                logger.info(
                    f"Confluence entry! RSI={rsi:.1f} < {self.rsi_oversold} "
                    f"AND MACD bullish (histogram={histogram:.6f})"
                )
                half_width = self.lp_range_pct / Decimal("2")
                range_lower = current_price * (Decimal("1") - half_width)
                range_upper = current_price * (Decimal("1") + half_width)

                return Intent.lp_open(
                    pool=self.pool,
                    amount0=self.amount0,
                    amount1=self.amount1,
                    range_lower=range_lower,
                    range_upper=range_upper,
                    protocol="uniswap_v3",
                )

            macd_status = "bullish" if macd_bullish else ("bearish" if macd_bearish else "neutral")
            return Intent.hold(
                reason=f"Waiting for confluence: RSI={rsi:.1f}, MACD={macd_status}"
            )

        # === EXIT LOGIC (disjunction: either signal triggers close) ===
        if rsi > self.rsi_overbought:
            logger.info(f"Exit: RSI={rsi:.1f} > {self.rsi_overbought}")
            return Intent.lp_close(
                position_id=self._current_position_id,
                pool=self.pool,
                collect_fees=True,
                protocol="uniswap_v3",
            )

        if macd_bearish:
            logger.info(f"Exit: MACD turned bearish (histogram={histogram:.6f})")
            return Intent.lp_close(
                position_id=self._current_position_id,
                pool=self.pool,
                collect_fees=True,
                protocol="uniswap_v3",
            )

        macd_status = "bullish" if macd_bullish else "neutral"
        return Intent.hold(
            reason=f"Holding LP {self._current_position_id}: RSI={rsi:.1f}, MACD={macd_status}"
        )

    def on_intent_executed(self, intent: Intent, success: bool, result: Any) -> None:
        """Track position ID after LP_OPEN, clear after LP_CLOSE."""
        if success and intent.intent_type.value == "LP_OPEN":
            position_id = getattr(result, "position_id", None) if result else None
            if position_id:
                self._current_position_id = str(position_id)
                logger.info(f"LP opened via confluence: position_id={position_id}")
        elif success and intent.intent_type.value == "LP_CLOSE":
            logger.info(f"LP closed: position_id={self._current_position_id}")
            self._current_position_id = None

    def get_persistent_state(self) -> dict[str, Any]:
        """Persist position, MACD history, and signal log."""
        state = super().get_persistent_state()
        if self._current_position_id:
            state["current_position_id"] = self._current_position_id
        if self._prev_macd_histogram is not None:
            state["prev_macd_histogram"] = self._prev_macd_histogram
        if self._signal_log:
            state["signal_log"] = self._signal_log
        return state

    def load_persistent_state(self, state: dict[str, Any]) -> None:
        """Load persistent state."""
        super().load_persistent_state(state)
        if "current_position_id" in state:
            self._current_position_id = str(state["current_position_id"])
        if "prev_macd_histogram" in state:
            self._prev_macd_histogram = float(state["prev_macd_histogram"])
        if "signal_log" in state:
            self._signal_log = state["signal_log"]

    # Teardown support

    def supports_teardown(self) -> bool:
        return True

    def get_open_positions(self):
        from almanak.framework.teardown import PositionInfo, PositionType, TeardownPositionSummary

        positions = []
        if self._current_position_id:
            positions.append(
                PositionInfo(
                    position_type=PositionType.LP,
                    position_id=str(self._current_position_id),
                    chain=self.chain,
                    protocol="uniswap_v3",
                    value_usd=self.amount0 * Decimal("3400") + self.amount1,  # Estimate for teardown info
                    details={
                        "pool": self.pool,
                        "token0": self.token0_symbol,
                        "token1": self.token1_symbol,
                    },
                )
            )
        return TeardownPositionSummary(
            strategy_id=getattr(self, "strategy_id", "demo_rsi_macd_lp"),
            timestamp=datetime.now(UTC),
            positions=positions,
        )

    def generate_teardown_intents(self, mode, market=None):
        if not self._current_position_id:
            return []
        return [
            Intent.lp_close(
                position_id=self._current_position_id,
                pool=self.pool,
                collect_fees=True,
                protocol="uniswap_v3",
            )
        ]
