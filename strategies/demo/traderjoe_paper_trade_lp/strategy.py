"""
===============================================================================
DEMO: TraderJoe Paper Trade — RSI-Gated LP on Avalanche
===============================================================================

This demo strategy is the vehicle for testing the paper trading engine
(``almanak strat backtest paper``) with TraderJoe V2 Liquidity Book on
Avalanche. Opens LP when RSI is range-bound, closes when RSI is extreme,
and holds otherwise.

TraderJoe V2 Liquidity Book uses discrete price bins instead of continuous
tick ranges (Uniswap V3). This exercises bin-based LP mechanics in the
paper trading pipeline, validating position tracking and PnL journaling
for a non-V3-style AMM on a chain not yet tested (Avalanche).

PURPOSE:
--------
1. Validate paper trading on Avalanche with TraderJoe V2
2. Exercise LP_OPEN / LP_CLOSE intents with bin-based mechanics
3. Generate multi-tick PnL journal entries for equity curve tracking
4. Test Avalanche-specific Anvil fork behavior (WAVAX, gas model)

USAGE:
------
    # Paper trade for 5 ticks at 60-second intervals
    almanak strat backtest paper start \\
        -s demo_traderjoe_paper_trade_lp \\
        --chain avalanche \\
        --max-ticks 5 \\
        --tick-interval 60 \\
        --foreground

    # Or run directly on Anvil (single iteration)
    almanak strat run -d strategies/demo/traderjoe_paper_trade_lp \\
        --network anvil --once

STRATEGY LOGIC:
---------------
Each tick:
  1. Read RSI(WAVAX, 14)
  2. If RSI is range-bound (35-65) and no LP position -> open LP
  3. If RSI is extreme (<35 or >65) and has LP position -> close LP
  4. Otherwise -> hold
===============================================================================
"""

import logging
from datetime import UTC, datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Any

from almanak.framework.api.timeline import TimelineEvent, TimelineEventType, add_event
from almanak.framework.intents import Intent
from almanak.framework.strategies import (
    IntentStrategy,
    MarketSnapshot,
    almanak_strategy,
)
from almanak.framework.utils.log_formatters import format_token_amount_human

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from almanak.framework.teardown import TeardownMode, TeardownPositionSummary


@almanak_strategy(
    name="demo_traderjoe_paper_trade_lp",
    description="Paper trading demo — RSI-gated TraderJoe V2 LP on Avalanche",
    version="1.0.0",
    author="Almanak",
    tags=["demo", "paper-trading", "lp", "traderjoe-v2", "avalanche", "backtesting", "liquidity-book"],
    supported_chains=["avalanche"],
    supported_protocols=["traderjoe_v2"],
    intent_types=["LP_OPEN", "LP_CLOSE", "HOLD"],
    default_chain="avalanche",
)
class TraderJoePaperTradeLPStrategy(IntentStrategy):
    """RSI-gated TraderJoe V2 LP strategy for paper trading validation.

    Uses Liquidity Book bin-based LP mechanics on Avalanche. Opens LP
    positions centered on current price when RSI is neutral, closes when
    RSI hits extremes.

    Configuration (config.json):
        pool: Pool identifier (e.g. "WAVAX/USDC/20")
        range_width_pct: Total price range width (0.10 = 10%)
        amount_x: Token X amount to LP (e.g. "0.5" WAVAX)
        amount_y: Token Y amount to LP (e.g. "10" USDC)
        rsi_period: RSI period (default: 14)
        rsi_oversold: RSI threshold for extreme low (default: 35)
        rsi_overbought: RSI threshold for extreme high (default: 65)
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # Pool configuration
        self.pool = str(self.get_config("pool", "WAVAX/USDC/20"))
        try:
            pool_parts = self.pool.split("/")
            self.token_x = pool_parts[0]
            self.token_y = pool_parts[1]
            self.bin_step = int(pool_parts[2])
        except (IndexError, ValueError):
            logger.warning(f"Could not parse pool '{self.pool}', falling back to defaults.")
            self.token_x = "WAVAX"
            self.token_y = "USDC"
            self.bin_step = 20

        # LP amounts
        self.amount_x = Decimal(str(self.get_config("amount_x", "0.5")))
        self.amount_y = Decimal(str(self.get_config("amount_y", "10")))
        self.range_width_pct = Decimal(str(self.get_config("range_width_pct", "0.10")))

        # RSI parameters
        self.rsi_period = int(self.get_config("rsi_period", 14))
        self.rsi_oversold = Decimal(str(self.get_config("rsi_oversold", "35")))
        self.rsi_overbought = Decimal(str(self.get_config("rsi_overbought", "65")))

        # Internal state
        self._has_position = False
        self._ticks_with_position = 0

        logger.info(
            f"TraderJoePaperTradeLP initialized: pool={self.pool}, "
            f"amounts={self.amount_x} {self.token_x} + {self.amount_y} {self.token_y}, "
            f"RSI({self.rsi_period}) range=[{self.rsi_oversold}, {self.rsi_overbought}]"
        )

    # =========================================================================
    # MAIN DECISION LOGIC
    # =========================================================================

    def decide(self, market: MarketSnapshot) -> Intent | None:
        """RSI-gated LP open/close decision."""
        try:
            # Get RSI for token X
            try:
                rsi = market.rsi(self.token_x, period=self.rsi_period)
                rsi_value = rsi.value
                logger.info(f"RSI({self.rsi_period}) = {rsi_value:.1f}")
            except (ValueError, KeyError, AttributeError) as e:
                logger.warning(f"RSI data unavailable: {e}")
                return Intent.hold(reason=f"RSI data unavailable: {e}")

            rsi_in_range = self.rsi_oversold <= rsi_value <= self.rsi_overbought
            rsi_extreme = rsi_value < self.rsi_oversold or rsi_value > self.rsi_overbought

            # Get current price for LP range calculation
            try:
                token_x_price_usd = market.price(self.token_x)
                token_y_price_usd = market.price(self.token_y)
                current_price = token_x_price_usd / token_y_price_usd
            except (ValueError, KeyError) as e:
                logger.warning(f"Price data unavailable: {e}")
                return Intent.hold(reason=f"Price data unavailable: {e}")

            # Check balances
            try:
                bal_x = market.balance(self.token_x)
                bal_y = market.balance(self.token_y)
                has_funds = bal_x.balance >= self.amount_x and bal_y.balance >= self.amount_y
            except (ValueError, KeyError):
                has_funds = False

            # Decision
            if self._has_position:
                self._ticks_with_position += 1

                if rsi_extreme:
                    logger.info(
                        f"RSI extreme ({rsi_value:.1f}), closing LP after "
                        f"{self._ticks_with_position} ticks"
                    )
                    return self._create_close_intent()

                return Intent.hold(
                    reason=f"LP active ({self._ticks_with_position} ticks), "
                    f"RSI={rsi_value:.1f} in range"
                )

            else:
                if rsi_in_range and has_funds:
                    logger.info(f"RSI in range ({rsi_value:.1f}), opening LP")
                    return self._create_open_intent(current_price)

                reason = []
                if not rsi_in_range:
                    reason.append(f"RSI={rsi_value:.1f} outside range")
                if not has_funds:
                    reason.append("insufficient funds")
                return Intent.hold(reason=f"No LP: {', '.join(reason)}")

        except Exception as e:
            logger.exception(f"Error in decide(): {e}")
            return Intent.hold(reason=f"Error: {e}")

    # =========================================================================
    # INTENT CREATION
    # =========================================================================

    def _create_open_intent(self, current_price: Decimal) -> Intent:
        """Create LP_OPEN intent for TraderJoe V2 Liquidity Book."""
        half_width = self.range_width_pct / Decimal("2")
        range_lower = current_price * (Decimal("1") - half_width)
        range_upper = current_price * (Decimal("1") + half_width)

        logger.info(
            f"LP_OPEN: {format_token_amount_human(self.amount_x, self.token_x)} + "
            f"{format_token_amount_human(self.amount_y, self.token_y)}, "
            f"price range [{range_lower:.4f} - {range_upper:.4f}], bin_step={self.bin_step}"
        )
        return Intent.lp_open(
            pool=self.pool,
            amount0=self.amount_x,
            amount1=self.amount_y,
            range_lower=range_lower,
            range_upper=range_upper,
            protocol="traderjoe_v2",
            chain=self.chain,
        )

    def _lp_position_id(self) -> str:
        """Canonical LP position ID used across close intent and teardown."""
        return f"traderjoe-lp-{self.pool.replace('/', '-')}"

    def _create_close_intent(self) -> Intent:
        """Create LP_CLOSE intent for TraderJoe V2."""
        logger.info(f"LP_CLOSE: {self.pool}")
        return Intent.lp_close(
            position_id=self._lp_position_id(),
            pool=self.pool,
            collect_fees=True,
            protocol="traderjoe_v2",
            chain=self.chain,
        )

    # =========================================================================
    # LIFECYCLE HOOKS
    # =========================================================================

    def on_intent_executed(self, intent: Any, success: bool, result: Any) -> None:
        """Track LP position state from execution results."""
        if not success:
            logger.warning(f"Intent failed: {getattr(intent, 'intent_type', 'unknown')}")
            return

        intent_type = getattr(intent, "intent_type", None)
        if intent_type is None:
            return

        intent_type_val = intent_type.value if hasattr(intent_type, "value") else str(intent_type)

        if intent_type_val == "LP_OPEN":
            self._has_position = True
            self._ticks_with_position = 0
            logger.info(f"LP position opened in {self.pool}")
            add_event(
                TimelineEvent(
                    timestamp=datetime.now(UTC),
                    event_type=TimelineEventType.POSITION_MODIFIED,
                    description=f"Opened TraderJoe LP in {self.pool}",
                    strategy_id=self.strategy_id,
                    details={"action": "lp_open", "pool": self.pool},
                )
            )

        elif intent_type_val == "LP_CLOSE":
            self._has_position = False
            self._ticks_with_position = 0
            logger.info(f"LP position closed in {self.pool}")
            add_event(
                TimelineEvent(
                    timestamp=datetime.now(UTC),
                    event_type=TimelineEventType.POSITION_MODIFIED,
                    description=f"Closed TraderJoe LP in {self.pool}",
                    strategy_id=self.strategy_id,
                    details={"action": "lp_close", "pool": self.pool},
                )
            )

    # =========================================================================
    # STATE PERSISTENCE
    # =========================================================================

    def get_persistent_state(self) -> dict[str, Any]:
        return {
            "has_position": self._has_position,
            "ticks_with_position": self._ticks_with_position,
        }

    def load_persistent_state(self, state: dict[str, Any]) -> None:
        if "has_position" in state:
            self._has_position = bool(state["has_position"])
        if "ticks_with_position" in state:
            self._ticks_with_position = int(state["ticks_with_position"])
        logger.info(
            f"Restored state: has_position={self._has_position}, "
            f"ticks={self._ticks_with_position}"
        )

    # =========================================================================
    # TEARDOWN
    # =========================================================================

    def supports_teardown(self) -> bool:
        return True

    def get_open_positions(self) -> "TeardownPositionSummary":
        from almanak.framework.teardown import PositionInfo, PositionType, TeardownPositionSummary

        positions = []
        if self._has_position:
            positions.append(
                PositionInfo(
                    position_type=PositionType.LP,
                    position_id=self._lp_position_id(),
                    chain=self.chain,
                    protocol="traderjoe_v2",
                    value_usd=Decimal("0"),
                    details={
                        "pool": self.pool,
                        "token_x": self.token_x,
                        "token_y": self.token_y,
                        "bin_step": self.bin_step,
                    },
                )
            )
        return TeardownPositionSummary(
            strategy_id=self.strategy_id,
            timestamp=datetime.now(UTC),
            positions=positions,
        )

    def generate_teardown_intents(self, mode: "TeardownMode", market=None) -> list[Intent]:
        intents = []
        if self._has_position:
            intents.append(self._create_close_intent())
        return intents

    # =========================================================================
    # STATUS
    # =========================================================================

    def get_status(self) -> dict[str, Any]:
        return {
            "strategy": "demo_traderjoe_paper_trade_lp",
            "chain": self.chain,
            "config": {
                "pool": self.pool,
                "bin_step": self.bin_step,
                "range_width_pct": str(self.range_width_pct),
                "amount_x": str(self.amount_x),
                "amount_y": str(self.amount_y),
                "rsi_period": self.rsi_period,
            },
            "state": {
                "has_position": self._has_position,
                "ticks_with_position": self._ticks_with_position,
            },
        }
