"""
===============================================================================
DEMO: Aerodrome Paper Trade — RSI-Based LP on Base
===============================================================================

This demo strategy is the vehicle for testing the paper trading engine
(``almanak strat backtest paper``).  The strategy itself is intentionally
simple: open an Aerodrome volatile LP when RSI is range-bound, close it
when RSI is extreme, and hold otherwise.

PURPOSE:
--------
1. Validate the paper trading pipeline end-to-end:
   - Anvil fork management (start, fund, reset per tick)
   - Strategy execution with real on-chain interactions
   - PnL journal entries and equity curve generation
   - Multi-iteration execution lifecycle
2. Exercise Aerodrome LP_OPEN / LP_CLOSE on Base via paper trading.

USAGE:
------
    # Paper trade for 5 ticks at 60-second intervals
    almanak strat backtest paper start \\
        -s demo_aerodrome_paper_trade \\
        --chain base \\
        --max-ticks 5 \\
        --tick-interval 60 \\
        --foreground

    # Or run directly on Anvil (single iteration)
    almanak strat run -d strategies/demo/aerodrome_paper_trade \\
        --network anvil --once

STRATEGY LOGIC:
---------------
Each tick:
  1. Read RSI(ETH, 14)
  2. If RSI is range-bound (35-65) and no LP position → open LP
  3. If RSI is extreme (<35 or >65) and has LP position → close LP
  4. Otherwise → hold

This creates a realistic pattern of opens and closes across ticks,
generating PnL journal entries for the paper trader to track.
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
    name="demo_aerodrome_paper_trade",
    description="Paper trading demo — RSI-gated Aerodrome LP on Base",
    version="1.0.0",
    author="Almanak",
    tags=["demo", "paper-trading", "lp", "aerodrome", "base", "backtesting"],
    supported_chains=["base"],
    supported_protocols=["aerodrome"],
    intent_types=["LP_OPEN", "LP_CLOSE", "HOLD"],
)
class AerodromePaperTradeStrategy(IntentStrategy):
    """RSI-gated Aerodrome LP strategy for paper trading validation.

    Configuration (config.json):
        pool: Pool pair (e.g. "WETH/USDC")
        stable: Pool type (true=stable, false=volatile)
        amount0: Token0 amount to LP (e.g. "0.001" WETH)
        amount1: Token1 amount to LP (e.g. "3" USDC)
        rsi_period: RSI period (default: 14)
        rsi_oversold: RSI threshold for extreme low (default: 35)
        rsi_overbought: RSI threshold for extreme high (default: 65)

    Running Notes:
        Use ``almanak strat backtest paper start`` for multi-tick paper trading
        with PnL tracking. Use ``--fresh`` flag on Anvil to clear stale state.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # Pool configuration
        self.pool = str(self.get_config("pool", "WETH/USDC"))
        pool_parts = self.pool.split("/")
        self.token0 = pool_parts[0] if len(pool_parts) > 0 else "WETH"
        self.token1 = pool_parts[1] if len(pool_parts) > 1 else "USDC"
        self.stable = bool(self.get_config("stable", False))

        # LP amounts
        self.amount0 = Decimal(str(self.get_config("amount0", "0.001")))
        self.amount1 = Decimal(str(self.get_config("amount1", "3")))

        # RSI parameters
        self.rsi_period = int(self.get_config("rsi_period", 14))
        self.rsi_oversold = Decimal(str(self.get_config("rsi_oversold", "35")))
        self.rsi_overbought = Decimal(str(self.get_config("rsi_overbought", "65")))

        # Internal state
        self._has_position = False
        self._lp_token_balance = Decimal("0")
        self._ticks_with_position = 0

        pool_type = "stable" if self.stable else "volatile"
        logger.info(
            f"AerodromePaperTrade initialized: pool={self.pool} ({pool_type}), "
            f"amounts={self.amount0} {self.token0} + {self.amount1} {self.token1}, "
            f"RSI({self.rsi_period}) range=[{self.rsi_oversold}, {self.rsi_overbought}]"
        )

    # =========================================================================
    # MAIN DECISION LOGIC
    # =========================================================================

    def decide(self, market: MarketSnapshot) -> Intent | None:
        """RSI-gated LP open/close decision."""
        try:
            # Get RSI
            try:
                rsi = market.rsi(self.token0, period=self.rsi_period)
                rsi_value = rsi.value
                logger.info(f"RSI({self.rsi_period}) = {rsi_value:.1f}")
            except (ValueError, KeyError, AttributeError) as e:
                logger.warning(f"Could not get RSI: {e}. Defaulting to neutral (50).")
                rsi_value = Decimal("50")

            rsi_in_range = self.rsi_oversold <= rsi_value <= self.rsi_overbought
            rsi_extreme = rsi_value < self.rsi_oversold or rsi_value > self.rsi_overbought

            # Check balances for LP
            try:
                bal0 = market.balance(self.token0)
                bal1 = market.balance(self.token1)
                has_funds = bal0.balance >= self.amount0 and bal1.balance >= self.amount1
            except (ValueError, KeyError):
                has_funds = False

            # Decision logic
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
                    return self._create_open_intent()

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

    def _create_open_intent(self) -> Intent:
        """Create LP_OPEN intent for Aerodrome."""
        pool_type = "stable" if self.stable else "volatile"
        pool_with_type = f"{self.pool}/{pool_type}"
        logger.info(
            f"LP_OPEN: {format_token_amount_human(self.amount0, self.token0)} + "
            f"{format_token_amount_human(self.amount1, self.token1)} "
            f"({pool_with_type})"
        )
        # Range values are required by Intent but not used by Aerodrome (full range)
        return Intent.lp_open(
            pool=pool_with_type,
            amount0=self.amount0,
            amount1=self.amount1,
            range_lower=Decimal("1"),
            range_upper=Decimal("1000000"),
            protocol="aerodrome",
            chain=self.chain,
        )

    def _create_close_intent(self) -> Intent:
        """Create LP_CLOSE intent for Aerodrome."""
        pool_type = "stable" if self.stable else "volatile"
        pool_with_type = f"{self.pool}/{pool_type}"
        logger.info(f"LP_CLOSE: {pool_with_type}")
        return Intent.lp_close(
            position_id=pool_with_type,
            pool=pool_with_type,
            collect_fees=True,
            protocol="aerodrome",
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
            # Extract LP token balance from result if available
            if hasattr(result, "extracted_data") and result.extracted_data:
                liquidity = result.extracted_data.get("liquidity")
                if liquidity:
                    self._lp_token_balance = Decimal(str(liquidity))
            logger.info(f"LP position opened in {self.pool}")
            add_event(
                TimelineEvent(
                    timestamp=datetime.now(UTC),
                    event_type=TimelineEventType.POSITION_MODIFIED,
                    description=f"Opened {self.pool} LP position",
                    strategy_id=self.strategy_id,
                    details={"action": "lp_open", "pool": self.pool},
                )
            )

        elif intent_type_val == "LP_CLOSE":
            self._has_position = False
            self._lp_token_balance = Decimal("0")
            logger.info(f"LP position closed in {self.pool}")
            add_event(
                TimelineEvent(
                    timestamp=datetime.now(UTC),
                    event_type=TimelineEventType.POSITION_MODIFIED,
                    description=f"Closed {self.pool} LP position",
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
            "lp_token_balance": str(self._lp_token_balance),
            "ticks_with_position": self._ticks_with_position,
        }

    def load_persistent_state(self, state: dict[str, Any]) -> None:
        if "has_position" in state:
            self._has_position = bool(state["has_position"])
        if "lp_token_balance" in state:
            self._lp_token_balance = Decimal(str(state["lp_token_balance"]))
        if "ticks_with_position" in state:
            self._ticks_with_position = int(state["ticks_with_position"])
        logger.info(
            f"Restored state: has_position={self._has_position}, "
            f"ticks={self._ticks_with_position}"
        )

    # =========================================================================
    # TEARDOWN
    # =========================================================================

    def get_open_positions(self) -> "TeardownPositionSummary":
        from almanak.framework.teardown import PositionInfo, PositionType, TeardownPositionSummary

        positions = []
        if self._has_position or self._lp_token_balance > 0:
            positions.append(
                PositionInfo(
                    position_type=PositionType.LP,
                    position_id=f"aerodrome-lp-{self.pool.replace('/', '-')}",
                    chain=self.chain,
                    protocol="aerodrome",
                    value_usd=Decimal("0"),  # Would need on-chain query for real value
                    details={
                        "pool": self.pool,
                        "stable": self.stable,
                        "lp_balance": str(self._lp_token_balance),
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
        if self._has_position or self._lp_token_balance > 0:
            intents.append(self._create_close_intent())
        return intents
