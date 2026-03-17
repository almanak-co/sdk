"""
===============================================================================
DEMO: TraderJoe Sweep LP — Parameter-Sweepable Liquidity Book LP on Avalanche
===============================================================================

This demo strategy is the vehicle for testing the parameter sweep engine
(``almanak strat backtest sweep``) with TraderJoe V2 Liquidity Book LP on
Avalanche.

Prior parameter sweeps tested RSI swap strategies on Arbitrum (VIB-579) and
Aerodrome LP on Base (VIB-1360). This is the first sweep on Avalanche and
the first with TraderJoe V2's discrete-bin LP model (non-tick-based),
validating that the sweep engine handles bin-width and bin-count grid search.

PURPOSE:
--------
1. First parameter sweep on Avalanche (new chain coverage).
2. First sweep with TraderJoe V2 Liquidity Book (bin-based LP, not ticks).
3. Sweep LP-specific parameters: range width, bin count, RSI gates.

USAGE:
------
    # Sweep range width and RSI thresholds
    almanak strat backtest sweep -s demo_traderjoe_sweep_lp \
        --start 2024-01-01 --end 2024-06-01 \
        --param "range_width_pct:0.05,0.10,0.15,0.20" \
        --param "rsi_oversold:25,30,35"

    # Sweep reentry cooldown and RSI overbought
    almanak strat backtest sweep -s demo_traderjoe_sweep_lp \
        --start 2024-01-01 --end 2024-06-01 \
        --param "reentry_cooldown:1,2,3,5" \
        --param "rsi_overbought:65,70,75,80"

    # Full grid: range + RSI (16 combinations)
    almanak strat backtest sweep -s demo_traderjoe_sweep_lp \
        --start 2024-01-01 --end 2024-06-01 \
        --param "range_width_pct:0.05,0.10,0.15,0.20" \
        --param "rsi_oversold:25,30,35,40" \
        --parallel 4

SWEEPABLE PARAMETERS:
---------------------
    range_width_pct: Total width of LP range (0.10 = 10%, i.e. +/-5%)
    num_bins: Number of Liquidity Book bins (informational; compiler uses fixed bin range)
    rsi_oversold: RSI level to open LP (lower = more selective)
    rsi_overbought: RSI level to close LP (higher = longer holds)
    amount_x: Token X (WAVAX) amount per LP position
    amount_y: Token Y (USDC) amount per LP position
    reentry_cooldown: Ticks to wait before re-opening after close

STRATEGY LOGIC:
---------------
Each tick:
  1. Read RSI(WAVAX, configurable period)
  2. If no LP and RSI in range and cooldown elapsed -> open LP
  3. If has LP and RSI extreme -> close LP, start cooldown
  4. Track cycle count against max_lp_cycles
  5. Otherwise -> hold
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
    name="demo_traderjoe_sweep_lp",
    description="Parameter sweep demo — TraderJoe V2 Liquidity Book LP with sweepable range/RSI on Avalanche",
    version="1.0.0",
    author="Almanak",
    tags=["demo", "sweep", "lp", "traderjoe-v2", "avalanche", "backtesting", "liquidity-book"],
    supported_chains=["avalanche"],
    supported_protocols=["traderjoe_v2"],
    intent_types=["LP_OPEN", "LP_CLOSE", "HOLD"],
)
class TraderJoeSweepLPStrategy(IntentStrategy):
    """TraderJoe V2 Liquidity Book LP strategy with sweep-optimizable parameters.

    All configuration parameters can be overridden by the sweep engine via
    ``--param "name:val1,val2,val3"`` on the CLI.

    Configuration (config.json):
        pool: Pool identifier (e.g. "WAVAX/USDC/20")
        range_width_pct: Total width of LP range (sweepable, e.g. 0.10 = 10%)
        num_bins: Number of discrete bins (sweepable, e.g. 11)
        amount_x: Token X (WAVAX) amount per LP (sweepable)
        amount_y: Token Y (USDC) amount per LP (sweepable)
        rsi_period: RSI calculation period (default: 14)
        rsi_oversold: RSI threshold for LP entry (sweepable, default: 30)
        rsi_overbought: RSI threshold for LP exit (sweepable, default: 70)
        reentry_cooldown: Ticks to wait before re-entering (sweepable, default: 2)
        max_lp_cycles: Max open/close cycles before stopping (default: 5)
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # Pool configuration
        self.pool = str(self.get_config("pool", "WAVAX/USDC/20"))
        pool_parts = self.pool.split("/")
        self.token_x = pool_parts[0] if len(pool_parts) > 0 else "WAVAX"
        self.token_y = pool_parts[1] if len(pool_parts) > 1 else "USDC"
        self.bin_step = int(pool_parts[2]) if len(pool_parts) > 2 else 20

        # Sweepable LP parameters
        self.range_width_pct = Decimal(str(self.get_config("range_width_pct", "0.10")))
        self.num_bins = int(self.get_config("num_bins", 11))
        self.amount_x = Decimal(str(self.get_config("amount_x", "0.5")))
        self.amount_y = Decimal(str(self.get_config("amount_y", "15")))

        # Sweepable RSI parameters
        self.rsi_period = int(self.get_config("rsi_period", 14))
        self.rsi_oversold = Decimal(str(self.get_config("rsi_oversold", "30")))
        self.rsi_overbought = Decimal(str(self.get_config("rsi_overbought", "70")))

        # Sweepable timing parameters
        self.reentry_cooldown = int(self.get_config("reentry_cooldown", 2))
        self.max_lp_cycles = int(self.get_config("max_lp_cycles", 5))

        # Internal state
        self._has_position = False
        self._lp_cycles = 0
        self._cooldown_remaining = 0
        self._tick_count = 0
        self._ticks_with_position = 0

        logger.info(
            f"TraderJoeSweepLP initialized: pool={self.pool}, "
            f"range_width={self.range_width_pct * 100}%, bins={self.num_bins}, "
            f"amounts={self.amount_x} {self.token_x} + {self.amount_y} {self.token_y}, "
            f"RSI({self.rsi_period}) range=[{self.rsi_oversold}, {self.rsi_overbought}], "
            f"cooldown={self.reentry_cooldown}, max_cycles={self.max_lp_cycles}"
        )

    # =========================================================================
    # MAIN DECISION LOGIC
    # =========================================================================

    def decide(self, market: MarketSnapshot) -> Intent | None:
        """RSI-gated Liquidity Book LP with cooldown and cycle limit."""
        self._tick_count += 1

        # Get current price for range calculation
        try:
            token_x_price = market.price(self.token_x)
            token_y_price = market.price(self.token_y)
            current_price = token_x_price / token_y_price
        except (ValueError, KeyError, ZeroDivisionError) as e:
            return Intent.hold(reason=f"Price data unavailable: {e}")

        # Get RSI
        try:
            rsi = market.rsi(self.token_x, period=self.rsi_period)
            rsi_value = rsi.value
        except (ValueError, KeyError, AttributeError) as e:
            logger.warning(f"RSI data unavailable: {e}")
            return Intent.hold(reason=f"RSI data unavailable: {e}")

        rsi_in_range = self.rsi_oversold <= rsi_value <= self.rsi_overbought
        rsi_extreme = rsi_value < self.rsi_oversold or rsi_value > self.rsi_overbought

        # If we have a position
        if self._has_position:
            self._ticks_with_position += 1

            if rsi_extreme:
                logger.info(
                    f"[tick {self._tick_count}] RSI extreme ({rsi_value:.1f}), "
                    f"closing LP after {self._ticks_with_position} ticks "
                    f"(cycle {self._lp_cycles}/{self.max_lp_cycles})"
                )
                return self._create_close_intent()

            return Intent.hold(
                reason=f"LP active ({self._ticks_with_position} ticks), "
                f"RSI={rsi_value:.1f} in range"
            )

        # No position: check cooldown
        if self._cooldown_remaining > 0:
            self._cooldown_remaining -= 1
            return Intent.hold(
                reason=f"Cooldown ({self._cooldown_remaining + 1} ticks remaining)"
            )

        # Check cycle limit
        if self._lp_cycles >= self.max_lp_cycles:
            return Intent.hold(
                reason=f"Max LP cycles ({self.max_lp_cycles}) reached"
            )

        # Check if we should open
        if rsi_in_range:
            try:
                bal_x = market.balance(self.token_x)
                bal_y = market.balance(self.token_y)
                has_funds = bal_x.balance >= self.amount_x and bal_y.balance >= self.amount_y
            except (ValueError, KeyError):
                has_funds = False

            if has_funds:
                logger.info(
                    f"[tick {self._tick_count}] RSI in range ({rsi_value:.1f}), "
                    f"opening LP (cycle {self._lp_cycles + 1}/{self.max_lp_cycles})"
                )
                return self._create_open_intent(current_price)

            return Intent.hold(reason="Insufficient funds for LP")

        return Intent.hold(
            reason=f"RSI={rsi_value:.1f} outside entry range "
            f"[{self.rsi_oversold}, {self.rsi_overbought}]"
        )

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
            f"range [{range_lower:.4f} - {range_upper:.4f}], "
            f"bins={self.num_bins}, bin_step={self.bin_step}"
        )
        # Derive bin_range (bins on each side of active bin) from num_bins.
        # The compiler reads protocol_params["bin_range"] to determine the
        # actual on-chain bin layout, so this is what makes num_bins sweepable.
        bin_range = max(1, self.num_bins // 2)
        return Intent.lp_open(
            pool=self.pool,
            amount0=self.amount_x,
            amount1=self.amount_y,
            range_lower=range_lower,
            range_upper=range_upper,
            protocol="traderjoe_v2",
            chain=self.chain,
            protocol_params={"bin_range": bin_range},
        )

    def _create_close_intent(self) -> Intent:
        """Create LP_CLOSE intent for TraderJoe V2 Liquidity Book."""
        logger.info(f"LP_CLOSE: {self.pool}")
        return Intent.lp_close(
            position_id=f"traderjoe-lp-{self.pool.replace('/', '-')}",
            pool=self.pool,
            collect_fees=True,
            protocol="traderjoe_v2",
            chain=self.chain,
        )

    # =========================================================================
    # LIFECYCLE HOOKS
    # =========================================================================

    def on_intent_executed(self, intent: Any, success: bool, result: Any) -> None:
        """Track LP position state and cycles from execution results."""
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
            self._lp_cycles += 1

            bin_ids = getattr(result, "bin_ids", None) if result else None
            logger.info(
                f"LP opened in {self.pool} (cycle {self._lp_cycles})"
                + (f", bins={bin_ids[:3]}..." if bin_ids else "")
            )
            add_event(
                TimelineEvent(
                    timestamp=datetime.now(UTC),
                    event_type=TimelineEventType.POSITION_MODIFIED,
                    description=f"Opened {self.pool} TraderJoe LP (cycle {self._lp_cycles})",
                    strategy_id=self.strategy_id,
                    details={"action": "lp_open", "pool": self.pool, "cycle": self._lp_cycles},
                )
            )

        elif intent_type_val == "LP_CLOSE":
            self._has_position = False
            self._cooldown_remaining = self.reentry_cooldown
            logger.info(f"LP closed in {self.pool}, cooldown={self.reentry_cooldown} ticks")
            add_event(
                TimelineEvent(
                    timestamp=datetime.now(UTC),
                    event_type=TimelineEventType.POSITION_MODIFIED,
                    description=f"Closed {self.pool} TraderJoe LP, cooldown {self.reentry_cooldown}t",
                    strategy_id=self.strategy_id,
                    details={"action": "lp_close", "pool": self.pool, "cooldown": self.reentry_cooldown},
                )
            )

    # =========================================================================
    # STATUS & PERSISTENCE
    # =========================================================================

    def get_status(self) -> dict[str, Any]:
        return {
            "strategy": "demo_traderjoe_sweep_lp",
            "chain": self.chain,
            "pool": self.pool,
            "has_position": self._has_position,
            "lp_cycles": self._lp_cycles,
            "cooldown_remaining": self._cooldown_remaining,
            "tick_count": self._tick_count,
            "range_width_pct": str(self.range_width_pct),
            "num_bins": self.num_bins,
            "rsi_oversold": str(self.rsi_oversold),
            "rsi_overbought": str(self.rsi_overbought),
            "reentry_cooldown": self.reentry_cooldown,
        }

    def get_persistent_state(self) -> dict[str, Any]:
        return {
            "has_position": self._has_position,
            "lp_cycles": self._lp_cycles,
            "cooldown_remaining": self._cooldown_remaining,
            "tick_count": self._tick_count,
            "ticks_with_position": self._ticks_with_position,
        }

    def load_persistent_state(self, state: dict[str, Any]) -> None:
        if "has_position" in state:
            self._has_position = bool(state["has_position"])
        if "lp_cycles" in state:
            self._lp_cycles = int(state["lp_cycles"])
        if "cooldown_remaining" in state:
            self._cooldown_remaining = int(state["cooldown_remaining"])
        if "tick_count" in state:
            self._tick_count = int(state["tick_count"])
        if "ticks_with_position" in state:
            self._ticks_with_position = int(state["ticks_with_position"])
        logger.info(
            f"Restored state: has_position={self._has_position}, "
            f"cycles={self._lp_cycles}, cooldown={self._cooldown_remaining}"
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
                    position_id=f"traderjoe-lp-{self.pool.replace('/', '-')}",
                    chain=self.chain,
                    protocol="traderjoe_v2",
                    value_usd=Decimal("0"),
                    details={
                        "pool": self.pool,
                        "bin_step": self.bin_step,
                        "range_width_pct": str(self.range_width_pct),
                        "num_bins": self.num_bins,
                        "cycle": self._lp_cycles,
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
