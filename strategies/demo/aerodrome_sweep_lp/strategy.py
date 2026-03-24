"""
===============================================================================
DEMO: Aerodrome Sweep LP — Parameter-Sweepable LP on Base
===============================================================================

This demo strategy is the vehicle for testing the parameter sweep engine
(``almanak strat backtest sweep``) with LP-specific parameters on Base.

Prior parameter sweeps (VIB-579) only tested RSI swap strategies on Arbitrum
with buy/sell threshold parameters. LP strategies have fundamentally different
sweep dimensions: RSI entry/exit gates, LP sizing, and reentry cooldown. This
exercises the sweep engine with LP-specific parameter grids.

PURPOSE:
--------
1. Validate the parameter sweep pipeline with LP intents:
   - Grid search over RSI thresholds (entry/exit gating)
   - LP size variation (capital allocation optimization)
   - Reentry cooldown (timing between LP cycles)
2. First parameter sweep on Base (prior sweeps only on Arbitrum).
3. First parameter sweep with LP intents (prior sweeps only SWAP).

USAGE:
------
    # Sweep RSI thresholds
    almanak strat backtest sweep -s demo_aerodrome_sweep_lp \\
        --start 2024-01-01 --end 2024-06-01 \\
        --param "rsi_oversold:20,25,30,35,40" \\
        --param "rsi_overbought:60,65,70,75,80"

    # Sweep LP sizing and cooldown
    almanak strat backtest sweep -s demo_aerodrome_sweep_lp \\
        --start 2024-01-01 --end 2024-06-01 \\
        --param "amount0:0.0005,0.001,0.005" \\
        --param "reentry_cooldown:1,2,3,5"

    # Full grid: RSI + sizing (20 combinations)
    almanak strat backtest sweep -s demo_aerodrome_sweep_lp \\
        --start 2024-01-01 --end 2024-06-01 \\
        --param "rsi_oversold:25,30,35" \\
        --param "rsi_overbought:65,70,75" \\
        --param "amount0:0.001,0.005" \\
        --parallel 4

SWEEPABLE PARAMETERS:
---------------------
    rsi_oversold: RSI level to open LP (lower = more selective)
    rsi_overbought: RSI level to close LP (higher = longer holds)
    amount0: Token0 (WETH) amount per LP position
    amount1: Token1 (USDC) amount per LP position
    reentry_cooldown: Ticks to wait before re-opening after close
    range_width_pct: LP range width as % of current price (e.g. 10 = +/-5%)

STRATEGY LOGIC:
---------------
Each tick:
  1. Read RSI(ETH, configurable period)
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
    name="demo_aerodrome_sweep_lp",
    description="Parameter sweep demo — Aerodrome LP with sweepable RSI/sizing on Base",
    version="1.0.0",
    author="Almanak",
    tags=["demo", "sweep", "lp", "aerodrome", "base", "backtesting"],
    supported_chains=["base"],
    supported_protocols=["aerodrome"],
    intent_types=["LP_OPEN", "LP_CLOSE", "HOLD"],
)
class AerodromeSweepLPStrategy(IntentStrategy):
    """Aerodrome LP strategy with sweep-optimizable parameters.

    All configuration parameters can be overridden by the sweep engine via
    ``--param "name:val1,val2,val3"`` on the CLI.

    Configuration (config.json):
        pool: Pool pair (e.g. "WETH/USDC")
        stable: Pool type (true=stable, false=volatile)
        amount0: Token0 amount per LP (sweepable, e.g. "0.001")
        amount1: Token1 amount per LP (sweepable, e.g. "3")
        rsi_period: RSI calculation period (default: 14)
        rsi_oversold: RSI threshold for LP entry (sweepable, default: 30)
        rsi_overbought: RSI threshold for LP exit (sweepable, default: 70)
        reentry_cooldown: Ticks to wait before re-entering (sweepable, default: 2)
        max_lp_cycles: Max open/close cycles before stopping (default: 5)
        range_width_pct: LP range width as % of current price (sweepable, default: 0
            meaning full range). E.g. 10 = +/-5% around current price.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # Pool configuration
        self.pool = str(self.get_config("pool", "WETH/USDC"))
        pool_parts = self.pool.split("/")
        self.token0 = pool_parts[0] if len(pool_parts) > 0 else "WETH"
        self.token1 = pool_parts[1] if len(pool_parts) > 1 else "USDC"
        stable_cfg = self.get_config("stable", False)
        if isinstance(stable_cfg, str):
            self.stable = stable_cfg.strip().lower() in {"1", "true", "yes", "on"}
        else:
            self.stable = bool(stable_cfg)

        # Sweepable LP parameters
        self.amount0 = Decimal(str(self.get_config("amount0", "0.001")))
        self.amount1 = Decimal(str(self.get_config("amount1", "3")))

        # Sweepable RSI parameters
        self.rsi_period = int(self.get_config("rsi_period", 14))
        self.rsi_oversold = Decimal(str(self.get_config("rsi_oversold", "30")))
        self.rsi_overbought = Decimal(str(self.get_config("rsi_overbought", "70")))

        # Sweepable timing parameters
        self.reentry_cooldown = int(self.get_config("reentry_cooldown", 2))
        self.max_lp_cycles = int(self.get_config("max_lp_cycles", 5))

        # Sweepable range width parameter (0 = full range)
        self.range_width_pct = Decimal(str(self.get_config("range_width_pct", "0")))
        if self.range_width_pct < 0:
            raise ValueError(f"range_width_pct must be >= 0, got {self.range_width_pct}")

        # Internal state
        self._has_position = False
        self._lp_token_balance = Decimal("0")
        self._lp_cycles = 0
        self._cooldown_remaining = 0
        self._tick_count = 0
        self._ticks_with_position = 0
        self._current_price: Decimal | None = None

        pool_type = "stable" if self.stable else "volatile"
        range_info = f"range_width={self.range_width_pct}%" if self.range_width_pct > 0 else "full_range"
        logger.info(
            f"AerodromeSweepLP initialized: pool={self.pool} ({pool_type}), "
            f"amounts={self.amount0} {self.token0} + {self.amount1} {self.token1}, "
            f"RSI({self.rsi_period}) range=[{self.rsi_oversold}, {self.rsi_overbought}], "
            f"cooldown={self.reentry_cooldown}, max_cycles={self.max_lp_cycles}, "
            f"{range_info}"
        )

    # =========================================================================
    # MAIN DECISION LOGIC
    # =========================================================================

    def decide(self, market: MarketSnapshot) -> Intent | None:
        """RSI-gated LP with cooldown and cycle limit."""
        self._tick_count += 1

        # Capture current price for range width calculation
        try:
            self._current_price = market.price(self.token0)
        except (ValueError, KeyError, AttributeError):
            self._current_price = None  # Clear stale price; falls back to full range

        # Get RSI
        try:
            rsi = market.rsi(self.token0, period=self.rsi_period)
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
                bal0 = market.balance(self.token0)
                bal1 = market.balance(self.token1)
                has_funds = bal0.balance >= self.amount0 and bal1.balance >= self.amount1
            except (ValueError, KeyError):
                has_funds = False

            if has_funds:
                logger.info(
                    f"[tick {self._tick_count}] RSI in range ({rsi_value:.1f}), "
                    f"opening LP (cycle {self._lp_cycles + 1}/{self.max_lp_cycles})"
                )
                return self._create_open_intent()

            return Intent.hold(reason="Insufficient funds for LP")

        return Intent.hold(
            reason=f"RSI={rsi_value:.1f} outside entry range "
            f"[{self.rsi_oversold}, {self.rsi_overbought}]"
        )

    # =========================================================================
    # INTENT CREATION
    # =========================================================================

    def _create_open_intent(self) -> Intent:
        """Create LP_OPEN intent for Aerodrome.

        If range_width_pct > 0, computes concentrated range bounds around
        the current price. Otherwise uses full range (1 to 1,000,000).
        """
        pool_type = "stable" if self.stable else "volatile"
        pool_with_type = f"{self.pool}/{pool_type}"

        # Compute range bounds
        if self.range_width_pct > 0 and self._current_price and self._current_price > 0:
            half_width = self._current_price * self.range_width_pct / Decimal("200")
            range_lower = max(self._current_price - half_width, Decimal("0.01"))
            range_upper = self._current_price + half_width
            range_info = f"range=[{range_lower:.2f}, {range_upper:.2f}] ({self.range_width_pct}% width)"
        else:
            range_lower = Decimal("1")
            range_upper = Decimal("1000000")
            range_info = "full_range"

        logger.info(
            f"LP_OPEN: {format_token_amount_human(self.amount0, self.token0)} + "
            f"{format_token_amount_human(self.amount1, self.token1)} ({pool_with_type}) {range_info}"
        )
        return Intent.lp_open(
            pool=pool_with_type,
            amount0=self.amount0,
            amount1=self.amount1,
            range_lower=range_lower,
            range_upper=range_upper,
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
            if result and hasattr(result, "extracted_data") and result.extracted_data:
                liquidity = result.extracted_data.get("liquidity")
                if liquidity:
                    self._lp_token_balance = Decimal(str(liquidity))
            logger.info(f"LP opened in {self.pool} (cycle {self._lp_cycles})")
            add_event(
                TimelineEvent(
                    timestamp=datetime.now(UTC),
                    event_type=TimelineEventType.POSITION_MODIFIED,
                    description=f"Opened {self.pool} LP (cycle {self._lp_cycles})",
                    strategy_id=self.strategy_id,
                    details={
                        "action": "lp_open",
                        "pool": self.pool,
                        "cycle": self._lp_cycles,
                    },
                )
            )

        elif intent_type_val == "LP_CLOSE":
            self._has_position = False
            self._lp_token_balance = Decimal("0")
            self._cooldown_remaining = self.reentry_cooldown
            logger.info(
                f"LP closed in {self.pool}, cooldown={self.reentry_cooldown} ticks"
            )
            add_event(
                TimelineEvent(
                    timestamp=datetime.now(UTC),
                    event_type=TimelineEventType.POSITION_MODIFIED,
                    description=f"Closed {self.pool} LP, cooldown {self.reentry_cooldown}t",
                    strategy_id=self.strategy_id,
                    details={
                        "action": "lp_close",
                        "pool": self.pool,
                        "cooldown": self.reentry_cooldown,
                    },
                )
            )

    # =========================================================================
    # STATUS & PERSISTENCE
    # =========================================================================

    def get_status(self) -> dict[str, Any]:
        return {
            "strategy": "demo_aerodrome_sweep_lp",
            "chain": self.chain,
            "pool": self.pool,
            "has_position": self._has_position,
            "lp_cycles": self._lp_cycles,
            "cooldown_remaining": self._cooldown_remaining,
            "tick_count": self._tick_count,
            "rsi_oversold": str(self.rsi_oversold),
            "rsi_overbought": str(self.rsi_overbought),
            "reentry_cooldown": self.reentry_cooldown,
        }

    def get_persistent_state(self) -> dict[str, Any]:
        return {
            "has_position": self._has_position,
            "lp_token_balance": str(self._lp_token_balance),
            "lp_cycles": self._lp_cycles,
            "cooldown_remaining": self._cooldown_remaining,
            "tick_count": self._tick_count,
            "ticks_with_position": self._ticks_with_position,
        }

    def load_persistent_state(self, state: dict[str, Any]) -> None:
        if "has_position" in state:
            self._has_position = bool(state["has_position"])
        if "lp_token_balance" in state:
            self._lp_token_balance = Decimal(str(state["lp_token_balance"]))
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
        if self._has_position or self._lp_token_balance > 0:
            positions.append(
                PositionInfo(
                    position_type=PositionType.LP,
                    position_id=f"aerodrome-lp-{self.pool.replace('/', '-')}",
                    chain=self.chain,
                    protocol="aerodrome",
                    value_usd=Decimal("0"),
                    details={
                        "pool": self.pool,
                        "stable": self.stable,
                        "lp_balance": str(self._lp_token_balance),
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
        if self._has_position or self._lp_token_balance > 0:
            intents.append(self._create_close_intent())
        return intents
