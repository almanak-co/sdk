"""
===============================================================================
Aerodrome Mean-Reversion LP Strategy
===============================================================================

Provides liquidity on Aerodrome (Base) when the market is range-bound,
and exits when a trend develops -- capturing fees during mean-reversion
while avoiding impermanent loss during directional moves.

Decision Logic:
  - RSI between 40-60 (range-bound) -> open LP to earn swap fees
  - RSI < 30 or > 70 (trending)     -> close LP to avoid impermanent loss
  - RSI in between (neutral zone)    -> hold current position

This exploits a fundamental DeFi insight: LP positions profit most in
ranging markets where prices oscillate around a mean, and lose from IL
when prices trend directionally.

Chain: Base
Protocol: Aerodrome (Solidly-based AMM)
Pool: WETH/USDC volatile

USAGE:
    almanak strat run -d strategies/incubating/aerodrome_mean_reversion_lp --network anvil --once
===============================================================================
"""

import logging
from dataclasses import dataclass, field
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

if TYPE_CHECKING:
    from almanak.framework.teardown import TeardownMode, TeardownPositionSummary

logger = logging.getLogger(__name__)


# =============================================================================
# Configuration
# =============================================================================


@dataclass
class MeanReversionLPConfig:
    """Configuration for Aerodrome Mean-Reversion LP strategy."""

    pool: str = "WETH/USDC"
    stable: bool = False
    amount0: Decimal = field(default_factory=lambda: Decimal("0.001"))
    amount1: Decimal = field(default_factory=lambda: Decimal("3"))
    rsi_period: int = 14
    rsi_timeframe: str = "4h"
    rsi_lower: int = 40
    rsi_upper: int = 60
    force_action: str = ""

    def __post_init__(self):
        """Convert string values to proper types."""
        if isinstance(self.amount0, str):
            self.amount0 = Decimal(self.amount0)
        if isinstance(self.amount1, str):
            self.amount1 = Decimal(self.amount1)
        if isinstance(self.stable, str):
            self.stable = self.stable.lower() in ("true", "1", "yes")
        if isinstance(self.rsi_period, str):
            self.rsi_period = int(self.rsi_period)
        if isinstance(self.rsi_lower, str):
            self.rsi_lower = int(self.rsi_lower)
        if isinstance(self.rsi_upper, str):
            self.rsi_upper = int(self.rsi_upper)

    def to_dict(self) -> dict:
        """Convert config to dictionary for serialization."""
        return {
            "pool": self.pool,
            "stable": self.stable,
            "amount0": str(self.amount0),
            "amount1": str(self.amount1),
            "rsi_period": self.rsi_period,
            "rsi_timeframe": self.rsi_timeframe,
            "rsi_lower": self.rsi_lower,
            "rsi_upper": self.rsi_upper,
            "force_action": self.force_action,
        }


# =============================================================================
# Strategy
# =============================================================================


@almanak_strategy(
    name="aerodrome_mean_reversion_lp",
    description="Mean-reversion LP strategy on Aerodrome -- provides liquidity in range-bound markets, exits in trends",
    version="1.0.0",
    author="YAInnick Loop (Iteration 5)",
    tags=["incubating", "lp", "aerodrome", "base", "mean-reversion", "rsi"],
    supported_chains=["base"],
    supported_protocols=["aerodrome"],
    intent_types=["LP_OPEN", "LP_CLOSE", "HOLD"],
)
class AerodromeMeanReversionLP(IntentStrategy[MeanReversionLPConfig]):
    """Aerodrome LP strategy that uses RSI to time entry/exit.

    Opens LP in range-bound markets (RSI 40-60) where fee income
    dominates IL, and closes when a trend develops (RSI extreme).
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # Parse pool tokens
        pool_parts = self.config.pool.split("/")
        self.token0_symbol = pool_parts[0] if len(pool_parts) > 0 else "WETH"
        self.token1_symbol = pool_parts[1] if len(pool_parts) > 1 else "USDC"

        # State tracking
        self._has_position: bool = False
        self._lp_token_balance: Decimal = Decimal("0")
        self._entry_rsi: float | None = None

        pool_type = "stable" if self.config.stable else "volatile"
        logger.info(
            f"AerodromeMeanReversionLP initialized: "
            f"pool={self.config.pool} ({pool_type}), "
            f"amounts={self.config.amount0} {self.token0_symbol} + {self.config.amount1} {self.token1_symbol}, "
            f"RSI range=[{self.config.rsi_lower}-{self.config.rsi_upper}]"
        )

    # =========================================================================
    # Decision Logic
    # =========================================================================

    def decide(self, market: MarketSnapshot) -> Intent | None:
        """Decide whether to open, close, or hold LP based on RSI regime."""
        try:
            # Handle forced actions (for testing)
            force = self.config.force_action.lower() if self.config.force_action else ""
            if force == "open":
                logger.info("Forced action: OPEN LP position")
                return self._create_open_intent()
            if force == "close":
                logger.info("Forced action: CLOSE LP position")
                return self._create_close_intent()

            # Get RSI -- market.rsi() returns RSIData with .value (Decimal)
            try:
                rsi_data = market.rsi(
                    self.token0_symbol,
                    period=self.config.rsi_period,
                    timeframe=self.config.rsi_timeframe,
                )
                rsi = float(rsi_data.value)
                logger.info(f"RSI({self.config.rsi_period}, {self.config.rsi_timeframe}): {rsi:.2f}")
            except Exception as e:
                logger.warning(f"Could not calculate RSI: {e}. Holding.")
                return Intent.hold(reason=f"RSI unavailable: {e}")

            # Log RSI regime
            add_event(
                TimelineEvent(
                    timestamp=datetime.now(UTC),
                    event_type=TimelineEventType.STATE_CHANGE,
                    description=f"RSI={rsi:.2f}, position={'YES' if self._has_position else 'NO'}",
                    strategy_id=self.strategy_id,
                    details={"rsi": rsi, "has_position": self._has_position},
                )
            )

            # Decision logic
            if self._has_position:
                # We have LP -- check if we should close
                if rsi < 30 or rsi > 70:
                    logger.info(f"RSI={rsi:.2f} -- trending market, closing LP to avoid IL")
                    return self._create_close_intent()
                return Intent.hold(reason=f"RSI={rsi:.2f} -- market stable, keeping LP")

            # No position -- check if we should open
            if self.config.rsi_lower <= rsi <= self.config.rsi_upper:
                # Check balances
                try:
                    bal0 = market.balance(self.token0_symbol)
                    bal1 = market.balance(self.token1_symbol)
                    if bal0.balance < self.config.amount0:
                        return Intent.hold(
                            reason=f"Insufficient {self.token0_symbol}: {bal0.balance} < {self.config.amount0}"
                        )
                    if bal1.balance < self.config.amount1:
                        return Intent.hold(
                            reason=f"Insufficient {self.token1_symbol}: {bal1.balance} < {self.config.amount1}"
                        )
                except (ValueError, KeyError, AttributeError):
                    logger.warning("Could not verify balances, proceeding anyway")

                logger.info(f"RSI={rsi:.2f} -- range-bound market, opening LP for fee capture")
                self._entry_rsi = rsi
                return self._create_open_intent()

            # RSI in neutral zone (30-40 or 60-70) -- wait
            return Intent.hold(reason=f"RSI={rsi:.2f} -- neutral zone, waiting for clearer signal")

        except Exception as e:
            logger.exception(f"Error in decide(): {e}")
            return Intent.hold(reason=f"Error: {e}")

    # =========================================================================
    # Intent Creation
    # =========================================================================

    def _create_open_intent(self) -> Intent:
        """Create LP_OPEN intent for Aerodrome volatile/stable pool."""
        pool_type = "stable" if self.config.stable else "volatile"
        pool_with_type = f"{self.config.pool}/{pool_type}"

        logger.info(
            f"LP_OPEN: {format_token_amount_human(self.config.amount0, self.token0_symbol)} + "
            f"{format_token_amount_human(self.config.amount1, self.token1_symbol)}, "
            f"pool={pool_with_type}"
        )

        return Intent.lp_open(
            pool=pool_with_type,
            amount0=self.config.amount0,
            amount1=self.config.amount1,
            range_lower=Decimal("1"),
            range_upper=Decimal("1000000"),
            protocol="aerodrome",
        )

    def _create_close_intent(self) -> Intent:
        """Create LP_CLOSE intent for Aerodrome pool."""
        pool_type = "stable" if self.config.stable else "volatile"
        pool_with_type = f"{self.config.pool}/{pool_type}"

        logger.info(f"LP_CLOSE: {pool_with_type}")

        return Intent.lp_close(
            position_id=pool_with_type,
            pool=pool_with_type,
            collect_fees=True,
            protocol="aerodrome",
        )

    # =========================================================================
    # Lifecycle Hooks
    # =========================================================================

    def on_intent_executed(self, intent: Intent, success: bool, result: Any) -> None:
        """Track position state after execution."""
        if success and intent.intent_type.value == "LP_OPEN":
            logger.info("Aerodrome LP position opened successfully")
            self._has_position = True
            add_event(
                TimelineEvent(
                    timestamp=datetime.now(UTC),
                    event_type=TimelineEventType.POSITION_OPENED,
                    description=f"LP opened on {self.config.pool}",
                    strategy_id=self.strategy_id,
                    details={
                        "pool": self.config.pool,
                        "stable": self.config.stable,
                        "entry_rsi": self._entry_rsi,
                    },
                )
            )
        elif success and intent.intent_type.value == "LP_CLOSE":
            logger.info("Aerodrome LP position closed successfully")
            self._has_position = False
            self._lp_token_balance = Decimal("0")
            self._entry_rsi = None

    # =========================================================================
    # Status & Teardown
    # =========================================================================

    def get_status(self) -> dict[str, Any]:
        """Get current strategy status."""
        return {
            "strategy": "aerodrome_mean_reversion_lp",
            "chain": self.chain,
            "wallet": self.wallet_address[:10] + "..." if self.wallet_address else "N/A",
            "config": self.config.to_dict(),
            "state": {
                "has_position": self._has_position,
                "lp_token_balance": str(self._lp_token_balance),
                "entry_rsi": self._entry_rsi,
            },
        }

    def supports_teardown(self) -> bool:
        """Indicate that this strategy supports safe teardown."""
        return True

    def get_open_positions(self) -> "TeardownPositionSummary":
        """Get summary of open LP positions for teardown preview."""
        from almanak.framework.teardown import (
            PositionInfo,
            PositionType,
            TeardownPositionSummary,
        )

        positions: list[PositionInfo] = []

        if self._has_position:
            estimated_value = self.config.amount0 * Decimal("2500") + self.config.amount1

            positions.append(
                PositionInfo(
                    position_type=PositionType.LP,
                    position_id=f"aerodrome-lp-{self.config.pool}-{self.chain}",
                    chain=self.chain,
                    protocol="aerodrome",
                    value_usd=estimated_value,
                    details={
                        "asset": f"{self.token0_symbol}/{self.token1_symbol}",
                        "pool": self.config.pool,
                        "stable": self.config.stable,
                        "amount0": str(self.config.amount0),
                        "amount1": str(self.config.amount1),
                    },
                )
            )

        total_value = sum(p.value_usd for p in positions)

        return TeardownPositionSummary(
            strategy_id=self.strategy_id,
            timestamp=datetime.now(UTC),
            total_value_usd=total_value,
            positions=positions,
        )

    def generate_teardown_intents(self, mode: "TeardownMode", market=None) -> list[Intent]:
        """Generate intents to close all LP positions."""
        intents: list[Intent] = []

        if self._has_position:
            logger.info(f"Generating teardown intent for Aerodrome LP (mode={mode.value})")
            intents.append(self._create_close_intent())

        return intents
