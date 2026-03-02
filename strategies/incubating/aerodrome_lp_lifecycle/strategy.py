"""
===============================================================================
Aerodrome LP Lifecycle Strategy
===============================================================================

Full LP lifecycle on Aerodrome (Base): open a liquidity position, then close it.
Tests LPCloseIntent on Aerodrome's Solidly-style fungible LP tokens, which use
fundamentally different close mechanics than V3 NFT-based protocols.

Key Difference from V3 LP_CLOSE:
  Aerodrome: approve ERC-20 LP token + removeLiquidity (2 txs)
  V3 (Uniswap/SushiSwap): decreaseLiquidity + collect + burn (3 txs, NFT-based)

Decision Logic:
  - force_action=lifecycle: open on first call, close on second (for testing)
  - force_action=open:      always open LP
  - force_action=close:     always close LP
  - RSI mode:
    - RSI in [open_lower, open_upper] + no position -> LP_OPEN
    - RSI < close_lower or > close_upper + has position -> LP_CLOSE
    - Otherwise -> HOLD

Chain: Base
Protocol: Aerodrome (Solidly-based AMM)
Pool: WETH/USDC volatile

USAGE:
    # Full lifecycle test (open then close, use --interval for 2 iterations)
    almanak strat run -d strategies/incubating/aerodrome_lp_lifecycle --network anvil --interval 15

    # Test LP_OPEN only
    # Set force_action="open" in config.json
    almanak strat run -d strategies/incubating/aerodrome_lp_lifecycle --network anvil --once

    # Test LP_CLOSE only (requires LP tokens in wallet)
    # Set force_action="close" in config.json
    almanak strat run -d strategies/incubating/aerodrome_lp_lifecycle --network anvil --once
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
class LPLifecycleConfig:
    """Configuration for Aerodrome LP Lifecycle strategy."""

    # Pool config
    pool: str = "WETH/USDC"
    stable: bool = False
    amount0: Decimal = field(default_factory=lambda: Decimal("0.001"))
    amount1: Decimal = field(default_factory=lambda: Decimal("2"))

    # RSI signal parameters
    rsi_period: int = 14
    rsi_timeframe: str = "1h"
    rsi_open_lower: int = 40
    rsi_open_upper: int = 60
    rsi_close_lower: int = 30
    rsi_close_upper: int = 70

    # Testing
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
        if isinstance(self.rsi_open_lower, str):
            self.rsi_open_lower = int(self.rsi_open_lower)
        if isinstance(self.rsi_open_upper, str):
            self.rsi_open_upper = int(self.rsi_open_upper)
        if isinstance(self.rsi_close_lower, str):
            self.rsi_close_lower = int(self.rsi_close_lower)
        if isinstance(self.rsi_close_upper, str):
            self.rsi_close_upper = int(self.rsi_close_upper)

    def to_dict(self) -> dict:
        """Convert config to dictionary for serialization."""
        return {
            "pool": self.pool,
            "stable": self.stable,
            "amount0": str(self.amount0),
            "amount1": str(self.amount1),
            "rsi_period": self.rsi_period,
            "rsi_timeframe": self.rsi_timeframe,
            "rsi_open_lower": self.rsi_open_lower,
            "rsi_open_upper": self.rsi_open_upper,
            "rsi_close_lower": self.rsi_close_lower,
            "rsi_close_upper": self.rsi_close_upper,
            "force_action": self.force_action,
        }


# =============================================================================
# Strategy
# =============================================================================


@almanak_strategy(
    name="aerodrome_lp_lifecycle",
    description="Full LP lifecycle on Aerodrome -- open then close, testing Solidly-style fungible LP token mechanics",
    version="1.0.0",
    author="YAInnick Loop (Iteration 13)",
    tags=["incubating", "lp", "aerodrome", "base", "lifecycle", "lp-close"],
    supported_chains=["base"],
    supported_protocols=["aerodrome"],
    intent_types=["LP_OPEN", "LP_CLOSE", "HOLD"],
)
class AerodromeLPLifecycle(IntentStrategy[LPLifecycleConfig]):
    """Aerodrome LP lifecycle strategy testing full open -> close flow.

    The primary purpose is to stress-test LPCloseIntent on Aerodrome's
    Solidly-style fungible LP tokens. Unlike V3 protocols that use NFT
    position managers, Aerodrome's pool contract IS the LP token (ERC-20).

    Close flow: approve LP token for router -> removeLiquidity
    (vs V3: decreaseLiquidity -> collect -> burn on NFT position manager)
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # Parse pool tokens
        pool_parts = self.config.pool.split("/")
        self.token0_symbol = pool_parts[0] if len(pool_parts) > 0 else "WETH"
        self.token1_symbol = pool_parts[1] if len(pool_parts) > 1 else "USDC"

        # Lifecycle state (in-memory, not persisted)
        self._has_position: bool = False
        self._lifecycle_phase: str = "idle"  # idle -> opened -> closed
        self._entry_rsi: float | None = None

        pool_type = "stable" if self.config.stable else "volatile"
        logger.info(
            f"AerodromeLPLifecycle initialized: "
            f"pool={self.config.pool} ({pool_type}), "
            f"amounts={self.config.amount0} {self.token0_symbol} + {self.config.amount1} {self.token1_symbol}, "
            f"force_action={self.config.force_action or 'RSI-based'}"
        )

    # =========================================================================
    # Decision Logic
    # =========================================================================

    def decide(self, market: MarketSnapshot) -> Intent | None:
        """Decide whether to open, close, or hold LP."""
        try:
            force = self.config.force_action.lower() if self.config.force_action else ""

            # Forced action: open
            if force == "open":
                logger.info("Forced action: OPEN LP position")
                return self._create_open_intent()

            # Forced action: close
            if force == "close":
                logger.info("Forced action: CLOSE LP position")
                return self._create_close_intent()

            # Lifecycle mode: open on first call, close on second
            if force == "lifecycle":
                return self._handle_lifecycle_mode()

            # RSI-based mode
            return self._handle_rsi_mode(market)

        except Exception as e:
            logger.exception(f"Error in decide(): {e}")
            return Intent.hold(reason=f"Error: {e}")

    def _handle_lifecycle_mode(self) -> Intent:
        """Handle lifecycle mode: open -> close -> done."""
        if self._lifecycle_phase == "idle":
            logger.info("Lifecycle phase 1: OPEN LP position")
            return self._create_open_intent()

        if self._lifecycle_phase == "opened":
            logger.info("Lifecycle phase 2: CLOSE LP position")
            return self._create_close_intent()

        # Already completed the lifecycle
        logger.info("Lifecycle complete: both open and close executed")
        return Intent.hold(reason="Lifecycle complete -- open and close both executed successfully")

    def _handle_rsi_mode(self, market: MarketSnapshot) -> Intent:
        """Handle RSI-based LP management."""
        # Get RSI signal
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

        # Log state
        add_event(
            TimelineEvent(
                timestamp=datetime.now(UTC),
                event_type=TimelineEventType.STATE_CHANGE,
                description=f"RSI={rsi:.2f}, position={'YES' if self._has_position else 'NO'}",
                strategy_id=self.strategy_id,
                details={"rsi": rsi, "has_position": self._has_position},
            )
        )

        if self._has_position:
            # Check if we should close
            if rsi < self.config.rsi_close_lower or rsi > self.config.rsi_close_upper:
                logger.info(f"RSI={rsi:.2f} extreme -- closing LP to protect from IL")
                return self._create_close_intent()
            return Intent.hold(reason=f"RSI={rsi:.2f} in safe zone -- keeping LP")

        # No position -- check if we should open
        if self.config.rsi_open_lower <= rsi <= self.config.rsi_open_upper:
            # Verify balances
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

            logger.info(f"RSI={rsi:.2f} range-bound -- opening LP for fee capture")
            self._entry_rsi = rsi
            return self._create_open_intent()

        return Intent.hold(reason=f"RSI={rsi:.2f} -- neutral zone, waiting for signal")

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
        """Create LP_CLOSE intent for Aerodrome pool.

        For Aerodrome, position_id is "TOKEN0/TOKEN1/pool_type" (not an NFT ID).
        The compiler queries LP token balance on-chain via the pool address.
        """
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
            self._lifecycle_phase = "opened"

            add_event(
                TimelineEvent(
                    timestamp=datetime.now(UTC),
                    event_type=TimelineEventType.POSITION_OPENED,
                    description=f"Aerodrome LP opened on {self.config.pool}",
                    strategy_id=self.strategy_id,
                    details={
                        "pool": self.config.pool,
                        "stable": self.config.stable,
                        "amount0": str(self.config.amount0),
                        "amount1": str(self.config.amount1),
                    },
                )
            )

        elif success and intent.intent_type.value == "LP_CLOSE":
            logger.info("Aerodrome LP position closed successfully")
            self._has_position = False
            self._lifecycle_phase = "closed"
            self._entry_rsi = None

            add_event(
                TimelineEvent(
                    timestamp=datetime.now(UTC),
                    event_type=TimelineEventType.LP_CLOSE,
                    description="Aerodrome LP closed",
                    strategy_id=self.strategy_id,
                    details={"pool": self.config.pool},
                )
            )

        elif not success:
            logger.warning(
                f"Intent {intent.intent_type.value} failed: "
                f"{result.error if result and hasattr(result, 'error') else 'unknown error'}"
            )

    # =========================================================================
    # Status & Teardown
    # =========================================================================

    def get_status(self) -> dict[str, Any]:
        """Get current strategy status."""
        return {
            "strategy": "aerodrome_lp_lifecycle",
            "chain": self.chain,
            "wallet": self.wallet_address[:10] + "..." if self.wallet_address else "N/A",
            "config": self.config.to_dict(),
            "state": {
                "has_position": self._has_position,
                "lifecycle_phase": self._lifecycle_phase,
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
