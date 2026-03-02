"""
===============================================================================
TraderJoe V2 LP Lifecycle Strategy
===============================================================================

Full LP lifecycle on TraderJoe V2 Liquidity Book (Avalanche): open a bin-based
liquidity position, then close it. Tests LPCloseIntent on TraderJoe V2's
unique mechanics: approveForAll + removeLiquidity (fungible ERC1155 LP tokens)
vs V3's decreaseLiquidity + collect + burn (NFT positions).

Key Difference from V3 LP_CLOSE:
  TraderJoe V2: approveForAll (LBPair -> Router) + removeLiquidity (2 txs, bin-based)
  V3 (Uniswap/SushiSwap): decreaseLiquidity + collect + burn (3 txs, NFT-based)

Decision Logic:
  - force_action=lifecycle: open on first call, close on second (for testing)
  - force_action=open:      always open LP
  - force_action=close:     always close LP
  - RSI mode:
    - RSI in [open_lower, open_upper] + no position -> LP_OPEN
    - RSI < close_lower or > close_upper + has position -> LP_CLOSE
    - Otherwise -> HOLD

Chain: Avalanche
Protocol: TraderJoe V2 (Liquidity Book)
Pool: WAVAX/USDC binStep=20

USAGE:
    # Full lifecycle test (open then close, use --interval for 2 iterations)
    almanak strat run -d strategies/incubating/traderjoe_lp_close --network anvil --interval 15

    # Test LP_OPEN only
    # Set force_action="open" in config.json
    almanak strat run -d strategies/incubating/traderjoe_lp_close --network anvil --once

    # Test LP_CLOSE only (requires LP tokens in wallet from a prior open)
    # Set force_action="close" in config.json
    almanak strat run -d strategies/incubating/traderjoe_lp_close --network anvil --once
===============================================================================
"""

import logging
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Any

from almanak.framework.api.timeline import TimelineEvent, TimelineEventType, add_event
from almanak.framework.connectors.traderjoe_v2 import BIN_ID_OFFSET
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
    """Configuration for TraderJoe V2 LP Lifecycle strategy."""

    # Pool config (TOKEN_X/TOKEN_Y/BIN_STEP)
    pool: str = "WAVAX/USDC/20"
    range_width_pct: Decimal = field(default_factory=lambda: Decimal("0.10"))
    amount_x: Decimal = field(default_factory=lambda: Decimal("0.1"))
    amount_y: Decimal = field(default_factory=lambda: Decimal("3"))
    num_bins: int = 11

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
        if isinstance(self.range_width_pct, str):
            self.range_width_pct = Decimal(self.range_width_pct)
        if isinstance(self.amount_x, str):
            self.amount_x = Decimal(self.amount_x)
        if isinstance(self.amount_y, str):
            self.amount_y = Decimal(self.amount_y)
        if isinstance(self.num_bins, str):
            self.num_bins = int(self.num_bins)
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
            "range_width_pct": str(self.range_width_pct),
            "amount_x": str(self.amount_x),
            "amount_y": str(self.amount_y),
            "num_bins": self.num_bins,
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
    name="traderjoe_lp_lifecycle",
    description="Full LP lifecycle on TraderJoe V2 -- open bin-based position then close, testing Liquidity Book LP_CLOSE mechanics",
    version="1.0.0",
    author="YAInnick Loop (Iteration 13)",
    tags=["incubating", "lp", "traderjoe-v2", "avalanche", "lifecycle", "lp-close"],
    supported_chains=["avalanche"],
    supported_protocols=["traderjoe_v2"],
    intent_types=["LP_OPEN", "LP_CLOSE", "HOLD"],
)
class TraderJoeLPLifecycle(IntentStrategy[LPLifecycleConfig]):
    """TraderJoe V2 LP lifecycle strategy testing full open -> close flow.

    The primary purpose is to stress-test LPCloseIntent on TraderJoe V2's
    Liquidity Book. Unlike V3 protocols that use NFT position managers,
    TraderJoe V2 uses fungible ERC1155 LP tokens per bin.

    Close flow: approveForAll (LBPair -> Router) -> removeLiquidity
    (vs V3: decreaseLiquidity -> collect -> burn on NFT position manager)
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # Parse pool config: TOKEN_X/TOKEN_Y/BIN_STEP
        pool_parts = self.config.pool.split("/")
        self.token_x_symbol = pool_parts[0] if len(pool_parts) > 0 else "WAVAX"
        self.token_y_symbol = pool_parts[1] if len(pool_parts) > 1 else "USDC"
        self.bin_step = int(pool_parts[2]) if len(pool_parts) > 2 else 20

        # Lifecycle state (in-memory, not persisted)
        self._has_position: bool = False
        self._lifecycle_phase: str = "idle"  # idle -> opened -> closed
        self._position_bin_ids: list[int] = []
        self._entry_rsi: float | None = None

        logger.info(
            f"TraderJoeLPLifecycle initialized: "
            f"pool={self.config.pool}, "
            f"range_width={self.config.range_width_pct * 100}%, "
            f"amounts={self.config.amount_x} {self.token_x_symbol} + {self.config.amount_y} {self.token_y_symbol}, "
            f"bins={self.config.num_bins}, "
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
                current_price = self._get_current_price(market)
                return self._create_open_intent(current_price)

            # Forced action: close
            if force == "close":
                logger.info("Forced action: CLOSE LP position (adapter will query on-chain)")
                return self._create_close_intent()

            # Lifecycle mode: open on first call, close on second
            if force == "lifecycle":
                return self._handle_lifecycle_mode(market)

            # RSI-based mode
            return self._handle_rsi_mode(market)

        except Exception as e:
            logger.exception(f"Error in decide(): {e}")
            return Intent.hold(reason=f"Error: {e}")

    def _handle_lifecycle_mode(self, market: MarketSnapshot) -> Intent:
        """Handle lifecycle mode: open -> close -> done."""
        if self._lifecycle_phase == "idle":
            logger.info("Lifecycle phase 1: OPEN LP position")
            current_price = self._get_current_price(market)
            return self._create_open_intent(current_price)

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
                self.token_x_symbol,
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
                bal_x = market.balance(self.token_x_symbol)
                bal_y = market.balance(self.token_y_symbol)
                if bal_x.balance < self.config.amount_x:
                    return Intent.hold(
                        reason=f"Insufficient {self.token_x_symbol}: {bal_x.balance} < {self.config.amount_x}"
                    )
                if bal_y.balance < self.config.amount_y:
                    return Intent.hold(
                        reason=f"Insufficient {self.token_y_symbol}: {bal_y.balance} < {self.config.amount_y}"
                    )
            except (ValueError, KeyError, AttributeError):
                logger.warning("Could not verify balances, proceeding anyway")

            logger.info(f"RSI={rsi:.2f} range-bound -- opening LP for fee capture")
            self._entry_rsi = rsi
            current_price = self._get_current_price(market)
            return self._create_open_intent(current_price)

        return Intent.hold(reason=f"RSI={rsi:.2f} -- neutral zone, waiting for signal")

    # =========================================================================
    # Intent Creation
    # =========================================================================

    def _get_current_price(self, market: MarketSnapshot) -> Decimal:
        """Get current pool price (token_y per token_x, e.g., USDC per WAVAX)."""
        try:
            price_x_usd = market.price(self.token_x_symbol)
            price_y_usd = market.price(self.token_y_symbol)
            if price_y_usd == Decimal("0"):
                logger.warning(f"{self.token_y_symbol} price is zero, using default")
                return Decimal("30")
            return price_x_usd / price_y_usd
        except (ValueError, KeyError) as e:
            logger.warning(f"Could not get price: {e}, using default")
            return Decimal("30")

    def _create_open_intent(self, current_price: Decimal) -> Intent:
        """Create LP_OPEN intent with bin range around current price.

        The intent compiler handles conversion to bin-based parameters.
        We specify a price range and the compiler + adapter translate
        that to delta_ids and distribution arrays.
        """
        half_width = self.config.range_width_pct / Decimal("2")
        range_lower = current_price * (Decimal("1") - half_width)
        range_upper = current_price * (Decimal("1") + half_width)

        logger.info(
            f"LP_OPEN: {format_token_amount_human(self.config.amount_x, self.token_x_symbol)} + "
            f"{format_token_amount_human(self.config.amount_y, self.token_y_symbol)}, "
            f"price range [{range_lower:.4f} - {range_upper:.4f}], bin_step={self.bin_step}"
        )

        return Intent.lp_open(
            pool=self.config.pool,
            amount0=self.config.amount_x,
            amount1=self.config.amount_y,
            range_lower=range_lower,
            range_upper=range_upper,
            protocol="traderjoe_v2",
        )

    def _create_close_intent(self) -> Intent:
        """Create LP_CLOSE intent for TraderJoe V2.

        For TraderJoe V2, position_id is the pool identifier (TOKEN_X/TOKEN_Y/BIN_STEP).
        The compiler queries LP token balances per bin via the adapter.
        """
        logger.info(f"LP_CLOSE: pool={self.config.pool}, bins={self._position_bin_ids}")

        return Intent.lp_close(
            position_id=self.config.pool,
            pool=self.config.pool,
            collect_fees=True,
            protocol="traderjoe_v2",
        )

    # =========================================================================
    # Lifecycle Hooks
    # =========================================================================

    def on_intent_executed(self, intent: Intent, success: bool, result: Any) -> None:
        """Track position state after execution."""
        if success and intent.intent_type.value == "LP_OPEN":
            logger.info("TraderJoe V2 LP position opened successfully")
            self._has_position = True
            self._lifecycle_phase = "opened"

            # Extract bin IDs from result enrichment if available
            bin_ids = getattr(result, "bin_ids", None) if result else None
            if bin_ids:
                self._position_bin_ids = list(bin_ids)
                logger.info(f"Position bin_ids: {bin_ids[:5]}...")
            else:
                logger.info("LP_OPEN succeeded (no bin_ids extracted from result)")

            add_event(
                TimelineEvent(
                    timestamp=datetime.now(UTC),
                    event_type=TimelineEventType.LP_OPEN,
                    description=f"TraderJoe V2 LP opened on {self.config.pool}",
                    strategy_id=self.strategy_id,
                    details={
                        "pool": self.config.pool,
                        "bin_step": self.bin_step,
                        "amount_x": str(self.config.amount_x),
                        "amount_y": str(self.config.amount_y),
                        "bin_ids": bin_ids,
                    },
                )
            )

        elif success and intent.intent_type.value == "LP_CLOSE":
            logger.info("TraderJoe V2 LP position closed successfully")
            self._has_position = False
            self._lifecycle_phase = "closed"
            self._position_bin_ids = []
            self._entry_rsi = None

            add_event(
                TimelineEvent(
                    timestamp=datetime.now(UTC),
                    event_type=TimelineEventType.LP_CLOSE,
                    description="TraderJoe V2 LP closed",
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
            "strategy": "traderjoe_lp_lifecycle",
            "chain": self.chain,
            "wallet": self.wallet_address[:10] + "..." if self.wallet_address else "N/A",
            "config": self.config.to_dict(),
            "state": {
                "has_position": self._has_position,
                "lifecycle_phase": self._lifecycle_phase,
                "position_bin_ids": self._position_bin_ids[:5] if self._position_bin_ids else [],
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
            # Estimate value from config amounts
            avax_price_usd = Decimal("30")
            estimated_value = self.config.amount_x * avax_price_usd + self.config.amount_y

            positions.append(
                PositionInfo(
                    position_type=PositionType.LP,
                    position_id=f"traderjoe-lp-{self.config.pool}-{self.chain}",
                    chain=self.chain,
                    protocol="traderjoe_v2",
                    value_usd=estimated_value,
                    details={
                        "asset": f"{self.token_x_symbol}/{self.token_y_symbol}",
                        "pool": self.config.pool,
                        "bin_step": self.bin_step,
                        "num_bins": len(self._position_bin_ids),
                        "bin_ids": self._position_bin_ids[:5],
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
            logger.info(f"Generating teardown intent for TraderJoe V2 LP (mode={mode.value})")
            intents.append(self._create_close_intent())

        return intents
