"""
===============================================================================
TraderJoe V2 ATR-Adaptive LP Strategy
===============================================================================

Provides liquidity on TraderJoe V2 (Avalanche) with range width dynamically
sized by ATR (Average True Range). Low volatility -> tight range for maximum
fee capture. High volatility -> wide range or exit to avoid impermanent loss.

Decision Logic:
  - ATR% < 2% (low vol)  -> open LP with tight range (5% width)
  - ATR% 2-5% (normal)   -> open LP with normal range (10% width)
  - ATR% > 5% (high vol) -> close LP or hold (IL risk too high)

This exploits the TraderJoe V2 Liquidity Book's discrete bins: concentrating
liquidity in fewer bins during calm periods earns disproportionately more
swap fees, while widening during volatile periods reduces IL exposure.

Chain: Avalanche
Protocol: TraderJoe V2 (Liquidity Book)
Pool: WAVAX/USDC (bin_step=20)

USAGE:
    almanak strat run -d strategies/incubating/traderjoe_atr_lp --network anvil --once
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
class ATRAdaptiveLPConfig:
    """Configuration for TraderJoe V2 ATR-Adaptive LP strategy."""

    pool: str = "WAVAX/USDC/20"
    amount_x: Decimal = field(default_factory=lambda: Decimal("1"))
    amount_y: Decimal = field(default_factory=lambda: Decimal("30"))

    # ATR indicator settings
    atr_period: int = 14
    atr_timeframe: str = "4h"
    atr_low_pct: Decimal = field(default_factory=lambda: Decimal("2.0"))
    atr_high_pct: Decimal = field(default_factory=lambda: Decimal("5.0"))

    # Range width per volatility regime (total width, not half-width)
    range_tight_pct: Decimal = field(default_factory=lambda: Decimal("0.05"))
    range_normal_pct: Decimal = field(default_factory=lambda: Decimal("0.10"))

    force_action: str = ""

    def __post_init__(self):
        """Convert string values to proper types."""
        if isinstance(self.amount_x, str):
            self.amount_x = Decimal(self.amount_x)
        if isinstance(self.amount_y, str):
            self.amount_y = Decimal(self.amount_y)
        if isinstance(self.atr_low_pct, str):
            self.atr_low_pct = Decimal(self.atr_low_pct)
        if isinstance(self.atr_high_pct, str):
            self.atr_high_pct = Decimal(self.atr_high_pct)
        if isinstance(self.range_tight_pct, str):
            self.range_tight_pct = Decimal(self.range_tight_pct)
        if isinstance(self.range_normal_pct, str):
            self.range_normal_pct = Decimal(self.range_normal_pct)
        if isinstance(self.atr_period, str):
            self.atr_period = int(self.atr_period)

    def to_dict(self) -> dict:
        """Convert config to dictionary for serialization."""
        return {
            "pool": self.pool,
            "amount_x": str(self.amount_x),
            "amount_y": str(self.amount_y),
            "atr_period": self.atr_period,
            "atr_timeframe": self.atr_timeframe,
            "atr_low_pct": str(self.atr_low_pct),
            "atr_high_pct": str(self.atr_high_pct),
            "range_tight_pct": str(self.range_tight_pct),
            "range_normal_pct": str(self.range_normal_pct),
            "force_action": self.force_action,
        }


# =============================================================================
# Strategy
# =============================================================================


@almanak_strategy(
    name="traderjoe_atr_lp",
    description="ATR-adaptive LP on TraderJoe V2 -- tight range in low vol, wide range in normal vol, exit in high vol",
    version="1.0.0",
    author="YAInnick Loop (Iteration 6)",
    tags=["incubating", "lp", "traderjoe-v2", "avalanche", "atr", "volatility-adaptive"],
    supported_chains=["avalanche"],
    supported_protocols=["traderjoe_v2"],
    intent_types=["LP_OPEN", "LP_CLOSE", "HOLD"],
)
class TraderJoeATRAdaptiveLP(IntentStrategy[ATRAdaptiveLPConfig]):
    """TraderJoe V2 LP strategy that adapts range width to volatility via ATR.

    Concentrates liquidity during calm markets for maximum fee capture,
    widens during moderate volatility for IL protection, and exits
    entirely when volatility exceeds safe thresholds.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # Parse pool tokens: "WAVAX/USDC/20" -> token_x, token_y, bin_step
        pool_parts = self.config.pool.split("/")
        self.token_x_symbol = pool_parts[0] if len(pool_parts) > 0 else "WAVAX"
        self.token_y_symbol = pool_parts[1] if len(pool_parts) > 1 else "USDC"
        self.bin_step = int(pool_parts[2]) if len(pool_parts) > 2 else 20

        # State tracking
        self._has_position: bool = False
        self._position_bin_ids: list[int] = []
        self._entry_atr_pct: float | None = None
        self._entry_range_width: Decimal | None = None

        logger.info(
            f"TraderJoeATRAdaptiveLP initialized: "
            f"pool={self.config.pool}, "
            f"amounts={self.config.amount_x} {self.token_x_symbol} + {self.config.amount_y} {self.token_y_symbol}, "
            f"ATR thresholds=[<{self.config.atr_low_pct}% tight, "
            f"{self.config.atr_low_pct}-{self.config.atr_high_pct}% normal, "
            f">{self.config.atr_high_pct}% exit]"
        )

    # =========================================================================
    # Decision Logic
    # =========================================================================

    def decide(self, market: MarketSnapshot) -> Intent | None:
        """Decide whether to open, close, or hold LP based on ATR volatility regime."""
        try:
            # Handle forced actions (for testing)
            force = self.config.force_action.lower() if self.config.force_action else ""
            if force == "open":
                logger.info("Forced action: OPEN LP position")
                return self._create_open_intent(market, self.config.range_normal_pct)
            if force == "close":
                logger.info("Forced action: CLOSE LP position")
                return self._create_close_intent()

            # Get ATR -- returns ATRData with .value_percent (Decimal)
            try:
                atr_data = market.atr(
                    self.token_x_symbol,
                    period=self.config.atr_period,
                    timeframe=self.config.atr_timeframe,
                )
                atr_pct = float(atr_data.value_percent)
                logger.info(
                    f"ATR({self.config.atr_period}, {self.config.atr_timeframe}): "
                    f"{atr_pct:.2f}% (value={float(atr_data.value):.4f})"
                )
            except Exception as e:
                logger.warning(f"Could not calculate ATR: {e}. Holding.")
                return Intent.hold(reason=f"ATR unavailable: {e}")

            # Determine volatility regime
            if atr_pct < float(self.config.atr_low_pct):
                regime = "LOW_VOL"
                range_width = self.config.range_tight_pct
            elif atr_pct <= float(self.config.atr_high_pct):
                regime = "NORMAL_VOL"
                range_width = self.config.range_normal_pct
            else:
                regime = "HIGH_VOL"
                range_width = None  # Signal to exit

            # Log volatility regime
            add_event(
                TimelineEvent(
                    timestamp=datetime.now(UTC),
                    event_type=TimelineEventType.STATE_CHANGE,
                    description=f"ATR={atr_pct:.2f}%, regime={regime}, position={'YES' if self._has_position else 'NO'}",
                    strategy_id=self.strategy_id,
                    details={"atr_pct": atr_pct, "regime": regime, "has_position": self._has_position},
                )
            )

            # Decision logic
            if self._has_position:
                if regime == "HIGH_VOL":
                    logger.info(f"ATR={atr_pct:.2f}% -- high volatility, closing LP to avoid IL")
                    return self._create_close_intent()
                return Intent.hold(
                    reason=f"ATR={atr_pct:.2f}% ({regime}) -- keeping LP position"
                )

            # No position
            if regime == "HIGH_VOL":
                return Intent.hold(
                    reason=f"ATR={atr_pct:.2f}% -- too volatile, waiting for calmer market"
                )

            # Low or normal vol -- check balances then open LP
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

            logger.info(
                f"ATR={atr_pct:.2f}% ({regime}) -- opening LP with {float(range_width) * 100:.0f}% range"
            )
            self._entry_atr_pct = atr_pct
            self._entry_range_width = range_width
            return self._create_open_intent(market, range_width)

        except Exception as e:
            logger.exception(f"Error in decide(): {e}")
            return Intent.hold(reason=f"Error: {e}")

    # =========================================================================
    # Intent Creation
    # =========================================================================

    def _create_open_intent(self, market: MarketSnapshot, range_width: Decimal) -> Intent:
        """Create LP_OPEN intent with price range sized by volatility regime."""
        # Get current price (token_y per token_x) for range calculation
        try:
            price_x = market.price(self.token_x_symbol)
            price_y = market.price(self.token_y_symbol)
            current_price = price_x / price_y
        except (ValueError, KeyError):
            logger.warning("Could not get price, using default AVAX/USDC = 25")
            current_price = Decimal("25")

        half_width = range_width / Decimal("2")
        range_lower = current_price * (Decimal("1") - half_width)
        range_upper = current_price * (Decimal("1") + half_width)

        logger.info(
            f"LP_OPEN: {format_token_amount_human(self.config.amount_x, self.token_x_symbol)} + "
            f"{format_token_amount_human(self.config.amount_y, self.token_y_symbol)}, "
            f"price range [{range_lower:.4f} - {range_upper:.4f}] ({float(range_width) * 100:.0f}% width)"
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
        """Create LP_CLOSE intent for TraderJoe V2 pool."""
        logger.info(f"LP_CLOSE: {self.config.pool}")

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
            self._has_position = True
            bin_ids = result.bin_ids if result else None
            if bin_ids:
                self._position_bin_ids = list(bin_ids)
            logger.info(
                f"TraderJoe LP opened: pool={self.config.pool}, "
                f"bins={self._position_bin_ids[:3] if self._position_bin_ids else 'unknown'}..."
            )
            atr_str = f"{self._entry_atr_pct:.2f}%" if self._entry_atr_pct is not None else "N/A"
            add_event(
                TimelineEvent(
                    timestamp=datetime.now(UTC),
                    event_type=TimelineEventType.POSITION_OPENED,
                    description=f"LP opened on {self.config.pool} (ATR={atr_str})",
                    strategy_id=self.strategy_id,
                    details={
                        "pool": self.config.pool,
                        "bin_step": self.bin_step,
                        "entry_atr_pct": self._entry_atr_pct,
                        "range_width": str(self._entry_range_width),
                        "bin_ids": self._position_bin_ids[:5] if self._position_bin_ids else None,
                    },
                )
            )
        elif success and intent.intent_type.value == "LP_CLOSE":
            logger.info("TraderJoe LP closed successfully")
            self._has_position = False
            self._position_bin_ids = []
            self._entry_atr_pct = None
            self._entry_range_width = None

    # =========================================================================
    # Status & Teardown
    # =========================================================================

    def get_status(self) -> dict[str, Any]:
        """Get current strategy status."""
        return {
            "strategy": "traderjoe_atr_lp",
            "chain": self.chain,
            "wallet": self.wallet_address[:10] + "..." if self.wallet_address else "N/A",
            "config": self.config.to_dict(),
            "state": {
                "has_position": self._has_position,
                "position_bin_ids": self._position_bin_ids[:5] if self._position_bin_ids else [],
                "entry_atr_pct": self._entry_atr_pct,
                "entry_range_width": str(self._entry_range_width) if self._entry_range_width else None,
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
            # Rough estimate: amount_x * AVAX price + amount_y
            estimated_value = self.config.amount_x * Decimal("25") + self.config.amount_y

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
                        "amount_x": str(self.config.amount_x),
                        "amount_y": str(self.config.amount_y),
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
            logger.info(f"Generating teardown intent for TraderJoe LP (mode={mode.value})")
            intents.append(self._create_close_intent())

        return intents
