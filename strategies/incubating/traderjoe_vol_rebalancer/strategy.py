"""
===============================================================================
TraderJoe V2 Volatility-Adaptive LP Rebalancer
===============================================================================

Dynamically adjusts LP bin range based on realized volatility (ATR).

- LOW volatility:  tight range (5%), more capital-efficient fee capture
- MEDIUM volatility: moderate range (10%), balanced
- HIGH volatility: wide range (20%), stays in range longer

Rebalances when:
  1. Volatility regime changes (tight -> wide or vice versa)
  2. Price drifts beyond 60% of the current range from center
  3. Cooldown of 4 hours between rebalances to avoid churn

USAGE:
    almanak strat run -d strategies/incubating/traderjoe_vol_rebalancer --network anvil --once

===============================================================================
"""

import logging
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
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
from almanak.framework.teardown import (
    PositionInfo,
    PositionType,
    TeardownPositionSummary,
)

if TYPE_CHECKING:
    from almanak.framework.teardown import TeardownMode

logger = logging.getLogger(__name__)

# Volatility regimes
REGIME_LOW = "low"
REGIME_MEDIUM = "medium"
REGIME_HIGH = "high"


@dataclass
class VolRebalancerConfig:
    """Configuration for Volatility-Adaptive LP Rebalancer."""

    chain: str = "avalanche"
    network: str = "anvil"

    # Pool
    pool: str = "WAVAX/USDC/20"

    # Capital amounts
    capital_x: Decimal = field(default_factory=lambda: Decimal("1.0"))
    capital_y: Decimal = field(default_factory=lambda: Decimal("30"))

    # ATR settings
    atr_period: int = 14

    # Range widths per regime (total width, e.g. 0.05 = +/-2.5%)
    low_vol_range_pct: Decimal = field(default_factory=lambda: Decimal("0.05"))
    med_vol_range_pct: Decimal = field(default_factory=lambda: Decimal("0.10"))
    high_vol_range_pct: Decimal = field(default_factory=lambda: Decimal("0.20"))

    # ATR thresholds for regime classification (as fraction of price)
    atr_low_threshold: Decimal = field(default_factory=lambda: Decimal("0.02"))
    atr_high_threshold: Decimal = field(default_factory=lambda: Decimal("0.05"))

    # Rebalance triggers
    drift_rebalance_pct: Decimal = field(default_factory=lambda: Decimal("0.60"))
    min_rebalance_interval_hours: int = 4

    # Testing
    force_action: str = ""

    def __post_init__(self):
        """Convert string values to proper types."""
        for attr in (
            "capital_x", "capital_y", "low_vol_range_pct", "med_vol_range_pct",
            "high_vol_range_pct", "atr_low_threshold", "atr_high_threshold",
            "drift_rebalance_pct",
        ):
            val = getattr(self, attr)
            if isinstance(val, str):
                setattr(self, attr, Decimal(val))
        if isinstance(self.atr_period, str):
            self.atr_period = int(self.atr_period)
        if isinstance(self.min_rebalance_interval_hours, str):
            self.min_rebalance_interval_hours = int(self.min_rebalance_interval_hours)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dict."""
        return {k: str(v) if isinstance(v, Decimal) else v for k, v in self.__dict__.items()}

    def update(self, **kwargs: Any) -> Any:
        """Update configuration values."""

        @dataclass
        class UpdateResult:
            success: bool = True
            updated_fields: list[str] = field(default_factory=list)

        updated = []
        for k, v in kwargs.items():
            if hasattr(self, k):
                setattr(self, k, v)
                updated.append(k)
        return UpdateResult(success=True, updated_fields=updated)


@almanak_strategy(
    name="incubating_traderjoe_vol_rebalancer",
    description="Volatility-adaptive TraderJoe V2 LP -- tightens range in calm markets, widens in volatile ones",
    version="1.0.0",
    author="Almanak",
    tags=["incubating", "lp", "traderjoe-v2", "avalanche", "volatility", "adaptive", "atr"],
    supported_chains=["avalanche"],
    supported_protocols=["traderjoe_v2"],
    intent_types=["LP_OPEN", "LP_CLOSE", "HOLD"],
)
class TJVolRebalancerStrategy(IntentStrategy[VolRebalancerConfig]):
    """
    Volatility-Adaptive TraderJoe V2 LP Rebalancer.

    Uses ATR to classify the market into LOW / MEDIUM / HIGH volatility regimes
    and adjusts LP bin range width accordingly. Rebalances on regime change or
    when price drifts beyond 60% of the current range.
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)

        # Parse pool
        self.pool = self.config.pool
        parts = self.pool.split("/")
        self.token_x = parts[0] if len(parts) > 0 else "WAVAX"
        self.token_y = parts[1] if len(parts) > 1 else "USDC"
        self.bin_step = int(parts[2]) if len(parts) > 2 else 20

        # Capital
        self.capital_x = self.config.capital_x
        self.capital_y = self.config.capital_y

        # ATR config
        self.atr_period = self.config.atr_period
        self.atr_low = self.config.atr_low_threshold
        self.atr_high = self.config.atr_high_threshold

        # Range widths per regime
        self._range_for_regime = {
            REGIME_LOW: self.config.low_vol_range_pct,
            REGIME_MEDIUM: self.config.med_vol_range_pct,
            REGIME_HIGH: self.config.high_vol_range_pct,
        }

        # Rebalance settings
        self.drift_pct = self.config.drift_rebalance_pct
        self.min_interval = timedelta(hours=self.config.min_rebalance_interval_hours)
        self.force_action = str(self.config.force_action).lower()

        # Restore persisted state
        persistent = getattr(self, "persistent_state", {})
        self._position_bin_ids: list[int] = persistent.get("position_bin_ids", [])
        self._center_price: Decimal | None = (
            Decimal(persistent["center_price"]) if persistent.get("center_price") else None
        )
        self._current_regime: str = persistent.get("current_regime", REGIME_MEDIUM)
        self._current_range_width: Decimal = Decimal(
            persistent.get("current_range_width", str(self.config.med_vol_range_pct))
        )
        self._last_rebalance: datetime | None = (
            datetime.fromisoformat(persistent["last_rebalance"])
            if persistent.get("last_rebalance")
            else None
        )
        # Track whether we're in a close-then-reopen cycle
        self._pending_reopen: bool = persistent.get("pending_reopen", False)
        self._pending_regime: str = persistent.get("pending_regime", REGIME_MEDIUM)

        logger.info(
            f"TJVolRebalancer initialized: pool={self.pool}, "
            f"regime={self._current_regime}, range={self._current_range_width*100}%, "
            f"position={'YES' if self._position_bin_ids else 'NO'}"
        )

    # =========================================================================
    # MAIN DECISION
    # =========================================================================

    def decide(self, market: MarketSnapshot) -> Intent | None:
        try:
            current_price = self._get_price(market)

            # Forced actions for testing
            if self.force_action == "open":
                return self._open_position(current_price, self._current_regime)
            if self.force_action == "close":
                return self._close_position()

            # If we just closed for a rebalance, re-open immediately
            if self._pending_reopen and not self._position_bin_ids:
                logger.info(f"Re-opening after rebalance with regime={self._pending_regime}")
                return self._open_position(current_price, self._pending_regime)

            # Classify volatility
            regime = self._classify_volatility(market, current_price)

            # No position -- open one
            if not self._position_bin_ids:
                return self._open_position(current_price, regime)

            # Position exists -- check for rebalance triggers
            if not self._can_rebalance():
                return Intent.hold(reason=f"Cooldown active (regime={regime})")

            # Trigger 1: Regime changed
            if regime != self._current_regime:
                logger.info(f"Regime changed: {self._current_regime} -> {regime}")
                self._pending_reopen = True
                self._pending_regime = regime
                add_event(TimelineEvent(
                    timestamp=datetime.now(UTC),
                    event_type=TimelineEventType.STATE_CHANGE,
                    description=f"Rebalancing: regime changed {self._current_regime} -> {regime}",
                    strategy_id=self.strategy_id,
                    details={"old_regime": self._current_regime, "new_regime": regime},
                ))
                return self._close_position()

            # Trigger 2: Price drifted too far from center
            if self._center_price:
                drift = abs(current_price - self._center_price) / self._center_price
                max_drift = self._current_range_width * self.drift_pct
                if drift > max_drift:
                    logger.info(f"Price drift {drift*100:.1f}% > threshold {max_drift*100:.1f}%")
                    self._pending_reopen = True
                    self._pending_regime = regime
                    add_event(TimelineEvent(
                        timestamp=datetime.now(UTC),
                        event_type=TimelineEventType.STATE_CHANGE,
                        description=f"Rebalancing: price drifted {drift*100:.1f}%",
                        strategy_id=self.strategy_id,
                        details={"drift_pct": str(drift * 100), "threshold_pct": str(max_drift * 100)},
                    ))
                    return self._close_position()

            return Intent.hold(
                reason=f"In range (regime={regime}, width={self._current_range_width*100}%)"
            )

        except Exception as e:
            logger.exception(f"Error in decide: {e}")
            return Intent.hold(reason=f"Error: {e}")

    # =========================================================================
    # VOLATILITY CLASSIFICATION
    # =========================================================================

    def _classify_volatility(self, market: MarketSnapshot, current_price: Decimal) -> str:
        """Classify market into LOW / MEDIUM / HIGH using ATR."""
        try:
            atr_value = market.atr(self.token_x, period=self.atr_period, timeframe="4h")
            # Normalize ATR as fraction of price
            atr_fraction = Decimal(str(atr_value)) / current_price
            logger.debug(f"ATR={atr_value:.4f}, fraction={atr_fraction:.4f}")

            if atr_fraction < self.atr_low:
                return REGIME_LOW
            elif atr_fraction > self.atr_high:
                return REGIME_HIGH
            else:
                return REGIME_MEDIUM
        except Exception as e:
            logger.warning(f"ATR unavailable ({e}), keeping current regime ({self._current_regime})")
            return self._current_regime

    # =========================================================================
    # POSITION MANAGEMENT
    # =========================================================================

    def _open_position(self, current_price: Decimal, regime: str) -> Intent:
        """Open LP position with regime-appropriate range width."""
        range_width = self._range_for_regime[regime]
        half = range_width / Decimal("2")
        range_lower = current_price * (Decimal("1") - half)
        range_upper = current_price * (Decimal("1") + half)

        # Defer state updates to on_intent_executed (only apply on success)
        self._intended_regime = regime
        self._intended_range_width = range_width
        self._intended_center_price = current_price

        logger.info(
            f"LP_OPEN: {self.capital_x} {self.token_x} + {self.capital_y} {self.token_y}, "
            f"range=[{range_lower:.4f}, {range_upper:.4f}], regime={regime}, width={range_width*100}%"
        )

        return Intent.lp_open(
            pool=self.pool,
            amount0=self.capital_x,
            amount1=self.capital_y,
            range_lower=range_lower,
            range_upper=range_upper,
            protocol="traderjoe_v2",
        )

    def _close_position(self) -> Intent:
        """Close current LP position."""
        logger.info(f"LP_CLOSE: pool={self.pool}")
        return Intent.lp_close(
            position_id=self.pool,
            pool=self.pool,
            collect_fees=True,
            protocol="traderjoe_v2",
        )

    def _can_rebalance(self) -> bool:
        """Check if enough time has passed since last rebalance."""
        if self._last_rebalance is None:
            return True
        return datetime.now(UTC) >= self._last_rebalance + self.min_interval

    # =========================================================================
    # HELPERS
    # =========================================================================

    def _get_price(self, market: MarketSnapshot) -> Decimal:
        """Get current price of token_x in terms of token_y."""
        try:
            px = market.price(self.token_x)
            py = market.price(self.token_y)
            return Decimal(str(px)) / Decimal(str(py))
        except (ValueError, KeyError) as e:
            raise RuntimeError(f"Price unavailable for {self.token_x}/{self.token_y}: {e}") from e

    def _price_to_bin_id(self, price: Decimal) -> int:
        import math
        if price <= 0:
            return BIN_ID_OFFSET - 1_000_000
        base = 1 + self.bin_step / 10_000
        return int(math.log(float(price)) / math.log(base)) + BIN_ID_OFFSET

    # =========================================================================
    # LIFECYCLE
    # =========================================================================

    def on_intent_executed(self, intent: Intent, success: bool, result: Any) -> None:
        if success and intent.intent_type.value == "LP_OPEN":
            logger.info("LP position opened successfully")
            # Apply deferred state from _open_position
            self._current_regime = getattr(self, "_intended_regime", self._current_regime)
            self._current_range_width = getattr(self, "_intended_range_width", self._current_range_width)
            self._center_price = getattr(self, "_intended_center_price", self._center_price)
            self._last_rebalance = datetime.now(UTC)
            self._pending_reopen = False
            if hasattr(result, "bin_ids") and result.bin_ids:
                self._position_bin_ids = list(result.bin_ids)
            else:
                center_bin = self._price_to_bin_id(self._center_price or Decimal("30"))
                num_bins = max(5, int(float(self._current_range_width) * 100))
                half = num_bins // 2
                self._position_bin_ids = list(range(center_bin - half, center_bin + half + 1))

            add_event(TimelineEvent(
                timestamp=datetime.now(UTC),
                event_type=TimelineEventType.LP_OPEN,
                description=f"TraderJoe LP opened: regime={self._current_regime}, width={self._current_range_width*100}%",
                strategy_id=self.strategy_id,
                details={
                    "pool": self.pool, "regime": self._current_regime,
                    "range_width": str(self._current_range_width),
                },
            ))

        elif success and intent.intent_type.value == "LP_CLOSE":
            logger.info("LP position closed successfully")
            self._position_bin_ids = []
            add_event(TimelineEvent(
                timestamp=datetime.now(UTC),
                event_type=TimelineEventType.LP_CLOSE,
                description="TraderJoe LP closed for rebalance",
                strategy_id=self.strategy_id,
                details={"pool": self.pool},
            ))

        # Persist state
        self.persistent_state.update({
            "position_bin_ids": self._position_bin_ids,
            "center_price": str(self._center_price) if self._center_price else None,
            "current_regime": self._current_regime,
            "current_range_width": str(self._current_range_width),
            "last_rebalance": self._last_rebalance.isoformat() if self._last_rebalance else None,
            "pending_reopen": self._pending_reopen,
            "pending_regime": self._pending_regime,
        })

    # =========================================================================
    # STATUS & TEARDOWN
    # =========================================================================

    def get_status(self) -> dict[str, Any]:
        return {
            "strategy": "incubating_traderjoe_vol_rebalancer",
            "chain": self.chain,
            "pool": self.pool,
            "regime": self._current_regime,
            "range_width_pct": str(self._current_range_width),
            "has_position": bool(self._position_bin_ids),
            "center_price": str(self._center_price) if self._center_price else None,
            "pending_reopen": self._pending_reopen,
        }

    def supports_teardown(self) -> bool:
        return True

    def get_open_positions(self) -> TeardownPositionSummary:
        positions: list[PositionInfo] = []
        if self._position_bin_ids:
            est_value = self.capital_x * Decimal("30") + self.capital_y
            positions.append(PositionInfo(
                position_type=PositionType.LP,
                position_id=self.pool,
                chain=self.chain,
                protocol="traderjoe_v2",
                value_usd=est_value,
                details={
                    "pool": self.pool, "regime": self._current_regime,
                    "range_width": str(self._current_range_width),
                    "bin_count": len(self._position_bin_ids),
                },
            ))
        return TeardownPositionSummary(
            strategy_id=self.strategy_id,
            timestamp=datetime.now(UTC),
            positions=positions,
        )

    def generate_teardown_intents(self, mode: "TeardownMode", market=None) -> list[Intent]:
        if self._position_bin_ids:
            return [Intent.lp_close(
                position_id=self.pool, pool=self.pool,
                collect_fees=True, protocol="traderjoe_v2",
            )]
        return []
