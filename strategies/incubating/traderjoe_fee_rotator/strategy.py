"""
===============================================================================
TraderJoe V2 Multi-Pool Fee Rotator with Swap Rebalancing
===============================================================================

Manages TWO TraderJoe V2 LP positions simultaneously across different pools
and rotates capital between them based on market regime (RSI + Bollinger Bands).

Pools:
  - Pool A: WAVAX/USDC/20   (safe stablecoin pair, steady fees)
  - Pool B: WAVAX/WETH.e/15  (volatile crypto pair, higher fees during moves)

Regime Detection:
  - RISK_ON  (RSI 40-70, narrow BB): 30% Pool A / 70% Pool B
  - RISK_OFF (RSI <30 or >70, wide BB): 70% Pool A / 30% Pool B
  - NEUTRAL  (else): 50/50 split

Rotation Flow (multi-step FSM):
  1. Close over-allocated pool
  2. Close under-allocated pool
  3. Swap tokens (USDC <-> WETH.e)
  4. Re-open Pool A at new allocation
  5. Re-open Pool B at new allocation

USAGE:
    uv run almanak strat run -d strategies/incubating/traderjoe_fee_rotator --network anvil --once
===============================================================================
"""

import logging
import math
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

# ============================================================================
# Constants
# ============================================================================

# Market regimes
REGIME_RISK_ON = "risk_on"
REGIME_RISK_OFF = "risk_off"
REGIME_NEUTRAL = "neutral"

# FSM phases
PHASE_INIT = "init"
PHASE_OPENING_B = "opening_b"
PHASE_MONITORING = "monitoring"
PHASE_ROT_CLOSE_A = "rot_close_a"
PHASE_ROT_CLOSE_B = "rot_close_b"
PHASE_ROT_SWAP = "rot_swap"
PHASE_ROT_OPEN_A = "rot_open_a"
PHASE_ROT_OPEN_B = "rot_open_b"

# Volatility regimes for range width
VOL_LOW = "low"
VOL_MEDIUM = "medium"
VOL_HIGH = "high"


# ============================================================================
# Configuration
# ============================================================================


@dataclass
class FeeRotatorConfig:
    """Configuration for the Multi-Pool Fee Rotator."""

    chain: str = "avalanche"
    network: str = "anvil"

    # Pool identifiers
    pool_a: str = "WAVAX/USDC/20"
    pool_b: str = "WAVAX/WETH.e/15"

    # Capital per pool (initial amounts)
    pool_a_wavax: Decimal = field(default_factory=lambda: Decimal("1.0"))
    pool_a_usdc: Decimal = field(default_factory=lambda: Decimal("30"))
    pool_b_wavax: Decimal = field(default_factory=lambda: Decimal("1.0"))
    pool_b_weth_e: Decimal = field(default_factory=lambda: Decimal("0.01"))

    # Swap amount for rotation (USDC <-> WETH.e)
    swap_rotation_usdc: Decimal = field(default_factory=lambda: Decimal("20"))

    # Allocation targets [pool_a_pct, pool_b_pct] per regime
    risk_on_alloc_a: Decimal = field(default_factory=lambda: Decimal("0.30"))
    risk_off_alloc_a: Decimal = field(default_factory=lambda: Decimal("0.70"))
    neutral_alloc_a: Decimal = field(default_factory=lambda: Decimal("0.50"))

    # Rotation
    rotation_threshold_pct: Decimal = field(default_factory=lambda: Decimal("0.20"))
    min_rotation_interval_hours: int = 12

    # RSI regime detection
    rsi_period: int = 14
    rsi_risk_on_lower: Decimal = field(default_factory=lambda: Decimal("40"))
    rsi_risk_on_upper: Decimal = field(default_factory=lambda: Decimal("70"))

    # BB squeeze detection
    bb_squeeze_threshold: Decimal = field(default_factory=lambda: Decimal("0.05"))

    # ATR for per-pool range widths
    atr_period: int = 14
    low_vol_range_pct: Decimal = field(default_factory=lambda: Decimal("0.05"))
    med_vol_range_pct: Decimal = field(default_factory=lambda: Decimal("0.10"))
    high_vol_range_pct: Decimal = field(default_factory=lambda: Decimal("0.20"))
    atr_low_threshold: Decimal = field(default_factory=lambda: Decimal("0.02"))
    atr_high_threshold: Decimal = field(default_factory=lambda: Decimal("0.05"))

    # Drift rebalance within a pool
    drift_rebalance_pct: Decimal = field(default_factory=lambda: Decimal("0.60"))
    min_rebalance_interval_hours: int = 4

    # Testing
    force_action: str = ""

    def __post_init__(self):
        """Convert string values to proper types."""
        decimal_fields = [
            "pool_a_wavax", "pool_a_usdc", "pool_b_wavax", "pool_b_weth_e",
            "swap_rotation_usdc",
            "risk_on_alloc_a", "risk_off_alloc_a", "neutral_alloc_a",
            "rotation_threshold_pct",
            "rsi_risk_on_lower", "rsi_risk_on_upper", "bb_squeeze_threshold",
            "low_vol_range_pct", "med_vol_range_pct", "high_vol_range_pct",
            "atr_low_threshold", "atr_high_threshold", "drift_rebalance_pct",
        ]
        for attr in decimal_fields:
            val = getattr(self, attr)
            if isinstance(val, str):
                setattr(self, attr, Decimal(val))
        for attr in ["rsi_period", "atr_period", "min_rotation_interval_hours", "min_rebalance_interval_hours"]:
            val = getattr(self, attr)
            if isinstance(val, str):
                setattr(self, attr, int(val))

    def to_dict(self) -> dict[str, Any]:
        return {k: str(v) if isinstance(v, Decimal) else v for k, v in self.__dict__.items()}

    def update(self, **kwargs: Any) -> Any:
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


# ============================================================================
# Strategy
# ============================================================================


@almanak_strategy(
    name="incubating_traderjoe_fee_rotator",
    description="Multi-pool TraderJoe V2 fee rotator -- rotates capital between safe and volatile pools based on RSI regime",
    version="1.0.0",
    author="Almanak",
    tags=["incubating", "lp", "traderjoe-v2", "avalanche", "multi-pool", "rotation", "rsi", "bollinger"],
    supported_chains=["avalanche"],
    supported_protocols=["traderjoe_v2"],
    intent_types=["LP_OPEN", "LP_CLOSE", "SWAP", "HOLD"],
)
class TJFeeRotatorStrategy(IntentStrategy[FeeRotatorConfig]):
    """
    Multi-Pool Fee Rotator with Swap Rebalancing.

    Manages two TraderJoe V2 LP positions simultaneously and rotates capital
    between them based on market regime detection using RSI and Bollinger Bands.

    FSM Phases:
        INIT -> OPENING_B -> MONITORING -> ROT_CLOSE_A -> ROT_CLOSE_B
        -> ROT_SWAP -> ROT_OPEN_A -> ROT_OPEN_B -> MONITORING
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)

        # Parse pool configs
        self.pool_a = self.config.pool_a
        self.pool_b = self.config.pool_b
        self._parse_pool(self.pool_a, "a")
        self._parse_pool(self.pool_b, "b")

        # Capital per pool
        self.capital_a = (self.config.pool_a_wavax, self.config.pool_a_usdc)
        self.capital_b = (self.config.pool_b_wavax, self.config.pool_b_weth_e)

        # Allocation map: regime -> pool_a_pct (pool_b_pct = 1 - pool_a_pct)
        self._alloc_for_regime = {
            REGIME_RISK_ON: self.config.risk_on_alloc_a,
            REGIME_RISK_OFF: self.config.risk_off_alloc_a,
            REGIME_NEUTRAL: self.config.neutral_alloc_a,
        }

        # Range widths per volatility tier
        self._range_for_vol = {
            VOL_LOW: self.config.low_vol_range_pct,
            VOL_MEDIUM: self.config.med_vol_range_pct,
            VOL_HIGH: self.config.high_vol_range_pct,
        }

        # Force action
        self.force_action = str(self.config.force_action).lower()

        # Restore persisted state
        ps = getattr(self, "persistent_state", {})
        self._phase: str = ps.get("phase", PHASE_INIT)
        self._pool_a_bins: list[int] = ps.get("pool_a_bins", [])
        self._pool_b_bins: list[int] = ps.get("pool_b_bins", [])
        self._pool_a_center: Decimal | None = (
            Decimal(ps["pool_a_center"]) if ps.get("pool_a_center") else None
        )
        self._pool_b_center: Decimal | None = (
            Decimal(ps["pool_b_center"]) if ps.get("pool_b_center") else None
        )
        self._pool_a_range_width: Decimal = Decimal(
            ps.get("pool_a_range_width", str(self.config.med_vol_range_pct))
        )
        self._pool_b_range_width: Decimal = Decimal(
            ps.get("pool_b_range_width", str(self.config.med_vol_range_pct))
        )
        self._current_regime: str = ps.get("current_regime", REGIME_NEUTRAL)
        self._current_alloc_a: Decimal = Decimal(
            ps.get("current_alloc_a", str(self.config.neutral_alloc_a))
        )
        self._last_rotation: datetime | None = (
            datetime.fromisoformat(ps["last_rotation"])
            if ps.get("last_rotation")
            else None
        )
        self._rotation_target_regime: str = ps.get("rotation_target_regime", REGIME_NEUTRAL)
        self._vol_tier: str = ps.get("vol_tier", VOL_MEDIUM)

        logger.info(
            f"TJFeeRotator initialized: phase={self._phase}, "
            f"regime={self._current_regime}, alloc_a={self._current_alloc_a}, "
            f"pool_a={'YES' if self._pool_a_bins else 'NO'}, "
            f"pool_b={'YES' if self._pool_b_bins else 'NO'}"
        )

    def _parse_pool(self, pool: str, suffix: str) -> None:
        """Parse pool string into token symbols and bin step."""
        parts = pool.split("/")
        setattr(self, f"token_x_{suffix}", parts[0] if len(parts) > 0 else "WAVAX")
        setattr(self, f"token_y_{suffix}", parts[1] if len(parts) > 1 else "USDC")
        setattr(self, f"bin_step_{suffix}", int(parts[2]) if len(parts) > 2 else 20)

    # =========================================================================
    # MAIN DECISION
    # =========================================================================

    def decide(self, market: MarketSnapshot) -> Intent | None:
        try:
            # Forced actions for testing individual steps
            if self.force_action:
                return self._handle_force_action(market)

            # Phase-based FSM
            return self._run_fsm(market)

        except Exception as e:
            logger.exception(f"Error in decide: {e}")
            return Intent.hold(reason=f"Error: {e}")

    def _handle_force_action(self, market: MarketSnapshot) -> Intent:
        """Handle forced actions for testing."""
        price_a = self._get_price(market, "a")
        price_b = self._get_price(market, "b")

        if self.force_action == "open_a":
            return self._open_pool_a(price_a)
        elif self.force_action == "open_b":
            return self._open_pool_b(price_b)
        elif self.force_action == "close_a":
            return self._close_pool_a()
        elif self.force_action == "close_b":
            return self._close_pool_b()
        elif self.force_action == "swap_usdc_to_weth":
            return Intent.swap(
                from_token="USDC", to_token="WETH.e",
                amount=self.config.swap_rotation_usdc,
                max_slippage=Decimal("0.01"),
                protocol="traderjoe_v2",
            )
        elif self.force_action == "open":
            # Alias: start normal flow from INIT
            return self._open_pool_a(price_a)
        else:
            return Intent.hold(reason=f"Unknown force_action: {self.force_action}")

    def _run_fsm(self, market: MarketSnapshot) -> Intent:
        """Run the multi-phase FSM."""
        # Update volatility tier for range width decisions
        try:
            price_for_vol = self._get_price(market, "a")
            self._vol_tier = self._classify_volatility(market, price_for_vol)
        except Exception:
            pass  # Keep existing _vol_tier on failure

        if self._phase == PHASE_INIT:
            # Open Pool A first
            logger.info("Phase INIT: Opening Pool A")
            return self._open_pool_a(self._get_price(market, "a"))

        elif self._phase == PHASE_OPENING_B:
            # Pool A is open, now open Pool B
            logger.info("Phase OPENING_B: Opening Pool B")
            return self._open_pool_b(self._get_price(market, "b"))

        elif self._phase == PHASE_MONITORING:
            return self._monitor(market, self._get_price(market, "a"), self._get_price(market, "b"))

        elif self._phase == PHASE_ROT_CLOSE_A:
            logger.info("Phase ROT_CLOSE_A: Closing Pool A for rotation")
            return self._close_pool_a()

        elif self._phase == PHASE_ROT_CLOSE_B:
            logger.info("Phase ROT_CLOSE_B: Closing Pool B for rotation")
            return self._close_pool_b()

        elif self._phase == PHASE_ROT_SWAP:
            return self._rotation_swap()

        elif self._phase == PHASE_ROT_OPEN_A:
            logger.info("Phase ROT_OPEN_A: Re-opening Pool A at new allocation")
            return self._open_pool_a(self._get_price(market, "a"))

        elif self._phase == PHASE_ROT_OPEN_B:
            logger.info("Phase ROT_OPEN_B: Re-opening Pool B at new allocation")
            return self._open_pool_b(self._get_price(market, "b"))

        else:
            logger.warning(f"Unknown phase: {self._phase}, resetting to MONITORING")
            self._phase = PHASE_MONITORING
            return Intent.hold(reason="Phase reset")

    # =========================================================================
    # MONITORING (core logic)
    # =========================================================================

    def _monitor(self, market: MarketSnapshot, price_a: Decimal, price_b: Decimal) -> Intent:
        """Monitor both pools and decide whether to rotate or hold."""
        # Detect market regime
        regime = self._detect_regime(market)
        target_alloc_a = self._alloc_for_regime[regime]
        target_alloc_b = Decimal("1") - target_alloc_a

        # Check for within-pool drift (rebalance single pool)
        drift_intent = self._check_pool_drift(price_a, price_b, market)
        if drift_intent:
            return drift_intent

        # Check if rotation is needed
        alloc_drift = abs(self._current_alloc_a - target_alloc_a)
        if alloc_drift > self.config.rotation_threshold_pct and self._can_rotate():
            logger.info(
                f"Rotation triggered: regime {self._current_regime} -> {regime}, "
                f"alloc drift={alloc_drift*100:.0f}% > threshold={self.config.rotation_threshold_pct*100:.0f}%"
            )
            self._rotation_target_regime = regime
            self._phase = PHASE_ROT_CLOSE_A
            add_event(TimelineEvent(
                timestamp=datetime.now(UTC),
                event_type=TimelineEventType.STATE_CHANGE,
                description=f"Starting rotation: {self._current_regime} -> {regime}",
                strategy_id=self.strategy_id,
                details={
                    "old_regime": self._current_regime, "new_regime": regime,
                    "alloc_drift": str(alloc_drift), "target_alloc_a": str(target_alloc_a),
                },
            ))
            return self._close_pool_a()

        return Intent.hold(
            reason=(
                f"Monitoring: regime={regime}, alloc_a={self._current_alloc_a}, "
                f"target_a={target_alloc_a}, drift={alloc_drift*100:.0f}%"
            )
        )

    def _check_pool_drift(
        self, price_a: Decimal, price_b: Decimal, market: MarketSnapshot
    ) -> Intent | None:
        """Check if either pool's price has drifted too far from center."""
        cooldown = timedelta(hours=self.config.min_rebalance_interval_hours)
        now = datetime.now(UTC)

        # Pool A drift
        if self._pool_a_bins and self._pool_a_center:
            drift = abs(price_a - self._pool_a_center) / self._pool_a_center
            max_drift = self._pool_a_range_width * self.config.drift_rebalance_pct
            if drift > max_drift:
                if not self._last_rotation or now >= self._last_rotation + cooldown:
                    logger.info(f"Pool A drift {drift*100:.1f}% > {max_drift*100:.1f}%, rebalancing")
                    # Close Pool A and it will reopen at new center
                    self._phase = PHASE_ROT_CLOSE_A
                    self._rotation_target_regime = self._current_regime
                    return self._close_pool_a()

        # Pool B drift
        if self._pool_b_bins and self._pool_b_center:
            drift = abs(price_b - self._pool_b_center) / self._pool_b_center
            max_drift = self._pool_b_range_width * self.config.drift_rebalance_pct
            if drift > max_drift:
                if not self._last_rotation or now >= self._last_rotation + cooldown:
                    logger.info(f"Pool B drift {drift*100:.1f}% > {max_drift*100:.1f}%, rebalancing")
                    self._phase = PHASE_ROT_CLOSE_A
                    self._rotation_target_regime = self._current_regime
                    return self._close_pool_a()  # Full rotation: close A -> close B -> swap -> open A -> open B

        return None

    # =========================================================================
    # REGIME DETECTION
    # =========================================================================

    def _detect_regime(self, market: MarketSnapshot) -> str:
        """Detect market regime using RSI + Bollinger Band width."""
        try:
            rsi = market.rsi("WAVAX", period=self.config.rsi_period, timeframe="4h")
        except Exception as e:
            logger.warning(f"RSI unavailable ({e}), using NEUTRAL")
            return REGIME_NEUTRAL

        try:
            bb = market.bollinger_bands("WAVAX", period=20, std_dev=2.0, timeframe="4h")
            bb_narrow = bb.bandwidth < float(self.config.bb_squeeze_threshold)
        except Exception as e:
            logger.warning(f"Bollinger Bands unavailable ({e}), ignoring BB")
            bb_narrow = False

        rsi_lower = float(self.config.rsi_risk_on_lower)
        rsi_upper = float(self.config.rsi_risk_on_upper)

        # Risk-On: RSI in comfortable range + narrow BB (low vol, trending)
        if rsi_lower <= rsi <= rsi_upper and bb_narrow:
            return REGIME_RISK_ON

        # Risk-Off: RSI extreme or wide BB (high vol, mean-reversion expected)
        if rsi < rsi_lower or rsi > rsi_upper:
            return REGIME_RISK_OFF

        return REGIME_NEUTRAL

    # =========================================================================
    # VOLATILITY (for range width)
    # =========================================================================

    def _classify_volatility(self, market: MarketSnapshot, price: Decimal) -> str:
        """Classify volatility using ATR (reused from Strategy 1)."""
        try:
            atr_value = market.atr("WAVAX", period=self.config.atr_period, timeframe="4h")
            atr_fraction = Decimal(str(atr_value)) / price
            if atr_fraction < self.config.atr_low_threshold:
                return VOL_LOW
            elif atr_fraction > self.config.atr_high_threshold:
                return VOL_HIGH
            return VOL_MEDIUM
        except Exception as e:
            logger.warning(f"ATR unavailable ({e}), defaulting to MEDIUM")
            return VOL_MEDIUM

    # =========================================================================
    # POSITION MANAGEMENT
    # =========================================================================

    def _open_pool_a(self, current_price: Decimal) -> Intent:
        """Open LP on Pool A (WAVAX/USDC)."""
        range_width = self._get_range_width(current_price)
        half = range_width / Decimal("2")
        range_lower = current_price * (Decimal("1") - half)
        range_upper = current_price * (Decimal("1") + half)

        self._pool_a_center = current_price
        self._pool_a_range_width = range_width

        amount_x, amount_y = self.capital_a
        logger.info(
            f"LP_OPEN Pool A: {amount_x} WAVAX + {amount_y} USDC, "
            f"range=[{range_lower:.4f}, {range_upper:.4f}], width={range_width*100}%"
        )

        return Intent.lp_open(
            pool=self.pool_a,
            amount0=amount_x,
            amount1=amount_y,
            range_lower=range_lower,
            range_upper=range_upper,
            protocol="traderjoe_v2",
        )

    def _open_pool_b(self, current_price: Decimal) -> Intent:
        """Open LP on Pool B (WAVAX/WETH.e)."""
        range_width = self._get_range_width(current_price)
        half = range_width / Decimal("2")
        range_lower = current_price * (Decimal("1") - half)
        range_upper = current_price * (Decimal("1") + half)

        self._pool_b_center = current_price
        self._pool_b_range_width = range_width

        amount_x, amount_y = self.capital_b
        logger.info(
            f"LP_OPEN Pool B: {amount_x} WAVAX + {amount_y} WETH.e, "
            f"range=[{range_lower:.6f}, {range_upper:.6f}], width={range_width*100}%"
        )

        return Intent.lp_open(
            pool=self.pool_b,
            amount0=amount_x,
            amount1=amount_y,
            range_lower=range_lower,
            range_upper=range_upper,
            protocol="traderjoe_v2",
        )

    def _close_pool_a(self) -> Intent:
        """Close Pool A."""
        logger.info(f"LP_CLOSE Pool A: {self.pool_a}")
        return Intent.lp_close(
            position_id=self.pool_a,
            pool=self.pool_a,
            collect_fees=True,
            protocol="traderjoe_v2",
        )

    def _close_pool_b(self) -> Intent:
        """Close Pool B."""
        logger.info(f"LP_CLOSE Pool B: {self.pool_b}")
        return Intent.lp_close(
            position_id=self.pool_b,
            pool=self.pool_b,
            collect_fees=True,
            protocol="traderjoe_v2",
        )

    def _rotation_swap(self) -> Intent:
        """Swap tokens during rotation to rebalance between pool token types."""
        target_alloc_a = self._alloc_for_regime[self._rotation_target_regime]
        old_alloc_a = self._current_alloc_a

        if target_alloc_a == old_alloc_a:
            # Drift-only rebalance (same regime), skip swap
            logger.info("Drift rebalance: no swap needed (same allocation)")
            self._phase = PHASE_ROT_OPEN_A
            self._persist_state()
            return Intent.hold(reason="Skip swap for same-regime drift rebalance")

        if target_alloc_a > old_alloc_a:
            # Moving capital toward Pool A: swap WETH.e -> USDC
            logger.info("Rotation swap: WETH.e -> USDC (increasing Pool A allocation)")
            return Intent.swap(
                from_token="WETH.e",
                to_token="USDC",
                amount=self.config.pool_b_weth_e / Decimal("2"),
                max_slippage=Decimal("0.01"),
                protocol="traderjoe_v2",
            )
        else:
            # Moving capital toward Pool B: swap USDC -> WETH.e
            logger.info("Rotation swap: USDC -> WETH.e (increasing Pool B allocation)")
            return Intent.swap(
                from_token="USDC",
                to_token="WETH.e",
                amount=self.config.swap_rotation_usdc,
                max_slippage=Decimal("0.01"),
                protocol="traderjoe_v2",
            )

    # =========================================================================
    # HELPERS
    # =========================================================================

    def _get_price(self, market: MarketSnapshot, pool_suffix: str) -> Decimal:
        """Get current price for a pool (token_y per token_x)."""
        token_x = getattr(self, f"token_x_{pool_suffix}")
        token_y = getattr(self, f"token_y_{pool_suffix}")
        try:
            px = market.price(token_x)
            py = market.price(token_y)
            return Decimal(str(px)) / Decimal(str(py))
        except (ValueError, KeyError) as e:
            raise RuntimeError(f"Price unavailable for {token_x}/{token_y}: {e}") from e

    def _get_range_width(self, current_price: Decimal) -> Decimal:
        """Get range width based on current volatility."""
        return self._range_for_vol.get(self._vol_tier, self.config.med_vol_range_pct)

    def _can_rotate(self) -> bool:
        """Check if enough time has passed since last rotation."""
        if self._last_rotation is None:
            return True
        cooldown = timedelta(hours=self.config.min_rotation_interval_hours)
        return datetime.now(UTC) >= self._last_rotation + cooldown

    def _price_to_bin_id(self, price: Decimal, bin_step: int) -> int:
        """Convert price to bin ID."""
        if price <= 0:
            return BIN_ID_OFFSET - 1_000_000
        base = 1 + bin_step / 10_000
        return int(math.log(float(price)) / math.log(base)) + BIN_ID_OFFSET

    # =========================================================================
    # LIFECYCLE
    # =========================================================================

    def on_intent_executed(self, intent: Intent, success: bool, result: Any) -> None:
        intent_type = intent.intent_type.value

        if success and intent_type == "LP_OPEN":
            self._handle_lp_open_success(intent, result)
        elif success and intent_type == "LP_CLOSE":
            self._handle_lp_close_success(intent)
        elif success and intent_type == "SWAP":
            self._handle_swap_success()
        elif not success:
            logger.error(f"Intent {intent_type} failed in phase {self._phase}")

        self._persist_state()

    def _handle_lp_open_success(self, intent: Intent, result: Any) -> None:
        """Handle successful LP_OPEN -- update bins and advance phase."""
        pool = getattr(intent, "pool", "")
        bin_ids = getattr(result, "bin_ids", None) if result else None

        if pool == self.pool_a:
            if bin_ids:
                self._pool_a_bins = list(bin_ids)
            else:
                self._pool_a_bins = self._estimate_bins(self._pool_a_center, self._pool_a_range_width, self.bin_step_a)
            logger.info(f"Pool A opened: {len(self._pool_a_bins)} bins")

            # Phase transitions
            if self._phase == PHASE_INIT:
                self._phase = PHASE_OPENING_B
            elif self._phase == PHASE_ROT_OPEN_A:
                self._phase = PHASE_ROT_OPEN_B
            else:
                self._phase = PHASE_MONITORING

            add_event(TimelineEvent(
                timestamp=datetime.now(UTC),
                event_type=TimelineEventType.LP_OPEN,
                description=f"Pool A opened: {len(self._pool_a_bins)} bins",
                strategy_id=self.strategy_id,
                details={"pool": self.pool_a, "phase": self._phase},
            ))

        elif pool == self.pool_b:
            if bin_ids:
                self._pool_b_bins = list(bin_ids)
            else:
                self._pool_b_bins = self._estimate_bins(self._pool_b_center, self._pool_b_range_width, self.bin_step_b)
            logger.info(f"Pool B opened: {len(self._pool_b_bins)} bins")

            # Phase transitions
            if self._phase in (PHASE_OPENING_B, PHASE_ROT_OPEN_B):
                self._phase = PHASE_MONITORING
                # Always update rotation timestamp when completing a rotation cycle
                self._last_rotation = datetime.now(UTC)
                # Update allocation/regime if regime changed
                if self._rotation_target_regime != self._current_regime:
                    self._current_regime = self._rotation_target_regime
                    self._current_alloc_a = self._alloc_for_regime[self._current_regime]
            else:
                self._phase = PHASE_MONITORING

            add_event(TimelineEvent(
                timestamp=datetime.now(UTC),
                event_type=TimelineEventType.LP_OPEN,
                description=f"Pool B opened: {len(self._pool_b_bins)} bins",
                strategy_id=self.strategy_id,
                details={"pool": self.pool_b, "phase": self._phase},
            ))

    def _handle_lp_close_success(self, intent: Intent) -> None:
        """Handle successful LP_CLOSE -- clear bins and advance phase."""
        pool = getattr(intent, "pool", "")

        if pool == self.pool_a:
            self._pool_a_bins = []
            logger.info("Pool A closed")
            if self._phase == PHASE_ROT_CLOSE_A:
                self._phase = PHASE_ROT_CLOSE_B

            add_event(TimelineEvent(
                timestamp=datetime.now(UTC),
                event_type=TimelineEventType.LP_CLOSE,
                description="Pool A closed for rotation",
                strategy_id=self.strategy_id,
                details={"pool": self.pool_a, "phase": self._phase},
            ))

        elif pool == self.pool_b:
            self._pool_b_bins = []
            logger.info("Pool B closed")
            if self._phase == PHASE_ROT_CLOSE_B:
                self._phase = PHASE_ROT_SWAP

            add_event(TimelineEvent(
                timestamp=datetime.now(UTC),
                event_type=TimelineEventType.LP_CLOSE,
                description="Pool B closed for rotation",
                strategy_id=self.strategy_id,
                details={"pool": self.pool_b, "phase": self._phase},
            ))

    def _handle_swap_success(self) -> None:
        """Handle successful swap -- advance to reopening pools."""
        logger.info("Rotation swap complete, reopening pools")
        if self._phase == PHASE_ROT_SWAP:
            self._phase = PHASE_ROT_OPEN_A

    def _estimate_bins(self, center: Decimal | None, width: Decimal, bin_step: int) -> list[int]:
        """Estimate bin IDs when not provided by result enrichment."""
        if not center:
            return [BIN_ID_OFFSET]
        center_bin = self._price_to_bin_id(center, bin_step)
        num_bins = max(5, int(float(width) * 100))
        half = num_bins // 2
        return list(range(center_bin - half, center_bin + half + 1))

    def _persist_state(self) -> None:
        """Save all FSM state to persistent storage."""
        self.persistent_state.update({
            "phase": self._phase,
            "pool_a_bins": self._pool_a_bins,
            "pool_b_bins": self._pool_b_bins,
            "pool_a_center": str(self._pool_a_center) if self._pool_a_center else None,
            "pool_b_center": str(self._pool_b_center) if self._pool_b_center else None,
            "pool_a_range_width": str(self._pool_a_range_width),
            "pool_b_range_width": str(self._pool_b_range_width),
            "current_regime": self._current_regime,
            "current_alloc_a": str(self._current_alloc_a),
            "last_rotation": self._last_rotation.isoformat() if self._last_rotation else None,
            "rotation_target_regime": self._rotation_target_regime,
            "vol_tier": self._vol_tier,
        })

    # =========================================================================
    # STATUS & TEARDOWN
    # =========================================================================

    def get_status(self) -> dict[str, Any]:
        return {
            "strategy": "incubating_traderjoe_fee_rotator",
            "chain": self.chain,
            "phase": self._phase,
            "regime": self._current_regime,
            "alloc_a_pct": str(self._current_alloc_a),
            "pool_a": {
                "pool": self.pool_a,
                "has_position": bool(self._pool_a_bins),
                "bin_count": len(self._pool_a_bins),
                "center_price": str(self._pool_a_center) if self._pool_a_center else None,
            },
            "pool_b": {
                "pool": self.pool_b,
                "has_position": bool(self._pool_b_bins),
                "bin_count": len(self._pool_b_bins),
                "center_price": str(self._pool_b_center) if self._pool_b_center else None,
            },
        }

    def supports_teardown(self) -> bool:
        return True

    def get_open_positions(self) -> TeardownPositionSummary:
        positions: list[PositionInfo] = []
        if self._pool_a_bins:
            est_value = self.config.pool_a_wavax * Decimal("30") + self.config.pool_a_usdc
            positions.append(PositionInfo(
                position_type=PositionType.LP,
                position_id=self.pool_a,
                chain=self.chain,
                protocol="traderjoe_v2",
                value_usd=est_value,
                details={"pool": self.pool_a, "bin_count": len(self._pool_a_bins)},
            ))
        if self._pool_b_bins:
            est_value = self.config.pool_b_wavax * Decimal("30") + self.config.pool_b_weth_e * Decimal("2500")
            positions.append(PositionInfo(
                position_type=PositionType.LP,
                position_id=self.pool_b,
                chain=self.chain,
                protocol="traderjoe_v2",
                value_usd=est_value,
                details={"pool": self.pool_b, "bin_count": len(self._pool_b_bins)},
            ))
        return TeardownPositionSummary(
            strategy_id=self.strategy_id,
            timestamp=datetime.now(UTC),
            positions=positions,
        )

    def generate_teardown_intents(self, mode: "TeardownMode", market=None) -> list[Intent]:
        intents: list[Intent] = []
        if self._pool_a_bins:
            intents.append(Intent.lp_close(
                position_id=self.pool_a, pool=self.pool_a,
                collect_fees=True, protocol="traderjoe_v2",
            ))
        if self._pool_b_bins:
            intents.append(Intent.lp_close(
                position_id=self.pool_b, pool=self.pool_b,
                collect_fees=True, protocol="traderjoe_v2",
            ))
        return intents
