"""
===============================================================================
Aerodrome Stable Pool LP Strategy
===============================================================================

Opens and manages a liquidity position in Aerodrome's USDC/DAI stable pool on Base.

This strategy exercises the Aerodrome connector's stable pool code path, which
uses a different invariant (x^3y + xy^3 = k) and fee tier (0.05%) compared to
volatile pools (x*y=k, 0.3%). It is the first kitchenloop test of the
pool_type="stable" path.

KEY DIFFERENCE FROM VOLATILE POOLS:
- Near-1:1 deposit ratio required (both tokens are USD-pegged)
- Lower fee tier (0.05% vs 0.3%)
- Depeg risk: if USDC/DAI ratio drifts > threshold, close position

DECISION LOGIC:
- force_action="lifecycle": open on first call, close on second
- force_action="open":      always open LP
- force_action="close":     always close LP
- Default: open LP if none exists; hold if position active

USAGE:
    # Full lifecycle (open then close) - requires 2 iterations
    almanak strat run -d strategies/incubating/aerodrome_stable_pool_lp --network anvil --interval 15

    # Open only
    almanak strat run -d strategies/incubating/aerodrome_stable_pool_lp --network anvil --once
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
class AerodromeStablePoolConfig:
    """Configuration for the Aerodrome Stable Pool LP strategy."""

    pool: str = "USDC/DAI"
    stable: bool = True
    amount0: Decimal = field(default_factory=lambda: Decimal("100"))
    amount1: Decimal = field(default_factory=lambda: Decimal("100"))
    force_action: str = ""
    depeg_threshold: Decimal = field(default_factory=lambda: Decimal("0.005"))

    def __post_init__(self) -> None:
        if isinstance(self.amount0, str):
            self.amount0 = Decimal(self.amount0)
        if isinstance(self.amount1, str):
            self.amount1 = Decimal(self.amount1)
        if isinstance(self.stable, str):
            self.stable = self.stable.lower() in ("true", "1", "yes")
        if isinstance(self.depeg_threshold, str):
            self.depeg_threshold = Decimal(self.depeg_threshold)


# =============================================================================
# Strategy
# =============================================================================


@almanak_strategy(
    name="aerodrome_stable_pool_lp",
    description="Aerodrome stable pool LP (USDC/DAI) on Base — exercises pool_type=stable code path",
    version="1.0.0",
    author="KitchenLoop iter-23",
    tags=["incubating", "lp", "aerodrome", "stable", "base", "stablecoin"],
    supported_chains=["base"],
    supported_protocols=["aerodrome"],
    intent_types=["LP_OPEN", "LP_CLOSE", "HOLD"],
)
class AerodromeStablePoolLPStrategy(IntentStrategy[AerodromeStablePoolConfig]):
    """
    Aerodrome Stable Pool LP strategy exercising pool_type=stable.

    Stablecoin LP pairs (USDC/DAI) on Aerodrome use:
    - Stable invariant: x^3*y + x*y^3 = k (tighter pricing)
    - Fee tier: 0.05% (vs 0.3% volatile)
    - Near-1:1 deposit ratio mandatory
    - Depeg monitoring: close position if ratio drifts > depeg_threshold
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)

        self.pool = self.config.pool
        pool_parts = self.pool.split("/")
        self.token0_symbol = pool_parts[0] if len(pool_parts) > 0 else "USDC"
        self.token1_symbol = pool_parts[1] if len(pool_parts) > 1 else "DAI"
        self.stable = self.config.stable
        self.amount0 = self.config.amount0
        self.amount1 = self.config.amount1
        self.force_action = self.config.force_action.lower() if self.config.force_action else ""
        self.depeg_threshold = self.config.depeg_threshold

        # Lifecycle state
        self._has_position: bool = False
        self._lp_opened_count: int = 0

        pool_type = "stable" if self.stable else "volatile"
        logger.info(
            f"AerodromeStablePoolLP initialized: pool={self.pool}, type={pool_type}, "
            f"amounts={self.amount0} {self.token0_symbol} + {self.amount1} {self.token1_symbol}, "
            f"depeg_threshold={self.depeg_threshold:.1%}"
        )

    def decide(self, market: MarketSnapshot) -> Intent | None:
        """
        Decide LP action based on position state and stablecoin peg status.

        Returns LP_OPEN, LP_CLOSE, or HOLD.
        """
        try:
            # ------------------------------------------------------------------
            # 1. Check stablecoin peg ratio
            # ------------------------------------------------------------------
            usdc_price = Decimal("1")
            dai_price = Decimal("1")
            try:
                usdc_price = market.price(self.token0_symbol)
                dai_price = market.price(self.token1_symbol)
            except Exception as e:
                logger.warning(f"Price fetch failed for {self.token0_symbol}/{self.token1_symbol}: {e}")

            # Ratio of USDC/DAI prices — should be ~1.0 for stable pools
            ratio = usdc_price / dai_price if dai_price > 0 else Decimal("1")
            depeg = abs(ratio - Decimal("1"))
            is_depegged = depeg > self.depeg_threshold

            logger.info(
                f"Prices: {self.token0_symbol}=${usdc_price:.4f}, {self.token1_symbol}=${dai_price:.4f}, "
                f"ratio={ratio:.4f}, depeg={depeg:.4f} ({'DEPEGGED' if is_depegged else 'OK'})"
            )

            # ------------------------------------------------------------------
            # 2. Handle forced actions
            # ------------------------------------------------------------------
            if self.force_action == "open":
                logger.info("force_action=open: opening LP position")
                return self._create_open_intent()

            if self.force_action == "close":
                logger.info("force_action=close: closing LP position")
                return self._create_close_intent()

            if self.force_action == "lifecycle":
                # First iteration: open; second iteration: close
                if not self._has_position and self._lp_opened_count == 0:
                    logger.info("lifecycle: first call -> LP_OPEN")
                    return self._create_open_intent()
                elif self._has_position or self._lp_opened_count > 0:
                    logger.info("lifecycle: second call -> LP_CLOSE")
                    return self._create_close_intent()

            # ------------------------------------------------------------------
            # 3. Default logic
            # ------------------------------------------------------------------
            if self._has_position:
                if is_depegged:
                    logger.warning(
                        f"Peg drift detected ({depeg:.4f} > {self.depeg_threshold}): closing LP position"
                    )
                    return self._create_close_intent()
                return Intent.hold(reason=f"Stable pool position active, ratio={ratio:.4f}")

            # No position — open one if peg is healthy
            if is_depegged:
                return Intent.hold(reason=f"Peg drifted {depeg:.4f} > {self.depeg_threshold} — skipping LP open")

            # Verify balances before opening
            try:
                bal0 = market.balance(self.token0_symbol)
                bal1 = market.balance(self.token1_symbol)
                if bal0.balance < self.amount0:
                    return Intent.hold(
                        reason=f"Insufficient {self.token0_symbol}: have {bal0.balance}, need {self.amount0}"
                    )
                if bal1.balance < self.amount1:
                    return Intent.hold(
                        reason=f"Insufficient {self.token1_symbol}: have {bal1.balance}, need {self.amount1}"
                    )
            except Exception as e:
                logger.warning(f"Balance check failed: {e}")

            logger.info("No position exists — opening stable pool LP")
            return self._create_open_intent()

        except Exception as e:
            logger.exception(f"decide() error: {e}")
            return Intent.hold(reason=f"Error in decide: {e}")

    # =========================================================================
    # Intent Helpers
    # =========================================================================

    def _create_open_intent(self) -> Intent:
        pool_type = "stable" if self.stable else "volatile"
        pool_with_type = f"{self.pool}/{pool_type}"
        logger.info(
            f"LP_OPEN: {format_token_amount_human(self.amount0, self.token0_symbol)} + "
            f"{format_token_amount_human(self.amount1, self.token1_symbol)}, pool={pool_with_type}"
        )
        return Intent.lp_open(
            pool=pool_with_type,
            amount0=self.amount0,
            amount1=self.amount1,
            range_lower=Decimal("1"),
            range_upper=Decimal("1000000"),
            protocol="aerodrome",
        )

    def _create_close_intent(self) -> Intent:
        pool_type = "stable" if self.stable else "volatile"
        pool_with_type = f"{self.pool}/{pool_type}"
        logger.info(f"LP_CLOSE: pool={pool_with_type}")
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
        if success and intent.intent_type.value == "LP_OPEN":
            self._has_position = True
            self._lp_opened_count += 1
            logger.info(f"LP_OPEN succeeded: position opened (count={self._lp_opened_count})")
            add_event(
                TimelineEvent(
                    timestamp=datetime.now(UTC),
                    event_type=TimelineEventType.POSITION_OPENED,
                    description=f"Aerodrome stable LP opened on {self.pool}",
                    strategy_id=self.strategy_id,
                    details={"pool": self.pool, "stable": self.stable, "amount0": str(self.amount0), "amount1": str(self.amount1)},
                )
            )
        elif success and intent.intent_type.value == "LP_CLOSE":
            self._has_position = False
            logger.info("LP_CLOSE succeeded: position closed")
            add_event(
                TimelineEvent(
                    timestamp=datetime.now(UTC),
                    event_type=TimelineEventType.POSITION_CLOSED,
                    description=f"Aerodrome stable LP closed on {self.pool}",
                    strategy_id=self.strategy_id,
                    details={"pool": self.pool},
                )
            )
        elif not success:
            logger.warning(f"Intent {intent.intent_type.value} FAILED: {result}")

    def get_status(self) -> dict[str, Any]:
        return {
            "strategy": "aerodrome_stable_pool_lp",
            "chain": self.chain,
            "pool": self.pool,
            "stable": self.stable,
            "has_position": self._has_position,
            "lp_opened_count": self._lp_opened_count,
        }

    # =========================================================================
    # Teardown
    # =========================================================================

    def supports_teardown(self) -> bool:
        return True

    def get_open_positions(self) -> "TeardownPositionSummary":
        from almanak.framework.teardown import PositionInfo, PositionType, TeardownPositionSummary

        positions: list[PositionInfo] = []
        if self._has_position:
            # Stablecoin LP: value ≈ 2x the USDC amount
            estimated_value = self.amount0 + self.amount1  # Both ~$1 each
            positions.append(
                PositionInfo(
                    position_type=PositionType.LP,
                    position_id=f"aerodrome-stable-{self.pool}-{self.chain}",
                    chain=self.chain,
                    protocol="aerodrome",
                    value_usd=estimated_value,
                    details={
                        "asset": f"{self.token0_symbol}/{self.token1_symbol}",
                        "pool": self.pool,
                        "stable": self.stable,
                        "amount0": str(self.amount0),
                        "amount1": str(self.amount1),
                    },
                )
            )
        return TeardownPositionSummary(
            strategy_id=self.strategy_id,
            timestamp=datetime.now(UTC),
            total_value_usd=sum(p.value_usd for p in positions),
            positions=positions,
        )

    def generate_teardown_intents(self, mode: "TeardownMode", market: Any = None) -> list[Intent]:
        if not self._has_position:
            return []
        return [self._create_close_intent()]
