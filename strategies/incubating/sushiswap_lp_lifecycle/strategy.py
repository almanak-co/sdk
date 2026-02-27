"""
===============================================================================
SushiSwap V3 LP Lifecycle Strategy
===============================================================================

Full lifecycle LP management on SushiSwap V3 (Arbitrum): open concentrated
liquidity positions in range-bound markets, close them when a trend develops.

This is the FIRST yailoop strategy to test LP_CLOSE end-to-end, exercising
the SushiSwap V3 connector, result enrichment (position_id extraction),
and the full open -> close lifecycle.

Decision Logic:
  - RSI in [40-60] + no position  -> LP_OPEN (range-bound = fee capture)
  - RSI < 30 or > 70 + has position -> LP_CLOSE (trend = IL risk)
  - Otherwise -> HOLD

Chain: Arbitrum
Protocol: SushiSwap V3 (Uniswap V3 fork)
Pool: WETH/USDC 0.3% fee tier

USAGE:
    # Test LP_OPEN (force_action=open in config.json)
    almanak strat run -d strategies/incubating/sushiswap_lp_lifecycle --network anvil --once

    # Test LP_CLOSE (update config.json: force_action=close, position_id=<id from LP_OPEN>)
    almanak strat run -d strategies/incubating/sushiswap_lp_lifecycle --network anvil --once
===============================================================================
"""

import logging
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Any

from almanak.framework.api.timeline import TimelineEvent, TimelineEventType, add_event
from almanak.framework.connectors.sushiswap_v3 import (
    get_max_tick,
    get_min_tick,
    get_nearest_tick,
    price_to_tick,
    tick_to_price,
)
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
    """Configuration for SushiSwap V3 LP Lifecycle strategy."""

    # Pool config
    pool: str = "WETH/USDC/3000"
    range_width_pct: Decimal = field(default_factory=lambda: Decimal("0.10"))
    amount0: Decimal = field(default_factory=lambda: Decimal("0.001"))
    amount1: Decimal = field(default_factory=lambda: Decimal("3"))
    fee_tier: int = 3000

    # RSI signal parameters
    rsi_period: int = 14
    rsi_timeframe: str = "4h"
    rsi_open_lower: int = 40
    rsi_open_upper: int = 60
    rsi_close_lower: int = 30
    rsi_close_upper: int = 70

    # Testing
    force_action: str = ""
    position_id: int | None = None

    def __post_init__(self):
        """Convert string values to proper types."""
        if isinstance(self.range_width_pct, str):
            self.range_width_pct = Decimal(self.range_width_pct)
        if isinstance(self.amount0, str):
            self.amount0 = Decimal(self.amount0)
        if isinstance(self.amount1, str):
            self.amount1 = Decimal(self.amount1)
        if isinstance(self.fee_tier, str):
            self.fee_tier = int(self.fee_tier)
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
        # Handle position_id from JSON (could be None, int, or string)
        if isinstance(self.position_id, str):
            self.position_id = int(self.position_id) if self.position_id else None

    def to_dict(self) -> dict:
        """Convert config to dictionary for serialization."""
        return {
            "pool": self.pool,
            "range_width_pct": str(self.range_width_pct),
            "amount0": str(self.amount0),
            "amount1": str(self.amount1),
            "fee_tier": self.fee_tier,
            "rsi_period": self.rsi_period,
            "rsi_timeframe": self.rsi_timeframe,
            "rsi_open_lower": self.rsi_open_lower,
            "rsi_open_upper": self.rsi_open_upper,
            "rsi_close_lower": self.rsi_close_lower,
            "rsi_close_upper": self.rsi_close_upper,
            "force_action": self.force_action,
            "position_id": self.position_id,
        }


# =============================================================================
# Strategy
# =============================================================================


@almanak_strategy(
    name="sushiswap_lp_lifecycle",
    description="Full LP lifecycle on SushiSwap V3 -- open in range-bound markets, close on trend signals",
    version="1.0.0",
    author="YAInnick Loop (Iteration 7)",
    tags=["incubating", "lp", "sushiswap-v3", "arbitrum", "lifecycle", "rsi"],
    supported_chains=["arbitrum"],
    supported_protocols=["sushiswap_v3"],
    intent_types=["LP_OPEN", "LP_CLOSE", "HOLD"],
)
class SushiSwapLPLifecycle(IntentStrategy[LPLifecycleConfig]):
    """SushiSwap V3 LP strategy testing full open -> close lifecycle.

    Opens concentrated liquidity positions when RSI indicates a range-bound market,
    and closes them when RSI signals a trend. The primary purpose is to stress-test
    the LP_CLOSE path which has never been tested in the yailoop.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # Parse pool config: TOKEN0/TOKEN1/FEE
        pool_parts = self.config.pool.split("/")
        self.token0_symbol = pool_parts[0] if len(pool_parts) > 0 else "WETH"
        self.token1_symbol = pool_parts[1] if len(pool_parts) > 1 else "USDC"
        self.fee_tier = int(pool_parts[2]) if len(pool_parts) > 2 else self.config.fee_tier

        # Position tracking -- restored from config if available
        self._position_id: int | None = self.config.position_id
        self._liquidity: int | None = None
        self._tick_lower: int | None = None
        self._tick_upper: int | None = None

        logger.info(
            f"SushiSwapLPLifecycle initialized: "
            f"pool={self.config.pool}, "
            f"range_width={self.config.range_width_pct * 100}%, "
            f"amounts={self.config.amount0} {self.token0_symbol} + {self.config.amount1} {self.token1_symbol}, "
            f"position_id={self._position_id}"
        )

    # =========================================================================
    # Decision Logic
    # =========================================================================

    def decide(self, market: MarketSnapshot) -> Intent | None:
        """Decide whether to open, close, or hold LP based on RSI regime."""
        try:
            # Get current price for tick calculation
            current_price = self._get_current_price(market)

            # Handle forced actions (for testing)
            force = self.config.force_action.lower() if self.config.force_action else ""
            if force == "open":
                logger.info("Forced action: OPEN LP position")
                return self._create_open_intent(current_price)
            if force == "close":
                if not self._position_id:
                    logger.warning("force_action=close but no position_id configured")
                    return Intent.hold(reason="Close requested but no position_id")
                logger.info(f"Forced action: CLOSE LP position #{self._position_id}")
                return self._create_close_intent()
            if force == "lifecycle":
                # Two-phase test: open on first iteration, close on second
                if not self._position_id:
                    logger.info("Lifecycle test: phase 1 -- OPEN LP position")
                    return self._create_open_intent(current_price)
                logger.info(f"Lifecycle test: phase 2 -- CLOSE LP position #{self._position_id}")
                return self._create_close_intent()

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
                    description=f"RSI={rsi:.2f}, position={'#' + str(self._position_id) if self._position_id else 'NONE'}",
                    strategy_id=self.strategy_id,
                    details={"rsi": rsi, "position_id": self._position_id},
                )
            )

            # Decision logic
            if self._position_id:
                # We have an LP position -- check if we should close
                if rsi < self.config.rsi_close_lower or rsi > self.config.rsi_close_upper:
                    logger.info(f"RSI={rsi:.2f} extreme -- closing LP to protect from IL")
                    return self._create_close_intent()
                return Intent.hold(
                    reason=f"RSI={rsi:.2f} in safe zone -- keeping LP position #{self._position_id}"
                )

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
                return self._create_open_intent(current_price)

            # Neutral zone -- wait
            return Intent.hold(reason=f"RSI={rsi:.2f} neutral zone, waiting for signal")

        except Exception as e:
            logger.exception(f"Error in decide(): {e}")
            return Intent.hold(reason=f"Error: {e}")

    # =========================================================================
    # Intent Creation
    # =========================================================================

    def _get_current_price(self, market: MarketSnapshot) -> Decimal:
        """Get current pool price (token1 per token0, e.g., USDC per WETH)."""
        try:
            price0_usd = market.price(self.token0_symbol)
            price1_usd = market.price(self.token1_symbol)
            if price1_usd == Decimal("0"):
                logger.warning(f"{self.token1_symbol} price is zero, using default")
                return Decimal("2500")
            return price0_usd / price1_usd
        except (ValueError, KeyError) as e:
            logger.warning(f"Could not get price: {e}, using default")
            return Decimal("2500")

    def _create_open_intent(self, current_price: Decimal) -> Intent:
        """Create LP_OPEN intent with concentrated range around current price."""
        # Calculate price range
        half_width = self.config.range_width_pct / Decimal("2")
        range_lower = current_price * (Decimal("1") - half_width)
        range_upper = current_price * (Decimal("1") + half_width)

        # Convert to ticks using token decimals via resolver (fail-fast, no default=18)
        from almanak.framework.data.tokens import get_token_resolver
        _resolver = get_token_resolver()
        chain = self.get_config("chain", "arbitrum")
        decimals0 = _resolver.get_decimals(chain, self.token0_symbol)
        decimals1 = _resolver.get_decimals(chain, self.token1_symbol)

        tick_lower = get_nearest_tick(price_to_tick(range_lower, decimals0, decimals1), self.fee_tier)
        tick_upper = get_nearest_tick(price_to_tick(range_upper, decimals0, decimals1), self.fee_tier)

        # Clamp to valid range
        min_tick = get_min_tick(self.fee_tier)
        max_tick = get_max_tick(self.fee_tier)
        tick_lower = max(tick_lower, min_tick)
        tick_upper = min(tick_upper, max_tick)

        if tick_lower >= tick_upper:
            raise ValueError(
                f"Invalid tick range: tick_lower={tick_lower} >= tick_upper={tick_upper}. "
                f"Widen range_width_pct (current: {self.config.range_width_pct})"
            )

        self._tick_lower = tick_lower
        self._tick_upper = tick_upper

        logger.info(
            f"LP_OPEN: {format_token_amount_human(self.config.amount0, self.token0_symbol)} + "
            f"{format_token_amount_human(self.config.amount1, self.token1_symbol)}, "
            f"price range [{range_lower:.4f} - {range_upper:.4f}], "
            f"ticks [{tick_lower} - {tick_upper}]"
        )

        return Intent.lp_open(
            pool=self.config.pool,
            amount0=self.config.amount0,
            amount1=self.config.amount1,
            range_lower=range_lower,
            range_upper=range_upper,
            protocol="sushiswap_v3",
        )

    def _create_close_intent(self) -> Intent:
        """Create LP_CLOSE intent for the tracked position."""
        logger.info(f"LP_CLOSE: position_id={self._position_id}")

        return Intent.lp_close(
            position_id=str(self._position_id),
            pool=self.config.pool,
            collect_fees=True,
            protocol="sushiswap_v3",
        )

    # =========================================================================
    # Lifecycle Hooks
    # =========================================================================

    def on_intent_executed(self, intent: Intent, success: bool, result: Any) -> None:
        """Track position state after execution."""
        if success and intent.intent_type.value == "LP_OPEN":
            # Result Enrichment: position_id extracted by framework from IncreaseLiquidity event
            position_id = result.position_id if result else None
            liquidity = result.extracted_data.get("liquidity") if result else None

            if position_id:
                self._position_id = int(position_id)
                self._liquidity = liquidity
                logger.info(
                    f"LP position opened: position_id={position_id}, liquidity={liquidity}"
                )
                logger.info(
                    f"*** To test LP_CLOSE, update config.json: "
                    f"position_id={position_id}, force_action=close ***"
                )
            else:
                logger.warning("LP_OPEN succeeded but no position_id extracted from receipt")

            add_event(
                TimelineEvent(
                    timestamp=datetime.now(UTC),
                    event_type=TimelineEventType.LP_OPEN,
                    description=f"SushiSwap V3 LP opened on {self.config.pool}",
                    strategy_id=self.strategy_id,
                    details={
                        "pool": self.config.pool,
                        "fee_tier": self.fee_tier,
                        "position_id": position_id,
                        "liquidity": liquidity,
                        "tick_lower": self._tick_lower,
                        "tick_upper": self._tick_upper,
                    },
                )
            )

        elif success and intent.intent_type.value == "LP_CLOSE":
            logger.info(f"LP position #{self._position_id} closed successfully")
            self._position_id = None
            self._liquidity = None
            self._tick_lower = None
            self._tick_upper = None

            add_event(
                TimelineEvent(
                    timestamp=datetime.now(UTC),
                    event_type=TimelineEventType.LP_CLOSE,
                    description="SushiSwap V3 LP closed",
                    strategy_id=self.strategy_id,
                    details={"pool": self.config.pool},
                )
            )

    # =========================================================================
    # Status & Teardown
    # =========================================================================

    def get_status(self) -> dict[str, Any]:
        """Get current strategy status."""
        return {
            "strategy": "sushiswap_lp_lifecycle",
            "chain": self.chain,
            "wallet": self.wallet_address[:10] + "..." if self.wallet_address else "N/A",
            "config": self.config.to_dict(),
            "state": {
                "position_id": self._position_id,
                "liquidity": self._liquidity,
                "tick_lower": self._tick_lower,
                "tick_upper": self._tick_upper,
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

        if self._position_id:
            estimated_value = self.config.amount0 * Decimal("2500") + self.config.amount1
            positions.append(
                PositionInfo(
                    position_type=PositionType.LP,
                    position_id=f"sushiswap-lp-{self._position_id}-{self.chain}",
                    chain=self.chain,
                    protocol="sushiswap_v3",
                    value_usd=estimated_value,
                    details={
                        "asset": f"{self.token0_symbol}/{self.token1_symbol}",
                        "pool": self.config.pool,
                        "fee_tier": self.fee_tier,
                        "nft_position_id": self._position_id,
                        "tick_lower": self._tick_lower,
                        "tick_upper": self._tick_upper,
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
        if self._position_id:
            logger.info(f"Generating teardown intent for LP position #{self._position_id}")
            intents.append(self._create_close_intent())
        return intents
