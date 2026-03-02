"""
===============================================================================
MACD Momentum LP Strategy
===============================================================================

LP lifecycle management on Uniswap V3 (Arbitrum) driven by MACD crossover
signals. Opens concentrated liquidity when MACD turns bullish, closes when
MACD turns bearish.

This is the first yailoop strategy to test the MACD indicator (market.macd())
and LP lifecycle on Uniswap V3 (previously only tested on SushiSwap V3).

Decision Logic:
  - MACD histogram crosses above 0 (bullish) + no position -> LP_OPEN
  - MACD histogram crosses below 0 (bearish) + has position -> LP_CLOSE
  - Otherwise -> HOLD

Chain: Arbitrum
Protocol: Uniswap V3
Pool: WETH/USDC 0.3% fee tier

USAGE:
    # Test LP_OPEN (force_action=open in config.json)
    almanak strat run -d strategies/incubating/macd_momentum_lp --network anvil --once

    # Test LP_CLOSE (update config.json: force_action=close, position_id=<id>)
    almanak strat run -d strategies/incubating/macd_momentum_lp --network anvil --once

    # Full lifecycle test (force_action=lifecycle -- opens iter 1, closes iter 2)
    almanak strat run -d strategies/incubating/macd_momentum_lp --network anvil --max-iterations 3
===============================================================================
"""

import logging
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Any

from almanak.framework.api.timeline import TimelineEvent, TimelineEventType, add_event
from almanak.framework.connectors.uniswap_v3 import (
    get_max_tick,
    get_min_tick,
    get_nearest_tick,
    price_to_tick,
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
class MACDLPConfig:
    """Configuration for MACD Momentum LP strategy."""

    # Pool config
    pool: str = "WETH/USDC/3000"
    range_width_pct: Decimal = field(default_factory=lambda: Decimal("0.10"))
    amount0: Decimal = field(default_factory=lambda: Decimal("0.001"))
    amount1: Decimal = field(default_factory=lambda: Decimal("3"))
    fee_tier: int = 3000

    # MACD signal parameters
    macd_fast: int = 12
    macd_slow: int = 26
    macd_signal: int = 9
    macd_timeframe: str = "1h"

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
        if isinstance(self.macd_fast, str):
            self.macd_fast = int(self.macd_fast)
        if isinstance(self.macd_slow, str):
            self.macd_slow = int(self.macd_slow)
        if isinstance(self.macd_signal, str):
            self.macd_signal = int(self.macd_signal)
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
            "macd_fast": self.macd_fast,
            "macd_slow": self.macd_slow,
            "macd_signal": self.macd_signal,
            "macd_timeframe": self.macd_timeframe,
            "force_action": self.force_action,
            "position_id": self.position_id,
        }


# =============================================================================
# Strategy
# =============================================================================


@almanak_strategy(
    name="macd_momentum_lp",
    description="MACD-driven LP lifecycle on Uniswap V3 -- open on bullish crossover, close on bearish",
    version="1.0.0",
    author="YAInnick Loop (Iteration 9)",
    tags=["incubating", "lp", "uniswap-v3", "arbitrum", "macd", "lifecycle"],
    supported_chains=["arbitrum"],
    supported_protocols=["uniswap_v3"],
    intent_types=["LP_OPEN", "LP_CLOSE", "HOLD"],
)
class MACDMomentumLPStrategy(IntentStrategy[MACDLPConfig]):
    """Uniswap V3 LP strategy driven by MACD crossover signals.

    Opens concentrated liquidity positions when MACD turns bullish (histogram
    crosses above zero), and closes them when MACD turns bearish (histogram
    crosses below zero).

    Exercises two untested paths: MACD indicator and Uniswap V3 LP lifecycle.
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

        # MACD crossover tracking
        self._prev_histogram: float | None = None

        logger.info(
            f"MACDMomentumLPStrategy initialized: "
            f"pool={self.config.pool}, "
            f"MACD({self.config.macd_fast},{self.config.macd_slow},{self.config.macd_signal}), "
            f"range_width={self.config.range_width_pct * 100}%, "
            f"amounts={self.config.amount0} {self.token0_symbol} + {self.config.amount1} {self.token1_symbol}, "
            f"position_id={self._position_id}"
        )

    # =========================================================================
    # Decision Logic
    # =========================================================================

    def decide(self, market: MarketSnapshot) -> Intent | None:
        """Decide whether to open, close, or hold LP based on MACD signals."""
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

            # Get MACD data
            try:
                macd_data = market.macd(
                    self.token0_symbol,
                    fast_period=self.config.macd_fast,
                    slow_period=self.config.macd_slow,
                    signal_period=self.config.macd_signal,
                    timeframe=self.config.macd_timeframe,
                )
                histogram = macd_data.histogram
                macd_line = macd_data.macd_line
                signal_line = macd_data.signal_line
            except Exception as e:
                logger.warning(f"MACD unavailable for {self.token0_symbol}: {e}. Holding.")
                return Intent.hold(reason=f"MACD unavailable: {e}")

            logger.info(
                f"MACD({self.config.macd_fast},{self.config.macd_slow},{self.config.macd_signal}): "
                f"line={macd_line:.4f}, signal={signal_line:.4f}, histogram={histogram:.4f}"
            )

            # Log state
            add_event(
                TimelineEvent(
                    timestamp=datetime.now(UTC),
                    event_type=TimelineEventType.STATE_CHANGE,
                    description=(
                        f"MACD histogram={histogram:.4f}, "
                        f"position={'#' + str(self._position_id) if self._position_id else 'NONE'}"
                    ),
                    strategy_id=self.strategy_id,
                    details={
                        "macd_line": macd_line,
                        "signal_line": signal_line,
                        "histogram": histogram,
                        "position_id": self._position_id,
                    },
                )
            )

            # Detect crossover (histogram sign change)
            prev = self._prev_histogram
            self._prev_histogram = histogram

            # Need at least 2 readings for crossover detection
            if prev is None:
                return Intent.hold(reason=f"Collecting initial MACD reading (histogram={histogram:.4f})")

            bullish_crossover = prev <= 0 and histogram > 0
            bearish_crossover = prev >= 0 and histogram < 0

            # Decision logic
            if self._position_id:
                # We have an LP position -- check for bearish crossover to close
                if bearish_crossover:
                    logger.info(
                        f"BEARISH CROSSOVER: histogram {prev:.4f} -> {histogram:.4f} -- closing LP"
                    )
                    return self._create_close_intent()
                trend = "bullish" if histogram > 0 else "bearish"
                return Intent.hold(
                    reason=f"MACD {trend} (histogram={histogram:.4f}), keeping LP position #{self._position_id}"
                )

            # No position -- check for bullish crossover to open
            if bullish_crossover:
                # Verify balances before opening
                try:
                    bal0 = market.balance(self.token0_symbol)
                    bal1 = market.balance(self.token1_symbol)
                    if bal0.balance < self.config.amount0:
                        return Intent.hold(
                            reason=f"Bullish crossover but insufficient {self.token0_symbol}: "
                            f"{bal0.balance} < {self.config.amount0}"
                        )
                    if bal1.balance < self.config.amount1:
                        return Intent.hold(
                            reason=f"Bullish crossover but insufficient {self.token1_symbol}: "
                            f"{bal1.balance} < {self.config.amount1}"
                        )
                except (ValueError, KeyError, AttributeError):
                    logger.warning("Could not verify balances, proceeding anyway")

                logger.info(
                    f"BULLISH CROSSOVER: histogram {prev:.4f} -> {histogram:.4f} -- opening LP"
                )
                return self._create_open_intent(current_price)

            # No crossover - hold
            trend = "bullish" if histogram > 0 else "bearish"
            return Intent.hold(
                reason=f"MACD {trend} (histogram={histogram:.4f}), waiting for crossover"
            )

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
            protocol="uniswap_v3",
        )

    def _create_close_intent(self) -> Intent:
        """Create LP_CLOSE intent for the tracked position."""
        logger.info(f"LP_CLOSE: position_id={self._position_id}")

        return Intent.lp_close(
            position_id=str(self._position_id),
            pool=self.config.pool,
            collect_fees=True,
            protocol="uniswap_v3",
        )

    # =========================================================================
    # Lifecycle Hooks
    # =========================================================================

    def on_intent_executed(self, intent: Intent, success: bool, result: Any) -> None:
        """Track position state after execution."""
        if success and intent.intent_type.value == "LP_OPEN":
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
                    description=f"Uniswap V3 LP opened on {self.config.pool}",
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
                    description="Uniswap V3 LP closed",
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
            "strategy": "macd_momentum_lp",
            "chain": self.chain,
            "wallet": self.wallet_address[:10] + "..." if self.wallet_address else "N/A",
            "config": self.config.to_dict(),
            "state": {
                "position_id": self._position_id,
                "liquidity": self._liquidity,
                "tick_lower": self._tick_lower,
                "tick_upper": self._tick_upper,
                "prev_histogram": self._prev_histogram,
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
                    position_id=f"uniswap-lp-{self._position_id}-{self.chain}",
                    chain=self.chain,
                    protocol="uniswap_v3",
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
