"""
===============================================================================
Aerodrome Trend-Following LP Strategy
===============================================================================

An Aerodrome volatile pool strategy that exits LP when trend reverses.
Uses EMA(9) and EMA(21) crossovers to determine bullish/bearish trends.

STRATEGY LOGIC:
---------------
1. Track EMA(9) and EMA(21) of WETH price
2. When EMA(9) > EMA(21): Bullish - Open LP position
3. When EMA(9) < EMA(21): Bearish - Close LP position, hold tokens
4. Tracks _is_in_position and _last_trend state

POOL CONFIGURATION:
-------------------
- Pool: WETH/USDC (volatile)
- Amount0: 0.002 WETH (~$6 at $3000)
- Amount1: 3 USDC
- Total value: ~$6

USAGE:
------
    python strategies/tests/lp/aero_trend_follower/run_anvil.py

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
from almanak.framework.teardown import (
    PositionInfo,
    PositionType,
    TeardownPositionSummary,
)

if TYPE_CHECKING:
    from almanak.framework.teardown import TeardownMode

logger = logging.getLogger(__name__)


@dataclass
class AeroTrendFollowerConfig:
    """Configuration for Aerodrome Trend-Following strategy."""

    chain: str = "base"
    network: str = "anvil"
    pool: str = "WETH/USDC"
    stable: bool = False
    amount0: Decimal = field(default_factory=lambda: Decimal("0.002"))  # WETH
    amount1: Decimal = field(default_factory=lambda: Decimal("3"))  # USDC
    force_action: str = ""

    def __post_init__(self) -> None:
        """Convert string values to proper types."""
        if isinstance(self.amount0, str):
            self.amount0 = Decimal(self.amount0)
        if isinstance(self.amount1, str):
            self.amount1 = Decimal(self.amount1)
        if isinstance(self.stable, str):
            self.stable = self.stable.lower() in ("true", "1", "yes")

    def to_dict(self) -> dict[str, Any]:
        """Convert config to dictionary."""
        return {
            "chain": self.chain,
            "network": self.network,
            "pool": self.pool,
            "stable": self.stable,
            "amount0": str(self.amount0),
            "amount1": str(self.amount1),
            "force_action": self.force_action,
        }

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
    name="test_aero_trend_follower",
    description="Aerodrome volatile pool strategy that exits LP when trend reverses using EMA crossovers",
    version="1.0.0",
    author="Almanak",
    tags=["test", "lp", "aerodrome", "base", "volatile", "trend-following", "ema"],
    supported_chains=["base"],
    supported_protocols=["aerodrome"],
    intent_types=["LP_OPEN", "LP_CLOSE", "HOLD"],
)
class AeroTrendFollowerStrategy(IntentStrategy[AeroTrendFollowerConfig]):
    """
    Aerodrome Trend-Following LP Strategy.

    Uses EMA(9) and EMA(21) crossovers to determine when to enter/exit LP:
    - EMA(9) > EMA(21): Bullish trend - be in LP position
    - EMA(9) < EMA(21): Bearish trend - exit LP, hold tokens

    This strategy aims to capture fees during uptrends while avoiding
    impermanent loss during downtrends.
    """

    # EMA periods
    EMA_FAST_PERIOD = 9
    EMA_SLOW_PERIOD = 21

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        """Initialize the strategy."""
        super().__init__(*args, **kwargs)

        self.pool = self.config.pool
        pool_parts = self.pool.split("/")
        self.token0_symbol = pool_parts[0] if len(pool_parts) > 0 else "WETH"
        self.token1_symbol = pool_parts[1] if len(pool_parts) > 1 else "USDC"

        self.stable = self.config.stable
        self.amount0 = self.config.amount0
        self.amount1 = self.config.amount1
        self.force_action = str(self.config.force_action).lower()

        # Internal state
        self._is_in_position: bool = False
        self._last_trend: str = ""  # "bullish" or "bearish"
        self._lp_token_balance: Decimal = Decimal("0")

        logger.info(
            f"AeroTrendFollowerStrategy initialized: pool={self.pool}, amounts={self.amount0} {self.token0_symbol} + {self.amount1} {self.token1_symbol}"
        )

    def _determine_trend(self, ema9: Decimal, ema21: Decimal) -> str:
        """
        Determine market trend based on EMA crossover.

        Args:
            ema9: Fast EMA (9-period)
            ema21: Slow EMA (21-period)

        Returns:
            "bullish" if EMA9 > EMA21, "bearish" otherwise
        """
        if ema9 > ema21:
            return "bullish"
        else:
            return "bearish"

    def decide(self, market: MarketSnapshot) -> Intent | None:
        """
        Make LP decision based on trend analysis.

        Decision Flow:
        1. If force_action is set, execute that action
        2. Calculate EMA(9) and EMA(21) (or use forced values)
        3. Determine trend (bullish/bearish)
        4. If trend changed:
           - Bullish: Open LP position (if not already in)
           - Bearish: Close LP position (if in position)
        5. Otherwise: Hold
        """
        try:
            # Get current price
            try:
                token0_price_usd = market.price(self.token0_symbol)
                logger.debug(f"Current {self.token0_symbol} price: ${token0_price_usd}")
            except (ValueError, KeyError) as e:
                logger.warning(f"Could not get price: {e}")
                token0_price_usd = Decimal("3000")  # Default ETH price

            # Handle forced actions (for testing)
            if self.force_action == "open":
                logger.info("Forced action: OPEN LP position")
                return self._create_open_intent()

            elif self.force_action == "close":
                logger.info("Forced action: CLOSE LP position")
                return self._create_close_intent()

            # Get EMA values from market snapshot using the unified indicator API
            try:
                ema9_data = market.ema(self.token0_symbol, period=self.EMA_FAST_PERIOD)
                ema21_data = market.ema(self.token0_symbol, period=self.EMA_SLOW_PERIOD)
                ema9 = ema9_data.value
                ema21 = ema21_data.value
            except ValueError as e:
                logger.warning(f"EMA indicators not available: {e}")
                return Intent.hold(reason="EMA indicators not available")

            # Determine current trend
            current_trend = self._determine_trend(ema9, ema21)
            logger.info(f"EMA Analysis: EMA9={ema9:.2f}, EMA21={ema21:.2f}, Trend={current_trend.upper()}")

            # Check for trend change
            if current_trend != self._last_trend:
                logger.info(f"Trend changed: {self._last_trend or 'none'} -> {current_trend}")
                self._last_trend = current_trend

                if current_trend == "bullish" and not self._is_in_position:
                    # Bullish crossover - open LP position
                    logger.info("EMA9 crossed above EMA21 - BULLISH - Opening LP position")

                    # Check balances
                    try:
                        token0_balance = market.balance(self.token0_symbol)
                        token1_balance = market.balance(self.token1_symbol)

                        if token0_balance.balance < self.amount0:
                            return Intent.hold(
                                reason=f"Insufficient {self.token0_symbol}: {token0_balance.balance} < {self.amount0}"
                            )
                        if token1_balance.balance < self.amount1:
                            return Intent.hold(
                                reason=f"Insufficient {self.token1_symbol}: {token1_balance.balance} < {self.amount1}"
                            )
                    except (ValueError, KeyError):
                        logger.warning("Could not verify balances, proceeding anyway")

                    add_event(
                        TimelineEvent(
                            timestamp=datetime.now(UTC),
                            event_type=TimelineEventType.STATE_CHANGE,
                            description="Bullish EMA crossover - opening LP position",
                            strategy_id=self.strategy_id,
                            details={
                                "trigger": "ema_crossover",
                                "trend": "bullish",
                                "ema9": str(ema9),
                                "ema21": str(ema21),
                            },
                        )
                    )
                    return self._create_open_intent()

                elif current_trend == "bearish" and self._is_in_position:
                    # Bearish crossover - close LP position
                    logger.info("EMA9 crossed below EMA21 - BEARISH - Closing LP position")

                    add_event(
                        TimelineEvent(
                            timestamp=datetime.now(UTC),
                            event_type=TimelineEventType.STATE_CHANGE,
                            description="Bearish EMA crossover - closing LP position",
                            strategy_id=self.strategy_id,
                            details={
                                "trigger": "ema_crossover",
                                "trend": "bearish",
                                "ema9": str(ema9),
                                "ema21": str(ema21),
                            },
                        )
                    )
                    return self._create_close_intent()

            # No action needed
            status = "in LP" if self._is_in_position else "out of LP"
            return Intent.hold(reason=f"Trend is {current_trend}, {status} - no action needed")

        except Exception as e:
            logger.exception(f"Error in decide(): {e}")
            return Intent.hold(reason=f"Error: {str(e)}")

    def _create_open_intent(self) -> Intent:
        """Create LP_OPEN intent for volatile pool."""
        pool_type = "volatile"
        pool_with_type = f"{self.pool}/{pool_type}"

        logger.info(f"LP_OPEN: {self.amount0} {self.token0_symbol} + {self.amount1} {self.token1_symbol}")

        return Intent.lp_open(
            pool=pool_with_type,
            amount0=self.amount0,
            amount1=self.amount1,
            range_lower=Decimal("1"),  # Dummy - Aerodrome uses full range
            range_upper=Decimal("1000000"),  # Dummy - Aerodrome uses full range
            protocol="aerodrome",
        )

    def _create_close_intent(self) -> Intent:
        """Create LP_CLOSE intent."""
        pool_type = "volatile"
        pool_with_type = f"{self.pool}/{pool_type}"

        logger.info(f"LP_CLOSE: {pool_with_type}")

        return Intent.lp_close(
            position_id=pool_with_type,
            pool=pool_with_type,
            collect_fees=True,
            protocol="aerodrome",
        )

    def on_intent_executed(self, intent: Intent, success: bool, result: Any) -> None:
        """Called after an intent is executed."""
        if success and intent.intent_type.value == "LP_OPEN":
            logger.info("Aerodrome LP position opened successfully")
            self._is_in_position = True
            add_event(
                TimelineEvent(
                    timestamp=datetime.now(UTC),
                    event_type=TimelineEventType.LP_OPEN,
                    description=f"Aerodrome LP position opened on {self.pool}",
                    strategy_id=self.strategy_id,
                    details={"pool": self.pool, "trend": self._last_trend},
                )
            )

        elif success and intent.intent_type.value == "LP_CLOSE":
            logger.info("Aerodrome LP position closed successfully")
            self._is_in_position = False
            self._lp_token_balance = Decimal("0")
            add_event(
                TimelineEvent(
                    timestamp=datetime.now(UTC),
                    event_type=TimelineEventType.LP_CLOSE,
                    description=f"Aerodrome LP position closed on {self.pool}",
                    strategy_id=self.strategy_id,
                    details={"pool": self.pool, "trend": self._last_trend},
                )
            )

    def get_status(self) -> dict[str, Any]:
        """Get current strategy status."""
        return {
            "strategy": "test_aero_trend_follower",
            "chain": self.chain,
            "wallet": self.wallet_address[:10] + "..." if self.wallet_address else "N/A",
            "config": {
                "pool": self.pool,
                "stable": self.stable,
                "amount0": str(self.amount0),
                "amount1": str(self.amount1),
            },
            "state": {
                "is_in_position": self._is_in_position,
                "last_trend": self._last_trend,
                "lp_token_balance": str(self._lp_token_balance),
            },
        }

    # =========================================================================
    # TEARDOWN SUPPORT
    # =========================================================================

    def get_open_positions(self) -> TeardownPositionSummary:
        """Get summary of open LP positions for teardown preview."""
        positions: list[PositionInfo] = []

        if self._is_in_position:
            # Estimate position value
            token0_price_usd = Decimal("3000")  # Default ETH price
            token1_price_usd = Decimal("1")  # USDC

            estimated_value = self.amount0 * token0_price_usd + self.amount1 * token1_price_usd

            positions.append(
                PositionInfo(
                    position_type=PositionType.LP,
                    position_id=self.pool,
                    chain=self.chain,
                    protocol="aerodrome",
                    value_usd=estimated_value,
                    details={
                        "pool": self.pool,
                        "stable": self.stable,
                        "amount0": str(self.amount0),
                        "amount1": str(self.amount1),
                        "trend": self._last_trend,
                    },
                )
            )

        return TeardownPositionSummary(
            strategy_id=self.strategy_id,
            timestamp=datetime.now(UTC),
            positions=positions,
        )

    def generate_teardown_intents(self, mode: "TeardownMode", market=None) -> list[Intent]:
        """Generate intents to close all LP positions."""
        intents: list[Intent] = []

        if self._is_in_position:
            pool_type = "volatile"
            pool_with_type = f"{self.pool}/{pool_type}"

            logger.info(f"Generating teardown intent for Aerodrome LP position (mode={mode.value})")

            intents.append(
                Intent.lp_close(
                    position_id=pool_with_type,
                    pool=pool_with_type,
                    collect_fees=True,
                    protocol="aerodrome",
                )
            )

        return intents


if __name__ == "__main__":
    print("=" * 60)
    print("AeroTrendFollowerStrategy - Test Strategy")
    print("=" * 60)
    print(f"\nStrategy Name: {AeroTrendFollowerStrategy.STRATEGY_NAME}")
    print(f"Version: {AeroTrendFollowerStrategy.STRATEGY_METADATA.version}")
    print(f"Supported Chains: {AeroTrendFollowerStrategy.STRATEGY_METADATA.supported_chains}")
    print(f"Supported Protocols: {AeroTrendFollowerStrategy.STRATEGY_METADATA.supported_protocols}")
    print(f"Intent Types: {AeroTrendFollowerStrategy.STRATEGY_METADATA.intent_types}")
    print(f"\nDescription: {AeroTrendFollowerStrategy.STRATEGY_METADATA.description}")
    print("\nTo test on Anvil:")
    print("  python strategies/tests/lp/aero_trend_follower/run_anvil.py")
