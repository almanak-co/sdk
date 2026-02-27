"""
Velodrome LP Strategy - Solidly-Based AMM on Optimism
=====================================================

Kitchen Loop Iteration 24 (VIB-314): Tests the Aerodrome/Solidly connector
portability from Base to Optimism. Velodrome is the canonical Solidly fork on
Optimism -- same contract architecture as Aerodrome on Base, same pool_type
semantics, different deployment addresses.

Key Questions This Strategy Answers:
1. Does protocol="aerodrome" work on Optimism, or does it require "velodrome"?
2. Are AERODROME_ADDRESSES / LP_POSITION_MANAGERS configured for Optimism?
3. Does token resolution work for WETH/USDC on Optimism?
4. Does the full LP lifecycle (LP_OPEN + LP_CLOSE) succeed?

Velodrome V2 Addresses (Optimism):
- Router: 0xa062aE8A9c5e11aaA026fc2670B0D65cCc8B2858
- Factory: 0xF1046053aa5682b4F9a81b5481394DA16BE5FF5a
- Voter:  0x41C914ee0c7E1A5edCD0295623e6dC557B5aBf3C
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

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from almanak.framework.teardown import TeardownMode, TeardownPositionSummary


@dataclass
class VelodromeLPConfig:
    """Configuration for Velodrome LP strategy on Optimism."""

    pool: str = "WETH/USDC"
    stable: bool = False
    amount0: Decimal = field(default_factory=lambda: Decimal("0.001"))
    amount1: Decimal = field(default_factory=lambda: Decimal("3"))
    force_action: str = ""

    def __post_init__(self):
        """Convert string values to proper types."""
        if isinstance(self.amount0, str):
            self.amount0 = Decimal(self.amount0)
        if isinstance(self.amount1, str):
            self.amount1 = Decimal(self.amount1)
        if isinstance(self.stable, str):
            self.stable = self.stable.lower() in ("true", "1", "yes")


@almanak_strategy(
    name="velodrome_lp_optimism",
    description="Velodrome LP lifecycle on Optimism - tests Solidly connector portability from Base",
    version="1.0.0",
    author="Kitchen Loop",
    tags=["incubating", "lp", "velodrome", "optimism", "solidly", "portability-test"],
    supported_chains=["optimism"],
    supported_protocols=["aerodrome"],
    intent_types=["LP_OPEN", "LP_CLOSE", "HOLD"],
)
class VelodromeLPStrategy(IntentStrategy[VelodromeLPConfig]):
    """
    Velodrome LP lifecycle strategy on Optimism.

    Tests whether the Aerodrome/Solidly connector is portable from Base to
    Optimism. Uses protocol="aerodrome" since Velodrome shares identical
    contract interfaces (Solidly fork).

    Lifecycle mode:
    - Iteration 1: LP_OPEN (approve WETH + approve USDC + addLiquidity)
    - Iteration 2: LP_CLOSE (approve LP token + removeLiquidity)
    - Iteration 3+: HOLD (lifecycle complete)
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.pool = self.config.pool
        pool_parts = self.pool.split("/")
        self.token0_symbol = pool_parts[0] if len(pool_parts) > 0 else "WETH"
        self.token1_symbol = pool_parts[1] if len(pool_parts) > 1 else "USDC"

        self.stable = self.config.stable
        self.amount0 = self.config.amount0
        self.amount1 = self.config.amount1
        self.force_action = self.config.force_action.lower() if self.config.force_action else ""

        self._has_position: bool = False
        self._close_succeeded: bool = False
        self._lp_token_balance: Decimal = Decimal("0")

        pool_type = "stable" if self.stable else "volatile"
        logger.info(
            f"VelodromeLPStrategy initialized: "
            f"pool={self.pool}, type={pool_type}, chain=optimism, "
            f"amounts={self.amount0} {self.token0_symbol} + {self.amount1} {self.token1_symbol}"
        )

    def decide(self, market: MarketSnapshot) -> Intent | None:
        """
        LP lifecycle decision logic.

        Lifecycle flow:
        1. If no position and not closed -> LP_OPEN
        2. If position exists -> LP_CLOSE
        3. If close succeeded -> HOLD (lifecycle complete)
        """
        try:
            # Get market data for logging
            try:
                token0_price_usd = market.price(self.token0_symbol)
                token1_price_usd = market.price(self.token1_symbol)
                logger.info(
                    f"Market: {self.token0_symbol}=${token0_price_usd:.2f}, "
                    f"{self.token1_symbol}=${token1_price_usd:.4f}"
                )
            except (ValueError, KeyError) as e:
                logger.warning(f"Could not get price: {e}")

            # Handle forced actions
            if self.force_action == "open":
                return self._create_open_intent()
            elif self.force_action == "close":
                return self._create_close_intent()
            elif self.force_action == "lifecycle":
                return self._lifecycle_step(market)

            # Default: open if no position
            if self._has_position:
                return Intent.hold(reason=f"Position exists in {self.pool} pool - monitoring")

            return self._create_open_intent()

        except Exception as e:
            logger.exception(f"Error in decide(): {e}")
            return Intent.hold(reason=f"Error: {str(e)}")

    def _lifecycle_step(self, market: MarketSnapshot) -> Intent | None:
        """Execute lifecycle: open -> close -> hold."""
        if self._close_succeeded:
            logger.info("Lifecycle complete: LP opened and closed successfully")
            return Intent.hold(reason="Lifecycle complete")

        if self._has_position:
            logger.info("Lifecycle step 2: closing LP position")
            return self._create_close_intent()

        # Check balances before opening
        try:
            token0_bal = market.balance(self.token0_symbol)
            token1_bal = market.balance(self.token1_symbol)
            if token0_bal.balance < self.amount0:
                return Intent.hold(
                    reason=f"Insufficient {self.token0_symbol}: {token0_bal.balance} < {self.amount0}"
                )
            if token1_bal.balance < self.amount1:
                return Intent.hold(
                    reason=f"Insufficient {self.token1_symbol}: {token1_bal.balance} < {self.amount1}"
                )
        except (ValueError, KeyError, AttributeError):
            logger.warning("Could not verify balances, proceeding anyway")

        logger.info("Lifecycle step 1: opening LP position")
        return self._create_open_intent()

    def _create_open_intent(self) -> Intent:
        """Create LP_OPEN intent for Velodrome (using aerodrome protocol)."""
        pool_type = "stable" if self.stable else "volatile"
        pool_with_type = f"{self.pool}/{pool_type}"

        logger.info(
            f"LP_OPEN: {format_token_amount_human(self.amount0, self.token0_symbol)} + "
            f"{format_token_amount_human(self.amount1, self.token1_symbol)}, pool_type={pool_type}"
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
        """Create LP_CLOSE intent for Velodrome (using aerodrome protocol)."""
        pool_type = "stable" if self.stable else "volatile"
        pool_with_type = f"{self.pool}/{pool_type}"

        logger.info(f"LP_CLOSE: {pool_with_type}")

        return Intent.lp_close(
            position_id=pool_with_type,
            pool=pool_with_type,
            collect_fees=True,
            protocol="aerodrome",
        )

    def on_intent_executed(self, intent: Intent, success: bool, result: Any) -> None:
        """Track position state after execution."""
        if success and intent.intent_type.value == "LP_OPEN":
            logger.info("Velodrome LP position opened successfully on Optimism")
            self._has_position = True
            add_event(
                TimelineEvent(
                    timestamp=datetime.now(UTC),
                    event_type=TimelineEventType.POSITION_OPENED,
                    description=f"Velodrome LP position opened: {self.pool} on Optimism",
                    strategy_id=self.strategy_id,
                    details={"pool": self.pool, "stable": self.stable, "chain": "optimism"},
                )
            )
        elif success and intent.intent_type.value == "LP_CLOSE":
            logger.info("Velodrome LP position closed successfully on Optimism")
            self._has_position = False
            self._close_succeeded = True
            self._lp_token_balance = Decimal("0")

    def get_status(self) -> dict[str, Any]:
        """Get current strategy status."""
        return {
            "strategy": "velodrome_lp_optimism",
            "chain": "optimism",
            "wallet": self.wallet_address[:10] + "..." if self.wallet_address else "N/A",
            "config": {
                "pool": self.pool,
                "stable": self.stable,
                "amount0": str(self.amount0),
                "amount1": str(self.amount1),
            },
            "state": {
                "has_position": self._has_position,
                "close_succeeded": self._close_succeeded,
                "lp_token_balance": str(self._lp_token_balance),
            },
        }

    # Teardown support

    def supports_teardown(self) -> bool:
        return True

    def get_open_positions(self) -> "TeardownPositionSummary":
        from almanak.framework.teardown import (
            PositionInfo,
            PositionType,
            TeardownPositionSummary,
        )

        positions: list[PositionInfo] = []
        if self._has_position:
            estimated_value = self.amount0 * Decimal("3000") + self.amount1 * Decimal("1")
            positions.append(
                PositionInfo(
                    position_type=PositionType.LP,
                    position_id=f"velodrome-lp-{self.pool}-optimism",
                    chain="optimism",
                    protocol="aerodrome",
                    value_usd=estimated_value,
                    details={
                        "asset": f"{self.token0_symbol}/{self.token1_symbol}",
                        "pool": self.pool,
                        "stable": self.stable,
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
        if self._has_position:
            return [self._create_close_intent()]
        return []
