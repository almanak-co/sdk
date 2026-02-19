"""
===============================================================================
Aerodrome Stable Yield Farmer Strategy
===============================================================================

An Aerodrome stable pool LP strategy for USDC/USDbC with minimal impermanent
loss. Stable pools use the x^3*y + y^3*x curve which is optimal for assets
that should maintain a 1:1 peg.

STRATEGY LOGIC:
---------------
1. Opens LP position in USDC/USDbC stable pool
2. No rebalancing - stable pool maintains peg automatically
3. Returns Intent.hold() normally since stable pools don't need active management
4. Supports force_action='open' and force_action='close' for testing

USAGE:
------
    python strategies/tests/lp/aero_stable_farmer/run_anvil.py

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
class AeroStableFarmerConfig:
    """Configuration for Aerodrome Stable Yield Farmer strategy."""

    chain: str = "base"
    network: str = "anvil"
    pool: str = "USDC/USDbC"
    stable: bool = True
    amount0: Decimal = field(default_factory=lambda: Decimal("3"))
    amount1: Decimal = field(default_factory=lambda: Decimal("3"))
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
    name="test_aero_stable_farmer",
    description="Aerodrome stable pool LP strategy for USDC/USDbC with minimal IL",
    version="1.0.0",
    author="Almanak",
    tags=["test", "lp", "aerodrome", "base", "stable", "yield-farming"],
    supported_chains=["base"],
    supported_protocols=["aerodrome"],
    intent_types=["LP_OPEN", "LP_CLOSE", "HOLD"],
)
class AeroStableFarmerStrategy(IntentStrategy[AeroStableFarmerConfig]):
    """
    Aerodrome Stable Yield Farmer Strategy.

    Provides liquidity to the USDC/USDbC stable pool on Base for low-risk yield.
    Stable pools have minimal impermanent loss for pegged assets.
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        """Initialize the strategy."""
        super().__init__(*args, **kwargs)

        self.pool = self.config.pool
        pool_parts = self.pool.split("/")
        self.token0_symbol = pool_parts[0] if len(pool_parts) > 0 else "USDC"
        self.token1_symbol = pool_parts[1] if len(pool_parts) > 1 else "USDbC"

        self.stable = self.config.stable
        self.amount0 = self.config.amount0
        self.amount1 = self.config.amount1
        self.force_action = str(self.config.force_action).lower()

        # Internal state
        self._has_position: bool = False
        self._lp_token_balance: Decimal = Decimal("0")

        pool_type = "stable" if self.stable else "volatile"
        logger.info(
            f"AeroStableFarmerStrategy initialized: pool={self.pool}, type={pool_type}, amounts={self.amount0} {self.token0_symbol} + {self.amount1} {self.token1_symbol}"
        )

    def decide(self, market: MarketSnapshot) -> Intent | None:
        """
        Make LP decision based on market conditions.

        Decision Flow:
        1. If force_action is set, execute that action
        2. If no position exists, open one
        3. If position exists, hold (stable pools don't need rebalancing)
        4. Otherwise, hold
        """
        try:
            # Get current price (for logging/monitoring purposes)
            try:
                token0_price_usd = market.price(self.token0_symbol)
                token1_price_usd = market.price(self.token1_symbol)
                current_price = token0_price_usd / token1_price_usd
                logger.debug(f"Current price ratio: {current_price:.4f}")
            except (ValueError, KeyError) as e:
                logger.warning(f"Could not get price: {e}")
                current_price = Decimal("1")  # Stable pair should be ~1:1

            # Handle forced actions (for testing)
            if self.force_action == "open":
                logger.info("Forced action: OPEN LP position")
                return self._create_open_intent()

            elif self.force_action == "close":
                logger.info("Forced action: CLOSE LP position")
                return self._create_close_intent()

            # Check current position status
            if self._has_position:
                # Stable pools maintain peg - no rebalancing needed
                return Intent.hold(reason=f"Stable position in {self.pool} - no rebalancing needed")

            # No position - check balances and open one
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

            # Open new position
            logger.info("No position found - opening new LP position")
            add_event(
                TimelineEvent(
                    timestamp=datetime.now(UTC),
                    event_type=TimelineEventType.STATE_CHANGE,
                    description="Opening new Aerodrome stable LP position",
                    strategy_id=self.strategy_id,
                    details={"action": "opening_new_position", "pool": self.pool, "stable": self.stable},
                )
            )
            return self._create_open_intent()

        except Exception as e:
            logger.exception(f"Error in decide(): {e}")
            return Intent.hold(reason=f"Error: {str(e)}")

    def _create_open_intent(self) -> Intent:
        """Create LP_OPEN intent for stable pool."""
        pool_type = "stable" if self.stable else "volatile"
        pool_with_type = f"{self.pool}/{pool_type}"

        logger.info(
            f"LP_OPEN: {self.amount0} {self.token0_symbol} + {self.amount1} {self.token1_symbol}, pool_type={pool_type}"
        )

        # Use LP_OPEN intent with aerodrome protocol
        # Range values are required by Intent but not used by Aerodrome (full range)
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
        """Called after an intent is executed."""
        if success and intent.intent_type.value == "LP_OPEN":
            logger.info("Aerodrome stable LP position opened successfully")
            self._has_position = True
            add_event(
                TimelineEvent(
                    timestamp=datetime.now(UTC),
                    event_type=TimelineEventType.LP_OPEN,
                    description=f"Aerodrome stable LP position opened on {self.pool}",
                    strategy_id=self.strategy_id,
                    details={"pool": self.pool, "stable": self.stable},
                )
            )

        elif success and intent.intent_type.value == "LP_CLOSE":
            logger.info("Aerodrome stable LP position closed successfully")
            self._has_position = False
            self._lp_token_balance = Decimal("0")
            add_event(
                TimelineEvent(
                    timestamp=datetime.now(UTC),
                    event_type=TimelineEventType.LP_CLOSE,
                    description=f"Aerodrome stable LP position closed on {self.pool}",
                    strategy_id=self.strategy_id,
                    details={"pool": self.pool},
                )
            )

    def get_status(self) -> dict[str, Any]:
        """Get current strategy status."""
        return {
            "strategy": "test_aero_stable_farmer",
            "chain": self.chain,
            "wallet": self.wallet_address[:10] + "..." if self.wallet_address else "N/A",
            "config": {
                "pool": self.pool,
                "stable": self.stable,
                "amount0": str(self.amount0),
                "amount1": str(self.amount1),
            },
            "state": {
                "has_position": self._has_position,
                "lp_token_balance": str(self._lp_token_balance),
            },
        }

    # =========================================================================
    # TEARDOWN SUPPORT
    # =========================================================================

    def supports_teardown(self) -> bool:
        """Indicate that this strategy supports safe teardown."""
        return True

    def get_open_positions(self) -> TeardownPositionSummary:
        """Get summary of open LP positions for teardown preview."""
        positions: list[PositionInfo] = []

        if self._has_position:
            # Stable pair - both tokens are ~$1
            token0_price_usd = Decimal("1")
            token1_price_usd = Decimal("1")

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

        if self._has_position:
            pool_type = "stable" if self.stable else "volatile"
            pool_with_type = f"{self.pool}/{pool_type}"

            logger.info(f"Generating teardown intent for Aerodrome stable LP position (mode={mode.value})")

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
    print("AeroStableFarmerStrategy - Test Strategy")
    print("=" * 60)
    print(f"\nStrategy Name: {AeroStableFarmerStrategy.STRATEGY_NAME}")
    print(f"Version: {AeroStableFarmerStrategy.STRATEGY_METADATA.version}")
    print(f"Supported Chains: {AeroStableFarmerStrategy.STRATEGY_METADATA.supported_chains}")
    print(f"Supported Protocols: {AeroStableFarmerStrategy.STRATEGY_METADATA.supported_protocols}")
    print(f"Intent Types: {AeroStableFarmerStrategy.STRATEGY_METADATA.intent_types}")
    print(f"\nDescription: {AeroStableFarmerStrategy.STRATEGY_METADATA.description}")
    print("\nTo test on Anvil:")
    print("  python strategies/tests/lp/aero_stable_farmer/run_anvil.py")
