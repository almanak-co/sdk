"""
===============================================================================
Uniswap V3 Asymmetric Bullish LP Strategy
===============================================================================

A Uniswap V3 LP strategy with asymmetric range favoring upside price movement.
The bullish bias provides more room for price appreciation while offering
tighter protection against downside moves.

STRATEGY LOGIC:
---------------
1. Open LP position with asymmetric range:
   - Lower bound: price * (1 - downside_pct)  [e.g., price * 0.92 = -8%]
   - Upper bound: price * (1 + upside_pct)    [e.g., price * 1.12 = +12%]
2. This creates a bullish bias with 60% upside room vs 40% downside room
3. Rebalance when price moves >5% from position center

ASYMMETRIC RANGE:
-----------------
Example at $3400 ETH price:
- range_lower = $3400 * (1 - 0.08) = $3128 (8% downside)
- range_upper = $3400 * (1 + 0.12) = $3808 (12% upside)
- Total range: $680 (20% width)
- Upside room: $408 (60% of range)
- Downside room: $272 (40% of range)

MULTI-CHAIN SUPPORT:
--------------------
Supported on: Arbitrum, Base, Optimism, Ethereum

POOL CONFIGURATION:
-------------------
- Pool: WETH/USDC/3000 (0.3% fee tier)
- Upside range: 12%
- Downside range: 8%
- Amount0: 0.002 WETH (~$6 at $3000)
- Amount1: 3 USDC
- Total value: ~$6

USAGE:
------
    python strategies/tests/lp/uni_asymmetric_bull/run_anvil.py

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


# Rebalance threshold: price moved >5% from position center
REBALANCE_THRESHOLD_PCT = Decimal("0.05")


@dataclass
class UniAsymmetricBullConfig:
    """Configuration for Uniswap V3 Asymmetric Bullish strategy."""

    chain: str = "arbitrum"
    network: str = "anvil"
    pool: str = "WETH/USDC/3000"
    upside_pct: Decimal = field(default_factory=lambda: Decimal("0.12"))  # 12% upside
    downside_pct: Decimal = field(default_factory=lambda: Decimal("0.08"))  # 8% downside
    amount0: Decimal = field(default_factory=lambda: Decimal("0.002"))  # WETH
    amount1: Decimal = field(default_factory=lambda: Decimal("3"))  # USDC
    force_action: str = ""
    position_id: str | None = None

    def __post_init__(self) -> None:
        """Convert string values to proper types."""
        if isinstance(self.upside_pct, str):
            self.upside_pct = Decimal(self.upside_pct)
        if isinstance(self.downside_pct, str):
            self.downside_pct = Decimal(self.downside_pct)
        if isinstance(self.amount0, str):
            self.amount0 = Decimal(self.amount0)
        if isinstance(self.amount1, str):
            self.amount1 = Decimal(self.amount1)

    def to_dict(self) -> dict[str, Any]:
        """Convert config to dictionary."""
        return {
            "chain": self.chain,
            "network": self.network,
            "pool": self.pool,
            "upside_pct": str(self.upside_pct),
            "downside_pct": str(self.downside_pct),
            "amount0": str(self.amount0),
            "amount1": str(self.amount1),
            "force_action": self.force_action,
            "position_id": self.position_id,
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
    name="test_uni_asymmetric_bull",
    description="Uniswap V3 LP strategy with asymmetric range favoring upside price movement",
    version="1.0.0",
    author="Almanak",
    tags=["test", "lp", "uniswap-v3", "asymmetric", "bullish", "multi-chain"],
    supported_chains=["arbitrum", "base", "optimism", "ethereum"],
    supported_protocols=["uniswap_v3"],
    intent_types=["LP_OPEN", "LP_CLOSE", "HOLD"],
)
class UniAsymmetricBullStrategy(IntentStrategy[UniAsymmetricBullConfig]):
    """
    Uniswap V3 Asymmetric Bullish LP Strategy.

    Uses asymmetric range with more upside room than downside:
    - Upside: +12% from current price
    - Downside: -8% from current price

    This bullish bias means:
    - More room for price to appreciate before exiting range
    - Tighter stop-loss on the downside
    - Suitable for bullish market outlook

    Rebalances when price moves >5% from position center.
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        """Initialize the strategy."""
        super().__init__(*args, **kwargs)

        self.pool = self.config.pool
        pool_parts = self.pool.split("/")
        self.token0_symbol = pool_parts[0] if len(pool_parts) > 0 else "WETH"
        self.token1_symbol = pool_parts[1] if len(pool_parts) > 1 else "USDC"
        self.fee_tier = int(pool_parts[2]) if len(pool_parts) > 2 else 3000

        self.upside_pct = self.config.upside_pct
        self.downside_pct = self.config.downside_pct
        self.amount0 = self.config.amount0
        self.amount1 = self.config.amount1
        self.force_action = str(self.config.force_action).lower()
        self.position_id = self.config.position_id

        # Internal state
        self._current_position_id: str | None = None
        self._position_range_lower: Decimal | None = None
        self._position_range_upper: Decimal | None = None
        self._position_center_price: Decimal | None = None

        logger.info(
            f"UniAsymmetricBullStrategy initialized: pool={self.pool}, "
            f"upside={self.upside_pct * 100}%, downside={self.downside_pct * 100}%, "
            f"amounts={self.amount0} {self.token0_symbol} + {self.amount1} {self.token1_symbol}"
        )

    def _calculate_asymmetric_range(self, current_price: Decimal) -> tuple[Decimal, Decimal]:
        """
        Calculate asymmetric range bounds.

        Args:
            current_price: Current price

        Returns:
            Tuple of (range_lower, range_upper)
        """
        # Asymmetric range: more upside room than downside
        range_lower = current_price * (Decimal("1") - self.downside_pct)
        range_upper = current_price * (Decimal("1") + self.upside_pct)

        return range_lower, range_upper

    def _is_price_moved_significantly(self, current_price: Decimal) -> bool:
        """
        Check if price has moved >5% from position center.

        Returns:
            True if price moved significantly
        """
        if self._position_center_price is None:
            return False

        price_deviation = abs((current_price - self._position_center_price) / self._position_center_price)

        return price_deviation > REBALANCE_THRESHOLD_PCT

    def decide(self, market: MarketSnapshot) -> Intent | None:
        """
        Make LP decision based on asymmetric bullish range strategy.

        Decision Flow:
        1. If force_action is set, execute that action
        2. If no position exists, open one with asymmetric range
        3. If price moved >5% from center: rebalance
        4. Otherwise: hold

        Args:
            market: Current market snapshot

        Returns:
            Intent for action to take
        """
        try:
            # Get current price
            try:
                token0_price_usd = market.price(self.token0_symbol)
                token1_price_usd = market.price(self.token1_symbol)
                current_price = token0_price_usd / token1_price_usd
                logger.debug(f"Current price: {current_price:.2f} {self.token1_symbol}/{self.token0_symbol}")
            except (ValueError, KeyError) as e:
                logger.warning(f"Could not get price: {e}")
                current_price = Decimal("3000")  # Default ETH price

            # Handle forced actions (for testing)
            if self.force_action == "open":
                logger.info("Forced action: OPEN LP position with asymmetric range")
                return self._create_open_intent(current_price)

            elif self.force_action == "close":
                if not self.position_id and not self._current_position_id:
                    logger.warning("force_action=close but no position_id")
                    return Intent.hold(reason="Close requested but no position_id")
                logger.info("Forced action: CLOSE LP position")
                return self._create_close_intent(self.position_id or self._current_position_id or "")

            # Check if we need to open a position
            if not self._current_position_id:
                return self._open_new_position(current_price)

            # Check for rebalance condition
            if self._is_price_moved_significantly(current_price):
                logger.info(
                    f"Price moved significantly from center "
                    f"({self._position_center_price:.2f} -> {current_price:.2f}). "
                    f"Rebalancing..."
                )
                add_event(
                    TimelineEvent(
                        timestamp=datetime.now(UTC),
                        event_type=TimelineEventType.STATE_CHANGE,
                        description="Price moved >5% from position center",
                        strategy_id=self.strategy_id,
                        details={
                            "trigger": "price_deviation",
                            "current_price": str(current_price),
                            "center_price": str(self._position_center_price),
                            "deviation_pct": str(
                                abs((current_price - self._position_center_price) / self._position_center_price * 100)
                            ),
                        },
                    )
                )
                # Close current position (will reopen on next cycle)
                return self._create_close_intent(self._current_position_id)

            # No action needed - position is within threshold
            return Intent.hold(
                reason=f"Position stable, center={self._position_center_price:.2f}, "
                f"current={current_price:.2f}, "
                f"range=[{self._position_range_lower:.2f}, {self._position_range_upper:.2f}]"
            )

        except Exception as e:
            logger.exception(f"Error in decide(): {e}")
            return Intent.hold(reason=f"Error: {str(e)}")

    def _open_new_position(self, current_price: Decimal) -> Intent:
        """
        Open a new LP position with asymmetric range.

        Args:
            current_price: Current price

        Returns:
            LP_OPEN intent
        """
        range_lower, range_upper = self._calculate_asymmetric_range(current_price)

        logger.info(
            f"Opening new asymmetric position: "
            f"price={current_price:.2f}, "
            f"range=[{range_lower:.2f}, {range_upper:.2f}] "
            f"(upside={self.upside_pct * 100}%, downside={self.downside_pct * 100}%)"
        )

        add_event(
            TimelineEvent(
                timestamp=datetime.now(UTC),
                event_type=TimelineEventType.STATE_CHANGE,
                description="Opening asymmetric bullish LP position",
                strategy_id=self.strategy_id,
                details={
                    "action": "opening_new_position",
                    "current_price": str(current_price),
                    "range_lower": str(range_lower),
                    "range_upper": str(range_upper),
                    "upside_pct": str(self.upside_pct),
                    "downside_pct": str(self.downside_pct),
                },
            )
        )

        return self._create_open_intent(current_price)

    def _create_open_intent(self, current_price: Decimal) -> Intent:
        """Create LP_OPEN intent with asymmetric range."""
        range_lower, range_upper = self._calculate_asymmetric_range(current_price)

        # Store position details
        self._position_range_lower = range_lower
        self._position_range_upper = range_upper
        self._position_center_price = current_price

        # Log the asymmetric range details
        total_width = range_upper - range_lower
        upside_room = range_upper - current_price
        upside_pct_of_range = (upside_room / total_width) * 100

        logger.info(
            f"LP_OPEN: {self.amount0} {self.token0_symbol} + {self.amount1} {self.token1_symbol}, "
            f"range [{range_lower:.2f} - {range_upper:.2f}], "
            f"upside room: {upside_pct_of_range:.1f}% of range"
        )

        return Intent.lp_open(
            pool=self.pool,
            amount0=self.amount0,
            amount1=self.amount1,
            range_lower=range_lower,
            range_upper=range_upper,
            protocol="uniswap_v3",
        )

    def _create_close_intent(self, position_id: str) -> Intent:
        """Create LP_CLOSE intent."""
        logger.info(f"LP_CLOSE: position={position_id}")

        return Intent.lp_close(
            position_id=position_id,
            pool=self.pool,
            collect_fees=True,
            protocol="uniswap_v3",
        )

    def on_intent_executed(self, intent: Intent, success: bool, result: Any) -> None:
        """Called after an intent is executed."""
        if success and intent.intent_type.value == "LP_OPEN":
            logger.info("Uniswap V3 asymmetric LP position opened successfully")
            # In production, extract actual position ID from result
            self._current_position_id = f"uni_v3_{self.pool.replace('/', '_')}"
            add_event(
                TimelineEvent(
                    timestamp=datetime.now(UTC),
                    event_type=TimelineEventType.LP_OPEN,
                    description=f"Uniswap V3 asymmetric LP position opened on {self.pool}",
                    strategy_id=self.strategy_id,
                    details={
                        "pool": self.pool,
                        "upside_pct": str(self.upside_pct),
                        "downside_pct": str(self.downside_pct),
                        "range_lower": str(self._position_range_lower),
                        "range_upper": str(self._position_range_upper),
                        "center_price": str(self._position_center_price),
                    },
                )
            )

        elif success and intent.intent_type.value == "LP_CLOSE":
            logger.info("Uniswap V3 asymmetric LP position closed successfully")
            add_event(
                TimelineEvent(
                    timestamp=datetime.now(UTC),
                    event_type=TimelineEventType.LP_CLOSE,
                    description=f"Uniswap V3 asymmetric LP position closed on {self.pool}",
                    strategy_id=self.strategy_id,
                    details={
                        "pool": self.pool,
                    },
                )
            )
            # Clear position state
            self._current_position_id = None
            self._position_range_lower = None
            self._position_range_upper = None
            self._position_center_price = None

    def get_status(self) -> dict[str, Any]:
        """Get current strategy status."""
        return {
            "strategy": "test_uni_asymmetric_bull",
            "chain": self.chain,
            "wallet": self.wallet_address[:10] + "..." if self.wallet_address else "N/A",
            "config": {
                "pool": self.pool,
                "upside_pct": str(self.upside_pct),
                "downside_pct": str(self.downside_pct),
                "amount0": str(self.amount0),
                "amount1": str(self.amount1),
            },
            "state": {
                "current_position_id": self._current_position_id,
                "position_range_lower": str(self._position_range_lower) if self._position_range_lower else None,
                "position_range_upper": str(self._position_range_upper) if self._position_range_upper else None,
                "position_center_price": str(self._position_center_price) if self._position_center_price else None,
            },
        }

    # =========================================================================
    # TEARDOWN SUPPORT
    # =========================================================================

    def get_open_positions(self) -> TeardownPositionSummary:
        """Get summary of open LP positions for teardown preview."""
        positions: list[PositionInfo] = []

        if self._current_position_id:
            # Estimate position value
            token0_price_usd = Decimal("3000")  # Default ETH price
            token1_price_usd = Decimal("1")  # USDC

            estimated_value = self.amount0 * token0_price_usd + self.amount1 * token1_price_usd

            positions.append(
                PositionInfo(
                    position_type=PositionType.LP,
                    position_id=self._current_position_id,
                    chain=self.chain,
                    protocol="uniswap_v3",
                    value_usd=estimated_value,
                    details={
                        "pool": self.pool,
                        "fee_tier": self.fee_tier,
                        "amount0": str(self.amount0),
                        "amount1": str(self.amount1),
                        "token0": self.token0_symbol,
                        "token1": self.token1_symbol,
                        "upside_pct": str(self.upside_pct),
                        "downside_pct": str(self.downside_pct),
                        "range_lower": str(self._position_range_lower),
                        "range_upper": str(self._position_range_upper),
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

        if self._current_position_id:
            logger.info(f"Generating teardown intent for Uniswap V3 asymmetric LP position (mode={mode.value})")

            intents.append(
                Intent.lp_close(
                    position_id=self._current_position_id,
                    pool=self.pool,
                    collect_fees=True,
                    protocol="uniswap_v3",
                )
            )

        return intents


if __name__ == "__main__":
    print("=" * 60)
    print("UniAsymmetricBullStrategy - Test Strategy")
    print("=" * 60)
    print(f"\nStrategy Name: {UniAsymmetricBullStrategy.STRATEGY_NAME}")
    print(f"Version: {UniAsymmetricBullStrategy.STRATEGY_METADATA.version}")
    print(f"Supported Chains: {UniAsymmetricBullStrategy.STRATEGY_METADATA.supported_chains}")
    print(f"Supported Protocols: {UniAsymmetricBullStrategy.STRATEGY_METADATA.supported_protocols}")
    print(f"Intent Types: {UniAsymmetricBullStrategy.STRATEGY_METADATA.intent_types}")
    print(f"\nDescription: {UniAsymmetricBullStrategy.STRATEGY_METADATA.description}")
    print("\nAsymmetric Range (Bullish Bias):")
    print("  - Upside: +12% from current price")
    print("  - Downside: -8% from current price")
    print("  - Result: 60% upside room, 40% downside room")
    print("\nRebalance Condition:")
    print(f"  - Price moves >{REBALANCE_THRESHOLD_PCT * 100}% from position center")
    print("\nTo test on Anvil:")
    print("  python strategies/tests/lp/uni_asymmetric_bull/run_anvil.py")
