"""
===============================================================================
TraderJoe V2 Wide-Range Accumulator Strategy
===============================================================================

A TraderJoe V2 LP strategy with wide 15% range for JOE/AVAX accumulation.
Uses 21 bins to distribute liquidity across a wider price range.

STRATEGY LOGIC:
---------------
1. Opens LP position with wide 15% price range (+/-7.5% from current price)
2. Uses 21 bins to distribute liquidity
3. Hybrid rebalancing:
   - Time-based: Rebalance after 7 days since last rebalance
   - Price-based: Rebalance when price moves >7% from position center
4. Designed for long-term JOE accumulation with lower maintenance

USAGE:
------
    python strategies/tests/lp/tj_wide_accumulator/run_anvil.py

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


@dataclass
class TJWideAccumulatorConfig:
    """Configuration for TraderJoe V2 Wide-Range Accumulator strategy."""

    chain: str = "avalanche"
    network: str = "anvil"
    pool: str = "JOE/WAVAX/20"
    range_width_pct: Decimal = field(default_factory=lambda: Decimal("0.15"))
    amount_x: Decimal = field(default_factory=lambda: Decimal("15"))  # ~$6 worth of JOE at $0.4
    amount_y: Decimal = field(default_factory=lambda: Decimal("0.15"))  # ~$4.5 worth of WAVAX at $30
    num_bins: int = 21
    rebalance_price_threshold_pct: Decimal = field(default_factory=lambda: Decimal("0.07"))
    rebalance_time_days: int = 7
    force_action: str = ""
    position_id: str | None = None

    def __post_init__(self) -> None:
        """Convert string values to proper types."""
        if isinstance(self.range_width_pct, str):
            self.range_width_pct = Decimal(self.range_width_pct)
        if isinstance(self.amount_x, str):
            self.amount_x = Decimal(self.amount_x)
        if isinstance(self.amount_y, str):
            self.amount_y = Decimal(self.amount_y)
        if isinstance(self.num_bins, str):
            self.num_bins = int(self.num_bins)
        if isinstance(self.rebalance_price_threshold_pct, str):
            self.rebalance_price_threshold_pct = Decimal(self.rebalance_price_threshold_pct)
        if isinstance(self.rebalance_time_days, str):
            self.rebalance_time_days = int(self.rebalance_time_days)

    def to_dict(self) -> dict[str, Any]:
        """Convert config to dictionary."""
        return {
            "chain": self.chain,
            "network": self.network,
            "pool": self.pool,
            "range_width_pct": str(self.range_width_pct),
            "amount_x": str(self.amount_x),
            "amount_y": str(self.amount_y),
            "num_bins": self.num_bins,
            "rebalance_price_threshold_pct": str(self.rebalance_price_threshold_pct),
            "rebalance_time_days": self.rebalance_time_days,
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
    name="test_tj_wide_accumulator",
    description="TraderJoe V2 wide-range LP strategy with 15% range for JOE/AVAX accumulation",
    version="1.0.0",
    author="Almanak",
    tags=["test", "lp", "traderjoe-v2", "avalanche", "wide-range", "accumulator"],
    supported_chains=["avalanche"],
    supported_protocols=["traderjoe_v2"],
    intent_types=["LP_OPEN", "LP_CLOSE", "HOLD"],
)
class TJWideAccumulatorStrategy(IntentStrategy[TJWideAccumulatorConfig]):
    """
    TraderJoe V2 Wide-Range Accumulator Strategy.

    Uses a wide 15% price range (+/-7.5% from current) for JOE/AVAX accumulation.
    Hybrid rebalancing: Rebalance if 7 days elapsed OR price moved >7% from center.
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        """Initialize the strategy."""
        super().__init__(*args, **kwargs)

        self.pool = self.config.pool
        pool_parts = self.pool.split("/")
        self.token_x_symbol = pool_parts[0] if len(pool_parts) > 0 else "JOE"
        self.token_y_symbol = pool_parts[1] if len(pool_parts) > 1 else "WAVAX"
        self.bin_step = int(pool_parts[2]) if len(pool_parts) > 2 else 20

        self.range_width_pct = self.config.range_width_pct
        self.amount_x = self.config.amount_x
        self.amount_y = self.config.amount_y
        self.num_bins = self.config.num_bins
        self.rebalance_price_threshold_pct = self.config.rebalance_price_threshold_pct
        self.rebalance_time_days = self.config.rebalance_time_days
        self.force_action = str(self.config.force_action).lower()

        # Internal state
        self._position_bin_ids: list[int] = []
        self._position_center_price: Decimal | None = None
        self._last_rebalance_time: datetime | None = None

        logger.info(
            f"TJWideAccumulatorStrategy initialized: "
            f"pool={self.pool}, "
            f"range_width={self.range_width_pct * 100}%, "
            f"rebalance_thresholds=(price>{self.rebalance_price_threshold_pct * 100}% OR time>{self.rebalance_time_days}d), "
            f"amounts={self.amount_x} {self.token_x_symbol} + {self.amount_y} {self.token_y_symbol}, "
            f"bins={self.num_bins}"
        )

    def decide(self, market: MarketSnapshot) -> Intent | None:
        """
        Make LP decision based on market conditions.

        Decision Flow:
        1. If force_action is set, execute that action
        2. If position exists and (7 days elapsed OR price moved >7% from center), rebalance
        3. If no position exists, open one
        4. Otherwise, hold
        """
        try:
            # Get current market price
            try:
                token_x_price_usd = market.price(self.token_x_symbol)
                token_y_price_usd = market.price(self.token_y_symbol)
                current_price = token_x_price_usd / token_y_price_usd
                logger.debug(f"Current price: {current_price:.6f} {self.token_y_symbol}/{self.token_x_symbol}")
            except (ValueError, KeyError) as e:
                logger.warning(f"Could not get price: {e}")
                # Default JOE/WAVAX price ratio (~$0.4 JOE / ~$30 WAVAX)
                current_price = Decimal("0.0133")

            # Handle forced actions (for testing)
            if self.force_action == "open":
                logger.info("Forced action: OPEN LP position")
                return self._create_open_intent(current_price)

            elif self.force_action == "close":
                if not self._position_bin_ids:
                    logger.warning("force_action=close but no position tracked")
                    return Intent.hold(reason="Close requested but no position tracked")
                logger.info("Forced action: CLOSE LP position")
                return self._create_close_intent()

            # Check if we need to rebalance (hybrid logic)
            if self._position_bin_ids and self._position_center_price:
                now = datetime.now(UTC)
                should_rebalance = False
                rebalance_reason = ""

                # Time-based trigger: 7 days since last rebalance
                if self._last_rebalance_time:
                    time_since_rebalance = now - self._last_rebalance_time
                    if time_since_rebalance >= timedelta(days=self.rebalance_time_days):
                        should_rebalance = True
                        rebalance_reason = f"time ({time_since_rebalance.days} days elapsed)"

                # Price-based trigger: >7% price movement from center
                price_change_pct = abs((current_price - self._position_center_price) / self._position_center_price)
                if price_change_pct > self.rebalance_price_threshold_pct:
                    should_rebalance = True
                    rebalance_reason = f"price ({price_change_pct * 100:.2f}% moved)"

                if should_rebalance:
                    logger.info(f"Rebalancing due to {rebalance_reason}")
                    add_event(
                        TimelineEvent(
                            timestamp=now,
                            event_type=TimelineEventType.STATE_CHANGE,
                            description=f"Rebalancing: {rebalance_reason}",
                            strategy_id=self.strategy_id,
                            details={
                                "trigger": "time" if "time" in rebalance_reason else "price",
                                "price_change_pct": str(price_change_pct * 100),
                                "days_since_rebalance": str((now - self._last_rebalance_time).days)
                                if self._last_rebalance_time
                                else "N/A",
                            },
                        )
                    )
                    # Close current position first, then open new one
                    return self._create_close_intent()

            # Check current position status
            if self._position_bin_ids:
                days_until_time_rebalance = "N/A"
                if self._last_rebalance_time:
                    time_since = datetime.now(UTC) - self._last_rebalance_time
                    days_until_time_rebalance = str(max(0, self.rebalance_time_days - time_since.days))
                return Intent.hold(
                    reason=f"Position in {len(self._position_bin_ids)} bins - next time rebalance in {days_until_time_rebalance} days"
                )

            # No position - open one
            try:
                token_x_balance = market.balance(self.token_x_symbol)
                token_y_balance = market.balance(self.token_y_symbol)

                if token_x_balance.balance < self.amount_x:
                    return Intent.hold(
                        reason=f"Insufficient {self.token_x_symbol}: {token_x_balance.balance} < {self.amount_x}"
                    )
                if token_y_balance.balance < self.amount_y:
                    return Intent.hold(
                        reason=f"Insufficient {self.token_y_symbol}: {token_y_balance.balance} < {self.amount_y}"
                    )
            except (ValueError, KeyError):
                logger.warning("Could not verify balances, proceeding anyway")

            logger.info("No position found - opening new LP position")
            add_event(
                TimelineEvent(
                    timestamp=datetime.now(UTC),
                    event_type=TimelineEventType.STATE_CHANGE,
                    description="Opening new TraderJoe LP position for JOE/AVAX accumulation",
                    strategy_id=self.strategy_id,
                    details={"action": "opening_new_position", "pool": self.pool},
                )
            )
            return self._create_open_intent(current_price)

        except Exception as e:
            logger.exception(f"Error in decide(): {e}")
            return Intent.hold(reason=f"Error: {str(e)}")

    def _create_open_intent(self, current_price: Decimal) -> Intent:
        """Create LP_OPEN intent with wide 15% range."""
        half_width = self.range_width_pct / Decimal("2")
        range_lower = current_price * (Decimal("1") - half_width)
        range_upper = current_price * (Decimal("1") + half_width)

        self._position_center_price = current_price
        self._last_rebalance_time = datetime.now(UTC)

        logger.info(
            f"LP_OPEN: {self.amount_x} {self.token_x_symbol} + {self.amount_y} {self.token_y_symbol}, "
            f"price range [{range_lower:.6f} - {range_upper:.6f}] (15% width)"
        )

        return Intent.lp_open(
            pool=self.pool,
            amount0=self.amount_x,
            amount1=self.amount_y,
            range_lower=range_lower,
            range_upper=range_upper,
            protocol="traderjoe_v2",
        )

    def _create_close_intent(self) -> Intent:
        """Create LP_CLOSE intent."""
        logger.info(f"LP_CLOSE: bins={self._position_bin_ids}")

        return Intent.lp_close(
            position_id=self.pool,
            pool=self.pool,
            collect_fees=True,
            protocol="traderjoe_v2",
        )

    def _price_to_bin_id(self, price: Decimal) -> int:
        """Convert a price to a bin ID."""
        import math

        if price <= 0:
            return BIN_ID_OFFSET - 1000000

        base = 1 + self.bin_step / 10000
        bin_id = int(math.log(float(price)) / math.log(base)) + BIN_ID_OFFSET
        return bin_id

    def _bin_id_to_price(self, bin_id: int) -> Decimal:
        """Convert a bin ID to a price."""
        base = Decimal("1") + Decimal(str(self.bin_step)) / Decimal("10000")
        exponent = bin_id - BIN_ID_OFFSET
        return base**exponent

    def on_intent_executed(self, intent: Intent, success: bool, result: Any) -> None:
        """Called after an intent is executed."""
        if success and intent.intent_type.value == "LP_OPEN":
            logger.info("TraderJoe LP position opened successfully")
            add_event(
                TimelineEvent(
                    timestamp=datetime.now(UTC),
                    event_type=TimelineEventType.LP_OPEN,
                    description=f"TraderJoe wide-range LP position opened on {self.pool}",
                    strategy_id=self.strategy_id,
                    details={
                        "pool": self.pool,
                        "bin_step": self.bin_step,
                        "range_width_pct": str(self.range_width_pct),
                    },
                )
            )
            # Track bin IDs from result if available
            if hasattr(result, "bin_ids"):
                self._position_bin_ids = result.bin_ids
            else:
                # Estimate bins based on current price and range
                center_bin = self._price_to_bin_id(self._position_center_price or Decimal("0.0133"))
                half_bins = self.num_bins // 2
                self._position_bin_ids = list(range(center_bin - half_bins, center_bin + half_bins + 1))

        elif success and intent.intent_type.value == "LP_CLOSE":
            logger.info("TraderJoe LP position closed successfully")
            add_event(
                TimelineEvent(
                    timestamp=datetime.now(UTC),
                    event_type=TimelineEventType.LP_CLOSE,
                    description=f"TraderJoe wide-range LP position closed on {self.pool}",
                    strategy_id=self.strategy_id,
                    details={"pool": self.pool},
                )
            )
            self._position_bin_ids = []
            self._position_center_price = None
            # Note: We don't reset _last_rebalance_time here - it gets set when opening new position

    def get_status(self) -> dict[str, Any]:
        """Get current strategy status."""
        days_until_time_rebalance = None
        if self._last_rebalance_time:
            time_since = datetime.now(UTC) - self._last_rebalance_time
            days_until_time_rebalance = max(0, self.rebalance_time_days - time_since.days)

        return {
            "strategy": "test_tj_wide_accumulator",
            "chain": self.chain,
            "wallet": self.wallet_address[:10] + "..." if self.wallet_address else "N/A",
            "config": {
                "pool": self.pool,
                "bin_step": self.bin_step,
                "range_width_pct": str(self.range_width_pct),
                "rebalance_price_threshold_pct": str(self.rebalance_price_threshold_pct),
                "rebalance_time_days": self.rebalance_time_days,
                "amount_x": str(self.amount_x),
                "amount_y": str(self.amount_y),
                "num_bins": self.num_bins,
            },
            "state": {
                "position_bin_ids": self._position_bin_ids,
                "position_center_price": str(self._position_center_price) if self._position_center_price else None,
                "last_rebalance_time": self._last_rebalance_time.isoformat() if self._last_rebalance_time else None,
                "days_until_time_rebalance": days_until_time_rebalance,
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

        if self._position_bin_ids:
            # Calculate estimated value
            token_x_price_usd = Decimal("0.4")  # Default JOE price
            token_y_price_usd = Decimal("30")  # Default WAVAX price

            estimated_value = self.amount_x * token_x_price_usd + self.amount_y * token_y_price_usd

            positions.append(
                PositionInfo(
                    position_type=PositionType.LP,
                    position_id=self.pool,
                    chain=self.chain,
                    protocol="traderjoe_v2",
                    value_usd=estimated_value,
                    details={
                        "pool": self.pool,
                        "bin_step": self.bin_step,
                        "bin_ids": self._position_bin_ids,
                        "amount_x": str(self.amount_x),
                        "amount_y": str(self.amount_y),
                        "range_width_pct": str(self.range_width_pct),
                        "last_rebalance_time": self._last_rebalance_time.isoformat()
                        if self._last_rebalance_time
                        else None,
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

        if self._position_bin_ids:
            logger.info(f"Generating teardown intent for TraderJoe LP position (mode={mode.value})")

            intents.append(
                Intent.lp_close(
                    position_id=self.pool,
                    pool=self.pool,
                    collect_fees=True,
                    protocol="traderjoe_v2",
                )
            )

        return intents


if __name__ == "__main__":
    print("=" * 60)
    print("TJWideAccumulatorStrategy - Test Strategy")
    print("=" * 60)
    metadata = TJWideAccumulatorStrategy.STRATEGY_METADATA
    print(f"\nStrategy Name: {TJWideAccumulatorStrategy.STRATEGY_NAME}")
    print(f"Version: {metadata.version}")
    print(f"Supported Chains: {metadata.supported_chains}")
    print(f"Supported Protocols: {metadata.supported_protocols}")
    print(f"Intent Types: {metadata.intent_types}")
    print(f"\nDescription: {metadata.description}")
    print("\nTo test on Anvil:")
    print("  python strategies/tests/lp/tj_wide_accumulator/run_anvil.py")
