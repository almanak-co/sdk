"""
===============================================================================
Uniswap V3 Volatility-Adaptive LP Strategy
===============================================================================

A Uniswap V3 LP strategy that dynamically adjusts range width based on ATR
(Average True Range) to adapt to market volatility conditions.

STRATEGY LOGIC:
---------------
1. Compute ATR(14) of the pool's base token price
2. Calculate adaptive range width: base_width * (1 + ATR/price)
3. Low volatility (ATR < 2%): ~5% width
4. High volatility (ATR > 5%): ~15% width
5. Rebalance when: volatility regime changes significantly OR price exits range

VOLATILITY REGIMES:
-------------------
- LOW: ATR < 2% of price - use tight 5% range
- MEDIUM: ATR 2-5% of price - use moderate 10% range
- HIGH: ATR > 5% of price - use wide 15% range

MULTI-CHAIN SUPPORT:
--------------------
Supported on: Arbitrum, Base, Optimism, Ethereum

POOL CONFIGURATION:
-------------------
- Pool: WETH/USDC/3000 (0.3% fee tier)
- Base range width: 10%
- Amount0: 0.002 WETH (~$6 at $3000)
- Amount1: 3 USDC
- Total value: ~$6

USAGE:
------
    python strategies/tests/lp/uni_vol_adaptive/run_anvil.py

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


# Volatility regime thresholds (as percentage of price)
LOW_VOL_THRESHOLD = Decimal("0.02")  # ATR < 2% = low volatility
HIGH_VOL_THRESHOLD = Decimal("0.05")  # ATR > 5% = high volatility

# Range widths for each regime
LOW_VOL_RANGE_WIDTH = Decimal("0.05")  # 5% range for low vol
MEDIUM_VOL_RANGE_WIDTH = Decimal("0.10")  # 10% range for medium vol
HIGH_VOL_RANGE_WIDTH = Decimal("0.15")  # 15% range for high vol


@dataclass
class UniVolAdaptiveConfig:
    """Configuration for Uniswap V3 Volatility-Adaptive strategy."""

    chain: str = "arbitrum"
    network: str = "anvil"
    pool: str = "WETH/USDC/3000"
    base_range_width_pct: Decimal = field(default_factory=lambda: Decimal("0.10"))
    amount0: Decimal = field(default_factory=lambda: Decimal("0.002"))  # WETH
    amount1: Decimal = field(default_factory=lambda: Decimal("3"))  # USDC
    force_action: str = ""
    position_id: str | None = None

    def __post_init__(self) -> None:
        """Convert string values to proper types."""
        if isinstance(self.base_range_width_pct, str):
            self.base_range_width_pct = Decimal(self.base_range_width_pct)
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
            "base_range_width_pct": str(self.base_range_width_pct),
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
    name="test_uni_vol_adaptive",
    description="Uniswap V3 LP strategy that dynamically adjusts range width based on ATR volatility",
    version="1.0.0",
    author="Almanak",
    tags=["test", "lp", "uniswap-v3", "volatility", "atr", "adaptive", "multi-chain"],
    supported_chains=["arbitrum", "base", "optimism", "ethereum"],
    supported_protocols=["uniswap_v3"],
    intent_types=["LP_OPEN", "LP_CLOSE", "HOLD"],
)
class UniVolAdaptiveStrategy(IntentStrategy[UniVolAdaptiveConfig]):
    """
    Uniswap V3 Volatility-Adaptive LP Strategy.

    Uses ATR (Average True Range) to dynamically adjust LP range width:
    - Low volatility: Tight range (5%) for maximum fee capture
    - High volatility: Wide range (15%) to stay in range longer

    Rebalances when:
    1. Volatility regime changes significantly (LOW -> HIGH or vice versa)
    2. Price exits the current position range
    """

    # ATR period
    ATR_PERIOD = 14

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        """Initialize the strategy."""
        super().__init__(*args, **kwargs)

        self.pool = self.config.pool
        pool_parts = self.pool.split("/")
        self.token0_symbol = pool_parts[0] if len(pool_parts) > 0 else "WETH"
        self.token1_symbol = pool_parts[1] if len(pool_parts) > 1 else "USDC"
        self.fee_tier = int(pool_parts[2]) if len(pool_parts) > 2 else 3000

        self.base_range_width_pct = self.config.base_range_width_pct
        self.amount0 = self.config.amount0
        self.amount1 = self.config.amount1
        self.force_action = str(self.config.force_action).lower()
        self.position_id = self.config.position_id

        # Internal state
        self._current_position_id: str | None = None
        self._current_volatility_regime: str = ""  # "low", "medium", "high"
        self._position_range_lower: Decimal | None = None
        self._position_range_upper: Decimal | None = None
        self._position_center_price: Decimal | None = None
        self._current_range_width: Decimal | None = None

        logger.info(
            f"UniVolAdaptiveStrategy initialized: pool={self.pool}, base_range_width={self.base_range_width_pct * 100}%, amounts={self.amount0} {self.token0_symbol} + {self.amount1} {self.token1_symbol}"
        )

    def _determine_volatility_regime(self, atr_pct: Decimal) -> str:
        """
        Determine volatility regime based on ATR as percentage of price.

        Args:
            atr_pct: ATR as percentage of price (e.g., 0.03 = 3%)

        Returns:
            "low", "medium", or "high"
        """
        if atr_pct < LOW_VOL_THRESHOLD:
            return "low"
        elif atr_pct > HIGH_VOL_THRESHOLD:
            return "high"
        else:
            return "medium"

    def _calculate_adaptive_range_width(self, volatility_regime: str) -> Decimal:
        """
        Calculate range width based on volatility regime.

        Args:
            volatility_regime: "low", "medium", or "high"

        Returns:
            Range width as decimal (e.g., 0.05 for 5%)
        """
        if volatility_regime == "low":
            return LOW_VOL_RANGE_WIDTH
        elif volatility_regime == "high":
            return HIGH_VOL_RANGE_WIDTH
        else:
            return MEDIUM_VOL_RANGE_WIDTH

    def _is_price_out_of_range(self, current_price: Decimal) -> bool:
        """
        Check if current price is outside position range.

        Returns:
            True if price is out of range
        """
        if self._position_range_lower is None or self._position_range_upper is None:
            return False

        return current_price < self._position_range_lower or current_price > self._position_range_upper

    def _has_regime_changed_significantly(self, new_regime: str) -> bool:
        """
        Check if volatility regime has changed significantly.

        Significant change = jump from low to high or vice versa
        (medium to low/high is not considered significant)

        Returns:
            True if regime changed significantly
        """
        if not self._current_volatility_regime:
            return False

        old = self._current_volatility_regime
        new = new_regime

        # Significant changes: low <-> high
        significant_changes = [
            (old == "low" and new == "high"),
            (old == "high" and new == "low"),
        ]

        return any(significant_changes)

    def decide(self, market: MarketSnapshot) -> Intent | None:
        """
        Make LP decision based on volatility analysis.

        Decision Flow:
        1. If force_action is set, execute that action
        2. Calculate ATR and determine volatility regime
        3. Calculate adaptive range width
        4. If no position exists, open one with adaptive range
        5. If regime changed significantly OR price out of range: rebalance
        6. Otherwise: hold

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
                logger.info("Forced action: OPEN LP position")
                return self._create_open_intent(current_price, MEDIUM_VOL_RANGE_WIDTH)

            elif self.force_action == "close":
                if not self.position_id and not self._current_position_id:
                    logger.warning("force_action=close but no position_id")
                    return Intent.hold(reason="Close requested but no position_id")
                logger.info("Forced action: CLOSE LP position")
                return self._create_close_intent(self.position_id or self._current_position_id or "")

            # Get ATR data from market snapshot using the unified indicator API
            try:
                atr_data = market.atr(self.token0_symbol, period=self.ATR_PERIOD)
            except ValueError as e:
                logger.warning(f"ATR indicator not available: {e}")
                # If no position yet, open with default range
                if not self._current_position_id:
                    return self._open_new_position(current_price, MEDIUM_VOL_RANGE_WIDTH, "medium")
                return Intent.hold(reason="ATR indicator not available")

            # Calculate ATR percentage and determine regime
            # Use value_percent if available, otherwise calculate
            if atr_data.value_percent and atr_data.value_percent != Decimal("0"):
                atr_pct = atr_data.value_percent / Decimal("100")
            else:
                atr_pct = atr_data.value / current_price
            volatility_regime = self._determine_volatility_regime(atr_pct)
            adaptive_range_width = self._calculate_adaptive_range_width(volatility_regime)

            logger.info(
                f"ATR Analysis: ATR={atr_data.value:.2f} ({atr_pct * 100:.2f}%), Regime={volatility_regime.upper()}, Range Width={adaptive_range_width * 100:.1f}%"
            )

            # Check if we need to open a position
            if not self._current_position_id:
                return self._open_new_position(current_price, adaptive_range_width, volatility_regime)

            # Check for rebalance conditions
            regime_changed = self._has_regime_changed_significantly(volatility_regime)
            out_of_range = self._is_price_out_of_range(current_price)

            if regime_changed:
                logger.info(
                    f"Volatility regime changed significantly: {self._current_volatility_regime} -> {volatility_regime}"
                )
                add_event(
                    TimelineEvent(
                        timestamp=datetime.now(UTC),
                        event_type=TimelineEventType.STATE_CHANGE,
                        description=f"Volatility regime changed: {self._current_volatility_regime} -> {volatility_regime}",
                        strategy_id=self.strategy_id,
                        details={
                            "trigger": "regime_change",
                            "old_regime": self._current_volatility_regime,
                            "new_regime": volatility_regime,
                            "atr": str(atr_data.value),
                            "atr_pct": str(atr_pct),
                        },
                    )
                )
                # Close current position and reopen with new range
                return self._create_close_intent(self._current_position_id)

            if out_of_range:
                logger.info(
                    f"Price {current_price:.2f} is out of range [{self._position_range_lower:.2f}, {self._position_range_upper:.2f}]"
                )
                add_event(
                    TimelineEvent(
                        timestamp=datetime.now(UTC),
                        event_type=TimelineEventType.STATE_CHANGE,
                        description="Price exited position range",
                        strategy_id=self.strategy_id,
                        details={
                            "trigger": "out_of_range",
                            "current_price": str(current_price),
                            "range_lower": str(self._position_range_lower),
                            "range_upper": str(self._position_range_upper),
                        },
                    )
                )
                # Close current position and reopen centered on new price
                return self._create_close_intent(self._current_position_id)

            # No action needed - position is in range and regime unchanged
            return Intent.hold(
                reason=f"Position in range, regime={volatility_regime}, range=[{self._position_range_lower:.2f}, {self._position_range_upper:.2f}]"
            )

        except Exception as e:
            logger.exception(f"Error in decide(): {e}")
            return Intent.hold(reason=f"Error: {str(e)}")

    def _open_new_position(self, current_price: Decimal, range_width: Decimal, volatility_regime: str) -> Intent:
        """
        Open a new LP position with given range width.

        Args:
            current_price: Current price
            range_width: Range width as decimal
            volatility_regime: Current volatility regime

        Returns:
            LP_OPEN intent
        """
        logger.info(f"Opening new position: regime={volatility_regime}, range_width={range_width * 100:.1f}%")

        add_event(
            TimelineEvent(
                timestamp=datetime.now(UTC),
                event_type=TimelineEventType.STATE_CHANGE,
                description=f"Opening LP with {volatility_regime} volatility range",
                strategy_id=self.strategy_id,
                details={
                    "action": "opening_new_position",
                    "volatility_regime": volatility_regime,
                    "range_width": str(range_width),
                    "current_price": str(current_price),
                },
            )
        )

        self._current_volatility_regime = volatility_regime
        return self._create_open_intent(current_price, range_width)

    def _create_open_intent(self, current_price: Decimal, range_width: Decimal) -> Intent:
        """Create LP_OPEN intent with calculated range."""
        half_width = range_width / Decimal("2")
        range_lower = current_price * (Decimal("1") - half_width)
        range_upper = current_price * (Decimal("1") + half_width)

        # Store position details
        self._position_range_lower = range_lower
        self._position_range_upper = range_upper
        self._position_center_price = current_price
        self._current_range_width = range_width

        logger.info(
            f"LP_OPEN: {self.amount0} {self.token0_symbol} + {self.amount1} {self.token1_symbol}, range [{range_lower:.2f} - {range_upper:.2f}]"
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
            logger.info("Uniswap V3 LP position opened successfully")
            # In production, extract actual position ID from result
            self._current_position_id = f"uni_v3_{self.pool.replace('/', '_')}"
            add_event(
                TimelineEvent(
                    timestamp=datetime.now(UTC),
                    event_type=TimelineEventType.LP_OPEN,
                    description=f"Uniswap V3 LP position opened on {self.pool}",
                    strategy_id=self.strategy_id,
                    details={
                        "pool": self.pool,
                        "volatility_regime": self._current_volatility_regime,
                        "range_width": str(self._current_range_width),
                        "range_lower": str(self._position_range_lower),
                        "range_upper": str(self._position_range_upper),
                    },
                )
            )

        elif success and intent.intent_type.value == "LP_CLOSE":
            logger.info("Uniswap V3 LP position closed successfully")
            add_event(
                TimelineEvent(
                    timestamp=datetime.now(UTC),
                    event_type=TimelineEventType.LP_CLOSE,
                    description=f"Uniswap V3 LP position closed on {self.pool}",
                    strategy_id=self.strategy_id,
                    details={
                        "pool": self.pool,
                        "volatility_regime": self._current_volatility_regime,
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
            "strategy": "test_uni_vol_adaptive",
            "chain": self.chain,
            "wallet": self.wallet_address[:10] + "..." if self.wallet_address else "N/A",
            "config": {
                "pool": self.pool,
                "base_range_width_pct": str(self.base_range_width_pct),
                "amount0": str(self.amount0),
                "amount1": str(self.amount1),
            },
            "state": {
                "current_position_id": self._current_position_id,
                "volatility_regime": self._current_volatility_regime,
                "current_range_width": str(self._current_range_width) if self._current_range_width else None,
                "position_range_lower": str(self._position_range_lower) if self._position_range_lower else None,
                "position_range_upper": str(self._position_range_upper) if self._position_range_upper else None,
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
                        "volatility_regime": self._current_volatility_regime,
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
            logger.info(f"Generating teardown intent for Uniswap V3 LP position (mode={mode.value})")

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
    print("UniVolAdaptiveStrategy - Test Strategy")
    print("=" * 60)
    print(f"\nStrategy Name: {UniVolAdaptiveStrategy.STRATEGY_NAME}")
    print(f"Version: {UniVolAdaptiveStrategy.STRATEGY_METADATA.version}")
    print(f"Supported Chains: {UniVolAdaptiveStrategy.STRATEGY_METADATA.supported_chains}")
    print(f"Supported Protocols: {UniVolAdaptiveStrategy.STRATEGY_METADATA.supported_protocols}")
    print(f"Intent Types: {UniVolAdaptiveStrategy.STRATEGY_METADATA.intent_types}")
    print(f"\nDescription: {UniVolAdaptiveStrategy.STRATEGY_METADATA.description}")
    print("\nVolatility Regimes:")
    print(f"  LOW (ATR < {LOW_VOL_THRESHOLD * 100}%): {LOW_VOL_RANGE_WIDTH * 100:.0f}% range")
    print(f"  MEDIUM: {MEDIUM_VOL_RANGE_WIDTH * 100:.0f}% range")
    print(f"  HIGH (ATR > {HIGH_VOL_THRESHOLD * 100}%): {HIGH_VOL_RANGE_WIDTH * 100:.0f}% range")
    print("\nTo test on Anvil:")
    print("  python strategies/tests/lp/uni_vol_adaptive/run_anvil.py")
