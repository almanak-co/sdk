"""
===============================================================================
TUTORIAL: Aerodrome Slipstream CL LP Strategy - Concentrated Liquidity on Base
===============================================================================

This is a tutorial strategy demonstrating how to manage Aerodrome Slipstream
concentrated liquidity (CL) positions on Base chain.

WHAT THIS STRATEGY DOES:
------------------------
1. Computes a tick range around the current market price (±range_percent %)
2. Opens a concentrated liquidity position (NFT) on Aerodrome Slipstream
3. Monitors the position
4. Can close positions and withdraw liquidity

AERODROME SLIPSTREAM EXPLAINED:
--------------------------------
Aerodrome Slipstream is the concentrated liquidity (CL) variant of Aerodrome.
Unlike the Classic AMM, Slipstream uses:

- NFT positions (ERC-721): Each LP position is a unique NFT tokenId
- Tick-based ranges: Liquidity is concentrated between tick_lower and tick_upper
- Uniswap V3-style pools: Same architecture as Uniswap V3 (tickSpacing instead of fee tier)
- Higher capital efficiency: Earn more fees when price stays in range

Pool Identification:
- Pool format: "TOKEN0/TOKEN1/tick_spacing" (e.g. "WETH/USDC/200")
- tick_spacing determines pool granularity (common values: 1, 10, 50, 100, 200)

Tick Range Computation:
- The strategy computes ticks from ±range_percent of current price
- Uses price_to_tick() from Uniswap V3 SDK utilities
- Snaps ticks to nearest tick_spacing boundary

Benefits vs Classic:
- 10-100x higher capital efficiency when price stays in range
- Higher APR from concentrated fees
- More complex management (out-of-range positions earn no fees)

USAGE:
------
    # Test on Anvil (local Base fork)
    almanak strat demo aerodrome_slipstream_lp

===============================================================================
"""

# =============================================================================
# IMPORTS
# =============================================================================

import logging
import math
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Any

# Timeline API for logging
from almanak.framework.api.timeline import TimelineEvent, TimelineEventType, add_event

# Intent is what your strategy returns - describes what action to take
from almanak.framework.intents import Intent

# Core strategy framework imports
from almanak.framework.market import MarketSnapshot
from almanak.framework.strategies import IntentStrategy, almanak_strategy

# Logging utilities for user-friendly output
from almanak.framework.utils.log_formatters import format_token_amount_human

# Logger for debugging and monitoring
logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from almanak.framework.teardown import TeardownMode, TeardownPositionSummary


# =============================================================================
# TICK UTILITIES
# =============================================================================

# Uniswap V3 tick constants (compatible with Aerodrome Slipstream)
MIN_TICK = -887272
MAX_TICK = 887272


def price_to_tick(price: Decimal, decimals0: int = 18, decimals1: int = 6) -> int:
    """Convert a token price to the nearest Uniswap V3 tick.

    Args:
        price: Price of token0 in terms of token1 (e.g. ETH/USDC price ~2500)
        decimals0: Decimals of token0 (e.g. 18 for WETH)
        decimals1: Decimals of token1 (e.g. 6 for USDC)

    Returns:
        Tick value (raw, not snapped to tick_spacing)
    """
    if price <= 0:
        return MIN_TICK
    # Adjust for decimal difference between tokens
    decimal_adjustment = 10 ** (decimals0 - decimals1)
    adjusted_price = float(price) / decimal_adjustment
    if adjusted_price <= 0:
        return MIN_TICK
    tick = math.floor(math.log(adjusted_price, 1.0001))
    return max(MIN_TICK, min(MAX_TICK, tick))


def snap_to_tick_spacing(tick: int, tick_spacing: int) -> int:
    """Snap a tick down to the nearest valid tick_spacing boundary (rounds toward -∞).

    Use for tick_lower.  For tick_upper use snap_to_tick_spacing_upper.

    Args:
        tick: Raw tick value
        tick_spacing: Pool tick spacing

    Returns:
        Tick snapped down to tick_spacing boundary
    """
    if tick_spacing <= 0:
        return tick
    return (tick // tick_spacing) * tick_spacing


def snap_to_tick_spacing_upper(tick: int, tick_spacing: int) -> int:
    """Snap a tick up to the nearest valid tick_spacing boundary (rounds toward +∞).

    Use for tick_upper to ensure the intended price range is not accidentally
    shrunk for negative ticks (floor division moves negative values farther down).

    Args:
        tick: Raw tick value
        tick_spacing: Pool tick spacing

    Returns:
        Tick snapped up to tick_spacing boundary
    """
    if tick_spacing <= 0:
        return tick
    return math.ceil(tick / tick_spacing) * tick_spacing


# =============================================================================
# CONFIGURATION CLASS
# =============================================================================


@dataclass
class AerodromeSlipstreamLPConfig:
    """Configuration for Aerodrome Slipstream CL LP strategy.

    Attributes:
        pool: Pool tokens (e.g. "WETH/USDC")
        tick_spacing: Pool tick spacing (200 for standard WETH/USDC CL pool on Base)
        amount0: Amount of token0 to provide
        amount1: Amount of token1 to provide
        range_percent: ±% range around current price for tick bounds (default 20%)
        range_lower_price: Explicit lower price bound (0 = use range_percent auto)
        range_upper_price: Explicit upper price bound (0 = use range_percent auto)
        force_action: Force "open" or "close" for dev/test only
    """

    pool: str = "WETH/USDC"
    tick_spacing: int = 200
    amount0: Decimal = field(default_factory=lambda: Decimal("0.001"))
    amount1: Decimal = field(default_factory=lambda: Decimal("3"))
    range_percent: Decimal = field(default_factory=lambda: Decimal("20"))
    range_lower_price: Decimal = field(default_factory=lambda: Decimal("0"))
    range_upper_price: Decimal = field(default_factory=lambda: Decimal("0"))
    force_action: str = ""

    def __post_init__(self) -> None:
        """Convert string values to proper types."""
        if isinstance(self.amount0, str):
            self.amount0 = Decimal(self.amount0)
        if isinstance(self.amount1, str):
            self.amount1 = Decimal(self.amount1)
        if isinstance(self.range_percent, str):
            self.range_percent = Decimal(self.range_percent)
        if isinstance(self.range_lower_price, str):
            self.range_lower_price = Decimal(self.range_lower_price)
        if isinstance(self.range_upper_price, str):
            self.range_upper_price = Decimal(self.range_upper_price)
        if isinstance(self.tick_spacing, str):
            self.tick_spacing = int(self.tick_spacing)

    def to_dict(self) -> dict:
        """Convert config to dictionary for serialization."""
        return {
            "pool": self.pool,
            "tick_spacing": self.tick_spacing,
            "amount0": str(self.amount0),
            "amount1": str(self.amount1),
            "range_percent": str(self.range_percent),
            "range_lower_price": str(self.range_lower_price),
            "range_upper_price": str(self.range_upper_price),
            "force_action": self.force_action,
        }


# =============================================================================
# STRATEGY METADATA (via decorator)
# =============================================================================


@almanak_strategy(
    name="demo_aerodrome_slipstream_lp",
    description="Tutorial LP strategy - manages Aerodrome Slipstream CL (concentrated liquidity) positions on Base",
    version="1.0.0",
    author="Almanak",
    tags=["demo", "tutorial", "lp", "liquidity", "aerodrome", "slipstream", "concentrated", "base", "clmm"],
    supported_chains=["base"],
    supported_protocols=["aerodrome_slipstream"],
    intent_types=["LP_OPEN", "LP_CLOSE", "HOLD"],
    default_chain="base",
)
class AerodromeSlipstreamLPStrategy(IntentStrategy[AerodromeSlipstreamLPConfig]):
    """
    An Aerodrome Slipstream CL LP strategy for educational purposes.

    This strategy demonstrates:
    - How to open Aerodrome Slipstream concentrated liquidity positions
    - How to compute tick ranges from current market price
    - How to track NFT tokenId positions
    - How to close CL positions and collect tokens + fees

    Configuration Parameters (from config.json):
    --------------------------------------------
    - pool: Pool token pair (e.g. "WETH/USDC")
    - tick_spacing: Pool tick spacing (200 for standard WETH/USDC CL pool)
    - amount0: Amount of token0 to provide
    - amount1: Amount of token1 to provide
    - range_percent: ±% range around current price (default 20%)
    - range_lower_price: Override lower price (0 = auto from range_percent)
    - range_upper_price: Override upper price (0 = auto from range_percent)

    Example Config:
    ---------------
    {
        "pool": "WETH/USDC",
        "tick_spacing": 200,
        "amount0": "0.001",
        "amount1": "3",
        "range_percent": "20"
    }
    """

    # =========================================================================
    # INITIALIZATION
    # =========================================================================

    def __init__(self, *args, **kwargs):
        """Initialize the CL LP strategy."""
        super().__init__(*args, **kwargs)

        self.pool = self.config.pool
        pool_parts = self.pool.split("/")
        self.token0_symbol = pool_parts[0] if len(pool_parts) > 0 else "WETH"
        self.token1_symbol = pool_parts[1] if len(pool_parts) > 1 else "USDC"

        self.tick_spacing = self.config.tick_spacing
        self.amount0 = self.config.amount0
        self.amount1 = self.config.amount1
        self.range_percent = self.config.range_percent
        self.range_lower_price = self.config.range_lower_price
        self.range_upper_price = self.config.range_upper_price
        self.force_action = self.config.force_action.lower() if self.config.force_action else ""

        # Internal state: track NFT tokenId
        self._has_position: bool = False
        self._position_token_id: str = ""

        logger.info(
            f"AerodromeSlipstreamLPStrategy initialized: "
            f"pool={self.pool}, tick_spacing={self.tick_spacing}, "
            f"amounts={self.amount0} {self.token0_symbol} + {self.amount1} {self.token1_symbol}, "
            f"range_percent=±{self.range_percent}%"
        )

    def _has_tracked_position(self) -> bool:
        """Return True if we have a tracked CL position."""
        return self._has_position or bool(self._position_token_id)

    # =========================================================================
    # TICK RANGE COMPUTATION
    # =========================================================================

    def _compute_tick_range(self, current_price: Decimal) -> tuple[int, int]:
        """Compute tick_lower and tick_upper from current price and range_percent.

        Uses explicit price bounds if configured, otherwise computes ±range_percent
        around current_price. Ticks are snapped to tick_spacing boundaries.

        Args:
            current_price: Current price of token0 in terms of token1

        Returns:
            Tuple of (tick_lower, tick_upper) snapped to tick_spacing
        """
        # Determine price bounds
        if self.range_lower_price > 0 and self.range_upper_price > 0:
            lower_price = self.range_lower_price
            upper_price = self.range_upper_price
        else:
            range_factor = self.range_percent / Decimal("100")
            lower_price = current_price * (Decimal("1") - range_factor)
            upper_price = current_price * (Decimal("1") + range_factor)

        # Derive token decimals (WETH=18, USDC=6 standard; resolver used in compiler)
        # For tick computation we need decimals0 and decimals1.
        # Strategy uses symbolic tokens — assume standard values as a best-effort.
        # The compiler resolves exact decimals on-chain; these are for tick estimation only.
        decimals0 = 18  # WETH / ETH-like
        decimals1 = 6  # USDC / stablecoin-like
        if self.token1_symbol in ("WETH", "wETH", "ETH"):
            decimals1 = 18
            decimals0 = 6

        tick_lower_raw = price_to_tick(lower_price, decimals0=decimals0, decimals1=decimals1)
        tick_upper_raw = price_to_tick(upper_price, decimals0=decimals0, decimals1=decimals1)

        # Snap tick_lower down and tick_upper up to ensure current price stays in range.
        # snap_to_tick_spacing floors toward -∞; for negative ticks this moves the upper
        # bound farther negative (shrinking the range) so we use the ceiling variant instead.
        tick_lower = snap_to_tick_spacing(tick_lower_raw, self.tick_spacing)
        tick_upper = snap_to_tick_spacing_upper(tick_upper_raw, self.tick_spacing)

        # Ensure tick_lower < tick_upper and both within bounds
        if tick_lower >= tick_upper:
            tick_lower = tick_upper - self.tick_spacing
        tick_lower = max(MIN_TICK, tick_lower)
        tick_upper = min(MAX_TICK, tick_upper)

        logger.debug(
            f"Tick range: price={current_price:.4f}, "
            f"bounds=[{lower_price:.4f}, {upper_price:.4f}], "
            f"ticks=[{tick_lower}, {tick_upper}] (spacing={self.tick_spacing})"
        )
        return tick_lower, tick_upper

    # =========================================================================
    # MAIN DECISION LOGIC
    # =========================================================================

    def decide(self, market: MarketSnapshot) -> Intent | None:
        """
        Make a CL LP decision based on market conditions.

        Decision Flow:
        1. If force_action is set, execute that action
        2. If no position exists, open one
        3. If position exists, hold and monitor
        """
        # Get current market price
        try:
            token0_price_usd = market.price(self.token0_symbol)
            token1_price_usd = market.price(self.token1_symbol)
            current_price = token0_price_usd / token1_price_usd
            logger.debug(f"Current price: {current_price:.4f} {self.token1_symbol}/{self.token0_symbol}")
        except (ValueError, KeyError) as e:
            logger.warning(f"Could not get price for {self.token0_symbol}/{self.token1_symbol}: {e}")
            return Intent.hold(reason=f"Price unavailable for {self.token0_symbol}/{self.token1_symbol}: {e}")

        # Handle forced actions (dev/test only)
        if self.force_action == "open":
            logger.info("Forced action: OPEN CL LP position")
            return self._create_open_intent(current_price)
        elif self.force_action == "close":
            logger.info("Forced action: CLOSE CL LP position")
            return self._create_close_intent()

        # Check current position status
        if self._has_tracked_position():
            return Intent.hold(reason=f"CL position exists (tokenId={self._position_token_id}) - monitoring")

        # Check sufficient balance
        try:
            token0_bal = market.balance(self.token0_symbol)
            token1_bal = market.balance(self.token1_symbol)
            if token0_bal.balance < self.amount0:
                return Intent.hold(reason=f"Insufficient {self.token0_symbol}: {token0_bal.balance} < {self.amount0}")
            if token1_bal.balance < self.amount1:
                return Intent.hold(reason=f"Insufficient {self.token1_symbol}: {token1_bal.balance} < {self.amount1}")
        except (ValueError, KeyError, AttributeError):
            logger.warning("Could not verify balances, proceeding anyway")

        # Open new CL position
        logger.info("No CL position found - opening new Slipstream LP position")
        return self._create_open_intent(current_price)

    # =========================================================================
    # INTENT CREATION HELPERS
    # =========================================================================

    def _create_open_intent(self, current_price: Decimal) -> Intent:
        """Create an LP_OPEN intent for Aerodrome Slipstream CL.

        Pool format for Slipstream: "TOKEN0/TOKEN1/tick_spacing"
        Tick range is computed from current_price ± range_percent.

        Args:
            current_price: Current price of token0 in token1 units

        Returns:
            LPOpenIntent ready for compilation
        """
        tick_lower, tick_upper = self._compute_tick_range(current_price)

        pool_with_spacing = f"{self.pool}/{self.tick_spacing}"

        logger.info(
            f"LP_OPEN (Slipstream CL): {format_token_amount_human(self.amount0, self.token0_symbol)} + "
            f"{format_token_amount_human(self.amount1, self.token1_symbol)}, "
            f"pool={pool_with_spacing}, ticks=[{tick_lower},{tick_upper}]"
        )

        return Intent.lp_open(
            pool=pool_with_spacing,
            amount0=self.amount0,
            amount1=self.amount1,
            range_lower=Decimal(str(tick_lower)),
            range_upper=Decimal(str(tick_upper)),
            protocol="aerodrome_slipstream",
        )

    def _create_close_intent(self) -> Intent:
        """Create an LP_CLOSE intent for Aerodrome Slipstream CL.

        Uses the tracked NFT tokenId as position_id.

        Returns:
            LPCloseIntent ready for compilation
        """
        position_id = self._position_token_id or "0"
        logger.info(f"LP_CLOSE (Slipstream CL): tokenId={position_id}")

        return Intent.lp_close(
            position_id=position_id,
            pool=f"{self.pool}/{self.tick_spacing}",
            collect_fees=True,
            protocol="aerodrome_slipstream",
        )

    # =========================================================================
    # LIFECYCLE HOOKS
    # =========================================================================

    def on_intent_executed(self, intent: Intent, success: bool, result: Any) -> None:
        """Called after an intent is executed."""
        if success and intent.intent_type.value == "LP_OPEN":
            # Extract tokenId first — only mark the position as tracked when we have it.
            # Without a tokenId we cannot emit LP_CLOSE later, so treat this as untracked.
            try:
                position_id = getattr(result, "position_id", None)
                if position_id and str(position_id) not in ("None", ""):
                    self._position_token_id = str(position_id)
                    self._has_position = True
                    logger.info(f"CL position opened: tokenId={self._position_token_id}")
                else:
                    logger.error(
                        "LP_OPEN succeeded on-chain but tokenId was not extracted from the result. "
                        "The position exists but cannot be automatically closed. "
                        "Check receipt parser logs and recover the tokenId manually."
                    )
            except Exception as e:
                logger.error(f"LP_OPEN succeeded but failed to extract tokenId — position not tracked: {e}")

            add_event(
                TimelineEvent(
                    timestamp=datetime.now(UTC),
                    event_type=TimelineEventType.POSITION_OPENED,
                    description=f"Aerodrome Slipstream CL position opened on {self.pool}",
                    deployment_id=self.deployment_id,
                    details={
                        "pool": self.pool,
                        "tick_spacing": self.tick_spacing,
                        "token_id": self._position_token_id,
                    },
                )
            )

        elif success and intent.intent_type.value == "LP_CLOSE":
            logger.info("Aerodrome Slipstream CL position closed successfully")
            self._has_position = False
            self._position_token_id = ""

    # =========================================================================
    # STATE PERSISTENCE
    # =========================================================================

    def get_persistent_state(self) -> dict[str, Any]:
        """Persist CL position state for restart recovery."""
        parent_get_state = getattr(super(), "get_persistent_state", None)
        state = parent_get_state() if callable(parent_get_state) else {}
        state["has_position"] = self._has_tracked_position()
        state["position_token_id"] = self._position_token_id
        return state

    def load_persistent_state(self, state: dict[str, Any]) -> None:
        """Restore persisted CL position state."""
        parent_load_state = getattr(super(), "load_persistent_state", None)
        if callable(parent_load_state):
            parent_load_state(state)

        raw_has_position = state.get("has_position", False)
        if isinstance(raw_has_position, str):
            self._has_position = raw_has_position.strip().lower() in {"1", "true", "yes", "on"}
        else:
            self._has_position = bool(raw_has_position)

        self._position_token_id = str(state.get("position_token_id", ""))
        if self._position_token_id and self._position_token_id not in ("", "None", "0"):
            self._has_position = True

    # =========================================================================
    # STATUS REPORTING
    # =========================================================================

    def get_status(self) -> dict[str, Any]:
        """Get current strategy status."""
        return {
            "strategy": "demo_aerodrome_slipstream_lp",
            "chain": self.chain,
            "wallet": self.wallet_address[:10] + "..." if self.wallet_address else "N/A",
            "config": {
                "pool": self.pool,
                "tick_spacing": self.tick_spacing,
                "amount0": str(self.amount0),
                "amount1": str(self.amount1),
                "range_percent": str(self.range_percent),
            },
            "state": {
                "has_position": self._has_position,
                "position_token_id": self._position_token_id,
            },
        }

    # =========================================================================
    # TEARDOWN SUPPORT
    # =========================================================================

    def get_open_positions(self) -> "TeardownPositionSummary":
        """Get summary of open CL positions for teardown preview."""
        from almanak.framework.teardown import (
            PositionInfo,
            PositionType,
            TeardownPositionSummary,
        )

        positions: list[PositionInfo] = []

        if self._has_tracked_position():
            token0_price_usd = Decimal("2500")
            token1_price_usd = Decimal("1")
            estimated_value = self.amount0 * token0_price_usd + self.amount1 * token1_price_usd

            positions.append(
                PositionInfo(
                    position_type=PositionType.LP,
                    position_id=self._position_token_id or "unknown",
                    chain=self.chain,
                    protocol="aerodrome_slipstream",
                    value_usd=estimated_value,
                    details={
                        "asset": f"{self.token0_symbol}/{self.token1_symbol}",
                        "pool": self.pool,
                        "tick_spacing": self.tick_spacing,
                        "token_id": self._position_token_id,
                        "amount0": str(self.amount0),
                        "amount1": str(self.amount1),
                    },
                )
            )

        total_value = sum(p.value_usd for p in positions)

        return TeardownPositionSummary(
            deployment_id=self.deployment_id,
            timestamp=datetime.now(UTC),
            total_value_usd=total_value,
            positions=positions,
        )

    def generate_teardown_intents(self, mode: "TeardownMode", market=None) -> list[Intent]:
        """Generate intents to close all CL positions."""
        intents: list[Intent] = []

        if self._has_tracked_position():
            position_id = self._position_token_id or "0"
            logger.info(
                f"Generating teardown intent for Slipstream CL position (mode={mode.value}, tokenId={position_id})"
            )

            intents.append(
                Intent.lp_close(
                    position_id=position_id,
                    pool=f"{self.pool}/{self.tick_spacing}",
                    collect_fees=True,
                    protocol="aerodrome_slipstream",
                )
            )

        return intents


# =============================================================================
# TESTING
# =============================================================================

if __name__ == "__main__":
    print("=" * 60)
    print("AerodromeSlipstreamLPStrategy - Demo Strategy")
    print("=" * 60)
    print(f"\nStrategy Name: {AerodromeSlipstreamLPStrategy.STRATEGY_NAME}")
    print(f"Version: {AerodromeSlipstreamLPStrategy.STRATEGY_METADATA.version}")
    print(f"Supported Chains: {AerodromeSlipstreamLPStrategy.STRATEGY_METADATA.supported_chains}")
    print(f"Supported Protocols: {AerodromeSlipstreamLPStrategy.STRATEGY_METADATA.supported_protocols}")
    print(f"Intent Types: {AerodromeSlipstreamLPStrategy.STRATEGY_METADATA.intent_types}")
    print(f"\nDescription: {AerodromeSlipstreamLPStrategy.STRATEGY_METADATA.description}")
