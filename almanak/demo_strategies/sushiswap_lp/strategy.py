"""
===============================================================================
TUTORIAL: SushiSwap V3 LP Strategy - Concentrated Liquidity Position Management
===============================================================================

This is a tutorial strategy demonstrating how to manage SushiSwap V3 concentrated
liquidity positions on Arbitrum. It shows the basics of tick-based LP management.

WHAT THIS STRATEGY DOES:
------------------------
1. Opens a liquidity position on SushiSwap V3 (concentrated liquidity)
2. Provides liquidity within a price range defined by ticks
3. Monitors if the position is still in range
4. Can close positions and collect fees

SUSHISWAP V3 EXPLAINED (Uniswap V3 Fork):
-----------------------------------------
SushiSwap V3 uses concentrated liquidity where:
- Liquidity is provided within a specific price range (not full range)
- Price ranges are defined by "ticks"
- LP positions are NFTs (non-fungible tokens)

Key Concepts:
- Tick: A discrete price point (price = 1.0001^tick)
- Tick Spacing: Minimum tick increment based on fee tier
- sqrtPriceX96: Square root of price * 2^96 (used internally)
- Position NFT: Each LP position is a unique NFT with tokenId

Fee Tiers:
- 0.01% (100): tick spacing = 1 (stablecoin pairs)
- 0.05% (500): tick spacing = 10 (stable pairs)
- 0.30% (3000): tick spacing = 60 (most pairs)
- 1.00% (10000): tick spacing = 200 (exotic pairs)

USAGE:
------
    # Test on Anvil (local Arbitrum fork)
    python strategies/demo/sushiswap_lp/run_anvil.py

    # Run once to open a position
    almanak strat run -d strategies/demo/sushiswap_lp --once

===============================================================================
"""

# =============================================================================
# IMPORTS
# =============================================================================

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from almanak.framework.teardown import TeardownMode, TeardownPositionSummary

# Timeline API for logging
from almanak.framework.api.timeline import TimelineEvent, TimelineEventType, add_event

# SushiSwap V3 utilities
from almanak.framework.connectors.sushiswap_v3 import (
    get_max_tick,
    get_min_tick,
    get_nearest_tick,
    price_to_tick,
    tick_to_price,
)

# Intent is what your strategy returns - describes what action to take
from almanak.framework.intents import Intent

# Core strategy framework imports
from almanak.framework.strategies import (
    IntentStrategy,
    MarketSnapshot,
    almanak_strategy,
)

# Logging utilities for user-friendly output
from almanak.framework.utils.log_formatters import format_token_amount_human

# Logger for debugging and monitoring
logger = logging.getLogger(__name__)


# =============================================================================
# STRATEGY CONFIGURATION
# =============================================================================


@dataclass
class SushiSwapLPConfig:
    """Configuration for SushiSwap V3 LP strategy.

    This dataclass properly loads all config fields from JSON.
    """

    # Runtime config (used by CLI if no config.json)
    chain: str = "arbitrum"
    network: str = "anvil"

    # Strategy-specific config
    # Pool format: TOKEN0/TOKEN1/FEE - token0 is the lower address (WETH < USDC on Arbitrum)
    pool: str = "WETH/USDC/3000"
    range_width_pct: Decimal = field(default_factory=lambda: Decimal("0.10"))
    amount0: Decimal = field(default_factory=lambda: Decimal("0.03"))  # WETH (token0)
    amount1: Decimal = field(default_factory=lambda: Decimal("100"))  # USDC (token1)
    fee_tier: int = 3000
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

    def to_dict(self) -> dict[str, Any]:
        """Convert config to dictionary."""
        return {
            "chain": self.chain,
            "network": self.network,
            "pool": self.pool,
            "range_width_pct": str(self.range_width_pct),
            "amount0": str(self.amount0),
            "amount1": str(self.amount1),
            "fee_tier": self.fee_tier,
            "force_action": self.force_action,
            "position_id": self.position_id,
        }

    def update(self, **kwargs: Any) -> Any:
        """Update configuration values."""

        @dataclass
        class UpdateResult:
            success: bool = True
            updated_fields: list = field(default_factory=list)

        updated = []
        for k, v in kwargs.items():
            if hasattr(self, k):
                setattr(self, k, v)
                updated.append(k)

        # Re-run type normalization after updates
        if updated:
            self.__post_init__()

        return UpdateResult(success=True, updated_fields=updated)


# =============================================================================
# STRATEGY METADATA (via decorator)
# =============================================================================


@almanak_strategy(
    # Unique identifier - used to run via CLI
    name="demo_sushiswap_lp",
    # Human-readable description
    description="Tutorial LP strategy - manages SushiSwap V3 concentrated liquidity positions on Arbitrum",
    # Semantic versioning
    version="1.0.0",
    # Author
    author="Almanak",
    # Tags for categorization
    tags=["demo", "tutorial", "lp", "liquidity", "sushiswap-v3", "arbitrum", "concentrated-liquidity"],
    # Supported blockchains (SushiSwap V3 is on multiple chains, primary is Arbitrum)
    supported_chains=["arbitrum", "ethereum", "base", "polygon", "avalanche", "optimism"],
    # Protocols this strategy interacts with
    supported_protocols=["sushiswap_v3"],
    # Types of intents this strategy may return
    intent_types=["LP_OPEN", "LP_CLOSE", "HOLD"],
    default_chain="arbitrum",
)
class SushiSwapLPStrategy(IntentStrategy[SushiSwapLPConfig]):
    """
    A SushiSwap V3 concentrated liquidity LP strategy for educational purposes.

    This strategy demonstrates:
    - How to open concentrated liquidity positions
    - How to calculate tick ranges from price ranges
    - How to manage LP position NFTs
    - How to close positions and collect fees

    Configuration Parameters (from config.json):
    --------------------------------------------
    - pool: Pool identifier (e.g., "WETH/USDC/3000" where WETH is token0)
    - range_width_pct: Total width of price range (0.10 = 10%)
    - amount0: Amount of token0 to provide (e.g., "0.03" WETH)
    - amount1: Amount of token1 to provide (e.g., "100" USDC)
    - fee_tier: Fee tier (100, 500, 3000, or 10000)
    - force_action: Force "open" or "close" for testing

    Example Config:
    ---------------
    {
        "pool": "WETH/USDC/3000",
        "range_width_pct": 0.10,
        "amount0": "0.03",
        "amount1": "100",
        "fee_tier": 3000,
        "force_action": "open"
    }
    """

    # =========================================================================
    # INITIALIZATION
    # =========================================================================

    def __init__(self, *args, **kwargs):
        """
        Initialize the LP strategy with configuration.

        The base class handles standard parameters:
        - self.config: Strategy configuration (SushiSwapLPConfig)
        - self.chain: Blockchain to operate on
        - self.wallet_address: Wallet for transactions
        """
        super().__init__(*args, **kwargs)

        # =====================================================================
        # Extract configuration from SushiSwapLPConfig
        # =====================================================================

        # Pool configuration
        # Format: "TOKEN0/TOKEN1/FEE_TIER"
        self.pool = self.config.pool

        # Parse pool to extract token symbols and fee tier
        pool_parts = self.pool.split("/")
        self.token0_symbol = pool_parts[0] if len(pool_parts) > 0 else "WETH"
        self.token1_symbol = pool_parts[1] if len(pool_parts) > 1 else "USDC"
        self.fee_tier = int(pool_parts[2]) if len(pool_parts) > 2 else self.config.fee_tier

        # Range width as percentage (e.g., 0.10 = 10% total width = +/-5% from current price)
        self.range_width_pct = self.config.range_width_pct

        # Token amounts to provide
        self.amount0 = self.config.amount0  # Token0 (e.g., WETH)
        self.amount1 = self.config.amount1  # Token1 (e.g., USDC)

        # Force action for testing ("open" or "close")
        self.force_action = str(self.config.force_action).lower()

        # Internal state - track position NFT
        self._position_id: int | None = self.config.position_id
        self._liquidity: int | None = None
        self._tick_lower: int | None = None
        self._tick_upper: int | None = None

        logger.info(
            f"SushiSwapLPStrategy initialized: "
            f"pool={self.pool}, "
            f"range_width={self.range_width_pct * 100}%, "
            f"amounts={self.amount0} {self.token0_symbol} + {self.amount1} {self.token1_symbol}"
        )

    # =========================================================================
    # MAIN DECISION LOGIC
    # =========================================================================

    def decide(self, market: MarketSnapshot) -> Intent | None:
        """
        Make an LP decision based on market conditions.

        Decision Flow:
        --------------
        1. If force_action is set, execute that action
        2. If no position exists, open one
        3. If position is out of range, close and re-open
        4. Otherwise, hold

        Parameters:
            market: MarketSnapshot containing prices, balances, etc.

        Returns:
            Intent: LP_OPEN, LP_CLOSE, or HOLD
        """
        # =================================================================
        # STEP 1: Get current market price
        # =================================================================
        # Price is expressed as token1 per token0 (USDC per WETH for WETH/USDC pool)
        # This gives us the ETH price in USD terms (e.g., ~3400)

        try:
            token0_price_usd = market.price(self.token0_symbol)
            token1_price_usd = market.price(self.token1_symbol)
            # Guard against division by zero
            if token1_price_usd == Decimal("0"):
                logger.warning(f"Token1 price is zero for {self.token1_symbol}")
                return Intent.hold(reason=f"Price data unavailable: {self.token1_symbol} price is zero")
            else:
                # V3 pool price = token1 per token0 (e.g., USDC per WETH)
                # To get this from USD prices: token0_usd / token1_usd
                current_price = token0_price_usd / token1_price_usd
            logger.debug(f"Current price: {current_price:.4f} {self.token1_symbol}/{self.token0_symbol}")
        except (ValueError, KeyError) as e:
            logger.warning(f"Could not get price: {e}")
            return Intent.hold(reason=f"Price data unavailable: {e}")

        # =================================================================
        # STEP 2: Handle forced actions (for testing)
        # =================================================================

        if self.force_action == "open":
            logger.info("Forced action: OPEN LP position")
            return self._create_open_intent(current_price)

        elif self.force_action == "close":
            if not self._position_id:
                logger.warning("force_action=close but no position tracked")
                return Intent.hold(reason="Close requested but no position tracked")
            logger.info("Forced action: CLOSE LP position")
            return self._create_close_intent()

        # =================================================================
        # STEP 3: Check current position status
        # =================================================================

        if self._position_id:
            # We have a position - check if it's still in range
            # In production, you would query the pool's current tick
            return Intent.hold(
                reason=f"Position {self._position_id} exists in range [{self._tick_lower}, {self._tick_upper}] - monitoring"
            )

        # =================================================================
        # STEP 4: No position - decide whether to open one
        # =================================================================

        # Check we have sufficient balance
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

        # Open new position centered on current price
        logger.info("No position found - opening new LP position")
        add_event(
            TimelineEvent(
                timestamp=datetime.now(UTC),
                event_type=TimelineEventType.STATE_CHANGE,
                description="No position found - opening new SushiSwap V3 LP position",
                strategy_id=self.strategy_id,
                details={"action": "opening_new_position", "pool": self.pool},
            )
        )
        return self._create_open_intent(current_price)

    # =========================================================================
    # INTENT CREATION HELPERS
    # =========================================================================

    def _create_open_intent(self, current_price: Decimal) -> Intent:
        """
        Create an LP_OPEN intent to open a new concentrated liquidity position.

        Calculates the tick range centered on the current price using
        the configured range_width_pct.

        For SushiSwap V3, this translates to:
        - Lower price bound -> lower tick
        - Upper price bound -> upper tick
        - Liquidity provided within that range

        Parameters:
            current_price: Current price (token0 per token1)

        Returns:
            LPOpenIntent ready for compilation
        """
        # Calculate price range
        half_width = self.range_width_pct / Decimal("2")
        range_lower = current_price * (Decimal("1") - half_width)
        range_upper = current_price * (Decimal("1") + half_width)

        # Convert price range to ticks
        # Look up decimals from the token resolver (works for any token on any chain)
        from almanak.framework.data.tokens import get_token_resolver

        resolver = get_token_resolver()
        decimals0 = resolver.get_decimals(self.chain, self.token0_symbol)
        decimals1 = resolver.get_decimals(self.chain, self.token1_symbol)
        tick_lower = get_nearest_tick(price_to_tick(range_lower, decimals0, decimals1), self.fee_tier)
        tick_upper = get_nearest_tick(price_to_tick(range_upper, decimals0, decimals1), self.fee_tier)

        # Ensure ticks are within valid range
        min_tick = get_min_tick(self.fee_tier)
        max_tick = get_max_tick(self.fee_tier)
        tick_lower = max(tick_lower, min_tick)
        tick_upper = min(tick_upper, max_tick)

        # Validate tick range is not collapsed
        if tick_lower >= tick_upper:
            raise ValueError(
                f"Invalid tick range after clamping: tick_lower={tick_lower} >= tick_upper={tick_upper}. "
                f"Try widening range_width_pct (currently {self.range_width_pct}) or check token decimals."
            )

        # Store for tracking
        self._tick_lower = tick_lower
        self._tick_upper = tick_upper

        logger.info(
            f"LP_OPEN: {format_token_amount_human(self.amount0, self.token0_symbol)} + "
            f"{format_token_amount_human(self.amount1, self.token1_symbol)}, "
            f"price range [{range_lower:.4f} - {range_upper:.4f}], "
            f"ticks [{tick_lower} - {tick_upper}]"
        )

        # Use LP_OPEN intent with sushiswap_v3 protocol
        return Intent.lp_open(
            pool=self.pool,
            amount0=self.amount0,
            amount1=self.amount1,
            range_lower=range_lower,
            range_upper=range_upper,
            protocol="sushiswap_v3",
        )

    def _create_close_intent(self) -> Intent:
        """
        Create an LP_CLOSE intent to close the existing position.

        For SushiSwap V3, closing a position:
        1. Decreases liquidity to 0
        2. Collects all tokens and fees

        Returns:
            LPCloseIntent ready for compilation
        """
        logger.info(f"LP_CLOSE: position_id={self._position_id}")

        return Intent.lp_close(
            position_id=str(self._position_id),
            pool=self.pool,
            collect_fees=True,
            protocol="sushiswap_v3",
        )

    # =========================================================================
    # TICK MATH UTILITIES
    # =========================================================================

    def _price_to_tick(self, price: Decimal, decimals0: int = 18, decimals1: int = 18) -> int:
        """
        Convert a price to a tick.

        Formula: tick = log(price) / log(1.0001)

        Parameters:
            price: Price (token0 per token1)
            decimals0: Decimals for token0 (default 18)
            decimals1: Decimals for token1 (default 18)

        Returns:
            Tick corresponding to the price
        """
        return price_to_tick(price, decimals0, decimals1)

    def _tick_to_price(self, tick: int, decimals0: int = 18, decimals1: int = 18) -> Decimal:
        """
        Convert a tick to a price.

        Formula: price = 1.0001^tick

        Parameters:
            tick: Tick value
            decimals0: Decimals for token0 (default 18)
            decimals1: Decimals for token1 (default 18)

        Returns:
            Price at that tick
        """
        return tick_to_price(tick, decimals0, decimals1)

    # =========================================================================
    # LIFECYCLE HOOKS
    # =========================================================================

    def on_intent_executed(self, intent: Intent, success: bool, result: Any) -> None:
        """
        Called after an intent is executed.

        Parameters:
            intent: The intent that was executed
            success: Whether execution succeeded
            result: Execution result
        """
        if success and intent.intent_type.value == "LP_OPEN":
            # Result Enrichment: position_id is automatically extracted by the framework
            position_id = result.position_id if result else None
            liquidity = result.extracted_data.get("liquidity") if result else None

            if position_id:
                self._position_id = int(position_id)
                self._liquidity = liquidity
                logger.info(f"SushiSwap V3 LP position opened: position_id={position_id}, liquidity={liquidity}")
            else:
                logger.info("SushiSwap V3 LP position opened successfully")

            add_event(
                TimelineEvent(
                    timestamp=datetime.now(UTC),
                    event_type=TimelineEventType.LP_OPEN,
                    description=f"SushiSwap V3 LP position opened on {self.pool}",
                    strategy_id=self.strategy_id,
                    details={
                        "pool": self.pool,
                        "fee_tier": self.fee_tier,
                        "position_id": position_id,
                        "liquidity": liquidity,
                        "tick_lower": self._tick_lower,
                        "tick_upper": self._tick_upper,
                    },
                )
            )

        elif success and intent.intent_type.value == "LP_CLOSE":
            logger.info(f"SushiSwap V3 LP position {self._position_id} closed successfully")
            self._position_id = None
            self._liquidity = None
            self._tick_lower = None
            self._tick_upper = None

    # =========================================================================
    # STATUS REPORTING
    # =========================================================================

    def get_status(self) -> dict[str, Any]:
        """
        Get current strategy status for monitoring/dashboards.

        Returns:
            Dictionary with strategy status information
        """
        return {
            "strategy": "demo_sushiswap_lp",
            "chain": self.chain,
            "wallet": self.wallet_address[:10] + "..." if self.wallet_address else "N/A",
            "config": {
                "pool": self.pool,
                "fee_tier": self.fee_tier,
                "range_width_pct": str(self.range_width_pct),
                "amount0": str(self.amount0),
                "amount1": str(self.amount1),
            },
            "state": {
                "position_id": self._position_id,
                "liquidity": self._liquidity,
                "tick_lower": self._tick_lower,
                "tick_upper": self._tick_upper,
            },
        }

    # =========================================================================
    # TEARDOWN SUPPORT
    # =========================================================================

    def get_open_positions(self) -> TeardownPositionSummary:
        """Get summary of open LP positions for teardown preview."""
        from almanak.framework.teardown import (
            PositionInfo,
            PositionType,
            TeardownPositionSummary,
        )

        positions: list[PositionInfo] = []

        if self._position_id:
            # Calculate estimated value using live prices
            try:
                snapshot = self.create_market_snapshot()
                token0_price_usd = snapshot.price(self.token0_symbol)
                token1_price_usd = snapshot.price(self.token1_symbol)
            except Exception:  # noqa: BLE001
                logger.debug("Could not get live prices for LP value estimate, using fallback $0")
                token0_price_usd = Decimal("0")
                token1_price_usd = Decimal("0")

            estimated_value = self.amount0 * token0_price_usd + self.amount1 * token1_price_usd

            positions.append(
                PositionInfo(
                    position_type=PositionType.LP,
                    position_id=f"sushiswap-lp-{self._position_id}-{self.chain}",
                    chain=self.chain,
                    protocol="sushiswap_v3",
                    value_usd=estimated_value,
                    details={
                        "asset": f"{self.token0_symbol}/{self.token1_symbol}",
                        "liquidity": str(self._liquidity or 0),
                        "pool": self.pool,
                        "fee_tier": self.fee_tier,
                        "nft_position_id": self._position_id,
                        "tick_lower": self._tick_lower,
                        "tick_upper": self._tick_upper,
                        "amount0": str(self.amount0),
                        "amount1": str(self.amount1),
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

    def generate_teardown_intents(self, mode: TeardownMode, market=None) -> list[Intent]:
        """Generate intents to close all LP positions."""

        intents: list[Intent] = []

        if self._position_id:
            logger.info(f"Generating teardown intent for SushiSwap V3 LP position (mode={mode.value})")

            intents.append(
                Intent.lp_close(
                    position_id=str(self._position_id),
                    pool=self.pool,
                    collect_fees=True,
                    protocol="sushiswap_v3",
                )
            )

        return intents


# =============================================================================
# TESTING
# =============================================================================

if __name__ == "__main__":
    print("=" * 60)
    print("SushiSwapLPStrategy - Demo Strategy")
    print("=" * 60)
    print(f"\nStrategy Name: {SushiSwapLPStrategy.STRATEGY_NAME}")
    print(f"Version: {SushiSwapLPStrategy.STRATEGY_METADATA.version}")
    print(f"Supported Chains: {SushiSwapLPStrategy.STRATEGY_METADATA.supported_chains}")
    print(f"Supported Protocols: {SushiSwapLPStrategy.STRATEGY_METADATA.supported_protocols}")
    print(f"Intent Types: {SushiSwapLPStrategy.STRATEGY_METADATA.intent_types}")
    print(f"\nDescription: {SushiSwapLPStrategy.STRATEGY_METADATA.description}")
    print("\nTo test on Anvil:")
    print("  python strategies/demo/sushiswap_lp/run_anvil.py")
