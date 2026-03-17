"""
===============================================================================
TUTORIAL: TraderJoe V2 LP Strategy - Liquidity Book Position Management
===============================================================================

This is a tutorial strategy demonstrating how to manage TraderJoe Liquidity Book
positions on Avalanche. It shows the basics of discrete-bin LP management.

WHAT THIS STRATEGY DOES:
------------------------
1. Opens a liquidity position on TraderJoe V2 (Liquidity Book)
2. Provides liquidity across multiple discrete bins around the current price
3. Monitors if the position is still earning fees
4. Can close positions and withdraw liquidity

LIQUIDITY BOOK EXPLAINED:
-------------------------
TraderJoe V2 uses a novel "Liquidity Book" AMM with discrete price bins:

- Traditional AMM: Liquidity spread continuously (like Uniswap V2/V3)
- Liquidity Book: Liquidity placed in discrete bins (each bin = specific price)

Key Concepts:
- Bin: A discrete price point holding liquidity
- BinStep: Fee tier in basis points (e.g., 20 = 0.2% between bins)
- Active Bin: The bin where current price sits (earns fees)
- Fungible LP Tokens: ERC1155-like tokens per bin (not NFTs like Uniswap V3)

Benefits:
- Zero slippage within a bin
- Highly capital efficient for tight ranges
- Simpler position management (no NFT positions)
- Dynamic fees based on volatility

BIN MATH:
---------
Price at bin ID: price = (1 + binStep/10000)^(binId - 8388608)
- Bin ID 8388608 = price of 1.0
- Higher bin ID = higher price
- Lower bin ID = lower price

USAGE:
------
    # Test on Anvil (local Avalanche fork)
    python strategies/demo/traderjoe_lp/run_anvil.py

    # Run once to open a position
    python -m src.cli.run --strategy demo_traderjoe_lp --once

===============================================================================
"""

# =============================================================================
# IMPORTS
# =============================================================================

import logging
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

# Timeline API for logging
from almanak.framework.api.timeline import TimelineEvent, TimelineEventType, add_event

# Intent is what your strategy returns - describes what action to take
from almanak.framework.intents import Intent

# Core strategy framework imports
from almanak.framework.strategies import (
    IntentStrategy,
    MarketSnapshot,
    almanak_strategy,
)

# =============================================================================
# STRATEGY CONFIGURATION
# =============================================================================


@dataclass
class TraderJoeLPConfig:
    """Configuration for TraderJoe V2 LP strategy.

    This dataclass properly loads all config fields from JSON.
    """

    # Runtime config (used by CLI if no config.json)
    chain: str = "avalanche"
    network: str = "anvil"

    # Strategy-specific config
    pool: str = "WAVAX/USDC/20"
    range_width_pct: Decimal = field(default_factory=lambda: Decimal("0.10"))
    amount_x: Decimal = field(default_factory=lambda: Decimal("0.001"))
    amount_y: Decimal = field(default_factory=lambda: Decimal("3"))
    num_bins: int = 11
    force_action: str = ""
    position_id: str | None = None

    def __post_init__(self):
        """Convert string values to proper types."""
        if isinstance(self.range_width_pct, str):
            self.range_width_pct = Decimal(self.range_width_pct)
        if isinstance(self.amount_x, str):
            self.amount_x = Decimal(self.amount_x)
        if isinstance(self.amount_y, str):
            self.amount_y = Decimal(self.amount_y)
        if isinstance(self.num_bins, str):
            self.num_bins = int(self.num_bins)

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
        return UpdateResult(success=True, updated_fields=updated)


# TraderJoe V2 constants
from almanak.framework.connectors.traderjoe_v2 import BIN_ID_OFFSET

# Logging utilities for user-friendly output
from almanak.framework.utils.log_formatters import format_token_amount_human

# Logger for debugging and monitoring
logger = logging.getLogger(__name__)


# =============================================================================
# STRATEGY METADATA (via decorator)
# =============================================================================


@almanak_strategy(
    # Unique identifier - used to run via CLI
    name="demo_traderjoe_lp",
    # Human-readable description
    description="Tutorial LP strategy - manages TraderJoe V2 Liquidity Book positions on Avalanche",
    # Semantic versioning
    version="1.0.0",
    # Author
    author="Almanak",
    # Tags for categorization
    tags=["demo", "tutorial", "lp", "liquidity", "traderjoe-v2", "avalanche", "liquidity-book"],
    # Supported blockchains (TraderJoe V2 is only on Avalanche)
    supported_chains=["avalanche"],
    # Protocols this strategy interacts with
    supported_protocols=["traderjoe_v2"],
    # Types of intents this strategy may return
    intent_types=["LP_OPEN", "LP_CLOSE", "HOLD"],
    default_chain="avalanche",
)
class TraderJoeLPStrategy(IntentStrategy[TraderJoeLPConfig]):
    """
    A TraderJoe V2 Liquidity Book LP strategy for educational purposes.

    This strategy demonstrates:
    - How to open Liquidity Book positions
    - How to calculate bin ranges from price ranges
    - How to distribute liquidity across bins
    - How to close positions and collect tokens

    Configuration Parameters (from config.json):
    --------------------------------------------
    - pool: Pool identifier (e.g., "WAVAX/USDC/20")
    - range_width_pct: Total width of price range (0.20 = 20%)
    - amount_x: Amount of token X to provide (e.g., "1.0" WAVAX)
    - amount_y: Amount of token Y to provide (e.g., "30" USDC)
    - bin_step: Bin step / fee tier (e.g., 20 = 0.2%)
    - force_action: Force "open" or "close" for testing

    Example Config:
    ---------------
    {
        "pool": "WAVAX/USDC/20",
        "range_width_pct": 0.10,
        "amount_x": "1.0",
        "amount_y": "30",
        "bin_step": 20,
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
        - self.config: Strategy configuration (TraderJoeLPConfig)
        - self.chain: Blockchain to operate on
        - self.wallet_address: Wallet for transactions
        """
        super().__init__(*args, **kwargs)

        # =====================================================================
        # Extract configuration from TraderJoeLPConfig
        # =====================================================================

        # Pool configuration
        # Format: "TOKEN_X/TOKEN_Y/BIN_STEP"
        self.pool = self.config.pool

        # Parse pool to extract token symbols and bin step
        pool_parts = self.pool.split("/")
        self.token_x_symbol = pool_parts[0] if len(pool_parts) > 0 else "WAVAX"
        self.token_y_symbol = pool_parts[1] if len(pool_parts) > 1 else "USDC"
        self.bin_step = int(pool_parts[2]) if len(pool_parts) > 2 else 20

        # Range width as percentage (e.g., 0.10 = 10% total width = ±5% from current price)
        self.range_width_pct = self.config.range_width_pct

        # Token amounts to provide
        self.amount_x = self.config.amount_x  # Token X (e.g., WAVAX)
        self.amount_y = self.config.amount_y  # Token Y (e.g., USDC)

        # Force action for testing ("open" or "close")
        self.force_action = str(self.config.force_action).lower()

        # Number of bins to distribute liquidity across
        self.num_bins = self.config.num_bins

        # Internal state - track bin IDs where we have liquidity
        self._position_bin_ids: list[int] = []

        logger.info(
            f"TraderJoeLPStrategy initialized: "
            f"pool={self.pool}, "
            f"range_width={self.range_width_pct * 100}%, "
            f"amounts={self.amount_x} {self.token_x_symbol} + {self.amount_y} {self.token_y_symbol}, "
            f"bins={self.num_bins}"
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
        try:
            # =================================================================
            # STEP 1: Get current market price
            # =================================================================
            # Price is expressed as token_y per token_x
            # For WAVAX/USDC: price = USDC per WAVAX (e.g., 30)

            try:
                token_x_price_usd = market.price(self.token_x_symbol)
                token_y_price_usd = market.price(self.token_y_symbol)
                current_price = token_x_price_usd / token_y_price_usd
                logger.debug(f"Current price: {current_price:.4f} {self.token_y_symbol}/{self.token_x_symbol}")
            except (ValueError, KeyError) as e:
                logger.warning(f"Could not get price: {e}")
                # Use a reasonable default for AVAX/USDC testing
                current_price = Decimal("30")  # ~$30 per AVAX

            # =================================================================
            # STEP 2: Handle forced actions (for testing)
            # =================================================================

            if self.force_action == "open":
                logger.info("Forced action: OPEN LP position")
                return self._create_open_intent(current_price)

            elif self.force_action == "close":
                # Don't check _position_bin_ids - the adapter queries on-chain for positions
                logger.info("Forced action: CLOSE LP position (adapter will query on-chain)")
                return self._create_close_intent()

            # =================================================================
            # STEP 3: Check current position status
            # =================================================================

            if self._position_bin_ids:
                # We have a position - check if it's still in range
                # In production, you would query the pool's active bin
                return Intent.hold(reason=f"Position exists in bins {self._position_bin_ids[:3]}... - monitoring")

            # =================================================================
            # STEP 4: No position - decide whether to open one
            # =================================================================

            # Check we have sufficient balance
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

            # Open new position centered on current price
            logger.info("No position found - opening new LP position")
            add_event(
                TimelineEvent(
                    timestamp=datetime.now(UTC),
                    event_type=TimelineEventType.STATE_CHANGE,
                    description="No position found - opening new TraderJoe LP position",
                    strategy_id=self.strategy_id,
                    details={"action": "opening_new_position", "pool": self.pool},
                )
            )
            return self._create_open_intent(current_price)

        except Exception as e:
            logger.exception(f"Error in decide(): {e}")
            return Intent.hold(reason=f"Error: {str(e)}")

    # =========================================================================
    # INTENT CREATION HELPERS
    # =========================================================================

    def _create_open_intent(self, current_price: Decimal) -> Intent:
        """
        Create an LP_OPEN intent to open a new Liquidity Book position.

        Calculates the price range centered on the current price using
        the configured range_width_pct.

        For TraderJoe V2, this translates to:
        - Lower price bound -> lower bin ID
        - Upper price bound -> upper bin ID
        - Liquidity distributed across bins in range

        Parameters:
            current_price: Current price (token_y per token_x)

        Returns:
            LPOpenIntent ready for compilation
        """
        # Calculate price range
        half_width = self.range_width_pct / Decimal("2")
        range_lower = current_price * (Decimal("1") - half_width)
        range_upper = current_price * (Decimal("1") + half_width)

        logger.info(
            f"💧 LP_OPEN: {format_token_amount_human(self.amount_x, self.token_x_symbol)} + "
            f"{format_token_amount_human(self.amount_y, self.token_y_symbol)}, "
            f"price range [{range_lower:.4f} - {range_upper:.4f}], bin_step={self.bin_step}"
        )

        # Use LP_OPEN intent with traderjoe_v2 protocol
        # The compiler will handle conversion to bin-based parameters
        return Intent.lp_open(
            pool=self.pool,
            amount0=self.amount_x,
            amount1=self.amount_y,
            range_lower=range_lower,
            range_upper=range_upper,
            protocol="traderjoe_v2",
        )

    def _create_close_intent(self) -> Intent:
        """
        Create an LP_CLOSE intent to close the existing position.

        For TraderJoe V2, closing a position:
        1. Removes liquidity from all bins where we have LP tokens
        2. Returns both tokens to the wallet

        Returns:
            LPCloseIntent ready for compilation
        """
        logger.info(f"💧 LP_CLOSE: bins={self._position_bin_ids}")

        # For TraderJoe V2, we use the pool identifier as position_id
        # The adapter will query our LP token balances in each bin
        return Intent.lp_close(
            position_id=self.pool,  # Use pool as identifier
            pool=self.pool,
            collect_fees=True,
            protocol="traderjoe_v2",
        )

    # =========================================================================
    # BIN MATH UTILITIES
    # =========================================================================

    def _price_to_bin_id(self, price: Decimal) -> int:
        """
        Convert a price to a bin ID.

        Formula: binId = log(price) / log(1 + binStep/10000) + BIN_ID_OFFSET

        Parameters:
            price: Price (token_y per token_x)

        Returns:
            Bin ID corresponding to the price
        """
        import math

        if price <= 0:
            return BIN_ID_OFFSET - 1000000  # Very low bin

        base = 1 + self.bin_step / 10000
        bin_id = int(math.log(float(price)) / math.log(base)) + BIN_ID_OFFSET
        return bin_id

    def _bin_id_to_price(self, bin_id: int) -> Decimal:
        """
        Convert a bin ID to a price.

        Formula: price = (1 + binStep/10000)^(binId - BIN_ID_OFFSET)

        Parameters:
            bin_id: Bin ID

        Returns:
            Price at that bin
        """
        base = Decimal("1") + Decimal(str(self.bin_step)) / Decimal("10000")
        exponent = bin_id - BIN_ID_OFFSET
        return base**exponent

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
            # Result Enrichment: bin_ids is automatically extracted by the framework
            # No manual parsing needed - data is attached directly to the result
            bin_ids = result.bin_ids if result else None

            if bin_ids:
                self._position_bin_ids = list(bin_ids)
                logger.info(f"TraderJoe LP position opened successfully: bin_ids={bin_ids[:3]}...")
            else:
                logger.info("TraderJoe LP position opened successfully")

            add_event(
                TimelineEvent(
                    timestamp=datetime.now(UTC),
                    event_type=TimelineEventType.LP_OPEN,
                    description=f"TraderJoe LP position opened on {self.pool}",
                    strategy_id=self.strategy_id,
                    details={"pool": self.pool, "bin_step": self.bin_step, "bin_ids": bin_ids},
                )
            )

        elif success and intent.intent_type.value == "LP_CLOSE":
            logger.info("TraderJoe LP position closed successfully")
            self._position_bin_ids = []

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
            "strategy": "demo_traderjoe_lp",
            "chain": self.chain,
            "wallet": self.wallet_address[:10] + "..." if self.wallet_address else "N/A",
            "config": {
                "pool": self.pool,
                "bin_step": self.bin_step,
                "range_width_pct": str(self.range_width_pct),
                "amount_x": str(self.amount_x),
                "amount_y": str(self.amount_y),
            },
            "state": {
                "position_bin_ids": self._position_bin_ids,
            },
        }

    # =========================================================================
    # TEARDOWN SUPPORT
    # =========================================================================

    def get_open_positions(self) -> "TeardownPositionSummary":
        """Get summary of open LP positions for teardown preview."""
        from almanak.framework.teardown import (
            PositionInfo,
            PositionType,
            TeardownPositionSummary,
        )

        positions: list[PositionInfo] = []

        if self._position_bin_ids:
            # Calculate estimated value
            token_x_price_usd = Decimal("30")  # Default AVAX price
            token_y_price_usd = Decimal("1")  # Default USDC price

            estimated_value = self.amount_x * token_x_price_usd + self.amount_y * token_y_price_usd

            positions.append(
                PositionInfo(
                    position_type=PositionType.LP,
                    position_id=f"traderjoe-lp-{self.pool}-{self.chain}",
                    chain=self.chain,
                    protocol="traderjoe_v2",
                    value_usd=estimated_value,
                    details={
                        "asset": f"{self.token_x_symbol}/{self.token_y_symbol}",
                        "num_bins": len(self._position_bin_ids),
                        "pool": self.pool,
                        "bin_step": self.bin_step,
                        "bin_ids": self._position_bin_ids,
                        "amount_x": str(self.amount_x),
                        "amount_y": str(self.amount_y),
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


# =============================================================================
# TESTING
# =============================================================================

if __name__ == "__main__":
    print("=" * 60)
    print("TraderJoeLPStrategy - Demo Strategy")
    print("=" * 60)
    print(f"\nStrategy Name: {TraderJoeLPStrategy.STRATEGY_NAME}")
    print(f"Version: {TraderJoeLPStrategy.STRATEGY_METADATA.version}")
    print(f"Supported Chains: {TraderJoeLPStrategy.SUPPORTED_CHAINS}")
    print(f"Supported Protocols: {TraderJoeLPStrategy.SUPPORTED_PROTOCOLS}")
    print(f"Intent Types: {TraderJoeLPStrategy.INTENT_TYPES}")
    print(f"\nDescription: {TraderJoeLPStrategy.STRATEGY_METADATA.description}")
    print("\nTo test on Anvil:")
    print("  python strategies/demo/traderjoe_lp/run_anvil.py")
