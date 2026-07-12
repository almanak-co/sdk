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
    python almanak/demo_strategies/traderjoe_lp/run_anvil.py

    # Run once to open a position
    python -m src.cli.run --strategy demo_traderjoe_lp --once

===============================================================================
"""

# =============================================================================
# IMPORTS
# =============================================================================

import logging
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import TYPE_CHECKING, Any

# Timeline API for logging
from almanak.framework.api.timeline import TimelineEvent, TimelineEventType, add_event

# Intent is what your strategy returns - describes what action to take
from almanak.framework.intents import AnyIntent, Intent

# Core strategy framework imports
from almanak.framework.market import MarketSnapshot
from almanak.framework.strategies import IntentStrategy, almanak_strategy

if TYPE_CHECKING:
    from almanak.framework.teardown import TeardownMode, TeardownPositionSummary

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
    # Minimum total inventory (USD) required to (re)open a position
    min_position_usd: Decimal = field(default_factory=lambda: Decimal("100"))
    # Rebalance hysteresis: deadband (fraction of range width beyond an edge
    # before recentering) + cooldown (minutes a fresh position must live first).
    rebalance_buffer_pct: Decimal = field(default_factory=lambda: Decimal("0.5"))
    rebalance_cooldown_minutes: int = 30
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
        if isinstance(self.min_position_usd, str):
            self.min_position_usd = Decimal(self.min_position_usd)
        if isinstance(self.rebalance_buffer_pct, str):
            self.rebalance_buffer_pct = Decimal(self.rebalance_buffer_pct)
        if isinstance(self.rebalance_cooldown_minutes, str):
            self.rebalance_cooldown_minutes = int(self.rebalance_cooldown_minutes)

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
            "min_position_usd": str(self.min_position_usd),
            "rebalance_buffer_pct": str(self.rebalance_buffer_pct),
            "rebalance_cooldown_minutes": self.rebalance_cooldown_minutes,
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
from almanak.connectors.traderjoe_v2 import BIN_ID_OFFSET

# Logging utilities for user-friendly output
from almanak.framework.utils.log_formatters import format_token_amount_human
from almanak.framework.utils.persistence import safe_int_list

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
    intent_types=["LP_OPEN", "LP_CLOSE", "SWAP", "HOLD"],
    default_chain="avalanche",
    quote_asset="USD",
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
    - min_position_usd: Minimum total inventory (USD) to (re)open a position (default 100)
    - force_action: Force "open" or "close" for testing

    Example Config:
    ---------------
    {
        "pool": "WAVAX/USDC/20",
        "range_width_pct": 0.10,
        "amount_x": "1.0",
        "amount_y": "30",
        "bin_step": 20,
        "min_position_usd": "100",
        "force_action": "open"
    }
    """

    # Default so the attribute exists even when __init__ is bypassed.
    _last_seen_market_ts: datetime | None = None

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

        # Minimum total inventory (USD) required to (re)open a position.
        self.min_position_usd = Decimal(str(self.get_config("min_position_usd", "100")))

        # Rebalance hysteresis — prevents close->reopen->close thrash when price
        # oscillates around the band edge (a recentering LP with no hysteresis
        # bleeds gas + realizes IL on every edge crossing):
        #   (1) DEADBAND: price must exit the LP range by rebalance_buffer_pct of
        #       the range width BEYOND an edge before a rebalance triggers.
        #   (2) COOLDOWN: a freshly-opened position must live at least
        #       rebalance_cooldown_minutes before it can be closed to rebalance.
        self.rebalance_buffer_pct = Decimal(str(self.get_config("rebalance_buffer_pct", "0.5")))
        self.rebalance_cooldown = timedelta(minutes=int(self.get_config("rebalance_cooldown_minutes", 30)))

        # Internal state - track bin IDs where we have liquidity
        self._position_bin_ids: list[int] = []

        # PRICE band the live position was opened with -- used to detect drift
        # and trigger a rebalance (close -> swap-to-ratio -> reopen). TraderJoe
        # is bin-based, but we detect drift on price (entry_price ± width),
        # mirroring traderjoe_crisis_lp.
        self._range_lower: Decimal | None = None
        self._range_upper: Decimal | None = None
        # Hysteresis bookkeeping (committed in on_intent_executed, post-fill).
        self._last_open_time: datetime | None = None
        # Market clock from the latest decide() snapshot.
        self._last_seen_market_ts: datetime | None = None
        self._rebalance_count = 0

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
        _ts = getattr(market, "timestamp", None)
        if isinstance(_ts, datetime):
            self._last_seen_market_ts = _ts if _ts.tzinfo is not None else _ts.replace(tzinfo=UTC)
        else:
            self._last_seen_market_ts = None

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
            return Intent.hold(reason=f"Price data unavailable: {e}")

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
        # STEP 3: Position open -> rebalance if price has drifted out of band
        # =================================================================
        # TraderJoe is bin-based, but we detect drift on price: the stored
        # band is current_price ± half the configured range width, set when
        # the position was opened (mirrors traderjoe_crisis_lp's entry-price
        # drift check). On drift, close via the existing close helper (which
        # preserves the TraderJoe bin_ids/protocol_params).
        if self._position_bin_ids:
            if self._range_lower is not None and self._range_upper is not None:
                # Hysteresis DEADBAND: only rebalance once price exits the LP
                # range by rebalance_buffer_pct of the range width beyond an edge,
                # so a small overshoot at the band edge does not trigger a churn.
                width = self._range_upper - self._range_lower
                buffer = width * self.rebalance_buffer_pct
                exit_lower = self._range_lower - buffer
                exit_upper = self._range_upper + buffer
                if current_price < exit_lower or current_price > exit_upper:
                    # COOLDOWN: don't rebalance a freshly-opened position; let it
                    # earn fees for at least rebalance_cooldown before recentering.
                    if not self._rebalance_cooldown_passed():
                        return Intent.hold(
                            reason=f"Price {current_price:.4f} out of deadband "
                            f"[{exit_lower:.4f}, {exit_upper:.4f}] but rebalance cooldown active "
                            f"({self.rebalance_cooldown})"
                        )
                    logger.info(
                        f"Price {current_price:.4f} exited deadband "
                        f"[{exit_lower:.4f}, {exit_upper:.4f}] (range [{self._range_lower:.4f}, "
                        f"{self._range_upper:.4f}] ± {self.rebalance_buffer_pct} width) — "
                        f"closing to rebalance (#{self._rebalance_count + 1})"
                    )
                    return self._create_close_intent()
                return Intent.hold(
                    reason=f"Position in bins {self._position_bin_ids[:3]}... in band "
                    f"[{self._range_lower:.4f}, {self._range_upper:.4f}] "
                    f"(deadband ±{self.rebalance_buffer_pct} width, {self._rebalance_count} rebalances)"
                )
            # Band unknown (e.g. opened by an older version) -- hold rather than
            # rebalance blindly.
            return Intent.hold(
                reason=f"Position exists in bins {self._position_bin_ids[:3]}... - band unknown"
            )

        # =================================================================
        # STEP 4: No position -> balance inventory to ~50/50, then (re)open
        # =================================================================
        # After a drift-close the wallet holds a skewed inventory (mostly one
        # token), so swap the heavy side back toward 50/50 BEFORE reopening --
        # otherwise the new range opens lopsided.
        try:
            tx = market.balance(self.token_x_symbol, price=token_x_price_usd)
            ty = market.balance(self.token_y_symbol, price=token_y_price_usd)
            token_x_balance = Decimal(str(tx.balance))
            token_y_balance = Decimal(str(ty.balance))
            token_x_usd = Decimal(str(tx.balance_usd))
            token_y_usd = Decimal(str(ty.balance_usd))
        except (ValueError, KeyError):
            return Intent.hold(reason="Cannot check balances")

        total_usd = token_x_usd + token_y_usd
        if total_usd < self.min_position_usd:
            return Intent.hold(
                reason=f"Total ${total_usd:.2f} below min_position_usd ${self.min_position_usd:.2f}"
            )

        swap_intent = self._rebalance_swap_intent(token_x_usd, token_y_usd, total_usd)
        if swap_intent is not None:
            return swap_intent

        # Open new position centered on current price with balanced inventory
        logger.info("No position found - opening new LP position with balanced inventory")
        add_event(
            TimelineEvent(
                timestamp=datetime.now(UTC),
                event_type=TimelineEventType.STATE_CHANGE,
                description="No position found - opening new TraderJoe LP position",
                deployment_id=self.deployment_id,
                details={"action": "opening_new_position", "pool": self.pool},
            )
        )
        # Deploy ~95% of each balanced side (small buffer for gas/rounding).
        return self._create_open_intent(
            current_price,
            amount_x=token_x_balance * Decimal("0.95"),
            amount_y=token_y_balance * Decimal("0.95"),
        )

    # =========================================================================
    # INTENT CREATION HELPERS
    # =========================================================================

    def _create_open_intent(
        self,
        current_price: Decimal,
        amount_x: Decimal | None = None,
        amount_y: Decimal | None = None,
    ) -> Intent:
        """
        Create an LP_OPEN intent to open a new Liquidity Book position.

        Calculates the price range centered on the current price using
        the configured range_width_pct. Amounts default to the configured
        amount_x/amount_y (initial open / force_action); the rebalance path
        passes the balanced wallet amounts to redeploy.

        For TraderJoe V2, this translates to:
        - Lower price bound -> lower bin ID
        - Upper price bound -> upper bin ID
        - Liquidity distributed across bins in range

        Parameters:
            current_price: Current price (token_y per token_x)
            amount_x/amount_y: Optional deploy amounts; fall back to config.

        Returns:
            LPOpenIntent ready for compilation
        """
        amount_x = self.amount_x if amount_x is None else amount_x
        amount_y = self.amount_y if amount_y is None else amount_y

        # Calculate price band
        half_width = self.range_width_pct / Decimal("2")
        range_lower = current_price * (Decimal("1") - half_width)
        range_upper = current_price * (Decimal("1") + half_width)

        # The drift band (_range_lower/_range_upper) is committed ONLY in
        # on_intent_executed, once the LP_OPEN fill is confirmed — never here,
        # pre-fill. decide()'s drift check is gated on _position_bin_ids (also
        # set post-fill), so a failed or never-filled open cannot leave a stale
        # band behind (promotion gate: no pre-fill state mutation in decide()).

        logger.info(
            f"💧 LP_OPEN: {format_token_amount_human(amount_x, self.token_x_symbol)} + "
            f"{format_token_amount_human(amount_y, self.token_y_symbol)}, "
            f"price range [{range_lower:.4f} - {range_upper:.4f}], bin_step={self.bin_step}"
        )

        # Use LP_OPEN intent with traderjoe_v2 protocol
        # The compiler will handle conversion to bin-based parameters
        return Intent.lp_open(
            pool=self.pool,
            amount0=amount_x,
            amount1=amount_y,
            range_lower=range_lower,
            range_upper=range_upper,
            protocol="traderjoe_v2",
        )

    def _rebalance_swap_intent(
        self, token_x_usd: Decimal, token_y_usd: Decimal, total_usd: Decimal
    ) -> Intent | None:
        """Swap the heavy side toward a ~50/50 USD split before (re)opening.

        Returns a SWAP intent when inventory is skewed beyond a 10% tolerance
        band, else None (balanced enough to open as-is).
        """
        half_usd = total_usd / Decimal("2")
        tolerance_usd = total_usd * Decimal("0.10")
        if token_x_usd - half_usd > tolerance_usd:
            logger.info(
                f"Rebalance swap: {self.token_x_symbol} -> {self.token_y_symbol} "
                f"(${token_x_usd - half_usd:.2f} to reach ~50/50)"
            )
            return Intent.swap(
                from_token=self.token_x_symbol,
                to_token=self.token_y_symbol,
                amount_usd=token_x_usd - half_usd,
                max_slippage=Decimal("0.01"),
                protocol="traderjoe_v2",
            )
        if token_y_usd - half_usd > tolerance_usd:
            logger.info(
                f"Rebalance swap: {self.token_y_symbol} -> {self.token_x_symbol} "
                f"(${token_y_usd - half_usd:.2f} to reach ~50/50)"
            )
            return Intent.swap(
                from_token=self.token_y_symbol,
                to_token=self.token_x_symbol,
                amount_usd=token_y_usd - half_usd,
                max_slippage=Decimal("0.01"),
                protocol="traderjoe_v2",
            )
        return None

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
            protocol_params={"bin_ids": list(self._position_bin_ids)},
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
            # ResultEnricher stores protocol-specific fields in extracted_data.
            # Some adapters also project them onto the result directly, but we
            # cannot rely on that for TraderJoe V2 bin IDs.
            bin_ids = None
            if result is not None:
                bin_ids = getattr(result, "bin_ids", None)
                if not bin_ids:
                    extracted = getattr(result, "extracted_data", None) or {}
                    bin_ids = extracted.get("bin_ids")

            if bin_ids:
                self._position_bin_ids = list(bin_ids)
                logger.info(f"TraderJoe LP position opened successfully: bin_ids={bin_ids[:3]}...")
            else:
                logger.info("TraderJoe LP position opened successfully")

            # Record the range we opened with so decide() can detect drift.
            rl = getattr(intent, "range_lower", None)
            ru = getattr(intent, "range_upper", None)
            self._range_lower = Decimal(str(rl)) if rl is not None else None
            self._range_upper = Decimal(str(ru)) if ru is not None else None
            # Start the rebalance cooldown clock from this confirmed open.
            self._last_open_time = self._last_seen_market_ts or datetime.now(UTC)

            add_event(
                TimelineEvent(
                    timestamp=datetime.now(UTC),
                    event_type=TimelineEventType.LP_OPEN,
                    description=f"TraderJoe LP position opened on {self.pool}",
                    deployment_id=self.deployment_id,
                    details={"pool": self.pool, "bin_step": self.bin_step, "bin_ids": bin_ids},
                )
            )

        elif success and intent.intent_type.value == "LP_CLOSE":
            # A drift-triggered close is the first half of a rebalance (the
            # reopen follows on later ticks). Teardown closes also land here but
            # leave _position_bin_ids cleared, so the count harmlessly reflects
            # "closes for recentering" over the run.
            self._rebalance_count += 1
            logger.info("TraderJoe LP position closed successfully (rebalance #%d)", self._rebalance_count)
            self._position_bin_ids = []
            self._range_lower = None
            self._range_upper = None
            self._last_open_time = None

    def _rebalance_cooldown_passed(self) -> bool:
        """True if the live position has existed at least rebalance_cooldown.

        Returns True when there is no recorded open time (e.g. a position
        restored from persisted state without one) so a genuinely out-of-range
        position is never stranded indefinitely.
        """
        if self._last_open_time is None:
            return True
        now = self._last_seen_market_ts or datetime.now(UTC)
        return now - self._last_open_time >= self.rebalance_cooldown

    def get_persistent_state(self) -> dict[str, Any]:
        """Persist bin IDs so teardown can recover after process restarts."""
        parent_get_state = getattr(super(), "get_persistent_state", None)
        state = parent_get_state() if callable(parent_get_state) else {}
        if self._position_bin_ids:
            state["position_bin_ids"] = list(self._position_bin_ids)
        range_lower = getattr(self, "_range_lower", None)
        range_upper = getattr(self, "_range_upper", None)
        if range_lower is not None:
            state["range_lower"] = str(range_lower)
        if range_upper is not None:
            state["range_upper"] = str(range_upper)
        # Defensive getattr: the demo-teardown regression test constructs the
        # strategy via __new__ (bypassing __init__), so these may be unset.
        state["rebalance_count"] = getattr(self, "_rebalance_count", 0)
        last_open = getattr(self, "_last_open_time", None)
        if last_open is not None:
            state["last_open_time"] = last_open.isoformat()
        return state

    def load_persistent_state(self, state: dict[str, Any]) -> None:
        """Restore bin IDs saved by get_persistent_state()."""
        parent_load_state = getattr(super(), "load_persistent_state", None)
        if callable(parent_load_state):
            parent_load_state(state)

        # safe_int_list: drop malformed entries with a warning rather
        # than aborting load_persistent_state on bad data (VIB-3757).
        self._position_bin_ids = safe_int_list(
            state.get("position_bin_ids") if state else None,
            name="position_bin_ids",
        )
        if self._position_bin_ids:
            logger.info("Restored TraderJoe LP bin_ids from state: %s...", self._position_bin_ids[:3])

        if state and state.get("range_lower") is not None:
            self._range_lower = Decimal(str(state["range_lower"]))
        if state and state.get("range_upper") is not None:
            self._range_upper = Decimal(str(state["range_upper"]))
        if state and state.get("rebalance_count") is not None:
            try:
                self._rebalance_count = int(state["rebalance_count"])
            except (TypeError, ValueError):
                logger.warning("Invalid rebalance_count in persisted state: %r", state.get("rebalance_count"))
        if state and state.get("last_open_time"):
            try:
                # fromisoformat() can yield a naive datetime; force aware UTC so
                # the later market-clock cooldown subtraction never raises on
                # naive/aware mismatch.
                last_open_time = datetime.fromisoformat(state["last_open_time"])
                if last_open_time.tzinfo is None:
                    last_open_time = last_open_time.replace(tzinfo=UTC)
                self._last_open_time = last_open_time.astimezone(UTC)
            except (TypeError, ValueError):
                logger.warning("Invalid last_open_time in persisted state: %r", state.get("last_open_time"))

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
                "rebalance_count": getattr(self, "_rebalance_count", 0),
                "range_lower": str(self._range_lower) if getattr(self, "_range_lower", None) is not None else None,
                "range_upper": str(self._range_upper) if getattr(self, "_range_upper", None) is not None else None,
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
            # Calculate estimated value using live prices
            try:
                snapshot = self.create_market_snapshot()
                token_x_price_usd = Decimal(str(snapshot.price(self.token_x_symbol)))
                token_y_price_usd = Decimal(str(snapshot.price(self.token_y_symbol)))
            except Exception:
                logger.warning(
                    f"Unable to fetch live prices for {self.token_x_symbol}/{self.token_y_symbol} in teardown valuation"
                )
                token_x_price_usd = Decimal("0")
                token_y_price_usd = Decimal("0")

            estimated_value = self.amount_x * token_x_price_usd + self.amount_y * token_y_price_usd

            # VIB-4877: the teardown post-condition verifier resolves the closed
            # LP by its 42-char LBPair contract address (details["pool_address"]).
            # The pool *symbol* triple ("WAVAX/USDC/20") is human-readable only —
            # if it leaks into the address slot the verifier rejects a
            # successfully-closed position and flips teardown to FAILED. Resolve
            # the real LBPair address here so pool_address is always a 42-char
            # hex string; keep the symbol under "pool" for readability.
            details: dict[str, Any] = {
                "asset": f"{self.token_x_symbol}/{self.token_y_symbol}",
                "num_bins": len(self._position_bin_ids),
                "pool": self.pool,
                "bin_step": self.bin_step,
                "bin_ids": self._position_bin_ids,
                "amount_x": str(self.amount_x),
                "amount_y": str(self.amount_y),
            }
            pool_address = self._resolve_lb_pair_address()
            if pool_address:
                details["pool_address"] = pool_address

            positions.append(
                PositionInfo(
                    position_type=PositionType.LP,
                    position_id=f"traderjoe-lp-{self.pool}-{self.chain}",
                    chain=self.chain,
                    protocol="traderjoe_v2",
                    value_usd=estimated_value,
                    details=details,
                )
            )

        total_value = sum(p.value_usd for p in positions)

        return TeardownPositionSummary(
            deployment_id=self.deployment_id,
            timestamp=datetime.now(UTC),
            total_value_usd=total_value,
            positions=positions,
        )

    def _resolve_lb_pair_address(self) -> str | None:
        """Resolve the 42-char LBPair contract address for this pool (VIB-4877).

        The teardown post-condition verifier needs the LBPair contract
        address (not the symbol triple) to confirm the position is closed.
        We resolve it through the TraderJoe V2 connector adapter, which routes
        its on-chain reads through the gateway — strategies never open their
        own RPC. Returns ``None`` (logged) on any failure so teardown preview
        still proceeds; the verifier then surfaces a precise missing-address
        error rather than crashing.
        """
        try:
            from almanak.connectors.traderjoe_v2 import (
                TraderJoeV2Adapter,
                TraderJoeV2Config,
            )

            adapter = TraderJoeV2Adapter(
                TraderJoeV2Config(
                    chain=self.chain,
                    wallet_address=self.wallet_address,
                    gateway_client=getattr(self, "_gateway_client", None),
                )
            )
            token_x_addr = adapter.resolve_token_address(self.token_x_symbol)
            token_y_addr = adapter.resolve_token_address(self.token_y_symbol)
            return adapter.sdk.get_pool_address(token_x_addr, token_y_addr, self.bin_step)
        except Exception as exc:  # noqa: BLE001 — fail-soft: preview must not crash
            logger.warning(
                "Could not resolve TraderJoe V2 LBPair address for %s (bin_step=%s): %s. "
                "Teardown verification will report a missing pool_address.",
                self.pool,
                self.bin_step,
                exc,
            )
            return None

    def generate_teardown_intents(self, mode: "TeardownMode", market=None) -> list[AnyIntent]:
        """Generate intents to close all LP positions."""

        intents: list[AnyIntent] = []

        if self._position_bin_ids:
            logger.info(f"Generating teardown intent for TraderJoe LP position (mode={mode.value})")

            intents.append(
                Intent.lp_close(
                    position_id=self.pool,
                    pool=self.pool,
                    collect_fees=True,
                    protocol="traderjoe_v2",
                    protocol_params={"bin_ids": list(self._position_bin_ids)},
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
    print(f"Supported Chains: {TraderJoeLPStrategy.STRATEGY_METADATA.supported_chains}")
    print(f"Supported Protocols: {TraderJoeLPStrategy.STRATEGY_METADATA.supported_protocols}")
    print(f"Intent Types: {TraderJoeLPStrategy.STRATEGY_METADATA.intent_types}")
    print(f"\nDescription: {TraderJoeLPStrategy.STRATEGY_METADATA.description}")
    print("\nTo test on Anvil:")
    print("  python almanak/demo_strategies/traderjoe_lp/run_anvil.py")
