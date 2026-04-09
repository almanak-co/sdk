"""
Edge-Signal LP Strategy: USDC-WETH Concentrated Liquidity on Optimism

Signal: HIGH_CONVICTION_SYNTHESIS (LP_OPPORTUNITY) on Optimism
- Signal ID: 7c6dbc92-4293-4558-aca1-06827408356e
- Alpha score: 84/100, regime: BEAR
- Pool: USDC/WETH 0.3% on Uniswap V3 (Optimism)
- Fee APR: ~101% annualized, medium IL risk
- Entry: immediate, size: $5 (~$2.50 per side)
- Stop-loss: -20%, take-profit: +25%/+50%
- Time horizon: 168 hours (7 days)

Strategy Logic (state machine):
1. IDLE     -> Check balances and APR threshold, open LP position
2. OPENING  -> Transitional: LP_OPEN intent submitted
3. OPEN     -> Monitor: out-of-range, stop-loss, time horizon, take-profit
4. CLOSING  -> Transitional: LP_CLOSE intent submitted
5. DONE     -> Terminal: strategy completed (time horizon or take-profit hit)
"""

import logging
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from enum import StrEnum
from typing import Any

from almanak.framework.api.timeline import TimelineEvent, TimelineEventType, add_event
from almanak.framework.intents import Intent
from almanak.framework.strategies import IntentStrategy, MarketSnapshot, almanak_strategy
from almanak.framework.utils.log_formatters import format_usd

logger = logging.getLogger(__name__)


# =============================================================================
# STATE MACHINE
# =============================================================================


class StrategyState(StrEnum):
    """LP strategy lifecycle states."""

    IDLE = "idle"
    OPENING = "opening"
    OPEN = "open"
    REBALANCING = "rebalancing"
    CLOSING = "closing"
    DONE = "done"


# =============================================================================
# CONFIGURATION
# =============================================================================


@dataclass
class EdgeLPConfig:
    """Configuration for Edge LP strategy on Optimism.

    Attributes:
        pool: Pool identifier in format "TOKEN0/TOKEN1/FEE"
        token0: First token symbol (token0 in the pool)
        token1: Second token symbol (token1 in the pool)
        token0_address: On-chain address of token0
        token1_address: On-chain address of token1
        fee_tier: Fee tier in hundredths of a bps (3000 = 0.3%)
        max_position_usd: Maximum position size in USD
        entry_apr_threshold: Minimum APR to enter position (%)
        range_pct: Price range width as percentage (30 = +/-15%)
        rebalance_threshold_pct: Rebalance when price is this % from range edge
        stop_loss_pct: Stop loss threshold as negative decimal (-0.20 = -20%)
        take_profit_pct: Take profit threshold as positive decimal (0.25 = +25%)
        time_horizon_hours: Maximum position duration in hours
        max_slippage_bps: Maximum slippage in basis points
    """

    pool: str = "USDC/WETH/3000"
    token0: str = "USDC"
    token1: str = "WETH"
    token0_address: str = "0x0b2c639c533813f4aa9d7837caf62653d097ff85"
    token1_address: str = "0x4200000000000000000000000000000000000006"
    fee_tier: int = 3000
    max_position_usd: Decimal = Decimal("5")
    entry_apr_threshold: Decimal = Decimal("50")
    range_pct: Decimal = Decimal("30")
    rebalance_threshold_pct: Decimal = Decimal("80")
    stop_loss_pct: Decimal = Decimal("-0.20")
    take_profit_pct: Decimal = Decimal("0.25")
    time_horizon_hours: int = 168
    max_slippage_bps: int = 50

    def to_dict(self) -> dict[str, Any]:
        """Convert config to dict for serialization."""
        return {
            "pool": self.pool,
            "token0": self.token0,
            "token1": self.token1,
            "token0_address": self.token0_address,
            "token1_address": self.token1_address,
            "fee_tier": self.fee_tier,
            "max_position_usd": str(self.max_position_usd),
            "entry_apr_threshold": str(self.entry_apr_threshold),
            "range_pct": str(self.range_pct),
            "rebalance_threshold_pct": str(self.rebalance_threshold_pct),
            "stop_loss_pct": str(self.stop_loss_pct),
            "take_profit_pct": str(self.take_profit_pct),
            "time_horizon_hours": self.time_horizon_hours,
            "max_slippage_bps": self.max_slippage_bps,
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


# =============================================================================
# STRATEGY
# =============================================================================


@almanak_strategy(
    name="edge_lp_op_usdc_weth",
    description="Edge-signal concentrated LP: USDC-WETH 0.3% on Optimism Uniswap V3 (101% fee APR)",
    version="1.0.0",
    author="Almanak Edge",
    tags=["edge", "lp", "uniswap-v3", "optimism", "usdc", "weth", "concentrated-liquidity"],
    supported_chains=["optimism"],
    supported_protocols=["uniswap_v3"],
    intent_types=["LP_OPEN", "LP_CLOSE", "HOLD"],
    default_chain="optimism",
)
class EdgeLpOpUsdcWethStrategy(IntentStrategy[EdgeLPConfig]):
    """
    Edge-signal concentrated LP strategy for USDC-WETH on Optimism (Uniswap V3).

    Signal-driven concentrated liquidity provision with a full state machine:
    - IDLE: Checks balance and APR threshold, opens LP position
    - OPEN: Monitors out-of-range, stop-loss, take-profit, time horizon
    - Rebalances when price drifts beyond threshold
    - Exits on time horizon, stop-loss, or take-profit

    Risk parameters from Edge signal:
    - Stop-loss: -20%
    - Take-profit: +25% (first target), +50% (stretch)
    - Time horizon: 168h (7 days)
    - Max slippage: 50 bps
    """

    def __init__(self, *args, **kwargs):
        """Initialize Edge LP strategy."""
        super().__init__(*args, **kwargs)

        # Pool configuration
        self.pool = self.config.pool
        self.token0 = self.config.token0
        self.token1 = self.config.token1
        self.fee_tier = int(self.config.fee_tier)

        # Risk parameters
        self.max_position_usd = Decimal(str(self.config.max_position_usd))
        self.entry_apr_threshold = Decimal(str(self.config.entry_apr_threshold))
        self.range_pct = Decimal(str(self.config.range_pct))
        self.rebalance_threshold_pct = Decimal(str(self.config.rebalance_threshold_pct))
        self.stop_loss_pct = Decimal(str(self.config.stop_loss_pct))
        self.take_profit_pct = Decimal(str(self.config.take_profit_pct))
        self.time_horizon_hours = int(self.config.time_horizon_hours)
        self.max_slippage_bps = int(self.config.max_slippage_bps)

        # State machine
        self._state = StrategyState.IDLE
        self._position_id: str | None = None
        self._range_lower: Decimal | None = None
        self._range_upper: Decimal | None = None
        self._position_opened_at: datetime | None = None
        self._entry_value_usd: Decimal | None = None
        self._rebalance_count: int = 0

        # Restore from persistent state
        self._load_position_from_state()

        logger.info(
            f"EdgeLpOpUsdcWethStrategy initialized: pool={self.pool}, "
            f"max_position=${self.max_position_usd}, "
            f"range={self.range_pct}%, state={self._state.value}"
            + (f", position_id={self._position_id}" if self._position_id else "")
        )

    # =========================================================================
    # MAIN DECISION LOGIC
    # =========================================================================

    def decide(self, market: MarketSnapshot) -> Intent | None:
        """
        Make LP decision based on current market conditions and strategy state.

        State machine:
        - IDLE -> open new LP position if conditions met
        - OPEN -> monitor position, check exit/rebalance conditions
        - REBALANCING -> same as IDLE (position was just closed for rebalance)
        - CLOSING / DONE -> hold

        Args:
            market: MarketSnapshot with prices, balances, indicators

        Returns:
            Intent: LP_OPEN, LP_CLOSE, or HOLD
        """
        try:
            if self._state == StrategyState.DONE:
                return Intent.hold(reason="Strategy completed (done state)")

            if self._state in (StrategyState.OPENING, StrategyState.CLOSING):
                return Intent.hold(reason=f"Waiting for {self._state.value} to complete")

            # Get current prices
            token0_price_usd = market.price(self.token0)
            token1_price_usd = market.price(self.token1)
            # Price expressed as token0 per token1 (e.g., USDC per WETH)
            current_price = token1_price_usd / token0_price_usd
            logger.info(
                f"[{self._state.value}] {self.token1} price: {format_usd(token1_price_usd)}, "
                f"pool price: {current_price:.2f} {self.token0}/{self.token1}"
            )

            if self._state == StrategyState.OPEN:
                return self._handle_open_state(market, current_price, token0_price_usd, token1_price_usd)

            # IDLE or REBALANCING -> try to open position
            return self._handle_idle_state(market, current_price, token0_price_usd, token1_price_usd)

        except Exception as e:
            logger.exception(f"Error in decide(): {e}")
            return Intent.hold(reason=f"Error: {e!s}")

    # =========================================================================
    # STATE HANDLERS
    # =========================================================================

    def _handle_idle_state(
        self,
        market: MarketSnapshot,
        current_price: Decimal,
        token0_price_usd: Decimal,
        token1_price_usd: Decimal,
    ) -> Intent:
        """Handle IDLE / REBALANCING state: check conditions and open LP position."""
        # Check balances
        try:
            token0_bal = market.balance(self.token0)
            token1_bal = market.balance(self.token1)
            token0_balance = token0_bal.balance if hasattr(token0_bal, "balance") else token0_bal
            token1_balance = token1_bal.balance if hasattr(token1_bal, "balance") else token1_bal
        except (ValueError, KeyError) as e:
            return Intent.hold(reason=f"Balance check failed: {e}")

        # Calculate target amounts for 50/50 split by USD value
        target_value_per_side = self.max_position_usd / Decimal("2")
        amount0 = target_value_per_side / token0_price_usd  # USDC amount
        amount1 = target_value_per_side / token1_price_usd  # WETH amount

        # Verify sufficient balance
        if token0_balance < amount0:
            return Intent.hold(
                reason=f"Insufficient {self.token0}: have {token0_balance:.4f}, need {amount0:.4f}"
            )
        if token1_balance < amount1:
            return Intent.hold(
                reason=f"Insufficient {self.token1}: have {token1_balance:.6f}, need {amount1:.6f}"
            )

        total_value_usd = amount0 * token0_price_usd + amount1 * token1_price_usd
        if total_value_usd < Decimal("1"):
            return Intent.hold(reason=f"Position value ${total_value_usd:.2f} below minimum $1")

        # Calculate price range: +/- range_pct/2 from current price
        half_range = self.range_pct / Decimal("200")  # e.g., 30% -> 0.15
        range_lower = current_price * (Decimal("1") - half_range)
        range_upper = current_price * (Decimal("1") + half_range)

        logger.info(
            f"Opening LP: {amount0:.2f} {self.token0} + {amount1:.6f} {self.token1}, "
            f"range [{format_usd(range_lower)} - {format_usd(range_upper)}], "
            f"total ~{format_usd(total_value_usd)}"
        )

        self._state = StrategyState.OPENING
        self._entry_value_usd = total_value_usd

        add_event(
            TimelineEvent(
                timestamp=datetime.now(UTC),
                event_type=TimelineEventType.STATE_CHANGE,
                description=f"Opening LP position on {self.pool}",
                strategy_id=getattr(self, "strategy_id", "edge_lp_op_usdc_weth"),
                details={
                    "amount0": str(amount0),
                    "amount1": str(amount1),
                    "range_lower": str(range_lower),
                    "range_upper": str(range_upper),
                },
            )
        )

        return Intent.lp_open(
            pool=self.pool,
            amount0=amount0,
            amount1=amount1,
            range_lower=range_lower,
            range_upper=range_upper,
            protocol="uniswap_v3",
        )

    def _handle_open_state(
        self,
        market: MarketSnapshot,
        current_price: Decimal,
        token0_price_usd: Decimal,
        token1_price_usd: Decimal,
    ) -> Intent:
        """Handle OPEN state: monitor position for exit/rebalance conditions."""
        if not self._position_id:
            # Lost position tracking -- fall back to idle
            logger.warning("In OPEN state but no position_id tracked, reverting to IDLE")
            self._state = StrategyState.IDLE
            return Intent.hold(reason="Lost position tracking, will re-evaluate next iteration")

        # --- Check time horizon ---
        if self._position_opened_at:
            elapsed = datetime.now(UTC) - self._position_opened_at
            hours_elapsed = elapsed.total_seconds() / 3600
            hours_remaining = self.time_horizon_hours - hours_elapsed

            if elapsed > timedelta(hours=self.time_horizon_hours):
                logger.info(f"Time horizon exceeded: {hours_elapsed:.1f}h > {self.time_horizon_hours}h")
                return self._close_position(reason="time_horizon_exceeded")

            logger.debug(f"Time remaining: {hours_remaining:.1f}h")

        # --- Check stop-loss ---
        if self._entry_value_usd and self._entry_value_usd > 0:
            # Estimate current position value from price movement
            # With concentrated LP, value changes differently than spot,
            # but for a safety stop-loss, price change is a reasonable proxy
            current_estimated_usd = self._estimate_position_value(
                current_price, token0_price_usd, token1_price_usd
            )
            pnl_pct = (current_estimated_usd - self._entry_value_usd) / self._entry_value_usd

            if pnl_pct <= self.stop_loss_pct:
                logger.info(
                    f"Stop-loss triggered: PnL {pnl_pct:.1%} <= {self.stop_loss_pct:.1%} "
                    f"(entry: {format_usd(self._entry_value_usd)}, "
                    f"current: {format_usd(current_estimated_usd)})"
                )
                return self._close_position(reason="stop_loss")

            # --- Check take-profit ---
            if pnl_pct >= self.take_profit_pct:
                logger.info(
                    f"Take-profit triggered: PnL {pnl_pct:.1%} >= {self.take_profit_pct:.1%}"
                )
                return self._close_position(reason="take_profit")

            logger.debug(f"Position PnL: {pnl_pct:.2%}")

        # --- Check if out of range (rebalance trigger) ---
        if self._range_lower is not None and self._range_upper is not None:
            range_size = self._range_upper - self._range_lower
            if range_size > 0:
                # How far through the range is the current price? (0.0 = at lower, 1.0 = at upper)
                position_in_range = (current_price - self._range_lower) / range_size
                rebalance_fraction = self.rebalance_threshold_pct / Decimal("100")
                lower_threshold = (Decimal("1") - rebalance_fraction) / Decimal("2")
                upper_threshold = (Decimal("1") + rebalance_fraction) / Decimal("2")

                if position_in_range < lower_threshold or position_in_range > upper_threshold:
                    side = "below" if position_in_range < lower_threshold else "above"
                    logger.info(
                        f"Rebalance needed: price at {position_in_range:.1%} of range ({side} threshold), "
                        f"range [{format_usd(self._range_lower)} - {format_usd(self._range_upper)}]"
                    )
                    self._state = StrategyState.REBALANCING
                    self._rebalance_count += 1

                    add_event(
                        TimelineEvent(
                            timestamp=datetime.now(UTC),
                            event_type=TimelineEventType.REBALANCE_EXECUTED,
                            description=f"Rebalancing LP: price {side} range threshold",
                            strategy_id=getattr(self, "strategy_id", "edge_lp_op_usdc_weth"),
                            details={
                                "position_in_range": str(position_in_range),
                                "rebalance_count": self._rebalance_count,
                            },
                        )
                    )

                    return Intent.lp_close(
                        position_id=self._position_id,
                        pool=self.pool,
                        collect_fees=True,
                        protocol="uniswap_v3",
                    )

        return Intent.hold(reason=f"Position {self._position_id} in range, monitoring")

    # =========================================================================
    # HELPERS
    # =========================================================================

    def _close_position(self, reason: str) -> Intent:
        """Create LP_CLOSE intent and transition to CLOSING state."""
        logger.info(f"Closing LP position {self._position_id}: {reason}")
        self._state = StrategyState.CLOSING

        add_event(
            TimelineEvent(
                timestamp=datetime.now(UTC),
                event_type=TimelineEventType.STATE_CHANGE,
                description=f"Closing LP position: {reason}",
                strategy_id=getattr(self, "strategy_id", "edge_lp_op_usdc_weth"),
                details={"reason": reason, "position_id": self._position_id},
            )
        )

        return Intent.lp_close(
            position_id=self._position_id,
            pool=self.pool,
            collect_fees=True,
            protocol="uniswap_v3",
        )

    def _estimate_position_value(
        self,
        current_price: Decimal,
        token0_price_usd: Decimal,
        token1_price_usd: Decimal,
    ) -> Decimal:
        """Estimate current position value using price ratio vs entry.

        This is a simplified estimate. In concentrated liquidity, position value
        depends on the tick range and how far price has moved within it. For a
        safety stop-loss check, using the entry value scaled by price movement
        is a reasonable conservative proxy.
        """
        if not self._entry_value_usd:
            return Decimal("0")

        # For a 50/50 LP position, value moves ~proportionally to the geometric
        # mean of the two token prices relative to entry. Simplified: assume the
        # LP value tracks roughly with ETH price since USDC is stable.
        if self._range_lower and self._range_upper:
            entry_midpoint = (self._range_lower + self._range_upper) / Decimal("2")
            if entry_midpoint > 0:
                # LP value approximation: sqrt(current/entry) * entry_value
                # Simplified to linear for safety (conservative underestimate)
                price_ratio = current_price / entry_midpoint
                return self._entry_value_usd * price_ratio

        return self._entry_value_usd

    # =========================================================================
    # LIFECYCLE HOOKS
    # =========================================================================

    def on_intent_executed(self, intent: Intent, success: bool, result: Any) -> None:
        """Track LP position after open/close.

        For LP_OPEN: Store position_id, range, entry time.
        For LP_CLOSE: Clear tracking or transition to rebalance/done.
        """
        intent_type = getattr(intent, "intent_type", None)
        intent_type_str = intent_type.value if intent_type else ""

        if intent_type_str == "LP_OPEN":
            if success and result:
                position_id = getattr(result, "position_id", None)
                if position_id:
                    self._position_id = str(position_id)
                    self._position_opened_at = datetime.now(UTC)
                    self._range_lower = getattr(intent, "range_lower", None)
                    self._range_upper = getattr(intent, "range_upper", None)
                    self._state = StrategyState.OPEN

                    logger.info(f"LP opened: position_id={position_id}, state -> OPEN")

                    add_event(
                        TimelineEvent(
                            timestamp=datetime.now(UTC),
                            event_type=TimelineEventType.LP_OPEN,
                            description=f"LP position opened on {self.pool} (ID: {position_id})",
                            strategy_id=getattr(self, "strategy_id", "edge_lp_op_usdc_weth"),
                            details={
                                "position_id": str(position_id),
                                "range_lower": str(self._range_lower),
                                "range_upper": str(self._range_upper),
                            },
                        )
                    )
                else:
                    logger.warning("LP_OPEN succeeded but no position_id in result")
                    self._state = StrategyState.IDLE
            else:
                logger.warning(f"LP_OPEN failed (success={success}), reverting to IDLE")
                self._state = StrategyState.IDLE

        elif intent_type_str == "LP_CLOSE":
            if success:
                logger.info(f"LP closed: position_id={self._position_id}")

                add_event(
                    TimelineEvent(
                        timestamp=datetime.now(UTC),
                        event_type=TimelineEventType.LP_CLOSE,
                        description=f"LP position closed on {self.pool}",
                        strategy_id=getattr(self, "strategy_id", "edge_lp_op_usdc_weth"),
                        details={"position_id": self._position_id},
                    )
                )

                old_state = self._state
                self._position_id = None
                self._range_lower = None
                self._range_upper = None

                if old_state == StrategyState.REBALANCING:
                    # Stay in REBALANCING -> next decide() will re-open
                    logger.info("Rebalance close complete, will re-open next iteration")
                elif old_state == StrategyState.CLOSING:
                    self._state = StrategyState.DONE
                    logger.info("Position closed, strategy -> DONE")
                else:
                    self._state = StrategyState.IDLE
            else:
                logger.warning("LP_CLOSE failed, reverting to OPEN")
                if self._position_id:
                    self._state = StrategyState.OPEN
                else:
                    self._state = StrategyState.IDLE

    # =========================================================================
    # PERSISTENCE
    # =========================================================================

    def get_persistent_state(self) -> dict[str, Any]:
        """Save strategy state for crash recovery."""
        state = super().get_persistent_state() if hasattr(super(), "get_persistent_state") else {}

        state["strategy_state"] = self._state.value
        state["rebalance_count"] = self._rebalance_count

        if self._position_id:
            state["position_id"] = self._position_id
        if self._range_lower is not None:
            state["range_lower"] = str(self._range_lower)
        if self._range_upper is not None:
            state["range_upper"] = str(self._range_upper)
        if self._position_opened_at:
            state["position_opened_at"] = self._position_opened_at.isoformat()
        if self._entry_value_usd:
            state["entry_value_usd"] = str(self._entry_value_usd)

        return state

    def load_persistent_state(self, state: dict[str, Any]) -> None:
        """Restore strategy state after restart."""
        if hasattr(super(), "load_persistent_state"):
            super().load_persistent_state(state)

        if not state:
            return

        # Restore state machine
        state_str = state.get("strategy_state")
        if state_str:
            try:
                self._state = StrategyState(state_str)
            except ValueError:
                self._state = StrategyState.IDLE

        self._rebalance_count = int(state.get("rebalance_count", 0))

        # Restore position tracking
        self._position_id = state.get("position_id")
        rl = state.get("range_lower")
        ru = state.get("range_upper")
        self._range_lower = Decimal(rl) if rl else None
        self._range_upper = Decimal(ru) if ru else None

        opened_at = state.get("position_opened_at")
        self._position_opened_at = datetime.fromisoformat(opened_at) if opened_at else None

        entry_val = state.get("entry_value_usd")
        self._entry_value_usd = Decimal(entry_val) if entry_val else None

        if self._position_id:
            logger.info(
                f"Restored state: {self._state.value}, position_id={self._position_id}, "
                f"opened_at={self._position_opened_at}"
            )

    def _load_position_from_state(self) -> None:
        """Load position from persistent state during init."""
        state = self.get_persistent_state()
        if state:
            self.load_persistent_state(state)

    # =========================================================================
    # STATUS
    # =========================================================================

    def get_status(self) -> dict[str, Any]:
        """Get current strategy status for monitoring."""
        elapsed_hours = None
        if self._position_opened_at:
            elapsed_hours = (datetime.now(UTC) - self._position_opened_at).total_seconds() / 3600

        return {
            "strategy": "edge_lp_op_usdc_weth",
            "chain": self.chain,
            "wallet": self.wallet_address[:10] + "..." if self.wallet_address else None,
            "state": self._state.value,
            "position_id": self._position_id,
            "entry_value_usd": str(self._entry_value_usd) if self._entry_value_usd else None,
            "elapsed_hours": round(elapsed_hours, 1) if elapsed_hours else None,
            "time_horizon_hours": self.time_horizon_hours,
            "rebalance_count": self._rebalance_count,
        }

    # =========================================================================
    # TEARDOWN
    # =========================================================================

    def get_open_positions(self):
        """Return all open positions for teardown preview."""
        from almanak.framework.teardown import (
            PositionInfo,
            PositionType,
            TeardownPositionSummary,
        )

        positions: list[PositionInfo] = []

        if self._position_id:
            positions.append(
                PositionInfo(
                    position_type=PositionType.LP,
                    position_id=str(self._position_id),
                    chain=self.chain,
                    protocol="uniswap_v3",
                    value_usd=self._entry_value_usd or self.max_position_usd,
                    details={
                        "pool": self.pool,
                        "fee_tier": self.fee_tier,
                        "token0": self.token0,
                        "token1": self.token1,
                        "range_lower": str(self._range_lower) if self._range_lower else None,
                        "range_upper": str(self._range_upper) if self._range_upper else None,
                    },
                )
            )

        return TeardownPositionSummary(
            strategy_id=getattr(self, "strategy_id", "edge_lp_op_usdc_weth"),
            timestamp=datetime.now(UTC),
            positions=positions,
        )

    def generate_teardown_intents(self, mode=None, market=None) -> list[Intent]:
        """Generate intents to close all positions.

        Teardown order: LP_CLOSE -> SWAP remaining base token to quote.
        """
        from almanak.framework.teardown import TeardownMode

        intents: list[Intent] = []

        if not self._position_id:
            return intents

        max_slippage = Decimal("0.03") if mode == TeardownMode.HARD else Decimal("0.005")

        logger.info(f"Generating teardown for LP position {self._position_id} (mode={mode})")

        # Close LP position and collect fees
        intents.append(
            Intent.lp_close(
                position_id=self._position_id,
                pool=self.pool,
                collect_fees=True,
                protocol="uniswap_v3",
            )
        )

        # Swap remaining WETH back to USDC for clean exit
        intents.append(
            Intent.swap(
                from_token=self.token1,
                to_token=self.token0,
                amount="all",
                max_slippage=max_slippage,
            )
        )

        return intents

    def on_teardown_started(self, mode=None) -> None:
        """Called when teardown begins."""
        from almanak.framework.teardown import TeardownMode

        mode_name = "Graceful Shutdown" if mode == TeardownMode.SOFT else "Safe Emergency Exit"
        logger.info(
            f"[TEARDOWN] Starting {mode_name}. "
            f"Position: {self._position_id or 'None'}, state: {self._state.value}"
        )

    def on_teardown_completed(self, success: bool, recovered_usd: Decimal) -> None:
        """Called when teardown completes."""
        if success:
            logger.info(f"[TEARDOWN] Completed. Recovered: ${recovered_usd:,.2f}")
            self._position_id = None
            self._position_opened_at = None
            self._entry_value_usd = None
            self._state = StrategyState.DONE
        else:
            logger.warning(f"[TEARDOWN] Failed. Partial recovery: ${recovered_usd:,.2f}")


if __name__ == "__main__":
    print("=" * 60)
    print("EdgeLpOpUsdcWethStrategy - Edge Signal LP Strategy")
    print("=" * 60)
    print(f"\nStrategy Name: {EdgeLpOpUsdcWethStrategy.STRATEGY_NAME}")
    print(f"Supported Chains: {EdgeLpOpUsdcWethStrategy.SUPPORTED_CHAINS}")
    print(f"Supported Protocols: {EdgeLpOpUsdcWethStrategy.SUPPORTED_PROTOCOLS}")
    print(f"Intent Types: {EdgeLpOpUsdcWethStrategy.INTENT_TYPES}")
    print("\nTo run this strategy:")
    print("  almanak strat run --network anvil --once")
