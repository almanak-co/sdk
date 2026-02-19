"""Morpho Leveraged wstETH Yield Strategy with Dynamic Deleverage.

Builds a leveraged wstETH position on Morpho Blue's wstETH/WETH market
(94.5% LLTV) through recursive borrowing. After reaching target leverage,
continuously monitors health factor and dynamically adjusts leverage:
- Auto-deleverages when HF drops below threshold (price drop)
- Re-leverages when HF rises above threshold (price recovery)

The yield comes from the spread between wstETH staking APR (~3-4%) and
WETH borrow rate (~1-2%), amplified by leverage. With 3x leverage on a
2% spread, the net yield is approximately 6%.

Multi-phase lifecycle:
    SETUP: Supply -> Borrow -> Swap -> Resupply (repeat N times)
    MONITOR: Track HF, hold position, earn yield spread
    DELEVERAGE: Swap wstETH -> WETH -> Repay (when HF drops)
    RELEVERAGE: Borrow -> Swap -> Supply (when HF recovers)

Example::

    # Full lifecycle on Anvil (continuous monitoring)
    almanak strat run -d strategies/incubating/morpho_leverage_lst \\
        --fresh --interval 15 --network anvil

    # Single step for debugging
    almanak strat run -d strategies/incubating/morpho_leverage_lst \\
        --fresh --once --network anvil

Known Limitations (Anvil testing):
    - Swap compilation fails due to price oracle case mismatch: "wstETH" resolves
      to on-chain symbol "WSTETH" which the price oracle doesn't recognize. This
      blocks the WETH->wstETH swap step. SDK fix needed: case-insensitive price lookup.
    - The wstETH/WETH Morpho Blue market may have limited liquidity on the forked
      block, preventing multiple borrow loops from executing.
    - Supply and borrow steps are fully verified on-chain.
"""

import logging
from datetime import UTC, datetime
from decimal import ROUND_DOWN, Decimal
from typing import Any

from almanak.framework.api.timeline import TimelineEvent, TimelineEventType, add_event
from almanak.framework.intents import Intent
from almanak.framework.strategies import (
    IntentStrategy,
    MarketSnapshot,
    almanak_strategy,
)
from almanak.framework.utils.log_formatters import format_token_amount_human

logger = logging.getLogger(__name__)


# Phase constants
PHASE_SETUP = "setup"
PHASE_MONITOR = "monitor"
PHASE_DELEVERAGE = "deleverage"
PHASE_RELEVERAGE = "releverage"
PHASE_COMPLETE = "complete"

# Sub-states within phases
STATE_IDLE = "idle"
STATE_SUPPLY = "supplying"
STATE_SUPPLIED = "supplied"
STATE_BORROW = "borrowing"
STATE_BORROWED = "borrowed"
STATE_SWAP = "swapping"
STATE_SWAPPED = "swapped"
STATE_WITHDRAW = "withdrawing"
STATE_WITHDRAWN = "withdrawn"
STATE_REPAY = "repaying"
STATE_REPAID = "repaid"


@almanak_strategy(
    name="demo_morpho_leverage_lst",
    description="Leveraged wstETH yield via Morpho Blue with dynamic health factor management",
    version="1.0.0",
    author="Almanak",
    tags=["demo", "lending", "leverage", "morpho", "lst", "yield", "advanced"],
    supported_chains=["ethereum"],
    supported_protocols=["morpho_blue", "uniswap_v3"],
    intent_types=["SUPPLY", "BORROW", "SWAP", "REPAY", "WITHDRAW", "HOLD"],
)
class MorphoLeverageLSTStrategy(IntentStrategy):
    """Leveraged wstETH yield with dynamic health factor management.

    This strategy demonstrates the full complexity frontier of the SDK:
    - Multi-phase lifecycle (setup, monitor, deleverage, releverage)
    - Dynamic bidirectional leverage management
    - Health factor monitoring with tiered response
    - State persistence and crash recovery
    - Full teardown with multi-step unwind
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        def get_config(key: str, default: Any) -> Any:
            if isinstance(self.config, dict):
                return self.config.get(key, default)
            if hasattr(self.config, "get"):
                return self.config.get(key, default)
            return getattr(self.config, key, default)

        # Market config: wstETH/WETH on Ethereum (94.5% LLTV)
        self.market_id = get_config(
            "market_id",
            "0xc54d7acf14de29e0e5527cabd7a576506870346a78a11a6762e2cca66322ec41",
        )
        self.collateral_token = get_config("collateral_token", "wstETH")
        self.borrow_token = get_config("borrow_token", "WETH")

        # Amount & leverage params
        self.initial_collateral = Decimal(str(get_config("initial_collateral", "1.0")))
        self.target_loops = int(get_config("target_loops", 3))
        self.target_ltv = Decimal(str(get_config("target_ltv", "0.85")))

        # Health factor thresholds
        self.deleverage_hf_threshold = Decimal(str(get_config("deleverage_hf_threshold", "1.3")))
        self.safe_hf_target = Decimal(str(get_config("safe_hf_target", "1.8")))
        self.releverage_hf_threshold = Decimal(str(get_config("releverage_hf_threshold", "2.5")))

        # Deleverage params
        self.deleverage_step_pct = Decimal(str(get_config("deleverage_step_pct", "0.25")))

        # Swap params
        self.swap_slippage = Decimal(str(get_config("swap_slippage", "0.003")))

        # Force specific phase (testing)
        self.force_phase = str(get_config("force_phase", "")).lower()

        # =====================================================================
        # State tracking
        # =====================================================================
        self._phase = PHASE_SETUP
        self._state = STATE_IDLE
        self._current_loop = 0
        self._loops_completed = 0

        # Position tracking
        self._total_collateral = Decimal("0")
        self._total_borrowed = Decimal("0")
        self._pending_amount = Decimal("0")  # Amount pending from last action

        # Health factor
        self._health_factor = Decimal("0")
        self._market_lltv = Decimal(str(get_config("market_lltv", "0.945")))

        # Deleverage tracking
        self._deleverage_collateral_amount = Decimal("0")  # wstETH to withdraw+swap
        self._deleverage_repay_amount = Decimal("0")  # WETH to repay

        # Last known prices (for teardown valuation)
        self._last_col_price = Decimal("0")
        self._last_bor_price = Decimal("0")

        # Retry tracking
        self._swap_retries = 0
        self._max_swap_retries = 2

        # Validate config: prevent instant liquidation and nonsensical thresholds
        if self.target_ltv >= self._market_lltv:
            raise ValueError(
                f"target_ltv ({self.target_ltv}) must be less than market_lltv ({self._market_lltv}). "
                f"Using target_ltv >= LLTV would result in immediate liquidation."
            )
        if self.deleverage_hf_threshold >= self.releverage_hf_threshold:
            raise ValueError(
                f"deleverage_hf_threshold ({self.deleverage_hf_threshold}) must be less than "
                f"releverage_hf_threshold ({self.releverage_hf_threshold}). "
                f"Otherwise the strategy would deleverage and releverage simultaneously."
            )
        if self.safe_hf_target <= self.deleverage_hf_threshold:
            raise ValueError(
                f"safe_hf_target ({self.safe_hf_target}) must be greater than "
                f"deleverage_hf_threshold ({self.deleverage_hf_threshold}). "
                f"The safe target must be above the deleverage trigger."
            )

        logger.info(
            f"MorphoLeverageLSTStrategy initialized: "
            f"market={self.market_id[:10]}..., "
            f"collateral={self.initial_collateral} {self.collateral_token}, "
            f"loops={self.target_loops}, ltv={self.target_ltv*100}%, "
            f"HF thresholds: deleverage<{self.deleverage_hf_threshold}, "
            f"safe={self.safe_hf_target}, releverage>{self.releverage_hf_threshold}"
        )

    # =========================================================================
    # MAIN DECISION LOGIC
    # =========================================================================

    def decide(self, market: MarketSnapshot) -> Intent | None:
        """Route to the appropriate phase handler.

        Phases:
        - SETUP: Build leverage through recursive borrow-swap-supply
        - MONITOR: Watch HF, trigger deleverage/releverage as needed
        - DELEVERAGE: Reduce leverage (swap collateral -> repay debt)
        - RELEVERAGE: Increase leverage (borrow more -> swap -> supply)
        """
        try:
            # Get prices for HF calculation
            col_price, bor_price = self._get_prices(market)

            # Calculate current health factor
            self._update_health_factor(col_price, bor_price)

            logger.info(
                f"[{self._phase.upper()}/{self._state}] "
                f"HF={self._health_factor:.3f}, "
                f"Collateral={format_token_amount_human(self._total_collateral, self.collateral_token)}, "
                f"Borrowed={format_token_amount_human(self._total_borrowed, self.borrow_token)}, "
                f"Loop={self._current_loop}/{self.target_loops}"
            )

            # Phase dispatch
            if self._phase == PHASE_SETUP:
                return self._handle_setup(market, col_price, bor_price)
            elif self._phase == PHASE_MONITOR:
                return self._handle_monitor(market, col_price, bor_price)
            elif self._phase == PHASE_DELEVERAGE:
                return self._handle_deleverage(market, col_price, bor_price)
            elif self._phase == PHASE_RELEVERAGE:
                return self._handle_releverage(market, col_price, bor_price)
            else:
                return Intent.hold(reason=f"Complete. HF={self._health_factor:.3f}")

        except Exception as e:
            logger.exception(f"Error in decide(): {e}")
            return Intent.hold(reason=f"Error: {str(e)}")

    # =========================================================================
    # SETUP PHASE: Build leverage through recursive borrowing
    # =========================================================================

    def _handle_setup(self, market: MarketSnapshot, col_price: Decimal, bor_price: Decimal) -> Intent:
        """Build leverage position through supply -> borrow -> swap -> resupply loop."""

        if self._state == STATE_IDLE:
            # First supply of initial collateral
            try:
                balance = market.balance(self.collateral_token)
                available = balance.balance if hasattr(balance, "balance") else balance
                if available < self.initial_collateral:
                    return Intent.hold(
                        reason=f"Insufficient {self.collateral_token}: {available} < {self.initial_collateral}"
                    )
            except (ValueError, KeyError):
                pass

            self._transition(STATE_SUPPLY)
            return self._supply_intent(self.initial_collateral)

        elif self._state == STATE_SUPPLIED:
            # Borrow against collateral
            borrow_amount = self._calculate_borrow_amount(col_price, bor_price)
            if borrow_amount <= 0:
                logger.warning("No borrowing capacity, completing setup")
                self._enter_monitor()
                return Intent.hold(reason="Setup complete (no borrow capacity)")

            self._transition(STATE_BORROW)
            return self._borrow_intent(borrow_amount)

        elif self._state == STATE_BORROWED:
            # Swap borrowed WETH -> wstETH
            if self._pending_amount <= 0:
                self._transition(STATE_SWAPPED)
                return Intent.hold(reason="No borrowed amount to swap")

            self._transition(STATE_SWAP)
            return self._swap_intent(self.borrow_token, self.collateral_token, self._pending_amount)

        elif self._state == STATE_SWAPPED:
            # Check if more loops needed
            self._loops_completed += 1
            self._current_loop += 1

            if self._current_loop < self.target_loops:
                # Re-supply the swapped wstETH for next loop
                supply_amount = self._pending_amount if self._pending_amount > 0 else self._estimate_swap_output()
                self._transition(STATE_SUPPLY)
                return self._supply_intent(supply_amount)
            else:
                # Setup complete, enter monitoring
                logger.info(
                    f"Setup complete: {self._loops_completed} loops, "
                    f"HF={self._health_factor:.3f}, "
                    f"effective leverage ~{self._calculate_leverage():.1f}x"
                )
                self._enter_monitor()
                return Intent.hold(
                    reason=f"Setup complete. {self._loops_completed} loops. HF={self._health_factor:.3f}"
                )

        return Intent.hold(reason=f"Setup: waiting ({self._state})")

    # =========================================================================
    # MONITOR PHASE: Watch health factor, trigger leverage adjustments
    # =========================================================================

    def _handle_monitor(self, market: MarketSnapshot, col_price: Decimal, bor_price: Decimal) -> Intent:
        """Monitor position health and trigger leverage adjustments."""

        if self._total_borrowed <= 0:
            return Intent.hold(reason="No active position to monitor")

        # Check health factor tiers
        if self._health_factor < Decimal("1.1"):
            # EMERGENCY: Must deleverage to repay -- can't repay directly because
            # wallet has no WETH. All WETH was swapped to wstETH during setup.
            # Route through deleverage: withdraw collateral -> swap -> repay.
            logger.warning(f"EMERGENCY: HF={self._health_factor:.3f} < 1.1. Emergency deleverage!")
            self._phase = PHASE_DELEVERAGE
            self._state = STATE_IDLE
            return self._handle_deleverage(market, col_price, bor_price)

        elif self._health_factor < self.deleverage_hf_threshold:
            # DELEVERAGE: Reduce leverage
            logger.warning(
                f"HF={self._health_factor:.3f} < {self.deleverage_hf_threshold}. "
                f"Entering deleverage phase."
            )
            self._phase = PHASE_DELEVERAGE
            self._state = STATE_IDLE
            return self._handle_deleverage(market, col_price, bor_price)

        elif self._health_factor > self.releverage_hf_threshold:
            # Check if we're below target leverage
            current_leverage = self._calculate_leverage()
            target_leverage = self._estimate_target_leverage()

            if current_leverage < target_leverage * Decimal("0.9"):
                logger.info(
                    f"HF={self._health_factor:.3f} > {self.releverage_hf_threshold}. "
                    f"Leverage {current_leverage:.1f}x < target {target_leverage:.1f}x. "
                    f"Re-leveraging."
                )
                self._phase = PHASE_RELEVERAGE
                self._state = STATE_IDLE
                return self._handle_releverage(market, col_price, bor_price)

        # Normal monitoring: position is healthy
        leverage = self._calculate_leverage()
        estimated_net_apy = self._estimate_net_apy(leverage)

        return Intent.hold(
            reason=f"Monitoring: HF={self._health_factor:.3f}, "
            f"leverage={leverage:.1f}x, "
            f"est. net APY={estimated_net_apy:.1f}%"
        )

    # =========================================================================
    # DELEVERAGE PHASE: Reduce leverage when HF drops
    # =========================================================================

    def _handle_deleverage(self, market: MarketSnapshot, col_price: Decimal, bor_price: Decimal) -> Intent:
        """Reduce leverage: withdraw collateral -> swap to WETH -> repay debt.

        The multi-step flow is required because all wstETH is locked as Morpho
        collateral. We must withdraw it first before we can swap and repay.

        Flow: IDLE -> WITHDRAW -> WITHDRAWN -> SWAP -> SWAPPED -> REPAY -> REPAID
        """

        if self._state == STATE_IDLE:
            # Calculate how much collateral to withdraw and how much WETH to repay
            col_amount, repay_amount = self._calculate_deleverage_amounts(col_price, bor_price)
            if col_amount <= 0 or repay_amount <= 0:
                logger.info("Deleverage not needed, returning to monitor")
                self._enter_monitor()
                return Intent.hold(reason="HF recovered, back to monitoring")

            self._deleverage_collateral_amount = col_amount
            self._deleverage_repay_amount = repay_amount
            self._transition(STATE_WITHDRAW)

            logger.info(
                f"Deleveraging: withdraw {format_token_amount_human(col_amount, self.collateral_token)} "
                f"-> swap -> repay {format_token_amount_human(repay_amount, self.borrow_token)}"
            )
            return self._withdraw_intent(col_amount)

        elif self._state == STATE_WITHDRAWN:
            # Swap withdrawn wstETH -> WETH
            self._transition(STATE_SWAP)
            return self._swap_intent(
                self.collateral_token, self.borrow_token, self._deleverage_collateral_amount
            )

        elif self._state == STATE_SWAPPED:
            # Repay with the WETH we got from the swap
            self._transition(STATE_REPAY)
            return self._repay_intent(repay_full=False, amount=self._deleverage_repay_amount)

        elif self._state == STATE_REPAID:
            # Check if HF is now safe
            self._update_health_factor(col_price, bor_price)

            if self._health_factor >= self.safe_hf_target:
                logger.info(f"Deleverage complete: HF={self._health_factor:.3f} >= {self.safe_hf_target}")
                self._enter_monitor()
                return Intent.hold(reason=f"Deleverage complete. HF={self._health_factor:.3f}")
            else:
                # Need more deleveraging
                logger.info(f"HF={self._health_factor:.3f} still below safe target {self.safe_hf_target}. Continuing deleverage.")
                self._state = STATE_IDLE
                return self._handle_deleverage(market, col_price, bor_price)

        return Intent.hold(reason=f"Deleverage: waiting ({self._state})")

    # =========================================================================
    # RELEVERAGE PHASE: Increase leverage when HF is high
    # =========================================================================

    def _handle_releverage(self, market: MarketSnapshot, col_price: Decimal, bor_price: Decimal) -> Intent:
        """Increase leverage by borrowing more and swapping to collateral."""

        if self._state == STATE_IDLE:
            borrow_amount = self._calculate_borrow_amount(col_price, bor_price)
            if borrow_amount <= 0:
                logger.info("No additional borrow capacity for re-leverage")
                self._enter_monitor()
                return Intent.hold(reason="Re-leverage: no capacity")

            self._transition(STATE_BORROW)
            return self._borrow_intent(borrow_amount)

        elif self._state == STATE_BORROWED:
            if self._pending_amount <= 0:
                self._enter_monitor()
                return Intent.hold(reason="Re-leverage: nothing to swap")

            self._transition(STATE_SWAP)
            return self._swap_intent(self.borrow_token, self.collateral_token, self._pending_amount)

        elif self._state == STATE_SWAPPED:
            # Supply the new collateral
            supply_amount = self._pending_amount if self._pending_amount > 0 else self._estimate_swap_output()
            self._transition(STATE_SUPPLY)
            return self._supply_intent(supply_amount)

        elif self._state == STATE_SUPPLIED:
            # Re-leverage complete
            leverage = self._calculate_leverage()
            logger.info(f"Re-leverage complete: HF={self._health_factor:.3f}, leverage={leverage:.1f}x")
            self._enter_monitor()
            return Intent.hold(reason=f"Re-leverage done. leverage={leverage:.1f}x")

        return Intent.hold(reason=f"Re-leverage: waiting ({self._state})")

    # =========================================================================
    # INTENT BUILDERS
    # =========================================================================

    def _supply_intent(self, amount: Decimal) -> Intent:
        logger.info(f"SUPPLY: {format_token_amount_human(amount, self.collateral_token)} to Morpho Blue")
        return Intent.supply(
            protocol="morpho_blue",
            token=self.collateral_token,
            amount=amount,
            use_as_collateral=True,
            market_id=self.market_id,
            chain=self.chain,
        )

    def _borrow_intent(self, amount: Decimal) -> Intent:
        logger.info(f"BORROW: {format_token_amount_human(amount, self.borrow_token)} from Morpho Blue")
        self._pending_amount = amount
        return Intent.borrow(
            protocol="morpho_blue",
            collateral_token=self.collateral_token,
            collateral_amount=Decimal("0"),
            borrow_token=self.borrow_token,
            borrow_amount=amount,
            market_id=self.market_id,
            chain=self.chain,
        )

    def _swap_intent(self, from_token: str, to_token: str, amount: Decimal) -> Intent:
        logger.info(f"SWAP: {format_token_amount_human(amount, from_token)} -> {to_token}")
        return Intent.swap(
            from_token=from_token,
            to_token=to_token,
            amount=amount,
            max_slippage=self.swap_slippage,
            chain=self.chain,
        )

    def _repay_intent(self, repay_full: bool = False, amount: Decimal | None = None) -> Intent:
        if repay_full:
            logger.info(f"REPAY: full debt via repay_full=True")
            return Intent.repay(
                protocol="morpho_blue",
                token=self.borrow_token,
                amount=Decimal("0"),
                repay_full=True,
                market_id=self.market_id,
                chain=self.chain,
            )
        else:
            repay_amount = amount or self._deleverage_repay_amount
            logger.info(f"REPAY: {format_token_amount_human(repay_amount, self.borrow_token)}")
            return Intent.repay(
                protocol="morpho_blue",
                token=self.borrow_token,
                amount=repay_amount,
                repay_full=False,
                market_id=self.market_id,
                chain=self.chain,
            )

    def _withdraw_intent(self, amount: Decimal) -> Intent:
        logger.info(f"WITHDRAW: {format_token_amount_human(amount, self.collateral_token)} from Morpho Blue")
        return Intent.withdraw(
            protocol="morpho_blue",
            token=self.collateral_token,
            amount=amount,
            withdraw_all=False,
            market_id=self.market_id,
            chain=self.chain,
        )

    # =========================================================================
    # CALCULATIONS
    # =========================================================================

    def _get_prices(self, market: MarketSnapshot) -> tuple[Decimal, Decimal]:
        """Get collateral and borrow token prices.

        Raises ValueError if prices are unavailable -- the caller (decide())
        will catch this and return HOLD. Never falls back to hardcoded prices
        because HF decisions based on stale/fake data can cause liquidation.
        """
        col_price = market.price(self.collateral_token)
        bor_price = market.price(self.borrow_token)
        # Cache for teardown valuation
        self._last_col_price = col_price
        self._last_bor_price = bor_price
        return col_price, bor_price

    def _update_health_factor(self, col_price: Decimal, bor_price: Decimal) -> None:
        """Recalculate health factor from positions and prices."""
        if self._total_borrowed <= 0:
            self._health_factor = Decimal("999")
            return

        collateral_value = self._total_collateral * col_price
        borrow_value = self._total_borrowed * bor_price

        # HF = (Collateral Value * LLTV) / Borrow Value
        self._health_factor = (collateral_value * self._market_lltv) / borrow_value

    def _calculate_borrow_amount(self, col_price: Decimal, bor_price: Decimal) -> Decimal:
        """Calculate safe borrow amount based on target LTV."""
        collateral_value = self._total_collateral * col_price
        max_borrow_value = collateral_value * self.target_ltv
        existing_borrow_value = self._total_borrowed * bor_price
        available = max_borrow_value - existing_borrow_value

        if available <= 0:
            return Decimal("0")

        amount = (available / bor_price).quantize(Decimal("0.0001"), rounding=ROUND_DOWN)
        return amount

    def _calculate_deleverage_amounts(
        self, col_price: Decimal, bor_price: Decimal
    ) -> tuple[Decimal, Decimal]:
        """Calculate collateral to withdraw/swap AND WETH debt to repay.

        After withdrawing collateral, the HF changes because collateral_value
        drops. We account for this in the math:

        Let C = collateral_value, B = borrow_value, L = LLTV, H_target = safe_hf
        After withdrawing W_col and repaying W_bor:
          new_HF = ((C - W_col*col_price) * L) / (B - W_bor*bor_price) = H_target
        Since W_col is swapped to W_bor: W_bor = W_col * col_price / bor_price
        Let x = W_col * col_price (value withdrawn):
          H_target = ((C - x) * L) / (B - x) => x = (C*L - B*H_target) / (L - H_target)

        Returns:
            (collateral_amount_wstETH, repay_amount_WETH). Both zero if no deleverage needed.
        """
        if self._total_borrowed <= 0:
            return Decimal("0"), Decimal("0")

        borrow_value = self._total_borrowed * bor_price
        collateral_value = self._total_collateral * col_price

        # Solve for x = value to withdraw (accounting for collateral reduction)
        denominator = self._market_lltv - self.safe_hf_target
        if denominator >= 0:
            # safe_hf_target >= LLTV means impossible to achieve, deleverage everything
            withdraw_value = collateral_value * self.deleverage_step_pct
        else:
            numerator = collateral_value * self._market_lltv - borrow_value * self.safe_hf_target
            withdraw_value = numerator / denominator

        if withdraw_value <= 0:
            return Decimal("0"), Decimal("0")

        # Cap at deleverage_step_pct of collateral value
        max_step_value = collateral_value * self.deleverage_step_pct
        withdraw_value = min(withdraw_value, max_step_value)

        collateral_to_withdraw = (withdraw_value / col_price).quantize(Decimal("0.0001"), rounding=ROUND_DOWN)
        repay_weth = (withdraw_value / bor_price).quantize(Decimal("0.0001"), rounding=ROUND_DOWN)

        # Ensure we don't withdraw more than we have
        collateral_to_withdraw = min(collateral_to_withdraw, self._total_collateral)
        repay_weth = min(repay_weth, self._total_borrowed)

        return collateral_to_withdraw, repay_weth

    def _calculate_leverage(self) -> Decimal:
        """Calculate effective leverage multiplier."""
        if self._total_collateral <= 0 or self.initial_collateral <= 0:
            return Decimal("1")
        return self._total_collateral / self.initial_collateral

    def _estimate_target_leverage(self) -> Decimal:
        """Estimate target leverage from config."""
        # Geometric series: 1 + ltv + ltv^2 + ... + ltv^(n-1)
        leverage = Decimal("0")
        for i in range(self.target_loops):
            leverage += self.target_ltv ** i
        return leverage

    def _estimate_swap_output(self) -> Decimal:
        """Estimate swap output for wstETH/WETH (highly correlated pair)."""
        # wstETH/WETH ratio is ~1.17, swap wstETH should get ~1.17x WETH
        return self._pending_amount * Decimal("0.85")  # Conservative estimate

    def _estimate_net_apy(self, leverage: Decimal) -> Decimal:
        """Estimate net APY from leverage position."""
        wsteth_staking_apy = Decimal("3.5")  # ~3.5% staking APR
        weth_borrow_apy = Decimal("1.5")  # ~1.5% borrow rate
        spread = wsteth_staking_apy - weth_borrow_apy
        return spread * leverage

    # =========================================================================
    # STATE MANAGEMENT
    # =========================================================================

    def _transition(self, new_state: str) -> None:
        """Transition to a new sub-state."""
        old = self._state
        self._state = new_state
        logger.debug(f"State: {old} -> {new_state} (phase={self._phase})")
        add_event(
            TimelineEvent(
                timestamp=datetime.now(UTC),
                event_type=TimelineEventType.STATE_CHANGE,
                description=f"[{self._phase}] {old} -> {new_state}",
                strategy_id=self.strategy_id,
                details={
                    "phase": self._phase,
                    "old_state": old,
                    "new_state": new_state,
                    "loop": self._current_loop,
                    "health_factor": str(self._health_factor),
                },
            )
        )

    def _enter_monitor(self) -> None:
        """Enter the monitoring phase."""
        self._phase = PHASE_MONITOR
        self._state = STATE_IDLE
        logger.info(f"Entering MONITOR phase. HF={self._health_factor:.3f}")

    # =========================================================================
    # LIFECYCLE HOOKS
    # =========================================================================

    def on_intent_executed(self, intent: Intent, success: bool, result: Any) -> None:
        """Track state after intent execution."""
        intent_type = intent.intent_type.value

        if success:
            if intent_type in ("SUPPLY_COLLATERAL", "SUPPLY"):
                self._state = STATE_SUPPLIED
                if hasattr(intent, "amount") and isinstance(intent.amount, Decimal):
                    self._total_collateral += intent.amount
                logger.info(f"Supply OK. Total collateral: {self._total_collateral} {self.collateral_token}")

            elif intent_type == "BORROW":
                self._state = STATE_BORROWED
                if hasattr(intent, "borrow_amount") and isinstance(intent.borrow_amount, Decimal):
                    self._total_borrowed += intent.borrow_amount
                    self._pending_amount = intent.borrow_amount
                logger.info(f"Borrow OK. Total borrowed: {self._total_borrowed} {self.borrow_token}")

            elif intent_type == "SWAP":
                self._state = STATE_SWAPPED
                self._swap_retries = 0
                # Update pending amount from actual swap output so subsequent
                # supply uses the realized amount, not the stale estimate
                if result and hasattr(result, "swap_amounts") and result.swap_amounts:
                    actual_out = getattr(result.swap_amounts, "amount_out", None)
                    if actual_out and isinstance(actual_out, Decimal) and actual_out > 0:
                        self._pending_amount = actual_out
                        logger.info(f"Swap OK. Received: {actual_out}")
                    else:
                        logger.info("Swap OK. (no amount_out in result)")
                else:
                    logger.info("Swap OK. (no swap_amounts in result)")

            elif intent_type == "WITHDRAW":
                self._state = STATE_WITHDRAWN
                if hasattr(intent, "amount") and isinstance(intent.amount, Decimal):
                    self._total_collateral = max(Decimal("0"), self._total_collateral - intent.amount)
                logger.info(f"Withdraw OK. Remaining collateral: {self._total_collateral} {self.collateral_token}")

            elif intent_type == "REPAY":
                self._state = STATE_REPAID
                if hasattr(intent, "repay_full") and intent.repay_full:
                    self._total_borrowed = Decimal("0")
                elif hasattr(intent, "amount") and isinstance(intent.amount, Decimal):
                    self._total_borrowed = max(Decimal("0"), self._total_borrowed - intent.amount)
                logger.info(f"Repay OK. Remaining debt: {self._total_borrowed} {self.borrow_token}")

            add_event(
                TimelineEvent(
                    timestamp=datetime.now(UTC),
                    event_type=TimelineEventType.POSITION_MODIFIED,
                    description=f"{intent_type} executed in {self._phase} phase",
                    strategy_id=self.strategy_id,
                    details={
                        "intent_type": intent_type,
                        "phase": self._phase,
                        "total_collateral": str(self._total_collateral),
                        "total_borrowed": str(self._total_borrowed),
                        "health_factor": str(self._health_factor),
                    },
                )
            )
        else:
            logger.warning(f"{intent_type} FAILED in {self._phase} phase (state={self._state})")
            # Retry logic for swap failures
            if intent_type == "SWAP" and self._state == STATE_SWAP:
                self._swap_retries += 1
                if self._swap_retries <= self._max_swap_retries:
                    logger.info(f"Swap failed, retry {self._swap_retries}/{self._max_swap_retries}: reverting to previous state")
                    # Revert to the state before swap depending on phase
                    if self._phase == PHASE_DELEVERAGE:
                        self._state = STATE_WITHDRAWN
                    else:
                        self._state = STATE_BORROWED
                else:
                    # Max retries exhausted: enter safe hold, do NOT advance state.
                    # Advancing would treat the failed swap as successful, which
                    # could leave funds in the wrong token or skip repayment.
                    logger.error(
                        f"Swap failed {self._swap_retries} times. "
                        f"Entering safe hold -- manual intervention may be needed."
                    )
                    self._enter_monitor()
                    self._swap_retries = 0

    # =========================================================================
    # STATUS & PERSISTENCE
    # =========================================================================

    def get_status(self) -> dict[str, Any]:
        return {
            "strategy": "demo_morpho_leverage_lst",
            "chain": self.chain,
            "phase": self._phase,
            "state": self._state,
            "health_factor": str(self._health_factor),
            "total_collateral": str(self._total_collateral),
            "total_borrowed": str(self._total_borrowed),
            "current_loop": self._current_loop,
            "loops_completed": self._loops_completed,
            "leverage": str(self._calculate_leverage()),
        }

    def get_persistent_state(self) -> dict[str, Any]:
        return {
            "phase": self._phase,
            "state": self._state,
            "current_loop": self._current_loop,
            "loops_completed": self._loops_completed,
            "total_collateral": str(self._total_collateral),
            "total_borrowed": str(self._total_borrowed),
            "pending_amount": str(self._pending_amount),
            "health_factor": str(self._health_factor),
            "deleverage_collateral_amount": str(self._deleverage_collateral_amount),
            "deleverage_repay_amount": str(self._deleverage_repay_amount),
            "last_col_price": str(self._last_col_price),
            "last_bor_price": str(self._last_bor_price),
        }

    def load_persistent_state(self, state: dict[str, Any]) -> None:
        if "phase" in state:
            self._phase = state["phase"]
        if "state" in state:
            self._state = state["state"]
        if "current_loop" in state:
            self._current_loop = int(state["current_loop"])
        if "loops_completed" in state:
            self._loops_completed = int(state["loops_completed"])
        if "total_collateral" in state:
            self._total_collateral = Decimal(str(state["total_collateral"]))
        if "total_borrowed" in state:
            self._total_borrowed = Decimal(str(state["total_borrowed"]))
        if "pending_amount" in state:
            self._pending_amount = Decimal(str(state["pending_amount"]))
        if "health_factor" in state:
            self._health_factor = Decimal(str(state["health_factor"]))
        if "deleverage_collateral_amount" in state:
            self._deleverage_collateral_amount = Decimal(str(state["deleverage_collateral_amount"]))
        if "deleverage_repay_amount" in state:
            self._deleverage_repay_amount = Decimal(str(state["deleverage_repay_amount"]))
        if "last_col_price" in state:
            self._last_col_price = Decimal(str(state["last_col_price"]))
        if "last_bor_price" in state:
            self._last_bor_price = Decimal(str(state["last_bor_price"]))

        logger.info(
            f"Restored: phase={self._phase}, state={self._state}, "
            f"loop={self._current_loop}, HF={self._health_factor}"
        )

    # =========================================================================
    # TEARDOWN
    # =========================================================================

    def supports_teardown(self) -> bool:
        return True

    def get_open_positions(self) -> "TeardownPositionSummary":  # noqa: F821
        from almanak.framework.teardown import PositionInfo, PositionType, TeardownPositionSummary

        positions = []
        if self._total_collateral > 0:
            positions.append(
                PositionInfo(
                    position_type=PositionType.SUPPLY,
                    position_id=f"morpho-collateral-{self.market_id[:16]}",
                    chain=self.chain,
                    protocol="morpho_blue",
                    value_usd=self._total_collateral * (self._last_col_price or Decimal("0")),
                    details={
                        "market_id": self.market_id,
                        "asset": self.collateral_token,
                        "amount": str(self._total_collateral),
                    },
                )
            )
        if self._total_borrowed > 0:
            positions.append(
                PositionInfo(
                    position_type=PositionType.BORROW,
                    position_id=f"morpho-borrow-{self.market_id[:16]}",
                    chain=self.chain,
                    protocol="morpho_blue",
                    value_usd=self._total_borrowed * (self._last_bor_price or Decimal("0")),
                    health_factor=self._health_factor,
                    details={
                        "market_id": self.market_id,
                        "asset": self.borrow_token,
                        "amount": str(self._total_borrowed),
                    },
                )
            )
        return TeardownPositionSummary(
            strategy_id=self.STRATEGY_NAME,
            timestamp=datetime.now(UTC),
            positions=positions,
        )

    def generate_teardown_intents(self, mode: "TeardownMode", market=None) -> list[Intent]:  # noqa: F821
        """Unwind the leveraged position.

        Order:
        1. Repay all WETH debt (frees collateral)
        2. Withdraw all wstETH collateral (returned to wallet as wstETH)
        """
        intents = []
        if self._total_borrowed > 0:
            intents.append(
                Intent.repay(
                    protocol="morpho_blue",
                    token=self.borrow_token,
                    amount=self._total_borrowed,
                    repay_full=True,
                    market_id=self.market_id,
                    chain=self.chain,
                )
            )
        if self._total_collateral > 0:
            intents.append(
                Intent.withdraw(
                    protocol="morpho_blue",
                    token=self.collateral_token,
                    amount=self._total_collateral,
                    withdraw_all=True,
                    market_id=self.market_id,
                    chain=self.chain,
                )
            )
        return intents

    def on_teardown_started(self, mode: "TeardownMode") -> None:  # noqa: F821
        from almanak.framework.teardown import TeardownMode
        mode_name = "graceful" if mode == TeardownMode.SOFT else "emergency"
        logger.info(
            f"Teardown ({mode_name}): "
            f"repaying {self._total_borrowed} {self.borrow_token}, "
            f"withdrawing {self._total_collateral} {self.collateral_token}"
        )

    def on_teardown_completed(self, success: bool, recovered_usd: Decimal) -> None:
        if success:
            logger.info(f"Teardown complete. Recovered ${recovered_usd:,.2f}")
            self._phase = PHASE_COMPLETE
            self._state = STATE_IDLE
            self._total_collateral = Decimal("0")
            self._total_borrowed = Decimal("0")
        else:
            logger.error("Teardown failed -- manual intervention may be needed")
