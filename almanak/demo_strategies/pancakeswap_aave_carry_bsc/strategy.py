"""
PancakeSwap V3 + Aave V3 Carry Trade on BSC
=============================================

T2 multi-protocol composition on BSC combining Aave V3 lending with
PancakeSwap V3 swaps. Full lifecycle with teardown:

Entry:
  1. SUPPLY: Supply WBNB collateral to Aave V3 (use_as_collateral=True)
  2. BORROW: Borrow USDC against the supplied collateral at `ltv_target` (30%),
     clamped to a safe fraction of Aave's LIVE available-borrow capacity
  3. SWAP: Swap borrowed USDC -> USDT via PancakeSwap V3

Teardown:
  4. SWAP_BACK: Swap USDT -> USDC via PancakeSwap V3
  5. REPAY: Repay Aave V3 USDC debt (repay_full=True)
  6. WITHDRAW: Withdraw WBNB collateral (withdraw_all=True)

Note: BSC USDC and USDT both have 18 decimals (not 6 like other chains).

Accounting note (VIB-3586): SUPPLY and BORROW are emitted as two distinct
intents — never bundled into a single ``Intent.borrow(collateral_amount>0)``.
The accounting layer writes exactly one ``accounting_events`` row per intent;
bundling the collateral leg into the borrow collapses the supply into the
BORROW event and drops the standalone SUPPLY event (and its ``supply:`` FIFO
lot, which the closing WITHDRAW needs). A fail-closed guard now rejects
``Intent.borrow(collateral_amount > 0)`` at decide()-time, so the collateral
MUST be deposited by a preceding standalone SUPPLY intent.

USAGE:
------
    # Run full lifecycle on Anvil
    almanak strat run -d almanak/demo_strategies/pancakeswap_aave_carry_bsc --network anvil --once

    # Repeat with --interval for multi-step lifecycle
    almanak strat run -d almanak/demo_strategies/pancakeswap_aave_carry_bsc --network anvil --interval 5
"""

import logging
from datetime import UTC, datetime
from decimal import ROUND_DOWN, Decimal
from typing import TYPE_CHECKING, Any

from almanak.framework.intents import Intent
from almanak.framework.market import HealthUnavailableError, MarketSnapshot
from almanak.framework.strategies import IntentStrategy, almanak_strategy
from almanak.framework.utils.log_formatters import format_token_amount_human, format_usd

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from almanak.framework.teardown import TeardownMode, TeardownPositionSummary

# Stable states
IDLE = "idle"
SUPPLIED = "supplied"
BORROWED = "borrowed"
SWAPPED = "swapped"
SWAP_BACK = "swap_back"
REPAID = "repaid"
COMPLETE = "complete"

# Transitional states
SUPPLYING = "supplying"
BORROWING = "borrowing"
SWAPPING = "swapping"
SWAPPING_BACK = "swapping_back"
REPAYING = "repaying"
WITHDRAWING = "withdrawing"

STABLE_STATES = {IDLE, SUPPLIED, BORROWED, SWAPPED, SWAP_BACK, REPAID, COMPLETE}
TRANSITIONAL_STATES = {SUPPLYING, BORROWING, SWAPPING, SWAPPING_BACK, REPAYING, WITHDRAWING}


@almanak_strategy(
    name="pancakeswap_aave_carry_bsc",
    description="PancakeSwap V3 + Aave V3 carry trade on BSC: borrow -> swap -> swap_back -> repay -> withdraw",
    version="1.0.0",
    author="Almanak",
    tags=["demo", "carry-trade", "aave-v3", "pancakeswap-v3", "lending", "swap", "bsc", "multi-protocol"],
    supported_chains=["bsc"],
    supported_protocols=["aave_v3", "pancakeswap_v3"],
    intent_types=["SUPPLY", "BORROW", "SWAP", "REPAY", "WITHDRAW", "HOLD"],
    default_chain="bsc",
    quote_asset="USD",
)
class PancakeswapAaveCarryBscStrategy(IntentStrategy):
    """T2 carry trade: Aave V3 lending + PancakeSwap V3 swap on BSC.

    State machine:
        idle -> supplying -> supplied -> borrowing -> borrowed
            -> swapping -> swapped -> swapping_back -> swap_back
            -> repaying -> repaid -> withdrawing -> complete

    Config parameters:
        collateral_token: Token to supply as collateral (default: WBNB)
        collateral_amount: Amount to supply (default: 0.5)
        borrow_token: Token to borrow (default: USDC)
        swap_to_token: Token to swap borrowed funds into (default: USDT)
        ltv_target: Target loan-to-value ratio (default: 0.3 = 30%)
        max_borrow_fraction: Cap the borrow at this fraction of Aave's live
            available-borrow capacity (default: 0.5) -- an enforced liquidation buffer.
    """

    def supports_teardown(self) -> bool:
        return True

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)

        self.collateral_token = str(self.get_config("collateral_token", "WBNB"))
        self.collateral_amount = Decimal(str(self.get_config("collateral_amount", "0.5")))
        self.borrow_token = str(self.get_config("borrow_token", "USDC"))
        self.swap_to_token = str(self.get_config("swap_to_token", "USDT"))
        self.ltv_target = Decimal(str(self.get_config("ltv_target", "0.3")))
        # Enforced safety ceiling: never borrow more than this fraction of
        # Aave's LIVE available-borrow capacity (see _do_borrow). Keeps the
        # position clear of the liquidation boundary even if the collateral
        # price drifts between the SUPPLY and BORROW steps.
        self.max_borrow_fraction = Decimal(str(self.get_config("max_borrow_fraction", "0.5")))

        self._state = IDLE
        self._previous_stable = IDLE

        self._supplied_amount = Decimal("0")
        self._borrowed_amount = Decimal("0")
        self._swapped_amount = Decimal("0")

        logger.info(
            f"PancakeswapAaveCarryBsc initialized: "
            f"collateral={self.collateral_amount} {self.collateral_token}, "
            f"borrow={self.borrow_token} LTV={self.ltv_target * 100}%, "
            f"swap_to={self.swap_to_token}"
        )

    # =========================================================================
    # MAIN DECISION LOGIC
    # =========================================================================

    def decide(self, market: MarketSnapshot) -> Intent | None:
        """Execute the next lifecycle step based on current state.

        Data-unavailable reads degrade to HOLD inside the phase helpers; any
        other exception propagates (no blanket ``except -> hold`` masking bugs).
        """
        # Handle stuck transitional states by reverting
        if self._state in TRANSITIONAL_STATES:
            revert_to = self._previous_stable
            logger.warning(f"Stuck in '{self._state}' -- reverting to '{revert_to}'")
            self._state = revert_to

        # === ENTRY PHASE ===
        if self._state == IDLE:
            return self._do_supply()

        if self._state == SUPPLIED:
            return self._do_borrow(market)

        if self._state == BORROWED:
            return self._do_swap()

        # === TEARDOWN PHASE ===
        if self._state == SWAPPED:
            return self._do_swap_back()

        if self._state == SWAP_BACK:
            return self._do_repay()

        if self._state == REPAID:
            return self._do_withdraw()

        if self._state == COMPLETE:
            return Intent.hold(
                reason=(
                    "Full lifecycle complete: SUPPLY -> BORROW -> SWAP -> SWAP_BACK -> REPAY -> WITHDRAW. "
                    "All positions closed."
                )
            )

        return Intent.hold(reason=f"Unknown state: {self._state}")

    # =========================================================================
    # PHASE HELPERS
    # =========================================================================

    def _do_supply(self) -> Intent:
        """Phase 1: Supply WBNB collateral into Aave V3 as a standalone intent.

        Emitting SUPPLY as its own intent (rather than bundling the collateral
        leg into the BORROW intent) produces a first-class SUPPLY accounting
        event and the ``supply:`` FIFO lot the closing WITHDRAW needs — see the
        module docstring (VIB-3586). The fail-closed guard now rejects a bundled
        ``Intent.borrow(collateral_amount > 0)``, so this phase is mandatory.
        """
        logger.info(
            f"Phase 1 SUPPLY: supply {format_token_amount_human(self.collateral_amount, self.collateral_token)} "
            f"as collateral into Aave V3"
        )
        self._transition(SUPPLYING)
        return Intent.supply(
            protocol="aave_v3",
            token=self.collateral_token,
            amount=self.collateral_amount,
            use_as_collateral=True,
            chain=self.chain,
        )

    def _do_borrow(self, market: MarketSnapshot) -> Intent:
        """Phase 2: Borrow USDC against the already-supplied WBNB collateral.

        ``collateral_amount=Decimal("0")`` because the collateral was deposited
        by the preceding standalone SUPPLY intent (VIB-3586) — bundling it here
        would collapse the supply into the BORROW accounting event and trip the
        fail-closed guard.
        """
        try:
            collateral_price = market.price(self.collateral_token)
            borrow_price = market.price(self.borrow_token)
        except (ValueError, KeyError) as e:
            return Intent.hold(reason=f"Price data unavailable: {e}")

        collateral_value = self.collateral_amount * collateral_price
        borrow_amount = (collateral_value * self.ltv_target / borrow_price).quantize(
            Decimal("0.01"), rounding=ROUND_DOWN
        )

        if borrow_amount <= 0:
            return Intent.hold(reason="Computed borrow amount is zero")

        # Enforced risk control: clamp the borrow to a safe fraction of Aave's
        # LIVE available-borrow capacity. Sizing off config alone ignores the
        # actual on-chain position, so a collateral-price drop between SUPPLY and
        # BORROW could push the real LTV past target. FAILS CLOSED (HOLD, retry
        # next iteration) when health data is unavailable -- an "enforced" guard
        # must not be silently bypassed exactly when its safety signal is missing.
        try:
            health = market.position_health(protocol="aave_v3", market_id=self.chain)
            safe_ceiling_usd = health.max_borrow_usd * self.max_borrow_fraction
            borrow_amount_usd = borrow_amount * borrow_price
            if borrow_amount_usd > safe_ceiling_usd:
                borrow_amount = (safe_ceiling_usd / borrow_price).quantize(
                    Decimal("0.01"), rounding=ROUND_DOWN
                )
                logger.warning(
                    f"Clamping borrow to {format_token_amount_human(borrow_amount, self.borrow_token)}: "
                    f"requested {format_usd(borrow_amount_usd)} exceeds {self.max_borrow_fraction:.0%} of "
                    f"live capacity {format_usd(health.max_borrow_usd)} (HF={health.health_factor})"
                )
            if borrow_amount <= 0:
                return Intent.hold(reason="No safe borrow capacity available (live HF guard)")
        except HealthUnavailableError as e:
            logger.warning(f"Live borrow-capacity guard unavailable; holding (fail-closed): {e}")
            return Intent.hold(reason="Live borrow-capacity unavailable (fail-closed risk guard)")

        logger.info(
            f"Phase 2 BORROW: collateral {format_token_amount_human(self.collateral_amount, self.collateral_token)} "
            f"(value={format_usd(collateral_value)}, already supplied), "
            f"borrow {format_token_amount_human(borrow_amount, self.borrow_token)} "
            f"from Aave V3 (LTV={self.ltv_target * 100:.0f}%)"
        )
        self._transition(BORROWING)
        return Intent.borrow(
            protocol="aave_v3",
            collateral_token=self.collateral_token,
            collateral_amount=Decimal("0"),  # Already supplied by the SUPPLY phase
            borrow_token=self.borrow_token,
            borrow_amount=borrow_amount,
            chain=self.chain,
        )

    def _do_swap(self) -> Intent:
        """Phase 2: Swap borrowed USDC -> USDT via PancakeSwap V3."""
        swap_amount = self._borrowed_amount
        logger.info(
            f"Phase 2 SWAP: {format_token_amount_human(swap_amount, self.borrow_token)} "
            f"-> {self.swap_to_token} via PancakeSwap V3"
        )
        self._transition(SWAPPING)
        return Intent.swap(
            from_token=self.borrow_token,
            to_token=self.swap_to_token,
            amount=swap_amount,
            max_slippage=Decimal("0.005"),
            protocol="pancakeswap_v3",
            chain=self.chain,
        )

    def _do_swap_back(self) -> Intent:
        """Phase 3: Swap USDT back to USDC via PancakeSwap V3."""
        logger.info(
            f"Phase 3 SWAP_BACK: {format_token_amount_human(self._swapped_amount, self.swap_to_token)} "
            f"-> {self.borrow_token} via PancakeSwap V3"
        )
        self._transition(SWAPPING_BACK)
        return Intent.swap(
            from_token=self.swap_to_token,
            to_token=self.borrow_token,
            amount=self._swapped_amount,
            max_slippage=Decimal("0.005"),
            protocol="pancakeswap_v3",
            chain=self.chain,
        )

    def _do_repay(self) -> Intent:
        """Phase 4: Repay Aave V3 USDC debt (repay_full=True)."""
        logger.info(
            f"Phase 4 REPAY: repay_full=True for {format_token_amount_human(self._borrowed_amount, self.borrow_token)} "
            f"to Aave V3"
        )
        self._transition(REPAYING)
        return Intent.repay(
            token=self.borrow_token,
            amount=self._borrowed_amount,
            protocol="aave_v3",
            repay_full=True,
            chain=self.chain,
        )

    def _do_withdraw(self) -> Intent:
        """Phase 5: Withdraw WBNB collateral from Aave V3."""
        logger.info(
            f"Phase 5 WITHDRAW: withdraw_all=True for "
            f"{format_token_amount_human(self._supplied_amount, self.collateral_token)} from Aave V3"
        )
        self._transition(WITHDRAWING)
        return Intent.withdraw(
            token=self.collateral_token,
            amount=self._supplied_amount,
            protocol="aave_v3",
            withdraw_all=True,
            chain=self.chain,
        )

    def _transition(self, new_state: str) -> None:
        old = self._state
        if old in STABLE_STATES:
            self._previous_stable = old
        self._state = new_state
        logger.info(f"State: {old} -> {new_state}")

    # =========================================================================
    # LIFECYCLE HOOKS
    # =========================================================================

    def on_intent_executed(self, intent: Any, success: bool, result: Any) -> None:
        intent_type = getattr(intent, "intent_type", None)
        if intent_type is None:
            return
        intent_type_val = intent_type.value if hasattr(intent_type, "value") else str(intent_type)

        if success:
            if intent_type_val == "SUPPLY":
                self._state = SUPPLIED
                # Track the amount actually supplied by the executed intent so
                # accounting/teardown reflect what executed even if config is
                # hot-reloaded mid-flight.
                self._supplied_amount = Decimal(str(getattr(intent, "amount", self.collateral_amount)))
                logger.info(
                    f"SUPPLY OK: supplied={self._supplied_amount} {self.collateral_token} -- state -> supplied"
                )

            elif intent_type_val == "BORROW":
                self._state = BORROWED
                if hasattr(intent, "borrow_amount"):
                    self._borrowed_amount = Decimal(str(intent.borrow_amount))
                logger.info(
                    f"BORROW OK: supplied={self._supplied_amount} {self.collateral_token}, "
                    f"borrowed={self._borrowed_amount} {self.borrow_token}"
                )

            elif intent_type_val == "SWAP" and self._state == SWAPPING:
                self._state = SWAPPED
                self._swapped_amount = self._borrowed_amount  # ~1:1 for stablecoins
                if result and hasattr(result, "swap_amounts") and result.swap_amounts:
                    try:
                        self._swapped_amount = result.swap_amounts.amount_out_decimal
                    except (AttributeError, TypeError):
                        pass
                logger.info(
                    f"SWAP OK: {self.borrow_token} -> {self.swap_to_token}, "
                    f"swapped_amount={self._swapped_amount}"
                )

            elif intent_type_val == "SWAP" and self._state == SWAPPING_BACK:
                self._state = SWAP_BACK
                self._swapped_amount = Decimal("0")
                logger.info(f"SWAP_BACK OK: {self.swap_to_token} -> {self.borrow_token}")

            elif intent_type_val == "REPAY":
                self._state = REPAID
                self._borrowed_amount = Decimal("0")
                logger.info("REPAY OK: Aave V3 debt cleared")

            elif intent_type_val == "WITHDRAW":
                self._state = COMPLETE
                self._supplied_amount = Decimal("0")
                logger.info("WITHDRAW OK: Aave V3 collateral reclaimed. Full lifecycle done.")

        else:
            revert_to = self._previous_stable
            logger.warning(f"{intent_type_val} FAILED in '{self._state}' -- reverting to '{revert_to}'")
            self._state = revert_to

    # =========================================================================
    # STATUS & STATE PERSISTENCE
    # =========================================================================

    def get_status(self) -> dict[str, Any]:
        return {
            "strategy": "pancakeswap_aave_carry_bsc",
            "chain": self.chain,
            "state": self._state,
            f"supplied_{self.collateral_token.lower()}": str(self._supplied_amount),
            f"borrowed_{self.borrow_token.lower()}": str(self._borrowed_amount),
            f"swapped_{self.swap_to_token.lower()}": str(self._swapped_amount),
        }

    def get_persistent_state(self) -> dict[str, Any]:
        return {
            "state": self._state,
            "previous_stable": self._previous_stable,
            "supplied_amount": str(self._supplied_amount),
            "borrowed_amount": str(self._borrowed_amount),
            "swapped_amount": str(self._swapped_amount),
        }

    def load_persistent_state(self, state: dict[str, Any]) -> None:
        self._state = state.get("state", IDLE)
        self._previous_stable = state.get("previous_stable", IDLE)
        self._supplied_amount = Decimal(str(state.get("supplied_amount", "0")))
        self._borrowed_amount = Decimal(str(state.get("borrowed_amount", "0")))
        self._swapped_amount = Decimal(str(state.get("swapped_amount", "0")))
        logger.info(f"Restored state: {self._state}")

    # =========================================================================
    # TEARDOWN
    # =========================================================================

    def get_open_positions(self) -> "TeardownPositionSummary":
        from almanak.framework.teardown import PositionInfo, PositionType, TeardownPositionSummary

        positions: list[PositionInfo] = []
        effective_state = self._previous_stable if self._state in TRANSITIONAL_STATES else self._state

        if self._supplied_amount > 0:
            positions.append(
                PositionInfo(
                    position_type=PositionType.SUPPLY,
                    position_id=f"aave-v3-supply-{self.collateral_token}-bsc",
                    chain=self.chain,
                    protocol="aave_v3",
                    value_usd=Decimal("0"),
                    details={"asset": self.collateral_token, "amount": str(self._supplied_amount)},
                )
            )

        if self._borrowed_amount > 0:
            positions.append(
                PositionInfo(
                    position_type=PositionType.BORROW,
                    position_id=f"aave-v3-borrow-{self.borrow_token}-bsc",
                    chain=self.chain,
                    protocol="aave_v3",
                    value_usd=Decimal("0"),
                    details={"asset": self.borrow_token, "amount": str(self._borrowed_amount)},
                )
            )

        if self._swapped_amount > 0 and effective_state == SWAPPED:
            positions.append(
                PositionInfo(
                    position_type=PositionType.TOKEN,
                    position_id=f"pancakeswap-swap-{self.swap_to_token}-bsc",
                    chain=self.chain,
                    protocol="pancakeswap_v3",
                    value_usd=Decimal("0"),
                    details={"asset": self.swap_to_token, "amount": str(self._swapped_amount), "origin": "swapped_from_borrow"},
                )
            )

        return TeardownPositionSummary(
            deployment_id=self.STRATEGY_NAME,
            timestamp=datetime.now(UTC),
            positions=positions,
        )

    def generate_teardown_intents(self, mode: "TeardownMode", market=None) -> list[Intent]:
        from almanak.framework.teardown import TeardownMode

        intents = []
        effective_state = self._previous_stable if self._state in TRANSITIONAL_STATES else self._state
        slippage = Decimal("0.03") if mode == TeardownMode.HARD else Decimal("0.005")

        if self._swapped_amount > 0 and effective_state in (SWAPPED, SWAP_BACK):
            intents.append(
                Intent.swap(
                    from_token=self.swap_to_token,
                    to_token=self.borrow_token,
                    amount=self._swapped_amount,
                    max_slippage=slippage,
                    protocol="pancakeswap_v3",
                    chain=self.chain,
                )
            )

        if self._borrowed_amount > 0:
            intents.append(
                Intent.repay(
                    token=self.borrow_token,
                    amount=self._borrowed_amount,
                    repay_full=True,
                    protocol="aave_v3",
                    chain=self.chain,
                )
            )

        if self._supplied_amount > 0:
            intents.append(
                Intent.withdraw(
                    token=self.collateral_token,
                    amount=self._supplied_amount,
                    protocol="aave_v3",
                    withdraw_all=True,
                    chain=self.chain,
                )
            )

        return intents
