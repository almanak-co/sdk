"""
PancakeSwap V3 + Aave V3 Carry Trade on BSC
=============================================

T2 multi-protocol composition on BSC combining Aave V3 lending with
PancakeSwap V3 swaps.

Entry (``decide()`` — runs once, then HOLDs the carry):
  1. SUPPLY: Supply WBNB collateral to Aave V3 (use_as_collateral=True)
  2. BORROW: Borrow USDC against the supplied collateral at `ltv_target` (30%),
     clamped to a safe fraction of Aave's LIVE available-borrow capacity
  3. SWAP: Swap borrowed USDC -> USDT via PancakeSwap V3
  4. HOLD: The carry is now established (WBNB collateral / USDC debt / USDT held).
     ``decide()`` holds it — a carry earns by being held, and it is unwound on a
     teardown signal, not auto-closed one iteration after opening.

Teardown (``generate_teardown_intents()`` — the HF-safe unwind lane):
  * SWAP the held USDT back to USDC (close the swap leg), THEN
  * delegate the lending unwind to the framework's HF-safe
    ``generate_lending_unwind()`` primitive (VIB-5467 / TD-09).

Why the unwind is NOT hand-rolled in ``decide()`` (VIB-5637 / VIB-5448 / VIB-589):
  A carry round-trips borrow -> swap -> swap-back, so after the round trip the
  wallet holds LESS USDC than the interest-grown debt. A naive
  ``REPAY(repay_full) -> WITHDRAW(withdraw_all)`` therefore leaves *dust debt*
  (``repay_full`` = MAX_UINT256 caps at the wallet balance), and ``withdraw_all``
  then reverts ``HealthFactorLowerThanLiquidationThreshold`` (``0x6679996d``)
  because no collateral can be withdrawn while ANY debt remains — stranding the
  collateral. ``generate_lending_unwind`` eliminates that: it sizes every leg from
  the LIVE ``variableDebt`` / ``balanceOf`` and sources the interest shortfall from
  collateral (WITHDRAW -> SWAP -> REPAY staircase) so debt is a TRUE zero before the
  final ``withdraw_all``. Running the unwind in ``decide()`` (the iteration lane)
  bypassed both this primitive and the teardown-lane fresh-state guard, which is
  exactly how the BSC-mainnet strand in VIB-5637 happened. See blueprint 14
  §"Aave Borrow Strategy" — "Do NOT hand-roll REPAY(all) -> WITHDRAW(all)".

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
    # Establish the carry on Anvil (SUPPLY -> BORROW -> SWAP over a few iterations,
    # then HOLD). The unwind runs on a teardown signal, not automatically.
    almanak strat run -d almanak/demo_strategies/pancakeswap_aave_carry_bsc --network anvil --interval 5

    # Unwind (HF-safe) via the teardown signal:
    almanak strat teardown request -s <deployment_id> --wait
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

# Stable states. Entry advances idle -> supplied -> borrowed -> swapped, then HOLDs
# (the carry is established and unwound on a teardown signal, not in decide()). The
# swap_back / repaid / complete constants are retained ONLY for crash-resume of state
# persisted by a pre-VIB-5637 build that unwound in decide(); decide() no longer
# transitions into them (a restored legacy state degrades to HOLD).
IDLE = "idle"
SUPPLIED = "supplied"
BORROWED = "borrowed"
SWAPPED = "swapped"
SWAP_BACK = "swap_back"  # legacy (pre-VIB-5637); decide() no longer emits swap-back
REPAID = "repaid"  # legacy (pre-VIB-5637)
COMPLETE = "complete"  # legacy (pre-VIB-5637)

# Transitional states. Only the entry ones (supplying/borrowing/swapping) are still
# emitted; swapping_back/repaying/withdrawing are legacy (kept for crash-resume).
SUPPLYING = "supplying"
BORROWING = "borrowing"
SWAPPING = "swapping"
SWAPPING_BACK = "swapping_back"  # legacy (pre-VIB-5637)
REPAYING = "repaying"  # legacy (pre-VIB-5637)
WITHDRAWING = "withdrawing"  # legacy (pre-VIB-5637)

STABLE_STATES = {IDLE, SUPPLIED, BORROWED, SWAPPED, SWAP_BACK, REPAID, COMPLETE}
TRANSITIONAL_STATES = {SUPPLYING, BORROWING, SWAPPING, SWAPPING_BACK, REPAYING, WITHDRAWING}


@almanak_strategy(
    name="pancakeswap_aave_carry_bsc",
    description="PancakeSwap V3 + Aave V3 carry trade on BSC: supply -> borrow -> swap -> hold; HF-safe unwind on teardown",
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

    State machine (entry only; the unwind is teardown-owned, see
    ``generate_teardown_intents``):
        idle -> supplying -> supplied -> borrowing -> borrowed
            -> swapping -> swapped -> (HOLD the carry until teardown)

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
        """Establish the carry (SUPPLY -> BORROW -> SWAP), then HOLD it.

        ``decide()`` only ever ADDS exposure. The unwind (swap-back, repay,
        withdraw) is NOT run here: doing so in the iteration lane bypasses the
        HF-safe ``generate_lending_unwind`` primitive and the teardown fresh-state
        guard, which strands the collateral once accrued interest leaves dust debt
        the naive ``withdraw_all`` cannot clear (VIB-5637 / VIB-5448). The carry is
        held until a teardown signal drives ``generate_teardown_intents``.

        Data-unavailable reads degrade to HOLD inside the phase helpers; any
        other exception propagates (no blanket ``except -> hold`` masking bugs).
        """
        # Handle stuck transitional states by reverting
        if self._state in TRANSITIONAL_STATES:
            revert_to = self._previous_stable
            logger.warning(f"Stuck in '{self._state}' -- reverting to '{revert_to}'")
            self._state = revert_to

        # === ENTRY PHASE (the only phase decide() drives) ===
        if self._state == IDLE:
            return self._do_supply()

        if self._state == SUPPLIED:
            return self._do_borrow(market)

        if self._state == BORROWED:
            return self._do_swap()

        if self._state == SWAPPED:
            return Intent.hold(
                reason=(
                    "Carry established (WBNB collateral / USDC debt / USDT held). Holding — "
                    "the position is unwound HF-safely on a teardown signal, not in decide()."
                )
            )

        # Any other state (incl. legacy pre-VIB-5637 teardown-phase states restored
        # from persisted state) is terminal for decide(): hold and let teardown unwind.
        return Intent.hold(
            reason=f"State '{self._state}' is teardown-owned — holding until teardown signal."
        )

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

    # The carry's unwind (swap-back -> repay -> withdraw) is intentionally NOT a set
    # of decide() phase helpers. It runs through the teardown lane's HF-safe
    # ``generate_lending_unwind`` primitive (see ``generate_teardown_intents``), so a
    # naive ``repay_full -> withdraw_all`` can never strand the collateral on dust
    # debt (VIB-5637 / VIB-5448).

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
        """Track ENTRY-phase transitions only.

        ``decide()`` drives the entry phase (SUPPLY -> BORROW -> SWAP -> hold); the
        unwind runs through the teardown lane and its intents do NOT advance this
        state machine, so only the entry transitions are handled here. Each success
        branch is gated on its originating transitional state, so a teardown-lane
        SWAP/REPAY/WITHDRAW (should the runner surface it here) is a harmless no-op
        rather than a spurious entry transition; and the failure revert fires only
        for the entry transitional states, never disrupting a teardown in flight.
        """
        intent_type = getattr(intent, "intent_type", None)
        if intent_type is None:
            return
        intent_type_val = intent_type.value if hasattr(intent_type, "value") else str(intent_type)

        if success:
            if intent_type_val == "SUPPLY" and self._state == SUPPLYING:
                self._state = SUPPLIED
                # Track the amount actually supplied by the executed intent so
                # accounting/teardown reflect what executed even if config is
                # hot-reloaded mid-flight.
                self._supplied_amount = Decimal(str(getattr(intent, "amount", self.collateral_amount)))
                logger.info(
                    f"SUPPLY OK: supplied={self._supplied_amount} {self.collateral_token} -- state -> supplied"
                )

            elif intent_type_val == "BORROW" and self._state == BORROWING:
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
                    f"swapped_amount={self._swapped_amount} -- carry established, holding"
                )

        elif self._state in (SUPPLYING, BORROWING, SWAPPING):
            # Entry-phase failure: revert to the last stable entry state and retry.
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
        """Unwind the carry HF-safely on a teardown signal.

        (1) Close the swap leg: swap any held ``swap_to_token`` (USDT) back to the
            ``borrow_token`` (USDC).
        (2) Delegate the Aave lending unwind to the framework's HF-safe
            ``generate_lending_unwind`` primitive (VIB-5467 / TD-09). It sizes every
            leg from the LIVE ``variableDebt`` / ``balanceOf`` and drives debt to a
            true zero — sourcing the accrued-interest shortfall from collateral via a
            ``WITHDRAW -> SWAP -> REPAY`` staircase — BEFORE the final ``withdraw_all``,
            so a naive ``repay_full -> withdraw_all`` can never strand the collateral
            on dust debt (blueprint 14 §"Aave Borrow Strategy"). The primitive is
            called **unconditionally** (not gated on cached amounts) so a crash where
            SUPPLY/BORROW landed on-chain but the cached amounts were never persisted
            still unwinds the live position — it reads LIVE state and returns ``[]``
            when the position is already flat (matches the ``aave_borrow`` reference).
            ``swap_protocol="pancakeswap_v3"`` pins the primitive's collateral->debt
            swaps to this BSC venue (the declared/entry venue) instead of the
            compiler's ``uniswap_v3`` default.

        Hand-rolling ``repay_full`` / ``withdraw_all`` here is exactly the
        VIB-5637 / VIB-5448 dust-debt strand — do not reintroduce it.
        """
        from almanak.framework.teardown import (
            LendingUnwindError,
            TeardownMode,
            generate_lending_unwind,
        )

        effective_state = self._previous_stable if self._state in TRANSITIONAL_STATES else self._state
        slippage = Decimal("0.03") if mode == TeardownMode.HARD else Decimal("0.005")

        intents: list[Intent] = []

        # (1) Close the swap leg — recover the held swap_to_token into the borrow
        #     token (``amount="all"`` so it sweeps whatever is actually on-chain, not
        #     a cached figure that entry slippage may have drifted from). The teardown
        #     manager skips this swap when the wallet balance is zero.
        if self._swapped_amount > 0 and effective_state in (SWAPPED, SWAP_BACK):
            intents.append(
                Intent.swap(
                    from_token=self.swap_to_token,
                    to_token=self.borrow_token,
                    amount="all",
                    max_slippage=slippage,
                    protocol="pancakeswap_v3",
                    chain=self.chain,
                )
            )

        # (2) HF-safe lending unwind — delegate unconditionally (reference pattern);
        #     the primitive reads LIVE state and returns [] when flat.
        snapshot = market if market is not None else self.create_market_snapshot()
        try:
            intents.extend(
                generate_lending_unwind(
                    market=snapshot,
                    protocol="aave_v3",
                    collateral_token=self.collateral_token,
                    borrow_token=self.borrow_token,
                    chain=self.chain,
                    mode=mode,
                    swap_protocol="pancakeswap_v3",
                )
            )
        except LendingUnwindError:
            # The primitive plans against the PRE-swap snapshot (the close-leg
            # USDT->USDC swap above has not executed yet), so it cannot see those
            # proceeds. If the health factor is too low to source the debt from
            # collateral, it raises rather than emit a reverting withdraw. Fall back
            # to a risk-reducing ``repay_full``: it is execution-time-sized, so once
            # the close-leg swap lands the wallet holds USDC and this pays the debt
            # down (raising HF) WITHOUT withdrawing collateral near liquidation, and
            # it never reverts (Aave caps repay at the debt). The collateral withdraw
            # then completes on the next teardown pass once HF has recovered. This is
            # a single-snapshot teardown-builder limitation; teardown's first job is
            # removing on-chain risk, so a risk-reducing repay beats a failed unwind.
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

        return intents

    def on_teardown_completed(self, success: bool, recovered_usd: Decimal) -> None:
        """Clear the tracked position amounts once teardown has fully completed.

        The unwind is teardown-owned, and the teardown lane surfaces its intents to
        ``on_intent_executed`` — but clearing the cached amounts there would fire on
        the HF-safe primitive's *intermediate* partial REPAY/WITHDRAW staircase steps
        (premature/wrong). So the cached amounts are cleared HERE, once, at teardown
        completion — the framework's canonical clear hook (teardown post-conditions
        note; matches ``morpho_looping``). This runs AFTER closure verification, which
        uses the pre-execution snapshot, so it cannot mask a partial close. Without it
        ``get_open_positions()`` would keep reporting stale positions after a
        successful teardown.
        """
        if success:
            logger.info(
                f"Teardown completed. Recovered {format_usd(recovered_usd)} — clearing tracked carry state."
            )
            self._supplied_amount = Decimal("0")
            self._borrowed_amount = Decimal("0")
            self._swapped_amount = Decimal("0")
            # Terminal, non-re-entrant state (decide() HOLDs; never IDLE, which would
            # re-open the carry — cf. the teardown-terminal-entry-latch class VIB-5572).
            self._state = COMPLETE
            self._previous_stable = COMPLETE
        else:
            logger.error(
                "Teardown failed for pancakeswap_aave_carry_bsc — tracked state left intact "
                "for a retry; manual intervention may be required."
            )
