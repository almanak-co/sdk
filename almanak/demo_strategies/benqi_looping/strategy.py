"""BENQI Leverage Loop — leveraged-long AVAX with a health-factor defense.

A cross-asset leverage loop on BENQI (a Compound V2 fork on Avalanche). It is
the leveraged counterpart (PRD §5 archetype #9, "leverage loop w/ HF defense")
to the ``benqi_lending_lifecycle`` *tutorial* (archetype #8), and the
BENQI/Compound-V2 sibling of the ``morpho_looping`` crown jewel.

What it does — leveraged long AVAX
----------------------------------
1.  BUILD. Supply native AVAX as collateral, borrow USDC against it at
    ``target_ltv``, swap the USDC back into AVAX, and re-supply — repeating for
    ``target_loops`` rounds to amplify long-AVAX exposure. Each loop is five
    legs because BENQI's AVAX market is *native* (``qiAVAX.mint()`` payable)
    while DEXes trade *wrapped* WAVAX:

        SUPPLY(AVAX) -> BORROW(USDC) -> SWAP(USDC->WAVAX) -> UNWRAP(WAVAX->AVAX) -> SUPPLY(AVAX)

2.  HOLD. Once the target leverage is built the strategy HOLDs, recomputing the
    health factor every tick from live prices.
3.  UNWIND. When the health factor crosses ``hf_danger`` (AVAX fell) **or** a
    teardown signal arrives, it deleverages via a health-factor-aware staircase
    — each round withdraws only as much AVAX collateral as keeps the
    post-withdraw HF above ``hf_unwind_floor``, wraps it, swaps to USDC, and
    repays:

        WITHDRAW(AVAX) -> WRAP(AVAX->WAVAX) -> SWAP(WAVAX->USDC) -> REPAY(USDC)

    until the debt is gone, then a final WITHDRAW(all) reclaims the residual
    AVAX. Because AVAX collateral is volatile, the HF genuinely moves and the
    HF-danger unwind is a real risk control, not a dormant branch.

AVAX is both gas and collateral
-------------------------------
The chain's native token is the collateral *and* the gas currency, so the
strategy never supplies or wraps the whole native balance — it supplies/wraps
*specific* tracked amounts and leaves ``gas_reserve`` untouched. Re-supply and
wrap amounts come from the realized swap/withdraw outputs, never ``"all"`` on a
native leg.

Design rules honoured (the golden promotion gate)
-------------------------------------------------
- Position state (``_collateral_avax``, ``_debt_usdc``, ``_loops_done``) is
  committed ONLY in ``on_intent_executed``, after a fill confirms — never
  speculatively in ``decide()`` (only the transitional markers move there, and
  they revert on failure).
- The health factor is real math (``collateral_usd · collateral_factor /
  debt_usd`` from live prices), and the unwind staircase is sized from it so a
  withdraw never trips the on-chain collateral check.
- Standalone SUPPLY before BORROW (VIB-3586): collateral is supplied by its own
  intent, never bundled into ``Intent.borrow(collateral_amount>0)``.
- Fail-fast config validation in ``__init__``.
- Data-unavailable reads (prices) degrade to HOLD; any other exception propagates.
- No direct network egress — all data via ``MarketSnapshot`` / the gateway.
"""

import logging
from datetime import UTC, datetime
from decimal import ROUND_DOWN, Decimal
from typing import TYPE_CHECKING, Any

from almanak.framework.data import MarketSnapshotError, PriceUnavailableError
from almanak.framework.intents import AnyIntent, Intent
from almanak.framework.market import MarketSnapshot
from almanak.framework.strategies import IntentStrategy, almanak_strategy
from almanak.framework.utils.log_formatters import format_token_amount_human, format_usd

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from almanak.framework.teardown import TeardownMode, TeardownPositionSummary

# Reads that mean "data unavailable" -> HOLD. Everything else propagates so a
# real bug is never masked behind a blanket except.
_DATA_UNAVAILABLE_ERRORS = (PriceUnavailableError, MarketSnapshotError, ValueError, KeyError)

# Position legs below this USD value are treated as fully closed/repaid.
_DUST_USD = Decimal("0.05")
# Native AVAX below this token amount is treated as zero (residual dust).
_DUST_AVAX = Decimal("0.0001")

# Extra collateral withdrawn on the debt-clearing round so the final repay stays
# funded despite swap slippage and interest accrued during the unwind.
_SETTLE_BUFFER = Decimal("1.04")  # 4% over the debt's worth of collateral
# Backstop on staircase rounds (the position unwinds in 1–3 rounds in practice).
_MAX_UNWIND_ROUNDS = 12

_STABLE_STATES = frozenset(
    {"idle", "supplied", "borrowed", "swapped", "unwrapped", "levered", "unwind_withdraw", "unwind_wrap", "unwind_swap", "unwind_repay", "complete"}
)
_TRANSITIONAL_STATES = frozenset({"supplying", "borrowing", "swapping", "unwrapping", "withdrawing", "wrapping", "repaying"})
_VALID_STATES = _STABLE_STATES | _TRANSITIONAL_STATES


class _LeverageUnwindError(RuntimeError):
    """Health factor too low for any safe collateral withdrawal (needs flash-loan)."""


@almanak_strategy(
    name="benqi_looping",
    description="BENQI leveraged-long-AVAX loop: supply AVAX, borrow USDC, swap+unwrap back to AVAX for N loops, hold, then unwind via a health-factor-aware staircase on HF-danger or teardown",
    version="2.0.0",
    author="Almanak",
    tags=["demo", "lending", "leverage", "looping", "benqi", "avalanche"],
    supported_chains=["avalanche"],
    default_chain="avalanche",
    supported_protocols=["benqi"],
    intent_types=["SUPPLY", "BORROW", "SWAP", "UNWRAP_NATIVE", "WRAP_NATIVE", "REPAY", "WITHDRAW", "HOLD"],
    quote_asset="USD",
)
class BenqiLoopingStrategy(IntentStrategy):
    """Cross-asset leveraged-long-AVAX loop on BENQI with a health-factor defense."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)

        self.collateral_token = str(self.get_config("collateral_token", "AVAX"))
        self.borrow_token = str(self.get_config("borrow_token", "USDC"))
        self.wrapped_native = str(self.get_config("wrapped_native", "WAVAX"))
        self.initial_collateral = Decimal(str(self.get_config("initial_collateral", "0.1")))  # AVAX
        self.target_loops = int(self.get_config("target_loops", 2))
        self.target_ltv = Decimal(str(self.get_config("target_ltv", "0.5")))
        # BENQI Comptroller collateral factor for AVAX (volatile -> conservative).
        self.collateral_factor = Decimal(str(self.get_config("collateral_factor", "0.5")))
        self.hf_danger = Decimal(str(self.get_config("hf_danger", "1.15")))
        self.hf_unwind_floor = Decimal(str(self.get_config("hf_unwind_floor", "1.05")))
        self.swap_slippage = Decimal(str(self.get_config("swap_slippage", "0.01")))
        # Native AVAX held back for gas — never supplied or wrapped.
        self.gas_reserve = Decimal(str(self.get_config("gas_reserve", "0.05")))

        # Fail-fast config validation.
        if self.initial_collateral <= 0:
            raise ValueError("initial_collateral must be > 0")
        if self.target_loops < 1:
            raise ValueError("target_loops must be >= 1")
        if not (Decimal("0") < self.target_ltv < self.collateral_factor):
            raise ValueError(
                f"target_ltv ({self.target_ltv}) must be in (0, collateral_factor={self.collateral_factor}); "
                "borrowing at or above the collateral factor liquidates immediately."
            )
        if not (Decimal("0") < self.collateral_factor < 1):
            raise ValueError(f"collateral_factor ({self.collateral_factor}) must be in (0, 1)")
        if self.hf_danger <= 1:
            raise ValueError(f"hf_danger ({self.hf_danger}) must be > 1")
        if self.hf_unwind_floor <= 1:
            raise ValueError(f"hf_unwind_floor ({self.hf_unwind_floor}) must be > 1")
        if self.hf_danger <= self.hf_unwind_floor:
            raise ValueError(
                f"hf_danger ({self.hf_danger}) must be > hf_unwind_floor ({self.hf_unwind_floor}): "
                "unwinding starts at the danger threshold and targets the floor, so danger must sit above it."
            )
        if self.collateral_token == self.borrow_token:
            raise ValueError("collateral_token and borrow_token must differ (this is the cross-asset loop)")

        # State machine. Transitional markers move in decide(); stable state and
        # all position amounts commit only in on_intent_executed.
        self._state = "idle"
        self._previous_stable_state = "idle"
        self._collateral_avax = Decimal("0")  # total AVAX supplied (tracked)
        self._debt_usdc = Decimal("0")  # total USDC borrowed (tracked)
        self._loops_done = 0  # borrow legs executed
        self._last_supplied_avax = Decimal("0")  # most recent SUPPLY amount (next borrow base)
        self._last_borrowed_usdc = Decimal("0")  # most recent BORROW amount (next swap base)
        self._wallet_usdc = Decimal("0")  # borrowed/realized USDC awaiting swap or repay
        self._wallet_wavax = Decimal("0")  # WAVAX awaiting unwrap or swap
        self._pending_resupply_avax = Decimal("0")  # AVAX from unwrap awaiting re-supply
        self._pending_withdraw_avax = Decimal("0")  # AVAX slice withdrawn awaiting wrap

        logger.info(
            "BenqiLoopingStrategy(v2 cross-asset) init: chain=%s collateral=%s borrow=%s initial=%s AVAX "
            "loops=%d ltv=%s cf=%s hf_danger=%s",
            self.chain,
            self.collateral_token,
            self.borrow_token,
            self.initial_collateral,
            self.target_loops,
            self.target_ltv,
            self.collateral_factor,
            self.hf_danger,
        )

    # ------------------------------------------------------------------ HF math

    def _prices(self, market: MarketSnapshot) -> tuple[Decimal, Decimal]:
        """(avax_price, usdc_price). Raises on data-unavailable (caller -> HOLD)."""
        avax = Decimal(str(market.price(self.collateral_token)))
        usdc = Decimal(str(market.price(self.borrow_token)))
        if avax <= 0 or usdc <= 0:
            raise PriceUnavailableError(f"Non-positive price: {self.collateral_token}={avax} {self.borrow_token}={usdc}")
        return avax, usdc

    def _health_factor(self, avax_price: Decimal, usdc_price: Decimal) -> Decimal:
        """``collateral_usd · collateral_factor / debt_usd``; large sentinel when no debt."""
        debt_usd = self._debt_usdc * usdc_price
        if debt_usd <= _DUST_USD:
            return Decimal("999")
        return (self._collateral_avax * avax_price * self.collateral_factor) / debt_usd

    def _safe_withdraw_slice_usd(self, collateral_usd: Decimal, debt_usd: Decimal) -> Decimal:
        """Largest collateral USD slice whose withdrawal keeps HF >= hf_unwind_floor."""
        return collateral_usd - (self.hf_unwind_floor * debt_usd / self.collateral_factor)

    # ------------------------------------------------------------------- decide

    def decide(self, market: MarketSnapshot) -> Intent | None:
        """Drive the build -> hold -> unwind state machine (one intent per tick)."""
        state = self._state

        # ---- BUILD phase ------------------------------------------------------
        if state == "idle":
            self._enter_transitional("supplying")
            logger.info("BUILD: supply initial collateral %s", self._fmt_avax(self.initial_collateral))
            return self._supply_intent(self.initial_collateral)

        if state == "supplied":
            try:
                avax_price, _ = self._prices(market)
            except _DATA_UNAVAILABLE_ERRORS as e:
                return Intent.hold(reason=f"Price data unavailable: {e}")
            borrow_usdc = (self._last_supplied_avax * avax_price * self.target_ltv).quantize(
                Decimal("0.01"), rounding=ROUND_DOWN
            )
            if borrow_usdc <= _DUST_USD:
                return Intent.hold(
                    reason=(
                        f"Computed borrow={borrow_usdc} USDC too small "
                        f"(supplied {self._last_supplied_avax} AVAX at LTV {self.target_ltv}). "
                        "Increase initial_collateral or target_ltv."
                    )
                )
            self._enter_transitional("borrowing")
            logger.info(
                "BUILD: loop %d/%d — borrow %s against %s AVAX collateral",
                self._loops_done + 1,
                self.target_loops,
                format_usd(borrow_usdc),
                self._fmt_avax(self._collateral_avax),
            )
            return self._borrow_intent(borrow_usdc)

        if state == "borrowed":
            self._enter_transitional("swapping")
            logger.info("BUILD: swap %s USDC -> %s", self._last_borrowed_usdc, self.wrapped_native)
            return self._swap_intent(self.borrow_token, self.wrapped_native, self._wallet_usdc)

        if state == "swapped":
            self._enter_transitional("unwrapping")
            logger.info("BUILD: unwrap %s %s -> %s", self._wallet_wavax, self.wrapped_native, self.collateral_token)
            return self._unwrap_intent(self._wallet_wavax)

        if state == "unwrapped":
            self._enter_transitional("supplying")
            logger.info("BUILD: re-supply %s", self._fmt_avax(self._pending_resupply_avax))
            return self._supply_intent(self._pending_resupply_avax)

        # ---- HOLD phase -------------------------------------------------------
        if state == "levered":
            try:
                avax_price, usdc_price = self._prices(market)
            except _DATA_UNAVAILABLE_ERRORS as e:
                return Intent.hold(reason=f"Price data unavailable: {e}")
            hf = self._health_factor(avax_price, usdc_price)
            if hf <= self.hf_danger:
                logger.warning(
                    "HOLD -> UNWIND: HF %.3f <= danger %.3f (collateral=%s AVAX debt=%s USDC, AVAX=$%.2f)",
                    hf,
                    self.hf_danger,
                    self._fmt_avax(self._collateral_avax),
                    self._debt_usdc,
                    avax_price,
                )
                self._state = "unwind_withdraw"
                self._previous_stable_state = "unwind_withdraw"
                return self._unwind_step_intent(avax_price, usdc_price)
            return Intent.hold(
                reason=(
                    f"Levered long AVAX: {self._loops_done} loops, HF={hf:.3f}, "
                    f"collateral={self._collateral_avax} AVAX, debt={self._debt_usdc} USDC"
                )
            )

        # ---- UNWIND phase -----------------------------------------------------
        if state in ("unwind_withdraw", "unwind_wrap", "unwind_swap", "unwind_repay"):
            try:
                avax_price, usdc_price = self._prices(market)
            except _DATA_UNAVAILABLE_ERRORS as e:
                return Intent.hold(reason=f"Price data unavailable during unwind: {e}")
            return self._unwind_step_intent(avax_price, usdc_price)

        if state == "complete":
            return Intent.hold(reason=f"Position fully unwound — {self._loops_done} loops repaid and reclaimed")

        # ---- Stuck transitional -> revert -------------------------------------
        if state in _TRANSITIONAL_STATES:
            logger.warning("Stuck in transitional state '%s' — reverting to '%s'", state, self._previous_stable_state)
            self._state = self._previous_stable_state
        return Intent.hold(reason=f"Waiting for state transition (current: {self._state})")

    def _unwind_step_intent(self, avax_price: Decimal, usdc_price: Decimal) -> Intent:
        """Emit the next intent of the health-factor-aware unwind staircase."""
        if self._state == "unwind_withdraw":
            debt_usd = self._debt_usdc * usdc_price
            if debt_usd <= _DUST_USD:
                # Debt cleared — reclaim remaining AVAX collateral and finish.
                # withdraw_all compiles redeem(<full qiToken balance>) since
                # VIB-5404, so the close reclaims accrued interest too and
                # leaves a truly flat position (the old redeemUnderlying(tracked)
                # workaround stranded every wei of accrued interest and tripped
                # the VIB-5795 post-close residual check).
                if self._collateral_avax > _DUST_AVAX:
                    self._enter_transitional("withdrawing")
                    self._pending_withdraw_avax = self._collateral_avax
                    logger.info("UNWIND: final WITHDRAW of %s residual collateral", self._fmt_avax(self._collateral_avax))
                    return self._withdraw_intent(withdraw_all=True)
                self._state = "complete"
                return Intent.hold(reason="Position flat — nothing left to unwind")

            collateral_usd = self._collateral_avax * avax_price
            safe_usd = self._safe_withdraw_slice_usd(collateral_usd, debt_usd)
            if safe_usd <= _DUST_USD:
                logger.critical(
                    "UNWIND BLOCKED: HF %.3f too low for a safe withdraw (collateral=$%.2f debt=$%.2f). "
                    "Needs flash-loan deleverage or manual intervention.",
                    self._health_factor(avax_price, usdc_price),
                    collateral_usd,
                    debt_usd,
                )
                return Intent.hold(reason="Unwind blocked: health factor too low for a safe withdrawal")

            # Cap the slice so we don't pull more than the debt (+ settle buffer) or
            # more collateral than we hold.
            slice_usd = min(safe_usd, debt_usd * _SETTLE_BUFFER, collateral_usd)
            slice_avax = (slice_usd / avax_price).quantize(Decimal("0.000001"), rounding=ROUND_DOWN)
            self._pending_withdraw_avax = slice_avax
            self._enter_transitional("withdrawing")
            logger.info(
                "UNWIND: WITHDRAW %s (safe slice; HF floor %.2f, debt=%s USDC)",
                self._fmt_avax(slice_avax),
                self.hf_unwind_floor,
                self._debt_usdc,
            )
            return self._withdraw_intent(amount=slice_avax)

        if self._state == "unwind_wrap":
            self._enter_transitional("wrapping")
            logger.info("UNWIND: WRAP %s -> %s", self._fmt_avax(self._pending_withdraw_avax), self.wrapped_native)
            return self._wrap_intent(self._pending_withdraw_avax)

        if self._state == "unwind_swap":
            self._enter_transitional("swapping")
            logger.info("UNWIND: SWAP %s %s -> %s", self._wallet_wavax, self.wrapped_native, self.borrow_token)
            return self._swap_intent(self.wrapped_native, self.borrow_token, self._wallet_wavax)

        # unwind_repay
        repay_usd = self._wallet_usdc * usdc_price
        debt_usd = self._debt_usdc * usdc_price
        if repay_usd >= debt_usd:
            self._enter_transitional("repaying")
            logger.info("UNWIND: REPAY(full) — wallet %s USDC covers debt %s", self._wallet_usdc, self._debt_usdc)
            return self._repay_intent(repay_full=True)
        self._enter_transitional("repaying")
        logger.info("UNWIND: REPAY %s USDC (partial; debt remaining %s)", self._wallet_usdc, self._debt_usdc)
        return self._repay_intent(amount=self._wallet_usdc)

    # ----------------------------------------------------------- intent factory

    def _supply_intent(self, amount: Decimal) -> Intent:
        return Intent.supply(
            protocol="benqi",
            token=self.collateral_token,
            amount=amount,
            use_as_collateral=True,  # idempotent enterMarkets so each supply counts as collateral
            chain=self.chain,
        )

    def _borrow_intent(self, amount_usdc: Decimal) -> Intent:
        # collateral_amount=0: collateral is supplied by the standalone SUPPLY
        # intent (VIB-3586); bundling it here would collapse the supply event.
        return Intent.borrow(
            protocol="benqi",
            collateral_token=self.collateral_token,
            collateral_amount=Decimal("0"),
            borrow_token=self.borrow_token,
            borrow_amount=amount_usdc,
            chain=self.chain,
        )

    def _swap_intent(self, from_token: str, to_token: str, amount: Decimal) -> Intent:
        return Intent.swap(
            from_token=from_token,
            to_token=to_token,
            amount=amount,
            max_slippage=self.swap_slippage,
            chain=self.chain,
        )

    def _unwrap_intent(self, amount: Decimal) -> Intent:
        # WAVAX is not the gas token, so unwrapping the whole WAVAX balance is safe.
        return Intent.unwrap(token=self.wrapped_native, amount=amount, chain=self.chain)

    def _wrap_intent(self, amount: Decimal) -> Intent:
        # Wrap only the withdrawn slice — never "all", which would wrap the gas reserve.
        return Intent.wrap(token=self.wrapped_native, amount=amount, chain=self.chain)

    def _withdraw_intent(self, *, amount: Decimal | None = None, withdraw_all: bool = False) -> Intent:
        return Intent.withdraw(
            token=self.collateral_token,
            amount=amount if amount is not None else Decimal("0"),
            protocol="benqi",
            withdraw_all=withdraw_all,
            chain=self.chain,
        )

    def _repay_intent(self, *, amount: Decimal | None = None, repay_full: bool = False) -> Intent:
        return Intent.repay(
            token=self.borrow_token,
            amount=amount if amount is not None else Decimal("0"),
            protocol="benqi",
            repay_full=repay_full,
            chain=self.chain,
        )

    # ------------------------------------------------------------ lifecycle hook

    def on_intent_executed(self, intent: Intent, success: bool, result: Any) -> None:
        """Commit position amounts + advance the stable state, only after a fill."""
        intent_type = intent.intent_type.value

        if not success:
            logger.warning(
                "%s failed in state '%s' — reverting to '%s'", intent_type, self._state, self._previous_stable_state
            )
            self._state = self._previous_stable_state
            self._pending_withdraw_avax = Decimal("0")
            return

        if intent_type == "SUPPLY":
            amount = Decimal(str(getattr(intent, "amount", self._last_supplied_avax)))
            self._collateral_avax += amount
            self._last_supplied_avax = amount
            self._pending_resupply_avax = max(Decimal("0"), self._pending_resupply_avax - amount)
            self._state = "levered" if self._loops_done >= self.target_loops else "supplied"
            self._previous_stable_state = self._state
            logger.info(
                "SUPPLY ok: +%s -> collateral=%s (loops %d/%d) -> %s",
                self._fmt_avax(amount),
                self._fmt_avax(self._collateral_avax),
                self._loops_done,
                self.target_loops,
                self._state,
            )

        elif intent_type == "BORROW":
            amount = Decimal(str(getattr(intent, "borrow_amount", self._last_borrowed_usdc)))
            self._debt_usdc += amount
            self._last_borrowed_usdc = amount
            self._wallet_usdc += amount
            self._loops_done += 1
            self._state = "borrowed"
            self._previous_stable_state = "borrowed"
            logger.info(
                "BORROW ok: +%s USDC -> debt=%s (loops %d/%d) -> borrowed",
                amount,
                self._debt_usdc,
                self._loops_done,
                self.target_loops,
            )

        elif intent_type == "SWAP":
            out = self._extract_swap_output_amount(result)
            if self._state in ("swapping",) and self._previous_stable_state == "borrowed":
                # BUILD swap: USDC -> WAVAX
                self._wallet_usdc = max(Decimal("0"), self._wallet_usdc - self._last_borrowed_usdc)
                wavax_out = out if out is not None else (self._last_borrowed_usdc * (Decimal("1") - self.swap_slippage))
                self._wallet_wavax += wavax_out
                self._state = "swapped"
                self._previous_stable_state = "swapped"
                logger.info("SWAP ok (build): +%s WAVAX -> swapped", self._fmt_avax(wavax_out))
            else:
                # UNWIND swap: WAVAX -> USDC
                usdc_out = out if out is not None else Decimal("0")
                self._wallet_wavax = Decimal("0")
                self._wallet_usdc += usdc_out
                self._state = "unwind_repay"
                self._previous_stable_state = "unwind_repay"
                logger.info("SWAP ok (unwind): +%s USDC -> unwind_repay", usdc_out)

        elif intent_type == "UNWRAP_NATIVE":
            unwrapped = self._wallet_wavax
            self._pending_resupply_avax += unwrapped
            self._wallet_wavax = Decimal("0")
            self._state = "unwrapped"
            self._previous_stable_state = "unwrapped"
            logger.info("UNWRAP ok: +%s AVAX -> unwrapped", self._fmt_avax(unwrapped))

        elif intent_type == "WRAP_NATIVE":
            wrapped = self._pending_withdraw_avax
            self._wallet_wavax += wrapped
            self._pending_withdraw_avax = Decimal("0")
            self._state = "unwind_swap"
            self._previous_stable_state = "unwind_swap"
            logger.info("WRAP ok: %s -> WAVAX -> unwind_swap", self._fmt_avax(wrapped))

        elif intent_type == "WITHDRAW":
            withdrawn = self._pending_withdraw_avax
            self._collateral_avax = max(Decimal("0"), self._collateral_avax - withdrawn)
            # Debt already cleared -> this was the final reclaim. Otherwise it's a
            # staircase slice that still needs wrap -> swap -> repay (the WRAP step
            # consumes _pending_withdraw_avax, so do NOT zero it here in that case).
            if self._debt_usdc <= _DUST_USD:
                self._pending_withdraw_avax = Decimal("0")
                self._collateral_avax = Decimal("0")  # residual interest dust left on-chain
                self._state = "complete"
                self._previous_stable_state = "complete"
                logger.info("WITHDRAW ok: reclaimed residual %s -> complete", self._fmt_avax(withdrawn))
            else:
                self._state = "unwind_wrap"
                self._previous_stable_state = "unwind_wrap"
                logger.info(
                    "WITHDRAW ok: -%s -> collateral=%s -> unwind_wrap",
                    self._fmt_avax(withdrawn),
                    self._fmt_avax(self._collateral_avax),
                )

        elif intent_type == "REPAY":
            repay_full = bool(getattr(intent, "repay_full", False))
            if repay_full:
                repaid = min(self._wallet_usdc, self._debt_usdc)
                self._debt_usdc = Decimal("0")
            else:
                repaid = Decimal(str(getattr(intent, "amount", "0")))
                self._debt_usdc = max(Decimal("0"), self._debt_usdc - repaid)
            self._wallet_usdc = max(Decimal("0"), self._wallet_usdc - repaid)
            self._state = "unwind_withdraw"
            self._previous_stable_state = "unwind_withdraw"
            logger.info("REPAY ok: -%s USDC -> debt=%s -> unwind_withdraw", repaid, self._debt_usdc)

    # ---------------------------------------------------------------- teardown

    def supports_teardown(self) -> bool:
        return True

    def get_open_positions(self) -> "TeardownPositionSummary":
        """Report the levered position (AVAX collateral supply + USDC debt) for teardown."""
        from almanak.framework.teardown import PositionInfo, PositionType, TeardownPositionSummary

        try:
            avax_price = Decimal(str(self.create_market_snapshot().price(self.collateral_token)))
        except Exception:
            logger.warning("Unable to fetch %s price for teardown valuation", self.collateral_token)
            avax_price = Decimal("0")

        positions: list[PositionInfo] = []
        if self._collateral_avax > _DUST_AVAX:
            positions.append(
                PositionInfo(
                    position_type=PositionType.SUPPLY,
                    position_id=f"benqi-collateral-{self.collateral_token}-{self.chain}",
                    chain=self.chain,
                    protocol="benqi",
                    value_usd=self._collateral_avax * avax_price,
                    details={"asset": self.collateral_token, "type": "collateral", "loops": self._loops_done},
                )
            )
        if self._debt_usdc > _DUST_USD:
            positions.append(
                PositionInfo(
                    position_type=PositionType.BORROW,
                    position_id=f"benqi-borrow-{self.borrow_token}-{self.chain}",
                    chain=self.chain,
                    protocol="benqi",
                    value_usd=self._debt_usdc,
                    details={"asset": self.borrow_token},
                )
            )
        return TeardownPositionSummary(deployment_id=self.STRATEGY_NAME, timestamp=datetime.now(UTC), positions=positions)

    def generate_teardown_intents(self, mode: "TeardownMode", market: Any = None) -> list[AnyIntent]:
        """Unwind the levered position via the same HF-aware staircase as the live path.

        Computed up front from tracked amounts + live prices: N x
        (withdraw safe AVAX slice, wrap, swap to USDC, repay) then a final
        withdraw-all of the residual AVAX collateral.
        """
        if self._debt_usdc <= _DUST_USD and self._collateral_avax <= _DUST_AVAX:
            return []

        market = market if market is not None else self.create_market_snapshot()
        avax_price, usdc_price = self._prices(market)

        intents: list[AnyIntent] = []
        collateral_avax = self._collateral_avax
        debt_usd = self._debt_usdc * usdc_price

        for _ in range(_MAX_UNWIND_ROUNDS):
            if debt_usd <= _DUST_USD:
                break
            collateral_usd = collateral_avax * avax_price
            safe_usd = self._safe_withdraw_slice_usd(collateral_usd, debt_usd)
            if safe_usd <= _DUST_USD:
                raise _LeverageUnwindError(
                    f"Cannot unwind benqi {self.collateral_token}/{self.borrow_token}: HF too low for a safe "
                    f"withdrawal (collateral=${collateral_usd:.2f}, debt=${debt_usd:.2f}, cf={self.collateral_factor})."
                )
            slice_usd = min(safe_usd, debt_usd * _SETTLE_BUFFER, collateral_usd)
            slice_avax = (slice_usd / avax_price).quantize(Decimal("0.000001"), rounding=ROUND_DOWN)
            intents.append(self._withdraw_intent(amount=slice_avax))
            intents.append(self._wrap_intent(slice_avax))
            intents.append(self._swap_intent(self.wrapped_native, self.borrow_token, slice_avax))
            # Estimated USDC out of the slice swap; repay full when it covers the debt.
            est_usdc_out = slice_avax * avax_price * (Decimal("1") - self.swap_slippage)
            # est_usdc_out is a USD estimate; compare against the *remaining* debt
            # (debt_usd shrinks each round), not the fixed original self._debt_usdc —
            # else repay_full never fires after round 1 and a dust debt is stranded.
            if est_usdc_out >= debt_usd:
                intents.append(self._repay_intent(repay_full=True))
                debt_usd = Decimal("0")
            else:
                intents.append(self._repay_intent(amount=est_usdc_out.quantize(Decimal("0.01"), rounding=ROUND_DOWN)))
                debt_usd -= est_usdc_out * usdc_price
            collateral_avax -= slice_avax

        if collateral_avax > _DUST_AVAX:
            # withdraw_all compiles redeem(<full qiToken balance>) since VIB-5404 —
            # reclaims accrued interest too, leaving a truly flat position for the
            # VIB-5795 post-close on-chain verification.
            intents.append(self._withdraw_intent(withdraw_all=True))

        logger.info("Teardown: %d intents to unwind %d-loop position", len(intents), self._loops_done)
        return intents

    # ------------------------------------------------------------ status/state

    def get_status(self) -> dict[str, Any]:
        return {
            "strategy": "benqi_looping",
            "chain": self.chain,
            "protocol": "benqi",
            "collateral_token": self.collateral_token,
            "borrow_token": self.borrow_token,
            "state": self._state,
            "loops_done": self._loops_done,
            "target_loops": self.target_loops,
            "collateral_avax": str(self._collateral_avax),
            "debt_usdc": str(self._debt_usdc),
        }

    def get_persistent_state(self) -> dict[str, Any]:
        return {
            "state": self._state,
            "previous_stable_state": self._previous_stable_state,
            "collateral_avax": str(self._collateral_avax),
            "debt_usdc": str(self._debt_usdc),
            "loops_done": self._loops_done,
            "last_supplied_avax": str(self._last_supplied_avax),
            "last_borrowed_usdc": str(self._last_borrowed_usdc),
            "wallet_usdc": str(self._wallet_usdc),
            "wallet_wavax": str(self._wallet_wavax),
            "pending_resupply_avax": str(self._pending_resupply_avax),
            "pending_withdraw_avax": str(self._pending_withdraw_avax),
        }

    def is_lifecycle_complete(self) -> bool:
        """Terminal when the leverage loop has been fully unwound.

        Feeds the resume-into-terminal-state boot guard (VIB-5887): resuming this
        deployment at ``"complete"`` (all loops repaid + collateral reclaimed)
        means ``decide()`` will HOLD forever, so if the wallet holds fresh capital
        the runner warns instead of silently no-oping. Note ``"levered"`` (holding
        a built leverage position) is deliberately NOT terminal — that HOLD is an
        active position, not a finished lifecycle.
        """
        return self._state == "complete"

    def load_persistent_state(self, state: dict[str, Any]) -> None:
        if state.get("state") in _VALID_STATES:
            self._state = state["state"]
        if state.get("previous_stable_state") in _STABLE_STATES:
            self._previous_stable_state = state["previous_stable_state"]
        for key, attr in (
            ("collateral_avax", "_collateral_avax"),
            ("debt_usdc", "_debt_usdc"),
            ("last_supplied_avax", "_last_supplied_avax"),
            ("last_borrowed_usdc", "_last_borrowed_usdc"),
            ("wallet_usdc", "_wallet_usdc"),
            ("wallet_wavax", "_wallet_wavax"),
            ("pending_resupply_avax", "_pending_resupply_avax"),
            ("pending_withdraw_avax", "_pending_withdraw_avax"),
        ):
            if key in state:
                try:
                    setattr(self, attr, Decimal(str(state[key])))
                except Exception:
                    logger.warning("Invalid %s in persisted state: %r", key, state[key])
        if "loops_done" in state:
            try:
                self._loops_done = int(state["loops_done"])
            except Exception:
                logger.warning("Invalid loops_done in persisted state: %r", state["loops_done"])

    # ---------------------------------------------------------------- helpers

    def _extract_swap_output_amount(self, result: Any) -> Decimal | None:
        """Return the realized swap output amount in human units when available."""
        if result is None:
            return None
        swap_amounts = getattr(result, "swap_amounts", None)
        if swap_amounts is None:
            extracted = getattr(result, "extracted_data", {}) or {}
            swap_amounts = extracted.get("swap_amounts")
        amount_out_decimal = getattr(swap_amounts, "amount_out_decimal", None)
        if amount_out_decimal is None and isinstance(swap_amounts, dict):
            amount_out_decimal = swap_amounts.get("amount_out_decimal")
        if amount_out_decimal is None:
            return None
        amount_out = Decimal(str(amount_out_decimal))
        return amount_out if amount_out > 0 else None

    def _enter_transitional(self, transitional: str) -> None:
        """Move to a transitional state, remembering the stable state to revert to."""
        if self._state in _STABLE_STATES:
            self._previous_stable_state = self._state
        self._state = transitional

    def _fmt_avax(self, amount: Decimal) -> str:
        return format_token_amount_human(amount, self.collateral_token)
