"""Hyperliquid Trailing-Stop Perp — a ratcheting take-profit / stop scalper on HyperEVM.

A single-direction perpetual on Hyperliquid (chain 999, via CoreWriter) that
holds one position at a time and manages the exit with a **trailing stop that
ratchets**: it locks in gains once the trade moves in your favour, rather than
sitting on a static bracket. It exists to exercise — and show off — the two
things that make the Hyperliquid connector distinctive, end to end:

  1. THE READ PIPE — the HyperCore position precompile (``0x0800``). Portfolio
     valuation reads the live position off HyperCore (a settlement OBSERVER: the
     order settles off the EVM and only "appears" on a later read). This demo's
     exit logic is driven by that same live mark-to-market, not a guess.

  2. THE WRITE PIPE — CoreWriter (``0x3333…3333``). Every open/close is an IOC
     order submitted through CoreWriter and settled asynchronously on HyperCore.

There are NO native trigger orders here — Hyperliquid (like every perp venue we
support today) only takes MARKET-style IOC orders through the intent vocabulary
(there is no resting limit / stop-limit / take-profit ORDER type an intent can
carry). So the take-profit, hard-stop, and trailing-stop are all evaluated
**strategy-side** each tick and executed as a market (reduce-only) close when a
threshold trips. That is the honest shape of a TP/SL on this venue — do not read
it as an on-chain bracket order resting on the book.

The "little interesting" part — the trailing stop:
    - A TAKE-PROFIT caps the upside (bank a win at ``take_profit_pct``).
    - A hard STOP-LOSS caps the downside (``stop_loss_pct``), evaluated against
      the FILL price, as the liquidation buffer.
    - Once the position is up by ``trail_activation_pct``, a TRAILING stop
      engages: the exit ratchets up behind the high-water PnL and closes if the
      trade gives back ``trail_pct`` from its peak. This turns a good move into a
      realised gain instead of round-tripping it back to the stop.
    - After a close, the strategy re-enters next tick (a continuous scalper),
      unless ``reenter_after_close`` is false (one-shot lifecycle).

Design rules honoured (the golden promotion gate — same as gmx_v2_directional_perp):
  - State (`_position_side`, `_entry_price`, `_high_water_pnl`) is committed ONLY
    in on_intent_executed, after a fill confirms — never speculatively in decide().
  - FILL-VS-SUBMISSION RECONCILIATION (VIB-5597): CoreWriter settles async — a
    submitted IOC order can partial-fill or be rejected while the EVM tx still
    returns status 1. So a PERP_OPEN submission commits the position as PENDING
    (unconfirmed), NOT as open. It is promoted to a live, managed position only
    once the fill is OBSERVED — via the reconcile_fill() seam, driven by the
    `0x0800` position read and/or orderStatus-by-cloid (the same settlement-
    observer shape gmx_v2 uses for keeper-settled fills). An unconfirmed
    submission is `unmeasured`, never assumed-filled: while pending, the strategy
    HOLDs (it neither manages a maybe-nonexistent position nor re-opens a
    maybe-existing one), and it reports no confirmed open position.
  - PnL is measured against the FILL price, not the signal-time price.
  - Data-unavailable reads degrade to HOLD; any other exception propagates.
  - No direct network egress — all data via MarketSnapshot / the gateway.
  - Margin lives on HyperCore (off-EVM), so there is deliberately NO EVM-wallet
    collateral-balance gate: the EVM balance is not the perp margin.

State machine (single direction, set by ``is_long``):
    FLAT --(open)--------------------------> IN_POSITION
    IN_POSITION --(take-profit hit)--------> close --> FLAT
    IN_POSITION --(hard stop hit)----------> close --> FLAT
    IN_POSITION --(gave back trail from peak)--> close --> FLAT
    FLAT --(reenter_after_close)-----------> IN_POSITION

Note: this demo targets ``hyperevm`` and cannot run on a managed Anvil fork —
the HyperCore precompiles only exist on the live node, and the gateway data
layer for hyperevm (price / balance) is not yet wired (VIB-5576). It is
quarantined for CI until that lands; it is the natural acceptance test for it.

Usage (once VIB-5576 lands the hyperevm data layer):
    almanak strat run -d almanak/demo_strategies/hyperliquid_trailing_perp --network mainnet --interval 15
"""

import logging
from datetime import UTC, datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Any

from almanak.connectors.hyperliquid.fill_reconciliation import FillStatus
from almanak.framework.data import BalanceUnavailableError, MarketSnapshotError, PriceUnavailableError
from almanak.framework.intents import Intent
from almanak.framework.market import MarketSnapshot
from almanak.framework.strategies import IntentStrategy, almanak_strategy

if TYPE_CHECKING:
    from almanak.framework.teardown import TeardownMode, TeardownPositionSummary

logger = logging.getLogger(__name__)

# Reads that mean "data unavailable" -> HOLD. Everything else propagates so a
# real bug is never masked behind a blanket except.
_DATA_UNAVAILABLE_ERRORS = (
    PriceUnavailableError,
    BalanceUnavailableError,
    MarketSnapshotError,
    ValueError,
)

LONG = "long"
SHORT = "short"

_PROTOCOL = "hyperliquid"
# HyperCore rejects opening orders below ~$10 notional (reduce-only closes are
# exempt). A demo default below this would never fill; warn loudly at construction.
_HYPERCORE_MIN_ORDER_USD = Decimal("10")


@almanak_strategy(
    name="hyperliquid_trailing_perp",
    description="Hyperliquid perp with a ratcheting trailing-stop: take-profit + hard stop + trailing exit, executed as market IOC closes via CoreWriter",
    version="1.0.0",
    author="Almanak",
    tags=["perp", "hyperliquid", "hyperevm", "corewriter", "trailing-stop", "take-profit"],
    supported_chains=["hyperevm"],
    default_chain="hyperevm",
    supported_protocols=["hyperliquid"],
    intent_types=["PERP_OPEN", "PERP_CLOSE", "HOLD"],
    quote_asset="USD",
)
class HyperliquidTrailingPerp(IntentStrategy):
    """Single-direction Hyperliquid perp with take-profit, hard stop, and a ratcheting trailing stop."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)

        # "ETH/USD" is the market (compiler normalises to the HyperCore asset);
        # base_token is the symbol the price oracle is keyed on for PnL reads.
        self.market = str(self.get_config("market", "ETH/USD"))
        self.base_token = str(self.get_config("base_token", "ETH"))
        # HyperCore perps are USDC-margined; carried for intent parity only
        # (the compiler ignores collateral on HyperEVM — margin is off-EVM).
        self.collateral_token = str(self.get_config("collateral_token", "USDC"))

        self.size_usd = Decimal(str(self.get_config("size_usd", "15")))
        self.leverage = Decimal(str(self.get_config("leverage", "2.0")))
        self.max_slippage = Decimal(str(self.get_config("max_slippage", "0.01")))
        self.is_long = bool(self.get_config("is_long", True))

        # Exit thresholds (fractions of entry price).
        self.take_profit_pct = Decimal(str(self.get_config("take_profit_pct", "0.02")))
        self.stop_loss_pct = Decimal(str(self.get_config("stop_loss_pct", "0.03")))
        # Trailing stop: engages once up trail_activation_pct, then exits if PnL
        # falls trail_pct below its high-water mark.
        self.trail_activation_pct = Decimal(str(self.get_config("trail_activation_pct", "0.01")))
        self.trail_pct = Decimal(str(self.get_config("trail_pct", "0.015")))

        self.reenter_after_close = bool(self.get_config("reenter_after_close", True))

        # Fail-fast config validation (reject nonsensical parameters at
        # construction rather than emitting a malformed intent at runtime).
        if self.size_usd <= 0:
            raise ValueError("size_usd must be > 0")
        if self.leverage < 1:
            raise ValueError("leverage must be >= 1 (Hyperliquid minimum)")
        if not 0 <= self.max_slippage <= 1:
            raise ValueError("max_slippage must be between 0 and 1")
        for name, val in (
            ("take_profit_pct", self.take_profit_pct),
            ("stop_loss_pct", self.stop_loss_pct),
            ("trail_activation_pct", self.trail_activation_pct),
            ("trail_pct", self.trail_pct),
        ):
            if not 0 < val < 1:
                raise ValueError(f"{name} must be between 0 and 1 (exclusive), got {val}")
        # The trailing stop must be able to engage before the take-profit caps the
        # trade, or it can never fire (TP would always close first).
        if self.trail_activation_pct >= self.take_profit_pct:
            raise ValueError(
                f"trail_activation_pct ({self.trail_activation_pct}) must be < take_profit_pct "
                f"({self.take_profit_pct}) or the trailing stop can never engage before take-profit"
            )

        if self.size_usd < _HYPERCORE_MIN_ORDER_USD:
            logger.warning(
                "size_usd $%s is below the HyperCore ~$%s minimum order value; opens will be rejected "
                "on-chain (reduce-only closes are exempt). Raise size_usd.",
                self.size_usd, _HYPERCORE_MIN_ORDER_USD,
            )

        # Liquidation distance ~ 1/leverage; the hard stop must sit inside it.
        liq_distance = Decimal("1") / self.leverage
        if self.stop_loss_pct >= liq_distance:
            logger.warning(
                "stop_loss_pct %.2f >= liquidation distance ~%.2f (1/leverage): the stop offers no "
                "buffer before liquidation. Lower stop_loss_pct or leverage.",
                self.stop_loss_pct, liq_distance,
            )

        self.force_action = str(self.get_config("force_action", "") or "").strip().lower()

        # State — committed only in on_intent_executed.
        self._position_side: str | None = None
        self._entry_price: Decimal | None = None
        # Peak favourable PnL fraction since entry; drives the trailing stop.
        self._high_water_pnl: Decimal = Decimal("0")
        # Decide-time price, the entry-price fallback if the fill omits one.
        self._pending_entry_price: Decimal | None = None
        # Fill-vs-submission reconciliation (VIB-5597): a PERP_OPEN submission sets
        # ``_position_side`` but leaves ``_fill_confirmed`` False (PENDING) — the
        # position is not managed as live until reconcile_fill() observes the fill.
        # None side + not-confirmed = FLAT; a side + not-confirmed = PENDING
        # (submitted, unconfirmed); a side + confirmed = a live managed position.
        self._fill_confirmed: bool = False

        logger.info(
            "HyperliquidTrailingPerp initialized: market=%s, dir=%s, size=$%s, leverage=%sx, "
            "TP=%.1f%%, stop=%.1f%%, trail=%.1f%%@+%.1f%%, reenter=%s",
            self.market, LONG if self.is_long else SHORT, self.size_usd, self.leverage,
            self.take_profit_pct * 100, self.stop_loss_pct * 100,
            self.trail_pct * 100, self.trail_activation_pct * 100, self.reenter_after_close,
        )

    # ------------------------------------------------------------------ #
    # decide()
    # ------------------------------------------------------------------ #

    def decide(self, market: MarketSnapshot) -> Intent | None:
        if self.force_action:
            return self._forced_intent(market)

        # PENDING (submitted, fill unconfirmed): do NOT manage a position that may
        # not exist, and do NOT re-open one that may. HOLD until reconcile_fill()
        # resolves the submission (VIB-5597). Empty ≠ Zero: unconfirmed ≠ flat.
        if self._position_side is not None and not self._fill_confirmed:
            return Intent.hold(reason=f"{self._position_side} submitted, awaiting fill confirmation")

        if self._position_side is None:
            return self._enter(market)
        return self._manage(market)

    # ------------------------------------------------------------------ #
    # Entry / management
    # ------------------------------------------------------------------ #

    def _enter(self, market: MarketSnapshot) -> Intent:
        """FLAT: open one position in the configured direction.

        No EVM-wallet collateral gate: HyperCore margin is off-EVM, so the EVM
        balance is not the perp margin. The account must be funded on HyperCore
        (bridged USDC) out of band — see the README funding section.
        """
        try:
            entry_price = market.price(self.base_token)
        except _DATA_UNAVAILABLE_ERRORS as exc:
            return Intent.hold(reason=f"Price unavailable, cannot open: {exc}")

        # collateral_amount is informational for HyperCore (margin is off-EVM),
        # but the intent requires a positive value. Report the USD margin the
        # position implies (USDC ~= $1), so the intent reads honestly.
        collateral_amount = self.size_usd / self.leverage
        self._pending_entry_price = entry_price
        logger.info(
            "OPEN %s %s: size=$%s, entry~%.2f, leverage=%sx",
            LONG if self.is_long else SHORT, self.market, self.size_usd, entry_price, self.leverage,
        )
        return Intent.perp_open(
            market=self.market,
            collateral_token=self.collateral_token,
            collateral_amount=collateral_amount,
            size_usd=self.size_usd,
            is_long=self.is_long,
            leverage=self.leverage,
            max_slippage=self.max_slippage,
            protocol=_PROTOCOL,
        )

    def _manage(self, market: MarketSnapshot) -> Intent:
        """Hold the open position; close on take-profit, hard stop, or trailing stop."""
        side = self._position_side
        try:
            price = market.price(self.base_token)
        except _DATA_UNAVAILABLE_ERRORS as exc:
            return Intent.hold(reason=f"Price unavailable, holding {side}: {exc}")

        if self._entry_price is None or self._entry_price <= 0:
            # Fill price still settling — hold rather than evaluate against a
            # missing reference.
            return Intent.hold(reason=f"{side} open, awaiting entry price")

        # Directional PnL: long profits when price rises, short when it falls.
        raw = (price - self._entry_price) / self._entry_price
        pnl_pct = raw if side == LONG else -raw

        # Ratchet the high-water mark (peak favourable PnL) — this is what the
        # trailing stop trails behind.
        if pnl_pct > self._high_water_pnl:
            self._high_water_pnl = pnl_pct

        # 1) Take-profit — bank the win.
        if pnl_pct >= self.take_profit_pct:
            logger.info("TAKE-PROFIT %s: PnL %.2f%% >= %.1f%% — closing", side, pnl_pct * 100, self.take_profit_pct * 100)
            return self._close(side, reason="take_profit")

        # 2) Hard stop-loss (liq buffer) — reduce risk first.
        if pnl_pct <= -self.stop_loss_pct:
            logger.info("STOP-LOSS %s: PnL %.2f%% <= -%.1f%% — closing", side, pnl_pct * 100, self.stop_loss_pct * 100)
            return self._close(side, reason="stop_loss")

        # 3) Trailing stop — only once the trade has run far enough in our favour.
        if self._high_water_pnl >= self.trail_activation_pct:
            giveback = self._high_water_pnl - pnl_pct
            if giveback >= self.trail_pct:
                logger.info(
                    "TRAILING-STOP %s: gave back %.2f%% from peak %.2f%% (trail %.1f%%) — closing",
                    side, giveback * 100, self._high_water_pnl * 100, self.trail_pct * 100,
                )
                return self._close(side, reason="trailing_stop")

        return Intent.hold(
            reason=f"{side} open, PnL {pnl_pct * 100:.2f}%, peak {self._high_water_pnl * 100:.2f}%"
        )

    def _close(self, side: str, *, reason: str) -> Intent:
        """Reduce-only market close of the current side (full position)."""
        return Intent.perp_close(
            market=self.market,
            collateral_token=self.collateral_token,
            is_long=side == LONG,
            size_usd=None,  # None = close the FULL on-chain position (reduce-only)
            max_slippage=self.max_slippage,
            protocol=_PROTOCOL,
        )

    def _forced_intent(self, market: MarketSnapshot) -> Intent:
        """force_action hook for deterministic lifecycle testing."""
        if self.force_action in ("open", "open_long", "open_short"):
            # Honour an explicit direction override; otherwise use the configured side.
            if self.force_action == "open_long":
                is_long = True
            elif self.force_action == "open_short":
                is_long = False
            else:
                is_long = self.is_long
            try:
                self._pending_entry_price = market.price(self.base_token)
            except _DATA_UNAVAILABLE_ERRORS:
                self._pending_entry_price = None
            return Intent.perp_open(
                market=self.market,
                collateral_token=self.collateral_token,
                collateral_amount=self.size_usd / self.leverage,
                size_usd=self.size_usd,
                is_long=is_long,
                leverage=self.leverage,
                max_slippage=self.max_slippage,
                protocol=_PROTOCOL,
            )
        if self.force_action == "close":
            side = self._position_side or (LONG if self.is_long else SHORT)
            return self._close(side, reason="forced")
        return Intent.hold(reason=f"Unsupported force_action: {self.force_action}")

    # ------------------------------------------------------------------ #
    # Lifecycle hooks — the ONLY place position state is committed
    # ------------------------------------------------------------------ #

    def on_intent_executed(self, intent: Any, success: bool, result: Any) -> None:
        if not success:
            logger.warning("Intent failed; position state unchanged (side=%s)", self._position_side)
            return

        intent_type = getattr(intent, "intent_type", None)
        type_value = intent_type.value if hasattr(intent_type, "value") else str(intent_type)

        if type_value == "PERP_OPEN":
            # VIB-5597: submission success ≠ fill. Record the intended side and the
            # fallback entry price but mark the position PENDING (unconfirmed). It
            # is promoted to a live, managed position only when reconcile_fill()
            # observes the fill on HyperCore. Until then decide() HOLDs.
            self._position_side = LONG if getattr(intent, "is_long", True) else SHORT
            self._entry_price = self._resolve_fill_price(result) or self._pending_entry_price
            self._pending_entry_price = None
            self._high_water_pnl = Decimal("0")  # reset the trail for the new position
            self._fill_confirmed = False
            logger.info(
                "OPEN submitted (PENDING fill confirmation): side=%s, entry~%s", self._position_side, self._entry_price
            )

        elif type_value == "PERP_CLOSE":
            logger.info("CLOSE confirmed: was %s, now FLAT (reenter=%s)", self._position_side, self.reenter_after_close)
            self._position_side = None
            self._entry_price = None
            self._high_water_pnl = Decimal("0")
            self._fill_confirmed = False
            if not self.reenter_after_close:
                # One-shot lifecycle: latch to force_action=hold so decide() stops
                # re-opening after the single round-trip completes.
                self.force_action = "done"

    def reconcile_fill(self, intent_type: str, status: FillStatus) -> None:
        """Drive position state from the OBSERVED fill (VIB-5597 seam).

        Called after a PERP_OPEN submission once a fill signal has been read from
        HyperCore (the ``0x0800`` position read and/or orderStatus-by-cloid,
        combined by ``fill_reconciliation.reconcile_fill``). This is the seam that
        turns a PENDING submission into a live managed position — or clears a
        phantom one — so the strategy never manages a position that did not fill.

        The wiring that feeds this each tick (gateway orderStatus lookup + a
        post-execution position re-read) is a runner-level follow-up; the seam and
        its state transitions live here so the reference behaviour is correct and
        unit-testable now.

        Transitions (Empty ≠ Zero — UNMEASURED changes nothing):
          * FILLED / PARTIALLY_FILLED → promote PENDING → live managed position.
          * REJECTED → clear PENDING → FLAT (no phantom position to manage/close).
          * RESTING / UNMEASURED → stay PENDING (keep HOLDing until resolved).
        """
        if intent_type != "PERP_OPEN":
            return
        if self._position_side is None or self._fill_confirmed:
            return  # nothing pending to reconcile
        if status.is_confirmed_fill:
            self._fill_confirmed = True
            logger.info("FILL confirmed (%s): promoting %s to a live position", status, self._position_side)
        elif status.is_confirmed_reject:
            logger.warning("OPEN REJECTED (%s): clearing phantom %s position → FLAT", status, self._position_side)
            self._position_side = None
            self._entry_price = None
            self._high_water_pnl = Decimal("0")
            self._fill_confirmed = False
        else:
            logger.info("Fill still unconfirmed (%s) for %s — staying PENDING", status, self._position_side)

    @staticmethod
    def _resolve_fill_price(result: Any) -> Decimal | None:
        """Pull the executed entry price from the result, if present (else None)."""
        if result is None:
            return None
        fill = getattr(result, "entry_price", None)
        if fill is None:
            extracted = getattr(result, "extracted_data", None) or {}
            fill = extracted.get("entry_price") if isinstance(extracted, dict) else None
        try:
            return Decimal(str(fill)) if fill is not None else None
        except (ValueError, TypeError, ArithmeticError):
            # Decimal(str(fill)) raises decimal.InvalidOperation (an
            # ArithmeticError, NOT a ValueError) on a non-numeric string like
            # "None"/"abc" — catch it so a garbage fill degrades to None rather
            # than crashing the strategy.
            return None

    # ------------------------------------------------------------------ #
    # State persistence
    # ------------------------------------------------------------------ #

    def get_persistent_state(self) -> dict[str, Any]:
        return {
            "position_side": self._position_side,
            "entry_price": str(self._entry_price) if self._entry_price is not None else None,
            "high_water_pnl": str(self._high_water_pnl),
            "fill_confirmed": self._fill_confirmed,
        }

    def load_persistent_state(self, state: dict[str, Any]) -> None:
        if not state:
            return
        self._position_side = state.get("position_side")
        ep = state.get("entry_price")
        self._entry_price = Decimal(str(ep)) if ep is not None else None
        hw = state.get("high_water_pnl")
        self._high_water_pnl = Decimal(str(hw)) if hw is not None else Decimal("0")
        # A restart with a side but no persisted confirmation flag (legacy row, or
        # a crash mid-open) is treated as PENDING — fail-safe: a fill must be
        # re-observed before the position is managed (Empty ≠ Zero).
        self._fill_confirmed = bool(state.get("fill_confirmed", False)) if self._position_side is not None else False

    def get_status(self) -> dict[str, Any]:
        if self._position_side is None:
            lifecycle = "flat"
        elif not self._fill_confirmed:
            lifecycle = "pending"  # submitted, fill unconfirmed (VIB-5597)
        else:
            lifecycle = self._position_side
        return {
            "strategy": "hyperliquid_trailing_perp",
            "chain": self.chain,
            "market": self.market,
            "direction": LONG if self.is_long else SHORT,
            "position_side": lifecycle,
            "fill_confirmed": self._fill_confirmed,
            "entry_price": str(self._entry_price) if self._entry_price is not None else None,
            "high_water_pnl_pct": str(self._high_water_pnl * 100),
        }

    # ------------------------------------------------------------------ #
    # Teardown
    # ------------------------------------------------------------------ #

    def supports_teardown(self) -> bool:
        return True

    def get_open_positions(self) -> "TeardownPositionSummary":
        from almanak.framework.teardown import PositionInfo, PositionType, TeardownPositionSummary

        positions: list[PositionInfo] = []
        # A PENDING (unconfirmed) submission is STILL surfaced for teardown: the
        # order may have filled on HyperCore even though we have not yet observed
        # it, so teardown must fail-closed and reduce that possible risk (a
        # reduce-only close is a safe no-op if nothing filled). VIB-5597.
        if self._position_side is not None:
            positions.append(
                PositionInfo(
                    position_type=PositionType.PERP,
                    position_id=f"hyperliquid-{self.market}-{self._position_side}",
                    chain=self.chain,
                    protocol=_PROTOCOL,
                    value_usd=Decimal("0"),
                    details={
                        "market": self.market,
                        "side": self._position_side,
                        "size_usd": str(self.size_usd),
                        "fill_confirmed": self._fill_confirmed,
                    },
                )
            )
        return TeardownPositionSummary(
            deployment_id=getattr(self, "deployment_id", "hyperliquid_trailing_perp"),
            timestamp=datetime.now(UTC),
            positions=positions,
        )

    def generate_teardown_intents(self, mode: "TeardownMode", market: MarketSnapshot | None = None) -> list[Intent]:
        from almanak.framework.teardown import TeardownMode

        if self._position_side is None:
            return []
        slippage = max(self.max_slippage, Decimal("0.02")) if mode == TeardownMode.HARD else self.max_slippage
        return [
            Intent.perp_close(
                market=self.market,
                collateral_token=self.collateral_token,
                is_long=self._position_side == LONG,
                size_usd=None,  # full reduce-only close
                max_slippage=slippage,
                protocol=_PROTOCOL,
            )
        ]
