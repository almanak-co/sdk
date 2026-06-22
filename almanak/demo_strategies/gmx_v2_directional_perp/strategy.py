"""GMX V2 Directional Perp — close-before-reverse, funding-gated, liq-buffered.

A directional perpetual-futures strategy on GMX V2 (Arbitrum) that goes long or
short an EMA-crossover signal. It exists as the reference for the three things a
directional perp MUST get right and that ad-hoc implementations routinely get
wrong:

  1. CLOSE-BEFORE-REVERSE. When the signal flips, the existing position is
     CLOSED first; the opposite side is only opened on a later tick once the
     close has confirmed. The strategy never opens an opposite position while
     one is still live — that is the "stranded leg" bug (an unhedged, doubled
     position) this seed is built to demonstrate the fix for.

  2. FUNDING-RATE GATE. Entries are gated on the funding rate so the strategy
     does not open into adverse funding, and an open position is closed if
     funding turns strongly against it.

  3. LIQUIDATION BUFFER. A stop-loss on fill-price PnL closes the position well
     before the liquidation price. `stop_loss_pct` must stay below the
     liquidation distance (~1/leverage); __init__ warns if it does not.

Design rules honoured (the golden promotion gate):
  - State (`_position_side`, `_entry_price`) is committed ONLY in
    on_intent_executed, after a fill confirms — never speculatively in decide().
  - PnL is measured against the FILL price, not the signal-time price.
  - Data-unavailable reads degrade to HOLD; any other exception propagates.
  - No direct network egress — all data via MarketSnapshot / the gateway.

State machine:
    FLAT --(signal long, funding ok)--> LONG
    FLAT --(signal short, funding ok)--> SHORT
    LONG --(stop-loss | adverse funding | signal flips short)--> close --> FLAT
    SHORT --(stop-loss | adverse funding | signal flips long)--> close --> FLAT

Usage:
    almanak strat run -d almanak/demo_strategies/gmx_v2_directional_perp --network anvil --interval 5
"""

import logging
from datetime import UTC, datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Any

from almanak.framework.data import BalanceUnavailableError, MarketSnapshotError, PriceUnavailableError
from almanak.framework.intents import Intent
from almanak.framework.market import MarketSnapshot
from almanak.framework.strategies import IntentStrategy, almanak_strategy

if TYPE_CHECKING:
    from almanak.framework.teardown import TeardownMode, TeardownPositionSummary

logger = logging.getLogger(__name__)

# Indicator / balance reads that mean "data unavailable" -> HOLD. Everything
# else propagates so a real bug is never masked behind a blanket except.
_DATA_UNAVAILABLE_ERRORS = (
    PriceUnavailableError,
    BalanceUnavailableError,
    MarketSnapshotError,
    ValueError,
)

LONG = "long"
SHORT = "short"


@almanak_strategy(
    name="gmx_v2_directional_perp",
    description="GMX V2 directional perp: EMA-crossover with close-before-reverse, funding gate, and a stop-loss liq buffer",
    version="1.0.0",
    author="Almanak",
    tags=["perp", "gmx-v2", "directional", "ema", "funding", "arbitrum"],
    supported_chains=["arbitrum"],
    default_chain="arbitrum",
    supported_protocols=["gmx_v2"],
    intent_types=["PERP_OPEN", "PERP_CLOSE", "HOLD"],
    quote_asset="USD",
)
class GmxV2DirectionalPerp(IntentStrategy):
    """Directional GMX V2 perp with safe reversal, funding gating, and a stop-loss."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)

        self.market = str(self.get_config("market", "ETH/USD"))
        self.funding_market = str(self.get_config("funding_market", "ETH-USD"))
        self.base_token = str(self.get_config("base_token", "ETH"))
        self.collateral_token = str(self.get_config("collateral_token", "USDC"))

        self.position_size_usd = Decimal(str(self.get_config("position_size_usd", "100")))
        self.leverage = Decimal(str(self.get_config("leverage", "2.0")))
        self.min_collateral_usd = Decimal(str(self.get_config("min_collateral_usd", "20")))
        self.max_slippage = Decimal(str(self.get_config("max_slippage", "0.01")))

        self.ema_fast_period = int(self.get_config("ema_fast_period", 9))
        self.ema_slow_period = int(self.get_config("ema_slow_period", 21))

        # Funding (per-hour). Positive funding = longs pay shorts.
        self.funding_entry_threshold = Decimal(str(self.get_config("funding_entry_threshold_hourly", "0.0005")))
        self.funding_exit_threshold = Decimal(str(self.get_config("funding_exit_threshold_hourly", "0.0015")))

        # Liquidation buffer: stop-loss on fill-price PnL.
        self.stop_loss_pct = Decimal(str(self.get_config("stop_loss_pct", "0.10")))

        # Fail-fast config validation. A golden seed should reject nonsensical
        # parameters at construction (prevents divide-by-zero / inverted signals
        # downstream) rather than emitting a malformed perp intent at runtime.
        if self.position_size_usd <= 0:
            raise ValueError("position_size_usd must be > 0")
        if self.leverage <= 0:
            raise ValueError("leverage must be > 0")
        if self.min_collateral_usd <= 0:
            raise ValueError("min_collateral_usd must be > 0")
        if not 0 <= self.max_slippage <= 1:
            raise ValueError("max_slippage must be between 0 and 1")
        if self.ema_fast_period <= 0 or self.ema_slow_period <= 0:
            raise ValueError("EMA periods must be > 0")
        if self.ema_fast_period >= self.ema_slow_period:
            raise ValueError("ema_fast_period must be < ema_slow_period")
        if self.funding_entry_threshold >= self.funding_exit_threshold:
            raise ValueError("funding_entry_threshold must be < funding_exit_threshold")
        if not 0 < self.stop_loss_pct < 1:
            raise ValueError("stop_loss_pct must be between 0 and 1 (exclusive)")

        self.force_action = str(self.get_config("force_action", "") or "").strip().lower()

        # State — committed only in on_intent_executed.
        self._position_side: str | None = None
        self._entry_price: Decimal | None = None
        # Decide-time price, used as the entry-price fallback for the GMX
        # two-step flow if the result does not carry a fill price.
        self._pending_entry_price: Decimal | None = None

        # Liquidation distance ~ 1/leverage; the stop must sit inside it.
        liq_distance = Decimal("1") / self.leverage if self.leverage > 0 else Decimal("1")
        if self.stop_loss_pct >= liq_distance:
            logger.warning(
                "stop_loss_pct %.2f >= liquidation distance ~%.2f (1/leverage): the stop offers no "
                "buffer before liquidation. Lower stop_loss_pct or leverage.",
                self.stop_loss_pct, liq_distance,
            )

        logger.info(
            "GmxV2DirectionalPerp initialized: market=%s, size=$%s, leverage=%sx, "
            "EMA(%d/%d), stop_loss=%.0f%%, funding_entry=%s/h",
            self.market, self.position_size_usd, self.leverage,
            self.ema_fast_period, self.ema_slow_period, self.stop_loss_pct * 100,
            self.funding_entry_threshold,
        )

    # ------------------------------------------------------------------ #
    # decide()
    # ------------------------------------------------------------------ #

    def decide(self, market: MarketSnapshot) -> Intent | None:
        if self.force_action:
            return self._forced_intent(market)

        try:
            ema_fast = market.ema(self.base_token, period=self.ema_fast_period).value
            ema_slow = market.ema(self.base_token, period=self.ema_slow_period).value
        except _DATA_UNAVAILABLE_ERRORS as exc:
            return Intent.hold(reason=f"EMA data unavailable: {exc}")

        signal = LONG if ema_fast > ema_slow else SHORT
        funding = self._funding_hourly(market)

        if self._position_side is None:
            return self._enter(market, signal, funding)
        if self._position_side == LONG:
            return self._manage(market, side=LONG, signal=signal, funding=funding)
        return self._manage(market, side=SHORT, signal=signal, funding=funding)

    # ------------------------------------------------------------------ #
    # Entry / management
    # ------------------------------------------------------------------ #

    def _enter(self, market: MarketSnapshot, signal: str, funding: Decimal | None) -> Intent:
        """FLAT: open in the signal direction if funding is acceptable."""
        # Funding gate: a long pays funding when rate > 0; a short pays when
        # rate < 0. Refuse to open into funding worse than the entry threshold.
        if funding is None:
            return Intent.hold(reason="Funding rate unavailable — refusing to open blind")
        if signal == LONG and funding > self.funding_entry_threshold:
            return Intent.hold(
                reason=f"Funding {funding:.6f}/h > entry threshold {self.funding_entry_threshold} — long would pay too much"
            )
        if signal == SHORT and funding < -self.funding_entry_threshold:
            return Intent.hold(
                reason=f"Funding {funding:.6f}/h < -{self.funding_entry_threshold} — short would pay too much"
            )

        # Required margin (USD) = notional / leverage. Compute up front (it needs
        # no price) so the balance gate checks the ACTUAL margin the open needs,
        # not just the static minimum. min_collateral_usd stays a position-size
        # floor: too-small a margin isn't worth opening.
        collateral_usd = self.position_size_usd / self.leverage
        if collateral_usd < self.min_collateral_usd:
            return Intent.hold(
                reason=f"Required margin ${collateral_usd:.2f} below min ${self.min_collateral_usd}"
            )

        try:
            collateral = market.balance(self.collateral_token)
        except _DATA_UNAVAILABLE_ERRORS as exc:
            return Intent.hold(reason=f"Balance unavailable: {exc}")
        if collateral.balance_usd < collateral_usd:
            return Intent.hold(
                reason=f"Insufficient {self.collateral_token}: ${collateral.balance_usd:.2f} "
                f"< required margin ${collateral_usd:.2f}"
            )

        try:
            entry_price = market.price(self.base_token)
            collateral_price = market.price(self.collateral_token)
        except _DATA_UNAVAILABLE_ERRORS as exc:
            return Intent.hold(reason=f"Price unavailable: {exc}")

        # collateral_amount is in COLLATERAL-TOKEN units, not USD. Sizing the
        # margin in USD and passing it straight to perp_open would deposit that
        # many tokens (e.g. 50 ETH instead of $50 of ETH) for any non-stablecoin
        # collateral. Convert: USD margin / collateral price.
        collateral_amount = collateral_usd / collateral_price
        is_long = signal == LONG
        # Captured for the entry-price fallback; committed to _entry_price only
        # on a confirmed fill (in on_intent_executed).
        self._pending_entry_price = entry_price
        logger.info(
            "OPEN %s %s: size=$%s, collateral=%s %s, entry~%.2f, funding=%s/h",
            signal.upper(), self.market, self.position_size_usd, collateral_amount,
            self.collateral_token, entry_price, funding,
        )
        return Intent.perp_open(
            market=self.market,
            collateral_token=self.collateral_token,
            collateral_amount=collateral_amount,
            size_usd=self.position_size_usd,
            is_long=is_long,
            leverage=self.leverage,
            max_slippage=self.max_slippage,
            protocol="gmx_v2",
        )

    def _manage(self, market: MarketSnapshot, *, side: str, signal: str, funding: Decimal | None) -> Intent:
        """Hold an open position; close on stop-loss, adverse funding, or a flip.

        Closing always emits a single PERP_CLOSE for the CURRENT side. The
        opposite side is opened only after this close confirms and the next
        decide() runs FLAT — never by opening an opposite leg here.
        """
        try:
            price = market.price(self.base_token)
        except _DATA_UNAVAILABLE_ERRORS as exc:
            return Intent.hold(reason=f"Price unavailable, holding {side}: {exc}")

        if self._entry_price is None or self._entry_price <= 0:
            # Entry price not yet known (e.g. fill price still settling). Hold
            # rather than evaluate a stop against a missing reference.
            return Intent.hold(reason=f"{side} open, awaiting entry price")

        # Directional PnL: long profits when price rises, short when it falls.
        raw = (price - self._entry_price) / self._entry_price
        pnl_pct = raw if side == LONG else -raw

        # 1) Stop-loss (liq buffer) — highest priority, always reduce risk first.
        if pnl_pct <= -self.stop_loss_pct:
            logger.info("STOP-LOSS %s: PnL %.2f%% <= -%.0f%% — closing", side, pnl_pct * 100, self.stop_loss_pct * 100)
            return self._close(side, reason="stop_loss")

        # 2) Adverse-funding exit: a long paying more than the exit threshold, or
        #    a short paying more than it (funding strongly negative), bleeds the
        #    carry — close.
        if funding is not None:
            if side == LONG and funding > self.funding_exit_threshold:
                logger.info("FUNDING EXIT long: %s/h > %s — closing", funding, self.funding_exit_threshold)
                return self._close(side, reason="adverse_funding")
            if side == SHORT and funding < -self.funding_exit_threshold:
                logger.info("FUNDING EXIT short: %s/h < -%s — closing", funding, self.funding_exit_threshold)
                return self._close(side, reason="adverse_funding")

        # 3) Signal flip -> CLOSE-BEFORE-REVERSE. Close the current side now; the
        #    next FLAT tick opens the opposite side.
        if signal != side:
            logger.info("REVERSE %s -> %s: closing %s first (open opposite next tick)", side, signal, side)
            return self._close(side, reason="reverse")

        return Intent.hold(
            reason=f"{side} open, PnL {pnl_pct * 100:.2f}%, funding={funding}/h"
        )

    def _close(self, side: str, *, reason: str) -> Intent:
        return Intent.perp_close(
            market=self.market,
            collateral_token=self.collateral_token,
            is_long=side == LONG,
            size_usd=self.position_size_usd,
            max_slippage=self.max_slippage,
            protocol="gmx_v2",
        )

    def _funding_hourly(self, market: MarketSnapshot) -> Decimal | None:
        """Current hourly funding rate, or None if unavailable (never fabricated)."""
        try:
            return Decimal(str(market.funding_rate("gmx_v2", self.funding_market).rate_hourly))
        except Exception as exc:  # noqa: BLE001 — funding is advisory; absence must not crash decide()
            logger.warning("Funding rate unavailable for %s: %s", self.funding_market, exc)
            return None

    def _forced_intent(self, market: MarketSnapshot) -> Intent:
        """force_action hook for deterministic lifecycle testing."""
        if self.force_action in ("open_long", "open_short"):
            is_long = self.force_action == "open_long"
            # Capture the decide-time price as the entry-price fallback, exactly
            # as _enter() does, so a forced open also commits a sensible entry on
            # fill (the result's fill price still takes precedence).
            try:
                self._pending_entry_price = market.price(self.base_token)
                collateral_price = market.price(self.collateral_token)
            except _DATA_UNAVAILABLE_ERRORS:
                self._pending_entry_price = None
                collateral_price = Decimal("1")  # forced-test fallback (assumes stable collateral)
            # Collateral in token units, not USD (see _enter for the rationale).
            collateral_amount = (self.position_size_usd / self.leverage) / collateral_price
            return Intent.perp_open(
                market=self.market,
                collateral_token=self.collateral_token,
                collateral_amount=collateral_amount,
                size_usd=self.position_size_usd,
                is_long=is_long,
                leverage=self.leverage,
                max_slippage=self.max_slippage,
                protocol="gmx_v2",
            )
        if self.force_action == "close":
            # Close whichever side is live (default long if state is unknown).
            side = self._position_side or LONG
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
            self._position_side = LONG if getattr(intent, "is_long", True) else SHORT
            self._entry_price = self._resolve_fill_price(result) or self._pending_entry_price
            self._pending_entry_price = None
            logger.info("OPEN confirmed: side=%s, entry=%s", self._position_side, self._entry_price)

        elif type_value == "PERP_CLOSE":
            logger.info("CLOSE confirmed: was %s, now FLAT", self._position_side)
            self._position_side = None
            self._entry_price = None

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
        except (ValueError, TypeError):
            return None

    # ------------------------------------------------------------------ #
    # State persistence
    # ------------------------------------------------------------------ #

    def get_persistent_state(self) -> dict[str, Any]:
        return {
            "position_side": self._position_side,
            "entry_price": str(self._entry_price) if self._entry_price is not None else None,
        }

    def load_persistent_state(self, state: dict[str, Any]) -> None:
        if not state:
            return
        self._position_side = state.get("position_side")
        ep = state.get("entry_price")
        self._entry_price = Decimal(str(ep)) if ep is not None else None

    def get_status(self) -> dict[str, Any]:
        return {
            "strategy": "gmx_v2_directional_perp",
            "chain": self.chain,
            "market": self.market,
            "position_side": self._position_side or "flat",
            "entry_price": str(self._entry_price) if self._entry_price is not None else None,
        }

    # ------------------------------------------------------------------ #
    # Teardown
    # ------------------------------------------------------------------ #

    def supports_teardown(self) -> bool:
        return True

    def get_open_positions(self) -> "TeardownPositionSummary":
        from almanak.framework.teardown import PositionInfo, PositionType, TeardownPositionSummary

        positions: list[PositionInfo] = []
        if self._position_side is not None:
            positions.append(
                PositionInfo(
                    position_type=PositionType.PERP,
                    position_id=f"gmx-v2-{self.market}-{self._position_side}",
                    chain=self.chain,
                    protocol="gmx_v2",
                    value_usd=Decimal("0"),
                    details={
                        "market": self.market,
                        "side": self._position_side,
                        "size_usd": str(self.position_size_usd),
                    },
                )
            )
        return TeardownPositionSummary(
            deployment_id=getattr(self, "deployment_id", "gmx_v2_directional_perp"),
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
                size_usd=self.position_size_usd,
                max_slippage=slippage,
                protocol="gmx_v2",
            )
        ]
