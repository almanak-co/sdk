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

from almanak.demo_strategies._address_config import require_evm_address
from almanak.framework.data import BalanceUnavailableError, MarketSnapshotError, PriceUnavailableError
from almanak.framework.intents import AnyIntent, Intent, IntentType
from almanak.framework.market import MarketSnapshot
from almanak.framework.strategies import IntentStrategy, almanak_strategy

if TYPE_CHECKING:
    from almanak.framework.strategies import PerpProbe
    from almanak.framework.teardown import PositionInfo, TeardownMode, TeardownPositionSummary

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
    intent_types=[IntentType.PERP_OPEN, IntentType.PERP_CLOSE, IntentType.HOLD],
    quote_asset="USD",
)
class GmxV2DirectionalPerp(IntentStrategy):
    """Directional GMX V2 perp with safe reversal, funding gating, and a stop-loss."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)

        self.market = str(self.get_config("market", "ETH/USD"))
        # Address-first contract: the author declares the GMX market-token
        # ADDRESS; the SDK verifies it (dynamic registry, VIB-6561) but never
        # maps a symbol on the author's behalf. The label above remains
        # display/funding vocabulary only.
        self.market_address = require_evm_address(self, "market_address")
        self.base_token = str(self.get_config("base_token", "ETH"))
        self.base_token_address = require_evm_address(self, "base_token_address")
        self.collateral_token = str(self.get_config("collateral_token", "USDC"))
        self.collateral_token_address = require_evm_address(self, "collateral_token_address")

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
        # True while a submitted open/close has no verdict yet. Deliberately
        # NOT persisted: after a restart the callback is lost, and a latched
        # True would brick decide() forever — resetting re-derives intent from
        # _position_side (same doctrine as the forced-open latch, VIB-6527).
        self._awaiting_fill = False
        self._verified_index_symbol: str | None = None

        # Liquidation distance ~ 1/leverage; the stop must sit inside it.
        liq_distance = Decimal("1") / self.leverage if self.leverage > 0 else Decimal("1")
        if self.stop_loss_pct >= liq_distance:
            logger.warning(
                "stop_loss_pct %.2f >= liquidation distance ~%.2f (1/leverage): the stop offers no "
                "buffer before liquidation. Lower stop_loss_pct or leverage.",
                self.stop_loss_pct,
                liq_distance,
            )

        logger.info(
            "GmxV2DirectionalPerp initialized: market=%s, size=$%s, leverage=%sx, "
            "EMA(%d/%d), stop_loss=%.0f%%, funding_entry=%s/h",
            self.market,
            self.position_size_usd,
            self.leverage,
            self.ema_fast_period,
            self.ema_slow_period,
            self.stop_loss_pct * 100,
            self.funding_entry_threshold,
        )

    def _get_tracked_tokens(self) -> list[str]:
        """Track wallet assets, excluding the GMX index identifier.

        GMX synthetic indexes use address-shaped identifiers that have no
        deployed ERC-20 contract. They are valid for venue position matching
        and market-data reads, but balance discovery must never treat them as
        wallet tokens. A perp wallet holds collateral, not the index asset.
        """
        return [self.collateral_token_address]

    def _index_symbol(self, market: MarketSnapshot) -> str:
        """Return the index symbol bound to the verified execution market."""
        if self._verified_index_symbol is None:
            try:
                metadata = market.perp_market("gmx_v2", self.market_address)
            except MarketSnapshotError:
                return self.base_token_address  # gateway-less paper: preserve exact-address behavior
            self._verified_index_symbol = metadata.index_symbol
        return self._verified_index_symbol

    # ------------------------------------------------------------------ #
    # decide()
    # ------------------------------------------------------------------ #

    def decide(self, market: MarketSnapshot) -> Intent | None:
        if self._awaiting_fill:
            # A submitted open/close has no verdict yet (delayed execution in
            # backtests; the keeper window live). Deciding again while FLAT
            # would stack a second open — observed as a doubled $200 exposure
            # whose second fill wiped the entry-price reference. Gated ahead
            # of EVERY submitting branch, forced actions included: the forced
            # latch keys on _position_side, which is only committed on the
            # fill verdict and cannot see an in-flight submission.
            return Intent.hold(reason="submitted intent awaiting confirmation")

        if self.force_action:
            return self._forced_intent(market)

        try:
            index_symbol = self._index_symbol(market)
            ema_fast = market.ema(index_symbol, period=self.ema_fast_period).value
            ema_slow = market.ema(index_symbol, period=self.ema_slow_period).value
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
            return Intent.hold(reason=f"Required margin ${collateral_usd:.2f} below min ${self.min_collateral_usd}")

        try:
            collateral = market.balance(self.collateral_token_address)
        except _DATA_UNAVAILABLE_ERRORS as exc:
            return Intent.hold(reason=f"Balance unavailable: {exc}")
        if collateral.balance_usd < collateral_usd:
            return Intent.hold(
                reason=f"Insufficient {self.collateral_token}: ${collateral.balance_usd:.2f} "
                f"< required margin ${collateral_usd:.2f}"
            )

        try:
            # Synthetic GMX indexes have no ERC-20 contract. Market data is
            # keyed by the verified index symbol; the address-shaped index
            # identifier is reserved for exact venue position matching.
            entry_price = market.price(self._index_symbol(market))
            collateral_price = market.price(self.collateral_token_address)
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
        self._awaiting_fill = True
        logger.info(
            "OPEN %s %s: size=$%s, collateral=%s %s, entry~%.2f, funding=%s/h",
            signal.upper(),
            self.market,
            self.position_size_usd,
            collateral_amount,
            self.collateral_token,
            entry_price,
            funding,
        )
        return Intent.perp_open(
            market=self.market_address,
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
            price = market.price(self._index_symbol(market))
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

        return Intent.hold(reason=f"{side} open, PnL {pnl_pct * 100:.2f}%, funding={funding}/h")

    def _close(self, side: str, *, reason: str) -> Intent:
        self._awaiting_fill = True
        return Intent.perp_close(
            market=self.market_address,
            collateral_token=self.collateral_token,
            is_long=side == LONG,
            size_usd=None,  # None = close the FULL on-chain position (never a cached notional — VIB-5950/ALM-2976)
            max_slippage=self.max_slippage,
            protocol="gmx_v2",
        )

    def _funding_hourly(self, market: MarketSnapshot) -> Decimal | None:
        """Current hourly funding rate, or None if unavailable (never fabricated).

        Queried by market ADDRESS: GMX funding factors are per-market, so the
        gateway serves live funding only for an unambiguous market — a pair
        label with several collateral variants falls back to the default rate
        (PR #3648). The declared address is the unambiguous spelling.
        """
        try:
            return Decimal(str(market.funding_rate("gmx_v2", self.market_address).rate_hourly))
        except Exception as exc:  # noqa: BLE001 — funding is advisory; absence must not crash decide()
            logger.warning("Funding rate unavailable for %s: %s", self.market, exc)
            return None

    def _forced_intent(self, market: MarketSnapshot) -> Intent:
        """force_action hook for deterministic lifecycle testing.

        The forced open LATCHES (VIB-5513): "open_long"/"open_short" means
        "ensure one open, then hold" — never "open again every tick". Without
        the latch a continuous runner re-attempts the open each iteration, and
        once the collateral is spent every retry reverts
        ``ERC20: transfer amount exceeds balance`` (~34 reverts observed on
        mainnet). ``gmx_perp_lifecycle`` is the reference behaviour
        ("Force action 'open' already executed" -> HOLD).

        What the latch keys on, and why (the polarity is inverted vs teardown):

        * PRIMARY: ``_position_side`` — this strategy's own persisted execution
          state, committed in ``on_intent_executed`` when the create-order
          transaction confirms. It is the only signal that stays truthful during
          GMX's asynchronous keeper window, where the venue HONESTLY reads FLAT
          after a successful submission — a venue-probe-gated open would re-open
          (stack) inside exactly that window.
        * BELT: the venue probe, consulted only when the cache is clear and only
          for its POSITIVE answer. ``OPEN`` is sound evidence in any state of
          the world ("presence is never weakened"), and holds when a wiped
          state DB meets a live venue position. ``FLAT`` and ``UNMEASURED``
          both fall through to the open: FLAT because it cannot distinguish
          "never opened" from "submitted, keeper pending" (the cache already
          answered that), and UNMEASURED because refusing to open on a flaky
          read would let the probe brick the strategy's one job — a fail-closed
          open gate that refuses 100% of the time is indistinguishable from the
          latch this fixes.

        Known limit (VIB-6527): the latch keys on the USER-FACING callback
        verdict, which can diverge from chain truth in both directions — a
        landed order reported ``success=False`` re-arms the retry, and a
        keeper-cancelled fill after ``success=True`` leaves the latch holding
        on a flat venue. Both directions are pre-existing, shared with
        ``gmx_perp_lifecycle``, and tracked there.
        """
        if self.force_action in ("open_long", "open_short"):
            if self._position_side is not None:
                return Intent.hold(
                    reason=f"Force action '{self.force_action}' already executed (side={self._position_side})"
                )
            probe = self._venue_probe(market)
            if probe.is_open:
                return Intent.hold(
                    reason=f"Force action '{self.force_action}': venue already holds a position — holding"
                )
            is_long = self.force_action == "open_long"
            # Capture the decide-time price as the entry-price fallback, exactly
            # as _enter() does, so a forced open also commits a sensible entry on
            # fill (the result's fill price still takes precedence).
            try:
                self._pending_entry_price = market.price(self._index_symbol(market))
                collateral_price = market.price(self.collateral_token_address)
            except _DATA_UNAVAILABLE_ERRORS:
                self._pending_entry_price = None
                collateral_price = Decimal("1")  # forced-test fallback (assumes stable collateral)
            # Collateral in token units, not USD (see _enter for the rationale).
            collateral_amount = (self.position_size_usd / self.leverage) / collateral_price
            self._awaiting_fill = True
            return Intent.perp_open(
                market=self.market_address,
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
        # The submitted intent has a verdict either way — decide() may act
        # again. Cleared before any early return so a failed submission never
        # leaves the strategy latched.
        self._awaiting_fill = False
        if not success:
            logger.warning("Intent failed; position state unchanged (side=%s)", self._position_side)
            return

        intent_type = getattr(intent, "intent_type", None)
        type_value = intent_type.value if hasattr(intent_type, "value") else str(intent_type)

        if type_value == "PERP_OPEN":
            self._position_side = LONG if getattr(intent, "is_long", True) else SHORT
            fill_price = self._resolve_fill_price(result) or self._pending_entry_price
            if fill_price is not None:
                # Never wipe a known entry reference with None: a duplicate
                # fill that carries no price must not blind the stop-loss.
                self._entry_price = fill_price
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
        if fill is None:
            # Simulated results (backtest) carry the executed price on the
            # trade record — the slippage-adjusted fill, exactly the entry
            # reference the stop-loss should measure against.
            trade_record = getattr(result, "trade_record", None)
            fill = getattr(trade_record, "executed_price", None)
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

    def _venue_probe(self, market: MarketSnapshot | None = None) -> "PerpProbe":
        """Ask GMX what it actually holds on this market (ALM-3109 / VIB-6159).

        ``_position_side`` records what this strategy *requested*: it is written in
        ``on_intent_executed`` when the create-order transaction succeeds, but the
        GMX keeper fill is asynchronous and can revert or be cancelled afterwards.
        Teardown enumerates from the answer below, so trusting the cache means a
        position the cache never learned about is never closed.

        Returns a three-valued probe. ``UNMEASURED`` is NOT flat — see the module
        docstring of ``perp_position_probe``.
        """
        from almanak.framework.strategies import probe_perp_position

        snapshot = market
        if snapshot is None:
            try:
                snapshot = self.create_market_snapshot()
            except Exception as exc:  # noqa: BLE001 — no snapshot ⇒ UNMEASURED, never flat
                logger.warning("teardown: no market snapshot for the venue probe (%s)", exc)
                snapshot = None
        index_symbol = self._index_symbol(snapshot) if snapshot is not None else self.base_token_address
        return probe_perp_position(
            snapshot,
            protocol="gmx_v2",
            chain=self.chain,
            # Address-first: the venue keys GMX positions by market address, so
            # the address is the exact-match probe key (the label would need
            # catalog metadata to resolve).
            market_symbol=self.market_address,
            # Dynamic GMX indexes can be synthetic identifiers, not ERC-20s.
            # Price only a venue-verified symbol; an unresolved identity leaves
            # notional unmeasured instead of probing a non-contract address.
            index_token_symbol=index_symbol if not index_symbol.startswith("0x") else None,
        )

    def _position_row(self, *, side: str, value_usd: Decimal, measured: bool) -> "PositionInfo":
        from almanak.framework.teardown import PositionInfo, PositionType

        details: dict[str, Any] = {
            # ``collateral_token`` is REQUIRED for this row to name its
            # position (VIB-6316). ``gmx_v2_perp_identity`` derives the
            # venue key from market + collateral + side, resolving each
            # symbol through the connector catalogue; without collateral
            # it derives nothing, the row falls through to its raw
            # ``position_id``, and the SAME physical position enumerates
            # twice — once here and once as the registry's bytes32 key.
            # Measured on mainnet before this was added: a single ETH/USD
            # long reported positions_total=2, positions_closed=2.
            # ``generate_teardown_intents`` below already supplies it, so
            # omitting it here was an asymmetry between the two halves of
            # one strategy, not a deliberate shape.
            "market": self.market_address,
            "market_address": self.market_address,
            "collateral_token": self.collateral_token,
            "side": side,
            "size_usd": str(value_usd),
            "position_source": "venue" if measured else "strategy_cache_unverified",
        }
        if not measured:
            # Empty ≠ Zero: the number is the size we ASKED for, not a measurement
            # of what the venue holds. Mark it so the valuer and the teardown CLI
            # preview treat it as unmeasured rather than as a priced position.
            details["value_usd_unknown"] = True
            details["valuation_status"] = "no_path"
        return PositionInfo(
            position_type=PositionType.PERP,
            position_id=f"gmx-v2-{self.market}-{side}",
            chain=self.chain,
            protocol="gmx_v2",
            value_usd=value_usd,
            details=details,
        )

    def get_open_positions(self) -> "TeardownPositionSummary":
        """Report what the VENUE holds, falling back to cache only when unmeasured.

        Three outcomes, and the third is the one that matters:

        * venue OPEN → report the venue's own side and mark-valued notional, even
          if ``_position_side`` is ``None`` (the divergence in ALM-3109 / VIB-6497).
        * venue FLAT → report nothing. A stale cache no longer publishes a phantom
          residual that fails the teardown verdict on a flat account.
        * venue UNMEASURED → keep the cached row, marked unverified. Reporting
          empty here is the false certification in VIB-6497.

        A real notional matters as much as the row: ``value_usd=0`` is dropped as
        dust (≤ $0.01) by the harness's post-teardown residual measurement, so a
        truthful row valued at zero is invisible to exactly the check that should
        catch it.
        """
        from almanak.framework.teardown import PositionInfo, TeardownPositionSummary

        probe = self._venue_probe()
        positions: list[PositionInfo] = []
        if probe.is_open:
            for found in probe.positions:
                positions.append(
                    self._position_row(
                        side=LONG if found.is_long else SHORT,
                        # Unpriceable notional degrades to the requested size WITH
                        # the unmeasured markers — never a measured $0.
                        value_usd=(found.notional_usd if found.notional_usd is not None else self.position_size_usd),
                        measured=found.notional_usd is not None,
                    )
                )
        elif not probe.is_measured:
            logger.warning(
                "teardown: GMX position read UNMEASURED (%s) — falling back to cached side=%s. "
                "An unmeasured read is not a flat account.",
                probe.reason,
                self._position_side,
            )
            if self._position_side is not None:
                positions.append(
                    self._position_row(
                        side=self._position_side,
                        value_usd=self.position_size_usd,
                        measured=False,
                    )
                )
        elif self._position_side is not None:
            logger.info(
                "teardown: GMX measured FLAT on %s while cached side=%s — reporting the venue",
                self.market,
                self._position_side,
            )
        return TeardownPositionSummary(
            deployment_id=getattr(self, "deployment_id", "gmx_v2_directional_perp"),
            timestamp=datetime.now(UTC),
            positions=positions,
        )

    def generate_teardown_intents(self, mode: "TeardownMode", market: MarketSnapshot | None = None) -> list[AnyIntent]:
        """Close what the venue holds. Must agree with ``get_open_positions()``.

        If enumeration reports a venue position this never emits a close for, the
        completeness check fails the teardown loudly and the position is stranded
        anyway (VIB-5469 / ALM-2900) — so both halves read the same probe.
        """
        from almanak.framework.teardown import TeardownMode

        slippage = max(self.max_slippage, Decimal("0.02")) if mode == TeardownMode.HARD else self.max_slippage
        probe = self._venue_probe(market)
        if probe.is_open:
            sides = [found.is_long for found in probe.positions]
        elif probe.is_flat:
            sides = []
        else:
            sides = [] if self._position_side is None else [self._position_side == LONG]
        return [
            Intent.perp_close(
                market=self.market_address,
                collateral_token=self.collateral_token,
                is_long=is_long,
                size_usd=None,  # None = close the FULL on-chain position (never a cached notional — VIB-5950/ALM-2976)
                max_slippage=slippage,
                protocol="gmx_v2",
            )
            for is_long in sides
        ]
