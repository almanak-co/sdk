"""GMX V2 strategy-authored pending-order and position lifecycle.

This is an advanced connector-conformance demo for GMX's asynchronous order,
cancellation, and settlement states. For a general strategy-authoring reference,
start from ``gmx_v2_directional_perp`` or the ``perps`` scaffold instead.

Kitchen Loop iteration 27 -- first test of PerpOpenIntent and PerpCloseIntent.
Tests the GMX V2 connector end-to-end on Arbitrum with USDC collateral
(ERC-20 approval path, different from WETH native token path).

Lifecycle modes via force_action config:
  null/None   - Observation-driven open -> observe -> close: submit a market
                open, wait until ``market.perp_positions`` measures the
                position on-venue, submit a full close, wait until the venue
                measures flat. The recommended perp pattern -- portable across
                live, managed Anvil, and the PnL backtest plane, because it
                asks for no pending-order or cancellation surface.
  "lifecycle" - Open A -> Cancel A -> Open B -> settle B -> Close B. The
                pending-order surface test: order A is requested in
                ``submission`` mode so the callback holds the authoritative
                ``OrderCreated`` key before the managed-Anvil keeper
                convenience can fill it; cancellation waits for GMX's
                account-cancel age gate. Live/Anvil only -- the PnL backtest
                plane refuses PERP_CANCEL_ORDER and has no pending orders.
  "open"      - Open a single long position (for isolated testing)
  "close"     - Close an existing position (for isolated testing)

Both lifecycles close only after a venue position read proves the open
settled: the measured read, not the strategy's own cache, is the authority
(the same three-valued discipline as ``probe_perp_position``).
"""

import logging
from datetime import UTC, datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Any, Literal

from almanak.demo_strategies._address_config import require_evm_address
from almanak.framework.intents import Intent, IntentType
from almanak.framework.market import MarketSnapshot
from almanak.framework.strategies import IntentStrategy, almanak_strategy
from almanak.framework.utils.log_formatters import format_usd

if TYPE_CHECKING:
    from almanak.framework.strategies import PerpProbe
    from almanak.framework.teardown import PositionInfo

logger = logging.getLogger(__name__)


@almanak_strategy(
    name="gmx_perp_lifecycle",
    description="GMX V2 authored lifecycle: open, cancel, replace, settle, and close",
    version="2.0.0",
    author="Kitchen Loop",
    tags=["perpetuals", "gmx", "lifecycle", "test"],
    supported_chains=["arbitrum", "avalanche"],
    supported_protocols=["gmx_v2"],
    intent_types=[IntentType.PERP_OPEN, IntentType.PERP_CANCEL_ORDER, IntentType.PERP_CLOSE, IntentType.HOLD],
    quote_asset="USD",
)
class GMXPerpLifecycleStrategy(IntentStrategy):
    """Lifecycle strategy for testing GMX V2 perp open/close on Anvil.

    Uses USDC collateral to exercise the ERC-20 approval + sendTokens path
    (distinct from WETH which uses sendWnt with msg.value).
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.market = self.get_config("market", "ETH/USD")
        # Address-first contract: the strategy author declares the GMX
        # market-token ADDRESS; the SDK verifies it (dynamic registry,
        # VIB-6561) but never maps a symbol to an address on the author's
        # behalf. The label above stays as display/signal vocabulary only.
        self.market_address = require_evm_address(self, "market_address")
        self.index_token_address = require_evm_address(self, "index_token_address")
        self.collateral_token = self.get_config("collateral_token", "USDC")
        self.collateral_token_address = require_evm_address(self, "collateral_token_address")
        self.collateral_amount = Decimal(str(self.get_config("collateral_amount", "10")))
        self.leverage = Decimal(str(self.get_config("leverage", "2.0")))
        self.is_long = self.get_config("is_long", True)
        self.max_slippage_pct = Decimal(str(self.get_config("max_slippage_pct", "2.0")))
        self.force_action = self.get_config("force_action", None)
        self.cancel_min_age_seconds = int(self.get_config("cancel_min_age_seconds", 315))
        if self.cancel_min_age_seconds < 0:
            raise ValueError("cancel_min_age_seconds must be >= 0")
        self.pending_trigger_distance_pct = Decimal(str(self.get_config("pending_trigger_distance_pct", "50")))
        if not Decimal("0") < self.pending_trigger_distance_pct < Decimal("100"):
            raise ValueError("pending_trigger_distance_pct must be in (0, 100)")

        # State machine: idle -> open -> closed
        self._loop_state = "idle"
        self._previous_stable_state = "idle"
        self._position_size_usd = Decimal("0")
        self._pending_order_key: str | None = None
        self._pending_order_created_at: datetime | None = None
        self._replacement_order_key: str | None = None
        self._close_order_key: str | None = None
        self._position_observed = False

        logger.info(
            f"GMXPerpLifecycle initialized: market={self.market}, "
            f"collateral={self.collateral_amount} {self.collateral_token}, "
            f"leverage={self.leverage}x, direction={'LONG' if self.is_long else 'SHORT'}, "
            f"force_action={self.force_action}"
        )

    # ------------------------------------------------------------------
    # Token tracking — use chain-specific addresses for every data lookup.
    # ``market`` and ``collateral_token`` remain human-readable labels for logs
    # and intents; they are never used as data-plane identities.
    # ------------------------------------------------------------------
    def _get_tracked_tokens(self) -> list[str]:
        return list(dict.fromkeys((self.index_token_address, self.collateral_token_address)))

    def decide(self, market: MarketSnapshot) -> Intent | None:  # noqa: C901
        """Main decision: open or close a perp position based on state.

        Price is only required for open paths; close actions must never be
        gated on price availability (an open perp would otherwise be stranded).
        The transitions stay together deliberately: this is a persisted safety
        state machine, and splitting individual phases across helpers makes the
        reachable transition graph harder to audit.
        """
        index_token = self.market.split("/")[0]

        def _price_for_open() -> Decimal | None:
            try:
                price = market.price(self.index_token_address)
                logger.info(f"{index_token} price: ${price:,.2f}")
                return price
            except (ValueError, KeyError) as exc:
                logger.warning(f"Could not get price for {index_token}: {exc}")
                return None

        # Force action mode (for isolated Anvil testing).
        # Still drives the state machine so a continuously running demo doesn't
        # stack positions on every iteration.
        if self.force_action == "open":
            if self._loop_state == "idle":
                current_price = _price_for_open()
                if current_price is None:
                    return Intent.hold(reason=f"Price data unavailable for {index_token}")
                logger.info("Force action: OPEN")
                self._previous_stable_state = "idle"
                self._loop_state = "opening"
                return self._create_open_intent(market)
            if self._loop_state == "open":
                return Intent.hold(reason="Force action 'open' already executed")
            return Intent.hold(reason=f"Waiting for {self._loop_state} to complete")

        if self.force_action == "close":
            if self._loop_state in ("idle", "open"):
                logger.info("Force action: CLOSE")
                self._previous_stable_state = self._loop_state
                self._loop_state = "closing"
                return self._create_close_intent()
            if self._loop_state == "closed":
                return Intent.hold(reason="Force action 'close' already executed")
            return Intent.hold(reason=f"Waiting for {self._loop_state} to complete")

        # Strategy-authored lifecycles. Default: observation-driven
        # open -> observe -> close. force_action="lifecycle": the pending-order
        # choreography (open A -> cancel A -> open B -> close B).
        if self._loop_state == "idle":
            current_price = _price_for_open()
            if current_price is None:
                return Intent.hold(reason=f"Price data unavailable for {index_token}")
            if self.force_action == "lifecycle":
                logger.info("Lifecycle: submitting pending order A")
                self._previous_stable_state = "idle"
                self._loop_state = "opening_a"
                trigger_fraction = self.pending_trigger_distance_pct / Decimal("100")
                trigger_price = (
                    current_price * (Decimal("1") - trigger_fraction)
                    if self.is_long
                    else current_price * (Decimal("1") + trigger_fraction)
                )
                return self._create_open_intent(
                    market,
                    settlement_mode="submission",
                    trigger_price=trigger_price,
                )
            logger.info("Lifecycle: submitting market open")
            self._previous_stable_state = "idle"
            self._loop_state = "opening_b"
            return self._create_open_intent(market)

        if self._loop_state == "pending_a":
            if self._pending_order_key is None:
                raise RuntimeError("pending_a requires a persisted authoritative order key")
            chain_now = market.block_timestamp(chain=self.chain)
            if chain_now is None:
                return Intent.hold(reason="GMX chain timestamp is unmeasured; refusing to age-gate cancellation")
            if self._pending_order_created_at is None:
                # Start conservatively at the first measured block after the
                # callback. This can wait slightly longer than GMX requires but
                # can never cancel early because a host clock ran ahead.
                self._pending_order_created_at = chain_now
                return Intent.hold(reason="Order A chain-time cancellation baseline captured")
            age_seconds = (chain_now - self._pending_order_created_at).total_seconds()
            if age_seconds < self.cancel_min_age_seconds:
                remaining = max(0, self.cancel_min_age_seconds - int(age_seconds))
                return Intent.hold(reason=f"Order A is pending; GMX cancellation gate opens in {remaining}s")
            self._previous_stable_state = "pending_a"
            self._loop_state = "cancelling_a"
            return Intent.perp_cancel_order(
                order_key=self._pending_order_key,
                protocol="gmx_v2",
                chain=self.chain,
            )

        if self._loop_state == "cancelled_a":
            current_price = _price_for_open()
            if current_price is None:
                return Intent.hold(reason=f"Price data unavailable for {index_token}")
            logger.info("Lifecycle: cancellation confirmed; submitting replacement order B")
            self._previous_stable_state = "cancelled_a"
            self._loop_state = "opening_b"
            return self._create_open_intent(market)

        if self._loop_state == "order_b_pending":
            position_open = self._target_position_is_open(market)
            if position_open is None:
                return Intent.hold(reason="Open settlement is unmeasured; refusing to close")
            if not position_open:
                return Intent.hold(reason="Submitted open is pending settlement")
            self._position_observed = True
            self._previous_stable_state = "order_b_pending"
            self._loop_state = "closing_b"
            logger.info("Lifecycle: position measured on-venue; submitting full close")
            return self._create_close_intent()

        if self._loop_state == "close_submitted":
            position_open = self._target_position_is_open(market)
            if position_open is None:
                return Intent.hold(reason="Final close state is unmeasured")
            if position_open:
                return Intent.hold(reason="Final close is awaiting keeper settlement")
            self._loop_state = "closed"
            self._previous_stable_state = "closed"
            self._position_observed = False
            self._position_size_usd = Decimal("0")
            if self.force_action == "lifecycle":
                logger.info("Lifecycle complete -- Open A, Cancel A, Open B, Close B confirmed")
            else:
                logger.info("Lifecycle complete -- open observed on-venue, close measured flat")
            return Intent.hold(reason="Lifecycle complete")

        if self._loop_state == "open":
            logger.info("Lifecycle: closing position")
            self._previous_stable_state = "open"
            self._loop_state = "closing"
            return self._create_close_intent()

        if self._loop_state == "closed":
            logger.info("Lifecycle complete -- both open and close executed")
            return Intent.hold(reason="Lifecycle complete")

        # Transitional states (opening, closing) -- hold while waiting
        logger.info(f"In transitional state '{self._loop_state}', holding")
        return Intent.hold(reason=f"Waiting for {self._loop_state} to complete")

    def _create_open_intent(
        self,
        market: MarketSnapshot,
        *,
        settlement_mode: Literal["auto", "submission"] = "auto",
        trigger_price: Decimal | None = None,
    ) -> Intent:
        """Create PerpOpenIntent sized against the collateral token's USD price.

        Position sizing always uses the collateral token's own price (e.g. a
        WETH-collateralised BTC/USD trade is sized by WETH, not BTC), so the
        index-token price isn't needed here.
        """
        collateral_value_usd = market.collateral_value_usd(
            self.collateral_token_address,
            self.collateral_amount,
        )
        position_size_usd = collateral_value_usd * self.leverage

        max_slippage = self.max_slippage_pct / Decimal("100")

        self._position_size_usd = position_size_usd

        direction = "LONG" if self.is_long else "SHORT"
        logger.info(
            f"Opening {direction}: {self.collateral_amount} {self.collateral_token} "
            f"({format_usd(collateral_value_usd)}) -> {format_usd(position_size_usd)} position "
            f"@ {self.leverage}x leverage, slippage={self.max_slippage_pct}%"
        )

        return Intent.perp_open(
            market=self.market_address,
            collateral_token=self.collateral_token,
            collateral_amount=self.collateral_amount,
            size_usd=position_size_usd,
            is_long=self.is_long,
            leverage=self.leverage,
            max_slippage=max_slippage,
            protocol="gmx_v2",
            settlement_mode=settlement_mode,
            trigger_price=trigger_price,
        )

    def _venue_probe(self, market: MarketSnapshot | None = None, *, is_long: bool | None = None) -> "PerpProbe":
        """Return normalized venue truth for this GMX market.

        ``MarketSnapshot.perp_positions()`` returns raw
        ``PerpsPositionOnChain`` rows. Strategy code must not value those rows
        directly: their numeric fields use venue-specific fixed-point scales.
        ``probe_perp_position`` is the public conversion seam and returns
        ``PerpProbePosition`` rows whose ``notional_usd`` is normalized.
        """
        from almanak.framework.strategies import probe_perp_position

        snapshot = market
        if snapshot is None:
            try:
                snapshot = self.create_market_snapshot()
            except Exception as exc:  # noqa: BLE001 — no snapshot is UNMEASURED, never flat
                logger.warning("teardown: no market snapshot for the venue probe (%s)", exc)
                snapshot = None
        return probe_perp_position(
            snapshot,
            protocol="gmx_v2",
            chain=self.chain,
            market_symbol=self.market_address,
            index_token_address=self.index_token_address,
            is_long=is_long,
        )

    def _target_position_is_open(self, market: MarketSnapshot) -> bool | None:
        """Return measured target exposure, or ``None`` when the read is incomplete."""
        probe = self._venue_probe(market, is_long=self.is_long)
        if not probe.is_measured:
            return None
        return probe.is_open

    def _create_close_intent(self) -> Intent:
        """Create PerpCloseIntent to close the full position."""
        max_slippage = self.max_slippage_pct / Decimal("100")

        direction = "LONG" if self.is_long else "SHORT"
        logger.info(f"Closing {direction}: {self.market}, size=FULL")

        return Intent.perp_close(
            market=self.market_address,
            collateral_token=self.collateral_token,
            is_long=self.is_long,
            size_usd=None,  # None = close the FULL on-chain position (never a cached notional — VIB-5950/ALM-2976)
            max_slippage=max_slippage,
            protocol="gmx_v2",
        )

    def on_intent_executed(self, intent: Intent, success: bool, result: Any) -> None:
        """Advance state machine on successful execution.

        Transitions are driven by the executed intent type, not by the current
        ``_loop_state`` value. This matters for teardown: the framework emits a
        PERP_CLOSE while the strategy is still in ``"open"`` (teardown bypasses
        the normal ``open -> closing -> closed`` cycle). A state-driven
        condition would silently skip the transition, leaving
        ``get_open_positions()`` reporting a stale synthetic position and
        failing post-teardown verification.
        """
        intent_type = intent.intent_type.value if hasattr(intent, "intent_type") else str(intent)

        if not success:
            logger.warning(f"Intent {intent_type} failed, reverting to {self._previous_stable_state}")
            self._loop_state = self._previous_stable_state
            return

        logger.info(f"Intent {intent_type} executed successfully")

        if intent_type == "PERP_OPEN":
            self._on_perp_open_executed(result)
        elif intent_type == "PERP_CANCEL_ORDER":
            if self._loop_state != "cancelling_a":
                logger.warning("Ignoring unexpected cancel callback in state %s", self._loop_state)
                return
            self._loop_state = "cancelled_a"
            self._previous_stable_state = "cancelled_a"
            logger.info("Order A cancellation confirmed: %s", self._pending_order_key)
            self._pending_order_key = None
            self._pending_order_created_at = None

        elif intent_type == "PERP_CLOSE":
            prior_state = self._loop_state
            if prior_state == "closing_b":
                order_key, capture_ok = self._order_key_or_observation_fallback(result, phase="closing_b")
                if not capture_ok:
                    return
                self._close_order_key = order_key
                self._loop_state = "close_submitted"
                self._previous_stable_state = "close_submitted"
                if order_key is not None:
                    logger.info("Final close order accepted: %s", order_key)
                return
            self._loop_state = "closed"
            self._previous_stable_state = "closed"
            # Clear the synthetic position size so get_open_positions() no
            # longer reports this position as open. Idempotent on repeat calls.
            self._position_size_usd = Decimal("0")
            if prior_state != "closed":
                logger.info(f"State: {prior_state} -> closed")

    def _on_perp_open_executed(self, result: Any) -> None:
        """Advance the machine after a successful PERP_OPEN (all lifecycle modes)."""
        prior_state = self._loop_state
        if prior_state == "opening_a":
            order_key = self._capture_order_key_or_require_recovery(result, phase="opening_a")
            if order_key is None:
                return
            self._pending_order_key = order_key
            self._pending_order_created_at = None
            self._loop_state = "pending_a"
            self._previous_stable_state = "pending_a"
            logger.info("Order A accepted and persisted: %s", order_key)
            return
        if prior_state == "opening_b":
            order_key, capture_ok = self._order_key_or_observation_fallback(result, phase="opening_b")
            if not capture_ok:
                return
            self._replacement_order_key = order_key
            self._loop_state = "order_b_pending"
            self._previous_stable_state = "order_b_pending"
            if order_key is not None:
                logger.info("Open order accepted and persisted: %s", order_key)
            return
        self._loop_state = "open"
        # Promote the stable-state marker so a later failed close reverts
        # to "open" (the current truth), not whatever was last recorded by
        # decide() — typically "idle" when decide() opened the position,
        # which would silently hide a live position from
        # get_open_positions() after a failed teardown.
        self._previous_stable_state = "open"
        if prior_state != "open":
            logger.info(f"State: {prior_state} -> open")

    @staticmethod
    def _authoritative_order_key(result: Any) -> str:
        orders = tuple(getattr(result, "async_orders", ()) or ())
        if len(orders) != 1:
            raise RuntimeError(f"Expected exactly one authoritative asynchronous order, received {len(orders)}")
        key = str(getattr(orders[0], "order_id", "") or "")
        if len(key) != 66 or not key.startswith("0x"):
            raise RuntimeError("Execution result did not contain a valid bytes32 order key")
        try:
            if int(key, 16) == 0:
                raise ValueError("zero key")
        except ValueError as exc:
            raise RuntimeError("Execution result contained a malformed order key") from exc
        return key.lower()

    def _capture_order_key_or_require_recovery(self, result: Any, *, phase: str) -> str | None:
        """Capture one authoritative key without re-arming a successful submission."""
        try:
            return self._authoritative_order_key(result)
        except RuntimeError as exc:
            self._loop_state = "recovery_required"
            self._previous_stable_state = "recovery_required"
            logger.critical(
                "GMX %s succeeded without a usable order identity; refusing replay and requiring residual teardown: %s",
                phase,
                exc,
            )
            return None

    def _order_key_or_observation_fallback(self, result: Any, *, phase: str) -> tuple[str | None, bool]:
        """Capture the order key, or fall back to venue-read verification.

        Returns ``(key, capture_ok)``. A result with NO asynchronous orders is
        a synchronous settlement (the PnL backtest plane books fills instantly,
        so no order ever exists) or a result that lost its order identity —
        either way the safe next step is the same: never replay, proceed to the
        observation gate, and let the measured ``perp_positions`` read decide.
        Observing an open position risk-reduces (it gets closed); an unsettled
        or lost order holds at the gate forever, exactly like the old
        ``recovery_required`` park but able to unwind when the venue does show
        the position. A result that DOES carry asynchronous orders but with a
        malformed or ambiguous key keeps the loud ``recovery_required`` path —
        that shape only occurs on a broken live enrichment.
        """
        orders = tuple(getattr(result, "async_orders", ()) or ())
        if not orders:
            logger.warning(
                "GMX %s executed with no asynchronous order; verifying settlement via the venue position read",
                phase,
            )
            return None, True
        order_key = self._capture_order_key_or_require_recovery(result, phase=phase)
        return order_key, order_key is not None

    # --- State persistence (required so teardown survives restarts) ---

    def get_persistent_state(self) -> dict[str, Any]:
        return {
            "loop_state": self._loop_state,
            "previous_stable_state": self._previous_stable_state,
            "position_size_usd": str(self._position_size_usd),
            "pending_order_key": self._pending_order_key,
            "pending_order_created_at": (
                self._pending_order_created_at.isoformat() if self._pending_order_created_at is not None else None
            ),
            "replacement_order_key": self._replacement_order_key,
            "close_order_key": self._close_order_key,
            "position_observed": self._position_observed,
        }

    def load_persistent_state(self, state: dict[str, Any]) -> None:
        restored = state.get("loop_state", "idle")
        previous = state.get("previous_stable_state", "idle")

        # A crash after submission but before its callback leaves chain outcome
        # unknown. Replaying any transitional verb can duplicate a live order,
        # close, or fee. Hold permanently and let connector residual discovery
        # measure/cancel/close the exact on-chain exposure during teardown.
        in_flight_states = {"opening", "opening_a", "closing", "cancelling_a", "opening_b", "closing_b"}
        if restored in in_flight_states:
            logger.critical(
                "Persisted in-flight state '%s' detected; refusing replay and requiring residual teardown",
                restored,
            )
            restored = "recovery_required"
            previous = "recovery_required"

        self._loop_state = restored
        self._previous_stable_state = previous
        try:
            self._position_size_usd = Decimal(str(state.get("position_size_usd", "0")))
        except Exception:  # noqa: BLE001
            self._position_size_usd = Decimal("0")
        self._pending_order_key = state.get("pending_order_key")
        raw_created_at = state.get("pending_order_created_at")
        try:
            self._pending_order_created_at = datetime.fromisoformat(raw_created_at) if raw_created_at else None
        except (TypeError, ValueError):
            self._pending_order_created_at = None
        self._replacement_order_key = state.get("replacement_order_key")
        self._close_order_key = state.get("close_order_key")
        self._position_observed = bool(state.get("position_observed", False))
        logger.info(f"Restored state: loop_state={self._loop_state}, position_size_usd={self._position_size_usd}")

    # --- Teardown ---

    def _cache_may_hold_position(self) -> bool:
        """Whether persisted state requires a fail-closed UNMEASURED fallback."""
        if self._position_observed:
            return self._loop_state != "closed"
        return self._loop_state in {
            "open",
            "opening",
            "opening_a",
            "pending_a",
            "cancelling_a",
            "opening_b",
            "order_b_pending",
            "closing",
            "closing_b",
            "close_submitted",
            "recovery_required",
        }

    def _cache_needs_close(self) -> bool:
        """Whether fallback state needs a new close rather than settlement observation."""
        return self._loop_state != "close_submitted" and self._cache_may_hold_position()

    def _teardown_position_row(
        self,
        *,
        is_long: bool,
        collateral_token: str,
        value_usd: Decimal,
        from_venue: bool,
        value_measured: bool,
    ) -> "PositionInfo":
        from almanak.framework.teardown import PositionInfo, PositionType

        side = "long" if is_long else "short"
        details: dict[str, Any] = {
            "market": self.market_address,
            "market_address": self.market_address,
            "is_long": is_long,
            "side": side,
            "leverage": str(self.leverage),
            "collateral_token": collateral_token,
            "position_source": "venue" if from_venue else "strategy_cache_unverified",
        }
        if not value_measured:
            details["value_usd_unknown"] = True
            details["valuation_status"] = "no_path"
        return PositionInfo(
            position_type=PositionType.PERP,
            position_id=(f"gmx-{self.market_address.lower()}-{collateral_token.lower()}-{side}"),
            chain=self.chain,
            protocol="gmx_v2",
            value_usd=value_usd,
            details=details,
        )

    def get_open_positions(self):
        """Enumerate GMX venue positions, retaining cache only when unmeasured."""
        from almanak.framework.teardown import PositionInfo, TeardownPositionSummary

        probe = self._venue_probe()
        positions: list[PositionInfo] = []
        if probe.is_open:
            for found in probe.positions:
                measured = found.notional_usd is not None
                positions.append(
                    self._teardown_position_row(
                        is_long=found.is_long,
                        collateral_token=found.collateral_token,
                        value_usd=found.notional_usd if measured else self._position_size_usd,
                        from_venue=True,
                        value_measured=measured,
                    )
                )
        elif not probe.is_measured and self._cache_may_hold_position():
            logger.warning(
                "teardown: GMX position read UNMEASURED (%s) — retaining cached side=%s; "
                "an unmeasured read is not a flat account",
                probe.reason,
                "long" if self.is_long else "short",
            )
            positions.append(
                self._teardown_position_row(
                    is_long=self.is_long,
                    collateral_token=self.collateral_token,
                    value_usd=self._position_size_usd,
                    from_venue=False,
                    value_measured=False,
                )
            )
        elif probe.is_flat and self._cache_may_hold_position():
            logger.info(
                "teardown: GMX measured FLAT on %s while cached state=%s — reporting the venue",
                self.market,
                self._loop_state,
            )

        return TeardownPositionSummary(
            deployment_id=self.deployment_id or self.STRATEGY_NAME,
            timestamp=datetime.now(UTC),
            positions=positions,
        )

    def generate_teardown_intents(self, mode, market=None):
        from almanak.framework.teardown import TeardownMode

        probe = self._venue_probe(market)
        if probe.is_open:
            close_targets = [(found.is_long, found.collateral_token) for found in probe.positions]
        elif probe.is_flat:
            close_targets = []
        elif self._cache_needs_close():
            close_targets = [(self.is_long, self.collateral_token)]
        else:
            close_targets = []

        slippage = Decimal("0.03") if mode == TeardownMode.HARD else Decimal("0.01")
        return [
            Intent.perp_close(
                market=self.market_address,
                collateral_token=collateral_token,
                is_long=is_long,
                # Full close: the compiler resolves the live size. Never pass a
                # cached or raw venue notional (VIB-5950 / ALM-2976 / ALM-3218).
                size_usd=None,
                max_slippage=slippage,
                protocol="gmx_v2",
            )
            for is_long, collateral_token in close_targets
        ]
