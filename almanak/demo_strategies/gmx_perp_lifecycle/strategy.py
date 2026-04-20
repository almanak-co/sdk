"""GMX V2 Perpetual Futures Lifecycle Strategy.

Kitchen Loop iteration 27 -- first test of PerpOpenIntent and PerpCloseIntent.
Tests the GMX V2 connector end-to-end on Arbitrum with USDC collateral
(ERC-20 approval path, different from WETH native token path).

Lifecycle modes via force_action config:
  "open"      - Open a single long position (for isolated testing)
  "close"     - Close an existing position (for isolated testing)
  "lifecycle" - Open then close on subsequent iterations
  null/None   - Same as "lifecycle"
"""

import logging
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from almanak.framework.intents import Intent
from almanak.framework.strategies import IntentStrategy, MarketSnapshot, almanak_strategy
from almanak.framework.utils.log_formatters import format_usd

logger = logging.getLogger(__name__)


@almanak_strategy(
    name="gmx_perp_lifecycle",
    description="GMX V2 perpetual futures lifecycle test (open + close)",
    version="1.0.0",
    author="Kitchen Loop",
    tags=["perpetuals", "gmx", "lifecycle", "test"],
    supported_chains=["arbitrum", "avalanche"],
    supported_protocols=["gmx_v2"],
    intent_types=["PERP_OPEN", "PERP_CLOSE", "HOLD"],
)
class GMXPerpLifecycleStrategy(IntentStrategy):
    """Lifecycle strategy for testing GMX V2 perp open/close on Anvil.

    Uses USDC collateral to exercise the ERC-20 approval + sendTokens path
    (distinct from WETH which uses sendWnt with msg.value).
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.market = self.get_config("market", "ETH/USD")
        self.collateral_token = self.get_config("collateral_token", "USDC")
        self.collateral_amount = Decimal(str(self.get_config("collateral_amount", "10")))
        self.leverage = Decimal(str(self.get_config("leverage", "2.0")))
        self.is_long = self.get_config("is_long", True)
        self.max_slippage_pct = Decimal(str(self.get_config("max_slippage_pct", "2.0")))
        self.force_action = self.get_config("force_action", None)

        # State machine: idle -> open -> closed
        self._loop_state = "idle"
        self._previous_stable_state = "idle"
        self._position_size_usd = Decimal("0")

        logger.info(
            f"GMXPerpLifecycle initialized: market={self.market}, "
            f"collateral={self.collateral_amount} {self.collateral_token}, "
            f"leverage={self.leverage}x, direction={'LONG' if self.is_long else 'SHORT'}, "
            f"force_action={self.force_action}"
        )

    def decide(self, market: MarketSnapshot) -> Intent | None:
        """Main decision: open or close a perp position based on state.

        Price is only required for open paths; close actions must never be
        gated on price availability (an open perp would otherwise be stranded).
        """
        index_token = self.market.split("/")[0]

        def _price_for_open() -> Decimal | None:
            try:
                price = market.price(index_token)
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

        # Lifecycle mode (open -> close)
        if self._loop_state == "idle":
            current_price = _price_for_open()
            if current_price is None:
                return Intent.hold(reason=f"Price data unavailable for {index_token}")
            logger.info("Lifecycle: opening position")
            self._previous_stable_state = "idle"
            self._loop_state = "opening"
            return self._create_open_intent(market)

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

    def _create_open_intent(self, market: MarketSnapshot) -> Intent:
        """Create PerpOpenIntent sized against the collateral token's USD price.

        Position sizing always uses the collateral token's own price (e.g. a
        WETH-collateralised BTC/USD trade is sized by WETH, not BTC), so the
        index-token price isn't needed here.
        """
        collateral_value_usd = market.collateral_value_usd(self.collateral_token, self.collateral_amount)
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
            market=self.market,
            collateral_token=self.collateral_token,
            collateral_amount=self.collateral_amount,
            size_usd=position_size_usd,
            is_long=self.is_long,
            leverage=self.leverage,
            max_slippage=max_slippage,
            protocol="gmx_v2",
        )

    def _create_close_intent(self) -> Intent:
        """Create PerpCloseIntent to close the full position."""
        max_slippage = self.max_slippage_pct / Decimal("100")

        # Use tracked size if available, otherwise close full position (size_usd=None)
        close_size = self._position_size_usd if self._position_size_usd > 0 else None

        direction = "LONG" if self.is_long else "SHORT"
        logger.info(
            f"Closing {direction}: {self.market}, size={format_usd(close_size) if close_size else 'FULL'}"
        )

        return Intent.perp_close(
            market=self.market,
            collateral_token=self.collateral_token,
            is_long=self.is_long,
            size_usd=close_size,
            max_slippage=max_slippage,
            protocol="gmx_v2",
        )

    def on_intent_executed(self, intent: Intent, success: bool, result: Any) -> None:
        """Advance state machine on successful execution."""
        intent_type = intent.intent_type.value if hasattr(intent, "intent_type") else str(intent)

        if success:
            logger.info(f"Intent {intent_type} executed successfully")
            if self._loop_state == "opening":
                self._loop_state = "open"
                logger.info("State: opening -> open")
            elif self._loop_state == "closing":
                self._loop_state = "closed"
                self._position_size_usd = Decimal("0")
                logger.info("State: closing -> closed")
        else:
            logger.warning(f"Intent {intent_type} failed, reverting to {self._previous_stable_state}")
            self._loop_state = self._previous_stable_state

    # --- State persistence (required so teardown survives restarts) ---

    def get_persistent_state(self) -> dict[str, Any]:
        return {
            "loop_state": self._loop_state,
            "previous_stable_state": self._previous_stable_state,
            "position_size_usd": str(self._position_size_usd),
        }

    def load_persistent_state(self, state: dict[str, Any]) -> None:
        restored = state.get("loop_state", "idle")
        previous = state.get("previous_stable_state", "idle")

        # If the worker crashed mid-transition (after emitting an intent but
        # before on_intent_executed ran), the persisted state may be "opening"
        # or "closing". In those states decide() would keep returning HOLD and
        # teardown would miss the position. Normalize back to the last stable
        # state so the next iteration re-evaluates cleanly.
        if restored == "opening":
            logger.warning("Persisted state 'opening' detected on load; normalising to 'idle'")
            restored = previous if previous in ("idle", "open", "closed") else "idle"
        elif restored == "closing":
            logger.warning("Persisted state 'closing' detected on load; normalising to 'open'")
            restored = previous if previous in ("idle", "open", "closed") else "open"

        self._loop_state = restored
        self._previous_stable_state = previous
        try:
            self._position_size_usd = Decimal(str(state.get("position_size_usd", "0")))
        except Exception:  # noqa: BLE001
            self._position_size_usd = Decimal("0")
        logger.info(
            f"Restored state: loop_state={self._loop_state}, position_size_usd={self._position_size_usd}"
        )

    # --- Teardown ---

    def get_open_positions(self):
        from almanak.framework.teardown import PositionInfo, PositionType, TeardownPositionSummary

        positions = []
        # Report an open position whenever state says "open", even if size is
        # unknown (corrupt/missing persisted state) — otherwise teardown would
        # silently skip a live position after a restart.
        if self._loop_state == "open":
            positions.append(
                PositionInfo(
                    position_type=PositionType.PERP,
                    position_id=f"gmx-{self.market}-{self.chain}",
                    chain=self.chain,
                    protocol="gmx_v2",
                    value_usd=self._position_size_usd,
                    details={
                        "market": self.market,
                        "is_long": self.is_long,
                        "leverage": str(self.leverage),
                        "collateral_token": self.collateral_token,
                        "size_known": self._position_size_usd > 0,
                    },
                )
            )

        return TeardownPositionSummary(
            strategy_id=self.strategy_id or self.STRATEGY_NAME,
            timestamp=datetime.now(UTC),
            positions=positions,
        )

    def generate_teardown_intents(self, mode, market=None):
        from almanak.framework.teardown import TeardownMode

        intents = []
        if self._loop_state == "open":
            slippage = Decimal("0.03") if mode == TeardownMode.HARD else Decimal("0.01")
            # If size is unknown (corrupt/missing persisted state), emit a full
            # close by passing size_usd=None. Passing size_usd=0 would be a no-op.
            close_size = self._position_size_usd if self._position_size_usd > 0 else None
            intents.append(
                Intent.perp_close(
                    market=self.market,
                    collateral_token=self.collateral_token,
                    is_long=self.is_long,
                    size_usd=close_size,
                    max_slippage=slippage,
                    protocol="gmx_v2",
                )
            )
        return intents
