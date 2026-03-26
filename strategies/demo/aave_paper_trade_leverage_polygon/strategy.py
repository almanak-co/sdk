"""Paper Trading Demo: Aave V3 + Uniswap V3 Leverage Loop on Polygon.

Kitchen Loop iteration 129 (VIB-1897). First paper trade on Polygon and
first paper trade with multi-protocol composition (lending + swap).

Exercises the paper trading pipeline with:
- Polygon-specific gas pricing (MATIC)
- Multi-protocol intent composition (Aave V3 SUPPLY/BORROW + Uniswap V3 SWAP)
- PnL tracking across lending + swap positions

Pipeline flow (one step per decide() call):
1. SUPPLY: Supply USDC to Aave V3 as collateral
2. BORROW: Borrow WETH from Aave V3 at 30% LTV
3. SWAP: Swap WETH -> WMATIC via Uniswap V3
4. HOLD: Leverage loop complete, paper trader tracks PnL

Usage:
    almanak strat backtest paper start \\
        -s demo_aave_paper_trade_leverage_polygon \\
        --chain polygon \\
        --max-ticks 5 \\
        --tick-interval 60 \\
        --foreground

    # Or single iteration on Anvil
    almanak strat run -d strategies/demo/aave_paper_trade_leverage_polygon \\
        --network anvil --once
"""

import logging
from datetime import UTC, datetime
from decimal import ROUND_DOWN, Decimal
from typing import TYPE_CHECKING, Any

from almanak.framework.api.timeline import TimelineEvent, TimelineEventType, add_event
from almanak.framework.data.market_snapshot import PriceUnavailableError
from almanak.framework.intents import Intent
from almanak.framework.strategies import IntentStrategy, MarketSnapshot, almanak_strategy
from almanak.framework.utils.log_formatters import format_token_amount_human, format_usd

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from almanak.framework.teardown import TeardownMode, TeardownPositionSummary

# Stable states
IDLE = "idle"
SUPPLIED = "supplied"
BORROWED = "borrowed"
COMPLETE = "complete"

# Transitional states
SUPPLYING = "supplying"
BORROWING = "borrowing"
SWAPPING = "swapping"

STABLE_STATES = {IDLE, SUPPLIED, BORROWED, COMPLETE}
TRANSITIONAL_STATES = {SUPPLYING, BORROWING, SWAPPING}


@almanak_strategy(
    name="demo_aave_paper_trade_leverage_polygon",
    description="Paper trading demo: Aave V3 + Uniswap V3 leverage loop on Polygon",
    version="1.0.0",
    author="Almanak",
    tags=["demo", "paper-trading", "lending", "swap", "aave-v3", "uniswap-v3", "polygon", "backtesting"],
    supported_chains=["polygon"],
    supported_protocols=["aave_v3", "uniswap_v3"],
    intent_types=["SUPPLY", "BORROW", "SWAP", "HOLD"],
    default_chain="polygon",
)
class AavePaperTradeLeveragePolygonStrategy(IntentStrategy):
    """Paper trading demo: Aave V3 + Uniswap V3 leverage loop on Polygon.

    Supplies USDC as collateral to Aave V3, borrows WETH at conservative LTV,
    then swaps WETH -> WMATIC via Uniswap V3. After the leverage loop completes,
    holds while the paper trader tracks PnL across remaining ticks.

    Configuration (config.json):
        collateral_token: Token to supply as collateral (default: "USDC")
        collateral_amount: Amount to supply (default: "500")
        borrow_token: Token to borrow (default: "WETH")
        swap_to_token: Target swap token (default: "WMATIC")
        ltv_target: Target LTV ratio (default: 0.3 = 30%)
        swap_protocol: Protocol for swap step (default: "uniswap_v3")
    """

    def supports_teardown(self) -> bool:
        return True

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)

        self.collateral_token = self.get_config("collateral_token", "USDC")
        self.collateral_amount = Decimal(str(self.get_config("collateral_amount", "500")))
        self.borrow_token = self.get_config("borrow_token", "WETH")
        self.swap_to_token = self.get_config("swap_to_token", "WMATIC")
        self.ltv_target = Decimal(str(self.get_config("ltv_target", "0.3")))
        self.swap_protocol = self.get_config("swap_protocol", "uniswap_v3")

        # State machine
        self._state = IDLE
        self._previous_stable_state = IDLE

        # Position tracking
        self._supplied_amount = Decimal("0")
        self._borrowed_amount = Decimal("0")
        self._swapped_amount = Decimal("0")

        logger.info(
            "AavePaperTradeLeveragePolygon initialized: "
            "collateral=%s %s, borrow=%s, LTV=%s%%, swap to %s via %s",
            self.collateral_amount,
            self.collateral_token,
            self.borrow_token,
            self.ltv_target * 100,
            self.swap_to_token,
            self.swap_protocol,
        )

    def decide(self, market: MarketSnapshot) -> Intent | None:
        # Wait for in-flight intents to settle (don't replay non-idempotent actions)
        if self._state in TRANSITIONAL_STATES:
            logger.info("Waiting for in-flight '%s' intent to settle", self._state)
            return Intent.hold(reason=f"Waiting for {self._state} execution")

        # Phase 1: Supply USDC collateral to Aave V3
        if self._state == IDLE:
            logger.info("Phase 1: SUPPLY %s %s to Aave V3", self.collateral_amount, self.collateral_token)
            self._transition(SUPPLYING)
            return Intent.supply(
                protocol="aave_v3",
                token=self.collateral_token,
                amount=self.collateral_amount,
                use_as_collateral=True,
                chain=self.chain,
            )

        # Phase 2: Borrow WETH against USDC collateral
        if self._state == SUPPLIED:
            try:
                collateral_price = market.price(self.collateral_token)
                borrow_price = market.price(self.borrow_token)
            except (PriceUnavailableError, ValueError) as e:
                logger.warning("Price fetch failed: %s", e)
                return Intent.hold(reason=f"Price data unavailable: {e}")

            if borrow_price <= 0:
                logger.warning("Borrow token price is zero or negative: %s", borrow_price)
                return Intent.hold(reason=f"Invalid borrow price: {borrow_price}")

            collateral_value = self.collateral_amount * collateral_price
            borrow_value = collateral_value * self.ltv_target
            borrow_amount = (borrow_value / borrow_price).quantize(Decimal("0.0001"), rounding=ROUND_DOWN)

            logger.info(
                "Phase 2: BORROW %s from Aave V3 (collateral value=%s, LTV=%s%%)",
                format_token_amount_human(borrow_amount, self.borrow_token),
                format_usd(collateral_value),
                self.ltv_target * 100,
            )
            self._transition(BORROWING)
            return Intent.borrow(
                protocol="aave_v3",
                collateral_token=self.collateral_token,
                collateral_amount=Decimal("0"),  # Already supplied in Phase 1
                borrow_token=self.borrow_token,
                borrow_amount=borrow_amount,
                chain=self.chain,
            )

        # Phase 3: Swap WETH -> WMATIC via Uniswap V3
        if self._state == BORROWED:
            swap_amount = self._borrowed_amount
            if swap_amount <= 0:
                logger.warning("Borrow amount unknown -- holding")
                return Intent.hold(reason="Borrow amount unknown, cannot swap")
            logger.info(
                "Phase 3: SWAP %s -> %s via %s",
                format_token_amount_human(swap_amount, self.borrow_token),
                self.swap_to_token,
                self.swap_protocol,
            )
            self._transition(SWAPPING)
            return Intent.swap(
                from_token=self.borrow_token,
                to_token=self.swap_to_token,
                amount=swap_amount,
                max_slippage=Decimal("0.01"),
                protocol=self.swap_protocol,
            )

        # Done -- paper trader tracks PnL on remaining ticks
        if self._state == COMPLETE:
            return Intent.hold(
                reason=(
                    f"Leverage loop complete: "
                    f"supplied {self._supplied_amount} {self.collateral_token}, "
                    f"borrowed {self._borrowed_amount} {self.borrow_token}, "
                    f"swapped to ~{self._swapped_amount} {self.swap_to_token}"
                )
            )

        return Intent.hold(reason=f"Unknown state: {self._state}")

    def _transition(self, new_state: str) -> None:
        old = self._state
        if old in STABLE_STATES:
            self._previous_stable_state = old
        self._state = new_state
        logger.info("State transition: %s -> %s", old, new_state)

    def on_intent_executed(self, intent: Any, success: bool, result: Any) -> None:
        intent_type = getattr(intent, "intent_type", None)
        if intent_type is None:
            return
        intent_type_val = intent_type.value if hasattr(intent_type, "value") else str(intent_type)

        if success:
            if intent_type_val in ("SUPPLY", "SUPPLY_COLLATERAL"):
                self._state = SUPPLIED
                self._supplied_amount = self.collateral_amount
                add_event(
                    TimelineEvent(
                        timestamp=datetime.now(UTC),
                        event_type=TimelineEventType.POSITION_MODIFIED,
                        description=f"Supplied {self._supplied_amount} {self.collateral_token} to Aave V3",
                        strategy_id=self.strategy_id,
                        details={"action": "supply", "amount": str(self._supplied_amount), "protocol": "aave_v3"},
                    )
                )
                logger.info("SUPPLY succeeded: %s %s to Aave V3", self._supplied_amount, self.collateral_token)

            elif intent_type_val == "BORROW":
                self._state = BORROWED
                if hasattr(intent, "borrow_amount"):
                    self._borrowed_amount = Decimal(str(intent.borrow_amount))
                add_event(
                    TimelineEvent(
                        timestamp=datetime.now(UTC),
                        event_type=TimelineEventType.POSITION_MODIFIED,
                        description=f"Borrowed {self._borrowed_amount} {self.borrow_token} from Aave V3",
                        strategy_id=self.strategy_id,
                        details={"action": "borrow", "amount": str(self._borrowed_amount), "protocol": "aave_v3"},
                    )
                )
                logger.info("BORROW succeeded: %s %s from Aave V3", self._borrowed_amount, self.borrow_token)

            elif intent_type_val == "SWAP" and self._state == SWAPPING:
                # Forward-loop phase 3: WETH -> WMATIC
                self._state = COMPLETE
                if hasattr(result, "swap_amounts") and result.swap_amounts:
                    sa = result.swap_amounts
                    if hasattr(sa, "amount_out_decimal") and sa.amount_out_decimal:
                        self._swapped_amount = Decimal(str(sa.amount_out_decimal))
                add_event(
                    TimelineEvent(
                        timestamp=datetime.now(UTC),
                        event_type=TimelineEventType.POSITION_MODIFIED,
                        description=f"Swapped {self._borrowed_amount} {self.borrow_token} -> {self.swap_to_token}",
                        strategy_id=self.strategy_id,
                        details={"action": "swap", "protocol": self.swap_protocol},
                    )
                )
                logger.info(
                    "SWAP succeeded: %s %s -> ~%s %s. Leverage loop complete.",
                    self._borrowed_amount,
                    self.borrow_token,
                    self._swapped_amount,
                    self.swap_to_token,
                )

            elif intent_type_val == "SWAP":
                # Teardown reverse swap: clear swapped amount
                self._swapped_amount = Decimal("0")
                logger.info("Teardown SWAP succeeded: cleared swapped amount")

            elif intent_type_val == "REPAY":
                self._borrowed_amount = Decimal("0")
                logger.info("REPAY succeeded: cleared borrowed amount")

            elif intent_type_val == "WITHDRAW":
                self._supplied_amount = Decimal("0")
                logger.info("WITHDRAW succeeded: cleared supplied amount")
        else:
            revert_to = self._previous_stable_state
            logger.warning("%s FAILED in state '%s' -- reverting to '%s'", intent_type_val, self._state, revert_to)
            self._state = revert_to

    def get_persistent_state(self) -> dict[str, Any]:
        return {
            "state": self._state,
            "previous_stable_state": self._previous_stable_state,
            "supplied_amount": str(self._supplied_amount),
            "borrowed_amount": str(self._borrowed_amount),
            "swapped_amount": str(self._swapped_amount),
        }

    def load_persistent_state(self, state: dict[str, Any]) -> None:
        self._state = state.get("state", IDLE)
        self._previous_stable_state = state.get("previous_stable_state", IDLE)
        self._supplied_amount = Decimal(str(state.get("supplied_amount", "0")))
        self._borrowed_amount = Decimal(str(state.get("borrowed_amount", "0")))
        self._swapped_amount = Decimal(str(state.get("swapped_amount", "0")))

    def get_status(self) -> dict[str, Any]:
        return {
            "strategy": "demo_aave_paper_trade_leverage_polygon",
            "chain": self.chain,
            "state": self._state,
            "supplied_usdc": str(self._supplied_amount),
            "borrowed_weth": str(self._borrowed_amount),
            "swapped_wmatic": str(self._swapped_amount),
        }

    # -------------------------------------------------------------------------
    # Teardown
    # -------------------------------------------------------------------------

    def get_open_positions(self) -> "TeardownPositionSummary":
        from almanak.framework.teardown import PositionInfo, PositionType, TeardownPositionSummary

        positions: list[PositionInfo] = []

        if self._supplied_amount > 0:
            positions.append(
                PositionInfo(
                    position_type=PositionType.SUPPLY,
                    position_id=f"aave-v3-supply-{self.collateral_token}-polygon",
                    chain=self.chain,
                    protocol="aave_v3",
                    value_usd=self._supplied_amount,
                    details={"asset": self.collateral_token, "type": "collateral"},
                )
            )

        if self._borrowed_amount > 0:
            positions.append(
                PositionInfo(
                    position_type=PositionType.BORROW,
                    position_id=f"aave-v3-borrow-{self.borrow_token}-polygon",
                    chain=self.chain,
                    protocol="aave_v3",
                    value_usd=self._borrowed_amount,
                    details={"asset": self.borrow_token},
                )
            )

        if self._swapped_amount > 0:
            positions.append(
                PositionInfo(
                    position_type=PositionType.TOKEN,
                    position_id=f"swap-holding-{self.swap_to_token}-polygon",
                    chain=self.chain,
                    protocol=self.swap_protocol,
                    value_usd=self._swapped_amount,
                    details={"asset": self.swap_to_token},
                )
            )

        return TeardownPositionSummary(
            strategy_id=self.STRATEGY_NAME,
            timestamp=datetime.now(UTC),
            positions=positions,
        )

    def generate_teardown_intents(self, mode: "TeardownMode", market=None) -> list[Intent]:
        from almanak.framework.teardown import TeardownMode

        slippage = Decimal("0.03") if mode == TeardownMode.HARD else Decimal("0.005")
        intents = []

        if self._swapped_amount > 0:
            intents.append(
                Intent.swap(
                    from_token=self.swap_to_token,
                    to_token=self.borrow_token,
                    amount=self._swapped_amount,
                    max_slippage=slippage,
                    protocol=self.swap_protocol,
                )
            )

        if self._borrowed_amount > 0:
            intents.append(
                Intent.repay(
                    token=self.borrow_token,
                    amount=self._borrowed_amount,
                    protocol="aave_v3",
                    repay_full=True,
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
