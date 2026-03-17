"""Aave V3 PnL Lending Strategy on Polygon — Supply WETH, Borrow USDC on Dips.

Polygon variant of the aave_pnl_lending demo. Exercises the PnL backtester
with lending dynamics on Polygon's gas model (MATIC). Same multi-tick logic:

1. Supplies WETH collateral on the first tick
2. Borrows USDC when ETH price drops (buy-the-dip thesis)
3. Repays USDC when ETH price rises (take profit on borrow)
4. Holds otherwise

Run PnL backtest:
    almanak strat backtest pnl -s demo_aave_pnl_lending_polygon \\
        --start 2025-01-01 --end 2025-02-01 \\
        --chain polygon --tokens WETH,USDC
"""

import logging
from datetime import UTC, datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Any

from almanak.framework.api.timeline import TimelineEvent, TimelineEventType, add_event
from almanak.framework.intents import Intent
from almanak.framework.strategies import (
    IntentStrategy,
    MarketSnapshot,
    almanak_strategy,
)

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from almanak.framework.teardown import TeardownMode, TeardownPositionSummary


@almanak_strategy(
    name="demo_aave_pnl_lending_polygon",
    description="Aave V3 lending strategy for PnL backtesting on Polygon",
    version="1.0.0",
    author="Almanak",
    tags=["demo", "backtesting", "lending", "aave-v3", "pnl", "polygon"],
    supported_chains=["polygon"],
    supported_protocols=["aave_v3"],
    intent_types=["SUPPLY", "BORROW", "REPAY", "WITHDRAW", "HOLD"],
    default_chain="polygon",
)
class AavePnLLendingPolygonStrategy(IntentStrategy):
    """Aave V3 lending strategy for PnL backtesting on Polygon.

    State machine: idle -> supplied -> borrowed -> repaid -> supplied (cycle)
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.supply_token = self.get_config("supply_token", "WETH")
        self.borrow_token = self.get_config("borrow_token", "USDC")
        self.supply_amount = Decimal(str(self.get_config("supply_amount", "0.01")))
        self.ltv_target = Decimal(str(self.get_config("ltv_target", "0.4")))
        self.price_drop_threshold = Decimal(str(self.get_config("price_drop_threshold", "0.03")))
        self.price_rise_threshold = Decimal(str(self.get_config("price_rise_threshold", "0.05")))

        self._state = "idle"
        self._previous_stable_state = "idle"
        self._supplied_amount = Decimal("0")
        self._borrowed_amount = Decimal("0")
        self._reference_price: Decimal | None = None
        self._previous_reference_price: Decimal | None = None

        logger.info(
            f"AavePnLLendingPolygonStrategy initialized: "
            f"supply={self.supply_amount} {self.supply_token}, "
            f"borrow_token={self.borrow_token}, "
            f"LTV target={self.ltv_target * 100}%"
        )

    def decide(self, market: MarketSnapshot) -> Intent | None:
        """Make lending decisions based on ETH price movement."""
        try:
            supply_price = market.price(self.supply_token)
        except (ValueError, KeyError) as e:
            logger.warning(f"Could not get {self.supply_token} price: {e}")
            return Intent.hold(reason=f"Price data unavailable for {self.supply_token}: {e}")

        # Step 1: Supply collateral if idle
        if self._state == "idle":
            self._previous_reference_price = self._reference_price
            self._reference_price = supply_price
            self._previous_stable_state = self._state
            self._state = "supplying"
            logger.info(f"SUPPLY {self.supply_amount} {self.supply_token} at ${supply_price:.2f}")
            return Intent.supply(
                protocol="aave_v3",
                token=self.supply_token,
                amount=self.supply_amount,
                use_as_collateral=True,
                chain=self.chain,
            )

        # Step 2: If supplied and price dropped, borrow
        if self._state == "supplied" and self._reference_price is not None:
            price_change = (supply_price - self._reference_price) / self._reference_price

            if price_change <= -self.price_drop_threshold:
                try:
                    borrow_price = market.price(self.borrow_token)
                except (ValueError, KeyError) as e:
                    logger.warning(f"Could not get {self.borrow_token} price: {e}")
                    return Intent.hold(reason=f"Price data unavailable for {self.borrow_token}: {e}")
                collateral_value = self._supplied_amount * supply_price
                borrow_value = collateral_value * self.ltv_target
                borrow_amount = (borrow_value / borrow_price).quantize(Decimal("0.01"))

                if borrow_amount > 0:
                    self._previous_stable_state = self._state
                    self._previous_reference_price = self._reference_price
                    self._state = "borrowing"
                    self._reference_price = supply_price
                    logger.info(
                        f"BORROW {borrow_amount} {self.borrow_token} "
                        f"(price drop {price_change * 100:.1f}%)"
                    )
                    return Intent.borrow(
                        protocol="aave_v3",
                        collateral_token=self.supply_token,
                        collateral_amount=Decimal("0"),
                        borrow_token=self.borrow_token,
                        borrow_amount=borrow_amount,
                        interest_rate_mode="variable",
                        chain=self.chain,
                    )

        # Step 3: If borrowed and price risen, repay
        if self._state == "borrowed" and self._reference_price is not None:
            price_change = (supply_price - self._reference_price) / self._reference_price

            if price_change >= self.price_rise_threshold:
                self._previous_stable_state = self._state
                self._previous_reference_price = self._reference_price
                self._state = "repaying"
                self._reference_price = supply_price
                logger.info(
                    f"REPAY {self._borrowed_amount} {self.borrow_token} "
                    f"(price rise {price_change * 100:.1f}%)"
                )
                return Intent.repay(
                    token=self.borrow_token,
                    amount=self._borrowed_amount,
                    protocol="aave_v3",
                    repay_full=True,
                )

        # Safety: revert stuck transitional states
        if self._state in ("supplying", "borrowing", "repaying"):
            revert_to = self._previous_stable_state
            logger.warning(f"Stuck in '{self._state}', reverting to '{revert_to}'")
            self._state = revert_to

        return Intent.hold(reason=f"Holding (state={self._state}, price=${supply_price:.2f})")

    def on_intent_executed(self, intent: Intent, success: bool, result: Any) -> None:
        intent_type = intent.intent_type.value

        if success:
            if intent_type == "SUPPLY":
                self._state = "supplied"
                self._supplied_amount = self.supply_amount
                add_event(
                    TimelineEvent(
                        timestamp=datetime.now(UTC),
                        event_type=TimelineEventType.POSITION_MODIFIED,
                        description=f"Supplied {self.supply_amount} {self.supply_token}",
                        strategy_id=self.strategy_id,
                        details={"action": "supply", "amount": str(self.supply_amount)},
                    )
                )
            elif intent_type == "BORROW":
                self._state = "borrowed"
                if hasattr(intent, "borrow_amount"):
                    self._borrowed_amount = Decimal(str(intent.borrow_amount))
                add_event(
                    TimelineEvent(
                        timestamp=datetime.now(UTC),
                        event_type=TimelineEventType.POSITION_MODIFIED,
                        description=f"Borrowed {self._borrowed_amount} {self.borrow_token}",
                        strategy_id=self.strategy_id,
                        details={"action": "borrow", "amount": str(self._borrowed_amount)},
                    )
                )
            elif intent_type == "REPAY":
                self._state = "supplied"
                self._borrowed_amount = Decimal("0")
                add_event(
                    TimelineEvent(
                        timestamp=datetime.now(UTC),
                        event_type=TimelineEventType.POSITION_MODIFIED,
                        description=f"Repaid {self.borrow_token}",
                        strategy_id=self.strategy_id,
                        details={"action": "repay"},
                    )
                )
            elif intent_type == "WITHDRAW":
                self._state = "idle"
                self._supplied_amount = Decimal("0")
                self._reference_price = None
                add_event(
                    TimelineEvent(
                        timestamp=datetime.now(UTC),
                        event_type=TimelineEventType.POSITION_MODIFIED,
                        description=f"Withdrew {self.supply_token}",
                        strategy_id=self.strategy_id,
                        details={"action": "withdraw"},
                    )
                )
        else:
            revert_to = self._previous_stable_state
            logger.warning(f"{intent_type} failed, reverting to '{revert_to}'")
            self._state = revert_to
            self._reference_price = self._previous_reference_price

    def get_status(self) -> dict[str, Any]:
        return {
            "strategy": "demo_aave_pnl_lending_polygon",
            "chain": self.chain,
            "state": self._state,
            "supplied_amount": str(self._supplied_amount),
            "borrowed_amount": str(self._borrowed_amount),
            "reference_price": str(self._reference_price) if self._reference_price else None,
        }

    def get_persistent_state(self) -> dict[str, Any]:
        return {
            "state": self._state,
            "previous_stable_state": self._previous_stable_state,
            "supplied_amount": str(self._supplied_amount),
            "borrowed_amount": str(self._borrowed_amount),
            "reference_price": str(self._reference_price) if self._reference_price else None,
        }

    def load_persistent_state(self, state: dict[str, Any]) -> None:
        if "state" in state:
            self._state = state["state"]
        if "previous_stable_state" in state:
            self._previous_stable_state = state["previous_stable_state"]
        if "supplied_amount" in state:
            self._supplied_amount = Decimal(str(state["supplied_amount"]))
        if "borrowed_amount" in state:
            self._borrowed_amount = Decimal(str(state["borrowed_amount"]))
        if "reference_price" in state:
            ref = state["reference_price"]
            self._reference_price = Decimal(str(ref)) if ref is not None else None

    # ── Teardown ──

    def supports_teardown(self) -> bool:
        return True

    def get_open_positions(self) -> "TeardownPositionSummary":
        from almanak.framework.teardown import PositionInfo, PositionType, TeardownPositionSummary

        positions: list[PositionInfo] = []

        if self._supplied_amount > 0:
            try:
                snapshot = self.create_market_snapshot()
                supply_price = snapshot.price(self.supply_token)
            except Exception:  # noqa: BLE001
                supply_price = Decimal("0")
            positions.append(
                PositionInfo(
                    position_type=PositionType.SUPPLY,
                    position_id=f"aave-supply-{self.supply_token}-{self.chain}",
                    chain=self.chain,
                    protocol="aave_v3",
                    value_usd=self._supplied_amount * supply_price,
                    details={"asset": self.supply_token, "amount": str(self._supplied_amount)},
                )
            )

        if self._borrowed_amount > 0:
            positions.append(
                PositionInfo(
                    position_type=PositionType.BORROW,
                    position_id=f"aave-borrow-{self.borrow_token}-{self.chain}",
                    chain=self.chain,
                    protocol="aave_v3",
                    value_usd=self._borrowed_amount,
                    details={"asset": self.borrow_token, "amount": str(self._borrowed_amount)},
                )
            )

        return TeardownPositionSummary(
            strategy_id=self.STRATEGY_NAME,
            timestamp=datetime.now(UTC),
            positions=positions,
        )

    def generate_teardown_intents(self, mode: "TeardownMode", market=None) -> list[Intent]:
        intents = []

        if self._borrowed_amount > 0:
            intents.append(
                Intent.repay(
                    token=self.borrow_token,
                    amount=self._borrowed_amount,
                    protocol="aave_v3",
                    repay_full=True,
                )
            )

        if self._supplied_amount > 0:
            intents.append(
                Intent.withdraw(
                    token=self.supply_token,
                    amount=self._supplied_amount,
                    protocol="aave_v3",
                    withdraw_all=True,
                )
            )

        return intents
