"""Aave V3 Sweep Lending on Polygon — Rate-Threshold-Based Lending.

Polygon variant of the Aave V3 parameter sweep demo. Exercises the sweep
engine with Aave V3 lending-specific parameters on Polygon, which has
different gas economics (MATIC) than Arbitrum.

Aave V3 on Polygon has PnL backtesting (demo: aave_pnl_lending_polygon) but
never had a parameter sweep. This is the first Aave V3 parameter sweep on
Polygon, validating the sweep engine with lending thresholds on a second chain.

SWEEPABLE PARAMETERS:
    supply_rate_threshold: Price volatility % to gate re-supply (default: 4.0)
    borrow_rate_threshold: Price volatility % ceiling for borrowing (default: 6.0)
    ltv_target: Target loan-to-value ratio 0.0-1.0 (default: 0.4)
    supply_amount: Amount of WETH to deposit (default: 0.01)

USAGE:
    # Sweep supply/borrow rate thresholds (4x4 = 16 combinations)
    almanak strat backtest sweep -s demo_aave_sweep_lending_polygon \\
        --start 2025-01-01 --end 2025-06-01 \\
        --param "supply_rate_threshold:2.0,4.0,6.0,8.0" \\
        --param "borrow_rate_threshold:3.0,5.0,7.0,10.0" \\
        --chain polygon --tokens WETH,USDC

Kitchen Loop — VIB-2053
"""

import logging
from datetime import UTC, datetime
from decimal import ROUND_DOWN, Decimal
from typing import TYPE_CHECKING, Any

from almanak.framework.api.timeline import TimelineEvent, TimelineEventType, add_event
from almanak.framework.intents import Intent
from almanak.framework.strategies import IntentStrategy, MarketSnapshot, almanak_strategy

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from almanak.framework.teardown import TeardownMode, TeardownPositionSummary


@almanak_strategy(
    name="demo_aave_sweep_lending_polygon",
    description="Parameter sweep demo — Aave V3 lending with sweepable rate thresholds on Polygon",
    version="1.0.0",
    author="Almanak",
    tags=["demo", "sweep", "lending", "aave-v3", "polygon", "backtesting"],
    supported_chains=["polygon"],
    supported_protocols=["aave_v3"],
    intent_types=["SUPPLY", "BORROW", "REPAY", "WITHDRAW", "HOLD"],
    default_chain="polygon",
)
class AaveSweepLendingPolygonStrategy(IntentStrategy):
    """Aave V3 lending strategy with sweep-optimizable rate thresholds on Polygon.

    Config:
        supply_token: Token to supply as collateral (default: WETH)
        borrow_token: Token to borrow (default: USDC)
        supply_amount: Amount to deposit (sweepable, default: 0.01)
        supply_rate_threshold: Volatility % to trigger supply (sweepable, default: 4.0)
        borrow_rate_threshold: Volatility % ceiling for borrowing (sweepable, default: 6.0)
        ltv_target: Target LTV for borrows (sweepable, default: 0.4)
        max_borrow_cycles: Max supply/borrow cycles (default: 5)
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.supply_token = str(self.get_config("supply_token", "WETH"))
        self.borrow_token = str(self.get_config("borrow_token", "USDC"))

        self.supply_amount = Decimal(str(self.get_config("supply_amount", "0.01")))
        self.supply_rate_threshold = Decimal(str(self.get_config("supply_rate_threshold", "4.0")))
        self.borrow_rate_threshold = Decimal(str(self.get_config("borrow_rate_threshold", "6.0")))
        self.ltv_target = Decimal(str(self.get_config("ltv_target", "0.4")))
        self.max_borrow_cycles = int(self.get_config("max_borrow_cycles", 5))

        if self.supply_rate_threshold < 0:
            raise ValueError(f"supply_rate_threshold must be >= 0, got {self.supply_rate_threshold}")
        if self.borrow_rate_threshold < 0:
            raise ValueError(f"borrow_rate_threshold must be >= 0, got {self.borrow_rate_threshold}")
        if not Decimal("0") < self.ltv_target < Decimal("1"):
            raise ValueError(f"ltv_target must be between 0 and 1 exclusive, got {self.ltv_target}")

        self._VALID_STATES = frozenset(
            {"idle", "supplying", "supplied", "borrowing", "borrowed", "repaying"}
        )
        self._state = "idle"
        self._previous_stable_state = "idle"
        self._supplied_amount = Decimal("0")
        self._borrowed_amount = Decimal("0")
        self._borrow_cycles = 0
        self._tick_count = 0
        self._reference_price: Decimal | None = None
        self._previous_reference_price: Decimal | None = None

        logger.info(
            f"AaveSweepLendingPolygon initialized: "
            f"supply={self.supply_amount} {self.supply_token}, "
            f"borrow_token={self.borrow_token}, "
            f"supply_rate_threshold={self.supply_rate_threshold}%, "
            f"borrow_rate_threshold={self.borrow_rate_threshold}%, "
            f"LTV target={self.ltv_target * 100}%, "
            f"max_cycles={self.max_borrow_cycles}"
        )

    def decide(self, market: MarketSnapshot) -> Intent | None:
        """Make lending decisions based on supply/borrow rate thresholds.

        Uses price volatility as a proxy for Aave rates (the PnL backtester
        doesn't provide real-time protocol rates). This lets the sweep engine
        produce meaningfully different execution paths for different threshold
        values.
        """
        self._tick_count += 1

        try:
            supply_price = market.price(self.supply_token)
        except (ValueError, KeyError) as e:
            return Intent.hold(reason=f"Price data unavailable for {self.supply_token}: {e}")

        if self._state in ("supplying", "borrowing", "repaying"):
            return Intent.hold(reason=f"Waiting for {self._state} to confirm")

        # Step 1: Supply collateral if idle
        if self._state == "idle":
            if self._reference_price is not None:
                price_change_pct = abs(
                    (supply_price - self._reference_price) / self._reference_price * 100
                )
                if price_change_pct < self.supply_rate_threshold:
                    return Intent.hold(
                        reason=f"Price volatility {price_change_pct:.1f}% < supply threshold {self.supply_rate_threshold}%"
                    )
            self._previous_reference_price = self._reference_price
            self._reference_price = supply_price
            self._previous_stable_state = self._state
            self._state = "supplying"
            logger.info(
                f"[tick {self._tick_count}] SUPPLY {self.supply_amount} {self.supply_token} "
                f"at ${supply_price:.2f}"
            )
            return Intent.supply(
                protocol="aave_v3",
                token=self.supply_token,
                amount=self.supply_amount,
                use_as_collateral=True,
                chain=self.chain,
            )

        # Step 2: Borrow when market is stable
        if self._state == "supplied":
            if self._borrow_cycles >= self.max_borrow_cycles:
                return Intent.hold(
                    reason=f"Max borrow cycles ({self.max_borrow_cycles}) reached"
                )

            if self._reference_price is None:
                # Establish baseline after repay — skip one tick before borrowing
                self._reference_price = supply_price
                return Intent.hold(reason="Establishing price baseline after repay")

            if self._reference_price > 0:
                price_change_pct = abs(
                    (supply_price - self._reference_price) / self._reference_price * 100
                )
                if price_change_pct > self.borrow_rate_threshold:
                    return Intent.hold(
                        reason=f"Price volatility {price_change_pct:.1f}% > borrow threshold {self.borrow_rate_threshold}%"
                    )

            try:
                borrow_price = market.price(self.borrow_token)
            except (ValueError, KeyError) as e:
                return Intent.hold(reason=f"Price data unavailable for {self.borrow_token}: {e}")

            if borrow_price <= 0:
                return Intent.hold(reason=f"Invalid {self.borrow_token} price: {borrow_price}")

            collateral_value = self._supplied_amount * supply_price
            borrow_value = collateral_value * self.ltv_target
            borrow_amount = (borrow_value / borrow_price).quantize(Decimal("0.01"), rounding=ROUND_DOWN)

            if borrow_amount > 0:
                self._previous_stable_state = self._state
                self._previous_reference_price = self._reference_price
                self._state = "borrowing"
                self._reference_price = supply_price
                logger.info(
                    f"[tick {self._tick_count}] BORROW {borrow_amount} {self.borrow_token} "
                    f"(LTV target={self.ltv_target * 100}%)"
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

            return Intent.hold(reason="Borrow amount too small")

        # Step 3: Repay when price moves significantly
        if self._state == "borrowed":
            if self._reference_price is not None and self._reference_price > 0:
                price_change_pct = abs(
                    (supply_price - self._reference_price) / self._reference_price * 100
                )
                repay_threshold = self.borrow_rate_threshold * Decimal("1.5")
                if price_change_pct < repay_threshold:
                    return Intent.hold(
                        reason=f"Price volatility {price_change_pct:.1f}% < repay threshold {repay_threshold:.1f}%"
                    )

            self._previous_stable_state = self._state
            self._state = "repaying"
            logger.info(
                f"[tick {self._tick_count}] REPAY {self._borrowed_amount} {self.borrow_token} "
                f"(cycle {self._borrow_cycles}/{self.max_borrow_cycles})"
            )
            return Intent.repay(
                token=self.borrow_token,
                amount=self._borrowed_amount,
                protocol="aave_v3",
                repay_full=True,
            )

        return Intent.hold(reason=f"Holding (state={self._state}, tick={self._tick_count})")

    def on_intent_executed(self, intent: Intent, success: bool, result: Any) -> None:
        intent_type = intent.intent_type.value

        if success:
            if intent_type == "SUPPLY":
                self._state = "supplied"
                self._previous_stable_state = "supplied"
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
                self._previous_stable_state = "borrowed"
                self._borrow_cycles += 1
                if hasattr(intent, "borrow_amount"):
                    self._borrowed_amount = Decimal(str(intent.borrow_amount))
                add_event(
                    TimelineEvent(
                        timestamp=datetime.now(UTC),
                        event_type=TimelineEventType.POSITION_MODIFIED,
                        description=f"Borrowed {self._borrowed_amount} {self.borrow_token}",
                        strategy_id=self.strategy_id,
                        details={"action": "borrow", "amount": str(self._borrowed_amount), "cycle": self._borrow_cycles},
                    )
                )
            elif intent_type == "REPAY":
                self._state = "supplied"
                self._previous_stable_state = "supplied"
                self._borrowed_amount = Decimal("0")
                # Reset reference price so the supplied branch compares against
                # the current market level, not the stale borrow-entry price.
                self._reference_price = None
                add_event(
                    TimelineEvent(
                        timestamp=datetime.now(UTC),
                        event_type=TimelineEventType.POSITION_MODIFIED,
                        description=f"Repaid {self.borrow_token} (cycle {self._borrow_cycles})",
                        strategy_id=self.strategy_id,
                        details={"action": "repay", "cycle": self._borrow_cycles},
                    )
                )
            elif intent_type == "WITHDRAW":
                self._state = "idle"
                self._previous_stable_state = "idle"
                self._supplied_amount = Decimal("0")
        else:
            revert_to = self._previous_stable_state
            logger.warning(f"{intent_type} failed, reverting to '{revert_to}'")
            self._state = revert_to
            if self._previous_reference_price is not None:
                self._reference_price = self._previous_reference_price
            elif intent_type == "SUPPLY" and revert_to == "idle":
                # First supply failed — clear stale reference price so idle branch
                # retries immediately instead of waiting for volatility threshold.
                self._reference_price = None

    def get_status(self) -> dict[str, Any]:
        return {
            "strategy": "demo_aave_sweep_lending_polygon",
            "chain": self.chain,
            "state": self._state,
            "supplied_amount": str(self._supplied_amount),
            "borrowed_amount": str(self._borrowed_amount),
            "borrow_cycles": self._borrow_cycles,
            "tick_count": self._tick_count,
            "supply_rate_threshold": str(self.supply_rate_threshold),
            "borrow_rate_threshold": str(self.borrow_rate_threshold),
            "ltv_target": str(self.ltv_target),
        }

    def get_persistent_state(self) -> dict[str, Any]:
        return {
            "state": self._state,
            "previous_stable_state": self._previous_stable_state,
            "supplied_amount": str(self._supplied_amount),
            "borrowed_amount": str(self._borrowed_amount),
            "borrow_cycles": self._borrow_cycles,
            "tick_count": self._tick_count,
            "reference_price": str(self._reference_price) if self._reference_price is not None else None,
        }

    def load_persistent_state(self, state: dict[str, Any]) -> None:
        if "state" in state:
            restored = str(state["state"])
            self._state = restored if restored in self._VALID_STATES else "idle"
        if "previous_stable_state" in state:
            restored_prev = str(state["previous_stable_state"])
            self._previous_stable_state = restored_prev if restored_prev in self._VALID_STATES else "idle"
        if "supplied_amount" in state:
            self._supplied_amount = Decimal(str(state["supplied_amount"]))
        if "borrowed_amount" in state:
            self._borrowed_amount = Decimal(str(state["borrowed_amount"]))
        if "borrow_cycles" in state:
            self._borrow_cycles = int(state["borrow_cycles"])
        if "tick_count" in state:
            self._tick_count = int(state["tick_count"])
        if state.get("reference_price") is not None:
            self._reference_price = Decimal(str(state["reference_price"]))

    def supports_teardown(self) -> bool:
        return True

    def get_open_positions(self) -> "TeardownPositionSummary":
        from almanak.framework.teardown import PositionInfo, PositionType, TeardownPositionSummary

        positions = []
        if self._supplied_amount > 0:
            positions.append(
                PositionInfo(
                    position_type=PositionType.SUPPLY,
                    position_id=f"aave-supply-{self.supply_token}-polygon",
                    chain=self.chain,
                    protocol="aave_v3",
                    value_usd=Decimal("0"),
                    details={"token": self.supply_token, "amount": str(self._supplied_amount)},
                )
            )
        if self._borrowed_amount > 0:
            positions.append(
                PositionInfo(
                    position_type=PositionType.BORROW,
                    position_id=f"aave-borrow-{self.borrow_token}-polygon",
                    chain=self.chain,
                    protocol="aave_v3",
                    value_usd=Decimal("0"),
                    details={"token": self.borrow_token, "amount": str(self._borrowed_amount)},
                )
            )
        return TeardownPositionSummary(
            strategy_id=self.strategy_id,
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
                    protocol="aave_v3",
                    token=self.supply_token,
                    amount=self._supplied_amount,
                    withdraw_all=True,
                    chain=self.chain,
                )
            )
        return intents
