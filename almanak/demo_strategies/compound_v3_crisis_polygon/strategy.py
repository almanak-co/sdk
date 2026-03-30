"""
===============================================================================
Compound V3 Crisis Scenario Backtest — Lending Under Stress on Polygon
===============================================================================

Stress-tests Compound V3 USDC lending under historical crisis conditions
on Polygon. First lending strategy used with crisis scenario backtesting --
all prior crisis backtests were swap or LP strategies.

CRISIS PARAMETERS:
- Higher withdrawal sensitivity (15% drawdown threshold) to protect capital
- Automatic re-supply when drawdown recovers below 8%
- Longer max hold (50 ticks) since lending is more passive than trading

USAGE:
------
    # Run against predefined crisis scenario
    almanak strat backtest scenario \
        -s demo_compound_v3_crisis_polygon \
        --scenario ftx_collapse \
        --chain polygon \
        --tokens USDC.e,MATIC \
        --initial-capital 10000

    # Run against all 3 predefined scenarios
    almanak strat backtest scenario \
        -s demo_compound_v3_crisis_polygon \
        --scenario black_thursday \
        --chain polygon --tokens USDC.e,MATIC

    almanak strat backtest scenario \
        -s demo_compound_v3_crisis_polygon \
        --scenario terra_collapse \
        --chain polygon --tokens USDC.e,MATIC

    almanak strat backtest scenario \
        -s demo_compound_v3_crisis_polygon \
        --scenario ftx_collapse \
        --chain polygon --tokens USDC.e,MATIC

KEY METRICS TO WATCH:
- Max drawdown during crisis (lending has lower IL but protocol risk)
- Number of withdraw/re-supply cycles during crisis
- Capital preservation vs hold-only baseline

===============================================================================
"""

import logging
from datetime import UTC, datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Any

from almanak.framework.data import PriceUnavailableError
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
    name="demo_compound_v3_crisis_polygon",
    description="Crisis scenario stress test -- Compound V3 USDC lending on Polygon",
    version="1.0.0",
    author="Almanak",
    tags=["demo", "crisis", "scenario-backtest", "lending", "compound-v3", "polygon", "backtesting"],
    supported_chains=["polygon"],
    supported_protocols=["compound_v3"],
    intent_types=["SUPPLY", "WITHDRAW", "HOLD"],
    default_chain="polygon",
)
class CompoundV3CrisisPolygonStrategy(IntentStrategy):
    """Compound V3 lending strategy tuned for crisis scenario backtesting on Polygon.

    Supplies USDC to Compound V3 and monitors portfolio drawdown during
    crisis periods. Withdraws when drawdown exceeds threshold, re-supplies
    when conditions stabilize. Uses price-based drawdown detection since
    lending rate data is typically unavailable in the backtester context.

    Configuration (config.json):
        supply_token: Token to supply (default: USDC.e)
        supply_amount: Amount to supply (default: 10000)
        market: Compound V3 market identifier (default: usdc_e)
        withdraw_drawdown_threshold: Portfolio drawdown to trigger withdrawal (default: 0.15 = 15%)
        resupply_recovery_threshold: Drawdown recovery to trigger re-supply (default: 0.08 = 8%)
        max_hold_ticks: Max ticks to hold before forced withdrawal (default: 50)
        force_entry_if_no_rate: Supply on first tick without rate data (default: True for backtest)
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)

        self.supply_token = self.get_config("supply_token", "USDC.e")
        self.supply_amount = Decimal(str(self.get_config("supply_amount", "10000")))
        self.market = self.get_config("market", "usdc_e")
        self.withdraw_drawdown_threshold = Decimal(
            str(self.get_config("withdraw_drawdown_threshold", "0.15"))
        )
        self.resupply_recovery_threshold = Decimal(
            str(self.get_config("resupply_recovery_threshold", "0.08"))
        )
        self.max_hold_ticks = int(self.get_config("max_hold_ticks", 50))

        raw_force = self.get_config("force_entry_if_no_rate", True)
        if isinstance(raw_force, bool):
            self.force_entry_if_no_rate = raw_force
        elif isinstance(raw_force, str):
            self.force_entry_if_no_rate = raw_force.strip().lower() in {"1", "true", "yes", "on"}
        else:
            self.force_entry_if_no_rate = bool(raw_force)

        # State machine: idle -> supplying -> supplied -> withdrawing -> idle (cycle)
        self._state = "idle"
        self._previous_stable_state = "idle"
        self._supplied_amount = Decimal("0")
        self._ticks_held = 0
        self._peak_value = Decimal("0")
        self._cycle_count = 0
        self._last_market_timestamp: datetime | None = None

        logger.info(
            "CompoundV3CrisisPolygonStrategy initialized: "
            "supply=%s %s, market=%s, withdraw_threshold=%.0f%%, resupply_threshold=%.0f%%",
            self.supply_amount,
            self.supply_token,
            self.market,
            float(self.withdraw_drawdown_threshold) * 100,
            float(self.resupply_recovery_threshold) * 100,
        )

    def decide(self, market: MarketSnapshot) -> Intent | None:
        """Make supply/withdraw decisions based on portfolio drawdown."""
        self._last_market_timestamp = getattr(market, "timestamp", None)

        # Get MATIC price for drawdown monitoring
        matic_price = self._get_matic_price(market)

        if self._state == "idle":
            if self._cycle_count == 0:
                # First cycle: supply immediately
                pass
            elif matic_price is not None and self._peak_value > 0:
                # After a withdrawal: only re-supply once drawdown recovers
                drawdown_from_peak = (self._peak_value - matic_price) / self._peak_value
                if drawdown_from_peak >= self.resupply_recovery_threshold:
                    return Intent.hold(
                        reason=f"Idle — drawdown {float(drawdown_from_peak) * 100:.1f}% "
                        f"still above {float(self.resupply_recovery_threshold) * 100:.0f}% recovery threshold"
                    )
            else:
                # No price data after a withdrawal — hold until we can verify recovery
                return Intent.hold(reason="Idle — waiting for price data to check recovery")

            logger.info(
                "SUPPLY %s %s to Compound V3 %s (cycle #%d)",
                self.supply_amount,
                self.supply_token,
                self.market,
                self._cycle_count + 1,
            )
            self._previous_stable_state = self._state
            self._state = "supplying"
            return Intent.supply(
                protocol="compound_v3",
                token=self.supply_token,
                amount=self.supply_amount,
                market_id=self.market,
                chain=self.chain,
            )

        if self._state == "supplied":
            self._ticks_held += 1

            # Track peak and compute drawdown
            if matic_price is not None:
                if matic_price > self._peak_value:
                    self._peak_value = matic_price
                drawdown = (
                    (self._peak_value - matic_price) / self._peak_value
                    if self._peak_value > 0
                    else Decimal("0")
                )
            else:
                drawdown = Decimal("0")

            # Withdraw on crisis drawdown or max hold
            should_withdraw = (
                drawdown >= self.withdraw_drawdown_threshold
                or self._ticks_held >= self.max_hold_ticks
            )

            if should_withdraw:
                reason = (
                    f"Drawdown {float(drawdown) * 100:.1f}% >= {float(self.withdraw_drawdown_threshold) * 100:.0f}% threshold"
                    if drawdown >= self.withdraw_drawdown_threshold
                    else f"Max hold ticks ({self.max_hold_ticks}) reached"
                )
                logger.info(
                    "WITHDRAW %s %s from Compound V3 %s (%s, cycle #%d)",
                    self._supplied_amount,
                    self.supply_token,
                    self.market,
                    reason,
                    self._cycle_count,
                )
                self._previous_stable_state = self._state
                self._state = "withdrawing"
                return Intent.withdraw(
                    protocol="compound_v3",
                    token=self.supply_token,
                    amount=self._supplied_amount,
                    withdraw_all=True,
                    market_id=self.market,
                    chain=self.chain,
                )

            drawdown_str = f"{float(drawdown) * 100:.1f}%" if matic_price else "N/A"
            return Intent.hold(
                reason=f"Holding supply (ticks={self._ticks_held}/{self.max_hold_ticks}, drawdown={drawdown_str})"
            )

        if self._state in ("supplying", "withdrawing"):
            # Auto-advance stuck transitional states (PnL backtester doesn't call on_intent_executed)
            previous = self._state
            if self._state == "supplying":
                self._state = "supplied"
                self._supplied_amount = self.supply_amount
                self._ticks_held = 0
            else:
                self._state = "idle"
                self._supplied_amount = Decimal("0")
                self._cycle_count += 1
            logger.warning("Stuck in '%s', auto-advancing to '%s'", previous, self._state)
            return Intent.hold(reason=f"Auto-advanced from {previous} to {self._state}")

        return Intent.hold(reason=f"Unexpected state: {self._state}")

    def _get_matic_price(self, market: MarketSnapshot) -> Decimal | None:
        """Get MATIC price for drawdown tracking."""
        for token in ("MATIC", "WMATIC", "POL"):
            try:
                return market.price(token)
            except (PriceUnavailableError, ValueError, KeyError, AttributeError):
                continue
        return None

    def on_intent_executed(self, intent: Intent, success: bool, result: Any = None) -> None:
        """Track execution results and advance state machine."""
        intent_type = intent.intent_type.value

        if success:
            if intent_type == "SUPPLY":
                self._state = "supplied"
                self._supplied_amount = self.supply_amount
                self._ticks_held = 0
                logger.info("SUPPLY confirmed: %s %s -> state=supplied", self.supply_amount, self.supply_token)

            elif intent_type == "WITHDRAW":
                self._state = "idle"
                self._supplied_amount = Decimal("0")
                self._cycle_count += 1
                # Preserve _peak_value so re-entry can check recovery threshold
                logger.info("WITHDRAW confirmed: cycle #%d complete -> state=idle", self._cycle_count)
        else:
            revert_to = self._previous_stable_state
            logger.warning("%s failed, reverting to '%s'", intent_type, revert_to)
            self._state = revert_to

    def get_status(self) -> dict[str, Any]:
        return {
            "strategy": "demo_compound_v3_crisis_polygon",
            "chain": self.chain,
            "state": self._state,
            "supplied_amount": str(self._supplied_amount),
            "ticks_held": self._ticks_held,
            "peak_value": str(self._peak_value),
            "cycle_count": self._cycle_count,
        }

    def get_persistent_state(self) -> dict[str, Any]:
        return {
            "state": self._state,
            "previous_stable_state": self._previous_stable_state,
            "supplied_amount": str(self._supplied_amount),
            "ticks_held": self._ticks_held,
            "peak_value": str(self._peak_value),
            "cycle_count": self._cycle_count,
        }

    def load_persistent_state(self, state: dict[str, Any]) -> None:
        if "state" in state:
            self._state = state["state"]
        if "previous_stable_state" in state:
            self._previous_stable_state = state["previous_stable_state"]
        if "supplied_amount" in state:
            self._supplied_amount = Decimal(str(state["supplied_amount"]))
        if "ticks_held" in state:
            self._ticks_held = int(state["ticks_held"])
        if "peak_value" in state:
            self._peak_value = Decimal(str(state["peak_value"]))
        if "cycle_count" in state:
            self._cycle_count = int(state["cycle_count"])

    # =========================================================================
    # TEARDOWN
    # =========================================================================

    def get_open_positions(self) -> "TeardownPositionSummary":
        from almanak.framework.teardown import PositionInfo, PositionType, TeardownPositionSummary

        positions: list[PositionInfo] = []

        if self._supplied_amount > 0:
            positions.append(
                PositionInfo(
                    position_type=PositionType.SUPPLY,
                    position_id=f"compound-v3-supply-{self.supply_token}-{self.chain}",
                    chain=self.chain,
                    protocol="compound_v3",
                    value_usd=self._supplied_amount,
                    details={"asset": self.supply_token, "amount": str(self._supplied_amount), "market": self.market},
                )
            )

        return TeardownPositionSummary(
            strategy_id=getattr(self, "strategy_id", None) or "demo_compound_v3_crisis_polygon",
            timestamp=self._last_market_timestamp or datetime.now(UTC),
            positions=positions,
        )

    def generate_teardown_intents(self, mode: "TeardownMode", market=None) -> list[Intent]:
        """Withdraw supplied USDC from Compound V3."""
        if self._supplied_amount <= 0:
            return []

        return [
            Intent.withdraw(
                protocol="compound_v3",
                token=self.supply_token,
                amount=self._supplied_amount,
                withdraw_all=True,
                market_id=self.market,
                chain=self.chain,
            )
        ]
