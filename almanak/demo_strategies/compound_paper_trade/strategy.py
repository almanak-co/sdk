"""
===============================================================================
DEMO: Compound V3 Paper Trade — Price-Gated Lending on Base
===============================================================================

This demo strategy is the vehicle for testing the paper trading engine
(``almanak strat backtest paper``) with a **lending** strategy.  Previous
paper trading demos use swap/LP strategies; this exercises the supply/withdraw
intent path and validates PnL tracking for interest-accruing positions.

PURPOSE:
--------
1. Validate the paper trading pipeline with lending intents:
   - Supply intent compilation and execution on Anvil fork
   - Withdraw intent compilation and execution
   - PnL journal entries for supply/withdraw operations
   - Equity curve generation with lending positions
2. Exercise Compound V3 SUPPLY / WITHDRAW on Base via paper trading.

USAGE:
------
    # Paper trade for 5 ticks at 60-second intervals
    almanak strat backtest paper start \\
        -s demo_compound_paper_trade \\
        --chain base \\
        --max-ticks 5 \\
        --tick-interval 60 \\
        --foreground

    # Or run directly on Anvil (single iteration)
    almanak strat run -d strategies/demo/compound_paper_trade \\
        --network anvil --once

STRATEGY LOGIC:
---------------
Each tick:
  1. Read ETH price as a simple signal proxy
  2. If price > $2000 and no supply position -> supply USDC to earn yield
  3. If price < $1500 and has supply position -> withdraw to reduce exposure
  4. Otherwise -> hold
===============================================================================
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
from almanak.framework.utils.log_formatters import format_token_amount_human

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from almanak.framework.teardown import TeardownMode, TeardownPositionSummary


@almanak_strategy(
    name="demo_compound_paper_trade",
    description="Paper trading demo — price-gated Compound V3 lending on Base",
    version="1.0.0",
    author="Almanak",
    tags=["demo", "paper-trading", "lending", "compound-v3", "base", "backtesting"],
    supported_chains=["base"],
    default_chain="base",
    supported_protocols=["compound_v3"],
    intent_types=["SUPPLY", "WITHDRAW", "HOLD"],
)
class CompoundPaperTradeStrategy(IntentStrategy):
    """Price-gated Compound V3 supply strategy for paper trading validation.

    Configuration (config.json):
        supply_token: Token to supply (e.g. "USDC")
        supply_amount: Amount to supply per tick (e.g. "100")
        market: Compound V3 market (e.g. "usdc")
        price_supply_above: ETH price above which to supply (default: 2000)
        price_withdraw_below: ETH price below which to withdraw (default: 1500)
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.supply_token = str(self.get_config("supply_token", "USDC"))
        self.supply_amount = Decimal(str(self.get_config("supply_amount", "100")))
        self.market = str(self.get_config("market", "usdc"))

        # ETH price thresholds as a simple signal
        # (In a real strategy, you'd query on-chain utilization rate)
        self.price_supply_above = Decimal(str(self.get_config("price_supply_above", "2000")))
        self.price_withdraw_below = Decimal(str(self.get_config("price_withdraw_below", "1500")))

        if self.supply_amount <= 0:
            raise ValueError("supply_amount must be greater than 0")
        if self.price_supply_above < self.price_withdraw_below:
            raise ValueError(
                "price_supply_above must be greater than or equal to price_withdraw_below"
            )

        # Internal state
        self._has_supply = False
        self._supplied_amount = Decimal("0")
        self._ticks_with_supply = 0

        logger.info(
            f"CompoundPaperTrade initialized: token={self.supply_token}, "
            f"amount={self.supply_amount}, market={self.market}"
        )

    # =========================================================================
    # MAIN DECISION LOGIC
    # =========================================================================

    def decide(self, market: MarketSnapshot) -> Intent | None:
        """Price-gated supply/withdraw decision."""
        try:
            # Use ETH price as a signal proxy (simple and always available)
            try:
                eth_price = market.price("ETH")
                logger.info(f"ETH price = ${eth_price:.2f}")
            except (ValueError, KeyError, AttributeError) as e:
                logger.warning(f"Could not get ETH price: {e}. Holding until price available.")
                return Intent.hold(reason=f"Price unavailable: {e}")

            # Check balances
            try:
                supply_bal = market.balance(self.supply_token)
                has_funds = supply_bal.balance >= self.supply_amount
            except (ValueError, KeyError):
                has_funds = False

            # Decision logic uses configurable thresholds
            supply_threshold = self.price_supply_above
            withdraw_threshold = self.price_withdraw_below

            if self._has_supply:
                self._ticks_with_supply += 1

                if eth_price < withdraw_threshold:
                    logger.info(
                        f"ETH ${eth_price:.0f} < ${withdraw_threshold} threshold, "
                        f"withdrawing after {self._ticks_with_supply} ticks"
                    )
                    return self._create_withdraw_intent()

                return Intent.hold(
                    reason=f"Supply active ({self._ticks_with_supply} ticks), "
                    f"ETH=${eth_price:.0f}"
                )

            else:
                if eth_price > supply_threshold and has_funds:
                    logger.info(f"ETH ${eth_price:.0f} > ${supply_threshold}, supplying {self.supply_amount} {self.supply_token}")
                    return self._create_supply_intent()

                reason = []
                if eth_price <= supply_threshold:
                    reason.append(f"ETH=${eth_price:.0f} below supply threshold")
                if not has_funds:
                    reason.append("insufficient funds")
                return Intent.hold(reason=f"No supply: {', '.join(reason)}")

        except Exception as e:
            logger.exception(f"Error in decide(): {e}")
            return Intent.hold(reason=f"Error: {e}")

    # =========================================================================
    # INTENT CREATION
    # =========================================================================

    def _create_supply_intent(self) -> Intent:
        """Create SUPPLY intent for Compound V3."""
        logger.info(
            f"SUPPLY: {format_token_amount_human(self.supply_amount, self.supply_token)} "
            f"to Compound V3 ({self.market} market)"
        )
        return Intent.supply(
            protocol="compound_v3",
            token=self.supply_token,
            amount=self.supply_amount,
            use_as_collateral=False,  # Base asset supply, not collateral
            chain=self.chain,
        )

    def _create_withdraw_intent(self) -> Intent:
        """Create WITHDRAW intent for Compound V3."""
        logger.info(
            f"WITHDRAW: {format_token_amount_human(self._supplied_amount, self.supply_token)} "
            f"from Compound V3 ({self.market} market)"
        )
        return Intent.withdraw(
            protocol="compound_v3",
            token=self.supply_token,
            amount=self._supplied_amount,
            withdraw_all=True,
            chain=self.chain,
        )

    # =========================================================================
    # LIFECYCLE HOOKS
    # =========================================================================

    def on_intent_executed(self, intent: Any, success: bool, result: Any) -> None:
        """Track supply position state from execution results."""
        if not success:
            logger.warning(f"Intent failed: {getattr(intent, 'intent_type', 'unknown')}")
            return

        intent_type = getattr(intent, "intent_type", None)
        if intent_type is None:
            return

        intent_type_val = intent_type.value if hasattr(intent_type, "value") else str(intent_type)

        if intent_type_val == "SUPPLY":
            self._has_supply = True
            self._ticks_with_supply = 0
            self._supplied_amount += self.supply_amount
            logger.info(f"Supplied {self.supply_amount} {self.supply_token} to Compound V3")
            add_event(
                TimelineEvent(
                    timestamp=datetime.now(UTC),
                    event_type=TimelineEventType.POSITION_MODIFIED,
                    description=f"Supplied {self.supply_amount} {self.supply_token} to Compound V3",
                    strategy_id=self.strategy_id,
                    details={"action": "supply", "token": self.supply_token, "amount": str(self.supply_amount)},
                )
            )

        elif intent_type_val == "WITHDRAW":
            self._has_supply = False
            self._supplied_amount = Decimal("0")
            self._ticks_with_supply = 0
            logger.info(f"Withdrew {self.supply_token} from Compound V3")
            add_event(
                TimelineEvent(
                    timestamp=datetime.now(UTC),
                    event_type=TimelineEventType.POSITION_MODIFIED,
                    description=f"Withdrew {self.supply_token} from Compound V3",
                    strategy_id=self.strategy_id,
                    details={"action": "withdraw", "token": self.supply_token},
                )
            )

    # =========================================================================
    # STATE PERSISTENCE
    # =========================================================================

    def get_persistent_state(self) -> dict[str, Any]:
        return {
            "has_supply": self._has_supply,
            "supplied_amount": str(self._supplied_amount),
            "ticks_with_supply": self._ticks_with_supply,
        }

    def load_persistent_state(self, state: dict[str, Any]) -> None:
        if "has_supply" in state:
            self._has_supply = bool(state["has_supply"])
        if "supplied_amount" in state:
            self._supplied_amount = Decimal(str(state["supplied_amount"]))
        if "ticks_with_supply" in state:
            self._ticks_with_supply = int(state["ticks_with_supply"])
        logger.info(
            f"Restored state: has_supply={self._has_supply}, "
            f"supplied={self._supplied_amount}, ticks={self._ticks_with_supply}"
        )

    # =========================================================================
    # TEARDOWN
    # =========================================================================

    def get_open_positions(self) -> "TeardownPositionSummary":
        from almanak.framework.teardown import PositionInfo, PositionType, TeardownPositionSummary

        positions = []
        if self._has_supply:
            positions.append(
                PositionInfo(
                    position_type=PositionType.SUPPLY,
                    position_id=f"compound-v3-supply-{self.supply_token}-{self.market}",
                    chain=self.chain,
                    protocol="compound_v3",
                    # Approximation: assumes stablecoin supply (USDC ~$1).
                    # A production strategy should query market for real USD value.
                    value_usd=self._supplied_amount,
                    details={
                        "token": self.supply_token,
                        "market": self.market,
                        "amount": str(self._supplied_amount),
                    },
                )
            )
        return TeardownPositionSummary(
            strategy_id=self.strategy_id,
            timestamp=datetime.now(UTC),
            positions=positions,
        )

    def generate_teardown_intents(self, mode: "TeardownMode", market=None) -> list[Intent]:
        intents = []
        if self._has_supply:
            intents.append(self._create_withdraw_intent())
        return intents
