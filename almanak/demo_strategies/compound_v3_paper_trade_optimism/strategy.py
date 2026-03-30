"""
===============================================================================
DEMO: Compound V3 Paper Trade — Price-Gated USDC Lending on Optimism
===============================================================================

This strategy is the vehicle for testing the paper trading engine
(``almanak strat backtest paper``) with Compound V3 lending on **Optimism**.

This is the **first Compound V3 paper trade on Optimism** and the
**first paper trade using a lending protocol on Optimism**.

Existing paper trade demos use:
  - Aave V3 Arbitrum  (aave_paper_lending)
  - TraderJoe Avalanche (LP, not lending)
  - Compound V3 Base  (compound_paper_trade)

This demo adds Optimism to the paper trading coverage map for lending.

PURPOSE:
--------
1. Validate the paper trading pipeline with lending intents on Optimism:
   - Supply USDC to Compound V3 USDC Comet on Optimism
   - Withdraw on price drop (de-risk signal)
   - PnL journal entries and equity curve generation
2. Exercise Compound V3 SUPPLY / WITHDRAW on Optimism via paper trading.

STRATEGY LOGIC:
---------------
Each tick:
  1. Read ETH price as a simple signal proxy
  2. If ETH price > $2000 and no active supply -> supply USDC to earn yield
  3. If ETH price < $1500 and has active supply -> withdraw to reduce exposure
  4. Otherwise -> hold (collecting Compound V3 lending yield)

USAGE:
------
    # Paper trade for 5 ticks at 60-second intervals
    almanak strat backtest paper start \\
        -s demo_compound_v3_paper_trade_optimism \\
        --chain optimism \\
        --max-ticks 5 \\
        --tick-interval 60 \\
        --foreground

    # Run directly on Anvil (single iteration)
    almanak strat run -d strategies/demo/compound_v3_paper_trade_optimism \\
        --network anvil --once
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

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from almanak.framework.teardown import TeardownMode, TeardownPositionSummary


@almanak_strategy(
    name="demo_compound_v3_paper_trade_optimism",
    description="Paper trading demo — price-gated Compound V3 USDC lending on Optimism",
    version="1.0.0",
    author="Almanak",
    tags=["demo", "paper-trading", "lending", "compound-v3", "optimism", "backtesting"],
    supported_chains=["optimism"],
    default_chain="optimism",
    supported_protocols=["compound_v3"],
    intent_types=["SUPPLY", "WITHDRAW", "HOLD"],
)
class CompoundV3PaperTradeOptimismStrategy(IntentStrategy):
    """Price-gated Compound V3 USDC supply strategy for paper trading on Optimism.

    Configuration (config.json):
        supply_token: Token to supply (default: "USDC")
        supply_amount: Amount to supply (default: "100")
        market: Compound V3 market identifier (default: "usdc")
        price_supply_above: ETH price above which to supply (default: 2000)
        price_withdraw_below: ETH price below which to withdraw (default: 1500)
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.supply_token = str(self.get_config("supply_token", "USDC"))
        self.supply_amount = Decimal(str(self.get_config("supply_amount", "100")))
        self.market = str(self.get_config("market", "usdc"))
        self.price_supply_above = Decimal(str(self.get_config("price_supply_above", "2000")))
        self.price_withdraw_below = Decimal(str(self.get_config("price_withdraw_below", "1500")))

        if self.supply_amount <= 0:
            raise ValueError("supply_amount must be greater than 0")
        if self.price_supply_above < self.price_withdraw_below:
            raise ValueError("price_supply_above must be >= price_withdraw_below")

        self._has_supply = False
        self._supplied_amount = Decimal("0")
        self._ticks_with_supply = 0

        logger.info(
            f"CompoundV3PaperTradeOptimism initialized: token={self.supply_token}, "
            f"amount={self.supply_amount}, market={self.market}, chain={self.chain}"
        )

    def decide(self, market: MarketSnapshot) -> Intent | None:
        """Price-gated supply/withdraw decision for Compound V3 on Optimism."""
        try:
            eth_price = market.price("ETH")
            logger.info(f"ETH price = ${eth_price:.2f}")
        except (ValueError, KeyError, AttributeError) as e:
            logger.warning(f"Could not get ETH price: {e}. Holding.")
            return Intent.hold(reason=f"Price unavailable: {e}")

        balance_error: str | None = None
        has_funds: bool = False
        try:
            supply_bal = market.balance(self.supply_token)
            has_funds = supply_bal >= self.supply_amount
        except (ValueError, KeyError, AttributeError) as e:
            logger.warning(f"Could not get {self.supply_token} balance: {e}.")
            balance_error = str(e)

        if self._has_supply:
            self._ticks_with_supply += 1

            if eth_price < self.price_withdraw_below:
                logger.info(
                    f"ETH ${eth_price:.0f} < ${self.price_withdraw_below} threshold, "
                    f"withdrawing after {self._ticks_with_supply} ticks"
                )
                return self._create_withdraw_intent()

            return Intent.hold(
                reason=(
                    f"Supply active ({self._ticks_with_supply} ticks), "
                    f"ETH=${eth_price:.0f} on Optimism"
                )
            )

        else:
            if eth_price > self.price_supply_above and has_funds:
                logger.info(
                    f"ETH ${eth_price:.0f} > ${self.price_supply_above}, "
                    f"supplying {self.supply_amount} {self.supply_token} on Optimism"
                )
                return self._create_supply_intent()

            reason = []
            if eth_price <= self.price_supply_above:
                reason.append(f"ETH=${eth_price:.0f} below supply threshold")
            if balance_error is not None:
                reason.append(f"balance unavailable: {balance_error}")
            elif not has_funds:
                reason.append("insufficient funds")
            return Intent.hold(reason=f"No supply: {', '.join(reason)}")

    def _create_supply_intent(self) -> Intent:
        return Intent.supply(
            protocol="compound_v3",
            token=self.supply_token,
            amount=self.supply_amount,
            use_as_collateral=False,  # base asset supply, not collateral
            market_id=self.market,
            chain=self.chain,
        )

    def _create_withdraw_intent(self) -> Intent:
        return Intent.withdraw(
            protocol="compound_v3",
            token=self.supply_token,
            amount=self._supplied_amount,
            withdraw_all=True,
            market_id=self.market,
            chain=self.chain,
        )

    def on_intent_executed(self, intent: Any, success: bool, result: Any) -> None:
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
            self._supplied_amount = self.supply_amount
            logger.info(f"Supplied {self.supply_amount} {self.supply_token} to Compound V3 on Optimism")
            add_event(
                TimelineEvent(
                    timestamp=datetime.now(UTC),
                    event_type=TimelineEventType.POSITION_MODIFIED,
                    description=f"Supplied {self.supply_amount} {self.supply_token} to Compound V3 (Optimism)",
                    strategy_id=self.strategy_id,
                    details={
                        "action": "supply",
                        "token": self.supply_token,
                        "amount": str(self.supply_amount),
                        "chain": "optimism",
                    },
                )
            )

        elif intent_type_val == "WITHDRAW":
            self._has_supply = False
            self._supplied_amount = Decimal("0")
            self._ticks_with_supply = 0
            logger.info(f"Withdrew {self.supply_token} from Compound V3 on Optimism")
            add_event(
                TimelineEvent(
                    timestamp=datetime.now(UTC),
                    event_type=TimelineEventType.POSITION_MODIFIED,
                    description=f"Withdrew {self.supply_token} from Compound V3 (Optimism)",
                    strategy_id=self.strategy_id,
                    details={"action": "withdraw", "token": self.supply_token, "chain": "optimism"},
                )
            )

    def get_status(self) -> dict[str, Any]:
        return {
            "strategy": "demo_compound_v3_paper_trade_optimism",
            "chain": self.chain,
            "has_supply": self._has_supply,
            "supplied_amount": str(self._supplied_amount),
            "ticks_with_supply": self._ticks_with_supply,
        }

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

    # Teardown interface
    def supports_teardown(self) -> bool:
        return True

    def get_open_positions(self) -> "TeardownPositionSummary":
        from almanak.framework.teardown import PositionInfo, PositionType, TeardownPositionSummary

        positions: list[PositionInfo] = []

        if self._has_supply and self._supplied_amount > 0:
            positions.append(
                PositionInfo(
                    position_type=PositionType.SUPPLY,
                    position_id=f"compound-v3-{self.supply_token}-optimism",
                    chain=self.chain,
                    protocol="compound_v3",
                    value_usd=self._supplied_amount,  # USDC is 1:1
                    details={"asset": self.supply_token, "amount": str(self._supplied_amount)},
                )
            )

        return TeardownPositionSummary(
            strategy_id=self.strategy_id,
            timestamp=datetime.now(UTC),
            positions=positions,
        )

    def generate_teardown_intents(self, mode: "TeardownMode", market=None) -> list[Intent]:
        if not self._has_supply or self._supplied_amount <= 0:
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
