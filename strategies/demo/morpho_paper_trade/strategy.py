"""Morpho Blue Paper Trade Strategy - Supply wstETH, Borrow USDC.

This strategy is the vehicle for testing the paper trading engine
(``almanak strat backtest paper``) with Morpho Blue lending on Ethereum.

PURPOSE:
--------
1. Validate the paper trading pipeline with lending intents:
   - Anvil fork management with Morpho Blue contracts
   - Supply/borrow/repay execution on forked Ethereum
   - PnL journal entries for lending positions
   - Interest accrual tracking across paper trading ticks
2. Exercise Morpho Blue SUPPLY / BORROW / REPAY on Ethereum via paper trading.

STRATEGY LOGIC:
---------------
Each tick:
  1. If no position: supply wstETH collateral to Morpho Blue market
  2. If supplied and ETH price dropped: borrow USDC (buy-the-dip leverage)
  3. If borrowed and ETH price recovered: repay USDC (de-leverage)
  4. Otherwise: hold

USAGE:
------
    # Paper trade for 5 ticks at 60-second intervals
    almanak strat backtest paper start \\
        -s demo_morpho_paper_trade \\
        --chain ethereum \\
        --max-ticks 5 \\
        --tick-interval 60 \\
        --foreground

    # Run directly on Anvil (single iteration)
    almanak strat run -d strategies/demo/morpho_paper_trade \\
        --network anvil --once
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

# Default Morpho Blue wstETH/USDC market on Ethereum
DEFAULT_MARKET_ID = "0xb323495f7e4148be5643a4ea4a8221eef163e4bccfdedc2a6f4696baacbc86cc"


@almanak_strategy(
    name="demo_morpho_paper_trade",
    description="Paper trading demo - Morpho Blue lending on Ethereum",
    version="1.0.0",
    author="Almanak",
    tags=["demo", "paper-trading", "lending", "morpho", "ethereum", "backtesting"],
    supported_chains=["ethereum"],
    supported_protocols=["morpho_blue"],
    intent_types=["SUPPLY", "BORROW", "REPAY", "HOLD"],
)
class MorphoPaperTradeStrategy(IntentStrategy):
    """Morpho Blue lending strategy for paper trading validation.

    Configuration (config.json):
        market_id: Morpho Blue market identifier
        collateral_token: Token to supply as collateral (default: "wstETH")
        borrow_token: Token to borrow (default: "USDC")
        collateral_amount: Amount to supply (default: "0.05")
        ltv_target: Target LTV for borrows (default: 0.5)
        price_drop_pct: Price drop to trigger borrow (default: 0.02 = 2%)
        price_rise_pct: Price rise to trigger repay (default: 0.04 = 4%)
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.market_id = self.get_config("market_id", DEFAULT_MARKET_ID)
        self.collateral_token = self.get_config("collateral_token", "wstETH")
        self.borrow_token = self.get_config("borrow_token", "USDC")
        self.collateral_amount = Decimal(str(self.get_config("collateral_amount", "0.05")))
        self.ltv_target = Decimal(str(self.get_config("ltv_target", "0.5")))
        self.price_drop_pct = Decimal(str(self.get_config("price_drop_pct", "0.02")))
        self.price_rise_pct = Decimal(str(self.get_config("price_rise_pct", "0.04")))

        # State machine
        self._state = "idle"
        self._previous_stable_state = "idle"
        self._supplied_amount = Decimal("0")
        self._borrowed_amount = Decimal("0")
        self._reference_price: Decimal | None = None

        logger.info(
            f"MorphoPaperTradeStrategy initialized: "
            f"supply={self.collateral_amount} {self.collateral_token}, "
            f"borrow_token={self.borrow_token}, "
            f"market_id={self.market_id[:16]}..."
        )

    def decide(self, market: MarketSnapshot) -> Intent | None:
        """Make lending decisions for paper trading ticks.

        Each tick evaluates collateral price movement and decides whether
        to supply, borrow, repay, or hold.
        """
        try:
            collateral_price = market.price(self.collateral_token)
        except (ValueError, KeyError):
            collateral_price = Decimal("3800")  # wstETH fallback

        try:
            borrow_price = market.price(self.borrow_token)
        except (ValueError, KeyError):
            borrow_price = Decimal("1")

        # Idle -> supply collateral
        if self._state == "idle":
            self._reference_price = collateral_price
            self._previous_stable_state = self._state
            self._state = "supplying"
            logger.info(
                f"SUPPLY {self.collateral_amount} {self.collateral_token} "
                f"to Morpho Blue market at ${collateral_price:.2f}"
            )
            return Intent.supply(
                protocol="morpho_blue",
                token=self.collateral_token,
                amount=self.collateral_amount,
                use_as_collateral=True,
                chain=self.chain,
                market_id=self.market_id,
            )

        # Supplied + price dropped -> borrow
        if self._state == "supplied" and self._reference_price is not None:
            price_change = (collateral_price - self._reference_price) / self._reference_price

            if price_change <= -self.price_drop_pct:
                collateral_value = self._supplied_amount * collateral_price
                borrow_value = collateral_value * self.ltv_target
                borrow_amount = (borrow_value / borrow_price).quantize(Decimal("0.01"))

                if borrow_amount > 0:
                    self._previous_stable_state = self._state
                    self._state = "borrowing"
                    self._reference_price = collateral_price
                    logger.info(
                        f"BORROW {borrow_amount} {self.borrow_token} "
                        f"from Morpho Blue (price drop {price_change * 100:.1f}%)"
                    )
                    return Intent.borrow(
                        protocol="morpho_blue",
                        collateral_token=self.collateral_token,
                        collateral_amount=Decimal("0"),
                        borrow_token=self.borrow_token,
                        borrow_amount=borrow_amount,
                        chain=self.chain,
                        market_id=self.market_id,
                    )

        # Borrowed + price risen -> repay
        if self._state == "borrowed" and self._reference_price is not None:
            price_change = (collateral_price - self._reference_price) / self._reference_price

            if price_change >= self.price_rise_pct:
                self._previous_stable_state = self._state
                self._state = "repaying"
                self._reference_price = collateral_price
                logger.info(
                    f"REPAY {self._borrowed_amount} {self.borrow_token} "
                    f"to Morpho Blue (price rise {price_change * 100:.1f}%)"
                )
                return Intent.repay(
                    token=self.borrow_token,
                    amount=self._borrowed_amount,
                    protocol="morpho_blue",
                    repay_full=True,
                    market_id=self.market_id,
                )

        # Revert stuck transitional states
        if self._state in ("supplying", "borrowing", "repaying"):
            revert_to = self._previous_stable_state
            logger.warning(f"Stuck in '{self._state}', reverting to '{revert_to}'")
            self._state = revert_to

        return Intent.hold(
            reason=f"Holding (state={self._state}, price=${collateral_price:.2f})"
        )

    def on_intent_executed(self, intent: Intent, success: bool, result: Any) -> None:
        intent_type = intent.intent_type.value

        if success:
            if intent_type == "SUPPLY":
                self._state = "supplied"
                self._supplied_amount = self.collateral_amount
                add_event(
                    TimelineEvent(
                        timestamp=datetime.now(UTC),
                        event_type=TimelineEventType.POSITION_MODIFIED,
                        description=f"Supplied {self.collateral_amount} {self.collateral_token} to Morpho Blue",
                        strategy_id=self.strategy_id,
                        details={"action": "supply", "amount": str(self.collateral_amount)},
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
                        description=f"Borrowed {self._borrowed_amount} {self.borrow_token} from Morpho Blue",
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
                        description=f"Repaid {self.borrow_token} to Morpho Blue",
                        strategy_id=self.strategy_id,
                        details={"action": "repay"},
                    )
                )
        else:
            revert_to = self._previous_stable_state
            logger.warning(f"{intent_type} failed, reverting to '{revert_to}'")
            self._state = revert_to

    def get_status(self) -> dict[str, Any]:
        return {
            "strategy": "demo_morpho_paper_trade",
            "chain": self.chain,
            "market_id": self.market_id[:16] + "...",
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
        if state.get("reference_price"):
            self._reference_price = Decimal(str(state["reference_price"]))

    # Teardown interface
    def supports_teardown(self) -> bool:
        return True

    def get_open_positions(self) -> "TeardownPositionSummary":
        from almanak.framework.teardown import PositionInfo, PositionType, TeardownPositionSummary

        positions: list[PositionInfo] = []

        if self._supplied_amount > 0:
            positions.append(
                PositionInfo(
                    position_type=PositionType.SUPPLY,
                    position_id=f"morpho-supply-{self.collateral_token}-{self.chain}",
                    chain=self.chain,
                    protocol="morpho_blue",
                    value_usd=self._supplied_amount * Decimal("3800"),
                    details={
                        "asset": self.collateral_token,
                        "amount": str(self._supplied_amount),
                        "market_id": self.market_id,
                    },
                )
            )

        if self._borrowed_amount > 0:
            positions.append(
                PositionInfo(
                    position_type=PositionType.BORROW,
                    position_id=f"morpho-borrow-{self.borrow_token}-{self.chain}",
                    chain=self.chain,
                    protocol="morpho_blue",
                    value_usd=self._borrowed_amount,
                    details={
                        "asset": self.borrow_token,
                        "amount": str(self._borrowed_amount),
                        "market_id": self.market_id,
                    },
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
                    protocol="morpho_blue",
                    repay_full=True,
                    market_id=self.market_id,
                )
            )

        if self._supplied_amount > 0:
            intents.append(
                Intent.withdraw(
                    token=self.collateral_token,
                    amount=self._supplied_amount,
                    protocol="morpho_blue",
                    withdraw_all=True,
                    market_id=self.market_id,
                )
            )

        return intents
