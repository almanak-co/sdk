"""
===============================================================================
Aave V3 Paper Trade — Lending Lifecycle on Ethereum
===============================================================================

Paper trading vehicle that cycles through Aave V3 supply/borrow/repay/withdraw
operations on Ethereum based on price movement thresholds. Designed to generate
frequent lending cycles during paper trade sessions, exercising the paper
trading engine's PnL tracking for lending positions on Ethereum mainnet.

Kitchen Loop iteration 155 (VIB-2310).

Gap filled:
- First paper trade strategy on Ethereum chain with lending protocol
- Tests paper trading engine with Aave V3 lending intents on Ethereum
- Validates Anvil fork + PnL tracking on Ethereum for lending lifecycle

USAGE:
------
    # Paper trade for 10 ticks at 60-second intervals
    almanak strat backtest paper start \
        -s aave_v3_paper_trade_ethereum \
        --chain ethereum \
        --max-ticks 10 \
        --tick-interval 60 \
        --foreground

    # Run on Anvil (single iteration)
    almanak strat run -d almanak/demo_strategies/aave_v3_paper_trade_ethereum \
        --network anvil --once
===============================================================================
"""

import logging
from datetime import UTC, datetime
from decimal import ROUND_DOWN, Decimal
from typing import TYPE_CHECKING, Any

from almanak.framework.intents import Intent
from almanak.framework.market import MarketSnapshot
from almanak.framework.strategies import IntentStrategy, almanak_strategy
from almanak.framework.utils.log_formatters import format_token_amount_human, format_usd

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from almanak.framework.teardown import TeardownMode, TeardownPositionSummary


@almanak_strategy(
    name="aave_v3_paper_trade_ethereum",
    description="Aave V3 lending lifecycle on Ethereum — paper trade vehicle",
    version="1.0.0",
    author="Kitchen Loop iter 155",
    tags=["demo", "paper-trading", "lending", "aave_v3", "ethereum", "backtesting"],
    supported_chains=["ethereum"],
    supported_protocols=["aave_v3"],
    intent_types=["SUPPLY", "BORROW", "REPAY", "WITHDRAW", "HOLD"],
    default_chain="ethereum",
)
class AaveV3PaperTradeEthereumStrategy(IntentStrategy):
    """Aave V3 lending lifecycle for paper trading on Ethereum.

    Decision logic (state machine):
    1. idle -> supply collateral (USDC) to Aave V3
    2. supplied -> if price moved > threshold OR max ticks: borrow (WETH)
    3. borrowed -> repay debt (one tick delay for realistic cycle)
    4. repaid -> withdraw collateral
    5. withdrawn -> back to idle (cycle restarts)

    This creates frequent supply/borrow/repay/withdraw cycles during paper
    trade sessions, exercising the paper trader's lending PnL tracking.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.collateral_token = str(self.get_config("collateral_token", "USDC"))
        self.collateral_amount = Decimal(str(self.get_config("collateral_amount", "500")))
        self.borrow_token = str(self.get_config("borrow_token", "WETH"))
        self.ltv_target = Decimal(str(self.get_config("ltv_target", "0.25")))
        self.supply_threshold_pct = Decimal(str(self.get_config("supply_threshold_pct", "3.0")))
        self.max_ticks_in_position = int(self.get_config("max_ticks_in_position", 4))

        self._state = "idle"
        self._ticks_in_state = 0
        self._entry_price = Decimal("0")
        self._supplied_amount = Decimal("0")
        self._borrowed_amount = Decimal("0")
        self._total_cycles = 0
        self._total_supplies = 0
        self._total_borrows = 0
        self._total_repays = 0
        self._total_withdraws = 0

        logger.info(
            f"AaveV3PaperTradeEthereum initialized: "
            f"collateral={self.collateral_amount} {self.collateral_token}, "
            f"borrow={self.borrow_token}, LTV={self.ltv_target * 100:.0f}%, "
            f"threshold={self.supply_threshold_pct}%, "
            f"max_ticks={self.max_ticks_in_position}"
        )

    def decide(self, market: MarketSnapshot) -> Intent | None:
        """Lending lifecycle decision based on state and price movement."""
        self._ticks_in_state += 1

        # Unwind states don't need price data — always proceed to close positions
        if self._state == "borrowed":
            logger.info("Repaying borrowed amount")
            return self._create_repay_intent()

        if self._state == "repaid":
            logger.info("Withdrawing collateral")
            return self._create_withdraw_intent()

        if self._state == "withdrawn":
            self._state = "idle"
            self._ticks_in_state = 0
            self._total_cycles += 1
            logger.info(f"Cycle #{self._total_cycles} complete, restarting")
            return Intent.hold(reason=f"Cycle #{self._total_cycles} complete, restarting next tick")

        # States that need price data
        try:
            collateral_price = market.price(self.collateral_token)
            borrow_price = market.price(self.borrow_token)
            if collateral_price <= 0 or borrow_price <= 0:
                return Intent.hold(
                    reason=f"Invalid price: {self.collateral_token}={collateral_price}, "
                    f"{self.borrow_token}={borrow_price}"
                )
        except Exception as e:
            return Intent.hold(reason=f"Price unavailable: {e}")

        if self._state == "idle":
            # Check balance before supplying
            try:
                collateral_bal = market.balance(self.collateral_token)
                if collateral_bal < self.collateral_amount:
                    return Intent.hold(
                        reason=f"Insufficient {self.collateral_token}: "
                        f"{collateral_bal} < {self.collateral_amount}"
                    )
            except Exception:
                return Intent.hold(reason=f"Balance unavailable for {self.collateral_token}")

            self._entry_price = borrow_price
            logger.info(
                f"Cycle start: supplying {format_token_amount_human(self.collateral_amount, self.collateral_token)} "
                f"to Aave V3 (borrow price entry: ${borrow_price:.2f})"
            )
            return self._create_supply_intent()

        if self._state == "supplied":
            # Trigger borrow on price movement or max ticks
            should_borrow = False
            reason = ""

            if self._entry_price > 0:
                price_change_pct = abs(borrow_price - self._entry_price) / self._entry_price * 100
                if price_change_pct >= self.supply_threshold_pct:
                    should_borrow = True
                    reason = f"price moved {price_change_pct:.2f}%"

            if self._ticks_in_state >= self.max_ticks_in_position:
                should_borrow = True
                reason = f"max ticks reached ({self._ticks_in_state}/{self.max_ticks_in_position})"

            if should_borrow:
                logger.info(f"Borrowing: {reason}")
                return self._create_borrow_intent(collateral_price, borrow_price)

            return Intent.hold(
                reason=f"Supplied, waiting ({self._ticks_in_state}/{self.max_ticks_in_position} ticks)"
            )

        return Intent.hold(reason=f"Unknown state: {self._state}")

    def _create_supply_intent(self) -> Intent:
        return Intent.supply(
            protocol="aave_v3",
            token=self.collateral_token,
            amount=self.collateral_amount,
            use_as_collateral=True,
            chain=self.chain,
        )

    def _create_borrow_intent(
        self, collateral_price: Decimal, borrow_price: Decimal
    ) -> Intent:
        collateral_value = self.collateral_amount * collateral_price
        borrow_value = collateral_value * self.ltv_target
        borrow_amount = (borrow_value / borrow_price).quantize(Decimal("0.000001"), rounding=ROUND_DOWN)

        logger.info(
            f"BORROW: collateral_value={format_usd(collateral_value)}, "
            f"LTV={self.ltv_target * 100:.0f}%, "
            f"borrow={format_token_amount_human(borrow_amount, self.borrow_token)}"
        )
        return Intent.borrow(
            protocol="aave_v3",
            collateral_token=self.collateral_token,
            collateral_amount=Decimal("0"),
            borrow_token=self.borrow_token,
            borrow_amount=borrow_amount,
            interest_rate_mode="variable",
            chain=self.chain,
        )

    def _create_repay_intent(self) -> Intent:
        return Intent.repay(
            protocol="aave_v3",
            token=self.borrow_token,
            amount=self._borrowed_amount if self._borrowed_amount > 0 else Decimal("0.001"),
            repay_full=True,
            chain=self.chain,
        )

    def _create_withdraw_intent(self) -> Intent:
        return Intent.withdraw(
            protocol="aave_v3",
            token=self.collateral_token,
            amount=self._supplied_amount if self._supplied_amount > 0 else self.collateral_amount,
            withdraw_all=True,
            chain=self.chain,
        )

    def on_intent_executed(self, intent: Any, success: bool, result: Any) -> None:
        if not success:
            return

        intent_type = getattr(intent, "intent_type", None)
        if intent_type is None:
            return

        intent_type_val = intent_type.value if hasattr(intent_type, "value") else str(intent_type)

        if intent_type_val == "SUPPLY":
            self._state = "supplied"
            self._ticks_in_state = 0
            self._supplied_amount = self.collateral_amount
            self._total_supplies += 1
            logger.info(f"Supply #{self._total_supplies} executed")

        elif intent_type_val == "BORROW":
            self._state = "borrowed"
            self._ticks_in_state = 0
            if hasattr(intent, "borrow_amount"):
                self._borrowed_amount = Decimal(str(intent.borrow_amount))
            self._total_borrows += 1
            logger.info(f"Borrow #{self._total_borrows} executed: {self._borrowed_amount} {self.borrow_token}")

        elif intent_type_val == "REPAY":
            self._state = "repaid"
            self._ticks_in_state = 0
            self._borrowed_amount = Decimal("0")
            self._total_repays += 1
            logger.info(f"Repay #{self._total_repays} executed")

        elif intent_type_val == "WITHDRAW":
            self._state = "withdrawn"
            self._ticks_in_state = 0
            self._supplied_amount = Decimal("0")
            self._total_withdraws += 1
            logger.info(f"Withdraw #{self._total_withdraws} executed, cycle ready to restart")

    # =========================================================================
    # STATE PERSISTENCE
    # =========================================================================

    def get_persistent_state(self) -> dict[str, Any]:
        return {
            "state": self._state,
            "ticks_in_state": self._ticks_in_state,
            "entry_price": str(self._entry_price),
            "supplied_amount": str(self._supplied_amount),
            "borrowed_amount": str(self._borrowed_amount),
            "total_cycles": self._total_cycles,
            "total_supplies": self._total_supplies,
            "total_borrows": self._total_borrows,
            "total_repays": self._total_repays,
            "total_withdraws": self._total_withdraws,
        }

    def load_persistent_state(self, state: dict[str, Any]) -> None:
        if "state" in state:
            self._state = state["state"]
        if "ticks_in_state" in state:
            self._ticks_in_state = int(state["ticks_in_state"])
        if "entry_price" in state:
            try:
                self._entry_price = Decimal(str(state["entry_price"]))
            except (ValueError, ArithmeticError):
                self._entry_price = Decimal("0")
        if "supplied_amount" in state:
            try:
                self._supplied_amount = Decimal(str(state["supplied_amount"]))
            except (ValueError, ArithmeticError):
                self._supplied_amount = Decimal("0")
        if "borrowed_amount" in state:
            try:
                self._borrowed_amount = Decimal(str(state["borrowed_amount"]))
            except (ValueError, ArithmeticError):
                self._borrowed_amount = Decimal("0")
        self._total_cycles = int(state.get("total_cycles", 0))
        self._total_supplies = int(state.get("total_supplies", 0))
        self._total_borrows = int(state.get("total_borrows", 0))
        self._total_repays = int(state.get("total_repays", 0))
        self._total_withdraws = int(state.get("total_withdraws", 0))

    # =========================================================================
    # TEARDOWN
    # =========================================================================

    def supports_teardown(self) -> bool:
        return True

    def get_open_positions(self) -> "TeardownPositionSummary":
        from almanak.framework.teardown import PositionInfo, PositionType, TeardownPositionSummary

        collateral_price = Decimal("0")
        borrow_price = Decimal("0")
        try:
            market = self.create_market_snapshot()
            collateral_price = Decimal(str(market.price(self.collateral_token)))
            borrow_price = Decimal(str(market.price(self.borrow_token)))
        except Exception:
            logger.warning("Unable to fetch live prices for teardown valuation")

        positions = []
        if self._supplied_amount > 0:
            positions.append(
                PositionInfo(
                    position_type=PositionType.SUPPLY,
                    position_id=f"aave-supply-{self.collateral_token}-ethereum",
                    chain=self.chain,
                    protocol="aave_v3",
                    value_usd=self._supplied_amount * collateral_price,
                    details={"asset": self.collateral_token, "amount": str(self._supplied_amount)},
                )
            )
        if self._borrowed_amount > 0:
            positions.append(
                PositionInfo(
                    position_type=PositionType.BORROW,
                    position_id=f"aave-borrow-{self.borrow_token}-ethereum",
                    chain=self.chain,
                    protocol="aave_v3",
                    value_usd=self._borrowed_amount * borrow_price,
                    details={"asset": self.borrow_token, "amount": str(self._borrowed_amount)},
                )
            )
        return TeardownPositionSummary(
            strategy_id=getattr(self, "strategy_id", "aave_v3_paper_trade_ethereum"),
            timestamp=datetime.now(UTC),
            positions=positions,
        )

    def generate_teardown_intents(self, mode: "TeardownMode", market=None) -> list[Intent]:
        intents = []
        if self._borrowed_amount > 0:
            intents.append(self._create_repay_intent())
        if self._supplied_amount > 0:
            intents.append(self._create_withdraw_intent())
        return intents

    # =========================================================================
    # STATUS
    # =========================================================================

    def get_status(self) -> dict[str, Any]:
        return {
            "strategy": "aave_v3_paper_trade_ethereum",
            "chain": self.chain,
            "config": {
                "collateral": f"{self.collateral_amount} {self.collateral_token}",
                "borrow_token": self.borrow_token,
                "ltv_target": str(self.ltv_target),
                "supply_threshold_pct": str(self.supply_threshold_pct),
                "max_ticks_in_position": self.max_ticks_in_position,
            },
            "state": {
                "current": self._state,
                "ticks_in_state": self._ticks_in_state,
                "total_cycles": self._total_cycles,
                "total_supplies": self._total_supplies,
                "total_borrows": self._total_borrows,
            },
        }
