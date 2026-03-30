"""
===============================================================================
Morpho Blue USDC Supply Yield — Paper Trading on Ethereum
===============================================================================

Paper trading strategy that supplies USDC to a Morpho Blue isolated market
to earn lending yield. First paper trading test with Morpho Blue USDC supply
(prior tests used wstETH collateral or other protocols).

STRATEGY LOGIC:
---------------
1. Tick 1: SUPPLY USDC to Morpho Blue market
2. Ticks 2-N: HOLD, earning supply yield
3. After N ticks: WITHDRAW all USDC (demonstrate full lifecycle)
4. If resupply enabled: SUPPLY again (multiple cycles in longer sessions)

KEY MORPHO BLUE CONCEPTS:
- Isolated markets identified by market_id (not pooled like Aave/Compound)
- Supplying USDC = providing loan asset (lenders earn yield from borrowers)
- Market ID encodes: loan_token, collateral_token, oracle, irm, lltv

USAGE:
------
    # Paper trade for 15 ticks at 60-second intervals
    almanak strat backtest paper start \
        -s demo_morpho_paper_usdc_yield \
        --chain ethereum \
        --max-ticks 15 \
        --tick-interval 60 \
        --foreground

    # Run directly on Anvil (single iteration)
    almanak strat run -d strategies/demo/morpho_paper_usdc_yield \
        --network anvil --once

===============================================================================
"""

import logging
from datetime import UTC, datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Any

from almanak.framework.intents import Intent
from almanak.framework.strategies import (
    IntentStrategy,
    MarketSnapshot,
    almanak_strategy,
)

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from almanak.framework.teardown import TeardownMode, TeardownPositionSummary

# Default Morpho Blue wstETH/USDC market on Ethereum (highest TVL)
# Supplying USDC to this market = lending USDC to wstETH-collateralized borrowers
DEFAULT_MARKET_ID = "0xb323495f7e4148be5643a4ea4a8221eef163e4bccfdedc2a6f4696baacbc86cc"


@almanak_strategy(
    name="demo_morpho_paper_usdc_yield",
    description="Paper trading demo — Morpho Blue USDC supply yield on Ethereum",
    version="1.0.0",
    author="Almanak",
    tags=["demo", "paper-trading", "lending", "morpho-blue", "ethereum", "usdc", "yield"],
    supported_chains=["ethereum"],
    supported_protocols=["morpho_blue"],
    intent_types=["SUPPLY", "WITHDRAW", "HOLD"],
    default_chain="ethereum",
)
class MorphoUSDCYieldPaperStrategy(IntentStrategy):
    """Morpho Blue USDC supply yield strategy for paper trading.

    Supplies USDC to a Morpho Blue isolated market, earns yield, then
    withdraws after a configurable number of ticks to demonstrate the
    full SUPPLY -> HOLD -> WITHDRAW lifecycle.

    Configuration (config.json):
        market_id: Morpho Blue market identifier
        supply_token: Token to supply (default: "USDC")
        supply_amount: Amount to supply (default: "1000")
        withdraw_after_ticks: Number of ticks before withdrawing (default: 8)
        resupply_after_withdraw: Re-supply after withdrawal (default: true)
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)

        self.market_id = self.get_config("market_id", DEFAULT_MARKET_ID)
        self.supply_token = self.get_config("supply_token", "USDC")
        self.supply_amount = Decimal(str(self.get_config("supply_amount", "1000")))
        self.withdraw_after_ticks = int(self.get_config("withdraw_after_ticks", 8))
        self.resupply_after_withdraw = bool(self.get_config("resupply_after_withdraw", True))

        # State
        self._state = "idle"
        self._supplied_amount = Decimal("0")
        self._tick_count = 0
        self._ticks_since_supply = 0
        self._cycle_count = 0

        logger.info(
            "MorphoUSDCYieldPaper initialized: supply=%s %s, market=%s..., "
            "withdraw_after=%d ticks, resupply=%s",
            self.supply_amount,
            self.supply_token,
            self.market_id[:10],
            self.withdraw_after_ticks,
            self.resupply_after_withdraw,
        )

    def decide(self, market: MarketSnapshot) -> Intent:
        """Supply USDC, hold for yield, withdraw after N ticks."""
        self._tick_count += 1

        # State: idle -> supply USDC
        if self._state == "idle":
            self._state = "supplying"
            self._cycle_count += 1
            logger.info(
                "Tick %d: SUPPLY %s %s to Morpho Blue (cycle #%d)",
                self._tick_count,
                self.supply_amount,
                self.supply_token,
                self._cycle_count,
            )
            return Intent.supply(
                protocol="morpho_blue",
                token=self.supply_token,
                amount=self.supply_amount,
                chain=self.chain,
                market_id=self.market_id,
            )

        # State: supplying -> auto-advance (paper trading may not call on_intent_executed)
        if self._state == "supplying":
            self._state = "supplied"
            self._supplied_amount = self.supply_amount
            self._ticks_since_supply = 0
            logger.info("Auto-advanced: supplying -> supplied")

        # State: supplied -> hold until withdraw time
        if self._state == "supplied":
            self._ticks_since_supply += 1

            if self._ticks_since_supply >= self.withdraw_after_ticks:
                self._state = "withdrawing"
                logger.info(
                    "Tick %d: WITHDRAW %s %s after %d ticks (cycle #%d)",
                    self._tick_count,
                    self._supplied_amount,
                    self.supply_token,
                    self._ticks_since_supply,
                    self._cycle_count,
                )
                return Intent.withdraw(
                    token=self.supply_token,
                    amount=self._supplied_amount,
                    protocol="morpho_blue",
                    withdraw_all=True,
                    chain=self.chain,
                    market_id=self.market_id,
                )

            return Intent.hold(
                reason=(
                    f"Earning yield on {self._supplied_amount} {self.supply_token} "
                    f"in Morpho Blue ({self._ticks_since_supply}/{self.withdraw_after_ticks} ticks)"
                )
            )

        # State: withdrawing -> auto-advance
        if self._state == "withdrawing":
            self._supplied_amount = Decimal("0")
            self._ticks_since_supply = 0
            if self.resupply_after_withdraw:
                self._state = "idle"
                logger.info("Withdrawn. Will re-supply next tick.")
            else:
                self._state = "done"
                logger.info("Withdrawn. Strategy complete.")

        # State: done -> hold forever
        if self._state == "done":
            return Intent.hold(reason="Strategy complete — all cycles finished")

        return Intent.hold(reason=f"State={self._state}, tick={self._tick_count}")

    def on_intent_executed(self, intent: Intent, success: bool, result: Any = None) -> None:
        """Handle intent execution results."""
        intent_type = intent.intent_type.value

        if success:
            if intent_type == "SUPPLY":
                self._state = "supplied"
                self._supplied_amount = self.supply_amount
                self._ticks_since_supply = 0
                logger.info("SUPPLY confirmed. State -> supplied")
            elif intent_type == "WITHDRAW":
                self._supplied_amount = Decimal("0")
                self._ticks_since_supply = 0
                if self.resupply_after_withdraw:
                    self._state = "idle"
                else:
                    self._state = "done"
                logger.info("WITHDRAW confirmed. State -> %s", self._state)
        else:
            # Reset transitional states to safe retryable states so
            # auto-advance in decide() doesn't promote a failed intent.
            if intent_type == "SUPPLY" and self._state == "supplying":
                self._state = "idle"
                logger.warning("SUPPLY failed. State -> idle (will retry)")
            elif intent_type == "WITHDRAW" and self._state == "withdrawing":
                self._state = "supplied"
                logger.warning("WITHDRAW failed. State -> supplied (will retry)")
            else:
                logger.warning("%s failed, staying in current state", intent_type)

    def get_status(self) -> dict[str, Any]:
        return {
            "strategy": "demo_morpho_paper_usdc_yield",
            "chain": self.chain,
            "state": self._state,
            "supplied_amount": str(self._supplied_amount),
            "tick_count": self._tick_count,
            "ticks_since_supply": self._ticks_since_supply,
            "cycle_count": self._cycle_count,
        }

    def get_persistent_state(self) -> dict[str, Any]:
        return {
            "state": self._state,
            "supplied_amount": str(self._supplied_amount),
            "tick_count": self._tick_count,
            "ticks_since_supply": self._ticks_since_supply,
            "cycle_count": self._cycle_count,
        }

    def load_persistent_state(self, state: dict[str, Any]) -> None:
        self._state = state.get("state", "idle")
        self._supplied_amount = Decimal(str(state.get("supplied_amount", "0")))
        self._tick_count = int(state.get("tick_count", 0))
        self._ticks_since_supply = int(state.get("ticks_since_supply", 0))
        self._cycle_count = int(state.get("cycle_count", 0))

    # =========================================================================
    # TEARDOWN
    # =========================================================================

    def supports_teardown(self) -> bool:
        return True

    def get_open_positions(self) -> "TeardownPositionSummary":
        from almanak.framework.teardown import PositionInfo, PositionType, TeardownPositionSummary

        positions: list[PositionInfo] = []
        if self._supplied_amount > 0:
            positions.append(
                PositionInfo(
                    position_type=PositionType.SUPPLY,
                    position_id=f"morpho-blue-usdc-supply-{self.chain}",
                    chain=self.chain,
                    protocol="morpho_blue",
                    value_usd=self._supplied_amount,  # USDC ~ $1
                    details={
                        "asset": self.supply_token,
                        "amount": str(self._supplied_amount),
                        "market_id": self.market_id,
                    },
                )
            )

        return TeardownPositionSummary(
            strategy_id=getattr(self, "strategy_id", "demo_morpho_paper_usdc_yield"),
            timestamp=datetime.now(UTC),
            positions=positions,
        )

    def generate_teardown_intents(self, mode: "TeardownMode", market=None) -> list[Intent]:
        if self._supplied_amount <= 0:
            return []

        return [
            Intent.withdraw(
                token=self.supply_token,
                amount=self._supplied_amount,
                protocol="morpho_blue",
                withdraw_all=True,
                chain=self.chain,
                market_id=self.market_id,
            )
        ]
