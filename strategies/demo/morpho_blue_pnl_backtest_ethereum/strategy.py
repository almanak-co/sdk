"""Morpho Blue wstETH Collateral Supply Strategy for PnL Backtesting on Ethereum.

This strategy supplies wstETH to the Morpho Blue wstETH/USDC market (highest TVL)
as collateral to exercise the PnL backtester with Morpho Blue lending supply intents.

PURPOSE:
--------
1. First Morpho Blue strategy to use the PnL backtester (``almanak strat backtest pnl``)
2. Validate that the PnL backtester's Morpho Blue APY provider works on Ethereum
3. Exercise the MorphoBlueAPYProvider and fee model in a real backtesting run

STRATEGY LOGIC:
---------------
- Supplies wstETH to the Morpho Blue wstETH/USDC market as collateral
- Supplies on the first tick, then holds to accrue lending yield
- PnL backtester tracks wstETH position value over time via MorphoBlueAPYProvider

MORPHO BLUE MARKET (Ethereum):
-------------------------------
    Market ID: 0xb323495f7e4148be5643a4ea4a8221eef163e4bccfdedc2a6f4696baacbc86cc
    Loan asset:      USDC
    Collateral:      wstETH
    LLTV:            86%

    We supply wstETH as collateral to the market. The PnL backtester tracks
    the wstETH position value over time using the Morpho Blue lending adapter
    and MorphoBlueAPYProvider (first Morpho Blue PnL backtest).

USAGE:
------
    # PnL backtest (exercises Morpho Blue lending adapter)
    almanak strat backtest pnl \\
        -d strategies/demo/morpho_blue_pnl_backtest_ethereum \\
        --start 2024-01-01 \\
        --end 2024-06-30

    # Run on Anvil fork (single tick)
    almanak strat run -d strategies/demo/morpho_blue_pnl_backtest_ethereum \\
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

# Morpho Blue wstETH/USDC market on Ethereum (highest TVL as of 2024)
DEFAULT_MARKET_ID = "0xb323495f7e4148be5643a4ea4a8221eef163e4bccfdedc2a6f4696baacbc86cc"


@almanak_strategy(
    name="demo_morpho_blue_pnl_backtest_ethereum",
    description="Morpho Blue wstETH collateral supply strategy for PnL backtesting on Ethereum",
    version="1.0.0",
    author="Almanak",
    tags=["demo", "backtesting", "lending", "morpho-blue", "ethereum", "pnl", "wsteth"],
    supported_chains=["ethereum"],
    supported_protocols=["morpho_blue"],
    intent_types=["SUPPLY", "WITHDRAW", "HOLD"],
)
class MorphoBluePnLBacktestStrategy(IntentStrategy):
    """Morpho Blue wstETH collateral supply strategy for PnL backtesting.

    Supplies wstETH to the Morpho Blue wstETH/USDC market as collateral,
    tracking position value via the PnL backtester's Morpho Blue lending adapter.
    This is the first strategy to exercise MorphoBlueAPYProvider in a PnL backtest.

    Configuration Parameters (from config.json):
        market_id: Morpho Blue market ID (default: wstETH/USDC on Ethereum)
        supply_token: Collateral token to supply (default: "wstETH")
        supply_amount: Amount to supply (default: "2")
        min_apy_bps: Minimum APY threshold in bps (default: 100 = 1%, informational)
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.market_id = self.get_config("market_id", DEFAULT_MARKET_ID)
        self.supply_token = self.get_config("supply_token", "wstETH")
        self.supply_amount = Decimal(str(self.get_config("supply_amount", "2")))
        self.min_apy_bps = int(self.get_config("min_apy_bps", 100))

        # State machine
        self._state = "idle"
        self._previous_stable_state = "idle"
        self._supplied_amount = Decimal("0")
        self._last_token_price = Decimal("0")
        self._tick_count = 0

        logger.info(
            f"MorphoBluePnLBacktestStrategy initialized: "
            f"supply={self.supply_amount} {self.supply_token}, "
            f"market={self.market_id[:10]}..., "
            f"min_apy={self.min_apy_bps}bps"
        )

    def decide(self, market: MarketSnapshot) -> Intent | None:
        """Supply wstETH to Morpho Blue as collateral, then hold.

        Tick 1: Supply wstETH to the wstETH/USDC market as collateral.
        Ticks 2+: Hold, with position value tracked by the PnL backtester.
        """
        self._tick_count += 1

        try:
            token_price = market.price(self.supply_token)
            self._last_token_price = token_price
        except (ValueError, KeyError):
            logger.warning(f"Price unavailable for {self.supply_token}, using last known price")
            token_price = self._last_token_price

        # Step 1: If idle, supply wstETH collateral
        if self._state == "idle" and token_price == Decimal("0"):
            return Intent.hold(reason=f"Price unavailable for {self.supply_token}, cannot supply (tick {self._tick_count})")

        if self._state == "idle":
            self._previous_stable_state = self._state
            self._state = "supplying"
            logger.info(
                f"Tick {self._tick_count}: SUPPLY {self.supply_amount} {self.supply_token} "
                f"to Morpho Blue market {self.market_id[:10]}... "
                f"(value ~${float(self.supply_amount * token_price):,.0f})"
            )
            return Intent.supply(
                protocol="morpho_blue",
                token=self.supply_token,
                amount=self.supply_amount,
                use_as_collateral=True,
                chain=self.chain,
                market_id=self.market_id,
            )

        # Step 2: If transitioning, hold until supply confirms
        if self._state == "supplying":
            logger.warning(f"Still in '{self._state}', supply may not have confirmed yet")
            return Intent.hold(reason=f"Waiting for supply confirmation (tick {self._tick_count})")

        # Step 3: If supplied, hold (PnL backtester tracks position value)
        if self._state == "supplied":
            supply_value = self._supplied_amount * token_price
            return Intent.hold(
                reason=(
                    f"Holding {self._supplied_amount} {self.supply_token} "
                    f"in Morpho Blue (value ~${float(supply_value):,.0f}, tick {self._tick_count})"
                )
            )

        # Fallback
        return Intent.hold(reason=f"State={self._state}, tick={self._tick_count}")

    def on_intent_executed(self, intent: Intent, success: bool, result: Any) -> None:
        intent_type = intent.intent_type.value

        if success:
            if intent_type == "SUPPLY":
                self._previous_stable_state = "supplied"
                self._state = "supplied"
                self._supplied_amount = self.supply_amount
                add_event(
                    TimelineEvent(
                        timestamp=datetime.now(UTC),
                        event_type=TimelineEventType.POSITION_OPENED,
                        description=f"Supplied {self.supply_amount} {self.supply_token} to Morpho Blue",
                        strategy_id=self.strategy_id,
                        details={
                            "action": "supply",
                            "amount": str(self.supply_amount),
                            "market_id": self.market_id,
                            "protocol": "morpho_blue",
                        },
                    )
                )
            elif intent_type == "WITHDRAW":
                self._state = "idle"
                self._supplied_amount = Decimal("0")
                add_event(
                    TimelineEvent(
                        timestamp=datetime.now(UTC),
                        event_type=TimelineEventType.POSITION_CLOSED,
                        description=f"Withdrew {self.supply_token} from Morpho Blue",
                        strategy_id=self.strategy_id,
                        details={"action": "withdraw", "protocol": "morpho_blue"},
                    )
                )
        else:
            # On failure, revert to previous stable state
            revert_to = self._previous_stable_state
            logger.warning(f"{intent_type} failed, reverting to '{revert_to}'")
            self._state = revert_to

    def get_status(self) -> dict[str, Any]:
        return {
            "strategy": "demo_morpho_blue_pnl_backtest_ethereum",
            "chain": self.chain,
            "state": self._state,
            "supplied_amount": str(self._supplied_amount),
            "market_id": self.market_id,
            "tick_count": self._tick_count,
        }

    def get_persistent_state(self) -> dict[str, Any]:
        return {
            "state": self._state,
            "previous_stable_state": self._previous_stable_state,
            "supplied_amount": str(self._supplied_amount),
            "last_token_price": str(self._last_token_price),
            "tick_count": self._tick_count,
        }

    def load_persistent_state(self, state: dict[str, Any]) -> None:
        if "state" in state:
            self._state = state["state"]
        if "previous_stable_state" in state:
            self._previous_stable_state = state["previous_stable_state"]
        if "supplied_amount" in state:
            self._supplied_amount = Decimal(str(state["supplied_amount"]))
        if "last_token_price" in state:
            self._last_token_price = Decimal(str(state["last_token_price"]))
        if "tick_count" in state:
            self._tick_count = int(state["tick_count"])

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
                    position_id=f"morpho-blue-supply-{self.supply_token}-{self.chain}",
                    chain=self.chain,
                    protocol="morpho_blue",
                    value_usd=self._supplied_amount * self._last_token_price,
                    details={
                        "asset": self.supply_token,
                        "amount": str(self._supplied_amount),
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
