"""
Edge USDT Supply ETH — Aave V3 Supply-Only Strategy

Implements a supply-only lending strategy on Aave V3 (Ethereum) based on
Edge signal 85fa7410-bd84-4e73-83cd-2f68a23d9145.

Signal thesis: USDT on Ethereum — elevated lending yield (5.14% net spread
vs Aave V3 borrowing cost). Also supported by RWA yield gap (USDT yields
759bps above 10Y Treasury).

PROTOCOL FALLBACK: The original signal targets Radiant V2 for supply, but
radiant_v2 is not a supported protocol in the Almanak SDK intent vocabulary.
We fall back to aave_v3 on Ethereum which also supports USDT supply and
captures similar elevated stablecoin lending yields.

Entry:  Supply 2 USDT to Aave V3 on Ethereum ($2 playground safety)
Hold:   While stop-loss not hit and time < 168h
Exit:   Withdraw when stop-loss triggers (USDT depeg) or time horizon expires
"""

import logging
import time
from datetime import UTC, datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Any

from almanak.framework.intents import Intent
from almanak.framework.market import MarketSnapshot
from almanak.framework.strategies import IntentStrategy, almanak_strategy

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from almanak.framework.teardown import TeardownMode, TeardownPositionSummary


# Protocol fallback: radiant_v2 is not supported by the SDK.
# Using aave_v3 on Ethereum as the supply protocol instead.
SUPPLY_PROTOCOL = "aave_v3"


@almanak_strategy(
    name="edge_usdt_supply_eth",
    description="Aave V3 supply-only strategy capturing elevated USDT lending yield on Ethereum",
    version="1.0.0",
    author="Almanak Edge",
    tags=["edge", "lending", "aave-v3", "ethereum", "usdt", "supply-only"],
    supported_chains=["ethereum"],
    supported_protocols=["aave_v3"],
    intent_types=["SUPPLY", "WITHDRAW", "HOLD"],
    default_chain="ethereum",
)
class EdgeUsdtSupplyEthStrategy(IntentStrategy):
    """Supply USDT to Aave V3 on Ethereum to capture elevated lending APY.

    Based on Edge signal 85fa7410: HIGH_CONVICTION_SYNTHESIS combining
    LENDING_ARBITRAGE + RWA_YIELD_GAP sources. Alpha score 86/100, BEAR regime.

    State machine:
        idle -> supplying -> supplied -> withdrawing -> done

    Exit conditions:
        - Time horizon exceeded (168h / 7 days)
        - Stop-loss triggered (USDT depeg > 10% from entry price)

    Configuration Parameters (from config.json):
        collateral_token: Token to supply (USDT)
        supply_amount: Amount to supply (2 USDT)
        stop_loss_pct: Max loss before exit (-0.10 = -10%)
        time_horizon_hours: Max holding period (168h = 7 days)
        signal_id: Edge signal ID for tracking
        min_collateral_usd: Min USD value to enter position
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # Token config
        self.collateral_token = self.get_config("collateral_token", "USDT")

        # Position sizing
        self.supply_amount = Decimal(str(self.get_config("supply_amount", "2")))
        self.min_collateral_usd = Decimal(
            str(self.get_config("min_collateral_usd", "1"))
        )

        # Exit conditions from signal spec
        self.stop_loss_pct = Decimal(str(self.get_config("stop_loss_pct", "-0.10")))
        self.time_horizon_hours = int(self.get_config("time_horizon_hours", 168))

        # Signal metadata
        self.signal_id = self.get_config(
            "signal_id", "85fa7410-bd84-4e73-83cd-2f68a23d9145"
        )

        # State machine
        self._state = "idle"
        self._previous_stable_state = "idle"
        self._supplied_amount = Decimal("0")
        self._entry_time: float | None = None
        self._entry_price: Decimal | None = None

        logger.info(
            f"EdgeUsdtSupplyEthStrategy initialized: "
            f"supply={self.supply_amount} {self.collateral_token} "
            f"to {SUPPLY_PROTOCOL}, "
            f"stop_loss={self.stop_loss_pct}, "
            f"time_horizon={self.time_horizon_hours}h"
        )

    def decide(self, market: MarketSnapshot) -> Intent | None:
        """Supply-only decision logic.

        1. idle -> supply USDT to Aave V3
        2. supplied -> monitor exit conditions (time horizon, depeg stop-loss)
        3. exit -> withdraw all USDT
        """
        try:
            # --- State: IDLE — enter the position ---
            if self._state == "idle":
                try:
                    balance = market.balance(self.collateral_token)
                except (ValueError, KeyError) as e:
                    return Intent.hold(
                        reason=f"Cannot check {self.collateral_token} balance: {e}"
                    )

                if balance.balance < self.supply_amount:
                    return Intent.hold(
                        reason=f"Insufficient {self.collateral_token}: "
                        f"have {balance.balance}, need {self.supply_amount}"
                    )

                self._previous_stable_state = self._state
                self._state = "supplying"
                logger.info(
                    f"Supplying {self.supply_amount} {self.collateral_token} "
                    f"to {SUPPLY_PROTOCOL}"
                )
                return Intent.supply(
                    protocol=SUPPLY_PROTOCOL,
                    token=self.collateral_token,
                    amount=self.supply_amount,
                    use_as_collateral=True,
                    chain=self.chain,
                )

            # --- State: SUPPLYING — wait for confirmation ---
            if self._state == "supplying":
                return Intent.hold(reason="Waiting for supply confirmation")

            # --- State: SUPPLIED — monitor exit conditions ---
            if self._state == "supplied":
                exit_reason = self._check_exit_conditions(market)
                if exit_reason:
                    self._previous_stable_state = self._state
                    self._state = "withdrawing"
                    logger.info(f"Exiting position: {exit_reason}")
                    return Intent.withdraw(
                        protocol=SUPPLY_PROTOCOL,
                        token=self.collateral_token,
                        amount=self._supplied_amount,
                        withdraw_all=True,
                        chain=self.chain,
                    )

                return Intent.hold(
                    reason=f"Holding {self._supplied_amount} {self.collateral_token} "
                    f"on {SUPPLY_PROTOCOL} (signal: {self.signal_id[:8]}...)"
                )

            # --- State: WITHDRAWING — wait for confirmation ---
            if self._state == "withdrawing":
                return Intent.hold(reason="Waiting for withdrawal confirmation")

            # --- State: DONE — strategy completed ---
            if self._state == "done":
                return Intent.hold(reason="Strategy completed — position closed")

            return Intent.hold(reason=f"Unknown state: {self._state}")

        except Exception as e:
            logger.exception(f"Error in decide(): {e}")
            return Intent.hold(reason=f"Error: {e!s}")

    def _check_exit_conditions(self, market: MarketSnapshot) -> str | None:
        """Check whether any exit condition is met.

        Returns:
            Reason string if should exit, None if should hold.
        """
        # 1. Time horizon exceeded (168h = 7 days)
        if self._entry_time is not None:
            elapsed_hours = (time.time() - self._entry_time) / 3600
            if elapsed_hours >= self.time_horizon_hours:
                return (
                    f"Time horizon exceeded: {elapsed_hours:.1f}h >= "
                    f"{self.time_horizon_hours}h"
                )

        # 2. Stop-loss: USDT depeg check
        #    For a stablecoin supply strategy, the primary risk is USDT depegging.
        #    We track price deviation from the entry price ($1).
        if self._entry_price is not None:
            try:
                current_price = market.price(self.collateral_token)
                pnl_pct = (current_price - self._entry_price) / self._entry_price
                if pnl_pct <= self.stop_loss_pct:
                    return (
                        f"Stop-loss triggered (USDT depeg): {pnl_pct:.2%} <= "
                        f"{self.stop_loss_pct:.2%}"
                    )
            except (ValueError, KeyError):
                logger.debug("Could not check USDT price for stop-loss")

        return None

    def on_intent_executed(self, intent: Intent, success: bool, result: Any) -> None:
        """Advance state machine after intent execution."""
        intent_type = getattr(intent, "intent_type", None)
        if not intent_type:
            return

        if success:
            if intent_type.value == "SUPPLY":
                self._state = "supplied"
                self._supplied_amount = self.supply_amount
                self._entry_time = time.time()
                # Record entry price for stop-loss (USDT depeg tracking)
                try:
                    snapshot = self.create_market_snapshot()
                    self._entry_price = snapshot.price(self.collateral_token)
                except Exception:  # noqa: BLE001
                    self._entry_price = Decimal("1")  # USDT ~= $1
                logger.info(
                    f"Supply confirmed: {self._supplied_amount} "
                    f"{self.collateral_token} to {SUPPLY_PROTOCOL}"
                )
            elif intent_type.value == "WITHDRAW":
                self._state = "done"
                self._supplied_amount = Decimal("0")
                logger.info("Withdrawal confirmed — strategy complete")
        else:
            revert_to = self._previous_stable_state
            logger.warning(
                f"{intent_type.value} failed, reverting state to '{revert_to}'"
            )
            self._state = revert_to

    def get_status(self) -> dict[str, Any]:
        """Get current strategy status for monitoring/dashboards."""
        elapsed_hours = None
        if self._entry_time is not None:
            elapsed_hours = round((time.time() - self._entry_time) / 3600, 1)
        return {
            "strategy": "edge_usdt_supply_eth",
            "chain": self.chain,
            "state": self._state,
            "protocol": SUPPLY_PROTOCOL,
            "supplied_amount": str(self._supplied_amount),
            "collateral_token": self.collateral_token,
            "signal_id": self.signal_id,
            "elapsed_hours": elapsed_hours,
            "wallet": (
                self.wallet_address[:10] + "..." if self.wallet_address else None
            ),
        }

    def get_persistent_state(self) -> dict[str, Any]:
        """Save strategy state between iterations."""
        return {
            "state": self._state,
            "previous_stable_state": self._previous_stable_state,
            "supplied_amount": str(self._supplied_amount),
            "entry_time": self._entry_time,
            "entry_price": str(self._entry_price) if self._entry_price else None,
        }

    def load_persistent_state(self, state: dict[str, Any]) -> None:
        """Restore strategy state from previous iteration."""
        if "state" in state:
            self._state = state["state"]
        if "previous_stable_state" in state:
            self._previous_stable_state = state["previous_stable_state"]
        if "supplied_amount" in state:
            self._supplied_amount = Decimal(str(state["supplied_amount"]))
        if "entry_time" in state:
            self._entry_time = state["entry_time"]
        if state.get("entry_price"):
            self._entry_price = Decimal(str(state["entry_price"]))

    # -------------------------------------------------------------------------
    # TEARDOWN (required)
    # -------------------------------------------------------------------------

    def get_open_positions(self) -> "TeardownPositionSummary":
        """Return all open positions for teardown preview."""
        from almanak.framework.teardown import (
            PositionInfo,
            PositionType,
            TeardownPositionSummary,
        )

        positions: list[PositionInfo] = []

        if self._supplied_amount > 0:
            try:
                snapshot = self.create_market_snapshot()
                price = snapshot.price(self.collateral_token)
            except Exception:  # noqa: BLE001
                price = Decimal("1")  # USDT fallback
            positions.append(
                PositionInfo(
                    position_type=PositionType.SUPPLY,
                    position_id=f"aave-supply-{self.collateral_token}-{self.chain}",
                    chain=self.chain,
                    protocol=SUPPLY_PROTOCOL,
                    value_usd=self._supplied_amount * price,
                    details={
                        "asset": self.collateral_token,
                        "amount": str(self._supplied_amount),
                        "signal_id": self.signal_id,
                    },
                )
            )

        return TeardownPositionSummary(
            deployment_id=getattr(self, "deployment_id", "edge_usdt_supply_eth"),
            timestamp=datetime.now(UTC),
            positions=positions,
        )

    def generate_teardown_intents(
        self, mode: "TeardownMode" = None, market=None
    ) -> list[Intent]:
        """Generate intents to close all positions.

        For a supply-only strategy, teardown is simply withdrawing
        all supplied USDT from Aave V3.
        """
        intents: list[Intent] = []

        if self._supplied_amount > 0:
            intents.append(
                Intent.withdraw(
                    protocol=SUPPLY_PROTOCOL,
                    token=self.collateral_token,
                    amount=self._supplied_amount,
                    withdraw_all=True,
                )
            )

        return intents


if __name__ == "__main__":
    print("=" * 60)
    print("EdgeUsdtSupplyEthStrategy")
    print("=" * 60)
    print(f"Strategy Name: {EdgeUsdtSupplyEthStrategy.STRATEGY_NAME}")
    print(f"Supported Chains: {EdgeUsdtSupplyEthStrategy.SUPPORTED_CHAINS}")
    print(f"Supported Protocols: {EdgeUsdtSupplyEthStrategy.SUPPORTED_PROTOCOLS}")
    print(f"Intent Types: {EdgeUsdtSupplyEthStrategy.INTENT_TYPES}")
    print(f"\nProtocol: {SUPPLY_PROTOCOL} (fallback from radiant_v2)")
    print("\nTo run this strategy:")
    print("  almanak strat run --dry-run --once")
