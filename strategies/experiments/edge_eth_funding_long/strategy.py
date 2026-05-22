"""
Edge Strategy: ETH Funding Rate Long

Signal: HIGH_CONVICTION_SYNTHESIS (FUNDING_EXTREME)
Thesis: ETH perpetual shorts are paying longs substantial carry — 62.2%/day
on Kraken, consensus across 4 venues (15.6%/day carry average). Open long
ETH perp to collect funding payments from crowded shorts.

State machine: idle -> opening -> open -> closing -> done
Exit conditions: stop-loss (-15%), take-profit (+25%), time horizon (168h)

Chain: arbitrum (GMX V2 perps — signal originates from ethereum but
    GMX V2 is the SDK's perp protocol and runs on Arbitrum)
Protocol: gmx_v2
Signal ID: 4869b240-e380-42c6-95dd-a70c913a35a8
"""

import logging
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from almanak.framework.intents import Intent
from almanak.framework.market import MarketSnapshot
from almanak.framework.strategies import IntentStrategy, almanak_strategy

logger = logging.getLogger(__name__)


@almanak_strategy(
    name="edge_eth_funding_long",
    description="Long ETH perp to collect funding carry from crowded shorts",
    version="1.0.0",
    author="Edge Signal 4869b240",
    tags=["edge", "funding", "perps", "carry", "long"],
    supported_chains=["arbitrum"],
    supported_protocols=["gmx_v2"],
    intent_types=["PERP_OPEN", "PERP_CLOSE", "HOLD"],
    default_chain="arbitrum",
)
class EdgeEthFundingLongStrategy(IntentStrategy):
    """Long ETH perp to collect funding carry from crowded shorts.

    Edge signal specs:
    - Alpha: 90/100, Regime: BEAR
    - 2x leverage, ETH/USD long
    - Stop-loss: -15%, Take-profit: +25%
    - Time horizon: 168 hours (7 days)
    - Max position: $5 USD (playground safety)

    State machine:
        idle     -> opening  (emit PERP_OPEN intent)
        opening  -> open     (on_intent_executed success)
        opening  -> idle     (on_intent_executed failure — rollback)
        open     -> closing  (exit condition triggered, emit PERP_CLOSE)
        closing  -> done     (on_intent_executed success)
        closing  -> open     (on_intent_executed failure — rollback)
        done     -> done     (terminal — hold forever)
    """

    # -- State constants --
    STATE_IDLE = "idle"
    STATE_OPENING = "opening"
    STATE_OPEN = "open"
    STATE_CLOSING = "closing"
    STATE_DONE = "done"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        cfg = self.config if isinstance(self.config, dict) else {}
        if hasattr(self.config, "get") and not isinstance(self.config, dict):
            cfg = self.config

        # Market / position config
        self.perp_market: str = cfg.get("perp_market", "ETH/USD")
        self.collateral_token: str = cfg.get("collateral_token", "USDC")
        self.collateral_amount = Decimal(str(cfg.get("collateral_amount", "2.5")))
        self.position_size_usd = Decimal(str(cfg.get("position_size_usd", "5")))
        self.leverage = Decimal(str(cfg.get("leverage", "2")))
        self.max_slippage = Decimal(str(cfg.get("max_slippage", "0.005")))
        self.base_token: str = cfg.get("base_token", "ETH")

        # Exit parameters
        self.take_profit_pct = Decimal(str(cfg.get("take_profit_pct", "0.25")))
        self.stop_loss_pct = Decimal(str(cfg.get("stop_loss_pct", "0.15")))
        self.time_horizon_hours = int(cfg.get("time_horizon_hours", 168))

        # Internal state (restored via load_persistent_state)
        self._state: str = self.STATE_IDLE
        self._previous_stable_state: str = self.STATE_IDLE
        self._entry_price: Decimal | None = None
        self._pending_entry_price: Decimal | None = None
        self._opened_at: str | None = None  # ISO timestamp

        logger.info(
            f"EdgeEthFundingLong initialized: market={self.perp_market}, "
            f"size=${self.position_size_usd}, leverage={self.leverage}x, "
            f"TP={self.take_profit_pct:.0%}, SL=-{self.stop_loss_pct:.0%}, "
            f"horizon={self.time_horizon_hours}h"
        )

    # -------------------------------------------------------------------------
    # Core decision logic
    # -------------------------------------------------------------------------

    def decide(self, market: MarketSnapshot) -> Intent | None:
        """Decide next action based on state machine and market conditions."""
        try:
            return self._decide_inner(market)
        except Exception as e:
            logger.exception(f"Error in decide(): {e}")
            return Intent.hold(reason=f"Error: {e}")

    def _decide_inner(self, market: MarketSnapshot) -> Intent:
        current_price = market.price(self.base_token)

        # -- IDLE: open position --
        if self._state == self.STATE_IDLE:
            return self._try_open(market, current_price)

        # -- OPENING: waiting for execution callback --
        if self._state == self.STATE_OPENING:
            return Intent.hold(reason="Waiting for PERP_OPEN execution")

        # -- OPEN: monitor exit conditions --
        if self._state == self.STATE_OPEN:
            return self._monitor_position(current_price)

        # -- CLOSING: waiting for execution callback --
        if self._state == self.STATE_CLOSING:
            return Intent.hold(reason="Waiting for PERP_CLOSE execution")

        # -- DONE: terminal state --
        if self._state == self.STATE_DONE:
            return Intent.hold(reason="Strategy complete — position closed")

        return Intent.hold(reason=f"Unknown state: {self._state}")

    def _try_open(self, market: MarketSnapshot, current_price: Decimal) -> Intent:
        """Attempt to open a long ETH perp position."""
        # Check collateral balance
        try:
            collateral_bal = market.balance(self.collateral_token)
            if collateral_bal.balance < self.collateral_amount:
                return Intent.hold(
                    reason=(
                        f"Insufficient {self.collateral_token}: "
                        f"have {collateral_bal.balance}, need {self.collateral_amount}"
                    )
                )
        except (ValueError, KeyError):
            return Intent.hold(reason=f"Cannot check {self.collateral_token} balance")

        logger.info(
            f"Opening LONG {self.perp_market} | "
            f"collateral={self.collateral_amount} {self.collateral_token} | "
            f"size=${self.position_size_usd} | leverage={self.leverage}x | "
            f"price={current_price}"
        )

        # Stash price for fallback entry tracking
        self._pending_entry_price = current_price

        # Transition: idle -> opening
        self._previous_stable_state = self.STATE_IDLE
        self._state = self.STATE_OPENING

        return Intent.perp_open(
            market=self.perp_market,
            collateral_token=self.collateral_token,
            collateral_amount=self.collateral_amount,
            size_usd=self.position_size_usd,
            is_long=True,
            leverage=self.leverage,
            max_slippage=self.max_slippage,
            protocol="gmx_v2",
        )

    def _monitor_position(self, current_price: Decimal) -> Intent:
        """Check exit conditions for an open position."""
        now = datetime.now(UTC)
        reasons: list[str] = []

        # 1. Stop-loss / take-profit
        if self._entry_price and self._entry_price > 0:
            pnl_pct = (current_price - self._entry_price) / self._entry_price

            if pnl_pct >= self.take_profit_pct:
                reasons.append(f"Take-profit triggered: PnL {pnl_pct:+.2%} >= {self.take_profit_pct:+.0%}")

            if pnl_pct <= -self.stop_loss_pct:
                reasons.append(f"Stop-loss triggered: PnL {pnl_pct:+.2%} <= -{self.stop_loss_pct:.0%}")
        else:
            pnl_pct = Decimal("0")

        # 2. Time horizon expiry
        if self._opened_at:
            opened_dt = datetime.fromisoformat(self._opened_at)
            hours_held = (now - opened_dt).total_seconds() / 3600
            if hours_held >= self.time_horizon_hours:
                reasons.append(
                    f"Time horizon expired: {hours_held:.1f}h >= {self.time_horizon_hours}h"
                )
        else:
            hours_held = 0.0

        # If any exit condition triggered, close
        if reasons:
            for r in reasons:
                logger.info(f"EXIT: {r}")
            return self._close_position()

        # Still holding — report status
        status = f"Holding LONG {self.perp_market} | PnL: {pnl_pct:+.2%} | {hours_held:.1f}h elapsed"
        logger.info(status)
        return Intent.hold(reason=status)

    def _close_position(self) -> Intent:
        """Emit PERP_CLOSE and transition to closing state."""
        logger.info(f"Closing LONG {self.perp_market} | size=${self.position_size_usd}")

        self._previous_stable_state = self.STATE_OPEN
        self._state = self.STATE_CLOSING

        return Intent.perp_close(
            market=self.perp_market,
            collateral_token=self.collateral_token,
            is_long=True,
            size_usd=self.position_size_usd,
            max_slippage=self.max_slippage,
            protocol="gmx_v2",
        )

    # -------------------------------------------------------------------------
    # Execution callback
    # -------------------------------------------------------------------------

    def on_intent_executed(self, intent, success: bool, result) -> None:
        """Advance or rollback state machine based on execution outcome."""
        intent_type = getattr(intent, "intent_type", None)
        if not intent_type:
            return

        type_val = intent_type.value

        if success:
            if type_val == "PERP_OPEN" and self._state == self.STATE_OPENING:
                self._state = self.STATE_OPEN
                self._opened_at = datetime.now(UTC).isoformat()

                # Try to get entry price from result enrichment, fall back to pending
                extracted = getattr(result, "extracted_data", {}) or {}
                ep = extracted.get("entry_price")
                if ep is not None:
                    self._entry_price = Decimal(str(ep))
                elif self._pending_entry_price is not None:
                    self._entry_price = self._pending_entry_price
                self._pending_entry_price = None

                logger.info(
                    f"PERP_OPEN success -> state=open, entry_price={self._entry_price}"
                )

            elif type_val == "PERP_CLOSE" and self._state == self.STATE_CLOSING:
                self._state = self.STATE_DONE
                logger.info("PERP_CLOSE success -> state=done")
                self._entry_price = None
                self._opened_at = None
        else:
            # Rollback to previous stable state
            old_state = self._state
            self._state = self._previous_stable_state
            self._pending_entry_price = None
            logger.warning(
                f"{type_val} failed — rolling back: {old_state} -> {self._state}"
            )

    # -------------------------------------------------------------------------
    # State persistence
    # -------------------------------------------------------------------------

    def get_persistent_state(self) -> dict[str, Any]:
        """Serialize strategy state for persistence across restarts."""
        return {
            "state": self._state,
            "previous_stable_state": self._previous_stable_state,
            "entry_price": str(self._entry_price) if self._entry_price else None,
            "opened_at": self._opened_at,
        }

    def load_persistent_state(self, state: dict[str, Any]) -> None:
        """Restore strategy state from persisted data."""
        if not state:
            return
        self._state = state.get("state", self.STATE_IDLE)
        self._previous_stable_state = state.get("previous_stable_state", self.STATE_IDLE)
        ep = state.get("entry_price")
        self._entry_price = Decimal(ep) if ep else None
        self._opened_at = state.get("opened_at")
        logger.info(
            f"Restored state: {self._state}, entry_price={self._entry_price}, "
            f"opened_at={self._opened_at}"
        )

    # -------------------------------------------------------------------------
    # Status
    # -------------------------------------------------------------------------

    def get_status(self) -> dict[str, Any]:
        """Return current strategy status for monitoring."""
        return {
            "strategy": "edge_eth_funding_long",
            "signal_id": "4869b240-e380-42c6-95dd-a70c913a35a8",
            "chain": self.chain,
            "state": self._state,
            "entry_price": str(self._entry_price) if self._entry_price else None,
            "opened_at": self._opened_at,
            "position_size_usd": str(self.position_size_usd),
            "leverage": str(self.leverage),
            "take_profit_pct": str(self.take_profit_pct),
            "stop_loss_pct": str(self.stop_loss_pct),
            "time_horizon_hours": self.time_horizon_hours,
        }

    # -------------------------------------------------------------------------
    # Teardown (required)
    # -------------------------------------------------------------------------

    def get_open_positions(self):
        """Return open positions for teardown preview."""
        from almanak.framework.teardown import (
            PositionInfo,
            PositionType,
            TeardownPositionSummary,
        )

        positions = []
        if self._state in (self.STATE_OPEN, self.STATE_OPENING, self.STATE_CLOSING):
            positions.append(
                PositionInfo(
                    position_type=PositionType.PERP,
                    position_id="edge_eth_funding_long_perp",
                    chain=self.chain,
                    protocol="gmx_v2",
                    value_usd=self.position_size_usd,
                    details={
                        "market": self.perp_market,
                        "is_long": True,
                        "leverage": str(self.leverage),
                        "collateral_token": self.collateral_token,
                        "entry_price": str(self._entry_price) if self._entry_price else "unknown",
                        "opened_at": self._opened_at,
                    },
                )
            )

        return TeardownPositionSummary(
            deployment_id=getattr(self, "deployment_id", "edge_eth_funding_long"),
            timestamp=datetime.now(UTC),
            positions=positions,
        )

    def generate_teardown_intents(self, mode=None, market=None) -> list[Intent]:
        """Generate intents to unwind all positions."""
        from almanak.framework.teardown import TeardownMode

        intents: list[Intent] = []
        if self._state in (self.STATE_OPEN, self.STATE_OPENING, self.STATE_CLOSING):
            slippage = Decimal("0.03") if mode == TeardownMode.HARD else Decimal("0.005")
            intents.append(
                Intent.perp_close(
                    market=self.perp_market,
                    collateral_token=self.collateral_token,
                    is_long=True,
                    size_usd=self.position_size_usd,
                    max_slippage=slippage,
                    protocol="gmx_v2",
                )
            )
        return intents


if __name__ == "__main__":
    print("=" * 60)
    print("EdgeEthFundingLongStrategy")
    print("=" * 60)
    print(f"Strategy Name: {EdgeEthFundingLongStrategy.STRATEGY_NAME}")
    print(f"Supported Chains: {EdgeEthFundingLongStrategy.SUPPORTED_CHAINS}")
    print(f"Supported Protocols: {EdgeEthFundingLongStrategy.SUPPORTED_PROTOCOLS}")
    print(f"Intent Types: {EdgeEthFundingLongStrategy.INTENT_TYPES}")
    print("\nTo run this strategy:")
    print("  almanak strat run --network anvil --once")
