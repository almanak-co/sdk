"""
===============================================================================
Pendle PT-wstETH Leveraged Fixed Yield -- Atomic Flash Loan Entry/Exit
===============================================================================

This strategy locks in a fixed yield on wstETH via Pendle Principal Tokens (PT),
then leverages the position using Morpho Blue flash loans on Arbitrum.

HOW IT WORKS:
-------------
PT tokens are bought at a discount to face value. At maturity, they redeem 1:1
for the underlying (wstETH). The discount IS your fixed yield. Leverage amplifies
this yield -- a 3% implied APY at 3x leverage becomes ~9% net.

ENTRY (atomic via flash loan):
  1. Flash loan (leverage-1) * capital WETH from Morpho
  2. Combine with own capital = total_amount WETH
  3. Swap all WETH -> PT-wstETH via Pendle (bought at discount)
  4. Supply PT-wstETH as collateral on Morpho (PT-wstETH/WETH market)
  5. Borrow WETH from Morpho to repay flash loan

MONITORING:
  - Track health factor (PT price fluctuations vs WETH debt)
  - Monitor days to maturity (auto-exit before expiry)
  - Watch borrow rate vs locked-in PT yield

EXIT (atomic via flash loan):
  1. Flash loan WETH to cover total debt
  2. Repay all Morpho debt
  3. Withdraw all PT collateral
  4. Swap PT -> WETH via Pendle (or redeem at maturity)
  5. Repay flash loan, pocket profit

KNOWN GAPS:
-----------
- No Morpho Blue PT-wstETH market exists on Arbitrum as of 2026-02-19.
  The strategy is structurally complete but the leverage loop will fail
  until a PT-wstETH/WETH market is created on Morpho Blue Arbitrum.
- Pendle swap (WETH -> PT-wstETH) should work independently.

RISKS:
------
- LIQUIDATION: If PT discount widens significantly, HF can drop
- BORROW RATE: Morpho WETH rates can spike, eating into fixed yield
- PT LIQUIDITY: Pendle AMM may have thin liquidity near maturity
- SMART CONTRACT: Multiple protocol interactions increase surface area

USAGE:
------
    almanak strat run -d strategies/incubating/pendle_pt_wsteth_leverage --fresh --once --network anvil

===============================================================================
"""

import logging
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from almanak.framework.api.timeline import TimelineEvent, TimelineEventType, add_event
from almanak.framework.intents import Intent
from almanak.framework.intents.pt_leverage import (
    build_pt_leverage_loop,
    build_pt_leverage_unwind,
)
from almanak.framework.strategies import (
    IntentStrategy,
    MarketSnapshot,
    almanak_strategy,
)

logger = logging.getLogger(__name__)

# Default LLTV -- assumed 86% for a hypothetical PT-wstETH/WETH market
DEFAULT_LLTV = Decimal("0.86")


@almanak_strategy(
    name="pendle_pt_wsteth_leverage",
    description="Leveraged fixed yield via Pendle PT-wstETH and Morpho Blue flash loans on Arbitrum",
    version="0.1.0",
    author="Almanak",
    tags=["pendle", "morpho", "leverage", "fixed-yield", "pt", "flash-loan", "wstETH", "arbitrum"],
    supported_chains=["arbitrum"],
    supported_protocols=["pendle", "morpho_blue", "enso"],
    intent_types=["FLASH_LOAN", "SWAP", "SUPPLY", "BORROW", "REPAY", "WITHDRAW", "HOLD"],
)
class PendlePTwstETHLeverageStrategy(IntentStrategy):
    """Leveraged PT-wstETH fixed yield via Morpho Blue flash loans on Arbitrum.

    State machine phases:
        idle -> entering -> active -> monitoring -> exiting -> settled

    Entry and exit are atomic single-intent operations using flash loans.
    Monitoring checks health factor and days to maturity each cycle.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        def get_config(key: str, default: Any) -> Any:
            if isinstance(self.config, dict):
                return self.config.get(key, default)
            if hasattr(self.config, "get"):
                return self.config.get(key, default)
            return getattr(self.config, key, default)

        # Market configuration
        self.morpho_market_id = get_config("morpho_market_id", "")
        self.pt_token = get_config("pt_token", "PT-wstETH")
        self.pt_token_address = get_config("pt_token_address", "0x71fbf40651e9d4278a74586afc99f307f369ce9a")
        self.pendle_market = get_config("pendle_market", "0xf78452e0f5c0b95fc5dc8353b8cd1e06e53fa25b")
        self.maturity_date = get_config("maturity_date", "2026-06-25")  # ISO date for PT maturity
        self.borrow_token = get_config("borrow_token", "WETH")

        # Leverage parameters
        self.target_leverage = Decimal(str(get_config("target_leverage", "3.0")))
        self.lltv = Decimal(str(get_config("lltv", str(DEFAULT_LLTV))))
        self.min_health_factor = Decimal(str(get_config("min_health_factor", "1.3")))
        self.max_slippage = Decimal(str(get_config("max_slippage", "0.01")))
        self.exit_days_before_maturity = int(get_config("exit_days_before_maturity", 7))

        # State machine
        self._phase = "idle"

        # Position tracking
        self._initial_capital = Decimal("0")
        self._total_pt_collateral = Decimal("0")
        self._total_debt = Decimal("0")
        self._entry_timestamp: str | None = None

        # Health tracking
        self._current_health_factor = Decimal("0")
        self._pt_price = Decimal("0")

        # Check for missing Morpho market
        self._morpho_market_missing = (
            not self.morpho_market_id
            or self.morpho_market_id.startswith("MISSING")
        )
        if self._morpho_market_missing:
            logger.warning(
                "No Morpho Blue PT-wstETH market configured for Arbitrum. "
                "Leverage loop will be unavailable. Pendle swap-only mode."
            )

        market_display = self.morpho_market_id[:16] + "..." if len(self.morpho_market_id) > 16 else self.morpho_market_id
        logger.info(
            f"PendlePTwstETHLeverage initialized: pt={self.pt_token}, "
            f"leverage={self.target_leverage}x, market={market_display}"
        )

    # =========================================================================
    # MAIN DECISION LOGIC
    # =========================================================================

    def decide(self, market: MarketSnapshot) -> Intent | None:
        """Advance the state machine by one step."""
        try:
            handler = {
                "idle": self._handle_idle,
                "entering": self._handle_entering,
                "swap_only": self._handle_swap_only,
                "active": self._handle_active,
                "monitoring": self._handle_monitoring,
                "exiting": self._handle_exiting,
                "settled": self._handle_settled,
            }.get(self._phase)

            if handler:
                return handler(market)

            return Intent.hold(reason=f"Unknown phase: {self._phase}")

        except Exception as e:
            logger.exception(f"Error in decide(): {e}")
            return Intent.hold(reason=f"Error: {str(e)}")

    # =========================================================================
    # STATE HANDLERS
    # =========================================================================

    def _handle_idle(self, market: MarketSnapshot) -> Intent:
        """IDLE: Check WETH balance and initiate entry.

        If Morpho market is missing, fall back to swap-only mode
        (buy PT-wstETH without leverage).
        """
        weth_balance = self._get_balance(market, self.borrow_token)

        min_balance = Decimal("0.01")
        if weth_balance < min_balance:
            return Intent.hold(
                reason=f"Insufficient {self.borrow_token}: {weth_balance} < {min_balance}"
            )

        self._initial_capital = weth_balance

        if self._morpho_market_missing:
            # Fallback: swap-only mode (no leverage)
            logger.info(
                f"Morpho market missing -- swap-only mode: "
                f"{weth_balance} {self.borrow_token} -> {self.pt_token}"
            )
            self._transition("idle", "swap_only")
            return Intent.swap(
                from_token=self.borrow_token,
                to_token=self.pt_token,
                amount=weth_balance,
                max_slippage=self.max_slippage,
                protocol="pendle",
            )

        # Full leverage loop
        logger.info(
            f"Entering leveraged position: {weth_balance} {self.borrow_token} "
            f"at {self.target_leverage}x leverage"
        )
        self._transition("idle", "entering")
        return self._build_entry_intent(weth_balance)

    def _handle_entering(self, market: MarketSnapshot) -> Intent:
        """ENTERING: Flash loan entry completed, transition to monitoring."""
        if self._total_pt_collateral > Decimal("0"):
            logger.info(
                f"Position entered: {self._total_pt_collateral} {self.pt_token} collateral, "
                f"{self._total_debt} {self.borrow_token} debt"
            )
            self._transition("entering", "monitoring")
            return self._handle_monitoring(market)

        return Intent.hold(reason="Waiting for flash loan entry confirmation")

    def _handle_swap_only(self, market: MarketSnapshot) -> Intent:
        """SWAP_ONLY: Pendle swap completed without leverage. Report result."""
        # Check if we now hold PT tokens
        pt_balance = self._get_balance(market, self.pt_token)

        if pt_balance > Decimal("0"):
            self._total_pt_collateral = pt_balance
            logger.info(
                f"Swap-only position: {pt_balance} {self.pt_token} "
                f"(no leverage, Morpho market unavailable)"
            )
            self._transition("swap_only", "settled")
            return self._handle_settled(market)

        return Intent.hold(
            reason=f"Swap-only mode: waiting for {self.pt_token} balance confirmation"
        )

    def _handle_active(self, market: MarketSnapshot) -> Intent:
        """ACTIVE: Transition to monitoring."""
        self._transition("active", "monitoring")
        return self._handle_monitoring(market)

    def _handle_monitoring(self, market: MarketSnapshot) -> Intent:
        """MONITORING: Check health factor and maturity, deleverage if needed."""
        self._update_health_factor(market)

        # Check maturity proximity
        days_to_maturity = self._estimate_days_to_maturity()
        if days_to_maturity is not None and days_to_maturity <= self.exit_days_before_maturity:
            logger.warning(
                f"Near maturity ({days_to_maturity} days), initiating exit"
            )
            self._transition("monitoring", "exiting")
            return self._build_exit_intent()

        # Check health factor
        if self._current_health_factor > Decimal("0") and self._current_health_factor < self.min_health_factor:
            logger.warning(
                f"Health factor {self._current_health_factor:.3f} < {self.min_health_factor}, "
                f"initiating emergency exit"
            )
            self._transition("monitoring", "exiting")
            return self._build_exit_intent()

        pnl_info = self._calculate_pnl(market)

        return Intent.hold(
            reason=f"Monitoring -- HF: {self._current_health_factor:.3f}, "
            f"Leverage: {self.target_leverage}x, "
            f"Collateral: {self._total_pt_collateral:.4f} {self.pt_token}, "
            f"Debt: {self._total_debt:.4f} {self.borrow_token}, "
            f"Days to maturity: {days_to_maturity or 'unknown'}, "
            f"P&L: {pnl_info}"
        )

    def _handle_exiting(self, market: MarketSnapshot) -> Intent:
        """EXITING: Flash loan exit completed, transition to settled."""
        if self._total_debt <= Decimal("0"):
            logger.info("Position fully unwound")
            self._transition("exiting", "settled")
            return self._handle_settled(market)

        return Intent.hold(reason="Waiting for flash loan exit confirmation")

    def _handle_settled(self, market: MarketSnapshot) -> Intent:
        """SETTLED: Position closed, report final P&L."""
        weth_balance = self._get_balance(market, self.borrow_token)
        profit = weth_balance - self._initial_capital

        mode = "swap-only" if self._morpho_market_missing else "leveraged"
        return Intent.hold(
            reason=f"Settled ({mode}) -- Initial: {self._initial_capital:.4f} {self.borrow_token}, "
            f"Final: {weth_balance:.4f} {self.borrow_token}, "
            f"PT held: {self._total_pt_collateral:.4f} {self.pt_token}, "
            f"P&L: {profit:+.4f} {self.borrow_token}"
        )

    # =========================================================================
    # INTENT CREATION
    # =========================================================================

    def _build_entry_intent(self, capital: Decimal) -> Intent:
        """Build atomic flash loan entry using PT leverage loop factory."""
        logger.info(
            f"FLASH LOAN ENTRY: {capital} {self.borrow_token} -> "
            f"{self.pt_token} at {self.target_leverage}x"
        )

        days_to_maturity = self._estimate_days_to_maturity()

        flash_intent = build_pt_leverage_loop(
            borrow_token=self.borrow_token,
            pt_token=self.pt_token,
            morpho_market_id=self.morpho_market_id,
            initial_amount=capital,
            target_leverage=self.target_leverage,
            lltv=self.lltv,
            max_slippage=self.max_slippage,
            chain="arbitrum",
            days_to_maturity=days_to_maturity,
        )

        add_event(
            TimelineEvent(
                timestamp=datetime.now(UTC),
                event_type=TimelineEventType.POSITION_MODIFIED,
                description=f"Flash loan entry: {capital} {self.borrow_token} at {self.target_leverage}x",
                strategy_id=self.strategy_id,
                details={
                    "action": "pt_leverage_entry",
                    "capital": str(capital),
                    "leverage": str(self.target_leverage),
                    "pt_token": self.pt_token,
                    "morpho_market": self.morpho_market_id,
                },
            )
        )

        return flash_intent

    def _build_exit_intent(self) -> Intent:
        """Build atomic flash loan exit using PT leverage unwind factory."""
        logger.info(
            f"FLASH LOAN EXIT: Unwinding {self._total_pt_collateral} {self.pt_token}, "
            f"repaying {self._total_debt} {self.borrow_token}"
        )

        flash_intent = build_pt_leverage_unwind(
            borrow_token=self.borrow_token,
            pt_token=self.pt_token,
            morpho_market_id=self.morpho_market_id,
            total_debt=self._total_debt,
            max_slippage=self.max_slippage,
            chain="arbitrum",
        )

        add_event(
            TimelineEvent(
                timestamp=datetime.now(UTC),
                event_type=TimelineEventType.POSITION_MODIFIED,
                description=f"Flash loan exit: unwinding {self._total_pt_collateral} {self.pt_token}",
                strategy_id=self.strategy_id,
                details={
                    "action": "pt_leverage_exit",
                    "collateral": str(self._total_pt_collateral),
                    "debt": str(self._total_debt),
                },
            )
        )

        return flash_intent

    # =========================================================================
    # HELPERS
    # =========================================================================

    def _get_balance(self, market: MarketSnapshot, token: str) -> Decimal:
        """Get token balance, returning 0 on error."""
        try:
            bal = market.balance(token)
            return bal.balance if hasattr(bal, "balance") else bal
        except (ValueError, KeyError, AttributeError):
            return Decimal("0")

    def _update_health_factor(self, market: MarketSnapshot) -> None:
        """Update estimated health factor from on-chain data."""
        if self._total_debt <= Decimal("0") or self._total_pt_collateral <= Decimal("0"):
            return

        try:
            pt_price = market.price(self.pt_token)
            self._pt_price = pt_price
        except (ValueError, KeyError):
            logger.warning(f"PT price unavailable for {self.pt_token} -- skipping HF update")
            return

        try:
            borrow_price = market.price(self.borrow_token)
        except (ValueError, KeyError):
            # WETH price ~= ETH price, fallback to ETH
            try:
                borrow_price = market.price("ETH")
            except (ValueError, KeyError):
                logger.warning(f"Borrow token price unavailable for {self.borrow_token} -- skipping HF update")
                return

        collateral_value = self._total_pt_collateral * pt_price
        debt_value = self._total_debt * borrow_price
        if debt_value > Decimal("0"):
            self._current_health_factor = (collateral_value * self.lltv) / debt_value

    def _estimate_days_to_maturity(self) -> int | None:
        """Estimate days until PT maturity from config maturity_date (ISO format)."""
        from datetime import datetime as dt

        if not self.maturity_date:
            return None

        try:
            maturity = dt.fromisoformat(self.maturity_date).replace(tzinfo=UTC)
            now = dt.now(UTC)
            delta = (maturity - now).days
            return max(delta, 0)
        except (ValueError, OverflowError):
            return None

    def _calculate_pnl(self, market: MarketSnapshot) -> str:
        """Calculate estimated P&L."""
        if self._total_pt_collateral <= Decimal("0"):
            return "no position"

        try:
            pt_price = market.price(self.pt_token)
        except (ValueError, KeyError):
            pt_price = Decimal("0.95")  # PT trades at ~5% discount

        try:
            borrow_price = market.price(self.borrow_token)
        except (ValueError, KeyError):
            borrow_price = Decimal("1")  # WETH ~= 1 ETH

        position_value = self._total_pt_collateral * pt_price
        debt_value = self._total_debt * borrow_price
        equity = position_value - debt_value
        pnl = equity - self._initial_capital

        return f"{pnl:+.4f} {self.borrow_token}"

    def _transition(self, old: str, new: str) -> None:
        """Transition state machine phase."""
        self._phase = new
        add_event(
            TimelineEvent(
                timestamp=datetime.now(UTC),
                event_type=TimelineEventType.STATE_CHANGE,
                description=f"Phase: {old.upper()} -> {new.upper()}",
                strategy_id=self.strategy_id,
                details={"old_phase": old, "new_phase": new},
            )
        )

    # =========================================================================
    # LIFECYCLE HOOKS
    # =========================================================================

    def on_intent_executed(self, intent: Intent, success: bool, result: Any) -> None:
        """Update state tracking after intent execution."""
        intent_type = intent.intent_type.value

        if not success:
            logger.warning(f"{intent_type} failed in phase {self._phase}")
            return

        if intent_type == "FLASH_LOAN":
            if self._phase == "entering":
                flash_amount = getattr(intent, "amount", Decimal("0"))
                total_bought = self._initial_capital * self.target_leverage
                self._total_pt_collateral = total_bought  # approximate
                self._total_debt = flash_amount
                self._entry_timestamp = datetime.now(UTC).isoformat()
                logger.info(
                    f"Entry complete: ~{self._total_pt_collateral} {self.pt_token} collateral, "
                    f"{self._total_debt} {self.borrow_token} debt"
                )
            elif self._phase == "exiting":
                logger.info("Exit complete: position unwound")
                self._total_pt_collateral = Decimal("0")
                self._total_debt = Decimal("0")

        elif intent_type == "SWAP":
            if self._phase == "swap_only":
                # In swap-only mode, track the PT we received
                logger.info(f"Swap-only: {self.borrow_token} -> {self.pt_token} executed")

    # =========================================================================
    # STATUS & PERSISTENCE
    # =========================================================================

    def get_status(self) -> dict[str, Any]:
        return {
            "strategy": "pendle_pt_wsteth_leverage",
            "chain": self.chain,
            "wallet": self.wallet_address[:10] + "..." if self.wallet_address else "N/A",
            "config": {
                "pt_token": self.pt_token,
                "morpho_market": self.morpho_market_id[:20] + "..." if len(self.morpho_market_id) > 20 else self.morpho_market_id,
                "target_leverage": str(self.target_leverage),
                "min_health_factor": str(self.min_health_factor),
                "morpho_market_missing": self._morpho_market_missing,
            },
            "state": {
                "phase": self._phase,
                "initial_capital": str(self._initial_capital),
                "total_pt_collateral": str(self._total_pt_collateral),
                "total_debt": str(self._total_debt),
                "health_factor": str(self._current_health_factor),
                "days_to_maturity": self._estimate_days_to_maturity(),
            },
        }

    def get_persistent_state(self) -> dict[str, Any]:
        return {
            "phase": self._phase,
            "initial_capital": str(self._initial_capital),
            "total_pt_collateral": str(self._total_pt_collateral),
            "total_debt": str(self._total_debt),
            "current_health_factor": str(self._current_health_factor),
            "pt_price": str(self._pt_price),
            "entry_timestamp": self._entry_timestamp,
        }

    def load_persistent_state(self, state: dict[str, Any]) -> None:
        if "phase" in state:
            self._phase = state["phase"]
        if "initial_capital" in state:
            self._initial_capital = Decimal(str(state["initial_capital"]))
        if "total_pt_collateral" in state:
            self._total_pt_collateral = Decimal(str(state["total_pt_collateral"]))
        if "total_debt" in state:
            self._total_debt = Decimal(str(state["total_debt"]))
        if "current_health_factor" in state:
            self._current_health_factor = Decimal(str(state["current_health_factor"]))
        if "pt_price" in state:
            self._pt_price = Decimal(str(state["pt_price"]))
        if "entry_timestamp" in state:
            self._entry_timestamp = state["entry_timestamp"]
        logger.info(
            f"Restored state: phase={self._phase}, "
            f"collateral={self._total_pt_collateral} {self.pt_token}, "
            f"debt={self._total_debt} {self.borrow_token}"
        )

    # =========================================================================
    # TEARDOWN
    # =========================================================================

    def supports_teardown(self) -> bool:
        return True

    def get_open_positions(self) -> "TeardownPositionSummary":  # noqa: F821
        from almanak.framework.teardown import PositionInfo, PositionType, TeardownPositionSummary

        positions = []
        if self._total_pt_collateral > Decimal("0"):
            if self._total_debt > Decimal("0"):
                # Leveraged position: collateral on Morpho
                positions.append(
                    PositionInfo(
                        position_type=PositionType.SUPPLY,
                        position_id=f"morpho-pt-{self.morpho_market_id[:16]}",
                        chain=self.chain,
                        protocol="morpho_blue",
                        value_usd=self._total_pt_collateral * self._pt_price if self._pt_price else self._total_pt_collateral,
                        details={
                            "market_id": self.morpho_market_id,
                            "asset": self.pt_token,
                            "amount": str(self._total_pt_collateral),
                        },
                    )
                )
                positions.append(
                    PositionInfo(
                        position_type=PositionType.BORROW,
                        position_id=f"morpho-debt-{self.morpho_market_id[:16]}",
                        chain=self.chain,
                        protocol="morpho_blue",
                        value_usd=self._total_debt,
                        health_factor=self._current_health_factor,
                        details={
                            "market_id": self.morpho_market_id,
                            "asset": self.borrow_token,
                            "amount": str(self._total_debt),
                        },
                    )
                )
            else:
                # Swap-only position: just holding PT tokens
                positions.append(
                    PositionInfo(
                        position_type=PositionType.TOKEN,
                        position_id="pendle-pt-wsteth-swap-only",
                        chain=self.chain,
                        protocol="pendle",
                        value_usd=self._total_pt_collateral * self._pt_price if self._pt_price else self._total_pt_collateral,
                        details={
                            "asset": self.pt_token,
                            "amount": str(self._total_pt_collateral),
                            "mode": "swap-only (no Morpho market)",
                        },
                    )
                )
        return TeardownPositionSummary(
            strategy_id=self.STRATEGY_NAME,
            timestamp=datetime.now(UTC),
            positions=positions,
        )

    def generate_teardown_intents(self, mode: "TeardownMode", market=None) -> list[Intent]:  # noqa: F821
        """Teardown: flash loan unwind for leveraged, simple swap for swap-only."""
        if self._total_debt > Decimal("0"):
            return [self._build_exit_intent()]
        elif self._total_pt_collateral > Decimal("0"):
            # Swap-only: sell PT back to WETH
            from almanak.framework.teardown import TeardownMode

            max_slippage = Decimal("0.05") if mode == TeardownMode.HARD else self.max_slippage
            return [
                Intent.swap(
                    from_token=self.pt_token,
                    to_token=self.borrow_token,
                    amount="all",
                    max_slippage=max_slippage,
                    protocol="pendle",
                )
            ]
        return []

    def on_teardown_started(self, mode: "TeardownMode") -> None:  # noqa: F821
        from almanak.framework.teardown import TeardownMode

        mode_name = "graceful" if mode == TeardownMode.SOFT else "emergency"
        logger.info(
            f"Teardown ({mode_name}): unwinding {self._total_pt_collateral} {self.pt_token}, "
            f"repaying {self._total_debt} {self.borrow_token}"
        )

    def on_teardown_completed(self, success: bool, recovered_usd: Decimal) -> None:
        if success:
            logger.info(f"Teardown completed. Recovered ${recovered_usd:,.2f}")
            self._phase = "settled"
            self._total_pt_collateral = Decimal("0")
            self._total_debt = Decimal("0")
        else:
            logger.error("Teardown failed -- manual intervention may be required")


if __name__ == "__main__":
    meta = PendlePTwstETHLeverageStrategy.STRATEGY_METADATA
    print("=" * 70)
    print("PendlePTwstETHLeverageStrategy -- Leveraged Fixed Yield via Pendle PT")
    print("=" * 70)
    print(f"\nStrategy: {PendlePTwstETHLeverageStrategy.STRATEGY_NAME}")
    print(f"Chains: {meta.supported_chains}")
    print(f"Protocols: {meta.supported_protocols}")
    print(f"Intents: {meta.intent_types}")
    print("\nKNOWN GAP: No Morpho Blue PT-wstETH market on Arbitrum.")
    print("Strategy falls back to swap-only mode (WETH -> PT-wstETH via Pendle).")
