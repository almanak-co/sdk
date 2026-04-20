"""
===============================================================================
Ethena PT-sUSDe Leveraged Fixed Yield -- Atomic Flash Loan Entry/Exit
===============================================================================

This strategy locks in a fixed yield on Ethena sUSDe via Pendle Principal
Tokens (PT), then leverages the position using Morpho Blue flash loans.

HOW IT WORKS:
-------------
PT tokens are bought at a discount to face value. At maturity, they redeem 1:1
for the underlying (sUSDe). The discount IS your fixed yield. Leverage amplifies
this yield -- a 4.42% implied APY at 3x leverage becomes ~13.3% net.

ENTRY (atomic via flash loan):
  1. Flash loan (leverage-1) * capital USDC from Morpho
  2. Combine with own capital = total_amount USDC
  3. Swap all USDC -> PT-sUSDe via Pendle (bought at discount)
  4. Supply PT-sUSDe as collateral on Morpho (PT-sUSDe/USDC market, 91.5% LLTV)
  5. Borrow USDC from Morpho to repay flash loan

MONITORING:
  - Track health factor (PT price fluctuations vs USDC debt)
  - Monitor days to maturity (auto-exit before expiry)
  - Watch borrow rate vs locked-in PT yield

EXIT (atomic via flash loan):
  1. Flash loan USDC to cover total debt
  2. Repay all Morpho debt
  3. Withdraw all PT collateral
  4. Swap PT -> USDC via Pendle (or redeem at maturity)
  5. Repay flash loan, pocket profit

RISKS:
------
- LIQUIDATION: If PT discount widens significantly, HF can drop
- BORROW RATE: Morpho USDC rates can spike, eating into fixed yield
- PT LIQUIDITY: Pendle AMM may have thin liquidity near maturity
- SMART CONTRACT: Multiple protocol interactions increase surface area

USAGE:
------
    almanak strat run -d strategies/reference/ethena_pt_leverage --fresh --once --network anvil

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

# Morpho market LLTV for PT-sUSDe collateral
DEFAULT_LLTV = Decimal("0.915")


@almanak_strategy(
    name="ethena_pt_leverage",
    description="Leveraged fixed yield via Pendle PT-sUSDe and Morpho Blue flash loans",
    version="1.0.0",
    author="Almanak",
    tags=["ethena", "pendle", "morpho", "leverage", "fixed-yield", "pt", "flash-loan"],
    supported_chains=["ethereum"],
    supported_protocols=["pendle", "morpho_blue", "enso"],
    intent_types=["FLASH_LOAN", "SWAP", "SUPPLY", "BORROW", "REPAY", "WITHDRAW", "HOLD"],
)
class EthenaPTLeverageStrategy(IntentStrategy):
    """Leveraged PT-sUSDe fixed yield via Morpho Blue flash loans.

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
        self.pt_token = get_config("pt_token", "PT-sUSDe")
        self.pt_token_address = get_config("pt_token_address", "")
        self.pendle_market = get_config("pendle_market", "")
        self.borrow_token = get_config("borrow_token", "USDC")

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

        logger.info(
            f"EthenaPTLeverage initialized: pt={self.pt_token}, "
            f"leverage={self.target_leverage}x, market={self.morpho_market_id[:16]}..."
        )

    # =========================================================================
    # MAIN DECISION LOGIC
    # =========================================================================

    def decide(self, market: MarketSnapshot) -> Intent | None:
        """Advance the state machine by one step."""
        handler = {
            "idle": self._handle_idle,
            "entering": self._handle_entering,
            "active": self._handle_active,
            "monitoring": self._handle_monitoring,
            "exiting": self._handle_exiting,
            "settled": self._handle_settled,
        }.get(self._phase)

        if handler:
            return handler(market)

        return Intent.hold(reason=f"Unknown phase: {self._phase}")


    # =========================================================================
    # STATE HANDLERS
    # =========================================================================

    def _handle_idle(self, market: MarketSnapshot) -> Intent:
        """IDLE: Check USDC balance and initiate leveraged entry."""
        usdc_balance = self._get_balance(market, self.borrow_token)

        if usdc_balance < Decimal("100"):
            return Intent.hold(
                reason=f"Insufficient {self.borrow_token}: {usdc_balance} < 100"
            )

        self._initial_capital = usdc_balance
        logger.info(
            f"Entering leveraged position: {usdc_balance} {self.borrow_token} "
            f"at {self.target_leverage}x leverage"
        )

        # Build atomic flash loan entry
        self._transition("idle", "entering")
        return self._build_entry_intent(usdc_balance)

    def _handle_entering(self, market: MarketSnapshot) -> Intent:
        """ENTERING: Flash loan entry completed, transition to monitoring."""
        # The flash loan intent is atomic -- if we're here, it either succeeded or failed.
        # Check if we have PT collateral on Morpho (indicating success).
        # In practice, on_intent_executed will update state.
        if self._total_pt_collateral > Decimal("0"):
            logger.info(
                f"Position entered: {self._total_pt_collateral} {self.pt_token} collateral, "
                f"{self._total_debt} {self.borrow_token} debt"
            )
            self._transition("entering", "monitoring")
            return self._handle_monitoring(market)

        # Still waiting for entry confirmation
        return Intent.hold(reason="Waiting for flash loan entry confirmation")

    def _handle_active(self, market: MarketSnapshot) -> Intent:
        """ACTIVE: Transition to monitoring."""
        self._transition("active", "monitoring")
        return self._handle_monitoring(market)

    def _handle_monitoring(self, market: MarketSnapshot) -> Intent:
        """MONITORING: Check health factor and maturity, deleverage if needed."""
        # Reconcile internal state against on-chain balances before any decisions
        self._reconcile_state(market)

        # Update health factor estimate
        self._update_health_factor(market)

        # Check if we should exit due to maturity proximity
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

        # Calculate P&L
        pnl_info = self._calculate_pnl(market)

        return Intent.hold(
            reason=f"Monitoring -- HF: {self._current_health_factor:.3f}, "
            f"Leverage: {self.target_leverage}x, "
            f"Collateral: {self._total_pt_collateral:.2f} {self.pt_token}, "
            f"Debt: {self._total_debt:.2f} {self.borrow_token}, "
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
        usdc_balance = self._get_balance(market, self.borrow_token)
        profit = usdc_balance - self._initial_capital

        return Intent.hold(
            reason=f"Settled -- Initial: {self._initial_capital} {self.borrow_token}, "
            f"Final: {usdc_balance} {self.borrow_token}, "
            f"P&L: {profit:+.2f} {self.borrow_token}"
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
            chain="ethereum",
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
            chain="ethereum",
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
            logger.warning(f"PT price unavailable for {self.pt_token} -- invalidating HF")
            self._current_health_factor = Decimal("0")
            return

        try:
            borrow_price = market.price(self.borrow_token)
        except (ValueError, KeyError):
            logger.warning(f"Borrow token price unavailable for {self.borrow_token} -- invalidating HF")
            self._current_health_factor = Decimal("0")
            return

        collateral_value = self._total_pt_collateral * pt_price
        debt_value = self._total_debt * borrow_price
        if debt_value > Decimal("0"):
            self._current_health_factor = (collateral_value * self.lltv) / debt_value

    def _estimate_days_to_maturity(self) -> int | None:
        """Estimate days until PT maturity from token name.

        PT names follow the pattern: PT-sUSDe-7MAY2026
        """
        import re
        from datetime import datetime as dt

        match = re.search(r"(\d{1,2})([A-Z]{3})(\d{4})", self.pt_token)
        if not match:
            return None

        day, month_str, year = match.groups()
        months = {
            "JAN": 1, "FEB": 2, "MAR": 3, "APR": 4, "MAY": 5, "JUN": 6,
            "JUL": 7, "AUG": 8, "SEP": 9, "OCT": 10, "NOV": 11, "DEC": 12,
        }
        month = months.get(month_str)
        if month is None:
            return None

        try:
            maturity = dt(int(year), month, int(day), tzinfo=UTC)
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
            return "unavailable (no PT price data)"

        try:
            borrow_price = market.price(self.borrow_token)
        except (ValueError, KeyError):
            return "unavailable (no borrow token price data)"

        position_value = self._total_pt_collateral * pt_price
        debt_value = self._total_debt * borrow_price
        equity = position_value - debt_value
        pnl = equity - self._initial_capital

        return f"{pnl:+.2f} {self.borrow_token}"

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

    def _reconcile_state(self, market: MarketSnapshot) -> None:
        """Reconcile internal state against on-chain balances.

        Called at the start of each monitoring cycle to correct any drift
        between tracked state and actual on-chain position. Atomicity
        protects against partial execution, but slippage/rounding can
        still cause tracked amounts to diverge from reality.
        """
        if self._phase not in ("monitoring", "entering", "exiting"):
            return

        try:
            # Query on-chain PT balance as ground truth for collateral
            pt_balance = self._get_balance(market, self.pt_token)
            if pt_balance > Decimal("0") and pt_balance != self._total_pt_collateral:
                drift = abs(pt_balance - self._total_pt_collateral)
                if drift > Decimal("0.01"):
                    logger.info(
                        f"State reconciliation: PT collateral {self._total_pt_collateral} -> {pt_balance} "
                        f"(drift: {drift})"
                    )
                    self._total_pt_collateral = pt_balance
        except Exception:
            logger.debug("Could not reconcile PT collateral from on-chain data")

    def on_intent_executed(self, intent: Intent, success: bool, result: Any) -> None:
        """Update state tracking after intent execution.

        Prefers receipt-derived data from ResultEnricher when available,
        falls back to intent parameters as an approximation.
        """
        intent_type = intent.intent_type.value

        if not success:
            logger.warning(f"{intent_type} failed in phase {self._phase}")
            return

        if intent_type == "FLASH_LOAN":
            if self._phase == "entering":
                # Try to extract actual amounts from execution result
                collateral_amount = Decimal("0")
                debt_amount = Decimal("0")

                # Prefer receipt-derived data from ResultEnricher
                if result and hasattr(result, "extracted_data"):
                    ed = result.extracted_data
                    if isinstance(ed, dict):
                        if "collateral_supplied" in ed:
                            collateral_amount = Decimal(str(ed["collateral_supplied"]))
                        if "debt_borrowed" in ed:
                            debt_amount = Decimal(str(ed["debt_borrowed"]))

                if result and hasattr(result, "swap_amounts") and result.swap_amounts:
                    sa = result.swap_amounts
                    if hasattr(sa, "amount_out_decimal") and sa.amount_out_decimal:
                        collateral_amount = sa.amount_out_decimal

                # Fall back to intent-based estimates if no receipt data
                if collateral_amount <= Decimal("0"):
                    collateral_amount = self._initial_capital * self.target_leverage
                    logger.warning(
                        "No receipt data for collateral -- using intent estimate "
                        f"({collateral_amount}). State may drift from on-chain reality."
                    )
                if debt_amount <= Decimal("0"):
                    debt_amount = getattr(intent, "amount", Decimal("0"))

                self._total_pt_collateral = collateral_amount
                self._total_debt = debt_amount
                self._entry_timestamp = datetime.now(UTC).isoformat()
                logger.info(
                    f"Entry complete: {self._total_pt_collateral} {self.pt_token} collateral, "
                    f"{self._total_debt} {self.borrow_token} debt"
                )
            elif self._phase == "exiting":
                # Exit flash loan succeeded -- position is unwound
                logger.info("Exit complete: position unwound")
                self._total_pt_collateral = Decimal("0")
                self._total_debt = Decimal("0")

    # =========================================================================
    # STATUS & PERSISTENCE
    # =========================================================================

    def get_status(self) -> dict[str, Any]:
        return {
            "strategy": "ethena_pt_leverage",
            "chain": self.chain,
            "wallet": self.wallet_address[:10] + "..." if self.wallet_address else "N/A",
            "config": {
                "pt_token": self.pt_token,
                "morpho_market": self.morpho_market_id[:20] + "...",
                "target_leverage": str(self.target_leverage),
                "min_health_factor": str(self.min_health_factor),
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

    def get_open_positions(self) -> "TeardownPositionSummary":  # noqa: F821
        from almanak.framework.teardown import PositionInfo, PositionType, TeardownPositionSummary

        positions = []
        if self._total_pt_collateral > Decimal("0"):
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
        if self._total_debt > Decimal("0"):
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
        return TeardownPositionSummary(
            strategy_id=self.STRATEGY_NAME,
            timestamp=datetime.now(UTC),
            positions=positions,
        )

    def generate_teardown_intents(self, mode: "TeardownMode", market=None) -> list[Intent]:  # noqa: F821
        """Teardown uses the flash loan unwind for atomic exit."""
        if self._total_debt > Decimal("0"):
            return [self._build_exit_intent()]
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
    meta = EthenaPTLeverageStrategy.STRATEGY_METADATA
    print("=" * 70)
    print("EthenaPTLeverageStrategy -- Leveraged Fixed Yield via Pendle PT")
    print("=" * 70)
    print(f"\nStrategy: {meta.name}")
    print(f"Chains: {meta.supported_chains}")
    print(f"Protocols: {meta.supported_protocols}")
    print(f"Intents: {meta.intent_types}")
