"""
===============================================================================
Pendle PT Maturity Rotator Strategy
===============================================================================

A signal-driven PT position manager that uses technical analysis on the
underlying asset to time entries, tracks maturity dates for auto-exits,
and implements dollar-cost averaging into PT positions.

HOW IT WORKS:
-------------
1. Monitors RSI on the underlying asset (e.g., wstETH)
2. When RSI < threshold (oversold), starts DCA buying PT in tranches
3. Holds PT position, monitoring maturity and underlying price
4. Auto-exits before maturity or on profit target / stop-loss
5. Waits for next cycle

STATE MACHINE:
--------------
    IDLE --> ACCUMULATING --> HOLDING --> EXITING --> IDLE
     |          |               |           |
     |  (RSI < 40, buy PT)     |    (sell PT -> base)
     |          |               |
     |  (repeat up to N        |
     |   DCA tranches)    (maturity window
     |                     or price floor hit)

CONFIGURATION:
--------------
See config.json for all parameters. Key ones:
- rsi_entry_threshold: RSI below this triggers entry (default 40)
- dca_tranches: Number of DCA tranches (default 4)
- max_pt_allocation_pct: Max % of capital in PT (default 60%)
- exit_days_before_maturity: Days before maturity to auto-exit (default 7)
- market_expiry: Market maturity date (e.g., "2026-06-25")

USAGE:
------
    almanak strat run -d strategies/demo/pendle_pt_rotator --network anvil --once

===============================================================================
"""

import logging
from datetime import UTC, datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Any

from almanak.framework.intents import Intent

if TYPE_CHECKING:
    from almanak.framework.teardown import TeardownMode, TeardownPositionSummary

from almanak.framework.strategies import (
    IntentStrategy,
    MarketSnapshot,
    almanak_strategy,
)

logger = logging.getLogger(__name__)


# Phase constants
PHASE_IDLE = "idle"
PHASE_ACCUMULATING = "accumulating"
PHASE_HOLDING = "holding"
PHASE_EXITING = "exiting"


@almanak_strategy(
    name="pendle_pt_rotator",
    description="Pendle PT position management with RSI-driven entries, DCA, and maturity-aware exits",
    version="1.0.0",
    author="Almanak",
    tags=["pendle", "yield", "pt", "fixed-yield", "rsi", "dca", "maturity"],
    supported_chains=["arbitrum"],
    supported_protocols=["pendle"],
    intent_types=["SWAP", "HOLD"],
)
class PendlePTRotatorStrategy(IntentStrategy):
    """Pendle PT Maturity Rotator - signal-driven PT position management.

    Uses RSI + MACD signals on the underlying asset to time PT entries,
    DCA for position building, and maturity-aware auto-exits.
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)

        def cfg(key: str, default: Any) -> Any:
            if isinstance(self.config, dict):
                return self.config.get(key, default)
            return getattr(self.config, key, default)

        # Market config
        self.base_token = cfg("base_token", "WSTETH")
        self.base_token_symbol = cfg("base_token_symbol", self.base_token)
        self.pt_token = cfg("pt_token", "PT-wstETH")
        self.pt_token_symbol = cfg("pt_token_symbol", self.pt_token)
        self.market_address = cfg("market_address", "0xf78452e0f5c0b95fc5dc8353b8cd1e06e53fa25b")
        self.market_expiry = datetime.strptime(cfg("market_expiry", "2026-06-25"), "%Y-%m-%d").replace(tzinfo=UTC)

        # Signal parameters
        self.rsi_entry_threshold = int(cfg("rsi_entry_threshold", 40))
        self.rsi_exit_threshold = int(cfg("rsi_exit_threshold", 70))
        self.rsi_period = int(cfg("rsi_period", 14))
        self.rsi_timeframe = cfg("rsi_timeframe", "4h")
        macd_cfg = cfg("require_macd_confirmation", True)
        self.require_macd_confirmation = macd_cfg if isinstance(macd_cfg, bool) else str(macd_cfg).lower() not in ("false", "0", "no")

        # DCA parameters
        self.dca_tranches = int(cfg("dca_tranches", 4))
        self.tranche_pct = Decimal(str(cfg("tranche_pct", "0.25")))

        # Risk parameters
        self.max_pt_allocation_pct = Decimal(str(cfg("max_pt_allocation_pct", "0.60")))
        self.min_days_to_maturity = int(cfg("min_days_to_maturity", 30))
        self.exit_days_before_maturity = int(cfg("exit_days_before_maturity", 7))
        self.max_slippage_bps = int(cfg("max_slippage_bps", 100))
        self.trade_size_token = Decimal(str(cfg("trade_size_token", "0.5")))
        self.underlying_price_floor_usd = Decimal(str(cfg("underlying_price_floor_usd", "1500")))

        # State (will be restored from persistent state if available)
        self._phase = PHASE_IDLE
        self._tranches_completed = 0
        self._total_pt_bought = Decimal("0")
        self._total_base_spent = Decimal("0")
        self._entry_prices: list[str] = []  # stored as strings for JSON serialization
        self._last_rsi: float | None = None
        self._cycle_count = 0

        logger.info(
            f"PendlePTRotator initialized: "
            f"market_expiry={self.market_expiry.date()}, "
            f"RSI_entry<{self.rsi_entry_threshold}, "
            f"DCA={self.dca_tranches} tranches, "
            f"max_alloc={self.max_pt_allocation_pct:.0%}"
        )

    # ─── Core Decision Logic ─────────────────────────────────────────

    def decide(self, market: MarketSnapshot) -> Intent | None:
        """Main decision method. Routes to phase-specific handlers."""
        try:
            days_to_maturity = self._days_to_maturity()

            # Global safety: check maturity
            if days_to_maturity <= self.exit_days_before_maturity and self._phase not in (PHASE_IDLE, PHASE_EXITING):
                logger.info(
                    f"Maturity approaching ({days_to_maturity} days left, "
                    f"threshold={self.exit_days_before_maturity}). Triggering exit."
                )
                self._phase = PHASE_EXITING

            # Global safety: check underlying price floor
            try:
                underlying_price = Decimal(str(market.price(self.base_token)))
                if underlying_price < self.underlying_price_floor_usd and self._phase in (
                    PHASE_ACCUMULATING,
                    PHASE_HOLDING,
                ):
                    logger.warning(
                        f"Underlying price ${underlying_price:.2f} below floor "
                        f"${self.underlying_price_floor_usd:.2f}. Emergency exit."
                    )
                    self._phase = PHASE_EXITING
            except (ValueError, TypeError) as e:
                self._price_floor_failures = getattr(self, "_price_floor_failures", 0) + 1
                logger.warning(
                    f"Price floor check unavailable (attempt {self._price_floor_failures}): {e}"
                )
                if self._price_floor_failures >= 3 and self._phase == PHASE_ACCUMULATING:
                    logger.warning("3 consecutive price failures during accumulation, pausing")
                    self._phase = PHASE_HOLDING
            else:
                self._price_floor_failures = 0  # Reset on success

            # Route to phase handler
            if self._phase == PHASE_IDLE:
                return self._handle_idle(market, days_to_maturity)
            elif self._phase == PHASE_ACCUMULATING:
                return self._handle_accumulating(market)
            elif self._phase == PHASE_HOLDING:
                return self._handle_holding(market, days_to_maturity)
            elif self._phase == PHASE_EXITING:
                return self._handle_exiting(market)
            else:
                logger.error(f"Unknown phase: {self._phase}, resetting to idle")
                self._phase = PHASE_IDLE
                return Intent.hold(reason="Phase reset")

        except Exception as e:
            logger.exception(f"Error in decide(): {e}")
            return Intent.hold(reason=f"Error: {e}")

    def _handle_idle(self, market: MarketSnapshot, days_to_maturity: int) -> Intent:
        """IDLE phase: wait for RSI entry signal."""
        # Don't enter new positions if market is too close to maturity
        if days_to_maturity < self.min_days_to_maturity:
            return Intent.hold(
                reason=f"Market expires in {days_to_maturity} days "
                f"(min={self.min_days_to_maturity}). Waiting for next maturity."
            )

        # Check RSI signal
        try:
            rsi = market.rsi(self.base_token, period=self.rsi_period, timeframe=self.rsi_timeframe)
            rsi_value = float(rsi.value)
            self._last_rsi = rsi_value
        except Exception as e:
            logger.debug(f"RSI unavailable: {e}")
            return Intent.hold(reason=f"RSI unavailable: {e}")

        # Check MACD for confirmation (optional)
        macd_bullish = not self.require_macd_confirmation  # bypass if not required
        if self.require_macd_confirmation:
            try:
                macd = market.macd(self.base_token, timeframe=self.rsi_timeframe)
                macd_bullish = macd.histogram > 0  # positive histogram = bullish momentum
            except Exception as e:
                logger.warning(f"MACD unavailable (allowing entry without confirmation): {e}")
                macd_bullish = True

        if rsi_value < self.rsi_entry_threshold and macd_bullish:
            logger.info(
                f"Entry signal: RSI={rsi_value:.1f} (threshold={self.rsi_entry_threshold}), "
                f"MACD={'bullish' if macd_bullish else 'bearish'}"
            )
            self._phase = PHASE_ACCUMULATING
            self._tranches_completed = 0
            self._cycle_count += 1
            return self._buy_pt_tranche(market)

        return Intent.hold(
            reason=f"Waiting for entry signal: RSI={rsi_value:.1f} "
            f"(need <{self.rsi_entry_threshold}), "
            f"MACD={'bullish' if macd_bullish else 'bearish'}, "
            f"days_to_maturity={days_to_maturity}"
        )

    def _handle_accumulating(self, market: MarketSnapshot) -> Intent:
        """ACCUMULATING phase: DCA buy PT in tranches."""
        # Check if we've completed all tranches
        if self._tranches_completed >= self.dca_tranches:
            logger.info(
                f"DCA complete: {self._tranches_completed}/{self.dca_tranches} tranches, "
                f"total PT bought={self._total_pt_bought}"
            )
            self._phase = PHASE_HOLDING
            return Intent.hold(reason="DCA complete, entering hold phase")

        # Check allocation cap
        try:
            base_bal = market.balance(self.base_token)
            pt_bal = market.balance(self.pt_token)
            total_value = base_bal + pt_bal  # rough: both in same underlying terms
            if total_value > 0:
                pt_allocation = pt_bal / total_value
                if pt_allocation >= self.max_pt_allocation_pct:
                    logger.info(
                        f"Max PT allocation reached: {pt_allocation:.1%} >= {self.max_pt_allocation_pct:.0%}"
                    )
                    self._phase = PHASE_HOLDING
                    return Intent.hold(reason=f"Max PT allocation {pt_allocation:.1%} reached")
        except Exception as e:
            logger.warning(f"Balance check for allocation cap failed: {e}")

        return self._buy_pt_tranche(market)

    def _handle_holding(self, market: MarketSnapshot, days_to_maturity: int) -> Intent:
        """HOLDING phase: monitor position, check exit conditions."""
        # Check RSI for overbought exit signal
        try:
            rsi = market.rsi(self.base_token, period=self.rsi_period, timeframe=self.rsi_timeframe)
            rsi_value = float(rsi.value)
            self._last_rsi = rsi_value

            if rsi_value > self.rsi_exit_threshold:
                logger.info(
                    f"RSI exit signal: RSI={rsi_value:.1f} > {self.rsi_exit_threshold}. "
                    f"Underlying may be overvalued, taking profit on PT."
                )
                self._phase = PHASE_EXITING
                return self._sell_all_pt(market)
        except Exception as e:
            logger.debug(f"RSI unavailable during hold: {e}")

        # Report position status
        try:
            pt_amount = market.balance(self.pt_token)
        except Exception:
            pt_amount = self._total_pt_bought

        return Intent.hold(
            reason=f"Holding PT position: ~{pt_amount:.4f} {self.pt_token}, "
            f"cycle #{self._cycle_count}, "
            f"{days_to_maturity} days to maturity, "
            f"RSI={self._last_rsi or 'N/A'}"
        )

    def _handle_exiting(self, market: MarketSnapshot) -> Intent:
        """EXITING phase: sell all PT back to base token."""
        try:
            pt_bal = market.balance(self.pt_token)
            if pt_bal <= Decimal("0.0001"):
                logger.info("Exit complete: no significant PT balance remaining")
                self._reset_cycle()
                return Intent.hold(reason="Exit complete, returning to idle")
        except Exception as e:
            logger.debug(f"Can't check PT balance during exit: {e}")

        return self._sell_all_pt(market)

    # ─── Trade Helpers ────────────────────────────────────────────────

    def _buy_pt_tranche(self, market: MarketSnapshot) -> Intent:
        """Buy a single DCA tranche of PT."""
        # Calculate tranche size
        try:
            available = market.balance(self.base_token)
        except Exception:
            available = self.trade_size_token

        tranche_amount = min(self.trade_size_token * self.tranche_pct, available)

        if tranche_amount <= Decimal("0.0001"):
            logger.warning(f"Insufficient {self.base_token} for tranche ({available})")
            self._phase = PHASE_HOLDING
            return Intent.hold(reason=f"Insufficient {self.base_token} balance for DCA tranche")

        tranche_num = self._tranches_completed + 1
        logger.info(
            f"DCA tranche {tranche_num}/{self.dca_tranches}: "
            f"swapping {tranche_amount:.6f} {self.base_token} -> {self.pt_token}"
        )

        max_slippage = Decimal(str(self.max_slippage_bps)) / Decimal("10000")

        return Intent.swap(
            from_token=self.base_token,
            to_token=self.pt_token,
            amount=tranche_amount,
            max_slippage=max_slippage,
            protocol="pendle",
        )

    def _sell_all_pt(self, market: MarketSnapshot) -> Intent:
        """Sell all PT back to base token."""
        logger.info(f"Selling all {self.pt_token} -> {self.base_token}")
        max_slippage = Decimal(str(self.max_slippage_bps)) / Decimal("10000")

        return Intent.swap(
            from_token=self.pt_token,
            to_token=self.base_token,
            amount="all",
            max_slippage=max_slippage,
            protocol="pendle",
        )

    # ─── Lifecycle Callbacks ──────────────────────────────────────────

    def on_intent_executed(self, intent: Any, success: bool, result: Any) -> None:
        """Track execution results to update state machine."""
        if not success:
            logger.warning(f"Intent failed: {intent}. Staying in phase={self._phase}")
            return

        intent_type = getattr(intent, "intent_type", None)
        if intent_type is None:
            return

        intent_type_val = intent_type.value if hasattr(intent_type, "value") else str(intent_type)

        if intent_type_val == "SWAP":
            from_token = getattr(intent, "from_token", "")
            to_token = getattr(intent, "to_token", "")

            if to_token == self.pt_token:
                # Bought PT
                self._tranches_completed += 1
                amount = getattr(intent, "amount", Decimal("0"))
                is_all = isinstance(amount, str)
                if is_all:
                    amount = Decimal("0")
                self._total_base_spent += amount

                # Track received PT from result enrichment
                if result and hasattr(result, "swap_amounts") and result.swap_amounts:
                    received = Decimal(str(result.swap_amounts.amount_out))
                    self._total_pt_bought += received
                    self._entry_prices.append(str(result.swap_amounts.effective_price or "0"))
                    logger.info(
                        f"Tranche {self._tranches_completed}/{self.dca_tranches}: "
                        f"received {received:.6f} {self.pt_token}"
                    )
                else:
                    if is_all:
                        logger.warning("PT buy with amount='all' but no enrichment data -- PT tracking may drift")
                    self._total_pt_bought += amount
                    logger.info(f"Tranche {self._tranches_completed}/{self.dca_tranches}: estimated ~{amount} PT")

                # Check if DCA is complete
                if self._tranches_completed >= self.dca_tranches:
                    self._phase = PHASE_HOLDING
                    logger.info(f"DCA complete after {self._tranches_completed} tranches")

            elif from_token == self.pt_token:
                # Sold PT (exiting)
                logger.info(f"PT sold successfully. Resetting cycle.")
                self._reset_cycle()

    # ─── State Persistence ────────────────────────────────────────────

    def get_persistent_state(self) -> dict[str, Any]:
        """Snapshot state for crash recovery."""
        return {
            "phase": self._phase,
            "tranches_completed": self._tranches_completed,
            "total_pt_bought": str(self._total_pt_bought),
            "total_base_spent": str(self._total_base_spent),
            "entry_prices": self._entry_prices,
            "last_rsi": self._last_rsi,
            "cycle_count": self._cycle_count,
        }

    def load_persistent_state(self, state: dict[str, Any]) -> None:
        """Restore state from last snapshot."""
        self._phase = state.get("phase", PHASE_IDLE)
        self._tranches_completed = state.get("tranches_completed", 0)
        self._total_pt_bought = Decimal(str(state.get("total_pt_bought", "0")))
        self._total_base_spent = Decimal(str(state.get("total_base_spent", "0")))
        self._entry_prices = state.get("entry_prices", [])
        self._last_rsi = state.get("last_rsi")
        self._cycle_count = state.get("cycle_count", 0)
        logger.info(f"Restored state: phase={self._phase}, tranches={self._tranches_completed}, cycle={self._cycle_count}")

    # ─── Utilities ────────────────────────────────────────────────────

    def _days_to_maturity(self) -> int:
        """Calculate days until market maturity."""
        now = datetime.now(UTC)
        delta = self.market_expiry - now
        return max(0, delta.days)

    def _reset_cycle(self) -> None:
        """Reset state for next cycle."""
        self._phase = PHASE_IDLE
        self._tranches_completed = 0
        self._total_pt_bought = Decimal("0")
        self._total_base_spent = Decimal("0")
        self._entry_prices = []

    def _get_tracked_tokens(self) -> list[str]:
        """Tokens to track for wallet balance."""
        return [self.base_token_symbol, self.pt_token_symbol]

    def get_status(self) -> dict[str, Any]:
        """Get strategy status for monitoring."""
        return {
            "strategy": "pendle_pt_rotator",
            "chain": self.chain,
            "phase": self._phase,
            "cycle": self._cycle_count,
            "tranches": f"{self._tranches_completed}/{self.dca_tranches}",
            "total_pt_bought": str(self._total_pt_bought),
            "total_base_spent": str(self._total_base_spent),
            "last_rsi": self._last_rsi,
            "days_to_maturity": self._days_to_maturity(),
            "market_expiry": self.market_expiry.date().isoformat(),
        }

    # ─── Teardown Support ─────────────────────────────────────────────

    def supports_teardown(self) -> bool:
        return True

    def get_open_positions(self) -> "TeardownPositionSummary":
        from almanak.framework.teardown import (
            PositionInfo,
            PositionType,
            TeardownPositionSummary,
        )

        positions: list[PositionInfo] = []

        if self._phase in (PHASE_ACCUMULATING, PHASE_HOLDING, PHASE_EXITING):
            positions.append(
                PositionInfo(
                    position_type=PositionType.TOKEN,
                    position_id="pendle_pt_rotator_0",
                    chain=self.chain,
                    protocol="pendle",
                    value_usd=Decimal("0"),  # would need price to estimate
                    details={
                        "pt_token": self.pt_token,
                        "base_token": self.base_token,
                        "total_pt_bought": str(self._total_pt_bought),
                        "phase": self._phase,
                        "cycle": self._cycle_count,
                    },
                )
            )

        return TeardownPositionSummary(
            strategy_id=getattr(self, "strategy_id", "pendle_pt_rotator"),
            timestamp=datetime.now(UTC),
            positions=positions,
        )

    def generate_teardown_intents(self, mode: "TeardownMode", market=None) -> list[Intent]:
        from almanak.framework.teardown import TeardownMode

        if self._phase == PHASE_IDLE:
            return []

        if mode == TeardownMode.HARD:
            max_slippage = Decimal("0.05")
        else:
            max_slippage = Decimal(str(self.max_slippage_bps)) / Decimal("10000")

        logger.info(f"Teardown ({mode.value}): selling all {self.pt_token} -> {self.base_token}")

        return [
            Intent.swap(
                from_token=self.pt_token,
                to_token=self.base_token,
                amount="all",
                max_slippage=max_slippage,
                protocol="pendle",
            )
        ]
