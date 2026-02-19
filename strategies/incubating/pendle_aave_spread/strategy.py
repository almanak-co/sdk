"""
===============================================================================
Pendle-Aave Yield Spread Trader
===============================================================================

A cross-protocol strategy that arbitrages the yield spread between Pendle's
fixed yield (PT discount) and Aave V3 lending supply rates.

Capital flows between the two protocols based on which offers superior
risk-adjusted yield.

HOW IT WORKS:
-------------
1. Estimates Pendle implied yield from PT discount vs underlying price
2. Fetches Aave V3 supply rate for the same asset via RateMonitor
3. Computes the yield spread
4. When spread exceeds threshold, rotates capital to the higher-yielding protocol
5. Uses IntentSequence for atomic multi-step rotations

STATE MACHINE:
--------------
    MONITORING
        |
        v
    [compute spread]
        |
    +---+---+
    |       |
    v       v
  TO_PENDLE  TO_AAVE
    |           |
    v           v
  IN_PENDLE  IN_AAVE
    |           |
    v           v
    MONITORING (cooldown)

WHAT MAKES THIS PUSH THE SDK'S LIMITS:
---------------------------------------
- Cross-protocol orchestration (Pendle + Aave V3)
- IntentSequence for multi-step atomic operations
- Implied yield estimation without Pendle API
- Dual-protocol state tracking
- Lending rate monitor + price data in same decide()
- Teardown must unwind both protocols correctly

USAGE:
------
    almanak strat run -d strategies/demo/pendle_aave_spread --network anvil --once

===============================================================================
"""

import logging
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
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
PHASE_MONITORING = "monitoring"
PHASE_IN_AAVE = "in_aave"
PHASE_IN_PENDLE = "in_pendle"
PHASE_ROTATING_TO_PENDLE = "rotating_to_pendle"
PHASE_ROTATING_TO_AAVE = "rotating_to_aave"
PHASE_INITIAL_SUPPLY = "initial_supply"
PHASE_EXITING = "exiting"


@almanak_strategy(
    name="pendle_aave_spread",
    description="Cross-protocol yield spread trader: Pendle PT fixed yield vs Aave V3 supply rate",
    version="1.0.0",
    author="Almanak",
    tags=["pendle", "aave", "yield", "spread", "cross-protocol", "arbitrage"],
    supported_chains=["arbitrum"],
    supported_protocols=["pendle", "aave_v3"],
    intent_types=["SWAP", "SUPPLY", "WITHDRAW", "HOLD"],
)
class PendleAaveSpreadStrategy(IntentStrategy):
    """Cross-protocol yield spread trader between Pendle PT and Aave V3.

    Monitors the spread between Pendle's implied fixed yield and Aave's
    supply rate, rotating capital to capture the best risk-adjusted yield.
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)

        def cfg(key: str, default: Any) -> Any:
            if isinstance(self.config, dict):
                return self.config.get(key, default)
            return getattr(self.config, key, default)

        # Token config
        self.base_token = cfg("base_token", "WSTETH")
        self.base_token_symbol = cfg("base_token_symbol", self.base_token)
        self.pt_token = cfg("pt_token", "PT-wstETH")
        self.pt_token_symbol = cfg("pt_token_symbol", self.pt_token)
        self.market_address = cfg("market_address", "0xf78452e0f5c0b95fc5dc8353b8cd1e06e53fa25b")
        self.market_expiry = datetime.strptime(cfg("market_expiry", "2026-06-25"), "%Y-%m-%d").replace(tzinfo=UTC)

        # Spread parameters
        self.spread_entry_threshold = Decimal(str(cfg("spread_entry_threshold_pct", "2.0")))
        self.spread_exit_threshold = Decimal(str(cfg("spread_exit_threshold_pct", "0.5")))

        # Rotation parameters
        self.max_rotation_pct = Decimal(str(cfg("max_rotation_pct", "0.50")))
        self.min_rotation_amount = Decimal(str(cfg("min_rotation_amount", "0.1")))
        self.rebalance_cooldown_seconds = int(cfg("rebalance_cooldown_seconds", 300))
        self.min_days_to_maturity = int(cfg("min_days_to_maturity", 30))
        self.exit_days_before_maturity = int(cfg("exit_days_before_maturity", 14))
        self.max_slippage_bps = int(cfg("max_slippage_bps", 100))
        self.initial_allocation = cfg("initial_allocation", "aave")

        # State
        self._phase = PHASE_MONITORING
        self._aave_supplied = Decimal("0")
        self._pendle_pt_held = Decimal("0")
        self._last_spread: Decimal | None = None
        self._last_aave_rate: Decimal | None = None
        self._last_pendle_yield: Decimal | None = None
        self._last_rotation_time: datetime | None = None
        self._rotation_count = 0
        self._total_rotations_to_pendle = 0
        self._total_rotations_to_aave = 0

        logger.info(
            f"PendleAaveSpread initialized: "
            f"spread_entry={self.spread_entry_threshold}%, "
            f"spread_exit={self.spread_exit_threshold}%, "
            f"max_rotation={self.max_rotation_pct:.0%}, "
            f"maturity={self.market_expiry.date()}"
        )

    # ─── Core Decision Logic ─────────────────────────────────────────

    def decide(self, market: MarketSnapshot) -> Any:
        """Main decision: compare yields and rotate if spread is attractive."""
        try:
            days_to_maturity = self._days_to_maturity()

            # Global safety: maturity approaching
            if days_to_maturity <= self.exit_days_before_maturity and self._phase not in (
                PHASE_MONITORING,
                PHASE_EXITING,
            ):
                logger.info(f"Maturity in {days_to_maturity} days. Exiting all positions.")
                self._phase = PHASE_EXITING

            # Route to phase handler
            if self._phase == PHASE_MONITORING:
                return self._handle_monitoring(market, days_to_maturity)
            elif self._phase == PHASE_INITIAL_SUPPLY:
                return self._handle_initial_supply(market)
            elif self._phase == PHASE_IN_AAVE:
                return self._handle_in_aave(market, days_to_maturity)
            elif self._phase == PHASE_IN_PENDLE:
                return self._handle_in_pendle(market, days_to_maturity)
            elif self._phase == PHASE_ROTATING_TO_PENDLE:
                return self._handle_rotating_to_pendle(market)
            elif self._phase == PHASE_ROTATING_TO_AAVE:
                return self._handle_rotating_to_aave(market)
            elif self._phase == PHASE_EXITING:
                return self._handle_exiting(market)
            else:
                logger.error(f"Unknown phase: {self._phase}")
                self._phase = PHASE_MONITORING
                return Intent.hold(reason="Phase reset")

        except Exception as e:
            logger.exception(f"Error in decide(): {e}")
            return Intent.hold(reason=f"Error: {e}")

    def _handle_monitoring(self, market: MarketSnapshot, days_to_maturity: int) -> Any:
        """MONITORING: initial state, determine where to allocate capital."""
        # Check if we have uninvested capital
        try:
            base_balance = market.balance(self.base_token)
            if base_balance > self.min_rotation_amount:
                # Block PT allocation near maturity
                if self.initial_allocation != "aave" and days_to_maturity < self.min_days_to_maturity:
                    logger.info(
                        f"Near maturity ({days_to_maturity}d), defaulting to Aave instead of Pendle"
                    )
                    self._phase = PHASE_INITIAL_SUPPLY
                    return self._supply_to_aave(base_balance)

                if self.initial_allocation == "aave":
                    logger.info(f"Initial allocation to Aave: {base_balance:.4f} {self.base_token}")
                    self._phase = PHASE_INITIAL_SUPPLY
                    return self._supply_to_aave(base_balance)
                else:
                    logger.info(f"Initial allocation to Pendle: {base_balance:.4f} {self.base_token}")
                    self._phase = PHASE_ROTATING_TO_PENDLE
                    return self._swap_to_pt(base_balance)
        except Exception as e:
            logger.warning(f"Balance check failed: {e}")

        # Already allocated, compute spread
        return self._compute_and_report_spread(market, days_to_maturity)

    def _handle_initial_supply(self, market: MarketSnapshot) -> Any:
        """INITIAL_SUPPLY: waiting for initial Aave supply to complete."""
        return Intent.hold(reason="Waiting for initial supply to complete")

    def _handle_in_aave(self, market: MarketSnapshot, days_to_maturity: int) -> Any:
        """IN_AAVE: capital is in Aave, check if Pendle offers better yield."""
        if days_to_maturity < self.min_days_to_maturity:
            return Intent.hold(reason=f"Near maturity ({days_to_maturity}d), staying in Aave")

        if not self._cooldown_elapsed():
            return Intent.hold(reason=f"Cooldown active, staying in Aave")

        spread = self._compute_spread(market)
        if spread is None:
            return Intent.hold(reason="Cannot compute spread, staying in Aave")

        self._last_spread = spread

        # Pendle yield significantly higher than Aave
        if spread > self.spread_entry_threshold:
            rotation_amount = self._aave_supplied * self.max_rotation_pct
            if rotation_amount < self.min_rotation_amount:
                return Intent.hold(
                    reason=f"Spread={spread:.2f}% favors Pendle but rotation too small ({rotation_amount:.4f})"
                )

            logger.info(
                f"Rotating to Pendle: spread={spread:.2f}% > {self.spread_entry_threshold}%, "
                f"amount={rotation_amount:.4f} {self.base_token}"
            )
            self._phase = PHASE_ROTATING_TO_PENDLE
            # IntentSequence: withdraw from Aave -> swap to PT
            return Intent.sequence(
                [
                    Intent.withdraw(
                        protocol="aave_v3",
                        token=self.base_token,
                        amount=rotation_amount,
                        withdraw_all=(rotation_amount >= self._aave_supplied * Decimal("0.99")),
                        chain=self.chain,
                    ),
                    Intent.swap(
                        from_token=self.base_token,
                        to_token=self.pt_token,
                        amount="all",
                        max_slippage=Decimal(str(self.max_slippage_bps)) / Decimal("10000"),
                        protocol="pendle",
                    ),
                ],
                description=f"Rotate {rotation_amount:.4f} {self.base_token} from Aave to Pendle PT",
            )

        return Intent.hold(
            reason=f"In Aave: spread={spread:.2f}% (threshold={self.spread_entry_threshold}%), "
            f"Aave={self._last_aave_rate or '?'}%, Pendle={self._last_pendle_yield or '?'}%"
        )

    def _handle_in_pendle(self, market: MarketSnapshot, days_to_maturity: int) -> Any:
        """IN_PENDLE: capital is in Pendle PT, check if Aave is better."""
        if not self._cooldown_elapsed():
            return Intent.hold(reason=f"Cooldown active, staying in Pendle")

        spread = self._compute_spread(market)
        if spread is None:
            return Intent.hold(reason="Cannot compute spread, staying in Pendle")

        self._last_spread = spread

        # Spread narrowed: Aave is now comparable or better
        if spread < self.spread_exit_threshold:
            logger.info(
                f"Rotating to Aave: spread={spread:.2f}% < {self.spread_exit_threshold}%, "
                f"selling PT -> supply to Aave"
            )
            self._phase = PHASE_ROTATING_TO_AAVE
            max_slippage = Decimal(str(self.max_slippage_bps)) / Decimal("10000")
            # IntentSequence: sell PT -> supply to Aave
            return Intent.sequence(
                [
                    Intent.swap(
                        from_token=self.pt_token,
                        to_token=self.base_token,
                        amount="all",
                        max_slippage=max_slippage,
                        protocol="pendle",
                    ),
                    Intent.supply(
                        protocol="aave_v3",
                        token=self.base_token,
                        amount="all",
                        use_as_collateral=False,
                        chain=self.chain,
                    ),
                ],
                description=f"Rotate PT -> {self.base_token} -> Aave supply",
            )

        return Intent.hold(
            reason=f"In Pendle: spread={spread:.2f}% (exit<{self.spread_exit_threshold}%), "
            f"days_to_maturity={days_to_maturity}"
        )

    def _handle_rotating_to_pendle(self, market: MarketSnapshot) -> Any:
        """Waiting for rotation to complete."""
        return Intent.hold(reason="Rotation to Pendle in progress")

    def _handle_rotating_to_aave(self, market: MarketSnapshot) -> Any:
        """Waiting for rotation to complete."""
        return Intent.hold(reason="Rotation to Aave in progress")

    def _handle_exiting(self, market: MarketSnapshot) -> Any:
        """Exit all positions back to base token."""
        # Check if we have PT to sell
        try:
            pt_balance = market.balance(self.pt_token)
            if pt_balance > Decimal("0.0001"):
                logger.info(f"Exit: selling {pt_balance:.4f} PT -> {self.base_token}")
                return Intent.swap(
                    from_token=self.pt_token,
                    to_token=self.base_token,
                    amount="all",
                    max_slippage=Decimal("0.02"),
                    protocol="pendle",
                )
        except Exception as e:
            logger.warning(f"PT balance check failed during exit: {e}")

        # Check if we have Aave position
        if self._aave_supplied > Decimal("0"):
            logger.info(f"Exit: withdrawing {self._aave_supplied:.4f} from Aave")
            return Intent.withdraw(
                protocol="aave_v3",
                token=self.base_token,
                amount=self._aave_supplied,
                withdraw_all=True,
                chain=self.chain,
            )

        logger.info("Exit complete")
        self._phase = PHASE_MONITORING
        return Intent.hold(reason="All positions closed")

    # ─── Yield Computation ────────────────────────────────────────────

    def _compute_spread(self, market: MarketSnapshot) -> Decimal | None:
        """Compute yield spread: Pendle implied yield - Aave supply rate.

        Returns spread in percentage points, or None if data unavailable.
        """
        # Get Aave supply rate
        aave_rate = self._get_aave_rate(market)
        if aave_rate is None:
            return None
        self._last_aave_rate = aave_rate

        # Estimate Pendle implied yield from PT price
        pendle_yield = self._estimate_pendle_yield(market)
        if pendle_yield is None:
            return None
        self._last_pendle_yield = pendle_yield

        spread = pendle_yield - aave_rate
        logger.debug(
            f"Yield spread: Pendle={pendle_yield:.2f}% - Aave={aave_rate:.2f}% = {spread:.2f}%"
        )
        return spread

    def _get_aave_rate(self, market: MarketSnapshot) -> Decimal | None:
        """Get Aave V3 supply rate for the base token."""
        try:
            rate = market.lending_rate("aave_v3", self.base_token, side="supply")
            return Decimal(str(rate.apy_percent))
        except Exception as e:
            logger.warning(f"Aave rate unavailable: {e}")
            return None

    def _estimate_pendle_yield(self, market: MarketSnapshot) -> Decimal | None:
        """Estimate Pendle implied yield from PT discount.

        Since we don't have Pendle API, we approximate:
        - PT trades at a discount to the underlying
        - implied_apy = (underlying/pt_price)^(365/days_to_maturity) - 1

        Without PT price feed, we use a conservative estimate based on
        typical Pendle market yields for wstETH.
        """
        days_to_maturity = self._days_to_maturity()
        if days_to_maturity <= 0:
            return Decimal("0")

        # Try to get PT price from market data
        try:
            underlying_price = Decimal(str(market.price(self.base_token)))
            pt_price = Decimal(str(market.price(self.pt_token)))

            if pt_price <= 0 or underlying_price <= 0:
                raise ValueError("Invalid prices")

            # implied_apy = (underlying/pt)^(365/days) - 1
            price_ratio = float(underlying_price / pt_price)
            annualization = 365.0 / days_to_maturity
            implied_apy = (price_ratio ** annualization - 1) * 100
            return Decimal(str(round(implied_apy, 4)))

        except (ValueError, TypeError, InvalidOperation, ZeroDivisionError) as e:
            logger.warning(f"Pendle yield estimation failed: {e}")
            return None

    # ─── Trade Helpers ────────────────────────────────────────────────

    def _supply_to_aave(self, amount: Decimal) -> Any:
        """Supply base token to Aave V3."""
        return Intent.supply(
            protocol="aave_v3",
            token=self.base_token,
            amount=amount,
            use_as_collateral=False,  # Pure yield, no borrowing
            chain=self.chain,
        )

    def _swap_to_pt(self, amount: Decimal) -> Any:
        """Swap base token to PT on Pendle."""
        max_slippage = Decimal(str(self.max_slippage_bps)) / Decimal("10000")
        return Intent.swap(
            from_token=self.base_token,
            to_token=self.pt_token,
            amount=amount,
            max_slippage=max_slippage,
            protocol="pendle",
        )

    # ─── Lifecycle Callbacks ──────────────────────────────────────────

    def on_intent_executed(self, intent: Any, success: bool, result: Any) -> None:
        """Track execution results to update state machine."""
        if not success:
            logger.warning(f"Intent failed in phase={self._phase}")
            # Revert to safe state on failure
            if self._phase in (PHASE_ROTATING_TO_PENDLE, PHASE_ROTATING_TO_AAVE):
                self._phase = PHASE_MONITORING
            return

        intent_type = getattr(intent, "intent_type", None)
        if intent_type is None:
            return
        intent_type_val = intent_type.value if hasattr(intent_type, "value") else str(intent_type)

        if intent_type_val == "SUPPLY_COLLATERAL" or intent_type_val == "SUPPLY":
            amount = getattr(intent, "amount", Decimal("0"))
            if isinstance(amount, str):
                # "all" means we supplied everything we had -- try result enrichment
                supply_amount = result.get_extracted("supply_amount") if result and hasattr(result, "get_extracted") else None
                if supply_amount is not None:
                    amount = Decimal(str(supply_amount))
                else:
                    logger.warning("Supply with amount='all' but no enrichment data -- tracking may drift")
                    amount = Decimal("0")
            self._aave_supplied += amount
            if self._phase == PHASE_INITIAL_SUPPLY:
                self._phase = PHASE_IN_AAVE
                logger.info(f"Initial supply to Aave complete: {self._aave_supplied:.4f}")
            elif self._phase == PHASE_ROTATING_TO_AAVE:
                self._phase = PHASE_IN_AAVE
                self._pendle_pt_held = Decimal("0")
                self._rotation_count += 1
                self._total_rotations_to_aave += 1
                self._last_rotation_time = datetime.now(UTC)
                logger.info(f"Rotation to Aave complete (rotation #{self._rotation_count})")

        elif intent_type_val == "WITHDRAW":
            amount = getattr(intent, "amount", Decimal("0"))
            if isinstance(amount, str):
                amount = self._aave_supplied
            self._aave_supplied = max(Decimal("0"), self._aave_supplied - amount)
            logger.info(f"Withdrew from Aave: remaining={self._aave_supplied:.4f}")

        elif intent_type_val == "SWAP":
            from_token = getattr(intent, "from_token", "")
            to_token = getattr(intent, "to_token", "")

            if to_token == self.pt_token:
                # Bought PT
                amount = getattr(intent, "amount", Decimal("0"))
                is_all = isinstance(amount, str)
                if is_all:
                    amount = Decimal("0")
                if result and hasattr(result, "swap_amounts") and result.swap_amounts:
                    self._pendle_pt_held += Decimal(str(result.swap_amounts.amount_out))
                else:
                    if is_all:
                        logger.warning("PT buy with amount='all' but no enrichment data -- PT tracking may drift")
                    self._pendle_pt_held += amount

                if self._phase == PHASE_ROTATING_TO_PENDLE:
                    self._phase = PHASE_IN_PENDLE
                    self._rotation_count += 1
                    self._total_rotations_to_pendle += 1
                    self._last_rotation_time = datetime.now(UTC)
                    logger.info(
                        f"Rotation to Pendle complete: PT held={self._pendle_pt_held:.4f} "
                        f"(rotation #{self._rotation_count})"
                    )

            elif from_token == self.pt_token:
                # Sold PT
                self._pendle_pt_held = Decimal("0")
                if self._phase == PHASE_EXITING:
                    logger.info("PT sold during exit")

    # ─── State Persistence ────────────────────────────────────────────

    def get_persistent_state(self) -> dict[str, Any]:
        return {
            "phase": self._phase,
            "aave_supplied": str(self._aave_supplied),
            "pendle_pt_held": str(self._pendle_pt_held),
            "last_spread": str(self._last_spread) if self._last_spread is not None else None,
            "last_aave_rate": str(self._last_aave_rate) if self._last_aave_rate is not None else None,
            "last_pendle_yield": str(self._last_pendle_yield) if self._last_pendle_yield is not None else None,
            "last_rotation_time": self._last_rotation_time.isoformat() if self._last_rotation_time else None,
            "rotation_count": self._rotation_count,
            "total_rotations_to_pendle": self._total_rotations_to_pendle,
            "total_rotations_to_aave": self._total_rotations_to_aave,
        }

    def load_persistent_state(self, state: dict[str, Any]) -> None:
        self._phase = state.get("phase", PHASE_MONITORING)
        self._aave_supplied = Decimal(str(state.get("aave_supplied", "0")))
        self._pendle_pt_held = Decimal(str(state.get("pendle_pt_held", "0")))
        ls = state.get("last_spread")
        self._last_spread = Decimal(ls) if ls is not None else None
        lar = state.get("last_aave_rate")
        self._last_aave_rate = Decimal(lar) if lar is not None else None
        lpy = state.get("last_pendle_yield")
        self._last_pendle_yield = Decimal(lpy) if lpy is not None else None
        lrt = state.get("last_rotation_time")
        self._last_rotation_time = datetime.fromisoformat(lrt) if lrt else None
        self._rotation_count = state.get("rotation_count", 0)
        self._total_rotations_to_pendle = state.get("total_rotations_to_pendle", 0)
        self._total_rotations_to_aave = state.get("total_rotations_to_aave", 0)
        logger.info(
            f"Restored state: phase={self._phase}, aave={self._aave_supplied}, "
            f"pendle_pt={self._pendle_pt_held}, rotations={self._rotation_count}"
        )

    # ─── Utilities ────────────────────────────────────────────────────

    def _days_to_maturity(self) -> int:
        now = datetime.now(UTC)
        delta = self.market_expiry - now
        return max(0, delta.days)

    def _cooldown_elapsed(self) -> bool:
        """Check if enough time has passed since last rotation."""
        if self._last_rotation_time is None:
            return True
        elapsed = (datetime.now(UTC) - self._last_rotation_time).total_seconds()
        return elapsed >= self.rebalance_cooldown_seconds

    def _compute_and_report_spread(self, market: MarketSnapshot, days_to_maturity: int) -> Any:
        """Compute spread and report status."""
        spread = self._compute_spread(market)
        if spread is not None:
            self._last_spread = spread
        return Intent.hold(
            reason=f"Monitoring: spread={spread or '?'}%, "
            f"Aave={self._last_aave_rate or '?'}%, "
            f"Pendle={self._last_pendle_yield or '?'}%, "
            f"maturity={days_to_maturity}d"
        )

    def _get_tracked_tokens(self) -> list[str]:
        return [self.base_token_symbol, self.pt_token_symbol]

    def get_status(self) -> dict[str, Any]:
        return {
            "strategy": "pendle_aave_spread",
            "chain": self.chain,
            "phase": self._phase,
            "aave_supplied": str(self._aave_supplied),
            "pendle_pt_held": str(self._pendle_pt_held),
            "last_spread": str(self._last_spread) if self._last_spread else "N/A",
            "last_aave_rate": str(self._last_aave_rate) if self._last_aave_rate else "N/A",
            "last_pendle_yield": str(self._last_pendle_yield) if self._last_pendle_yield else "N/A",
            "rotation_count": self._rotation_count,
            "days_to_maturity": self._days_to_maturity(),
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

        if self._aave_supplied > 0:
            positions.append(
                PositionInfo(
                    position_type=PositionType.LENDING,
                    position_id="aave_supply_0",
                    chain=self.chain,
                    protocol="aave_v3",
                    value_usd=Decimal("0"),
                    details={"token": self.base_token, "supplied": str(self._aave_supplied)},
                )
            )

        if self._pendle_pt_held > 0:
            positions.append(
                PositionInfo(
                    position_type=PositionType.TOKEN,
                    position_id="pendle_pt_0",
                    chain=self.chain,
                    protocol="pendle",
                    value_usd=Decimal("0"),
                    details={"pt_token": self.pt_token, "held": str(self._pendle_pt_held)},
                )
            )

        return TeardownPositionSummary(
            strategy_id=getattr(self, "strategy_id", "pendle_aave_spread"),
            timestamp=datetime.now(UTC),
            positions=positions,
        )

    def generate_teardown_intents(self, mode: "TeardownMode", market=None) -> list[Any]:
        from almanak.framework.teardown import TeardownMode

        intents: list[Any] = []
        max_slippage = Decimal("0.05") if mode == TeardownMode.HARD else Decimal(str(self.max_slippage_bps)) / Decimal("10000")

        # Sell PT first
        if self._pendle_pt_held > 0:
            intents.append(
                Intent.swap(
                    from_token=self.pt_token,
                    to_token=self.base_token,
                    amount="all",
                    max_slippage=max_slippage,
                    protocol="pendle",
                )
            )

        # Withdraw from Aave
        if self._aave_supplied > 0:
            intents.append(
                Intent.withdraw(
                    protocol="aave_v3",
                    token=self.base_token,
                    amount=self._aave_supplied,
                    withdraw_all=True,
                    chain=self.chain,
                )
            )

        return intents
