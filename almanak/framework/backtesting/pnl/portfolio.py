"""Simulated position and portfolio models for PnL backtesting.

This module provides models for tracking simulated positions and portfolio state
during backtests, including spot, LP, perpetual, and lending positions.

Models:
    - PositionType: Types of positions that can be held
    - SimulatedPosition: A simulated position with protocol-specific fields
    - SimulatedFill: Details of a simulated trade execution
    - SimulatedPortfolio: Portfolio tracker with positions, cash, and metrics
"""

import logging
from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Any

from almanak.core.constants import STABLECOINS  # noqa: F401 - used by SimulatedPortfolio._STABLECOIN_SYMBOLS
from almanak.framework.backtesting.models import (
    BacktestMetrics,
    DataCoverageMetrics,
    EquityPoint,
    IntentType,
    LendingLiquidationEvent,
    LendingMetrics,
    LiquidationEvent,
    LPMetrics,
    PerpMetrics,
    SlippageMetrics,
    TradeRecord,
)
from almanak.framework.backtesting.paper.token_registry import (
    is_token_known,
    resolve_to_canonical_symbol,
)
from almanak.framework.backtesting.pnl.data_provider import MarketState

# Position models extracted to position_models.py for module size management
from almanak.framework.backtesting.pnl.position_models import (  # noqa: F401
    PositionType,
    SimulatedFill,
    SimulatedPosition,
)

if TYPE_CHECKING:
    from almanak.framework.backtesting.adapters.base import StrategyBacktestAdapter
    from almanak.framework.backtesting.pnl.engine import DataQualityTracker

logger = logging.getLogger(__name__)


#: Stablecoins the portfolio holds as ``cash_usd`` at $1. Inflows of these
#: are swept into ``cash_usd`` and outflows debit the token balance first,
#: then ``cash_usd``. The engine exposes ``cash_usd`` to strategies under
#: these same symbols (see ``engine.market_snapshot_from_state``).
#: Deliberately narrower than :data:`almanak.core.constants.STABLECOINS`,
#: which also lists yield-bearing tokens (sDAI, sUSDe, ...) whose price is
#: not $1 -- treating those as cash would itself violate conservation.
CASH_EQUIVALENT_STABLECOINS: frozenset[str] = frozenset({"USDC", "USDT", "DAI"})

#: Relative shortfall below which a debit is treated as spend-all instead of
#: failing the fill. Absorbs Decimal round-trip error from flow computations
#: (``amount_usd / price`` on both legs) without permitting economically
#: meaningful overdrafts.
_DEBIT_DUST_RELATIVE_TOLERANCE = Decimal("1e-9")

#: Intent types whose venue fee is already netted into the fill's token
#: flows (``_calculate_swap_flows`` haircuts ``tokens_in`` by fee and
#: slippage), so charging ``fee_usd`` against cash would double-count.
_FEE_EMBEDDED_IN_FLOWS: frozenset[IntentType] = frozenset({IntentType.SWAP})

#: Intent types whose slippage is already embodied in portfolio value:
#: SWAP nets it out of ``tokens_in``; perp intents carry it in
#: ``executed_price`` (adverse per side, see ``get_executed_price``), which
#: flows into the position's entry price and realized PnL. Debiting
#: ``slippage_usd`` from cash for these would double-count.
_SLIPPAGE_EMBEDDED_IN_PRICE: frozenset[IntentType] = frozenset(
    {IntentType.SWAP, IntentType.PERP_OPEN, IntentType.PERP_CLOSE}
)


# PositionType, SimulatedPosition, and SimulatedFill are now imported from position_models.py above.


@dataclass
class SimulatedPortfolio:
    """Portfolio tracker for PnL backtesting.

    Manages simulated positions, cash balances, and tracks portfolio value
    over time for backtest analysis.

    Attributes:
        cash_usd: Available cash in USD
        tokens: Dict of token symbol -> amount held (spot holdings)
        positions: List of open positions (LP, perp, lending)
        equity_curve: List of (timestamp, value) equity points
        trades: List of trade records for this portfolio
        initial_capital_usd: Starting capital in USD
        initial_margin_ratio: Initial margin ratio for perp positions (default 0.1 = 10%)
        maintenance_margin_ratio: Maintenance margin ratio (default 0.05 = 5%)
        max_margin_utilization: Maximum margin utilization ratio observed during backtest
        health_factor_warning_threshold: Threshold for emitting health factor warnings (default 1.2)
        liquidation_penalty: Penalty applied during lending liquidation (default 0.05 = 5%)

    Example:
        portfolio = SimulatedPortfolio(initial_capital_usd=Decimal("10000"))
        portfolio.apply_fill(fill)
        value = portfolio.get_total_value_usd(market_state)
        metrics = portfolio.get_metrics()
    """

    initial_capital_usd: Decimal = Decimal("10000")
    cash_usd: Decimal = field(default=Decimal("0"))
    tokens: dict[str, Decimal] = field(default_factory=dict)
    positions: list[SimulatedPosition] = field(default_factory=list)
    equity_curve: list[EquityPoint] = field(default_factory=list)
    trades: list[TradeRecord] = field(default_factory=list)
    # Margin configuration
    initial_margin_ratio: Decimal = Decimal("0.1")  # 10% default
    maintenance_margin_ratio: Decimal = Decimal("0.05")  # 5% default
    # Health factor configuration
    health_factor_warning_threshold: Decimal = Decimal("1.2")
    # Lending liquidation configuration
    liquidation_penalty: Decimal = Decimal("0.05")  # 5% default
    # When True, raise instead of falling back to $1 stablecoin assumptions
    strict_reproducibility: bool = False
    # Internal tracking
    _closed_positions: list[SimulatedPosition] = field(default_factory=list)
    _max_margin_utilization: Decimal = field(default=Decimal("0"))
    _min_health_factor: Decimal = field(default=Decimal("999"))
    _health_factor_warnings: int = field(default=0)
    _lending_liquidations: list[LendingLiquidationEvent] = field(default_factory=list)
    _perp_liquidations: list[LiquidationEvent] = field(default_factory=list)
    # Realized/unrealized PnL tracking
    _realized_pnl: Decimal = field(default=Decimal("0"))
    _unrealized_pnl: Decimal = field(default=Decimal("0"))

    _STABLECOIN_SYMBOLS: frozenset[str] = STABLECOINS

    def __post_init__(self) -> None:
        """Initialize cash from initial capital if not set."""
        if self.cash_usd == Decimal("0") and self.initial_capital_usd > 0:
            self.cash_usd = self.initial_capital_usd

    def _stablecoin_fallback(self, token: str, context: str) -> Decimal:
        """Return $1 fallback for token, raising in strict mode for non-stablecoins."""
        if self.strict_reproducibility and token.upper() not in self._STABLECOIN_SYMBOLS:
            raise ValueError(
                f"Price unavailable for non-stablecoin {token} in {context} and strict_reproducibility=True. "
                "Cannot assume $1 price."
            )
        logger.warning("Price unavailable for %s, falling back to $1 stablecoin assumption in %s", token, context)
        return Decimal("1")

    def apply_fill(self, fill: SimulatedFill, market_state: MarketState | None = None) -> bool:
        """Apply a simulated fill to update portfolio state.

        Conservation contract: a fill may only spend value the portfolio
        actually holds. ``tokens_out`` entries for cash-equivalent
        stablecoins (:data:`CASH_EQUIVALENT_STABLECOINS`) debit the token
        balance first and ``cash_usd`` for the remainder at $1, mirroring
        the inflow sweep that moves those stables into ``cash_usd``.

        Other tokens debit the ``tokens`` balance. For USD-notional
        position-funding intents (everything except SWAP) a shortfall is
        implicitly converted from ``cash_usd`` at the ``market_state``
        price -- a value-conserving, zero-fee zap recorded under
        ``metadata["implicit_conversions"]`` -- because the engine's flow
        producers size those legs from USD notional, not held balances
        (e.g. LP_OPEN splits ``amount_usd`` 50/50). SWAP outflows are
        strict: selling a token the portfolio does not hold is
        short-from-nothing and fails.

        A fill that cannot be covered -- or that arrives with
        ``success=False`` from an adapter validation failure -- is recorded
        as a failed trade and mutates no balance, cash, or position state.

        On success this method updates:
        - Token balances (tokens_in adds, tokens_out subtracts)
        - Positions (opens new or closes existing)
        - Cash balance (deducts gas and non-embedded venue costs, adjusts
          for stablecoin trades)
        - Trades list (records the trade)

        Venue costs (``fee_usd`` / ``slippage_usd``) are debited from cash
        exactly when nothing else in the fill embodies them (see
        :meth:`_venue_cash_costs`): SWAP nets both into ``tokens_in``,
        perps embody slippage in ``executed_price`` but pay fees in cash,
        and every other intent type pays both from cash.

        Perp positions have no token flows; their cash movement follows the
        position lifecycle instead: opening debits the collateral from
        ``cash_usd`` (an open the portfolio cannot fund is recorded as a
        failed trade with no state mutation) and closing credits collateral
        plus realized PnL (price PnL + accumulated funding) back to
        ``cash_usd``, keeping total portfolio value conserved across both
        transitions.

        For LP_CLOSE intents, also calculates and records:
        - il_loss_usd: Impermanent loss in USD
        - fees_earned_usd: Accumulated trading fees in USD
        - net_lp_pnl_usd: (Current Value + Fees) - Initial Value

        Args:
            fill: The simulated fill to apply
            market_state: Current market prices, used to price implicit
                cash conversions for non-SWAP token outflows. Without it,
                only held balances (and cash for stablecoins) can be spent.

        Returns:
            True if the fill was applied. False if it was rejected: the
            trade is recorded with ``success=False`` and a
            ``failure_reason`` in ``metadata``, and portfolio state is
            unchanged.

            Contract: a False return always implies ``fill.success`` has
            been set to False (every rejection path goes through
            ``_record_failed_fill``), so ``fill.to_trade_record()`` carries
            the rejection and callers may safely ignore the return value
            when they consume ``trade_record.success`` downstream instead.
        """
        if not fill.success:
            self._record_failed_fill(
                fill,
                fill.metadata.get("failure_reason", "fill marked failed by producer"),
            )
            return False

        token_debits, cash_debit, conversions, failure_reason = self._plan_token_debits(
            fill.tokens_out, fill.intent_type, market_state
        )
        if failure_reason is not None:
            self._record_failed_fill(fill, failure_reason)
            return False

        # Aggregate cash check: planned stablecoin/conversion debits and
        # perp-open collateral draw from the same cash_usd, so they must be
        # validated as one sum (each passing individually could still
        # overdraw) before any state mutation.
        funding_failure = self._cash_funding_failure(fill, cash_debit)
        if funding_failure is not None:
            self._record_failed_fill(fill, funding_failure)
            return False

        if conversions:
            fill.metadata["implicit_conversions"] = {token: str(amount) for token, amount in conversions.items()}

        self._apply_token_flows(fill, token_debits, cash_debit)

        # Initialize LP PnL breakdown fields
        il_loss_usd: Decimal | None = None
        fees_earned_usd: Decimal | None = None
        net_lp_pnl_usd: Decimal | None = None
        closed_position: SimulatedPosition | None = None

        # Handle position changes
        if fill.position_close_id:
            # Close an existing position and capture it for LP PnL calculation
            closed_position = self._close_position(fill.position_close_id, fill.timestamp)

        if fill.position_delta:
            # Open a new position. Perp collateral moves from cash into the
            # position (which every valuation path prices as collateral +
            # unrealized PnL + funding); without this debit the open would
            # mint the collateral amount.
            if fill.position_delta.is_perp and fill.success:
                self.cash_usd -= fill.position_delta.collateral_usd
            self.positions.append(fill.position_delta)

        # Calculate PnL for this trade
        pnl_usd = self._calculate_trade_pnl(fill)

        # Calculate LP PnL breakdown if this is an LP_CLOSE with a valid closed position
        if fill.intent_type == IntentType.LP_CLOSE and closed_position and closed_position.is_lp:
            il_loss_usd, fees_earned_usd, net_lp_pnl_usd = self._calculate_lp_pnl_breakdown(closed_position, fill)
            # Update pnl_usd with the net LP PnL if not already set from metadata
            if pnl_usd == Decimal("0") and net_lp_pnl_usd is not None:
                pnl_usd = net_lp_pnl_usd

        # Perp close: return collateral + realized PnL to cash, so the
        # close is value-neutral at the close instant.
        if closed_position is not None and closed_position.is_perp and fill.success:
            pnl_usd = self._apply_perp_close_credit(closed_position, fill, pnl_usd)

        # Record the trade with LP PnL breakdown if applicable
        trade = fill.to_trade_record(
            pnl_usd=pnl_usd,
            il_loss_usd=il_loss_usd,
            fees_earned_usd=fees_earned_usd,
            net_lp_pnl_usd=net_lp_pnl_usd,
        )
        self.trades.append(trade)

        # Accumulate realized PnL from position close operations
        # Realized PnL is the PnL locked in when a position is closed
        if fill.position_close_id and pnl_usd != Decimal("0"):
            self._realized_pnl += pnl_usd

        return True

    def _apply_token_flows(
        self,
        fill: SimulatedFill,
        token_debits: dict[str, Decimal],
        cash_debit: Decimal,
    ) -> None:
        """Commit the planned token debits, credits, stablecoin sweep, gas,
        and non-embedded venue costs.

        Runs only after ``_plan_token_debits`` validated affordability, so
        every debit is covered by the held balance.
        """
        # Update token balances - subtract tokens_out
        for token, debit in token_debits.items():
            new_amount = self.tokens.get(token, Decimal("0")) - debit
            if new_amount <= Decimal("0"):
                self.tokens.pop(token, None)
            else:
                self.tokens[token] = new_amount
        self.cash_usd -= cash_debit

        # Update token balances - add tokens_in
        for token, amount in fill.tokens_in.items():
            current = self.tokens.get(token, Decimal("0"))
            self.tokens[token] = current + amount

        # Handle cash-equivalent stablecoins as cash
        for stable in CASH_EQUIVALENT_STABLECOINS:
            if stable in self.tokens:
                self.cash_usd += self.tokens.pop(stable)

        # Deduct gas and non-embedded venue costs (fee/slippage) from cash
        self.cash_usd -= fill.gas_cost_usd + self._venue_cash_costs(fill)

    def _plan_token_debits(
        self,
        tokens_out: dict[str, Decimal],
        intent_type: IntentType,
        market_state: MarketState | None,
    ) -> tuple[dict[str, Decimal], Decimal, dict[str, Decimal], str | None]:
        """Validate ``tokens_out`` against held balances without mutating state.

        Cash-equivalent stablecoins draw from the token balance first and
        then from ``cash_usd`` at $1. Other tokens draw from the ``tokens``
        balance; for non-SWAP intents a shortfall is planned as an implicit
        cash conversion at the ``market_state`` price (the engine's flow
        producers size those legs from USD notional, not held balances).
        A relative shortfall within :data:`_DEBIT_DUST_RELATIVE_TOLERANCE`
        is treated as spend-all rather than a failure. Non-positive amounts
        are ignored.

        Returns:
            ``(token_debits, cash_debit, conversions, failure_reason)``
            where ``conversions`` maps token -> implicitly converted token
            amount. When ``failure_reason`` is not None the fill must be
            rejected and the other values ignored.
        """
        no_plan: tuple[dict[str, Decimal], Decimal, dict[str, Decimal]] = ({}, Decimal("0"), {})
        token_debits: dict[str, Decimal] = {}
        conversions: dict[str, Decimal] = {}
        cash_needed = Decimal("0")

        for token, amount in tokens_out.items():
            if amount <= Decimal("0"):
                continue
            held = self.tokens.get(token, Decimal("0"))
            from_tokens = min(held, amount)
            shortfall = amount - from_tokens
            token_debits[token] = from_tokens
            if shortfall <= amount * _DEBIT_DUST_RELATIVE_TOLERANCE:
                continue  # Fully covered (spend-all within dust tolerance)
            if self._is_cash_equivalent(token):
                cash_needed += shortfall
            elif intent_type == IntentType.SWAP:
                # Selling a token the portfolio does not hold is
                # short-from-nothing; never fund a SWAP leg from cash.
                return (*no_plan, f"insufficient {token} balance: required {amount}, held {held}")
            else:
                price = self._conversion_price(token, market_state)
                if price is None:
                    return (
                        *no_plan,
                        f"insufficient {token} balance (required {amount}, held {held}) "
                        "and no market price available for implicit cash conversion",
                    )
                cash_needed += shortfall * price
                conversions[token] = shortfall

        cash_shortfall = cash_needed - self.cash_usd
        if cash_shortfall > Decimal("0"):
            if cash_shortfall > cash_needed * _DEBIT_DUST_RELATIVE_TOLERANCE:
                return (
                    *no_plan,
                    "insufficient cash for stablecoin outflow and implicit conversions: "
                    f"required {cash_needed}, cash {self.cash_usd}",
                )
            # Spend-all within dust tolerance
            cash_needed = self.cash_usd

        return token_debits, cash_needed, conversions, None

    @staticmethod
    def _conversion_price(token: str, market_state: MarketState | None) -> Decimal | None:
        """Market price for an implicit cash conversion, or None if unavailable.

        No $1 fallback here: under-pricing a non-stablecoin leg would mint
        value, which is exactly what this debit path exists to prevent.
        """
        if market_state is None:
            return None
        try:
            price = market_state.get_price(token)
        except KeyError:
            return None
        if price <= Decimal("0"):
            return None
        return price

    def _is_cash_equivalent(self, token: Any) -> bool:
        """True if ``token`` is a stablecoin the portfolio holds as ``cash_usd``."""
        return isinstance(token, str) and token.upper() in CASH_EQUIVALENT_STABLECOINS

    def _venue_cash_costs(self, fill: SimulatedFill) -> Decimal:
        """Fee and slippage payable from cash for this fill (VIB-5079).

        A venue cost is debited from cash exactly when nothing else in the
        fill embodies it. SWAP nets both costs into ``tokens_in``; perps
        embody slippage in ``executed_price`` but pay their fee in cash;
        every other intent type's flows are sized at oracle price for the
        full notional, so both costs are debited. Without this debit those
        costs exist only on the TradeRecord and the backtest overstates
        PnL by exactly the venue costs of every non-SWAP trade.
        """
        costs = Decimal("0")
        if fill.intent_type not in _FEE_EMBEDDED_IN_FLOWS:
            costs += fill.fee_usd
        if fill.intent_type not in _SLIPPAGE_EMBEDDED_IN_PRICE:
            costs += fill.slippage_usd
        return costs

    def _cash_funding_failure(self, fill: SimulatedFill, cash_debit: Decimal) -> str | None:
        """Reason this fill cannot fund its cash legs, or None if it can.

        Aggregates every cash draw the fill will make -- the planned
        stablecoin-outflow / implicit-conversion debit plus perp-open
        collateral -- and requires gas and non-embedded venue costs
        (:meth:`_venue_cash_costs`) on top, so partial checks cannot each
        pass while their sum overdraws ``cash_usd``.

        Fills that draw nothing from cash (sells, closes, inflow-only
        fills) are exempt: gas and venue costs stay charged unconditionally
        there so a risk-reducing close is never blocked for being low on
        cash (the close itself replenishes it). Those debits may
        transiently drive ``cash_usd`` negative -- an accepted modeling
        choice, since a debit cannot mint value.
        """
        required_cash = cash_debit
        if fill.position_delta is not None and fill.position_delta.is_perp:
            required_cash += fill.position_delta.collateral_usd
        if required_cash <= Decimal("0"):
            return None
        venue_costs = self._venue_cash_costs(fill)
        required_with_costs = required_cash + fill.gas_cost_usd + venue_costs
        if required_with_costs > self.cash_usd:
            return (
                f"insufficient cash for fill: required {required_with_costs} "
                f"(cash legs {required_cash} + gas {fill.gas_cost_usd} + venue costs {venue_costs}), "
                f"cash {self.cash_usd}"
            )
        return None

    def _apply_perp_close_credit(
        self,
        position: SimulatedPosition,
        fill: SimulatedFill,
        pnl_usd: Decimal,
    ) -> Decimal:
        """Credit cash for a closed perp position and return the trade's PnL.

        Liquidated positions already carry their loss and penalty in
        ``collateral_usd`` (see the liquidation simulators), so only that
        remainder comes back and ``pnl_usd`` passes through unchanged.
        """
        if position.is_liquidated:
            self.cash_usd += position.collateral_usd
            return pnl_usd
        realized = self._perp_realized_pnl(position, fill)
        self.cash_usd += position.collateral_usd + realized
        return realized

    def _record_failed_fill(self, fill: SimulatedFill, reason: str) -> None:
        """Record ``fill`` as a failed trade without touching balances.

        Execution costs are zeroed (originals stashed in ``metadata`` under
        ``*_unapplied``) so the recorded trade matches the books: a rejected
        fill charges nothing.
        """
        fill.success = False
        fill.metadata.setdefault("failure_reason", reason)
        for cost_field in ("fee_usd", "slippage_usd", "gas_cost_usd"):
            original = getattr(fill, cost_field)
            if original != Decimal("0"):
                fill.metadata[f"{cost_field}_unapplied"] = str(original)
                setattr(fill, cost_field, Decimal("0"))
        logger.warning(
            "Rejected %s fill on %s: %s",
            fill.intent_type.value,
            fill.protocol,
            reason,
        )
        self.trades.append(fill.to_trade_record(pnl_usd=Decimal("0")))

    def _perp_realized_pnl(self, position: SimulatedPosition, fill: SimulatedFill) -> Decimal:
        """Realized PnL for closing a perp: price PnL + accumulated funding.

        ``fill.metadata["realized_pnl_usd"]`` (set by adapter lanes) takes
        precedence. Otherwise the price PnL is computed from the fill's
        executed price against the position entry, matching the formula
        every perp valuation path uses (collateral + price PnL +
        accumulated funding), so the close credit is value-neutral at the
        close instant.
        """
        if "realized_pnl_usd" in fill.metadata:
            # str() round-trips Decimal losslessly, so no type branch is
            # needed (and the VIB-4062 bifurcation guard forbids one).
            return Decimal(str(fill.metadata["realized_pnl_usd"]))

        if position.entry_price <= Decimal("0") or fill.executed_price <= Decimal("0"):
            logger.warning(
                "Cannot compute perp price PnL for %s (entry_price=%s, executed_price=%s); "
                "crediting collateral + accumulated funding only",
                position.position_id,
                position.entry_price,
                fill.executed_price,
            )
            return position.accumulated_funding

        price_change_pct = (fill.executed_price - position.entry_price) / position.entry_price
        if position.position_type == PositionType.PERP_SHORT:
            price_change_pct = -price_change_pct
        return price_change_pct * position.notional_usd + position.accumulated_funding

    def _close_position(self, position_id: str, timestamp: datetime) -> SimulatedPosition | None:
        """Close a position by ID and move to closed list.

        Args:
            position_id: ID of position to close
            timestamp: When the position was closed

        Returns:
            The closed position if found, None otherwise
        """
        for i, pos in enumerate(self.positions):
            if pos.position_id == position_id:
                closed = self.positions.pop(i)
                closed.last_updated = timestamp
                self._closed_positions.append(closed)
                return closed
        return None

    def validate_margin_for_perp(
        self,
        position_size: Decimal,
        collateral: Decimal,
        margin_ratio: Decimal | None = None,
    ) -> tuple[bool, str]:
        """Validate margin requirements for a perpetual position.

        Checks if the collateral is sufficient for the given position size
        and whether opening the position would exceed margin utilization limits.

        Args:
            position_size: Notional size of the position in USD
            collateral: Collateral amount in USD
            margin_ratio: Required margin ratio (default: self.initial_margin_ratio)

        Returns:
            Tuple of (is_valid: bool, message: str)

        Example:
            is_valid, msg = portfolio.validate_margin_for_perp(
                position_size=Decimal("10000"),
                collateral=Decimal("1000"),
            )
            if not is_valid:
                print(f"Cannot open position: {msg}")
        """
        # Lazy import to avoid circular dependency
        from almanak.framework.backtesting.pnl.calculators.margin import MarginValidator

        validator = MarginValidator(
            default_initial_margin_ratio=self.initial_margin_ratio,
            default_maintenance_margin_ratio=self.maintenance_margin_ratio,
        )

        required_ratio = margin_ratio or self.initial_margin_ratio

        # Get current margin state
        current_margin_used = self._get_total_margin_used()

        # Use the validator to check all conditions
        return validator.can_open_position(
            position_size=position_size,
            collateral=collateral,
            available_capital=self.cash_usd,
            current_margin_used=current_margin_used,
            margin_ratio=required_ratio,
        )

    def _get_total_margin_used(self) -> Decimal:
        """Calculate total margin currently locked in perp positions.

        Returns:
            Total collateral locked in open perp positions
        """
        total = Decimal("0")
        for pos in self.positions:
            if pos.is_perp:
                total += pos.collateral_usd
        return total

    def _get_total_perp_notional(self) -> Decimal:
        """Calculate total notional value of open perp positions.

        Returns:
            Total notional value of all open perp positions
        """
        total = Decimal("0")
        for pos in self.positions:
            if pos.is_perp:
                total += pos.notional_usd
        return total

    def calculate_unrealized_pnl(self, market_state: MarketState) -> Decimal:
        """Calculate total unrealized PnL from all open positions.

        This method calculates the unrealized PnL by comparing the current
        market value of each open position to its entry value.

        For SPOT positions:
        - Unrealized PnL = (current_price - entry_price) * amount

        For LP positions:
        - Unrealized PnL = current_value - entry_value + accumulated_fees
        - Takes into account impermanent loss

        For PERP positions:
        - Unrealized PnL = (current_price - entry_price) * notional / leverage
        - Includes accumulated funding payments

        For SUPPLY positions:
        - Unrealized PnL = accrued interest (positive, as interest is earned)

        For BORROW positions:
        - Unrealized PnL = -accrued interest (negative, as interest is owed)

        Args:
            market_state: Current market state containing prices

        Returns:
            Total unrealized PnL in USD across all open positions
        """
        total_unrealized = Decimal("0")

        for position in self.positions:
            if position.is_spot:
                total_unrealized += self._calculate_spot_unrealized_pnl(position, market_state)
            elif position.is_lp:
                total_unrealized += self._calculate_lp_unrealized_pnl(position, market_state)
            elif position.is_perp:
                total_unrealized += self._calculate_perp_unrealized_pnl(position, market_state)
            elif position.is_lending:
                total_unrealized += self._calculate_lending_unrealized_pnl(position)

        return total_unrealized

    def _calculate_spot_unrealized_pnl(self, position: SimulatedPosition, market_state: MarketState) -> Decimal:
        """Calculate unrealized PnL for a spot position.

        Args:
            position: The spot position
            market_state: Current market state

        Returns:
            Unrealized PnL in USD
        """
        unrealized = Decimal("0")
        for token, amount in position.amounts.items():
            try:
                current_price = market_state.get_price(token)
            except KeyError:
                # Fall back to entry price if current price unavailable
                current_price = position.entry_price
            entry_price = position.entry_price
            unrealized += (current_price - entry_price) * amount
        return unrealized

    def _calculate_lp_unrealized_pnl(self, position: SimulatedPosition, market_state: MarketState) -> Decimal:
        """Calculate unrealized PnL for an LP position.

        For LP positions, unrealized PnL includes:
        - Value change from impermanent loss
        - Accumulated trading fees

        Args:
            position: The LP position
            market_state: Current market state

        Returns:
            Unrealized PnL in USD
        """
        if len(position.tokens) < 2:
            return Decimal("0")

        token0 = position.tokens[0]
        token1 = position.tokens[1]

        # Get current prices
        try:
            token0_price = market_state.get_price(token0)
        except KeyError:
            if self.strict_reproducibility:
                raise ValueError(
                    f"Price unavailable for {token0} in get_unrealized_pnl and strict_reproducibility=True."
                ) from None
            logger.warning("Price unavailable for %s, falling back to entry_price in get_unrealized_pnl", token0)
            token0_price = position.entry_price

        # token1 delegates to _stablecoin_fallback which allows $1 for known stablecoins
        # even in strict mode -- token0 has no safe fallback so it raises unconditionally.
        try:
            token1_price = market_state.get_price(token1)
        except KeyError:
            token1_price = self._stablecoin_fallback(token1, "get_unrealized_pnl")

        # Calculate current value
        current_token0 = position.amounts.get(token0, Decimal("0"))
        current_token1 = position.amounts.get(token1, Decimal("0"))
        current_value = current_token0 * token0_price + current_token1 * token1_price

        # Calculate entry value using entry amounts stored in metadata or estimate
        # Entry value is typically: entry_token0 * entry_price + entry_token1 * 1 (for stablecoin quote)
        # For simplicity, use liquidity * sqrt(entry_price) as a proxy if entry amounts not stored
        entry_amounts = position.metadata.get("entry_amounts", {})
        if entry_amounts:
            entry_token0 = Decimal(str(entry_amounts.get(token0, "0")))
            entry_token1 = Decimal(str(entry_amounts.get(token1, "0")))
            entry_value = entry_token0 * position.entry_price + entry_token1 * token1_price
        else:
            # Use initial value from liquidity (approximate)
            # This is a fallback; in practice, entry amounts should be tracked
            entry_value = position.liquidity * position.entry_price.sqrt() if position.liquidity > 0 else Decimal("0")

        # Include accumulated fees as part of unrealized gains
        fees_earned = position.accumulated_fees_usd
        if fees_earned == Decimal("0"):
            fees_earned = position.fees_earned

        # Unrealized PnL = (current value + fees) - entry value
        return (current_value + fees_earned) - entry_value

    def _calculate_perp_unrealized_pnl(self, position: SimulatedPosition, market_state: MarketState) -> Decimal:
        """Calculate unrealized PnL for a perpetual position.

        For perp positions, unrealized PnL includes:
        - Price movement PnL: (current - entry) * notional / entry for longs
        - Price movement PnL: (entry - current) * notional / entry for shorts
        - Accumulated funding (positive = received, negative = paid)

        Args:
            position: The perpetual position
            market_state: Current market state

        Returns:
            Unrealized PnL in USD
        """
        if not position.tokens:
            return Decimal("0")

        primary_token = position.primary_token
        try:
            current_price = market_state.get_price(primary_token)
        except KeyError:
            current_price = position.entry_price

        entry_price = position.entry_price
        if entry_price == Decimal("0"):
            return Decimal("0")

        # Calculate price movement PnL
        price_change_pct = (current_price - entry_price) / entry_price

        if position.position_type == PositionType.PERP_LONG:
            # Long profits when price goes up
            price_pnl = position.notional_usd * price_change_pct
        else:  # PERP_SHORT
            # Short profits when price goes down
            price_pnl = -position.notional_usd * price_change_pct

        # Include accumulated funding (positive = received, negative = paid)
        # Net funding = received - paid
        net_funding = position.cumulative_funding_received - position.cumulative_funding_paid

        return price_pnl + net_funding

    def _calculate_lending_unrealized_pnl(self, position: SimulatedPosition) -> Decimal:
        """Calculate unrealized PnL for a lending position.

        For SUPPLY positions:
        - Unrealized PnL = interest accrued (positive)

        For BORROW positions:
        - Unrealized PnL = -interest accrued (negative, as it's debt)

        Args:
            position: The lending position

        Returns:
            Unrealized PnL in USD
        """
        if position.position_type == PositionType.SUPPLY:
            # Interest earned is positive PnL
            return position.interest_accrued
        else:  # BORROW
            # Interest owed is negative PnL
            return -position.interest_accrued

    def get_margin_utilization(self) -> Decimal:
        """Calculate current margin utilization ratio.

        Formula:
            utilization = total_margin_used / (total_margin_used + available_cash)

        Returns:
            Margin utilization ratio (0 to 1)
        """
        total_margin = self._get_total_margin_used()
        total_capital = total_margin + self.cash_usd

        if total_capital == Decimal("0"):
            return Decimal("0")

        return total_margin / total_capital

    def update_max_margin_utilization(self) -> None:
        """Update the maximum margin utilization observed during backtest.

        This should be called after each trade or mark-to-market to track
        the peak margin utilization for metrics reporting.
        """
        current = self.get_margin_utilization()
        if current > self._max_margin_utilization:
            self._max_margin_utilization = current

    def check_can_open_perp_position(
        self,
        position: SimulatedPosition,
    ) -> tuple[bool, str]:
        """Check if a perp position can be opened given current capital constraints.

        This method validates:
        1. Sufficient collateral for the position size (margin check)
        2. Enough available cash for the collateral
        3. Would not exceed maximum margin utilization

        Args:
            position: The SimulatedPosition to validate (must be a perp position)

        Returns:
            Tuple of (can_open: bool, reason: str)

        Example:
            can_open, reason = portfolio.check_can_open_perp_position(perp_long)
            if not can_open:
                # Reject the position
                return None
        """
        if not position.is_perp:
            return True, "Not a perp position, no margin check needed"

        return self.validate_margin_for_perp(
            position_size=position.notional_usd,
            collateral=position.collateral_usd,
        )

    def _calculate_trade_pnl(self, fill: SimulatedFill) -> Decimal:
        """Calculate PnL for a trade based on fill details.

        For now, this is a simplified calculation. The actual PnL
        depends on the intent type:
        - SWAP: realized when converting between assets
        - LP_CLOSE: realized from IL + fees
        - PERP_CLOSE: realized from price movement + funding
        - WITHDRAW/REPAY: realized from interest

        Args:
            fill: The simulated fill

        Returns:
            Realized PnL in USD (before execution costs)
        """
        # For swaps, PnL is the difference between value received and value sent
        # minus slippage (slippage is the cost of the trade itself)
        if fill.intent_type == IntentType.SWAP:
            # The slippage_usd already captures the "loss" from non-ideal execution
            # So trade PnL for a swap is essentially 0 minus costs
            return Decimal("0")

        # For position closes, check if we have metadata about the close
        if fill.intent_type in (IntentType.LP_CLOSE, IntentType.PERP_CLOSE):
            pnl_value = fill.metadata.get("realized_pnl_usd", Decimal("0"))
            return Decimal(str(pnl_value)) if not isinstance(pnl_value, Decimal) else pnl_value

        # For lending operations, interest is typically in metadata
        if fill.intent_type in (IntentType.WITHDRAW, IntentType.REPAY):
            interest_value = fill.metadata.get("interest_usd", Decimal("0"))
            return Decimal(str(interest_value)) if not isinstance(interest_value, Decimal) else interest_value

        return Decimal("0")

    def _calculate_lp_pnl_breakdown(
        self,
        position: SimulatedPosition,
        fill: SimulatedFill,
    ) -> tuple[Decimal, Decimal, Decimal]:
        """Calculate LP PnL breakdown for a closed position.

        Calculates the detailed PnL components for an LP position at close:
        - Impermanent Loss (IL): Loss compared to just holding the tokens
        - Fees Earned: Trading fees accumulated during the position lifetime
        - Net LP PnL: (Current Value + Fees) - Initial Value

        The IL is calculated using the ImpermanentLossCalculator based on
        price movement from entry to close.

        Args:
            position: The closed LP position with accumulated fees
            fill: The fill that closed the position (contains current prices in tokens_in)

        Returns:
            Tuple of (il_loss_usd, fees_earned_usd, net_lp_pnl_usd)
            - il_loss_usd: Impermanent loss in USD (positive = loss)
            - fees_earned_usd: Total fees earned in USD
            - net_lp_pnl_usd: Net PnL = (Current Value + Fees) - Initial Value
        """
        # Lazy import to avoid circular dependency
        from almanak.framework.backtesting.pnl.calculators.impermanent_loss import (
            ImpermanentLossCalculator,
        )

        # Extract fees earned from the position
        fees_earned_usd = position.accumulated_fees_usd
        if fees_earned_usd == Decimal("0"):
            # Fall back to fees_earned if accumulated_fees_usd not set
            fees_earned_usd = position.fees_earned

        # Calculate initial value at entry (token amounts * entry price)
        # For LP positions, entry amounts are tracked separately from current amounts
        # We need to calculate what the position was worth at entry
        if len(position.tokens) < 2:
            return Decimal("0"), fees_earned_usd, Decimal("0")

        token0 = position.tokens[0]
        token1 = position.tokens[1]

        # Get prices - use fill metadata, executed_price, or fall back to entry price
        token0_price = fill.metadata.get("token0_price_usd", fill.executed_price)
        if not isinstance(token0_price, Decimal):
            token0_price = Decimal(str(token0_price))

        # Token1 is typically the quote token (USDC, etc.)
        token1_price = fill.metadata.get("token1_price_usd")
        if token1_price is None:
            token1_price = self._stablecoin_fallback(token1, "record_lp_close")
        if not isinstance(token1_price, Decimal):
            token1_price = Decimal(str(token1_price))

        # Calculate current value from tokens received (tokens_in from the close)
        current_token0 = fill.tokens_in.get(token0, position.amounts.get(token0, Decimal("0")))
        current_token1 = fill.tokens_in.get(token1, position.amounts.get(token1, Decimal("0")))
        current_value = current_token0 * token0_price + current_token1 * token1_price

        # Use ImpermanentLossCalculator to get entry token amounts and IL
        il_calculator = ImpermanentLossCalculator()

        # Get tick bounds
        tick_lower = position.tick_lower if position.tick_lower is not None else -887272
        tick_upper = position.tick_upper if position.tick_upper is not None else 887272

        # Calculate price ratio (current price relative to entry price in token1 terms)
        if position.entry_price > 0:
            current_price_ratio = token0_price / token1_price if token1_price > 0 else position.entry_price

            # Calculate IL percentage
            il_pct, entry_token0, entry_token1 = il_calculator.calculate_il_v3(
                entry_price=position.entry_price,
                current_price=current_price_ratio,
                tick_lower=tick_lower,
                tick_upper=tick_upper,
                liquidity=position.liquidity,
            )

            # Calculate initial value (what the position was worth at entry)
            # Use entry amounts calculated by IL calculator
            initial_value = entry_token0 * position.entry_price * token1_price + entry_token1 * token1_price

            # IL in USD = IL percentage * hold value
            # hold_value is what we would have if we just held entry tokens at current prices
            hold_value = entry_token0 * token0_price + entry_token1 * token1_price
            il_loss_usd = il_pct * hold_value
        else:
            # Fallback if no entry price - use fill amount_usd as initial value estimate
            initial_value = fill.amount_usd if fill.amount_usd > 0 else current_value
            il_loss_usd = Decimal("0")

        # Net LP PnL = (Current Value + Fees) - Initial Value
        # This captures: price appreciation/depreciation, IL, and fee earnings
        net_lp_pnl_usd = (current_value + fees_earned_usd) - initial_value

        return il_loss_usd, fees_earned_usd, net_lp_pnl_usd

    def _resolve_token_symbol(
        self,
        token: str,
        chain_id: int | None,
        require_symbol_mapping: bool,
        data_tracker: "DataQualityTracker | None",
    ) -> str:
        """Resolve a token key to its canonical symbol.

        This method enforces symbol resolution when require_symbol_mapping is enabled.
        It uses the token registry to map addresses to symbols.

        Args:
            token: Token address or symbol to resolve
            chain_id: Chain ID for registry lookup (required for address resolution)
            require_symbol_mapping: If True, fail when symbol cannot be resolved
            data_tracker: Optional tracker to record unresolved tokens

        Returns:
            Resolved symbol (unchanged if already a symbol, or resolved from address)

        Raises:
            ValueError: If require_symbol_mapping is True and token cannot be resolved
        """
        # If token looks like an address (starts with 0x), try to resolve it
        if token.startswith("0x") and len(token) == 42:
            if chain_id is not None:
                # Check if token is known in registry
                if not is_token_known(chain_id, token):
                    # Token is not in registry - it's unresolved
                    if data_tracker is not None:
                        data_tracker.record_unresolved_token(token, chain_id)

                    if require_symbol_mapping:
                        raise ValueError(
                            f"Token address {token} on chain {chain_id} cannot be resolved "
                            f"to a symbol. Enable allow_unknown_tokens or add to registry."
                        )
                    else:
                        # Log warning and use checksummed address
                        logger.warning(
                            f"Unknown token address {token} on chain {chain_id}, "
                            f"using address as fallback for price lookup"
                        )
                        return resolve_to_canonical_symbol(chain_id, token)

                # Resolve to canonical symbol
                return resolve_to_canonical_symbol(chain_id, token)
            else:
                # No chain_id provided - cannot resolve addresses
                if require_symbol_mapping:
                    raise ValueError(
                        f"Token address {token} cannot be resolved without chain_id. "
                        f"Provide chain_id for symbol resolution."
                    )
                # Return token unchanged
                return token

        # Token is already a symbol (not an address)
        return token

    def _handle_missing_price(
        self,
        token: str,
        chain_id: int | None,
        data_tracker: "DataQualityTracker | None",
        simulation_timestamp: datetime | None,
        strict_price_mode: bool,
        context: str = "valuation",
    ) -> None:
        """Handle a missing price lookup with logging and tracking.

        Records the missing price in the data tracker (if provided), logs a warning
        with context, and optionally raises an error in strict mode.

        Args:
            token: The token symbol or address for which price was not found
            chain_id: Chain ID for context
            data_tracker: Optional tracker to record missing prices
            simulation_timestamp: Current simulation timestamp for logging
            strict_price_mode: If True, raise ValueError after logging
            context: Description of where the price was needed (e.g., "SPOT position")

        Raises:
            ValueError: If strict_price_mode is True
        """
        # Record missing price in tracker
        if data_tracker is not None:
            data_tracker.record_missing_price(
                token=token,
                timestamp=simulation_timestamp,
                chain_id=chain_id,
            )

        # Log warning with context
        timestamp_str = simulation_timestamp.isoformat() if simulation_timestamp else "unknown"
        logger.warning(
            "Missing price for token %s at timestamp %s (chain_id=%s, context=%s). Using fallback value.",
            token,
            timestamp_str,
            chain_id or "unknown",
            context,
        )

        # In strict mode, fail instead of using fallback
        if strict_price_mode:
            raise ValueError(
                f"Missing price for token {token} at timestamp {timestamp_str} "
                f"(chain_id={chain_id}, context={context}) and strict_price_mode is enabled. "
                "Ensure price data is available or disable strict_price_mode."
            )

    def get_total_value_usd(
        self,
        market_state: MarketState,
        *,
        require_symbol_mapping: bool = False,
        chain_id: int | None = None,
        data_tracker: "DataQualityTracker | None" = None,
        simulation_timestamp: datetime | None = None,
        strict_price_mode: bool = False,
    ) -> Decimal:
        """Calculate total portfolio value at current market prices.

        Sums:
        - Cash balance (USD)
        - Token holdings (valued at market prices)
        - Position values (spot value only - IL/funding handled by mark_to_market)

        Args:
            market_state: Current market state with prices
            require_symbol_mapping: If True, fail when token symbols cannot be resolved.
                When False (default), use address as fallback and log warning.
            chain_id: Chain ID for token registry lookup. Required when
                require_symbol_mapping is True and tokens are addresses.
            data_tracker: Optional tracker to record missing prices.
            simulation_timestamp: Current simulation timestamp for logging context.
            strict_price_mode: If True, raise ValueError when price is missing.
                When False (default), log warning and skip the token.

        Returns:
            Total portfolio value in USD

        Raises:
            ValueError: If require_symbol_mapping is True and any token cannot be resolved,
                or if strict_price_mode is True and any price is missing.
        """
        total = self.cash_usd

        # Value of token holdings
        for token, amount in self.tokens.items():
            try:
                # Resolve token to symbol if needed
                resolved_token = self._resolve_token_symbol(token, chain_id, require_symbol_mapping, data_tracker)
                price = market_state.get_price(resolved_token)
                total += amount * price
            except KeyError:
                # Record missing price in tracker
                if data_tracker is not None:
                    data_tracker.record_missing_price(
                        token=token,
                        timestamp=simulation_timestamp,
                        chain_id=chain_id,
                    )
                # Log warning with context
                timestamp_str = simulation_timestamp.isoformat() if simulation_timestamp else "unknown"
                logger.warning(
                    "Missing price for token %s at timestamp %s (chain_id=%s). Token skipped in portfolio valuation.",
                    token,
                    timestamp_str,
                    chain_id or "unknown",
                )
                # In strict mode, fail instead of skipping
                if strict_price_mode:
                    raise ValueError(
                        f"Missing price for token {token} at timestamp {timestamp_str} "
                        f"(chain_id={chain_id}) and strict_price_mode is enabled. "
                        "Ensure price data is available or disable strict_price_mode."
                    ) from None

        # Value of positions (basic calculation - mark_to_market handles complex cases)
        for position in self.positions:
            total += self._get_position_value(
                position,
                market_state,
                require_symbol_mapping=require_symbol_mapping,
                chain_id=chain_id,
                data_tracker=data_tracker,
                simulation_timestamp=simulation_timestamp,
                strict_price_mode=strict_price_mode,
            )

        return total

    def _get_position_value(
        self,
        position: SimulatedPosition,
        market_state: MarketState,
        *,
        require_symbol_mapping: bool = False,
        chain_id: int | None = None,
        data_tracker: "DataQualityTracker | None" = None,
        simulation_timestamp: datetime | None = None,
        strict_price_mode: bool = False,
    ) -> Decimal:
        """Calculate the current value of a position.

        For SPOT positions, this is simply amount * price.
        For other position types, we use a basic calculation here
        (the mark_to_market method handles IL, funding, interest in detail).

        Args:
            position: The position to value
            market_state: Current market state
            require_symbol_mapping: If True, fail when token symbols cannot be resolved
            chain_id: Chain ID for token registry lookup
            data_tracker: Optional tracker to record missing prices
            simulation_timestamp: Current simulation timestamp for logging context
            strict_price_mode: If True, raise ValueError when price is missing

        Returns:
            Position value in USD

        Raises:
            ValueError: If require_symbol_mapping is True and any token cannot be resolved,
                or if strict_price_mode is True and any price is missing.
        """
        if position.is_spot:
            # Simple spot position
            token = position.primary_token
            resolved_token = self._resolve_token_symbol(token, chain_id, require_symbol_mapping, data_tracker)
            try:
                price = market_state.get_price(resolved_token)
                return position.total_amount * price
            except KeyError:
                # Record and log missing price
                self._handle_missing_price(
                    token=token,
                    chain_id=chain_id,
                    data_tracker=data_tracker,
                    simulation_timestamp=simulation_timestamp,
                    strict_price_mode=strict_price_mode,
                    context="SPOT position",
                )
                # Fall back to entry price if not in strict mode
                return position.total_amount * position.entry_price

        elif position.is_lp:
            # LP position: sum of token values + fees earned
            value = Decimal("0")
            for token, amount in position.amounts.items():
                resolved_token = self._resolve_token_symbol(token, chain_id, require_symbol_mapping, data_tracker)
                try:
                    price = market_state.get_price(resolved_token)
                    value += amount * price
                except KeyError:
                    # Record and log missing price
                    self._handle_missing_price(
                        token=token,
                        chain_id=chain_id,
                        data_tracker=data_tracker,
                        simulation_timestamp=simulation_timestamp,
                        strict_price_mode=strict_price_mode,
                        context="LP position",
                    )
            # Add accumulated fees
            value += position.fees_earned
            return value

        elif position.is_perp:
            # Perp position: collateral + unrealized PnL
            token = position.primary_token
            resolved_token = self._resolve_token_symbol(token, chain_id, require_symbol_mapping, data_tracker)
            try:
                current_price = market_state.get_price(resolved_token)
            except KeyError:
                # Record and log missing price
                self._handle_missing_price(
                    token=token,
                    chain_id=chain_id,
                    data_tracker=data_tracker,
                    simulation_timestamp=simulation_timestamp,
                    strict_price_mode=strict_price_mode,
                    context="PERP position",
                )
                current_price = position.entry_price

            # Calculate unrealized PnL
            price_change = current_price - position.entry_price
            if position.position_type == PositionType.PERP_SHORT:
                price_change = -price_change  # Short profits when price falls

            unrealized_pnl = (price_change / position.entry_price) * position.notional_usd

            return position.collateral_usd + unrealized_pnl + position.accumulated_funding

        elif position.is_lending:
            # Lending position: principal + interest
            token = position.primary_token
            resolved_token = self._resolve_token_symbol(token, chain_id, require_symbol_mapping, data_tracker)
            try:
                price = market_state.get_price(resolved_token)
            except KeyError:
                # Record and log missing price
                self._handle_missing_price(
                    token=token,
                    chain_id=chain_id,
                    data_tracker=data_tracker,
                    simulation_timestamp=simulation_timestamp,
                    strict_price_mode=strict_price_mode,
                    context="LENDING position",
                )
                price = position.entry_price

            principal_value = position.total_amount * price

            if position.position_type == PositionType.SUPPLY:
                # Supply earns interest
                return principal_value + position.interest_accrued
            else:
                # Borrow owes interest (returns negative for debt value)
                return -(principal_value + position.interest_accrued)

        return Decimal("0")

    def get_metrics(self) -> BacktestMetrics:
        """Calculate backtest metrics from equity curve and trades.

        Calculates comprehensive performance metrics including:
        - PnL and returns
        - Sharpe and Sortino ratios
        - Max drawdown
        - Win rate and profit factor
        - Execution costs

        Returns:
            BacktestMetrics instance with all calculated metrics
        """
        if not self.equity_curve:
            return BacktestMetrics()

        # Extract values for calculations
        equity_values = [p.value_usd for p in self.equity_curve]
        timestamps = [p.timestamp for p in self.equity_curve]

        # Total PnL
        initial_value = equity_values[0] if equity_values else self.initial_capital_usd
        final_value = equity_values[-1] if equity_values else self.initial_capital_usd

        total_pnl = final_value - initial_value

        # Execution costs
        total_fees = sum((t.fee_usd for t in self.trades), Decimal("0"))
        total_slippage = sum((t.slippage_usd for t in self.trades), Decimal("0"))
        total_gas = sum((t.gas_cost_usd for t in self.trades), Decimal("0"))
        net_pnl = total_pnl - total_fees - total_slippage - total_gas

        # Returns
        total_return = (final_value - initial_value) / initial_value if initial_value > 0 else Decimal("0")

        # Annualized return (if we have timestamps)
        annualized_return = Decimal("0")
        if len(timestamps) >= 2:
            duration_days = (timestamps[-1] - timestamps[0]).total_seconds() / (24 * 3600)
            if duration_days > 0:
                years = Decimal(str(duration_days)) / Decimal("365")
                if years > 0:
                    # (1 + total_return) ^ (1/years) - 1
                    annualized_return = (Decimal("1") + total_return) ** (Decimal("1") / years) - Decimal("1")

        # Calculate returns series for volatility/Sharpe
        returns = self._calculate_returns(equity_values)

        # Volatility (annualized std dev of returns)
        volatility = self._calculate_volatility(returns)

        # Sharpe ratio (assuming 0 risk-free rate)
        sharpe = self._calculate_sharpe(returns, volatility)

        # Sortino ratio
        sortino = self._calculate_sortino(returns)

        # Max drawdown
        max_drawdown = self._calculate_max_drawdown(equity_values)

        # Calmar ratio
        calmar = Decimal("0")
        if max_drawdown > 0:
            calmar = annualized_return / max_drawdown

        # Trade statistics
        winning_trades = [t for t in self.trades if t.net_pnl_usd > 0]
        losing_trades = [t for t in self.trades if t.net_pnl_usd <= 0]

        win_rate = Decimal(str(len(winning_trades))) / Decimal(str(len(self.trades))) if self.trades else Decimal("0")

        # Profit factor
        gross_profit = sum((t.net_pnl_usd for t in winning_trades), Decimal("0"))
        gross_loss = abs(sum((t.net_pnl_usd for t in losing_trades), Decimal("0")))
        profit_factor = gross_profit / gross_loss if gross_loss > Decimal("0") else Decimal("0")

        # Average trade PnL
        avg_trade_pnl = (
            sum((t.net_pnl_usd for t in self.trades), Decimal("0")) / Decimal(str(len(self.trades)))
            if self.trades
            else Decimal("0")
        )

        # Largest win/loss
        trade_pnls = [t.net_pnl_usd for t in self.trades]
        largest_win = max(trade_pnls, default=Decimal("0"))
        largest_loss = min(trade_pnls, default=Decimal("0"))

        # Average win/loss
        avg_win = (
            sum((t.net_pnl_usd for t in winning_trades), Decimal("0")) / Decimal(str(len(winning_trades)))
            if winning_trades
            else Decimal("0")
        )
        avg_loss = (
            sum((t.net_pnl_usd for t in losing_trades), Decimal("0")) / Decimal(str(len(losing_trades)))
            if losing_trades
            else Decimal("0")
        )

        # Aggregate fees from LP positions (both open and closed)
        total_fees_earned = Decimal("0")
        fees_by_pool: dict[str, Decimal] = {}
        lp_fee_confidence_breakdown: dict[str, int] = {"high": 0, "medium": 0, "low": 0}

        # Aggregate funding from perp positions (both open and closed)
        total_funding_paid = Decimal("0")
        total_funding_received = Decimal("0")

        # Aggregate interest from lending positions (both open and closed)
        total_interest_earned = Decimal("0")
        total_interest_paid = Decimal("0")

        all_positions = list(self.positions) + list(self._closed_positions)
        for position in all_positions:
            if position.is_lp:
                total_fees_earned += position.fees_earned
                # Use position_id as pool identifier
                pool_id = position.position_id
                if pool_id in fees_by_pool:
                    fees_by_pool[pool_id] += position.fees_earned
                else:
                    fees_by_pool[pool_id] = position.fees_earned
                # Track fee confidence breakdown
                if position.fee_confidence in lp_fee_confidence_breakdown:
                    lp_fee_confidence_breakdown[position.fee_confidence] += 1
                elif position.fee_confidence is not None:
                    # Unknown confidence level - treat as low
                    lp_fee_confidence_breakdown["low"] += 1
            elif position.is_perp:
                total_funding_paid += position.cumulative_funding_paid
                total_funding_received += position.cumulative_funding_received
            elif position.is_lending:
                # SUPPLY positions earn interest, BORROW positions pay interest
                if position.position_type == PositionType.SUPPLY:
                    total_interest_earned += position.interest_accrued
                else:
                    total_interest_paid += position.interest_accrued

        return BacktestMetrics(
            total_pnl_usd=total_pnl,
            net_pnl_usd=net_pnl,
            sharpe_ratio=sharpe,
            max_drawdown_pct=max_drawdown,
            win_rate=win_rate,
            total_trades=len(self.trades),
            profit_factor=profit_factor,
            # VIB-2915: `*_return_pct` fields store actual percentages (e.g. 10 for 10%),
            # not decimal ratios. Local `total_return`/`annualized_return` stay as ratios
            # so the calmar calculation above (which divides by `max_drawdown`, still a ratio) stays correct.
            total_return_pct=total_return * Decimal("100"),
            annualized_return_pct=annualized_return * Decimal("100"),
            total_fees_usd=total_fees,
            total_slippage_usd=total_slippage,
            total_gas_usd=total_gas,
            winning_trades=len(winning_trades),
            losing_trades=len(losing_trades),
            avg_trade_pnl_usd=avg_trade_pnl,
            largest_win_usd=largest_win,
            largest_loss_usd=largest_loss,
            avg_win_usd=avg_win,
            avg_loss_usd=avg_loss,
            volatility=volatility,
            sortino_ratio=sortino,
            calmar_ratio=calmar,
            total_fees_earned_usd=total_fees_earned,
            fees_by_pool=fees_by_pool,
            lp_fee_confidence_breakdown=lp_fee_confidence_breakdown,
            total_funding_paid=total_funding_paid,
            total_funding_received=total_funding_received,
            max_margin_utilization=self._max_margin_utilization,
            total_interest_earned=total_interest_earned,
            total_interest_paid=total_interest_paid,
            min_health_factor=self._min_health_factor,
            health_factor_warnings=self._health_factor_warnings,
            realized_pnl=self._realized_pnl,
            unrealized_pnl=self._unrealized_pnl,
        )

    def calculate_data_coverage_metrics(self) -> DataCoverageMetrics:  # noqa: C901
        """Calculate data coverage metrics across all position types.

        Aggregates confidence levels and data sources from all positions
        (LP, Perp, Lending) and slippage calculations to provide an overall
        view of data quality in the backtest.

        Returns:
            DataCoverageMetrics with breakdown by position type and overall coverage.
        """
        # LP metrics
        lp_positions = [p for p in list(self.positions) + list(self._closed_positions) if p.is_lp]
        lp_confidence_breakdown: dict[str, int] = {"high": 0, "medium": 0, "low": 0}
        lp_data_sources: list[str] = []

        for position in lp_positions:
            if position.fee_confidence in lp_confidence_breakdown:
                lp_confidence_breakdown[position.fee_confidence] += 1
            elif position.fee_confidence is not None:
                lp_confidence_breakdown["low"] += 1
            # Track data sources (from metadata or position fields)
            if hasattr(position, "metadata") and position.metadata.get("data_source"):
                source = position.metadata["data_source"]
                if source not in lp_data_sources:
                    lp_data_sources.append(source)

        lp_metrics = LPMetrics(
            position_count=len(lp_positions),
            fee_confidence_breakdown=lp_confidence_breakdown,
            data_sources=lp_data_sources,
        )

        # Perp metrics
        perp_positions = [p for p in list(self.positions) + list(self._closed_positions) if p.is_perp]
        perp_confidence_breakdown: dict[str, int] = {"high": 0, "medium": 0, "low": 0}
        perp_data_sources: list[str] = []

        for position in perp_positions:
            if position.funding_confidence in perp_confidence_breakdown:
                perp_confidence_breakdown[position.funding_confidence] += 1
            elif position.funding_confidence is not None:
                perp_confidence_breakdown["low"] += 1
            # Track funding data sources
            if position.funding_data_source and position.funding_data_source not in perp_data_sources:
                perp_data_sources.append(position.funding_data_source)

        perp_metrics = PerpMetrics(
            position_count=len(perp_positions),
            funding_confidence_breakdown=perp_confidence_breakdown,
            data_sources=perp_data_sources,
        )

        # Lending metrics
        lending_positions = [p for p in list(self.positions) + list(self._closed_positions) if p.is_lending]
        lending_confidence_breakdown: dict[str, int] = {"high": 0, "medium": 0, "low": 0}
        lending_data_sources: list[str] = []

        for position in lending_positions:
            if position.apy_confidence in lending_confidence_breakdown:
                lending_confidence_breakdown[position.apy_confidence] += 1
            elif position.apy_confidence is not None:
                lending_confidence_breakdown["low"] += 1
            # Track APY data sources
            if position.apy_data_source and position.apy_data_source not in lending_data_sources:
                lending_data_sources.append(position.apy_data_source)

        lending_metrics = LendingMetrics(
            position_count=len(lending_positions),
            apy_confidence_breakdown=lending_confidence_breakdown,
            data_sources=lending_data_sources,
        )

        # Slippage metrics - collect from all LP positions (slippage confidence tracked there)
        slippage_confidence_breakdown: dict[str, int] = {"high": 0, "medium": 0, "low": 0}
        slippage_calculation_count = 0

        for position in lp_positions:
            if position.slippage_confidence is not None:
                slippage_calculation_count += 1
                if position.slippage_confidence in slippage_confidence_breakdown:
                    slippage_confidence_breakdown[position.slippage_confidence] += 1
                else:
                    slippage_confidence_breakdown["low"] += 1

        slippage_metrics = SlippageMetrics(
            calculation_count=slippage_calculation_count,
            slippage_confidence_breakdown=slippage_confidence_breakdown,
        )

        return DataCoverageMetrics(
            lp_metrics=lp_metrics,
            perp_metrics=perp_metrics,
            lending_metrics=lending_metrics,
            slippage_metrics=slippage_metrics,
        )

    def _calculate_returns(self, values: list[Decimal]) -> list[Decimal]:
        """Calculate period-over-period returns from equity values.

        Args:
            values: List of equity values

        Returns:
            List of returns (value[i]/value[i-1] - 1)
        """
        if len(values) < 2:
            return []

        returns = []
        for i in range(1, len(values)):
            if values[i - 1] > 0:
                ret = (values[i] - values[i - 1]) / values[i - 1]
                returns.append(ret)
        return returns

    def _calculate_volatility(self, returns: list[Decimal]) -> Decimal:
        """Calculate annualized volatility from returns.

        Args:
            returns: List of period returns

        Returns:
            Annualized volatility (std dev * sqrt(252))
        """
        if len(returns) < 2:
            return Decimal("0")

        # Calculate mean
        mean = sum(returns) / Decimal(str(len(returns)))

        # Calculate variance
        variance = sum((r - mean) ** 2 for r in returns) / Decimal(str(len(returns) - 1))

        # Standard deviation (approximation for Decimal)
        std_dev = self._decimal_sqrt(variance)

        # Annualize (assuming daily returns, 252 trading days)
        return std_dev * self._decimal_sqrt(Decimal("252"))

    def _calculate_sharpe(self, returns: list[Decimal], volatility: Decimal) -> Decimal:
        """Calculate Sharpe ratio (assuming 0 risk-free rate).

        Args:
            returns: List of period returns
            volatility: Annualized volatility

        Returns:
            Annualized Sharpe ratio
        """
        if volatility == 0 or not returns:
            return Decimal("0")

        mean_return = sum(returns) / Decimal(str(len(returns)))
        # Annualize mean return (assuming daily)
        annualized_return = mean_return * Decimal("252")

        return annualized_return / volatility

    def _calculate_sortino(self, returns: list[Decimal]) -> Decimal:
        """Calculate Sortino ratio (downside deviation).

        Args:
            returns: List of period returns

        Returns:
            Sortino ratio
        """
        if len(returns) < 2:
            return Decimal("0")

        # Calculate downside deviation (only negative returns)
        negative_returns = [r for r in returns if r < 0]
        if not negative_returns:
            return Decimal("0")

        downside_variance = sum(r**2 for r in negative_returns) / Decimal(str(len(returns)))
        downside_dev = self._decimal_sqrt(downside_variance)

        if downside_dev == 0:
            return Decimal("0")

        # Annualize
        annualized_downside = downside_dev * self._decimal_sqrt(Decimal("252"))
        mean_return = sum(returns) / Decimal(str(len(returns)))
        annualized_return = mean_return * Decimal("252")

        return annualized_return / annualized_downside

    def _calculate_max_drawdown(self, values: list[Decimal]) -> Decimal:
        """Calculate maximum drawdown from equity curve.

        Args:
            values: List of equity values

        Returns:
            Max drawdown as decimal (0.1 = 10%)
        """
        if len(values) < 2:
            return Decimal("0")

        max_drawdown = Decimal("0")
        peak = values[0]

        for value in values:
            if value > peak:
                peak = value
            elif peak > 0:
                drawdown = (peak - value) / peak
                max_drawdown = max(max_drawdown, drawdown)

        return max_drawdown

    def _decimal_sqrt(self, n: Decimal) -> Decimal:
        """Calculate square root of a Decimal using Newton's method.

        Args:
            n: Non-negative Decimal to find sqrt of

        Returns:
            Square root approximation
        """
        if n < 0:
            raise ValueError("Cannot compute sqrt of negative number")
        if n == 0:
            return Decimal("0")

        # Initial guess
        x = n
        # Newton's method iterations
        for _ in range(50):  # Max iterations
            x_new = (x + n / x) / Decimal("2")
            if abs(x_new - x) < Decimal("1e-28"):
                break
            x = x_new
        return x

    def mark_to_market(
        self,
        market_state: MarketState,
        timestamp: datetime,
        adapter: "StrategyBacktestAdapter | None" = None,
    ) -> Decimal:
        """Update portfolio valuation based on current market prices and record to equity curve.

        This method calculates the current value of all positions using market
        prices and records the total portfolio value to the equity curve.

        When an adapter is provided, it will be used for position valuation,
        allowing strategy-specific valuation logic (e.g., LP fee accrual,
        perp funding, lending interest).

        For SPOT positions:
        - Uses market_state.get_price(token) to get current prices
        - Multiplies token amount by current price
        - Sums across all spot positions and token holdings

        For LP positions (with adapter):
        - Uses adapter.value_position() for accurate fee-inclusive valuation
        - Falls back to internal calculation if no adapter

        For PERP positions (with adapter):
        - Uses adapter.value_position() for funding-inclusive valuation
        - Falls back to internal calculation if no adapter

        For lending positions (with adapter):
        - Uses adapter.value_position() for interest-inclusive valuation
        - Falls back to internal calculation if no adapter

        Args:
            market_state: Current market state containing prices
            timestamp: Time at which to mark the portfolio
            adapter: Optional strategy-specific adapter for position valuation.
                When provided, uses adapter.value_position() for non-spot positions.

        Returns:
            Total portfolio value in USD at the given timestamp

        Example:
            # Without adapter (uses internal valuation)
            value = portfolio.mark_to_market(market_state, datetime.now(timezone.utc))

            # With adapter (uses adapter valuation)
            value = portfolio.mark_to_market(market_state, timestamp, adapter=lp_adapter)
            print(f"Portfolio value: ${value}")
        """
        total_value = self.cash_usd

        # Value of direct token holdings (not in positions)
        for token, amount in self.tokens.items():
            try:
                price = market_state.get_price(token)
                total_value += amount * price
            except KeyError:
                # If price not available, skip this token
                pass

        # Value positions using adapter if available, otherwise use internal methods
        for position in self.positions:
            if position.is_spot:
                # Spot positions always use internal valuation
                total_value += self._mark_spot_position(position, market_state)
            elif adapter is not None:
                # Use adapter for non-spot position valuation
                # Pass timestamp for deterministic, reproducible valuation
                try:
                    total_value += adapter.value_position(position, market_state, timestamp)
                except Exception:
                    # Fall back to internal valuation on error
                    total_value += self._value_position_fallback(position, market_state, timestamp)
            elif position.is_lp:
                total_value += self._mark_lp_position(position, market_state, timestamp)
            elif position.is_perp:
                total_value += self._mark_perp_position(position, market_state, timestamp)
            elif position.is_lending:
                total_value += self._mark_lending_position(position, market_state, timestamp)

        # Update health factors for lending positions after all values are calculated
        self._update_health_factors(market_state)

        # Update unrealized PnL tracking at each mark_to_market
        self._unrealized_pnl = self.calculate_unrealized_pnl(market_state)

        # Record the equity point
        self.equity_curve.append(EquityPoint(timestamp=timestamp, value_usd=total_value))

        return total_value

    def _value_position_fallback(
        self,
        position: SimulatedPosition,
        market_state: MarketState,
        timestamp: datetime,
    ) -> Decimal:
        """Fallback position valuation when adapter fails.

        This method routes to the appropriate internal marking method
        based on position type. Used when adapter.value_position() fails.

        Args:
            position: The position to value
            market_state: Current market state
            timestamp: Current timestamp

        Returns:
            Position value in USD
        """
        if position.is_lp:
            return self._mark_lp_position(position, market_state, timestamp)
        elif position.is_perp:
            return self._mark_perp_position(position, market_state, timestamp)
        elif position.is_lending:
            return self._mark_lending_position(position, market_state, timestamp)
        else:
            return self._mark_spot_position(position, market_state)

    def _mark_spot_position(self, position: SimulatedPosition, market_state: MarketState) -> Decimal:
        """Mark a spot position to market.

        Args:
            position: The spot position to value
            market_state: Current market state

        Returns:
            Position value in USD
        """
        value = Decimal("0")
        for token, amount in position.amounts.items():
            try:
                price = market_state.get_price(token)
                value += amount * price
            except KeyError:
                # Fall back to entry price if current price unavailable
                value += amount * position.entry_price
        return value

    def _mark_lp_position(
        self,
        position: SimulatedPosition,
        market_state: MarketState,
        timestamp: datetime,
    ) -> Decimal:
        """Mark an LP position to market, calculating IL and accruing fees.

        This method:
        1. Gets current prices for both tokens in the LP pair
        2. Uses ImpermanentLossCalculator to compute current token amounts
        3. Calculates impermanent loss based on price movement
        4. Simulates fee accrual based on position value and time elapsed
        5. Updates position's fees_earned, amounts, and last_updated

        Args:
            position: The LP position to value
            market_state: Current market state
            timestamp: Current timestamp for fee accrual

        Returns:
            Total LP position value in USD (token values + accrued fees)
        """
        # Lazy import to avoid circular dependency
        from almanak.framework.backtesting.pnl.calculators.impermanent_loss import (
            ImpermanentLossCalculator,
        )

        if len(position.tokens) < 2:
            return Decimal("0")

        token0 = position.tokens[0]
        token1 = position.tokens[1]

        # Get current prices
        try:
            token0_price = market_state.get_price(token0)
        except KeyError:
            if self.strict_reproducibility:
                raise ValueError(
                    f"Price unavailable for {token0} in calculate_il and strict_reproducibility=True."
                ) from None
            logger.warning("Price unavailable for %s, falling back to entry_price in calculate_il", token0)
            token0_price = position.entry_price

        # token1 delegates to _stablecoin_fallback which allows $1 for known stablecoins
        # even in strict mode -- token0 has no safe fallback so it raises unconditionally.
        try:
            token1_price = market_state.get_price(token1)
        except KeyError:
            token1_price = self._stablecoin_fallback(token1, "calculate_il")

        # Calculate the price ratio (token0 in terms of token1)
        # This is what Uniswap V3 uses: price = token1/token0
        if token1_price > 0:
            current_price = token0_price / token1_price
        else:
            current_price = position.entry_price

        # Ensure we have tick bounds for V3 calculations
        tick_lower = position.tick_lower if position.tick_lower is not None else -887272
        tick_upper = position.tick_upper if position.tick_upper is not None else 887272

        # Use ImpermanentLossCalculator to get current token amounts
        il_calculator = ImpermanentLossCalculator()

        il_pct, current_token0, current_token1 = il_calculator.calculate_il_v3(
            entry_price=position.entry_price,
            current_price=current_price,
            tick_lower=tick_lower,
            tick_upper=tick_upper,
            liquidity=position.liquidity,
        )

        # Update position's current token amounts
        position.amounts[token0] = current_token0
        position.amounts[token1] = current_token1

        # Calculate current position value (before fees)
        position_value = current_token0 * token0_price + current_token1 * token1_price

        # Simulate fee accrual
        fees_to_add = self._simulate_lp_fee_accrual(position, position_value, timestamp)
        position.fees_earned += fees_to_add

        # Update last_updated timestamp
        position.last_updated = timestamp

        # Total value = token values + accumulated fees
        total_value = position_value + position.fees_earned

        return total_value

    def _simulate_lp_fee_accrual(
        self,
        position: SimulatedPosition,
        position_value_usd: Decimal,
        timestamp: datetime,
    ) -> Decimal:
        """Simulate fee accrual for an LP position.

        This method estimates the fees earned by an LP position based on:
        - The position's fee tier (e.g., 0.3% for Uniswap V3)
        - The position's liquidity share (higher liquidity = more fee capture)
        - Position value
        - Time elapsed since last update
        - Estimated trading volume

        The model uses a volume-based fee calculation:
        1. Estimate daily volume based on position value and multiplier
        2. Calculate fees as: volume * fee_tier * liquidity_share
        3. Liquidity share scales fee capture (concentrated = more share per $)

        Fee Tier APR estimates (rough guidelines):
        - 0.01% tier: ~5-15% APR (stablecoin pairs, high volume)
        - 0.05% tier: ~10-30% APR (blue chip pairs)
        - 0.30% tier: ~15-50% APR (volatile pairs)
        - 1.00% tier: ~5-20% APR (exotic pairs, lower volume)

        This method also updates the position's detailed fee tracking fields:
        - accumulated_fees_usd: Total fees in USD
        - fees_token0: Fees attributed to token0 (50% by default)
        - fees_token1: Fees attributed to token1 (50% by default)

        Args:
            position: The LP position (updated in place with fee tracking)
            position_value_usd: Current position value in USD
            timestamp: Current timestamp

        Returns:
            Fees to add to the position in USD
        """
        if position_value_usd <= 0:
            return Decimal("0")

        # Calculate time elapsed since last update
        if position.last_updated:
            time_elapsed = timestamp - position.last_updated
        else:
            time_elapsed = timestamp - position.entry_time

        # Convert to days
        days_elapsed = Decimal(str(time_elapsed.total_seconds())) / Decimal("86400")

        if days_elapsed <= 0:
            return Decimal("0")

        # Calculate liquidity share factor
        # Higher liquidity positions capture more fees proportionally
        # We use a simple model: liquidity_share = min(1, liquidity / base_liquidity)
        # where base_liquidity is a reference point (e.g., 1M units)
        base_liquidity = Decimal("1000000")
        liquidity_share = min(
            Decimal("1"),
            position.liquidity / base_liquidity if position.liquidity > 0 else Decimal("0.5"),
        )
        # Ensure minimum share of 10% for small positions
        liquidity_share = max(Decimal("0.1"), liquidity_share)

        # Estimate daily trading volume as a multiple of position value
        # Volume multiplier varies by fee tier (lower tiers = higher volume pools)
        fee_tier_pct = position.fee_tier * Decimal("100")  # Convert to percentage

        if fee_tier_pct <= Decimal("0.01"):
            volume_multiplier = Decimal("50")  # Stablecoin pools: 50x daily volume
            base_apr = Decimal("0.10")
        elif fee_tier_pct <= Decimal("0.05"):
            volume_multiplier = Decimal("20")  # Blue chip pairs: 20x daily volume
            base_apr = Decimal("0.20")
        elif fee_tier_pct <= Decimal("0.30"):
            volume_multiplier = Decimal("10")  # Volatile pairs: 10x daily volume
            base_apr = Decimal("0.25")
        else:
            volume_multiplier = Decimal("3")  # Exotic pairs: 3x daily volume
            base_apr = Decimal("0.10")

        # Calculate estimated daily volume
        estimated_daily_volume = position_value_usd * volume_multiplier

        # Calculate fees from volume
        # fees = volume * fee_tier * liquidity_share
        # But we also apply APR-based calculation for comparison
        volume_based_fees = estimated_daily_volume * position.fee_tier * liquidity_share * days_elapsed

        # APR-based calculation (fallback/comparison)
        daily_fee_rate = base_apr / Decimal("365")
        apr_based_fees = position_value_usd * daily_fee_rate * days_elapsed

        # Use the average of both approaches for a balanced estimate
        fees_usd = (volume_based_fees + apr_based_fees) / Decimal("2")

        # Update detailed fee tracking fields on the position
        # Fees are split 50/50 between token0 and token1 (simplified model)
        # In reality, the split depends on which direction trades occur
        position.accumulated_fees_usd += fees_usd

        # Get token prices for fee attribution (use position amounts as proxy)
        token0 = position.tokens[0] if len(position.tokens) > 0 else ""
        token1 = position.tokens[1] if len(position.tokens) > 1 else ""

        # Calculate fee attribution based on position composition
        # If position has both tokens, split fees proportionally
        total_amount0 = position.amounts.get(token0, Decimal("0"))
        total_amount1 = position.amounts.get(token1, Decimal("0"))

        if position_value_usd > 0 and position.entry_price > 0:
            # Estimate token values
            token0_value = total_amount0 * position.entry_price
            token1_value = total_amount1  # Assume token1 is the quote currency

            total_value = token0_value + token1_value
            if total_value > 0:
                # Split fees proportionally based on token composition
                token0_ratio = token0_value / total_value
                token1_ratio = token1_value / total_value
            else:
                # Default to 50/50 split
                token0_ratio = Decimal("0.5")
                token1_ratio = Decimal("0.5")
        else:
            # Default to 50/50 split
            token0_ratio = Decimal("0.5")
            token1_ratio = Decimal("0.5")

        # Convert USD fees to token amounts
        # For token0: fees_token0 = (fees_usd * token0_ratio) / entry_price
        # For token1: fees_token1 = fees_usd * token1_ratio (assuming quote currency)
        if position.entry_price > 0:
            position.fees_token0 += (fees_usd * token0_ratio) / position.entry_price
        position.fees_token1 += fees_usd * token1_ratio

        return fees_usd

    def _mark_perp_position(
        self,
        position: SimulatedPosition,
        market_state: MarketState,
        timestamp: datetime,
    ) -> Decimal:
        """Mark a perpetual position to market, calculating unrealized PnL and funding.

        This method:
        1. Gets current price for the position's token
        2. Calculates unrealized PnL based on price movement
        3. Uses FundingCalculator to calculate funding payments
        4. Updates position's accumulated_funding and cumulative_funding_paid/received
        5. Checks liquidation proximity and emits warnings if needed
        6. Returns total position value (collateral + unrealized PnL + funding)

        The unrealized PnL calculation:
        - PERP_LONG: profits when price goes up, loses when price goes down
        - PERP_SHORT: profits when price goes down, loses when price goes up

        The funding calculation:
        - Uses FundingCalculator with time-based funding rate
        - PERP_LONG: pays funding when market is bullish (funding rate positive)
        - PERP_SHORT: receives funding when market is bullish
        - Updates cumulative_funding_paid and cumulative_funding_received fields

        Liquidation monitoring:
        - Checks if current price is within configurable % of liquidation
        - Emits warning when price approaches liquidation level

        Args:
            position: The perpetual position to value (PERP_LONG or PERP_SHORT)
            market_state: Current market state
            timestamp: Current timestamp for funding accrual

        Returns:
            Total perpetual position value in USD (collateral + unrealized PnL + funding)
        """
        # Lazy imports to avoid circular dependency
        from almanak.framework.backtesting.pnl.calculators.funding import FundingCalculator
        from almanak.framework.backtesting.pnl.calculators.liquidation import (
            LiquidationCalculator,
        )

        token = position.primary_token

        # Get current price
        try:
            current_price = market_state.get_price(token)
        except KeyError:
            # Fall back to entry price if current price unavailable
            current_price = position.entry_price

        # Calculate unrealized PnL based on price movement
        # Price change as a ratio of entry price
        price_change_pct = (current_price - position.entry_price) / position.entry_price

        if position.position_type == PositionType.PERP_LONG:
            # Long profits when price goes up
            unrealized_pnl = price_change_pct * position.notional_usd
        else:
            # Short profits when price goes down
            unrealized_pnl = -price_change_pct * position.notional_usd

        # Calculate funding payments using FundingCalculator
        funding_calculator = FundingCalculator()

        # Calculate time elapsed since last update (or entry if first time)
        if position.last_updated:
            time_elapsed = timestamp - position.last_updated
        else:
            time_elapsed = timestamp - position.entry_time

        hours_elapsed = Decimal(str(time_elapsed.total_seconds())) / Decimal("3600")

        # Only apply funding if time has elapsed
        if hours_elapsed > Decimal("0"):
            # Get protocol-specific funding rate
            funding_rate = funding_calculator.get_funding_rate_for_protocol(position.protocol)

            # Calculate funding payment for this period
            try:
                funding_result = funding_calculator.calculate_funding_payment(
                    position=position,
                    funding_rate=funding_rate,
                    time_delta_hours=hours_elapsed,
                )

                # Apply funding to position - updates accumulated_funding and cumulative fields
                funding_calculator.apply_funding_to_position(position, funding_result)

            except ValueError:
                # If position type is invalid, no funding
                pass

        # Update last_updated timestamp
        position.last_updated = timestamp

        # Check liquidation proximity and emit warning if within threshold
        liq_calculator = LiquidationCalculator()

        # Update liquidation price if not set
        if position.liquidation_price is None:
            liq_calculator.update_position_liquidation_price(position)

        # Check if current price is near liquidation and emit warning
        liq_calculator.check_liquidation_proximity(
            position=position,
            current_price=current_price,
            emit_warning=True,
        )

        # Total position value = collateral + unrealized PnL + accumulated funding
        # Note: accumulated_funding is already signed correctly
        # (negative if long pays, positive if short receives)
        total_value = position.collateral_usd + unrealized_pnl + position.accumulated_funding

        # Ensure position value doesn't go below zero (liquidation would occur)
        # For simulation purposes, we still return the calculated value
        # The engine should handle liquidation logic separately
        return total_value

    def _mark_lending_position(
        self,
        position: SimulatedPosition,
        market_state: MarketState,
        timestamp: datetime,
    ) -> Decimal:
        """Mark a lending position to market, calculating interest accrual.

        This method:
        1. Gets current price for the position's token
        2. Uses InterestCalculator to calculate interest accrued
        3. Updates position's interest_accrued field
        4. Returns total position value

        For SUPPLY positions:
        - Earns interest over time
        - Returns positive value (principal + interest earned)

        For BORROW positions:
        - Pays interest over time
        - Returns negative value (debt = principal + interest owed)

        The interest calculation uses compound interest via InterestCalculator:
        - Supports protocol-specific APYs
        - Uses daily compounding by default
        - Formula: interest = principal * ((1 + apy/365)^days - 1)

        Args:
            position: The lending position to value (SUPPLY or BORROW)
            market_state: Current market state
            timestamp: Current timestamp for interest accrual

        Returns:
            Position value in USD:
            - Positive for SUPPLY (asset value + interest earned)
            - Negative for BORROW (debt = principal + interest owed)
        """
        # Lazy import to avoid circular dependency
        from almanak.framework.backtesting.pnl.calculators.interest import InterestCalculator

        token = position.primary_token

        # Get current price
        try:
            current_price = market_state.get_price(token)
        except KeyError:
            # Fall back to entry price if current price unavailable
            current_price = position.entry_price

        # Calculate principal value at current price
        principal_amount = position.total_amount
        principal_value_usd = principal_amount * current_price

        # Calculate time elapsed since position entry or last update
        if position.last_updated:
            time_elapsed = timestamp - position.last_updated
        else:
            time_elapsed = timestamp - position.entry_time

        # Convert to days for annual rate calculation
        days_elapsed = Decimal(str(time_elapsed.total_seconds())) / Decimal("86400")

        # Calculate interest for this period using InterestCalculator
        if days_elapsed > Decimal("0"):
            interest_calculator = InterestCalculator()

            # Determine the APY to use
            # First, try to use position's APY at entry
            # If not set or zero, fall back to protocol-specific APY
            if position.apy_at_entry > Decimal("0"):
                apy = position.apy_at_entry
            else:
                # Get protocol-specific APY based on position type
                if position.position_type == PositionType.SUPPLY:
                    apy = interest_calculator.get_supply_apy_for_protocol(position.protocol)
                else:
                    apy = interest_calculator.get_borrow_apy_for_protocol(position.protocol)

            # Calculate interest on the principal value at entry
            # Use entry price value to be consistent with lending protocol behavior
            principal_at_entry = principal_amount * position.entry_price

            # Calculate interest using compound interest (default behavior)
            result = interest_calculator.calculate_interest(
                principal=principal_at_entry,
                apy=apy,
                time_delta=days_elapsed,
                compound=True,
            )

            # Add to accumulated interest
            position.interest_accrued += result.interest

        # Update last_updated timestamp
        position.last_updated = timestamp

        if position.position_type == PositionType.SUPPLY:
            # Supply position: asset value + interest earned (positive)
            # Interest is in USD, add directly
            return principal_value_usd + position.interest_accrued
        else:
            # Borrow position: debt = principal + interest owed (negative)
            # Returns negative since this is a liability
            return -(principal_value_usd + position.interest_accrued)

    def _update_health_factors(self, market_state: MarketState) -> None:
        """Update health factors for all borrow positions. Delegates to liquidation_simulator module."""
        from .liquidation_simulator import update_health_factors

        update_health_factors(self, market_state)

    def _simulate_lending_liquidation(
        self,
        borrow_position: SimulatedPosition,
        health_factor: Decimal,
        total_collateral_usd: Decimal,
        debt_value_usd: Decimal,
        market_state: MarketState,
    ) -> None:
        """Simulate a lending liquidation event. Delegates to liquidation_simulator module."""
        from .liquidation_simulator import simulate_lending_liquidation

        simulate_lending_liquidation(
            self, borrow_position, health_factor, total_collateral_usd, debt_value_usd, market_state
        )

    def record_equity_point(self, timestamp: datetime, market_state: MarketState) -> None:
        """Record current portfolio value to equity curve.

        Args:
            timestamp: Time of the snapshot
            market_state: Current market state for valuation
        """
        value = self.get_total_value_usd(market_state)
        self.equity_curve.append(EquityPoint(timestamp=timestamp, value_usd=value))

    def get_position(self, position_id: str) -> SimulatedPosition | None:
        """Get a position by its ID.

        Args:
            position_id: ID of the position to find

        Returns:
            The position if found, None otherwise
        """
        for pos in self.positions:
            if pos.position_id == position_id:
                return pos
        return None

    def get_positions_by_type(self, position_type: PositionType) -> list[SimulatedPosition]:
        """Get all positions of a specific type.

        Args:
            position_type: Type of positions to retrieve

        Returns:
            List of matching positions
        """
        return [p for p in self.positions if p.position_type == position_type]

    def get_token_balance(self, token: str) -> Decimal:
        """Get balance of a specific token.

        Args:
            token: Token symbol

        Returns:
            Amount held, or 0 if not held
        """
        return self.tokens.get(token.upper(), Decimal("0"))

    def get_lending_liquidations(self) -> list[LendingLiquidationEvent]:
        """Get all lending liquidation events that occurred during the backtest.

        Returns:
            List of LendingLiquidationEvent instances
        """
        return self._lending_liquidations.copy()

    def get_perp_liquidations(self) -> list[LiquidationEvent]:
        """Get all perpetual position liquidation events that occurred during the backtest.

        Returns:
            List of LiquidationEvent instances
        """
        return self._perp_liquidations.copy()

    def add_perp_liquidation(self, event: LiquidationEvent) -> None:
        """Record a perpetual position liquidation event.

        Args:
            event: The liquidation event to record
        """
        self._perp_liquidations.append(event)

    def to_dict(self) -> dict[str, Any]:
        """Serialize to dictionary."""
        return {
            "initial_capital_usd": str(self.initial_capital_usd),
            "cash_usd": str(self.cash_usd),
            "tokens": {k: str(v) for k, v in self.tokens.items()},
            "positions": [p.to_dict() for p in self.positions],
            "equity_curve": [e.to_dict() for e in self.equity_curve],
            "trades": [t.to_dict() for t in self.trades],
            "closed_positions": [p.to_dict() for p in self._closed_positions],
            "initial_margin_ratio": str(self.initial_margin_ratio),
            "maintenance_margin_ratio": str(self.maintenance_margin_ratio),
            "max_margin_utilization": str(self._max_margin_utilization),
            "health_factor_warning_threshold": str(self.health_factor_warning_threshold),
            "min_health_factor": str(self._min_health_factor),
            "health_factor_warnings": self._health_factor_warnings,
            "liquidation_penalty": str(self.liquidation_penalty),
            "lending_liquidations": [ll.to_dict() for ll in self._lending_liquidations],
            "perp_liquidations": [pl.to_dict() for pl in self._perp_liquidations],
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "SimulatedPortfolio":
        """Deserialize from dictionary.

        Args:
            data: Dictionary with serialized SimulatedPortfolio data

        Returns:
            SimulatedPortfolio instance
        """
        portfolio = cls(
            initial_capital_usd=Decimal(data.get("initial_capital_usd", "10000")),
            cash_usd=Decimal(data.get("cash_usd", "0")),
            tokens={k: Decimal(v) for k, v in data.get("tokens", {}).items()},
            positions=[SimulatedPosition.from_dict(p) for p in data.get("positions", [])],
            equity_curve=[
                EquityPoint(
                    timestamp=datetime.fromisoformat(e["timestamp"]),
                    value_usd=Decimal(e["value_usd"]),
                )
                for e in data.get("equity_curve", [])
            ],
            trades=[
                TradeRecord(
                    timestamp=datetime.fromisoformat(t["timestamp"]),
                    intent_type=IntentType(t["intent_type"]),
                    executed_price=Decimal(t["executed_price"]),
                    fee_usd=Decimal(t["fee_usd"]),
                    slippage_usd=Decimal(t["slippage_usd"]),
                    gas_cost_usd=Decimal(t["gas_cost_usd"]),
                    pnl_usd=Decimal(t["pnl_usd"]),
                    success=t["success"],
                    amount_usd=Decimal(t.get("amount_usd", "0")),
                    protocol=t.get("protocol", ""),
                    tokens=t.get("tokens", []),
                )
                for t in data.get("trades", [])
            ],
            _closed_positions=[SimulatedPosition.from_dict(p) for p in data.get("closed_positions", [])],
            initial_margin_ratio=Decimal(data.get("initial_margin_ratio", "0.1")),
            maintenance_margin_ratio=Decimal(data.get("maintenance_margin_ratio", "0.05")),
            health_factor_warning_threshold=Decimal(data.get("health_factor_warning_threshold", "1.2")),
            liquidation_penalty=Decimal(data.get("liquidation_penalty", "0.05")),
        )
        # Set tracking fields from data
        portfolio._max_margin_utilization = Decimal(data.get("max_margin_utilization", "0"))
        portfolio._min_health_factor = Decimal(data.get("min_health_factor", "999"))
        portfolio._health_factor_warnings = data.get("health_factor_warnings", 0)
        # Deserialize lending liquidations
        portfolio._lending_liquidations = [
            LendingLiquidationEvent.from_dict(ll) for ll in data.get("lending_liquidations", [])
        ]
        # Deserialize perp liquidations
        portfolio._perp_liquidations = [LiquidationEvent.from_dict(pl) for pl in data.get("perp_liquidations", [])]
        return portfolio


__all__ = [
    "PositionType",
    "SimulatedPosition",
    "SimulatedFill",
    "SimulatedPortfolio",
]
