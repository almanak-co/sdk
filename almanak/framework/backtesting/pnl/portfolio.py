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
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Any

from almanak.core.chains import DEFAULT_CHAIN
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
    PricePoint,
    SlippageMetrics,
    TradeRecord,
)
from almanak.framework.backtesting.paper.token_registry import (
    is_token_known,
    resolve_to_canonical_symbol,
)
from almanak.framework.backtesting.pnl.data_provider import (
    MarketState,
    TokenKey,
    TokenRef,
    is_address_like,
    is_token_key,
    normalize_token_key,
    normalize_token_ref,
    token_ref_display,
)
from almanak.framework.backtesting.pnl.money import TokenIdentity

# Position models extracted to position_models.py for module size management
from almanak.framework.backtesting.pnl.position_models import (  # noqa: F401
    PositionType,
    SimulatedFill,
    SimulatedPosition,
)
from almanak.framework.data.tokens import TokenResolutionError, get_token_resolver

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
CASH_EQUIVALENT_STABLECOIN_SYMBOLS: frozenset[str] = frozenset({"USDC", "USDT", "DAI"})
CASH_EQUIVALENT_STABLECOINS: frozenset[str] = CASH_EQUIVALENT_STABLECOIN_SYMBOLS


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


@dataclass(frozen=True)
class PositionMetricsAggregate:
    """Position-derived metric aggregates over a portfolio's open + closed positions.

    These fields come from the simulated positions themselves (LP fee accrual,
    perp funding, lending interest) and the portfolio's running risk trackers
    (health factor, margin utilization, realized/unrealized PnL, and the perp +
    lending liquidation ledgers) -- NOT from the equity curve or the trade
    records. Both code paths that assemble a
    :class:`~almanak.framework.backtesting.models.BacktestMetrics` --
    :meth:`SimulatedPortfolio.get_metrics` (the portfolio-native path) and
    ``metrics_calculator.calculate_metrics`` (the path the PnL engine *result*
    actually uses, ``_engine_helpers.finalize_backtest_result``) -- now source
    this block from :meth:`SimulatedPortfolio.aggregate_position_metrics`, so the
    two can never drift.

    They DID drift (VIB-5079 v1.1 reporting): ``calculate_metrics`` never
    populated any of these, so every engine-result LP backtest reported
    ``total_fees_earned_usd=0`` / ``fees_by_pool={}`` even when fees demonstrably
    accrued and were credited into equity at close -- a reporting/KPI bug, not a
    value bug (conservation stayed exact). The same omission silently zeroed perp
    funding, lending interest, health/margin extrema, and realized/unrealized PnL
    in the engine result. Centralizing the aggregation here fixes all of them in
    lockstep and removes the duplicated loop that allowed the drift.

    ``liquidations_count`` / ``liquidation_losses_usd`` were a step worse: never
    populated by *either* path (only ever round-tripped through ``from_dict``),
    so every result reported zero liquidations regardless of what happened. They
    are aggregated here too -- perp loss is the explicit ``LiquidationEvent.loss_usd``;
    lending loss is ``collateral_seized - debt_repaid`` (the liquidation-penalty
    bonus the liquidator keeps, since the simulator sets
    ``collateral_seized = debt_repaid * (1 + penalty)``).
    """

    total_fees_earned_usd: Decimal
    fees_by_pool: dict[str, Decimal]
    lp_fee_confidence_breakdown: dict[str, int]
    total_funding_paid: Decimal
    total_funding_received: Decimal
    total_interest_earned: Decimal
    total_interest_paid: Decimal
    max_margin_utilization: Decimal
    min_health_factor: Decimal
    health_factor_warnings: int
    realized_pnl: Decimal
    unrealized_pnl: Decimal
    liquidations_count: int
    liquidation_losses_usd: Decimal


@dataclass(frozen=True)
class _DebitPlan:
    token_debits: dict[TokenRef, Decimal]
    cash_debit: Decimal
    conversions: dict[TokenRef, Decimal]


@dataclass(frozen=True)
class _PositionEffects:
    closed_position: SimulatedPosition | None = None


@dataclass(frozen=True)
class _TradePnlComponents:
    pnl_usd: Decimal | None
    il_loss_usd: Decimal | None = None
    fees_earned_usd: Decimal | None = None
    net_lp_pnl_usd: Decimal | None = None


@dataclass(frozen=True)
class _ReductionFlow:
    amounts: dict[TokenRef, Decimal]
    label: str
    verb: str


@dataclass(frozen=True)
class _LpFeeTierModel:
    volume_multiplier: Decimal
    base_apr: Decimal


@dataclass(frozen=True)
class _EquitySummary:
    total_pnl: Decimal
    total_return: Decimal
    annualized_return: Decimal


@dataclass(frozen=True)
class _ExecutionCostTotals:
    fees: Decimal
    slippage: Decimal
    gas: Decimal


@dataclass(frozen=True)
class _RiskMetrics:
    volatility: Decimal
    sharpe: Decimal
    sortino: Decimal
    max_drawdown: Decimal
    calmar: Decimal


@dataclass
class SimulatedPortfolio:
    """Portfolio tracker for PnL backtesting.

    Manages simulated positions, cash balances, and tracks portfolio value
    over time for backtest analysis.

    Attributes:
        cash_usd: Available cash in USD
        tokens: Dict of token identity -> amount held (spot holdings)
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
    chain: str = DEFAULT_CHAIN
    tokens: dict[TokenRef, Decimal] = field(default_factory=dict)
    positions: list[SimulatedPosition] = field(default_factory=list)
    equity_curve: list[EquityPoint] = field(default_factory=list)
    trades: list[TradeRecord] = field(default_factory=list)
    #: Per-tick token-price snapshots, one per equity point (captured by
    #: ``mark_to_market`` from the tick's ``MarketState.prices``). Purely a
    #: reporting/visualization export — never read back by the simulation.
    price_series: list[PricePoint] = field(default_factory=list)
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
    #: Weighted-average USD cost basis per unit of each spot token currently
    #: held (VIB-5083). A SWAP that acquires a token raises its average cost;
    #: a SWAP that disposes of a token realizes proceeds - units x avg_cost.
    #: Keyed by the same token identities as ``tokens`` (cash-equivalent stablecoins are
    #: never tracked here -- they live in ``cash_usd`` at $1 by definition).
    _cost_basis: dict[TokenRef, Decimal] = field(default_factory=dict)
    #: UPPERCASE symbol of the strategy's declared numeraire token (VIB-5127),
    #: or ``None`` for the USD default. Set by the engine at boot
    #: (``initialize_backtest``). When set, ``mark_to_market`` captures the
    #: numeraire token's USD price onto each equity point for the reporting
    #: projection; it never affects ``value_usd`` or the conservation core.
    _numeraire_symbol: str | None = field(default=None)
    #: Address-native token identity for the declared numeraire, when known.
    #: Non-USD provider states may be keyed by ``(chain, address)`` rather than
    #: by symbol; this key is the authoritative lookup path for those runs.
    _numeraire_token: TokenRef | None = field(default=None)

    #: Operational gas ledger: gas is EOA-paid, never strategy capital.
    #: ``None`` budget = unlimited (metered, never binds).
    gas_tank_budget_usd: Decimal | None = field(default=None)
    gas_tank_spent_usd: Decimal = field(default=Decimal("0"))

    _STABLECOIN_SYMBOLS: frozenset[str] = STABLECOINS
    _cash_equivalent_token_keys: frozenset[TokenKey] = field(default_factory=frozenset, init=False)
    # symbol (upper) -> typed identity; all balance-key decisions resolve
    # through _resolve_key against this one table (ALM-2960).
    _identity_table: dict[str, TokenIdentity] = field(default_factory=dict, init=False)

    def __post_init__(self) -> None:
        """Initialize cash from initial capital if not set."""
        self.chain = str(self.chain).lower()
        self._cash_equivalent_token_keys = self._resolve_cash_equivalent_token_keys(self.chain)
        self.tokens = self._normalize_amounts(self.tokens)
        self._cost_basis = self._normalize_amounts(self._cost_basis)
        if self._numeraire_token is not None:
            self._numeraire_token = normalize_token_ref(self._numeraire_token, self.chain)
        for position in [*self.positions, *self._closed_positions]:
            self._normalize_position_token_refs(position)
        has_existing_state = bool(
            self.tokens or self.positions or self._closed_positions or self.trades or self.equity_curve
        )
        if self.cash_usd == Decimal("0") and self.initial_capital_usd > 0 and not has_existing_state:
            self.cash_usd = self.initial_capital_usd

    @staticmethod
    def _resolve_cash_equivalent_token_keys(chain: str) -> frozenset[TokenKey]:
        keys: set[TokenKey] = set()
        resolver = get_token_resolver()
        for symbol in CASH_EQUIVALENT_STABLECOIN_SYMBOLS:
            try:
                resolved = resolver.resolve(symbol, chain, log_errors=False, skip_gateway=True)
            except TokenResolutionError:
                continue
            if resolved and resolved.address:
                keys.add(normalize_token_key(chain, resolved.address))
        return frozenset(keys)

    def _normalize_token_ref(self, token: TokenRef) -> TokenRef:
        return normalize_token_ref(token, self.chain)

    def _normalize_amounts(self, amounts: dict[TokenRef, Decimal]) -> dict[TokenRef, Decimal]:
        normalized: dict[TokenRef, Decimal] = {}
        for token, amount in amounts.items():
            key = self._normalize_token_ref(token)
            normalized[key] = normalized.get(key, Decimal("0")) + amount
        return normalized

    def _normalize_position_token_refs(self, position: SimulatedPosition) -> None:
        position.tokens = [self._normalize_token_ref(token) for token in position.tokens]
        position.amounts = self._normalize_amounts(position.amounts)

    def _normalize_fill_token_refs(self, fill: SimulatedFill) -> None:
        fill.tokens = [self._normalize_token_ref(token) for token in fill.tokens]
        fill.tokens_in = self._normalize_amounts(fill.tokens_in)
        fill.tokens_out = self._normalize_amounts(fill.tokens_out)
        fill.position_reduce_amounts = self._normalize_amounts(fill.position_reduce_amounts)
        if fill.position_delta is not None:
            self._normalize_position_token_refs(fill.position_delta)

    def _stablecoin_fallback(self, token: TokenRef, context: str) -> Decimal:
        """Return $1 fallback for token, raising in strict mode for non-stablecoins."""
        if (
            self.strict_reproducibility
            and not self._is_cash_equivalent(token)
            and not (isinstance(token, str) and token.upper() in self._STABLECOIN_SYMBOLS)
        ):
            raise ValueError(
                f"Price unavailable for non-stablecoin {token} in {context} and strict_reproducibility=True. "
                "Cannot assume $1 price."
            )
        logger.warning("Price unavailable for %s, falling back to $1 stablecoin assumption in %s", token, context)
        return Decimal("1")

    def apply_fill(
        self,
        fill: SimulatedFill,
        market_state: MarketState | None = None,
        adapter: "StrategyBacktestAdapter | None" = None,
    ) -> bool:
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
            adapter: Optional strategy-specific adapter (same object
                ``mark_to_market`` receives). Used only to accrue lending
                interest through the fill instant before a partial position
                reduction, with the adapter lane's own rate sources.

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
            return self._reject_fill(
                fill,
                fill.metadata.get("failure_reason", "fill marked failed by producer"),
            )

        self._normalize_fill_token_refs(fill)
        debit_plan, failure_reason = self._build_debit_plan(fill, market_state)
        if failure_reason is not None:
            return self._reject_fill(fill, failure_reason)
        assert debit_plan is not None

        self._annotate_implicit_conversions(fill, debit_plan.conversions)
        self._apply_token_flows(fill, debit_plan.token_debits, debit_plan.cash_debit, market_state)
        position_effects = self._apply_position_effects(fill, market_state, adapter)
        components = self._trade_pnl_components(fill, position_effects.closed_position)
        self._record_successful_fill(fill, components)

        return True

    def _reject_fill(self, fill: SimulatedFill, reason: str) -> bool:
        self._record_failed_fill(fill, reason)
        return False

    def _build_debit_plan(
        self,
        fill: SimulatedFill,
        market_state: MarketState | None,
    ) -> tuple[_DebitPlan | None, str | None]:
        token_debits, cash_debit, conversions, failure_reason = self._plan_token_debits(
            fill.tokens_out, fill.intent_type, market_state
        )
        if failure_reason is not None:
            return None, failure_reason

        # Aggregate cash check: planned stablecoin/conversion debits and
        # perp-open collateral draw from the same cash_usd, so they must be
        # validated as one sum (each passing individually could still
        # overdraw) before any state mutation.
        funding_failure = self._cash_funding_failure(fill, cash_debit, token_debits)
        if funding_failure is not None:
            return None, funding_failure

        gas_failure = self._gas_exhaustion_failure(fill)
        if gas_failure is not None:
            return None, gas_failure

        reduce_failure = self._position_reduce_failure(fill)
        if reduce_failure is not None:
            return None, reduce_failure

        close_failure = self._position_close_failure(fill)
        if close_failure is not None:
            return None, close_failure

        return _DebitPlan(token_debits, cash_debit, conversions), None

    @staticmethod
    def _annotate_implicit_conversions(fill: SimulatedFill, conversions: dict[TokenRef, Decimal]) -> None:
        if conversions:
            fill.metadata["implicit_conversions"] = {
                token_ref_display(token): str(amount) for token, amount in conversions.items()
            }

    def _apply_position_effects(
        self,
        fill: SimulatedFill,
        market_state: MarketState | None,
        adapter: "StrategyBacktestAdapter | None",
    ) -> _PositionEffects:
        closed_position: SimulatedPosition | None = None

        if fill.position_close_id:
            closed_position = self._close_position(fill.position_close_id, fill.timestamp)
        elif fill.position_reduce_id:
            # Partially reduce a position (validated in _position_reduce_failure).
            # Accrue interest through the fill instant FIRST: pending intents
            # execute before the per-tick adapter update / mark, so without
            # this the interval since the previous mark would accrue on the
            # post-reduction principal -- under-accruing the interest the
            # withdrawn slice earned before the withdraw.
            self._accrue_interest_through_fill(fill, market_state, adapter)
            self._reduce_position(fill.position_reduce_id, fill.position_reduce_amounts)
            # Boundary repay/withdraw: the flow covered principal + part of the
            # accrued interest. The principal left via _reduce_position; realize
            # the covered interest now -- decrement it off the position so it
            # stops counting in equity, exactly as a full close would
            # (VIB-5098). An ordinary sub-principal partial carries no
            # interest_usd and this is a no-op.
            self._realize_reduce_interest(fill)

        if fill.position_delta:
            # Open a new position. Perp collateral moves from cash into the
            # position (which every valuation path prices as collateral +
            # unrealized PnL + funding); without this debit the open would
            # mint the collateral amount.
            if fill.position_delta.is_perp and fill.success:
                self._debit_cash_like(fill.position_delta.collateral_usd)
            self.positions.append(fill.position_delta)

        return _PositionEffects(closed_position=closed_position)

    def _trade_pnl_components(
        self,
        fill: SimulatedFill,
        closed_position: SimulatedPosition | None,
    ) -> _TradePnlComponents:
        # ``None`` means "no realized PnL yet" (an opening /
        # inventory-building trade) -- distinct from a measured zero.
        pnl_usd = self._calculate_trade_pnl(fill)
        il_loss_usd: Decimal | None = None
        fees_earned_usd: Decimal | None = None
        net_lp_pnl_usd: Decimal | None = None

        if fill.intent_type == IntentType.LP_CLOSE and closed_position and closed_position.is_lp:
            il_loss_usd, fees_earned_usd, net_lp_pnl_usd = self._calculate_lp_pnl_breakdown(closed_position, fill)
            if (pnl_usd is None or pnl_usd == Decimal("0")) and net_lp_pnl_usd is not None:
                pnl_usd = net_lp_pnl_usd

        # Perp close: return collateral + realized PnL to cash, so the
        # close is value-neutral at the close instant.
        if closed_position is not None and closed_position.is_perp and fill.success:
            pnl_usd = self._apply_perp_close_credit(closed_position, fill, pnl_usd or Decimal("0"))

        return _TradePnlComponents(pnl_usd, il_loss_usd, fees_earned_usd, net_lp_pnl_usd)

    def _record_successful_fill(self, fill: SimulatedFill, components: _TradePnlComponents) -> None:
        trade = fill.to_trade_record(
            pnl_usd=components.pnl_usd,
            il_loss_usd=components.il_loss_usd,
            fees_earned_usd=components.fees_earned_usd,
            net_lp_pnl_usd=components.net_lp_pnl_usd,
        )
        self.trades.append(trade)
        self._accumulate_realized_pnl(fill, components.pnl_usd)

    def _accumulate_realized_pnl(self, fill: SimulatedFill, pnl_usd: Decimal | None) -> None:
        # Position closes lock in their PnL here (guarded by position_close_id).
        # SWAP disposals carry realized PnL via metadata["realized_pnl_usd"].
        realizes_pnl = bool(fill.position_close_id) or fill.intent_type == IntentType.SWAP
        if realizes_pnl and pnl_usd is not None and pnl_usd != Decimal("0"):
            self._realized_pnl += pnl_usd

    def _apply_token_flows(
        self,
        fill: SimulatedFill,
        token_debits: dict[TokenRef, Decimal],
        cash_debit: Decimal,
        market_state: MarketState | None = None,
    ) -> None:
        """Commit the planned token debits, credits, stablecoin sweep, gas,
        and non-embedded venue costs.

        Runs only after ``_plan_token_debits`` validated affordability, so
        every debit is covered by the held balance.

        For SWAP fills this also maintains the per-token average cost basis
        (VIB-5083) and stashes the realized PnL of the disposed leg under
        ``metadata["realized_pnl_usd"]`` BEFORE the basis is mutated, so
        :meth:`_calculate_trade_pnl` can attribute the gain/loss to the
        closing trade.
        """
        # SWAP cost-basis accounting must run against the PRE-disposal basis,
        # so it happens before the balance mutations below consume tokens_out.
        if fill.intent_type == IntentType.SWAP:
            self._record_swap_cost_basis(fill, token_debits, market_state)

        # Update token balances - subtract tokens_out
        for token, debit in token_debits.items():
            new_amount = self.tokens.get(token, Decimal("0")) - debit
            if new_amount <= Decimal("0"):
                self.tokens.pop(token, None)
                self._cost_basis.pop(token, None)
            else:
                self.tokens[token] = new_amount
        self._debit_cash_like(cash_debit)

        # Update token balances - add tokens_in. Credits resolve to the SAME
        # key identity debits use: a close crediting plain "WETH" beside an
        # address-keyed funding plane split-brains the portfolio — the
        # balance seeding then judges the address form unheld and zero-seeds
        # over the real balance, freezing re-entries (found via the CLMM
        # 6-month run: WETH read 0 forever after the first close).
        for token, amount in fill.tokens_in.items():
            credit_token = self._resolve_key(token)
            current = self.tokens.get(credit_token, Decimal("0"))
            self.tokens[credit_token] = current + amount

        # Handle cash-equivalent stablecoins as cash.
        for token in list(self.tokens):
            if self._is_cash_equivalent(token):
                self.cash_usd += self.tokens.pop(token)
                # Swept to cash at $1: no spot basis to carry.
                self._cost_basis.pop(token, None)

        # Gas draws from the operational tank; venue costs (fee/slippage)
        # are strategy capital and debit cash-like assets.
        self._draw_gas_tank(fill.gas_cost_usd)
        self._debit_cash_like(self._venue_cash_costs(fill))

    def _swap_disposed_tokens(
        self,
        token_debits: dict[TokenRef, Decimal],
        proceeds_unpriceable: bool,
    ) -> dict[TokenRef, Decimal]:
        if proceeds_unpriceable:
            return {}
        return {
            token: debit
            for token, debit in token_debits.items()
            if debit > Decimal("0") and not self._is_cash_equivalent(token)
        }

    def _swap_realized_pnl(
        self,
        disposed: dict[TokenRef, Decimal],
        in_value: Decimal,
        market_state: MarketState | None,
    ) -> Decimal | None:
        disposed_units_value = self._leg_usd_value(disposed, market_state)
        disposed_prices = {token: self._token_price(token, market_state) for token in disposed}
        use_even_split = disposed_units_value <= Decimal("0") or any(
            price is None for price in disposed_prices.values()
        )

        realized = Decimal("0")
        realized_any = False
        for token, debit in disposed.items():
            avg_cost = self._cost_basis.get(token)
            if avg_cost is None:
                continue
            if use_even_split:
                # No single priced basis to weight by: split proceeds evenly so
                # a single-leg sale still nets proceeds - cost exactly.
                proceeds = in_value / Decimal(str(len(disposed)))
            else:
                unit_value = disposed_prices[token]
                assert unit_value is not None  # use_even_split is False -> all priced
                proceeds = in_value * (debit * unit_value / disposed_units_value)
            realized += proceeds - debit * avg_cost
            realized_any = True
        return realized if realized_any else None

    def _record_acquired_swap_basis(
        self,
        fill: SimulatedFill,
        out_value: Decimal,
        market_state: MarketState | None,
    ) -> None:
        # Basis keys must match the balance-credit keys (ALM-2960): the sell
        # side debits and realizes by the held identity, so a basis recorded
        # under the raw fill key would never be found again. Pricing still
        # tries the fill's own key form first — a symbol the market quotes
        # directly must not degrade to the even-split fallback.
        acquired: dict[TokenRef, Decimal] = {}
        price_lookup: dict[TokenRef, TokenRef] = {}
        for token, amount in fill.tokens_in.items():
            if amount <= Decimal("0") or self._is_cash_equivalent(token):
                continue
            key = self._resolve_key(token)
            acquired[key] = acquired.get(key, Decimal("0")) + amount
            price_lookup.setdefault(key, token)
        # _token_price returns None (never 0) when unavailable, so `or`
        # short-circuits safely and each lookup evaluates once (review, #3310).
        acquired_prices = {
            token: self._token_price(token, market_state) or self._token_price(price_lookup[token], market_state)
            for token in acquired
        }
        use_even_split = any(price is None for price in acquired_prices.values())
        acquired_units_value = Decimal("0")
        if not use_even_split:
            for token, amount in acquired.items():
                unit_value = acquired_prices[token]
                assert unit_value is not None
                acquired_units_value += amount * unit_value
            use_even_split = acquired_units_value <= Decimal("0")

        for token, amount in acquired.items():
            if out_value <= Decimal("0"):
                # The paid leg could not be priced: seeding a zero cost basis
                # would make a later sale realize the full proceeds as a
                # fabricated gain. Leave the basis unset (unknown) instead
                # (VIB-5083, CodeRabbit).
                continue
            if use_even_split:
                # No price to anchor a basis: fall back to the leg's own
                # notional per unit so a later sale still nets to zero rather
                # than fabricating a gain.
                share = out_value / Decimal(str(len(acquired)))
            else:
                unit_value = acquired_prices[token]
                assert unit_value is not None
                share = out_value * (amount * unit_value / acquired_units_value)
            self._add_to_cost_basis(token, amount, share)

    def _record_swap_cost_basis(
        self,
        fill: SimulatedFill,
        token_debits: dict[TokenRef, Decimal],
        market_state: MarketState | None,
    ) -> None:
        """Update average cost basis for a SWAP and stash its realized PnL.

        Average-cost model (acceptable for v1 -- blueprint 31 section 4):

        - A token leaving the portfolio (in ``token_debits``, the covered
          ``tokens_out``) REALIZES PnL: ``proceeds - units x avg_cost``,
          where proceeds is the USD value received on the inflow leg. The
          token's stored average cost is unchanged by a partial sale.
        - A token entering the portfolio (non-cash ``tokens_in``) raises its
          weighted-average cost by the USD value paid on the outflow leg.

        The realized PnL of the disposed leg is written to
        ``metadata["realized_pnl_usd"]`` so :meth:`_calculate_trade_pnl`
        attributes it. A swap that only ACQUIRES inventory (a cash->token
        buy, no tracked token disposed) writes nothing: its PnL is unknown,
        not zero (Empty != Zero), and :meth:`_calculate_trade_pnl` returns
        ``None`` for it.

        Cash-equivalent stablecoins are never tracked: they are held at $1 in
        ``cash_usd``, so a swap leg in/out of them is pure value transfer with
        no cost-basis effect of its own.
        """
        out_value = self._leg_usd_value(fill.tokens_out, market_state)
        in_value = self._leg_usd_value(fill.tokens_in, market_state)

        # Proceeds are unpriceable when the inflow leg has tokens to receive
        # but none of them could be valued (in_value == 0 with a non-empty
        # inflow). Booking ``proceeds - cost`` in that case fabricates a loss
        # equal to the whole disposed cost -- not a measured outcome but
        # missing data (Empty != Zero, blueprint 31 section 7). Leave
        # ``realized_pnl_usd`` UNSET so _calculate_trade_pnl returns None and
        # the trade is excluded from win/loss stats (VIB-5083, CodeRabbit).
        inflow_has_tokens = any(amount > Decimal("0") for amount in fill.tokens_in.values())
        proceeds_unpriceable = inflow_has_tokens and in_value <= Decimal("0")

        # Disposed legs realize PnL against their average cost. Proceeds
        # (``in_value``) are allocated pro-rata across the disposed tracked
        # tokens by market value -- symmetric with the acquired-leg split
        # below -- so a multi-token-out swap does not count the full proceeds
        # once per disposed leg.
        disposed = self._swap_disposed_tokens(token_debits, proceeds_unpriceable)
        # Pick ONE allocation mode for the whole disposed set so the per-leg
        # shares always sum to in_value. Mixing modes (pro-rata for priced
        # legs, even-split for unpriced ones) let priced tokens claim the full
        # in_value by value while unpriced tokens also drew an even-split share,
        # over-allocating proceeds (CodeRabbit PR #2805). Even-split when any
        # disposed leg lacks a price or the set has no positive value;
        # otherwise pro-rata by market value.
        realized = self._swap_realized_pnl(disposed, in_value, market_state)

        # Acquired non-cash legs raise their weighted-average cost by the USD
        # paid on the outflow leg (split pro-rata when multiple are acquired).
        self._record_acquired_swap_basis(fill, out_value, market_state)

        if realized is not None:
            fill.metadata["realized_pnl_usd"] = str(realized)

    def _add_to_cost_basis(self, token: TokenRef, units: Decimal, cost_usd: Decimal) -> None:
        """Fold ``units`` acquired for ``cost_usd`` into ``token``'s avg cost."""
        if units <= Decimal("0"):
            return
        held = self.tokens.get(token, Decimal("0"))
        prior_cost = self._cost_basis.get(token, Decimal("0")) * held
        new_units = held + units
        if new_units <= Decimal("0"):
            return
        self._cost_basis[token] = (prior_cost + cost_usd) / new_units

    def _token_price(self, token: TokenRef, market_state: MarketState | None) -> Decimal | None:
        """Market price for ``token``, or None when unavailable (no $1 guess)."""
        if market_state is None:
            return None
        try:
            price = market_state.get_price(token)
        except KeyError:
            return None
        return price if price > Decimal("0") else None

    def _leg_usd_value(self, leg: dict[TokenRef, Decimal], market_state: MarketState | None) -> Decimal:
        """USD value of a token leg: cash-equivalents at $1, others at market.

        Missing prices contribute zero (the conservation guards already
        rejected fills that would mint value); this is a valuation aid for
        cost-basis attribution, never a balance mutation.
        """
        total = Decimal("0")
        for token, amount in leg.items():
            if amount <= Decimal("0"):
                continue
            if self._is_cash_equivalent(token):
                total += amount
                continue
            price = self._token_price(token, market_state)
            if price is not None:
                total += amount * price
        return total

    def _plan_token_debits(
        self,
        tokens_out: dict[TokenRef, Decimal],
        intent_type: IntentType,
        market_state: MarketState | None,
    ) -> tuple[dict[TokenRef, Decimal], Decimal, dict[TokenRef, Decimal], str | None]:
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
        no_plan: tuple[dict[TokenRef, Decimal], Decimal, dict[TokenRef, Decimal]] = ({}, Decimal("0"), {})
        token_debits: dict[TokenRef, Decimal] = {}
        conversions: dict[TokenRef, Decimal] = {}
        cash_needed = Decimal("0")

        for token, amount in tokens_out.items():
            if amount <= Decimal("0"):
                continue
            debit_token = self._resolve_key(token)
            held = self.tokens.get(debit_token, Decimal("0"))
            from_tokens = min(held, amount)
            shortfall = amount - from_tokens
            token_debits[debit_token] = token_debits.get(debit_token, Decimal("0")) + from_tokens
            if shortfall <= amount * _DEBIT_DUST_RELATIVE_TOLERANCE:
                continue  # Fully covered (spend-all within dust tolerance)
            if self._is_cash_equivalent(token) or self._is_cash_equivalent(debit_token):
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

        # Nothing to fund -> nothing to reject: a fill with zero cash need must
        # not be blocked by already-negative cash-like (gas paid from a wallet
        # holding no stables drives it negative) — that rejects every sell-all
        # with "required 0, cash-like -0.001", and even blocks a cash-RAISING
        # LP close (ALM-2936).
        if cash_needed <= Decimal("0"):
            return token_debits, Decimal("0"), conversions, None

        cash_available = self._cash_like_available(token_debits)
        cash_shortfall = cash_needed - cash_available
        if cash_shortfall > Decimal("0"):
            if cash_shortfall > cash_needed * _DEBIT_DUST_RELATIVE_TOLERANCE:
                return (
                    *no_plan,
                    "insufficient cash for stablecoin outflow and implicit conversions: "
                    f"required {cash_needed}, cash-like {cash_available}",
                )
            # Spend-all within dust tolerance
            cash_needed = cash_available

        return token_debits, cash_needed, conversions, None

    @staticmethod
    def _conversion_price(token: TokenRef, market_state: MarketState | None) -> Decimal | None:
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
        if is_token_key(token):
            return normalize_token_key(token[0], token[1]) in self._cash_equivalent_token_keys
        if isinstance(token, str):
            if is_address_like(token):
                return normalize_token_key(self.chain, token) in self._cash_equivalent_token_keys
            return token.upper() in CASH_EQUIVALENT_STABLECOIN_SYMBOLS
        return False

    def register_token_identities(self, token_addresses: Mapping[str, tuple[str, str]] | None) -> None:
        """Build the run's typed identity table from the registered token map.

        Every balance-key decision (credit, debit, cost basis, balance read)
        resolves through :meth:`_resolve_key` against this table.
        """
        if not token_addresses:
            return
        for symbol, entry in token_addresses.items():
            if is_token_key(entry):
                chain, address = normalize_token_key(entry[0], entry[1])
                if not is_address_like(address):
                    continue
                if chain != self.chain:
                    # Foreign-chain entries never join this portfolio's key
                    # plane (mirrors the snapshot's same-chain alias filter).
                    continue
                self._identity_table[str(symbol).upper()] = TokenIdentity(
                    chain=chain, address=address, symbol=str(symbol)
                )

    def _resolve_key(self, token: TokenRef) -> TokenRef:
        """Resolve a token reference to its single balance key.

        Order: held form, address form, identity table, registry-resolved key
        if held, raw. Credits and debits use the same resolution, so one
        asset can never occupy two keys (ALM-2960).
        """
        normalized = self._normalize_token_ref(token)
        if normalized in self.tokens:
            return normalized
        if not isinstance(normalized, str) or is_address_like(normalized):
            return normalized

        identity = self._identity_table.get(normalized.upper())
        if identity is not None:
            return identity.key

        try:
            resolved = get_token_resolver().resolve(
                normalized,
                self.chain,
                log_errors=False,
                skip_gateway=True,
            )
        except TokenResolutionError:
            return normalized
        if resolved and resolved.address:
            key = normalize_token_key(self.chain, resolved.address)
            if key in self.tokens:
                return key
        return normalized

    def cash_like_available(self) -> Decimal:
        """Public read of spendable cash: ``cash_usd`` plus stable-token balances.

        Adapters must use THIS for capital checks, not bare ``cash_usd`` —
        token-funded portfolios (the platform's ``token_funding`` startup path)
        hold their stables as tokens and have ``cash_usd == 0``, so a bare
        ``cash_usd`` pre-check rejects every open on token-funded runs.
        """
        return self._cash_like_available()

    def _cash_like_available(self, planned_token_debits: dict[TokenRef, Decimal] | None = None) -> Decimal:
        """Cash plus explicit cash-equivalent token balances not already debited."""
        planned_token_debits = planned_token_debits or {}
        total = self.cash_usd
        for token, amount in self.tokens.items():
            if not self._is_cash_equivalent(token):
                continue
            remaining = amount - planned_token_debits.get(token, Decimal("0"))
            if remaining > Decimal("0"):
                total += remaining
        return total

    def _debit_cash_like(self, amount: Decimal) -> None:
        """Debit explicit cash-equivalent token balances first, then cash_usd."""
        remaining = amount
        if remaining <= Decimal("0"):
            return

        for token in sorted(
            (token for token in self.tokens if self._is_cash_equivalent(token)),
            key=token_ref_display,
        ):
            balance = self.tokens.get(token, Decimal("0"))
            debit = min(balance, remaining)
            if debit <= Decimal("0"):
                continue
            new_amount = balance - debit
            if new_amount <= Decimal("0"):
                self.tokens.pop(token, None)
                self._cost_basis.pop(token, None)
            else:
                self.tokens[token] = new_amount
            remaining -= debit
            if remaining <= Decimal("0"):
                return

        self.cash_usd -= remaining

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

    def _cash_funding_failure(
        self,
        fill: SimulatedFill,
        cash_debit: Decimal,
        token_debits: dict[TokenRef, Decimal],
    ) -> str | None:
        """Reason this fill cannot fund its cash legs, or None if it can.

        Aggregates every cash draw the fill will make -- the planned
        stablecoin-outflow / implicit-conversion debit plus perp-open
        collateral -- and requires non-embedded venue costs
        (:meth:`_venue_cash_costs`) on top, so partial checks cannot each
        pass while their sum overdraws ``cash_usd``.

        Gas is not part of this gate: it draws from the operational gas
        tank (:meth:`_gas_exhaustion_failure`), never from strategy cash.
        Venue costs (fee/slippage) are strategy capital and stay gated.

        Fills that draw nothing from cash (sells, closes, inflow-only
        fills) are exempt so a risk-reducing close is never blocked for
        being low on cash; their venue costs stay charged unconditionally
        and may transiently drive ``cash_usd`` negative.
        """
        required_cash = cash_debit
        if fill.position_delta is not None and fill.position_delta.is_perp:
            required_cash += fill.position_delta.collateral_usd
        if required_cash <= Decimal("0"):
            return None
        venue_costs = self._venue_cash_costs(fill)
        required_with_costs = required_cash + venue_costs
        cash_like_available = self._cash_like_available(token_debits)
        if required_with_costs > cash_like_available:
            return (
                f"insufficient cash for fill: required {required_with_costs} "
                f"(cash legs {required_cash} + venue costs {venue_costs}), "
                f"cash-like {cash_like_available}"
            )
        return None

    def _gas_exhaustion_failure(self, fill: SimulatedFill) -> str | None:
        """Reason a finite gas tank cannot cover this fill's gas, or None."""
        if self.gas_tank_budget_usd is None or fill.gas_cost_usd <= Decimal("0"):
            return None
        remaining = self.gas_tank_budget_usd - self.gas_tank_spent_usd
        if fill.gas_cost_usd > remaining:
            return (
                f"gas tank exhausted: fill needs ${fill.gas_cost_usd} gas, "
                f"${max(remaining, Decimal('0'))} of ${self.gas_tank_budget_usd} remaining — "
                "increase gas_funding_usd or reduce trading activity"
            )
        return None

    def _draw_gas_tank(self, gas_cost_usd: Decimal) -> None:
        if gas_cost_usd > Decimal("0"):
            self.gas_tank_spent_usd += gas_cost_usd

    @property
    def gas_tank_remaining_usd(self) -> Decimal | None:
        """Remaining tank budget, or None for an unlimited tank."""
        if self.gas_tank_budget_usd is None:
            return None
        return self.gas_tank_budget_usd - self.gas_tank_spent_usd

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

    def _position_close_failure(self, fill: SimulatedFill) -> str | None:
        """Reason this fill's full position close cannot apply, or None."""
        if fill.position_close_id is None:
            return None
        if self.get_position(fill.position_close_id) is None:
            return f"position {fill.position_close_id} not found for close"
        return None

    @staticmethod
    def _positive_amounts(amounts: dict[TokenRef, Decimal]) -> dict[TokenRef, Decimal]:
        return {token: amount for token, amount in amounts.items() if amount > Decimal("0")}

    @staticmethod
    def _reduction_flow_for_position(fill: SimulatedFill, position: SimulatedPosition) -> _ReductionFlow:
        if position.position_type == PositionType.BORROW:
            return _ReductionFlow(fill.tokens_out, "debited outflow", "debits")
        return _ReductionFlow(fill.tokens_in, "credited inflow", "credits")

    @staticmethod
    def _reduction_key_failure(
        position_id: str,
        tied: dict[TokenRef, Decimal],
        reduced: dict[TokenRef, Decimal],
        flow_label: str,
    ) -> str | None:
        if not reduced:
            return f"position {position_id} missing positive partial-reduction amounts"
        if tied.keys() != reduced.keys():
            return (
                f"position {position_id} partial reduction tokens "
                f"{sorted(token_ref_display(token) for token in reduced)} "
                f"do not match {flow_label} tokens {sorted(token_ref_display(token) for token in tied)}"
            )
        return None

    def _reduction_interest_tokens(
        self,
        fill: SimulatedFill,
        tied: dict[TokenRef, Decimal],
        realized_interest_usd: Decimal,
    ) -> Decimal:
        if len(tied) == 1 and realized_interest_usd > Decimal("0") and fill.executed_price > Decimal("0"):
            return realized_interest_usd / fill.executed_price
        return Decimal("0")

    @staticmethod
    def _reduction_interest_note(realized_interest_usd: Decimal, interest_tokens: Decimal) -> str:
        if interest_tokens <= Decimal("0"):
            return ""
        return f" (net of ${realized_interest_usd} realized interest)"

    def _reduction_amount_tie_failure(
        self,
        fill: SimulatedFill,
        tied: dict[TokenRef, Decimal],
        reduced: dict[TokenRef, Decimal],
        flow_verb: str,
    ) -> str | None:
        realized_interest_usd = abs(self._fill_realized_interest_usd(fill))
        interest_tokens = self._reduction_interest_tokens(fill, tied, realized_interest_usd)
        for token, tied_amount in tied.items():
            reduced_amount = reduced[token]
            expected = tied_amount - interest_tokens
            tolerance = max(tied_amount, reduced_amount) * _DEBIT_DUST_RELATIVE_TOLERANCE
            if abs(reduced_amount - expected) > tolerance:
                interest_note = self._reduction_interest_note(realized_interest_usd, interest_tokens)
                return (
                    f"position {fill.position_reduce_id} {flow_verb} {tied_amount} {token}{interest_note} "
                    f"but reduces {reduced_amount}"
                )
        return None

    @staticmethod
    def _reduction_holdings_failure(
        position: SimulatedPosition,
        reduced: dict[TokenRef, Decimal],
        position_id: str,
    ) -> str | None:
        for token, amount in reduced.items():
            held = position.get_amount(token)
            shortfall = amount - held
            if shortfall > amount * _DEBIT_DUST_RELATIVE_TOLERANCE:
                return f"position {position_id} holds {held} {token}, cannot reduce by {amount}"
        return None

    def _position_reduce_failure(self, fill: SimulatedFill) -> str | None:
        """Reason this fill's partial position reduction cannot apply, or None.

        Part of apply_fill's validate-then-commit stage: a reduce targeting
        a missing position, or removing more of a token than the position
        holds (beyond dust tolerance), rejects the whole fill before any
        state mutation -- crediting the inflow without debiting the
        position would mint value.

        The reduce map must also tie exactly to the fill's flow on the
        position's side (CodeRabbit, PR #2758): the credited ``tokens_in``
        for an asset position (a partial WITHDRAW takes tokens out of the
        supply) and the debited ``tokens_out`` for a BORROW position (a
        partial REPAY's outflow extinguishes the debt -- VIB-5098). An
        empty map, or per-token amounts that do not match within dust
        tolerance, is under- or over-reduction -- a minting class either
        way. The only producer (engine ``_execute_intent``) builds
        ``position_reduce_amounts`` from the matching flow directly, so
        this is defense-in-depth at the conservation boundary.
        """
        if fill.position_reduce_id is None:
            return None
        position = self.get_position(fill.position_reduce_id)
        if position is None:
            return f"position {fill.position_reduce_id} not found for partial reduction"
        flow = self._reduction_flow_for_position(fill, position)
        tied = self._positive_amounts(flow.amounts)
        reduced = self._positive_amounts(fill.position_reduce_amounts)
        key_failure = self._reduction_key_failure(fill.position_reduce_id, tied, reduced, flow.label)
        if key_failure is not None:
            return key_failure
        # A reduce may legitimately fall SHORT of the flow by exactly the
        # realized-interest slice: a BOUNDARY repay/withdraw's flow covers
        # principal + part of the accrued interest, but only the principal
        # leaves the position -- the interest is realized as PnL, not removed as
        # principal (VIB-5098). That slice in token units is
        # |interest_usd| / executed_price (single-token lending position). Any
        # OTHER shortfall -- and ANY excess (reduced > flow, the original
        # minting class) -- is still rejected.
        amount_failure = self._reduction_amount_tie_failure(fill, tied, reduced, flow.verb)
        if amount_failure is not None:
            return amount_failure
        return self._reduction_holdings_failure(position, reduced, fill.position_reduce_id)

    def _accrue_interest_through_fill(
        self,
        fill: SimulatedFill,
        market_state: MarketState | None,
        adapter: "StrategyBacktestAdapter | None",
    ) -> None:
        """Accrue lending interest on the reduce target up to the fill instant.

        Pending intents execute BEFORE the per-tick adapter update and
        ``mark_to_market`` (``pnl/_engine_helpers.execute_iteration_loop``),
        so without this the interval since the previous mark would accrue on
        the POST-reduction principal -- under-accruing the interest earned
        by the withdrawn slice before the withdraw (CodeRabbit, PR #2758).

        Each lane accrues with its own math, so no rate logic is duplicated:
        the adapter lane calls ``adapter.update_position`` (the adapter's
        own APY sources), the generic lane calls
        :meth:`_mark_lending_position` (entry APY / protocol default). Both
        advance ``position.last_updated`` to ``fill.timestamp``, which makes
        the same-tick follow-up accrual a no-op: the generic mark's elapsed
        basis is ``last_updated`` already, and the engine clamps the adapter
        lane's per-position elapsed to ``last_updated``
        (``PnLBacktester._update_positions_via_adapter``).

        Accrual failures are non-fatal: the fill still applies under the
        status-quo timing semantics (a one-tick bounded interest error)
        rather than leaving the fill half-committed.
        """
        if market_state is None or fill.position_reduce_id is None:
            return
        position = self.get_position(fill.position_reduce_id)
        if position is None or not position.is_lending:
            return
        try:
            if adapter is not None:
                basis = position.last_updated or position.entry_time
                elapsed_seconds = (fill.timestamp - basis).total_seconds()
                if elapsed_seconds > 0:
                    adapter.update_position(position, market_state, elapsed_seconds, fill.timestamp)
            else:
                self._mark_lending_position(position, market_state, fill.timestamp)
        except Exception:
            logger.warning(
                "Interest accrual before partial reduce of %s failed; "
                "the reduce proceeds with mark-owned accrual timing",
                fill.position_reduce_id,
                exc_info=True,
            )

    def _reduce_position(self, position_id: str, reduce_amounts: dict[TokenRef, Decimal]) -> None:
        """Commit a validated partial reduction of a position's principal.

        Runs only after :meth:`_position_reduce_failure` validated coverage,
        so every debit is held (a sub-dust shortfall clamps to zero). This
        method removes PRINCIPAL only; any interest covered by a boundary
        reduce is realized separately by :meth:`_realize_reduce_interest`.
        Interest through the fill instant has already been accrued by
        :meth:`_accrue_interest_through_fill`, which also advanced
        ``last_updated``; the mark/update paths own accrual from there on.
        """
        position = self.get_position(position_id)
        if position is None:  # pragma: no cover - guarded by the plan stage
            return
        for token, amount in reduce_amounts.items():
            if amount <= Decimal("0"):
                continue
            remaining = position.get_amount(token) - amount
            position.amounts[token] = remaining if remaining > Decimal("0") else Decimal("0")
        if position.total_amount <= Decimal("0"):
            # Reached by a BOUNDARY reduce (amount covers all principal + part
            # of the accrued interest -- VIB-5098) or a sub-dust clamp.
            # Deliberately NOT auto-closed here: _close_position would drop the
            # position's REMAINING accrued interest from equity without
            # realizing it. The empty position stays open carrying that
            # remainder; the next WITHDRAW/REPAY full-closes it (principal 0 +
            # interest), which conserves value.
            logger.warning(
                "Partial reduce clamped position %s principal to zero; "
                "position remains open until a full close realizes its accrued interest",
                position_id,
            )

    @staticmethod
    def _fill_realized_interest_usd(fill: SimulatedFill) -> Decimal:
        """Interest USD a REPAY/WITHDRAW reduce realizes (0 if none).

        Reads ``metadata["interest_usd"]`` (set by the engine only on a
        BOUNDARY partial, where the flow covers principal + part of accrued
        interest). Sign matches the close convention: NEGATIVE for a REPAY
        (borrow interest owed), POSITIVE for a WITHDRAW (interest earned). An
        ordinary sub-principal partial carries no such metadata and returns 0.

        The engine stamps this as a ``str(Decimal)`` (engine ``_execute_intent``),
        so ``Decimal(str(...))`` round-trips both the stored string and the
        ``Decimal`` default losslessly -- no caller-bifurcating ``isinstance``
        coercion (VIB-4062).
        """
        return Decimal(str(fill.metadata.get("interest_usd", "0")))

    def _realize_reduce_interest(self, fill: SimulatedFill) -> None:
        """Realize the interest a boundary partial reduce covered.

        :meth:`_reduce_position` removed the principal; this decrements the
        covered interest off ``position.interest_accrued`` so it leaves equity
        (mirroring a full close) and accumulates ``_realized_pnl``. The full
        close path accumulates ``_realized_pnl`` in ``apply_fill`` (guarded by
        ``position_close_id``); the reduce path is not covered there, so it is
        accumulated here. ``_accrue_interest_through_fill`` has already grown
        ``interest_accrued`` to the fill instant, so the realized slice is
        bounded by it (clamped defensively).

        When the clamp bites, ``metadata["interest_usd"]`` is rewritten to the
        clamped signed amount so the TradeRecord stays in sync: ``apply_fill``
        calls :meth:`_calculate_trade_pnl` (which reads this metadata) AFTER this
        method, so without the rewrite the trade would report the unclamped
        value while ``_realized_pnl`` carries the clamped one (CodeRabbit PR
        #2777 round 2).
        """
        realized = self._fill_realized_interest_usd(fill)
        if realized == Decimal("0") or fill.position_reduce_id is None:
            return
        position = self.get_position(fill.position_reduce_id)
        if position is None:  # pragma: no cover - guarded by the plan stage
            return
        interest_paid = min(abs(realized), position.interest_accrued)
        position.interest_accrued -= interest_paid
        # Sign mirrors the close convention: REPAY realizes a LOSS, WITHDRAW a
        # gain.
        signed_realized = -interest_paid if realized < Decimal("0") else interest_paid
        self._realized_pnl += signed_realized
        fill.metadata["interest_usd"] = str(signed_realized)

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
        entry_amounts = position.metadata.get("entry_amounts", {})
        if entry_amounts:
            entry_token0 = Decimal(str(entry_amounts.get(token0, "0")))
            entry_token1 = Decimal(str(entry_amounts.get(token1, "0")))
        else:
            # Fallback: derive the entry token amounts from the position's V3
            # liquidity units at the entry price (VIB-5096 — liquidity holds
            # L-units, so amounts must come from the V3 math, never from
            # treating liquidity as a USD figure).
            from almanak.framework.backtesting.pnl.calculators.impermanent_loss import (
                ImpermanentLossCalculator,
            )

            _, entry_token0, entry_token1 = ImpermanentLossCalculator().calculate_il_v3(
                entry_price=position.entry_price,
                current_price=position.entry_price,
                tick_lower=position.tick_lower if position.tick_lower is not None else -887272,
                tick_upper=position.tick_upper if position.tick_upper is not None else 887272,
                liquidity=position.liquidity,
            )
        # entry_price is the token0/token1 ratio, so the parenthesised sum is
        # the entry composition in token1 units; one multiply by token1_price
        # converts to USD. (Adding entry_token1 * token1_price to a token1-
        # denominated term would mix units whenever token1 is not a $1 stable.)
        entry_value = (entry_token0 * position.entry_price + entry_token1) * token1_price

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

    def _calculate_trade_pnl(self, fill: SimulatedFill) -> Decimal | None:
        """Calculate realized PnL for a trade, or None when it has none yet.

        Realization semantics by intent type (blueprint 31 section 4;
        VIB-5083):
        - SWAP: a leg that DISPOSES of a tracked spot token realizes
          ``proceeds - units x avg_cost`` (stashed in
          ``metadata["realized_pnl_usd"]`` by :meth:`_record_swap_cost_basis`
          before the basis is mutated). A swap that only ACQUIRES inventory
          (a cash->token buy) realizes nothing yet -- its PnL is UNKNOWN, so
          this returns ``None`` (Empty != Zero). The metrics layer excludes
          ``None`` from win/loss stats rather than scoring it as a loss.
        - LP_CLOSE / PERP_CLOSE: realized from IL + fees / price + funding,
          carried in ``metadata["realized_pnl_usd"]``.
        - WITHDRAW / REPAY: realized from interest in ``metadata["interest_usd"]``.

        Other intent types (opens, supplies, holds) realize nothing and
        return ``None``.

        Args:
            fill: The simulated fill

        Returns:
            Realized PnL in USD (before execution costs), or ``None`` when the
            trade realizes no PnL (an opening / inventory-building trade).
        """
        if fill.intent_type == IntentType.SWAP:
            # A disposing swap stashed its realized PnL; an inventory-building
            # buy stashed nothing -> unknown, not zero.
            if "realized_pnl_usd" not in fill.metadata:
                return None
            return Decimal(str(fill.metadata["realized_pnl_usd"]))

        # For position closes, check if we have metadata about the close
        if fill.intent_type in (IntentType.LP_CLOSE, IntentType.PERP_CLOSE):
            pnl_value = fill.metadata.get("realized_pnl_usd", Decimal("0"))
            return Decimal(str(pnl_value)) if not isinstance(pnl_value, Decimal) else pnl_value

        # For lending operations, interest is typically in metadata
        if fill.intent_type in (IntentType.WITHDRAW, IntentType.REPAY):
            interest_value = fill.metadata.get("interest_usd", Decimal("0"))
            return Decimal(str(interest_value)) if not isinstance(interest_value, Decimal) else interest_value

        return None

    @staticmethod
    def _lp_fees_earned(position: SimulatedPosition) -> Decimal:
        fees_earned_usd = position.accumulated_fees_usd
        return position.fees_earned if fees_earned_usd == Decimal("0") else fees_earned_usd

    @staticmethod
    def _metadata_decimal(value: Any) -> Decimal:
        return value if type(value) is Decimal else Decimal(str(value))

    def _lp_close_prices(
        self,
        fill: SimulatedFill,
        token1: TokenRef,
    ) -> tuple[Decimal, Decimal]:
        token0_price = self._metadata_decimal(fill.metadata.get("token0_price_usd", fill.executed_price))
        token1_price = fill.metadata.get("token1_price_usd")
        if token1_price is None:
            token1_price = self._stablecoin_fallback(token1, "record_lp_close")
        return token0_price, self._metadata_decimal(token1_price)

    @staticmethod
    def _lp_close_value(
        position: SimulatedPosition,
        fill: SimulatedFill,
        token0: TokenRef,
        token1: TokenRef,
        token0_price: Decimal,
        token1_price: Decimal,
    ) -> Decimal:
        current_token0 = fill.tokens_in.get(token0, position.amounts.get(token0, Decimal("0")))
        current_token1 = fill.tokens_in.get(token1, position.amounts.get(token1, Decimal("0")))
        return current_token0 * token0_price + current_token1 * token1_price

    @staticmethod
    def _lp_realized_close_value(fill: SimulatedFill, close_value: Decimal, fees_earned_usd: Decimal) -> Decimal:
        collect_fees = fill.metadata.get("collect_fees")
        if collect_fees is True or collect_fees is False:
            return close_value
        return close_value + fees_earned_usd

    @staticmethod
    def _lp_tick_bounds(position: SimulatedPosition) -> tuple[int, int]:
        tick_lower = position.tick_lower if position.tick_lower is not None else -887272
        tick_upper = position.tick_upper if position.tick_upper is not None else 887272
        return tick_lower, tick_upper

    def _lp_entry_value_and_il(
        self,
        position: SimulatedPosition,
        fill: SimulatedFill,
        current_value: Decimal,
        token0_price: Decimal,
        token1_price: Decimal,
    ) -> tuple[Decimal, Decimal]:
        if position.entry_price <= 0:
            initial_value = fill.amount_usd if fill.amount_usd > 0 else current_value
            return initial_value, Decimal("0")

        from almanak.framework.backtesting.pnl.calculators.impermanent_loss import (
            ImpermanentLossCalculator,
        )

        tick_lower, tick_upper = self._lp_tick_bounds(position)
        current_price_ratio = token0_price / token1_price if token1_price > 0 else position.entry_price
        il_pct, entry_token0, entry_token1 = ImpermanentLossCalculator().calculate_il_v3(
            entry_price=position.entry_price,
            current_price=current_price_ratio,
            tick_lower=tick_lower,
            tick_upper=tick_upper,
            liquidity=position.liquidity,
        )
        initial_value = entry_token0 * position.entry_price * token1_price + entry_token1 * token1_price
        hold_value = entry_token0 * token0_price + entry_token1 * token1_price
        return initial_value, il_pct * hold_value

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
        fees_earned_usd = self._lp_fees_earned(position)
        if len(position.tokens) < 2:
            return Decimal("0"), fees_earned_usd, Decimal("0")

        token0 = position.tokens[0]
        token1 = position.tokens[1]
        token0_price, token1_price = self._lp_close_prices(fill, token1)
        current_value = self._lp_close_value(position, fill, token0, token1, token0_price, token1_price)
        initial_value, il_loss_usd = self._lp_entry_value_and_il(
            position,
            fill,
            current_value,
            token0_price,
            token1_price,
        )
        realized_value = self._lp_realized_close_value(fill, current_value, fees_earned_usd)
        net_lp_pnl_usd = realized_value - initial_value

        return il_loss_usd, fees_earned_usd, net_lp_pnl_usd

    def _resolve_token_symbol(
        self,
        token: TokenRef,
        chain_id: int | None,
        require_symbol_mapping: bool,
        data_tracker: "DataQualityTracker | None",
    ) -> TokenRef:
        """Resolve a token key for valuation.

        Address-native portfolio keys price directly through MarketState.
        Legacy bare address strings fall through the old registry bridge only
        when the caller explicitly requires symbol mapping.

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
        if is_token_key(token):
            return normalize_token_key(token[0], token[1])

        # If token looks like an address (starts with 0x), try to resolve it
        if isinstance(token, str) and token.startswith("0x") and len(token) == 42:
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
        token: TokenRef,
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
                token=token_ref_display(token),
                timestamp=simulation_timestamp,
                chain_id=chain_id,
            )

        # Log warning with context
        timestamp_str = simulation_timestamp.isoformat() if simulation_timestamp else "unknown"
        logger.warning(
            "Missing price for token %s at timestamp %s (chain_id=%s, context=%s). Using fallback value.",
            token_ref_display(token),
            timestamp_str,
            chain_id or "unknown",
            context,
        )

        # In strict mode, fail instead of using fallback
        if strict_price_mode:
            raise ValueError(
                f"Missing price for token {token_ref_display(token)} at timestamp {timestamp_str} "
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
                        token=token_ref_display(token),
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
                        f"Missing price for token {token_ref_display(token)} at timestamp {timestamp_str} "
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

    def aggregate_position_metrics(self) -> PositionMetricsAggregate:
        """Aggregate position-derived metrics over open + closed positions.

        Single source of truth for the position-level metric block shared by
        :meth:`get_metrics` and the engine-result path
        (``metrics_calculator.calculate_metrics``). LP fee accrual, perp funding,
        and lending interest are summed over both ``positions`` (open) and
        ``_closed_positions``; the risk extrema and realized/unrealized PnL are
        read off the portfolio's running trackers. See
        :class:`PositionMetricsAggregate` for why this is centralized.
        """
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
                # Use the SAME fee source as the per-trade close reporter
                # (_calculate_lp_pnl_breakdown), value_position, and
                # _calculate_lp_unrealized_pnl: the detailed accumulator
                # (accumulated_fees_usd) is primary, with fees_earned as the
                # fallback. The two are kept in lockstep by both accrual lanes
                # today, but matching the close reporter makes the fee-reporting
                # tie-out robust by construction even if a lane only updates the
                # detailed accumulator (CodeRabbit, PR #2852).
                lp_fees = position.accumulated_fees_usd
                if lp_fees == Decimal("0"):
                    lp_fees = position.fees_earned
                total_fees_earned += lp_fees
                # Use position_id as pool identifier
                pool_id = position.position_id
                fees_by_pool[pool_id] = fees_by_pool.get(pool_id, Decimal("0")) + lp_fees
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

        # Liquidation ledgers. Perp records an explicit loss_usd; lending records
        # collateral_seized / debt_repaid, whose difference is the liquidation
        # penalty (the borrower's loss) -- the simulator sets
        # collateral_seized = debt_repaid * (1 + penalty).
        perp_liquidation_losses = sum((liq.loss_usd for liq in self._perp_liquidations), Decimal("0"))
        lending_liquidation_losses = sum(
            (liq.collateral_seized - liq.debt_repaid for liq in self._lending_liquidations),
            Decimal("0"),
        )

        return PositionMetricsAggregate(
            total_fees_earned_usd=total_fees_earned,
            fees_by_pool=fees_by_pool,
            lp_fee_confidence_breakdown=lp_fee_confidence_breakdown,
            total_funding_paid=total_funding_paid,
            total_funding_received=total_funding_received,
            total_interest_earned=total_interest_earned,
            total_interest_paid=total_interest_paid,
            max_margin_utilization=self._max_margin_utilization,
            min_health_factor=self._min_health_factor,
            health_factor_warnings=self._health_factor_warnings,
            realized_pnl=self._realized_pnl,
            unrealized_pnl=self._unrealized_pnl,
            liquidations_count=len(self._perp_liquidations) + len(self._lending_liquidations),
            liquidation_losses_usd=perp_liquidation_losses + lending_liquidation_losses,
        )

    def _equity_summary(self, equity_values: list[Decimal], timestamps: list[datetime]) -> _EquitySummary:
        initial_value = equity_values[0] if equity_values else self.initial_capital_usd
        final_value = equity_values[-1] if equity_values else self.initial_capital_usd
        total_pnl = final_value - initial_value
        total_return = (final_value - initial_value) / initial_value if initial_value > 0 else Decimal("0")
        annualized_return = self._annualized_return(total_return, timestamps)
        return _EquitySummary(total_pnl, total_return, annualized_return)

    @staticmethod
    def _annualized_return(total_return: Decimal, timestamps: list[datetime]) -> Decimal:
        if len(timestamps) < 2:
            return Decimal("0")
        duration_days = (timestamps[-1] - timestamps[0]).total_seconds() / (24 * 3600)
        if duration_days <= 0:
            return Decimal("0")
        years = Decimal(str(duration_days)) / Decimal("365")
        if years <= 0:
            return Decimal("0")
        if total_return <= Decimal("-1"):
            return Decimal("0")
        return (Decimal("1") + total_return) ** (Decimal("1") / years) - Decimal("1")

    def _execution_cost_totals(self) -> _ExecutionCostTotals:
        return _ExecutionCostTotals(
            fees=sum((t.fee_usd for t in self.trades), Decimal("0")),
            slippage=sum((t.slippage_usd for t in self.trades), Decimal("0")),
            gas=sum((t.gas_cost_usd for t in self.trades), Decimal("0")),
        )

    def _risk_metrics(self, equity_values: list[Decimal], annualized_return: Decimal) -> _RiskMetrics:
        returns = self._calculate_returns(equity_values)
        volatility = self._calculate_volatility(returns)
        sharpe = self._calculate_sharpe(returns, volatility)
        sortino = self._calculate_sortino(returns)
        max_drawdown = self._calculate_max_drawdown(equity_values)
        calmar = annualized_return / max_drawdown if max_drawdown > 0 else Decimal("0")
        return _RiskMetrics(volatility, sharpe, sortino, max_drawdown, calmar)

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

        equity_values = [p.value_usd for p in self.equity_curve]
        timestamps = [p.timestamp for p in self.equity_curve]
        equity = self._equity_summary(equity_values, timestamps)

        # Execution costs (informational breakdown only -- the equity curve
        # is already net of them: gas and venue fee/slippage are debited from
        # cash in apply_fill, SWAP fee/slippage are netted into tokens_in).
        # Subtracting them from total_pnl again would double-count every
        # cost, so net_pnl == total_pnl (same contract as calculate_metrics).
        costs = self._execution_cost_totals()
        risk = self._risk_metrics(equity_values, equity.annualized_return)

        # Trade statistics (VIB-5083): shared with calculate_metrics so the
        # win/loss / realized-PnL discipline lives in one place. Lazy import
        # avoids the metrics_calculator <-> portfolio import cycle.
        from almanak.framework.backtesting.pnl.calculators.attribution import calculate_all_attributions
        from almanak.framework.backtesting.pnl.metrics_calculator import _compute_trade_statistics

        stats = _compute_trade_statistics(self.trades)
        pnl_by_protocol, pnl_by_intent_type, pnl_by_asset = calculate_all_attributions(self.trades)

        # Position-derived metrics (LP fees, perp funding, lending interest,
        # health/margin extrema, realized/unrealized PnL). Sourced from the same
        # helper the engine-result path uses so the two can never drift -- see
        # PositionMetricsAggregate.
        pos = self.aggregate_position_metrics()

        return BacktestMetrics(
            total_pnl_usd=equity.total_pnl,
            net_pnl_usd=equity.total_pnl,
            sharpe_ratio=risk.sharpe,
            max_drawdown_pct=risk.max_drawdown,
            win_rate=stats.win_rate,
            # Successful trades only -- rejected fills are reported separately
            # as failed_trades and must stay out of the performance denominator
            # (VIB-5083, CodeRabbit).
            total_trades=len(self.trades) - stats.failed_trades,
            profit_factor=stats.profit_factor,
            # VIB-2915: `*_return_pct` fields store actual percentages (e.g. 10 for 10%),
            # not decimal ratios. Local `total_return`/`annualized_return` stay as ratios
            # so the calmar calculation above (which divides by `max_drawdown`, still a ratio) stays correct.
            total_return_pct=equity.total_return * Decimal("100"),
            annualized_return_pct=equity.annualized_return * Decimal("100"),
            total_fees_usd=costs.fees,
            total_slippage_usd=costs.slippage,
            total_gas_usd=costs.gas,
            winning_trades=stats.winning_trades,
            losing_trades=stats.losing_trades,
            trades_with_realized_pnl=stats.trades_with_realized_pnl,
            failed_trades=stats.failed_trades,
            avg_trade_pnl_usd=stats.avg_trade_pnl,
            largest_win_usd=stats.largest_win,
            largest_loss_usd=stats.largest_loss,
            avg_win_usd=stats.avg_win,
            avg_loss_usd=stats.avg_loss,
            volatility=risk.volatility,
            sortino_ratio=risk.sortino,
            calmar_ratio=risk.calmar,
            total_fees_earned_usd=pos.total_fees_earned_usd,
            fees_by_pool=pos.fees_by_pool,
            lp_fee_confidence_breakdown=pos.lp_fee_confidence_breakdown,
            total_funding_paid=pos.total_funding_paid,
            total_funding_received=pos.total_funding_received,
            max_margin_utilization=pos.max_margin_utilization,
            total_interest_earned=pos.total_interest_earned,
            total_interest_paid=pos.total_interest_paid,
            min_health_factor=pos.min_health_factor,
            health_factor_warnings=pos.health_factor_warnings,
            realized_pnl=pos.realized_pnl,
            unrealized_pnl=pos.unrealized_pnl,
            liquidations_count=pos.liquidations_count,
            liquidation_losses_usd=pos.liquidation_losses_usd,
            pnl_by_protocol=pnl_by_protocol,
            pnl_by_intent_type=pnl_by_intent_type,
            pnl_by_asset=pnl_by_asset,
        )

    @staticmethod
    def _empty_confidence_breakdown() -> dict[str, int]:
        return {"high": 0, "medium": 0, "low": 0}

    @staticmethod
    def _record_confidence(breakdown: dict[str, int], confidence: str | None) -> None:
        if confidence is None:
            return
        bucket = confidence if confidence in breakdown else "low"
        breakdown[bucket] += 1

    @staticmethod
    def _append_unique_source(data_sources: list[str], source: Any) -> None:
        if source and source not in data_sources:
            data_sources.append(source)

    def _tracked_positions(self) -> list[SimulatedPosition]:
        return list(self.positions) + list(self._closed_positions)

    @classmethod
    def _confidence_breakdown(cls, positions: list[SimulatedPosition], attr: str) -> dict[str, int]:
        breakdown = cls._empty_confidence_breakdown()
        for position in positions:
            confidence = getattr(position, attr, None)
            cls._record_confidence(breakdown, confidence)
        return breakdown

    @classmethod
    def _unique_sources_from_attr(cls, positions: list[SimulatedPosition], attr: str) -> list[str]:
        data_sources: list[str] = []
        for position in positions:
            cls._append_unique_source(data_sources, getattr(position, attr, None))
        return data_sources

    @classmethod
    def _unique_sources_from_metadata(cls, positions: list[SimulatedPosition], key: str) -> list[str]:
        """Aggregate distinct data sources across positions.

        Reads the CUMULATIVE ``<key>s`` list when a position carries one (a
        position that degraded mid-run touched several sources — exporting
        only the latest hides the degradation); the singular latest-wins key
        is the fallback for positions written before the list existed.
        """
        data_sources: list[str] = []
        for position in positions:
            cumulative = position.metadata.get(f"{key}s")
            if isinstance(cumulative, list) and cumulative:
                for source in cumulative:
                    cls._append_unique_source(data_sources, source)
            else:
                cls._append_unique_source(data_sources, position.metadata.get(key))
        return data_sources

    def _lp_data_coverage_metrics(self, lp_positions: list[SimulatedPosition]) -> LPMetrics:
        return LPMetrics(
            position_count=len(lp_positions),
            fee_confidence_breakdown=self._confidence_breakdown(lp_positions, "fee_confidence"),
            data_sources=self._unique_sources_from_metadata(lp_positions, "data_source"),
        )

    def _perp_data_coverage_metrics(self, perp_positions: list[SimulatedPosition]) -> PerpMetrics:
        return PerpMetrics(
            position_count=len(perp_positions),
            funding_confidence_breakdown=self._confidence_breakdown(perp_positions, "funding_confidence"),
            data_sources=self._unique_sources_from_attr(perp_positions, "funding_data_source"),
        )

    def _lending_data_coverage_metrics(self, lending_positions: list[SimulatedPosition]) -> LendingMetrics:
        return LendingMetrics(
            position_count=len(lending_positions),
            apy_confidence_breakdown=self._confidence_breakdown(lending_positions, "apy_confidence"),
            data_sources=self._unique_sources_from_attr(lending_positions, "apy_data_source"),
        )

    def _slippage_data_coverage_metrics(self, lp_positions: list[SimulatedPosition]) -> SlippageMetrics:
        return SlippageMetrics(
            calculation_count=sum(1 for position in lp_positions if position.slippage_confidence is not None),
            slippage_confidence_breakdown=self._confidence_breakdown(lp_positions, "slippage_confidence"),
        )

    def calculate_data_coverage_metrics(self) -> DataCoverageMetrics:
        """Calculate data coverage metrics across all position types.

        Aggregates confidence levels and data sources from all positions
        (LP, Perp, Lending) and slippage calculations to provide an overall
        view of data quality in the backtest.

        Returns:
            DataCoverageMetrics with breakdown by position type and overall coverage.
        """
        tracked_positions = self._tracked_positions()
        lp_positions = [position for position in tracked_positions if position.is_lp]
        perp_positions = [position for position in tracked_positions if position.is_perp]
        lending_positions = [position for position in tracked_positions if position.is_lending]

        return DataCoverageMetrics(
            lp_metrics=self._lp_data_coverage_metrics(lp_positions),
            perp_metrics=self._perp_data_coverage_metrics(perp_positions),
            lending_metrics=self._lending_data_coverage_metrics(lending_positions),
            slippage_metrics=self._slippage_data_coverage_metrics(lp_positions),
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

    def _token_holdings_value(self, market_state: MarketState) -> Decimal:
        value = Decimal("0")
        for token, amount in self.tokens.items():
            try:
                value += amount * market_state.get_price(token)
            except KeyError:
                if self.strict_reproducibility:
                    raise
                pass
        return value

    def _adapter_position_value(
        self,
        position: SimulatedPosition,
        market_state: MarketState,
        timestamp: datetime,
        adapter: "StrategyBacktestAdapter",
    ) -> Decimal:
        value = adapter.value_position(position, market_state, timestamp)
        # Adapter contract (LendingBacktestAdapter.value_position): BORROW
        # valuations return the debt magnitude; portfolio equity subtracts it.
        if position.position_type == PositionType.BORROW:
            return -value
        return value

    def _mark_position_value(
        self,
        position: SimulatedPosition,
        market_state: MarketState,
        timestamp: datetime,
        adapter: "StrategyBacktestAdapter | None",
    ) -> Decimal:
        if position.is_spot:
            return self._mark_spot_position(position, market_state)
        if adapter is not None:
            try:
                return self._adapter_position_value(position, market_state, timestamp, adapter)
            except Exception:
                return self._value_position_fallback(position, market_state, timestamp)
        return self._value_position_fallback(position, market_state, timestamp)

    def _numeraire_price_usd(self, market_state: MarketState) -> Decimal | None:
        if self._numeraire_token is not None:
            try:
                return market_state.get_price(self._numeraire_token)
            except KeyError:
                pass
        if self._numeraire_symbol is None:
            return None
        try:
            return market_state.get_price(self._numeraire_symbol)
        except KeyError:
            return None

    def _record_equity_point(
        self,
        timestamp: datetime,
        value_usd: Decimal,
        numeraire_price_usd: Decimal | None,
    ) -> None:
        self.equity_curve.append(
            EquityPoint(
                timestamp=timestamp,
                value_usd=value_usd,
                numeraire_price_usd=numeraire_price_usd,
            )
        )

    def _record_price_point(self, timestamp: datetime, market_state: MarketState) -> None:
        """Snapshot the tick's token prices next to the equity point.

        Captures ``market_state.prices`` verbatim — the exact prices this mark
        valued the portfolio with — under the ``pnl_by_asset`` attribution key
        convention (``chain:address`` for resolved ERC20s, symbols for natives
        and custom fixtures). Reporting-only; the simulation never reads it.

        Duck-typed market states that expose only ``get_price`` (test
        fixtures) record an empty snapshot: the 1:1 alignment with
        ``equity_curve`` is preserved and nothing is fabricated.
        """
        prices = getattr(market_state, "prices", None)
        snapshot: dict[str, Decimal] = (
            {token_ref_display(token): price for token, price in prices.items()} if isinstance(prices, dict) else {}
        )
        self.price_series.append(PricePoint(timestamp=timestamp, prices=snapshot))

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
        total_value = self.cash_usd + self._token_holdings_value(market_state)
        for position in self.positions:
            total_value += self._mark_position_value(position, market_state, timestamp, adapter)

        # Update health factors for lending positions after all values are calculated
        self._update_health_factors(market_state)

        # Update unrealized PnL tracking at each mark_to_market
        self._unrealized_pnl = self.calculate_unrealized_pnl(market_state)

        # Record the equity point. ``value_usd`` stays USD (the conservation
        # core). For a non-USD numeraire (VIB-5127), capture the numeraire
        # token's USD price at this timestamp so the reporting layer can divide
        # value_usd by it; a missing price stays None and fails loud at metrics
        # time rather than aborting an otherwise-valid USD run mid-loop.
        self._record_equity_point(timestamp, total_value, self._numeraire_price_usd(market_state))
        # Snapshot the prices this mark used (aligned 1:1 with equity_curve)
        # for the result's visualization/audit price series.
        self._record_price_point(timestamp, market_state)

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

    @staticmethod
    def _lp_fee_days_elapsed(position: SimulatedPosition, timestamp: datetime) -> Decimal:
        start = position.last_updated or position.entry_time
        time_elapsed = timestamp - start
        return Decimal(str(time_elapsed.total_seconds())) / Decimal("86400")

    @staticmethod
    def _lp_liquidity_share(position_value_usd: Decimal) -> Decimal:
        # The numerator must be the position's USD value -- position.liquidity
        # holds V3 L-units (VIB-5096), not a USD figure. There is deliberately
        # NO floor here: clamping tiny real shares to 10% minted LP fee value.
        base_liquidity = Decimal("1000000")
        return min(Decimal("1"), position_value_usd / base_liquidity)

    @staticmethod
    def _lp_fee_tier_model(fee_tier: Decimal) -> _LpFeeTierModel:
        fee_tier_pct = fee_tier * Decimal("100")
        if fee_tier_pct <= Decimal("0.01"):
            return _LpFeeTierModel(Decimal("50"), Decimal("0.10"))
        if fee_tier_pct <= Decimal("0.05"):
            return _LpFeeTierModel(Decimal("20"), Decimal("0.20"))
        if fee_tier_pct <= Decimal("0.30"):
            return _LpFeeTierModel(Decimal("10"), Decimal("0.25"))
        return _LpFeeTierModel(Decimal("3"), Decimal("0.10"))

    def _lp_fee_estimate(
        self,
        position_value_usd: Decimal,
        fee_tier: Decimal,
        days_elapsed: Decimal,
    ) -> Decimal:
        model = self._lp_fee_tier_model(fee_tier)
        estimated_daily_volume = position_value_usd * model.volume_multiplier
        volume_based_fees = estimated_daily_volume * fee_tier * self._lp_liquidity_share(position_value_usd)
        apr_based_fees = position_value_usd * (model.base_apr / Decimal("365"))
        return ((volume_based_fees + apr_based_fees) * days_elapsed) / Decimal("2")

    @staticmethod
    def _lp_fee_tokens(position: SimulatedPosition) -> tuple[TokenRef, TokenRef]:
        token0 = position.tokens[0] if len(position.tokens) > 0 else ""
        token1 = position.tokens[1] if len(position.tokens) > 1 else ""
        return token0, token1

    @staticmethod
    def _lp_fee_attribution_ratios(
        position: SimulatedPosition,
        position_value_usd: Decimal,
        token0: TokenRef,
        token1: TokenRef,
    ) -> tuple[Decimal, Decimal]:
        if position_value_usd <= 0 or position.entry_price <= 0:
            return Decimal("0.5"), Decimal("0.5")

        token0_value = position.amounts.get(token0, Decimal("0")) * position.entry_price
        token1_value = position.amounts.get(token1, Decimal("0"))
        total_value = token0_value + token1_value
        if total_value <= 0:
            return Decimal("0.5"), Decimal("0.5")
        return token0_value / total_value, token1_value / total_value

    def _apply_lp_fee_attribution(
        self,
        position: SimulatedPosition,
        fees_usd: Decimal,
        position_value_usd: Decimal,
    ) -> None:
        position.accumulated_fees_usd += fees_usd
        token0, token1 = self._lp_fee_tokens(position)
        token0_ratio, token1_ratio = self._lp_fee_attribution_ratios(position, position_value_usd, token0, token1)
        if position.entry_price > 0:
            position.fees_token0 += (fees_usd * token0_ratio) / position.entry_price
        position.fees_token1 += fees_usd * token1_ratio

    def _simulate_lp_fee_accrual(
        self,
        position: SimulatedPosition,
        position_value_usd: Decimal,
        timestamp: datetime,
    ) -> Decimal:
        """Simulate fee accrual for an LP position.

        This method estimates the fees earned by an LP position based on:
        - The position's fee tier (e.g., 0.3% for Uniswap V3)
        - The position's share of pool TVL (higher USD value = more fee capture)
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

        days_elapsed = self._lp_fee_days_elapsed(position, timestamp)
        if days_elapsed <= 0:
            return Decimal("0")

        fees_usd = self._lp_fee_estimate(position_value_usd, position.fee_tier, days_elapsed)
        self._apply_lp_fee_attribution(position, fees_usd, position_value_usd)
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

    def get_token_balance(self, token: TokenRef) -> Decimal:
        """Get balance of a specific token.

        Args:
            token: Token identity

        Returns:
            Amount held, or 0 if not held
        """
        return self.tokens.get(self._resolve_key(token), Decimal("0"))

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
            "chain": self.chain,
            "tokens": {token_ref_display(k): str(v) for k, v in self.tokens.items()},
            "positions": [p.to_dict() for p in self.positions],
            "equity_curve": [e.to_dict() for e in self.equity_curve],
            "price_series": [p.to_dict() for p in self.price_series],
            "trades": [t.to_dict() for t in self.trades],
            "closed_positions": [p.to_dict() for p in self._closed_positions],
            "initial_margin_ratio": str(self.initial_margin_ratio),
            "maintenance_margin_ratio": str(self.maintenance_margin_ratio),
            "max_margin_utilization": str(self._max_margin_utilization),
            "health_factor_warning_threshold": str(self.health_factor_warning_threshold),
            "min_health_factor": str(self._min_health_factor),
            "health_factor_warnings": self._health_factor_warnings,
            "liquidation_penalty": str(self.liquidation_penalty),
            # Realized PnL total is live attribution state: a portfolio resumed
            # after realized SWAP/close trades must report the accumulated
            # realized_pnl, not 0 until the next close (VIB-5083, CodeRabbit).
            "realized_pnl": str(self._realized_pnl),
            "unrealized_pnl": str(self._unrealized_pnl),
            "gas_tank_budget_usd": str(self.gas_tank_budget_usd) if self.gas_tank_budget_usd is not None else None,
            "gas_tank_spent_usd": str(self.gas_tank_spent_usd),
            "token_identities": {
                symbol: [identity.chain, identity.address] for symbol, identity in self._identity_table.items()
            },
            "lending_liquidations": [ll.to_dict() for ll in self._lending_liquidations],
            "perp_liquidations": [pl.to_dict() for pl in self._perp_liquidations],
            # Per-token average cost basis is live attribution state: without it
            # a resumed portfolio forgets its average costs, so a later
            # disposing sell would realize no PnL (VIB-5083, CodeRabbit).
            "cost_basis": {token_ref_display(k): str(v) for k, v in self._cost_basis.items()},
            # Numeraire reporting context (VIB-5127): a resumed non-USD run must
            # keep capturing/reporting against the same numeraire. None for USD.
            "numeraire_symbol": self._numeraire_symbol,
            "numeraire_token": token_ref_display(self._numeraire_token) if self._numeraire_token is not None else None,
        }

    @staticmethod
    def _equity_point_from_dict(data: dict[str, Any]) -> EquityPoint:
        return EquityPoint(
            timestamp=datetime.fromisoformat(data["timestamp"]),
            value_usd=Decimal(data["value_usd"]),
            eth_price_usd=None if data.get("eth_price_usd") is None else Decimal(str(data["eth_price_usd"])),
            spot_value_usd=None if data.get("spot_value_usd") is None else Decimal(str(data["spot_value_usd"])),
            position_value_usd=(
                None if data.get("position_value_usd") is None else Decimal(str(data["position_value_usd"]))
            ),
            valuation_source=data.get("valuation_source", "simple"),
            # Preserve the captured numeraire price (VIB-5127); absent /
            # null round-trips to None (a USD point).
            numeraire_price_usd=(
                None if data.get("numeraire_price_usd") is None else Decimal(str(data["numeraire_price_usd"]))
            ),
        )

    @staticmethod
    def _trade_record_from_dict(data: dict[str, Any]) -> TradeRecord:
        return TradeRecord(
            timestamp=datetime.fromisoformat(data["timestamp"]),
            intent_type=IntentType(data["intent_type"]),
            executed_price=Decimal(data["executed_price"]),
            fee_usd=Decimal(data["fee_usd"]),
            slippage_usd=Decimal(data["slippage_usd"]),
            gas_cost_usd=Decimal(data["gas_cost_usd"]),
            # pnl_usd is nullable (None = realized nothing yet, an opening /
            # inventory-building trade); a bare Decimal(None) would crash on
            # round-trip (VIB-5083, CodeRabbit).
            pnl_usd=None if data.get("pnl_usd") is None else Decimal(str(data["pnl_usd"])),
            success=data["success"],
            amount_usd=Decimal(data.get("amount_usd", "0")),
            protocol=data.get("protocol", ""),
            tokens=data.get("tokens", []),
            tx_hash=data.get("tx_hash"),
            error=data.get("error"),
            metadata=data.get("metadata", {}),
            actual_amount_in=None if data.get("actual_amount_in") is None else Decimal(str(data["actual_amount_in"])),
            actual_amount_out=None
            if data.get("actual_amount_out") is None
            else Decimal(str(data["actual_amount_out"])),
            expected_amount_in=(
                None if data.get("expected_amount_in") is None else Decimal(str(data["expected_amount_in"]))
            ),
            expected_amount_out=(
                None if data.get("expected_amount_out") is None else Decimal(str(data["expected_amount_out"]))
            ),
            il_loss_usd=None if data.get("il_loss_usd") is None else Decimal(str(data["il_loss_usd"])),
            fees_earned_usd=None if data.get("fees_earned_usd") is None else Decimal(str(data["fees_earned_usd"])),
            net_lp_pnl_usd=None if data.get("net_lp_pnl_usd") is None else Decimal(str(data["net_lp_pnl_usd"])),
            gas_price_gwei=None if data.get("gas_price_gwei") is None else Decimal(str(data["gas_price_gwei"])),
            estimated_mev_cost_usd=(
                None if data.get("estimated_mev_cost_usd") is None else Decimal(str(data["estimated_mev_cost_usd"]))
            ),
            delayed_at_end=bool(data.get("delayed_at_end", False)),
            position_id=data.get("position_id"),
        )

    @classmethod
    def _portfolio_base_from_dict(cls, data: dict[str, Any]) -> "SimulatedPortfolio":
        return cls(
            initial_capital_usd=Decimal(data.get("initial_capital_usd", "10000")),
            cash_usd=Decimal(data.get("cash_usd", "0")),
            chain=data.get("chain", DEFAULT_CHAIN),
            tokens={k: Decimal(v) for k, v in data.get("tokens", {}).items()},
            positions=[SimulatedPosition.from_dict(p) for p in data.get("positions", [])],
            equity_curve=[cls._equity_point_from_dict(e) for e in data.get("equity_curve", [])],
            price_series=[PricePoint.from_dict(p) for p in data.get("price_series", [])],
            trades=[cls._trade_record_from_dict(t) for t in data.get("trades", [])],
            _closed_positions=[SimulatedPosition.from_dict(p) for p in data.get("closed_positions", [])],
            initial_margin_ratio=Decimal(data.get("initial_margin_ratio", "0.1")),
            maintenance_margin_ratio=Decimal(data.get("maintenance_margin_ratio", "0.05")),
            health_factor_warning_threshold=Decimal(data.get("health_factor_warning_threshold", "1.2")),
            liquidation_penalty=Decimal(data.get("liquidation_penalty", "0.05")),
        )

    @staticmethod
    def _restored_realized_pnl(portfolio: "SimulatedPortfolio", data: dict[str, Any]) -> Decimal:
        if "realized_pnl" in data:
            return Decimal(str(data["realized_pnl"]))
        return sum((t.pnl_usd for t in portfolio.trades if t.success and t.pnl_usd is not None), Decimal("0"))

    @classmethod
    def _restore_portfolio_tracking(cls, portfolio: "SimulatedPortfolio", data: dict[str, Any]) -> None:
        portfolio._max_margin_utilization = Decimal(data.get("max_margin_utilization", "0"))
        portfolio._min_health_factor = Decimal(data.get("min_health_factor", "999"))
        portfolio._health_factor_warnings = data.get("health_factor_warnings", 0)
        portfolio._lending_liquidations = [
            LendingLiquidationEvent.from_dict(ll) for ll in data.get("lending_liquidations", [])
        ]
        portfolio._perp_liquidations = [LiquidationEvent.from_dict(pl) for pl in data.get("perp_liquidations", [])]
        # Restore per-token average cost basis so a resumed portfolio still
        # realizes PnL on later disposing sells (VIB-5083, CodeRabbit).
        portfolio._cost_basis = portfolio._normalize_amounts(
            {k: Decimal(str(v)) for k, v in data.get("cost_basis", {}).items()}
        )
        # Restore the numeraire reporting context (VIB-5127); absent -> None (USD).
        portfolio._numeraire_symbol = data.get("numeraire_symbol")
        numeraire_token = data.get("numeraire_token")
        portfolio._numeraire_token = (
            normalize_token_ref(numeraire_token, portfolio.chain) if isinstance(numeraire_token, str) else None
        )
        # Older artifacts predate realized_pnl; fall back to summing successful
        # realized trades so resumed portfolios stay consistent.
        portfolio._realized_pnl = cls._restored_realized_pnl(portfolio, data)
        portfolio._unrealized_pnl = Decimal(str(data.get("unrealized_pnl", "0")))
        budget = data.get("gas_tank_budget_usd")
        portfolio.gas_tank_budget_usd = Decimal(str(budget)) if budget is not None else None
        portfolio.gas_tank_spent_usd = Decimal(str(data.get("gas_tank_spent_usd", "0")))
        # Restore the identity table: a resumed run must keep resolving
        # symbol-shaped credits onto the registered plane (review, #3314).
        identities = data.get("token_identities") or {}
        portfolio.register_token_identities(
            {symbol: (entry[0], entry[1]) for symbol, entry in identities.items() if len(entry) == 2}
        )

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "SimulatedPortfolio":
        """Deserialize from dictionary.

        Args:
            data: Dictionary with serialized SimulatedPortfolio data

        Returns:
            SimulatedPortfolio instance
        """
        portfolio = cls._portfolio_base_from_dict(data)
        cls._restore_portfolio_tracking(portfolio, data)
        return portfolio


__all__ = [
    "PositionType",
    "SimulatedPosition",
    "SimulatedFill",
    "SimulatedPortfolio",
]
