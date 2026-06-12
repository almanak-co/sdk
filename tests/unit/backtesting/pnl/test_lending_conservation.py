"""Conservation-of-value invariants for the lending lane (VIB-5097).

A lending SUPPLY -> WITHDRAW round trip must conserve value: the withdrawn
principal comes OUT of the matched SUPPLY position. Before VIB-5097 the
WITHDRAW flow carried no position-close linkage in either lane (the lending
adapter's ``_execute_withdraw`` defers to generic execution, and generic
WITHDRAW fills had no ``position_close_id``), so the inflow credited the
portfolio while the open position kept counting in equity -- a $5,000
round trip on a $10,000 portfolio ended at ~$15,000.

The fixed semantics (engine ``_resolve_withdraw_close``):

- Matching mirrors the perp close pattern (PR #2751): exact-id precedence,
  then FIFO by (token, protocol) via ``find_lending_close_position_id``.
- Full withdraw (``withdraw_all``, unresolvable amount, or amount >=
  principal) closes the position and credits principal + accrued interest;
  the interest is the trade's realized PnL.
- Partial withdraw reduces the position's principal by exactly the fill's
  inflow token amounts; accrued interest stays on the position until it
  closes in full.
- A WITHDRAW with no matching open SUPPLY position is a failed fill with
  zero state mutation (apply_fill's validate-then-commit contract) --
  crediting the inflow from nothing would mint value.

Companion to ``test_perp_conservation.py`` (collateral lane) and
``test_portfolio_conservation.py`` (token-flow lanes).
"""

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from types import SimpleNamespace

import pytest

from almanak.framework.backtesting.adapters.lending_adapter import LendingBacktestAdapter
from almanak.framework.backtesting.models import EquityPoint, IntentType
from almanak.framework.backtesting.pnl.data_provider import MarketState
from almanak.framework.backtesting.pnl.engine import (
    DefaultFeeModel,
    DefaultSlippageModel,
    PnLBacktestConfig,
    PnLBacktester,
)
from almanak.framework.backtesting.pnl.intent_extraction import (
    find_lending_close_position_id,
)
from almanak.framework.backtesting.pnl.portfolio import (
    SimulatedFill,
    SimulatedPortfolio,
    SimulatedPosition,
)
from almanak.framework.intents.lending_intents import SupplyIntent, WithdrawIntent
from tests.unit.backtesting.pnl._mocks import MockDataProvider
from tests.validation.backtesting.trust_matrix import (
    INITIAL_CAPITAL,
    ScriptedStrategy,
    flat_series,
    run_backtest,
)

TS = datetime(2024, 1, 1, 0, 0, tzinfo=UTC)
INITIAL_CASH = Decimal("10000")
SUPPLY_AMOUNT = Decimal("5000")


def market(usdc: Decimal = Decimal("1")) -> MarketState:
    return MarketState(
        timestamp=TS,
        prices={"USDC": usdc, "WETH": Decimal("2000")},
        chain="arbitrum",
    )


def supply_position(
    amount: Decimal = SUPPLY_AMOUNT,
    token: str = "USDC",
    protocol: str = "aave_v3",
    entry_time: datetime = TS,
    interest_accrued: Decimal = Decimal("0"),
) -> SimulatedPosition:
    position = SimulatedPosition.supply(
        token=token,
        amount=amount,
        apy=Decimal("0.05"),
        entry_price=Decimal("1"),
        entry_time=entry_time,
        protocol=protocol,
    )
    position.interest_accrued = interest_accrued
    return position


def supply_intent(amount: Decimal = SUPPLY_AMOUNT) -> SupplyIntent:
    return SupplyIntent(protocol="aave_v3", token="USDC", amount=amount)


def withdraw_intent(amount: Decimal, withdraw_all: bool = False) -> WithdrawIntent:
    return WithdrawIntent(protocol="aave_v3", token="USDC", amount=amount, withdraw_all=withdraw_all)


def _backtester() -> PnLBacktester:
    return PnLBacktester(
        data_provider=MockDataProvider(),
        fee_models={"default": DefaultFeeModel(fee_pct=Decimal("0"))},
        slippage_models={"default": DefaultSlippageModel(slippage_pct=Decimal("0"))},
    )


def _config() -> PnLBacktestConfig:
    return PnLBacktestConfig(
        start_time=TS,
        end_time=TS + timedelta(hours=1),
        initial_capital_usd=INITIAL_CASH,
        include_gas_costs=False,
    )


def _exact_interest(principal: Decimal, hours: int) -> Decimal:
    """The exact interest both accrual lanes produce for the interval.

    Mirrors ``_mark_lending_position`` / ``LendingBacktestAdapter
    .update_position``: elapsed seconds -> days via ``Decimal(str(...))``,
    compound daily interest on the principal at apy 0.05 (the fixture APY,
    matching the engine's SUPPLY default).
    """
    from almanak.framework.backtesting.pnl.calculators.interest import InterestCalculator

    days = Decimal(str(timedelta(hours=hours).total_seconds())) / Decimal("86400")
    return InterestCalculator().calculate_interest(
        principal=principal,
        apy=Decimal("0.05"),
        time_delta=days,
        compound=True,
    ).interest


# =============================================================================
# Engine lane: SUPPLY -> WITHDRAW through PnLBacktester._execute_intent
# =============================================================================


class TestEngineWithdrawClose:
    """Decimal-exact conservation through the engine's generic lane.

    No mark_to_market runs between the two executions, so every equality is
    exact (zero fees / slippage / gas). Full closes realize only the interest
    already stamped on the position; a PARTIAL withdraw additionally accrues
    the interest earned through the fill instant on the pre-reduce principal
    (accrue-before-reduce, CodeRabbit PR #2758), which is computed exactly
    via ``_exact_interest``.
    """

    @pytest.mark.asyncio
    async def test_full_withdraw_round_trip_returns_initial_capital(self) -> None:
        backtester = _backtester()
        config = _config()
        portfolio = SimulatedPortfolio(initial_capital_usd=INITIAL_CASH)
        state = market()

        await backtester._execute_intent(supply_intent(), portfolio, state, TS, config)

        assert len(portfolio.positions) == 1
        assert portfolio.cash_usd == INITIAL_CASH - SUPPLY_AMOUNT
        assert portfolio.get_total_value_usd(state) == INITIAL_CASH

        await backtester._execute_intent(
            withdraw_intent(SUPPLY_AMOUNT, withdraw_all=True),
            portfolio,
            state,
            TS + timedelta(hours=1),
            config,
        )

        assert portfolio.positions == []
        assert len(portfolio._closed_positions) == 1
        assert portfolio.cash_usd == INITIAL_CASH
        assert portfolio.get_total_value_usd(state) == INITIAL_CASH
        assert all(trade.success for trade in portfolio.trades)

    @pytest.mark.asyncio
    async def test_exact_amount_withdraw_closes_in_full(self) -> None:
        """amount == principal (withdraw_all=False) is a full close, no dust position."""
        backtester = _backtester()
        config = _config()
        portfolio = SimulatedPortfolio(initial_capital_usd=INITIAL_CASH)
        state = market()

        await backtester._execute_intent(supply_intent(), portfolio, state, TS, config)
        await backtester._execute_intent(
            withdraw_intent(SUPPLY_AMOUNT),
            portfolio,
            state,
            TS + timedelta(hours=1),
            config,
        )

        assert portfolio.positions == []
        assert portfolio.cash_usd == INITIAL_CASH
        assert portfolio.get_total_value_usd(state) == INITIAL_CASH

    @pytest.mark.asyncio
    async def test_partial_withdraw_reduces_position_principal(self) -> None:
        backtester = _backtester()
        config = _config()
        portfolio = SimulatedPortfolio(initial_capital_usd=INITIAL_CASH)
        state = market()
        partial = Decimal("2000")

        await backtester._execute_intent(supply_intent(), portfolio, state, TS, config)
        await backtester._execute_intent(
            withdraw_intent(partial),
            portfolio,
            state,
            TS + timedelta(hours=1),
            config,
        )

        assert len(portfolio.positions) == 1
        position = portfolio.positions[0]
        assert position.amounts["USDC"] == SUPPLY_AMOUNT - partial
        assert portfolio._closed_positions == []
        assert portfolio.cash_usd == INITIAL_CASH - SUPPLY_AMOUNT + partial
        # The hour between supply and withdraw accrues on the FULL principal
        # at the fill instant (accrue-before-reduce); equity grows by exactly
        # that yield and nothing else.
        accrued = _exact_interest(SUPPLY_AMOUNT, hours=1)
        assert position.interest_accrued == accrued
        assert portfolio.get_total_value_usd(state) == INITIAL_CASH + accrued
        assert all(trade.success for trade in portfolio.trades)

    @pytest.mark.asyncio
    async def test_full_withdraw_realizes_accrued_interest_as_pnl(self) -> None:
        """Interest accrued up to the withdraw is credited and realized as PnL."""
        backtester = _backtester()
        config = _config()
        portfolio = SimulatedPortfolio(initial_capital_usd=INITIAL_CASH)
        state = market()
        interest = Decimal("25")

        await backtester._execute_intent(supply_intent(), portfolio, state, TS, config)
        # Interest accrual is owned by the mark/update paths; stamp it the
        # way a mark would have.
        portfolio.positions[0].interest_accrued = interest

        await backtester._execute_intent(
            withdraw_intent(SUPPLY_AMOUNT, withdraw_all=True),
            portfolio,
            state,
            TS + timedelta(hours=1),
            config,
        )

        assert portfolio.positions == []
        assert portfolio.cash_usd == INITIAL_CASH + interest
        assert portfolio.get_total_value_usd(state) == INITIAL_CASH + interest
        withdraw_trade = portfolio.trades[-1]
        assert withdraw_trade.success
        assert withdraw_trade.pnl_usd == interest
        assert withdraw_trade.amount_usd == SUPPLY_AMOUNT + interest
        assert portfolio._realized_pnl == interest

    @pytest.mark.asyncio
    async def test_withdraw_beyond_principal_caps_to_position(self) -> None:
        """Withdrawing more than supplied cannot mint the difference."""
        backtester = _backtester()
        config = _config()
        portfolio = SimulatedPortfolio(initial_capital_usd=INITIAL_CASH)
        state = market()

        await backtester._execute_intent(supply_intent(), portfolio, state, TS, config)
        await backtester._execute_intent(
            withdraw_intent(Decimal("9000")),
            portfolio,
            state,
            TS + timedelta(hours=1),
            config,
        )

        assert portfolio.positions == []
        assert portfolio.cash_usd == INITIAL_CASH
        assert portfolio.get_total_value_usd(state) == INITIAL_CASH

    @pytest.mark.asyncio
    async def test_withdraw_without_open_supply_is_rejected(self) -> None:
        """No matching open SUPPLY position = failed fill, zero mutation."""
        backtester = _backtester()
        config = _config()
        portfolio = SimulatedPortfolio(initial_capital_usd=INITIAL_CASH)
        state = market()

        await backtester._execute_intent(
            withdraw_intent(SUPPLY_AMOUNT),
            portfolio,
            state,
            TS,
            config,
        )

        assert portfolio.positions == []
        assert portfolio.tokens == {}
        assert portfolio.cash_usd == INITIAL_CASH
        assert portfolio.get_total_value_usd(state) == INITIAL_CASH
        trade = portfolio.trades[-1]
        assert trade.success is False
        assert "no open supply position" in trade.metadata["failure_reason"]
        # Rejected fills charge nothing (costs zeroed by _record_failed_fill).
        assert trade.fee_usd == Decimal("0")
        assert trade.gas_cost_usd == Decimal("0")

    @pytest.mark.asyncio
    async def test_withdraw_other_token_does_not_cross_match(self) -> None:
        """A WETH withdraw must not close a USDC supply position."""
        backtester = _backtester()
        config = _config()
        portfolio = SimulatedPortfolio(initial_capital_usd=INITIAL_CASH)
        state = market()

        await backtester._execute_intent(supply_intent(), portfolio, state, TS, config)
        await backtester._execute_intent(
            WithdrawIntent(protocol="aave_v3", token="WETH", amount=Decimal("1")),
            portfolio,
            state,
            TS + timedelta(hours=1),
            config,
        )

        assert len(portfolio.positions) == 1
        assert portfolio.positions[0].amounts["USDC"] == SUPPLY_AMOUNT
        assert portfolio.trades[-1].success is False
        assert portfolio.get_total_value_usd(state) == INITIAL_CASH


# =============================================================================
# Matching semantics: find_lending_close_position_id
# =============================================================================


class TestLendingCloseMatching:
    """Exact-id precedence, then FIFO by (token, protocol) -- PR #2751 pattern."""

    def test_fifo_matches_oldest_supply(self) -> None:
        older = supply_position(entry_time=TS)
        newer = supply_position(entry_time=TS + timedelta(hours=1))

        matched = find_lending_close_position_id(withdraw_intent(SUPPLY_AMOUNT), [newer, older])

        assert matched == older.position_id

    def test_exact_position_id_takes_precedence_over_fifo(self) -> None:
        older = supply_position(entry_time=TS)
        newer = supply_position(entry_time=TS + timedelta(hours=1))
        intent = withdraw_intent(SUPPLY_AMOUNT)
        object.__setattr__(intent, "position_id", newer.position_id)

        assert find_lending_close_position_id(intent, [older, newer]) == newer.position_id

    def test_token_mismatch_returns_none(self) -> None:
        position = supply_position(token="WETH")

        assert find_lending_close_position_id(withdraw_intent(SUPPLY_AMOUNT), [position]) is None

    def test_protocol_mismatch_returns_none(self) -> None:
        position = supply_position(protocol="compound_v3")

        assert find_lending_close_position_id(withdraw_intent(SUPPLY_AMOUNT), [position]) is None

    def test_borrow_positions_are_not_withdraw_targets(self) -> None:
        borrow = SimulatedPosition.borrow(
            token="USDC",
            amount=SUPPLY_AMOUNT,
            apy=Decimal("0.08"),
            entry_price=Decimal("1"),
            entry_time=TS,
            protocol="aave_v3",
        )

        assert find_lending_close_position_id(withdraw_intent(SUPPLY_AMOUNT), [borrow]) is None

    def test_explicit_id_naming_borrow_fails_closed(self) -> None:
        """An explicit id pointing at a BORROW is refused outright -- it must
        not be honored, and it must not fall through to FIFO either (the
        producer is confused; draining any position would be a guess)."""
        borrow = SimulatedPosition.borrow(
            token="USDC",
            amount=SUPPLY_AMOUNT,
            apy=Decimal("0.08"),
            entry_price=Decimal("1"),
            entry_time=TS,
            protocol="aave_v3",
        )
        supply = supply_position()
        intent = withdraw_intent(SUPPLY_AMOUNT)
        object.__setattr__(intent, "position_id", borrow.position_id)

        assert find_lending_close_position_id(intent, [borrow, supply]) is None

    def test_tokenless_intent_fails_closed(self) -> None:
        """No token/asset on the intent = no protocol-only fallback matching."""
        intent = SimpleNamespace(protocol="aave_v3", amount=SUPPLY_AMOUNT)

        assert find_lending_close_position_id(intent, [supply_position()]) is None

    def test_two_same_token_supplies_still_fifo_match_oldest(self) -> None:
        """FIFO-oldest among multiple (token, protocol) matches is the
        documented contract (PR #2751 perps pattern) -- multiple matches are
        NOT ambiguous-and-refused, they target the oldest with a warning."""
        older = supply_position(entry_time=TS)
        newer = supply_position(entry_time=TS + timedelta(hours=1))

        assert find_lending_close_position_id(withdraw_intent(SUPPLY_AMOUNT), [newer, older]) == older.position_id


# =============================================================================
# Portfolio reduce contract: validate-then-commit
# =============================================================================


def _reduce_fill(
    position_id: str,
    amounts: dict[str, Decimal],
    timestamp: datetime = TS,
) -> SimulatedFill:
    total = sum(amounts.values(), Decimal("0"))
    return SimulatedFill(
        timestamp=timestamp,
        intent_type=IntentType.WITHDRAW,
        protocol="aave_v3",
        tokens=list(amounts),
        executed_price=Decimal("1"),
        amount_usd=total,
        fee_usd=Decimal("0"),
        slippage_usd=Decimal("0"),
        gas_cost_usd=Decimal("0"),
        tokens_in=dict(amounts),
        tokens_out={},
        success=True,
        position_reduce_id=position_id,
        position_reduce_amounts=dict(amounts),
    )


class TestApplyFillPositionReduce:
    def test_reduce_commits_principal_and_inflow_together(self) -> None:
        portfolio = SimulatedPortfolio(initial_capital_usd=INITIAL_CASH)
        position = supply_position()
        portfolio.positions.append(position)
        portfolio.cash_usd = INITIAL_CASH - SUPPLY_AMOUNT

        applied = portfolio.apply_fill(_reduce_fill(position.position_id, {"USDC": Decimal("2000")}))

        assert applied is True
        assert position.amounts["USDC"] == Decimal("3000")
        assert portfolio.cash_usd == INITIAL_CASH - SUPPLY_AMOUNT + Decimal("2000")
        assert portfolio.get_total_value_usd(market()) == INITIAL_CASH
        # The TradeRecord carries the reduced position's id, the same way a
        # full close carries position_close_id (CodeRabbit, PR #2758).
        assert portfolio.trades[-1].position_id == position.position_id

    def test_empty_reduce_map_rejects_fill_without_mutation(self) -> None:
        """A credited inflow with an empty reduce map is under-reduction: minting."""
        portfolio = SimulatedPortfolio(initial_capital_usd=INITIAL_CASH)
        position = supply_position()
        portfolio.positions.append(position)

        fill = _reduce_fill(position.position_id, {"USDC": Decimal("2000")})
        fill.position_reduce_amounts = {}

        assert portfolio.apply_fill(fill) is False
        assert position.amounts["USDC"] == SUPPLY_AMOUNT
        assert portfolio.cash_usd == INITIAL_CASH
        assert portfolio.tokens == {}
        assert "missing positive partial-reduction amounts" in portfolio.trades[-1].metadata["failure_reason"]

    def test_under_reduction_rejects_fill_without_mutation(self) -> None:
        """Reducing less than the credited inflow mints the difference."""
        portfolio = SimulatedPortfolio(initial_capital_usd=INITIAL_CASH)
        position = supply_position()
        portfolio.positions.append(position)

        fill = _reduce_fill(position.position_id, {"USDC": Decimal("2000")})
        fill.position_reduce_amounts = {"USDC": Decimal("1500")}

        assert portfolio.apply_fill(fill) is False
        assert position.amounts["USDC"] == SUPPLY_AMOUNT
        assert portfolio.cash_usd == INITIAL_CASH
        assert "but reduces" in portfolio.trades[-1].metadata["failure_reason"]

    def test_reduce_token_mismatch_rejects_fill_without_mutation(self) -> None:
        """The reduce map must debit the same tokens the fill credits."""
        portfolio = SimulatedPortfolio(initial_capital_usd=INITIAL_CASH)
        position = supply_position()
        portfolio.positions.append(position)

        fill = _reduce_fill(position.position_id, {"USDC": Decimal("2000")})
        fill.position_reduce_amounts = {"WETH": Decimal("1")}

        assert portfolio.apply_fill(fill) is False
        assert position.amounts["USDC"] == SUPPLY_AMOUNT
        assert "do not match credited inflow tokens" in portfolio.trades[-1].metadata["failure_reason"]

    def test_reduce_of_missing_position_rejects_fill_without_mutation(self) -> None:
        portfolio = SimulatedPortfolio(initial_capital_usd=INITIAL_CASH)

        applied = portfolio.apply_fill(_reduce_fill("SUPPLY_aave_v3_USDC_0", {"USDC": Decimal("2000")}))

        assert applied is False
        assert portfolio.cash_usd == INITIAL_CASH
        assert portfolio.tokens == {}
        trade = portfolio.trades[-1]
        assert trade.success is False
        assert "not found for partial reduction" in trade.metadata["failure_reason"]

    def test_reduce_beyond_held_amount_rejects_fill_without_mutation(self) -> None:
        portfolio = SimulatedPortfolio(initial_capital_usd=INITIAL_CASH)
        position = supply_position()
        portfolio.positions.append(position)
        portfolio.cash_usd = INITIAL_CASH - SUPPLY_AMOUNT

        applied = portfolio.apply_fill(_reduce_fill(position.position_id, {"USDC": Decimal("6000")}))

        assert applied is False
        assert position.amounts["USDC"] == SUPPLY_AMOUNT
        assert portfolio.cash_usd == INITIAL_CASH - SUPPLY_AMOUNT
        assert "cannot reduce by" in portfolio.trades[-1].metadata["failure_reason"]


# =============================================================================
# Accrue-before-reduce: interest timing across a partial WITHDRAW
# =============================================================================


class TestAccrueBeforeReduce:
    """A partial WITHDRAW accrues interest through the fill instant FIRST.

    Pending intents execute before the per-tick adapter update and
    ``mark_to_market`` at the same timestamp
    (``pnl/_engine_helpers.execute_iteration_loop``), so without
    accrue-before-reduce the tick containing a partial withdraw accrues its
    whole interval on the POST-reduction principal -- under-accruing the
    interest the withdrawn slice earned before the withdraw (CodeRabbit,
    PR #2758). These tests pin the exact interest attributed to the
    interval ending at the partial-withdraw timestamp, in both lanes.
    """

    def test_generic_lane_accrues_interval_on_pre_reduce_principal(self) -> None:
        portfolio = SimulatedPortfolio(initial_capital_usd=INITIAL_CASH)
        position = supply_position()
        portfolio.positions.append(position)
        portfolio.cash_usd = INITIAL_CASH - SUPPLY_AMOUNT

        # Tick 1: the mark accrues [TS, TS+1h] on the full principal.
        portfolio.mark_to_market(market(), TS + timedelta(hours=1))
        assert position.interest_accrued == _exact_interest(SUPPLY_AMOUNT, hours=1)

        # Tick 2: the partial withdraw lands BEFORE the tick's mark. The
        # interval [TS+1h, TS+2h] must accrue on the FULL $5k principal at
        # the fill instant -- not on the post-reduce $3k at the next mark.
        fill = _reduce_fill(
            position.position_id,
            {"USDC": Decimal("2000")},
            timestamp=TS + timedelta(hours=2),
        )
        assert portfolio.apply_fill(fill, market_state=market()) is True

        expected = _exact_interest(SUPPLY_AMOUNT, hours=1) * 2
        assert position.interest_accrued == expected
        assert position.last_updated == TS + timedelta(hours=2)
        assert position.amounts["USDC"] == Decimal("3000")

        # The same-tick mark accrues nothing further (elapsed == 0).
        portfolio.mark_to_market(market(), TS + timedelta(hours=2))
        assert position.interest_accrued == expected

    def test_adapter_lane_accrues_interval_on_pre_reduce_principal(self) -> None:
        adapter = LendingBacktestAdapter()
        portfolio = SimulatedPortfolio(initial_capital_usd=INITIAL_CASH)
        position = supply_position()
        portfolio.positions.append(position)
        portfolio.cash_usd = INITIAL_CASH - SUPPLY_AMOUNT

        fill = _reduce_fill(
            position.position_id,
            {"USDC": Decimal("2000")},
            timestamp=TS + timedelta(hours=1),
        )
        assert portfolio.apply_fill(fill, market_state=market(), adapter=adapter) is True

        # The adapter accrued [TS, TS+1h] on the full principal with its own
        # rate path (position-entry APY here), and stamped last_updated.
        assert position.interest_accrued == _exact_interest(SUPPLY_AMOUNT, hours=1)
        assert position.last_updated == TS + timedelta(hours=1)
        assert position.amounts["USDC"] == Decimal("3000")

    def test_same_tick_adapter_update_does_not_double_accrue(self) -> None:
        """The end-of-tick adapter update clamps elapsed to last_updated,
        so the interval accrued at the fill is not re-accrued on the
        reduced principal."""
        backtester = _backtester()
        backtester._adapter = LendingBacktestAdapter()
        portfolio = SimulatedPortfolio(initial_capital_usd=INITIAL_CASH)
        position = supply_position()
        portfolio.positions.append(position)
        portfolio.equity_curve.append(EquityPoint(timestamp=TS, value_usd=INITIAL_CASH))

        tick = TS + timedelta(hours=1)
        fill = _reduce_fill(position.position_id, {"USDC": Decimal("2000")}, timestamp=tick)
        assert portfolio.apply_fill(fill, market_state=market(), adapter=backtester._adapter) is True
        accrued_at_fill = position.interest_accrued
        assert accrued_at_fill == _exact_interest(SUPPLY_AMOUNT, hours=1)

        backtester._update_positions_via_adapter(portfolio, market(), tick)

        assert position.interest_accrued == accrued_at_fill


# =============================================================================
# Real engine loop, both lanes (trust-matrix harness, network-free)
# =============================================================================


def _round_trip_intents(withdraw_amount: Decimal, withdraw_all: bool) -> list:
    return [
        SupplyIntent(protocol="aave_v3", token="USDC", amount=SUPPLY_AMOUNT),
        None,
        WithdrawIntent(protocol="aave_v3", token="USDC", amount=withdraw_amount, withdraw_all=withdraw_all),
    ]


class TestEngineLoopBothLanes:
    """SUPPLY -> WITHDRAW through the REAL engine iteration loop.

    ``strategy_type=None`` exercises the generic lane;
    ``strategy_type="lending"`` wires the LendingBacktestAdapter (which
    defers fill construction to the generic lane but owns per-tick interest
    accrual and valuation). Equity drift must equal the withdraw trade's
    realized interest PnL exactly -- the principal must not double-count.
    """

    @pytest.mark.parametrize("strategy_type", [None, "lending"])
    def test_full_round_trip_conserves_value(self, strategy_type: str | None) -> None:
        result = run_backtest(
            ScriptedStrategy(_round_trip_intents(SUPPLY_AMOUNT, withdraw_all=True)),
            flat_series(10),
            hours=6,
            strategy_type=strategy_type,
        )

        assert result.success
        assert result.metrics.total_trades == 2
        assert all(trade.success for trade in result.trades)
        drift = result.final_capital_usd - INITIAL_CAPITAL
        # The only equity source is interest accrued while the supply was
        # open (a few cents over a few hours); it is realized on close.
        assert drift == result.trades[-1].pnl_usd
        assert Decimal("0") < drift < Decimal("1")

    @pytest.mark.parametrize("strategy_type", [None, "lending"])
    def test_partial_withdraw_conserves_value(self, strategy_type: str | None) -> None:
        result = run_backtest(
            ScriptedStrategy(_round_trip_intents(Decimal("2000"), withdraw_all=False)),
            flat_series(10),
            hours=6,
            strategy_type=strategy_type,
        )

        assert result.success
        assert result.metrics.total_trades == 2
        assert all(trade.success for trade in result.trades)
        # Equity may only grow by accrued interest (position stays open on
        # the remaining $3k principal) -- never by the withdrawn $2k.
        drift = result.final_capital_usd - INITIAL_CAPITAL
        assert Decimal("0") < drift < Decimal("1")

    @pytest.mark.parametrize("strategy_type", [None, "lending"])
    def test_withdraw_without_supply_is_rejected_with_zero_mutation(self, strategy_type: str | None) -> None:
        result = run_backtest(
            ScriptedStrategy([WithdrawIntent(protocol="aave_v3", token="USDC", amount=SUPPLY_AMOUNT)]),
            flat_series(10),
            hours=6,
            strategy_type=strategy_type,
        )

        assert result.success
        assert result.metrics.total_trades == 1
        trade = result.trades[0]
        assert trade.success is False
        assert "no open supply position" in trade.metadata["failure_reason"]
        assert result.final_capital_usd == INITIAL_CAPITAL
        assert all(point.value_usd == INITIAL_CAPITAL for point in result.equity_curve)


class TestProtocolScopedMatching:
    """A protocol-scoped WITHDRAW must not drain a position with no protocol stamp.

    Gemini review on PR #2758: the permissive shape
    ``protocol and position.protocol and ...`` let a ``protocol=None``
    position satisfy an intent that explicitly names a protocol.
    """

    def test_missing_protocol_position_does_not_match_protocol_intent(self) -> None:
        unstamped = supply_position()
        unstamped.protocol = None

        assert find_lending_close_position_id(withdraw_intent(SUPPLY_AMOUNT), [unstamped]) is None

    def test_missing_protocol_position_still_matches_protocol_free_intent(self) -> None:
        unstamped = supply_position()
        unstamped.protocol = None
        intent = WithdrawIntent(protocol="default", token="USDC", amount=SUPPLY_AMOUNT)

        assert find_lending_close_position_id(intent, [unstamped]) == unstamped.position_id


class TestZeroPrincipalPosition:
    """An empty supply position is not a FIFO trap: it full-closes for its interest.

    Gemini review on PR #2758 suggested auto-closing a position whose
    principal hits zero after a partial reduce. That would DROP accrued
    interest from equity without realizing it (a conservation violation in
    the opposite direction). The value-correct behavior, pinned here: the
    withdraw router resolves any withdraw against a zero-principal position
    as a FULL close whose notional is exactly the accrued interest.
    """

    def test_withdraw_from_zero_principal_position_full_closes_for_interest(self) -> None:
        interest = Decimal("12.34")
        empty = supply_position(amount=Decimal("0"), interest_accrued=interest)
        portfolio = SimulatedPortfolio(initial_capital_usd=INITIAL_CAPITAL)
        portfolio.positions.append(empty)
        backtester = _backtester()

        resolution = backtester._resolve_withdraw_close(
            withdraw_intent(Decimal("100")), portfolio, Decimal("100"), market()
        )

        assert resolution.position_close_id == empty.position_id
        assert resolution.position_reduce_id is None
        assert resolution.amount_usd == interest
        assert resolution.interest_usd == interest
