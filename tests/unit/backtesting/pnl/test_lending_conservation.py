"""Conservation-of-value invariants for the lending lane (VIB-5097, VIB-5098).

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

The debt side mirrors the same mechanics (VIB-5098). Before that fix BORROW
was an economic no-op (``BorrowIntent.borrow_amount`` was invisible to the
engine's amount extraction, so no cash inflow and a zero-amount debt
position) and REPAY burned cash (the outflow debited cash but never closed
or reduced the matched BORROW position -- a $2,000 repay lost ~$2,000 of
equity). The fixed semantics (engine ``_resolve_repay_close``):

- BORROW credits the borrowed token (swept to cash when stable) AND opens a
  BORROW-type position whose valuation SUBTRACTS debt, so the open is
  equity-neutral minus costs.
- Full repay (``repay_full``, unresolvable amount, or amount >= debt
  principal) closes the matched BORROW position and debits debt principal +
  accrued borrow interest; the interest realizes as NEGATIVE PnL.
- Partial repay reduces the debt principal by exactly the fill's outflow
  token amounts; accrued borrow interest stays on the position until it
  closes in full.
- A REPAY with no matching open BORROW position is a failed fill with zero
  state mutation -- debiting cash with no debt to extinguish burns value.

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
from almanak.framework.intents.lending_intents import (
    BorrowIntent,
    RepayIntent,
    SupplyIntent,
    WithdrawIntent,
)
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
BORROW_AMOUNT = Decimal("2000")
#: The engine's default borrow APY (_borrow_delta) for intents without one.
BORROW_APY = Decimal("0.08")


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


def borrow_position(
    amount: Decimal = BORROW_AMOUNT,
    token: str = "USDC",
    protocol: str = "aave_v3",
    entry_time: datetime = TS,
    interest_accrued: Decimal = Decimal("0"),
) -> SimulatedPosition:
    position = SimulatedPosition.borrow(
        token=token,
        amount=amount,
        apy=BORROW_APY,
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


def borrow_intent(amount: Decimal = BORROW_AMOUNT, token: str = "USDC") -> BorrowIntent:
    return BorrowIntent(
        protocol="aave_v3",
        collateral_token="USDC",
        collateral_amount=Decimal("0"),
        borrow_token=token,
        borrow_amount=amount,
    )


def repay_intent(amount: Decimal, repay_full: bool = False, token: str = "USDC") -> RepayIntent:
    return RepayIntent(protocol="aave_v3", token=token, amount=amount, repay_full=repay_full)


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


def _exact_interest(principal: Decimal, hours: int, apy: Decimal = Decimal("0.05")) -> Decimal:
    """The exact interest both accrual lanes produce for the interval.

    Mirrors ``_mark_lending_position`` / ``LendingBacktestAdapter
    .update_position``: elapsed seconds -> days via ``Decimal(str(...))``,
    compound daily interest on the principal at the position's entry APY
    (0.05 = the engine's SUPPLY default, 0.08 = its BORROW default).
    """
    from almanak.framework.backtesting.pnl.calculators.interest import InterestCalculator

    days = Decimal(str(timedelta(hours=hours).total_seconds())) / Decimal("86400")
    return (
        InterestCalculator()
        .calculate_interest(
            principal=principal,
            apy=apy,
            time_delta=days,
            compound=True,
        )
        .interest
    )


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
    async def test_boundary_withdraw_takes_all_principal_and_partial_interest(self) -> None:
        """A withdraw in (principal, principal + interest) removes ALL principal
        and realizes only the WITHDRAWN interest; the position stays open
        carrying the remainder (VIB-5098 / PR #2777 mirror of the repay fix)."""
        backtester = _backtester()
        config = _config()
        portfolio = SimulatedPortfolio(initial_capital_usd=INITIAL_CASH)
        state = market()
        interest = Decimal("25")

        await backtester._execute_intent(supply_intent(), portfolio, state, TS, config)
        # Stamp earned interest the way a mark would; withdraw at the SAME
        # instant so accrue-before-reduce is a no-op and the math is exact.
        portfolio.positions[0].interest_accrued = interest

        # Covers all $5,000 principal + $10 of the $25 earned interest.
        await backtester._execute_intent(
            withdraw_intent(SUPPLY_AMOUNT + Decimal("10")), portfolio, state, TS, config
        )

        assert len(portfolio.positions) == 1
        position = portfolio.positions[0]
        assert position.amounts.get("USDC", Decimal("0")) == Decimal("0")
        assert position.interest_accrued == Decimal("15")  # 25 - 10 withdrawn
        assert portfolio._closed_positions == []
        # Cash in equals the REQUESTED withdraw -- not the full balance (the old
        # bug over-credited to $5,025 here).
        assert portfolio.cash_usd == INITIAL_CASH - SUPPLY_AMOUNT + (SUPPLY_AMOUNT + Decimal("10"))
        assert portfolio._realized_pnl == Decimal("10")
        withdraw_trade = portfolio.trades[-1]
        assert withdraw_trade.success
        assert withdraw_trade.pnl_usd == Decimal("10")
        assert withdraw_trade.amount_usd == SUPPLY_AMOUNT + Decimal("10")
        # Equity drift is the full earned interest ($25): $10 realized + $15
        # still on the open position. Principal is conserved.
        assert portfolio.get_total_value_usd(state) == INITIAL_CASH + interest

    @pytest.mark.asyncio
    async def test_withdraw_covering_principal_only_defers_interest(self) -> None:
        """Withdrawing EXACTLY the principal (interest earned) is a sub-principal
        partial: principal -> 0, interest deferred (not over-credited)."""
        backtester = _backtester()
        config = _config()
        portfolio = SimulatedPortfolio(initial_capital_usd=INITIAL_CASH)
        state = market()
        interest = Decimal("25")

        await backtester._execute_intent(supply_intent(), portfolio, state, TS, config)
        portfolio.positions[0].interest_accrued = interest

        await backtester._execute_intent(withdraw_intent(SUPPLY_AMOUNT), portfolio, state, TS, config)

        assert len(portfolio.positions) == 1
        position = portfolio.positions[0]
        assert position.amounts.get("USDC", Decimal("0")) == Decimal("0")
        assert position.interest_accrued == interest  # fully deferred
        assert portfolio._realized_pnl == Decimal("0")
        # Cash in == principal exactly (the old bug closed and credited $5,025).
        assert portfolio.cash_usd == INITIAL_CASH
        assert portfolio.get_total_value_usd(state) == INITIAL_CASH + interest

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

    def test_explicit_unknown_id_fails_closed_without_fifo_fallback(self) -> None:
        """A typoed/stale explicit id must NOT fall through to FIFO and
        withdraw the oldest supply for that token (CodeRabbit, PR #2777):
        exact-id intent => exact-id match or nothing."""
        older = supply_position(entry_time=TS)
        newer = supply_position(entry_time=TS + timedelta(hours=1))
        intent = withdraw_intent(SUPPLY_AMOUNT)
        object.__setattr__(intent, "position_id", "SUPPLY_aave_v3_USDC_does_not_exist")

        assert find_lending_close_position_id(intent, [older, newer]) is None

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
        # Rejected fill is recorded but not counted as a trade; it
        # surfaces as failed_trades (VIB-5083, CodeRabbit).
        assert result.metrics.total_trades == 0
        assert result.metrics.failed_trades == 1
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
    """An empty supply position is not a FIFO trap.

    Gemini review on PR #2758 suggested auto-closing a position whose
    principal hits zero after a partial reduce. That would DROP accrued
    interest from equity without realizing it (a conservation violation in
    the opposite direction). The value-correct behavior, pinned here: a
    withdraw that covers the accrued interest FULL-closes for exactly that
    interest; a withdraw that does NOT cover it is rejected (zero mutation)
    rather than force-closed for the whole interest, which would credit more
    than the requested amount (CodeRabbit PR #2777 round 2).
    """

    def test_withdraw_covering_interest_of_zero_principal_position_full_closes(self) -> None:
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

    def test_withdraw_below_interest_of_zero_principal_position_is_rejected(self) -> None:
        """A withdraw that does not cover the full earned interest of an
        interest-only position is rejected -- NOT force-closed for the whole
        interest (the over-credit mirror of the REPAY fix)."""
        interest = Decimal("12.34")
        empty = supply_position(amount=Decimal("0"), interest_accrued=interest)
        portfolio = SimulatedPortfolio(initial_capital_usd=INITIAL_CAPITAL)
        portfolio.positions.append(empty)
        backtester = _backtester()

        resolution = backtester._resolve_withdraw_close(
            withdraw_intent(Decimal("5")), portfolio, Decimal("5"), market()
        )

        assert resolution.position_close_id is None
        assert resolution.position_reduce_id is None
        assert resolution.failure_reason is not None
        assert "interest-only" in resolution.failure_reason


# =============================================================================
# Debt side (VIB-5098): BORROW opens debt, REPAY extinguishes it
# =============================================================================


class TestEngineBorrowOpensDebt:
    """BORROW credits cash AND opens a debt position: equity-neutral entry.

    Pre-VIB-5098, ``BorrowIntent.borrow_amount`` was invisible to the
    engine's amount extraction, so BORROW produced a $0 fill: no cash
    inflow, a zero-amount debt position -- leverage loops backtested as if
    borrows never happened.
    """

    @pytest.mark.asyncio
    async def test_borrow_credits_cash_and_opens_debt(self) -> None:
        backtester = _backtester()
        config = _config()
        portfolio = SimulatedPortfolio(initial_capital_usd=INITIAL_CASH)
        state = market()

        await backtester._execute_intent(supply_intent(), portfolio, state, TS, config)
        await backtester._execute_intent(borrow_intent(), portfolio, state, TS, config)

        borrow_trade = portfolio.trades[-1]
        assert borrow_trade.success
        assert borrow_trade.amount_usd == BORROW_AMOUNT
        # Cash: -supply +borrow; the borrowed USDC sweeps into cash_usd.
        assert portfolio.cash_usd == INITIAL_CASH - SUPPLY_AMOUNT + BORROW_AMOUNT
        debts = [p for p in portfolio.positions if p.position_type.value == "BORROW"]
        assert len(debts) == 1
        assert debts[0].amounts["USDC"] == BORROW_AMOUNT
        # Debt subtracts from equity: the borrow is equity-neutral.
        assert portfolio._get_position_value(debts[0], state) == -BORROW_AMOUNT
        assert portfolio.get_total_value_usd(state) == INITIAL_CASH

    @pytest.mark.asyncio
    async def test_borrow_prices_the_borrow_token_not_the_collateral_token(self) -> None:
        """A WETH borrow against USDC collateral is sized at the WETH price.

        Pre-fix the token scan in ``get_intent_amount_usd`` would have hit
        ``collateral_token`` first; the borrow leg must price ``borrow_token``.
        """
        backtester = _backtester()
        config = _config()
        portfolio = SimulatedPortfolio(initial_capital_usd=INITIAL_CASH)
        state = market()

        await backtester._execute_intent(supply_intent(), portfolio, state, TS, config)
        await backtester._execute_intent(borrow_intent(amount=Decimal("1"), token="WETH"), portfolio, state, TS, config)

        borrow_trade = portfolio.trades[-1]
        assert borrow_trade.success
        assert borrow_trade.amount_usd == Decimal("2000")  # 1 WETH @ $2000
        assert portfolio.tokens["WETH"] == Decimal("1")
        debts = [p for p in portfolio.positions if p.position_type.value == "BORROW"]
        assert len(debts) == 1
        assert debts[0].amounts["WETH"] == Decimal("1")
        assert portfolio.get_total_value_usd(state) == INITIAL_CASH

    def test_adapter_lane_mark_subtracts_debt(self) -> None:
        """The adapter lane's mark must SUBTRACT debt, like every other path.

        ``LendingBacktestAdapter.value_position`` returns the debt MAGNITUDE
        for BORROW positions by contract ("the portfolio handles subtracting
        borrows"); pre-VIB-5098 ``mark_to_market`` added it, so every open
        borrow inflated adapter-lane equity by 2x the debt.
        """
        adapter = LendingBacktestAdapter()
        portfolio = SimulatedPortfolio(initial_capital_usd=INITIAL_CASH)
        interest = Decimal("25")
        portfolio.positions.append(borrow_position(interest_accrued=interest))

        adapter_value = portfolio.mark_to_market(market(), TS, adapter=adapter)
        generic_value = portfolio.mark_to_market(market(), TS)

        assert adapter_value == INITIAL_CASH - BORROW_AMOUNT - interest
        assert adapter_value == generic_value


class TestEngineRepayClose:
    """REPAY debits cash AND closes/reduces the matched BORROW position.

    Pre-VIB-5098 the outflow debited cash with no position linkage: a
    $2,000 repay lost ~$2,000 of equity (measured -$1,999.71 with default
    costs).
    """

    @pytest.mark.asyncio
    async def test_borrow_then_full_repay_round_trip_conserves_value(self) -> None:
        backtester = _backtester()
        config = _config()
        portfolio = SimulatedPortfolio(initial_capital_usd=INITIAL_CASH)
        state = market()

        await backtester._execute_intent(supply_intent(), portfolio, state, TS, config)
        await backtester._execute_intent(borrow_intent(), portfolio, state, TS, config)
        # No mark runs in between, so rates are exactly zero: the round trip
        # is Decimal-exact (zero fees / slippage / gas in this fixture).
        await backtester._execute_intent(
            repay_intent(Decimal("0"), repay_full=True), portfolio, state, TS + timedelta(hours=1), config
        )

        assert all(trade.success for trade in portfolio.trades)
        assert [p.position_type.value for p in portfolio.positions] == ["SUPPLY"]
        assert len(portfolio._closed_positions) == 1
        assert portfolio.cash_usd == INITIAL_CASH - SUPPLY_AMOUNT
        assert portfolio.get_total_value_usd(state) == INITIAL_CASH

    @pytest.mark.asyncio
    async def test_exact_amount_repay_closes_in_full(self) -> None:
        """amount == debt principal (repay_full=False) is a full close."""
        backtester = _backtester()
        config = _config()
        portfolio = SimulatedPortfolio(initial_capital_usd=INITIAL_CASH)
        state = market()

        await backtester._execute_intent(borrow_intent(), portfolio, state, TS, config)
        await backtester._execute_intent(repay_intent(BORROW_AMOUNT), portfolio, state, TS, config)

        assert portfolio.positions == []
        assert portfolio.cash_usd == INITIAL_CASH
        assert portfolio.get_total_value_usd(state) == INITIAL_CASH

    @pytest.mark.asyncio
    async def test_partial_repay_reduces_debt_principal(self) -> None:
        backtester = _backtester()
        config = _config()
        portfolio = SimulatedPortfolio(initial_capital_usd=INITIAL_CASH)
        state = market()
        partial = Decimal("500")

        await backtester._execute_intent(borrow_intent(), portfolio, state, TS, config)
        await backtester._execute_intent(repay_intent(partial), portfolio, state, TS + timedelta(hours=1), config)

        assert all(trade.success for trade in portfolio.trades)
        assert len(portfolio.positions) == 1
        position = portfolio.positions[0]
        assert position.amounts["USDC"] == BORROW_AMOUNT - partial
        assert portfolio._closed_positions == []
        assert portfolio.cash_usd == INITIAL_CASH + BORROW_AMOUNT - partial
        # The hour between borrow and repay accrues borrow interest on the
        # FULL pre-reduce debt at the fill instant (accrue-before-reduce);
        # equity shrinks by exactly that owed interest and nothing else.
        accrued = _exact_interest(BORROW_AMOUNT, hours=1, apy=BORROW_APY)
        assert position.interest_accrued == accrued
        assert portfolio.get_total_value_usd(state) == INITIAL_CASH - accrued

    @pytest.mark.asyncio
    async def test_full_repay_realizes_accrued_borrow_interest_as_negative_pnl(self) -> None:
        """Accrued borrow interest is paid on close and realizes as a loss."""
        backtester = _backtester()
        config = _config()
        portfolio = SimulatedPortfolio(initial_capital_usd=INITIAL_CASH)
        state = market()
        interest = Decimal("25")

        await backtester._execute_intent(borrow_intent(), portfolio, state, TS, config)
        # Interest accrual is owned by the mark/update paths; stamp it the
        # way a mark would have.
        portfolio.positions[0].interest_accrued = interest

        await backtester._execute_intent(
            repay_intent(Decimal("0"), repay_full=True), portfolio, state, TS + timedelta(hours=1), config
        )

        assert portfolio.positions == []
        assert portfolio.cash_usd == INITIAL_CASH - interest
        assert portfolio.get_total_value_usd(state) == INITIAL_CASH - interest
        repay_trade = portfolio.trades[-1]
        assert repay_trade.success
        assert repay_trade.pnl_usd == -interest
        assert repay_trade.amount_usd == BORROW_AMOUNT + interest
        assert portfolio._realized_pnl == -interest

    @pytest.mark.asyncio
    async def test_repay_beyond_debt_caps_to_debt_plus_interest(self) -> None:
        """Repaying more than owed cannot burn the difference."""
        backtester = _backtester()
        config = _config()
        portfolio = SimulatedPortfolio(initial_capital_usd=INITIAL_CASH)
        state = market()

        await backtester._execute_intent(borrow_intent(), portfolio, state, TS, config)
        await backtester._execute_intent(
            repay_intent(Decimal("9000")), portfolio, state, TS + timedelta(hours=1), config
        )

        assert portfolio.positions == []
        assert portfolio.cash_usd == INITIAL_CASH
        assert portfolio.get_total_value_usd(state) == INITIAL_CASH
        assert portfolio.trades[-1].amount_usd == BORROW_AMOUNT

    @pytest.mark.asyncio
    async def test_boundary_repay_pays_all_principal_and_partial_interest(self) -> None:
        """A repay in (principal, principal + interest) extinguishes ALL
        principal and realizes only the COVERED interest as a loss; the position
        stays open carrying the unpaid interest remainder (VIB-5098 / PR #2777).

        Pre-fix this overspent: a $2,010 repay against $2,000 principal + $25
        interest full-closed and debited the whole $2,025.
        """
        backtester = _backtester()
        config = _config()
        portfolio = SimulatedPortfolio(initial_capital_usd=INITIAL_CASH)
        state = market()
        interest = Decimal("25")

        await backtester._execute_intent(borrow_intent(), portfolio, state, TS, config)
        # Stamp accrued interest the way a mark would; repay at the SAME instant
        # so accrue-before-reduce is a no-op and the math is exact.
        portfolio.positions[0].interest_accrued = interest

        # Covers all $2,000 principal + $10 of the $25 accrued interest.
        await backtester._execute_intent(repay_intent(Decimal("2010")), portfolio, state, TS, config)

        assert len(portfolio.positions) == 1
        position = portfolio.positions[0]
        assert position.amounts.get("USDC", Decimal("0")) == Decimal("0")
        assert position.interest_accrued == Decimal("15")  # 25 - 10 paid
        assert portfolio._closed_positions == []
        # Cash out equals the REQUESTED repay -- not the full debt.
        assert portfolio.cash_usd == INITIAL_CASH + BORROW_AMOUNT - Decimal("2010")
        assert portfolio._realized_pnl == Decimal("-10")
        repay_trade = portfolio.trades[-1]
        assert repay_trade.success
        assert repay_trade.pnl_usd == Decimal("-10")
        assert repay_trade.amount_usd == Decimal("2010")
        # Equity drift is the full accrued interest ($25): $10 realized + $15
        # still owed on the open position. Principal is conserved.
        assert portfolio.get_total_value_usd(state) == INITIAL_CASH - interest

    @pytest.mark.asyncio
    async def test_repay_covering_principal_only_defers_interest(self) -> None:
        """Repaying EXACTLY the principal (interest accrued) is a sub-principal
        partial: principal -> 0, interest deferred -- it does NOT full-close and
        overspend the accrued interest (the pre-fix bug)."""
        backtester = _backtester()
        config = _config()
        portfolio = SimulatedPortfolio(initial_capital_usd=INITIAL_CASH)
        state = market()
        interest = Decimal("25")

        await backtester._execute_intent(borrow_intent(), portfolio, state, TS, config)
        portfolio.positions[0].interest_accrued = interest

        await backtester._execute_intent(repay_intent(BORROW_AMOUNT), portfolio, state, TS, config)

        assert len(portfolio.positions) == 1
        position = portfolio.positions[0]
        assert position.amounts.get("USDC", Decimal("0")) == Decimal("0")
        assert position.interest_accrued == interest  # fully deferred
        assert portfolio._realized_pnl == Decimal("0")
        # Cash out == principal exactly (the old bug spent $2,025).
        assert portfolio.cash_usd == INITIAL_CASH
        assert portfolio.get_total_value_usd(state) == INITIAL_CASH - interest

    @pytest.mark.asyncio
    async def test_boundary_repay_then_small_repay_of_interest_remainder_is_rejected(self) -> None:
        """After a boundary repay leaves a 0-principal interest remainder, a
        SUBSEQUENT repay of LESS than that interest is rejected -- it must not
        force-close and over-pay the whole remainder (CodeRabbit PR #2777 r2:
        the boundary fix must not re-introduce the over-spend on the leftover)."""
        backtester = _backtester()
        config = _config()
        portfolio = SimulatedPortfolio(initial_capital_usd=INITIAL_CASH)
        state = market()
        interest = Decimal("25")

        await backtester._execute_intent(borrow_intent(), portfolio, state, TS, config)
        portfolio.positions[0].interest_accrued = interest
        # Boundary repay: all $2,000 principal + $10 interest -> 0 principal, $15 interest left.
        await backtester._execute_intent(repay_intent(Decimal("2010")), portfolio, state, TS, config)
        position = portfolio.positions[0]
        assert position.amounts.get("USDC", Decimal("0")) == Decimal("0")
        assert position.interest_accrued == Decimal("15")

        cash_before = portfolio.cash_usd
        realized_before = portfolio._realized_pnl
        # Subsequent $5 repay < $15 remaining interest -> rejected, zero mutation.
        await backtester._execute_intent(repay_intent(Decimal("5")), portfolio, state, TS, config)

        reject = portfolio.trades[-1]
        assert reject.success is False
        assert "interest-only" in reject.metadata["failure_reason"]
        assert reject.fee_usd == Decimal("0") and reject.gas_cost_usd == Decimal("0")
        assert portfolio.cash_usd == cash_before
        assert portfolio._realized_pnl == realized_before
        assert position.interest_accrued == Decimal("15")
        assert len(portfolio.positions) == 1

    @pytest.mark.asyncio
    async def test_repay_without_open_borrow_is_rejected(self) -> None:
        """No matching open BORROW position = failed fill, zero mutation."""
        backtester = _backtester()
        config = _config()
        portfolio = SimulatedPortfolio(initial_capital_usd=INITIAL_CASH)
        state = market()

        await backtester._execute_intent(repay_intent(BORROW_AMOUNT), portfolio, state, TS, config)

        assert portfolio.positions == []
        assert portfolio.tokens == {}
        assert portfolio.cash_usd == INITIAL_CASH
        assert portfolio.get_total_value_usd(state) == INITIAL_CASH
        trade = portfolio.trades[-1]
        assert trade.success is False
        assert "no open borrow position" in trade.metadata["failure_reason"]
        # Rejected fills charge nothing (costs zeroed by _record_failed_fill).
        assert trade.fee_usd == Decimal("0")
        assert trade.gas_cost_usd == Decimal("0")

    @pytest.mark.asyncio
    async def test_repay_other_token_does_not_cross_match(self) -> None:
        """A WETH repay must not close a USDC borrow position."""
        backtester = _backtester()
        config = _config()
        portfolio = SimulatedPortfolio(initial_capital_usd=INITIAL_CASH)
        state = market()

        await backtester._execute_intent(borrow_intent(), portfolio, state, TS, config)
        await backtester._execute_intent(
            repay_intent(Decimal("1"), token="WETH"), portfolio, state, TS + timedelta(hours=1), config
        )

        assert len(portfolio.positions) == 1
        assert portfolio.positions[0].amounts["USDC"] == BORROW_AMOUNT
        assert portfolio.trades[-1].success is False
        assert portfolio.get_total_value_usd(state) == INITIAL_CASH

    @pytest.mark.asyncio
    async def test_unfundable_repay_is_rejected_without_mutation(self) -> None:
        """A repay the portfolio cannot fund fails via the cash-funding check."""
        backtester = _backtester()
        config = _config()
        portfolio = SimulatedPortfolio(initial_capital_usd=INITIAL_CASH)
        state = market()

        await backtester._execute_intent(borrow_intent(), portfolio, state, TS, config)
        # Burn the cash so the full repay (debt 2000) cannot be funded.
        portfolio.cash_usd = Decimal("100")

        await backtester._execute_intent(
            repay_intent(Decimal("0"), repay_full=True), portfolio, state, TS + timedelta(hours=1), config
        )

        trade = portfolio.trades[-1]
        assert trade.success is False
        assert "insufficient cash" in trade.metadata["failure_reason"]
        assert len(portfolio.positions) == 1
        assert portfolio.positions[0].amounts["USDC"] == BORROW_AMOUNT
        assert portfolio.cash_usd == Decimal("100")


class TestBorrowCloseMatching:
    """find_borrow_close_position_id: exact-id precedence, FIFO, fail-closed."""

    def _find(self, intent, positions):  # noqa: ANN001, ANN202 - thin import shim
        from almanak.framework.backtesting.pnl.intent_extraction import (
            find_borrow_close_position_id,
        )

        return find_borrow_close_position_id(intent, positions)

    def test_fifo_matches_oldest_borrow(self) -> None:
        older = borrow_position(entry_time=TS)
        newer = borrow_position(entry_time=TS + timedelta(hours=1))

        assert self._find(repay_intent(BORROW_AMOUNT), [newer, older]) == older.position_id

    def test_exact_position_id_takes_precedence_over_fifo(self) -> None:
        older = borrow_position(entry_time=TS)
        newer = borrow_position(entry_time=TS + timedelta(hours=1))
        intent = repay_intent(BORROW_AMOUNT)
        object.__setattr__(intent, "position_id", newer.position_id)

        assert self._find(intent, [older, newer]) == newer.position_id

    def test_explicit_unknown_id_fails_closed_without_fifo_fallback(self) -> None:
        """A typoed/stale explicit id must NOT fall through to FIFO and repay
        the oldest borrow for that token (CodeRabbit, PR #2777): exact-id
        intent => exact-id match or nothing."""
        older = borrow_position(entry_time=TS)
        newer = borrow_position(entry_time=TS + timedelta(hours=1))
        intent = repay_intent(BORROW_AMOUNT)
        object.__setattr__(intent, "position_id", "BORROW_aave_v3_USDC_does_not_exist")

        assert self._find(intent, [older, newer]) is None

    def test_token_mismatch_returns_none(self) -> None:
        assert self._find(repay_intent(BORROW_AMOUNT), [borrow_position(token="WETH")]) is None

    def test_protocol_mismatch_returns_none(self) -> None:
        assert self._find(repay_intent(BORROW_AMOUNT), [borrow_position(protocol="compound_v3")]) is None

    def test_supply_positions_are_not_repay_targets(self) -> None:
        assert self._find(repay_intent(BORROW_AMOUNT), [supply_position()]) is None

    def test_explicit_id_naming_supply_fails_closed(self) -> None:
        """An explicit id pointing at a SUPPLY is refused outright -- it must
        not be honored, and it must not fall through to FIFO either."""
        supply = supply_position()
        borrow = borrow_position()
        intent = repay_intent(BORROW_AMOUNT)
        object.__setattr__(intent, "position_id", supply.position_id)

        assert self._find(intent, [supply, borrow]) is None

    def test_tokenless_intent_fails_closed(self) -> None:
        """No token/asset on the intent = no protocol-only fallback matching."""
        intent = SimpleNamespace(protocol="aave_v3", amount=BORROW_AMOUNT)

        assert self._find(intent, [borrow_position()]) is None

    def test_missing_protocol_position_does_not_match_protocol_intent(self) -> None:
        unstamped = borrow_position()
        unstamped.protocol = None

        assert self._find(repay_intent(BORROW_AMOUNT), [unstamped]) is None


def _repay_reduce_fill(
    position_id: str,
    amounts: dict[str, Decimal],
    timestamp: datetime = TS,
) -> SimulatedFill:
    total = sum(amounts.values(), Decimal("0"))
    return SimulatedFill(
        timestamp=timestamp,
        intent_type=IntentType.REPAY,
        protocol="aave_v3",
        tokens=list(amounts),
        executed_price=Decimal("1"),
        amount_usd=total,
        fee_usd=Decimal("0"),
        slippage_usd=Decimal("0"),
        gas_cost_usd=Decimal("0"),
        tokens_in={},
        tokens_out=dict(amounts),
        success=True,
        position_reduce_id=position_id,
        position_reduce_amounts=dict(amounts),
    )


class TestApplyFillBorrowReduce:
    """A BORROW reduce ties to the fill's OUTFLOW (the repaid tokens)."""

    def test_borrow_reduce_commits_principal_and_outflow_together(self) -> None:
        portfolio = SimulatedPortfolio(initial_capital_usd=INITIAL_CASH)
        position = borrow_position()
        portfolio.positions.append(position)
        portfolio.cash_usd = INITIAL_CASH + BORROW_AMOUNT

        applied = portfolio.apply_fill(_repay_reduce_fill(position.position_id, {"USDC": Decimal("500")}))

        assert applied is True
        assert position.amounts["USDC"] == Decimal("1500")
        assert portfolio.cash_usd == INITIAL_CASH + BORROW_AMOUNT - Decimal("500")
        assert portfolio.get_total_value_usd(market()) == INITIAL_CASH
        assert portfolio.trades[-1].position_id == position.position_id

    def test_borrow_reduce_not_tied_to_outflow_rejects_fill(self) -> None:
        """Reducing debt by more than the repaid outflow mints the difference."""
        portfolio = SimulatedPortfolio(initial_capital_usd=INITIAL_CASH)
        position = borrow_position()
        portfolio.positions.append(position)
        portfolio.cash_usd = INITIAL_CASH + BORROW_AMOUNT

        fill = _repay_reduce_fill(position.position_id, {"USDC": Decimal("500")})
        fill.position_reduce_amounts = {"USDC": Decimal("1500")}

        assert portfolio.apply_fill(fill) is False
        assert position.amounts["USDC"] == BORROW_AMOUNT
        assert portfolio.cash_usd == INITIAL_CASH + BORROW_AMOUNT
        assert "but reduces" in portfolio.trades[-1].metadata["failure_reason"]

    def test_boundary_reduce_realizes_interest_and_passes_tie_check(self) -> None:
        """A boundary reduce removes principal in full, realizes the COVERED
        interest off the position, and passes the (loosened) conservation guard
        because the flow-vs-principal gap equals exactly the realized interest
        (VIB-5098 / PR #2777)."""
        portfolio = SimulatedPortfolio(initial_capital_usd=INITIAL_CASH)
        position = borrow_position(interest_accrued=Decimal("25"))
        portfolio.positions.append(position)
        portfolio.cash_usd = INITIAL_CASH + BORROW_AMOUNT

        fill = _repay_reduce_fill(position.position_id, {"USDC": Decimal("2010")})
        # Principal-only reduce ($2,000); the extra $10 of outflow retires
        # accrued interest, realized as -$10 PnL (not removed as principal).
        fill.position_reduce_amounts = {"USDC": BORROW_AMOUNT}
        fill.metadata = {"interest_usd": "-10"}

        assert portfolio.apply_fill(fill) is True
        assert position.amounts.get("USDC", Decimal("0")) == Decimal("0")
        assert position.interest_accrued == Decimal("15")  # 25 - 10 realized
        assert portfolio._realized_pnl == Decimal("-10")
        assert portfolio.cash_usd == INITIAL_CASH + BORROW_AMOUNT - Decimal("2010")
        assert portfolio.get_total_value_usd(market()) == INITIAL_CASH - Decimal("25")

    def test_boundary_reduce_over_reduction_still_rejected(self) -> None:
        """Reducing principal by MORE than the outflow net of realized interest
        is still minting and rejected, even on the boundary path."""
        portfolio = SimulatedPortfolio(initial_capital_usd=INITIAL_CASH)
        position = borrow_position(interest_accrued=Decimal("25"))
        portfolio.positions.append(position)
        portfolio.cash_usd = INITIAL_CASH + BORROW_AMOUNT

        fill = _repay_reduce_fill(position.position_id, {"USDC": Decimal("2010")})
        # Reduces principal by the FULL outflow including the interest slice --
        # the $10 must be realized, not removed as principal.
        fill.position_reduce_amounts = {"USDC": Decimal("2010")}
        fill.metadata = {"interest_usd": "-10"}

        assert portfolio.apply_fill(fill) is False
        assert position.amounts["USDC"] == BORROW_AMOUNT
        assert position.interest_accrued == Decimal("25")
        assert portfolio.cash_usd == INITIAL_CASH + BORROW_AMOUNT
        assert "but reduces" in portfolio.trades[-1].metadata["failure_reason"]

    def test_boundary_reduce_clamps_realized_interest_and_syncs_trade_record(self) -> None:
        """Defensive clamp: when a (malformed) fill claims more realized interest
        than the position holds, the realized amount clamps to interest_accrued
        AND the TradeRecord stays in sync with _realized_pnl -- the metadata is
        rewritten to the clamped value before _calculate_trade_pnl reads it
        (CodeRabbit PR #2777 round 2). The engine flow never reaches this (the
        resolver bounds interest_paid by the accrued interest), so it is only
        reachable via a hand-built fill."""
        portfolio = SimulatedPortfolio(initial_capital_usd=INITIAL_CASH)
        position = borrow_position(interest_accrued=Decimal("3"))  # only $3 accrued
        portfolio.positions.append(position)
        portfolio.cash_usd = INITIAL_CASH + BORROW_AMOUNT

        fill = _repay_reduce_fill(position.position_id, {"USDC": Decimal("2010")})
        fill.position_reduce_amounts = {"USDC": BORROW_AMOUNT}  # principal only
        # Metadata claims $10 of interest but the position holds only $3.
        fill.metadata = {"interest_usd": "-10"}

        assert portfolio.apply_fill(fill) is True
        assert position.interest_accrued == Decimal("0")  # clamped: 3 - min(10, 3)
        # The clamped realized amount (-$3) lands in BOTH _realized_pnl and the
        # TradeRecord -- no divergence between portfolio state and the trade.
        assert portfolio._realized_pnl == Decimal("-3")
        assert portfolio.trades[-1].pnl_usd == Decimal("-3")
        assert portfolio.trades[-1].metadata["interest_usd"] == "-3"


def _borrow_repay_intents(repay_amount: Decimal, repay_full: bool) -> list:
    return [
        SupplyIntent(protocol="aave_v3", token="USDC", amount=SUPPLY_AMOUNT),
        BorrowIntent(
            protocol="aave_v3",
            collateral_token="USDC",
            collateral_amount=Decimal("0"),
            borrow_token="USDC",
            borrow_amount=BORROW_AMOUNT,
        ),
        None,
        RepayIntent(protocol="aave_v3", token="USDC", amount=repay_amount, repay_full=repay_full),
    ]


class TestEngineLoopBorrowRepayBothLanes:
    """SUPPLY -> BORROW -> REPAY through the REAL engine iteration loop.

    ``strategy_type=None`` exercises the generic lane;
    ``strategy_type="lending"`` wires the LendingBacktestAdapter (which
    health-factor-validates the borrow, then defers fill construction to the
    generic lane). Equity drift must equal accrued interest only -- never the
    borrowed or repaid principal.
    """

    @pytest.mark.parametrize("strategy_type", [None, "lending"])
    def test_borrow_full_repay_round_trip_conserves_value(self, strategy_type: str | None) -> None:
        result = run_backtest(
            ScriptedStrategy(_borrow_repay_intents(Decimal("0"), repay_full=True)),
            flat_series(10),
            hours=6,
            strategy_type=strategy_type,
        )

        assert result.success
        assert result.metrics.total_trades == 3
        assert all(trade.success for trade in result.trades)
        # The borrow must be visible: $2,000 notional, not a $0 no-op.
        assert result.trades[1].amount_usd == BORROW_AMOUNT
        # Borrow interest realizes as a LOSS on the repay.
        assert result.trades[-1].pnl_usd < Decimal("0")
        # Drift = supply interest earned - borrow interest owed: cents, not
        # principal. Pre-fix this was ~-$2,000 (the repay burned cash).
        drift = result.final_capital_usd - INITIAL_CAPITAL
        assert abs(drift) < Decimal("1")

    @pytest.mark.parametrize("strategy_type", [None, "lending"])
    def test_partial_repay_conserves_value(self, strategy_type: str | None) -> None:
        result = run_backtest(
            ScriptedStrategy(_borrow_repay_intents(Decimal("500"), repay_full=False)),
            flat_series(10),
            hours=6,
            strategy_type=strategy_type,
        )

        assert result.success
        assert result.metrics.total_trades == 3
        assert all(trade.success for trade in result.trades)
        drift = result.final_capital_usd - INITIAL_CAPITAL
        assert abs(drift) < Decimal("1")

    @pytest.mark.parametrize("strategy_type", [None, "lending"])
    def test_repay_without_borrow_is_rejected_with_zero_mutation(self, strategy_type: str | None) -> None:
        result = run_backtest(
            ScriptedStrategy([RepayIntent(protocol="aave_v3", token="USDC", amount=BORROW_AMOUNT)]),
            flat_series(10),
            hours=6,
            strategy_type=strategy_type,
        )

        assert result.success
        # Rejected fill is recorded but not counted as a trade; it
        # surfaces as failed_trades (VIB-5083, CodeRabbit).
        assert result.metrics.total_trades == 0
        assert result.metrics.failed_trades == 1
        trade = result.trades[0]
        assert trade.success is False
        assert "no open borrow position" in trade.metadata["failure_reason"]
        assert result.final_capital_usd == INITIAL_CAPITAL
        assert all(point.value_usd == INITIAL_CAPITAL for point in result.equity_curve)


class TestZeroPrincipalBorrowPosition:
    """An empty borrow position settles its accrued interest, never mints/over-pays.

    Mirror of ``TestZeroPrincipalPosition`` on the debt side: dropping the
    position would FORGIVE owed interest (minting), so a repay that covers the
    accrued interest FULL-closes for exactly that interest (realized as negative
    PnL). A repay that does NOT cover it is rejected (zero mutation) rather than
    force-closed for the whole interest, which would pay more than the requested
    amount (CodeRabbit PR #2777 round 2).
    """

    def test_repay_of_zero_principal_position_full_closes_for_interest(self) -> None:
        interest = Decimal("12.34")
        empty = borrow_position(amount=Decimal("0"), interest_accrued=interest)
        portfolio = SimulatedPortfolio(initial_capital_usd=INITIAL_CAPITAL)
        portfolio.positions.append(empty)
        backtester = _backtester()

        resolution = backtester._resolve_repay_close(repay_intent(Decimal("100")), portfolio, Decimal("100"), market())

        assert resolution.position_close_id == empty.position_id
        assert resolution.position_reduce_id is None
        assert resolution.amount_usd == interest
        assert resolution.interest_usd == -interest

    def test_repay_below_interest_of_zero_principal_position_is_rejected(self) -> None:
        """A repay that does not cover the full accrued interest of an
        interest-only position is rejected -- NOT force-closed for the whole
        interest (the over-spend the round-2 review caught)."""
        interest = Decimal("12.34")
        empty = borrow_position(amount=Decimal("0"), interest_accrued=interest)
        portfolio = SimulatedPortfolio(initial_capital_usd=INITIAL_CAPITAL)
        portfolio.positions.append(empty)
        backtester = _backtester()

        resolution = backtester._resolve_repay_close(repay_intent(Decimal("5")), portfolio, Decimal("5"), market())

        assert resolution.position_close_id is None
        assert resolution.position_reduce_id is None
        assert resolution.failure_reason is not None
        assert "interest-only" in resolution.failure_reason


class TestAdapterLaneHealthFactor:
    """The adapter lane must not fabricate HF=0 for engine-managed borrows.

    The engine loop calls plain ``adapter.update_position`` (which never has
    collateral registered in the adapter's bookkeeping), then
    ``mark_to_market`` -> ``liquidation_simulator.update_health_factors``
    computes the portfolio-wide health factor from open SUPPLY collateral.
    Before the fix, the adapter substituted Decimal("0") for its unpopulated
    per-position collateral map and logged "CRITICAL: Health factor 0.0000
    ... Liquidation imminent" on every tick, despite a $5,000 supply backing
    a $2,000 borrow (true HF = 5000 * 0.825 / 2000 = 2.0625).
    """

    def test_no_false_critical_hf_warning_with_ample_collateral(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        backtester = _backtester()
        backtester._adapter = LendingBacktestAdapter()
        portfolio = SimulatedPortfolio(initial_capital_usd=INITIAL_CASH)
        portfolio.positions.append(supply_position())  # $5,000 USDC collateral
        borrow = SimulatedPosition.borrow(
            token="USDC",
            amount=Decimal("2000"),
            apy=Decimal("0.07"),
            entry_price=Decimal("1"),
            entry_time=TS,
            protocol="aave_v3",
        )
        portfolio.positions.append(borrow)

        # Tick 0 seeds the equity curve (the elapsed-time basis for the
        # adapter lane) and sets the portfolio-level health factor.
        portfolio.mark_to_market(market(), TS, adapter=backtester._adapter)
        assert borrow.health_factor == Decimal("2.0625")

        tick = TS + timedelta(hours=1)
        tick_market = MarketState(timestamp=tick, prices={"USDC": Decimal("1")}, chain="arbitrum")
        with caplog.at_level("WARNING"):
            backtester._update_positions_via_adapter(portfolio, tick_market, tick)
            # The adapter must leave the portfolio-level HF untouched
            assert borrow.health_factor == Decimal("2.0625")
            portfolio.mark_to_market(tick_market, tick, adapter=backtester._adapter)

        assert not any("Health factor" in record.getMessage() for record in caplog.records)
        # Portfolio-level path keeps the HF in the true neighborhood
        # (accrued interest on both legs shifts it marginally).
        assert borrow.health_factor == pytest.approx(Decimal("2.0625"), rel=Decimal("0.001"))
        # The adapter lane still accrued borrow interest for the tick.
        assert borrow.interest_accrued > Decimal("0")


class TestRejectedCloseSkipsGasResolution:
    """A rejected close records the failure instead of raising on missing gas.

    CodeRabbit PR #2805 (comment_id 3410921746): the generic lane resolved
    MEV/slippage/gas before apply_fill recorded a known-rejected close. With
    VIB-5088 fail-loud gas, a missing ETH/WETH price RAISES at gas resolution,
    halting the run and undercounting failed_trades. The fix skips
    MEV/slippage/gas for rejected fills (mirroring the adapter lane).
    """

    @pytest.mark.asyncio
    async def test_rejected_withdraw_with_no_eth_price_records_failure(self) -> None:
        backtester = _backtester()
        # include_gas_costs=True is the default; no gas_eth_price_override.
        config = PnLBacktestConfig(
            start_time=TS,
            end_time=TS + timedelta(hours=1),
            initial_capital_usd=INITIAL_CASH,
            include_gas_costs=True,
        )
        portfolio = SimulatedPortfolio(initial_capital_usd=INITIAL_CASH)
        # Market has NO WETH/ETH price -> gas resolution would raise pre-fix.
        no_eth_market = MarketState(timestamp=TS, prices={"USDC": Decimal("1")}, chain="arbitrum")

        # WITHDRAW with no matching open supply position -> resolution fails.
        trade = await backtester._execute_intent(
            withdraw_intent(SUPPLY_AMOUNT, withdraw_all=True),
            portfolio,
            no_eth_market,
            TS,
            config,
        )

        # Recorded as a rejected trade rather than raising / halting the run.
        assert trade.success is False
        assert trade.gas_cost_usd == Decimal("0")
        assert trade.metadata.get("failure_reason")
        # No value moved: equity unchanged, no position created.
        assert portfolio.positions == []
        assert portfolio.cash_usd == INITIAL_CASH
