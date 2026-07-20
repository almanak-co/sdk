"""Simulation lanes for LP_COLLECT_FEES, DELEVERAGE and WRAP/UNWRAP_NATIVE.

Three officially-supported intent types used to die with
``UnsupportedIntentError`` in the PnL engine's generic lane. Their simulated
semantics:

- LP_COLLECT_FEES: the matched LP position's accrued-uncollected fees move to
  the wallet on the SAME plane LP_CLOSE pays out (``_lp_fees_earned``, USD
  value split across the pair at current prices); the position stays open and
  its fee counters reset, so a later close pays only fees accrued SINCE
  (the double-pay guard). Unknown position -> typed rejection.
- DELEVERAGE: rides REPAY's close-resolution lane verbatim (DeleverageIntent
  is "structurally identical to a RepayIntent at the protocol level"); the
  trade record keeps intent_type DELEVERAGE so accounting can tell forced
  unwinds from routine repays. No debt -> REPAY's rejection contract.
- WRAP_NATIVE / UNWRAP_NATIVE: 1:1 native<->wrapped token conversion sized
  from ONE price (fee 0, slippage 0, gas still charged), using the chain
  registry's native descriptor as the wrap map. Chains without a registered
  wrapped-native mapping -> typed rejection. Native symbol balances price
  through the wrapped ERC-20 (MarketState native->wrapped lookup alias).
"""

from tests.backtesting_funding import pnl_token_funding as _pnl_token_funding

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from types import SimpleNamespace

import pytest

from almanak.framework.backtesting.models import IntentType
from almanak.framework.backtesting.pnl._engine_helpers import (
    GENERIC_SIMULATED_INTENT_TYPES,
    _wrap_conversion_legs,
    resolve_native_wrap_pair,
)
from almanak.framework.backtesting.pnl.data_provider import MarketState
from almanak.framework.backtesting.pnl.engine import (
    DefaultFeeModel,
    DefaultSlippageModel,
    PnLBacktestConfig,
    PnLBacktester,
)
from almanak.framework.backtesting.pnl.portfolio import (
    SimulatedPortfolio,
    SimulatedPosition,
)
from almanak.framework.intents.advanced_intents import UnwrapNativeIntent, WrapNativeIntent
from almanak.framework.intents.lending_intents import DeleverageIntent, RepayIntent
from almanak.framework.intents.vocabulary import CollectFeesIntent
from tests.unit.backtesting.pnl._mocks import MockDataProvider

TS = datetime(2024, 1, 1, 0, 0, tzinfo=UTC)
INITIAL_CASH = Decimal("10000")
WETH_PRICE = Decimal("2000")


def market() -> MarketState:
    return MarketState(
        timestamp=TS,
        prices={"WETH": WETH_PRICE, "USDC": Decimal("1")},
        chain="ethereum",
    )


def _backtester() -> PnLBacktester:
    return PnLBacktester(
        data_provider=MockDataProvider(),
        fee_models={"default": DefaultFeeModel()},
        slippage_models={"default": DefaultSlippageModel()},
    )


def _config(include_gas_costs: bool = False) -> PnLBacktestConfig:
    return PnLBacktestConfig(
        start_time=TS,
        end_time=TS + timedelta(hours=1),
        token_funding=_pnl_token_funding(INITIAL_CASH),
        chain="ethereum",
        include_gas_costs=include_gas_costs,
    )


def _portfolio() -> SimulatedPortfolio:
    return SimulatedPortfolio(initial_capital_usd=INITIAL_CASH, chain="ethereum")


def lp_position(fees_usd: Decimal = Decimal("0")) -> SimulatedPosition:
    position = SimulatedPosition.lp(
        token0="WETH",
        token1="USDC",
        amount0=Decimal("1"),
        amount1=Decimal("2000"),
        liquidity=Decimal("1000"),
        tick_lower=-887272,
        tick_upper=887272,
        fee_tier=Decimal("0.003"),
        entry_price=WETH_PRICE,
        entry_time=TS,
        protocol="uniswap_v3",
    )
    # Both accrual planes move in lockstep (marker and adapter both increment
    # the pair together); the token attribution rides beside them.
    position.fees_earned = fees_usd
    position.accumulated_fees_usd = fees_usd
    position.fees_token0 = (fees_usd / Decimal("2")) / WETH_PRICE
    position.fees_token1 = fees_usd / Decimal("2")
    return position


def borrow_position(amount: Decimal, interest_accrued: Decimal = Decimal("0")) -> SimulatedPosition:
    position = SimulatedPosition.borrow(
        token="USDC",
        amount=amount,
        apy=Decimal("0.08"),
        entry_price=Decimal("1"),
        entry_time=TS,
        protocol="aave_v3",
    )
    position.interest_accrued = interest_accrued
    return position


def collect_intent(pool: str = "WETH/USDC/3000") -> CollectFeesIntent:
    return CollectFeesIntent(pool=pool, protocol="uniswap_v3")


def deleverage_intent(
    amount: Decimal = Decimal("0"),
    repay_full: bool = False,
) -> DeleverageIntent:
    return DeleverageIntent(
        protocol="aave_v3",
        token="USDC",
        amount=amount if not repay_full else Decimal("0"),
        repay_full=repay_full,
        trigger_reason="HF 1.05 < emergency_threshold 1.2",
        observed_hf=Decimal("1.05"),
        target_hf=Decimal("1.5"),
    )


# =============================================================================
# Envelope
# =============================================================================


class TestEnvelope:
    def test_new_types_are_inside_the_simulated_envelope(self) -> None:
        for intent_type in (
            IntentType.LP_COLLECT_FEES,
            IntentType.DELEVERAGE,
            IntentType.WRAP_NATIVE,
            IntentType.UNWRAP_NATIVE,
        ):
            assert intent_type in GENERIC_SIMULATED_INTENT_TYPES

    def test_support_matrix_counts_new_types_as_simulated(self) -> None:
        # The support matrix and the refusal message derive from the same set.
        from almanak.framework.backtesting.pnl import support_matrix as sm

        simulated = {t.value for t in GENERIC_SIMULATED_INTENT_TYPES}
        assert {"LP_COLLECT_FEES", "DELEVERAGE", "WRAP_NATIVE", "UNWRAP_NATIVE"} <= simulated
        assert sm is not None  # the lane reads GENERIC_SIMULATED_INTENT_TYPES directly


# =============================================================================
# LP_COLLECT_FEES
# =============================================================================


class TestLpCollectFees:
    @pytest.mark.asyncio
    async def test_collect_pays_accrued_fees_and_keeps_position_open(self) -> None:
        backtester = _backtester()
        portfolio = _portfolio()
        position = lp_position(fees_usd=Decimal("30"))
        portfolio.positions.append(position)
        state = market()
        value_before = portfolio.get_total_value_usd(state)

        record = await backtester._execute_intent(collect_intent(), portfolio, state, TS, _config())

        assert record.success
        assert record.intent_type == IntentType.LP_COLLECT_FEES
        assert record.amount_usd == Decimal("30")
        assert record.fee_usd == Decimal("0")
        assert record.slippage_usd == Decimal("0")
        assert record.pnl_usd == Decimal("30")
        assert record.fees_earned_usd == Decimal("30")
        assert record.position_id == position.position_id

        # Position stays open with reset fee counters (all four: one accrual,
        # two planes plus the per-token attribution).
        assert portfolio.positions == [position]
        assert position.fees_earned == Decimal("0")
        assert position.accumulated_fees_usd == Decimal("0")
        assert position.fees_token0 == Decimal("0")
        assert position.fees_token1 == Decimal("0")

        # Payout mirrors the generic LP_CLOSE plane: $30 split 50/50 at
        # current prices. The USDC half sweeps to cash; the WETH half lands
        # in the wallet.
        assert portfolio.tokens.get("WETH") == Decimal("15") / WETH_PRICE
        assert portfolio.cash_usd == INITIAL_CASH + Decimal("15")

        # Collect converts unrealized fee accrual to wallet value: equity is
        # unchanged at the collect instant (gas off in this config).
        assert portfolio.get_total_value_usd(state) == value_before

    @pytest.mark.asyncio
    async def test_later_close_does_not_double_pay_collected_fees(self) -> None:
        backtester = _backtester()
        portfolio = _portfolio()
        position = lp_position(fees_usd=Decimal("30"))
        portfolio.positions.append(position)
        state = market()

        await backtester._execute_intent(collect_intent(), portfolio, state, TS, _config())
        assert SimulatedPortfolio._lp_fees_earned(position) == Decimal("0")

        # Fees accrued SINCE the collect are the only fees a close may pay.
        position.fees_earned = Decimal("5")
        position.accumulated_fees_usd = Decimal("5")

        close = SimpleNamespace(
            intent_type="LP_CLOSE",
            position_id=position.position_id,
            pool="WETH/USDC",
            protocol="uniswap_v3",
            amount_usd=Decimal("4000"),
            collect_fees=True,
        )
        close_record = await backtester._execute_intent(close, portfolio, state, TS, _config())

        assert close_record.success
        assert portfolio.positions == []
        # The close realizes only the post-collect accrual, never the $30
        # already paid out.
        assert close_record.fees_earned_usd == Decimal("5")

    @pytest.mark.asyncio
    async def test_zero_accrued_fees_collect_fills_at_zero(self) -> None:
        backtester = _backtester()
        portfolio = _portfolio()
        portfolio.positions.append(lp_position(fees_usd=Decimal("0")))
        state = market()

        record = await backtester._execute_intent(collect_intent(), portfolio, state, TS, _config())

        assert record.success
        assert record.amount_usd == Decimal("0")
        assert portfolio.cash_usd == INITIAL_CASH

    @pytest.mark.asyncio
    async def test_unknown_position_is_a_typed_rejection(self) -> None:
        backtester = _backtester()
        portfolio = _portfolio()
        portfolio.positions.append(lp_position(fees_usd=Decimal("30")))
        state = market()
        value_before = portfolio.get_total_value_usd(state)

        record = await backtester._execute_intent(
            collect_intent(pool="WBTC/DAI/3000"), portfolio, state, TS, _config()
        )

        assert not record.success
        assert "matched no open LP position" in record.metadata["failure_reason"]
        assert portfolio.get_total_value_usd(state) == value_before
        assert portfolio.positions[0].fees_earned == Decimal("30")


# =============================================================================
# DELEVERAGE
# =============================================================================


class TestDeleverage:
    @pytest.mark.asyncio
    async def test_full_deleverage_matches_equivalent_repay_exactly(self) -> None:
        backtester = _backtester()
        state = market()
        config = _config()
        debt = Decimal("2000")
        interest = Decimal("12")

        portfolio_repay = _portfolio()
        portfolio_repay.positions.append(borrow_position(debt, interest_accrued=interest))
        repay_record = await backtester._execute_intent(
            RepayIntent(protocol="aave_v3", token="USDC", amount=Decimal("0"), repay_full=True),
            portfolio_repay,
            state,
            TS,
            config,
        )

        portfolio_dlv = _portfolio()
        portfolio_dlv.positions.append(borrow_position(debt, interest_accrued=interest))
        dlv_record = await backtester._execute_intent(
            deleverage_intent(repay_full=True), portfolio_dlv, state, TS, config
        )

        # Same lane, same numbers — only the intent_type label differs.
        assert dlv_record.success and repay_record.success
        assert dlv_record.intent_type == IntentType.DELEVERAGE
        assert repay_record.intent_type == IntentType.REPAY
        assert dlv_record.amount_usd == repay_record.amount_usd == debt + interest
        assert dlv_record.pnl_usd == repay_record.pnl_usd == -interest
        assert dlv_record.fee_usd == repay_record.fee_usd
        assert portfolio_dlv.positions == [] and portfolio_repay.positions == []
        assert portfolio_dlv.cash_usd == portfolio_repay.cash_usd

    @pytest.mark.asyncio
    async def test_partial_deleverage_reduces_debt_like_repay(self) -> None:
        backtester = _backtester()
        portfolio = _portfolio()
        portfolio.positions.append(borrow_position(Decimal("2000")))
        state = market()

        record = await backtester._execute_intent(
            deleverage_intent(amount=Decimal("500")), portfolio, state, TS, _config()
        )

        assert record.success
        assert record.intent_type == IntentType.DELEVERAGE
        assert len(portfolio.positions) == 1
        assert portfolio.positions[0].amounts["USDC"] == Decimal("1500")

    @pytest.mark.asyncio
    async def test_deleverage_with_no_debt_is_rejected(self) -> None:
        backtester = _backtester()
        portfolio = _portfolio()
        state = market()

        record = await backtester._execute_intent(
            deleverage_intent(repay_full=True), portfolio, state, TS, _config()
        )

        assert not record.success
        assert record.metadata["failure_reason"] == "DELEVERAGE matched no open borrow position to repay"
        assert portfolio.cash_usd == INITIAL_CASH


# =============================================================================
# WRAP_NATIVE / UNWRAP_NATIVE
# =============================================================================


class TestWrapUnwrapNative:
    def test_native_symbol_prices_through_the_wrapped_token(self) -> None:
        # The wallet's native balance stays valued when only the wrapped
        # ERC-20 is priced — symbol plane and address-alias plane.
        state = MarketState(timestamp=TS, prices={"WETH": WETH_PRICE}, chain="ethereum")
        assert state.get_price("ETH") == WETH_PRICE

        address = "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"
        keyed = MarketState(timestamp=TS, prices={("ethereum", address): WETH_PRICE}, chain="ethereum")
        keyed.register_symbol_aliases({"WETH": ("ethereum", address)})
        assert keyed.get_price("ETH") == WETH_PRICE

    def test_wrap_pair_resolves_from_chain_registry(self) -> None:
        assert resolve_native_wrap_pair("ethereum") == ("ETH", "WETH")
        assert resolve_native_wrap_pair("base") == ("ETH", "WETH")
        assert resolve_native_wrap_pair("not_a_chain") is None

    def test_unpriced_wrap_pair_is_a_typed_refusal(self) -> None:
        # Neither the wrapped ERC-20 nor the native symbol has a price: the
        # conversion must raise through the typed plane, never size units
        # from raw USD (a $3000 wrap would otherwise move 3000 "ETH").
        from almanak.framework.market.errors import PriceUnavailableError

        state = MarketState(timestamp=TS, prices={"USDC": Decimal("1")}, chain="ethereum")
        intent = WrapNativeIntent(token="WETH", amount=Decimal("1"), chain="ethereum")
        with pytest.raises(PriceUnavailableError):
            _wrap_conversion_legs(intent, Decimal("3000"), state, None)

    @pytest.mark.asyncio
    async def test_wrap_converts_native_to_wrapped_one_to_one(self) -> None:
        backtester = _backtester()
        portfolio = _portfolio()
        portfolio.tokens["ETH"] = Decimal("2")
        state = market()
        value_before = portfolio.get_total_value_usd(state)

        record = await backtester._execute_intent(
            WrapNativeIntent(token="WETH", amount=Decimal("1"), chain="ethereum"),
            portfolio,
            state,
            TS,
            _config(),
        )

        assert record.success
        assert record.intent_type == IntentType.WRAP_NATIVE
        assert record.fee_usd == Decimal("0")
        assert record.slippage_usd == Decimal("0")
        assert portfolio.tokens["ETH"] == Decimal("1")
        assert portfolio.tokens["WETH"] == Decimal("1")
        assert portfolio.get_total_value_usd(state) == value_before

    @pytest.mark.asyncio
    async def test_wrap_unwrap_round_trip_is_exactly_one_to_one(self) -> None:
        backtester = _backtester()
        portfolio = _portfolio()
        portfolio.tokens["ETH"] = Decimal("2")
        state = market()

        wrap = await backtester._execute_intent(
            WrapNativeIntent(token="WETH", amount=Decimal("1.5"), chain="ethereum"),
            portfolio,
            state,
            TS,
            _config(),
        )
        unwrap = await backtester._execute_intent(
            UnwrapNativeIntent(token="WETH", amount=Decimal("1.5"), chain="ethereum"),
            portfolio,
            state,
            TS,
            _config(),
        )

        assert wrap.success and unwrap.success
        assert unwrap.intent_type == IntentType.UNWRAP_NATIVE
        assert portfolio.tokens["ETH"] == Decimal("2")
        assert "WETH" not in portfolio.tokens
        assert portfolio.cash_usd == INITIAL_CASH

    @pytest.mark.asyncio
    async def test_gas_is_charged_but_token_conversion_stays_one_to_one(self) -> None:
        backtester = _backtester()
        portfolio = SimulatedPortfolio(
            initial_capital_usd=INITIAL_CASH,
            chain="ethereum",
            gas_tank_budget_usd=Decimal("50"),
        )
        portfolio.tokens["ETH"] = Decimal("1")
        state = market()

        record = await backtester._execute_intent(
            WrapNativeIntent(token="WETH", amount=Decimal("1"), chain="ethereum"),
            portfolio,
            state,
            TS,
            _config(include_gas_costs=True),
        )

        assert record.success
        assert record.gas_cost_usd > Decimal("0")
        assert portfolio.tokens["WETH"] == Decimal("1")
        assert "ETH" not in portfolio.tokens

    @pytest.mark.asyncio
    async def test_unwrap_all_converts_the_full_wrapped_balance(self) -> None:
        backtester = _backtester()
        portfolio = _portfolio()
        portfolio.tokens["WETH"] = Decimal("2")
        state = market()

        record = await backtester._execute_intent(
            UnwrapNativeIntent(token="WETH", amount="all", chain="ethereum"),
            portfolio,
            state,
            TS,
            _config(),
        )

        assert record.success
        assert portfolio.tokens["ETH"] == Decimal("2")
        assert "WETH" not in portfolio.tokens

    @pytest.mark.asyncio
    async def test_wrap_with_insufficient_native_is_rejected_not_cash_funded(self) -> None:
        # Live fidelity: weth.deposit() reverts when the wallet holds no ETH.
        # The simulated lane must NOT silently convert cash into the native
        # leg (that overstates capital efficiency vs live) — it rejects with
        # the same named insufficient-balance shape SWAP uses.
        backtester = _backtester()
        portfolio = _portfolio()
        state = market()
        value_before = portfolio.get_total_value_usd(state)

        record = await backtester._execute_intent(
            WrapNativeIntent(token="WETH", amount=Decimal("1"), chain="ethereum"),
            portfolio,
            state,
            TS,
            _config(),
        )

        assert not record.success
        assert "insufficient ETH balance" in record.metadata["failure_reason"]
        assert "WETH" not in portfolio.tokens
        assert portfolio.cash_usd == INITIAL_CASH
        assert portfolio.get_total_value_usd(state) == value_before

    @pytest.mark.asyncio
    async def test_unwrap_with_insufficient_wrapped_is_rejected_not_cash_funded(self) -> None:
        # weth.withdraw() reverts without the wrapped balance; same contract.
        backtester = _backtester()
        portfolio = _portfolio()
        portfolio.tokens["WETH"] = Decimal("0.25")
        state = market()

        record = await backtester._execute_intent(
            UnwrapNativeIntent(token="WETH", amount=Decimal("1"), chain="ethereum"),
            portfolio,
            state,
            TS,
            _config(),
        )

        assert not record.success
        assert "insufficient WETH balance" in record.metadata["failure_reason"]
        assert portfolio.tokens["WETH"] == Decimal("0.25")
        assert "ETH" not in portfolio.tokens
        assert portfolio.cash_usd == INITIAL_CASH

    @pytest.mark.asyncio
    async def test_wrap_on_unmapped_chain_is_a_typed_rejection(self) -> None:
        backtester = _backtester()
        portfolio = _portfolio()
        portfolio.tokens["ETH"] = Decimal("1")
        state = market()

        record = await backtester._execute_intent(
            WrapNativeIntent(token="WNOT", amount=Decimal("1"), chain="not_a_chain"),
            portfolio,
            state,
            TS,
            _config(),
        )

        assert not record.success
        assert "no registered native<->wrapped token mapping" in record.metadata["failure_reason"]
        assert portfolio.tokens["ETH"] == Decimal("1")
