"""Conservation-of-value invariant tests for SimulatedPortfolio.apply_fill.

These are model-free invariants: at constant prices, no fill may create or
destroy portfolio value beyond its explicitly charged execution costs
(fee/slippage embedded in the flow legs, gas debited from cash).

Regression guard for the apply_fill clamp bug (2026-06): tokens_out of a
cash-equivalent stablecoin debited nothing (stables live in cash_usd, and
the old clamp silently popped the absent token key), so every stable-quoted
buy minted portfolio value equal to the trade size, and selling more of a
token than held minted the full proceeds (short-from-nothing).

The TrustTest plan (docs/internal/reference/backtesting/Backtesting-TrustTest.md
Tests 1.1 / 1.2) describes these invariants; this file makes them
CI-enforced.
"""

from datetime import UTC, datetime
from decimal import Decimal

import pytest

from almanak.framework.backtesting.models import IntentType
from almanak.framework.backtesting.pnl.data_provider import MarketState, TokenRef
from almanak.framework.backtesting.pnl.portfolio import (
    CASH_EQUIVALENT_STABLECOINS,
    SimulatedFill,
    SimulatedPortfolio,
)

WETH_PRICE = Decimal("3000")
TS = datetime(2025, 11, 1, tzinfo=UTC)


@pytest.fixture
def portfolio() -> SimulatedPortfolio:
    """Fresh portfolio with 10,000 USD initial capital."""
    return SimulatedPortfolio(initial_capital_usd=Decimal("10000"))


@pytest.fixture
def market_state() -> MarketState:
    """Constant-price market state for closed-form value checks."""
    return MarketState(
        timestamp=TS,
        prices={"WETH": WETH_PRICE, "USDC": Decimal("1")},
        chain="arbitrum",
    )


def make_swap_fill(
    tokens_out: dict[TokenRef, Decimal],
    tokens_in: dict[TokenRef, Decimal],
    *,
    amount_usd: Decimal = Decimal("0"),
    fee_usd: Decimal = Decimal("0"),
    slippage_usd: Decimal = Decimal("0"),
    gas_cost_usd: Decimal = Decimal("0"),
    timestamp: datetime = TS,
) -> SimulatedFill:
    return SimulatedFill(
        timestamp=timestamp,
        intent_type=IntentType.SWAP,
        protocol="uniswap_v3",
        tokens=list(tokens_out) + list(tokens_in),
        executed_price=WETH_PRICE,
        amount_usd=amount_usd,
        fee_usd=fee_usd,
        slippage_usd=slippage_usd,
        gas_cost_usd=gas_cost_usd,
        tokens_in=tokens_in,
        tokens_out=tokens_out,
    )


class TestNoTradeConservation:
    """TrustTest 1.1: a portfolio that does nothing keeps its exact value."""

    def test_no_trade_value_equals_initial_capital(
        self, portfolio: SimulatedPortfolio, market_state: MarketState
    ) -> None:
        assert portfolio.get_total_value_usd(market_state) == Decimal("10000")
        assert portfolio.mark_to_market(market_state, TS) == Decimal("10000")


class TestSingleTradeClosedForm:
    """TrustTest 1.2: a single trade changes value by exactly its costs."""

    BASE_USDC = ("base", "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913")
    BASE_WETH = ("base", "0x4200000000000000000000000000000000000006")
    POLYGON_USDC_E = ("polygon", "0x2791bca1f2de4661ed88a30c99a7a9449aa84174")

    def test_stable_quoted_buy_debits_cash(self, portfolio: SimulatedPortfolio, market_state: MarketState) -> None:
        """The 2026-06 clamp-bug repro: a $50 USDC->WETH buy with $20 gas.

        tokens_out of a stablecoin must debit cash_usd. Before the fix this
        fill left cash at 9980 (only gas) and produced a portfolio value of
        10029.50 -- $50 minted out of nothing.
        """
        applied = portfolio.apply_fill(
            make_swap_fill(
                tokens_out={"USDC": Decimal("50")},
                tokens_in={"WETH": Decimal("0.0165")},
                amount_usd=Decimal("50"),
                slippage_usd=Decimal("0.5"),
                gas_cost_usd=Decimal("20"),
            )
        )

        assert applied is True
        assert portfolio.cash_usd == Decimal("9930")  # 10000 - 50 - 20
        assert portfolio.tokens["WETH"] == Decimal("0.0165")
        # 9930 cash + 0.0165 * 3000 = 9979.50
        assert portfolio.get_total_value_usd(market_state) == Decimal("9979.50")

    def test_symbol_debit_uses_exact_address_native_funding_without_cash_sweep(self) -> None:
        portfolio = SimulatedPortfolio(
            initial_capital_usd=Decimal("0"),
            cash_usd=Decimal("0"),
            tokens={self.POLYGON_USDC_E: Decimal("10000")},
            chain="polygon",
        )
        fill = SimulatedFill(
            timestamp=TS,
            intent_type=IntentType.SUPPLY,
            protocol="compound_v3",
            tokens=["USDC.e"],
            executed_price=Decimal("1"),
            amount_usd=Decimal("9000"),
            fee_usd=Decimal("0"),
            slippage_usd=Decimal("0"),
            gas_cost_usd=Decimal("0"),
            tokens_in={},
            tokens_out={"USDC.e": Decimal("9000")},
        )
        state = MarketState(timestamp=TS, prices={self.POLYGON_USDC_E: Decimal("1")}, chain="polygon")

        applied = portfolio.apply_fill(fill, market_state=state)

        assert applied is True
        assert portfolio.cash_usd == Decimal("0")
        assert portfolio.tokens == {self.POLYGON_USDC_E: Decimal("1000")}

    def test_buy_value_change_equals_embedded_costs(
        self, portfolio: SimulatedPortfolio, market_state: MarketState
    ) -> None:
        """value_after = initial - fee - slippage - gas at constant prices."""
        fee = Decimal("9")
        slippage = Decimal("3")
        gas = Decimal("1")
        amount = Decimal("3000")
        # Inflow leg carries the fee/slippage haircut, as the engine computes it
        weth_in = (amount - fee - slippage) / WETH_PRICE

        portfolio.apply_fill(
            make_swap_fill(
                tokens_out={"USDC": amount},
                tokens_in={"WETH": weth_in},
                amount_usd=amount,
                fee_usd=fee,
                slippage_usd=slippage,
                gas_cost_usd=gas,
            )
        )

        expected = Decimal("10000") - fee - slippage - gas
        assert portfolio.get_total_value_usd(market_state) == expected

    def test_address_keyed_stable_buy_conserves_value(self) -> None:
        portfolio = SimulatedPortfolio(initial_capital_usd=Decimal("10000"), chain="base")
        market_state = MarketState(
            timestamp=TS,
            prices={self.BASE_USDC: Decimal("1"), self.BASE_WETH: WETH_PRICE},
            chain="base",
        )
        fee = Decimal("9")
        slippage = Decimal("3")
        gas = Decimal("1")
        amount = Decimal("3000")

        portfolio.apply_fill(
            make_swap_fill(
                tokens_out={self.BASE_USDC: amount},
                tokens_in={self.BASE_WETH: (amount - fee - slippage) / WETH_PRICE},
                amount_usd=amount,
                fee_usd=fee,
                slippage_usd=slippage,
                gas_cost_usd=gas,
            )
        )

        assert portfolio.get_total_value_usd(market_state) == Decimal("10000") - fee - slippage - gas


class TestRoundTripConservation:
    """Buy-then-sell at the same price with zero fees returns to initial minus gas."""

    def test_zero_fee_round_trip_conserves_value(
        self, portfolio: SimulatedPortfolio, market_state: MarketState
    ) -> None:
        portfolio.apply_fill(
            make_swap_fill(
                tokens_out={"USDC": Decimal("3000")},
                tokens_in={"WETH": Decimal("1")},
                amount_usd=Decimal("3000"),
            )
        )
        portfolio.apply_fill(
            make_swap_fill(
                tokens_out={"WETH": Decimal("1")},
                tokens_in={"USDC": Decimal("3000")},
                amount_usd=Decimal("3000"),
            )
        )

        assert portfolio.cash_usd == Decimal("10000")
        assert portfolio.tokens == {}
        assert portfolio.get_total_value_usd(market_state) == Decimal("10000")

    def test_repeated_buys_do_not_mint_value(self, portfolio: SimulatedPortfolio, market_state: MarketState) -> None:
        """The end-to-end symptom: 58 x $50 stable-quoted buys minted ~$2900."""
        for _ in range(58):
            portfolio.apply_fill(
                make_swap_fill(
                    tokens_out={"USDC": Decimal("50")},
                    tokens_in={"WETH": Decimal("50") / WETH_PRICE},
                    amount_usd=Decimal("50"),
                )
            )

        assert portfolio.get_total_value_usd(market_state) == Decimal("10000")


class TestInsufficientBalanceFailsFill:
    """Fills the portfolio cannot afford are rejected and recorded, never clamped."""

    def test_sell_more_than_held_is_rejected(self, portfolio: SimulatedPortfolio) -> None:
        """Short-from-nothing: selling unheld WETH must not credit proceeds."""
        applied = portfolio.apply_fill(
            make_swap_fill(
                tokens_out={"WETH": Decimal("1")},
                tokens_in={"USDC": Decimal("3000")},
                amount_usd=Decimal("3000"),
            )
        )

        assert applied is False
        assert portfolio.cash_usd == Decimal("10000")
        assert portfolio.tokens == {}
        assert len(portfolio.trades) == 1
        trade = portfolio.trades[0]
        assert trade.success is False
        assert "insufficient WETH balance" in trade.metadata["failure_reason"]

    def test_partial_balance_is_not_silently_deleted(self, portfolio: SimulatedPortfolio) -> None:
        """The old clamp popped the balance when overselling; now it is kept."""
        portfolio.tokens["WETH"] = Decimal("0.4")

        applied = portfolio.apply_fill(
            make_swap_fill(
                tokens_out={"WETH": Decimal("1")},
                tokens_in={"USDC": Decimal("3000")},
                amount_usd=Decimal("3000"),
            )
        )

        assert applied is False
        assert portfolio.tokens["WETH"] == Decimal("0.4")
        assert portfolio.cash_usd == Decimal("10000")

    def test_stable_spend_beyond_cash_is_rejected(self) -> None:
        portfolio = SimulatedPortfolio(initial_capital_usd=Decimal("30"))

        applied = portfolio.apply_fill(
            make_swap_fill(
                tokens_out={"USDC": Decimal("50")},
                tokens_in={"WETH": Decimal("0.0165")},
                amount_usd=Decimal("50"),
            )
        )

        assert applied is False
        assert portfolio.cash_usd == Decimal("30")
        assert portfolio.tokens == {}
        assert portfolio.trades[0].success is False
        assert "insufficient cash" in portfolio.trades[0].metadata["failure_reason"]

    def test_rejected_fill_charges_no_execution_costs(self, portfolio: SimulatedPortfolio) -> None:
        """A rejected fill must not charge gas/fee/slippage; the recorded trade matches the books."""
        applied = portfolio.apply_fill(
            make_swap_fill(
                tokens_out={"WETH": Decimal("1")},
                tokens_in={"USDC": Decimal("3000")},
                amount_usd=Decimal("3000"),
                fee_usd=Decimal("9"),
                slippage_usd=Decimal("3"),
                gas_cost_usd=Decimal("1"),
            )
        )

        assert applied is False
        assert portfolio.cash_usd == Decimal("10000")
        trade = portfolio.trades[0]
        assert trade.fee_usd == Decimal("0")
        assert trade.slippage_usd == Decimal("0")
        assert trade.gas_cost_usd == Decimal("0")
        assert trade.metadata["gas_cost_usd_unapplied"] == "1"

    def test_rejected_fill_does_not_open_or_close_positions(
        self, portfolio: SimulatedPortfolio, market_state: MarketState
    ) -> None:
        from almanak.framework.backtesting.pnl.portfolio import SimulatedPosition

        position = SimulatedPosition.lp(
            token0="WETH",
            token1="USDC",
            amount0=Decimal("1"),
            amount1=Decimal("3000"),
            liquidity=Decimal("1000"),
            tick_lower=-100,
            tick_upper=100,
            fee_tier=Decimal("0.003"),
            entry_price=WETH_PRICE,
            entry_time=TS,
        )
        fill = SimulatedFill(
            timestamp=TS,
            intent_type=IntentType.LP_OPEN,
            protocol="uniswap_v3",
            tokens=["WETH", "USDC"],
            executed_price=WETH_PRICE,
            amount_usd=Decimal("6000"),
            fee_usd=Decimal("0"),
            slippage_usd=Decimal("0"),
            gas_cost_usd=Decimal("0"),
            tokens_in={},
            tokens_out={"WETH": Decimal("1"), "USDC": Decimal("3000")},  # WETH not held
            position_delta=position,
        )

        applied = portfolio.apply_fill(fill)

        assert applied is False
        assert portfolio.positions == []
        assert portfolio.get_total_value_usd(market_state) == Decimal("10000")

    def test_missing_close_position_rejects_without_crediting_inflow(
        self, portfolio: SimulatedPortfolio, market_state: MarketState
    ) -> None:
        """A close fill must not credit returned assets unless a position closed."""
        fill = SimulatedFill(
            timestamp=TS,
            intent_type=IntentType.LP_CLOSE,
            protocol="uniswap_v3",
            tokens=["WETH", "USDC"],
            executed_price=WETH_PRICE,
            amount_usd=Decimal("6000"),
            fee_usd=Decimal("0"),
            slippage_usd=Decimal("0"),
            gas_cost_usd=Decimal("0"),
            tokens_in={"WETH": Decimal("1"), "USDC": Decimal("3000")},
            tokens_out={},
            position_close_id="missing-position",
        )

        applied = portfolio.apply_fill(fill, market_state=market_state)

        assert applied is False
        assert portfolio.cash_usd == Decimal("10000")
        assert portfolio.tokens == {}
        assert portfolio.positions == []
        assert portfolio._closed_positions == []
        trade = portfolio.trades[0]
        assert trade.success is False
        assert "not found for close" in trade.metadata["failure_reason"]


class TestStableCashDebitMechanics:
    """Stable outflows draw from the token balance first, then cash_usd at $1."""

    def test_stable_spend_draws_tokens_then_cash(self, portfolio: SimulatedPortfolio) -> None:
        portfolio.tokens["USDC"] = Decimal("20")

        applied = portfolio.apply_fill(
            make_swap_fill(
                tokens_out={"USDC": Decimal("50")},
                tokens_in={"WETH": Decimal("50") / WETH_PRICE},
                amount_usd=Decimal("50"),
            )
        )

        assert applied is True
        assert "USDC" not in portfolio.tokens
        # 20 from the USDC balance, 30 from cash
        assert portfolio.cash_usd == Decimal("9970")

    def test_all_cash_equivalents_debit_cash(self) -> None:
        for stable in sorted(CASH_EQUIVALENT_STABLECOINS):
            portfolio = SimulatedPortfolio(initial_capital_usd=Decimal("100"))
            applied = portfolio.apply_fill(
                make_swap_fill(
                    tokens_out={stable: Decimal("60")},
                    tokens_in={"WETH": Decimal("0.02")},
                    amount_usd=Decimal("60"),
                )
            )
            assert applied is True, stable
            assert portfolio.cash_usd == Decimal("40"), stable

    def test_lp_open_debits_stable_leg_from_cash_and_token_leg_from_tokens(
        self, portfolio: SimulatedPortfolio, market_state: MarketState
    ) -> None:
        from almanak.framework.backtesting.pnl.portfolio import SimulatedPosition

        portfolio.tokens["WETH"] = Decimal("1")
        position = SimulatedPosition.lp(
            token0="WETH",
            token1="USDC",
            amount0=Decimal("1"),
            amount1=Decimal("3000"),
            liquidity=Decimal("1000"),
            tick_lower=-100,
            tick_upper=100,
            fee_tier=Decimal("0.003"),
            entry_price=WETH_PRICE,
            entry_time=TS,
        )
        fill = SimulatedFill(
            timestamp=TS,
            intent_type=IntentType.LP_OPEN,
            protocol="uniswap_v3",
            tokens=["WETH", "USDC"],
            executed_price=WETH_PRICE,
            amount_usd=Decimal("6000"),
            fee_usd=Decimal("0"),
            slippage_usd=Decimal("0"),
            gas_cost_usd=Decimal("0"),
            tokens_in={},
            tokens_out={"WETH": Decimal("1"), "USDC": Decimal("3000")},
            position_delta=position,
        )

        applied = portfolio.apply_fill(fill)

        assert applied is True
        assert "WETH" not in portfolio.tokens
        assert portfolio.cash_usd == Decimal("7000")
        # Wallet 7000 cash + LP position (1 WETH + 3000 USDC) = 13000;
        # initial wallet was 10000 cash + 1 WETH (3000) = 13000. Conserved.
        assert portfolio.get_total_value_usd(market_state) == Decimal("13000")

    def test_supply_stable_debits_cash(self, portfolio: SimulatedPortfolio, market_state: MarketState) -> None:
        from almanak.framework.backtesting.pnl.portfolio import SimulatedPosition

        position = SimulatedPosition.supply(
            token="USDC",
            amount=Decimal("5000"),
            apy=Decimal("0.05"),
            entry_price=Decimal("1"),
            entry_time=TS,
        )
        fill = SimulatedFill(
            timestamp=TS,
            intent_type=IntentType.SUPPLY,
            protocol="aave_v3",
            tokens=["USDC"],
            executed_price=Decimal("1"),
            amount_usd=Decimal("5000"),
            fee_usd=Decimal("0"),
            slippage_usd=Decimal("0"),
            gas_cost_usd=Decimal("0"),
            tokens_in={},
            tokens_out={"USDC": Decimal("5000")},
            position_delta=position,
        )

        applied = portfolio.apply_fill(fill)

        assert applied is True
        assert portfolio.cash_usd == Decimal("5000")
        assert portfolio.get_total_value_usd(market_state) == Decimal("10000")


class TestDustTolerance:
    """Decimal round-trip dust must not fail honest sell-all fills."""

    def test_sell_all_within_dust_tolerance_clears_balance(self, portfolio: SimulatedPortfolio) -> None:
        # Held balance computed via one division; sell-all reconstructed via
        # another -- they differ in the last digits of Decimal precision.
        held = Decimal("1000") / Decimal("2999.999999")
        portfolio.tokens["WETH"] = held
        requested = (held * Decimal("2999.999999")) / Decimal("2999.999999")

        applied = portfolio.apply_fill(
            make_swap_fill(
                tokens_out={"WETH": requested},
                tokens_in={"USDC": Decimal("1000")},
                amount_usd=Decimal("1000"),
            )
        )

        assert applied is True
        assert "WETH" not in portfolio.tokens

    def test_shortfall_beyond_dust_tolerance_fails(self, portfolio: SimulatedPortfolio) -> None:
        portfolio.tokens["WETH"] = Decimal("0.999")

        applied = portfolio.apply_fill(
            make_swap_fill(
                tokens_out={"WETH": Decimal("1")},
                tokens_in={"USDC": Decimal("3000")},
                amount_usd=Decimal("3000"),
            )
        )

        assert applied is False
        assert portfolio.tokens["WETH"] == Decimal("0.999")


class TestImplicitCashConversion:
    """Non-SWAP intents may fund non-stable legs from cash at market price.

    The engine's flow producers size LP_OPEN / SUPPLY / VAULT legs from USD
    notional (LP_OPEN splits amount_usd 50/50), not from held balances, so
    a cash-only portfolio must be able to fund them. The conversion is a
    zero-fee zap at the market price: value-conserving by construction.
    SWAP legs are excluded -- selling an unheld token must fail.
    """

    def _lp_open_fill(self, weth_amount: Decimal, usdc_amount: Decimal) -> SimulatedFill:
        from almanak.framework.backtesting.pnl.portfolio import SimulatedPosition

        position = SimulatedPosition.lp(
            token0="WETH",
            token1="USDC",
            amount0=weth_amount,
            amount1=usdc_amount,
            liquidity=Decimal("1000"),
            tick_lower=-100,
            tick_upper=100,
            fee_tier=Decimal("0.003"),
            entry_price=WETH_PRICE,
            entry_time=TS,
        )
        return SimulatedFill(
            timestamp=TS,
            intent_type=IntentType.LP_OPEN,
            protocol="uniswap_v3",
            tokens=["WETH", "USDC"],
            executed_price=WETH_PRICE,
            amount_usd=weth_amount * WETH_PRICE + usdc_amount,
            fee_usd=Decimal("0"),
            slippage_usd=Decimal("0"),
            gas_cost_usd=Decimal("0"),
            tokens_in={},
            tokens_out={"WETH": weth_amount, "USDC": usdc_amount},
            position_delta=position,
        )

    def test_lp_open_from_cash_only_converts_at_market_price(
        self, portfolio: SimulatedPortfolio, market_state: MarketState
    ) -> None:
        fill = self._lp_open_fill(Decimal("1"), Decimal("3000"))

        applied = portfolio.apply_fill(fill, market_state=market_state)

        assert applied is True
        # 10000 - 3000 (WETH leg converted at $3000) - 3000 (USDC leg)
        assert portfolio.cash_usd == Decimal("4000")
        assert fill.metadata["implicit_conversions"] == {"WETH": "1"}
        # Wallet 4000 + position (1 WETH + 3000 USDC) = 10000. Conserved.
        assert portfolio.get_total_value_usd(market_state) == Decimal("10000")

    def test_lp_open_conversion_beyond_cash_is_rejected(self, market_state: MarketState) -> None:
        portfolio = SimulatedPortfolio(initial_capital_usd=Decimal("5000"))
        fill = self._lp_open_fill(Decimal("1"), Decimal("3000"))  # needs $6000

        applied = portfolio.apply_fill(fill, market_state=market_state)

        assert applied is False
        assert portfolio.cash_usd == Decimal("5000")
        assert portfolio.positions == []
        assert "insufficient cash" in portfolio.trades[0].metadata["failure_reason"]

    def test_conversion_requires_market_price(self, portfolio: SimulatedPortfolio) -> None:
        """Without a market price the leg cannot be valued -- a $1 guess
        would under-debit and mint value, so the fill must fail."""
        no_weth_price = MarketState(timestamp=TS, prices={"USDC": Decimal("1")}, chain="arbitrum")
        fill = self._lp_open_fill(Decimal("1"), Decimal("3000"))

        applied = portfolio.apply_fill(fill, market_state=no_weth_price)

        assert applied is False
        assert "no market price available" in portfolio.trades[0].metadata["failure_reason"]

    def test_swap_is_never_funded_by_conversion(
        self, portfolio: SimulatedPortfolio, market_state: MarketState
    ) -> None:
        """A SWAP selling unheld WETH must fail even when cash could cover it."""
        applied = portfolio.apply_fill(
            make_swap_fill(
                tokens_out={"WETH": Decimal("1")},
                tokens_in={"USDC": Decimal("3000")},
                amount_usd=Decimal("3000"),
            ),
            market_state=market_state,
        )

        assert applied is False
        assert portfolio.cash_usd == Decimal("10000")

    def test_partial_holding_converts_only_the_shortfall(
        self, portfolio: SimulatedPortfolio, market_state: MarketState
    ) -> None:
        portfolio.tokens["WETH"] = Decimal("0.4")
        fill = self._lp_open_fill(Decimal("1"), Decimal("3000"))

        applied = portfolio.apply_fill(fill, market_state=market_state)

        assert applied is True
        assert "WETH" not in portfolio.tokens
        # 10000 - 0.6 * 3000 (converted shortfall) - 3000 (USDC leg)
        assert portfolio.cash_usd == Decimal("5200.0")
        assert fill.metadata["implicit_conversions"] == {"WETH": "0.6"}
        # Initial wallet: 10000 cash + 0.4 WETH (1200) = 11200. Conserved.
        assert portfolio.get_total_value_usd(market_state) == Decimal("11200.0")


class TestAggregateCashValidation:
    """All STRATEGY-CAPITAL cash draws of a fill are validated as one sum.

    Split checks (stable debits in _plan_token_debits, perp collateral in
    its own gate) could each pass while their sum overdrew cash_usd. GAS is
    deliberately NOT part of the gate (ALM-2958): live gas is EOA-paid
    native ETH the strategy never sizes for, so a fill whose capital legs
    are fully funded must apply, with gas charged unconditionally (cash may
    go transiently negative -- a debit cannot mint value). Fills that draw
    nothing from cash keep gas unconditional so risk-reducing sells/closes
    are never blocked for being cash-poor.
    """

    def test_full_capital_spend_applies_with_gas_charged_beyond_cash(self) -> None:
        # ALM-2958: 100% of cash into the swap + $1 gas. Live this fills
        # (the EOA pays gas); the backtest must too, with the gas debit
        # driving cash exactly -1 so PnL stays net-of-gas.
        portfolio = SimulatedPortfolio(initial_capital_usd=Decimal("100"))

        applied = portfolio.apply_fill(
            make_swap_fill(
                tokens_out={"USDC": Decimal("100")},
                tokens_in={"WETH": Decimal("100") / WETH_PRICE},
                amount_usd=Decimal("100"),
                gas_cost_usd=Decimal("1"),
            )
        )

        assert applied is True
        assert portfolio.cash_usd == Decimal("-1")  # gas, and only gas
        assert portfolio.tokens["WETH"] == Decimal("100") / WETH_PRICE

    def test_capital_legs_beyond_cash_still_rejected(self) -> None:
        # The gate itself survives ALM-2958: spending MORE CAPITAL than the
        # portfolio holds is still refused -- only gas left the sum.
        portfolio = SimulatedPortfolio(initial_capital_usd=Decimal("100"))

        applied = portfolio.apply_fill(
            make_swap_fill(
                tokens_out={"USDC": Decimal("101")},
                tokens_in={"WETH": Decimal("101") / WETH_PRICE},
                amount_usd=Decimal("101"),
                gas_cost_usd=Decimal("0"),
            )
        )

        assert applied is False
        assert portfolio.cash_usd == Decimal("100")
        assert portfolio.tokens == {}
        assert "insufficient cash" in portfolio.trades[0].metadata["failure_reason"]

    def test_cash_neutral_sell_is_never_blocked_by_gas(self) -> None:
        """A sell that draws nothing from cash must apply even when cash
        cannot cover gas -- the sale itself replenishes cash."""
        portfolio = SimulatedPortfolio(initial_capital_usd=Decimal("0"))
        portfolio.tokens["WETH"] = Decimal("1")

        applied = portfolio.apply_fill(
            make_swap_fill(
                tokens_out={"WETH": Decimal("1")},
                tokens_in={"USDC": Decimal("3000")},
                amount_usd=Decimal("3000"),
                gas_cost_usd=Decimal("1"),
            )
        )

        assert applied is True
        assert portfolio.cash_usd == Decimal("2999")  # proceeds swept, gas debited
        assert portfolio.tokens == {}

    def test_perp_collateral_plus_stable_debit_validated_as_one_sum(self) -> None:
        """Each draw passes alone (50 <= 100, 60 <= 100) but the sum (110)
        overdraws cash; the aggregate check must reject before mutation."""
        from almanak.framework.backtesting.pnl.portfolio import SimulatedPosition

        portfolio = SimulatedPortfolio(initial_capital_usd=Decimal("100"))
        position = SimulatedPosition.perp_long(
            token="WETH",
            collateral_usd=Decimal("60"),
            leverage=Decimal("2"),
            entry_price=WETH_PRICE,
            entry_time=TS,
        )
        fill = SimulatedFill(
            timestamp=TS,
            intent_type=IntentType.PERP_OPEN,
            protocol="gmx",
            tokens=["WETH", "USDC"],
            executed_price=WETH_PRICE,
            amount_usd=Decimal("120"),
            fee_usd=Decimal("0"),
            slippage_usd=Decimal("0"),
            gas_cost_usd=Decimal("0"),
            tokens_in={},
            tokens_out={"USDC": Decimal("50")},
            position_delta=position,
        )

        applied = portfolio.apply_fill(fill)

        assert applied is False
        assert portfolio.cash_usd == Decimal("100")
        assert portfolio.positions == []
        assert "insufficient cash" in portfolio.trades[0].metadata["failure_reason"]


class TestProducerFailedFills:
    """Adapter-produced success=False fills must be record-only."""

    def test_failed_fill_from_adapter_is_not_applied(self, portfolio: SimulatedPortfolio) -> None:
        """e.g. lending adapter health-factor rejections carry tokens_out;
        applying their flows would debit balances for a trade that never
        happened."""
        portfolio.tokens["WETH"] = Decimal("2")
        fill = make_swap_fill(
            tokens_out={"WETH": Decimal("1")},
            tokens_in={"USDC": Decimal("3000")},
            amount_usd=Decimal("3000"),
        )
        fill.success = False
        fill.metadata["failure_reason"] = "Health factor would be below 1.0"

        applied = portfolio.apply_fill(fill)

        assert applied is False
        assert portfolio.tokens["WETH"] == Decimal("2")
        assert portfolio.cash_usd == Decimal("10000")
        assert portfolio.trades[0].success is False
        # The producer's reason is preserved, not overwritten
        assert portfolio.trades[0].metadata["failure_reason"] == "Health factor would be below 1.0"


class TestCreditKeyIdentity:
    """ALM-2960: credits resolve to the same key identity debits use — a
    symbol-shaped credit must never mint a parallel entry beside the
    address-keyed funding plane (the unheld-token zero-seed then erases the
    real balance and re-entries freeze)."""

    BASE_WETH = ("base", "0x4200000000000000000000000000000000000006")

    def _address_keyed_portfolio(self) -> SimulatedPortfolio:
        portfolio = SimulatedPortfolio(initial_capital_usd=Decimal("0"), chain="base")
        # The funding plane: registered identities (what the engine loop
        # registers from the run's token_addresses map), fully deployed (no
        # dust) — the exact state in which the split-brain froze the
        # 6-month run.
        portfolio.register_token_identities({"WETH": self.BASE_WETH})
        return portfolio

    def test_symbol_credit_lands_on_the_address_key(self) -> None:
        portfolio = self._address_keyed_portfolio()

        fill = make_swap_fill(
            tokens_out={},
            tokens_in={"WETH": Decimal("0.65")},
        )
        fill.intent_type = IntentType.LP_CLOSE
        applied = portfolio.apply_fill(fill)

        assert applied is True
        assert portfolio.tokens.get(self.BASE_WETH) == Decimal("0.65")
        assert "WETH" not in portfolio.tokens  # no parallel symbol identity

    def test_credit_accretes_onto_a_held_address_key(self) -> None:
        portfolio = self._address_keyed_portfolio()
        portfolio.tokens[self.BASE_WETH] = Decimal("0.1")

        fill = make_swap_fill(tokens_out={}, tokens_in={"WETH": Decimal("0.65")})
        fill.intent_type = IntentType.LP_CLOSE
        portfolio.apply_fill(fill)

        assert portfolio.tokens[self.BASE_WETH] == Decimal("0.75")
        assert "WETH" not in portfolio.tokens

    def test_credit_prefers_an_existing_symbol_holding(self) -> None:
        # A portfolio already keyed by symbol keeps accreting there — the fix
        # unifies identity, it does not force-migrate legacy holdings.
        portfolio = SimulatedPortfolio(initial_capital_usd=Decimal("0"), chain="base")
        portfolio.tokens["WETH"] = Decimal("1")

        fill = make_swap_fill(tokens_out={}, tokens_in={"WETH": Decimal("0.5")})
        fill.intent_type = IntentType.LP_CLOSE
        portfolio.apply_fill(fill)

        assert portfolio.tokens["WETH"] == Decimal("1.5")
        assert self.BASE_WETH not in portfolio.tokens

    def test_unregistered_symbol_credits_under_its_own_key(self) -> None:
        # Symbols outside the run's registered plane keep legacy behavior.
        portfolio = self._address_keyed_portfolio()

        fill = make_swap_fill(tokens_out={}, tokens_in={"NOT-A-REAL-TOKEN": Decimal("5")})
        fill.intent_type = IntentType.LP_CLOSE
        portfolio.apply_fill(fill)

        assert portfolio.tokens.get("NOT-A-REAL-TOKEN") == Decimal("5")

    def test_unregistered_portfolio_keeps_symbol_keys(self) -> None:
        # Bare portfolios (no engine registration) are untouched by ALM-2960.
        portfolio = SimulatedPortfolio(initial_capital_usd=Decimal("0"), chain="base")

        fill = make_swap_fill(tokens_out={}, tokens_in={"WETH": Decimal("0.5")})
        fill.intent_type = IntentType.LP_CLOSE
        portfolio.apply_fill(fill)

        assert portfolio.tokens.get("WETH") == Decimal("0.5")
        assert self.BASE_WETH not in portfolio.tokens

    def test_stablecoin_credit_still_sweeps_to_cash(self) -> None:
        portfolio = self._address_keyed_portfolio()

        fill = make_swap_fill(tokens_out={}, tokens_in={"USDC": Decimal("2861")})
        fill.intent_type = IntentType.LP_CLOSE
        portfolio.apply_fill(fill)

        assert portfolio.cash_usd == Decimal("2861")
        assert all("USDC" not in str(k).upper() or portfolio.tokens[k] == 0 for k in portfolio.tokens)

    def test_registered_stablecoin_credit_sweeps_through_the_address_key(self) -> None:
        # Review round (#3310): with USDC itself REGISTERED, the credit maps
        # to its address-native key first — the cash-equivalent sweep must
        # recognize that key and still move the proceeds into cash_usd.
        base_usdc = ("base", "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913")
        portfolio = SimulatedPortfolio(initial_capital_usd=Decimal("0"), chain="base")
        portfolio.register_token_identities({"WETH": self.BASE_WETH, "USDC": base_usdc})

        fill = make_swap_fill(tokens_out={}, tokens_in={"USDC": Decimal("2861")})
        fill.intent_type = IntentType.LP_CLOSE
        portfolio.apply_fill(fill)

        assert portfolio.cash_usd == Decimal("2861")
        assert base_usdc not in portfolio.tokens  # swept, not left as a token
        assert "USDC" not in portfolio.tokens

    def test_registered_identity_debits_find_the_credited_balance(self) -> None:
        # Review round (#3310, Codex): a symbol UNKNOWN to the global token
        # registry but registered via token_funding must round-trip — credit
        # lands on the registered key, and a later symbol-shaped DEBIT must
        # find it there instead of rejecting a funded fill.
        fantasy_key = ("base", "0x00000000000000000000000000000000000fee75")
        portfolio = SimulatedPortfolio(initial_capital_usd=Decimal("0"), chain="base")
        portfolio.register_token_identities({"FANTASYTOKEN": fantasy_key})

        credit = make_swap_fill(tokens_out={}, tokens_in={"FANTASYTOKEN": Decimal("10")})
        credit.intent_type = IntentType.LP_CLOSE
        assert portfolio.apply_fill(credit) is True
        assert portfolio.tokens.get(fantasy_key) == Decimal("10")

        debit = make_swap_fill(
            tokens_out={"FANTASYTOKEN": Decimal("4")},
            tokens_in={"WETH": Decimal("0.001")},
        )
        assert portfolio.apply_fill(debit) is True
        assert portfolio.tokens.get(fantasy_key) == Decimal("6")
