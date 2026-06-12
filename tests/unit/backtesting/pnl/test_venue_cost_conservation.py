"""Venue-cost (fee/slippage) cash-debit invariants for SimulatedPortfolio.apply_fill.

Regression guard for the venue-fee gap (2026-06, VIB-5079): apply_fill
debited only ``gas_cost_usd`` from cash, so every non-SWAP fill recorded
``fee_usd`` / ``slippage_usd`` on its TradeRecord without ever charging
them against portfolio value -- backtests overstated PnL by exactly the
venue costs of every LP / perp / lending / vault trade.

The decision table: a venue cost is debited from cash exactly when nothing
else in the fill already embodies it.

==============  ========  =============  =====================================
Intent type     fee_usd   slippage_usd   Where the cost lives
==============  ========  =============  =====================================
SWAP            embedded  embedded       netted out of ``tokens_in``
                                         (``_calculate_swap_flows``)
PERP_OPEN /     cash      embedded       slippage is in ``executed_price``
PERP_CLOSE                               (adverse per side, flows into entry
                                         price / realized PnL); fees are paid
                                         in cash
everything      cash      cash           flows are sized at oracle price for
else                                     the full notional; ``executed_price``
                                         never enters valuation
==============  ========  =============  =====================================

Companion to ``test_portfolio_conservation.py`` (token-flow conservation)
and ``test_perp_conservation.py`` (collateral-lane conservation).
"""

from datetime import UTC, datetime
from decimal import Decimal

import pytest

from almanak.framework.backtesting.models import IntentType
from almanak.framework.backtesting.pnl.data_provider import MarketState
from almanak.framework.backtesting.pnl.portfolio import (
    SimulatedFill,
    SimulatedPortfolio,
    SimulatedPosition,
)

WETH_PRICE = Decimal("3000")
TS = datetime(2025, 11, 1, tzinfo=UTC)

FEE = Decimal("15")
SLIPPAGE = Decimal("5")
GAS = Decimal("2")


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


def make_fill(
    intent_type: IntentType,
    *,
    protocol: str,
    tokens_out: dict[str, Decimal] | None = None,
    tokens_in: dict[str, Decimal] | None = None,
    amount_usd: Decimal = Decimal("0"),
    fee_usd: Decimal = FEE,
    slippage_usd: Decimal = SLIPPAGE,
    gas_cost_usd: Decimal = GAS,
    position_delta: SimulatedPosition | None = None,
    position_close_id: str | None = None,
    metadata: dict | None = None,
) -> SimulatedFill:
    tokens_out = tokens_out or {}
    tokens_in = tokens_in or {}
    return SimulatedFill(
        timestamp=TS,
        intent_type=intent_type,
        protocol=protocol,
        tokens=list(tokens_out) + list(tokens_in),
        executed_price=WETH_PRICE,
        amount_usd=amount_usd,
        fee_usd=fee_usd,
        slippage_usd=slippage_usd,
        gas_cost_usd=gas_cost_usd,
        tokens_in=tokens_in,
        tokens_out=tokens_out,
        position_delta=position_delta,
        position_close_id=position_close_id,
        metadata=metadata or {},
    )


def lp_position() -> SimulatedPosition:
    return SimulatedPosition.lp(
        token0="WETH",
        token1="USDC",
        amount0=Decimal("1"),
        amount1=Decimal("3000"),
        liquidity=Decimal("6000"),
        tick_lower=-100,
        tick_upper=100,
        fee_tier=Decimal("0.003"),
        entry_price=WETH_PRICE,
        entry_time=TS,
    )


class TestSwapCostsStayEmbedded:
    """SWAP fee/slippage are netted in tokens_in; cash must not be debited again."""

    def test_swap_buy_debits_only_notional_and_gas(
        self, portfolio: SimulatedPortfolio, market_state: MarketState
    ) -> None:
        amount = Decimal("3000")
        weth_in = (amount - FEE - SLIPPAGE) / WETH_PRICE

        applied = portfolio.apply_fill(
            make_fill(
                IntentType.SWAP,
                protocol="uniswap_v3",
                tokens_out={"USDC": amount},
                tokens_in={"WETH": weth_in},
                amount_usd=amount,
            )
        )

        assert applied is True
        # Cash drops by notional + gas only; fee/slippage live in the haircut inflow.
        assert portfolio.cash_usd == Decimal("10000") - amount - GAS
        assert portfolio.get_total_value_usd(market_state) == Decimal("10000") - FEE - SLIPPAGE - GAS


class TestLpVenueCosts:
    """LP flows are sized at oracle price; fee AND slippage must hit cash."""

    def test_lp_open_charges_fee_and_slippage(self, portfolio: SimulatedPortfolio, market_state: MarketState) -> None:
        fill = make_fill(
            IntentType.LP_OPEN,
            protocol="uniswap_v3",
            tokens_out={"WETH": Decimal("1"), "USDC": Decimal("3000")},
            amount_usd=Decimal("6000"),
            position_delta=lp_position(),
        )

        applied = portfolio.apply_fill(fill, market_state=market_state)

        assert applied is True
        # 10000 - 3000 (implicit WETH conversion) - 3000 (USDC leg) - 15 - 5 - 2
        assert portfolio.cash_usd == Decimal("3978")
        # Position is worth its 6000 notional, so total drops by exactly the costs.
        assert portfolio.get_total_value_usd(market_state) == Decimal("10000") - FEE - SLIPPAGE - GAS

    def test_lp_close_charges_fee_and_slippage(self, portfolio: SimulatedPortfolio, market_state: MarketState) -> None:
        position = lp_position()
        portfolio.positions.append(position)
        value_before = portfolio.get_total_value_usd(market_state)
        assert value_before == Decimal("16000")  # 10000 cash + 6000 LP

        applied = portfolio.apply_fill(
            make_fill(
                IntentType.LP_CLOSE,
                protocol="uniswap_v3",
                tokens_in={"WETH": Decimal("1"), "USDC": Decimal("3000")},
                amount_usd=Decimal("6000"),
                position_close_id=position.position_id,
            ),
            market_state=market_state,
        )

        assert applied is True
        # 10000 + 3000 swept USDC - 15 - 5 - 2
        assert portfolio.cash_usd == Decimal("12978")
        assert portfolio.get_total_value_usd(market_state) == value_before - FEE - SLIPPAGE - GAS


class TestPerpVenueCosts:
    """Perps pay fees in cash; slippage is already in executed_price."""

    def test_perp_open_charges_fee_but_not_slippage(
        self, portfolio: SimulatedPortfolio, market_state: MarketState
    ) -> None:
        position = SimulatedPosition.perp_long(
            token="WETH",
            collateral_usd=Decimal("1000"),
            leverage=Decimal("5"),
            entry_price=WETH_PRICE,
            entry_time=TS,
            protocol="gmx",
        )

        applied = portfolio.apply_fill(
            make_fill(
                IntentType.PERP_OPEN,
                protocol="gmx",
                amount_usd=Decimal("5000"),
                position_delta=position,
            ),
            market_state=market_state,
        )

        assert applied is True
        # Collateral + fee + gas leave cash; slippage_usd does NOT (it is
        # embodied in executed_price and would double-count).
        assert portfolio.cash_usd == Decimal("10000") - Decimal("1000") - FEE - GAS
        assert portfolio.get_total_value_usd(market_state) == Decimal("10000") - FEE - GAS

    def test_perp_close_charges_fee_but_not_slippage(
        self, portfolio: SimulatedPortfolio, market_state: MarketState
    ) -> None:
        position = SimulatedPosition.perp_long(
            token="WETH",
            collateral_usd=Decimal("1000"),
            leverage=Decimal("5"),
            entry_price=WETH_PRICE,
            entry_time=TS,
            protocol="gmx",
        )
        portfolio.positions.append(position)
        portfolio.cash_usd -= position.collateral_usd  # as PERP_OPEN would have left it

        realized = Decimal("500")  # +10% on 5000 notional
        fill = make_fill(
            IntentType.PERP_CLOSE,
            protocol="gmx",
            amount_usd=Decimal("5000"),
            position_close_id=position.position_id,
            metadata={"realized_pnl_usd": str(realized)},
        )

        applied = portfolio.apply_fill(fill, market_state=market_state)

        assert applied is True
        # 9000 + collateral + realized PnL - fee - gas; slippage_usd untouched.
        assert portfolio.cash_usd == Decimal("9000") + Decimal("1000") + realized - FEE - GAS
        assert portfolio.get_total_value_usd(market_state) == Decimal("10000") + realized - FEE - GAS


class TestLendingAndVaultVenueCosts:
    """Lending/vault flows are sized at oracle price; both costs hit cash."""

    def test_supply_charges_fee_and_slippage(self, portfolio: SimulatedPortfolio, market_state: MarketState) -> None:
        position = SimulatedPosition.supply(
            token="USDC",
            amount=Decimal("5000"),
            apy=Decimal("0.05"),
            entry_price=Decimal("1"),
            entry_time=TS,
        )

        applied = portfolio.apply_fill(
            make_fill(
                IntentType.SUPPLY,
                protocol="aave_v3",
                tokens_out={"USDC": Decimal("5000")},
                amount_usd=Decimal("5000"),
                position_delta=position,
            ),
            market_state=market_state,
        )

        assert applied is True
        assert portfolio.cash_usd == Decimal("10000") - Decimal("5000") - FEE - SLIPPAGE - GAS
        assert portfolio.get_total_value_usd(market_state) == Decimal("10000") - FEE - SLIPPAGE - GAS

    def test_withdraw_charges_fee_and_slippage(self, portfolio: SimulatedPortfolio, market_state: MarketState) -> None:
        position = SimulatedPosition.supply(
            token="USDC",
            amount=Decimal("5000"),
            apy=Decimal("0.05"),
            entry_price=Decimal("1"),
            entry_time=TS,
        )
        portfolio.positions.append(position)
        value_before = portfolio.get_total_value_usd(market_state)
        assert value_before == Decimal("15000")

        applied = portfolio.apply_fill(
            make_fill(
                IntentType.WITHDRAW,
                protocol="aave_v3",
                tokens_in={"USDC": Decimal("5000")},
                amount_usd=Decimal("5000"),
                position_close_id=position.position_id,
            ),
            market_state=market_state,
        )

        assert applied is True
        assert portfolio.cash_usd == Decimal("15000") - FEE - SLIPPAGE - GAS
        assert portfolio.get_total_value_usd(market_state) == value_before - FEE - SLIPPAGE - GAS

    def test_borrow_charges_fee_and_slippage(self, portfolio: SimulatedPortfolio, market_state: MarketState) -> None:
        position = SimulatedPosition.borrow(
            token="USDC",
            amount=Decimal("5000"),
            apy=Decimal("0.08"),
            entry_price=Decimal("1"),
            entry_time=TS,
        )

        applied = portfolio.apply_fill(
            make_fill(
                IntentType.BORROW,
                protocol="aave_v3",
                tokens_in={"USDC": Decimal("5000")},
                amount_usd=Decimal("5000"),
                position_delta=position,
            ),
            market_state=market_state,
        )

        assert applied is True
        # Borrowed stables sweep into cash; the debt position offsets them.
        assert portfolio.cash_usd == Decimal("15000") - FEE - SLIPPAGE - GAS
        assert portfolio.get_total_value_usd(market_state) == Decimal("10000") - FEE - SLIPPAGE - GAS

    def test_repay_charges_fee_and_slippage(self, portfolio: SimulatedPortfolio, market_state: MarketState) -> None:
        position = SimulatedPosition.borrow(
            token="USDC",
            amount=Decimal("5000"),
            apy=Decimal("0.08"),
            entry_price=Decimal("1"),
            entry_time=TS,
        )
        portfolio.positions.append(position)
        value_before = portfolio.get_total_value_usd(market_state)
        assert value_before == Decimal("5000")  # 10000 cash - 5000 debt

        applied = portfolio.apply_fill(
            make_fill(
                IntentType.REPAY,
                protocol="aave_v3",
                tokens_out={"USDC": Decimal("5000")},
                amount_usd=Decimal("5000"),
                position_close_id=position.position_id,
            ),
            market_state=market_state,
        )

        assert applied is True
        assert portfolio.cash_usd == Decimal("5000") - FEE - SLIPPAGE - GAS
        assert portfolio.get_total_value_usd(market_state) == value_before - FEE - SLIPPAGE - GAS

    def test_vault_deposit_charges_fee_and_slippage(
        self, portfolio: SimulatedPortfolio, market_state: MarketState
    ) -> None:
        position = SimulatedPosition.supply(
            token="USDC",
            amount=Decimal("5000"),
            apy=Decimal("0.05"),
            entry_price=Decimal("1"),
            entry_time=TS,
            protocol="erc4626",
        )

        applied = portfolio.apply_fill(
            make_fill(
                IntentType.VAULT_DEPOSIT,
                protocol="erc4626",
                tokens_out={"USDC": Decimal("5000")},
                amount_usd=Decimal("5000"),
                position_delta=position,
            ),
            market_state=market_state,
        )

        assert applied is True
        assert portfolio.cash_usd == Decimal("10000") - Decimal("5000") - FEE - SLIPPAGE - GAS
        assert portfolio.get_total_value_usd(market_state) == Decimal("10000") - FEE - SLIPPAGE - GAS

    def test_vault_redeem_charges_fee_and_slippage(
        self, portfolio: SimulatedPortfolio, market_state: MarketState
    ) -> None:
        position = SimulatedPosition.supply(
            token="USDC",
            amount=Decimal("5000"),
            apy=Decimal("0.05"),
            entry_price=Decimal("1"),
            entry_time=TS,
            protocol="erc4626",
        )
        portfolio.positions.append(position)
        value_before = portfolio.get_total_value_usd(market_state)

        applied = portfolio.apply_fill(
            make_fill(
                IntentType.VAULT_REDEEM,
                protocol="erc4626",
                tokens_in={"USDC": Decimal("5000")},
                amount_usd=Decimal("5000"),
                position_close_id=position.position_id,
            ),
            market_state=market_state,
        )

        assert applied is True
        assert portfolio.get_total_value_usd(market_state) == value_before - FEE - SLIPPAGE - GAS


class TestNoFlowIntentVenueCosts:
    """Intent types without a flow handler still pay venue costs from cash.

    BRIDGE (like HOLD and any future type) falls through
    ``calculate_token_flows`` with empty flows and creates no position, so
    nothing else can embody its costs -- the catch-all debit lane is the
    only place they can land.
    """

    def test_bridge_charges_fee_and_slippage(self, portfolio: SimulatedPortfolio, market_state: MarketState) -> None:
        applied = portfolio.apply_fill(
            make_fill(
                IntentType.BRIDGE,
                protocol="across",
                amount_usd=Decimal("5000"),
            ),
            market_state=market_state,
        )

        assert applied is True
        assert portfolio.cash_usd == Decimal("10000") - FEE - SLIPPAGE - GAS
        assert portfolio.get_total_value_usd(market_state) == Decimal("10000") - FEE - SLIPPAGE - GAS


class TestVenueCostFunding:
    """Venue costs join the aggregate cash check, mirroring gas semantics."""

    def test_fill_that_cannot_fund_venue_costs_is_rejected(self, market_state: MarketState) -> None:
        """Notional alone is affordable, notional + fee is not -> reject before mutation."""
        portfolio = SimulatedPortfolio(initial_capital_usd=Decimal("5010"))

        applied = portfolio.apply_fill(
            make_fill(
                IntentType.SUPPLY,
                protocol="aave_v3",
                tokens_out={"USDC": Decimal("5000")},
                amount_usd=Decimal("5000"),
                fee_usd=FEE,
                slippage_usd=SLIPPAGE,
                gas_cost_usd=GAS,
            ),
            market_state=market_state,
        )

        assert applied is False
        assert portfolio.cash_usd == Decimal("5010")
        assert portfolio.positions == []
        trade = portfolio.trades[0]
        assert trade.success is False
        assert "insufficient cash" in trade.metadata["failure_reason"]
        # Rejected fills charge nothing; originals stashed for the books.
        assert trade.fee_usd == Decimal("0")
        assert trade.slippage_usd == Decimal("0")
        assert trade.metadata["fee_usd_unapplied"] == str(FEE)

    def test_perp_open_funding_check_includes_fee(self, market_state: MarketState) -> None:
        """Collateral alone fits, collateral + fee + gas does not -> reject."""
        portfolio = SimulatedPortfolio(initial_capital_usd=Decimal("1010"))
        position = SimulatedPosition.perp_long(
            token="WETH",
            collateral_usd=Decimal("1000"),
            leverage=Decimal("5"),
            entry_price=WETH_PRICE,
            entry_time=TS,
            protocol="gmx",
        )

        applied = portfolio.apply_fill(
            make_fill(
                IntentType.PERP_OPEN,
                protocol="gmx",
                amount_usd=Decimal("5000"),
                position_delta=position,
            ),
            market_state=market_state,
        )

        assert applied is False
        assert portfolio.cash_usd == Decimal("1010")
        assert portfolio.positions == []

    def test_cash_poor_close_still_applies_and_charges_costs(self, market_state: MarketState) -> None:
        """Risk-reducing fills with no cash legs are never blocked by venue
        costs; the costs are charged unconditionally (transient negative
        cash is an accepted modeling choice -- a debit cannot mint value)."""
        portfolio = SimulatedPortfolio(initial_capital_usd=Decimal("0"))
        position = SimulatedPosition.perp_long(
            token="WETH",
            collateral_usd=Decimal("1000"),
            leverage=Decimal("5"),
            entry_price=WETH_PRICE,
            entry_time=TS,
            protocol="gmx",
        )
        portfolio.positions.append(position)

        applied = portfolio.apply_fill(
            make_fill(
                IntentType.PERP_CLOSE,
                protocol="gmx",
                amount_usd=Decimal("5000"),
                position_close_id=position.position_id,
                metadata={"realized_pnl_usd": "0"},
            ),
            market_state=market_state,
        )

        assert applied is True
        assert portfolio.positions == []
        assert portfolio.cash_usd == Decimal("1000") - FEE - GAS

    def test_zero_cost_fill_is_unaffected(self, portfolio: SimulatedPortfolio, market_state: MarketState) -> None:
        """Zero fee/slippage/gas keeps every lane value-neutral (regression
        guard: the venue-cost debit must not invent charges)."""
        position = SimulatedPosition.supply(
            token="USDC",
            amount=Decimal("5000"),
            apy=Decimal("0.05"),
            entry_price=Decimal("1"),
            entry_time=TS,
        )

        applied = portfolio.apply_fill(
            make_fill(
                IntentType.SUPPLY,
                protocol="aave_v3",
                tokens_out={"USDC": Decimal("5000")},
                amount_usd=Decimal("5000"),
                fee_usd=Decimal("0"),
                slippage_usd=Decimal("0"),
                gas_cost_usd=Decimal("0"),
                position_delta=position,
            ),
            market_state=market_state,
        )

        assert applied is True
        assert portfolio.get_total_value_usd(market_state) == Decimal("10000")
