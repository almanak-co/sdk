"""Conservation-of-value invariants for the perp lane of the PnL backtester.

Legacy perp intents without explicit collateral token amounts use the cash
lane. Connector-authenticated perp intents debit their declared collateral
token on open and return it on close:

- PERP_OPEN moves collateral from cash or token holdings into the position.
  Opening a perp must not change total portfolio value apart from modeled
  venue costs.
- PERP_CLOSE moves collateral + realized PnL (price PnL + accumulated
  funding) back into the funding asset. Closing must not change total
  portfolio value at the close instant.
- A round trip therefore moves total portfolio value by exactly the
  realized PnL (plus modeled execution costs).

Without this wiring the books are broken in both directions: the open
mints the collateral (position valued at collateral + uPnL while cash is
never debited) and the close burns collateral + uPnL (position vanishes,
nothing credits cash) -- a profitable round trip ends exactly at initial
capital.

Companion to ``test_portfolio_conservation.py`` (VIB-5082), which covers
the other token-flow lanes (SWAP / LP / lending); this file covers the
perp collateral lane.
"""

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from almanak.framework.backtesting.models import IntentType
from almanak.framework.backtesting.pnl.data_provider import MarketState
from almanak.framework.backtesting.pnl.engine import (
    DefaultFeeModel,
    DefaultSlippageModel,
    PnLBacktestConfig,
    PnLBacktester,
)
from almanak.framework.backtesting.pnl.portfolio import (
    SimulatedFill,
    SimulatedPortfolio,
    SimulatedPosition,
)
from tests.backtesting_funding import pnl_token_funding as _pnl_token_funding
from tests.unit.backtesting.pnl._mocks import MockDataProvider

TS = datetime(2024, 1, 1, 0, 0, tzinfo=UTC)
INITIAL_CASH = Decimal("10000")
COLLATERAL = Decimal("1000")
LEVERAGE = Decimal("5")
ENTRY_PRICE = Decimal("3000")
# 5000 notional, +10% price move => +500 long PnL
NOTIONAL = COLLATERAL * LEVERAGE


def market(price: Decimal) -> MarketState:
    return MarketState(
        timestamp=TS,
        prices={"WETH": price, "USDC": Decimal("1")},
        chain="arbitrum",
    )


def collateral_market(base_price: Decimal, collateral_price: Decimal) -> MarketState:
    return MarketState(
        timestamp=TS,
        prices={"WETH": base_price, "TBTC": collateral_price, "USDC": Decimal("1")},
        chain="arbitrum",
    )


def perp_long(collateral: Decimal = COLLATERAL) -> SimulatedPosition:
    return SimulatedPosition.perp_long(
        token="WETH",
        collateral_usd=collateral,
        leverage=LEVERAGE,
        entry_price=ENTRY_PRICE,
        entry_time=TS,
        protocol="gmx",
    )


def perp_short(collateral: Decimal = COLLATERAL) -> SimulatedPosition:
    return SimulatedPosition.perp_short(
        token="WETH",
        collateral_usd=collateral,
        leverage=LEVERAGE,
        entry_price=ENTRY_PRICE,
        entry_time=TS,
        protocol="gmx",
    )


def open_fill(position: SimulatedPosition) -> SimulatedFill:
    return SimulatedFill(
        timestamp=TS,
        intent_type=IntentType.PERP_OPEN,
        protocol="gmx",
        tokens=["WETH"],
        executed_price=ENTRY_PRICE,
        amount_usd=position.notional_usd,
        fee_usd=Decimal("0"),
        slippage_usd=Decimal("0"),
        gas_cost_usd=Decimal("0"),
        tokens_in={},
        tokens_out={},
        success=True,
        position_delta=position,
    )


def close_fill(
    position: SimulatedPosition,
    executed_price: Decimal,
    metadata: dict | None = None,
) -> SimulatedFill:
    return SimulatedFill(
        timestamp=TS,
        intent_type=IntentType.PERP_CLOSE,
        protocol="gmx",
        tokens=["WETH"],
        executed_price=executed_price,
        amount_usd=position.notional_usd,
        fee_usd=Decimal("0"),
        slippage_usd=Decimal("0"),
        gas_cost_usd=Decimal("0"),
        tokens_in={},
        tokens_out={},
        success=True,
        position_close_id=position.position_id,
        metadata=metadata or {},
    )


class TestPerpOpenConservation:
    """PERP_OPEN must move collateral from cash, not mint it."""

    def test_open_debits_collateral_from_cash(self) -> None:
        portfolio = SimulatedPortfolio(initial_capital_usd=INITIAL_CASH)

        portfolio.apply_fill(open_fill(perp_long()))

        assert portfolio.cash_usd == INITIAL_CASH - COLLATERAL
        assert len(portfolio.positions) == 1

    def test_open_conserves_total_value(self) -> None:
        portfolio = SimulatedPortfolio(initial_capital_usd=INITIAL_CASH)

        portfolio.apply_fill(open_fill(perp_long()))

        assert portfolio.get_total_value_usd(market(ENTRY_PRICE)) == INITIAL_CASH

    def test_open_conserves_value_under_mark_to_market(self) -> None:
        """mark_to_market at the entry instant agrees with get_total_value_usd."""
        portfolio = SimulatedPortfolio(initial_capital_usd=INITIAL_CASH)

        portfolio.apply_fill(open_fill(perp_long()))

        # Same timestamp as entry: zero elapsed time, so no funding accrual.
        assert portfolio.mark_to_market(market(ENTRY_PRICE), TS) == INITIAL_CASH

    def test_open_with_insufficient_cash_is_rejected_without_mutation(self) -> None:
        """A perp open the portfolio cannot fund must fail, not overdraw cash."""
        portfolio = SimulatedPortfolio(initial_capital_usd=Decimal("500"))
        fill = open_fill(perp_long(collateral=Decimal("1000")))

        portfolio.apply_fill(fill)

        assert fill.success is False
        assert "failure_reason" in fill.metadata
        assert portfolio.cash_usd == Decimal("500")
        assert portfolio.positions == []
        # The rejection is recorded as a failed trade.
        assert len(portfolio.trades) == 1
        assert portfolio.trades[0].success is False
        assert portfolio.get_total_value_usd(market(ENTRY_PRICE)) == Decimal("500")

    def test_open_with_full_collateral_applies_despite_gas(self) -> None:
        """ALM-2958: collateral (strategy capital) gates; gas (EOA-paid live)
        does not. Full-cash collateral fills and gas meters to the tank, so
        cash lands at exactly 0 rather than going negative."""
        portfolio = SimulatedPortfolio(initial_capital_usd=COLLATERAL)
        fill = open_fill(perp_long())  # collateral == full cash balance
        fill.gas_cost_usd = Decimal("5")

        portfolio.apply_fill(fill)

        assert fill.success is True
        assert portfolio.cash_usd == Decimal("0")  # collateral drawn; gas meters to tank
        assert portfolio.gas_tank_spent_usd == Decimal("5")
        assert len(portfolio.positions) == 1

    def test_open_where_collateral_alone_exceeds_cash_is_rejected(self) -> None:
        """The collateral gate survives ALM-2958 -- only gas left the sum."""
        portfolio = SimulatedPortfolio(initial_capital_usd=COLLATERAL - Decimal("1"))
        fill = open_fill(perp_long())  # collateral > cash balance
        fill.gas_cost_usd = Decimal("0")

        portfolio.apply_fill(fill)

        assert fill.success is False
        assert portfolio.cash_usd == COLLATERAL - Decimal("1")
        assert portfolio.positions == []

    def test_explicit_non_cash_collateral_moves_into_position(self) -> None:
        portfolio = SimulatedPortfolio(initial_capital_usd=Decimal("0"))
        portfolio.tokens["WETH"] = Decimal("0.5")
        position = perp_long(collateral=Decimal("1500"))
        position.metadata.update(
            {
                "perp_collateral_token": "WETH",
                "perp_collateral_amount": "0.5",
            }
        )
        fill = open_fill(position)
        fill.tokens_out = {"WETH": Decimal("0.5")}

        applied = portfolio.apply_fill(fill, market_state=market(ENTRY_PRICE))

        assert applied is True
        assert portfolio.tokens == {}
        assert portfolio.cash_usd == Decimal("0")
        assert position.metadata["perp_collateral_funding"] == "token"
        assert portfolio.get_total_value_usd(market(ENTRY_PRICE)) == Decimal("1500")

    def test_non_cash_collateral_funds_open_fee_without_negative_cash(self) -> None:
        portfolio = SimulatedPortfolio(initial_capital_usd=Decimal("0"))
        portfolio.tokens["WETH"] = Decimal("0.5")
        position = perp_long(collateral=Decimal("1500"))
        position.metadata.update(
            {
                "perp_collateral_token": "WETH",
                "perp_collateral_amount": "0.5",
            }
        )
        fill = open_fill(position)
        fill.tokens_out = {"WETH": Decimal("0.5")}
        fill.fee_usd = Decimal("15")

        applied = portfolio.apply_fill(fill, market_state=market(ENTRY_PRICE))

        assert applied is True
        assert portfolio.cash_usd == Decimal("0")
        assert position.collateral_usd == Decimal("1485")
        assert position.metadata["perp_collateral_units"] == "0.495"
        assert fill.metadata["venue_costs_funded_from_token_collateral_usd"] == "15"
        assert portfolio.get_total_value_usd(market(ENTRY_PRICE)) == Decimal("1485")

    def test_explicit_stable_collateral_can_fund_fee_from_deposit(self) -> None:
        portfolio = SimulatedPortfolio(initial_capital_usd=Decimal("1000"))
        position = perp_long(collateral=Decimal("1000"))
        position.metadata.update(
            {
                "perp_collateral_token": "USDC",
                "perp_collateral_amount": "1000",
            }
        )
        fill = open_fill(position)
        fill.tokens_out = {"USDC": Decimal("1000")}
        fill.fee_usd = Decimal("15")

        applied = portfolio.apply_fill(fill, market_state=market(ENTRY_PRICE))

        assert applied is True
        assert portfolio.cash_usd == Decimal("0")
        assert position.collateral_usd == Decimal("985")
        assert fill.metadata["venue_costs_funded_from_token_collateral_usd"] == "15"
        assert portfolio.get_total_value_usd(market(ENTRY_PRICE)) == Decimal("985")


class TestPerpCloseConservation:
    """PERP_CLOSE must credit collateral + realized PnL back to cash."""

    def _open_portfolio(self, position: SimulatedPosition) -> SimulatedPortfolio:
        portfolio = SimulatedPortfolio(initial_capital_usd=INITIAL_CASH)
        portfolio.apply_fill(open_fill(position))
        return portfolio

    def test_long_round_trip_profit_lands_in_cash(self) -> None:
        position = perp_long()
        portfolio = self._open_portfolio(position)
        exit_price = ENTRY_PRICE * Decimal("1.1")  # +10% => +500 on 5000 notional

        portfolio.apply_fill(close_fill(position, exit_price))

        assert portfolio.positions == []
        assert portfolio.cash_usd == INITIAL_CASH + Decimal("500")
        assert portfolio.get_total_value_usd(market(exit_price)) == INITIAL_CASH + Decimal("500")
        assert portfolio.trades[-1].pnl_usd == Decimal("500")
        assert portfolio._realized_pnl == Decimal("500")

    def test_long_round_trip_loss_lands_in_cash(self) -> None:
        position = perp_long()
        portfolio = self._open_portfolio(position)
        exit_price = ENTRY_PRICE * Decimal("0.9")  # -10% => -500 on 5000 notional

        portfolio.apply_fill(close_fill(position, exit_price))

        assert portfolio.cash_usd == INITIAL_CASH - Decimal("500")
        assert portfolio.get_total_value_usd(market(exit_price)) == INITIAL_CASH - Decimal("500")
        assert portfolio.trades[-1].pnl_usd == Decimal("-500")

    def test_short_round_trip_profits_when_price_falls(self) -> None:
        position = perp_short()
        portfolio = self._open_portfolio(position)
        exit_price = ENTRY_PRICE * Decimal("0.9")  # -10% => +500 for the short

        portfolio.apply_fill(close_fill(position, exit_price))

        assert portfolio.cash_usd == INITIAL_CASH + Decimal("500")
        assert portfolio.trades[-1].pnl_usd == Decimal("500")

    def test_close_conserves_value_at_close_instant(self) -> None:
        """Total portfolio value must be identical just before and after close."""
        position = perp_long()
        portfolio = self._open_portfolio(position)
        exit_price = ENTRY_PRICE * Decimal("1.1")
        state = market(exit_price)

        value_before = portfolio.get_total_value_usd(state)
        portfolio.apply_fill(close_fill(position, exit_price))
        value_after = portfolio.get_total_value_usd(state)

        assert value_before == value_after

    def test_close_includes_accumulated_funding(self) -> None:
        """Funding the position accrued is part of the realized close credit."""
        position = perp_long()
        portfolio = self._open_portfolio(position)
        position.accumulated_funding = Decimal("-50")  # long paid 50 in funding
        state = market(ENTRY_PRICE)

        value_before = portfolio.get_total_value_usd(state)
        portfolio.apply_fill(close_fill(position, ENTRY_PRICE))

        assert portfolio.cash_usd == INITIAL_CASH - Decimal("50")
        assert portfolio.get_total_value_usd(state) == value_before
        assert portfolio.trades[-1].pnl_usd == Decimal("-50")

    def test_close_metadata_override_takes_precedence(self) -> None:
        """Adapter-supplied realized_pnl_usd drives both cash and the trade record."""
        position = perp_long()
        portfolio = self._open_portfolio(position)

        portfolio.apply_fill(
            close_fill(
                position,
                ENTRY_PRICE * Decimal("1.1"),
                metadata={"realized_pnl_usd": Decimal("123")},
            )
        )

        assert portfolio.cash_usd == INITIAL_CASH - COLLATERAL + COLLATERAL + Decimal("123")
        assert portfolio.trades[-1].pnl_usd == Decimal("123")

    def test_liquidated_position_close_credits_remaining_collateral_only(self) -> None:
        """Liquidation losses already live in collateral_usd; do not re-add price PnL."""
        position = perp_long()
        portfolio = self._open_portfolio(position)
        # Simulate what check_and_simulate_liquidation does: loss and penalty
        # are deducted from collateral_usd and the position is flagged.
        position.is_liquidated = True
        position.collateral_usd = Decimal("100")

        portfolio.apply_fill(close_fill(position, ENTRY_PRICE))

        assert portfolio.cash_usd == INITIAL_CASH - COLLATERAL + Decimal("100")
        assert portfolio.positions == []

    def test_token_funded_close_returns_collateral_and_pnl_in_token(self) -> None:
        portfolio = SimulatedPortfolio(initial_capital_usd=Decimal("0"))
        portfolio.tokens["WETH"] = Decimal("0.5")
        position = perp_long(collateral=Decimal("1500"))
        position.metadata.update(
            {
                "perp_collateral_token": "WETH",
                "perp_collateral_amount": "0.5",
            }
        )
        opened = open_fill(position)
        opened.tokens_out = {"WETH": Decimal("0.5")}
        assert portfolio.apply_fill(opened, market_state=market(ENTRY_PRICE)) is True

        exit_price = ENTRY_PRICE * Decimal("1.1")
        closing = close_fill(position, exit_price)
        assert portfolio.apply_fill(closing, market_state=market(exit_price)) is True

        realized = Decimal("750")
        expected_units = Decimal("0.5") + realized / exit_price
        assert portfolio.cash_usd == Decimal("0")
        assert portfolio.tokens["WETH"] == expected_units
        assert closing.tokens_in["WETH"] == expected_units
        assert portfolio.get_total_value_usd(market(exit_price)) == Decimal("2400")

    def test_partial_symbol_funded_collateral_settles_to_same_wallet_key(self) -> None:
        """A resolver address in the descriptor must not split a symbol-held
        balance when the authenticated open debit used the symbol key."""
        portfolio = SimulatedPortfolio(initial_capital_usd=Decimal("0"))
        portfolio.tokens["WETH"] = Decimal("1")
        position = perp_long(collateral=Decimal("1500"))
        position.metadata.update(
            {
                "perp_collateral_token": "WETH",
                "perp_collateral_address": "0x82af49447d8a07e3bd95bd0d56f35241523fbab1",
                "perp_collateral_amount": "0.5",
            }
        )
        opened = open_fill(position)
        opened.tokens_out = {"WETH": Decimal("0.5")}

        assert portfolio.apply_fill(opened, market_state=market(ENTRY_PRICE)) is True
        assert portfolio.tokens == {"WETH": Decimal("0.5")}
        assert position.metadata["perp_collateral_wallet_key"] == "WETH"
        assert position.perp_collateral_token_units == ("WETH", Decimal("0.5"))

        closing = close_fill(position, ENTRY_PRICE)
        assert portfolio.apply_fill(closing, market_state=market(ENTRY_PRICE)) is True
        assert portfolio.tokens == {"WETH": Decimal("1.0")}
        assert closing.tokens_in == {"WETH": Decimal("0.5")}

    def test_token_collateral_keeps_units_and_independent_price_exposure(self) -> None:
        portfolio = SimulatedPortfolio(initial_capital_usd=Decimal("0"))
        portfolio.tokens["TBTC"] = Decimal("0.5")
        position = perp_short(collateral=Decimal("1500"))
        position.metadata.update(
            {
                "perp_collateral_token": "TBTC",
                "perp_collateral_amount": "0.5",
            }
        )
        opened = open_fill(position)
        opened.tokens_out = {"TBTC": Decimal("0.5")}
        entry_state = collateral_market(ENTRY_PRICE, Decimal("3000"))
        assert portfolio.apply_fill(opened, market_state=entry_state) is True

        appreciated = collateral_market(ENTRY_PRICE, Decimal("3300"))
        assert portfolio.get_total_value_usd(appreciated) == Decimal("1650")
        assert portfolio.calculate_unrealized_pnl(appreciated) == Decimal("150")

        closing = close_fill(position, ENTRY_PRICE)
        assert portfolio.apply_fill(closing, market_state=appreciated) is True
        assert portfolio.cash_usd == Decimal("0")
        assert portfolio.tokens["TBTC"] == Decimal("0.5")
        assert closing.tokens_in["TBTC"] == Decimal("0.5")

    def test_liquidated_token_collateral_keeps_written_down_value(self) -> None:
        portfolio = SimulatedPortfolio(initial_capital_usd=Decimal("0"))
        portfolio.tokens["TBTC"] = Decimal("0.5")
        position = perp_short(collateral=Decimal("1500"))
        position.metadata.update(
            {
                "perp_collateral_token": "TBTC",
                "perp_collateral_amount": "0.5",
            }
        )
        opened = open_fill(position)
        opened.tokens_out = {"TBTC": Decimal("0.5")}
        assert (
            portfolio.apply_fill(
                opened,
                market_state=collateral_market(ENTRY_PRICE, Decimal("3000")),
            )
            is True
        )

        position.is_liquidated = True
        position.collateral_usd = Decimal("100")
        repriced = collateral_market(Decimal("4000"), Decimal("4000"))

        assert portfolio.get_total_value_usd(repriced) == Decimal("100")
        assert portfolio.calculate_unrealized_pnl(repriced) == Decimal("0")

    def test_token_collateral_basis_survives_close_and_prices_payout_at_exit(self) -> None:
        portfolio = SimulatedPortfolio(initial_capital_usd=Decimal("0"))
        portfolio.tokens["TBTC"] = Decimal("0.5")
        portfolio._cost_basis["TBTC"] = Decimal("2400")
        position = perp_short(collateral=Decimal("1500"))
        position.metadata.update(
            {
                "perp_collateral_token": "TBTC",
                "perp_collateral_amount": "0.5",
            }
        )
        opened = open_fill(position)
        opened.tokens_out = {"TBTC": Decimal("0.5")}
        assert (
            portfolio.apply_fill(
                opened,
                market_state=collateral_market(ENTRY_PRICE, Decimal("3000")),
            )
            is True
        )
        assert portfolio.tokens == {}
        assert portfolio._cost_basis == {}

        exit_base_price = Decimal("2700")
        exit_collateral_price = Decimal("3300")
        exit_state = collateral_market(exit_base_price, exit_collateral_price)
        closing = close_fill(position, exit_base_price)
        assert portfolio.apply_fill(closing, market_state=exit_state) is True

        realized_perp = Decimal("750")
        payout_units = realized_perp / exit_collateral_price
        returned_units = Decimal("0.5") + payout_units
        expected_basis = (Decimal("0.5") * Decimal("2400") + payout_units * exit_collateral_price) / returned_units
        assert portfolio.tokens["TBTC"] == returned_units
        assert portfolio._cost_basis["TBTC"] == expected_basis

        proceeds = returned_units * exit_collateral_price
        swap = SimulatedFill(
            timestamp=TS,
            intent_type=IntentType.SWAP,
            protocol="curve",
            tokens=["TBTC", "USDC"],
            executed_price=exit_collateral_price,
            amount_usd=proceeds,
            fee_usd=Decimal("0"),
            slippage_usd=Decimal("0"),
            gas_cost_usd=Decimal("0"),
            tokens_in={"USDC": proceeds},
            tokens_out={"TBTC": returned_units},
            success=True,
        )
        assert portfolio.apply_fill(swap, market_state=exit_state) is True
        assert portfolio.trades[-1].pnl_usd == Decimal("450")

    def test_loss_consumed_collateral_realizes_basis_before_remaining_swap(self) -> None:
        """Collateral units consumed by perp loss are disposed at the close
        price; a later swap can realize only the returned units' spot PnL."""
        portfolio = SimulatedPortfolio(initial_capital_usd=Decimal("0"))
        portfolio.tokens["TBTC"] = Decimal("0.5")
        portfolio._cost_basis["TBTC"] = Decimal("2400")
        position = perp_long(collateral=Decimal("1500"))
        position.metadata.update(
            {
                "perp_collateral_token": "TBTC",
                "perp_collateral_amount": "0.5",
            }
        )
        opened = open_fill(position)
        opened.tokens_out = {"TBTC": Decimal("0.5")}
        entry_state = collateral_market(ENTRY_PRICE, Decimal("3000"))
        assert portfolio.apply_fill(opened, market_state=entry_state) is True

        exit_base_price = Decimal("2800")
        exit_collateral_price = Decimal("3300")
        exit_state = collateral_market(exit_base_price, exit_collateral_price)
        closing = close_fill(position, exit_base_price)
        assert portfolio.apply_fill(closing, market_state=exit_state) is True

        perp_loss = Decimal("-500")
        consumed_units = -perp_loss / exit_collateral_price
        disposal_pnl = consumed_units * (exit_collateral_price - Decimal("2400"))
        returned_units = Decimal("0.5") - consumed_units
        assert closing.metadata["perp_collateral_disposal_pnl_usd"] == str(disposal_pnl)
        assert portfolio.trades[-1].pnl_usd == perp_loss + disposal_pnl

        proceeds = returned_units * exit_collateral_price
        swap = SimulatedFill(
            timestamp=TS,
            intent_type=IntentType.SWAP,
            protocol="curve",
            tokens=["TBTC", "USDC"],
            executed_price=exit_collateral_price,
            amount_usd=proceeds,
            fee_usd=Decimal("0"),
            slippage_usd=Decimal("0"),
            gas_cost_usd=Decimal("0"),
            tokens_in={"USDC": proceeds},
            tokens_out={"TBTC": returned_units},
            success=True,
        )
        assert portfolio.apply_fill(swap, market_state=exit_state) is True
        remaining_spot_pnl = returned_units * (exit_collateral_price - Decimal("2400"))
        assert portfolio.trades[-1].pnl_usd == remaining_spot_pnl
        assert portfolio._realized_pnl == Decimal("-50")

    def test_token_funded_close_embeds_fee_without_negative_cash(self) -> None:
        portfolio = SimulatedPortfolio(initial_capital_usd=Decimal("0"))
        portfolio.tokens["TBTC"] = Decimal("0.5")
        position = perp_short(collateral=Decimal("1500"))
        position.metadata.update(
            {
                "perp_collateral_token": "TBTC",
                "perp_collateral_amount": "0.5",
            }
        )
        state = collateral_market(ENTRY_PRICE, Decimal("3000"))
        opened = open_fill(position)
        opened.tokens_out = {"TBTC": Decimal("0.5")}
        assert portfolio.apply_fill(opened, market_state=state) is True

        closing = close_fill(position, ENTRY_PRICE)
        closing.fee_usd = Decimal("15")
        assert portfolio.apply_fill(closing, market_state=state) is True

        assert portfolio.cash_usd == Decimal("0")
        assert portfolio.tokens["TBTC"] == Decimal("0.495")
        assert closing.tokens_in["TBTC"] == Decimal("0.495")
        assert closing.metadata["venue_costs_funded_from_token_collateral_usd"] == "15"


class TestEngineGenericPerpLane:
    """Conservation through PnLBacktester._execute_intent (no adapter)."""

    @staticmethod
    def _backtester() -> PnLBacktester:
        return PnLBacktester(
            data_provider=MockDataProvider(),
            fee_models={"default": DefaultFeeModel(fee_pct=Decimal("0"))},
            slippage_models={"default": DefaultSlippageModel(slippage_pct=Decimal("0"))},
        )

    @staticmethod
    def _config() -> PnLBacktestConfig:
        return PnLBacktestConfig(
            start_time=TS,
            end_time=TS + timedelta(hours=1),
            token_funding=_pnl_token_funding(INITIAL_CASH),
            include_gas_costs=False,
        )

    @pytest.mark.asyncio
    async def test_engine_round_trip_conserves_value_and_realizes_pnl(self) -> None:
        @dataclass
        class PerpOpenStub:
            intent_type: str = "PERP_OPEN"
            token: str = "WETH"
            amount_usd: Decimal = COLLATERAL  # engine maps this to collateral
            leverage: Decimal = LEVERAGE
            is_long: bool = True
            protocol: str = "gmx"

        @dataclass
        class PerpCloseStub:
            position_id: str
            intent_type: str = "PERP_CLOSE"
            token: str = "WETH"
            amount_usd: Decimal = NOTIONAL
            protocol: str = "gmx"

        backtester = self._backtester()
        config = self._config()
        portfolio = SimulatedPortfolio(initial_capital_usd=INITIAL_CASH)
        entry_state = market(ENTRY_PRICE)

        await backtester._execute_intent(PerpOpenStub(), portfolio, entry_state, TS, config)

        assert len(portfolio.positions) == 1
        position = portfolio.positions[0]
        assert position.collateral_usd == COLLATERAL
        assert position.notional_usd == NOTIONAL
        assert portfolio.cash_usd == INITIAL_CASH - COLLATERAL
        assert portfolio.get_total_value_usd(entry_state) == INITIAL_CASH

        exit_price = ENTRY_PRICE * Decimal("1.1")
        exit_state = MarketState(
            timestamp=TS + timedelta(hours=1),
            prices={"WETH": exit_price, "USDC": Decimal("1")},
            chain="arbitrum",
        )

        await backtester._execute_intent(
            PerpCloseStub(position_id=position.position_id),
            portfolio,
            exit_state,
            TS + timedelta(hours=1),
            config,
        )

        assert portfolio.positions == []
        assert portfolio.cash_usd == INITIAL_CASH + Decimal("500")
        assert portfolio.get_total_value_usd(exit_state) == INITIAL_CASH + Decimal("500")
        assert portfolio.trades[-1].pnl_usd == Decimal("500")
