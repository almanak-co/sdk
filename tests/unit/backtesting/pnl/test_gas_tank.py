"""Gas tank contract: gas is operational spend, never strategy capital."""

from datetime import UTC, datetime
from decimal import Decimal

from almanak.framework.backtesting.models import IntentType
from almanak.framework.backtesting.pnl.portfolio import SimulatedPortfolio
from almanak.framework.backtesting.pnl.position_models import SimulatedFill


def _swap_fill(gas: str, amount_usd: str = "100") -> SimulatedFill:
    return SimulatedFill(
        timestamp=datetime(2024, 1, 1, tzinfo=UTC),
        intent_type=IntentType.SWAP,
        protocol="uniswap_v3",
        tokens=["USDC", "WETH"],
        executed_price=Decimal("2000"),
        amount_usd=Decimal(amount_usd),
        fee_usd=Decimal("0"),
        slippage_usd=Decimal("0"),
        gas_cost_usd=Decimal(gas),
        tokens_in={"WETH": Decimal(amount_usd) / Decimal("2000")},
        tokens_out={"USDC": Decimal(amount_usd)},
    )


class TestUnlimitedTank:
    def test_gas_meters_without_touching_cash(self) -> None:
        portfolio = SimulatedPortfolio(initial_capital_usd=Decimal("1000"))

        assert portfolio.apply_fill(_swap_fill(gas="2"))
        assert portfolio.apply_fill(_swap_fill(gas="3"))

        assert portfolio.gas_tank_spent_usd == Decimal("5")
        assert portfolio.cash_usd == Decimal("800")  # two $100 buys, no gas debit
        assert portfolio.gas_tank_remaining_usd is None

    def test_zero_gas_fill_does_not_draw(self) -> None:
        portfolio = SimulatedPortfolio(initial_capital_usd=Decimal("1000"))
        assert portfolio.apply_fill(_swap_fill(gas="0"))
        assert portfolio.gas_tank_spent_usd == Decimal("0")


class TestFiniteTank:
    def test_exhaustion_rejects_with_zero_drift(self) -> None:
        portfolio = SimulatedPortfolio(initial_capital_usd=Decimal("1000"), gas_tank_budget_usd=Decimal("4"))

        assert portfolio.apply_fill(_swap_fill(gas="3"))
        cash_before = portfolio.cash_usd
        tokens_before = dict(portfolio.tokens)

        applied = portfolio.apply_fill(_swap_fill(gas="3"))

        assert applied is False
        rejected = portfolio.trades[-1]
        assert rejected.success is False
        assert "gas tank exhausted" in (rejected.error or "")
        assert portfolio.cash_usd == cash_before
        assert portfolio.tokens == tokens_before
        assert portfolio.gas_tank_spent_usd == Decimal("3")  # rejected fill drew nothing

    def test_remaining_tracks_draws(self) -> None:
        portfolio = SimulatedPortfolio(initial_capital_usd=Decimal("1000"), gas_tank_budget_usd=Decimal("10"))
        assert portfolio.apply_fill(_swap_fill(gas="2.5"))
        assert portfolio.gas_tank_remaining_usd == Decimal("7.5")

    def test_exact_budget_fill_is_allowed(self) -> None:
        portfolio = SimulatedPortfolio(initial_capital_usd=Decimal("1000"), gas_tank_budget_usd=Decimal("2"))
        assert portfolio.apply_fill(_swap_fill(gas="2"))
        assert portfolio.gas_tank_remaining_usd == Decimal("0")


class TestConfigValidation:
    def test_negative_gas_funding_rejected(self) -> None:
        from datetime import UTC, datetime

        import pytest

        from almanak.framework.backtesting.pnl.config import PnLBacktestConfig

        with pytest.raises(ValueError, match="gas_funding_usd"):
            PnLBacktestConfig(
                start_time=datetime(2024, 1, 1, tzinfo=UTC),
                end_time=datetime(2024, 1, 2, tzinfo=UTC),
                gas_funding_usd=Decimal("-1"),
            )
