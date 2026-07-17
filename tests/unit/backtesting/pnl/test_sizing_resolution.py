"""Single-owner ``amount="all"`` resolution (ALM-2943 phase-1 contracts)."""

from datetime import UTC, datetime
from decimal import Decimal
from types import SimpleNamespace

from almanak.framework.backtesting.models import IntentType
from almanak.framework.backtesting.pnl.portfolio import SimulatedPortfolio
from almanak.framework.backtesting.pnl.sizing import (
    RejectionCode,
    ResolvedAllSizing,
    SizingRejection,
    apply_resolved_sizing,
    resolve_all_sizing,
)
from almanak.framework.intents.vocabulary import SwapIntent


def _market(prices: dict[str, str]):
    table = {token: Decimal(price) for token, price in prices.items()}

    def get_price(token):
        return table[token]

    return SimpleNamespace(prices=table, get_price=get_price, timestamp=datetime(2024, 1, 1, tzinfo=UTC))


class TestResolveAllSizing:
    def test_no_sentinel_returns_none(self) -> None:
        portfolio = SimulatedPortfolio(initial_capital_usd=Decimal("100"))
        intent = SwapIntent(from_token="WETH", to_token="USDC", amount=Decimal("1"))
        assert resolve_all_sizing(intent, IntentType.SWAP, portfolio, _market({"WETH": "2000"})) is None

    def test_swap_all_resolves_held_units(self) -> None:
        portfolio = SimulatedPortfolio(initial_capital_usd=Decimal("0"))
        portfolio.tokens["WETH"] = Decimal("0.5")
        intent = SwapIntent(from_token="WETH", to_token="USDC", amount="all")

        resolution = resolve_all_sizing(intent, IntentType.SWAP, portfolio, _market({"WETH": "2000"}))

        assert isinstance(resolution, ResolvedAllSizing)
        assert resolution.units == Decimal("0.5")
        assert resolution.amount_usd == Decimal("1000.0")

    def test_stablecoin_all_spends_cash_like(self) -> None:
        # Cash-equivalent stables live in cash_usd; "all" of one is the
        # full cash-like balance at $1.
        portfolio = SimulatedPortfolio(initial_capital_usd=Decimal("500"))
        intent = SwapIntent(from_token="USDC", to_token="WETH", amount="all")

        resolution = resolve_all_sizing(intent, IntentType.SWAP, portfolio, _market({"WETH": "2000", "USDC": "1"}))

        assert isinstance(resolution, ResolvedAllSizing)
        assert resolution.units == Decimal("500")
        assert resolution.amount_usd == Decimal("500")

    def test_empty_balance_rejects_typed(self) -> None:
        portfolio = SimulatedPortfolio(initial_capital_usd=Decimal("0"))
        intent = SwapIntent(from_token="WETH", to_token="USDC", amount="all")

        resolution = resolve_all_sizing(intent, IntentType.SWAP, portfolio, _market({"WETH": "2000"}))

        assert isinstance(resolution, SizingRejection)
        assert resolution.code is RejectionCode.INSUFFICIENT_BALANCE

    def test_unpriceable_rejects_typed(self) -> None:
        portfolio = SimulatedPortfolio(initial_capital_usd=Decimal("0"))
        portfolio.tokens["XYZ"] = Decimal("5")
        intent = SwapIntent(from_token="XYZ", to_token="USDC", amount="all")

        resolution = resolve_all_sizing(intent, IntentType.SWAP, portfolio, _market({"WETH": "2000"}))

        assert isinstance(resolution, SizingRejection)
        assert resolution.code is RejectionCode.UNPRICEABLE

    def test_perp_collateral_all_stays_fail_closed(self) -> None:
        # Not a wallet-sized type: rejected with the typed unsupported code
        # (the split-brain's two figures are unreachable — phase 5 lifts it).
        portfolio = SimulatedPortfolio(initial_capital_usd=Decimal("1000"))
        intent = SimpleNamespace(intent_type=IntentType.PERP_OPEN, market="ETH", collateral_amount="all", size_usd=None)

        resolution = resolve_all_sizing(intent, IntentType.PERP_OPEN, portfolio, _market({"WETH": "2000"}))

        assert isinstance(resolution, SizingRejection)
        assert resolution.code is RejectionCode.UNSUPPORTED_ALL_SIZING

    def test_perp_non_cash_collateral_all_rejects_typed(self) -> None:
        # Sizing from held WETH while the portfolio debits cash for margin
        # would measure one balance and spend another — refused, typed.
        portfolio = SimulatedPortfolio(initial_capital_usd=Decimal("0"))
        portfolio.tokens["WETH"] = Decimal("2")
        intent = SimpleNamespace(
            intent_type=IntentType.PERP_OPEN,
            market="ETH",
            collateral_token="WETH",
            collateral_amount="all",
            size_usd=None,
        )

        resolution = resolve_all_sizing(intent, IntentType.PERP_OPEN, portfolio, _market({"WETH": "2000"}))

        assert isinstance(resolution, SizingRejection)
        assert resolution.code is RejectionCode.UNSUPPORTED_ALL_SIZING
        assert "cash-equivalent" in resolution.detail

    def test_close_shaped_intents_are_not_resolved_here(self) -> None:
        # LP_CLOSE "all" is the close-in-full sentinel, owned by
        # position-close resolution.
        portfolio = SimulatedPortfolio(initial_capital_usd=Decimal("100"))
        intent = SimpleNamespace(intent_type=IntentType.LP_CLOSE, position_id="x", amount="all")
        assert resolve_all_sizing(intent, IntentType.LP_CLOSE, portfolio, _market({})) is None


class TestApplyResolvedSizing:
    def test_pydantic_intent_copied_not_mutated(self) -> None:
        intent = SwapIntent(from_token="WETH", to_token="USDC", amount="all")
        resolution = ResolvedAllSizing(token="WETH", units=Decimal("0.5"), amount_usd=Decimal("1000"))

        resolved = apply_resolved_sizing(intent, resolution)

        assert resolved.amount == Decimal("0.5")
        assert intent.amount == "all"  # original untouched

    def test_duck_intent_copied_not_mutated(self) -> None:
        intent = SimpleNamespace(intent_type=IntentType.SWAP, from_token="WETH", to_token="USDC", amount="all")
        resolution = ResolvedAllSizing(token="WETH", units=Decimal("2"), amount_usd=Decimal("4000"))

        resolved = apply_resolved_sizing(intent, resolution)

        assert resolved.amount == Decimal("2")
        assert intent.amount == "all"


class TestOffParStablecoinInput:
    """The depeg cases the review asked for: cash-equivalent inputs are
    USD-denominated inside the sim and never re-priced at market."""

    def test_cash_stable_all_ignores_depegged_market_price(self) -> None:
        portfolio = SimulatedPortfolio(initial_capital_usd=Decimal("500"))
        intent = SwapIntent(from_token="USDC", to_token="WETH", amount="all")

        resolution = resolve_all_sizing(
            intent, IntentType.SWAP, portfolio, _market({"WETH": "2000", "USDC": "0.98"})
        )

        assert isinstance(resolution, ResolvedAllSizing)
        # Units == the spendable dollars; re-pricing at 0.98 would claim
        # 510.2 "units" the cash plane cannot debit.
        assert resolution.units == Decimal("500")
        assert resolution.amount_usd == Decimal("500")

    def test_non_stable_input_still_prices_at_market(self) -> None:
        portfolio = SimulatedPortfolio(initial_capital_usd=Decimal("0"))
        portfolio.tokens["WETH"] = Decimal("2")

        resolution = resolve_all_sizing(
            SwapIntent(from_token="WETH", to_token="USDC", amount="all"),
            IntentType.SWAP,
            portfolio,
            _market({"WETH": "1900"}),
        )

        assert isinstance(resolution, ResolvedAllSizing)
        assert resolution.amount_usd == Decimal("3800")
