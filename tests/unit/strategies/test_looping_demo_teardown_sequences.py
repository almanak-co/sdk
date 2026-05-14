from decimal import Decimal
from unittest.mock import MagicMock

from almanak.demo_strategies.aave_loop_mantle.strategy import AaveLoopMantleStrategy
from almanak.demo_strategies.aave_paper_trade_leverage_polygon.strategy import (
    AavePaperTradeLeveragePolygonStrategy,
)
from almanak.demo_strategies.morpho_looping.strategy import MorphoLoopingStrategy
from almanak.framework.intents import RepayIntent, SwapIntent, WithdrawIntent
from almanak.framework.teardown import TeardownMode


def _market(prices: dict[str, Decimal]) -> MagicMock:
    market = MagicMock()
    market.price.side_effect = lambda token: prices[token]
    return market


def test_aave_loop_mantle_teardown_withdraws_before_swap_and_repay() -> None:
    strategy = AaveLoopMantleStrategy.__new__(AaveLoopMantleStrategy)
    strategy._chain = "mantle"
    strategy.supply_token = "WETH"
    strategy.borrow_token = "USDC"
    strategy._total_supplied = Decimal("1")
    strategy._total_borrowed = Decimal("1000")
    strategy._supply_price_usd = Decimal("2000")
    strategy._borrow_price_usd = Decimal("1")

    intents = strategy.generate_teardown_intents(
        TeardownMode.SOFT,
        market=_market({"USDC": Decimal("1"), "WETH": Decimal("2000")}),
    )

    assert [intent.intent_type.value for intent in intents] == [
        "WITHDRAW",
        "SWAP",
        "REPAY",
        "WITHDRAW",
    ]

    partial_withdraw = intents[0]
    assert isinstance(partial_withdraw, WithdrawIntent)
    assert partial_withdraw.token == "WETH"
    assert partial_withdraw.amount == Decimal("0.55")
    assert partial_withdraw.withdraw_all is False

    swap = intents[1]
    assert isinstance(swap, SwapIntent)
    assert swap.from_token == "WETH"
    assert swap.to_token == "USDC"
    assert swap.amount == Decimal("0.55")

    repay = intents[2]
    assert isinstance(repay, RepayIntent)
    assert repay.token == "USDC"
    assert repay.repay_full is True

    final_withdraw = intents[3]
    assert isinstance(final_withdraw, WithdrawIntent)
    assert final_withdraw.token == "WETH"
    assert final_withdraw.withdraw_all is True


def test_morpho_looping_teardown_sources_debt_token_before_repay() -> None:
    strategy = MorphoLoopingStrategy.__new__(MorphoLoopingStrategy)
    strategy._chain = "ethereum"
    strategy.market_id = "0xmarket"
    strategy.collateral_token = "wstETH"
    strategy.borrow_token = "USDC"
    strategy.swap_slippage = Decimal("0.005")
    strategy._total_collateral = Decimal("1")
    strategy._total_borrowed = Decimal("1700")
    strategy._pending_wallet_collateral = Decimal("0")

    intents = strategy.generate_teardown_intents(
        TeardownMode.SOFT,
        market=_market({"USDC": Decimal("1"), "wstETH": Decimal("3400")}),
    )

    assert [intent.intent_type.value for intent in intents] == [
        "WITHDRAW",
        "SWAP",
        "REPAY",
        "WITHDRAW",
        "SWAP",
    ]

    partial_withdraw = intents[0]
    assert isinstance(partial_withdraw, WithdrawIntent)
    assert partial_withdraw.token == "wstETH"
    assert partial_withdraw.amount == Decimal("0.55")
    assert partial_withdraw.withdraw_all is False

    repay_funding_swap = intents[1]
    assert isinstance(repay_funding_swap, SwapIntent)
    assert repay_funding_swap.from_token == "wstETH"
    assert repay_funding_swap.to_token == "USDC"
    assert repay_funding_swap.amount == Decimal("0.55")

    repay = intents[2]
    assert isinstance(repay, RepayIntent)
    assert repay.repay_full is True

    final_withdraw = intents[3]
    assert isinstance(final_withdraw, WithdrawIntent)
    assert final_withdraw.withdraw_all is True

    final_recovery_swap = intents[4]
    assert isinstance(final_recovery_swap, SwapIntent)
    assert final_recovery_swap.from_token == "wstETH"
    assert final_recovery_swap.to_token == "USDC"
    assert final_recovery_swap.amount == "all"


def test_aave_paper_trade_leverage_metadata_includes_teardown_intents() -> None:
    intent_types = AavePaperTradeLeveragePolygonStrategy.STRATEGY_METADATA.intent_types

    assert "REPAY" in intent_types
    assert "WITHDRAW" in intent_types
