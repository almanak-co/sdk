"""Regression tests for the two lifecycle defects salvaged from PR #3506."""

from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import MagicMock

from almanak.framework.intents import IntentType
from strategies.incubating.benqi_lending_lifecycle_avalanche.strategy import (
    BORROWED,
    IDLE,
    SUPPLIED,
    BenqiLendingLifecycleAvalancheStrategy,
)
from strategies.incubating.metamorpho_vault_base.strategy import MetaMorphoVaultBase


def test_metamorpho_accepts_a_funded_configured_amount_below_ten() -> None:
    strategy = object.__new__(MetaMorphoVaultBase)
    strategy.deposit_token = "USDC"
    strategy.deposit_amount = Decimal("3")
    strategy.vault_address = "0xc1256Ae5FF1cf2719D4937adb3bbCCab2E00A2Ca"
    strategy._chain = "base"
    strategy._state = "idle"
    strategy._previous_stable_state = "idle"

    market = MagicMock()
    market.balance.return_value = SimpleNamespace(balance=Decimal("3"), balance_usd=Decimal("3"))

    intent = strategy._do_deposit(market)

    assert intent.intent_type is IntentType.VAULT_DEPOSIT
    assert intent.amount == Decimal("3")


def test_benqi_supplies_before_issuing_a_standalone_borrow() -> None:
    strategy = object.__new__(BenqiLendingLifecycleAvalancheStrategy)
    strategy.collateral_token = "USDC"
    strategy.collateral_amount = Decimal("4")
    strategy.borrow_token = "USDT"
    strategy.borrow_amount_override = Decimal("1.2")
    strategy.ltv_target = Decimal("0.3")
    strategy._chain = "avalanche"
    strategy._loop_state = IDLE
    strategy._previous_stable_state = IDLE
    strategy._supplied_amount = Decimal("0")
    strategy._borrowed_amount = Decimal("0")

    supply = strategy.decide(MagicMock())
    assert supply is not None
    assert supply.intent_type is IntentType.SUPPLY
    assert IntentType.SUPPLY in strategy.STRATEGY_METADATA.intent_types

    strategy.on_intent_executed(supply, True, None)
    assert strategy._loop_state == SUPPLIED

    borrow = strategy.decide(MagicMock())
    assert borrow is not None
    assert borrow.intent_type is IntentType.BORROW
    assert borrow.collateral_amount == Decimal("0")

    strategy.on_intent_executed(borrow, True, None)
    assert strategy._loop_state == BORROWED
    assert strategy._supplied_amount == Decimal("4")
