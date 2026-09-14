"""Adversarial state and observation boundaries for the bStocks LP example."""

from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from types import SimpleNamespace

import pytest

from almanak.framework.execution.extracted_data import SwapAmounts
from almanak.framework.execution.orchestrator import ExecutionPhase, ExecutionResult
from almanak.framework.intents import Intent
from almanak.framework.market.models import PriceData, TokenBalance
from strategies.bstocks_lp.strategy import BStocksLPConfig, BStocksLPStrategy


def _strategy():
    return BStocksLPStrategy(
        config=BStocksLPConfig(quote_allocation=Decimal("6")),
        chain="bsc",
        wallet_address="0x0000000000000000000000000000000000000001",
    )


def _market(*, age=0, stale=False, missing=False):
    now = datetime.now(UTC)
    return SimpleNamespace(
        timestamp=now,
        balance=lambda token: TokenBalance(
            symbol=token, balance=Decimal("100"), balance_usd=Decimal("100"), address=token
        ),
        price_data=lambda _: PriceData(
            price=Decimal("1"), timestamp=None if missing else now - timedelta(seconds=age), stale=stale
        ),
    )


@pytest.mark.parametrize("phase", ["ready", "funded"])
@pytest.mark.parametrize("observation", [{"age": 121}, {"age": -1}, {"stale": True}, {"missing": True}])
def test_unusable_observation_cannot_create_entry_or_mint(phase, observation):
    strategy = _strategy()
    strategy.phase = phase
    strategy.owned = {"GOOGLB": Decimal("0.01"), "USDT": Decimal("3")}
    before = dict(strategy.owned)
    result = strategy.decide(_market(**observation))
    assert result.intent_type.value == "HOLD"
    assert strategy.phase == phase
    assert strategy.pending_intent_id is None
    assert strategy.owned == before


@pytest.mark.parametrize("phase", ["entry_pending", "mint_pending"])
def test_unrelated_close_callback_preserves_pending_reconciliation(phase):
    strategy = _strategy()
    strategy.phase = phase
    strategy.pending_intent_id = "pending-owned-intent"
    unrelated = Intent.lp_close(position_id="99", protocol="pancakeswap_v3")
    strategy.on_intent_executed(unrelated, True, SimpleNamespace())
    assert strategy.pending_intent_id == "pending-owned-intent"
    assert strategy.phase == phase
    assert strategy.decide(_market()).intent_type.value == "HOLD"


@pytest.mark.parametrize("success", [True, False])
def test_repeated_entry_callback_cannot_rewrite_acquired_inventory(success):
    strategy = _strategy()
    entry = strategy.decide(_market())
    receipt = ExecutionResult(
        success=True,
        phase=ExecutionPhase.COMPLETE,
        swap_amounts=SwapAmounts(
            amount_in=3 * 10**18,
            amount_out=10**16,
            amount_in_decimal_resolved=True,
            amount_out_decimal_resolved=True,
            amount_in_decimal=Decimal("3"),
            amount_out_decimal=Decimal("0.01"),
        ),
    )
    strategy.on_intent_executed(entry, True, receipt)
    before = strategy.get_persistent_state()
    receipt.swap_amounts = replace(receipt.swap_amounts, amount_out_decimal=Decimal("100"))
    strategy.on_intent_executed(entry, success, receipt)
    assert strategy.get_persistent_state() == before
