"""Modeled fund balances never read raw totals or duplicate allocation ownership."""

import json

import pytest

from almanak.backtest.types import UnixTime
from almanak.connectors.morpho_blue.domain import Accounting, AccountObservation, Position
from almanak.connectors.morpho_blue.economic_account import AccountView, validate_ownership
from almanak.connectors.morpho_blue.economic_market import MarketLedger, seed
from almanak.connectors.morpho_blue.math import to_assets
from tests.test_morpho_blue_reader import recorded


@pytest.fixture(params=[1, 8453])
def account(request):
    raw = recorded(request.param, True)["accounts"][1]
    observed = AccountObservation.model_validate_json(json.dumps(raw))
    return AccountView(
        market=observed.market,
        ledger=seed(observed.market, UnixTime(observed.market.block.timestamp + 600)),
        fund=observed.fund,
        position=observed.position,
    )


def test_balances_use_hypothetical_totals_and_preserve_raw_evidence(account):
    totals = account.ledger.accounting
    position = Position(
        supply_shares=totals.supply_shares // 10,
        borrow_shares=totals.borrow_shares // 10,
        collateral=account.position.collateral,
    )
    changed = Accounting(**(totals.model_dump() | {"supply_assets": totals.supply_assets * 2}))
    ledger = MarketLedger(**(account.ledger.model_dump() | {"accounting": changed}))
    modeled = AccountView(**(account.model_dump() | {"ledger": ledger, "position": position}))
    assert modeled.market == account.market
    assert modeled.supplied_assets == to_assets(
        position.supply_shares, changed.supply_assets, changed.supply_shares
    )
    assert modeled.borrowed_assets == to_assets(
        position.borrow_shares, changed.borrow_assets, changed.borrow_shares, round_up=True
    )
    assert modeled.health_factor is not None
    assert AccountView.model_validate_json(modeled.model_dump_json()) == modeled


def test_missing_oracle_blocks_health_only_when_debt_exists(account):
    missing = account.market.model_copy(update={"price": None, "price_error": "unavailable"})
    view = AccountView(**(account.model_dump() | {"market": missing}))
    with pytest.raises(ValueError, match="configured market oracle"):
        _ = view.health_factor
    assert view.borrowed_assets > 0
    empty = AccountView(
        **(
            view.model_dump()
            | {"position": Position(supply_shares=0, borrow_shares=0, collateral=0)}
        )
    )
    assert empty.health_factor is None


def test_ownership_counts_funds_once_and_rejects_conflicting_allocations(account):
    validate_ownership(account.ledger, (account, account))
    changed = account.model_copy(
        update={"position": account.position.model_copy(update={"collateral": 0})}
    )
    with pytest.raises(ValueError, match="allocations disagree"):
        validate_ownership(account.ledger, (account, changed))
    position = Position(
        supply_shares=account.ledger.accounting.supply_shares,
        borrow_shares=0,
        collateral=0,
    )
    first = AccountView(**(account.model_dump() | {"position": position}))
    fund = account.fund.model_copy(update={"address": "0x" + "12" * 20})
    second = AccountView(**(first.model_dump() | {"fund": fund}))
    with pytest.raises(ValueError, match="aggregate owned shares"):
        validate_ownership(first.ledger, (first, second))


def test_owned_shares_are_bounded_by_modeled_market(account):
    position = account.position.model_copy(
        update={"borrow_shares": account.ledger.accounting.borrow_shares + 1}
    )
    with pytest.raises(ValueError, match="owned shares exceed modeled"):
        AccountView(**(account.model_dump() | {"position": position}))
