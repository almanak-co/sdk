"""Immediate liquidation clears debt and retains shared losses without wallet repayments."""

import pytest

from almanak.connectors.morpho_blue.economic_liquidation import LiquidationChange, apply
from almanak.connectors.morpho_blue.economic_positions import account_view
from almanak.connectors.morpho_blue.math import ORACLE_SCALE, WAD
from tests.test_morpho_blue_config import parameters as parameters
from tests.test_morpho_blue_economic_actions import execute
from tests.test_morpho_blue_economic_actions import model as model


def borrowed_model(parameters, model):
    model, _ = execute(parameters, model, parameters.supply("earn", 1000 * 10**6), 1000 * 10**6)
    model, _ = execute(parameters, model, parameters.supply("collateral", 10**18), 10**18)
    model, _ = execute(parameters, model, parameters.borrow("loan", 100 * 10**6), 0)
    market, ledger, positions = model
    fund = parameters.target("loan").fund
    borrower = tuple(p for p in positions if p.target.fund == fund)
    return market, ledger, positions, fund, borrower


@pytest.mark.parametrize("fraction", [99, 10, 0])
def test_liquidation_clears_debt_and_bad_debt_reduces_lender_assets(parameters, model, fraction):
    market, ledger, positions, fund, borrower = borrowed_model(parameters, model)
    account = account_view(borrower, market, ledger, fund)
    boundary = (
        account.borrowed_assets
        * ORACLE_SCALE
        * WAD
        // (account.position.collateral * market.source.lltv)
    )
    price = market.price.model_copy(update={"price": boundary * fraction // 100})
    stressed = market.model_copy(update={"price": price})
    change = apply(borrower, stressed, ledger, fund, time=ledger.effective_at)
    assert change is not None
    assert LiquidationChange.model_validate_json(change.model_dump_json()) == change
    assert not any(p.borrow_shares for p in change.positions)
    assert all(p.repaid == 0 for p in change.positions)
    lender = tuple(p for p in positions if p.target.fund != fund)
    before = account_view(lender, stressed, ledger, lender[0].target.fund)
    after = account_view(lender, stressed, change.market.after, lender[0].target.fund)
    assert after.position.supply_shares == before.position.supply_shares
    assert ledger.accounting.supply_assets - change.market.after.accounting.supply_assets == (
        change.liquidation.bad_debt_assets
    )
    if fraction == 99:
        assert change.liquidation.bad_debt_assets == 0
        assert after.supplied_assets == before.supplied_assets
    else:
        assert change.liquidation.bad_debt_assets > 0
        assert after.supplied_assets < before.supplied_assets
    assert (
        apply(
            change.positions,
            stressed,
            change.market.after,
            fund,
            time=change.market.after.effective_at,
        )
        is None
    )


def test_healthy_fund_does_not_liquidate_and_missing_oracle_is_not_health(parameters, model):
    market, ledger, _, fund, borrower = borrowed_model(parameters, model)
    assert apply(borrower, market, ledger, fund, time=ledger.effective_at) is None
    missing = market.model_copy(update={"price": None, "price_error": "unavailable"})
    with pytest.raises(ValueError, match="configured market oracle"):
        apply(borrower, missing, ledger, fund, time=ledger.effective_at)
