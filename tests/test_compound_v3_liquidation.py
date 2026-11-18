"""Absorption math matches retained real Comet calls with explicit fork oracle stress."""

import gzip
import json
from pathlib import Path

import pytest

from almanak.connectors.compound_v3.liquidation import absorb
from tests.test_compound_v3_risk import account, collateral_change

EVIDENCE = (
    Path(__file__).parents[1]
    / "docs/internal/reports/evidence/2026-09-07-rollout/compound-v3-liquidation"
)


def calibration(network, scenario):
    raw = json.loads(
        gzip.decompress((EVIDENCE / f"{network}-liquidation-{scenario}.json.gz").read_bytes())
    )
    step = next(s for s in raw["steps"] if s["kind"] == "absorb")
    observed = account(network)
    market = observed.market
    # Reuse verified market terms; replace the principal and accrued indices with this
    # independently captured absorption transaction's values, plus its explicit fixture price.
    accounting = market.accounting.model_copy(
        update={
            "supply_index": step["after"]["totals"][0],
            "borrow_index": step["after"]["totals"][1],
        }
    )
    observed = observed.model_copy(
        update={
            "principal": step["before"]["accounts"]["borrower"]["user"][0],
            "market": market.model_copy(update={"accounting": accounting}),
        }
    )
    collateral = next(c for c in market.collaterals if c.asset_type.reference == raw["collateral"])
    observed = collateral_change(
        observed,
        collateral.offset,
        price=collateral.price.model_copy(update={"price": raw["comparison"]["fixture_price"]}),
    )
    return raw, step, observed


@pytest.mark.parametrize("network", ["mainnet", "base"])
@pytest.mark.parametrize("scenario", ["credit", "bad_debt"])
def test_absorption_matches_actual_principal_credit_deficit_and_collateral(network, scenario):
    raw, step, observed = calibration(network, scenario)
    result = absorb(observed)
    assert result.principal == step["after"]["accounts"]["borrower"]["user"][0]
    assert result.collateral_credit.amount == raw["comparison"]["collateral_credit"]
    assert result.absorbed_deficit.amount == raw["comparison"]["absorbed_deficit"]
    assert all(c.balance == 0 for c in result.remaining)
    assert sum(c.balance for c in result.seized) == 10**18
    assert result.collateral_credit.asset == observed.market.source.base_asset
    assert result.fund == observed.fund and result.market == observed.market.source


def test_absorption_refuses_healthy_fund():
    with pytest.raises(ValueError, match="liquidatable"):
        absorb(account())


def test_controlled_absorption_keeps_zero_liquidation_factor_collateral():
    _, _, observed = calibration("base", "bad_debt")
    index = next(
        c.offset for c in observed.market.collaterals if observed.collaterals[c.offset].balance
    )
    excluded = collateral_change(observed, index, liquidation_factor=0)
    result = absorb(excluded)
    assert result.remaining[index].balance == 10**18
    assert result.seized[index].balance == 0
    assert result.principal == 0 and result.collateral_credit.amount == 0
    assert result.absorbed_deficit.amount > 0


def test_controlled_zero_liquidate_factor_uses_unpopulated_price_array_entry():
    _, _, observed = calibration("base", "bad_debt")
    index = next(
        c.offset for c in observed.market.collaterals if observed.collaterals[c.offset].balance
    )
    excluded = collateral_change(
        observed, index, liquidate_factor=0, price=None, price_error="unavailable"
    )
    result = absorb(excluded)
    assert result.seized[index].balance == 10**18
    assert result.remaining[index].balance == 0
    assert result.collateral_credit.amount == 0
