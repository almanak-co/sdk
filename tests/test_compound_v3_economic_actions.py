"""Comet simulation transitions retain native math, whole-fund risk and source evidence."""

from datetime import UTC, datetime

import pytest

from almanak.connectors.compound_v3.domain import Reserve
from almanak.connectors.compound_v3.economic_actions import apply
from almanak.connectors.compound_v3.economic_data import EconomicMarket, RiskSample
from almanak.connectors.compound_v3.economic_liquidation import liquidate
from almanak.connectors.compound_v3.economic_state import ModeledIndices, Position, Snapshot
from almanak.connectors.compound_v3.economic_views import account_view, risk_account
from almanak.connectors.compound_v3.models import Allocation, Parameters
from almanak.connectors.compound_v3.risk import active
from almanak.gateway.evm import native_asset
from almanak.models import AccountBinding, AssetAmount, Eoa, StrategyAccounts
from almanak.strategy_api import SimulationEvidence
from almanak.triggers import IntervalTrigger
from tests.test_compound_v3_liquidation import calibration
from tests.test_compound_v3_risk import account, collateral_change


def setup(observed):
    native = native_asset(observed.fund.chain.chain_id)
    reserves = [
        (
            "loan",
            Reserve(
                market=observed.market.source,
                asset=observed.market.source.base_asset,
                price_feed=observed.market.source.base_price_feed,
            ),
        )
    ]
    reserves += [
        (
            "collateral",
            Reserve(market=observed.market.source, asset=c.asset_type, price_feed=c.price_feed),
        )
        for c in observed.market.collaterals
        if observed.collaterals[c.offset].balance
    ]
    config = Parameters(
        decision_trigger=IntervalTrigger(kind="interval", seconds=60),
        accounts=StrategyAccounts(
            bindings=(AccountBinding(name="borrower", profile=Eoa(kind="eoa", fund=observed.fund)),)
        ),
        allocations=tuple(
            Allocation(
                name=name,
                account="borrower",
                reserve=reserve,
                capital=2 * 10**18 if name == "collateral" else 0,
                borrow=name == "loan",
                minimum_health_factor=101 * 10**16,
                native_asset=native,
                minimum_native_balance=0,
            )
            for name, reserve in reserves
        ),
    )
    positions = tuple(
        Position(
            target=config.target(name),
            principal=observed.principal if name == "loan" else 0,
            collateral=0
            if name == "loan"
            else next(c.balance for c in observed.collaterals if c.asset == reserve.asset),
            collateral_enabled=name != "loan"
            and active(
                observed,
                next(
                    c.offset for c in observed.market.collaterals if c.asset_type == reserve.asset
                ),
            ),
        )
        for name, reserve in reserves
    )
    block = observed.market.block
    sample = RiskSample(
        market=observed.market,
        block_group=(block,),
        following_block=block.model_copy(
            update={"number": block.number + 1, "timestamp": block.timestamp + 1}
        ),
    )
    market = EconomicMarket(
        sample,
        observed.market.accounting.supply_index,
        observed.market.accounting.borrow_index,
        block.timestamp,
    )
    return config, positions, market


@pytest.mark.parametrize("network", ["mainnet", "base"])
@pytest.mark.parametrize("scenario", ["credit", "bad_debt"])
def test_economic_absorb_matches_native_calibration_and_preserves_recoverable_credit(
    network, scenario
):
    raw, step, observed = calibration(network, scenario)
    config, positions, market = setup(observed)
    result = liquidate(config, positions, market)
    loan, collateral = result.positions
    assert loan.principal == step["after"]["accounts"]["borrower"]["user"][0]
    assert loan.absorption_credit == raw["comparison"]["collateral_credit"]
    assert loan.absorbed_deficit == raw["comparison"]["absorbed_deficit"]
    assert collateral.collateral == 0 and not collateral.collateral_enabled
    assert collateral.seized_collateral == 10**18
    assert len(result.absorptions) == 1
    if scenario == "credit":
        assert loan.principal > 0
        exit = apply(config, config.withdraw("loan"), result.positions, market, 0, result.positions)
        assert exit.positions[0].empty and exit.inventory_delta == market.balance(loan.principal)
    else:
        assert loan.principal == 0 and loan.absorbed_deficit > 0


def test_excluded_collateral_remains_owned_but_loses_membership_after_absorption():
    _, _, observed = calibration("base", "bad_debt")
    index = next(
        c.offset for c in observed.market.collaterals if observed.collaterals[c.offset].balance
    )
    observed = collateral_change(observed, index, liquidation_factor=0)
    config, positions, market = setup(observed)
    result = liquidate(config, positions, market)
    assert result.positions[1].collateral == 10**18
    assert not result.positions[1].collateral_enabled
    extra = apply(
        config, config.supply("collateral", 1), result.positions, market, 1, result.positions
    )
    assert extra.positions[1].collateral == 10**18 + 1
    assert not extra.positions[1].collateral_enabled
    view = account_view(extra.positions, market, observed.fund)
    assert view.assets_in == view.extended_assets_in == 0


def test_paused_absorption_cannot_fabricate_completed_liquidation():
    _, _, observed = calibration("mainnet", "bad_debt")
    observed = observed.model_copy(
        update={
            "market": observed.market.model_copy(
                update={
                    "accounting": observed.market.accounting.model_copy(update={"pause_flags": 8})
                }
            )
        }
    )
    config, positions, market = setup(observed)
    with pytest.raises(ValueError, match="absorb is paused"):
        liquidate(config, positions, market)


@pytest.mark.parametrize("network", ["mainnet", "base"])
def test_partial_and_full_repayment_keep_inventory_and_principal_separate(network):
    config, positions, market = setup(account(network))
    paid = apply(
        config, config.repay_amount("loan", 100 * 10**6), positions, market, 100 * 10**6, positions
    )
    assert paid.inventory_delta == -100 * 10**6 and paid.positions[0].principal < 0
    debt = -market.balance(paid.positions[0].principal)
    full = apply(config, config.repay("loan"), paid.positions, market, debt, paid.positions)
    assert full.positions[0].principal == 0 and full.inventory_delta == -debt
    with pytest.raises(ValueError, match="missing fund inventory"):
        apply(config, config.repay("loan"), paid.positions, market, debt - 1, paid.positions)
    with pytest.raises(ValueError, match="exceeds the debt"):
        apply(
            config,
            config.repay_amount("loan", debt + 1),
            paid.positions,
            market,
            debt + 1,
            paid.positions,
        )


def test_modeled_snapshot_keeps_observed_indices_and_requires_modeled_balance_evidence():
    observed = account("mainnet")
    config, positions, initial = setup(observed)
    market = EconomicMarket(
        initial.sample,
        initial.supply_index + 10**12,
        initial.borrow_index + 10**12,
        initial.time + 20,
    )
    view = account_view(positions, market, observed.fund)
    assert view.market == observed.market
    assert risk_account(view, market).market.accounting.borrow_index == market.borrow_index
    snapshot = Snapshot(
        target=config.target("loan"),
        account=view,
        indices=ModeledIndices(supply=market.supply_index, borrow=market.borrow_index),
        wallet_balance=0,
        native_balance=AssetAmount(asset=native_asset(observed.fund.chain.chain_id), amount=0),
        payer=observed.fund,
        effective_at=datetime.fromtimestamp(market.time, UTC),
        simulation=SimulationEvidence(
            execution_id="synthetic", model="synthetic", inputs=("synthetic-rate",)
        ),
    )
    assert Snapshot.model_validate_json(snapshot.model_dump_json()) == snapshot
    changed = snapshot.model_copy(
        update={"account": view.model_copy(update={"borrowed_base": view.borrowed_base + 1})}
    )
    with pytest.raises(ValueError, match="modeled indices"):
        Snapshot.model_validate_json(changed.model_dump_json())
