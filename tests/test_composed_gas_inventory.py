"""Shared native cash remains covered after any connector spends fund gas."""

from types import SimpleNamespace

import pytest

from almanak.connectors.composed.inventory import owned_gas_effects
from almanak.connectors.composed.models import wallet_key
from almanak.effects import Balance, apply_assets
from almanak.models import AssetAmount
from tests import test_composed_inventory


@pytest.fixture
def config():
    return test_composed_inventory.config.__wrapped__()


@pytest.mark.parametrize(("owned", "gas", "remaining"), [(40, 30, 40), (90, 30, 70), (100, 30, 70)])
def test_fund_gas_consumes_unallocated_reserve_before_owned_cash(config, owned, gas, remaining):
    target = config.target("loan")
    asset = config.allocation("loan").native_asset
    inventory = (Balance(key=wallet_key(target, asset), amount=owned),)
    outcome = SimpleNamespace(
        gas_paid=AssetAmount(asset=asset, amount=gas),
        before=SimpleNamespace(
            payer=target.fund, native_balance=AssetAmount(asset=asset, amount=100)
        ),
    )
    changed = apply_assets(inventory, owned_gas_effects(inventory, target, outcome))
    assert changed[0].amount == remaining


def test_keeper_and_separate_operator_do_not_debit_owned_native_cash(config):
    target = config.target("loan")
    assert owned_gas_effects((), target, SimpleNamespace()) == ()
    outcome = SimpleNamespace(
        gas_paid=AssetAmount(asset=config.allocation("loan").native_asset, amount=30),
        before=SimpleNamespace(payer=object()),
    )
    assert owned_gas_effects((), target, outcome) == ()


def test_invalid_fund_gas_evidence_refuses_ownership_update(config):
    target = config.target("loan")
    asset = config.allocation("loan").native_asset
    inventory = (Balance(key=wallet_key(target, asset), amount=101),)
    outcome = SimpleNamespace(
        gas_paid=AssetAmount(asset=asset, amount=1),
        before=SimpleNamespace(
            payer=target.fund, native_balance=AssetAmount(asset=asset, amount=100)
        ),
    )
    with pytest.raises(ValueError, match="sufficient measured native cash"):
        owned_gas_effects(inventory, target, outcome)
