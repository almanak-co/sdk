"""Risk boundaries preserve deployed Comet ordering and missing-price behavior."""

import json

import pytest

from almanak.connectors.compound_v3.domain import AccountObservation
from almanak.connectors.compound_v3.risk import (
    active,
    base_liquidity,
    is_borrow_collateralized,
    is_liquidatable,
)
from tests.test_compound_v3_reader import recorded


def account(network="base"):
    _, data = recorded(network, True)
    return AccountObservation.model_validate_json(json.dumps(data["accounts"][1]))


def collateral_change(observation, index, **changes):
    collaterals = list(observation.market.collaterals)
    collaterals[index] = collaterals[index].model_copy(update=changes)
    return observation.model_copy(
        update={"market": observation.market.model_copy(update={"collaterals": tuple(collaterals)})}
    )


@pytest.mark.parametrize("network", ["mainnet", "base"])
def test_funded_risk_and_empty_principal_with_unavailable_prices(network):
    observed = account(network)
    assert is_borrow_collateralized(observed)
    assert not is_liquidatable(observed)
    missing = observed.model_copy(
        update={
            "market": observed.market.model_copy(
                update={"base_price": None, "base_price_error": "missing"}
            )
        }
    )
    with pytest.raises(ValueError, match="oracle"):
        is_liquidatable(missing)
    cleared = missing.model_copy(update={"principal": 0})
    assert is_borrow_collateralized(cleared) and not is_liquidatable(cleared)


@pytest.mark.parametrize("network", ["mainnet", "base"])
def test_risk_uses_stored_indices_and_does_not_treat_inactive_balances_as_collateral(network):
    observed = account(network)
    debt = -observed.principal * observed.market.accounting.borrow_index // 10**15
    expected = -(debt * observed.market.base_price.price // 10**observed.market.base_asset.decimals)
    assert base_liquidity(observed) == expected
    inactive = observed.model_copy(update={"assets_in": 0, "extended_assets_in": 0})
    assert not is_borrow_collateralized(inactive) and is_liquidatable(inactive)
    assert active(inactive.model_copy(update={"extended_assets_in": 1}), 16)


@pytest.mark.parametrize("network", ["mainnet", "base"])
def test_low_collateral_prices_cross_borrow_and_liquidation_boundaries(network):
    observed = account(network)
    index = next(c.offset for c in observed.market.collaterals if active(observed, c.offset))
    collateral = observed.market.collaterals[index]
    stressed = collateral_change(
        observed, index, price=collateral.price.model_copy(update={"price": 100 * 10**8})
    )
    assert not is_borrow_collateralized(stressed) and is_liquidatable(stressed)


@pytest.mark.parametrize("network", ["mainnet", "base"])
def test_zero_factors_skip_unavailable_prices_only_in_extended_controls_revision(network):
    observed = account(network)
    index = next(c.offset for c in observed.market.collaterals if active(observed, c.offset))
    missing = collateral_change(
        observed, index, borrow_factor=0, liquidate_factor=0, price=None, price_error="unavailable"
    )
    if network == "base":
        assert not is_borrow_collateralized(missing) and is_liquidatable(missing)
    else:
        with pytest.raises(ValueError, match="oracle"):
            is_borrow_collateralized(missing)
        with pytest.raises(ValueError, match="oracle"):
            is_liquidatable(missing)


def test_deactivated_collateral_refuses_only_when_reached_before_early_return():
    observed = account()
    index = next(c.offset for c in observed.market.collaterals if active(observed, c.offset))
    controls = observed.market.controls.model_copy(update={"deactivated_collaterals": 1 << index})
    blocked = observed.model_copy(
        update={"market": observed.market.model_copy(update={"controls": controls})}
    )
    with pytest.raises(ValueError, match="deactivated"):
        is_borrow_collateralized(blocked)
    assert not is_liquidatable(blocked)
    later = index + 1
    assert later < len(observed.market.collaterals)
    controls = controls.model_copy(update={"deactivated_collaterals": 1 << later})
    extra = observed.model_copy(
        update={
            "assets_in": observed.assets_in | (1 << later),
            "market": observed.market.model_copy(update={"controls": controls}),
        }
    )
    extra = collateral_change(extra, later, price=None, price_error="unavailable")
    assert is_borrow_collateralized(extra) and not is_liquidatable(extra)
