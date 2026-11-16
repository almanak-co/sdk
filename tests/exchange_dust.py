"""Economic exchange state that holds only counter dust, for the shared exit driver.

The component model holds exactly the requested inventory with matching fund
wallets. The dust amount is found from the model's own prepared quote, so each
connector case uses an amount its captured pool truly cannot swap.
"""

from types import SimpleNamespace

import pytest

from almanak.dex_intents import ExitExchange
from almanak.models import AssetAmount


def owned_values(owned):
    if hasattr(owned, "inventory"):
        return owned.inventory.root
    return owned.inventory0, owned.inventory1


def with_values(owned, amounts):
    if hasattr(owned, "inventory"):
        values = tuple(
            v.model_copy(update={"amount": amounts.get(v.asset, v.amount)})
            for v in owned.inventory.root
        )
        return owned.model_copy(update={"inventory": type(owned.inventory)(values)})
    return owned.model_copy(
        update={
            f"inventory{i}": v.model_copy(update={"amount": amounts.get(v.asset, v.amount)})
            for i, v in enumerate((owned.inventory0, owned.inventory1))
        }
    )


def holding(model, state, amounts):
    """`state` holding exactly `amounts` of the allocation's inventory, with its wallets."""
    held = state.portfolio.positions[0]
    owned = held.payload.owned
    deltas = tuple(amounts.get(v.asset, v.amount) - v.amount for v in owned_values(owned))
    payload = held.payload.model_copy(update={"owned": with_values(owned, amounts)})
    native = model.config.allocations[0].native_asset
    effects = model.parameters.transition_effects(model.settings, held, payload, deltas, native, 0)
    return state.model_copy(update={"portfolio": state.portfolio.apply(effects)})


def dust_amount(state, quote, asset):
    """The largest tried counter amount whose modeled exit returns nothing."""
    for amount in (10**6, 10**3, 10, 1):
        if not quote(state, AssetAmount(asset=asset, amount=amount)).amount_out.amount:
            return amount
    pytest.fail("no tried counter amount has a zero-output exit quote")


def settle_dust(model, state, target, counter, quote):
    """Only measured dust remains: observation, closure and a zero-effect exit agree."""
    amount = dust_amount(state, quote, counter)
    dusty = holding(model, state, {counter: amount})
    observed = model.observe(dusty)[0]
    assert [r.amount_in for r in observed.residuals] == [AssetAmount(asset=counter, amount=amount)]
    assert all(not r.amount_out.amount for r in observed.residuals)
    assert model.closed(dusty, (observed,))
    pending = SimpleNamespace(
        intent=ExitExchange[type(target)](target=target),
        identity="dust-exit",
        decision_time=dusty.time,
        eligible_at=dusty.time + 1,
    )
    transition = model.execute(dusty, pending)
    assert transition.complete and transition.result.steps == ()
    assert transition.result.gas_paid.amount == 0
    assert transition.result.before.residuals == observed.residuals
    assert dusty.portfolio.apply(transition.effects) == dusty.portfolio
    return dusty
