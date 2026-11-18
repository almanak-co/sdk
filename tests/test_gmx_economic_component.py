"""Composition may retain cash after GMX protocol escrow, shares and allowances close."""

import pytest

from almanak.connectors.gmx_v2.economic_accounting import (
    allowances_after_acceptance,
    cumulative_spending,
)
from almanak.connectors.gmx_v2.economic_component import initial_position, protocol_closed
from almanak.connectors.gmx_v2.economic_spending import spending_assets
from almanak.connectors.gmx_v2.exchange_observations import initial_owned
from tests.test_gmx_economic_book import prepared as prepared


def test_initial_component_retains_configured_input_assets_and_shared_cash(prepared):
    _, config, _, _ = prepared
    allocation = config.allocations[0]
    standalone = initial_position(config, allocation)
    cash = standalone.owned.inventory[0].model_copy(update={"amount": 1})
    shared = standalone.owned.model_copy(
        update={"inventory": (cash, *standalone.owned.inventory[1:])}
    )
    composed = initial_position(config, allocation, shared)
    assert composed.owned == shared
    assert composed.target == standalone.target
    assert composed.spent == standalone.spent
    assert composed.allowances == standalone.allowances
    assert protocol_closed(composed)
    assert any(value.amount for value in composed.owned.inventory)


def test_component_protocol_closure_preserves_shares_and_allowances_as_obligations(prepared):
    _, config, _, _ = prepared
    payload = initial_position(config, config.allocations[0])
    with_shares = payload.model_copy(
        update={
            "owned": payload.owned.model_copy(
                update={
                    "market_tokens": payload.owned.market_tokens.model_copy(update={"amount": 1})
                }
            )
        }
    )
    assert not protocol_closed(with_shares)
    with_allowance = payload.model_copy(
        update={
            "allowances": (
                payload.allowances[0].model_copy(update={"amount": 1}),
                *payload.allowances[1:],
            )
        }
    )
    assert not protocol_closed(with_allowance)


def test_component_initialization_refuses_another_target(prepared):
    _, config, _, _ = prepared
    allocation = config.allocations[0]
    owned = initial_owned(config, allocation.name)
    wrong = owned.model_copy(
        update={"target": owned.target.model_copy(update={"allocation": "Other"})}
    )
    with pytest.raises(ValueError, match="configured target"):
        initial_position(config, allocation, wrong)


def test_acceptance_reports_cumulative_inputs_and_leaves_request_sized_allowances_spent(prepared):
    """Scope-only authority: inputs accumulate without a limit, and the request-sized
    approval each request needs is consumed by its own escrow transfer."""
    _, config, _, _ = prepared
    allocation = config.allocations[0]
    payload = initial_position(config, allocation)
    assert tuple(value.asset for value in payload.spent) == spending_assets(allocation)
    huge = tuple(value.model_copy(update={"amount": 10**40}) for value in payload.spent)
    spent = cumulative_spending(payload, huge)
    assert cumulative_spending(payload.model_copy(update={"spent": spent}), huge) == tuple(
        value.model_copy(update={"amount": 2 * 10**40}) for value in payload.spent
    )
    stale = tuple(value.model_copy(update={"amount": 3}) for value in payload.allowances)
    used = payload.model_copy(update={"allowances": stale})
    after = allowances_after_acceptance(used, huge[:1])
    assert after[0].amount == 0 and all(value.amount == 3 for value in after[1:])
    foreign = huge[0].model_copy(update={"asset": allocation.market.share_asset})
    if foreign.asset not in spending_assets(allocation):
        with pytest.raises(ValueError, match="configured input assets"):
            cumulative_spending(payload, (foreign,))
