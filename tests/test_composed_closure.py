"""Protocol teardown cannot claim exclusive ownership of shared fund cash."""

from types import SimpleNamespace

import pytest

from almanak.connectors.composed.lifecycle import validate_component
from almanak.connectors.composition import connector_for
from almanak.connectors.identifiers import ConnectorId
from almanak.connectors.uniswap_v3.lifecycle import validate_closure
from tests.test_uniswap_v3_validation import state_for


def retained_cash():
    state, before, _ = state_for()
    owned = before.owned.model_copy(
        update={
            "inventory0": before.owned.inventory0.model_copy(update={"amount": 1}),
            "inventory1": before.owned.inventory1.model_copy(update={"amount": 1}),
        }
    )
    observed = before.model_copy(
        update={
            "owned": owned,
            "wallet0": owned.inventory0,
            "wallet1": owned.inventory1,
            "allowances": tuple(a.model_copy(update={"amount": 0}) for a in before.allowances),
        }
    )
    return state.config, SimpleNamespace(owned=(owned,)), observed


def test_shared_cash_can_remain_after_protocol_obligations_close():
    config, state, observed = retained_cash()
    validate_component(
        connector_for(config), state, (observed,), "validate_closure", ConnectorId("uniswap_v3")
    )
    with pytest.raises(ValueError, match="unmeasured non-funding inventory"):
        validate_closure(state, (observed,))


def test_shared_cash_does_not_excuse_remaining_spending_authority():
    config, state, observed = retained_cash()
    first, *rest = observed.allowances
    observed = observed.model_copy(
        update={"allowances": (first.model_copy(update={"amount": 1}), *rest)}
    )
    with pytest.raises(ValueError, match="cleanup"):
        validate_component(
            connector_for(config), state, (observed,), "validate_closure", ConnectorId("uniswap_v3")
        )
