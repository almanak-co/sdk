"""Retained position plans need only owned inventory and the retained position."""

import pytest

from almanak.connectors.gmx_v2 import authority, execution, planning
from almanak.connectors.gmx_v2.intents import ClosePosition, DecreasePosition, RevokePosition
from almanak.connectors.gmx_v2.operations import Approval, CancelOrder
from almanak.gateway.ethereum import DataError
from almanak.models import AssetAmount
from tests.gmx_evidence import accepted, checkpoint, holding


def test_close_prepares_finite_fee_allowance_then_exact_position_request():
    state = holding()
    observed = state.outcomes[-1].after
    target = observed.target
    intent = ClosePosition(target=target)
    approve = planning.operation(state.config, state, intent, observed)
    assert isinstance(approve, Approval) and approve.amount.amount == 2 * 10**16
    current = observed.model_copy(update={"allowance": approve.amount.amount})
    operation = planning.operation(state.config, state, intent, current)
    assert operation.order_type == 4 and operation.size_delta_usd_e30 == 1000 * 10**30
    assert operation.collateral_delta.amount == 0
    excess = observed.model_copy(update={"allowance": 1})
    assert planning.operation(state.config, state, intent, excess).amount.amount == 0
    revoke = RevokePosition(target=target)
    assert planning.operation(state.config, state, revoke, excess).amount.amount == 0
    with pytest.raises(ValueError, match="already closed"):
        planning.operation(state.config, state, revoke, observed)


def test_decrease_requires_retained_position_and_cannot_hide_collateral_withdrawal():
    state = holding()
    observed = state.outcomes[-1].after
    target = observed.target
    zero = AssetAmount(asset=target.collateral_asset, amount=0)
    intent = DecreasePosition(
        target=target, collateral_withdrawal=zero, size_delta_usd_e30=500 * 10**30
    )
    assert planning.decrease_amounts(intent, observed) == (500 * 10**30, zero)
    with pytest.raises(ValueError, match="retained aggregate"):
        planning.decrease_amounts(intent, accepted().outcomes[-1].after)
    with pytest.raises(ValueError, match="exceeds retained"):
        planning.decrease_amounts(
            intent.model_copy(update={"size_delta_usd_e30": 2000 * 10**30}), observed
        )
    withdrawal = intent.model_copy(
        update={"collateral_withdrawal": zero.model_copy(update={"amount": 1})}
    )
    # Scope-only authority: a collateral withdrawal is bounded by the retained position only.
    authority.admit(state.capabilities, withdrawal)
    assert planning.decrease_amounts(withdrawal, observed)[1].amount == 1


def test_reentry_after_keeper_delivery_needs_only_owned_inventory():
    state = holding()
    observed = state.outcomes[-1].after
    request = state.outcomes[-1].operation
    planning.validate_inventory(request, observed)
    poor = observed.owned.model_copy(
        update={
            "inventory": (
                observed.owned.inventory[0].model_copy(update={"amount": 0}),
                *observed.owned.inventory[1:],
            )
        }
    )
    with pytest.raises(DataError, match="owned wrapped-native inventory"):
        planning.validate_inventory(request, observed.model_copy(update={"owned": poor}))


def test_economic_authority_refuses_foreign_targets_and_admits_any_size():
    state = holding()
    intent = state.outcomes[-1].decision.intent
    authority.validate(state.config, state.capabilities)
    authority.admit(state.capabilities, intent)
    authority.admit(state.capabilities, ClosePosition(target=intent.target))
    with pytest.raises(ValueError, match="authoritative configured"):
        authority.validate(state.config, state.capabilities.model_copy(update={"requirements": ()}))
    with pytest.raises(ValueError, match="retained target"):
        authority.admit(
            state.capabilities,
            intent.model_copy(
                update={"target": intent.target.model_copy(update={"allocation": "foreign"})}
            ),
        )
    # Size and collateral have no author bound; inventory and GMX's own limits apply later.
    authority.admit(
        state.capabilities, intent.model_copy(update={"size_delta_usd_e30": 1001 * 10**30})
    )
    authority.admit(
        state.capabilities,
        intent.model_copy(
            update={"collateral": intent.collateral.model_copy(update={"amount": 10**18})}
        ),
    )


def test_changed_allowance_position_and_accounting_refuse_new_exposure():
    state = accepted()
    recorded = checkpoint().outcomes[1]
    before = recorded.before
    state = state.model_copy(
        update={"requests": (), "outcomes": state.outcomes[:1], "owned": (before.owned,)}
    )
    execution.validate_operation(None, state, recorded.operation, before)
    operation = recorded.operation.model_copy(update={"size_delta_usd_e30": 1})
    with pytest.raises(ValueError, match="independently planned"):
        execution.validate_operation(None, state, operation, before)
    with pytest.raises(DataError, match="running reconciled"):
        execution.validate_entry(
            state.model_copy(update={"mode": "teardown"}), before, recorded.operation
        )
    with pytest.raises(DataError, match="allowance changed"):
        execution.validate_current(None, recorded, before.model_copy(update={"allowance": 0}))
    with pytest.raises(DataError, match="position changed"):
        execution.validate_current(None, recorded, checkpoint().outcomes[2].after)
    cancel = CancelOrder(target=before.target, request_key=accepted().requests[0].key)
    with pytest.raises(DataError, match="settled or became ineligible"):
        execution.validate_current(None, recorded.model_copy(update={"operation": cancel}), before)
