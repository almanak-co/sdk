"""Captured GM inputs keep their floors at admission and owned inventory before submission."""

from types import SimpleNamespace

import pytest

from almanak.connectors.gmx_v2 import liquidity_planning
from almanak.connectors.gmx_v2.exchange_validation import validate_current
from almanak.gateway.ethereum import DataError
from tests.gmx_native_runtime import gm_completed as gm_completed


@pytest.fixture(params=["gmx_create_deposit", "gmx_create_withdrawal"])
def creation(completed, request):
    return next(o for o in completed.outcomes if o.operation.kind == request.param)


def test_gm_current_state_rejects_expiry_and_changed_owned_shares(creation):
    plan = SimpleNamespace(
        operation=creation.operation, decision=creation.decision, before=creation.before
    )
    validate_current(plan, creation.before)
    late = creation.before.model_copy(
        update={
            "block": creation.before.block.model_copy(
                update={"timestamp": creation.decision.intent.deadline}
            )
        }
    )
    with pytest.raises(DataError, match="deadline expired"):
        validate_current(plan, late)
    shares = creation.before.market_tokens.model_copy(
        update={"amount": creation.before.market_tokens.amount + 1}
    )
    with pytest.raises(DataError, match="share balance changed"):
        validate_current(plan, creation.before.model_copy(update={"market_tokens": shares}))


def owned(before, short):
    """The captured ownership with one asset's balance one unit below the request input."""

    def reduced(value):
        if value.asset != short.asset:
            return value
        return value.model_copy(update={"amount": short.amount - 1})

    return SimpleNamespace(
        owned=SimpleNamespace(
            inventory=tuple(reduced(value) for value in before.owned.inventory),
            market_tokens=reduced(before.owned.market_tokens),
        )
    )


def test_gm_request_rejects_only_exhausted_owned_inventory(creation):
    liquidity_planning.validate_inventory(creation.operation, creation.before)
    for value in creation.operation.inputs:
        if not value.amount:
            continue
        with pytest.raises(DataError, match="owned currency or share inventory"):
            liquidity_planning.validate_inventory(creation.operation, owned(creation.before, value))


def test_gm_admission_takes_any_deposit_amount_and_refuses_outputs_below_floors(
    completed, creation
):
    capability = completed.capabilities.requirements[0]
    intent = creation.decision.intent
    liquidity_planning.admit(capability, intent)
    if creation.operation.kind == "gmx_create_deposit":
        # Scope-only authority: a deposit of any size is admitted; inventory bounds it later.
        larger = intent.maximum0.model_copy(update={"amount": intent.maximum0.amount * 1000})
        liquidity_planning.admit(capability, intent.model_copy(update={"maximum0": larger}))
        return
    below = intent.minimum0.model_copy(
        update={"amount": capability.minimum_outputs.root[0].amount - 1}
    )
    with pytest.raises(ValueError, match="configured asset floor"):
        liquidity_planning.admit(capability, intent.model_copy(update={"minimum0": below}))
