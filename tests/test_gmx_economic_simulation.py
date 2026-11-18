"""Isolated request timing exercises real captured pricing and synthetic admission clocks."""

import pytest

from almanak.connectors.gmx_v2.economic_requests import admit
from almanak.connectors.gmx_v2.economic_simulation import advance
from almanak.connectors.gmx_v2.exchange_domain import Revoke
from tests.gmx_retired import retired_closure as retired_closure
from tests.test_gmx_economic_book import prepared as prepared
from tests.test_gmx_economic_requests import request_case as request_case

# The retained prepared datasets predate GMX's 1 October rotation; these accounting checks
# re-decode them against the retired closure they were collected from (tests.gmx_retired).
pytestmark = pytest.mark.usefixtures("retired_closure")


def accepted_case(case):
    config, settings, _, held, pending, risk, opportunity = case
    after, _ = admit(config, held.payload, pending, risk)
    # The short captured range cannot itself prove an earlier admission.
    escrow = after.escrow.model_copy(
        update={"accepted_at": opportunity.publication.minimum_timestamp - 1}
    )
    after = after.model_copy(update={"escrow": escrow})
    return config, settings, after, pending, opportunity


def frame(opportunity):
    from types import SimpleNamespace

    return SimpleNamespace(
        payload=SimpleNamespace(risk=(opportunity.risk,), opportunities=(opportunity,))
    )


def test_successful_keeper_settlement_retains_original_escrow_identity(request_case):
    config, settings, payload, pending, opportunity = accepted_case(request_case)
    pending = pending.model_copy(
        update={"identity": "continuation", "eligible_at": opportunity.publication.block.timestamp}
    )
    result = advance(config, settings, payload, pending, frame(opportunity))
    assert result.phase == "settled"
    assert result.after.escrow is None
    assert result.keeper.amount + result.refund.amount == payload.escrow.execution_fee.amount
    assert result.gas.amount == 0
    assert result.after.spent == payload.spent


def test_failed_minimum_output_cancels_and_returns_fund_inputs(request_case):
    config, settings, payload, pending, opportunity = accepted_case(request_case)
    intent = pending.intent.model_copy(
        update={
            "minimum_output": pending.intent.minimum_output.model_copy(update={"amount": 10**70})
        }
    )
    payload = payload.model_copy(
        update={"escrow": payload.escrow.model_copy(update={"intent": intent})}
    )
    pending = pending.model_copy(
        update={"intent": intent, "eligible_at": opportunity.publication.block.timestamp}
    )
    result = advance(config, settings, payload, pending, frame(opportunity))
    assert result.phase == "cancelled"
    assert "minimum" in result.reason
    returned = next(value for value in result.assets if value.key.asset == intent.amount.asset)
    assert returned.amount == intent.amount.amount
    assert result.keeper.amount > 0
    assert result.after.spent == payload.spent


def test_unexpired_request_waits_without_new_cost_or_input_spending(request_case):
    config, settings, payload, pending, opportunity = accepted_case(request_case)
    pending = pending.model_copy(
        update={"eligible_at": opportunity.publication.block.timestamp - 1}
    )
    result = advance(config, settings, payload, pending, frame(opportunity))
    assert result.phase == "waiting"
    assert result.after == payload
    assert result.assets == ()
    assert result.gas.amount == result.keeper.amount == result.refund.amount == 0


def test_expired_owner_cancellation_refunds_full_native_fee_and_leaves_no_allowance(
    request_case,
):
    config, settings, payload, pending, opportunity = accepted_case(request_case)
    risk = opportunity.risk.model_copy(update={"request_expiration_seconds": 1})
    record = frame(opportunity)
    record.payload.risk = (risk,)
    pending = pending.model_copy(
        update={
            "intent": Revoke(target=payload.target),
            "eligible_at": opportunity.publication.block.timestamp,
        }
    )
    cancelled = advance(config, settings, payload, pending, record)
    assert cancelled.phase == "cancelled"
    assert cancelled.after.escrow is None
    assert cancelled.refund.amount == payload.escrow.execution_fee.amount
    assert cancelled.keeper.amount == 0
    # The request-sized approval was consumed by escrow, so nothing remains to revoke.
    assert not any(value.amount for value in cancelled.after.allowances)
    with pytest.raises(ValueError, match="already closed"):
        advance(config, settings, cancelled.after, pending, record)


def test_continuation_cannot_substitute_another_swap(request_case):
    config, settings, payload, pending, opportunity = accepted_case(request_case)
    changed = pending.intent.model_copy(update={"deadline": pending.intent.deadline + 1})
    pending = pending.model_copy(update={"intent": changed})
    with pytest.raises(ValueError, match="cannot replace"):
        advance(config, settings, payload, pending, frame(opportunity))


def test_revoking_empty_authority_refuses_an_unmeasured_cost(request_case):
    config, settings, _, held, pending, _, opportunity = request_case
    pending = pending.model_copy(update={"intent": Revoke(target=held.payload.target)})
    with pytest.raises(ValueError, match="already closed"):
        advance(config, settings, held.payload, pending, frame(opportunity))
