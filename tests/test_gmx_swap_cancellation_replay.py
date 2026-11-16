"""Actual cancellation preserves the original request and deadline while refunding escrow."""

import gzip
from types import SimpleNamespace

import pytest

from almanak.connectors.gmx_v2 import exchange_planning, exchange_reconciliation
from almanak.connectors.gmx_v2.exchange_validation import validate_current
from almanak.connectors.gmx_v2.state import Checkpoint
from almanak.gateway.ethereum import DataError
from tests.gmx_retired import current_json
from tests.retained_fixtures import retained


@pytest.mark.parametrize("chain", ["arbitrum", "avalanche"])
@pytest.mark.parametrize("profile", ["safe_roles", "eoa"])
def test_native_swap_cancellation_replays_after_admission_expiry(chain, profile):
    path = (
        retained("2026-09-09-gmx-exchange")
        / f"{chain}-{profile}-cancellation-runtime/checkpoint.json.gz"
    )
    completed = Checkpoint.model_validate_json(current_json(gzip.decompress(path.read_bytes())))
    outcome = completed.outcomes[2]
    before = completed.model_copy(
        update={
            "mode": "teardown",
            "decision": outcome.decision,
            "requests": (outcome.before.request,),
            "owned": (outcome.before.owned,),
            "outcomes": completed.outcomes[:2],
        }
    )
    operation = exchange_planning.operation(
        completed.config, before, outcome.decision.intent, outcome.before
    )
    assert operation == outcome.operation
    plan = SimpleNamespace(operation=operation, decision=outcome.decision, before=outcome.before)
    assert outcome.before.block.timestamp > outcome.decision.intent.deadline
    validate_current(plan, outcome.before)
    settled = outcome.before.model_copy(update={"pending_order": None})
    with pytest.raises(DataError, match="settled or became ineligible"):
        validate_current(plan, settled)
    request = exchange_reconciliation.completed_request(plan, outcome.receipt, outcome.after.block)
    assert request == outcome.request
    assets, owned = exchange_reconciliation.transaction_effects(
        plan, outcome.receipt, outcome.after, request, True
    )
    assert assets == outcome.assets and owned == outcome.after.owned
    assert request.status == "cancelled"
    assert request.admission_deadline == outcome.before.request.admission_deadline
    assert not outcome.strategy_result().success and outcome.strategy_result().complete
