"""Retained process deaths prove allowance cleanup without an economic fill."""

import gzip
import json
from pathlib import Path

import pytest

from almanak.connectors.curve.state import Checkpoint
from almanak.connectors.identifiers import ConnectorId, ProtocolVariant
from tests.curve_retired import current_json
from tests.fork.test_curve_cleanup import (
    assert_allocation_cleanup,
    assert_approval_committed,
    assert_closed_effects,
)
from tests.qa.frozen_reference import compressed_reference
from tests.qa.worker import fork_actions
from tests.strategy_fork.curve_assertions import assert_gas_cost

ROOT = Path(__file__).parents[1]
DIRECTORY = ROOT / "docs/internal/reports/evidence/2026-09-07-rollout/curve-approval-cleanup"
DIRECTORIES = (
    DIRECTORY,
    DIRECTORY.parent / "curve-base-approval-cleanup",
    DIRECTORY.parent / "curve-base-twocrypto-approval-cleanup",
    DIRECTORY.parent / "curve-base-tricrypto-approval-cleanup",
    DIRECTORY.parent / "curve-arbitrum-approval-cleanup",
)
CASES = tuple(
    directory / "native" / case["case"]
    for directory in DIRECTORIES
    for case in json.loads((directory / "manifest.json").read_text())["cases"]
)


def raw(case, filename):
    return gzip.decompress((case / (filename + ".gz")).read_bytes())


@pytest.fixture(params=CASES)
def retained(request):
    record = json.loads(raw(request.param, "result.json"))
    before = Checkpoint.model_validate_json(
        current_json(json.dumps(record["interrupted_checkpoint"]))
    )
    after = Checkpoint.model_validate_json(current_json(raw(request.param, "checkpoint.json")))
    assert after.strategy == compressed_reference(
        request.param.parents[1], "source/strategies/" + record["strategy"] + "/strategy.py"
    )
    return record, before, after


def test_actual_strategy_cleanup_preserves_funds_and_reconciles_payer_gas(retained):
    record, before, after = retained
    assert record["status"] == "passed" and after.mode == "closed"

    assert_approval_committed(before)
    assert_closed_effects(before, after)
    for owned, initial, final in zip(after.owned, after.baseline, after.closure, strict=True):
        assert_allocation_cleanup(owned, initial, final)
        events = [o for o in after.outcomes if o.execution.submitter == initial.payer]
        assert_gas_cost(events, initial, final)


def test_qa_credits_only_the_successful_zero_approval(retained):
    _, before, after = retained
    target = after.outcomes[-1].operation.target
    kwargs = dict(connector=ConnectorId("curve"), variant=ProtocolVariant(target.pool.family))
    initial = before.model_dump(mode="json")["outcomes"]
    outcomes = after.model_dump(mode="json")["outcomes"]
    assert "revoke_exchange" not in fork_actions(initial, {target.allocation}, **kwargs)
    assert "revoke_exchange" in fork_actions(outcomes, {target.allocation}, **kwargs)
    outcomes[-1]["operation"]["amount"] = 1
    assert "revoke_exchange" not in fork_actions(outcomes, {target.allocation}, **kwargs)


@pytest.mark.parametrize("field", ["amount", "target"])
def test_nonzero_or_foreign_cleanup_cannot_satisfy_the_retained_process_proof(retained, field):
    _, before, after = retained
    original = after.outcomes[-1]
    value = 1 if field == "amount" else after.owned[1].target
    wrong = original.model_copy(
        update={"operation": original.operation.model_copy(update={field: value})}
    )
    with pytest.raises(AssertionError):
        assert_closed_effects(
            before, after.model_copy(update={"outcomes": (after.outcomes[0], wrong)})
        )
