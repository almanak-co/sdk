"""Recheck retained native transitions offline without advertising candidate support."""

import gzip
import json
from types import SimpleNamespace

import pytest

from almanak.connectors.curve import abi
from almanak.connectors.curve import connector as binding
from almanak.connectors.curve import stable_math as math
from almanak.connectors.curve.execution import CurveExecution
from almanak.connectors.curve.lifecycle import CurveLifecycle
from almanak.connectors.curve.operations import Addition, Approval, Removal, Swap
from almanak.connectors.curve.receipts import movement
from almanak.connectors.curve.reconciliation import (
    allowances_after,
    owned_after,
    validate_balances,
)
from almanak.connectors.curve.state import Checkpoint
from almanak.connectors.curve.validation import validate_current, validate_operation
from almanak.gateway.ethereum import DataError
from tests.curve_retired import current_json
from tests.retained_fixtures import retained

DIRECTORY = retained("2026-09-07-rollout") / "curve-native-stable-initial"
CASES = tuple(
    DIRECTORY / case["path"]
    for case in json.loads((DIRECTORY / "manifest.json").read_text())["cases"]
    if case["status"] == "passed"
)

BASE = DIRECTORY.parent / "curve-base-stable-native"
CASES += tuple(
    BASE / "native" / case["case"]
    for case in json.loads((BASE / "manifest.json").read_text())["cases"]
)


ARBITRUM = DIRECTORY.parent / "curve-arbitrum-stable-native"
CASES += tuple(
    ARBITRUM / "native" / case["case"]
    for case in json.loads((ARBITRUM / "manifest.json").read_text())["cases"]
)


@pytest.fixture(params=CASES)
def retained(request):
    raw = gzip.decompress((request.param / "checkpoint.json.gz").read_bytes())
    return Checkpoint.model_validate_json(current_json(raw))


def test_native_receipts_reconcile_exact_coin_movements_shares_and_allowances(retained):
    for outcome in retained.outcomes:
        assert outcome.success and not outcome.accounting_issues
        effects = movement(outcome.receipt, outcome.operation, outcome.before)
        assert (effects.asset_changes, effects.share_change) == (
            outcome.asset_changes,
            outcome.share_change,
        )
        assert owned_after(outcome.before, outcome.operation, effects, True) == outcome.after.owned
        assert (
            allowances_after(outcome.before, outcome.operation, effects, True)
            == outcome.after.allowances
        )
        assert (
            validate_balances(outcome, outcome.after, effects, outcome.gas_paid.amount, True) == ()
        )
        assert abi.call(outcome.operation) == outcome.call


def test_integer_pool_predictions_equal_native_swaps_mints_and_burns(retained):
    for outcome in retained.outcomes:
        operation = outcome.operation
        pool = outcome.before.pool.math_state(timestamp=outcome.after.block.timestamp)
        changes = tuple(value.amount for value in outcome.asset_changes)
        if isinstance(operation, Swap):
            incoming = operation.target.assets.index(operation.amount.asset)
            outgoing = operation.target.assets.index(operation.minimum.asset)
            prediction = math.swap(pool, incoming, outgoing, operation.amount.amount)
            assert changes[outgoing] == prediction.output
        elif isinstance(operation, Addition):
            predicted = math.addition(pool, tuple(value.amount for value in operation.amounts.root))
            assert outcome.share_change.amount == predicted.minted
        elif isinstance(operation, Removal):
            if operation.output_asset is None:
                assert changes == math.removal_proportional(pool, operation.shares.amount)
            else:
                index = operation.target.assets.index(operation.output_asset)
                assert (
                    changes[index] == math.removal_one(pool, index, operation.shares.amount).output
                )
        else:
            assert isinstance(operation, Approval) and not any(changes)


def test_native_admission_replays_retained_decision_bounds_and_owned_shares(retained):
    for index, outcome in enumerate(retained.outcomes):
        state = SimpleNamespace(
            config=retained.config,
            capabilities=retained.capabilities,
            decision=outcome.decision,
            owned=(outcome.before.owned,),
            outcomes=retained.outcomes[:index],
            mode="running",
        )
        backend = SimpleNamespace(config=retained.config)
        validate_operation(backend, state, outcome.operation, outcome.before)
        validate_current(
            backend,
            SimpleNamespace(
                operation=outcome.operation, before=outcome.before, decision=outcome.decision
            ),
            outcome.before,
        )
        assert CurveExecution.call(retained.config, outcome.operation) == outcome.call
        assert outcome.strategy_result().status == (
            "completed" if outcome.complete() else "intermediate"
        )


def test_a_signed_approval_is_rebroadcast_after_its_intent_expires(retained):
    outcome = next(o for o in retained.outcomes if isinstance(o.operation, Approval))
    backend = SimpleNamespace(config=retained.config)
    intent = outcome.decision.intent
    pool = outcome.before.pool
    block = pool.block.model_copy(update={"timestamp": intent.deadline})
    expired = outcome.before.model_copy(update={"pool": pool.model_copy(update={"block": block})})
    unsigned = SimpleNamespace(
        operation=outcome.operation, before=outcome.before, decision=outcome.decision
    )
    with pytest.raises(DataError, match="retained deadline expired"):
        validate_current(backend, unsigned, expired)
    validate_current(backend, SimpleNamespace(**vars(unsigned), raw_transaction="0x01"), expired)


def test_a_signed_curve_action_is_rebroadcast_past_its_prepared_deadline(retained):
    """Curve calls encode no deadline: signed bytes stay executable, so they are never released."""
    outcome = next(
        o for o in retained.outcomes if isinstance(o.operation, (Swap, Addition, Removal))
    )
    backend = SimpleNamespace(config=retained.config)
    pool = outcome.before.pool
    block = pool.block.model_copy(update={"timestamp": outcome.operation.deadline + 1})
    late = outcome.before.model_copy(update={"pool": pool.model_copy(update={"block": block})})
    unsigned = SimpleNamespace(
        operation=outcome.operation, before=outcome.before, decision=outcome.decision
    )
    # Preparation and signing stay bounded by the prepared deadline.
    with pytest.raises(DataError, match="deadline expired"):
        validate_current(backend, unsigned, late)
    validate_current(backend, SimpleNamespace(**vars(unsigned), raw_transaction="0x01"), late)
    assert getattr(CurveExecution, "expired", None) is None


def test_native_initialization_and_closure_keep_named_funds_separate(retained):
    initial = CurveLifecycle.initial(
        retained.config, retained.strategy, retained.capabilities, retained.permissions
    )
    CurveExecution.validate_config(retained.config)
    CurveLifecycle.validate_initial(initial, retained.baseline)
    for account in retained.config.accounts.bindings:
        funding, cleanup = CurveExecution.preflight(
            retained.config, account.name, retained.baseline
        )
        assert cleanup and all(value["funded"] for value in funding)
    owned = initial.owned
    for outcome in retained.outcomes:
        owned = CurveLifecycle.commit_owned(owned, outcome)
    assert owned == retained.owned
    CurveLifecycle.validate_closure(retained, retained.closure)
    projection = binding.connector().projection
    report = projection.report(retained)
    context, _ = projection.dashboard(retained)
    assert report["transaction_accounting_complete"]
    assert all(value["shares"]["amount"] == "0" for value in report["positions"])
    assert all(
        value.amount == "0" for key, value in context.values.items() if key.endswith("-shares")
    )
