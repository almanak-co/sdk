"""Economic transitions replay the receipts of the actual two-Safe borrowing proof."""

import gzip
import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from almanak.connectors.aave_v4.authority import validate_capabilities
from almanak.connectors.aave_v4.execution import validate_balances, validate_effects
from almanak.connectors.aave_v4.lifecycle import commit_ownership
from almanak.connectors.aave_v4.models import DecisionInput, OutcomeInput, Ownership
from almanak.connectors.aave_v4.operations import BorrowOperation, RepayOperation
from almanak.connectors.aave_v4.reader import plan_operation, receipt_movement
from almanak.connectors.aave_v4.state import Checkpoint
from almanak.connectors.aave_v4.validation import require_zero_debt, validate_repayment_funding
from almanak.dashboard.evaluate import evaluate
from almanak.gateway.ethereum import DataError
from almanak.gateway.fork_execution import ForkGateway
from almanak.reporting import report
from almanak.runtime import validate_closure
from almanak.triggers import TeardownEvent
from tests.lending_captures import current_capture
from tests.reference_parameters import TypedHooks

ROOT = Path(__file__).parents[1]


@pytest.fixture(scope="module")
def borrowing_proof():
    data = gzip.decompress(
        (ROOT / "tests/fixtures/safe-aave-borrowing-lifecycle.json.gz").read_bytes()
    )
    from tests.retained_scheduling import checkpoint_input

    return Checkpoint.model_validate_json(
        json.dumps(checkpoint_input(current_capture(json.loads(data))))
    )


@pytest.fixture
def reference_module():
    spec = importlib.util.spec_from_file_location(
        "aave_reference", ROOT / "strategies/aave_v4_weth_supply/strategy.py"
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return TypedHooks(module)


def beginning(proof, module):
    config = proof.config
    return proof.model_copy(
        update={
            "mode": "running",
            "outcomes": (),
            "delivered": 0,
            "closure": (),
            "memory": module.State(),
            "capabilities": module.capabilities(config),
            "owned": tuple(
                Ownership(target=config.target(a.name), shares=0) for a in config.positions
            ),
        }
    )


def test_real_strategy_and_connector_replay_every_borrowing_transition(
    borrowing_proof, reference_module
):
    state = beginning(borrowing_proof, reference_module)
    reference_module.validate_config(state.config)
    validate_capabilities(state.config, state.capabilities)
    for outcome in borrowing_proof.outcomes:
        state = replay_step(state, outcome, reference_module)
    assert state.owned == borrowing_proof.owned
    assert (
        state.memory.model_dump(exclude={"version", "values", "schema_id"})
        == borrowing_proof.memory.values
    )
    validate_closure(state, borrowing_proof.closure)


def replay_step(state, outcome, module):
    if outcome.decision.intent.kind in ("repay", "withdraw"):
        state = state.model_copy(update={"mode": "teardown"})
    context = DecisionInput(
        config=state.config,
        trigger=(
            TeardownEvent(started_at=outcome.decision.trigger.started_at)
            if state.mode == "teardown"
            else outcome.decision.trigger
        ),
        memory=state.memory,
        owned=state.owned,
        observations=outcome.decision.observations,
    )
    decision = module.decide(context)
    assert decision.intent == outcome.decision.intent
    allocation = state.config.allocation(outcome.operation.allocation)
    assert plan_operation(decision.intent, outcome.before, allocation) == outcome.operation
    state = state.model_copy(update={"decision": outcome.decision})
    gateway = object.__new__(ForkGateway)
    gateway.config, gateway.guard = state.config, lambda: None
    gateway.reader = SimpleNamespace(chain=outcome.operation.target.fund.chain)
    observed = {o.allocation: o for o in outcome.decision.observations}
    gateway.snapshot = lambda name, block, reference=None: observed[name]
    gateway.validate_operation(state, outcome.operation, outcome.before)
    verify_economics(state, outcome)
    memory = module.on_outcome(
        OutcomeInput(config=state.config, memory=state.memory, result=outcome.strategy_result())
    )
    return state.model_copy(
        update={
            "memory": memory,
            "owned": commit_ownership(state.owned, outcome),
            "outcomes": (*state.outcomes, outcome),
            "decision": None,
        }
    )


def verify_economics(state, outcome):
    allocation = state.config.allocation(outcome.operation.allocation)
    assets, shares = receipt_movement(
        outcome.receipt, allocation, outcome.operation.target.fund, outcome.operation
    )
    assert (assets, shares) == (outcome.assets, outcome.shares)
    pending = SimpleNamespace(before=outcome.before, operation=outcome.operation)
    validate_balances(pending, outcome.after, assets, shares, outcome.gas_paid.amount)
    amount = outcome.operation.amount.amount if outcome.operation.amount else 0
    validate_effects(pending, outcome.after, outcome.success, assets, shares, amount)


def test_debt_dashboard_and_costs_preserve_liability_semantics(borrowing_proof, reference_strategy):
    proof = borrowing_proof.model_copy(update={"strategy": reference_strategy})
    response = evaluate(proof)
    assert response.status == "ready", response.diagnostic
    rebuilt = report(proof)
    assert len(rebuilt["gas_payers"]) == 2
    assert sum(int(p["gas_paid"]["amount"]) for p in rebuilt["gas_payers"]) == sum(
        o.gas_paid.amount for o in proof.outcomes
    )
    for debt, row in zip(proof.config.debts, rebuilt["debts"], strict=True):
        assert int(row["borrowed"]["amount"]) == borrowed_assets(proof, debt.name)
        cost = int(row["realized_borrowing_cost"]["amount"])
        assert 0 < cost < 1_000_000
        assert response.snapshot.values[debt.name + "-cost"].amount == str(cost)
        assert response.snapshot.values[debt.name + "-debt"].amount == "0"


@pytest.mark.parametrize("fault", ["funding", "drawn", "premium", "offset"])
def test_unresolved_debt_cannot_release_collateral(borrowing_proof, fault):
    row = next(o for o in borrowing_proof.outcomes if isinstance(o.operation, RepayOperation))
    debt = borrowing_proof.config.allocation(row.operation.allocation)
    if fault == "funding":
        with pytest.raises(DataError, match="send the shortfall"):
            validate_repayment_funding(row.before.model_copy(update={"wallet_balance": 0}), debt)
    else:
        field = {
            "drawn": "drawn_shares",
            "premium": "premium_shares",
            "offset": "premium_offset_ray",
        }[fault]
        residual = row.after.model_copy(update={field: 1})
        with pytest.raises(DataError, match="zero drawn and premium"):
            require_zero_debt(residual)
        incomplete = row.model_copy(update={"after": residual}).strategy_result()
        assert incomplete.success and not incomplete.complete


def test_projected_borrow_intent_cannot_inherit_supply_permission(
    borrowing_proof, reference_module
):
    from almanak.connectors.aave_v4.authority import admit_intent

    cap = reference_module.capabilities(borrowing_proof.config)
    debt = borrowing_proof.config.debts[0]
    amount = borrowed_assets(borrowing_proof, debt.name)
    with pytest.raises(ValueError, match="asset-specific"):
        admit_intent(cap, borrowing_proof.config.supply(debt.name, amount))
    supply = borrowing_proof.config.allocations[0]
    with pytest.raises(ValueError, match="asset-specific"):
        admit_intent(cap, borrowing_proof.config.borrow(supply.name, supply.capital))
    borrowed = next(o for o in borrowing_proof.outcomes if isinstance(o.operation, BorrowOperation))
    assert borrowed.after.wallet_balance - borrowed.before.wallet_balance == amount
    # The debt's amount is the strategy's decision: the reference borrows its BORROW constant.
    assert amount == reference_module.borrow_amount(
        reference_module.typed_config(borrowing_proof.config), debt.name
    )


def borrowed_assets(proof, name):
    """Assets the recorded lifecycle borrowed for one debt; the config states no amount."""
    return sum(
        o.assets
        for o in proof.outcomes
        if o.success and isinstance(o.operation, BorrowOperation) and o.operation.allocation == name
    )
