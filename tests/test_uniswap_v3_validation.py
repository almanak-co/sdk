"""Admission bounds inputs by inventory before approval and binds retained NFT evidence."""

import gzip
import json
from pathlib import Path
from types import SimpleNamespace

import pytest
from pydantic import TypeAdapter

from almanak.connectors.uniswap_v3.authority import declared
from almanak.connectors.uniswap_v3.models import Parameters, Target
from almanak.connectors.uniswap_v3.observations import Snapshot
from almanak.connectors.uniswap_v3.operations import ProtocolOperation
from almanak.connectors.uniswap_v3.planning import operation as plan
from almanak.connectors.uniswap_v3.validation import (
    validate_current,
    validate_inventory,
    validate_operation,
    validate_owned,
)
from almanak.dex_intents import ExitExchange, IncreaseLiquidity, OpenLiquidity, SwapExactInput
from almanak.gateway.ethereum import DataError
from tests.legacy_references import current_effective

CAPTURE = json.loads(
    gzip.decompress(
        (Path(__file__).with_name("fixtures") / "uniswap-v3-operations.json.gz").read_bytes()
    )
)


def state_for(kind="swap", chain=0):
    row = CAPTURE[chain]
    config = Parameters.model_validate_json(json.dumps(current_effective(row["config"])))
    step = next(s for s in row["steps"] if s["operation"]["kind"] == kind)
    before = Snapshot.model_validate_json(json.dumps(step["before"]))
    op = TypeAdapter(ProtocolOperation).validate_json(json.dumps(step["operation"]))
    if kind == "swap":
        intent = SwapExactInput[Target](
            target=op.target, amount=op.amount, minimum_output=op.minimum, deadline=op.deadline
        )
    else:
        cls = OpenLiquidity if kind == "mint" else IncreaseLiquidity
        intent = cls[Target](
            target=op.target, maximum0=op.maximum0, maximum1=op.maximum1, deadline=op.deadline
        )
    state = SimpleNamespace(
        config=config,
        owned=(before.owned,),
        capabilities=declared(config),
        decision=SimpleNamespace(intent=intent, observations=(before,)),
        outcomes=(),
        mode="running",
    )
    return state, before, intent


@pytest.mark.parametrize("kind", ["swap", "mint", "increase"])
@pytest.mark.parametrize("chain", [0, 1])
def test_valid_economic_action_and_approval_share_admission(kind, chain):
    state, before, intent = state_for(kind, chain)
    op = plan(state.config, state, intent, before)
    validate_operation(None, state, op, before)
    empty = before.model_copy(
        update={"allowances": tuple(a.model_copy(update={"amount": 0}) for a in before.allowances)}
    )
    approval = plan(state.config, state, intent, empty)
    assert approval.kind == "approve"
    validate_operation(None, state, approval, empty)


@pytest.mark.parametrize("kind", ["swap", "mint", "increase"])
def test_repeated_actions_have_no_cumulative_budget(kind):
    state, before, intent = state_for(kind)
    op = plan(state.config, state, intent, before)
    state.outcomes = tuple(
        SimpleNamespace(operation=op, success=True, accounting_issues=()) for _ in range(1000)
    )
    empty = before.model_copy(
        update={"allowances": tuple(a.model_copy(update={"amount": 0}) for a in before.allowances)}
    )
    approval = plan(state.config, state, intent, empty)
    validate_operation(None, state, approval, empty)


@pytest.mark.parametrize("kind", ["swap", "mint", "increase"])
def test_inputs_beyond_attributed_inventory_are_refused(kind):
    state, before, intent = state_for(kind)
    validate_inventory(intent, before)
    asset = intent.amount.asset if kind == "swap" else intent.maximum0.asset
    owned = before.owned
    field = "inventory0" if owned.inventory0.asset == asset else "inventory1"
    held = getattr(owned, field)
    requested = intent.amount.amount if kind == "swap" else intent.maximum0.amount
    short = owned.model_copy(update={field: held.model_copy(update={"amount": requested - 1})})
    with pytest.raises(DataError, match="attributed input inventory") as refused:
        validate_inventory(intent, before.model_copy(update={"owned": short}))
    assert refused.value.kind == "inventory"


@pytest.mark.parametrize(
    "change", ["simulated", "deadline", "minimum", "inventory", "accounting", "mode"]
)
def test_invalid_execution_is_refused(change):
    state, before, intent = state_for()
    if change == "simulated":
        before = before.model_copy(update={"simulated": True})
    elif change == "deadline":
        intent = intent.model_copy(update={"deadline": before.block.timestamp})
    elif change == "minimum":
        intent = intent.model_copy(
            update={"minimum_output": intent.minimum_output.model_copy(update={"amount": 1})}
        )
    elif change == "inventory":
        state.owned = (
            before.owned.model_copy(
                update={"inventory0": before.owned.inventory0.model_copy(update={"amount": 0})}
            ),
        )
        before = before.model_copy(update={"owned": state.owned[0]})
    elif change == "accounting":
        state.outcomes = (
            SimpleNamespace(
                operation=None, success=False, accounting_issues=("unexplained movement",)
            ),
        )
    else:
        state.mode = "teardown"
    state.decision.intent = intent
    op = plan(state.config, state, intent, before)
    with pytest.raises((ValueError, DataError)):
        validate_operation(None, state, op, before)


def test_nft_external_checkpoint_change_and_stale_prepared_authority_rejected():
    state, before, intent = state_for("increase")
    foreign = before.position.model_copy(update={"owed0": before.position.owed0 + 1})
    with pytest.raises(DataError, match="checkpoint"):
        validate_owned(before.owned, before.model_copy(update={"position": foreign}))
    prepared = SimpleNamespace(
        before=before, operation=plan(state.config, state, intent, before), decision=state.decision
    )
    backend = SimpleNamespace(config=state.config)
    validate_current(backend, prepared, before)
    changed = before.model_copy(
        update={"allowances": tuple(a.model_copy(update={"amount": 0}) for a in before.allowances)}
    )
    with pytest.raises(DataError, match="authority changed"):
        validate_current(backend, prepared, changed)


def late(snapshot, timestamp):
    block = snapshot.block.model_copy(update={"timestamp": timestamp})
    return snapshot.model_copy(update={"pool": snapshot.pool.model_copy(update={"block": block})})


@pytest.mark.parametrize("kind", ["mint", "increase"])
def test_a_signed_approval_is_rebroadcast_after_its_intent_expires(kind):
    """An approval has no protocol deadline: once signed, only its own validity bounds it."""
    row = CAPTURE[0]
    config = Parameters.model_validate_json(json.dumps(current_effective(row["config"])))
    steps = row["steps"]
    index = next(i for i, s in enumerate(steps) if s["operation"]["kind"] == kind)
    assert steps[index - 1]["operation"]["kind"] == "approve"
    approval = TypeAdapter(ProtocolOperation).validate_json(
        json.dumps(steps[index - 1]["operation"])
    )
    action = TypeAdapter(ProtocolOperation).validate_json(json.dumps(steps[index]["operation"]))
    before = Snapshot.model_validate_json(json.dumps(steps[index - 1]["before"]))
    cls = OpenLiquidity if kind == "mint" else IncreaseLiquidity
    intent = cls[Target](
        target=action.target,
        maximum0=action.maximum0,
        maximum1=action.maximum1,
        deadline=action.deadline,
    )
    assert before.block.timestamp < intent.deadline
    backend = SimpleNamespace(config=config)
    unsigned = SimpleNamespace(
        before=before, operation=approval, decision=SimpleNamespace(intent=intent)
    )
    expired = late(before, intent.deadline)
    validate_current(backend, unsigned, before)
    # Signing still requires the intent's admission window.
    with pytest.raises(DataError, match="retained deadline expired"):
        validate_current(backend, unsigned, expired)
    # The recorded signed approval stays valid for rebroadcast after the intent expires.
    signed = SimpleNamespace(**vars(unsigned), raw_transaction="0x01")
    validate_current(backend, signed, expired)
    # A signed protocol action past its own deadline is still refused before submission.
    action_signed = SimpleNamespace(**{**vars(signed), "operation": action})
    with pytest.raises(DataError, match="prepared protocol deadline expired"):
        validate_current(backend, action_signed, late(before, intent.deadline + 1))


def test_a_signed_collect_is_rebroadcast_past_its_prepared_deadline():
    """The position manager's `collect` takes no deadline, so signed bytes stay executable."""
    row = CAPTURE[0]
    config = Parameters.model_validate_json(json.dumps(current_effective(row["config"])))
    step = next(s for s in row["steps"] if s["operation"]["kind"] == "collect")
    collect = TypeAdapter(ProtocolOperation).validate_json(json.dumps(step["operation"]))
    before = Snapshot.model_validate_json(json.dumps(step["before"]))
    # An exit-style intent has no admission window, so only the prepared deadline applies.
    intent = ExitExchange[Target](target=collect.target)
    backend = SimpleNamespace(config=config)
    unsigned = SimpleNamespace(
        before=before, operation=collect, decision=SimpleNamespace(intent=intent)
    )
    expired = late(before, collect.deadline + 1)
    validate_current(backend, unsigned, late(before, collect.deadline - 1))
    # Signing at the deadline is already too late, though a signed copy could still land.
    with pytest.raises(DataError, match="prepared protocol deadline expired"):
        validate_current(backend, unsigned, late(before, collect.deadline))
    validate_current(backend, SimpleNamespace(**vars(unsigned), raw_transaction="0x01"), expired)
