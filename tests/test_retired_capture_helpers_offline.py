"""The retained-capture helpers behave as their replay tests rely on, without evidence.

The replay tests that use these helpers read retained captures, so hosted CI skips them.
These cases run the helpers on synthetic values only.
"""

import json
from types import SimpleNamespace

import pytest
from pydantic import BaseModel

from almanak.connectors.gmx_v2.exchange_operations import ExchangeApproval
from tests.gmx_retired import assert_request_sized, current_json, replayed_operation
from tests.repeating_entries import without_repeated_entry_quotes


class Allowance(BaseModel):
    asset: str
    amount: int


class Observation(BaseModel):
    allowances: tuple[Allowance, ...]


def amount(asset, value):
    return SimpleNamespace(asset=asset, amount=value)


def approval(asset, value, target="router"):
    return ExchangeApproval.model_construct(target=target, amount=amount(asset, value))


def request(target="router", **escrow):
    return SimpleNamespace(target=target, inputs=[amount(a, v) for a, v in escrow.items()])


def test_current_drops_retired_fields_and_partition_quotas_only():
    raw = {
        "maximum_input": 1,
        "kept": [{"maximum_inputs": [2], "entry_amount": 3}, "text"],
        "partition": {"grants": ["g"], "quotas": ["q"]},
        "quotas": ["not a partition"],
    }
    assert json.loads(current_json(json.dumps(raw))) == {
        "kept": [{"entry_amount": 3}, "text"],
        "partition": {"grants": ["g"]},
        "quotas": ["not a partition"],
    }


def test_repeated_entry_quotes_are_removed_only_after_a_completed_entry():
    value = [
        {"entry_quote": "q", "owned": {"entry_complete": True}, "other": [1]},
        {"entry_quote": "q", "owned": {"entry_complete": False}},
        {"entry_quote": "q", "owned": None},
        {"nested": {"entry_quote": "q", "owned": {"entry_complete": True}}},
        "text",
    ]
    assert without_repeated_entry_quotes(value) == [
        {"entry_quote": None, "owned": {"entry_complete": True}, "other": [1]},
        {"entry_quote": "q", "owned": {"entry_complete": False}},
        {"entry_quote": "q", "owned": None},
        {"nested": {"entry_quote": None, "owned": {"entry_complete": True}}},
        "text",
    ]


def test_request_sized_accepts_an_unchanged_request_and_its_input_approvals():
    captured = request(a=5, b=0, c=7)
    approvals = (approval("a", 5), approval("c", 7))
    assert_request_sized(approvals, captured, captured, None)
    with pytest.raises(AssertionError):
        assert_request_sized((approval("a", 5),), captured, captured, None)
    with pytest.raises(AssertionError):
        assert_request_sized(
            (approval("a", 5, "other"), approval("c", 7)), captured, captured, None
        )


def test_request_sized_replays_a_revocation_exactly():
    assert_request_sized((), approval("a", 0), approval("a", 0), None)
    with pytest.raises(AssertionError):
        assert_request_sized((), approval("a", 1), approval("a", 0), None)


def test_request_sized_entry_approval_equals_the_following_escrow():
    following = request(a=5, b=3)
    assert_request_sized((approval("b", 3),), approval("a", 5), approval("a", 9), following)


@pytest.mark.parametrize(
    ("approvals", "operation", "following"),
    [
        ((), request(a=5), request(a=5)),
        ((), approval("a", 5, "other"), request(a=5)),
        ((), approval("b", 5), request(a=5, b=5)),
        ((), approval("a", 5), None),
        ((), approval("a", 5), request("other", a=5)),
        ((), approval("a", 4), request(a=5)),
        ((), approval("a", 0), request(a=0)),
        ((approval("b", 2),), approval("a", 5), request(a=5, b=3)),
    ],
)
def test_request_sized_entry_approval_refuses_any_mismatch(approvals, operation, following):
    with pytest.raises((AssertionError, KeyError)):
        assert_request_sized(approvals, operation, approval("a", 9), following)


def planner(need, final):
    """Approve the first asset whose allowance is below `need`, else plan `final`."""

    def plan(config, state, intent, observed):
        for value in observed.allowances:
            if value.amount < need.get(value.asset, 0):
                return approval(value.asset, need[value.asset])
        return final

    return plan


def outcome(captured, *allowances):
    before = Observation(allowances=tuple(Allowance(asset=a, amount=v) for a, v in allowances))
    return SimpleNamespace(before=before, decision=SimpleNamespace(intent=None), operation=captured)


def test_replay_starts_from_zero_allowances_and_returns_the_request():
    final = request(a=5, b=3)
    plan = planner({"a": 5, "b": 3}, final)
    approvals, operation = replayed_operation(plan, None, None, outcome(final, ("a", 9), ("b", 9)))
    assert (approvals, operation) == ((approval("a", 5), approval("b", 3)), final)


def test_replay_of_an_entry_approval_stops_at_the_captured_asset():
    plan = planner({"a": 5, "b": 3}, request(a=5, b=3))
    captured = approval("b", 9)
    approvals, operation = replayed_operation(
        plan, None, None, outcome(captured, ("a", 9), ("b", 9))
    )
    assert (approvals, operation) == ((approval("a", 5),), approval("b", 3))


def test_replay_of_a_revocation_uses_its_own_observation():
    captured = approval("a", 0)
    seen = []

    def plan(config, state, intent, observed):
        seen.append(observed)
        return captured

    observed = outcome(captured, ("a", 9))
    assert replayed_operation(plan, None, None, observed) == ((), captured)
    assert seen == [observed.before]


def test_replay_refuses_approvals_that_never_converge():
    def plan(config, state, intent, observed):
        return approval("a", 1)

    with pytest.raises(AssertionError, match="did not converge"):
        replayed_operation(plan, None, None, outcome(request(a=1), ("a", 0)))
