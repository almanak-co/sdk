"""Decision-result branch contracts for execution plan construction."""

from __future__ import annotations

import logging
from types import SimpleNamespace

import pytest

from almanak.framework.execution.plan import StepStatus
from almanak.framework.execution.plan_builder import build_plan_from_decide_result
from almanak.framework.intents.vocabulary import IntentSequence, IntentType


def _intent(intent_id: str, intent_type: IntentType = IntentType.SWAP, chain: str | None = None):
    return SimpleNamespace(
        intent_id=intent_id,
        intent_type=intent_type,
        chain=chain,
        from_token="USDC",
        to_token="WETH",
        protocol="uniswap_v3",
    )


def _planned_intent_ids(plan) -> list[str]:
    assert plan is not None
    return [step.intent["intent_id"] for step in plan.steps]


@pytest.mark.parametrize(
    "decide_result",
    [
        None,
        _intent("hold", IntentType.HOLD),
        [],
        [_intent("hold", IntentType.HOLD), object()],
    ],
)
def test_no_action_results_do_not_build_a_plan(decide_result) -> None:
    assert build_plan_from_decide_result(decide_result) is None


def test_single_intent_preserves_plan_and_step_metadata() -> None:
    plan = build_plan_from_decide_result(
        _intent("swap-1"),
        deployment_id="deployment:test-plan",
        default_chain="optimism",
    )

    assert plan is not None
    assert plan.deployment_id == "deployment:test-plan"
    assert plan.plan_id.startswith("deployment-")
    assert plan.description == "SWAP on optimism"
    assert _planned_intent_ids(plan) == ["swap-1"]
    assert plan.steps[0].chain == "optimism"
    assert plan.steps[0].dependencies == []
    assert plan.steps[0].status is StepStatus.PENDING
    assert plan.steps[0].max_retries == 3


def test_sequence_preserves_description_order_and_dependencies() -> None:
    sequence = IntentSequence(
        [_intent("swap-1", chain="base"), _intent("swap-2", chain="arbitrum")],
        description="Bridge allocation",
    )

    plan = build_plan_from_decide_result(sequence, deployment_id="deployment:sequence")

    assert plan is not None
    assert plan.description == "Bridge allocation"
    assert _planned_intent_ids(plan) == ["swap-1", "swap-2"]
    assert plan.steps[0].dependencies == []
    assert plan.steps[1].dependencies == [plan.steps[0].step_id]


def test_list_flattens_sequences_in_place_and_filters_top_level_holds() -> None:
    sequence = IntentSequence([_intent("swap-2"), _intent("swap-3")])
    result = [
        _intent("swap-1"),
        sequence,
        _intent("hold", IntentType.HOLD),
        object(),
        _intent("swap-4"),
    ]

    plan = build_plan_from_decide_result(result)

    assert _planned_intent_ids(plan) == ["swap-1", "swap-2", "swap-3", "swap-4"]
    assert plan is not None
    assert [step.dependencies for step in plan.steps] == [
        [],
        [plan.steps[0].step_id],
        [plan.steps[1].step_id],
        [plan.steps[2].step_id],
    ]


def test_unknown_result_type_logs_warning_and_returns_none(caplog: pytest.LogCaptureFixture) -> None:
    with caplog.at_level(logging.WARNING):
        result = build_plan_from_decide_result(("not", "a", "supported", "result"))

    assert result is None
    assert "Unknown decide_result type: <class 'tuple'>" in caplog.text


def test_intent_attribute_errors_propagate() -> None:
    class BrokenIntent:
        @property
        def intent_type(self):
            raise RuntimeError("intent type unavailable")

    with pytest.raises(RuntimeError, match="intent type unavailable"):
        build_plan_from_decide_result(BrokenIntent())
