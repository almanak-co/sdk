"""Retained process proofs constrain candidate codecs, author hooks and shared execution."""

import gzip
import importlib
import json
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace

import pytest

from almanak.connectors.composition import connector_for, connector_from_wire
from almanak.connectors.pancakeswap_v3.models import Parameters
from almanak.connectors.pancakeswap_v3.state import Checkpoint, DecisionInput
from almanak.connectors.pancakeswap_v3.support import candidate_support
from almanak.connectors.support import RunMode
from almanak.runtime import report, strategy_capabilities, strategy_decision
from almanak.strategy_api import Memory
from almanak.triggers import DecisionSchedule
from scripts.legacy_configuration.author import read_author
from tests.exchange_scope import without_exchange_budgets
from tests.legacy_references import current_imports_if_legacy, harness_directory
from tests.pancake_frozen_source import assert_frozen_reference, recaptured_reference
from tests.strategy_fork.v3_pool_assertions import assert_closed

ROOT = Path(__file__).parents[1]
EVIDENCE = ROOT / "docs/internal/reports/evidence/2026-09-07-rollout/pancakeswap-v3-bnb-native"
# LP lifecycles recaptured after #160 moved the mint and increase minimums to
# the edge of the admitted price window; the earlier operations no longer re-plan exactly.
RECAPTURED = EVIDENCE.with_name(EVIDENCE.name + "-2026-10-20")


@pytest.fixture
def candidate_binding(monkeypatch):
    module = importlib.import_module("almanak.connectors.pancakeswap_v3.connector")
    native = module.connector
    monkeypatch.setattr(
        module, "connector", lambda: replace(native(), chain_support=candidate_support())
    )


@pytest.mark.parametrize("case", ["eoa-trading", "eoa-lp", "safe-trading"])
def test_recorded_runtime_closure_revalidates_plans_reports_and_author_hook(
    candidate_binding, case
):
    style = "lp" if case == "eoa-lp" else "trading"
    reference = ROOT / f"strategies/pancakeswap_v3_bnb_{style}/strategy.py"
    if case == "eoa-lp":
        validate_recorded_case(RECAPTURED / case, reference, recaptured_reference(RECAPTURED))
    else:
        validate_recorded_case(EVIDENCE / case, reference)


def validate_recorded_case(evidence, reference, frozen=assert_frozen_reference):
    state = Checkpoint.model_validate_json(
        json.dumps(
            without_exchange_budgets(
                json.loads(gzip.decompress((evidence / "checkpoint.json.gz").read_bytes()))
            )
        )
    )
    connector = connector_for(state.config)
    assert_closed(None, state)
    report_file = evidence / "report.json"
    recorded_report = (
        report_file.read_bytes()
        if report_file.exists()
        else gzip.decompress((evidence / "report.json.gz").read_bytes())
    )
    assert report(state) == json.loads(recorded_report)
    for index, outcome in enumerate(state.outcomes):
        prior = state.model_copy(
            update={
                "mode": "teardown" if outcome.decision.trigger.kind == "teardown" else "running",
                "decision": outcome.decision,
                "owned": (outcome.before.owned,),
                "outcomes": state.outcomes[:index],
            }
        )
        connector.execution.validate_operation(
            SimpleNamespace(config=state.config), prior, outcome.operation, outcome.before
        )
        assert connector.execution.call(state.config, outcome.operation) == outcome.call
        assert outcome.strategy_result().intent == outcome.decision.intent
    context, projected = connector.projection.dashboard(state)
    assert context.config.connector == "pancakeswap_v3"
    assert context.positions[0].target == state.owned[0].target
    assert projected.values
    parameters = Parameters.model_validate(
        {name: getattr(state.config, name) for name in Parameters.model_fields}
    )
    definition = state.strategy
    frozen(reference, definition)
    definition = current_imports_if_legacy(definition)
    assert strategy_capabilities(definition, parameters) == state.capabilities
    observed = state.outcomes[0].before
    decision = strategy_decision(
        definition,
        DecisionInput(
            config=parameters,
            memory=Memory(),
            owned=(observed.owned,),
            observations=(observed,),
            trigger=DecisionSchedule().event(observed.effective_at),
        ),
    )
    assert decision.intent == state.outcomes[0].decision.intent


def test_normal_runtime_support_stages_production_on_every_qualified_chain():
    connector = connector_from_wire({"connector": "pancakeswap_v3"})
    expected = {"eip155:56", "eip155:1", "eip155:8453", "eip155:42161"}
    assert {str(c) for c in connector.supported_chains(RunMode.FORK)} == expected
    assert {str(c) for c in connector.supported_chains(RunMode.BACKTEST)} == expected
    assert {str(c) for c in connector.supported_chains(RunMode.PRODUCTION)} == expected
    for network in ("bnb", "ethereum", "base", "arbitrum"):
        for style in ("trading", "lp"):
            read_author(harness_directory(f"pancakeswap_v3_{network}_{style}") / "config.yaml")


ADDITIONAL = (
    ROOT / "docs/internal/reports/evidence/2026-09-07-rollout/pancakeswap-v3-additional-native"
)
ADDITIONAL_CASES = json.loads((ADDITIONAL / "manifest.json").read_text())["cases"]
ADDITIONAL_RECAPTURED = ADDITIONAL.with_name(ADDITIONAL.name + "-2026-10-20")
RECAPTURED_CASES = {
    case["name"]
    for case in json.loads((ADDITIONAL_RECAPTURED / "manifest.json").read_text())["cases"]
}


@pytest.mark.parametrize("case", ADDITIONAL_CASES, ids=lambda case: case["name"])
def test_additional_recorded_lifecycles(candidate_binding, case):
    reference = ROOT / "strategies" / case["reference"] / "strategy.py"
    if case["name"] in RECAPTURED_CASES:
        validate_recorded_case(
            ADDITIONAL_RECAPTURED / case["name"],
            reference,
            recaptured_reference(ADDITIONAL_RECAPTURED),
        )
    else:
        validate_recorded_case(ADDITIONAL / case["name"], reference)
