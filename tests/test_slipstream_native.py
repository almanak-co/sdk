"""Retained process proofs constrain candidate codecs, author hooks and shared execution."""

import gzip
import importlib
import json
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace

import pytest

from almanak.caip import ChainId
from almanak.connectors.aerodrome_slipstream.models import Parameters
from almanak.connectors.aerodrome_slipstream.state import Checkpoint, DecisionInput
from almanak.connectors.aerodrome_slipstream.support import candidate_support
from almanak.connectors.composition import connector_for, connector_from_wire
from almanak.connectors.support import RunMode
from almanak.runtime import report, strategy_capabilities, strategy_decision
from almanak.strategy import StrategyDefinition
from almanak.strategy_api import Memory
from almanak.triggers import DecisionSchedule
from scripts.legacy_configuration.author import read_author
from tests.exchange_scope import without_exchange_budgets
from tests.legacy_references import (
    current_imports_if_legacy,
    harness_directory,
    reviewed_current_imports,
)
from tests.pancake_frozen_source import frozen_sources
from tests.strategy_fork.v3_pool_assertions import assert_closed

ROOT = Path(__file__).parents[1]
EVIDENCE = ROOT / "docs/internal/reports/evidence/2026-09-07-rollout/slipstream-base-native"


@pytest.fixture
def candidate_binding(monkeypatch):
    module = importlib.import_module("almanak.connectors.aerodrome_slipstream.connector")
    native = module.connector
    monkeypatch.setattr(
        module, "connector", lambda: replace(native(), chain_support=candidate_support())
    )


CASES = json.loads((EVIDENCE / "manifest.json").read_text())["cases"]
# The LP lifecycle recaptured after #160 moved the mint and increase minimums
# to the edge of the admitted price window; the earlier operations no longer re-plan exactly.
RECAPTURED = EVIDENCE.with_name(EVIDENCE.name + "-2026-10-20")
RECAPTURED_CASES = {
    case["name"] for case in json.loads((RECAPTURED / "manifest.json").read_text())["cases"]
}
# The executed LP author source of the original capture.
LP_SOURCE = "7df7b9be3ff214f3842131cb839da4b390b8fe2a590fe5b6d6c8051f06223c61"


@pytest.mark.parametrize("case", CASES, ids=lambda case: case["name"])
def test_recorded_runtime_closure_revalidates_plans_reports_and_author_hook(
    candidate_binding, case
):
    reference = ROOT / "strategies" / case["reference"] / "strategy.py"
    if case["name"] in RECAPTURED_CASES:
        frozen = frozen_sources(RECAPTURED, "source-freeze.json.gz")
        validate_recorded_case(
            RECAPTURED / case["name"], reference, frozen[reference.relative_to(ROOT).as_posix()]
        )
    else:
        validate_recorded_case(EVIDENCE / case["name"], reference)


def validate_recorded_case(evidence, reference, lp_source=LP_SOURCE):
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
    assert context.config.connector == "aerodrome_slipstream"
    assert context.positions[0].target == state.owned[0].target
    assert projected.values
    parameters = Parameters.model_validate(
        {name: getattr(state.config, name) for name in Parameters.model_fields}
    )
    definition = StrategyDefinition.read(reference)
    if reference.parent.name.endswith("_lp"):
        definition = state.strategy
        assert definition.sha256 == lp_source
        definition = current_imports_if_legacy(definition)
    else:
        assert definition == reviewed_current_imports(state.strategy)
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


def test_qualified_slipstream_support_admits_base_production():
    connector = connector_from_wire({"connector": "aerodrome_slipstream"})
    assert connector.supported_chains(RunMode.FORK) == (ChainId("eip155:8453"),)
    assert connector.supported_chains(RunMode.BACKTEST) == (ChainId("eip155:8453"),)
    assert connector.supported_chains(RunMode.PRODUCTION) == (ChainId("eip155:8453"),)
    for style in ("trading", "lp"):
        read_author(harness_directory(f"aerodrome_slipstream_base_{style}") / "config.yaml")
