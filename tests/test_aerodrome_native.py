"""Retained process proofs constrain candidate codecs, author hooks and shared execution."""

import gzip
import importlib
import json
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace

import pytest

from almanak.caip import ChainId
from almanak.connectors.aerodrome.models import Parameters
from almanak.connectors.aerodrome.receipts import movement
from almanak.connectors.aerodrome.reconciliation import allowances_after, validate_balances
from almanak.connectors.aerodrome.state import Checkpoint, DecisionInput
from almanak.connectors.aerodrome.support import candidate_support
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
from tests.retained_fixtures import retained
from tests.strategy_fork.aerodrome_assertions import assert_closed

ROOT = Path(__file__).parents[1]
EVIDENCE = retained("2026-09-07-rollout/aerodrome-base-native")


@pytest.fixture
def candidate_binding(monkeypatch):
    module = importlib.import_module("almanak.connectors.aerodrome.connector")
    native = module.connector
    monkeypatch.setattr(
        module, "connector", lambda: replace(native(), chain_support=candidate_support())
    )


CASES = [
    c
    for c in json.loads((EVIDENCE / "manifest.json").read_text())["cases"]
    if c["status"] == "passed"
]


@pytest.mark.parametrize("case", CASES, ids=lambda case: case["name"])
def test_recorded_runtime_closure_revalidates_plans_reports_and_author_hook(
    candidate_binding, case
):
    validate_recorded_case(
        EVIDENCE / case["name"], ROOT / "strategies" / case["reference"] / "strategy.py"
    )


def validate_recorded_case(evidence, reference):
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
    initial = state.outcomes[0].before
    funding, clean = connector.execution.preflight(state.config, initial.target.account, (initial,))
    assert clean and all(row["funded"] for row in funding)
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
        connector.execution.validate_current(
            SimpleNamespace(config=state.config), outcome, outcome.before
        )
        effects = movement(outcome.receipt, outcome.operation, outcome.before.pool.pool_fees)
        assert (
            allowances_after(outcome.before, outcome.operation, effects, outcome.success)
            == outcome.after.allowances
        )
        assert (
            validate_balances(
                outcome, outcome.after, effects, outcome.gas_paid.amount, outcome.success
            )
            == outcome.accounting_issues
        )
    context, projected = connector.projection.dashboard(state)
    assert context.config.connector == "aerodrome"
    assert context.positions[0].target == state.owned[0].target
    assert projected.values
    parameters = Parameters.model_validate(
        {name: getattr(state.config, name) for name in Parameters.model_fields}
    )
    definition = StrategyDefinition.read(reference)
    if reference.parent.name.endswith("_lp"):
        definition = state.strategy
        assert (
            definition.sha256 == "b76068231837adb8a972adef60c00c48a0ac279e9644b33831424e622e4f7b58"
        )
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


def test_qualified_aerodrome_support_admits_base_production():
    connector = connector_from_wire({"connector": "aerodrome"})
    assert connector.supported_chains(RunMode.FORK) == (ChainId("eip155:8453"),)
    assert connector.supported_chains(RunMode.BACKTEST) == (ChainId("eip155:8453"),)
    assert connector.supported_chains(RunMode.PRODUCTION) == (ChainId("eip155:8453"),)
    for style in ("stable_trading", "stable_lp", "volatile_trading", "volatile_lp"):
        read_author(harness_directory(f"aerodrome_base_{style}") / "config.yaml")
