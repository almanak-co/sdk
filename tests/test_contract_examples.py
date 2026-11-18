"""Committed examples of the JSON contracts the SDK emits.

Every example validates against its schema, and the SDK producers still emit them.
"""

import copy
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import jsonschema
import pytest
from referencing import Registry, Resource

from almanak.backtest import progress
from almanak.checks import CheckFailure, check
from almanak.cli import error_document
from almanak.dashboard.evaluate import evaluate
from almanak.dashboard.preview import schema as dashboard_schema
from almanak.gateway.permissions import installation_bundle, statement
from almanak.strategy import StrategyDefinition
from tests.contract_examples import CONTRACTS, DIRECTORY, assert_example, index, load
from tests.test_checks_online import REFERENCE, arguments
from tests.test_checks_online import archive as archive
from tests.test_checks_online import authority_trace as authority_trace
from tests.test_checks_online import fixture as fixture
from tests.test_checks_online import fork_case as fork_case
from tests.test_checks_online import original as original
from tests.test_checks_online import production_case as production_case
from tests.test_checks_online import rpc as rpc
from tests.test_checks_online import safe_generation as safe_generation
from tests.test_checks_online import setup as setup

CASES = [
    (contract, case)
    for contract, entry in sorted(index()["contracts"].items())
    for case in sorted(entry["examples"])
]


def registry() -> Registry:
    schemas = [json.loads(path.read_text()) for path in sorted(CONTRACTS.glob("*.schema.json"))]
    return Registry().with_resources(
        (schema["$id"], Resource.from_contents(schema)) for schema in schemas
    )


def validator(source: dict) -> jsonschema.Draft202012Validator:
    if "command" in source:
        assert source["command"] == "almanak support --schema dashboard"
        return jsonschema.Draft202012Validator(dashboard_schema())
    root = json.loads((CONTRACTS / source["file"]).read_text())
    return jsonschema.Draft202012Validator(
        {"$ref": root["$id"] + source.get("ref", "")},
        registry=registry(),
        format_checker=jsonschema.Draft202012Validator.FORMAT_CHECKER,
    )


@pytest.mark.parametrize(("contract", "case"), CASES)
def test_every_example_validates_against_its_schema(contract, case):
    validator(index()["contracts"][contract]["schema"]).validate(load(contract, case))


def test_the_index_names_every_example_and_a_schema_change_breaks_one():
    on_disk = {
        (path.parent.name, path.stem)
        for path in DIRECTORY.glob("*/*.json")
        if path.parent != DIRECTORY
    }
    assert on_disk == set(CASES)
    for entry in index()["contracts"].values():
        assert set(entry) >= {"schema", "producer", "checked_by", "examples"}
        assert (Path(__file__).parents[1] / entry["checked_by"]).is_file()
    # A schema requiring a member the producers do not emit refuses the committed example.
    funding = json.loads((CONTRACTS / "funding-v1.schema.json").read_text())
    stricter = copy.deepcopy(funding)
    stricter["required"].append("observed_balances")
    with pytest.raises(jsonschema.ValidationError, match="observed_balances"):
        jsonschema.validate(load("almanak-funding-v1", "fork"), stricter)


def test_online_checks_emit_the_check_and_funding_examples(fork_case):
    fork = check(
        arguments(
            fork_case.directory,
            fork_case.operator_path,
            fork_case.setup.env_file,
            fork=True,
            online=True,
        )
    )
    assert "permissions" not in fork
    assert_example("almanak-check", "fork_online", fork)
    assert_example("almanak-funding-v1", "fork", fork["funding"])


def test_a_safe_production_check_emits_the_permissions_and_refusal_examples(
    production_case, safe_generation, tmp_path
):
    case = production_case
    online = arguments(
        case.directory, case.operator_path, case.setup.env_file, production=True, online=True
    )
    report = check(online)
    assert_example("almanak-check", "production_online_safe", report)
    assert_example("almanak-funding-v1", "production", report["funding"])
    safe_generation.calls["error"] = ValueError("Roles modifier is not enabled on the Safe")
    with pytest.raises(CheckFailure) as failure:
        check(online)
    refusal = error_document(failure.value)
    assert refusal["funding"] == report["funding"]
    assert_example("almanak-check-refusal", "permissions_refused", refusal)


def test_a_refused_backtest_check_emits_its_refusal_example(tmp_path):
    broken = tmp_path / "backtest.yaml"
    broken.write_text(
        (REFERENCE / "backtest.yaml").read_text().replace("lending_1:", "unknown_fund:")
    )
    with pytest.raises(CheckFailure) as failure:
        check(arguments(REFERENCE, backtest_config=broken))
    assert_example("almanak-check-refusal", "backtest_refused", error_document(failure.value))


def test_permission_statements_emit_the_permissions_examples(authority_trace):
    from almanak.connectors.aave_v4.permission_adapter import PermissionManifest

    manifest = PermissionManifest.model_validate_json(json.dumps(authority_trace["manifest"]))
    assert_example("almanak-permissions-v1", "installed", statement(manifest))
    first = manifest.partitions[0]
    owed = first.model_copy(update={"transactions": installation_bundle(first)})
    pending = manifest.model_copy(update={"partitions": (owed, *manifest.partitions[1:])})
    assert_example("almanak-permissions-v1", "pending", statement(pending))


class Clock:
    """Monotonic seconds and UTC time that move only when a test advances them."""

    start = datetime(2026, 10, 1, tzinfo=UTC)

    def __init__(self):
        self.seconds = 0.0

    def monotonic(self) -> float:
        return self.seconds

    def datetime(self):
        clock = self

        class Frozen(datetime):
            @classmethod
            def now(cls, tz=None):
                return clock.start + timedelta(seconds=clock.seconds)

        return Frozen


def progress_job(tmp_path, snapshots, history, attempt):
    job = progress.ProgressJob(
        tmp_path / f"progress-{attempt}.json",
        "4f0c7c3ad2b2d4a6e1a9c0f8b7e6d5c4b3a2918070605040302010f0e0d0c0b0",
        history=history,
        callback=lambda snapshot: snapshots.append(json.loads(snapshot.model_dump_json())),
    )
    job.job_id, job.attempt_id = f"{attempt}" * 32, f"{attempt + 1}" * 32
    return job


def start(job):
    """Plan as the CLI does, after entering, so a phase picks up its timing history."""
    job.plan("startup", "Validating inputs", 1.0, "phases")
    job.plan("simulation", "Simulating strategy", 1209600.0, "simulation_seconds")
    job.plan("finalization", "Finalizing result", 1.0, "phases")
    job.begin("startup", "Validating inputs", 1.0, "phases")


def test_a_progress_job_emits_the_progress_examples(tmp_path, monkeypatch):
    clock = Clock()
    monkeypatch.setattr(progress, "monotonic", clock.monotonic)
    monkeypatch.setattr(progress, "datetime", clock.datetime())
    # Snapshots come only from the job's own transitions, never a wall-clock heartbeat.
    monkeypatch.setattr(progress.ProgressJob, "heartbeat", lambda self: None)
    history = tmp_path / "history.json"
    first: list[dict] = []
    with progress_job(tmp_path, first, history, 1) as job:
        start(job)
        clock.seconds += 2.0
        job.begin("simulation", "Simulating strategy", 1209600.0, "simulation_seconds")
        clock.seconds += 30.0
        job.update(1209600.0)
        job.finish("simulation")
        job.begin("finalization", "Finalizing result", 1.0, "phases")
        clock.seconds += 1.0
        job.finish("finalization")
    assert_example("backtest-progress-v1", "calibrating", first[0])
    assert_example("backtest-progress-v1", "completed", first[-1])
    second: list[dict] = []
    with pytest.raises(RuntimeError), progress_job(tmp_path, second, history, 3) as job:
        start(job)
        clock.seconds += 2.0
        job.begin("simulation", "Simulating strategy", 1209600.0, "simulation_seconds")
        clock.seconds += 600.0
        job.update(120960.0)
        raise RuntimeError("strategy failed")
    assert_example("backtest-progress-v1", "running", second[-2])
    assert_example("backtest-progress-v1", "failed", second[-1])
    third: list[dict] = []
    with pytest.raises(KeyboardInterrupt), progress_job(tmp_path, third, None, 5) as job:
        start(job)
        clock.seconds += 1.0
        raise KeyboardInterrupt
    assert_example("backtest-progress-v1", "interrupted", third[-1])


def test_dashboard_evaluation_emits_the_response_examples(
    recorded, reference_strategy, monkeypatch
):
    state = recorded.model_copy(update={"strategy": reference_strategy})
    ready = evaluate(state, captured_at=1790812800)
    assert ready.status == "ready"
    assert_example("almanak-dashboard-response", "ready", ready.model_dump(mode="json"))
    without_hook = StrategyDefinition(filename="strategy.py", source="def decide(ctx): pass\n")
    absent = evaluate(state.model_copy(update={"strategy": without_hook}), captured_at=1790812800)
    assert absent.status == "absent"
    assert_example("almanak-dashboard-response", "absent", absent.model_dump(mode="json"))

    def unavailable():
        raise RuntimeError("missing sandbox")

    monkeypatch.setattr("almanak.worker_process.worker_command", unavailable)
    refused = evaluate(state, captured_at=1790812800)
    assert refused.status == "unavailable"
    assert_example("almanak-dashboard-response", "unavailable", refused.model_dump(mode="json"))


def test_a_historical_backtest_emits_the_report_example(tmp_path):
    from almanak.backtest.cli import create
    from tests.backtest_support import ROOT, Archive, archive_server, converted_config

    history = Archive()
    server, thread = archive_server(history)
    try:
        config, operator, env_file = converted_config(tmp_path, history, server)
        report = create(
            SimpleNamespace(
                strategy=ROOT / "strategies/aave_v4_weth_supply/strategy.py",
                config=config,
                backtest_config=config.with_name("backtest.yaml"),
                gateway_config=operator,
                env_file=env_file,
                output=tmp_path / "run",
            )
        )
    finally:
        server.shutdown()
        server.server_close()
        thread.join()
    assert report["closed"] and report["completed_steps"] == 12
    assert_example("almanak-backtest-report", "historical_path", report)
