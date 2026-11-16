"""One lifecycle, with target collections kept separate from execution routing."""

import importlib.util
import json
import shutil
import signal
import sqlite3
import subprocess
import sys
import time
from contextlib import ExitStack
from dataclasses import dataclass
from pathlib import Path
from uuid import uuid4

from almanak.caip import ChainId
from almanak.configuration.unresolved import public_root
from almanak.configuration.yaml import load
from almanak.connectors.composition import connector_for
from almanak.gateway.fork_fixture import Fork
from almanak.gateway.fork_workspace import ForkSet
from almanak.models import Block, Chain, SafeRoles
from almanak.runtime import report
from almanak.strategy import StrategyDefinition
from tests.fork.support import (
    CLI,
    processes,
    request_teardown,
    resume,
    start_gateway,
    wait_state,
)

ROOT = Path(__file__).resolve().parents[2]
# References whose connector declares fork mode before their fork scenario is written. The
# QA catalog carries their fork obligations as blocked cases (`tests/qa/catalog.py`), so
# discovery leaves them out instead of refusing the missing scenario.
PENDING_SCENARIOS: frozenset[str] = frozenset()


@dataclass(frozen=True)
class Case:
    name: str
    strategy: Path
    config: Path
    assertions: Path


def discover(root: Path, scenarios: Path) -> tuple[Case, ...]:
    cases = []
    for source in sorted(root.rglob("strategy.py")):
        relative = source.parent.relative_to(root)
        case = Case(
            relative.as_posix(),
            source,
            source.with_name("config.yaml"),
            scenarios / relative / "assertions.py",
        )
        if backtest_only(case.config) or case.name in PENDING_SCENARIOS:
            continue
        require_files(case)
        cases.append(case)
    if not cases:
        raise ValueError("no strategies discovered")
    return tuple(cases)


def backtest_only(config: Path) -> bool:
    """A reference whose every market's connector declares no fork mode has no fork
    lifecycle to discover."""
    from almanak.connectors.composition import connector_from_wire
    from almanak.connectors.support import RunMode

    if not config.is_file():
        return False
    markets = load(config).value.get("markets")
    if not isinstance(markets, dict) or not markets:
        return False
    return all(
        isinstance(market, dict)
        and not connector_from_wire({"connector": market.get("connector")}).supported_chains(
            RunMode.FORK
        )
        for market in markets.values()
    )


def require_files(case):
    for path in (case.config, case.assertions):
        if not path.is_file():
            raise ValueError(f"{case.name}: required file is missing: {path}")


def load_assertions(case):
    spec = importlib.util.spec_from_file_location("strategy_fork_assertions", case.assertions)
    if spec is None or spec.loader is None:
        raise ValueError(f"{case.name}: cannot load assertions")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    for name in ("entered", "assert_holding", "assert_closed"):
        if not callable(getattr(module, name, None)):
            raise ValueError(f"{case.name}: assertions must define {name}")
    return module


@dataclass(frozen=True)
class Binding:
    origin: Block
    execution: Chain
    instance: str
    endpoint: str
    node: Fork


def resolve_config(config, env_file, cache):
    return config.resolve_all(env_file, cache)


def case_fork_config(case, env_file, directory):
    """Build a fork config from the reference root currently on disk."""
    if public_root(load(case.config).value):
        from tests.strategy_fork.adapter import fork_config_for, resolve_reference

        operator = ROOT / "examples/operators" / (case.name + ".fork.yaml")
        progress = resolve_reference(case.config, operator, env_file, directory)
        return fork_config_for(case.config, operator, progress)
    from scripts.legacy_configuration.author import read_author

    return read_author(case.config).fork_config()


def start_targets(config, env_file, stack) -> dict[ChainId, Binding]:
    bindings = {}
    for target in config.forks:
        node = stack.enter_context(Fork(env_file, target.block, target=target))
        instance = node.metadata["instanceId"]
        execution = Chain(chain_id=target.chain.chain_id, environment="anvil:" + instance)
        bindings[target.chain.chain_id] = Binding(
            target.block, execution, instance, node.rpc.endpoint, node
        )
    return bindings


def run_case(case, env_file, cache, temporary, artifacts, operator_mode="configured"):
    if public_root(load(case.config).value):
        from tests.strategy_fork.supervised import run_supervised_case

        operator = ROOT / "examples/operators" / (case.name + ".fork.yaml")
        return run_supervised_case(case, operator, env_file, temporary, artifacts, operator_mode)
    artifact = artifacts / (case.name.replace("/", "--") + "-" + uuid4().hex)
    artifact.mkdir(parents=True)
    # Pytest can reuse numbered temporary paths after cleanup or a reboot.
    # Durable database claims require a fresh path for each independent fixture.
    temporary = temporary / artifact.name
    temporary.mkdir(parents=True)
    record = {
        "strategy": case.name,
        "phases": [],
        "status": "failed",
    }
    try:
        record["sha256"] = StrategyDefinition.read(case.strategy).sha256
        if operator_mode == "configured":
            execute_case(case, env_file, cache, temporary, record)
        else:
            execute_case(case, env_file, cache, temporary, record, operator_mode)
        record["status"] = "passed"
    except BaseException as exc:
        record["failure_type"] = type(exc).__name__
        raise
    finally:
        retain(temporary, artifact, record)
        print("Strategy fork evidence:", artifact)


def execute_case(case, env_file, cache, temporary, record, operator_mode="configured"):
    assertions = load_assertions(case)
    from scripts.legacy_configuration.author import read_author

    config = read_author(case.config).fork_config()
    if operator_mode == "distinct":
        config = distinct_operators(config)
    record["operator_mode"] = operator_mode
    config = resolve_config(config, env_file, cache)
    record["origins"] = [target.block.model_dump(mode="json") for target in config.forks]
    with ExitStack() as stack:
        nodes, node = bind_workspace(config, env_file, stack, record)
        launch = stack.enter_context(processes(temporary))
        directory = temporary / "deployment"
        deployment = nodes.prepare(config, directory)
        require_safe_accounts(deployment)
        journal, arguments = start_gateway(node, directory, launch, lease_seconds=30)
        install_permissions(case, nodes, directory, deployment, arguments)
        record["phases"].append("generated_permissions_installed_and_verified")
        check = getattr(assertions, "assert_permissions", None)
        if check is not None:
            record["authority_checks"] = check(node, deployment)
            record["phases"].append("generated_permissions_rejected_forbidden_calls")
        lifecycle(case, node, assertions, journal, arguments, launch, record["phases"])


def bind_workspace(config, env_file, stack, record):
    bindings = start_targets(config, env_file, stack)
    nodes = ForkSet(config, env_file)
    nodes.nodes = {b.origin.chain: b.node for b in bindings.values()}
    record["execution"] = [b.execution.model_dump(mode="json") for b in bindings.values()]
    return nodes, next(iter(nodes.nodes.values())) if len(nodes.nodes) == 1 else nodes


def require_safe_accounts(deployment):
    if not all(isinstance(b.profile, SafeRoles) for b in deployment.accounts.bindings):
        raise ValueError("every discovered strategy lifecycle requires Safe/Zodiac fund bindings")


def distinct_operators(config):
    from almanak.gateway.fork_config import ForkOperator, ForkSafeFund

    operators, funds = [], []
    for index, fund in enumerate(config.funds):
        if not isinstance(fund, ForkSafeFund):
            raise ValueError("operator variation requires declared Safe funds")
        existing = next(o for o in config.operators if o.name == fund.operator)
        name = f"operator_{index}"
        operators.append(
            ForkOperator(
                name=name, native_balance=existing.native_balance, foundry_account_index=100 + index
            )
        )
        funds.append(fund.model_copy(update={"operator": name}))
    return type(config).model_validate(
        {**config.model_dump(), "operators": tuple(operators), "funds": tuple(funds)}
    )


def install_permissions(case, node, directory, deployment, arguments):

    output = directory / "permissions"
    generated = subprocess.run(
        [
            CLI,
            "permissions",
            "generate",
            "--strategy",
            str(case.strategy),
            *arguments,
            "--output",
            str(output),
        ],
        capture_output=True,
        text=True,
        timeout=180,
    )
    assert generated.returncode == 0, generated.stderr
    (directory / "permission-generation.json").write_text(generated.stdout)
    manifest = connector_for(deployment).permissions.manifest_type.model_validate_json(
        (output / "manifest.json").read_text()
    )
    receipts = install_owner_bundle(node, manifest, deployment)
    (directory / "permission-installation.json").write_text(json.dumps(receipts, indent=2) + "\n")
    verified = subprocess.run(
        [CLI, "permissions", "verify", "--manifest", str(output / "manifest.json"), *arguments[2:]],
        capture_output=True,
        text=True,
        timeout=180,
    )
    assert verified.returncode == 0, verified.stderr
    (directory / "permission-verification.json").write_text(verified.stdout)


def install_owner_bundle(node, manifest, deployment):
    from almanak.gateway.safe_fixture import install_manifest

    for partition in manifest.partitions:
        assert partition.profile == deployment.accounts.binding(partition.account).profile
    return install_manifest(node, manifest)


def lifecycle(case, node, assertions, journal, arguments, launch, phases):
    running, recovered_hash = start_runtime(
        case, node, assertions, journal, arguments, launch, phases
    )
    drive_external(assertions, node, journal, running)
    holding = wait_state(
        journal, assertions.entered, running, timeout=getattr(assertions, "entry_timeout", 180)
    )
    assert_recovered_once(holding, recovered_hash)
    assert holding.strategy == StrategyDefinition.read(case.strategy)
    verify_hold(node, assertions, journal, running, holding)
    retain_entry_budgets(journal, arguments, "entered")
    phases.append("entered_and_held")
    stop_runtime(running, timeout=getattr(assertions, "process_timeout", 20))
    running = resume(launch, journal, arguments)
    verify_hold(node, assertions, journal, running, holding)
    retain_entry_budgets(journal, arguments, "runtime_resumed")
    phases.append("runtime_resumed")
    stop_runtime(running, timeout=getattr(assertions, "process_timeout", 20))
    restart_gateway(node, launch, timeout=getattr(assertions, "process_timeout", 20))
    running = resume(launch, journal, arguments)
    verify_hold(node, assertions, journal, running, holding)
    retain_entry_budgets(journal, arguments, "gateway_replaced")
    phases.append("gateway_replaced_and_resumed")
    exercise = getattr(assertions, "exercise_hold", None)
    if exercise is not None:
        exercise(node, journal, running)
        phases.append("timed_strategy_exit_verified")
    align_teardown(assertions, node, journal, running, phases)
    request_teardown(arguments)
    drive_external(assertions, node, journal, running)
    closed = wait_state(
        journal,
        lambda state: state.mode == "closed",
        running,
        timeout=getattr(assertions, "teardown_timeout", 180),
    )
    assert running.wait(timeout=getattr(assertions, "process_timeout", 20)) == 0
    assertions.assert_closed(node, closed)
    retain_entry_budgets(journal, arguments, "closed")
    phases.append("closed")


def assert_recovered_once(state, tx_hash):
    if tx_hash:
        assert len([o for o in state.outcomes if o.tx_hash == tx_hash]) == 1


def start_runtime(case, node, assertions, journal, arguments, launch, phases):
    operation = getattr(assertions, "crash_operation", None)
    if operation is None:
        return launch([CLI, "service", "start", "--strategy", str(case.strategy), *arguments]), None
    driver = Path(__file__).parents[1] / "fork/driver.py"
    stage = getattr(assertions, "crash_stage", "broadcast")
    if stage not in ("broadcast", "commit"):
        raise ValueError("lifecycle recovery requires a broadcast or committed-effect crash")
    interrupted = launch(
        [
            sys.executable,
            str(driver),
            "--strategy",
            str(case.strategy),
            *arguments,
            "--crash",
            stage,
            "--operation",
            operation,
        ]
    )
    assert interrupted.wait(timeout=getattr(assertions, "crash_timeout", 180)) == -signal.SIGKILL
    tx_hash = interrupted_identity(journal.load(), operation, stage)
    restart_gateway(node, launch, timeout=getattr(assertions, "process_timeout", 20))
    phases.append(
        "submitted_pending_identity_survived_runtime_and_gateway_replacement"
        if stage == "broadcast"
        else "committed_effect_survived_before_delivery_and_gateway_replacement"
    )
    return resume(launch, journal, arguments), tx_hash


def pending_steps(state):
    return tuple(slot.pending for slot in (state.work or (state,)) if slot.pending is not None)


def interrupted_identity(state, operation, stage):
    if stage == "broadcast":
        matches = tuple(p for p in pending_steps(state) if p.operation.kind == operation)
        assert len(matches) == 1
        return matches[0].tx_hash
    return committed_identity(state, operation)


def committed_identity(state, operation):
    assert state.delivered < len(state.outcomes)
    outcome = state.outcomes[-1]
    assert outcome.operation.kind == operation
    assert all(p.tx_hash != outcome.tx_hash for p in pending_steps(state))
    return outcome.tx_hash


def verify_hold(node, assertions, journal, running, baseline):
    # Holding still evaluates the strategy and records scans without economic outcomes.
    deadline = time.monotonic() + max(
        30, baseline.config.decision_trigger.seconds * 3, getattr(assertions, "hold_timeout", 0)
    )
    previous = journal.load()
    advances = 0
    scans_previous = baseline
    while time.monotonic() < deadline:
        assert running.poll() is None, "runtime exited during hold"
        current = journal.load()
        validate_hold_scans(scans_previous, current)
        scans_previous = current
        # Repayment coverage is re-measured at every decision while debt accrues; it is a
        # measurement like the scans, not economic or strategy state.
        excluded = {"schedule", "external_scans", "repayment"}
        assert stable_state(current).model_dump(exclude=excluded) == stable_state(
            baseline
        ).model_dump(exclude=excluded), "hold/resume changed economic or strategy state"
        if current.schedule != previous.schedule:
            assert current.schedule.last_evaluation.due_at == previous.schedule.next_due_at
            advances += 1
            previous = current
        if advances == 2:
            assertions.assert_holding(node, current)
            return
        time.sleep(0.1)
    raise AssertionError("hold did not observe two scheduled evaluations")


def validate_hold_scans(before, after):
    from almanak.external_effects import ExternalScan

    initial = getattr(before, "external_scans", ())
    current = getattr(after, "external_scans", ())
    assert current[: len(initial)] == initial, "hold changed retained external scan history"
    assert all(
        isinstance(page, ExternalScan) and page.outcome is None for page in current[len(initial) :]
    ), "hold recorded an external economic outcome"


def stable_state(state):
    return state.model_copy(update={"scheduler_cursor": 0}) if hasattr(state, "work") else state


def stop_runtime(process, *, timeout=20):
    process.send_signal(signal.SIGINT)
    assert process.wait(timeout=timeout) == 130


def restart_gateway(node, launch, *, timeout=20):
    node.gateway_process.send_signal(signal.SIGINT)
    assert node.gateway_process.wait(timeout=timeout) == 0
    node.gateway_process = launch(node.gateway_arguments, ready="gateway_ready")


def retain(temporary, artifact, record):
    primary = sys.exception()
    error = retain_outputs(temporary, artifact, record)
    (artifact / "result.json").write_text(json.dumps(record, indent=2) + "\n")
    if error is not None and primary is None:
        raise RuntimeError(record["evidence_error"]) from error


def retain_outputs(temporary, artifact, record):
    try:
        retain_execution(temporary, artifact, record)
    except Exception as error:
        record["status"] = "failed"
        record.setdefault(
            "evidence_error",
            f"could not retain complete deployment evidence ({type(error).__name__})",
        )
        return error
    return None


def retain_execution(temporary, artifact, record):
    for log in temporary.glob("process-*.log"):
        shutil.copyfile(log, artifact / log.name)
    retain_context(temporary / "deployment", artifact)
    database = temporary / "deployment/state.sqlite"
    if database.exists():
        retain_database(database, artifact, record)
    if record.get("evidence_error"):
        raise ValueError(record["evidence_error"])


def retain_context(directory, artifact):
    for name in ("permissions",):
        if (directory / name).is_dir():
            shutil.copytree(directory / name, artifact / name)
    for name in (
        "deployment.json",
        "production.json",
        "rehearsal-routes.json",
        "fork-routes.json",
        "fixture-installation.json",
        "permission-generation.json",
        "permission-installation.json",
        "permission-verification.json",
        "permission-budget-observations.json",
        "lp-burn-minima.json",
        "external-execution.json",
    ):
        if (directory / name).is_file():
            shutil.copyfile(directory / name, artifact / name)


def retain_database(database, artifact, record):
    from almanak.state import Journal

    try:
        journal = Journal(database)
        with journal.connect() as source, sqlite3.connect(artifact / "state.sqlite") as target:
            source.backup(target)
        state = journal.load()
        (artifact / "checkpoint.json").write_text(state.model_dump_json(indent=2) + "\n")
        (artifact / "report.json").write_text(json.dumps(report(state), indent=2) + "\n")
    except (ValueError, sqlite3.Error):
        record["evidence_error"] = "could not decode or back up the deployment database"
        return
    retain_dashboard(state, artifact)


def retain_dashboard(state, artifact):
    from almanak.dashboard.evaluate import evaluate
    from almanak.dashboard.preview import render_html, write_private

    response = evaluate(state)
    if response.status == "ready":
        write_private(artifact / "dashboard.json", response.model_dump_json(indent=2))
        write_private(artifact / "dashboard.html", render_html(response))


def retain_entry_budgets(journal, arguments, stage):
    from almanak.gateway.client import GatewayClient

    client = GatewayClient(arguments[3], Path(arguments[5]))
    try:
        state = journal.load()
        regenerated = client.generate_permissions(state.config, state.strategy)
        assert not any(p.transactions for p in regenerated.partitions)
        result = client.verify_permissions(regenerated)
        assert_consumed_budgets(result)
        path = journal.path.parent / "permission-budget-observations.json"
        observations = json.loads(path.read_text()) if path.exists() else []
        observations.append({"stage": stage, "verification": result})
        path.write_text(json.dumps(observations, indent=2) + "\n")
    finally:
        client.close()


def assert_consumed_budgets(result):
    """Installed authority still verifies mid-run; scope-only grants carry no quota."""
    assert result["partitions"]
    assert all("quotas" not in partition for partition in result["partitions"])


def align_teardown(assertions, node, journal, running, phases):
    """A scenario may move the fork clock so its closing requests meet retained fixture inputs."""
    align = getattr(assertions, "align_teardown", None)
    if align is not None:
        align(node, journal, running)
        phases.append("fork_clock_aligned_for_teardown")


def drive_external(assertions, node, journal, running):
    driver = getattr(assertions, "drive_external", None)
    if driver is not None:
        driver(node, journal, running)
