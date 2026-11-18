"""Actual Aave reference through resolved inputs and generated fixture authority."""

import hashlib
import json
import secrets
import shutil
import sqlite3
import subprocess
import threading
import time
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

import pytest

from almanak.capabilities import CapabilityCodec
from almanak.configuration.admission import PreparedDeployment
from almanak.configuration.bootstrap import read_progress, resolve_configuration
from almanak.configuration.fork_preparation import prepare_fork
from almanak.configuration.unresolved import read_inputs
from almanak.connectors.support import RunMode
from almanak.gateway.fixture_accounts import provision_accounts
from almanak.gateway.fixture_preparation import compile_fixture
from almanak.gateway.fork_permissions import prepare_permissions
from almanak.gateway.fork_workspace import ForkSet, fork_routes
from almanak.gateway.operator_config import read_operator_config
from almanak.gateway.operator_fixture import EoaFixtureFund
from almanak.gateway.service import GatewayService, http_server
from almanak.runtime import Runtime
from almanak.state import Journal
from almanak.strategy import StrategyDefinition
from almanak.supervisor_preparation import create_workspace, retained
from almanak.supervisor_process import running, stop
from almanak.workspace import read as read_workspace
from almanak.workspace import request as control
from tests.strategy_fork.harness import Case
from tests.strategy_fork.scenarios.aave_v4_weth_supply.assertions import (
    assert_closed,
    assert_holding,
    entered,
)
from tests.strategy_fork.supervised import (
    PHASE_TIMEOUT,
    await_checkpoint,
    await_stage,
    end_process,
    fork_handle,
    launch_supervisor,
    retain_workspace,
    run_supervised_case,
)
from tests.test_author_resolution import REFERENCE
from tests.test_bootstrap_gateway import connected
from tests.test_operator_fixture import fixture as fixture
from tests.test_operator_fixture import operator as operator
from tests.test_operator_fixture import original as original
from tests.test_prepared_configuration import forbid_hooks
from tests.test_resolution_context import revalidate

ROOT = Path(__file__).parents[2]
EXAMPLE_OPERATOR = ROOT / "examples/operators/aave_v4_weth_supply.fork.yaml"
pytestmark = pytest.mark.fork


@contextmanager
def gateway(bundle, operator, database, directory, token):
    config = bundle.configuration()
    backend = fork_routes(config, directory / "fork-routes.json", directory / "fork-keys.json")
    try:
        service = GatewayService(
            config, database, backend, lease_seconds=30, preparation=bundle, operator=operator
        )
        server = http_server(service, token, 0)
        thread = threading.Thread(target=server.serve_forever)
        thread.start()
        try:
            yield f"http://127.0.0.1:{server.server_port}"
        finally:
            server.shutdown()
            thread.join(5)
            server.server_close()
    finally:
        backend.close()


def advance(runtime, predicate):
    deadline = time.monotonic() + PHASE_TIMEOUT
    while time.monotonic() < deadline:
        state = runtime.journal.load()
        if predicate(state):
            return state
        if not runtime.tick():
            time.sleep(0.05)
    raise AssertionError("reference lifecycle did not reach its required phase")


def eoa_operator(operator):
    fixture = operator.fork
    funds = tuple(
        EoaFixtureFund(
            kind="eoa", account=f.account, native_balance=fixture.native_balance(f), assets=f.assets
        )
        for f in fixture.funds
    )
    return revalidate(operator, fork=revalidate(fixture, funds=funds, operators=()))


@pytest.mark.parametrize("kind", ["safe_roles", "eoa"])
def test_prepared_reference_fork_roundtrip(request, tmp_path, operator, kind, monkeypatch):
    if kind == "eoa":
        operator = eoa_operator(operator)
    document = read_inputs(REFERENCE / "config.yaml")
    context = document.inputs.context(operator, RunMode.FORK, operator.fork.anchors)
    definition = StrategyDefinition.read(REFERENCE / "strategy.py")
    token = tmp_path / "auth.token"
    token.write_text(secrets.token_hex(32))
    token.chmod(0o600)
    setup = SimpleNamespace(
        context=context,
        operator=operator,
        env_file=request.config.getoption("--fork-env").resolve(),
        token_file=token,
    )
    artifact = (
        ROOT / "docs/internal/reports/evidence" / ("prepared-aave-" + kind + "-" + uuid4().hex)
    )
    artifact.mkdir(parents=True)
    report = {
        "status": "failed",
        "account_profile": kind,
        "phases": [],
        "source_revision": subprocess.check_output(
            ["git", "rev-parse", "HEAD"], cwd=ROOT, text=True
        ).strip(),
        "authority": "identified Anvil only",
        "strategy_sha256": definition.sha256,
        "proof": "actual HTTP services and runtime replacement; no OS process-death claim",
    }
    try:
        with connected(setup) as client:
            resolve_configuration(tmp_path / "resolution.json", context, client)
        progress = read_progress(tmp_path / "resolution.json")
        report["phases"].append("authenticated_archive_resolution")
        plan = compile_fixture(context, operator, progress.result.anchors)
        with ForkSet(plan, setup.env_file) as nodes:
            execute_reference(
                tmp_path,
                artifact,
                setup,
                nodes,
                document,
                definition,
                progress,
                report,
                monkeypatch,
            )
        report["status"] = "passed"
    finally:
        retain_inputs(tmp_path, artifact)
        report["artifacts"] = {
            path.name: hashlib.sha256(path.read_bytes()).hexdigest()
            for path in sorted(artifact.iterdir())
            if path.is_file()
        }
        (artifact / "report.json").write_text(json.dumps(report, indent=2) + "\n")
        print("Prepared Aave proof:", artifact)


def retain_inputs(root, artifact):
    for relative in (
        "resolution.json",
        "hooks.json",
        "fixture/fixture-started.json",
        "fixture/fixture-accounts.json",
        "fixture/fixture-installation.json",
    ):
        retain_file(root / relative, artifact)
    database = root / "state.sqlite"
    if database.is_file():
        retain_database(database, artifact / "state.sqlite")


def retain_file(path, artifact):
    if path.is_file():
        shutil.copyfile(path, artifact / path.name)


def retain_database(database, destination):
    with sqlite3.connect(f"file:{database}?mode=ro", uri=True) as source:
        with sqlite3.connect(destination) as retained:
            source.backup(retained)
    assert Journal(destination).load() == Journal(database).load()
    assert (
        Journal(destination).configuration_preparation()
        == Journal(database).configuration_preparation()
    )


def execute_reference(
    root, artifact, setup, nodes, document, definition, progress, report, monkeypatch
):
    directory = root / "fixture"
    fixture = provision_accounts(directory, nodes.config, setup.context, setup.operator, nodes)
    report["phases"].append("actual_accounts_funded")
    record = prepare_fork(
        root / "hooks.json", document, definition, progress, fixture, setup.operator
    )
    bundle = PreparedDeployment(resolution=progress, preparation=record, fixture=fixture)
    (artifact / "preparation.json").write_text(bundle.model_dump_json(indent=2) + "\n")
    report["phases"].append("final_sandboxed_hooks")
    forbid_hooks(monkeypatch)

    def forbidden(*_, **__):
        raise AssertionError("validated hooks cannot repeat during execution or replacement")

    monkeypatch.setattr(CapabilityCodec, "evaluate", forbidden)
    monkeypatch.setattr("almanak.runtime.validate_strategy_config", forbidden)
    monkeypatch.setattr("almanak.runtime.strategy_capabilities", forbidden)
    config = bundle.configuration()
    prepare_reference_permissions(nodes, config, directory, record, artifact, report)
    database = root / "state.sqlite"
    holding = enter_reference(
        bundle, setup, database, directory, config, definition, nodes, artifact
    )
    report["phases"].append("supplied_borrowed_and_held")
    assert_preparation_reused(
        root, directory, nodes, setup, fixture, document, definition, progress, record
    )
    close_reference(bundle, setup, database, directory, config, nodes, holding, artifact, report)
    assert Journal(database).configuration_preparation() == bundle
    report["phases"].append("repaid_withdrawn_and_closed")


def prepare_reference_permissions(nodes, config, directory, record, artifact, report):
    if report["account_profile"] != "safe_roles":
        return
    manifest = prepare_permissions(
        nodes, config, REFERENCE / "strategy.py", directory, prepared=record.prepared
    )
    (artifact / "permissions.json").write_text(manifest.model_dump_json(indent=2) + "\n")
    report["phases"].append("generated_roles_installed_and_verified")


def enter_reference(bundle, setup, database, directory, config, definition, nodes, artifact):
    with gateway(bundle, setup.operator, database, directory, setup.token_file) as endpoint:
        runtime = Runtime(endpoint, setup.token_file, create=config, strategy=definition)
        try:
            holding = advance(runtime, entered)
            assert_holding(nodes, holding)
            (artifact / "holding.json").write_text(holding.model_dump_json(indent=2) + "\n")
            return holding
        finally:
            runtime.close()


def assert_preparation_reused(
    root, directory, nodes, setup, fixture, document, definition, progress, record
):
    before = (root / "hooks.json").read_bytes()
    assert (
        provision_accounts(directory, nodes.config, setup.context, setup.operator, nodes) == fixture
    )
    assert (
        prepare_fork(root / "hooks.json", document, definition, progress, fixture, setup.operator)
        == record
    )
    assert (root / "hooks.json").read_bytes() == before


def close_reference(bundle, setup, database, directory, config, nodes, holding, artifact, report):
    with gateway(bundle, setup.operator, database, directory, setup.token_file) as endpoint:
        runtime = Runtime(endpoint, setup.token_file, expected=config)
        try:
            retained = runtime.journal.load()
            assert retained == holding
            assert_holding(nodes, retained)
            report["phases"].append("gateway_and_runtime_replaced_without_repeated_hooks")
            runtime.gateway.teardown()
            closed = advance(runtime, lambda state: state.mode == "closed")
            assert_closed(nodes, closed)
            (artifact / "closed.json").write_text(closed.model_dump_json(indent=2) + "\n")
        finally:
            runtime.close()


@pytest.mark.parametrize("kind", ["safe_roles", "eoa"])
def test_supervised_prepared_reference_fork_roundtrip(request, tmp_path, operator, kind):
    """Actual supervisor processes drive the prepared fork path end to end.

    The first supervisor is terminated after its coordinator pauses and its
    gateway stops; a replacement supervisor process reattaches the retained Anvil
    node, reuses the frozen preparation and launches a new gateway through the
    prepared fork route. This is graceful process replacement, not recovery from
    an OS kill during a broadcast.
    """
    operator = supervised_operator(operator, kind)
    document = read_inputs(REFERENCE / "config.yaml")
    context = document.inputs.context(operator, RunMode.FORK, operator.fork.anchors)
    definition = StrategyDefinition.read(REFERENCE / "strategy.py")
    operator_path = tmp_path / "operator.json"
    operator_path.write_text(operator.model_dump_json())
    env_file = request.config.getoption("--fork-env").resolve()
    artifact = (
        ROOT
        / "docs/internal/reports/evidence"
        / ("prepared-aave-supervised-" + kind + "-" + uuid4().hex)
    )
    artifact.mkdir(parents=True)
    report = supervised_report(kind, definition)
    root = create_workspace(
        tmp_path / "run", document, definition, context, operator_path, env_file
    )
    processes = [launch_supervisor(root)]
    try:
        first, bundle, holding, frozen = supervised_entry(root, artifact, report)
        replace_supervisor(root, processes, first, bundle, holding, frozen, report)
        supervised_teardown(root, artifact, bundle, report)
        report["status"] = "passed"
    finally:
        finish_supervised(root, processes, artifact, report)


def supervised_entry(root, artifact, report):
    status = await_stage(root, ("running",))
    report["phases"].append("supervised_resolution_fixture_hooks_and_admission")
    first = read_workspace(root)["processes"]
    assert status["coordinator"] == "running"
    bundle = retained(root)
    assert bundle.fixture is not None
    assert bundle.preparation.policy is None
    assert json.loads((root / "preflight.json").read_text())["ready"]
    holding = await_checkpoint(root, entered)
    assert_holding(fork_handle(root), holding)
    (artifact / "holding.json").write_text(holding.model_dump_json(indent=2) + "\n")
    report["phases"].append("supplied_borrowed_and_held")
    return first, bundle, holding, frozen_preparation(root)


def replace_supervisor(root, processes, first, bundle, holding, frozen, report):
    stop(first["coordinator"], interrupt=True)
    stop(first["gateway"])
    end_process(processes[0])
    assert running(first["fork-eip155:1"])
    processes.append(launch_supervisor(root))
    await_stage(root, ("paused",))
    replaced = read_workspace(root)["processes"]
    assert_replacement_processes(first, replaced)
    assert_replacement_state(root, bundle, holding, frozen)
    report["phases"].append("supervisor_gateway_and_coordinator_replaced_without_repeated_hooks")


def assert_replacement_processes(first, replaced):
    assert replaced["fork-eip155:1"] == first["fork-eip155:1"]
    assert replaced["read-gateway"] == first["read-gateway"]
    assert replaced["gateway"]["pid"] != first["gateway"]["pid"]
    assert running(replaced["gateway"])


def assert_replacement_state(root, bundle, holding, frozen):
    assert frozen_files(root, frozen) == frozen
    assert retained(root) == bundle
    status = control(root, "resume")
    assert status["stage"] == "running"
    assert await_checkpoint(root, entered) == holding
    assert_holding(fork_handle(root), holding)


def supervised_teardown(root, artifact, bundle, report):
    control(root, "teardown")
    closed = await_checkpoint(root, lambda state: state.mode == "closed")
    assert_closed(fork_handle(root), closed)
    (artifact / "closed.json").write_text(closed.model_dump_json(indent=2) + "\n")
    assert Journal(root / "state.sqlite").configuration_preparation() == bundle
    report["phases"].append("repaid_withdrawn_and_closed")
    control(root, "shutdown")


def supervised_operator(operator, kind):
    return eoa_operator(operator) if kind == "eoa" else operator


def supervised_report(kind, definition):
    return {
        "status": "failed",
        "account_profile": kind,
        "phases": [],
        "source_revision": subprocess.check_output(
            ["git", "rev-parse", "HEAD"], cwd=ROOT, text=True
        ).strip(),
        "anvil_version": subprocess.check_output(["anvil", "--version"], text=True).split()[2],
        "authority": "identified Anvil only",
        "strategy_sha256": definition.sha256,
        "proof": (
            "actual supervisor, read gateway, execution gateway and coordinator processes; "
            "graceful supervisor, gateway and coordinator replacement; no OS-kill claim"
        ),
    }


def frozen_preparation(root):
    names = (
        "private/parameters.json",
        "private/deployment/preparation.json",
        "private/deployment/deployment.json",
        "private/deployment/fixture/fixture-accounts.json",
    )
    return frozen_files(root, names)


def frozen_files(root, names):
    return {name: ((root / name).read_bytes(), (root / name).stat().st_mtime_ns) for name in names}


def finish_supervised(root, processes, artifact, report):
    for entry in read_workspace(root)["processes"].values():
        stop(entry)
    for process in processes:
        end_process(process)
    retain_workspace(root, artifact)
    report["artifacts"] = {
        path.name: hashlib.sha256(path.read_bytes()).hexdigest()
        for path in sorted(artifact.iterdir())
        if path.is_file()
    }
    (artifact / "report.json").write_text(json.dumps(report, indent=2) + "\n")
    print("Supervised prepared Aave proof:", artifact)


@pytest.mark.parametrize("kind", ["safe_roles", "eoa"])
def test_public_cli_supervised_reference_fork_roundtrip(request, tmp_path, kind):
    """The published commands alone drive the converted reference over its example operator.

    `almanak run --fork` reads the shipped author files and
    `examples/operators/aave_v4_weth_supply.fork.yaml`, then pause, resume, teardown
    and shutdown run as public commands in child processes. The EOA variant only
    rewrites that example's fixture funds; the safe_roles variant uses it verbatim.
    """
    operator_path = EXAMPLE_OPERATOR
    if kind == "eoa":
        operator_path = tmp_path / "operator.json"
        operator_path.write_text(
            eoa_operator(read_operator_config(EXAMPLE_OPERATOR)).model_dump_json()
        )
    case = Case(
        "aave_v4_weth_supply",
        REFERENCE / "strategy.py",
        REFERENCE / "config.yaml",
        ROOT / "tests/strategy_fork/scenarios/aave_v4_weth_supply/assertions.py",
    )
    package = (
        ROOT / "docs/internal/reports/evidence" / ("prepared-aave-cli-" + kind + "-" + uuid4().hex)
    )
    package.mkdir(parents=True)
    revision = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True)
    anvil = subprocess.check_output(["anvil", "--version"], text=True).split()[2]
    (package / "proof.json").write_text(
        json.dumps(
            {
                "account_profile": kind,
                "operator_source": str(EXAMPLE_OPERATOR.relative_to(ROOT)),
                "source_revision": revision.strip(),
                "anvil_version": anvil,
                "proof": (
                    "almanak run, pause, resume, teardown and shutdown as child processes "
                    "over the shipped author files and the example operator; no OS-kill claim"
                ),
            },
            indent=2,
        )
        + "\n"
    )
    run_supervised_case(
        case, operator_path, request.config.getoption("--fork-env").resolve(), tmp_path, package
    )
