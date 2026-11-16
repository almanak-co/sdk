"""Prepared fork supervision reuses complete fixtures and never recreates a lost node."""

import json
import os
import signal
import subprocess
import sys
from contextlib import ExitStack
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

import pytest

from almanak import checks, supervisor_forks, supervisor_preparation, workspace
from almanak.configuration.prepared import read_preparation
from almanak.configuration.resolution import AnchorSelection
from almanak.configuration.unresolved import read_inputs
from almanak.connectors.support import RunMode
from almanak.gateway import fixture_accounts
from almanak.gateway.ethereum import DataError
from almanak.gateway.fixture_accounts import provision_accounts, require_fixture_nodes
from almanak.gateway.fixture_preparation import compile_fixture
from almanak.gateway.fork_config import provider_endpoint
from almanak.gateway.fork_fixture import Fork, Rpc
from almanak.gateway.fork_workspace import ForkSet
from almanak.strategy import StrategyDefinition, ValidationRequest
from almanak.supervisor import Supervisor
from almanak.supervisor_forks import attach_nodes, reconcile_fixture
from almanak.supervisor_preparation import create_workspace, provision, retained
from almanak.supervisor_process import running, stop
from tests.strategy_fork.supervised import eoa_inputs
from tests.test_author_resolution import REFERENCE, gasless_reference
from tests.test_author_resolution import archive as archive
from tests.test_author_resolution import rpc as rpc
from tests.test_author_resolution import setup as setup
from tests.test_bootstrap_gateway import CHECKED_CODE
from tests.test_operator_fixture import anchors as anchors
from tests.test_operator_fixture import context as context
from tests.test_operator_fixture import fixture as fixture
from tests.test_operator_fixture import operator as operator
from tests.test_operator_fixture import original as original
from tests.test_prepared_configuration import forbid_hooks
from tests.test_resolution_context import revalidate

ROOT = Path(__file__).parents[1]


class FakeAnvil:
    """One child process per node so identity and termination are real."""

    def __init__(self, tmp_path):
        self.tmp_path, self.processes = tmp_path, []

    def start(self, origin, instance, port=0):
        log = self.tmp_path / f"anvil-{uuid4().hex}.log"
        process = subprocess.Popen(
            [
                sys.executable,
                "-m",
                "tests.fake_anvil",
                "--port",
                str(port),
                "--log",
                str(log),
                "--instance",
                instance,
                "--block-number",
                str(origin.number),
                "--block-hash",
                origin.hash,
            ],
            cwd=ROOT,
            stdout=subprocess.PIPE,
            text=True,
            start_new_session=True,
        )
        assert process.stdout is not None
        port = int(process.stdout.readline())
        self.processes.append(process)
        return SimpleNamespace(process=process, port=port, log=log)

    def kill(self, process):
        os.kill(process.pid, signal.SIGKILL)
        process.wait(timeout=5)

    def close(self):
        for process in self.processes:
            if process.poll() is None:
                process.kill()
                process.wait(timeout=5)


def calls(log):
    return [json.loads(line) for line in log.read_text().splitlines()] if log.exists() else []


@pytest.fixture
def fake_nodes(tmp_path, monkeypatch):
    fake = FakeAnvil(tmp_path)
    started, instances, funded = [], {}, []

    class FakeForkSet(ForkSet):
        def __enter__(self):
            started.append(self)
            self.stack = ExitStack()
            for target in self.config.forks:
                node = Fork(self.env_file, target.block, target=target)
                node.cleanup = ExitStack()
                self.stack.callback(node.cleanup.close)
                instance = instances.setdefault(target.chain, "0x" + uuid4().hex * 2)
                anvil = fake.start(target.block, instance)
                node.process = anvil.process
                node.rpc = Rpc(f"http://127.0.0.1:{anvil.port}")
                node.metadata = node.rpc("anvil_metadata")
                node.start_proxy(provider_endpoint(target.provider, self.env_file))
                self.nodes[target.chain] = node
                if self.on_ready is not None:
                    self.on_ready(target.chain, node)
            return self

    def fund(node, owner, account, funding):
        funded.append((account, funding))
        return {"fixture_test_fund": account, "funding": funding.model_dump(mode="json")}

    monkeypatch.setattr(supervisor_forks, "ForkSet", FakeForkSet)
    monkeypatch.setattr("almanak.gateway.safe_funding.fund_asset", fund)
    try:
        yield SimpleNamespace(fake=fake, started=started, funded=funded)
    finally:
        fake.close()


def close_supervisor(supervisor):
    supervisor.lock.close()
    if supervisor.nodes is not None:
        supervisor.nodes.stack.close()


@pytest.fixture
def harness(tmp_path, operator, context, anchors, fake_nodes):
    operator = revalidate(operator, fork=eoa_inputs(operator.fork))
    context = revalidate(context, operator_digest=operator.digest)
    anchors = revalidate(anchors, context_digest=context.digest)
    plan = compile_fixture(context, operator, anchors)
    env_file = tmp_path / "operator.env"
    env_file.write_text("ALCHEMY_API_KEY=fixture-test-key\n")
    root = tmp_path / "run"
    for name in ("", "logs", "private", "private/deployment"):
        (root / name).mkdir(mode=0o700)
    (root / "control.token").write_text("a" * 64)
    workspace.save(
        root / "workspace.json",
        {
            "version": 1,
            "identity": "fork-test",
            "configuration": "prepared-fork-v1",
            "mode": "fork",
            "stage": "created",
            "credentials": {
                "gateway_config": str(tmp_path / "operator.json"),
                "env_file": str(env_file),
            },
            "processes": {},
        },
    )
    supervisor = Supervisor(root)
    case = SimpleNamespace(
        supervisor=supervisor,
        root=root,
        plan=plan,
        context=context,
        operator=operator,
        fake=fake_nodes.fake,
        started=fake_nodes.started,
        funded=fake_nodes.funded,
        directory=supervisor_forks.fixture_directory(supervisor),
    )
    try:
        yield case
    finally:
        close_supervisor(case.supervisor)


@pytest.fixture
def supervised(tmp_path, setup, fixture, fake_nodes):
    inputs = revalidate(
        eoa_inputs(fixture),
        anchors=(AnchorSelection(chain=setup.block.chain.chain_id, height=setup.block.number),),
    )
    setup.operator = revalidate(setup.operator, modes=(RunMode.FORK,), fork=inputs)
    document = read_inputs(gasless_reference(tmp_path))
    context = document.inputs.context(setup.operator, RunMode.FORK, inputs.anchors)
    definition = StrategyDefinition.read(REFERENCE / "strategy.py")
    operator_path = tmp_path / "operator.json"
    operator_path.write_text(setup.operator.model_dump_json())
    root = create_workspace(
        tmp_path / "run", document, definition, context, operator_path, setup.env_file
    )
    case = SimpleNamespace(
        supervisor=Supervisor(root),
        root=root,
        setup=setup,
        document=document,
        context=context,
        definition=definition,
        operator_path=operator_path,
        fake=fake_nodes.fake,
        started=fake_nodes.started,
        directory=root / "private/deployment/fixture",
    )
    try:
        yield case
    finally:
        for process in workspace.read(root)["processes"].values():
            stop(process)
        close_supervisor(case.supervisor)


def provision_plan(case, nodes):
    return provision_accounts(case.directory, case.plan, case.context, case.operator, nodes)


def replace(case):
    close_supervisor(case.supervisor)
    case.supervisor = Supervisor(case.root)
    return case.supervisor


def node_endpoint(supervisor):
    return supervisor.state["processes"]["fork-eip155:1"]["endpoint"]


def forbid_setup(monkeypatch):
    def forbidden(*_, **__):
        raise AssertionError("prepared fixtures must not repeat setup")

    monkeypatch.setattr(fixture_accounts, "prepare_accounts", forbidden)


def frozen_files(root):
    names = [
        "private/resolution.json",
        "private/fixture-plan.json",
        "private/parameters.json",
        "private/deployment/preparation.json",
        "private/deployment/deployment.json",
    ]
    files = {name: (root / name).read_bytes() for name in names}
    for path in (root / "private/deployment/fixture").iterdir():
        files["fixture/" + path.name] = path.read_bytes()
    return files


def interrupt_once(monkeypatch):
    original = fixture_accounts.prepare_accounts
    attempts = []

    def interrupted(node, config, chain):
        attempts.append(chain)
        if len(attempts) == 1:
            node.rpc("anvil_setBalance", ["0x" + "11" * 20, "0x1"])
            raise OSError("funding interrupted")
        return original(node, config, chain)

    monkeypatch.setattr(fixture_accounts, "prepare_accounts", interrupted)
    return attempts


def node_log(case, nodes):
    node = next(iter(nodes.nodes.values()))
    return next(
        Path(process.args[process.args.index("--log") + 1])
        for process in case.fake.processes
        if process.pid == node.process.pid
    )


def test_lost_fork_node_is_never_recreated(harness):
    nodes = attach_nodes(harness.supervisor, harness.plan)
    entry = harness.supervisor.state["processes"]["fork-eip155:1"]
    assert running(entry) and entry["snapshot"] == "0x1"
    node = next(iter(nodes.nodes.values()))
    harness.fake.kill(node.process)
    replacement = replace(harness)
    with pytest.raises(RuntimeError, match="original fork node is stopped"):
        attach_nodes(replacement, harness.plan)
    assert len(harness.started) == 1 and replacement.nodes is None
    assert replacement.state["processes"] == {"fork-eip155:1": entry}
    replacement.persist(processes={})
    harness.directory.mkdir()
    with pytest.raises(RuntimeError, match="never recreated"):
        attach_nodes(replacement, harness.plan)
    assert len(harness.started) == 1


def interrupt_funding(monkeypatch):
    def interrupted(node, config, chain):
        node.rpc("anvil_setBalance", ["0x" + "11" * 20, "0x1"])
        raise OSError("funding interrupted")

    monkeypatch.setattr(fixture_accounts, "prepare_accounts", interrupted)


def test_interrupted_funding_rolls_back_only_through_retained_snapshot(harness, monkeypatch):
    nodes = attach_nodes(harness.supervisor, harness.plan)
    log = node_log(harness, nodes)
    with monkeypatch.context() as scoped:
        interrupt_funding(scoped)
        with pytest.raises(OSError, match="funding interrupted"):
            provision_plan(harness, nodes)
    assert (harness.directory / "fixture-started.json").exists()
    assert not (harness.directory / "fixture-accounts.json").exists()
    replacement = replace(harness)
    nodes = attach_nodes(replacement, harness.plan)
    reconcile_fixture(replacement, nodes)
    reverts = [c for c in calls(log) if c[0] == "evm_revert"]
    assert reverts == [["evm_revert", ["0x1"]]]
    entry = replacement.state["processes"]["fork-eip155:1"]
    assert entry["snapshot"] == "0x2"
    assert not harness.directory.exists()
    archived = list((harness.root / "private").glob("interrupted-*"))
    assert len(archived) == 1 and (archived[0] / "fixture-started.json").exists()
    with monkeypatch.context() as scoped:
        interrupt_funding(scoped)
        with pytest.raises(OSError):
            provision_plan(harness, nodes)
    replacement.persist(
        processes={**replacement.state["processes"], "fork-eip155:1": {**entry, "snapshot": None}}
    )
    before = len(calls(log))
    with pytest.raises(RuntimeError, match="uncertain"):
        reconcile_fixture(replacement, nodes)
    assert calls(log)[before:] == [["anvil_metadata", []]]
    assert (harness.directory / "fixture-started.json").exists()
    assert len(list((harness.root / "private").glob("interrupted-*"))) == 1


def test_completed_fixture_is_never_rolled_back(harness, monkeypatch):
    nodes = attach_nodes(harness.supervisor, harness.plan)
    log = node_log(harness, nodes)
    record = provision_plan(harness, nodes)
    assert {a.name for a in record.accounts.bindings} == {f.account for f in harness.plan.funds}
    files = {p.name: p.read_bytes() for p in harness.directory.iterdir()}
    replacement = replace(harness)
    nodes = attach_nodes(replacement, harness.plan)
    before = len(calls(log))
    reconcile_fixture(replacement, nodes)

    def forbidden(*_, **__):
        raise AssertionError("account setup must not repeat")

    monkeypatch.setattr(fixture_accounts, "prepare_accounts", forbidden)
    assert provision_plan(harness, nodes) == record
    assert not any(c[0] in ("evm_revert", "anvil_setBalance") for c in calls(log)[before:])
    assert {p.name: p.read_bytes() for p in harness.directory.iterdir()} == files
    assert replacement.state["processes"]["fork-eip155:1"]["snapshot"] == "0x1"
    (harness.root / "state.sqlite").touch()
    with pytest.raises(ValueError, match="original preparation artifacts"):
        reconcile_fixture(replacement, nodes)
    (harness.root / "state.sqlite").unlink()
    (harness.directory / "fixture-accounts.json").unlink()
    (harness.root / "private/parameters.json").write_text("{}")
    before = len(calls(log))
    with pytest.raises(RuntimeError, match="no complete fixture"):
        reconcile_fixture(replacement, nodes)
    assert calls(log)[before:] == []
    assert harness.directory.exists()


def test_fixture_node_identity_is_verified_read_only(harness):
    nodes = attach_nodes(harness.supervisor, harness.plan)
    record = provision_plan(harness, nodes)
    routes = harness.directory / "fork-routes.json"
    (harness.directory / "fork-keys.json").unlink()
    require_fixture_nodes(record, routes)
    entries = json.loads(routes.read_text())
    changed = routes.with_name("changed-routes.json")
    changed.write_text(
        json.dumps([{**entries[0], "chain": {**entries[0]["chain"], "chain_id": "eip155:2"}}])
    )
    with pytest.raises(ValueError, match="differ from the retained fixture bindings"):
        require_fixture_nodes(record, changed)
    changed.write_text(json.dumps([{**entries[0], "extra": True}]))
    with pytest.raises(ValueError, match="only their explicit chain and endpoint"):
        require_fixture_nodes(record, changed)
    # A connector's venue beside the node joins the route (`fork_venues`).
    venue = {"hypercore": {"model": "http://127.0.0.1:9", "agent_keys": "/keys.json"}}
    changed.write_text(json.dumps([{**entries[0], "venues": venue}]))
    require_fixture_nodes(record, changed)
    node = next(iter(nodes.nodes.values()))
    port = int(node.rpc.endpoint.rsplit(":", 1)[1])
    harness.fake.kill(node.process)
    with pytest.raises(DataError) as unavailable:
        require_fixture_nodes(record, routes)
    assert unavailable.value.kind == "fork_unavailable"
    harness.fake.start(node.origin, "0x" + "b" * 64, port=port)
    with pytest.raises(DataError) as mismatch:
        require_fixture_nodes(record, routes)
    assert mismatch.value.kind == "fork_mismatch"


def test_fork_workspace_persists_identity_and_inputs_before_processes(supervised):
    root, state = supervised.root, workspace.read(supervised.root)
    assert state["configuration"] == "prepared-fork-v1" and state["mode"] == "fork"
    assert state["stage"] == "created" and state["processes"] == {}
    assert state["credentials"] == {
        "gateway_config": str(supervised.operator_path),
        "env_file": str(supervised.setup.env_file),
    }
    assert "key_file" not in json.dumps(state)
    assert json.loads(
        (root / "private/read-context.json").read_text()
    ) == supervised.context.model_dump(mode="json")
    for name in ("private/fixture-plan.json", "private/parameters.json", "state.sqlite"):
        assert not (root / name).exists()
    assert not supervised.directory.exists()
    for path in root.rglob("*"):
        if path.name != "supervisor.lock":
            assert path.stat().st_mode & 0o777 == (0o700 if path.is_dir() else 0o600)


@pytest.mark.parametrize("fault", ["fork", "production", "mode"])
def test_workspace_refuses_operator_without_its_mode_settings(supervised, tmp_path, fault):
    if fault == "mode":
        state = workspace.read(supervised.root)
        workspace.save(supervised.root / "workspace.json", {**state, "mode": "production"})
        with pytest.raises(ValueError, match="workspace mode differs"):
            supervisor_preparation.operator_for(supervised.root)
        return
    mode = RunMode.FORK if fault == "fork" else RunMode.PRODUCTION
    operator = revalidate(supervised.setup.operator, modes=(mode,), fork=None)
    context = supervised.document.inputs.context(operator, mode, supervised.context.anchors)
    expected = "operator fixture inputs" if fault == "fork" else "signing configuration"
    path = tmp_path / "changed-operator.json"
    path.write_text(operator.model_dump_json())
    with pytest.raises(ValueError, match=expected):
        create_workspace(
            tmp_path / "refused",
            supervised.document,
            supervised.definition,
            context,
            path,
            supervised.setup.env_file,
        )
    assert not (tmp_path / "refused").exists()


def test_supervisor_retains_nodes_before_funding_freezes_hooks_once_and_publishes_last(
    supervised, monkeypatch
):
    supervisor, root, events = supervised.supervisor, supervised.root, []
    validations, publications = [], []
    original_prepare = fixture_accounts.prepare_accounts
    original_worker = checks.strategy_worker
    original_publish = supervisor_preparation.publish

    def observed_prepare(node, config, chain):
        state = workspace.read(root)
        entry = state["processes"]["fork-eip155:1"]
        assert running(entry) and entry["snapshot"] == "0x1" and state["stage"] == "provisioning"
        assert (supervised.directory / "fixture-started.json").exists()
        assert not (root / "private/parameters.json").exists()
        return original_prepare(node, config, chain)

    def observed_worker(request):
        if isinstance(request, ValidationRequest):
            validations.append(request)
        return original_worker(request)

    def observed_publish(path, value):
        publications.append(path.name)
        return original_publish(path, value)

    monkeypatch.setattr(fixture_accounts, "prepare_accounts", observed_prepare)
    monkeypatch.setattr(checks, "strategy_worker", observed_worker)
    monkeypatch.setattr(supervisor_preparation, "publish", observed_publish)
    monkeypatch.setattr(supervisor, "start_gateway", lambda: events.append("gateway"))
    monkeypatch.setattr(
        supervisor,
        "start_coordinator",
        lambda *, create, admitted: events.append(("coordinator", create, admitted)),
    )
    supervisor.setup()
    record = retained(root)
    assert events == ["gateway", ("coordinator", True, True)]
    assert len(validations) == 1
    assert validations[0].config == record.preparation.prepared.parameters
    assert record.fixture.accounts == record.preparation.prepared.parameters.accounts
    assert record.preparation.policy is None
    assert record.configuration().fork_bindings == record.fixture.bindings
    assert publications.index("preparation.json") < publications.index("deployment.json")
    assert (root / "private/fixture-plan.json").exists()
    assert not running(supervisor.state["processes"]["read-gateway"])
    assert supervisor.state["preparation_digest"] == record.digest
    preflight = json.loads((root / "preflight.json").read_text())
    assert preflight["ready"] and preflight["execution_environment"] == "identified_anvil"
    assert supervisor.state["stage"] == "provisioned"


def test_complete_fixture_reuses_without_funding_hooks_rollback_or_archive_reads(
    supervised, monkeypatch
):
    provision(supervised.supervisor)
    root = supervised.root
    frozen = frozen_files(root)
    endpoint = node_endpoint(supervised.supervisor)
    before_calls = len(supervised.setup.rpc.calls)
    log = next(p for p in supervised.fake.processes).args
    log = Path(log[log.index("--log") + 1])
    before_node = len(calls(log))
    forbid_hooks(monkeypatch)
    forbid_setup(monkeypatch)
    replacement = replace(supervised)
    provision(replacement)
    assert frozen_files(root) == frozen
    assert len(supervised.setup.rpc.calls) == before_calls
    assert len(supervised.started) == 1 and node_endpoint(replacement) == endpoint
    # Reuse repeats only node identity and the fork compatibility code reads.
    reused = calls(log)[before_node:]
    assert {c[0] for c in reused} == {"anvil_metadata", "eth_getCode"}
    assert {c[1][0] for c in reused if c[0] == "eth_getCode"} == set(CHECKED_CODE)
    assert "read-gateway" not in replacement.state["processes"] or not running(
        replacement.state["processes"]["read-gateway"]
    )
    arguments = replacement.gateway_arguments(1234)
    fixture = root / "private/deployment/fixture"
    assert "--fork-routes" in arguments and "--fork-key-file" in arguments
    assert arguments[arguments.index("--fork-routes") + 1] == str(fixture / "fork-routes.json")
    assert arguments[arguments.index("--fork-key-file") + 1] == str(fixture / "fork-keys.json")
    assert "--preparation-file" in arguments and "--database" in arguments
    for forbidden in ("--env-file", "--production", "--key-file", "--config", "--read-context"):
        assert forbidden not in arguments
    read_only = supervisor_preparation.gateway_arguments(replacement, 1235, read_only=True)
    assert "--fork" in read_only and "--read-context" in read_only
    assert "--production" not in read_only and "--fork-key-file" not in read_only


def test_interrupted_funding_recovers_once_through_the_original_node(supervised, monkeypatch):
    supervisor, root = supervised.supervisor, supervised.root
    validations = []
    original_worker = checks.strategy_worker

    def observed_worker(request):
        if isinstance(request, ValidationRequest):
            validations.append(request)
        return original_worker(request)

    monkeypatch.setattr(checks, "strategy_worker", observed_worker)
    attempts = interrupt_once(monkeypatch)
    with pytest.raises(OSError, match="funding interrupted"):
        provision(supervisor)
    assert supervisor.state["stage"] == "provisioning"
    assert (supervised.directory / "fixture-started.json").exists()
    assert not (root / "private/parameters.json").exists() and validations == []
    replacement = replace(supervised)
    provision(replacement)
    log = next(p for p in supervised.fake.processes).args
    log = Path(log[log.index("--log") + 1])
    assert [c for c in calls(log) if c[0] == "evm_revert"] == [["evm_revert", ["0x1"]]]
    assert replacement.state["processes"]["fork-eip155:1"]["snapshot"] == "0x2"
    archived = list((root / "private").glob("interrupted-*"))
    assert len(archived) == 1 and (archived[0] / "fixture-started.json").exists()
    assert len(attempts) == 2 and len(validations) == 1 and len(supervised.started) == 1
    assert (
        retained(root).fixture.accounts == retained(root).preparation.prepared.parameters.accounts
    )


def test_execution_database_never_triggers_fresh_fixture_or_rollback(supervised, monkeypatch):
    (supervised.root / "state.sqlite").touch()
    with monkeypatch.context() as scoped:
        forbid_hooks(scoped)
        with pytest.raises(ValueError, match="original preparation artifacts"):
            provision(supervised.supervisor)
    assert not supervised.supervisor.state["processes"] and supervised.started == []
    (supervised.root / "state.sqlite").unlink()
    provision(supervised.supervisor)
    (supervised.root / "state.sqlite").touch()
    forbid_hooks(monkeypatch)
    forbid_setup(monkeypatch)
    replacement = replace(supervised)
    provision(replacement)
    assert replacement.state["processes"]["fork-eip155:1"]["snapshot"] == "0x1"
    assert not list((supervised.root / "private").glob("interrupted-*"))


@pytest.mark.parametrize(
    "fault", ["operator", "node", "keys", "accounts", "progress", "config", "lost"]
)
def test_changed_operator_node_or_artifact_refuses_reuse(supervised, monkeypatch, fault):
    provision(supervised.supervisor)
    root = supervised.root
    before_calls = len(supervised.setup.rpc.calls)
    forbid_hooks(monkeypatch)
    forbid_setup(monkeypatch)
    entry = supervised.supervisor.state["processes"]["fork-eip155:1"]
    if fault == "operator":
        value = json.loads(supervised.operator_path.read_text())
        value["fork"]["funds"][0]["native_balance"]["amount"] += 1
        supervised.operator_path.write_text(json.dumps(value))
    elif fault in ("node", "lost"):
        node = next(iter(supervised.supervisor.nodes.nodes.values()))
        port = int(entry["endpoint"].rsplit(":", 1)[1])
        supervised.fake.kill(node.process)
        if fault == "node":
            supervised.fake.start(node.origin, "0x" + "c" * 64, port=port)
    else:
        path = (
            root
            / {
                "keys": "private/deployment/fixture/fork-keys.json",
                "accounts": "private/deployment/fixture/fixture-accounts.json",
                "progress": "private/parameters.json",
                "config": "private/deployment/deployment.json",
            }[fault]
        )
        value = json.loads(path.read_text())
        if fault == "keys":
            value[next(iter(value))] = "0x" + "22" * 32
        elif fault == "accounts":
            value["installation_digest"] = "0" * 64
        elif fault == "progress":
            value["prepared"]["strategy"]["source"] += "\n# changed\n"
        else:
            value["allocations"][0]["capital"] += 1
        path.write_text(json.dumps(value))
    replacement = replace(supervised)
    with pytest.raises((ValueError, RuntimeError)):
        provision(replacement)
    assert len(supervised.setup.rpc.calls) == before_calls
    assert len(supervised.started) == 1
    assert not list((root / "private").glob("interrupted-*"))


def test_failed_preparation_publication_retains_fixture_and_hooks_for_retry(
    supervised, monkeypatch
):
    original = supervisor_preparation.publish

    def failed(path, value):
        if path.name == "preparation.json":
            raise OSError("publication failed")
        return original(path, value)

    with monkeypatch.context() as scoped:
        scoped.setattr(supervisor_preparation, "publish", failed)
        with pytest.raises(OSError, match="publication failed"):
            provision(supervised.supervisor)
    first = read_preparation(supervised.root / "private/parameters.json")
    assert first.policy is None
    assert (supervised.directory / "fixture-accounts.json").exists()
    assert not (supervised.root / "private/deployment/deployment.json").exists()
    assert not running(supervised.supervisor.state["processes"]["read-gateway"])
    forbid_hooks(monkeypatch)
    forbid_setup(monkeypatch)
    provision(supervised.supervisor)
    assert retained(supervised.root).preparation == first


def test_failed_read_endpoint_publication_stops_the_started_process(supervised, monkeypatch):
    original_spawn = supervisor_preparation.spawn
    original_persist = supervised.supervisor.persist
    processes = []

    def spawned(*args, **kwargs):
        process = original_spawn(*args, **kwargs)
        processes.append(process)
        return process

    def failed(**changes):
        if "read_gateway_url" in changes:
            raise OSError("endpoint publication failed")
        return original_persist(**changes)

    monkeypatch.setattr(supervisor_preparation, "spawn", spawned)
    monkeypatch.setattr(supervised.supervisor, "persist", failed)
    with pytest.raises(OSError, match="endpoint publication failed"):
        provision(supervised.supervisor)
    assert len(processes) == 1 and not running(processes[0])
    assert not supervised.supervisor.state["processes"] and supervised.started == []


@pytest.mark.parametrize("fault", ["killed", "replaced"])
def test_fork_admission_requires_live_identified_nodes(supervised, monkeypatch, fault):
    supervisor = supervised.supervisor
    provision(supervisor)
    assert (
        supervisor.admission()
        and json.loads((supervised.root / "preflight.json").read_text())["ready"]
    )
    node = next(iter(supervisor.nodes.nodes.values()))
    port = int(node.rpc.endpoint.rsplit(":", 1)[1])
    supervised.fake.kill(node.process)
    if fault == "replaced":
        supervised.fake.start(node.origin, "0x" + "d" * 64, port=port)
    monkeypatch.setattr(supervisor, "start_gateway", lambda: pytest.fail("gateway started"))
    assert not supervisor.admission()
    preflight = json.loads((supervised.root / "preflight.json").read_text())
    expected = "fork_unavailable" if fault == "killed" else "fork_mismatch"
    assert not preflight["ready"] and preflight["reason"].startswith(expected)
    assert supervisor.state["stage"] == "action_required"
    with pytest.raises(DataError):
        supervisor.verify_forks()
