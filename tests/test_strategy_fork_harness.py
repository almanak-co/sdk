import json
import sys
from contextlib import ExitStack
from pathlib import Path
from types import SimpleNamespace

import pytest

from almanak.caip import ChainId
from almanak.connectors.aave_v4.fork import ForkConfig
from almanak.gateway.fork_config import ForkTarget
from tests.fork.support import processes
from tests.strategy_fork import harness
from tests.strategy_fork import supervised as public_supervised

ROOT = Path(__file__).parents[1]


def test_public_supervision_exercises_scenario_before_teardown(monkeypatch, tmp_path):
    events: list[object] = []
    assertions = SimpleNamespace(
        entered=lambda state: True,
        assert_holding=lambda node, state: None,
        assert_closed=lambda node, state: events.append("closed"),
        exercise_hold=lambda node, journal, process: events.append(("exercise", process.poll())),
        align_teardown=lambda node, journal, process: events.append(("align", node)),
    )
    follower = SimpleNamespace(wait=lambda timeout: 0)
    record: dict[str, list] = {"phases": []}
    monkeypatch.setattr(harness, "load_assertions", lambda case: assertions)
    monkeypatch.setattr(public_supervised, "await_workspace", lambda root, process: None)
    monkeypatch.setattr(public_supervised, "await_stage", lambda root, stages: None)
    monkeypatch.setattr(
        public_supervised,
        "hold",
        lambda root, assertions, record, phase: (
            events.append("hold"),
            record["phases"].append(phase),
        ),
    )
    monkeypatch.setattr(public_supervised, "fork_handle", lambda root: "fork")

    def checkpoint(root, predicate):
        events.append("checkpoint")
        return SimpleNamespace(mode="closed")

    monkeypatch.setattr(public_supervised, "await_checkpoint", checkpoint)
    monkeypatch.setattr(
        public_supervised,
        "read_workspace",
        lambda root: {"processes": {"coordinator": {"pid": 123}}},
    )
    monkeypatch.setattr(public_supervised, "running", lambda process: True)
    monkeypatch.setattr(
        harness,
        "drive_external",
        lambda assertions, node, journal, process: events.append(
            ("external", node, process.poll())
        ),
    )

    def control(root, action, record):
        events.append(action)

    monkeypatch.setattr(public_supervised, "control_command", control)
    public_supervised.supervise(SimpleNamespace(), tmp_path, record, follower)

    assert events == [
        "checkpoint",
        ("external", "fork", None),
        "hold",
        "pause",
        "resume",
        "hold",
        ("exercise", None),
        ("align", "fork"),
        "teardown",
        "checkpoint",
        ("external", "fork", None),
        "checkpoint",
        "closed",
        "shutdown",
    ]
    assert record["phases"] == [
        "supervised_resolution_fixture_hooks_and_admission",
        "entered_and_held",
        "paused_and_resumed",
        "timed_strategy_exit_verified",
        "fork_clock_aligned_for_teardown",
        "closed",
        "workspace_shutdown",
    ]


def test_public_supervision_retains_exercise_evidence_without_credentials(tmp_path):
    root = tmp_path / "run"
    artifact = tmp_path / "evidence"
    root.mkdir()
    artifact.mkdir()
    retained = (
        "lp-burn-minima.json",
        "external-execution.json",
        "permission-budget-observations.json",
    )
    for name in retained:
        (root / name).write_text('{"proof": true}\n')
    (root / "fork-keys.json").write_text('{"secret": true}\n')
    (root / "operator.env").write_text("RPC_KEY=secret\n")
    private = root / "private"
    private.mkdir()
    (private / "credential-password").write_text("secret\n")

    public_supervised.retain_workspace(root, artifact)

    assert {path.name for path in artifact.iterdir()} == set(retained)


def make_case(tmp_path, name="example"):
    folder = tmp_path / "strategies" / name
    folder.mkdir(parents=True)
    (folder / "strategy.py").write_text("def decide(context): return None\n")
    (folder / "config.yaml").write_text("{}")
    scenario = tmp_path / "scenarios" / name
    scenario.mkdir(parents=True)
    (scenario / "assertions.py").write_text(
        "def entered(state): return True\n"
        "def assert_holding(node, state): pass\n"
        "def assert_closed(node, state): pass\n"
    )
    return folder, scenario


def test_discovery_includes_nested_strategies_without_importing_them(tmp_path):
    make_case(tmp_path, "nested/b")
    source, _ = make_case(tmp_path, "a")
    (source / "strategy.py").write_text('raise RuntimeError("must stay sandboxed")\n')
    cases = harness.discover(tmp_path / "strategies", tmp_path / "scenarios")
    assert [case.name for case in cases] == ["a", "nested/b"]
    assert harness.load_assertions(cases[0]).entered(None)


@pytest.mark.parametrize("missing", ["config", "assertions", "empty", "hook"])
def test_discovery_and_contract_fail_visibly(tmp_path, missing):
    folder, scenario = make_case(tmp_path)
    paths = {
        "config": folder / "config.yaml",
        "assertions": scenario / "assertions.py",
        "empty": folder / "strategy.py",
    }
    if missing == "hook":
        (scenario / "assertions.py").write_text("entered = None\n")
    else:
        paths[missing].unlink()
    with pytest.raises(ValueError):
        cases = harness.discover(tmp_path / "strategies", tmp_path / "scenarios")
        harness.load_assertions(cases[0])


def test_resolution_cache_keeps_chain_and_provider_scope(monkeypatch):
    config = ForkConfig.model_validate_json(
        (ROOT / "tests/fixtures/author/aave_v4_weth_supply/config.json").read_text()
    )
    block = ForkConfig.model_validate_json(
        (ROOT / "tests/fixtures/aave-v4-interval-config.json").read_text()
    ).origin
    calls = []

    def resolve(target, _env):
        calls.append(target.provider)
        return block

    monkeypatch.setattr(ForkTarget, "resolve", resolve)
    cache = {}
    assert harness.resolve_config(config, Path("unused"), cache).origin == block
    assert harness.resolve_config(config, Path("unused"), cache).origin == block
    assert len(calls) == 1
    raw = config.model_dump(mode="json")
    raw["forks"][0]["provider"]["api_key_env"] = "OTHER_KEY"
    other = ForkConfig.model_validate_json(json.dumps(raw))
    harness.resolve_config(other, Path("unused"), cache)
    assert len(calls) == 2


def test_target_collection_retains_identity_and_cleans_up(monkeypatch):
    config = ForkConfig.model_validate_json(
        (ROOT / "tests/fixtures/aave-v4-interval-config.json").read_text()
    )
    events = []

    class Node:
        metadata = {"instanceId": "0x" + "12" * 32}
        rpc = SimpleNamespace(endpoint="http://127.0.0.1:1234")

        def __init__(self, *_args, **_kwargs):
            pass

        def __enter__(self):
            events.append("start")
            return self

        def __exit__(self, *_):
            events.append("stop")

    monkeypatch.setattr(harness, "Fork", Node)
    with pytest.raises(RuntimeError, match="gateway failed"), ExitStack() as stack:
        bindings = harness.start_targets(config, Path("unused"), stack)
        binding = bindings[ChainId("eip155:1")]
        assert binding.origin == config.origin
        assert binding.execution.chain_id == binding.origin.chain.chain_id
        assert binding.execution.environment == "anvil:" + binding.instance
        assert binding.endpoint == Node.rpc.endpoint
        raise RuntimeError("gateway failed")
    assert events == ["start", "stop"]


def test_process_cleanup_after_readiness_failure(tmp_path):
    with processes(tmp_path) as launch:
        process = launch([sys.executable, "-c", "import time; time.sleep(30)"], ready=False)
        with pytest.raises(AssertionError):
            launch([sys.executable, "-c", "print('failed startup')"])
    assert process.poll() is not None
    assert "failed startup" in (tmp_path / "process-1.log").read_text()


@pytest.mark.parametrize("changed,exit_code", [(True, None), (False, 1)])
def test_hold_refuses_changed_state_or_dead_runtime(monkeypatch, recorded, changed, exit_code):
    clock = iter([0, 0])
    monkeypatch.setattr(harness.time, "monotonic", lambda: next(clock))
    altered = recorded.model_copy(update={"mode": "running"}) if changed else recorded
    journal = SimpleNamespace(load=lambda: altered)
    runtime = SimpleNamespace(poll=lambda: exit_code)
    with pytest.raises(AssertionError):
        harness.verify_hold(None, None, journal, runtime, recorded)


def test_hold_checks_on_chain_after_scheduled_evaluations(monkeypatch, recorded):
    monkeypatch.setattr(harness.time, "monotonic", lambda: 0)
    monkeypatch.setattr(harness.time, "sleep", lambda _: None)
    calls = []
    assertions = SimpleNamespace(assert_holding=lambda *args: calls.append(args))
    first = next_scheduled_state(recorded)
    second = next_scheduled_state(first)
    states = iter([recorded, first, second])
    journal = SimpleNamespace(load=lambda: next(states))
    harness.verify_hold("node", assertions, journal, SimpleNamespace(poll=lambda: None), recorded)
    assert calls == [("node", second)]


def next_scheduled_state(state):
    schedule = state.schedule
    return state.model_copy(
        update={
            "schedule": schedule.after(
                state.config.decision_trigger, schedule.event(schedule.next_due_at)
            )
        }
    )


def test_hold_rejects_a_runtime_that_never_evaluates(monkeypatch, recorded):
    clock = iter([0, 0, 1000])
    monkeypatch.setattr(harness.time, "monotonic", lambda: next(clock))
    monkeypatch.setattr(harness.time, "sleep", lambda _: None)
    journal = SimpleNamespace(load=lambda: recorded)
    with pytest.raises(AssertionError, match="two scheduled evaluations"):
        harness.verify_hold(None, None, journal, SimpleNamespace(poll=lambda: None), recorded)


@pytest.mark.parametrize("invalid_source", [False, True])
def test_failure_retains_evidence_without_copying_credentials(
    tmp_path, monkeypatch, invalid_source
):
    make_case(tmp_path)
    case = harness.discover(tmp_path / "strategies", tmp_path / "scenarios")[0]
    temporary = tmp_path / "temporary"
    temporary.mkdir()
    (temporary / "process-0.log").write_text("runtime failed\n")
    workspaces = []

    def fail(case, env_file, cache, workspace, record):
        workspaces.append(workspace)
        (workspace / "process-0.log").write_text("runtime failed\n")
        (workspace / "deployment").mkdir()
        (workspace / "deployment/fork-keys.json").write_text("secret-key")
        (workspace / "deployment/gateway-auth.token").write_text("secret-token")
        raise RuntimeError("unavailable")

    monkeypatch.setattr(harness, "execute_case", fail)
    if invalid_source:
        case.strategy.write_text("def broken(\n")
    expected = SyntaxError if invalid_source else RuntimeError
    for _ in range(2):
        with pytest.raises(expected):
            harness.run_case(case, Path("unused"), {}, temporary, tmp_path / "artifacts")
    for artifact in (tmp_path / "artifacts").iterdir():
        assert json.loads((artifact / "result.json").read_text())["status"] == "failed"
        files = {"result.json"} if invalid_source else {"result.json", "process-0.log"}
        assert {path.name for path in artifact.iterdir()} == files
    if not invalid_source:
        assert workspaces[0] != workspaces[1] and temporary not in workspaces


def test_database_backup_and_corrupt_evidence(tmp_path, recorded):
    from almanak.state import Journal

    database = tmp_path / "state.sqlite"
    journal = Journal(database)
    journal.create(recorded.config, recorded.strategy)
    artifact = tmp_path / "evidence"
    artifact.mkdir()
    record = {}
    harness.retain_database(database, artifact, record)
    assert Journal(artifact / "state.sqlite").load() == journal.load()
    assert (artifact / "report.json").is_file()
    corrupt = tmp_path / "corrupt.sqlite"
    corrupt.write_bytes(b"broken database")
    harness.retain_database(corrupt, artifact, record)
    assert "evidence_error" in record


@pytest.mark.parametrize("primary_failure", [False, True])
@pytest.mark.parametrize("dashboard_error", [AttributeError, ValueError])
def test_retention_failure_preserves_primary_and_execution_evidence(
    tmp_path, recorded, monkeypatch, primary_failure, dashboard_error
):
    from almanak.state import Journal

    temporary = tmp_path / "temporary"
    deployment = temporary / "deployment"
    deployment.mkdir(parents=True)
    artifact = tmp_path / "evidence"
    artifact.mkdir()
    journal = Journal(deployment / "state.sqlite")
    journal.create(recorded.config, recorded.strategy)
    (temporary / "process-0.log").write_text("retained process output\n")
    for name in ("deployment.json", "fork-routes.json"):
        (deployment / name).write_text("{}\n")
    for name in ("fork-keys.json", "gateway-auth.token"):
        (deployment / name).write_text("private credential")

    def fail_dashboard(*args):
        raise dashboard_error("dashboard projection failed")

    monkeypatch.setattr(harness, "retain_dashboard", fail_dashboard)
    record = {"status": "passed"}
    expected = AssertionError if primary_failure else RuntimeError
    with pytest.raises(expected) as raised:
        try:
            if primary_failure:
                raise AssertionError("original lifecycle failure")
        finally:
            harness.retain(temporary, artifact, record)
    if primary_failure:
        assert str(raised.value) == "original lifecycle failure"
    retained = json.loads((artifact / "result.json").read_text())
    assert retained["status"] == "failed" and retained["evidence_error"]
    assert Journal(artifact / "state.sqlite").load() == journal.load()
    expected_files = {
        "process-0.log",
        "deployment.json",
        "fork-routes.json",
        "state.sqlite",
        "checkpoint.json",
        "report.json",
        "result.json",
    }
    assert {path.name for path in artifact.iterdir()} == expected_files
    assert json.loads((artifact / "checkpoint.json").read_text()) == (
        journal.load().model_dump(mode="json")
    )


def test_runtime_stop_requires_expected_exit():
    sent = []
    process = SimpleNamespace(send_signal=sent.append, wait=lambda **_: 130)
    harness.stop_runtime(process)
    assert len(sent) == 1
    process.wait = lambda **_: 1
    with pytest.raises(AssertionError):
        harness.stop_runtime(process)


def test_gateway_replacement_reuses_authority_arguments():
    calls = []
    process = SimpleNamespace(send_signal=lambda _: None, wait=lambda **_: 0)
    node = SimpleNamespace(
        gateway_process=process, gateway_arguments=["gateway", "same-config", "same-database"]
    )

    def launch(arguments, **kwargs):
        calls.append((arguments, kwargs))
        return "replacement"

    harness.restart_gateway(node, launch)
    assert calls == [(node.gateway_arguments, {"ready": "gateway_ready"})]
    assert node.gateway_process == "replacement"


def test_interruption_identity_uses_retained_chain_slots():
    from types import SimpleNamespace

    from tests.strategy_fork.harness import interrupted_identity

    pending = SimpleNamespace(operation=SimpleNamespace(kind="swap"), tx_hash="0x" + "12" * 32)
    state = SimpleNamespace(
        work=(SimpleNamespace(pending=pending), SimpleNamespace(pending=None)), pending=None
    )
    assert interrupted_identity(state, "swap", "broadcast") == pending.tx_hash
    state.work = (SimpleNamespace(pending=pending), SimpleNamespace(pending=pending))
    with pytest.raises(AssertionError):
        interrupted_identity(state, "swap", "broadcast")
    state.work = (SimpleNamespace(pending=None), SimpleNamespace(pending=None))
    state.outcomes, state.delivered = (pending,), 0
    assert interrupted_identity(state, "swap", "commit") == pending.tx_hash
    state.delivered = 1
    with pytest.raises(AssertionError):
        interrupted_identity(state, "swap", "commit")


def fork_entry(chain, port):
    return {
        "endpoint": f"http://127.0.0.1:{port}",
        "origin": {
            "chain": {"chain_id": chain, "environment": "mainnet"},
            "number": 50974851,
            "hash": "0x" + "d7" * 32,
            "timestamp": 1788739049,
        },
        "metadata": {"instanceId": chain.replace(":", "-")},
        "snapshot": "0x0",
    }


@pytest.mark.parametrize("chains", (("eip155:8453",), ("eip155:1", "eip155:43114")))
def test_supervised_fork_handle_follows_the_workspace_fork_nodes(tmp_path, chains):
    from almanak.models import Chain
    from tests.strategy_fork.supervised import fork_handle

    processes = {"read-gateway": {}, "coordinator": {}}
    processes.update({f"fork-{c}": fork_entry(c, 1000 + i) for i, c in enumerate(chains)})
    (tmp_path / "workspace.json").write_text(json.dumps({"processes": processes}))
    handle = fork_handle(tmp_path)
    if len(chains) == 1:
        assert (handle.origin.number, str(handle.origin.chain.chain_id)) == (50974851, chains[0])
        assert handle.rpc.endpoint == "http://127.0.0.1:1000"
        forked = {"forkBlockNumber": 50974851, "forkBlockHash": "0x" + "d7" * 32}
        metadata = {"instanceId": "eip155-8453", "chainId": 31337, "forkedNetwork": forked}
        handle.rpc = AnvilMetadata(metadata)
        handle._validate_origin()
        handle.rpc = AnvilMetadata({**metadata, "instanceId": "another"})
        with pytest.raises(ValueError, match="Anvil origin differs"):
            handle._validate_origin()
        return
    for index, chain in enumerate(chains):
        origin = Chain(chain_id=ChainId(chain), environment="mainnet")
        execution = Chain(chain_id=ChainId(chain), environment="anvil:" + chain.replace(":", "-"))
        for scope in (origin, execution):
            assert handle.for_chain(scope).rpc.endpoint == f"http://127.0.0.1:{1000 + index}"
    with pytest.raises(StopIteration):
        handle.for_chain(Chain(chain_id=ChainId("eip155:10"), environment="mainnet"))
    (tmp_path / "workspace.json").write_text(json.dumps({"processes": {"coordinator": {}}}))
    with pytest.raises(AssertionError, match="no fork node"):
        fork_handle(tmp_path)


class AnvilMetadata:
    """A node whose only answer is its metadata; the handle never sends transactions."""

    signing_chain = ChainId("eip155:31337")

    def __init__(self, metadata):
        self.metadata = metadata

    def __call__(self, method, params=()):
        assert method == "anvil_metadata"
        return self.metadata


def test_discovery_skips_a_reference_whose_connector_has_no_fork_mode(tmp_path, monkeypatch):
    from almanak.connectors import composition

    folder, scenario = make_case(tmp_path, "perp")
    (folder / "config.yaml").write_text(
        "markets:\n  btc:\n    connector: offline\n    chain: eip155:999\n    market: BTC\n"
    )
    (scenario / "assertions.py").unlink()
    wire = composition.connector_from_wire
    offline = SimpleNamespace(supported_chains=lambda mode: ())
    monkeypatch.setattr(
        composition,
        "connector_from_wire",
        lambda config: offline if config["connector"] == "offline" else wire(config),
    )
    make_case(tmp_path, "a")
    cases = harness.discover(tmp_path / "strategies", tmp_path / "scenarios")
    assert [case.name for case in cases] == ["a"]
    assert not harness.backtest_only(tmp_path / "strategies" / "a" / "config.yaml")
    assert not harness.backtest_only(tmp_path / "strategies" / "missing.yaml")


def test_discovery_leaves_out_a_reference_whose_fork_scenario_is_pending(tmp_path, monkeypatch):
    _, scenario = make_case(tmp_path, "perp")
    (scenario / "assertions.py").unlink()
    make_case(tmp_path, "a")
    with pytest.raises(ValueError, match="required file is missing"):
        harness.discover(tmp_path / "strategies", tmp_path / "scenarios")
    monkeypatch.setattr(harness, "PENDING_SCENARIOS", frozenset({"perp"}))
    cases = harness.discover(tmp_path / "strategies", tmp_path / "scenarios")
    assert [case.name for case in cases] == ["a"]
