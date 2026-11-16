"""A real read gateway prepares the actual Aave reference for supervised admission."""

import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from almanak import checks, production_setup, supervisor_preparation, workspace
from almanak.caip import ChainId
from almanak.configuration.admission import PreparedDeployment
from almanak.configuration.anchors import AnchorSelection
from almanak.configuration.prepared import read_preparation
from almanak.connectors.support import RunMode
from almanak.gateway.operator_config import read_operator_config
from almanak.strategy import StrategyDefinition, ValidationRequest
from almanak.supervisor import Supervisor
from almanak.supervisor_preparation import create_workspace, provision, retained
from almanak.supervisor_process import running, stop
from tests.test_prepared_configuration import archive as archive
from tests.test_prepared_configuration import case as case
from tests.test_prepared_configuration import forbid_hooks
from tests.test_prepared_configuration import rpc as rpc
from tests.test_prepared_configuration import setup as setup


@pytest.fixture
def supervised(case, tmp_path, request):
    """An indirect parameter replaces the operator's production signing section."""
    operator_path = tmp_path / "operator.json"
    context = case.progress.context
    signing = getattr(request, "param", None)
    if signing is None:
        operator_path.write_text(case.setup.operator.model_dump_json())
    else:
        raw = case.setup.operator.model_dump(mode="json")
        raw["production"]["signing"] = signing
        operator_path.write_text(json.dumps(raw))
        context = case.document.inputs.context(
            read_operator_config(operator_path), context.mode, context.anchors
        )
    root = create_workspace(
        tmp_path / "run",
        case.document,
        case.definition,
        context,
        operator_path,
        case.setup.env_file,
    )
    supervisor = Supervisor(root)
    try:
        yield SimpleNamespace(
            case=case, root=root, supervisor=supervisor, operator_path=operator_path
        )
    finally:
        for process in workspace.read(root)["processes"].values():
            stop(process)
        supervisor.lock.close()


def test_provisional_workspace_preserves_exact_source_and_context_before_processes(supervised):
    root, case = supervised.root, supervised.case
    state = workspace.read(root)
    assert state["configuration"] == "prepared-production-v1" and state["stage"] == "created"
    assert state["processes"] == {}
    assert state["credentials"] == {
        "gateway_config": str(supervised.operator_path),
        "env_file": str(case.setup.env_file),
    }
    assert StrategyDefinition.read(root / "strategy.py") == case.definition
    assert json.loads(
        (root / "private/read-context.json").read_text()
    ) == case.progress.context.model_dump(mode="json")
    assert json.loads(
        (root / "private/author-inputs.json").read_text()
    ) == case.document.inputs.model_dump(mode="json")
    assert not (root / "private/parameters.json").exists()
    assert not (root / "private/deployment/deployment.json").exists()
    assert not (root / "state.sqlite").exists()
    for path in root.rglob("*"):
        if path.name != "supervisor.lock":
            assert path.stat().st_mode & 0o777 == (0o700 if path.is_dir() else 0o600)


def test_supervisor_resolves_then_checks_hooks_once_and_waits_for_current_admission(
    supervised, monkeypatch
):
    supervisor, root = supervised.supervisor, supervised.root
    calls, validations = [], []
    original = checks.strategy_worker

    def observed(request):
        if isinstance(request, ValidationRequest):
            validations.append(request)
        return original(request)

    monkeypatch.setattr(checks, "strategy_worker", observed)
    monkeypatch.setattr(
        production_setup,
        "preflight_config",
        lambda config, _: calls.append(config) or {"funded": False},
    )
    supervisor.setup()
    record = retained(root)
    assert len(validations) == 1 and validations[0].config == record.preparation.prepared.parameters
    assert calls == [record.configuration()]
    assert record.preparation.prepared.strategy == supervised.case.definition
    assert len(record.preparation.prepared.capabilities.requirements) == 2
    assert record.preparation.policy.execution.gas == (supervised.case.policy,)
    assert supervisor.state["stage"] == "action_required"
    assert not running(supervisor.state["processes"]["read-gateway"])
    assert (
        "gateway" not in supervisor.state["processes"]
        and "coordinator" not in supervisor.state["processes"]
    )
    assert not (root / "state.sqlite").exists()
    frozen = {
        name: (root / name).read_bytes()
        for name in (
            "private/resolution.json",
            "private/parameters.json",
            "private/deployment/preparation.json",
            "private/deployment/deployment.json",
        )
    }
    before_calls = len(supervised.case.setup.rpc.calls)
    forbid_hooks(monkeypatch)
    supervisor.lock.close()
    replacement = Supervisor(root)
    try:
        replacement.setup()
        assert replacement.state["stage"] == "action_required"
        assert retained(root) == record
    finally:
        replacement.lock.close()
    assert {name: (root / name).read_bytes() for name in frozen} == frozen
    assert len(supervised.case.setup.rpc.calls) == before_calls
    assert calls == [record.configuration(), record.configuration()]


def test_successful_admission_passes_frozen_bundle_to_existing_gateway_and_coordinator(
    supervised, monkeypatch
):
    supervisor, events = supervised.supervisor, []

    def admit(config, env, source, output, *, prepared):
        record = retained(supervised.root)
        assert config == record.configuration()
        assert prepared == record.preparation.prepared
        assert StrategyDefinition.read(source) == prepared.strategy
        events.append("admission")
        return {"ready": True}

    def start_gateway():
        arguments = supervisor.gateway_arguments(1234)
        assert "--gateway-config" in arguments and "--preparation-file" in arguments
        assert "--key-file" not in arguments and "--config" not in arguments
        # Authority is verified from Roles storage; no history checkpoint is passed.
        assert "--roles-history" not in arguments
        events.append("gateway")

    monkeypatch.setattr(production_setup, "admit", admit)
    monkeypatch.setattr(supervisor, "start_gateway", start_gateway)
    monkeypatch.setattr(
        supervisor,
        "start_coordinator",
        lambda *, create, admitted: events.append(("coordinator", create, admitted)),
    )
    supervisor.setup()
    assert events == ["admission", "gateway", ("coordinator", True, True)]
    assert supervisor.state["preparation_digest"] == retained(supervised.root).digest


def test_failed_final_publication_reuses_validated_hooks_on_supervisor_retry(
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
    assert first.policy is not None
    assert not (supervised.root / "private/deployment/deployment.json").exists()
    assert not running(supervised.supervisor.state["processes"]["read-gateway"])
    forbid_hooks(monkeypatch)
    provision(supervised.supervisor)
    assert retained(supervised.root).preparation == first


@pytest.mark.parametrize("fault", ["source", "author", "context", "operator", "progress", "config"])
def test_resume_refuses_changed_workspace_inputs_without_hooks_or_provider_access(
    supervised, monkeypatch, fault
):
    provision(supervised.supervisor)
    before_calls = len(supervised.case.setup.rpc.calls)
    forbid_hooks(monkeypatch)
    root = supervised.root
    if fault == "source":
        source = root / "strategy.py"
        source.write_text(source.read_text() + "\n# changed\n")
    elif fault == "operator":
        value = json.loads(supervised.operator_path.read_text())
        value["production"]["networks"][0]["max_observation_age_seconds"] += 1
        supervised.operator_path.write_text(json.dumps(value))
    else:
        path = (
            root
            / {
                "author": "private/author-inputs.json",
                "context": "private/read-context.json",
                "progress": "private/parameters.json",
                "config": "private/deployment/deployment.json",
            }[fault]
        )
        value = json.loads(path.read_text())
        if fault == "author":
            value["strategy"]["allocations"]["main_weth"]["capital"] = "0.002"
        elif fault == "context":
            value["identity"] = "a" * 32
        elif fault == "progress":
            value["prepared"]["strategy"]["source"] += "\n# changed\n"
        else:
            value["allocations"][0]["capital"] += 1
        path.write_text(json.dumps(value))
    with pytest.raises(ValueError):
        provision(supervised.supervisor)
    assert len(supervised.case.setup.rpc.calls) == before_calls


def test_existing_database_never_triggers_fresh_preparation_when_artifact_is_missing(
    supervised, monkeypatch
):
    (supervised.root / "state.sqlite").touch()
    forbid_hooks(monkeypatch)
    with pytest.raises(ValueError, match="original preparation artifacts"):
        provision(supervised.supervisor)
    assert not supervised.supervisor.state["processes"]


def test_changed_operator_refuses_reuse_of_a_running_gateway(supervised, monkeypatch):
    provision(supervised.supervisor)
    operator = json.loads(supervised.operator_path.read_text())
    operator["production"]["payers"][0]["max_gas_limit"] -= 1
    supervised.operator_path.write_text(json.dumps(operator))
    monkeypatch.setattr("almanak.supervisor.running", lambda _: True)

    def forbidden(*_, **__):
        raise AssertionError("operator mismatch must precede execution gateway access")

    monkeypatch.setattr(supervised.supervisor, "check_gateway", forbidden)
    with pytest.raises(ValueError):
        supervised.supervisor.start_gateway()


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
    assert not supervised.supervisor.state["processes"]


def test_operator_password_reference_is_used_without_copying_signing_config(
    supervised, monkeypatch
):
    def forbidden(*_, **__):
        raise AssertionError("operator password file should avoid an interactive prompt")

    monkeypatch.setattr("getpass.getpass", forbidden)
    assert workspace.credential_password(supervised.root) is None
    arguments = supervised.supervisor.gateway_arguments(1234)
    supervised.supervisor.require_password(arguments)
    assert "--password-stdin" not in arguments and "--password-file" not in arguments
    assert "key_file" not in workspace.read(supervised.root)["credentials"]


@pytest.mark.parametrize(
    "supervised", [{"signer_file": "/private/remote-signer.json"}], indirect=True
)
def test_remote_signer_never_prompts_for_a_keystore_password(supervised, monkeypatch):
    def forbidden(*_, **__):
        raise AssertionError("a remote signer takes no keystore password")

    monkeypatch.setattr("getpass.getpass", forbidden)
    assert workspace.credential_password(supervised.root) is None
    arguments = supervised.supervisor.gateway_arguments(1234)
    supervised.supervisor.require_password(arguments)
    assert "--password-stdin" not in arguments


@pytest.mark.parametrize(
    "supervised", [{"key_file": "/private/never-opened-keystore"}], indirect=True
)
def test_keystore_without_password_file_prompts_and_forwards_the_password(supervised, monkeypatch):
    monkeypatch.setattr("getpass.getpass", lambda *_: "typed")
    assert workspace.credential_password(supervised.root) == "typed"
    arguments = supervised.supervisor.gateway_arguments(1234)
    with pytest.raises(ValueError, match="interactive keystore password"):
        supervised.supervisor.require_password(arguments)
    supervised.supervisor.password = "typed"
    supervised.supervisor.require_password(arguments)
    assert arguments[-1] == "--password-stdin"


def test_author_readable_signing_file_refuses_workspace_before_any_hook(case, tmp_path):
    raw = case.setup.operator.model_dump(mode="json")
    raw["production"]["signing"]["key_file"] = str(Path(checks.__file__).resolve())
    path = tmp_path / "unsafe-operator.json"
    path.write_text(json.dumps(raw))
    operator = read_operator_config(path)
    context = case.document.inputs.context(
        operator, case.progress.context.mode, case.progress.context.anchors
    )
    with pytest.raises(ValueError, match="readable"):
        create_workspace(
            tmp_path / "refused", case.document, case.definition, context, path, case.setup.env_file
        )
    assert not (tmp_path / "refused").exists()


def test_frozen_deployment_codec_rejects_partial_preparation(supervised):
    from almanak.configuration.prepared import PreparationProgress, build_prepared

    prepared = build_prepared(
        supervised.case.document, supervised.case.definition, supervised.case.progress
    )
    with pytest.raises(ValueError, match="complete production"):
        PreparedDeployment(
            resolution=supervised.case.progress,
            preparation=PreparationProgress(prepared=prepared, prepared_digest=prepared.digest),
        )


def test_selected_anchors_cover_several_chains_in_a_stable_order():
    inputs = SimpleNamespace(
        requirements=[SimpleNamespace(chain=ChainId(c)) for c in ("eip155:43114", "eip155:1")]
    )
    fresh = supervisor_preparation.selected_anchors(None, RunMode.PRODUCTION, inputs)
    assert [str(anchor.chain) for anchor in fresh] == ["eip155:1", "eip155:43114"]
    assert all(anchor.fresh for anchor in fresh)
    anchors = tuple(
        AnchorSelection(chain=ChainId(c), height=1) for c in ("eip155:43114", "eip155:1")
    )
    operator = SimpleNamespace(fork=SimpleNamespace(anchors=anchors))
    assert supervisor_preparation.selected_anchors(operator, RunMode.FORK, inputs) == anchors
    partial = SimpleNamespace(fork=SimpleNamespace(anchors=anchors[:1]))
    with pytest.raises(ValueError, match="exactly the authored account chains"):
        supervisor_preparation.selected_anchors(partial, RunMode.FORK, inputs)
