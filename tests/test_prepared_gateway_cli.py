"""Prepared gateway launch checks immutable authority before opening any signer."""

import json
import sqlite3
from pathlib import Path
from types import SimpleNamespace

import pytest

from almanak.cli import run_gateway
from almanak.cli_arguments import build_parser
from almanak.gateway import prepared_cli
from almanak.gateway.operator_execution import SigningFiles
from almanak.state import Journal
from tests.test_bootstrap_gateway import connected
from tests.test_prepared_admission import archive as archive
from tests.test_prepared_admission import case as case
from tests.test_prepared_admission import deployment as deployment
from tests.test_prepared_admission import rpc as rpc
from tests.test_prepared_admission import setup as setup
from tests.test_prepared_configuration import forbid_hooks, resolve, run
from tests.test_resolution_context import revalidate


def launch_for(deployment, tmp_path):
    path, operator = tmp_path / "preparation.json", tmp_path / "operator.json"
    path.write_text(deployment.retained.model_dump_json())
    operator.write_text(deployment.case.setup.operator.model_dump_json())
    return build_parser().parse_args(
        [
            "service",
            "gateway",
            "--gateway-config",
            str(operator),
            "--preparation-file",
            str(path),
            "--database",
            str(deployment.database),
            "--production",
            "--env-file",
            str(deployment.case.setup.env_file),
            "--auth-token-file",
            str(deployment.case.setup.token_file),
            "--port",
            "1234",
        ]
    )


@pytest.fixture
def launch(deployment, tmp_path):
    return launch_for(deployment, tmp_path)


def no_backend(monkeypatch):
    def forbidden(*_, **__):
        raise AssertionError("invalid preparation must fail before provider or signer access")

    monkeypatch.setattr(prepared_cli, "production_routes", forbidden)
    monkeypatch.setattr(prepared_cli, "read_password", forbidden)


def test_cli_launch_derives_config_and_signing_only_from_frozen_inputs(
    deployment, launch, monkeypatch
):
    seen = []

    def backend(config, env, key, password_file, *, password, signer_file=None):
        assert config == deployment.config and env == deployment.case.setup.env_file
        assert key == Path("/private/never-opened-keystore")
        assert password_file == Path("/private/never-opened-password") and password is None
        assert signer_file is None
        seen.append("backend")
        return deployment.backend

    def serve(service, token, port):
        assert service.preparation == deployment.retained
        assert token == deployment.case.setup.token_file and port == 1234
        service.dispatch("create", service.request_type(strategy=deployment.case.definition))
        seen.append("serve")

    monkeypatch.setattr(prepared_cli, "production_routes", backend)
    monkeypatch.setattr(prepared_cli, "serve", serve)
    assert run_gateway(launch) == 0
    assert seen == ["backend", "serve"]
    assert Journal(deployment.database).configuration_preparation() == deployment.retained


def test_cli_launch_uses_remote_signer_from_frozen_operator_context(case, monkeypatch, tmp_path):
    authority = revalidate(
        case.setup.operator.production,
        signing=SigningFiles(signer_file=Path("/private/remote-signer.json")),
    )
    case.setup.operator = revalidate(case.setup.operator, production=authority)
    case.progress = resolve(case.document, case.setup, case.progress.context.mode)
    with connected(case.setup, mode=case.progress.context.mode) as client:
        preparation = run(case, client)
    retained = prepared_cli.PreparedDeployment(resolution=case.progress, preparation=preparation)
    forbid_hooks(monkeypatch)
    deployment = SimpleNamespace(
        retained=retained,
        config=retained.configuration(),
        case=case,
        database=tmp_path / "remote-state.sqlite",
        backend=SimpleNamespace(close=lambda: None),
    )
    launch = launch_for(deployment, tmp_path)
    seen = []

    def backend(config, env, key, password_file, *, password, signer_file=None):
        assert config == deployment.config and env == deployment.case.setup.env_file
        assert key is password_file is password is None
        assert signer_file == Path("/private/remote-signer.json")
        seen.append("backend")
        return deployment.backend

    def serve(service, token, port):
        assert service.preparation == retained
        assert token == deployment.case.setup.token_file and port == 1234
        seen.append("serve")

    monkeypatch.setattr(prepared_cli, "production_routes", backend)
    monkeypatch.setattr(prepared_cli, "serve", serve)
    assert run_gateway(launch) == 0
    assert seen == ["backend", "serve"]


@pytest.mark.parametrize(
    "field",
    [
        "accounts",
        "key_file",
        "password_file",
        "fork_key_file",
        "fork_url",
        "fork_routes",
        "fork",
        "backtest",
        "read_context",
    ],
)
def test_author_or_transport_overrides_refuse_before_any_backend(launch, monkeypatch, field):
    no_backend(monkeypatch)
    setattr(launch, field, True if field in ("fork", "backtest") else Path("/private/override"))
    with pytest.raises(ValueError, match="operator signing settings"):
        run_gateway(launch)


@pytest.mark.parametrize("field", ["database", "env_file"])
def test_missing_required_context_refuses_before_any_backend(launch, monkeypatch, field):
    no_backend(monkeypatch)
    setattr(launch, field, None)
    with pytest.raises(ValueError, match="requires operator config"):
        run_gateway(launch)


def test_operator_policy_replacement_refuses_before_any_backend(launch, monkeypatch):
    no_backend(monkeypatch)
    raw = json.loads(launch.gateway_config.read_text())
    raw["production"]["payers"][0]["max_gas_limit"] -= 1
    launch.gateway_config.write_text(json.dumps(raw))
    with pytest.raises(ValueError):
        run_gateway(launch)


def test_password_source_conflict_refuses_before_reading_stdin(launch, monkeypatch):
    no_backend(monkeypatch)
    launch.password_stdin = True
    with pytest.raises(ValueError, match="cannot both be selected"):
        run_gateway(launch)


def test_author_readable_operator_key_refuses_before_backend(launch, monkeypatch):
    no_backend(monkeypatch)
    monkeypatch.setattr(prepared_cli, "readable_paths", lambda: (Path("/private"),))
    with pytest.raises(ValueError, match="readable"):
        run_gateway(launch)


@pytest.mark.parametrize("fault", ["missing_preparation", "changed_capabilities", "changed_config"])
def test_durable_mismatch_refuses_before_unlocking_signers(deployment, launch, monkeypatch, fault):
    journal = Journal(deployment.database)
    journal.create(deployment.config, deployment.case.definition, preparation=deployment.retained)
    with sqlite3.connect(deployment.database) as db:
        if fault == "missing_preparation":
            db.execute("DROP TABLE configuration_preparation")
        else:
            value = json.loads(db.execute("SELECT body FROM checkpoint").fetchone()[0])
            if fault == "changed_capabilities":
                value["capabilities"]["requirements"] = []
            else:
                value["config"]["allocations"][0]["capital"] += 1
            db.execute("UPDATE checkpoint SET body=?", (json.dumps(value),))
    no_backend(monkeypatch)
    with pytest.raises(ValueError):
        run_gateway(launch)


def test_complete_config_gateway_cannot_ignore_a_preparation_argument(
    deployment, launch, monkeypatch
):
    no_backend(monkeypatch)
    launch.config = launch.gateway_config
    launch.gateway_config = None
    with pytest.raises(ValueError, match="execution gateway requires"):
        run_gateway(launch)


def test_prepared_config_cannot_use_the_complete_config_signer_path(
    deployment, launch, monkeypatch
):
    no_backend(monkeypatch)
    launch.config = launch.gateway_config.with_name("deployment.json")
    launch.config.write_text(deployment.config.model_dump_json())
    launch.gateway_config = launch.preparation_file = None
    with pytest.raises(ValueError, match="operator gateway config and preparation file"):
        run_gateway(launch)


def test_invalid_lease_refuses_before_any_backend(launch, monkeypatch):
    no_backend(monkeypatch)
    launch.lease_seconds = 1
    with pytest.raises(ValueError, match="lease policy"):
        run_gateway(launch)


def test_cli_refuses_ambiguous_read_and_prepared_contexts(launch):
    with pytest.raises(SystemExit):
        build_parser().parse_args(
            [
                "service",
                "gateway",
                "--gateway-config",
                str(launch.gateway_config),
                "--read-context",
                "context.json",
                "--preparation-file",
                str(launch.preparation_file),
                "--production",
                "--auth-token-file",
                str(launch.auth_token_file),
                "--port",
                "1234",
            ]
        )
