"""Prepared fork gateway launch verifies identity and the journal before opening keys."""

import json
import sqlite3
from pathlib import Path
from types import SimpleNamespace

import pytest

from almanak.capabilities import CapabilityCodec
from almanak.cli import run_gateway
from almanak.cli_arguments import build_parser
from almanak.configuration.admission import PreparedDeployment
from almanak.gateway import prepared_cli
from almanak.gateway.ethereum import DataError
from almanak.state import Journal
from tests.test_fork_preparation import case as case
from tests.test_fork_preparation import fixture_case as fixture_case
from tests.test_fork_preparation import run as prepare
from tests.test_operator_fixture import anchors as anchors
from tests.test_operator_fixture import context as context
from tests.test_operator_fixture import fixture as fixture
from tests.test_operator_fixture import operator as operator
from tests.test_operator_fixture import original as original
from tests.test_prepared_configuration import archive as archive
from tests.test_prepared_configuration import forbid_hooks
from tests.test_prepared_configuration import rpc as rpc
from tests.test_prepared_configuration import setup as setup
from tests.test_supervised_fork import FakeAnvil


@pytest.fixture
def deployment(case, monkeypatch, tmp_path):
    preparation = prepare(case)
    retained = PreparedDeployment(
        resolution=case.progress, preparation=preparation, fixture=case.fixture
    )
    forbid_hooks(monkeypatch)

    def forbidden(*_, **__):
        raise AssertionError("frozen admission cannot repeat hooks, sign or broadcast")

    monkeypatch.setattr("almanak.runtime.strategy_capabilities", forbidden)
    monkeypatch.setattr("almanak.runtime.validate_strategy_config", forbidden)
    monkeypatch.setattr(CapabilityCodec, "evaluate", forbidden)
    monkeypatch.setattr("almanak.state.Path.home", lambda: tmp_path)
    fake = FakeAnvil(tmp_path)
    binding = retained.fixture.bindings[0]
    anvil = fake.start(binding.origin, binding.instance)
    routes = tmp_path / "fork-routes.json"
    routes.write_text(
        json.dumps(
            [
                {
                    "chain": binding.chain.model_dump(mode="json"),
                    "endpoint": f"http://127.0.0.1:{anvil.port}",
                }
            ]
        )
    )
    try:
        yield SimpleNamespace(
            retained=retained,
            config=retained.configuration(),
            case=case,
            database=tmp_path / "state.sqlite",
            routes=routes,
            keys=case.directory / "fork-keys.json",
            fake=fake,
            anvil=anvil,
            binding=binding,
            backend=SimpleNamespace(
                sign=forbidden,
                broadcast=forbidden,
                audit_history=lambda _: None,
                close=lambda: None,
            ),
        )
    finally:
        fake.close()


@pytest.fixture
def launch(deployment, tmp_path):
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
            "--fork-routes",
            str(deployment.routes),
            "--fork-key-file",
            str(deployment.keys),
            "--auth-token-file",
            str(deployment.case.setup.token_file),
            "--port",
            "1234",
        ]
    )


def no_backend(monkeypatch):
    def forbidden(*_, **__):
        raise AssertionError("invalid preparation must fail before provider, node or key access")

    monkeypatch.setattr(prepared_cli, "fork_routes", forbidden)
    monkeypatch.setattr(prepared_cli, "production_routes", forbidden)
    monkeypatch.setattr(prepared_cli, "read_password", forbidden)


def test_fork_launch_verifies_journal_and_identity_before_opening_keys(
    deployment, launch, monkeypatch
):
    seen = []
    original_identity = prepared_cli.require_fixture_nodes
    original_preparation = prepared_cli.require_preparation

    def journal(*args, **kwargs):
        seen.append("journal")
        return original_preparation(*args, **kwargs)

    def identity(fixture, routes):
        assert fixture == deployment.retained.fixture and routes == deployment.routes
        seen.append("identity")
        return original_identity(fixture, routes)

    def backend(config, routes, keys):
        assert config == deployment.config
        assert routes == deployment.routes and keys == deployment.keys
        seen.append("backend")
        return deployment.backend

    def serve(service, token, port):
        assert service.preparation == deployment.retained
        assert token == deployment.case.setup.token_file and port == 1234
        if deployment.case.kind == "eoa":
            # Safe creation generates grants from live Roles state; the actual fork proof covers it.
            service.dispatch("create", service.request_type(strategy=deployment.case.definition))
        seen.append("serve")

    monkeypatch.setattr(prepared_cli, "require_preparation", journal)
    monkeypatch.setattr(prepared_cli, "require_fixture_nodes", identity)
    monkeypatch.setattr(prepared_cli, "fork_routes", backend)
    monkeypatch.setattr(prepared_cli, "production_routes", no_backend)
    monkeypatch.setattr(prepared_cli, "serve", serve)
    assert run_gateway(launch) == 0
    assert seen == ["journal", "identity", "backend", "serve"]
    if deployment.case.kind == "eoa":
        journal = Journal(deployment.database)
        assert journal.configuration_preparation() == deployment.retained
        assert journal.load().config.fork_bindings == (deployment.binding,)


@pytest.mark.parametrize(
    "field",
    [
        "config",
        "read_context",
        "accounts",
        "key_file",
        "password_file",
        "password_stdin",
        "env_file",
        "fork_url",
        "fork",
        "backtest",
    ],
)
def test_author_production_or_single_node_inputs_refuse_before_any_backend(
    launch, monkeypatch, field
):
    no_backend(monkeypatch)
    value = True if field in ("fork", "backtest", "password_stdin") else Path("/private/override")
    setattr(launch, field, value)
    with pytest.raises(ValueError, match="generated fixture routes"):
        run_gateway(launch)


@pytest.mark.parametrize("field", ["fork_routes", "fork_key_file"])
def test_missing_generated_transport_refuses_before_any_backend(launch, monkeypatch, field):
    no_backend(monkeypatch)
    setattr(launch, field, None)
    with pytest.raises(ValueError, match="generated fixture routes"):
        run_gateway(launch)


def test_missing_database_refuses_before_any_backend(launch, monkeypatch):
    no_backend(monkeypatch)
    launch.database = None
    with pytest.raises(ValueError, match="requires operator config"):
        run_gateway(launch)


def test_production_flag_refuses_a_fork_preparation_before_any_backend(
    deployment, launch, monkeypatch
):
    no_backend(monkeypatch)
    launch.production, launch.fork_routes, launch.fork_key_file = True, None, None
    launch.env_file = deployment.case.setup.env_file
    with pytest.raises(ValueError, match="production preparation"):
        run_gateway(launch)


def test_author_readable_fixture_key_refuses_before_any_backend(deployment, launch, monkeypatch):
    no_backend(monkeypatch)
    monkeypatch.setattr(prepared_cli, "readable_paths", lambda: (deployment.keys.parent,))
    with pytest.raises(ValueError, match="readable"):
        run_gateway(launch)


@pytest.mark.parametrize("fault", ["replaced", "killed", "routes"])
def test_changed_or_lost_node_refuses_before_any_backend(deployment, launch, monkeypatch, fault):
    no_backend(monkeypatch)
    if fault == "routes":
        entries = json.loads(deployment.routes.read_text())
        entries[0]["chain"]["chain_id"] = "eip155:2"
        deployment.routes.write_text(json.dumps(entries))
        with pytest.raises(ValueError, match="retained fixture bindings"):
            run_gateway(launch)
        return
    deployment.fake.kill(deployment.anvil.process)
    if fault == "replaced":
        deployment.fake.start(
            deployment.binding.origin, "0x" + "e" * 64, port=deployment.anvil.port
        )
    with pytest.raises(DataError) as refused:
        run_gateway(launch)
    assert refused.value.kind == ("fork_mismatch" if fault == "replaced" else "fork_unavailable")


@pytest.mark.parametrize("fault", ["missing_preparation", "changed_capabilities", "changed_config"])
def test_durable_mismatch_refuses_before_opening_keys(deployment, launch, monkeypatch, fault):
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


def test_changed_operator_fixture_refuses_before_any_backend(launch, monkeypatch):
    no_backend(monkeypatch)
    raw = json.loads(launch.gateway_config.read_text())
    raw["fork"]["funds"][0]["assets"][0]["amount"] += 1
    launch.gateway_config.write_text(json.dumps(raw))
    with pytest.raises(ValueError):
        run_gateway(launch)
