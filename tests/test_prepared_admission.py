"""Prepared hooks and policy survive durable deployment creation without signing."""

import json
import sqlite3
import threading
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace

import pytest

from almanak.capabilities import CapabilityCodec
from almanak.configuration.admission import PreparedDeployment
from almanak.connectors.aave_v4.models import LendingConfig
from almanak.connectors.support import RunMode
from almanak.gateway.client import GatewayClient
from almanak.gateway.ethereum import DataError
from almanak.gateway.service import GatewayService, http_server
from almanak.runtime import Runtime
from almanak.state import Journal
from tests.test_bootstrap_gateway import connected
from tests.test_prepared_configuration import archive as archive
from tests.test_prepared_configuration import case as case
from tests.test_prepared_configuration import forbid_hooks, run
from tests.test_prepared_configuration import rpc as rpc
from tests.test_prepared_configuration import setup as setup
from tests.test_resolution_context import revalidate


@pytest.fixture
def deployment(case, monkeypatch, tmp_path):
    with connected(case.setup, mode=RunMode.PRODUCTION) as client:
        preparation = run(case, client)
    retained = PreparedDeployment(resolution=case.progress, preparation=preparation)
    forbid_hooks(monkeypatch)

    def forbidden(*_, **__):
        raise AssertionError("frozen admission cannot repeat hooks, sign or broadcast")

    monkeypatch.setattr("almanak.runtime.strategy_capabilities", forbidden)
    monkeypatch.setattr("almanak.runtime.validate_strategy_config", forbidden)
    monkeypatch.setattr(CapabilityCodec, "evaluate", forbidden)
    monkeypatch.setattr("almanak.state.Path.home", lambda: tmp_path)
    return SimpleNamespace(
        retained=retained,
        config=retained.configuration(),
        case=case,
        database=tmp_path / "state.sqlite",
        backend=SimpleNamespace(
            sign=forbidden, broadcast=forbidden, audit_history=lambda _: None, close=lambda: None
        ),
    )


def service(deployment, **changes):
    arguments = {
        "preparation": deployment.retained,
        "operator": deployment.case.setup.operator,
    }
    arguments.update(changes)
    return GatewayService(
        deployment.config, deployment.database, deployment.backend, lease_seconds=30, **arguments
    )


@contextmanager
def connection(deployment):
    current = service(deployment)
    server = http_server(current, deployment.case.setup.token_file, 0)
    thread = threading.Thread(target=server.serve_forever)
    thread.start()
    client = GatewayClient(
        f"http://127.0.0.1:{server.server_port}", deployment.case.setup.token_file
    )
    try:
        yield client, current
    finally:
        client.close()
        server.shutdown()
        thread.join(timeout=5)
        server.server_close()


def test_creation_commits_frozen_hooks_policy_and_resolution_in_existing_journal(deployment):
    before_calls = len(deployment.case.setup.rpc.calls)
    with connection(deployment) as (client, current):
        client.create(deployment.config, deployment.case.definition)
        first = client.load()
        retained = current.journal.configuration_preparation()
    assert retained == deployment.retained
    assert first.config.preparation_digest == retained.digest
    assert first.capabilities == retained.preparation.prepared.capabilities
    assert first.strategy == retained.preparation.prepared.strategy
    assert first.config.gas == first.config.production.gas == (deployment.case.policy,)
    assert first.pending is None and not first.initialized
    assert retained.preparation.authority == "unadmitted"
    with sqlite3.connect(deployment.database) as db:
        body = db.execute("SELECT body FROM configuration_preparation WHERE id=1").fetchone()[0]
    for secret in ("never-opened-keystore", "never-opened-password", "hidden-input-secret"):
        assert secret not in body
    with connection(deployment) as (client, current):
        assert client.load() == first
        assert current.journal.configuration_preparation() == retained
    assert len(deployment.case.setup.rpc.calls) == before_calls


def test_actual_runtime_creates_and_resumes_without_repeating_validation(deployment):
    with connection(deployment) as (client, _):
        runtime = Runtime(
            client.endpoint,
            deployment.case.setup.token_file,
            create=deployment.config,
            strategy=deployment.case.definition,
        )
        try:
            first = runtime.journal.load()
            assert first.capabilities == deployment.retained.preparation.prepared.capabilities
        finally:
            runtime.close()
    with connection(deployment) as (client, _):
        runtime = Runtime(client.endpoint, deployment.case.setup.token_file)
        try:
            assert runtime.journal.load() == first
        finally:
            runtime.close()


@pytest.mark.parametrize("fault", ["source", "filename", "capabilities"])
def test_creation_request_cannot_replace_frozen_authority(deployment, fault):
    definition = deployment.case.definition
    fields = {"strategy": definition.model_dump(mode="json")}
    if fault == "source":
        fields["strategy"]["source"] += "\n# changed\n"
    elif fault == "filename":
        fields["strategy"]["filename"] = "other.py"
    else:
        fields["capabilities"] = {"requirements": []}
    with connection(deployment) as (client, _), pytest.raises(DataError):
        client.request("create", **fields)
    assert not deployment.database.exists()


@pytest.mark.parametrize("missing", ["operator", "preparation", "both"])
def test_gateway_requires_both_frozen_preparation_and_independent_operator(deployment, missing):
    changes = {name: None for name in ("operator", "preparation") if missing in (name, "both")}
    with pytest.raises(ValueError, match="preparation|operator"):
        service(deployment, **changes)
    assert not deployment.database.exists()


@pytest.mark.parametrize("fault", ["policy", "accounts", "provider", "mode"])
def test_gateway_independently_rechecks_current_operator_before_creation(deployment, fault):
    operator = deployment.case.setup.operator
    if fault == "policy":
        authority = operator.production
        ceiling = revalidate(
            authority.payers[0], max_gas_limit=deployment.case.policy.max_gas_limit - 1
        )
        operator = revalidate(operator, production=revalidate(authority, payers=(ceiling,)))
    elif fault == "accounts":
        profile = operator.accounts.bindings[0].profile
        fund = revalidate(profile.fund, address="0x" + "12" * 20)
        binding = revalidate(operator.accounts.bindings[0], profile=revalidate(profile, fund=fund))
        operator = revalidate(operator, accounts=revalidate(operator.accounts, bindings=(binding,)))
    elif fault == "provider":
        network = revalidate(
            operator.networks[0], provider={"kind": "rpc_url", "url_env": "OTHER_RPC"}
        )
        operator = revalidate(operator, networks=(network,))
    else:
        # Read-only codecs cannot acquire production authority by replaying a policy reply.
        operator = revalidate(operator, modes=(RunMode.FORK,), production=None)
    with pytest.raises(ValueError):
        service(deployment, operator=operator)
    assert not deployment.database.exists()


@pytest.mark.parametrize(
    "fault", ["missing_table", "empty_table", "changed_record", "missing_marker"]
)
def test_gateway_replacement_refuses_lost_or_replaced_preparation(deployment, fault):
    with connection(deployment) as (client, _):
        client.create(deployment.config, deployment.case.definition)
    with sqlite3.connect(deployment.database) as db:
        if fault == "missing_table":
            db.execute("DROP TABLE configuration_preparation")
        elif fault == "empty_table":
            db.execute("DELETE FROM configuration_preparation")
        elif fault == "changed_record":
            body = json.loads(
                db.execute("SELECT body FROM configuration_preparation").fetchone()[0]
            )
            body["preparation"]["prepared"]["strategy"]["source"] += "\n# changed\n"
            db.execute("UPDATE configuration_preparation SET body=?", (json.dumps(body),))
        else:
            deployment.config = revalidate(deployment.config, preparation_digest=None)
    changes = {"preparation": None, "operator": None} if fault == "missing_marker" else {}
    with pytest.raises(ValueError):
        service(deployment, **changes)


def test_journal_refuses_missing_preparation_before_creating_any_file(deployment):
    with pytest.raises(ValueError, match="retained preparation"):
        Journal(deployment.database).create(deployment.config, deployment.case.definition)
    assert not deployment.database.exists()


@pytest.mark.parametrize("field", ["capital", "account", "precision", "gas", "execution", "marker"])
def test_prepared_deployment_cannot_relabel_parameters_or_execution(deployment, field):
    raw = deployment.config.model_dump(mode="json")
    if field == "capital":
        raw["allocations"][0]["capital"] += 1
    elif field == "account":
        raw["accounts"]["bindings"][0]["profile"]["fund"]["address"] = "0x" + "12" * 20
    elif field == "precision":
        next(iter(raw["asset_metadata"].values()))["decimals"] -= 1
    elif field == "gas":
        raw["gas"][0]["max_gas_price_wei"] -= 1
        raw["production"]["gas"][0]["max_gas_price_wei"] -= 1
    elif field == "execution":
        raw["production"]["networks"][0]["max_observation_age_seconds"] += 1
    else:
        raw["preparation_digest"] = "b" * 64
    config = LendingConfig.model_validate_json(json.dumps(raw))
    with pytest.raises(ValueError, match="differs"):
        deployment.retained.require_configuration(config, deployment.case.definition)


def test_old_configuration_wire_does_not_gain_a_preparation_marker():
    from almanak.connectors.composition import configuration_from_json

    fixture = Path(__file__).with_name("fixtures") / "aave-v4-interval-lifecycle.json"
    from tests.lending_captures import current_config

    # The capture predates scope-only authority; read its wire in today's interface.
    original = current_config(json.loads(fixture.read_text())["config"])
    loaded = configuration_from_json(json.dumps(original))
    assert loaded.preparation_digest is None
    # The existing codec expands this older supply-only capture's omitted debt list.
    assert loaded.model_dump(mode="json") == {**original, "debts": []}
