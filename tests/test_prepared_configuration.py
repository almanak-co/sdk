"""Authenticated gas checks retain the actual reference's authoritative hook bundle."""

import fcntl
import json
from types import SimpleNamespace

import pytest
from pydantic import ValidationError

from almanak import checks
from almanak.configuration.prepared import (
    PreparationProgress,
    prepare_production,
    read_preparation,
)
from almanak.configuration.yaml import load
from almanak.connectors.support import RunMode
from almanak.gateway.bootstrap import BootstrapService
from almanak.gateway.ethereum import DataError
from almanak.gateway.operator_execution import ProductionAuthority
from almanak.gateway.production_config import GasPolicy
from almanak.models import StrategyAccounts
from almanak.strategy import StrategyDefinition, ValidationRequest
from tests.test_author_resolution import (
    REFERENCE,
    frozen_production_gas,
    parsed,
    resolve,
)
from tests.test_author_resolution import archive as archive
from tests.test_author_resolution import rpc as rpc
from tests.test_author_resolution import setup as setup
from tests.test_bootstrap_gateway import connected
from tests.test_resolution_context import revalidate


@pytest.fixture
def case(setup):
    raw = load(REFERENCE / "config.production.yaml").value
    policy = GasPolicy.model_validate_json(json.dumps(frozen_production_gas()[0]))
    binding = revalidate(setup.operator.accounts.bindings[1], name="treasury")
    assert binding.profile.kind == "eoa"
    ceiling = policy.model_dump(
        mode="json",
        include={
            "accounts",
            "max_gas_price_wei",
            "max_gas_limit",
            "max_transaction_fee",
            "total_budget",
        },
    )
    authority = ProductionAuthority.model_validate_json(
        json.dumps(
            {
                "networks": [{"chain": "eip155:1", "max_observation_age_seconds": 120}],
                "payers": [{**ceiling, "allow_replacement": False}],
                "signing": {
                    "key_file": "/private/never-opened-keystore",
                    "password_file": "/private/never-opened-password",
                },
            }
        )
    )
    setup.operator = revalidate(
        setup.operator, accounts=StrategyAccounts(bindings=(binding,)), production=authority
    )
    document = parsed(raw)
    return SimpleNamespace(
        setup=setup,
        document=document,
        progress=resolve(document, setup, RunMode.PRODUCTION),
        definition=StrategyDefinition.read(REFERENCE / "strategy.py"),
        path=setup.root / "prepared.json",
        policy=policy,
    )


def run(case, client):
    return prepare_production(case.path, case.document, case.definition, case.progress, client)


def forbid_hooks(monkeypatch):
    def forbidden(*_, **__):
        raise AssertionError("retained preparation must not repeat strategy hooks")

    monkeypatch.setattr(checks, "strategy_worker", forbidden)
    monkeypatch.setattr(checks, "strategy_capabilities", forbidden)


def test_actual_strategy_hooks_and_policy_are_frozen_across_gateway_replacement(case, monkeypatch):
    seen, validations = [], []
    worker, capability = checks.strategy_worker, checks.strategy_capabilities

    def observed_worker(request):
        seen.append(request.config)
        if isinstance(request, ValidationRequest):
            validations.append(request)
        return worker(request)

    def observed_capability(definition, parameters):
        seen.append(parameters)
        return capability(definition, parameters)

    monkeypatch.setattr(checks, "strategy_worker", observed_worker)
    monkeypatch.setattr(checks, "strategy_capabilities", observed_capability)
    before_calls = len(case.setup.rpc.calls)
    with connected(case.setup, mode=RunMode.PRODUCTION) as client:
        record = run(case, client)
    assert len(seen) == 3 and len(validations) == 1
    assert all(item is record.prepared.parameters for item in seen)
    assert record.prepared.parameters.gas == (case.policy,)
    assert len(record.prepared.capabilities.requirements) == 2
    assert record.prepared.dependencies.model_dump(mode="json") == {
        "assets": [],
        "markets": [],
        "prices": [],
    }
    assert record.authority == "unadmitted" and record.policy.authority == "read_only"
    assert record.policy.execution.gas == record.prepared.parameters.gas
    frozen = case.path.read_bytes(), case.path.stat().st_mtime_ns
    assert case.path.stat().st_mode & 0o777 == 0o600
    for secret in (
        "hidden-input-secret",
        "never-opened-password",
        "never-opened-keystore",
        case.setup.rpc.endpoint,
    ):
        assert secret not in case.path.read_text()
    forbid_hooks(monkeypatch)
    with connected(case.setup, mode=RunMode.PRODUCTION) as client:
        assert run(case, client) == record
    assert (case.path.read_bytes(), case.path.stat().st_mtime_ns) == frozen
    assert len(case.setup.rpc.calls) == before_calls


@pytest.mark.parametrize("lost", ["request", "reply"])
def test_lost_policy_connection_retains_hooks_and_retries_only_policy(case, monkeypatch, lost):
    with connected(case.setup, mode=RunMode.PRODUCTION) as client:

        def fail(method, **fields):
            assert read_preparation(case.path).policy is None
            if lost == "reply":
                client.request(method, **fields)
            raise DataError("gateway_unavailable", "connection lost")

        with pytest.raises(DataError, match="connection lost"):
            run(case, SimpleNamespace(request=fail))
    first = read_preparation(case.path)
    assert first.policy is None
    forbid_hooks(monkeypatch)
    with connected(case.setup, mode=RunMode.PRODUCTION) as client:
        result = run(case, client)
    assert result.prepared == first.prepared and result.prepared_digest == first.prepared_digest
    assert result.policy.execution.gas == (case.policy,)


def test_operator_denial_retains_validated_parameters_without_readiness(case):
    authority = case.setup.operator.production
    ceiling = revalidate(authority.payers[0], max_gas_limit=case.policy.max_gas_limit - 1)
    case.setup.operator = revalidate(
        case.setup.operator, production=revalidate(authority, payers=(ceiling,))
    )
    case.progress = resolve(case.document, case.setup, RunMode.PRODUCTION)
    with (
        connected(case.setup, mode=RunMode.PRODUCTION) as client,
        pytest.raises(DataError, match="operator permits"),
    ):
        run(case, client)
    record = read_preparation(case.path)
    assert record.policy is None and record.authority == "unadmitted"
    assert record.prepared.parameters.gas == (case.policy,)


@pytest.mark.parametrize("fault", ["economic_policy", "payer", "context", "operator", "ready"])
def test_policy_reply_cannot_change_parameters_or_promote_read_authority(case, fault):
    with connected(case.setup, mode=RunMode.PRODUCTION) as client:

        def wrong(method, **fields):
            reply = client.request(method, **fields)
            if fault == "economic_policy":
                reply["execution"]["gas"][0]["max_gas_price_wei"] -= 1
            elif fault == "payer":
                reply["accounts"]["bindings"][0]["profile"]["fund"]["address"] = "0x" + "12" * 20
            elif fault == "context":
                reply["context_digest"] = "b" * 64
            elif fault == "operator":
                reply["operator_digest"] = "b" * 64
            else:
                reply["authority"] = "execution"
            return reply

        with pytest.raises(ValueError):
            run(case, SimpleNamespace(request=wrong))
    assert read_preparation(case.path).policy is None


def test_failed_policy_publication_preserves_hooks(case, monkeypatch):
    from almanak.configuration import prepared

    save = prepared.publish_preparation

    def fail_policy(path, record):
        if record.policy is not None:
            raise OSError("disk publication failed")
        save(path, record)

    with monkeypatch.context() as scoped:
        scoped.setattr(prepared, "publish_preparation", fail_policy)
        with (
            connected(case.setup, mode=RunMode.PRODUCTION) as client,
            pytest.raises(OSError, match="disk publication"),
        ):
            run(case, client)
    assert read_preparation(case.path).policy is None
    forbid_hooks(monkeypatch)
    with connected(case.setup, mode=RunMode.PRODUCTION) as client:
        assert run(case, client).policy is not None


@pytest.mark.parametrize("fault", ["source", "filename", "inputs", "operator"])
def test_reuse_refuses_changed_source_inputs_or_operator_without_repeating_hooks(
    case, monkeypatch, fault
):
    with connected(case.setup, mode=RunMode.PRODUCTION) as client:
        run(case, client)
    before = case.path.read_bytes()
    forbid_hooks(monkeypatch)
    if fault == "source":
        case.definition = revalidate(
            case.definition, source=case.definition.source + "\n# changed\n"
        )
    elif fault == "filename":
        case.definition = revalidate(case.definition, filename="other.py")
    elif fault == "inputs":
        raw = json.loads(case.document.inputs.model_dump_json())
        raw["strategy"]["allocations"]["main_weth"]["capital"] = "0.002"
        case.document = parsed(raw)
    else:
        changed = revalidate(case.setup.operator, production=None)
        with pytest.raises(ValueError, match="operator configuration"):
            BootstrapService(case.setup.context, changed, RunMode.PRODUCTION, case.setup.env_file)
        case.setup.operator = changed
        case.progress = resolve(case.document, case.setup, RunMode.PRODUCTION)
    with connected(case.setup, mode=RunMode.PRODUCTION) as client, pytest.raises(ValueError):
        run(case, client)
    assert case.path.read_bytes() == before


def test_parallel_owner_is_refused_before_hooks_or_gateway_io(case, monkeypatch):
    forbid_hooks(monkeypatch)
    with case.path.with_suffix(".lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        with pytest.raises(RuntimeError, match="active owner"):
            run(case, None)
    assert not case.path.exists()


@pytest.mark.parametrize("field", ["prepared_digest", "parameters_digest", "context_digest"])
def test_corrupt_retained_bundle_cannot_be_used(case, field):
    with connected(case.setup, mode=RunMode.PRODUCTION) as client:
        record = run(case, client)
    raw = record.model_dump(mode="json")
    target = raw if field == "prepared_digest" else raw["prepared"]
    target[field] = "b" * 64
    with pytest.raises(ValidationError):
        PreparationProgress.model_validate_json(json.dumps(raw))


@pytest.mark.parametrize("fault", ["auth", "fields", "account", "mode", "empty"])
def test_policy_endpoint_authenticates_and_bounds_its_request_without_provider_io(case, fault):
    mode = RunMode.PRODUCTION
    policies = [case.policy.model_dump(mode="json")]
    extra = {}
    if fault == "mode":
        mode = RunMode.FORK
        case.setup.context = revalidate(case.setup.context, mode=mode)
    elif fault == "account":
        policies[0]["accounts"] = ["unbound"]
    elif fault == "empty":
        policies = []
    elif fault == "fields":
        extra["key_file"] = "/private/forbidden"
    before = len(case.setup.rpc.calls)
    with connected(case.setup, mode=mode) as client:
        if fault == "auth":
            client.token = "0" * 64
        with pytest.raises(DataError):
            client.request(
                "resolution-gas-policy",
                context_digest=case.setup.context.digest,
                policies=policies,
                **extra,
            )
    assert len(case.setup.rpc.calls) == before
