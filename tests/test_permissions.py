"""Replay canonical fork authority observations and refuse mutated authorization."""

import gzip
import json
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from threading import Barrier
from types import SimpleNamespace

import httpx
import jsonschema
import pytest

from almanak.connectors.aave_v4.models import LendingConfig
from almanak.connectors.aave_v4.permission_adapter import (
    AavePermissions,
    PermissionManifest,
    generate,
    verify,
)
from almanak.gateway.ethereum import DataError, EthereumReader
from almanak.gateway.permission_models import OwnerTransaction
from almanak.gateway.permissions import statement, write_artifacts
from almanak.gateway.roles_conditions import bounded, equal, matches
from almanak.gateway.roles_storage import mapping_slot
from tests.reference_parameters import authored_values

FIXTURE = Path(__file__).with_name("fixtures") / "safe-roles-2.1.0-authority.json.gz"
PARAMETERIZED_CONFIGURATION = "5cb6d399569054c0e99e155c0455ba2c12533c12842aa85f9f524aed3a16de9b"
STATEMENT_SCHEMA = json.loads(
    (Path(__file__).parents[1] / "docs/contracts/permissions-v1.schema.json").read_text()
)


@pytest.fixture(scope="module")
def authority_trace():
    from tests.retained_scheduling import config_input

    raw = json.loads(gzip.decompress(FIXTURE.read_bytes()))
    raw["config"] = config_input(raw["config"])
    from almanak.gateway.permissions import configuration_hash

    config = LendingConfig.model_validate_json(json.dumps(raw["config"]))
    for name in ("manifest", "regenerated"):
        raw[name]["configuration_hash"] = configuration_hash(config)
    manifest = PermissionManifest.model_validate_json(json.dumps(raw["manifest"]))
    raw["verification"]["manifest"] = manifest.digest
    return raw


@pytest.fixture
def authority_backend(authority_trace, monkeypatch):
    # This recording proves installed Roles storage. Target
    # admission now performs additional reads, exercised by the protocol tests
    # and actual fork lifecycles rather than invented responses in this trace.
    monkeypatch.setattr(
        "almanak.connectors.aave_v4.permission_adapter.verify_deployments", lambda *_: None
    )
    monkeypatch.setattr(
        "almanak.connectors.aave_v4.permission_adapter.read_reserve", lambda *_: None
    )
    clients = []

    def create(override=None, phase="requests"):
        rows = authority_trace[phase]
        responses = {
            json.dumps([r["method"], r["params"]], sort_keys=True): r["result"] for r in rows
        }
        config = LendingConfig.model_validate_json(json.dumps(authority_trace["config"]))
        batches: list[int] = []

        def answer(body):
            key = json.dumps([body["method"], body["params"]], sort_keys=True)
            assert key in responses, (body["method"], body["params"])
            value = responses[key]
            result = override(body, value) if override else {"result": value}
            return {"jsonrpc": "2.0", "id": body["id"], **result}

        def dispatch(request):
            # The recording holds one row per logical read. A batch is answered member
            # by member from the same rows, so batching cannot add or alter a response.
            body = json.loads(request.content)
            if isinstance(body, list):
                batches.append(len(body))
                return httpx.Response(200, json=[answer(member) for member in body])
            return httpx.Response(200, json=answer(body))

        client = httpx.Client(transport=httpx.MockTransport(dispatch))
        clients.append(client)
        reader = EthereumReader(
            config.accounts.bindings[0].profile.fund.chain, "http://127.0.0.1:1234", client=client
        )
        return config, SimpleNamespace(reader=reader, guard=lambda: None, batches=batches)

    yield create
    for client in clients:
        client.close()


def test_verification_reads_installed_grants_from_storage(authority_trace, authority_backend):
    config, backend = authority_backend()
    manifest = PermissionManifest.model_validate_json(json.dumps(authority_trace["manifest"]))
    result = verify(config, manifest, backend)
    expected = dict(authority_trace["verification"])
    # Storage and code reads only; the recorded Roles history is no longer read.
    expected["verification_rpc_calls"] = 181
    expected["observations"] = [expected["observation"]] * len(manifest.partitions)
    expected["observation"] = None
    assert result == expected
    assert backend.batches == []


def test_rpc_measurements_exclude_concurrent_gateway_requests(authority_backend):
    _, backend = authority_backend()
    barrier = Barrier(2)
    with ThreadPoolExecutor(max_workers=2) as workers:
        one = workers.submit(measure_rpc_calls, backend.reader, barrier, 1)
        three = workers.submit(measure_rpc_calls, backend.reader, barrier, 3)
        assert (one.result(timeout=10), three.result(timeout=10)) == (1, 3)
    assert backend.reader.rpc_count == 0


def measure_rpc_calls(reader, barrier, count):
    initial = reader.rpc_count
    barrier.wait(timeout=5)
    for _ in range(count):
        reader._rpc("eth_chainId", [])
    barrier.wait(timeout=5)
    return reader.rpc_count - initial


@pytest.mark.parametrize("frozen", [False, True])
def test_regeneration_reproduces_installed_grants(
    authority_trace, authority_backend, reference_strategy, tmp_path, monkeypatch, frozen
):
    config, backend = authority_backend(phase="generation_requests")
    # The recording predates strategy.parameters; the knobs are the reference's.
    config = config.model_copy(update={"parameters": authored_values("aave_v4_weth_supply")})
    # The recording reused its already chain-verified read transport.
    backend.reader._verified = True
    # Current author code must reproduce the recorded installed grants.
    strategy = reference_strategy
    if frozen:
        from almanak.backtest.dependencies import DataDependencies
        from almanak.capabilities import CapabilityCodec
        from almanak.configuration.prepared import PreparedParameters
        from almanak.connectors.aave_v4.models import LendingParameters
        from almanak.connectors.aave_v4.permission_adapter import AavePermissions
        from almanak.connectors.support import RunMode
        from almanak.gateway.operator_config import content_digest
        from almanak.gateway.permissions import generate as generate_prepared

        parameters = LendingParameters.model_validate(
            {name: getattr(config, name) for name in LendingParameters.model_fields}
        )
        connector = AavePermissions()
        prepared = PreparedParameters(
            context_digest="0" * 64,
            resolution_digest="0" * 64,
            mode=RunMode.FORK,
            strategy=strategy,
            parameters=parameters,
            parameters_digest=content_digest(parameters),
            capabilities=connector.codec.evaluate(strategy, parameters),
            dependencies=DataDependencies(),
        )

        def forbidden(*_, **__):
            raise AssertionError("permission generation must reuse the frozen capabilities")

        monkeypatch.setattr(CapabilityCodec, "evaluate", forbidden)
        manifest = generate_prepared(
            config, strategy, backend, connector=connector, prepared=prepared
        )
    else:
        manifest = generate(config, strategy, backend)
    # The recording predates strategy.parameters; this pins the digest with the reference's.
    assert manifest.model_dump(mode="json") == {
        **authority_trace["regenerated"],
        "source_hash": strategy.sha256,
        "configuration_hash": PARAMETERIZED_CONFIGURATION,
    }
    assert not any(p.transactions for p in manifest.partitions)
    write_artifacts(manifest, tmp_path / "permissions")
    # Installed authority leaves the owner nothing to sign, so no batch is exported.
    assert (tmp_path / "permissions/manifest.json").is_file()
    assert not list((tmp_path / "permissions").glob("safe-*.json"))


@pytest.mark.parametrize(
    "mutation",
    [
        "extra_target",
        "missing_member",
        "default_role",
        "code",
    ],
)
def test_installed_authority_mutations_are_refused(authority_trace, authority_backend, mutation):
    manifest = PermissionManifest.model_validate_json(json.dumps(authority_trace["manifest"]))
    profile = manifest.partitions[0].profile
    root = mapping_slot("bytes32", bytes.fromhex(profile.role_key[2:]), 4)
    slots = {
        "extra_target": mapping_slot("address", manifest.partitions[0].grants[0].target, root + 1),
        "missing_member": mapping_slot("address", profile.operator.address, root),
        "default_role": mapping_slot("address", profile.operator.address, 7),
    }

    def mutate(body, value):
        method, params = body["method"], body["params"]
        if mutation == "code" and method == "eth_getCode" and params[0] == profile.fund.address:
            value = "0x00"
        if (
            mutation in slots
            and method == "eth_getStorageAt"
            and params[:2] == [profile.roles_module, hex(slots[mutation])]
        ):
            value = hex({"extra_target": 1, "missing_member": 0, "default_role": 1}[mutation])
        return {"result": value}

    config, backend = authority_backend(mutate)
    with pytest.raises(DataError):
        verify(config, manifest, backend)


def test_manifest_cannot_widen_correlated_branches(authority_trace, authority_backend):
    manifest = PermissionManifest.model_validate_json(json.dumps(authority_trace["manifest"]))
    partition = manifest.partitions[0]
    # Start from today's derivation, so only the widened branch differs from it.
    derived = AavePermissions().derive(partition.profile, manifest.capabilities)
    partition = partition.model_copy(update={"grants": derived})
    grant = partition.grants[0].model_copy(
        update={"condition": matches(equal("uint256", 7), bounded(2**127))}
    )
    partition = partition.model_copy(update={"grants": (grant, *partition.grants[1:])})
    config, backend = authority_backend()
    with pytest.raises(ValueError, match="trusted connector derivation"):
        verify(
            config,
            manifest.model_copy(update={"partitions": (partition, *manifest.partitions[1:])}),
            backend,
        )


def test_condition_limits_refuse_widening():
    from almanak.gateway.roles_conditions import Condition, flatten

    with pytest.raises(ValueError, match="contract limits"):
        flatten(matches(*(equal("uint256", i) for i in range(257))))
    with pytest.raises(ValueError, match="uint128"):
        bounded(2**128)
    with pytest.raises(ValueError):
        Condition(parameter=5, operator=0)


def test_statement_names_installed_safes_and_the_batch_admission_exports(authority_trace, tmp_path):
    manifest = PermissionManifest.model_validate_json(json.dumps(authority_trace["manifest"]))
    installed = statement(manifest)
    jsonschema.validate(installed, STATEMENT_SCHEMA)
    assert installed["manifest_digest"] == manifest.digest and not installed["pending"]
    assert {p["status"] for p in installed["partitions"]} == {"installed"}
    assert all(p["bundle"] is None for p in installed["partitions"])

    first = manifest.partitions[0]
    owed = first.model_copy(
        update={"transactions": (OwnerTransaction(to=first.profile.roles_module, data="0x1234"),)}
    )
    pending_manifest = manifest.model_copy(update={"partitions": (owed, *manifest.partitions[1:])})
    pending = statement(pending_manifest)
    jsonschema.validate(pending, STATEMENT_SCHEMA)
    entry = pending["partitions"][0]
    assert pending["pending"] and entry["status"] == "pending"
    assert (entry["account"], entry["safe"]) == (first.account, first.profile.fund.address)
    assert entry["observed_block"] == first.observation.number
    assert entry["explanation"] == [e for grant in first.grants for e in grant.explanation]
    assert pending["runtime_checks"] == list(manifest.runtime_checks)
    # The owner signs exactly the batch admission exports for the same manifest.
    write_artifacts(pending_manifest, tmp_path / "exported")
    assert entry["bundle"] == json.loads((tmp_path / "exported/safe-0.json").read_text())
    assert sorted(path.name for path in (tmp_path / "exported").glob("safe-*.json")) == [
        f"safe-{index}.json"
        for index, partition in enumerate(pending_manifest.partitions)
        if partition.transactions
    ]
