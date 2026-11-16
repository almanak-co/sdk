"""Historical dependency admission selects the same verified factory as execution."""

import gzip
import json
from pathlib import Path

import httpx
import pytest

from almanak.connectors.curve.crypto_deployment import verify_pool
from almanak.connectors.curve.crypto_observation import CryptoObservation
from almanak.connectors.curve.economic_prepare import crypto_dependencies
from almanak.gateway.ethereum import DataError
from tests.test_curve_crypto_deployment import deployment as deployment
from tests.test_curve_reader import CapturedReader, request_key

DIRECTORY = (
    Path(__file__).parents[1]
    / "docs/internal/reports/evidence/2026-09-07-rollout/curve-robinhood-crypto-discovery"
)


@pytest.fixture(params=("legacy", "core"))
def captured(request, deployment, monkeypatch):
    def refuse(*args, **kwargs):
        raise AssertionError("dependency acquisition replay cannot access HTTP")

    monkeypatch.setattr(httpx.Client, "send", refuse)
    if request.param == "legacy":
        yield deployment[:3]
        return
    data = json.loads(
        gzip.decompress((DIRECTORY / "curve-robinhood-agent-crypto-quotes.json.gz").read_bytes())
    )
    pool = CryptoObservation.model_validate_json(json.dumps(data["families"][0]["observation"]))
    with CapturedReader(pool.source, data["records"]) as reader:
        block = reader.pin_block(pool.block.number)
        assert block == pool.block
        yield pool.source, reader, block


def test_dependencies_match_execution_without_access_to_unadmitted_code(captured, monkeypatch):
    source, reader, block = captured
    execution = verify_pool(reader, source, block)
    original = reader._rpc
    calls = []

    def restricted(method, params):
        if method in {"eth_getCode", "eth_call", "eth_getStorageAt"}:
            address = params[0]["to"] if method == "eth_call" else params[0]
            assert address in {source.factory, source.pool}
        calls.append((method, params))
        return original(method, params)

    monkeypatch.setattr(reader, "_rpc", restricted)
    assert crypto_dependencies(reader, source, block) == (
        execution.blueprint.address,
        execution.math.address,
        execution.views.address,
    )
    assert sum(method == "eth_getStorageAt" for method, _ in calls) == 1


@pytest.mark.parametrize("role", ("factory", "pool"))
def test_dependency_admission_refuses_changed_executable_code(captured, role):
    source, reader, block = captured
    key = request_key(
        "eth_getCode", [getattr(source, role), {"blockHash": block.hash, "requireCanonical": True}]
    )
    raw = reader.records[key]
    reader.records[key] = "0x" + f"{int(raw[2:4], 16) ^ 1:02x}" + raw[4:]
    with pytest.raises(DataError, match="compiled"):
        crypto_dependencies(reader, source, block)


def test_dependency_admission_refuses_mismatched_immutable_coin_order(captured):
    source, reader, block = captured
    with pytest.raises(DataError, match="coin order"):
        crypto_dependencies(reader, source.model_copy(update={"coins": source.coins[::-1]}), block)
