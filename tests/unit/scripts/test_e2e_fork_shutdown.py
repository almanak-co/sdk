"""Admission replays bytes emitted by the real owner and gateway custody code."""

from types import SimpleNamespace

import pytest
import pytest_asyncio

from almanak.gateway.data.price.qa_pool import _canonical
from almanak.gateway.qa_fork_custody import ForkCustody
from qa_lab.e2e_card import digest, load_json
from qa_lab.e2e_fork_shutdown import CONTRACT, validate_fork_shutdown
from qa_lab.e2e_ownership import OwnershipStore


@pytest_asyncio.fixture
async def released(tmp_path):
    (tmp_path / "preparation").mkdir()
    card = _canonical({"run_id": "released-test-run", "schema_version": 3})
    (tmp_path / "preparation/card.json").write_bytes(card)
    manifest = {
        "run_id": "released-test-run",
        "preparation_sha256": digest(card),
        "fork_block": 100,
        "fork_hash": "0x" + "ab" * 32,
    }
    (tmp_path / "pool-input.json").write_bytes(_canonical(manifest))
    manifest_hash = digest(_canonical(manifest))
    identity = {
        "manifest_sha256": manifest_hash,
        "instance_id": "owned-fork",
        "chain_id": 42161,
        "fork_block": 100,
        "fork_hash": manifest["fork_hash"],
    }
    (tmp_path / "gateway-startup.json").write_bytes(
        _canonical(
            {
                "run_id": manifest["run_id"],
                "scope": "qa_managed_fork_startup",
                "fork_identity": identity,
            }
        )
    )
    custody = ForkCustody(
        SimpleNamespace(
            root=tmp_path,
            manifest_hash=manifest_hash,
            manifest=SimpleNamespace(run_id=manifest["run_id"]),
        )
    )
    store = OwnershipStore(tmp_path / "ownership.sqlite")
    store.initialize(run_id=manifest["run_id"], card_hash=digest(card))
    lease = store.acquire("controller")
    store.request_cleanup(lease)
    store.release_fork(lease)
    assert await custody.wait_for_release(timeout=0.01) == "RELEASED"
    custody.record_stopped("RELEASED", {"arbitrum": SimpleNamespace(is_running=False)})
    return tmp_path


@pytest.mark.asyncio
async def test_real_release_handshake_proves_only_recorded_fork_shutdown(released):
    result = validate_fork_shutdown(released, {"fork_shutdown": CONTRACT})
    assert result["status"] == "PASS"
    assert result["scope"] == "recorded_managed_fork_shutdown"
    assert "fork-release-observed.json" in result["source_artifacts"]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mutation", ["missing", "running", "timeout", "numeric_true", "ack", "fork", "request", "card"]
)
async def test_partial_release_forgery_cannot_pass(released, mutation):
    name = "fork-shutdown.json"
    if mutation == "missing":
        (released / "fork-release-observed.json").unlink()
    elif mutation == "request":
        (released / "fork-release.json").write_bytes(_canonical(load_json(released / "fork-release.json")) + b"\n")
    else:
        name = {
            "ack": "fork-release-observed.json",
            "fork": "gateway-startup.json",
            "card": "preparation/card.json",
        }.get(mutation, name)
        value = load_json(released / name)
        if mutation == "running":
            value["processes_stopped"] = False
        elif mutation == "timeout":
            value["reason"] = "OBSERVATION_TIMEOUT"
        elif mutation == "numeric_true":
            value["processes_stopped"] = 1
        elif mutation == "ack":
            value["request_sha256"] = "0" * 64
        elif mutation == "fork":
            value["fork_identity"]["instance_id"] = "another-fork"
        else:
            value["run_id"] = "another-test-run"
        (released / name).write_bytes(_canonical(value))
    with pytest.raises(ValueError):
        validate_fork_shutdown(released, {"fork_shutdown": CONTRACT})


def test_legacy_contract_does_not_inherit_fork_shutdown_claim(tmp_path):
    assert validate_fork_shutdown(tmp_path, {}) is None
