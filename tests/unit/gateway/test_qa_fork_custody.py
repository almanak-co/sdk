"""Terminal proof retention stops owned handles without looking up logged PIDs."""

import hashlib
import threading
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from almanak.gateway.data.price.qa_pool import _canonical
from almanak.gateway.managed import ManagedGateway
from almanak.gateway.qa_fork_custody import ForkCustody


@pytest.mark.asyncio
async def test_real_cleanup_owner_release_is_accepted_by_gateway(tmp_path):
    from qa_lab.e2e_ownership import OwnershipStore

    manifest = _canonical({"preparation_sha256": "a" * 64})
    manifest_hash = hashlib.sha256(manifest).hexdigest()
    identity = {"manifest_sha256": manifest_hash, "instance_id": "owned-fork"}
    (tmp_path / "pool-input.json").write_bytes(manifest)
    (tmp_path / "gateway-startup.json").write_bytes(_canonical({"run_id": "local-run", "fork_identity": identity}))
    route = SimpleNamespace(root=tmp_path, manifest_hash=manifest_hash, manifest=SimpleNamespace(run_id="local-run"))
    custody = ForkCustody(route)
    store = OwnershipStore(tmp_path / "ownership.sqlite")
    store.initialize(run_id="local-run", card_hash="a" * 64)
    lease = store.acquire("controller")
    store.request_cleanup(lease)
    store.release_fork(lease)
    assert await custody.wait_for_release(timeout=0.01) == "RELEASED"


@pytest.fixture
def custody(tmp_path):
    identity = {"manifest_sha256": "ab" * 32, "instance_id": "owned-fork"}
    (tmp_path / "gateway-startup.json").write_bytes(_canonical({"run_id": "local-run", "fork_identity": identity}))
    return ForkCustody(
        SimpleNamespace(root=tmp_path, manifest_hash="ab" * 32, manifest=SimpleNamespace(run_id="local-run"))
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("release", ["valid", "wrong-fork", "missing"])
async def test_release_observation_distinguishes_completion_from_failure(custody, release):
    if release != "missing":
        value = dict(custody.expected)
        if release == "wrong-fork":
            value["fork_identity"] = {"instance_id": "different-fork"}
        (custody.root / "fork-release.json").write_bytes(_canonical(value))
    result = await custody.wait_for_release(timeout=0.01)
    assert result == {"valid": "RELEASED", "wrong-fork": "INVALID_RELEASE", "missing": "OBSERVATION_TIMEOUT"}[release]
    assert (custody.root / "fork-release-observed.json").exists() == (release == "valid")


@pytest.mark.asyncio
async def test_gateway_stops_original_manager_after_observation(custody):
    manager = SimpleNamespace(is_running=True)

    async def stop():
        manager.is_running = False

    manager.stop = AsyncMock(side_effect=stop)
    (custody.root / "fork-release.json").write_bytes(_canonical(custody.expected))
    gateway = SimpleNamespace(
        _qa_fork_custody=custody, _keep_anvil=True, _anvil_managers={"arbitrum": manager}, _original_env={}
    )
    await ManagedGateway._stop_anvil_forks(gateway)
    manager.stop.assert_awaited_once()
    assert b'"processes_stopped":true' in (custody.root / "fork-shutdown.json").read_bytes()


@pytest.mark.asyncio
async def test_observer_failure_still_stops_owned_process(custody):
    manager = SimpleNamespace(is_running=False, stop=AsyncMock())
    custody.wait_for_release = AsyncMock(side_effect=OSError("evidence disk unavailable"))
    gateway = SimpleNamespace(
        _qa_fork_custody=custody, _keep_anvil=True, _anvil_managers={"arbitrum": manager}, _original_env={}
    )
    with pytest.raises(OSError, match="evidence disk"):
        await ManagedGateway._stop_anvil_forks(gateway)
    manager.stop.assert_awaited_once()
    assert b'"observation_complete":false' in (custody.root / "fork-shutdown.json").read_bytes()


def test_gateway_stop_proof_follows_drained_server_before_fork_release(custody, monkeypatch):
    from almanak.gateway import server as server_module

    order = []

    async def stopped():
        order.append("server_stopped")

    async def forks():
        assert (custody.root / "subject-gateway-stopped.json").is_file()
        order.append("fork_release")

    original = custody.record_gateway_stopped

    def recorded():
        assert order == ["server_stopped"]
        original()
        order.append("proof_retained")

    custody.record_gateway_stopped = recorded
    server = SimpleNamespace(start=AsyncMock(), stop=AsyncMock(side_effect=stopped))
    monkeypatch.setattr(server_module, "GatewayServer", lambda settings: server)
    stop = threading.Event()
    stop.set()
    gateway = SimpleNamespace(
        settings=SimpleNamespace(),
        _anvil_chains=[],
        _anvil_managers={"arbitrum": object()},
        _qa_fork_custody=custody,
        _stop_requested=stop,
        _started=threading.Event(),
        _anvil_watchdog=AsyncMock(),
        _stop_anvil_forks=AsyncMock(side_effect=forks),
    )
    ManagedGateway._run_server(gateway)
    assert order == ["server_stopped", "proof_retained", "fork_release"]
