"""Startup discovery uses gateway-owned identity and a real boot database."""

import sqlite3
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from almanak.core.rpc_network import Network
from almanak.framework.runner.identity import resolve_deployment_id
from almanak.gateway.data.price import qa_pool
from qa_lab.e2e_card import REPO, TEMPLATE, bind_pool_input, canonical, digest, load_json, prepare
from qa_lab.e2e_ownership import OwnershipError, OwnershipStore
from qa_lab.e2e_startup import StartupPending, observe_startup
from qa_lab.qa_execution_context import ForkExecutionContext
from tests.unit.scripts import test_e2e_card as card_tests

checkout = card_tests.checkout
WALLET = "0x" + "11" * 20


@pytest.fixture
def lane(checkout, tmp_path, monkeypatch, request):
    root = tmp_path / "run"
    prepare(checkout, REPO / TEMPLATE, root / "preparation", "startup-test-run")
    root.chmod(0o700)
    anchor = root / "anchor.json"
    anchor.write_bytes(canonical({"chain_id": 42161, "fork_block": 100, "fork_hash": "0x" + "ab" * 32}))
    bind_pool_input(root / "preparation", checkout, root / "subject", anchor, root / "pool-input.json")
    store = OwnershipStore(root / "ownership.sqlite")
    store.initialize(run_id="startup-test-run", card_hash=digest((root / "preparation/card.json").read_bytes()))
    lease = store.acquire("controller", seconds=300)
    token = store.reserve_launch(lease, "subject")
    if getattr(request, "param", "claimed") == "claimed":
        store.claim_launch(lease, role="subject", token=token)
    monkeypatch.setattr(qa_pool, "local_db_path", lambda: root / "subject/almanak_state.db")
    monkeypatch.setattr(qa_pool, "get_rpc_url", lambda *args, **kwargs: "http://127.0.0.1:54321")
    client = SimpleNamespace(
        provider=SimpleNamespace(
            endpoint_uri="http://127.0.0.1:54321",
            make_request=lambda *args: {
                "result": {"instanceId": "fixture-instance", "forkedNetwork": {"forkBlockNumber": 100}}
            },
        ),
        eth=SimpleNamespace(chain_id=42161, get_block=lambda number: {"hash": bytes.fromhex("ab" * 32)}),
    )
    monkeypatch.setattr(qa_pool.PoolInputRoute, "_client", lambda self: client)
    route = qa_pool.PoolInputRoute(
        SimpleNamespace(
            network=Network.ANVIL,
            chains=["arbitrum"],
            enable_manual_price_overrides=False,
            qa_pool_price_manifest=root / "pool-input.json",
        )
    )
    reads = []

    def observe(context):
        assert context.rpc_url == client.provider.endpoint_uri
        assert context.instance_id == "fixture-instance"
        reads.append(context)
        return client

    monkeypatch.setattr(ForkExecutionContext, "assert_rpc_identity", observe)
    return SimpleNamespace(root=root, repo=checkout, store=store, lease=lease, token=token, route=route, reads=reads)


async def publish(lane):
    manager = SimpleNamespace(anvil_port=54321, fund_wallet=AsyncMock(return_value=False))
    with pytest.raises(ValueError, match="gas reserve"):
        await lane.route.provision_stimulus(manager, WALLET)
    return load_json(lane.root / "gateway-startup.json")


@pytest.mark.asyncio
async def test_failed_funding_still_identifies_fork_without_claiming_execution(lane):
    value = await publish(lane)
    assert value["funding_status"] == value["execution_status"] == "UNMEASURED"
    result = observe_startup(lane.root, lane.store, lane.lease, repo=lane.repo)
    assert result["status"] == "STARTING"
    assert result["deployment_id"] is None
    assert load_json(lane.root / "context.json")["instance_id"] == "fixture-instance"
    identity = resolve_deployment_id(wallet_address=WALLET, chain="arbitrum")
    with sqlite3.connect(lane.root / "subject/almanak_state.db") as db:
        db.execute("CREATE TABLE strategy_state (deployment_id TEXT)")
        db.execute("INSERT INTO strategy_state VALUES (?)", (identity,))
    booted = observe_startup(lane.root, lane.store, lane.lease, repo=lane.repo)
    assert booted["status"] == "OBSERVED"
    assert booted["deployment_id"] == identity
    assert booted["execution_status"] == "UNMEASURED"


@pytest.mark.asyncio
async def test_successful_provisioning_retains_subject_balance_baseline(lane):
    from almanak.framework.data.tokens import get_token_resolver

    client = lane.route._client()
    usdc = get_token_resolver().resolve("USDC", "arbitrum", skip_gateway=True).address.lower()
    amount = lane.route.manifest.stimulus_usdc_raw
    client.eth.get_block = lambda number: {"number": 100, "hash": bytes.fromhex("ab" * 32)}
    client.eth.call = lambda call, **kw: (amount if call["to"].lower() == usdc else 0).to_bytes(32, "big")
    client.eth.get_balance = lambda *args, **kwargs: 10**18
    client.eth.get_transaction_count = lambda *args, **kwargs: 0
    client.eth.get_code = lambda *args, **kwargs: b""
    manager = SimpleNamespace(
        anvil_port=54321, fund_wallet=AsyncMock(return_value=True), fund_tokens_report=AsyncMock(return_value=[])
    )
    await lane.route.provision_stimulus(manager, WALLET)
    observed = load_json(lane.root / "subject-initial-balances.json")
    assert observed["scope"] == "pre_dispatch_subject_inventory"
    assert observed["run_id"] == lane.route.manifest.run_id
    assert observed["balances"]["wallet"].lower() == WALLET
    assert len(observed["balances"]["tokens"]) == 3
    assert observed["balances"]["native_wei"] == str(10**18)


def test_missing_startup_is_pending_not_a_retry_permission(lane):
    with pytest.raises(StartupPending):
        observe_startup(lane.root, lane.store, lane.lease, repo=lane.repo)
    assert not lane.reads
    with pytest.raises(OwnershipError, match="already reserved"):
        lane.store.reserve_launch(lane.lease, "subject")


@pytest.mark.asyncio
@pytest.mark.parametrize("mutation", ["public_rpc", "manifest", "database", "wallet", "fork", "context"])
async def test_startup_cannot_cross_bind_or_replace_context(lane, mutation):
    value = await publish(lane)
    if mutation == "public_rpc":
        value["rpc_url"] = "https://example.invalid"
    elif mutation == "manifest":
        value["fork_identity"]["manifest_sha256"] = "0" * 64
    elif mutation == "database":
        value["database_path"] = str(lane.root / "other/almanak_state.db")
    elif mutation == "wallet":
        value["subject_wallet"] = lane.route.manifest.stimulus_wallet
    elif mutation == "fork":
        value["fork_identity"]["fork_block"] += 1
    else:
        (lane.root / "context.json").write_bytes(canonical({"instance_id": "another-run"}))
    (lane.root / "gateway-startup.json").write_bytes(canonical(value))
    with pytest.raises(ValueError):
        observe_startup(lane.root, lane.store, lane.lease, repo=lane.repo)
    if mutation != "context":
        assert not lane.reads
        assert not (lane.root / "context.json").exists()


@pytest.mark.asyncio
async def test_supervisor_wait_observes_boot_then_rejects_a_dead_worker(lane):
    from qa_lab.e2e_startup import wait_for_startup

    await publish(lane)
    identity = resolve_deployment_id(wallet_address=WALLET, chain="arbitrum")
    with sqlite3.connect(lane.root / "subject/almanak_state.db") as db:
        db.execute("CREATE TABLE strategy_state (deployment_id TEXT)")
        db.execute("INSERT INTO strategy_state VALUES (?)", (identity,))
    worker = SimpleNamespace(poll=lambda: None)
    result = wait_for_startup(lane.root, lane.store, lane.lease, worker, output=lane.root / "observed", repo=lane.repo)
    assert result["status"] == "OBSERVED"
    assert lane.store.snapshot()["ownership"]["cleanup"] == 0
    dead = SimpleNamespace(poll=lambda: 0)
    with pytest.raises(RuntimeError, match="exited during startup"):
        wait_for_startup(lane.root, lane.store, lane.lease, dead, output=lane.root / "dead", repo=lane.repo)
    assert lane.store.snapshot()["ownership"]["cleanup"] == 1
    assert load_json(lane.root / "dead/failure.json")["cleanup_request"] == "RECORDED"


@pytest.mark.parametrize("during_capture", [False, True])
def test_cleanup_child_exit_fences_startup_even_with_complete_observation(lane, monkeypatch, during_capture):
    from qa_lab import e2e_startup

    backup = SimpleNamespace(returncode=None if during_capture else 0)
    backup.poll = lambda: backup.returncode
    observations = []

    def observe(*args, **kwargs):
        observations.append(True)
        backup.returncode = 0
        return {"status": "OBSERVED", "execution_status": "UNMEASURED"}

    monkeypatch.setattr(e2e_startup, "observe_startup", observe)
    output = lane.root / "lost-cleanup"
    with pytest.raises(RuntimeError, match="Cleanup worker exited"):
        e2e_startup.wait_for_startup(
            lane.root,
            lane.store,
            lane.lease,
            SimpleNamespace(poll=lambda: None),
            output=output,
            repo=lane.repo,
            cleanup_worker=backup,
        )
    assert len(observations) == int(during_capture)
    assert not (output / "result.json").exists()
    failure = load_json(output / "failure.json")
    assert failure["cleanup_worker_returncode"] == 0
    assert failure["cleanup_request"] == "RECORDED"
    with pytest.raises(OwnershipError):
        lane.store.reserve_launch(lane.lease, "stimulus")


def test_startup_timeout_fences_launch_without_claiming_closure(lane, monkeypatch):
    from qa_lab import e2e_startup

    clock = [0.0]
    monkeypatch.setattr(e2e_startup.time, "monotonic", lambda: clock[0])
    monkeypatch.setattr(e2e_startup.time, "sleep", lambda seconds: clock.__setitem__(0, clock[0] + seconds))
    worker = SimpleNamespace(poll=lambda: None)
    with pytest.raises(TimeoutError):
        e2e_startup.wait_for_startup(
            lane.root, lane.store, lane.lease, worker, output=lane.root / "timed-out", repo=lane.repo, timeout_seconds=2
        )
    failure = load_json(lane.root / "timed-out/failure.json")
    assert failure["cleanup_request"] == "RECORDED"
    assert failure["execution_status"] == "UNMEASURED"
    with pytest.raises(OwnershipError, match="cleanup"):
        lane.store.reserve_launch(lane.lease, "stimulus")
    assert len(lane.store.snapshot()["launches"]) == 1


def test_reused_startup_output_is_not_modified_on_failure(lane):
    from qa_lab.e2e_startup import wait_for_startup

    output = lane.root / "existing-attempt"
    output.mkdir()
    (output / "result.json").write_bytes(b"retained prior evidence")
    with pytest.raises(FileExistsError):
        wait_for_startup(
            lane.root, lane.store, lane.lease, SimpleNamespace(poll=lambda: None), output=output, repo=lane.repo
        )
    assert [path.name for path in output.iterdir()] == ["result.json"]
    assert (output / "result.json").read_bytes() == b"retained prior evidence"
    assert lane.store.snapshot()["ownership"]["cleanup"] == 1


@pytest.mark.asyncio
async def test_wrong_sdk_boot_identity_cannot_publish_context(lane):
    await publish(lane)
    with sqlite3.connect(lane.root / "subject/almanak_state.db") as db:
        db.execute("CREATE TABLE strategy_state (deployment_id TEXT)")
        db.execute("INSERT INTO strategy_state VALUES ('deployment:aaaaaaaaaaaa')")
    with pytest.raises(ValueError, match="boot deployment differs"):
        observe_startup(lane.root, lane.store, lane.lease, repo=lane.repo)
    assert not (lane.root / "context.json").exists()


@pytest.mark.asyncio
@pytest.mark.parametrize("mutation", ["network", "manager_port"])
async def test_gateway_does_not_publish_startup_outside_owned_anvil(lane, mutation):
    manager = SimpleNamespace(anvil_port=54321, fund_wallet=AsyncMock())
    if mutation == "network":
        lane.route.settings.network = Network.MAINNET
    else:
        manager.anvil_port = 54322
    with pytest.raises(ValueError):
        await lane.route.provision_stimulus(manager, WALLET)
    manager.fund_wallet.assert_not_awaited()
    assert not (lane.root / "gateway-startup.json").exists()


def test_linked_run_root_cannot_mutate_startup_or_ownership(lane):
    from qa_lab.e2e_startup import wait_for_startup

    linked = lane.root.parent / "linked-run"
    linked.symlink_to(lane.root, target_is_directory=True)
    store = OwnershipStore(linked / "ownership.sqlite")
    with pytest.raises(ValueError, match="canonical"):
        wait_for_startup(
            linked, store, lane.lease, SimpleNamespace(poll=lambda: None), output=linked / "startup", repo=lane.repo
        )
    assert not (lane.root / "startup").exists()
    assert lane.store.snapshot()["ownership"]["cleanup"] == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("lane", ["reserved"], indirect=True)
async def test_startup_waits_for_worker_claim_without_reading_or_relaunching(lane, monkeypatch):
    from qa_lab import e2e_startup

    await publish(lane)
    identity = resolve_deployment_id(wallet_address=WALLET, chain="arbitrum")
    with sqlite3.connect(lane.root / "subject/almanak_state.db") as db:
        db.execute("CREATE TABLE strategy_state (deployment_id TEXT)")
        db.execute("INSERT INTO strategy_state VALUES (?)", (identity,))

    def claim_after_first_observation(seconds):
        assert not lane.reads
        assert not (lane.root / "context.json").exists()
        lane.store.claim_launch(lane.lease, role="subject", token=lane.token)

    monkeypatch.setattr(e2e_startup.time, "sleep", claim_after_first_observation)
    output = lane.root / "handoff"
    result = e2e_startup.wait_for_startup(
        lane.root, lane.store, lane.lease, SimpleNamespace(poll=lambda: None), output=output, repo=lane.repo
    )
    assert load_json(output / "observation-0000.json")["status"] == "STARTING"
    assert result["status"] == "OBSERVED"
    assert len(lane.reads) == 1
    assert len(lane.store.snapshot()["launches"]) == 1
    with pytest.raises(OwnershipError, match="already reserved"):
        lane.store.reserve_launch(lane.lease, "subject")


@pytest.mark.parametrize("lane", ["reserved"], indirect=True)
@pytest.mark.parametrize("outcome", ["exit", "timeout"])
def test_unclaimed_worker_failure_fences_its_late_claim(lane, monkeypatch, outcome):
    from qa_lab import e2e_startup

    clock = [0.0]
    monkeypatch.setattr(e2e_startup.time, "monotonic", lambda: clock[0])
    monkeypatch.setattr(e2e_startup.time, "sleep", lambda seconds: clock.__setitem__(0, clock[0] + seconds))
    worker = SimpleNamespace(poll=lambda: 0 if outcome == "exit" else None)
    with pytest.raises(RuntimeError if outcome == "exit" else TimeoutError):
        e2e_startup.wait_for_startup(
            lane.root, lane.store, lane.lease, worker, output=lane.root / outcome, repo=lane.repo, timeout_seconds=2
        )
    assert not lane.reads
    assert lane.store.snapshot()["ownership"]["cleanup"] == 1
    with pytest.raises(OwnershipError):
        lane.store.claim_launch(lane.lease, role="subject", token=lane.token)
    assert lane.store.snapshot()["launches"][0]["claimed"] == 0
