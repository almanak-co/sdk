"""Cleanup adoption precedes late fork discovery and cannot grant a second launch."""

import os
import subprocess
import sys
import time
from pathlib import Path

import pytest

from qa_lab import e2e_preboot_watchdog as watchdog
from qa_lab.e2e_card import canonical, load_json
from qa_lab.e2e_ownership import OwnershipError
from tests.unit.scripts import test_e2e_startup as startup_tests

checkout = startup_tests.checkout
lane = startup_tests.lane
publish = startup_tests.publish


@pytest.mark.asyncio
async def test_adopts_before_late_gateway_identity_then_cleans_same_fork(lane, monkeypatch):
    startup = await publish(lane)
    source = lane.root / "gateway-startup.json"
    source.unlink()
    clock = [lane.store.clock()]
    lane.store.clock = lambda: clock[0]
    sleeps = []

    def sleep(seconds):
        sleeps.append(seconds)
        clock[0] += 5 if lane.store.snapshot()["ownership"]["cleanup"] else 301
        if lane.store.snapshot()["ownership"]["cleanup"]:
            with pytest.raises(OwnershipError):
                lane.store.reserve_launch(lane.lease, "stimulus")
            source.write_bytes(canonical(startup))

    calls = []

    def cleanup(context, store, lease, **kwargs):
        assert store.snapshot()["ownership"]["cleanup"] == 1
        assert lease.generation == lane.lease.generation + 1
        calls.append(context)
        return {"status": "UNMEASURED"}, {"status": "UNMEASURED"}, "subject"

    monkeypatch.setattr(watchdog.time, "sleep", sleep)
    monkeypatch.setattr(watchdog, "_cleanup_until_boot", cleanup)
    result = watchdog.watch_prepared_cleanup(
        lane.root, lane.store, owner="backup", output=lane.root / "backup", supervised=True, repo=lane.repo
    )
    assert len(sleeps) == 2
    assert len(calls) == len(lane.reads) == 1
    assert calls[0].instance_id == startup["fork_identity"]["instance_id"]
    assert result["status"] == "UNMEASURED"
    assert len(lane.store.snapshot()["launches"]) == 1


@pytest.mark.asyncio
async def test_controller_recovers_late_startup_under_existing_cleanup_lease(lane, monkeypatch):
    startup = await publish(lane)
    source = lane.root / "gateway-startup.json"
    source.unlink()
    lane.store.request_cleanup(lane.lease)

    def late_startup(seconds):
        assert lane.store.snapshot()["ownership"]["owner"] == lane.lease.owner
        source.write_bytes(canonical(startup))

    def cleanup(context, store, lease, **kwargs):
        assert lease == lane.lease
        assert context.instance_id == startup["fork_identity"]["instance_id"]
        with pytest.raises(OwnershipError):
            store.reserve_launch(lease, "stimulus")
        return {"status": "UNMEASURED"}, {"status": "UNMEASURED"}, "subject"

    monkeypatch.setattr(watchdog.time, "sleep", late_startup)
    monkeypatch.setattr(watchdog, "_cleanup_until_boot", cleanup)
    result = watchdog.watch_prepared_cleanup(
        lane.root,
        lane.store,
        owner=lane.lease.owner,
        output=lane.root / "recovery",
        supervised=True,
        repo=lane.repo,
        cleanup_lease=lane.lease,
    )
    assert result["e2e_admission"] == "UNMEASURED"
    assert lane.store.snapshot()["ownership"]["generation"] == lane.lease.generation
    assert load_json(lane.root / "recovery/retained.json")["generation"] == lane.lease.generation
    assert not (lane.root / "recovery/adopted.json").exists()
    assert len(lane.reads) == 1


def test_controller_recovery_cannot_use_a_running_lease(lane):
    with pytest.raises(ValueError):
        watchdog.watch_prepared_cleanup(
            lane.root,
            lane.store,
            owner=lane.lease.owner,
            output=lane.root / "recovery",
            supervised=True,
            repo=lane.repo,
            cleanup_lease=lane.lease,
        )
    assert not (lane.root / "recovery").exists()


def test_missing_identity_retains_cleanup_and_never_claims_success(lane, monkeypatch):
    clock = [lane.store.clock() + 301]
    lane.store.clock = lambda: clock[0]

    def interrupted(seconds):
        state = lane.store.snapshot()["ownership"]
        assert state["owner"] == "backup" and state["cleanup"] == 1
        raise KeyboardInterrupt()

    monkeypatch.setattr(watchdog.time, "sleep", interrupted)
    output = lane.root / "backup"
    with pytest.raises(KeyboardInterrupt):
        watchdog.watch_prepared_cleanup(
            lane.root, lane.store, owner="backup", output=output, supervised=True, repo=lane.repo
        )
    assert not (output / "result.json").exists()
    assert load_json(output / "interrupted.json")["cleanup"] == "UNMEASURED"
    assert not lane.reads
    with pytest.raises(OwnershipError):
        lane.store.reserve_launch(lane.lease, "stimulus")


@pytest.mark.asyncio
async def test_changed_manifest_cannot_redirect_cleanup_after_adoption(lane, monkeypatch):
    await publish(lane)
    clock = [lane.store.clock()]
    lane.store.clock = lambda: clock[0]

    def changed(seconds):
        clock[0] += 301
        path = lane.root / "pool-input.json"
        value = load_json(path)
        value["run_id"] = "different-run"
        path.write_bytes(canonical(value))

    monkeypatch.setattr(watchdog.time, "sleep", changed)
    with pytest.raises(ValueError, match="changed while waiting"):
        watchdog.watch_prepared_cleanup(
            lane.root, lane.store, owner="backup", output=lane.root / "backup", supervised=True, repo=lane.repo
        )
    assert not lane.reads
    assert lane.store.snapshot()["ownership"]["cleanup"] == 1


@pytest.fixture
def prepared(checkout, tmp_path):
    from qa_lab.e2e_card import REPO, TEMPLATE, bind_pool_input, digest, prepare
    from qa_lab.e2e_ownership import OwnershipStore

    root = tmp_path / "prepared-run"
    prepare(checkout, REPO / TEMPLATE, root / "preparation", "prepared-backup-test")
    root.chmod(0o700)
    anchor = root / "anchor.json"
    anchor.write_bytes(canonical({"chain_id": 42161, "fork_block": 100, "fork_hash": "0x" + "ab" * 32}))
    bind_pool_input(root / "preparation", checkout, root / "subject", anchor, root / "pool-input.json")
    store = OwnershipStore(root / "ownership.sqlite")
    store.initialize(run_id="prepared-backup-test", card_hash=digest((root / "preparation/card.json").read_bytes()))

    return root, store, checkout


def test_standby_can_start_before_any_controller_or_worker(prepared, monkeypatch):
    root, store, checkout = prepared

    def stop(seconds):
        state = store.snapshot()
        assert state["ownership"]["generation"] == 0
        assert state["launches"] == []
        raise KeyboardInterrupt()

    monkeypatch.setattr(watchdog.time, "sleep", stop)
    with pytest.raises(KeyboardInterrupt):
        watchdog.watch_prepared_cleanup(
            root, store, owner="backup", output=root / "backup", supervised=True, repo=checkout
        )
    assert not (root / "backup/adopted.json").exists()
    assert not (root / "backup/result.json").exists()


def test_lost_readiness_reader_does_not_stop_cleanup_standby(prepared, monkeypatch):
    root, store, checkout = prepared
    reader, writer = os.pipe()
    os.close(reader)

    def stop(seconds):
        assert (root / "backup/controller-disconnected.json").is_file()
        assert store.snapshot()["ownership"]["generation"] == 0
        raise KeyboardInterrupt()

    monkeypatch.setattr(watchdog.time, "sleep", stop)
    with pytest.raises(KeyboardInterrupt):
        watchdog.watch_prepared_cleanup(
            root, store, owner="backup", output=root / "backup", supervised=True, repo=checkout, ready_fd=writer
        )
    assert not (root / "backup/result.json").exists()


def test_separate_standby_adopts_after_controller_process_exits(prepared, monkeypatch):
    root, store, repo = prepared
    monkeypatch.chdir(root)
    environment = {**os.environ, "PYTHONPATH": str(Path(__file__).resolve().parents[3])}
    watcher_code = """
import sys
from pathlib import Path
from qa_lab.e2e_ownership import OwnershipStore
from qa_lab.e2e_preboot_watchdog import watch_prepared_cleanup
root, repo = map(Path, sys.argv[1:3])
watch_prepared_cleanup(root, OwnershipStore(root / 'ownership.sqlite'), owner='backup-process',
                       output=root / 'backup', supervised=True, repo=repo, ready_fd=int(sys.argv[3]))
"""
    controller_code = """
import sys
from pathlib import Path
from qa_lab.e2e_ownership import OwnershipStore
store = OwnershipStore(Path(sys.argv[1]))
lease = store.acquire('controller-process', seconds=1)
store.reserve_launch(lease, 'subject')
"""
    with (root / "watcher-process.log").open("w") as log:
        reader, writer = os.pipe()
        watcher = subprocess.Popen(
            [sys.executable, "-c", watcher_code, str(root), str(repo), str(writer)],
            stdout=log,
            stderr=subprocess.STDOUT,
            pass_fds=(writer,),
            start_new_session=True,
            env=environment,
        )
        os.close(writer)
        try:
            ready = watchdog.wait_for_cleanup_ready(
                watcher,
                reader,
                run_id="prepared-backup-test",
                owner="backup-process",
                card_hash=store.snapshot()["ownership"]["card_hash"],
                timeout=15,
            )
            assert ready["pid"] == watcher.pid
            assert (root / "backup/started.json").exists()
            assert store.snapshot()["ownership"]["generation"] == 0
            controller = subprocess.run(
                [sys.executable, "-c", controller_code, str(store.path)],
                capture_output=True,
                timeout=10,
                env=environment,
            )
            assert controller.returncode == 0, controller.stderr.decode()
            deadline = time.monotonic() + 15
            while store.snapshot()["ownership"]["owner"] != "backup-process" and time.monotonic() < deadline:
                assert watcher.poll() is None
                time.sleep(0.05)
            state = store.snapshot()
            assert state["ownership"]["owner"] == "backup-process"
            assert state["ownership"]["generation"] == 2
            assert state["ownership"]["cleanup"] == 1
            assert len(state["launches"]) == 1 and state["launches"][0]["claimed"] == 0
            assert watcher.poll() is None
            assert load_json(root / "backup/started.json")["pid"] == watcher.pid != os.getpid()
            assert not (root / "backup/discovered.json").exists()
            assert not (root / "backup/result.json").exists()
        finally:
            os.close(reader)
            if watcher.poll() is None:
                watcher.terminate()
            watcher.wait(timeout=10)


@pytest.mark.parametrize("mode", ["silent", "wrong-run", "oversized", "closed"])
def test_readiness_refuses_unresponsive_or_unrelated_child(mode):
    reader, writer = os.pipe()
    code = """
import json, os, socket, sys, time
fd, mode = int(sys.argv[1]), sys.argv[2]
if mode == 'wrong-run':
    os.write(fd, (json.dumps({'scope': 'prepared_cleanup_standby', 'run_id': 'another-run',
        'card_sha256': 'hash', 'owner': 'backup', 'pid': os.getpid(),
        'hostname': socket.gethostname(), 'cleanup': 'UNMEASURED'}) + '\\n').encode())
elif mode == 'oversized':
    os.write(fd, b'x' * 4097)
elif mode == 'closed':
    os.close(fd)
time.sleep(10)
"""
    worker = subprocess.Popen([sys.executable, "-c", code, str(writer), mode], pass_fds=(writer,))
    os.close(writer)
    try:
        error = {"silent": TimeoutError, "wrong-run": ValueError, "oversized": ValueError, "closed": RuntimeError}[mode]
        with pytest.raises(error):
            watchdog.wait_for_cleanup_ready(
                worker, reader, run_id="expected-run", card_hash="hash", owner="backup", timeout=1
            )
    finally:
        os.close(reader)
        if worker.poll() is None:
            worker.terminate()
        worker.wait(timeout=10)
