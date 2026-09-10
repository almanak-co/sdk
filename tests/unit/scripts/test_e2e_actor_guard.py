import json
import sqlite3
import subprocess
import sys
from contextlib import closing
from types import SimpleNamespace

import pytest

from qa_lab import e2e_actor_guard as guard
from qa_lab.e2e_ownership import OwnershipError, OwnershipStore


@pytest.fixture
def context(tmp_path):
    (tmp_path / "actor").mkdir()
    return SimpleNamespace(root=tmp_path, require_owned=lambda path: path)


def request(context, status):
    with closing(sqlite3.connect(context.root / "actor/almanak_state.db")) as db:
        db.execute("CREATE TABLE teardown_requests (deployment_id TEXT, status TEXT)")
        identity = guard.resolve_deployment_id(wallet_address=guard.anvil_default_address(1), chain="arbitrum")
        db.execute("INSERT INTO teardown_requests VALUES (?, ?)", (identity, status))
        db.commit()


@pytest.mark.parametrize("status", [None, "running", "completed"])
def test_unfinished_or_completed_request_does_not_force_actor_exit(context, status):
    if status is not None:
        request(context, status)
    assert guard._stop_reason(context) is None


def test_failed_unwind_reaps_actual_child_without_claiming_position_closure(context):
    request(context, "failed")
    child = subprocess.Popen((sys.executable, "-c", "import time; time.sleep(60)"), start_new_session=True)
    try:
        assert guard.guard_actor(context, child) == 1
        assert child.poll() is not None
        result = json.loads((context.root / "actor-process-result.json").read_text())
        assert result["reason"] == "SDK_TEARDOWN_FAILED"
        assert result["child_reaped"] is True
        assert result["position_cleanup"] == "UNMEASURED"
    finally:
        guard.reap_child(child)


def test_invalid_release_retains_failure_and_reaps_actual_child(context):
    (context.root / "fork-shutdown.json").write_text("{}")
    child = subprocess.Popen((sys.executable, "-c", "import time; time.sleep(60)"), start_new_session=True)
    try:
        assert guard.guard_actor(context, child) == 1
        result = json.loads((context.root / "actor-process-result.json").read_text())
        assert result["reason"] == "GUARD_ERROR"
        assert result["error_type"] == "ValueError"
        assert child.poll() is not None
    finally:
        guard.reap_child(child)


def test_normal_sdk_exit_is_reaped_without_manufacturing_cleanup(context):
    child = subprocess.Popen((sys.executable, "-c", "pass"), start_new_session=True)
    try:
        child.wait(timeout=10)
        assert guard.guard_actor(context, child) == 0
        result = json.loads((context.root / "actor-process-result.json").read_text())
        assert result["reason"] == "SDK_EXITED"
        assert result["position_cleanup"] == "UNMEASURED"
    finally:
        guard.reap_child(child)


def test_claimed_spawn_is_single_use_and_returns_owned_child(context):
    store = OwnershipStore(context.root / "ownership.sqlite")
    store.initialize(run_id="actor-guard", card_hash="a" * 64)
    lease = store.acquire("controller")
    token = store.reserve_launch(lease, "stimulus")
    argv = (sys.executable, "-c", "pass")
    child = store.spawn_launch(lease, role="stimulus", token=token, argv=argv)
    try:
        assert child.wait(timeout=10) == 0
        with pytest.raises(OwnershipError):
            store.spawn_launch(lease, role="stimulus", token=token, argv=argv)
    finally:
        guard.reap_child(child)


def test_cleanup_prevents_spawn(context):
    store = OwnershipStore(context.root / "ownership.sqlite")
    store.initialize(run_id="actor-guard", card_hash="a" * 64)
    lease = store.acquire("controller")
    token = store.reserve_launch(lease, "stimulus")
    store.request_cleanup(lease)
    with pytest.raises(OwnershipError, match="cleanup"):
        store.spawn_launch(lease, role="stimulus", token=token, argv=("/does-not-exist",))


def test_failed_spawn_cannot_retry_ambiguous_launch(context):
    store = OwnershipStore(context.root / "ownership.sqlite")
    store.initialize(run_id="actor-guard", card_hash="a" * 64)
    lease = store.acquire("controller")
    token = store.reserve_launch(lease, "stimulus")
    with pytest.raises(OSError):
        store.spawn_launch(lease, role="stimulus", token=token, argv=("/does-not-exist",))
    with pytest.raises(OwnershipError):
        store.spawn_launch(lease, role="stimulus", token=token, argv=("/does-not-exist",))
