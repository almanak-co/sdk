"""Offline cancellation with real pool files and competing request leases."""

import argparse
from threading import Event, Thread

import pytest

from qa_lab import chains as C
from qa_lab import mainnet_intent_coordinator as coordinator
from qa_lab import qa_request_cancellation as cancellation
from qa_lab.qa_interrupted_run import write_json


@pytest.fixture(autouse=True)
def authorized_control_plane(monkeypatch):
    monkeypatch.setattr(coordinator, "assert_mainnet_lane_enabled", lambda *args, **kwargs: None)


@pytest.fixture
def unapproved(tmp_path, monkeypatch):
    monkeypatch.delenv("ALMANAK_QA_FORK_CONTEXT", raising=False)
    monkeypatch.setenv("ALMANAK_QA_STORE", str(tmp_path / "store"))
    monkeypatch.setattr(C, "POOL_FILE", tmp_path / "pool.json")
    C.save_pool({"wallets": [{"index": 0, "address": "0x" + "11" * 20, "private_key": "test-only", "funded": False}]})
    row = C.reserve_pool_entry(request_id="cancel-test", cell_id="cell", git_sha="a" * 40)
    directory = coordinator._request_dir("cancel-test")
    request = {
        "request_id": "cancel-test",
        "state": "AWAITING_APPROVAL",
        "cell_id": "cell",
        "git_sha": "a" * 40,
        "plan_sha256": "b" * 64,
        "pool_index": 0,
        "wallet": row["address"],
        "worktree": str(tmp_path),
        "environment": str(tmp_path / "venv"),
        "execution_context": None,
    }
    write_json(directory / "request.json", request)
    write_json(directory / "plan.json", {"plan_sha256": request["plan_sha256"]})
    return directory, argparse.Namespace(
        request_id="cancel-test", operator="qa-owner", approver="qa-owner", plan_sha256="b" * 64
    )


def test_cancel_releases_owned_unfunded_and_terminal_retry_ignores_new_owner(unapproved):
    directory, args = unapproved
    assert coordinator.cancel(args) == 0
    request = coordinator._load(directory / "request.json")
    assert request["state"] == "CANCELLED"
    assert request["cancellation"]["status"] == "RELEASED_UNFUNDED_RESERVATION"
    pool = C.load_pool()
    assert pool["wallets"][0].get("reserved_by") is None
    assert pool["wallets"][0]["private_key"] == "test-only"
    assert pool["pending_reservation_cancellations"] == {}
    C.reserve_pool_entry(request_id="new-owner", cell_id="new-cell", git_sha="c" * 40)
    before = C.POOL_FILE.read_bytes()
    assert coordinator.cancel(args) == 0
    assert C.POOL_FILE.read_bytes() == before
    with pytest.raises(RuntimeError, match="exact awaiting"):
        coordinator.approve(args)
    with pytest.raises(RuntimeError, match="not QUEUED"):
        coordinator.run(args)


@pytest.mark.parametrize(
    "change", [{"funded": True}, {"funded": None}, {"reserved_by": "another"}, {"address": "changed"}]
)
def test_cancel_refuses_funded_foreign_or_changed_wallet(unapproved, change):
    directory, args = unapproved
    pool = C.load_pool()
    pool["wallets"][0].update(change)
    C.save_pool(pool)
    before = C.POOL_FILE.read_bytes()
    with pytest.raises(ValueError, match="Cancellation refuses"):
        coordinator.cancel(args)
    assert C.POOL_FILE.read_bytes() == before
    assert coordinator._load(directory / "request.json")["state"] == "AWAITING_APPROVAL"


@pytest.mark.parametrize("state", ["RUNNING", "FAILED_DISPATCH", "SEALED"])
def test_cancel_refuses_approved_or_executed_states(unapproved, state):
    directory, args = unapproved
    request = coordinator._load(directory / "request.json")
    request["state"] = state
    write_json(directory / "request.json", request)
    with pytest.raises(
        RuntimeError,
        match="bound approval"
        if state == "FAILED_DISPATCH"
        else "ownership is missing"
        if state == "SEALED"
        else "Only an unapproved",
    ):
        coordinator.cancel(args)
    assert C.pool_entry(0)["reserved_by"] == "cancel-test"


def test_partial_approval_artifact_refuses_cancel(unapproved):
    directory, args = unapproved
    write_json(directory / "approval.json", {"already_approved": True})
    with pytest.raises(RuntimeError, match="approval artifact"):
        coordinator.cancel(args)
    assert C.pool_entry(0)["reserved_by"] == "cancel-test"


def test_cancel_crash_after_pool_release_then_new_allocation_resumes(unapproved, monkeypatch):
    directory, args = unapproved
    writer = cancellation.write_json

    def crash(path, value):
        if value.get("state") == "CANCELLED":
            raise OSError("simulated crash after pool commit")
        writer(path, value)

    monkeypatch.setattr(cancellation, "write_json", crash)
    with pytest.raises(OSError, match="simulated crash"):
        coordinator.cancel(args)
    assert coordinator._load(directory / "request.json")["state"] == "CANCELLING"
    pool = C.load_pool()
    assert pool["wallets"][0].get("reserved_by") is None
    assert "cancel-test" in pool["pending_reservation_cancellations"]
    C.reserve_pool_entry(request_id="new-owner", cell_id="new-cell", git_sha="c" * 40)
    pool = C.load_pool()
    pool["wallets"][0]["funded"] = True
    C.save_pool(pool)
    new_row = dict(C.pool_entry(0))
    monkeypatch.setattr(cancellation, "write_json", writer)
    assert coordinator.cancel(args) == 0
    assert C.pool_entry(0) == new_row
    assert C.load_pool()["pending_reservation_cancellations"] == {}
    assert coordinator._load(directory / "request.json")["state"] == "CANCELLED"


def test_cancel_crash_before_pool_commit_retains_barrier_and_owned_reservation(unapproved, monkeypatch):
    directory, args = unapproved
    save = C.save_pool
    monkeypatch.setattr(C, "save_pool", lambda pool: (_ for _ in ()).throw(OSError("pool persistence failure")))
    with pytest.raises(OSError, match="pool persistence failure"):
        coordinator.cancel(args)
    assert coordinator._load(directory / "request.json")["state"] == "CANCELLING"
    assert C.pool_entry(0)["reserved_by"] == "cancel-test"
    with pytest.raises(RuntimeError, match="exact awaiting"):
        coordinator.approve(args)
    monkeypatch.setattr(C, "save_pool", save)
    assert coordinator.cancel(args) == 0


def test_approve_and_cancel_share_exclusive_request_lease(unapproved, monkeypatch):
    directory, args = unapproved
    entered, release = Event(), Event()
    errors = []
    monkeypatch.setattr(coordinator, "_clean_env", lambda **kwargs: {})

    def approving(*args, **kwargs):
        entered.set()
        assert release.wait(5)
        write_json(directory / "approval.json", {"approved": True})
        return ""

    monkeypatch.setattr(coordinator, "_run", approving)

    def execute():
        try:
            coordinator.approve(args)
        except BaseException as exc:
            errors.append(exc)

    thread = Thread(target=execute)
    thread.start()
    try:
        assert entered.wait(5)
        with pytest.raises(RuntimeError, match="lease is held"):
            coordinator.cancel(args)
    finally:
        release.set()
        thread.join(5)
    assert errors == []
    assert coordinator._load(directory / "request.json")["state"] == "QUEUED"
    assert C.pool_entry(0)["reserved_by"] == "cancel-test"


def test_terminal_cancellation_retries_receipt_cleanup_after_reallocation(unapproved, monkeypatch):
    directory, args = unapproved
    save = C.save_pool
    writes = 0

    def cleanup_failure(pool):
        nonlocal writes
        writes += 1
        if writes == 2:
            raise OSError("pending receipt cleanup interrupted")
        save(pool)

    monkeypatch.setattr(C, "save_pool", cleanup_failure)
    with pytest.raises(OSError, match="cleanup interrupted"):
        coordinator.cancel(args)
    assert coordinator._load(directory / "request.json")["state"] == "CANCELLED"
    monkeypatch.setattr(C, "save_pool", save)
    C.reserve_pool_entry(request_id="new-owner", cell_id="new-cell", git_sha="c" * 40)
    new_row = dict(C.pool_entry(0))
    assert coordinator.cancel(args) == 0
    assert C.pool_entry(0) == new_row
    assert C.load_pool()["pending_reservation_cancellations"] == {}


@pytest.fixture
def failed_unfunded(unapproved, monkeypatch):
    import platform

    from qa_lab import qa_interrupted_run as recovery
    from qa_lab.mainnet_approval_ledger import consume_approval
    from qa_lab.mainnet_intent_recipe import AAVE_V3_ARBITRUM_SUPPLY_EOA, build_approval, build_run_plan

    directory, args = unapproved
    request = coordinator._load(directory / "request.json")
    funding = {"cell_id": AAVE_V3_ARBITRUM_SUPPLY_EOA.cell_id, "pool_index": 0, "wallet": request["wallet"]}
    plan = build_run_plan(
        recipe=AAVE_V3_ARBITRUM_SUPPLY_EOA,
        funding_plan=funding,
        git_sha=request["git_sha"],
        request_id=request["request_id"],
    )
    approval = build_approval(plan=plan, approver="operator")
    request.update(
        state="FAILED_DISPATCH",
        plan_sha256=plan["plan_sha256"],
        cell_id=plan["cell_id"],
        execution_owner={"host": platform.node(), "pid": 987654, "session": 987654, "process_group": 987654},
    )
    pool = C.load_pool()
    pool["wallets"][0]["reserved_cell_id"] = plan["cell_id"]
    C.save_pool(pool)
    write_json(directory / "request.json", request)
    write_json(directory / "plan.json", plan)
    write_json(directory / "approval.json", approval)
    consume_approval(
        path=C.POOL_FILE.parent / "approvals-consumed.jsonl",
        approval=approval,
        bundle=(directory / "bundle").resolve(),
        lock=C.file_lock,
    )

    def stopped(*args):
        raise ProcessLookupError()

    monkeypatch.setattr(recovery.os, "killpg", stopped)
    return directory, args


def test_cancel_failed_dispatch_only_when_consumed_and_never_claimed(failed_unfunded):
    directory, args = failed_unfunded
    assert coordinator.cancel(args) == 0
    assert C.pool_entry(0)["funded"] is False
    assert not C.pool_entry(0).get("reserved_by")
    C.reserve_pool_entry(request_id="next", cell_id="next", git_sha="c" * 40)
    before = C.POOL_FILE.read_bytes()
    assert coordinator.cancel(args) == 0
    assert C.POOL_FILE.read_bytes() == before
    assert coordinator._load(directory / "request.json")["cancellation_origin"] == "CONSUMED_UNFUNDED"


@pytest.mark.parametrize("defect", ["funded", "live", "foreign"])
def test_failed_dispatch_cancellation_refuses_missing_safety_proof(failed_unfunded, monkeypatch, defect):
    from qa_lab import qa_interrupted_run as recovery

    directory, args = failed_unfunded
    if defect == "live":
        monkeypatch.setattr(recovery.os, "killpg", lambda *args: None)
    else:
        pool = C.load_pool()
        pool["wallets"][0].update({"funded": True} if defect == "funded" else {"reserved_by": "foreign"})
        C.save_pool(pool)
    before = C.POOL_FILE.read_bytes()
    with pytest.raises((ValueError, RuntimeError)):
        coordinator.cancel(args)
    assert C.POOL_FILE.read_bytes() == before
    assert coordinator._load(directory / "request.json")["state"] == "FAILED_DISPATCH"


@pytest.mark.parametrize("state", ["SEALED", "QUEUED", "QUEUED_EXPIRED"])
def test_cancel_terminal_or_queued_unfunded_reservation(failed_unfunded, state):
    directory, args = failed_unfunded
    request = coordinator._load(directory / "request.json")
    request["state"] = state.split("_")[0]
    if state.startswith("QUEUED"):
        request.pop("execution_owner")
        (C.POOL_FILE.parent / "approvals-consumed.jsonl").unlink()
        if state == "QUEUED_EXPIRED":
            from qa_lab.mainnet_intent_recipe import build_approval

            plan = coordinator._load(directory / "plan.json")
            write_json(
                directory / "approval.json",
                build_approval(plan=plan, approver="operator", approved_at="2020-01-01T00:00:00Z"),
            )
    write_json(directory / "request.json", request)
    assert coordinator.cancel(args) == 0
    assert C.pool_entry(0).get("reserved_by") is None
    assert coordinator._load(directory / "request.json")["state"] == "CANCELLED"
    with pytest.raises(RuntimeError, match="not QUEUED"):
        coordinator.run(args)
    C.reserve_pool_entry(request_id="new", cell_id="new", git_sha="c" * 40)
    original = C.POOL_FILE.read_bytes()
    assert coordinator.cancel(args) == 0
    assert C.POOL_FILE.read_bytes() == original


def test_cancel_blocked_preparation_without_inventing_a_plan(unapproved):
    directory, args = unapproved
    request = coordinator._load(directory / "request.json")
    request["state"] = "BLOCKED"
    request.pop("plan_sha256")
    (directory / "plan.json").unlink()
    write_json(directory / "request.json", request)
    assert coordinator.cancel(args) == 0
    assert C.pool_entry(0).get("reserved_by") is None
    cancelled = coordinator._load(directory / "request.json")
    assert cancelled["cancellation"]["identity"]["plan_sha256"] is None
    assert coordinator.cancel(args) == 0


@pytest.mark.parametrize("registered", [False, True])
def test_cancel_failed_dispatch_before_consumption_is_fenced_and_reusable(failed_unfunded, registered):
    directory, args = failed_unfunded
    ledger = C.POOL_FILE.parent / "approvals-consumed.jsonl"
    ledger.unlink()
    request = coordinator._load(directory / "request.json")
    if not registered:
        request.pop("execution_owner")
    write_json(directory / "request.json", request)
    assert coordinator.cancel(args) == 0
    assert not ledger.exists()
    cancelled = coordinator._load(directory / "request.json")
    assert cancelled["cancellation_origin"] == "FENCED_UNFUNDED"
    assert C.pool_entry(0).get("reserved_by") is None
    with pytest.raises(RuntimeError, match="not QUEUED"):
        coordinator.run(args)
    C.reserve_pool_entry(request_id="next", cell_id="next", git_sha="c" * 40)
    original = C.POOL_FILE.read_bytes()
    assert coordinator.cancel(args) == 0
    assert C.POOL_FILE.read_bytes() == original


def test_cancel_refuses_consumption_bound_to_another_bundle(failed_unfunded):
    from qa_lab.mainnet_approval_ledger import consume_approval

    directory, args = failed_unfunded
    ledger = C.POOL_FILE.parent / "approvals-consumed.jsonl"
    ledger.unlink()
    consume_approval(
        path=ledger,
        approval=coordinator._load(directory / "approval.json"),
        bundle=directory / "foreign-bundle",
        lock=C.file_lock,
    )
    original = C.POOL_FILE.read_bytes()
    with pytest.raises(ValueError, match="another execution bundle"):
        coordinator.cancel(args)
    assert C.POOL_FILE.read_bytes() == original
