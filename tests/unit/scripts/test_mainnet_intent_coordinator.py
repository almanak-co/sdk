from __future__ import annotations

import json
from pathlib import Path

import pytest

from qa_lab import chains
from qa_lab import mainnet_intent_coordinator as coordinator
from qa_lab.mainnet_intent_recipe import AAVE_V3_ARBITRUM_SUPPLY_EOA, build_run_plan, verify_run_plan


def _pool(path: Path) -> None:
    path.write_text(
        json.dumps(
            {
                "wallets": [
                    {"index": 1, "address": "0x" + "11" * 20, "funded": False},
                    {"index": 2, "address": "0x" + "22" * 20, "funded": False},
                ]
            }
        )
    )


def test_reservation_is_atomic_owned_and_releasable(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    pool = tmp_path / "pool.json"
    _pool(pool)
    monkeypatch.setattr(chains, "POOL_FILE", pool)

    first = chains.reserve_pool_entry(request_id="req-1", cell_id="cell-1", git_sha="a" * 40)
    second = chains.reserve_pool_entry(request_id="req-2", cell_id="cell-2", git_sha="a" * 40)

    assert first["index"] == 1
    assert second["index"] == 2
    chains.assert_pool_reservation(index=1, request_id="req-1", address=first["address"])
    with pytest.raises(SystemExit, match="not reserved"):
        chains.assert_pool_reservation(index=1, request_id="req-2", address=first["address"])
    chains.release_pool_reservation(index=1, request_id="req-1")
    assert chains.pool_entry(1).get("reserved_by") is None


def test_plan_digest_binds_request_and_execution_attestation(monkeypatch) -> None:
    monkeypatch.delenv("ALMANAK_QA_FORK_CONTEXT", raising=False)
    funding = {
        "cell_id": AAVE_V3_ARBITRUM_SUPPLY_EOA.cell_id,
        "pool_index": 1,
        "wallet": "0x" + "11" * 20,
    }
    plan = build_run_plan(
        recipe=AAVE_V3_ARBITRUM_SUPPLY_EOA,
        funding_plan=funding,
        git_sha="a" * 40,
        request_id="req-1",
        execution_attestation={"git_tree": "b" * 40},
    )
    verify_run_plan(plan)
    plan["request_id"] = "req-2"
    with pytest.raises(ValueError, match="digest"):
        verify_run_plan(plan)


def test_source_digest_hashes_symlink_identity_without_dereferencing(tmp_path: Path, monkeypatch) -> None:
    (tmp_path / "target").mkdir()
    (tmp_path / "link").symlink_to("target")
    result = type("Result", (), {"stdout": b"link\0"})()
    monkeypatch.setattr(coordinator.subprocess, "run", lambda *args, **kwargs: result)

    first = coordinator._source_digest(tmp_path)
    (tmp_path / "target" / "secret").write_text("must not affect symlink identity")

    assert coordinator._source_digest(tmp_path) == first


def test_prepare_failure_preserves_child_reservation(tmp_path, monkeypatch):
    from argparse import Namespace

    directory = tmp_path / "request"
    monkeypatch.setattr(coordinator, "assert_mainnet_lane_enabled", lambda *a, **k: None)
    monkeypatch.setattr(coordinator, "_git", lambda *a, **k: "a" * 40)
    monkeypatch.setattr(coordinator, "_bootstrap_is_committed", lambda *a: None)
    monkeypatch.setattr(coordinator, "_request_dir", lambda *a: directory)
    monkeypatch.setattr(coordinator, "_operator_changes", lambda: [])
    monkeypatch.setattr(coordinator, "_context_identity", lambda: None)
    monkeypatch.setattr(coordinator, "_clean_env", lambda **k: {})
    monkeypatch.setattr(coordinator.tempfile, "mkdtemp", lambda **k: str(tmp_path / "runtime"))

    def child(*a, **k):
        path = directory / "request.json"
        state = coordinator._load(path)
        state.update(pool_index=7, wallet="0xreserved", plan_sha256="b" * 64)
        coordinator._write(path, state)
        return Namespace(returncode=-9)

    monkeypatch.setattr(coordinator.subprocess, "run", child)
    with pytest.raises(RuntimeError, match="exit -9"):
        coordinator.prepare(Namespace(cell_id="cell", requester="operator"))
    state = coordinator._load(directory / "request.json")
    assert state["state"] == "BLOCKED"
    assert (state["pool_index"], state["wallet"], state["plan_sha256"]) == (7, "0xreserved", "b" * 64)


@pytest.mark.parametrize(
    "mutation",
    [
        "none",
        "unregistered",
        "missing_attestation",
        "wrong_request",
        "wrong_plan",
        "wrong_approval",
        "wrong_attestation",
        "wrong_environment",
        "foreign_owner",
        "wrong_reservation",
        "funded",
        "wrong_output",
    ],
)
def test_live_execution_requires_real_owned_worker_and_bound_artifacts(tmp_path, mutation):
    import os
    import subprocess
    import sys
    import textwrap

    script = textwrap.dedent("""
        import json, os, sys
        from pathlib import Path
        from qa_lab import mainnet_intent_coordinator as c
        from qa_lab import run_mainnet_intent as r
        from qa_lab.mainnet_intent_recipe import AAVE_V3_ARBITRUM_SUPPLY_EOA, build_run_plan, build_approval
        from qa_lab.qa_interrupted_run import request_lease, owned_execution, process_owner, write_json
        mutation = sys.argv[1]
        request_id = "owned-worker-test"
        directory = c._request_dir(request_id)
        attestation = {"artifact_kind": "almanak.mainnet_intent_execution_attestation"}
        funding = {"cell_id": AAVE_V3_ARBITRUM_SUPPLY_EOA.cell_id, "pool_index": 1, "wallet": "0x" + "11" * 20}
        plan = build_run_plan(recipe=AAVE_V3_ARBITRUM_SUPPLY_EOA, funding_plan=funding, git_sha="a" * 40, request_id=request_id, execution_attestation=attestation)
        plan = json.loads(json.dumps(plan))
        approval = build_approval(plan=plan, approver="offline-control")
        request = {k: plan[k] for k in ("request_id", "plan_sha256", "git_sha", "cell_id", "wallet", "pool_index")}
        request.update(state="RUNNING", execution_context=None, worktree=str(r.REPO), environment=sys.prefix, lease_token="owned", execution_owner=process_owner("owned"))
        pool = {"address": plan["wallet"], "funded": False, "reserved_by": request_id, "reserved_cell_id": plan["cell_id"], "reserved_git_sha": plan["git_sha"]}
        r.C.pool_entry = lambda index: pool
        output = directory / "bundle"
        if mutation == "wrong_request": request["request_id"] = "other"
        if mutation == "wrong_environment": request["environment"] = "/tmp/foreign-venv"
        if mutation == "foreign_owner": request["execution_owner"]["pid"] += 1
        if mutation == "wrong_reservation": pool["reserved_cell_id"] = "other"
        if mutation == "funded": pool["funded"] = True
        if mutation == "wrong_output": output = directory / "other-bundle"
        write_json(directory / "request.json", request)
        write_json(directory / "plan.json", plan)
        write_json(directory / "approval.json", approval)
        write_json(directory / "execution-attestation.json", attestation)
        if mutation == "missing_attestation": plan.pop("execution_attestation")
        if mutation == "wrong_plan": write_json(directory / "plan.json", {**plan, "wallet": "other"})
        if mutation == "wrong_approval": write_json(directory / "approval.json", {**approval, "approver": "other"})
        if mutation == "wrong_attestation": write_json(directory / "execution-attestation.json", {"artifact_kind": "other"})
        def check():
            r._assert_coordinator_execution(plan=plan, approval=approval, plan_path=directory / "plan.json", approval_path=directory / "approval.json", output=output)
        try:
            with request_lease(directory) as descriptor:
                if mutation == "unregistered": check()
                else:
                    with owned_execution(directory, descriptor): check()
        except (ValueError, RuntimeError):
            assert mutation != "none"
        else:
            assert mutation == "none"
        assert not output.exists()
        print("PASS")
    """)
    env = {**os.environ, "ALMANAK_QA_STORE": str(tmp_path / "store"), "PYTHONPATH": str(coordinator.REPO)}
    env.pop("ALMANAK_QA_FORK_CONTEXT", None)
    result = subprocess.run(
        [sys.executable, "-c", script, mutation], env=env, capture_output=True, text=True, start_new_session=True
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "PASS"


def test_standalone_live_run_refuses_before_rpc_bundle_or_nonce(tmp_path, monkeypatch):
    import asyncio

    from qa_lab import run_mainnet_intent as runner
    from qa_lab.mainnet_intent_recipe import build_approval

    monkeypatch.setattr(runner, "assert_mainnet_lane_enabled", lambda *args, **kwargs: None)
    monkeypatch.setattr(runner, "active_context", lambda: None)
    monkeypatch.setattr(runner, "_git_sha", lambda: "a" * 40)
    monkeypatch.setattr(runner.C, "load_env", lambda: pytest.fail("must refuse before RPC setup"))
    monkeypatch.setattr(
        runner, "consume_approval", lambda **kwargs: pytest.fail("must refuse before consuming approval")
    )
    plan = build_run_plan(
        recipe=AAVE_V3_ARBITRUM_SUPPLY_EOA,
        funding_plan={"cell_id": AAVE_V3_ARBITRUM_SUPPLY_EOA.cell_id, "pool_index": 1, "wallet": "0x" + "11" * 20},
        git_sha="a" * 40,
    )
    (tmp_path / "plan.json").write_text(json.dumps(plan))
    (tmp_path / "approval.json").write_text(json.dumps(build_approval(plan=plan, approver="offline-control")))
    with pytest.raises(ValueError, match="coordinator request_id"):
        asyncio.run(
            runner.execute_plan(
                plan_path=tmp_path / "plan.json", approval_path=tmp_path / "approval.json", output=tmp_path / "bundle"
            )
        )
    assert not (tmp_path / "bundle").exists()


def test_fork_acceptance_does_not_require_live_coordinator_admission(monkeypatch):
    from qa_lab import run_mainnet_intent as runner

    monkeypatch.setattr(runner, "active_context", lambda: object())
    runner._assert_coordinator_execution(
        plan={}, approval={}, plan_path=Path("unused"), approval_path=Path("unused"), output=Path("unused")
    )


@pytest.mark.parametrize(
    "ineligible",
    [{"safe_owner_for": "20a"}, {"safe": {"id": "20a"}}, {"held": True}, {"quarantined": True}, {"funded": None}],
)
def test_allocator_skips_delegated_held_or_unmeasured_wallets(tmp_path, monkeypatch, ineligible):
    pool = tmp_path / "pool.json"
    held = {"index": 20, "address": "0x" + "11" * 20, "funded": False, **ineligible}
    free = {"index": 21, "address": "0x" + "22" * 20, "funded": False}
    pool.write_text(json.dumps({"wallets": [held, free]}))
    monkeypatch.setattr(chains, "POOL_FILE", pool)
    reserved = chains.reserve_pool_entry(request_id="request", cell_id="cell", git_sha="a" * 40)
    assert reserved["index"] == 21
    assert chains.pool_entry(20) == held
