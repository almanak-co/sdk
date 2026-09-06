"""Offline controls: real OS termination, with no signing or blockchain harness."""

from __future__ import annotations

import argparse
import json
import os
import signal
import subprocess
import sys
import time
from pathlib import Path

import pytest

from qa_lab import mainnet_intent_coordinator as coordinator
from qa_lab import qa_interrupted_run as recovery
from qa_lab.mainnet_intent_recipe import AAVE_V3_ARBITRUM_SUPPLY_EOA, build_approval, build_run_plan


@pytest.fixture
def run_request(tmp_path, monkeypatch):
    monkeypatch.setattr(coordinator, "assert_mainnet_lane_enabled", lambda *args, **kwargs: None)
    monkeypatch.delenv("ALMANAK_QA_FORK_CONTEXT", raising=False)
    monkeypatch.setenv("ALMANAK_QA_STORE", str(tmp_path / "store"))
    directory = coordinator._request_dir("recovery-test")
    plan = build_run_plan(
        recipe=AAVE_V3_ARBITRUM_SUPPLY_EOA,
        funding_plan={"cell_id": AAVE_V3_ARBITRUM_SUPPLY_EOA.cell_id, "pool_index": 1, "wallet": "0x" + "11" * 20},
        git_sha="a" * 40,
        request_id="recovery-test",
    )
    payload = {
        **{k: plan[k] for k in ("request_id", "plan_sha256", "git_sha", "cell_id", "wallet", "pool_index")},
        "state": "QUEUED",
        "worktree": str(coordinator.REPO),
        "environment": str(tmp_path / "venv"),
        "execution_context": None,
    }
    recovery.write_json(directory / "request.json", payload)
    recovery.write_json(directory / "plan.json", plan)
    recovery.write_json(directory / "approval.json", build_approval(plan=plan, approver="offline-control"))
    return directory, payload


def _wait(predicate, timeout=10):
    until = time.monotonic() + timeout
    while time.monotonic() < until:
        if predicate():
            return
        time.sleep(0.025)
    raise AssertionError("Process control did not reach its durable checkpoint")


def _stopped(group):
    try:
        os.killpg(group, 0)
        return False
    except ProcessLookupError:
        return True
    except PermissionError:
        return False


def _launch(run_request, tmp_path, *, spawn_descendant=False):
    directory, payload = run_request
    python = Path(payload["environment"]) / "bin/python"
    python.parent.mkdir(parents=True)
    python.write_text(
        f"#!{sys.executable}\n"
        "import sys,time,json,subprocess\n"
        f"sys.path.insert(0,{str(coordinator.REPO)!r})\n"
        "from qa_lab import mainnet_intent_coordinator as c\n"
        "c.assert_mainnet_lane_enabled=lambda *a,**k: None\n"
        "def inert(d, *, operator_authorized=False):\n"
        " (d/'bundle').mkdir()\n"
        " (d/'bundle'/'approval-consumed.json').write_text('{\"consumed\":true}')\n"
        " (d/'executions').open('a').write('target-entry\\n')\n"
        + (
            " (d/'descendant').write_text(str(subprocess.Popen([sys.executable,'-c','import time;time.sleep(300)']).pid))\n"
            if spawn_descendant
            else ""
        )
        + " (d/'ready').write_text('ready')\n"
        " time.sleep(300)\n"
        " return 0\n"
        "c._execute_runner=inert\n"
        "sys.argv=sys.argv[2:]\n"
        "raise SystemExit(c.main())\n"
    )
    python.chmod(0o755)
    command = (
        "import argparse; from qa_lab import mainnet_intent_coordinator as c; "
        "c.assert_mainnet_lane_enabled=lambda *a,**k: None; "
        "c._git=lambda *a,**k: 'a'*40 if a[0]=='rev-parse' else ''; "
        "raise SystemExit(c.run(argparse.Namespace(request_id='recovery-test')))"
    )
    log = (tmp_path / "process.log").open("w")
    child = subprocess.Popen([sys.executable, "-c", command], cwd=coordinator.REPO, stdout=log, stderr=log)
    try:
        _wait(lambda: (directory / "ready").exists())
    except Exception:
        child.kill()
        child.wait()
        raise AssertionError((tmp_path / "process.log").read_text()) from None
    finally:
        log.close()
    return child


@pytest.mark.parametrize("kill_coordinator", [False, True])
def test_sigkill_then_reconcile_never_replays_target(run_request, tmp_path, monkeypatch, kill_coordinator):
    directory, _payload = run_request
    process = _launch(run_request, tmp_path)
    owner = coordinator._load(directory / "request.json")["execution_owner"]
    args = argparse.Namespace(request_id="recovery-test")
    monkeypatch.setattr(coordinator, "_validate_source", lambda request: coordinator.REPO)
    try:
        if kill_coordinator:
            process.kill()
            assert process.wait(timeout=10) == -signal.SIGKILL
            with pytest.raises(RuntimeError, match="lease is held"):
                coordinator.reconcile(args)
        os.kill(owner["pid"], signal.SIGKILL)
        if not kill_coordinator:
            process.wait(timeout=10)
        _wait(lambda: _stopped(owner["process_group"]))
        consumed = (directory / "bundle/approval-consumed.json").read_bytes()
        monkeypatch.setattr(recovery, "observe_chain", lambda plan, inventory: {"status": "OFFLINE_CONTROL_NO_CHAIN"})
        from qa_lab import run_mainnet_intent as runner
        from qa_lab.qa_failure_envelope import preserve_failure_bundle

        sealed = tmp_path / "sealed"

        def seal(*, output):
            preserve_failure_bundle(output, sealed)
            return str(sealed)

        monkeypatch.setattr(runner, "_seal_failure_record", seal)
        assert coordinator.reconcile(args) == 0
        final = coordinator._load(directory / "request.json")
        assert final["state"] == "RECONCILED_FAIL"
        assert final["result"]["overall"] == "FAIL"
        assert final["result"]["terminal_position_zero"] is None
        assert (directory / "executions").read_text() == "target-entry\n"
        assert (directory / "bundle/approval-consumed.json").read_bytes() == consumed
        assert (sealed / "reconciliation/process-ownership.json").is_file()
        assert (sealed / "reconciliation/observations.json").is_file()
        assert coordinator.reconcile(args) == 0
        with pytest.raises(RuntimeError, match="not QUEUED"):
            coordinator.run(args)
    finally:
        if process.poll() is None:
            process.kill()
            process.wait()
        try:
            os.killpg(owner["process_group"], signal.SIGKILL)
        except (ProcessLookupError, PermissionError):
            pass


def test_live_descendant_blocks_even_after_lease_owner_dies(run_request, monkeypatch):
    directory, payload = run_request
    with recovery.request_lease(directory):
        with pytest.raises(RuntimeError, match="lease is held"):
            with recovery.request_lease(directory):
                pass
    owner = {"host": recovery.platform.node(), "pid": 987654, "process_group": 987654, "session": 987654}
    monkeypatch.setattr(os, "killpg", lambda *args: None)
    with pytest.raises(RuntimeError, match="still exists"):
        recovery.assert_owner_stopped(owner)


def test_absent_or_foreign_owner_never_means_stopped():
    with pytest.raises(RuntimeError, match="ownership is missing"):
        recovery.assert_owner_stopped({})
    with pytest.raises(RuntimeError, match="another host"):
        recovery.assert_owner_stopped({"host": "foreign-host"})


def test_truncated_journal_keeps_known_hash_and_unknown_coverage(tmp_path):
    path = tmp_path / "transaction-journal/target.jsonl"
    path.parent.mkdir()
    tx_hash = "0x" + "aa" * 32
    path.write_text(json.dumps({"tx_hash": tx_hash}) + '\n{"tx_hash":')
    inventory = recovery.journal_inventory(tmp_path)
    assert inventory["hashes"] == [tx_hash]
    assert inventory["malformed_events"][0]["line"] == 2
    assert "never prove no broadcast" in inventory["coverage"]


def test_reconciliation_plan_tamper_refuses_before_observation(run_request, monkeypatch):
    directory, payload = run_request
    plan = coordinator._load(directory / "plan.json")
    plan["wallet"] = "0x" + "22" * 20
    recovery.write_json(directory / "plan.json", plan)
    with pytest.raises(ValueError, match="digest"):
        recovery.preserve_identity(directory, directory / "bundle", payload)


def test_request_path_traversal_is_rejected():
    with pytest.raises(ValueError, match="Invalid request"):
        coordinator._request_dir("../../outside")


@pytest.mark.parametrize("crash_point", ["after_rename", "after_ledger"])
def test_seal_retry_commits_once_without_reobserving(run_request, tmp_path, monkeypatch, crash_point):
    from qa_lab import qa_coverage as qa
    from qa_lab import run_mainnet_intent as runner
    from tests.unit.scripts.test_qa_coverage import TEST_SDK

    directory, payload = run_request
    payload.update({"state": "RUNNING", "execution_owner": {"pid": 123, "lease_token": "identity"}})
    monkeypatch.setattr(recovery, "observe_chain", lambda *args: {"offline": True})
    history = qa._load_history_module()
    monkeypatch.setattr(history, "provenance_from_worktree", lambda *args: dict(TEST_SDK))
    monkeypatch.setattr(qa, "render_lab", lambda **kwargs: None)
    store = tmp_path / "store"
    catalog = coordinator.REPO / "qa_lab/docs/catalog/v1/cells.yaml"
    append = history.append_experiment
    attempts = []

    def interrupted_append(**kwargs):
        attempts.append(kwargs["run_id"])
        if crash_point == "after_ledger":
            append(**kwargs)
        raise RuntimeError("injected interruption at ledger boundary")

    monkeypatch.setattr(history, "append_experiment", interrupted_append)
    monkeypatch.setattr(
        runner,
        "_seal_failure_record",
        lambda *, output: str(qa.seal_mainnet_intent_failure(store=store, catalog_path=catalog, bundle=output)),
    )
    with pytest.raises(RuntimeError, match="injected interruption"):
        recovery.reconcile_bundle(directory, payload)
    observation_path = directory / "bundle/reconciliation/observations.json"
    original = observation_path.read_bytes()
    monkeypatch.setattr(history, "append_experiment", append)
    monkeypatch.setattr(recovery, "observe_chain", lambda *args: pytest.fail("recovery must not rewrite observations"))
    result = recovery.reconcile_bundle(directory, payload)
    assert result["overall"] == "FAIL"
    assert observation_path.read_bytes() == original
    assert len(history.read_history(store)) == 1
    assert history.verify_history(store)["status"] == "PASS"
    assert recovery.reconcile_bundle(directory, payload)["seal_path"] == result["seal_path"]
    assert len(history.read_history(store)) == 1
    assert attempts == ["recovery-test-interrupted"]


def test_recovery_context_environment_excludes_operator_secrets(tmp_path, monkeypatch):
    import types

    root = tmp_path / "acceptance"
    context = types.SimpleNamespace(root=root, store=root / "store")
    monkeypatch.setitem(
        sys.modules, "qa_lab.qa_execution_context", types.SimpleNamespace(active_context=lambda: context)
    )
    monkeypatch.setenv("ALMANAK_QA_FORK_CONTEXT", str(tmp_path / "descriptor.json"))
    monkeypatch.setenv("ALMANAK_PRIVATE_KEY", "secret-must-not-propagate")
    monkeypatch.setenv("ARBITRUM_RPC_URL", "https://secret-must-not-propagate.example")
    monkeypatch.setenv("GATEWAY_AUTH_TOKEN", "secret-must-not-propagate")
    env = coordinator._clean_env(environment=root / "venv")
    assert "secret-must-not-propagate" not in json.dumps(env)
    assert env["HOME"] == str(root / "home")
    assert env["ALMANAK_QA_STORE"] == str(root / "store")


def test_interruption_before_attempt_marker_preserves_first_observations(run_request, tmp_path, monkeypatch):
    from qa_lab import run_mainnet_intent as runner

    directory, payload = run_request
    payload.update({"state": "RUNNING", "execution_owner": {"pid": 123, "lease_token": "identity"}})
    monkeypatch.setattr(recovery, "observe_chain", lambda *args: {"first_observation": True})
    write = recovery.write_json

    def interrupted_write(path, content):
        if path.name == "attempt.json":
            raise OSError("injected interruption before seal attempt marker")
        write(path, content)

    monkeypatch.setattr(recovery, "write_json", interrupted_write)
    with pytest.raises(OSError, match="injected interruption"):
        recovery.reconcile_bundle(directory, payload)
    original = (directory / "bundle/reconciliation/observations.json").read_bytes()
    monkeypatch.setattr(recovery, "write_json", write)
    monkeypatch.setattr(recovery, "observe_chain", lambda *args: pytest.fail("must preserve first observation"))
    monkeypatch.setattr(runner, "_seal_failure_record", lambda **kwargs: str(tmp_path / "sealed"))
    recovery.reconcile_bundle(directory, payload)
    assert (directory / "bundle/reconciliation/observations.json").read_bytes() == original


def test_changed_observation_refuses_seal_retry(run_request, monkeypatch):
    from qa_lab import run_mainnet_intent as runner

    directory, payload = run_request
    payload.update({"state": "RUNNING", "execution_owner": {"pid": 123, "lease_token": "identity"}})
    monkeypatch.setattr(recovery, "observe_chain", lambda *args: {"offline": True})

    def interrupt(**kwargs):
        raise OSError("seal interrupted")

    monkeypatch.setattr(runner, "_seal_failure_record", interrupt)
    with pytest.raises(OSError, match="seal interrupted"):
        recovery.reconcile_bundle(directory, payload)
    (directory / "bundle/reconciliation/observations.json").write_text("{}")
    with pytest.raises(ValueError, match="observations changed"):
        recovery.reconcile_bundle(directory, payload)


def test_surviving_funding_subprocess_blocks_reconcile_after_worker_sigkill(run_request, tmp_path, monkeypatch):
    directory, _payload = run_request
    process = _launch(run_request, tmp_path, spawn_descendant=True)
    owner = coordinator._load(directory / "request.json")["execution_owner"]
    descendant = int((directory / "descendant").read_text())
    monkeypatch.setattr(coordinator, "_validate_source", lambda request: coordinator.REPO)
    try:
        os.kill(owner["pid"], signal.SIGKILL)
        process.wait(timeout=10)
        with recovery.request_lease(directory):
            pass
        with pytest.raises(RuntimeError, match="process group still exists"):
            coordinator.reconcile(argparse.Namespace(request_id="recovery-test"))
    finally:
        if process.poll() is None:
            process.kill()
            process.wait()
        try:
            os.killpg(owner["process_group"], signal.SIGKILL)
        except (ProcessLookupError, PermissionError):
            pass
    _wait(lambda: _stopped(owner["process_group"]))
    assert descendant != owner["pid"]


@pytest.fixture
def serialized_zero_index_request(tmp_path, monkeypatch):
    from types import SimpleNamespace

    from qa_lab import qa_execution_context

    fixture = Path(__file__).parent / "fixtures/interrupted_pool_zero_identity.json"
    captured = json.loads(fixture.read_text())
    context_identity = captured["plan"]["execution_context"]
    context = SimpleNamespace(chain=context_identity["chain"], public_identity=lambda: context_identity)
    monkeypatch.setattr(qa_execution_context, "active_context", lambda: context)
    directory = tmp_path / "request"
    for name in ("plan", "approval", "request"):
        recovery.write_json(directory / f"{name}.json", captured[name])
    return directory, captured


def test_recovery_accepts_serialized_zero_pool_index(serialized_zero_index_request):
    directory, captured = serialized_zero_index_request
    original = {name: (directory / f"{name}.json").read_bytes() for name in ("plan", "approval", "request")}
    plan = recovery.preserve_identity(directory, directory / "bundle", captured["request"])
    assert plan["pool_index"] == 0
    assert type(plan["pool_index"]) is int
    for name in ("plan", "approval"):
        assert (directory / "bundle" / f"{name}.json").read_bytes() == original[name]
    assert (directory / "request.json").read_bytes() == original["request"]


@pytest.mark.parametrize("invalid", [None, False, True, "0", 0.0, -1, 1])
def test_recovery_rejects_absent_malformed_or_mismatched_pool_index(serialized_zero_index_request, invalid):
    directory, captured = serialized_zero_index_request
    captured["request"]["pool_index"] = invalid
    with pytest.raises(ValueError, match="pool_index"):
        recovery.preserve_identity(directory, directory / "bundle", captured["request"])
    assert not (directory / "bundle").exists()


def test_recovery_rejects_missing_pool_index(serialized_zero_index_request):
    directory, captured = serialized_zero_index_request
    del captured["request"]["pool_index"]
    with pytest.raises(ValueError, match="pool_index"):
        recovery.preserve_identity(directory, directory / "bundle", captured["request"])


@pytest.fixture
def recorded_fork_observation(tmp_path, monkeypatch):
    from types import SimpleNamespace
    from unittest.mock import Mock

    from hexbytes import HexBytes
    from web3 import Web3

    from qa_lab import chains as C

    captured = json.loads((Path(__file__).parent / "fixtures/interrupted_fork_observation.json").read_text())
    identity = captured["plan"]["execution_context"]
    root = tmp_path / "fork"
    root.mkdir(mode=0o700)
    descriptor = root / "context.json"
    rpc = "http://127.0.0.1:54321"
    recovery.write_json(descriptor, {**identity, "root": str(root), "rpc_url": rpc})
    monkeypatch.setenv("ALMANAK_QA_FORK_CONTEXT", str(descriptor))
    monkeypatch.setenv("ALMANAK_QA_STORE", str(root / "store"))
    outside = tmp_path / "execution-checkout"
    outside.mkdir()
    monkeypatch.chdir(outside)
    receipt = captured["receipt"]

    def block(number):
        if number == identity["fork_block"]:
            return {"number": number, "hash": HexBytes(identity["fork_hash"])}
        return {"number": receipt["blockNumber"], "hash": HexBytes(receipt["blockHash"])}

    rpc_client = SimpleNamespace(
        provider=SimpleNamespace(
            endpoint_uri=rpc,
            make_request=Mock(
                return_value={
                    "result": {
                        "instanceId": identity["instance_id"],
                        "forkedNetwork": {"forkBlockNumber": identity["fork_block"]},
                    }
                }
            ),
        ),
        to_checksum_address=Web3.to_checksum_address,
        eth=SimpleNamespace(
            chain_id=identity["chain_id"],
            get_block=Mock(side_effect=block),
            get_transaction_receipt=Mock(return_value=receipt),
            get_transaction=Mock(return_value=captured["transaction"]),
            get_balance=Mock(return_value=123),
            call=Mock(return_value=bytes(32)),
            get_transaction_count=Mock(return_value=2),
            send_raw_transaction=Mock(side_effect=AssertionError("reconciliation must never broadcast")),
        ),
    )
    monkeypatch.setattr(C, "load_env", lambda: {})
    monkeypatch.setattr(C, "make_w3", lambda *args, **kwargs: rpc_client)
    return captured, rpc_client, root, outside


def test_recorded_fork_observation_is_read_only_outside_context_root(recorded_fork_observation):
    captured, rpc_client, root, outside = recorded_fork_observation
    before = sorted(str(p) for p in root.rglob("*"))
    tx_hash = captured["receipt"]["transactionHash"]
    observed = recovery._observation(lambda: recovery.observe_chain(captured["plan"], {"hashes": [tx_hash]}))
    assert observed["status"] == "OBSERVED"
    assert observed["value"]["transactions"][0]["receipt"]["value"] == captured["receipt"]
    assert observed["value"]["transactions"][0]["transaction"]["value"] == captured["transaction"]
    assert observed["value"]["wallet_balances"]["status"] == "MEASURED"
    assert observed["value"]["wallet_balances"]["raw_balances"]["native"] == "123"
    assert sorted(str(p) for p in root.rglob("*")) == before
    assert list(outside.iterdir()) == []
    rpc_client.eth.send_raw_transaction.assert_not_called()


@pytest.mark.parametrize("mutation", ["endpoint", "chain", "instance"])
def test_observation_rejects_unbound_fork_rpc(recorded_fork_observation, mutation):
    captured, rpc_client, _root, _outside = recorded_fork_observation
    if mutation == "endpoint":
        rpc_client.provider.endpoint_uri = "http://127.0.0.1:54322"
    elif mutation == "chain":
        rpc_client.eth.chain_id = 1
    else:
        rpc_client.provider.make_request.return_value["result"]["instanceId"] = "another-instance"
    result = recovery._observation(lambda: recovery.observe_chain(captured["plan"], {"hashes": []}))
    assert result == {"status": "UNKNOWN", "error_type": "ValueError"}
    rpc_client.eth.get_transaction_receipt.assert_not_called()


@pytest.mark.parametrize("failing_setup", ["load_env", "make_w3"])
def test_rpc_setup_exit_still_seals_unknown_failure(run_request, monkeypatch, tmp_path, failing_setup):
    from qa_lab import chains as C
    from qa_lab import run_mainnet_intent as runner
    from qa_lab.qa_failure_envelope import preserve_failure_bundle

    directory, request = run_request
    request.update({"state": "RUNNING", "execution_owner": {"pid": 123, "lease_token": "identity"}})
    monkeypatch.setattr(C, "load_env", lambda: {})

    def unavailable(*args, **kwargs):
        raise SystemExit("FATAL: unavailable RPC with a credential-bearing diagnostic")

    monkeypatch.setattr(C, failing_setup, unavailable)
    sealed = tmp_path / "sealed"

    def seal(*, output):
        preserve_failure_bundle(output, sealed)
        return str(sealed)

    monkeypatch.setattr(runner, "_seal_failure_record", seal)
    result = recovery.reconcile_bundle(directory, request)
    assert result["overall"] == "FAIL"
    observations = json.loads((sealed / "reconciliation/observations.json").read_text())
    assert observations["chain_observations"] == {"status": "UNKNOWN", "error_type": "RuntimeError"}
    assert "credential-bearing" not in json.dumps(observations)


@pytest.mark.parametrize("overall", ["PASS", "FAIL"])
def test_completed_unsealed_result_preserves_recover_seal_inputs(run_request, monkeypatch, overall):
    from qa_lab import run_mainnet_intent as runner

    directory, request = run_request
    bundle = directory / "bundle"
    result = {"overall": overall, "target": "PASS", "cleanup": "PASS", "sweep": "PASS", "terminal_position_zero": True}
    recovery.write_json(bundle / "result.json", result)
    original = (bundle / "result.json").read_bytes()
    monkeypatch.setattr(
        runner, "_seal_failure_record", lambda **kwargs: pytest.fail("completed evidence must not be replaced")
    )
    monkeypatch.setattr(recovery, "observe_chain", lambda *args: pytest.fail("completed run needs no new observations"))
    with pytest.raises(RuntimeError, match="recover-seal"):
        recovery.reconcile_bundle(directory, request)
    assert (bundle / "result.json").read_bytes() == original
    assert not (bundle / "reconciliation").exists()


def test_completed_product_failure_keeps_measured_phases(run_request, monkeypatch):
    from qa_lab import run_mainnet_intent as runner

    directory, request = run_request
    original = {"overall": "FAIL", "target": "FAIL", "cleanup": "PASS", "sweep": "PASS", "terminal_position_zero": True}
    recovery.write_json(directory / "bundle/result.json", original)
    monkeypatch.setattr(runner, "_seal_failure_record", lambda **kwargs: "/sealed")
    monkeypatch.setattr(
        recovery, "observe_chain", lambda *args: pytest.fail("completed failure already has observations")
    )
    result = recovery.reconcile_bundle(directory, request)
    assert result == {**original, "seal_path": "/sealed"}


@pytest.mark.parametrize("raw", [b'{"overall":"FA', b"[1,2]", b'{"error":"bad \xff'])
def test_unreadable_result_preserved_before_interruption_seal(run_request, monkeypatch, raw):
    from qa_lab import run_mainnet_intent as runner
    from qa_lab.qa_failure_envelope import preserve_failure_bundle

    directory, request = run_request
    request.update(state="RUNNING", execution_owner={"pid": 123})
    bundle = directory / "bundle"
    bundle.mkdir()
    (bundle / "result.json").write_bytes(raw)
    monkeypatch.setattr(recovery, "observe_chain", lambda *args: {"observed": True})

    def seal(*, output):
        preserve_failure_bundle(output, directory / "sealed")
        return str(directory / "sealed")

    monkeypatch.setattr(runner, "_seal_failure_record", seal)
    result = recovery.reconcile_bundle(directory, request)
    assert result["overall"] == "FAIL"
    assert (bundle / "reconciliation/prior-result.raw").read_bytes() == raw
    observations = json.loads((directory / "sealed/reconciliation/observations.json").read_text())
    assert observations["prior_result"]["status"] == "UNREADABLE"
    assert observations["chain_observations"]["status"] == "OBSERVED"


def test_reconciliation_redacts_strings_without_corrupting_json(run_request, monkeypatch):
    from qa_lab import run_mainnet_intent as runner
    from qa_lab.qa_failure_envelope import preserve_failure_bundle

    directory, request = run_request
    request.update(state="RUNNING", execution_owner={"pid": 123})
    bundle = directory / "bundle"
    recovery.write_json(
        bundle / "result.json",
        {
            "error": 'RPC "https://provider.invalid/private-secret" failed',
            "nested": ["https://provider.invalid/private-secret"],
        },
    )
    monkeypatch.setattr(recovery, "observe_chain", lambda *args: {})

    def seal(*, output):
        preserve_failure_bundle(output, directory / "sealed")
        return str(directory / "sealed")

    monkeypatch.setattr(runner, "_seal_failure_record", seal)
    recovery.reconcile_bundle(directory, request)
    for path in (directory / "sealed").rglob("*.json"):
        json.loads(path.read_text())
        assert "private-secret" not in path.read_text()
    assert "private-secret" not in (directory / "sealed/reconciliation/prior-result.raw").read_text()


def test_atomic_money_result_write_never_publishes_partial_json(tmp_path, monkeypatch):
    from qa_lab.mainnet_intent_recipe import write_json

    path = tmp_path / "result.json"
    write_json(path, {"overall": "PASS"})
    original = path.read_bytes()

    def interrupted_dump(payload, stream, **kwargs):
        stream.write('{"overall":"FA')
        raise OSError("writer interrupted after first bytes")

    monkeypatch.setattr(recovery.json, "dump", interrupted_dump)
    with pytest.raises(OSError, match="writer interrupted"):
        write_json(path, {"overall": "FAIL"})
    assert path.read_bytes() == original


@pytest.mark.parametrize("tamper", [False, True])
def test_reconcile_adopts_completed_seal_only_when_ledger_valid(run_request, tmp_path, monkeypatch, tamper):
    from qa_lab import qa_coverage as qa
    from tests.unit.scripts.test_qa_coverage import TEST_SDK

    directory, request = run_request
    request.update(state="RUNNING", lease_token="owner", execution_owner={"lease_token": "owner"})
    recovery.write_json(directory / "request.json", request)
    bundle = directory / "bundle"
    recovery.preserve_identity(directory, bundle, request)
    result = {"overall": "FAIL", "target": "FAIL", "cleanup": "PASS", "sweep": "PASS", "terminal_position_zero": True}
    recovery.write_json(bundle / "result.json", result)
    history = qa._load_history_module()
    monkeypatch.setattr(history, "provenance_from_worktree", lambda *args: dict(TEST_SDK))
    monkeypatch.setattr(qa, "render_lab", lambda **kwargs: None)
    target = qa.seal_mainnet_intent_failure(
        store=tmp_path / "store", catalog_path=coordinator.REPO / "qa_lab/docs/catalog/v1/cells.yaml", bundle=bundle
    )
    result["seal_path"] = str(target)
    recovery.write_json(bundle / "result.json", result)
    monkeypatch.setattr(coordinator, "_validate_source", lambda request: coordinator.REPO)
    monkeypatch.setattr(recovery, "assert_owner_stopped", lambda owner: None)
    monkeypatch.setattr(
        recovery, "observe_chain", lambda *args: pytest.fail("sealed child must not replay observations")
    )
    if tamper:
        (target / "result.json").write_text("{}")
        with pytest.raises(ValueError):
            coordinator.reconcile(argparse.Namespace(request_id=request["request_id"]))
        assert json.loads((directory / "request.json").read_text())["state"] == "RUNNING"
    else:
        assert coordinator.reconcile(argparse.Namespace(request_id=request["request_id"])) == 0
        assert json.loads((directory / "request.json").read_text())["state"] == "SEALED"
        assert len(history.read_history(tmp_path / "store")) == 1


@pytest.mark.parametrize("scheme", ["https", "HTTPS", "HtTp"])
def test_structured_diagnostics_redact_mixed_case_url_schemes(scheme):
    from qa_lab.qa_external_provenance import redact_json

    value = {"error": f'RPC "{scheme}://user:secret@provider.invalid/private?key=secret" failed'}
    redacted = redact_json(value)
    assert "secret" not in json.dumps(redacted)
    assert "provider.invalid" in redacted["error"]
    assert json.loads(json.dumps(redacted)) == redacted
