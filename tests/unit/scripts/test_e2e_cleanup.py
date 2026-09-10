import json
import sqlite3
import subprocess
from pathlib import Path
from types import SimpleNamespace

import pytest

from almanak.framework.runner.identity import resolve_deployment_id
from qa_lab import e2e_cleanup as cleanup
from qa_lab.e2e_card import digest
from qa_lab.e2e_ownership import OwnershipError, OwnershipStore

FIXTURES = Path(__file__).resolve().parents[2] / "fixtures/accounting/harness/lp-dual-generations"


@pytest.fixture
def run(tmp_path, monkeypatch):
    opened = json.loads((FIXTURES / "positions-open.json").read_text())
    terminal = json.loads((FIXTURES / "positions-terminal.json").read_text())
    wallet = opened["wallet"]
    identity = resolve_deployment_id(wallet_address=wallet, chain="arbitrum")
    subject = tmp_path / "subject"
    subject.mkdir()
    database = subject / "almanak_state.db"
    with sqlite3.connect(database) as db:
        db.execute("CREATE TABLE strategy_state (deployment_id TEXT)")
        db.execute("INSERT INTO strategy_state VALUES (?)", (identity,))
        db.execute(
            "CREATE TABLE teardown_requests (deployment_id TEXT, status TEXT, positions_failed INTEGER, requested_at TEXT, acknowledged_at TEXT, started_at TEXT, completed_at TEXT)"
        )
    (tmp_path / "preparation").mkdir()
    card = tmp_path / "preparation/card.json"
    card.write_text(json.dumps({"run_id": "cleanup"}))
    store = OwnershipStore(tmp_path / "ownership.sqlite")
    store.initialize(run_id="cleanup", card_hash=digest(card.read_bytes()))
    lease = store.acquire("controller", seconds=300)
    token = store.reserve_launch(lease, "subject")
    store.claim_launch(lease, role="subject", token=token)
    store.publish_phase(lease, "open", json.dumps(opened).encode())
    context = SimpleNamespace(
        root=tmp_path,
        network="anvil",
        chain="arbitrum",
        fork_block=1,
        require_owned=lambda path: path,
        assert_rpc_identity=lambda: None,
    )
    monkeypatch.setattr(cleanup, "capture_position_generations", lambda *args: terminal)
    return SimpleNamespace(
        context=context,
        store=store,
        lease=lease,
        wallet=wallet,
        identity=identity,
        database=database,
        opened=opened,
        terminal=terminal,
    )


def complete(run):
    with sqlite3.connect(run.database) as db:
        db.execute(
            "INSERT INTO teardown_requests VALUES (?, 'completed', 0, ?, ?, ?, ?)",
            (run.identity, *[f"2026-09-07T00:00:0{i}+00:00" for i in range(4)]),
        )


def execute(run, name="cleanup"):
    return cleanup.cleanup_subject(
        run.context, run.store, run.lease, wallet=run.wallet, output=run.context.root / name, timeout_seconds=1
    )


def test_existing_completion_is_adopted_without_replacing_request(run, monkeypatch):
    complete(run)
    monkeypatch.setattr(subprocess, "run", lambda *args, **kwargs: pytest.fail("existing teardown was replaced"))
    result = execute(run)
    assert result["status"] == "PASS"
    assert result["terminal_closure"]["closed_generations"] == ["5685394", "5685395"]
    assert result["actor_cleanup"] == result["fork_shutdown"] == "UNMEASURED"
    assert result["quantity_capture"]["status"] == "UNMEASURED"
    output = run.context.root / "cleanup"
    assert json.loads((output / "result.json").read_text())["terminal_closure"]["status"] == "PASS"
    assert json.loads((output / "quantities/result.json").read_text())["status"] == "UNMEASURED"


@pytest.mark.parametrize("live", [False, True])
def test_cleanup_before_first_observation_retains_terminal_evidence_without_certifying_closure(run, monkeypatch, live):
    complete(run)
    with sqlite3.connect(run.store.path) as db:
        db.execute("DELETE FROM phase_evidence")
    (run.context.root / "positions-open.json").unlink()
    if live:
        monkeypatch.setattr(cleanup, "capture_position_generations", lambda *a: run.opened)
    result = execute(run)
    assert result["status"] == ("FAIL" if live else "UNMEASURED")
    assert bool(result["terminal_closure"]["observed_live_generations"]) is live
    assert result["sdk_request_status"] == "completed"
    assert "quantity_capture" in result and "actor_inventory" in result
    assert (run.context.root / "cleanup/positions-terminal.json").is_file()
    assert not (run.context.root / "positions-open.json").exists()


def test_signal_uses_boot_identity_and_separate_cli_then_observes_chain(run, monkeypatch):
    commands = []

    def signal(argv, **kwargs):
        commands.append(argv)
        complete(run)
        return SimpleNamespace(returncode=0)

    monkeypatch.setattr(subprocess, "run", signal)
    assert execute(run)["status"] == "PASS"
    assert len(commands) == 1
    assert commands[0][:7] == ("uv", "run", "--no-sync", "almanak", "strat", "teardown", "request")
    assert commands[0][commands[0].index("-s") + 1] == run.identity
    assert "--wait" not in commands[0]
    assert execute(run, "adoption")["status"] == "PASS"
    assert len(commands) == 1


def test_sdk_completion_cannot_hide_live_liquidity(run, monkeypatch):
    complete(run)
    monkeypatch.setattr(cleanup, "capture_position_generations", lambda *args: run.opened)
    result = execute(run)
    assert result["status"] == result["terminal_closure"]["status"] == "FAIL"


def test_fork_loss_never_writes_success(run, monkeypatch):
    complete(run)

    def lost(*args):
        raise OSError("fork lost")

    monkeypatch.setattr(cleanup, "capture_position_generations", lost)
    with pytest.raises(OSError, match="fork lost"):
        execute(run)
    assert not (run.context.root / "cleanup/result.json").exists()


def test_ambiguous_signal_is_consumed_and_never_resent(run, monkeypatch):
    run.store.request_cleanup(run.lease)

    def fail(argv, **kwargs):
        raise subprocess.TimeoutExpired(argv, 30)

    monkeypatch.setattr(subprocess, "run", fail)
    with (run.context.root / "signal.log").open("wb") as log:
        with pytest.raises(subprocess.TimeoutExpired):
            run.store.dispatch_cleanup(run.lease, role="subject", argv=("uv", "run"), log=log)
        assert run.store.dispatch_cleanup(run.lease, role="subject", argv=("uv", "run"), log=log) is False


def test_missing_acknowledgement_cannot_certify_sdk_completion(run):
    complete(run)
    with sqlite3.connect(run.database) as db:
        db.execute("UPDATE teardown_requests SET acknowledged_at=NULL")
    assert execute(run)["status"] == "FAIL"


def test_signal_timeout_still_observes_completed_teardown_without_resend(run, monkeypatch):
    def signal(argv, **kwargs):
        complete(run)
        raise subprocess.TimeoutExpired(argv, 30)

    monkeypatch.setattr(subprocess, "run", signal)
    assert execute(run)["status"] == "PASS"
    assert json.loads((run.context.root / "cleanup/signal-error.json").read_text()) == {"error_type": "TimeoutExpired"}


def test_changed_card_cannot_direct_cleanup(run):
    (run.context.root / "preparation/card.json").write_text(json.dumps({"run_id": "other"}))
    with pytest.raises(ValueError, match="differs from the prepared"):
        execute(run)


def test_takeover_adopts_consumed_signal_and_fences_previous_controller(run, monkeypatch):
    run.store.request_cleanup(run.lease)
    commands = []

    def signal(argv, **kwargs):
        commands.append(argv)
        return SimpleNamespace(returncode=0)

    monkeypatch.setattr(subprocess, "run", signal)
    with (run.context.root / "signal.log").open("wb") as log:
        assert run.store.dispatch_cleanup(run.lease, role="subject", argv=("uv", "run"), log=log)
        expired = run.store.snapshot()["ownership"]["expires"] + 1
        run.store.clock = lambda: expired
        replacement = run.store.acquire("cleanup-owner", cleanup=True)
        with pytest.raises(OwnershipError, match="stale"):
            run.store.dispatch_cleanup(run.lease, role="subject", argv=("uv", "run"), log=log)
        assert not run.store.dispatch_cleanup(replacement, role="subject", argv=("uv", "run"), log=log)
        with pytest.raises(OwnershipError, match="differs"):
            run.store.dispatch_cleanup(replacement, role="subject", argv=("uv", "other"), log=log)
    assert commands == [("uv", "run")]


def test_delayed_request_after_timeout_is_adopted_by_new_owner_without_resend(run, monkeypatch):
    commands = []
    run.store.request_cleanup(run.lease)

    def signal(argv, **kwargs):
        commands.append(argv)
        raise subprocess.TimeoutExpired(argv, 30)

    monkeypatch.setattr(subprocess, "run", signal)
    subject = run.context.root / "subject"
    command = cleanup.teardown_command(subject)
    command = command[: command.index("--wait")]
    with (run.context.root / "original-signal.log").open("xb") as log:
        with pytest.raises(subprocess.TimeoutExpired):
            run.store.dispatch_cleanup(run.lease, role="subject", argv=command, log=log)
    assert cleanup.teardown_state(subject, run.identity) is None
    old_lease = run.lease
    expired = run.store.snapshot()["ownership"]["expires"] + 1
    replacement = OwnershipStore(run.store.path, clock=lambda: expired)
    run.lease = replacement.acquire("replacement-cleanup", cleanup=True)
    run.store = replacement
    with pytest.raises(OwnershipError, match="stale"):
        run.store.renew(old_lease, seconds=300)
    clock = [0.0]
    monkeypatch.setattr(cleanup.time, "monotonic", lambda: clock[0])

    def delayed_commit(seconds):
        assert cleanup.teardown_state(subject, run.identity) is None
        clock[0] += seconds
        complete(run)

    monkeypatch.setattr(cleanup.time, "sleep", delayed_commit)
    assert execute(run, "adopted-cleanup")["status"] == "PASS"
    assert len(commands) == 1
    evidence = json.loads((run.context.root / "adopted-cleanup/sdk-teardown.json").read_text())
    assert evidence["request"]["status"] == "completed"


def test_timed_out_signal_with_no_visible_request_never_claims_completion_or_retries(run, monkeypatch):
    commands = []

    def signal(argv, **kwargs):
        commands.append(argv)
        raise subprocess.TimeoutExpired(argv, 30)

    monkeypatch.setattr(subprocess, "run", signal)
    clock = [0.0]
    monkeypatch.setattr(cleanup.time, "monotonic", lambda: clock[0])
    monkeypatch.setattr(cleanup.time, "sleep", lambda seconds: clock.__setitem__(0, clock[0] + seconds))
    for name in ("first-attempt", "observation-only-retry"):
        result = execute(run, name)
        assert result["status"] == "FAIL"
        assert result["sdk_request_status"] == "UNMEASURED"
        assert result["terminal_closure"]["status"] == "PASS"
        evidence = json.loads((run.context.root / name / "sdk-teardown.json").read_text())
        assert evidence["request"] is None
    assert len(commands) == 1


@pytest.mark.parametrize("mutation", ["changed", "missing", "unrecorded_managed"])
def test_cleanup_cannot_certify_mutated_or_unowned_phase_files(run, mutation):
    complete(run)
    path = run.context.root / "positions-open.json"
    if mutation == "changed":
        path.write_bytes(path.read_bytes() + b" ")
    elif mutation == "missing":
        path.unlink()
    else:
        (run.context.root / "positions-managed.json").write_text(json.dumps(run.opened))
    with pytest.raises(OwnershipError, match="phase evidence"):
        execute(run)
    assert not (run.context.root / "cleanup/result.json").exists()
    assert (run.context.root / "cleanup/sdk-teardown.json").is_file()
