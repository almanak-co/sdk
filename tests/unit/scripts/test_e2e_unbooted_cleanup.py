"""Loss of boot state cannot erase independently observed subject exposure."""

import sqlite3
import subprocess

import pytest

from qa_lab import e2e_unbooted_cleanup as observation
from qa_lab.e2e_card import load_json
from qa_lab.e2e_ownership import OwnershipError
from tests.unit.scripts import test_e2e_cleanup as cleanup_tests
from tests.unit.scripts import test_e2e_startup as startup_tests

base_run = cleanup_tests.run
checkout = startup_tests.checkout
startup_lane = startup_tests.lane


@pytest.fixture
def run(base_run):
    base_run.context.fork_block = base_run.opened["fork_identity"]["fork_block"]
    base_run.context.public_identity = lambda: base_run.opened["fork_identity"]
    return base_run


def remove_boot(run, kind):
    if kind == "missing":
        run.database.unlink()
    elif kind == "empty":
        with sqlite3.connect(run.database) as db:
            db.execute("DELETE FROM strategy_state")
    else:
        with sqlite3.connect(run.database) as db:
            db.execute("INSERT INTO strategy_state VALUES ('deployment:aaaaaaaaaaaa')")


@pytest.mark.parametrize("kind", ["missing", "empty", "ambiguous"])
@pytest.mark.parametrize("live", [True, False])
def test_unbooted_cleanup_retains_observed_positions_and_never_dispatches(run, monkeypatch, kind, live):
    remove_boot(run, kind)
    monkeypatch.setattr(subprocess, "run", lambda *args, **kwargs: pytest.fail("unbooted teardown dispatched"))
    raw = run.opened if live else run.terminal
    monkeypatch.setattr(observation, "capture_position_generations", lambda *args: raw)
    result = cleanup_tests.execute(run)
    assert run.store.snapshot()["ownership"]["cleanup"] == 1
    assert result["status"] == ("FAIL" if live else "UNMEASURED")
    assert result["sdk_request_status"] == result["terminal_closure"]["status"] == "UNMEASURED"
    assert result["dispatch_status"] == "NOT_ATTEMPTED"
    assert result["subject_wallet_authority"] == "owned_opening"
    assert load_json(run.context.root / "cleanup/positions-terminal.json") == raw
    assert load_json(run.context.root / "cleanup/result.json") == result
    assert (run.context.root / "cleanup/actor-inventory/result.json").exists()
    with pytest.raises(OwnershipError, match="cleanup"):
        run.store.reserve_launch(run.lease, "stimulus")


def test_late_boot_can_receive_one_normal_teardown_after_observation_only(run, monkeypatch):
    saved = run.database.read_bytes()
    remove_boot(run, "missing")
    monkeypatch.setattr(observation, "capture_position_generations", lambda *args: run.opened)
    commands = []

    def signal(argv, **kwargs):
        commands.append(argv)
        cleanup_tests.complete(run)
        return type("Completed", (), {"returncode": 0})()

    monkeypatch.setattr(subprocess, "run", signal)
    assert cleanup_tests.execute(run)["dispatch_status"] == "NOT_ATTEMPTED"
    assert commands == []
    run.database.write_bytes(saved)
    assert cleanup_tests.execute(run, "late-boot")["status"] == "PASS"
    assert cleanup_tests.execute(run, "adopted")["status"] == "PASS"
    assert len(commands) == 1


def test_unbooted_fork_read_failure_remains_unmeasured(run, monkeypatch):
    remove_boot(run, "missing")

    def unavailable(*args):
        raise OSError("unavailable")

    monkeypatch.setattr(observation, "capture_position_generations", unavailable)
    result = cleanup_tests.execute(run)
    assert result["status"] == "UNMEASURED"
    assert result["position_census"]["error_type"] == "OSError"
    assert not (run.context.root / "cleanup/positions-terminal.json").exists()


def test_unbooted_wallet_mismatch_does_not_authorize_an_observation(run, monkeypatch):
    remove_boot(run, "missing")
    run.wallet = "0x" + "11" * 20
    monkeypatch.setattr(observation, "capture_position_generations", lambda *args: pytest.fail("unbound wallet read"))
    with pytest.raises(ValueError, match="owned opening"):
        cleanup_tests.execute(run)
    assert run.store.snapshot()["ownership"]["cleanup"] == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("has_witness", [True, False])
async def test_cleanup_before_any_opening_uses_startup_identity_or_stays_unbound(
    startup_lane, monkeypatch, has_witness
):
    from qa_lab.e2e_cleanup import cleanup_subject
    from qa_lab.e2e_startup import _context

    lane = startup_lane
    startup = await startup_tests.publish(lane)
    context, wallet = _context(lane.root, lane.route.manifest, startup)
    if not has_witness:
        (lane.root / "gateway-startup.json").unlink()
    raw = load_json(cleanup_tests.FIXTURES / "positions-terminal.json")
    raw.update(
        fork_identity=context.public_identity(), wallet=wallet, start_block=101, end_block=102, logs=[], generations=[]
    )
    reads = []

    def capture(*args):
        reads.append(args)
        return raw

    monkeypatch.setattr(observation, "capture_position_generations", capture)
    result = cleanup_subject(context, lane.store, lane.lease, wallet=wallet, output=lane.root / "cleanup")
    assert result["status"] == "UNMEASURED"
    assert result["subject_wallet_authority"] == ("gateway_startup" if has_witness else "UNMEASURED")
    assert len(reads) == int(has_witness)
    assert result["terminal_closure"]["status"] == "UNMEASURED"
