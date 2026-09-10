"""Real controller components with synthetic clocks/LP rebalance and captured ABI bytes."""

import sqlite3
from copy import deepcopy
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest

from almanak.framework.runner.identity import resolve_deployment_id
from qa_lab import e2e_cleanup, e2e_controller, e2e_hold, e2e_phase_capture
from qa_lab.e2e_card import REPO, TEMPLATE, canonical, digest, load_json, prepare
from qa_lab.e2e_generation_verdict import generation_predicates
from qa_lab.e2e_ownership import OwnershipStore
from qa_lab.qa_execution_context import ForkExecutionContext
from tests.unit.scripts import test_e2e_card as card_tests
from tests.unit.scripts import test_e2e_generation_verdict as generation_tests

checkout = card_tests.checkout
captured = generation_tests.captured
rebalanced = generation_tests.rebalanced


def synthetic_terminal(managed, captured_terminal):
    terminal = deepcopy(managed)
    block = managed["end_block"] + 1
    block_hash = "0x" + "44" * 32
    for position in terminal["generations"]:
        if position["burned"]:
            continue
        token = position["token_id"]
        original = "5685394" if token == "5685396" else token
        events = [event for event in captured_terminal["logs"] if str(int(event["topics"][-1], 16)) == original]
        # The final two events are the captured full decrease and burn.
        for original_event in events[-2:]:
            event = deepcopy(original_event)
            event.update(
                block_number=block,
                block_hash=block_hash,
                log_index=len(terminal["logs"]),
                transaction_hash="0x" + "55" * 32,
            )
            event["topics"][-1] = "0x" + f"{int(token):064x}"
            terminal["logs"].append(event)
        position.update(burned=True, owner="0x" + "00" * 20, terminal_status="BURNED", terminal_liquidity_raw=None)
    terminal.update(end_block=block, end_block_hash=block_hash)
    return terminal


@pytest.fixture
def lane(checkout, tmp_path, monkeypatch, captured, rebalanced):
    opened, managed = rebalanced
    terminal = synthetic_terminal(managed, captured[1])
    assert generation_predicates(opened, managed, terminal)["terminal_closure"]["status"] == "PASS"
    root = tmp_path / "lane"
    root.mkdir(mode=0o700)
    scenario = load_json(REPO / TEMPLATE)
    scenario["hold_policy"] = {
        "minimum_seconds": 2,
        "maximum_gap_seconds": 3,
        "interval_seconds": 1,
        "deadline_seconds": 8,
    }
    template = tmp_path / "short-unit-scenario.json"
    template.write_bytes(canonical(scenario))
    prepare(checkout, template, root / "preparation", "controller-unit-run")
    monkeypatch.setattr(e2e_controller, "REPO", checkout)
    monkeypatch.setattr(e2e_phase_capture, "REPO", checkout)
    identity = dict(opened["fork_identity"])
    identity.pop("mode")
    context = ForkExecutionContext(root=root, rpc_url="http://127.0.0.1:19999", **identity)
    monkeypatch.setattr(ForkExecutionContext, "assert_rpc_identity", lambda *args: None)
    clock = [1000.0]
    monkeypatch.setattr(e2e_hold.time, "monotonic", lambda: clock[0])
    monkeypatch.setattr(e2e_hold.time, "monotonic_ns", lambda: int(clock[0] * 1e9))
    monkeypatch.setattr(e2e_hold.time, "sleep", lambda seconds: clock.__setitem__(0, clock[0] + seconds))

    class ClockDateTime(datetime):
        @classmethod
        def now(cls, tz=None):
            return datetime(2026, 9, 7, tzinfo=UTC) + timedelta(seconds=clock[0])

    monkeypatch.setattr(e2e_hold, "datetime", ClockDateTime)
    store = OwnershipStore(root / "ownership.sqlite", clock=lambda: clock[0])
    store.initialize(run_id="controller-unit-run", card_hash=digest((root / "preparation/card.json").read_bytes()))
    lease = store.acquire("controller", seconds=300)
    subject = root / "subject"
    subject.mkdir()
    deployment = resolve_deployment_id(wallet_address=opened["wallet"], chain="arbitrum")
    database = subject / "almanak_state.db"
    with sqlite3.connect(database) as db:
        db.execute("CREATE TABLE strategy_state (deployment_id TEXT)")
        db.execute("INSERT INTO strategy_state VALUES (?)", (deployment,))
        db.execute(
            "CREATE TABLE teardown_requests (deployment_id TEXT, status TEXT, positions_failed INTEGER, requested_at TEXT, acknowledged_at TEXT, started_at TEXT, completed_at TEXT)"
        )
    token = store.reserve_launch(lease, "subject")
    store.claim_launch(lease, role="subject", token=token)
    monkeypatch.setattr(e2e_phase_capture, "capture_position_generations", lambda *args: deepcopy(opened))
    e2e_phase_capture.capture_phase(context, store, lease, phase="open", wallet=opened["wallet"])
    token = store.reserve_launch(lease, "stimulus")
    store.claim_launch(lease, role="stimulus", token=token)
    monkeypatch.setattr(e2e_phase_capture, "capture_position_generations", lambda *args: deepcopy(managed))
    e2e_phase_capture.capture_phase(context, store, lease, phase="managed", wallet=opened["wallet"])
    monkeypatch.setattr(
        e2e_controller,
        "cleanup_stimulus",
        lambda *args, **kwargs: {
            "status": "UNMEASURED",
            "reason": "Synthetic stimulus; no actor execution in this component test",
        },
    )
    lane = SimpleNamespace(
        context=context,
        store=store,
        lease=lease,
        opened=opened,
        managed=managed,
        terminal=terminal,
        samples=[],
        commands=[],
        sample_value=managed,
    )

    def capture(*args):
        lane.samples.append(deepcopy(lane.sample_value))
        clock[0] += 0.25
        return deepcopy(lane.sample_value)

    real_run = e2e_cleanup.subprocess.run

    def signal(argv, **kwargs):
        if argv[0] != "uv":
            return real_run(argv, **kwargs)
        lane.commands.append(argv)
        with sqlite3.connect(database) as db:
            db.execute(
                "INSERT INTO teardown_requests VALUES (?, 'completed', 0, ?, ?, ?, ?)",
                (deployment, *[f"2026-09-07T01:00:0{i}+00:00" for i in range(4)]),
            )
        return SimpleNamespace(returncode=0)

    monkeypatch.setattr(e2e_hold, "capture_position_generations", capture)
    monkeypatch.setattr(e2e_cleanup, "capture_position_generations", lambda *args: deepcopy(lane.terminal))
    monkeypatch.setattr(e2e_cleanup.subprocess, "run", signal)
    return lane


def execute(lane):
    return e2e_controller.monitor_and_cleanup(
        lane.context, lane.store, lane.lease, wallet=lane.opened["wallet"], supervised=True
    )


def test_component_lane_uses_raw_evidence_and_still_does_not_certify_e2e(lane):
    result = execute(lane)
    assert result["sampled_hold"]["status"] == "PASS"
    assert result["sampled_hold"]["elapsed_seconds"] >= 2
    assert result["subject_cleanup"]["terminal_closure"]["status"] == "PASS"
    assembly = load_json(lane.context.root / "bundle-result.json")
    assert assembly["status"] == "INCOMPLETE"
    manifest = load_json(lane.context.root / assembly["path"] / "assembly.json")
    assert manifest["e2e_admission"] == "UNMEASURED"
    assert result["status"] == result["e2e_admission"] == "UNMEASURED"
    assert len(lane.commands) == 1
    assert lane.store.snapshot()["ownership"]["cleanup"] == 1


def test_pre_rebalance_positions_cannot_be_counted_as_post_rebalance_hold(lane):
    lane.sample_value = lane.opened
    with pytest.raises(ExceptionGroup) as error:
        execute(lane)
    assert "backwards" in str(error.value.exceptions[0])
    assert len(lane.samples) == 1
    assert len(lane.commands) == 1
    assert load_json(lane.context.root / "hold-observations/000000-positions.json") == lane.opened
    assert not (lane.context.root / "hold-result.json").exists()
    result = load_json(lane.context.root / "controller-result.json")
    assert result["status"] == "FAIL"
    assert result["sampled_hold"] is None
    assert result["subject_cleanup"]["terminal_closure"]["status"] == "PASS"
    assembly = load_json(lane.context.root / "bundle-result.json")
    assert assembly["status"] == "INCOMPLETE"
    manifest = load_json(lane.context.root / assembly["path"] / "assembly.json")
    assert manifest["e2e_admission"] == "UNMEASURED"
