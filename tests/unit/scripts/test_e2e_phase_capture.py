import json

"""Owned publication uses captured opening bytes and a synthetic replacement."""

from copy import deepcopy
from types import SimpleNamespace

import pytest

from qa_lab import e2e_phase_capture as phases
from qa_lab.e2e_card import canonical, digest
from qa_lab.e2e_ownership import OwnershipError, OwnershipStore
from tests.unit.scripts import test_e2e_generation_verdict as generations

captured = generations.captured
rebalanced = generations.rebalanced


@pytest.fixture
def lane(tmp_path, rebalanced, monkeypatch):
    opened, managed = rebalanced
    preparation = tmp_path / "preparation"
    preparation.mkdir()
    card = canonical({"run_id": "owned-phase-test"})
    (preparation / "card.json").write_bytes(card)
    now = [1000.0]
    store = OwnershipStore(tmp_path / "ownership.sqlite", clock=lambda: now[0])
    store.initialize(run_id="owned-phase-test", card_hash=digest(card))
    lease = store.acquire("controller", seconds=300)
    token = store.reserve_launch(lease, "subject")
    store.claim_launch(lease, role="subject", token=token)
    context = SimpleNamespace(
        root=tmp_path,
        require_owned=lambda path: path,
        fork_block=opened["fork_identity"]["fork_block"],
        public_identity=lambda: opened["fork_identity"],
    )
    monkeypatch.setattr(phases, "verify_preparation", lambda *args: None)
    monkeypatch.setattr(
        phases,
        "boot_deployment_id",
        lambda *args: phases.resolve_deployment_id(wallet_address=opened["wallet"], chain="arbitrum"),
    )
    current = [opened]
    monkeypatch.setattr(phases, "capture_position_generations", lambda *args: deepcopy(current[0]))
    return SimpleNamespace(
        context=context,
        store=store,
        lease=lease,
        now=now,
        current=current,
        opened=opened,
        managed=managed,
        wallet=opened["wallet"],
    )


def capture(lane, phase):
    return phases.capture_phase(lane.context, lane.store, lane.lease, phase=phase, wallet=lane.wallet)


def launch_stimulus(lane):
    token = lane.store.reserve_launch(lane.lease, "stimulus")
    lane.store.claim_launch(lane.lease, role="stimulus", token=token)


def test_owned_opening_then_replacement_can_feed_the_monitor(lane):
    opened = capture(lane, "open")
    launch_stimulus(lane)
    lane.current[0] = lane.managed
    managed = capture(lane, "managed")
    assert phases.load_owned_phases(lane.context, lane.store, lane.lease, wallet=lane.wallet) == (opened, managed)
    assert managed["capture"]["phase"] == "managed"
    assert managed["capture"]["lease_generation"] == lane.lease.generation
    with pytest.raises(OwnershipError, match="already recorded"):
        capture(lane, "managed")


def test_managed_capture_before_a_replacement_is_not_recorded(lane):
    capture(lane, "open")
    launch_stimulus(lane)
    with pytest.raises(phases.PhasePending, match="measured replacement"):
        capture(lane, "managed")
    assert not (lane.context.root / "positions-managed.json").exists()


def test_operator_files_cannot_replace_owned_phase_records(lane):
    (lane.context.root / "positions-open.json").write_bytes(canonical(lane.opened))
    (lane.context.root / "positions-managed.json").write_bytes(canonical(lane.managed))
    with pytest.raises(OwnershipError, match="no record"):
        phases.load_owned_phases(lane.context, lane.store, lane.lease, wallet=lane.wallet)
    with pytest.raises(FileExistsError):
        capture(lane, "open")


@pytest.mark.parametrize("fault", ["expired", "cleanup", "stimulus_reserved"])
def test_unowned_or_late_opening_capture_is_refused(lane, fault):
    if fault == "expired":
        lane.now[0] += 301
    elif fault == "cleanup":
        lane.store.request_cleanup(lane.lease)
    else:
        lane.store.reserve_launch(lane.lease, "stimulus")
    with pytest.raises(OwnershipError):
        capture(lane, "open")
    assert not (lane.context.root / "positions-open.json").exists()


def test_lease_expiry_during_capture_cannot_publish_evidence(lane, monkeypatch):
    def slow(*args):
        lane.now[0] += 301
        return deepcopy(lane.opened)

    monkeypatch.setattr(phases, "capture_position_generations", slow)
    with pytest.raises(OwnershipError, match="expired"):
        capture(lane, "open")
    assert not (lane.context.root / "positions-open.json").exists()


def test_edited_owned_opening_prevents_managed_capture(lane):
    capture(lane, "open")
    launch_stimulus(lane)
    lane.current[0] = lane.managed
    (lane.context.root / "positions-open.json").write_bytes(canonical(lane.opened))
    with pytest.raises(OwnershipError, match="changed"):
        capture(lane, "managed")
    assert not (lane.context.root / "positions-managed.json").exists()


def test_other_wallet_cannot_borrow_the_subject_launch(lane):
    with pytest.raises(ValueError, match="subject boot identity"):
        phases.capture_phase(lane.context, lane.store, lane.lease, phase="open", wallet="0x" + "33" * 20)
    assert not (lane.context.root / "positions-open.json").exists()


def test_cleanup_adopts_original_phase_hash_without_continuing_monitor(lane):
    opened = capture(lane, "open")
    with pytest.raises(OwnershipError, match="cleanup ownership"):
        lane.store.read_cleanup_phase(lane.lease, "open")
    lane.store.request_cleanup(lane.lease)
    expired = lane.store.snapshot()["ownership"]["expires"] + 1
    lane.store.clock = lambda: expired
    successor = lane.store.acquire("independent-cleanup", cleanup=True)
    assert json.loads(lane.store.read_cleanup_phase(successor, "open")) == opened
    assert lane.store.read_cleanup_phase(successor, "managed", optional=True) is None
    with pytest.raises(OwnershipError, match="Cleanup prohibits"):
        lane.store.read_phase(successor, "open")
    with pytest.raises(OwnershipError, match="stale"):
        lane.store.read_cleanup_phase(lane.lease, "open")


def test_narrow_close_prefix_waits_without_publishing_managed_phase(lane):
    capture(lane, "open")
    launch_stimulus(lane)
    prefix = deepcopy(lane.managed)
    replacement = prefix["generations"].pop()["token_id"]
    prefix["logs"] = [event for event in prefix["logs"] if str(int(event["topics"][-1], 16)) != replacement]
    lane.current[0] = prefix
    with pytest.raises(phases.PhasePending):
        capture(lane, "managed")
    assert not (lane.context.root / "positions-managed.json").exists()
    lane.current[0] = lane.managed
    assert capture(lane, "managed")["capture"]["phase"] == "managed"


def test_closing_both_positions_is_failure_not_pending(lane, captured):
    capture(lane, "open")
    launch_stimulus(lane)
    lane.current[0] = captured[1]
    with pytest.raises(ValueError) as failure:
        capture(lane, "managed")
    assert not isinstance(failure.value, phases.PhasePending)
    assert not (lane.context.root / "positions-managed.json").exists()
