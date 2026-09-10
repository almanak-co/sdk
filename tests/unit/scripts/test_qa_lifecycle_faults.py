from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from qa_lab import qa_lifecycle_faults as faults


@pytest.fixture
def fork(monkeypatch, tmp_path):
    calls = []
    context = SimpleNamespace(
        root=tmp_path,
        context_id="isolated-acceptance",
        assert_rpc_identity=lambda web3: calls.append(web3),
    )
    monkeypatch.setattr(faults, "_fork_context", lambda: context)
    return context, calls


def configure(root: Path, **overrides):
    config = {
        "context_id": "isolated-acceptance",
        "checkpoint": "after_broadcast",
        "phase": "execution",
        "action": "raise",
        **overrides,
    }
    (root / "faults.json").write_text(json.dumps(config))


def test_production_path_cannot_activate_faults(monkeypatch, tmp_path):
    configure(tmp_path)
    monkeypatch.setattr(faults, "_fork_context", lambda: None)
    faults.checkpoint("after_broadcast", output=tmp_path / "bundle", phase="execution")
    assert not (tmp_path / "bundle").exists()
    assert not (tmp_path / "fault-consumed.json").exists()


def test_rpc_identity_must_be_verified_before_any_fault_evidence(fork):
    context, _calls = fork
    configure(context.root)

    def refuse(_web3):
        raise ValueError("endpoint is not the bound fork")

    context.assert_rpc_identity = refuse
    with pytest.raises(ValueError, match="bound fork"):
        faults.checkpoint("after_broadcast", output=context.root / "bundle", phase="execution")
    assert not (context.root / "bundle").exists()
    assert not (context.root / "fault-consumed.json").exists()


def test_fault_is_recorded_before_raise_and_consumed_once(fork):
    context, calls = fork
    configure(context.root)
    output = context.root / "bundle"
    with pytest.raises(faults.InjectedLifecycleFailure, match="execution/after_broadcast"):
        faults.checkpoint("after_broadcast", output=output, phase="execution", tx_hash="0x123")
    marker = json.loads((context.root / "fault-consumed.json").read_text())
    assert marker["tx_hash"] == "0x123"
    assert marker["network"] == "anvil"
    selected = json.loads((output / "lifecycle-checkpoints/selected-fault.json").read_text())
    assert selected == marker
    faults.checkpoint("after_broadcast", output=output, phase="execution", tx_hash="0x456")
    assert json.loads((context.root / "fault-consumed.json").read_text()) == marker
    assert len((output / "lifecycle-checkpoints/events.jsonl").read_text().splitlines()) == 2
    assert len(calls) == 2


@pytest.mark.parametrize("override", [{"phase": "funding"}, {"checkpoint": "before_sweep"}])
def test_unselected_boundary_does_not_consume_fault(fork, override):
    context, _calls = fork
    configure(context.root, **override)
    faults.checkpoint("after_broadcast", output=context.root / "bundle", phase="execution")
    assert not (context.root / "fault-consumed.json").exists()


@pytest.mark.parametrize(
    "override",
    [
        {"context_id": "different-fork"},
        {"action": "shell"},
        {"checkpoint": "unrecognized"},
        {"phase": ""},
        {"pause_timeout_seconds": 121},
        {"pause_timeout_seconds": True},
    ],
)
def test_invalid_fault_configuration_is_refused(fork, override):
    context, _calls = fork
    configure(context.root, **override)
    with pytest.raises(ValueError):
        faults.checkpoint("after_broadcast", output=context.root / "bundle", phase="execution")
    assert not (context.root / "fault-consumed.json").exists()


def test_fault_configuration_cannot_be_changed_after_consumption(fork):
    context, _calls = fork
    configure(context.root)
    with pytest.raises(faults.InjectedLifecycleFailure):
        faults.checkpoint("after_broadcast", output=context.root / "bundle", phase="execution")
    configure(context.root, action="pause")
    with pytest.raises(ValueError, match="changed after consumption"):
        faults.checkpoint("after_broadcast", output=context.root / "bundle", phase="execution")


def test_pause_publishes_marker_before_waiting_and_times_out(fork, monkeypatch):
    context, _calls = fork
    configure(context.root, action="pause", pause_timeout_seconds=1)
    output = context.root / "bundle"
    times = iter([0, 0, 2])
    observed = []

    def observe_pause(_seconds):
        observed.append(json.loads((output / "lifecycle-checkpoints/selected-fault.json").read_text()))

    monkeypatch.setattr(faults, "time", SimpleNamespace(monotonic=lambda: next(times), sleep=observe_pause))
    with pytest.raises(faults.InjectedLifecycleFailure, match="pause timed out"):
        faults.checkpoint("after_broadcast", output=output, phase="execution")
    assert len(observed) == 1
    assert observed[0]["action"] == "pause"


def test_fault_output_cannot_escape_context(fork):
    context, _calls = fork
    configure(context.root)
    with pytest.raises(ValueError, match="within its isolated context"):
        faults.checkpoint("after_broadcast", output=context.root.parent, phase="execution")


def test_fault_evidence_cannot_follow_symlink(fork):
    context, _calls = fork
    configure(context.root)
    output = context.root / "bundle"
    output.mkdir()
    target = context.root / "unrelated"
    target.mkdir()
    (output / "lifecycle-checkpoints").symlink_to(target, target_is_directory=True)
    with pytest.raises(ValueError, match="symlink"):
        faults.checkpoint("after_broadcast", output=output, phase="execution")
    assert not list(target.iterdir())


@pytest.mark.parametrize("tx_hash,expected", [("123abc", "0x123abc"), ("0x123abc", "0x123abc"), (None, None)])
def test_fault_transaction_identity_matches_journal_canonical_form(fork, tx_hash, expected):
    context, _ = fork
    configure(context.root)
    output = context.root / "bundle"
    with pytest.raises(faults.InjectedLifecycleFailure):
        faults.checkpoint("after_broadcast", output=output, phase="execution", tx_hash=tx_hash)
    marker = json.loads((context.root / "fault-consumed.json").read_text())
    selected = json.loads((output / "lifecycle-checkpoints/selected-fault.json").read_text())
    events = [json.loads(line) for line in (output / "lifecycle-checkpoints/events.jsonl").read_text().splitlines()]
    assert marker["tx_hash"] == selected["tx_hash"] == events[0]["tx_hash"] == expected
