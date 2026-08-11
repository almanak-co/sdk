"""Fail-closed operator recovery controls for execution replay barriers."""

from __future__ import annotations

from types import SimpleNamespace

import pytest
from click.testing import CliRunner

from almanak.framework.cli import execution_recovery as recovery_module
from almanak.framework.cli.execution_recovery import assess_replay_barrier, execution_recovery, seal_landed_repair
from almanak.framework.runner.runner_models import (
    ExecutionBarrierPhase,
    ExecutionProgress,
    StepSubmissionEvidence,
    SubmissionProvenance,
)
from almanak.framework.state import StateValuePreconditionError


def _progress(*, evidence: list[StepSubmissionEvidence] | None = None) -> ExecutionProgress:
    return ExecutionProgress(
        execution_id="execution-1",
        deployment_id="deployment:test",
        intents_hash="intent-hash",
        total_steps=1,
        reconciliation_required_step_index=0,
        submission_evidence=evidence or [],
    )


def _evidence(
    provenance: SubmissionProvenance,
    *,
    tx_ids: list[str] | None = None,
    chain: str = "arbitrum",
) -> StepSubmissionEvidence:
    return StepSubmissionEvidence(
        step_index=0,
        chain=chain,
        submission_provenance=provenance,
        submitted_transaction_ids=tx_ids or [],
    )


def test_legacy_marker_without_typed_evidence_fails_closed() -> None:
    verdict = assess_replay_barrier(_progress())

    assert verdict.releasable is False
    assert "legacy or malformed" in verdict.reason


def test_unspecified_proto_skew_fails_closed() -> None:
    verdict = assess_replay_barrier(_progress(evidence=[_evidence(SubmissionProvenance.UNSPECIFIED)]))

    assert verdict.releasable is False
    assert "mixed-version" in verdict.reason


def test_explicit_not_attempted_is_safe_to_release() -> None:
    verdict = assess_replay_barrier(_progress(evidence=[_evidence(SubmissionProvenance.NOT_ATTEMPTED)]))

    assert verdict.releasable is True
    assert "not attempted" in verdict.reason


def test_not_attempted_with_transaction_identity_is_contradictory() -> None:
    verdict = assess_replay_barrier(
        _progress(evidence=[_evidence(SubmissionProvenance.NOT_ATTEMPTED, tx_ids=["0xabc"])])
    )

    assert verdict.releasable is False
    assert "contradicts" in verdict.reason


def test_attempted_requires_every_exact_transaction_to_be_reverted() -> None:
    progress = _progress(evidence=[_evidence(SubmissionProvenance.ATTEMPTED, tx_ids=["0xaaa", "0xbbb"])])

    assert assess_replay_barrier(progress, {"0xaaa": "reverted", "0xbbb": "reverted"}).releasable is True
    assert assess_replay_barrier(progress, {"0xaaa": "reverted", "0xbbb": "pending"}).releasable is False
    assert assess_replay_barrier(progress, {"0xaaa": "reverted"}).releasable is False


def test_confirmed_transaction_and_accounting_pending_never_release_for_retry() -> None:
    progress = _progress(evidence=[_evidence(SubmissionProvenance.ATTEMPTED, tx_ids=["0xabc"])])
    assert assess_replay_barrier(progress, {"0xabc": "confirmed"}).releasable is False

    progress.accounting_pending_step_index = 0
    verdict = assess_replay_barrier(progress, {"0xabc": "reverted"})
    assert verdict.releasable is False
    assert "accounting/state completion" in verdict.reason


def test_attempted_submission_requires_chain_and_transaction_identity() -> None:
    no_chain = _progress(evidence=[_evidence(SubmissionProvenance.ATTEMPTED, tx_ids=["0xabc"], chain="")])
    no_ids = _progress(evidence=[_evidence(SubmissionProvenance.ATTEMPTED)])

    assert assess_replay_barrier(no_chain, {"0xabc": "reverted"}).releasable is False
    assert assess_replay_barrier(no_ids).releasable is False


def test_duplicate_evidence_and_out_of_range_step_fail_closed() -> None:
    evidence = _evidence(SubmissionProvenance.NOT_ATTEMPTED)
    duplicate = _progress(evidence=[evidence, evidence])
    assert assess_replay_barrier(duplicate).releasable is False

    invalid_step = _progress(evidence=[evidence])
    invalid_step.reconciliation_required_step_index = 1
    assert assess_replay_barrier(invalid_step).releasable is False


def test_release_command_reconciles_then_compare_deletes_exact_marker(monkeypatch) -> None:
    progress = _progress(evidence=[_evidence(SubmissionProvenance.ATTEMPTED, tx_ids=["0xaaa", "0xbbb"])])
    marker = progress.to_dict()
    client = SimpleNamespace(disconnect=lambda: None)
    manager = object()
    deleted: list[tuple[object, str, str, dict]] = []

    async def _load(_client, deployment_id):
        assert deployment_id == "deployment:test"
        return manager, object(), marker

    async def _delete(state_manager, deployment_id, key, expected):
        deleted.append((state_manager, deployment_id, key, expected))

    monkeypatch.setattr(recovery_module, "_make_client", lambda _host, _port: client)
    monkeypatch.setattr(recovery_module, "_load_marker", _load)
    monkeypatch.setattr(
        recovery_module,
        "_query_statuses",
        lambda _client, _progress: {"0xaaa": "reverted", "0xbbb": "reverted"},
    )
    monkeypatch.setattr(recovery_module, "compare_and_delete_state_value", _delete)

    result = CliRunner().invoke(
        execution_recovery,
        [
            "release",
            "--deployment-id",
            "deployment:test",
            "--execution-id",
            "execution-1",
            "--apply",
        ],
    )

    assert result.exit_code == 0, result.output
    assert deleted == [(manager, "deployment:test", "execution_progress", marker)]


@pytest.mark.parametrize("status", ["confirmed", "pending", "unknown"])
def test_release_command_never_deletes_without_revert_proof(monkeypatch, status: str) -> None:
    progress = _progress(evidence=[_evidence(SubmissionProvenance.ATTEMPTED, tx_ids=["0xaaa"])])
    marker = progress.to_dict()
    client = SimpleNamespace(disconnect=lambda: None)

    async def _load(_client, _deployment_id):
        return object(), object(), marker

    async def _unexpected_delete(*_args):
        raise AssertionError("unsafe marker delete")

    monkeypatch.setattr(recovery_module, "_make_client", lambda _host, _port: client)
    monkeypatch.setattr(recovery_module, "_load_marker", _load)
    monkeypatch.setattr(recovery_module, "_query_statuses", lambda _client, _progress: {"0xaaa": status})
    monkeypatch.setattr(recovery_module, "compare_and_delete_state_value", _unexpected_delete)

    result = CliRunner().invoke(
        execution_recovery,
        [
            "release",
            "--deployment-id",
            "deployment:test",
            "--execution-id",
            "execution-1",
            "--apply",
        ],
    )

    assert result.exit_code == 1
    assert "release refused" in result.output


def test_release_command_surfaces_compare_delete_race(monkeypatch) -> None:
    progress = _progress(evidence=[_evidence(SubmissionProvenance.NOT_ATTEMPTED)])
    marker = progress.to_dict()
    client = SimpleNamespace(disconnect=lambda: None)

    async def _load(_client, _deployment_id):
        return object(), object(), marker

    async def _changed(*_args):
        raise StateValuePreconditionError("changed")

    monkeypatch.setattr(recovery_module, "_make_client", lambda _host, _port: client)
    monkeypatch.setattr(recovery_module, "_load_marker", _load)
    monkeypatch.setattr(recovery_module, "compare_and_delete_state_value", _changed)

    result = CliRunner().invoke(
        execution_recovery,
        [
            "release",
            "--deployment-id",
            "deployment:test",
            "--execution-id",
            "execution-1",
            "--apply",
        ],
    )

    assert result.exit_code == 1
    assert "marker changed concurrently" in result.output


def _landed_progress() -> ExecutionProgress:
    progress = _progress(evidence=[_evidence(SubmissionProvenance.ATTEMPTED, tx_ids=["0xlanded"])])
    progress.barrier_phase = ExecutionBarrierPhase.LANDED_REPAIR_PENDING
    progress.accounting_pending_step_index = 0
    return progress


def test_landed_repair_seal_requires_confirmed_tx_and_both_repair_references() -> None:
    progress = _landed_progress()
    with pytest.raises(ValueError, match="authoritatively confirmed"):
        seal_landed_repair(
            progress,
            operator="alice",
            accounting_repair_reference="ledger:1",
            strategy_state_repair_reference="state:1",
            transaction_statuses={"0xlanded": "pending"},
        )
    with pytest.raises(ValueError, match="accounting repair reference"):
        seal_landed_repair(
            progress,
            operator="alice",
            accounting_repair_reference="",
            strategy_state_repair_reference="state:1",
            transaction_statuses={"0xlanded": "confirmed"},
        )

    sealed = seal_landed_repair(
        progress,
        operator="alice",
        accounting_repair_reference="ledger:1",
        strategy_state_repair_reference="state:1",
        transaction_statuses={"0xlanded": "confirmed"},
    )
    assert sealed.effective_barrier_phase is ExecutionBarrierPhase.COMPLETED
    assert sealed.intents_hash == "landed-complete"
    assert sealed.accounting_pending_step_index is None
    assert sealed.repair_attestation is not None
    assert sealed.repair_attestation.accounting_repair_reference == "ledger:1"
    assert progress.accounting_pending_step_index == 0  # pure builder


def test_retry_release_can_never_delete_landed_repair_marker() -> None:
    progress = _landed_progress()
    verdict = assess_replay_barrier(progress, {"0xlanded": "reverted"})
    assert verdict.releasable is False
    assert "accounting/state completion" in verdict.reason


def test_intermediate_repair_seal_resumes_next_exact_plan_step() -> None:
    progress = _landed_progress()
    progress.total_steps = 2
    sealed = seal_landed_repair(
        progress,
        operator="alice",
        accounting_repair_reference="ledger:1",
        strategy_state_repair_reference="state:1",
        transaction_statuses={"0xlanded": "confirmed"},
    )
    assert sealed.effective_barrier_phase is ExecutionBarrierPhase.RECOMPILE_REQUIRED
    assert sealed.completed_step_index == 0
    assert sealed.failed_at_step_index == 1
    assert sealed.intents_hash != "landed-complete"


def test_seal_repair_command_uses_exact_compare_replace(monkeypatch) -> None:
    progress = _landed_progress()
    marker = progress.to_dict()
    client = SimpleNamespace(disconnect=lambda: None)
    manager = object()
    replaced: list[tuple[object, str, str, dict, dict]] = []

    async def _load(_client, _deployment_id):
        return manager, object(), marker

    async def _replace(state_manager, deployment_id, key, expected, replacement):
        replaced.append((state_manager, deployment_id, key, expected, replacement))

    monkeypatch.setattr(recovery_module, "_make_client", lambda _host, _port: client)
    monkeypatch.setattr(recovery_module, "_load_marker", _load)
    monkeypatch.setattr(recovery_module, "_query_statuses", lambda _client, _progress: {"0xlanded": "confirmed"})
    monkeypatch.setattr(recovery_module, "compare_and_replace_state_value", _replace)

    result = CliRunner().invoke(
        execution_recovery,
        [
            "seal-repair",
            "--deployment-id",
            "deployment:test",
            "--execution-id",
            "execution-1",
            "--operator",
            "alice",
            "--accounting-repair-ref",
            "ledger:1",
            "--strategy-state-repair-ref",
            "state:1",
            "--apply",
        ],
    )
    assert result.exit_code == 0, result.output
    assert len(replaced) == 1
    assert replaced[0][:4] == (manager, "deployment:test", "execution_progress", marker)
    assert replaced[0][4]["barrier_phase"] == "completed"
