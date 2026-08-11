"""Durable per-step submission evidence is backward-compatible and lossless."""

from almanak.framework.execution.submission import (
    ReplayPolicy,
    SubmissionProvenance,
    SubmissionTransactionEvidence,
    TransactionRole,
)
from almanak.framework.runner.runner_models import ExecutionProgress


def _progress() -> ExecutionProgress:
    return ExecutionProgress(
        execution_id="exec-1",
        deployment_id="deployment-1",
        intents_hash="hash",
        total_steps=2,
    )


def test_per_step_submission_evidence_round_trip() -> None:
    progress = _progress()
    progress.record_submission_evidence(
        step_index=1,
        chain="solana",
        submission_provenance=SubmissionProvenance.ATTEMPTED,
        submitted_transaction_ids=["sig-a", "sig-b"],
        execution_plan_hash="a" * 64,
        submission_transactions=[
            SubmissionTransactionEvidence(
                tx_id="sig-a",
                role=TransactionRole.SETUP_APPROVAL,
                replay_policy=ReplayPolicy.RECOMPILE_ONLY,
            ),
            SubmissionTransactionEvidence(
                tx_id="sig-b",
                role=TransactionRole.ACTION,
                replay_policy=ReplayPolicy.NEVER,
            ),
        ],
    )

    restored = ExecutionProgress.from_dict(progress.to_dict())

    [evidence] = restored.submission_evidence
    assert evidence.step_index == 1
    assert evidence.chain == "solana"
    assert evidence.submission_provenance is SubmissionProvenance.ATTEMPTED
    assert evidence.submitted_transaction_ids == ["sig-a", "sig-b"]
    assert evidence.execution_plan_hash == "a" * 64
    assert evidence.submission_transactions == [
        SubmissionTransactionEvidence("sig-a", TransactionRole.SETUP_APPROVAL, ReplayPolicy.RECOMPILE_ONLY),
        SubmissionTransactionEvidence("sig-b", TransactionRole.ACTION, ReplayPolicy.NEVER),
    ]


def test_legacy_progress_without_submission_evidence_stays_readable() -> None:
    payload = _progress().to_dict()
    payload.pop("submission_evidence")

    assert ExecutionProgress.from_dict(payload).submission_evidence == []


def test_legacy_step_evidence_defaults_new_certification_fields() -> None:
    progress = _progress()
    progress.record_submission_evidence(
        step_index=0,
        chain="arbitrum",
        submission_provenance=SubmissionProvenance.ATTEMPTED,
        submitted_transaction_ids=["0xabc"],
    )
    payload = progress.to_dict()
    payload["submission_evidence"][0].pop("execution_plan_hash")
    payload["submission_evidence"][0].pop("submission_transactions")

    [restored] = ExecutionProgress.from_dict(payload).submission_evidence
    assert restored.execution_plan_hash == ""
    assert restored.submission_transactions == []
