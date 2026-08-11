"""Operator recovery for a durable execution replay barrier.

The command is intentionally conservative.  Inspection is always safe, while
release for retry requires exact marker identity plus authoritative gateway
evidence that the marked step was not submitted or that every submitted
transaction reverted.  Unknown, pending, confirmed, legacy, and malformed
evidence remain blocked.
"""

from __future__ import annotations

import asyncio
import json
from dataclasses import asdict, dataclass
from typing import Any

import click

from almanak.config.cli_options import gateway_client_options
from almanak.framework.gateway_client import GatewayClient, GatewayClientConfig
from almanak.framework.runner.runner_models import (
    ExecutionBarrierPhase,
    ExecutionProgress,
    ExecutionRepairAttestation,
    SubmissionProvenance,
)
from almanak.framework.state import (
    StateValuePreconditionError,
    compare_and_delete_state_value,
    compare_and_replace_state_value,
)
from almanak.framework.state.gateway_state_manager import GatewayStateManager
from almanak.gateway.proto import gateway_pb2


@dataclass(frozen=True)
class RecoveryAssessment:
    """Fail-closed release verdict for one durable replay barrier."""

    releasable: bool
    reason: str
    step_index: int | None
    chain: str | None
    transaction_statuses: dict[str, str]


def assess_replay_barrier(
    progress: ExecutionProgress,
    transaction_statuses: dict[str, str] | None = None,
) -> RecoveryAssessment:
    """Decide whether retry release is proven safe.

    ``transaction_statuses`` is keyed by the exact submitted transaction id.
    Only ``reverted`` is a release proof for an attempted submission.  This
    deliberately treats absent/novel gateway statuses as unknown so protocol
    skew cannot turn uncertainty into permission to rebroadcast.
    """
    statuses = transaction_statuses or {}
    if progress.is_accounting_pending:
        return RecoveryAssessment(
            False,
            "broadcast landed; accounting/state completion is pending",
            progress.accounting_pending_step_index,
            None,
            statuses,
        )
    step_index = progress.reconciliation_required_step_index
    if step_index is None:
        return RecoveryAssessment(False, "marker does not request broadcast reconciliation", None, None, statuses)
    if step_index < 0 or step_index >= progress.total_steps:
        return RecoveryAssessment(False, "marker step index is outside the sealed plan", step_index, None, statuses)

    matching_evidence = [item for item in progress.submission_evidence if item.step_index == step_index]
    if len(matching_evidence) != 1:
        return RecoveryAssessment(
            False,
            "legacy or malformed marker does not have exactly one per-step submission record",
            step_index,
            None,
            statuses,
        )
    evidence = matching_evidence[0]
    if evidence.submission_provenance is SubmissionProvenance.NOT_ATTEMPTED:
        if evidence.submitted_transaction_ids:
            return RecoveryAssessment(
                False,
                "NOT_ATTEMPTED evidence contradicts non-empty transaction ids",
                step_index,
                evidence.chain,
                statuses,
            )
        return RecoveryAssessment(True, "gateway proved submission was not attempted", step_index, evidence.chain, {})
    if evidence.submission_provenance is not SubmissionProvenance.ATTEMPTED:
        return RecoveryAssessment(
            False,
            "submission provenance is unspecified; mixed-version markers fail closed",
            step_index,
            evidence.chain,
            statuses,
        )
    if not evidence.chain or not evidence.submitted_transaction_ids:
        return RecoveryAssessment(
            False,
            "attempted submission lacks exact chain or transaction identity",
            step_index,
            evidence.chain,
            statuses,
        )

    observed = {tx_id: statuses.get(tx_id, "unknown").lower() for tx_id in evidence.submitted_transaction_ids}
    if all(status == "reverted" for status in observed.values()):
        return RecoveryAssessment(
            True,
            "gateway proved every submitted transaction reverted",
            step_index,
            evidence.chain,
            observed,
        )
    return RecoveryAssessment(
        False,
        "at least one submitted transaction is confirmed, pending, unknown, or unobserved",
        step_index,
        evidence.chain,
        observed,
    )


def seal_landed_repair(
    progress: ExecutionProgress,
    *,
    operator: str,
    accounting_repair_reference: str,
    strategy_state_repair_reference: str,
    transaction_statuses: dict[str, str],
) -> ExecutionProgress:
    """Build a completed tombstone after explicit landed-work repair.

    This is not a retry release.  The transaction must still be authoritatively
    confirmed and both accounting and strategy-state repairs must be named.
    The caller persists the returned marker with full-value CAS.
    """
    if progress.effective_barrier_phase is not ExecutionBarrierPhase.LANDED_REPAIR_PENDING:
        raise ValueError("marker is not awaiting landed-work repair")
    for label, value in (
        ("operator", operator),
        ("accounting repair reference", accounting_repair_reference),
        ("strategy-state repair reference", strategy_state_repair_reference),
    ):
        if not value.strip():
            raise ValueError(f"{label} must be non-empty")

    step_index = progress.accounting_pending_step_index
    if step_index is None:
        # Backward-compatible single-chain landed marker.  It has one sealed
        # step but older writers did not populate accounting_pending_step_index.
        if progress.total_steps != 1 or progress.intents_hash != "landed-accounting-pending":
            raise ValueError("landed marker does not identify one repair-pending step")
        step_index = 0
    evidence = [item for item in progress.submission_evidence if item.step_index == step_index]
    if len(evidence) != 1:
        raise ValueError("landed marker must contain exactly one submission record for the repaired step")
    submitted = evidence[0]
    if (
        submitted.submission_provenance is not SubmissionProvenance.ATTEMPTED
        or not submitted.chain
        or not submitted.submitted_transaction_ids
    ):
        raise ValueError("landed repair requires exact attempted transaction identity")
    observed = {
        tx_id: transaction_statuses.get(tx_id, "unknown").lower() for tx_id in submitted.submitted_transaction_ids
    }
    if not all(status == "confirmed" for status in observed.values()):
        raise ValueError("every repaired transaction must be authoritatively confirmed")

    sealed = ExecutionProgress.from_dict(progress.to_dict())
    attestation = ExecutionRepairAttestation(
        operator=operator.strip(),
        accounting_repair_reference=accounting_repair_reference.strip(),
        strategy_state_repair_reference=strategy_state_repair_reference.strip(),
    )
    sealed.seal_repaired_step(step_index, attestation)
    return sealed


def _make_client(host: str, port: int) -> GatewayClient:
    config = GatewayClientConfig.from_env()
    config.host = "127.0.0.1" if host == "localhost" else host
    config.port = port
    client = GatewayClient(config)
    client.connect()
    if not client.health_check():
        client.disconnect()
        raise click.ClickException(f"gateway at {config.host}:{config.port} is not healthy")
    return client


async def _load_marker(client: GatewayClient, deployment_id: str) -> tuple[GatewayStateManager, Any, dict[str, Any]]:
    manager = GatewayStateManager(client)
    state = await manager.load_state(deployment_id)
    if state is None:
        raise click.ClickException(f"no durable strategy state exists for {deployment_id}")
    marker = state.state.get("execution_progress")
    if not isinstance(marker, dict):
        raise click.ClickException(f"no durable execution replay barrier exists for {deployment_id}")
    return manager, state, marker


def _query_statuses(client: GatewayClient, progress: ExecutionProgress) -> dict[str, str]:
    step_index = (
        progress.accounting_pending_step_index
        if progress.is_accounting_pending
        else progress.reconciliation_required_step_index
    )
    if step_index is None and progress.intents_hash == "landed-accounting-pending" and progress.total_steps == 1:
        step_index = 0
    matching_evidence = [item for item in progress.submission_evidence if item.step_index == step_index]
    if len(matching_evidence) != 1:
        return {}
    evidence = matching_evidence[0]
    if evidence.submission_provenance is not SubmissionProvenance.ATTEMPTED:
        return {}
    statuses: dict[str, str] = {}
    for tx_id in evidence.submitted_transaction_ids:
        response = client.execution.GetTransactionStatus(
            gateway_pb2.TxStatusRequest(tx_hash=tx_id, chain=evidence.chain),
            timeout=30,
        )
        statuses[tx_id] = response.status or "unknown"
    return statuses


def _render(progress: ExecutionProgress, assessment: RecoveryAssessment, *, as_json: bool) -> None:
    payload = {
        "execution_id": progress.execution_id,
        "intents_hash": progress.intents_hash,
        "assessment": asdict(assessment),
        "progress": progress.to_dict(),
    }
    if as_json:
        click.echo(json.dumps(payload, indent=2, sort_keys=True, default=str))
        return
    click.echo(f"execution_id: {progress.execution_id}")
    click.echo(f"intents_hash: {progress.intents_hash}")
    click.echo(f"step_index: {assessment.step_index}")
    click.echo(f"chain: {assessment.chain or '-'}")
    for tx_id, status in assessment.transaction_statuses.items():
        click.echo(f"transaction {tx_id}: {status}")
    click.echo(f"release_for_retry: {'SAFE' if assessment.releasable else 'BLOCKED'}")
    click.echo(f"reason: {assessment.reason}")


@click.group("execution-recovery")
def execution_recovery() -> None:
    """Inspect or safely release a durable no-rebroadcast barrier."""


def _common_options(command):  # noqa: ANN001, ANN202 - Click decorator
    command = click.option("--json", "as_json", is_flag=True, help="Emit machine-readable JSON.")(command)
    command = click.option("--deployment-id", "-s", required=True)(command)
    return gateway_client_options(command)


@execution_recovery.command("inspect")
@_common_options
def inspect_recovery(deployment_id: str, as_json: bool, gateway_host: str, gateway_port: int) -> None:
    """Inspect the persisted barrier without querying chain status."""
    client = _make_client(gateway_host, gateway_port)
    try:
        _, _, marker = asyncio.run(_load_marker(client, deployment_id))
        progress = ExecutionProgress.from_dict(marker)
        _render(progress, assess_replay_barrier(progress), as_json=as_json)
    finally:
        client.disconnect()


@execution_recovery.command("reconcile")
@_common_options
def reconcile_recovery(deployment_id: str, as_json: bool, gateway_host: str, gateway_port: int) -> None:
    """Query authoritative transaction status and print the release verdict."""
    client = _make_client(gateway_host, gateway_port)
    try:
        _, _, marker = asyncio.run(_load_marker(client, deployment_id))
        progress = ExecutionProgress.from_dict(marker)
        assessment = assess_replay_barrier(progress, _query_statuses(client, progress))
        _render(progress, assessment, as_json=as_json)
        if not assessment.releasable:
            raise click.exceptions.Exit(2)
    finally:
        client.disconnect()


@execution_recovery.command("release")
@click.option(
    "--execution-id",
    required=True,
    help="Exact execution id printed by inspect; prevents releasing a replacement marker.",
)
@click.option("--apply", is_flag=True, help="Apply the compare-and-delete after live reconciliation.")
@_common_options
def release_recovery(
    deployment_id: str,
    execution_id: str,
    apply: bool,
    as_json: bool,
    gateway_host: str,
    gateway_port: int,
) -> None:
    """Release for retry only after exact authoritative no-land proof."""
    if not apply:
        raise click.UsageError("release is mutating; pass --apply after reviewing `execution-recovery reconcile`")
    client = _make_client(gateway_host, gateway_port)
    try:
        manager, _, marker = asyncio.run(_load_marker(client, deployment_id))
        progress = ExecutionProgress.from_dict(marker)
        if progress.execution_id != execution_id:
            raise click.ClickException("execution id changed; inspect the current marker before retrying release")
        assessment = assess_replay_barrier(progress, _query_statuses(client, progress))
        _render(progress, assessment, as_json=as_json)
        if not assessment.releasable:
            raise click.ClickException("release refused: replay safety has not been proven")
        try:
            asyncio.run(compare_and_delete_state_value(manager, deployment_id, "execution_progress", marker))
        except StateValuePreconditionError as exc:
            raise click.ClickException("marker changed concurrently; nothing was released") from exc
        if not as_json:
            click.secho("Replay barrier released for retry.", fg="green")
    finally:
        client.disconnect()


@execution_recovery.command("seal-repair")
@click.option("--execution-id", required=True, help="Exact execution id printed by inspect.")
@click.option("--operator", required=True, help="Operator identity accepting the repair evidence.")
@click.option("--accounting-repair-ref", required=True, help="Immutable ledger/accounting repair reference.")
@click.option("--strategy-state-repair-ref", required=True, help="Immutable strategy-state repair reference.")
@click.option("--apply", is_flag=True, help="Apply the exact-CAS repair seal.")
@_common_options
def seal_repair(
    deployment_id: str,
    execution_id: str,
    operator: str,
    accounting_repair_ref: str,
    strategy_state_repair_ref: str,
    apply: bool,
    as_json: bool,
    gateway_host: str,
    gateway_port: int,
) -> None:
    """Seal landed work only after accounting and strategy state were repaired."""
    if not apply:
        raise click.UsageError("seal-repair is mutating; pass --apply after independently verifying both repairs")
    client = _make_client(gateway_host, gateway_port)
    try:
        manager, _, marker = asyncio.run(_load_marker(client, deployment_id))
        progress = ExecutionProgress.from_dict(marker)
        if progress.execution_id != execution_id:
            raise click.ClickException("execution id changed; inspect the current marker before sealing repair")
        try:
            sealed = seal_landed_repair(
                progress,
                operator=operator,
                accounting_repair_reference=accounting_repair_ref,
                strategy_state_repair_reference=strategy_state_repair_ref,
                transaction_statuses=_query_statuses(client, progress),
            )
            asyncio.run(
                compare_and_replace_state_value(
                    manager,
                    deployment_id,
                    "execution_progress",
                    marker,
                    sealed.to_dict(),
                )
            )
        except ValueError as exc:
            raise click.ClickException(f"repair seal refused: {exc}") from exc
        except StateValuePreconditionError as exc:
            raise click.ClickException("marker changed concurrently; nothing was sealed") from exc
        if as_json:
            click.echo(json.dumps(sealed.to_dict(), indent=2, sort_keys=True))
        else:
            click.secho("Landed-work repair sealed; replay remains forbidden.", fg="green")
    finally:
        client.disconnect()


if __name__ == "__main__":
    execution_recovery()
