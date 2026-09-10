"""Validate action completion from fresh canonical receipt observations."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from eth_utils import keccak
from hexbytes import HexBytes

from almanak.framework.execution.interfaces import TransactionReceipt
from almanak.framework.execution.nonce_recovery import build_complete_evm_receipt
from almanak.framework.execution.submission import SubmissionProvenance, SubmissionTransactionEvidence


class PlanCompletionUnproven(ValueError):
    """Evidence cannot authorize accounting or callbacks for a completed plan."""


_SAFE_SUCCESS = keccak(text="ExecutionSuccess(bytes32,uint256)")
_SAFE_FAILURE = keccak(text="ExecutionFailure(bytes32,uint256)")
_MODULE_SUCCESS = keccak(text="ExecutionFromModuleSuccess(address)")
_MODULE_FAILURE = keccak(text="ExecutionFromModuleFailure(address)")
_SAFE_OUTCOMES = {_SAFE_SUCCESS, _SAFE_FAILURE, _MODULE_SUCCESS, _MODULE_FAILURE}


def _identity(value: Any, size: int) -> bytes:
    try:
        if not isinstance(value, str | bytes) or isinstance(value, str) and not value.startswith(("0x", "0X")):
            raise ValueError("identity must be hex encoded")
        result = bytes(HexBytes(value))
        if len(result) != size:
            raise ValueError("identity has incorrect width")
        return result
    except (TypeError, ValueError) as exc:
        raise PlanCompletionUnproven("Malformed transaction, block, or wallet identity") from exc


def _hash_identity(value: Any) -> bytes:
    # Web3 object hashes and RPC JSON encode the same 32 bytes with different prefixes.
    if isinstance(value, str) and len(value) == 64 and all(c in "0123456789abcdefABCDEF" for c in value):
        value = "0x" + value
    return _identity(value, 32)


def _safe_event_success(log: dict[str, Any]) -> bool | None:
    topics = log.get("topics")
    if not isinstance(topics, list | tuple) or not topics:
        raise PlanCompletionUnproven("Safe log topics missing")
    signature = _hash_identity(topics[0])
    if signature not in _SAFE_OUTCOMES:
        return None
    try:
        if not isinstance(log.get("data"), str | bytes):
            raise ValueError("event data must be hex encoded")
        data = bytes(HexBytes(log["data"]))
        for topic in topics:
            _hash_identity(topic)
        if signature in {_MODULE_SUCCESS, _MODULE_FAILURE}:
            valid_shape = len(topics) == 2 and not data and _hash_identity(topics[1])[:12] == bytes(12)
        else:
            # Safe 1.3 stores txHash in data; Safe 1.4 indexes it.
            valid_shape = (len(topics) == 1 and len(data) == 64) or (len(topics) == 2 and len(data) == 32)
        if not valid_shape:
            raise ValueError("invalid Safe outcome ABI")
    except (KeyError, TypeError, ValueError) as exc:
        raise PlanCompletionUnproven("Malformed Safe outcome event") from exc
    return signature in {_SAFE_SUCCESS, _MODULE_SUCCESS}


def _require_safe_success(receipt: TransactionReceipt, safe_address: str) -> None:
    safe = _identity(safe_address, 20)
    outcomes: list[bool] = []
    for log in receipt.logs:
        if not isinstance(log, dict):
            raise PlanCompletionUnproven("Malformed receipt log")
        if _identity(log.get("address"), 20) != safe:
            continue
        if log.get("removed", False) is not False:
            raise PlanCompletionUnproven("Removed Safe event")
        outcome = _safe_event_success(log)
        if outcome is not None:
            outcomes.append(outcome)
    if outcomes != [True]:
        raise PlanCompletionUnproven("Safe inner execution is not uniquely proven successful")


def _ordered_plan_evidence(
    evidence: Sequence[SubmissionTransactionEvidence], submitted_tx_ids: Sequence[str]
) -> list[SubmissionTransactionEvidence]:
    ids = [_hash_identity(tx_id) for tx_id in submitted_tx_ids]
    if not ids or len(ids) != len(set(ids)) or len(ids) != len(evidence):
        raise PlanCompletionUnproven("Submitted transaction coverage is missing or ambiguous")
    count = evidence[0].plan_transaction_count
    if type(count) is not int or count <= 0:
        raise PlanCompletionUnproven("Original plan size is unmeasured")
    covered: set[int] = set()
    bound_ids: set[bytes] = set()
    wallets: set[str] = set()
    for item in evidence:
        tx_id = _hash_identity(item.tx_id)
        indices = item.plan_indices
        if (
            type(item.plan_transaction_count) is not int
            or item.plan_transaction_count != count
            or not isinstance(indices, tuple | list)
            or not indices
            or any(type(index) is not int or not 0 <= index < count for index in indices)
            or len(set(indices)) != len(indices)
            or covered.intersection(indices)
            or tx_id in bound_ids
        ):
            raise PlanCompletionUnproven("Plan coverage overlaps or contradicts the original plan")
        if not isinstance(item.safe_address, str):
            raise PlanCompletionUnproven("Invalid Safe identity")
        wallets.add(_identity(item.safe_address, 20).hex() if item.safe_address else "")
        if len(indices) > 1 and not item.safe_address:
            raise PlanCompletionUnproven("Multiple logical transactions lack atomic Safe identity")
        covered.update(indices)
        bound_ids.add(tx_id)
    if len(covered) != count or bound_ids != set(ids) or len(wallets) != 1:
        raise PlanCompletionUnproven("The submitted set does not cover one complete wallet plan")
    return sorted(evidence, key=lambda item: min(item.plan_indices))


def prove_completed_evm_plan(
    *,
    expected_plan_hash: str,
    observed_plan_hash: str,
    provenance: SubmissionProvenance,
    submitted_tx_ids: Sequence[str],
    evidence: Sequence[SubmissionTransactionEvidence],
    receipts: Sequence[TransactionReceipt],
) -> tuple[TransactionReceipt, ...]:
    """Return receipts in logical plan order, or retain the recovery barrier.

    Callers must obtain fresh receipts through the canonical gateway observer.
    This function proves full plan coverage and Safe inner outcomes; it neither
    writes accounting nor authorizes replay, and it cannot establish finality.
    """
    if (
        provenance is not SubmissionProvenance.ATTEMPTED
        or not isinstance(expected_plan_hash, str)
        or len(expected_plan_hash) != 64
        or any(character not in "0123456789abcdef" for character in expected_plan_hash)
        or observed_plan_hash != expected_plan_hash
    ):
        raise PlanCompletionUnproven("Original execution plan identity is unproven")
    ordered = _ordered_plan_evidence(evidence, submitted_tx_ids)
    by_hash: dict[bytes, TransactionReceipt] = {}
    for receipt in receipts:
        tx_id = _hash_identity(receipt.tx_hash)
        _hash_identity(receipt.block_hash)
        if tx_id in by_hash or build_complete_evm_receipt(receipt.to_dict(), expected_tx_hash=receipt.tx_hash) is None:
            raise PlanCompletionUnproven("Receipt set is incomplete or ambiguous")
        if not receipt.success:
            raise PlanCompletionUnproven("A submitted transaction reverted")
        by_hash[tx_id] = receipt
    if set(by_hash) != {_hash_identity(item.tx_id) for item in ordered}:
        raise PlanCompletionUnproven("Canonical receipts do not cover the submitted transaction set")
    result: list[TransactionReceipt] = []
    for item in ordered:
        receipt = by_hash[_hash_identity(item.tx_id)]
        if item.safe_address:
            _require_safe_success(receipt, item.safe_address)
        result.append(receipt)
    return tuple(result)
