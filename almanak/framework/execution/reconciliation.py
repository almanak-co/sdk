"""Retry-safety helpers for outcomes that may already have reached a chain."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from almanak.framework.execution.submission import (
    ReplayPolicy,
    SubmissionProvenance,
    SubmissionTransactionEvidence,
    TransactionRole,
)

RECONCILIATION_REQUIRED_PREFIX = "BROADCAST_RECONCILIATION_REQUIRED"
_MISSING = object()


def _field(value: Any, name: str) -> Any:
    if isinstance(value, Mapping):
        return value.get(name)
    return getattr(value, name, None)


def _envelopes(result: Any) -> tuple[Any, ...]:
    """Return an execution wrapper followed by its explicit transaction result.

    ``IntentExecutionResult`` is the production envelope used by the multi-leg
    lanes; hashes and receipts live on its ``tx_result``.  Limit traversal to
    that documented seam and guard cycles so malformed adapter objects cannot
    make failure handling recursive or unbounded.
    """
    envelopes: list[Any] = []
    seen: set[int] = set()
    current = result
    while current is not None and len(envelopes) < 4:
        identity = id(current)
        if identity in seen:
            break
        seen.add(identity)
        envelopes.append(current)
        current = _field(current, "tx_result")
    return tuple(envelopes)


def _canonical_hash(candidate: Any) -> tuple[str, str] | None:
    tx_hash = candidate.strip() if isinstance(candidate, str) else ""
    if not tx_hash:
        return None
    canonical = tx_hash.lower().removeprefix("0x") if tx_hash.startswith(("0x", "0X")) else tx_hash
    return tx_hash, canonical


def _receipt_field(receipt: Any, *names: str) -> Any:
    for name in names:
        if isinstance(receipt, Mapping):
            if name in receipt:
                return receipt[name]
        elif hasattr(receipt, name):
            return getattr(receipt, name)
    return _MISSING


def _quantity(value: Any) -> int | None:
    """Parse a non-negative RPC quantity without accepting booleans."""
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value if value >= 0 else None
    if not isinstance(value, str):
        return None
    text = value.strip()
    if not text:
        return None
    try:
        parsed = int(text, 16) if text.startswith(("0x", "0X")) else int(text, 10)
    except ValueError:
        return None
    return parsed if parsed >= 0 else None


def _receipt_proves_revert(receipt: Any, submitted_hash: str) -> bool:
    """Whether a complete receipt matches ``submitted_hash`` and reverted.

    Cardinality alone is never evidence that replay is safe. A receipt must
    retain the canonical transaction identity, block inclusion, gas fields,
    status, and logs emitted by ``TransactionReceipt.to_dict``. Only status
    zero proves the submitted action did not take effect; success or unknown
    status remains an operator-reconciliation incident.
    """
    if receipt is None:
        return False
    receipt_signature = _canonical_hash(_receipt_field(receipt, "signature"))
    expected_hash = _canonical_hash(submitted_hash)
    if receipt_signature is not None:
        slot = _quantity(_receipt_field(receipt, "slot"))
        fee_lamports = _quantity(_receipt_field(receipt, "fee_lamports"))
        return bool(
            expected_hash is not None
            and receipt_signature[1] == expected_hash[1]
            and _receipt_field(receipt, "success") is False
            and slot not in (None, 0)
            and fee_lamports is not None
            and _receipt_field(receipt, "err") not in (_MISSING, None)
            and isinstance(_receipt_field(receipt, "logs"), list | tuple)
        )

    receipt_hash = _canonical_hash(_receipt_field(receipt, "tx_hash", "transaction_hash", "transactionHash"))
    if receipt_hash is None or expected_hash is None or receipt_hash[1] != expected_hash[1]:
        return False
    if _quantity(_receipt_field(receipt, "status")) != 0:
        return False
    required_quantities = (
        _receipt_field(receipt, "block_number", "blockNumber"),
        _receipt_field(receipt, "gas_used", "gasUsed"),
        _receipt_field(receipt, "effective_gas_price", "effectiveGasPrice"),
    )
    if any(_quantity(value) is None for value in required_quantities):
        return False
    block_hash = _receipt_field(receipt, "block_hash", "blockHash")
    if not isinstance(block_hash, str) or not block_hash.strip():
        return False
    logs = _receipt_field(receipt, "logs")
    return isinstance(logs, list | tuple)


def _complete_receipt_error(
    receipt: Any,
    submitted_hash: str,
    index: int,
    *,
    solana: bool,
    require_success: bool,
) -> str | None:
    if not isinstance(receipt, Mapping):
        return f"receipt {index} is not an object"
    identity_names = ("signature",) if solana else ("tx_hash", "transaction_hash", "transactionHash")
    receipt_hash = _canonical_hash(_receipt_field(receipt, *identity_names))
    expected_hash = _canonical_hash(submitted_hash)
    if receipt_hash is None:
        return f"receipt {index} has no {'signature' if solana else 'transaction hash'}"
    if expected_hash is None or receipt_hash[1] != expected_hash[1]:
        return f"receipt {index} identity does not match submitted transaction"
    if solana:
        slot, fee = _quantity(_receipt_field(receipt, "slot")), _quantity(_receipt_field(receipt, "fee_lamports"))
        success = _receipt_field(receipt, "success")
        err = _receipt_field(receipt, "err")
        valid = (
            isinstance(success, bool)
            and (not require_success or success is True)
            and slot is not None
            and slot > 0
            and fee is not None
            and ((success is True and err in (_MISSING, None)) or (success is False and err not in (_MISSING, None)))
        )
    else:
        quantities = (
            _receipt_field(receipt, "block_number", "blockNumber"),
            _receipt_field(receipt, "gas_used", "gasUsed"),
            _receipt_field(receipt, "effective_gas_price", "effectiveGasPrice"),
        )
        block_hash = _receipt_field(receipt, "block_hash", "blockHash")
        status = _quantity(_receipt_field(receipt, "status"))
        valid = (
            status in ({1} if require_success else {0, 1})
            and all(_quantity(value) is not None for value in quantities)
            and isinstance(block_hash, str)
            and bool(block_hash.strip())
        )
    return (
        None
        if valid and isinstance(_receipt_field(receipt, "logs"), list | tuple)
        else f"receipt {index} is incomplete"
    )


def successful_receipt_set_error(
    tx_hashes: Any,
    receipts: Any,
    *,
    solana: bool = False,
) -> str | None:
    return complete_receipt_set_error(tx_hashes, receipts, solana=solana, require_success=True)


def complete_receipt_set_error(
    tx_hashes: Any,
    receipts: Any,
    *,
    solana: bool = False,
    require_success: bool = False,
) -> str | None:
    """Validate complete identity-preserving receipts.

    Complete failed receipts are serializable evidence. Callers asserting an
    execution success set ``require_success=True`` so status-zero/failed
    receipts remain invalid on that stricter boundary.
    """
    if isinstance(tx_hashes, str) or not isinstance(tx_hashes, list | tuple):
        return "submitted transaction identifiers are not an array"
    if not isinstance(receipts, list | tuple):
        return "receipts are not an array"
    if len(tx_hashes) != len(receipts):
        return f"{len(tx_hashes)} submitted transaction identifiers != {len(receipts)} receipts"

    seen: set[str] = set()
    for index, (raw_hash, receipt) in enumerate(zip(tx_hashes, receipts, strict=True)):
        normalized = _canonical_hash(raw_hash)
        if normalized is None:
            return f"submitted transaction identifier {index} is blank"
        if normalized[1] in seen:
            return f"submitted transaction identifier {index} is duplicated"
        seen.add(normalized[1])
        if error := _complete_receipt_error(
            receipt,
            normalized[0],
            index,
            solana=solana,
            require_success=require_success,
        ):
            return error
    return None


def submitted_transaction_hashes(result: Any) -> tuple[str, ...]:
    """Return unique, non-blank transaction hashes retained by an outcome.

    Gateway outcomes carry ``tx_hashes`` even when their receipt set is
    unusable. Local outcomes usually carry hashes on ``transaction_results``.
    Multi-leg execution wraps either shape in ``IntentExecutionResult`` and
    stores it under ``tx_result``. Reading every documented shape is
    load-bearing: receipt conversion must never erase evidence that submission
    already happened.
    """
    hashes: list[str] = []
    seen: set[str] = set()
    for envelope in _envelopes(result):
        # EVM/gateway producers call this carrier ``tx_hashes``; the Solana
        # planner's chain-neutral ExecutionOutcome calls it ``tx_ids``.
        plural = _field(envelope, "tx_hashes") or _field(envelope, "tx_ids") or ()
        if isinstance(plural, str):
            plural = (plural,)
        candidates: list[Any] = list(plural)
        child_candidates = [
            _field(transaction_result, "tx_hash")
            for transaction_result in (_field(envelope, "transaction_results") or ())
        ]
        candidates.extend(child_candidates)
        # Older adapters and direct consumers expose only the singular action
        # hash. Keep it as a fallback after the ordered per-transaction carrier.
        candidates.append(_field(envelope, "tx_hash"))
        for candidate in candidates:
            normalized = _canonical_hash(candidate)
            if normalized is None:
                continue
            tx_hash, canonical = normalized
            if canonical not in seen:
                seen.add(canonical)
                hashes.append(tx_hash)
    return tuple(hashes)


def _has_duplicate_submitted_hash_evidence(result: Any) -> bool:
    """Reject duplicate hashes inside any authoritative submitted-hash carrier.

    Compatibility surfaces may echo the same transaction (for example a
    gateway envelope exposes both ``tx_hashes`` and derived
    ``transaction_results``), so duplicates are evaluated only inside the
    highest-fidelity carrier on each envelope. A duplicate inside that carrier
    makes the receipt set contradictory even when every paired receipt claims
    a revert: there is no one-to-one submitted-transaction identity to prove.
    """
    for envelope in _envelopes(result):
        plural = _field(envelope, "tx_hashes") or _field(envelope, "tx_ids") or ()
        if isinstance(plural, str):
            plural = (plural,)
        canonical_hashes = [normalized[1] for item in plural if (normalized := _canonical_hash(item)) is not None]
        if canonical_hashes:
            if len(canonical_hashes) != len(set(canonical_hashes)):
                return True
            continue

        transaction_results = _field(envelope, "transaction_results") or ()
        canonical_hashes = [
            normalized[1]
            for transaction_result in transaction_results
            if (normalized := _canonical_hash(_field(transaction_result, "tx_hash"))) is not None
        ]
        if len(canonical_hashes) != len(set(canonical_hashes)):
            return True
    return False


def _proven_reverted_transaction_hashes(result: Any) -> set[str]:
    """Return submitted hashes with complete matching status-zero receipts."""
    reverted: set[str] = set()
    for envelope in _envelopes(result):
        plural = _field(envelope, "tx_hashes") or _field(envelope, "tx_ids") or ()
        if isinstance(plural, str):
            plural = (plural,)
        plural_hashes = [normalized for item in plural if (normalized := _canonical_hash(item)) is not None]
        if plural_hashes:
            receipts = _field(envelope, "receipts")
            if isinstance(receipts, list | tuple) and len(receipts) == len(plural_hashes):
                reverted.update(
                    canonical
                    for ((tx_hash, canonical), receipt) in zip(plural_hashes, receipts, strict=True)
                    if _receipt_proves_revert(receipt, tx_hash)
                )
            # Gateway envelopes expose a derived singular ``tx_hash`` too. The
            # plural positional contract is authoritative for that shape.
            continue

        transaction_results = _field(envelope, "transaction_results") or ()
        if transaction_results:
            for transaction_result in transaction_results:
                normalized = _canonical_hash(_field(transaction_result, "tx_hash"))
                if normalized is not None and _receipt_proves_revert(
                    _field(transaction_result, "receipt"), normalized[0]
                ):
                    reverted.add(normalized[1])
            continue

        singular = _canonical_hash(_field(envelope, "tx_hash"))
        if singular is not None and _receipt_proves_revert(_field(envelope, "receipt"), singular[0]):
            reverted.add(singular[1])
    return reverted


def failed_submission_requires_reconciliation(result: Any) -> bool:
    """Whether a failed result has broadcasts without complete receipt evidence.

    A complete, matching status-zero receipt for every submitted hash proves
    that an on-chain retry cannot duplicate successful work. Any successful,
    unknown, malformed, mismatched, absent, or partial receipt is an
    operator-reconciliation incident.
    """
    success = result.get("success", False) if isinstance(result, Mapping) else getattr(result, "success", False)
    if bool(success):
        return False
    provenance = submission_provenance(result)
    submitted = submitted_transaction_hashes(result)
    if provenance is SubmissionProvenance.NOT_ATTEMPTED:
        # Transaction identity is direct evidence that submission crossed the
        # boundary. It dominates a contradictory NOT_ATTEMPTED stamp from a
        # malformed or skewed producer; teardown and ordinary execution must
        # fail closed on the contradiction rather than retry a landed tx.
        return bool(submitted)
    if provenance in {SubmissionProvenance.ATTEMPTED, SubmissionProvenance.UNSPECIFIED}:
        return not failed_submission_proves_revert(result)
    if not submitted:
        return False
    return not failed_submission_proves_revert(result)


def submission_provenance(result: Any) -> SubmissionProvenance:
    """Read typed provenance across local/gateway/outcome envelopes.

    Missing and malformed fields are UNSPECIFIED, never NOT_ATTEMPTED. If an
    outer compatibility envelope is unspecified but its documented inner
    result is authoritative, use the inner value.
    """
    for envelope in _envelopes(result):
        parsed = SubmissionProvenance.parse(_field(envelope, "submission_provenance"))
        if parsed is not SubmissionProvenance.UNSPECIFIED:
            return parsed
    return SubmissionProvenance.UNSPECIFIED


def failed_submission_proves_revert(result: Any) -> bool:
    """Whether every submitted transaction is conclusively proven reverted.

    Unlike ``failed_submission_requires_reconciliation``, this predicate is
    intentionally false when no submitted identity was retained. Callers that
    have already crossed a broadcast boundary must not interpret missing hash
    evidence as proof that nothing was submitted.
    """
    success = result.get("success", False) if isinstance(result, Mapping) else getattr(result, "success", False)
    if bool(success):
        return False
    submitted = submitted_transaction_hashes(result)
    if not submitted or _has_duplicate_submitted_hash_evidence(result):
        return False
    proven_reverted = _proven_reverted_transaction_hashes(result)
    return all((_canonical_hash(tx_hash) or ("", ""))[1] in proven_reverted for tx_hash in submitted)


def _recompile_envelope_is_safe(envelope: Any) -> bool:
    tx_hashes = _field(envelope, "tx_hashes") or _field(envelope, "tx_ids")
    receipts = _field(envelope, "receipts")
    raw_evidence = _field(envelope, "submission_transactions")
    if not isinstance(tx_hashes, list | tuple) or not tx_hashes:
        return False
    if not isinstance(receipts, list | tuple) or not isinstance(raw_evidence, list | tuple):
        return False
    if len(raw_evidence) != len(tx_hashes) or complete_receipt_set_error(tx_hashes, receipts) is not None:
        return False

    evidence = [SubmissionTransactionEvidence.from_value(item) for item in raw_evidence]
    if any(item is None for item in evidence):
        return False
    saw_landed_setup = False
    for tx_hash, receipt, item in zip(tx_hashes, receipts, evidence, strict=True):
        assert item is not None
        expected_id = _canonical_hash(tx_hash)
        actual_id = _canonical_hash(item.tx_id)
        if expected_id is None or actual_id is None or expected_id[1] != actual_id[1]:
            return False
        status = _quantity(_receipt_field(receipt, "status"))
        if item.role is TransactionRole.ACTION:
            if item.replay_policy is not ReplayPolicy.NEVER or status != 0:
                return False
        elif item.role is TransactionRole.SETUP_APPROVAL:
            if item.replay_policy is not ReplayPolicy.RECOMPILE_ONLY or status not in {0, 1}:
                return False
            saw_landed_setup = saw_landed_setup or status == 1
        else:
            return False
    return saw_landed_setup


def failed_submission_allows_recompile(result: Any, *, expected_plan_hash: str) -> bool:
    """Prove that only replay-elidable setup landed in a failed plan.

    This predicate never authorizes replay of the original bundle.  A true
    result permits only a fresh compile after current prices and allowances
    have been re-read.  Gateway-certified physical transaction roles, exact
    plan binding, and a complete positional receipt set are mandatory.
    """
    success = result.get("success", False) if isinstance(result, Mapping) else getattr(result, "success", False)
    valid_hash = (
        isinstance(expected_plan_hash, str)
        and len(expected_plan_hash) == 64
        and expected_plan_hash == expected_plan_hash.lower()
        and all(character in "0123456789abcdef" for character in expected_plan_hash)
    )
    if bool(success) or submission_provenance(result) is not SubmissionProvenance.ATTEMPTED or not valid_hash:
        return False
    return any(
        _recompile_envelope_is_safe(envelope)
        for envelope in _envelopes(result)
        if _field(envelope, "execution_plan_hash") == expected_plan_hash
    )


def reconciliation_required_error(result: Any) -> str:
    """Build the stable terminal error consumed by both runner retry engines."""
    hashes = submitted_transaction_hashes(result)
    error = result.get("error") if isinstance(result, Mapping) else getattr(result, "error", None)
    original = str(error or "execution failed after transaction submission")
    if hashes:
        evidence = f"transaction submission returned known hash(es) {list(hashes)!r}"
    else:
        evidence = "execution crossed the submission boundary without retaining a transaction identifier"
    return f"{RECONCILIATION_REQUIRED_PREFIX}: {evidence}; refusing automatic replay until reconciled: {original}"


__all__ = [
    "RECONCILIATION_REQUIRED_PREFIX",
    "complete_receipt_set_error",
    "failed_submission_allows_recompile",
    "failed_submission_proves_revert",
    "failed_submission_requires_reconciliation",
    "reconciliation_required_error",
    "successful_receipt_set_error",
    "submission_provenance",
    "submitted_transaction_hashes",
]
