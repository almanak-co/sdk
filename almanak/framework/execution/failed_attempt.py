"""Durable identity and receipt evidence for confirmed, retryable failed attempts."""

from __future__ import annotations

from copy import deepcopy
from typing import Any
from uuid import NAMESPACE_URL, uuid5

from .reconciliation import (
    failed_submission_allows_recompile,
    failed_submission_proves_revert,
    submitted_transaction_hashes,
)


def confirmed_failed_attempt_id(result: Any, *, deployment_id: str, chain: str) -> str | None:
    """Scope one complete failed attempt independently of cycles and retry counters."""
    plan_hash = getattr(result, "execution_plan_hash", "")
    if not (
        failed_submission_proves_revert(result)
        or failed_submission_allows_recompile(result, expected_plan_hash=plan_hash)
    ):
        return None
    if not deployment_id or not chain:
        raise ValueError("Failed attempt identity requires deployment and chain")
    from almanak.core.chains import ChainRegistry

    descriptor = ChainRegistry.try_resolve(chain)
    if descriptor is None:
        return None
    hashes = list(submitted_transaction_hashes(result))
    if descriptor.family.value == "EVM":
        normalized = []
        for value in hashes:
            raw = value[2:] if value.startswith(("0x", "0X")) else value
            if len(raw) != 64 or any(character not in "0123456789abcdefABCDEF" for character in raw):
                return None
            normalized.append("0x" + raw.lower())
        hashes = normalized
    hashes.sort()
    return str(uuid5(NAMESPACE_URL, "almanak:failed-attempt:v1:" + repr((deployment_id, chain.lower(), hashes))))


def retain_failed_attempt_receipts(result: Any, *, attempt_id: str) -> None:
    """Keep measured receipt fields, including nullable additive fees, in ledger JSON."""
    receipts = []
    for transaction in result.transaction_results:
        receipt = transaction.receipt
        if receipt is None:
            raise ValueError("Confirmed failed attempt is missing its typed receipt")
        receipts.append(deepcopy(receipt.to_dict()))
    evidence = {
        "schema_version": 1,
        "ledger_entry_id": attempt_id,
        "receipts": receipts,
        "total_gas_used": result.total_gas_used,
        "measured_gas_cost_wei": str(result.total_gas_cost_wei) if result.total_gas_cost_wei is not None else None,
    }
    existing = result.extracted_data.get("failed_attempt")
    if existing is not None and existing != evidence:
        raise ValueError("Confirmed failed attempt receipt evidence changed")
    result.extracted_data["failed_attempt"] = evidence


def retain_execution_intent_id(result: Any, intent: Any, *, required: bool = False) -> str | None:
    """Bind execution evidence to the original logical intent across retries."""
    identity = getattr(intent, "intent_id", None)
    if not isinstance(identity, str) or not identity.strip():
        if required:
            raise ValueError("Confirmed failed attempt has no logical intent identity")
        return None
    if result is None:
        return None
    extracted = getattr(result, "extracted_data", None)
    if extracted is None:
        extracted = {}
        result.extracted_data = extracted
    if not isinstance(extracted, dict):
        raise ValueError("Execution evidence must be a mapping")
    if "execution_intent_id" in extracted and extracted["execution_intent_id"] != identity:
        raise ValueError("Execution evidence belongs to a different logical intent")
    extracted["execution_intent_id"] = identity
    return identity
