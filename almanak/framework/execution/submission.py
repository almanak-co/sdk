"""Typed provenance for whether an execution crossed the submit boundary."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from dataclasses import dataclass
from enum import StrEnum
from typing import Any


class SubmissionProvenance(StrEnum):
    """Authoritative knowledge about transaction submission.

    ``UNSPECIFIED`` is deliberately conservative. It is the protobuf zero
    value and the fallback for legacy/malformed producers, so mixed-version
    deployments never turn absence of evidence into permission to replay.
    """

    UNSPECIFIED = "UNSPECIFIED"
    NOT_ATTEMPTED = "NOT_ATTEMPTED"
    ATTEMPTED = "ATTEMPTED"

    @classmethod
    def parse(cls, value: Any) -> SubmissionProvenance:
        """Parse an internal/string value, failing closed on unknown input."""
        if isinstance(value, cls):
            return value
        if isinstance(value, str):
            normalized = value.strip().upper()
            for member in cls:
                if normalized in {member.name, member.value}:
                    return member
        return cls.UNSPECIFIED


class TransactionRole(StrEnum):
    """Gateway-certified role of one submitted transaction in a sealed plan."""

    UNKNOWN = "UNKNOWN"
    SETUP_APPROVAL = "SETUP_APPROVAL"
    ACTION = "ACTION"

    @classmethod
    def parse(cls, value: Any) -> TransactionRole:
        if isinstance(value, cls):
            return value
        if isinstance(value, str):
            normalized = value.strip().upper()
            for member in cls:
                if normalized in {member.name, member.value}:
                    return member
        return cls.UNKNOWN


class ReplayPolicy(StrEnum):
    """Permitted recovery operation for a landed transaction.

    ``RECOMPILE_ONLY`` never authorizes replay of the serialized transaction.
    It only permits the runner to discard the old bundle, re-read chain state,
    and compile a new plan in which an already-satisfied setup transaction is
    omitted.
    """

    NEVER = "NEVER"
    RECOMPILE_ONLY = "RECOMPILE_ONLY"

    @classmethod
    def parse(cls, value: Any) -> ReplayPolicy:
        if isinstance(value, cls):
            return value
        if isinstance(value, str):
            normalized = value.strip().upper()
            for member in cls:
                if normalized in {member.name, member.value}:
                    return member
        return cls.NEVER


@dataclass(frozen=True)
class SubmissionTransactionEvidence:
    """Plan-bound role evidence for one submitted transaction identifier."""

    tx_id: str
    role: TransactionRole = TransactionRole.UNKNOWN
    replay_policy: ReplayPolicy = ReplayPolicy.NEVER

    def __post_init__(self) -> None:
        object.__setattr__(self, "role", TransactionRole.parse(self.role))
        object.__setattr__(self, "replay_policy", ReplayPolicy.parse(self.replay_policy))

    def to_dict(self) -> dict[str, str]:
        return {
            "tx_id": self.tx_id,
            "role": self.role.value,
            "replay_policy": self.replay_policy.value,
        }

    @classmethod
    def from_value(cls, value: Any) -> SubmissionTransactionEvidence | None:
        if isinstance(value, cls):
            return value
        if isinstance(value, Mapping):
            tx_id = value.get("tx_id")
            if isinstance(tx_id, str) and tx_id.strip():
                return cls(
                    tx_id=tx_id.strip(),
                    role=TransactionRole.parse(value.get("role")),
                    replay_policy=ReplayPolicy.parse(value.get("replay_policy")),
                )
        return None


def execution_plan_hash(action_bundle: Any) -> str:
    """Return a stable full-width hash of the exact compiled action plan."""
    if hasattr(action_bundle, "to_dict"):
        value = action_bundle.to_dict()
    elif hasattr(action_bundle, "model_dump"):
        value = action_bundle.model_dump()
    else:
        value = action_bundle
    encoded = json.dumps(value, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def certify_submission_transactions(
    action_bundle: Any,
    tx_ids: list[str] | tuple[str, ...],
    *,
    atomic_batch: bool = False,
) -> list[SubmissionTransactionEvidence]:
    """Certify conservative per-transaction roles from the compiled plan.

    Only a transaction explicitly labelled as an approval *and* carrying the
    canonical ERC-20 ``approve(address,uint256)`` selector with zero native
    value is replay-elidable after recompilation. Everything else is an ACTION
    with a NEVER policy. Connector prose or receipt logs alone are not trusted.
    """
    if hasattr(action_bundle, "to_dict"):
        bundle = action_bundle.to_dict()
    elif isinstance(action_bundle, dict):
        bundle = action_bundle
    else:
        bundle = {}
    transactions = bundle.get("transactions", []) if isinstance(bundle, dict) else []
    if not isinstance(transactions, list):
        transactions = []

    def _role_and_policy(transaction: Any) -> tuple[TransactionRole, ReplayPolicy]:
        role = TransactionRole.ACTION
        policy = ReplayPolicy.NEVER
        if isinstance(transaction, dict):
            tx_type = str(transaction.get("tx_type", "")).strip().lower()
            data = str(transaction.get("data", "")).strip().lower()
            raw_value = transaction.get("value", 0)
            try:
                native_value = int(str(raw_value), 0)
            except (TypeError, ValueError):
                native_value = -1
            canonical_approval = (
                len(data) == 138
                and data.startswith("0x095ea7b3")
                and all(character in "0123456789abcdef" for character in data[2:])
            )
            if tx_type in {"approve", "approval", "approve_reset"} and canonical_approval and native_value == 0:
                role = TransactionRole.SETUP_APPROVAL
                policy = ReplayPolicy.RECOMPILE_ONLY
        return role, policy

    # A Safe/Zodiac MultiSend collapses every logical transaction into one
    # physical, atomic transaction. Positional certification would label an
    # approve+swap wrapper from logical transaction zero and could therefore
    # misclassify a landed swap as an idempotent approval. The physical hash is
    # replay-elidable only when *every* member of the wrapper is certified
    # setup. Any action member makes the whole physical transaction an ACTION.
    if atomic_batch:
        all_setup = bool(transactions) and all(
            _role_and_policy(transaction) == (TransactionRole.SETUP_APPROVAL, ReplayPolicy.RECOMPILE_ONLY)
            for transaction in transactions
        )
        role = TransactionRole.SETUP_APPROVAL if all_setup else TransactionRole.ACTION
        policy = ReplayPolicy.RECOMPILE_ONLY if all_setup else ReplayPolicy.NEVER
        return [SubmissionTransactionEvidence(tx_id=tx_id, role=role, replay_policy=policy) for tx_id in tx_ids]

    evidence: list[SubmissionTransactionEvidence] = []
    for index, tx_id in enumerate(tx_ids):
        transaction = transactions[index] if index < len(transactions) else None
        role, policy = _role_and_policy(transaction)
        evidence.append(SubmissionTransactionEvidence(tx_id=tx_id, role=role, replay_policy=policy))
    return evidence


__all__ = [
    "ReplayPolicy",
    "SubmissionProvenance",
    "SubmissionTransactionEvidence",
    "TransactionRole",
    "certify_submission_transactions",
    "execution_plan_hash",
]
