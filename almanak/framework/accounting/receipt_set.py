"""Pure validation of receipt evidence persisted on landed ledger rows.

This module answers one deliberately bounded question: does every ledger row
that the framework says landed carry a complete, internally consistent set of
typed sub-transaction receipts?  Both the Accountant Test and the operator-run
accounting regression gate delegate here so their verdicts cannot drift.

The evaluator cannot prove that the ledger contains every transaction sent by
the wallet.  A whole failed attempt (including successful approvals followed by
a reverted action) can be absent from every accounting table, as documented by
VIB-6303.  Proving that stronger property requires independent nonce-window and
chain-receipt evidence (VIB-6368); a PASS here must never be described as such a
proof.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

from almanak.framework.accounting.ledger_guard import landed

_VALID_ROLES = frozenset({"APPROVAL", "ACTION", "INCIDENTAL"})


@dataclass(frozen=True)
class ReceiptSetFinding:
    """One stable, machine-readable receipt-set violation."""

    code: str
    row_id: str
    detail: str


@dataclass(frozen=True)
class ReceiptSetEvaluation:
    """Result of evaluating a sequence of transaction-ledger rows."""

    landed_rows: int
    sub_transactions: int
    findings: tuple[ReceiptSetFinding, ...]

    @property
    def passed(self) -> bool:
        """Whether every inspected landed row satisfied the invariant."""
        return self.landed_rows > 0 and not self.findings


def _optional_field(row: Any, key: str) -> Any:
    """Read an optional field from dicts and ``sqlite3.Row`` values."""
    if isinstance(row, dict):
        return row.get(key)
    return row[key] if key in row.keys() else None


def _row_id(row: Any, index: int) -> str:
    value = _optional_field(row, "id")
    text = str(value).strip() if value is not None else ""
    return text or f"row[{index}]"


def _canonical_hash(value: str) -> str:
    """Canonicalize EVM hashes while preserving case-sensitive non-EVM IDs."""
    return value.lower() if value.startswith(("0x", "0X")) else value


def _decode_extracted_data(raw: Any) -> dict[str, Any] | None:
    if isinstance(raw, dict):
        return raw
    if not isinstance(raw, str) or not raw.strip():
        return None
    try:
        decoded = json.loads(raw)
    except (TypeError, ValueError):
        return None
    return decoded if isinstance(decoded, dict) else None


def _measured_gas(value: Any) -> int | None:
    """Return a typed gas measurement; ``None`` is unmeasured or malformed.

    ``0`` is a valid measured value.  Strings and booleans are rejected rather
    than coerced so the persisted evidence remains typed and Empty != Zero.
    """
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        return None
    return value


def _evaluate_sub_transaction(
    sub_transaction: Any,
    *,
    location: str,
    row_id: str,
    parent_hash: str,
    seen_hashes: dict[str, str],
) -> tuple[int | None, bool, list[ReceiptSetFinding]]:
    findings: list[ReceiptSetFinding] = []
    if not isinstance(sub_transaction, dict):
        return (
            None,
            False,
            [ReceiptSetFinding("sub_transaction_not_object", row_id, f"{location} is not an object")],
        )

    raw_hash = sub_transaction.get("tx_hash")
    tx_hash = raw_hash.strip() if isinstance(raw_hash, str) else ""
    if not tx_hash:
        findings.append(
            ReceiptSetFinding(
                "sub_transaction_hash_blank",
                row_id,
                f"{location}.tx_hash is not a non-blank string",
            )
        )
    else:
        canonical_hash = _canonical_hash(tx_hash)
        prior_row = seen_hashes.get(canonical_hash)
        if prior_row is not None:
            findings.append(
                ReceiptSetFinding(
                    "sub_transaction_hash_duplicate",
                    row_id,
                    f"{location}.tx_hash duplicates receipt evidence already used by {prior_row}",
                )
            )
        else:
            seen_hashes[canonical_hash] = row_id

    gas_used = _measured_gas(sub_transaction.get("gas_used"))
    if gas_used is None:
        findings.append(
            ReceiptSetFinding(
                "sub_transaction_gas_unmeasured",
                row_id,
                f"{location}.gas_used is not a non-negative integer",
            )
        )

    status = sub_transaction.get("status")
    if status != "success":
        findings.append(
            ReceiptSetFinding(
                "sub_transaction_not_successful",
                row_id,
                f"{location}.status is {status!r}, expected 'success' on a landed row",
            )
        )

    role = sub_transaction.get("role")
    if role not in _VALID_ROLES:
        findings.append(
            ReceiptSetFinding(
                "sub_transaction_role_invalid",
                row_id,
                f"{location}.role is {role!r}, expected one of {sorted(_VALID_ROLES)}",
            )
        )
    for text_field in ("target_contract", "function_selector"):
        if not isinstance(sub_transaction.get(text_field), str):
            findings.append(
                ReceiptSetFinding(
                    "sub_transaction_field_untyped",
                    row_id,
                    f"{location}.{text_field} is not a string",
                )
            )
    parent_action = bool(
        role == "ACTION" and tx_hash and parent_hash and _canonical_hash(tx_hash) == _canonical_hash(parent_hash)
    )
    return gas_used, parent_action, findings


def _evaluate_landed_row(
    row: Any,
    *,
    index: int,
    seen_hashes: dict[str, str],
) -> tuple[int, list[ReceiptSetFinding]]:
    row_id = _row_id(row, index)
    extracted = _decode_extracted_data(_optional_field(row, "extracted_data_json"))
    sub_transactions = extracted.get("sub_transactions") if extracted is not None else None
    if not isinstance(sub_transactions, list) or not sub_transactions:
        return 0, [
            ReceiptSetFinding(
                "sub_transactions_missing",
                row_id,
                "landed row has no non-empty sub_transactions array",
            )
        ]

    parent_raw = _optional_field(row, "tx_hash")
    parent_hash = parent_raw.strip() if isinstance(parent_raw, str) else ""
    findings: list[ReceiptSetFinding] = []
    sub_gas: list[int] = []
    parent_action_found = False
    for sub_index, sub_transaction in enumerate(sub_transactions):
        gas_used, parent_action, sub_findings = _evaluate_sub_transaction(
            sub_transaction,
            location=f"sub_transactions[{sub_index}]",
            row_id=row_id,
            parent_hash=parent_hash,
            seen_hashes=seen_hashes,
        )
        findings.extend(sub_findings)
        parent_action_found = parent_action_found or parent_action
        if gas_used is not None:
            sub_gas.append(gas_used)

    if not parent_action_found:
        findings.append(
            ReceiptSetFinding(
                "parent_action_mismatch",
                row_id,
                "parent tx_hash does not match any ACTION sub-transaction",
            )
        )

    ledger_gas = _measured_gas(_optional_field(row, "gas_used"))
    if ledger_gas is None:
        findings.append(
            ReceiptSetFinding(
                "ledger_gas_unmeasured",
                row_id,
                "transaction_ledger.gas_used is not a non-negative integer",
            )
        )
    elif len(sub_gas) == len(sub_transactions):
        receipt_gas = sum(sub_gas)
        if ledger_gas != receipt_gas:
            findings.append(
                ReceiptSetFinding(
                    "gas_sum_mismatch",
                    row_id,
                    f"ledger gas {ledger_gas} != sub-transaction gas sum {receipt_gas}",
                )
            )
    return len(sub_transactions), findings


def evaluate_landed_receipt_sets(rows: list[Any]) -> ReceiptSetEvaluation:
    """Evaluate persisted receipt-set invariants for every landed ledger row.

    A valid landed row has a non-empty ``sub_transactions`` array whose entries
    are objects with a unique, non-blank transaction hash, measured integer gas,
    a successful receipt status, and a known role.  The parent ledger hash must
    identify at least one ACTION entry, and the parent aggregate gas must equal
    the exact sum of every entry's gas.

    Hash uniqueness is checked across the whole supplied population, not merely
    within one row: one chain transaction cannot substantiate two ledger rows.
    """
    findings: list[ReceiptSetFinding] = []
    seen_hashes: dict[str, str] = {}
    landed_rows = 0
    sub_transaction_count = 0

    for index, row in enumerate(rows):
        if not landed(
            _optional_field(row, "success"),
            _optional_field(row, "error"),
            _optional_field(row, "tx_hash"),
        ):
            continue
        landed_rows += 1
        row_sub_transactions, row_findings = _evaluate_landed_row(
            row,
            index=index,
            seen_hashes=seen_hashes,
        )
        sub_transaction_count += row_sub_transactions
        findings.extend(row_findings)

    return ReceiptSetEvaluation(
        landed_rows=landed_rows,
        sub_transactions=sub_transaction_count,
        findings=tuple(findings),
    )


__all__ = [
    "ReceiptSetEvaluation",
    "ReceiptSetFinding",
    "evaluate_landed_receipt_sets",
]
