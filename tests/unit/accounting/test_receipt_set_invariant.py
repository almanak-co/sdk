"""Receipt-set integrity negative controls for Accountant G17 and ship-gate G8."""

from __future__ import annotations

import copy
import json
import sqlite3
from typing import Any

import pytest

import scripts.ci.accounting_regression_assert as regression_gate
from almanak.framework.accounting.accountant_test import _cell_g17_receipt_set
from almanak.framework.accounting.ledger_guard import DEGRADED_PREFIX
from almanak.framework.accounting.receipt_set import evaluate_landed_receipt_sets
from almanak.framework.execution.gateway_orchestrator import GatewayExecutionResult
from almanak.framework.observability.ledger import _build_extracted_data_json

_APPROVAL_HASH = "0x" + "a" * 64
_ACTION_HASH = "0x" + "b" * 64
_OTHER_HASH = "0x" + "c" * 64


def _valid_row() -> dict[str, Any]:
    return {
        "id": "ledger-1",
        "success": 1,
        "error": "",
        "tx_hash": _ACTION_HASH,
        "gas_used": 30,
        "extracted_data_json": json.dumps(
            {
                "sub_transactions": [
                    {
                        "tx_hash": _APPROVAL_HASH,
                        "target_contract": "0x" + "d" * 40,
                        "function_selector": "0x095ea7b3",
                        "gas_used": 10,
                        "status": "success",
                        "role": "APPROVAL",
                    },
                    {
                        "tx_hash": _ACTION_HASH,
                        "target_contract": "0x" + "e" * 40,
                        "function_selector": "0x12345678",
                        "gas_used": 20,
                        "status": "success",
                        "role": "ACTION",
                    },
                ]
            }
        ),
    }


def _mutate_sub_transactions(row: dict[str, Any], mutation) -> None:
    extracted = json.loads(row["extracted_data_json"])
    mutation(extracted["sub_transactions"])
    row["extracted_data_json"] = json.dumps(extracted)


def _delete_leg(row: dict[str, Any]) -> None:
    _mutate_sub_transactions(row, lambda legs: legs.pop(0))


def _duplicate_hash(row: dict[str, Any]) -> None:
    _mutate_sub_transactions(row, lambda legs: legs[0].__setitem__("tx_hash", _ACTION_HASH))


def _failed_status(row: dict[str, Any]) -> None:
    _mutate_sub_transactions(row, lambda legs: legs[1].__setitem__("status", "failure"))


def _blank_hash(row: dict[str, Any]) -> None:
    _mutate_sub_transactions(row, lambda legs: legs[0].__setitem__("tx_hash", " \t"))


def _parent_mismatch(row: dict[str, Any]) -> None:
    row["tx_hash"] = _OTHER_HASH


def _gas_plus_one(row: dict[str, Any]) -> None:
    row["gas_used"] += 1


def _gas_minus_one(row: dict[str, Any]) -> None:
    row["gas_used"] -= 1


@pytest.mark.parametrize(
    ("mutation", "expected_code"),
    [
        (_delete_leg, "gas_sum_mismatch"),
        (_duplicate_hash, "sub_transaction_hash_duplicate"),
        (_failed_status, "sub_transaction_not_successful"),
        (_blank_hash, "sub_transaction_hash_blank"),
        (_parent_mismatch, "parent_action_mismatch"),
        (_gas_plus_one, "gas_sum_mismatch"),
        (_gas_minus_one, "gas_sum_mismatch"),
    ],
    ids=[
        "delete-leg",
        "duplicate-hash",
        "failed-status",
        "blank-hash",
        "parent-mismatch",
        "gas-plus-one",
        "gas-minus-one",
    ],
)
def test_required_negative_controls_fail(mutation, expected_code: str) -> None:
    row = copy.deepcopy(_valid_row())
    mutation(row)

    evaluation = evaluate_landed_receipt_sets([row])

    assert not evaluation.passed
    assert expected_code in {finding.code for finding in evaluation.findings}


def test_valid_multi_leg_row_passes_with_exact_gas() -> None:
    evaluation = evaluate_landed_receipt_sets([_valid_row()])

    assert evaluation.passed
    assert evaluation.landed_rows == 1
    assert evaluation.sub_transactions == 2
    assert evaluation.findings == ()


def test_zero_gas_is_measured_not_empty() -> None:
    row = _valid_row()
    row["gas_used"] = 0
    _mutate_sub_transactions(
        row,
        lambda legs: (
            legs.clear(),
            legs.append(
                {
                    "tx_hash": _ACTION_HASH,
                    "target_contract": "",
                    "function_selector": "",
                    "gas_used": 0,
                    "status": "success",
                    "role": "ACTION",
                }
            ),
        ),
    )

    assert evaluate_landed_receipt_sets([row]).passed


def test_gateway_solana_fee_receipt_passes_shared_g17_g8_evidence() -> None:
    signature = "5VERv8NMHKRYsGeYfVb9oKzvoHvU9vE3yo9Xq2Gj8j3B8VeqiZLzQQDCbPVmXNgTjEFGdYkhNmj1PYqC7GsQzXvA"
    result = GatewayExecutionResult(
        success=True,
        tx_hashes=[signature],
        total_gas_used=5000,
        receipts=[
            {
                "signature": signature,
                "slot": 12345,
                "block_time": 1_700_000_000,
                "fee_lamports": 5000,
                "success": True,
                "err": None,
                "logs": [],
                "pre_token_balances": [],
                "post_token_balances": [],
            }
        ],
        execution_id="solana-g17",
        chain_family="SOLANA",
    )
    row = {
        "id": "solana-ledger",
        "success": 1,
        "error": "",
        "tx_hash": signature,
        "gas_used": result.total_gas_used,
        "extracted_data_json": _build_extracted_data_json(result),
    }

    evaluation = evaluate_landed_receipt_sets([row])

    assert evaluation.passed
    assert evaluation.sub_transactions == 1


def test_hash_uniqueness_spans_ledger_rows() -> None:
    first = _valid_row()
    second = _valid_row()
    second["id"] = "ledger-2"

    evaluation = evaluate_landed_receipt_sets([first, second])

    assert "sub_transaction_hash_duplicate" in {finding.code for finding in evaluation.findings}


def test_degraded_but_landed_row_is_evaluated() -> None:
    row = _valid_row()
    row["success"] = 0
    row["error"] = f"{DEGRADED_PREFIX}amounts_unmeasured: fixture"

    evaluation = evaluate_landed_receipt_sets([row])

    assert evaluation.passed
    assert evaluation.landed_rows == 1


def test_wholly_failed_attempt_is_out_of_scope_and_never_passes() -> None:
    row = _valid_row()
    row["success"] = 0
    row["error"] = "execution reverted"

    evaluation = evaluate_landed_receipt_sets([row])

    assert not evaluation.passed
    assert evaluation.landed_rows == 0
    assert evaluation.findings == ()


def test_accountant_g17_and_operator_g8_share_the_same_failure(tmp_path) -> None:
    row = _valid_row()
    _gas_plus_one(row)
    cell = _cell_g17_receipt_set([row])

    db = tmp_path / "state.db"
    conn = sqlite3.connect(db)
    conn.row_factory = sqlite3.Row
    conn.execute(
        "CREATE TABLE transaction_ledger ("
        "id TEXT, success INTEGER, error TEXT, tx_hash TEXT, gas_used INTEGER, "
        "extracted_data_json TEXT)"
    )
    conn.execute(
        "INSERT INTO transaction_ledger VALUES (:id, :success, :error, :tx_hash, :gas_used, :extracted_data_json)",
        row,
    )
    gate = regression_gate.gate_sub_transactions(conn)

    assert cell.status == "FAIL"
    assert gate.status == "FAIL"
    assert "gas_sum_mismatch" in cell.diagnostic
    assert "gas_sum_mismatch" in gate.detail
