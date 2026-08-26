from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from scripts.qa.quant_books import derive_quant_books_claim

TX_HASH = "0x" + "12" * 32


def _write_bundle(bundle: Path) -> None:
    (bundle / "receipt-reconciliation.json").write_text(
        json.dumps(
            {
                "canonical_hashes": [TX_HASH],
                "intents": [
                    {
                        "intent_id": "intent-1",
                        "transactions": [
                            {
                                "tx_hash": TX_HASH,
                                "receipt_status": 1,
                                "raw_receipt": {"transactionHash": TX_HASH, "status": "0x1"},
                            }
                        ],
                    }
                ],
                "submission_receipt_integrity": {
                    "status": "PASS",
                    "submitted_transaction_count": 1,
                    "terminal_receipt_count": 1,
                },
            }
        )
    )
    (bundle / "accountant-test.json").write_text(
        json.dumps(
            {
                "scores": {"passed": 1, "failed": 0, "xfailed": 0, "total": 2},
                "cell_details": [
                    {"id": "G6", "status": "PASS"},
                    {"id": "G11", "status": "SKIP"},
                ],
                "g6_decomposition": {
                    "wallet_pnl_usd": "1.00",
                    "component_pnl_usd": "1.01",
                    "gap_usd": "0.01",
                    "epsilon_threshold_usd": "0.10",
                    "epsilon_vacuous": "False",
                },
            }
        )
    )
    connection = sqlite3.connect(bundle / "db.sqlite")
    try:
        connection.execute(
            "CREATE TABLE transaction_ledger "
            "(tx_hash TEXT, success INTEGER, extracted_data_json TEXT, deployment_id TEXT)"
        )
        for table in ("accounting_events", "portfolio_snapshots", "portfolio_metrics"):
            connection.execute(f"CREATE TABLE {table} (deployment_id TEXT)")  # noqa: S608
            connection.execute(f"INSERT INTO {table} VALUES ('deployment:test')")  # noqa: S608
        extracted = json.dumps({"sub_transactions": [{"tx_hash": TX_HASH}]})
        connection.execute(
            "INSERT INTO transaction_ledger VALUES (?, 1, ?, 'deployment:test')",
            (TX_HASH, extracted),
        )
        connection.commit()
    finally:
        connection.close()


def test_books_pass_is_rederived_from_receipts_sqlite_and_accountant(tmp_path: Path) -> None:
    _write_bundle(tmp_path)

    claim = derive_quant_books_claim(tmp_path)

    assert claim["status"] == "PASS"
    assert [check["id"] for check in claim["checks"]] == [
        "receipt_balance_reconciliation",
        "sqlite_accounting_consistency",
        "accountant_applicable_coverage",
    ]
    assert {check["status"] for check in claim["checks"]} == {"PASS"}


def test_books_cannot_pass_when_accountant_gap_is_merely_declared_pass(tmp_path: Path) -> None:
    _write_bundle(tmp_path)
    path = tmp_path / "accountant-test.json"
    accountant = json.loads(path.read_text())
    accountant["g6_decomposition"]["gap_usd"] = "0.00"
    path.write_text(json.dumps(accountant))

    claim = derive_quant_books_claim(tmp_path)

    assert claim["status"] == "FAIL"
    check = next(check for check in claim["checks"] if check["id"] == "receipt_balance_reconciliation")
    assert check["status"] == "FAIL"
    assert "does not equal" in check["measurement"]["error"]


def test_books_cannot_pass_when_sqlite_and_receipt_identities_diverge(tmp_path: Path) -> None:
    _write_bundle(tmp_path)
    connection = sqlite3.connect(tmp_path / "db.sqlite")
    try:
        connection.execute("UPDATE transaction_ledger SET tx_hash = ?", ("0x" + "34" * 32,))
        connection.execute("UPDATE transaction_ledger SET extracted_data_json = '{}'")
        connection.commit()
    finally:
        connection.close()

    claim = derive_quant_books_claim(tmp_path)

    assert claim["status"] == "FAIL"
    check = next(check for check in claim["checks"] if check["id"] == "sqlite_accounting_consistency")
    assert check["status"] == "FAIL"
    assert "bijectively match" in check["measurement"]["error"]


def test_books_cannot_pass_an_all_skipped_accountant_run(tmp_path: Path) -> None:
    _write_bundle(tmp_path)
    path = tmp_path / "accountant-test.json"
    accountant = json.loads(path.read_text())
    accountant["scores"] = {"passed": 0, "failed": 0, "xfailed": 0, "total": 1}
    accountant["cell_details"] = [{"id": "G11", "status": "SKIP"}]
    path.write_text(json.dumps(accountant))

    claim = derive_quant_books_claim(tmp_path)

    assert claim["status"] == "FAIL"
    check = next(check for check in claim["checks"] if check["id"] == "accountant_applicable_coverage")
    assert check["status"] == "FAIL"
    assert "no applicable passing cell" in check["measurement"]["error"]


def test_missing_mechanical_books_evidence_is_unmeasured(tmp_path: Path) -> None:
    (tmp_path / "books-observation.json").write_text('{"status":"PASS"}')

    claim = derive_quant_books_claim(tmp_path)

    assert claim["status"] == "UNMEASURED"
    assert claim["reason_codes"] == ["books_mechanical_evidence_absent"]
