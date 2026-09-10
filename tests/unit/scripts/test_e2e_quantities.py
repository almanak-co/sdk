import json
import sqlite3
from collections import Counter
from pathlib import Path

import pytest

from qa_lab.e2e_quantities import ledger_quantities, validate_quantity_contract

FIXTURE = Path(__file__).resolve().parents[2] / "fixtures/accounting/harness/lp-dual-quantities/captured.json"


@pytest.fixture
def run(tmp_path, request):
    captured = json.loads(FIXTURE.with_name(getattr(request, "param", "captured.json")).read_text())
    database = tmp_path / "almanak_state.db"
    columns = list(captured["rows"][0])
    with sqlite3.connect(database) as db:
        types = {"id": "INTEGER", "success": "INTEGER"}
        db.execute(
            "CREATE TABLE transaction_ledger (" + ",".join(f"{key} {types.get(key, 'TEXT')}" for key in columns) + ")"
        )
        db.executemany(
            "INSERT INTO transaction_ledger VALUES (" + ",".join("?" for _ in columns) + ")",
            [[row[key] for key in columns] for row in captured["rows"]],
        )
    witnesses = tmp_path / "chain"
    witnesses.mkdir()
    for name, value in captured["witnesses"].items():
        (witnesses / name).write_text(json.dumps(value))
    return database, witnesses, captured


def check(run):
    return ledger_quantities(run[0], run[1], wallet=run[2]["wallet"])


def test_captured_ledger_matches_independent_raw_wallet_amounts(run):
    result = check(run)
    assert result["status"] == "PASS"
    assert len(result["rows"]) == 6
    assert len(result["transaction_hashes"]) == 17
    assert Counter(row["intent_type"] for row in result["rows"]) == {"SWAP": 2, "LP_OPEN": 2, "LP_CLOSE": 2}


@pytest.mark.parametrize(
    "mutation",
    [
        "amount_x10",
        "typed_amount",
        "labels",
        "swap_output",
        "fractional_raw",
        "deep_fraction",
        "duplicate_tx",
        "failed_primary",
        "position_id",
        "liquidity",
    ],
)
def test_sdk_quantity_mutations_cannot_match_unchanged_chain_evidence(run, mutation):
    database, _, _ = run
    with sqlite3.connect(database) as db:
        if mutation == "swap_output":
            db.execute("UPDATE transaction_ledger SET amount_out='0.0001' WHERE intent_type='SWAP'")
        elif mutation == "labels":
            db.execute("UPDATE transaction_ledger SET token_in='USDC',token_out='WETH' WHERE intent_type='LP_OPEN'")
        elif mutation == "fractional_raw":
            db.execute("UPDATE transaction_ledger SET amount_out='1.0000001' WHERE intent_type='LP_OPEN'")
        elif mutation == "deep_fraction":
            db.execute(
                "UPDATE transaction_ledger SET amount_out=amount_out||? WHERE intent_type='LP_OPEN'", ("0" * 90 + "1",)
            )
        else:
            key, amount, raw = db.execute(
                "SELECT id,amount_in,extracted_data_json FROM transaction_ledger WHERE intent_type='LP_OPEN' ORDER BY id LIMIT 1"
            ).fetchone()
            extracted = json.loads(raw)
            if mutation == "duplicate_tx":
                extracted["all_tx_results"].append(extracted["all_tx_results"][0])
            elif mutation == "failed_primary":
                primary = db.execute("SELECT tx_hash FROM transaction_ledger WHERE id=?", (key,)).fetchone()[0]
                for item in extracted["all_tx_results"]:
                    if item["tx_hash"] == primary:
                        item["success"] = False
            elif mutation in {"position_id", "liquidity"}:
                changed = int(extracted["lp_open_data"][mutation]) + 1
                extracted["lp_open_data"][mutation] = str(changed)
                extracted[mutation] = changed
            else:
                extracted["lp_open_data"]["amount0"] = str(int(extracted["lp_open_data"]["amount0"]) * 10)
                if mutation == "amount_x10":
                    from decimal import Decimal

                    amount = str(Decimal(amount) * 10)
            db.execute(
                "UPDATE transaction_ledger SET amount_in=?,extracted_data_json=? WHERE id=?",
                (amount, json.dumps(extracted), key),
            )
    result = check(run)
    assert result["status"] == "FAIL"
    assert {
        "amount_x10": "independently observed",
        "typed_amount": "Typed SDK",
        "labels": "token order",
        "swap_output": "independently observed",
        "fractional_raw": "raw-unit",
        "deep_fraction": "raw-unit",
        "duplicate_tx": "reuses a transaction",
        "failed_primary": "successful primary transaction",
        "position_id": "position ID or liquidity",
        "liquidity": "position ID or liquidity",
    }[mutation] in result["reason"]


@pytest.mark.parametrize(
    "mutation",
    ["missing", "missing_balance", "wrong_balance", "decimals", "receipt_amount", "receipt_wallet", "native_value"],
)
def test_chain_witness_mutations_cannot_certify_quantities(run, mutation):
    _, witnesses, captured = run
    tx = next(row["tx_hash"] for row in captured["rows"] if row["intent_type"] == "LP_OPEN")
    if mutation == "missing":
        (witnesses / f"receipt-{tx}.json").unlink()
    elif mutation.startswith("receipt"):
        path = witnesses / f"receipt-{tx}.json"
        receipt = json.loads(path.read_text())
        if mutation == "receipt_wallet":
            receipt["from"] = "0x" + "33" * 20
        else:
            event = next(
                log
                for log in receipt["logs"]
                if log["address"].lower().startswith("0x82af") and len(log["topics"]) == 3
            )
            event["data"] = "0x" + f"{1:064x}"
        path.write_text(json.dumps(receipt))
    elif mutation == "native_value":
        path = witnesses / f"transaction-{tx}.json"
        transaction = json.loads(path.read_text())
        transaction["value"] = "0x1"
        path.write_text(json.dumps(transaction))
    else:
        path = witnesses / f"balances-{tx}.json"
        balances = json.loads(path.read_text())
        if mutation == "missing_balance":
            balances.pop()
        elif mutation == "wrong_balance":
            balances[0]["actual_delta_raw"] = "0"
        else:
            balances[0]["token_decimals"] = 6
        path.write_text(json.dumps(balances))
    result = check(run)
    assert result["status"] == ("UNMEASURED" if mutation in {"missing", "missing_balance", "native_value"} else "FAIL")


def test_absent_database_is_unmeasured_and_never_created(tmp_path):
    database = tmp_path / "missing.sqlite"
    result = ledger_quantities(database, tmp_path, wallet="0x" + "33" * 20)
    assert result["status"] == "UNMEASURED"
    assert not database.exists()


def quantity_contract(run):
    database, witnesses, captured = run
    bundle = database.parent
    source = FIXTURE.parent.parent / "lp-dual-generations/positions-open.json"
    (bundle / "positions-open.json").write_bytes(source.read_bytes())
    (bundle / "positions-terminal.json").write_bytes((source.parent / "positions-terminal.json").read_bytes())
    contract = {"wallet_quantities": {"schema_version": 1, "model": "uniswap-v3-weth-usdc-v1"}}
    state = {"path": database.name}
    lifecycle = {"lifecycle_transaction_ids": [row["tx_hash"] for row in captured["rows"]]}
    cell = {
        "strategy_path": "strategies/accounting/lp_dual",
        "primitive": "lp",
        "chain": "arbitrum",
        "protocol": "uniswap_v3",
        "network": "anvil",
        "exec_path": "eoa",
    }
    return bundle, contract, state, lifecycle, cell


def test_frozen_quantity_contract_reads_raw_authorities_and_rejects_unproved_primary_rows(run):
    bundle, contract, state, lifecycle, cell = quantity_contract(run)
    result = validate_quantity_contract(bundle, contract, state, lifecycle, catalog_cell=cell)
    assert result["status"] == "PASS"
    assert len(result["source_artifacts"]) == 53
    for source in result["source_artifacts"]:
        assert (bundle / source).is_file()
    lifecycle["lifecycle_transaction_ids"].pop()
    with pytest.raises(ValueError, match="outside the proved lifecycle"):
        validate_quantity_contract(bundle, contract, state, lifecycle, catalog_cell=cell)
    with pytest.raises(ValueError, match="exact supported frozen model"):
        validate_quantity_contract(
            bundle,
            {"wallet_quantities": {"schema_version": True, "model": "uniswap-v3-weth-usdc-v1"}},
            state,
            lifecycle,
            catalog_cell=cell,
        )


def test_sdk_and_receipt_identity_forgery_cannot_override_independent_census(run):
    bundle, contract, state, lifecycle, cell = quantity_contract(run)
    with sqlite3.connect(run[0]) as db:
        key, tx, raw = db.execute(
            "SELECT id,tx_hash,extracted_data_json FROM transaction_ledger WHERE intent_type='LP_OPEN' LIMIT 1"
        ).fetchone()
        data = json.loads(raw)
        forged_id = int(data["position_id"]) + 100
        data["position_id"] = forged_id
        data["lp_open_data"]["position_id"] = forged_id
        db.execute("UPDATE transaction_ledger SET extracted_data_json=? WHERE id=?", (json.dumps(data), key))
    path = run[1] / f"receipt-{tx}.json"
    receipt = json.loads(path.read_text())
    manager = json.loads((bundle / "positions-open.json").read_text())["position_manager"].lower()
    for log in receipt["logs"]:
        if log["address"].lower() == manager:
            log["topics"][-1] = "0x" + f"{forged_id:064x}"
    path.write_text(json.dumps(receipt))
    assert check(run)["status"] == "PASS"
    with pytest.raises(ValueError, match="independent position census"):
        validate_quantity_contract(bundle, contract, state, lifecycle, catalog_cell=cell)


@pytest.mark.parametrize("run", ["rebalance-captured.json"], indirect=True)
@pytest.mark.parametrize("mutation", [None, "nonzero", "wrong_currency", "empty_currency", "amount"])
def test_real_rebalance_zero_leg_has_no_parser_currency_but_exact_wallet_proof(run, mutation):
    database, _, captured = run
    row = next(
        row
        for row in captured["rows"]
        if row["intent_type"] == "LP_CLOSE"
        and json.loads(row["extracted_data_json"])["lp_close_data"]["currency0"] is None
    )
    extracted = json.loads(row["extracted_data_json"])
    assert extracted["lp_close_data"]["amount0_collected"] == "0"
    assert row["amount_in"] == "0"
    if mutation:
        typed = extracted["lp_close_data"]
        if mutation == "nonzero":
            typed["amount0_collected"] = "1"
            row["amount_in"] = "0.000000000000000001"
        elif mutation == "amount":
            typed["amount0_collected"] = "1"
        else:
            typed["currency0"] = "" if mutation == "empty_currency" else typed["currency1"]
        with sqlite3.connect(database) as db:
            db.execute(
                "UPDATE transaction_ledger SET amount_in=?,extracted_data_json=? WHERE id=?",
                (row["amount_in"], json.dumps(extracted), row["id"]),
            )
    result = check(run)
    assert result["status"] == ("PASS" if mutation is None else "UNMEASURED" if mutation == "nonzero" else "FAIL")
    if mutation is None:
        assert len(result["rows"]) == 9
        assert len(result["transaction_hashes"]) == 27
