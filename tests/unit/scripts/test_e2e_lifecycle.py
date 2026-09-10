import json
import sqlite3

import pytest

from qa_lab.e2e_card import REPO, TEMPLATE, Scenario, canonical, lifecycle_contract, load_json
from qa_lab.e2e_lifecycle import derive_lifecycle, validate_sdk_lifecycle
from tests.unit.scripts import test_e2e_quantities as quantity_tests

FIXTURE = quantity_tests.FIXTURE
run = quantity_tests.run


@pytest.fixture
def captured(run):
    database, chain, _ = run
    details = json.loads((FIXTURE.parent / "lifecycle.json").read_text())
    with sqlite3.connect(database) as db:
        db.execute("ALTER TABLE transaction_ledger ADD COLUMN cycle_id TEXT")
        db.execute("ALTER TABLE transaction_ledger ADD COLUMN timestamp TEXT")
        for row in details["rows"]:
            db.execute(
                "UPDATE transaction_ledger SET cycle_id=?,timestamp=? WHERE id=?",
                (row["cycle_id"], row["timestamp"], row["id"]),
            )
        columns = list(details["request"])
        db.execute("CREATE TABLE teardown_requests (" + ",".join(columns) + ")")
        db.execute(
            "INSERT INTO teardown_requests VALUES (" + ",".join("?" for _ in columns) + ")",
            list(details["request"].values()),
        )
    contract = database.parent / "lifecycle-contract.json"
    contract.write_bytes(canonical(lifecycle_contract(Scenario.model_validate(load_json(REPO / TEMPLATE)))))
    return database, chain, contract


def test_real_open_close_run_cannot_satisfy_rebalance_obligations(captured):
    result = derive_lifecycle(*captured)
    assert result["status"] == "FAIL"
    assert result["unmet_requirements"] == ["open-and-replace", "rebalance-close"]
    observations = {row["requirement_id"]: row for row in result["coverage"]["observations"]}
    assert observations["open-and-replace"]["executed"] == 2
    assert observations["rebalance-close"]["executed"] == 0
    assert observations["teardown-close"]["executed"] == 2
    assert observations["teardown-swap"]["executed"] == 1
    assert len(result["receipt_reconciliation"]["canonical_hashes"]) == 17
    assert result["e2e_admission"] == "UNMEASURED"


@pytest.mark.parametrize("mutation", ["runtime_relabel", "naive", "duplicate", "failed"])
def test_phase_and_receipt_contradictions_cannot_supply_lifecycle_evidence(captured, mutation):
    database, _, _ = captured
    with sqlite3.connect(database) as db:
        if mutation == "runtime_relabel":
            db.execute(
                "UPDATE transaction_ledger SET cycle_id='008a491b-7fe5-4b74-9967-a0ca8db29914' WHERE intent_type='LP_CLOSE'"
            )
        elif mutation == "naive":
            db.execute("UPDATE transaction_ledger SET timestamp=replace(timestamp,'+00:00','')")
        elif mutation == "failed":
            db.execute("UPDATE transaction_ledger SET success=0 WHERE intent_type='LP_CLOSE'")
        else:
            row = db.execute("SELECT id,extracted_data_json FROM transaction_ledger LIMIT 1").fetchone()
            extracted = json.loads(row[1])
            extracted["all_tx_results"].append(extracted["all_tx_results"][0])
            db.execute(
                "UPDATE transaction_ledger SET extracted_data_json=? WHERE id=?", (json.dumps(extracted), row[0])
            )
    with pytest.raises(
        ValueError,
        match={
            "runtime_relabel": "overlaps",
            "naive": "timezone-aware",
            "duplicate": "reuses",
            "failed": "unsuccessful",
        }[mutation],
    ):
        derive_lifecycle(*captured)


def test_sdk_phase_obligation_recomputes_coverage_and_raw_receipts(captured):
    database, chain, contract_path = captured
    contract = load_json(contract_path)
    contract["requirements"] = [r for r in contract["requirements"] if r["id"] != "rebalance-close"]
    next(r for r in contract["requirements"] if r["id"] == "open-and-replace")["min_executed"] = 2
    contract_path.write_bytes(canonical(contract))
    result = derive_lifecycle(*captured)
    bundle = database.parent
    for name, key in (
        ("lifecycle-coverage.json", "coverage"),
        ("receipt-reconciliation.json", "receipt_reconciliation"),
    ):
        (bundle / name).write_bytes(canonical(result[key]))
    cell = {
        "strategy_path": "strategies/accounting/lp_dual",
        "chain": "arbitrum",
        "protocol": "uniswap_v3",
        "network": "anvil",
        "exec_path": "eoa",
        "primitive": "lp",
    }
    state = {"path": database.name}
    admitted = validate_sdk_lifecycle(bundle, contract, state, catalog_cell=cell)
    assert admitted["status"] == "PASS"
    assert len(admitted["source_artifacts"]) == 17
    coverage = result["coverage"]
    coverage["observations"][1]["executed"] = 3
    (bundle / "lifecycle-coverage.json").write_bytes(canonical(coverage))
    with pytest.raises(ValueError, match="disagrees with lifecycle-coverage"):
        validate_sdk_lifecycle(bundle, contract, state, catalog_cell=cell)


def test_cleanup_collection_retains_failed_lifecycle_without_inventing_rebalance(captured):
    import shutil
    from types import SimpleNamespace

    from qa_lab.e2e_card import digest
    from qa_lab.e2e_evidence import _capture_lifecycle

    database, chain, contract_path = captured
    preparation = database.parent / "preparation"
    preparation.mkdir()
    raw = contract_path.read_bytes()
    (preparation / contract_path.name).write_bytes(raw)
    (preparation / "card.json").write_bytes(canonical({"artifacts": {contract_path.name: digest(raw)}}))
    output = database.parent / "capture"
    output.mkdir()
    shutil.copytree(chain, output / "chain")
    context = SimpleNamespace(root=database.parent, require_owned=lambda path: path)
    result = _capture_lifecycle(context, database, output)
    assert result["status"] == "FAIL"
    assert result["unmet_requirements"] == ["open-and-replace", "rebalance-close"]
    assert (output / "lifecycle-contract.json").read_bytes() == raw
    assert len(load_json(output / "receipt-reconciliation.json")["canonical_hashes"]) == 17
    assert load_json(output / "lifecycle-coverage.json")["observations"][2]["executed"] == 0
