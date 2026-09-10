"""Synthetic actor admission controls derived from captured subject swap bytes."""

import json
import re
import sqlite3
from pathlib import Path
from uuid import uuid4

import pytest

from almanak.framework.anvil.accounts import anvil_default_address
from almanak.framework.runner.identity import resolve_deployment_id
from qa_lab.e2e_actor_quantities import validate_actor_quantity_contract
from qa_lab.e2e_card import canonical, digest, load_json

POLICY = {"actor_quantities": {"schema_version": 1, "model": "stimulus-two-swaps-v1"}}


@pytest.fixture
def actor_bundle(tmp_path):
    fixture = Path(__file__).resolve().parents[2] / "fixtures/accounting/harness/lp-dual-quantities/captured.json"
    captured = load_json(fixture)
    wallet = anvil_default_address(1).lower()
    text = re.sub(captured["wallet"][2:], wallet[2:], json.dumps(captured), flags=re.IGNORECASE)
    captured = json.loads(text)
    rows = sorted((row for row in captured["rows"] if row["intent_type"] == "SWAP"), key=lambda row: row["token_in"])
    identity = resolve_deployment_id(wallet_address=wallet, chain="arbitrum")
    hashes = {item["tx_hash"] for row in rows for item in json.loads(row["extracted_data_json"])["all_tx_results"]}
    witnesses = {name: value for name, value in captured["witnesses"].items() if any(tx in name for tx in hashes)}
    transactions = [witnesses[f"transaction-{tx}.json"] for tx in hashes]
    start = min(int(tx["blockNumber"], 16) for tx in transactions) - 1
    end = max(int(tx["blockNumber"], 16) for tx in transactions)

    def block_hash(number):
        return "0x" + f"{number:064x}"

    for tx in hashes:
        receipt = witnesses[f"receipt-{tx}.json"]
        number = int(receipt["blockNumber"], 16)
        receipt["blockHash"] = block_hash(number)
        for log in receipt["logs"]:
            log["blockHash"] = block_hash(number)
        witnesses[f"transaction-{tx}.json"]["blockHash"] = block_hash(number)
    blocks = [
        {
            "number": hex(number),
            "hash": block_hash(number),
            "parentHash": block_hash(number - 1),
            "transactions": [tx for tx in transactions if int(tx["blockNumber"], 16) == number],
        }
        for number in range(start + 1, end + 1)
    ]
    census = {
        "status": "PASS",
        "fork_block": start,
        "last_block": end,
        "chain_id": 42161,
        "wallets": [wallet],
        "blocks": blocks,
    }
    root = tmp_path / "stimulus-quantities"
    (root / "chain").mkdir(parents=True)
    for name, value in {**witnesses, "submission-census.json": census}.items():
        (root / "chain" / name).write_bytes(canonical(value))
    for index, row in enumerate(rows):
        row.update(
            deployment_id=identity,
            cycle_id=str(uuid4()) if index == 0 else "teardown-td_123456789abc",
            timestamp=f"2026-09-07T00:0{index}:00+00:00",
        )
    columns = list(rows[0])
    with sqlite3.connect(root / "almanak_state.db") as db:
        db.execute("CREATE TABLE transaction_ledger (" + ",".join(columns) + ")")
        db.executemany(
            "INSERT INTO transaction_ledger VALUES (" + ",".join("?" for _ in columns) + ")",
            [[row[column] for column in columns] for row in rows],
        )
        db.execute("CREATE TABLE teardown_requests (deployment_id,status,positions_failed,started_at,completed_at)")
        db.execute(
            "INSERT INTO teardown_requests VALUES (?,?,?,?,?)",
            (identity, "completed", 0, "2026-09-07T00:01:00+00:00", "2026-09-07T00:02:00+00:00"),
        )
    fork = {"instance_id": "synthetic-actor", "fork_block": start, "fork_hash": block_hash(start)}
    (tmp_path / "stimulus-terminal.json").write_bytes(
        canonical({"fork_identity": fork, "block_number": end, "block_hash": block_hash(end)})
    )
    preparation = tmp_path / "preparation"
    preparation.mkdir()
    (preparation / "scenario.json").write_bytes(
        canonical({"stimulus": {"max_total_usdc_raw": "10000000000000", "max_slippage": "0.005", "max_swaps": 3}})
    )
    (preparation / "card.json").write_bytes(
        canonical({"artifacts": {"scenario.json": digest((preparation / "scenario.json").read_bytes())}})
    )
    actor = tmp_path / "stimulus"
    actor.mkdir()
    config = {
        "chain": "arbitrum",
        "fork_instance": fork["instance_id"],
        "fork_block": start,
        "fork_hash": fork["fork_hash"],
        "amount_usdc_raw": 4000000,
        "max_usdc_raw": 10000000000000,
        "max_slippage": "0.005",
        "max_swaps": 3,
        "usdc_decimals": 6,
        "weth_decimals": 18,
    }
    values = {"config": config, "quote": {"selected": {"amount_in_raw": "4000000"}}, "provisioning": {"wallet": wallet}}
    bindings = {"preparation_sha256": digest((preparation / "card.json").read_bytes())}
    for name, value in values.items():
        raw = canonical(value)
        (actor / f"{name}.json").write_bytes(raw)
        bindings[f"{name}_sha256"] = digest(raw)
    (actor / "binding.json").write_bytes(canonical({"config": config, "bindings": bindings}))
    return tmp_path


def test_actor_amount_admission_recomputes_both_swaps_and_complete_census(actor_bundle):
    result = validate_actor_quantity_contract(actor_bundle, POLICY, terminal={"status": "PASS"})
    assert result["status"] == "PASS"
    assert result["actor_lifecycle"] == "one_stimulus_one_unwind"
    assert len(result["rows"]) == 2
    assert "stimulus-quantities/chain/submission-census.json" in result["source_artifacts"]
    assert "stimulus-quantities/almanak_state.db" in result["source_artifacts"]


def test_every_actor_quantity_authority_is_required_for_admission(actor_bundle):
    result = validate_actor_quantity_contract(actor_bundle, POLICY, terminal={"status": "PASS"})
    for name in result["source_artifacts"]:
        path = actor_bundle / name
        original = path.read_bytes()
        try:
            path.write_bytes(b"corrupted evidence")
            with pytest.raises(ValueError):
                validate_actor_quantity_contract(actor_bundle, POLICY, terminal={"status": "PASS"})
        finally:
            path.write_bytes(original)


@pytest.mark.parametrize(
    "field,value", [("max_slippage", "0.004"), ("max_swaps", 4), ("usdc_decimals", 18), ("weth_decimals", 6)]
)
def test_rehashed_actor_limits_cannot_differ_from_frozen_scenario(actor_bundle, field, value):
    path = actor_bundle / "stimulus/config.json"
    config = load_json(path)
    config[field] = value
    path.write_bytes(canonical(config))
    binding_path = actor_bundle / "stimulus/binding.json"
    binding = load_json(binding_path)
    binding["config"] = config
    binding["bindings"]["config_sha256"] = digest(path.read_bytes())
    binding_path.write_bytes(canonical(binding))
    with pytest.raises(ValueError, match="bound stimulus and reserves"):
        validate_actor_quantity_contract(actor_bundle, POLICY, terminal={"status": "PASS"})


@pytest.mark.parametrize("attack", ["amount", "extra_tx", "missing_receipt", "config", "fork", "unfinished", "wal"])
def test_actor_quantity_forgery_cannot_pass(actor_bundle, attack):
    root = actor_bundle / "stimulus-quantities"
    if attack in {"amount", "unfinished"}:
        with sqlite3.connect(root / "almanak_state.db") as db:
            db.execute(
                "UPDATE transaction_ledger SET amount_out='0'"
                if attack == "amount"
                else "UPDATE teardown_requests SET status='running'"
            )
    elif attack == "wal":
        (root / "almanak_state.db-wal").write_bytes(b"pending")
    elif attack == "missing_receipt":
        next((root / "chain").glob("receipt-*.json")).unlink()
    elif attack == "config":
        (actor_bundle / "stimulus/config.json").write_bytes(canonical({"amount_usdc_raw": 1}))
    else:
        path = root / "chain/submission-census.json"
        census = load_json(path)
        if attack == "fork":
            census["blocks"][0]["parentHash"] = "0x" + "ee" * 32
        else:
            census["blocks"][-1]["transactions"].append(
                {"from": anvil_default_address(1).lower(), "hash": "0x" + "dd" * 32}
            )
        path.write_bytes(canonical(census))
    with pytest.raises(ValueError):
        validate_actor_quantity_contract(actor_bundle, POLICY, terminal={"status": "PASS"})
