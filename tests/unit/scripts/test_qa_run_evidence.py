"""Mutation guards for quant-run journal, nonce, and receipt evidence."""

from __future__ import annotations

import hashlib
import importlib.util
import json
import os
import sqlite3
import sys
from decimal import Decimal
from pathlib import Path

import pytest

SCRIPT = Path(__file__).parents[3] / "qa_lab" / "qa_run_evidence.py"


def _load():
    spec = importlib.util.spec_from_file_location("qa_run_evidence_test", SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def _row(total: int, parts: list[int], *, row_id: str = "intent-1", price: str = "2000"):
    hashes = ["0x" + hashlib.sha256(f"{row_id}:{i}".encode()).hexdigest() for i in range(len(parts))]
    sub = [
        {"tx_hash": tx_hash, "gas_used": gas, "role": "ACTION" if i == len(parts) - 1 else "APPROVAL"}
        for i, (tx_hash, gas) in enumerate(zip(hashes, parts, strict=True))
    ]
    return {
        "chain": "ethereum",
        "id": row_id,
        "intent_type": "TEST",
        "tx_hash": hashes[-1],
        "gas_used": total,
        "gas_usd": str(Decimal(total) * Decimal(10**9) / Decimal(10**18) * Decimal(price)),
        "extracted_data_json": json.dumps(
            {
                "sub_transactions": sub,
                "all_tx_results": [{"tx_hash": x["tx_hash"], "gas_used": x["gas_used"]} for x in sub],
            }
        ),
        "price_inputs_json": json.dumps(
            {"ETH": {"price_usd": price, "oracle_source": "chainlink", "fetched_at": "2026-08-08T00:00:00Z"}}
        ),
    }


def _receipts(rows):
    result = {}
    module = _load()
    for row in rows:
        for member in module.canonical_transactions(row):
            result[member["tx_hash"]] = {
                "transactionHash": member["tx_hash"],
                "blockNumber": "0x64",
                "blockHash": "0x" + "ab" * 32,
                "gasUsed": hex(member["gas_used"]),
                "effectiveGasPrice": hex(10**9),
                "status": "0x1",
                "logs": [],
            }
    return result


@pytest.mark.parametrize(
    ("name", "total", "parts"),
    [
        ("aave", 525_059, [33_501, 491_558]),
        ("benqi", 1_409_016, [46_327, 55_437, 1_307_252]),
        ("trading_agent", 429_765, [429_765]),
        ("looping", 2_749_789, [33_501, 55_437, 74_295, 2_586_556]),
        ("lp_dual", 2_257_587, [86_329, 86_329, 2_084_929]),
    ],
)
def test_real_shape_canonical_totals_include_auxiliary_transactions(name, total, parts):
    module = _load()
    row = _row(total, parts, row_id=name)
    receipts = _receipts([row])
    result = module.reconcile_receipts([row], receipt_lookup=receipts.get)
    assert result["native_unit_reconciliation"] == {
        "aggregate_gas_cost_wei": total * 10**9,
        "aggregate_gas_used": total,
        "status": "PASS",
    }
    assert result["canonical_hash_count"] == len(parts)


def test_log_hashes_are_supplemental_and_cannot_replace_missing_canonical_receipt():
    module = _load()
    row = _row(2_749_789, [33_501, 55_437, 74_295, 2_586_556])
    receipts = _receipts([row])
    missing = module.canonical_transactions(row)[0]["tx_hash"]
    receipts.pop(missing)
    with pytest.raises(module.EvidenceError, match="canonical receipt missing"):
        module.reconcile_receipts([row], receipt_lookup=receipts.get, supplemental_hashes=[missing])


def test_zero_ledger_rows_cannot_report_receipt_reconciliation_pass():
    module = _load()

    with pytest.raises(module.EvidenceError, match="at least one canonical ledger intent"):
        module.reconcile_receipts([], receipt_lookup=lambda _tx_hash: None)


def test_terminal_manifest_rejects_hand_written_zero_receipt_pass(tmp_path):
    module = _load()
    bundle = tmp_path / "bundle"
    bundle.mkdir()
    module.verify_journal = lambda _path: (1, "a" * 64)
    (bundle / "command-journal-start-anchor.json").write_text(json.dumps({"run_id": "run-1", "head_sha256": "a" * 64}))
    for name in ("nonce-start.json", "nonce-end.json", "nonce-reconciliation.json"):
        (bundle / name).write_text("{}")
    (bundle / "receipt-reconciliation.json").write_text(
        json.dumps(
            {
                "canonical_hash_count": 0,
                "canonical_hashes": [],
                "native_unit_reconciliation": {"status": "PASS"},
                "run_scope": {"run_id": "run-1"},
                "usd_price_basis_reconciliation": {"status": "PASS"},
            }
        )
    )

    with pytest.raises(module.EvidenceError, match="at least one canonical transaction"):
        module.finalize_manifest(bundle, tmp_path / "manifest.json", run_id="run-1")


def test_mutated_receipt_gas_fails_closed():
    module = _load()
    row = _row(525_059, [33_501, 491_558])
    receipts = _receipts([row])
    receipts[row["tx_hash"]]["gasUsed"] = hex(491_557)
    with pytest.raises(module.EvidenceError, match="receipt gas mismatch"):
        module.reconcile_receipts([row], receipt_lookup=receipts.get)


def test_disagreeing_persisted_membership_arrays_fail_closed():
    module = _load()
    row = _row(525_059, [33_501, 491_558])
    extracted = json.loads(row["extracted_data_json"])
    extracted["all_tx_results"][0]["gas_used"] += 1
    row["extracted_data_json"] = json.dumps(extracted)
    with pytest.raises(module.EvidenceError, match="disagree"):
        module.canonical_transactions(row)


def test_contemporaneous_prices_verify_without_false_fixed_anchor_correction():
    module = _load()
    first = _row(250_000, [250_000], row_id="a", price="1913.27")
    second = _row(275_059, [275_059], row_id="b", price="1913.21040564")
    receipts = _receipts([first, second])
    result = module.reconcile_receipts([first, second], receipt_lookup=receipts.get)
    assert result["usd_price_basis_reconciliation"]["status"] == "PASS"
    assert [x["native_price_basis"]["price_usd"] for x in result["intents"]] == [
        "1913.27",
        "1913.21040564",
    ]
    anchor_reprice = Decimal(525_059) * Decimal(10**9) / Decimal(10**18) * Decimal("1913.96")
    assert anchor_reprice != Decimal(result["usd_price_basis_reconciliation"]["aggregate_ledger_gas_usd"])


def test_journal_full_chain_and_separate_start_anchor_detect_mutation(tmp_path):
    module = _load()
    journal = module.initialize_journal(tmp_path, run_id="run-1", actor="operator", timestamp="2026-08-09T00:00:00Z")
    module.append_journal(
        journal,
        run_id="run-1",
        actor="operator",
        phase="run",
        event="command_finished",
        details={"argv": ["uv", "run", "almanak"], "returncode": 0},
        timestamp="2026-08-09T00:01:00Z",
    )
    assert module.verify_journal(journal)[0] == 2
    records = journal.read_text().splitlines()
    records[0] = records[0].replace("operator", "attacker")
    journal.write_text("\n".join(records) + "\n")
    with pytest.raises(module.EvidenceError, match="digest mismatch"):
        module.verify_journal(journal)


def test_terminal_manifest_rejects_a_fully_rewritten_valid_chain(tmp_path):
    module = _load()
    bundle = tmp_path / "bundle"
    journal = module.initialize_journal(bundle, run_id="run-1", actor="operator", timestamp="2026-08-09T00:00:00Z")
    module.append_journal(
        journal,
        run_id="run-1",
        actor="operator",
        phase="run",
        event="command_finished",
        details={"returncode": 0},
        timestamp="2026-08-09T00:01:00Z",
    )
    (bundle / "git.json").write_text(json.dumps({"commit": "a" * 40, "dirty": False, "sdk_version": "1.2.3"}))
    start = {
        "block_hash": "0x" + "11" * 32,
        "block_number": 1,
        "chain_id": 1,
        "label": "start",
        "nonce": 4,
        "run_id": "run-1",
        "wallet": "0x" + "22" * 20,
    }
    end = {**start, "block_hash": "0x" + "33" * 32, "block_number": 2, "label": "end", "nonce": 5}
    tx_hash = "0x" + "44" * 32
    reconciliation = module.bind_nonce_transaction_hashes(module.reconcile_nonce_anchors(start, end), [tx_hash])
    (bundle / "nonce-start.json").write_text(json.dumps(start))
    (bundle / "nonce-end.json").write_text(json.dumps(end))
    (bundle / "nonce-reconciliation.json").write_text(json.dumps(reconciliation))
    (bundle / "receipt-reconciliation.json").write_text(
        json.dumps(
            {
                "canonical_hashes": [tx_hash],
                "canonical_hash_count": 1,
                "native_unit_reconciliation": {"status": "PASS"},
                "run_scope": {"run_id": "run-1"},
                "usd_price_basis_reconciliation": {"status": "PASS"},
            }
        )
    )
    external_manifest = tmp_path / "sealed-manifest.json"
    module.finalize_manifest(bundle, external_manifest, run_id="run-1")

    records = [json.loads(line) for line in journal.read_text().splitlines()]
    records[0]["details"]["trust_boundary"] = "rewritten"
    previous = module.ZERO_HASH
    for sequence, record in enumerate(records):
        record["sequence"] = sequence
        record["previous_sha256"] = previous
        record.pop("record_sha256", None)
        record["record_sha256"] = module._sha(record)
        previous = record["record_sha256"]
    journal.write_text("\n".join(json.dumps(record) for record in records) + "\n")
    assert module.verify_journal(journal)[0] == 2
    with pytest.raises(module.EvidenceError, match="terminal manifest"):
        module.verify_manifest(bundle, external_manifest)


def test_nonce_anchors_are_block_pinned_and_bound_all_wallet_transactions():
    module = _load()
    wallet = "0x" + "11" * 20

    def rpc(method, params):
        return {
            "eth_chainId": "0x1",
            "eth_blockNumber": "0x64",
            "eth_getBlockByNumber": {"hash": "0x" + "22" * 32},
            "eth_getTransactionCount": "0x7",
            "eth_getCode": "0x",
        }[method]

    start = module.capture_nonce_anchor(rpc=rpc, wallet=wallet, label="start", run_id="run-1")
    end = {**start, "block_number": 120, "nonce": 10, "label": "end"}
    result = module.reconcile_nonce_anchors(start, end)
    assert result["wallet_originated_transaction_count"] == 3
    assert "approvals" in result["scope"]
    assert "before sweep" in result["scope"]
    hashes = ["0x" + f"{i:064x}" for i in range(1, 4)]
    assert module.bind_nonce_transaction_hashes(result, hashes)["transaction_hash_binding"]["status"] == "PASS"


def test_nonce_hash_binding_fails_when_count_does_not_match_delta():
    module = _load()
    reconciliation = {"wallet_originated_transaction_count": 2}
    with pytest.raises(module.EvidenceError, match="nonce delta"):
        module.bind_nonce_transaction_hashes(reconciliation, ["0x" + "11" * 32])


def test_alm_3267_reverted_primary_receipt_is_retained_and_classified():
    module = _load()
    row = _row(429_765, [429_765])
    row["success"] = 0
    receipts = _receipts([row])
    receipts[row["tx_hash"]]["status"] = "0x0"
    result = module.reconcile_receipts([row], receipt_lookup=receipts.get)
    assert result["intents"][0]["ledger_execution_outcome"] == "FAILED"
    assert result["intents"][0]["transactions"][0]["execution_outcome"] == "REVERTED"
    assert result["submission_receipt_integrity"] == {
        "async_order_ids": [],
        "reverted_transaction_hashes": [row["tx_hash"]],
        "status": "FAIL",
        "submitted_transaction_count": 1,
        "successful_transaction_hashes": [],
        "terminal_receipt_count": 1,
        "unresolved_submission_control": "nonce reconciliation binds the wallet-originated count to these hashes",
        "unresolved_submission_count": 0,
    }


def test_alm_3276_canonical_receipt_requires_explicit_matching_identity():
    """A lookup keyed by the right hash cannot fill in an identity-free receipt."""
    module = _load()
    row = _row(429_765, [429_765])
    receipts = _receipts([row])
    receipts[row["tx_hash"]].pop("transactionHash")

    with pytest.raises(module.EvidenceError, match="has no transaction identity"):
        module.reconcile_receipts([row], receipt_lookup=receipts.get)


def test_nonce_capture_fails_closed_when_rpc_response_is_incomplete():
    module = _load()
    with pytest.raises(module.EvidenceError, match="incomplete"):
        module.capture_nonce_anchor(rpc=lambda *_: None, wallet="0x" + "11" * 20, label="start", run_id="run-1")


@pytest.mark.parametrize(
    ("demo", "profile"),
    [("benqi_lending_lifecycle", "lending_lifecycle"), ("accounting_looping", "looping"), ("v4_roundtrip", "spot")],
)
def test_accountant_profile_is_explicit_and_demo_specific(demo, profile):
    module = _load()
    result = module.accountant_profile_from_card(f"# Card\nACCOUNTANT_PROFILE: {profile}\n", demo=demo)
    assert result == {"applicability": "APPLICABLE", "profile": profile, "reason": None}


@pytest.mark.parametrize(
    "card",
    [
        "# no profile\n",
        "ACCOUNTANT_PROFILE: invented\n",
        "ACCOUNTANT_PROFILE: N/A: short\n",
        "ACCOUNTANT_PROFILE: looping\nACCOUNTANT_PROFILE: lp\n",
    ],
)
def test_accountant_profile_missing_unknown_or_ambiguous_fails_closed(card):
    module = _load()
    with pytest.raises(module.EvidenceError):
        module.accountant_profile_from_card(card, demo="some_demo")


def test_benqi_profile_cannot_be_inferred_as_looping_or_declared_inapplicable():
    module = _load()
    with pytest.raises(module.EvidenceError, match="requires"):
        module.accountant_profile_from_card("ACCOUNTANT_PROFILE: looping\n", demo="benqi_lending_lifecycle")
    with pytest.raises(module.EvidenceError, match="cannot declare"):
        module.accountant_profile_from_card(
            "ACCOUNTANT_PROFILE: N/A: lending rows were not inspected\n", demo="benqi_lending_lifecycle"
        )


def _unbound_row(row_id: str, error: str) -> dict:
    return {
        "chain": "ethereum",
        "id": row_id,
        "intent_type": "BORROW",
        "tx_hash": None,
        "success": 0,
        "gas_used": 0,
        "gas_usd": "0",
        "extracted_data_json": "",
        "price_inputs_json": "{}",
        "error": error,
    }


def test_intent_without_transaction_identity_is_listed_unbound_not_never_submitted():
    module = _load()
    submitted = _row(429_765, [429_765], row_id="swap")
    lost_hash = _unbound_row(
        "borrow",
        "BROADCAST_RECONCILIATION_REQUIRED: execution crossed the submission boundary"
        " without retaining a transaction identifier; refusing automatic replay until reconciled: x",
    )
    pre_submit = _unbound_row("borrow-2", "Insufficient ETH: need 2, have 1")
    receipts = _receipts([submitted])
    result = module.reconcile_receipts([submitted, lost_hash, pre_submit], receipt_lookup=receipts.get)
    assert result["canonical_hash_count"] == 1
    assert [x["intent_type"] for x in result["intents"]] == ["TEST"]
    assert "not_submitted_intents" not in result
    assert result["unbound_intents"] == [
        {
            "intent_id": "borrow",
            "intent_type": "BORROW",
            "ledger_execution_outcome": "SUBMISSION_UNRESOLVED",
            "error": lost_hash["error"],
        },
        {
            "intent_id": "borrow-2",
            "intent_type": "BORROW",
            "ledger_execution_outcome": "NO_TRANSACTION_IDENTITY",
            "error": pre_submit["error"],
        },
    ]
    integrity = result["submission_receipt_integrity"]
    assert integrity["unresolved_submission_count"] == 1
    assert "nonce reconciliation" in integrity["unresolved_submission_control"]
    with pytest.raises(module.EvidenceError, match="at least one canonical ledger intent"):
        module.reconcile_receipts([lost_hash], receipt_lookup=receipts.get)


def test_identity_less_row_with_unmeasured_success_fails_closed():
    module = _load()
    submitted = _row(429_765, [429_765], row_id="swap")
    unmeasured = _unbound_row("borrow", "BROADCAST_RECONCILIATION_REQUIRED: lost hash")
    unmeasured["success"] = None
    receipts = _receipts([submitted])
    with pytest.raises(module.EvidenceError, match="no canonical transaction membership"):
        module.reconcile_receipts([submitted, unmeasured], receipt_lookup=receipts.get)


def test_ledger_rows_carry_the_error_an_unbound_record_reports(tmp_path):
    module = _load()
    db = tmp_path / "db.sqlite"
    with sqlite3.connect(db) as connection:
        connection.execute(
            "CREATE TABLE transaction_ledger (id TEXT, deployment_id TEXT, timestamp TEXT, intent_type TEXT,"
            " chain TEXT, protocol TEXT, tx_hash TEXT, gas_used INTEGER, gas_usd TEXT, success INTEGER,"
            " error TEXT, extracted_data_json TEXT, price_inputs_json TEXT)"
        )
        connection.execute(
            "INSERT INTO transaction_ledger VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                "borrow",
                "deployment:abc",
                "2026-09-06T05:43:27+00:00",
                "BORROW",
                "robinhood",
                "morpho_blue",
                None,
                0,
                "0",
                0,
                "BROADCAST_RECONCILIATION_REQUIRED: insufficient funds for gas",
                "",
                "{}",
            ),
        )
    rows = module.load_ledger_rows(
        db, deployment_id="deployment:abc", run_start="2026-09-06T00:00:00+00:00", run_end="2026-09-07T00:00:00+00:00"
    )
    assert rows[0]["error"] == "BROADCAST_RECONCILIATION_REQUIRED: insufficient funds for gas"
    assert module._unbound_intent_record(rows[0])["error"] == rows[0]["error"]


def _reconciliation_result():
    return {
        "intents": [
            {"transactions": [{"tx_hash": "0xaa", "raw_receipt": {"status": "0x1", "transactionHash": "0xaa"}}]},
            {"transactions": [{"tx_hash": "0xbb", "raw_receipt": {"status": "0x1", "transactionHash": "0xbb"}}]},
        ]
    }


def test_interrupted_publication_leaves_no_reconciliation(tmp_path):
    module = _load()
    receipt_dir = tmp_path / "receipts"
    receipt_dir.mkdir()
    receipt_dir.chmod(0o555)
    try:
        with pytest.raises(PermissionError):
            module.publish_receipt_reconciliation(_reconciliation_result(), tmp_path / "out.json", receipt_dir)
    finally:
        receipt_dir.chmod(0o755)
    assert not (tmp_path / "out.json").exists()
    assert list(receipt_dir.iterdir()) == []
    module.publish_receipt_reconciliation(_reconciliation_result(), tmp_path / "out.json", receipt_dir)
    assert (tmp_path / "out.json").exists()
    assert sorted(p.name for p in receipt_dir.iterdir()) == ["receipt-0xaa.json", "receipt-0xbb.json"]


def test_interrupted_write_leaves_no_partial_file_at_a_final_name(tmp_path, monkeypatch):
    module = _load()
    receipt_dir = tmp_path / "receipts"
    calls = []
    real_replace = os.replace

    def interrupted_replace(src, dst):
        calls.append(dst)
        if len(calls) == 2:
            raise KeyboardInterrupt
        return real_replace(src, dst)

    monkeypatch.setattr(module.os, "replace", interrupted_replace)
    with pytest.raises(KeyboardInterrupt):
        module.publish_receipt_reconciliation(_reconciliation_result(), tmp_path / "out.json", receipt_dir)
    monkeypatch.undo()
    assert sorted(p.name for p in receipt_dir.iterdir()) == ["receipt-0xaa.json"]
    assert not (tmp_path / "out.json").exists()
    assert not list(tmp_path.glob(".*.tmp.*")) and not list(receipt_dir.glob(".*.tmp.*"))
    module.publish_receipt_reconciliation(_reconciliation_result(), tmp_path / "out.json", receipt_dir)
    assert (tmp_path / "out.json").exists()
    assert sorted(p.name for p in receipt_dir.iterdir()) == ["receipt-0xaa.json", "receipt-0xbb.json"]


def test_publication_reuses_identical_receipts_and_refuses_different_ones(tmp_path):
    module = _load()
    receipt_dir = tmp_path / "receipts"
    module.publish_receipt_reconciliation(_reconciliation_result(), tmp_path / "first.json", receipt_dir)
    module.publish_receipt_reconciliation(_reconciliation_result(), tmp_path / "second.json", receipt_dir)
    assert (tmp_path / "second.json").exists()
    (receipt_dir / "receipt-0xbb.json").write_text("SENTINEL\n")
    with pytest.raises(module.EvidenceError, match="refusing to overwrite canonical receipt"):
        module.publish_receipt_reconciliation(_reconciliation_result(), tmp_path / "third.json", receipt_dir)
    assert not (tmp_path / "third.json").exists()
    assert (receipt_dir / "receipt-0xbb.json").read_text() == "SENTINEL\n"
    with pytest.raises(module.EvidenceError, match="refusing to overwrite receipt reconciliation"):
        module.publish_receipt_reconciliation(_reconciliation_result(), tmp_path / "first.json", receipt_dir)


@pytest.mark.parametrize("include_l1_in_ledger", [True, False])
def test_observed_base_l1_fee_is_required_in_ledger_reconciliation(include_l1_in_ledger):
    module = _load()
    observed = json.loads((SCRIPT.parent.parent / "tests/fixtures/execution/base_additive_l1_fee.json").read_text())
    receipts = {raw["transactionHash"]: raw for raw in observed["raw_receipts"]}
    parts = [int(raw["gasUsed"], 16) for raw in receipts.values()]
    row = _row(sum(parts), parts, row_id="base-observed", price="2498.5")
    sub = [
        {"tx_hash": tx_hash, "gas_used": int(raw["gasUsed"], 16), "role": "ACTION"} for tx_hash, raw in receipts.items()
    ]
    row.update(chain="base", tx_hash=sub[-1]["tx_hash"], extracted_data_json=json.dumps({"sub_transactions": sub}))
    cost = Decimal(observed["expected_cost_wei"] if include_l1_in_ledger else observed["expected_execution_cost_wei"])
    row["gas_usd"] = str(cost / Decimal(10**18) * Decimal("2498.5"))
    if not include_l1_in_ledger:
        with pytest.raises(module.EvidenceError, match="does not reconcile"):
            module.reconcile_receipts([row], receipt_lookup=receipts.get)
        return
    result = module.reconcile_receipts([row], receipt_lookup=receipts.get)
    assert result["native_unit_reconciliation"]["aggregate_gas_cost_wei"] == 2927288871908
    assert result["usd_price_basis_reconciliation"]["status"] == "PASS"
    assert sum(tx["l1_fee_wei"] for tx in result["intents"][0]["transactions"]) == 22745682539


def test_malformed_l1_receipt_fee_fails_closed_in_evidence():
    module = _load()
    row = _row(21000, [21000])
    receipts = _receipts([row])
    next(iter(receipts.values()))["l1Fee"] = "invalid"
    with pytest.raises(module.EvidenceError, match="invalid additive receipt fee"):
        module.reconcile_receipts([row], receipt_lookup=receipts.get)


@pytest.mark.parametrize("chain", ["base", "optimism"])
@pytest.mark.parametrize("missing", [True, False])
def test_op_receipt_without_measured_l1_fee_cannot_pass_reconciliation(chain, missing):
    module = _load()
    row = _row(21000, [21000])
    row["chain"] = chain
    receipts = _receipts([row])
    receipt = next(iter(receipts.values()))
    if not missing:
        receipt["l1Fee"] = None
    with pytest.raises(module.EvidenceError, match="L1 receipt fee is unmeasured"):
        module.reconcile_receipts([row], receipt_lookup=receipts.get)
    receipt["l1Fee"] = "0x0"
    result = module.reconcile_receipts([row], receipt_lookup=receipts.get)
    assert result["usd_price_basis_reconciliation"]["status"] == "PASS"


@pytest.mark.parametrize("chain", ["ethereum", "arbitrum"])
def test_nonadditive_chain_cost_does_not_require_op_fee(chain):
    module = _load()
    row = _row(21000, [21000])
    row["chain"] = chain
    receipts = _receipts([row])
    if chain == "arbitrum":
        next(iter(receipts.values()))["gasUsedForL1"] = "0x42"
    result = module.reconcile_receipts([row], receipt_lookup=receipts.get)
    assert result["native_unit_reconciliation"]["aggregate_gas_cost_wei"] == 21000 * 10**9
