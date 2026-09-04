"""Mutation guards for quant-run journal, nonce, and receipt evidence."""

from __future__ import annotations

import hashlib
import importlib.util
import json
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
    [("benqi_lending_lifecycle", "lending_lifecycle"), ("accounting_looping", "looping")],
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
