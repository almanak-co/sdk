"""G10 distinguishes a receipt-proven retry from partially committed intent batches."""

import copy
import json
from uuid import NAMESPACE_URL, uuid5

import pytest

from almanak.framework.accounting.accountant_test import _cell_g10_multi_tx_atomicity

# Actual Base LP close revert and successful retry, 2026-09-08; economics were
# not written for the revert. The synthetic IDs model the corrected producer.
FAILED_HASH = "0xc8b986e169a85c689a2dfd64db42b95b3f9eecaab7ea483eaac22131909d3fb5"
SUCCESS_HASH = "0x4b5048dc6d5c3b4811650d345d3ba826efe7bb5bd7e3d3a8c4807792231aa0c5"
BLOCK_HASH = "0x906bf148e0108906f695300cb47e58ec96b5141d7e77a8ef93175c438e0407bf"
DEPLOYMENT = "deployment:a7e7ebacafd4"


def rows():
    attempt_id = str(uuid5(NAMESPACE_URL, "almanak:failed-attempt:v1:" + repr((DEPLOYMENT, "base", [FAILED_HASH]))))
    receipt = {
        "tx_hash": FAILED_HASH,
        "status": 0,
        "block_number": 51057008,
        "block_hash": BLOCK_HASH,
        "gas_used": 129355,
        "effective_gas_price": "6000000",
        "l1_fee_wei": "1276003431",
        "logs": [],
    }
    failure = {
        "id": attempt_id,
        "deployment_id": DEPLOYMENT,
        "cycle_id": "same-real-cycle",
        "chain": "base",
        "protocol": "uniswap_v4",
        "intent_type": "LP_CLOSE",
        "timestamp": "2026-09-08T21:35:00Z",
        "tx_hash": FAILED_HASH,
        "success": False,
        "gas_used": receipt["gas_used"],
        "extracted_data_json": {
            "execution_intent_id": "same-original-intent",
            "failed_attempt": {
                "schema_version": 1,
                "ledger_entry_id": attempt_id,
                "receipts": [receipt],
                "total_gas_used": receipt["gas_used"],
            },
            "sub_transactions": [
                {
                    "tx_hash": FAILED_HASH,
                    "status": "failure",
                    "role": "ACTION",
                    "gas_used": receipt["gas_used"],
                    "receipt_evidence": {
                        "status": 0,
                        "block_number": receipt["block_number"],
                        "block_hash": BLOCK_HASH,
                        "gas_used": str(receipt["gas_used"]),
                    },
                }
            ],
        },
    }
    success = {
        **{key: value for key, value in failure.items() if key != "extracted_data_json"},
        "id": "successful-retry",
        "success": True,
        "tx_hash": SUCCESS_HASH,
        "timestamp": "2026-09-08T21:36:00Z",
        "extracted_data_json": {
            "execution_intent_id": "same-original-intent",
            "sub_transactions": [{"tx_hash": SUCCESS_HASH, "status": "success", "receipt_evidence": {"status": 1}}]
        },
    }
    return [failure, success]


def evaluate(values, positions=None, accounting=None):
    copied = copy.deepcopy(values)
    for row in copied:
        row["extracted_data_json"] = json.dumps(row["extracted_data_json"])
    return _cell_g10_multi_tx_atomicity(copied, positions or [], accounting or [])


def test_actual_mainnet_receipt_retry_same_cycle_passes_without_economic_revert_event():
    values = rows()
    assert values[0]["cycle_id"] == values[1]["cycle_id"]
    assert evaluate(values).status == "PASS"


@pytest.mark.parametrize(
    "mutation",
    [
        "marker_only",
        "missing_receipts",
        "status_success",
        "status_bool",
        "logs",
        "hash_mismatch",
        "missing_block",
        "missing_price",
        "sub_hash_missing",
        "sub_hash_duplicate",
        "shared_hash",
        "gas_mismatch",
        "receipt_evidence_mismatch",
        "id_mismatch",
        "missing_timestamp",
        "earlier_success",
        "wrong_chain",
        "wrong_deployment",
        "wrong_protocol",
        "different_intent",
        "missing_success_population",
    ],
)
def test_incomplete_or_non_atomic_retry_remains_failure(mutation):
    values = rows()
    failed, succeeded = values
    data = failed["extracted_data_json"]
    receipt = data["failed_attempt"]["receipts"][0]
    mutations = {
        "marker_only": lambda: data.pop("sub_transactions"),
        "missing_receipts": lambda: data["failed_attempt"].pop("receipts"),
        "status_success": lambda: receipt.update(status=1),
        "status_bool": lambda: receipt.update(status=False),
        "logs": lambda: receipt.update(logs=[{"address": "0x1234"}]),
        "hash_mismatch": lambda: receipt.update(tx_hash=SUCCESS_HASH),
        "missing_block": lambda: receipt.pop("block_hash"),
        "missing_price": lambda: receipt.pop("effective_gas_price"),
        "sub_hash_missing": lambda: data["sub_transactions"][0].update(tx_hash=""),
        "sub_hash_duplicate": lambda: data["sub_transactions"].append(data["sub_transactions"][0]),
        "shared_hash": lambda: succeeded["extracted_data_json"]["sub_transactions"].append({"tx_hash": FAILED_HASH}),
        "gas_mismatch": lambda: failed.update(gas_used=failed["gas_used"] + 1),
        "receipt_evidence_mismatch": lambda: data["sub_transactions"][0]["receipt_evidence"].update(
            block_hash="0x" + "ff" * 32
        ),
        "id_mismatch": lambda: failed.update(id="forged-marker"),
        "missing_timestamp": lambda: failed.pop("timestamp"),
        "earlier_success": lambda: succeeded.update(timestamp="2026-09-08T21:34:00Z"),
        "wrong_chain": lambda: succeeded.update(chain="arbitrum"),
        "wrong_deployment": lambda: succeeded.update(deployment_id="deployment:other"),
        "wrong_protocol": lambda: succeeded.update(protocol="uniswap_v3"),
        "different_intent": lambda: failed.update(intent_type="APPROVE"),
        "missing_success_population": lambda: succeeded.update(extracted_data_json={}),
    }
    mutations[mutation]()
    assert evaluate(values).status == "FAIL", mutation


@pytest.mark.parametrize("lane", ["position", "accounting"])
@pytest.mark.parametrize("binding", ["ledger_entry_id", "tx_hash", "payload"])
def test_economic_event_on_reverted_attempt_is_not_exempt(lane, binding):
    values = rows()
    event = {"ledger_entry_id": values[0]["id"]} if binding == "ledger_entry_id" else {"tx_hash": FAILED_HASH}
    if binding == "payload":
        event = {"payload_json": json.dumps(event)}
    kwargs = {"positions" if lane == "position" else "accounting": [event]}
    assert evaluate(values, **kwargs).status == "FAIL"


def test_duplicate_attempt_does_not_pass_as_two_retries():
    values = rows()
    assert evaluate([values[0], copy.deepcopy(values[0]), values[1]]).status == "FAIL"


@pytest.mark.parametrize("failure", ["partial_retry", "unmeasured_retry", "bogus_block", "partial_first_attempt"])
def test_retry_exception_never_masks_partial_execution(failure):
    values = rows()
    failed_data = values[0]["extracted_data_json"]
    retry = values[1]["extracted_data_json"]["sub_transactions"][0]
    if failure == "partial_retry":
        retry["receipt_evidence"]["status"] = 0
    elif failure == "unmeasured_retry":
        retry.pop("receipt_evidence")
    elif failure == "bogus_block":
        failed_data["failed_attempt"]["receipts"][0]["block_hash"] = "garbage"
        failed_data["sub_transactions"][0]["receipt_evidence"]["block_hash"] = "garbage"
    else:
        receipt = copy.deepcopy(failed_data["failed_attempt"]["receipts"][0])
        receipt.update(tx_hash="0x" + "ab" * 32, status=1)
        failed_data["failed_attempt"]["receipts"].append(receipt)
        failed_data["sub_transactions"].append({"tx_hash": receipt["tx_hash"], "status": "success"})
    assert evaluate(values).status == "FAIL"


@pytest.mark.parametrize("lineage", [None, "", "other-position-close"])
def test_same_type_different_or_missing_intent_is_not_a_retry(lineage):
    values = rows()
    values[1]["extracted_data_json"]["execution_intent_id"] = lineage
    assert evaluate(values).status == "FAIL"


def test_missing_failed_intent_lineage_is_unproven():
    values = rows()
    values[0]["extracted_data_json"].pop("execution_intent_id")
    assert evaluate(values).status == "FAIL"


@pytest.mark.parametrize("form", ["bare", "uppercase_bare", "mixed_prefix"])
def test_real_local_receipt_hash_forms_have_identical_identity(form):
    values = rows()
    for row in values:
        row["tx_hash"] = row["tx_hash"].removeprefix("0x")
        data = row["extracted_data_json"]
        for transaction in data["sub_transactions"]:
            transaction["tx_hash"] = transaction["tx_hash"].removeprefix("0x")
        for receipt in data.get("failed_attempt", {}).get("receipts", []):
            receipt["tx_hash"] = receipt["tx_hash"].removeprefix("0x")
            receipt["block_hash"] = receipt["block_hash"].removeprefix("0x")
            data["sub_transactions"][0]["receipt_evidence"]["block_hash"] = receipt["block_hash"]
            if form == "uppercase_bare":
                receipt["tx_hash"] = receipt["tx_hash"].upper()
                receipt["block_hash"] = receipt["block_hash"].upper()
            elif form == "mixed_prefix":
                receipt["tx_hash"] = "0x" + receipt["tx_hash"]
                receipt["block_hash"] = "0x" + receipt["block_hash"]
    assert evaluate(values).status == "PASS"


@pytest.mark.parametrize("value", ["f" * 63, "f" * 65, "0x" + "z" * 64, " " + "f" * 64])
def test_invalid_hash_width_or_hex_remains_rejected(value):
    values = rows()
    values[0]["extracted_data_json"]["sub_transactions"][0]["tx_hash"] = value
    assert evaluate(values).status == "FAIL"
