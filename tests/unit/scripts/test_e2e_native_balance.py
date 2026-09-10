import copy

import pytest

from qa_lab.e2e_native_balance import reconcile_native_balance

WALLET = "0x" + "aa" * 20
OTHER = "0x" + "bb" * 20
TX = "0x" + "cc" * 32
START = {"number": 100, "hash": "0x" + "dd" * 32}
END = {"number": 101, "hash": "0x" + "ee" * 32}


@pytest.fixture
def evidence():
    opening = {"wallet": WALLET, "wallet_code": "0x", "block": START, "native_wei": "1000000", "nonce": 4}
    closing = {**opening, "block": END, "native_wei": "958000", "nonce": 5}
    receipt = {
        "transactionHash": TX,
        "transactionIndex": 0,
        "blockNumber": 101,
        "blockHash": END["hash"],
        "status": 1,
        "logs": [],
        "from": WALLET,
        "gasUsed": 21000,
        "effectiveGasPrice": 2,
    }
    transaction = {
        "hash": TX,
        "blockHash": END["hash"],
        "blockNumber": 101,
        "transactionIndex": 0,
        "from": WALLET,
        "to": OTHER,
        "nonce": 4,
        "gas": 30000,
        "type": 2,
        "value": 0,
    }
    census = {
        "blocks": [
            {
                "block": {**END, "parentHash": START["hash"], "transactions": [TX]},
                "receipts": [receipt],
                "transactions": [transaction],
            }
        ]
    }
    return census, opening, closing


def test_exact_native_difference_matches_gas_and_wallet_nonce(evidence):
    result = reconcile_native_balance(*evidence)
    assert result["status"] == "PASS"
    assert result["gas_cost_wei"] == "42000"
    assert result["transactions"] == [{"tx_hash": TX, "gas_cost_wei": "42000"}]


def test_reverted_transaction_still_costs_gas(evidence):
    evidence[0]["blocks"][0]["receipts"][0]["status"] = 0
    assert reconcile_native_balance(*evidence)["status"] == "PASS"


@pytest.mark.parametrize(
    "fault", ["balance", "gas", "nonce_gap", "closing_nonce", "wrong_sender", "wrong_hash", "missing_transaction"]
)
def test_native_contradictions_cannot_pass(evidence, fault):
    census, _, closing = evidence
    item = census["blocks"][0]
    tx = item["transactions"][0]
    if fault == "balance":
        closing["native_wei"] = "958001"
    elif fault == "gas":
        item["receipts"][0]["gasUsed"] += 1
    elif fault == "nonce_gap":
        tx["nonce"] += 1
    elif fault == "closing_nonce":
        closing["nonce"] += 1
    elif fault == "wrong_sender":
        tx["from"] = OTHER
    elif fault == "wrong_hash":
        tx["hash"] = START["hash"]
    else:
        item["transactions"].clear()
    assert reconcile_native_balance(*evidence)["status"] == "FAIL"


@pytest.mark.parametrize(
    "unsupported", ["native_send", "delegated", "authorization", "missing_code", "missing_balance", "missing_tx"]
)
def test_unmeasured_or_unsupported_native_paths_do_not_inherit_gas_only_proof(evidence, unsupported):
    census, opening, closing = evidence
    if unsupported == "native_send":
        census["blocks"][0]["transactions"][0]["value"] = 1
    elif unsupported == "delegated":
        closing["wallet_code"] = "0xef0100" + OTHER[2:]
    elif unsupported == "authorization":
        census["blocks"][0]["transactions"][0]["type"] = 4
    elif unsupported == "missing_code":
        opening.pop("wallet_code")
    elif unsupported == "missing_balance":
        opening["native_wei"] = None
    else:
        census["blocks"][0].pop("transactions")
    assert reconcile_native_balance(*evidence)["status"] == "UNMEASURED"


def test_gas_from_another_wallet_cannot_be_charged_to_subject(evidence):
    census, opening, closing = copy.deepcopy(evidence)
    item = census["blocks"][0]
    item["receipts"][0]["from"] = OTHER
    item["transactions"][0]["from"] = OTHER
    closing.update(native_wei=opening["native_wei"], nonce=opening["nonce"])
    assert reconcile_native_balance(census, opening, closing)["gas_cost_wei"] == "0"
