import copy
from types import SimpleNamespace

import pytest

from qa_lab.e2e_wallet_discovery import SINGLE, TRANSFER, capture_wallet_census, discover_wallet_assets

WALLET = "0x" + "aa" * 20
SENDER = "0x" + "bb" * 20
FUNDED = "0x" + "cc" * 20
UNEXPECTED = "0x" + "dd" * 20
TX = "0x" + "11" * 32
START = {"number": 100, "hash": "0x" + "22" * 32}
END = {"number": 101, "hash": "0x" + "33" * 32}


def topic(address):
    return "0x" + address[2:].rjust(64, "0")


@pytest.fixture
def census():
    receipt = {"transactionHash": TX, "transactionIndex": 0, "blockNumber": 101, "blockHash": END["hash"], "status": 1}
    log = {
        **receipt,
        "logIndex": 0,
        "removed": False,
        "address": UNEXPECTED,
        "topics": [TRANSFER, topic(SENDER), topic(WALLET)],
        "data": "0x" + f"{7:064x}",
    }
    log.pop("status")
    return [
        {"block": {**END, "parentHash": START["hash"], "transactions": [TX]}, "receipts": [{**receipt, "logs": [log]}]}
    ]


def replay(census):
    return discover_wallet_assets(census, wallet=WALLET, funded_tokens=[FUNDED], start=START, terminal=END)


def test_discovers_unsolicited_token_outside_funded_assets_and_sdk_transactions(census):
    result = replay(census)
    assert result["assets"][UNEXPECTED] == {"standard": "ERC20", "funded": False, "net_transfer_raw": "7"}
    assert result["assets"][FUNDED] == {"standard": "ERC20", "funded": True, "net_transfer_raw": "0"}
    assert result["transactions_scanned"] == 1
    assert result["preexisting_undiscovered_assets"] == "UNMEASURED"


@pytest.mark.parametrize("standard", ["ERC721", "ERC1155_UNSUPPORTED"])
def test_nfts_and_unsupported_asset_standards_cannot_disappear(census, standard):
    log = census[0]["receipts"][0]["logs"][0]
    if standard == "ERC721":
        log["topics"].append("0x" + f"{123:064x}")
        log["data"] = "0x"
    else:
        log["topics"] = [SINGLE, topic(SENDER), topic(SENDER), topic(WALLET)]
        log["data"] = "0x" + f"{123:064x}{5:064x}"
    result = replay(census)
    assert result["assets"][UNEXPECTED]["standard"] == standard


@pytest.mark.parametrize(
    "fault",
    [
        "receipt_missing",
        "wrong_receipt",
        "missing_block",
        "parent",
        "receipt_block",
        "duplicate_log",
        "log_index_gap",
        "removed",
        "unmined",
        "bad_topic",
        "truncated_amount",
    ],
)
def test_census_gaps_and_contradictory_logs_refuse_discovery(census, fault):
    item = census[0]
    receipt = item["receipts"][0]
    log = receipt["logs"][0]
    if fault == "receipt_missing":
        item["receipts"] = []
    elif fault == "wrong_receipt":
        receipt["transactionHash"] = "0x" + "44" * 32
    elif fault == "missing_block":
        census.clear()
    elif fault == "parent":
        item["block"]["parentHash"] = END["hash"]
    elif fault == "receipt_block":
        receipt["blockNumber"] = 102
    elif fault == "duplicate_log":
        receipt["logs"].append(copy.deepcopy(log))
    elif fault == "log_index_gap":
        log["logIndex"] = 1
    elif fault == "removed":
        log["removed"] = True
    elif fault == "unmined":
        receipt["status"] = 0
    elif fault == "bad_topic":
        log["topics"][1] = "0x" + "1" * 64
    else:
        log["data"] = "0x07"
    with pytest.raises(ValueError):
        replay(census)


def test_self_transfer_is_discovered_without_creating_money(census):
    census[0]["receipts"][0]["logs"][0]["topics"][1] = topic(WALLET)
    assert replay(census)["assets"][UNEXPECTED]["net_transfer_raw"] == "0"


def test_outgoing_transfer_keeps_negative_raw_amount(census):
    census[0]["receipts"][0]["logs"][0]["topics"] = [TRANSFER, topic(WALLET), topic(SENDER)]
    assert replay(census)["assets"][UNEXPECTED]["net_transfer_raw"] == "-7"


def test_collector_fetches_all_receipts_and_discovers_external_transfer(census):
    captured = []
    block = {
        **census[0]["block"],
        "hash": bytes.fromhex(END["hash"][2:]),
        "parentHash": bytes.fromhex(START["hash"][2:]),
        "transactions": [bytes.fromhex(TX[2:])],
    }

    def receipt(tx):
        captured.append(tx)
        return census[0]["receipts"][0]

    client = SimpleNamespace(
        eth=SimpleNamespace(
            get_block=lambda number: block, get_transaction_receipt=receipt, get_transaction=lambda tx: {"hash": tx}
        )
    )
    context = SimpleNamespace(
        fork_block=100,
        fork_hash=START["hash"],
        assert_rpc_identity=lambda *args: client,
        public_identity=lambda: {"instance_id": "owned"},
    )
    raw = capture_wallet_census(context, END)
    assert captured == [TX]
    assert raw["blocks"][0]["transactions"] == [{"hash": TX}]
    assert raw["blocks"][0]["receipts"][0]["from"] == ""
    assert replay(raw["blocks"])["assets"][UNEXPECTED]["net_transfer_raw"] == "7"


def test_admission_exposes_discovery_without_claiming_wallet_reconciliation(census, tmp_path):
    from qa_lab.e2e_card import canonical, digest
    from qa_lab.e2e_residual_policy import assess_residual_policy

    (tmp_path / "preparation").mkdir()
    identity = {"fork_block": START["number"], "fork_hash": START["hash"]}
    raw = {
        "schema_version": 1,
        "scope": "complete_fork_receipt_census",
        "fork_identity": identity,
        "start": START,
        "terminal": END,
        "blocks": census,
    }
    files = {
        "wallet-receipt-census.json": raw,
        "terminal-boundary.json": {"wallet_census_sha256": digest(canonical(raw))},
        "positions-terminal.json": {
            "wallet": WALLET,
            "fork_identity": identity,
            "end_block": END["number"],
            "end_block_hash": END["hash"],
        },
        "preparation/funding.json": {"subject_effective_token_amounts": {FUNDED: "10"}},
    }
    for name, value in files.items():
        (tmp_path / name).write_bytes(canonical(value))
    contract = {
        "residual_policy": {
            "known_nft_liquidity_raw": "0",
            "pending_orders": 0,
            "wallet_policy": "inventory_all_tokens_and_reconcile_subject_transactions",
        }
    }

    def assess():
        return assess_residual_policy(contract, generations=None, quantities=None, actor=None, bundle=tmp_path)

    result = assess()
    assert result["status"] == "UNMEASURED"
    assert result["wallet_asset_discovery"]["status"] == "DISCOVERED"
    assert UNEXPECTED in result["wallet_asset_discovery"]["assets"]
    assert result["all_token_wallet_inventory"] == "UNMEASURED"
    (tmp_path / "wallet-receipt-census.json").write_bytes(canonical({**raw, "blocks": []}))
    assert assess()["status"] == "FAIL"
    assert "subject_wallet_discovery_failed" in assess()["reason_codes"]
