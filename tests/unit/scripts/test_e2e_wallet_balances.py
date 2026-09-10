import copy
from types import SimpleNamespace

import pytest
from web3 import Web3

from almanak.gateway.qa_wallet_balances import capture_wallet_balances
from qa_lab.e2e_wallet_balance_replay import reconcile_inventory_balances

WALLET = "0x" + "aa" * 20
TOKEN = "0x" + "bb" * 20
START = {"number": 100, "hash": "0x" + "cc" * 32}
END = {"number": 101, "hash": "0x" + "dd" * 32}


def measurement(block, amount):
    return {
        "schema_version": 1,
        "scope": "raw_wallet_balances",
        "wallet": WALLET,
        "block": block,
        "native_wei": "1000000000000000000",
        "nonce": 0,
        "wallet_code": "0x",
        "tokens": {
            TOKEN: {
                "call": {"to": Web3.to_checksum_address(TOKEN), "data": "0x70a08231" + WALLET[2:].rjust(64, "0")},
                "response": "0x" + f"{amount:064x}",
            }
        },
    }


@pytest.fixture
def evidence():
    return (
        {"wallet": WALLET, "assets": {TOKEN: {"standard": "ERC20", "net_transfer_raw": "7"}}},
        measurement(START, 10),
        measurement(END, 17),
    )


def replay(evidence):
    return reconcile_inventory_balances(*evidence, start=START, terminal=END)


def test_exact_measured_balance_difference_matches_transfer_census(evidence):
    result = replay(evidence)
    assert result["status"] == "PASS"
    assert result["assets"][TOKEN]["delta_raw"] == "7"
    assert result["native_reconciliation"] == "UNMEASURED"
    assert result["preexisting_undiscovered_assets"] == "UNMEASURED"


@pytest.mark.parametrize("fault", ["quantity", "wrong_call", "wallet", "block"])
def test_contradictory_measurement_cannot_reconcile(evidence, fault):
    _, _, closing = evidence
    if fault == "quantity":
        closing["tokens"][TOKEN]["response"] = "0x" + f"{18:064x}"
    elif fault == "wrong_call":
        closing["tokens"][TOKEN]["call"]["to"] = Web3.to_checksum_address(WALLET)
    elif fault == "wallet":
        closing["wallet"] = TOKEN
    else:
        closing["block"] = START
    assert replay(evidence)["status"] == "FAIL"


@pytest.mark.parametrize("missing", [None, "", "0x"])
def test_empty_token_measurement_is_not_zero(evidence, missing):
    evidence[2]["tokens"][TOKEN]["response"] = missing
    assert replay(evidence)["status"] == "UNMEASURED"


def test_measured_zero_is_valid(evidence):
    evidence[0]["assets"][TOKEN]["net_transfer_raw"] = "-10"
    evidence[2]["tokens"][TOKEN]["response"] = "0x" + "0" * 64
    assert replay(evidence)["status"] == "PASS"


def test_missing_newly_discovered_asset_is_not_inferred_to_have_zero_opening_balance(evidence):
    evidence[1]["tokens"].clear()
    assert replay(evidence)["status"] == "UNMEASURED"


def test_unsupported_token_standard_cannot_inherit_erc20_balance_math(evidence):
    evidence[0]["assets"][TOKEN]["standard"] = "ERC1155_UNSUPPORTED"
    assert replay(evidence)["status"] == "UNMEASURED"


def test_collector_retains_raw_calls_at_the_pinned_block():
    calls = []
    block = {"number": 100, "hash": bytes.fromhex(START["hash"][2:])}

    def balance(call, *, block_identifier):
        calls.append((copy.deepcopy(call), block_identifier))
        return (10).to_bytes(32, "big")

    client = SimpleNamespace(
        eth=SimpleNamespace(
            get_block=lambda identifier: block,
            call=balance,
            get_balance=lambda *args, **kw: 10**18,
            get_transaction_count=lambda *args, **kw: 0,
            get_code=lambda *args, **kw: b"",
        )
    )
    observed = capture_wallet_balances(client, WALLET, [TOKEN], block_identifier=100)
    assert observed["tokens"] == measurement(START, 10)["tokens"]
    assert calls == [(observed["tokens"][TOKEN]["call"], 100)]
    assert observed["block"] == START


def test_inventory_replay_binds_baseline_census_and_closing_measurement(tmp_path):
    from qa_lab.e2e_card import canonical, digest
    from qa_lab.e2e_wallet_inventory import observe_subject_inventory

    (tmp_path / "preparation").mkdir()
    identity = {"fork_block": 100, "fork_hash": START["hash"], "manifest_sha256": "a" * 64}
    initial = {
        "schema_version": 1,
        "scope": "pre_dispatch_subject_inventory",
        "run_id": "inventory-test",
        "fork_identity": identity,
        "manifest_sha256": "a" * 64,
        "balances": measurement(START, 10),
    }
    census = {
        "start": START,
        "terminal": END,
        "fork_identity": identity,
        "blocks": [
            {"block": {**END, "parentHash": START["hash"], "transactions": []}, "receipts": [], "transactions": []}
        ],
    }
    captured = {
        "schema_version": 1,
        "scope": "subject_wallet_inventory_capture",
        "status": "CAPTURED",
        "initial_sha256": digest(canonical(initial)),
        "census_sha256": digest(canonical(census)),
        "closing": measurement(END, 10),
    }
    files = {
        "subject-initial-balances.json": initial,
        "wallet-receipt-census.json": census,
        "wallet-inventory.json": captured,
        "terminal-boundary.json": {"wallet_inventory_sha256": digest(canonical(captured))},
        "preparation/funding.json": {"subject_effective_token_amounts": {TOKEN: "10"}},
        "gateway-startup.json": {"run_id": "inventory-test", "fork_identity": identity, "subject_wallet": WALLET},
        "positions-terminal.json": {
            "wallet": WALLET,
            "fork_identity": identity,
            "end_block": 101,
            "end_block_hash": END["hash"],
        },
    }
    for name, value in files.items():
        (tmp_path / name).write_bytes(canonical(value))
    result = observe_subject_inventory(tmp_path)
    assert result["status"] == "PASS"
    assert result["native_reconciliation"]["status"] == "PASS"
    captured["closing"] = measurement(END, 11)
    (tmp_path / "wallet-inventory.json").write_bytes(canonical(captured))
    assert observe_subject_inventory(tmp_path)["status"] == "FAIL"
    captured["closing"] = measurement(END, 10)
    initial["balances"] = measurement(START, 11)
    (tmp_path / "wallet-inventory.json").write_bytes(canonical(captured))
    (tmp_path / "subject-initial-balances.json").write_bytes(canonical(initial))
    assert observe_subject_inventory(tmp_path)["status"] == "FAIL"
