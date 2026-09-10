"""Captured no-rebalance controls plus a synthetic third-generation transition."""

import json
from copy import deepcopy
from pathlib import Path

import pytest
from eth_abi import decode, encode
from web3 import Web3

from qa_lab.e2e_generation_verdict import GENERATION_ARTIFACTS, generation_predicates, validate_generation_contract
from qa_lab.e2e_positions import DECREASE, POSITION_TYPES

FIXTURES = Path(__file__).resolve().parents[2] / "fixtures/accounting/harness/lp-dual-generations"
CELL = {
    "strategy_path": "strategies/accounting/lp_dual",
    "chain": "arbitrum",
    "protocol": "uniswap_v3",
    "primitive": "lp",
    "network": "anvil",
    "exec_path": "eoa",
}
CONTRACT = {"nft_generation_scenario": "lp-dual-rebalance-v1"}


@pytest.fixture
def captured():
    return tuple(
        json.loads((FIXTURES / name).read_text()) for name in ("positions-open.json", "positions-terminal.json")
    )


def test_real_open_close_is_not_a_rebalance(captured):
    opened, terminal = captured
    verdict = generation_predicates(opened, opened, terminal)
    assert verdict["rebalance"]["status"] == "FAIL"
    assert "exactly one new" in verdict["rebalance"]["reason"]
    assert verdict["terminal_closure"] == {"status": "PASS", "closed_generations": ["5685394", "5685395"]}


def test_absent_observation_cannot_certify_rebalance_or_closure(captured):
    verdict = generation_predicates(captured[0], None, None)
    assert verdict["rebalance"]["status"] == verdict["terminal_closure"]["status"] == "UNMEASURED"


@pytest.fixture
def rebalanced(captured):
    opened, terminal = captured
    managed = deepcopy(opened)
    narrow = opened["generations"][0]["token_id"]
    new_token = str(int(narrow) + 2)
    closure = [
        event for event in terminal["logs"][len(opened["logs"]) :] if int(event["topics"][-1], 16) == int(narrow)
    ]
    managed["logs"].extend(deepcopy(closure))
    block = closure[-1]["block_number"] + 1
    managed["end_block"] = block
    managed["end_block_hash"] = "0x" + "22" * 32
    managed["generations"][0] = deepcopy(terminal["generations"][0])
    for index, original in enumerate(opened["logs"][:2]):
        event = deepcopy(original)
        event.update(
            block_number=block, block_hash=managed["end_block_hash"], log_index=index, transaction_hash="0x" + "11" * 32
        )
        event["topics"][-1] = "0x" + f"{int(new_token):064x}"
        managed["logs"].append(event)
    position = deepcopy(opened["generations"][0])
    raw = list(decode(POSITION_TYPES, Web3.to_bytes(hexstr=position["terminal_position_response"])))
    raw[5] += 740
    raw[6] += 740
    position.update(
        token_id=new_token,
        mint_block=block,
        mint_transaction="0x" + "11" * 32,
        terminal_position_response=Web3.to_hex(encode(POSITION_TYPES, raw)),
        tick_lower=raw[5],
        tick_upper=raw[6],
    )
    managed["generations"].append(position)
    return opened, managed


def test_synthetic_replacement_with_held_wide_generation(rebalanced):
    opened, managed = rebalanced
    verdict = generation_predicates(opened, managed, None)
    assert verdict["rebalance"] == {
        "status": "PASS",
        "narrow_original": "5685394",
        "narrow_replacement": "5685396",
        "wide_held": "5685395",
    }
    assert verdict["terminal_closure"]["status"] == "UNMEASURED"


@pytest.mark.parametrize(
    "fault", ["inventory", "summary_liquidity", "raw_liquidity", "dropped_event", "duplicate", "fork", "wide_transfer"]
)
def test_generation_mutations_fail_for_their_actual_contradiction(rebalanced, fault):
    opened, managed = rebalanced
    if fault == "inventory":
        managed["generations"].pop()
        reason = "inventory"
    elif fault == "summary_liquidity":
        managed["generations"][-1]["terminal_liquidity_raw"] = "0"
        reason = "liquidity"
    elif fault == "raw_liquidity":
        position = managed["generations"][-1]
        raw = list(decode(POSITION_TYPES, Web3.to_bytes(hexstr=position["terminal_position_response"])))
        raw[7] = 0
        position["terminal_position_response"] = Web3.to_hex(encode(POSITION_TYPES, raw))
        position["terminal_liquidity_raw"] = "0"
        reason = "liquidity"
    elif fault == "dropped_event":
        managed["logs"].pop(1)
        reason = "removal exceeds"
    elif fault == "duplicate":
        managed["logs"].insert(1, deepcopy(managed["logs"][0]))
        reason = "duplicate"
    elif fault == "fork":
        managed["fork_identity"]["instance_id"] = "another-instance"
        reason = "different runs"
    else:
        event = deepcopy(managed["logs"][-2])
        event["log_index"] = 2
        event["topics"][1] = "0x" + opened["wallet"][2:].lower().zfill(64)
        event["topics"][2] = "0x" + ("33" * 20).zfill(64)
        event["topics"][3] = "0x" + f"{5685395:064x}"
        managed["logs"].append(event)
        managed["generations"][1]["owner"] = "0x" + "33" * 20
        reason = "wide generation was transferred"
    verdict = generation_predicates(opened, managed, None)["rebalance"]
    assert verdict["status"] == "FAIL"
    assert reason in verdict["reason"]


def test_deleted_terminal_generation_cannot_prove_closure(captured):
    opened, terminal = captured
    terminal["generations"].pop()
    verdict = generation_predicates(opened, None, terminal)["terminal_closure"]
    assert verdict["status"] == "FAIL"
    assert "inventory" in verdict["reason"]


def test_replacement_minted_before_old_liquidity_removal_is_not_rebalance(rebalanced):
    opened, managed = rebalanced
    replacement_block = managed["end_block"]
    closure = managed["logs"][len(opened["logs"]) : -2]
    for offset, event in enumerate(closure, 1):
        event["block_number"] = replacement_block + offset
    managed["logs"].sort(key=lambda event: (event["block_number"], event["log_index"]))
    managed["end_block"] = managed["logs"][-1]["block_number"]
    managed["end_block_hash"] = managed["logs"][-1]["block_hash"]
    verdict = generation_predicates(opened, managed, None)["rebalance"]
    assert verdict["status"] == "FAIL"
    assert "closed before replacement" in verdict["reason"]


@pytest.fixture
def generation_bundle(rebalanced, tmp_path):
    opened, managed = rebalanced
    terminal = deepcopy(managed)
    terminal["end_block"] += 1
    terminal["end_block_hash"] = "0x" + "66" * 32
    for index, position in enumerate(terminal["generations"]):
        if position["burned"]:
            continue
        amount = int(position["terminal_liquidity_raw"])
        event = {
            "block_number": terminal["end_block"],
            "block_hash": terminal["end_block_hash"],
            "log_index": index,
            "transaction_hash": "0x" + f"{index + 1:064x}",
            "address": terminal["position_manager"],
            "topics": [Web3.to_hex(DECREASE), "0x" + f"{int(position['token_id']):064x}"],
            "data": Web3.to_hex(encode(["uint128", "uint256", "uint256"], [amount, 1, 1])),
        }
        terminal["logs"].append(event)
        raw = list(decode(POSITION_TYPES, Web3.to_bytes(hexstr=position["terminal_position_response"])))
        raw[7] = 0
        position.update(terminal_position_response=Web3.to_hex(encode(POSITION_TYPES, raw)), terminal_liquidity_raw="0")
    for name, witness in zip(GENERATION_ARTIFACTS, (opened, managed, terminal), strict=True):
        (tmp_path / name).write_text(json.dumps(witness))
    return tmp_path, sorted({event["transaction_hash"] for event in terminal["logs"]})


def test_generation_contract_recomputes_closed_rebalance(generation_bundle):
    bundle, transactions = generation_bundle
    evidence = validate_generation_contract(bundle, CONTRACT, transactions, catalog_cell=CELL)
    assert evidence["rebalance"]["status"] == evidence["terminal_closure"]["status"] == "PASS"
    assert evidence["transactions"] == transactions


def test_generation_contract_cannot_borrow_transactions_outside_lifecycle(generation_bundle):
    bundle, transactions = generation_bundle
    with pytest.raises(ValueError, match="absent from the receipt-backed"):
        validate_generation_contract(bundle, CONTRACT, transactions[1:], catalog_cell=CELL)


@pytest.mark.parametrize(
    ("field", "value"), [("network", "mainnet"), ("exec_path", "safe"), ("chain", "base"), ("strategy_path", "other")]
)
def test_generation_contract_cannot_paint_another_cell(generation_bundle, field, value):
    bundle, transactions = generation_bundle
    with pytest.raises(ValueError, match="exact Arbitrum Anvil EOA"):
        validate_generation_contract(bundle, CONTRACT, transactions, catalog_cell={**CELL, field: value})
