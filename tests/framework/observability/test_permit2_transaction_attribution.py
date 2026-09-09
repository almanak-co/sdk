"""Permit2 allowance receipts must not become an intent's action identity."""

from types import SimpleNamespace

from eth_utils import keccak

from almanak.framework.observability.ledger import (
    _classify_sub_tx_role,
    _extract_tx_and_gas,
)


def _transaction(tx_hash, signatures):
    return SimpleNamespace(
        tx_hash=tx_hash,
        receipt=SimpleNamespace(logs=[{"topics": [keccak(text=s)]} for s in signatures]),
    )


def test_erc20_permit2_swap_bundle_attributes_actual_action_and_total_gas():
    transactions = [
        _transaction("erc20-approval", ["Approval(address,address,uint256)"]),
        _transaction("permit2-approval", ["Approval(address,address,address,uint160,uint48)"]),
        _transaction("swap", ["Swap(bytes32,address,int128,int128,uint160,uint128,int24,uint24)"]),
    ]
    assert [_classify_sub_tx_role(tx) for tx in transactions] == ["APPROVAL", "APPROVAL", "ACTION"]
    result = SimpleNamespace(transaction_results=transactions, total_gas_used=235841)
    tx_hash, gas_used, _ = _extract_tx_and_gas(result)
    assert tx_hash == "swap"
    assert gas_used == 235841


def test_permit2_approval_does_not_hide_an_action_in_same_receipt():
    tx = _transaction(
        "swap",
        [
            "Approval(address,address,address,uint160,uint48)",
            "Transfer(address,address,uint256)",
        ],
    )
    assert _classify_sub_tx_role(tx) == "ACTION"
