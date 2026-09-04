"""Receipt normalization contracts for nonce recovery."""

from typing import Any

import pytest

from almanak.framework.execution.interfaces import TransactionReceipt
from almanak.framework.execution.nonce_recovery import _quantity, build_complete_evm_receipt


@pytest.mark.parametrize(
    ("value", "expected"),
    [(0, 0), (7, 7), (" 0010 ", 10), ("0x10", 16), ("0X10", 16)],
)
def test_quantity_parses_supported_rpc_values(value: Any, expected: int) -> None:
    assert _quantity(value) == expected


@pytest.mark.parametrize("value", [None, True, False, -1, "-1", "", " ", "invalid", "0xGG", 1.5])
def test_quantity_rejects_invalid_rpc_values(value: Any) -> None:
    assert _quantity(value) is None


def test_build_complete_evm_receipt_normalizes_mapping_aliases_and_quantities() -> None:
    receipt = build_complete_evm_receipt(
        {
            "transaction_hash": "ABC",
            "block_number": "0x10",
            "block_hash": "0xblock",
            "gas_used": "21000",
            "effective_gas_price": "0x7",
            "status": "0x0",
            "logs": ({"address": "0xtoken"},),
            "contract_address": "0xcontract",
            "from_address": "0xfrom",
            "to_address": "0xto",
        },
        expected_tx_hash="0xabc",
    )

    assert receipt == TransactionReceipt(
        tx_hash="ABC",
        block_number=16,
        block_hash="0xblock",
        gas_used=21000,
        effective_gas_price=7,
        status=0,
        logs=[{"address": "0xtoken"}],
        contract_address="0xcontract",
        from_address="0xfrom",
        to_address="0xto",
    )


def test_build_complete_evm_receipt_accepts_complete_typed_receipt() -> None:
    original = TransactionReceipt(
        tx_hash="0xabc",
        block_number=16,
        block_hash="0xblock",
        gas_used=21000,
        effective_gas_price=7,
        status=1,
        logs=[],
    )

    assert build_complete_evm_receipt(original, expected_tx_hash="ABC") == original


@pytest.mark.parametrize(
    "override",
    [
        {"transactionHash": "0xdef"},
        {"blockNumber": None},
        {"blockHash": ""},
        {"gasUsed": True},
        {"effectiveGasPrice": -1},
        {"status": 2},
        {"logs": "not-logs"},
        {"logs": [object()]},
    ],
)
def test_build_complete_evm_receipt_rejects_invalid_evidence(override: dict[str, Any]) -> None:
    raw_receipt = {
        "transactionHash": "0xabc",
        "blockNumber": 16,
        "blockHash": "0xblock",
        "gasUsed": 21000,
        "effectiveGasPrice": 7,
        "status": 0,
        "logs": [],
    }
    raw_receipt.update(override)

    assert build_complete_evm_receipt(raw_receipt, expected_tx_hash="0xabc") is None
