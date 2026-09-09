"""Replay observed Base receipts through typed, recovered and gateway fee paths."""

import json
from decimal import Decimal
from pathlib import Path

import pytest

from almanak.framework.execution.gateway_orchestrator import GatewayExecutionResult
from almanak.framework.execution.interfaces import TransactionReceipt
from almanak.framework.execution.nonce_recovery import build_complete_evm_receipt
from almanak.framework.execution.receipt_costs import measured_gas_cost_wei, receipt_l1_fee_wei

FIXTURE = Path(__file__).resolve().parents[2] / "fixtures/execution/base_additive_l1_fee.json"


def test_observed_base_bundle_preserves_l1_fee_across_receipt_boundaries():
    observed = json.loads(FIXTURE.read_text())
    raw_receipts = observed["raw_receipts"]
    typed = [build_complete_evm_receipt(raw, expected_tx_hash=raw["transactionHash"]) for raw in raw_receipts]
    assert all(item is not None for item in typed)
    assert sum(item.gas_used * item.effective_gas_price for item in typed) == 2904543189369
    assert sum(item.l1_fee_wei for item in typed) == 22745682539
    assert sum(item.gas_cost_wei for item in typed) == 2927288871908
    assert sum(item.gas_cost_eth for item in typed) == Decimal("0.000002927288871908")
    serialized = json.loads(json.dumps([item.to_dict() for item in typed]))
    roundtripped = [TransactionReceipt.from_dict(item) for item in serialized]
    assert [item.l1_fee_wei for item in roundtripped] == [item.l1_fee_wei for item in typed]
    recovered = [build_complete_evm_receipt(item, expected_tx_hash=item.tx_hash) for item in roundtripped]
    assert sum(item.gas_cost_wei for item in recovered) == 2927288871908
    result = GatewayExecutionResult(
        success=True,
        tx_hashes=[item.tx_hash for item in typed],
        total_gas_used=235841,
        receipts=serialized,
        execution_id="observed-base-fees",
    )
    assert result.success
    assert result.total_gas_cost_wei == 2927288871908
    assert sum(item.gas_cost_wei for item in result.transaction_results) == 2927288871908
    assert [item.receipt.l1_fee_wei for item in result.transaction_results] == [item.l1_fee_wei for item in typed]


@pytest.mark.parametrize("status", [0, 1])
def test_additive_l1_fee_is_paid_on_success_and_revert(status):
    raw = json.loads(FIXTURE.read_text())["raw_receipts"][0]
    raw["status"] = hex(status)
    receipt = build_complete_evm_receipt(raw, expected_tx_hash=raw["transactionHash"])
    assert receipt is not None
    assert receipt.gas_cost_wei == int(raw["gasUsed"], 16) * int(raw["effectiveGasPrice"], 16) + int(raw["l1Fee"], 16)


@pytest.mark.parametrize("fee", [None, 0, 12345])
def test_absent_and_measured_zero_fees_remain_distinct_on_wire(fee):
    receipt = TransactionReceipt(
        tx_hash="0x" + "11" * 32,
        block_number=1,
        block_hash="0x" + "22" * 32,
        gas_used=21000,
        effective_gas_price=7,
        status=1,
        l1_fee_wei=fee,
    )
    copied = TransactionReceipt.from_dict(receipt.to_dict())
    assert copied.l1_fee_wei == fee
    assert copied.gas_cost_wei == 147000 + (fee if fee is not None else 0)
    assert receipt.to_dict()["l1_fee_wei"] is None if fee is None else receipt.to_dict()["l1_fee_wei"] == str(fee)


@pytest.mark.parametrize("value", ["0x0", "0", 0])
def test_explicit_zero_is_measured(value):
    assert receipt_l1_fee_wei({"l1Fee": value}) == 0


def test_l1_gas_and_blob_components_are_not_added_twice():
    arb = {"gasUsedForL1": 10000, "l1GasUsed": "0x123", "l1GasPrice": "0x456"}
    assert receipt_l1_fee_wei(arb) is None
    assert measured_gas_cost_wei(21000, 7, receipt_l1_fee_wei(arb)) == 147000
    op = {**arb, "l1Fee": "0x64", "l1BlobBaseFee": "0x1000", "blobGasUsed": "0x100"}
    assert measured_gas_cost_wei(21000, 7, receipt_l1_fee_wei(op)) == 147100


@pytest.mark.parametrize("bad", [True, False, -1, "-1", "", "garbage", 1.5])
def test_invalid_additive_fee_is_not_silently_zeroed(bad):
    with pytest.raises(ValueError, match="nonnegative integer"):
        receipt_l1_fee_wei({"l1Fee": bad})


def test_conflicting_wire_and_rpc_fee_values_are_rejected():
    with pytest.raises(ValueError, match="Conflicting"):
        receipt_l1_fee_wei({"l1_fee_wei": "5", "l1Fee": "0x6"})


@pytest.mark.parametrize("field", ["gas_used", "effective_gas_price"])
@pytest.mark.parametrize("value", [None, "", "bogus", True, False, -1, "-1", 1.5, "1.5"])
def test_gateway_does_not_price_partial_execution_cost(field, value):
    from almanak.framework.accounting.accountant_test import _cell_g11_failed_intents
    from almanak.framework.observability.ledger import _extract_tx_and_gas

    receipt = {"gas_used": 21000, "effective_gas_price": 7, "status": 0, field: value}
    result = GatewayExecutionResult(
        success=False,
        tx_hashes=["0x" + "ab" * 32],
        total_gas_used=21000,
        receipts=[receipt],
        execution_id="unmeasured-gas",
    )
    assert result.total_gas_cost_wei is None
    _, gas_used, gas_usd = _extract_tx_and_gas(result, chain="ethereum", price_oracle={"ETH": Decimal(2500)})
    assert gas_usd == ""
    assert _cell_g11_failed_intents([{"success": False, "gas_used": gas_used, "gas_usd": gas_usd}]).status == "FAIL"


@pytest.mark.parametrize(
    "receipts",
    [
        [],
        [{}],
        [None],
        [{"gasUsed": 1}],
        [{"gas_used": 1, "gasUsed": 2, "effectiveGasPrice": 7}],
        [{"gas_used": 1, "effective_gas_price": 7}, {}],
    ],
)
def test_gateway_incomplete_or_conflicting_receipts_do_not_return_a_subtotal(receipts):
    result = GatewayExecutionResult(
        success=False,
        tx_hashes=["0x" + "ab" * 32],
        total_gas_used=21000,
        receipts=receipts,
        execution_id="incomplete-gas",
    )
    assert result.total_gas_cost_wei is None


@pytest.mark.parametrize("price,expected", [(0, 0), ("0x0", 0), ("0x7", 147000), ("7", 147000)])
def test_gateway_explicit_zero_and_hex_costs_remain_measured(price, expected):
    result = GatewayExecutionResult(
        success=False,
        tx_hashes=["0x" + "ab" * 32],
        total_gas_used=21000,
        receipts=[{"gas_used": "0x5208", "gasUsed": 21000, "effectiveGasPrice": price}],
        execution_id="measured-gas",
    )
    assert result.total_gas_cost_wei == expected


def test_gateway_solana_cost_keeps_measured_lamports():
    result = GatewayExecutionResult(
        success=False,
        tx_hashes=["signature"],
        total_gas_used=5000,
        receipts=[{"fee_lamports": 5000}],
        execution_id="solana-fee",
        chain_family="SOLANA",
    )
    assert result.total_gas_cost_wei == 5000
    result.receipts[0]["fee_lamports"] = None
    assert result.total_gas_cost_wei is None


@pytest.mark.parametrize("price", [None, "bogus", False])
def test_multichain_adapter_refuses_unmeasured_gateway_cost_before_ledger_write(price):
    from types import SimpleNamespace

    from almanak.framework.runner.strategy_runner import StrategyRunner

    gateway_result = GatewayExecutionResult(
        success=False,
        tx_hashes=["0x" + "ab" * 32],
        total_gas_used=21000,
        receipts=[{"gas_used": 21000, "effective_gas_price": price}],
        execution_id="gateway-leg",
    )
    runner = StrategyRunner.__new__(StrategyRunner)
    leg = SimpleNamespace(tx_result=gateway_result)
    with pytest.raises(ValueError, match="receipt reconciliation is required"):
        runner._adapt_leg_to_execution_result(leg)
    with pytest.raises(ValueError, match="receipt reconciliation is required"):
        runner._failed_leg_execution_result(leg, [], 0, 0, "failed leg")
    gateway_result.receipts[0]["effective_gas_price"] = 0
    adapted, _ = runner._adapt_leg_to_execution_result(leg)
    assert adapted.total_gas_cost_wei == 0


@pytest.mark.parametrize("fields", [{"l1_fee_wei": "bad"}, {"l1_fee_wei": 1, "l1Fee": 2}])
def test_gateway_invalid_l1_fee_cannot_become_execution_only_cost(fields):
    result = GatewayExecutionResult(
        success=False,
        tx_hashes=["0x" + "ab" * 32],
        total_gas_used=21000,
        receipts=[
            {
                "tx_hash": "0x" + "ab" * 32,
                "block_number": 100,
                "block_hash": "0x" + "cd" * 32,
                "status": 0,
                "logs": [],
                "gas_used": 21000,
                "effective_gas_price": 1,
                **fields,
            }
        ],
        execution_id="invalid-l1",
    )
    assert result.total_gas_cost_wei is None
    with pytest.raises(ValueError):
        _ = result.transaction_results
