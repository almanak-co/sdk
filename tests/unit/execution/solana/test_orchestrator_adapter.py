"""Branch coverage for SolanaExecutionResult.transaction_results.

The compatibility property re-shapes Solana receipt dicts into the EVM-style
TransactionResult / TransactionReceipt pair StrategyRunner and ResultEnricher
expect. Covered: the failure / empty-receipt short-circuits, field mapping
(slot -> block_number, fee_lamports -> gas_used), the success-flag default,
and tx-hash alignment when hashes are missing. Pure data — no planner, no
network.
"""

from almanak.framework.execution.solana.orchestrator_adapter import SolanaExecutionResult


def _receipt(**overrides):
    data = {
        "signature": "sig1",
        "slot": 250000000,
        "fee_lamports": 5000,
        "success": True,
        "logs": ["Program log: ok"],
    }
    data.update(overrides)
    return data


class TestTransactionResults:
    def test_failed_result_returns_empty_list(self):
        result = SolanaExecutionResult(success=False, receipts=[_receipt()], tx_hashes=["sig1"])
        assert result.transaction_results == []

    def test_success_without_receipts_returns_empty_list(self):
        result = SolanaExecutionResult(success=True, receipts=[], tx_hashes=["sig1"])
        assert result.transaction_results == []

    def test_maps_receipt_fields_onto_evm_shapes(self):
        result = SolanaExecutionResult(
            success=True,
            tx_hashes=["sig1"],
            receipts=[_receipt()],
        )

        [tx_result] = result.transaction_results

        assert tx_result.success is True
        assert tx_result.tx_hash == "sig1"
        assert tx_result.gas_used == 5000
        assert tx_result.logs == ["Program log: ok"]
        receipt = tx_result.receipt
        assert receipt.tx_hash == "sig1"
        assert receipt.block_number == 250000000
        assert receipt.block_hash == ""
        assert receipt.gas_used == 5000
        assert receipt.effective_gas_price == 1
        assert receipt.status == 1

    def test_failed_receipt_maps_to_status_zero(self):
        result = SolanaExecutionResult(
            success=True,
            tx_hashes=["sig1"],
            receipts=[_receipt(success=False)],
        )

        [tx_result] = result.transaction_results

        assert tx_result.success is False
        assert tx_result.receipt.status == 0

    def test_missing_receipt_keys_use_defaults(self):
        result = SolanaExecutionResult(
            success=True,
            tx_hashes=["sig1"],
            receipts=[{}],
        )

        [tx_result] = result.transaction_results

        # success defaults to True, fee/slot/logs default to 0 / 0 / [].
        assert tx_result.success is True
        assert tx_result.gas_used == 0
        assert tx_result.logs == []
        assert tx_result.receipt.block_number == 0

    def test_more_receipts_than_hashes_pads_with_empty_hash(self):
        result = SolanaExecutionResult(
            success=True,
            tx_hashes=["sig1"],
            receipts=[_receipt(), _receipt(slot=250000001, fee_lamports=7000)],
        )

        first, second = result.transaction_results

        assert first.tx_hash == "sig1"
        assert second.tx_hash == ""
        assert second.gas_used == 7000
