"""Branch coverage for MultiChainOrchestrator._compile_and_execute_intent.

Extends the no-op cases in test_multichain_gateway.py with the remaining
branches: compile failure, price-oracle override/restore, hex coercions,
EOA sequential execution (success and mid-sequence failure), and Safe-mode
MultiSend bundling. Compiler and executor are mocked; no chain access.
"""

import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest

from almanak.framework.execution.chain_executor import TransactionExecutionResult
from almanak.framework.execution.multichain import ExecutionError, MultiChainOrchestrator
from almanak.framework.intents.compiler import CompilationResult, CompilationStatus
from almanak.framework.models.reproduction_bundle import ActionBundle


def _make_mco():
    mock_config = MagicMock()
    mock_config.execution_address = "0x" + "ee" * 20
    mco = MultiChainOrchestrator(config=mock_config)
    mock_compiler = MagicMock()
    mock_compiler.price_oracle = {"ETH": "3000"}
    mock_compiler._using_placeholders = False
    mco._compilers = {"arbitrum": mock_compiler}
    return mco, mock_compiler


def _executor(*, safe_mode=False, results=None):
    executor = MagicMock()
    executor._chain = "arbitrum"
    executor._chain_id = 42161
    executor.is_safe_mode = safe_mode
    executor.get_gas_params = AsyncMock(
        return_value={"max_fee_per_gas": 100, "max_priority_fee_per_gas": 2}
    )
    executor.get_next_nonce = AsyncMock(side_effect=[7, 8, 9])
    executor.execute_transaction = AsyncMock(
        side_effect=results or [TransactionExecutionResult(success=True, tx_hash="0x1")]
    )
    executor.execute_bundle = AsyncMock(
        return_value=TransactionExecutionResult(success=True, tx_hash="0xbundle")
    )
    return executor


def _wire_compile(mock_compiler, transactions, status=CompilationStatus.SUCCESS, error=None):
    bundle = ActionBundle(intent_type="SWAP", transactions=transactions, metadata={})
    mock_compiler.compile.return_value = CompilationResult(
        status=status,
        intent_id="intent-abc",
        action_bundle=bundle if status == CompilationStatus.SUCCESS else None,
        error=error,
    )


def _intent():
    intent = MagicMock()
    intent.intent_id = "intent-abcdef123456"
    return intent


def _tx(**overrides):
    tx = {"to": "0x" + "aa" * 20, "data": "0xdeadbeef"}
    tx.update(overrides)
    return tx


class TestCompileFailures:
    def test_failed_compilation_raises(self):
        mco, compiler = _make_mco()
        _wire_compile(compiler, [], status=CompilationStatus.FAILED, error="no pool")
        with pytest.raises(ExecutionError, match="Intent compilation failed"):
            asyncio.run(mco._compile_and_execute_intent(_intent(), _executor()))


class TestPriceOracleOverride:
    def test_override_applied_and_restored(self):
        mco, compiler = _make_mco()
        _wire_compile(compiler, [_tx()])
        asyncio.run(
            mco._compile_and_execute_intent(
                _intent(), _executor(), price_oracle={"ETH": "3500"}
            )
        )
        compiler.update_prices.assert_called_once_with({"ETH": "3500"})
        compiler.restore_prices.assert_called_once_with({"ETH": "3000"}, False)

    def test_restored_even_when_compile_raises(self):
        mco, compiler = _make_mco()
        compiler.compile.side_effect = ValueError("compiler exploded")
        with pytest.raises(ValueError, match="compiler exploded"):
            asyncio.run(
                mco._compile_and_execute_intent(
                    _intent(), _executor(), price_oracle={"ETH": "3500"}
                )
            )
        compiler.restore_prices.assert_called_once()

    def test_no_override_without_price_oracle(self):
        mco, compiler = _make_mco()
        _wire_compile(compiler, [_tx()])
        asyncio.run(mco._compile_and_execute_intent(_intent(), _executor()))
        compiler.update_prices.assert_not_called()
        compiler.restore_prices.assert_not_called()


class TestEoaExecution:
    def test_single_transaction_success(self):
        mco, compiler = _make_mco()
        _wire_compile(compiler, [_tx()])
        executor = _executor()
        result = asyncio.run(mco._compile_and_execute_intent(_intent(), executor))
        assert result.success
        assert result.tx_hash == "0x1"
        unsigned = executor.execute_transaction.call_args[0][0]
        assert unsigned.to == _tx()["to"]
        assert unsigned.value == 0
        assert unsigned.gas_limit == int(500000 * 1.2)  # default estimate + buffer
        assert unsigned.nonce == 7
        assert unsigned.from_address == mco._config.execution_address
        assert unsigned.max_fee_per_gas == 100

    @pytest.mark.parametrize(
        ("gas_estimate", "value", "expected_gas", "expected_value"),
        [
            ("0x7a120", "0x10", int(500000 * 1.2), 16),
            ("300000", "25", int(300000 * 1.2), 25),
            (200000, 5, int(200000 * 1.2), 5),
        ],
    )
    def test_hex_and_string_coercions(self, gas_estimate, value, expected_gas, expected_value):
        mco, compiler = _make_mco()
        _wire_compile(compiler, [_tx(gas_estimate=gas_estimate, value=value)])
        executor = _executor()
        asyncio.run(mco._compile_and_execute_intent(_intent(), executor))
        unsigned = executor.execute_transaction.call_args[0][0]
        assert unsigned.gas_limit == expected_gas
        assert unsigned.value == expected_value

    def test_multi_transaction_sequential_returns_last(self):
        mco, compiler = _make_mco()
        _wire_compile(compiler, [_tx(description="approve"), _tx(description="swap")])
        executor = _executor(
            results=[
                TransactionExecutionResult(success=True, tx_hash="0xapprove"),
                TransactionExecutionResult(success=True, tx_hash="0xswap"),
            ]
        )
        result = asyncio.run(mco._compile_and_execute_intent(_intent(), executor))
        assert result.tx_hash == "0xswap"
        assert executor.execute_transaction.call_count == 2
        nonces = [call[0][0].nonce for call in executor.execute_transaction.call_args_list]
        assert nonces == [7, 8]

    def test_mid_sequence_failure_stops_execution(self):
        mco, compiler = _make_mco()
        _wire_compile(compiler, [_tx(), _tx()])
        executor = _executor(
            results=[
                TransactionExecutionResult(success=False, tx_hash="", error="reverted"),
                TransactionExecutionResult(success=True, tx_hash="0xnever"),
            ]
        )
        result = asyncio.run(mco._compile_and_execute_intent(_intent(), executor))
        assert not result.success
        assert result.error == "reverted"
        assert executor.execute_transaction.call_count == 1


class TestSafeMode:
    def test_multiple_transactions_bundled_atomically(self):
        mco, compiler = _make_mco()
        _wire_compile(compiler, [_tx(), _tx()])
        executor = _executor(safe_mode=True)
        result = asyncio.run(mco._compile_and_execute_intent(_intent(), executor))
        assert result.tx_hash == "0xbundle"
        executor.execute_bundle.assert_awaited_once()
        unsigned_txs = executor.execute_bundle.call_args[0][0]
        assert len(unsigned_txs) == 2
        executor.execute_transaction.assert_not_called()

    def test_single_transaction_skips_bundling(self):
        mco, compiler = _make_mco()
        _wire_compile(compiler, [_tx()])
        executor = _executor(safe_mode=True)
        result = asyncio.run(mco._compile_and_execute_intent(_intent(), executor))
        assert result.tx_hash == "0x1"
        executor.execute_bundle.assert_not_called()
