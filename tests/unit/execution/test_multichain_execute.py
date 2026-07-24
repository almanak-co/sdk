"""Branch coverage for MultiChainOrchestrator.execute (single-intent routing).

Covers both modes with the compile/execute internals faked (no chain, no
gateway): gateway routing (build_tx_func ignored), config-mode build_tx_func
vs compile paths, success/failure result mapping, the InvalidChainError
short-circuit, and the catch-all exception path.

Construction seams follow test_multichain_execute_sequence.py (MagicMock
config) and test_multichain_check_chain_health.py (from_gateway with a
mock client).
"""

import asyncio
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock

import pytest

from almanak.framework.execution.chain_executor import TransactionExecutionResult
from almanak.framework.execution.multichain import ExecutionStatus, MultiChainOrchestrator
from almanak.framework.intents import Intent

WALLET = "0x1234567890abcdef1234567890abcdef12345678"


def _swap(chain="base"):
    return Intent.swap("USDC", "WETH", amount=Decimal("100"), chain=chain)


@pytest.fixture
def orchestrator(monkeypatch):
    config = MagicMock()
    config.chains = ["base"]
    config.primary_chain = "base"
    orch = MultiChainOrchestrator(config=config)
    # _get_executor builds a real ChainExecutor (signer + RPC) — stub it out.
    monkeypatch.setattr(orch, "_get_executor", lambda chain: MagicMock())
    return orch


class TestConfigMode:
    def test_compile_path_success(self, orchestrator, monkeypatch):
        tx_result = TransactionExecutionResult(success=True, tx_hash="0x" + "ab" * 32)
        monkeypatch.setattr(
            orchestrator,
            "_compile_and_execute_intent",
            AsyncMock(return_value=tx_result),
        )

        result = asyncio.run(orchestrator.execute(_swap()))

        assert result.status == ExecutionStatus.SUCCESS
        assert result.success
        assert result.chain == "base"
        assert result.tx_result is tx_result
        assert result.error is None
        assert result.execution_time_ms >= 0

    def test_compile_path_success_with_empty_tx_hash(self, orchestrator, monkeypatch):
        """No-op compilations return an empty tx_hash; success mapping must not slice it."""
        tx_result = TransactionExecutionResult(success=True, tx_hash="")
        monkeypatch.setattr(
            orchestrator,
            "_compile_and_execute_intent",
            AsyncMock(return_value=tx_result),
        )

        result = asyncio.run(orchestrator.execute(_swap()))

        assert result.status == ExecutionStatus.SUCCESS

    def test_build_tx_func_path_uses_executor_directly(self, orchestrator, monkeypatch):
        executor = MagicMock()
        tx_result = TransactionExecutionResult(success=True, tx_hash="0xdead")
        executor.execute_transaction = AsyncMock(return_value=tx_result)
        monkeypatch.setattr(orchestrator, "_get_executor", lambda chain: executor)
        compile_mock = AsyncMock()
        monkeypatch.setattr(orchestrator, "_compile_and_execute_intent", compile_mock)

        unsigned = MagicMock()
        build_calls = []

        async def build_tx_func(intent, ex):
            build_calls.append((intent, ex))
            return unsigned

        result = asyncio.run(orchestrator.execute(_swap(), build_tx_func=build_tx_func))

        assert result.status == ExecutionStatus.SUCCESS
        assert build_calls and build_calls[0][1] is executor
        executor.execute_transaction.assert_awaited_once_with(unsigned)
        compile_mock.assert_not_awaited()

    def test_failed_tx_result_maps_to_failed_status(self, orchestrator, monkeypatch):
        tx_result = TransactionExecutionResult(success=False, tx_hash="0xdead", error="reverted: slippage")
        monkeypatch.setattr(
            orchestrator,
            "_compile_and_execute_intent",
            AsyncMock(return_value=tx_result),
        )

        result = asyncio.run(orchestrator.execute(_swap()))

        assert result.status == ExecutionStatus.FAILED
        assert not result.success
        assert result.error == "reverted: slippage"
        assert result.tx_result is tx_result

    def test_failed_tx_result_without_error_uses_unknown(self, orchestrator, monkeypatch):
        tx_result = TransactionExecutionResult(success=False, tx_hash="0xdead", error=None)
        monkeypatch.setattr(
            orchestrator,
            "_compile_and_execute_intent",
            AsyncMock(return_value=tx_result),
        )

        result = asyncio.run(orchestrator.execute(_swap()))

        assert result.status == ExecutionStatus.FAILED
        assert result.error == "Unknown error"

    def test_unconfigured_chain_returns_failed_result(self, orchestrator):
        """InvalidChainError from chain resolution never raises; it maps to FAILED."""
        result = asyncio.run(orchestrator.execute(_swap(chain="solana")))

        assert result.status == ExecutionStatus.FAILED
        assert result.chain == "solana"
        assert "not configured" in result.error

    def test_unexpected_exception_returns_failed_result(self, orchestrator, monkeypatch):
        monkeypatch.setattr(
            orchestrator,
            "_compile_and_execute_intent",
            AsyncMock(side_effect=RuntimeError("compiler exploded")),
        )

        result = asyncio.run(orchestrator.execute(_swap()))

        assert result.status == ExecutionStatus.FAILED
        assert result.chain == "base"
        assert result.error == "Unexpected error: compiler exploded"

    def test_unexpected_exception_without_intent_chain_uses_primary(self, orchestrator, monkeypatch):
        monkeypatch.setattr(orchestrator, "_resolve_chain", MagicMock(side_effect=RuntimeError("boom")))

        intent = _swap()
        object.__setattr__(intent, "chain", None)
        result = asyncio.run(orchestrator.execute(intent))

        assert result.status == ExecutionStatus.FAILED
        assert result.chain == "base"


class TestGatewayMode:
    @pytest.fixture
    def orchestrator(self):
        return MultiChainOrchestrator.from_gateway(
            gateway_client=MagicMock(),
            chains=["base"],
            wallet_address=WALLET,
        )

    def test_gateway_path_ignores_build_tx_func(self, orchestrator, monkeypatch):
        gw_result = MagicMock(success=True, tx_hash="0x" + "cd" * 32)
        compile_execute = AsyncMock(return_value=gw_result)
        monkeypatch.setattr(orchestrator, "_gateway_compile_and_execute", compile_execute)

        build_tx_func = AsyncMock()
        intent = _swap()
        result = asyncio.run(
            orchestrator.execute(intent, build_tx_func=build_tx_func, price_map={"WETH": "3000"})
        )

        assert result.status == ExecutionStatus.SUCCESS
        assert result.tx_result is gw_result
        compile_execute.assert_awaited_once_with(intent, "base", price_map={"WETH": "3000"})
        build_tx_func.assert_not_awaited()

    def test_gateway_failure_maps_error(self, orchestrator, monkeypatch):
        gw_result = MagicMock(success=False, error="gateway rejected bundle")
        monkeypatch.setattr(
            orchestrator,
            "_gateway_compile_and_execute",
            AsyncMock(return_value=gw_result),
        )

        result = asyncio.run(orchestrator.execute(_swap()))

        assert result.status == ExecutionStatus.FAILED
        assert result.error == "gateway rejected bundle"
