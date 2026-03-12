"""Tests for ChainExecutionStrategy ABC, EvmExecutionStrategy, and SolanaExecutionPlanner.

Verifies:
  - ChainExecutionStrategy cannot be instantiated directly
  - EvmExecutionStrategy delegates to underlying orchestrator
  - SolanaExecutionPlanner raises NotImplementedError for all methods
"""

from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock

import pytest

from almanak.framework.execution.chain_strategy import ChainExecutionStrategy
from almanak.framework.execution.evm_strategy import EvmExecutionStrategy
from almanak.framework.execution.outcome import ExecutionOutcome
from almanak.framework.execution.solana.planner import SolanaExecutionPlanner
from almanak.framework.execution.solana.types import SolanaTransaction


class TestChainExecutionStrategyABC:
    def test_cannot_instantiate_abc(self):
        with pytest.raises(TypeError, match="abstract"):
            ChainExecutionStrategy()

    def test_concrete_subclass_works(self):
        class DummyStrategy(ChainExecutionStrategy):
            chain_family = "TEST"
            wallet_address = "0xtest"

            async def execute_actions(self, actions, context=None):
                return ExecutionOutcome(success=True, chain_family="TEST")

            async def check_connection(self):
                return True

        s = DummyStrategy()
        assert s.chain_family == "TEST"
        assert s.wallet_address == "0xtest"


class TestEvmExecutionStrategy:
    def test_chain_family_is_evm(self):
        mock_orch = MagicMock()
        mock_orch.wallet_address = "0xabc"
        strategy = EvmExecutionStrategy(orchestrator=mock_orch)
        assert strategy.chain_family == "EVM"
        assert strategy.wallet_address == "0xabc"

    def test_wallet_address_from_constructor(self):
        strategy = EvmExecutionStrategy(orchestrator=MagicMock(), wallet_address="0x123")
        assert strategy.wallet_address == "0x123"

    @pytest.mark.asyncio
    async def test_execute_actions_delegates_to_orchestrator(self):
        mock_result = MagicMock()
        mock_result.to_outcome.return_value = ExecutionOutcome(
            success=True,
            tx_ids=["0xaaa"],
            chain_family="EVM",
        )

        mock_orch = AsyncMock()
        mock_orch.execute = AsyncMock(return_value=mock_result)
        mock_orch.wallet_address = "0xabc"

        strategy = EvmExecutionStrategy(orchestrator=mock_orch)
        bundle = {"actions": []}
        outcome = await strategy.execute_actions([bundle], {"strategy_id": "test", "dry_run": True})

        assert outcome.success is True
        assert outcome.tx_ids == ["0xaaa"]
        mock_orch.execute.assert_awaited_once_with(
            action_bundle=bundle,
            strategy_id="test",
            intent_id="",
            dry_run=True,
            simulation_enabled=True,
        )

    @pytest.mark.asyncio
    async def test_execute_actions_empty(self):
        strategy = EvmExecutionStrategy(orchestrator=MagicMock(), wallet_address="0x1")
        outcome = await strategy.execute_actions([])
        assert outcome.success is True
        assert outcome.tx_ids == []

    @pytest.mark.asyncio
    async def test_execute_actions_fallback_without_to_outcome(self):
        mock_result = MagicMock(spec=[])
        mock_result.success = True
        mock_result.tx_hashes = ["0xbbb"]
        mock_result.receipts = [{"status": 1}]
        mock_result.total_gas_used = 21000
        mock_result.error = None
        mock_result.position_id = 42
        mock_result.swap_amounts = None
        mock_result.lp_close_data = None
        mock_result.extracted_data = {"tick_lower": -100}
        mock_result.extraction_warnings = []

        mock_orch = AsyncMock()
        mock_orch.execute = AsyncMock(return_value=mock_result)
        mock_orch.wallet_address = "0xabc"

        strategy = EvmExecutionStrategy(orchestrator=mock_orch)
        outcome = await strategy.execute_actions([{"bundle": True}])

        assert outcome.success is True
        assert outcome.tx_ids == ["0xbbb"]
        assert outcome.position_id == 42
        assert outcome.total_fee_native == Decimal(21000)

    @pytest.mark.asyncio
    async def test_check_connection_delegates(self):
        mock_orch = AsyncMock()
        mock_orch.check_connection = AsyncMock(return_value=True)
        mock_orch.wallet_address = "0xabc"

        strategy = EvmExecutionStrategy(orchestrator=mock_orch)
        assert await strategy.check_connection() is True

    @pytest.mark.asyncio
    async def test_check_connection_returns_true_without_method(self):
        mock_orch = MagicMock(spec=[])
        strategy = EvmExecutionStrategy(orchestrator=mock_orch, wallet_address="0x1")
        assert await strategy.check_connection() is True


class TestSolanaExecutionPlanner:
    def test_construction(self):
        planner = SolanaExecutionPlanner(
            wallet_address="SoLWallet1111111111111111111111111111111111",
            rpc_url="https://api.mainnet-beta.solana.com",
            commitment="finalized",
            priority_fee_ceiling_lamports=5_000_000,
        )
        assert planner.chain_family == "SOLANA"
        assert planner.wallet_address == "SoLWallet1111111111111111111111111111111111"
        assert planner.rpc_url == "https://api.mainnet-beta.solana.com"
        assert planner.commitment == "finalized"
        assert planner.priority_fee_ceiling_lamports == 5_000_000

    def test_default_values(self):
        planner = SolanaExecutionPlanner(wallet_address="abc")
        assert planner.rpc_url == ""
        assert planner.commitment == "confirmed"
        assert planner.priority_fee_ceiling_lamports == 10_000_000
        assert planner.cu_buffer_multiplier == 1.2

    @pytest.mark.asyncio
    async def test_execute_actions_no_rpc(self):
        """execute_actions returns error when no RPC URL configured."""
        planner = SolanaExecutionPlanner(wallet_address="abc")
        outcome = await planner.execute_actions([])
        assert outcome.success is False
        assert "no RPC URL" in outcome.error

    @pytest.mark.asyncio
    async def test_check_connection_no_rpc(self):
        """check_connection returns False when no RPC URL configured."""
        planner = SolanaExecutionPlanner(wallet_address="abc")
        result = await planner.check_connection()
        assert result is False

    @pytest.mark.asyncio
    async def test_ensure_atas_raises(self):
        planner = SolanaExecutionPlanner(wallet_address="abc")
        with pytest.raises(NotImplementedError, match="ensure_atas"):
            await planner.ensure_atas(mints=["mint1"])

    @pytest.mark.asyncio
    async def test_simulate_cu_raises(self):
        planner = SolanaExecutionPlanner(wallet_address="abc")
        tx = SolanaTransaction()
        with pytest.raises(NotImplementedError, match="simulate_cu"):
            await planner.simulate_cu(tx)

    @pytest.mark.asyncio
    async def test_resolve_luts_raises(self):
        planner = SolanaExecutionPlanner(wallet_address="abc")
        with pytest.raises(NotImplementedError, match="resolve_luts"):
            await planner.resolve_luts(["lut1"])

    @pytest.mark.asyncio
    async def test_fetch_blockhash_no_rpc(self):
        """fetch_blockhash raises RuntimeError when no RPC configured."""
        planner = SolanaExecutionPlanner(wallet_address="abc")
        with pytest.raises(RuntimeError, match="No RPC client"):
            await planner.fetch_blockhash()

    @pytest.mark.asyncio
    async def test_sign_transaction_raises(self):
        planner = SolanaExecutionPlanner(wallet_address="abc")
        tx = SolanaTransaction()
        with pytest.raises(NotImplementedError, match="sign_transaction"):
            await planner.sign_transaction(tx)

    @pytest.mark.asyncio
    async def test_submit_and_confirm_raises(self):
        planner = SolanaExecutionPlanner(wallet_address="abc")
        from almanak.framework.execution.solana.types import SignedSolanaTransaction

        signed = SignedSolanaTransaction(raw_tx=b"", signature="sig", unsigned_tx=SolanaTransaction())
        with pytest.raises(NotImplementedError, match="submit_and_confirm"):
            await planner.submit_and_confirm(signed)

    @pytest.mark.asyncio
    async def test_error_messages_reference_remaining_stubs(self):
        """Ensure remaining stubs still raise NotImplementedError with clear messages."""
        planner = SolanaExecutionPlanner(wallet_address="abc")
        with pytest.raises(NotImplementedError, match="ensure_atas"):
            await planner.ensure_atas(mints=["mint1"])
