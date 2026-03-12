"""Tests for ExecutionService Solana routing (VIB-369).

Verifies:
1. GetTransactionStatus() routes to _get_solana_tx_status() for Solana chains
2. _get_solana_tx_status() correctly maps Solana commitment levels
3. _get_solana_tx_status() handles errors and unknown signatures
4. EVM path is unchanged (regression guard)
"""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from almanak.gateway.proto import gateway_pb2


def _make_settings(**overrides):
    """Create a minimal GatewaySettings mock."""
    settings = MagicMock()
    settings.network = overrides.get("network", "mainnet")
    settings.private_key = overrides.get("private_key", "0x" + "ab" * 32)
    settings.solana_private_key = overrides.get("solana_private_key", None)
    return settings


def _make_context():
    """Create a mock gRPC ServicerContext."""
    ctx = AsyncMock()
    ctx.set_code = MagicMock()
    ctx.set_details = MagicMock()
    return ctx


# Valid Solana signature (88 base58 chars)
SOLANA_SIG = "5VERv8NMHKRYsGeYfVb9oKzvoHvU9vE3yo9Xq2Gj8j3B8VeqiZLzQQDCbPVmXNgTjEFGdYkhNmj1PYqC7GsQzXvA"
# Valid EVM tx hash
EVM_TX_HASH = "0x" + "a1" * 32


class TestGetTransactionStatusRouting:
    """GetTransactionStatus routes to correct chain-family handler."""

    @pytest.mark.asyncio
    async def test_solana_chain_routes_to_solana_handler(self):
        """Chain='solana' should call _get_solana_tx_status."""
        from almanak.gateway.services.execution_service import ExecutionServiceServicer

        service = ExecutionServiceServicer(_make_settings())
        ctx = _make_context()

        request = gateway_pb2.TxStatusRequest(tx_hash=SOLANA_SIG, chain="solana")

        with patch.object(service, "_get_solana_tx_status", new_callable=AsyncMock) as mock_sol:
            mock_sol.return_value = gateway_pb2.TxStatus(status="confirmed")

            result = await service.GetTransactionStatus(request, ctx)

            mock_sol.assert_called_once_with(SOLANA_SIG, "solana", ctx)
            assert result.status == "confirmed"

    @pytest.mark.asyncio
    async def test_evm_chain_routes_to_evm_handler(self):
        """Chain='arbitrum' should call _get_evm_tx_status."""
        from almanak.gateway.services.execution_service import ExecutionServiceServicer

        service = ExecutionServiceServicer(_make_settings())
        ctx = _make_context()

        request = gateway_pb2.TxStatusRequest(tx_hash=EVM_TX_HASH, chain="arbitrum")

        with patch.object(service, "_get_evm_tx_status", new_callable=AsyncMock) as mock_evm:
            mock_evm.return_value = gateway_pb2.TxStatus(status="confirmed")

            result = await service.GetTransactionStatus(request, ctx)

            mock_evm.assert_called_once_with(EVM_TX_HASH, "arbitrum", ctx)
            assert result.status == "confirmed"

    @pytest.mark.asyncio
    async def test_invalid_chain_rejected(self):
        """Invalid chain returns error without calling any handler."""
        from almanak.gateway.services.execution_service import ExecutionServiceServicer

        service = ExecutionServiceServicer(_make_settings())
        ctx = _make_context()

        request = gateway_pb2.TxStatusRequest(tx_hash="abc", chain="invalid_chain")

        result = await service.GetTransactionStatus(request, ctx)

        assert result.status == "invalid"
        assert "not allowed" in result.error

    @pytest.mark.asyncio
    async def test_evm_hash_rejected_for_solana(self):
        """EVM hex hash is rejected when chain='solana'."""
        from almanak.gateway.services.execution_service import ExecutionServiceServicer

        service = ExecutionServiceServicer(_make_settings())
        ctx = _make_context()

        request = gateway_pb2.TxStatusRequest(tx_hash=EVM_TX_HASH, chain="solana")

        result = await service.GetTransactionStatus(request, ctx)

        assert result.status == "invalid"
        assert "Solana signature" in result.error


class TestSolanaTxStatusMapping:
    """_get_solana_tx_status() correctly maps commitment levels."""

    @pytest.mark.asyncio
    async def test_finalized_maps_to_confirmed_32(self):
        """Finalized = confirmed with 32 confirmations."""
        from almanak.gateway.services.execution_service import ExecutionServiceServicer

        service = ExecutionServiceServicer(_make_settings())
        ctx = _make_context()

        mock_statuses = [{"confirmationStatus": "finalized", "slot": 12345, "err": None}]

        with (
            patch(
                "almanak.framework.execution.solana.rpc.SolanaRpcClient",
                autospec=True,
            ) as MockClient,
            patch("almanak.gateway.utils.rpc_provider.get_rpc_url", return_value="http://localhost:8899"),
        ):
            instance = MockClient.return_value
            instance.get_signature_statuses = AsyncMock(return_value=mock_statuses)

            result = await service._get_solana_tx_status(SOLANA_SIG, "solana", ctx)

        assert result.status == "confirmed"
        assert result.confirmations == 32
        assert result.block_number == 12345

    @pytest.mark.asyncio
    async def test_confirmed_maps_to_confirmed_1(self):
        """Confirmed = confirmed with 1 confirmation."""
        from almanak.gateway.services.execution_service import ExecutionServiceServicer

        service = ExecutionServiceServicer(_make_settings())
        ctx = _make_context()

        mock_statuses = [{"confirmationStatus": "confirmed", "slot": 99999, "err": None}]

        with (
            patch(
                "almanak.framework.execution.solana.rpc.SolanaRpcClient",
                autospec=True,
            ) as MockClient,
            patch("almanak.gateway.utils.rpc_provider.get_rpc_url", return_value="http://localhost:8899"),
        ):
            instance = MockClient.return_value
            instance.get_signature_statuses = AsyncMock(return_value=mock_statuses)

            result = await service._get_solana_tx_status(SOLANA_SIG, "solana", ctx)

        assert result.status == "confirmed"
        assert result.confirmations == 1
        assert result.block_number == 99999

    @pytest.mark.asyncio
    async def test_processed_maps_to_pending(self):
        """Processed = pending (not yet confirmed)."""
        from almanak.gateway.services.execution_service import ExecutionServiceServicer

        service = ExecutionServiceServicer(_make_settings())
        ctx = _make_context()

        mock_statuses = [{"confirmationStatus": "processed", "slot": 11111, "err": None}]

        with (
            patch(
                "almanak.framework.execution.solana.rpc.SolanaRpcClient",
                autospec=True,
            ) as MockClient,
            patch("almanak.gateway.utils.rpc_provider.get_rpc_url", return_value="http://localhost:8899"),
        ):
            instance = MockClient.return_value
            instance.get_signature_statuses = AsyncMock(return_value=mock_statuses)

            result = await service._get_solana_tx_status(SOLANA_SIG, "solana", ctx)

        assert result.status == "pending"
        assert result.block_number == 11111

    @pytest.mark.asyncio
    async def test_error_maps_to_reverted(self):
        """Transaction with err != None maps to 'reverted'."""
        from almanak.gateway.services.execution_service import ExecutionServiceServicer

        service = ExecutionServiceServicer(_make_settings())
        ctx = _make_context()

        mock_statuses = [
            {
                "confirmationStatus": "finalized",
                "slot": 55555,
                "err": {"InstructionError": [0, "Custom(1)"]},
            }
        ]

        with (
            patch(
                "almanak.framework.execution.solana.rpc.SolanaRpcClient",
                autospec=True,
            ) as MockClient,
            patch("almanak.gateway.utils.rpc_provider.get_rpc_url", return_value="http://localhost:8899"),
        ):
            instance = MockClient.return_value
            instance.get_signature_statuses = AsyncMock(return_value=mock_statuses)

            result = await service._get_solana_tx_status(SOLANA_SIG, "solana", ctx)

        assert result.status == "reverted"
        assert "Transaction failed" in result.error
        assert result.block_number == 55555

    @pytest.mark.asyncio
    async def test_unknown_signature_returns_pending(self):
        """Unknown signature (None status) returns pending."""
        from almanak.gateway.services.execution_service import ExecutionServiceServicer

        service = ExecutionServiceServicer(_make_settings())
        ctx = _make_context()

        with (
            patch(
                "almanak.framework.execution.solana.rpc.SolanaRpcClient",
                autospec=True,
            ) as MockClient,
            patch("almanak.gateway.utils.rpc_provider.get_rpc_url", return_value="http://localhost:8899"),
        ):
            instance = MockClient.return_value
            instance.get_signature_statuses = AsyncMock(return_value=[None])

            result = await service._get_solana_tx_status(SOLANA_SIG, "solana", ctx)

        assert result.status == "pending"

    @pytest.mark.asyncio
    async def test_rpc_exception_returns_unknown(self):
        """RPC exception returns 'unknown' with error message."""
        from almanak.gateway.services.execution_service import ExecutionServiceServicer

        service = ExecutionServiceServicer(_make_settings())
        ctx = _make_context()

        with (
            patch(
                "almanak.framework.execution.solana.rpc.SolanaRpcClient",
                autospec=True,
            ) as MockClient,
            patch("almanak.gateway.utils.rpc_provider.get_rpc_url", return_value="http://localhost:8899"),
        ):
            instance = MockClient.return_value
            instance.get_signature_statuses = AsyncMock(side_effect=ConnectionError("RPC timeout"))

            result = await service._get_solana_tx_status(SOLANA_SIG, "solana", ctx)

        assert result.status == "unknown"
        assert "RPC timeout" in result.error
