"""Security tests for gateway services - injection attack prevention.

These tests verify that gateway services properly reject malicious inputs
that could be used for injection attacks (SQL, command, path traversal, etc.).
"""

import pytest

from almanak.gateway.core.settings import GatewaySettings
from almanak.gateway.proto import gateway_pb2


@pytest.fixture
def settings():
    """Create test settings."""
    return GatewaySettings()


class TestRpcServiceInjection:
    """Injection tests for RpcService."""

    @pytest.fixture
    def rpc_service(self, settings):
        """Create RpcService instance."""
        from almanak.gateway.services.rpc_service import RpcServiceServicer

        return RpcServiceServicer(settings)

    @pytest.mark.asyncio
    async def test_chain_injection_blocked(self, rpc_service):
        """Test that chain parameter injection is blocked."""
        from unittest.mock import MagicMock

        context = MagicMock()

        injection_attempts = [
            "ethereum; DROP TABLE",
            "arbitrum' OR '1'='1",
            "../../../etc/passwd",
            "base<script>alert(1)</script>",
        ]

        for chain in injection_attempts:
            request = gateway_pb2.RpcRequest(
                chain=chain,
                method="eth_blockNumber",
                params="[]",
                id="1",
            )
            response = await rpc_service.Call(request, context)
            assert not response.success, f"Chain injection should be blocked: {chain}"

    @pytest.mark.asyncio
    async def test_dangerous_rpc_methods_blocked(self, rpc_service):
        """Test that dangerous RPC methods are blocked."""
        from unittest.mock import MagicMock

        context = MagicMock()

        dangerous_methods = [
            "debug_traceTransaction",
            "admin_addPeer",
            "personal_unlockAccount",
            "miner_start",
            "eth_sign",
        ]

        for method in dangerous_methods:
            request = gateway_pb2.RpcRequest(
                chain="ethereum",
                method=method,
                params="[]",
                id="1",
            )
            response = await rpc_service.Call(request, context)
            assert not response.success, f"Dangerous method should be blocked: {method}"
            assert "not allowed" in response.error


class TestStateServiceInjection:
    """Injection tests for StateService."""

    @pytest.fixture
    def state_service(self, settings):
        """Create StateService instance."""
        from almanak.gateway.services.state_service import StateServiceServicer

        return StateServiceServicer(settings)

    @pytest.mark.asyncio
    async def test_strategy_id_path_traversal_blocked(self, state_service):
        """Test that path traversal in strategy_id is blocked."""
        from unittest.mock import MagicMock

        context = MagicMock()

        traversal_attempts = [
            "../../../etc/passwd",
            "..\\..\\windows\\system32",
            "strategy/../secret",
            "/etc/passwd",
        ]

        for strategy_id in traversal_attempts:
            request = gateway_pb2.LoadStateRequest(strategy_id=strategy_id)
            await state_service.LoadState(request, context)
            # Verify error was set
            context.set_code.assert_called()

    @pytest.mark.asyncio
    async def test_strategy_id_sql_injection_blocked(self, state_service):
        """Test that SQL injection in strategy_id is blocked."""
        from unittest.mock import MagicMock

        context = MagicMock()

        injection_attempts = [
            "strategy'; DROP TABLE strategies; --",
            "1 OR 1=1",
            "strategy UNION SELECT * FROM users",
            "'; DELETE FROM state WHERE '1'='1",
        ]

        for strategy_id in injection_attempts:
            request = gateway_pb2.LoadStateRequest(strategy_id=strategy_id)
            await state_service.LoadState(request, context)
            context.set_code.assert_called()

    @pytest.mark.asyncio
    async def test_state_size_limit_enforced(self, state_service):
        """Test that state size limit is enforced."""
        from unittest.mock import MagicMock

        from almanak.gateway.validation import MAX_STATE_SIZE_BYTES

        context = MagicMock()

        # Create oversized data
        large_data = b"x" * (MAX_STATE_SIZE_BYTES + 1)

        request = gateway_pb2.SaveStateRequest(
            strategy_id="test-strategy",
            data=large_data,
            expected_version=0,
        )
        response = await state_service.SaveState(request, context)
        assert not response.success
        assert "exceeds maximum" in response.error


class TestExecutionServiceInjection:
    """Injection tests for ExecutionService."""

    @pytest.fixture
    def execution_service(self, settings):
        """Create ExecutionService instance."""
        from almanak.gateway.services.execution_service import ExecutionServiceServicer

        return ExecutionServiceServicer(settings)

    @pytest.mark.asyncio
    async def test_wallet_address_injection_blocked(self, execution_service):
        """Test that invalid wallet addresses are blocked."""
        from unittest.mock import MagicMock

        context = MagicMock()

        invalid_addresses = [
            "not-an-address",
            "0x123",  # Too short
            "0x" + "G" * 40,  # Invalid hex
            "0x1234567890123456789012345678901234567890; DROP TABLE",
        ]

        for address in invalid_addresses:
            request = gateway_pb2.ExecuteRequest(
                chain="ethereum",
                wallet_address=address,
                action_bundle=b"{}",
            )
            response = await execution_service.Execute(request, context)
            assert not response.success, f"Invalid address should be blocked: {address}"

    @pytest.mark.asyncio
    async def test_chain_injection_blocked(self, execution_service):
        """Test that chain injection is blocked in execution service."""
        from unittest.mock import MagicMock

        context = MagicMock()

        request = gateway_pb2.ExecuteRequest(
            chain="ethereum; DROP TABLE",
            wallet_address="0x1234567890123456789012345678901234567890",
            action_bundle=b"{}",
        )
        response = await execution_service.Execute(request, context)
        assert not response.success

    @pytest.mark.asyncio
    async def test_tx_hash_validation(self, execution_service):
        """Test that invalid tx hashes are blocked."""
        from unittest.mock import MagicMock

        context = MagicMock()

        invalid_hashes = [
            "not-a-hash",
            "0x123",  # Too short
            "0x" + "G" * 64,  # Invalid hex
        ]

        for tx_hash in invalid_hashes:
            request = gateway_pb2.TxStatusRequest(
                tx_hash=tx_hash,
                chain="ethereum",
            )
            response = await execution_service.GetTransactionStatus(request, context)
            assert response.status == "invalid"


class TestIntegrationServiceInjection:
    """Injection tests for IntegrationService."""

    @pytest.fixture
    def integration_service(self, settings):
        """Create IntegrationService instance."""
        from almanak.gateway.services.integration_service import IntegrationServiceServicer

        return IntegrationServiceServicer(settings)

    @pytest.mark.asyncio
    async def test_binance_symbol_injection_blocked(self, integration_service):
        """Test that Binance symbol injection is blocked."""
        from unittest.mock import MagicMock

        context = MagicMock()

        injection_attempts = [
            "BTCUSDT; DROP TABLE",
            "BTCUSDT<script>",
            "../../../etc/passwd",
            "BTC-USDT",  # Invalid format
        ]

        for symbol in injection_attempts:
            request = gateway_pb2.BinanceTickerRequest(symbol=symbol)
            await integration_service.BinanceGetTicker(request, context)
            context.set_code.assert_called()

    @pytest.mark.asyncio
    async def test_coingecko_token_id_injection_blocked(self, integration_service):
        """Test that CoinGecko token_id injection is blocked."""
        from unittest.mock import MagicMock

        context = MagicMock()

        injection_attempts = [
            "ethereum; DROP TABLE",
            "bitcoin<script>",
            "../../../etc/passwd",
            "token_with_underscore",  # Invalid format
        ]

        for token_id in injection_attempts:
            request = gateway_pb2.CoinGeckoGetPriceRequest(
                token_id=token_id,
                vs_currencies=["usd"],
            )
            await integration_service.CoinGeckoGetPrice(request, context)
            context.set_code.assert_called()

    @pytest.mark.asyncio
    async def test_thegraph_introspection_blocked(self, integration_service):
        """Test that GraphQL introspection queries are blocked."""
        from unittest.mock import MagicMock

        context = MagicMock()

        introspection_queries = [
            "{ __schema { types { name } } }",
            '{ __type(name: "Pool") { fields { name } } }',
        ]

        for query in introspection_queries:
            request = gateway_pb2.TheGraphQueryRequest(
                subgraph_id="uniswap-v3-ethereum",
                query=query,
            )
            response = await integration_service.TheGraphQuery(request, context)
            assert not response.success
            assert "introspection" in response.errors.lower()

    @pytest.mark.asyncio
    async def test_thegraph_query_size_limit(self, integration_service):
        """Test that GraphQL query size limit is enforced."""
        from unittest.mock import MagicMock

        from almanak.gateway.validation import MAX_GRAPHQL_QUERY_LENGTH

        context = MagicMock()

        # Create oversized query
        large_query = "{ " + "a" * (MAX_GRAPHQL_QUERY_LENGTH + 1) + " }"

        request = gateway_pb2.TheGraphQueryRequest(
            subgraph_id="uniswap-v3-ethereum",
            query=large_query,
        )
        response = await integration_service.TheGraphQuery(request, context)
        assert not response.success


class TestMarketServiceInjection:
    """Injection tests for MarketService."""

    @pytest.fixture
    def market_service(self, settings):
        """Create MarketService instance."""
        from almanak.gateway.services.market_service import MarketServiceServicer

        return MarketServiceServicer(settings)

    @pytest.mark.asyncio
    async def test_chain_injection_blocked(self, market_service):
        """Test that chain injection is blocked in market service."""
        from unittest.mock import MagicMock

        context = MagicMock()

        request = gateway_pb2.BalanceRequest(
            token="ETH",
            chain="ethereum; DROP TABLE",
            wallet_address="0x1234567890123456789012345678901234567890",
        )
        await market_service.GetBalance(request, context)
        context.set_code.assert_called()

    @pytest.mark.asyncio
    async def test_wallet_address_injection_blocked(self, market_service):
        """Test that wallet address injection is blocked."""
        from unittest.mock import MagicMock

        context = MagicMock()

        request = gateway_pb2.BalanceRequest(
            token="ETH",
            chain="ethereum",
            wallet_address="invalid-address",
        )
        await market_service.GetBalance(request, context)
        context.set_code.assert_called()
