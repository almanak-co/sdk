"""Tests for gateway-backed provider implementations."""

from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest

from almanak.framework.gateway_client import GatewayClient, GatewayClientConfig
from almanak.gateway.proto import gateway_pb2


class TestGatewayPriceOracle:
    """Tests for GatewayPriceOracle."""

    @pytest.fixture
    def mock_client(self):
        """Create mock gateway client."""
        client = MagicMock(spec=GatewayClient)
        client.market = MagicMock()
        return client

    def test_get_aggregated_price_success(self, mock_client):
        """get_aggregated_price returns PriceResult from gateway."""
        from almanak.framework.data.price.gateway_oracle import GatewayPriceOracle

        # Mock the gRPC response
        mock_response = gateway_pb2.PriceResponse(
            price="2500.50",
            timestamp=int(datetime.now(UTC).timestamp()),
            source="coingecko",
            confidence=0.95,
            stale=False,
        )
        mock_client.market.GetPrice.return_value = mock_response

        oracle = GatewayPriceOracle(mock_client)

        # Use sync wrapper for testing
        import asyncio

        result = asyncio.run(oracle.get_aggregated_price("ETH", "USD"))

        assert result.price == Decimal("2500.50")
        assert result.source == "coingecko"
        assert result.confidence == 0.95
        assert result.stale is False

    def test_get_aggregated_price_empty_response(self, mock_client):
        """get_aggregated_price raises on empty response."""
        from almanak.framework.data.interfaces import AllDataSourcesFailed
        from almanak.framework.data.price.gateway_oracle import GatewayPriceOracle

        mock_response = gateway_pb2.PriceResponse(price="")
        mock_client.market.GetPrice.return_value = mock_response

        oracle = GatewayPriceOracle(mock_client)

        import asyncio

        with pytest.raises(AllDataSourcesFailed):
            asyncio.run(oracle.get_aggregated_price("ETH", "USD"))


class TestGatewayBalanceProvider:
    """Tests for GatewayBalanceProvider."""

    @pytest.fixture
    def mock_client(self):
        """Create mock gateway client."""
        client = MagicMock(spec=GatewayClient)
        client.market = MagicMock()
        return client

    def test_get_balance_success(self, mock_client):
        """get_balance returns BalanceResult from gateway."""
        from almanak.framework.data.balance.gateway_provider import GatewayBalanceProvider

        mock_response = gateway_pb2.BalanceResponse(
            balance="10.5",
            balance_usd="26250.00",
            address="0x1234",
            decimals=18,
            raw_balance="10500000000000000000",
            timestamp=int(datetime.now(UTC).timestamp()),
            stale=False,
        )
        mock_client.market.GetBalance.return_value = mock_response

        provider = GatewayBalanceProvider(
            client=mock_client,
            wallet_address="0x1234",
            chain="arbitrum",
        )

        import asyncio

        result = asyncio.run(provider.get_balance("WETH"))

        assert result.balance == Decimal("10.5")
        assert result.decimals == 18
        assert result.stale is False

    def test_cache_invalidation(self, mock_client):
        """invalidate_cache clears cached balances."""
        from almanak.framework.data.balance.gateway_provider import GatewayBalanceProvider

        provider = GatewayBalanceProvider(
            client=mock_client,
            wallet_address="0x1234",
            chain="arbitrum",
        )

        # Manually add cache entry
        provider._cache["WETH"] = MagicMock()
        assert "WETH" in provider._cache

        provider.invalidate_cache("WETH")
        assert "WETH" not in provider._cache

    def test_invalidate_all_cache(self, mock_client):
        """invalidate_cache(None) clears all cache."""
        from almanak.framework.data.balance.gateway_provider import GatewayBalanceProvider

        provider = GatewayBalanceProvider(
            client=mock_client,
            wallet_address="0x1234",
            chain="arbitrum",
        )

        provider._cache["WETH"] = MagicMock()
        provider._cache["USDC"] = MagicMock()

        provider.invalidate_cache()  # None = all
        assert len(provider._cache) == 0


class TestGatewayStateManager:
    """Tests for GatewayStateManager."""

    @pytest.fixture
    def mock_client(self):
        """Create mock gateway client."""
        client = MagicMock(spec=GatewayClient)
        client.state = MagicMock()
        return client

    def test_load_state_success(self, mock_client):
        """load_state returns StateData from gateway."""
        import json

        from almanak.framework.state.gateway_state_manager import GatewayStateManager

        state_dict = {"position": "long", "entry_price": 2500}
        mock_response = gateway_pb2.StateData(
            strategy_id="test-strategy",
            version=5,
            data=json.dumps(state_dict).encode("utf-8"),
            schema_version=1,
            checksum="abc123",
            created_at=int(datetime.now(UTC).timestamp()),
            loaded_from="warm",
        )
        mock_client.state.LoadState.return_value = mock_response

        manager = GatewayStateManager(mock_client)

        import asyncio

        result = asyncio.run(manager.load_state("test-strategy"))

        assert result is not None
        assert result.strategy_id == "test-strategy"
        assert result.version == 5
        assert result.state == state_dict

    def test_load_state_not_found(self, mock_client):
        """load_state returns None when state not found."""
        from almanak.framework.state.gateway_state_manager import GatewayStateManager

        mock_response = gateway_pb2.StateData()  # Empty = not found
        mock_client.state.LoadState.return_value = mock_response

        manager = GatewayStateManager(mock_client)

        import asyncio

        result = asyncio.run(manager.load_state("nonexistent"))

        assert result is None

    def test_save_state_success(self, mock_client):
        """save_state returns updated StateData."""
        from almanak.framework.state.gateway_state_manager import GatewayStateManager
        from almanak.framework.state.state_manager import StateData

        mock_response = gateway_pb2.SaveStateResponse(
            success=True,
            new_version=6,
            checksum="new_checksum",
        )
        mock_client.state.SaveState.return_value = mock_response

        manager = GatewayStateManager(mock_client)
        state = StateData(
            strategy_id="test-strategy",
            version=5,
            state={"position": "short"},
            schema_version=1,
        )

        import asyncio

        result = asyncio.run(manager.save_state(state, expected_version=5))

        assert result.version == 6
        assert result.checksum == "new_checksum"


class TestGatewayExecutionOrchestrator:
    """Tests for GatewayExecutionOrchestrator."""

    @pytest.fixture
    def mock_client(self):
        """Create mock gateway client."""
        client = MagicMock(spec=GatewayClient)
        client.execution = MagicMock()
        return client

    def test_execute_success(self, mock_client):
        """execute returns GatewayExecutionResult from gateway."""
        import json

        from almanak.framework.execution.gateway_orchestrator import (
            GatewayExecutionOrchestrator,
        )

        mock_response = gateway_pb2.ExecutionResult(
            success=True,
            tx_hashes=["0xabc", "0xdef"],
            total_gas_used=150000,
            receipts=json.dumps([{"status": 1}]).encode("utf-8"),
            execution_id="exec-123",
        )
        mock_client.execution.Execute.return_value = mock_response

        orchestrator = GatewayExecutionOrchestrator(
            client=mock_client,
            chain="arbitrum",
            wallet_address="0x1234",
        )

        # Mock action bundle
        action_bundle = {"actions": [{"type": "swap"}]}

        import asyncio

        result = asyncio.run(
            orchestrator.execute(
                action_bundle=action_bundle,
                strategy_id="test",
                dry_run=False,
            )
        )

        assert result.success is True
        assert result.tx_hashes == ["0xabc", "0xdef"]
        assert result.total_gas_used == 150000

    def test_execute_failure(self, mock_client):
        """execute returns error result on failure."""
        from almanak.framework.execution.gateway_orchestrator import (
            GatewayExecutionOrchestrator,
        )

        mock_response = gateway_pb2.ExecutionResult(
            success=False,
            error="Insufficient funds",
            error_code="INSUFFICIENT_BALANCE",
        )
        mock_client.execution.Execute.return_value = mock_response

        orchestrator = GatewayExecutionOrchestrator(
            client=mock_client,
            chain="arbitrum",
            wallet_address="0x1234",
        )

        import asyncio

        result = asyncio.run(
            orchestrator.execute(
                action_bundle={"actions": []},
                strategy_id="test",
            )
        )

        assert result.success is False
        assert result.error == "Insufficient funds"
