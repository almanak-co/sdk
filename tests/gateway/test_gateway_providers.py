"""Tests for gateway-backed provider implementations."""

from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

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


class TestGatewayStateManagerSnapshotFallback:
    """Tests for GatewayStateManager SQLite snapshot fallback."""

    @pytest.fixture
    def mock_client(self):
        """Create mock gateway client."""
        client = MagicMock(spec=GatewayClient)
        client.state = MagicMock()
        return client

    @pytest.fixture
    def sample_snapshot(self):
        """Create a sample PortfolioSnapshot for testing."""
        from decimal import Decimal

        from almanak.framework.portfolio.models import PortfolioSnapshot, ValueConfidence

        return PortfolioSnapshot(
            timestamp=datetime.now(UTC),
            strategy_id="test-strategy",
            total_value_usd=Decimal("1000.50"),
            available_cash_usd=Decimal("500.25"),
            value_confidence=ValueConfidence.HIGH,
            chain="arbitrum",
        )

    def test_save_portfolio_snapshot_success(self, mock_client, sample_snapshot):
        """save_portfolio_snapshot delegates to SQLite fallback and returns snapshot ID."""
        import asyncio

        from almanak.framework.state.gateway_state_manager import GatewayStateManager

        manager = GatewayStateManager(mock_client)

        # Mock the SQLiteStore that _get_sqlite_fallback creates
        mock_store = MagicMock()
        mock_store.initialize = AsyncMock()
        mock_store.save_portfolio_snapshot = AsyncMock(return_value=42)
        mock_store.close = AsyncMock()

        # Inject mock store directly
        manager._sqlite_fallback = mock_store

        result = asyncio.run(manager.save_portfolio_snapshot(sample_snapshot))

        assert result == 42
        mock_store.save_portfolio_snapshot.assert_called_once_with(sample_snapshot)

    def test_save_portfolio_snapshot_failure_returns_zero(self, mock_client, sample_snapshot):
        """save_portfolio_snapshot returns 0 on SQLite failure."""
        import asyncio

        from almanak.framework.state.gateway_state_manager import GatewayStateManager

        manager = GatewayStateManager(mock_client)

        mock_store = MagicMock()
        mock_store.save_portfolio_snapshot = AsyncMock(side_effect=RuntimeError("DB write failed"))
        manager._sqlite_fallback = mock_store

        result = asyncio.run(manager.save_portfolio_snapshot(sample_snapshot))

        assert result == 0

    def test_get_latest_snapshot_success(self, mock_client, sample_snapshot):
        """get_latest_snapshot returns snapshot from SQLite fallback."""
        import asyncio

        from almanak.framework.state.gateway_state_manager import GatewayStateManager

        manager = GatewayStateManager(mock_client)

        mock_store = MagicMock()
        mock_store.get_latest_snapshot = AsyncMock(return_value=sample_snapshot)
        manager._sqlite_fallback = mock_store

        result = asyncio.run(manager.get_latest_snapshot("test-strategy"))

        assert result is not None
        assert result.strategy_id == "test-strategy"

    def test_get_latest_snapshot_failure_returns_none(self, mock_client):
        """get_latest_snapshot returns None on SQLite failure."""
        import asyncio

        from almanak.framework.state.gateway_state_manager import GatewayStateManager

        manager = GatewayStateManager(mock_client)

        mock_store = MagicMock()
        mock_store.get_latest_snapshot = AsyncMock(side_effect=RuntimeError("DB read failed"))
        manager._sqlite_fallback = mock_store

        result = asyncio.run(manager.get_latest_snapshot("test-strategy"))

        assert result is None

    def test_get_snapshots_since_success(self, mock_client, sample_snapshot):
        """get_snapshots_since returns snapshots from SQLite fallback."""
        import asyncio

        from almanak.framework.state.gateway_state_manager import GatewayStateManager

        manager = GatewayStateManager(mock_client)

        mock_store = MagicMock()
        mock_store.get_snapshots_since = AsyncMock(return_value=[sample_snapshot])
        manager._sqlite_fallback = mock_store

        since = datetime.now(UTC)
        result = asyncio.run(manager.get_snapshots_since("test-strategy", since))

        assert len(result) == 1
        assert result[0].strategy_id == "test-strategy"

    def test_get_snapshots_since_failure_returns_empty(self, mock_client):
        """get_snapshots_since returns empty list on SQLite failure."""
        import asyncio

        from almanak.framework.state.gateway_state_manager import GatewayStateManager

        manager = GatewayStateManager(mock_client)

        mock_store = MagicMock()
        mock_store.get_snapshots_since = AsyncMock(side_effect=RuntimeError("DB failed"))
        manager._sqlite_fallback = mock_store

        since = datetime.now(UTC)
        result = asyncio.run(manager.get_snapshots_since("test-strategy", since))

        assert result == []

    def test_close_cleans_up_sqlite_fallback(self, mock_client):
        """close() properly shuts down the SQLite fallback store."""
        import asyncio

        from almanak.framework.state.gateway_state_manager import GatewayStateManager

        manager = GatewayStateManager(mock_client)

        mock_store = MagicMock()
        mock_store.close = AsyncMock()
        manager._sqlite_fallback = mock_store

        asyncio.run(manager.close())

        mock_store.close.assert_called_once()
        assert manager._sqlite_fallback is None

    def test_close_without_sqlite_fallback_is_safe(self, mock_client):
        """close() works fine when no SQLite fallback was initialized."""
        import asyncio

        from almanak.framework.state.gateway_state_manager import GatewayStateManager

        manager = GatewayStateManager(mock_client)
        assert manager._sqlite_fallback is None

        # Should not raise
        asyncio.run(manager.close())

    def test_sqlite_fallback_lazy_init_caches_store(self, mock_client, monkeypatch):
        """_get_sqlite_fallback creates store once and caches it."""
        import asyncio

        from almanak.framework.state.gateway_state_manager import GatewayStateManager

        manager = GatewayStateManager(mock_client)

        mock_store = MagicMock()
        mock_store.initialize = AsyncMock()

        mock_sqlite_config = MagicMock()
        mock_sqlite_store_cls = MagicMock(return_value=mock_store)

        monkeypatch.setattr(
            "almanak.framework.state.backends.sqlite.SQLiteStore",
            mock_sqlite_store_cls,
        )
        monkeypatch.setattr(
            "almanak.framework.state.backends.sqlite.SQLiteConfig",
            mock_sqlite_config,
        )

        # First call initializes
        store1 = asyncio.run(manager._get_sqlite_fallback())
        # Second call returns cached
        store2 = asyncio.run(manager._get_sqlite_fallback())

        assert store1 is store2
        mock_store.initialize.assert_called_once()


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
