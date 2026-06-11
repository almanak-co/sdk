"""Tests for gateway-backed provider implementations."""

from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock

import pytest

from almanak.framework.gateway_client import GatewayClient
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

    def test_get_aggregated_price_forwards_chain_context(self, mock_client):
        """Explicit/default chain context must be forwarded to PriceRequest."""
        from almanak.framework.data.price.gateway_oracle import GatewayPriceOracle

        mock_response = gateway_pb2.PriceResponse(
            price="2500.50",
            timestamp=int(datetime.now(UTC).timestamp()),
            source="coingecko",
            confidence=0.95,
            stale=False,
        )
        mock_client.market.GetPrice.return_value = mock_response

        oracle = GatewayPriceOracle(mock_client, default_chain="base")

        import asyncio

        asyncio.run(oracle.get_aggregated_price("0xabc", "USD"))
        first_request = mock_client.market.GetPrice.call_args_list[0].args[0]
        assert first_request.chain == "base"

        asyncio.run(oracle.get_aggregated_price("0xabc", "USD", chain="arbitrum"))
        second_request = mock_client.market.GetPrice.call_args_list[1].args[0]
        assert second_request.chain == "arbitrum"

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
        request = mock_client.market.GetBalance.call_args.args[0]
        assert request.force_refresh is False

    def test_get_balance_threads_force_refresh(self, mock_client):
        """force_refresh is sent to the gateway for read-after-write checks."""
        from almanak.framework.data.balance.gateway_provider import GatewayBalanceProvider

        mock_client.market.GetBalance.return_value = gateway_pb2.BalanceResponse(
            balance="10.5",
            decimals=18,
            raw_balance="10500000000000000000",
            timestamp=int(datetime.now(UTC).timestamp()),
        )
        provider = GatewayBalanceProvider(
            client=mock_client,
            wallet_address="0x1234",
            chain="arbitrum",
        )

        import asyncio

        asyncio.run(provider.get_balance("WETH", force_refresh=True))

        request = mock_client.market.GetBalance.call_args.args[0]
        assert request.force_refresh is True

    def test_pinned_read_threads_block_tag_and_accepts_echoed_block(self, mock_client):
        """VIB-3350: as_of_block sets block_tag; a response echoing that block is accepted."""
        from almanak.framework.data.balance.gateway_provider import GatewayBalanceProvider

        mock_client.market.GetBalance.return_value = gateway_pb2.BalanceResponse(
            balance="5.0",
            decimals=18,
            raw_balance="5000000000000000000",
            timestamp=int(datetime.now(UTC).timestamp()),
            block_number=21_000_000,
        )
        provider = GatewayBalanceProvider(client=mock_client, wallet_address="0x1234", chain="arbitrum")
        import asyncio

        result = asyncio.run(provider.get_balance("WETH", as_of_block=21_000_000))
        assert result.balance == Decimal("5.0")
        request = mock_client.market.GetBalance.call_args.args[0]
        assert request.block_tag == 21_000_000

    def test_pinned_read_rejects_unhonored_zero_block(self, mock_client):
        """VIB-3350 (Codex follow-up): block_number=0 on a pinned read means the
        gateway did NOT honor block_tag (ignored it / legacy). The SDK + gateway
        ship together, so 0 is NOT proof of a pin — reject it as a failed pinned
        read so the runner degrades, rather than accept an unverified 'latest'."""
        from almanak.framework.data.balance.gateway_provider import GatewayBalanceProvider
        from almanak.framework.data.interfaces import DataSourceUnavailable

        mock_client.market.GetBalance.return_value = gateway_pb2.BalanceResponse(
            balance="5.0", decimals=18, raw_balance="5000000000000000000", block_number=0
        )
        provider = GatewayBalanceProvider(client=mock_client, wallet_address="0x1234", chain="arbitrum")
        import asyncio

        with pytest.raises(DataSourceUnavailable):
            asyncio.run(provider.get_balance("WETH", as_of_block=21_000_000))

    def test_pinned_read_rejects_wrong_echoed_block(self, mock_client):
        """VIB-3350 (audit I2): if the gateway serves a DIFFERENT block than
        requested on a pinned read, reject it (no stale fallback) rather than
        accept a wrong-block balance as if it were pinned."""
        from almanak.framework.data.balance.gateway_provider import GatewayBalanceProvider
        from almanak.framework.data.interfaces import DataSourceUnavailable

        mock_client.market.GetBalance.return_value = gateway_pb2.BalanceResponse(
            balance="999.0", decimals=18, raw_balance="999000000000000000000", block_number=20_999_000
        )
        provider = GatewayBalanceProvider(client=mock_client, wallet_address="0x1234", chain="arbitrum")
        import asyncio

        with pytest.raises(DataSourceUnavailable):
            asyncio.run(provider.get_balance("WETH", as_of_block=21_000_000))

    def test_get_balance_retries_then_returns_stale_cache(self, mock_client):
        """All retries exhausted on retryable errors falls back to stale cache."""
        from almanak.framework.data.balance.gateway_provider import GatewayBalanceProvider

        # Seed cache with a prior successful read.
        mock_client.market.GetBalance.return_value = gateway_pb2.BalanceResponse(
            balance="42.0",
            decimals=18,
            raw_balance="42000000000000000000",
            timestamp=int(datetime.now(UTC).timestamp()),
        )
        provider = GatewayBalanceProvider(
            client=mock_client,
            wallet_address="0x1234",
            chain="arbitrum",
        )
        import asyncio
        from unittest.mock import patch

        seeded = asyncio.run(provider.get_balance("WETH"))
        assert seeded.balance == Decimal("42.0")
        assert seeded.stale is False

        # Now make every call raise a retryable error and skip real backoff.
        mock_client.market.GetBalance.side_effect = RuntimeError("UNAVAILABLE: peer reset")
        with patch("almanak.framework.data.balance.gateway_provider.asyncio.sleep", new=AsyncMock(return_value=None)):
            result = asyncio.run(provider.get_balance("WETH"))

        assert result.balance == Decimal("42.0")
        assert result.stale is True
        # 1 seeding call + 3 retry attempts on the failure path.
        assert mock_client.market.GetBalance.call_count == 4

    def test_get_balance_non_retryable_no_cache_raises_underlying(self, mock_client):
        """Non-retryable error with no cache propagates the original exception."""
        from almanak.framework.data.balance.gateway_provider import GatewayBalanceProvider

        class BadRequest(Exception):
            pass

        mock_client.market.GetBalance.side_effect = BadRequest("INVALID_ARGUMENT: bad symbol")
        provider = GatewayBalanceProvider(
            client=mock_client,
            wallet_address="0x1234",
            chain="arbitrum",
        )
        import asyncio

        with pytest.raises(BadRequest, match="INVALID_ARGUMENT"):
            asyncio.run(provider.get_balance("BOGUS"))
        # Non-retryable: should NOT have retried.
        assert mock_client.market.GetBalance.call_count == 1

    def test_get_balance_retryable_no_cache_raises_data_source_unavailable(self, mock_client):
        """Retries exhausted with no cache surfaces DataSourceUnavailable."""
        from almanak.framework.data.balance.gateway_provider import GatewayBalanceProvider
        from almanak.framework.data.interfaces import DataSourceUnavailable

        mock_client.market.GetBalance.side_effect = RuntimeError("DEADLINE_EXCEEDED")
        provider = GatewayBalanceProvider(
            client=mock_client,
            wallet_address="0x1234",
            chain="arbitrum",
        )
        import asyncio
        from unittest.mock import patch

        with patch("almanak.framework.data.balance.gateway_provider.asyncio.sleep", new=AsyncMock(return_value=None)):
            with pytest.raises(DataSourceUnavailable):
                asyncio.run(provider.get_balance("WETH"))
        assert mock_client.market.GetBalance.call_count == 3

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
            deployment_id="test-strategy",
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
        assert result.deployment_id == "test-strategy"
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
            deployment_id="test-strategy",
            version=5,
            state={"position": "short"},
            schema_version=1,
        )

        import asyncio

        result = asyncio.run(manager.save_state(state, expected_version=5))

        assert result.version == 6
        assert result.checksum == "new_checksum"


class TestGatewayStateManagerSnapshots:
    """Tests for GatewayStateManager portfolio snapshot methods via gateway gRPC."""

    @pytest.fixture
    def mock_client(self):
        """Create mock gateway client."""
        client = MagicMock(spec=GatewayClient)
        client.state = MagicMock()
        return client

    @pytest.fixture
    def sample_snapshot(self):
        """Create a sample PortfolioSnapshot for testing."""
        from almanak.framework.portfolio.models import PortfolioSnapshot, ValueConfidence

        return PortfolioSnapshot(
            timestamp=datetime.now(UTC),
            deployment_id="test-strategy",
            total_value_usd=Decimal("1000.50"),
            available_cash_usd=Decimal("500.25"),
            value_confidence=ValueConfidence.HIGH,
            chain="arbitrum",
        )

    def test_save_portfolio_snapshot_success(self, mock_client, sample_snapshot):
        """save_portfolio_snapshot calls gRPC and returns snapshot ID."""
        import asyncio

        from almanak.framework.state.gateway_state_manager import GatewayStateManager

        manager = GatewayStateManager(mock_client)

        mock_response = gateway_pb2.SaveSnapshotResponse(success=True, snapshot_id=42)
        mock_client.state.SavePortfolioSnapshot.return_value = mock_response

        result = asyncio.run(manager.save_portfolio_snapshot(sample_snapshot))

        assert result == 42
        mock_client.state.SavePortfolioSnapshot.assert_called_once()

    def test_save_portfolio_snapshot_serializes_metadata_envelope(self, mock_client, sample_snapshot):
        """Snapshot metadata is persisted via the backward-compatible envelope with positions."""
        import asyncio
        import json

        from almanak.framework.portfolio.models import PositionValue
        from almanak.framework.state.gateway_state_manager import GatewayStateManager
        from almanak.framework.teardown.models import PositionType

        sample_snapshot.positions = [
            PositionValue(
                position_type=PositionType.LP,
                protocol="traderjoe_v2",
                chain="avalanche",
                value_usd=Decimal("4.70"),
                label="WAVAX/USDT",
                tokens=["WAVAX", "USDT"],
                details={"pool_address": "0xpool"},
            )
        ]
        sample_snapshot.snapshot_metadata = {
            "valuation_source": "reconciled_external",
            "external_total_value_usd": "4.70",
        }
        manager = GatewayStateManager(mock_client)

        mock_response = gateway_pb2.SaveSnapshotResponse(success=True, snapshot_id=42)
        mock_client.state.SavePortfolioSnapshot.return_value = mock_response

        result = asyncio.run(manager.save_portfolio_snapshot(sample_snapshot))

        assert result == 42
        request = mock_client.state.SavePortfolioSnapshot.call_args.args[0]
        payload = json.loads(request.positions_json.decode("utf-8"))
        assert len(payload["positions"]) == 1
        assert payload["positions"][0]["protocol"] == "traderjoe_v2"
        assert payload["metadata"]["valuation_source"] == "reconciled_external"

    def test_save_portfolio_snapshot_failure_raises(self, mock_client, sample_snapshot):
        """save_portfolio_snapshot raises AccountingPersistenceError on gRPC failure.

        VIB-3157: the legacy ``return 0`` swallow-on-failure contract was a
        silent accounting-loss footgun. Failures now propagate so the runner
        can halt the cycle and alert the operator. Also asserts the typed
        ``write_kind`` / ``deployment_id`` metadata so a regression to a
        different accounting-error shape would be caught.
        """
        import asyncio

        from almanak.framework.state.exceptions import AccountingPersistenceError
        from almanak.framework.state.gateway_state_manager import GatewayStateManager

        manager = GatewayStateManager(mock_client)

        mock_response = gateway_pb2.SaveSnapshotResponse(success=False, error="internal error")
        mock_client.state.SavePortfolioSnapshot.return_value = mock_response

        with pytest.raises(AccountingPersistenceError) as excinfo:
            asyncio.run(manager.save_portfolio_snapshot(sample_snapshot))

        assert "internal error" in str(excinfo.value)
        assert excinfo.value.write_kind == "snapshot"
        assert excinfo.value.deployment_id == sample_snapshot.deployment_id

    def test_save_portfolio_snapshot_exception_raises(self, mock_client, sample_snapshot):
        """save_portfolio_snapshot raises AccountingPersistenceError on gRPC exception."""
        import asyncio

        from almanak.framework.state.exceptions import AccountingPersistenceError
        from almanak.framework.state.gateway_state_manager import GatewayStateManager

        manager = GatewayStateManager(mock_client)
        mock_client.state.SavePortfolioSnapshot.side_effect = RuntimeError("gRPC failed")

        with pytest.raises(AccountingPersistenceError) as excinfo:
            asyncio.run(manager.save_portfolio_snapshot(sample_snapshot))

        # Public ``cause`` attribute, not ``__cause__`` dunder — see
        # test_portfolio_metrics_rpc for rationale. Asserting write_kind AND
        # deployment_id locks the runner's accounting-failure dispatch contract.
        assert "gRPC failed" in str(excinfo.value) or excinfo.value.cause is not None
        assert excinfo.value.write_kind == "snapshot"
        assert excinfo.value.deployment_id == sample_snapshot.deployment_id

    def test_get_latest_snapshot_success(self, mock_client):
        """get_latest_snapshot returns snapshot from gRPC response."""
        import asyncio

        from almanak.framework.state.gateway_state_manager import GatewayStateManager

        manager = GatewayStateManager(mock_client)

        ts = int(datetime.now(UTC).timestamp())
        mock_response = gateway_pb2.SnapshotData(
            deployment_id="test-strategy",
            timestamp=ts,
            total_value_usd="1000.50",
            available_cash_usd="500.25",
            value_confidence="HIGH",
            positions_json=b"[]",
            chain="arbitrum",
            found=True,
        )
        mock_client.state.GetLatestSnapshot.return_value = mock_response

        result = asyncio.run(manager.get_latest_snapshot("test-strategy"))

        assert result is not None
        assert result.deployment_id == "test-strategy"

    def test_get_latest_snapshot_supports_metadata_envelope(self, mock_client):
        """get_latest_snapshot reads both positions and snapshot metadata from envelope rows."""
        import asyncio
        import json

        from almanak.framework.state.gateway_state_manager import GatewayStateManager

        manager = GatewayStateManager(mock_client)
        ts = int(datetime.now(UTC).timestamp())
        mock_response = gateway_pb2.SnapshotData(
            deployment_id="test-strategy",
            timestamp=ts,
            total_value_usd="4.70",
            available_cash_usd="0",
            value_confidence="ESTIMATED",
            positions_json=json.dumps(
                {
                    "positions": [
                        {
                            "position_type": "LP",
                            "protocol": "traderjoe_v2",
                            "chain": "avalanche",
                            "value_usd": "4.70",
                            "label": "WAVAX/USDT",
                            "tokens": ["WAVAX", "USDT"],
                            "details": {"pool_address": "0xpool"},
                        }
                    ],
                    "metadata": {
                        "valuation_source": "reconciled_external",
                        "external_total_value_usd": "4.70",
                    },
                }
            ).encode("utf-8"),
            chain="avalanche",
            found=True,
        )
        mock_client.state.GetLatestSnapshot.return_value = mock_response

        result = asyncio.run(manager.get_latest_snapshot("test-strategy"))

        assert result is not None
        assert len(result.positions) == 1
        assert result.positions[0].protocol == "traderjoe_v2"
        assert result.snapshot_metadata["valuation_source"] == "reconciled_external"
        assert result.snapshot_metadata["external_total_value_usd"] == "4.70"

    def test_get_latest_snapshot_not_found(self, mock_client):
        """get_latest_snapshot returns None when no snapshot exists."""
        import asyncio

        from almanak.framework.state.gateway_state_manager import GatewayStateManager

        manager = GatewayStateManager(mock_client)

        mock_response = gateway_pb2.SnapshotData(found=False)
        mock_client.state.GetLatestSnapshot.return_value = mock_response

        result = asyncio.run(manager.get_latest_snapshot("test-strategy"))

        assert result is None

    def test_get_latest_snapshot_failure_returns_none(self, mock_client):
        """get_latest_snapshot returns None on gRPC failure."""
        import asyncio

        from almanak.framework.state.gateway_state_manager import GatewayStateManager

        manager = GatewayStateManager(mock_client)
        mock_client.state.GetLatestSnapshot.side_effect = RuntimeError("gRPC failed")

        result = asyncio.run(manager.get_latest_snapshot("test-strategy"))

        assert result is None

    def test_get_snapshots_since_success(self, mock_client):
        """get_snapshots_since returns snapshots from gRPC response."""
        import asyncio

        from almanak.framework.state.gateway_state_manager import GatewayStateManager

        manager = GatewayStateManager(mock_client)

        ts = int(datetime.now(UTC).timestamp())
        mock_response = gateway_pb2.SnapshotList(
            snapshots=[
                gateway_pb2.SnapshotData(
                    deployment_id="test-strategy",
                    timestamp=ts,
                    total_value_usd="1000.50",
                    available_cash_usd="500.25",
                    value_confidence="HIGH",
                    positions_json=b"[]",
                    chain="arbitrum",
                    found=True,
                )
            ]
        )
        mock_client.state.GetSnapshotsSince.return_value = mock_response

        since = datetime.now(UTC)
        result = asyncio.run(manager.get_snapshots_since("test-strategy", since))

        assert len(result) == 1
        assert result[0].deployment_id == "test-strategy"

    def test_get_snapshots_since_failure_returns_empty(self, mock_client):
        """get_snapshots_since returns empty list on gRPC failure."""
        import asyncio

        from almanak.framework.state.gateway_state_manager import GatewayStateManager

        manager = GatewayStateManager(mock_client)
        mock_client.state.GetSnapshotsSince.side_effect = RuntimeError("gRPC failed")

        since = datetime.now(UTC)
        result = asyncio.run(manager.get_snapshots_since("test-strategy", since))

        assert result == []


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
                deployment_id="test",
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
                deployment_id="test",
            )
        )

        assert result.success is False
        assert result.error == "Insufficient funds"
