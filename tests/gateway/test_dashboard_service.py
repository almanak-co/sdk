"""Tests for the gateway DashboardService.

Tests cover:
- ListStrategies RPC (with filters)
- GetStrategyDetails RPC
- GetTimeline RPC
- GetStrategyConfig RPC
- GetStrategyState RPC
- ExecuteAction RPC (pause/resume)
- GetQuantHeader RPC (Senior-Quant header aggregation)
- GetTradeTape RPC (joined ledger × accounting × position events)
- Validation error handling
"""

import json
import tempfile
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

import grpc
import pytest

from almanak.framework.portfolio.models import PortfolioSnapshot, TokenBalance, ValueConfidence
from almanak.gateway.core.settings import GatewaySettings
from almanak.gateway.proto import gateway_pb2
from almanak.gateway.services.dashboard_service import DashboardServiceServicer
from almanak.gateway.timeline.store import TimelineEvent, reset_timeline_store


@pytest.fixture
def settings():
    """Create gateway settings for testing."""
    return GatewaySettings()


@pytest.fixture
def mock_context():
    """Create mock gRPC context."""
    context = MagicMock(spec=grpc.aio.ServicerContext)
    return context


@pytest.fixture
def dashboard_service(settings):
    """Create DashboardService instance."""
    service = DashboardServiceServicer(settings)
    return service


@pytest.fixture
def temp_strategies_dir():
    """Create temporary strategies directory with test data."""
    with tempfile.TemporaryDirectory() as tmpdir:
        strategies_root = Path(tmpdir) / "strategies"
        demo_dir = strategies_root / "demo"
        demo_dir.mkdir(parents=True)

        # Create test strategy
        strategy_dir = demo_dir / "test_strategy"
        strategy_dir.mkdir()

        config = {
            "deployment_id": "test_strategy",
            "strategy_name": "Test Strategy",
            "chain": "arbitrum",
            "protocol": "Uniswap V3",
            "wallet_address": "0x1234567890abcdef1234567890abcdef12345678",
        }
        (strategy_dir / "config.json").write_text(json.dumps(config))

        yield strategies_root


def _make_instance(
    deployment_id: str = "test_strategy",
    strategy_name: str | None = None,
    chain: str = "arbitrum",
    protocol: str = "Uniswap V3",
    status: str = "RUNNING",
    last_heartbeat_at: datetime | None = None,
) -> MagicMock:
    """Create a mock StrategyInstance for testing."""
    inst = MagicMock()
    inst.deployment_id = deployment_id
    inst.strategy_name = strategy_name or deployment_id.split(":")[0]
    inst.template_name = "TestStrategy"
    inst.chain = chain
    inst.protocol = protocol
    inst.wallet_address = "0x1234"
    inst.chains = chain
    inst.chain_wallets = ""
    inst.status = status
    inst.archived = False
    inst.last_heartbeat_at = last_heartbeat_at or datetime.now(UTC)
    return inst


class TestListStrategies:
    """Tests for ListStrategies RPC.

    Covers the behavior matrix:
    - REGISTRY (default): instances only
    - AVAILABLE: templates only, deduplicated by canonical template ID
    - ALL: combined (registry + filesystem, deduplicated)
    - Status filters (RUNNING, PAUSED, etc.): applied on registry results
    - Invalid filters: return INVALID_ARGUMENT
    """

    @pytest.fixture(autouse=True)
    def _no_paper_sessions(self, dashboard_service):
        """Prevent real ~/.almanak/paper/ from leaking into tests."""
        with patch.object(dashboard_service, "_discover_paper_sessions", return_value=[]):
            yield

    @pytest.mark.asyncio
    async def test_default_filter_is_registry(self, dashboard_service, mock_context):
        """Default (no filter) should use REGISTRY mode, returning empty when no instances."""
        dashboard_service._initialized = True
        dashboard_service._strategies_root = Path("/nonexistent")

        mock_registry = MagicMock()
        mock_registry.list_all.return_value = []

        with patch("almanak.gateway.services.dashboard_service.get_instance_registry", return_value=mock_registry):
            request = gateway_pb2.ListStrategiesRequest()
            response = await dashboard_service.ListStrategies(request, mock_context)

        assert response.total_count == 0
        assert len(response.strategies) == 0

    @pytest.mark.asyncio
    async def test_registry_returns_instances(self, dashboard_service, mock_context):
        """REGISTRY filter returns only instance registry entries."""
        dashboard_service._initialized = True
        dashboard_service._strategies_root = Path("/nonexistent")

        mock_registry = MagicMock()
        mock_registry.list_all.return_value = [
            _make_instance("uniswap_lp:abc123", chain="arbitrum", status="RUNNING"),
        ]

        with patch("almanak.gateway.services.dashboard_service.get_instance_registry", return_value=mock_registry):
            request = gateway_pb2.ListStrategiesRequest(status_filter="REGISTRY")
            response = await dashboard_service.ListStrategies(request, mock_context)

        assert response.total_count == 1
        assert response.strategies[0].deployment_id == "uniswap_lp:abc123"
        assert response.strategies[0].status == "RUNNING"

    @pytest.mark.asyncio
    async def test_available_returns_filesystem_templates(self, dashboard_service, mock_context, temp_strategies_dir):
        """AVAILABLE filter returns filesystem-discovered templates."""
        dashboard_service._initialized = True
        dashboard_service._strategies_root = temp_strategies_dir

        # Empty registry - all templates should appear
        mock_registry = MagicMock()
        mock_registry.list_all.return_value = []

        with patch("almanak.gateway.services.dashboard_service.get_instance_registry", return_value=mock_registry):
            request = gateway_pb2.ListStrategiesRequest(status_filter="AVAILABLE")
            response = await dashboard_service.ListStrategies(request, mock_context)

        assert response.total_count == 1
        assert response.strategies[0].deployment_id == "test_strategy"
        assert response.strategies[0].chain == "arbitrum"
        assert response.strategies[0].protocol == "Uniswap V3"

    @pytest.mark.asyncio
    async def test_available_deduplicates_exact_match(self, dashboard_service, mock_context, temp_strategies_dir):
        """AVAILABLE excludes templates with exact-match instance IDs (--once runs)."""
        dashboard_service._initialized = True
        dashboard_service._strategies_root = temp_strategies_dir

        # Instance with exact template ID (from --once run)
        mock_registry = MagicMock()
        mock_registry.list_all.return_value = [
            _make_instance("test_strategy"),  # Exact match to filesystem template
        ]

        with patch("almanak.gateway.services.dashboard_service.get_instance_registry", return_value=mock_registry):
            request = gateway_pb2.ListStrategiesRequest(status_filter="AVAILABLE")
            response = await dashboard_service.ListStrategies(request, mock_context)

        assert response.total_count == 0  # Template excluded

    @pytest.mark.asyncio
    async def test_available_deduplicates_suffixed_instance(self, dashboard_service, mock_context, temp_strategies_dir):
        """AVAILABLE excludes templates when instance uses suffixed ID (continuous runs)."""
        dashboard_service._initialized = True
        dashboard_service._strategies_root = temp_strategies_dir

        # Instance with UUID suffix (from continuous run)
        mock_registry = MagicMock()
        mock_registry.list_all.return_value = [
            _make_instance("test_strategy:abc123def456"),  # Suffixed ID
        ]

        with patch("almanak.gateway.services.dashboard_service.get_instance_registry", return_value=mock_registry):
            request = gateway_pb2.ListStrategiesRequest(status_filter="AVAILABLE")
            response = await dashboard_service.ListStrategies(request, mock_context)

        assert response.total_count == 0  # Template excluded by canonical match

    @pytest.mark.asyncio
    async def test_all_returns_combined(self, dashboard_service, mock_context, temp_strategies_dir):
        """ALL filter returns registry instances plus unseen filesystem templates."""
        dashboard_service._initialized = True
        dashboard_service._strategies_root = temp_strategies_dir

        # Registry has one instance for a different strategy
        mock_registry = MagicMock()
        mock_registry.list_all.return_value = [
            _make_instance("other_strategy:xyz789", chain="base", status="RUNNING", protocol="Aave V3"),
        ]

        with patch("almanak.gateway.services.dashboard_service.get_instance_registry", return_value=mock_registry):
            request = gateway_pb2.ListStrategiesRequest(status_filter="ALL")
            response = await dashboard_service.ListStrategies(request, mock_context)

        assert response.total_count == 2  # 1 registry + 1 filesystem
        ids = {s.deployment_id for s in response.strategies}
        assert "other_strategy:xyz789" in ids
        assert "test_strategy" in ids

    @pytest.mark.asyncio
    async def test_all_deduplicates_templates(self, dashboard_service, mock_context, temp_strategies_dir):
        """ALL filter deduplicates filesystem templates that have registry instances."""
        dashboard_service._initialized = True
        dashboard_service._strategies_root = temp_strategies_dir

        mock_registry = MagicMock()
        mock_registry.list_all.return_value = [
            _make_instance("test_strategy:run001"),  # Same template as filesystem
        ]

        with patch("almanak.gateway.services.dashboard_service.get_instance_registry", return_value=mock_registry):
            request = gateway_pb2.ListStrategiesRequest(status_filter="ALL")
            response = await dashboard_service.ListStrategies(request, mock_context)

        assert response.total_count == 1  # Only the registry instance
        assert response.strategies[0].deployment_id == "test_strategy:run001"

    @pytest.mark.asyncio
    async def test_status_filter_applied_on_registry(self, dashboard_service, mock_context):
        """Status filters (RUNNING, PAUSED, etc.) filter registry instances."""
        dashboard_service._initialized = True
        dashboard_service._strategies_root = Path("/nonexistent")

        mock_registry = MagicMock()
        mock_registry.list_all.return_value = [
            _make_instance("strat_a:001", status="RUNNING"),
            _make_instance("strat_b:002", status="PAUSED"),
        ]

        with patch("almanak.gateway.services.dashboard_service.get_instance_registry", return_value=mock_registry):
            request = gateway_pb2.ListStrategiesRequest(status_filter="RUNNING")
            response = await dashboard_service.ListStrategies(request, mock_context)

        assert response.total_count == 1
        assert response.strategies[0].deployment_id == "strat_a:001"

    @pytest.mark.asyncio
    async def test_archived_filter_includes_archived_instances(self, dashboard_service, mock_context):
        """ARCHIVED filter passes include_archived=True to registry and filters by status."""
        dashboard_service._initialized = True
        dashboard_service._strategies_root = Path("/nonexistent")

        archived_inst = _make_instance("old_strat:001", status="ARCHIVED")
        archived_inst.archived = True

        mock_registry = MagicMock()
        mock_registry.list_all.return_value = [archived_inst]

        with patch("almanak.gateway.services.dashboard_service.get_instance_registry", return_value=mock_registry):
            request = gateway_pb2.ListStrategiesRequest(status_filter="ARCHIVED")
            response = await dashboard_service.ListStrategies(request, mock_context)

        # Verify include_archived=True was passed
        mock_registry.list_all.assert_called_once_with(include_archived=True)
        assert response.total_count == 1
        assert response.strategies[0].deployment_id == "old_strat:001"

    @pytest.mark.asyncio
    async def test_chain_filter(self, dashboard_service, mock_context, temp_strategies_dir):
        """Chain filter works across both AVAILABLE and REGISTRY modes."""
        dashboard_service._initialized = True
        dashboard_service._strategies_root = temp_strategies_dir

        mock_registry = MagicMock()
        mock_registry.list_all.return_value = []

        with patch("almanak.gateway.services.dashboard_service.get_instance_registry", return_value=mock_registry):
            # Match
            request = gateway_pb2.ListStrategiesRequest(status_filter="AVAILABLE", chain_filter="arbitrum")
            response = await dashboard_service.ListStrategies(request, mock_context)
            assert response.total_count == 1

            # No match
            request = gateway_pb2.ListStrategiesRequest(status_filter="AVAILABLE", chain_filter="base")
            response = await dashboard_service.ListStrategies(request, mock_context)
            assert response.total_count == 0

    @pytest.mark.asyncio
    async def test_invalid_filter_returns_error(self, dashboard_service, mock_context):
        """Unknown filter values should return INVALID_ARGUMENT."""
        dashboard_service._initialized = True
        dashboard_service._strategies_root = Path("/nonexistent")

        mock_context.abort = AsyncMock(side_effect=grpc.aio.AbortError(grpc.StatusCode.INVALID_ARGUMENT, "test"))

        request = gateway_pb2.ListStrategiesRequest(status_filter="BOGUS")

        with pytest.raises(grpc.aio.AbortError):
            await dashboard_service.ListStrategies(request, mock_context)

        mock_context.abort.assert_called_once()
        call_args = mock_context.abort.call_args
        assert call_args[0][0] == grpc.StatusCode.INVALID_ARGUMENT
        assert "BOGUS" in call_args[0][1]

    @pytest.mark.asyncio
    async def test_missing_heartbeat_does_not_crash(self, dashboard_service, mock_context):
        """Instance with None last_heartbeat_at should not crash listing."""
        dashboard_service._initialized = True
        dashboard_service._strategies_root = Path("/nonexistent")

        inst = _make_instance("strat:001", status="INACTIVE")
        inst.last_heartbeat_at = None

        mock_registry = MagicMock()
        mock_registry.list_all.return_value = [inst]

        with patch("almanak.gateway.services.dashboard_service.get_instance_registry", return_value=mock_registry):
            request = gateway_pb2.ListStrategiesRequest(status_filter="REGISTRY")
            response = await dashboard_service.ListStrategies(request, mock_context)

        assert response.total_count == 1
        assert response.strategies[0].last_action_at == 0


class TestGetStrategyDetails:
    """Tests for GetStrategyDetails RPC."""

    @pytest.mark.asyncio
    async def test_get_details_not_found(self, dashboard_service, mock_context, temp_strategies_dir):
        """Test getting details for non-existent strategy."""
        dashboard_service._initialized = True
        dashboard_service._strategies_root = temp_strategies_dir

        request = gateway_pb2.GetStrategyDetailsRequest(deployment_id="nonexistent")
        await dashboard_service.GetStrategyDetails(request, mock_context)

        mock_context.set_code.assert_called_with(grpc.StatusCode.NOT_FOUND)

    @pytest.mark.asyncio
    async def test_get_details_success(self, dashboard_service, mock_context, temp_strategies_dir):
        """Test getting details for existing strategy."""
        dashboard_service._initialized = True
        dashboard_service._strategies_root = temp_strategies_dir

        request = gateway_pb2.GetStrategyDetailsRequest(
            deployment_id="test_strategy",
            include_timeline=False,
        )
        response = await dashboard_service.GetStrategyDetails(request, mock_context)

        assert response.summary.deployment_id == "test_strategy"
        assert response.summary.chain == "arbitrum"

    @pytest.mark.asyncio
    async def test_get_details_invalid_deployment_id(self, dashboard_service, mock_context):
        """Test validation of deployment_id."""
        dashboard_service._initialized = True
        dashboard_service._strategies_root = Path("/nonexistent")

        # Empty deployment_id
        request = gateway_pb2.GetStrategyDetailsRequest(deployment_id="")
        await dashboard_service.GetStrategyDetails(request, mock_context)

        mock_context.set_code.assert_called_with(grpc.StatusCode.INVALID_ARGUMENT)

    @pytest.mark.asyncio
    async def test_get_details_postgres_fallback_when_source_missing(self, dashboard_service, mock_context):
        """Decoupled hosted dashboard (ALM-2732): the registry → filesystem →
        paper cascade misses (the strategy runs in a separate pod), but the
        shared Postgres has a snapshot. GetStrategyDetails must NOT 404 — it
        returns position balances from the snapshot so the Current Position
        panel renders instead of showing zeros.
        """
        dashboard_service._initialized = True
        dashboard_service._strategies_root = Path("/nonexistent")  # filesystem discovery misses

        deployment_id = str(uuid4())  # not registered in the local instance registry
        snapshot = PortfolioSnapshot(
            timestamp=datetime.now(UTC),
            deployment_id=deployment_id,
            total_value_usd=Decimal("5.99"),
            available_cash_usd=Decimal("0.77"),
            value_confidence=ValueConfidence.HIGH,
            wallet_balances=[
                TokenBalance(symbol="WETH", balance=Decimal("0.0017"), value_usd=Decimal("5.22")),
                TokenBalance(symbol="USDC", balance=Decimal("0.77"), value_usd=Decimal("0.77")),
            ],
        )
        dashboard_service._get_strategy_state_data = AsyncMock(return_value=None)
        dashboard_service._get_latest_snapshot = AsyncMock(return_value=snapshot)
        dashboard_service._get_portfolio_value_and_pnl = AsyncMock(return_value=("5.99", "0"))
        dashboard_service._get_portfolio_metrics = AsyncMock(return_value=None)

        # Force the local discovery cascade to miss deterministically (empty
        # registry + no paper sessions), so the test exercises the Postgres
        # fallback rather than passing on ambient local data.
        mock_registry = MagicMock()
        mock_registry.get.return_value = None
        with (
            patch("almanak.gateway.services.dashboard_service.get_instance_registry", return_value=mock_registry),
            patch.object(dashboard_service, "_discover_paper_sessions", return_value=[]),
        ):
            request = gateway_pb2.GetStrategyDetailsRequest(deployment_id=deployment_id, include_timeline=False)
            response = await dashboard_service.GetStrategyDetails(request, mock_context)

        # Did NOT 404 (regression: this used to return NOT_FOUND on the dashboard pod).
        mock_context.set_code.assert_not_called()
        assert response.summary.deployment_id == deployment_id
        assert response.summary.total_value_usd == "5.99"
        # Position balances populated from the Postgres snapshot.
        balances = {b.symbol: b for b in response.position.token_balances}
        assert balances["WETH"].balance == "0.0017"
        assert balances["WETH"].value_usd == "5.22"
        assert balances["USDC"].value_usd == "0.77"

    @pytest.mark.asyncio
    async def test_get_details_still_404_when_no_postgres_trace(self, dashboard_service, mock_context):
        """No registry entry, no filesystem source, AND no Postgres state or
        snapshot → genuinely unknown deployment → still NOT_FOUND.
        """
        dashboard_service._initialized = True
        dashboard_service._strategies_root = Path("/nonexistent")

        deployment_id = str(uuid4())
        dashboard_service._get_strategy_state_data = AsyncMock(return_value=None)
        dashboard_service._get_latest_snapshot = AsyncMock(return_value=None)

        mock_registry = MagicMock()
        mock_registry.get.return_value = None
        with (
            patch("almanak.gateway.services.dashboard_service.get_instance_registry", return_value=mock_registry),
            patch.object(dashboard_service, "_discover_paper_sessions", return_value=[]),
        ):
            request = gateway_pb2.GetStrategyDetailsRequest(deployment_id=deployment_id)
            await dashboard_service.GetStrategyDetails(request, mock_context)

        mock_context.set_code.assert_called_with(grpc.StatusCode.NOT_FOUND)


class TestPortfolioFallback:
    """Tests for dashboard portfolio fallback behavior."""

    @pytest.mark.asyncio
    async def test_fresh_metrics_preferred_over_external(self, dashboard_service):
        """Fresh metrics should win and avoid external reads."""
        dashboard_service._initialized = True
        dashboard_service._state_manager = AsyncMock()
        dashboard_service._state_manager.get_portfolio_metrics = AsyncMock(
            return_value=MagicMock(total_value_usd=Decimal("123.45"))
        )
        dashboard_service._state_manager.get_latest_snapshot = AsyncMock(
            return_value=PortfolioSnapshot(
                timestamp=datetime.now(UTC),
                deployment_id="test_strategy",
                total_value_usd=Decimal("123.45"),
                available_cash_usd=Decimal("100"),
                value_confidence=ValueConfidence.HIGH,
            )
        )
        dashboard_service._portfolio_chain = AsyncMock()

        result = await dashboard_service._get_portfolio_value_and_pnl(
            "test_strategy",
        )

        assert result == ("123.45", "0")
        dashboard_service._portfolio_chain.get_wallet_portfolio.assert_not_called()

    @pytest.mark.asyncio
    async def test_metrics_always_win_even_with_stale_snapshot(self, dashboard_service):
        """Metrics are authoritative and should not be skipped due to stale snapshots."""
        dashboard_service._initialized = True
        dashboard_service._state_manager = AsyncMock()
        dashboard_service._state_manager.get_portfolio_metrics = AsyncMock(
            return_value=MagicMock(total_value_usd=Decimal("100"))
        )
        dashboard_service._state_manager.get_latest_snapshot = AsyncMock(
            return_value=PortfolioSnapshot(
                timestamp=datetime.fromtimestamp(0, tz=UTC),
                deployment_id="test_strategy",
                total_value_usd=Decimal("100"),
                available_cash_usd=Decimal("0"),
                value_confidence=ValueConfidence.STALE,
            )
        )
        dashboard_service._portfolio_chain = AsyncMock()

        result = await dashboard_service._get_portfolio_value_and_pnl(
            "test_strategy",
        )

        assert result == ("100", "0")
        # Zerion should not be called when metrics are available
        dashboard_service._portfolio_chain.get_wallet_portfolio.assert_not_called()

    @pytest.mark.asyncio
    async def test_stale_snapshot_returns_zeros_when_no_metrics(self, dashboard_service):
        """Stale snapshot without metrics should return zeros (no external fallback)."""
        dashboard_service._initialized = True
        dashboard_service._state_manager = AsyncMock()
        dashboard_service._state_manager.get_portfolio_metrics = AsyncMock(return_value=None)
        dashboard_service._state_manager.get_latest_snapshot = AsyncMock(
            return_value=PortfolioSnapshot(
                timestamp=datetime.fromtimestamp(0, tz=UTC),
                deployment_id="test_strategy",
                total_value_usd=Decimal("100"),
                available_cash_usd=Decimal("0"),
                value_confidence=ValueConfidence.STALE,
            )
        )

        result = await dashboard_service._get_portfolio_value_and_pnl(
            "test_strategy",
        )

        # Stale snapshots are no longer used — simplified read path returns zeros
        assert result == ("0", "0")

    @pytest.mark.asyncio
    async def test_no_metrics_no_snapshot_returns_zeros(self, dashboard_service):
        """No metrics and no snapshot should return zeros without external calls."""
        dashboard_service._initialized = True
        dashboard_service._state_manager = AsyncMock()
        dashboard_service._state_manager.get_portfolio_metrics = AsyncMock(return_value=None)
        dashboard_service._state_manager.get_latest_snapshot = AsyncMock(return_value=None)
        dashboard_service._portfolio_chain = AsyncMock()

        result = await dashboard_service._get_portfolio_value_and_pnl(
            "test_strategy",
        )

        assert result == ("0", "0")
        # External portfolio API should never be called
        dashboard_service._portfolio_chain.get_wallet_portfolio.assert_not_called()


class TestGetTimeline:
    """Tests for GetTimeline RPC."""

    @pytest.fixture(autouse=True)
    def setup_timeline_store(self):
        """Reset timeline store before each test."""
        reset_timeline_store()
        yield
        reset_timeline_store()

    @pytest.mark.asyncio
    async def test_get_timeline_empty(self, dashboard_service, mock_context, temp_strategies_dir):
        """Test getting timeline for strategy with no events."""
        dashboard_service._initialized = True
        dashboard_service._strategies_root = temp_strategies_dir

        request = gateway_pb2.GetTimelineRequest(
            deployment_id="test_strategy",
            limit=50,
        )
        response = await dashboard_service.GetTimeline(request, mock_context)

        assert len(response.events) == 0

    @pytest.mark.asyncio
    async def test_get_timeline_with_events(self, dashboard_service, mock_context, temp_strategies_dir):
        """Test getting timeline with events from TimelineStore."""
        dashboard_service._initialized = True
        dashboard_service._strategies_root = temp_strategies_dir

        # Add events to TimelineStore
        from almanak.gateway.timeline.store import get_timeline_store

        store = get_timeline_store()
        store.add_event(
            TimelineEvent(
                event_id=str(uuid4()),
                deployment_id="test_strategy",
                timestamp=datetime.now(UTC),
                event_type="TRADE",
                description="Test trade",
                tx_hash="0xabc123",
                chain="arbitrum",
            )
        )

        request = gateway_pb2.GetTimelineRequest(
            deployment_id="test_strategy",
            limit=50,
        )
        response = await dashboard_service.GetTimeline(request, mock_context)

        assert len(response.events) == 1
        assert response.events[0].event_type == "TRADE"
        assert response.events[0].tx_hash == "0xabc123"

    @pytest.mark.asyncio
    async def test_get_timeline_with_filter(self, dashboard_service, mock_context, temp_strategies_dir):
        """Test filtering timeline by event type."""
        dashboard_service._initialized = True
        dashboard_service._strategies_root = temp_strategies_dir

        # Add events to TimelineStore
        from almanak.gateway.timeline.store import get_timeline_store

        store = get_timeline_store()
        store.add_event(
            TimelineEvent(
                event_id=str(uuid4()),
                deployment_id="test_strategy",
                timestamp=datetime.now(UTC),
                event_type="TRADE",
                description="Trade event",
            )
        )
        store.add_event(
            TimelineEvent(
                event_id=str(uuid4()),
                deployment_id="test_strategy",
                timestamp=datetime.now(UTC),
                event_type="ERROR",
                description="Error event",
            )
        )

        request = gateway_pb2.GetTimelineRequest(
            deployment_id="test_strategy",
            limit=50,
            event_type_filter="TRADE",
        )
        response = await dashboard_service.GetTimeline(request, mock_context)

        assert len(response.events) == 1
        assert response.events[0].event_type == "TRADE"


class TestGetStrategyConfig:
    """Tests for GetStrategyConfig RPC."""

    @pytest.mark.asyncio
    async def test_get_config_success(self, dashboard_service, mock_context, temp_strategies_dir):
        """Test getting strategy config successfully."""
        dashboard_service._initialized = True
        dashboard_service._strategies_root = temp_strategies_dir

        request = gateway_pb2.GetStrategyConfigRequest(deployment_id="test_strategy")
        response = await dashboard_service.GetStrategyConfig(request, mock_context)

        assert response.deployment_id == "test_strategy"
        assert response.strategy_name == "Test Strategy"

        config = json.loads(response.config_json)
        assert config["chain"] == "arbitrum"
        assert config["protocol"] == "Uniswap V3"

    @pytest.mark.asyncio
    async def test_get_config_not_found(self, dashboard_service, mock_context, temp_strategies_dir):
        """Test getting config for non-existent strategy."""
        dashboard_service._initialized = True
        dashboard_service._strategies_root = temp_strategies_dir

        request = gateway_pb2.GetStrategyConfigRequest(deployment_id="nonexistent")
        await dashboard_service.GetStrategyConfig(request, mock_context)

        mock_context.set_code.assert_called_with(grpc.StatusCode.NOT_FOUND)


class TestGetStrategyState:
    """Tests for GetStrategyState RPC."""

    @pytest.mark.asyncio
    async def test_get_state_no_state_manager(self, dashboard_service, mock_context):
        """Test getting state when state manager not available."""
        dashboard_service._initialized = True
        dashboard_service._state_manager = None

        request = gateway_pb2.GetStrategyStateRequest(deployment_id="test_strategy")
        await dashboard_service.GetStrategyState(request, mock_context)

        mock_context.set_code.assert_called_with(grpc.StatusCode.NOT_FOUND)

    @pytest.mark.asyncio
    async def test_get_state_with_state_manager(self, dashboard_service, mock_context):
        """Test getting state with state manager available."""
        dashboard_service._initialized = True

        # Mock state manager
        mock_state_manager = AsyncMock()
        mock_state = MagicMock()
        mock_state.state = {"key": "value", "count": 42}
        mock_state.version = 5
        mock_state.updated_at = datetime.now(UTC)
        mock_state_manager.load_state = AsyncMock(return_value=mock_state)
        dashboard_service._state_manager = mock_state_manager

        request = gateway_pb2.GetStrategyStateRequest(deployment_id="test_strategy")
        response = await dashboard_service.GetStrategyState(request, mock_context)

        assert response.deployment_id == "test_strategy"
        assert response.version == 5

        state = json.loads(response.state_json)
        assert state["key"] == "value"
        assert state["count"] == 42

    @pytest.mark.asyncio
    async def test_get_state_with_field_filter(self, dashboard_service, mock_context):
        """Test getting specific fields from state."""
        dashboard_service._initialized = True

        # Mock state manager
        mock_state_manager = AsyncMock()
        mock_state = MagicMock()
        mock_state.state = {"key": "value", "count": 42, "secret": "hidden"}
        mock_state.version = 1
        mock_state.updated_at = datetime.now(UTC)
        mock_state_manager.load_state = AsyncMock(return_value=mock_state)
        dashboard_service._state_manager = mock_state_manager

        request = gateway_pb2.GetStrategyStateRequest(
            deployment_id="test_strategy",
            fields=["key", "count"],
        )
        response = await dashboard_service.GetStrategyState(request, mock_context)

        state = json.loads(response.state_json)
        assert "key" in state
        assert "count" in state
        assert "secret" not in state


class TestExecuteAction:
    """Tests for ExecuteAction RPC."""

    @pytest.mark.asyncio
    async def test_execute_action_no_reason(self, dashboard_service, mock_context):
        """Test that reason is required for actions."""
        dashboard_service._initialized = True

        request = gateway_pb2.ExecuteActionRequest(
            deployment_id="test_strategy",
            action="PAUSE",
            reason="",  # Empty reason
        )
        response = await dashboard_service.ExecuteAction(request, mock_context)

        assert response.success is False
        assert "Reason is required" in response.error

    @pytest.mark.asyncio
    async def test_execute_pause_success(self, dashboard_service, mock_context):
        """Test pausing a strategy successfully."""
        dashboard_service._initialized = True

        mock_store = MagicMock()

        with patch("almanak.gateway.lifecycle.get_lifecycle_store", return_value=mock_store):
            request = gateway_pb2.ExecuteActionRequest(
                deployment_id="test_strategy",
                action="PAUSE",
                reason="Maintenance",
            )
            response = await dashboard_service.ExecuteAction(request, mock_context)

        assert response.success is True
        assert response.action_id  # Should have an action ID

        mock_store.write_command.assert_called_once_with(
            deployment_id="test_strategy",
            command="PAUSE",
            issued_by="dashboard:Maintenance",
        )

    @pytest.mark.asyncio
    async def test_execute_resume_success(self, dashboard_service, mock_context):
        """Test resuming a strategy successfully."""
        dashboard_service._initialized = True

        mock_store = MagicMock()

        with patch("almanak.gateway.lifecycle.get_lifecycle_store", return_value=mock_store):
            request = gateway_pb2.ExecuteActionRequest(
                deployment_id="test_strategy",
                action="RESUME",
                reason="Ready to continue",
            )
            response = await dashboard_service.ExecuteAction(request, mock_context)

        assert response.success is True

        mock_store.write_command.assert_called_once_with(
            deployment_id="test_strategy",
            command="RESUME",
            issued_by="dashboard:Ready to continue",
        )

    @pytest.mark.asyncio
    async def test_execute_unknown_action(self, dashboard_service, mock_context):
        """Test executing an unknown action."""
        dashboard_service._initialized = True

        request = gateway_pb2.ExecuteActionRequest(
            deployment_id="test_strategy",
            action="UNKNOWN_ACTION",
            reason="Test",
        )
        response = await dashboard_service.ExecuteAction(request, mock_context)

        assert response.success is False
        mock_context.set_code.assert_called_with(grpc.StatusCode.UNIMPLEMENTED)


class TestCanonicalTemplateId:
    """Tests for _canonical_template_id helper."""

    def test_plain_id(self, dashboard_service):
        assert dashboard_service._canonical_template_id("uniswap_lp") == "uniswap_lp"

    def test_suffixed_id(self, dashboard_service):
        assert dashboard_service._canonical_template_id("uniswap_lp:abc123") == "uniswap_lp"

    def test_multiple_colons(self, dashboard_service):
        assert dashboard_service._canonical_template_id("my:complex:id") == "my"

    def test_empty_string(self, dashboard_service):
        assert dashboard_service._canonical_template_id("") == ""


class TestProtocolDerivation:
    """Tests for protocol derivation from config.

    The deployment-id substring-sniff ladder was deleted in VIB-4810
    (Phase 1): a user-controlled ``deployment_id`` is not safe to route on,
    and the canonical ``ProtocolName`` registry (Phase 3) carries protocol
    identity. Only an explicit ``config["protocol"]`` string is honoured;
    everything else returns ``"Unknown"``.
    """

    def test_derive_protocol_from_explicit_config(self, dashboard_service):
        """Test deriving protocol when explicitly set in config."""
        config = {"protocol": "Custom Protocol"}
        result = dashboard_service._derive_protocol_from_config(config, "test")
        assert result == "Custom Protocol"

    def test_unknown_when_no_explicit_protocol(self, dashboard_service):
        """Test that unrecognised deployments without explicit protocol return Unknown.

        Pre-VIB-4810 these all returned protocol-specific strings via a
        substring-sniff on ``deployment_id``. That ladder is gone — the
        contract is now: explicit ``config["protocol"]`` or
        ``"Unknown"``. Nothing in between.
        """
        assert dashboard_service._derive_protocol_from_config({}, "uniswap_lp") == "Unknown"
        assert dashboard_service._derive_protocol_from_config({}, "aave_lending") == "Unknown"
        assert dashboard_service._derive_protocol_from_config({}, "gmx_perps") == "Unknown"
        assert dashboard_service._derive_protocol_from_config({}, "pancake_swap") == "Unknown"
        assert dashboard_service._derive_protocol_from_config({}, "aerodrome_lp") == "Unknown"
        assert dashboard_service._derive_protocol_from_config({}, "tj_liquidity") == "Unknown"
        assert dashboard_service._derive_protocol_from_config({}, "unknown_strategy") == "Unknown"

    def test_pool_in_config_no_longer_routes_to_uniswap(self, dashboard_service):
        """Pre-VIB-4810 ``"pool" in config`` returned ``"Uniswap V3"`` —
        removed for the same reason as the deployment-id ladder. A config
        key name is not a protocol identifier."""
        assert dashboard_service._derive_protocol_from_config({"pool": "0x123"}, "test") == "Unknown"


class TestDeployedModeIdentityPassThrough:
    """Identity is passed through verbatim — no gateway-side translation.

    Per blueprint 29 (VIB-4722) the gateway no longer rewrites the SDK's
    ``deployment_id`` to a separate hosted env id: there is one canonical
    ``deployment_id``, resolved once at runner boot, and every gateway RPC
    filters whatever the caller passed. These tests pin that pass-through
    behaviour — the registry / heartbeat is keyed on the exact wire id.
    """

    @pytest.mark.asyncio
    async def test_get_strategy_details_uses_wire_id(self, dashboard_service, mock_context):
        """GetStrategyDetails queries the registry with the wire deployment_id."""
        dashboard_service._initialized = True
        dashboard_service._strategies_root = Path("/nonexistent")

        mock_registry = MagicMock()
        inst = _make_instance("platform-agent-1234", strategy_name="uniswap_rsi")
        mock_registry.get.return_value = inst

        with patch("almanak.gateway.services.dashboard_service.get_instance_registry", return_value=mock_registry):
            request = gateway_pb2.GetStrategyDetailsRequest(deployment_id="platform-agent-1234")
            response = await dashboard_service.GetStrategyDetails(request, mock_context)

        # Registry queried with the exact wire id — no translation.
        mock_registry.get.assert_called_with("platform-agent-1234")
        assert response.summary.deployment_id == "platform-agent-1234"

    @pytest.mark.asyncio
    async def test_register_instance_stores_wire_id(self, dashboard_service, mock_context):
        """RegisterStrategyInstance stores under the wire deployment_id verbatim."""
        mock_registry = MagicMock()
        mock_registry.get.return_value = None
        mock_registry.register.return_value = True

        with patch("almanak.gateway.services.dashboard_service.get_instance_registry", return_value=mock_registry):
            request = gateway_pb2.RegisterInstanceRequest(
                deployment_id="platform-agent-1234",
                strategy_name="uniswap_rsi",
                chain="arbitrum",
                protocol="Uniswap V3",
            )
            response = await dashboard_service.RegisterStrategyInstance(request, mock_context)

        assert response.success
        registered = mock_registry.register.call_args[0][0]
        assert registered.deployment_id == "platform-agent-1234"

    @pytest.mark.asyncio
    async def test_update_status_uses_wire_id(self, dashboard_service, mock_context):
        """UpdateStrategyInstanceStatus heartbeats the wire deployment_id verbatim."""
        mock_registry = MagicMock()
        mock_registry.heartbeat.return_value = True

        with patch("almanak.gateway.services.dashboard_service.get_instance_registry", return_value=mock_registry):
            request = gateway_pb2.UpdateInstanceStatusRequest(
                deployment_id="deployment:abc123def456",
                heartbeat_only=True,
            )
            response = await dashboard_service.UpdateStrategyInstanceStatus(request, mock_context)

        assert response.success
        mock_registry.heartbeat.assert_called_with("deployment:abc123def456")

    @pytest.mark.asyncio
    async def test_local_id_passes_through(self, dashboard_service, mock_context):
        """A local deployment_id passes through unchanged."""
        dashboard_service._initialized = True
        dashboard_service._strategies_root = Path("/nonexistent")

        mock_registry = MagicMock()
        mock_registry.get.return_value = _make_instance("deployment:abc123def456")

        with patch("almanak.gateway.services.dashboard_service.get_instance_registry", return_value=mock_registry):
            request = gateway_pb2.GetStrategyDetailsRequest(deployment_id="deployment:abc123def456")
            await dashboard_service.GetStrategyDetails(request, mock_context)

        mock_registry.get.assert_called_with("deployment:abc123def456")

    @pytest.mark.asyncio
    async def test_get_strategy_config_falls_back_to_registry(self, dashboard_service, mock_context):
        """GetStrategyConfig serves config from registry when filesystem misses."""
        dashboard_service._initialized = True
        dashboard_service._strategies_root = Path("/nonexistent")

        config_data = {"strategy_name": "uniswap_rsi", "chain": "arbitrum"}
        mock_registry = MagicMock()
        inst = _make_instance("platform-agent-1234", strategy_name="uniswap_rsi")
        inst.config_json = json.dumps(config_data)
        inst.updated_at = datetime.now(UTC)
        mock_registry.get.return_value = inst

        with patch("almanak.gateway.services.dashboard_service.get_instance_registry", return_value=mock_registry):
            request = gateway_pb2.GetStrategyConfigRequest(deployment_id="platform-agent-1234")
            response = await dashboard_service.GetStrategyConfig(request, mock_context)

        assert response.deployment_id == "platform-agent-1234"
        assert response.strategy_name == "uniswap_rsi"
        assert json.loads(response.config_json) == config_data


# ---------------------------------------------------------------------------
# Phase 5a-chars: characterization tests for ListStrategies + GetStrategyDetails
#
# These tests pin down current behavior of the 3-way source lookup (registry,
# filesystem, paper sessions), the status filter matrix, chain filtering, and
# the state-enrichment branches. The #1706 "last-branch-wins" elif-ordering
# quirk has been fixed — is_paused now takes precedence over is_running when
# both flags are set. The assertion below has been flipped accordingly.
# ---------------------------------------------------------------------------


class TestListStrategiesSourceMatrix:
    """Characterization: 3-way source lookup (registry / filesystem / paper)."""

    @pytest.mark.asyncio
    async def test_registry_only_source(self, dashboard_service, mock_context):
        """REGISTRY filter: only registry instances, no paper, no filesystem."""
        dashboard_service._initialized = True
        dashboard_service._strategies_root = Path("/nonexistent")

        mock_registry = MagicMock()
        mock_registry.list_all.return_value = [
            _make_instance("only_in_registry:xyz", chain="base", status="RUNNING"),
        ]

        with (
            patch("almanak.gateway.services.dashboard_service.get_instance_registry", return_value=mock_registry),
            patch.object(dashboard_service, "_discover_paper_sessions", return_value=[]),
        ):
            request = gateway_pb2.ListStrategiesRequest(status_filter="REGISTRY")
            response = await dashboard_service.ListStrategies(request, mock_context)

        assert response.total_count == 1
        assert response.strategies[0].deployment_id == "only_in_registry:xyz"
        assert response.strategies[0].chain == "base"

    @pytest.mark.asyncio
    async def test_filesystem_only_source(self, dashboard_service, mock_context, temp_strategies_dir):
        """AVAILABLE filter with empty registry: strategy found only on disk."""
        dashboard_service._initialized = True
        dashboard_service._strategies_root = temp_strategies_dir

        mock_registry = MagicMock()
        mock_registry.list_all.return_value = []

        with (
            patch("almanak.gateway.services.dashboard_service.get_instance_registry", return_value=mock_registry),
            patch.object(dashboard_service, "_discover_paper_sessions", return_value=[]),
        ):
            request = gateway_pb2.ListStrategiesRequest(status_filter="AVAILABLE")
            response = await dashboard_service.ListStrategies(request, mock_context)

        assert response.total_count == 1
        assert response.strategies[0].deployment_id == "test_strategy"
        # Filesystem templates default to PAUSED status
        assert response.strategies[0].status == "PAUSED"

    @pytest.mark.asyncio
    async def test_paper_session_only_source(self, dashboard_service, mock_context):
        """Paper session appears in REGISTRY mode when not in registry or filesystem."""
        dashboard_service._initialized = True
        dashboard_service._strategies_root = Path("/nonexistent")

        mock_registry = MagicMock()
        mock_registry.list_all.return_value = []

        paper_session = {
            "deployment_id": "paper:my_strat",
            "name": "My Strat (Paper)",
            "status": "PAPER_TRADING",
            "chain": "arbitrum",
            "protocol": "Uniswap V3",
            "total_value_usd": "100",
            "pnl_24h_usd": "0",
            "last_action_at": 0,
            "attention_required": False,
            "attention_reason": "",
            "is_multi_chain": False,
            "chains": ["arbitrum"],
            "execution_mode": "paper",
            "paper_metrics_json": "{}",
        }

        with (
            patch("almanak.gateway.services.dashboard_service.get_instance_registry", return_value=mock_registry),
            patch.object(dashboard_service, "_discover_paper_sessions", return_value=[paper_session]),
        ):
            request = gateway_pb2.ListStrategiesRequest(status_filter="REGISTRY")
            response = await dashboard_service.ListStrategies(request, mock_context)

        assert response.total_count == 1
        assert response.strategies[0].deployment_id == "paper:my_strat"
        assert response.strategies[0].status == "PAPER_TRADING"
        assert response.strategies[0].execution_mode == "paper"

    @pytest.mark.asyncio
    async def test_mixed_sources_registry_plus_paper(self, dashboard_service, mock_context, temp_strategies_dir):
        """ALL filter combines registry + filesystem + paper sessions."""
        dashboard_service._initialized = True
        dashboard_service._strategies_root = temp_strategies_dir

        mock_registry = MagicMock()
        mock_registry.list_all.return_value = [
            _make_instance("live_strat:abc", chain="base", status="RUNNING"),
        ]

        paper_session = {
            "deployment_id": "paper:sim_strat",
            "name": "Sim (Paper)",
            "status": "PAPER_TRADING",
            "chain": "arbitrum",
            "protocol": "Unknown",
            "total_value_usd": "50",
            "pnl_24h_usd": "0",
            "last_action_at": 0,
            "attention_required": False,
            "attention_reason": "",
            "is_multi_chain": False,
            "chains": ["arbitrum"],
            "execution_mode": "paper",
            "paper_metrics_json": "{}",
        }

        with (
            patch("almanak.gateway.services.dashboard_service.get_instance_registry", return_value=mock_registry),
            patch.object(dashboard_service, "_discover_paper_sessions", return_value=[paper_session]),
        ):
            request = gateway_pb2.ListStrategiesRequest(status_filter="ALL")
            response = await dashboard_service.ListStrategies(request, mock_context)

        ids = {s.deployment_id for s in response.strategies}
        # Registry (live_strat:abc) + filesystem (test_strategy) + paper (paper:sim_strat)
        assert ids == {"live_strat:abc", "test_strategy", "paper:sim_strat"}

    @pytest.mark.asyncio
    async def test_paper_excluded_from_available_source(self, dashboard_service, mock_context, temp_strategies_dir):
        """AVAILABLE mode is library-only: paper sessions must not leak through."""
        dashboard_service._initialized = True
        dashboard_service._strategies_root = temp_strategies_dir

        mock_registry = MagicMock()
        mock_registry.list_all.return_value = []

        paper_session = {
            "deployment_id": "paper:sim_strat",
            "name": "Sim (Paper)",
            "status": "PAPER_TRADING",
            "chain": "arbitrum",
            "protocol": "Unknown",
            "total_value_usd": "0",
            "pnl_24h_usd": "0",
            "last_action_at": 0,
            "attention_required": False,
            "attention_reason": "",
            "is_multi_chain": False,
            "chains": ["arbitrum"],
            "execution_mode": "paper",
            "paper_metrics_json": "{}",
        }

        with (
            patch("almanak.gateway.services.dashboard_service.get_instance_registry", return_value=mock_registry),
            patch.object(dashboard_service, "_discover_paper_sessions", return_value=[paper_session]),
        ):
            request = gateway_pb2.ListStrategiesRequest(status_filter="AVAILABLE")
            response = await dashboard_service.ListStrategies(request, mock_context)

        ids = {s.deployment_id for s in response.strategies}
        assert "paper:sim_strat" not in ids
        assert "test_strategy" in ids


class TestListStrategiesStatusFilters:
    """Characterization: every status filter value matches its registry status."""

    @pytest.fixture(autouse=True)
    def _no_paper_sessions(self, dashboard_service):
        with patch.object(dashboard_service, "_discover_paper_sessions", return_value=[]):
            yield

    @pytest.mark.asyncio
    @pytest.mark.parametrize("status", ["RUNNING", "PAUSED", "ERROR", "STUCK", "INACTIVE"])
    async def test_each_status_filter_matches_registry_status(self, dashboard_service, mock_context, status):
        """RUNNING / PAUSED / ERROR / STUCK / INACTIVE each pick only their own."""
        dashboard_service._initialized = True
        dashboard_service._strategies_root = Path("/nonexistent")

        mock_registry = MagicMock()
        mock_registry.list_all.return_value = [
            _make_instance("strat_running:01", status="RUNNING"),
            _make_instance("strat_paused:02", status="PAUSED"),
            _make_instance("strat_error:03", status="ERROR"),
            _make_instance("strat_stuck:04", status="STUCK"),
            _make_instance("strat_inactive:05", status="INACTIVE"),
        ]

        with patch(
            "almanak.gateway.services.dashboard_service.get_instance_registry",
            return_value=mock_registry,
        ):
            request = gateway_pb2.ListStrategiesRequest(status_filter=status)
            response = await dashboard_service.ListStrategies(request, mock_context)

        assert response.total_count == 1
        assert response.strategies[0].status == status

    @pytest.mark.asyncio
    async def test_chain_filter_arbitrum_selects_only_matching(self, dashboard_service, mock_context):
        """chain_filter='arbitrum' picks out only arbitrum instances."""
        dashboard_service._initialized = True
        dashboard_service._strategies_root = Path("/nonexistent")

        mock_registry = MagicMock()
        mock_registry.list_all.return_value = [
            _make_instance("strat_arb:01", chain="arbitrum", status="RUNNING"),
            _make_instance("strat_base:02", chain="base", status="RUNNING"),
            _make_instance("strat_avax:03", chain="avalanche", status="RUNNING"),
        ]

        with patch(
            "almanak.gateway.services.dashboard_service.get_instance_registry",
            return_value=mock_registry,
        ):
            request = gateway_pb2.ListStrategiesRequest(chain_filter="arbitrum")
            response = await dashboard_service.ListStrategies(request, mock_context)

        assert response.total_count == 1
        assert response.strategies[0].chain == "arbitrum"

    @pytest.mark.asyncio
    async def test_chain_filter_substring_matches_multichain(self, dashboard_service, mock_context):
        """chain_filter does a substring match against the chain column."""
        dashboard_service._initialized = True
        dashboard_service._strategies_root = Path("/nonexistent")

        mock_registry = MagicMock()
        mock_registry.list_all.return_value = [
            _make_instance("multi:01", chain="arbitrum,base", status="RUNNING"),
            _make_instance("single:02", chain="avalanche", status="RUNNING"),
        ]

        with patch(
            "almanak.gateway.services.dashboard_service.get_instance_registry",
            return_value=mock_registry,
        ):
            request = gateway_pb2.ListStrategiesRequest(chain_filter="base")
            response = await dashboard_service.ListStrategies(request, mock_context)

        # Substring match: "arbitrum,base" contains "base" → included
        assert response.total_count == 1
        assert response.strategies[0].deployment_id == "multi:01"
        assert response.strategies[0].is_multi_chain is True


class TestListStrategiesRegistryEnrichment:
    """Characterization: registry-instance enrichment (chain_wallets, wallet_address, state)."""

    @pytest.fixture(autouse=True)
    def _no_paper_sessions(self, dashboard_service):
        with patch.object(dashboard_service, "_discover_paper_sessions", return_value=[]):
            yield

    @pytest.mark.asyncio
    async def test_chain_wallets_json_parse_success(self, dashboard_service, mock_context):
        """Valid chain_wallets JSON is parsed and exposed on the summary proto."""
        dashboard_service._initialized = True
        dashboard_service._strategies_root = Path("/nonexistent")

        inst = _make_instance("multi:01", chain="arbitrum,base", status="RUNNING")
        inst.chain_wallets = json.dumps({"arbitrum": "0xAAA", "base": "0xBBB"})

        mock_registry = MagicMock()
        mock_registry.list_all.return_value = [inst]

        with patch(
            "almanak.gateway.services.dashboard_service.get_instance_registry",
            return_value=mock_registry,
        ):
            response = await dashboard_service.ListStrategies(
                gateway_pb2.ListStrategiesRequest(status_filter="REGISTRY"), mock_context
            )

        assert response.total_count == 1
        s = response.strategies[0]
        assert dict(s.chain_wallets) == {"arbitrum": "0xAAA", "base": "0xBBB"}

    @pytest.mark.asyncio
    async def test_chain_wallets_json_parse_failure_is_silent(self, dashboard_service, mock_context):
        """Malformed chain_wallets JSON is swallowed; map ends up empty."""
        dashboard_service._initialized = True
        dashboard_service._strategies_root = Path("/nonexistent")

        inst = _make_instance("broken:01", status="RUNNING")
        inst.chain_wallets = "{not valid json"

        mock_registry = MagicMock()
        mock_registry.list_all.return_value = [inst]

        with patch(
            "almanak.gateway.services.dashboard_service.get_instance_registry",
            return_value=mock_registry,
        ):
            response = await dashboard_service.ListStrategies(
                gateway_pb2.ListStrategiesRequest(status_filter="REGISTRY"), mock_context
            )

        assert response.total_count == 1
        # Parse failure leaves chain_wallets empty and does NOT crash listing
        assert dict(response.strategies[0].chain_wallets) == {}

    @pytest.mark.asyncio
    async def test_wallet_address_is_propagated_to_summary(self, dashboard_service, mock_context):
        """wallet_address from the registry is forwarded on the summary proto."""
        dashboard_service._initialized = True
        dashboard_service._strategies_root = Path("/nonexistent")

        inst = _make_instance("strat:01", status="RUNNING")
        inst.wallet_address = "0xDEADBEEF"

        mock_registry = MagicMock()
        mock_registry.list_all.return_value = [inst]

        with patch(
            "almanak.gateway.services.dashboard_service.get_instance_registry",
            return_value=mock_registry,
        ):
            response = await dashboard_service.ListStrategies(
                gateway_pb2.ListStrategiesRequest(status_filter="REGISTRY"), mock_context
            )

        assert response.strategies[0].wallet_address == "0xDEADBEEF"

    @pytest.mark.asyncio
    async def test_state_enrichment_consecutive_errors_parsed(self, dashboard_service, mock_context):
        """Valid consecutive_errors in state is reflected on the summary."""
        dashboard_service._initialized = True
        dashboard_service._strategies_root = Path("/nonexistent")

        mock_registry = MagicMock()
        mock_registry.list_all.return_value = [
            _make_instance("strat:01", status="RUNNING"),
        ]

        async def fake_state(deployment_id, fallback_deployment_id=None):
            return {"consecutive_errors": 3}

        with (
            patch(
                "almanak.gateway.services.dashboard_service.get_instance_registry",
                return_value=mock_registry,
            ),
            patch.object(dashboard_service, "_get_strategy_state_data", side_effect=fake_state),
        ):
            response = await dashboard_service.ListStrategies(
                gateway_pb2.ListStrategiesRequest(status_filter="REGISTRY"), mock_context
            )

        assert response.strategies[0].consecutive_errors == 3

    @pytest.mark.asyncio
    async def test_state_enrichment_consecutive_errors_parse_failure_defaults_to_zero(
        self, dashboard_service, mock_context
    ):
        """Unparseable consecutive_errors (non-numeric) falls back to 0 without crashing."""
        dashboard_service._initialized = True
        dashboard_service._strategies_root = Path("/nonexistent")

        mock_registry = MagicMock()
        mock_registry.list_all.return_value = [
            _make_instance("strat:01", status="RUNNING"),
        ]

        async def fake_state(deployment_id, fallback_deployment_id=None):
            return {"consecutive_errors": "not-a-number"}

        with (
            patch(
                "almanak.gateway.services.dashboard_service.get_instance_registry",
                return_value=mock_registry,
            ),
            patch.object(dashboard_service, "_get_strategy_state_data", side_effect=fake_state),
        ):
            response = await dashboard_service.ListStrategies(
                gateway_pb2.ListStrategiesRequest(status_filter="REGISTRY"), mock_context
            )

        assert response.strategies[0].consecutive_errors == 0

    @pytest.mark.asyncio
    async def test_state_enrichment_last_iteration_at_parse_failure_defaults_to_zero(
        self, dashboard_service, mock_context
    ):
        """Malformed last_iteration.timestamp → graceful 0, no crash."""
        dashboard_service._initialized = True
        dashboard_service._strategies_root = Path("/nonexistent")

        mock_registry = MagicMock()
        mock_registry.list_all.return_value = [
            _make_instance("strat:01", status="RUNNING"),
        ]

        async def fake_state(deployment_id, fallback_deployment_id=None):
            return {"last_iteration": {"timestamp": "not-an-iso-date"}}

        with (
            patch(
                "almanak.gateway.services.dashboard_service.get_instance_registry",
                return_value=mock_registry,
            ),
            patch.object(dashboard_service, "_get_strategy_state_data", side_effect=fake_state),
        ):
            response = await dashboard_service.ListStrategies(
                gateway_pb2.ListStrategiesRequest(status_filter="REGISTRY"), mock_context
            )

        assert response.strategies[0].last_iteration_at == 0


class TestGetStrategyDetailsStateEnrichment:
    """Characterization: GetStrategyDetails state-enrichment precedence rules.

    Covers the branch cascade in `_dashboard_helpers.py:enrich_strategy_info`:
      1. registry PAUSED wins over state-derived status
      2. EXECUTION_FAILED / STRATEGY_ERROR → ERROR
      3. is_paused → PAUSED  (fix #1706: paused > running)
      4. is_running → RUNNING
    """

    @pytest.mark.asyncio
    async def test_iteration_status_execution_failed_overrides_registry_running(
        self, dashboard_service, mock_context, monkeypatch
    ):
        """EXECUTION_FAILED in state overrides RUNNING from registry → status=ERROR."""
        monkeypatch.delenv("ALMANAK_IS_HOSTED", raising=False)
        dashboard_service._initialized = True
        dashboard_service._strategies_root = Path("/nonexistent")

        mock_registry = MagicMock()
        mock_registry.get.return_value = _make_instance("strat:01", status="RUNNING")

        async def fake_state(deployment_id, fallback_deployment_id=None):
            return {"last_iteration": {"status": "EXECUTION_FAILED"}}

        with (
            patch(
                "almanak.gateway.services.dashboard_service.get_instance_registry",
                return_value=mock_registry,
            ),
            patch.object(dashboard_service, "_get_strategy_state_data", side_effect=fake_state),
        ):
            request = gateway_pb2.GetStrategyDetailsRequest(deployment_id="strat:01")
            response = await dashboard_service.GetStrategyDetails(request, mock_context)

        assert response.summary.status == "ERROR"
        assert response.summary.attention_required is True
        assert "EXECUTION_FAILED" in response.summary.attention_reason

    @pytest.mark.asyncio
    async def test_registry_paused_wins_over_iteration_error(self, dashboard_service, mock_context, monkeypatch):
        """Registry PAUSED preserved even when state reports EXECUTION_FAILED."""
        monkeypatch.delenv("ALMANAK_IS_HOSTED", raising=False)
        dashboard_service._initialized = True
        dashboard_service._strategies_root = Path("/nonexistent")

        mock_registry = MagicMock()
        mock_registry.get.return_value = _make_instance("strat:01", status="PAUSED")

        async def fake_state(deployment_id, fallback_deployment_id=None):
            return {
                "last_iteration": {"status": "EXECUTION_FAILED"},
                "is_running": True,  # would also say RUNNING, but registry PAUSED wins
            }

        with (
            patch(
                "almanak.gateway.services.dashboard_service.get_instance_registry",
                return_value=mock_registry,
            ),
            patch.object(dashboard_service, "_get_strategy_state_data", side_effect=fake_state),
        ):
            request = gateway_pb2.GetStrategyDetailsRequest(deployment_id="strat:01")
            response = await dashboard_service.GetStrategyDetails(request, mock_context)

        assert response.summary.status == "PAUSED"

    @pytest.mark.asyncio
    async def test_is_running_in_state_promotes_to_running(self, dashboard_service, mock_context, monkeypatch):
        """is_running=True in state promotes registry status to RUNNING."""
        monkeypatch.delenv("ALMANAK_IS_HOSTED", raising=False)
        dashboard_service._initialized = True
        dashboard_service._strategies_root = Path("/nonexistent")

        # Registry says INACTIVE but state says is_running
        mock_registry = MagicMock()
        mock_registry.get.return_value = _make_instance("strat:01", status="INACTIVE")

        async def fake_state(deployment_id, fallback_deployment_id=None):
            return {"is_running": True}

        with (
            patch(
                "almanak.gateway.services.dashboard_service.get_instance_registry",
                return_value=mock_registry,
            ),
            patch.object(dashboard_service, "_get_strategy_state_data", side_effect=fake_state),
        ):
            request = gateway_pb2.GetStrategyDetailsRequest(deployment_id="strat:01")
            response = await dashboard_service.GetStrategyDetails(request, mock_context)

        assert response.summary.status == "RUNNING"

    @pytest.mark.asyncio
    async def test_issue_1706_paused_wins_over_running(self, dashboard_service, mock_context, monkeypatch):
        """Fix #1706: when state sets BOTH is_running=True and is_paused=True, PAUSED wins.

        A strategy carrying both flags is almost certainly mid-transition; treating it
        as PAUSED is the safer default. Advertising a paused strategy as RUNNING would
        mislead operators into thinking funds are actively being managed.
        """
        monkeypatch.delenv("ALMANAK_IS_HOSTED", raising=False)
        dashboard_service._initialized = True
        dashboard_service._strategies_root = Path("/nonexistent")

        mock_registry = MagicMock()
        mock_registry.get.return_value = _make_instance("strat:01", status="INACTIVE")

        async def fake_state(deployment_id, fallback_deployment_id=None):
            return {"is_running": True, "is_paused": True}

        with (
            patch(
                "almanak.gateway.services.dashboard_service.get_instance_registry",
                return_value=mock_registry,
            ),
            patch.object(dashboard_service, "_get_strategy_state_data", side_effect=fake_state),
        ):
            request = gateway_pb2.GetStrategyDetailsRequest(deployment_id="strat:01")
            response = await dashboard_service.GetStrategyDetails(request, mock_context)

        # Fix #1706: paused precedence — a concurrent is_running=True must not mask pause.
        assert response.summary.status == "PAUSED"

    @pytest.mark.asyncio
    async def test_is_paused_in_state_promotes_to_paused(self, dashboard_service, mock_context, monkeypatch):
        """is_paused=True in state promotes to PAUSED when is_running is absent."""
        monkeypatch.delenv("ALMANAK_IS_HOSTED", raising=False)
        dashboard_service._initialized = True
        dashboard_service._strategies_root = Path("/nonexistent")

        mock_registry = MagicMock()
        mock_registry.get.return_value = _make_instance("strat:01", status="INACTIVE")

        async def fake_state(deployment_id, fallback_deployment_id=None):
            return {"is_paused": True}

        with (
            patch(
                "almanak.gateway.services.dashboard_service.get_instance_registry",
                return_value=mock_registry,
            ),
            patch.object(dashboard_service, "_get_strategy_state_data", side_effect=fake_state),
        ):
            request = gateway_pb2.GetStrategyDetailsRequest(deployment_id="strat:01")
            response = await dashboard_service.GetStrategyDetails(request, mock_context)

        assert response.summary.status == "PAUSED"

    @pytest.mark.asyncio
    async def test_last_iteration_at_parse_failure_defaults_to_zero(self, dashboard_service, mock_context, monkeypatch):
        """Malformed last_iteration.timestamp in state → last_iteration_at=0."""
        monkeypatch.delenv("ALMANAK_IS_HOSTED", raising=False)
        dashboard_service._initialized = True
        dashboard_service._strategies_root = Path("/nonexistent")

        mock_registry = MagicMock()
        mock_registry.get.return_value = _make_instance("strat:01", status="RUNNING")

        async def fake_state(deployment_id, fallback_deployment_id=None):
            return {"last_iteration": {"timestamp": "garbage"}}

        with (
            patch(
                "almanak.gateway.services.dashboard_service.get_instance_registry",
                return_value=mock_registry,
            ),
            patch.object(dashboard_service, "_get_strategy_state_data", side_effect=fake_state),
        ):
            request = gateway_pb2.GetStrategyDetailsRequest(deployment_id="strat:01")
            response = await dashboard_service.GetStrategyDetails(request, mock_context)

        assert response.summary.last_iteration_at == 0


class TestGetStrategyDetailsFallbacksAndOptIns:
    """Characterization: filesystem / paper fallbacks, position balances, opt-in flags."""

    @pytest.mark.asyncio
    async def test_falls_back_to_paper_session_when_not_in_registry_or_filesystem(
        self, dashboard_service, mock_context, monkeypatch
    ):
        """Paper session ID (paper:xxx) resolves via _discover_paper_sessions fallback."""
        monkeypatch.delenv("ALMANAK_IS_HOSTED", raising=False)
        dashboard_service._initialized = True
        dashboard_service._strategies_root = Path("/nonexistent")

        mock_registry = MagicMock()
        mock_registry.get.return_value = None

        paper_session = {
            "deployment_id": "paper:my_sim",
            "name": "My Sim (Paper)",
            "status": "PAPER_TRADING",
            "chain": "arbitrum",
            "protocol": "Unknown",
            "total_value_usd": "200",
            "pnl_24h_usd": "0",
            "last_action_at": 0,
            "attention_required": False,
            "attention_reason": "",
            "is_multi_chain": False,
            "chains": ["arbitrum"],
            "execution_mode": "paper",
            "paper_metrics_json": '{"tick_count": 10}',
        }

        with (
            patch(
                "almanak.gateway.services.dashboard_service.get_instance_registry",
                return_value=mock_registry,
            ),
            patch.object(dashboard_service, "_discover_paper_sessions", return_value=[paper_session]),
        ):
            request = gateway_pb2.GetStrategyDetailsRequest(deployment_id="paper:my_sim")
            response = await dashboard_service.GetStrategyDetails(request, mock_context)

        assert response.summary.deployment_id == "paper:my_sim"
        assert response.summary.execution_mode == "paper"
        assert response.summary.status == "PAPER_TRADING"
        assert response.summary.paper_metrics_json == '{"tick_count": 10}'

    @pytest.mark.asyncio
    async def test_issue_1705_tuple_chains_accepted(self, dashboard_service, mock_context, monkeypatch):
        """Fix #1705: a paper/filesystem source that returns ``chains`` as a
        tuple (rather than a list) must still produce chain_health entries.

        The pre-fix code used a strict ``isinstance(raw_chains, list)`` check,
        silently coercing tuples to an empty list — multi-chain strategies
        whose producer returned a tuple would show "no chains" on the
        operator dashboard. The fix accepts any Sequence[str] (except
        str/bytes) and logs a warning when coercion still happens.
        """
        monkeypatch.delenv("ALMANAK_IS_HOSTED", raising=False)
        dashboard_service._initialized = True
        dashboard_service._strategies_root = Path("/nonexistent")

        mock_registry = MagicMock()
        mock_registry.get.return_value = None

        paper_session = {
            "deployment_id": "paper:multi_chain_tuple",
            "name": "Multi Chain Tuple (Paper)",
            "status": "PAPER_TRADING",
            "chain": "arbitrum,base",
            "protocol": "Unknown",
            "total_value_usd": "0",
            "pnl_24h_usd": "0",
            "last_action_at": 0,
            "attention_required": False,
            "attention_reason": "",
            "is_multi_chain": True,
            # Deliberately a tuple (not a list) — pre-fix this collapsed to []
            "chains": ("arbitrum", "base"),
            "execution_mode": "paper",
            "paper_metrics_json": "",
        }

        with (
            patch(
                "almanak.gateway.services.dashboard_service.get_instance_registry",
                return_value=mock_registry,
            ),
            patch.object(dashboard_service, "_discover_paper_sessions", return_value=[paper_session]),
        ):
            request = gateway_pb2.GetStrategyDetailsRequest(deployment_id="paper:multi_chain_tuple")
            response = await dashboard_service.GetStrategyDetails(request, mock_context)

        assert response.summary.deployment_id == "paper:multi_chain_tuple"
        # Fix #1705: chain health is populated from the tuple, not empty.
        assert set(response.chain_health.keys()) == {"arbitrum", "base"}

    @pytest.mark.asyncio
    async def test_position_prefers_snapshot_wallet_balances_over_state_dict(
        self, dashboard_service, mock_context, monkeypatch, temp_strategies_dir
    ):
        """PositionInfo.token_balances come from PortfolioSnapshot when available,
        NOT from state["balances"] (snapshot wins over state dict fallback)."""
        from almanak.framework.portfolio.models import TokenBalance

        monkeypatch.delenv("ALMANAK_IS_HOSTED", raising=False)
        dashboard_service._initialized = True
        dashboard_service._strategies_root = temp_strategies_dir

        snapshot = PortfolioSnapshot(
            timestamp=datetime.now(UTC),
            deployment_id="test_strategy",
            total_value_usd=Decimal("500"),
            available_cash_usd=Decimal("500"),
            value_confidence=ValueConfidence.HIGH,
            wallet_balances=[
                TokenBalance(symbol="USDC", balance=Decimal("500"), value_usd=Decimal("500")),
            ],
        )

        async def fake_snapshot(deployment_id):
            return snapshot

        async def fake_state(deployment_id, fallback_deployment_id=None):
            # State also has balances — must NOT win
            return {"balances": {"WETH": {"balance": "99", "value_usd": "99"}}}

        with (
            patch.object(dashboard_service, "_get_latest_snapshot", side_effect=fake_snapshot),
            patch.object(dashboard_service, "_get_strategy_state_data", side_effect=fake_state),
        ):
            request = gateway_pb2.GetStrategyDetailsRequest(deployment_id="test_strategy")
            response = await dashboard_service.GetStrategyDetails(request, mock_context)

        symbols = [tb.symbol for tb in response.position.token_balances]
        assert symbols == ["USDC"]
        assert response.position.token_balances[0].balance == "500"

    @pytest.mark.asyncio
    async def test_position_falls_back_to_state_balances_when_snapshot_empty(
        self, dashboard_service, mock_context, monkeypatch, temp_strategies_dir
    ):
        """With no snapshot balances, PositionInfo is populated from state["balances"]."""
        monkeypatch.delenv("ALMANAK_IS_HOSTED", raising=False)
        dashboard_service._initialized = True
        dashboard_service._strategies_root = temp_strategies_dir

        async def fake_snapshot(deployment_id):
            return None  # No snapshot available

        async def fake_state(deployment_id, fallback_deployment_id=None):
            return {
                "balances": {
                    "USDC": {"balance": "100", "value_usd": "100"},
                    "WETH": {"balance": "0.5", "value_usd": "1500"},
                },
                "health_factor": "1.85",
                "leverage": "2.0",
            }

        with (
            patch.object(dashboard_service, "_get_latest_snapshot", side_effect=fake_snapshot),
            patch.object(dashboard_service, "_get_strategy_state_data", side_effect=fake_state),
        ):
            request = gateway_pb2.GetStrategyDetailsRequest(deployment_id="test_strategy")
            response = await dashboard_service.GetStrategyDetails(request, mock_context)

        symbols = {tb.symbol for tb in response.position.token_balances}
        assert symbols == {"USDC", "WETH"}
        assert response.position.health_factor == "1.85"
        assert response.position.leverage == "2.0"

    @pytest.mark.asyncio
    async def test_timeline_opt_in_true_invokes_inner_get_timeline(
        self, dashboard_service, mock_context, monkeypatch, temp_strategies_dir
    ):
        """include_timeline=True calls self.GetTimeline with the requested limit."""
        monkeypatch.delenv("ALMANAK_IS_HOSTED", raising=False)
        dashboard_service._initialized = True
        dashboard_service._strategies_root = temp_strategies_dir

        fake_event = gateway_pb2.TimelineEventInfo(
            timestamp=1_700_000_000,
            event_type="TRADE",
            description="fake",
        )
        timeline_response = gateway_pb2.GetTimelineResponse(events=[fake_event])

        with patch.object(
            dashboard_service,
            "GetTimeline",
            new=AsyncMock(return_value=timeline_response),
        ) as mock_get_timeline:
            request = gateway_pb2.GetStrategyDetailsRequest(
                deployment_id="test_strategy",
                include_timeline=True,
                timeline_limit=7,
            )
            response = await dashboard_service.GetStrategyDetails(request, mock_context)

        mock_get_timeline.assert_awaited_once()
        inner_req = mock_get_timeline.await_args[0][0]
        assert inner_req.deployment_id == "test_strategy"
        assert inner_req.limit == 7
        assert len(response.timeline) == 1
        assert response.timeline[0].event_type == "TRADE"

    @pytest.mark.asyncio
    async def test_timeline_opt_in_false_does_not_call_inner(
        self, dashboard_service, mock_context, monkeypatch, temp_strategies_dir
    ):
        """include_timeline=False (default) must NOT invoke self.GetTimeline."""
        monkeypatch.delenv("ALMANAK_IS_HOSTED", raising=False)
        dashboard_service._initialized = True
        dashboard_service._strategies_root = temp_strategies_dir

        with patch.object(
            dashboard_service,
            "GetTimeline",
            new=AsyncMock(return_value=gateway_pb2.GetTimelineResponse()),
        ) as mock_get_timeline:
            request = gateway_pb2.GetStrategyDetailsRequest(
                deployment_id="test_strategy",
                include_timeline=False,
            )
            response = await dashboard_service.GetStrategyDetails(request, mock_context)

        mock_get_timeline.assert_not_called()
        assert len(response.timeline) == 0

    @pytest.mark.asyncio
    async def test_pnl_history_opt_in_true_invokes_build_pnl_history(
        self, dashboard_service, mock_context, monkeypatch, temp_strategies_dir
    ):
        """include_pnl_history=True calls _build_pnl_history and attaches points."""
        monkeypatch.delenv("ALMANAK_IS_HOSTED", raising=False)
        dashboard_service._initialized = True
        dashboard_service._strategies_root = temp_strategies_dir

        fake_points = [
            gateway_pb2.PnLDataPoint(timestamp=1_700_000_000, value_usd="100", pnl_usd="5"),
            gateway_pb2.PnLDataPoint(timestamp=1_700_003_600, value_usd="101", pnl_usd="6"),
        ]

        with patch.object(
            dashboard_service,
            "_build_pnl_history",
            new=AsyncMock(return_value=fake_points),
        ) as mock_build:
            request = gateway_pb2.GetStrategyDetailsRequest(
                deployment_id="test_strategy",
                include_pnl_history=True,
            )
            response = await dashboard_service.GetStrategyDetails(request, mock_context)

        mock_build.assert_awaited_once()
        assert len(response.pnl_history) == 2
        assert response.pnl_history[0].value_usd == "100"

    @pytest.mark.asyncio
    async def test_pnl_history_opt_in_false_does_not_build(
        self, dashboard_service, mock_context, monkeypatch, temp_strategies_dir
    ):
        """include_pnl_history=False (default) must NOT call _build_pnl_history."""
        monkeypatch.delenv("ALMANAK_IS_HOSTED", raising=False)
        dashboard_service._initialized = True
        dashboard_service._strategies_root = temp_strategies_dir

        with patch.object(
            dashboard_service,
            "_build_pnl_history",
            new=AsyncMock(return_value=[]),
        ) as mock_build:
            request = gateway_pb2.GetStrategyDetailsRequest(
                deployment_id="test_strategy",
                include_pnl_history=False,
            )
            response = await dashboard_service.GetStrategyDetails(request, mock_context)

        mock_build.assert_not_called()
        assert len(response.pnl_history) == 0

    @pytest.mark.asyncio
    async def test_wallet_address_absent_from_filesystem_source(
        self, dashboard_service, mock_context, monkeypatch, temp_strategies_dir
    ):
        """Filesystem-sourced strategies don't set wallet_address (proto default=empty)."""
        monkeypatch.delenv("ALMANAK_IS_HOSTED", raising=False)
        dashboard_service._initialized = True
        dashboard_service._strategies_root = temp_strategies_dir

        mock_registry = MagicMock()
        mock_registry.get.return_value = None

        with (
            patch(
                "almanak.gateway.services.dashboard_service.get_instance_registry",
                return_value=mock_registry,
            ),
            patch.object(dashboard_service, "_discover_paper_sessions", return_value=[]),
        ):
            request = gateway_pb2.GetStrategyDetailsRequest(deployment_id="test_strategy")
            response = await dashboard_service.GetStrategyDetails(request, mock_context)

        # Filesystem path does not carry wallet_address — proto default is empty string
        assert response.summary.wallet_address == ""
        assert response.summary.deployment_id == "test_strategy"


class TestGetPnLSummary:
    """Smoke tests for GetPnLSummary RPC (VIB-3969).

    The aggregation logic is exhaustively covered by
    ``tests/unit/dashboard/test_quant_aggregations.py``. The tests here
    exercise the gateway-side surface: request validation, proto
    roundtripping, and the no-data degraded shape.
    """

    @pytest.mark.asyncio
    async def test_invalid_deployment_id_returns_invalid_argument(self, dashboard_service, mock_context):
        dashboard_service._initialized = True
        request = gateway_pb2.GetPnLSummaryRequest(deployment_id="")
        response = await dashboard_service.GetPnLSummary(request, mock_context)
        mock_context.set_code.assert_called_once_with(grpc.StatusCode.INVALID_ARGUMENT)
        assert isinstance(response, gateway_pb2.PnLSummary)

    @pytest.mark.asyncio
    async def test_no_state_manager_returns_degraded_response(self, dashboard_service, mock_context):
        dashboard_service._initialized = True
        dashboard_service._state_manager = None
        request = gateway_pb2.GetPnLSummaryRequest(deployment_id="test_strategy")
        response = await dashboard_service.GetPnLSummary(request, mock_context)
        assert isinstance(response, gateway_pb2.PnLSummary)
        assert response.value_confidence in ("", "UNAVAILABLE")

    @pytest.mark.asyncio
    async def test_empty_backend_returns_zero_decimals(self, dashboard_service, mock_context):
        dashboard_service._initialized = True
        sm = MagicMock()
        sm.get_portfolio_metrics = AsyncMock(return_value=None)
        sm.get_snapshots_since = AsyncMock(return_value=[])
        sm.get_ledger_entries = AsyncMock(return_value=[])
        sm.get_accounting_events_sync = MagicMock(return_value=[])
        dashboard_service._state_manager = sm

        request = gateway_pb2.GetPnLSummaryRequest(deployment_id="test_strategy")
        response = await dashboard_service.GetPnLSummary(request, mock_context)
        assert isinstance(response, gateway_pb2.PnLSummary)
        for field in ("deployed_usd", "nav_usd", "lifetime_pnl_usd"):
            value = getattr(response, field)
            assert value in ("", "0", "0.00") or Decimal(value) == Decimal("0")


class TestGetCostStack:
    """Smoke tests for GetCostStack RPC (VIB-3969)."""

    @pytest.mark.asyncio
    async def test_invalid_deployment_id_returns_invalid_argument(self, dashboard_service, mock_context):
        dashboard_service._initialized = True
        request = gateway_pb2.GetCostStackRequest(deployment_id="")
        response = await dashboard_service.GetCostStack(request, mock_context)
        mock_context.set_code.assert_called_once_with(grpc.StatusCode.INVALID_ARGUMENT)
        assert isinstance(response, gateway_pb2.CostStackInfo)

    @pytest.mark.asyncio
    async def test_no_state_manager_returns_zero_decimals(self, dashboard_service, mock_context):
        dashboard_service._initialized = True
        dashboard_service._state_manager = None
        request = gateway_pb2.GetCostStackRequest(deployment_id="test_strategy")
        response = await dashboard_service.GetCostStack(request, mock_context)
        assert isinstance(response, gateway_pb2.CostStackInfo)
        for field in (
            "cost_gas_usd",
            "cost_protocol_fees_usd",
            "cost_slippage_usd",
            "fees_earned_usd",
        ):
            value = getattr(response, field)
            assert value in ("", "0", "0.00") or Decimal(value) == Decimal("0")


class TestGetAuditPosture:
    """Smoke tests for GetAuditPosture RPC (VIB-3969)."""

    @pytest.mark.asyncio
    async def test_invalid_deployment_id_returns_invalid_argument(self, dashboard_service, mock_context):
        dashboard_service._initialized = True
        request = gateway_pb2.GetAuditPostureRequest(deployment_id="")
        response = await dashboard_service.GetAuditPosture(request, mock_context)
        mock_context.set_code.assert_called_once_with(grpc.StatusCode.INVALID_ARGUMENT)
        assert isinstance(response, gateway_pb2.AuditPosture)

    @pytest.mark.asyncio
    async def test_no_state_manager_returns_na_status(self, dashboard_service, mock_context):
        dashboard_service._initialized = True
        dashboard_service._state_manager = None
        request = gateway_pb2.GetAuditPostureRequest(deployment_id="test_strategy")
        response = await dashboard_service.GetAuditPosture(request, mock_context)
        assert isinstance(response, gateway_pb2.AuditPosture)
        # Empty inputs → no events → G6 has no data → NA status, not
        # PASS or FAIL (G6 must never appear to pass on zero rows).
        assert response.g6_status in ("", "NA")


class TestGetTradeTape:
    """Smoke tests for GetTradeTape RPC.

    Joins ``transaction_ledger × accounting_events × position_events`` on
    ``ledger_entry_id`` and ``cycle_id``. Aggregation/joining logic is
    covered in ``tests/unit/dashboard/test_data_client.py`` and the
    proto-roundtrip suite in ``tests/gateway/test_proto_compatibility.py``.
    The tests here cover the servicer surface: validation + empty-input
    safety + cursor pass-through.
    """

    @pytest.mark.asyncio
    async def test_invalid_deployment_id_returns_invalid_argument(self, dashboard_service, mock_context):
        """An invalid deployment_id sets INVALID_ARGUMENT and returns an empty proto."""
        dashboard_service._initialized = True
        request = gateway_pb2.GetTradeTapeRequest(deployment_id="")
        response = await dashboard_service.GetTradeTape(request, mock_context)
        mock_context.set_code.assert_called_once_with(grpc.StatusCode.INVALID_ARGUMENT)
        assert isinstance(response, gateway_pb2.GetTradeTapeResponse)
        assert len(response.rows) == 0

    @pytest.mark.asyncio
    async def test_no_state_manager_returns_empty_tape(self, dashboard_service, mock_context):
        """No state_manager → empty rows, has_more False, no crash."""
        dashboard_service._initialized = True
        dashboard_service._state_manager = None
        request = gateway_pb2.GetTradeTapeRequest(deployment_id="test_strategy", limit=10)
        response = await dashboard_service.GetTradeTape(request, mock_context)
        assert isinstance(response, gateway_pb2.GetTradeTapeResponse)
        assert len(response.rows) == 0
        assert response.has_more is False

    @pytest.mark.asyncio
    async def test_empty_backend_returns_empty_tape(self, dashboard_service, mock_context):
        """All backend calls returning empty lists collapse to an empty tape."""
        dashboard_service._initialized = True
        sm = MagicMock()
        sm.get_ledger_entries = AsyncMock(return_value=[])
        sm.get_accounting_events_sync = MagicMock(return_value=[])
        sm.get_position_events_sync = MagicMock(return_value=[])
        dashboard_service._state_manager = sm

        request = gateway_pb2.GetTradeTapeRequest(deployment_id="test_strategy", limit=50)
        response = await dashboard_service.GetTradeTape(request, mock_context)
        assert isinstance(response, gateway_pb2.GetTradeTapeResponse)
        assert len(response.rows) == 0
        assert response.has_more is False
        # before_timestamp=0 means "no cursor" — the call still went through.
        sm.get_ledger_entries.assert_awaited_once()
