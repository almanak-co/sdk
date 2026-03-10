"""Tests for the gateway DashboardService.

Tests cover:
- ListStrategies RPC (with filters)
- GetStrategyDetails RPC
- GetTimeline RPC
- GetStrategyConfig RPC
- GetStrategyState RPC
- ExecuteAction RPC (pause/resume)
- Validation error handling
"""

import json
import tempfile
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

import grpc
import pytest

from almanak.gateway.core.settings import GatewaySettings
from almanak.gateway.proto import gateway_pb2
from almanak.gateway.services.dashboard_service import DashboardServiceServicer
from almanak.gateway.timeline.store import TimelineEvent, TimelineStore, reset_timeline_store


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
            "strategy_id": "test_strategy",
            "strategy_name": "Test Strategy",
            "chain": "arbitrum",
            "protocol": "Uniswap V3",
            "wallet_address": "0x1234567890abcdef1234567890abcdef12345678",
        }
        (strategy_dir / "config.json").write_text(json.dumps(config))

        yield strategies_root


def _make_instance(
    strategy_id: str = "test_strategy",
    strategy_name: str | None = None,
    chain: str = "arbitrum",
    protocol: str = "Uniswap V3",
    status: str = "RUNNING",
    last_heartbeat_at: datetime | None = None,
) -> MagicMock:
    """Create a mock StrategyInstance for testing."""
    inst = MagicMock()
    inst.strategy_id = strategy_id
    inst.strategy_name = strategy_name or strategy_id.split(":")[0]
    inst.template_name = "TestStrategy"
    inst.chain = chain
    inst.protocol = protocol
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
        assert response.strategies[0].strategy_id == "uniswap_lp:abc123"
        assert response.strategies[0].status == "RUNNING"

    @pytest.mark.asyncio
    async def test_available_returns_filesystem_templates(
        self, dashboard_service, mock_context, temp_strategies_dir
    ):
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
        assert response.strategies[0].strategy_id == "test_strategy"
        assert response.strategies[0].chain == "arbitrum"
        assert response.strategies[0].protocol == "Uniswap V3"

    @pytest.mark.asyncio
    async def test_available_deduplicates_exact_match(
        self, dashboard_service, mock_context, temp_strategies_dir
    ):
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
    async def test_available_deduplicates_suffixed_instance(
        self, dashboard_service, mock_context, temp_strategies_dir
    ):
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
    async def test_all_returns_combined(
        self, dashboard_service, mock_context, temp_strategies_dir
    ):
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
        ids = {s.strategy_id for s in response.strategies}
        assert "other_strategy:xyz789" in ids
        assert "test_strategy" in ids

    @pytest.mark.asyncio
    async def test_all_deduplicates_templates(
        self, dashboard_service, mock_context, temp_strategies_dir
    ):
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
        assert response.strategies[0].strategy_id == "test_strategy:run001"

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
        assert response.strategies[0].strategy_id == "strat_a:001"

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
        assert response.strategies[0].strategy_id == "old_strat:001"

    @pytest.mark.asyncio
    async def test_chain_filter(
        self, dashboard_service, mock_context, temp_strategies_dir
    ):
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

        mock_context.abort = AsyncMock(side_effect=grpc.aio.AbortError(
            grpc.StatusCode.INVALID_ARGUMENT, "test"
        ))

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

        request = gateway_pb2.GetStrategyDetailsRequest(strategy_id="nonexistent")
        response = await dashboard_service.GetStrategyDetails(request, mock_context)

        mock_context.set_code.assert_called_with(grpc.StatusCode.NOT_FOUND)

    @pytest.mark.asyncio
    async def test_get_details_success(self, dashboard_service, mock_context, temp_strategies_dir):
        """Test getting details for existing strategy."""
        dashboard_service._initialized = True
        dashboard_service._strategies_root = temp_strategies_dir

        request = gateway_pb2.GetStrategyDetailsRequest(
            strategy_id="test_strategy",
            include_timeline=False,
        )
        response = await dashboard_service.GetStrategyDetails(request, mock_context)

        assert response.summary.strategy_id == "test_strategy"
        assert response.summary.chain == "arbitrum"

    @pytest.mark.asyncio
    async def test_get_details_invalid_strategy_id(self, dashboard_service, mock_context):
        """Test validation of strategy_id."""
        dashboard_service._initialized = True
        dashboard_service._strategies_root = Path("/nonexistent")

        # Empty strategy_id
        request = gateway_pb2.GetStrategyDetailsRequest(strategy_id="")
        response = await dashboard_service.GetStrategyDetails(request, mock_context)

        mock_context.set_code.assert_called_with(grpc.StatusCode.INVALID_ARGUMENT)


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
            strategy_id="test_strategy",
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
                strategy_id="test_strategy",
                timestamp=datetime.now(UTC),
                event_type="TRADE",
                description="Test trade",
                tx_hash="0xabc123",
                chain="arbitrum",
            )
        )

        request = gateway_pb2.GetTimelineRequest(
            strategy_id="test_strategy",
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
                strategy_id="test_strategy",
                timestamp=datetime.now(UTC),
                event_type="TRADE",
                description="Trade event",
            )
        )
        store.add_event(
            TimelineEvent(
                event_id=str(uuid4()),
                strategy_id="test_strategy",
                timestamp=datetime.now(UTC),
                event_type="ERROR",
                description="Error event",
            )
        )

        request = gateway_pb2.GetTimelineRequest(
            strategy_id="test_strategy",
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

        request = gateway_pb2.GetStrategyConfigRequest(strategy_id="test_strategy")
        response = await dashboard_service.GetStrategyConfig(request, mock_context)

        assert response.strategy_id == "test_strategy"
        assert response.strategy_name == "Test Strategy"

        config = json.loads(response.config_json)
        assert config["chain"] == "arbitrum"
        assert config["protocol"] == "Uniswap V3"

    @pytest.mark.asyncio
    async def test_get_config_not_found(self, dashboard_service, mock_context, temp_strategies_dir):
        """Test getting config for non-existent strategy."""
        dashboard_service._initialized = True
        dashboard_service._strategies_root = temp_strategies_dir

        request = gateway_pb2.GetStrategyConfigRequest(strategy_id="nonexistent")
        response = await dashboard_service.GetStrategyConfig(request, mock_context)

        mock_context.set_code.assert_called_with(grpc.StatusCode.NOT_FOUND)


class TestGetStrategyState:
    """Tests for GetStrategyState RPC."""

    @pytest.mark.asyncio
    async def test_get_state_no_state_manager(self, dashboard_service, mock_context):
        """Test getting state when state manager not available."""
        dashboard_service._initialized = True
        dashboard_service._state_manager = None

        request = gateway_pb2.GetStrategyStateRequest(strategy_id="test_strategy")
        response = await dashboard_service.GetStrategyState(request, mock_context)

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

        request = gateway_pb2.GetStrategyStateRequest(strategy_id="test_strategy")
        response = await dashboard_service.GetStrategyState(request, mock_context)

        assert response.strategy_id == "test_strategy"
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
            strategy_id="test_strategy",
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
            strategy_id="test_strategy",
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
                strategy_id="test_strategy",
                action="PAUSE",
                reason="Maintenance",
            )
            response = await dashboard_service.ExecuteAction(request, mock_context)

        assert response.success is True
        assert response.action_id  # Should have an action ID

        mock_store.write_command.assert_called_once_with(
            agent_id="test_strategy",
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
                strategy_id="test_strategy",
                action="RESUME",
                reason="Ready to continue",
            )
            response = await dashboard_service.ExecuteAction(request, mock_context)

        assert response.success is True

        mock_store.write_command.assert_called_once_with(
            agent_id="test_strategy",
            command="RESUME",
            issued_by="dashboard:Ready to continue",
        )

    @pytest.mark.asyncio
    async def test_execute_unknown_action(self, dashboard_service, mock_context):
        """Test executing an unknown action."""
        dashboard_service._initialized = True

        request = gateway_pb2.ExecuteActionRequest(
            strategy_id="test_strategy",
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
    """Tests for protocol derivation from config."""

    def test_derive_protocol_from_explicit_config(self, dashboard_service):
        """Test deriving protocol when explicitly set in config."""
        config = {"protocol": "Custom Protocol"}
        result = dashboard_service._derive_protocol_from_config(config, "test")
        assert result == "Custom Protocol"

    def test_derive_protocol_from_pool_config(self, dashboard_service):
        """Test deriving Uniswap from pool config."""
        config = {"pool": "0x123"}
        result = dashboard_service._derive_protocol_from_config(config, "test")
        assert result == "Uniswap V3"

    def test_derive_protocol_from_strategy_id(self, dashboard_service):
        """Test deriving protocol from strategy ID."""
        assert dashboard_service._derive_protocol_from_config({}, "uniswap_lp") == "Uniswap V3"
        assert dashboard_service._derive_protocol_from_config({}, "aave_lending") == "Aave V3"
        assert dashboard_service._derive_protocol_from_config({}, "gmx_perps") == "GMX V2"
        assert dashboard_service._derive_protocol_from_config({}, "pancake_swap") == "PancakeSwap V3"
        assert dashboard_service._derive_protocol_from_config({}, "aerodrome_lp") == "Aerodrome"
        assert dashboard_service._derive_protocol_from_config({}, "tj_liquidity") == "TraderJoe V2"
        assert dashboard_service._derive_protocol_from_config({}, "unknown_strategy") == "Unknown"
