"""Tests for AGENT_ID normalization in StateService.

Verifies that LoadState, SaveState, and DeleteState use the platform
AGENT_ID (when set) to access state, ensuring deployed dashboards can
read state written by the SDK runner.
"""

import json
from unittest.mock import AsyncMock, MagicMock, patch

import grpc
import pytest

from almanak.gateway.core.settings import GatewaySettings
from almanak.gateway.proto import gateway_pb2
from almanak.gateway.services.state_service import StateServiceServicer


@pytest.fixture
def settings():
    return GatewaySettings()


@pytest.fixture
def mock_context():
    return MagicMock(spec=grpc.aio.ServicerContext)


@pytest.fixture
def state_service(settings):
    service = StateServiceServicer(settings)
    return service


class TestStateServiceAgentIdResolution:
    """Verify AGENT_ID normalization at the StateService boundary."""

    @pytest.mark.asyncio
    async def test_load_state_resolves_agent_id(self, state_service, mock_context, monkeypatch):
        """LoadState queries with AGENT_ID when env var is set."""
        monkeypatch.setenv("AGENT_ID", "platform-uuid-1234")

        mock_sm = AsyncMock()
        mock_sm.load_state.return_value = MagicMock(
            strategy_id="platform-uuid-1234",
            version=1,
            state={"balance": 100},
            schema_version=1,
            checksum="abc",
            created_at=None,
            loaded_from=None,
        )
        state_service._state_manager = mock_sm
        state_service._initialized = True

        request = gateway_pb2.LoadStateRequest(strategy_id="uniswap_rsi:abc123")
        await state_service.LoadState(request, mock_context)

        # StateManager was called with the resolved AGENT_ID, not the SDK strategy_id
        mock_sm.load_state.assert_called_once_with("platform-uuid-1234")

    @pytest.mark.asyncio
    async def test_save_state_resolves_agent_id(self, state_service, mock_context, monkeypatch):
        """SaveState stores under AGENT_ID when env var is set."""
        monkeypatch.setenv("AGENT_ID", "platform-uuid-1234")

        mock_sm = AsyncMock()
        mock_sm.save_state.return_value = MagicMock(version=2, checksum="def")
        state_service._state_manager = mock_sm
        state_service._initialized = True

        state_bytes = json.dumps({"balance": 200}).encode("utf-8")
        request = gateway_pb2.SaveStateRequest(
            strategy_id="uniswap_rsi:abc123",
            expected_version=1,
            data=state_bytes,
            schema_version=1,
        )
        response = await state_service.SaveState(request, mock_context)

        assert response.success
        # The saved state should have strategy_id = AGENT_ID
        saved = mock_sm.save_state.call_args[0][0]
        assert saved.strategy_id == "platform-uuid-1234"

    @pytest.mark.asyncio
    async def test_delete_state_resolves_agent_id(self, state_service, mock_context, monkeypatch):
        """DeleteState targets AGENT_ID when env var is set."""
        monkeypatch.setenv("AGENT_ID", "platform-uuid-1234")

        mock_sm = AsyncMock()
        mock_sm.delete_state.return_value = True
        state_service._state_manager = mock_sm
        state_service._initialized = True

        request = gateway_pb2.DeleteStateRequest(strategy_id="uniswap_rsi:abc123")
        response = await state_service.DeleteState(request, mock_context)

        assert response.success
        mock_sm.delete_state.assert_called_once_with("platform-uuid-1234")

    @pytest.mark.asyncio
    async def test_load_state_falls_back_to_legacy_key(self, state_service, mock_context, monkeypatch):
        """LoadState tries the original strategy_id if AGENT_ID lookup returns nothing.

        This covers the upgrade path: warm state written before this PR lives
        under the SDK strategy_id. After upgrade, LoadState resolves to AGENT_ID
        first but falls back to the legacy key so the strategy doesn't cold-start.
        """
        monkeypatch.setenv("AGENT_ID", "platform-uuid-1234")

        legacy_state = MagicMock(
            strategy_id="uniswap_rsi:abc123",
            version=5,
            state={"balance": 999},
            schema_version=1,
            checksum="legacy",
            created_at=None,
            loaded_from=None,
        )

        async def _load_side_effect(sid):
            if sid == "platform-uuid-1234":
                return None  # no state under AGENT_ID yet
            if sid == "uniswap_rsi:abc123":
                return legacy_state  # legacy state exists under SDK key
            return None

        mock_sm = AsyncMock()
        mock_sm.load_state.side_effect = _load_side_effect
        state_service._state_manager = mock_sm
        state_service._initialized = True

        request = gateway_pb2.LoadStateRequest(strategy_id="uniswap_rsi:abc123")
        response = await state_service.LoadState(request, mock_context)

        # Should have tried AGENT_ID first, then fallen back to legacy key
        assert mock_sm.load_state.call_count == 2
        mock_sm.load_state.assert_any_call("platform-uuid-1234")
        mock_sm.load_state.assert_any_call("uniswap_rsi:abc123")
        # Should have returned the legacy state successfully
        assert response.version == 5

    @pytest.mark.asyncio
    async def test_local_mode_passes_through(self, state_service, mock_context, monkeypatch):
        """Without AGENT_ID, strategy_id passes through unchanged."""
        monkeypatch.delenv("AGENT_ID", raising=False)

        mock_sm = AsyncMock()
        mock_sm.load_state.return_value = None
        state_service._state_manager = mock_sm
        state_service._initialized = True

        request = gateway_pb2.LoadStateRequest(strategy_id="uniswap_rsi:abc123")
        await state_service.LoadState(request, mock_context)

        mock_sm.load_state.assert_called_once_with("uniswap_rsi:abc123")
