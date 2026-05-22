"""Tests for StateService identity handling — VIB-4722 / blueprint 29.

Per blueprint 29 the gateway performs NO identity translation: LoadState,
SaveState, and DeleteState filter the caller-supplied ``deployment_id`` (the
canonical ``deployment_id`` resolved once at runner boot) directly. There is
no ``resolve_deployment_id`` rewrite and no original-key fallback — a zero-row
read genuinely means the deployment has no such state.
"""

import json
from unittest.mock import AsyncMock, MagicMock

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
    return StateServiceServicer(settings)


class TestStateServiceIdentityPassThrough:
    """The StateService keys state ops on the wire deployment_id verbatim."""

    @pytest.mark.asyncio
    async def test_load_state_uses_wire_id(self, state_service, mock_context):
        """LoadState queries with the exact wire deployment_id — no translation."""
        mock_sm = AsyncMock()
        mock_sm.load_state.return_value = MagicMock(
            deployment_id="deployment:abc123def456",
            version=1,
            state={"balance": 100},
            schema_version=1,
            checksum="abc",
            created_at=None,
            loaded_from=None,
        )
        state_service._state_manager = mock_sm
        state_service._initialized = True

        request = gateway_pb2.LoadStateRequest(deployment_id="deployment:abc123def456")
        await state_service.LoadState(request, mock_context)

        # Exactly one lookup, with the wire id — no translation, no fallback.
        mock_sm.load_state.assert_called_once_with("deployment:abc123def456")

    @pytest.mark.asyncio
    async def test_save_state_uses_wire_id(self, state_service, mock_context):
        """SaveState stores under the wire deployment_id verbatim."""
        mock_sm = AsyncMock()
        mock_sm.save_state.return_value = MagicMock(version=2, checksum="def")
        state_service._state_manager = mock_sm
        state_service._initialized = True

        state_bytes = json.dumps({"balance": 200}).encode("utf-8")
        request = gateway_pb2.SaveStateRequest(
            deployment_id="deployment:abc123def456",
            expected_version=1,
            data=state_bytes,
            schema_version=1,
        )
        response = await state_service.SaveState(request, mock_context)

        assert response.success
        saved = mock_sm.save_state.call_args[0][0]
        assert saved.deployment_id == "deployment:abc123def456"

    @pytest.mark.asyncio
    async def test_delete_state_uses_wire_id(self, state_service, mock_context):
        """DeleteState targets the wire deployment_id verbatim."""
        mock_sm = AsyncMock()
        mock_sm.delete_state.return_value = True
        state_service._state_manager = mock_sm
        state_service._initialized = True

        request = gateway_pb2.DeleteStateRequest(deployment_id="deployment:abc123def456")
        response = await state_service.DeleteState(request, mock_context)

        assert response.success
        mock_sm.delete_state.assert_called_once_with("deployment:abc123def456")

    @pytest.mark.asyncio
    async def test_load_state_no_fallback_on_miss(self, state_service, mock_context):
        """A LoadState miss is exactly one lookup — no legacy-key fallback.

        Blueprint 29 §4: the gateway no longer translates identity, so a
        zero-row read genuinely means no state. The old hosted-env fallback
        double-lookup is removed.
        """
        mock_sm = AsyncMock()
        mock_sm.load_state.return_value = None
        state_service._state_manager = mock_sm
        state_service._initialized = True

        request = gateway_pb2.LoadStateRequest(deployment_id="deployment:abc123def456")
        await state_service.LoadState(request, mock_context)

        mock_sm.load_state.assert_called_once_with("deployment:abc123def456")

    @pytest.mark.asyncio
    async def test_hosted_id_passes_through(self, state_service, mock_context, monkeypatch):
        """A hosted platform deployment id passes through unchanged."""
        monkeypatch.setenv("ALMANAK_IS_HOSTED", "true")
        monkeypatch.setenv("ALMANAK_DEPLOYMENT_ID", "platform-agent-1234")

        mock_sm = AsyncMock()
        mock_sm.load_state.return_value = None
        state_service._state_manager = mock_sm
        state_service._initialized = True

        request = gateway_pb2.LoadStateRequest(deployment_id="platform-agent-1234")
        await state_service.LoadState(request, mock_context)

        mock_sm.load_state.assert_called_once_with("platform-agent-1234")
