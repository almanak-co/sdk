"""StateService implementation - handles strategy state persistence.

This service provides state persistence for strategy containers via gRPC.
All state storage backends (PostgreSQL, SQLite) are accessed here in
the gateway; strategy containers only see the state data.
"""

from __future__ import annotations

import json
import logging
from datetime import UTC, datetime
from typing import TYPE_CHECKING

import grpc

from almanak.framework.state.state_manager import StateNotFoundError

if TYPE_CHECKING:
    from almanak.framework.state.state_manager import StateManager
from almanak.gateway.core.settings import GatewaySettings
from almanak.gateway.proto import gateway_pb2, gateway_pb2_grpc
from almanak.gateway.validation import (
    ValidationError,
    validate_state_size,
    validate_strategy_id,
)

logger = logging.getLogger(__name__)


class StateServiceServicer(gateway_pb2_grpc.StateServiceServicer):
    """Implements StateService gRPC interface.

    Provides state persistence for strategy containers:
    - LoadState: Load strategy state from tiered storage
    - SaveState: Save strategy state with optimistic locking
    - DeleteState: Delete strategy state
    """

    def __init__(self, settings: GatewaySettings):
        """Initialize StateService.

        Args:
            settings: Gateway settings with database configuration.
        """
        self.settings = settings
        self._state_manager: StateManager | None = None
        self._initialized = False

    async def _ensure_initialized(self) -> None:
        """Lazy initialization of state manager."""
        if self._initialized:
            return

        from almanak.framework.state.state_manager import (
            StateManager,
            StateManagerConfig,
            WarmBackendType,
        )

        # Use SQLite for local development, PostgreSQL for production
        if self.settings.database_url:
            backend_type = WarmBackendType.POSTGRESQL
            config = StateManagerConfig(
                warm_backend=backend_type,
                database_url=self.settings.database_url,
            )
        else:
            backend_type = WarmBackendType.SQLITE
            config = StateManagerConfig(warm_backend=backend_type)

        self._state_manager = StateManager(config)
        await self._state_manager.initialize()

        self._initialized = True
        logger.debug(f"StateService initialized with {backend_type.name} backend")

    async def LoadState(
        self,
        request: gateway_pb2.LoadStateRequest,
        context: grpc.aio.ServicerContext,
    ) -> gateway_pb2.StateData:
        """Load strategy state from persistence.

        Args:
            request: Load request with strategy_id
            context: gRPC context

        Returns:
            StateData with state bytes, version, checksum
        """
        # Validate strategy_id format BEFORE initialization
        try:
            strategy_id = validate_strategy_id(request.strategy_id)
        except ValidationError as e:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(str(e))
            return gateway_pb2.StateData()

        await self._ensure_initialized()
        assert self._state_manager is not None

        try:
            state = await self._state_manager.load_state(strategy_id)

            if state is None:
                context.set_code(grpc.StatusCode.NOT_FOUND)
                context.set_details(f"State not found for strategy: {strategy_id}")
                return gateway_pb2.StateData()

            # Serialize state dict to JSON bytes
            state_bytes = json.dumps(state.state).encode("utf-8")

            # Convert StateTier enum to string for protobuf
            loaded_from_str = state.loaded_from.name if state.loaded_from else "warm"

            return gateway_pb2.StateData(
                strategy_id=state.strategy_id,
                version=state.version,
                data=state_bytes,
                schema_version=state.schema_version,
                checksum=state.checksum or "",
                created_at=int(state.created_at.timestamp()) if state.created_at else 0,
                updated_at=int(datetime.now(UTC).timestamp()),
                loaded_from=loaded_from_str,
            )
        except StateNotFoundError:
            # New strategy with no state yet - this is expected
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details(f"State not found for strategy: {strategy_id}")
            return gateway_pb2.StateData()
        except Exception as e:
            logger.error(f"LoadState failed for {strategy_id}: {e}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
            return gateway_pb2.StateData()

    async def SaveState(
        self,
        request: gateway_pb2.SaveStateRequest,
        context: grpc.aio.ServicerContext,
    ) -> gateway_pb2.SaveStateResponse:
        """Save strategy state with optimistic locking.

        Args:
            request: Save request with strategy_id, expected_version, data
            context: gRPC context

        Returns:
            SaveStateResponse with success, new_version, checksum
        """
        # Validate inputs BEFORE initialization
        try:
            strategy_id = validate_strategy_id(request.strategy_id)
        except ValidationError as e:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(str(e))
            return gateway_pb2.SaveStateResponse(success=False, error=str(e))

        try:
            validate_state_size(request.data)
        except ValidationError as e:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(str(e))
            return gateway_pb2.SaveStateResponse(success=False, error=str(e))

        await self._ensure_initialized()

        try:
            # Deserialize state from JSON bytes
            state_dict = json.loads(request.data.decode("utf-8"))

            from almanak.framework.state.state_manager import StateData as FrameworkStateData

            # Create state data object
            state = FrameworkStateData(
                strategy_id=strategy_id,
                version=request.expected_version,
                state=state_dict,
                schema_version=request.schema_version or 1,
            )

            # expected_version of 0 means new state (no version check)
            expected_version = request.expected_version if request.expected_version > 0 else None

            assert self._state_manager is not None
            saved_state = await self._state_manager.save_state(state, expected_version)

            return gateway_pb2.SaveStateResponse(
                success=True,
                new_version=saved_state.version,
                checksum=saved_state.checksum or "",
            )

        except Exception as e:
            error_msg = str(e)
            logger.error(f"SaveState failed for {strategy_id}: {error_msg}")

            # Check for version conflict
            if "version" in error_msg.lower() or "conflict" in error_msg.lower():
                context.set_code(grpc.StatusCode.ABORTED)
            else:
                context.set_code(grpc.StatusCode.INTERNAL)

            context.set_details(error_msg)
            return gateway_pb2.SaveStateResponse(success=False, error=error_msg)

    async def DeleteState(
        self,
        request: gateway_pb2.DeleteStateRequest,
        context: grpc.aio.ServicerContext,
    ) -> gateway_pb2.DeleteStateResponse:
        """Delete strategy state.

        Args:
            request: Delete request with strategy_id
            context: gRPC context

        Returns:
            DeleteStateResponse with success status
        """
        # Validate strategy_id format BEFORE initialization
        try:
            strategy_id = validate_strategy_id(request.strategy_id)
        except ValidationError as e:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(str(e))
            return gateway_pb2.DeleteStateResponse(success=False, error=str(e))

        await self._ensure_initialized()
        assert self._state_manager is not None

        try:
            success = await self._state_manager.delete_state(strategy_id)

            if not success:
                return gateway_pb2.DeleteStateResponse(
                    success=False,
                    error=f"State not found for strategy: {strategy_id}",
                )

            return gateway_pb2.DeleteStateResponse(success=True)

        except Exception as e:
            error_msg = str(e)
            logger.error(f"DeleteState failed for {strategy_id}: {error_msg}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(error_msg)
            return gateway_pb2.DeleteStateResponse(success=False, error=error_msg)
