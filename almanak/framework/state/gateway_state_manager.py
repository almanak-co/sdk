"""Gateway-backed StateManager implementation.

This module provides a StateManager that persists state through the gateway
sidecar instead of directly accessing the database. Used in strategy containers
that have no access to database credentials.
"""

import json
import logging
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from almanak.framework.gateway_client import GatewayClient
from almanak.framework.state.state_manager import StateData
from almanak.gateway.proto import gateway_pb2

if TYPE_CHECKING:
    from almanak.framework.state.portfolio import PortfolioSnapshot  # type: ignore[import-untyped]

logger = logging.getLogger(__name__)


class GatewayStateManager:
    """StateManager that persists state through the gateway.

    This implementation routes all state operations to the gateway sidecar,
    which has access to the actual storage backends (PostgreSQL, SQLite).

    The interface mirrors the standard StateManager but works via gRPC.

    Example:
        from almanak.framework.gateway_client import GatewayClient
        from almanak.framework.state.gateway_state_manager import GatewayStateManager

        with GatewayClient() as client:
            state_manager = GatewayStateManager(client)
            state = await state_manager.load_state("my-strategy")
            if state:
                print(f"Loaded state version {state.version}")
    """

    def __init__(self, client: GatewayClient, timeout: float = 30.0):
        """Initialize gateway-backed state manager.

        Args:
            client: Connected GatewayClient instance
            timeout: RPC timeout in seconds
        """
        self._client = client
        self._timeout = timeout

    async def initialize(self) -> None:
        """Initialize the state manager.

        For the gateway-backed version, this is a no-op since the actual
        initialization happens in the gateway.
        """
        logger.debug("Gateway state manager initialized (no-op)")

    async def close(self) -> None:
        """Close the state manager.

        For the gateway-backed version, this is a no-op since the actual
        cleanup happens in the gateway.
        """
        logger.debug("Gateway state manager closed (no-op)")

    async def load_state(self, strategy_id: str) -> StateData | None:
        """Load strategy state from gateway.

        Args:
            strategy_id: Unique strategy identifier

        Returns:
            StateData if found, None if not found

        Raises:
            StateError: If gateway request fails
        """
        try:
            request = gateway_pb2.LoadStateRequest(strategy_id=strategy_id)
            response = self._client.state.LoadState(request, timeout=self._timeout)

            if not response.strategy_id:
                return None

            # Deserialize state from JSON bytes
            state_dict = json.loads(response.data.decode("utf-8"))

            return StateData(
                strategy_id=response.strategy_id,
                version=response.version,
                state=state_dict,
                schema_version=response.schema_version,
                checksum=response.checksum or "",
                created_at=datetime.fromtimestamp(response.created_at, tz=UTC)
                if response.created_at
                else datetime.now(UTC),
            )

        except Exception as e:
            error_msg = str(e)
            # NOT_FOUND is expected for new strategies
            if "NOT_FOUND" in error_msg:
                return None

            logger.error(f"Gateway load state failed for {strategy_id}: {error_msg}")
            raise

    async def save_state(self, state: StateData, expected_version: int | None = None) -> StateData:
        """Save strategy state through gateway.

        Uses optimistic locking: if expected_version is provided, the save
        will fail if the current version doesn't match.

        Args:
            state: State data to save
            expected_version: Expected current version for CAS semantics

        Returns:
            Updated StateData with new version

        Raises:
            StateConflictError: If version conflict (CAS failure)
            StateError: If gateway request fails
        """
        try:
            # Serialize state to JSON bytes
            state_bytes = json.dumps(state.state).encode("utf-8")

            request = gateway_pb2.SaveStateRequest(
                strategy_id=state.strategy_id,
                expected_version=expected_version or 0,
                data=state_bytes,
                schema_version=state.schema_version,
            )
            response = self._client.state.SaveState(request, timeout=self._timeout)

            if not response.success:
                error_msg = response.error or "Unknown save error"

                # Check for version conflict
                if "version" in error_msg.lower() or "conflict" in error_msg.lower():
                    from almanak.framework.state.state_manager import StateConflictError

                    raise StateConflictError(
                        strategy_id=state.strategy_id,
                        expected_version=expected_version or 0,
                        actual_version=response.new_version,
                    )

                raise RuntimeError(f"State save failed: {error_msg}")

            # Return updated state with new version
            return StateData(
                strategy_id=state.strategy_id,
                version=response.new_version,
                state=state.state,
                schema_version=state.schema_version,
                checksum=response.checksum or "",
                created_at=state.created_at,
            )

        except Exception as e:
            if "StateConflictError" in type(e).__name__:
                raise
            logger.error(f"Gateway save state failed for {state.strategy_id}: {e}")
            raise

    async def delete_state(self, strategy_id: str) -> bool:
        """Delete strategy state through gateway.

        Args:
            strategy_id: Unique strategy identifier

        Returns:
            True if deleted, False if not found
        """
        try:
            request = gateway_pb2.DeleteStateRequest(strategy_id=strategy_id)
            response = self._client.state.DeleteState(request, timeout=self._timeout)

            return response.success

        except Exception as e:
            logger.error(f"Gateway delete state failed for {strategy_id}: {e}")
            raise

    def invalidate_hot_cache(self, strategy_id: str | None = None) -> None:
        """Invalidate hot cache.

        For the gateway-backed version, this is a no-op since caching
        is handled in the gateway.

        Args:
            strategy_id: Strategy to invalidate, or None for all
        """
        logger.debug(f"Cache invalidation requested for {strategy_id or 'all'} (no-op)")

    async def save_portfolio_snapshot(self, snapshot: "PortfolioSnapshot") -> int:
        """Save portfolio snapshot.

        For the gateway-backed version, this is currently a no-op stub.
        Portfolio snapshots are not yet supported via gRPC.

        Args:
            snapshot: Portfolio snapshot to save

        Returns:
            Snapshot ID (placeholder value)
        """
        logger.debug(
            f"Portfolio snapshot save requested (not yet supported via gateway), strategy={snapshot.strategy_id}"
        )
        # Return a placeholder ID - feature not yet implemented in gateway
        return 0
