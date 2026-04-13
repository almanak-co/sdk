"""Gateway-backed StateManager implementation.

This module provides a StateManager that persists state through the gateway
sidecar instead of directly accessing the database. Used in strategy containers
that have no access to database credentials.

Portfolio snapshots are persisted via gateway gRPC (SavePortfolioSnapshot,
GetLatestSnapshot, GetSnapshotsSince) which routes to PostgreSQL in deployed
mode.  Portfolio metrics (PnL baseline) are persisted via SavePortfolioMetrics
and GetPortfolioMetrics.  Local mode uses the regular StateManager with
SQLiteStore.
"""

import json
import logging
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from almanak.framework.gateway_client import GatewayClient
from almanak.framework.state.state_manager import StateData
from almanak.gateway.proto import gateway_pb2

if TYPE_CHECKING:
    from almanak.framework.portfolio.models import PortfolioMetrics
    from almanak.framework.state.portfolio import PortfolioSnapshot

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
        """Close the state manager."""
        logger.debug("Gateway state manager closed")

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
            state_bytes = json.dumps(state.state, default=str, sort_keys=True).encode("utf-8")

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
        """Save portfolio snapshot via gateway gRPC → PostgreSQL.

        Args:
            snapshot: Portfolio snapshot to save

        Returns:
            Snapshot ID from the database
        """
        try:
            # Pack positions, token_prices, and wallet_balances into the
            # positions_json envelope. The state_service on the receiving end
            # unpacks this and persists each field to its own column.
            payload = snapshot.to_positions_payload()
            if isinstance(payload, list):
                # Convert bare list to envelope so we can attach extra data
                payload = {"positions": payload, "metadata": {}}
            # Attach accounting data to the envelope
            if snapshot.token_prices:
                payload["token_prices"] = snapshot.token_prices
            if snapshot.wallet_balances:
                payload["wallet_balances"] = [
                    {
                        "symbol": b.symbol,
                        "balance": str(b.balance),
                        "value_usd": str(b.value_usd),
                        "address": b.address,
                        "price_usd": str(b.price_usd) if b.price_usd is not None else None,
                    }
                    for b in snapshot.wallet_balances
                ]

            positions_bytes = json.dumps(payload, default=str, sort_keys=True).encode("utf-8")

            request = gateway_pb2.SaveSnapshotRequest(
                strategy_id=snapshot.strategy_id,
                timestamp=int(snapshot.timestamp.timestamp()),
                iteration_number=snapshot.iteration_number,
                total_value_usd=str(snapshot.total_value_usd),
                available_cash_usd=str(snapshot.available_cash_usd),
                value_confidence=snapshot.value_confidence.value,
                positions_json=positions_bytes,
                chain=snapshot.chain or "",
            )
            response = self._client.state.SavePortfolioSnapshot(request, timeout=self._timeout)

            if not response.success:
                logger.error("SavePortfolioSnapshot failed: %s", response.error)
                return 0

            logger.debug(
                "Portfolio snapshot saved via gateway: strategy=%s, value=$%.2f, confidence=%s",
                snapshot.strategy_id,
                snapshot.total_value_usd,
                snapshot.value_confidence.value,
            )
            return response.snapshot_id
        except Exception:
            logger.exception("Failed to save portfolio snapshot via gateway")
            return 0

    async def get_latest_snapshot(self, strategy_id: str) -> "PortfolioSnapshot | None":
        """Get most recent portfolio snapshot via gateway gRPC."""
        try:
            request = gateway_pb2.GetLatestSnapshotRequest(strategy_id=strategy_id)
            response = self._client.state.GetLatestSnapshot(request, timeout=self._timeout)

            if not response.found:
                return None

            return self._proto_to_snapshot(response)
        except Exception as e:
            logger.debug("Failed to get latest snapshot via gateway: %s", e)
            return None

    async def get_snapshots_since(
        self, strategy_id: str, since: datetime, limit: int = 168
    ) -> list["PortfolioSnapshot"]:
        """Get portfolio snapshots since a given time via gateway gRPC."""
        try:
            request = gateway_pb2.GetSnapshotsSinceRequest(
                strategy_id=strategy_id,
                since=int(since.timestamp()),
                limit=limit,
            )
            response = self._client.state.GetSnapshotsSince(request, timeout=self._timeout)

            return [self._proto_to_snapshot(s) for s in response.snapshots if s.found]
        except Exception as e:
            logger.debug("Failed to get snapshots via gateway: %s", e)
            return []

    async def save_portfolio_metrics(self, metrics: "PortfolioMetrics") -> bool:
        """Save portfolio metrics via gateway gRPC.

        Args:
            metrics: PortfolioMetrics to persist.

        Returns:
            True if save succeeded.
        """
        try:
            request = gateway_pb2.SaveMetricsRequest(
                strategy_id=metrics.strategy_id,
                initial_value_usd=str(metrics.initial_value_usd),
                initial_timestamp=int(metrics.timestamp.timestamp()),
                deposits_usd=str(metrics.deposits_usd),
                withdrawals_usd=str(metrics.withdrawals_usd),
                gas_spent_usd=str(metrics.gas_spent_usd),
            )
            response = self._client.state.SavePortfolioMetrics(request, timeout=self._timeout)

            if not response.success:
                logger.error("SavePortfolioMetrics failed: %s", response.error)
                return False

            logger.debug("Portfolio metrics saved via gateway for strategy=%s", metrics.strategy_id)
            return True
        except Exception:
            logger.exception("Failed to save portfolio metrics via gateway")
            return False

    async def get_portfolio_metrics(self, strategy_id: str) -> "PortfolioMetrics | None":
        """Get portfolio metrics via gateway gRPC.

        Args:
            strategy_id: Strategy identifier.

        Returns:
            PortfolioMetrics or None if not found.
        """
        from decimal import Decimal

        from almanak.framework.portfolio.models import PortfolioMetrics

        try:
            request = gateway_pb2.GetMetricsRequest(strategy_id=strategy_id)
            response = self._client.state.GetPortfolioMetrics(request, timeout=self._timeout)

            if not response.found:
                return None

            return PortfolioMetrics(
                strategy_id=response.strategy_id,
                timestamp=datetime.fromtimestamp(response.updated_at, tz=UTC)
                if response.updated_at
                else datetime.now(UTC),
                total_value_usd=Decimal("0"),  # Not stored in metrics, get from latest snapshot
                initial_value_usd=Decimal(response.initial_value_usd or "0"),
                deposits_usd=Decimal(response.deposits_usd or "0"),
                withdrawals_usd=Decimal(response.withdrawals_usd or "0"),
                gas_spent_usd=Decimal(response.gas_spent_usd or "0"),
            )
        except Exception as e:
            logger.debug("Failed to get portfolio metrics via gateway: %s", e)
            return None

    @staticmethod
    def _proto_to_snapshot(data: gateway_pb2.SnapshotData) -> "PortfolioSnapshot":
        """Convert a SnapshotData protobuf message to a PortfolioSnapshot."""
        from almanak.framework.portfolio.models import PortfolioSnapshot

        positions_payload = json.loads(data.positions_json.decode("utf-8")) if data.positions_json else []
        positions_list, snapshot_metadata = PortfolioSnapshot.unpack_positions_payload(positions_payload)

        # Extract accounting data from envelope (Phase 1c)
        token_prices: dict = {}
        wallet_balances_raw: list[dict] = []
        if isinstance(positions_payload, dict):
            token_prices = positions_payload.get("token_prices", {})
            wallet_balances_raw = positions_payload.get("wallet_balances", [])

        snapshot_dict = {
            "timestamp": datetime.fromtimestamp(data.timestamp, tz=UTC).isoformat(),
            "strategy_id": data.strategy_id,
            "total_value_usd": data.total_value_usd or "0",
            "available_cash_usd": data.available_cash_usd or "0",
            "value_confidence": data.value_confidence or "HIGH",
            "error": None,
            "positions": positions_list,
            "wallet_balances": wallet_balances_raw,
            "token_prices": token_prices,
            "chain": data.chain or "",
            "iteration_number": data.iteration_number,
            "snapshot_metadata": snapshot_metadata,
        }

        return PortfolioSnapshot.from_dict(snapshot_dict)
