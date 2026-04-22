"""StateService implementation - handles strategy state persistence.

This service provides state persistence for strategy containers via gRPC.
All state storage backends (PostgreSQL, SQLite) are accessed here in
the gateway; strategy containers only see the state data.

Portfolio snapshots are persisted to the ``portfolio_snapshots`` table
(PostgreSQL in deployed mode, SQLite in local dev) and exposed via three
RPCs: SavePortfolioSnapshot, GetLatestSnapshot, GetSnapshotsSince.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import uuid
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

import grpc

from almanak.framework.state.state_manager import StateNotFoundError

if TYPE_CHECKING:
    import asyncpg

    from almanak.framework.state.state_manager import StateManager
from almanak.gateway.core.settings import GatewaySettings
from almanak.gateway.proto import gateway_pb2, gateway_pb2_grpc
from almanak.gateway.validation import (
    ValidationError,
    resolve_agent_id,
    validate_state_size,
    validate_strategy_id,
)

logger = logging.getLogger(__name__)

# Upper bound on snapshot queries to prevent unbounded materialisation
MAX_SNAPSHOTS = 1000


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
        self._snapshot_pool: asyncpg.Pool | None = None
        self._snapshot_pool_initialized = False
        self._snapshot_pool_lock = asyncio.Lock()
        self._snapshot_schema: str | None = None

    async def _ensure_initialized(self) -> None:
        """Lazy initialization of state manager."""
        if self._initialized:
            return

        from almanak.framework.state.state_manager import (
            SQLiteConfigLight,
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
            db_path = os.environ.get("ALMANAK_STATE_DB", "./almanak_state.db")
            config = StateManagerConfig(
                warm_backend=backend_type,
                sqlite_config=SQLiteConfigLight(db_path=db_path),
            )

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

        # In deployed mode, use platform AGENT_ID for consistent data access
        original_strategy_id = strategy_id
        strategy_id = resolve_agent_id(strategy_id)

        await self._ensure_initialized()
        assert self._state_manager is not None

        try:
            state = await self._state_manager.load_state(strategy_id)

            # Fallback: if AGENT_ID resolved to a different key and no state was
            # found, try the original strategy_id.  This bridges legacy warm state
            # written under the SDK key before this normalization was deployed.
            if state is None and strategy_id != original_strategy_id:
                state = await self._state_manager.load_state(original_strategy_id)

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

        # In deployed mode, use platform AGENT_ID for consistent data access
        strategy_id = resolve_agent_id(strategy_id)

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

        # In deployed mode, use platform AGENT_ID for consistent data access
        strategy_id = resolve_agent_id(strategy_id)

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

    # =========================================================================
    # Portfolio Snapshot RPCs
    # =========================================================================

    async def _ensure_snapshot_pool(self) -> None:
        """Lazy-init asyncpg pool for portfolio snapshot persistence."""
        if self._snapshot_pool_initialized:
            return

        async with self._snapshot_pool_lock:
            # Re-check after acquiring lock to avoid double-init
            if self._snapshot_pool_initialized:
                return

            if not self.settings.database_url:
                self._snapshot_pool_initialized = True
                return

            import asyncpg

            # Strip ?schema= parameter (asyncpg doesn't support it)
            parsed = urlparse(self.settings.database_url)
            params = parse_qsl(parsed.query, keep_blank_values=True)
            self._snapshot_schema = next((v for k, v in params if k == "schema"), None) or None
            clean_params = [(k, v) for k, v in params if k != "schema"]
            clean_url = urlunparse(parsed._replace(query=urlencode(clean_params)))

            self._snapshot_pool = await asyncpg.create_pool(clean_url, min_size=1, max_size=2, statement_cache_size=0)
            self._snapshot_pool_initialized = True
            logger.debug("Snapshot asyncpg pool initialized")

    async def _snapshot_execute(self, query: str, *args: Any) -> str:
        """Execute a query on the snapshot pool with optional schema."""
        assert self._snapshot_pool is not None
        async with self._snapshot_pool.acquire() as conn:
            if self._snapshot_schema:
                await conn.fetchval(
                    "SELECT pg_catalog.set_config('search_path', $1, true)",
                    self._snapshot_schema,
                )
            return await conn.execute(query, *args)

    async def _snapshot_fetchrow(self, query: str, *args: Any) -> Any:
        """Fetch a single row from the snapshot pool with optional schema."""
        assert self._snapshot_pool is not None
        async with self._snapshot_pool.acquire() as conn:
            if self._snapshot_schema:
                await conn.fetchval(
                    "SELECT pg_catalog.set_config('search_path', $1, true)",
                    self._snapshot_schema,
                )
            return await conn.fetchrow(query, *args)

    async def _snapshot_fetch(self, query: str, *args: Any) -> list[Any]:
        """Fetch multiple rows from the snapshot pool with optional schema."""
        assert self._snapshot_pool is not None
        async with self._snapshot_pool.acquire() as conn:
            if self._snapshot_schema:
                await conn.fetchval(
                    "SELECT pg_catalog.set_config('search_path', $1, true)",
                    self._snapshot_schema,
                )
            return await conn.fetch(query, *args)

    async def SavePortfolioSnapshot(
        self,
        request: gateway_pb2.SaveSnapshotRequest,
        context: grpc.aio.ServicerContext,
    ) -> gateway_pb2.SaveSnapshotResponse:
        """Save a portfolio snapshot to the portfolio_snapshots table."""
        try:
            strategy_id = validate_strategy_id(request.strategy_id)
        except ValidationError as e:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(str(e))
            return gateway_pb2.SaveSnapshotResponse(success=False, error=str(e))

        # Validate payload before backend split
        if request.timestamp <= 0:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details("timestamp must be positive")
            return gateway_pb2.SaveSnapshotResponse(success=False, error="timestamp must be positive")
        if request.positions_json:
            try:
                positions_payload = json.loads(request.positions_json)
            except (json.JSONDecodeError, UnicodeDecodeError):
                context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
                context.set_details("positions_json must be valid JSON")
                return gateway_pb2.SaveSnapshotResponse(success=False, error="positions_json must be valid JSON")

            # Validate envelope shape: must be a list (legacy) or {positions: list, metadata: dict}
            is_legacy = isinstance(positions_payload, list)
            is_envelope = (
                isinstance(positions_payload, dict)
                and isinstance(positions_payload.get("positions", []), list)
                and isinstance(positions_payload.get("metadata", {}), dict)
            )
            if not (is_legacy or is_envelope):
                error = "positions_json must be a list or {positions: list, metadata: object}"
                context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
                context.set_details(error)
                return gateway_pb2.SaveSnapshotResponse(success=False, error=error)

        strategy_id = resolve_agent_id(strategy_id)
        await self._ensure_snapshot_pool()

        ts = datetime.fromtimestamp(request.timestamp, tz=UTC)
        now = datetime.now(UTC)

        if self._snapshot_pool is not None:
            # PostgreSQL mode (deployed)
            try:
                row = await self._snapshot_fetchrow(
                    """
                    INSERT INTO portfolio_snapshots (
                        agent_id, timestamp, iteration_number, total_value_usd,
                        available_cash_usd, value_confidence, positions_json, chain, created_at
                    ) VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb, $8, $9)
                    ON CONFLICT (agent_id, timestamp) DO UPDATE SET
                        iteration_number = EXCLUDED.iteration_number,
                        total_value_usd = EXCLUDED.total_value_usd,
                        available_cash_usd = EXCLUDED.available_cash_usd,
                        value_confidence = EXCLUDED.value_confidence,
                        positions_json = EXCLUDED.positions_json,
                        chain = EXCLUDED.chain
                    RETURNING id
                    """,
                    strategy_id,
                    ts,
                    request.iteration_number,
                    request.total_value_usd,
                    request.available_cash_usd,
                    request.value_confidence or "HIGH",
                    request.positions_json.decode("utf-8") if request.positions_json else "[]",
                    request.chain,
                    now,
                )
                snapshot_id = row["id"] if row else 0
                return gateway_pb2.SaveSnapshotResponse(success=True, snapshot_id=snapshot_id)
            except Exception as e:
                logger.error(f"SavePortfolioSnapshot failed for {strategy_id}: {e}")
                context.set_code(grpc.StatusCode.INTERNAL)
                context.set_details("internal server error")
                return gateway_pb2.SaveSnapshotResponse(success=False, error="internal server error")
        else:
            # SQLite mode (local dev) — delegate to StateManager's SQLiteStore
            try:
                await self._ensure_initialized()
                assert self._state_manager is not None
                warm = self._state_manager.warm_backend
                assert warm is not None
                from decimal import Decimal

                from almanak.framework.portfolio.models import PortfolioSnapshot, ValueConfidence

                snapshot = PortfolioSnapshot(
                    timestamp=ts,
                    strategy_id=strategy_id,
                    total_value_usd=Decimal(request.total_value_usd or "0"),
                    available_cash_usd=Decimal(request.available_cash_usd or "0"),
                    value_confidence=ValueConfidence(request.value_confidence or "HIGH"),
                    chain=request.chain,
                    iteration_number=request.iteration_number,
                )
                # Deserialize positions from JSON bytes
                if request.positions_json:
                    snapshot_dict = snapshot.to_dict()
                    positions_payload = json.loads(request.positions_json.decode("utf-8"))
                    positions, snapshot_metadata = PortfolioSnapshot.unpack_positions_payload(positions_payload)
                    snapshot_dict["positions"] = positions
                    snapshot_dict["snapshot_metadata"] = snapshot_metadata
                    # Extract accounting data from envelope (Phase 1c)
                    if isinstance(positions_payload, dict):
                        if "token_prices" in positions_payload:
                            snapshot_dict["token_prices"] = positions_payload["token_prices"]
                        if "wallet_balances" in positions_payload:
                            snapshot_dict["wallet_balances"] = positions_payload["wallet_balances"]
                    snapshot = PortfolioSnapshot.from_dict(snapshot_dict)

                snapshot_id = await warm.save_portfolio_snapshot(snapshot)  # type: ignore[attr-defined]
                return gateway_pb2.SaveSnapshotResponse(success=True, snapshot_id=snapshot_id)
            except Exception as e:
                logger.error(f"SavePortfolioSnapshot (SQLite) failed for {strategy_id}: {e}")
                context.set_code(grpc.StatusCode.INTERNAL)
                context.set_details("internal server error")
                return gateway_pb2.SaveSnapshotResponse(success=False, error="internal server error")

    async def GetLatestSnapshot(
        self,
        request: gateway_pb2.GetLatestSnapshotRequest,
        context: grpc.aio.ServicerContext,
    ) -> gateway_pb2.SnapshotData:
        """Get the most recent portfolio snapshot."""
        try:
            strategy_id = validate_strategy_id(request.strategy_id)
        except ValidationError as e:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(str(e))
            return gateway_pb2.SnapshotData(found=False)

        strategy_id = resolve_agent_id(strategy_id)
        await self._ensure_snapshot_pool()

        if self._snapshot_pool is not None:
            try:
                row = await self._snapshot_fetchrow(
                    """
                    SELECT agent_id, timestamp, iteration_number, total_value_usd,
                           available_cash_usd, value_confidence, positions_json, chain
                    FROM portfolio_snapshots
                    WHERE agent_id = $1
                    ORDER BY timestamp DESC
                    LIMIT 1
                    """,
                    strategy_id,
                )
                if row is None:
                    return gateway_pb2.SnapshotData(found=False)
                return self._row_to_snapshot_data(row)
            except Exception as e:
                logger.error(f"GetLatestSnapshot failed for {strategy_id}: {e}")
                context.set_code(grpc.StatusCode.INTERNAL)
                context.set_details("internal server error")
                return gateway_pb2.SnapshotData(found=False)
        else:
            try:
                await self._ensure_initialized()
                assert self._state_manager is not None
                warm = self._state_manager.warm_backend
                assert warm is not None
                snapshot = await warm.get_latest_snapshot(strategy_id)  # type: ignore[attr-defined]
                if snapshot is None:
                    return gateway_pb2.SnapshotData(found=False)
                return self._snapshot_to_proto(snapshot)
            except Exception as e:
                logger.error(f"GetLatestSnapshot (SQLite) failed for {strategy_id}: {e}")
                context.set_code(grpc.StatusCode.INTERNAL)
                context.set_details("internal server error")
                return gateway_pb2.SnapshotData(found=False)

    async def GetSnapshotsSince(
        self,
        request: gateway_pb2.GetSnapshotsSinceRequest,
        context: grpc.aio.ServicerContext,
    ) -> gateway_pb2.SnapshotList:
        """Get portfolio snapshots since a given timestamp."""
        try:
            strategy_id = validate_strategy_id(request.strategy_id)
        except ValidationError as e:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(str(e))
            return gateway_pb2.SnapshotList()

        strategy_id = resolve_agent_id(strategy_id)
        since = datetime.fromtimestamp(request.since, tz=UTC)
        limit = min(request.limit if request.limit > 0 else 168, MAX_SNAPSHOTS)
        await self._ensure_snapshot_pool()

        if self._snapshot_pool is not None:
            try:
                rows = await self._snapshot_fetch(
                    """
                    SELECT agent_id, timestamp, iteration_number, total_value_usd,
                           available_cash_usd, value_confidence, positions_json, chain
                    FROM portfolio_snapshots
                    WHERE agent_id = $1 AND timestamp >= $2
                    ORDER BY timestamp ASC
                    LIMIT $3
                    """,
                    strategy_id,
                    since,
                    limit,
                )
                return gateway_pb2.SnapshotList(snapshots=[self._row_to_snapshot_data(row) for row in rows])
            except Exception as e:
                logger.error(f"GetSnapshotsSince failed for {strategy_id}: {e}")
                context.set_code(grpc.StatusCode.INTERNAL)
                context.set_details("internal server error")
                return gateway_pb2.SnapshotList()
        else:
            try:
                await self._ensure_initialized()
                assert self._state_manager is not None
                warm = self._state_manager.warm_backend
                assert warm is not None
                snapshots = await warm.get_snapshots_since(strategy_id, since, limit)  # type: ignore[attr-defined]
                return gateway_pb2.SnapshotList(snapshots=[self._snapshot_to_proto(s) for s in snapshots])
            except Exception as e:
                logger.error(f"GetSnapshotsSince (SQLite) failed for {strategy_id}: {e}")
                context.set_code(grpc.StatusCode.INTERNAL)
                context.set_details("internal server error")
                return gateway_pb2.SnapshotList()

    @staticmethod
    def _row_to_snapshot_data(row: Any) -> gateway_pb2.SnapshotData:
        """Convert an asyncpg row to a SnapshotData protobuf message."""
        ts = row["timestamp"]
        positions_json = row["positions_json"]
        if isinstance(positions_json, str):
            positions_bytes = positions_json.encode("utf-8")
        elif isinstance(positions_json, dict | list):
            positions_bytes = json.dumps(positions_json).encode("utf-8")
        else:
            positions_bytes = b"[]"

        return gateway_pb2.SnapshotData(
            strategy_id=row["agent_id"],
            timestamp=int(ts.timestamp()) if hasattr(ts, "timestamp") else 0,
            iteration_number=row["iteration_number"] or 0,
            total_value_usd=row["total_value_usd"] or "0",
            available_cash_usd=row["available_cash_usd"] or "0",
            value_confidence=row["value_confidence"] or "HIGH",
            positions_json=positions_bytes,
            chain=row["chain"] or "",
            found=True,
        )

    @staticmethod
    def _snapshot_to_proto(snapshot: Any) -> gateway_pb2.SnapshotData:
        """Convert a PortfolioSnapshot to a SnapshotData protobuf message."""
        positions_bytes = json.dumps(snapshot.to_positions_payload()).encode("utf-8")
        return gateway_pb2.SnapshotData(
            strategy_id=snapshot.strategy_id,
            timestamp=int(snapshot.timestamp.timestamp()),
            iteration_number=snapshot.iteration_number,
            total_value_usd=str(snapshot.total_value_usd),
            available_cash_usd=str(snapshot.available_cash_usd),
            value_confidence=snapshot.value_confidence.value,
            positions_json=positions_bytes,
            chain=snapshot.chain or "",
            found=True,
        )

    # =========================================================================
    # Portfolio Metrics RPCs
    # =========================================================================

    async def SavePortfolioMetrics(
        self,
        request: gateway_pb2.SaveMetricsRequest,
        context: grpc.aio.ServicerContext,
    ) -> gateway_pb2.SaveMetricsResponse:
        """Save or update portfolio metrics (PnL baseline)."""
        from decimal import Decimal, InvalidOperation

        from almanak.framework.portfolio.models import PortfolioMetrics

        try:
            strategy_id = validate_strategy_id(request.strategy_id)
        except ValidationError as e:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(str(e))
            return gateway_pb2.SaveMetricsResponse(success=False, error=str(e))

        strategy_id = resolve_agent_id(strategy_id)

        try:
            initial_value_usd = Decimal(request.initial_value_usd or "0")
            deposits_usd = Decimal(request.deposits_usd or "0")
            withdrawals_usd = Decimal(request.withdrawals_usd or "0")
            gas_spent_usd = Decimal(request.gas_spent_usd or "0")
        except InvalidOperation:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details("metrics fields must be valid decimal strings")
            return gateway_pb2.SaveMetricsResponse(success=False, error="metrics fields must be valid decimal strings")

        if request.initial_timestamp < 0:
            error = "initial_timestamp must be non-negative"
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(error)
            return gateway_pb2.SaveMetricsResponse(success=False, error=error)

        try:
            ts = (
                datetime.fromtimestamp(request.initial_timestamp, tz=UTC)
                if request.initial_timestamp
                else datetime.now(UTC)
            )
        except (OverflowError, OSError, ValueError):
            error = "initial_timestamp is out of range"
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(error)
            return gateway_pb2.SaveMetricsResponse(success=False, error=error)

        now = datetime.now(UTC)

        await self._ensure_snapshot_pool()

        if self._snapshot_pool is not None:
            # PostgreSQL mode (deployed)
            try:
                await self._snapshot_fetchrow(
                    """
                    INSERT INTO portfolio_metrics (
                        agent_id, initial_value_usd, initial_timestamp,
                        deposits_usd, withdrawals_usd, gas_spent_usd,
                        deployment_id, cycle_id, execution_mode, is_complete,
                        updated_at
                    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
                    ON CONFLICT (agent_id) DO UPDATE SET
                        initial_value_usd = EXCLUDED.initial_value_usd,
                        initial_timestamp = EXCLUDED.initial_timestamp,
                        deposits_usd = EXCLUDED.deposits_usd,
                        withdrawals_usd = EXCLUDED.withdrawals_usd,
                        gas_spent_usd = EXCLUDED.gas_spent_usd,
                        deployment_id = EXCLUDED.deployment_id,
                        cycle_id = EXCLUDED.cycle_id,
                        execution_mode = EXCLUDED.execution_mode,
                        is_complete = EXCLUDED.is_complete,
                        updated_at = EXCLUDED.updated_at
                    RETURNING agent_id
                    """,
                    strategy_id,
                    str(initial_value_usd),
                    ts,
                    str(deposits_usd),
                    str(withdrawals_usd),
                    str(gas_spent_usd),
                    request.deployment_id or "",
                    request.cycle_id or "",
                    request.execution_mode or "",
                    request.is_complete,
                    now,
                )
                logger.debug("Portfolio metrics saved for strategy=%s", strategy_id)
                return gateway_pb2.SaveMetricsResponse(success=True)
            except Exception as e:
                logger.error("SavePortfolioMetrics failed for %s: %s", strategy_id, e)
                context.set_code(grpc.StatusCode.INTERNAL)
                context.set_details("internal server error")
                return gateway_pb2.SaveMetricsResponse(success=False, error="internal server error")
        else:
            # SQLite mode (local dev) — delegate to StateManager's SQLiteStore
            try:
                await self._ensure_initialized()
                assert self._state_manager is not None

                # Resolve total_value_usd from the latest snapshot (VIB-2765).
                # The proto doesn't carry total_value_usd, so we read it from
                # the most recent snapshot which was saved moments before this call.
                total_value_usd = Decimal("0")
                try:
                    warm_be = self._state_manager.warm_backend
                    if warm_be and hasattr(warm_be, "get_latest_snapshot"):
                        latest = await warm_be.get_latest_snapshot(strategy_id)
                        if latest is not None:
                            total_value_usd = latest.total_value_usd
                except Exception as snap_err:
                    logger.warning(
                        "Could not resolve total_value_usd from snapshot for %s: %s",
                        strategy_id,
                        snap_err,
                    )

                metrics = PortfolioMetrics(
                    strategy_id=strategy_id,
                    timestamp=ts,
                    total_value_usd=total_value_usd,
                    initial_value_usd=initial_value_usd,
                    deposits_usd=deposits_usd,
                    withdrawals_usd=withdrawals_usd,
                    gas_spent_usd=gas_spent_usd,
                    # Phase 4 accounting identity fields (VIB-2835/2837/2839)
                    deployment_id=request.deployment_id or "",
                    cycle_id=request.cycle_id or None,
                    execution_mode=request.execution_mode or "",
                    is_complete=request.is_complete,
                )

                warm = self._state_manager.warm_backend
                if warm and hasattr(warm, "save_portfolio_metrics"):
                    result = await warm.save_portfolio_metrics(metrics)
                    if result:
                        logger.debug("Portfolio metrics saved (SQLite) for strategy=%s", strategy_id)
                        return gateway_pb2.SaveMetricsResponse(success=True)
                    return gateway_pb2.SaveMetricsResponse(
                        success=False, error="Backend save_portfolio_metrics returned False"
                    )

                return gateway_pb2.SaveMetricsResponse(
                    success=False, error="No warm backend with portfolio metrics support"
                )
            except Exception as e:
                logger.error("SavePortfolioMetrics (SQLite) failed for %s: %s", strategy_id, e)
                context.set_code(grpc.StatusCode.INTERNAL)
                context.set_details("internal server error")
                return gateway_pb2.SaveMetricsResponse(success=False, error="internal server error")

    async def GetPortfolioMetrics(
        self,
        request: gateway_pb2.GetMetricsRequest,
        context: grpc.aio.ServicerContext,
    ) -> gateway_pb2.PortfolioMetricsData:
        """Get portfolio metrics for a strategy."""
        try:
            strategy_id = validate_strategy_id(request.strategy_id)
        except ValidationError as e:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(str(e))
            return gateway_pb2.PortfolioMetricsData(found=False)

        strategy_id = resolve_agent_id(strategy_id)
        await self._ensure_snapshot_pool()

        if self._snapshot_pool is not None:
            # PostgreSQL mode (deployed)
            try:
                row = await self._snapshot_fetchrow(
                    """
                    SELECT agent_id, initial_value_usd, initial_timestamp,
                           deposits_usd, withdrawals_usd, gas_spent_usd,
                           deployment_id, cycle_id, execution_mode, is_complete,
                           updated_at
                    FROM portfolio_metrics
                    WHERE agent_id = $1
                    """,
                    strategy_id,
                )
                if row is None:
                    return gateway_pb2.PortfolioMetricsData(found=False)

                return gateway_pb2.PortfolioMetricsData(
                    strategy_id=row["agent_id"],
                    initial_value_usd=row["initial_value_usd"],
                    initial_timestamp=int(row["initial_timestamp"].timestamp()),
                    deposits_usd=row["deposits_usd"] or "0",
                    withdrawals_usd=row["withdrawals_usd"] or "0",
                    gas_spent_usd=row["gas_spent_usd"] or "0",
                    updated_at=int(row["updated_at"].timestamp()),
                    found=True,
                    deployment_id=row["deployment_id"] or "",
                    cycle_id=row["cycle_id"] or "",
                    execution_mode=row["execution_mode"] or "",
                    is_complete=bool(row["is_complete"]) if row["is_complete"] is not None else True,
                )
            except Exception as e:
                logger.error("GetPortfolioMetrics failed for %s: %s", strategy_id, e)
                context.set_code(grpc.StatusCode.INTERNAL)
                context.set_details("internal server error")
                return gateway_pb2.PortfolioMetricsData(found=False)
        else:
            # SQLite mode (local dev) — delegate to StateManager's SQLiteStore
            try:
                await self._ensure_initialized()
                assert self._state_manager is not None

                warm = self._state_manager.warm_backend
                if warm and hasattr(warm, "get_portfolio_metrics"):
                    metrics = await warm.get_portfolio_metrics(strategy_id)
                    if metrics is None:
                        return gateway_pb2.PortfolioMetricsData(found=False)

                    return gateway_pb2.PortfolioMetricsData(
                        strategy_id=metrics.strategy_id,
                        initial_value_usd=str(metrics.initial_value_usd),
                        initial_timestamp=int(metrics.timestamp.timestamp()),
                        deposits_usd=str(metrics.deposits_usd),
                        withdrawals_usd=str(metrics.withdrawals_usd),
                        gas_spent_usd=str(metrics.gas_spent_usd),
                        updated_at=int(metrics.timestamp.timestamp()),
                        found=True,
                        deployment_id=getattr(metrics, "deployment_id", "") or "",
                        cycle_id=getattr(metrics, "cycle_id", "") or "",
                        execution_mode=getattr(metrics, "execution_mode", "") or "",
                        is_complete=getattr(metrics, "is_complete", True),
                    )

                return gateway_pb2.PortfolioMetricsData(found=False)
            except Exception as e:
                logger.error("GetPortfolioMetrics (SQLite) failed for %s: %s", strategy_id, e)
                context.set_code(grpc.StatusCode.INTERNAL)
                context.set_details("internal server error")
                return gateway_pb2.PortfolioMetricsData(found=False)

    # =========================================================================
    # Transaction Ledger RPC (VIB-3201)
    # =========================================================================

    async def SaveLedgerEntry(
        self,
        request: gateway_pb2.SaveLedgerEntryRequest,
        context: grpc.aio.ServicerContext,
    ) -> gateway_pb2.SaveLedgerEntryResponse:
        """Persist a transaction ledger entry (VIB-3201).

        Closes the VIB-3157 gateway gap: before this RPC existed,
        ``GatewayStateManager.save_ledger_entry`` raised NotImplementedError
        and live gateway deployments produced no durable trade records. The
        handler mirrors ``SavePortfolioSnapshot``: fail-closed on DB error
        (``success=false, error=...``) so the client raises
        ``AccountingPersistenceError`` and the runner halts with
        ``ACCOUNTING_FAILED``.

        Note: ``extracted_data_json`` is accepted over the wire but is not
        written to the deployed ``transaction_ledger`` Postgres table yet --
        the column lives in the SQLite reference DDL and the metrics-database
        migration that adds it to Postgres is tracked separately. SQLite
        (local dev) persists the full payload via the warm backend.
        """
        try:
            strategy_id = validate_strategy_id(request.strategy_id)
        except ValidationError as e:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(str(e))
            return gateway_pb2.SaveLedgerEntryResponse(success=False, error=str(e))

        entry_id = (request.id or "").strip()
        if not entry_id:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details("id is required")
            return gateway_pb2.SaveLedgerEntryResponse(success=False, error="id is required")
        try:
            uuid.UUID(entry_id)
        except ValueError:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details("id must be a valid UUID")
            return gateway_pb2.SaveLedgerEntryResponse(success=False, error="id must be a valid UUID")

        if request.timestamp <= 0:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details("timestamp must be positive")
            return gateway_pb2.SaveLedgerEntryResponse(success=False, error="timestamp must be positive")

        strategy_id = resolve_agent_id(strategy_id)
        await self._ensure_snapshot_pool()

        try:
            ts = datetime.fromtimestamp(request.timestamp, tz=UTC)
        except (ValueError, OSError, OverflowError):
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details("timestamp out of range")
            return gateway_pb2.SaveLedgerEntryResponse(success=False, error="timestamp out of range")

        if request.extracted_data_json:
            try:
                extracted_json = request.extracted_data_json.decode("utf-8")
            except UnicodeDecodeError:
                context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
                context.set_details("extracted_data_json must be valid UTF-8")
                return gateway_pb2.SaveLedgerEntryResponse(
                    success=False, error="extracted_data_json must be valid UTF-8"
                )
        else:
            extracted_json = ""

        slippage_bps = request.slippage_bps if request.HasField("slippage_bps") else None

        if self._snapshot_pool is not None:
            # PostgreSQL mode (deployed) -- columns match the transaction_ledger
            # reference DDL in almanak/gateway/database.py. ``extracted_data_json``
            # is intentionally excluded: it lives on the SDK-local SQLite schema
            # but has not yet been added to the metrics-database Postgres
            # migration. Proto carries it forward-compat so the wire format is
            # stable once the column lands.
            try:
                await self._snapshot_execute(
                    """
                    INSERT INTO transaction_ledger (
                        id, cycle_id, agent_id, deployment_id, execution_mode,
                        timestamp, intent_type,
                        token_in, amount_in, token_out, amount_out,
                        effective_price, slippage_bps, gas_used, gas_usd,
                        tx_hash, chain, protocol, success, error
                    ) VALUES (
                        $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
                        $11, $12, $13, $14, $15, $16, $17, $18, $19, $20
                    )
                    ON CONFLICT (id) DO UPDATE SET
                        cycle_id = EXCLUDED.cycle_id,
                        deployment_id = EXCLUDED.deployment_id,
                        execution_mode = EXCLUDED.execution_mode,
                        timestamp = EXCLUDED.timestamp,
                        intent_type = EXCLUDED.intent_type,
                        token_in = EXCLUDED.token_in,
                        amount_in = EXCLUDED.amount_in,
                        token_out = EXCLUDED.token_out,
                        amount_out = EXCLUDED.amount_out,
                        effective_price = EXCLUDED.effective_price,
                        slippage_bps = EXCLUDED.slippage_bps,
                        gas_used = EXCLUDED.gas_used,
                        gas_usd = EXCLUDED.gas_usd,
                        tx_hash = EXCLUDED.tx_hash,
                        chain = EXCLUDED.chain,
                        protocol = EXCLUDED.protocol,
                        success = EXCLUDED.success,
                        error = EXCLUDED.error
                    """,
                    entry_id,
                    request.cycle_id,
                    strategy_id,
                    request.deployment_id,
                    request.execution_mode,
                    ts,
                    request.intent_type,
                    request.token_in,
                    request.amount_in,
                    request.token_out,
                    request.amount_out,
                    request.effective_price,
                    slippage_bps,
                    request.gas_used,
                    request.gas_usd,
                    request.tx_hash,
                    request.chain,
                    request.protocol,
                    request.success,
                    request.error,
                )
                return gateway_pb2.SaveLedgerEntryResponse(success=True)
            except Exception as e:
                logger.error("SaveLedgerEntry failed for %s (id=%s): %s", strategy_id, request.id, e)
                context.set_code(grpc.StatusCode.INTERNAL)
                context.set_details("internal server error")
                return gateway_pb2.SaveLedgerEntryResponse(success=False, error="internal server error")
        else:
            # SQLite mode (local dev) — delegate to StateManager's warm backend.
            try:
                await self._ensure_initialized()
                assert self._state_manager is not None
                warm = self._state_manager.warm_backend
                if warm is None or not hasattr(warm, "save_ledger_entry"):
                    error = "warm backend does not support save_ledger_entry"
                    logger.error("SaveLedgerEntry (SQLite) unsupported for %s: %s", strategy_id, error)
                    context.set_code(grpc.StatusCode.UNIMPLEMENTED)
                    context.set_details(error)
                    return gateway_pb2.SaveLedgerEntryResponse(success=False, error=error)

                from almanak.framework.observability.ledger import LedgerEntry

                entry = LedgerEntry(
                    id=entry_id,
                    cycle_id=request.cycle_id,
                    strategy_id=strategy_id,
                    deployment_id=request.deployment_id,
                    execution_mode=request.execution_mode,
                    timestamp=ts,
                    intent_type=request.intent_type,
                    token_in=request.token_in,
                    amount_in=request.amount_in,
                    token_out=request.token_out,
                    amount_out=request.amount_out,
                    effective_price=request.effective_price,
                    slippage_bps=slippage_bps,
                    gas_used=request.gas_used,
                    gas_usd=request.gas_usd,
                    tx_hash=request.tx_hash,
                    chain=request.chain,
                    protocol=request.protocol,
                    success=request.success,
                    error=request.error,
                    extracted_data_json=extracted_json,
                )
                await warm.save_ledger_entry(entry)
                return gateway_pb2.SaveLedgerEntryResponse(success=True)
            except Exception as e:
                logger.error("SaveLedgerEntry (SQLite) failed for %s (id=%s): %s", strategy_id, request.id, e)
                context.set_code(grpc.StatusCode.INTERNAL)
                context.set_details("internal server error")
                return gateway_pb2.SaveLedgerEntryResponse(success=False, error="internal server error")
