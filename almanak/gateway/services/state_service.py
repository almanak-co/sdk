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


# Module-level whitelist of all valid accounting event type strings.
# Built once at import time from the canonical enum definitions in models.py.
# ALL_ACCOUNTING_EVENT_TYPES covers all 6 economic categories:
# lending, pendle, lp, perp, vault, swap (VIB-3480).
def _build_accounting_type_sets() -> tuple[frozenset[str], frozenset[str]]:
    from almanak.framework.accounting.models import LendingEventType, PendleEventType

    return frozenset(e.value for e in LendingEventType), frozenset(e.value for e in PendleEventType)


def _build_all_accounting_event_types() -> frozenset[str]:
    from almanak.framework.accounting.models import ALL_ACCOUNTING_EVENT_TYPES

    return ALL_ACCOUNTING_EVENT_TYPES


try:
    _LENDING_EVENT_TYPES, _PENDLE_EVENT_TYPES = _build_accounting_type_sets()
    _ALL_ACCOUNTING_EVENT_TYPES = _build_all_accounting_event_types()
except Exception as _e:  # pragma: no cover — graceful fallback if models not importable at load time
    logger.error("Failed to build accounting type sets; SaveAccountingEvent will reject all event_types: %s", _e)
    _LENDING_EVENT_TYPES = frozenset()
    _PENDLE_EVENT_TYPES = frozenset()
    _ALL_ACCOUNTING_EVENT_TYPES = frozenset()


class _RawAccountingEvent:
    """Pass-through wrapper for accounting event categories without typed models yet.

    Satisfies the duck-typed interface expected by SQLiteStore.save_accounting_event:
    .identity, .event_type, .position_key, .confidence, .schema_version, .to_payload_json().
    Used for LP/Perp/Vault/Swap events until VIB-3470–3473 add their typed models.
    """

    def __init__(
        self,
        identity: Any,
        event_type: str,
        position_key: str,
        confidence: Any,
        schema_version: int,
        _payload_json: str,
    ) -> None:
        self.identity = identity
        self.event_type = event_type
        self.position_key = position_key
        self.confidence = confidence
        self.schema_version = schema_version
        self._payload_json = _payload_json

    def to_payload_json(self) -> str:
        return self._payload_json


def _row_timestamp_epoch(row: dict[str, Any]) -> int:
    """Best-effort epoch extraction from a SQLite accounting_events row.

    SQLite stores timestamps as ISO strings; the GetAccountingEvents handler
    needs them as Unix seconds for the wire and for the ``since_timestamp``
    filter pushdown done in Python.
    """
    ts = row.get("timestamp")
    if ts is None:
        return 0
    if isinstance(ts, int | float):
        return int(ts)
    try:
        return int(datetime.fromisoformat(str(ts)).timestamp())
    except (ValueError, TypeError):
        return 0


def _pg_row_to_accounting_event(row: Any) -> gateway_pb2.AccountingEvent:
    """Convert one Postgres asyncpg.Record to the proto wire shape.

    The PG SELECT casts ``payload_json::text`` so we get a str (not a dict)
    and re-encode as UTF-8 bytes to match the SaveAccountingEventRequest
    contract. ``agent_id`` is mapped back to wire field ``strategy_id``.
    """
    payload_text = row["payload_text"] or "{}"
    return gateway_pb2.AccountingEvent(
        id=row["id"] or "",
        deployment_id=row["deployment_id"] or "",
        strategy_id=row["agent_id"] or "",
        cycle_id=row["cycle_id"] or "",
        execution_mode=row["execution_mode"] or "",
        timestamp=int(row["ts_epoch"] or 0),
        chain=row["chain"] or "",
        protocol=row["protocol"] or "",
        wallet_address=row["wallet_address"] or "",
        event_type=row["event_type"] or "",
        position_key=row["position_key"] or "",
        ledger_entry_id=row["ledger_entry_id"] or "",
        tx_hash=row["tx_hash"] or "",
        confidence=row["confidence"] or "",
        payload_json=payload_text.encode("utf-8"),
        schema_version=int(row["schema_version"] or 1),
    )


def _sqlite_row_to_accounting_event(row: dict[str, Any]) -> gateway_pb2.AccountingEvent:
    """Convert one SQLite row dict to the proto wire shape.

    SQLite uses ``strategy_id`` as the column name (not ``agent_id``) per
    the convention in sqlite.py. Timestamps are ISO strings.
    """
    payload_text = row.get("payload_json") or "{}"
    if isinstance(payload_text, bytes):
        payload_text = payload_text.decode("utf-8")
    return gateway_pb2.AccountingEvent(
        id=row.get("id") or "",
        deployment_id=row.get("deployment_id") or "",
        strategy_id=row.get("strategy_id") or row.get("agent_id") or "",
        cycle_id=row.get("cycle_id") or "",
        execution_mode=row.get("execution_mode") or "",
        timestamp=_row_timestamp_epoch(row),
        chain=row.get("chain") or "",
        protocol=row.get("protocol") or "",
        wallet_address=row.get("wallet_address") or "",
        event_type=row.get("event_type") or "",
        position_key=row.get("position_key") or "",
        ledger_entry_id=row.get("ledger_entry_id") or "",
        tx_hash=row.get("tx_hash") or "",
        confidence=row.get("confidence") or "",
        payload_json=payload_text.encode("utf-8"),
        schema_version=int(row.get("schema_version") or 1),
    )


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
            # VIB-3761/-3835: strict resolution by default; the gateway
            # CLI's ``--standalone`` flag is the only operator-facing
            # opt-in for the lenient utility-DB fallback.
            from almanak.gateway._server_start_helpers import resolve_gateway_local_db_path

            backend_type = WarmBackendType.SQLITE
            db_path = str(resolve_gateway_local_db_path(self.settings))
            config = StateManagerConfig(
                warm_backend=backend_type,
                sqlite_config=SQLiteConfigLight(db_path=db_path),
            )

        self._state_manager = StateManager(config)
        await self._state_manager.initialize()

        self._initialized = True
        mode = (
            "POSTGRESQL"
            if backend_type == WarmBackendType.POSTGRESQL
            else ("STANDALONE" if self.settings.standalone else "STRATEGY-PINNED")
        )
        logger.info(
            "StateService initialized with %s backend (%s)",
            backend_type.name,
            mode,
        )

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
                    # VIB-3894 — SaveSnapshotRequest is missing
                    # ``deployed_capital_usd`` and ``wallet_total_value_usd``
                    # on the proto wire. The runner's GatewayStateManager
                    # smuggles them through the envelope's metadata under
                    # ``__deployed_capital_usd__`` / ``__wallet_total_value_usd__``
                    # keys; lift them onto the rebuilt snapshot here so the
                    # SQLite writer persists the actual values rather than
                    # the ``Decimal("0")`` default. Backwards-compatible:
                    # legacy snapshots without the keys default to "0".
                    if isinstance(snapshot_metadata, dict):
                        dep_str = snapshot_metadata.pop("__deployed_capital_usd__", None)
                        wtv_str = snapshot_metadata.pop("__wallet_total_value_usd__", None)
                        if dep_str is not None:
                            snapshot_dict["deployed_capital_usd"] = str(dep_str)
                        if wtv_str is not None:
                            snapshot_dict["wallet_total_value_usd"] = str(wtv_str)
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
        """Save or update portfolio metrics (PnL baseline).

        Orchestrates three phases decomposed into
        :mod:`._save_metrics_helpers`:

        1. ``validate_strategy_id`` + ``resolve_agent_id`` (local, small).
        2. :func:`parse_metrics_inputs` — decimals + timestamp validation.
        3. Branch on ``_snapshot_pool``: PostgreSQL UPSERT or SQLite warm-
           backend delegation.

        Error-path ``grpc.StatusCode`` / ``set_details`` / response wording
        are preserved byte-for-byte against the pre-refactor behaviour —
        downstream observability may grep the exact strings.
        """
        from almanak.gateway.services._save_metrics_helpers import (
            MetricsValidationError,
            parse_metrics_inputs,
        )

        try:
            strategy_id = validate_strategy_id(request.strategy_id)
        except ValidationError as e:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(str(e))
            return gateway_pb2.SaveMetricsResponse(success=False, error=str(e))

        strategy_id = resolve_agent_id(strategy_id)

        try:
            inputs = parse_metrics_inputs(request, strategy_id)
        except MetricsValidationError as exc:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(exc.message)
            return gateway_pb2.SaveMetricsResponse(success=False, error=exc.message)

        now = datetime.now(UTC)

        await self._ensure_snapshot_pool()

        if self._snapshot_pool is not None:
            return await self._save_portfolio_metrics_pg(inputs, request, now, context)
        return await self._save_portfolio_metrics_sqlite(inputs, request, context)

    async def _save_portfolio_metrics_pg(
        self,
        inputs: Any,
        request: gateway_pb2.SaveMetricsRequest,
        now: datetime,
        context: grpc.aio.ServicerContext,
    ) -> gateway_pb2.SaveMetricsResponse:
        """PostgreSQL (deployed) persistence path for SavePortfolioMetrics.

        Uses the prepared UPSERT query + positional args from
        :mod:`._save_metrics_helpers`. Any exception maps to a uniform
        ``INTERNAL`` response with ``internal server error`` details —
        wording preserved byte-for-byte.

        VIB-3933 review finding #1: the proto contract (VIB-2765) does not
        carry ``total_value_usd``, so we mirror the SQLite path's
        :func:`resolve_total_value_usd` lookup against the latest snapshot
        before issuing the UPSERT. Without this, the schema default of '0'
        leaks through to ``GetPortfolioMetrics`` and the dashboard renders
        a $0 NAV despite snapshots existing.
        """
        from almanak.gateway.services._save_metrics_helpers import (
            PG_UPSERT_QUERY,
            build_pg_upsert_args,
            resolve_total_value_usd,
        )

        try:
            # Resolve total_value_usd from the latest snapshot via PostgresStore.
            # Best-effort: the helper swallows any backend exception and returns
            # Decimal("0"), so a stale or missing snapshot backend never aborts
            # the metrics write — same contract as the SQLite path.
            await self._ensure_initialized()
            assert self._state_manager is not None
            warm = self._state_manager.warm_backend
            total_value_usd = await resolve_total_value_usd(warm, inputs.strategy_id)

            await self._snapshot_fetchrow(
                PG_UPSERT_QUERY,
                *build_pg_upsert_args(inputs, request, now, total_value_usd),
            )
            logger.debug("Portfolio metrics saved for strategy=%s", inputs.strategy_id)
            return gateway_pb2.SaveMetricsResponse(success=True)
        except Exception as e:
            logger.error("SavePortfolioMetrics failed for %s: %s", inputs.strategy_id, e)
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details("internal server error")
            return gateway_pb2.SaveMetricsResponse(success=False, error="internal server error")

    async def _save_portfolio_metrics_sqlite(
        self,
        inputs: Any,
        request: gateway_pb2.SaveMetricsRequest,
        context: grpc.aio.ServicerContext,
    ) -> gateway_pb2.SaveMetricsResponse:
        """SQLite (local dev) persistence path for SavePortfolioMetrics.

        Delegates to the StateManager's warm backend:
        - resolves ``total_value_usd`` best-effort from the latest snapshot;
        - builds the ``PortfolioMetrics`` dataclass;
        - dispatches to ``warm.save_portfolio_metrics`` when available;
        - maps the (result / no-backend / missing-method / exception)
          outcomes to the exact pre-refactor response shapes.
        """
        from almanak.gateway.services._save_metrics_helpers import (
            build_portfolio_metrics,
            resolve_total_value_usd,
        )

        try:
            await self._ensure_initialized()
            assert self._state_manager is not None
            warm = self._state_manager.warm_backend

            total_value_usd = await resolve_total_value_usd(warm, inputs.strategy_id)
            metrics = build_portfolio_metrics(inputs, request, total_value_usd)

            if warm and hasattr(warm, "save_portfolio_metrics"):
                result = await warm.save_portfolio_metrics(metrics)
                if result:
                    logger.debug("Portfolio metrics saved (SQLite) for strategy=%s", inputs.strategy_id)
                    return gateway_pb2.SaveMetricsResponse(success=True)
                return gateway_pb2.SaveMetricsResponse(
                    success=False, error="Backend save_portfolio_metrics returned False"
                )

            return gateway_pb2.SaveMetricsResponse(
                success=False, error="No warm backend with portfolio metrics support"
            )
        except Exception as e:
            logger.error("SavePortfolioMetrics (SQLite) failed for %s: %s", inputs.strategy_id, e)
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

        VIB-3503 Part 2b: the audit-grade replay JSONB columns
        (``extracted_data_json``, ``price_inputs_json``, ``pre_state_json``,
        ``post_state_json``) are now persisted in PostgreSQL. Empty bytes
        from the wire bind to NULL.
        """
        try:
            strategy_id = validate_strategy_id(request.strategy_id)
        except ValidationError as e:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(str(e))
            return gateway_pb2.SaveLedgerEntryResponse(success=False, error=str(e))

        # Reject blank / whitespace-only deployment_id at the boundary.
        # Symmetric with the GetAccountingEvents read path: rows persisted
        # with an empty deployment_id are unrecoverable by the new replay
        # RPC (which requires deployment_id to be set), so accepting them
        # would silently break restart reconstruction and snapshot enrichment.
        deployment_id = request.deployment_id.strip() if request.deployment_id else ""
        if not deployment_id:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details("deployment_id is required")
            return gateway_pb2.SaveLedgerEntryResponse(success=False, error="deployment_id is required")

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
            # PostgreSQL mode (deployed). VIB-3503 Part 2b: the 4 audit-grade
            # replay JSONB columns (extracted_data_json, price_inputs_json,
            # pre_state_json, post_state_json) are now persisted. Empty bytes
            # from the wire bind to NULL so pre-VIB-3503 rows and rows where
            # the SDK chose not to capture replay inputs both store NULL
            # rather than the JSON-invalid empty string.

            def _decode_jsonb_or_none(field_name: str, raw: bytes) -> str | None:
                """UTF-8 + JSON validate at the gateway boundary.

                The PG ::jsonb cast would surface malformed JSON as INTERNAL,
                while the SQLite path would persist the raw string -- a
                cross-backend divergence. Validate here so both backends
                reject the same inputs with INVALID_ARGUMENT.
                """
                if not raw:
                    return None
                try:
                    decoded = raw.decode("utf-8")
                except UnicodeDecodeError as exc:
                    raise ValueError(f"{field_name} must be valid UTF-8") from exc
                try:
                    json.loads(decoded)
                except json.JSONDecodeError as exc:
                    raise ValueError(f"{field_name} must be valid JSON") from exc
                return decoded

            try:
                price_inputs_json = _decode_jsonb_or_none("price_inputs_json", request.price_inputs_json)
                pre_state_json = _decode_jsonb_or_none("pre_state_json", request.pre_state_json)
                post_state_json = _decode_jsonb_or_none("post_state_json", request.post_state_json)
                # extracted_data_json was UTF-8 decoded earlier (line 950) but
                # never JSON-validated; the PG ::jsonb cast would silently
                # diverge from SQLite for malformed inputs. Validate here.
                if extracted_json:
                    try:
                        json.loads(extracted_json)
                    except json.JSONDecodeError as exc:
                        raise ValueError("extracted_data_json must be valid JSON") from exc
            except ValueError as exc:
                context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
                context.set_details(str(exc))
                return gateway_pb2.SaveLedgerEntryResponse(success=False, error=str(exc))

            extracted_data_for_pg: str | None = extracted_json or None

            try:
                await self._snapshot_execute(
                    """
                    INSERT INTO transaction_ledger (
                        id, cycle_id, agent_id, deployment_id, execution_mode,
                        timestamp, intent_type,
                        token_in, amount_in, token_out, amount_out,
                        effective_price, slippage_bps, gas_used, gas_usd,
                        tx_hash, chain, protocol, success, error,
                        extracted_data_json, price_inputs_json, pre_state_json, post_state_json
                    ) VALUES (
                        $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
                        $11, $12, $13, $14, $15, $16, $17, $18, $19, $20,
                        $21::jsonb, $22::jsonb, $23::jsonb, $24::jsonb
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
                        error = EXCLUDED.error,
                        extracted_data_json = EXCLUDED.extracted_data_json,
                        price_inputs_json = EXCLUDED.price_inputs_json,
                        pre_state_json = EXCLUDED.pre_state_json,
                        post_state_json = EXCLUDED.post_state_json
                    """,
                    entry_id,
                    request.cycle_id,
                    strategy_id,
                    deployment_id,
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
                    extracted_data_for_pg,
                    price_inputs_json,
                    pre_state_json,
                    post_state_json,
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

                class _InvalidUtf8FieldError(ValueError):
                    pass

                def _decode_optional_bytes(field_name: str, b: bytes) -> str:
                    if not b:
                        return ""
                    try:
                        return b.decode("utf-8")
                    except UnicodeDecodeError:
                        raise _InvalidUtf8FieldError(f"{field_name} must be valid UTF-8") from None

                try:
                    price_inputs_json = _decode_optional_bytes("price_inputs_json", request.price_inputs_json)
                    pre_state_json = _decode_optional_bytes("pre_state_json", request.pre_state_json)
                    post_state_json = _decode_optional_bytes("post_state_json", request.post_state_json)
                except _InvalidUtf8FieldError as exc:
                    context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
                    context.set_details(str(exc))
                    return gateway_pb2.SaveLedgerEntryResponse(success=False, error=str(exc))

                entry = LedgerEntry(
                    id=entry_id,
                    cycle_id=request.cycle_id,
                    strategy_id=strategy_id,
                    deployment_id=deployment_id,
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
                    price_inputs_json=price_inputs_json,
                    pre_state_json=pre_state_json,
                    post_state_json=post_state_json,
                )
                await warm.save_ledger_entry(entry)
                return gateway_pb2.SaveLedgerEntryResponse(success=True)
            except Exception as e:
                logger.error("SaveLedgerEntry (SQLite) failed for %s (id=%s): %s", strategy_id, request.id, e)
                context.set_code(grpc.StatusCode.INTERNAL)
                context.set_details("internal server error")
                return gateway_pb2.SaveLedgerEntryResponse(success=False, error="internal server error")

    # =========================================================================
    # Accounting Events RPC (VIB-3449)
    # =========================================================================

    async def SaveAccountingEvent(
        self,
        request: gateway_pb2.SaveAccountingEventRequest,
        context: grpc.aio.ServicerContext,
    ) -> gateway_pb2.SaveAccountingEventResponse:
        """Persist a typed accounting event (VIB-3449).

        Routes to the warm backend's ``save_accounting_event`` method, which
        writes to the ``accounting_events`` table (SQLite in local dev,
        PostgreSQL in deployed mode once the metrics-database migration lands).
        Non-blocking in non-live modes: on DB failure returns success=false.
        """
        try:
            strategy_id = validate_strategy_id(request.strategy_id)
        except ValidationError as e:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(str(e))
            return gateway_pb2.SaveAccountingEventResponse(success=False, error=str(e))

        # Reject blank / whitespace-only deployment_id at the boundary --
        # symmetric with GetAccountingEvents. Persisting a row with a blank
        # deployment_id would make it unrecoverable by the new replay RPC,
        # which silently breaks restart reconstruction and PnL enrichment.
        deployment_id = request.deployment_id.strip() if request.deployment_id else ""
        if not deployment_id:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details("deployment_id is required")
            return gateway_pb2.SaveAccountingEventResponse(success=False, error="deployment_id is required")

        event_id = (request.id or "").strip()
        if not event_id:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details("id is required")
            return gateway_pb2.SaveAccountingEventResponse(success=False, error="id is required")

        try:
            uuid.UUID(event_id)
        except ValueError:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details("id must be a valid UUID")
            return gateway_pb2.SaveAccountingEventResponse(success=False, error="id must be a valid UUID")

        if request.timestamp <= 0:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details("timestamp must be positive")
            return gateway_pb2.SaveAccountingEventResponse(success=False, error="timestamp must be positive")

        # Validate event_type against all known accounting schemas (all 5 categories)
        # before any deserialization attempt — unknown types get INVALID_ARGUMENT, not INTERNAL.
        if request.event_type not in _ALL_ACCOUNTING_EVENT_TYPES:
            err = f"unknown event_type: {request.event_type!r}"
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(err)
            return gateway_pb2.SaveAccountingEventResponse(success=False, error=err)

        if not request.payload_json:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details("payload_json is required")
            return gateway_pb2.SaveAccountingEventResponse(success=False, error="payload_json is required")

        try:
            payload_str = request.payload_json.decode("utf-8")
        except UnicodeDecodeError:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details("payload_json must be valid UTF-8")
            return gateway_pb2.SaveAccountingEventResponse(success=False, error="payload_json must be valid UTF-8")

        strategy_id = resolve_agent_id(strategy_id)

        try:
            json.loads(payload_str)
        except json.JSONDecodeError:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details("payload_json must be valid JSON")
            return gateway_pb2.SaveAccountingEventResponse(success=False, error="payload_json must be valid JSON")

        try:
            ts = datetime.fromtimestamp(request.timestamp, tz=UTC)
        except (ValueError, OSError, OverflowError):
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details("timestamp out of range")
            return gateway_pb2.SaveAccountingEventResponse(success=False, error="timestamp out of range")

        from almanak.framework.accounting.models import AccountingConfidence

        raw_confidence = AccountingConfidence.ESTIMATED
        if request.confidence:
            try:
                raw_confidence = AccountingConfidence(request.confidence)
            except ValueError:
                err = f"invalid confidence: {request.confidence!r}"
                context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
                context.set_details(err)
                return gateway_pb2.SaveAccountingEventResponse(success=False, error=err)

        await self._ensure_snapshot_pool()

        if self._snapshot_pool is not None:
            # PostgreSQL mode (deployed). VIB-3503 Part 2a: persist the typed
            # accounting event row to the metrics-database accounting_events
            # table. The PG schema column is `agent_id` (resolved above via
            # resolve_agent_id); the wire field stays `strategy_id`. UPSERT
            # by `id` is exercised by retries -- the UUIDv5 id is deterministic
            # in (deployment, cycle, intent_type, tx, position) so re-delivery
            # of the same event collapses to one row. Per ticket spec
            # corrections are welcome: ON CONFLICT DO UPDATE refreshes all
            # non-id columns so the latest write wins.
            try:
                await self._snapshot_execute(
                    """
                    INSERT INTO accounting_events (
                        id, deployment_id, agent_id, cycle_id, execution_mode,
                        timestamp, chain, protocol, wallet_address, event_type,
                        position_key, ledger_entry_id, tx_hash, confidence,
                        payload_json, schema_version
                    ) VALUES (
                        $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
                        $11, $12, $13, $14, $15::jsonb, $16
                    )
                    ON CONFLICT (id) DO UPDATE SET
                        deployment_id   = EXCLUDED.deployment_id,
                        agent_id        = EXCLUDED.agent_id,
                        cycle_id        = EXCLUDED.cycle_id,
                        execution_mode  = EXCLUDED.execution_mode,
                        timestamp       = EXCLUDED.timestamp,
                        chain           = EXCLUDED.chain,
                        protocol        = EXCLUDED.protocol,
                        wallet_address  = EXCLUDED.wallet_address,
                        event_type      = EXCLUDED.event_type,
                        position_key    = EXCLUDED.position_key,
                        ledger_entry_id = EXCLUDED.ledger_entry_id,
                        tx_hash         = EXCLUDED.tx_hash,
                        confidence      = EXCLUDED.confidence,
                        payload_json    = EXCLUDED.payload_json,
                        schema_version  = EXCLUDED.schema_version
                    """,
                    event_id,
                    deployment_id,
                    strategy_id,
                    request.cycle_id,
                    request.execution_mode,
                    ts,
                    request.chain,
                    request.protocol,
                    request.wallet_address,
                    request.event_type,
                    request.position_key,
                    request.ledger_entry_id,
                    request.tx_hash,
                    str(raw_confidence),
                    payload_str,
                    request.schema_version or 1,
                )
                logger.debug(
                    "Accounting event saved (Postgres) id=%s, type=%s, agent=%s",
                    event_id,
                    request.event_type,
                    strategy_id,
                )
                return gateway_pb2.SaveAccountingEventResponse(success=True)
            except Exception as e:
                logger.error("SaveAccountingEvent PG failed for id=%s: %s", event_id, e)
                context.set_code(grpc.StatusCode.INTERNAL)
                context.set_details("internal server error")
                return gateway_pb2.SaveAccountingEventResponse(success=False, error="internal server error")

        try:
            await self._ensure_initialized()
            assert self._state_manager is not None
            warm = self._state_manager.warm_backend
            if warm is None or not hasattr(warm, "save_accounting_event"):
                error = "warm backend does not support save_accounting_event"
                logger.error("SaveAccountingEvent unsupported for id=%s: %s", event_id, error)
                context.set_code(grpc.StatusCode.UNIMPLEMENTED)
                context.set_details(error)
                return gateway_pb2.SaveAccountingEventResponse(success=False, error=error)

            from almanak.framework.accounting.models import (
                AccountingIdentity,
                LendingAccountingEvent,
                PendleAccountingEvent,
            )

            identity = AccountingIdentity(
                id=event_id,
                deployment_id=deployment_id,
                strategy_id=strategy_id,
                cycle_id=request.cycle_id,
                execution_mode=request.execution_mode,
                timestamp=ts,
                chain=request.chain,
                protocol=request.protocol,
                wallet_address=request.wallet_address,
                tx_hash=request.tx_hash,
                ledger_entry_id=request.ledger_entry_id,
            )

            # Reconstruct the typed event from payload_json so the SQLite
            # store receives the correct dataclass (with to_payload_json(),
            # event_type, confidence, schema_version attributes).
            # event_type has already been validated against ALL_ACCOUNTING_EVENT_TYPES.
            # Known typed deserializers exist for Lending and Pendle; all other valid
            # categories (LP, Perp, Vault, Swap) use a pass-through wrapper until
            # their handler models are added in VIB-3470–3473.

            try:
                accounting_event: LendingAccountingEvent | PendleAccountingEvent | _RawAccountingEvent
                if request.event_type in _LENDING_EVENT_TYPES:
                    accounting_event = LendingAccountingEvent.from_payload_json(identity, payload_str)
                elif request.event_type in _PENDLE_EVENT_TYPES:
                    accounting_event = PendleAccountingEvent.from_payload_json(identity, payload_str)
                else:
                    # Pass-through for categories without typed models yet.
                    accounting_event = _RawAccountingEvent(
                        identity=identity,
                        event_type=request.event_type,
                        position_key=request.position_key,
                        confidence=raw_confidence,
                        schema_version=request.schema_version or 1,
                        _payload_json=payload_str,
                    )
            except (ValueError, TypeError, KeyError, json.JSONDecodeError) as exc:
                err = f"invalid payload_json for event_type {request.event_type!r}: {exc}"
                context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
                context.set_details(err)
                return gateway_pb2.SaveAccountingEventResponse(success=False, error=err)

            result = await warm.save_accounting_event(accounting_event)
            if result:
                logger.debug(
                    "Accounting event saved (SQLite) id=%s, type=%s, strategy=%s",
                    event_id,
                    request.event_type,
                    strategy_id,
                )
            return gateway_pb2.SaveAccountingEventResponse(success=bool(result))
        except Exception as e:
            logger.error("SaveAccountingEvent failed for id=%s: %s", event_id, e)
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details("internal server error")
            return gateway_pb2.SaveAccountingEventResponse(success=False, error="internal server error")

    # =========================================================================
    # Position Events RPC (VIB-3449)
    # =========================================================================

    async def SavePositionEvent(
        self,
        request: gateway_pb2.SavePositionEventRequest,
        context: grpc.aio.ServicerContext,
    ) -> gateway_pb2.SavePositionEventResponse:
        """Persist a position lifecycle event (VIB-3449).

        Routes to the warm backend's ``save_position_event`` method, which
        writes to the ``position_events`` table. Non-blocking: on DB failure
        logs a warning and returns success=false rather than raising.
        """
        # Reject blank / whitespace-only deployment_id at the boundary --
        # symmetric with the other Save*/Get* paths so position rows can
        # always be correlated back to the deployment that wrote them.
        deployment_id = request.deployment_id.strip() if request.deployment_id else ""
        if not deployment_id:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details("deployment_id is required")
            return gateway_pb2.SavePositionEventResponse(success=False, error="deployment_id is required")

        event_id = (request.id or "").strip()
        if not event_id:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details("id is required")
            return gateway_pb2.SavePositionEventResponse(success=False, error="id is required")

        try:
            uuid.UUID(event_id)
        except ValueError:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details("id must be a valid UUID")
            return gateway_pb2.SavePositionEventResponse(success=False, error="id must be a valid UUID")

        if request.timestamp <= 0:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details("timestamp must be positive")
            return gateway_pb2.SavePositionEventResponse(success=False, error="timestamp must be positive")

        # Validate position_type and event_type against known enum values to
        # reject typos at the gateway boundary rather than persisting corrupt records.
        from almanak.framework.observability.position_events import PositionEventType, PositionType

        valid_position_types = frozenset(e.value for e in PositionType)
        valid_event_types = frozenset(e.value for e in PositionEventType)

        if request.position_type not in valid_position_types:
            err = f"unknown position_type: {request.position_type!r}"
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(err)
            return gateway_pb2.SavePositionEventResponse(success=False, error=err)

        if request.event_type not in valid_event_types:
            err = f"unknown event_type: {request.event_type!r}"
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(err)
            return gateway_pb2.SavePositionEventResponse(success=False, error=err)

        try:
            await self._ensure_initialized()
            assert self._state_manager is not None
            warm = self._state_manager.warm_backend
            if warm is None or not hasattr(warm, "save_position_event"):
                error = "warm backend does not support save_position_event"
                logger.error("SavePositionEvent unsupported for id=%s: %s", event_id, error)
                context.set_code(grpc.StatusCode.UNIMPLEMENTED)
                context.set_details(error)
                return gateway_pb2.SavePositionEventResponse(success=False, error=error)

            try:
                ts = datetime.fromtimestamp(request.timestamp, tz=UTC)
            except (ValueError, OSError, OverflowError):
                context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
                context.set_details("timestamp out of range")
                return gateway_pb2.SavePositionEventResponse(success=False, error="timestamp out of range")

            from almanak.framework.observability.position_events import PositionEvent

            event = PositionEvent(
                id=event_id,
                deployment_id=deployment_id,
                cycle_id=request.cycle_id,
                execution_mode=request.execution_mode,
                position_id=request.position_id,
                position_type=request.position_type,
                event_type=request.event_type,
                timestamp=ts,
                protocol=request.protocol,
                chain=request.chain,
                token0=request.token0,
                token1=request.token1,
                amount0=request.amount0,
                amount1=request.amount1,
                value_usd=request.value_usd,
                tick_lower=request.tick_lower if request.HasField("tick_lower") else None,
                tick_upper=request.tick_upper if request.HasField("tick_upper") else None,
                liquidity=request.liquidity,
                in_range=request.in_range if request.HasField("in_range") else None,
                fees_token0=request.fees_token0,
                fees_token1=request.fees_token1,
                leverage=request.leverage,
                entry_price=request.entry_price,
                mark_price=request.mark_price,
                unrealized_pnl=request.unrealized_pnl,
                is_long=request.is_long if request.HasField("is_long") else None,
                tx_hash=request.tx_hash,
                gas_usd=request.gas_usd,
                ledger_entry_id=request.ledger_entry_id,
                protocol_fees_usd=request.protocol_fees_usd,
                attribution_json=request.attribution_json or "{}",
                attribution_version=request.attribution_version,
            )

            result = await warm.save_position_event(event)
            if result:
                logger.debug(
                    "Position event saved (SQLite) id=%s, type=%s, position=%s",
                    event_id,
                    request.event_type,
                    request.position_id,
                )
            return gateway_pb2.SavePositionEventResponse(success=bool(result))
        except Exception as e:
            logger.error("SavePositionEvent failed for id=%s: %s", event_id, e)
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details("internal server error")
            return gateway_pb2.SavePositionEventResponse(success=False, error="internal server error")

    # =========================================================================
    # Read accounting events RPC (VIB-3503 Part 2c)
    # =========================================================================

    async def GetAccountingEvents(
        self,
        request: gateway_pb2.GetAccountingEventsRequest,
        context: grpc.aio.ServicerContext,
    ) -> gateway_pb2.GetAccountingEventsResponse:
        """Read accounting events for FIFO basis reconstruction and PnL enrichment.

        Two readers depend on this RPC in deployed mode:
        - Runner startup ``_run_loop_helpers`` reconstructs the lending FIFO
          basis store so REPAY / PT_REDEEM realized-PnL is correct after a
          restart (see VIB-3484).
        - ``PortfolioValuer`` per-snapshot prefetch enriches lending and
          vault positions with cost_basis_usd / unrealized_pnl_usd /
          realized_pnl_usd at snapshot time.

        Read-side fail-quiet: on backend error returns an empty list rather
        than raising, since stale PnL is preferred over halting snapshot
        building. The write paths stay fail-closed.

        Empty-string filters mean "no filter on this field." ``limit=0``
        means "no limit" -- FIFO reconstruction needs the full history
        from the opening event forward.
        """
        try:
            strategy_id = validate_strategy_id(request.strategy_id)
        except ValidationError as e:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(str(e))
            return gateway_pb2.GetAccountingEventsResponse(events=[])

        deployment_id = request.deployment_id.strip() if request.deployment_id else ""
        if not deployment_id:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details("deployment_id is required")
            return gateway_pb2.GetAccountingEventsResponse(events=[])

        # Reject negative limit / since_timestamp at the boundary so PG and
        # SQLite paths never disagree on what they accept. limit=0 is the
        # documented sentinel for "no limit"; negatives have no defined
        # meaning and would silently fall through to backend-specific
        # behaviour (PG: empty result for limit=-1; SQLite: list[:negative]
        # slices from the end).
        if request.limit < 0:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details("limit must be >= 0")
            return gateway_pb2.GetAccountingEventsResponse(events=[])

        if request.since_timestamp < 0:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details("since_timestamp must be >= 0")
            return gateway_pb2.GetAccountingEventsResponse(events=[])

        strategy_id = resolve_agent_id(strategy_id)
        await self._ensure_snapshot_pool()

        if self._snapshot_pool is not None:
            try:
                rows = await self._snapshot_fetch(
                    """
                    SELECT id, deployment_id, agent_id, cycle_id, execution_mode,
                           EXTRACT(EPOCH FROM timestamp)::bigint AS ts_epoch,
                           chain, protocol, wallet_address, event_type,
                           position_key, ledger_entry_id, tx_hash, confidence,
                           payload_json::text AS payload_text, schema_version
                    FROM accounting_events
                    WHERE agent_id = $1
                      AND deployment_id = $2
                      AND ($3 = '' OR position_key = $3)
                      AND ($4 = '' OR event_type = $4)
                      AND ($5 = 0 OR timestamp >= to_timestamp($5))
                    ORDER BY timestamp ASC
                    LIMIT NULLIF($6, 0)
                    """,
                    strategy_id,
                    deployment_id,
                    request.position_key,
                    request.event_type,
                    request.since_timestamp,
                    request.limit,
                )
                events = [_pg_row_to_accounting_event(r) for r in rows]
                return gateway_pb2.GetAccountingEventsResponse(events=events)
            except Exception as e:
                logger.warning("GetAccountingEvents PG failed for agent=%s: %s", strategy_id, e)
                return gateway_pb2.GetAccountingEventsResponse(events=[])

        # SQLite mode (local dev) — delegate to the warm backend's sync primitive.
        try:
            await self._ensure_initialized()
            assert self._state_manager is not None
            warm = self._state_manager.warm_backend
            if warm is None or not hasattr(warm, "get_accounting_events_sync"):
                return gateway_pb2.GetAccountingEventsResponse(events=[])

            position_key_filter = request.position_key or None
            rows = warm.get_accounting_events_sync(
                deployment_id=deployment_id,
                position_key=position_key_filter,
            )

            # Apply event_type / since_timestamp / limit in Python -- the SQLite
            # primitive only supports deployment_id + position_key. Pushing the
            # other filters into SQLite is a separate concern; doing them in
            # Python is fine because local-mode datasets are small.
            if request.event_type:
                rows = [r for r in rows if r.get("event_type") == request.event_type]
            if request.since_timestamp > 0:
                rows = [r for r in rows if _row_timestamp_epoch(r) >= request.since_timestamp]
            if request.limit > 0:
                rows = rows[: request.limit]

            events = [_sqlite_row_to_accounting_event(r) for r in rows]
            return gateway_pb2.GetAccountingEventsResponse(events=events)
        except Exception as e:
            logger.warning("GetAccountingEvents SQLite failed: %s", e)
            return gateway_pb2.GetAccountingEventsResponse(events=[])

    # =========================================================================
    # Accounting Outbox RPCs — crash-safe durability for AccountingProcessor
    # DDL: metrics-database PR #24 (VIB-3503) + per-position columns added in
    # VIB-3658.  PG primary key = ledger_entry_id.
    # =========================================================================

    def _pg_outbox_row_to_proto(self, row: Any) -> gateway_pb2.OutboxEntry:
        """Convert a PG asyncpg.Record from accounting_outbox to the proto shape.

        Column-name translation (PG vs SQLite vs wire):
        - PG ``agent_id`` ↔ SQLite ``strategy_id`` ↔ wire ``strategy_id``
          (same identity, different column names — agent_id is the deployed
          ``resolve_agent_id()`` form, strategy_id is the local form).
        - PG ``ledger_entry_id`` is also the primary key; we mirror it into
          the proto's ``id`` field so AccountingProcessor.drain_one can treat
          PG and SQLite rows identically.
        - PG ``retry_count`` / ``last_error`` map to proto ``attempts`` /
          ``error`` (SQLite-compatible names).
        """
        ledger_id = row["ledger_entry_id"] or ""
        created = row.get("created_at")
        created_str = created.isoformat() if hasattr(created, "isoformat") else str(created or "")
        processed = row.get("processed_at")
        updated_str = processed.isoformat() if hasattr(processed, "isoformat") else created_str
        return gateway_pb2.OutboxEntry(
            id=ledger_id,
            deployment_id=row.get("deployment_id") or "",
            strategy_id=row.get("agent_id") or "",
            cycle_id=row.get("cycle_id") or "",
            ledger_entry_id=ledger_id,
            intent_type=row.get("intent_type") or "",
            wallet_address=row.get("wallet_address") or "",
            position_key=row.get("position_key") or "",
            market_id=row.get("market_id") or "",
            status=row.get("status") or "pending",
            attempts=int(row.get("retry_count") or 0),
            error=row.get("last_error") or "",
            created_at=created_str,
            updated_at=updated_str,
        )

    def _sqlite_outbox_row_to_proto(self, row: dict[str, Any]) -> gateway_pb2.OutboxEntry:
        return gateway_pb2.OutboxEntry(
            id=row.get("id") or "",
            deployment_id=row.get("deployment_id") or "",
            strategy_id=row.get("strategy_id") or "",
            cycle_id=row.get("cycle_id") or "",
            ledger_entry_id=row.get("ledger_entry_id") or "",
            intent_type=row.get("intent_type") or "",
            wallet_address=row.get("wallet_address") or "",
            position_key=row.get("position_key") or "",
            market_id=row.get("market_id") or "",
            status=row.get("status") or "pending",
            attempts=int(row.get("attempts") or 0),
            error=row.get("error") or "",
            created_at=row.get("created_at") or "",
            updated_at=row.get("updated_at") or "",
        )

    async def SaveOutboxEntry(
        self,
        request: gateway_pb2.SaveOutboxEntryRequest,
        context: grpc.aio.ServicerContext,
    ) -> gateway_pb2.SaveOutboxEntryResponse:
        """Write one accounting_outbox row (INSERT OR IGNORE — idempotent).

        Fail-closed in live mode: if the PG write fails the runner raises
        AccountingPersistenceError so the cycle halts rather than continuing
        without a durable outbox record.
        """
        ledger_entry_id = (request.ledger_entry_id or "").strip()
        deployment_id = (request.deployment_id or "").strip()
        if not ledger_entry_id:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details("ledger_entry_id is required")
            return gateway_pb2.SaveOutboxEntryResponse(success=False, error="ledger_entry_id is required")
        if not deployment_id:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details("deployment_id is required")
            return gateway_pb2.SaveOutboxEntryResponse(success=False, error="deployment_id is required")

        strategy_id_raw = (request.strategy_id or "").strip()
        try:
            strategy_id = validate_strategy_id(strategy_id_raw)
        except ValidationError as e:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(str(e))
            return gateway_pb2.SaveOutboxEntryResponse(success=False, error=str(e))
        strategy_id = resolve_agent_id(strategy_id)

        await self._ensure_snapshot_pool()

        if self._snapshot_pool is not None:
            try:
                await self._snapshot_execute(
                    """
                    INSERT INTO accounting_outbox
                        (ledger_entry_id, agent_id, deployment_id, intent_type,
                         cycle_id, wallet_address, position_key, market_id,
                         status, retry_count)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 'pending', 0)
                    ON CONFLICT (ledger_entry_id) DO NOTHING
                    """,
                    ledger_entry_id,
                    strategy_id,
                    deployment_id,
                    request.intent_type or "",
                    request.cycle_id or "",
                    request.wallet_address or "",
                    request.position_key or "",
                    request.market_id or "",
                )
                return gateway_pb2.SaveOutboxEntryResponse(success=True)
            except Exception as e:
                logger.error("SaveOutboxEntry PG failed for ledger_id=%s: %s", ledger_entry_id, e)
                return gateway_pb2.SaveOutboxEntryResponse(success=False, error="internal server error")

        # SQLite path
        try:
            await self._ensure_initialized()
            assert self._state_manager is not None
            warm = self._state_manager.warm_backend
            if warm is None or not hasattr(warm, "save_outbox_entry"):
                return gateway_pb2.SaveOutboxEntryResponse(
                    success=False, error="warm backend does not support save_outbox_entry"
                )
            await warm.save_outbox_entry(
                outbox_id=request.outbox_id or ledger_entry_id,
                deployment_id=deployment_id,
                strategy_id=strategy_id,
                cycle_id=request.cycle_id or "",
                ledger_entry_id=ledger_entry_id,
                intent_type=request.intent_type or "",
                wallet_address=request.wallet_address or "",
                position_key=request.position_key or "",
                market_id=request.market_id or "",
                created_at=request.created_at or datetime.now(UTC).isoformat(),
            )
            return gateway_pb2.SaveOutboxEntryResponse(success=True)
        except Exception as e:
            logger.error("SaveOutboxEntry SQLite failed for ledger_id=%s: %s", ledger_entry_id, e)
            return gateway_pb2.SaveOutboxEntryResponse(success=False, error="internal server error")

    async def GetOutboxEntry(
        self,
        request: gateway_pb2.GetOutboxEntryRequest,
        context: grpc.aio.ServicerContext,
    ) -> gateway_pb2.GetOutboxEntryResponse:
        """Fetch the outbox row for a given ledger_entry_id, or found=False."""
        ledger_entry_id = (request.ledger_entry_id or "").strip()
        if not ledger_entry_id:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details("ledger_entry_id is required")
            return gateway_pb2.GetOutboxEntryResponse(found=False)

        await self._ensure_snapshot_pool()

        if self._snapshot_pool is not None:
            try:
                row = await self._snapshot_fetchrow(
                    """
                    SELECT ledger_entry_id, agent_id, deployment_id, intent_type,
                           cycle_id, wallet_address, position_key, market_id,
                           status, retry_count, last_error, created_at, processed_at
                    FROM accounting_outbox
                    WHERE ledger_entry_id = $1
                    """,
                    ledger_entry_id,
                )
                if row is None:
                    return gateway_pb2.GetOutboxEntryResponse(found=False)
                return gateway_pb2.GetOutboxEntryResponse(found=True, entry=self._pg_outbox_row_to_proto(row))
            except Exception as e:
                logger.warning("GetOutboxEntry PG failed for ledger_id=%s: %s", ledger_entry_id, e)
                return gateway_pb2.GetOutboxEntryResponse(found=False)

        # SQLite path
        try:
            await self._ensure_initialized()
            assert self._state_manager is not None
            warm = self._state_manager.warm_backend
            if warm is None or not hasattr(warm, "get_outbox_by_ledger_id"):
                return gateway_pb2.GetOutboxEntryResponse(found=False)
            row = await warm.get_outbox_by_ledger_id(ledger_entry_id)
            if row is None:
                return gateway_pb2.GetOutboxEntryResponse(found=False)
            return gateway_pb2.GetOutboxEntryResponse(found=True, entry=self._sqlite_outbox_row_to_proto(row))
        except Exception as e:
            logger.warning("GetOutboxEntry SQLite failed: %s", e)
            return gateway_pb2.GetOutboxEntryResponse(found=False)

    async def GetOutboxPending(
        self,
        request: gateway_pb2.GetOutboxPendingRequest,
        context: grpc.aio.ServicerContext,
    ) -> gateway_pb2.GetOutboxPendingResponse:
        """Return pending/failed/stuck-processing outbox rows for a deployment.

        Used by AccountingProcessor.drain_pending() on runner startup to recover
        any rows that were in-flight when the container last restarted.
        """
        deployment_id = (request.deployment_id or "").strip()
        if not deployment_id:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details("deployment_id is required")
            return gateway_pb2.GetOutboxPendingResponse(entries=[])
        max_retries = request.max_retries if request.max_retries > 0 else 3

        await self._ensure_snapshot_pool()

        if self._snapshot_pool is not None:
            try:
                rows = await self._snapshot_fetch(
                    """
                    SELECT ledger_entry_id, agent_id, deployment_id, intent_type,
                           cycle_id, wallet_address, position_key, market_id,
                           status, retry_count, last_error, created_at, processed_at
                    FROM accounting_outbox
                    WHERE deployment_id = $1
                      AND status IN ('pending', 'failed', 'processing')
                      AND retry_count < $2
                    ORDER BY created_at ASC
                    """,
                    deployment_id,
                    max_retries,
                )
                return gateway_pb2.GetOutboxPendingResponse(entries=[self._pg_outbox_row_to_proto(r) for r in rows])
            except Exception as e:
                logger.warning("GetOutboxPending PG failed for deployment=%s: %s", deployment_id, e)
                return gateway_pb2.GetOutboxPendingResponse(entries=[])

        # SQLite path
        try:
            await self._ensure_initialized()
            assert self._state_manager is not None
            warm = self._state_manager.warm_backend
            if warm is None or not hasattr(warm, "get_outbox_pending"):
                return gateway_pb2.GetOutboxPendingResponse(entries=[])
            rows = await warm.get_outbox_pending(deployment_id, max_retries=max_retries)
            return gateway_pb2.GetOutboxPendingResponse(entries=[self._sqlite_outbox_row_to_proto(r) for r in rows])
        except Exception as e:
            logger.warning("GetOutboxPending SQLite failed: %s", e)
            return gateway_pb2.GetOutboxPendingResponse(entries=[])

    async def UpdateOutboxEntry(
        self,
        request: gateway_pb2.UpdateOutboxEntryRequest,
        context: grpc.aio.ServicerContext,
    ) -> gateway_pb2.UpdateOutboxEntryResponse:
        """Update the status (and optionally retry_count) of an outbox row.

        Fail-closed: used to mark rows as 'processing', 'processed', or 'failed'.
        On PG, outbox_id == ledger_entry_id (the table's primary key).
        """
        outbox_id = (request.outbox_id or "").strip()
        if not outbox_id:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details("outbox_id is required")
            return gateway_pb2.UpdateOutboxEntryResponse(success=False, error="outbox_id is required")
        status = (request.status or "").strip()
        if status not in ("pending", "processing", "processed", "failed"):
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(f"invalid status: {status!r}")
            return gateway_pb2.UpdateOutboxEntryResponse(success=False, error=f"invalid status: {status!r}")

        await self._ensure_snapshot_pool()

        if self._snapshot_pool is not None:
            try:
                has_attempts = request.HasField("attempts")
                if has_attempts:
                    await self._snapshot_execute(
                        """
                        UPDATE accounting_outbox
                        SET status = $1, last_error = $2, retry_count = $3,
                            processed_at = CASE WHEN $1 = 'processed' THEN NOW() ELSE processed_at END
                        WHERE ledger_entry_id = $4
                        """,
                        status,
                        request.error or "",
                        request.attempts,
                        outbox_id,
                    )
                else:
                    await self._snapshot_execute(
                        """
                        UPDATE accounting_outbox
                        SET status = $1, last_error = $2,
                            processed_at = CASE WHEN $1 = 'processed' THEN NOW() ELSE processed_at END
                        WHERE ledger_entry_id = $3
                        """,
                        status,
                        request.error or "",
                        outbox_id,
                    )
                return gateway_pb2.UpdateOutboxEntryResponse(success=True)
            except Exception as e:
                logger.error("UpdateOutboxEntry PG failed for outbox_id=%s: %s", outbox_id, e)
                return gateway_pb2.UpdateOutboxEntryResponse(success=False, error="internal server error")

        # SQLite path
        try:
            await self._ensure_initialized()
            assert self._state_manager is not None
            warm = self._state_manager.warm_backend
            if warm is None or not hasattr(warm, "update_outbox_entry"):
                return gateway_pb2.UpdateOutboxEntryResponse(
                    success=False, error="warm backend does not support update_outbox_entry"
                )
            attempts_val = request.attempts if request.HasField("attempts") else None
            await warm.update_outbox_entry(
                outbox_id=outbox_id,
                status=status,
                error=request.error or "",
                attempts=attempts_val,
            )
            return gateway_pb2.UpdateOutboxEntryResponse(success=True)
        except Exception as e:
            logger.error("UpdateOutboxEntry SQLite failed for outbox_id=%s: %s", outbox_id, e)
            return gateway_pb2.UpdateOutboxEntryResponse(success=False, error="internal server error")

    async def HasAccountingEventsForLedger(
        self,
        request: gateway_pb2.HasAccountingEventsForLedgerRequest,
        context: grpc.aio.ServicerContext,
    ) -> gateway_pb2.HasAccountingEventsForLedgerResponse:
        """Return True if accounting_events already has a row for this ledger entry.

        Used by AccountingProcessor.drain_one as an idempotency guard: if we
        already produced an accounting event for this trade, skip re-processing.
        Uses the idx_ae_ledger index so the query is a single index seek.
        """
        ledger_entry_id = (request.ledger_entry_id or "").strip()
        if not ledger_entry_id:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details("ledger_entry_id is required")
            return gateway_pb2.HasAccountingEventsForLedgerResponse(has_events=False)

        await self._ensure_snapshot_pool()

        if self._snapshot_pool is not None:
            try:
                row = await self._snapshot_fetchrow(
                    "SELECT 1 FROM accounting_events WHERE ledger_entry_id = $1 LIMIT 1",
                    ledger_entry_id,
                )
                return gateway_pb2.HasAccountingEventsForLedgerResponse(has_events=row is not None)
            except Exception as e:
                # Propagate as gRPC INTERNAL so the client raises instead of receiving
                # has_events=False.  Returning False on a DB failure would conflate
                # "no row" with "lookup failed" and risk re-processing an already-written
                # ledger entry (the client now raises on any gRPC exception).
                logger.error("HasAccountingEventsForLedger PG failed for ledger_id=%s: %s", ledger_entry_id, e)
                context.set_code(grpc.StatusCode.INTERNAL)
                context.set_details("internal server error")
                return gateway_pb2.HasAccountingEventsForLedgerResponse(has_events=False)

        # SQLite path
        try:
            await self._ensure_initialized()
            assert self._state_manager is not None
            warm = self._state_manager.warm_backend
            if warm is None or not hasattr(warm, "has_accounting_events_for_ledger"):
                return gateway_pb2.HasAccountingEventsForLedgerResponse(has_events=False)
            result = await warm.has_accounting_events_for_ledger(ledger_entry_id)
            return gateway_pb2.HasAccountingEventsForLedgerResponse(has_events=bool(result))
        except Exception as e:
            logger.error("HasAccountingEventsForLedger SQLite failed for ledger_id=%s: %s", ledger_entry_id, e)
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details("internal server error")
            return gateway_pb2.HasAccountingEventsForLedgerResponse(has_events=False)

    async def GetLedgerEntry(
        self,
        request: gateway_pb2.GetLedgerEntryRequest,
        context: grpc.aio.ServicerContext,
    ) -> gateway_pb2.GetLedgerEntryResponse:
        """Fetch a transaction_ledger row by id for AccountingProcessor.drain_one.

        Returns the full row as LedgerEntryData so the category handler can
        compute cost basis, PnL, and confidence without a SQLite fallback.
        """
        ledger_entry_id = (request.ledger_entry_id or "").strip()
        if not ledger_entry_id:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details("ledger_entry_id is required")
            return gateway_pb2.GetLedgerEntryResponse(found=False)

        await self._ensure_snapshot_pool()

        if self._snapshot_pool is not None:
            try:
                row = await self._snapshot_fetchrow(
                    """
                    SELECT id, cycle_id, agent_id, deployment_id, execution_mode,
                           EXTRACT(EPOCH FROM timestamp)::bigint AS ts_epoch,
                           intent_type, token_in, amount_in, token_out, amount_out,
                           effective_price, slippage_bps, gas_used, gas_usd,
                           tx_hash, chain, protocol, success, error,
                           extracted_data_json::text AS extracted_data_text,
                           price_inputs_json::text  AS price_inputs_text,
                           pre_state_json::text      AS pre_state_text,
                           post_state_json::text     AS post_state_text
                    FROM transaction_ledger
                    WHERE id = $1
                    LIMIT 1
                    """,
                    ledger_entry_id,
                )
                if row is None:
                    return gateway_pb2.GetLedgerEntryResponse(found=False)

                def _opt_bytes(text: str | None) -> bytes:
                    return text.encode("utf-8") if text else b""

                slippage_raw = row.get("slippage_bps")
                entry = gateway_pb2.LedgerEntryData(
                    id=row["id"] or "",
                    cycle_id=row["cycle_id"] or "",
                    strategy_id=row["agent_id"] or "",
                    deployment_id=row["deployment_id"] or "",
                    execution_mode=row["execution_mode"] or "",
                    timestamp=int(row["ts_epoch"] or 0),
                    intent_type=row["intent_type"] or "",
                    token_in=row["token_in"] or "",
                    amount_in=row["amount_in"] or "",
                    token_out=row["token_out"] or "",
                    amount_out=row["amount_out"] or "",
                    effective_price=row["effective_price"] or "",
                    gas_used=int(row["gas_used"] or 0),
                    gas_usd=row["gas_usd"] or "",
                    tx_hash=row["tx_hash"] or "",
                    chain=row["chain"] or "",
                    protocol=row["protocol"] or "",
                    success=bool(row["success"]),
                    error=row["error"] or "",
                    extracted_data_json=_opt_bytes(row.get("extracted_data_text")),
                    price_inputs_json=_opt_bytes(row.get("price_inputs_text")),
                    pre_state_json=_opt_bytes(row.get("pre_state_text")),
                    post_state_json=_opt_bytes(row.get("post_state_text")),
                )
                if slippage_raw is not None:
                    entry.slippage_bps = float(slippage_raw)
                return gateway_pb2.GetLedgerEntryResponse(found=True, entry=entry)
            except Exception as e:
                logger.error("GetLedgerEntry PG failed for id=%s: %s", ledger_entry_id, e)
                context.set_code(grpc.StatusCode.INTERNAL)
                context.set_details("internal server error")
                return gateway_pb2.GetLedgerEntryResponse(found=False)

        # SQLite path
        try:
            await self._ensure_initialized()
            assert self._state_manager is not None
            warm = self._state_manager.warm_backend
            if warm is None or not hasattr(warm, "get_ledger_entry_by_id"):
                return gateway_pb2.GetLedgerEntryResponse(found=False)
            row = await warm.get_ledger_entry_by_id(ledger_entry_id)
            if row is None:
                return gateway_pb2.GetLedgerEntryResponse(found=False)

            def _opt_bytes_sqlite(val: str | bytes | None) -> bytes:
                if not val:
                    return b""
                return val if isinstance(val, bytes) else val.encode("utf-8")

            ts_raw = row.get("timestamp")
            if isinstance(ts_raw, str):
                try:
                    from datetime import datetime as _dt

                    ts_epoch = int(_dt.fromisoformat(ts_raw).timestamp())
                except Exception:
                    ts_epoch = 0
            else:
                ts_epoch = int(ts_raw or 0)

            entry = gateway_pb2.LedgerEntryData(
                id=row.get("id") or "",
                cycle_id=row.get("cycle_id") or "",
                strategy_id=row.get("strategy_id") or row.get("agent_id") or "",
                deployment_id=row.get("deployment_id") or "",
                execution_mode=row.get("execution_mode") or "",
                timestamp=ts_epoch,
                intent_type=row.get("intent_type") or "",
                token_in=row.get("token_in") or "",
                amount_in=row.get("amount_in") or "",
                token_out=row.get("token_out") or "",
                amount_out=row.get("amount_out") or "",
                effective_price=row.get("effective_price") or "",
                gas_used=int(row.get("gas_used") or 0),
                gas_usd=row.get("gas_usd") or "",
                tx_hash=row.get("tx_hash") or "",
                chain=row.get("chain") or "",
                protocol=row.get("protocol") or "",
                success=bool(row.get("success")),
                error=row.get("error") or "",
                extracted_data_json=_opt_bytes_sqlite(row.get("extracted_data_json")),
                price_inputs_json=_opt_bytes_sqlite(row.get("price_inputs_json")),
                pre_state_json=_opt_bytes_sqlite(row.get("pre_state_json")),
                post_state_json=_opt_bytes_sqlite(row.get("post_state_json")),
            )
            slippage_raw = row.get("slippage_bps")
            if slippage_raw is not None:
                entry.slippage_bps = float(slippage_raw)
            return gateway_pb2.GetLedgerEntryResponse(found=True, entry=entry)
        except Exception as e:
            logger.error("GetLedgerEntry SQLite failed: %s", e)
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details("internal server error")
            return gateway_pb2.GetLedgerEntryResponse(found=False)
