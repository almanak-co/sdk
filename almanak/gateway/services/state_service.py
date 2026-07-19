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
from collections.abc import Iterable
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from typing import TYPE_CHECKING, Any, cast
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
    validate_deployment_id,
    validate_state_size,
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


def _row_text(row: dict[str, Any], key: str) -> Any:
    return row.get(key) or ""


def _row_int(row: dict[str, Any], key: str, default: int = 0) -> int:
    return int(row.get(key) or default)


def _item_text(row: Any, key: str, default: str = "") -> Any:
    return row[key] or default


def _item_int(row: Any, key: str, default: int = 0) -> int:
    return int(row[key] or default)


def _ledger_entry_to_proto(entry: Any) -> gateway_pb2.LedgerEntryInfo:
    """Convert a ``LedgerEntry`` to the wire ``LedgerEntryInfo`` (VIB-5416).

    Mirrors the conversion in ``DashboardService.GetTransactionLedger`` so the
    measured-read RPC returns byte-identical row shapes. ``timestamp`` is a
    ``datetime`` on the dataclass and is serialised to Unix seconds.
    """
    ts = getattr(entry, "timestamp", None)
    ts_epoch = int(ts.timestamp()) if isinstance(ts, datetime) else 0
    return gateway_pb2.LedgerEntryInfo(
        id=entry.id or "",
        cycle_id=entry.cycle_id or "",
        deployment_id=entry.deployment_id or "",
        timestamp=ts_epoch,
        intent_type=entry.intent_type or "",
        token_in=entry.token_in or "",
        amount_in=entry.amount_in or "",
        token_out=entry.token_out or "",
        amount_out=entry.amount_out or "",
        effective_price=entry.effective_price or "",
        slippage_bps=entry.slippage_bps or 0.0,
        gas_used=entry.gas_used or 0,
        gas_usd=entry.gas_usd or "",
        tx_hash=entry.tx_hash or "",
        chain=entry.chain or "",
        protocol=entry.protocol or "",
        success=bool(entry.success),
        error=entry.error or "",
    )


def _pg_row_to_accounting_event(row: Any) -> gateway_pb2.AccountingEvent:
    """Convert one Postgres asyncpg.Record to the proto wire shape.

    The PG SELECT casts ``payload_json::text`` so we get a str (not a dict)
    and re-encode as UTF-8 bytes to match the SaveAccountingEventRequest
    contract. VIB-4721/4722: ``accounting_events`` has a single identity
    column, ``deployment_id``.
    """
    payload_text = _item_text(row, "payload_text", "{}")
    return gateway_pb2.AccountingEvent(
        id=_item_text(row, "id"),
        deployment_id=_item_text(row, "deployment_id"),
        cycle_id=_item_text(row, "cycle_id"),
        execution_mode=_item_text(row, "execution_mode"),
        timestamp=_item_int(row, "ts_epoch"),
        chain=_item_text(row, "chain"),
        protocol=_item_text(row, "protocol"),
        wallet_address=_item_text(row, "wallet_address"),
        event_type=_item_text(row, "event_type"),
        position_key=_item_text(row, "position_key"),
        ledger_entry_id=_item_text(row, "ledger_entry_id"),
        tx_hash=_item_text(row, "tx_hash"),
        confidence=_item_text(row, "confidence"),
        payload_json=payload_text.encode("utf-8"),
        schema_version=_item_int(row, "schema_version", 1),
    )


def _ledger_json_bytes(value: str | bytes | None) -> bytes:
    if not value:
        return b""
    return value if isinstance(value, bytes) else value.encode("utf-8")


def _set_ledger_slippage(entry: gateway_pb2.LedgerEntryData, slippage_raw: Any) -> None:
    if slippage_raw is not None:
        entry.slippage_bps = float(slippage_raw)


def _ledger_entry_from_row(
    row: Any,
    *,
    timestamp: int,
    extracted_data_key: str,
    price_inputs_key: str,
    pre_state_key: str,
    post_state_key: str,
) -> gateway_pb2.LedgerEntryData:
    entry = gateway_pb2.LedgerEntryData(
        id=row.get("id") or "",
        cycle_id=row.get("cycle_id") or "",
        deployment_id=row.get("deployment_id") or "",
        execution_mode=row.get("execution_mode") or "",
        timestamp=timestamp,
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
        extracted_data_json=_ledger_json_bytes(row.get(extracted_data_key)),
        price_inputs_json=_ledger_json_bytes(row.get(price_inputs_key)),
        pre_state_json=_ledger_json_bytes(row.get(pre_state_key)),
        post_state_json=_ledger_json_bytes(row.get(post_state_key)),
    )
    _set_ledger_slippage(entry, row.get("slippage_bps"))
    return entry


def _pg_ledger_entry_row_to_proto(row: Any) -> gateway_pb2.LedgerEntryData:
    return _ledger_entry_from_row(
        row,
        timestamp=int(row.get("ts_epoch") or 0),
        extracted_data_key="extracted_data_text",
        price_inputs_key="price_inputs_text",
        pre_state_key="pre_state_text",
        post_state_key="post_state_text",
    )


def _sqlite_ledger_entry_row_to_proto(row: dict[str, Any]) -> gateway_pb2.LedgerEntryData:
    return _ledger_entry_from_row(
        row,
        timestamp=_row_timestamp_epoch(row),
        extracted_data_key="extracted_data_json",
        price_inputs_key="price_inputs_json",
        pre_state_key="pre_state_json",
        post_state_key="post_state_json",
    )


def _position_event_row_to_proto(row: dict[str, Any]) -> gateway_pb2.PositionEventData:
    """Convert one SQLite ``position_events`` row dict to the proto wire shape (VIB-3944).

    SQLite stores timestamp as an ISO string; the proto carries epoch seconds.
    Optional integer / boolean fields (``tick_lower``, ``tick_upper``,
    ``in_range``, ``is_long``) are only set on the proto when the SQLite row
    has a non-None value, so the wire stays consistent with the SavePositionEvent
    convention where None == absent.
    """
    msg = gateway_pb2.PositionEventData(
        id=row.get("id") or "",
        deployment_id=row.get("deployment_id") or "",
        cycle_id=row.get("cycle_id") or "",
        execution_mode=row.get("execution_mode") or "",
        position_id=row.get("position_id") or "",
        position_type=row.get("position_type") or "",
        event_type=row.get("event_type") or "",
        timestamp=_row_timestamp_epoch(row),
        protocol=row.get("protocol") or "",
        chain=row.get("chain") or "",
        token0=row.get("token0") or "",
        token1=row.get("token1") or "",
        amount0=row.get("amount0") or "",
        amount1=row.get("amount1") or "",
        value_usd=row.get("value_usd") or "",
        liquidity=row.get("liquidity") or "",
        fees_token0=row.get("fees_token0") or "",
        fees_token1=row.get("fees_token1") or "",
        leverage=row.get("leverage") or "",
        entry_price=row.get("entry_price") or "",
        mark_price=row.get("mark_price") or "",
        unrealized_pnl=row.get("unrealized_pnl") or "",
        tx_hash=row.get("tx_hash") or "",
        gas_usd=row.get("gas_usd") or "",
        ledger_entry_id=row.get("ledger_entry_id") or "",
        protocol_fees_usd=row.get("protocol_fees_usd") or "",
        attribution_json=row.get("attribution_json") or "{}",
        attribution_version=int(row.get("attribution_version") or 0),
    )
    tick_lower = row.get("tick_lower")
    if tick_lower is not None:
        msg.tick_lower = int(tick_lower)
    tick_upper = row.get("tick_upper")
    if tick_upper is not None:
        msg.tick_upper = int(tick_upper)
    in_range = row.get("in_range")
    if in_range is not None:
        msg.in_range = bool(in_range)
    is_long = row.get("is_long")
    if is_long is not None:
        msg.is_long = bool(is_long)
    return msg


def _wrap_pg_persistence_error(exc: BaseException, ledger_id: str) -> gateway_pb2.SaveLedgerAndRegistryResponse:
    """Wrap an asyncpg failure as a typed SaveLedgerAndRegistryResponse.

    Builds an :class:`AccountingPersistenceError` from the original
    exception so :meth:`StateServiceServicer._classify_save_ledger_and_registry_error`
    routes it through the SQLite-path's existing typed-error → wire-class
    mapping (LEDGER write-kind). This keeps the wire ``error_class``
    string identical across backends.
    """
    from almanak.framework.state.exceptions import (
        AccountingPersistenceError,
        AccountingWriteKind,
    )

    wrapped = AccountingPersistenceError(
        AccountingWriteKind.LEDGER,
        message=f"Postgres write failed for id={ledger_id}: {exc}",
        cause=exc,
    )
    logger.error(
        "SaveLedgerAndRegistry PG persistence failure for id=%s: %s",
        ledger_id,
        exc,
    )
    return gateway_pb2.SaveLedgerAndRegistryResponse(
        success=False,
        error=str(wrapped),
        error_class="AccountingPersistenceError",
    )


def _sqlite_row_to_accounting_event(row: dict[str, Any]) -> gateway_pb2.AccountingEvent:
    """Convert one SQLite row dict to the proto wire shape.

    SQLite uses ``deployment_id`` as the canonical identity column. Timestamps
    are ISO strings.
    """
    payload_text = row.get("payload_json") or "{}"
    if isinstance(payload_text, bytes):
        payload_text = payload_text.decode("utf-8")
    return gateway_pb2.AccountingEvent(
        id=row.get("id") or "",
        deployment_id=row.get("deployment_id") or "",
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


def _coerce_decimal_str(raw: object) -> str | None:
    """Validate a smuggled decimal scalar before it reaches a TEXT column /
    ``Decimal(...)`` (VIB-5007). Returns the stringified value only when ``raw``
    is a Decimal-parseable scalar; any malformed value (dict/list/bool/None or a
    non-numeric string) degrades to ``None`` so the column keeps its default
    rather than persisting junk on Postgres or crashing ``from_dict`` on the
    SQLite write path. Symmetric with the collection type-guards in
    ``_extract_smuggled_snapshot_fields``.
    """
    if raw is None or isinstance(raw, bool):
        return None
    if not isinstance(raw, str | int | float):
        return None
    try:
        parsed = Decimal(str(raw))
    except (InvalidOperation, ValueError):
        return None
    # NaN / ±Infinity are valid Decimal constructions (and json.loads accepts the
    # literals), but must never reach a money column / Decimal arithmetic.
    if not parsed.is_finite():
        return None
    return str(raw)


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
            request: Load request with deployment_id
            context: gRPC context

        Returns:
            StateData with state bytes, version, checksum
        """
        # Validate deployment_id format BEFORE initialization
        try:
            deployment_id = validate_deployment_id(request.deployment_id)
        except ValidationError as e:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(str(e))
            return gateway_pb2.StateData()

        # One identity (blueprint 29 §4): the caller passes the canonical
        # deployment_id; the gateway filters it directly with no translation.
        await self._ensure_initialized()
        assert self._state_manager is not None

        try:
            state = await self._state_manager.load_state(deployment_id)

            if state is None:
                context.set_code(grpc.StatusCode.NOT_FOUND)
                context.set_details(f"State not found for strategy: {deployment_id}")
                return gateway_pb2.StateData()

            # Serialize state dict to JSON bytes
            state_bytes = json.dumps(state.state).encode("utf-8")

            # Convert StateTier enum to lowercase string for protobuf (matches
            # the gateway.proto:236 contract — "hot"/"warm"). The fallback also
            # uses lowercase so the wire value is consistent regardless of
            # whether state.loaded_from was set or None (issue #2053).
            loaded_from_str = state.loaded_from.name.lower() if state.loaded_from else "warm"

            return gateway_pb2.StateData(
                deployment_id=state.deployment_id,
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
            context.set_details(f"State not found for strategy: {deployment_id}")
            return gateway_pb2.StateData()
        except Exception as e:
            logger.error(f"LoadState failed for {deployment_id}: {e}")
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
            request: Save request with deployment_id, expected_version, data
            context: gRPC context

        Returns:
            SaveStateResponse with success, new_version, checksum
        """
        # Validate inputs BEFORE initialization
        try:
            deployment_id = validate_deployment_id(request.deployment_id)
        except ValidationError as e:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(str(e))
            return gateway_pb2.SaveStateResponse(success=False, error=str(e))

        # One identity (blueprint 29 §4): the validated deployment_id IS the
        # canonical deployment_id; no gateway-side translation.
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
                deployment_id=deployment_id,
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
            logger.error(f"SaveState failed for {deployment_id}: {error_msg}")

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
            request: Delete request with deployment_id
            context: gRPC context

        Returns:
            DeleteStateResponse with success status
        """
        # Validate deployment_id format BEFORE initialization
        try:
            deployment_id = validate_deployment_id(request.deployment_id)
        except ValidationError as e:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(str(e))
            return gateway_pb2.DeleteStateResponse(success=False, error=str(e))

        # One identity (blueprint 29 §4): no gateway-side translation.
        await self._ensure_initialized()
        assert self._state_manager is not None

        try:
            success = await self._state_manager.delete_state(deployment_id)

            if not success:
                return gateway_pb2.DeleteStateResponse(
                    success=False,
                    error=f"State not found for strategy: {deployment_id}",
                )

            return gateway_pb2.DeleteStateResponse(success=True)

        except Exception as e:
            error_msg = str(e)
            logger.error(f"DeleteState failed for {deployment_id}: {error_msg}")
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

    @staticmethod
    def _validate_save_snapshot_payload(request: gateway_pb2.SaveSnapshotRequest) -> str | None:
        """Return an error string if the request payload is invalid, else None.

        Validates timestamp positivity, positions_json well-formedness, and
        envelope shape (legacy list OR ``{positions: list, metadata: dict}``).
        """
        if request.timestamp <= 0:
            return "timestamp must be positive"
        if not request.positions_json:
            return None
        try:
            positions_payload = json.loads(request.positions_json)
        except (json.JSONDecodeError, UnicodeDecodeError):
            return "positions_json must be valid JSON"
        is_legacy = isinstance(positions_payload, list)
        is_envelope = (
            isinstance(positions_payload, dict)
            and isinstance(positions_payload.get("positions", []), list)
            and isinstance(positions_payload.get("metadata", {}), dict)
        )
        if not (is_legacy or is_envelope):
            return "positions_json must be a list or {positions: list, metadata: object}"
        return None

    # VIB-4721/4722 — deployment_id is the sole identity column on
    # portfolio_snapshots (the old identity column was DROPPED by the
    # metrics-database migration). It is part of the (deployment_id,
    # timestamp) unique constraint and NOT NULL, so it is always the
    # caller-supplied canonical id — no asymmetric "preserve once stamped"
    # CASE logic is needed for it. cycle_id/execution_mode remain optional
    # Phase-4 columns and keep the once-stamped-never-blanked guard.
    # VIB-5007 — these four columns exist in the deployed Postgres schema
    # (defaults '0'/'0'/'[]'/'{}') but were never bound by the INSERT, so
    # every hosted snapshot persisted them at default. The values cannot ride
    # the proto wire (SaveSnapshotRequest has no slots — see VIB-3894); they
    # are smuggled through the positions_json envelope and lifted by
    # ``_extract_smuggled_snapshot_fields``, mirroring the SQLite path.
    _SAVE_SNAPSHOT_PG_SQL = """
        INSERT INTO portfolio_snapshots (
            deployment_id, timestamp, iteration_number, total_value_usd,
            available_cash_usd, value_confidence, positions_json, chain, created_at,
            deployed_capital_usd, wallet_total_value_usd, wallet_balances_json,
            token_prices_json, cycle_id, execution_mode
        ) VALUES (
            $1, $2, $3, $4, $5, $6, $7::jsonb, $8, $9, $10, $11, $12::jsonb,
            $13::jsonb, $14, $15
        )
        ON CONFLICT (deployment_id, timestamp) DO UPDATE SET
            iteration_number = EXCLUDED.iteration_number,
            total_value_usd = EXCLUDED.total_value_usd,
            available_cash_usd = EXCLUDED.available_cash_usd,
            value_confidence = EXCLUDED.value_confidence,
            positions_json = EXCLUDED.positions_json,
            chain = EXCLUDED.chain,
            -- VIB-5007 — value-aware update: a conflicting upsert for the same
            -- (deployment_id, timestamp) from a degraded or legacy envelope
            -- (missing/invalid wallet fields → bound as defaults) must NOT wipe
            -- already-persisted values back to defaults. Mirrors the
            -- once-stamped-never-blanked guard on cycle_id/execution_mode below:
            -- a default EXCLUDED preserves the existing column; a real value
            -- still lands (incl. upgrading a prior default to a measured value).
            deployed_capital_usd = CASE
                WHEN EXCLUDED.deployed_capital_usd IS NULL
                  OR EXCLUDED.deployed_capital_usd = '0'
                THEN portfolio_snapshots.deployed_capital_usd
                ELSE EXCLUDED.deployed_capital_usd
            END,
            wallet_total_value_usd = CASE
                WHEN EXCLUDED.wallet_total_value_usd IS NULL
                  OR EXCLUDED.wallet_total_value_usd = '0'
                THEN portfolio_snapshots.wallet_total_value_usd
                ELSE EXCLUDED.wallet_total_value_usd
            END,
            wallet_balances_json = CASE
                WHEN EXCLUDED.wallet_balances_json IS NULL
                  OR EXCLUDED.wallet_balances_json = '[]'::jsonb
                THEN portfolio_snapshots.wallet_balances_json
                ELSE EXCLUDED.wallet_balances_json
            END,
            token_prices_json = CASE
                WHEN EXCLUDED.token_prices_json IS NULL
                  OR EXCLUDED.token_prices_json = '{}'::jsonb
                THEN portfolio_snapshots.token_prices_json
                ELSE EXCLUDED.token_prices_json
            END,
            cycle_id = CASE
                WHEN portfolio_snapshots.cycle_id IS NULL
                  OR portfolio_snapshots.cycle_id = ''
                THEN EXCLUDED.cycle_id
                ELSE portfolio_snapshots.cycle_id
            END,
            execution_mode = CASE
                WHEN portfolio_snapshots.execution_mode IS NULL
                  OR portfolio_snapshots.execution_mode = ''
                THEN EXCLUDED.execution_mode
                ELSE portfolio_snapshots.execution_mode
            END
        RETURNING id
        """

    @staticmethod
    def _extract_smuggled_snapshot_fields(
        positions_payload: object,
        metadata: object,
    ) -> tuple[str | None, str | None, list | None, dict | None]:
        """Lift the four snapshot fields the ``SaveSnapshotRequest`` proto wire
        cannot carry directly (VIB-3894 / Phase 1c / VIB-5007).

        ``deployed_capital_usd`` and ``wallet_total_value_usd`` are smuggled
        through the envelope ``metadata`` under double-underscore keys and are
        **popped** so the persisted metadata stays clean; ``wallet_balances``
        and ``token_prices`` ride on the envelope payload.

        Returns ``(deployed_capital_usd, wallet_total_value_usd,
        wallet_balances, token_prices)``. Each element is ``None`` when the
        source key is absent so callers can distinguish "unset — keep the
        column/constructor default" from a measured value. Shared by the
        SQLite and Postgres write paths so the two backends cannot drift
        (the drift WAS the VIB-5007 bug).
        """
        deployed_capital_usd: str | None = None
        wallet_total_value_usd: str | None = None
        if isinstance(metadata, dict):
            deployed_capital_usd = _coerce_decimal_str(metadata.pop("__deployed_capital_usd__", None))
            wallet_total_value_usd = _coerce_decimal_str(metadata.pop("__wallet_total_value_usd__", None))
        # Type-guard the envelope sub-fields (defense-in-depth at the
        # persistence boundary): ``_validate_save_snapshot_payload`` checks the
        # top-level shape but not these inner types. A malformed payload (e.g.
        # ``wallet_balances`` as a string) must degrade to the column/constructor
        # default, not crash ``PortfolioSnapshot.from_dict`` on the SQLite write
        # path. Mirrors the isinstance guards on the read side
        # (``_pg_row_to_portfolio_snapshot``).
        wallet_balances: list | None = None
        token_prices: dict | None = None
        if isinstance(positions_payload, dict):
            wb_raw = positions_payload.get("wallet_balances")
            if isinstance(wb_raw, list):
                wallet_balances = wb_raw
            tp_raw = positions_payload.get("token_prices")
            if isinstance(tp_raw, dict):
                token_prices = tp_raw
        return deployed_capital_usd, wallet_total_value_usd, wallet_balances, token_prices

    async def _save_snapshot_postgres(
        self,
        deployment_id: str,
        ts: datetime,
        now: datetime,
        request: gateway_pb2.SaveSnapshotRequest,
    ) -> int:
        """Run the snapshot upsert against Postgres and return the row id.

        VIB-5007 — binds ``deployed_capital_usd`` / ``wallet_total_value_usd``
        / ``wallet_balances_json`` / ``token_prices_json`` lifted from the
        envelope, mirroring ``_build_sqlite_snapshot``. ``positions_json`` is
        persisted verbatim (smuggle keys retained, matching prior behaviour).
        """
        positions_json = request.positions_json.decode("utf-8") if request.positions_json else "[]"
        try:
            positions_payload: object = json.loads(positions_json)
        except (ValueError, TypeError):
            positions_payload = None
        metadata = positions_payload.get("metadata") if isinstance(positions_payload, dict) else None
        dep, wtv, wallet_balances, token_prices = self._extract_smuggled_snapshot_fields(positions_payload, metadata)
        row = await self._snapshot_fetchrow(
            self._SAVE_SNAPSHOT_PG_SQL,
            deployment_id,
            ts,
            request.iteration_number,
            request.total_value_usd,
            request.available_cash_usd,
            request.value_confidence or "HIGH",
            positions_json,
            request.chain,
            now,
            dep if dep is not None else "0",
            wtv if wtv is not None else "0",
            json.dumps(wallet_balances if wallet_balances is not None else []),
            json.dumps(token_prices if token_prices is not None else {}),
            request.cycle_id or "",
            request.execution_mode or "",
        )
        return row["id"] if row else 0

    @staticmethod
    def _build_sqlite_snapshot(
        deployment_id: str,
        ts: datetime,
        request: gateway_pb2.SaveSnapshotRequest,
    ):
        """Rebuild a PortfolioSnapshot from the wire request for the SQLite
        writer. Pulls smuggled cash-split fields out of envelope metadata
        (VIB-3894) and lifts token_prices / wallet_balances off the
        envelope (Phase 1c)."""
        from decimal import Decimal

        from almanak.framework.portfolio.models import PortfolioSnapshot, ValueConfidence

        snapshot = PortfolioSnapshot(
            timestamp=ts,
            total_value_usd=Decimal(request.total_value_usd or "0"),
            available_cash_usd=Decimal(request.available_cash_usd or "0"),
            value_confidence=ValueConfidence(request.value_confidence or "HIGH"),
            chain=request.chain,
            iteration_number=request.iteration_number,
            # VIB-4095 (3.4) — Phase 4 identity reaches the SQLite writer
            # (VIB-4096 / 3.5) on the rebuilt object. Source: framework
            # client (3.3) populated the proto from runner-stamped snapshot
            # fields (3.8).
            deployment_id=request.deployment_id or deployment_id or "",
            cycle_id=request.cycle_id or "",
            execution_mode=request.execution_mode or "",
        )
        if not request.positions_json:
            return snapshot

        positions_payload = json.loads(request.positions_json.decode("utf-8"))
        positions, snapshot_metadata = PortfolioSnapshot.unpack_positions_payload(positions_payload)
        snapshot_dict = snapshot.to_dict()
        snapshot_dict["positions"] = positions
        snapshot_dict["snapshot_metadata"] = snapshot_metadata
        # VIB-3894 / VIB-5007 — SaveSnapshotRequest cannot carry
        # ``deployed_capital_usd`` / ``wallet_total_value_usd`` /
        # ``wallet_balances`` / ``token_prices`` on the proto wire. The runner's
        # GatewayStateManager smuggles them through the envelope; lift them onto
        # the rebuilt snapshot (shared with the Postgres path) so the SQLite
        # writer persists the actual values rather than the ``Decimal("0")`` /
        # empty defaults. ``snapshot_metadata`` is mutated in place (smuggle
        # keys popped) so the persisted metadata stays clean.
        dep_str, wtv_str, wallet_balances, token_prices = StateServiceServicer._extract_smuggled_snapshot_fields(
            positions_payload, snapshot_metadata
        )
        if dep_str is not None:
            snapshot_dict["deployed_capital_usd"] = dep_str
        if wtv_str is not None:
            snapshot_dict["wallet_total_value_usd"] = wtv_str
        if token_prices is not None:
            snapshot_dict["token_prices"] = token_prices
        if wallet_balances is not None:
            snapshot_dict["wallet_balances"] = wallet_balances
        return PortfolioSnapshot.from_dict(snapshot_dict)

    async def _save_snapshot_sqlite(
        self,
        deployment_id: str,
        ts: datetime,
        request: gateway_pb2.SaveSnapshotRequest,
    ) -> int:
        """Persist the snapshot via StateManager's warm SQLiteStore backend."""
        await self._ensure_initialized()
        assert self._state_manager is not None
        warm = self._state_manager.warm_backend
        assert warm is not None
        snapshot = self._build_sqlite_snapshot(deployment_id, ts, request)
        return await warm.save_portfolio_snapshot(snapshot)  # type: ignore[attr-defined]

    async def SavePortfolioSnapshot(
        self,
        request: gateway_pb2.SaveSnapshotRequest,
        context: grpc.aio.ServicerContext,
    ) -> gateway_pb2.SaveSnapshotResponse:
        """Save a portfolio snapshot to the portfolio_snapshots table."""
        try:
            deployment_id = validate_deployment_id(request.deployment_id)
        except ValidationError as e:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(str(e))
            return gateway_pb2.SaveSnapshotResponse(success=False, error=str(e))

        payload_error = self._validate_save_snapshot_payload(request)
        if payload_error:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(payload_error)
            return gateway_pb2.SaveSnapshotResponse(success=False, error=payload_error)

        await self._ensure_snapshot_pool()
        ts = datetime.fromtimestamp(request.timestamp, tz=UTC)

        backend_label = "Postgres" if self._snapshot_pool is not None else "SQLite"
        try:
            if self._snapshot_pool is not None:
                snapshot_id = await self._save_snapshot_postgres(deployment_id, ts, datetime.now(UTC), request)
            else:
                snapshot_id = await self._save_snapshot_sqlite(deployment_id, ts, request)
            return gateway_pb2.SaveSnapshotResponse(success=True, snapshot_id=snapshot_id)
        except Exception as e:
            logger.error(f"SavePortfolioSnapshot ({backend_label}) failed for {deployment_id}: {e}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details("internal server error")
            return gateway_pb2.SaveSnapshotResponse(success=False, error="internal server error")

    async def GetLatestSnapshot(
        self,
        request: gateway_pb2.GetLatestSnapshotRequest,
        context: grpc.aio.ServicerContext,
    ) -> gateway_pb2.SnapshotData:
        """Get the most recent portfolio snapshot.

        Both Postgres (`PostgresStore.get_latest_snapshot`) and SQLite
        (`SQLiteStore.get_latest_snapshot`) backends now implement this read
        identically — VIB-3933 closed the hosted-Postgres reader gap. Single
        delegation path; no inline SQL or backend branching.
        """
        try:
            deployment_id = validate_deployment_id(request.deployment_id)
        except ValidationError as e:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(str(e))
            return gateway_pb2.SnapshotData(found=False)

        try:
            await self._ensure_initialized()
            assert self._state_manager is not None
            warm = self._state_manager.warm_backend
            if warm is None or not hasattr(warm, "get_latest_snapshot"):
                return gateway_pb2.SnapshotData(found=False)
            snapshot = await warm.get_latest_snapshot(deployment_id)
            if snapshot is None:
                return gateway_pb2.SnapshotData(found=False)
            return self._snapshot_to_proto(snapshot)
        except Exception as e:
            logger.error(f"GetLatestSnapshot failed for {deployment_id}: {e}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details("internal server error")
            return gateway_pb2.SnapshotData(found=False)

    async def GetSnapshotsSince(
        self,
        request: gateway_pb2.GetSnapshotsSinceRequest,
        context: grpc.aio.ServicerContext,
    ) -> gateway_pb2.SnapshotList:
        """Get portfolio snapshots since a given timestamp.

        See :meth:`GetLatestSnapshot` for the dedup rationale (VIB-3933 Phase 2).
        """
        try:
            deployment_id = validate_deployment_id(request.deployment_id)
        except ValidationError as e:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(str(e))
            return gateway_pb2.SnapshotList()

        since = datetime.fromtimestamp(request.since, tz=UTC)
        limit = min(request.limit if request.limit > 0 else 168, MAX_SNAPSHOTS)

        try:
            await self._ensure_initialized()
            assert self._state_manager is not None
            warm = self._state_manager.warm_backend
            if warm is None or not hasattr(warm, "get_snapshots_since"):
                return gateway_pb2.SnapshotList()
            snapshots = await warm.get_snapshots_since(deployment_id, since, limit)
            return gateway_pb2.SnapshotList(snapshots=[self._snapshot_to_proto(s) for s in snapshots])
        except Exception as e:
            logger.error(f"GetSnapshotsSince failed for {deployment_id}: {e}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details("internal server error")
            return gateway_pb2.SnapshotList()

    @staticmethod
    def _snapshot_to_proto(snapshot: Any) -> gateway_pb2.SnapshotData:
        """Convert a PortfolioSnapshot to a SnapshotData protobuf message."""
        positions_bytes = json.dumps(snapshot.to_positions_payload()).encode("utf-8")
        return gateway_pb2.SnapshotData(
            timestamp=int(snapshot.timestamp.timestamp()),
            iteration_number=snapshot.iteration_number,
            total_value_usd=str(snapshot.total_value_usd),
            available_cash_usd=str(snapshot.available_cash_usd),
            value_confidence=snapshot.value_confidence.value,
            positions_json=positions_bytes,
            chain=snapshot.chain or "",
            found=True,
            # VIB-4097 (3.6) — Phase 4 identity on the read response.
            deployment_id=snapshot.deployment_id,
            cycle_id=getattr(snapshot, "cycle_id", "") or "",
            execution_mode=getattr(snapshot, "execution_mode", "") or "",
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

        1. ``validate_deployment_id`` (local, small).
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
            deployment_id = validate_deployment_id(request.deployment_id)
        except ValidationError as e:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(str(e))
            return gateway_pb2.SaveMetricsResponse(success=False, error=str(e))

        try:
            inputs = parse_metrics_inputs(request, deployment_id)
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
            total_value_usd = await resolve_total_value_usd(warm, inputs.deployment_id)

            await self._snapshot_fetchrow(
                PG_UPSERT_QUERY,
                *build_pg_upsert_args(inputs, request, now, total_value_usd),
            )
            logger.debug("Portfolio metrics saved for strategy=%s", inputs.deployment_id)
            return gateway_pb2.SaveMetricsResponse(success=True)
        except Exception as e:
            logger.error("SavePortfolioMetrics failed for %s: %s", inputs.deployment_id, e)
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

            total_value_usd = await resolve_total_value_usd(warm, inputs.deployment_id)
            metrics = build_portfolio_metrics(inputs, request, total_value_usd)

            if warm and hasattr(warm, "save_portfolio_metrics"):
                result = await warm.save_portfolio_metrics(metrics)
                if result:
                    logger.debug("Portfolio metrics saved (SQLite) for strategy=%s", inputs.deployment_id)
                    return gateway_pb2.SaveMetricsResponse(success=True)
                return gateway_pb2.SaveMetricsResponse(
                    success=False, error="Backend save_portfolio_metrics returned False"
                )

            return gateway_pb2.SaveMetricsResponse(
                success=False, error="No warm backend with portfolio metrics support"
            )
        except Exception as e:
            logger.error("SavePortfolioMetrics (SQLite) failed for %s: %s", inputs.deployment_id, e)
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details("internal server error")
            return gateway_pb2.SaveMetricsResponse(success=False, error="internal server error")

    @staticmethod
    def _pg_portfolio_metrics_to_proto(row: Any) -> gateway_pb2.PortfolioMetricsData:
        return gateway_pb2.PortfolioMetricsData(
            initial_value_usd=row["initial_value_usd"],
            initial_timestamp=int(row["initial_timestamp"].timestamp()),
            # Empty≠Zero: '' is the UNMEASURED sentinel for these two columns
            # and must reach the client verbatim — coercing it to "0" would
            # fabricate a measured zero. SQL NULL keeps the historical "0"
            # (legacy rows predate the sentinel). VIB-5866.
            deposits_usd=row["deposits_usd"] if row["deposits_usd"] is not None else "0",
            withdrawals_usd=row["withdrawals_usd"] if row["withdrawals_usd"] is not None else "0",
            gas_spent_usd=row["gas_spent_usd"] or "0",
            updated_at=int(row["updated_at"].timestamp()),
            found=True,
            deployment_id=row["deployment_id"] or "",
            cycle_id=row["cycle_id"] or "",
            execution_mode=row["execution_mode"] or "",
            is_complete=bool(row["is_complete"]) if row["is_complete"] is not None else True,
        )

    @staticmethod
    def _sqlite_portfolio_metrics_to_proto(metrics: Any) -> gateway_pb2.PortfolioMetricsData:
        from almanak.framework.portfolio.models import encode_optional_flow

        return gateway_pb2.PortfolioMetricsData(
            initial_value_usd=str(metrics.initial_value_usd),
            initial_timestamp=int(metrics.timestamp.timestamp()),
            # Empty≠Zero: unmeasured flows travel as '' (VIB-5866).
            deposits_usd=encode_optional_flow(metrics.deposits_usd),
            withdrawals_usd=encode_optional_flow(metrics.withdrawals_usd),
            gas_spent_usd=str(metrics.gas_spent_usd),
            updated_at=int(metrics.timestamp.timestamp()),
            found=True,
            deployment_id=metrics.deployment_id,
            cycle_id=getattr(metrics, "cycle_id", "") or "",
            execution_mode=getattr(metrics, "execution_mode", "") or "",
            is_complete=getattr(metrics, "is_complete", True),
        )

    def _portfolio_metrics_error_response(
        self,
        context: grpc.aio.ServicerContext,
    ) -> gateway_pb2.PortfolioMetricsData:
        context.set_code(grpc.StatusCode.INTERNAL)
        context.set_details("internal server error")
        return gateway_pb2.PortfolioMetricsData(found=False)

    async def _get_portfolio_metrics_pg(
        self,
        deployment_id: str,
        context: grpc.aio.ServicerContext,
    ) -> gateway_pb2.PortfolioMetricsData:
        try:
            row = await self._snapshot_fetchrow(
                """
                SELECT initial_value_usd, initial_timestamp,
                       deposits_usd, withdrawals_usd, gas_spent_usd,
                       deployment_id, cycle_id, execution_mode, is_complete,
                       updated_at
                FROM portfolio_metrics
                WHERE deployment_id = $1
                """,
                deployment_id,
            )
            if row is None:
                return gateway_pb2.PortfolioMetricsData(found=False)
            return self._pg_portfolio_metrics_to_proto(row)
        except Exception as e:
            logger.error("GetPortfolioMetrics failed for %s: %s", deployment_id, e)
            return self._portfolio_metrics_error_response(context)

    async def _get_portfolio_metrics_sqlite(
        self,
        deployment_id: str,
        context: grpc.aio.ServicerContext,
    ) -> gateway_pb2.PortfolioMetricsData:
        # SQLite mode (local dev) — delegate to StateManager's SQLiteStore.
        try:
            await self._ensure_initialized()
            assert self._state_manager is not None

            warm = self._state_manager.warm_backend
            if not warm or not hasattr(warm, "get_portfolio_metrics"):
                return gateway_pb2.PortfolioMetricsData(found=False)

            metrics = await warm.get_portfolio_metrics(deployment_id)
            if metrics is None:
                return gateway_pb2.PortfolioMetricsData(found=False)
            return self._sqlite_portfolio_metrics_to_proto(metrics)
        except Exception as e:
            logger.error("GetPortfolioMetrics (SQLite) failed for %s: %s", deployment_id, e)
            return self._portfolio_metrics_error_response(context)

    async def GetPortfolioMetrics(
        self,
        request: gateway_pb2.GetMetricsRequest,
        context: grpc.aio.ServicerContext,
    ) -> gateway_pb2.PortfolioMetricsData:
        """Get portfolio metrics for a strategy."""
        try:
            deployment_id = validate_deployment_id(request.deployment_id)
        except ValidationError as e:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(str(e))
            return gateway_pb2.PortfolioMetricsData(found=False)

        await self._ensure_snapshot_pool()

        if self._snapshot_pool is not None:
            return await self._get_portfolio_metrics_pg(deployment_id, context)
        return await self._get_portfolio_metrics_sqlite(deployment_id, context)

    # =========================================================================
    # Transaction Ledger RPC (VIB-3201)
    # =========================================================================

    async def SaveLedgerEntry(  # noqa: C901
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
            deployment_id = validate_deployment_id(request.deployment_id)
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
                        id, cycle_id, deployment_id, execution_mode,
                        timestamp, intent_type,
                        token_in, amount_in, token_out, amount_out,
                        effective_price, slippage_bps, gas_used, gas_usd,
                        tx_hash, chain, protocol, success, error,
                        extracted_data_json, price_inputs_json, pre_state_json, post_state_json
                    ) VALUES (
                        $1, $2, $3, $4, $5, $6, $7, $8, $9,
                        $10, $11, $12, $13, $14, $15, $16, $17, $18, $19,
                        $20::jsonb, $21::jsonb, $22::jsonb, $23::jsonb
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
                logger.error("SaveLedgerEntry failed for %s (id=%s): %s", deployment_id, request.id, e)
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
                    logger.error("SaveLedgerEntry (SQLite) unsupported for %s: %s", deployment_id, error)
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
                logger.error("SaveLedgerEntry (SQLite) failed for %s (id=%s): %s", deployment_id, request.id, e)
                context.set_code(grpc.StatusCode.INTERNAL)
                context.set_details("internal server error")
                return gateway_pb2.SaveLedgerEntryResponse(success=False, error="internal server error")

    # =========================================================================
    # Accounting Events RPC (VIB-3449)
    # =========================================================================

    async def SaveAccountingEvent(  # noqa: C901
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
            deployment_id = validate_deployment_id(request.deployment_id)
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
            # table. VIB-4721/4722: the PG schema now carries a single
            # identity column, `deployment_id`; it is written the canonical deployment id with no
            # gateway-side translation (blueprint 29 §4-5). UPSERT
            # by `id` is exercised by retries -- the UUIDv5 id is deterministic
            # in (deployment, cycle, intent_type, tx, position) so re-delivery
            # of the same event collapses to one row. Per ticket spec
            # corrections are welcome: ON CONFLICT DO UPDATE refreshes all
            # non-id columns so the latest write wins.
            try:
                await self._snapshot_execute(
                    """
                    INSERT INTO accounting_events (
                        id, deployment_id, cycle_id, execution_mode,
                        timestamp, chain, protocol, wallet_address, event_type,
                        position_key, ledger_entry_id, tx_hash, confidence,
                        payload_json, schema_version
                    ) VALUES (
                        $1, $2, $3, $4, $5, $6, $7, $8, $9,
                        $10, $11, $12, $13, $14::jsonb, $15
                    )
                    ON CONFLICT (id) DO UPDATE SET
                        deployment_id   = EXCLUDED.deployment_id,
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
                    deployment_id,
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
                    deployment_id,
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

    @staticmethod
    def _validate_position_state_rows(
        rows: Iterable[gateway_pb2.PositionStateSnapshotRow],
    ) -> str | None:
        """Boundary-check per-row required fields for
        :meth:`SavePositionStateSnapshots`. Returns the first violation
        message or ``None`` if all rows are well-formed.

        Runs BEFORE the warm-backend capability check so malformed input
        surfaces as ``INVALID_ARGUMENT`` rather than being masked as
        ``UNIMPLEMENTED`` on hosted backends (CodeRabbit P1, 2026-05-17).
        Extracted from the handler to keep its CC under threshold.
        """
        # Derive accepted enum sets from the SAME Literal type the typed
        # row uses, so adding a new position_type / confidence value in
        # ``position_state.py`` automatically flows here without a parallel
        # hardcoded frozenset (Claude pr-auditor P3, 2026-05-17).
        from typing import get_args

        from almanak.framework.accounting.position_state import (
            ConfidenceLiteral,
        )
        from almanak.framework.accounting.position_state import (
            PositionType as PositionTypeLiteral,
        )

        valid_position_types = frozenset(get_args(PositionTypeLiteral))
        valid_confidences = frozenset(get_args(ConfidenceLiteral))

        for idx, proto_row in enumerate(rows):
            if not (proto_row.deployment_id or "").strip():
                return f"rows[{idx}].deployment_id is required"
            if not (proto_row.deployment_id or "").strip():
                return f"rows[{idx}].deployment_id is required"
            if proto_row.position_type not in valid_position_types:
                return f"rows[{idx}].position_type unknown: {proto_row.position_type!r}"
            # Whitespace-only position_id passes ``if not proto_row.position_id``
            # because Python truthiness on "   " is True; strip first
            # (CodeRabbit P3, 2026-05-17).
            stripped_position_id = (proto_row.position_id or "").strip()
            if not stripped_position_id:
                return f"rows[{idx}].position_id is required"
            # position_id is intentionally free-form (materializer's
            # fallback id can include the human-readable label with spaces,
            # e.g. "morpho_blue:ethereum:morpho_blue SUPPLY"). Guard
            # against pathological sizes + ASCII control chars at the
            # gateway boundary (CodeRabbit P3 / 2026-05-17 second-round
            # "Gateway is the security boundary").
            if len(stripped_position_id) > 256:
                return f"rows[{idx}].position_id rejected: must be ≤256 chars"
            if any(ord(c) < 32 for c in stripped_position_id if c != "\t"):
                return f"rows[{idx}].position_id rejected: contains ASCII control character"
            # ``PositionStateRow.timestamp`` is non-optional (sqlite.py:560
            # ``captured_at TEXT NOT NULL``). Validate presence AND ISO-8601
            # shape at the boundary rather than constructing a typed row with
            # a synthetic timestamp or letting the parse-failure mask as
            # UNIMPLEMENTED on hosted.
            captured_at = (proto_row.captured_at or "").strip()
            if not captured_at:
                return f"rows[{idx}].captured_at is required"
            try:
                datetime.fromisoformat(captured_at)
            except ValueError:
                return f"rows[{idx}].captured_at is not ISO-8601: {captured_at!r}"
            if proto_row.value_confidence and proto_row.value_confidence not in valid_confidences:
                return (
                    f"rows[{idx}].value_confidence unknown: {proto_row.value_confidence!r} "
                    f"(expected one of {sorted(valid_confidences)})"
                )
        return None

    async def SavePositionStateSnapshots(
        self,
        request: gateway_pb2.SavePositionStateSnapshotsRequest,
        context: grpc.aio.ServicerContext,
    ) -> gateway_pb2.SavePositionStateSnapshotsResponse:
        """Bulk-persist Track-C per-iteration position state rows (VIB-3891 / VIB-4541).

        Delegates to the warm backend's ``save_position_state_snapshots``
        method (SQLite implementation at
        ``almanak/framework/state/backends/sqlite.py``). When the warm
        backend lacks the method (hosted Postgres pre-metrics-database
        migration, PRD T-DRAFT-25) returns UNIMPLEMENTED so the client
        (GatewayStateManager) can map it to a silent zero — matching the
        runner's deployment-time capability-gate semantics at
        runner_state.py:480.

        Boundary validation matches :meth:`SavePositionEvent` — reject
        blank snapshot_id / per-row deployment_id / per-row position_id /
        unknown position_type values rather than persisting corrupt rows.
        """
        if request.snapshot_id <= 0:
            err = "snapshot_id must be positive"
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(err)
            return gateway_pb2.SavePositionStateSnapshotsResponse(success=False, error=err)

        if not request.rows:
            # Empty rows is a measured zero per AccountingPersistenceError
            # contract — return success=True so the client's path keeps the
            # "0 = measured" semantic intact.
            return gateway_pb2.SavePositionStateSnapshotsResponse(success=True, rows_written=0)

        validation_error = self._validate_position_state_rows(request.rows)
        if validation_error is not None:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(validation_error)
            return gateway_pb2.SavePositionStateSnapshotsResponse(success=False, error=validation_error)

        try:
            await self._ensure_initialized()
            assert self._state_manager is not None
            warm = self._state_manager.warm_backend
            if warm is None or not hasattr(warm, "save_position_state_snapshots"):
                error = "warm backend does not support save_position_state_snapshots"
                logger.warning(
                    "SavePositionStateSnapshots unsupported for snapshot_id=%d: %s "
                    "(expected on hosted PG pre-metrics-database migration)",
                    request.snapshot_id,
                    error,
                )
                context.set_code(grpc.StatusCode.UNIMPLEMENTED)
                context.set_details(error)
                return gateway_pb2.SavePositionStateSnapshotsResponse(success=False, error=error)

            from almanak.framework.accounting.position_state import (
                ConfidenceLiteral,
                PositionStateRow,
                PositionType,
            )

            class _RowFieldInvalid(Exception):
                """Raised by ``_opt_decimal`` / ``_opt_int`` when a present-but-malformed
                wire value can't be parsed. Caught in the row-building loop below and
                surfaced as ``INVALID_ARGUMENT`` rather than the generic ``INTERNAL``
                error the outer ``except Exception`` would otherwise emit (CodeRabbit
                P1, 2026-05-17 — "Gateway is the security boundary; verify input
                validation on all service methods")."""

            def _opt_decimal(field_name: str, row: gateway_pb2.PositionStateSnapshotRow) -> Decimal | None:
                # HasField()==False ⇒ unmeasured (None). HasField()==True ⇒ measured;
                # the wire string is a Decimal-parseable representation per the
                # client-side serializer that wrote it. Empty != Zero (CLAUDE.md
                # §Accounting): "0" arrives as Decimal("0"), absence as None.
                # The proto stubs declare HasField with a narrow Literal of valid
                # field names; the helper signature is intentionally generic
                # (one helper drives ~14 optional Decimal fields) so the type
                # ignore is the standard proto-typed-stub trade-off.
                if not row.HasField(field_name):  # type: ignore[arg-type]
                    return None
                raw = getattr(row, field_name)
                try:
                    return Decimal(raw)
                except InvalidOperation as exc:
                    raise _RowFieldInvalid(f"{field_name}={raw!r} is not a valid Decimal") from exc

            def _opt_int(field_name: str, row: gateway_pb2.PositionStateSnapshotRow) -> int | None:
                if not row.HasField(field_name):  # type: ignore[arg-type]
                    return None
                raw = getattr(row, field_name)
                try:
                    # proto int64 already arrives as int; sqrt_price_x96 / liquidity
                    # ride a string field (uint256 cannot fit int64) — cast handles both.
                    return int(raw)
                except (TypeError, ValueError) as exc:
                    raise _RowFieldInvalid(f"{field_name}={raw!r} is not a valid int") from exc

            warm_rows: list[PositionStateRow] = []
            for idx, proto_row in enumerate(request.rows):
                # Strip + validate identifiers per row (CodeRabbit
                # P3 + Claude pr-auditor P3, 2026-05-17). validate_deployment_id
                # blocks 1MB strings / control chars. Per blueprint 29 §4 the
                # gateway does NOT translate identity — the caller passes the
                # canonical deployment_id and it is used directly.
                # captured_at is already ISO-8601 valid per the boundary
                # validator above — re-parse here just to build the typed value.
                stripped_deployment_id = (proto_row.deployment_id or "").strip()
                stripped_position_id = (proto_row.position_id or "").strip()
                # validate_deployment_id covers character class + length limits
                # for the canonical deployment id (see
                # docs/internal/blueprints/06-state-management.md §"Deployment-ID
                # conventions"). position_id is intentionally
                # free-form: the Track-C materializer's fallback id is
                # ``"{protocol}:{chain}:{label}"`` which can contain spaces
                # via the human-readable label (e.g.
                # ``"morpho_blue:ethereum:morpho_blue SUPPLY"``).
                # For position_id we only guard against pathological sizes
                # and embedded control characters — the symmetric security-
                # boundary concern (CodeRabbit P3, 2026-05-17 second-round).
                try:
                    validate_deployment_id(stripped_deployment_id, f"rows[{idx}].deployment_id")
                except Exception as ve:  # noqa: BLE001 — surface as INVALID_ARGUMENT
                    err = f"rows[{idx}] identifier rejected: {ve}"
                    context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
                    context.set_details(err)
                    return gateway_pb2.SavePositionStateSnapshotsResponse(success=False, error=err)
                captured_at = datetime.fromisoformat(proto_row.captured_at.strip())

                try:
                    warm_rows.append(
                        PositionStateRow(
                            snapshot_id=None,  # warm backend stamps from request.snapshot_id
                            deployment_id=stripped_deployment_id,
                            cycle_id=proto_row.cycle_id,
                            timestamp=captured_at,
                            position_id=stripped_position_id,
                            position_type=cast(PositionType, proto_row.position_type),
                            current_tick=_opt_int("current_tick", proto_row),
                            in_range=proto_row.in_range if proto_row.HasField("in_range") else None,
                            liquidity=_opt_int("liquidity", proto_row),
                            sqrt_price_x96=_opt_int("sqrt_price_x96", proto_row),
                            supply_balance=_opt_decimal("supply_balance", proto_row),
                            borrow_balance=_opt_decimal("borrow_balance", proto_row),
                            health_factor=_opt_decimal("health_factor", proto_row),
                            supply_apy_pct=_opt_decimal("supply_apy_pct", proto_row),
                            borrow_apy_pct=_opt_decimal("borrow_apy_pct", proto_row),
                            interest_accrued_since_last=_opt_decimal("interest_accrued_since_last", proto_row),
                            mark_price=_opt_decimal("mark_price", proto_row),
                            unrealized_pnl=_opt_decimal("unrealized_pnl", proto_row),
                            funding_accrued_since_last=_opt_decimal("funding_accrued_since_last", proto_row),
                            liquidation_price=_opt_decimal("liquidation_price", proto_row),
                            margin_utilisation_pct=_opt_decimal("margin_utilisation_pct", proto_row),
                            delta_vs_protocol_pct=_opt_decimal("delta_vs_protocol_pct", proto_row),
                            value_confidence=cast(ConfidenceLiteral, proto_row.value_confidence),
                            schema_version=int(proto_row.schema_version),
                            formula_version=int(proto_row.formula_version),
                            matching_policy_version=int(proto_row.matching_policy_version),
                        )
                    )
                except _RowFieldInvalid as exc:
                    err = f"rows[{idx}] {exc}"
                    context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
                    context.set_details(err)
                    return gateway_pb2.SavePositionStateSnapshotsResponse(success=False, error=err)

            rows_written = await warm.save_position_state_snapshots(
                snapshot_id=int(request.snapshot_id),
                rows=warm_rows,
            )
            return gateway_pb2.SavePositionStateSnapshotsResponse(
                success=True,
                rows_written=int(rows_written or 0),
            )
        except Exception as e:
            logger.error(
                "SavePositionStateSnapshots failed for snapshot_id=%d: %s",
                request.snapshot_id,
                e,
            )
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details("internal server error")
            return gateway_pb2.SavePositionStateSnapshotsResponse(success=False, error="internal server error")

    async def GetPositionHistory(
        self,
        request: gateway_pb2.GetPositionHistoryRequest,
        context: grpc.aio.ServicerContext,
    ) -> gateway_pb2.GetPositionHistoryResponse:
        """Read full lifecycle (OPEN -> SNAPSHOT* -> CLOSE) for a single position (VIB-3944).

        Used by ``pnl_attributor`` to pair a CLOSE with its matching OPEN
        for FIFO realised-PnL attribution. Without this RPC the attributor
        crashes with ``AttributeError: 'GatewayStateManager' object has no
        attribute 'get_position_history'`` whenever a strategy closes a
        position through the gateway-sidecar architecture.

        Mirrors :meth:`SavePositionEvent` — delegates to the warm backend's
        ``get_position_history`` method. Hosted mode requires the warm
        backend to provide the same method (same gap as SavePositionEvent).
        Read-side fail-quiet: on backend error returns an empty list rather
        than raising, so a transient gRPC blip degrades attribution (loud
        warning in pnl_attributor) instead of halting the runner.
        """
        # Validate the wire deployment_id for the input contract; the warm
        # backend keys on deployment_id (blueprint 29 §4 — no translation),
        # so the validated value itself is not used downstream.
        try:
            validate_deployment_id(request.deployment_id)
        except ValidationError as e:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(str(e))
            return gateway_pb2.GetPositionHistoryResponse(events=[])

        deployment_id = request.deployment_id.strip() if request.deployment_id else ""
        if not deployment_id:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details("deployment_id is required")
            return gateway_pb2.GetPositionHistoryResponse(events=[])

        position_id = request.position_id.strip() if request.position_id else ""
        if not position_id:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details("position_id is required")
            return gateway_pb2.GetPositionHistoryResponse(events=[])

        # ``deployment_id`` is validated above so we don't break the wire
        # contract, but the warm backend's ``get_position_history`` keys on
        # ``deployment_id`` (the canonical runner-stable id), so only
        # ``deployment_id`` is passed through. Per blueprint 29 §4 there is
        # no gateway-side identity translation.
        try:
            await self._ensure_initialized()
            assert self._state_manager is not None
            warm = self._state_manager.warm_backend
            if warm is None or not hasattr(warm, "get_position_history"):
                logger.warning("GetPositionHistory: warm backend does not support get_position_history")
                return gateway_pb2.GetPositionHistoryResponse(events=[])

            rows = await warm.get_position_history(deployment_id, position_id)
            events = [_position_event_row_to_proto(r) for r in rows]
            return gateway_pb2.GetPositionHistoryResponse(events=events)
        except Exception as e:
            logger.warning(
                "GetPositionHistory failed for deployment=%s position=%s: %s",
                deployment_id,
                position_id,
                e,
            )
            return gateway_pb2.GetPositionHistoryResponse(events=[])

    async def UpdatePositionAttribution(
        self,
        request: gateway_pb2.UpdatePositionAttributionRequest,
        context: grpc.aio.ServicerContext,
    ) -> gateway_pb2.UpdatePositionAttributionResponse:
        """Partial-update of attribution columns on a position_event row (VIB-3944).

        Companion to ``GetPositionHistory``. Without this RPC,
        ``pnl_attributor.run_attribution_on_close`` falls back to
        ``save_position_event`` which is ``INSERT OR IGNORE`` and silently
        NO-OPs because the row already exists with the same ``id`` —
        attribution_json never reaches disk in gateway-sidecar mode.

        Mirrors the SQLite signature
        ``update_position_attribution(event_id, attribution_json, attribution_version)``.
        Non-blocking write: returns ``success=false`` on backend error rather
        than raising, since pnl_attributor wraps the call in a logged
        ``try/except`` already and a transient blip should degrade
        attribution to a warning, not halt the runner.
        """
        event_id = (request.event_id or "").strip()
        if not event_id:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details("event_id is required")
            return gateway_pb2.UpdatePositionAttributionResponse(success=False, error="event_id is required")

        try:
            uuid.UUID(event_id)
        except ValueError:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details("event_id must be a valid UUID")
            return gateway_pb2.UpdatePositionAttributionResponse(success=False, error="event_id must be a valid UUID")

        # CR audit (PR #2018): reject malformed attribution_json at the gateway
        # boundary so a corrupt payload can't reach the position_events column
        # and break every downstream consumer that calls
        # ``json.loads(row["attribution_json"])``.
        attribution_json = request.attribution_json or "{}"
        try:
            json.loads(attribution_json)
        except (json.JSONDecodeError, TypeError):
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details("attribution_json must be valid JSON")
            return gateway_pb2.UpdatePositionAttributionResponse(
                success=False, error="attribution_json must be valid JSON"
            )

        # CR audit (PR #2018): if the caller scoped the request with
        # deployment_id (optional, future-proofing for the hosted PostgresStore
        # write path where multi-tenant scoping matters), reject blank /
        # whitespace-only values so the field can't silently degrade. SQLite's
        # WHERE clause keys on the UUID event_id which is globally unique
        # by construction, so deployment_id remains a defense-in-depth scope
        # rather than a query filter today.
        if request.deployment_id and not request.deployment_id.strip():
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details("deployment_id must be non-empty when provided")
            return gateway_pb2.UpdatePositionAttributionResponse(
                success=False, error="deployment_id must be non-empty when provided"
            )

        try:
            await self._ensure_initialized()
            assert self._state_manager is not None
            warm = self._state_manager.warm_backend
            if warm is None or not hasattr(warm, "update_position_attribution"):
                error = "warm backend does not support update_position_attribution"
                logger.warning(
                    "UpdatePositionAttribution unsupported for event_id=%s: %s",
                    event_id,
                    error,
                )
                context.set_code(grpc.StatusCode.UNIMPLEMENTED)
                context.set_details(error)
                return gateway_pb2.UpdatePositionAttributionResponse(success=False, error=error)

            result = await warm.update_position_attribution(
                event_id, attribution_json, int(request.attribution_version or 0)
            )
            if not result:
                # Either the row was missing (event_id never INSERTed) or the
                # WHERE clause matched zero rows. Treat as a soft failure so
                # the caller can log a warning and the operator can correlate
                # via grep on event_id.
                logger.warning(
                    "UpdatePositionAttribution: no row matched event_id=%s (attribution dropped)",
                    event_id,
                )
                return gateway_pb2.UpdatePositionAttributionResponse(
                    success=False, error="no position_event row matched event_id"
                )
            return gateway_pb2.UpdatePositionAttributionResponse(success=True)
        except Exception as e:
            # CR audit (PR #2018): never leak raw backend exception text to
            # RPC callers — keep diagnostics in logs, return a generic error.
            logger.warning(
                "UpdatePositionAttribution failed for event_id=%s: %s",
                event_id,
                e,
            )
            return gateway_pb2.UpdatePositionAttributionResponse(success=False, error="internal server error")

    # =========================================================================
    # Read accounting events RPC (VIB-3503 Part 2c)
    # =========================================================================

    def _invalid_get_accounting_events_response(
        self,
        context: grpc.aio.ServicerContext,
        message: str,
    ) -> gateway_pb2.GetAccountingEventsResponse:
        context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
        context.set_details(message)
        return gateway_pb2.GetAccountingEventsResponse(events=[])

    def _validate_get_accounting_events_request(
        self,
        request: gateway_pb2.GetAccountingEventsRequest,
        context: grpc.aio.ServicerContext,
    ) -> str | gateway_pb2.GetAccountingEventsResponse:
        try:
            validate_deployment_id(request.deployment_id)
        except ValidationError as e:
            return self._invalid_get_accounting_events_response(context, str(e))

        deployment_id = request.deployment_id.strip() if request.deployment_id else ""
        if not deployment_id:
            return self._invalid_get_accounting_events_response(context, "deployment_id is required")

        # Reject negative limit / since_timestamp at the boundary so PG and
        # SQLite paths never disagree on what they accept. limit=0 is the
        # documented sentinel for "no limit"; negatives have no defined meaning.
        if request.limit < 0:
            return self._invalid_get_accounting_events_response(context, "limit must be >= 0")
        if request.since_timestamp < 0:
            return self._invalid_get_accounting_events_response(context, "since_timestamp must be >= 0")
        return deployment_id

    async def _get_accounting_events_pg(
        self,
        request: gateway_pb2.GetAccountingEventsRequest,
        deployment_id: str,
    ) -> gateway_pb2.GetAccountingEventsResponse:
        try:
            rows = await self._snapshot_fetch(
                """
                SELECT id, deployment_id, cycle_id, execution_mode,
                       EXTRACT(EPOCH FROM timestamp)::bigint AS ts_epoch,
                       chain, protocol, wallet_address, event_type,
                       position_key, ledger_entry_id, tx_hash, confidence,
                       payload_json::text AS payload_text, schema_version
                FROM accounting_events
                WHERE deployment_id = $1
                  AND ($2 = '' OR position_key = $2)
                  AND ($3 = '' OR event_type = $3)
                  AND ($4 = 0 OR timestamp >= to_timestamp($4))
                ORDER BY timestamp ASC
                LIMIT NULLIF($5, 0)
                """,
                deployment_id,
                request.position_key,
                request.event_type,
                request.since_timestamp,
                request.limit,
            )
            events = [_pg_row_to_accounting_event(r) for r in rows]
            # VIB-5185: read succeeded against a present backend → MEASURED. An
            # empty list here is a real zero, not an unmeasured gap.
            return gateway_pb2.GetAccountingEventsResponse(
                events=events,
                backend_status=gateway_pb2.ACCOUNTING_BACKEND_STATUS_AVAILABLE,
            )
        except Exception as e:
            logger.warning("GetAccountingEvents PG failed for deployment=%s: %s", deployment_id, e)
            # VIB-5185: the read errored (e.g. hosted before the metrics-database
            # migration adds the accounting_events table). Empty ≠ Zero — report
            # ERRORED so the client treats the empty list as UNMEASURED and the
            # teardown swap-back clamp fails closed.
            return gateway_pb2.GetAccountingEventsResponse(
                events=[],
                backend_status=gateway_pb2.ACCOUNTING_BACKEND_STATUS_ERRORED,
            )

    @staticmethod
    def _filter_sqlite_accounting_event_rows(
        rows: list[dict[str, Any]],
        request: gateway_pb2.GetAccountingEventsRequest,
    ) -> list[dict[str, Any]]:
        if request.event_type:
            rows = [r for r in rows if r.get("event_type") == request.event_type]
        if request.since_timestamp > 0:
            rows = [r for r in rows if _row_timestamp_epoch(r) >= request.since_timestamp]
        if request.limit > 0:
            rows = rows[: request.limit]
        return rows

    async def _get_accounting_events_sqlite(
        self,
        request: gateway_pb2.GetAccountingEventsRequest,
        deployment_id: str,
    ) -> gateway_pb2.GetAccountingEventsResponse:
        # SQLite mode (local dev) — delegate to the warm backend's sync primitive.
        try:
            await self._ensure_initialized()
            assert self._state_manager is not None
            warm = self._state_manager.warm_backend
            if warm is None or not hasattr(warm, "get_accounting_events_sync"):
                # VIB-5185: no warm backend able to serve accounting events →
                # the backend is structurally ABSENT. Empty ≠ Zero: report
                # ABSENT so the client treats the empty list as UNMEASURED.
                return gateway_pb2.GetAccountingEventsResponse(
                    events=[],
                    backend_status=gateway_pb2.ACCOUNTING_BACKEND_STATUS_ABSENT,
                )

            rows = warm.get_accounting_events_sync(
                deployment_id=deployment_id,
                position_key=request.position_key or None,
            )
            rows = self._filter_sqlite_accounting_event_rows(rows, request)
            events = [_sqlite_row_to_accounting_event(r) for r in rows]
            # VIB-5185: read succeeded against a present backend → MEASURED.
            return gateway_pb2.GetAccountingEventsResponse(
                events=events,
                backend_status=gateway_pb2.ACCOUNTING_BACKEND_STATUS_AVAILABLE,
            )
        except Exception as e:
            logger.warning("GetAccountingEvents SQLite failed: %s", e)
            # VIB-5185: the read raised → UNMEASURED, not measured-zero.
            return gateway_pb2.GetAccountingEventsResponse(
                events=[],
                backend_status=gateway_pb2.ACCOUNTING_BACKEND_STATUS_ERRORED,
            )

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
        validated = self._validate_get_accounting_events_request(request, context)
        if isinstance(validated, gateway_pb2.GetAccountingEventsResponse):
            return validated
        deployment_id = validated

        await self._ensure_snapshot_pool()
        if self._snapshot_pool is not None:
            return await self._get_accounting_events_pg(request, deployment_id)
        return await self._get_accounting_events_sqlite(request, deployment_id)

    async def GetLedgerEntriesMeasured(
        self,
        request: gateway_pb2.GetLedgerEntriesMeasuredRequest,
        context: grpc.aio.ServicerContext,
    ) -> gateway_pb2.GetLedgerEntriesMeasuredResponse:
        """Deployment-scoped transaction_ledger read WITH the measured backend signal (VIB-5416).

        The teardown swap-back clamp folds NO_ACCOUNTING ledger rows
        (STAKE→wstETH, WRAP→WETH, CDP MINT→stablecoin) into its tracked map so a
        held no-accounting token is not stranded as ``untracked_token``. Empty ≠
        Zero (mirrors :meth:`GetAccountingEvents` / VIB-5185): an UNMEASURED read
        (backend ABSENT or ERRORED) must be distinguishable from a measured-empty
        one so the client drops the NO_ACCOUNTING lane (the token strands — the
        safe under-sweep direction) rather than treating an empty read as
        measured-zero inventory. ``DashboardService.GetTransactionLedger`` carries
        no such signal, which is why this fund-safety read lives on StateService.

        ``limit=0`` means full history: the clamp must not paginate a STAKE
        acquisition out while keeping a later disposal (which would over-count) —
        newest-first truncation only ever drops OLD rows (under-count, safe).
        """
        try:
            validate_deployment_id(request.deployment_id)
        except ValidationError as e:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(str(e))
            return gateway_pb2.GetLedgerEntriesMeasuredResponse(
                backend_status=gateway_pb2.ACCOUNTING_BACKEND_STATUS_ERRORED,
            )
        deployment_id = request.deployment_id.strip() if request.deployment_id else ""
        if not deployment_id:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details("deployment_id is required")
            return gateway_pb2.GetLedgerEntriesMeasuredResponse(
                backend_status=gateway_pb2.ACCOUNTING_BACKEND_STATUS_ERRORED,
            )
        if request.limit < 0 or request.since_timestamp < 0:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details("limit and since_timestamp must be >= 0")
            return gateway_pb2.GetLedgerEntriesMeasuredResponse(
                backend_status=gateway_pb2.ACCOUNTING_BACKEND_STATUS_ERRORED,
            )

        await self._ensure_initialized()
        # Read the warm backend DIRECTLY (not via ``StateManager.get_ledger_entries``,
        # which collapses "no warm backend", "missing method", and caught backend
        # exceptions into an empty list). Empty ≠ Zero: the clamp must distinguish a
        # measured-empty read from an ABSENT/ERRORED one, exactly as
        # ``_get_accounting_events_sqlite`` does for accounting events.
        warm = getattr(self._state_manager, "warm_backend", None) if self._state_manager is not None else None
        if warm is None or not hasattr(warm, "get_ledger_entries"):
            # No warm backend able to serve ledger rows → structurally ABSENT.
            return gateway_pb2.GetLedgerEntriesMeasuredResponse(
                backend_status=gateway_pb2.ACCOUNTING_BACKEND_STATUS_ABSENT,
            )

        # A huge positive epoch overflows ``datetime.fromtimestamp`` — reject at the
        # boundary as INVALID_ARGUMENT rather than letting OverflowError/OSError
        # escape the validated path.
        since = None
        if request.since_timestamp > 0:
            try:
                since = datetime.fromtimestamp(request.since_timestamp, tz=UTC)
            except (OverflowError, OSError, ValueError):
                context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
                context.set_details("since_timestamp out of range")
                return gateway_pb2.GetLedgerEntriesMeasuredResponse(
                    backend_status=gateway_pb2.ACCOUNTING_BACKEND_STATUS_ERRORED,
                )
        intent_type = request.intent_type_filter or None
        # limit=0 → effectively-full history (SQLite ``LIMIT 0`` returns ZERO rows,
        # so 0 cannot be passed through as "no limit"). 100k bounds the
        # pathological case while covering every real deployment's lifetime ledger.
        limit = request.limit if request.limit > 0 else 100_000
        try:
            entries = await warm.get_ledger_entries(deployment_id, since=since, intent_type=intent_type, limit=limit)
        except Exception as e:  # noqa: BLE001 — Empty ≠ Zero: an errored read is UNMEASURED.
            logger.warning("GetLedgerEntriesMeasured failed for deployment=%s: %s", deployment_id, e)
            return gateway_pb2.GetLedgerEntriesMeasuredResponse(
                backend_status=gateway_pb2.ACCOUNTING_BACKEND_STATUS_ERRORED,
            )

        # VIB-5416: the backend read is ``ORDER BY timestamp DESC`` (and the wire
        # ``LedgerEntryInfo.timestamp`` is truncated to whole seconds), so same-block
        # rows could otherwise reach the clamp in an arbitrary order and replay a
        # NO_ACCOUNTING disposal before its acquisition (over-count → over-sweep).
        # Return entries in DETERMINISTIC chronological order at full datetime
        # precision (``id`` as the final tiebreak for the impossible exact-micros
        # tie); the clamp's stable timestamp-merge then preserves this execution
        # order for same-second synthetic events.
        entries = sorted(entries, key=lambda e: (e.timestamp, e.id or ""))
        proto_entries = [_ledger_entry_to_proto(entry) for entry in entries]
        return gateway_pb2.GetLedgerEntriesMeasuredResponse(
            entries=proto_entries,
            backend_status=gateway_pb2.ACCOUNTING_BACKEND_STATUS_AVAILABLE,
        )

    # =========================================================================
    # Accounting Outbox RPCs — crash-safe durability for AccountingProcessor
    # DDL: metrics-database PR #24 (VIB-3503) + per-position columns added in
    # VIB-3658.  PG primary key = ledger_entry_id.
    # =========================================================================

    def _pg_outbox_row_to_proto(self, row: Any) -> gateway_pb2.OutboxEntry:
        """Convert a PG asyncpg.Record from accounting_outbox to the proto shape.

        Column-name translation (PG vs SQLite vs wire):
        - VIB-4721/4722: ``accounting_outbox`` has a single identity column,
          ``deployment_id`` (one canonical id, resolved once at boot, no
          gateway-side translation).
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

        deployment_id_raw = (request.deployment_id or "").strip()
        try:
            deployment_id = validate_deployment_id(deployment_id_raw)
        except ValidationError as e:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(str(e))
            return gateway_pb2.SaveOutboxEntryResponse(success=False, error=str(e))

        await self._ensure_snapshot_pool()

        if self._snapshot_pool is not None:
            try:
                await self._snapshot_execute(
                    """
                    INSERT INTO accounting_outbox
                        (ledger_entry_id, deployment_id, intent_type,
                         cycle_id, wallet_address, position_key, market_id,
                         status, retry_count)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, 'pending', 0)
                    ON CONFLICT (ledger_entry_id) DO NOTHING
                    """,
                    ledger_entry_id,
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
                    SELECT ledger_entry_id, deployment_id, intent_type,
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
                    SELECT ledger_entry_id, deployment_id, intent_type,
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

    async def _get_ledger_entry_pg_response(
        self,
        ledger_entry_id: str,
    ) -> gateway_pb2.GetLedgerEntryResponse:
        row = await self._snapshot_fetchrow(
            """
            SELECT id, cycle_id, deployment_id, execution_mode,
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
        return gateway_pb2.GetLedgerEntryResponse(found=True, entry=_pg_ledger_entry_row_to_proto(row))

    async def _get_ledger_entry_sqlite_response(
        self,
        ledger_entry_id: str,
    ) -> gateway_pb2.GetLedgerEntryResponse:
        await self._ensure_initialized()
        assert self._state_manager is not None
        warm = self._state_manager.warm_backend
        if warm is None or not hasattr(warm, "get_ledger_entry_by_id"):
            return gateway_pb2.GetLedgerEntryResponse(found=False)
        row = await warm.get_ledger_entry_by_id(ledger_entry_id)
        if row is None:
            return gateway_pb2.GetLedgerEntryResponse(found=False)
        return gateway_pb2.GetLedgerEntryResponse(found=True, entry=_sqlite_ledger_entry_row_to_proto(row))

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
                return await self._get_ledger_entry_pg_response(ledger_entry_id)
            except Exception as e:
                logger.error("GetLedgerEntry PG failed for id=%s: %s", ledger_entry_id, e)
                context.set_code(grpc.StatusCode.INTERNAL)
                context.set_details("internal server error")
                return gateway_pb2.GetLedgerEntryResponse(found=False)

        # SQLite path
        try:
            return await self._get_ledger_entry_sqlite_response(ledger_entry_id)
        except Exception as e:
            logger.error("GetLedgerEntry SQLite failed: %s", e)
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details("internal server error")
            return gateway_pb2.GetLedgerEntryResponse(found=False)

    async def SumLedgerGasUsd(
        self,
        request: gateway_pb2.SumLedgerGasUsdRequest,
        context: grpc.aio.ServicerContext,
    ) -> gateway_pb2.SumLedgerGasUsdResponse:
        """Return Σ transaction_ledger.gas_usd for portfolio metrics.

        One identity (blueprint 29 §4): ``transaction_ledger`` keys on the
        single ``deployment_id`` column on both backends; the gateway filters
        the caller-supplied ``deployment_id`` directly with no translation.
        """
        deployment_id = (request.deployment_id or "").strip()
        if not deployment_id:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details("deployment_id is required")
            return gateway_pb2.SumLedgerGasUsdResponse(success=False, error="deployment_id is required")

        try:
            deployment_id = validate_deployment_id(deployment_id, field="deployment_id")
        except ValidationError as e:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(str(e))
            return gateway_pb2.SumLedgerGasUsdResponse(success=False, error=str(e))

        await self._ensure_snapshot_pool()

        if self._snapshot_pool is not None:
            try:
                row = await self._snapshot_fetchrow(
                    """
                    SELECT COALESCE(
                        SUM(
                            CASE
                                WHEN NULLIF(BTRIM(gas_usd), '') ~ '^[+-]?(?:[0-9]+(?:[.][0-9]*)?|[.][0-9]+)(?:[eE][+-]?[0-9]+)?$'
                                THEN NULLIF(BTRIM(gas_usd), '')::numeric
                                ELSE 0
                            END
                        ),
                        0
                    ) AS total
                    FROM transaction_ledger
                    WHERE deployment_id = $1
                    """,
                    deployment_id,
                )
                total = Decimal(str((row or {"total": 0})["total"] or 0))
                return gateway_pb2.SumLedgerGasUsdResponse(success=True, gas_usd_total=str(total))
            except Exception as e:
                logger.error(
                    "SumLedgerGasUsd PG failed for deployment_id=%s: %s",
                    deployment_id,
                    e,
                )
                context.set_code(grpc.StatusCode.INTERNAL)
                context.set_details("internal server error")
                return gateway_pb2.SumLedgerGasUsdResponse(success=False, error="internal server error")

        try:
            await self._ensure_initialized()
            assert self._state_manager is not None
            warm = self._state_manager.warm_backend
            if warm is None or not hasattr(warm, "sum_ledger_gas_usd"):
                error = "warm backend does not support sum_ledger_gas_usd"
                logger.error("SumLedgerGasUsd unsupported for deployment_id=%s: %s", deployment_id, error)
                context.set_code(grpc.StatusCode.UNIMPLEMENTED)
                context.set_details(error)
                return gateway_pb2.SumLedgerGasUsdResponse(success=False, error=error)

            total = await warm.sum_ledger_gas_usd(deployment_id)
            return gateway_pb2.SumLedgerGasUsdResponse(success=True, gas_usd_total=str(total))
        except Exception as e:
            logger.error("SumLedgerGasUsd SQLite failed for deployment_id=%s: %s", deployment_id, e)
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details("internal server error")
            return gateway_pb2.SumLedgerGasUsdResponse(success=False, error="internal server error")

    # =========================================================================
    # Cutover storage RPCs (VIB-4208 / T22 SQLite; VIB-4205 / T19 Postgres)
    # =========================================================================
    #
    # Both backends implemented:
    #   - SQLite branch (T22): routes to the WARM backend's typed accessors.
    #   - Postgres branch (T19): uses the existing ``_snapshot_pool``
    #     asyncpg pool with the SAME wire shapes as the SQLite branch.
    #     ``SaveLedgerAndRegistry`` acquires a single connection wrapped
    #     in ``async with conn.transaction():`` so the three writes
    #     (ledger + registry + handle backfill) commit as one Postgres
    #     transaction — the anti-bypass invariant from VIB-4205 acceptance.
    #
    # AGENTS.md "Database schema ownership": local SQLite is SDK-owned;
    # hosted Postgres schema is owned by the metrics-database repo (VIB-4191).
    # T19 deletes the ``_POSTGRES_DEFERRED_TABLES`` entries for
    # ``position_registry`` and ``migration_state`` so the boot validator
    # fails loud when the deployed schema is missing either table.

    async def UpsertMigrationState(
        self,
        request: gateway_pb2.UpsertMigrationStateRequest,
        context: grpc.aio.ServicerContext,
    ) -> gateway_pb2.UpsertMigrationStateResponse:
        """Idempotent insert of a baseline migration_state row (cutover spec §2.1).

        Routes to the WARM backend's ``upsert_migration_state`` on SQLite,
        and uses the snapshot pool on Postgres (T19 / VIB-4205). Both
        backends use ``INSERT … ON CONFLICT DO NOTHING`` keyed by
        ``(deployment_id, primitive, cutover_key)`` — re-Upsert with the
        same triple is a true no-op (does NOT mutate ``complete``,
        counters, or ``completed_at``).
        """
        deployment_id, primitive, cutover_key = self._strip_required_triple(request)
        if not deployment_id:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details("deployment_id is required")
            return gateway_pb2.UpsertMigrationStateResponse(success=False, error="deployment_id is required")
        if not primitive:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details("primitive is required")
            return gateway_pb2.UpsertMigrationStateResponse(success=False, error="primitive is required")
        if not cutover_key:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details("cutover_key is required")
            return gateway_pb2.UpsertMigrationStateResponse(success=False, error="cutover_key is required")

        await self._ensure_snapshot_pool()
        if self._snapshot_pool is not None:
            # PostgreSQL mode (T19 / VIB-4205). Idempotent baseline insert;
            # re-Upsert is a true no-op (ON CONFLICT DO NOTHING absorbs
            # the second call without touching counters or completed_at).
            #
            # VIB-4191-dep: assumed JSONB for ``migration_state.notes``
            # (matches existing ``accounting_events.payload_json`` pattern);
            # if Infra deploys TEXT, change ``'{}'::jsonb`` to ``'{}'`` here
            # and drop the ``::text`` cast on read in GetMigrationState.
            #
            # VIB-4191-dep: assumed BOOLEAN for
            # ``migration_state.position_registry_backfill_complete``
            # (SQLite uses INTEGER 0/1; Postgres-native is BOOLEAN); if
            # Infra deploys INTEGER, change ``FALSE`` to ``0`` here and
            # in MarkBackfillComplete's ``TRUE`` to ``1``.
            #
            # VIB-4191-dep: assumed TIMESTAMPTZ for ``created_at`` /
            # ``updated_at`` (Postgres-native); ``NOW()`` returns the
            # transaction timestamp. If Infra deploys TEXT, this still
            # works because asyncpg binds ``NOW()`` server-side; the only
            # affected path is GetMigrationState's ``.isoformat()`` read
            # conversion.
            try:
                await self._snapshot_execute(
                    """
                    INSERT INTO migration_state (
                        deployment_id, primitive, cutover_key,
                        position_registry_backfill_complete,
                        backfill_source_table, backfill_reader_version,
                        rows_synthesized, rows_skipped_already_present,
                        notes, created_at, updated_at
                    ) VALUES (
                        $1, $2, $3, FALSE, 'position_events', 1, 0, 0,
                        '{}'::jsonb, NOW(), NOW()
                    )
                    ON CONFLICT (deployment_id, primitive, cutover_key)
                    DO NOTHING
                    """,
                    deployment_id,
                    primitive,
                    cutover_key,
                )
                return gateway_pb2.UpsertMigrationStateResponse(success=True)
            except Exception as e:
                logger.error(
                    "UpsertMigrationState PG failed (deployment=%s primitive=%s cutover_key=%s): %s",
                    deployment_id,
                    primitive,
                    cutover_key,
                    e,
                )
                context.set_code(grpc.StatusCode.INTERNAL)
                context.set_details("internal server error")
                return gateway_pb2.UpsertMigrationStateResponse(success=False, error="internal server error")

        try:
            await self._ensure_initialized()
            assert self._state_manager is not None
            warm = self._state_manager.warm_backend
            if warm is None or not hasattr(warm, "upsert_migration_state"):
                error = "warm backend does not support upsert_migration_state"
                logger.error(
                    "UpsertMigrationState unsupported (deployment=%s primitive=%s cutover_key=%s): %s",
                    deployment_id,
                    primitive,
                    cutover_key,
                    error,
                )
                context.set_code(grpc.StatusCode.UNIMPLEMENTED)
                context.set_details(error)
                return gateway_pb2.UpsertMigrationStateResponse(success=False, error=error)
            await warm.upsert_migration_state(
                deployment_id=deployment_id,
                primitive=primitive,
                cutover_key=cutover_key,
            )
            return gateway_pb2.UpsertMigrationStateResponse(success=True)
        except Exception as e:
            logger.error(
                "UpsertMigrationState SQLite failed (deployment=%s primitive=%s cutover_key=%s): %s",
                deployment_id,
                primitive,
                cutover_key,
                e,
            )
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details("internal server error")
            return gateway_pb2.UpsertMigrationStateResponse(success=False, error="internal server error")

    @staticmethod
    def _optional_int_field(request, field_name: str) -> int | None:
        """Unpack a proto3 ``optional int64`` field.

        ``HasField`` distinguishes "not supplied" from "supplied with value 0"
        — critical for migration counters where 0 is a measured value, not a
        sentinel. Returns the field value when set, else ``None``. Used by
        :meth:`UpdateMigrationState` for the in-flight progress counters.
        """
        return getattr(request, field_name) if request.HasField(field_name) else None

    @staticmethod
    def _parse_iso_datetime(value: str) -> datetime:
        """Parse an ISO-8601 string to a timezone-aware ``datetime``.

        asyncpg's TIMESTAMPTZ codec rejects raw strings client-side, even when
        the SQL has a ``::timestamptz`` cast (VIB-4313). Naive datetimes are
        coerced to UTC so the wire value is unambiguous; the metrics-database
        ``backfill_started_at`` / ``backfill_completed_at`` columns are
        TIMESTAMPTZ.
        """
        # ``fromisoformat`` accepts ``...+00:00`` and (Python 3.11+) the
        # trailing ``Z``; normalise either to a tz-aware UTC datetime.
        normalized = value.replace("Z", "+00:00") if value.endswith("Z") else value
        parsed = datetime.fromisoformat(normalized)
        return parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=UTC)

    @classmethod
    def _parse_optional_iso_datetime(cls, value: str) -> datetime | None:
        """ISO-8601 string → ``datetime`` with ``""`` proto sentinel = None.

        Mirrors the ``request.field or None`` short-circuit the migration_state
        RPCs use for "don't touch" semantics.
        """
        stripped = (value or "").strip()
        if not stripped:
            return None
        return cls._parse_iso_datetime(stripped)

    @staticmethod
    def _strip_required_triple(request) -> tuple[str, str, str]:
        """Strip the ``(deployment_id, primitive, cutover_key)`` migration_state key.

        Every migration_state RPC (Upsert / Get / Update / MarkBackfillComplete)
        keys its row by this triple and starts with the same three ``or ""`` /
        ``strip()`` short-circuits. Callers still own the empty-string
        validation because the per-method error-message granularity differs
        (Upsert/Get report per-field; Update/MarkBackfillComplete report all
        three together).
        """
        return (
            (request.deployment_id or "").strip(),
            (request.primitive or "").strip(),
            (request.cutover_key or "").strip(),
        )

    @staticmethod
    def _strip_optional(value: str) -> str | None:
        """Normalize an optional proto3 string filter to ``None`` when empty.

        Proto3 strings default to ``""``, but the SQLite accessors take
        ``None`` to mean "no filter on this column". Used by
        :meth:`GetPositionRegistryOpenRows` for ``chain`` / ``primitive`` /
        ``accounting_category`` so each call site stays a single line.
        """
        return (value or "").strip() or None

    @staticmethod
    def _marshal_migration_state_row(row) -> gateway_pb2.MigrationStateData:
        """Translate a ``MigrationStateRow`` dataclass to its proto twin.

        Centralizes the ``or ""`` / ``or 0`` Optional-to-empty fallbacks and
        the ``notes`` JSON encoding so :meth:`GetMigrationState` stays a thin
        validation → dispatch → marshal handler. See
        ``almanak/framework/migration/backfill.py`` for the ``MigrationStateRow``
        dataclass definition.
        """
        notes_payload = row.notes if isinstance(row.notes, dict) else {}
        try:
            notes_bytes = json.dumps(notes_payload, sort_keys=True).encode("utf-8")
        except (TypeError, ValueError):
            notes_bytes = b"{}"
        return gateway_pb2.MigrationStateData(
            deployment_id=row.deployment_id or "",
            primitive=row.primitive or "",
            cutover_key=row.cutover_key or "",
            position_registry_backfill_complete=bool(row.position_registry_backfill_complete),
            backfill_started_at=row.backfill_started_at or "",
            backfill_completed_at=row.backfill_completed_at or "",
            backfill_source_table=row.backfill_source_table or "",
            backfill_reader_version=int(row.backfill_reader_version or 0),
            rows_synthesized=int(row.rows_synthesized or 0),
            rows_skipped_already_present=int(row.rows_skipped_already_present or 0),
            notes=notes_bytes,
            created_at=row.created_at or "",
            updated_at=row.updated_at or "",
        )

    @staticmethod
    def _pg_timestamp_to_iso(value: Any) -> str:
        """Convert an asyncpg timestamp value to ISO-8601 (empty string when NULL).

        VIB-4191-dep: assumed TIMESTAMPTZ for ``migration_state`` time columns;
        asyncpg returns ``datetime`` for TIMESTAMPTZ and ``str`` for TEXT —
        this helper handles both shapes so the wire stays identical regardless
        of Infra's eventual schema choice.
        """
        if value is None:
            return ""
        if isinstance(value, datetime):
            return value.isoformat()
        return str(value)

    @classmethod
    def _marshal_migration_state_pg_row(cls, row: Any) -> gateway_pb2.MigrationStateData:
        """Translate an asyncpg ``Record`` to ``MigrationStateData`` proto.

        Parallel sibling of :meth:`_marshal_migration_state_row` (which takes
        the SQLite dataclass shape); centralizes Optional fallbacks and the
        ``notes::text`` → bytes re-encoding so
        :meth:`GetMigrationState` Postgres branch stays a thin
        fetch → marshal handler. Same wire shape as the SQLite branch.
        """
        notes_text = _item_text(row, "notes_text", "{}")
        return gateway_pb2.MigrationStateData(
            deployment_id=_item_text(row, "deployment_id"),
            primitive=_item_text(row, "primitive"),
            cutover_key=_item_text(row, "cutover_key"),
            position_registry_backfill_complete=bool(row["position_registry_backfill_complete"]),
            backfill_started_at=cls._pg_timestamp_to_iso(row["backfill_started_at"]),
            backfill_completed_at=cls._pg_timestamp_to_iso(row["backfill_completed_at"]),
            backfill_source_table=_item_text(row, "backfill_source_table"),
            backfill_reader_version=_item_int(row, "backfill_reader_version"),
            rows_synthesized=_item_int(row, "rows_synthesized"),
            rows_skipped_already_present=_item_int(row, "rows_skipped_already_present"),
            notes=notes_text.encode("utf-8"),
            created_at=cls._pg_timestamp_to_iso(row["created_at"]),
            updated_at=cls._pg_timestamp_to_iso(row["updated_at"]),
        )

    @staticmethod
    def _get_migration_state_error(
        context: grpc.aio.ServicerContext,
        code: grpc.StatusCode,
        error: str,
    ) -> gateway_pb2.GetMigrationStateResponse:
        context.set_code(code)
        context.set_details(error)
        return gateway_pb2.GetMigrationStateResponse(found=False, error=error)

    def _validate_get_migration_state_request(
        self,
        request: gateway_pb2.GetMigrationStateRequest,
        context: grpc.aio.ServicerContext,
    ) -> tuple[str, str, str] | gateway_pb2.GetMigrationStateResponse:
        deployment_id, primitive, cutover_key = self._strip_required_triple(request)
        for field, value in (
            ("deployment_id", deployment_id),
            ("primitive", primitive),
            ("cutover_key", cutover_key),
        ):
            if value:
                continue
            return self._get_migration_state_error(
                context,
                grpc.StatusCode.INVALID_ARGUMENT,
                f"{field} is required",
            )
        return deployment_id, primitive, cutover_key

    async def _get_migration_state_pg(
        self,
        *,
        deployment_id: str,
        primitive: str,
        cutover_key: str,
        context: grpc.aio.ServicerContext,
    ) -> gateway_pb2.GetMigrationStateResponse:
        try:
            row = await self._snapshot_fetchrow(
                """
                SELECT deployment_id, primitive, cutover_key,
                       position_registry_backfill_complete,
                       backfill_started_at, backfill_completed_at,
                       backfill_source_table, backfill_reader_version,
                       rows_synthesized, rows_skipped_already_present,
                       notes::text AS notes_text,
                       created_at, updated_at
                FROM migration_state
                WHERE deployment_id = $1
                  AND primitive = $2
                  AND cutover_key = $3
                """,
                deployment_id,
                primitive,
                cutover_key,
            )
            if row is None:
                return gateway_pb2.GetMigrationStateResponse(found=False)
            return gateway_pb2.GetMigrationStateResponse(
                found=True,
                data=self._marshal_migration_state_pg_row(row),
            )
        except Exception as e:
            logger.error(
                "GetMigrationState PG failed (deployment=%s primitive=%s cutover_key=%s): %s",
                deployment_id,
                primitive,
                cutover_key,
                e,
            )
            return self._get_migration_state_error(
                context,
                grpc.StatusCode.INTERNAL,
                "internal server error",
            )

    async def _get_migration_state_sqlite(
        self,
        *,
        deployment_id: str,
        primitive: str,
        cutover_key: str,
        context: grpc.aio.ServicerContext,
    ) -> gateway_pb2.GetMigrationStateResponse:
        try:
            await self._ensure_initialized()
            assert self._state_manager is not None
            warm = self._state_manager.warm_backend
            if warm is None or not hasattr(warm, "get_migration_state"):
                error = "warm backend does not support get_migration_state"
                logger.error(
                    "GetMigrationState unsupported (deployment=%s primitive=%s cutover_key=%s): %s",
                    deployment_id,
                    primitive,
                    cutover_key,
                    error,
                )
                return self._get_migration_state_error(context, grpc.StatusCode.UNIMPLEMENTED, error)
            row = await warm.get_migration_state(
                deployment_id=deployment_id,
                primitive=primitive,
                cutover_key=cutover_key,
            )
            if row is None:
                return gateway_pb2.GetMigrationStateResponse(found=False)
            return gateway_pb2.GetMigrationStateResponse(
                found=True,
                data=self._marshal_migration_state_row(row),
            )
        except Exception as e:
            logger.error(
                "GetMigrationState SQLite failed (deployment=%s primitive=%s cutover_key=%s): %s",
                deployment_id,
                primitive,
                cutover_key,
                e,
            )
            return self._get_migration_state_error(
                context,
                grpc.StatusCode.INTERNAL,
                "internal server error",
            )

    async def GetMigrationState(
        self,
        request: gateway_pb2.GetMigrationStateRequest,
        context: grpc.aio.ServicerContext,
    ) -> gateway_pb2.GetMigrationStateResponse:
        """Read the migration_state row for ``(deployment_id, primitive, cutover_key)``.

        Returns ``found=true`` with all 13 columns populated when the row
        exists; ``found=false`` (empty data) when absent. Both backends use
        the same wire shape; Postgres reads the JSONB ``notes`` column as
        text and re-encodes as UTF-8 bytes (mirrors the SaveAccountingEvent
        ``payload_json::text`` convention).

        ``SnapshotData.found`` pattern: a default-populated proto would be
        indistinguishable from "row absent" so the explicit boolean flag
        is required.
        """
        validated = self._validate_get_migration_state_request(request, context)
        if isinstance(validated, gateway_pb2.GetMigrationStateResponse):
            return validated
        deployment_id, primitive, cutover_key = validated

        await self._ensure_snapshot_pool()
        if self._snapshot_pool is not None:
            # PostgreSQL mode (T19 / VIB-4205). Single-row fetch keyed by the
            # composite PK. ``notes::text`` cast normalizes the JSONB column
            # to a string so we can re-encode as bytes for the proto wire
            # (matches the SaveAccountingEvent ``payload_json::text`` pattern).
            #
            # VIB-4191-dep: ``notes::text`` cast assumes JSONB column type;
            # if Infra deploys TEXT, drop the cast (TEXT is already a string).
            return await self._get_migration_state_pg(
                deployment_id=deployment_id,
                primitive=primitive,
                cutover_key=cutover_key,
                context=context,
            )
        return await self._get_migration_state_sqlite(
            deployment_id=deployment_id,
            primitive=primitive,
            cutover_key=cutover_key,
            context=context,
        )

    @staticmethod
    def _update_migration_state_error(
        context: grpc.aio.ServicerContext,
        code: grpc.StatusCode,
        err: str,
    ) -> gateway_pb2.UpdateMigrationStateResponse:
        context.set_code(code)
        context.set_details(err)
        return gateway_pb2.UpdateMigrationStateResponse(success=False, error=err)

    def _validate_update_migration_state_request(
        self,
        request: gateway_pb2.UpdateMigrationStateRequest,
        context: grpc.aio.ServicerContext,
    ) -> tuple[str, str, str] | gateway_pb2.UpdateMigrationStateResponse:
        deployment_id, primitive, cutover_key = self._strip_required_triple(request)
        if not deployment_id or not primitive or not cutover_key:
            return self._update_migration_state_error(
                context,
                grpc.StatusCode.INVALID_ARGUMENT,
                "deployment_id, primitive, and cutover_key are required",
            )
        return deployment_id, primitive, cutover_key

    def _update_migration_state_pg_values(
        self,
        request: gateway_pb2.UpdateMigrationStateRequest,
        context: grpc.aio.ServicerContext,
    ) -> tuple[datetime | None, int | None, int | None] | gateway_pb2.UpdateMigrationStateResponse:
        try:
            backfill_started_at = self._parse_optional_iso_datetime(request.backfill_started_at)
        except ValueError as e:
            return self._update_migration_state_error(
                context,
                grpc.StatusCode.INVALID_ARGUMENT,
                f"backfill_started_at must be ISO-8601: {e}",
            )
        return (
            backfill_started_at,
            self._optional_int_field(request, "rows_synthesized"),
            self._optional_int_field(request, "rows_skipped_already_present"),
        )

    @staticmethod
    def _update_migration_state_pg_statement(
        *,
        deployment_id: str,
        primitive: str,
        cutover_key: str,
        backfill_started_at: datetime | None,
        rows_synthesized: int | None,
        rows_skipped_already_present: int | None,
    ) -> tuple[str, list[Any]] | None:
        sets: list[str] = []
        params: list[Any] = []
        for column, value in (
            ("backfill_started_at", backfill_started_at),
            ("rows_synthesized", rows_synthesized),
            ("rows_skipped_already_present", rows_skipped_already_present),
        ):
            if value is None:
                continue
            params.append(value)
            sets.append(f"{column} = ${len(params)}")
        if not sets:
            return None

        sets.append("updated_at = NOW()")
        where_start = len(params) + 1
        params.extend([deployment_id, primitive, cutover_key])
        sql = (
            f"UPDATE migration_state SET {', '.join(sets)} "
            f"WHERE deployment_id = ${where_start} "
            f"AND primitive = ${where_start + 1} "
            f"AND cutover_key = ${where_start + 2}"
        )
        return sql, params

    async def _update_migration_state_pg(
        self,
        *,
        request: gateway_pb2.UpdateMigrationStateRequest,
        context: grpc.aio.ServicerContext,
        deployment_id: str,
        primitive: str,
        cutover_key: str,
    ) -> gateway_pb2.UpdateMigrationStateResponse:
        # PostgreSQL mode (T19 / VIB-4205). Dynamic SET clause matches the
        # SQLite backend's partial-update semantics (sqlite.py:4066).
        values = self._update_migration_state_pg_values(request, context)
        if isinstance(values, gateway_pb2.UpdateMigrationStateResponse):
            return values
        statement = self._update_migration_state_pg_statement(
            deployment_id=deployment_id,
            primitive=primitive,
            cutover_key=cutover_key,
            backfill_started_at=values[0],
            rows_synthesized=values[1],
            rows_skipped_already_present=values[2],
        )
        if statement is None:
            # Empty request -> no-op (mirrors sqlite.py:4077).
            return gateway_pb2.UpdateMigrationStateResponse(success=True)

        sql, params = statement
        try:
            await self._snapshot_execute(sql, *params)
            return gateway_pb2.UpdateMigrationStateResponse(success=True)
        except Exception as e:
            logger.error(
                "UpdateMigrationState PG failed (deployment=%s primitive=%s cutover_key=%s): %s",
                deployment_id,
                primitive,
                cutover_key,
                e,
            )
            return self._update_migration_state_error(
                context,
                grpc.StatusCode.INTERNAL,
                "internal server error",
            )

    async def _update_migration_state_sqlite(
        self,
        *,
        request: gateway_pb2.UpdateMigrationStateRequest,
        context: grpc.aio.ServicerContext,
        deployment_id: str,
        primitive: str,
        cutover_key: str,
    ) -> gateway_pb2.UpdateMigrationStateResponse:
        try:
            await self._ensure_initialized()
            assert self._state_manager is not None
            warm = self._state_manager.warm_backend
            if warm is None or not hasattr(warm, "update_migration_state"):
                return self._update_migration_state_error(
                    context,
                    grpc.StatusCode.UNIMPLEMENTED,
                    "warm backend does not support update_migration_state",
                )
            await warm.update_migration_state(
                deployment_id=deployment_id,
                primitive=primitive,
                cutover_key=cutover_key,
                backfill_started_at=request.backfill_started_at or None,
                rows_synthesized=self._optional_int_field(request, "rows_synthesized"),
                rows_skipped_already_present=self._optional_int_field(request, "rows_skipped_already_present"),
            )
            return gateway_pb2.UpdateMigrationStateResponse(success=True)
        except Exception as e:
            logger.error(
                "UpdateMigrationState SQLite failed (deployment=%s primitive=%s cutover_key=%s): %s",
                deployment_id,
                primitive,
                cutover_key,
                e,
            )
            return self._update_migration_state_error(
                context,
                grpc.StatusCode.INTERNAL,
                "internal server error",
            )

    async def UpdateMigrationState(
        self,
        request: gateway_pb2.UpdateMigrationStateRequest,
        context: grpc.aio.ServicerContext,
    ) -> gateway_pb2.UpdateMigrationStateResponse:
        """Partial update of migration_state in-flight progress columns.

        ``backfill_started_at`` is a string with "" sentinel for "don't
        touch". The counters are ``optional int64`` so 0 stays
        distinguishable from "not supplied".

        Both backends programmatically build the SET clause from supplied
        fields; an empty request (no fields set) is a no-op.
        """
        validated = self._validate_update_migration_state_request(request, context)
        if isinstance(validated, gateway_pb2.UpdateMigrationStateResponse):
            return validated
        deployment_id, primitive, cutover_key = validated

        await self._ensure_snapshot_pool()
        if self._snapshot_pool is not None:
            return await self._update_migration_state_pg(
                request=request,
                context=context,
                deployment_id=deployment_id,
                primitive=primitive,
                cutover_key=cutover_key,
            )
        return await self._update_migration_state_sqlite(
            request=request,
            context=context,
            deployment_id=deployment_id,
            primitive=primitive,
            cutover_key=cutover_key,
        )

    @staticmethod
    def _mark_backfill_complete_error(
        context: grpc.aio.ServicerContext,
        code: grpc.StatusCode,
        err: str,
    ) -> gateway_pb2.MarkBackfillCompleteResponse:
        context.set_code(code)
        context.set_details(err)
        return gateway_pb2.MarkBackfillCompleteResponse(success=False, error=err)

    def _validate_mark_backfill_complete_request(
        self,
        request: gateway_pb2.MarkBackfillCompleteRequest,
        context: grpc.aio.ServicerContext,
    ) -> tuple[str, str, str, str, datetime] | gateway_pb2.MarkBackfillCompleteResponse:
        deployment_id, primitive, cutover_key = self._strip_required_triple(request)
        if not deployment_id or not primitive or not cutover_key:
            return self._mark_backfill_complete_error(
                context,
                grpc.StatusCode.INVALID_ARGUMENT,
                "deployment_id, primitive, and cutover_key are required",
            )

        backfill_completed_at_str = (request.backfill_completed_at or "").strip()
        if not backfill_completed_at_str:
            return self._mark_backfill_complete_error(
                context,
                grpc.StatusCode.INVALID_ARGUMENT,
                "backfill_completed_at is required",
            )
        try:
            backfill_completed_at = self._parse_iso_datetime(backfill_completed_at_str)
        except ValueError as e:
            return self._mark_backfill_complete_error(
                context,
                grpc.StatusCode.INVALID_ARGUMENT,
                f"backfill_completed_at must be ISO-8601: {e}",
            )
        return deployment_id, primitive, cutover_key, backfill_completed_at_str, backfill_completed_at

    @staticmethod
    def _pg_update_rowcount(status: Any) -> int | None:
        if not isinstance(status, str) or not status.startswith("UPDATE "):
            return None
        try:
            return int(status.split(" ", 1)[1])
        except (ValueError, IndexError):
            return None

    @staticmethod
    def _warn_mark_backfill_complete_zero_rows(
        deployment_id: str,
        primitive: str,
        cutover_key: str,
    ) -> None:
        logger.warning(
            "MarkBackfillComplete PG matched 0 rows "
            "(deployment=%s primitive=%s cutover_key=%s) — "
            "UpsertMigrationState was supposed to seed the baseline; "
            "this is a contract violation, not infra failure",
            deployment_id,
            primitive,
            cutover_key,
        )

    async def _mark_backfill_complete_pg(
        self,
        *,
        request: gateway_pb2.MarkBackfillCompleteRequest,
        context: grpc.aio.ServicerContext,
        deployment_id: str,
        primitive: str,
        cutover_key: str,
        backfill_completed_at: datetime,
    ) -> gateway_pb2.MarkBackfillCompleteResponse:
        try:
            status = await self._snapshot_execute(
                """
                UPDATE migration_state
                SET position_registry_backfill_complete = TRUE,
                    rows_synthesized = $4,
                    rows_skipped_already_present = $5,
                    backfill_completed_at = $6,
                    updated_at = NOW()
                WHERE deployment_id = $1
                  AND primitive = $2
                  AND cutover_key = $3
                """,
                deployment_id,
                primitive,
                cutover_key,
                int(request.rows_synthesized),
                int(request.rows_skipped_already_present),
                backfill_completed_at,
            )
            if self._pg_update_rowcount(status) == 0:
                self._warn_mark_backfill_complete_zero_rows(deployment_id, primitive, cutover_key)
            return gateway_pb2.MarkBackfillCompleteResponse(success=True)
        except Exception as e:
            logger.error(
                "MarkBackfillComplete PG failed (deployment=%s primitive=%s cutover_key=%s): %s",
                deployment_id,
                primitive,
                cutover_key,
                e,
            )
            return self._mark_backfill_complete_error(
                context,
                grpc.StatusCode.INTERNAL,
                "internal server error",
            )

    async def _mark_backfill_complete_sqlite(
        self,
        *,
        request: gateway_pb2.MarkBackfillCompleteRequest,
        context: grpc.aio.ServicerContext,
        deployment_id: str,
        primitive: str,
        cutover_key: str,
        backfill_completed_at_str: str,
    ) -> gateway_pb2.MarkBackfillCompleteResponse:
        try:
            await self._ensure_initialized()
            assert self._state_manager is not None
            warm = self._state_manager.warm_backend
            if warm is None or not hasattr(warm, "mark_backfill_complete"):
                return self._mark_backfill_complete_error(
                    context,
                    grpc.StatusCode.UNIMPLEMENTED,
                    "warm backend does not support mark_backfill_complete",
                )
            await warm.mark_backfill_complete(
                deployment_id=deployment_id,
                primitive=primitive,
                cutover_key=cutover_key,
                rows_synthesized=int(request.rows_synthesized),
                rows_skipped_already_present=int(request.rows_skipped_already_present),
                backfill_completed_at=backfill_completed_at_str,
            )
            return gateway_pb2.MarkBackfillCompleteResponse(success=True)
        except Exception as e:
            logger.error(
                "MarkBackfillComplete SQLite failed (deployment=%s primitive=%s cutover_key=%s): %s",
                deployment_id,
                primitive,
                cutover_key,
                e,
            )
            return self._mark_backfill_complete_error(
                context,
                grpc.StatusCode.INTERNAL,
                "internal server error",
            )

    async def MarkBackfillComplete(
        self,
        request: gateway_pb2.MarkBackfillCompleteRequest,
        context: grpc.aio.ServicerContext,
    ) -> gateway_pb2.MarkBackfillCompleteResponse:
        """Terminal flip — set complete=1 + final counters + completed_at."""
        validated = self._validate_mark_backfill_complete_request(request, context)
        if isinstance(validated, gateway_pb2.MarkBackfillCompleteResponse):
            return validated
        deployment_id, primitive, cutover_key, backfill_completed_at_str, backfill_completed_at = validated

        await self._ensure_snapshot_pool()
        if self._snapshot_pool is not None:
            # PostgreSQL mode (T19 / VIB-4205). Single-statement terminal
            # flip; matches the SQLite mark_backfill_complete contract
            # (sqlite.py:4122). A missing target row is NOT treated as an
            # error: the SQLite path also doesn't check ``rowcount``. The
            # upstream UpsertMigrationState was supposed to seed the
            # baseline row; absence is a contract violation, not infra
            # failure. We log a WARN below if rowcount=0.
            #
            # VIB-4191-dep: assumed BOOLEAN for
            # ``position_registry_backfill_complete`` (TRUE literal); if
            # Infra deploys INTEGER 0/1, change ``TRUE`` to ``1``.
            #
            # ``backfill_completed_at`` is bound as a ``datetime`` so asyncpg's
            # TIMESTAMPTZ codec accepts it. asyncpg rejects raw strings client-side
            # before any ``::timestamptz`` SQL cast would run (VIB-4313).
            return await self._mark_backfill_complete_pg(
                request=request,
                context=context,
                deployment_id=deployment_id,
                primitive=primitive,
                cutover_key=cutover_key,
                backfill_completed_at=backfill_completed_at,
            )
        return await self._mark_backfill_complete_sqlite(
            request=request,
            context=context,
            deployment_id=deployment_id,
            primitive=primitive,
            cutover_key=cutover_key,
            backfill_completed_at_str=backfill_completed_at_str,
        )

    @staticmethod
    def _position_events_filtered_error(
        context: grpc.aio.ServicerContext,
        code: grpc.StatusCode,
        err: str,
    ) -> gateway_pb2.GetPositionEventsFilteredResponse:
        context.set_code(code)
        context.set_details(err)
        return gateway_pb2.GetPositionEventsFilteredResponse(error=err)

    @staticmethod
    def _pg_position_event_row_dict(row: Any) -> dict[str, Any]:
        row_dict = dict(row)
        # Normalize the ``attribution_text`` alias back to the
        # ``attribution_json`` key the proto helper expects. VIB-4191-dep:
        # ``attribution_json::text`` cast assumes JSONB column; if Infra
        # deploys TEXT, drop the cast and remove this alias remap.
        row_dict["attribution_json"] = row_dict.pop("attribution_text", None) or "{}"
        return row_dict

    @staticmethod
    def _position_events_filtered_response(
        rows: Iterable[dict[str, Any]],
    ) -> gateway_pb2.GetPositionEventsFilteredResponse:
        response = gateway_pb2.GetPositionEventsFilteredResponse()
        for row in rows:
            response.events.append(_position_event_row_to_proto(row))
        return response

    async def _get_position_events_filtered_pg(
        self,
        *,
        deployment_id: str,
        position_types: frozenset[str],
        context: grpc.aio.ServicerContext,
    ) -> gateway_pb2.GetPositionEventsFilteredResponse:
        # PostgreSQL mode (T19 / VIB-4205). Empty position_types -> empty list
        # without hitting the DB (matches sqlite.py:4173).
        if not position_types:
            return gateway_pb2.GetPositionEventsFilteredResponse()

        try:
            rows = await self._snapshot_fetch(
                """
                SELECT id, deployment_id, cycle_id, execution_mode,
                       position_id, position_type, event_type,
                       EXTRACT(EPOCH FROM timestamp)::bigint AS timestamp,
                       protocol, chain,
                       token0, token1, amount0, amount1, value_usd, liquidity,
                       fees_token0, fees_token1, leverage, entry_price, mark_price,
                       unrealized_pnl, tx_hash, gas_usd, ledger_entry_id,
                       protocol_fees_usd,
                       attribution_json::text AS attribution_text,
                       attribution_version,
                       tick_lower, tick_upper, in_range, is_long
                FROM position_events
                WHERE deployment_id = $1
                  AND position_type = ANY($2::text[])
                ORDER BY position_id ASC, timestamp ASC, id ASC
                """,
                deployment_id,
                list(position_types),
            )
            row_dicts = [self._pg_position_event_row_dict(row) for row in rows]
            return self._position_events_filtered_response(row_dicts)
        except Exception as e:
            logger.error("GetPositionEventsFiltered PG failed: %s", e)
            return self._position_events_filtered_error(
                context,
                grpc.StatusCode.INTERNAL,
                "internal server error",
            )

    async def _get_position_events_filtered_sqlite(
        self,
        *,
        deployment_id: str,
        position_types: frozenset[str],
        context: grpc.aio.ServicerContext,
    ) -> gateway_pb2.GetPositionEventsFilteredResponse:
        try:
            await self._ensure_initialized()
            assert self._state_manager is not None
            warm = self._state_manager.warm_backend
            if warm is None or not hasattr(warm, "get_position_events_filtered"):
                return self._position_events_filtered_error(
                    context,
                    grpc.StatusCode.UNIMPLEMENTED,
                    "warm backend does not support get_position_events_filtered",
                )
            rows = await warm.get_position_events_filtered(
                deployment_id=deployment_id,
                position_types=position_types,
            )
            return self._position_events_filtered_response(rows)
        except Exception as e:
            logger.error("GetPositionEventsFiltered SQLite failed: %s", e)
            return self._position_events_filtered_error(
                context,
                grpc.StatusCode.INTERNAL,
                "internal server error",
            )

    async def GetPositionEventsFiltered(
        self,
        request: gateway_pb2.GetPositionEventsFilteredRequest,
        context: grpc.aio.ServicerContext,
    ) -> gateway_pb2.GetPositionEventsFilteredResponse:
        """Streamed position_events read for the backfill reader.

        Returns rows in (position_id ASC, timestamp ASC, id ASC) order
        (the SQLite accessor enforces this — cutover spec §3.5 fold
        determinism). Fresh deployments with no legacy position_events
        return an empty list — the backfill driver then iterates zero
        groups, calls MarkBackfillComplete with counters=0, and the
        cutover guard flips to complete=1 with no rows synthesized. This
        is the "no migration needed" fast path (cutover spec §2.4).
        """
        deployment_id = (request.deployment_id or "").strip()
        if not deployment_id:
            err = "deployment_id is required"
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(err)
            return gateway_pb2.GetPositionEventsFilteredResponse(error=err)

        types = frozenset(t.strip() for t in request.position_types if t.strip())
        await self._ensure_snapshot_pool()
        if self._snapshot_pool is not None:
            # PostgreSQL mode (T19 / VIB-4205). Empty position_types →
            # empty list without hitting the DB (matches sqlite.py:4173;
            # cutover spec §2.4 "no migration needed" fast path on fresh
            # deployments).
            return await self._get_position_events_filtered_pg(
                deployment_id=deployment_id,
                position_types=types,
                context=context,
            )
        return await self._get_position_events_filtered_sqlite(
            deployment_id=deployment_id,
            position_types=types,
            context=context,
        )

    @staticmethod
    def _position_registry_row_dict_to_proto(row: dict[str, Any]) -> gateway_pb2.PositionRegistryRow:
        """Map a WARM-backend row dict to the proto message.

        Pure projection — every field defaults to its proto-3 zero value
        when missing. ``payload`` re-serializes the dict to canonical JSON
        bytes (sort_keys=True for determinism); on payload serialization
        failure we emit ``b"{}"`` so the wire stays valid.
        """
        payload_obj = row.get("payload") or {}
        try:
            payload_bytes = json.dumps(payload_obj, sort_keys=True).encode("utf-8")
        except (TypeError, ValueError):
            payload_bytes = b"{}"
        return gateway_pb2.PositionRegistryRow(
            deployment_id=_row_text(row, "deployment_id"),
            chain=_row_text(row, "chain"),
            primitive=_row_text(row, "primitive"),
            accounting_category=_row_text(row, "accounting_category"),
            physical_identity_hash=_row_text(row, "physical_identity_hash"),
            semantic_grouping_key=_row_text(row, "semantic_grouping_key"),
            grouping_policy_version=_row_text(row, "grouping_policy_version"),
            handle=_row_text(row, "handle"),
            status=_row_text(row, "status"),
            payload=payload_bytes,
            opened_at_block=_row_int(row, "opened_at_block"),
            opened_tx=_row_text(row, "opened_tx"),
            closed_at_block=_row_int(row, "closed_at_block"),
            closed_tx=_row_text(row, "closed_tx"),
            last_reconciled_at_block=_row_int(row, "last_reconciled_at_block"),
            matching_policy_version=_row_int(row, "matching_policy_version", 1),
            payload_raw=_row_text(row, "payload_raw"),
            payload_decode_error=_row_text(row, "payload_decode_error"),
            payload_shape_error=_row_text(row, "payload_shape_error"),
        )

    @staticmethod
    def _position_registry_open_rows_query(
        deployment_id: str,
        *,
        chain: str | None,
        primitive: str | None,
        accounting_category: str | None,
    ) -> tuple[str, list[Any]]:
        sql_parts = [
            "SELECT deployment_id, chain, primitive, accounting_category,",
            "       physical_identity_hash, semantic_grouping_key,",
            "       grouping_policy_version, handle, status,",
            "       payload::text AS payload_text,",
            "       opened_at_block, opened_tx,",
            "       closed_at_block, closed_tx,",
            "       last_reconciled_at_block, matching_policy_version",
            "FROM position_registry",
            "WHERE deployment_id = $1 AND status = 'open'",
        ]
        params: list[Any] = [deployment_id]
        for column, value in (
            ("chain", chain),
            ("primitive", primitive),
            ("accounting_category", accounting_category),
        ):
            if value is None:
                continue
            params.append(value)
            sql_parts.append(f"  AND {column} = ${len(params)}")

        # SQLite default ASC ordering places NULLs FIRST. Postgres defaults
        # NULLs LAST for ASC, so pin the hosted path for cross-backend fold order.
        sql_parts.append("ORDER BY opened_at_block ASC NULLS FIRST, opened_tx ASC NULLS FIRST")
        return "\n".join(sql_parts), params

    @staticmethod
    def _pg_position_registry_row_dict(row: Any) -> dict[str, Any]:
        # Mirror the dict-shape guard in SQLite (sqlite.py:3819-3866):
        # parse payload text, surface diagnostic fields on failures, and
        # coerce to {} so downstream callers can always ``.get(...)``.
        row_dict: dict[str, Any] = dict(row)
        payload_text = row_dict.pop("payload_text", None) or "{}"
        try:
            parsed = json.loads(payload_text)
        except (TypeError, ValueError) as exc:
            logger.warning(
                "position_registry.payload JSON decode failed (PG) for "
                "deployment_id=%s chain=%s primitive=%s "
                "physical_identity_hash=%s: %s",
                row["deployment_id"],
                row["chain"],
                row["primitive"],
                row["physical_identity_hash"],
                exc,
            )
            row_dict["payload_raw"] = payload_text
            row_dict["payload_decode_error"] = str(exc)
            row_dict["payload"] = {}
            return row_dict

        if isinstance(parsed, dict):
            row_dict["payload"] = parsed
            return row_dict

        logger.warning(
            "position_registry.payload is not a JSON object (PG, got %s) for "
            "deployment_id=%s chain=%s primitive=%s "
            "physical_identity_hash=%s — coercing to {}.",
            type(parsed).__name__,
            row["deployment_id"],
            row["chain"],
            row["primitive"],
            row["physical_identity_hash"],
        )
        row_dict["payload_raw"] = payload_text
        row_dict["payload_shape_error"] = f"expected JSON object, got {type(parsed).__name__}"
        row_dict["payload"] = {}
        return row_dict

    def _position_registry_open_rows_response(
        self,
        rows: Iterable[dict[str, Any]],
    ) -> gateway_pb2.GetPositionRegistryOpenRowsResponse:
        response = gateway_pb2.GetPositionRegistryOpenRowsResponse()
        for row in rows:
            response.rows.append(self._position_registry_row_dict_to_proto(row))
        return response

    async def _get_position_registry_open_rows_pg(
        self,
        *,
        deployment_id: str,
        chain: str | None,
        primitive: str | None,
        accounting_category: str | None,
        context: grpc.aio.ServicerContext,
    ) -> gateway_pb2.GetPositionRegistryOpenRowsResponse:
        # PostgreSQL mode (T19 / VIB-4205). Dynamic WHERE additions mirror
        # the SQLite accessor (sqlite.py:3805-3813); empty-string filters were
        # normalized to None by ``_strip_optional`` before dispatch.
        try:
            sql, params = self._position_registry_open_rows_query(
                deployment_id,
                chain=chain,
                primitive=primitive,
                accounting_category=accounting_category,
            )
            rows = await self._snapshot_fetch(sql, *params)
            row_dicts = [self._pg_position_registry_row_dict(row) for row in rows]
            return self._position_registry_open_rows_response(row_dicts)
        except Exception as e:
            logger.error("GetPositionRegistryOpenRows PG failed: %s", e)
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details("internal server error")
            return gateway_pb2.GetPositionRegistryOpenRowsResponse(error="internal server error")

    async def _get_position_registry_open_rows_sqlite(
        self,
        *,
        deployment_id: str,
        chain: str | None,
        primitive: str | None,
        accounting_category: str | None,
        context: grpc.aio.ServicerContext,
    ) -> gateway_pb2.GetPositionRegistryOpenRowsResponse:
        try:
            await self._ensure_initialized()
            assert self._state_manager is not None
            warm = self._state_manager.warm_backend
            if warm is None or not hasattr(warm, "get_position_registry_open_rows"):
                error = "warm backend does not support get_position_registry_open_rows"
                logger.error("GetPositionRegistryOpenRows unsupported: %s", error)
                context.set_code(grpc.StatusCode.UNIMPLEMENTED)
                context.set_details(error)
                return gateway_pb2.GetPositionRegistryOpenRowsResponse(error=error)
            rows = await warm.get_position_registry_open_rows(
                deployment_id,
                chain=chain,
                primitive=primitive,
                accounting_category=accounting_category,
            )
            return self._position_registry_open_rows_response(rows)
        except Exception as e:
            logger.error("GetPositionRegistryOpenRows SQLite failed: %s", e)
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details("internal server error")
            return gateway_pb2.GetPositionRegistryOpenRowsResponse(error="internal server error")

    async def GetPositionRegistryOpenRows(
        self,
        request: gateway_pb2.GetPositionRegistryOpenRowsRequest,
        context: grpc.aio.ServicerContext,
    ) -> gateway_pb2.GetPositionRegistryOpenRowsResponse:
        """Read OPEN ``position_registry`` rows for a deployment.

        Routes to the WARM backend's ``get_position_registry_open_rows`` on
        SQLite. Filters are forwarded as-is — the empty string sentinel
        means "no filter on that column". In Postgres mode, the gateway builds
        the same filter shape against hosted ``position_registry``.

        The proto wire shape mirrors the dict returned by the SQLite
        accessor — the framework client adapter re-materializes the dict
        so the runner sees the same shape across local and gateway-backed
        runs. ``payload`` is JSON-bytes; the adapter parses it back to a
        dict. Block / tx anchors use 0 / "" as the "null" sentinel since
        proto3 scalars have no native null.
        """
        deployment_id = (request.deployment_id or "").strip()
        if not deployment_id:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details("deployment_id is required")
            return gateway_pb2.GetPositionRegistryOpenRowsResponse(error="deployment_id is required")

        chain = self._strip_optional(request.chain)
        primitive = self._strip_optional(request.primitive)
        accounting_category = self._strip_optional(request.accounting_category)

        await self._ensure_snapshot_pool()
        if self._snapshot_pool is not None:
            return await self._get_position_registry_open_rows_pg(
                deployment_id=deployment_id,
                chain=chain,
                primitive=primitive,
                accounting_category=accounting_category,
                context=context,
            )
        return await self._get_position_registry_open_rows_sqlite(
            deployment_id=deployment_id,
            chain=chain,
            primitive=primitive,
            accounting_category=accounting_category,
            context=context,
        )

    # =========================================================================
    # SaveLedgerAndRegistry helpers (extracted to keep the handler under the
    # CRAP gate threshold — see crap-refactor protocol applied to VIB-4208).
    # Each helper has one job; the handler orchestrates them in sequence.
    # =========================================================================

    @staticmethod
    def _validate_save_ledger_and_registry_request(
        request: gateway_pb2.SaveLedgerAndRegistryRequest,
        context: grpc.aio.ServicerContext,
    ) -> tuple[str, str, str, datetime] | gateway_pb2.SaveLedgerAndRegistryResponse:
        """Validate request invariants, mirroring SaveLedgerEntry.

        Returns either the validated tuple ``(deployment_id, registry_deployment_id,
        ledger_id, ts)`` or an error response (with gRPC context already marked). Enforces:
          * ``ledger.id`` is a non-empty UUID
          * ``deployment_id`` is non-blank
          * ``deployment_id`` matches the gateway format (validate only —
            no identity translation, identical to SaveLedgerEntry)
          * ``timestamp`` is in range
        CodeRabbit (PR #2230) flagged that the atomic RPC bypassed these
        invariants; this closes the gap symmetrically with the legacy path.
        """
        ledger_id = (request.id or "").strip()
        if not ledger_id:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details("ledger.id is required")
            return gateway_pb2.SaveLedgerAndRegistryResponse(
                success=False, error="ledger.id is required", error_class="ValueError"
            )
        try:
            uuid.UUID(ledger_id)
        except ValueError:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details("ledger.id must be a valid UUID")
            return gateway_pb2.SaveLedgerAndRegistryResponse(
                success=False, error="ledger.id must be a valid UUID", error_class="ValueError"
            )

        try:
            deployment_id = validate_deployment_id(request.deployment_id)
        except ValidationError as exc:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(str(exc))
            return gateway_pb2.SaveLedgerAndRegistryResponse(success=False, error=str(exc), error_class="ValueError")

        deployment_id = request.deployment_id.strip() if request.deployment_id else ""
        if not deployment_id:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details("deployment_id is required")
            return gateway_pb2.SaveLedgerAndRegistryResponse(
                success=False, error="deployment_id is required", error_class="ValueError"
            )

        if request.timestamp <= 0:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details("timestamp must be positive")
            return gateway_pb2.SaveLedgerAndRegistryResponse(
                success=False, error="timestamp must be positive", error_class="ValueError"
            )
        try:
            ts = datetime.fromtimestamp(request.timestamp, tz=UTC)
        except (ValueError, OSError, OverflowError):
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details("timestamp out of range")
            return gateway_pb2.SaveLedgerAndRegistryResponse(
                success=False, error="timestamp out of range", error_class="ValueError"
            )

        return deployment_id, deployment_id, ledger_id, ts

    @staticmethod
    def _decode_registry_payload(
        request: gateway_pb2.SaveLedgerAndRegistryRequest,
        context: grpc.aio.ServicerContext,
    ) -> dict[str, Any] | gateway_pb2.SaveLedgerAndRegistryResponse:
        """Decode ``registry_payload_json`` bytes to a dict.

        Rejects non-object JSON (array / string / number / null) with
        INVALID_ARGUMENT instead of silently coercing to ``{}`` — addresses
        CodeRabbit (PR #2230) concern that bad payloads would commit a row
        with empty payload, dropping data without surfacing a client error.
        Returns the dict on success or a response on validation failure.
        """
        if not request.registry_payload_json:
            return {}
        try:
            parsed = json.loads(request.registry_payload_json.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(f"registry_payload_json invalid: {exc}")
            return gateway_pb2.SaveLedgerAndRegistryResponse(
                success=False, error=f"registry_payload_json invalid: {exc}", error_class="ValueError"
            )
        if not isinstance(parsed, dict):
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details("registry_payload_json must decode to a JSON object")
            return gateway_pb2.SaveLedgerAndRegistryResponse(
                success=False,
                error="registry_payload_json must decode to a JSON object",
                error_class="ValueError",
            )
        return parsed

    @staticmethod
    def _build_ledger_entry_from_request(
        request: gateway_pb2.SaveLedgerAndRegistryRequest,
        ledger_id: str,
        deployment_id: str,
        ts: datetime,
    ) -> Any:
        """Reconstruct the ``LedgerEntry`` dataclass from the proto request.

        Field order mirrors :meth:`SaveLedgerEntry`. ``deployment_id`` and
        ``deployment_id`` come from the validated, resolved values rather
        than the raw request to keep both paths in lockstep.
        """
        from almanak.framework.observability.ledger import LedgerEntry

        def _opt_str(b: bytes | None) -> str:
            return b.decode("utf-8") if b else ""

        return LedgerEntry(
            id=ledger_id,
            cycle_id=request.cycle_id,
            deployment_id=deployment_id,
            execution_mode=request.execution_mode,
            timestamp=ts,
            intent_type=request.intent_type,
            token_in=request.token_in,
            amount_in=request.amount_in,
            token_out=request.token_out,
            amount_out=request.amount_out,
            effective_price=request.effective_price,
            slippage_bps=request.slippage_bps if request.HasField("slippage_bps") else None,
            gas_used=request.gas_used,
            gas_usd=request.gas_usd,
            tx_hash=request.tx_hash,
            chain=request.chain,
            protocol=request.protocol,
            success=request.success,
            error=request.error,
            extracted_data_json=_opt_str(request.extracted_data_json),
            price_inputs_json=_opt_str(request.price_inputs_json),
            pre_state_json=_opt_str(request.pre_state_json),
            post_state_json=_opt_str(request.post_state_json),
        )

    @staticmethod
    def _build_registry_row_from_request(
        request: gateway_pb2.SaveLedgerAndRegistryRequest,
        deployment_id: str,
        payload_dict: dict[str, Any],
        context: grpc.aio.ServicerContext,
    ) -> Any | gateway_pb2.SaveLedgerAndRegistryResponse:
        """Reconstruct the ``RegistryRow`` dataclass from the proto request.

        Validates ``registry_status`` against the Literal allowed values
        before assignment (addresses gemini-code-assist (PR #2230) comment
        about the ``# type: ignore[arg-type]``; the dataclass holds a
        Literal, not an Enum, so we validate explicitly and the type
        ignore is removed).
        """
        from typing import Literal, cast

        from almanak.framework.accounting.commit import RegistryRow

        status_raw = request.registry_status
        if status_raw not in ("open", "closed", "reorg_invalidated"):
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            details = f"registry_status must be one of 'open' / 'closed' / 'reorg_invalidated' (got {status_raw!r})"
            context.set_details(details)
            return gateway_pb2.SaveLedgerAndRegistryResponse(success=False, error=details, error_class="ValueError")
        # Runtime guard above narrows ``status_raw`` to the Literal union;
        # ``cast`` informs mypy of the narrowing (proto3 fields are typed
        # as ``str`` on the wire — see gemini PR #2230 thread).
        status_literal = cast(Literal["open", "closed", "reorg_invalidated"], status_raw)

        return RegistryRow(
            deployment_id=deployment_id,
            chain=request.registry_chain,
            primitive=request.registry_primitive,
            accounting_category=request.registry_accounting_category,
            physical_identity_hash=request.registry_physical_identity_hash,
            semantic_grouping_key=request.registry_semantic_grouping_key,
            grouping_policy_version=request.registry_grouping_policy_version,
            handle=request.registry_handle or None,
            status=status_literal,
            payload=payload_dict,
            matching_policy_version=request.registry_matching_policy_version or 1,
            opened_at_block=(
                request.registry_opened_at_block if request.HasField("registry_opened_at_block") else None
            ),
            opened_tx=request.registry_opened_tx or None,
            closed_at_block=(
                request.registry_closed_at_block if request.HasField("registry_closed_at_block") else None
            ),
            closed_tx=request.registry_closed_tx or None,
            last_reconciled_at_block=(
                request.registry_last_reconciled_at_block
                if request.HasField("registry_last_reconciled_at_block")
                else None
            ),
        )

    @staticmethod
    def _build_handle_mapping_from_request(
        request: gateway_pb2.SaveLedgerAndRegistryRequest,
    ) -> Any | None:
        """Reconstruct optional ``HandleMapping``; None when handle is absent."""
        if not request.handle_mapping_handle:
            return None
        from almanak.framework.accounting.commit import HandleMapping

        return HandleMapping(
            handle=request.handle_mapping_handle,
            deployment_id=request.handle_mapping_deployment_id,
            accounting_category=request.handle_mapping_accounting_category,
        )

    @staticmethod
    def _validate_ledger_replay_json_fields(
        request: gateway_pb2.SaveLedgerAndRegistryRequest,
        context: grpc.aio.ServicerContext,
    ) -> gateway_pb2.SaveLedgerAndRegistryResponse | None:
        """Validate the four ledger replay JSON fields at the gateway boundary.

        Mirrors the validation already performed by :meth:`SaveLedgerEntry`
        (state_service.py:1196–1227): every payload bound to ``::jsonb``
        in the Postgres branch must be (1) valid UTF-8 and (2) parseable
        as JSON. Without this gate, bad input surfaces as a downstream
        ``AccountingPersistenceError`` / gRPC INTERNAL rather than an
        ``INVALID_ARGUMENT`` at the boundary — and the SQLite path
        (which stores the raw string un-validated) diverges silently.

        Returns ``None`` on success or a populated INVALID_ARGUMENT
        response on the first failure. Only applies on the Postgres
        path; the SQLite branch already accepts raw strings by design.

        CodeRabbit (PR #2239) major finding.
        """
        fields = (
            ("extracted_data_json", request.extracted_data_json),
            ("price_inputs_json", request.price_inputs_json),
            ("pre_state_json", request.pre_state_json),
            ("post_state_json", request.post_state_json),
        )
        for field_name, raw in fields:
            if not raw:
                continue
            try:
                decoded = raw.decode("utf-8")
            except UnicodeDecodeError as exc:
                details = f"{field_name} must be valid UTF-8: {exc}"
                context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
                context.set_details(details)
                return gateway_pb2.SaveLedgerAndRegistryResponse(
                    success=False,
                    error=details,
                    error_class="ValueError",
                )
            try:
                json.loads(decoded)
            except json.JSONDecodeError as exc:
                details = f"{field_name} must be valid JSON: {exc}"
                context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
                context.set_details(details)
                return gateway_pb2.SaveLedgerAndRegistryResponse(
                    success=False,
                    error=details,
                    error_class="ValueError",
                )
        return None

    @staticmethod
    def _classify_save_ledger_and_registry_error(
        exc: Exception,
        ledger_id: str,
    ) -> gateway_pb2.SaveLedgerAndRegistryResponse | None:
        """Map a backend exception to the typed error response.

        Returns the typed response for known classes (VIB-4200 collision
        and AccountingPersistenceError); returns ``None`` if the exception
        should propagate to the generic INTERNAL handler. Keeps the
        success-path commit out of the broad ``except`` block.
        """
        from almanak.framework.state.exceptions import AccountingPersistenceError
        from almanak.framework.state.registry_errors import RegistryAutoCollisionError

        if isinstance(exc, RegistryAutoCollisionError):
            logger.error(
                "SaveLedgerAndRegistry RegistryAutoCollisionError for id=%s: %s",
                ledger_id,
                exc,
            )
            return gateway_pb2.SaveLedgerAndRegistryResponse(
                success=False,
                error=str(exc),
                error_class="RegistryAutoCollisionError",
            )
        if isinstance(exc, AccountingPersistenceError):
            logger.error(
                "SaveLedgerAndRegistry AccountingPersistenceError for id=%s: %s",
                ledger_id,
                exc,
            )
            return gateway_pb2.SaveLedgerAndRegistryResponse(
                success=False,
                error=str(exc),
                error_class="AccountingPersistenceError",
            )
        return None

    # VIB-4191-dep: the partial unique index on ``position_registry``
    # WHERE ``status = 'open' AND handle IS NULL`` MUST be named
    # ``ix_registry_auto_mode`` in the metrics-database migration; this
    # name is what ``_save_ledger_and_registry_pg`` matches on
    # ``asyncpg.UniqueViolationError.constraint_name`` to distinguish
    # auto-mode collisions from generic handle / PK conflicts. If Infra
    # uses a different name, change the constraint_name string here.
    _AUTO_MODE_INDEX_NAME = "ix_registry_auto_mode"

    async def _set_pg_transaction_search_path(self, conn: Any) -> None:
        # search_path MUST be set INSIDE the transaction so the
        # ``is_local=true`` (3rd arg) SET is scoped to the transaction
        # that owns the writes below. If set before ``conn.transaction()``,
        # asyncpg wraps the SET in its own implicit transaction, which
        # commits-and-reverts the search_path before the atomic block.
        if not self._snapshot_schema:
            return
        await conn.fetchval(
            "SELECT pg_catalog.set_config('search_path', $1, true)",
            self._snapshot_schema,
        )

    async def _classify_pg_save_ledger_registry_unique_violation(
        self,
        *,
        uve: BaseException,
        effective_handle: str | None,
        ledger_id: str,
        deployment_id: str,
        registry: Any,
        category_str: str,
    ) -> gateway_pb2.SaveLedgerAndRegistryResponse:
        # VIB-4200 / UAT D3.F1, F8, F10: distinguish auto-mode collision
        # from other UNIQUE-constraint violations. Handle-bearing rows cannot
        # trip ``ix_registry_auto_mode`` because the partial index is defined
        # over ``status='open' AND handle IS NULL``.
        if effective_handle is not None:
            return _wrap_pg_persistence_error(uve, ledger_id)
        if getattr(uve, "constraint_name", None) != self._AUTO_MODE_INDEX_NAME:
            return _wrap_pg_persistence_error(uve, ledger_id)

        existing_pih, existing_tx = await self._lookup_auto_mode_collision_partner(
            deployment_id=deployment_id,
            chain=registry.chain,
            accounting_category=category_str,
            semantic_grouping_key=registry.semantic_grouping_key,
        )
        if not existing_pih or existing_pih == registry.physical_identity_hash:
            return _wrap_pg_persistence_error(uve, ledger_id)

        from almanak.framework.state.registry_errors import (
            RegistryAutoCollisionError,
        )

        return self._classify_save_ledger_and_registry_error(
            RegistryAutoCollisionError(
                semantic_grouping_key=registry.semantic_grouping_key,
                existing_physical_identity_hash=existing_pih,
                opened_tx=existing_tx or "",
                accounting_category=category_str,
            ),
            ledger_id,
        ) or gateway_pb2.SaveLedgerAndRegistryResponse(
            success=False,
            error="auto-mode collision",
            error_class="RegistryAutoCollisionError",
        )

    async def _save_ledger_and_registry_pg(
        self,
        *,
        ledger: Any,
        registry: Any,
        effective_handle: str | None,
        ledger_id: str,
        mode: str = "commit",
    ) -> gateway_pb2.SaveLedgerAndRegistryResponse:
        """Atomic Postgres commit of ledger + position_registry + handle (T19 / VIB-4205).

        Mirrors :meth:`SQLiteStore.save_ledger_and_registry_atomic`
        line-for-line, but uses asyncpg with a single explicitly-acquired
        connection wrapped in ``async with conn.transaction():`` so the
        three writes commit as one Postgres transaction. **DO NOT** call
        ``_snapshot_execute`` three times here — that acquires a fresh
        connection per call and defeats atomicity. The anti-bypass test
        ``test_atomic_three_writes_one_tx`` verifies this contract.

        Mode contract (T24 / VIB-4210 / VIB-4221 ADR §8.1 — ratified Option (c)):

        - ``mode='commit'`` (default; backward-compatible): runs all three
          writes (ledger INSERT → registry UPSERT → handle backfill).
        - ``mode='registry_reconciliation'``: control-plane reconciliation
          path. SKIPS the ledger INSERT (step 1) — runs only the registry
          UPSERT + handle backfill atomically inside the same Postgres
          transaction. Invoked exclusively by ``PositionService.Reconcile``
          when ``apply=true`` (ADR §2.3 #1+#2: ledger MUST NOT be touched
          on the reconciliation path). The ledger transaction is still
          opened (so the registry + handle still commit atomically), but
          the INSERT statement itself is skipped. Backward compatibility:
          callers that omit the mode argument run the original T11/T19
          three-write contract bit-identically.

        Idempotency:
        - Ledger: ``INSERT ... ON CONFLICT (id) DO UPDATE SET <all cols>``
          (same shape as the existing SaveLedgerEntry PG branch).
        - Registry: ``ON CONFLICT (deployment_id, chain, primitive,
          physical_identity_hash) DO UPDATE SET ... WHERE
          <priority_excluded> > <priority_existing>`` — strict-monotone
          priority guard (open=0, closed=1, reorg_invalidated=1).
        - Handle backfill: separate idempotent UPDATE gated on existing
          ``handle IS NULL`` (matches sqlite.py:3065).

        Error classification — three branches, mirrors the SQLite contract:
        1. ``RegistryAutoCollisionError`` when
           ``asyncpg.UniqueViolationError.constraint_name ==
           _AUTO_MODE_INDEX_NAME`` AND the incoming row has no handle
           (handle-bearing rows can't have come from the partial index per
           UAT D3.F10).
        2. ``AccountingPersistenceError`` for any other ``PostgresError``
           (CHECK / NOT NULL / FK / other UNIQUE / connection errors).
        3. ``INTERNAL`` for everything else.
        """
        import asyncpg

        assert self._snapshot_pool is not None

        # T24 / VIB-4210: validate mode at the boundary. Same rule as the
        # SQLite path (sqlite.py:save_ledger_and_registry_atomic) — only
        # two values are accepted; anything else surfaces as ValueError
        # rather than silently routing to a default branch.
        if mode not in ("commit", "registry_reconciliation"):
            raise ValueError(
                f"_save_ledger_and_registry_pg: invalid mode={mode!r}; expected 'commit' or 'registry_reconciliation'."
            )
        skip_ledger = mode == "registry_reconciliation"

        primitive_str = registry.primitive_value()
        category_str = registry.accounting_category_value()
        payload_json = registry.payload_json()
        deployment_id = registry.deployment_id

        async def _upsert_ledger_row(conn: Any) -> None:
            # 1) Ledger row — upsert keyed on id (matches the existing
            # SaveLedgerEntry PG branch line 1207).
            await conn.execute(
                """
                INSERT INTO transaction_ledger (
                    id, cycle_id, deployment_id, execution_mode,
                    timestamp, intent_type,
                    token_in, amount_in, token_out, amount_out,
                    effective_price, slippage_bps, gas_used, gas_usd,
                    tx_hash, chain, protocol, success, error,
                    extracted_data_json, price_inputs_json,
                    pre_state_json, post_state_json
                ) VALUES (
                    $1, $2, $3, $4, $5, $6, $7, $8, $9,
                    $10, $11, $12, $13, $14, $15, $16, $17, $18, $19,
                    $20::jsonb, $21::jsonb, $22::jsonb, $23::jsonb
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
                ledger.id,
                ledger.cycle_id,
                getattr(ledger, "deployment_id", "") or "",
                getattr(ledger, "execution_mode", "") or "",
                ledger.timestamp,
                ledger.intent_type,
                ledger.token_in,
                ledger.amount_in,
                ledger.token_out,
                ledger.amount_out,
                ledger.effective_price,
                ledger.slippage_bps,
                ledger.gas_used,
                ledger.gas_usd,
                ledger.tx_hash,
                ledger.chain,
                ledger.protocol,
                ledger.success,
                ledger.error,
                ledger.extracted_data_json or None,
                ledger.price_inputs_json or None,
                ledger.pre_state_json or None,
                ledger.post_state_json or None,
            )

        async def _release_reusable_terminal_handle(conn: Any) -> None:
            # 2a) Handle reuse after close (VIB-5051, mirrors the SQLite
            # branch). Release only TERMINAL rows inside this transaction so
            # the handle tracks the CURRENT physical position.
            if effective_handle is None:
                return
            await conn.execute(
                """
                UPDATE position_registry
                SET handle = NULL
                WHERE deployment_id = $1
                  AND accounting_category = $2
                  AND handle = $3
                  AND status IN ('closed', 'reorg_invalidated')
                  AND physical_identity_hash != $4
                """,
                deployment_id,
                category_str,
                effective_handle,
                registry.physical_identity_hash,
            )

        async def _upsert_registry_row(conn: Any) -> None:
            # 2) Registry row with priority-gated UPSERT. The CASE expression
            # materializes the priority inline so the comparison is atomic with
            # the existing row. Mapping kept in lock-step with blueprint 28 §4.3.
            await conn.execute(
                """
                INSERT INTO position_registry (
                    deployment_id, chain, primitive, accounting_category,
                    physical_identity_hash, semantic_grouping_key,
                    grouping_policy_version,
                    handle, status, payload,
                    opened_at_block, opened_tx,
                    closed_at_block, closed_tx,
                    last_reconciled_at_block, matching_policy_version
                ) VALUES (
                    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10::jsonb,
                    $11, $12, $13, $14, $15, $16
                )
                ON CONFLICT (deployment_id, chain, primitive, physical_identity_hash)
                DO UPDATE SET
                    status = EXCLUDED.status,
                    payload = EXCLUDED.payload,
                    handle = COALESCE(position_registry.handle, EXCLUDED.handle),
                    closed_at_block = COALESCE(EXCLUDED.closed_at_block, position_registry.closed_at_block),
                    closed_tx = COALESCE(EXCLUDED.closed_tx, position_registry.closed_tx),
                    last_reconciled_at_block = COALESCE(
                        EXCLUDED.last_reconciled_at_block,
                        position_registry.last_reconciled_at_block
                    ),
                    grouping_policy_version = EXCLUDED.grouping_policy_version,
                    matching_policy_version = EXCLUDED.matching_policy_version,
                    semantic_grouping_key = EXCLUDED.semantic_grouping_key,
                    accounting_category = EXCLUDED.accounting_category
                WHERE
                    (CASE EXCLUDED.status
                        WHEN 'open' THEN 0
                        WHEN 'closed' THEN 1
                        WHEN 'reorg_invalidated' THEN 1
                        ELSE -1
                    END)
                    >
                    (CASE position_registry.status
                        WHEN 'open' THEN 0
                        WHEN 'closed' THEN 1
                        WHEN 'reorg_invalidated' THEN 1
                        ELSE -1
                    END)
                """,
                deployment_id,
                registry.chain,
                primitive_str,
                category_str,
                registry.physical_identity_hash,
                registry.semantic_grouping_key,
                registry.grouping_policy_version,
                effective_handle,
                registry.status,
                payload_json,
                registry.opened_at_block,
                registry.opened_tx,
                registry.closed_at_block,
                registry.closed_tx,
                registry.last_reconciled_at_block,
                registry.matching_policy_version,
            )

        async def _backfill_same_status_handle(conn: Any) -> None:
            # 3) Same-status retry handle backfill (matches sqlite.py:3065).
            # The priority-gated WHERE above skips the DO UPDATE entirely when
            # status doesn't strictly increase, so a row landed with handle=NULL
            # stays NULL forever without this idempotent UPDATE.
            if effective_handle is None:
                return
            await conn.execute(
                """
                UPDATE position_registry
                SET handle = $1
                WHERE deployment_id = $2
                  AND chain = $3
                  AND primitive = $4
                  AND physical_identity_hash = $5
                  AND handle IS NULL
                """,
                effective_handle,
                deployment_id,
                registry.chain,
                primitive_str,
                registry.physical_identity_hash,
            )

        async def _write_atomic_rows(conn: Any) -> None:
            await self._set_pg_transaction_search_path(conn)
            # T24 / VIB-4210: under mode='registry_reconciliation' the ledger
            # INSERT is skipped, but the transaction stays open so registry
            # UPSERT + handle backfill still commit atomically.
            if not skip_ledger:
                await _upsert_ledger_row(conn)
            await _release_reusable_terminal_handle(conn)
            await _upsert_registry_row(conn)
            await _backfill_same_status_handle(conn)

        # VIB-4191-dep: JSONB / TIMESTAMPTZ assumptions match the
        # SaveLedgerEntry PG branch above; ``::jsonb`` casts on the four
        # replay columns and ``payload`` are required iff Infra deploys
        # JSONB. If Infra deploys TEXT, drop every ``::jsonb`` cast in
        # this method (the underlying bind is already a UTF-8 string).
        try:
            async with self._snapshot_pool.acquire() as conn:
                async with conn.transaction():
                    await _write_atomic_rows(conn)
            return gateway_pb2.SaveLedgerAndRegistryResponse(success=True)
        except asyncpg.UniqueViolationError as uve:
            return await self._classify_pg_save_ledger_registry_unique_violation(
                uve=uve,
                effective_handle=effective_handle,
                ledger_id=ledger_id,
                deployment_id=deployment_id,
                registry=registry,
                category_str=category_str,
            )
        except asyncpg.PostgresError as pe:
            # CHECK / NOT NULL / FK / OperationalError / connection drop /
            # anything else PG-typed — wrap as AccountingPersistenceError.
            return _wrap_pg_persistence_error(pe, ledger_id)

    async def _lookup_auto_mode_collision_partner(
        self,
        *,
        deployment_id: str,
        chain: str,
        accounting_category: str,
        semantic_grouping_key: str,
    ) -> tuple[str | None, str | None]:
        """Find the opposing OPEN handle-less row for the auto-mode collision.

        Run after the failing INSERT's transaction has rolled back; the
        SELECT runs on a fresh connection / implicit transaction. Returns
        (physical_identity_hash, opened_tx) of the row that's blocking
        the partial unique index, or (None, None) if no match.

        VIB-4191-dep: the SELECT predicate MUST exactly mirror the
        ``ix_registry_auto_mode`` partial index's WHERE clause
        (``status = 'open' AND handle IS NULL``). If Infra changes either
        the index predicate OR the rows-on-which-it-fires, this lookup
        diverges from the partial-index condition and may misclassify.
        """
        row = await self._snapshot_fetchrow(
            """
            SELECT physical_identity_hash, opened_tx
            FROM position_registry
            WHERE deployment_id = $1
              AND chain = $2
              AND accounting_category = $3
              AND semantic_grouping_key = $4
              AND status = 'open'
              AND handle IS NULL
            LIMIT 1
            """,
            deployment_id,
            chain,
            accounting_category,
            semantic_grouping_key,
        )
        if row is None:
            return None, None
        return row["physical_identity_hash"], row["opened_tx"]

    async def SaveLedgerAndRegistry(
        self,
        request: gateway_pb2.SaveLedgerAndRegistryRequest,
        context: grpc.aio.ServicerContext,
    ) -> gateway_pb2.SaveLedgerAndRegistryResponse:
        """Atomic ledger + position_registry + handle commit (T11 / VIB-4197 SQLite; T19 / VIB-4205 Postgres).

        Dispatches to ``_save_ledger_and_registry_pg`` (T19 / VIB-4205)
        when the snapshot pool is configured, or to
        ``StateManager.save_ledger_and_registry`` (SQLite) otherwise.

        SQLite path wraps the three writes in ``BEGIN IMMEDIATE`` /
        ``COMMIT``; Postgres path wraps them in a single
        ``async with conn.transaction():``. Failure on either backend
        rolls back atomically.

        Error contract (matches T11):
        - RegistryAutoCollisionError → error_class="RegistryAutoCollisionError"
          (programming bug, NOT infrastructure failure — VIB-4200).
        - AccountingPersistenceError → error_class="AccountingPersistenceError".
        - Any other backend exception → error_class="INTERNAL", gRPC INTERNAL.
        The client adapter raises the matching exception class so the
        runner's existing fail-closed pipeline handles each one correctly.

        Input validation mirrors :meth:`SaveLedgerEntry` (UUID, non-blank
        non-blank deployment_id, valid deployment_id format, positive timestamp). Helpers
        live alongside the handler so the success path stays a thin
        orchestrator under the CRAP threshold.
        """
        validated = self._validate_save_ledger_and_registry_request(request, context)
        if isinstance(validated, gateway_pb2.SaveLedgerAndRegistryResponse):
            return validated
        deployment_id, _, ledger_id, ts = validated

        payload = self._decode_registry_payload(request, context)
        if isinstance(payload, gateway_pb2.SaveLedgerAndRegistryResponse):
            return payload

        # T24 / VIB-4210: mode validation at the boundary. Proto3 default
        # "" routes to the legacy ("commit") path bit-identically.
        mode = request.mode or "commit"
        if mode not in ("commit", "registry_reconciliation"):
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(f"invalid mode={mode!r}; expected 'commit' or 'registry_reconciliation'")
            return gateway_pb2.SaveLedgerAndRegistryResponse(
                success=False,
                error=f"invalid mode={mode!r}",
                error_class="ValueError",
            )

        try:
            ledger = self._build_ledger_entry_from_request(request, ledger_id, deployment_id, ts)
            registry = self._build_registry_row_from_request(request, deployment_id, payload, context)
            if isinstance(registry, gateway_pb2.SaveLedgerAndRegistryResponse):
                return registry
            handle = self._build_handle_mapping_from_request(request)

            await self._ensure_snapshot_pool()
            if self._snapshot_pool is not None:
                # PostgreSQL mode (T19 / VIB-4205). Validate the four
                # ledger replay JSON fields at the boundary so malformed
                # input is rejected as INVALID_ARGUMENT (matching
                # :meth:`SaveLedgerEntry`) instead of surfacing as a
                # downstream ``AccountingPersistenceError`` / INTERNAL
                # from the ``::jsonb`` cast (CodeRabbit major, PR #2239).
                json_invalid = self._validate_ledger_replay_json_fields(request, context)
                if json_invalid is not None:
                    return json_invalid
                # Effective handle resolution matches sqlite.py:2915.
                effective_handle = (
                    registry.handle if registry.handle is not None else (handle.handle if handle is not None else None)
                )
                return await self._save_ledger_and_registry_pg(
                    ledger=ledger,
                    registry=registry,
                    effective_handle=effective_handle,
                    ledger_id=ledger_id,
                    mode=mode,
                )

            # SQLite mode (T22 / VIB-4208).
            await self._ensure_initialized()
            assert self._state_manager is not None
            try:
                await self._state_manager.save_ledger_and_registry(
                    ledger=ledger,
                    registry=registry,
                    handle=handle,
                    mode=mode,
                )
            except Exception as exc:
                typed = self._classify_save_ledger_and_registry_error(exc, ledger_id)
                if typed is not None:
                    return typed
                raise
            return gateway_pb2.SaveLedgerAndRegistryResponse(success=True)
        except Exception as e:
            logger.error("SaveLedgerAndRegistry failed for id=%s: %s", ledger_id, e)
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details("internal server error")
            return gateway_pb2.SaveLedgerAndRegistryResponse(
                success=False, error="internal server error", error_class="INTERNAL"
            )
