"""Tests for the SaveLedgerEntry gRPC endpoint (VIB-3201).

Closes the VIB-3157 gateway gap: verifies the handler persists ledger
entries via the warm backend (SQLite mode), validates required fields,
respects the fail-closed contract on backend failures, and preserves
``slippage_bps`` presence/absence across the wire.
"""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock

import grpc
import pytest

from almanak.framework.observability.ledger import LedgerEntry
from almanak.gateway.core.settings import GatewaySettings
from almanak.gateway.proto import gateway_pb2
from almanak.gateway.services.state_service import StateServiceServicer


@pytest.fixture
def settings() -> GatewaySettings:
    return GatewaySettings()


@pytest.fixture
def mock_context() -> MagicMock:
    ctx = MagicMock(spec=grpc.aio.ServicerContext)
    ctx.set_code = MagicMock()
    ctx.set_details = MagicMock()
    return ctx


@pytest.fixture
def state_service(settings: GatewaySettings) -> StateServiceServicer:
    svc = StateServiceServicer(settings)
    svc._initialized = True
    svc._snapshot_pool_initialized = True  # skip asyncpg pool init
    svc._snapshot_pool = None  # force SQLite delegate path
    return svc


_ENTRY_UUID = "550e8400-e29b-41d4-a716-446655440000"


def _base_request(**overrides) -> gateway_pb2.SaveLedgerEntryRequest:
    defaults = {
        "id": _ENTRY_UUID,
        "cycle_id": "cycle-1",
        "deployment_id": "deploy-1",
        "execution_mode": "live",
        "timestamp": 1712000000,
        "intent_type": "SWAP",
        "token_in": "USDC",
        "amount_in": "100",
        "token_out": "ETH",
        "amount_out": "0.05",
        "effective_price": "2000",
        "gas_used": 21000,
        "gas_usd": "1.50",
        "tx_hash": "0xabc",
        "chain": "arbitrum",
        "protocol": "uniswap_v3",
        "success": True,
        "error": "",
        "extracted_data_json": b"",
    }
    defaults.update(overrides)
    req = gateway_pb2.SaveLedgerEntryRequest(**defaults)
    # slippage_bps is ``optional``; only set if the caller wants it present.
    if "slippage_bps" in overrides:
        req.slippage_bps = overrides["slippage_bps"]
    return req


class TestSaveLedgerEntryValidation:
    """Handler rejects malformed requests before touching the backend."""

    @pytest.mark.asyncio
    @pytest.mark.parametrize("deployment_id", ["", "   "])
    async def test_missing_deployment_id(self, state_service, mock_context, deployment_id):
        request = _base_request(deployment_id=deployment_id)
        response = await state_service.SaveLedgerEntry(request, mock_context)

        assert response.success is False
        mock_context.set_code.assert_called_with(grpc.StatusCode.INVALID_ARGUMENT)

    @pytest.mark.asyncio
    async def test_missing_entry_id(self, state_service, mock_context):
        request = _base_request(id="")
        response = await state_service.SaveLedgerEntry(request, mock_context)

        assert response.success is False
        assert "id is required" in response.error
        mock_context.set_code.assert_called_with(grpc.StatusCode.INVALID_ARGUMENT)

    @pytest.mark.asyncio
    @pytest.mark.parametrize("timestamp", [0, -1])
    async def test_non_positive_timestamp(self, state_service, mock_context, timestamp):
        request = _base_request(timestamp=timestamp)
        response = await state_service.SaveLedgerEntry(request, mock_context)

        assert response.success is False
        assert "timestamp must be positive" in response.error
        mock_context.set_code.assert_called_with(grpc.StatusCode.INVALID_ARGUMENT)

    @pytest.mark.asyncio
    async def test_timestamp_out_of_range(self, state_service, mock_context):
        """Timestamps too large for datetime.fromtimestamp() return INVALID_ARGUMENT."""
        request = _base_request(timestamp=9_999_999_999_999)
        response = await state_service.SaveLedgerEntry(request, mock_context)

        assert response.success is False
        assert "timestamp out of range" in response.error
        mock_context.set_code.assert_called_with(grpc.StatusCode.INVALID_ARGUMENT)

    @pytest.mark.asyncio
    async def test_invalid_utf8_extracted_data_json(self, state_service, mock_context):
        """Non-UTF-8 bytes in extracted_data_json return INVALID_ARGUMENT, not INTERNAL."""
        request = _base_request(extracted_data_json=b"\xff\xfe invalid utf-8")
        response = await state_service.SaveLedgerEntry(request, mock_context)

        assert response.success is False
        assert "extracted_data_json" in response.error
        mock_context.set_code.assert_called_with(grpc.StatusCode.INVALID_ARGUMENT)

    @pytest.mark.asyncio
    async def test_invalid_uuid_entry_id(self, state_service, mock_context):
        """A non-UUID entry id is rejected with INVALID_ARGUMENT."""
        request = _base_request(id="not-a-uuid")
        response = await state_service.SaveLedgerEntry(request, mock_context)

        assert response.success is False
        assert "UUID" in response.error
        mock_context.set_code.assert_called_with(grpc.StatusCode.INVALID_ARGUMENT)

    @pytest.mark.asyncio
    @pytest.mark.parametrize("deployment_id", ["", "   ", "\t", "\n  "])
    async def test_blank_or_whitespace_deployment_id_rejected(self, state_service, mock_context, deployment_id):
        """Blank / whitespace-only deployment_id is rejected at the boundary.

        Symmetric with GetAccountingEvents -- a row written with no
        deployment_id can never be returned by the new replay RPC, which
        would silently break restart reconstruction and snapshot enrichment.
        """
        request = _base_request(deployment_id=deployment_id)
        response = await state_service.SaveLedgerEntry(request, mock_context)

        assert response.success is False
        assert "deployment_id" in response.error
        assert "required" in response.error
        mock_context.set_code.assert_called_with(grpc.StatusCode.INVALID_ARGUMENT)


class TestSaveLedgerEntrySqliteDelegate:
    """SQLite mode delegates to the warm backend's ``save_ledger_entry``."""

    @pytest.mark.asyncio
    async def test_happy_path(self, state_service, mock_context):
        warm = AsyncMock()
        warm.save_ledger_entry = AsyncMock()
        mock_sm = MagicMock()
        mock_sm.warm_backend = warm
        state_service._state_manager = mock_sm

        request = _base_request(slippage_bps=12.5)
        response = await state_service.SaveLedgerEntry(request, mock_context)

        assert response.success is True
        assert response.error == ""
        warm.save_ledger_entry.assert_called_once()

        entry: LedgerEntry = warm.save_ledger_entry.call_args[0][0]
        assert isinstance(entry, LedgerEntry)
        assert entry.id == _ENTRY_UUID
        assert entry.cycle_id == "cycle-1"
        assert entry.deployment_id == "deploy-1"
        assert entry.execution_mode == "live"
        assert entry.timestamp == datetime.fromtimestamp(1712000000, tz=UTC)
        assert entry.intent_type == "SWAP"
        assert entry.slippage_bps == pytest.approx(12.5)
        assert entry.gas_used == 21000
        assert entry.tx_hash == "0xabc"
        assert entry.chain == "arbitrum"
        assert entry.protocol == "uniswap_v3"
        assert entry.success is True
        assert entry.error == ""

    @pytest.mark.asyncio
    async def test_slippage_absent_maps_to_none(self, state_service, mock_context):
        """Omitted ``slippage_bps`` must round-trip as ``None``, not 0.0."""
        warm = AsyncMock()
        warm.save_ledger_entry = AsyncMock()
        mock_sm = MagicMock()
        mock_sm.warm_backend = warm
        state_service._state_manager = mock_sm

        request = _base_request()  # no slippage_bps
        response = await state_service.SaveLedgerEntry(request, mock_context)

        assert response.success is True
        entry: LedgerEntry = warm.save_ledger_entry.call_args[0][0]
        assert entry.slippage_bps is None

    @pytest.mark.asyncio
    async def test_extracted_data_json_decoded(self, state_service, mock_context):
        warm = AsyncMock()
        warm.save_ledger_entry = AsyncMock()
        mock_sm = MagicMock()
        mock_sm.warm_backend = warm
        state_service._state_manager = mock_sm

        payload = b'{"foo": "bar"}'
        request = _base_request(extracted_data_json=payload)
        response = await state_service.SaveLedgerEntry(request, mock_context)

        assert response.success is True
        entry: LedgerEntry = warm.save_ledger_entry.call_args[0][0]
        assert entry.extracted_data_json == '{"foo": "bar"}'

    @pytest.mark.asyncio
    async def test_no_warm_backend(self, state_service, mock_context):
        mock_sm = MagicMock()
        mock_sm.warm_backend = None
        state_service._state_manager = mock_sm

        request = _base_request()
        response = await state_service.SaveLedgerEntry(request, mock_context)

        assert response.success is False
        assert "save_ledger_entry" in response.error
        mock_context.set_code.assert_called_with(grpc.StatusCode.UNIMPLEMENTED)

    @pytest.mark.asyncio
    async def test_backend_failure(self, state_service, mock_context):
        """Fail-closed: backend RuntimeError maps to INTERNAL + success=false."""
        warm = AsyncMock()
        warm.save_ledger_entry = AsyncMock(side_effect=RuntimeError("DB down"))
        mock_sm = MagicMock()
        mock_sm.warm_backend = warm
        state_service._state_manager = mock_sm

        request = _base_request()
        response = await state_service.SaveLedgerEntry(request, mock_context)

        assert response.success is False
        assert response.error == "internal server error"
        mock_context.set_code.assert_called_with(grpc.StatusCode.INTERNAL)
        mock_context.set_details.assert_called_with("internal server error")

    @pytest.mark.asyncio
    async def test_duplicate_id_is_idempotent(self, state_service, mock_context):
        """Two calls with the same ``id`` both succeed -- retry safe."""
        warm = AsyncMock()
        warm.save_ledger_entry = AsyncMock()  # SQLite backend's own INSERT OR REPLACE
        mock_sm = MagicMock()
        mock_sm.warm_backend = warm
        state_service._state_manager = mock_sm

        request = _base_request()
        first = await state_service.SaveLedgerEntry(request, mock_context)
        second = await state_service.SaveLedgerEntry(request, mock_context)

        assert first.success is True
        assert second.success is True
        assert warm.save_ledger_entry.await_count == 2


class TestSaveLedgerEntryPostgresJsonbColumns:
    """VIB-3503 Part 2b: the 4 audit-grade replay JSONB columns are persisted
    to Postgres in deployed mode. Empty bytes from the wire bind to NULL so
    pre-VIB-3503 rows and rows where the SDK chose not to capture replay
    inputs both store NULL rather than the JSON-invalid empty string.

    These tests pin the positional argument layout of the INSERT — slots 20
    (extracted_data_json) through 23 (post_state_json) — so a future column
    re-order or insertion is caught immediately. VIB-4721/4722:
    ``transaction_ledger`` has a single identity column, ``deployment_id``
    (the legacy ``deployment_id`` column was DROPPED), so the slots shifted down
    by one from the legacy 24-column shape.
    """

    @pytest.fixture
    def pg_service(self, settings: GatewaySettings) -> StateServiceServicer:
        svc = StateServiceServicer(settings)
        svc._initialized = True
        svc._snapshot_pool_initialized = True
        svc._snapshot_pool = MagicMock()  # truthy => Postgres branch
        svc._snapshot_execute = AsyncMock(return_value="INSERT 0 1")
        svc._ensure_snapshot_pool = AsyncMock()
        return svc

    @pytest.mark.asyncio
    async def test_all_four_jsonb_fields_populated(self, pg_service, mock_context):
        """All 4 JSON fields populated → bound as JSON strings at slots 20-23."""
        request = _base_request(
            extracted_data_json=b'{"foo": 1}',
            price_inputs_json=b'{"USDC": "1.00"}',
            pre_state_json=b'{"hf": "1.5"}',
            post_state_json=b'{"hf": "1.4"}',
        )

        response = await pg_service.SaveLedgerEntry(request, mock_context)

        assert response.success is True
        pg_service._snapshot_execute.assert_awaited_once()
        args = pg_service._snapshot_execute.call_args.args
        # args[0] is SQL, then 23 positional values
        assert args[20] == '{"foo": 1}'
        assert args[21] == '{"USDC": "1.00"}'
        assert args[22] == '{"hf": "1.5"}'
        assert args[23] == '{"hf": "1.4"}'

    @pytest.mark.asyncio
    async def test_all_four_jsonb_fields_empty_bind_none(self, pg_service, mock_context):
        """Empty bytes for each → bound as None so PG stores NULL (not '')."""
        request = _base_request()  # extracted_data_json defaults to b""; others default to b""

        response = await pg_service.SaveLedgerEntry(request, mock_context)

        assert response.success is True
        args = pg_service._snapshot_execute.call_args.args
        assert args[20] is None
        assert args[21] is None
        assert args[22] is None
        assert args[23] is None

    @pytest.mark.asyncio
    async def test_mixed_populated_and_empty_jsonb_fields(self, pg_service, mock_context):
        """Mixed: 2 populated, 2 empty → mix of JSON strings and None at slots 20-23."""
        request = _base_request(
            extracted_data_json=b'{"a": 1}',
            pre_state_json=b'{"b": 2}',
            # price_inputs_json and post_state_json default to b""
        )

        response = await pg_service.SaveLedgerEntry(request, mock_context)

        assert response.success is True
        args = pg_service._snapshot_execute.call_args.args
        assert args[20] == '{"a": 1}'
        assert args[21] is None
        assert args[22] == '{"b": 2}'
        assert args[23] is None

    @pytest.mark.asyncio
    async def test_insert_uses_jsonb_cast(self, pg_service, mock_context):
        """SQL string contains explicit ::jsonb cast on the 4 column placeholders."""
        request = _base_request(extracted_data_json=b'{"a": 1}')

        await pg_service.SaveLedgerEntry(request, mock_context)

        sql = pg_service._snapshot_execute.call_args.args[0]
        assert "$20::jsonb" in sql
        assert "$21::jsonb" in sql
        assert "$22::jsonb" in sql
        assert "$23::jsonb" in sql
        assert "extracted_data_json" in sql
        assert "price_inputs_json" in sql
        assert "pre_state_json" in sql
        assert "post_state_json" in sql
        assert "strategy_id" not in sql
        assert "agent_id" not in sql

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "field",
        ["extracted_data_json", "price_inputs_json", "pre_state_json", "post_state_json"],
    )
    async def test_malformed_json_rejected_with_invalid_argument(self, pg_service, mock_context, field):
        """Non-JSON bytes in any of the 4 replay fields must be rejected at the
        gateway boundary with INVALID_ARGUMENT, BEFORE the PG INSERT runs.

        Without this check the PG ::jsonb cast would surface as INTERNAL while
        the SQLite path would silently persist the raw string -- a cross-backend
        divergence the gateway boundary is responsible for closing.
        """
        request = _base_request(**{field: b"not json at all {"})

        response = await pg_service.SaveLedgerEntry(request, mock_context)

        assert response.success is False
        assert "valid JSON" in response.error
        mock_context.set_code.assert_called_with(grpc.StatusCode.INVALID_ARGUMENT)
        # Validation runs before the INSERT -- no PG round trip on bad input.
        pg_service._snapshot_execute.assert_not_called()
