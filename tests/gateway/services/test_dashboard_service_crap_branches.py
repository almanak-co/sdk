"""Branch-complete tests for DashboardService CRAP allowlist removal."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import grpc
import pytest

from almanak.gateway.core.settings import GatewaySettings
from almanak.gateway.proto import gateway_pb2
from almanak.gateway.services.dashboard_service import DashboardServiceServicer


@pytest.fixture
def service() -> DashboardServiceServicer:
    return DashboardServiceServicer(GatewaySettings())


@pytest.fixture
def context() -> MagicMock:
    return MagicMock(spec=grpc.aio.ServicerContext)


class TestUnixToDatetime:
    @pytest.mark.parametrize("timestamp", [0, -1])
    def test_non_positive_timestamp_is_an_open_bound(self, timestamp):
        assert DashboardServiceServicer._unix_to_dt(timestamp) is None

    def test_positive_timestamp_is_converted_to_utc(self):
        assert DashboardServiceServicer._unix_to_dt(1) == datetime(1970, 1, 1, 0, 0, 1, tzinfo=UTC)

    def test_out_of_range_timestamp_is_normalized_to_value_error(self):
        timestamp = 9_223_372_036_854_775_807

        with pytest.raises(ValueError, match=f"^timestamp out of range: {timestamp}$"):
            DashboardServiceServicer._unix_to_dt(timestamp)


class TestRegisterStrategyInstance:
    @pytest.mark.asyncio
    async def test_rejects_invalid_deployment_id_before_registry_access(self, service, context):
        with patch("almanak.gateway.services.dashboard_service.get_instance_registry") as registry_factory:
            response = await service.RegisterStrategyInstance(gateway_pb2.RegisterInstanceRequest(), context)

        assert response.success is False
        assert response.error
        registry_factory.assert_not_called()

    @pytest.mark.asyncio
    async def test_registers_all_wire_fields_and_derives_protocol_from_config(self, service, context):
        registry = MagicMock()
        registry.get.return_value = None
        request = gateway_pb2.RegisterInstanceRequest(
            deployment_id="deployment:abc123",
            strategy_name="yield_strategy",
            template_name="YieldStrategy",
            chain="arbitrum",
            wallet_address="0xabc",
            config_json='{"protocol": "Aave V3"}',
            version="2.27.0",
            chains=["arbitrum", "base"],
            chain_wallets={"arbitrum": "0xabc", "base": "0xdef"},
        )

        with patch("almanak.gateway.services.dashboard_service.get_instance_registry", return_value=registry):
            response = await service.RegisterStrategyInstance(request, context)

        assert response.success is True
        assert response.already_existed is False
        registry.get.assert_called_once_with("deployment:abc123")
        instance = registry.register.call_args.args[0]
        assert instance.deployment_id == "deployment:abc123"
        assert instance.strategy_name == "yield_strategy"
        assert instance.template_name == "YieldStrategy"
        assert instance.chain == "arbitrum"
        assert instance.protocol == "Aave V3"
        assert instance.wallet_address == "0xabc"
        assert instance.config_json == '{"protocol": "Aave V3"}'
        assert instance.chains == "arbitrum,base"
        assert json.loads(instance.chain_wallets) == {"arbitrum": "0xabc", "base": "0xdef"}
        assert instance.status == "RUNNING"
        assert instance.archived is False
        assert instance.created_at == instance.updated_at == instance.last_heartbeat_at
        assert instance.created_at.tzinfo is UTC
        assert instance.version == "2.27.0"

    @pytest.mark.asyncio
    async def test_reregistration_preserves_creation_and_archive_metadata(self, service, context):
        created_at = datetime(2025, 1, 2, 3, 4, tzinfo=UTC)
        existing = SimpleNamespace(archived=True, created_at=created_at)
        registry = MagicMock()
        registry.get.return_value = existing
        request = gateway_pb2.RegisterInstanceRequest(
            deployment_id="platform-agent-1234",
            chain="ethereum",
            protocol="Uniswap V3",
        )

        with patch("almanak.gateway.services.dashboard_service.get_instance_registry", return_value=registry):
            response = await service.RegisterStrategyInstance(request, context)

        assert response.success is True
        assert response.already_existed is True
        instance = registry.register.call_args.args[0]
        assert instance.deployment_id == "platform-agent-1234"
        assert instance.strategy_name == "platform-agent-1234"
        assert instance.protocol == "Uniswap V3"
        assert instance.chains == "ethereum"
        assert instance.chain_wallets == ""
        assert instance.archived is True
        assert instance.created_at is created_at
        assert instance.updated_at == instance.last_heartbeat_at
        assert instance.updated_at > created_at

    @pytest.mark.asyncio
    @pytest.mark.parametrize("config_json", ["", "{"])
    async def test_unknown_derived_protocol_is_empty(self, service, context, config_json):
        registry = MagicMock()
        registry.get.return_value = None
        request = gateway_pb2.RegisterInstanceRequest(
            deployment_id="deployment:abc123",
            strategy_name="custom_strategy",
            chain="base",
            config_json=config_json,
        )

        with patch("almanak.gateway.services.dashboard_service.get_instance_registry", return_value=registry):
            response = await service.RegisterStrategyInstance(request, context)

        assert response.success is True
        assert registry.register.call_args.args[0].protocol == ""

    @pytest.mark.asyncio
    async def test_registry_failure_returns_error_without_raising(self, service, context):
        registry = MagicMock()
        registry.get.side_effect = RuntimeError("registry unavailable")

        with patch("almanak.gateway.services.dashboard_service.get_instance_registry", return_value=registry):
            response = await service.RegisterStrategyInstance(
                gateway_pb2.RegisterInstanceRequest(deployment_id="deployment:abc123"),
                context,
            )

        assert response.success is False
        assert response.error == "registry unavailable"


def _ledger_entry(entry_id: str, timestamp: datetime, **overrides) -> SimpleNamespace:
    values = {
        "id": entry_id,
        "cycle_id": f"cycle-{entry_id}",
        "deployment_id": "deployment:abc123",
        "timestamp": timestamp,
        "intent_type": "SWAP",
        "token_in": "WETH",
        "amount_in": "1.5",
        "token_out": "USDC",
        "amount_out": "3000",
        "effective_price": "2000",
        "slippage_bps": 2.5,
        "gas_used": 21000,
        "gas_usd": "0.5",
        "tx_hash": f"0x{entry_id}",
        "chain": "arbitrum",
        "protocol": "uniswap_v3",
        "success": True,
        "error": "",
        "extracted_data_json": '{"sub_transactions": []}',
    }
    values.update(overrides)
    return SimpleNamespace(**values)


class TestGetTransactionLedger:
    @pytest.mark.asyncio
    async def test_rejects_invalid_deployment_id_before_initialization(self, service, context):
        service._ensure_initialized = AsyncMock()

        response = await service.GetTransactionLedger(gateway_pb2.GetTransactionLedgerRequest(), context)

        assert list(response.entries) == []
        assert response.has_more is False
        context.set_code.assert_called_once_with(grpc.StatusCode.INVALID_ARGUMENT)
        service._ensure_initialized.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_rejects_out_of_range_timestamp_before_backend_access(self, service, context):
        service._initialized = True
        state_manager = MagicMock()
        state_manager.get_ledger_entries = AsyncMock()
        service._state_manager = state_manager
        request = gateway_pb2.GetTransactionLedgerRequest(
            deployment_id="deployment:abc123",
            since_timestamp=9_223_372_036_854_775_807,
        )

        response = await service.GetTransactionLedger(request, context)

        assert list(response.entries) == []
        context.set_code.assert_called_once_with(grpc.StatusCode.INVALID_ARGUMENT)
        assert "timestamp out of range" in context.set_details.call_args.args[0]
        state_manager.get_ledger_entries.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_missing_state_manager_returns_empty_page(self, service, context):
        service._initialized = True
        service._state_manager = None

        response = await service.GetTransactionLedger(
            gateway_pb2.GetTransactionLedgerRequest(deployment_id="deployment:abc123"),
            context,
        )

        assert list(response.entries) == []
        assert response.has_more is False
        context.set_code.assert_not_called()

    @pytest.mark.asyncio
    async def test_backend_failure_preserves_legacy_empty_ok_response(self, service, context):
        service._initialized = True
        state_manager = MagicMock()
        state_manager.get_ledger_entries = AsyncMock(side_effect=RuntimeError("postgres unavailable"))
        service._state_manager = state_manager

        response = await service.GetTransactionLedger(
            gateway_pb2.GetTransactionLedgerRequest(deployment_id="deployment:abc123"),
            context,
        )

        state_manager.get_ledger_entries.assert_awaited_once_with(
            "deployment:abc123",
            since=None,
            intent_type=None,
            limit=101,
        )
        assert list(response.entries) == []
        assert response.has_more is False
        context.set_code.assert_not_called()
        context.set_details.assert_not_called()

    @pytest.mark.asyncio
    async def test_filters_paginates_in_backend_order_and_maps_every_proto_field(self, service, context):
        service._initialized = True
        newest = datetime(2026, 1, 3, 12, tzinfo=UTC)
        middle = datetime(2026, 1, 2, 12, tzinfo=UTC)
        oldest = datetime(2026, 1, 1, 12, tzinfo=UTC)
        entries = [
            _ledger_entry("newest", newest),
            _ledger_entry(
                "middle",
                middle,
                slippage_bps=None,
                gas_used=0,
                success=False,
                error="reverted",
            ),
            _ledger_entry("oldest", oldest),
        ]
        state_manager = MagicMock()
        state_manager.get_ledger_entries = AsyncMock(return_value=entries)
        service._state_manager = state_manager
        since_epoch = int(oldest.timestamp())
        request = gateway_pb2.GetTransactionLedgerRequest(
            deployment_id="deployment:abc123",
            since_timestamp=since_epoch,
            intent_type_filter="SWAP",
            limit=2,
        )

        response = await service.GetTransactionLedger(request, context)

        state_manager.get_ledger_entries.assert_awaited_once_with(
            "deployment:abc123",
            since=datetime.fromtimestamp(since_epoch, tz=UTC),
            intent_type="SWAP",
            limit=3,
        )
        assert response.has_more is True
        assert [entry.id for entry in response.entries] == ["newest", "middle"]
        first = response.entries[0]
        assert first.cycle_id == "cycle-newest"
        assert first.deployment_id == "deployment:abc123"
        assert first.timestamp == int(newest.timestamp())
        assert first.intent_type == "SWAP"
        assert first.token_in == "WETH"
        assert first.amount_in == "1.5"
        assert first.token_out == "USDC"
        assert first.amount_out == "3000"
        assert first.effective_price == "2000"
        assert first.slippage_bps == 2.5
        assert first.gas_used == 21000
        assert first.gas_usd == "0.5"
        assert first.tx_hash == "0xnewest"
        assert first.chain == "arbitrum"
        assert first.protocol == "uniswap_v3"
        assert first.success is True
        assert first.error == ""
        assert first.extracted_data_json == '{"sub_transactions": []}'
        assert response.entries[1].slippage_bps == 0.0
        assert response.entries[1].success is False
        assert response.entries[1].error == "reverted"


class TestPurgeStrategyInstance:
    @pytest.mark.asyncio
    async def test_rejects_invalid_deployment_id_before_registry_access(self, service, context):
        with patch("almanak.gateway.services.dashboard_service.get_instance_registry") as registry_factory:
            response = await service.PurgeStrategyInstance(gateway_pb2.PurgeInstanceRequest(reason="cleanup"), context)

        assert response.success is False
        assert response.error
        registry_factory.assert_not_called()

    @pytest.mark.asyncio
    async def test_requires_audit_reason_before_registry_access(self, service, context):
        with patch("almanak.gateway.services.dashboard_service.get_instance_registry") as registry_factory:
            response = await service.PurgeStrategyInstance(
                gateway_pb2.PurgeInstanceRequest(deployment_id="deployment:abc123"),
                context,
            )

        assert response.success is False
        assert response.error == "Reason is required for audit when purging"
        registry_factory.assert_not_called()

    @pytest.mark.asyncio
    async def test_not_found_preserves_cached_data_and_does_not_clear_timeline(self, service, context):
        service._cached_positions = {"deployment:abc123": [MagicMock()]}
        registry = MagicMock()
        registry.purge_with_events.return_value = False
        timeline_store = MagicMock()

        with (
            patch("almanak.gateway.services.dashboard_service.get_instance_registry", return_value=registry),
            patch("almanak.gateway.services.dashboard_service.get_timeline_store", return_value=timeline_store),
        ):
            response = await service.PurgeStrategyInstance(
                gateway_pb2.PurgeInstanceRequest(deployment_id="deployment:abc123", reason="cleanup"),
                context,
            )

        assert response.success is False
        assert response.error == "Instance not found: deployment:abc123"
        registry.purge_with_events.assert_called_once_with("deployment:abc123")
        timeline_store.clear_events.assert_not_called()
        assert "deployment:abc123" in service._cached_positions

    @pytest.mark.asyncio
    async def test_success_clears_only_requested_deployment_and_retry_is_not_found(self, service, context):
        service._cached_positions = {
            "deployment:abc123": [MagicMock()],
            "deployment:other456": [MagicMock()],
        }
        registry = MagicMock()
        registry.purge_with_events.side_effect = [True, False]
        timeline_store = MagicMock()
        request = gateway_pb2.PurgeInstanceRequest(deployment_id="deployment:abc123", reason="operator cleanup")

        with (
            patch("almanak.gateway.services.dashboard_service.get_instance_registry", return_value=registry),
            patch("almanak.gateway.services.dashboard_service.get_timeline_store", return_value=timeline_store),
        ):
            first = await service.PurgeStrategyInstance(request, context)
            second = await service.PurgeStrategyInstance(request, context)

        assert first.success is True
        assert second.success is False
        assert second.error == "Instance not found: deployment:abc123"
        assert [item.args for item in registry.purge_with_events.call_args_list] == [
            ("deployment:abc123",),
            ("deployment:abc123",),
        ]
        timeline_store.clear_events.assert_called_once_with("deployment:abc123")
        assert "deployment:abc123" not in service._cached_positions
        assert "deployment:other456" in service._cached_positions

    @pytest.mark.asyncio
    async def test_timeline_clear_failure_is_nonfatal(self, service, context):
        service._cached_positions = {"deployment:abc123": [MagicMock()]}
        registry = MagicMock()
        registry.purge_with_events.return_value = True
        timeline_store = MagicMock()
        timeline_store.clear_events.side_effect = RuntimeError("postgres unavailable")

        with (
            patch("almanak.gateway.services.dashboard_service.get_instance_registry", return_value=registry),
            patch("almanak.gateway.services.dashboard_service.get_timeline_store", return_value=timeline_store),
        ):
            response = await service.PurgeStrategyInstance(
                gateway_pb2.PurgeInstanceRequest(deployment_id="deployment:abc123", reason="cleanup"),
                context,
            )

        assert response.success is True
        assert "deployment:abc123" not in service._cached_positions

    @pytest.mark.asyncio
    async def test_registry_failure_returns_error_without_clearing_timeline(self, service, context):
        registry = MagicMock()
        registry.purge_with_events.side_effect = RuntimeError("registry unavailable")
        timeline_store = MagicMock()

        with (
            patch("almanak.gateway.services.dashboard_service.get_instance_registry", return_value=registry),
            patch("almanak.gateway.services.dashboard_service.get_timeline_store", return_value=timeline_store),
        ):
            response = await service.PurgeStrategyInstance(
                gateway_pb2.PurgeInstanceRequest(deployment_id="deployment:abc123", reason="cleanup"),
                context,
            )

        assert response.success is False
        assert response.error == "registry unavailable"
        timeline_store.clear_events.assert_not_called()
