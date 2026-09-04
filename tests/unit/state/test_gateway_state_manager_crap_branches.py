"""Branch-complete contracts for GatewayStateManager state and accounting writes."""

from __future__ import annotations

import json
import logging
from datetime import UTC, datetime
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import MagicMock

import grpc
import pytest

from almanak.framework.accounting.models import (
    AccountingConfidence,
    AccountingIdentity,
    LendingAccountingEvent,
    LendingEventType,
    PendleAccountingEvent,
    PendleEventType,
)
from almanak.framework.accounting.writer import augment_accounting_payload
from almanak.framework.models.run_mode import RunMode
from almanak.framework.state.exceptions import AccountingPersistenceError
from almanak.framework.state.gateway_state_manager import GatewayStateManager
from almanak.framework.state.state_manager import StateConflictError, StateData

_NOW = datetime(2026, 9, 3, 12, 34, 56, tzinfo=UTC)


class _RpcError(grpc.RpcError):
    def __init__(self, status_code: grpc.StatusCode) -> None:
        self._status_code = status_code

    def code(self) -> grpc.StatusCode:
        return self._status_code


class _NoCodeRpcError(grpc.RpcError):
    pass


def _manager(**rpc_methods: MagicMock) -> tuple[GatewayStateManager, SimpleNamespace]:
    state_service = SimpleNamespace(**rpc_methods)
    return GatewayStateManager(SimpleNamespace(state=state_service), timeout=7.5), state_service


def _state() -> StateData:
    return StateData(
        deployment_id="deployment:state-identity",
        version=4,
        state={"z_decimal": Decimal("1.2300"), "a_nested": {"enabled": True}},
        schema_version=3,
        created_at=_NOW,
    )


def _identity(mode: RunMode) -> AccountingIdentity:
    return AccountingIdentity(
        id="accounting-event-id",
        deployment_id="deployment:accounting-identity",
        cycle_id="cycle-id",
        execution_mode=mode,
        timestamp=_NOW,
        chain="arbitrum",
        protocol="aave_v3",
        wallet_address="0x0000000000000000000000000000000000000001",
        tx_hash="0xtransaction",
        ledger_entry_id="ledger-entry-id",
    )


def _lending_event(mode: RunMode = RunMode.PAPER) -> LendingAccountingEvent:
    return LendingAccountingEvent(
        identity=_identity(mode),
        event_type=LendingEventType.SUPPLY,
        position_key="lending:arbitrum:aave_v3:wallet:USDC",
        market_id="aave_v3:USDC",
        asset="USDC",
        collateral_value_before_usd=None,
        collateral_value_after_usd=Decimal("0"),
        debt_value_before_usd=None,
        debt_value_after_usd=Decimal("0"),
        net_equity_before_usd=None,
        net_equity_after_usd=Decimal("0"),
        health_factor_before=None,
        health_factor_after=Decimal("0"),
        liquidation_threshold=None,
        lltv=Decimal("0"),
        supply_apr_bps=0,
        borrow_apr_bps=None,
        principal_delta_usd=Decimal("0"),
        interest_delta_usd=None,
        gas_usd=Decimal("0"),
        amount_token=None,
        confidence=AccountingConfidence.HIGH,
        schema_version=1,
        primitive_version=1,
    )


def _pendle_event(mode: RunMode = RunMode.PAPER) -> PendleAccountingEvent:
    return PendleAccountingEvent(
        identity=_identity(mode),
        event_type=PendleEventType.PT_BUY,
        position_key="pendle:arbitrum:market:wallet:PT",
        market_id="pendle-market",
        pt_token="PT",
        maturity_timestamp=None,
        pt_amount=Decimal("0"),
        sy_amount=None,
        pt_price=Decimal("0"),
        implied_apr_bps=0,
        days_to_maturity=0,
        realized_yield_usd=None,
        confidence=AccountingConfidence.ESTIMATED,
        schema_version=1,
        primitive_version=1,
    )


def _malformed_event(mode: RunMode, payload: str = "{") -> SimpleNamespace:
    return SimpleNamespace(
        identity=_identity(mode),
        event_type=LendingEventType.SUPPLY,
        position_key="position-key",
        confidence=AccountingConfidence.HIGH,
        schema_version=1,
        to_payload_json=MagicMock(return_value=payload),
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("expected_version", "response_checksum"),
    [(None, ""), (0, "checksum-zero"), (4, "checksum-cas")],
)
async def test_save_state_success_preserves_wire_and_return_contract(
    expected_version: int | None,
    response_checksum: str,
) -> None:
    save_rpc = MagicMock(
        return_value=SimpleNamespace(success=True, new_version=5, checksum=response_checksum, error="")
    )
    manager, _ = _manager(SaveState=save_rpc)
    state = _state()

    saved = await manager.save_state(state, expected_version=expected_version)

    request = save_rpc.call_args.args[0]
    assert save_rpc.call_args.kwargs == {"timeout": 7.5}
    assert request.deployment_id == state.deployment_id
    assert request.expected_version == (expected_version or 0)
    assert request.data == json.dumps(state.state, default=str, sort_keys=True).encode("utf-8")
    assert request.schema_version == state.schema_version
    assert saved.deployment_id == state.deployment_id
    assert saved.version == 5
    assert saved.state is state.state
    assert saved.schema_version == state.schema_version
    assert saved.created_at is state.created_at
    assert saved.checksum == (response_checksum or state.checksum)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("error_message", "expected_version"),
    [("version mismatch", None), ("write conflict", 4)],
)
async def test_save_state_response_conflict_preserves_cas_identity(
    error_message: str,
    expected_version: int | None,
) -> None:
    save_rpc = MagicMock(return_value=SimpleNamespace(success=False, error=error_message, new_version=9, checksum=""))
    manager, _ = _manager(SaveState=save_rpc)

    with pytest.raises(StateConflictError) as exc_info:
        await manager.save_state(_state(), expected_version=expected_version)

    assert exc_info.value.deployment_id == "deployment:state-identity"
    assert exc_info.value.expected_version == (expected_version or 0)
    assert exc_info.value.actual_version == 9


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("response_error", "expected_message"),
    [("disk full", "State save failed: disk full"), ("", "State save failed: Unknown save error")],
)
async def test_save_state_response_failure_raises_runtime_error(
    response_error: str,
    expected_message: str,
) -> None:
    save_rpc = MagicMock(return_value=SimpleNamespace(success=False, error=response_error, new_version=0, checksum=""))
    manager, _ = _manager(SaveState=save_rpc)

    with pytest.raises(RuntimeError, match=expected_message):
        await manager.save_state(_state())


@pytest.mark.asyncio
async def test_save_state_aborted_rpc_maps_to_conflict() -> None:
    rpc_error = _RpcError(grpc.StatusCode.ABORTED)
    manager, _ = _manager(SaveState=MagicMock(side_effect=rpc_error))

    with pytest.raises(StateConflictError) as exc_info:
        await manager.save_state(_state(), expected_version=4)

    assert exc_info.value.deployment_id == "deployment:state-identity"
    assert exc_info.value.expected_version == 4
    assert exc_info.value.actual_version == 0
    assert exc_info.value.__cause__ is rpc_error


@pytest.mark.asyncio
@pytest.mark.parametrize("rpc_error", [_RpcError(grpc.StatusCode.UNAVAILABLE), _NoCodeRpcError()])
async def test_save_state_non_conflict_rpc_error_propagates(rpc_error: grpc.RpcError) -> None:
    manager, _ = _manager(SaveState=MagicMock(side_effect=rpc_error))

    with pytest.raises(grpc.RpcError) as exc_info:
        await manager.save_state(_state())

    assert exc_info.value is rpc_error


@pytest.mark.asyncio
async def test_save_state_non_rpc_error_propagates() -> None:
    failure = ValueError("serialization transport failed")
    manager, _ = _manager(SaveState=MagicMock(side_effect=failure))

    with pytest.raises(ValueError) as exc_info:
        await manager.save_state(_state())

    assert exc_info.value is failure


@pytest.mark.asyncio
@pytest.mark.parametrize("event_factory", [_lending_event, _pendle_event])
async def test_save_accounting_event_success_preserves_typed_wire_payload(event_factory) -> None:
    save_rpc = MagicMock(return_value=SimpleNamespace(success=True, error=""))
    manager, _ = _manager(SaveAccountingEvent=save_rpc)
    event = event_factory()

    assert await manager.save_accounting_event(event) is True

    request = save_rpc.call_args.args[0]
    identity = event.identity
    expected_payload = augment_accounting_payload(
        event.to_payload_json(),
        is_live=False,
        registry_lookup=None,
    ).encode("utf-8")
    assert save_rpc.call_args.kwargs == {"timeout": 7.5}
    assert request.id == identity.id
    assert request.deployment_id == identity.deployment_id
    assert request.cycle_id == identity.cycle_id
    assert request.execution_mode == identity.execution_mode
    assert request.timestamp == int(identity.timestamp.timestamp())
    assert request.chain == identity.chain
    assert request.protocol == identity.protocol
    assert request.wallet_address == identity.wallet_address
    assert request.tx_hash == identity.tx_hash
    assert request.ledger_entry_id == identity.ledger_entry_id
    assert request.event_type == event.event_type
    assert request.position_key == event.position_key
    assert request.confidence == event.confidence
    assert request.schema_version == event.schema_version
    assert request.payload_json == expected_payload

    payload = json.loads(request.payload_json)
    assert payload["schema_version"] == event.schema_version
    assert type(payload["primitive_version"]) is int
    assert type(payload["formula_version"]) is int
    assert type(payload["matching_policy_version"]) is int
    if isinstance(event, LendingAccountingEvent):
        assert payload["collateral_value_before_usd"] is None
        assert payload["collateral_value_after_usd"] == "0"
        assert payload["amount_token"] is None
        assert payload["principal_delta_usd"] == "0"
    else:
        assert payload["pt_amount"] == "0"
        assert payload["sy_amount"] is None
        assert payload["realized_yield_usd"] is None


@pytest.mark.asyncio
async def test_save_accounting_event_non_live_malformed_payload_passes_through() -> None:
    save_rpc = MagicMock(return_value=SimpleNamespace(success=True, error=""))
    manager, _ = _manager(SaveAccountingEvent=save_rpc)
    event = _malformed_event(RunMode.PAPER, payload="[")

    assert await manager.save_accounting_event(event) is True
    assert save_rpc.call_args.args[0].payload_json == b"["


@pytest.mark.asyncio
async def test_save_accounting_event_live_malformed_payload_propagates_typed_error() -> None:
    save_rpc = MagicMock()
    manager, _ = _manager(SaveAccountingEvent=save_rpc)

    with pytest.raises(AccountingPersistenceError) as exc_info:
        await manager.save_accounting_event(_malformed_event(RunMode.LIVE))

    assert exc_info.value.write_kind == "accounting"
    save_rpc.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize(("mode", "raises"), [(RunMode.PAPER, False), (RunMode.LIVE, True)])
async def test_save_accounting_event_response_failure_is_mode_aware(mode: RunMode, raises: bool) -> None:
    save_rpc = MagicMock(return_value=SimpleNamespace(success=False, error="database unavailable"))
    manager, _ = _manager(SaveAccountingEvent=save_rpc)

    if raises:
        with pytest.raises(AccountingPersistenceError) as exc_info:
            await manager.save_accounting_event(_lending_event(mode))
        assert exc_info.value.deployment_id == "deployment:accounting-identity"
        assert exc_info.value.write_kind == "accounting"
    else:
        assert await manager.save_accounting_event(_lending_event(mode)) is False


@pytest.mark.asyncio
@pytest.mark.parametrize(("mode", "raises"), [(RunMode.DRY_RUN, False), (RunMode.LIVE, True)])
async def test_save_accounting_event_exception_is_mode_aware(mode: RunMode, raises: bool) -> None:
    failure = RuntimeError("gateway unavailable")
    manager, _ = _manager(SaveAccountingEvent=MagicMock(side_effect=failure))

    if raises:
        with pytest.raises(AccountingPersistenceError) as exc_info:
            await manager.save_accounting_event(_lending_event(mode))
        assert exc_info.value.deployment_id == "deployment:accounting-identity"
        assert exc_info.value.write_kind == "accounting"
        assert exc_info.value.__cause__ is failure
        assert exc_info.value.cause is failure
    else:
        assert await manager.save_accounting_event(_lending_event(mode)) is False


@pytest.mark.asyncio
@pytest.mark.parametrize("failure_kind", ["response", "exception"])
async def test_save_accounting_event_non_live_failure_logs_error(caplog, failure_kind: str) -> None:
    if failure_kind == "response":
        save_rpc = MagicMock(return_value=SimpleNamespace(success=False, error="database unavailable"))
    else:
        save_rpc = MagicMock(side_effect=RuntimeError("gateway unavailable"))
    manager, _ = _manager(SaveAccountingEvent=save_rpc)

    with caplog.at_level(logging.ERROR, logger="almanak.framework.state.gateway_state_manager"):
        assert await manager.save_accounting_event(_lending_event()) is False

    assert any(record.levelno == logging.ERROR for record in caplog.records)
