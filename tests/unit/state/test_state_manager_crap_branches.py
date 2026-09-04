"""Branch-complete contracts for local StateManager persistence facades."""

from __future__ import annotations

import inspect
import json
from datetime import UTC, datetime
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from almanak.framework.portfolio.models import PortfolioSnapshot, TokenBalance, ValueConfidence
from almanak.framework.state.exceptions import AccountingPersistenceError
from almanak.framework.state.gateway_state_manager import GatewayStateManager
from almanak.framework.state.state_manager import StateManager, StateManagerConfig, StateTier

_DEPLOYMENT_ID = "deployment:crap-branches"
_NOW = datetime(2026, 9, 4, 12, 34, 56, tzinfo=UTC)
_OMITTED = object()


def _manager(warm_backend: object | None, *, initialized: bool = True) -> StateManager:
    manager = StateManager(StateManagerConfig(enable_hot=False, enable_warm=False))
    manager._warm = warm_backend  # type: ignore[assignment]
    manager._initialized = initialized
    return manager


def _snapshot() -> PortfolioSnapshot:
    return PortfolioSnapshot(
        timestamp=_NOW,
        deployment_id=_DEPLOYMENT_ID,
        total_value_usd=Decimal("100.25"),
        available_cash_usd=Decimal("20.50"),
        deployed_capital_usd=Decimal("79.75"),
        wallet_total_value_usd=Decimal("101.00"),
        value_confidence=ValueConfidence.HIGH,
        chain="arbitrum",
        iteration_number=7,
        cycle_id="cycle-7",
        execution_mode="paper",
    )


def _assert_metric(manager: StateManager, operation: str, *, success: bool, error: str | None = None) -> None:
    metric = manager.get_metrics(limit=1)[0]
    assert metric.tier is StateTier.WARM
    assert metric.operation == operation
    assert metric.success is success
    assert metric.error == error
    assert metric.latency_ms >= 0


@pytest.mark.asyncio
async def test_get_clob_order_delegates_after_lazy_initialization() -> None:
    order = object()
    get_order = AsyncMock(return_value=order)
    manager = _manager(SimpleNamespace(get_clob_order=get_order), initialized=False)

    result = await manager.get_clob_order("order-1", deployment_id=_DEPLOYMENT_ID)

    assert result is order
    assert manager.is_initialized
    get_order.assert_awaited_once_with("order-1", deployment_id=_DEPLOYMENT_ID)
    _assert_metric(manager, "get_clob_order", success=True)


@pytest.mark.asyncio
async def test_get_open_clob_orders_delegates_after_lazy_initialization() -> None:
    orders = [object(), object()]
    get_orders = AsyncMock(return_value=orders)
    manager = _manager(SimpleNamespace(get_open_clob_orders=get_orders), initialized=False)

    result = await manager.get_open_clob_orders(deployment_id=_DEPLOYMENT_ID, market_id="ETH-USD")

    assert result is orders
    get_orders.assert_awaited_once_with("ETH-USD", deployment_id=_DEPLOYMENT_ID)
    _assert_metric(manager, "get_open_clob_orders", success=True)


@pytest.mark.asyncio
async def test_update_clob_order_status_delegates_after_lazy_initialization() -> None:
    update_status = AsyncMock(return_value=True)
    manager = _manager(SimpleNamespace(update_clob_order_status=update_status), initialized=False)

    result = await manager.update_clob_order_status(
        "order-1",
        "FILLED",
        deployment_id=_DEPLOYMENT_ID,
        error="exchange acknowledgement delayed",
    )

    assert result is True
    update_status.assert_awaited_once_with(
        order_id="order-1",
        status="FILLED",
        fills=None,
        filled_size=None,
        average_fill_price=None,
        deployment_id=_DEPLOYMENT_ID,
        error="exchange acknowledgement delayed",
    )
    _assert_metric(manager, "update_clob_order_status", success=True)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("method_name", "args", "kwargs"),
    [
        ("get_clob_order", ("order-1",), {"deployment_id": ""}),
        ("get_open_clob_orders", (), {"deployment_id": ""}),
        ("update_clob_order_status", ("order-1", "OPEN"), {"deployment_id": ""}),
    ],
)
async def test_clob_facades_reject_missing_deployment_identity(method_name: str, args: tuple, kwargs: dict) -> None:
    manager = _manager(None)

    with pytest.raises(ValueError, match="deployment_id is required"):
        await getattr(manager, method_name)(*args, **kwargs)


@pytest.mark.asyncio
@pytest.mark.parametrize("warm_backend", [None, SimpleNamespace()])
@pytest.mark.parametrize(
    ("method_name", "args", "kwargs", "expected"),
    [
        ("get_clob_order", ("order-1",), {"deployment_id": _DEPLOYMENT_ID}, None),
        ("get_open_clob_orders", (), {"deployment_id": _DEPLOYMENT_ID}, []),
        (
            "update_clob_order_status",
            ("order-1", "OPEN"),
            {"deployment_id": _DEPLOYMENT_ID},
            False,
        ),
    ],
)
async def test_clob_facades_return_soft_read_defaults_without_backend_capability(
    warm_backend: object | None,
    method_name: str,
    args: tuple,
    kwargs: dict,
    expected: object,
) -> None:
    manager = _manager(warm_backend)

    result = await getattr(manager, method_name)(*args, **kwargs)

    assert result == expected
    assert manager.get_metrics() == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("method_name", "args", "kwargs"),
    [
        ("get_clob_order", ("order-1",), {"deployment_id": _DEPLOYMENT_ID}),
        ("get_open_clob_orders", (), {"deployment_id": _DEPLOYMENT_ID, "market_id": None}),
        (
            "update_clob_order_status",
            ("order-1", "FAILED"),
            {"deployment_id": _DEPLOYMENT_ID, "error": None},
        ),
    ],
)
async def test_clob_facades_record_backend_errors_and_return_soft_defaults(
    method_name: str,
    args: tuple,
    kwargs: dict,
) -> None:
    failure = RuntimeError(f"{method_name} failed")
    backend_method = AsyncMock(side_effect=failure)
    manager = _manager(SimpleNamespace(**{method_name: backend_method}))

    result = await getattr(manager, method_name)(*args, **kwargs)

    expected = (
        False if method_name == "update_clob_order_status" else ([] if method_name == "get_open_clob_orders" else None)
    )
    assert result == expected
    _assert_metric(manager, method_name, success=False, error=str(failure))


@pytest.mark.asyncio
async def test_save_portfolio_snapshot_delegates_without_mutating_identity() -> None:
    save_snapshot = AsyncMock(return_value=41)
    manager = _manager(SimpleNamespace(save_portfolio_snapshot=save_snapshot), initialized=False)
    snapshot = _snapshot()

    result = await manager.save_portfolio_snapshot(snapshot)

    assert result == 41
    assert snapshot.cycle_id == "cycle-7"
    assert snapshot.execution_mode == "paper"
    save_snapshot.assert_awaited_once_with(snapshot)
    _assert_metric(manager, "save_portfolio_snapshot", success=True)


@pytest.mark.asyncio
async def test_save_portfolio_snapshot_fails_closed_without_warm_backend() -> None:
    snapshot = _snapshot()
    manager = _manager(None)

    with pytest.raises(AccountingPersistenceError, match="No WARM backend configured") as exc_info:
        await manager.save_portfolio_snapshot(snapshot)
    assert exc_info.value.write_kind == "snapshot"
    assert exc_info.value.deployment_id == _DEPLOYMENT_ID


@pytest.mark.asyncio
async def test_save_portfolio_snapshot_requires_backend_capability() -> None:
    manager = _manager(SimpleNamespace())

    with pytest.raises(AccountingPersistenceError, match="does not support portfolio snapshot storage") as exc_info:
        await manager.save_portfolio_snapshot(_snapshot())
    assert exc_info.value.write_kind == "snapshot"
    assert exc_info.value.deployment_id == _DEPLOYMENT_ID


@pytest.mark.asyncio
async def test_save_portfolio_snapshot_wraps_backend_error() -> None:
    failure = RuntimeError("snapshot database unavailable")
    manager = _manager(SimpleNamespace(save_portfolio_snapshot=AsyncMock(side_effect=failure)))
    snapshot = _snapshot()

    with pytest.raises(AccountingPersistenceError) as exc_info:
        await manager.save_portfolio_snapshot(snapshot)

    assert exc_info.value.write_kind == "snapshot"
    assert exc_info.value.deployment_id == _DEPLOYMENT_ID
    assert exc_info.value.cause is failure
    assert exc_info.value.__cause__ is failure
    _assert_metric(manager, "save_portfolio_snapshot", success=False, error=str(failure))


@pytest.mark.asyncio
async def test_save_portfolio_snapshot_preserves_typed_backend_error() -> None:
    failure = AccountingPersistenceError("snapshot", deployment_id=_DEPLOYMENT_ID, message="write rejected")
    manager = _manager(SimpleNamespace(save_portfolio_snapshot=AsyncMock(side_effect=failure)))

    with pytest.raises(AccountingPersistenceError) as exc_info:
        await manager.save_portfolio_snapshot(_snapshot())

    assert exc_info.value is failure
    _assert_metric(manager, "save_portfolio_snapshot", success=False, error="AccountingPersistenceError")


class _LedgerBackend:
    def __init__(self, result: list[object] | None = None, error: Exception | None = None) -> None:
        self.result = result or []
        self.error = error
        self.calls: list[dict[str, object]] = []

    async def get_ledger_entries(
        self,
        deployment_id: str,
        since: datetime | None = None,
        intent_type: str | None = None,
        limit: int = 100,
        *,
        before: datetime | object = _OMITTED,
    ) -> list[object]:
        self.calls.append(
            {
                "deployment_id": deployment_id,
                "since": since,
                "intent_type": intent_type,
                "before": before,
                "limit": limit,
            }
        )
        if self.error is not None:
            raise self.error
        return self.result


class _LegacyLedgerBackend:
    def __init__(self) -> None:
        self.called = False

    async def get_ledger_entries(
        self,
        deployment_id: str,
        since: datetime | None = None,
        intent_type: str | None = None,
        limit: int = 100,
    ) -> list[object]:
        self.called = True
        return []


class _UninspectableLedgerCallable:
    __signature__ = "invalid"

    def __init__(self) -> None:
        self.called = False

    async def __call__(self, *args: object, **kwargs: object) -> list[object]:
        self.called = True
        return []


@pytest.mark.asyncio
async def test_get_ledger_entries_forwards_all_filters_and_before_cursor() -> None:
    entries = [object(), object()]
    backend = _LedgerBackend(entries)
    manager = _manager(backend, initialized=False)
    since = datetime(2026, 9, 1, tzinfo=UTC)
    before = datetime(2026, 9, 2, tzinfo=UTC)

    result = await manager.get_ledger_entries(
        _DEPLOYMENT_ID,
        since=since,
        intent_type="SWAP",
        before=before,
        limit=25,
    )

    assert result is entries
    assert backend.calls == [
        {
            "deployment_id": _DEPLOYMENT_ID,
            "since": since,
            "intent_type": "SWAP",
            "before": before,
            "limit": 25,
        }
    ]
    _assert_metric(manager, "get_ledger_entries", success=True)


@pytest.mark.asyncio
async def test_get_ledger_entries_omits_unrequested_before_cursor() -> None:
    backend = _LedgerBackend()
    manager = _manager(backend)

    assert await manager.get_ledger_entries(_DEPLOYMENT_ID) == []

    assert backend.calls[0]["before"] is _OMITTED
    _assert_metric(manager, "get_ledger_entries", success=True)


@pytest.mark.asyncio
async def test_get_ledger_entries_returns_empty_without_warm_backend() -> None:
    manager = _manager(None)

    assert await manager.get_ledger_entries(_DEPLOYMENT_ID) == []
    assert manager.get_metrics() == []


@pytest.mark.asyncio
async def test_get_ledger_entries_requires_backend_capability() -> None:
    manager = _manager(SimpleNamespace())

    assert await manager.get_ledger_entries(_DEPLOYMENT_ID) == []
    assert manager.get_metrics() == []


@pytest.mark.asyncio
async def test_get_ledger_entries_refuses_to_drop_before_cursor() -> None:
    backend = _LegacyLedgerBackend()
    manager = _manager(backend)
    before = datetime(2026, 9, 2, tzinfo=UTC)

    assert "before" not in inspect.signature(backend.get_ledger_entries).parameters
    assert await manager.get_ledger_entries(_DEPLOYMENT_ID, before=before) == []
    assert backend.called is False
    _assert_metric(
        manager,
        "get_ledger_entries",
        success=False,
        error=(
            "Warm backend get_ledger_entries() does not support the 'before' pagination cursor; "
            "refusing to fall back to an uncursored read which would produce duplicate or looping trade-tape pages."
        ),
    )


@pytest.mark.asyncio
async def test_get_ledger_entries_rejects_uninspectable_cursor_backend() -> None:
    get_entries = _UninspectableLedgerCallable()
    manager = _manager(SimpleNamespace(get_ledger_entries=get_entries))

    assert await manager.get_ledger_entries(_DEPLOYMENT_ID, before=_NOW) == []
    assert get_entries.called is False
    assert "does not support the 'before' pagination cursor" in (manager.get_metrics(limit=1)[0].error or "")


@pytest.mark.asyncio
async def test_get_ledger_entries_records_backend_error_and_returns_empty() -> None:
    failure = RuntimeError("ledger database unavailable")
    manager = _manager(_LedgerBackend(error=failure))

    assert await manager.get_ledger_entries(_DEPLOYMENT_ID) == []

    _assert_metric(manager, "get_ledger_entries", success=False, error=str(failure))


@pytest.mark.asyncio
async def test_gateway_snapshot_preserves_wire_identity_and_accounting_envelope() -> None:
    save_rpc = MagicMock(return_value=SimpleNamespace(success=True, snapshot_id=73, error=""))
    manager = GatewayStateManager(SimpleNamespace(state=SimpleNamespace(SavePortfolioSnapshot=save_rpc)), timeout=4.5)
    snapshot = _snapshot()
    snapshot.token_prices = {"arbitrum:0xusdc": {"price_usd": "1", "symbol": "USDC", "decimals": 6}}
    snapshot.wallet_balances = [
        TokenBalance("USDC", Decimal("20.5"), Decimal("20.5"), "0xusdc", Decimal("1")),
        TokenBalance("ARB", Decimal("0"), Decimal("0"), "0xarb", None),
    ]

    assert await manager.save_portfolio_snapshot(snapshot) == 73

    request = save_rpc.call_args.args[0]
    assert save_rpc.call_args.kwargs == {"timeout": 4.5}
    assert request.deployment_id == _DEPLOYMENT_ID
    assert request.cycle_id == "cycle-7"
    assert request.execution_mode == "paper"
    assert request.timestamp == int(_NOW.timestamp())
    assert request.iteration_number == 7
    assert request.total_value_usd == "100.25"
    assert request.available_cash_usd == "20.50"
    assert request.value_confidence == "HIGH"
    assert request.chain == "arbitrum"
    payload = json.loads(request.positions_json)
    assert payload["metadata"]["__deployed_capital_usd__"] == "79.75"
    assert payload["metadata"]["__wallet_total_value_usd__"] == "101.00"
    assert payload["token_prices"] == snapshot.token_prices
    assert payload["wallet_balances"] == [
        {
            "address": "0xusdc",
            "balance": "20.5",
            "price_usd": "1",
            "symbol": "USDC",
            "value_usd": "20.5",
        },
        {
            "address": "0xarb",
            "balance": "0",
            "price_usd": None,
            "symbol": "ARB",
            "value_usd": "0",
        },
    ]
