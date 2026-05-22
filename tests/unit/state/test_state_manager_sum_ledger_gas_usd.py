"""Unit coverage for ``StateManager.sum_ledger_gas_usd`` (VIB-4225 ACC-02).

Pins the orchestrator's six-branch delegation contract:

1. Happy path → warm backend returns the sum.
2. No warm backend → returns ``Decimal("0")``.
3. Warm backend without ``sum_ledger_gas_usd`` method → returns ``Decimal("0")``.
4. ``AccountingPersistenceError`` from warm propagates unchanged.
5. ``NotImplementedError`` from warm propagates unchanged (hosted-mode
   contract — the runner's type-narrow catch must see this exception class
   directly; CodeRabbit thread #6).
6. Generic ``Exception`` from warm wraps as ``AccountingPersistenceError``
   with ``write_kind="metrics"``.
"""

from __future__ import annotations

from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock

import pytest

from almanak.framework.state.exceptions import (
    AccountingPersistenceError,
    AccountingWriteKind,
)
from almanak.framework.state.state_manager import StateManager


def _make_manager(*, warm: object | None = None) -> StateManager:
    """Build a StateManager skipping initialize() and inject ``_warm``."""
    sm = StateManager.__new__(StateManager)
    sm._initialized = True
    sm._warm = warm
    sm._record_metrics = MagicMock()
    return sm


@pytest.mark.asyncio
async def test_happy_path_delegates_to_warm() -> None:
    warm = MagicMock()
    warm.sum_ledger_gas_usd = AsyncMock(return_value=Decimal("0.42"))
    sm = _make_manager(warm=warm)

    result = await sm.sum_ledger_gas_usd("deployment-X")

    assert result == Decimal("0.42")
    warm.sum_ledger_gas_usd.assert_awaited_once_with("deployment-X")


@pytest.mark.asyncio
async def test_no_warm_backend_returns_zero() -> None:
    sm = _make_manager(warm=None)
    assert await sm.sum_ledger_gas_usd("any") == Decimal("0")


@pytest.mark.asyncio
async def test_legacy_backend_without_method_returns_zero() -> None:
    """An older backend without sum_ledger_gas_usd method gets graceful 0."""
    warm = object()  # bare object — no sum_ledger_gas_usd attribute
    sm = _make_manager(warm=warm)
    assert await sm.sum_ledger_gas_usd("any") == Decimal("0")


@pytest.mark.asyncio
async def test_accounting_persistence_error_propagates_unchanged() -> None:
    warm = MagicMock()
    warm.sum_ledger_gas_usd = AsyncMock(
        side_effect=AccountingPersistenceError(
            write_kind=AccountingWriteKind.METRICS,
            deployment_id="strategy-Y",
        )
    )
    sm = _make_manager(warm=warm)

    with pytest.raises(AccountingPersistenceError):
        await sm.sum_ledger_gas_usd("deployment-X")


@pytest.mark.asyncio
async def test_not_implemented_error_propagates_unchanged() -> None:
    """Hosted-mode contract: the runner's type-narrow catch sees
    ``NotImplementedError`` directly. Wrapping it as
    ``AccountingPersistenceError`` would shadow the contract.
    """
    warm = MagicMock()
    warm.sum_ledger_gas_usd = AsyncMock(side_effect=NotImplementedError("VIB-4247"))
    sm = _make_manager(warm=warm)

    with pytest.raises(NotImplementedError):
        await sm.sum_ledger_gas_usd("deployment-X")


@pytest.mark.asyncio
async def test_generic_exception_wraps_as_accounting_persistence_error() -> None:
    """Backend OperationalError / RuntimeError / etc. must surface as
    ``AccountingPersistenceError`` so the runner halts in live mode.
    """
    warm = MagicMock()
    warm.sum_ledger_gas_usd = AsyncMock(side_effect=RuntimeError("disk i/o"))
    sm = _make_manager(warm=warm)

    with pytest.raises(AccountingPersistenceError) as excinfo:
        await sm.sum_ledger_gas_usd("deployment-X")
    assert excinfo.value.write_kind == AccountingWriteKind.METRICS.value
    assert excinfo.value.deployment_id == "deployment-X"
    assert isinstance(excinfo.value.cause, RuntimeError)


@pytest.mark.asyncio
async def test_wrapped_error_uses_deployment_id() -> None:
    """Wrapped metrics errors are attributed to the deployment_id."""
    warm = MagicMock()
    warm.sum_ledger_gas_usd = AsyncMock(side_effect=RuntimeError("disk i/o"))
    sm = _make_manager(warm=warm)

    with pytest.raises(AccountingPersistenceError) as excinfo:
        await sm.sum_ledger_gas_usd("deployment-X")
    assert excinfo.value.deployment_id == "deployment-X"
