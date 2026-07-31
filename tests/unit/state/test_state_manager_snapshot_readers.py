"""Behavioral coverage for StateManager portfolio-snapshot read wrappers.

These readers deliberately degrade ordinary backend failures to an empty result,
but corrupt persisted ``value_confidence`` is a typed boundary failure that must
remain loud.  Covering the wrapper contract prevents either case from silently
changing while keeping the diff-aware CRAP gate tied to executable behavior.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from almanak.framework.portfolio.models import ValueConfidenceParseError
from almanak.framework.state.state_manager import StateManager, StateTier

_DEPLOYMENT_ID = "deployment:state-reader"
_TIMESTAMP = datetime(2026, 7, 31, tzinfo=UTC)
_SNAPSHOT = object()


@dataclass(frozen=True)
class _ReaderCase:
    method_name: str
    args: tuple[Any, ...]
    empty_result: Any
    success_result: Any


_READER_CASES = (
    _ReaderCase(
        method_name="get_snapshots_since",
        args=(_DEPLOYMENT_ID, _TIMESTAMP, 42),
        empty_result=[],
        success_result=[_SNAPSHOT],
    ),
    _ReaderCase(
        method_name="get_snapshot_at",
        args=(_DEPLOYMENT_ID, _TIMESTAMP),
        empty_result=None,
        success_result=_SNAPSHOT,
    ),
)


class _UnsupportedWarmBackend:
    """Warm backend predating portfolio-snapshot readers."""


def _manager(*, warm: object | None, initialized: bool = True) -> StateManager:
    manager = StateManager.__new__(StateManager)
    manager._initialized = initialized
    manager._warm = warm
    manager._record_metrics = MagicMock()
    return manager


def _warm_reader(case: _ReaderCase, *, result: Any = None, error: Exception | None = None) -> tuple[Any, AsyncMock]:
    reader = AsyncMock(return_value=result, side_effect=error)
    warm = SimpleNamespace()
    setattr(warm, case.method_name, reader)
    return warm, reader


async def _read(manager: StateManager, case: _ReaderCase) -> Any:
    return await getattr(manager, case.method_name)(*case.args)


def _assert_metric(manager: StateManager, case: _ReaderCase, *, success: bool, error: str | None = None) -> None:
    args = manager._record_metrics.call_args.args
    assert args[0] == StateTier.WARM
    assert args[1] == case.method_name
    assert isinstance(args[2], float)
    assert args[2] >= 0
    assert args[3] is success
    if error is None:
        assert len(args) == 4
    else:
        assert args[4] == error


@pytest.mark.parametrize("case", _READER_CASES, ids=lambda case: case.method_name)
@pytest.mark.asyncio
async def test_reader_initializes_then_delegates_and_records_success(case: _ReaderCase) -> None:
    warm, reader = _warm_reader(case, result=case.success_result)
    manager = _manager(warm=warm, initialized=False)

    async def _initialize() -> None:
        manager._initialized = True

    manager.initialize = AsyncMock(side_effect=_initialize)  # type: ignore[method-assign]

    assert await _read(manager, case) == case.success_result

    manager.initialize.assert_awaited_once()
    reader.assert_awaited_once_with(*case.args)
    _assert_metric(manager, case, success=True)


@pytest.mark.parametrize("case", _READER_CASES, ids=lambda case: case.method_name)
@pytest.mark.asyncio
async def test_reader_without_warm_backend_returns_empty(case: _ReaderCase) -> None:
    manager = _manager(warm=None)

    assert await _read(manager, case) == case.empty_result
    manager._record_metrics.assert_not_called()


@pytest.mark.parametrize("case", _READER_CASES, ids=lambda case: case.method_name)
@pytest.mark.asyncio
async def test_reader_with_unsupported_backend_returns_empty(case: _ReaderCase) -> None:
    manager = _manager(warm=_UnsupportedWarmBackend())

    assert await _read(manager, case) == case.empty_result
    manager._record_metrics.assert_not_called()


@pytest.mark.parametrize("case", _READER_CASES, ids=lambda case: case.method_name)
@pytest.mark.asyncio
async def test_reader_reraises_invalid_persisted_confidence(case: _ReaderCase) -> None:
    error = ValueConfidenceParseError("invalid persisted value_confidence 'MYSTERY'")
    warm, _reader = _warm_reader(case, error=error)
    manager = _manager(warm=warm)

    with pytest.raises(ValueConfidenceParseError, match="MYSTERY") as exc_info:
        await _read(manager, case)

    assert exc_info.value is error
    _assert_metric(manager, case, success=False, error=str(error))


@pytest.mark.parametrize("case", _READER_CASES, ids=lambda case: case.method_name)
@pytest.mark.asyncio
async def test_reader_degrades_ordinary_backend_failure(case: _ReaderCase) -> None:
    error = RuntimeError("database unavailable")
    warm, _reader = _warm_reader(case, error=error)
    manager = _manager(warm=warm)

    assert await _read(manager, case) == case.empty_result
    _assert_metric(manager, case, success=False, error=str(error))
