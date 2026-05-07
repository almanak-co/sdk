"""Unit tests for ``StrategyRunner._run_position_event_attribution``.

Helper extracted from ``_write_ledger_entry`` to keep that method's
cyclomatic complexity in budget. The contract:

* No position_id → no-op (caller passes a freshly built event before id assigned).
* event_type=="OPEN" → call ``stamp_entry_state_on_open``; failures log WARN and swallow.
* event_type=="CLOSE" → call ``run_attribution_on_close``; failures log DEBUG and swallow.
* Any other event_type (INCREASE / DECREASE / SNAPSHOT) → no-op.

Failures must NEVER bubble up — they are best-effort attribution side-effects;
a stamp failure should not block the cycle's accounting writes.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from almanak.framework.runner.strategy_runner import StrategyRunner


def _make_runner() -> StrategyRunner:
    """Bare-minimum StrategyRunner — only the fields the SUT touches."""
    runner = StrategyRunner.__new__(StrategyRunner)
    runner.state_manager = MagicMock()
    runner.price_oracle = MagicMock()
    return runner


def _ev(event_type: str = "OPEN", position_id: str = "lp:eth:univ3:0x1:0x2"):
    return SimpleNamespace(event_type=event_type, position_id=position_id)


@pytest.mark.asyncio
async def test_noop_when_position_id_missing(monkeypatch):
    runner = _make_runner()
    stamp = AsyncMock()
    monkeypatch.setattr(
        "almanak.framework.observability.pnl_attributor.stamp_entry_state_on_open",
        stamp,
    )
    await runner._run_position_event_attribution(_ev(position_id=""))
    stamp.assert_not_called()


@pytest.mark.asyncio
async def test_open_calls_stamp(monkeypatch):
    runner = _make_runner()
    stamp = AsyncMock()
    monkeypatch.setattr(
        "almanak.framework.observability.pnl_attributor.stamp_entry_state_on_open",
        stamp,
    )
    pos_event = _ev(event_type="OPEN")
    await runner._run_position_event_attribution(pos_event)
    stamp.assert_awaited_once()


@pytest.mark.asyncio
async def test_open_swallows_stamp_exception(monkeypatch, caplog):
    runner = _make_runner()
    stamp = AsyncMock(side_effect=RuntimeError("boom"))
    monkeypatch.setattr(
        "almanak.framework.observability.pnl_attributor.stamp_entry_state_on_open",
        stamp,
    )
    # Must not raise.
    await runner._run_position_event_attribution(_ev(event_type="OPEN"))
    stamp.assert_awaited_once()


@pytest.mark.asyncio
async def test_close_calls_attribution(monkeypatch):
    runner = _make_runner()
    attribution = AsyncMock()
    monkeypatch.setattr(
        "almanak.framework.observability.pnl_attributor.run_attribution_on_close",
        attribution,
    )
    await runner._run_position_event_attribution(_ev(event_type="CLOSE"))
    attribution.assert_awaited_once()


@pytest.mark.asyncio
async def test_close_swallows_attribution_exception(monkeypatch):
    runner = _make_runner()
    attribution = AsyncMock(side_effect=ValueError("nope"))
    monkeypatch.setattr(
        "almanak.framework.observability.pnl_attributor.run_attribution_on_close",
        attribution,
    )
    await runner._run_position_event_attribution(_ev(event_type="CLOSE"))
    attribution.assert_awaited_once()


@pytest.mark.asyncio
async def test_other_event_type_is_noop(monkeypatch):
    runner = _make_runner()
    stamp = AsyncMock()
    attribution = AsyncMock()
    monkeypatch.setattr(
        "almanak.framework.observability.pnl_attributor.stamp_entry_state_on_open",
        stamp,
    )
    monkeypatch.setattr(
        "almanak.framework.observability.pnl_attributor.run_attribution_on_close",
        attribution,
    )
    for et in ("INCREASE", "DECREASE", "SNAPSHOT", "COLLECT_FEES", ""):
        await runner._run_position_event_attribution(_ev(event_type=et))
    stamp.assert_not_called()
    attribution.assert_not_called()
