"""Frozen QA execution cannot inherit a newly funded replacement fork."""

import threading
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from almanak.gateway import managed


@pytest.mark.asyncio
@pytest.mark.parametrize("qa_run", [False, True])
async def test_fork_loss_stops_qa_gateway_but_retains_normal_restart(qa_run, monkeypatch):
    stop = threading.Event()

    async def reset():
        stop.set()
        return True

    manager = SimpleNamespace(is_running=False, reset_to_latest=AsyncMock(side_effect=reset))
    gateway = SimpleNamespace(
        settings=SimpleNamespace(qa_pool_price_manifest="prepared.json" if qa_run else None),
        _stop_requested=stop,
        _watchdog_interval=0,
        _anvil_managers={"arbitrum": manager},
        _resetting_chains=set(),
        _fund_anvil_wallets=AsyncMock(),
    )
    monkeypatch.setattr(managed.asyncio, "sleep", AsyncMock())
    await managed.ManagedGateway._anvil_watchdog(gateway)
    assert stop.is_set()
    if qa_run:
        manager.reset_to_latest.assert_not_awaited()
        gateway._fund_anvil_wallets.assert_not_awaited()
    else:
        manager.reset_to_latest.assert_awaited_once()
        gateway._fund_anvil_wallets.assert_awaited_once_with(chains=["arbitrum"])


@pytest.mark.asyncio
async def test_healthy_qa_fork_is_not_stopped_by_watchdog(monkeypatch):
    stop = threading.Event()
    manager = SimpleNamespace(is_running=True, reset_to_latest=AsyncMock())
    gateway = SimpleNamespace(
        settings=SimpleNamespace(qa_pool_price_manifest="prepared.json"),
        _stop_requested=stop,
        _watchdog_interval=0,
        _anvil_managers={"arbitrum": manager},
        _resetting_chains=set(),
        _fund_anvil_wallets=AsyncMock(),
    )
    ticks = []

    async def tick(seconds):
        ticks.append(seconds)
        if len(ticks) == 2:
            assert not stop.is_set()
            stop.set()

    monkeypatch.setattr(managed.asyncio, "sleep", tick)
    await managed.ManagedGateway._anvil_watchdog(gateway)
    assert len(ticks) == 2
    manager.reset_to_latest.assert_not_awaited()
    gateway._fund_anvil_wallets.assert_not_awaited()
