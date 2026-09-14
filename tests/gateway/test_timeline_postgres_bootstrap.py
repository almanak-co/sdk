"""Postgres bootstrap retains ownership until its actual worker stops."""

import asyncio
from threading import Event, Thread
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from almanak.gateway.timeline import store as timeline_module


@pytest.fixture
def bootstrap_worker(monkeypatch):
    store = timeline_module.TimelineStore(database_url="postgresql://unused/test?schema=tenant")
    workers = []

    def start_worker(**kwargs):
        worker = Thread(**kwargs)
        workers.append((store._pg_loop, worker))
        return worker

    monkeypatch.setattr(timeline_module, "threading", SimpleNamespace(Thread=start_worker))
    yield store, workers
    for loop, worker in workers:
        if worker.is_alive():
            loop.call_soon_threadsafe(loop.stop)
            Thread.join(worker, timeout=2)
        assert not worker.is_alive(), "Bootstrap test left an active worker"
        loop.close()


def test_successful_bootstrap_runs_pool_and_history_on_owned_worker(bootstrap_worker, monkeypatch):
    store, workers = bootstrap_worker
    pool = SimpleNamespace(close=AsyncMock())
    initialized_on = []
    history_on = []

    async def initialize_pool():
        initialized_on.append(asyncio.get_running_loop())
        store._pg_pool = pool

    async def load_history():
        history_on.append(asyncio.get_running_loop())
        return []

    monkeypatch.setattr(store, "_async_init_pool", initialize_pool)
    monkeypatch.setattr(store, "_async_load_events", load_history)
    store.initialize()
    loop, worker = workers[0]
    assert initialized_on == [loop]
    assert history_on == [loop]
    assert worker.is_alive() and store._initialized
    assert store._database_url_clean == "postgresql://unused/test"
    assert store._pg_schema == "tenant"
    store.close()
    pool.close.assert_awaited_once()
    assert not worker.is_alive()
    assert store._pg_loop is None and store._pg_thread is None and store._pg_pool is None


def test_failed_bootstrap_clears_handles_after_worker_stops(bootstrap_worker, monkeypatch):
    store, workers = bootstrap_worker
    error = RuntimeError("Pool bootstrap failed")

    async def initialize_pool():
        raise error

    monkeypatch.setattr(store, "_async_init_pool", initialize_pool)
    with pytest.raises(RuntimeError) as raised:
        store.initialize()
    assert raised.value is error
    assert not workers[0][1].is_alive()
    assert not store._initialized
    assert store._pg_loop is None and store._pg_thread is None and store._pg_pool is None
    store.close()


def test_failed_bootstrap_retains_handles_when_worker_outlives_join(bootstrap_worker, monkeypatch):
    store, workers = bootstrap_worker
    entered, release = Event(), Event()
    pool = object()
    error = RuntimeError("Pool bootstrap failed after allocation")
    submit = store._pg_submit
    joins = []

    async def initialize_pool():
        store._pg_pool = pool
        raise error

    def busy_worker():
        entered.set()
        assert release.wait(5)

    def join_deadline_expires(timeout):
        # The real worker remains busy; model its join budget expiring without a five-second sleep.
        joins.append(timeout)
        assert workers[0][1].is_alive()

    def submit_with_busy_worker(coroutine, timeout=30):
        try:
            return submit(coroutine, timeout=timeout)
        except RuntimeError:
            loop, worker = workers[0]
            loop.call_soon_threadsafe(busy_worker)
            assert entered.wait(2)
            monkeypatch.setattr(worker, "join", join_deadline_expires)
            raise

    monkeypatch.setattr(store, "_async_init_pool", initialize_pool)
    monkeypatch.setattr(store, "_pg_submit", submit_with_busy_worker)
    try:
        with pytest.raises(RuntimeError) as raised:
            store.initialize()
        assert raised.value is error
        loop, worker = workers[0]
        assert joins == [5]
        assert worker.is_alive() and not store._initialized
        assert store._pg_loop is loop and store._pg_thread is worker and store._pg_pool is pool
    finally:
        release.set()
