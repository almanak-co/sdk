"""Gateway operational stores retain their server owner through shutdown."""

import sqlite3
from datetime import UTC, datetime

import pytest

from almanak.gateway.registry import store as registry_module
from almanak.gateway.timeline import store as timeline_module


@pytest.fixture(autouse=True)
def isolated_globals(monkeypatch, tmp_path):
    monkeypatch.setattr("almanak.gateway.core.settings.DEFAULT_GATEWAY_DB_PATH", str(tmp_path / "legacy.db"))
    monkeypatch.setattr(registry_module, "_instance_registry", None)
    monkeypatch.setattr(timeline_module, "_timeline_store", None)
    yield
    registry_module.reset_instance_registry()
    timeline_module.reset_timeline_store()


def _event(identifier):
    return timeline_module.TimelineEvent(
        event_id=identifier,
        deployment_id="deployment:store-owner-a",
        timestamp=datetime.now(UTC),
        event_type="CUSTOM",
        description="Store ownership probe",
    )


def test_uninitialized_registry_refuses_legacy_database(tmp_path):
    with pytest.raises(RuntimeError, match="explicit storage"):
        registry_module.get_instance_registry()
    assert not (tmp_path / "legacy.db").exists()


def test_uninitialized_timeline_refuses_non_durable_events(tmp_path):
    with pytest.raises(RuntimeError, match="explicit storage"):
        timeline_module.get_timeline_store()
    assert list(tmp_path.iterdir()) == []


def test_explicit_second_owner_configuration_refuses(tmp_path):
    registry_module.get_instance_registry(tmp_path / "a.db")
    timeline_module.get_timeline_store(db_path=tmp_path / "a.db")
    with pytest.raises(RuntimeError, match="different storage"):
        registry_module.get_instance_registry(tmp_path / "b.db")
    with pytest.raises(RuntimeError, match="different storage"):
        timeline_module.get_timeline_store(db_path=tmp_path / "b.db")
    assert not (tmp_path / "b.db").exists()


def test_closed_timeline_reference_cannot_reopen_database(tmp_path):
    store = timeline_module.get_timeline_store(db_path=tmp_path / "a.db")
    timeline_module.reset_timeline_store()
    with pytest.raises(RuntimeError, match="closed"):
        store.add_event(_event("after-close"))
    with sqlite3.connect(tmp_path / "a.db") as connection:
        assert connection.execute("SELECT event_id FROM timeline_events").fetchall() == []


def test_closed_registry_reference_cannot_reinitialize(tmp_path):
    store = registry_module.get_instance_registry(tmp_path / "a.db")
    registry_module.reset_instance_registry()
    with pytest.raises(RuntimeError, match="closed"):
        store.initialize()
    assert not store._initialized


def test_two_real_gateways_keep_storage_independent_through_sibling_shutdown(
    tmp_path, monkeypatch, unused_tcp_port_factory
):
    from almanak.framework.gateway_client import GatewayClient, GatewayClientConfig
    from almanak.gateway.core.settings import GatewaySettings
    from almanak.gateway.proto import gateway_pb2
    from tests.conftest_gateway import GatewayServerThread

    monkeypatch.delenv("ALMANAK_IS_HOSTED", raising=False)
    servers = []
    clients = []
    try:
        for name in ("a", "b"):
            db_path = tmp_path / f"{name}.db"
            monkeypatch.setenv("ALMANAK_STATE_DB", str(db_path))
            monkeypatch.setenv("ALMANAK_GATEWAY_GATEWAY_DB_PATH", str(db_path))
            port = unused_tcp_port_factory()
            settings = GatewaySettings(
                grpc_host="127.0.0.1",
                grpc_port=port,
                gateway_db_path=str(db_path),
                network="mainnet",
                allow_insecure=True,
                metrics_enabled=False,
                audit_enabled=False,
                database_url="",
            )
            server = GatewayServerThread(settings)
            servers.append(server)
            server.start()
            client = GatewayClient(GatewayClientConfig(host="127.0.0.1", port=port))
            clients.append(client)
            client.connect()
        response = clients[1].observe.RecordTimelineEvent(
            gateway_pb2.RecordTimelineEventRequest(
                deployment_id="deployment:store-owner-b",
                event_type="CUSTOM",
                description="Second owner event",
            ),
            timeout=5,
        )
        assert response.success
        with sqlite3.connect(tmp_path / "a.db") as connection:
            assert connection.execute("SELECT deployment_id FROM timeline_events").fetchall() == []
        with sqlite3.connect(tmp_path / "b.db") as connection:
            assert connection.execute("SELECT deployment_id FROM timeline_events").fetchall() == [
                ("deployment:store-owner-b",)
            ]
        response = clients[1].dashboard.RegisterStrategyInstance(
            gateway_pb2.RegisterInstanceRequest(
                deployment_id="deployment:store-owner-b",
                strategy_name="owner-b",
                template_name="OwnerB",
                chain="bsc",
                wallet_address="0x" + "11" * 20,
                config_json="{}",
            ),
            timeout=5,
        )
        assert response.success
        response = clients[1].lifecycle.WriteState(
            gateway_pb2.WriteAgentStateRequest(
                deployment_id="deployment:store-owner-b",
                state="RUNNING",
            ),
            timeout=5,
        )
        assert response.success
        for name, expected in (("a", []), ("b", [("deployment:store-owner-b",)])):
            with sqlite3.connect(tmp_path / f"{name}.db") as connection:
                assert connection.execute("SELECT deployment_id FROM strategy_instances").fetchall() == expected
                assert connection.execute("SELECT deployment_id FROM agent_state").fetchall() == expected
        first_owner = servers[0]._server._operational_stores
        servers[0].stop()
        response = clients[1].observe.RecordTimelineEvent(
            gateway_pb2.RecordTimelineEventRequest(
                deployment_id="deployment:store-owner-b",
                event_type="CUSTOM",
                description="Surviving owner event",
            ),
            timeout=5,
        )
        assert response.success
        response = clients[1].lifecycle.Heartbeat(
            gateway_pb2.HeartbeatRequest(
                deployment_id="deployment:store-owner-b",
            ),
            timeout=5,
        )
        assert response.success
        with pytest.raises(RuntimeError, match="closed"):
            first_owner.timeline.add_event(_event("old-handler"))
        with sqlite3.connect(tmp_path / "b.db") as connection:
            assert connection.execute("SELECT count(*) FROM timeline_events").fetchone() == (2,)
    finally:
        for client in reversed(clients):
            client.disconnect()
        for server in reversed(servers):
            server.stop()


def test_close_waits_for_history_read_without_serializing_timeline_writes(tmp_path, monkeypatch):
    from concurrent.futures import ThreadPoolExecutor
    from threading import Event

    store = timeline_module.TimelineStore(db_path=tmp_path / "timeline.db")
    store.initialize()
    store._pg_history_truncated = True
    store._pg_cache_floor = datetime.now(UTC)
    entered, release = Event(), Event()

    def pending_history(coroutine, **kwargs):
        coroutine.close()
        entered.set()
        assert release.wait(5)
        return []

    monkeypatch.setattr(store, "_pg_submit", pending_history)
    with ThreadPoolExecutor(max_workers=2) as executor:
        reading = executor.submit(store.get_events, "deployment:store-owner-a")
        try:
            assert entered.wait(2)
            store.add_event(_event("while-history-waits"), timeout=0.1)
            closing = executor.submit(store.close)
            with store._lifetime._condition:
                assert store._lifetime._condition.wait_for(lambda: store._lifetime._closing, timeout=2)
            assert not closing.done()
            with pytest.raises(RuntimeError, match="closed"):
                store.add_event(_event("during-close"))
        finally:
            release.set()
        assert reading.result(timeout=2) == []
        closing.result(timeout=2)
    with sqlite3.connect(tmp_path / "timeline.db") as connection:
        assert connection.execute("SELECT event_id FROM timeline_events").fetchall() == [("while-history-waits",)]
    with pytest.raises(RuntimeError, match="closed"):
        store.get_events("deployment:store-owner-a")


def test_lifecycle_owner_drains_admitted_worker_and_rejects_retained_reference():
    from concurrent.futures import ThreadPoolExecutor
    from threading import Event
    from unittest.mock import MagicMock

    from almanak.gateway.lifecycle.store import LifecycleStore
    from almanak.gateway.operational_stores import OwnedLifecycleStore

    entered, release = Event(), Event()
    backend = MagicMock(spec=LifecycleStore)

    def heartbeat(deployment):
        entered.set()
        assert release.wait(5)

    backend.heartbeat.side_effect = heartbeat
    store = OwnedLifecycleStore(backend)
    with ThreadPoolExecutor(max_workers=2) as executor:
        writing = executor.submit(store.heartbeat, "deployment:worker")
        try:
            assert entered.wait(2)
            closing = executor.submit(store.close)
            with store._lifetime._condition:
                assert store._lifetime._condition.wait_for(lambda: store._lifetime._closing, timeout=2)
            backend.close.assert_not_called()
        finally:
            release.set()
        writing.result(timeout=2)
        closing.result(timeout=2)
    backend.close.assert_called_once_with()
    with pytest.raises(RuntimeError, match="closed"):
        store.heartbeat("deployment:worker")
    with pytest.raises(RuntimeError, match="closed"):
        store.initialize()
    assert backend.heartbeat.call_count == 1


def test_hosted_factory_configuration_and_protocol_are_preserved(tmp_path, monkeypatch):
    from unittest.mock import MagicMock

    from almanak.gateway import operational_stores as owners
    from almanak.gateway.core.settings import GatewaySettings
    from almanak.gateway.lifecycle.store import LifecycleStore

    monkeypatch.setattr("almanak.framework.deployment.mode.is_hosted", lambda: True)
    monkeypatch.setattr("almanak.framework.deployment.mode.deployment_id", lambda: "deployment:hosted-owner")
    timeline = MagicMock(spec=timeline_module.TimelineStore)
    timeline_factory = MagicMock(return_value=timeline)
    backend = MagicMock(spec=LifecycleStore)
    lifecycle_factory = MagicMock(return_value=backend)
    monkeypatch.setattr(owners, "TimelineStore", timeline_factory)
    monkeypatch.setattr(owners, "create_lifecycle_store", lifecycle_factory)
    settings = GatewaySettings(database_url="postgresql://placeholder", gateway_db_path=str(tmp_path / "registry.db"))
    owner = owners.OperationalStores.create(settings)
    try:
        timeline_factory.assert_called_once_with(
            database_url="postgresql://placeholder",
            scope_deployment_id="deployment:hosted-owner",
            startup_load_limit=settings.timeline_startup_load_limit,
        )
        lifecycle_factory.assert_called_once_with(
            database_url="postgresql://placeholder",
            sqlite_path=str(tmp_path / "registry.db"),
        )
        owner.lifecycle.read_state("deployment:hosted-owner")
        backend.read_state.assert_called_once_with("deployment:hosted-owner")
    finally:
        owner.close()
    backend.close.assert_called_once_with()


@pytest.mark.asyncio
async def test_failed_start_releases_owned_stores_and_database_lock(tmp_path, monkeypatch):
    from unittest.mock import AsyncMock

    from almanak.gateway import server as server_module
    from almanak.gateway.core.settings import GatewaySettings

    path = tmp_path / "failed-start.db"
    monkeypatch.delenv("ALMANAK_IS_HOSTED", raising=False)
    monkeypatch.setenv("ALMANAK_STATE_DB", str(path))
    monkeypatch.setenv("ALMANAK_GATEWAY_GATEWAY_DB_PATH", str(path))
    monkeypatch.setattr(
        server_module, "validate_state_schema_at_boot", AsyncMock(side_effect=RuntimeError("schema refusal"))
    )
    server = server_module.GatewayServer(
        GatewaySettings(
            gateway_db_path=str(path),
            database_url="",
            metrics_enabled=False,
            allow_insecure=True,
        )
    )
    with pytest.raises(RuntimeError, match="schema refusal"):
        await server.start()
    assert server._local_db_lock is None
    assert server._operational_stores is not None
    with pytest.raises(RuntimeError, match="closed"):
        server._operational_stores.registry.initialize()
    with pytest.raises(RuntimeError, match="closed"):
        server._operational_stores.lifecycle.read_state("deployment:old")


def test_backend_close_failure_remains_visible_and_terminal(tmp_path, monkeypatch):
    from unittest.mock import MagicMock

    store = timeline_module.TimelineStore(db_path=tmp_path / "timeline.db")
    store.initialize()
    store._pg_pool = MagicMock()
    store._pg_loop = MagicMock()
    monkeypatch.setattr(store, "_pg_submit", MagicMock(side_effect=TimeoutError("close budget")))
    with pytest.raises(RuntimeError, match="PostgreSQL close failed"):
        store.close()
    with pytest.raises(RuntimeError, match="previously failed"):
        store.close()
    with pytest.raises(RuntimeError, match="closed"):
        store.add_event(_event("after-failed-close"))


def test_operational_store_close_preserves_all_backend_failures():
    from unittest.mock import MagicMock

    from almanak.gateway.operational_stores import OperationalStores

    lifecycle, registry, timeline = (MagicMock(), MagicMock(), MagicMock())
    lifecycle.close.side_effect = RuntimeError("lifecycle failure")
    timeline.close.side_effect = OSError("timeline failure")
    owner = OperationalStores(registry=registry, timeline=timeline, lifecycle=lifecycle)

    with pytest.raises(ExceptionGroup) as raised:
        owner.close()

    assert [str(error) for error in raised.value.exceptions] == ["lifecycle failure", "timeline failure"]
    lifecycle.close.assert_called_once_with()
    registry.close.assert_called_once_with()
    timeline.close.assert_called_once_with()


def test_lifecycle_timeout_does_not_claim_underlying_plugin_work_was_cancelled():
    from concurrent.futures import ThreadPoolExecutor
    from threading import Event
    from unittest.mock import MagicMock

    from almanak.gateway.lifecycle.store import LifecycleStore
    from almanak.gateway.operational_stores import OwnedLifecycleStore

    release, committed = Event(), Event()
    backend = MagicMock(spec=LifecycleStore)
    store = OwnedLifecycleStore(backend)
    with ThreadPoolExecutor(max_workers=1) as executor:

        def delayed_commit():
            assert release.wait(5)
            committed.set()

        future = executor.submit(delayed_commit)
        backend.heartbeat.side_effect = lambda deployment: future.result(timeout=0.01)
        try:
            with pytest.raises(TimeoutError):
                store.heartbeat("deployment:timed-out")
            store.close()
            assert not committed.is_set()
            with pytest.raises(RuntimeError, match="closed"):
                store.heartbeat("deployment:new-call")
        finally:
            release.set()
        future.result(timeout=2)
    assert committed.is_set()
    assert backend.heartbeat.call_count == 1


def test_same_postgres_url_cannot_rebind_explicit_deployment_scope(monkeypatch):
    monkeypatch.setattr(timeline_module.TimelineStore, "initialize", lambda self: None)
    first = timeline_module.get_timeline_store(database_url="postgresql://placeholder", scope_deployment_id="owner-a")
    assert timeline_module.get_timeline_store() is first
    from pathlib import Path

    with pytest.raises(RuntimeError, match="different storage"):
        timeline_module.get_timeline_store(db_path=Path.cwd())
    with pytest.raises(RuntimeError, match="different storage"):
        timeline_module.get_timeline_store(database_url="postgresql://placeholder", scope_deployment_id="owner-b")


def test_close_during_lazy_initialization_refuses_without_creating_database(tmp_path, monkeypatch):
    from concurrent.futures import ThreadPoolExecutor
    from threading import Event

    store = registry_module.InstanceRegistry(tmp_path / "uninitialized.db")
    original_initialize = store.initialize
    entered, release = Event(), Event()

    def delayed_initialize():
        entered.set()
        assert release.wait(5)
        original_initialize()

    monkeypatch.setattr(store, "initialize", delayed_initialize)
    with ThreadPoolExecutor(max_workers=2) as executor:
        reading = executor.submit(store.list_all)
        try:
            assert entered.wait(2)
            closing = executor.submit(store.close)
            with store._lifetime._condition:
                assert store._lifetime._condition.wait_for(lambda: store._lifetime._closing, timeout=2)
        finally:
            release.set()
        with pytest.raises(RuntimeError, match="closed"):
            reading.result(timeout=2)
        closing.result(timeout=2)
    assert not (tmp_path / "uninitialized.db").exists()


@pytest.mark.asyncio
async def test_cancelled_shutdown_keeps_database_lock_until_owned_worker_finishes(monkeypatch):
    import asyncio
    from threading import Event
    from unittest.mock import MagicMock

    from almanak.gateway.core.settings import GatewaySettings
    from almanak.gateway.lifecycle.store import LifecycleStore
    from almanak.gateway.operational_stores import OperationalStores, OwnedLifecycleStore
    from almanak.gateway.server import GatewayServer

    entered, release = Event(), Event()
    backend = MagicMock(spec=LifecycleStore)

    def heartbeat(deployment):
        entered.set()
        assert release.wait(5)

    backend.heartbeat.side_effect = heartbeat
    lifecycle = OwnedLifecycleStore(backend)
    owner = OperationalStores(MagicMock(), MagicMock(), lifecycle)
    server = GatewayServer(GatewaySettings(metrics_enabled=False))
    server._operational_stores = owner
    server._local_db_lock = 4242
    released = MagicMock()
    monkeypatch.setattr("almanak.framework.local_paths.release_local_db_lock", released)
    worker = asyncio.create_task(asyncio.to_thread(lifecycle.heartbeat, "deployment:active-worker"))

    def wait_for_close():
        with lifecycle._lifetime._condition:
            return lifecycle._lifetime._condition.wait_for(lambda: lifecycle._lifetime._closing, timeout=3)

    try:
        assert await asyncio.to_thread(entered.wait, 2)
        stopping = asyncio.create_task(server.stop(grace=0))
        assert await asyncio.to_thread(wait_for_close)
        stopping.cancel()
        await asyncio.sleep(0)
        assert not stopping.done()
        released.assert_not_called()
        backend.close.assert_not_called()
    finally:
        release.set()
    await worker
    with pytest.raises(asyncio.CancelledError):
        await stopping
    backend.close.assert_called_once_with()
    released.assert_called_once_with(4242)
    assert server._local_db_lock is None


@pytest.mark.asyncio
async def test_cancelled_shutdown_preserves_close_failure():
    import asyncio
    from threading import Event

    from almanak.gateway.core.settings import GatewaySettings
    from almanak.gateway.server import GatewayServer

    entered, release = Event(), Event()

    def close():
        entered.set()
        assert release.wait(5)
        raise RuntimeError("registry close failed")

    server = GatewayServer(GatewaySettings(metrics_enabled=False))
    server._operational_stores = type("Owner", (), {"close": staticmethod(close)})()
    stopping = asyncio.create_task(server._close_operational_stores())
    try:
        assert await asyncio.to_thread(entered.wait, 2)
        stopping.cancel()
        await asyncio.sleep(0)
        assert not stopping.done()
    finally:
        release.set()
    with pytest.raises(asyncio.CancelledError):
        await stopping
