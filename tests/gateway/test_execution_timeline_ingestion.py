"""Execution events must persist without synchronously calling their own server loop."""

import asyncio
import sqlite3
from unittest.mock import MagicMock

from almanak.framework.api.timeline import get_event_gateway_client, set_event_gateway_client
from almanak.framework.execution.events import ExecutionEventType
from almanak.framework.execution.orchestrator import ExecutionContext
from almanak.framework.gateway_client import GatewayClient, GatewayClientConfig
from almanak.gateway.core.settings import GatewaySettings
from almanak.gateway.services.execution_service import ExecutionServiceServicer
from tests.conftest_gateway import GatewayServerThread


def test_gateway_execution_event_is_durable_on_its_own_server_loop(tmp_path, monkeypatch, unused_tcp_port):
    monkeypatch.setattr("almanak.framework.api.timeline.EVENTS_CACHE_FILE", tmp_path / "events.json")
    db_path = tmp_path / "almanak_state.db"
    monkeypatch.setenv("ALMANAK_STATE_DB", str(db_path))
    monkeypatch.setenv("ALMANAK_GATEWAY_GATEWAY_DB_PATH", str(db_path))
    monkeypatch.delenv("ALMANAK_IS_HOSTED", raising=False)
    monkeypatch.setattr("almanak.gateway.utils.get_rpc_url", lambda *args, **kwargs: "http://127.0.0.1:65500")
    settings = GatewaySettings(
        grpc_host="127.0.0.1",
        grpc_port=unused_tcp_port,
        gateway_db_path=str(db_path),
        network="mainnet",
        allow_insecure=True,
        metrics_enabled=False,
        audit_enabled=False,
        database_url="",
    )
    server = GatewayServerThread(settings)
    client = GatewayClient(GatewayClientConfig(host="127.0.0.1", port=unused_tcp_port))
    previous = get_event_gateway_client()
    try:
        server.start()
        client.connect()
        set_event_gateway_client(client)
        rpc = MagicMock(side_effect=AssertionError("Execution must not call its own gateway"))
        monkeypatch.setattr(client.observe, "RecordTimelineEvent", rpc)
        service = ExecutionServiceServicer(settings)
        monkeypatch.setattr(service, "_create_signer", lambda wallet: MagicMock())

        async def emit_on_server_loop():
            orchestrator = await service._get_orchestrator("bsc", "0x" + "11" * 20)
            orchestrator._emit_event(
                ExecutionEventType.SIMULATING,
                ExecutionContext(deployment_id="timeline-loop-test", chain="bsc", correlation_id="test-cycle"),
                {"tx_count": 2},
            )

        assert server._loop is not None
        asyncio.run_coroutine_threadsafe(emit_on_server_loop(), server._loop).result(timeout=10)
        with sqlite3.connect(db_path) as connection:
            rows = connection.execute(
                "SELECT deployment_id, event_type, chain, details_json FROM timeline_events WHERE deployment_id=?",
                ("timeline-loop-test",),
            ).fetchall()
        rpc.assert_not_called()
        assert len(rows) == 1
        assert rows[0][:3] == ("timeline-loop-test", "CUSTOM", "bsc")
        assert '"correlation_id": "test-cycle"' in rows[0][3]
    finally:
        set_event_gateway_client(previous)
        client.disconnect()
        server.stop()
