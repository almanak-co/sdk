"""Operator replay-barrier controls through real gRPC and durable local state."""

import asyncio
import json
import sqlite3
from types import SimpleNamespace

import pytest
from click.testing import CliRunner

from almanak.framework.cli import execution_recovery as recovery_module
from almanak.framework.gateway_client import GatewayClient, GatewayClientConfig
from almanak.framework.runner.runner_models import ExecutionProgress, SubmissionProvenance
from almanak.framework.state import StateData
from almanak.framework.state.gateway_state_manager import GatewayStateManager
from almanak.gateway.core.settings import GatewaySettings
from almanak.gateway.proto import gateway_pb2
from almanak.gateway.services.execution_service import ExecutionServiceServicer
from tests.conftest_gateway import GatewayServerThread

DEPLOYMENT = "deployment:recovery-contract"
TX_HASH = "0x" + "12" * 32


@pytest.fixture
def recovery_gateway(tmp_path, monkeypatch, unused_tcp_port):
    db_path = tmp_path / "almanak_state.db"
    monkeypatch.setenv("ALMANAK_STATE_DB", str(db_path))
    monkeypatch.setenv("ALMANAK_GATEWAY_GATEWAY_DB_PATH", str(db_path))
    monkeypatch.delenv("ALMANAK_IS_HOSTED", raising=False)
    monkeypatch.delenv("ALMANAK_GATEWAY_AUTH_TOKEN", raising=False)
    statuses = {}
    status_queries = []

    async def controlled_status(self, tx_hash, chain, context):
        status_queries.append((chain, tx_hash))
        if tx_hash not in statuses:
            raise AssertionError("Unexpected transaction-status query")
        return statuses[tx_hash]

    monkeypatch.setattr(ExecutionServiceServicer, "_get_evm_tx_status", controlled_status)
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
    try:
        server.start()
        client.connect()
        yield SimpleNamespace(
            manager=GatewayStateManager(client),
            port=unused_tcp_port,
            db_path=db_path,
            statuses=statuses,
            status_queries=status_queries,
        )
    finally:
        client.disconnect()
        server.stop()


def _marker(provenance=SubmissionProvenance.NOT_ATTEMPTED):
    progress = ExecutionProgress(
        execution_id="execution-control-plane",
        deployment_id=DEPLOYMENT,
        intents_hash="sealed-plan",
        total_steps=1,
    )
    progress.record_submission_evidence(
        step_index=0,
        chain="bsc",
        submission_provenance=provenance,
        submitted_transaction_ids=[TX_HASH] if provenance is SubmissionProvenance.ATTEMPTED else [],
    )
    progress.mark_reconciliation_required(0, "Submission outcome requires reconciliation")
    return progress.to_dict()


def _save(gateway, marker):
    return asyncio.run(
        gateway.manager.save_state(
            StateData(
                deployment_id=DEPLOYMENT,
                state={"execution_progress": marker, "strategy": {"phase": "HELD"}, "runner_overlay": {"paused": True}},
            )
        )
    )


def _row(gateway):
    with sqlite3.connect(gateway.db_path) as connection:
        version, raw = connection.execute(
            "SELECT version, state_data FROM strategy_state WHERE deployment_id=?", (DEPLOYMENT,)
        ).fetchone()
    return version, json.loads(raw)


def _invoke(gateway, action, *options):
    return CliRunner().invoke(
        recovery_module.execution_recovery,
        [
            action,
            "--deployment-id",
            DEPLOYMENT,
            "--gateway-host",
            "127.0.0.1",
            "--gateway-port",
            str(gateway.port),
            *options,
        ],
    )


def test_not_attempted_inspect_reconcile_and_release_is_durable(recovery_gateway):
    gateway = recovery_gateway
    marker = _marker()
    _save(gateway, marker)
    before = _row(gateway)
    for action in ("inspect", "reconcile"):
        result = _invoke(gateway, action, "--json")
        assert result.exit_code == 0, result.output
        assert json.loads(result.output)["assessment"]["releasable"] is True
        assert _row(gateway) == before
    result = _invoke(gateway, "release", "--execution-id", marker["execution_id"], "--apply")
    assert result.exit_code == 0, result.output
    version, state = _row(gateway)
    assert version > before[0]
    assert state == {"strategy": {"phase": "HELD"}, "runner_overlay": {"paused": True}}
    assert gateway.status_queries == []
    result = _invoke(gateway, "inspect")
    assert result.exit_code == 1
    assert "no durable execution replay barrier" in result.output


@pytest.mark.parametrize("status", ["pending", "unknown", "confirmed", "reverted"])
def test_attempted_release_requires_canonical_no_land_proof(recovery_gateway, status):
    gateway = recovery_gateway
    marker = _marker(SubmissionProvenance.ATTEMPTED)
    _save(gateway, marker)
    before = _row(gateway)
    gateway.statuses[TX_HASH] = gateway_pb2.TxStatus(status=status)
    reconciled = _invoke(gateway, "reconcile", "--json")
    assert reconciled.exit_code == 2, reconciled.output
    assessment = json.loads(reconciled.output)["assessment"]
    assert assessment["releasable"] is False
    expected_status = "canonical_receipt_unavailable" if status in {"confirmed", "reverted"} else status
    assert assessment["transaction_statuses"] == {TX_HASH: expected_status}
    assert _row(gateway) == before
    result = _invoke(gateway, "release", "--execution-id", marker["execution_id"], "--apply")
    assert result.exit_code == 1
    assert "release refused" in result.output
    assert gateway.status_queries == [("bsc", TX_HASH), ("bsc", TX_HASH)]
    assert _row(gateway) == before


def test_canonical_reverted_receipt_allows_durable_retry_release(recovery_gateway):
    gateway = recovery_gateway
    marker = _marker(SubmissionProvenance.ATTEMPTED)
    _save(gateway, marker)
    gateway.statuses[TX_HASH] = gateway_pb2.TxStatus(
        status="reverted",
        block_number=7,
        gas_used=21_000,
        canonical_receipt=json.dumps(
            {
                "tx_hash": TX_HASH,
                "block_number": 7,
                "block_hash": "0x" + "34" * 32,
                "gas_used": 21_000,
                "effective_gas_price": "6",
                "status": 0,
                "logs": [],
            }
        ).encode(),
    )
    before = _row(gateway)
    inspected = _invoke(gateway, "inspect", "--json")
    assert inspected.exit_code == 0, inspected.output
    assert not json.loads(inspected.output)["assessment"]["releasable"]
    assert gateway.status_queries == []
    reconciled = _invoke(gateway, "reconcile", "--json")
    assert reconciled.exit_code == 0, reconciled.output
    assert json.loads(reconciled.output)["assessment"]["releasable"]
    assert _row(gateway) == before
    result = _invoke(gateway, "release", "--execution-id", marker["execution_id"], "--apply")
    assert result.exit_code == 0, result.output
    assert "execution_progress" not in _row(gateway)[1]
    assert gateway.status_queries == [("bsc", TX_HASH), ("bsc", TX_HASH)]


@pytest.mark.parametrize(
    "options, message",
    [
        (("--execution-id", "execution-control-plane"), "pass --apply"),
        (("--execution-id", "outdated-execution", "--apply"), "execution id changed"),
    ],
)
def test_release_requires_explicit_apply_and_exact_execution_id(recovery_gateway, options, message):
    _save(recovery_gateway, _marker())
    before = _row(recovery_gateway)
    result = _invoke(recovery_gateway, "release", *options)
    assert result.exit_code != 0
    assert message in result.output
    assert _row(recovery_gateway) == before
    assert recovery_gateway.status_queries == []


def test_same_execution_id_changed_marker_is_not_deleted(recovery_gateway, monkeypatch):
    gateway = recovery_gateway
    marker = _marker()
    _save(gateway, marker)
    original_query = recovery_module._query_statuses
    concurrent_state = None

    def update_while_reconciling(client, progress):
        nonlocal concurrent_state
        statuses = original_query(client, progress)
        current = asyncio.run(gateway.manager.load_state(DEPLOYMENT))
        assert current is not None
        current.state["execution_progress"]["failure_error"] = "New evidence arrived for the same execution"
        current.state["runner_overlay"]["new_observation"] = True
        asyncio.run(gateway.manager.save_state(current, expected_version=current.version))
        concurrent_state = _row(gateway)
        return statuses

    monkeypatch.setattr(recovery_module, "_query_statuses", update_while_reconciling)
    result = _invoke(gateway, "release", "--execution-id", marker["execution_id"], "--apply")
    assert result.exit_code == 1
    assert "marker changed concurrently" in result.output
    assert _row(gateway) == concurrent_state
    assert gateway.status_queries == []
