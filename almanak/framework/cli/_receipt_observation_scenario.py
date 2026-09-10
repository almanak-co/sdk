"""One-shot observation loss for the guarded managed-Anvil scenario surface."""

from __future__ import annotations

import json
import logging
import os
import tempfile
from pathlib import Path
from typing import TYPE_CHECKING, Any, cast

from almanak.framework.execution.gateway_orchestrator import GatewayExecutionOrchestrator
from almanak.gateway.proto import gateway_pb2

from ._reference_scenario import require_reference_test_runtime

if TYPE_CHECKING:
    from almanak.framework.gateway_client import GatewayClient

logger = logging.getLogger(__name__)


def _publish_once(path: Path, payload: dict) -> bool:
    descriptor, temporary = tempfile.mkstemp(prefix=".receipt-observation-", dir=path.parent)
    try:
        with os.fdopen(descriptor, "w") as stream:
            json.dump(payload, stream, sort_keys=True)
            stream.flush()
            os.fsync(stream.fileno())
        try:
            os.link(temporary, path)
        except FileExistsError:
            return False
        directory = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
        return True
    finally:
        os.unlink(temporary)


class _ExecutionObservations:
    def __init__(self, client: Any, *, network: str, managed: bool, chain: str, marker: Path, digest: str):
        self._client = client
        self._network, self._managed, self._chain = network, managed, chain
        self._marker, self._digest = marker, digest

    def Execute(self, request: Any, **kwargs: Any) -> Any:  # noqa: N802 - protobuf API
        require_reference_test_runtime(network=self._network, managed=self._managed)
        if request.chain != self._chain:
            raise ValueError("Receipt observation scenario chain mismatch")
        if self._marker.is_symlink():
            raise ValueError("Receipt observation marker cannot be a symlink")
        if self._marker.exists():
            consumed = json.loads(self._marker.read_text())
            if (
                consumed.get("scenario_sha256") != self._digest
                or consumed.get("deployment_id") != request.deployment_id
            ):
                raise ValueError("Receipt observation scenario differs from its consumed marker")
            return self._client.execution.Execute(request, **kwargs)
        version = self._client.rpc.Call(
            gateway_pb2.RpcRequest(chain=self._chain, method="web3_clientVersion", params="[]"),
            timeout=10,
        )
        if not version.success or not str(json.loads(version.result)).lower().startswith("anvil"):
            raise ValueError("Receipt observation scenario gateway is not connected to Anvil")
        response = self._client.execution.Execute(request, **kwargs)
        if not response.success or not response.tx_hashes:
            return response
        try:
            bundle = json.loads(request.action_bundle)
        except (TypeError, ValueError):
            return response
        if not isinstance(bundle, dict) or str(bundle.get("intent_type", "")).upper() != "SWAP":
            return response
        evidence = {
            "synthetic": True,
            "scenario_sha256": self._digest,
            "deployment_id": request.deployment_id,
            "chain": self._chain,
            "intent_id": request.intent_id,
            "tx_hashes": list(response.tx_hashes),
            "execution_plan_hash": response.execution_plan_hash,
        }
        if not _publish_once(self._marker, evidence):
            return response
        withheld = gateway_pb2.ExecutionResult()
        withheld.CopyFrom(response)
        withheld.success = False
        withheld.receipts = b""
        withheld.error_code = "RECEIPT_SET_INCOMPLETE"
        withheld.error = "Synthetic managed-Anvil scenario withheld execution receipts; canonical observation required"
        logger.warning("SYNTHETIC_EXECUTION_RECEIPTS_WITHHELD %s", json.dumps(evidence, sort_keys=True))
        return withheld

    def __getattr__(self, name: str) -> Any:
        return getattr(self._client.execution, name)


class _ObservationClient:
    def __init__(self, client: Any, execution: _ExecutionObservations):
        self._delegate, self.execution = client, execution

    def __getattr__(self, name: str) -> Any:
        return getattr(self._delegate, name)


def install_receipt_observation_scenario(
    runner: Any,
    *,
    network: str,
    managed: bool,
    working_dir: str,
    digest: str,
) -> None:
    require_reference_test_runtime(network=network, managed=managed)
    orchestrator = runner.execution_orchestrator
    if not isinstance(orchestrator, GatewayExecutionOrchestrator):
        raise ValueError("Receipt observation scenario requires a gateway-backed single-chain runner")
    directory = Path(working_dir).resolve() / ".almanak"
    if directory.is_symlink():
        raise ValueError("Receipt observation scenario directory cannot be a symlink")
    directory.mkdir(exist_ok=True)
    client = orchestrator._client
    execution = _ExecutionObservations(
        client,
        network=network,
        managed=managed,
        chain=orchestrator._chain,
        marker=directory / "receipt-observation-consumed.json",
        digest=digest,
    )
    orchestrator._client = cast("GatewayClient", _ObservationClient(client, execution))
