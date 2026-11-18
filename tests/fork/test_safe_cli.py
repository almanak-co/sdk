"""The manual fork command installs generated permissions before reporting readiness."""

import json
import signal
import socket

import pytest

from almanak.connectors.aave_v4.models import LendingConfig
from almanak.connectors.aave_v4.permission_adapter import PermissionManifest, verify
from almanak.gateway.fork_execution import ForkGateway
from almanak.strategy import StrategyDefinition
from tests.fork.support import CLI, processes
from tests.fork.test_safe_recovery import STRATEGY, one_safe_config


def available_port():
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        return listener.getsockname()[1]


@pytest.mark.fork
def test_manual_safe_fork_prepares_generated_owner_permissions(request, tmp_path):
    author = tmp_path / "config.json"
    author.write_text(one_safe_config().model_dump_json())
    output = tmp_path / "deployment"
    port = available_port()
    command = [
        CLI,
        "service",
        "fork",
        "--strategy",
        str(STRATEGY),
        "--config",
        str(author),
        "--env-file",
        str(request.config.getoption("--fork-env").resolve()),
        "--output",
        str(output),
        "--port",
        str(port),
    ]
    with processes(tmp_path) as launch:
        running = launch(command, ready="fork_ready")
        verify_prepared_workspace(output, port)
        running.send_signal(signal.SIGINT)
        assert running.wait(timeout=20) == 0


def verify_prepared_workspace(output, port):
    config = LendingConfig.model_validate_json((output / "deployment.json").read_text())
    manifest = PermissionManifest.model_validate_json(
        (output / "permissions/manifest.json").read_text()
    )
    assert manifest.source_hash == StrategyDefinition.read(STRATEGY).sha256
    assert manifest.partitions[0].transactions
    backend = ForkGateway(config, f"http://127.0.0.1:{port}", output / "fork-keys.json")
    try:
        result = verify(config, manifest, backend)
        assert "quotas" not in result["partitions"][0]
    finally:
        backend.close()
    assert json.loads((output / "permission-installation.json").read_text())
