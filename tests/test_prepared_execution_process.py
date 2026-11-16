"""Replace actual gateway/runtime processes after frozen deployment creation.

The gateway launch uses a non-signing test backend. This proves orchestration
and persistence, not production provider identity or protocol execution.
"""

import json
import selectors
import subprocess
import sys
import time
from contextlib import contextmanager

from almanak.state import Journal
from tests.test_bootstrap_process import free_port
from tests.test_prepared_gateway_cli import archive as archive
from tests.test_prepared_gateway_cli import case as case
from tests.test_prepared_gateway_cli import deployment as deployment
from tests.test_prepared_gateway_cli import launch as launch
from tests.test_prepared_gateway_cli import rpc as rpc
from tests.test_prepared_gateway_cli import setup as setup

GATEWAY = """
import sys
from pathlib import Path
from types import SimpleNamespace
from almanak.gateway import prepared_cli
from almanak.capabilities import CapabilityCodec
from almanak import runtime
claim_root = Path(sys.argv[1])
Path.home = staticmethod(lambda: claim_root)
sys.argv = [sys.argv[0], *sys.argv[2:]]
def forbidden(*args, **kwargs):
    raise AssertionError("this process proof cannot sign, broadcast, or repeat author hooks")
CapabilityCodec.evaluate = forbidden
runtime.strategy_capabilities = forbidden
runtime.validate_strategy_config = forbidden
prepared_cli.production_routes = lambda *a, **kw: SimpleNamespace(
    sign=forbidden, broadcast=forbidden, audit_history=lambda _: None, close=lambda: None
)
from almanak.cli import main
raise SystemExit(main())
"""

RUNTIME = """
import sys
from pathlib import Path
from almanak import runtime
from almanak.configuration.admission import PreparedDeployment
def forbidden(*args, **kwargs):
    raise AssertionError("runtime creation and resume must reuse retained validation")
runtime.validate_strategy_config = forbidden
record = PreparedDeployment.model_validate_json(Path(sys.argv[3]).read_text())
starting = sys.argv[4] == "create"
runner = runtime.Runtime(
    sys.argv[1], Path(sys.argv[2]),
    create=record.configuration() if starting else None,
    strategy=record.preparation.prepared.strategy if starting else None,
)
try:
    print(runner.journal.load().deployment_id, flush=True)
    sys.stdin.readline()
finally:
    runner.close()
"""


def next_line(process):
    with selectors.DefaultSelector() as selector:
        selector.register(process.stdout, selectors.EVENT_READ)
        assert selector.select(15), "process did not report readiness"
    line = process.stdout.readline()
    assert line, process.stderr.read()
    return line.strip()


@contextmanager
def gateway(launch, tmp_path):
    port = free_port()
    arguments = [
        "service",
        "gateway",
        "--gateway-config",
        str(launch.gateway_config),
        "--preparation-file",
        str(launch.preparation_file),
        "--database",
        str(launch.database),
        "--production",
        "--env-file",
        str(launch.env_file),
        "--auth-token-file",
        str(launch.auth_token_file),
        "--port",
        str(port),
        "--lease-seconds",
        "2",
    ]
    process = subprocess.Popen(
        [sys.executable, "-c", GATEWAY, str(tmp_path / "claims"), *arguments],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        assert json.loads(next_line(process))["event"] == "gateway_ready"
        yield process, f"http://127.0.0.1:{port}"
    finally:
        if process.poll() is None:
            process.kill()
        process.communicate(timeout=10)


def runtime(launch, endpoint, mode):
    return subprocess.Popen(
        [
            sys.executable,
            "-c",
            RUNTIME,
            endpoint,
            str(launch.auth_token_file),
            str(launch.preparation_file),
            mode,
        ],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )


def test_both_processes_restart_from_one_durable_frozen_preparation(deployment, launch, tmp_path):
    with gateway(launch, tmp_path) as (service, endpoint):
        coordinator = runtime(launch, endpoint, "create")
        try:
            identity = next_line(coordinator)
            journal = Journal(deployment.database)
            first = journal.load()
            retained = journal.configuration_preparation()
            assert first.deployment_id == identity
            assert retained == deployment.retained
            coordinator.kill()
            coordinator.wait(timeout=10)
            service.kill()
            service.wait(timeout=10)
        finally:
            if coordinator.poll() is None:
                coordinator.kill()
            coordinator.communicate(timeout=10)
    # Process death releases OS locks; the existing durable lease must expire.
    time.sleep(2.1)
    with gateway(launch, tmp_path) as (_, endpoint):
        coordinator = runtime(launch, endpoint, "resume")
        try:
            assert next_line(coordinator) == identity
            stdout, stderr = coordinator.communicate("\n", timeout=10)
            assert coordinator.returncode == 0, stderr
        finally:
            if coordinator.poll() is None:
                coordinator.kill()
                coordinator.communicate(timeout=10)
    assert journal.load() == first
    assert journal.configuration_preparation() == retained
    assert first.pending is None and not first.initialized
