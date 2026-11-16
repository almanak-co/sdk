"""Kill both real processes after hooks are retained and before policy acknowledgement."""

import selectors
import subprocess
import sys

from almanak.configuration.prepared import read_preparation
from tests.test_bootstrap_process import gateway_process
from tests.test_prepared_configuration import archive as archive
from tests.test_prepared_configuration import case as case
from tests.test_prepared_configuration import rpc as rpc
from tests.test_prepared_configuration import setup as setup

RUNTIME = """
import sys
from pathlib import Path
from almanak import checks
from almanak.configuration.bootstrap import ResolutionProgress
from almanak.configuration.prepared import prepare_production
from almanak.configuration.unresolved import read_inputs
from almanak.gateway.client import GatewayClient
from almanak.strategy import StrategyDefinition
document = read_inputs(Path(sys.argv[1]))
progress = ResolutionProgress.model_validate_json(Path(sys.argv[2]).read_text())
definition = StrategyDefinition.model_validate_json(Path(sys.argv[3]).read_text())
client = GatewayClient(sys.argv[5], Path(sys.argv[6]))
if sys.argv[7] == "pause":
    original = client.request
    def stopped(method, **fields):
        print("policy_request_ready", flush=True)
        sys.stdin.readline()
        return original(method, **fields)
    client.request = stopped
else:
    def forbidden(*args, **kwargs):
        raise AssertionError("retained hooks must not execute again after process replacement")
    checks.strategy_worker = forbidden
    checks.strategy_capabilities = forbidden
try:
    result = prepare_production(Path(sys.argv[4]), document, definition, progress, client)
    print(result.prepared_digest, flush=True)
finally:
    client.close()
"""


def launch(case, tmp_path, endpoint, phase):
    inputs, resolution, strategy = [
        tmp_path / name for name in ("inputs.yaml", "resolution.json", "strategy.json")
    ]
    inputs.write_text(case.document.inputs.model_dump_json())
    resolution.write_text(case.progress.model_dump_json())
    strategy.write_text(case.definition.model_dump_json())
    return subprocess.Popen(
        [
            sys.executable,
            "-c",
            RUNTIME,
            str(inputs),
            str(resolution),
            str(strategy),
            str(case.path),
            endpoint,
            str(case.setup.token_file),
            phase,
        ],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )


def test_runtime_and_gateway_replacement_reuses_frozen_hooks(case, tmp_path):
    before_calls = len(case.setup.rpc.calls)
    with gateway_process(case.setup, tmp_path, mode="production") as (gateway, _, endpoint):
        runtime = launch(case, tmp_path, endpoint, "pause")
        try:
            with selectors.DefaultSelector() as selector:
                selector.register(runtime.stdout, selectors.EVENT_READ)
                assert selector.select(20), "preparation did not reach its retained policy request"
            line = runtime.stdout.readline()
            assert line.strip() == "policy_request_ready", runtime.stderr.read()
            retained = read_preparation(case.path)
            assert retained.policy is None
            runtime.kill()
            runtime.wait(timeout=10)
            gateway.kill()
            gateway.wait(timeout=10)
        finally:
            if runtime.poll() is None:
                runtime.kill()
            runtime.communicate(timeout=10)
    with gateway_process(case.setup, tmp_path, mode="production") as (_, _, endpoint):
        resumed = launch(case, tmp_path, endpoint, "resume")
        stdout, stderr = resumed.communicate(timeout=20)
        assert resumed.returncode == 0, stderr
        assert stdout.strip() == retained.prepared_digest
        completed = read_preparation(case.path)
        assert completed.prepared == retained.prepared
        assert completed.policy.execution.gas == (case.policy,)
        before = case.path.read_bytes(), case.path.stat().st_mtime_ns
        repeated = launch(case, tmp_path, endpoint, "resume")
        stdout, stderr = repeated.communicate(timeout=20)
        assert repeated.returncode == 0, stderr
        assert stdout.strip() == retained.prepared_digest
        assert (case.path.read_bytes(), case.path.stat().st_mtime_ns) == before
    assert len(case.setup.rpc.calls) == before_calls
