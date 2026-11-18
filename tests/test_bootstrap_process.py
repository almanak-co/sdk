"""Actual runtime and gateway replacement during retained archive resolution."""

import json
import selectors
import socket
import subprocess
import sys
import threading
from contextlib import contextmanager

import pytest

from almanak.configuration.bootstrap import read_progress
from tests.test_bootstrap_gateway import (
    archive as archive,
)
from tests.test_bootstrap_gateway import (
    context as context,
)
from tests.test_bootstrap_gateway import (
    operator as operator,
)
from tests.test_bootstrap_gateway import (
    rpc as rpc,
)
from tests.test_bootstrap_gateway import (
    setup as setup,
)

RUNTIME = """
import sys
from pathlib import Path
from almanak.configuration.bootstrap import resolve_configuration
from almanak.configuration.resolution import ReadContext
from almanak.gateway.client import GatewayClient
context = ReadContext.model_validate_json(Path(sys.argv[1]).read_text())
client = GatewayClient(sys.argv[3], Path(sys.argv[4]))
try:
    result = resolve_configuration(Path(sys.argv[2]), context, client)
    print(result.digest, flush=True)
finally:
    client.close()
"""


def free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


@contextmanager
def gateway_process(setup, tmp_path, *, mode="fork"):
    operator_path, context_path = tmp_path / "operator.json", tmp_path / "context.json"
    operator_path.write_text(setup.operator.model_dump_json())
    context_path.write_text(setup.context.model_dump_json())
    port = free_port()
    command = [
        sys.executable,
        "-m",
        "almanak.cli",
        "service",
        "gateway",
        "--gateway-config",
        str(operator_path),
        "--read-context",
        str(context_path),
        "--env-file",
        str(setup.env_file),
        "--auth-token-file",
        str(setup.token_file),
        f"--{mode}",
        "--port",
        str(port),
    ]
    process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    try:
        with selectors.DefaultSelector() as selector:
            selector.register(process.stdout, selectors.EVENT_READ)
            assert selector.select(10), "read gateway did not report readiness"
        line = process.stdout.readline()
        assert line, process.stderr.read()
        ready = json.loads(line)
        assert ready["event"] == "gateway_ready"
        assert ready["authority"] == "read_only"
        yield process, context_path, f"http://127.0.0.1:{port}"
    finally:
        if process.poll() is None:
            process.kill()
        process.communicate(timeout=10)


def start_runtime(setup, context_path, endpoint):
    return subprocess.Popen(
        [
            sys.executable,
            "-c",
            RUNTIME,
            str(context_path),
            str(setup.path),
            endpoint,
            str(setup.token_file),
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )


def test_runtime_death_and_gateway_replacement_preserve_anchor_then_publish_once(setup, tmp_path):
    entered, release = threading.Event(), threading.Event()

    def pause_read(body, reply):
        if body["method"] == "eth_getCode" and not entered.is_set():
            entered.set()
            assert release.wait(10)
        return reply

    setup.rpc.mutation = pause_read
    with gateway_process(setup, tmp_path) as (gateway, context_path, endpoint):
        runtime = start_runtime(setup, context_path, endpoint)
        try:
            assert entered.wait(10), "runtime did not reach bounded archive resolution"
            progress = read_progress(setup.path)
            assert str(progress.checkpoints[0].reference) == setup.block.hash
            assert progress.result is progress.result_digest is None
            runtime.kill()
            runtime.wait(timeout=10)
            gateway.kill()
            gateway.wait(timeout=10)
        finally:
            release.set()
            if runtime.poll() is None:
                runtime.kill()
            runtime.communicate(timeout=10)
    setup.rpc.mutation = None
    setup.rpc.calls.clear()
    with gateway_process(setup, tmp_path) as (_, context_path, endpoint):
        resumed = start_runtime(setup, context_path, endpoint)
        stdout, stderr = resumed.communicate(timeout=15)
        assert resumed.returncode == 0, stderr
        result = read_progress(setup.path).result
        assert result is not None and result.digest == stdout.strip()
        assert result.markets[0].resolved.reserve == setup.source
        assert not any(c["params"] == ["latest", False] for c in setup.rpc.calls)
        before = setup.path.read_bytes()
        repeated = start_runtime(setup, context_path, endpoint)
        stdout, stderr = repeated.communicate(timeout=15)
        assert repeated.returncode == 0, stderr
        assert stdout.strip() == result.digest
        assert setup.path.read_bytes() == before


@pytest.mark.parametrize(
    "flag", ["--accounts", "--key-file", "--fork-key-file", "--database", "--fork-url"]
)
def test_cli_refuses_execution_inputs_in_read_bootstrap(tmp_path, flag):
    args = [
        sys.executable,
        "-m",
        "almanak.cli",
        "service",
        "gateway",
        "--gateway-config",
        str(tmp_path / "operator.json"),
        "--read-context",
        str(tmp_path / "context.json"),
        "--env-file",
        str(tmp_path / "gateway.env"),
        "--auth-token-file",
        str(tmp_path / "token"),
        "--port",
        "9999",
        "--fork",
        flag,
        "unused",
    ]
    result = subprocess.run(args, capture_output=True, text=True, timeout=10)
    assert result.returncode != 0
    assert "No such file" not in result.stderr
    assert "read bootstrap cannot load" in result.stderr or "not allowed" in result.stderr
