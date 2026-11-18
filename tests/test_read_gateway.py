"""A read-only gateway process serves commands that have no supervised workspace."""

from pathlib import Path

import pytest

from almanak.configuration.bootstrap import resolve_configuration
from almanak.configuration.read_gateway import ReadGateway
from almanak.connectors.support import RunMode
from almanak.supervisor_process import running
from tests.test_aave_resolution import archive as archive
from tests.test_bootstrap_gateway import rpc as rpc
from tests.test_bootstrap_gateway import setup as setup
from tests.test_resolution_context import context as context
from tests.test_resolution_context import operator as operator
from tests.test_resolution_context import revalidate


@pytest.fixture
def operator_file(setup, tmp_path):
    path = tmp_path / "operator.json"
    path.write_text(setup.operator.model_dump_json())
    return path


def test_read_gateway_resolves_then_stops_and_retains_no_secret(
    setup, operator_file, tmp_path, monkeypatch
):
    # The service runs from another working directory, so a relative private
    # directory must be resolved before its paths are handed over.
    monkeypatch.chdir(tmp_path)
    private = tmp_path / "private"
    gateway = ReadGateway(
        Path("private"), operator_file, setup.context, setup.env_file, RunMode.FORK
    )
    with gateway as client:
        record = gateway.process
        assert running(record)
        assert gateway.directory == private
        result = resolve_configuration(setup.path, setup.context, client)
        assert result.markets[0].resolved.reserve == setup.source
        token = (private / "gateway-auth.token").read_text()
    assert not running(record)
    assert gateway.process is None and gateway.client is None
    context_file = private / "read-context.json"
    for path in (context_file, private / "gateway-auth.token"):
        assert path.stat().st_mode & 0o777 == 0o600
    assert "must-not-appear" not in context_file.read_text()
    assert setup.rpc.endpoint not in context_file.read_text()
    assert (private / "logs/read-gateway.log").exists()
    with ReadGateway(private, operator_file, setup.context, setup.env_file, RunMode.FORK):
        assert (private / "gateway-auth.token").read_text() == token


def test_read_gateway_reports_a_refused_start_and_leaves_no_process(setup, operator_file, tmp_path):
    context = revalidate(setup.context, operator_digest="b" * 64)
    private = tmp_path / "private"
    gateway = ReadGateway(private, operator_file, context, setup.env_file, RunMode.FORK)
    with pytest.raises(RuntimeError, match="exited during start"):
        with gateway:
            pass
    assert gateway.process is None and gateway.client is None
    assert not setup.rpc.calls
    log = (tmp_path / "private/logs/read-gateway.log").read_text()
    assert "differs from the selected operator" in log
