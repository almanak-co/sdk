"""Actual runtime, gateway and sandbox processes over a synthetic archive server."""

import json
import os
import signal
import subprocess
import sys
from types import SimpleNamespace

import pytest
import yaml

from almanak.backtest.engine import Journal
from almanak.configuration.yaml import ConfigurationError
from almanak.connectors.aave_v4.backtest import AaveState
from tests.backtest_support import (
    LEGACY,
    ROOT,
    Archive,
    archive_server,
    converted_config,
)


@pytest.fixture
def archive(tmp_path):
    data = Archive()
    server, thread = archive_server(data)
    config, operator, env_file = converted_config(tmp_path, data, server)
    yield data, config, operator, env_file
    server.shutdown()
    server.server_close()
    thread.join()


def run_cli(arguments):
    return subprocess.run(
        [
            sys.executable,
            "-c",
            "from almanak.cli import main; raise SystemExit(main())",
            *arguments,
        ],
        cwd=ROOT,
        capture_output=True,
        text=True,
        # Reproducing a run at its configured cadence takes minutes on a loaded
        # two-core runner; the bound only guards against a hung process.
        timeout=300,
    )


def test_actual_aave_strategy_reproduces_offline(archive, tmp_path):
    from almanak.backtest.cli import create

    data, config, operator, env_file = archive
    directory = tmp_path / "run"
    report = create(
        SimpleNamespace(
            strategy=ROOT / "strategies/aave_v4_weth_supply/strategy.py",
            config=config,
            backtest_config=config.with_name("backtest.yaml"),
            gateway_config=operator,
            env_file=env_file,
            output=directory,
        )
    )
    assert report["closed"] and report["completed_steps"] == 12
    before = len(data.calls)
    reproduced = run_cli(["backtest", "reproduce", "--run", str(directory)])
    assert reproduced.returncode == 0, reproduced.stderr
    assert json.loads(reproduced.stdout)["reproduced"]
    assert len(data.calls) == before
    assert {method for method, _ in data.calls} <= {
        "eth_chainId",
        "eth_getBlockByNumber",
        "eth_call",
        "eth_getCode",
        "eth_getStorageAt",
    }


FAULT_DRIVER = """
import json, os, signal, sys
from pathlib import Path
from almanak.backtest.cli import GatewayProcess
from almanak.backtest.engine import Simulator
from almanak.cli import main
stage = sys.argv.pop(1)
pidfile = Path(sys.argv.pop(1))
original_gateway = GatewayProcess.__enter__
def enter(self):
    result = original_gateway(self)
    pidfile.write_text(str(self.process.pid))
    return result
GatewayProcess.__enter__ = enter
original_run = Simulator.run
def run(self):
    def fault(current):
        state = self.journal.load()
        if current == stage and len(state.completed) >= 1:
            os._exit(74)
    self.fault = fault
    return original_run(self)
Simulator.run = run
raise SystemExit(main())
"""


@pytest.mark.parametrize(
    "stage", ["pending_commit", "effects", "effects_commit", "outcome", "delivery_commit"]
)
def test_runtime_death_and_gateway_replacement_resume_once(archive, tmp_path, stage):
    _, config, operator, env_file = archive
    directory = tmp_path / "run"
    pidfile = tmp_path / "gateway.pid"
    command = [
        sys.executable,
        "-c",
        FAULT_DRIVER,
        stage,
        str(pidfile),
        "backtest",
        "run",
        "--strategy",
        str(ROOT / "strategies/aave_v4_weth_supply/strategy.py"),
        "--config",
        str(config),
        "--backtest-config",
        str(config.with_name("backtest.yaml")),
        "--gateway-config",
        str(operator),
        "--env-file",
        str(env_file),
        "--output",
        str(directory),
    ]
    try:
        failed = subprocess.run(command, cwd=ROOT, capture_output=True, text=True, timeout=90)
        assert failed.returncode == 74, failed.stderr
    finally:
        if pidfile.exists():
            try:
                os.kill(int(pidfile.read_text()), signal.SIGTERM)
            except ProcessLookupError:
                pass
    result = run_cli(["backtest", "resume", "--run", str(directory), "--env-file", str(env_file)])
    assert result.returncode == 0, result.stderr
    state = Journal(directory / "state.sqlite", AaveState).load()
    assert len(state.completed) == state.delivered == 12
    assert all(p.payload.shares == p.payload.allowance == 0 for p in state.portfolio.positions)
    assert all(p.payload.supplied == 10**16 for p in state.portfolio.positions)
    reproduced = run_cli(["backtest", "reproduce", "--run", str(directory)])
    assert reproduced.returncode == 0, reproduced.stderr


def test_requested_historical_borrowing_is_rejected_before_provider_access(archive, tmp_path):
    from almanak.backtest.cli import create
    from almanak.backtest.types import AvailabilityError
    from almanak.strategy import StrategyDefinition
    from scripts.legacy_configuration.author import read_author
    from scripts.legacy_configuration.backtest import read_backtest

    data, _, _, env = archive
    path = LEGACY / "config.yaml"
    author = read_author(path)
    settings = read_backtest(LEGACY / "backtest.yaml", author)
    strategy = StrategyDefinition.read(ROOT / "strategies/aave_v4_weth_supply/strategy.py")
    before = len(data.calls)
    output = tmp_path / "unsupported-debt"
    with pytest.raises(AvailabilityError, match="unsupported backtest action"):
        create(
            SimpleNamespace(
                strategy=strategy.filename,
                config=path,
                backtest_config=path.with_name("backtest.yaml"),
                env_file=env,
                output=output,
                _backtest_inputs=(author, settings, strategy),
            )
        )
    assert len(data.calls) == before
    assert not output.exists()


def test_historical_rejects_prepared_dataset_before_provider_access(archive, tmp_path):
    from almanak.backtest.cli import create

    data, config, operator, env_file = archive
    output = tmp_path / "run"
    with pytest.raises(ValueError, match="does not consume"):
        create(
            SimpleNamespace(
                config=config,
                backtest_config=config.with_name("backtest.yaml"),
                gateway_config=operator,
                dataset=tmp_path / "data",
                env_file=env_file,
                output=output,
            )
        )
    assert not data.calls and not output.exists()


@pytest.fixture
def public_archive(tmp_path):
    data = Archive()
    server, thread = archive_server(data)
    config, operator, env_file = converted_config(tmp_path, data, server)
    yield data, config, operator, env_file
    server.shutdown()
    server.server_close()
    thread.join()


def tamper_facts(directory):
    """Change one resolved fact and republish consistent digests, as an editor would."""
    from almanak.configuration.bootstrap import ResolutionProgress
    from almanak.gateway.bootstrap import ResolutionResult

    path = directory / "resolution.json"
    raw = json.loads(path.read_text())
    raw["result"]["markets"][0]["resolved"]["asset"]["decimals"] = 6
    result = ResolutionResult.model_validate_json(json.dumps(raw["result"]))
    raw["result_digest"] = result.digest
    path.write_text(ResolutionProgress.model_validate_json(json.dumps(raw)).model_dump_json())


def substitute_resolution_identity(directory):
    """Keep facts exact while replacing the invocation, operator and fund binding."""
    from almanak.configuration.bootstrap import ResolutionProgress
    from almanak.configuration.resolution import ReadContext
    from almanak.gateway.bootstrap import ResolutionResult

    path = directory / "resolution.json"
    raw = json.loads(path.read_text())
    raw["context"]["identity"] = "f" * 32
    raw["context"]["operator_digest"] = "e" * 64
    context = ReadContext.model_validate_json(json.dumps(raw["context"]))
    for binding in (raw["binding"], raw["result"]["binding"]):
        binding["context_digest"] = context.digest
        binding["operator_digest"] = "e" * 64
        binding["accounts"]["bindings"][0]["profile"]["fund"]["address"] = "0x" + "1" * 40
    raw["result"]["anchors"]["context_digest"] = context.digest
    result = ResolutionResult.model_validate_json(json.dumps(raw["result"]))
    raw["result_digest"] = result.digest
    path.write_text(ResolutionProgress.model_validate_json(json.dumps(raw)).model_dump_json())


def test_public_root_resolves_once_then_reproduces_from_retained_facts(public_archive, tmp_path):
    from almanak.configuration.bootstrap import read_progress

    data, config, operator, env_file = public_archive
    directory = tmp_path / "run"
    created = run_cli(
        [
            "backtest",
            "run",
            "--config",
            str(config),
            "--backtest-config",
            str(config.with_name("backtest.yaml")),
            "--gateway-config",
            str(operator),
            "--env-file",
            str(env_file),
            "--strategy",
            str(ROOT / "strategies/aave_v4_weth_supply/strategy.py"),
            "--output",
            str(directory),
        ]
    )
    assert created.returncode == 0, created.stderr
    report = json.loads(created.stdout)
    assert report["closed"] and report["completed_steps"] == 12
    progress = read_progress(directory / "resolution.json")
    assert progress.context.mode == "backtest"
    assert progress.context.anchors[0].at == progress.checkpoints[0].timestamp + 1
    manifest = json.loads((directory / "manifest.json").read_text())
    assert manifest["resolution_facts"] == progress.result.facts_digest
    assert manifest["resolution_identity"] == progress.result.digest
    funds = {f["name"]: f["fund"]["address"] for f in manifest["settings"]["funds"]}
    bindings = manifest["config"]["accounts"]["bindings"]
    assert funds == {b["name"]: b["profile"]["fund"]["address"] for b in bindings}
    assert manifest["settings"]["history"][0]["provider"] == {
        "kind": "rpc_url",
        "url_env": "ARCHIVE_TEST_RPC",
    }
    endpoint = env_file.read_text().split("=")[1].strip()
    assert endpoint not in (directory / "resolution.json").read_text()
    before = len(data.calls)
    reproduced = run_cli(["backtest", "reproduce", "--run", str(directory)])
    assert reproduced.returncode == 0, reproduced.stderr
    assert json.loads(reproduced.stdout)["reproduced"]
    assert len(data.calls) == before
    original_resolution = (directory / "resolution.json").read_text()
    substitute_resolution_identity(directory)
    for command in ("resume", "reproduce"):
        refused = run_cli(["backtest", command, "--run", str(directory)])
        assert refused.returncode != 0
        assert "resolution identity differs" in refused.stderr + refused.stdout
    assert len(data.calls) == before
    (directory / "resolution.json").write_text(original_resolution)
    original_manifest = (directory / "manifest.json").read_text()
    legacy_manifest = json.loads(original_manifest)
    del legacy_manifest["resolution_identity"]
    (directory / "manifest.json").write_text(json.dumps(legacy_manifest))
    refused = run_cli(["backtest", "reproduce", "--run", str(directory)])
    assert refused.returncode != 0
    assert "lacks its retained resolution identity" in refused.stderr + refused.stdout
    (directory / "manifest.json").write_text(original_manifest)
    tamper_facts(directory)
    refused = run_cli(["backtest", "reproduce", "--run", str(directory)])
    assert refused.returncode != 0
    assert "differ from retained reads" in refused.stderr + refused.stdout
    assert len(data.calls) == before


def test_public_root_requires_operator_configuration_and_legacy_roots_are_invalid(
    public_archive, tmp_path
):
    from almanak.backtest.inputs import selected

    data, config, operator, env_file = public_archive
    harness = LEGACY / "config.yaml"
    strategy = ROOT / "strategies/aave_v4_weth_supply/strategy.py"
    with pytest.raises(ValueError, match="requires --gateway-config"):
        selected(
            SimpleNamespace(
                config=config,
                backtest_config=config.with_name("backtest.yaml"),
                strategy=strategy,
            )
        )
    with pytest.raises(ConfigurationError):
        selected(
            SimpleNamespace(
                config=harness,
                backtest_config=harness.with_name("backtest.yaml"),
                strategy=strategy,
                gateway_config=operator,
            )
        )
    selection = selected(
        SimpleNamespace(
            config=config,
            backtest_config=config.with_name("backtest.yaml"),
            strategy=strategy,
            gateway_config=operator,
        )
    )
    with pytest.raises(ValueError, match="requires --env-file"):
        selection.resolve(tmp_path / "unresolved", None)
    assert not data.calls


def test_public_root_continues_incomplete_retained_resolution(public_archive, tmp_path):
    from almanak.backtest.inputs import selected
    from almanak.configuration.bootstrap import ResolutionProgress, publish, read_progress
    from almanak.connectors.support import RunMode

    data, config, operator, env_file = public_archive
    directory = tmp_path / "run"
    directory.mkdir()
    selection = selected(
        SimpleNamespace(
            config=config,
            backtest_config=config.with_name("backtest.yaml"),
            strategy=ROOT / "strategies/aave_v4_weth_supply/strategy.py",
            gateway_config=operator,
        )
    )
    context = selection.document.inputs.context(
        selection.operator, RunMode.BACKTEST, selection.anchors
    )
    publish(directory / "resolution.json", ResolutionProgress(context=context))

    selection.resolve(directory, env_file)

    progress = read_progress(directory / "resolution.json")
    assert progress.context == context
    assert progress.result is not None
    assert data.calls


def test_public_root_rejects_retained_resolution_from_changed_operator(public_archive, tmp_path):
    from almanak.backtest.inputs import selected

    data, config, operator, env_file = public_archive
    directory = tmp_path / "run"
    original = selected(
        SimpleNamespace(
            config=config,
            backtest_config=config.with_name("backtest.yaml"),
            strategy=ROOT / "strategies/aave_v4_weth_supply/strategy.py",
            gateway_config=operator,
        )
    )
    original.resolve(directory, env_file)
    before = len(data.calls)
    original_operator = operator.read_text()
    changed = yaml.safe_load(original_operator)
    changed["networks"][0]["provider"]["url_env"] = "CHANGED_ARCHIVE"
    operator.write_text(yaml.safe_dump(changed))
    replacement = selected(
        SimpleNamespace(
            config=config,
            backtest_config=config.with_name("backtest.yaml"),
            strategy=ROOT / "strategies/aave_v4_weth_supply/strategy.py",
            gateway_config=operator,
        )
    )

    with pytest.raises(ValueError, match="selected operator configuration or mode"):
        replacement.resolve(directory, None)
    assert len(data.calls) == before

    operator.write_text(original_operator)
    changed_config = yaml.safe_load(config.read_text())
    changed_config["strategy"]["decision_interval"] = "10s"
    config.write_text(yaml.safe_dump(changed_config))
    replacement = selected(
        SimpleNamespace(
            config=config,
            backtest_config=config.with_name("backtest.yaml"),
            strategy=ROOT / "strategies/aave_v4_weth_supply/strategy.py",
            gateway_config=operator,
        )
    )
    with pytest.raises(ValueError, match="differs from the authored choices"):
        replacement.resolve(directory, None)
    assert len(data.calls) == before
