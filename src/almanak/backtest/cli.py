"""Provision, run and reproduce connector historical models without a fork."""

import hashlib
import json
import os
import secrets
import select
import subprocess
import sys
import time
from pathlib import Path
from typing import TextIO

from almanak.backtest.engine import Journal, Simulator, sync_directory
from almanak.backtest.evm import EvmHistory
from almanak.backtest.gateway import GatewayConfig
from almanak.backtest.history import Capture, HistoricalReader
from almanak.backtest.strategy_prices import gateway_authority
from almanak.connectors.composition import (
    connector_for,
    connector_from_wire,
    historical_manifest_from_json,
)
from almanak.gateway.auth import read_token
from almanak.gateway.ethereum import DataError
from almanak.models import Model
from almanak.runtime import strategy_capabilities, validate_strategy_config
from almanak.sandbox import readable_paths, validate_key_path
from almanak.triggers import IntervalSelection
from almanak.worker_session import preloaded_workers


def sdk_digest() -> str:
    root = Path(__file__).parents[1]
    digest = hashlib.sha256()
    for path in sorted(root.rglob("*.py")):
        digest.update(path.relative_to(root).as_posix().encode())
        digest.update(path.read_bytes())
    import importlib.metadata

    for package in ("pydantic", "httpx", "eth-abi", "eth-utils"):
        digest.update(f"{package}={importlib.metadata.version(package)}".encode())
    return digest.hexdigest()


class GatewayProcess:
    def __init__(self, directory: Path, env_file: Path | None):
        self.directory = directory.resolve()
        self.env_file = env_file.resolve() if env_file else None
        self.process: subprocess.Popen[str] | None = None
        self.log: TextIO | None = None

    def __enter__(self):
        arguments = [
            sys.executable,
            "-m",
            "almanak.backtest.gateway",
            "--run",
            str(self.directory),
        ]
        if self.env_file:
            arguments.extend(("--env-file", str(self.env_file)))
        self.log = (self.directory / "gateway.log").open("a")
        self.process = subprocess.Popen(
            arguments, stdout=subprocess.PIPE, stderr=self.log, text=True
        )
        try:
            assert self.process.stdout is not None
            if not select.select([self.process.stdout], [], [], 30)[0]:
                raise RuntimeError("historical gateway did not become ready")
            payload = json.loads(self.process.stdout.readline())
            if payload.get("event") != "historical_gateway_ready":
                raise RuntimeError("historical gateway returned invalid readiness")
            self.endpoint = f"http://127.0.0.1:{payload['port']}"
            return self
        except BaseException:
            self.__exit__()
            raise

    def __exit__(self, *_):
        if self.process is not None:
            self.process.terminate()
            try:
                self.process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait()
            if self.process.stdout:
                self.process.stdout.close()
        if self.log:
            self.log.close()


def write_json(path: Path, value: Model | dict):
    body = value.model_dump(mode="json") if isinstance(value, Model) else value
    temporary = path.with_suffix(path.suffix + ".tmp")
    with temporary.open("w") as output:
        output.write(json.dumps(body, indent=2) + "\n")
        output.flush()
        os.fsync(output.fileno())
    temporary.replace(path)
    sync_directory(path.parent)


def historical(config):
    return connector_for(config).backtest("historical_path")


def provision(directory, settings, config):
    contracts = historical(config).contracts(settings, config)
    # A public root already retained resolution.json here; a provisioned run
    # still refuses to be overwritten through its exclusive token creation.
    directory.mkdir(parents=True, exist_ok=True)
    sync_directory(directory.parent)
    token = directory / "gateway-auth.token"
    validate_key_path(token, readable_paths())
    with token.open("x") as output:
        os.chmod(token, 0o600)
        output.write(secrets.token_hex(32))
    write_json(
        directory / "history.json",
        gateway_authority(
            settings, GatewayConfig(providers=settings.history, contracts=tuple(contracts))
        ),
    )


def open_model(gateway, config, settings, strategy, baselines=(), authority=None):
    token = read_token(gateway.directory / "gateway-auth.token")
    histories = {}
    try:
        for binding in settings.history:
            reader = HistoricalReader(binding.chain, gateway.endpoint, token)
            histories[binding.chain] = EvmHistory(reader)
        return historical(config).model(config, settings, strategy, histories, baselines, authority)
    except BaseException:
        for history in histories.values():
            history.reader.__exit__()
        raise


def close_model(model):
    for history in model.histories.values():
        history.reader.__exit__()


def create(args):
    import time

    started = time.perf_counter()
    report = create_run(args)
    report["end_to_end_seconds"] = time.perf_counter() - started
    write_json(args.output.resolve() / "report.json", report)
    return report


def create_run(args):
    from almanak.backtest.inputs import selected

    selection = selected(args)
    directory = args.output.resolve()
    env_file = getattr(args, "env_file", None)
    if selection.market_model == "economic":
        author, settings = selection.resolve(directory, env_file)
        return selection.connector.backtest("economic").create(args, author, settings)
    if getattr(args, "dataset", None) is not None:
        raise ValueError("historical_path does not consume a prepared --dataset")
    if env_file is None:
        raise ValueError("historical_path requires an explicit --env-file")
    author, settings = selection.resolve(directory, env_file)
    return historical_run(args, selection, author, settings, directory)


def historical_run(args, selection, author, settings, directory):
    from almanak.backtest.inputs import HarnessSelection

    adapter = selection.connector.backtest("historical_path")
    interval = IntervalSelection(
        configured=author.decision_trigger,
        override_seconds=settings.schedule.decision_interval_override_seconds,
    )
    config = adapter.parameters(author, settings, interval)
    strategy = selection.strategy
    if strategy is None:
        raise ValueError("backtesting requires a strategy")
    initial = adapter.initial(settings, config, interval)
    for binding in settings.history:
        adapter.capability(binding.chain).require(settings.schedule.start)
    validation = author if isinstance(selection, HarnessSelection) else config
    validate_strategy_config(strategy, validation)
    validate_historical_dependencies(strategy, config, settings)
    authority = strategy_capabilities(strategy, config)
    provision(directory, settings, config)
    with GatewayProcess(directory, args.env_file) as gateway:
        model = open_model(gateway, config, settings, strategy, authority=authority)
        try:
            preflight(directory, model, initial)
            baselines = adapter.baselines(model, initial)
            model.baselines = baselines
            manifest = adapter.manifest_type(
                sdk_digest=sdk_digest(),
                config=config,
                settings=settings,
                strategy=strategy,
                capabilities=model.capabilities,
                authority=model.authority,
                baselines=baselines,
                initial=initial,
                interval=interval,
                cadence_fidelity=interval.report()["cadence_fidelity"],
                resolution_facts=selection.resolution_facts,
                resolution_identity=selection.resolution_identity,
            )
            write_json(directory / "manifest.json", manifest)
            journal = Journal(directory / "state.sqlite", adapter.state_type)
            journal.create(initial)
            return execute(directory, journal, model)
        finally:
            close_model(model)


def preflight(directory, model, initial):
    for history in model.histories.values():
        history.floor(initial.schedule.start)
        history.floor(initial.schedule.end)
    write_json(
        directory / "preflight.json",
        {
            "decision_interval": initial.interval.report(),
            "nominal_decision_opportunities": initial.schedule.opportunities(
                initial.interval.effective.seconds
            ),
            "coverage": "window_boundaries_verified; market inputs checked at each consumed anchor",
            "window": initial.schedule.model_dump(mode="json"),
        },
    )


def retained_manifest(directory):
    """The frozen implementation and, for public roots, the replayed market facts."""
    from almanak.backtest.inputs import require_facts

    manifest = historical_manifest_from_json((directory / "manifest.json").read_text())
    if manifest.sdk_digest != sdk_digest():
        raise ValueError("retained run requires the captured implementation")
    require_facts(directory, manifest)
    return manifest


def resume(args):
    directory = args.run.resolve()
    wire = json.loads((directory / "manifest.json").read_text())
    binding = connector_from_wire(wire["config"]).backtest(wire["settings"]["market_model"])
    if wire["settings"]["market_model"] == "economic":
        return binding.resume(args)
    manifest = retained_manifest(directory)
    journal = Journal(directory / "state.sqlite", historical(manifest.config).state_type)
    retained = journal.load()
    if retained.interval != manifest.interval or retained.schedule != manifest.settings.schedule:
        raise ValueError("resume cannot change the captured interval selection or window")
    with GatewayProcess(directory, args.env_file) as gateway:
        model = open_model(
            gateway,
            manifest.config,
            manifest.settings,
            manifest.strategy,
            manifest.baselines,
            manifest.authority,
        )
        try:
            if model.capabilities != manifest.capabilities:
                raise ValueError("connector historical capability changed")
            journal = Journal(directory / "state.sqlite", historical(manifest.config).state_type)
            return execute(directory, journal, model)
        finally:
            close_model(model)


def reproduce(args):
    directory = args.run.resolve()
    wire = json.loads((directory / "manifest.json").read_text())
    binding = connector_from_wire(wire["config"]).backtest(wire["settings"]["market_model"])
    if wire["settings"]["market_model"] == "economic":
        return binding.reproduce(args)
    manifest = retained_manifest(directory)
    # A separate journal preserves the original run, its pending work and evidence.
    path = directory / ("reproduce-" + secrets.token_hex(4) + ".sqlite")
    journal = Journal(path, historical(manifest.config).state_type)
    journal.create(manifest.initial)
    with GatewayProcess(directory, None) as gateway:
        model = open_model(
            gateway,
            manifest.config,
            manifest.settings,
            manifest.strategy,
            manifest.baselines,
            manifest.authority,
        )
        try:
            simulator = Simulator(journal, model)
            with preloaded_workers():
                result = simulator.run()
            expected = Journal(
                directory / "state.sqlite", historical(manifest.config).state_type
            ).load()
            if result != expected:
                raise ValueError("offline reproduction differs from the recorded state")
            report = {"reproduced": True, "network": "disabled", "state": str(path)}
            write_json(directory / "reproduction.json", report)
            return report
        finally:
            close_model(model)


def execute(directory, journal, model):
    started = time.monotonic()
    try:
        model.validate_state(journal.load())
        with preloaded_workers():
            state = Simulator(journal, model).run()
        report = result_report(state, model)
        report["elapsed_seconds"] = time.monotonic() - started
        report["captured_rpc_requests"] = Capture(directory / "observations.sqlite").count()
        write_json(directory / "report.json", report)
        return report
    except (ValueError, RuntimeError, DataError) as error:
        write_json(directory / "failure.json", {"error": str(error), "state": str(journal.path)})
        raise


def result_report(state, model):
    return {
        **historical(model.config).report(state, model),
        "decision_interval": state.interval.report(),
        "decision_schedule": state.decision_schedule.model_dump(mode="json"),
        "evaluations": state.evaluations,
    }


def run_with_progress(args, *, on_progress=None, result_sink=None):
    """Run a backtest command with the same progress contract as the terminal."""
    operations = {
        "backtest": create,
        "backtest-prepare": prepare,
        "backtest-resume": resume,
        "backtest-reproduce": reproduce,
    }
    from almanak.backtest.progress_cli import execute

    return execute(args, operations[args.product], callback=on_progress, result_sink=result_sink)


def prepare(args):
    from almanak.backtest.inputs import selected

    selection = selected(args)
    if selection.market_model != "economic":
        raise ValueError("backtest-prepare requires explicit economic settings")
    return selection.connector.backtest("economic").prepare(args)


def print_result(result):
    print(json.dumps(result, indent=2), flush=True)


def run(args):
    from almanak.presentation import emit

    if args.product == "backtest-recent":
        from almanak.backtest.recent import command

        return command(args)
    if args.product in ("backtest", "backtest-prepare"):
        check_inputs(args)
    run_with_progress(args, result_sink=lambda result: emit(result, args))
    return 0


def check_inputs(args):
    """Reject invalid public inputs before creating a run or contacting providers."""
    from almanak.checks import selected_paths
    from almanak.configuration.unresolved import read_inputs
    from almanak.typed_parameters import model_fields

    config, strategy = selected_paths(args)
    read_inputs(config, fields=model_fields(strategy))
    if getattr(args, "gateway_config", None) is None:
        raise ValueError("backtesting requires --gateway-config with operator settings")


def validate_historical_dependencies(strategy, config, settings):
    from almanak.backtest.dependencies import DataDependencies
    from almanak.backtest.strategy_prices import strategy_bindings
    from almanak.runtime import strategy_worker

    request = connector_for(config).codec.request_type("dependencies")(
        strategy=strategy, config=config
    )
    dependencies = DataDependencies.model_validate_json(strategy_worker(request))
    if dependencies.markets or dependencies.assets:
        raise ValueError("historical_path does not support extra strategy markets or assets")
    # Resolution folded the declared series into the settings; they must still agree.
    if set(dependencies.prices) != set(strategy_bindings(settings)):
        raise ValueError("strategy price series differ from the resolved backtest settings")
