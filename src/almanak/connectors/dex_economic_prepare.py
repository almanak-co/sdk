"""Prepare immutable exchange pool inputs through a dedicated read-only gateway."""

import fcntl
import json
import secrets
import time
from collections.abc import Callable
from contextlib import ExitStack
from dataclasses import dataclass
from types import ModuleType

from almanak.backtest import progress
from almanak.backtest.cli import GatewayProcess, write_json
from almanak.backtest.evm import EvmHistory
from almanak.backtest.gateway import GatewayConfig
from almanak.backtest.history import Capture, HistoricalReader
from almanak.backtest.prepared import (
    PROVISIONAL,
    DatasetWriter,
    Preparation,
    acquisition_request,
    require_private_directory,
)
from almanak.backtest.strategy_prices import strategy_requirements
from almanak.connectors.dex_economic_dataset import PoolDataset, targets
from almanak.gateway.auth import read_token
from almanak.triggers import IntervalSelection


def validate_dependencies(args, config, settings):
    from almanak.backtest.inputs import strategy_source
    from almanak.backtest.strategy_prices import (
        declared_prices,
        require_history_chains,
        strategy_bindings,
    )

    require_history_chains(settings)
    if getattr(args, "strategy", None) is None:
        return
    # Resolution folded the declared series into the settings; they must still agree.
    if set(declared_prices(strategy_source(args), config)) != set(strategy_bindings(settings)):
        raise ValueError("strategy price series differ from the resolved backtest settings")


def gateway_config(settings, contracts):
    """The preparation gateway's authority: providers, contracts and declared series reads."""
    from almanak.backtest.strategy_prices import gateway_authority

    return gateway_authority(
        settings, GatewayConfig(providers=settings.history, contracts=contracts)
    )


def open_histories(stack, directory, settings, endpoint):
    histories = {}
    token = read_token(directory / "gateway-auth.token")
    for binding in settings.history:
        reader = HistoricalReader(binding.chain, endpoint, token)
        stack.callback(reader.__exit__)
        histories[binding.chain] = EvmHistory(reader)
    return histories


@dataclass(frozen=True)
class PreparationBinding:
    data: ModuleType
    collection: ModuleType
    book_type: type[PoolDataset]
    parameters: Callable
    validate_policy: Callable
    dependencies: Callable | None = None
    model_identity: Callable | None = None

    def revisions(self, config):
        if self.model_identity is not None:
            return self.model_identity(config)
        return self.data.MODEL, self.data.REVISION

    def selected_parameters(self, author, settings):
        interval = IntervalSelection(
            configured=author.decision_trigger,
            override_seconds=settings.schedule.decision_interval_override_seconds,
        )
        return self.parameters(author, settings, interval)

    def validate_sources(self, args, author, settings):
        from almanak.connectors.composition import connector_for
        from almanak.connectors.support import RunMode, historical_capability

        config = self.selected_parameters(author, settings)
        self.collection.contract_bindings(settings, config)
        self.validate_policy(config, settings)
        connector = connector_for(config)
        for target in targets(config):
            connector.require_target(
                target.fund.chain.chain_id,
                target,
                RunMode.BACKTEST,
                model=self.revisions(config)[0],
            )
            historical_capability(
                connector.support_for(target.fund.chain.chain_id),
                target.fund.chain,
                self.data.CONNECTOR,
                self.revisions(config)[0],
            ).require(settings.schedule.start)
        validate_dependencies(args, config, settings)
        return config

    def dataset_directory(self, args, author, settings):
        return (
            args.output.resolve().parent
            / ".backtest-datasets"
            / self.book_type.identity(self.selected_parameters(author, settings), settings)
        )

    def provision(self, directory, config, settings, resolution_facts=None):
        request = acquisition_request(self.book_type.identity(config, settings), resolution_facts)
        path = directory / "acquisition.json"
        if path.exists():
            if json.loads(path.read_text()) != request:
                raise ValueError(
                    "exchange pool preparation cannot change its sources, window or policy"
                )
        elif any(p.name not in PROVISIONAL for p in directory.iterdir()):
            raise ValueError("exchange pool preparation requires a new or matching directory")
        else:
            write_json(path, request)
        token = directory / "gateway-auth.token"
        if not token.exists():
            with token.open("x") as output:
                token.chmod(0o600)
                output.write(secrets.token_hex(32))

    def authorize_ranges(self, directory, config, settings, env_file):
        write_json(
            directory / "history.json",
            gateway_config(settings, self.collection.contract_bindings(settings, config)),
        )
        with ExitStack() as stack:
            gateway = stack.enter_context(GatewayProcess(directory, env_file))
            histories = open_histories(stack, directory, settings, gateway.endpoint)
            with progress.phase(
                "preparation.boundaries", "Resolving exchange historical block bounds", 1, "phases"
            ):
                boundaries = self.collection.resolve_boundaries(histories, config, settings)
            contracts = self.collection.contract_bindings(settings, config, boundaries)
            if self.dependencies is not None:
                contracts = self.dependencies(histories, config, boundaries, contracts)
        # Canonical headers and verified dependency reads bound the expanded authority.
        write_json(directory / "history.json", gateway_config(settings, contracts))
        return boundaries

    def prepare(self, args):
        directory = args.output.resolve()
        require_private_directory(directory)
        directory.mkdir(parents=True, exist_ok=True)
        with (directory / "acquisition.lock").open("a") as lock:
            try:
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                raise RuntimeError(
                    "another coordinator owns this exchange pool preparation"
                ) from None
            return self.prepare_locked(args)

    def prepare_locked(self, args):
        from almanak.backtest.inputs import selected

        started = time.perf_counter()
        selection = selected(args)
        author, settings = selection.resolve(args.output.resolve(), getattr(args, "env_file", None))
        config = self.validate_sources(args, author, settings)
        directory = args.output.resolve()
        self.provision(directory, config, settings, selection.resolution_facts)
        reused = (directory / "dataset.json").exists()
        if not reused:
            env_file = getattr(args, "env_file", None)
            boundaries = self.authorize_ranges(directory, config, settings, env_file)
            self.acquire(directory, config, settings, env_file, boundaries)
        with progress.phase(
            "preparation.verification", "Verifying exchange pool dataset", 1, "phases"
        ):
            book = self.book_type(directory, config, settings)
            manifest = book.manifest
            book.close()
        capture = Capture(directory / "observations.sqlite")
        try:
            activity = capture.activity()
        finally:
            capture.close()
        report = {
            "dataset": manifest.model_dump(mode="json"),
            "elapsed_seconds": time.perf_counter() - started,
            "provider_activity": activity,
            "cache_state": "completed_dataset" if reused else "cold_or_resumed_acquisition",
        }
        if not reused:
            write_json(directory / "preparation-report.json", report)
        return report

    def acquire(self, directory, config, settings, env_file, boundaries):
        preparation = Preparation(
            model=self.revisions(config)[0],
            acquisition_revision=self.revisions(config)[1],
            policy_digest=self.book_type.identity(config, settings),
            requirements=tuple(self.data.requirement(t, settings) for t in targets(config))
            + strategy_requirements(settings),
        )
        with ExitStack() as stack:
            gateway = stack.enter_context(GatewayProcess(directory, env_file))
            histories = open_histories(stack, directory, settings, gateway.endpoint)
            if self.collection.resolve_boundaries(histories, config, settings) != boundaries:
                raise ValueError("exchange historical boundaries changed after log authorization")
            writer = DatasetWriter(directory, preparation)
            stack.callback(writer.close)
            for target in targets(config):
                self.collection.collect(writer, histories[target.fund.chain], target, settings)
            collect_strategy_prices(writer, histories, settings)
            with progress.phase(
                "preparation.finalization", "Publishing exchange prepared pools", 1, "phases"
            ):
                writer.publish()


def collect_strategy_prices(writer, histories, settings):
    """Each declared series from its warm-up start, through the preparation gateway."""
    from almanak.backtest.prepared import AcquiredRange
    from almanak.backtest.strategy_prices import acquire_series, series_bindings

    start, end = settings.schedule.start, settings.schedule.deadline
    missing = [binding for binding in series_bindings(settings) if writer.missing(binding.key)]
    if not missing:
        return
    with progress.phase(
        "preparation.strategy_prices", "Reading declared pool price series", len(missing), "series"
    ):
        for binding in missing:
            rows, evidence = acquire_series(histories[binding.chain], binding, start, end)
            requirement = writer.preparation.requirement(binding.key)
            writer.commit_range(
                AcquiredRange(
                    key=binding.key,
                    start=requirement.start,
                    end=requirement.end,
                    evidence=tuple(sorted(item.digest for item in evidence)),
                ),
                rows,
                evidence,
            )
            progress.advance()
