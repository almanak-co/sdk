"""Prepare Comet inputs with bounded discovery and immutable range publication."""

import fcntl
import json
import secrets
import time
from contextlib import ExitStack

from almanak.backtest import progress
from almanak.backtest.cli import GatewayProcess, write_json
from almanak.backtest.evm import EvmHistory
from almanak.backtest.history import Capture, HistoricalReader
from almanak.backtest.prepared import (
    PROVISIONAL,
    DatasetWriter,
    Preparation,
    acquisition_request,
    require_private_directory,
)
from almanak.backtest.strategy_prices import strategy_bindings
from almanak.connectors.compound_v3.economic_acquisition import (
    bounds,
    collect_accounting,
    collect_risk,
    contracts,
    discover,
    retained_risk_blocks,
)
from almanak.connectors.compound_v3.economic_data import CONNECTOR, MODEL, REVISION, key
from almanak.connectors.compound_v3.economic_parameters import (
    identity,
    markets,
    parameters,
    requirements,
)
from almanak.connectors.dex_economic_prepare import (
    collect_strategy_prices,
    gateway_config,
    validate_dependencies,
)
from almanak.gateway.auth import read_token
from almanak.triggers import IntervalSelection


def validate_sources(args, author, settings):
    from almanak.connectors.composition import connector_for
    from almanak.connectors.support import historical_capability

    interval = IntervalSelection(
        configured=author.decision_trigger,
        override_seconds=settings.schedule.decision_interval_override_seconds,
    )
    config = parameters(author, settings, interval)
    sources = markets(config)
    validate_collection(sources, settings)
    connector = connector_for(config)
    for chain in dict.fromkeys(a.chain for a in sources):
        historical_capability(
            connector.support_for(chain.chain_id), chain, CONNECTOR, MODEL
        ).require(settings.schedule.start)
    validate_dependencies(args, config, settings)
    return config


def validate_collection(sources, settings):
    if {a.chain for a in sources} != {h.chain for h in settings.history}:
        raise ValueError("Comet historical providers must cover exactly the configured chains")
    chains = {h.chain for h in settings.history}
    if any(b.chain not in chains for b in strategy_bindings(settings)):
        raise ValueError("Comet strategy price series must use a configured historical chain")
    count = (
        settings.schedule.deadline - settings.schedule.start
    ) // settings.data.sample_seconds + 1
    if count * len(sources) > settings.data.max_accounting_samples:
        raise ValueError("Comet accounting preparation exceeds its explicit sample limit")


def dataset_directory(args, author, settings):
    interval = IntervalSelection(
        configured=author.decision_trigger,
        override_seconds=settings.schedule.decision_interval_override_seconds,
    )
    config = parameters(author, settings, interval)
    return args.output.resolve().parent / ".backtest-datasets" / identity(config, settings)


def provision(directory, config, settings, resolution_facts=None):
    request = acquisition_request(identity(config, settings), resolution_facts)
    path = directory / "acquisition.json"
    if path.exists():
        if json.loads(path.read_text()) != request:
            raise ValueError("Comet preparation cannot change its sources, window or data policy")
    elif any(p.name not in PROVISIONAL for p in directory.iterdir()):
        raise ValueError("Comet preparation requires a new or matching acquisition directory")
    else:
        write_json(path, request)
    token = directory / "gateway-auth.token"
    if not token.exists():
        with token.open("x") as output:
            token.chmod(0o600)
            output.write(secrets.token_hex(32))


def open_histories(stack, directory, settings, endpoint):
    histories = {}
    token = read_token(directory / "gateway-auth.token")
    for binding in settings.history:
        reader = HistoricalReader(binding.chain, endpoint, token)
        stack.callback(reader.__exit__)
        histories[binding.chain] = EvmHistory(reader)
    return histories


def discover_contracts(directory, config, settings, env_file):
    write_json(
        directory / "history.json",
        gateway_config(settings, contracts(config)),
    )
    with ExitStack() as stack:
        gateway = stack.enter_context(GatewayProcess(directory, env_file))
        histories = open_histories(stack, directory, settings, gateway.endpoint)
        bounds(histories, config, settings)
        with progress.phase(
            "preparation.discovery",
            "Resolving configured Comet market contracts",
            len(markets(config)),
            "markets",
        ):
            discoveries = []
            for source in markets(config):
                discoveries.append(
                    discover(histories[source.chain], source, settings.schedule.start)
                )
                progress.advance()
    # Captured Comet implementation and asset-list reads extend the read-only binding.
    # Neither process has signing or submission authority.
    write_json(
        directory / "history.json",
        gateway_config(settings, contracts(config, discoveries)),
    )
    write_json(
        directory / "discovery.json",
        {
            "identity": identity(config, settings),
            "markets": [d.model_dump(mode="json") for d in discoveries],
        },
    )


def prepare(args):
    directory = args.output.resolve()
    require_private_directory(directory)
    directory.mkdir(parents=True, exist_ok=True)
    with (directory / "acquisition.lock").open("a") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise RuntimeError("another coordinator owns this Comet preparation") from None
        return prepare_locked(args)


def prepare_locked(args):
    from almanak.backtest.inputs import selected

    started = time.perf_counter()
    selection = selected(args)
    author, settings = selection.resolve(args.output.resolve(), getattr(args, "env_file", None))
    config = validate_sources(args, author, settings)
    directory = args.output.resolve()
    provision(directory, config, settings, selection.resolution_facts)
    reused = (directory / "dataset.json").exists()
    if not reused:
        discover_contracts(directory, config, settings, getattr(args, "env_file", None))
        acquire(directory, config, settings, getattr(args, "env_file", None))
    with progress.phase(
        "preparation.reuse" if reused else "preparation.verification",
        "Verifying Comet dataset coverage",
        1,
        "phases",
    ):
        from almanak.connectors.compound_v3.economic_book import Book

        book = Book(directory, config, settings)
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
        "risk_resolution": "every historical timestamp group at end-of-block visibility",
    }
    if not reused:
        write_json(directory / "preparation-report.json", report)
    return report


def acquire(directory, config, settings, env_file):
    preparation = Preparation(
        model=MODEL,
        acquisition_revision=REVISION,
        policy_digest=identity(config, settings),
        requirements=requirements(config, settings),
    )
    with ExitStack() as stack:
        gateway = stack.enter_context(GatewayProcess(directory, env_file))
        histories = open_histories(stack, directory, settings, gateway.endpoint)
        ranges = bounds(histories, config, settings)
        writer = DatasetWriter(directory, preparation)
        stack.callback(writer.close)
        for source in markets(config):
            collect(
                writer,
                histories[source.chain],
                config,
                source,
                settings,
                ranges[source.chain],
            )
        collect_strategy_prices(writer, histories, settings)
        with progress.phase(
            "preparation.finalization", "Publishing Comet prepared inputs", 1, "phases"
        ):
            writer.publish()


def collect(writer, history, config, source, settings, boundaries):
    if writer.missing(key(source, "risk")):
        count = boundaries[1].number - boundaries[0].number + 1
        with progress.phase(
            f"preparation.risk.{source.comet}",
            "Reading Comet risk and oracle blocks",
            count,
            "blocks",
            completed=retained_risk_blocks(writer, source),
        ):
            collect_risk(writer, history, config, source, settings)
    missing = writer.missing(key(source, "accounting"))
    if missing:
        count = sum((right - left) // settings.data.sample_seconds + 1 for left, right in missing)
        with progress.phase(
            f"preparation.accounting.{source.comet}",
            "Reading Comet accounting samples",
            count,
            "samples",
        ):
            collect_accounting(writer, history, config, source, settings)
