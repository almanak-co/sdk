"""Coordinate native read-only acquisition after validating the single strategy hook."""

import fcntl
import json
import time
from copy import copy

from almanak.backtest import progress
from almanak.backtest.cli import write_json
from almanak.backtest.dataset import file_digest
from almanak.backtest.prepared import PROVISIONAL, acquisition_request, require_private_directory
from almanak.connectors.aave_v3 import economic_prepare as lending
from almanak.connectors.across import economic_prepare as across
from almanak.connectors.aerodrome import economic_prepare as aerodrome
from almanak.connectors.aerodrome_slipstream import economic_prepare as slipstream
from almanak.connectors.composed import aave_v4_economic
from almanak.connectors.composed.economic_book import Book, Descriptor, identity
from almanak.connectors.composed.economic_inventory import initial, parameters
from almanak.connectors.composed.economic_prices import prepare_prices, validate_chains
from almanak.connectors.compound_v3 import economic_prepare as compound
from almanak.connectors.curve import economic_prepare as curve
from almanak.connectors.dex_economic_prepare import validate_dependencies
from almanak.connectors.gmx_v2 import economic_prepare as gmx
from almanak.connectors.identifiers import ConnectorId
from almanak.connectors.morpho_blue import economic_prepare as morpho
from almanak.connectors.pancakeswap_v3 import economic_prepare as pancake
from almanak.connectors.sparklend import economic_prepare as spark
from almanak.connectors.uniswap_v3 import economic_prepare as exchange
from almanak.connectors.uniswap_v4 import economic_prepare as v4
from almanak.triggers import IntervalSelection


def selected_parameters(author, settings):
    interval = IntervalSelection(
        configured=author.decision_trigger,
        override_seconds=settings.schedule.decision_interval_override_seconds,
    )
    config = parameters(author, settings, interval)
    initial(settings, config, interval)
    return config


def component_args(args, config, settings, connector):
    child = copy(args)
    child.strategy = None
    native = config.component(connector)
    child._backtest_inputs = native, settings.component(connector, native), None
    return child


NATIVE_PREPARATION = {
    ConnectorId("aave_v4"): aave_v4_economic,
    ConnectorId("aave_v3"): lending,
    ConnectorId("sparklend"): spark,
    ConnectorId("uniswap_v4"): v4,
    ConnectorId("uniswap_v3"): exchange,
    ConnectorId("pancakeswap_v3"): pancake,
    ConnectorId("aerodrome"): aerodrome,
    ConnectorId("aerodrome_slipstream"): slipstream,
    ConnectorId("morpho_blue"): morpho,
    ConnectorId("compound_v3"): compound,
    ConnectorId("curve"): curve,
    ConnectorId("gmx_v2"): gmx,
    ConnectorId("across"): across,
}


def native_preparation(connector):
    module = NATIVE_PREPARATION.get(connector)
    if module is None:
        raise ValueError("no reviewed native acquisition binding")
    return module


def validate_sources(args, author, settings):
    config = selected_parameters(author, settings)
    if set(settings.models) != set(config.parameter_components):
        raise ValueError("economic model policies must match configured connectors exactly")
    if {a.chain for a in config.positions} != {h.chain for h in settings.history}:
        raise ValueError("composed historical providers must cover configured chains exactly")
    # The strategy decides once for the composition: its declared series bind to the
    # composed settings, never to a component, whose views run without the strategy.
    validate_dependencies(args, config, settings)
    validate_chains(settings)
    for connector in config.parameter_components:
        child = component_args(args, config, settings, connector)
        native, policy, _ = child._backtest_inputs
        native_preparation(connector).validate_sources(child, native, policy)
    return config


def dataset_directory(args, author, settings):
    return (
        args.output.resolve().parent
        / ".backtest-datasets"
        / identity(selected_parameters(author, settings), settings)
    )


def prepare(args):
    directory = args.output.resolve()
    require_private_directory(directory)
    directory.mkdir(parents=True, exist_ok=True)
    with (directory / "acquisition.lock").open("a") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise RuntimeError("another coordinator owns this composed preparation") from None
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
    reports = {} if reused else acquire_components(args, config, settings)
    book = Book(directory, config, settings)
    book.close()
    report = {
        "elapsed_seconds": time.perf_counter() - started,
        "cache_state": "completed_dataset" if reused else "cold_or_resumed_acquisition",
        "components": reports,
        "dataset": book.manifest.model_dump(mode="json"),
    }
    if not reused:
        write_json(directory / "preparation-report.json", report)
    return report


def provision(directory, config, settings, resolution_facts=None):
    acquisition = directory / "acquisition.json"
    request = acquisition_request(identity(config, settings), resolution_facts)
    if acquisition.exists():
        if json.loads(acquisition.read_text()) != request:
            raise ValueError("composed acquisition cannot change its frozen inputs")
    else:
        if any(p.name not in PROVISIONAL for p in directory.iterdir()):
            raise ValueError("composed preparation requires a new or matching directory")
        write_json(acquisition, request)


def acquire_components(args, config, settings):
    directory = args.output.resolve()
    reports = {}
    for connector in config.parameter_components:
        child = component_args(args, config, settings, connector)
        child.output = directory / connector.value
        with progress.phase_scope(connector.value):
            reports[connector.value] = native_preparation(connector).prepare(child)
    policy_digest = identity(config, settings)
    write_json(
        directory / "dataset.json",
        Descriptor(
            policy_digest=policy_digest,
            components={
                connector: file_digest(directory / connector.value / "dataset.json")
                for connector in config.parameter_components
            },
            prices=prepare_prices(
                directory, policy_digest, settings, getattr(args, "env_file", None)
            ),
        ),
    )
    return reports
