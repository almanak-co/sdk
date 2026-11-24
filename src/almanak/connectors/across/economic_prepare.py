"""A modeled bridge prepares no history: its dataset is the frozen statement of its model.

Economic runs prepare or reuse a dataset and then run offline. The bridge model reads
nothing, so its dataset holds no observation: only the model, and the identity of the
routes, the window and the stated latency it was frozen for. Preparing it asks no
provider.
"""

import fcntl
import json
import time
from typing import Literal

from almanak.backtest.cli import write_json
from almanak.backtest.dataset import Digest, file_digest
from almanak.backtest.prepared import PROVISIONAL, acquisition_request, require_private_directory
from almanak.connectors.across.economic_model import MODEL, identity, parameters
from almanak.connectors.across.settlement_types import CONNECTOR
from almanak.models import Model
from almanak.triggers import IntervalSelection


class Descriptor(Model):
    model: Literal["across-full-fee-ceiling-fixed-latency-v1"] = MODEL
    policy_digest: Digest
    # The model reads no history, so the dataset names none.
    inputs: Literal["none"] = "none"


class Book:
    """The frozen model statement of one run. It holds no market data."""

    def __init__(self, directory, config, settings, digest=None):
        path = directory / "dataset.json"
        if digest is not None and file_digest(path) != digest:
            raise ValueError("modeled bridge dataset manifest differs from the frozen run")
        self.manifest = Descriptor.model_validate_json(path.read_text())
        if self.manifest.policy_digest != identity(config, settings):
            raise ValueError("modeled bridge dataset differs from the frozen routes and settings")

    def close(self):
        pass


def selected_parameters(author, settings):
    interval = IntervalSelection(
        configured=author.decision_trigger,
        override_seconds=settings.schedule.decision_interval_override_seconds,
    )
    return parameters(author, settings, interval)


def validate_sources(args, author, settings):
    from almanak.backtest.strategy_prices import strategy_bindings
    from almanak.connectors.composition import connector_for
    from almanak.connectors.support import historical_capability

    config = selected_parameters(author, settings)
    connector = connector_for(config)
    if strategy_bindings(settings):
        raise ValueError("the bridge model delivers no strategy price series")
    for chain in dict.fromkeys(a.chain for a in config.positions):
        historical_capability(
            connector.support_for(chain.chain_id), chain, CONNECTOR, MODEL
        ).require(settings.schedule.start)
    return config


def dataset_directory(args, author, settings):
    config = selected_parameters(author, settings)
    return args.output.resolve().parent / ".backtest-datasets" / identity(config, settings)


def provision(directory, config, settings, resolution_facts=None):
    request = acquisition_request(identity(config, settings), resolution_facts)
    path = directory / "acquisition.json"
    if path.exists():
        if json.loads(path.read_text()) != request:
            raise ValueError("modeled bridge preparation cannot change its routes or settings")
    elif any(p.name not in PROVISIONAL for p in directory.iterdir()):
        raise ValueError("modeled bridge preparation requires a new or matching directory")
    else:
        write_json(path, request)


def prepare(args):
    directory = args.output.resolve()
    require_private_directory(directory)
    directory.mkdir(parents=True, exist_ok=True)
    with (directory / "acquisition.lock").open("a") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise RuntimeError("another coordinator owns this bridge preparation") from None
        return prepare_locked(args)


def prepare_locked(args):
    from almanak.backtest.inputs import selected

    started = time.perf_counter()
    selection = selected(args)
    directory = args.output.resolve()
    author, settings = selection.resolve(directory, getattr(args, "env_file", None))
    config = validate_sources(args, author, settings)
    provision(directory, config, settings, selection.resolution_facts)
    reused = (directory / "dataset.json").exists()
    if not reused:
        write_json(directory / "dataset.json", Descriptor(policy_digest=identity(config, settings)))
    book = Book(directory, config, settings)
    report = {
        "dataset": book.manifest.model_dump(mode="json"),
        "elapsed_seconds": time.perf_counter() - started,
        "provider_activity": [],
        "cache_state": "completed_dataset" if reused else "modeled_without_acquisition",
    }
    if not reused:
        write_json(directory / "preparation-report.json", report)
    return report
