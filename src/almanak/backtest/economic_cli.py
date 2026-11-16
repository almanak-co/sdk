"""Offline economic runs bound to an immutable historical dataset."""

import json
import secrets
import time

from almanak.backtest import progress
from almanak.backtest.cli import sdk_digest, write_json
from almanak.backtest.dataset import file_digest
from almanak.backtest.engine import Journal, Simulator
from almanak.backtest.prepared import require_private_directory
from almanak.connectors.composition import connector_for, connector_from_wire
from almanak.runtime import validate_strategy_config
from almanak.triggers import IntervalSelection
from almanak.worker_session import preloaded_workers


def create(args, author, settings):
    from almanak.backtest.inputs import HarnessSelection, frozen

    binding = connector_for(author).backtest("economic")
    if not isinstance(settings, binding.settings_type):
        raise ValueError("economic execution requires economic settings")
    started = time.perf_counter()
    interval = IntervalSelection(
        configured=author.decision_trigger,
        override_seconds=settings.schedule.decision_interval_override_seconds,
    )
    config = binding.parameters(author, settings, interval)
    selection = frozen(args, author, settings)
    strategy = selection.strategy
    if strategy is None:
        raise ValueError("backtesting requires a strategy")
    validation = author if isinstance(selection, HarnessSelection) else config
    validate_strategy_config(strategy, validation, economic=True)
    initial = binding.initial(settings, config, interval)
    dataset, preparation_seconds = resolve_dataset(
        args, author, settings, selection.resolution_facts
    )
    progress.simulation_setup()
    # The retained manifest must claim the model family the dataset was prepared
    # for, not the connector's manifest default. GMX shares one Manifest across
    # swap/GM/perpetual models, so the default leaks the wrong string otherwise.
    # Single-connector datasets record it under preparation.model; composed
    # datasets record the composed model at the top level instead.
    dataset_meta = json.loads((dataset / "dataset.json").read_text())
    prepared_model = dataset_meta.get("preparation", {}).get("model") or dataset_meta.get("model")
    if prepared_model is None:
        raise ValueError("economic dataset does not record its prepared model")
    manifest = binding.manifest_type(
        sdk_digest=sdk_digest(),
        dataset=dataset,
        dataset_sha256=file_digest(dataset / "dataset.json"),
        config=config,
        settings=settings,
        strategy=strategy,
        initial=initial,
        interval=interval,
        resolution_facts=selection.resolution_facts,
        resolution_identity=selection.resolution_identity,
        model=prepared_model,
    )
    book = connector_for(manifest.config).backtest("economic").open_book(manifest)
    try:
        directory = args.output.resolve()
        require_private_directory(directory)
        new_run(directory)
        write_json(directory / "manifest.json", manifest)
        journal = Journal(directory / "state.sqlite", type(manifest.initial))
        journal.create(manifest.initial)
        report = execute(directory, journal, manifest, book)
        report.update(
            preparation_seconds=preparation_seconds,
            end_to_end_seconds=time.perf_counter() - started,
        )
        write_json(directory / "report.json", report)
        return report
    finally:
        book.close()


def new_run(directory):
    # Provisional resolution may already live here; a recorded run may not.
    if (directory / "manifest.json").exists():
        raise FileExistsError("backtest output already holds a recorded run")
    directory.mkdir(parents=True, exist_ok=True)


def require_dataset_facts(dataset, facts):
    """A prepared dataset serves only runs built from the facts it was prepared with."""
    path = dataset / "acquisition.json"
    retained = json.loads(path.read_text()).get("resolution_facts") if path.exists() else None
    if retained != facts:
        raise ValueError("prepared dataset belongs to different resolved market facts")


def resolve_dataset(args, author, settings, facts=None):
    from types import SimpleNamespace

    binding = connector_for(author).backtest("economic")
    binding.validate_sources(args, author, settings)

    explicit = getattr(args, "dataset", None)
    if explicit is not None:
        require_dataset_facts(explicit.resolve(), facts)
        return explicit.resolve(), 0.0
    directory = binding.dataset_directory(args, author, settings)
    started = time.perf_counter()
    binding.prepare(
        SimpleNamespace(
            config=args.config,
            strategy=args.strategy,
            backtest_config=args.backtest_config,
            output=directory,
            env_file=getattr(args, "env_file", None),
            _backtest_inputs=getattr(args, "_backtest_inputs", None),
        )
    )
    return directory, time.perf_counter() - started


def load(directory):
    from almanak.backtest.inputs import require_facts

    raw = (directory / "manifest.json").read_text()
    binding = connector_from_wire(json.loads(raw)["config"]).backtest("economic")
    manifest = binding.manifest_type.model_validate_json(raw)
    if manifest.sdk_digest != sdk_digest():
        raise ValueError("economic resume requires the captured implementation")
    require_facts(directory, manifest)
    return manifest


def resume(args):
    directory = args.run.resolve()
    manifest = load(directory)
    book = connector_for(manifest.config).backtest("economic").open_book(manifest)
    try:
        journal = Journal(directory / "state.sqlite", type(manifest.initial))
        return execute(directory, journal, manifest, book)
    finally:
        book.close()


def reproduce(args):
    directory = args.run.resolve()
    manifest = load(directory)
    book = connector_for(manifest.config).backtest("economic").open_book(manifest)
    try:
        path = directory / ("reproduce-" + secrets.token_hex(4) + ".sqlite")
        journal = Journal(path, type(manifest.initial))
        journal.create(manifest.initial)
        driver = (
            connector_for(manifest.config)
            .backtest("economic")
            .driver(manifest.config, manifest.settings, manifest.strategy, book)
        )
        with preloaded_workers():
            result = Simulator(journal, driver).run()
        expected = Journal(directory / "state.sqlite", type(manifest.initial)).load()
        if result != expected:
            raise ValueError("offline economic reproduction differs from recorded state")
        report = {"reproduced": True, "network": "disabled", "state": str(path)}
        write_json(directory / "reproduction.json", report)
        return report
    finally:
        book.close()


def execute(directory, journal, manifest, book):
    driver = (
        connector_for(manifest.config)
        .backtest("economic")
        .driver(manifest.config, manifest.settings, manifest.strategy, book)
    )
    started = time.perf_counter()
    try:
        driver.observe(journal.load())
        with preloaded_workers():
            state = Simulator(journal, driver).run()
        report = connector_for(manifest.config).backtest("economic").report(state, driver)
        report.update(
            elapsed_seconds=time.perf_counter() - started,
            dataset_sha256=manifest.dataset_sha256,
            provider_requests_during_execution=0,
        )
        write_json(directory / "report.json", report)
        return report
    except (ValueError, RuntimeError) as error:
        record_failure(directory, journal, manifest, driver, error)
        raise


def record_failure(directory, journal, manifest, driver, error):
    state = journal.load()
    failure = {
        "error": str(error),
        "state": str(journal.path),
        "portfolio": state.portfolio.model_dump(mode="json"),
    }
    try:
        report = connector_for(manifest.config).backtest("economic").report(state, driver)
        report.update(error=str(error), closed=False)
        write_json(directory / "report.json", report)
    except (ValueError, RuntimeError) as reporting_error:
        failure["reporting_error"] = str(reporting_error)
    write_json(directory / "failure.json", failure)
