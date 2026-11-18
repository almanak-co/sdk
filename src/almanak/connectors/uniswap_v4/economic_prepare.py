"""Resumable bounded pool sampling using the shared historical gateway and publisher."""

import fcntl
import hashlib
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
    AcquiredRange,
    DatasetWriter,
    Preparation,
    acquisition_request,
    load_dataset,
    require_private_directory,
)
from almanak.backtest.strategy_prices import strategy_requirements
from almanak.backtest.types import UnixTime
from almanak.connectors.band_fee_growth import validate_envelope
from almanak.connectors.dex_economic_prepare import (
    collect_strategy_prices,
    gateway_config,
    validate_dependencies,
)
from almanak.connectors.support import historical_capability
from almanak.connectors.uniswap_v4.backtest import economic_parameters as parameters
from almanak.connectors.uniswap_v4.backtest_binding import contract_bindings
from almanak.connectors.uniswap_v4.economic_acquisition import evidence, sample
from almanak.connectors.uniswap_v4.economic_data import (
    CONNECTOR,
    MODEL,
    REVISION,
    key,
    requirement,
)
from almanak.connectors.uniswap_v4.history import preflight
from almanak.gateway.auth import read_token
from almanak.triggers import IntervalSelection


def capability(connector, chain):
    # Resolve the declaration installed on the connector binding, never the shared
    # declaration function: a connector bound with a different chain support would
    # otherwise be unreachable from the economic paths.
    if not any(support.chain == chain.chain_id for support in connector.chain_support):
        raise ValueError("Uniswap economic model does not support the requested chain")
    return historical_capability(connector.support_for(chain.chain_id), chain, CONNECTOR, MODEL)


def targets(config):
    return tuple(config.target(a.name) for a in config.allocations)


def identity(config, settings):
    body = {
        "targets": [t.model_dump(mode="json") for t in targets(config)],
        "data": settings.data.model_dump(mode="json"),
        "start": int(settings.schedule.start),
        "end": int(settings.schedule.deadline),
        "providers": [h.model_dump(mode="json", exclude={"collection"}) for h in settings.history],
        "model": MODEL,
        "acquisition_revision": REVISION,
    }
    return hashlib.sha256(json.dumps(body, sort_keys=True).encode()).hexdigest()


def validate_sources(args, author, settings):
    from almanak.connectors.composition import connector_for

    interval = IntervalSelection(
        configured=author.decision_trigger,
        override_seconds=settings.schedule.decision_interval_override_seconds,
    )
    config = parameters(author, settings, interval)
    contract_bindings(settings, config)
    if {a.name for a in config.allocations} != {c.allocation for c in settings.data.coverage}:
        raise ValueError("prepared coverage must identify every configured allocation exactly")
    connector = connector_for(config)
    for target in targets(config):
        capability(connector, target.fund.chain).require(
            settings.schedule.start, "swap_exact_input"
        )
        bound = settings.data.for_target(target)
        words = (
            ((bound.upper // target.pool.tick_spacing) >> 8)
            - ((bound.lower // target.pool.tick_spacing) >> 8)
            + 1
        )
        if words > settings.data.max_bitmap_words:
            raise ValueError("tick coverage exceeds the explicit bitmap collection limit")
        if target.band is not None:
            validate_envelope(target.band, bound.lower, bound.upper, target.pool.tick_spacing)
    validate_dependencies(args, config, settings)
    return config


def dataset_directory(args, author, settings):
    interval = IntervalSelection(
        configured=author.decision_trigger,
        override_seconds=settings.schedule.decision_interval_override_seconds,
    )
    config = parameters(author, settings, interval)
    return args.output.resolve().parent / ".backtest-datasets" / identity(config, settings)


def provision(directory, config, settings, resolution_facts=None):
    path = directory / "acquisition.json"
    request = acquisition_request(identity(config, settings), resolution_facts)
    if path.exists():
        if json.loads(path.read_text()) != request:
            raise ValueError("preparation resume cannot change pool, window or data policy")
    elif any(p.name not in PROVISIONAL for p in directory.iterdir()):
        raise ValueError("preparation requires a new or matching acquisition directory")
    else:
        write_json(path, request)
    token = directory / "gateway-auth.token"
    if not token.exists():
        with token.open("x") as output:
            token.chmod(0o600)
            output.write(secrets.token_hex(32))
    write_json(
        directory / "history.json", gateway_config(settings, contract_bindings(settings, config))
    )


def prepare(args):
    directory = args.output.resolve()
    require_private_directory(directory)
    directory.mkdir(parents=True, exist_ok=True)
    with (directory / "acquisition.lock").open("a") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise RuntimeError("another coordinator owns this preparation") from None
        return prepare_locked(args)


def prepare_locked(args):
    started = time.perf_counter()
    from almanak.backtest.inputs import selected

    selection = selected(args)
    author, settings = selection.resolve(args.output.resolve(), getattr(args, "env_file", None))
    config = validate_sources(args, author, settings)
    directory = args.output.resolve()
    provision(directory, config, settings, selection.resolution_facts)
    reused = (directory / "dataset.json").exists()
    if reused:
        with progress.phase("preparation.reuse", "Verifying cached pool dataset", 1, "phases"):
            manifest, reader = load_dataset(directory)
            reader.close()
            if manifest.preparation.policy_digest != identity(config, settings):
                raise ValueError("cached dataset differs from selected policy")
    else:
        acquire(directory, config, settings, getattr(args, "env_file", None))
        manifest, reader = load_dataset(directory)
        reader.close()
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


def acquire(directory, config, settings, env_file):
    preparation = Preparation(
        model=MODEL,
        acquisition_revision=REVISION,
        policy_digest=identity(config, settings),
        requirements=tuple(requirement(t, settings) for t in targets(config))
        + strategy_requirements(settings),
    )
    job = progress.current()
    if job is not None:
        job.discard("preparation")
        job.begin("preparation.metadata", "Resolving pool preparation", 1, "phases")
    with ExitStack() as stack:
        gateway = stack.enter_context(GatewayProcess(directory, env_file))
        histories = {}
        for binding in settings.history:
            reader = HistoricalReader(
                binding.chain, gateway.endpoint, read_token(directory / "gateway-auth.token")
            )
            stack.callback(reader.__exit__)
            histories[binding.chain] = EvmHistory(reader)
        writer = DatasetWriter(directory, preparation)
        stack.callback(writer.close)
        plan_collection(writer, config, settings)
        for target in targets(config):
            collect(writer, histories[target.fund.chain], target, settings)
        collect_strategy_prices(writer, histories, settings)
        with progress.phase("preparation.finalization", "Publishing pool dataset", 1, "phases"):
            writer.publish()


def collect(writer, history, target, settings):
    if not writer.missing(key(target)):
        return
    with progress.phase(
        f"preparation.boundaries.{target.allocation}", "Verifying LP continuity", 1, "phases"
    ):
        baseline = preflight(history, target, settings.schedule)
        baseline_evidence = evidence(history.reader)
    interval = settings.data.sample_seconds
    missing = writer.missing(key(target))
    count = sum((end - start) // interval + 1 for start, end in missing)
    # Continuity reads are retained once, with the first range this pass commits;
    # each record reaches them through the continuity proof it names.
    shared = baseline_evidence
    with progress.phase(
        f"preparation.pool.{target.allocation}", "Sampling bounded pool state", count, "samples"
    ):
        for left, right in missing:
            for at in range(left, right + 1, interval):
                record, raw = sample(history, target, at, settings.data, baseline)
                raw = tuple({e.digest: e for e in (*raw, *shared)}.values())
                shared = ()
                span = AcquiredRange(
                    key=key(target),
                    start=UnixTime(at),
                    end=UnixTime(min(right, at + interval - 1)),
                    evidence=tuple(e.digest for e in raw),
                )
                writer.commit_range(span, (record,), raw)
                progress.advance()


def plan_collection(writer, config, settings):
    job = progress.current()
    if job is None:
        return
    for target in targets(config):
        missing = writer.missing(key(target))
        if not missing:
            continue
        count = sum((end - start) // settings.data.sample_seconds + 1 for start, end in missing)
        job.plan(
            f"preparation.boundaries.{target.allocation}", "Verifying LP continuity", 1, "phases"
        )
        job.plan(
            f"preparation.pool.{target.allocation}", "Sampling bounded pool state", count, "samples"
        )
    job.plan("preparation.finalization", "Publishing pool dataset", 1, "phases")
    job.finish("preparation.metadata")
