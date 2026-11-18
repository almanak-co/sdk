"""Collect a bounded accounting grid plus every supported parameter-change time."""

from almanak.backtest import progress
from almanak.backtest.dataset import RawEvidence
from almanak.backtest.gateway import ContractBinding
from almanak.backtest.history_logs import MAX_LOG_BLOCKS
from almanak.backtest.prepared import AcquiredRange
from almanak.backtest.types import UnixTime
from almanak.connectors.curve.economic_acquisition import prepared_sample
from almanak.connectors.curve.economic_data import key
from almanak.connectors.curve.economic_history import log_scopes, preflight
from almanak.connectors.dex_economic_dataset import targets
from almanak.connectors.v3_prepared_acquisition import evidence


def contract_bindings(settings, config, boundaries=None):
    if {a.chain for a in config.allocations} != {h.chain for h in settings.history}:
        raise ValueError("Curve history must cover exactly the configured pool chains")
    result = []
    for binding in settings.history:
        selected = tuple(t for t in targets(config) if t.fund.chain == binding.chain)
        addresses = {
            address
            for target in selected
            for address in (
                target.pool.factory,
                target.pool.pool,
                *(asset.reference for asset in target.assets),
            )
        }
        logs = (
            ()
            if boundaries is None
            else tuple(
                dict.fromkeys(
                    scope
                    for target in selected
                    for scope in log_scopes(target.pool, *boundaries[binding.chain])
                )
            )
        )
        result.append(
            ContractBinding(chain=binding.chain, contracts=tuple(sorted(addresses)), logs=logs)
        )
    return tuple(result)


def resolve_boundaries(histories, config, settings):
    boundaries = {}
    for chain in dict.fromkeys(a.chain for a in config.allocations):
        history = histories[chain]
        first, last = (
            history.floor(settings.schedule.start),
            history.floor(settings.schedule.deadline),
        )
        count = (last.number - first.number + MAX_LOG_BLOCKS - 1) // MAX_LOG_BLOCKS
        if count > settings.data.max_parameter_log_requests:
            raise ValueError("Curve window exceeds its explicit parameter log request limit")
        boundaries[chain] = first, last
    return boundaries


def grid(settings):
    return tuple(
        range(settings.schedule.start, settings.schedule.deadline + 1, settings.data.sample_seconds)
    )


def sample_times(settings, path):
    times = tuple(sorted(set(grid(settings)) | {event.block.timestamp for event in path.events}))
    if len(times) > settings.data.max_samples:
        raise ValueError("Curve grid and parameter changes exceed the explicit sample limit")
    return times


def collect(writer, history, target, settings):
    missing = writer.missing(key(target))
    if not missing:
        return
    with progress.phase(
        f"preparation.parameters.{target.allocation}",
        "Collecting Curve parameter history",
        1,
        "phases",
    ):
        path = preflight(history, target, settings.schedule, settings.data)
        parameter_raw = evidence(history.reader)
        path_raw = RawEvidence(body=path.model_dump_json())
    times = sample_times(settings, path)
    required = tuple(at for at in times if any(left <= at <= right for left, right in missing))
    with progress.phase(
        f"preparation.pool.{target.allocation}",
        "Sampling Curve pool state",
        len(required),
        "samples",
    ):
        # Parameter-history reads are retained once, with the first range this pass
        # commits; every range keeps the parameter path its records name.
        shared = parameter_raw
        for at in required:
            record, raw = prepared_sample(history, target, at, path_raw.digest)
            retained = tuple({value.digest: value for value in (*raw, *shared, path_raw)}.values())
            shared = ()
            following = next((time for time in times if time > at), settings.schedule.deadline + 1)
            writer.commit_range(
                AcquiredRange(
                    key=key(target),
                    start=UnixTime(at),
                    end=UnixTime(following - 1),
                    evidence=tuple(value.digest for value in retained),
                ),
                (record,),
                retained,
            )
            progress.advance()
