"""Acquire canonical pool samples through the dedicated read-only historical transport."""

from almanak.backtest.types import UnixTime
from almanak.connectors.curve import economic_data
from almanak.connectors.curve.deployment import verify_pool
from almanak.connectors.curve.reader import read_pool
from almanak.connectors.v3_prepared_acquisition import evidence


def sample(reader, target, block, parameter_path, successor):
    return economic_data.PoolSample(
        pool=read_pool(reader, target.pool, block),
        contracts=verify_pool(reader, target.pool, block).identities,
        parameter_path=parameter_path,
        successor=successor,
    )


def prepared_sample(history, target, at, parameter_path):
    anchor = history.floor(UnixTime(at))
    successor = history.header(anchor.number + 1)
    reader = history.reader
    reader.used.clear()
    history.pin(successor)
    block = history.pin(anchor)
    observed = sample(reader, target, block, parameter_path, successor)
    raw = evidence(reader)
    return economic_data.Record(
        key=economic_data.key(target),
        unit=economic_data.unit(target.pool),
        payload=observed,
        reference=block,
        source_time=UnixTime(block.timestamp),
        available_at=UnixTime(at),
        retrieved_at=max(reader.cache[key].retrieved_at for key in reader.used),
        evidence=(*tuple(value.digest for value in raw), parameter_path),
    ), raw
