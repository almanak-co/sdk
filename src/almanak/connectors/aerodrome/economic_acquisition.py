"""Canonical sampled balances, separate fee indices and verified runtime revisions."""

from almanak.backtest.types import UnixTime
from almanak.connectors.aerodrome import economic_data
from almanak.connectors.aerodrome.deployments import (
    require_direct_fund,
    verify_pool,
    verify_router,
)
from almanak.connectors.aerodrome.reader import read_pool
from almanak.connectors.v3_prepared_acquisition import evidence


def sample(reader, target, block):
    require_direct_fund(reader, target, block)
    return economic_data.PoolSample(
        pool=read_pool(reader, target.pool, block),
        contracts=(
            *verify_pool(reader, target.pool, block),
            *verify_router(reader, target.pool, target.contracts, block),
        ),
    )


def prepared_sample(history, target, at):
    reader = history.reader
    anchor = history.floor(UnixTime(at))
    reader.used.clear()
    block = history.pin(anchor)
    observed = sample(reader, target, block)
    raw = evidence(reader)
    return economic_data.Record(
        key=economic_data.key(target),
        unit=economic_data.UNIT,
        payload=observed,
        reference=block,
        source_time=UnixTime(block.timestamp),
        available_at=UnixTime(at),
        retrieved_at=max(reader.cache[k].retrieved_at for k in reader.used),
        evidence=tuple(e.digest for e in raw),
    ), raw
