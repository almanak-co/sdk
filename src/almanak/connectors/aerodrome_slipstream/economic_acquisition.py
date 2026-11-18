"""Slipstream prepared acquisition with explicit protocol validation."""

from almanak.connectors.aerodrome_slipstream import economic_data
from almanak.connectors.aerodrome_slipstream.economic_data import PoolSample
from almanak.connectors.aerodrome_slipstream.history import contract_identities
from almanak.connectors.aerodrome_slipstream.reader import read_pool, read_tick
from almanak.connectors.v3_prepared_acquisition import evidence as evidence
from almanak.connectors.v3_prepared_acquisition import prepared_sample as retained_sample
from almanak.connectors.v3_prepared_acquisition import sample as pool_sample


def sample(reader, source, block, *, lower, upper, max_words, max_ticks, **carried):
    return pool_sample(
        reader,
        source,
        block,
        lower=lower,
        upper=upper,
        max_words=max_words,
        max_ticks=max_ticks,
        read_pool=read_pool,
        sample_type=PoolSample,
        tick_reader=read_tick,
        **carried,
    )


def prepared_sample(history, target, at, policy, baseline, **carried):
    return retained_sample(
        history,
        target,
        at,
        policy,
        baseline,
        sample=sample,
        data=economic_data,
        contract_identities=contract_identities,
        **carried,
    )
