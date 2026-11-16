"""PancakeSwap V3 prepared acquisition with explicit protocol validation."""

from almanak.connectors.pancakeswap_v3 import economic_data
from almanak.connectors.pancakeswap_v3.economic_data import PoolSample
from almanak.connectors.pancakeswap_v3.history import contract_identities
from almanak.connectors.pancakeswap_v3.reader import read_pool
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
