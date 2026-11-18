"""Sample each configured pool on an explicit bounded historical grid."""

from almanak.backtest import progress
from almanak.backtest.gateway import ContractBinding
from almanak.backtest.prepared import AcquiredRange
from almanak.backtest.types import UnixTime
from almanak.connectors.aerodrome.economic_acquisition import prepared_sample
from almanak.connectors.aerodrome.economic_data import key
from almanak.connectors.dex_economic_dataset import targets


def contract_bindings(settings, config, boundaries=None):
    if {a.chain for a in config.allocations} != {h.chain for h in settings.history}:
        raise ValueError("Aerodrome history must cover exactly the configured pool chains")
    return tuple(
        ContractBinding(
            chain=binding.chain,
            contracts=tuple(
                sorted(
                    {
                        address
                        for target in targets(config)
                        if target.fund.chain == binding.chain
                        for address in (
                            target.pool.factory,
                            target.pool.pool,
                            target.contracts.router,
                            *(asset.reference for asset in target.assets),
                        )
                    }
                )
            ),
        )
        for binding in settings.history
    )


def resolve_boundaries(histories, config, settings):
    return {
        chain: (
            histories[chain].floor(settings.schedule.start),
            histories[chain].floor(settings.schedule.deadline),
        )
        for chain in dict.fromkeys(a.chain for a in config.allocations)
    }


def sample_count(settings):
    return (
        settings.schedule.deadline - settings.schedule.start
    ) // settings.data.sample_seconds + 1


def collect(writer, history, target, settings):
    if sample_count(settings) > settings.data.max_samples:
        raise ValueError("Aerodrome sampling exceeds its explicit per-pool sample limit")
    missing = writer.missing(key(target))
    interval = settings.data.sample_seconds
    count = sum((right - left) // interval + 1 for left, right in missing)
    with progress.phase(
        f"preparation.pool.{target.allocation}", "Sampling Aerodrome pool state", count, "samples"
    ):
        for left, right in missing:
            for at in range(left, right + 1, interval):
                record, raw = prepared_sample(history, target, at)
                writer.commit_range(
                    AcquiredRange(
                        key=key(target),
                        start=UnixTime(at),
                        end=UnixTime(min(right, at + interval - 1)),
                        evidence=record.evidence,
                    ),
                    (record,),
                    raw,
                )
                progress.advance()
