"""Bounded archival V3 sampling and canonical range evidence publication."""

from almanak.backtest import progress
from almanak.backtest.dataset import RawEvidence
from almanak.backtest.gateway import ContractBinding
from almanak.backtest.history_logs import LogScope
from almanak.backtest.prepared import AcquiredRange
from almanak.backtest.read_ahead import ReadAhead
from almanak.backtest.types import UnixTime
from almanak.connectors.dex_economic_dataset import targets
from almanak.connectors.v3_pool_history import liquidity_event, require_log_requests
from almanak.connectors.v3_pool_receipts import POOL_BURN, POOL_MINT
from almanak.connectors.v3_prepared_acquisition import Carry, evidence, tick_read
from almanak.gateway.evm import erc20_address


def contract_bindings(settings, config, boundaries=None):
    if {a.chain for a in config.allocations} != {h.chain for h in settings.history}:
        raise ValueError("V3 historical providers must cover exactly the configured pool chains")
    result = []
    for binding in settings.history:
        selected = tuple(t for t in targets(config) if t.fund.chain == binding.chain)
        addresses = {
            address
            for target in selected
            for address in (
                target.pool.factory,
                target.pool.pool,
                *target.contracts.model_dump().values(),
                *(erc20_address(a, binding.chain.chain_id) for a in target.assets),
            )
        }
        logs: tuple[LogScope, ...] = ()
        if boundaries is not None:
            first, last = boundaries[binding.chain]
            logs = tuple(
                dict.fromkeys(
                    LogScope(
                        address=t.pool.pool,
                        topics=(POOL_MINT, POOL_BURN),
                        first_block=first.number,
                        last_block=last.number,
                    )
                    for t in selected
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
        first = history.floor(settings.schedule.start)
        last = history.floor(settings.schedule.deadline)
        require_log_requests(first, last, settings.data)
        boundaries[chain] = first, last
    return boundaries


def liquidity_carry(target, baseline):
    """Tick carry over the liquidity events the continuity evidence retains."""
    return Carry(tuple((e.block.number, *liquidity_event(target, e)[:2]) for e in baseline.events))


def collect(writer, history, target, settings, *, key, preflight, prepared_sample):
    missing = writer.missing(key(target))
    if not missing:
        return
    with progress.phase(
        f"preparation.boundaries.{target.allocation}", "Verifying V3 range continuity", 1, "phases"
    ):
        baseline = preflight(history, target, settings.schedule, settings.data)
        baseline_evidence = evidence(history.reader)
    interval = settings.data.sample_seconds
    count = sum((right - left) // interval + 1 for left, right in missing)
    carry = liquidity_carry(target, baseline)
    proof = RawEvidence(body=baseline.model_dump_json())
    # The continuity reads grow with the window's block count, so they are retained
    # once, with the first range this pass commits. Every record still names the
    # continuity proof, which in turn names those reads.
    shared = baseline_evidence
    with progress.phase(
        f"preparation.pool.{target.allocation}", "Sampling bounded V3 pool state", count, "samples"
    ):
        for left, right in missing:
            times = range(left, right + 1, interval)
            ahead = ReadAhead(history, times)
            # Each range starts from a direct read of every tick.
            carry.reset()
            for at in times:
                ahead.before(at)
                record, raw = prepared_sample(
                    history,
                    target,
                    at,
                    settings.data,
                    baseline,
                    proof=proof,
                    carry=carry,
                    # The range's last sample confirms every tick it carried.
                    audit=at == times[-1],
                )
                ahead.learn(record.reference, keep=repeated_reads(carry))
                retained = tuple({e.digest: e for e in (*raw, *shared)}.values())
                shared = ()
                writer.commit_range(
                    AcquiredRange(
                        key=key(target),
                        start=UnixTime(at),
                        end=UnixTime(min(right, at + interval - 1)),
                        evidence=tuple(e.digest for e in retained),
                    ),
                    (record,),
                    retained,
                )
                progress.advance()


def repeated_reads(carry):
    """Which of a sample's reads the next samples are expected to repeat.

    The next sample starts from the ticks beside the price. The other ticks this one
    read were crossed or changed by a liquidity event, which the next interval need not
    repeat.
    """
    beside = carry.beside()

    def keep(request):
        tick = tick_read(request)
        return tick is None or tick in beside

    return keep
