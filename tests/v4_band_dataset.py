"""Synthetic multi-sample Uniswap V4 band datasets built on one captured pool sample.

Sample zero is the committed Robinhood V4 LP fixture's first pool state. That capture
has no tick growth, so the path starts from zero global and outside growth, which is
the state the pool's own initialization rule gives when growth has not yet accrued.
Later samples follow an explicit synthetic path through the tick ledger. The dataset is
not historical evidence of any market.
"""

from eth_abi import encode

from almanak.backtest.dataset import RawEvidence
from almanak.backtest.prepared import AcquiredRange, DatasetWriter, Preparation
from almanak.connectors.uniswap_v4.economic_data import (
    MODEL,
    REVISION,
    PoolSample,
    Record,
    Tick,
    key,
    requirement,
)
from almanak.connectors.uniswap_v4.economic_prepare import identity
from almanak.connectors.uniswap_v4.history import BoundaryEvidence
from almanak.connectors.uniswap_v4.pool import pool_id
from almanak.connectors.uniswap_v4.receipts import MODIFY
from tests.v3_band_dataset import OWNER, SyntheticPath, digest

UNIT = "uniswap_v4_integer_pool"


def with_growth(sample):
    """The captured sample with the zero growth state the synthetic path starts from."""
    return PoolSample.model_validate(
        {
            **sample.model_dump(),
            # The fixed-range capture's range growth and boundary gross do not apply.
            "fee_growth": None,
            "boundary_gross": None,
            "global_growth": (0, 0),
            "outside": tuple((0, 0) for _ in sample.ticks),
        }
    )


class V4SyntheticPath(SyntheticPath):
    def __init__(self, sample, target, seconds):
        super().__init__(with_growth(sample), target, seconds)

    def initial_ticks(self, sample):
        return sample.growth_ticks()

    def next_sample(self, pool, words, covered):
        states = tuple(self.ledger.ticks[t] for t in covered)
        return PoolSample.model_validate(
            {
                **self.sample.model_dump(),
                "pool": pool.model_dump(),
                "words": words,
                "ticks": tuple(
                    Tick(index=t, gross=s[0], net=s[1])
                    for t, s in zip(covered, states, strict=True)
                ),
                "outside": tuple((s[2], s[3]) for s in states),
                "global_growth": tuple(self.ledger.growth),
            }
        )

    def log(self, index, lower, upper, change):
        block = self.block(index)
        data = encode(("int24", "int24", "int256", "bytes32"), (lower, upper, change, b"\0" * 32))
        return {
            "address": self.target.pool.pool_manager,
            "topics": [MODIFY, pool_id(self.target.pool), OWNER],
            "data": "0x" + data.hex(),
            "blockNumber": hex(block.number),
            "blockHash": block.hash,
            "logIndex": hex(len(self.logs)),
            "transactionIndex": hex(len(self.logs)),
            "transactionHash": digest(f"synthetic-v4-log-{len(self.logs)}"),
            "removed": False,
        }


def publish(directory, config, settings, path, *, drop_event=None):
    """Publish the path as a prepared V4 dataset; `drop_event` omits one retained log."""
    target = config.target(config.allocations[0].name)
    blocks = path.blocks()
    baseline = BoundaryEvidence(
        target=target,
        first=blocks[0],
        last=blocks[-1],
        initial_gross=None,
        final_gross=None,
        events=tuple(e for i, e in enumerate(path.logs) if i != drop_event),
        contracts=(),
        inputs=(),
    )
    proof = RawEvidence(body=baseline.model_dump_json())
    preparation = Preparation(
        model=MODEL,
        acquisition_revision=REVISION,
        policy_digest=identity(config, settings),
        requirements=(requirement(target, settings),),
    )
    writer = DatasetWriter(directory, preparation)
    try:
        for sample, block in zip(path.samples, blocks, strict=True):
            at = block.timestamp
            payload = sample.model_copy(update={"baseline_digest": proof.digest})
            raw = RawEvidence(body=payload.model_dump_json())
            record = Record(
                key=key(target),
                unit=UNIT,
                payload=payload,
                reference=block,
                source_time=at,
                available_at=at,
                retrieved_at=float(at),
                evidence=(proof.digest, raw.digest),
            )
            end = min(settings.schedule.deadline, at + path.seconds - 1)
            writer.commit_range(
                AcquiredRange(key=key(target), start=at, end=end, evidence=record.evidence),
                (record,),
                (proof, raw),
            )
        writer.publish()
    finally:
        writer.close()
