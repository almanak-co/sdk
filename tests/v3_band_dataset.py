"""Synthetic multi-sample band datasets built on one captured Uniswap V3 pool sample.

Sample zero is the captured pool state. Later samples follow an explicit synthetic
path of fee growth, price moves and external liquidity events applied through the tick
ledger, so the published dataset exercises book validation and offline execution with
a known answer. It is not historical evidence of any market.
"""

import hashlib

from eth_abi import encode

from almanak.backtest.dataset import RawEvidence
from almanak.backtest.history_logs import HistoricalLog
from almanak.backtest.prepared import AcquiredRange, DatasetWriter, Preparation
from almanak.connectors.concentrated_math import sqrt_at_tick
from almanak.connectors.concentrated_pool import Word
from almanak.connectors.uniswap_v3.economic_book import identity
from almanak.connectors.uniswap_v3.economic_data import (
    MODEL,
    REVISION,
    UNIT,
    PreparedSample,
    Record,
    TickState,
    key,
    requirement,
)
from almanak.connectors.uniswap_v3.history import BoundaryEvidence
from almanak.connectors.v3_pool_receipts import POOL_BURN, POOL_MINT
from almanak.models import Block
from tests.band_pool_ledger import Ledger

SENDER = "0x" + "ab" * 20
OWNER = "0x" + "00" * 12 + "ab" * 20


def topic(value):
    return "0x" + encode(("int24",), (value,)).hex()


def digest(text):
    return "0x" + hashlib.sha256(text.encode()).hexdigest()


class SyntheticPath:
    """A captured sample followed by scripted steps, one sample per `seconds`."""

    def __init__(self, sample, target, seconds):
        self.sample, self.target, self.seconds = sample, target, seconds
        self.ledger = Ledger(
            tick=sample.pool.tick,
            growth=self.initial_growth(sample),
            ticks=self.initial_ticks(sample),
            liquidity=sample.pool.liquidity,
        )
        self.samples, self.marks, self.logs = [sample], [self.ledger.mark()], []

    def initial_growth(self, sample):
        return sample.global_growth

    def initial_ticks(self, sample):
        return sample.ticks

    def block(self, index):
        base = self.sample.pool.block
        return Block(
            chain=base.chain,
            number=base.number + index,
            hash=digest(f"synthetic-block-{index}"),
            timestamp=base.timestamp + index * self.seconds,
        )

    def liquidity(self, lower, upper, change):
        self.ledger.modify(lower, upper, change)
        self.logs.append(self.log(len(self.samples), lower, upper, change))

    def reinitialize(self, tick, partner):
        """Clear and re-create `tick` inside one interval, so its growth is not continuous.

        The partner range above it lends gross liquidity for the clearing burn and is
        restored afterwards; every step is a retained Mint or Burn event.
        """
        gross = self.ledger.ticks[tick][0]
        spacing = self.sample.pool.source.tick_spacing
        self.liquidity(partner - spacing, partner, gross)
        self.liquidity(tick, partner, -gross)
        self.liquidity(tick, partner, gross)
        self.liquidity(partner - spacing, partner, -gross)

    def log(self, index, lower, upper, change):
        if change > 0:
            topics = (POOL_MINT, OWNER, topic(lower), topic(upper))
            data = encode(("address", "uint128", "uint256", "uint256"), (SENDER, change, 0, 0))
        else:
            topics = (POOL_BURN, OWNER, topic(lower), topic(upper))
            data = encode(("uint128", "uint256", "uint256"), (-change, 0, 0))
        return HistoricalLog(
            block=self.block(index),
            transaction_hash=digest(f"synthetic-log-{len(self.logs)}"),
            transaction_index=len(self.logs),
            log_index=0,
            address=self.target.pool.pool,
            topics=topics,
            data="0x" + data.hex(),
        )

    def step(self, growth, tick):
        self.ledger.accrue(*growth)
        self.ledger.move(tick)
        block = self.block(len(self.samples))
        pool = self.sample.pool.model_copy(
            update={
                "block": block,
                "tick": tick,
                "sqrt_price_x96": sqrt_at_tick(tick),
                "liquidity": self.ledger.liquidity,
            }
        )
        words, covered = self.bitmap()
        self.samples.append(self.next_sample(pool, words, covered))
        self.marks.append(self.ledger.mark())

    def next_sample(self, pool, words, covered):
        ticks = tuple(
            TickState(index=t, gross=s[0], net=s[1], outside0=s[2], outside1=s[3])
            for t, s in ((t, self.ledger.ticks[t]) for t in covered)
        )
        return type(self.sample).model_validate(
            {
                **self.sample.model_dump(),
                "pool": pool.model_dump(),
                "words": words,
                "ticks": ticks,
                "global_growth": tuple(self.ledger.growth),
            }
        )

    def bitmap(self):
        spacing = self.sample.pool.source.tick_spacing
        indices = tuple(word.index for word in self.sample.words)
        covered = sorted(t for t in self.ledger.ticks if (t // spacing) >> 8 in indices)
        bits = dict.fromkeys(indices, 0)
        for t in covered:
            bits[(t // spacing) >> 8] |= 1 << ((t // spacing) & 255)
        return tuple(Word(index=index, bitmap=bits[index]) for index in indices), covered

    def blocks(self):
        return tuple(sample.pool.block for sample in self.samples)


def publish(directory, config, settings, path, *, drop_event=None):
    """Publish the path as a prepared dataset; `drop_event` omits one retained log."""
    target = config.target("position")
    blocks = path.blocks()
    events = tuple(e for i, e in enumerate(path.logs) if i != drop_event)
    baseline = BoundaryEvidence(
        target=target,
        first=blocks[0],
        last=blocks[-1],
        initial_gross=None,
        final_gross=None,
        events=events,
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
            raw = RawEvidence(body=sample.model_dump_json())
            record = Record(
                key=key(target),
                unit=UNIT,
                payload=PreparedSample(**sample.model_dump(), baseline_digest=proof.digest),
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
