"""Offline source-checked pool timelines and retained LP continuity proofs."""

from bisect import bisect_left
from dataclasses import dataclass

from almanak.backtest.prepared import load_dataset
from almanak.backtest.strategy_prices import StrategySeries, strategy_requirements
from almanak.connectors.band_fee_growth import (
    band_growth,
    partition,
    validate_band_coverage,
    validate_covered,
)
from almanak.connectors.uniswap_v4.economic_data import (
    MODEL,
    REVISION,
    GrowthTick,
    Record,
    key,
    requirement,
)
from almanak.connectors.uniswap_v4.economic_prepare import identity, targets
from almanak.connectors.uniswap_v4.history import BoundaryEvidence, continuous_gross, modification


@dataclass(frozen=True)
class GrowthSample:
    """The sampled state bracketed band growth reads, detached from the stored payload."""

    pool: object
    ticks: tuple[GrowthTick, ...]
    global_growth: tuple[int, int]


def growth_sample(payload):
    return GrowthSample(
        pool=payload.pool, ticks=payload.growth_ticks(), global_growth=payload.global_growth
    )


class PoolBook:
    def __init__(self, directory, config, settings, expected_hash=None):
        self.manifest, self.reader = load_dataset(directory, expected_hash)
        self.settings = settings
        self.timelines = {}
        self.baselines = {}
        self.bands = {}
        try:
            preparation = self.manifest.preparation
            if (preparation.model, preparation.acquisition_revision, preparation.policy_digest) != (
                MODEL,
                REVISION,
                identity(config, settings),
            ):
                raise ValueError("prepared pools differ from the frozen model or policy")
            if preparation.requirements != tuple(
                requirement(t, settings) for t in targets(config)
            ) + strategy_requirements(settings):
                raise ValueError("prepared pool requirements differ from configured coverage")
            for target in targets(config):
                self.load_target(target)
            self.prices = StrategySeries(self.reader, self.manifest, settings)
        except BaseException:
            self.close()
            raise

    def load_target(self, target):
        timeline = self.reader.timeline(self.manifest.preparation.requirement(key(target)), Record)
        baselines = set()
        for row in timeline.rows:
            self.validate_row(target, row)
            baselines.add(row.payload.baseline_digest)
        if len(baselines) != 1:
            raise ValueError("pool samples must use one retained continuity proof")
        proof = self.reader.evidence(next(iter(baselines)))
        baseline = BoundaryEvidence.model_validate_json(proof.body)
        self.validate_baseline(target, baseline, timeline)
        self.timelines[target] = timeline
        self.baselines[target] = baseline
        if target.band is not None:
            self.bands[target] = self.band_growth(target, baseline, timeline)

    def band_growth(self, target, baseline, timeline):
        rows = timeline.rows
        validate_band_coverage(target.band, rows[0].payload, target.pool.tick_spacing)
        words = {word.index for word in rows[0].payload.words}
        spacing = target.pool.tick_spacing
        events = tuple((int(e["blockNumber"], 16), modification(e)) for e in baseline.events)
        changes = partition(events, tuple(row.reference.number for row in rows))
        return band_growth(
            tuple(growth_sample(row.payload) for row in rows),
            changes,
            lambda tick: ((tick // spacing) >> 8) in words,
        )

    def band_bounds(self, target, record, selected):
        """Bracketed cumulative growth inside a band position's range at a prepared sample."""
        if target not in self.bands:
            raise ValueError("banded fee growth requires a prepared band target")
        timeline = self.timelines[target]
        index = bisect_left(timeline.times, record.available_at)
        if index == len(timeline.rows) or timeline.rows[index] != record:
            raise ValueError("banded fee growth requires a retained prepared sample")
        validate_covered(record.payload.lower, record.payload.upper, selected)
        return self.bands[target].at(index, selected.lower, selected.upper)

    def validate_row(self, target, row):
        payload, block = row.payload, row.reference
        bound = self.settings.data.for_target(target)
        if payload.pool.source != target.pool or payload.pool.block != block:
            raise ValueError("prepared pool has another chain, pool or block identity")
        if row.source_time != block.timestamp or (payload.lower, payload.upper) != (
            bound.lower,
            bound.upper,
        ):
            raise ValueError("prepared pool time or tick bounds differ from retained evidence")
        if (
            len(payload.words) > self.settings.data.max_bitmap_words
            or len(payload.ticks) > self.settings.data.max_initialized_ticks
        ):
            raise ValueError("prepared pool exceeds configured collection limits")
        if (payload.fee_growth is not None) != (target.range is not None):
            raise ValueError("prepared LP range requires explicit fee growth")
        if (payload.outside is not None) != (target.band is not None):
            raise ValueError("prepared liquidity band requires sampled tick growth")
        for digest in row.evidence:
            self.reader.evidence(digest)

    def validate_baseline(self, target, baseline, timeline):
        if baseline.target != target or baseline.first != timeline.rows[0].reference:
            raise ValueError("prepared boundary evidence identifies another target or start")
        if baseline.last.timestamp > self.settings.schedule.deadline:
            raise ValueError("prepared boundary evidence exceeds the configured window")
        for row in timeline.rows:
            if row.reference.number > baseline.last.number:
                raise ValueError("prepared sample is outside continuity evidence")
            if target.range is not None:
                events = tuple(
                    e for e in baseline.events if int(e["blockNumber"], 16) <= row.reference.number
                )
                if (
                    continuous_gross(target, baseline.initial_gross, events)
                    != row.payload.boundary_gross
                ):
                    raise ValueError(
                        "prepared LP boundary state differs from its continuity evidence"
                    )

    def at(self, target, time):
        if target not in self.timelines:
            raise ValueError("requested pool is outside prepared source bindings")
        return self.timelines[target].at(time)

    def close(self):
        self.reader.close()
