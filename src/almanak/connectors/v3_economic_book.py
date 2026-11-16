"""Offline V3 pool timelines retain exact source and range-continuity evidence."""

from bisect import bisect_left
from itertools import pairwise
from types import ModuleType

from almanak.backtest.prepared import load_dataset
from almanak.backtest.strategy_prices import StrategySeries, strategy_requirements
from almanak.connectors.band_fee_growth import (
    band_growth,
    partition,
    validate_band_coverage,
    validate_covered,
)
from almanak.connectors.dex_economic_dataset import identity, targets
from almanak.connectors.v3_fee_growth import sampled_inside
from almanak.connectors.v3_pool_history import BoundaryEvidence, continuous_gross, liquidity_event


class PoolBook:
    data: ModuleType
    boundary_type: type[BoundaryEvidence]

    @classmethod
    def identity(cls, config, settings):
        return identity(config, settings, cls.data.MODEL, cls.data.REVISION)

    def __init__(self, directory, config, settings, expected_hash=None):
        self.manifest, self.reader = load_dataset(directory, expected_hash)
        self.settings = settings
        self.timelines = {}
        self.baselines = {}
        self.bands = {}
        try:
            preparation = self.manifest.preparation
            if (preparation.model, preparation.acquisition_revision, preparation.policy_digest) != (
                self.data.MODEL,
                self.data.REVISION,
                self.identity(config, settings),
            ):
                raise ValueError("prepared V3 pools differ from the frozen model or policy")
            if preparation.requirements != tuple(
                self.data.requirement(t, settings) for t in targets(config)
            ) + strategy_requirements(settings):
                raise ValueError("prepared V3 requirements differ from configured coverage")
            for target in targets(config):
                self.load_target(target)
            self.prices = StrategySeries(self.reader, self.manifest, settings)
        except BaseException:
            self.close()
            raise

    def load_target(self, target):
        timeline = self.reader.timeline(
            self.manifest.preparation.requirement(self.data.key(target)), self.data.Record
        )
        digests = {row.payload.baseline_digest for row in timeline.rows}
        if len(digests) != 1:
            raise ValueError("V3 pool samples must share one retained continuity proof")
        baseline = self.boundary_type.model_validate_json(
            self.reader.evidence(next(iter(digests))).body
        )
        self.validate_baseline(target, baseline, timeline)
        for row in timeline.rows:
            self.validate_row(target, baseline, row)
        self.timelines[target] = timeline
        self.baselines[target] = baseline
        if target.band is not None:
            self.bands[target] = self.band_growth(target, baseline, timeline)

    def band_growth(self, target, baseline, timeline):
        rows = timeline.rows
        validate_band_coverage(target.band, rows[0].payload, target.pool.tick_spacing)
        words = {word.index for word in rows[0].payload.words}
        spacing = target.pool.tick_spacing
        events = tuple((e.block.number, liquidity_event(target, e)) for e in baseline.events)
        changes = partition(events, tuple(row.reference.number for row in rows))
        return band_growth(
            tuple(row.payload for row in rows),
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

    def validate_baseline(self, target, baseline, timeline):
        if baseline.target != target or baseline.first != timeline.rows[0].reference:
            raise ValueError("V3 continuity evidence identifies another target or start")
        if baseline.last.timestamp > self.settings.schedule.deadline:
            raise ValueError("V3 continuity evidence exceeds the configured window")
        if len(baseline.events) > self.settings.data.max_liquidity_events:
            raise ValueError("V3 continuity evidence exceeds the configured event limit")
        anchors = (*tuple(row.reference for row in timeline.rows), baseline.last)
        for prior, current in pairwise(anchors):
            if current.number < prior.number or (
                current.number == prior.number and current != prior
            ):
                raise ValueError("V3 samples require one increasing canonical block path")

    def validate_row(self, target, baseline, row):
        payload, block = row.payload, row.reference
        bound = self.settings.data.for_target(target)
        if payload.pool.source != target.pool or payload.pool.block != block:
            raise ValueError("prepared V3 pool has another chain, pool or block identity")
        if row.source_time != block.timestamp or (payload.lower, payload.upper) != (
            bound.lower,
            bound.upper,
        ):
            raise ValueError("prepared V3 time or coverage differs from its retained evidence")
        if (
            len(payload.words) > self.settings.data.max_bitmap_words
            or len(payload.ticks) > self.settings.data.max_initialized_ticks
        ):
            raise ValueError("prepared V3 pool exceeds configured collection limits")
        if not baseline.first.number <= block.number <= baseline.last.number:
            raise ValueError("prepared V3 sample is outside continuity evidence")
        if payload.baseline_digest not in row.evidence:
            raise ValueError("prepared V3 sample omits its continuity evidence")
        for digest in row.evidence:
            self.reader.verify(digest)
        if target.range is not None:
            self.validate_range(target, baseline, row)

    def validate_range(self, target, baseline, row):
        sampled_inside(row.payload, target)
        events = tuple(e for e in baseline.events if e.block.number <= row.reference.number)
        ticks = {t.index: t.gross for t in row.payload.ticks}
        measured = tuple(ticks[t] for t in (target.range.lower, target.range.upper))
        if continuous_gross(target, baseline.initial_gross, events) != measured:
            raise ValueError("prepared V3 range differs from its continuity evidence")

    def at(self, target, time):
        if target not in self.timelines:
            raise ValueError("requested V3 pool is outside prepared source bindings")
        return self.timelines[target].at(time)

    def close(self):
        self.reader.close()
