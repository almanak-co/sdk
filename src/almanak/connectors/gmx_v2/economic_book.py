"""Frozen GMX frames prove complete bounded risk and signed-report source coverage offline."""

from itertools import pairwise

from almanak.backtest.prepared import load_dataset
from almanak.backtest.strategy_prices import StrategySeries, strategy_requirements
from almanak.connectors.dex_economic_dataset import identity, targets
from almanak.connectors.gmx_v2 import economic_data as data
from almanak.connectors.gmx_v2.economic_replay import ReplayReader, validate_frame
from almanak.connectors.gmx_v2.published_collection import scan_publications


class MarketBook:
    @staticmethod
    def identity(config, settings):
        return identity(config, settings, *data.revisions(config))

    def __init__(self, directory, config, settings, expected_hash=None):
        self.manifest, self.reader = load_dataset(directory, expected_hash)
        self.settings = settings
        self.timelines = {}
        try:
            preparation = self.manifest.preparation
            if (preparation.model, preparation.acquisition_revision, preparation.policy_digest) != (
                *data.revisions(config),
                self.identity(config, settings),
            ):
                raise ValueError("GMX prepared inputs differ from the frozen model or policy")
            if preparation.requirements != tuple(
                data.requirement(t, settings) for t in targets(config)
            ) + strategy_requirements(settings):
                raise ValueError("GMX prepared requirements differ from configured coverage")
            for target in targets(config):
                self.load_target(target)
            self.prices = StrategySeries(self.reader, self.manifest, settings)
        except BaseException:
            self.close()
            raise

    def load_target(self, target):
        timeline = self.reader.timeline(data.requirement(target, self.settings), data.Record)
        first, last = timeline.rows[0].reference, timeline.rows[-1].reference
        count = sum(len(row.payload.risk) for row in timeline.rows)
        if (
            count != last.number - first.number + 1
            or count > self.settings.data.maximum_risk_blocks
        ):
            raise ValueError("GMX prepared risk does not cover its exact bounded block range")
        for previous, current in pairwise(timeline.rows):
            if previous.reference.number + 1 != current.payload.risk[0].block.number:
                raise ValueError("GMX prepared risk omitted an intervening canonical block")
        family = data.risk_family(self.settings)
        if any(type(row.payload.risk[0]) is not family for row in timeline.rows):
            raise ValueError("GMX prepared frames differ from the selected request family")
        candidates = self.validate_boundaries(target, timeline)
        source = self.settings.data.publication(target.allocation)
        for row in timeline.rows:
            self.validate_row(target, row)
            with ReplayReader(target.fund.chain, self.reader, row.evidence) as replay:
                validate_frame(replay, row.payload, source, candidates)
        self.timelines[target] = timeline

    def validate_boundaries(self, target, timeline):
        first, last = timeline.rows[0].reference, timeline.rows[-1].reference
        source = self.settings.data.publication(target.allocation)
        with ReplayReader(target.fund.chain, self.reader, timeline.rows[0].evidence) as replay:
            following_first = replay.pin_block(first.number + 1)
            following_last = replay.pin_block(last.number + 1)
            if not first.timestamp <= self.settings.schedule.start < following_first.timestamp:
                raise ValueError(
                    "GMX prepared initialization differs from the canonical time floor"
                )
            if not last.timestamp <= self.settings.schedule.deadline < following_last.timestamp:
                raise ValueError("GMX prepared coverage does not reach its exact time deadline")
            return scan_publications(
                replay,
                data.accounting_market(target.market),
                source,
                first,
                last,
                self.settings.data.publication_limits,
            )

    def validate_row(self, target, row):
        if row.payload.target != target or row.payload.block != row.reference:
            raise ValueError("GMX prepared frame differs from its exact target or canonical block")
        if row.source_time != row.reference.timestamp or row.available_at != max(
            self.settings.schedule.start, row.reference.timestamp
        ):
            raise ValueError("GMX prepared frame changed its canonical publication time")

    def at(self, target, time):
        if target not in self.timelines:
            raise ValueError("GMX request is outside prepared target bindings")
        return self.timelines[target].at(time)

    def close(self):
        self.reader.close()
