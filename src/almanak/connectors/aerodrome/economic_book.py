"""Offline pool samples are decoded again from retained read-only RPC evidence."""

from almanak.backtest.prepared import load_dataset
from almanak.backtest.strategy_prices import StrategySeries, strategy_requirements
from almanak.connectors.aerodrome import economic_data
from almanak.connectors.aerodrome.economic_acquisition import sample
from almanak.connectors.aerodrome.economic_collection import sample_count
from almanak.connectors.dex_economic_dataset import identity as dataset_identity
from almanak.connectors.dex_economic_dataset import targets
from almanak.connectors.dex_economic_replay import RetainedReader as RetainedReader


class PoolBook:
    @staticmethod
    def identity(config, settings):
        return dataset_identity(config, settings, economic_data.MODEL, economic_data.REVISION)

    def __init__(self, directory, config, settings, expected_hash=None):
        self.manifest, self.reader = load_dataset(directory, expected_hash)
        self.settings = settings
        self.timelines = {}
        try:
            preparation = self.manifest.preparation
            if (preparation.model, preparation.acquisition_revision, preparation.policy_digest) != (
                economic_data.MODEL,
                economic_data.REVISION,
                self.identity(config, settings),
            ):
                raise ValueError("prepared Aerodrome pools differ from the frozen model or policy")
            if preparation.requirements != tuple(
                economic_data.requirement(t, settings) for t in targets(config)
            ) + strategy_requirements(settings):
                raise ValueError("prepared Aerodrome requirements differ from configured coverage")
            for target in targets(config):
                self.load_target(target)
            self.prices = StrategySeries(self.reader, self.manifest, settings)
        except BaseException:
            self.close()
            raise

    def load_target(self, target):
        timeline = self.reader.timeline(
            self.manifest.preparation.requirement(economic_data.key(target)), economic_data.Record
        )
        if (
            len(timeline.rows) != sample_count(self.settings)
            or len(timeline.rows) > self.settings.data.max_samples
        ):
            raise ValueError("prepared Aerodrome sample count differs from its explicit grid")
        previous = None
        for index, row in enumerate(timeline.rows):
            self.validate_row(target, row, index)
            if previous is not None:
                self.validate_continuity(previous, row)
            previous = row
        self.timelines[target] = timeline

    def validate_row(self, target, row, index):
        if (row.payload.pool.source, row.payload.pool.block) != (target.pool, row.reference):
            raise ValueError("prepared Aerodrome sample has another pool or block identity")
        expected_at = self.settings.schedule.start + index * self.settings.data.sample_seconds
        if row.available_at != expected_at or row.source_time != row.reference.timestamp:
            raise ValueError("prepared Aerodrome sample differs from its configured time grid")
        if expected_at - row.source_time > self.settings.data.max_age_seconds:
            raise ValueError("prepared Aerodrome sample exceeds its source age limit")
        reader = RetainedReader(
            target.fund.chain, tuple(self.reader.evidence(d) for d in row.evidence)
        )
        if reader.pin_block(row.reference.number) != row.reference:
            raise ValueError("prepared Aerodrome reference differs from retained canonical header")
        if sample(reader, target, row.reference) != row.payload:
            raise ValueError("prepared Aerodrome payload differs from retained RPC evidence")

    @staticmethod
    def validate_continuity(previous, current):
        if previous.payload.contracts != current.payload.contracts:
            raise ValueError("prepared Aerodrome implementation identities changed")
        if current.reference.number < previous.reference.number or (
            current.reference.number == previous.reference.number
            and current.reference != previous.reference
        ):
            raise ValueError("prepared Aerodrome samples require increasing canonical blocks")
        if any(
            getattr(current.payload.pool, f"index{i}") < getattr(previous.payload.pool, f"index{i}")
            for i in (0, 1)
        ):
            raise ValueError("prepared Aerodrome fee indices moved backwards")

    def at(self, target, time):
        if target not in self.timelines:
            raise ValueError("requested Aerodrome pool is outside prepared source bindings")
        return self.timelines[target].at(time)

    def close(self):
        self.reader.close()
