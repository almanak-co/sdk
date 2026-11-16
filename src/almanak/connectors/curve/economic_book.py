"""Verify accounting samples and complete parameter scans again from retained evidence."""

from almanak.backtest.evm import EvmHistory
from almanak.backtest.prepared import load_dataset
from almanak.backtest.strategy_prices import StrategySeries, strategy_requirements
from almanak.connectors.curve import economic_data
from almanak.connectors.curve.economic_acquisition import sample
from almanak.connectors.curve.economic_collection import sample_times
from almanak.connectors.curve.economic_history import scan, validate_parameters
from almanak.connectors.dex_economic_dataset import identity as dataset_identity
from almanak.connectors.dex_economic_dataset import targets
from almanak.connectors.dex_economic_replay import RetainedReader


class PoolBook:
    @staticmethod
    def identity(config, settings):
        return dataset_identity(config, settings, *economic_data.model_identity(config))

    def __init__(self, directory, config, settings, expected_hash=None):
        self.manifest, self.reader = load_dataset(directory, expected_hash)
        self.settings = settings
        self.timelines = {}
        self.paths = {}
        try:
            preparation = self.manifest.preparation
            if (preparation.model, preparation.acquisition_revision, preparation.policy_digest) != (
                *economic_data.model_identity(config),
                self.identity(config, settings),
            ):
                raise ValueError("prepared Curve pools differ from the frozen model or policy")
            if preparation.requirements != tuple(
                economic_data.requirement(t, settings) for t in targets(config)
            ) + strategy_requirements(settings):
                raise ValueError("prepared Curve requirements differ from configured coverage")
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
        digests = {row.payload.parameter_path for row in timeline.rows}
        if len(digests) != 1:
            raise ValueError("Curve samples must share one complete parameter path")
        path = economic_data.ParameterPath.model_validate_json(
            self.reader.evidence(next(iter(digests))).body
        )
        self.validate_path(target, path)
        if tuple(row.available_at for row in timeline.rows) != sample_times(self.settings, path):
            raise ValueError("Curve samples omit a grid time or parameter change")
        previous = None
        for row in timeline.rows:
            self.validate_row(target, row)
            if previous is not None:
                validate_continuity(previous, row, path)
            previous = row
        self.timelines[target], self.paths[target] = timeline, path

    def validate_path(self, target, path):
        if path.source != target.pool:
            raise ValueError("Curve parameter path identifies another pool")
        schedule = self.settings.schedule
        if not (
            path.first.timestamp <= schedule.start < path.first_next.timestamp
            and path.last.timestamp <= schedule.deadline < path.last_next.timestamp
        ):
            raise ValueError("Curve parameter path differs from its frozen historical window")
        reader = RetainedReader(
            target.fund.chain, tuple(self.reader.evidence(d) for d in path.inputs)
        )
        history = EvmHistory(reader)
        for block in (path.first, path.first_next, path.last, path.last_next):
            history.pin(block)
        if scan(history, target.pool, path.first, path.last, self.settings.data) != path.events:
            raise ValueError("Curve parameter path differs from retained canonical log responses")

    def validate_row(self, target, row):
        if (row.payload.pool.source, row.payload.pool.block) != (target.pool, row.reference):
            raise ValueError("prepared Curve sample has another pool or block identity")
        if (
            row.source_time != row.reference.timestamp
            or not row.source_time <= row.available_at < row.payload.successor.timestamp
        ):
            raise ValueError("prepared Curve sample differs from its canonical time floor")
        if row.available_at - row.source_time > self.settings.data.max_age_seconds:
            raise ValueError("prepared Curve sample exceeds its source age limit")
        if row.payload.parameter_path not in row.evidence:
            raise ValueError("prepared Curve sample omits its parameter path evidence")
        raw = tuple(
            self.reader.evidence(d) for d in row.evidence if d != row.payload.parameter_path
        )
        reader = RetainedReader(target.fund.chain, raw)
        for block in (row.payload.successor, row.reference):
            if reader.pin_block(block.number) != block:
                raise ValueError("Curve sample header differs from retained RPC evidence")
        if (
            sample(reader, target, row.reference, row.payload.parameter_path, row.payload.successor)
            != row.payload
        ):
            raise ValueError("prepared Curve payload differs from retained RPC evidence")

    def at(self, target, time):
        if target not in self.timelines:
            raise ValueError("requested Curve pool is outside prepared source bindings")
        return self.timelines[target].at(time)

    def close(self):
        self.reader.close()


def validate_continuity(previous, current, path):
    if previous.payload.contracts != current.payload.contracts:
        raise ValueError("prepared Curve executable identities changed")
    if current.reference.number < previous.reference.number or (
        current.reference.number == previous.reference.number
        and current.reference != previous.reference
    ):
        raise ValueError("prepared Curve samples require increasing canonical blocks")
    events = tuple(
        event
        for event in path.events
        if previous.reference.number < event.block.number <= current.reference.number
    )
    validate_parameters(previous.payload.pool, current.payload.pool, events)
