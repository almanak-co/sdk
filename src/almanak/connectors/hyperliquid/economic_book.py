"""Open a prepared HyperCore dataset and rebuild every record from its retained answer.

A record is accepted only if the `/info` answer it names, parsed again through the
closed shapes, yields exactly that record, so a changed record or answer refuses the
dataset. The book then serves each target's candles and funding rates by time.
"""

from bisect import bisect_right

from almanak.backtest.prepared import load_dataset
from almanak.connectors.dex_economic_dataset import identity, targets
from almanak.connectors.hyperliquid import economic_data as data
from almanak.gateway.hypercore_info import InfoResult, typed


class HyperCoreBook:
    @staticmethod
    def identity(config, settings):
        return identity(config, settings, data.MODEL, data.REVISION)

    def __init__(self, directory, config, settings, expected_hash=None):
        """Open the dataset, refusing another model, policy or coverage than configured."""
        self.manifest, self.reader = load_dataset(directory, expected_hash)
        self.settings = settings
        self.candles = {}
        self.funding = {}
        try:
            preparation = self.manifest.preparation
            if (preparation.model, preparation.acquisition_revision, preparation.policy_digest) != (
                data.MODEL,
                data.REVISION,
                self.identity(config, settings),
            ):
                raise ValueError("HyperCore prepared inputs differ from the frozen model or policy")
            expected = tuple(
                requirement
                for target in targets(config)
                for requirement in data.requirements(target, settings)
            )
            if preparation.requirements != expected:
                raise ValueError("HyperCore prepared requirements differ from configured coverage")
            for target in targets(config):
                self.load_target(target)
        except BaseException:
            self.close()
            raise

    def load_target(self, target):
        """Load one target's candles and funding, each rebuilt from its answer."""
        candles, funding = data.requirements(target, self.settings)
        candle_rows = self.reader.timeline(candles, data.CandleRecord)
        funding_rows = self.reader.timeline(funding, data.FundingRecord)
        seconds = self.settings.data.candle_seconds
        self.rebuild(
            candle_rows.rows,
            lambda answer, digest, received: tuple(
                data.candle_record(candles.key, candle, seconds, digest, received)
                for candle in answer
            ),
        )
        self.rebuild(
            funding_rows.rows,
            lambda answer, digest, received: tuple(
                data.funding_record(funding.key, rate, digest, received) for rate in answer
            ),
        )
        self.candles[target] = candle_rows
        self.funding[target] = funding_rows

    def rebuild(self, rows, records_of):
        """Refuse any row that differs from the record its retained answer rebuilds."""
        rebuilt = {}
        for digest in sorted({digest for row in rows for digest in row.evidence}):
            result = InfoResult.model_validate_json(self.reader.evidence(digest).body)
            for record in records_of(typed(result), digest, result.received_at / 1000):
                rebuilt[(digest, int(record.available_at))] = record
        for row in rows:
            (digest,) = row.evidence
            if rebuilt.get((digest, int(row.available_at))) != row:
                raise ValueError("HyperCore prepared record differs from its retained answer")

    def candle(self, target, time):
        return self.timeline(self.candles, target).at(time)

    def candles_between(self, target, start, end):
        """Candles that become available after `start` and at or before `end`."""
        return between(self.timeline(self.candles, target), start, end)

    def funding_between(self, target, start, end):
        """Funding rates that become available after `start` and at or before `end`."""
        return between(self.timeline(self.funding, target), start, end)

    @staticmethod
    def timeline(timelines, target):
        if target not in timelines:
            raise ValueError("HyperCore request is outside prepared target bindings")
        return timelines[target]

    def close(self):
        self.reader.close()


def between(timeline, start, end):
    return timeline.rows[bisect_right(timeline.times, start) : bisect_right(timeline.times, end)]
