"""Immutable Morpho accounting and complete end-of-block risk evidence."""

from almanak.backtest.prepared import load_dataset
from almanak.backtest.strategy_prices import StrategySeries
from almanak.connectors.morpho_blue.economic_data import (
    MODEL,
    REVISION,
    RateRecord,
    RiskRecord,
    key,
    requirement,
)
from almanak.connectors.morpho_blue.economic_parameters import identity, markets, requirements


class Book:
    def __init__(self, directory, config, settings, expected_hash=None):
        self.manifest, self.reader = load_dataset(directory, expected_hash)
        self.config, self.settings = config, settings
        self.rates, self.risks = {}, {}
        try:
            prepared = self.manifest.preparation
            if (prepared.model, prepared.acquisition_revision, prepared.policy_digest) != (
                MODEL,
                REVISION,
                identity(config, settings),
            ):
                raise ValueError("Morpho dataset differs from the frozen model or data policy")
            if prepared.requirements != requirements(config, settings):
                raise ValueError("Morpho dataset differs from its configured market requirements")
            for source in markets(config):
                self.load(source)
            self.validate_counts()
            self.prices = StrategySeries(self.reader, self.manifest, settings)
        except BaseException:
            self.close()
            raise

    def load(self, source):
        rates = self.reader.timeline(requirement(source, self.settings, "accounting"), RateRecord)
        risks = self.reader.timeline(requirement(source, self.settings, "risk"), RiskRecord)
        self.validate_rates(source, rates)
        self.rates[source] = rates
        previous = None
        for record in risks.rows:
            self.validate_risk(source, record, previous)
            previous = record
        if risks.rows[-1].payload.following_block.timestamp <= self.settings.schedule.deadline:
            raise ValueError("Morpho risk history ends before its configured coverage boundary")
        self.risks[source] = risks
        for observed in (
            *(record.payload for record in rates.rows),
            *(record.payload.market for record in risks.rows),
        ):
            self.validate_targets(observed)
        for record in (*rates.rows, *risks.rows):
            for digest in record.evidence:
                self.reader.evidence(digest)

    def validate_targets(self, observed):
        from almanak.configuration.metadata import verify

        verify(self.config, observed.loan_asset)
        verify(self.config, observed.collateral_asset)

    def validate_rates(self, source, rates):
        interval = self.settings.data.sample_seconds
        if rates.times != tuple(
            range(self.settings.schedule.start, self.settings.schedule.deadline + 1, interval)
        ):
            raise ValueError("Morpho accounting is missing a fixed sample boundary")
        for record in rates.rows:
            if record.key != key(source, "accounting") or record.payload.source != source:
                raise ValueError("Morpho accounting mixes configured markets")
            if (
                record.reference != record.payload.block
                or record.source_time != record.reference.timestamp
            ):
                raise ValueError("Morpho accounting differs from its pinned historical evidence")

    def validate_risk(self, source, record, previous):
        sample = record.payload
        block = sample.market.block
        if (
            record.key != key(source, "risk")
            or sample.market.source != source
            or record.reference != block
        ):
            raise ValueError("Morpho risk record differs from its configured market or block")
        if record.source_time != block.timestamp or record.available_at != max(
            block.timestamp, self.settings.schedule.start
        ):
            raise ValueError("Morpho risk record has unsupported historical visibility")
        if sample.following_block.timestamp <= record.available_at:
            raise ValueError("Morpho risk initialization is not the latest eligible block")
        if previous is not None and previous.payload.following_block != sample.block_group[0]:
            raise ValueError("Morpho risk history has a missing canonical block")

    def validate_counts(self):
        if (
            sum(len(t.rows) for t in self.rates.values())
            > self.settings.data.max_accounting_samples
        ):
            raise ValueError("Morpho prepared accounting exceeds its explicit sample limit")
        if sum(len(t.rows) for t in self.risks.values()) > self.settings.data.max_risk_samples:
            raise ValueError("Morpho prepared risk exceeds its explicit sample limit")
        blocks = {
            (b.chain, b.number)
            for t in self.risks.values()
            for r in t.rows
            for b in r.payload.block_group
        }
        if len(blocks) > self.settings.data.max_risk_blocks:
            raise ValueError("Morpho prepared risk exceeds its explicit canonical-block limit")

    def at(self, source, time):
        if source not in self.risks:
            raise ValueError("requested Morpho market is outside the prepared dataset")
        self.rates[source].at(time)
        return self.risks[source].at(time).payload.market

    def evidence(self, source, time):
        risk = self.risks[source].at(time)
        self.rates[source].at(time)
        # Every preceding accounting boundary contributes to the modeled market.
        rates = tuple(r for r in self.rates[source].rows if r.available_at <= time)
        return tuple(sorted({d for record in (*rates, risk) for d in record.evidence}))

    def next_boundary(self, time, until):
        candidates = [
            boundary
            for source, risk in self.risks.items()
            for timeline in (risk, self.rates[source])
            for boundary in (timeline.next(time), timeline.expiry(time))
            if boundary is not None and boundary <= until
        ]
        return min(candidates) if candidates else None

    def close(self):
        self.reader.close()
