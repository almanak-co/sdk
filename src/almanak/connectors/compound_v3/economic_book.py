"""Immutable whole-Comet accounting and complete end-of-block risk evidence."""

from almanak.backtest.prepared import load_dataset
from almanak.backtest.strategy_prices import StrategySeries
from almanak.connectors.compound_v3.economic_data import (
    MODEL,
    REVISION,
    EconomicMarket,
    RateCurve,
    RateRecord,
    RiskRecord,
    key,
    requirement,
)
from almanak.connectors.compound_v3.economic_parameters import identity, markets, requirements


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
                raise ValueError("Comet dataset differs from the frozen model or data policy")
            if prepared.requirements != requirements(config, settings):
                raise ValueError("Comet dataset differs from its configured market requirements")
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
        self.rates[source] = RateCurve(rates)
        previous = None
        for record in risks.rows:
            self.validate_risk(source, record, previous)
            previous = record
        if risks.rows[-1].payload.following_block.timestamp <= self.settings.schedule.deadline:
            raise ValueError("Comet risk history ends before its configured coverage boundary")
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
        collateral = {c.asset_type: c.price_feed for c in observed.collaterals}
        for allocation in self.config.positions:
            reserve = allocation.reserve
            if reserve.market != observed.source or reserve.is_base:
                continue
            if collateral.get(reserve.asset) != reserve.price_feed:
                raise ValueError("Comet prepared market differs from its configured asset/feed")

    def validate_risk(self, source, record, previous):
        sample = record.payload
        block = sample.market.block
        if (
            record.key != key(source, "risk")
            or sample.market.source != source
            or record.reference != block
        ):
            raise ValueError("Comet risk record differs from its configured market or block")
        if record.source_time != block.timestamp or record.available_at != max(
            block.timestamp, self.settings.schedule.start
        ):
            raise ValueError("Comet risk record has unsupported historical visibility")
        if sample.following_block.timestamp <= record.available_at:
            raise ValueError("Comet risk initialization is not the latest eligible block")
        if previous is not None and previous.payload.following_block != sample.block_group[0]:
            raise ValueError("Comet risk history has a missing canonical block")

    def validate_counts(self):
        if (
            sum(len(t.records.rows) for t in self.rates.values())
            > self.settings.data.max_accounting_samples
        ):
            raise ValueError("Comet prepared accounting exceeds its explicit sample limit")
        if sum(len(t.rows) for t in self.risks.values()) > self.settings.data.max_risk_samples:
            raise ValueError("Comet prepared risk exceeds its explicit sample limit")
        blocks = {
            (b.chain, b.number)
            for t in self.risks.values()
            for r in t.rows
            for b in r.payload.block_group
        }
        if len(blocks) > self.settings.data.max_risk_blocks:
            raise ValueError("Comet prepared risk exceeds its explicit canonical-block limit")

    def at(self, source, time):
        if source not in self.risks:
            raise ValueError("requested Comet market is outside the prepared dataset")
        record = self.risks[source].at(time)
        supply, borrow = self.rates[source].at(time)
        return EconomicMarket(record.payload, supply, borrow, time)

    def evidence(self, source, time):
        risk = self.risks[source].at(time)
        self.rates[source].records.at(time)
        # Every preceding accounting boundary contributes to the modeled index.
        rates = tuple(r for r in self.rates[source].records.rows if r.available_at <= time)
        return tuple(sorted({d for record in (*rates, risk) for d in record.evidence}))

    def next_boundary(self, time, until):
        candidates = [
            boundary
            for source, risk in self.risks.items()
            for timeline in (risk, self.rates[source].records)
            for boundary in (timeline.next(time), timeline.expiry(time))
            if boundary is not None and boundary <= until
        ]
        return min(candidates) if candidates else None

    def close(self):
        self.reader.close()
