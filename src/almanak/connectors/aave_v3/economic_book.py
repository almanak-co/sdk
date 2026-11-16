"""Immutable V3 accounting samples and complete end-of-block risk observations."""

from bisect import bisect_right

from almanak.backtest.prepared import load_dataset
from almanak.backtest.strategy_prices import StrategySeries
from almanak.connectors.aave_v3.economic_data import (
    MODEL,
    REVISION,
    Market,
    RateCurve,
    RateRecord,
    RiskRecord,
    key,
    requirement,
)
from almanak.connectors.aave_v3.economic_parameters import allocations, identity, requirements


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
                raise ValueError("V3 dataset differs from the frozen model or data policy")
            if prepared.requirements != requirements(config, settings):
                raise ValueError("V3 dataset differs from its configured market requirements")
            for allocation in allocations(config):
                self.load(allocation)
            self.validate_counts()
            self.prices = StrategySeries(self.reader, self.manifest, settings)
        except BaseException:
            self.close()
            raise

    def load(self, allocation):
        source = allocation.reserve
        rates = self.reader.timeline(requirement(source, self.settings, "accounting"), RateRecord)
        risks = self.reader.timeline(requirement(source, self.settings, "risk"), RiskRecord)
        self.rates[source] = RateCurve(rates)
        previous = None
        for record in risks.rows:
            self.validate_risk(allocation, record, previous)
            previous = record
        if risks.rows[-1].payload.following_block.timestamp <= self.settings.schedule.deadline:
            raise ValueError("V3 risk history ends before its configured coverage boundary")
        self.risks[source] = risks
        for record in (*rates.rows, *risks.rows):
            for digest in record.evidence:
                self.reader.evidence(digest)

    def validate_risk(self, allocation, record, previous):
        sample, source = record.payload, allocation.reserve
        block = sample.reserve.block
        if (
            record.key != key(source, "risk")
            or sample.reserve.source != source
            or record.reference != block
        ):
            raise ValueError("V3 risk record differs from its configured reserve or block")
        if record.source_time != block.timestamp or record.available_at != max(
            block.timestamp, self.settings.schedule.start
        ):
            raise ValueError("V3 risk record has unsupported historical visibility")
        if sample.following_block.timestamp <= record.available_at:
            raise ValueError("V3 risk initialization is not the latest eligible block")
        if previous is not None and previous.payload.following_block != sample.block_group[0]:
            raise ValueError("V3 risk history has a missing canonical block")
        quote = sample.reserve.oracle
        if quote is None or (quote.oracle, quote.feed) != (
            allocation.oracle,
            allocation.price_source,
        ):
            raise ValueError("V3 prepared risk lacks its configured oracle evidence")

    def validate_counts(self):
        if (
            sum(len(t.records.rows) for t in self.rates.values())
            > self.settings.data.max_accounting_samples
        ):
            raise ValueError("prepared accounting exceeds its explicit sample limit")
        if sum(len(t.rows) for t in self.risks.values()) > self.settings.data.max_risk_samples:
            raise ValueError("prepared risk exceeds its explicit sample limit")
        blocks = {
            (b.chain, b.number)
            for t in self.risks.values()
            for r in t.rows
            for b in r.payload.block_group
        }
        if len(blocks) > self.settings.data.max_risk_blocks:
            raise ValueError("prepared risk exceeds its explicit canonical-block limit")

    def at(self, source, time):
        if source not in self.risks:
            raise ValueError("requested V3 reserve is outside the prepared dataset")
        record = self.risks[source].at(time)
        supply_index, debt_index = self.rates[source].at(time)
        return Market(record.payload, supply_index, debt_index, time)

    def evidence(self, source, time):
        records = (self.rates[source].records.at(time), self.risks[source].at(time))
        # Earlier accounting boundaries determine the integrated index. Retain
        # their evidence as well as the currently selected risk observation.
        rates = tuple(r for r in self.rates[source].records.rows if r.available_at <= time)
        return tuple(
            sorted({digest for record in (*rates, *records) for digest in record.evidence})
        )

    def next_boundary(self, time, until):
        candidates: list[int] = []
        for source, risks in self.risks.items():
            for timeline in (risks, self.rates[source].records):
                candidates.extend(
                    t
                    for t in (timeline.next(time), timeline.expiry(time))
                    if t is not None and t <= until
                )
            index = bisect_right(risks.times, time) - 1
            if index < 0:
                continue
            sample = risks.rows[index].payload
            grace = sample.liquidation_grace_until + 1
            if time < grace <= until:
                candidates.append(grace)
        return min(candidates) if candidates else None

    def close(self):
        self.reader.close()
