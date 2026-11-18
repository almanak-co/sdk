"""Literal prepared lending records shared by the offline economic book tests.

Each case pairs one lending connector's prepared-data modules with a risk sample built
from the literal observations its offline tests already use, so book and acquisition
checks run without retained evidence.
"""

from dataclasses import dataclass, field
from types import ModuleType, SimpleNamespace

from almanak.backtest.dataset import Timeline
from almanak.backtest.types import UnixTime
from almanak.connectors.aave_v3 import economic_acquisition as aave_acquisition
from almanak.connectors.aave_v3 import economic_book as aave_book
from almanak.connectors.aave_v3 import economic_data as aave_data
from almanak.connectors.compound_v3 import economic_acquisition as compound_acquisition
from almanak.connectors.compound_v3 import economic_book as compound_book
from almanak.connectors.compound_v3 import economic_data as compound_data
from almanak.connectors.morpho_blue import economic_acquisition as morpho_acquisition
from almanak.connectors.morpho_blue import economic_book as morpho_book
from almanak.connectors.morpho_blue import economic_data as morpho_data
from almanak.connectors.sparklend import economic_acquisition as spark_acquisition
from almanak.connectors.sparklend import economic_book as spark_book
from almanak.connectors.sparklend import economic_data as spark_data
from tests import test_compound_v3_offline as compound_offline
from tests import test_morpho_blue_offline as morpho_offline
from tests.test_aave_v3_economic_actions import markets as aave_markets
from tests.test_sparklend_economic_actions import markets as spark_markets

DIGEST = "a" * 64
SAMPLE_SECONDS = 60
MAX_AGE = 120
SPACING = 12


@dataclass
class Case:
    name: str
    book: ModuleType
    data: ModuleType
    acquisition: ModuleType
    sample: object
    source: object
    # Aave V3 and SparkLend check risk against the allocation's oracle; the others
    # check against the configured market itself.
    subject: object
    rates_wrapped: bool
    observed_field: str
    extra: dict = field(default_factory=dict)

    @property
    def block(self):
        return self.observed(self.sample).block

    def observed(self, sample):
        return getattr(sample, self.observed_field)

    def at(self, sample, block):
        """The same sample re-pinned at another block, with its following boundary.

        Every nested quote pinned at the old block moves with it, so the observation
        passes its own validation again.
        """
        before = self.observed(sample)
        wire = before.model_dump_json().replace(
            before.block.model_dump_json(), block.model_dump_json()
        )
        observed = type(before).model_validate_json(wire)
        following = block.model_copy(
            update={"number": block.number + 1, "timestamp": block.timestamp + SPACING}
        )
        return type(sample).model_validate(
            dict(sample)
            | {self.observed_field: observed, "block_group": (block,), "following_block": following}
        )

    def history(self, count):
        """Consecutive one-block risk samples starting at the literal sample."""
        samples = [self.sample]
        while len(samples) < count:
            samples.append(self.at(samples[-1], samples[-1].following_block))
        return samples

    def settings(self, *, start=None, deadline=None, **limits):
        start = self.block.timestamp if start is None else start
        data = {
            "sample_seconds": SAMPLE_SECONDS,
            "max_age_seconds": MAX_AGE,
            "max_accounting_samples": 100,
            "max_risk_samples": 100,
            "max_risk_blocks": 100,
        } | limits
        return SimpleNamespace(
            schedule=SimpleNamespace(
                start=UnixTime(start),
                deadline=UnixTime(start + SAMPLE_SECONDS if deadline is None else deadline),
            ),
            data=SimpleNamespace(**data),
        )

    def risk(self, sample, settings, *, available_at=None):
        block = self.observed(sample).block
        available = (
            max(block.timestamp, settings.schedule.start) if available_at is None else available_at
        )
        return self.data.RiskRecord(
            key=self.data.key(self.source, "risk"),
            unit=self.data.requirement(self.source, settings, "risk").unit,
            source_time=UnixTime(block.timestamp),
            available_at=UnixTime(available),
            retrieved_at=float(available + 10**6),
            evidence=(DIGEST,),
            reference=block,
            payload=sample,
        )

    def accounting(self, sample, settings, available_at):
        block = self.observed(sample).block
        return self.data.RateRecord(
            key=self.data.key(self.source, "accounting"),
            unit=self.data.requirement(self.source, settings, "accounting").unit,
            source_time=UnixTime(block.timestamp),
            available_at=UnixTime(available_at),
            retrieved_at=float(available_at + 10**6),
            evidence=("b" * 64,),
            reference=block,
            payload=self.observed(sample),
        )

    def risk_timeline(self, samples, settings):
        records = tuple(self.risk(s, settings) for s in samples)
        return Timeline(records, self.data.requirement(self.source, settings, "risk"))

    def accounting_timeline(self, settings):
        """One record per fixed boundary, each carrying the literal observation."""
        times = range(settings.schedule.start, settings.schedule.deadline + 1, SAMPLE_SECONDS)
        records = tuple(self.accounting(self.sample, settings, t) for t in times)
        return Timeline(records, self.data.requirement(self.source, settings, "accounting"))

    def empty_book(self, settings, config=None):
        book = object.__new__(self.book.Book)
        book.config, book.settings = config, settings
        book.rates, book.risks = {}, {}
        return book

    def rates(self, timeline):
        return SimpleNamespace(records=timeline) if self.rates_wrapped else timeline


def aave_like(name, book, data, acquisition, sample):
    quote = sample.reserve.oracle
    allocation = SimpleNamespace(
        reserve=sample.reserve.source, oracle=quote.oracle, price_source=quote.feed
    )
    return Case(
        name=name,
        book=book,
        data=data,
        acquisition=acquisition,
        sample=sample,
        source=sample.reserve.source,
        subject=allocation,
        rates_wrapped=True,
        observed_field="reserve",
    )


def morpho_sample():
    observed = morpho_offline.observation()
    block = observed.block
    following = block.model_copy(
        update={"number": block.number + 1, "timestamp": block.timestamp + SPACING}
    )
    return morpho_data.RiskSample(market=observed, block_group=(block,), following_block=following)


def cases():
    compound = compound_offline.economic_market().sample
    return (
        aave_like("aave_v3", aave_book, aave_data, aave_acquisition, aave_markets()[0].sample),
        aave_like(
            "sparklend", spark_book, spark_data, spark_acquisition, spark_markets()[0].sample
        ),
        Case(
            name="compound_v3",
            book=compound_book,
            data=compound_data,
            acquisition=compound_acquisition,
            sample=compound,
            source=compound.market.source,
            subject=compound.market.source,
            rates_wrapped=True,
            observed_field="market",
        ),
        Case(
            name="morpho_blue",
            book=morpho_book,
            data=morpho_data,
            acquisition=morpho_acquisition,
            sample=morpho_sample(),
            source=morpho_offline.MARKET,
            subject=morpho_offline.MARKET,
            rates_wrapped=False,
            observed_field="market",
        ),
    )


CASES = {case.name: case for case in cases()}
