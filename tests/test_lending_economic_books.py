"""Prepared lending books refuse records that differ from their pinned history.

Aave V3, SparkLend, Compound V3 and Morpho Blue share the same risk-history contract:
each sample is the last block of its timestamp group, is visible from that block (or the
run start), precedes its following boundary, and chains to the previous sample's
following block. The same refusals apply to every connector, so each case runs for all
four on literal observations.
"""

from types import SimpleNamespace

import pytest

from almanak.gateway.ethereum import DataError
from almanak.models import AssetMetadata
from tests import test_compound_v3_offline as compound_offline
from tests.lending_economic_cases import CASES, DIGEST, SAMPLE_SECONDS

ALL = sorted(CASES)
AAVE_LIKE = ("aave_v3", "sparklend")


@pytest.fixture(params=ALL)
def case(request):
    return CASES[request.param]


def test_a_chained_risk_history_is_admitted(case):
    settings = case.settings()
    first, second = case.history(2)
    book = case.empty_book(settings)
    previous = case.risk(first, settings)
    book.validate_risk(case.subject, previous, None)
    book.validate_risk(case.subject, case.risk(second, settings), previous)


def test_risk_from_another_product_or_block_is_refused(case):
    settings = case.settings()
    book = case.empty_book(settings)
    record = case.risk(case.sample, settings)
    accounting = record.model_copy(update={"key": case.data.key(case.source, "accounting")})
    with pytest.raises(ValueError, match="differs from its configured"):
        book.validate_risk(case.subject, accounting, None)
    later = record.model_copy(update={"reference": case.sample.following_block})
    with pytest.raises(ValueError, match="differs from its configured"):
        book.validate_risk(case.subject, later, None)


def test_risk_visible_before_its_block_or_the_run_start_is_refused(case):
    settings = case.settings()
    book = case.empty_book(settings)
    record = case.risk(case.sample, settings)
    early = record.model_copy(update={"source_time": record.source_time - 1})
    with pytest.raises(ValueError, match="unsupported historical visibility"):
        book.validate_risk(case.subject, early, None)
    # The run starts after the block, so the record must become visible at the start.
    started = case.empty_book(case.settings(start=case.block.timestamp + 1))
    with pytest.raises(ValueError, match="unsupported historical visibility"):
        started.validate_risk(case.subject, record, None)


def test_risk_initialized_at_or_after_its_following_block_is_refused(case):
    # A run starting at the following block's time makes this sample stale at once.
    following = case.sample.following_block.timestamp
    settings = case.settings(start=following)
    book = case.empty_book(settings)
    record = case.risk(case.sample, settings)
    assert record.available_at == following
    with pytest.raises(ValueError, match="not the latest eligible block"):
        book.validate_risk(case.subject, record, None)


def test_risk_history_that_skips_a_canonical_block_is_refused(case):
    settings = case.settings()
    first, second, third = case.history(3)
    book = case.empty_book(settings)
    with pytest.raises(ValueError, match="missing canonical block"):
        book.validate_risk(case.subject, case.risk(third, settings), case.risk(first, settings))
    assert second.block_group[0] == first.following_block


@pytest.mark.parametrize("name", AAVE_LIKE)
def test_aave_family_risk_requires_its_configured_oracle_quote(name):
    case = CASES[name]
    settings = case.settings()
    book = case.empty_book(settings)
    record = case.risk(case.sample, settings)
    other_feed = SimpleNamespace(
        reserve=case.subject.reserve, oracle=case.subject.oracle, price_source="0x" + "ee" * 20
    )
    with pytest.raises(ValueError, match="lacks its configured oracle evidence"):
        book.validate_risk(other_feed, record, None)
    unpriced = record.model_copy(
        update={
            "payload": record.payload.model_copy(
                update={"reserve": record.payload.reserve.model_copy(update={"oracle": None})}
            )
        }
    )
    with pytest.raises(ValueError, match="lacks its configured oracle evidence"):
        book.validate_risk(case.subject, unpriced, None)


def counted_book(case, **limits):
    settings = case.settings(**limits)
    book = case.empty_book(settings)
    # Two accounting boundaries and three one-block risk samples.
    book.rates[case.source] = case.rates(case.accounting_timeline(settings))
    book.risks[case.source] = case.risk_timeline(case.history(3), settings)
    return book


def test_prepared_counts_within_their_limits_are_admitted(case):
    counted_book(
        case, max_accounting_samples=2, max_risk_samples=3, max_risk_blocks=3
    ).validate_counts()


@pytest.mark.parametrize(
    ("limits", "message"),
    [
        ({"max_accounting_samples": 1}, "accounting exceeds its explicit sample limit"),
        ({"max_risk_samples": 2}, "risk exceeds its explicit sample limit"),
        ({"max_risk_blocks": 2}, "canonical-block limit"),
    ],
)
def test_prepared_counts_above_an_explicit_limit_are_refused(case, limits, message):
    with pytest.raises(ValueError, match=message):
        counted_book(case, **limits).validate_counts()


def test_next_boundary_is_the_earliest_sample_or_expiry_before_the_horizon(case):
    book = counted_book(case)
    start = book.settings.schedule.start
    # Risk samples arrive every block (12 seconds); accounting every minute.
    assert book.next_boundary(start, start + 600) == start + 12
    assert book.next_boundary(start + 12, start + 600) == start + 24
    # After the last risk sample, accounting's next boundary is the earliest change.
    assert book.next_boundary(start + 24, start + 600) == start + SAMPLE_SECONDS
    # Nothing changes before a horizon that precedes every boundary.
    assert book.next_boundary(start, start + 11) is None


@pytest.mark.parametrize("name", AAVE_LIKE)
def test_aave_family_next_boundary_includes_the_end_of_a_liquidation_grace(name):
    case = CASES[name]
    settings = case.settings()
    book = case.empty_book(settings)
    start = settings.schedule.start
    graced = case.sample.model_copy(update={"liquidation_grace_until": start + 5})
    book.rates[case.source] = case.rates(case.accounting_timeline(settings))
    book.risks[case.source] = case.risk_timeline((graced,), settings)
    boundary = book.next_boundary(start, start + 600)
    if name == "aave_v3":
        # Aave V3 resumes liquidations the second after the grace period ends.
        assert boundary == start + 6
    else:
        # SparkLend models no grace period; the next change is the next accounting sample.
        assert boundary == start + SAMPLE_SECONDS
    # Grace already over, or before any risk sample, adds no boundary.
    assert book.next_boundary(start + 6, start + 59) is None
    assert book.next_boundary(start - 1, start - 1) is None


def test_book_evidence_names_every_accounting_boundary_up_to_the_time(case):
    book = counted_book(case)
    start = book.settings.schedule.start
    assert book.evidence(case.source, start) == tuple(sorted({DIGEST, "b" * 64}))


class Reader:
    """Serves prepared timelines by product and records each evidence body read."""

    def __init__(self, timelines):
        self.timelines = timelines
        self.read = []

    def timeline(self, requirement, record_type):
        timeline = self.timelines[requirement.key.product]
        assert all(isinstance(row, record_type) for row in timeline.rows)
        return timeline

    def evidence(self, digest):
        self.read.append(digest)


LOADED = ("compound_v3", "morpho_blue")


def loading_book(case, samples, config):
    settings = case.settings()
    book = case.empty_book(settings, config)
    book.reader = Reader(
        {
            "accounting": case.accounting_timeline(settings),
            "risk": case.risk_timeline(samples, settings),
        }
    )
    return book


def loaded_config(name):
    if name == "compound_v3":
        return compound_offline.parameters()
    return SimpleNamespace(asset_metadata={})


@pytest.mark.parametrize("name", LOADED)
def test_load_retains_verified_history_reaching_past_the_deadline(name):
    case = CASES[name]
    # Six one-block samples: the last one's following block lies after the deadline.
    book = loading_book(case, case.history(6), loaded_config(name))
    book.load(case.source)
    assert len(book.risks[case.source].rows) == 6
    assert set(book.reader.read) == {DIGEST, "b" * 64}
    assert len(book.reader.read) == 6 + 2


@pytest.mark.parametrize("name", LOADED)
def test_load_refuses_risk_history_ending_before_the_deadline(name):
    case = CASES[name]
    book = loading_book(case, case.history(3), loaded_config(name))
    with pytest.raises(ValueError, match="ends before its configured coverage boundary"):
        book.load(case.source)
    assert case.source not in book.risks


def test_comet_targets_require_the_configured_collateral_feed():
    case = CASES["compound_v3"]
    book = case.empty_book(case.settings(), compound_offline.parameters())
    observed = case.sample.market
    book.validate_targets(observed)
    collateral = observed.collaterals[0].model_copy(update={"price_feed": "0x" + "ee" * 20})
    with pytest.raises(ValueError, match="differs from its configured asset/feed"):
        book.validate_targets(observed.model_copy(update={"collaterals": (collateral,)}))


def test_morpho_targets_require_declared_asset_precision():
    case = CASES["morpho_blue"]
    observed = case.sample.market
    declared = {observed.loan_asset.identifier: AssetMetadata(decimals=18)}
    book = case.empty_book(case.settings(), SimpleNamespace(asset_metadata=declared))
    with pytest.raises(DataError, match="configured decimals 18 differ from observed 6"):
        book.validate_targets(observed)


def test_morpho_accounting_requires_every_boundary_of_one_pinned_market():
    case = CASES["morpho_blue"]
    settings = case.settings()
    book = case.empty_book(settings)
    timeline = case.accounting_timeline(settings)
    book.validate_rates(case.source, timeline)
    short = SimpleNamespace(times=timeline.times[:1], rows=timeline.rows[:1])
    with pytest.raises(ValueError, match="missing a fixed sample boundary"):
        book.validate_rates(case.source, short)
    first = timeline.rows[0]
    risk_keyed = first.model_copy(update={"key": case.data.key(case.source, "risk")})
    with pytest.raises(ValueError, match="mixes configured markets"):
        book.validate_rates(case.source, SimpleNamespace(times=timeline.times, rows=(risk_keyed,)))
    moved = first.model_copy(update={"reference": case.sample.following_block})
    with pytest.raises(ValueError, match="differs from its pinned historical evidence"):
        book.validate_rates(case.source, SimpleNamespace(times=timeline.times, rows=(moved,)))
