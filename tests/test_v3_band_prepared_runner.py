"""The band reference recentres offline over a published synthetic prepared dataset.

Sample zero is a captured Uniswap V3 pool state; later samples follow an explicit
synthetic path (see `tests/v3_band_dataset.py`). This proves publication, book
validation, bracketed fee attribution, recovery and offline execution of the actual
`uniswap_v3_robinhood_lp_band` strategy code. It is not historical, archival or fork
evidence and makes no accuracy claim about any market.
"""

from pathlib import Path

import httpx
import pytest

from almanak.backtest.engine import Journal, Simulator
from almanak.configuration.yaml import load
from almanak.connectors.concentrated_policy import Coverage
from almanak.connectors.uniswap_v3.economic_binding import UniswapEconomic
from almanak.connectors.uniswap_v3.economic_book import PoolBook
from almanak.connectors.uniswap_v3.economic_model import EconomicModel
from almanak.connectors.uniswap_v3.economic_reporting import report
from almanak.connectors.uniswap_v3.economic_state import State
from almanak.connectors.v3_fee_growth import accrued
from almanak.strategy import StrategyDefinition
from almanak.triggers import IntervalSelection
from almanak.worker_session import preloaded_workers
from scripts.legacy_configuration.author import normalize
from scripts.legacy_configuration.backtest import read_backtest
from tests.legacy_references import harness_directory
from tests.test_uniswap_v3_economic_simulation import setup
from tests.v3_band_dataset import SyntheticPath, publish

ROOT = Path(__file__).parents[1]
REFERENCE = ROOT / "strategies/uniswap_v3_robinhood_lp_band"
SECONDS = 60
BAND = {"lower": 196520, "upper": 199520, "width": 1000}
GROWTH = (2**95, 2**123)


def author_and_settings(sample, band=BAND):
    frozen = harness_directory("uniswap_v3_ethereum_lp")
    wire = load(frozen / "config.yaml").value
    wire.pop("execution")
    allocation = wire["strategy"]["allocations"]["position"]
    allocation.update(range=None, band=band, hold_seconds="1h")
    author = normalize(wire)
    settings = read_backtest(frozen / "backtest-economic.yaml", author)
    start = sample.pool.block.timestamp
    schedule = settings.schedule.model_copy(
        update={"start": start, "end": start + 300, "cleanup_seconds": 120, "latency_seconds": 5}
    )
    data = settings.data.model_copy(
        update={
            "coverage": (Coverage(allocation="position", lower=sample.lower, upper=sample.upper),),
            "max_samples": 20,
        }
    )
    settings = settings.model_copy(
        update={
            "schedule": schedule,
            "data": data,
            "liquidity_model": "bracketed_historical_fee_growth",
        }
    )
    return author, settings


def scripted_path(sample, target):
    """Hold, approach the first range's upper tick, leave the range, settle above it.

    The captured pool initializes every tick near its price, where the bracket is
    exact. Clearing and re-creating the first range's upper tick while the price sits
    just below it makes that interval's growth near the boundary unattributable.
    """
    path = SyntheticPath(sample, target, SECONDS)
    tick = sample.pool.tick
    path.step(GROWTH, tick + 30)
    # An external position whose upper tick is the recentred position's lower boundary.
    path.liquidity(197820, 198220, 10**16)
    path.step(GROWTH, 198515)
    path.reinitialize(198520, 198600)
    path.step(GROWTH, tick + 700)
    path.step(GROWTH, tick + 720)
    path.liquidity(197820, 198220, -(10**16))
    path.step(GROWTH, tick + 690)
    path.step(GROWTH, tick + 710)
    path.step(GROWTH, tick + 705)
    return path


def prepared(directory, band=BAND, script=None, **options):
    _, record, _ = setup(0)
    author, settings = author_and_settings(record.payload, band)
    interval = IntervalSelection(configured=author.decision_trigger)
    config = UniswapEconomic.parameters(author, settings, interval)
    path = (script or scripted_path)(record.payload, config.target("position"))
    publish(directory, config, settings, path, **options)
    strategy = StrategyDefinition.read(REFERENCE / "strategy.py")
    return config, settings, interval, strategy, path


@pytest.fixture
def offline(monkeypatch):
    def forbid(*args, **kwargs):
        raise AssertionError("prepared band execution cannot use network inputs")

    monkeypatch.setattr(httpx.Client, "send", forbid)


def run(directory, config, settings, strategy, path, *, fault=None):
    book = PoolBook(directory, config, settings)
    try:
        model = EconomicModel(config, settings, strategy, book)
        with preloaded_workers():
            result = Simulator(Journal(path, State), model, fault=fault).run()
        return result, report(result, model)
    finally:
        book.close()


def fresh(tmp_path, name, config, settings, interval):
    journal = Journal(tmp_path / name, State)
    journal.create(UniswapEconomic.initial(settings, config, interval))
    return journal.path


def test_band_reference_recentres_and_brackets_the_fees_its_ranges_earned(tmp_path, offline):
    directory = tmp_path / "dataset"
    config, settings, interval, strategy, path = prepared(directory)
    journal = fresh(tmp_path, "state.sqlite", config, settings, interval)
    state, summary = run(directory, config, settings, strategy, journal)
    lifetimes = positions(state)
    band = config.target("position").band
    assert len(lifetimes) >= 2 and len({held["range"] for held in lifetimes}) >= 2
    for held in lifetimes:
        lower, upper = held["range"]
        assert band.lower <= lower < upper <= band.upper and upper - lower == band.width
    # The credited lower bracket never exceeds what the scripted path earned, and the
    # retained upper bracket never falls below it.
    earned = [0, 0]
    for held in lifetimes:
        marks = path.marks[held["opened"]][0], path.marks[held["closed"]][0]
        growth = path.ledger.earned(*held["range"], *marks)
        for token in (0, 1):
            earned[token] += accrued(held["liquidity"], 0, growth[token])
    payload = state.portfolio.positions[0].payload
    for token in (0, 1):
        credited = getattr(payload, f"fees{token}")
        bound = credited + getattr(payload, f"unattributed{token}")
        assert credited <= earned[token] <= bound
    assert payload.unattributed0 + payload.unattributed1 > 0
    assert summary["closed"] and payload.owned.position is None
    position = summary["positions"][0]
    assert position["fee_attribution"] == "bracketed_lower_bound"
    assumptions = summary["assumptions"]
    assert any("banded LP fees credit only growth" in a for a in assumptions)
    # A band run states the neutral growth basis, never the exact fixed-range wording.
    assert not any("fixed historical range growth" in a for a in assumptions)
    swaps = [
        item.transition.result.fill
        for item in state.completed
        if item.transition.result.success and item.pending.intent.kind == "swap_exact_input"
    ]
    assert swaps and all(fill.calculation == "prepared_integer_pool" for fill in swaps)


def positions(state):
    """Each opened band position with its liquidity, range and sample indices."""
    start = state.schedule.start
    held, current = [], None
    for item in state.completed:
        result = item.transition.result
        if not result.success:
            continue
        index = (int(result.effective_at.timestamp()) - start) // SECONDS
        if item.pending.intent.kind == "open_liquidity":
            selected = result.after.position.range
            current = {
                "liquidity": result.liquidity_delta,
                "range": (selected.lower, selected.upper),
                "opened": index,
            }
        elif result.after.position is None and current is not None:
            held.append({**current, "closed": index})
            current = None
    return held


@pytest.mark.parametrize("stage", ["pending_commit", "effects_commit", "delivery_commit"])
def test_band_run_resumes_from_its_journal_without_duplicate_effects(tmp_path, offline, stage):
    directory = tmp_path / "dataset"
    config, settings, interval, strategy, _ = prepared(directory)
    journal = fresh(tmp_path, "state.sqlite", config, settings, interval)

    def crash(at):
        if at == stage:
            raise RuntimeError("interrupted band run")

    with pytest.raises(RuntimeError, match="interrupted band run"):
        run(directory, config, settings, strategy, journal, fault=crash)
    resumed, _ = run(directory, config, settings, strategy, journal)
    control = fresh(tmp_path, "control.sqlite", config, settings, interval)
    uninterrupted, _ = run(directory, config, settings, strategy, control)
    assert resumed.portfolio == uninterrupted.portfolio
    assert resumed.memory == uninterrupted.memory
    assert len(resumed.completed) == len(uninterrupted.completed)


def test_book_refuses_band_history_that_omits_a_liquidity_event(tmp_path):
    directory = tmp_path / "dataset"
    config, settings, _, _, _ = prepared(directory, drop_event=0)
    with pytest.raises(ValueError, match="does not reconcile"):
        PoolBook(directory, config, settings)


def test_book_serves_bracketed_growth_only_for_retained_band_samples(tmp_path):
    directory = tmp_path / "dataset"
    config, settings, _, _, path = prepared(directory)
    target = config.target("position")
    book = PoolBook(directory, config, settings)
    try:
        record = book.at(target, settings.schedule.start + 3 * SECONDS)
        selected = target.band.centered(path.samples[0].pool.tick, target.pool.tick_spacing)
        bounds = book.band_bounds(target, record, selected)
        assert all(low <= high for low, high in zip(bounds.lower, bounds.upper, strict=True))
        foreign = record.model_copy(update={"available_at": record.available_at + 1})
        with pytest.raises(ValueError, match="retained prepared sample"):
            book.band_bounds(target, foreign, selected)
    finally:
        book.close()


def test_unbounded_band_reference_recentres_on_the_observed_tick(tmp_path, offline):
    directory = tmp_path / "dataset"
    config, settings, interval, strategy, _ = prepared(directory, {"width": BAND["width"]})
    journal = fresh(tmp_path, "state.sqlite", config, settings, interval)
    state, summary = run(directory, config, settings, strategy, journal)
    lifetimes = positions(state)
    target = config.target("position")
    assert not target.band.bounded
    assert len({held["range"] for held in lifetimes}) >= 2
    for held in lifetimes:
        lower, upper = held["range"]
        assert upper - lower == target.band.width and not lower % target.pool.tick_spacing
    assert summary["closed"] and not summary["failures"]


def beyond_coverage(sample, target):
    """Move the price past the sampled coverage so a recentred position leaves it."""
    path = SyntheticPath(sample, target, SECONDS)
    tick = sample.pool.tick
    path.step(GROWTH, tick + 30)
    for offset in (1800, 1810, 1805, 1800, 1801, 1802):
        path.step(GROWTH, tick + offset)
    return path


def test_unbounded_band_run_stops_when_a_position_leaves_its_coverage(tmp_path, offline):
    from almanak.connectors.band_fee_growth import CoverageExceeded

    directory = tmp_path / "dataset"
    config, settings, interval, strategy, _ = prepared(
        directory, {"width": BAND["width"]}, beyond_coverage
    )
    journal = fresh(tmp_path, "state.sqlite", config, settings, interval)
    with pytest.raises(CoverageExceeded, match="widen data.coverage"):
        run(directory, config, settings, strategy, journal)
