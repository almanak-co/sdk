"""The V4 band reference recentres offline over a published synthetic prepared dataset.

Sample zero is the committed Robinhood V4 LP fixture's first pool state; later samples
follow an explicit synthetic path (see `tests/v4_band_dataset.py`). This proves
publication, book validation, bracketed fee attribution, recovery and offline execution
of the actual `uniswap_v4_robinhood_lp_band` strategy code. It is not historical,
archival or fork evidence and makes no accuracy claim about any market.
"""

import json
from pathlib import Path
from types import SimpleNamespace

import httpx
import pytest

from almanak.backtest.engine import Journal, Simulator
from almanak.configuration.strategy_parameters import StrategyParameters
from almanak.connectors.band_fee_growth import CoverageExceeded
from almanak.connectors.uniswap_v4 import history
from almanak.connectors.uniswap_v4.backtest import (
    economic_parameters,
    parameters,
    transition_effects,
)
from almanak.connectors.uniswap_v4.economic_binding import UniswapEconomic
from almanak.connectors.uniswap_v4.economic_book import PoolBook
from almanak.connectors.uniswap_v4.economic_data import PoolSample
from almanak.connectors.uniswap_v4.economic_model import EconomicModel, initial_state
from almanak.connectors.uniswap_v4.economic_state import State
from almanak.connectors.uniswap_v4.math import accrued
from almanak.connectors.uniswap_v4.models import Parameters
from almanak.connectors.uniswap_v4.observation_models import SimulatedLiquidity
from almanak.dex_intents import ExitExchange
from almanak.strategy import StrategyDefinition
from almanak.triggers import IntervalSelection
from almanak.worker_session import preloaded_workers
from tests.uniswap_economic_support import prepared as captured_lp
from tests.v4_band_dataset import V4SyntheticPath, publish

ROOT = Path(__file__).parents[1]
REFERENCE = ROOT / "strategies/uniswap_v4_robinhood_lp_band"
SECONDS = 60
BAND = {"lower": -200130, "upper": -196130, "width": 980}
GROWTH = (2**120, 2**100)
# Token0 growth this small leaves the first range's close with token0 fees whose
# exact-size exit quote returns nothing: 23,270 wei at the captured pool price.
DUST_GROWTH = (2**100, 2**100)
BAND_MODEL = "bracketed_historical_fee_growth"


def captured(tmp_path):
    (tmp_path / "captured").mkdir(parents=True)
    manifest, _, model = captured_lp(tmp_path / "captured", "robinhood", "lp")
    try:
        config = manifest.config
        target = config.target(config.allocations[0].name)
        sample = model.book.timelines[target].rows[0].payload
    finally:
        model.book.close()
    return config, manifest.settings, sample


def band_inputs(tmp_path, band=BAND):
    fixed, settings, sample = captured(tmp_path)
    raw = fixed.model_dump(mode="json")
    raw["allocations"][0].update(range=None, band=band, hold_seconds=3600, collect_seconds=1800)
    config = Parameters.model_validate_json(json.dumps(raw))
    start = sample.pool.block.timestamp
    schedule = settings.schedule.model_copy(
        update={"start": start, "end": start + 300, "cleanup_seconds": 120, "latency_seconds": 5}
    )
    settings = settings.model_copy(update={"schedule": schedule, "liquidity_model": BAND_MODEL})
    return config, settings, sample


def scripted_path(sample, target, growth=GROWTH):
    """Hold, drift toward the range's upper edge, leave it and settle above it."""
    path = V4SyntheticPath(sample, target, SECONDS)
    path.step(growth, -198170)
    path.liquidity(-198100, -197900, 10**15)
    path.step(growth, -197800)
    path.step(growth, -197720)
    # Just above the first range's upper tick; the pool's liquidity thins further up.
    path.step(growth, -197700)
    path.liquidity(-198100, -197900, -(10**15))
    path.step(growth, -197690)
    path.step(growth, -197705)
    path.step(growth, -197695)
    return path


def dataset(tmp_path, growth=GROWTH, band=BAND, **options):
    config, settings, sample = band_inputs(tmp_path, band)
    target = config.target(config.allocations[0].name)
    path = scripted_path(sample, target, growth)
    directory = tmp_path / "dataset"
    publish(directory, config, settings, path, **options)
    return directory, config, settings, path


@pytest.fixture
def offline(monkeypatch):
    def forbid(*args, **kwargs):
        raise AssertionError("prepared V4 band execution cannot use network inputs")

    monkeypatch.setattr(httpx.Client, "send", forbid)


def run(directory, config, settings, journal, *, fault=None):
    book = PoolBook(directory, config, settings)
    try:
        strategy = StrategyDefinition.read(REFERENCE / "strategy.py")
        model = EconomicModel(config, settings, strategy, book)
        with preloaded_workers():
            state = Simulator(Journal(journal, State), model, fault=fault).run()
        return state, UniswapEconomic.report(state, model)
    finally:
        book.close()


def fresh(tmp_path, name, config, settings):
    journal = Journal(tmp_path / name, State)
    interval = IntervalSelection(configured=config.decision_trigger)
    journal.create(initial_state(settings, config, interval))
    return journal.path


def lifetimes(state):
    start, held, current = state.schedule.start, [], None
    for item in state.completed:
        result = item.transition.result
        index = (int(result.effective_at.timestamp()) - start) // SECONDS
        if item.pending.intent.kind == "open_liquidity":
            position = result.after.position
            current = {
                "liquidity": position.liquidity,
                "range": (position.range.lower, position.range.upper),
                "opened": index,
            }
        elif result.after.position is None and current is not None:
            held.append({**current, "closed": index})
            current = None
    return held


def test_v4_band_reference_recentres_and_brackets_the_fees_its_ranges_earned(tmp_path, offline):
    directory, config, settings, path = dataset(tmp_path)
    state, report = run(
        directory, config, settings, fresh(tmp_path, "state.sqlite", config, settings)
    )
    held = lifetimes(state)
    band = config.target(config.allocations[0].name).band
    assert len({h["range"] for h in held}) >= 2
    for position in held:
        lower, upper = position["range"]
        assert band.lower <= lower < upper <= band.upper and upper - lower == band.width
    earned = [0, 0]
    for position in held:
        marks = path.marks[position["opened"]][0], path.marks[position["closed"]][0]
        growth = path.ledger.earned(*position["range"], *marks)
        for token in (0, 1):
            earned[token] += accrued(position["liquidity"], 0, growth[token])
    payload = state.portfolio.positions[0].payload
    for token in (0, 1):
        credited = getattr(payload, f"fees{token}")
        assert credited <= earned[token] <= credited + getattr(payload, f"unattributed{token}")
    assert payload.unattributed0 + payload.unattributed1 > 0
    assert report["closed"] and payload.owned.position is None
    entry = report["positions"][0]
    assert entry["fee_attribution"] == "bracketed_lower_bound"
    assert any("banded LP fees credit only growth" in a for a in report["assumptions"])


def test_v4_unbounded_band_recentres_on_the_observed_tick_over_the_same_path(tmp_path, offline):
    directory, config, settings, _ = dataset(tmp_path, band={"width": BAND["width"]})
    state, report = run(
        directory, config, settings, fresh(tmp_path, "state.sqlite", config, settings)
    )
    held = lifetimes(state)
    target = config.target(config.allocations[0].name)
    band, spacing = target.band, target.pool.tick_spacing
    assert not band.bounded and len({h["range"] for h in held}) >= 2
    for position in held:
        lower, upper = position["range"]
        assert upper - lower == band.width and not lower % spacing
    assert report["closed"] and state.portfolio.positions[0].payload.owned.position is None


def test_v4_unbounded_band_refuses_a_position_outside_its_coverage(tmp_path):
    directory, config, settings, _ = dataset(tmp_path, band={"width": BAND["width"]})
    target = config.target(config.allocations[0].name)
    book = PoolBook(directory, config, settings)
    try:
        record = book.timelines[target].rows[0]
        lower, upper = record.payload.lower, record.payload.upper
        inside = target.band.centered((lower + upper) // 2, target.pool.tick_spacing)
        book.band_bounds(target, record, inside)
        above = SimpleNamespace(lower=inside.lower, upper=upper + target.pool.tick_spacing)
        with pytest.raises(CoverageExceeded, match="widen data.coverage"):
            book.band_bounds(target, record, above)
    finally:
        book.close()


@pytest.mark.parametrize("stage", ["pending_commit", "effects_commit", "delivery_commit"])
def test_v4_band_run_resumes_without_duplicate_effects(tmp_path, offline, stage):
    directory, config, settings, _ = dataset(tmp_path)
    journal = fresh(tmp_path, "state.sqlite", config, settings)

    def crash(at):
        if at == stage:
            raise RuntimeError("interrupted V4 band run")

    with pytest.raises(RuntimeError, match="interrupted V4 band run"):
        run(directory, config, settings, journal, fault=crash)
    resumed, _ = run(directory, config, settings, journal)
    control, _ = run(
        directory, config, settings, fresh(tmp_path, "control.sqlite", config, settings)
    )
    assert resumed.portfolio == control.portfolio
    assert resumed.memory == control.memory
    assert len(resumed.completed) == len(control.completed)


def test_v4_book_refuses_band_history_that_omits_a_liquidity_event(tmp_path):
    directory, config, settings, _ = dataset(tmp_path, drop_event=0)
    with pytest.raises(ValueError, match="does not reconcile"):
        PoolBook(directory, config, settings)


def test_v4_band_parameters_require_the_economic_bracketed_model(tmp_path):
    config, settings, _ = band_inputs(tmp_path)
    interval = SimpleNamespace(effective=config.decision_trigger)
    author = SimpleNamespace(
        allocations=config.allocations,
        asset_metadata=config.asset_metadata,
        parameters=StrategyParameters(),
    )
    assert economic_parameters(author, settings, interval).allocations == config.allocations
    fixed = settings.model_copy(update={"liquidity_model": "fixed_historical_fee_growth"})
    with pytest.raises(ValueError, match=BAND_MODEL):
        economic_parameters(author, fixed, interval)
    with pytest.raises(ValueError, match="historical_path"):
        parameters(author, settings, interval)
    unbanded, _, _ = captured(tmp_path / "fixed")
    fixed_author = SimpleNamespace(
        allocations=unbanded.allocations,
        asset_metadata={},
        parameters=StrategyParameters(),
    )
    with pytest.raises(ValueError, match="requires a banded liquidity allocation"):
        economic_parameters(fixed_author, settings, interval)


def test_v4_band_samples_and_evidence_retain_their_shape(tmp_path):
    config, _, sample = band_inputs(tmp_path)
    target = config.target(config.allocations[0].name)
    with pytest.raises(ValueError, match="every tick's outside growth"):
        PoolSample.model_validate({**sample.model_dump(), "global_growth": (0, 0)})
    with pytest.raises(ValueError, match="no tick growth"):
        sample.growth_ticks()
    block = sample.pool.block
    base = dict(target=target, first=block, last=block, events=(), contracts=(), inputs=())
    history.BoundaryEvidence(**base, initial_gross=None, final_gross=None)
    with pytest.raises(ValueError, match="fixed boundary measurements"):
        history.BoundaryEvidence(**base, initial_gross=(1, 1), final_gross=(1, 1))
    with pytest.raises(ValueError, match="historical_path"):
        history.refuse_band(target)
    with pytest.raises(ValueError, match="concrete range"):
        SimulatedLiquidity(simulation_id="x", liquidity=1, fee_growth=(0, 0), bound_growth=(0, 0))


def with_owned(model, state, owned):
    """The run's final state holding `owned` instead, with matching fund wallets."""
    held = state.portfolio.positions[0]
    before = held.payload.owned
    deltas = tuple(
        new.amount - old.amount
        for new, old in zip(
            (owned.inventory0, owned.inventory1),
            (before.inventory0, before.inventory1),
            strict=True,
        )
    )
    payload = held.payload.model_copy(update={"owned": owned})
    native = model.config.allocations[0].native_asset
    effects = transition_effects(model.settings, held, payload, deltas, native, 0)
    return state.model_copy(update={"portfolio": state.portfolio.apply(effects)})


def test_v4_band_close_leaves_counter_dust_as_a_measured_residual(tmp_path, offline):
    """An out-of-range close leaves token0 fee dust that no exit swap can return.

    The run no longer aborts on its decision observation: the dust is retained as a
    zero-output exit residual, and an exit holding only that dust completes without a
    step while the dust stays owned and reported.
    """
    directory, config, settings, _ = dataset(tmp_path, growth=DUST_GROWTH)
    state, report = run(
        directory, config, settings, fresh(tmp_path, "state.sqlite", config, settings)
    )
    assert state.mode == "finished" and report["closed"]
    closes = [
        c.transition.result for c in state.completed if c.pending.intent.kind == "close_liquidity"
    ]
    target = config.target(config.allocations[0].name)
    counter = next(
        a
        for a in (closes[0].after.inventory0, closes[0].after.inventory1)
        if a.asset == target.counter_currency
    )
    assert counter.amount == 23_270
    book = PoolBook(directory, config, settings)
    try:
        strategy = StrategyDefinition.read(REFERENCE / "strategy.py")
        model = EconomicModel(config, settings, strategy, book)
        dusty = with_owned(model, state, closes[0].after)
        observed = model.observe(dusty)[0]
        assert observed.exit_quote is None
        assert observed.exit_residual.amount == counter
        assert observed.exit_residual.output.amount == 0
        assert model.closed(dusty, (observed,))
        pending = SimpleNamespace(
            intent=ExitExchange[type(target)](target=target),
            identity="dust-exit",
            decision_time=dusty.time,
            eligible_at=dusty.time,
        )
        transition = model.execute(dusty, pending)
        assert transition.complete and transition.result.steps == ()
        assert transition.result.gas_paid.amount == 0
        assert transition.result.before.exit_residual == observed.exit_residual
        assert dusty.portfolio.apply(transition.effects) == dusty.portfolio
        entry = UniswapEconomic.report(dusty, model)["positions"][0]
        assert [r["amount"] for r in entry["exit_residuals"]] == [counter.model_dump(mode="json")]
        assert "exit_residuals" not in report["positions"][0]
    finally:
        book.close()
