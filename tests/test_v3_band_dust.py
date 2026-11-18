"""A V3 band close below its range leaves counter fee dust that no exit swap can return.

The synthetic path (see `tests/v3_band_dataset.py`) starts from the captured pool
state and leaves the first centred range below its lower tick with token1 growth too
small to swap back. The run finishes instead of failing its decision observation, and
an exit holding only that dust completes without a step. Not historical evidence.
"""

from types import SimpleNamespace

from almanak.connectors.uniswap_v3.economic_binding import UniswapEconomic
from almanak.connectors.uniswap_v3.economic_book import PoolBook
from almanak.connectors.uniswap_v3.economic_model import EconomicModel
from almanak.connectors.uniswap_v3.economic_reporting import report
from almanak.dex_intents import ExitExchange
from almanak.strategy import StrategyDefinition
from almanak.triggers import IntervalSelection
from tests.exchange_dust import holding
from tests.test_uniswap_v3_economic_simulation import setup
from tests.test_v3_band_prepared_runner import (
    REFERENCE,
    SECONDS,
    author_and_settings,
    fresh,
    offline,
    run,
)
from tests.v3_band_dataset import SyntheticPath, publish

# Token1 growth this small leaves only WETH fee dust after a close below the range.
GROWTH = (2**95, 2**95)
DUST = 14_273

__all__ = ["offline"]


def below_range_path(sample, target):
    path = SyntheticPath(sample, target, SECONDS)
    tick = sample.pool.tick
    for offset in (30, -300, -520, -700, -690, -710, -705):
        path.step(GROWTH, tick + offset)
    return path


def test_v3_band_close_below_range_leaves_counter_dust_as_a_measured_residual(tmp_path, offline):
    _, record, _ = setup(0)
    author, settings = author_and_settings(record.payload)
    interval = IntervalSelection(configured=author.decision_trigger)
    config = UniswapEconomic.parameters(author, settings, interval)
    target = config.target("position")
    directory = tmp_path / "dataset"
    publish(directory, config, settings, below_range_path(record.payload, target))
    strategy = StrategyDefinition.read(REFERENCE / "strategy.py")
    journal = fresh(tmp_path, "state.sqlite", config, settings, interval)
    state, summary = run(directory, config, settings, strategy, journal)
    assert state.mode == "finished" and summary["closed"]
    close = next(
        c.transition.result for c in state.completed if c.pending.intent.kind == "close_liquidity"
    )
    counter = next(
        v
        for v in (close.after.inventory0, close.after.inventory1)
        if v.asset == target.counter_currency
    )
    assert counter.amount == DUST
    book = PoolBook(directory, config, settings)
    try:
        model = EconomicModel(config, settings, strategy, book)
        dusty = holding(model, state, {counter.asset: DUST})
        observed = model.observe(dusty)[0]
        assert observed.exit_quote is None and observed.exit_residual.amount_in == counter
        assert observed.exit_residual.amount_out.amount == 0
        assert model.closed(dusty, (observed,))
        pending = SimpleNamespace(
            intent=ExitExchange[type(target)](target=target),
            identity="dust-exit",
            decision_time=dusty.time,
            eligible_at=dusty.time,
        )
        transition = model.execute(dusty, pending)
        assert transition.complete and transition.result.steps == ()
        assert dusty.portfolio.apply(transition.effects) == dusty.portfolio
        entry = report(dusty, model)["positions"][0]
        assert [r["amount_in"] for r in entry["exit_residuals"]] == [
            counter.model_dump(mode="json")
        ]
        assert "exit_residuals" not in summary["positions"][0]
    finally:
        book.close()
