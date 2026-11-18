"""Explicit synthetic shocks over captured Arbitrum markets and actual strategy ownership."""

from dataclasses import replace

import pytest

from almanak.backtest.engine import Journal, Simulator
from almanak.connectors.aave_v3.economic_binding import AaveEconomic
from almanak.connectors.aave_v3.economic_book import Book
from almanak.connectors.aave_v3.economic_events import EconomicDriver
from almanak.connectors.aave_v3.economic_liquidation import liquidate
from almanak.connectors.aave_v3.economic_math import risk
from almanak.connectors.aave_v3.economic_state import State
from almanak.worker_session import preloaded_workers
from tests.test_aave_v3_prepared import prepared as prepared

pytestmark = pytest.mark.parametrize("prepared", ["arbitrum"], indirect=True)


@pytest.fixture
def borrowed(prepared, tmp_path):
    inputs = prepared
    journal = Journal(tmp_path / "adverse.sqlite", State)
    journal.create(AaveEconomic.initial(inputs.settings, inputs.config, inputs.interval))

    def stop(stage):
        if (
            stage == "effects_commit"
            and journal.load().completed[-1].pending.intent.kind == "borrow"
        ):
            raise RuntimeError("retained borrowed position")

    book = Book(inputs.dataset, inputs.config, inputs.settings)
    try:
        driver = EconomicDriver(inputs.config, inputs.settings, inputs.strategy, book)
        with preloaded_workers(), pytest.raises(RuntimeError, match="retained borrowed"):
            Simulator(journal, driver, fault=stop).run()
        state = journal.load()
        held, markets = driver.group(state, inputs.config.target("collateral"), state.time)
        positions = tuple(p.payload for p in held)
        assert risk(positions, markets).risk.health_factor_wad > 10**18
        # This price is an explicit adverse scenario, never substituted into the dataset.
        market = markets[0]
        oracle = market.reserve.oracle.model_copy(update={"price": 100 * 10**8})
        reserve = market.reserve.model_copy(update={"oracle": oracle})
        shocked = replace(market, sample=market.sample.model_copy(update={"reserve": reserve}))
        yield inputs.config, state, positions, (shocked, markets[1])
    finally:
        book.close()


def test_captured_borrowing_liquidates_with_scoped_deficit_under_explicit_price_shock(borrowed):
    config, state, positions, markets = borrowed
    balances = state.portfolio.balances
    assert risk(positions, markets).risk.health_factor_wad < 10**18
    result = liquidate(config, positions, markets, positions, 8)
    assert len(result.liquidations) == 1 and result.deficits and not result.blocked
    assert result.positions[0].supply_scaled == result.positions[1].debt_scaled == 0
    assert result.positions[1].deficit == sum(d.amount for d in result.deficits)
    assert all(d.asset == config.target("loan").asset for d in result.deficits)
    assert result.liquidations[0].collateral_removed.asset == config.target("collateral").asset
    assert state.portfolio.balances == balances
    assert result.positions[1].repaid == 0


@pytest.mark.parametrize("fault", ["paused", "grace"])
def test_unsettleable_shock_preserves_borrowed_obligations(borrowed, fault):
    config, _, positions, markets = borrowed
    market = markets[0]
    if fault == "grace":
        sample = market.sample.model_copy(update={"liquidation_grace_until": market.time})
    else:
        configuration = market.reserve.configuration.model_copy(
            update={"raw": market.reserve.configuration.raw | (1 << 60)}
        )
        reserve = market.reserve.model_copy(update={"configuration": configuration})
        sample = market.sample.model_copy(update={"reserve": reserve})
    result = liquidate(
        config, positions, (replace(market, sample=sample), markets[1]), positions, 8
    )
    assert result.positions == positions and result.blocked
    assert result.liquidations == result.deficits == ()
