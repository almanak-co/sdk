"""Configured borrowing floors use the actual reference decisions on each chain."""

from pathlib import Path

import pytest

from almanak.backtest.economic_events import EconomicDriver
from almanak.backtest.economic_model import initial_state, parameters
from almanak.backtest.economic_state import EconomicState
from almanak.backtest.engine import Journal, Simulator
from almanak.connectors.composition import connector_for
from almanak.strategy import StrategyDefinition
from almanak.triggers import IntervalSelection
from almanak.worker_session import preloaded_workers
from scripts.legacy_configuration.author import read_author
from scripts.legacy_configuration.backtest import read_backtest
from tests.reference_parameters import authored_parameters
from tests.test_economic_math import reserve as reserve
from tests.test_economic_model import FixedBook

ROOT = Path(__file__).parents[1]


def reference_inputs(name):
    """Converted references are read from their frozen effective fixtures."""
    from tests.legacy_references import harness_directory
    from tests.test_author_resolution import FROZEN, frozen_fork

    directory = harness_directory(name)
    if name == "aave_v4_weth_supply":
        author = frozen_fork()
        settings_type = connector_for(author).backtest("economic").settings_type
        return author, settings_type.model_validate_json(
            (FROZEN / "backtest-economic.json").read_text()
        )
    author = read_author(directory / "config.yaml")
    # The frozen legacy input predates strategy.parameters; the knobs are the reference's.
    author = author.model_copy(update={"parameters": authored_parameters(name)})
    return author, read_backtest(directory / "backtest-economic.yaml", author)


def configured_model(tmp_path, reserve, name, price, other_hub):
    directory = ROOT / "strategies" / name
    author, settings = reference_inputs(name)
    if other_hub:
        # This synthetic binding exercises fund/Spoke risk across Hubs; it is
        # not a claim that this extra Hub is deployed on the reference chain.
        author = author.model_copy(
            update={
                "debts": tuple(
                    debt.model_copy(
                        update={
                            "reserve": debt.reserve.model_copy(update={"hub": "0x" + "ab" * 20})
                        }
                    )
                    for debt in author.debts
                )
            }
        )
    settings = settings.model_copy(
        update={
            "schedule": settings.schedule.model_copy(
                update={
                    "end": settings.schedule.start + 60,
                    "decision_interval_override_seconds": None,
                    "latency_seconds": 1,
                }
            )
        }
    )
    interval = IntervalSelection(configured=author.decision_trigger)
    config = parameters(settings, author, interval)
    source = config.allocations[0].reserve
    reserve = reserve.model_copy(
        update={"block": reserve.block.model_copy(update={"chain": source.chain})}
    )
    book = FixedBook(config, reserve)
    for target, view in tuple(book.markets.items()):
        if target.reserve_id == 0:
            book.markets[target] = view.model_copy(
                update={"price": view.price.model_copy(update={"price": price})}
            )
    driver = EconomicDriver(
        config, settings, StrategyDefinition.read(directory / "strategy.py"), book
    )
    journal = Journal(tmp_path / "state.sqlite", EconomicState)
    journal.create(initial_state(settings, config, interval))
    return journal, driver


def admit_borrow(journal, simulator):
    for _ in range(100):
        state = journal.load()
        if state.pending is not None and state.pending.intent.kind == "borrow":
            return state
        simulator.tick()
    raise AssertionError("actual reference did not request borrowing")


@pytest.mark.parametrize(
    "name,threshold_price",
    [("aave_v4_weth_supply", 1250 * 10**8), ("aave_v4_avalanche_borrowing", 125 * 10**6)],
)
@pytest.mark.parametrize("offset", [-1, 0, 1])
@pytest.mark.parametrize("other_hub", [False, True])
def test_reference_respects_post_borrow_floor(
    tmp_path, reserve, name, threshold_price, offset, other_hub
):
    # 0.01 WETH / 10 WAVAX at 80% collateral factor, with 5 USDC of debt,
    # requires 1250 USD / 1.25 USD respectively to reach configured health 2.0.
    journal, driver = configured_model(tmp_path, reserve, name, threshold_price + offset, other_hub)
    with preloaded_workers():
        simulator = Simulator(journal, driver)
        before = admit_borrow(journal, simulator)
        if offset < 0:
            with pytest.raises(ValueError, match="below its configured threshold"):
                simulator.run()
            after = journal.load()
            assert after.portfolio == before.portfolio
            assert after.completed == before.completed and after.pending == before.pending
            assert after.memory == before.memory
        else:
            transition = driver.execute(before, before.pending)
            assert transition.result.success
            assert transition.result.assets.amount == 5 * 10**6
            assert Simulator(journal, driver).run().mode == "finished"
