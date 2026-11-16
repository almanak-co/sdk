"""Independent fund memory and cash on explicit frozen two-chain component inputs."""

from pathlib import Path
from types import SimpleNamespace

import httpx
import pytest

from almanak.backtest.engine import Journal, Simulator
from almanak.connectors.composed.economic_binding import ComposedEconomic
from almanak.connectors.composed.economic_model import EconomicDriver
from almanak.connectors.composed.economic_state import State
from almanak.connectors.identifiers import ConnectorId
from almanak.connectors.interfaces import Connector
from almanak.runtime import validate_strategy_config
from almanak.strategy import StrategyDefinition
from almanak.worker_session import preloaded_workers
from tests.composed_public import economic_inputs
from tests.test_composed_economic_runner import FrozenLendingBook, synthetic_support
from tests.test_uniswap_v3_economic_simulation import setup

REFERENCE = Path(__file__).parents[1] / "strategies/ethereum_base_borrow_and_trade"


def inputs():
    parameters, config, settings, interval = economic_inputs(REFERENCE.name)
    strategy = StrategyDefinition.read(REFERENCE / "strategy.py")
    return parameters, settings, interval, config, strategy


def frozen_books():
    lending = FrozenLendingBook()
    lending.markets.extend(FrozenLendingBook("ethereum").markets)
    rows = [setup(index)[1] for index in (0, 1)]
    records = {r.payload.pool.source: r for r in rows}
    return SimpleNamespace(
        start=max(r.available_at for r in rows),
        components={
            ConnectorId("aave_v3"): lending,
            ConnectorId("uniswap_v3"): SimpleNamespace(
                at=lambda target, time: records[target.pool], baselines={}
            ),
        },
    )


@pytest.fixture
def runner(monkeypatch):
    def forbid(*args, **kwargs):
        raise AssertionError("synthetic multichain proof cannot access providers")

    monkeypatch.setattr(httpx.Client, "send", forbid)
    monkeypatch.setattr(Connector, "support_for", synthetic_support)

    def build(path):
        author, settings, interval, config, strategy = inputs()
        validate_strategy_config(strategy, author)
        books = frozen_books()
        settings = settings.model_copy(
            update={
                "schedule": settings.schedule.model_copy(
                    update={
                        "start": books.start,
                        "end": books.start + 30,
                    }
                )
            }
        )
        driver = EconomicDriver(config, settings, strategy, books)
        journal = Journal(path, State)
        journal.create(ComposedEconomic.initial(settings, config, interval))
        return journal, driver

    return build


def test_two_chain_reference_completes_independent_borrow_and_trade_roundtrips(runner, tmp_path):
    journal, driver = runner(tmp_path / "state.sqlite")
    with preloaded_workers():
        result = Simulator(journal, driver).run()
    assert driver.closed(result, driver.observe(result))
    assert len(result.portfolio.balances) == 6
    for account in ("ethereum", "base"):
        actions = [
            c.pending.intent.kind
            for c in result.completed
            if c.pending.intent.target.account == account
        ]
        assert actions == [
            "supply",
            "borrow",
            "swap_exact_input",
            "exit_exchange",
            "repay",
            "withdraw",
        ]
    assert all(c.transition.result.success for c in result.completed)
    assert result.memory.values["phases"] == {"ethereum": 3, "base": 3}
    assert result.memory.values["traded"] == {"ethereum": False, "base": False}
