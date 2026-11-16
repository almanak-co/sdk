"""Explicit synthetic risk stress over captured markets exercises reference cleanup."""

from dataclasses import replace

import pytest

from almanak.backtest.engine import Journal, Simulator
from almanak.connectors.compound_v3.economic_binding import CompoundEconomic
from almanak.connectors.compound_v3.economic_book import Book
from almanak.connectors.compound_v3.economic_events import EconomicDriver
from almanak.connectors.compound_v3.economic_model import blocked
from almanak.connectors.compound_v3.economic_state import State
from almanak.worker_session import preloaded_workers
from tests.test_compound_v3_liquidation import calibration
from tests.test_compound_v3_prepared import prepared as prepared


class StressBook:
    def __init__(self, inputs, scenario):
        self.book = Book(inputs.dataset, inputs.config, inputs.settings)
        self.prices = self.book.prices
        self.start = inputs.settings.schedule.start
        self.scenario = scenario
        self.asset = inputs.config.target("collateral").asset
        network = "base" if str(self.asset.chain) == "eip155:8453" else "mainnet"
        raw, _, _ = calibration(network, "credit" if scenario == "credit" else "bad_debt")
        self.price = raw["comparison"]["fixture_price"]

    def at(self, source, time):
        market = self.book.at(source, time)
        if time < self.start + 20:
            return market
        observed = market.observed
        if self.scenario == "missing":
            observed = observed.model_copy(
                update={"base_price": None, "base_price_error": "synthetic oracle gap"}
            )
        else:
            observed = observed.model_copy(
                update={
                    "collaterals": tuple(
                        c.model_copy(
                            update={"price": c.price.model_copy(update={"price": self.price})}
                        )
                        if c.asset_type == self.asset
                        else c
                        for c in observed.collaterals
                    )
                }
            )
            if self.scenario == "paused":
                observed = observed.model_copy(
                    update={"accounting": observed.accounting.model_copy(update={"pause_flags": 8})}
                )
        return replace(market, sample=market.sample.model_copy(update={"market": observed}))

    def evidence(self, source, time):
        return (*self.book.evidence(source, time), "explicit-synthetic-stress:" + self.scenario)

    def next_boundary(self, time, until):
        candidates = [
            b
            for b in (self.book.next_boundary(time, until), self.start + 20)
            if b is not None and time < b <= until
        ]
        return min(candidates) if candidates else None

    def close(self):
        self.book.close()


@pytest.mark.parametrize("scenario", ["credit", "bad_debt"])
def test_absorption_commit_recovers_once_and_reference_closes_all_remaining_ownership(
    prepared, tmp_path, scenario
):
    inputs = prepared
    book = StressBook(inputs, scenario)
    journal = Journal(tmp_path / "stress.sqlite", State)
    journal.create(CompoundEconomic.initial(inputs.settings, inputs.config, inputs.interval))

    def crash(stage):
        if stage == "event_commit" and journal.load().events:
            raise RuntimeError("interrupted after absorption commit")

    try:
        driver = EconomicDriver(inputs.config, inputs.settings, inputs.strategy, book)
        with preloaded_workers(), pytest.raises(RuntimeError, match="after absorption commit"):
            Simulator(journal, driver, fault=crash).run()
    finally:
        book.close()
    retained = journal.load()
    assert len(retained.events) == 1 and retained.events_delivered == 0
    loan = next(
        p.payload for p in retained.portfolio.positions if p.payload.target.allocation == "loan"
    )
    assert (
        (loan.principal > 0)
        if scenario == "credit"
        else (loan.principal == 0 and loan.absorbed_deficit > 0)
    )
    book = StressBook(inputs, scenario)
    try:
        driver = EconomicDriver(inputs.config, inputs.settings, inputs.strategy, book)
        with preloaded_workers():
            result = Simulator(Journal(journal.path, State), driver).run()
        assert len(result.events) == result.events_delivered == 1
        assert all(p.payload.empty for p in result.portfolio.positions)
        assert CompoundEconomic.report(result, driver)["closed"]
        exits = [c.pending.intent for c in result.completed[3:]]
        assert [i.target.allocation for i in exits] == (
            ["loan", "earn"] if scenario == "credit" else ["earn"]
        )
        assert all(i.kind == "withdraw" for i in exits)
    finally:
        book.close()


@pytest.mark.parametrize("scenario", ["missing", "paused"])
def test_borrower_risk_fault_preserves_debt_but_independent_lender_closes(
    prepared, tmp_path, scenario
):
    inputs = prepared
    book = StressBook(inputs, scenario)
    journal = Journal(tmp_path / "fault.sqlite", State)
    journal.create(CompoundEconomic.initial(inputs.settings, inputs.config, inputs.interval))
    try:
        driver = EconomicDriver(inputs.config, inputs.settings, inputs.strategy, book)
        with preloaded_workers(), pytest.raises(ValueError, match="unresolved obligations"):
            Simulator(journal, driver).run()
        result = journal.load()
        positions = {p.payload.target.allocation: p.payload for p in result.portfolio.positions}
        assert positions["loan"].principal < 0 and positions["collateral"].collateral > 0
        assert positions["earn"].empty
        assert len(result.events) == result.events_delivered == 1
        assert blocked(result, positions["loan"].target) is not None
        assert blocked(result, positions["earn"].target) is None
        assert result.completed[-1].pending.intent.target.allocation == "earn"
        report = CompoundEconomic.report(result, driver)
        assert not report["closed"] and report["missing_inputs"]
        assert State.model_validate_json(result.model_dump_json()) == result
    finally:
        book.close()
