"""Experimental model coverage uses test-local support, never public lifecycle acceptance."""

from pathlib import Path

import pytest

from almanak.backtest.engine import Journal, Simulator
from almanak.connectors.gmx_v2.economic_book import MarketBook
from almanak.connectors.gmx_v2.economic_parameters import initial
from almanak.connectors.gmx_v2.economic_reporting import report
from almanak.connectors.gmx_v2.economic_state import EconomicResult, State
from almanak.strategy import StrategyDefinition
from almanak.triggers import IntervalSelection
from tests.gmx_candidates import candidate_model as candidate_model
from tests.gmx_retired import retired_closure as retired_closure
from tests.test_gmx_economic_book import prepared as prepared
from tests.test_gmx_economic_requests import request_case as request_case

# The retained prepared datasets predate GMX's 1 October rotation; these accounting checks
# re-decode them against the retired closure they were collected from (tests.gmx_retired).
pytestmark = pytest.mark.usefixtures("retired_closure")


def test_real_strategy_marks_accepted_escrow_without_using_an_earlier_publication(
    prepared, candidate_model
):
    directory, config, settings, _ = prepared
    name = (
        "arbitrum" if str(config.allocations[0].chain.chain_id) == "eip155:42161" else "avalanche"
    )
    strategy = StrategyDefinition.read(Path(f"strategies/gmx_v2_{name}_swap/strategy.py"))
    book = MarketBook(directory, config, settings)
    try:
        driver = candidate_model(config, settings, strategy, book)
        journal = Journal(directory / "run.sqlite", State)
        journal.create(
            initial(settings, config, IntervalSelection(configured=config.decision_trigger))
        )
        simulator = Simulator(journal, driver)
        state = simulator.run()
        accepted = [
            value.transition.result
            for value in state.completed
            if isinstance(value.transition.result, EconomicResult)
        ]
        assert accepted and accepted[0].phase == "accepted"
        assert all(value.phase in ("accepted", "waiting") for value in accepted)
        assert state.portfolio.positions[0].payload.escrow is not None
        assert state.mode == "finished"
        result = report(state, driver)
        assert result["closed"] is False
        assert result["positions"][0]["escrow"] is not None
        assert result["request_steps"][0]["keeper_paid"]["amount"] == 0
    finally:
        book.close()


@pytest.mark.parametrize("stage", ["effects_commit", "outcome"])
def test_real_strategy_resumes_accepted_escrow_without_duplicate_debits(
    prepared, stage, candidate_model
):
    directory, config, settings, _ = prepared
    name = (
        "arbitrum" if str(config.allocations[0].chain.chain_id) == "eip155:42161" else "avalanche"
    )
    strategy = StrategyDefinition.read(Path(f"strategies/gmx_v2_{name}_swap/strategy.py"))
    seed = initial(settings, config, IntervalSelection(configured=config.decision_trigger))
    with_book = MarketBook(directory, config, settings)
    try:
        baseline = Journal(directory / "baseline.sqlite", State)
        baseline.create(seed)
        driver = candidate_model(config, settings, strategy, with_book)
        expected = Simulator(baseline, driver).run()
        interrupted = Journal(directory / "interrupted.sqlite", State)
        interrupted.create(seed)

        def interrupt(actual):
            if actual == stage:
                raise RuntimeError("simulated process interruption")

        with pytest.raises(RuntimeError, match="simulated process interruption"):
            Simulator(interrupted, driver, fault=interrupt).run()
        restored = Journal(directory / "interrupted.sqlite", State)
        assert restored.load().portfolio.positions[0].payload.escrow is not None
        replacement = candidate_model(config, settings, strategy, with_book)
        assert Simulator(restored, replacement).run() == expected
    finally:
        with_book.close()


def test_rejected_admission_preserves_balances_and_has_no_fabricated_receipt(
    prepared, request_case, candidate_model
):
    from almanak.connectors.gmx_v2.authority import declared
    from almanak.connectors.gmx_v2.economic_state import EconomicFailure

    directory, _, _, _ = prepared
    config, settings, state, _, pending, _, _ = request_case
    book = MarketBook(directory, config, settings)
    try:
        model = candidate_model(config, settings, None, book, authority=declared(config))
        expired = pending.intent.model_copy(update={"deadline": settings.schedule.start})
        rejected = model.execute(state, pending.model_copy(update={"intent": expired}))
        assert isinstance(rejected.result, EconomicFailure)
        assert rejected.complete
        assert rejected.result.unavailable is None
        assert state.portfolio.apply(rejected.effects) == state.portfolio
        assert "receipt" not in rejected.result.model_dump()
    finally:
        book.close()


def test_teardown_exit_cancels_accepted_escrow_even_when_all_allowances_are_spent(
    prepared, request_case, candidate_model
):
    from almanak.connectors.gmx_v2.api import Actions
    from almanak.connectors.gmx_v2.authority import declared
    from almanak.connectors.gmx_v2.economic_requests import admit
    from almanak.dex_intents import RevokeExchange

    directory, _, _, _ = prepared
    config, settings, state, held, pending, risk, _ = request_case
    payload, _ = admit(config, held.payload, pending, risk)
    payload = payload.model_copy(
        update={
            "allowances": tuple(
                value.model_copy(update={"amount": 0}) for value in payload.allowances
            )
        }
    )
    book = MarketBook(directory, config, settings)
    try:
        model = candidate_model(config, settings, None, book, authority=declared(config))
        observed = model.snapshot(state, payload, state.time)
        assert observed.needs_cleanup and observed.request_pending
        assert isinstance(
            Actions(None).exit_exchange(payload.target.allocation, observed), RevokeExchange
        )
    finally:
        book.close()
