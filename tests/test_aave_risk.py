"""Risk decisions and explicit oracle failures using retained debt evidence."""

import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from almanak.connectors.aave_v4.debt import validate_oracle_sources
from almanak.connectors.aave_v4.models import DecisionInput
from almanak.connectors.aave_v4.state import Checkpoint
from almanak.gateway.ethereum import DataError
from tests.lending_captures import read_capture
from tests.retained_scheduling import checkpoint_input
from tests.test_aave_debt import reference_module as reference_module


@pytest.fixture(scope="module")
def borrowing_proof():
    # The recorded lifecycle predates scope-only authority; read it in today's interface.
    path = Path(__file__).parent / "fixtures/safe-aave-borrowing-lifecycle.json.gz"
    return Checkpoint.model_validate_json(
        json.dumps(checkpoint_input(json.loads(read_capture(path))))
    )


def holding_observations(proof):
    observations = {}
    for outcome in proof.outcomes:
        if outcome.operation.kind in ("repay", "withdraw"):
            break
        observations.update((o.allocation, o) for o in outcome.decision.observations)
        observations[outcome.after.allocation] = outcome.after
    return tuple(observations[a.name] for a in proof.config.positions)


def test_measured_health_breach_requests_repayment_before_main_withdrawal(
    borrowing_proof, reference_module
):
    proof = borrowing_proof
    observations = holding_observations(proof)
    debt = proof.config.debts[0]
    stressed = tuple(
        o.model_copy(update={"health_factor": debt.minimum_health_factor - 1})
        if o.allocation == debt.name
        else o
        for o in observations
    )
    decision = reference_module.decide(
        DecisionInput(
            config=proof.config,
            trigger=proof.outcomes[0].decision.trigger,
            memory=reference_module.State(**proof.memory.values),
            owned=proof.owned,
            observations=stressed,
        )
    )
    assert decision.intent == proof.config.repay(debt.name)


@pytest.mark.parametrize("fault", ["oracle", "feed", "count", "zero", "unavailable"])
def test_borrowing_cannot_substitute_a_missing_or_changed_oracle(borrowing_proof, fault):
    config = borrowing_proof.config
    debt = config.debts[0]
    observed = next(o.before for o in borrowing_proof.outcomes if o.operation.kind == "borrow")
    reader = oracle_reader(debt, fault)
    with pytest.raises(DataError):
        validate_oracle_sources(reader, config, debt, observed.position.reserve.block)


def oracle_reader(debt, fault):
    def call(address, signature, block, **kwargs):
        if signature == "ORACLE()":
            return ("0x" + "ab" * 20 if fault == "oracle" else debt.oracle,)
        if signature == "getReserveSource(uint256)":
            if fault == "feed":
                return ("0x" + "ab" * 20,)
            return (
                debt.debt_price_source
                if kwargs["args"][0] == debt.reserve.reserve_id
                else debt.collateral_price_sources[0],
            )
        return oracle_prices(signature, fault)

    return SimpleNamespace(call=call)


def oracle_prices(signature, fault):
    if signature != "getReservesPrices(uint256[])":
        raise AssertionError("unexpected price fallback")
    if fault == "unavailable":
        raise DataError("unavailable", "fixture oracle read unavailable")
    return ((1,) if fault == "count" else (1, 0),)
