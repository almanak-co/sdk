"""Both historical models apply contract principal minima despite uncollected fees."""

import pytest

from almanak.backtest.engine import Journal, Simulator
from almanak.connectors.uniswap_v4.math import amounts
from almanak.connectors.uniswap_v4.reader import sqrt_bounds
from tests.uniswap_economic_support import prepared
from tests.uniswap_support import historical_model


def state_before_close(manifest, expected):
    portfolio = manifest.initial.portfolio
    for completed in expected.completed:
        pending = completed.pending
        if pending.intent.kind == "close_liquidity":
            return manifest.initial.model_copy(
                update={
                    "portfolio": portfolio,
                    "pending": pending,
                    "time": pending.decision_time,
                    "mode": "teardown"
                    if pending.decision_time >= manifest.initial.schedule.end
                    else "running",
                }
            ), completed.transition.result
        portfolio = portfolio.apply(completed.transition.effects)
    raise AssertionError("retained reference lifecycle did not close liquidity")


@pytest.mark.parametrize("chain", ["ethereum", "avalanche"])
@pytest.mark.parametrize("mode", ["historical", "economic"])
@pytest.mark.parametrize("currency", [0, 1])
@pytest.mark.parametrize("offset", [-1, 0, 1])
def test_close_checks_principal_and_credits_fees(tmp_path, chain, mode, currency, offset):
    manifest, expected, model = (
        prepared(tmp_path, chain, "lp")
        if mode == "economic"
        else historical_model("lp" if chain == "ethereum" else "avalanche_lp")
    )
    try:
        state, original = state_before_close(manifest, expected)
        before = original.before
        principal = amounts(
            before.owned.position.liquidity, before.pool.sqrt_price_x96, *sqrt_bounds(before.target)
        )
        fees = (original.fees0, original.fees1)
        assert fees[currency] > 0 and principal[currency] > 0
        field = f"minimum{currency}"
        intent = state.pending.intent
        intent = intent.model_copy(
            update={
                field: getattr(intent, field).model_copy(
                    update={"amount": principal[currency] + offset}
                )
            }
        )
        state = state.model_copy(
            update={"pending": state.pending.model_copy(update={"intent": intent})}
        )
        journal = Journal(tmp_path / "state.sqlite", type(state))
        journal.create(state)
        simulator = Simulator(journal, model)
        if offset > 0:
            with pytest.raises(ValueError, match="principal minima"):
                simulator.tick()
            assert journal.load() == state
        else:
            simulator.tick()
            result = journal.load().completed[0].transition.result
            assert result.after.position is None
            assert (result.delta0, result.delta1) == tuple(
                p + f for p, f in zip(principal, fees, strict=True)
            )
    finally:
        if mode == "economic":
            model.book.close()
