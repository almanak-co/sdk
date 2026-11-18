"""Driver component proofs with captured state; no sandbox or lifecycle claim."""

from types import SimpleNamespace

import pytest

from almanak.backtest.engine import Schedule
from almanak.backtest.inventory import Fund
from almanak.connectors.uniswap_v3 import authoring
from almanak.connectors.uniswap_v3.authority import declared
from almanak.connectors.uniswap_v3.economic_model import EconomicModel
from almanak.connectors.uniswap_v3.economic_parameters import initial, parameters, wallet
from almanak.connectors.uniswap_v3.economic_state import EconomicFailure, State
from almanak.dex_intents import ExitExchange
from almanak.models import AssetAmount, SimulationAccount
from almanak.triggers import IntervalSelection
from tests.exchange_dust import holding, settle_dust
from tests.test_uniswap_v3_economic_simulation import setup


def component(index=0):
    author, record, _ = setup(index)
    allocation = author.allocations[0]
    at = record.available_at
    schedule = Schedule(
        start=at, end=at + 100, latency_seconds=1, end_policy="teardown", cleanup_seconds=100
    )
    settings = SimpleNamespace(
        schedule=schedule,
        liquidity_model="fixed_historical_fee_growth",
        funds=(
            Fund(
                name="fund",
                profile=SimulationAccount(fund=author.accounts.bindings[0].profile.fund),
                balances=(
                    *tuple(
                        AssetAmount(
                            asset=a,
                            amount=allocation.capital.amount
                            if a == allocation.capital.asset
                            else 0,
                        )
                        for a in (allocation.pool.currency0, allocation.pool.currency1)
                    ),
                    AssetAmount(asset=allocation.native_asset, amount=10000),
                ),
            ),
        ),
        payers=(),
        costs=SimpleNamespace(
            kind="fixed_native",
            swap=3,
            mint=5,
            increase=5,
            decrease=4,
            collect=2,
            close=4,
            revoke=1,
        ),
    )
    interval = IntervalSelection(configured=author.decision_trigger)
    config = parameters(author, settings, interval)
    state = initial(settings, config, interval)
    # Wiring/support and sandbox admission have separate integration gates.
    model = object.__new__(EconomicModel)
    model.config, model.settings, model.strategy = config, settings, None
    model.book = SimpleNamespace(at=lambda target, time: record)
    model.authority = declared(config)
    model.capabilities = (SimpleNamespace(chain=allocation.chain),)
    return model, state


def execute(model, state, intent, *, latency=1):
    pending = SimpleNamespace(
        intent=intent,
        identity=f"step-{state.time}",
        decision_time=state.time,
        eligible_at=state.time + latency,
    )
    result = model.execute(state, pending)
    after = state.model_copy(
        update={"portfolio": state.portfolio.apply(result.effects), "time": pending.eligible_at}
    )
    assert State.model_validate_json(after.model_dump_json()) == after
    return after, result


@pytest.mark.parametrize("index", [0, 1])
def test_component_exchange_roundtrip_and_exit_continuation_keep_payer_costs(index):
    model, state = component(index)
    allocation = model.config.allocations[0]
    observed = model.observe(state)[0]
    state, result = execute(
        model, state, authoring.swap(allocation, observed, observed.entry_quote)
    )
    assert result.result.success and result.result.gas_paid.amount == 3
    observed = model.observe(state)[0]
    state, result = execute(
        model, state, authoring.open_liquidity(allocation, observed, fraction_bps=7000)
    )
    assert result.result.success
    intent = ExitExchange[type(observed.target)](target=observed.target)
    state, result = execute(model, state, intent)
    assert result.result.success and not result.complete
    assert result.result.after.position is None
    state, result = execute(model, state, intent)
    assert result.result.success and result.complete
    assert model.closed(state, model.observe(state))
    assert (
        state.portfolio.balance(wallet(observed.target, allocation.native_asset))
        == 10000 - 3 - 5 - 4 - 3
    )


@pytest.mark.parametrize("fault", ["deadline", "minimum", "mode", "payer"])
def test_refused_component_actions_preserve_portfolio_and_serialize_failure(fault):
    model, state = component()
    observed = model.observe(state)[0]
    intent = authoring.swap(model.config.allocations[0], observed, observed.entry_quote)
    latency = 1
    if fault == "deadline":
        latency = 301
    elif fault == "minimum":
        intent = intent.model_copy(
            update={"minimum_output": intent.minimum_output.model_copy(update={"amount": 1})}
        )
    elif fault == "mode":
        state = state.model_copy(update={"mode": "teardown"})
    else:
        model.settings.costs.swap = 10001
    after, result = execute(model, state, intent, latency=latency)
    assert after.portfolio == state.portfolio
    assert isinstance(result.result, EconomicFailure) and result.result.status == "failed"
    assert EconomicFailure.model_validate_json(result.result.model_dump_json()) == result.result


def test_exit_holding_only_counter_dust_completes_without_a_step():
    model, state = component(0)
    target = model.config.target(model.config.allocations[0].name)
    record = model.book.at(target, state.time)

    def quote(at, amount):
        return model.simulation.quote_at(record, target, amount, at.time)

    settle_dust(model, state, target, target.counter_currency, quote)
    swappable = holding(model, state, {target.counter_currency: 10**15})
    observed = model.observe(swappable)[0]
    assert observed.exit_quote is not None and observed.exit_residual is None
    assert not model.closed(swappable, (observed,))
