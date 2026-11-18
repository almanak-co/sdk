"""Fungible share economics use captured pools and explicit synthetic fee changes."""

import json
from datetime import UTC, datetime
from types import SimpleNamespace

import pytest

from almanak.connectors.aerodrome import authoring
from almanak.connectors.aerodrome.economic_acquisition import sample
from almanak.connectors.aerodrome.economic_simulation import principal, quote_at, simulate
from almanak.connectors.aerodrome.economic_state import EconomicPosition
from almanak.connectors.aerodrome.models import Parameters, Target
from almanak.dex_intents import CollectLiquidityFees, RevokeExchange
from tests.exchange_scope import without_exchange_budgets
from tests.test_aerodrome_observations import CAPTURE
from tests.test_aerodrome_reader import Replay


def setup(index):
    config = Parameters.model_validate_json(
        json.dumps(without_exchange_budgets(CAPTURE["rows"][index]["config"]))
    )
    target = config.target("pool")
    with Replay(index) as reader:
        block = reader.pin_block(CAPTURE["block"]["number"])
        pool = sample(reader, target, block)
    from almanak.connectors.aerodrome.observations import initial_owned

    payload = EconomicPosition(target=target, owned=initial_owned(config, "pool"))
    record = SimpleNamespace(
        payload=pool,
        reference=block,
        source_time=block.timestamp,
        available_at=block.timestamp,
        evidence=(),
    )
    return config, record, payload


def observed(record, payload):
    return SimpleNamespace(
        target=payload.target,
        owned=payload.owned,
        pool=record.payload.pool,
        block=record.reference,
        effective_at=datetime.fromtimestamp(record.available_at, UTC),
        simulated=True,
    )


def enter(config, record, payload):
    allocation = config.allocations[0]
    time = record.available_at
    quote = quote_at(
        record,
        payload.target,
        allocation.capital.model_copy(update={"amount": allocation.capital.amount // 2}),
        time,
    )
    intent = authoring.swap(allocation, observed(record, payload), quote)
    payload = simulate(config, intent, payload, record, time, "entry")[0]
    intent = authoring.open_liquidity(allocation, observed(record, payload), fraction_bps=5000)
    return simulate(config, intent, payload, record, time, "mint")[0]


@pytest.mark.parametrize("index", [0, 1])
def test_complete_lifecycle_keeps_fee_claims_separate_from_principal(index):
    config, record, payload = setup(index)
    original = record.payload.model_dump_json()
    payload = enter(config, record, payload)
    assert record.payload.model_dump_json() == original
    initial_shares = payload.owned.position.shares.amount
    increment = 10**18 // initial_shares + 1
    old_pool = record.payload.pool
    pool = old_pool.model_copy(
        update={"index0": old_pool.index0 + increment, "index1": old_pool.index1 + increment}
    )
    record.payload = record.payload.model_copy(update={"pool": pool})
    expected_fees = initial_shares * increment // 10**18
    allocation, time = config.allocations[0], record.available_at
    intent = authoring.increase_liquidity(allocation, observed(record, payload), fraction_bps=5000)
    payload = simulate(config, intent, payload, record, time, "increase")[0]
    assert payload.owned.position.fees0.amount == expected_fees
    assert payload.owned.position.fees1.amount == expected_fees
    shares = payload.owned.position.shares.amount // 2
    intent = authoring.decrease_liquidity(allocation, observed(record, payload), shares)
    payload, deltas, fees, delta, kind, _ = simulate(
        config, intent, payload, record, time, "partial"
    )
    assert deltas == tuple(shares * v // pool.total_supply for v in (pool.balance0, pool.balance1))
    assert delta == -shares and kind == "decrease" and fees == (0, 0)
    assert payload.owned.position.fees0.amount == expected_fees
    intent = CollectLiquidityFees[Target](target=payload.target, deadline=time + 300)
    payload, deltas, fees, delta, kind, _ = simulate(config, intent, payload, record, time, "claim")
    assert deltas == fees == (expected_fees, expected_fees)
    assert delta == 0 and kind == "collect"
    assert payload.owned.position.fees0.amount == payload.owned.position.fees1.amount == 0
    intent = authoring.close_liquidity(allocation, observed(record, payload))
    payload = simulate(config, intent, payload, record, time, "close")[0]
    assert payload.owned.position is None
    counter = next(
        v
        for v in (payload.owned.inventory0, payload.owned.inventory1)
        if v.asset == payload.target.counter_currency
    )
    quote = quote_at(record, payload.target, counter, time)
    intent = authoring.swap(allocation, observed(record, payload), quote)
    payload = simulate(config, intent, payload, record, time, "exit")[0]
    intent = RevokeExchange[Target](target=payload.target)
    assert simulate(config, intent, payload, record, time, "cleanup")[0] == payload
    assert EconomicPosition.model_validate_json(payload.model_dump_json()) == payload
    assert all(
        v.amount == 0
        for v in (payload.owned.inventory0, payload.owned.inventory1)
        if v.asset == payload.target.counter_currency
    )


def test_fixed_external_share_model_does_not_impose_historical_supply_on_hypothetical_shares():
    config, record, payload = setup(0)
    pool = record.payload.pool
    assert principal(pool, 2 * pool.total_supply) == (2 * pool.balance0, 2 * pool.balance1)
    payload = enter(config, record, payload)
    position = payload.owned.position.model_copy(
        update={
            "shares": payload.owned.position.shares.model_copy(update={"amount": 0}),
            "fees0": payload.owned.position.fees0.model_copy(update={"amount": 7}),
        }
    )
    payload = payload.model_copy(
        update={"owned": payload.owned.model_copy(update={"position": position})}
    )
    intent = authoring.close_liquidity(config.allocations[0], observed(record, payload))
    after, deltas, fees, delta, kind, _ = simulate(
        config, intent, payload, record, record.available_at, "fees-only"
    )
    assert after.owned.position is None and deltas == fees == (7, 0)
    assert delta == 0 and kind == "close"


def test_future_paused_and_unowned_position_inputs_are_refused():
    config, record, payload = setup(0)
    amount = config.allocations[0].capital
    with pytest.raises(ValueError, match="future"):
        quote_at(record, payload.target, amount, record.available_at - 1)
    record.payload = record.payload.model_copy(
        update={"pool": record.payload.pool.model_copy(update={"paused": True})}
    )
    with pytest.raises(ValueError, match="paused"):
        quote_at(record, payload.target, amount, record.available_at)
    intent = CollectLiquidityFees[Target](target=payload.target, deadline=record.available_at + 300)
    with pytest.raises(ValueError, match="hypothetical shares"):
        simulate(config, intent, payload, record, record.available_at, "foreign")
