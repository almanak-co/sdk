"""Shared effect assertions use real captured pools and explicit synthetic fee changes."""

from datetime import UTC, datetime
from types import SimpleNamespace

from almanak.connectors.v3_pool_positions import SimulatedPosition
from almanak.dex_intents import CollectLiquidityFees, RevokeExchange


def observed(record, payload):
    return SimpleNamespace(
        target=payload.target,
        owned=payload.owned,
        pool=record.payload.pool,
        block=record.reference,
        effective_at=datetime.fromtimestamp(record.available_at, UTC),
    )


def assert_all_actions(config, record, payload, authoring, quote_at, simulate, position_type):
    target, allocation = payload.target, config.allocations[0]
    original = record.payload.model_dump_json()
    time = record.available_at
    quote = quote_at(
        record,
        target,
        allocation.capital.model_copy(update={"amount": allocation.capital.amount // 2}),
        time,
    )
    intent = authoring.swap(allocation, observed(record, payload), quote)
    payload, _, _, _, kind, fill = simulate(config, intent, payload, record, time, "swap-entry")
    assert kind == "swap" and fill.calculation == "prepared_integer_pool"
    assert record.payload.model_dump_json() == original
    intent = authoring.open_liquidity(allocation, observed(record, payload), fraction_bps=7000)
    payload = simulate(config, intent, payload, record, time, "mint-identity")[0]
    assert isinstance(payload.owned.position, SimulatedPosition)
    liquidity = payload.owned.position.liquidity
    # This is an explicit synthetic fee increment, not a measured temporal path.
    record.payload = record.payload.model_copy(
        update={"global_growth": tuple((v + 2**128) % 2**256 for v in record.payload.global_growth)}
    )
    intent = authoring.increase_liquidity(allocation, observed(record, payload), fraction_bps=5000)
    payload = simulate(config, intent, payload, record, time, "increase-identity")[0]
    assert payload.owned.position.identity == "mint-identity"
    assert payload.owned.position.owed0 == liquidity and payload.owned.position.owed1 == liquidity
    intent = authoring.decrease_liquidity(
        allocation, observed(record, payload), payload.owned.position.liquidity // 3
    )
    payload, _, fees, delta, kind, _ = simulate(config, intent, payload, record, time, "decrease")
    assert fees == (liquidity, liquidity) and delta < 0 and kind == "decrease"
    intent = CollectLiquidityFees[type(target)](target=target, deadline=time + 300)
    payload, deltas, fees, _, kind, _ = simulate(config, intent, payload, record, time, "collect")
    assert deltas == fees == (0, 0) and kind == "collect"
    intent = authoring.close_liquidity(allocation, observed(record, payload))
    payload = simulate(config, intent, payload, record, time, "close")[0]
    assert payload.owned.position is None
    counter = next(
        a
        for a in (payload.owned.inventory0, payload.owned.inventory1)
        if a.asset == target.counter_currency
    )
    quote = quote_at(record, target, counter, time)
    intent = authoring.swap(allocation, observed(record, payload), quote)
    payload = simulate(config, intent, payload, record, time, "swap-exit")[0]
    intent = RevokeExchange[type(target)](target=target)
    payload = simulate(config, intent, payload, record, time, "cleanup")[0]
    assert (
        next(
            a.amount
            for a in (payload.owned.inventory0, payload.owned.inventory1)
            if a.asset == target.counter_currency
        )
        == 0
    )
    assert position_type.model_validate_json(payload.model_dump_json()) == payload
