"""TwoCrypto price-scale changes preserve profit gates and loss reverts."""

from dataclasses import dataclass, replace

from almanak.connectors.curve.crypto_integer import PRECISION, unsigned
from almanak.connectors.curve.twocrypto_liquidity import xcp
from almanak.connectors.curve.twocrypto_math import invariant
from almanak.connectors.curve.twocrypto_prices import marginal_price, oracle


@dataclass(frozen=True)
class PriceState:
    cached_oracle: int
    last_price: int
    last_timestamp: int
    packed_rebalancing: int

    def __post_init__(self):
        for value in (self.cached_oracle, self.last_price):
            if unsigned(value) == 0:
                raise ValueError("TwoCrypto rebalancing requires positive prices")
        unsigned(self.last_timestamp)
        if not 0 < unsigned(self.packed_rebalancing) < 2**192:
            raise ValueError("TwoCrypto rebalancing requires three packed uint64 parameters")
        if self.packed_rebalancing & (2**64 - 1) == 0:
            raise ValueError("TwoCrypto moving-average duration must be positive")

    @property
    def parameters(self):
        return tuple((self.packed_rebalancing >> shift) & (2**64 - 1) for shift in (128, 64, 0))


def adjust(state, normalized, profit, virtual_price, price_oracle, parameters):
    extra_profit, minimum_step, _ = parameters
    if unsigned(virtual_price * 2 - PRECISION) <= unsigned(profit + 2 * extra_profit):
        return state, virtual_price
    distance = abs((price_oracle * PRECISION) % 2**256 // state.price_scale - PRECISION)
    step = max(minimum_step, distance // 5)
    if distance <= step:
        return state, virtual_price
    scale = unsigned(state.price_scale * (distance - step) + step * price_oracle) // distance
    values = normalized[0], unsigned(normalized[1] * scale) // state.price_scale
    updated = invariant(state.amplification, state.gamma, values)
    candidate = unsigned(PRECISION * xcp(updated, scale)) // state.total_supply
    if candidate > PRECISION and unsigned(2 * candidate - PRECISION) > profit:
        return replace(state, invariant=updated, price_scale=scale), candidate
    return state, virtual_price


def tweak(
    state, admin, prices, normalized, *, timestamp, ramp_end, new_invariant=0, previous_ratio=0
):
    price_oracle = oracle(
        cached=prices.cached_oracle,
        last_price=prices.last_price,
        scale=state.price_scale,
        last_timestamp=prices.last_timestamp,
        timestamp=timestamp,
        duration=prices.parameters[2],
    )
    updated = new_invariant or invariant(
        state.amplification, state.gamma, normalized, previous_ratio
    )
    last_price = (
        unsigned(
            marginal_price(normalized, updated, state.amplification, state.gamma)
            * state.price_scale
        )
        // PRECISION
    )
    virtual_price = unsigned(PRECISION * xcp(updated, state.price_scale)) // state.total_supply
    profit = unsigned(admin.profit * virtual_price) // admin.virtual_price
    if ramp_end < timestamp and virtual_price <= admin.virtual_price:
        raise ValueError("TwoCrypto virtual price decreased outside an active ramp")
    changed, virtual_price = adjust(
        replace(state, invariant=updated),
        normalized,
        profit,
        virtual_price,
        price_oracle,
        prices.parameters,
    )
    return (
        changed,
        replace(admin, profit=profit, virtual_price=virtual_price),
        replace(
            prices, cached_oracle=price_oracle, last_price=last_price, last_timestamp=timestamp
        ),
    )
