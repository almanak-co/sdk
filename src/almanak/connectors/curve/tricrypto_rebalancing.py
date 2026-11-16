"""TriCrypto price-scale changes preserve profit gates and loss reverts."""

from dataclasses import dataclass, replace
from math import isqrt

from almanak.connectors.curve.crypto_integer import PRECISION, unsigned
from almanak.connectors.curve.crypto_prices import oracle
from almanak.connectors.curve.tricrypto_liquidity import xcp
from almanak.connectors.curve.tricrypto_math import invariant, marginal_prices


@dataclass(frozen=True)
class PriceState:
    cached_oracle: tuple[int, int]
    last_price: tuple[int, int]
    last_timestamp: int
    packed_rebalancing: int

    def __post_init__(self):
        if len(self.cached_oracle) != 2 or len(self.last_price) != 2:
            raise ValueError("TriCrypto rebalancing requires two prices")
        for value in (*self.cached_oracle, *self.last_price):
            if unsigned(value) == 0:
                raise ValueError("TriCrypto rebalancing requires positive prices")
        unsigned(self.last_timestamp)
        if not 0 < unsigned(self.packed_rebalancing) < 2**192:
            raise ValueError("TriCrypto rebalancing requires three packed uint64 parameters")
        if self.packed_rebalancing & (2**64 - 1) == 0:
            raise ValueError("TriCrypto moving-average duration must be positive")

    @property
    def parameters(self):
        return tuple((self.packed_rebalancing >> shift) & (2**64 - 1) for shift in (128, 64, 0))


def adjust(state, normalized, profit, virtual_price, price_oracle, parameters):
    extra_profit, minimum_step, _ = parameters
    if unsigned(virtual_price * 2 - PRECISION) <= unsigned(profit + 2 * extra_profit):
        return state, virtual_price
    distance = isqrt(
        sum(
            (unsigned(price * PRECISION) // scale - PRECISION) ** 2
            for price, scale in zip(price_oracle, state.price_scales, strict=True)
        )
    )
    step = max(minimum_step, distance // 5)
    if distance <= step:
        return state, virtual_price
    scales = tuple(
        unsigned(scale * (distance - step) + step * price) // distance
        for scale, price in zip(state.price_scales, price_oracle, strict=True)
    )
    values = (
        normalized[0],
        *(
            unsigned(normalized[index] * scales[index - 1]) // state.price_scales[index - 1]
            for index in (1, 2)
        ),
    )
    updated = invariant(state.amplification, state.gamma, values)
    candidate = unsigned(PRECISION * xcp(updated, scales)) // state.total_supply
    if candidate > PRECISION and unsigned(2 * candidate - PRECISION) > profit:
        return replace(state, invariant=updated, price_scales=scales), candidate
    return state, virtual_price


def tweak(
    state, admin, prices, normalized, *, timestamp, ramp_end, new_invariant=0, previous_ratio=0
):
    price_oracle = tuple(
        oracle(
            cached=cached,
            last_price=last,
            scale=scale,
            last_timestamp=prices.last_timestamp,
            timestamp=timestamp,
            duration=prices.parameters[2],
        )
        for cached, last, scale in zip(
            prices.cached_oracle, prices.last_price, state.price_scales, strict=True
        )
    )
    updated = new_invariant or invariant(
        state.amplification, state.gamma, normalized, previous_ratio
    )
    marginal = marginal_prices(normalized, updated, state.amplification, state.gamma)
    last_price = tuple(
        unsigned(price * scale) // PRECISION
        for price, scale in zip(marginal, state.price_scales, strict=True)
    )
    virtual_price = unsigned(PRECISION * xcp(updated, state.price_scales)) // state.total_supply
    profit = unsigned(admin.profit * virtual_price) // admin.virtual_price
    if ramp_end < timestamp and virtual_price <= admin.virtual_price:
        raise ValueError("TriCrypto virtual price decreased outside an active ramp")
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
