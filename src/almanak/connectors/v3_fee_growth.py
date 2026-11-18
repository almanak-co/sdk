"""Verified Uniswap/Pancake V3 growth with initialized-range requirements."""

MODULUS = 2**256
Q128 = 2**128


def inside(current, lower, upper, global_growth, outside_lower, outside_upper):
    if not -887272 <= lower < upper <= 887272 or not -887272 <= current <= 887272:
        raise ValueError("fee growth requires valid ordered ticks")
    if any(
        type(value) is not int or not 0 <= value < MODULUS
        for pair in (global_growth, outside_lower, outside_upper)
        for value in pair
    ):
        raise ValueError("fee growth requires uint256 accounting words")
    result = []
    for total, low, high in zip(global_growth, outside_lower, outside_upper, strict=True):
        below = low if current >= lower else total - low
        above = high if current < upper else total - high
        result.append((total - below - above) % MODULUS)
    return tuple(result)


def accrued(liquidity, previous, current):
    if type(liquidity) is not int or not 0 <= liquidity < Q128:
        raise ValueError("fee accrual requires uint128 liquidity")
    if any(type(value) is not int or not 0 <= value < MODULUS for value in (previous, current)):
        raise ValueError("fee accrual requires uint256 checkpoints")
    # V3 casts each earned quantity to uint128 before adding it to tokensOwed.
    return (((current - previous) % MODULUS) * liquidity // Q128) % Q128


def sampled_inside(sample, target):
    if sample.pool.source != target.pool or target.range is None:
        raise ValueError("LP fee sample requires the exact configured pool and range")
    ticks = {tick.index: tick for tick in sample.ticks}
    boundaries = (target.range.lower, target.range.upper)
    if any(tick not in ticks or not ticks[tick].gross for tick in boundaries):
        raise ValueError("fixed historical LP growth requires both initialized boundary ticks")
    lower, upper = (ticks[tick] for tick in boundaries)
    return inside(
        sample.pool.tick,
        *boundaries,
        sample.global_growth,
        (lower.outside0, lower.outside1),
        (upper.outside0, upper.outside1),
    )
