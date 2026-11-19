"""Integer tick and liquidity arithmetic shared by independently compared V3/V4 paths.

Constants and algorithms follow MIT-licensed Uniswap v4-core revision
46c6834698c48bc4a463a86d8420f4eb1d7f3b75. V3 contract comparisons are retained
in tests/fixtures/uniswap-v3-math-vectors.json.gz. Swap fee and step accounting
remain connector-owned because partial-step semantics differ.
"""

from math import isqrt

Q96 = 1 << 96
Q128 = 1 << 128
UINT256 = 1 << 256
TICK_FACTORS = (
    0xFFFCB933BD6FAD37AA2D162D1A594001,
    0xFFF97272373D413259A46990580E213A,
    0xFFF2E50F5F656932EF12357CF3C7FDCC,
    0xFFE5CACA7E10E4E61C3624EAA0941CD0,
    0xFFCB9843D60F6159C9DB58835C926644,
    0xFF973B41FA98C081472E6896DFB254C0,
    0xFF2EA16466C96A3843EC78B326B52861,
    0xFE5DEE046A99A2A811C461F1969C3053,
    0xFCBE86C7900A88AEDCFFC83B479AA3A4,
    0xF987A7253AC413176F2B074CF7815E54,
    0xF3392B0822B70005940C7A398E4B70F3,
    0xE7159475A2C29B7443B29C7FA6E889D9,
    0xD097F3BDFD2022B8845AD8F792AA5825,
    0xA9F746462D870FDF8A65DC1F90E061E5,
    0x70D869A156D2A1B890BB3DF62BAF32F7,
    0x31BE135F97D08FD981231505542FCFA6,
    0x9AA508B5B7A84E1C677DE54F3E99BC9,
    0x5D6AF8DEDB81196699C329225EE604,
    0x2216E584F5FA1EA926041BEDFE98,
    0x48A170391F7DC42444E8FA2,
)


def sqrt_at_tick(tick: int) -> int:
    if type(tick) is not int or abs(tick) > 887272:
        raise ValueError("tick is outside Uniswap TickMath bounds")
    ratio = Q128
    for bit, factor in enumerate(TICK_FACTORS):
        if abs(tick) & (1 << bit):
            ratio = ratio * factor >> 128
    if tick > 0:
        ratio = (UINT256 - 1) // ratio
    return (ratio + (1 << 32) - 1) >> 32


def divide(numerator: int, denominator: int, up: bool = False) -> int:
    return (numerator + denominator - 1) // denominator if up else numerator // denominator


def amounts(liquidity: int, price: int, lower: int, upper: int, *, up=False):
    if liquidity < 0 or not 0 < lower < upper or price <= 0:
        raise ValueError("invalid liquidity or sqrt-price interval")
    clipped = max(lower, min(price, upper))
    amount0 = divide(liquidity * Q96 * (upper - clipped), clipped * upper, up)
    amount1 = divide(liquidity * (clipped - lower), Q96, up)
    return amount0, amount1


def liquidity_for(amount0: int, amount1: int, price: int, lower: int, upper: int) -> int:
    if min(amount0, amount1) < 0 or not 0 < lower < upper or price <= 0:
        raise ValueError("invalid inventory or sqrt-price interval")
    clipped = max(lower, min(price, upper))
    limits = []
    if clipped < upper:
        limits.append(amount0 * (clipped * upper // Q96) // (upper - clipped))
    if clipped > lower:
        limits.append(amount1 * Q96 // (clipped - lower))
    return min(limits)


def tolerance_edges(price: int, tolerance_bps: int) -> tuple[int, int]:
    """The sqrt prices at the two edges of a price window of `tolerance_bps` around `price`."""
    if type(tolerance_bps) is not int or not 0 <= tolerance_bps < 10000:
        raise ValueError("price tolerance requires integer basis points in [0, 10000)")
    low = isqrt(price * price * (10000 - tolerance_bps) // 10000)
    high = isqrt(price * price * (10000 + tolerance_bps) // 10000) + 1
    return low, high


def addition_maximums(
    amount0: int, amount1: int, price: int, lower: int, upper: int, tolerance_bps: int
) -> tuple[int, int]:
    """The most of each currency an addition takes at any price within `tolerance_bps`.

    A position manager takes the liquidity both offered amounts cover at the execution
    price. The currency that is not the limiting one is taken in proportion, and that
    proportion grows without bound as the price moves against it: token0 as the price
    falls, token1 as it rises. So the most token0 is taken at the low edge of the
    window and the most token1 at the high edge. Offering no more than these two
    amounts changes nothing inside the window, and outside it the trimmed currency
    becomes the limiting one, so the other falls below the floor `addition_minimums`
    derives from the same pair: the addition is bounded in both price directions.
    """
    low, high = tolerance_edges(price, tolerance_bps)
    at_low = liquidity_for(amount0, amount1, low, lower, upper)
    at_high = liquidity_for(amount0, amount1, high, lower, upper)
    return (
        min(amount0, amounts(at_low, low, lower, upper, up=True)[0]),
        min(amount1, amounts(at_high, high, lower, upper, up=True)[1]),
    )


def addition_minimums(
    amount0: int, amount1: int, price: int, lower: int, upper: int, tolerance_bps: int
) -> tuple[int, int, int]:
    """Amount and liquidity floors that hold at every price within `tolerance_bps` of `price`.

    A position manager recomputes liquidity from the maxima at the execution price, so
    the token0 it pulls only falls as price rises and token1 only falls as price falls.
    Each floor is therefore what a mint takes at the far edge of the admitted price
    window, P·(1 ± tolerance). Floors taken at the observed price alone revert a narrow
    band on a move of a single tick.

    Liquidity is floored from the maxima at each price, so what is pulled wobbles by
    the amounts of a couple of liquidity units and a parts-per-billion share of the
    ratio rounding; every floor leaves that margin.
    """
    low, high = tolerance_edges(price, tolerance_bps)
    low_liquidity = liquidity_for(amount0, amount1, low, lower, upper)
    high_liquidity = liquidity_for(amount0, amount1, high, lower, upper)
    liquidity = min(
        low_liquidity, high_liquidity, liquidity_for(amount0, amount1, price, lower, upper)
    )
    return (
        rounding_floor(
            amounts(high_liquidity, high, lower, upper)[0],
            amounts(2, low, lower, upper, up=True)[0],
        ),
        rounding_floor(
            amounts(low_liquidity, low, lower, upper)[1],
            amounts(2, high, lower, upper, up=True)[1],
        ),
        rounding_floor(liquidity, 2),
    )


def rounding_floor(amount: int, unit: int) -> int:
    return max(0, amount - unit - amount // 10**9 - 1)


def accrued(liquidity: int, previous: int, current: int) -> int:
    if min(liquidity, previous, current) < 0 or max(previous, current) >= UINT256:
        raise ValueError("fee growth requires uint256 values and nonnegative liquidity")
    return liquidity * ((current - previous) % UINT256) // Q128


def amount_delta(price, target, liquidity, currency0, *, up):
    lower, upper = sorted((price, target))
    numerator = liquidity * (upper - lower)
    return divide(numerator * Q96, lower * upper, up) if currency0 else divide(numerator, Q96, up)


def next_price(price, liquidity, amount, zero_for_one):
    if liquidity <= 0 or price <= 0:
        raise ValueError("next price requires positive liquidity and price")
    if not zero_for_one:
        result = price + amount * Q96 // liquidity
        if result >= 2**160:
            raise ValueError("next sqrt price overflows uint160")
        return result
    if amount == 0:
        return price
    numerator, product = liquidity * Q96, amount * price
    # Match the Solidity overflow branch, whose rounded fallback is not always
    # identical to an unbounded rational calculation.
    if numerator + product < UINT256:
        return divide(numerator * price, numerator + product, True)
    return divide(numerator, numerator // price + amount, True)


def next_tick(words, tick, spacing, zero_for_one):
    compressed = tick // spacing + (0 if zero_for_one else 1)
    word, bit = compressed >> 8, compressed & 255
    if word not in words:
        raise ValueError("swap requires an unprepared tick bitmap word")
    if zero_for_one:
        masked = words[word] & ((1 << (bit + 1)) - 1)
        offset = masked.bit_length() - 1 if masked else 0
    else:
        masked = words[word] & ~((1 << bit) - 1)
        offset = (masked & -masked).bit_length() - 1 if masked else 255
    return max(-887272, min(887272, ((word << 8) + offset) * spacing)), bool(masked)


def tick_at_price(price):
    low, high = -887272, 887272
    while low < high:
        middle = (low + high + 1) // 2
        if sqrt_at_tick(middle) <= price:
            low = middle
        else:
            high = middle - 1
    return low
