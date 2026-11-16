"""Checked Morpho Blue share arithmetic and AdaptiveCurve IRM revision math."""

WAD = 10**18
ORACLE_SCALE = 10**36
VIRTUAL_SHARES = 10**6
YEAR = 365 * 24 * 3600
TARGET_UTILIZATION = 9 * 10**17
MIN_TARGET_RATE = 10**15 // YEAR
MAX_TARGET_RATE = 2 * WAD // YEAR


def uint(value: int, bits: int = 256) -> int:
    if type(value) is not int or not 0 <= value < 2**bits:
        raise ValueError(f"Morpho arithmetic requires uint{bits}")
    return value


def signed(value: int) -> int:
    if type(value) is not int or not -(2**255) <= value < 2**255:
        raise ValueError("Morpho arithmetic requires int256")
    return value


def div_to_zero(numerator: int, denominator: int) -> int:
    signed(numerator)
    if not signed(denominator):
        raise ValueError("Morpho arithmetic cannot divide by zero")
    magnitude = abs(numerator) // abs(denominator)
    return signed(-magnitude if (numerator < 0) != (denominator < 0) else magnitude)


def mul_div(left: int, right: int, denominator: int, *, round_up: bool = False) -> int:
    product = uint(uint(left) * uint(right))
    if not uint(denominator):
        raise ValueError("Morpho arithmetic cannot divide by zero")
    return uint(product + denominator - 1) // denominator if round_up else product // denominator


def to_shares(assets: int, total_assets: int, total_shares: int, *, round_up: bool = False) -> int:
    return mul_div(
        assets,
        uint(uint(total_shares) + VIRTUAL_SHARES),
        uint(uint(total_assets) + 1),
        round_up=round_up,
    )


def to_assets(shares: int, total_assets: int, total_shares: int, *, round_up: bool = False) -> int:
    return mul_div(
        shares,
        uint(uint(total_assets) + 1),
        uint(uint(total_shares) + VIRTUAL_SHARES),
        round_up=round_up,
    )


def compounded(rate: int, elapsed: int) -> int:
    first = uint(uint(rate) * uint(elapsed))
    second = mul_div(first, first, 2 * WAD)
    third = mul_div(second, first, 3 * WAD)
    return uint(uint(first + second) + third)


def exponential(value: int) -> int:
    signed(value)
    if value < -41446531673892822312:
        return 0
    if value >= 93859467695000404319:
        return 57716089161558943949701069502944508345128422502756744429568
    log_two = 693147180559945309
    adjustment = -(log_two // 2) if value < 0 else log_two // 2
    exponent = div_to_zero(value + adjustment, log_two)
    remainder = value - exponent * log_two
    polynomial = WAD + remainder + remainder * remainder // WAD // 2
    return polynomial << exponent if exponent >= 0 else polynomial >> -exponent


def adapted_target(start: int, linear: int) -> int:
    point = div_to_zero(signed(signed(start) * exponential(linear)), WAD)
    return min(MAX_TARGET_RATE, max(MIN_TARGET_RATE, point))


def average_target(start: int, linear: int) -> tuple[int, int]:
    if start == 0:
        initial = 4 * 10**16 // YEAR
        return initial, initial
    if linear == 0:
        return start, start
    end = adapted_target(start, linear)
    middle = adapted_target(start, div_to_zero(linear, 2))
    return div_to_zero(signed(start + end + 2 * middle), 4), end


def adaptive_rate(supply: int, borrow: int, target: int, elapsed: int) -> tuple[int, int]:
    uint(supply, 128)
    uint(borrow, 128)
    uint(target)
    signed(target)
    uint(elapsed)
    if borrow > supply:
        raise ValueError("Morpho borrowed market assets exceed supplied assets")
    if target and not MIN_TARGET_RATE <= target <= MAX_TARGET_RATE:
        raise ValueError("Morpho target rate lies outside the verified IRM bounds")
    utilization = mul_div(borrow, WAD, supply) if supply else 0
    normalizer = (
        WAD - TARGET_UTILIZATION if utilization > TARGET_UTILIZATION else TARGET_UTILIZATION
    )
    error = div_to_zero(signed((utilization - TARGET_UTILIZATION) * WAD), normalizer)
    speed = div_to_zero(signed((50 * WAD // YEAR) * error), WAD)
    average, end = average_target(target, signed(speed * elapsed))
    coefficient = 75 * 10**16 if error < 0 else 3 * WAD
    curve = signed(div_to_zero(signed(coefficient * error), WAD) + WAD)
    return uint(div_to_zero(signed(curve * average), WAD)), end
