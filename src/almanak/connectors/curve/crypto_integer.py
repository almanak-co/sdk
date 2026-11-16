"""Integer operations used by revision-bound Crypto NG economic calculations.

Signed division truncates toward zero, as in the EVM. Python floor division is
only suitable for the unsigned portions of these calculations.
"""

from math import isqrt

PRECISION = 10**18


def unsigned(value):
    if type(value) is not int or not 0 <= value < 2**256:
        raise ValueError("Crypto NG requires a uint256 quantity")
    return value


def signed(value):
    if type(value) is not int or not -(2**255) <= value < 2**255:
        raise ValueError("Crypto NG signed arithmetic overflow")
    return value


def divide(numerator, denominator):
    signed(numerator)
    signed(denominator)
    if denominator == 0:
        raise ValueError("Crypto NG division by zero")
    magnitude = abs(numerator) // abs(denominator)
    return signed(-magnitude if (numerator < 0) != (denominator < 0) else magnitude)


def cube_root(value):
    """Preserve the contract's scaled seven-step cube-root rounding."""
    unsigned(value)
    if value == 0:
        return 0
    boundary = 115792089237316195423570985008687907853269
    if value >= boundary * PRECISION:
        scaled, multiplier = value, 10**12
    elif value >= boundary:
        scaled, multiplier = value * PRECISION, 10**6
    else:
        scaled, multiplier = value * 10**36, 1
    unsigned(scaled)
    power = scaled.bit_length() - 1
    remainder = power % 3
    result = 2 ** (power // 3) * 1260**remainder // 1000**remainder
    for _ in range(7):
        result = (2 * result + scaled // (result * result)) // 3
    return unsigned(result * multiplier)


def cubic_scale(first, second, leading):
    threshold = min(abs(first), abs(second), leading)
    for exponent in (48, 46, 44, 42, 40, 38, 36, 34, 32, 30, 28, 26, 24, 20):
        if threshold > 10**exponent:
            return 10 ** (exponent - 18)
    return 1


def cubic_deltas(a, b, c, d):
    first = signed(divide(3 * a * c, b) - b)
    second = signed(3 * first + b - divide(divide(27 * a * a, b) * d, b))
    return first, second


def cubic_root(a, b, c, d):
    first, second = cubic_deltas(a, b, c, d)
    scale = cubic_scale(first, second, a)
    a, b, c, d = (divide(value, scale) for value in (a, b, c, d))
    first, second = cubic_deltas(a, b, c, d)
    radicand = signed(second * second + divide(4 * first * first, b) * first)
    if radicand <= 0:
        return None
    square = isqrt(radicand)
    first_root = cube_root(abs(b)) * (1 if b > 0 else -1)
    second_root = (
        cube_root((second + square) // 2) if second > 0 else -cube_root((square - second) // 2)
    )
    combined = divide(divide(first_root * first_root, PRECISION) * second_root, PRECISION)
    return divide(
        PRECISION * combined - PRECISION * b - divide(PRECISION * b, combined) * first,
        3 * a,
    )
