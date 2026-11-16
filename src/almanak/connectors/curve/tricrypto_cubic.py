"""TriCrypto v2.0.0 cubic solver, preserving signed division and scaling order.

The compared Math revision is 6bfe070cca8788ce438dcdf15d9fe1bcea74cba6.
Its scaling and root convention differ from the TwoCrypto solver.
"""

from math import isqrt

from almanak.connectors.curve.crypto_integer import (
    PRECISION,
    cube_root,
    divide,
    signed,
)


def coefficients(amplification, gamma, first, second, invariant):
    gamma_squared = gamma * gamma
    a = 10**36 // 27
    b = (
        10**36 // 9
        + divide(2 * PRECISION * gamma, 27)
        - divide(
            divide(
                divide(divide(invariant * invariant, first) * gamma_squared * amplification, 27**2),
                10000,
            ),
            second,
        )
    )
    c = (
        10**36 // 9
        + divide(gamma * (gamma + 4 * PRECISION), 27)
        + divide(
            divide(
                divide(gamma_squared * (first + second - invariant), invariant) * amplification, 27
            ),
            10000,
        )
    )
    d = divide((PRECISION + gamma) ** 2, 27)
    return a, b, c, d


def scaled_coefficients(a, b, c, d):
    magnitude = abs(divide(3 * a * c, b) - b)
    divisor = 1
    for exponent in (48, 44, 40, 36, 32, 28, 24, 20):
        if magnitude > 10**exponent:
            divisor = 10 ** (exponent - 18)
            break
    if abs(a) > abs(b):
        extra = abs(divide(a, b))
        return tuple(divide(value * extra, divisor) for value in (a, b, c, d))
    extra = abs(divide(b, a))
    return tuple(divide(divide(value, extra), divisor) for value in (a, b, c, d))


def root(a, b, c, d):
    a, b, c, d = scaled_coefficients(a, b, c, d)
    product = 3 * a * c
    first = signed(divide(product, b) - b)
    second = signed(divide(3 * product, b) - 2 * b - divide(divide(27 * a * a, b) * d, b))
    radicand = signed(second * second + divide(4 * first * first, b) * first)
    if radicand <= 0:
        return None
    square = isqrt(radicand)
    first_root = cube_root(abs(b)) * (1 if b >= 0 else -1)
    second_root = (
        cube_root((second + square) // 2) if second > 0 else -cube_root((square - second) // 2)
    )
    combined = divide(divide(first_root * first_root, PRECISION) * second_root, PRECISION)
    return divide(b + divide(b * first, combined) - combined, 3), a
