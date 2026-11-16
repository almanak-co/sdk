"""TwoCrypto NG v2.1.0 integer invariant and output-balance calculations.

Compared with CurveCryptoMathOptimized2 at official revision
65dd026f198b3a1b6d020d6b2407ff8a1e4f7360. Protocol-native quantities remain
inside this connector. These kernels alone do not establish lifecycle support.
"""

from math import isqrt

from almanak.connectors.curve.crypto_integer import (
    PRECISION,
    cubic_root,
    divide,
    unsigned,
)


def parameters(amplification, gamma):
    unsigned(amplification)
    unsigned(gamma)
    if not 4000 <= amplification <= 40000000 or not 10**10 <= gamma <= 199 * 10**15:
        raise ValueError("TwoCrypto amplification or gamma is outside the revision's domain")


def balances(values):
    if len(values) != 2:
        raise ValueError("TwoCrypto requires exactly two normalized balances")
    for value in values:
        unsigned(value)
    if min(values) == 0:
        raise ValueError("TwoCrypto requires funded reserves")


def balance_domain(amplification, gamma, values, invariant, index):
    parameters(amplification, gamma)
    balances(values)
    unsigned(invariant)
    if type(index) is not int or index not in (0, 1):
        raise ValueError("TwoCrypto coin index is out of range")
    if not 10**17 <= invariant <= 10**33:
        raise ValueError("TwoCrypto invariant is outside the revision's domain")
    limit = 100 * PRECISION
    if gamma > 2 * 10**16:
        limit = limit * (2 * 10**16) // gamma
    ratio = unsigned(2 * PRECISION * values[1 - index]) // invariant
    if not 10**36 // limit <= ratio <= limit:
        raise ValueError("TwoCrypto known balance is outside the safe invariant ratio")
    return limit


def output_domain(value, invariant, limit):
    ratio = unsigned(value * PRECISION) // invariant
    if not (10**36 // 2) // limit <= ratio <= limit // 2:
        raise ValueError("TwoCrypto output balance is outside the safe invariant ratio")
    return value


def newton_balance_step(amplification, gamma, other, invariant, current):
    ratio = 2 * PRECISION * other // invariant * current * 2 // invariant
    total = other + current
    gap = abs(gamma + PRECISION - ratio) + 1
    first = PRECISION * invariant // gamma * gap // gamma * gap * 10000 // amplification
    second = PRECISION + 2 * PRECISION * ratio // gap
    derivative = PRECISION * current + total * second + first
    decrement = invariant * second
    if derivative < decrement:
        return current // 2
    derivative -= decrement
    slope = derivative // current
    negative = first // slope
    positive = (derivative + PRECISION * invariant) // slope + negative * PRECISION // ratio
    negative += PRECISION * total // slope
    return current // 2 if positive < negative else unsigned(positive - negative)


def newton_balance(amplification, gamma, values, invariant, index):
    limit = balance_domain(amplification, gamma, values, invariant, index)
    other = values[1 - index]
    current = unsigned(invariant * invariant) // unsigned(other * 4)
    tolerance = max(other // 10**14, invariant // 10**14, 100)
    for _ in range(255):
        previous = current
        current = newton_balance_step(amplification, gamma, other, invariant, current)
        if abs(current - previous) < max(tolerance, current // 10**14):
            return output_domain(current, invariant, limit)
    raise ValueError("TwoCrypto output balance did not converge")


def balance_coefficients(amplification, gamma, other, invariant):
    amplified_gamma = amplification * gamma * gamma
    a = 10**32
    b = (
        divide(divide(invariant * amplified_gamma, 400000000), other)
        - 3 * 10**32
        - 2 * gamma * 10**14
    )
    c = (
        3 * 10**32
        + 4 * gamma * 10**14
        + divide(gamma * gamma, 10000)
        + divide(divide(4 * amplified_gamma, 400000000) * other, invariant)
        - divide(4 * amplified_gamma, 400000000)
    )
    d = -divide((PRECISION + gamma) ** 2, 10000)
    return a, b, c, d


def solve_balance(amplification, gamma, values, invariant, index):
    limit = balance_domain(amplification, gamma, values, invariant, index)
    other = values[1 - index]
    root = cubic_root(*balance_coefficients(amplification, gamma, other, invariant))
    if root is None:
        return newton_balance(amplification, gamma, values, invariant, index), 0
    unsigned(root)
    result = unsigned(divide(divide(divide(invariant * invariant, other) * root, 4), PRECISION))
    return output_domain(result, invariant, limit), root


def invariant_step(amplification, gamma, values, current):
    total = sum(values)
    ratio = 4 * PRECISION * values[0] // current * values[1] // current
    gap = abs(gamma + PRECISION - ratio) + 1
    first = PRECISION * current // gamma * gap // gamma * gap * 10000 // amplification
    second = 4 * PRECISION * ratio // gap
    slope = unsigned(
        total + total * second // PRECISION + first * 2 // ratio - second * current // PRECISION
    )
    positive = current * (slope + total) // slope
    negative = current * current // slope
    correction = current * (first // slope) // PRECISION * abs(PRECISION - ratio) // ratio
    negative = unsigned(negative + correction if PRECISION > ratio else negative - correction)
    return unsigned(positive - negative if positive > negative else (negative - positive) // 2)


def invariant(amplification, gamma, values, previous_ratio=0):
    parameters(amplification, gamma)
    balances(values)
    unsigned(previous_ratio)
    ordered = tuple(sorted(values, reverse=True))
    if not 10**9 <= ordered[0] <= 10**33 or ordered[1] * PRECISION // ordered[0] < 10**14:
        raise ValueError("TwoCrypto input balances are outside the revision's domain")
    product = unsigned(ordered[0] * ordered[1])
    current = (
        2 * isqrt(product)
        if previous_ratio == 0
        else min(sum(ordered), isqrt(unsigned(4 * product // previous_ratio * PRECISION)))
    )
    for _ in range(255):
        previous = current
        current = invariant_step(amplification, gamma, ordered, current)
        if abs(current - previous) * 10**14 < max(10**16, current):
            for value in ordered:
                ratio = value * PRECISION // current
                if not 10**16 // 2 <= ratio <= 10**20 // 2:
                    raise ValueError("TwoCrypto invariant produces an unsafe balance ratio")
            return current
    raise ValueError("TwoCrypto invariant did not converge")


def dynamic_fee(values, packed):
    balances(values)
    unsigned(packed)
    mid, out, gamma = ((packed >> shift) & (2**64 - 1) for shift in (128, 64, 0))
    if not 0 < gamma <= PRECISION or not 0 <= mid <= out <= 10**10:
        raise ValueError("TwoCrypto packed fees are outside the supported domain")
    total = sum(values)
    ratio = 4 * PRECISION * values[0] // total * values[1] // total
    weight = gamma * PRECISION // (gamma + PRECISION - ratio)
    return (mid * weight + out * (PRECISION - weight)) // PRECISION
