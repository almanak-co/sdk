"""Integer price and EMA calculations for the verified TwoCrypto math revision.

The exponential follows the revision's Remco Bloemen rational approximation,
including signed division and the final uint256 multiplication.
"""

from almanak.connectors.curve.crypto_integer import PRECISION, unsigned
from almanak.connectors.curve.crypto_prices import (
    exponential as exponential,
)
from almanak.connectors.curve.crypto_prices import (
    exponential_polynomials as exponential_polynomials,
)
from almanak.connectors.curve.crypto_prices import (
    oracle as oracle,
)
from almanak.connectors.curve.twocrypto_math import balances, parameters


def marginal_price(values, invariant, amplification, gamma):
    balances(values)
    parameters(amplification, gamma)
    if not 10**17 <= unsigned(invariant) <= 10**33:
        raise ValueError("TwoCrypto marginal price requires a supported invariant")
    product = unsigned(unsigned(4 * values[0]) * values[1]) // invariant
    ratio = unsigned(product * 10**36) // invariant
    first = unsigned(unsigned(2 * ratio) * ratio) // 10**36
    first = unsigned(first * ratio) // 10**36
    last = (ratio * ratio) % 2**256 // 10**36
    last = unsigned(last * (2 * gamma + 3 * PRECISION)) // PRECISION
    gradient = unsigned(first + (gamma + PRECISION) ** 2 - last)
    leverage = (amplification * gamma**2) % 2**256 // 10000
    terms = tuple(
        unsigned(unsigned(leverage * value) // invariant * ratio) // 10**36 for value in values
    )
    numerator = unsigned(values[0] * unsigned(gradient + terms[1])) // values[1]
    return unsigned(numerator * PRECISION) // unsigned(gradient + terms[0])
