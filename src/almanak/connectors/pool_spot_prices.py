"""Spot prices of Aerodrome classic and Curve pools from their own state.

The one copy of these formulas: strategy price series read them from raw state values
and `ax pool` (`connectors/resolved_pool_prices.py`) from its resolved observations.
Every price is in whole units, fees excluded, and a pool without liquidity on a priced
side has no price (None).

- Aerodrome: the marginal price of the stored reserves its swaps price against, the
  reserve ratio for a volatile pool and the x³y + y³x invariant's for a stable one.
- Curve StableSwap: the invariant D's partial derivatives, as the pool's `get_p` does.
- Curve crypto pools: the pool's own `last_prices`, each pricing a coin in coin 0.
"""

from fractions import Fraction
from math import prod

from almanak.connectors.curve import stable_math
from almanak.connectors.curve.stable_math import AMPLIFICATION_PRECISION

CRYPTO_PRECISION = 10**18


def aerodrome_spot(
    stable: bool, reserve0: int, reserve1: int, decimals0: int, decimals1: int
) -> Fraction | None:
    """Whole token1 per whole token0 from the stored reserves."""
    if not reserve0 or not reserve1:
        return None
    x = Fraction(reserve0, 10**decimals0)
    y = Fraction(reserve1, 10**decimals1)
    if stable:
        # x³y + xy³ = k, so -dy/dx = (3x²y + y³) / (x³ + 3xy²).
        return (3 * x**2 * y + y**3) / (x**3 + 3 * x * y**2)
    return y / x


def stable_spot(
    balances: tuple[int, ...],
    rates: tuple[int, ...],
    decimals: tuple[int, ...],
    amplification: int,
    revision: stable_math.StableRevision,
) -> tuple[Fraction, ...] | None:
    """Whole coin 0 per whole coin i, for each coin after coin 0."""
    values = stable_math.normalize(balances, rates)
    if not all(values):
        return None
    count = len(values)
    invariant = stable_math.invariant(values, amplification, revision=revision)
    leverage = Fraction(amplification * count, AMPLIFICATION_PRECISION) * values[0]
    reduced = Fraction(invariant ** (count + 1), count**count * prod(values))
    # The derivative prices normalized units; a whole coin is 10**decimals * rate of them.
    whole = [10**places * rate for places, rate in zip(decimals, rates, strict=True)]
    return tuple(
        (leverage + reduced * Fraction(values[0], value))
        / (leverage + reduced)
        * Fraction(size, whole[0])
        for value, size in zip(values[1:], whole[1:], strict=True)
    )


def crypto_spot(last_prices: tuple[int, ...]) -> tuple[Fraction, ...] | None:
    """Whole coin 0 per whole coin i as the pool itself last traded, for each later coin.

    TwoCrypto stores one price, TriCrypto one for each coin after coin 0.
    """
    if not all(last_prices):
        return None
    return tuple(Fraction(value, CRYPTO_PRECISION) for value in last_prices)
