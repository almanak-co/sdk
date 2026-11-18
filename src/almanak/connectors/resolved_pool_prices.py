"""Prices for the Aerodrome classic and Curve pools that `ax pool` resolves.

Every price is whole units of `quote` per whole unit of `base` at the pinned block,
fees excluded, and names its `kind`:

- `spot`: the marginal price of the pool's own invariant at its current state, the
  same meaning as the V3-style pool price. Aerodrome uses the stored reserves its
  swaps price against; Curve StableSwap uses the invariant D, as its `get_p` does.
- `last_spot` and `oracle`: a Curve crypto pool's own `last_prices`, its marginal
  price after the last swap or liquidity change (not an execution price), and
  `price_oracle` (an EMA), each pricing a coin in coin 0.

A pool without liquidity on a priced side has no price. An Aerodrome report also carries
each currency's stored reserve, its depth, in base units
(`amount`, with `decimals`) and whole units (`quantity`). A Curve report also carries
the pool's balances, LP supply, fee, amplification and virtual price
(`connectors/curve/pool_report.py`).
"""

from fractions import Fraction

from almanak.connectors.pool_spot_prices import CRYPTO_PRECISION, aerodrome_spot, stable_spot
from almanak.report_values import exact_decimal


def entry(base, quote, kind: str, value: Fraction) -> dict:
    return {
        "base": str(base.identifier),
        "quote": str(quote.identifier),
        "kind": kind,
        "numerator": str(value.numerator),
        "denominator": str(value.denominator),
    }


def aerodrome_price(observation) -> dict | None:
    """Currency1 per whole currency0 from the stored reserves."""
    value = aerodrome_spot(
        observation.source.stable,
        observation.reserve0,
        observation.reserve1,
        observation.currency0.decimals,
        observation.currency1.decimals,
    )
    if value is None:
        return None
    return entry(observation.currency0, observation.currency1, "spot", value)


def stable_prices(observation) -> list[dict] | None:
    """Coin 0 per whole coin i from the StableSwap invariant's partial derivatives."""
    state = observation.math_state()
    values = stable_spot(
        state.balances,
        state.rates,
        tuple(coin.decimals for coin in observation.currencies),
        state.amplification,
        state.revision,
    )
    if values is None:
        return None
    first = observation.currencies[0]
    return [
        entry(coin, first, "spot", value)
        for coin, value in zip(observation.currencies[1:], values, strict=True)
    ]


def crypto_prices(observation) -> list[dict]:
    """Coin 0 per whole coin i as the pool last priced and averages it."""
    # TwoCrypto stores one price per value; TriCrypto one for each coin after coin 0.
    last, oracle = observation.last_prices, observation.price_oracle
    if isinstance(last, int):
        last, oracle = (last,), (oracle,)
    pairs = zip(last, oracle, strict=True)
    first = observation.currencies[0]
    return [
        entry(coin, first, kind, Fraction(value, CRYPTO_PRECISION))
        for coin, (spot, averaged) in zip(observation.currencies[1:], pairs, strict=True)
        for kind, value in (("last_spot", spot), ("oracle", averaged))
    ]


def curve_prices(observation) -> list[dict] | None:
    if observation.source.family in ("twocrypto_ng", "tricrypto_ng"):
        return crypto_prices(observation)
    return stable_prices(observation)


def aerodrome_reserves(observation) -> list[dict]:
    """Each currency's stored reserve, the classic pool's depth, in base units and whole units."""
    return [
        {
            "asset": str(currency.identifier),
            "amount": str(reserve),
            "decimals": currency.decimals,
            "quantity": exact_decimal(reserve, currency.decimals),
        }
        for currency, reserve in (
            (observation.currency0, observation.reserve0),
            (observation.currency1, observation.reserve1),
        )
    ]


def aerodrome_prices(observation) -> dict:
    return {"price": aerodrome_price(observation), "reserves": aerodrome_reserves(observation)}


def curve_report(observation) -> dict:
    from almanak.connectors.curve.pool_report import pool_state

    return {"prices": curve_prices(observation), "pool_state": pool_state(observation)}
