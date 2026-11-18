"""How each kind of pool's price series reads its state and its trades.

A resolved pool names its `pricing`. Its state at a boundary block is one or more
`eth_call` reads; decoded, they value each coin in coin 0 (whole units), and the
declared asset's price in its quote is the ratio of the two values. Trade events in a
log window are kept as compact records, `(block, log index, *fields)`, whose fields
give each trade's price and quote-token amount.

- `sqrt_price` (Uniswap V3, PancakeSwap V3, Aerodrome Slipstream, Uniswap V4): `slot0`
  or `StateView.getSlot0`. A Swap event holds the signed amounts and resulting
  sqrtPriceX96.
- `aerodrome_volatile` and `aerodrome_stable`: `getReserves`, priced by the reserves'
  spot price. A `Sync` event holds the reserves after each trade (and liquidity
  change), so its spot price; a `Swap` event holds the amounts in and out.
- `curve_stableswap`: `get_balances` and `A_precise`, priced by the invariant's
  marginal price. Rates follow coin decimals: pool resolution refuses other rates.
- `curve_twocrypto` and `curve_tricrypto`: the pool's own `last_prices`.
- Every Curve pool: a `TokenExchange` event holds the sold and bought amounts, whose
  ratio is the trade's execution price, fees included. Trades between other coins of
  the pool are not the declared pair's and are ignored.
"""

from fractions import Fraction
from typing import Literal

from eth_utils import keccak

from almanak.connectors.pool_spot_prices import aerodrome_spot, crypto_spot, stable_spot


def selector(signature: str) -> str:
    return "0x" + keccak(text=signature)[:4].hex()


def topic(signature: str) -> str:
    return "0x" + keccak(text=signature).hex()


SLOT0 = selector("slot0()")
GET_SLOT0 = selector("getSlot0(bytes32)")
GET_RESERVES = selector("getReserves()")
GET_BALANCES = selector("get_balances()")
A_PRECISE = selector("A_precise()")
LAST_PRICES = selector("last_prices()")
LAST_PRICES_AT = selector("last_prices(uint256)")
V3_SWAP = topic("Swap(address,address,int256,int256,uint160,uint128,int24)")
PANCAKE_SWAP = topic("Swap(address,address,int256,int256,uint160,uint128,int24,uint128,uint128)")
V4_SWAP = topic("Swap(bytes32,address,int128,int128,uint160,uint128,int24,uint24)")
SYNC = topic("Sync(uint256,uint256)")
AERODROME_SWAP = topic("Swap(address,address,uint256,uint256,uint256,uint256)")
STABLE_EXCHANGE = topic("TokenExchange(address,int128,uint256,int128,uint256)")
CRYPTO_EXCHANGE = topic("TokenExchange(address,uint256,uint256,uint256,uint256,uint256,uint256)")

Pricing = Literal[
    "sqrt_price",
    "aerodrome_volatile",
    "aerodrome_stable",
    "curve_stableswap",
    "curve_twocrypto",
    "curve_tricrypto",
]
SQRT_SWAPS = (V3_SWAP, PANCAKE_SWAP, V4_SWAP)
CURVE_EXCHANGES = (STABLE_EXCHANGE, CRYPTO_EXCHANGE)
# The leading data words each event's compact record keeps, and whether each is signed.
LAYOUTS: dict[str, tuple[bool, ...]] = {
    **dict.fromkeys(SQRT_SWAPS, (True, True, False)),
    SYNC: (False, False),
    AERODROME_SWAP: (False, False, False, False),
    STABLE_EXCHANGE: (True, False, True, False),
    CRYPTO_EXCHANGE: (False, False, False, False),
}
# How many coins a Curve pool may hold; exchange events name coins by index.
MAX_COINS = 8


def word(value: int) -> str:
    return value.to_bytes(32, "big").hex()


def state_calls(pool) -> tuple[tuple[str, str], ...]:
    """The `(contract, calldata)` reads that give the pool's state at a block."""
    pricing = pool.pricing
    if pricing == "sqrt_price":
        v4 = len(pool.pool) == 66
        return ((pool.state, GET_SLOT0 + pool.pool[2:] if v4 else SLOT0),)
    if pricing.startswith("aerodrome"):
        return ((pool.state, GET_RESERVES),)
    if pricing == "curve_stableswap":
        return ((pool.state, GET_BALANCES), (pool.state, A_PRECISE))
    if pricing == "curve_twocrypto":
        return ((pool.state, LAST_PRICES),)
    return tuple((pool.state, LAST_PRICES_AT + word(k)) for k in range(len(pool.assets) - 1))


def words(result: str) -> list[int]:
    data = bytes.fromhex(result[2:])
    return [int.from_bytes(data[i : i + 32], "big") for i in range(0, len(data) - 31, 32)]


def state_values(pool, results: tuple[str, ...]) -> tuple[int, ...]:
    """The raw state the reads returned: sqrtPriceX96; reserves; balances and A; or
    last prices."""
    decoded = [words(result) for result in results]
    if any(not values for values in decoded):
        raise ValueError("pool price read returned no price word")
    pricing = pool.pricing
    if pricing == "sqrt_price":
        if decoded[0][0] >= 2**160:
            raise ValueError("pool price read returned an invalid sqrt price")
        return (decoded[0][0],)
    if pricing.startswith("aerodrome"):
        return tuple(decoded[0][:2])
    if pricing == "curve_stableswap":
        # A dynamic array: offset, length, then the items.
        balances = decoded[0][2:]
        if decoded[0][:2] != [32, len(pool.assets)] or len(balances) != len(pool.assets):
            raise ValueError("Curve balances read differs from the pool's coins")
        return (*balances, decoded[1][0])
    return tuple(values[0] for values in decoded)


def coin_values(pool, state: tuple[int, ...]) -> tuple[Fraction, ...]:
    """Each coin's value in whole coin 0 per whole coin, from the pool's raw state."""
    decimals = tuple(asset.decimals for asset in pool.assets)
    pricing = pool.pricing
    later: tuple[Fraction, ...] | None
    if pricing == "sqrt_price":
        ratio = Fraction(state[0] * state[0], 2**192) * Fraction(10) ** (decimals[0] - decimals[1])
        later = (1 / ratio,) if ratio else None
    elif pricing.startswith("aerodrome"):
        spot = aerodrome_spot(pricing == "aerodrome_stable", *state, *decimals)
        later = None if spot is None else (1 / spot,)
    elif pricing == "curve_stableswap":
        rates = tuple(10 ** (36 - places) for places in decimals)
        later = stable_spot(state[:-1], rates, decimals, state[-1], pool.revision)
    else:
        later = crypto_spot(state)
    if later is None or len(later) != len(pool.assets) - 1:
        raise ValueError("the pool has no price at this block")
    return (Fraction(1), *later)


def event_fields(event: str, data: bytes) -> tuple[int, ...]:
    """The fields a compact record keeps from one event log's data."""
    layout = LAYOUTS[event]
    if len(data) < 32 * len(layout):
        raise ValueError("swap log data lacks its event fields")
    fields = tuple(
        int.from_bytes(data[32 * i : 32 * i + 32], "big", signed=signed)
        for i, signed in enumerate(layout)
    )
    if event in SQRT_SWAPS and not 0 < fields[2] < 2**160:
        raise ValueError("swap log carries an invalid sqrt price")
    if event in CURVE_EXCHANGES and not (0 <= fields[0] < MAX_COINS and 0 <= fields[2] < MAX_COINS):
        raise ValueError("Curve exchange log names an invalid coin")
    return fields


def trade(pool, record, pair: tuple[int, int]) -> tuple[Fraction | None, int | None]:
    """One record's price of the asset in the quote (whole units) and its quote-token
    amount (raw), each None when the record carries none for the declared pair."""
    fields = tuple(record[2:])
    asset, quote = pair
    pricing = pool.pricing
    if pricing == "sqrt_price" and len(fields) == 3:
        values = coin_values(pool, fields[2:])
        return values[asset] / values[quote], abs(fields[quote])
    if pricing.startswith("aerodrome") and len(fields) == 2:
        if not all(fields):
            return None, None
        values = coin_values(pool, fields)
        return values[asset] / values[quote], None
    if pricing.startswith("aerodrome") and len(fields) == 4:
        # One side is zero in an ordinary swap; a swap moving the quote both ways (a
        # flash-style borrow and repay) traded the larger amount, not their sum or net.
        return None, max(fields[quote], fields[2 + quote])
    if pricing.startswith("curve") and len(fields) == 4:
        return exchange(pool, fields, pair)
    raise ValueError("swap evidence differs from its pool's events")


def exchange(pool, fields, pair) -> tuple[Fraction | None, int | None]:
    sold, sold_amount, bought, bought_amount = fields
    asset, quote = pair
    if {sold, bought} != {asset, quote}:
        return None, None
    asset_amount, quote_amount = (
        (sold_amount, bought_amount) if sold == asset else (bought_amount, sold_amount)
    )
    if not asset_amount or not quote_amount:
        return None, quote_amount
    decimals = (pool.assets[asset].decimals, pool.assets[quote].decimals)
    price = Fraction(quote_amount, 10 ** decimals[1]) / Fraction(asset_amount, 10 ** decimals[0])
    return price, quote_amount


def pool_facts(connector: str, resolved, reader, block) -> dict:
    """The PricePool fields a connector's resolution decides: coins, pricing, events."""
    if connector == "aerodrome":
        return {
            "assets": resolved.currencies,
            "pricing": "aerodrome_stable" if resolved.pool.stable else "aerodrome_volatile",
            "swap_topics": ((SYNC, AERODROME_SWAP),),
        }
    if connector != "curve":
        return {"assets": (resolved.asset0, resolved.asset1), "pricing": "sqrt_price"}
    family = resolved.pool.family
    if family != "stableswap_ng":
        return {
            "assets": resolved.coins,
            "pricing": "curve_twocrypto" if family == "twocrypto_ng" else "curve_tricrypto",
            "swap_topics": (CRYPTO_EXCHANGE,),
        }
    require_standard_rates(reader, resolved, block)
    return {
        "assets": resolved.coins,
        "pricing": "curve_stableswap",
        "revision": resolved.revision,
        "swap_topics": (STABLE_EXCHANGE,),
    }


def require_standard_rates(reader, resolved, block) -> None:
    """The connector reads a StableSwap pool only with rates set by coin decimals."""
    from almanak.gateway.ethereum import DataError

    rates = reader.call(resolved.pool.pool, "stored_rates()", block, outputs=("uint256[]",))[0]
    if tuple(rates) != tuple(10 ** (36 - coin.decimals) for coin in resolved.coins):
        raise DataError(
            "asset_units", "Curve standard-coin rates differ from immutable token precision"
        )
