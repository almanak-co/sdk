"""Read-only pool reads for `ax pool`.

An `ax pool` request names only a chain and a pool address. The connector's own
resolver derives the pool's identities and refuses a pool created by any factory
other than the reviewed deployment, so an unverified pool is refused rather than
reported. The V3-style concentrated-liquidity connectors also report the pinned
pool state, spot price and optional depth; Aerodrome classic and Curve pools
report their resolution and prices (`resolved_pool_prices`), without depth. A Curve
pool also reports its balances, LP supply, fee, amplification and virtual price, and
an unfunded one is reported as `unfunded_pool` (`connectors/curve/pool_report.py`).
"""

from collections.abc import Callable
from dataclasses import dataclass
from importlib import import_module
from math import isqrt
from types import ModuleType

from pydantic import model_validator

from almanak.caip import ChainId
from almanak.connectors.concentrated_math import amount_delta, sqrt_at_tick, tick_at_price
from almanak.connectors.discovery import pool_connectors, pool_discovery
from almanak.gateway.ethereum import DataError
from almanak.models import ZERO_ADDRESS, Address, Block, Model

# Depth reads stay bounded: a band wider than half the price, or one holding
# more initialized ticks than the limit, is refused instead of truncated,
# because a partial walk would understate depth. At tick spacing 1 the widest
# band spans 45 bitmap words, so the word limit only guards the arithmetic.
MAX_DEPTH_BPS = 5_000
MAX_BITMAP_WORDS = 64
MAX_INITIALIZED_TICKS = 2_048
MAX_TICK = 887272


class PoolRequest(Model):
    chain: ChainId
    pool: Address

    @model_validator(mode="after")
    def scope(self):
        if self.chain.namespace != "eip155" or self.pool == ZERO_ADDRESS:
            raise ValueError("pool request requires an EVM chain and a nonzero pool address")
        return self


POOL_CONNECTORS = pool_connectors("concentrated")
RESOLVED_POOL_CONNECTORS = pool_connectors("resolved")


@dataclass(frozen=True)
class Family:
    resolution: ModuleType
    support: ModuleType
    read_pool: Callable
    read_tick: Callable
    price_request: Callable


def family(connector: str) -> Family:
    if connector not in POOL_CONNECTORS:
        raise ValueError(f"unsupported pool connector '{connector}'")
    package = f"almanak.connectors.{connector}"
    reader = import_module(f"{package}.reader")
    # Slipstream tick records carry staking fields, so it owns its tick reader;
    # the Uniswap and PancakeSwap V3 records share the prepared-acquisition layout.
    read_tick = reader.read_tick if connector == "aerodrome_slipstream" else prepared_read_tick()
    return Family(
        resolution=import_module(f"{package}.resolution"),
        support=import_module(f"{package}.support"),
        read_pool=reader.read_pool,
        read_tick=read_tick,
        price_request=import_module(f"{package}.domain").PoolPriceRequest,
    )


def prepared_read_tick() -> Callable:
    from almanak.connectors.v3_prepared_acquisition import read_tick

    return read_tick


def support_level(connector: str, chain: ChainId) -> str:
    """Refuse an undeclared chain before any transport opens; name what is declared."""
    support = support_tables(connector)
    if chain in {entry.chain for entry in support.verified_support()}:
        return "verified"
    declared = {entry.chain for entry in support.candidate_support()}
    if chain in declared:
        return "candidate"
    listed = ", ".join(sorted(str(value) for value in declared))
    raise ValueError(f"{connector} declares no support on {chain}; declared chains: {listed}")


def read_pool_state(
    reader, connector: str, request: PoolRequest, block: Block, depth_bps: int | None = None
) -> dict:
    if request.chain != reader.chain.chain_id:
        raise ValueError("request chain differs from the reader chain")
    level = support_level(connector, request.chain)
    selected = family(connector)
    selection = selected.resolution.MarketSelection(
        connector=connector, chain=request.chain, pool=request.pool
    )
    resolved = selected.resolution.resolve_market(reader, selection, block)
    observation = selected.read_pool(reader, resolved.pool, block)
    result = {
        "product": "pool_state",
        "request": {**request.model_dump(mode="json"), "connector": connector},
        "support": level,
        "resolution": resolved.summary(),
        "price": spot_price(selected, observation),
        "observation": observation.model_dump(mode="json"),
    }
    if depth_bps is not None:
        result["depth"] = depth(reader, selected, observation, block, depth_bps)
    return result


def support_tables(connector: str) -> ModuleType:
    if connector not in pool_discovery():
        raise ValueError(f"unsupported pool connector '{connector}'")
    return import_module(f"almanak.connectors.{connector}.support")


def read_resolved_pool(reader, connector: str, request: PoolRequest, block: Block) -> dict:
    """A resolved-family pool (Aerodrome classic, Curve), verified by its connector's own
    resolver and priced by the function its discovery declaration names."""
    if connector not in RESOLVED_POOL_CONNECTORS:
        raise ValueError(f"unsupported pool connector '{connector}'")
    if request.chain != reader.chain.chain_id:
        raise ValueError("request chain differs from the reader chain")
    level = support_level(connector, request.chain)
    resolution = import_module(f"almanak.connectors.{connector}.resolution")
    selection = resolution.MarketSelection(
        connector=connector, chain=request.chain, pool=request.pool
    )
    resolved = resolution.resolve_market(reader, selection, block)
    report = {
        "product": "pool_resolution",
        "request": {**request.model_dump(mode="json"), "connector": connector},
        "support": level,
        "resolution": resolved.summary(),
    }
    found = pool_discovery()[connector]
    unfunded = found.unfunded_report(reader, resolved, block)
    if unfunded is not None:
        return {**report, **unfunded}
    observation = import_module(f"almanak.connectors.{connector}.reader").read_pool(
        reader, resolved.pool, block
    )
    return {**report, **found.price_reader()(observation)}


def spot_price(selected: Family, observation) -> dict | None:
    """Currency1 per whole currency0; absent when no liquidity is active or the pool is locked."""
    if not observation.liquidity or not observation.is_unlocked:
        return None
    base, quote = observation.currency0.identifier, observation.currency1.identifier
    request = selected.price_request(source=observation.source, base=base, quote=quote)
    price = observation.spot_price(request)
    return {
        "base": str(base),
        "quote": str(quote),
        "kind": "spot",
        "numerator": str(price.numerator),
        "denominator": str(price.denominator),
    }


def depth(reader, selected: Family, observation, block: Block, bps: int) -> dict:
    """Pool output available before the price moves `bps` in each direction, fees excluded.

    The band is relative to the pool's own price (currency1 per currency0). Falling
    to the lower bound, the pool pays out currency1; rising to the upper bound, it
    pays out currency0. Amounts are raw integer units of each currency.
    """
    from almanak.connectors.v3_prepared_acquisition import tick_state

    if not 0 < bps <= MAX_DEPTH_BPS:
        raise ValueError(f"depth band must be between 1 and {MAX_DEPTH_BPS} basis points")
    lower, upper = band(observation.sqrt_price_x96, bps)
    words, ticks = tick_state(
        reader,
        observation.source,
        block,
        tick_at_price(lower),
        min(tick_at_price(upper) + 1, MAX_TICK),
        MAX_BITMAP_WORDS,
        MAX_INITIALIZED_TICKS,
        tick_reader=selected.read_tick,
    )
    nets = {tick.index: tick.net for tick in ticks}
    return {
        "bps": bps,
        "lower_sqrt_price_x96": str(lower),
        "upper_sqrt_price_x96": str(upper),
        "to_lower": side(observation.currency1, walk_down(observation, lower, nets)),
        "to_upper": side(observation.currency0, walk_up(observation, upper, nets)),
        "bitmap_words": len(words),
        "initialized_ticks": len(ticks),
    }


def band(price: int, bps: int) -> tuple[int, int]:
    """Sqrt-price bounds for a +/- `bps` move in price; integer square roots round down."""
    lower = isqrt(price * price * (10_000 - bps) // 10_000)
    upper = isqrt(price * price * (10_000 + bps) // 10_000)
    return max(lower, sqrt_at_tick(-MAX_TICK)), min(upper, sqrt_at_tick(MAX_TICK))


def walk_down(observation, target: int, nets: dict[int, int]) -> int:
    """Currency1 paid out while the price falls to `target`, crossing ticks at or below."""
    price, liquidity, paid = observation.sqrt_price_x96, observation.liquidity, 0
    for index in sorted((tick for tick in nets if tick <= observation.tick), reverse=True):
        boundary = sqrt_at_tick(index)
        if boundary <= target:
            break
        paid += amount_delta(price, boundary, liquidity, False, up=False)
        price, liquidity = boundary, crossed(liquidity - nets[index])
    return paid + amount_delta(price, target, liquidity, False, up=False)


def walk_up(observation, target: int, nets: dict[int, int]) -> int:
    """Currency0 paid out while the price rises to `target`, crossing ticks above."""
    price, liquidity, paid = observation.sqrt_price_x96, observation.liquidity, 0
    for index in sorted(tick for tick in nets if tick > observation.tick):
        boundary = sqrt_at_tick(index)
        if boundary >= target:
            break
        paid += amount_delta(price, boundary, liquidity, True, up=False)
        price, liquidity = boundary, crossed(liquidity + nets[index])
    return paid + amount_delta(price, target, liquidity, True, up=False)


def crossed(liquidity: int) -> int:
    if not 0 <= liquidity < 2**128:
        raise DataError("inconsistent", "pool tick liquidity crosses outside uint128")
    return liquidity


def side(asset, amount: int) -> dict:
    return {"asset": str(asset.identifier), "amount": str(amount), "decimals": asset.decimals}
