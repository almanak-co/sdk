"""How `almanak ax` discovers each connector's markets, as the connector declares it.

A connector package that `ax` can discover holds a `discovery.py` with a `POOL`,
`LENDING` or `PERP` declaration; `ax` builds its connector tables and CLI choices from
these instead of keeping its own lists. Every other installed connector is named in `NOT_DISCOVERED`
with the reason, and a test holds the two together, so adding a connector without
deciding its discovery fails.
"""

from collections.abc import Callable
from dataclasses import dataclass
from functools import cache
from importlib import import_module
from typing import Literal

# concentrated: V3-style pool state, spot price and depth (`read_pool_state`).
# resolved: the connector's own pool resolution and prices (`read_resolved_pool`).
# keyed: a Uniswap V4 pool, named by its key, not a contract address.
PoolFamily = Literal["concentrated", "resolved", "keyed"]


@dataclass(frozen=True)
class PoolDiscovery:
    family: PoolFamily
    # Prefixes of the CoinGecko on-chain DEX labels its pools carry. A label only
    # selects which resolver to try; the resolver decides what the pool is.
    vendor_labels: tuple[str, ...]
    # Its pools answer `factory()`, so a label the SDK does not know can be matched
    # against the connector's reviewed factory.
    answers_factory: bool = False
    # The resolution summary field listing the pool's assets, which `ax` compares
    # with the requested pair. A keyed pool's assets are its key's currencies.
    currencies: str = "currencies"
    # A resolved pool's price function, as "module:function", imported only when a
    # price is read; it returns the report's `price` or `prices` entry.
    prices: str | None = None
    # A resolved pool's funding read, as "module:function" taking the reader, the
    # resolved pool and the block: the report of an unfunded pool, or None when funded.
    funding: str | None = None

    def price_reader(self) -> Callable:
        return declared_function(self.prices, "this pool family reads no declared price function")

    def unfunded_report(self, reader, resolved, block) -> dict | None:
        if self.funding is None:
            return None
        return declared_function(self.funding, "")(reader, resolved, block)


def declared_function(spec: str | None, missing: str) -> Callable:
    module, name = (spec or "").partition(":")[::2]
    if not module or not name:
        raise ValueError(missing)
    return getattr(import_module(module), name)


# pool_reserves: the reviewed Pool's reserve list (`networks.pool_for`).
# hub_spokes: the reserves of each Spoke the chain's Hubs serve.
# comet_deployments: the Comets the chain's Configurator deployed.
# morpho_markets: Morpho's API where it indexes the chain, else the core's events.
LendingListing = Literal["pool_reserves", "hub_spokes", "comet_deployments", "morpho_markets"]


@dataclass(frozen=True)
class LendingDiscovery:
    # The protocol record `ax lending-markets` lists the connector's markets from.
    listing: LendingListing


@dataclass(frozen=True)
class PerpDiscovery:
    """The connector's perp markets are found with `ax perp-markets`, by a market
    address or a label like BTC/USD, and its positions read with `ax perp-position`."""


# Installed connectors whose markets `ax` does not discover, and why.
NOT_DISCOVERED: dict[str, str] = {
    "hyperliquid": "HyperCore markets are venue lookups, not chain discovery: "
    "`ax hypercore-markets` lists them from `/info` (blueprint 30, section 6.1).",
}


def declaration(connector: str):
    """The connector's `discovery` module, or None when it declares none."""
    name = f"almanak.connectors.{connector}.discovery"
    try:
        return import_module(name)
    except ModuleNotFoundError as error:
        if error.name != name:
            raise
        return None


def declared(attribute: str) -> dict:
    """Every installed connector's declaration of one kind, in installation order."""
    from almanak.connectors.composition import CONNECTOR_IDS

    found = {}
    for key in CONNECTOR_IDS:
        module = declaration(str(key))
        if module is not None and hasattr(module, attribute):
            found[str(key)] = getattr(module, attribute)
    return found


@cache
def pool_discovery() -> dict[str, PoolDiscovery]:
    return declared("POOL")


@cache
def lending_discovery() -> dict[str, LendingDiscovery]:
    return declared("LENDING")


@cache
def perp_discovery() -> dict[str, PerpDiscovery]:
    return declared("PERP")


def pool_connectors(*families: PoolFamily) -> tuple[str, ...]:
    return tuple(c for c, found in pool_discovery().items() if found.family in families)
