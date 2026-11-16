"""Config-ready lending reserve lookups for `almanak ax`.

A request is the market selection an author writes under `markets:`. The
connector's own resolver verifies the deployment and reserve at the pinned
block, and the lookup adds the price sources the protocol uses there, so an
author can state them without reading SDK code or making raw chain reads.
Resolution refuses an unreviewed deployment instead of reporting it.
"""

import json
from collections.abc import Callable
from dataclasses import dataclass
from importlib import import_module
from types import ModuleType

from almanak.connectors.lending_rates import UNOBSERVED, rates_entry
from almanak.connectors.lending_risk import (
    RiskQuery,
    comet_risk,
    morpho_risk,
    pool_risk,
    risk_entry,
    spoke_risk,
)
from almanak.gateway.ethereum import DataError
from almanak.models import Account, Block

LENDING_CONNECTORS = ("aave_v3", "sparklend", "aave_v4", "compound_v3", "morpho_blue")


@dataclass(frozen=True)
class Family:
    resolution: ModuleType
    reader: ModuleType
    sources: Callable
    observe: Callable
    position: Callable
    risk: Callable


def family(connector: str) -> Family:
    if connector not in LENDING_CONNECTORS:
        raise ValueError(f"unsupported lending connector '{connector}'")
    package = f"almanak.connectors.{connector}"
    sources, observe, position, risk = FAMILIES[connector]
    return Family(
        resolution=import_module(f"{package}.resolution"),
        reader=import_module(f"{package}.reader"),
        sources=sources,
        observe=observe,
        position=position,
        risk=risk,
    )


def selection(connector: str, values: dict):
    """Validate one market selection exactly as the author loader would."""
    named = values.get("connector", connector)
    if named != connector:
        raise ValueError(f"request names connector '{named}' but --connector is '{connector}'")
    model = family(connector).resolution.MarketSelection
    return model.model_validate_json(json.dumps({**values, "connector": connector}))


def resolve(reader, selected: Family, request, block: Block):
    if request.chain != reader.chain.chain_id:
        raise ValueError("request chain differs from the reader chain")
    return selected.resolution.resolve_market(reader, request, block)


def read_lending_reserve(reader, request, block: Block, query: RiskQuery | None = None) -> dict:
    selected = family(request.connector)
    resolved, sources, config = market_config(reader, selected, request, block)
    return {
        "product": "lending_reserve",
        "connector": request.connector,
        "resolution": resolved.summary(),
        "price_sources": sources,
        "config": config,
        **optional_observation(
            request.connector,
            resolved,
            lambda: selected.observe(reader, selected, resolved, block),
            lambda observation: risk_entry(
                lambda: selected.risk(reader, resolved, observation, block), query
            ),
        ),
    }


def market_config(reader, selected: Family, request, block: Block):
    """The resolved market, its price sources and its config-ready entries."""
    resolved = resolve(reader, selected, request, block)
    sources, pins = selected.sources(reader, selected, resolved, block)
    market = market_entry(request, pins.pop("assertions", None))
    return resolved, sources, {"market": market, **pins}


def read_lending_position(reader, request, account: Account, block: Block) -> dict:
    selected = family(request.connector)
    resolved = resolve(reader, selected, request, block)
    observation = selected.position(reader, selected, resolved, account, block)
    return {
        "product": "lending_position",
        "connector": request.connector,
        "resolution": resolved.summary(),
        "observation": observation.model_dump(mode="json"),
    }


def market_entry(request, assertions: dict | None) -> dict:
    """The selection as a `markets:` entry; resolved assertions are optional pins."""
    entry = request.model_dump(mode="json", exclude_none=True)
    if assertions:
        entry["assertions"] = {**entry.get("assertions", {}), **assertions}
    return entry


def optional_observation(
    connector: str, resolved, read: Callable, risk: Callable | None = None
) -> dict:
    # The price sources are the point of the lookup; a reserve state read that
    # the protocol refuses at this block is reported, not allowed to hide them.
    try:
        observation = read()
    except DataError as error:
        return {"observation": None, "observation_error": f"{error.kind}: {error}"}
    return {
        "observation": observation.model_dump(mode="json"),
        **rates_entry(connector, observation, selected_asset(connector, resolved)),
        **({} if risk is None else risk(observation)),
    }


def observed_rates(reader, connector: str, resolved, block: Block) -> dict:
    """A verified market's annual rates, from the same reserve read `lending-reserve` makes.

    Nothing is read for a connector whose observation holds no rate. A read the
    protocol refuses, or an observation that fails its own invariants, is reported
    beside the verified market rather than ending the listing.
    """
    if connector in UNOBSERVED:
        return rates_entry(connector, None, None)
    selected = family(connector)
    try:
        observation = selected.observe(reader, selected, resolved, block)
    except (DataError, ValueError) as error:
        kind = error.kind if isinstance(error, DataError) else "invalid"
        return {"rates": None, "rates_error": f"{kind}: {error}"}
    return rates_entry(connector, observation, selected_asset(connector, resolved))


def selected_asset(connector: str, resolved):
    # Only a connector whose observation holds rates needs the selected asset.
    return None if connector in UNOBSERVED else resolved.reserve.asset


def reserve_observation(reader, selected: Family, resolved, block: Block):
    return selected.reader.read_reserve(reader, resolved.reserve, block)


def market_observation(reader, selected: Family, resolved, block: Block):
    return selected.reader.read_market(reader, resolved.reserve.market, block)


def pool_sources(reader, selected: Family, resolved, block: Block):
    from almanak.connectors.pool_lending_reader import price_source

    provider = selected.reader.pool_provider(reader, resolved.reserve, block)
    oracle, feed = price_source(reader, resolved.reserve, block, provider)
    sources = {"oracle": oracle, "price_source": feed}
    # Aave V3 and SparkLend allocations pin both values under these names.
    return sources, {"allocation": dict(sources)}


def spoke_sources(reader, selected: Family, resolved, block: Block):
    from almanak.connectors.aave_v4.debt import reserve_source

    (oracle,) = reader.call(resolved.reserve.spoke, "ORACLE()", block, outputs=("address",))
    source = reserve_source(reader, oracle, resolved.reserve.reserve_id, block)
    # An Aave V4 debt names the Spoke oracle once, its own reserve's source as
    # `debt_price_source`, and each collateral reserve's source in the order of
    # its `collateral` list.
    return {"oracle": oracle, "price_source": source}, {
        "debt": {"oracle": oracle, "debt_price_source": source},
        "collateral_price_source": source,
    }


def comet_sources(reader, selected: Family, resolved, block: Block):
    market = resolved.reserve.market
    sources = {"price_feed": resolved.reserve.price_feed, "base_price_feed": market.base_price_feed}
    return sources, {"assertions": {"price_feed": resolved.reserve.price_feed}}


def morpho_sources(reader, selected: Family, resolved, block: Block):
    oracle = resolved.market.oracle
    return {"oracle": oracle}, {"assertions": {"oracle": oracle}}


def pool_position(reader, selected: Family, resolved, account: Account, block: Block):
    return selected.reader.read_position(reader, resolved.reserve, account, block)


def market_position(reader, selected: Family, resolved, account: Account, block: Block):
    market = market_observation(reader, selected, resolved, block)
    return selected.reader.read_account(reader, market, account)


FAMILIES: dict[str, tuple[Callable, Callable, Callable, Callable]] = {
    "aave_v3": (pool_sources, reserve_observation, pool_position, pool_risk),
    "sparklend": (pool_sources, reserve_observation, pool_position, pool_risk),
    "aave_v4": (spoke_sources, reserve_observation, pool_position, spoke_risk),
    "compound_v3": (comet_sources, market_observation, market_position, comet_risk),
    "morpho_blue": (morpho_sources, market_observation, market_position, morpho_risk),
}
