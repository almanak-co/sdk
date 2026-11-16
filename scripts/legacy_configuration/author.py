"""Named author inputs resolve once into connector-owned parameter types."""

import json
from pathlib import Path

from pydantic import TypeAdapter, ValidationError

from almanak.configuration.parameter_codec import (
    AuthorAccount,
    AuthorBase,
    author_type,
    origin_scope,
    parameters_from_wire,
    validation_parameters,
)
from almanak.configuration.selection import require_connector
from almanak.configuration.values import (
    amount,
    asset_catalog,
    configured_quantity,
    quantity,
    seconds,
)
from almanak.configuration.yaml import ConfigurationError, load
from almanak.models import Model

__all__ = [
    "AuthorAccount",
    "AuthorBase",
    "author_type",
    "origin_scope",
    "parameters_from_wire",
    "validation_parameters",
]


def fields(value, permitted, required=()):
    if not isinstance(value, dict):
        raise ConfigurationError("expected a mapping")
    extra, missing = set(value) - set(permitted), set(required) - set(value)
    if extra or missing:
        raise ConfigurationError(
            f"unknown fields: {sorted(extra)}; missing fields: {sorted(missing)}"
        )


def normalize(raw):
    if (
        isinstance(raw, dict)
        and isinstance(raw.get("strategy"), dict)
        and "connector" not in raw["strategy"]
    ):
        from scripts.legacy_configuration.composed import normalize as composed

        return composed(raw)
    fields(
        raw,
        {"strategy", "assets", "markets", "accounts", "execution"},
        {"strategy", "assets", "markets", "accounts"},
    )
    strategy = raw["strategy"]
    fields(
        strategy,
        {"connector", "decision_interval", "allocations", "debts", "parameters"},
        {"connector", "decision_interval", "allocations"},
    )
    connector = str(require_connector(strategy["connector"]))
    assets = asset_catalog(raw["assets"])
    validate_markets(raw["markets"], connector)
    output = {
        "connector": connector,
        "decision_trigger": {"kind": "interval", "seconds": seconds(strategy["decision_interval"])},
        "asset_metadata": assets,
        "author_accounts": raw["accounts"],
        "allocations": allocations(strategy["allocations"], raw["markets"], assets, connector),
        "execution": execution(raw.get("execution", {}), assets),
    }
    if "debts" in strategy:
        output["debts"] = allocations(strategy["debts"], raw["markets"], assets, connector)
    if strategy.get("parameters"):
        output["parameters"] = strategy["parameters"]
    author = author_type(connector).model_validate_json(json.dumps(output))
    if "fork" in author.execution:
        author.fork_config()
    if "production" in author.execution:
        _ = author.production
    return author


def allocations(values, markets, assets, connector):
    if not isinstance(values, dict):
        raise ConfigurationError("allocations must be a named mapping")
    result = []
    for name, entry in values.items():
        try:
            result.append(allocation(name, entry, markets, assets, connector))
        except (ConfigurationError, KeyError) as exc:
            field = f"allocations.{name}" + (
                f".{exc.field}" if isinstance(exc, ConfigurationError) and exc.field else ""
            )
            raise ConfigurationError(str(exc), field=field) from exc
    return result


def allocation(name, entry, markets, assets, connector):
    if not isinstance(entry, dict):
        raise ConfigurationError("allocation requires a mapping")
    if set(entry) & {"name", "reserve", "pool", "contracts"}:
        raise ConfigurationError("allocation targets come only from its named market")
    item = dict(entry)
    reference = item.pop("market", None)
    if reference not in markets:
        raise ConfigurationError(f"unknown market {reference}", field="market")
    market = markets[reference]
    if connector in ("aave_v4", "aave_v3", "sparklend", "compound_v3", "morpho_blue"):
        item["reserve"] = market
        if market["asset"] not in assets:
            raise ConfigurationError(
                f"missing asset metadata for {market['asset']}", field="market"
            )
        for key in ("capital", "amount", "supply_limit", "borrow_limit", "entry_amount"):
            if key in item:
                item[key] = configured_quantity(item[key], assets[market["asset"]]["decimals"], key)
        retired_lending(item)
    else:
        item.update(market)
        if connector == "uniswap_v4":
            exchange_variant(item)
        if connector == "curve":
            # Archived `maximums` sized Roles quotas; grants are scope-only (blueprint 27).
            item.pop("maximums", None)
            if not isinstance(item.get("capital"), list):
                raise ConfigurationError(
                    "Curve quantities require an ordered asset list", field="capital"
                )
            item["capital"] = [amount(value, assets) for value in item["capital"]]
        else:
            # Archived exchange allocations sized Roles quotas with these fields. Grants are
            # scope-only (blueprint 27), so the frozen values are history the model refuses.
            for key in ("maximum0", "maximum1"):
                item.pop(key, None)
            if item.get("capital") is not None:
                item["capital"] = amount(item["capital"], assets)
    if connector == "gmx_v2":
        from scripts.legacy_configuration.gmx_authoring import normalize as normalize_gmx

        normalize_gmx(item, assets)
    normalize_policy(item, assets)
    return {"name": name, **item}


def retired_lending(item):
    """Read archived lending budgets in the scope-only interface (blueprint 27).

    The archived entry supply (`entry_amount`, else `supply_limit`) is the capital the
    allocation started with, and a nonzero `borrow_limit` granted borrowing. An Aave V4
    supply `amount` was its entry supply; a debt's `amount` was only a budget. An archived
    `repayment_ceiling` (since removed) only bounded repayment, so it goes.
    """
    item.pop("repayment_ceiling", None)
    if "supply_limit" in item:
        entry = item.pop("entry_amount", None)
        supplied = item.pop("supply_limit")
        item["capital"] = supplied if entry is None else entry
        item["borrow"] = item.pop("borrow_limit", 0) > 0
    elif "amount" in item:
        amount = item.pop("amount")
        if "debt_price_source" not in item:
            item["capital"] = amount


def validate_markets(markets, connector):
    from almanak.connectors.aave_v3.domain import Reserve as V3Reserve
    from almanak.connectors.aave_v4.domain import AaveReserve
    from almanak.connectors.aerodrome.domain import AerodromePool
    from almanak.connectors.aerodrome.domain import Contracts as AerodromeContracts
    from almanak.connectors.aerodrome_slipstream.domain import SlipstreamPool
    from almanak.connectors.compound_v3.domain import Reserve as CompoundReserve
    from almanak.connectors.curve.domain import CurvePool
    from almanak.connectors.gmx_v2.domain import Market as GMXMarket
    from almanak.connectors.gmx_v2.exchange_domain import ExchangeMarket as GMXExchangeMarket
    from almanak.connectors.morpho_blue.domain import Reserve as MorphoReserve
    from almanak.connectors.pancakeswap_v3.domain import PancakePool
    from almanak.connectors.sparklend.domain import Reserve as SparkReserve
    from almanak.connectors.uniswap_v3.domain import Contracts as V3Contracts
    from almanak.connectors.uniswap_v3.domain import UniswapPool as V3Pool
    from almanak.connectors.uniswap_v4.domain import UniswapPool
    from almanak.connectors.uniswap_v4.models import Deployment

    if not isinstance(markets, dict):
        raise ConfigurationError("markets must be a named mapping", field="markets")
    for name, market in markets.items():
        try:
            if connector == "aave_v4":
                AaveReserve.model_validate_json(json.dumps(market))
            elif connector == "aave_v3":
                V3Reserve.model_validate_json(json.dumps(market))
            elif connector == "sparklend":
                SparkReserve.model_validate_json(json.dumps(market))
            elif connector == "morpho_blue":
                MorphoReserve.model_validate_json(json.dumps(market))
            elif connector == "compound_v3":
                CompoundReserve.model_validate_json(json.dumps(market))
            elif connector == "gmx_v2":
                fields(market, {"market"}, {"market"})
                TypeAdapter(GMXMarket | GMXExchangeMarket).validate_json(
                    json.dumps(market["market"])
                )
            elif connector == "curve":
                fields(market, {"pool"}, {"pool"})
                CurvePool.model_validate_json(json.dumps(market["pool"]))
            elif connector == "aerodrome":
                fields(market, {"pool", "contracts"}, {"pool", "contracts"})
                AerodromePool.model_validate_json(json.dumps(market["pool"]))
                AerodromeContracts.model_validate_json(json.dumps(market["contracts"]))
            elif connector in ("uniswap_v3", "pancakeswap_v3", "aerodrome_slipstream"):
                fields(market, {"pool", "contracts"}, {"pool", "contracts"})
                pool_types: dict[str, type[Model]] = {
                    "uniswap_v3": V3Pool,
                    "pancakeswap_v3": PancakePool,
                    "aerodrome_slipstream": SlipstreamPool,
                }
                pool_types[connector].model_validate_json(json.dumps(market["pool"]))
                V3Contracts.model_validate_json(json.dumps(market["contracts"]))
            else:
                fields(market, {"pool", "contracts"}, {"pool", "contracts"})
                UniswapPool.model_validate_json(json.dumps(market["pool"]))
                Deployment.model_validate_json(json.dumps(market["contracts"]))
        except (ValueError, KeyError) as exc:
            raise ConfigurationError(str(exc), field=f"markets.{name}") from exc


def exchange_variant(item):
    from almanak.configuration.exchange import ALLOCATION

    ALLOCATION.validate_json(json.dumps(item))


def normalize_policy(item, assets):
    for key in ("hold_seconds", "collect_seconds", "deadline_seconds"):
        if item.get(key) is not None:
            item[key] = seconds(item[key])
    if "minimum_native_balance" in item:
        identity = item["native_asset"]
        item["minimum_native_balance"] = quantity(
            item["minimum_native_balance"], assets[identity]["decimals"]
        )
    if "minimum_health_factor" in item:
        item["minimum_health_factor"] = quantity(item["minimum_health_factor"], 18)


def execution(raw, assets):
    fields(raw, {"fork", "production"})
    result = json.loads(json.dumps(raw))
    if "fork" in result:
        fork = result["fork"]
        fields(fork, {"targets", "funds", "operators"}, {"targets", "funds"})
        for fund in fork["funds"]:
            for asset in fund.get("assets", []):
                asset["balance"] = amount(asset["balance"], assets)
            if "native_balance" in fund:
                fund["native_balance"] = amount(fund["native_balance"], assets)
        for operator in fork.get("operators", []):
            operator["native_balance"] = amount(operator["native_balance"], assets)
    if "production" in result:
        result["production"] = production(result["production"], assets)
    return result


def production(raw, assets):
    fields(raw, {"networks", "payers"}, {"networks", "payers"})
    result = {"networks": raw["networks"], "gas": []}
    for network in result["networks"]:
        network["max_observation_age_seconds"] = seconds(network["max_observation_age_seconds"])
    for name, value in raw["payers"].items():
        if "name" in value:
            raise ConfigurationError("payer policy name is its mapping key")
        policy = {"name": name, **value}
        for key in ("max_transaction_fee", "total_budget", "exit_reserve"):
            policy[key] = amount(policy[key], assets)
        if policy.get("replacement"):
            for key in ("interval_seconds", "max_elapsed_seconds"):
                policy["replacement"][key] = seconds(policy["replacement"][key])
        result["gas"].append(policy)
    return result


def read_author(path: Path):
    if path.suffix == ".json":
        from almanak.connectors.composition import author_configuration_from_json

        return author_configuration_from_json(path.read_text())
    from almanak.configuration.unresolved import public_root

    document = load(path)
    if public_root(document.value):
        raise ConfigurationError(
            "this file uses the public author root; harness loading applies to "
            "unconverted references only"
        ).at(file=document.file, field="markets", line=document.locations.get("markets"))
    try:
        return normalize(document.value)
    except (ConfigurationError, ValidationError, KeyError) as exc:
        field = exc.field if isinstance(exc, ConfigurationError) else ""
        if isinstance(exc, ValidationError):
            error = exc.errors(include_input=False, include_url=False)[0]
            field = ".".join(map(str, error["loc"]))
            message = error["msg"]
        else:
            message = str(exc)
        field = author_field(field, document.value)
        location = field
        while location not in document.locations and "." in location:
            location = location.rsplit(".", 1)[0]
        diagnostic = exc if isinstance(exc, ConfigurationError) else ConfigurationError(message)
        raise diagnostic.at(
            file=document.file, field=field, line=document.locations.get(location)
        ) from exc


def author_field(field, raw):
    parts = field.split(".")
    if parts[0] in ("allocations", "debts"):
        kind = parts[0]
        entries = raw.get("strategy", {}).get(kind, {})
        parts = named_entry(parts, entries)
        if len(parts) > 1 and parts[1] in raw.get("strategy", {}).get("debts", {}):
            parts[0] = "debts"
        return "strategy." + ".".join(parts)
    return field.replace("asset_metadata", "assets", 1).replace("author_accounts", "accounts", 1)


def named_entry(parts, entries):
    if len(parts) > 1 and parts[1].isdigit():
        parts[1] = list(entries)[int(parts[1])]
    return parts
