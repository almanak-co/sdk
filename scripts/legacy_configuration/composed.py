"""Archival normalization for pre-resolution composed author files."""

import json

from almanak.configuration.composed import Author
from almanak.configuration.values import amount, asset_catalog, seconds
from almanak.configuration.yaml import ConfigurationError
from scripts.legacy_configuration.author import allocation, execution, fields
from scripts.legacy_configuration.markets import named_markets


def normalize(raw):
    fields(
        raw,
        {"strategy", "assets", "markets", "accounts", "execution", "inventory"},
        {"strategy", "assets", "markets", "accounts"},
    )
    strategy = raw["strategy"]
    fields(
        strategy,
        {"decision_interval", "allocations", "parameters"},
        {"decision_interval", "allocations"},
    )
    assets = asset_catalog(raw["assets"])
    markets = named_markets(raw["markets"])
    author = Author.model_validate_json(
        json.dumps(
            {
                "decision_trigger": {
                    "kind": "interval",
                    "seconds": seconds(strategy["decision_interval"]),
                },
                "allocations": normalize_allocations(strategy["allocations"], markets, assets),
                "asset_metadata": assets,
                "author_accounts": raw["accounts"],
                "execution": execution(raw.get("execution", {}), assets),
                "inventory": normalize_inventory(raw.get("inventory", []), raw["accounts"], assets),
                **({"parameters": strategy["parameters"]} if strategy.get("parameters") else {}),
            }
        )
    )
    if "fork" in author.execution:
        author.fork_config()
    return author


def normalize_inventory(raw, accounts, assets):
    if not isinstance(raw, list):
        raise ConfigurationError("inventory must be a list of named fund asset amounts")
    result = []
    for item in raw:
        fields(item, {"account", "balance"}, {"account", "balance"})
        account = item["account"]
        if account not in accounts:
            raise ConfigurationError("inventory must identify a configured fund account")
        balance = amount(item["balance"], assets)
        result.append(
            {
                "key": {
                    "account": account,
                    "chain": accounts[account]["chain"],
                    "asset": balance["asset"],
                },
                "amount": balance["amount"],
            }
        )
    return result


def normalize_allocations(raw, markets, assets):
    if not isinstance(raw, dict):
        raise ConfigurationError("allocations must be a named mapping")
    targets = {name: market.allocation_target() for name, market in markets.items()}
    result = []
    for name, value in raw.items():
        if not isinstance(value, dict) or value.get("market") not in markets:
            raise ConfigurationError(
                "allocation requires a configured market", field=f"allocations.{name}.market"
            )
        market = markets[value["market"]]
        result.append(allocation(name, value, targets, assets, market.connector))
    return result
