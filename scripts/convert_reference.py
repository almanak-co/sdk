"""Rewrite one reference directory from the harness author format to the public root.

The rewrite is deterministic and offline: author choices stay in the strategy
directory, and the execution block becomes operator example files. Only
connectors with a public market shape convert; every other reference is refused
unchanged. This script is removed once every reference is converted.
"""

import argparse
import json
from decimal import Decimal
from pathlib import Path

import yaml

from almanak.caip import ChainId
from almanak.configuration.unresolved import public_root
from almanak.configuration.values import quantity
from almanak.connectors.aave_v3.networks import pool_for as aave_v3_pool
from almanak.connectors.morpho_blue.networks import core_for
from almanak.connectors.sparklend.networks import pool_for as sparklend_pool

ROOT = Path(__file__).resolve().parents[1]
SIGNING = {
    "key_file": "/private/operator/keystores.json",
    "password_file": "/private/operator/keystore-password",
}
GAS_KEYS = (
    "source",
    "base_fee_basis",
    "reward_percentile",
    "base_fee_multiplier_bps",
    "max_gas_price_wei",
    "max_gas_limit",
    "gas_limit_buffer_bps",
    "replacement",
)
GAS_AMOUNTS = ("max_transaction_fee", "total_budget", "exit_reserve")


def hint(schema):
    return f"# yaml-language-server: $schema=../../docs/schemas/{schema}.schema.json\n"


def dump(value):
    return yaml.safe_dump(value, sort_keys=False, allow_unicode=True)


def chain_id(chain):
    """Legacy chain objects name mainnet explicitly; the public root names the chain only."""
    if not isinstance(chain, dict) or chain.get("environment") != "mainnet":
        raise ValueError("only mainnet chains convert to the public root")
    return chain["chain_id"]


def flatten_aave(market):
    return {
        "connector": "aave_v4",
        "chain": chain_id(market["chain"]),
        "spoke": market["spoke"],
        "reserve_id": market["reserve_id"],
    }


def flatten_pool(connector):
    """V3-family markets select the pool; currencies, fee and periphery are resolved."""

    def flatten(market):
        pool = market["pool"]
        return {"connector": connector, "chain": chain_id(pool["chain"]), "pool": pool["pool"]}

    return flatten


def flatten_reserve(connector, reviewed):
    """Pool-lending markets select the asset; the Pool must be the reviewed table entry."""

    def flatten(market):
        chain = chain_id(market["chain"])
        if market["pool"] != reviewed(ChainId(chain)):
            raise ValueError(
                f"{connector} pool {market['pool']} is not the reviewed Pool on {chain}"
            )
        return {"connector": connector, "chain": chain, "asset": market["asset"]}

    return flatten


def flatten_comet(market):
    """A Comet is the market identity; its base asset and feeds are resolved."""
    comet = market["market"]
    return {
        "connector": "compound_v3",
        "chain": chain_id(comet["chain"]),
        "comet": comet["comet"],
        "asset": market["asset"],
    }


def flatten_morpho(market):
    """The leg follows the authored asset; the core must be the reviewed table entry."""
    source = market["market"]
    chain = chain_id(source["chain"])
    if source["core"] != core_for(ChainId(chain)):
        raise ValueError(f"morpho_blue core {source['core']} is not the reviewed core on {chain}")
    sides = {source["loan_asset"]: "loan", source["collateral_asset"]: "collateral"}
    return {
        "connector": "morpho_blue",
        "chain": chain,
        "market_id": source["market_id"],
        "leg": sides[market["asset"]],
    }


def flatten_pool_key(market):
    """A V4 market is its pool key; the PoolManager and StateView are reviewed roles."""
    pool = market["pool"]
    return {
        "connector": "uniswap_v4",
        "chain": chain_id(pool["chain"]),
        **{key: pool[key] for key in ("currency0", "currency1", "fee", "tick_spacing", "hooks")},
    }


def flatten_gmx(market):
    """A GMX market is its GM share token; the underlying assets and roles are resolved."""
    value = market["market"]
    return {
        "connector": "gmx_v2",
        "chain": chain_id(value["chain"]),
        "market": value["share_asset"],
    }


MARKET_SHAPES = {
    "aave_v4": flatten_aave,
    "uniswap_v3": flatten_pool("uniswap_v3"),
    "uniswap_v4": flatten_pool_key,
    "pancakeswap_v3": flatten_pool("pancakeswap_v3"),
    "aerodrome_slipstream": flatten_pool("aerodrome_slipstream"),
    "aerodrome": flatten_pool("aerodrome"),
    "curve": flatten_pool("curve"),
    "aave_v3": flatten_reserve("aave_v3", aave_v3_pool),
    "sparklend": flatten_reserve("sparklend", sparklend_pool),
    "compound_v3": flatten_comet,
    "morpho_blue": flatten_morpho,
    "gmx_v2": flatten_gmx,
}


def used_markets(raw):
    """Resolution reads only the markets a position names; unused ones are dropped."""
    used = {
        position["market"]
        for group in ("allocations", "debts")
        for position in (raw["strategy"].get(group) or {}).values()
    }
    return {name: market for name, market in raw["markets"].items() if name in used}


def market_kind(market):
    """Connector roots carry their kind directly or in their protocol target."""
    for key in ("reserve", "pool", "market"):
        nested = market.get(key)
        if isinstance(nested, dict) and "kind" in nested:
            return nested["kind"]
    return market.get("kind")


def flatten_market(name, market):
    kind = market_kind(market)
    if kind in MARKET_SHAPES:
        return MARKET_SHAPES[kind](market.get("reserve", market))
    raise ValueError(f"market {name}: connector {kind} has no public market shape yet")


def convert_position(position, connector):
    """Native asset and null choices come from the resolved market or the defaults.

    A GMX perpetual position carried no author kind because the harness root
    already named the connector; the public position kinds discriminate it.
    """
    result = {
        key: value for key, value in position.items() if key != "native_asset" and value is not None
    }
    if connector == "gmx_v2" and "kind" not in result:
        result = {"kind": "gmx_position", **result}
    return scope_only(result, connector)


# Legacy author fields that sized Roles quotas. Grants are scope-only (blueprint 27).
RETIRED_BUDGETS = frozenset(
    {
        "maximum0",
        "maximum1",
        "maximums",
        "maximum_input",
        "maximum_size_delta_usd_e30",
        "maximum_inputs",
        "maximum_market_token_input",
        "maximum_price_per_base_unit",
        "maximum_collateral_withdrawal_base_units",
        "repayment_ceiling",
    }
)


def scope_only(position, connector):
    """Translate legacy budgets: the entry supply becomes `capital`, borrowing a flag."""
    result = {k: v for k, v in position.items() if k not in RETIRED_BUDGETS}
    if connector == "aave_v4":
        amount = result.pop("amount", None)
        # A debt's amount was only a budget; a supply allocation's was its entry supply.
        if amount is not None and "collateral" not in result:
            result["capital"] = amount
    elif "supply_limit" in result:
        entry = result.pop("entry_amount", None)
        supplied = result.pop("supply_limit")
        result["capital"] = supplied if entry is None else entry
        if nonzero(result.pop("borrow_limit", 0)):
            result["borrow"] = True
    return result


def nonzero(quantity):
    value = quantity.get("base_units", 0) if isinstance(quantity, dict) else quantity
    return Decimal(str(value)) != 0


def convert_gas(payers):
    result = {}
    for name, policy in payers.items():
        chain = policy["asset"].rsplit("/", 1)[0]
        converted = {"accounts": policy["accounts"], "chain": chain}
        converted.update({key: policy[key] for key in GAS_KEYS if key in policy})
        converted.update({key: gas_quantity(policy[key]) for key in GAS_AMOUNTS if key in policy})
        result[name] = converted
    return result


def gas_quantity(value):
    return value["quantity"] if isinstance(value, dict) and "quantity" in value else value


def convert_author(raw):
    strategy = {"decision_interval": raw["strategy"]["decision_interval"]}
    markets = used_markets(raw)
    for group in ("allocations", "debts"):
        items = raw["strategy"].get(group) or {}
        if items:
            converted = {
                name: convert_position(position, market_kind(markets[position["market"]]))
                for name, position in items.items()
                if group != "allocations" or not aave_v4_debt(position, markets)
            }
            if converted:
                strategy[group] = converted
    debts = {
        name: convert_position(position, "aave_v4")
        for name, position in raw["strategy"].get("allocations", {}).items()
        if aave_v4_debt(position, markets)
    }
    if debts:
        strategy["debts"] = {**strategy.get("debts", {}), **debts}
    production = raw.get("execution", {}).get("production")
    if production is not None:
        strategy["gas"] = convert_gas(production["payers"])
    result = {
        "strategy": strategy,
        "markets": {name: flatten_market(name, m) for name, m in markets.items()},
        "accounts": {name: {"chain": chain_id(a["chain"])} for name, a in raw["accounts"].items()},
    }
    if raw.get("inventory"):
        result["inventory"] = raw["inventory"]
    return result


def aave_v4_debt(position, markets):
    """Aave V4 borrowing is a distinct public position kind selected by collateral."""
    market = markets.get(position.get("market"))
    return market is not None and market_kind(market) == "aave_v4" and "collateral" in position


def provider_binding(provider):
    if provider["kind"] == "alchemy":
        return {"kind": "alchemy", "api_key_env": provider["api_key_env"]}
    return {"kind": provider["kind"], "url_env": provider["url_env"]}


def base_units(balance, assets):
    asset = balance["asset"]
    return {"asset": asset, "amount": quantity(balance["quantity"], assets[asset]["decimals"])}


def placeholder_binding(name, chain, index, digit):
    fund = {"chain": {"chain_id": chain, "environment": "mainnet"}}
    fund["address"] = address(index, digit)
    return {"name": name, "profile": {"kind": "eoa", "fund": fund}}


def address(index, digit):
    return "0x" + (f"{index + 1:040x}" if digit is None else str(digit) * 40)


def fork_operator(raw):
    """The fork fixture the harness carried, as provisional operator inputs."""
    fork, assets = raw["execution"]["fork"], raw["assets"]
    accounts = list(raw["accounts"].items())
    return {
        "schema_version": "operator-gateway-config-v1",
        "modes": ["fork"],
        "networks": [
            {"chain": chain_id(t["chain"]), "provider": provider_binding(t["provider"])}
            for t in fork["targets"]
        ],
        "accounts": {
            "bindings": [
                placeholder_binding(name, chain_id(a["chain"]), i, None)
                for i, (name, a) in enumerate(accounts)
            ]
        },
        "fork": {
            "anchors": [
                {
                    "chain": chain_id(t["chain"]),
                    "height": t["block"]["number"],
                    "reference": t["block"]["hash"],
                }
                for t in fork["targets"]
            ],
            "funds": [fixture_fund(f, assets) for f in fork["funds"]],
            "operators": [
                {"name": o["name"], "native_balance": base_units(o["native_balance"], assets)}
                for o in fork["operators"]
            ],
        },
    }


def fixture_fund(fund, assets):
    if "operator" not in fund:
        raise ValueError("only Safe fixture funds convert; EOA fixtures are operator choices")
    return {
        "kind": "safe_roles",
        "account": fund["account"],
        "operator": fund["operator"],
        "assets": [base_units(a["balance"], assets) for a in fund["assets"]],
    }


def production_operator(raw):
    """Reviewed operator files replace the placeholders before any production run."""
    production, assets = raw["execution"]["production"], raw["assets"]
    accounts = list(raw["accounts"].items())
    return {
        "schema_version": "operator-gateway-config-v1",
        "modes": ["production"],
        "networks": [
            {"chain": chain_id(n["chain"]), "provider": provider_binding(n["provider"])}
            for n in production["networks"]
        ],
        "accounts": {
            "bindings": [
                placeholder_binding(name, chain_id(a["chain"]), i, i + 1)
                for i, (name, a) in enumerate(accounts)
            ]
        },
        "production": {
            "networks": [
                {
                    "chain": chain_id(n["chain"]),
                    "max_observation_age_seconds": n["max_observation_age_seconds"],
                }
                for n in production["networks"]
            ],
            "payers": [ceiling(policy, assets) for policy in production["payers"].values()],
            "signing": dict(SIGNING),
        },
    }


def ceiling(policy, assets):
    asset = policy["asset"]
    return {
        "accounts": policy["accounts"],
        "max_gas_price_wei": policy["max_gas_price_wei"],
        "max_gas_limit": policy["max_gas_limit"],
        "max_transaction_fee": gas_amount(asset, policy["max_transaction_fee"], assets),
        "allow_replacement": "replacement" in policy,
    } | (
        {"total_budget": gas_amount(asset, policy["total_budget"], assets)}
        if "total_budget" in policy
        else {}
    )


def gas_amount(asset, value, assets):
    return {"asset": asset, "amount": quantity(gas_quantity(value), assets[asset]["decimals"])}


def convert_backtest(raw):
    """Historical providers move to the operator file; payers name their chain by CAIP-2 text."""
    result = json.loads(json.dumps(raw))
    data = result.get("data")
    if isinstance(data, dict):
        data.pop("history", None)
        # Vendor access moves to operator bindings; authors keep name and kind only.
        for vendor in data.get("vendors") or []:
            provider = vendor.pop("provider", None)
            if isinstance(provider, dict):
                vendor["kind"] = provider.get("kind", "coingecko")
        if not data:
            del result["data"]
    for payer in (result.get("inventory") or {}).get("payers", {}).values():
        if isinstance(payer.get("chain"), dict):
            payer["chain"] = chain_id(payer["chain"])
    return result


def author_header(name, production):
    if production:
        return (
            "# Explicit production example. The operator context is\n"
            f"# examples/operators/{name}.production.yaml.\n"
        )
    return (
        "# Author choices only. Providers, account bindings and the fork fixture are\n"
        f"# operator inputs: examples/operators/{name}.fork.yaml.\n"
    )


def operator_header(name, mode):
    return (
        f"# Operator execution context for a {mode} run of strategies/{name}. Provider\n"
        "# credentials stay in the selected --env-file. The account bindings are\n"
        "# placeholders: fork provisioning generates the actual fixture accounts, and a\n"
        "# production run requires reviewed bindings and signing files outside\n"
        "# strategy-readable paths.\n"
    )


def convert_directory(directory: Path, operators: Path):
    """Rewrite the author files in place and emit the operator examples; return the paths."""
    name = directory.name
    written = []
    for path in sorted(directory.glob("config*.yaml")):
        raw = yaml.safe_load(path.read_text())
        if public_root(raw):
            raise ValueError(f"{path} is already a public author root")
        production = "production" in raw.get("execution", {})
        header = hint("strategy") + author_header(name, production)
        path.write_text(header + dump(convert_author(raw)))
        written.append(path)
        written.extend(write_operators(raw, name, operators, production))
    for path in sorted(directory.glob("backtest*.yaml")):
        raw = yaml.safe_load(path.read_text())
        path.write_text(hint("backtest") + dump(convert_backtest(raw)))
        written.append(path)
    return written


def write_operators(raw, name, operators, production):
    """One operator example per execution block the author file carried."""
    operators.mkdir(parents=True, exist_ok=True)
    written = []
    if "fork" in raw.get("execution", {}):
        path = operators / f"{name}.fork.yaml"
        path.write_text(operator_header(name, "fork") + dump(fork_operator(raw)))
        written.append(path)
    if production:
        path = operators / f"{name}.production.yaml"
        path.write_text(operator_header(name, "production") + dump(production_operator(raw)))
        written.append(path)
    return written


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory", type=Path, help="one strategies/<name> directory")
    parser.add_argument("--operators", type=Path, default=ROOT / "examples/operators")
    args = parser.parse_args(argv)
    written = convert_directory(args.directory.resolve(), args.operators.resolve())
    print(json.dumps({"converted": [str(p) for p in written]}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
