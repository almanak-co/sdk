"""Human historical settings resolve to frozen simulation-only inputs."""

import json
from pathlib import Path

from almanak.configuration.resolution import simulation_address
from almanak.configuration.values import amount, duration_fields, instant, seconds
from almanak.configuration.yaml import ConfigurationError, load
from scripts.legacy_configuration.author import fields

SECONDS_FIELDS = ("sample_seconds", "rate_sample_seconds", "max_age_seconds")


def adapter_account(name, chain):
    return {"chain": chain, "address": simulation_address(name, chain["chain_id"])}


data_seconds = duration_fields


def inventory(raw, author):
    fields(raw, {"funds", "payers"}, {"funds"})
    payers = []
    for name, item in raw.get("payers", {}).items():
        fields(item, {"chain", "balances"}, {"chain", "balances"})
        payers.append(
            {
                "name": name,
                "fund": adapter_account(name, item["chain"]),
                "balances": [amount(value, author.asset_metadata) for value in item["balances"]],
            }
        )
    funds = []
    for name, item in raw["funds"].items():
        fields(item, {"balances", "payer"}, {"balances"})
        if name not in author.author_accounts:
            raise ConfigurationError(f"{name}: hypothetical fund has no author account")
        chain = author.author_accounts[name].chain.model_dump(mode="json")
        account = adapter_account(name, chain)
        fund = {
            "name": name,
            "balances": [amount(v, author.asset_metadata) for v in item["balances"]],
        }
        if author.connector == "aave_v4":
            fund["fund"] = account
        else:
            profile = {"kind": "simulation", "fund": account}
            if "payer" in item:
                candidates = [p for p in payers if p["name"] == item["payer"]]
                if len(candidates) != 1 or candidates[0]["fund"]["chain"] != chain:
                    raise ConfigurationError(f"{name}: payer must identify the same chain")
                profile["payer"] = candidates[0]["fund"]
            fund["profile"] = profile
        funds.append(fund)
    return funds, payers


def normalize(raw, author):
    fields(
        raw,
        {"mode", "window", "inventory", "execution", "data", "costs"},
        {"mode", "window", "inventory", "execution", "data", "costs"},
    )
    window, execution = raw["window"], raw["execution"]
    fields(
        window, {"start", "end", "end_policy", "cleanup"}, {"start", "end", "end_policy", "cleanup"}
    )
    fields(execution, {"latency", "decision_interval", "models"}, {"latency"})
    schedule = {
        "start": instant(window["start"]),
        "end": instant(window["end"]),
        "end_policy": window["end_policy"],
        "cleanup_seconds": seconds(window["cleanup"]),
        "latency_seconds": seconds(execution["latency"]),
    }
    if "decision_interval" in execution:
        schedule["decision_interval_override_seconds"] = seconds(execution["decision_interval"])
    models = execution.get("models", {})
    if author.connector == "uniswap_v4" and all(a.range is None for a in author.allocations):
        if "liquidity_model" in models:
            raise ConfigurationError("trading backtests must omit irrelevant liquidity_model")
    data = dict(raw["data"])
    history = data.pop("history", None)
    if history is None:
        raise ConfigurationError("data.history requires explicit chain/provider bindings")
    data = data_seconds(data)
    funds, payers = inventory(raw["inventory"], author)
    if author.connector is None:
        return composed(raw, schedule, history, funds, payers, data, models)
    output = {
        "market_model": raw["mode"],
        "schedule": schedule,
        "history": history,
        "funds": funds,
        "costs": raw["costs"],
        **execution.get("models", {}),
    }
    if author.connector != "aave_v4":
        output["payers"] = payers
    if data:
        output["data"] = data
    return output


def read_backtest(path: Path, author):
    from almanak.connectors.composition import connector_for

    document = load(path)
    wire = normalize(document.value, author)
    binding = connector_for(author).backtest(wire["market_model"])
    return binding.settings_type.model_validate_json(json.dumps(wire))


def composed(raw, schedule, history, funds, payers, data, models):
    fields(data, {"connectors"}, {"connectors"})
    policies = data["connectors"]
    costs = raw["costs"]
    if not all(isinstance(value, dict) for value in (policies, costs, models)):
        raise ConfigurationError("composed data, costs and models require connector mappings")
    if not policies or set(policies) != set(costs) or set(policies) != set(models):
        raise ConfigurationError("composed data, costs and models must name the same connectors")
    selected = {}
    for connector, policy in policies.items():
        selected[connector] = {
            **models[connector],
            "data": data_seconds(policy),
            "costs": costs[connector],
        }
    return {
        "market_model": raw["mode"],
        "schedule": schedule,
        "history": history,
        "funds": funds,
        "payers": payers,
        "models": selected,
    }
