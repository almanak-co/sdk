"""Resolve QA variations once without changing author-maintained inputs."""

import json
from datetime import UTC, datetime

import yaml
from dotenv import dotenv_values

from almanak.caip import ChainId
from almanak.configuration.backtest_inputs import read_backtest_inputs
from almanak.configuration.unresolved import read_inputs
from almanak.configuration.values import instant, seconds
from almanak.configuration.yaml import load
from almanak.gateway.ethereum import DataError
from almanak.gateway.fork_config import ForkTarget
from almanak.gateway.providers import provider_endpoint
from almanak.models import Chain
from scripts.legacy_configuration.author import read_author
from scripts.legacy_configuration.backtest import read_backtest
from tests.qa.catalog import ROOT, converted
from tests.qa.operators import case_inputs, case_operator, example_operator


def write_json(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(value, indent=2, sort_keys=True, default=str) + "\n")
    temporary.replace(path)


def source_config(case):
    if case.scenario == "curve_mixed":
        return ROOT / "tests/qa/scenarios" / case.scenario / "config.yaml"
    return ROOT / "strategies" / case.reference / "config.yaml"


def operator_mode(case):
    return "fork" if case.lane == "fork" else "backtest"


def require_providers(case, env_file):
    if env_file is None or not env_file.is_file():
        raise ValueError("provider env file is missing")
    if not case.reference:
        return
    if converted(case):
        for network in example_operator(case, operator_mode(case)).networks:
            provider_endpoint(network.binding(), env_file)
        require_vendor_credentials(case, env_file)
        return
    for target in read_author(source_config(case)).fork_config().forks:
        provider_endpoint(target.provider, env_file)


def require_vendor_credentials(case, env_file):
    if not case.backtest_file:
        return
    document = read_backtest_inputs(source_config(case).with_name(case.backtest_file))
    vendors = document.inputs.data.get("vendors", ())
    if not isinstance(vendors, list):
        raise ValueError("selected vendor settings must declare a vendor list")
    selected: dict[str, str] = {}
    for vendor in vendors:
        if isinstance(vendor, dict):
            name, kind = vendor.get("name"), vendor.get("kind")
            if isinstance(name, str) and isinstance(kind, str):
                selected[name] = kind
    operator = example_operator(case, "backtest")
    bindings = {binding.name: binding for binding in operator.vendors}
    required = set()
    for name, kind in selected.items():
        binding = bindings.get(name)
        if binding is None or binding.provider.kind != kind:
            raise ValueError(f"operator has no {kind} vendor binding named {name}")
        required.add(binding.provider.api_key_env)
    values = dotenv_values(env_file, interpolate=False)
    missing = sorted(name for name in required if not values.get(name))
    if missing:
        raise ValueError(f"{', '.join(missing)} is required in the selected env file")


def case_targets(case):
    """Converted references take their fork providers from the operator networks."""
    if not converted(case):
        return read_author(source_config(case)).fork_config().forks
    return tuple(
        ForkTarget(
            chain=Chain(chain_id=network.chain, environment="mainnet"),
            provider=network.binding(),
            block="latest",
        )
        for network in example_operator(case, "fork").networks
    )


def fresh_targets(cases):
    targets: dict[ChainId, ForkTarget] = {}
    for case in cases:
        if (case.lane, case.input_policy) == ("fork", "fresh"):
            for target in case_targets(case):
                targets.setdefault(target.chain.chain_id, target)
    return targets.values()


def resolve_target(target, env_file):
    if env_file is None:
        raise ValueError("fresh fork resolution requires --env-file")
    fresh = ForkTarget.model_validate_json(
        json.dumps({**target.model_dump(mode="json"), "block": "latest"})
    )
    return fresh.resolve(env_file).model_dump(mode="json")


def resolve_origins(cases, env_file, directory):
    resolved, failures = {}, {}
    for target in fresh_targets(cases):
        chain = target.chain.chain_id
        try:
            resolved[chain] = resolve_target(target, env_file)
        except (ValueError, RuntimeError, OSError, DataError) as error:
            failures[chain] = f"{type(error).__name__}: {error}"
    write_json(
        directory / "origins.json",
        {
            "resolved": {str(k): v for k, v in resolved.items()},
            "failures": {str(k): v for k, v in failures.items()},
        },
    )
    return resolved, failures


FROZEN_KEYS = {"input_time", "resolved", "failures"}


def freeze_inputs(cases, env_file, directory):
    """Resolve fresh fork origins and the input time once, before a hosted fan-out.

    Every job of one hosted matrix then forks the same fresh block per chain and
    ends recent historical windows at the same instant, as one local run does.
    """
    resolved, failures = resolve_origins(cases, env_file, directory)
    value = {
        "input_time": utc_now(),
        "resolved": {str(k): v for k, v in resolved.items()},
        "failures": {str(k): v for k, v in failures.items()},
    }
    write_json(directory / "frozen-inputs.json", value)
    return value


def frozen_origins(path, directory):
    """Read inputs frozen by `freeze_inputs` and hand them to workers unchanged."""
    value = json.loads(path.read_text())
    if not isinstance(value, dict) or set(value) != FROZEN_KEYS:
        raise ValueError(f"frozen inputs need exactly {sorted(FROZEN_KEYS)}")
    if type(value["input_time"]) is not int or value["input_time"] <= 0:
        raise ValueError("frozen input time must be a positive integer")
    write_json(
        directory / "origins.json",
        {"resolved": value["resolved"], "failures": value["failures"]},
    )
    return (
        value["input_time"],
        {ChainId(k): v for k, v in value["resolved"].items()},
        {ChainId(k): v for k, v in value["failures"].items()},
    )


def apply_native_hold(raw):
    policy = json.loads((ROOT / "tests/qa/policy.json").read_text())
    for allocation in raw["strategy"]["allocations"].values():
        if allocation.get("hold_seconds") is not None:
            allocation["hold_seconds"] = str(policy["native_hold_seconds"]) + "s"
        if "collect_seconds" in allocation:
            allocation["collect_seconds"] = str(policy["native_collect_seconds"]) + "s"


def drops_debts(case):
    return (
        case.lane == "backtest"
        and str(case.connector) == "aave_v4"
        and case.model_mode == "historical_path"
    )


def referenced_markets(raw):
    """A dropped debt leaves its market unused; resolution must not read it anyway."""
    strategy = raw["strategy"]
    used = {
        item["market"]
        for group in ("allocations", "debts")
        for item in strategy.get(group, {}).values()
    }
    return {name: value for name, value in raw["markets"].items() if name in used}


def converted_author(case, directory, origins):
    raw = load(source_config(case)).value
    if drops_debts(case):
        raw["strategy"]["debts"] = {}
        raw["markets"] = referenced_markets(raw)
    if case.input_policy == "native":
        apply_native_hold(raw)
    path = directory / "config.yaml"
    path.write_text(yaml.safe_dump(raw, sort_keys=False))
    read_inputs(path)
    case_operator(case, directory, origins)
    return path


def resolved_author(case, directory, origins):
    if converted(case):
        return converted_author(case, directory, origins)
    raw = load(source_config(case)).value
    update_legacy_author(case, raw, origins)
    path = directory / "config.yaml"
    path.write_text(yaml.safe_dump(raw, sort_keys=False))
    read_author(path)
    return path


def update_legacy_author(case, raw, origins):
    refresh_legacy_anchors(case, raw, origins)
    drop_legacy_debts(case, raw)
    apply_legacy_native_hold(case, raw)


def refresh_legacy_anchors(case, raw, origins):
    if case.lane == "fork" and case.input_policy == "fresh":
        for target in raw["execution"]["fork"]["targets"]:
            target["block"] = origins[ChainId(target["chain"]["chain_id"])]


def drop_legacy_debts(case, raw):
    if drops_debts(case):
        raw["strategy"]["debts"] = {}


def apply_legacy_native_hold(case, raw):
    if case.input_policy == "native":
        apply_native_hold(raw)


def resolved_history(case, directory, now):
    mode = case.model_mode
    name = case.backtest_file or (
        "backtest-economic.yaml" if mode == "economic" else "backtest.yaml"
    )
    raw = load(source_config(case).with_name(name)).value
    policy = json.loads((ROOT / "tests/qa/policy.json").read_text())
    window, execution = raw["window"], raw["execution"]
    if case.input_policy == "fresh":
        lag = max(policy["source_lags"][str(chain)] for chain in case.chains)
        duration = instant(window["end"]) - instant(window["start"])
        end = now - lag - seconds(window["cleanup"])
        window.update(start=iso_time(end - duration), end=iso_time(end))
    if str(case.connector) == "aave_v4" and mode == "historical_path":
        execution["decision_interval"] = str(policy["detailed_aave_interval_seconds"]) + "s"
    if case.input_policy == "native":
        execution.pop("decision_interval", None)
        window["end"] = iso_time(instant(window["start"]) + policy["native_window_seconds"])
        window["cleanup"] = str(policy["native_window_seconds"]) + "s"
        if mode == "economic":
            limits = native_collection_limits(case.reference, policy)
            raw.setdefault("data", {}).update(limits)
            component_limits = native_component_collection_limits(case.reference, policy)
            if component_limits:
                configured = raw["data"]["connectors"]
                if set(component_limits) != set(configured):
                    raise ValueError(
                        "native component collection limits must cover configured connectors"
                    )
                for connector, selected in component_limits.items():
                    configured[connector].update(selected)
            write_json(
                directory / "native-collection-policy.json",
                {"reference": case.reference, "limits": limits, "components": component_limits},
            )
    path = directory / "backtest.yaml"
    path.write_text(yaml.safe_dump(raw, sort_keys=False))
    validate_history(case, path, directory)
    return path


def validate_history(case, path, directory):
    if converted(case):
        read_backtest_inputs(path).require_accounts(case_inputs(case, directory))
        return
    author = read_author(
        directory / "config.yaml" if (directory / "config.yaml").exists() else source_config(case)
    )
    read_backtest(path, author)


def iso_time(timestamp):
    return datetime.fromtimestamp(timestamp, UTC).isoformat()


def utc_now():
    return int(datetime.now(UTC).timestamp())


def native_collection_limits(reference, policy):
    limits = policy.get("native_collection_limits", {}).get(reference, {})
    return validated_collection_limits(limits)


def native_component_collection_limits(reference, policy):
    from almanak.connectors.identifiers import ConnectorId

    components = policy.get("native_component_collection_limits", {}).get(reference, {})
    return {
        str(ConnectorId(connector)): validated_collection_limits(limits)
        for connector, limits in components.items()
    }


def validated_collection_limits(limits):
    allowed = {
        "max_accounting_samples",
        "max_risk_blocks",
        "max_risk_samples",
        "max_samples",
        "maximum_risk_blocks",
        "publication_limits",
    }
    require_limit_keys(limits, allowed)
    publications = limits.get("publication_limits")
    simple = {key: value for key, value in limits.items() if key != "publication_limits"}
    require_positive_limits(simple)
    if "publication_limits" in limits:
        validate_publication_limits(publications, limits.get("maximum_risk_blocks"))
    return limits


def require_positive_limits(limits):
    if any(type(value) is not int or value <= 0 for value in limits.values()):
        raise ValueError("native collection policy requires explicit positive collection limits")


def require_limit_keys(limits, allowed):
    if not isinstance(limits, dict) or set(limits) - allowed:
        raise ValueError("native collection policy requires explicit positive collection limits")


def validate_publication_limits(publications, risk_blocks):
    names = {
        "page_blocks",
        "maximum_blocks",
        "maximum_pages",
        "maximum_logs",
        "maximum_transactions",
    }
    require_publication_keys(publications, names)
    require_positive_limits(publications)
    pages = (publications["maximum_blocks"] + publications["page_blocks"] - 1) // publications[
        "page_blocks"
    ]
    if publications["maximum_pages"] < pages:
        raise ValueError("native GMX publication page limit cannot cover its block limit")
    if risk_blocks is not None and publications["maximum_blocks"] < risk_blocks:
        raise ValueError("native GMX publication block limit cannot cover its risk range")


def require_publication_keys(publications, names):
    if not isinstance(publications, dict) or set(publications) != names:
        raise ValueError("native GMX publication policy requires every collection limit")


def preparation_timeout(case):
    if case.input_policy != "native" or case.model_mode != "economic":
        return 1800
    policy = json.loads((ROOT / "tests/qa/policy.json").read_text())
    value = policy.get("native_preparation_timeout_seconds", {}).get(case.reference, 1800)
    if type(value) is not int or value <= 0:
        raise ValueError("native preparation timeout must be a positive integer")
    return value
