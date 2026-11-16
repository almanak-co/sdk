"""Public backtest settings agree with the harness reader on the shipped references."""

import json
from pathlib import Path

import pytest
import yaml

from almanak.backtest.config import HistoryBinding
from almanak.backtest.engine import Schedule
from almanak.backtest.pricing import CoinGeckoProvider, VendorBinding
from almanak.caip import ChainId
from almanak.configuration.anchors import AnchorSelection
from almanak.configuration.backtest_inputs import (
    history_bindings,
    parse_backtest,
    read_backtest_inputs,
    resolve_backtest,
)
from almanak.configuration.parameter_types import parameter_type
from almanak.configuration.resolution import simulation_binding
from almanak.configuration.values import instant
from almanak.configuration.yaml import ConfigurationError, load, parse
from almanak.connectors.composition import connector_for
from almanak.connectors.identifiers import ConnectorId
from almanak.connectors.support import RunMode
from almanak.gateway.operator_config import OperatorConfig, OperatorNetwork
from almanak.gateway.providers import UrlProvider
from almanak.models import AssetMetadata, Chain, StrategyAccounts
from scripts.legacy_configuration.author import read_author
from scripts.legacy_configuration.backtest import read_backtest
from tests.legacy_references import harness_directory
from tests.test_author_resolution import (
    FROZEN,
    LEGACY,
    REFERENCE,
    ROOT,
    expected_parameters,
    frozen_parameters,
    parsed,
    resolve,
)
from tests.test_author_resolution import archive as archive
from tests.test_author_resolution import raw as raw
from tests.test_author_resolution import rpc as rpc
from tests.test_author_resolution import setup as setup
from tests.test_dx_schemas import validator

ETH = ChainId("eip155:1")
AVALANCHE = ChainId("eip155:43114")
SETTINGS = ("backtest", "backtest-economic", "backtest-economic-vendor")
VENDORS = (
    VendorBinding(
        name="market_prices",
        provider=CoinGeckoProvider(api="pro", api_key_env="COINGECKO_API_KEY"),
    ),
)
# Harness-root references; a converted name reads its frozen pre-conversion copy.
UNCONVERTED = ("morpho_blue_base_lending", "uniswap_v3_base_trading")
COMPOSED = "base_borrow_and_trade"
PAYER = {"chain": "eip155:1", "balances": [{"asset": "eip155:1/slip44:60", "quantity": "1"}]}


def converted(name):
    """The shipped converted backtest file, plus the provider block its legacy form carried."""
    value = load(REFERENCE / f"{name}.yaml").value
    history = load(LEGACY / f"{name}.yaml").value["data"]["history"]
    return value, tuple(HistoryBinding.model_validate_json(json.dumps(h)) for h in history)


def parsed_backtest(value):
    text = yaml.safe_dump(json.loads(json.dumps(value)), sort_keys=False)
    return parse_backtest(parse(text, file="backtest.yaml"))


def simulation_parameters():
    accounts = StrategyAccounts(
        bindings=tuple(simulation_binding(name, ETH) for name in ("lending_0", "lending_1"))
    )
    return frozen_parameters(accounts)


def harness_parameters(directory: Path):
    """The harness author of an unconverted reference, bound to simulation accounts."""
    author = read_author(directory / "config.yaml")
    bindings = tuple(
        simulation_binding(name, account.chain.chain_id)
        for name, account in author.author_accounts.items()
    )
    wire = {
        **author.parameter_wire(),
        "accounts": StrategyAccounts(bindings=bindings).model_dump(mode="json"),
    }
    identity = ConnectorId(author.connector) if author.connector else None
    return author, parameter_type(identity).model_validate_json(json.dumps(wire))


def public_inputs(path: Path):
    """The same file as public inputs: operator providers out, CAIP-2 payer chains."""
    value = json.loads(json.dumps(load(path).value))
    history = value["data"].pop("history")
    for payer in value["inventory"].get("payers", {}).values():
        payer["chain"] = payer["chain"]["chain_id"]
    return value, tuple(HistoryBinding.model_validate_json(json.dumps(h)) for h in history)


@pytest.mark.parametrize("name", SETTINGS)
def test_reference_settings_resolve_exactly_like_the_harness_reader(name):
    value, history = converted(name)
    document = parsed_backtest(value)
    parameters = simulation_parameters()
    settings = resolve_backtest(document, parameters, history, VENDORS)
    harness = read_backtest(LEGACY / f"{name}.yaml", read_author(LEGACY / "config.yaml"))
    assert settings == harness
    assert settings.history == history
    assert Schedule.model_validate(document.inputs.schedule()) == harness.schedule
    frozen = type(settings).model_validate_json((FROZEN / f"{name}.json").read_text())
    # The frozen fixture predates hypothetical fund identities; everything else is equal.
    view = {"funds": {"__all__": {"fund"}}}
    assert settings.model_dump(exclude=view) == frozen.model_dump(exclude=view)
    assert validator("backtest.schema.json").is_valid(value)


@pytest.mark.parametrize("name", SETTINGS)
def test_public_reader_reads_files_and_refuses_author_provider_bindings(name, tmp_path):
    value, _ = converted(name)
    path = tmp_path / f"{name}.yaml"
    path.write_text(yaml.safe_dump(value, sort_keys=False))
    document = read_backtest_inputs(path)
    assert document.inputs.mode == value["mode"]
    assert document.source.file == str(path)
    with pytest.raises(ConfigurationError) as error:
        parsed_backtest(load(LEGACY / f"{name}.yaml").value)
    assert error.value.field == "data.history"
    assert error.value.file == "backtest.yaml" and error.value.line is not None
    assert "operator configuration" in str(error.value)


def test_vendor_access_in_author_inputs_is_refused_at_its_location():
    value, _ = converted("backtest-economic-vendor")
    value["data"]["vendors"][0] = {
        "name": "market_prices",
        "provider": {"api": "pro", "api_key_env": "COINGECKO_API_KEY"},
    }
    with pytest.raises(ConfigurationError) as error:
        parsed_backtest(value)
    assert not validator("backtest.schema.json").is_valid(value)
    assert error.value.field == "data.vendors.0.provider"
    assert error.value.file == "backtest.yaml" and error.value.line is not None
    assert "operator configuration" in str(error.value)


@pytest.mark.parametrize(
    "declarations,field",
    [
        (
            [{"name": "market_prices", "kind": "coingecko"}, {"kind": "coingecko"}],
            "data.vendors.1.name",
        ),
        ([{"name": "market_prices", "kind": "coingecko", "tier": "pro"}], "data.vendors.0.tier"),
        ({"name": "market_prices", "kind": "coingecko"}, "data.vendors"),
        (["market_prices"], "data.vendors.0"),
        ([{"name": "market_prices", "kind": "unknown"}], "data.vendors.0.kind"),
    ],
)
def test_vendor_selection_shape_is_strict_and_located(declarations, field):
    value, _ = converted("backtest-economic-vendor")
    value["data"]["vendors"] = declarations
    with pytest.raises(ConfigurationError) as error:
        parsed_backtest(value)
    assert error.value.field == field
    assert error.value.file == "backtest.yaml" and error.value.line is not None


@pytest.mark.parametrize("fault", ["missing", "kind"])
def test_selected_vendor_requires_an_exact_operator_binding(fault):
    value, history = converted("backtest-economic-vendor")
    if fault == "kind":
        value["data"]["vendors"][0]["kind"] = "not-coingecko"
        field = "data.vendors.0.kind"
    else:
        field = "data.vendors.0.name"
    with pytest.raises(ConfigurationError) as error:
        resolve_backtest(
            parsed_backtest(value),
            simulation_parameters(),
            history,
            VENDORS if fault == "kind" else (),
        )
    assert error.value.field == field
    assert error.value.file == "backtest.yaml" and error.value.line is not None


def test_only_selected_operator_vendor_enters_effective_settings():
    value, history = converted("backtest-economic-vendor")
    unused = VendorBinding(
        name="unused",
        provider=CoinGeckoProvider(api="demo", api_key_env="UNUSED_KEY"),
    )
    settings = resolve_backtest(
        parsed_backtest(value), simulation_parameters(), history, (*VENDORS, unused)
    )
    assert settings.data.vendors == VENDORS


def test_anchors_select_the_last_block_at_the_window_start(raw):
    value, _ = converted("backtest")
    document = parsed_backtest(value)
    inputs = parsed(raw).inputs
    document.require_accounts(inputs)
    assert document.inputs.anchors(inputs) == (
        AnchorSelection(chain=ETH, at=instant("2026-08-20T00:00:00Z")),
    )


@pytest.mark.parametrize("fault", ["unknown_fund", "foreign_asset"])
def test_funds_must_name_author_accounts_on_their_own_chain(raw, fault):
    value, _ = converted("backtest")
    if fault == "unknown_fund":
        value["inventory"]["funds"]["treasury"] = value["inventory"]["funds"].pop("lending_1")
        field = "inventory.funds.treasury"
    else:
        raw["accounts"]["lending_0"]["chain"] = "eip155:43114"
        for allocation in raw["strategy"]["allocations"].values():
            allocation["account"] = "lending_1"
        for debt in raw["strategy"]["debts"].values():
            debt["account"] = "lending_1"
        field = "inventory.funds.lending_0.balances.0.asset"
    document = parsed_backtest(value)
    with pytest.raises(ConfigurationError) as error:
        document.require_accounts(parsed(raw).inputs)
    assert error.value.field == field
    assert error.value.file == "backtest.yaml" and error.value.line is not None


@pytest.mark.parametrize(
    "field,value",
    [
        ("inventory.funds.lending_0.balances.0.quantity", 1),
        ("inventory.funds.lending_0.balances.0.quantity", {"base_units": -1}),
        ("window.cleanup", "soon"),
        ("window.start", 1787184000),
        ("window.end", "2026-08-21T00:00:00"),
        ("execution.decision_interval", 0),
        ("mode", "replay"),
    ],
)
def test_syntax_faults_name_the_field_and_line_without_precision(field, value):
    raw, _ = converted("backtest")
    target = raw
    *parents, last = field.split(".")
    for part in parents:
        target = target[int(part)] if isinstance(target, list) else target[part]
    target[last] = value
    with pytest.raises(ConfigurationError) as error:
        parsed_backtest(raw)
    assert error.value.field.startswith(field)
    assert error.value.file == "backtest.yaml" and error.value.line is not None


def test_history_bindings_come_from_the_operator_networks():
    operator = OperatorConfig(
        modes=(RunMode.BACKTEST,),
        networks=(
            OperatorNetwork(chain=ETH, provider=UrlProvider(kind="rpc_url", url_env="ARCHIVE")),
        ),
        accounts=StrategyAccounts(bindings=()),
    )
    (binding,) = history_bindings(operator, (ETH,))
    assert binding.chain.chain_id == ETH and binding.chain.environment == "mainnet"
    assert binding.provider == UrlProvider(kind="rpc_url", url_env="ARCHIVE")
    with pytest.raises(ValueError, match="no provider binding"):
        history_bindings(operator, (ETH, ChainId("eip155:43114")))


@pytest.mark.parametrize("fault", ["unbound_fund", "missing_metadata", "precision", "window"])
def test_resolution_faults_locate_the_backtest_input(fault):
    value, history = converted("backtest-economic")
    parameters = simulation_parameters()
    field = "inventory.funds.lending_0.balances.2"
    if fault == "unbound_fund":
        parameters = parameters.model_copy(
            update={"accounts": StrategyAccounts(bindings=parameters.accounts.bindings[:1])}
        )
        field = "inventory.funds.lending_1"
    elif fault == "missing_metadata":
        parameters = parameters.model_copy(
            update={
                "asset_metadata": {
                    k: v for k, v in parameters.asset_metadata.items() if "a0b8" not in str(k)
                }
            }
        )
    elif fault == "precision":
        value["inventory"]["funds"]["lending_0"]["balances"][2]["quantity"] = "1.0000001"
    else:
        value["window"]["end"] = value["window"]["start"]
        field = "schedule"
    document = parsed_backtest(value)
    with pytest.raises(ConfigurationError) as error:
        resolve_backtest(document, parameters, history, VENDORS)
    assert error.value.field == field
    assert error.value.file == "backtest.yaml"
    if fault == "window":
        assert "increasing window" in str(error.value)
    else:
        assert error.value.line is not None


def test_settings_type_follows_the_selected_mode():
    value, history = converted("backtest-economic")
    parameters = simulation_parameters()
    settings = resolve_backtest(parsed_backtest(value), parameters, history, VENDORS)
    assert type(settings) is connector_for(parameters).backtest("economic").settings_type
    assert settings.data.rate_sample_seconds == 300
    assert Path(FROZEN / "backtest-economic.json").exists()


def test_backtest_run_input_check_accepts_only_public_author_inputs():
    from types import SimpleNamespace

    from almanak.backtest.cli import check_inputs

    public = SimpleNamespace(
        directory=REFERENCE,
        config=None,
        strategy=None,
        gateway_config=Path("operator.yaml"),
    )
    check_inputs(public)
    assert (public.config, public.strategy) == (
        REFERENCE / "config.yaml",
        REFERENCE / "strategy.py",
    )
    harness = SimpleNamespace(
        directory=LEGACY,
        config=None,
        strategy=REFERENCE / "strategy.py",
        gateway_config=Path("operator.yaml"),
    )
    with pytest.raises(ConfigurationError):
        check_inputs(harness)
    assert harness.config == LEGACY / "config.yaml"
    with pytest.raises(ValueError, match="requires --gateway-config"):
        check_inputs(SimpleNamespace(directory=REFERENCE, config=None, strategy=None))
    with pytest.raises(ValueError, match="must be YAML"):
        check_inputs(
            SimpleNamespace(
                directory=REFERENCE,
                config=Path("run.json"),
                strategy=None,
                gateway_config=Path("operator.yaml"),
            )
        )


@pytest.mark.parametrize("name", UNCONVERTED)
def test_single_connector_settings_resolve_exactly_like_the_harness_reader(name):
    directory = harness_directory(name)
    author, parameters = harness_parameters(directory)
    value, history = public_inputs(directory / "backtest-economic.yaml")
    settings = resolve_backtest(parsed_backtest(value), parameters, history)
    assert settings == read_backtest(directory / "backtest-economic.yaml", author)
    assert all(fund.profile.kind == "simulation" for fund in settings.funds)
    declared = value["inventory"].get("payers", {})
    assert [payer.name for payer in settings.payers] == list(declared)
    assert [len(payer.balances) for payer in settings.payers] == [
        len(item["balances"]) for item in declared.values()
    ]
    for payer in settings.payers:
        assert payer.fund == simulation_binding(payer.name, payer.fund.chain.chain_id).profile.fund
        assert {f.profile.payer for f in settings.funds if f.profile.payer} == {payer.fund}


def test_composed_settings_pair_every_connector_with_its_data_and_costs():
    from tests.composed_public import resolved_parameters
    from tests.test_reference_conversion import legacy_snapshot

    directory = ROOT / "strategies" / COMPOSED
    _, parameters = resolved_parameters(COMPOSED)
    value = load(directory / "backtest-economic.yaml").value
    historical = legacy_snapshot(COMPOSED)["files"]["backtest-economic.yaml"]["value"]["history"]
    history = tuple(HistoryBinding.model_validate_json(json.dumps(item)) for item in historical)
    settings = resolve_backtest(parsed_backtest(value), parameters, history)
    assert set(settings.models) == {ConnectorId("aave_v3"), ConnectorId("uniswap_v3")}
    assert settings.payers == ()
    assert settings.models[ConnectorId("uniswap_v3")].data.sample_seconds == 60
    value["costs"].pop("uniswap_v3")
    with pytest.raises(ConfigurationError) as error:
        resolve_backtest(parsed_backtest(value), parameters, history)
    assert error.value.field == "data.connectors"
    assert error.value.file == "backtest.yaml" and error.value.line is not None
    assert "same connectors" in str(error.value)


@pytest.mark.parametrize("fault", ["declared", "referenced"])
def test_aave_root_refuses_a_separately_modeled_gas_payer(fault):
    value, history = converted("backtest-economic")
    if fault == "declared":
        value["inventory"]["payers"] = {"operator": PAYER}
        field = "inventory.payers.operator.chain"
    else:
        value["inventory"]["funds"]["lending_0"]["payer"] = "operator"
        field = "inventory.funds.lending_0.payer"
    with pytest.raises(ConfigurationError) as error:
        resolve_backtest(parsed_backtest(value), simulation_parameters(), history)
    assert error.value.field == field
    assert error.value.file == "backtest.yaml" and error.value.line is not None
    assert "no separate gas payer" in str(error.value)


@pytest.mark.parametrize("fault", ["undeclared", "payer_chain", "collision", "foreign_asset"])
def test_payers_must_be_declared_on_the_chain_of_the_funds_they_pay(raw, fault):
    value, _ = converted("backtest")
    inventory = value["inventory"]
    inventory["funds"]["lending_0"]["payer"] = "operator"
    field = "inventory.funds.lending_0.payer"
    if fault != "undeclared":
        inventory["payers"] = {"operator": json.loads(json.dumps(PAYER))}
    if fault == "payer_chain":
        inventory["payers"]["operator"] = {
            "chain": str(AVALANCHE),
            "balances": [{"asset": f"{AVALANCHE}/slip44:60", "quantity": "1"}],
        }
    elif fault == "collision":
        inventory["payers"]["lending_1"] = inventory["payers"].pop("operator")
        inventory["funds"]["lending_0"]["payer"] = "lending_1"
        field = "inventory.payers.lending_1.chain"
    elif fault == "foreign_asset":
        inventory["payers"]["operator"]["balances"][0]["asset"] = f"{AVALANCHE}/slip44:60"
        field = "inventory.payers.operator.balances.0.asset"
    document = parsed_backtest(value)
    with pytest.raises(ConfigurationError) as error:
        document.require_accounts(parsed(raw).inputs)
    assert error.value.field == field
    assert error.value.file == "backtest.yaml" and error.value.line is not None


def test_editor_schema_accepts_public_payer_declarations():
    value, _ = converted("backtest-economic")
    value["inventory"]["payers"] = {"operator": json.loads(json.dumps(PAYER))}
    value["inventory"]["funds"]["lending_0"]["payer"] = "operator"
    assert validator("backtest.schema.json").is_valid(value)
    value["inventory"]["payers"]["operator"]["chain"] = "eip155"
    assert not validator("backtest.schema.json").is_valid(value)


@pytest.mark.parametrize("fault", ["foreign_chain", "precision"])
def test_frozen_positions_stay_inside_the_retained_resolved_markets(raw, setup, fault):
    from almanak.backtest.inputs import require_positions

    result = resolve(parsed(raw), setup).result
    parameters = expected_parameters(setup)
    require_positions(parameters, result)
    if fault == "foreign_chain":
        foreign = Chain(chain_id=AVALANCHE, environment="mainnet")
        update = {
            "allocations": tuple(
                item.model_copy(
                    update={"reserve": item.reserve.model_copy(update={"chain": foreign})}
                )
                for item in parameters.allocations
            )
        }
    else:
        identity = result.markets[0].resolved.assets[0].identifier
        update = {
            "asset_metadata": {**parameters.asset_metadata, identity: AssetMetadata(decimals=1)}
        }
    with pytest.raises(ValueError, match="frozen positions differ"):
        require_positions(parameters.model_copy(update=update), result)


def test_stale_backtest_resolution_names_the_recovery(tmp_path):
    from almanak.backtest.inputs import stale

    message = stale("retained resolution differs", tmp_path / "out" / "resolution.json")
    assert "fresh --output directory" in message
    assert str(tmp_path / "out") in message


def test_retain_refuses_a_stale_resolution_with_the_recovery(tmp_path, monkeypatch):
    from types import SimpleNamespace

    from almanak.backtest.inputs import BacktestSelection

    path = tmp_path / "out" / "resolution.json"
    path.parent.mkdir()
    path.write_text("{}")
    stale_result = SimpleNamespace(result=SimpleNamespace(facts_digest="old", digest="old"))
    monkeypatch.setattr("almanak.backtest.inputs.read_progress", lambda p: stale_result)
    selection = SimpleNamespace(resolution_facts="new", resolution_identity="new")
    with pytest.raises(ValueError, match="fresh --output directory"):
        BacktestSelection.retain(selection, path)
