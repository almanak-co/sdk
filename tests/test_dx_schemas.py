"""Editor schemas and runtime normalization agree on the shipped author inputs."""

import json
from pathlib import Path

import pytest
import yaml
from jsonschema import Draft202012Validator
from referencing import Registry, Resource

ROOT = Path(__file__).parents[1]
SCHEMAS = ROOT / "docs/schemas"


def validator(name):
    registry = Registry().with_resources(
        (path.as_uri(), Resource.from_contents(json.loads(path.read_text())))
        for path in SCHEMAS.rglob("*.json")
    )
    return Draft202012Validator({"$ref": (SCHEMAS / name).as_uri()}, registry=registry)


def author_schema(path):
    if path.name.startswith("backtest"):
        return "backtest.schema.json"
    return "strategy.schema.json"


@pytest.mark.parametrize("path", sorted((ROOT / "strategies").glob("*/*.yaml")))
def test_editor_schema_accepts_current_examples(path):
    validator(author_schema(path)).validate(yaml.safe_load(path.read_text()))


@pytest.mark.parametrize("path", sorted((ROOT / "strategies").glob("*/*.yaml")))
def test_editor_hint_names_the_schema_that_validates_the_file(path):
    hint = path.read_text().splitlines()[0]
    assert "yaml-language-server" in hint, path
    assert hint.endswith("/" + author_schema(path)), hint


def test_composed_backtest_schema_refuses_author_owned_provider_bindings():
    from almanak.configuration.backtest_inputs import parse_backtest
    from almanak.configuration.yaml import ConfigurationError, parse

    path = ROOT / "strategies/base_borrow_and_trade/backtest-economic.yaml"
    raw = yaml.safe_load(path.read_text())
    raw["data"]["history"] = []
    assert not validator("backtest.schema.json").is_valid(raw)
    with pytest.raises(ConfigurationError, match="operator configuration"):
        parse_backtest(parse(yaml.safe_dump(raw)))


def public_reference():
    from tests.test_author_resolution import REFERENCE

    return yaml.safe_load((REFERENCE / "config.yaml").read_text())


def test_public_root_schema_accepts_the_converted_reference():
    raw = public_reference()
    validator("strategy.schema.json").validate(raw)
    raw["strategy"]["gas"] = {
        "payer": {
            "accounts": ["lending_0"],
            "chain": "eip155:1",
            "source": "network_rpc_fee_history",
            "reward_percentile": 50,
            "base_fee_multiplier_bps": 20000,
            "max_gas_price_wei": 5000000000,
            "max_gas_limit": 1000000,
            "max_transaction_fee": "0.005",
            "total_budget": {"base_units": 30000000000000000},
            "exit_reserve": "0.01",
            "gas_limit_buffer_bps": 2500,
        }
    }
    raw["assets"] = {"eip155:1/slip44:60": {"decimals": 18, "symbol": "ETH"}}
    validator("strategy.schema.json").validate(raw)


@pytest.mark.parametrize(
    "mutation",
    ["execution", "connector", "native_asset", "unknown_connector", "chainless", "float_amount"],
)
def test_public_root_schema_refuses_harness_and_contract_errors(mutation):
    raw = public_reference()
    if mutation == "execution":
        raw["execution"] = {"fork": {}}
    elif mutation == "connector":
        raw["strategy"]["connector"] = "aave_v4"
    elif mutation == "native_asset":
        raw["strategy"]["allocations"]["main_weth"]["native_asset"] = "eip155:1/slip44:60"
    elif mutation == "unknown_connector":
        raw["markets"]["main_weth_market"]["connector"] = "aave_v5"
    elif mutation == "chainless":
        del raw["markets"]["main_weth_market"]["chain"]
    else:
        raw["strategy"]["allocations"]["main_weth"]["amount"] = 0.01
    assert not validator("strategy.schema.json").is_valid(raw)


@pytest.mark.parametrize("mutation", ["unknown", "unquoted", "lp_field"])
def test_editor_schema_refuses_author_contract_errors(mutation):
    raw = yaml.safe_load((ROOT / "strategies/uniswap_v4_trading/config.yaml").read_text())
    if mutation == "unknown":
        raw["unknown"] = 1
    elif mutation == "unquoted":
        raw["strategy"]["allocations"]["trading"]["capital"]["quantity"] = 100.5
    else:
        raw["strategy"]["allocations"]["trading"]["collect_seconds"] = "5m"
    assert not validator("strategy.schema.json").is_valid(raw)


@pytest.mark.parametrize("field", ["implementation_revision", "family"])
def test_curve_editor_schema_refuses_author_selected_deployment_facts(field):
    raw = yaml.safe_load((ROOT / "strategies/curve_optimism_tricrypto_lp/config.yaml").read_text())
    raw["markets"]["crypto_pool"][field] = "unverified"
    assert not validator("strategy.schema.json").is_valid(raw)


def test_position_registry_validates_allocations_and_inventory():
    raw = public_reference()
    weth = "eip155:1/erc20:0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"
    raw["inventory"] = [{"account": "lending_0", "balance": {"asset": weth, "quantity": "0.5"}}]
    validator("strategy.schema.json").validate(raw)
    supply = raw["strategy"]["allocations"]["main_weth"]
    assert validator("positions.schema.json").is_valid(supply)
    assert not validator("positions.schema.json").is_valid({**supply, "range": None})
    debt = raw["strategy"]["debts"]["main_usdc"]
    assert validator("positions/aave_v4_debt.schema.json").is_valid(debt)
    raw["inventory"][0]["balance"]["quantity"] = 0.5
    assert not validator("strategy.schema.json").is_valid(raw)


UNISWAP_MARKET = {
    "connector": "uniswap_v3",
    "chain": "eip155:8453",
    "pool": "0xd0b53d9277642d899df5c87a3966a349a798f224",
    "assertions": {"fee": 500, "tick_spacing": 10, "decimals0": 18, "decimals1": 6},
}
UNISWAP_POSITION = {
    "market": "pool",
    "account": "fund",
    "capital": {
        "asset": "eip155:8453/erc20:0x833589fcd6edb6e08f4c7c32d4f71b54bda02913",
        "quantity": {"base_units": 100000000},
    },
    "entry_swap_bps": 10000,
    "hold_seconds": "2h",
    "slippage_bps": 100,
    "deadline_seconds": 300,
    "minimum_native_balance": {"base_units": 0},
}


def uniswap_root():
    raw = public_reference()
    raw["markets"] = {"pool": UNISWAP_MARKET}
    raw["accounts"] = {"fund": {"chain": "eip155:8453"}}
    raw["assets"] = {}
    raw["strategy"] = {"decision_interval": "5s", "allocations": {"position": UNISWAP_POSITION}}
    return json.loads(json.dumps(raw))


def test_market_and_position_registries_validate_uniswap_v3_through_the_registry():
    validator("markets.schema.json").validate({"pool": UNISWAP_MARKET})
    validator("positions.schema.json").validate(UNISWAP_POSITION)
    validator("strategy.schema.json").validate(uniswap_root())
    lp = {**UNISWAP_POSITION, "range": {"lower": -198530, "upper": -197530}}
    assert validator("positions.schema.json").is_valid(lp)


@pytest.mark.parametrize(
    "mutation",
    [
        "foreign_key",
        "retired_budget",
        "unknown_connector",
        "poolless",
        "spent_amount",
        "float_tick",
    ],
)
def test_registry_refuses_foreign_uniswap_v3_keys(mutation):
    raw = uniswap_root()
    market, position = raw["markets"]["pool"], raw["strategy"]["allocations"]["position"]
    if mutation == "foreign_key":
        position["native_asset"] = "eip155:8453/slip44:60"
    elif mutation == "retired_budget":
        # Grants are scope-only (blueprint 27); the per-pool spending maxima are gone.
        position["maximum1"] = {"asset": position["capital"]["asset"], "quantity": "1000"}
    elif mutation == "unknown_connector":
        market["connector"] = "uniswap_v5"
    elif mutation == "poolless":
        del market["pool"]
    elif mutation == "spent_amount":
        position["capital"] = {"base_units": 100000000}
    else:
        position["range"] = {"lower": -198530.5, "upper": -197530}
    assert not validator("strategy.schema.json").is_valid(raw)


@pytest.mark.parametrize(
    "band",
    [
        {"lower": -200120, "upper": -196120, "width_bps": 500},
        {"lower": -200120, "upper": -196120, "width": 980},
    ],
)
def test_position_schemas_accept_one_band_width(band):
    banded = {**UNISWAP_POSITION, "band": band}
    assert validator("positions.schema.json").is_valid(banded)
    both = {**UNISWAP_POSITION, "band": {**band, "width": 980, "width_bps": 500}}
    assert not validator("positions.schema.json").is_valid(both)
    assert not validator("positions.schema.json").is_valid(
        {**UNISWAP_POSITION, "band": {"lower": -200120, "upper": -196120}}
    )


@pytest.mark.parametrize("field", ["hold_seconds", "deadline_seconds"])
def test_a_zero_hold_or_deadline_is_refused_before_resolution(tmp_path, field):
    from almanak.configuration.unresolved import read_inputs
    from almanak.configuration.yaml import ConfigurationError

    raw = yaml.safe_load((ROOT / "strategies/uniswap_v3_base_trading/config.yaml").read_text())
    (allocation,) = raw["strategy"]["allocations"].values()
    allocation[field] = "0s"
    assert not validator("strategy.schema.json").is_valid(raw)
    path = tmp_path / "config.yaml"
    path.write_text(yaml.safe_dump(raw))
    with pytest.raises(ConfigurationError) as refused:
        read_inputs(path)
    assert refused.value.field == f"strategy.allocations.position.{field}"
