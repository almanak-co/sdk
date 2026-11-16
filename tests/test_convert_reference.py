"""The conversion script reproduces the hand-converted Aave reference and its operator files."""

import shutil
from pathlib import Path

import pytest
import yaml

from scripts import convert_reference
from tests.test_author_resolution import LEGACY, REFERENCE, ROOT

EXAMPLES = ROOT / "examples/operators"


@pytest.fixture
def converted(tmp_path):
    directory = tmp_path / "strategies" / REFERENCE.name
    directory.mkdir(parents=True)
    for path in LEGACY.glob("*.yaml"):
        shutil.copyfile(path, directory / path.name)
    (directory / "strategy.py").write_bytes((REFERENCE / "strategy.py").read_bytes())
    operators = tmp_path / "examples/operators"
    assert convert_reference.main([str(directory), "--operators", str(operators)]) == 0
    return directory, operators


def loaded(path: Path):
    return yaml.safe_load(path.read_text())


def live(document: dict) -> dict:
    """The converter keeps the legacy 5s cadence; shipped references decide every 60s."""
    strategy = document.get("strategy") or {}
    if strategy.get("decision_interval") == "5s":
        strategy["decision_interval"] = "60s"
    return document


def without_added_parameters(document: dict) -> dict:
    """The shipped reference's knobs moved from strategy constants into `strategy.parameters`
    after conversion; the converter has no constants to read them from."""
    (document.get("strategy") or {}).pop("parameters", None)
    return document


def without_example_gas(document: dict) -> dict:
    """The shipped config.yaml and its fork operator gained an example gas policy and its
    ceilings after conversion; the legacy config states none."""
    (document.get("strategy") or {}).pop("gas", None)
    (document.get("fork") or {}).pop("gas", None)
    return document


@pytest.mark.parametrize(
    "name",
    ["config.yaml", "config.production.yaml", "backtest.yaml", "backtest-economic.yaml"],
)
def test_author_files_match_the_shipped_public_root(converted, name):
    directory, _ = converted
    produced = directory / name
    shipped = loaded(REFERENCE / name)
    if name in ("config.yaml", "config.production.yaml"):
        shipped = without_added_parameters(shipped)
    if name == "config.yaml":
        shipped = without_example_gas(shipped)
    assert live(loaded(produced)) == shipped
    schema = "backtest" if name.startswith("backtest") else "strategy"
    assert produced.read_text().splitlines()[0].endswith(f"/{schema}.schema.json")


@pytest.mark.parametrize("mode", ["fork", "production"])
def test_operator_examples_match_the_shipped_examples(converted, mode):
    _, operators = converted
    name = f"{REFERENCE.name}.{mode}.yaml"
    shipped = loaded(EXAMPLES / name)
    assert loaded(operators / name) == (without_example_gas(shipped) if mode == "fork" else shipped)


def test_public_roots_and_unknown_market_shapes_are_refused(converted, tmp_path):
    directory, operators = converted
    with pytest.raises(ValueError, match="already a public author root"):
        convert_reference.convert_directory(directory, operators)
    raw = loaded(LEGACY / "config.yaml")
    market = dict(raw["markets"]["main_weth_market"])
    with pytest.raises(ValueError, match="no public market shape yet"):
        convert_reference.flatten_market("main_weth_market", {**market, "kind": "uniswap_v2"})
    foreign = {**market, "chain": {**market["chain"], "environment": "anvil:1"}}
    with pytest.raises(ValueError, match="only mainnet chains"):
        convert_reference.flatten_market("main_weth_market", foreign)
    assert set(convert_reference.used_markets(raw)) == set(raw["markets"])
    raw["strategy"]["debts"] = {}
    assert "main_usdc_market" not in convert_reference.used_markets(raw)


# The unconverted market and trading position of strategies/uniswap_v3_base_trading,
# kept here because that reference now ships the public root.
POOL_MARKET = {
    "pool": {
        "kind": "uniswap_v3",
        "chain": {"chain_id": "eip155:8453", "environment": "mainnet"},
        "factory": "0x33128a8fc17869897dce68ed026d694621f6fdfd",
        "pool": "0xd0b53d9277642d899df5c87a3966a349a798f224",
        "currency0": "eip155:8453/erc20:0x4200000000000000000000000000000000000006",
        "currency1": "eip155:8453/erc20:0x833589fcd6edb6e08f4c7c32d4f71b54bda02913",
        "fee": 500,
        "tick_spacing": 10,
    },
    "contracts": {
        "router": "0x2626664c2603336e57b271c5c0b26f421741e481",
        "position_manager": "0x03a520b32c04bf3beef7beb72e919cf822ed34f1",
        "quoter": "0x3d4e44eb1374240ce5f1b871ab261cd16335b76a",
    },
}
POOL_POSITION = {
    "market": "pool",
    "account": "fund",
    "range": None,
    "capital": {
        "asset": "eip155:8453/erc20:0x833589fcd6edb6e08f4c7c32d4f71b54bda02913",
        "quantity": {"base_units": 100000000},
    },
    "entry_swap_bps": 10000,
    "hold_seconds": "2h",
    "slippage_bps": 100,
    "deadline_seconds": 300,
    "native_asset": "eip155:8453/slip44:60",
    "minimum_native_balance": {"base_units": 0},
}


def test_pool_markets_select_the_pool_only():
    assert convert_reference.market_kind(POOL_MARKET) == "uniswap_v3"
    assert convert_reference.flatten_market("pool", POOL_MARKET) == {
        "connector": "uniswap_v3",
        "chain": "eip155:8453",
        "pool": "0xd0b53d9277642d899df5c87a3966a349a798f224",
    }
    position = convert_reference.convert_position(POOL_POSITION, "uniswap_v3")
    assert "range" not in position and "native_asset" not in position
    assert position["capital"]["quantity"] == {"base_units": 100000000}
    assert (
        convert_reference.flatten_market("pool", POOL_MARKET)
        == loaded(ROOT / "strategies/uniswap_v3_base_trading/config.yaml")["markets"]["pool"]
    )


def test_a_file_with_fork_and_production_blocks_yields_both_operator_examples(tmp_path):
    name = "aave_v4_multichain_borrowing"
    directory = tmp_path / "strategies" / name
    directory.mkdir(parents=True)
    frozen = ROOT / "tests/fixtures/author/legacy" / name
    for path in frozen.glob("*.yaml"):
        shutil.copyfile(path, directory / path.name)
    operators = tmp_path / "examples/operators"
    assert convert_reference.main([str(directory), "--operators", str(operators)]) == 0
    generated_fork = loaded(operators / f"{name}.fork.yaml")
    shipped_fork = loaded(EXAMPLES / f"{name}.fork.yaml")
    assert "gas" not in generated_fork["fork"]
    assert generated_fork["fork"] == {
        key: value for key, value in shipped_fork["fork"].items() if key != "gas"
    }
    assert loaded(operators / f"{name}.production.yaml") == loaded(
        EXAMPLES / f"{name}.production.yaml"
    )
    assert live(loaded(directory / "config.yaml")) == without_added_parameters(
        loaded(ROOT / "strategies" / name / "config.yaml")
    )


# The unconverted market and liquidity position of strategies/uniswap_v4_lp,
# kept here because that reference now ships the public root.
POOL_KEY_MARKET = {
    "pool": {
        "kind": "uniswap_v4",
        "chain": {"chain_id": "eip155:1", "environment": "mainnet"},
        "pool_manager": "0x000000000004444c5dc75cb358380d2e3de08a90",
        "state_view": "0x7ffe42c4a5deea5b0fec41c94c136cf115597227",
        "currency0": "eip155:1/erc20:0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
        "currency1": "eip155:1/erc20:0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2",
        "fee": 500,
        "tick_spacing": 10,
        "hooks": "0x0000000000000000000000000000000000000000",
    },
    "contracts": {
        "router": "0x66a9893cc07d91d95644aedd05d03f95e1dba8af",
        "position_manager": "0xbd216513d74c8cf14cf4747e6aaa6420ff64ee9e",
        "quoter": "0x52f0e24d1c21c8a0cb1e5a5dd6198556bd9e1203",
        "permit2": "0x000000000022d473030f116ddee9f6b43ac78ba3",
    },
}


def test_pool_key_markets_select_the_five_key_members_only():
    assert convert_reference.market_kind(POOL_KEY_MARKET) == "uniswap_v4"
    flattened = convert_reference.flatten_market("lp_market", POOL_KEY_MARKET)
    assert flattened == {
        "connector": "uniswap_v4",
        "chain": "eip155:1",
        "currency0": "eip155:1/erc20:0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
        "currency1": "eip155:1/erc20:0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2",
        "fee": 500,
        "tick_spacing": 10,
        "hooks": "0x0000000000000000000000000000000000000000",
    }
    shipped = loaded(ROOT / "strategies/uniswap_v4_lp/config.yaml")["markets"]["lp_market"]
    assert flattened == shipped


# The unconverted market and perpetual position of strategies/gmx_v2_arbitrum_position,
# kept here because that reference now ships the public root.
GM_MARKET = {
    "market": {
        "kind": "gmx_v2",
        "chain": {"chain_id": "eip155:42161", "environment": "mainnet"},
        "share_asset": "eip155:42161/erc20:0x70d95587d40a2caf56bd97485ab3eec10bee6336",
        "index_asset": "eip155:42161/erc20:0x82af49447d8a07e3bd95bd0d56f35241523fbab1",
        "long_asset": "eip155:42161/erc20:0x82af49447d8a07e3bd95bd0d56f35241523fbab1",
        "short_asset": "eip155:42161/erc20:0xaf88d065e77c8cc2239327c5edb3a432268e5831",
        "wrapped_native": "eip155:42161/erc20:0x82af49447d8a07e3bd95bd0d56f35241523fbab1",
        "contracts": {"reader": "0x470fbc46bcc0f16532691df360a07d8bf5ee0789"},
    }
}
GM_POSITION = {
    "market": "weth_usdc",
    "account": "fund",
    "side": "long",
    "native_asset": "eip155:42161/slip44:60",
    "minimum_native_balance": {"base_units": 0},
}


def test_gm_markets_select_the_share_token_and_positions_gain_their_kind():
    assert convert_reference.market_kind(GM_MARKET) == "gmx_v2"
    flattened = convert_reference.flatten_market("weth_usdc", GM_MARKET)
    assert flattened == {
        "connector": "gmx_v2",
        "chain": "eip155:42161",
        "market": "eip155:42161/erc20:0x70d95587d40a2caf56bd97485ab3eec10bee6336",
    }
    shipped = loaded(ROOT / "strategies/gmx_v2_arbitrum_position/config.yaml")
    assert flattened == shipped["markets"]["weth_usdc"]
    position = convert_reference.convert_position(GM_POSITION, "gmx_v2")
    assert position["kind"] == "gmx_position" and "native_asset" not in position
    swap = convert_reference.convert_position({**GM_POSITION, "kind": "gmx_swap"}, "gmx_v2")
    assert swap["kind"] == "gmx_swap"
