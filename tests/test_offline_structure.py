"""Structural rules the offline author check enforces without resolution."""

import importlib
import inspect
import json
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
import yaml

from almanak.checks import CheckFailure, check
from almanak.configuration.unresolved import parse_inputs
from almanak.configuration.yaml import ConfigurationError, load, parse
from almanak.connectors.composition import CONNECTOR_IDS, installed_connectors
from almanak.connectors.support import (
    ONE_ALLOCATION_PER_FUND,
    ONE_ALLOCATION_PER_FUND_CONNECTORS,
)

ROOT = Path(__file__).resolve().parents[1]
COMPOSED = ROOT / "strategies/arbitrum_aave_borrow_and_trade"
WETH = "eip155:42161/erc20:0x82af49447d8a07e3bd95bd0d56f35241523fbab1"
SECOND_POOL = "0xc31e54c7a869b9fcbecc14363cf510d1c41fa443"


def composed():
    return json.loads(json.dumps(load(COMPOSED / "config.yaml").value))


def dumped(raw):
    return yaml.safe_dump(json.loads(json.dumps(raw)), sort_keys=False)


def parsed(raw):
    return parse_inputs(parse(dumped(raw), file="config.yaml"))


def checked_offline(tmp_path, raw):
    (tmp_path / "strategy.py").write_bytes((COMPOSED / "strategy.py").read_bytes())
    (tmp_path / "config.yaml").write_text(dumped(raw))
    return check(SimpleNamespace(directory=tmp_path, config=None, strategy=None))


def with_second_pool(raw, account="fund"):
    raw["markets"]["second_pool"] = {**raw["markets"]["pool"], "pool": SECOND_POOL}
    raw["strategy"]["allocations"]["second_trade"] = {
        **raw["strategy"]["allocations"]["trade"],
        "market": "second_pool",
        "account": account,
    }
    return raw


def test_offline_check_refuses_a_second_v3_allocation_on_one_fund(tmp_path):
    with pytest.raises(CheckFailure) as failure:
        checked_offline(tmp_path, with_second_pool(composed()))
    detail = failure.value.diagnostic()
    assert detail["code"] == "one_allocation_per_fund"
    assert detail["field"] == "strategy.allocations.second_trade.account"
    assert "'trade' and 'second_trade'" in detail["message"]
    assert detail["stages"][0] == {
        "stage": "configuration",
        "status": "failed",
        "message": detail["message"],
    }


@pytest.mark.parametrize("connector", sorted(ONE_ALLOCATION_PER_FUND_CONNECTORS))
def test_each_dex_connector_takes_one_allocation_per_fund(connector):
    from almanak.configuration.unresolved import require_one_allocation_per_fund

    position = SimpleNamespace(market="pool", account="fund")
    inputs: Any = SimpleNamespace(
        markets={"pool": SimpleNamespace(connector=connector)},
        strategy=SimpleNamespace(allocations={"a": position, "b": position}),
    )
    with pytest.raises(ConfigurationError, match=f"{connector} takes one allocation per fund"):
        require_one_allocation_per_fund(inputs)
    inputs.strategy.allocations["b"] = SimpleNamespace(market="pool", account="other")
    require_one_allocation_per_fund(inputs)


def test_lending_allocations_may_share_a_fund():
    raw = composed()
    assert {raw["strategy"]["allocations"][n]["account"] for n in ("collateral", "loan")} == {
        "fund"
    }
    parsed(raw)


def test_offline_check_refuses_composed_inventory_missing_a_named_asset(tmp_path):
    raw = composed()
    raw["inventory"] = [item for item in raw["inventory"] if item["balance"]["asset"] != WETH]
    with pytest.raises(CheckFailure) as failure:
        checked_offline(tmp_path, raw)
    detail = failure.value.diagnostic()
    assert detail["code"] == "composed_inventory_incomplete"
    assert detail["field"] == "inventory"
    assert f"'collateral' uses {WETH} in fund 'fund'" in detail["message"]
    raw["inventory"].append({"account": "fund", "balance": {"asset": WETH, "quantity": "0"}})
    assert checked_offline(tmp_path, raw)["valid"]


def per_fund_rule(identity) -> bool:
    module = importlib.import_module(f"almanak.connectors.{identity}.models")
    source = inspect.getsource(module)
    return "allocation per fund" in source or "allocation per named fund" in source


@pytest.mark.parametrize("identity", [str(i) for i in CONNECTOR_IDS])
def test_the_offline_rule_matches_each_connectors_parameters(identity):
    assert per_fund_rule(identity) == (identity in ONE_ALLOCATION_PER_FUND_CONNECTORS)


def test_support_lists_the_rule_on_every_declared_chain():
    for identity, connector in installed_connectors():
        if str(identity) not in ONE_ALLOCATION_PER_FUND_CONNECTORS:
            continue
        for entry in connector.chain_support:
            assert any(ONE_ALLOCATION_PER_FUND in item for item in entry.restrictions), (
                identity,
                entry.chain,
            )
