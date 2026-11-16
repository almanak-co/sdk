"""The converted reference reproduces the harness hook contracts exactly."""

import json

import pytest
import yaml

from almanak.configuration.yaml import load
from scripts import capture_author_hooks as collector
from scripts import compare_reference_conversion as comparison
from tests.test_author_hook_capture import placeholder_accounts
from tests.test_author_resolution import LEGACY, REFERENCE, frozen_cadence, parsed, resolve
from tests.test_author_resolution import archive as archive
from tests.test_author_resolution import rpc as rpc
from tests.test_author_resolution import setup as setup
from tests.test_resolution_context import revalidate


@pytest.fixture
def captures(tmp_path, monkeypatch, setup):
    monkeypatch.setattr(collector, "source_revision", lambda _: "component-test")
    strategy = tmp_path / "strategy.py"
    strategy.write_bytes((REFERENCE / "strategy.py").read_bytes())
    # The frozen harness input predates strategy.parameters; it gains the reference's knobs,
    # which equal the strategy constants it was captured with.
    legacy = load(LEGACY / "config.yaml").value
    knobs = load(REFERENCE / "config.yaml").value["strategy"]["parameters"]
    legacy["strategy"]["parameters"] = knobs
    harness = tmp_path / "harness.yaml"
    harness.write_text(yaml.safe_dump(legacy, sort_keys=False))
    accounts = tmp_path / "accounts.json"
    accounts.write_text(placeholder_accounts().model_dump_json())
    before = collector.capture(harness, strategy, accounts)
    setup.operator = revalidate(setup.operator, accounts=placeholder_accounts())
    raw = frozen_cadence(load(REFERENCE / "config.yaml").value)
    public = tmp_path / "config.yaml"
    public.write_text(yaml.safe_dump(raw, sort_keys=False))
    resolution = tmp_path / "resolution.json"
    resolution.write_text(resolve(parsed(raw), setup).model_dump_json())
    after = collector.capture_resolved(public, strategy, resolution)
    return before, after, raw, public, strategy, resolution, setup


def test_public_root_hooks_equal_the_harness_hooks(captures, tmp_path):
    before, after, *_ = captures
    result = comparison.compare(before, after)
    assert result["equal"], json.dumps(result["groups"], indent=1)
    assert set(result["groups"]) == {"economics", "targets", "capabilities", "dependencies"}
    before_path, after_path = tmp_path / "before.json", tmp_path / "after.json"
    before_path.write_text(json.dumps(before))
    after_path.write_text(json.dumps(after))
    output = tmp_path / "comparison.json"
    assert (
        comparison.main(
            ["--before", str(before_path), "--after", str(after_path), "--output", str(output)]
        )
        == 0
    )
    assert json.loads(output.read_text())["equal"]


def test_changed_economics_or_targets_are_reported(captures, tmp_path):
    before, after, raw, public, strategy, resolution, setup = captures
    raw["strategy"]["allocations"]["main_weth"]["capital"] = "0.02"
    public.write_text(yaml.safe_dump(raw, sort_keys=False))
    resolution.write_text(resolve(parsed(raw), setup).model_dump_json())
    changed = collector.capture_resolved(public, strategy, resolution)
    result = comparison.compare(before, changed)
    assert not result["equal"] and not result["groups"]["economics"]["equal"]
    paths = {d["path"] for d in result["groups"]["economics"]["differences"]}
    assert paths == {"positions.main_weth.capital"}
    assert result["groups"]["targets"]["equal"]
    swapped = json.loads(json.dumps(after))
    swapped["bound_parameters"]["allocations"][0]["reserve"]["spoke"] = "0x" + "ab" * 20
    result = comparison.compare(before, swapped)
    assert not result["groups"]["targets"]["equal"] and result["groups"]["economics"]["equal"]


BINDING = {"name": "fund_0", "profile": {"fund": {"address": "0x" + "11" * 20}}}


def synthetic(allocations, **parameters):
    """The smallest capture shape compare reads: no fixtures, no connector imports."""
    return {
        "source_revision": "synthetic",
        "bound_parameters": {
            "decision_trigger": {"kind": "interval", "seconds": 5},
            "accounts": {"bindings": [BINDING]},
            "allocations": allocations,
            **parameters,
        },
        "capabilities": {"scopes": []},
        "dependencies": {"requests": []},
    }


def pool_position(**overrides):
    return {
        "name": "weth_usdc",
        "account": "fund_0",
        "pool": {"chain": "eip155:1", "address": "0x" + "22" * 20, "fee": 500},
        "contracts": {"manager": "0x" + "33" * 20},
        "range": {"lower": -100, "upper": 100},
        "capital": {"asset": "eip155:1/slip44:60", "amount": "1"},
        "hold_seconds": 300,
        **overrides,
    }


def paths(result, group):
    return {d["path"] for d in result["groups"][group]["differences"]}


def test_identical_pool_captures_compare_equal():
    result = comparison.compare(synthetic([pool_position()]), synthetic([pool_position()]))
    assert result["equal"]


def test_pool_sub_object_changes_are_targets():
    pool = {**pool_position()["pool"], "fee": 3000}
    result = comparison.compare(synthetic([pool_position()]), synthetic([pool_position(pool=pool)]))
    assert paths(result, "targets") == {"positions.weth_usdc.pool.fee"}
    assert result["groups"]["economics"]["equal"]


def test_range_and_capital_changes_are_economics():
    result = comparison.compare(
        synthetic([pool_position()]),
        synthetic(
            [
                pool_position(
                    range={"lower": -100, "upper": 200},
                    capital={"asset": "eip155:1/slip44:60", "amount": "2"},
                )
            ]
        ),
    )
    assert paths(result, "economics") == {
        "positions.weth_usdc.range.upper",
        "positions.weth_usdc.capital.amount",
    }
    assert result["groups"]["targets"]["equal"]


def test_market_named_sub_objects_are_targets():
    position = {
        "name": "eth_perp",
        "account": "fund_0",
        "market": {"chain": "eip155:42161", "market_token": "0x" + "44" * 20},
        "index_pool": {"address": "0x" + "55" * 20},
        "side": "long",
    }
    changed = {
        **position,
        "market": {**position["market"], "market_token": "0x" + "66" * 20},
        "index_pool": {"address": "0x" + "77" * 20},
    }
    result = comparison.compare(synthetic([position]), synthetic([changed]))
    assert paths(result, "targets") == {
        "positions.eth_perp.market.market_token",
        "positions.eth_perp.index_pool.address",
    }
    assert result["groups"]["economics"]["equal"]


def test_inventory_and_gas_are_economics_of_a_composed_capture():
    gas = [{"account": "fund_0", "payer": "treasury"}]
    inventory = [{"key": {"account": "fund_0", "asset": "eip155:1/slip44:60"}, "amount": "1"}]
    before = synthetic([pool_position()], gas=gas, inventory=inventory)
    assert comparison.compare(before, synthetic([pool_position()], gas=gas, inventory=inventory))[
        "equal"
    ]
    after = synthetic(
        [pool_position()],
        inventory=[{**inventory[0], "amount": "2"}],
    )
    result = comparison.compare(before, after)
    assert paths(result, "economics") == {"gas", "inventory"}
    assert result["groups"]["targets"]["equal"]


def test_account_and_name_are_never_economic_paths():
    position = pool_position()
    economics = comparison.economics(synthetic([position])["bound_parameters"])
    assert set(economics["positions"]) == {"weth_usdc"}
    assert not {"name", "account"} & set(economics["positions"]["weth_usdc"])
    result = comparison.compare(
        synthetic([position]),
        synthetic([pool_position(account="fund_1", hold_seconds=600)]),
    )
    assert paths(result, "economics") == {"positions.weth_usdc.hold_seconds"}
