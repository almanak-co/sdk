"""A test run's `run_metadata` line (run_facts.py)."""

import json
from types import SimpleNamespace

from almanak import run_facts
from almanak.caip import AssetType
from almanak.models import AssetMetadata

USDC = "eip155:42161/erc20:0xaf88d065e77c8cc2239327c5edb3a432268e5831"
NATIVE = "eip155:42161/slip44:60"


def allocation(name, module):
    kind = type("Allocation", (), {"__module__": module})
    made = kind()
    made.name = name
    return made


class Config:
    asset_metadata = {
        AssetType(USDC): AssetMetadata(decimals=6, symbol="USDC"),
        AssetType(NATIVE): AssetMetadata(decimals=18),
    }
    allocations = (
        allocation("collateral", "almanak.connectors.aave_v3.models"),
        allocation("liquidity", "almanak.connectors.uniswap_v3.models"),
        allocation("odd", "somewhere.else"),
    )


def test_assets_and_venues_come_from_the_resolved_config():
    assert run_facts.facts(Config()) == {
        "event": "run_metadata",
        "assets": {USDC: {"symbol": "USDC", "decimals": 6}, NATIVE: {"decimals": 18}},
        "venues": {"collateral": "aave_v3", "liquidity": "uniswap_v3"},
    }


def test_the_line_goes_to_stderr_and_never_raises(capsys):
    run_facts.log(SimpleNamespace(config=Config()))
    assert json.loads(capsys.readouterr().err)["event"] == "run_metadata"
    run_facts.log(object())  # a state without a config: no line, not an error
    assert capsys.readouterr().err == ""
    run_facts.log(SimpleNamespace(config=object()))  # no metadata or allocations: an empty line
    assert json.loads(capsys.readouterr().err) == {
        "event": "run_metadata",
        "assets": {},
        "venues": {},
    }
