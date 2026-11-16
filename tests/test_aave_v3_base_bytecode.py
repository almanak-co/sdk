"""Independent Base compilation comparisons for the V3 revision-11 reader."""

import gzip
import json
import re
from pathlib import Path

import pytest
from eth_utils import keccak


def captured(network="base"):
    path = Path(__file__).parent / f"fixtures/aave-v3-{network}-compiled-comparisons.json.gz"
    return json.loads(gzip.decompress(path.read_bytes()))


def clear_locations(expected, observed, locations, value):
    for location in locations:
        start, length = location["start"], location["length"]
        assert observed[start : start + length].hex() == value
        expected[start : start + length] = observed[start : start + length] = bytes(length)


@pytest.mark.parametrize(
    ("network", "name"),
    [
        (network, name)
        for network in ("base", "optimism", "bnb", "polygon", "avalanche")
        for name in captured(network)["comparisons"]
    ],
)
def test_l2_executables_match_compiled_revision_with_exact_declared_bindings(name, network):
    rows = captured(network)["comparisons"]
    row = rows[name]
    bytecode = row["compiled_bytecode"]
    expected = bytearray.fromhex(
        re.sub(r"__\$[a-f0-9]{34}\$__", "00" * 20, bytecode["object"].removeprefix("0x"))
    )
    observed = bytearray.fromhex(row["observed_code"][2:])
    assert "0x" + keccak(observed).hex() == row["code_hash"]
    for identifier, locations in bytecode.get("immutableReferences", {}).items():
        clear_locations(expected, observed, locations, row["immutables"][identifier])
    for libraries in bytecode["linkReferences"].values():
        for library, locations in libraries.items():
            target = row["libraries"][library]
            assert target == rows[library]["address"]
            clear_locations(expected, observed, locations, target[2:])
    if name.endswith("Logic"):
        assert observed[0] == expected[0] == 0x73
        assert observed[1:21] == bytes.fromhex(row["address"][2:])
        expected[1:21] = observed[1:21] = bytes(20)
    assert observed == expected


def test_immutables_bind_the_base_pool_provider_treasury_and_rewards():
    raw = captured()
    bindings = {
        "ADDRESSES_PROVIDER": "e20fcbdbffc4dd138ce8b2e6fbb6cb49777ad64d",
        "_addressesProvider": "e20fcbdbffc4dd138ce8b2e6fbb6cb49777ad64d",
        "POOL": "a238dd80c259a72e81d7e4664a9801593f98d1c5",
        "RESERVE_INTEREST_RATE_STRATEGY": "86ab1c62a8bf868e1b3e1ab87d587aba6fbcbdc5",
        "TREASURY": "ba9424d650a4f5c80a0da641254d1acce2a37057",
        "REWARDS_CONTROLLER": "f9cc4f0d883f1a1eb2c253bdb46c254ca51e1f44",
        "_chainId": "2105",
    }
    for row in raw["comparisons"].values():
        for identifier, value in row["immutables"].items():
            assert value == bindings[raw["immutable_meanings"][identifier]].rjust(64, "0")
