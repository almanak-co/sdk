"""The three token implementations match their own verified compilation sources."""

import gzip
import json
from pathlib import Path

import pytest

ROWS = json.loads(
    gzip.decompress(
        (
            Path(__file__).parent / "fixtures/sparklend-token-bytecode-comparisons.json.gz"
        ).read_bytes()
    )
)
VALUES = {
    "POOL": int("c13e21b648a5ee794902342038ff3adab66be987", 16),
    "_addressesProvider": int("02c3ea4e34c0cbd694d2adfa2c690eecbc1793ee", 16),
    "_chainId": 1,
}


@pytest.mark.parametrize("row", ROWS, ids=[row["name"] for row in ROWS])
def test_verified_token_compilation_matches_complete_deployed_bytecode(row):
    expected = bytearray.fromhex(row["compiled_raw"])
    assert {binding["declaration"] for binding in row["bindings"]} == set(VALUES)
    for binding in row["bindings"]:
        assert binding["kind"] == "immutable"
        value = VALUES[binding["declaration"]].to_bytes(32, "big")
        assert binding["value"] == "0x" + value.hex()
        location = binding["reference"]
        assert location["length"] == 32
        offset = location["start"]
        assert expected[offset : offset + 32] == bytes(32)
        expected[offset : offset + 32] = value
    assert expected.hex() == row["compiled_bound"] == row["observed"]
