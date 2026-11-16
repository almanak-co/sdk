"""Compiled verified Pool and linked libraries match their retained deployed executables."""

import gzip
import json
from pathlib import Path

import pytest

ROWS = json.loads(
    gzip.decompress(
        (
            Path(__file__).parent / "fixtures/sparklend-pool-bytecode-comparisons.json.gz"
        ).read_bytes()
    )
)


def executable(code):
    # The only excluded bytes are this compiler's IPFS/solc CBOR metadata map.
    assert code[-53:-43] == bytes.fromhex("a2646970667358221220")
    assert code[-11:] == bytes.fromhex("64736f6c634300080a0033")
    return code[:-53]


@pytest.mark.parametrize("row", ROWS, ids=[row["name"] for row in ROWS])
def test_pool_and_library_executables_match_with_declared_bindings(row):
    expected = bytearray.fromhex(row["compiled_raw"])
    observed = bytes.fromhex(row["observed"])
    for binding in row["bindings"]:
        apply_binding(expected, binding, row)
    assert expected.hex() == row["compiled_bound"]
    assert executable(expected) == executable(observed)


def apply_binding(expected, binding, row):
    if binding["kind"] == "immutable":
        assert binding["declaration"] == "ADDRESSES_PROVIDER" and row["name"] == "Pool"
        value = bytes.fromhex("02c3ea4e34c0cbd694d2adfa2c690eecbc1793ee").rjust(32, b"\0")
        assert binding["value"] == "0x" + value.hex()
        location = binding["reference"]
        assert location["length"] == 32
        expected[location["start"] : location["start"] + 32] = value
    else:
        assert binding["kind"] == "compiler_library_self_address_guard"
        assert binding["value"] == row["address"]
        assert expected[:23] == b"\x73" + bytes(20) + b"\x30\x14"
        expected[1:21] = bytes.fromhex(row["address"][2:])
