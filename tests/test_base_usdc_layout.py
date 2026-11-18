"""Independent compiler comparison for the Base-only USDC fixture revision."""

import gzip
import json
from pathlib import Path

import pytest
from eth_utils import keccak


def captured():
    path = Path(__file__).parent / "fixtures/base-usdc-layout-reads.json.gz"
    return json.loads(gzip.decompress(path.read_bytes()))


def executable(data):
    # The compiler emits CBOR metadata followed by its two-byte length.
    return data[: -(int(data[-4:], 16) + 2) * 2]


@pytest.mark.parametrize(
    "name,field",
    [("FiatTokenProxy", "proxy"), ("FiatTokenV2_2", "code"), ("SignatureChecker", "library")],
)
def test_deployed_code_matches_compiler_with_only_declared_link_slots(name, field):
    raw = captured()
    base = raw["base"]
    compiled = raw["compiled"][name]["deployedBytecode"]
    candidate = compiled["object"]
    actual = base[field] if field != "library" else base["library"]["code"]
    library = base["library"]["address"][2:]
    for links in compiled["linkReferences"].values():
        for references in links.values():
            for reference in references:
                start = 2 + reference["start"] * 2
                stop = start + reference["length"] * 2
                assert actual[start:stop] == library
                candidate = candidate[:start] + actual[start:stop] + candidate[stop:]
    if name == "SignatureChecker":
        assert actual[:4] == "0x73" and actual[4:44] == library
        candidate = candidate[:4] + actual[4:44] + candidate[44:]
    assert executable(candidate) == executable(actual)


def test_fixture_manifest_binds_verified_code_and_compiler_balance_slot():
    raw = captured()
    identity = raw["manifest"]["contracts"][0]
    base = raw["base"]
    assert identity["code_hash"] == "0x" + keccak(bytes.fromhex(base["proxy"][2:])).hex()
    assert identity["implementation_hash"] == "0x" + keccak(bytes.fromhex(base["code"][2:])).hex()
    layout = raw["compiled"]["FiatTokenV2_2"]["storageLayout"]
    balance = next(s for s in layout["storage"] if s["label"] == "balanceAndBlacklistStates")
    assert balance["slot"] == "9" and balance["offset"] == 0
    assert layout["types"][balance["type"]]["label"] == "mapping(address => uint256)"
