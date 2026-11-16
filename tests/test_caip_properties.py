"""Generated CAIP strings: every accepted identity survives its own wire form.

Published vectors and named refusals stay in test_caip.py. Here any string either
parses into an identifier whose text parses back to an equal identifier, or is
refused with ValueError; EVM contract references canonicalize to lowercase once.
"""

import json

import pytest
from hypothesis import given
from hypothesis import strategies as st
from pydantic import TypeAdapter

from almanak.caip import ASSET_ID, ASSET_TYPE, CHAIN, AssetId, AssetType, ChainId

KINDS = ((ChainId, CHAIN), (AssetType, ASSET_TYPE), (AssetId, ASSET_ID))
hexadecimal = st.text("0123456789abcdefABCDEF", min_size=40, max_size=40)
uint256 = st.integers(0, 2**256 - 1)


def parsed_or_refused(kind, wire):
    try:
        return kind(wire)
    except ValueError:
        return None


def canonical_wire(wire):
    """The wire form unchanged, except an EVM ERC contract reference in lowercase."""
    parts = wire.split("/")
    if len(parts) < 2 or not parts[0].startswith("eip155:"):
        return wire
    namespace, reference = parts[1].split(":", 1)
    if namespace.startswith("erc"):
        parts[1] = f"{namespace}:{reference.lower()}"
    return "/".join(parts)


def assert_wire_round_trip(value):
    kind = type(value)
    assert kind(str(value)) == value
    adapter = TypeAdapter(kind)
    assert adapter.validate_json(adapter.dump_json(value)) == value
    assert adapter.dump_json(value) == json.dumps(str(value)).encode()


@pytest.mark.parametrize(("kind", "pattern"), KINDS, ids=lambda k: getattr(k, "__name__", ""))
@given(data=st.data())
def test_syntactic_strings_parse_back_to_an_equal_identifier_or_are_refused(kind, pattern, data):
    wire = data.draw(st.from_regex(pattern, fullmatch=True))
    value = parsed_or_refused(kind, wire)
    if value is not None:
        assert_wire_round_trip(value)
        # Only an EVM contract reference may change, and only to lowercase.
        assert str(value) == canonical_wire(wire)


@pytest.mark.parametrize("kind", [ChainId, AssetType, AssetId])
@given(st.text(max_size=80))
def test_arbitrary_text_is_parsed_or_refused_with_value_error(kind, wire):
    value = parsed_or_refused(kind, wire)
    if value is not None:
        assert_wire_round_trip(value)


@pytest.mark.parametrize("kind", [ChainId, AssetType, AssetId])
@given(st.one_of(st.integers(), st.binary(), st.none(), st.booleans(), st.floats()))
def test_non_string_inputs_are_refused(kind, wire):
    with pytest.raises(ValueError):
        kind(wire)


@given(st.integers(0, 10**32 - 1))
def test_eip155_references_are_canonical_decimal(number):
    chain = ChainId(f"eip155:{number}")
    assert (chain.namespace, int(chain.reference)) == ("eip155", number)
    assert_wire_round_trip(chain)
    with pytest.raises(ValueError):
        ChainId(f"eip155:0{number}")


@given(st.integers(0, 10**12), hexadecimal)
def test_erc20_references_canonicalize_to_one_lowercase_identity(number, address):
    chain = ChainId(f"eip155:{number}")
    asset = AssetType(f"{chain}/erc20:0x{address}")
    assert asset == AssetType(f"{chain}/erc20:0x{address.lower()}")
    assert asset == AssetType(f"{chain}/erc20:0x{address.upper()}")
    assert (asset.chain, asset.namespace, asset.reference) == (
        chain,
        "erc20",
        "0x" + address.lower(),
    )
    assert_wire_round_trip(asset)


@given(st.integers(0, 10**12), hexadecimal, uint256)
def test_evm_token_ids_round_trip_as_canonical_uint256(number, address, token):
    asset = AssetId(f"eip155:{number}/erc721:0x{address}/{token}")
    assert (int(asset.token_id), asset.chain) == (token, ChainId(f"eip155:{number}"))
    assert asset.asset_type == AssetType(f"eip155:{number}/erc721:0x{address}")
    assert_wire_round_trip(asset)
    for invalid in (f"0{token}", str(token + 2**256)):
        with pytest.raises(ValueError):
            AssetId(f"eip155:{number}/erc721:0x{address}/{invalid}")


@given(st.from_regex(CHAIN, fullmatch=True))
def test_identifier_kinds_never_compare_equal_across_types(wire):
    chain = parsed_or_refused(ChainId, wire)
    asset = parsed_or_refused(AssetType, f"{wire}/slip44:60")
    if chain is not None and asset is not None:
        assert asset.chain == chain and asset != chain and hash(asset) != hash(chain)
