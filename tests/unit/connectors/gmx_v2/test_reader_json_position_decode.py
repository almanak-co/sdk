"""VIB-5950 regression: the vendored ``reader.json`` ``getAccountPositions`` ABI
must decode the CURRENT 10-field ``Position.Numbers`` (signed ``int256``
``pendingImpactAmount`` at index 3), and ``GMXV2SDK._parse_raw_positions`` must
read the shifted fields from their new indices.

This pins the "empty-array hides a stale ABI" class. The stale 11-``uint256``
ABI decoded an EMPTY position array fine (so unit tests that used empty books
stayed green) but ran ``eth_abi`` out of bytes on any NON-EMPTY real position —
silently failing every live GMX read. That is exactly what stranded the
ALM-2976 residual position: the demo's ``size_usd=None`` full-close made the
compiler live-read the venue size, the web3 decode against the stale ABI threw,
the REST fallback 403'd, and the compiler fail-closed ("refusing to guess").

We therefore decode a NON-EMPTY position with a NEGATIVE ``pendingImpactAmount``
through the ``reader.json`` contract-ABI codec (the same ``eth_abi`` codec web3
uses under the hood), NOT the hand-built ``perps_read`` decode.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from eth_abi import decode as abi_decode
from eth_abi import encode as abi_encode
from eth_utils import to_checksum_address

import almanak.connectors.gmx_v2 as gmx_pkg
from almanak.connectors.gmx_v2.sdk import GMXV2SDK

# Canonical 10-field type the current on-chain struct decodes to.
_EXPECTED_OUTPUT_TYPE = (
    "((address,address,address),"
    "(uint256,uint256,uint256,int256,uint256,uint256,uint256,uint256,uint256,uint256),"
    "(bool))[]"
)
# The stale layout VIB-5289/VIB-5950 replaced: 11 uint256, no int256, block fields present.
_STALE_OUTPUT_TYPE = (
    "((address,address,address),"
    "(uint256,uint256,uint256,uint256,uint256,uint256,uint256,uint256,uint256,uint256,uint256),"
    "(bool))[]"
)


def _abi_type(param: dict) -> str:
    """Render an ABI output param (with nested components) to an eth-abi type string."""
    base = param["type"]
    if base.startswith("tuple"):
        inner = "(" + ",".join(_abi_type(c) for c in param["components"]) + ")"
        return inner + base[len("tuple"):]  # preserve any array suffix, e.g. "[]"
    return base


def _reader_get_account_positions_output_type() -> str:
    abi = json.loads((Path(gmx_pkg.__file__).parent / "abis" / "reader.json").read_text())
    fn = next(i for i in abi if i.get("type") == "function" and i.get("name") == "getAccountPositions")
    assert len(fn["outputs"]) == 1
    return _abi_type(fn["outputs"][0])


def test_reader_json_output_type_is_current_10_field_struct():
    """The vendored ABI itself must be the current 10-field struct — a revert to
    ``["uint256"] * 11`` (or dropping the int256) trips here."""
    assert _reader_get_account_positions_output_type() == _EXPECTED_OUTPUT_TYPE


def test_reader_json_decodes_nonempty_position_and_parse_reindexes():
    out_type = _reader_get_account_positions_output_type()

    account = to_checksum_address("0x" + "11" * 20)
    market = to_checksum_address("0x" + "22" * 20)
    usdc = to_checksum_address("0x" + "cc" * 20)
    # NON-EMPTY position. pendingImpactAmount is the real negative value from the
    # VIB-5252 real-fork proof — proves int256 signedness round-trips and does not
    # corrupt the field-shift.
    numbers = (
        5000 * 10**30,  # [0] sizeInUsd (30 decimals)
        25 * 10**17,  # [1] sizeInTokens
        1000 * 10**6,  # [2] collateralAmount (USDC, 6 decimals)
        -84278987710162044,  # [3] pendingImpactAmount (int256, negative)
        71,  # [4] borrowingFactor
        91,  # [5] fundingFeeAmountPerSize
        11,  # [6] longTokenClaimableFundingAmountPerSize
        13,  # [7] shortTokenClaimableFundingAmountPerSize
        1_700_000_000,  # [8] increasedAtTime
        1_700_000_001,  # [9] decreasedAtTime
    )
    props = [((account, market, usdc), numbers, (True,))]
    raw = abi_encode([out_type], [props])

    # Decode through the reader.json-derived contract ABI type (web3's codec).
    decoded = abi_decode([out_type], raw)[0]
    parsed = GMXV2SDK._parse_raw_positions(decoded)

    assert len(parsed) == 1
    p = parsed[0]
    # Valuation-critical fields (indices 0/1/2 + flags) — the ones the compiler
    # live-read consumes to size a full close.
    assert p["size_in_usd"] == 5000 * 10**30
    assert p["size_in_tokens"] == 25 * 10**17
    assert p["collateral_amount"] == 1000 * 10**6
    assert p["is_long"] is True
    # Shifted fields land on their NEW indices (4/5/8/9), NOT the stale (3/4/9/10)
    # — the stale layout would have read pendingImpactAmount(-8.4e16) as borrowing_factor.
    assert p["borrowing_factor"] == 71
    assert p["funding_fee_amount_per_size"] == 91
    assert p["increased_at_time"] == 1_700_000_000
    assert p["decreased_at_time"] == 1_700_000_001
    # Block fields no longer exist on-chain — must NOT be fabricated.
    assert "increased_at_block" not in p
    assert "decreased_at_block" not in p


def test_stale_abi_hid_the_bug_only_on_nonempty_arrays():
    """Demonstrate the exact hiding mechanism: the stale 11-``uint256`` ABI
    decodes an EMPTY position array identically to the fixed ABI (so any test
    that used an empty book passed), but runs out of bytes on the NON-EMPTY real
    return that the fixed ABI decodes cleanly."""
    out_type = _reader_get_account_positions_output_type()

    # Empty book: BOTH ABIs decode it to an empty tuple — this is why the stale
    # ABI survived undetected until a live position appeared.
    empty_raw = abi_encode([out_type], [[]])
    assert abi_decode([out_type], empty_raw)[0] == ()
    assert abi_decode([_STALE_OUTPUT_TYPE], empty_raw)[0] == ()

    # Non-empty 10-field position: fixed ABI decodes it; stale ABI runs out of bytes.
    account = to_checksum_address("0x" + "11" * 20)
    market = to_checksum_address("0x" + "22" * 20)
    usdc = to_checksum_address("0x" + "cc" * 20)
    numbers = (10**31, 10**18, 10**6, -5, 71, 91, 11, 13, 1_700_000_000, 1_700_000_001)
    raw = abi_encode([out_type], [[((account, market, usdc), numbers, (True,))]])
    assert len(abi_decode([out_type], raw)[0]) == 1
    with pytest.raises(Exception):
        abi_decode([_STALE_OUTPUT_TYPE], raw)
