"""Unit tests for :class:`almanak.core.models.quote_asset.QuoteAsset`.

Definition-only value type (PR-1): exercises construction, parsing,
normalisation, validation, and round-tripping. There is intentionally no
valuation/accounting behaviour to test — the SDK does not yet branch on the
quote asset.
"""

from __future__ import annotations

import dataclasses

import pytest

from almanak.core.models.quote_asset import QuoteAsset

# A few canonical addresses used across cases (checksummed on input).
WETH_ETH = "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2"
WETH_ARB = "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1"
SOL_MINT = "So11111111111111111111111111111111111111112"  # non-EVM (base58)


# --------------------------------------------------------------------------- #
# Constructors
# --------------------------------------------------------------------------- #


def test_usd_constructor():
    qa = QuoteAsset.usd()
    assert qa.kind == "fiat_usd"
    assert qa.chain_id is None
    assert qa.address is None
    assert qa.is_usd is True
    assert str(qa) == "USD"


def test_token_constructor_lowercases_evm_address():
    qa = QuoteAsset.token(42161, WETH_ARB)
    assert qa.kind == "token"
    assert qa.chain_id == 42161
    assert qa.address == WETH_ARB.lower()  # canonical key
    assert qa.is_usd is False
    assert str(qa) == f"token:42161:{WETH_ARB.lower()}"
    # Uppercase 0X prefix is accepted and canonicalised to lowercase 0x.
    assert QuoteAsset.token(42161, "0X" + WETH_ARB[2:]).address == WETH_ARB.lower()


def test_token_constructor_rejects_non_int_chain_id():
    # No silent coercion: bool / float / numeric-string chain_id are rejected,
    # matching the strict __post_init__ check (asset identity must stay exact).
    for bad in ("1", 1.0, True):
        with pytest.raises(ValueError, match="integer chain_id"):
            QuoteAsset.token(bad, WETH_ETH)  # type: ignore[arg-type]


def test_solana_sentinel_chain_id_and_non_evm_address_preserved():
    # chain_id 0 is the non-EVM sentinel; base58 addresses are kept verbatim.
    qa = QuoteAsset.token(0, SOL_MINT)
    assert qa.chain_id == 0
    assert qa.address == SOL_MINT  # not lower-cased (case-sensitive base58)


# --------------------------------------------------------------------------- #
# parse()
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize("raw", [None, "USD", "usd", " usd ", {"type": "fiat_usd"}, {"kind": "USD"}, {}])
def test_parse_resolves_to_usd(raw):
    assert QuoteAsset.parse(raw) == QuoteAsset.usd()


def test_parse_token_dict():
    qa = QuoteAsset.parse({"type": "token", "chain_id": 1, "address": WETH_ETH})
    assert qa == QuoteAsset.token(1, WETH_ETH)


def test_parse_infers_token_when_type_omitted_but_fields_present():
    qa = QuoteAsset.parse({"chain_id": 42161, "address": WETH_ARB})
    assert qa == QuoteAsset.token(42161, WETH_ARB)


def test_parse_passthrough_quote_asset():
    qa = QuoteAsset.token(1, WETH_ETH)
    assert QuoteAsset.parse(qa) is qa


def test_parse_round_trips_through_to_dict():
    for original in (QuoteAsset.usd(), QuoteAsset.token(8453, "0x4200000000000000000000000000000000000006")):
        assert QuoteAsset.parse(original.to_dict()) == original


# --------------------------------------------------------------------------- #
# Rejections — the whole point of structural validation
# --------------------------------------------------------------------------- #


def test_rejects_chain_name_string():
    # The locked design: numeric chain_id only, never a chain-name string.
    with pytest.raises(ValueError, match="chain_id"):
        QuoteAsset.parse({"type": "token", "chain": "arbitrum", "address": WETH_ARB})


def test_rejects_unrecognised_string():
    with pytest.raises(ValueError, match="unrecognised quote_asset string"):
        QuoteAsset.parse("WETH")


def test_rejects_token_dict_missing_fields():
    with pytest.raises(ValueError, match="requires"):
        QuoteAsset.parse({"type": "token", "chain_id": 1})
    with pytest.raises(ValueError, match="requires"):
        QuoteAsset.parse({"type": "token", "address": WETH_ETH})


def test_token_evm_chain_requires_evm_shaped_address():
    # On EVM chains (chain_id != 0) the address must be 0x + 40 hex; wrong length,
    # non-hex, or non-0x garbage are all rejected (was silently accepted before).
    for bad in ("0x123", "0x" + "z" * 40, WETH_ETH[:-1], "not-an-address"):
        with pytest.raises(ValueError, match="EVM chain"):
            QuoteAsset.token(1, bad)


def test_rejects_unknown_type():
    with pytest.raises(ValueError, match="unknown quote_asset type"):
        QuoteAsset.parse({"type": "stablecoin", "chain_id": 1, "address": WETH_ETH})


def test_rejects_non_mapping_input():
    with pytest.raises(TypeError):
        QuoteAsset.parse(42)


def test_fiat_with_token_fields_is_invalid():
    with pytest.raises(ValueError, match="must not set"):
        QuoteAsset(kind="fiat_usd", chain_id=1)


def test_token_requires_integer_chain_id():
    with pytest.raises(ValueError, match="integer chain_id"):
        QuoteAsset(kind="token", chain_id="1", address=WETH_ETH)  # type: ignore[arg-type]
    # bool is an int subclass — must be rejected explicitly.
    with pytest.raises(ValueError, match="integer chain_id"):
        QuoteAsset(kind="token", chain_id=True, address=WETH_ETH)


def test_token_rejects_negative_chain_id():
    with pytest.raises(ValueError, match="non-negative"):
        QuoteAsset(kind="token", chain_id=-1, address=WETH_ETH)


def test_unknown_kind_rejected():
    with pytest.raises(ValueError, match="unknown quote asset kind"):
        QuoteAsset(kind="crypto")  # type: ignore[arg-type]


def test_token_rejects_empty_or_non_string_address():
    with pytest.raises(ValueError, match="non-empty address"):
        QuoteAsset(kind="token", chain_id=1, address="")
    with pytest.raises(ValueError, match="non-empty address"):
        QuoteAsset(kind="token", chain_id=1, address=None)


# --------------------------------------------------------------------------- #
# Value semantics
# --------------------------------------------------------------------------- #


def test_is_frozen():
    qa = QuoteAsset.usd()
    with pytest.raises(dataclasses.FrozenInstanceError):
        qa.kind = "token"  # type: ignore[misc]


def test_equality_and_hashing():
    a = QuoteAsset.token(1, WETH_ETH)
    b = QuoteAsset.token(1, WETH_ETH.lower())  # different case -> same canonical key
    assert a == b
    assert hash(a) == hash(b)
    assert QuoteAsset.usd() == QuoteAsset.usd()
    assert a != QuoteAsset.usd()
    # usable in sets/dicts (hashable value type)
    assert {a, b} == {a}
