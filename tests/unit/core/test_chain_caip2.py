"""Tests for the CAIP-2 chain-id codec and registry integration (VIB-5175).

CAIP-2 (https://chainagnostic.org/CAIPs/caip-2) is an additive string
serialization over data the registry already owns (``chain_id`` + ``family``).
These tests guard:

1. **Parity** — every registered descriptor round-trips ``to_caip2`` →
   ``by_caip2`` back to itself, and CAIP-2 input resolves to the same
   descriptor as the canonical name.
2. **Format** — EVM is ``eip155:<chain_id>``; Solana is
   ``solana:<genesis-hash>``.
3. **Validation** — ``caip2_reference`` is required iff non-EVM; the grammar
   is enforced; ``NativeToken.slip44`` rejects negatives.
"""

from __future__ import annotations

import pytest

from almanak.core.chains import (
    ChainDescriptor,
    ChainRegistry,
    GasProfile,
    NativeToken,
    parse_caip2,
    to_caip2,
)
from almanak.core.constants import resolve_chain_name
from almanak.core.enums import Chain, ChainFamily


def _native() -> NativeToken:
    return NativeToken(symbol="X", name="X token", decimals=18)


# ---------------------------------------------------------------------------
# Parity over the whole registry
# ---------------------------------------------------------------------------


def test_every_descriptor_round_trips_caip2() -> None:
    for descriptor in ChainRegistry.all():
        caip2 = descriptor.caip2
        assert ChainRegistry.by_caip2(caip2) is descriptor
        assert to_caip2(descriptor) == caip2
        assert to_caip2(descriptor.enum) == caip2
        assert to_caip2(descriptor.name) == caip2


def test_caip2_input_resolves_same_descriptor_as_name() -> None:
    for descriptor in ChainRegistry.all():
        assert ChainRegistry.resolve(descriptor.caip2) is ChainRegistry.resolve(descriptor.name)


# ---------------------------------------------------------------------------
# Format
# ---------------------------------------------------------------------------


def test_evm_caip2_is_eip155_chain_id() -> None:
    assert to_caip2(Chain.ARBITRUM) == "eip155:42161"
    assert to_caip2(Chain.ETHEREUM) == "eip155:1"
    assert to_caip2("base") == "eip155:8453"
    arb = ChainRegistry.get(Chain.ARBITRUM)
    assert arb.caip2 == f"eip155:{arb.chain_id}"


def test_solana_caip2_uses_genesis_reference() -> None:
    assert to_caip2(Chain.SOLANA) == "solana:5eykt4UsFv8P8NJdTREpY1vzqKqZKvdp"
    # Solana is not indexed by numeric id (chain_id == 0 sentinel) but resolves by CAIP-2.
    assert ChainRegistry.by_caip2("solana:5eykt4UsFv8P8NJdTREpY1vzqKqZKvdp").enum is Chain.SOLANA


def test_solana_caip2_reference_is_case_sensitive() -> None:
    # Lowercasing the base58 genesis hash must NOT match (reference is verbatim).
    assert ChainRegistry.try_resolve_caip2("solana:5eykt4usfv8p8njdtrepy1vzqkqzkvdp") is None


# ---------------------------------------------------------------------------
# Registry lookups + resolve_chain_name passthrough
# ---------------------------------------------------------------------------


def test_resolve_routes_caip2_for_evm() -> None:
    assert ChainRegistry.resolve("eip155:42161") is ChainRegistry.resolve("arbitrum")


def test_resolve_chain_name_accepts_caip2() -> None:
    assert resolve_chain_name("eip155:137") == "polygon"
    assert resolve_chain_name("solana:5eykt4UsFv8P8NJdTREpY1vzqKqZKvdp") == "solana"
    # Non-CAIP aliases still work unchanged.
    assert resolve_chain_name("bnb") == "bsc"


def test_try_resolve_caip2_detector_semantics() -> None:
    # Bare names / aliases are NOT CAIP-2 shaped -> None (so callers can detect).
    assert ChainRegistry.try_resolve_caip2("arbitrum") is None
    assert ChainRegistry.try_resolve_caip2("solana") is None
    # CAIP-2 shaped but unknown -> None.
    assert ChainRegistry.try_resolve_caip2("eip155:99999") is None
    # Unknown namespace -> None.
    assert ChainRegistry.try_resolve_caip2("bip122:000000000019d6689c085ae165831e93") is None
    # Known -> descriptor.
    assert ChainRegistry.try_resolve_caip2("eip155:1").enum is Chain.ETHEREUM


def test_by_caip2_raises_on_unknown_or_malformed() -> None:
    with pytest.raises(ValueError, match="Unknown or malformed CAIP-2"):
        ChainRegistry.by_caip2("eip155:99999")
    with pytest.raises(ValueError, match="Unknown or malformed CAIP-2"):
        ChainRegistry.by_caip2("not-a-caip2")


# ---------------------------------------------------------------------------
# parse_caip2
# ---------------------------------------------------------------------------


def test_parse_caip2_valid() -> None:
    assert parse_caip2("eip155:1") == ("eip155", "1")
    assert parse_caip2("solana:5eykt4UsFv8P8NJdTREpY1vzqKqZKvdp") == (
        "solana",
        "5eykt4UsFv8P8NJdTREpY1vzqKqZKvdp",
    )


@pytest.mark.parametrize(
    "bad",
    [
        "arbitrum",  # no colon
        "eip155",  # no reference
        "eip155:",  # empty reference
        "x:1",  # namespace too short
        "eip155:" + "0" * 33,  # reference too long
        "EIP155:1",  # namespace must be lowercase
    ],
)
def test_parse_caip2_rejects_malformed(bad: str) -> None:
    with pytest.raises(ValueError, match="Malformed CAIP-2"):
        parse_caip2(bad)


# ---------------------------------------------------------------------------
# Descriptor validation
# ---------------------------------------------------------------------------


def test_evm_descriptor_rejects_explicit_caip2_reference() -> None:
    with pytest.raises(ValueError, match="caip2_reference must be None"):
        ChainDescriptor(
            enum=Chain.ETHEREUM,
            name="ethereum",
            chain_id=1,
            family=ChainFamily.EVM,
            native=_native(),
            gas=GasProfile(),
            caip2_reference="1",
        )


def test_non_evm_descriptor_without_reference_constructs_but_caip2_raises() -> None:
    # Construction stays permissive so synthetic non-EVM fixtures are buildable
    # (the registration-time guard is what enforces presence for real chains)...
    descriptor = ChainDescriptor(
        enum=Chain.SOLANA,
        name="solana",
        chain_id=0,
        family=ChainFamily.SOLANA,
        native=_native(),
        gas=GasProfile(),
    )
    # ...but a CAIP-2 id cannot be formed without a reference.
    with pytest.raises(ValueError, match="no caip2_reference"):
        _ = descriptor.caip2


def test_non_evm_descriptor_validates_reference_grammar() -> None:
    with pytest.raises(ValueError, match="CAIP-2 reference grammar"):
        ChainDescriptor(
            enum=Chain.SOLANA,
            name="solana",
            chain_id=0,
            family=ChainFamily.SOLANA,
            native=_native(),
            gas=GasProfile(),
            caip2_reference="bad/reference!",
        )


def test_native_token_rejects_negative_slip44() -> None:
    with pytest.raises(ValueError, match="slip44 must be non-negative"):
        NativeToken(symbol="X", name="X", decimals=18, slip44=-1)


def test_eth_native_chains_carry_slip44_60() -> None:
    for chain in (Chain.ETHEREUM, Chain.ARBITRUM, Chain.OPTIMISM, Chain.BASE, Chain.BLAST, Chain.LINEA):
        assert ChainRegistry.get(chain).native.slip44 == 60
    assert ChainRegistry.get(Chain.SOLANA).native.slip44 == 501


def test_non_eth_native_chains_carry_verified_slip44_values() -> None:
    assert ChainRegistry.get(Chain.POLYGON).native.slip44 == 966
    assert ChainRegistry.get(Chain.AVALANCHE).native.slip44 == 9000
    assert ChainRegistry.get(Chain.BSC).native.slip44 == 9006
    assert ChainRegistry.get(Chain.BERACHAIN).native.slip44 == 8008
    assert ChainRegistry.get(Chain.SONIC).native.slip44 == 10007
    assert ChainRegistry.get(Chain.MONAD).native.slip44 == 268435779
    assert ChainRegistry.get(Chain.HYPEREVM).native.slip44 == 2457


def test_chains_without_verified_slip44_leave_slip44_unset() -> None:
    for chain in (Chain.XLAYER, Chain.MANTLE, Chain.PLASMA, Chain.ZEROG):
        assert ChainRegistry.get(chain).native.slip44 is None


def test_every_registered_chain_has_slip44_coverage() -> None:
    covered_with_slip44 = {
        Chain.ETHEREUM,
        Chain.ARBITRUM,
        Chain.OPTIMISM,
        Chain.BASE,
        Chain.BLAST,
        Chain.LINEA,
        Chain.SOLANA,
        Chain.POLYGON,
        Chain.AVALANCHE,
        Chain.BSC,
        Chain.BERACHAIN,
        Chain.SONIC,
        Chain.MONAD,
        Chain.HYPEREVM,
    }
    covered_without_slip44 = {Chain.XLAYER, Chain.MANTLE, Chain.PLASMA, Chain.ZEROG}
    all_registered = {descriptor.enum for descriptor in ChainRegistry.all()}

    assert all_registered == covered_with_slip44 | covered_without_slip44
