"""Tests for the shared chain-aware address normalizer."""

from __future__ import annotations

import pytest

from almanak.core.addresses import normalize_address
from almanak.core.chains import ChainRegistry
from almanak.core.enums import ChainFamily

EVM_MIXED_CASE = "0xAf88D065E77C8cC2239327C5EDb3A432268e5831"
SOLANA_CASE_SENSITIVE = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"


def test_evm_address_is_trimmed_and_lowercased() -> None:
    assert normalize_address(f"  {EVM_MIXED_CASE}\n", "arbitrum") == EVM_MIXED_CASE.lower()


def test_solana_address_is_trimmed_without_changing_case() -> None:
    assert normalize_address(f"\t{SOLANA_CASE_SENSITIVE} ", "solana") == SOLANA_CASE_SENSITIVE


def test_chain_alias_uses_registered_family() -> None:
    assert normalize_address(SOLANA_CASE_SENSITIVE, "SOLANA") == SOLANA_CASE_SENSITIVE
    assert normalize_address(EVM_MIXED_CASE, "arb") == EVM_MIXED_CASE.lower()


def test_unknown_chain_preserves_historical_evm_fallback() -> None:
    assert normalize_address(EVM_MIXED_CASE, "not-a-chain") == EVM_MIXED_CASE.lower()


def test_every_registered_family_uses_its_casing_contract() -> None:
    for descriptor in ChainRegistry.all():
        candidate = SOLANA_CASE_SENSITIVE if descriptor.family is ChainFamily.SOLANA else EVM_MIXED_CASE
        expected = candidate if descriptor.family is ChainFamily.SOLANA else candidate.lower()
        assert normalize_address(candidate, descriptor.name) == expected


def test_non_string_address_is_rejected() -> None:
    with pytest.raises(TypeError, match="Address must be a string"):
        normalize_address(123, "arbitrum")  # type: ignore[arg-type]
