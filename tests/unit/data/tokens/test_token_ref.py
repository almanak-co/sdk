"""Tests for the ADR-002 TokenRef identity primitive."""

from __future__ import annotations

from datetime import datetime

import pytest

from almanak.core.enums import Chain
from almanak.framework.data.tokens import NATIVE_SENTINEL, TokenRef, normalize_token_address_for_chain
from almanak.framework.data.tokens.models import CHAIN_ID_MAP, BridgeType, ResolvedToken
from almanak.framework.data.tokens.resolver import _normalize_address_for_chain

USDC_ARBITRUM_MIXED = "0xAf88d065E77c8cC2239327C5EDb3A432268e5831"
USDC_ARBITRUM_LOWER = "0xaf88d065e77c8cc2239327c5edb3a432268e5831"
USDC_ETHEREUM = "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"
USDC_SOLANA = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"


def _resolved_token(**overrides: object) -> ResolvedToken:
    values = {
        "symbol": "USDC",
        "address": USDC_ARBITRUM_MIXED,
        "decimals": 6,
        "chain": Chain.ARBITRUM,
        "chain_id": CHAIN_ID_MAP[Chain.ARBITRUM],
        "name": "USD Coin",
        "coingecko_id": "usd-coin",
        "is_stablecoin": True,
        "canonical_symbol": "USDC",
        "bridge_type": BridgeType.NATIVE,
        "source": "static",
        "is_verified": True,
        "resolved_at": datetime(2026, 1, 1),
    }
    values.update(overrides)
    return ResolvedToken(**values)  # type: ignore[arg-type]


def test_token_ref_identity_ignores_display_metadata() -> None:
    left = TokenRef(
        chain=Chain.ARBITRUM,
        address=USDC_ARBITRUM_MIXED,
        decimals=6,
        symbol="USDC",
        provenance="static",
    )
    right = TokenRef(
        chain="arbitrum",
        address=USDC_ARBITRUM_LOWER,
        decimals=18,
        symbol="USD Coin",
        provenance="manual",
    )

    assert left == right
    assert hash(left) == hash(right)
    assert left.identity_key == (Chain.ARBITRUM, USDC_ARBITRUM_LOWER)


def test_token_ref_equality_rejects_other_types() -> None:
    token_ref = TokenRef(chain=Chain.ARBITRUM, address=USDC_ARBITRUM_LOWER, decimals=6, symbol="USDC")

    assert token_ref.__eq__(object()) is NotImplemented


def test_token_ref_same_symbol_different_address_is_distinct() -> None:
    arbitrum_usdc = TokenRef(chain=Chain.ARBITRUM, address=USDC_ARBITRUM_LOWER, decimals=6, symbol="USDC")
    ethereum_usdc_on_arbitrum = TokenRef(chain=Chain.ARBITRUM, address=USDC_ETHEREUM, decimals=6, symbol="USDC")

    assert arbitrum_usdc != ethereum_usdc_on_arbitrum


def test_token_ref_same_address_different_chain_is_distinct() -> None:
    on_arbitrum = TokenRef(chain=Chain.ARBITRUM, address=USDC_ARBITRUM_LOWER, decimals=6, symbol="USDC")
    on_ethereum = TokenRef(chain=Chain.ETHEREUM, address=USDC_ARBITRUM_LOWER, decimals=6, symbol="USDC")

    assert on_arbitrum != on_ethereum


def test_token_ref_normalization_matches_resolver_helper() -> None:
    assert normalize_token_address_for_chain(USDC_ARBITRUM_MIXED, Chain.ARBITRUM) == _normalize_address_for_chain(
        USDC_ARBITRUM_MIXED, "arbitrum"
    )
    assert normalize_token_address_for_chain(f" {USDC_ARBITRUM_MIXED}\n", Chain.ARBITRUM) == USDC_ARBITRUM_LOWER
    assert normalize_token_address_for_chain(USDC_SOLANA, Chain.SOLANA) == _normalize_address_for_chain(
        USDC_SOLANA, "solana"
    )
    assert normalize_token_address_for_chain(f"\t{USDC_SOLANA} ", Chain.SOLANA) == USDC_SOLANA
    assert normalize_token_address_for_chain(USDC_SOLANA, Chain.SOLANA) == USDC_SOLANA


def test_token_ref_normalization_rejects_non_string_address() -> None:
    with pytest.raises(TypeError, match="Token address must be a string"):
        normalize_token_address_for_chain(123, Chain.ARBITRUM)  # type: ignore[arg-type]


def test_token_ref_native_sentinel_is_valid_identity() -> None:
    native = TokenRef(chain=Chain.ARBITRUM, address=NATIVE_SENTINEL, decimals=18, symbol="ETH")

    assert native.address == NATIVE_SENTINEL.lower()
    assert native.identity_key == (Chain.ARBITRUM, NATIVE_SENTINEL.lower())


def test_token_ref_rejects_bad_identity_inputs() -> None:
    with pytest.raises(ValueError, match="address cannot be empty"):
        TokenRef(chain=Chain.ARBITRUM, address="", decimals=18)
    with pytest.raises(ValueError, match="address cannot be empty"):
        TokenRef(chain=Chain.ARBITRUM, address="   ", decimals=18)
    with pytest.raises(ValueError, match="Invalid decimals"):
        TokenRef(chain=Chain.ARBITRUM, address=USDC_ARBITRUM_LOWER, decimals=-1)
    with pytest.raises(ValueError, match="Unknown chain"):
        TokenRef(chain="not-a-chain", address=USDC_ARBITRUM_LOWER, decimals=6)


def test_token_ref_wire_shape_round_trips() -> None:
    original = TokenRef(
        chain=Chain.SOLANA,
        address=USDC_SOLANA,
        decimals=6,
        symbol="USDC",
        provenance="jupiter",
    )

    restored = TokenRef.from_dict(original.to_dict())

    assert restored == original
    assert restored.to_dict() == {
        "chain": "SOLANA",
        "address": USDC_SOLANA,
        "decimals": 6,
        "symbol": "USDC",
        "provenance": "jupiter",
    }


def test_resolved_token_exposes_token_ref_identity() -> None:
    resolved = _resolved_token()

    assert resolved.address == USDC_ARBITRUM_MIXED
    assert resolved.token_ref == TokenRef(
        chain=Chain.ARBITRUM,
        address=USDC_ARBITRUM_LOWER,
        decimals=6,
        symbol="USDC",
        provenance="static",
    )


def test_resolved_token_wire_shape_preserves_address_form() -> None:
    resolved = _resolved_token()
    restored = ResolvedToken.from_dict(resolved.to_dict())

    assert restored == resolved
    assert restored.address == USDC_ARBITRUM_MIXED
    assert restored.token_ref.address == USDC_ARBITRUM_LOWER


def test_resolved_token_rejects_bad_inputs() -> None:
    with pytest.raises(ValueError, match="Token symbol cannot be empty"):
        _resolved_token(symbol="")
    with pytest.raises(ValueError, match="Token address cannot be empty"):
        _resolved_token(address="")
    with pytest.raises(ValueError, match="Invalid decimals"):
        _resolved_token(decimals=78)
    with pytest.raises(ValueError, match="has chain_id"):
        _resolved_token(chain_id=1)
