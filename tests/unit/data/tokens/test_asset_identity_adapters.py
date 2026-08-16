"""Compatibility adapters between token models and AssetIdentity (VIB-6675)."""

from __future__ import annotations

from datetime import datetime

import pytest

from almanak.core.asset_identity import (
    AssetIdentity,
    AssetNamespace,
    KnownDecimals,
    NativeIdentityUnavailable,
    NativeIdentityUnavailableReason,
    UnmeasuredDecimals,
    UnmeasuredDecimalsReason,
)
from almanak.core.constants import ETH_ADDRESS
from almanak.framework.data.tokens.models import BridgeType, ResolvedToken, TokenRef

USDC_ARBITRUM = "0xaf88d065e77c8cc2239327c5edb3a432268e5831"
USDC_SOLANA = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"


def test_token_ref_to_identity_excludes_decimals_symbol_and_provenance() -> None:
    first = TokenRef("arbitrum", USDC_ARBITRUM, 6, "USDC", "static")
    second = TokenRef("arbitrum", USDC_ARBITRUM.upper().replace("0X", "0x"), 18, "OTHER", "cache")

    assert first.asset_identity == second.asset_identity
    assert first.asset_identity == AssetIdentity("arbitrum", AssetNamespace.ERC20, USDC_ARBITRUM)


def test_identity_to_token_ref_requires_explicit_decimals() -> None:
    identity = AssetIdentity("arbitrum", AssetNamespace.ERC20, USDC_ARBITRUM)

    ref = TokenRef.from_asset_identity(identity, decimals=KnownDecimals(6), symbol="USDC", provenance="resolver")

    assert ref == TokenRef("arbitrum", USDC_ARBITRUM, 6)
    assert ref.decimals == 6
    with pytest.raises(TypeError):
        TokenRef.from_asset_identity(identity)  # type: ignore[call-arg]
    with pytest.raises(TypeError, match="decimals must be KnownDecimals"):
        TokenRef.from_asset_identity(
            identity,
            decimals=UnmeasuredDecimals(UnmeasuredDecimalsReason.NOT_RESOLVED),  # type: ignore[arg-type]
        )


def test_native_token_ref_and_identity_round_trip_without_becoming_wrapped_native() -> None:
    ref = TokenRef("arbitrum", ETH_ADDRESS, 18, "ETH")

    identity = ref.asset_identity

    assert identity == AssetIdentity.native("arbitrum")
    assert (
        TokenRef.from_asset_identity(identity, decimals=KnownDecimals(18), symbol="ETH").address == ETH_ADDRESS.lower()
    )


def test_native_adapter_exposes_typed_missing_slip44_without_persisting_a_sentinel() -> None:
    ref = TokenRef("mantle", ETH_ADDRESS, 18, "MNT")

    result = ref.resolve_asset_identity()

    assert result == NativeIdentityUnavailable(
        "mantle",
        NativeIdentityUnavailableReason.MISSING_SLIP44,
    )
    assert not hasattr(result, "to_wire")
    with pytest.raises(ValueError, match="no SLIP-44 coin type"):
        _ = ref.asset_identity


def test_solana_token_ref_adapter_preserves_mint_case() -> None:
    ref = TokenRef("solana", USDC_SOLANA, 6, "USDC")

    assert ref.asset_identity == AssetIdentity("solana", AssetNamespace.TOKEN, USDC_SOLANA)
    assert TokenRef.from_asset_identity(ref.asset_identity, decimals=KnownDecimals(6)).address == USDC_SOLANA


def test_resolved_token_exposes_the_same_writer_safe_identity() -> None:
    resolved = ResolvedToken(
        symbol="USDC",
        address=USDC_ARBITRUM,
        decimals=6,
        chain="arbitrum",
        chain_id=42161,
        bridge_type=BridgeType.NATIVE,
        source="static",
        resolved_at=datetime(2026, 8, 15),
    )

    assert resolved.asset_identity == resolved.token_ref.asset_identity
    assert resolved.asset_decimals == KnownDecimals(6)
    assert resolved.asset_identity.caip19 == f"eip155:42161/erc20:{USDC_ARBITRUM}"


def test_resolved_token_native_flag_is_authoritative_for_non_sentinel_provider_address() -> None:
    resolved = ResolvedToken(
        symbol="ETH",
        address="0x0000000000000000000000000000000000000800",
        decimals=18,
        chain="arbitrum",
        chain_id=42161,
        is_native=True,
    )

    assert resolved.resolve_asset_identity() == AssetIdentity.native("arbitrum")


def test_resolved_token_rejects_non_native_flag_with_native_sentinel() -> None:
    resolved = ResolvedToken(
        symbol="NOT_NATIVE",
        address=ETH_ADDRESS,
        decimals=18,
        chain="arbitrum",
        chain_id=42161,
        is_native=False,
    )

    with pytest.raises(ValueError, match="contradicts its native sentinel"):
        resolved.resolve_asset_identity()
    with pytest.raises(ValueError, match="contradicts its native sentinel"):
        _ = resolved.asset_identity


@pytest.mark.parametrize("is_native", ["false", 1])
def test_resolved_token_identity_rejects_truthy_non_boolean_native_flags(is_native: object) -> None:
    resolved = ResolvedToken(
        symbol="TOKEN",
        address=USDC_ARBITRUM,
        decimals=6,
        chain="arbitrum",
        chain_id=42161,
        is_native=is_native,  # type: ignore[arg-type]
    )

    with pytest.raises(TypeError, match="is_native must be bool"):
        resolved.resolve_asset_identity()


def test_adapter_rejects_non_identity_input() -> None:
    with pytest.raises(TypeError, match="identity must be AssetIdentity"):
        TokenRef.from_asset_identity("eip155:1/slip44:60", decimals=KnownDecimals(18))  # type: ignore[arg-type]
