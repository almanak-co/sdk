"""Tests for typed compiler-to-receipt token metadata."""

from __future__ import annotations

from almanak.framework.data.tokens import build_swap_token_meta, build_token_meta_hint_map, parse_swap_token_meta


def test_build_swap_token_meta_is_json_safe_and_chain_aware() -> None:
    metadata = build_swap_token_meta(
        {"address": "0x00000000000000000000000000000000000000AA", "symbol": "USDC", "decimals": 6},
        {"address": "0x00000000000000000000000000000000000000BB", "symbol": "WETH", "decimals": 18},
        chain="base",
    )

    assert metadata == {
        "token_in": {
            "address": "0x00000000000000000000000000000000000000aa",
            "symbol": "USDC",
            "decimals": 6,
        },
        "token_out": {
            "address": "0x00000000000000000000000000000000000000bb",
            "symbol": "WETH",
            "decimals": 18,
        },
    }


def test_parse_prefers_canonical_metadata_over_legacy_fields() -> None:
    metadata = parse_swap_token_meta(
        {
            "swap_token_meta": {
                "token_in": {"address": "0x00000000000000000000000000000000000000AA", "symbol": "A", "decimals": 7}
            },
            "from_token": {
                "address": "0x00000000000000000000000000000000000000BB",
                "symbol": "B",
                "decimals": 9,
            },
        },
        chain="ethereum",
    )

    assert metadata["token_in"]["symbol"] == "A"
    assert "token_out" not in metadata


def test_parse_legacy_metadata_coerces_decimals_and_skips_native() -> None:
    metadata = parse_swap_token_meta(
        {
            "from_token": {
                "address": "0x00000000000000000000000000000000000000AA",
                "symbol": "ETH",
                "decimals": 18,
                "is_native": True,
            },
            "to_token": {
                "address": "0x00000000000000000000000000000000000000BB",
                "symbol": "USDC",
                "decimals": "6",
            },
        },
        chain="ethereum",
    )

    assert metadata == {
        "token_out": {
            "address": "0x00000000000000000000000000000000000000bb",
            "symbol": "USDC",
            "decimals": 6,
        }
    }


def test_solana_metadata_preserves_mint_case() -> None:
    mint = "So11111111111111111111111111111111111111112"

    metadata = build_swap_token_meta(
        {"address": mint, "symbol": "SOL", "decimals": 9},
        None,
        chain="solana",
    )

    assert metadata["token_in"]["address"] == mint


def test_boolean_and_fractional_decimals_are_rejected() -> None:
    address = "0x00000000000000000000000000000000000000AA"

    for invalid in (True, 6.0, 6.5, "6.5"):
        token = {"address": address, "symbol": "USDC", "decimals": invalid}
        assert build_swap_token_meta(token, None, chain="ethereum") == {}
        assert build_token_meta_hint_map({"token_in": token}) == {}
