"""Tests for the CAIP-19 asset-id codec and resolver integration (VIB-5175).

CAIP-19 is the canonical string form of the ADR-002 ``TokenRef`` identity
(``(chain, address)`` + an is-native check). These tests guard:

1. **to_caip19** — erc20 (EVM), slip44 (native), token (Solana SPL); fail-loud
   when a native chain has no SLIP-44 coin type.
2. **parse_caip19** — valid forms + malformed rejection.
3. **Resolver equivalence** — ``resolve_caip19`` and a CAIP-19 string through
   ``resolve`` return the same token as address+chain resolution, and a
   ResolvedToken round-trips through CAIP-19.
"""

from __future__ import annotations

import pytest

from almanak.core.enums import Chain
from almanak.framework.data.tokens import (
    NATIVE_SENTINEL,
    ParsedAsset,
    TokenRef,
    get_token_resolver,
    parse_caip19,
)

DAI_ETHEREUM = "0x6b175474e89094c44da98b954eedeac495271d0f"
USDC_SOLANA = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"


# ---------------------------------------------------------------------------
# to_caip19
# ---------------------------------------------------------------------------


def test_to_caip19_erc20_evm() -> None:
    ref = TokenRef(chain=Chain.ETHEREUM, address=DAI_ETHEREUM, decimals=18, symbol="DAI")
    assert ref.to_caip19() == f"eip155:1/erc20:{DAI_ETHEREUM}"


def test_to_caip19_bridged_token_is_still_address_based() -> None:
    # A bridged variant (USDC.e) has a distinct address but is still erc20-by-address.
    usdc_e = "0xff970a61a04b1ca14834a43f5de4533ebddb5cc8"
    ref = TokenRef(chain=Chain.ARBITRUM, address=usdc_e, decimals=6, symbol="USDC.e")
    assert ref.to_caip19() == f"eip155:42161/erc20:{usdc_e}"


def test_to_caip19_native_uses_slip44() -> None:
    ref = TokenRef(chain=Chain.ARBITRUM, address=NATIVE_SENTINEL, decimals=18, symbol="ETH")
    assert ref.to_caip19() == "eip155:42161/slip44:60"


@pytest.mark.parametrize(
    ("chain", "expected"),
    [
        (Chain.POLYGON, "eip155:137/slip44:966"),  # Matic
        (Chain.AVALANCHE, "eip155:43114/slip44:9000"),  # Avalanche
        (Chain.BSC, "eip155:56/slip44:9006"),  # Binance Smart Chain
        (Chain.BERACHAIN, "eip155:80094/slip44:8008"),  # Berachain
        (Chain.SONIC, "eip155:146/slip44:10007"),  # SONIC
        (Chain.MONAD, "eip155:143/slip44:268435779"),  # Monad
    ],
)
def test_to_caip19_native_for_non_eth_chains(chain: Chain, expected: str) -> None:
    ref = TokenRef(chain=chain, address=NATIVE_SENTINEL, decimals=18, symbol="X")
    assert ref.to_caip19() == expected


def test_to_caip19_solana_spl_token() -> None:
    ref = TokenRef(chain=Chain.SOLANA, address=USDC_SOLANA, decimals=6, symbol="USDC")
    assert ref.to_caip19() == f"solana:5eykt4UsFv8P8NJdTREpY1vzqKqZKvdp/token:{USDC_SOLANA}"


@pytest.mark.parametrize(
    ("chain", "symbol"),
    [
        (Chain.XLAYER, "OKB"),
        (Chain.MANTLE, "MNT"),
        (Chain.PLASMA, "XPL"),
        (Chain.ZEROG, "A0GI"),
    ],
)
def test_to_caip19_native_fails_loud_without_slip44(chain: Chain, symbol: str) -> None:
    # These chains have no verified SLIP-44 coin type -> fail loudly rather
    # than emit a non-standard native id (the slip44 field stays None).
    ref = TokenRef(chain=chain, address=NATIVE_SENTINEL, decimals=18, symbol=symbol)
    with pytest.raises(ValueError, match="no SLIP-44 coin type"):
        ref.to_caip19()


# ---------------------------------------------------------------------------
# parse_caip19
# ---------------------------------------------------------------------------


def test_parse_caip19_valid() -> None:
    assert parse_caip19(f"eip155:1/erc20:{DAI_ETHEREUM}") == ParsedAsset(
        caip2="eip155:1", asset_namespace="erc20", asset_reference=DAI_ETHEREUM
    )
    assert parse_caip19("eip155:42161/slip44:60") == ParsedAsset(
        caip2="eip155:42161", asset_namespace="slip44", asset_reference="60"
    )


@pytest.mark.parametrize(
    "bad",
    [
        "eip155:1",  # no asset part
        "eip155:1/erc20",  # no asset reference
        "not-a-caip2/erc20:0xabc",  # bad chain part
        "eip155:1/E:0xabc",  # asset namespace too short
        f"eip155:1/erc721:{DAI_ETHEREUM}/1234",  # NFT token_id unsupported in Phase 1
    ],
)
def test_parse_caip19_rejects_malformed(bad: str) -> None:
    with pytest.raises(ValueError, match="Malformed CAIP-19"):
        parse_caip19(bad)


# ---------------------------------------------------------------------------
# Resolver equivalence
# ---------------------------------------------------------------------------


def test_resolve_caip19_matches_address_resolution() -> None:
    resolver = get_token_resolver()
    via_caip = resolver.resolve_caip19(f"eip155:1/erc20:{DAI_ETHEREUM}", skip_gateway=True)
    via_address = resolver.resolve(DAI_ETHEREUM, "ethereum", skip_gateway=True)
    assert via_caip == via_address
    assert via_caip.symbol == "DAI"
    assert via_caip.decimals == 18


def test_resolve_routes_caip19_string() -> None:
    resolver = get_token_resolver()
    # The chain arg is ignored for a self-describing CAIP-19 token.
    via_resolve = resolver.resolve(f"eip155:1/erc20:{DAI_ETHEREUM}", "ignored", skip_gateway=True)
    via_caip = resolver.resolve_caip19(f"eip155:1/erc20:{DAI_ETHEREUM}", skip_gateway=True)
    assert via_resolve == via_caip


def test_resolve_caip19_native_slip44() -> None:
    resolver = get_token_resolver()
    native = resolver.resolve_caip19("eip155:1/slip44:60", skip_gateway=True)
    assert native.is_native
    assert native.chain is Chain.ETHEREUM


@pytest.mark.parametrize(
    "bad",
    [
        "eip155:1/slip44:501",  # 501 is SOL, not Ethereum's native ETH (60)
        f"eip155:1/erc721:{DAI_ETHEREUM}",  # erc721 unsupported in Phase 1
        f"eip155:1/token:{DAI_ETHEREUM}",  # 'token' (SPL) namespace on an EVM chain
    ],
)
def test_resolve_caip19_rejects_semantic_mismatch(bad: str) -> None:
    # Grammatically valid but semantically invalid CAIP-19 ids must NOT silently
    # alias to a real asset — they are rejected rather than resolved.
    resolver = get_token_resolver()
    with pytest.raises(ValueError):
        resolver.resolve_caip19(bad, skip_gateway=True)


def test_caip2_chain_arg_accepted_by_resolver() -> None:
    resolver = get_token_resolver()
    via_caip2 = resolver.resolve("USDC", "eip155:42161", skip_gateway=True)
    via_name = resolver.resolve("USDC", "arbitrum", skip_gateway=True)
    assert via_caip2 == via_name


def test_resolved_token_round_trips_through_caip19() -> None:
    resolver = get_token_resolver()
    resolved = resolver.resolve(DAI_ETHEREUM, "ethereum", skip_gateway=True)
    caip19 = resolved.token_ref.to_caip19()
    assert caip19 == f"eip155:1/erc20:{DAI_ETHEREUM}"
    assert resolver.resolve_caip19(caip19, skip_gateway=True) == resolved
