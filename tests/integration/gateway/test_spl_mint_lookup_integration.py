"""Integration tests for SplMintLookup against real Solana mainnet-beta.

These tests hit the public Solana RPC and verify that the SPL mint account
reader correctly recovers decimals for real long-tail mints — the exact class
of tokens that Jupiter's curated list does not cover.

Gated on network reachability: skip gracefully in CI without outbound access.

To run:
    uv run pytest tests/integration/gateway/test_spl_mint_lookup_integration.py -v
"""

from __future__ import annotations

import pytest

from almanak.gateway.services.spl_mint_lookup import (
    SPL_TOKEN_PROGRAM,
    TOKEN_2022_PROGRAM,
    SplMintLookup,
)


SOLANA_RPC_URL = "https://api.mainnet-beta.solana.com"


# Canonical well-known mints. Jupiter's curated list knows these too, but we
# want to prove the direct SPL path works regardless of Jupiter.
KNOWN_MINTS: dict[str, dict[str, object]] = {
    "USDC": {
        "mint": "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
        "decimals": 6,
        "owner": SPL_TOKEN_PROGRAM,
    },
    "SOL (wrapped)": {
        "mint": "So11111111111111111111111111111111111111112",
        "decimals": 9,
        "owner": SPL_TOKEN_PROGRAM,
    },
    "BONK": {
        "mint": "DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263",
        "decimals": 5,
        "owner": SPL_TOKEN_PROGRAM,
    },
    "PYUSD (Token-2022)": {
        "mint": "2b1kV6DkPAnxd5ixfnxCpjxmKwqjjaYmCZfHsFu24GXo",
        "decimals": 6,
        "owner": TOKEN_2022_PROGRAM,
    },
}


@pytest.fixture(scope="module")
def solana_available() -> bool:
    """Skip tests if mainnet-beta is unreachable."""
    import requests

    try:
        resp = requests.post(
            SOLANA_RPC_URL,
            json={"jsonrpc": "2.0", "id": 1, "method": "getHealth"},
            timeout=5,
        )
        return resp.status_code == 200
    except Exception:
        return False


@pytest.fixture
def lookup() -> SplMintLookup:
    return SplMintLookup(rpc_url=SOLANA_RPC_URL, timeout=15.0)


@pytest.mark.integration
class TestSplMintLookupMainnet:
    @pytest.mark.asyncio
    @pytest.mark.parametrize("label", list(KNOWN_MINTS.keys()))
    async def test_known_mint_resolves_with_correct_decimals(
        self,
        label: str,
        lookup: SplMintLookup,
        solana_available: bool,
    ) -> None:
        if not solana_available:
            pytest.skip("Solana mainnet-beta unreachable")

        expected = KNOWN_MINTS[label]
        info = await lookup.lookup(expected["mint"])  # type: ignore[arg-type]

        assert info is not None, f"{label} ({expected['mint']}) should resolve via SPL RPC"
        assert info.decimals == expected["decimals"], (
            f"{label}: expected {expected['decimals']} decimals, got {info.decimals}"
        )
        assert info.owner_program == expected["owner"], (
            f"{label}: expected owner {expected['owner']}, got {info.owner_program}"
        )
        assert info.is_initialized

    @pytest.mark.asyncio
    async def test_invalid_mint_returns_none(
        self,
        lookup: SplMintLookup,
        solana_available: bool,
    ) -> None:
        """A base58-shaped address with no matching account must resolve to None."""
        if not solana_available:
            pytest.skip("Solana mainnet-beta unreachable")

        # Valid base58 format, but deliberately constructed not to exist on-chain.
        info = await lookup.lookup("1nCbfHUJzqNzfA1b3BqrSUwZr5fJBLc9HJjXw2HcFake")
        assert info is None

    @pytest.mark.asyncio
    async def test_system_program_rejected(
        self,
        lookup: SplMintLookup,
        solana_available: bool,
    ) -> None:
        """The System Program account exists but isn't a mint — must be refused."""
        if not solana_available:
            pytest.skip("Solana mainnet-beta unreachable")

        info = await lookup.lookup("11111111111111111111111111111111")
        assert info is None
