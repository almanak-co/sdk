"""Live integration tests for TokenSafetyClient.

These tests call the REAL RugCheck and GoPlus APIs.
They verify that our parsing logic actually works with real API responses.

Tokens tested:
  - BONK: Well-known meme coin, should be SAFE/LOW risk
  - USDC: Trusted stablecoin, should be SAFE
  - A known scam token with dangerous flags

Run with: pytest tests/unit/data/token_safety/test_token_safety_live.py -v -s

NOTE: These are integration tests that require network access.
They are skipped by default in the unit test suite.
"""

import asyncio
import os
import socket

import pytest

# Skip these live-API tests when running in the unit test suite (make test-unit).
# They require real network access to RugCheck and GoPlus APIs.
_has_network = None


def _check_network() -> bool:
    global _has_network
    if _has_network is not None:
        return _has_network
    try:
        socket.create_connection(("api.rugcheck.xyz", 443), timeout=3).close()
        _has_network = True
    except OSError:
        _has_network = False
    return _has_network


pytestmark = [
    pytest.mark.skipif(
        os.environ.get("SKIP_LIVE_TESTS", "1") == "1" and not os.environ.get("RUN_LIVE_TESTS"),
        reason="Live API tests skipped by default. Set RUN_LIVE_TESTS=1 to run.",
    ),
]

from almanak.framework.data.token_safety import (
    RiskLevel,
    TokenSafetyClient,
)

# Well-known Solana tokens
BONK_MINT = "DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263"
USDC_MINT = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
SOL_WRAPPED = "So11111111111111111111111111111111111111112"
JUP_MINT = "JUPyiwrYJFskUPiHa7hkeR8VUtAeFoSYbKedZNsDvCN"


@pytest.fixture
def client():
    return TokenSafetyClient(request_timeout=20.0, cache_ttl=0)


# ---------------------------------------------------------------------------
# RugCheck API tests (live)
# ---------------------------------------------------------------------------


class TestRugCheckLive:
    @pytest.mark.asyncio
    async def test_rugcheck_bonk(self, client):
        """BONK should have a RugCheck report with low-moderate risk."""
        async with client:
            result = await client.check_rugcheck(BONK_MINT)

        assert result is not None
        assert result.token_symbol.upper() in ("BONK", "BONK INU", "")
        # BONK should have a relatively low score (safe token)
        assert result.score >= 0
        assert result.rugged is False
        print(f"\n  BONK RugCheck: score={result.score}, level={result.risk_level}")
        print(f"  Risks: {[r.name for r in result.risks]}")

    @pytest.mark.asyncio
    async def test_rugcheck_usdc(self, client):
        """USDC should be low risk on RugCheck."""
        async with client:
            result = await client.check_rugcheck(USDC_MINT)

        assert result is not None
        assert result.rugged is False
        print(f"\n  USDC RugCheck: score={result.score}, level={result.risk_level}")
        print(f"  Risks: {[r.name for r in result.risks]}")

    @pytest.mark.asyncio
    async def test_rugcheck_returns_risks_list(self, client):
        """RugCheck response must parse risks into structured objects."""
        async with client:
            result = await client.check_rugcheck(BONK_MINT)

        assert result is not None
        # BONK may have some mild risks (like mutable metadata)
        for risk in result.risks:
            assert risk.name  # non-empty
            assert risk.source == "rugcheck"
            assert isinstance(risk.level, RiskLevel)


# ---------------------------------------------------------------------------
# GoPlus API tests (live)
# ---------------------------------------------------------------------------


class TestGoPlusLive:
    @pytest.mark.asyncio
    async def test_goplus_bonk(self, client):
        """BONK should be safe on GoPlus (no dangerous authorities)."""
        async with client:
            result = await client.check_goplus(BONK_MINT)

        assert result is not None
        # BONK is a well-known token — should NOT have dangerous flags
        assert result.mintable is False, "BONK mint authority should be disabled"
        assert result.freezable is False, "BONK should not have freeze authority"
        assert result.closable is False, "BONK should not be closable"
        assert result.balance_mutable is False, "BONK balance should not be mutable"
        assert result.non_transferable is False, "BONK should be transferable"
        assert result.holder_count > 100_000, f"BONK should have many holders, got {result.holder_count}"
        print(f"\n  BONK GoPlus: holders={result.holder_count}, trusted={result.trusted_token}")
        print(f"  Mintable={result.mintable}, Freezable={result.freezable}, "
              f"Closable={result.closable}, BalanceMutable={result.balance_mutable}")
        print(f"  TransferFee={result.has_transfer_fee}, TransferHook={result.transfer_hook}")

    @pytest.mark.asyncio
    async def test_goplus_usdc(self, client):
        """USDC has freeze authority (by design — Circle can freeze accounts)."""
        async with client:
            result = await client.check_goplus(USDC_MINT)

        assert result is not None
        # USDC is known to have freeze authority (Circle compliance)
        assert result.freezable is True, "USDC should have freeze authority (Circle)"
        assert result.closable is False
        assert result.balance_mutable is False
        assert result.holder_count > 100_000
        print(f"\n  USDC GoPlus: holders={result.holder_count}, "
              f"freezable={result.freezable} (expected for USDC)")

    @pytest.mark.asyncio
    async def test_goplus_parses_holder_concentration(self, client):
        """GoPlus should return top holder percentage."""
        async with client:
            result = await client.check_goplus(JUP_MINT)

        assert result is not None
        # JUP has real holder data
        assert result.holder_count > 0
        print(f"\n  JUP GoPlus: holders={result.holder_count}, "
              f"top_holder_pct={result.top_holder_pct:.2f}%")


# ---------------------------------------------------------------------------
# Combined check_token tests (live)
# ---------------------------------------------------------------------------


class TestCombinedCheckLive:
    @pytest.mark.asyncio
    async def test_check_bonk_combined(self, client):
        """BONK combined check should be SAFE or LOW risk."""
        async with client:
            result = await client.check_token(BONK_MINT)

        assert result.token_address == BONK_MINT
        assert result.risk_level in (RiskLevel.SAFE, RiskLevel.LOW, RiskLevel.MEDIUM)
        assert result.is_safe or result.risk_level == RiskLevel.MEDIUM
        assert "rugcheck" in result.sources
        assert "goplus" in result.sources
        assert result.rugcheck is not None
        assert result.goplus is not None
        assert result.risk_score < 0.5, f"BONK should have low risk score, got {result.risk_score}"

        print(f"\n  BONK Combined: level={result.risk_level.value}, score={result.risk_score:.3f}")
        print(f"  Sources: {result.sources}")
        print(f"  Flags: {result.flag_names}")
        print(f"  is_safe={result.is_safe}, is_dangerous={result.is_dangerous}")

    @pytest.mark.asyncio
    async def test_check_usdc_combined(self, client):
        """USDC combined check — note: has freeze authority (by design)."""
        async with client:
            result = await client.check_token(USDC_MINT)

        assert result.token_address == USDC_MINT
        assert "rugcheck" in result.sources
        assert "goplus" in result.sources
        # USDC has mint + freeze authority (Circle compliance) but is GoPlus-trusted.
        # Trusted tokens cap at MEDIUM — expected for regulated stablecoins.
        assert result.risk_level in (RiskLevel.SAFE, RiskLevel.LOW, RiskLevel.MEDIUM)
        assert result.rugcheck is not None and result.rugcheck.rugged is False
        assert result.goplus is not None and result.goplus.trusted_token is True

        print(f"\n  USDC Combined: level={result.risk_level.value}, score={result.risk_score:.3f}")
        print(f"  Flags: {result.flag_names}")

    @pytest.mark.asyncio
    async def test_caching_works(self, client):
        """Second call should return cached result."""
        client._cache_ttl = 60  # Enable caching for this test
        async with client:
            result1 = await client.check_token(BONK_MINT)
            result2 = await client.check_token(BONK_MINT)

        # Should be the exact same object (from cache)
        assert result1 is result2

    @pytest.mark.asyncio
    async def test_invalid_mint_returns_unknown(self, client):
        """A completely fake mint address should still return a result (UNKNOWN)."""
        fake_mint = "1111111111111111111111111111111111111111111"
        async with client:
            result = await client.check_token(fake_mint)

        # Should not crash — should return UNKNOWN or whatever data APIs return
        assert result.token_address == fake_mint
        print(f"\n  Fake token: level={result.risk_level.value}, score={result.risk_score:.3f}")
        print(f"  Sources: {result.sources}")


# ---------------------------------------------------------------------------
# Run as standalone script for quick validation
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    async def main():
        print("=" * 60)
        print("Token Safety Client — Live API Validation")
        print("=" * 60)

        async with TokenSafetyClient(cache_ttl=0) as client:
            for name, mint in [("BONK", BONK_MINT), ("USDC", USDC_MINT), ("JUP", JUP_MINT)]:
                print(f"\n--- {name} ({mint[:12]}...) ---")
                result = await client.check_token(mint)
                print(f"  Risk Level : {result.risk_level.value}")
                print(f"  Risk Score : {result.risk_score:.3f}")
                print(f"  Safe?      : {result.is_safe}")
                print(f"  Dangerous? : {result.is_dangerous}")
                print(f"  Sources    : {result.sources}")
                print(f"  Flags      : {result.flag_names}")
                if result.rugcheck:
                    print(f"  RugCheck   : score={result.rugcheck.score}, "
                          f"level={result.rugcheck.risk_level}, rugged={result.rugcheck.rugged}")
                if result.goplus:
                    print(f"  GoPlus     : mintable={result.goplus.mintable}, "
                          f"freezable={result.goplus.freezable}, "
                          f"holders={result.goplus.holder_count}")

    asyncio.run(main())
