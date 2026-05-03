"""Regression guards for VIB-3833 (QA-PostFixes April31 — Solana FAIL diagnosis).

The April-31 verification harness flagged ``lst_depeg_arb`` as
``EXECUTION_FAILED`` with diagnosis pending. Investigation found that two of
the three LSTs the strategy iterates over were already in ``tokens.json``
(JITOSOL, MSOL) but **bSOL (BlazeStake Staked SOL) was missing entirely** — the
same token-registry-gap class that VIB-3816 (XBTC on Solana) closed in Tier 3.

This pins:

1. All three ``lst_depeg_arb`` LST mints (``JitoSOL``, ``mSOL``, ``bSOL``)
   resolve via ``TokenResolver`` for ``chain="solana"``.
2. Their decimals match the SPL on-chain values (9 for all three LSTs).
3. The configured mint address in the strategy config matches the resolver's
   address — a defence against future config-vs-registry drift.

The other Solana strategy filed under VIB-3833 — ``edge_sol_raydium_usds_lp``
— uses USDS, USDC, and SOL, all of which already have Solana entries (USDS
since 2026-04-03 per VIB-2374). Its remaining failure is most likely a
Raydium-CLMM tick-array initialization gap (sister of VIB-3818) or a
strategy-internal config issue; the framework safety-net for Anchor
``InstructionFallbackNotFound`` keyword classification (added by VIB-3817)
already prevents retry-storms if the IDL ever drifts.
"""

from __future__ import annotations

import pytest

from almanak.framework.data.tokens import TokenResolver

LST_MINTS = {
    "JITOSOL": "J1toso1uCk3RLmjorhTtrVwY9HJ7X8V9yYac6Y7kGCPn",
    "MSOL": "mSoLzYCxHdYgdzU16g5QSh3i5K3z3KZK7ytfqcJm7So",
    "BSOL": "bSo13r4TkiE4KumL71LsHTPpL2euBYLFx6h9HP3piy1",
}


@pytest.fixture(autouse=True)
def _reset_token_resolver() -> None:
    TokenResolver.reset_instance()
    yield
    TokenResolver.reset_instance()


@pytest.fixture()
def resolver() -> TokenResolver:
    return TokenResolver()


@pytest.mark.parametrize("symbol,mint", list(LST_MINTS.items()))
def test_lst_resolves_on_solana(
    symbol: str, mint: str, resolver: TokenResolver
) -> None:
    info = resolver.resolve(symbol, "solana")
    assert info is not None, f"{symbol} must resolve on Solana"
    assert info.address == mint
    assert info.decimals == 9, f"{symbol} decimals must be 9 (SPL native)"


def test_bsol_address_not_evm_format(resolver: TokenResolver) -> None:
    """Defence against accidentally pasting an EVM 0x... address into the
    Solana entry — bSOL is SPL-only and has no EVM deployment."""
    info = resolver.resolve("BSOL", "solana")
    assert info is not None
    assert not info.address.startswith("0x")
    # SPL base58 mints are 32-44 chars
    assert 32 <= len(info.address) <= 44


def test_lst_depeg_arb_config_mints_match_registry() -> None:
    """If someone edits the strategy config without updating tokens.json
    (or vice-versa), this test catches the divergence at unit-test time."""
    import json
    from pathlib import Path

    config_path = (
        Path(__file__).parents[3]
        / "strategies"
        / "incubating"
        / "lst_depeg_arb"
        / "config.json"
    )
    config = json.loads(config_path.read_text())

    resolver = TokenResolver()
    for symbol_in_config, entry in config["lst_tokens"].items():
        # config uses mixed-case symbols ("JitoSOL", "mSOL", "bSOL");
        # registry uses upper-case canonical symbols.
        resolved = resolver.resolve(symbol_in_config.upper(), "solana")
        assert resolved is not None, (
            f"Config LST {symbol_in_config!r} (-> {symbol_in_config.upper()}) "
            f"must resolve in registry"
        )
        assert resolved.address == entry["mint"], (
            f"Config mint for {symbol_in_config} ({entry['mint']}) does not "
            f"match registry mint ({resolved.address})"
        )
