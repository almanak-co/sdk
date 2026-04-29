"""Unit test for Arbitrum SOL/USD Chainlink feed registration (VIB-3718 / BUG-49).

`gmx_sol_long_arb` strategy timed out fetching `SOL/USD@arbitrum` because
SOL/USD was not registered in ARBITRUM_PRICE_FEEDS — the gateway's
OnChainPriceSource had no Chainlink address to query, fell through to
DexScreener/CoinGecko/Binance fallbacks, and the strategy hit
StatusCode.DEADLINE_EXCEEDED before any source returned.

Address verified on-chain 2026-04-29 via `description()` returning
"SOL / USD" and `latestAnswer()` returning a sensible Solana price
(answer / 1e8 ≈ $83 at verification).
"""

from almanak.core.chainlink import ARBITRUM_PRICE_FEEDS, TOKEN_TO_PAIR

EXPECTED_ARBITRUM_SOL_USD = "0x24ceA4b8ce57cdA5058b924B9B9987992450590c"


def test_sol_usd_present_on_arbitrum():
    assert "SOL/USD" in ARBITRUM_PRICE_FEEDS


def test_sol_usd_arbitrum_address_matches_canonical():
    """Address must match the on-chain-verified canonical Chainlink proxy."""
    assert ARBITRUM_PRICE_FEEDS["SOL/USD"] == EXPECTED_ARBITRUM_SOL_USD


def test_sol_token_resolves_via_arbitrum_feeds():
    """`market.price("SOL")` on Arbitrum must reach a registered feed."""
    pair = TOKEN_TO_PAIR["SOL"]
    assert pair == "SOL/USD"
    assert pair in ARBITRUM_PRICE_FEEDS
