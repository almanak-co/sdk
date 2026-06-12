"""Morpho Blue Anvil-fork yield-poke function.

Triggers on-chain interest accrual on a persistent Anvil fork by calling
``accrueInterest(MarketParams)`` on the Morpho Blue contract. This updates
the interest index for the catalogued wstETH/USDC market.

Addresses are Ethereum-specific (the only chain supported for Morpho Blue
poking in V1). Additional chains require separate poke functions with
chain-specific contract addresses.
"""

from __future__ import annotations

from almanak.connectors._strategy_base.yield_poke_base import (
    PokeResult,
    _pad_address,
    _pad_uint256,
    _send_tx,
)
from almanak.connectors.morpho_blue.addresses import MORPHO_BLUE, MORPHO_MARKETS

__all__ = ["poke_morpho_blue"]

# ---------------------------------------------------------------------------
# Morpho Blue constants (Ethereum)
# ---------------------------------------------------------------------------
MORPHO_BLUE_ETHEREUM = MORPHO_BLUE["ethereum"]["morpho"]
# accrueInterest((address,address,address,address,uint256))
MORPHO_ACCRUE_SIG = "0x151c1ade"
# Market poked for accrual: wstETH/USDC (86% LLTV), the top-TVL market in the
# connector's MORPHO_MARKETS catalogue. accrueInterest derives the market id
# by hashing ALL MarketParams fields and only checks that the resulting market
# exists, so every field (including oracle/irm/lltv) must match the on-chain
# market config exactly — placeholder values hash to a nonexistent market and
# the poke reverts (PR #2755 review).
POKE_MARKET_ID = "0xb323495f7e4148be5643a4ea4a8221eef163e4bccfdedc2a6f4696baacbc86cc"


def _accrue_interest_calldata(market: dict) -> str:
    """ABI-encode accrueInterest(MarketParams) for a MORPHO_MARKETS entry."""
    return (
        MORPHO_ACCRUE_SIG
        + _pad_address(market["loan_token_address"])
        + _pad_address(market["collateral_token_address"])
        + _pad_address(market["oracle"])
        + _pad_address(market["irm"])
        + _pad_uint256(market["lltv"])
    )


async def poke_morpho_blue(rpc_url: str, wallet_address: str) -> PokeResult:
    """Poke Morpho Blue by calling accrueInterest(MarketParams).

    Triggers the interest index update for the catalogued wstETH/USDC market.
    MarketParams struct: (loanToken, collateralToken, oracle, irm, lltv) —
    resolved from the connector's MORPHO_MARKETS table so the encoded struct
    hashes to a real market id.
    """
    try:
        market = MORPHO_MARKETS["ethereum"][POKE_MARKET_ID]
        data = _accrue_interest_calldata(market)
        tx_hash = await _send_tx(rpc_url, wallet_address, MORPHO_BLUE_ETHEREUM, data)
        return PokeResult(protocol="morpho_blue", success=True, tx_hash=tx_hash)
    except Exception as e:
        return PokeResult(protocol="morpho_blue", success=False, error=str(e))
