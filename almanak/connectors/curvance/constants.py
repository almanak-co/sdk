"""Curvance protocol constants — Monad deployment.

Sources:
    - https://docs.curvance.com/cve/protocol-overview/contract-addresses (2026-04-18 snapshot)
    - docs/internal/curvance-monad-research.md

Per-market cToken and BorrowableCToken addresses are distinct across markets —
Curvance deploys a fresh token pair for each collateral/debt pairing. NEVER
cache a single ``cUSDC`` / ``cWETH`` address; always look up via ``market_id``.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class CurvanceMarket:
    """A single Curvance isolated market.

    Attributes:
        name: Human-readable label (e.g. "WETH-USDC").
        market_manager: MarketManager address — used as the canonical ``market_id``.
        collateral_ctoken: cToken contract (collateral side, ERC-4626 style).
        borrowable_ctoken: BorrowableCToken contract (debt side).
        collateral_symbol: Underlying asset symbol for the collateral side.
        debt_symbol: Underlying asset symbol for the debt side.
        simple_position_manager: SimplePositionManager address for native leverage (may be None).
        native_position_manager: NativePositionManager address (only for native-collateral markets).
    """

    name: str
    market_manager: str
    collateral_ctoken: str
    borrowable_ctoken: str
    collateral_symbol: str
    debt_symbol: str
    simple_position_manager: str | None = None
    native_position_manager: str | None = None


# Protocol-wide contracts on Monad (not market-specific).
CURVANCE_PROTOCOL_CONTRACTS: dict[str, dict[str, str]] = {
    "monad": {
        "central_registry": "0x1310f352f1389969Ece6741671c4B919523912fF",
        "oracle_manager": "0x32faD39e79FAc67f80d1C86CbD1598043e52CDb6",
        "protocol_reader": "0x878cDfc2F3D96a49A5CbD805FAF4F3080768a6d2",
        "simple_zapper": "0x91da3583924263ee0da81a06d01946E95FFFB22E",
        "vault_zapper": "0x9cb6cc5029c23140662636A50c4E8Dc618cC13F1",
        "native_vault_zapper": "0xbfE3612D3Db96dc58F2210e2e7FDfe9F4B8c5Ec0",
    },
}


# Markets are declared once as EIP-55-checksummed tuples, then assembled into
# the lookup dict with lowercased keys at import time. Keeping the string
# literals here checksummed satisfies the production EIP-55 lint.
_MONAD_MARKETS: tuple[CurvanceMarket, ...] = (
    CurvanceMarket(
        name="ezETH-WETH",
        market_manager="0x83840d837E7A3E00bBb0B8501E60E989A8987c37",
        collateral_ctoken="0x20f1A13BfbF85a22Aa59D189861790981372220b",
        borrowable_ctoken="0xa206D51C02c0202a2Eed8E6A757b49Ab13930227",
        collateral_symbol="ezETH",
        debt_symbol="WETH",
        simple_position_manager="0xd8d3E46E52C3AB8FD98D04143984C763B4Bbc584",
    ),
    CurvanceMarket(
        name="WETH-USDC",
        market_manager="0xb3E9E0134354cc91b7FB9F9d6C3ab0dE7854BB49",
        collateral_ctoken="0x8Af00fbbb2601A8F7636EabbF6243B30BEA47D50",
        borrowable_ctoken="0x21aDBb60a5fB909e7F1fB48aACC4569615CD97b5",
        collateral_symbol="WETH",
        debt_symbol="USDC",
        simple_position_manager="0x3A1B2Dc11a81Fe106eC667BaB4056fC72498ff73",
    ),
    CurvanceMarket(
        name="WMON-USDC",
        market_manager="0xa6A2A92F126b79Ee0804845ee6B52899b4491093",
        collateral_ctoken="0x1e240E30E51491546deC3aF16B0b4EAC8Dd110D4",
        borrowable_ctoken="0x8EE9FC28B8Da872c38A496e9dDB9700bb7261774",
        collateral_symbol="WMON",
        debt_symbol="USDC",
        simple_position_manager="0x960c49E523e6A87282D2bC5032d0AeCb35Dc20ef",
    ),
    CurvanceMarket(
        name="WBTC-USDC",
        market_manager="0x01C4a0d396EFE982B1B103BE9910321d34e1aEA9",
        collateral_ctoken="0x3D2Ff9F862D89Ba526a0fC166bD56ABe04EF28d5",
        borrowable_ctoken="0x7C9d4f1695C6282Da5e5509Aa51fC9fb417C6f1d",
        collateral_symbol="WBTC",
        debt_symbol="USDC",
        simple_position_manager="0x8cA51d155C07e91B206Cf11C8D52d5aB082657F6",
    ),
    CurvanceMarket(
        name="aprMON-WMON",
        market_manager="0x5EA0a1Cf3501C954b64902c5e92100b8A2CaB1Ac",
        collateral_ctoken="0xD9E2025b907E95EcC963A5018f56B87575B4aB26",
        borrowable_ctoken="0xF32B334042DC1EB9732454cc9bc1a06205d184f2",
        collateral_symbol="aprMON",
        debt_symbol="WMON",
        simple_position_manager="0x669786FF3da7544c98FE74eE1006D787118d1770",
        native_position_manager="0x82f88A810E86a730699231F5fBF5686c643467C8",
    ),
    CurvanceMarket(
        name="shMON-WMON",
        market_manager="0xE1C24B2E93230FBe33d32Ba38ECA3218284143e2",
        collateral_ctoken="0x926C101Cf0a3dE8725Eb24a93E980f9FE34d6230",
        borrowable_ctoken="0x0fcEd51b526BfA5619F83d97b54a57e3327eB183",
        collateral_symbol="shMON",
        debt_symbol="WMON",
        simple_position_manager="0x7E8705a164DA7Cc5a4119C750b1F79837AD89D5E",
        native_position_manager="0x2fAA792502aC5a329DcCc1580C5308fCc0a772fe",
    ),
)


# Public: per-chain MarketManager->CurvanceMarket lookup, keyed lowercase.
CURVANCE_MARKETS: dict[str, dict[str, CurvanceMarket]] = {
    "monad": {m.market_manager.lower(): m for m in _MONAD_MARKETS},
}


# Default market per chain — used when a strategy omits ``market_id``.
# Chosen to be the bluechip pair most useful for documentation examples.
# Keys are lowercased to match the CURVANCE_MARKETS lookup.
_DEFAULT_MARKET_NAME_BY_CHAIN: dict[str, str] = {"monad": "WETH-USDC"}
DEFAULT_MARKET_BY_CHAIN: dict[str, str] = {
    chain: next(m for m in markets.values() if m.name == _DEFAULT_MARKET_NAME_BY_CHAIN[chain]).market_manager.lower()
    for chain, markets in CURVANCE_MARKETS.items()
    if chain in _DEFAULT_MARKET_NAME_BY_CHAIN
}


SUPPORTED_CHAINS: tuple[str, ...] = ("monad",)


def get_market(chain: str, market_id: str | None) -> CurvanceMarket:
    """Resolve a market by chain + MarketManager address.

    Args:
        chain: Chain name (lowercase).
        market_id: MarketManager address (case-insensitive). If None, uses the
            chain's default market.

    Returns:
        The matching ``CurvanceMarket`` record.

    Raises:
        KeyError: chain not supported.
        ValueError: market_id not registered on this chain.
    """
    chain_lower = chain.lower()
    if chain_lower not in CURVANCE_MARKETS:
        raise KeyError(f"Curvance is not deployed on chain '{chain}'. Supported: {', '.join(SUPPORTED_CHAINS)}")
    markets = CURVANCE_MARKETS[chain_lower]

    if market_id is None:
        default_id = DEFAULT_MARKET_BY_CHAIN.get(chain_lower)
        if default_id is None:
            raise ValueError(f"No default Curvance market for chain '{chain}'")
        return markets[default_id]

    key = market_id.lower()
    if key not in markets:
        known = ", ".join(sorted(m.name for m in markets.values()))
        raise ValueError(f"Unknown Curvance market '{market_id}' on {chain}. Known markets: {known}")
    return markets[key]
