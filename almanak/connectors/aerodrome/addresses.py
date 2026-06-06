"""Aerodrome (and Velodrome V2 on Optimism) contract addresses per chain.

Single source of truth for this connector's on-chain addresses. Replaces
the entries previously held in ``almanak.core.contracts`` (W1 / VIB-4853
/ epic VIB-4851). Surfaced to non-connector callers through
:class:`GatewayAddressCapability` on ``AerodromeGatewayConnector``;
strategy-side connector code reads the dicts directly.
"""

from __future__ import annotations

AERODROME: dict[str, dict[str, str]] = {
    "base": {
        "router": "0xcF77a3Ba9A5CA399B7c97c74d54e5b1Beb874E43",
        "factory": "0x420DD381b31aEf6683db6B902084cB0FFECe40Da",
        "voter": "0x16613524e02ad97eDfeF371bC883F2F5d6C480A5",
        "cl_router": "0xBE6D8f0d05cC4be24d5167a3eF062215bE6D18a5",
        "cl_factory": "0x5e7BB104d84c7CB9B682AaC2F3d509f5F406809A",
        "cl_nft": "0x827922686190790b37229fd06084350E74485b72",  # Slipstream NonfungiblePositionManager
        "cl_quoter": "0x254cF9E1E6e233aa1AC962CB9B05b2cfeAaE15b0",
    },
    # Velodrome V2 on Optimism — same Solidly fork interface as Aerodrome on Base.
    # Addresses verified on Optimism block explorer (optimistic.etherscan.io).
    "optimism": {
        "router": "0xa062aE8A9c5e11aaA026fc2670B0D65cCc8B2858",
        "factory": "0xF1046053aa5682b4F9a81b5481394DA16BE5FF5a",
        "voter": "0x41C914ee0c7E1A5edCD0295623e6dC557B5aBf3C",
    },
}

AERODROME_TOKENS: dict[str, dict[str, str]] = {
    "base": {
        "ETH": "0xEeeeeEeeeEeEeeEeEeEeeEEEeeeeEeeeeeeeEEeE",
        "WETH": "0x4200000000000000000000000000000000000006",
        "USDC": "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913",
        "USDbC": "0xd9aAEc86B65D86f6A7B5B1b0c42FFA531710b6CA",
        "DAI": "0x50c5725949A6F0c72E6C4a641F24049A917DB0Cb",
        "AERO": "0x940181a94A35A4569E4529A3CDfB74e38FD98631",
        "cbETH": "0x2Ae3F1Ec7F1F5012CFEab0185bfc7aa3cf0DEc22",
        "rETH": "0xB6fe221Fe9EeF5aBa221c348bA20A1Bf5e73624c",
    },
}
__all__ = ["AERODROME", "AERODROME_TOKENS"]
