"""Uniswap V3 backtest TWAP reference pools (VIB-4851 Phase D).

Well-known per-chain pool addresses and the default ``token -> pool-key``
resolution the framework TWAP provider (``backtesting/pnl/providers/twap.py``)
uses to turn ``(token, chain)`` into a pool address before issuing the
gateway ``GetDexTwap`` RPC. Connector-owned reference data: declared on the
manifest via ``DexVolumeDecl.twap_reference_pools`` and consumed through
``DexVolumeRegistry`` — never imported by framework code directly.

Dynamic pool discovery through the gateway is the follow-up that retires
these static tables entirely (Phase D plan §5).
"""

from __future__ import annotations

ETHEREUM_POOLS: dict[str, str] = {
    "WETH/USDC-500": "0x88e6A0c2dDD26FEEb64F039a2c41296FcB3f5640",
    "WETH/USDC-3000": "0x8ad599c3A0ff1De082011EFDDc58f1908eb6e6D8",
    "WBTC/WETH-3000": "0xCBCdF9626bC03E24f779434178A73a0B4bad62eD",
}
ARBITRUM_POOLS: dict[str, str] = {
    "WETH/USDC-500": "0xC31E54c7a869B9FcBEcc14363CF510d1c41fa443",
    "WETH/USDC-3000": "0xC6962004f452bE9203591991D15f6b388e09E8D0",
    "WBTC/WETH-500": "0x2f5e87C9312fa29aed5c179E456625D79015299c",
}
BASE_POOLS: dict[str, str] = {
    "WETH/USDC-500": "0xd0b53D9277642d899DF5C87A3966A349A798F224",
}
OPTIMISM_POOLS: dict[str, str] = {
    "WETH/USDC-500": "0x85149247691df622eaF1a8Bd0CaFd40BC45154a9",
}
POLYGON_POOLS: dict[str, str] = {
    "WMATIC/USDC-500": "0xA374094527e1673A86dE625aa59517c5dE346d32",
}

UNISWAP_V3_POOLS: dict[str, dict[str, str]] = {
    "ethereum": ETHEREUM_POOLS,
    "arbitrum": ARBITRUM_POOLS,
    "base": BASE_POOLS,
    "optimism": OPTIMISM_POOLS,
    "polygon": POLYGON_POOLS,
}

TOKEN_TO_POOL: dict[str, dict[str, str]] = {
    "ETH": {
        "ethereum": "WETH/USDC-500",
        "arbitrum": "WETH/USDC-500",
        "base": "WETH/USDC-500",
        "optimism": "WETH/USDC-500",
    },
    "WETH": {
        "ethereum": "WETH/USDC-500",
        "arbitrum": "WETH/USDC-500",
        "base": "WETH/USDC-500",
        "optimism": "WETH/USDC-500",
    },
    "BTC": {
        "ethereum": "WBTC/WETH-3000",
        "arbitrum": "WBTC/WETH-500",
    },
    "WBTC": {
        "ethereum": "WBTC/WETH-3000",
        "arbitrum": "WBTC/WETH-500",
    },
    "MATIC": {"polygon": "WMATIC/USDC-500"},
    "WMATIC": {"polygon": "WMATIC/USDC-500"},
}

# Single manifest-declared entry point (DexVolumeDecl.twap_reference_pools).
TWAP_REFERENCE_POOLS: dict[str, dict] = {
    "pools": UNISWAP_V3_POOLS,
    "token_to_pool": TOKEN_TO_POOL,
}

__all__ = ["TOKEN_TO_POOL", "TWAP_REFERENCE_POOLS", "UNISWAP_V3_POOLS"]
