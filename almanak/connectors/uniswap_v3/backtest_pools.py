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
    "WETH/USDT-3000": "0x4e68Ccd3E89f51C3074ca5072bbAC773960dFa36",
    "WBTC/USDC-3000": "0x99ac8cA7087fA4A2A1FB6357269965A2014ABc35",
    "WBTC/WETH-3000": "0xCBCdF9626bC03E24f779434178A73a0B4bad62eD",
    "USDC/USDT-100": "0x3416cF6C708Da44DB2624D63ea0AAef7113527C6",
    "LINK/WETH-3000": "0xa6Cc3C2531FdaA6Ae1A3CA84c2855806728693e8",
    "UNI/WETH-3000": "0x1d42064Fc4Beb5F8aAF85F4617AE8b3b5B8Bd801",
    "AAVE/WETH-3000": "0x5aB53EE1d50eeF2C1DD3d5402789cd27bB52c1bB",
}
ARBITRUM_POOLS: dict[str, str] = {
    # Verified on-chain 2026-07-16 (token0/token1/fee eth_calls + factory
    # getPool, ALM-2947): USDC keys mean NATIVE USDC (0xaf88...). The bridged
    # USDC.e pool 0xC31E54c7 previously sat under "WETH/USDC-500" and
    # 0xc473e2aE (WETH/USDC 0.3%) was mislabeled "ARB/USDC-500", pricing ARB
    # at WETH's price in TWAP.
    "WETH/USDC-500": "0xC6962004f452bE9203591991D15f6b388e09E8D0",
    "WETH/USDC-3000": "0xc473e2aEE3441BF9240Be85eb122aBB059A3B57c",
    "ARB/USDC-500": "0xb0f6cA40411360c03d41C5fFc5F179b8403CdcF8",
    "WBTC/WETH-500": "0x2f5e87C9312fa29aed5c179E456625D79015299c",
    "GMX/WETH-3000": "0x80A9ae39310abf666A87C743d6ebBD0E8C42158E",
    "LINK/WETH-3000": "0x468b88941e7Cc0B88c1869d68ab6b570bCEF62Ff",
}
BASE_POOLS: dict[str, str] = {
    "WETH/USDC-500": "0xd0b53D9277642d899DF5C87A3966A349A798F224",
    "CBETH/WETH-500": "0x10648BA41B8565907Cfa1496765fA4D95390aa0d",
}
OPTIMISM_POOLS: dict[str, str] = {
    "WETH/USDC-500": "0x85149247691df622eaF1a8Bd0CaFd40BC45154a9",
    "OP/USDC-3000": "0x1C3140aB59d6cAf9fa7459C6f83D4B52ba881d36",
    "WBTC/WETH-500": "0x73B14a78a0D396C521f954532d43fd5fFe385216",
}
POLYGON_POOLS: dict[str, str] = {
    "WETH/USDC-500": "0x45dDa9cb7c25131DF268515131f647d726f50608",
    "WMATIC/USDC-500": "0xA374094527e1673A86dE625aa59517c5dE346d32",
    "WBTC/WETH-500": "0x50eaEDB835021E4A108B7290636d62E9765cc6d7",
}
AVALANCHE_POOLS: dict[str, str] = {
    "WAVAX/USDC-3000": "0xfAe3f424a0a47706811521E3ee268f00cFb5c45E",
}

UNISWAP_V3_POOLS: dict[str, dict[str, str]] = {
    "ethereum": ETHEREUM_POOLS,
    "arbitrum": ARBITRUM_POOLS,
    "base": BASE_POOLS,
    "optimism": OPTIMISM_POOLS,
    "polygon": POLYGON_POOLS,
    "avalanche": AVALANCHE_POOLS,
}

TOKEN_TO_POOL: dict[str, dict[str, str]] = {
    "ETH": {
        "ethereum": "WETH/USDC-500",
        "arbitrum": "WETH/USDC-500",
        "base": "WETH/USDC-500",
        "optimism": "WETH/USDC-500",
        "polygon": "WETH/USDC-500",
    },
    "WETH": {
        "ethereum": "WETH/USDC-500",
        "arbitrum": "WETH/USDC-500",
        "base": "WETH/USDC-500",
        "optimism": "WETH/USDC-500",
        "polygon": "WETH/USDC-500",
    },
    "BTC": {
        "ethereum": "WBTC/USDC-3000",
        "arbitrum": "WBTC/WETH-500",
        "optimism": "WBTC/WETH-500",
        "polygon": "WBTC/WETH-500",
    },
    "WBTC": {
        "ethereum": "WBTC/USDC-3000",
        "arbitrum": "WBTC/WETH-500",
        "optimism": "WBTC/WETH-500",
        "polygon": "WBTC/WETH-500",
    },
    "LINK": {"ethereum": "LINK/WETH-3000", "arbitrum": "LINK/WETH-3000"},
    "UNI": {"ethereum": "UNI/WETH-3000"},
    "AAVE": {"ethereum": "AAVE/WETH-3000"},
    "ARB": {"arbitrum": "ARB/USDC-500"},
    "GMX": {"arbitrum": "GMX/WETH-3000"},
    "OP": {"optimism": "OP/USDC-3000"},
    "CBETH": {"base": "CBETH/WETH-500"},
    "MATIC": {"polygon": "WMATIC/USDC-500"},
    "WMATIC": {"polygon": "WMATIC/USDC-500"},
    "AVAX": {"avalanche": "WAVAX/USDC-3000"},
    "WAVAX": {"avalanche": "WAVAX/USDC-3000"},
}

# Single manifest-declared entry point (DexVolumeDecl.twap_reference_pools).
TWAP_REFERENCE_POOLS: dict[str, dict] = {
    "pools": UNISWAP_V3_POOLS,
    "token_to_pool": TOKEN_TO_POOL,
}

__all__ = ["TOKEN_TO_POOL", "TWAP_REFERENCE_POOLS", "UNISWAP_V3_POOLS"]
