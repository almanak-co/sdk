"""Aster Perps contract addresses per chain.

Single source of truth for this connector's on-chain addresses. Replaces
the entries previously held in ``almanak.core.contracts`` (W1 / VIB-4853
/ epic VIB-4851). Surfaced to non-connector callers through
:class:`GatewayAddressCapability` on ``AsterPerpsGatewayConnector``;
strategy-side connector code reads the dicts directly.

PancakeSwap Perps is powered by Aster (formerly ApolloX, rebranded March
2025). The legacy ``PANCAKESWAP_PERPS_*`` aliases are preserved here so
the ``pancakeswap_perps/`` deprecation shim continues to import them
unchanged.

Three surfaces live here:

* ``ASTER_PERPS`` — per-chain Diamond proxy router for Aster's
  perpetual platform on BSC.
* ``ASTER_PERPS_MARKETS`` — ``pairBase`` addresses for each crypto
  market (BTC/USD, ETH/USD, BNB/USD), keyed by the underlying BSC ERC20.
* ``ASTER_PERPS_TOKENS`` — margin tokens accepted by the router
  (WBNB / USDT / USDC); native BNB is auto-wrapped via
  ``openMarketTradeBNB()``.

Broker-id constants (``PANCAKESWAP_PERPS_BROKER_ID = 2`` for PCS
attribution; ``ASTER_PERPS_BROKER_RAW = 0`` for un-attributed Aster
calls) are also published here — they are not contract addresses, but
they live on the same logical surface (Diamond facet attribution) and
co-locating them keeps the deprecation shim's import path simple.

The contract-kind vocabulary (``router``) is connector-private — callers
outside this folder should consume the gateway registry, not guess key
names.
"""

from __future__ import annotations

# =============================================================================
# PancakeSwap Perps (ApolloX Diamond on BSC, PCS broker id = 2)
# =============================================================================
# Aster Perps (formerly ApolloX; PancakeSwap Perps is broker id = 2 on this venue)
# =============================================================================
# Router is a Diamond proxy (EIP-2535) fronting the Aster perpetual platform.
# Key facets verified on BSCScan:
#   TradingPortalFacet (open/close):    0x5553F3B5E2fAD83edA4031a3894ee59e25ee90bF
#   TradingReaderFacet (views):         0x28dE81Bc5B6164d8522ad32AD7D139A21fa1E3b4
#   TradingOpenFacet (keeper settle):   0xdbe2b7e92f00dBd70478199577393bE5BBe37201
#   TradingCloseFacet (keeper settle):  0x8ECa88449B9AFF247F775B96be6e3479bBE72a09
#   PriceFacadeFacet  (keeper entry):   0x646CbAD1B150E5D3a019827a304717950ba6442e
#   PairsManagerFacet (markets):        0xA32b528D70D1d5bA93a17D2697Efe5D17F1A6F8d
ASTER_PERPS: dict[str, dict[str, str]] = {
    "bsc": {
        "router": "0x1b6F2d3844C6ae7D56ceb3C3643b9060ba28FEb0",
    },
}

# pairBase addresses — each market is keyed by the underlying BSC ERC20 address.
# For crypto markets (v1 scope) these are the real BSC-pegged tokens;
# non-crypto markets (NVDA, TSLA, ...) use synthetic ApolloX-issued contracts and are out of v1 scope.
ASTER_PERPS_MARKETS: dict[str, dict[str, str]] = {
    "bsc": {
        "BTC/USD": "0x7130d2A12B9BCbFAe4f2634d864A1Ee1Ce3Ead9c",  # BTCB
        "ETH/USD": "0x2170Ed0880ac9A755fd29B2688956BD959F933F8",  # ETH (BSC)
        "BNB/USD": "0xbb4CdB9CBd36B01bD1cBaEBF2De08d9173bc095c",  # WBNB
    },
}

# Margin tokens supported by the Aster Perps router on BSC.
# The router accepts native BNB via openMarketTradeBNB() (auto-wraps to WBNB internally)
# and ERC20 collateral via openMarketTrade().
ASTER_PERPS_TOKENS: dict[str, dict[str, str]] = {
    "bsc": {
        "WBNB": "0xbb4CdB9CBd36B01bD1cBaEBF2De08d9173bc095c",
        "USDT": "0x55d398326f99059fF775485246999027B3197955",
        "USDC": "0x8AC76a51cc950d9822D68b83fE1Ad97B32Cd580d",
    },
}

# Broker ids (attribution only — does not affect routing or fills).
PANCAKESWAP_PERPS_BROKER_ID: int = 2  # PancakeSwap Perps attribution id.
ASTER_PERPS_BROKER_RAW: int = 0  # Raw Aster (no broker attribution).

# Backwards-compatibility aliases. Prefer the ASTER_PERPS_* names; the
# PANCAKESWAP_PERPS_* aliases exist for the pancakeswap_perps/ shim and any
# callers still importing the legacy names (pre-VIB-3044 rebrand extraction).
PANCAKESWAP_PERPS: dict[str, dict[str, str]] = ASTER_PERPS
PANCAKESWAP_PERPS_MARKETS: dict[str, dict[str, str]] = ASTER_PERPS_MARKETS
PANCAKESWAP_PERPS_TOKENS: dict[str, dict[str, str]] = ASTER_PERPS_TOKENS


__all__ = [
    "ASTER_PERPS",
    "ASTER_PERPS_MARKETS",
    "ASTER_PERPS_TOKENS",
    "PANCAKESWAP_PERPS",
    "PANCAKESWAP_PERPS_MARKETS",
    "PANCAKESWAP_PERPS_TOKENS",
    "PANCAKESWAP_PERPS_BROKER_ID",
    "ASTER_PERPS_BROKER_RAW",
]
