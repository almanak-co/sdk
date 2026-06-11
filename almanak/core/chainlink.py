"""Shared Chainlink oracle constants used by both gateway pricing and backtesting.

This module is the single source of truth for Chainlink price feed addresses,
function selectors, and token-to-pair mappings. Both the gateway's OnChainPriceSource
and the backtesting ChainlinkDataProvider import from here.
"""

from collections.abc import Mapping
from decimal import Decimal

from almanak.core.chains._helpers import (
    chainlink_chain_ids_map,
    chainlink_eth_denominated_map,
    chainlink_usd_feeds_map,
)

# =============================================================================
# Chainlink Aggregator Function Selectors
# =============================================================================

# latestRoundData() function selector
# Returns: (roundId, answer, startedAt, updatedAt, answeredInRound)
LATEST_ROUND_DATA_SELECTOR = "0xfeaf968c"

# getRoundData(uint80 _roundId) function selector
# Returns: (roundId, answer, startedAt, updatedAt, answeredInRound)
GET_ROUND_DATA_SELECTOR = "0x9a6fc8f5"

# decimals() function selector
DECIMALS_SELECTOR = "0x313ce567"


# =============================================================================
# Chainlink Price Feed Addresses by Chain
# =============================================================================
# Per-chain aggregator addresses live on ``ChainDescriptor.chainlink`` (one
# file per chain under almanak/core/chains/ — VIB-4851 CS-5). The views
# below preserve this module's public lookup surface; feed-SELECTION policy
# (TOKEN_TO_PAIR, staleness, ETH-denominated derivation) stays here.

# Combined price feeds by chain — registry-derived view.
CHAINLINK_PRICE_FEEDS: Mapping[str, Mapping[str, str]] = chainlink_usd_feeds_map()

# Token symbol to pair mapping (for convenience)
# Maps token symbol to the standard Chainlink pair format
TOKEN_TO_PAIR: dict[str, str] = {
    "ETH": "ETH/USD",
    "WETH": "ETH/USD",
    "BTC": "BTC/USD",
    "WBTC": "BTC/USD",
    "LINK": "LINK/USD",
    "USDC": "USDC/USD",
    "USDT": "USDT/USD",
    "DAI": "DAI/USD",
    "AAVE": "AAVE/USD",
    "UNI": "UNI/USD",
    "CRV": "CRV/USD",
    "COMP": "COMP/USD",
    "MKR": "MKR/USD",
    "SNX": "SNX/USD",
    "MATIC": "MATIC/USD",
    "ARB": "ARB/USD",
    "OP": "OP/USD",
    "LDO": "LDO/USD",
    "WSTETH": "WSTETH/USD",
    "STETH": "WSTETH/USD",  # wstETH is the standard Chainlink feed for stETH pricing
    "CBETH": "CBETH/USD",
    "RETH": "RETH/USD",
    "SOL": "SOL/USD",
    "AVAX": "AVAX/USD",
    "WAVAX": "AVAX/USD",
    "GMX": "GMX/USD",
    "PENDLE": "PENDLE/USD",
    "RDNT": "RDNT/USD",
    "MAGIC": "MAGIC/USD",
    "WOO": "WOO/USD",
    "JOE": "JOE/USD",
    "CAKE": "CAKE/USD",
    "BNB": "BNB/USD",
    "WBNB": "BNB/USD",
    "S": "S/USD",
    "WS": "S/USD",  # Wrapped Sonic (wS) uses same S/USD feed
}

# Chainlink heartbeat intervals (seconds) for staleness checks
# Reference: https://docs.chain.link/data-feeds/price-feeds#check-the-timestamp-of-the-latest-answer
CHAINLINK_HEARTBEATS: dict[str, int] = {
    "ETH/USD": 3600,  # 1 hour on most chains
    "BTC/USD": 3600,
    "LINK/USD": 3600,
    "USDC/USD": 86400,  # Stablecoins have 24h heartbeat
    "USDT/USD": 86400,
    "DAI/USD": 3600,
    "default": 3600,  # Default heartbeat for unlisted pairs
}

# Chainlink deviation threshold percentages
# Price updates are triggered when deviation exceeds this threshold
CHAINLINK_DEVIATION_THRESHOLDS: dict[str, Decimal] = {
    "ETH/USD": Decimal("0.5"),  # 0.5% deviation threshold
    "BTC/USD": Decimal("0.5"),
    "LINK/USD": Decimal("1.0"),
    "USDC/USD": Decimal("0.25"),  # Stablecoins have tighter thresholds
    "USDT/USD": Decimal("0.25"),
    "DAI/USD": Decimal("0.25"),
    "default": Decimal("1.0"),
}

# Expected chain IDs for Chainlink-supported chains (for RPC validation).
# Derived from ``ChainDescriptor.chain_id`` (membership == chains with
# feeds) — the legacy literal dict duplicated the descriptor ids verbatim
# and could drift (VIB-4851 CS-5).
CHAINLINK_CHAIN_IDS: Mapping[str, int] = chainlink_chain_ids_map()

# =============================================================================
# ETH-denominated Chainlink feeds for derived USD pricing
# =============================================================================
# Some tokens only have TOKEN/ETH feeds (no TOKEN/USD). For these, the OnChain
# price source computes TOKEN/USD = TOKEN/ETH * ETH/USD. (Why the path exists:
# VIB-4439 — Ethereum's direct WSTETH/USD feed can return empty on some Anvil
# forks, and Base's wstETH feed is an 18-decimal exchange-rate feed, not USD.)
# Per-chain addresses live on ``ChainDescriptor.chainlink.eth_denominated``.
ETH_DENOMINATED_FEEDS: Mapping[str, Mapping[str, str]] = chainlink_eth_denominated_map()

# Token symbol to ETH-denominated pair mapping
TOKEN_TO_ETH_PAIR: dict[str, str] = {
    "WSTETH": "WSTETH/ETH",
    "STETH": "WSTETH/ETH",
}
