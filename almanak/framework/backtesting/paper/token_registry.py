"""Token registry for address-to-symbol resolution.

Provides a structured registry mapping token addresses to their metadata
(symbol, decimals) for use in receipt processing and portfolio tracking.

This module enables Paper Trader to display human-readable token symbols
instead of raw addresses in portfolio reports and trade records.

Internally delegates to TokenResolver for unified token resolution, with
TOKEN_REGISTRY as a local fallback for backward compatibility.

Example:
    >>> from almanak.framework.backtesting.paper.token_registry import (
    ...     TOKEN_REGISTRY, get_token_info, CHAIN_ID_ETHEREUM
    ... )
    >>> info = get_token_info(CHAIN_ID_ETHEREUM, "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48")
    >>> info.symbol
    'USDC'
    >>> info.decimals
    6

    # Async lookup with on-chain fallback
    >>> symbol = await get_token_symbol_with_fallback(
    ...     CHAIN_ID_ETHEREUM,
    ...     "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
    ...     "https://eth.llamarpc.com"
    ... )
    >>> symbol
    'USDC'
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from web3 import AsyncWeb3

logger = logging.getLogger(__name__)

# Reverse mapping from chain_id (int) -> chain name (str) for TokenResolver calls
_CHAIN_ID_TO_NAME: dict[int, str] = {
    1: "ethereum",
    42161: "arbitrum",
    10: "optimism",
    8453: "base",
    43114: "avalanche",
    137: "polygon",
    56: "bsc",
    146: "sonic",
    9745: "plasma",
    81457: "blast",
    5000: "mantle",
    80094: "berachain",
}


def _get_resolver():
    """Lazy import and return the TokenResolver singleton.

    Uses lazy import to avoid circular dependencies and import-time overhead.
    Returns None if TokenResolver is not available (should not happen in practice).
    """
    try:
        from almanak.framework.data.tokens.resolver import get_token_resolver

        return get_token_resolver()
    except Exception:
        logger.debug("TokenResolver not available, using local TOKEN_REGISTRY only")
        return None


# =============================================================================
# Chain IDs (EIP-155)
# =============================================================================
CHAIN_ID_ETHEREUM = 1
CHAIN_ID_ARBITRUM = 42161
CHAIN_ID_BASE = 8453
CHAIN_ID_OPTIMISM = 10
CHAIN_ID_POLYGON = 137
CHAIN_ID_AVALANCHE = 43114
CHAIN_ID_BSC = 56

# Native token sentinel addresses
NATIVE_ETH_ADDRESS = "0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
NATIVE_AVAX_ADDRESS = "0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
NATIVE_MATIC_ADDRESS = "0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
NATIVE_BNB_ADDRESS = "0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"


@dataclass(frozen=True)
class TokenInfo:
    """Immutable token metadata.

    Attributes:
        symbol: Human-readable token symbol (e.g., 'USDC', 'WETH')
        decimals: Number of decimal places for token amounts
        address: Token contract address (lowercase, checksummed format not required)

    Example:
        >>> usdc = TokenInfo(symbol="USDC", decimals=6, address="0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48")
        >>> usdc.symbol
        'USDC'
    """

    symbol: str
    decimals: int
    address: str


# =============================================================================
# Token Registry
# =============================================================================
# Structure: chain_id -> lowercase_address -> TokenInfo
# Addresses are stored lowercase for case-insensitive matching

TOKEN_REGISTRY: dict[int, dict[str, TokenInfo]] = {
    # =========================================================================
    # Ethereum Mainnet (Chain ID: 1)
    # =========================================================================
    CHAIN_ID_ETHEREUM: {
        # Native ETH (sentinel address)
        NATIVE_ETH_ADDRESS: TokenInfo(
            symbol="ETH",
            decimals=18,
            address=NATIVE_ETH_ADDRESS,
        ),
        # WETH - Wrapped Ether
        "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2": TokenInfo(
            symbol="WETH",
            decimals=18,
            address="0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2",
        ),
        # Stablecoins
        "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48": TokenInfo(
            symbol="USDC",
            decimals=6,
            address="0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
        ),
        "0xdac17f958d2ee523a2206206994597c13d831ec7": TokenInfo(
            symbol="USDT",
            decimals=6,
            address="0xdac17f958d2ee523a2206206994597c13d831ec7",
        ),
        "0x6b175474e89094c44da98b954eedeac495271d0f": TokenInfo(
            symbol="DAI",
            decimals=18,
            address="0x6b175474e89094c44da98b954eedeac495271d0f",
        ),
        "0x853d955acef822db058eb8505911ed77f175b99e": TokenInfo(
            symbol="FRAX",
            decimals=18,
            address="0x853d955acef822db058eb8505911ed77f175b99e",
        ),
        "0x5f98805a4e8be255a32880fdec7f6728c6568ba0": TokenInfo(
            symbol="LUSD",
            decimals=18,
            address="0x5f98805a4e8be255a32880fdec7f6728c6568ba0",
        ),
        "0x83f20f44975d03b1b09e64809b757c47f942beea": TokenInfo(
            symbol="sDAI",
            decimals=18,
            address="0x83f20f44975d03b1b09e64809b757c47f942beea",
        ),
        # WBTC - Wrapped Bitcoin
        "0x2260fac5e5542a773aa44fbcfedf7c193bc2c599": TokenInfo(
            symbol="WBTC",
            decimals=8,
            address="0x2260fac5e5542a773aa44fbcfedf7c193bc2c599",
        ),
        # Liquid Staking Tokens
        "0xae7ab96520de3a18e5e111b5eaab095312d7fe84": TokenInfo(
            symbol="stETH",
            decimals=18,
            address="0xae7ab96520de3a18e5e111b5eaab095312d7fe84",
        ),
        "0x7f39c581f595b53c5cb19bd0b3f8da6c935e2ca0": TokenInfo(
            symbol="wstETH",
            decimals=18,
            address="0x7f39c581f595b53c5cb19bd0b3f8da6c935e2ca0",
        ),
        "0xae78736cd615f374d3085123a210448e74fc6393": TokenInfo(
            symbol="rETH",
            decimals=18,
            address="0xae78736cd615f374d3085123a210448e74fc6393",
        ),
        "0xbe9895146f7af43049ca1c1ae358b0541ea49704": TokenInfo(
            symbol="cbETH",
            decimals=18,
            address="0xbe9895146f7af43049ca1c1ae358b0541ea49704",
        ),
        # Protocol Tokens
        "0x514910771af9ca656af840dff83e8264ecf986ca": TokenInfo(
            symbol="LINK",
            decimals=18,
            address="0x514910771af9ca656af840dff83e8264ecf986ca",
        ),
        "0x1f9840a85d5af5bf1d1762f925bdaddc4201f984": TokenInfo(
            symbol="UNI",
            decimals=18,
            address="0x1f9840a85d5af5bf1d1762f925bdaddc4201f984",
        ),
        "0x7fc66500c84a76ad7e9c93437bfc5ac33e2ddae9": TokenInfo(
            symbol="AAVE",
            decimals=18,
            address="0x7fc66500c84a76ad7e9c93437bfc5ac33e2ddae9",
        ),
        "0xc00e94cb662c3520282e6f5717214004a7f26888": TokenInfo(
            symbol="COMP",
            decimals=18,
            address="0xc00e94cb662c3520282e6f5717214004a7f26888",
        ),
        "0xd533a949740bb3306d119cc777fa900ba034cd52": TokenInfo(
            symbol="CRV",
            decimals=18,
            address="0xd533a949740bb3306d119cc777fa900ba034cd52",
        ),
        "0xba100000625a3754423978a60c9317c58a424e3d": TokenInfo(
            symbol="BAL",
            decimals=18,
            address="0xba100000625a3754423978a60c9317c58a424e3d",
        ),
        "0x9f8f72aa9304c8b593d555f12ef6589cc3a579a2": TokenInfo(
            symbol="MKR",
            decimals=18,
            address="0x9f8f72aa9304c8b593d555f12ef6589cc3a579a2",
        ),
    },
    # =========================================================================
    # Arbitrum One (Chain ID: 42161)
    # =========================================================================
    CHAIN_ID_ARBITRUM: {
        # Native ETH (sentinel address)
        NATIVE_ETH_ADDRESS: TokenInfo(
            symbol="ETH",
            decimals=18,
            address=NATIVE_ETH_ADDRESS,
        ),
        # WETH - Wrapped Ether
        "0x82af49447d8a07e3bd95bd0d56f35241523fbab1": TokenInfo(
            symbol="WETH",
            decimals=18,
            address="0x82af49447d8a07e3bd95bd0d56f35241523fbab1",
        ),
        # Stablecoins
        "0xaf88d065e77c8cc2239327c5edb3a432268e5831": TokenInfo(
            symbol="USDC",
            decimals=6,
            address="0xaf88d065e77c8cc2239327c5edb3a432268e5831",
        ),
        "0xff970a61a04b1ca14834a43f5de4533ebddb5cc8": TokenInfo(
            symbol="USDC.e",
            decimals=6,
            address="0xff970a61a04b1ca14834a43f5de4533ebddb5cc8",
        ),
        "0xfd086bc7cd5c481dcc9c85ebe478a1c0b69fcbb9": TokenInfo(
            symbol="USDT",
            decimals=6,
            address="0xfd086bc7cd5c481dcc9c85ebe478a1c0b69fcbb9",
        ),
        "0xda10009cbd5d07dd0cecc66161fc93d7c9000da1": TokenInfo(
            symbol="DAI",
            decimals=18,
            address="0xda10009cbd5d07dd0cecc66161fc93d7c9000da1",
        ),
        "0x17fc002b466eec40dae837fc4be5c67993ddbd6f": TokenInfo(
            symbol="FRAX",
            decimals=18,
            address="0x17fc002b466eec40dae837fc4be5c67993ddbd6f",
        ),
        "0x93b346b6bc2548da6a1e7d98e9a421b42541425b": TokenInfo(
            symbol="LUSD",
            decimals=18,
            address="0x93b346b6bc2548da6a1e7d98e9a421b42541425b",
        ),
        # WBTC - Wrapped Bitcoin
        "0x2f2a2543b76a4166549f7aab2e75bef0aefc5b0f": TokenInfo(
            symbol="WBTC",
            decimals=8,
            address="0x2f2a2543b76a4166549f7aab2e75bef0aefc5b0f",
        ),
        # Liquid Staking Tokens
        "0x5979d7b546e38e414f7e9822514be443a4800529": TokenInfo(
            symbol="wstETH",
            decimals=18,
            address="0x5979d7b546e38e414f7e9822514be443a4800529",
        ),
        "0xec70dcb4a1efa46b8f2d97c310c9c4790ba5ffa8": TokenInfo(
            symbol="rETH",
            decimals=18,
            address="0xec70dcb4a1efa46b8f2d97c310c9c4790ba5ffa8",
        ),
        # Protocol Tokens
        "0x912ce59144191c1204e64559fe8253a0e49e6548": TokenInfo(
            symbol="ARB",
            decimals=18,
            address="0x912ce59144191c1204e64559fe8253a0e49e6548",
        ),
        "0xfc5a1a6eb076a2c7ad06ed22c90d7e710e35ad0a": TokenInfo(
            symbol="GMX",
            decimals=18,
            address="0xfc5a1a6eb076a2c7ad06ed22c90d7e710e35ad0a",
        ),
        "0xf97f4df75117a78c1a5a0dbb814af92458539fb4": TokenInfo(
            symbol="LINK",
            decimals=18,
            address="0xf97f4df75117a78c1a5a0dbb814af92458539fb4",
        ),
        "0xfa7f8980b0f1e64a2062791cc3b0871572f1f7f0": TokenInfo(
            symbol="UNI",
            decimals=18,
            address="0xfa7f8980b0f1e64a2062791cc3b0871572f1f7f0",
        ),
    },
    # =========================================================================
    # Base (Chain ID: 8453)
    # =========================================================================
    CHAIN_ID_BASE: {
        # Native ETH (sentinel address)
        NATIVE_ETH_ADDRESS: TokenInfo(
            symbol="ETH",
            decimals=18,
            address=NATIVE_ETH_ADDRESS,
        ),
        # WETH - Wrapped Ether
        "0x4200000000000000000000000000000000000006": TokenInfo(
            symbol="WETH",
            decimals=18,
            address="0x4200000000000000000000000000000000000006",
        ),
        # Stablecoins
        "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913": TokenInfo(
            symbol="USDC",
            decimals=6,
            address="0x833589fcd6edb6e08f4c7c32d4f71b54bda02913",
        ),
        "0xd9aaec86b65d86f6a7b5b1b0c42ffa531710b6ca": TokenInfo(
            symbol="USDbC",
            decimals=6,
            address="0xd9aaec86b65d86f6a7b5b1b0c42ffa531710b6ca",
        ),
        "0x50c5725949a6f0c72e6c4a641f24049a917db0cb": TokenInfo(
            symbol="DAI",
            decimals=18,
            address="0x50c5725949a6f0c72e6c4a641f24049a917db0cb",
        ),
        # Liquid Staking Tokens
        "0xc1cba3fcea344f92d9239c08c0568f6f2f0ee452": TokenInfo(
            symbol="wstETH",
            decimals=18,
            address="0xc1cba3fcea344f92d9239c08c0568f6f2f0ee452",
        ),
        "0x2ae3f1ec7f1f5012cfeab0185bfc7aa3cf0dec22": TokenInfo(
            symbol="cbETH",
            decimals=18,
            address="0x2ae3f1ec7f1f5012cfeab0185bfc7aa3cf0dec22",
        ),
        # Protocol Tokens - Aerodrome
        "0x940181a94a35a4569e4529a3cdfb74e38fd98631": TokenInfo(
            symbol="AERO",
            decimals=18,
            address="0x940181a94a35a4569e4529a3cdfb74e38fd98631",
        ),
    },
    # =========================================================================
    # Optimism (Chain ID: 10)
    # =========================================================================
    CHAIN_ID_OPTIMISM: {
        # Native ETH (sentinel address)
        NATIVE_ETH_ADDRESS: TokenInfo(
            symbol="ETH",
            decimals=18,
            address=NATIVE_ETH_ADDRESS,
        ),
        # WETH - Wrapped Ether
        "0x4200000000000000000000000000000000000006": TokenInfo(
            symbol="WETH",
            decimals=18,
            address="0x4200000000000000000000000000000000000006",
        ),
        # Stablecoins
        "0x0b2c639c533813f4aa9d7837caf62653d097ff85": TokenInfo(
            symbol="USDC",
            decimals=6,
            address="0x0b2c639c533813f4aa9d7837caf62653d097ff85",
        ),
        "0x7f5c764cbc14f9669b88837ca1490cca17c31607": TokenInfo(
            symbol="USDC.e",
            decimals=6,
            address="0x7f5c764cbc14f9669b88837ca1490cca17c31607",
        ),
        "0x94b008aa00579c1307b0ef2c499ad98a8ce58e58": TokenInfo(
            symbol="USDT",
            decimals=6,
            address="0x94b008aa00579c1307b0ef2c499ad98a8ce58e58",
        ),
        "0xda10009cbd5d07dd0cecc66161fc93d7c9000da1": TokenInfo(
            symbol="DAI",
            decimals=18,
            address="0xda10009cbd5d07dd0cecc66161fc93d7c9000da1",
        ),
        "0x2e3d870790dc77a83dd1d18184acc7439a53f475": TokenInfo(
            symbol="FRAX",
            decimals=18,
            address="0x2e3d870790dc77a83dd1d18184acc7439a53f475",
        ),
        "0xc40f949f8a4e094d1b49a23ea9241d289b7b2819": TokenInfo(
            symbol="LUSD",
            decimals=18,
            address="0xc40f949f8a4e094d1b49a23ea9241d289b7b2819",
        ),
        # WBTC - Wrapped Bitcoin
        "0x68f180fcce6836688e9084f035309e29bf0a2095": TokenInfo(
            symbol="WBTC",
            decimals=8,
            address="0x68f180fcce6836688e9084f035309e29bf0a2095",
        ),
        # Liquid Staking Tokens
        "0x1f32b1c2345538c0c6f582fcb022739c4a194ebb": TokenInfo(
            symbol="wstETH",
            decimals=18,
            address="0x1f32b1c2345538c0c6f582fcb022739c4a194ebb",
        ),
        "0x9bcef72be871e61ed4fbbc7630889bee758eb81d": TokenInfo(
            symbol="rETH",
            decimals=18,
            address="0x9bcef72be871e61ed4fbbc7630889bee758eb81d",
        ),
        # Protocol Tokens
        "0x4200000000000000000000000000000000000042": TokenInfo(
            symbol="OP",
            decimals=18,
            address="0x4200000000000000000000000000000000000042",
        ),
        "0x350a791bfc2c21f9ed5d10980dad2e2638ffa7f6": TokenInfo(
            symbol="LINK",
            decimals=18,
            address="0x350a791bfc2c21f9ed5d10980dad2e2638ffa7f6",
        ),
        # Velodrome
        "0x9560e827af36c94d2ac33a39bce1fe78631088db": TokenInfo(
            symbol="VELO",
            decimals=18,
            address="0x9560e827af36c94d2ac33a39bce1fe78631088db",
        ),
    },
    # =========================================================================
    # Polygon (Chain ID: 137)
    # =========================================================================
    CHAIN_ID_POLYGON: {
        # Native MATIC (sentinel address)
        NATIVE_MATIC_ADDRESS: TokenInfo(
            symbol="MATIC",
            decimals=18,
            address=NATIVE_MATIC_ADDRESS,
        ),
        # WMATIC - Wrapped MATIC
        "0x0d500b1d8e8ef31e21c99d1db9a6444d3adf1270": TokenInfo(
            symbol="WMATIC",
            decimals=18,
            address="0x0d500b1d8e8ef31e21c99d1db9a6444d3adf1270",
        ),
        # WETH - Wrapped Ether
        "0x7ceb23fd6bc0add59e62ac25578270cff1b9f619": TokenInfo(
            symbol="WETH",
            decimals=18,
            address="0x7ceb23fd6bc0add59e62ac25578270cff1b9f619",
        ),
        # Stablecoins
        "0x3c499c542cef5e3811e1192ce70d8cc03d5c3359": TokenInfo(
            symbol="USDC",
            decimals=6,
            address="0x3c499c542cef5e3811e1192ce70d8cc03d5c3359",
        ),
        "0x2791bca1f2de4661ed88a30c99a7a9449aa84174": TokenInfo(
            symbol="USDC.e",
            decimals=6,
            address="0x2791bca1f2de4661ed88a30c99a7a9449aa84174",
        ),
        "0xc2132d05d31c914a87c6611c10748aeb04b58e8f": TokenInfo(
            symbol="USDT",
            decimals=6,
            address="0xc2132d05d31c914a87c6611c10748aeb04b58e8f",
        ),
        "0x8f3cf7ad23cd3cadbd9735aff958023239c6a063": TokenInfo(
            symbol="DAI",
            decimals=18,
            address="0x8f3cf7ad23cd3cadbd9735aff958023239c6a063",
        ),
        "0x45c32fa6df82ead1e2ef74d17b76547eddfaff89": TokenInfo(
            symbol="FRAX",
            decimals=18,
            address="0x45c32fa6df82ead1e2ef74d17b76547eddfaff89",
        ),
        # WBTC - Wrapped Bitcoin
        "0x1bfd67037b42cf73acf2047067bd4f2c47d9bfd6": TokenInfo(
            symbol="WBTC",
            decimals=8,
            address="0x1bfd67037b42cf73acf2047067bd4f2c47d9bfd6",
        ),
        # Liquid Staking Tokens
        "0x03b54a6e9a984069379fae1a4fc4dbae93b3bccd": TokenInfo(
            symbol="wstETH",
            decimals=18,
            address="0x03b54a6e9a984069379fae1a4fc4dbae93b3bccd",
        ),
        "0x0266f4f08d82372cf0fcbccc0ff74309089c74d1": TokenInfo(
            symbol="stMATIC",
            decimals=18,
            address="0x0266f4f08d82372cf0fcbccc0ff74309089c74d1",
        ),
        # Protocol Tokens
        "0x53e0bca35ec356bd5dddfebbd1fc0fd03fabad39": TokenInfo(
            symbol="LINK",
            decimals=18,
            address="0x53e0bca35ec356bd5dddfebbd1fc0fd03fabad39",
        ),
        "0xb33eaad8d922b1083446dc23f610c2567fb5180f": TokenInfo(
            symbol="UNI",
            decimals=18,
            address="0xb33eaad8d922b1083446dc23f610c2567fb5180f",
        ),
        "0xd6df932a45c0f255f85145f286ea0b292b21c90b": TokenInfo(
            symbol="AAVE",
            decimals=18,
            address="0xd6df932a45c0f255f85145f286ea0b292b21c90b",
        ),
    },
    # =========================================================================
    # Avalanche C-Chain (Chain ID: 43114)
    # =========================================================================
    CHAIN_ID_AVALANCHE: {
        # Native AVAX (sentinel address)
        NATIVE_AVAX_ADDRESS: TokenInfo(
            symbol="AVAX",
            decimals=18,
            address=NATIVE_AVAX_ADDRESS,
        ),
        # WAVAX - Wrapped AVAX
        "0xb31f66aa3c1e785363f0875a1b74e27b85fd66c7": TokenInfo(
            symbol="WAVAX",
            decimals=18,
            address="0xb31f66aa3c1e785363f0875a1b74e27b85fd66c7",
        ),
        # WETH - Wrapped Ether
        "0x49d5c2bdffac6ce2bfdb6640f4f80f226bc10bab": TokenInfo(
            symbol="WETH.e",
            decimals=18,
            address="0x49d5c2bdffac6ce2bfdb6640f4f80f226bc10bab",
        ),
        # Stablecoins
        "0xb97ef9ef8734c71904d8002f8b6bc66dd9c48a6e": TokenInfo(
            symbol="USDC",
            decimals=6,
            address="0xb97ef9ef8734c71904d8002f8b6bc66dd9c48a6e",
        ),
        "0xa7d7079b0fead91f3e65f86e8915cb59c1a4c664": TokenInfo(
            symbol="USDC.e",
            decimals=6,
            address="0xa7d7079b0fead91f3e65f86e8915cb59c1a4c664",
        ),
        "0x9702230a8ea53601f5cd2dc00fdbc13d4df4a8c7": TokenInfo(
            symbol="USDT",
            decimals=6,
            address="0x9702230a8ea53601f5cd2dc00fdbc13d4df4a8c7",
        ),
        "0xc7198437980c041c805a1edcba50c1ce5db95118": TokenInfo(
            symbol="USDT.e",
            decimals=6,
            address="0xc7198437980c041c805a1edcba50c1ce5db95118",
        ),
        "0xd586e7f844cea2f87f50152665bcbc2c279d8d70": TokenInfo(
            symbol="DAI.e",
            decimals=18,
            address="0xd586e7f844cea2f87f50152665bcbc2c279d8d70",
        ),
        "0xd24c2ad096400b6fbcd2ad8b24e7acbc21a1da64": TokenInfo(
            symbol="FRAX",
            decimals=18,
            address="0xd24c2ad096400b6fbcd2ad8b24e7acbc21a1da64",
        ),
        # WBTC - Wrapped Bitcoin
        "0x50b7545627a5162f82a992c33b87adc75187b218": TokenInfo(
            symbol="WBTC.e",
            decimals=8,
            address="0x50b7545627a5162f82a992c33b87adc75187b218",
        ),
        "0x152b9d0fdc40c096757f570a51e494bd4b943e50": TokenInfo(
            symbol="BTC.b",
            decimals=8,
            address="0x152b9d0fdc40c096757f570a51e494bd4b943e50",
        ),
        # Liquid Staking Tokens
        "0x2b2c81e08f1af8835a78bb2a90ae924ace0ea4be": TokenInfo(
            symbol="sAVAX",
            decimals=18,
            address="0x2b2c81e08f1af8835a78bb2a90ae924ace0ea4be",
        ),
        # Protocol Tokens
        "0x5947bb275c521040051d82396192181b413227a3": TokenInfo(
            symbol="LINK.e",
            decimals=18,
            address="0x5947bb275c521040051d82396192181b413227a3",
        ),
        "0x6e84a6216ea6dacc71ee8e6b0a5b7322eebc0fdd": TokenInfo(
            symbol="JOE",
            decimals=18,
            address="0x6e84a6216ea6dacc71ee8e6b0a5b7322eebc0fdd",
        ),
    },
    # =========================================================================
    # Binance Smart Chain (Chain ID: 56)
    # =========================================================================
    CHAIN_ID_BSC: {
        # Native BNB (sentinel address)
        NATIVE_BNB_ADDRESS: TokenInfo(
            symbol="BNB",
            decimals=18,
            address=NATIVE_BNB_ADDRESS,
        ),
        # WBNB - Wrapped BNB
        "0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c": TokenInfo(
            symbol="WBNB",
            decimals=18,
            address="0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c",
        ),
        # WETH - Wrapped Ether
        "0x2170ed0880ac9a755fd29b2688956bd959f933f8": TokenInfo(
            symbol="ETH",
            decimals=18,
            address="0x2170ed0880ac9a755fd29b2688956bd959f933f8",
        ),
        # Stablecoins
        "0x8ac76a51cc950d9822d68b83fe1ad97b32cd580d": TokenInfo(
            symbol="USDC",
            decimals=18,
            address="0x8ac76a51cc950d9822d68b83fe1ad97b32cd580d",
        ),
        "0x55d398326f99059ff775485246999027b3197955": TokenInfo(
            symbol="USDT",
            decimals=18,
            address="0x55d398326f99059ff775485246999027b3197955",
        ),
        "0xe9e7cea3dedca5984780bafc599bd69add087d56": TokenInfo(
            symbol="BUSD",
            decimals=18,
            address="0xe9e7cea3dedca5984780bafc599bd69add087d56",
        ),
        "0x1af3f329e8be154074d8769d1ffa4ee058b1dbc3": TokenInfo(
            symbol="DAI",
            decimals=18,
            address="0x1af3f329e8be154074d8769d1ffa4ee058b1dbc3",
        ),
        # BTCB - Bitcoin BEP-20
        "0x7130d2a12b9bcbfae4f2634d864a1ee1ce3ead9c": TokenInfo(
            symbol="BTCB",
            decimals=18,
            address="0x7130d2a12b9bcbfae4f2634d864a1ee1ce3ead9c",
        ),
        # Protocol Tokens
        "0xf8a0bf9cf54bb92f17374d9e9a321e6a111a51bd": TokenInfo(
            symbol="LINK",
            decimals=18,
            address="0xf8a0bf9cf54bb92f17374d9e9a321e6a111a51bd",
        ),
        "0xbf5140a22578168fd562dccf235e5d43a02ce9b1": TokenInfo(
            symbol="UNI",
            decimals=18,
            address="0xbf5140a22578168fd562dccf235e5d43a02ce9b1",
        ),
        "0x0e09fabb73bd3ade0a17ecc321fd13a19e81ce82": TokenInfo(
            symbol="CAKE",
            decimals=18,
            address="0x0e09fabb73bd3ade0a17ecc321fd13a19e81ce82",
        ),
    },
}


def get_token_info(chain_id: int, address: str) -> TokenInfo | None:
    """Look up token info from the registry.

    Delegates to TokenResolver for unified resolution, falls back to
    local TOKEN_REGISTRY if resolver is unavailable.

    Args:
        chain_id: EIP-155 chain ID (e.g., 1 for Ethereum, 42161 for Arbitrum)
        address: Token contract address (case-insensitive)

    Returns:
        TokenInfo if found in registry, None otherwise

    Example:
        >>> info = get_token_info(1, "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48")
        >>> info.symbol if info else None
        'USDC'
    """
    normalized_address = address.lower()

    # Try TokenResolver first
    chain_name = _CHAIN_ID_TO_NAME.get(chain_id)
    if chain_name:
        resolver = _get_resolver()
        if resolver:
            try:
                resolved = resolver.resolve(normalized_address, chain_name)
                return TokenInfo(
                    symbol=resolved.symbol,
                    decimals=resolved.decimals,
                    address=resolved.address.lower(),
                )
            except Exception:
                pass  # Fall through to local registry

    # Fallback to local TOKEN_REGISTRY
    chain_registry = TOKEN_REGISTRY.get(chain_id)
    if chain_registry is None:
        return None
    return chain_registry.get(normalized_address)


def get_token_symbol(chain_id: int, address: str) -> str | None:
    """Get token symbol from registry.

    Convenience function that returns just the symbol.

    Args:
        chain_id: EIP-155 chain ID
        address: Token contract address (case-insensitive)

    Returns:
        Token symbol if found, None otherwise

    Example:
        >>> get_token_symbol(1, "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48")
        'USDC'
    """
    info = get_token_info(chain_id, address)
    return info.symbol if info else None


def get_token_decimals(chain_id: int, address: str) -> int | None:
    """Get token decimals from registry.

    Convenience function that returns just the decimals.

    Args:
        chain_id: EIP-155 chain ID
        address: Token contract address (case-insensitive)

    Returns:
        Token decimals if found, None otherwise

    Example:
        >>> get_token_decimals(1, "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48")
        6
    """
    info = get_token_info(chain_id, address)
    return info.decimals if info else None


def is_token_known(chain_id: int, address: str) -> bool:
    """Check if a token is registered in the canonical registry.

    Useful for determining whether a token address can be resolved to
    a canonical symbol or if it requires on-chain lookup.

    Args:
        chain_id: EIP-155 chain ID (e.g., 1 for Ethereum, 42161 for Arbitrum)
        address: Token contract address (case-insensitive)

    Returns:
        True if the token is in the registry, False otherwise

    Example:
        >>> is_token_known(1, "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48")
        True
        >>> is_token_known(1, "0x1234567890123456789012345678901234567890")
        False
    """
    return get_token_info(chain_id, address) is not None


def resolve_to_canonical_symbol(chain_id: int, address: str) -> str:
    """Resolve a token address to its canonical symbol deterministically.

    This function provides a deterministic mapping from token address to symbol,
    ensuring consistent token identification across the backtesting system.

    Unlike get_token_symbol_with_fallback, this function:
    - Does NOT make network calls
    - Returns a deterministic result (either known symbol or checksummed address)
    - Is suitable for use in hot paths where consistency is critical

    The resolution priority is:
    1. If token is in TOKEN_REGISTRY, return the registered symbol
    2. If not found, return the checksummed address as the symbol

    Args:
        chain_id: EIP-155 chain ID (e.g., 1 for Ethereum, 42161 for Arbitrum)
        address: Token contract address (case-insensitive)

    Returns:
        Canonical symbol (e.g., "USDC") if known, or checksummed address if unknown

    Example:
        >>> resolve_to_canonical_symbol(1, "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48")
        'USDC'
        >>> resolve_to_canonical_symbol(1, "0x1234567890123456789012345678901234567890")
        '0x1234567890123456789012345678901234567890'
    """
    symbol = get_token_symbol(chain_id, address)
    if symbol is not None:
        return symbol
    # Return checksummed address as fallback symbol
    return _checksum_address(address)


def get_supported_chain_ids() -> list[int]:
    """Get all chain IDs supported by the token registry.

    Returns:
        List of supported EIP-155 chain IDs

    Example:
        >>> chain_ids = get_supported_chain_ids()
        >>> 1 in chain_ids  # Ethereum
        True
        >>> 42161 in chain_ids  # Arbitrum
        True
    """
    return list(TOKEN_REGISTRY.keys())


def get_all_tokens_for_chain(chain_id: int) -> list[TokenInfo]:
    """Get all registered tokens for a specific chain.

    Args:
        chain_id: EIP-155 chain ID

    Returns:
        List of TokenInfo objects for the chain, empty list if chain not supported

    Example:
        >>> tokens = get_all_tokens_for_chain(1)
        >>> len(tokens) > 0
        True
    """
    chain_registry = TOKEN_REGISTRY.get(chain_id)
    if chain_registry is None:
        return []
    return list(chain_registry.values())


def get_token_count() -> int:
    """Get total number of tokens across all chains in the registry.

    Returns:
        Total count of registered tokens

    Example:
        >>> get_token_count() > 50
        True
    """
    return sum(len(chain_tokens) for chain_tokens in TOKEN_REGISTRY.values())


# =============================================================================
# ERC-20 symbol() function selector
# =============================================================================
# Keccak256("symbol()")[:4] = 0x95d89b41
SYMBOL_SELECTOR = "0x95d89b41"


def _checksum_address(address: str) -> str:
    """Convert address to checksummed format (EIP-55).

    Uses Keccak-256 hash as per EIP-55 specification.

    Args:
        address: Ethereum address (any case)

    Returns:
        Checksummed address string

    Example:
        >>> _checksum_address("0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48")
        '0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48'
    """
    # Import eth_utils for correct keccak256-based checksum
    # This is the standard library used by web3.py
    try:
        from eth_utils import to_checksum_address

        return to_checksum_address(address)
    except ImportError:
        # Fallback: use web3 if eth_utils not directly available
        from web3 import Web3

        return Web3.to_checksum_address(address)


async def _query_symbol_onchain(web3: AsyncWeb3, address: str) -> str | None:
    """Query ERC-20 symbol() from the blockchain.

    Makes an eth_call to the token contract's symbol() function.

    Args:
        web3: AsyncWeb3 instance connected to the chain
        address: Token contract address (checksummed)

    Returns:
        Token symbol if call succeeds and returns valid data, None otherwise
    """
    try:
        result = await web3.eth.call({"to": address, "data": SYMBOL_SELECTOR})  # type: ignore[typeddict-item]

        if len(result) < 64:
            # Symbol is a string, so it should be ABI-encoded
            # Minimum: 32 bytes offset + 32 bytes length = 64 bytes
            return None

        # ABI-decode the string
        # Layout: offset (32 bytes) + length (32 bytes) + data (variable)
        offset = int.from_bytes(result[0:32], byteorder="big")
        length = int.from_bytes(result[offset : offset + 32], byteorder="big")
        symbol_bytes = result[offset + 32 : offset + 32 + length]

        # Decode as UTF-8, strip null bytes
        symbol = symbol_bytes.decode("utf-8").rstrip("\x00")

        if not symbol:
            return None

        return symbol

    except Exception as e:
        logger.debug(f"Failed to query symbol for {address}: {e}")
        return None


async def get_token_symbol_with_fallback(
    chain_id: int,
    address: str,
    rpc_url: str | None = None,
) -> str:
    """Get token symbol with registry lookup and on-chain fallback.

    Attempts to resolve token address to symbol using the following priority:
    1. TokenResolver lookup (unified resolution via cache/registry/gateway)
    2. Local TOKEN_REGISTRY lookup (fallback)
    3. On-chain ERC-20 symbol() call (requires RPC, skipped if rpc_url is None)
    4. Checksummed address as fallback (always succeeds)

    Args:
        chain_id: EIP-155 chain ID (e.g., 1 for Ethereum, 42161 for Arbitrum)
        address: Token contract address (case-insensitive)
        rpc_url: RPC endpoint URL for on-chain fallback queries (optional)

    Returns:
        Token symbol if found, or checksummed address if all lookups fail

    Example:
        >>> symbol = await get_token_symbol_with_fallback(
        ...     1,
        ...     "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
        ...     "https://eth.llamarpc.com"
        ... )
        >>> symbol
        'USDC'
    """
    # 1. Try registry lookup first (delegates to TokenResolver internally)
    symbol = get_token_symbol(chain_id, address)
    if symbol is not None:
        return symbol

    # 2. Try on-chain symbol() query (only if RPC URL provided)
    if rpc_url is not None:
        try:
            from web3 import AsyncHTTPProvider, AsyncWeb3

            web3 = AsyncWeb3(AsyncHTTPProvider(rpc_url))
            checksum_address = web3.to_checksum_address(address)

            symbol = await _query_symbol_onchain(web3, checksum_address)
            if symbol is not None:
                return symbol

        except Exception as e:
            logger.debug(f"On-chain symbol lookup failed for {address}: {e}")

    # 3. Fall back to checksummed address
    fallback_address = _checksum_address(address)
    logger.warning(
        f"Token symbol not found for {address} on chain {chain_id}, using address as fallback: {fallback_address}"
    )
    return fallback_address
