"""Default token definitions for the unified token resolution system.

This module provides the authoritative token registry with 50+ tokens
across 8 chains (Ethereum, Arbitrum, Optimism, Base, Polygon, Avalanche, BSC, Plasma).

This is the SINGLE SOURCE OF TRUTH for token addresses and decimals.
All components (IntentCompiler, adapters, price services) should use
the TokenResolver which delegates to this registry.

Tokens included:
    - Native tokens: ETH, MATIC, AVAX, BNB
    - Wrapped native: WETH, WMATIC, WAVAX, WBNB
    - Stablecoins: USDC, USDT, DAI, USDC.e, USDbC, USDT.e, USDe, sUSDe
    - DeFi blue chips: WBTC, LINK, UNI, AAVE, CRV, GMX, PENDLE
    - Chain tokens: ARB, OP
    - LST/LRT tokens: wstETH, WEETH

Authoritative Sources:
    - Official protocol documentation
    - Verified Etherscan/block explorer contracts
    - CoinGecko API (for coingecko_id)
    - Circle (for native USDC addresses)

Last Updated: 2026-02-05
"""

from .models import BridgeType, ChainTokenConfig, Token
from .registry import TokenRegistry

# =============================================================================
# CONSTANTS
# =============================================================================

# Native token sentinel address - used by many protocols to represent native ETH/MATIC/AVAX/etc
# This is a convention, not a real contract
NATIVE_SENTINEL: str = "0xEeeeeEeeeEeEeeEeEeEeeEEEeeeeEeeeeeeeEEeE"

# Wrapped native token addresses per chain
# Maps chain name to the wrapped native token address
WRAPPED_NATIVE: dict[str, str] = {
    "ethereum": "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2",  # WETH
    "arbitrum": "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1",  # WETH
    "optimism": "0x4200000000000000000000000000000000000006",  # WETH
    "base": "0x4200000000000000000000000000000000000006",  # WETH
    "polygon": "0x0d500B1d8E8eF31E21C99d1Db9A6444d3ADf1270",  # WMATIC
    "avalanche": "0xB31f66AA3C1e785363F0875A1B74E27b85FD66c7",  # WAVAX
    "bsc": "0xbb4CdB9CBd36B01bD1cBaEBF2De08d9173bc095c",  # WBNB
    "sonic": "0x039e2fB66102314Ce7b64Ce5Ce3E5183bc94aD38",  # wS (Wrapped Sonic)
    "plasma": "0x6100E367285b01F48D07953803A2d8dCA5D19873",  # WXPL
    "solana": "So11111111111111111111111111111111111111112",  # WSOL
    "mantle": "0x78c1b0C915c4FAA5FffA6CAbf0219DA63d7f4cb8",  # WMNT
    "berachain": "0x6969696969696969696969696969696969696969",  # WBERA
    "monad": "0x3bd359C1119dA7Da1D913D1C4D2B7c461115433A",  # WMON
}

# Set of stablecoin symbols for quick identification
# Includes both native and bridged variants
STABLECOINS: set[str] = {
    "USDC",
    "USDT",
    "DAI",
    "USDC.E",
    "USDBC",
    "USDT.E",
    "USDE",
    "FRAX",
    "LUSD",
    "TUSD",
    "BUSD",
}

# =============================================================================
# SYMBOL ALIASES
# =============================================================================
# Maps (chain, alias_symbol_upper) -> canonical_address_lower
# Used for bridged token resolution (USDC.e -> bridged USDC address)
# All alias symbols should be UPPERCASE, all addresses should be lowercase

SYMBOL_ALIASES: dict[tuple[str, str], str] = {
    # Arbitrum bridged tokens
    ("arbitrum", "USDC.E"): "0xFF970A61A04b1cA14834A43f5dE4533eBDDB5CC8",  # Bridged USDC
    # Optimism bridged tokens
    ("optimism", "USDC.E"): "0x7F5c764cBc14f9669B88837ca1490cCa17c31607",  # Bridged USDC
    # Base bridged tokens
    ("base", "USDBC"): "0xd9aAEc86B65D86f6A7B5B1b0c42FFA531710b6CA",  # USD Base Coin
    # Polygon bridged tokens
    ("polygon", "USDC.E"): "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174",  # Bridged USDC (PoS Bridge)
    # Avalanche bridged tokens
    ("avalanche", "USDC.E"): "0xA7D7079b0FEaD91F3e65f86E8915Cb59c1a4C664",  # Bridged USDC
    ("avalanche", "USDT.E"): "0xc7198437980c041c805A1EDcbA50c1Ce5db95118",  # Bridged USDT
    ("avalanche", "WETH.E"): "0x49D5c2BdFfac6CE2BFdB6640F4F80f226bc10bAB",  # Bridged WETH
    ("avalanche", "BTCB"): "0x152b9D0fDC40c096DE20232db1E35ae6A57fa6C0",  # BTC.b alias (no dot)
    # Berachain bridged tokens
    ("berachain", "USDC.E"): "0x549943e04f40284185054145c6E4e9568C1D3241",  # Bridged USDC (Stargate)
    ("berachain", "USDC"): "0x549943e04f40284185054145c6E4e9568C1D3241",  # USDC -> bridged USDC on Berachain
}

# =============================================================================
# NATIVE TOKENS
# =============================================================================

# Ether (native gas token on Ethereum L1 and L2s)
ETH = Token(
    symbol="ETH",
    name="Ether",
    decimals=18,
    addresses={
        "ethereum": NATIVE_SENTINEL,
        "arbitrum": NATIVE_SENTINEL,
        "optimism": NATIVE_SENTINEL,
        "base": NATIVE_SENTINEL,
    },
    coingecko_id="ethereum",
    is_stablecoin=False,
)

# MATIC (native gas token on Polygon)
MATIC = Token(
    symbol="MATIC",
    name="Polygon",
    decimals=18,
    addresses={
        "ethereum": "0x7D1AfA7B718fb893dB30A3aBc0Cfc608AaCfeBB0",  # MATIC ERC20 on Ethereum
        "polygon": NATIVE_SENTINEL,  # Native on Polygon
    },
    coingecko_id="matic-network",
    is_stablecoin=False,
)

# AVAX (native gas token on Avalanche)
AVAX = Token(
    symbol="AVAX",
    name="Avalanche",
    decimals=18,
    addresses={
        "avalanche": NATIVE_SENTINEL,
    },
    coingecko_id="avalanche-2",
    is_stablecoin=False,
)

# BNB (native gas token on BSC)
BNB = Token(
    symbol="BNB",
    name="Binance Coin",
    decimals=18,
    addresses={
        "bsc": NATIVE_SENTINEL,
    },
    coingecko_id="binancecoin",
    is_stablecoin=False,
)

# =============================================================================
# WRAPPED NATIVE TOKENS
# =============================================================================

# Wrapped Ether
WETH = Token(
    symbol="WETH",
    name="Wrapped Ether",
    decimals=18,
    addresses={
        "ethereum": "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2",
        "arbitrum": "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1",
        "optimism": "0x4200000000000000000000000000000000000006",
        "base": "0x4200000000000000000000000000000000000006",
        "polygon": "0x7ceB23fD6bC0adD59E62ac25578270cFf1b9f619",  # Bridged WETH on Polygon
        "avalanche": "0x49D5c2BdFfac6CE2BFdB6640F4F80f226bc10bAB",  # WETH.e on Avalanche
        "bsc": "0x2170Ed0880ac9A755fd29B2688956BD959F933F8",  # Binance-Peg ETH
        "mantle": "0xdEAddEaDdeadDEadDEADDEAddEADDEAddead1111",  # Canonical Mantle Bridged WETH (deterministic bridge address, not a placeholder)
        "berachain": "0x2F6F07CDcf3588944Bf4C42aC74ff24bF56e7590",  # Bridged WETH on Berachain
        "monad": "0xEE8c0E9f1BFFb4Eb878d8f15f368A02a35481242",  # WETH on Monad
        "sonic": "0x50c42dEAcD8Fc9773493ED674b675bE577f2634b",  # Bridged WETH on Sonic
    },
    coingecko_id="weth",
    is_stablecoin=False,
)

# Wrapped MATIC
WMATIC = Token(
    symbol="WMATIC",
    name="Wrapped MATIC",
    decimals=18,
    addresses={
        "polygon": "0x0d500B1d8E8eF31E21C99d1Db9A6444d3ADf1270",
    },
    coingecko_id="wmatic",
    is_stablecoin=False,
)

# Wrapped AVAX
WAVAX = Token(
    symbol="WAVAX",
    name="Wrapped AVAX",
    decimals=18,
    addresses={
        "avalanche": "0xB31f66AA3C1e785363F0875A1B74E27b85FD66c7",
    },
    coingecko_id="wrapped-avax",
    is_stablecoin=False,
)

# Wrapped BNB
WBNB = Token(
    symbol="WBNB",
    name="Wrapped BNB",
    decimals=18,
    addresses={
        "bsc": "0xbb4CdB9CBd36B01bD1cBaEBF2De08d9173bc095c",
    },
    coingecko_id="wbnb",
    is_stablecoin=False,
)

# =============================================================================
# NATIVE STABLECOINS (issued directly on chain by Circle, Tether, etc.)
# =============================================================================

# USD Coin - Native (issued by Circle directly on each chain)
USDC = Token(
    symbol="USDC",
    name="USD Coin",
    decimals=6,
    addresses={
        "ethereum": "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
        "arbitrum": "0xaf88d065e77c8cC2239327C5EDb3A432268e5831",  # Native USDC on Arbitrum
        "optimism": "0x0b2C639c533813f4Aa9D7837CAf62653d097Ff85",  # Native USDC on Optimism
        "base": "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913",  # Native USDC on Base
        "polygon": "0x3c499c542cEF5E3811e1192ce70d8cC03d5c3359",  # Native USDC on Polygon
        "avalanche": "0xB97EF9Ef8734C71904D8002F8b6Bc66Dd9c48a6E",  # Native USDC on Avalanche
        "bsc": "0x8AC76a51cc950d9822D68b83fE1Ad97B32Cd580d",  # Binance-Peg USDC (18 decimals on BSC)
        "solana": "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",  # Native USDC on Solana
        "mantle": "0x09Bc4E0D864854c6aFB6eB9A9cdF58aC190D0dF9",  # Bridged USDC on Mantle
        "monad": "0x754704Bc059F8C67012fEd69BC8A327a5aafb603",  # Native USDC on Monad
        "sonic": "0x29219dd400f2Bf60E5a23d13Be72B486D4038894",  # Bridged USDC on Sonic
    },
    coingecko_id="usd-coin",
    is_stablecoin=True,
    chain_overrides={
        "bsc": ChainTokenConfig(
            address="0x8AC76a51cc950d9822D68b83fE1Ad97B32Cd580d",
            decimals=18,  # BSC uses 18 decimals for USDC
            is_native=False,
            bridge_type=BridgeType.BRIDGED,
        ),
    },
)

# Tether USD
USDT = Token(
    symbol="USDT",
    name="Tether USD",
    decimals=6,
    addresses={
        "ethereum": "0xdAC17F958D2ee523a2206206994597C13D831ec7",
        "arbitrum": "0xFd086bC7CD5C481DCC9C85ebE478A1C0b69FCbb9",
        "optimism": "0x94b008aA00579c1307B0EF2c499aD98a8ce58e58",
        "polygon": "0xc2132D05D31c914a87C6611C10748AEb04B58e8F",
        "avalanche": "0x9702230A8Ea53601f5cD2dc00fDBc13d4dF4A8c7",  # USDT on Avalanche
        "bsc": "0x55d398326f99059fF775485246999027B3197955",  # Binance-Peg USDT (18 decimals on BSC)
        "solana": "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB",  # USDT on Solana
        "mantle": "0x779Ded0c9e1022225f8E0630b35a9b54bE713736",  # USDT0 on Mantle (LayerZero bridged)
    },
    coingecko_id="tether",
    is_stablecoin=True,
    chain_overrides={
        "bsc": ChainTokenConfig(
            address="0x55d398326f99059fF775485246999027B3197955",
            decimals=18,  # BSC uses 18 decimals for USDT
            is_native=False,
            bridge_type=BridgeType.BRIDGED,
        ),
    },
)

# Dai Stablecoin
DAI = Token(
    symbol="DAI",
    name="Dai Stablecoin",
    decimals=18,
    addresses={
        "ethereum": "0x6B175474E89094C44Da98b954EedeAC495271d0F",
        "arbitrum": "0xDA10009cBd5D07dd0CeCc66161FC93D7c9000da1",
        "optimism": "0xDA10009cBd5D07dd0CeCc66161FC93D7c9000da1",
        "base": "0x50c5725949A6F0c72E6C4a641F24049A917DB0Cb",
        "polygon": "0x8f3Cf7ad23Cd3CaDbD9735AFf958023239c6A063",
        "avalanche": "0xd586E7F844cEa2F87f50152665BCbc2C279D8d70",  # DAI.e on Avalanche
        "bsc": "0x1AF3F329e8BE154074D8769D1FFa4eE058B1DBc3",  # Binance-Peg DAI
    },
    coingecko_id="dai",
    is_stablecoin=True,
)

# =============================================================================
# BRIDGED STABLECOINS (bridged from Ethereum via official bridges)
# =============================================================================

# Bridged USDC on Arbitrum (USDC.e) - older bridged version
USDC_E_ARBITRUM = Token(
    symbol="USDC.E",
    name="Bridged USDC (Arbitrum)",
    decimals=6,
    addresses={
        "arbitrum": "0xFF970A61A04b1cA14834A43f5dE4533eBDDB5CC8",
    },
    coingecko_id="arbitrum-bridged-usdc-arbitrum",
    is_stablecoin=True,
    chain_overrides={
        "arbitrum": ChainTokenConfig(
            address="0xFF970A61A04b1cA14834A43f5dE4533eBDDB5CC8",
            decimals=6,
            is_native=False,
            bridge_type=BridgeType.BRIDGED,
        ),
    },
)

# Bridged USDC on Optimism (USDC.e) - older bridged version
USDC_E_OPTIMISM = Token(
    symbol="USDC.E",
    name="Bridged USDC (Optimism)",
    decimals=6,
    addresses={
        "optimism": "0x7F5c764cBc14f9669B88837ca1490cCa17c31607",
    },
    coingecko_id="standard-bridged-usdc-e-optimism",
    is_stablecoin=True,
    chain_overrides={
        "optimism": ChainTokenConfig(
            address="0x7F5c764cBc14f9669B88837ca1490cCa17c31607",
            decimals=6,
            is_native=False,
            bridge_type=BridgeType.BRIDGED,
        ),
    },
)

# Bridged USDC on Polygon (USDC.e) - older bridged version via PoS bridge
USDC_E_POLYGON = Token(
    symbol="USDC.E",
    name="Bridged USDC (Polygon)",
    decimals=6,
    addresses={
        "polygon": "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174",
    },
    coingecko_id="bridged-usdc-polygon-pos-bridge",
    is_stablecoin=True,
    chain_overrides={
        "polygon": ChainTokenConfig(
            address="0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174",
            decimals=6,
            is_native=False,
            bridge_type=BridgeType.BRIDGED,
        ),
    },
)

# Bridged USDC on Avalanche (USDC.e)
USDC_E_AVALANCHE = Token(
    symbol="USDC.E",
    name="Bridged USDC (Avalanche)",
    decimals=6,
    addresses={
        "avalanche": "0xA7D7079b0FEaD91F3e65f86E8915Cb59c1a4C664",
    },
    coingecko_id="usd-coin-avalanche-bridged-usdc-e",
    is_stablecoin=True,
    chain_overrides={
        "avalanche": ChainTokenConfig(
            address="0xA7D7079b0FEaD91F3e65f86E8915Cb59c1a4C664",
            decimals=6,
            is_native=False,
            bridge_type=BridgeType.BRIDGED,
        ),
    },
)

# Bridged USDC on Base (USDbC) - Coinbase bridged
USDBC = Token(
    symbol="USDBC",
    name="USD Base Coin",
    decimals=6,
    addresses={
        "base": "0xd9aAEc86B65D86f6A7B5B1b0c42FFA531710b6CA",
    },
    coingecko_id="bridged-usd-coin-base",
    is_stablecoin=True,
    chain_overrides={
        "base": ChainTokenConfig(
            address="0xd9aAEc86B65D86f6A7B5B1b0c42FFA531710b6CA",
            decimals=6,
            is_native=False,
            bridge_type=BridgeType.BRIDGED,
        ),
    },
)

# Bridged USDT on Avalanche (USDT.e)
USDT_E_AVALANCHE = Token(
    symbol="USDT.E",
    name="Bridged USDT (Avalanche)",
    decimals=6,
    addresses={
        "avalanche": "0xc7198437980c041c805A1EDcbA50c1Ce5db95118",
    },
    coingecko_id="tether",
    is_stablecoin=True,
    chain_overrides={
        "avalanche": ChainTokenConfig(
            address="0xc7198437980c041c805A1EDcbA50c1Ce5db95118",
            decimals=6,
            is_native=False,
            bridge_type=BridgeType.BRIDGED,
        ),
    },
)

# Bridged USDT on Arbitrum (USDT is actually native but for completeness, aliasing)
# Note: Arbitrum USDT is native, not bridged, so we don't add a separate USDT.e

# Bridged WETH on Avalanche (WETH.e)
WETH_E_AVALANCHE = Token(
    symbol="WETH.E",
    name="Bridged WETH (Avalanche)",
    decimals=18,
    addresses={
        "avalanche": "0x49D5c2BdFfac6CE2BFdB6640F4F80f226bc10bAB",
    },
    coingecko_id="weth",
    is_stablecoin=False,
    chain_overrides={
        "avalanche": ChainTokenConfig(
            address="0x49D5c2BdFfac6CE2BFdB6640F4F80f226bc10bAB",
            decimals=18,
            is_native=False,
            bridge_type=BridgeType.BRIDGED,
        ),
    },
)

# =============================================================================
# WRAPPED BITCOIN
# =============================================================================

# Wrapped Bitcoin (8 decimals!)
WBTC = Token(
    symbol="WBTC",
    name="Wrapped Bitcoin",
    decimals=8,
    addresses={
        "ethereum": "0x2260FAC5E5542a773Aa44fBCfeDf7C193bc2C599",
        "arbitrum": "0x2f2a2543B76A4166549F7aaB2e75Bef0aefC5B0f",
        "optimism": "0x68f180fcCe6836688e9084f035309E29Bf0A2095",
        "polygon": "0x1BFD67037B42Cf73acF2047067bd4F2C47D9BfD6",
        "avalanche": "0x50b7545627a5162F82A992c33b87aDc75187B218",  # WBTC.e on Avalanche
        "bsc": "0x7130d2A12B9BCbFAe4f2634d864A1Ee1Ce3Ead9c",  # Binance-Peg BTCB
        "berachain": "0x0555E30da8f98308EdB960aa94C0Db47230d2B9c",  # WBTC on Berachain
        "monad": "0x0555E30da8f98308EdB960aa94C0Db47230d2B9c",  # WBTC on Monad
    },
    coingecko_id="wrapped-bitcoin",
    is_stablecoin=False,
)

# BTC.b (Avalanche bridged BTC via Bitcoin Bridge, 8 decimals)
BTC_B = Token(
    symbol="BTC.B",
    name="Bitcoin (BTC.b)",
    decimals=8,
    addresses={
        "avalanche": "0x152b9D0fDC40c096DE20232db1E35ae6A57fa6C0",
    },
    coingecko_id="bitcoin-avalanche-bridged-btc-b",
    is_stablecoin=False,
)

# =============================================================================
# L2 / CHAIN TOKENS
# =============================================================================

# Arbitrum Token
ARB = Token(
    symbol="ARB",
    name="Arbitrum",
    decimals=18,
    addresses={
        "ethereum": "0xB50721BCf8d664c30412Cfbc6cf7a15145234ad1",
        "arbitrum": "0x912CE59144191C1204E64559FE8253a0e49E6548",
    },
    coingecko_id="arbitrum",
    is_stablecoin=False,
)

# Optimism Token
OP = Token(
    symbol="OP",
    name="Optimism",
    decimals=18,
    addresses={
        "ethereum": "0x4200000000000000000000000000000000000042",
        "optimism": "0x4200000000000000000000000000000000000042",
    },
    coingecko_id="optimism",
    is_stablecoin=False,
)

# =============================================================================
# DEFI PROTOCOL TOKENS
# =============================================================================

# Chainlink
LINK = Token(
    symbol="LINK",
    name="Chainlink",
    decimals=18,
    addresses={
        "ethereum": "0x514910771AF9Ca656af840dff83E8264EcF986CA",
        "arbitrum": "0xf97f4df75117a78c1A5a0DBb814Af92458539FB4",
        "optimism": "0x350a791Bfc2C21F9Ed5d10980Dad2e2638ffa7f6",
        "base": "0x88Fb150BDc53A65fe94Dea0c9BA0a6dAf8C6e196",
        "polygon": "0x53E0bca35eC356BD5ddDFebbD1Fc0fD03FaBad39",
        "avalanche": "0x5947BB275c521040051D82396192181b413227A3",
        "bsc": "0xF8A0BF9cF54Bb92F17374d9e9A321E6a111a51bD",
    },
    coingecko_id="chainlink",
    is_stablecoin=False,
)

# Uniswap
UNI = Token(
    symbol="UNI",
    name="Uniswap",
    decimals=18,
    addresses={
        "ethereum": "0x1f9840a85d5aF5bf1D1762F925BDADdC4201F984",
        "arbitrum": "0xFa7F8980b0f1E64A2062791cc3b0871572f1F7f0",
        "optimism": "0x6fd9d7AD17242c41f7131d257212c54A0e816691",
        "base": "0xc3De830EA07524a0761646a6a4e4be0e114a3C83",
        "polygon": "0xb33EaAd8d922B1083446DC23f610c2567fB5180f",
        "avalanche": "0x8eBAf22B6F053dFFeaf46f4Dd9eFA95D89ba8580",
        "bsc": "0xBf5140A22578168FD562DCcF235E5D43A02ce9B1",
    },
    coingecko_id="uniswap",
    is_stablecoin=False,
)

# Aave
AAVE = Token(
    symbol="AAVE",
    name="Aave",
    decimals=18,
    addresses={
        "ethereum": "0x7Fc66500c84A76Ad7e9c93437bFc5Ac33E2DDaE9",
        "arbitrum": "0xba5DdD1f9d7F570dc94a51479a000E3BCE967196",
        "optimism": "0x76FB31fb4af56892A25e32cFC43De717950c9278",
        "base": "0x18c11FD286C5EC11c3b683Caa813B77f5163A122",
        "polygon": "0xD6DF932A45C0f255f85145f286eA0b292B21C90B",
        "avalanche": "0x63a72806098Bd3D9520cC43356dD78afe5D386D9",
        "bsc": "0xfb6115445Bff7b52FeB98650C87f44907E58f802",
    },
    coingecko_id="aave",
    is_stablecoin=False,
)

# Curve DAO Token
CRV = Token(
    symbol="CRV",
    name="Curve DAO Token",
    decimals=18,
    addresses={
        "ethereum": "0xD533a949740bb3306d119CC777fa900bA034cd52",
        "arbitrum": "0x11cDb42B0EB46D95f990BeDD4695A6e3fA034978",
        "optimism": "0x0994206dfE8De6Ec6920FF4D779B0d950605Fb53",
        "base": "0x8Ee73c484A26e0A5df2Ee2a4960B789967dd0415",
        "polygon": "0x172370d5Cd63279eFa6d502DAB29171933a610AF",
        "avalanche": "0x249848BeCA43aC405b8102Ec90Dd5F22CA513c06",
    },
    coingecko_id="curve-dao-token",
    is_stablecoin=False,
)

# GMX
GMX = Token(
    symbol="GMX",
    name="GMX",
    decimals=18,
    addresses={
        "arbitrum": "0xfc5A1A6EB076a2C7aD06eD22C90d7E710E35ad0a",
        "avalanche": "0x62edc0692BD897D2295872a9FFCac5425011c661",
    },
    coingecko_id="gmx",
    is_stablecoin=False,
)

# Pendle
PENDLE = Token(
    symbol="PENDLE",
    name="Pendle",
    decimals=18,
    addresses={
        "ethereum": "0x808507121B80c02388fAd14726482e061B8da827",
        "arbitrum": "0x0c880f6761F1af8d9Aa9C466984b80DAb9a8c9e8",
        "optimism": "0xBC7B1Ff1c6989f006a1185318eD4E7b5796e66E1",
        "bsc": "0xb3Ed0A426155B79B898849803E3B36552f7ED507",
    },
    coingecko_id="pendle",
    is_stablecoin=False,
)

# =============================================================================
# CURVE LP TOKENS
# =============================================================================
# Registering well-known Curve LP tokens here prevents a ~30s gateway timeout
# during LP_CLOSE compilation (token resolver falls back to slow on-chain lookup
# for unregistered LP token addresses). See VIB-1509.

# Curve 3pool LP (3Crv) -- DAI/USDC/USDT stableswap on Ethereum
CRV_3CRV = Token(
    symbol="3Crv",
    name="Curve 3pool LP",
    decimals=18,
    addresses={
        "ethereum": "0x6c3F90f043a72FA612cbac8115EE7e52BDe6E490",
    },
    coingecko_id="lp-3pool-curve",
    is_stablecoin=False,
)

# Curve FRAX/USDC LP -- frax_usdc stableswap on Ethereum
CRV_FRAX_USDC_LP = Token(
    symbol="crvFRAX",
    name="Curve FRAX/USDC LP",
    decimals=18,
    addresses={
        "ethereum": "0x3175Df0976dFA876431C2E9eE6Bc45b65d3473CC",
    },
    coingecko_id=None,
    is_stablecoin=False,
)

# Curve tricrypto2 LP -- USDT/WBTC/WETH on Ethereum
CRV_TRICRYPTO2_LP = Token(
    symbol="crv3crypto",
    name="Curve Tricrypto2 LP",
    decimals=18,
    addresses={
        "ethereum": "0xc4AD29ba4B3c580e6D59105FFf484999997675Ff",
    },
    coingecko_id=None,
    is_stablecoin=False,
)

# Curve 2pool LP (2CRV) -- USDC.e/USDT stableswap on Arbitrum
CRV_2CRV = Token(
    symbol="2CRV",
    name="Curve 2pool LP (Arbitrum)",
    decimals=18,
    addresses={
        "arbitrum": "0x7f90122BF0700F9E7e1F688fe926940E8839F353",
    },
    coingecko_id=None,
    is_stablecoin=False,
)

# Curve tricrypto LP -- USDT/WBTC/WETH on Arbitrum
CRV_TRICRYPTO_ARB_LP = Token(
    symbol="crvUSDBTCETH",
    name="Curve Tricrypto LP (Arbitrum)",
    decimals=18,
    addresses={
        "arbitrum": "0x8e0B8c8BB9db49a46697F3a5Bb8A308e744821D2",
    },
    coingecko_id=None,
    is_stablecoin=False,
)

# Curve WETH/cbETH LP -- Twocrypto on Base
CRV_WETH_CBETH_LP = Token(
    symbol="crvWETHcbETH",
    name="Curve WETH/cbETH LP (Base)",
    decimals=18,
    addresses={
        "base": "0x98244d93D42b42aB3E3A4D12A5dc0B3e7f8F32f9",
    },
    coingecko_id=None,
    is_stablecoin=False,
)

# =============================================================================
# ETHENA PROTOCOL TOKENS
# =============================================================================

# Ethena USDe (yield-bearing synthetic dollar)
USDe = Token(
    symbol="USDe",
    name="Ethena USDe",
    decimals=18,
    addresses={
        "ethereum": "0x4c9EDD5852cd905f086C759E8383e09bff1E68B3",
        "mantle": "0x5d3a1Ff2b6BAb83b63cd9AD0787074081a52ef34",  # USDe on Mantle
    },
    coingecko_id="ethena-usde",
    is_stablecoin=True,
)

# Staked USDe (yield-bearing sUSDe)
sUSDe = Token(
    symbol="sUSDe",
    name="Ethena Staked USDe",
    decimals=18,
    addresses={
        "ethereum": "0x9D39A5DE30e57443BfF2A8307A4256c8797A3497",
    },
    coingecko_id="ethena-staked-usde",
    is_stablecoin=False,
)

# Aave GHO stablecoin
GHO = Token(
    symbol="GHO",
    name="GHO Stablecoin",
    decimals=18,
    addresses={
        "ethereum": "0x40D16FC0246aD3160Ccc09B8D0D3A2cD28aE6C2f",
        "arbitrum": "0x7dfF72693f6A4149b17e7C6314655f6A9F7c8B33",
        "mantle": "0xfc421aD3C883Bf9E7C4f42dE845C4e4405799e73",  # GHO on Mantle
    },
    coingecko_id="gho",
    is_stablecoin=True,
)

# =============================================================================
# LIQUID STAKING TOKENS (LST/LRT)
# =============================================================================

# Lido Wrapped Staked ETH
wstETH = Token(
    symbol="wstETH",
    name="Wrapped Staked Ether",
    decimals=18,
    addresses={
        "ethereum": "0x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0",
        "arbitrum": "0x5979D7b546E38E414F7E9822514be443A4800529",
        "optimism": "0x1f32B1C2345538c0C6F582FB0220C6C2C0C9c6c6",
        "base": "0xc1CBa3fCea344f92D9239c08C0568f6F2F0ee452",
        "polygon": "0x03b54A6e9a984069379fae1a4fC4dBAE93B3bCCD",
    },
    coingecko_id="wrapped-steth",
    is_stablecoin=False,
)

# EtherFi Wrapped eETH (liquid restaking token)
WEETH = Token(
    symbol="WEETH",
    name="Wrapped eETH",
    decimals=18,
    addresses={
        "ethereum": "0xCd5fE23C85820F7B72D0926FC9b05b43E359b7ee",
        "arbitrum": "0x35751007a407ca6FEFfE80b3cB397736D2cf4dbe",
    },
    coingecko_id="wrapped-eeth",
    is_stablecoin=False,
)

# BENQI Staked AVAX (liquid staking token on Avalanche)
SAVAX = Token(
    symbol="SAVAX",
    name="Staked AVAX",
    decimals=18,
    addresses={
        "avalanche": "0x2b2C81e08f1Af8835a78Bb2A90AE924ACE0eA4bE",
    },
    coingecko_id="benqi-liquid-staked-avax",
    is_stablecoin=False,
)

# =============================================================================
# BASE CHAIN TOKENS
# =============================================================================

# Coinbase Wrapped Staked ETH (cbETH) — liquid staking token native to Base
CBETH = Token(
    symbol="cbETH",
    name="Coinbase Wrapped Staked ETH",
    decimals=18,
    addresses={
        "base": "0x2Ae3F1Ec7F1F5012CFEab0185bfc7aa3cf0DEc22",
        "ethereum": "0xBe9895146f7AF43049ca1c1AE358B0541Ea49704",
    },
    coingecko_id="coinbase-wrapped-staked-eth",
    is_stablecoin=False,
)

# Almanak (governance token on Base)
ALMANAK = Token(
    symbol="ALMANAK",
    name="Almanak",
    decimals=18,
    addresses={
        "base": "0xDeFA1D21c5F1cbeac00eeB54B44C7D86467cc3a3",
    },
    coingecko_id="almanak",
    is_stablecoin=False,
)

# =============================================================================
# PLASMA CHAIN TOKENS
# =============================================================================

# XPL (native gas token on Plasma) - from compiler.py
XPL = Token(
    symbol="XPL",
    name="Plasma",
    decimals=18,
    addresses={
        "plasma": NATIVE_SENTINEL,
    },
    coingecko_id=None,  # Not listed yet
    is_stablecoin=False,
)

# Wrapped XPL
WXPL = Token(
    symbol="WXPL",
    name="Wrapped XPL",
    decimals=18,
    addresses={
        "plasma": "0x6100E367285b01F48D07953803A2d8dCA5D19873",
    },
    coingecko_id=None,
    is_stablecoin=False,
)

# USDT0 on Plasma (from compiler.py)
USDT0 = Token(
    symbol="USDT0",
    name="USDT Zero",
    decimals=6,  # USDT-like stablecoin
    addresses={
        "plasma": "0xB8CE59FC3717ada4C02eaDF9682A9e934F625ebb",
    },
    coingecko_id=None,
    is_stablecoin=True,
)

# Fluid USDT0 on Plasma (from compiler.py)
FUSDT0 = Token(
    symbol="FUSDT0",
    name="Fluid USDT Zero",
    decimals=6,
    addresses={
        "plasma": "0x1DD4b13fcAE900C60a350589BE8052959D2Ed27B",
    },
    coingecko_id=None,
    is_stablecoin=True,
)

# PENDLE on Plasma (from compiler.py)
PENDLE_PLASMA = Token(
    symbol="PENDLE",
    name="Pendle (Plasma)",
    decimals=18,
    addresses={
        "plasma": "0x17Bac5F906c9A0282aC06a59958D85796c831f24",
    },
    coingecko_id="pendle",
    is_stablecoin=False,
)

# =============================================================================
# SOLANA CHAIN TOKENS
# =============================================================================

# SOL (native gas token on Solana, 9 decimals)
SOL = Token(
    symbol="SOL",
    name="Solana",
    decimals=9,
    addresses={
        "solana": NATIVE_SENTINEL,
    },
    coingecko_id="solana",
    is_stablecoin=False,
)

# Wrapped SOL (SPL token representation of native SOL)
WSOL = Token(
    symbol="WSOL",
    name="Wrapped SOL",
    decimals=9,
    addresses={
        "solana": "So11111111111111111111111111111111111111112",
    },
    coingecko_id="wrapped-solana",
    is_stablecoin=False,
)

# Marinade Staked SOL (liquid staking derivative)
MSOL = Token(
    symbol="mSOL",
    name="Marinade Staked SOL",
    decimals=9,
    addresses={
        "solana": "mSoLzYCxHdYgdzU16g5QSh3i5K3z3KZK7ytfqcJm7So",
    },
    coingecko_id="msol",
    is_stablecoin=False,
)

# Jito Staked SOL (liquid staking derivative with MEV rewards)
JITOSOL = Token(
    symbol="JitoSOL",
    name="Jito Staked SOL",
    decimals=9,
    addresses={
        "solana": "J1toso1uCk3RLmjorhTtrVwY9HJ7X8V9yYac6Y7kGCPn",
    },
    coingecko_id="jito-staked-sol",
    is_stablecoin=False,
)

# Jupiter governance token
JUP = Token(
    symbol="JUP",
    name="Jupiter",
    decimals=6,
    addresses={
        "solana": "JUPyiwrYJFskUPiHa7hkeR8VUtAeFoSYbKedZNsDvCN",
    },
    coingecko_id="jupiter-exchange-solana",
    is_stablecoin=False,
)

# USDC on Solana (SPL token, 6 decimals)
USDC_SOL = Token(
    symbol="USDC",
    name="USD Coin (Solana)",
    decimals=6,
    addresses={
        "solana": "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
    },
    coingecko_id="usd-coin",
    is_stablecoin=True,
)

# USDT on Solana (SPL token, 6 decimals)
USDT_SOL = Token(
    symbol="USDT",
    name="Tether USD (Solana)",
    decimals=6,
    addresses={
        "solana": "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB",
    },
    coingecko_id="tether",
    is_stablecoin=True,
)

# =============================================================================
# MANTLE CHAIN TOKENS
# =============================================================================

# MNT (native gas token on Mantle)
MNT = Token(
    symbol="MNT",
    name="Mantle",
    decimals=18,
    addresses={
        "mantle": NATIVE_SENTINEL,
    },
    coingecko_id="mantle",
    is_stablecoin=False,
)

# Wrapped MNT
WMNT = Token(
    symbol="WMNT",
    name="Wrapped MNT",
    decimals=18,
    addresses={
        "mantle": "0x78c1b0C915c4FAA5FffA6CAbf0219DA63d7f4cb8",
    },
    coingecko_id="wrapped-mantle",
    is_stablecoin=False,
)

# =============================================================================
# MONAD CHAIN TOKENS
# =============================================================================

# MON (native gas token on Monad)
MON = Token(
    symbol="MON",
    name="Monad",
    decimals=18,
    addresses={
        "monad": NATIVE_SENTINEL,
    },
    coingecko_id="monad",
    is_stablecoin=False,
)

# Wrapped MON
WMON = Token(
    symbol="WMON",
    name="Wrapped MON",
    decimals=18,
    addresses={
        "monad": "0x3bd359C1119dA7Da1D913D1C4D2B7c461115433A",
    },
    coingecko_id=None,
    is_stablecoin=False,
)

# USDT0 on Monad (LayerZero bridged USDT)
USDT0_MONAD = Token(
    symbol="USDT0",
    name="USDT Zero (Monad)",
    decimals=6,
    addresses={
        "monad": "0xe7cd86e13AC4309349F30B3435a9d337750fC82D",
    },
    coingecko_id=None,
    is_stablecoin=True,
)

# =============================================================================
# BERACHAIN TOKENS
# =============================================================================

# BERA (native gas token on Berachain)
BERA = Token(
    symbol="BERA",
    name="Berachain",
    decimals=18,
    addresses={
        "berachain": NATIVE_SENTINEL,
    },
    coingecko_id="berachain-bera",
    is_stablecoin=False,
)

# Wrapped BERA
WBERA = Token(
    symbol="WBERA",
    name="Wrapped BERA",
    decimals=18,
    addresses={
        "berachain": "0x6969696969696969696969696969696969696969",
    },
    coingecko_id="wrapped-bera",
    is_stablecoin=False,
)

# HONEY (Berachain native stablecoin - 18 decimals, not 6)
HONEY = Token(
    symbol="HONEY",
    name="Honey",
    decimals=18,
    addresses={
        "berachain": "0xFCBD14DC51f0A4d49d5E53C2E0950e0bC26d0Dce",
    },
    coingecko_id="honey-berachain",
    is_stablecoin=True,
)

# Bridged USDC on Berachain (USDC.e via Stargate)
USDC_E_BERACHAIN = Token(
    symbol="USDC.E",
    name="Bridged USDC (Berachain)",
    decimals=6,
    addresses={
        "berachain": "0x549943e04f40284185054145c6E4e9568C1D3241",
    },
    coingecko_id="usd-coin",
    is_stablecoin=True,
    chain_overrides={
        "berachain": ChainTokenConfig(
            address="0x549943e04f40284185054145c6E4e9568C1D3241",
            decimals=6,
            is_native=False,
            bridge_type=BridgeType.BRIDGED,
        ),
    },
)

# USDT0 on Berachain (LayerZero bridged USDT)
USDT0_BERACHAIN = Token(
    symbol="USDT0",
    name="USDT Zero (Berachain)",
    decimals=6,
    addresses={
        "berachain": "0x779Ded0c9e1022225f8E0630b35a9b54bE713736",
    },
    coingecko_id=None,
    is_stablecoin=True,
)

# =============================================================================
# PENDLE PT TOKENS (used as collateral on Morpho Blue)
# WARNING: PT tokens are maturity-bound. These addresses expire and new ones
# are deployed. Check Pendle app for current PT addresses before using.
# TODO: Replace with dynamic PT resolution via Pendle API in a follow-up PR.
# =============================================================================

PT_sUSDe = Token(
    symbol="PT-sUSDe",
    name="Pendle PT sUSDe",  # Maturity: 29-MAY-2025
    decimals=18,
    addresses={
        "ethereum": "0xaE4750d0813B5E37A51f7629beedd72AF1f9cA35",
    },
    coingecko_id=None,
    is_stablecoin=False,
)

PT_eUSDe = Token(
    symbol="PT-eUSDe",
    name="Pendle PT eUSDe",
    decimals=18,
    addresses={
        "ethereum": "0x308c36baF407f543DaC3A6340b7b6B31079e8e0D",
    },
    coingecko_id=None,
    is_stablecoin=False,
)

PT_USDe = Token(
    symbol="PT-USDe",
    name="Pendle PT USDe",
    decimals=18,
    addresses={
        "ethereum": "0x8A47b431A7D947c6a3ED6E42d501803615a97EAa",
    },
    coingecko_id=None,
    is_stablecoin=False,
)

PT_USDai = Token(
    symbol="PT-USDai",
    name="Pendle PT USDai",
    decimals=18,
    addresses={
        "arbitrum": "0x3B0C5Ef8D4c8aE6Db1A3E3b9c876A53f3fe8C0b1",
    },
    coingecko_id=None,
    is_stablecoin=False,
)

PT_wstETH = Token(
    symbol="PT-wstETH",
    name="Pendle PT wstETH",  # Maturity: 25-JUN-2026
    decimals=18,
    addresses={
        "arbitrum": "0x71fBF40651E9D4278a74586AfC99F307f369Ce9A",
    },
    coingecko_id=None,
    is_stablecoin=False,
)


# =============================================================================
# DEFAULT TOKENS LIST
# =============================================================================

# List of all default tokens (50+ tokens across 8 chains)
# Order: Native -> Wrapped Native -> Stablecoins -> Bridged Stablecoins -> DeFi tokens
DEFAULT_TOKENS: list[Token] = [
    # Native tokens
    ETH,
    MATIC,
    AVAX,
    BNB,
    # Wrapped native tokens
    WETH,
    WMATIC,
    WAVAX,
    WBNB,
    # Native stablecoins
    USDC,
    USDT,
    DAI,
    # Bridged stablecoins
    USDC_E_ARBITRUM,
    USDC_E_OPTIMISM,
    USDC_E_POLYGON,
    USDC_E_AVALANCHE,
    USDBC,
    USDT_E_AVALANCHE,
    WETH_E_AVALANCHE,
    # Wrapped Bitcoin
    WBTC,
    BTC_B,
    # L2/Chain tokens
    ARB,
    OP,
    # DeFi protocol tokens
    LINK,
    UNI,
    AAVE,
    CRV,
    GMX,
    PENDLE,
    # Curve LP tokens (avoids 30s timeout in LP_CLOSE compilation -- VIB-1509)
    CRV_3CRV,
    CRV_FRAX_USDC_LP,
    CRV_TRICRYPTO2_LP,
    CRV_2CRV,
    CRV_TRICRYPTO_ARB_LP,
    CRV_WETH_CBETH_LP,
    # Ethena tokens
    USDe,
    sUSDe,
    GHO,
    # LST/LRT tokens
    wstETH,
    WEETH,
    SAVAX,
    # Base chain tokens
    CBETH,
    ALMANAK,
    # Plasma chain tokens
    XPL,
    WXPL,
    USDT0,
    FUSDT0,
    PENDLE_PLASMA,
    # Solana chain tokens
    SOL,
    WSOL,
    MSOL,
    JITOSOL,
    JUP,
    # NOTE: USDC_SOL and USDT_SOL are intentionally excluded from DEFAULT_TOKENS.
    # The main USDC and USDT Token definitions already include "solana" addresses,
    # so registering USDC_SOL/USDT_SOL would create duplicates. The variables are
    # kept above for backward-compatibility imports.
    # Mantle chain tokens
    MNT,
    WMNT,
    # Berachain tokens
    BERA,
    WBERA,
    HONEY,
    USDC_E_BERACHAIN,
    USDT0_BERACHAIN,
    # Monad chain tokens
    MON,
    WMON,
    USDT0_MONAD,
    # Pendle PT tokens (used as collateral)
    PT_sUSDe,
    PT_eUSDe,
    PT_USDe,
    PT_USDai,
    PT_wstETH,
]


def get_default_registry() -> TokenRegistry:
    """Create a TokenRegistry pre-populated with common DeFi tokens.

    Returns:
        TokenRegistry with ETH, WETH, WBNB, USDC, USDT, DAI, WBTC, ARB, OP,
        MATIC, LINK, UNI, AAVE, CRV, GMX, PENDLE, and more pre-registered.

    Example:
        registry = get_default_registry()
        usdc = registry.get("USDC")
        assert usdc is not None
        assert usdc.decimals == 6
    """
    registry = TokenRegistry()
    for token in DEFAULT_TOKENS:
        registry.register(token)
    return registry


def get_coingecko_id(symbol: str) -> str | None:
    """Get CoinGecko ID for a token symbol.

    Args:
        symbol: Token symbol (e.g., "ETH", "WETH", "USDC")

    Returns:
        CoinGecko ID if found, None otherwise

    Example:
        cg_id = get_coingecko_id("ETH")  # Returns "ethereum"
        cg_id = get_coingecko_id("USDC")  # Returns "usd-coin"
    """
    symbol_upper = symbol.upper()
    for token in DEFAULT_TOKENS:
        if token.symbol.upper() == symbol_upper:
            return token.coingecko_id
    return None


def get_coingecko_ids() -> dict[str, str]:
    """Get mapping of all token symbols to CoinGecko IDs.

    Returns:
        Dictionary mapping token symbols to CoinGecko IDs

    Example:
        ids = get_coingecko_ids()
        print(ids["ETH"])  # "ethereum"
    """
    return {token.symbol.upper(): token.coingecko_id for token in DEFAULT_TOKENS if token.coingecko_id}


__all__ = [
    # Constants
    "NATIVE_SENTINEL",
    "WRAPPED_NATIVE",
    "STABLECOINS",
    "SYMBOL_ALIASES",
    # Native tokens
    "ETH",
    "MATIC",
    "AVAX",
    "BNB",
    # Wrapped native tokens
    "WETH",
    "WMATIC",
    "WAVAX",
    "WBNB",
    # Native stablecoins
    "USDC",
    "USDT",
    "DAI",
    # Bridged stablecoins
    "USDC_E_ARBITRUM",
    "USDC_E_OPTIMISM",
    "USDC_E_POLYGON",
    "USDC_E_AVALANCHE",
    "USDBC",
    "USDT_E_AVALANCHE",
    "WETH_E_AVALANCHE",
    # Wrapped Bitcoin
    "WBTC",
    "BTC_B",
    # L2/Chain tokens
    "ARB",
    "OP",
    # DeFi protocol tokens
    "LINK",
    "UNI",
    "AAVE",
    "CRV",
    "GMX",
    "PENDLE",
    # Curve LP tokens
    "CRV_3CRV",
    "CRV_FRAX_USDC_LP",
    "CRV_TRICRYPTO2_LP",
    "CRV_2CRV",
    "CRV_TRICRYPTO_ARB_LP",
    "CRV_WETH_CBETH_LP",
    # Ethena tokens
    "USDe",
    "sUSDe",
    "GHO",
    # LST/LRT tokens
    "wstETH",
    "WEETH",
    "SAVAX",
    # Base chain tokens
    "CBETH",
    # Plasma chain tokens
    "XPL",
    "WXPL",
    "USDT0",
    "FUSDT0",
    "PENDLE_PLASMA",
    # Solana chain tokens
    "SOL",
    "WSOL",
    "MSOL",
    "JITOSOL",
    "JUP",
    # Mantle chain tokens
    "MNT",
    "WMNT",
    # Berachain tokens
    "BERA",
    "WBERA",
    "HONEY",
    "USDC_E_BERACHAIN",
    "USDT0_BERACHAIN",
    # Monad chain tokens
    "MON",
    "WMON",
    "USDT0_MONAD",
    # Pendle PT tokens
    "PT_sUSDe",
    "PT_eUSDe",
    "PT_USDe",
    "PT_USDai",
    # Functions and lists
    "DEFAULT_TOKENS",
    "get_default_registry",
    "get_coingecko_id",
    "get_coingecko_ids",
]
