"""Shared Chainlink oracle constants used by both gateway pricing and backtesting.

This module is the single source of truth for Chainlink price feed addresses,
function selectors, and token-to-pair mappings. Both the gateway's OnChainPriceSource
and the backtesting ChainlinkDataProvider import from here.
"""

from decimal import Decimal

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

# Ethereum Mainnet price feeds (Chain ID: 1)
# Reference: https://docs.chain.link/data-feeds/price-feeds/addresses?network=ethereum
ETHEREUM_PRICE_FEEDS: dict[str, str] = {
    "ETH/USD": "0x5f4eC3Df9cbd43714FE2740f5E3616155c5b8419",
    "BTC/USD": "0xF4030086522a5bEEa4988F8cA5B36dbC97BeE88c",
    "LINK/USD": "0x2c1d072e956AFFC0D435Cb7AC38EF18d24d9127c",
    "USDC/USD": "0x8fFfFfd4AfB6115b954Bd326cbe7B4BA576818f6",
    "USDT/USD": "0x3E7d1eAB13ad0104d2750B8863b489D65364e32D",
    "DAI/USD": "0xAed0c38402a5d19df6E4c03F4E2DceD6e29c1ee9",
    "AAVE/USD": "0x547a514d5e3769680Ce22B2361c10Ea13619e8a9",
    "UNI/USD": "0x553303d460EE0afB37EdFf9bE42922D8FF63220e",
    "CRV/USD": "0xcD627aa160A6fA45Eb793D19286F3879d5cdCe0a",
    "COMP/USD": "0xdBD020CAef83eFd542f4de03864E8c5D2d9bc6CA",
    "MKR/USD": "0xEC1D1b3b0443256Cc3860E24a46f108E699cF2b4",
    "SNX/USD": "0xDC3EA94CD0AC27d9A86C180091e7f78C683d3699",
    "MATIC/USD": "0x7bAC85A8a13A4BcD8abb3eB7d6b4d632c5a57676",
    "ARB/USD": "0x31697852a68433DBcC2FF612A4c1C919a0254678",
    "LDO/USD": "0x4e844125952d32acdF339be976C98FE6D1F5F8bE",
    "WSTETH/USD": "0x164b276057258D81941072Eb5f9D7F71C3Dd94b8",
    "CBETH/USD": "0xF017fcB346A1885194689bA23Eff2fE6fA5C483b",
    "RETH/USD": "0x536218f9E9Eb48863970252233c8F271f554C2d0",
    "SOL/USD": "0x4ffC43a60e009B551865A93d232E33Fce9f01507",
}

# Arbitrum One price feeds (Chain ID: 42161)
# Reference: https://docs.chain.link/data-feeds/price-feeds/addresses?network=arbitrum
ARBITRUM_PRICE_FEEDS: dict[str, str] = {
    "ETH/USD": "0x639Fe6ab55C921f74e7fac1ee960C0B6293ba612",
    "BTC/USD": "0x6CE185860A4963106506C203335A2910F5E5E8CC",
    "LINK/USD": "0x86E53CF1B870786351Da77A57575e79CB55812CB",
    "USDC/USD": "0x50834F3163758fcC1Df9973b6e91f0F0F0434aD3",
    "USDT/USD": "0x3f3f5dF88dC9F13eac63DF89EC16ef6e7E25DdE7",
    "DAI/USD": "0xc5C8E77B397E531B8EC06BFb0048328B30E9eCfB",
    "ARB/USD": "0xb2A824043730FE05F3DA2efaFa1CBbe83fa548D6",
    "GMX/USD": "0xDB98056FecFff59D032aB628337A4887110df3dB",
    "UNI/USD": "0x9C917083fDb403ab5ADbEC26Ee294f6EcAda2720",
    "AAVE/USD": "0xaD1d5344AaDE45F43E596773Bcc4c423EAbdD034",
    # Note: Arbitrum has no direct WSTETH/USD Chainlink feed.
    # Use derived price (WSTETH/ETH * ETH/USD) via ETH_DENOMINATED_FEEDS below.
    "PENDLE/USD": "0x66853E19D73C0F9301fE099c324A1e9726953C89",
    "RDNT/USD": "0x20d0Fcab0ECFD078B036b6CAf1FaC69A6453b352",
    "MAGIC/USD": "0x47E55cCec6582838E173f252D08Afd8116c2202d",
    "WOO/USD": "0x5d5Ab15fb857De6FA209B6B41C7375F1C4BD9B90",
}

# Base price feeds (Chain ID: 8453)
# Reference: https://docs.chain.link/data-feeds/price-feeds/addresses?network=base
BASE_PRICE_FEEDS: dict[str, str] = {
    "ETH/USD": "0x71041dddad3595F9CEd3DcCFBe3D1F4b0a16Bb70",
    "BTC/USD": "0x64c911996d3C6Ac71e9B8934F4e4f21B9C3bD7d1",
    "LINK/USD": "0x17CAb8FE31E32f08326e5E27412894e49B0f9D65",
    "USDC/USD": "0x7e860098F58bBFC8648a4311b374B1D669a2bc6B",
    "DAI/USD": "0x591e79239a7d679378eC8c847e5038150364C78F",
    "CBETH/USD": "0xd7818272B9e248357d13057AAb0B417aF31E817d",
    # Note: Base has no direct WSTETH/USD Chainlink feed.
    # Use derived price (WSTETH/ETH * ETH/USD) via ETH_DENOMINATED_FEEDS below.
}

# Optimism price feeds (Chain ID: 10)
# Reference: https://docs.chain.link/data-feeds/price-feeds/addresses?network=optimism
OPTIMISM_PRICE_FEEDS: dict[str, str] = {
    "ETH/USD": "0x13e3Ee699D1909E989722E753853AE30b17e08c5",
    "BTC/USD": "0xD702DD976Fb76Fffc2D3963D037dfDae5b04E593",
    "LINK/USD": "0xCC232DcFAaE6354cE191bd574108c1Ad03F86CeA",
    "USDC/USD": "0x16a9FA2FDa030272Ce99B29CF780dFA30361E0f3",
    "USDT/USD": "0xECef79e109E997BCa29c1c0897EC9D7678e00BB1",
    "DAI/USD": "0x8dBa75e83DA73cc766A7e5a0ee71F656BAb470d6",
    "OP/USD": "0x0D276FC14719f9292D5C1eA2198673d1f4269246",
    "SNX/USD": "0x2FCF37343e916eAEd1f1DdaaF84458a359b53877",
    "AAVE/USD": "0x338ed6787f463394D24813b297401B9F05a8C9d1",
    "WSTETH/USD": "0x698B585CbC4407e2D54aa898B2600B53C68958f7",
}

# Polygon price feeds (Chain ID: 137)
# Reference: https://docs.chain.link/data-feeds/price-feeds/addresses?network=polygon
POLYGON_PRICE_FEEDS: dict[str, str] = {
    "ETH/USD": "0xF9680D99D6C9589e2a93a78A04A279e509205945",
    "BTC/USD": "0xc907E116054Ad103354f2D350FD2514433D57F6f",
    "MATIC/USD": "0xAB594600376Ec9fD91F8e885dADF0CE036862dE0",
    "LINK/USD": "0xd9FFdb71EbE7496cC440152d43986Aae0AB76665",
    "USDC/USD": "0xfE4A8cc5b5B2366C1B58Bea3858e81843581b2F7",
    "USDT/USD": "0x0A6513e40db6EB1b165753AD52E80663aeA50545",
    "DAI/USD": "0x4746DeC9e833A82EC7C2C1356372CcF2cfcD2F3D",
    "AAVE/USD": "0x72484B12719E23115761D5DA1646945632979bB6",
    "UNI/USD": "0xdf0Fb4e4F928d2dCB76f438575fDD8682386e13C",
    "CRV/USD": "0x336584C8E6Dc19637A5b36206B1c79923111b405",
    "WSTETH/USD": "0x10f964234cae09cB6a9854B56FF7D4F38Cda5E6a",
}

# Sonic price feeds (Chain ID: 146)
# Reference: https://bgd-labs.github.io/aave-address-book/AaveV3Sonic.html
# Feed addresses sourced from Aave V3 Sonic deployment (bgd-labs/aave-address-book)
SONIC_PRICE_FEEDS: dict[str, str] = {
    "ETH/USD": "0x824364077993847f71293B24ccA8567c00c2de11",
    "USDC/USD": "0x7A8443a2a5D772db7f1E40DeFe32db485108F128",
    "S/USD": "0xc76dFb89fF298145b417d221B2c747d84952e01d",
}

# BNB Smart Chain price feeds (Chain ID: 56)
# Reference: https://docs.chain.link/data-feeds/price-feeds/addresses?network=bnb-chain
BSC_PRICE_FEEDS: dict[str, str] = {
    "BNB/USD": "0x0567F2323251f0Aab15c8dFb1967E4e8A7D42aeE",
    "BTC/USD": "0x264990fbd0A4796A3E3d8E37C4d5F87a3aCa5Ebf",
    "ETH/USD": "0x9ef1B8c0E4F7dc8bF5719Ea496883DC6401d5b2e",
    "USDC/USD": "0x51597f405303c4377E36123CbF172bc359765377",
    "USDT/USD": "0xB97Ad0E74fa7d920791E90258A6E2085088b4320",
    "DAI/USD": "0x132d3C0B1D2cEa0BC552588063bdBb210FDeecfA",
    "LINK/USD": "0xca236E327F629f9Fc2c30A4E95775EbF0B89fac8",
    "CAKE/USD": "0xb6064eD41d4F67e353768AA239CA98F9c422E159",
    "AAVE/USD": "0xA8357BF572460fC40f4B0aCacbB2a6A61c89f475",
}

# Avalanche C-Chain price feeds (Chain ID: 43114)
# Reference: https://docs.chain.link/data-feeds/price-feeds/addresses?network=avalanche
AVALANCHE_PRICE_FEEDS: dict[str, str] = {
    "AVAX/USD": "0x0A77230d17318075983913bC2145DB16C7366156",
    "ETH/USD": "0x976B3D034E162d8bD72D6b9C989d545b839003b0",
    "BTC/USD": "0x2779D32d5166BAaa2B2b658333bA7e6Ec0C65743",
    "LINK/USD": "0x49cCd9Ca821efeAb2B98C60Dc60f518e765EdADc",
    "USDC/USD": "0xF096872672F44d6EBA71458D74fe67F9a77a23B9",
    "USDT/USD": "0xEBE676ee90Fe1112671f19b6B7459bC678B67e8a",
    "DAI/USD": "0x51D7180edA2260cc4F6e4EebB82FEF5c3c2B8300",
    "AAVE/USD": "0x3CA13391E9fb38a75330fb28f8cc2eB3D9ceceED",
    "JOE/USD": "0x02D35d3a8aC3e1626d3eE09A78Dd87286F5E8e3a",
    "WAVAX/USD": "0x0A77230d17318075983913bC2145DB16C7366156",
}

# Combined price feeds by chain
CHAINLINK_PRICE_FEEDS: dict[str, dict[str, str]] = {
    "ethereum": ETHEREUM_PRICE_FEEDS,
    "arbitrum": ARBITRUM_PRICE_FEEDS,
    "base": BASE_PRICE_FEEDS,
    "optimism": OPTIMISM_PRICE_FEEDS,
    "polygon": POLYGON_PRICE_FEEDS,
    "bsc": BSC_PRICE_FEEDS,
    "avalanche": AVALANCHE_PRICE_FEEDS,
    "sonic": SONIC_PRICE_FEEDS,
}

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

# Expected chain IDs for Chainlink-supported chains (for RPC validation)
CHAINLINK_CHAIN_IDS: dict[str, int] = {
    "ethereum": 1,
    "arbitrum": 42161,
    "base": 8453,
    "optimism": 10,
    "polygon": 137,
    "bsc": 56,
    "avalanche": 43114,
    "sonic": 146,
}

# =============================================================================
# ETH-denominated Chainlink feeds for derived USD pricing
# =============================================================================
# Some tokens only have TOKEN/ETH feeds (no TOKEN/USD). For these, the OnChain
# price source computes TOKEN/USD = TOKEN/ETH * ETH/USD.
#
# Format: { chain: { "TOKEN/ETH": feed_address } }
ETH_DENOMINATED_FEEDS: dict[str, dict[str, str]] = {
    "arbitrum": {
        "WSTETH/ETH": "0xb523AE262D20A936BC152e6023996e46FDC2A95D",
    },
    "base": {
        # 0x43a5... is the wstETH/ETH exchange rate feed (18 decimals), NOT a USD feed.
        # Previously misconfigured as BASE_PRICE_FEEDS["WSTETH/USD"], causing prices of ~$12B.
        "WSTETH/ETH": "0x43a5C292A453A3bF3606fa856197f09D7B74251a",
    },
}

# Token symbol to ETH-denominated pair mapping
TOKEN_TO_ETH_PAIR: dict[str, str] = {
    "WSTETH": "WSTETH/ETH",
    "STETH": "WSTETH/ETH",
}
