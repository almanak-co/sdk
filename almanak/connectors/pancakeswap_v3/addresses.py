"""PancakeSwap V3 contract addresses per chain.

Single source of truth for this connector's on-chain addresses. Replaces
the entries previously held in ``almanak.core.contracts`` (W1 / VIB-4853
/ epic VIB-4851). Surfaced to non-connector callers through
:class:`GatewayAddressCapability` on
``PancakeSwapV3GatewayConnector``; strategy-side connector code reads
the dicts directly.
"""

from __future__ import annotations

PANCAKESWAP_V3: dict[str, dict[str, str]] = {
    "bsc": {
        "swap_router": "0x13f4EA83D0bd40E75C8222255bc855a974568Dd4",  # SmartRouter
        "factory": "0x0BFbCF9fa4f9C56B0F40a671Ad40E0805A091865",
        "quoter": "0xB048Bbc1Ee6b733FFfCFb9e9CeF7375518e25997",
        "nft": "0x46A15B0b27311cedF172AB29E4f4766fbE7F4364",  # NonfungiblePositionManager
    },
    "ethereum": {
        "swap_router": "0x13f4EA83D0bd40E75C8222255bc855a974568Dd4",  # SmartRouter
        "factory": "0x0BFbCF9fa4f9C56B0F40a671Ad40E0805A091865",
        "quoter": "0xB048Bbc1Ee6b733FFfCFb9e9CeF7375518e25997",
        "nft": "0x46A15B0b27311cedF172AB29E4f4766fbE7F4364",  # NonfungiblePositionManager
    },
    "arbitrum": {
        "swap_router": "0x32226588378236Fd0c7c4053999F88aC0e5cAc77",  # SmartRouter
        "factory": "0x0BFbCF9fa4f9C56B0F40a671Ad40E0805A091865",
        "quoter": "0xB048Bbc1Ee6b733FFfCFb9e9CeF7375518e25997",
        "nft": "0x46A15B0b27311cedF172AB29E4f4766fbE7F4364",  # NonfungiblePositionManager
    },
    "base": {
        "swap_router": "0x678Aa4bF4E210cf2166753e054d5b7c31cc7fa86",  # SmartRouter
        "factory": "0x0BFbCF9fa4f9C56B0F40a671Ad40E0805A091865",
        "quoter": "0xB048Bbc1Ee6b733FFfCFb9e9CeF7375518e25997",
        "nft": "0x46A15B0b27311cedF172AB29E4f4766fbE7F4364",  # NonfungiblePositionManager
    },
    "linea": {
        "swap_router": "0x678Aa4bF4E210cf2166753e054d5b7c31cc7fa86",  # SmartRouter
        "factory": "0x0BFbCF9fa4f9C56B0F40a671Ad40E0805A091865",
        "quoter": "0xB048Bbc1Ee6b733FFfCFb9e9CeF7375518e25997",
        "nft": "0x46A15B0b27311cedF172AB29E4f4766fbE7F4364",  # NonfungiblePositionManager
    },
}

PANCAKESWAP_V3_TOKENS: dict[str, dict[str, str]] = {
    "bsc": {
        "WBNB": "0xbb4CdB9CBd36B01bD1cBaEBF2De08d9173bc095c",
        "USDT": "0x55d398326f99059fF775485246999027B3197955",
        "USDC": "0x8AC76a51cc950d9822D68b83fE1Ad97B32Cd580d",
        "BUSD": "0xe9e7CEA3DedcA5984780Bafc599bD69ADd087D56",
        "ETH": "0x2170Ed0880ac9A755fd29B2688956BD959F933F8",
        "BTCB": "0x7130d2A12B9BCbFAe4f2634d864A1Ee1Ce3Ead9c",
        "CAKE": "0x0E09FaBB73Bd3Ade0a17ECC321fD13a19e81cE82",
    },
    "ethereum": {
        "WETH": "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2",
        "USDC": "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
        "USDT": "0xdAC17F958D2ee523a2206206994597C13D831ec7",
        "WBTC": "0x2260FAC5E5542a773Aa44fBCfeDf7C193bc2C599",
        "DAI": "0x6B175474E89094C44Da98b954EedeAC495271d0F",
    },
    "arbitrum": {
        "WETH": "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1",
        "USDC": "0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
        "USDT": "0xFd086bC7CD5C481DCC9C85ebE478A1C0b69FCbb9",
        "WBTC": "0x2f2a2543B76A4166549F7aaB2e75Bef0aefC5B0f",
        "ARB": "0x912CE59144191C1204E64559FE8253a0e49E6548",
    },
    "linea": {
        "WETH": "0xe5D7C2a44FfDDf6b295A15c148167daaAf5Cf34f",
        "USDC": "0x176211869cA2b568f2A7D4EE941E073a821EE1ff",
        "USDT": "0xA219439258ca9da29E9Cc4cE5596924745e12B93",
    },
}


__all__ = ["PANCAKESWAP_V3", "PANCAKESWAP_V3_TOKENS"]
