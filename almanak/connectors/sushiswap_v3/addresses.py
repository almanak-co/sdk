"""SushiSwap V3 contract addresses per chain.

Single source of truth for this connector's on-chain addresses. Replaces
the entries previously held in ``almanak.core.contracts`` (W1 / VIB-4853
/ epic VIB-4851). Surfaced to non-connector callers through
:class:`GatewayAddressCapability` on
``SushiSwapV3GatewayConnector``; strategy-side connector code reads
the dicts directly.
"""

from __future__ import annotations

SUSHISWAP_V3: dict[str, dict[str, str]] = {
    "ethereum": {
        "swap_router": "0x2E6cd2d30aa43f40aa81619ff4b6E0a41479B13F",
        "factory": "0xbACEB8eC6b9355Dfc0269C18bac9d6E2Bdc29C4F",
        "position_manager": "0x2214A42d8e2A1d20635c2cb0664422c528B6A432",
        "quoter_v2": "0x64e8802FE490fa7cc61d3463958199161Bb608A7",
    },
    "arbitrum": {
        "swap_router": "0x8A21F6768C1f8075791D08546Dadf6daA0bE820c",
        "factory": "0x1af415a1EbA07a4986a52B6f2e7dE7003D82231e",
        "position_manager": "0xF0cBce1942A68BEB3d1b73F0dd86C8DCc363eF49",
        "quoter_v2": "0x0524E833cCD057e4d7A296e3aaAb9f7675964Ce1",
    },
    "base": {
        "swap_router": "0xfB7ef66A7e61fF9e400671e4b5BFbaBE2ea025B4",
        "factory": "0xc35DADB65012eC5796536bD9864eD8773aBc74C4",
        "position_manager": "0x80C7DD17B01855a6D2347444a0FCC36136a314de",
        "quoter_v2": "0xb1E835Dc2785b52265711e17fCCb0fd018226a6e",
    },
    "polygon": {
        "swap_router": "0x0aF89E1620b96170e2a9D0b68fEebb767eD044c3",
        "factory": "0x917933899c6a5F8E37F31E19f92CdBFF7e8FF0e2",
        "position_manager": "0xb7402ee99F0A008e461098AC3A27F4957Df89a40",
        "quoter_v2": "0xb1E835Dc2785b52265711e17fCCb0fd018226a6e",
    },
    "avalanche": {
        "swap_router": "0x717b7948AA264DeCf4D780aa6914482e5F46Da3e",
        "factory": "0x3e603C14aF37EBdaD31709C4f848Fc6aD5BEc715",
        "position_manager": "0x18350b048AB366ed601fFDbC669110Ecb36016f3",
        "quoter_v2": "0xb1E835Dc2785b52265711e17fCCb0fd018226a6e",
    },
    "bsc": {
        "swap_router": "0xB45e53277a7e0F1D35f2a77160e91e25507f1763",
        "factory": "0x126555dd55a39328F69400d6aE4F782Bd4C34ABb",
        "position_manager": "0xF70c086618dcf2b1A461311275e00D6B722ef914",
        "quoter_v2": "0xb1E835Dc2785b52265711e17fCCb0fd018226a6e",
    },
    "optimism": {
        "swap_router": "0x8516944E89f296eb6473d79aED1Ba12088016c9e",
        "factory": "0x9c6522117e2ed1fE5bdb72bb0eD5E3f2bdE7DBe0",
        "position_manager": "0x1af415a1EbA07a4986a52B6f2e7dE7003D82231e",
        "quoter_v2": "0xb1E835Dc2785b52265711e17fCCb0fd018226a6e",
    },
}

SUSHISWAP_V3_TOKENS: dict[str, dict[str, str]] = {
    "ethereum": {
        "ETH": "0xEeeeeEeeeEeEeeEeEeEeeEEEeeeeEeeeeeeeEEeE",
        "WETH": "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2",
        "USDC": "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
        "USDT": "0xdAC17F958D2ee523a2206206994597C13D831ec7",
        "DAI": "0x6B175474E89094C44Da98b954EedeAC495271d0F",
        "WBTC": "0x2260FAC5E5542a773Aa44fBCfeDf7C193bc2C599",
        "SUSHI": "0x6B3595068778DD592e39A122f4f5a5cF09C90fE2",
    },
    "arbitrum": {
        "ETH": "0xEeeeeEeeeEeEeeEeEeEeeEEEeeeeEeeeeeeeEEeE",
        "WETH": "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1",
        "USDC": "0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
        "USDC.e": "0xFF970A61A04b1cA14834A43f5dE4533eBDDB5CC8",
        "USDT": "0xFd086bC7CD5C481DCC9C85ebE478A1C0b69FCbb9",
        "DAI": "0xDA10009cBd5D07dd0CeCc66161FC93D7c9000da1",
        "WBTC": "0x2f2a2543B76A4166549F7aaB2e75Bef0aefC5B0f",
        "ARB": "0x912CE59144191C1204E64559FE8253a0e49E6548",
        "SUSHI": "0xd4d42F0b6DEF4CE0383636770eF773390d85c61A",
    },
    "base": {
        "ETH": "0xEeeeeEeeeEeEeeEeEeEeeEEEeeeeEeeeeeeeEEeE",
        "WETH": "0x4200000000000000000000000000000000000006",
        "USDC": "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913",
        "DAI": "0x50c5725949A6F0c72E6C4a641F24049A917DB0Cb",
    },
    "polygon": {
        "MATIC": "0xEeeeeEeeeEeEeeEeEeEeeEEEeeeeEeeeeeeeEEeE",
        "WMATIC": "0x0d500B1d8E8eF31E21C99d1Db9A6444d3ADf1270",
        "WETH": "0x7ceB23fD6bC0adD59E62ac25578270cFf1b9f619",
        "USDC": "0x3c499c542cEF5E3811e1192ce70d8cC03d5c3359",
        "USDC.e": "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174",
        "USDT": "0xc2132D05D31c914a87C6611C10748AEb04B58e8F",
        "DAI": "0x8f3Cf7ad23Cd3CaDbD9735AFf958023239c6A063",
        "SUSHI": "0x0b3F868E0BE5597D5DB7fEB59E1CADBb0fdDa50a",
    },
    "avalanche": {
        "AVAX": "0xEeeeeEeeeEeEeeEeEeEeeEEEeeeeEeeeeeeeEEeE",
        "WAVAX": "0xB31f66AA3C1e785363F0875A1B74E27b85FD66c7",
        "WETH.e": "0x49D5c2BdFfac6CE2BFdB6640F4F80f226bc10bAB",
        "USDC": "0xB97EF9Ef8734C71904D8002F8b6Bc66Dd9c48a6E",
        "USDT": "0x9702230A8Ea53601f5cD2dc00fDBc13d4dF4A8c7",
        "DAI.e": "0xd586E7F844cEa2F87f50152665BCbc2C279D8d70",
        "SUSHI": "0x37B608519F91f70F2EeB0e5Ed9AF4061722e4F76",
    },
    "bsc": {
        "BNB": "0xEeeeeEeeeEeEeeEeEeEeeEEEeeeeEeeeeeeeEEeE",
        "WBNB": "0xbb4CdB9CBd36B01bD1cBaEBF2De08d9173bc095c",
        "WETH": "0x2170Ed0880ac9A755fd29B2688956BD959F933F8",
        "USDC": "0x8AC76a51cc950d9822D68b83fE1Ad97B32Cd580d",
        "USDT": "0x55d398326f99059fF775485246999027B3197955",
        "DAI": "0x1AF3F329e8BE154074D8769D1FFa4eE058B1DBc3",
        "SUSHI": "0x947950BcC74888a40Ffa2593C5798F11Fc9124C4",
    },
    "optimism": {
        "ETH": "0xEeeeeEeeeEeEeeEeEeEeeEEEeeeeEeeeeeeeEEeE",
        "WETH": "0x4200000000000000000000000000000000000006",
        "USDC": "0x0b2C639c533813f4Aa9D7837CAf62653d097Ff85",
        "USDC.e": "0x7F5c764cBc14f9669B88837ca1490cCa17c31607",
        "USDT": "0x94b008aA00579c1307B0EF2c499aD98a8ce58e58",
        "DAI": "0xDA10009cBd5D07dd0CeCc66161FC93D7c9000da1",
        "OP": "0x4200000000000000000000000000000000000042",
        "SUSHI": "0x3eaEb77b03dBc0F6321AE1b72b2E9aDb0F60112B",
    },
}


__all__ = ["SUSHISWAP_V3", "SUSHISWAP_V3_TOKENS"]
