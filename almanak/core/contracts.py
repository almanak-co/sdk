"""Centralized Contract Address Registry.

Single source of truth for all protocol contract addresses across chains.
Organized by protocol, then chain, with contract types as keys.

Usage:
    from almanak.core.contracts import UNISWAP_V3, UNISWAP_V3_TOKENS, get_address

    # Get all Uniswap V3 addresses for a chain
    addresses = UNISWAP_V3["arbitrum"]
    router = addresses["swap_router"]

    # Or use the helper
    router = get_address(UNISWAP_V3, "arbitrum", "swap_router")
"""

# =============================================================================
# Uniswap V3
# =============================================================================

UNISWAP_V3: dict[str, dict[str, str]] = {
    "ethereum": {
        "swap_router": "0x68b3465833fb72A70ecDF485E0e4C7bD8665Fc45",  # SwapRouter02
        "swap_router_02": "0x68b3465833fb72A70ecDF485E0e4C7bD8665Fc45",
        "factory": "0x1F98431c8aD98523631AE4a59f267346ea31F984",
        "position_manager": "0xC36442b4a4522E871399CD717aBDD847Ab11FE88",
        "quoter_v2": "0x61fFE014bA17989E743c5F6cB21bF9697530B21e",
    },
    "arbitrum": {
        "swap_router": "0x68b3465833fb72A70ecDF485E0e4C7bD8665Fc45",  # SwapRouter02
        "swap_router_02": "0x68b3465833fb72A70ecDF485E0e4C7bD8665Fc45",
        "factory": "0x1F98431c8aD98523631AE4a59f267346ea31F984",
        "position_manager": "0xC36442b4a4522E871399CD717aBDD847Ab11FE88",
        "quoter_v2": "0x61fFE014bA17989E743c5F6cB21bF9697530B21e",
    },
    "optimism": {
        "swap_router": "0x68b3465833fb72A70ecDF485E0e4C7bD8665Fc45",  # SwapRouter02
        "swap_router_02": "0x68b3465833fb72A70ecDF485E0e4C7bD8665Fc45",
        "factory": "0x1F98431c8aD98523631AE4a59f267346ea31F984",
        "position_manager": "0xC36442b4a4522E871399CD717aBDD847Ab11FE88",
        "quoter_v2": "0x61fFE014bA17989E743c5F6cB21bF9697530B21e",
    },
    "polygon": {
        "swap_router": "0x68b3465833fb72A70ecDF485E0e4C7bD8665Fc45",  # SwapRouter02
        "swap_router_02": "0x68b3465833fb72A70ecDF485E0e4C7bD8665Fc45",
        "factory": "0x1F98431c8aD98523631AE4a59f267346ea31F984",
        "position_manager": "0xC36442b4a4522E871399CD717aBDD847Ab11FE88",
        "quoter_v2": "0x61fFE014bA17989E743c5F6cB21bF9697530B21e",
    },
    "base": {
        "swap_router": "0x2626664c2603336E57B271c5C0b26F421741e481",
        "swap_router_02": "0x2626664c2603336E57B271c5C0b26F421741e481",
        "factory": "0x33128a8fC17869897dcE68Ed026d694621f6FDfD",
        "position_manager": "0x03a520b32C04BF3bEEf7BEb72E919cf822Ed34f1",
        "quoter_v2": "0x3d4e44Eb1374240CE5F1B871ab261CD16335B76a",
    },
    "avalanche": {
        "swap_router": "0xbb00FF08d01D300023C629E8fFfFcb65A5a578cE",
        "swap_router_02": "0xbb00FF08d01D300023C629E8fFfFcb65A5a578cE",
        "factory": "0x740b1c1de25031C31FF4fC9A62f554A55cdC1baD",
        "position_manager": "0x655C406EBFa14EE2006250925e54ec43AD184f8B",
        "quoter_v2": "0xbe0F5544EC67e9B3b2D979aaA43f18Fd87E6257F",
    },
    "bsc": {
        "swap_router": "0xB971eF87ede563556b2ED4b1C0b0019111Dd85d2",
        "swap_router_02": "0xB971eF87ede563556b2ED4b1C0b0019111Dd85d2",
        "factory": "0xdB1d10011AD0Ff90774D0C6Bb92e5C5c8b4461F7",
        "position_manager": "0x7b8A01B39D58278b5DE7e48c8449c9f4F5170613",
        "quoter_v2": "0x78D78E420Da98ad378D7799bE8f4AF69033EB077",
    },
    "linea": {
        "swap_router": "0x3d4e44Eb1374240CE5F1B871ab261CD16335B76a",
        "swap_router_02": "0x3d4e44Eb1374240CE5F1B871ab261CD16335B76a",
        "factory": "0x31FAfd4889FA1269F7a13A66eE0fB458f27D72A9",
        "position_manager": "0x4615C383F85D0a2BbED973d83ccecf5CB7121463",
        "quoter_v2": "0x42bE4D6527829FeFA1493e1fb9F3676d2425C3C1",
    },
    "blast": {
        "swap_router": "0x549FEB8c9bd4c12Ad2AB27022dA12492aC452B66",
        "swap_router_02": "0x549FEB8c9bd4c12Ad2AB27022dA12492aC452B66",
        "factory": "0x792edAdE80af5fC680d96a2eD80A44247D2Cf6Fd",
        "position_manager": "0xB218e4f7cF0533d4696fDfC419A0023D33345F28",
        "quoter_v2": "0x6Cdcd65e03c1CEc3730AeeCd45bc140D57A25C77",
    },
    "mantle": {
        # Agni Finance (Uniswap V3 fork) — the primary V3 DEX on Mantle
        "swap_router": "0x319B69888b0d11cEC22caA5034e25FfFBDc88421",  # Agni SwapRouter (V1 style, with deadline)
        "swap_router_02": "0x319B69888b0d11cEC22caA5034e25FfFBDc88421",
        "factory": "0x25780dc8Fc3cfBD75F33bFDAB65e969b603b2035",  # Agni Factory
        "position_manager": "0x218bf598D1453383e2F4AA7b14fFB9BfB102D637",  # Agni NonfungiblePositionManager
        "quoter_v2": "0xc4aaDc921E1cdb66c5300Bc158a313292923C0cb",  # Agni QuoterV2
    },
    "monad": {
        # Provisional — Monad testnet (chain 143) addresses; verify before mainnet launch
        "swap_router": "0xfE31F71C1b106EAc32F1A19239c9a9A72ddfb900",  # SwapRouter02
        "swap_router_02": "0xfE31F71C1b106EAc32F1A19239c9a9A72ddfb900",
        "factory": "0x204FAca1764B154221e35c0d20aBb3c525710498",
        "position_manager": "0x7197E214c0b767cFB76Fb734ab638E2c192F4E53",
        "quoter_v2": "0x661E93cca42AfacB172121EF892830cA3b70F08d",
    },
}

UNISWAP_V3_TOKENS: dict[str, dict[str, str]] = {
    "ethereum": {
        "ETH": "0xEeeeeEeeeEeEeeEeEeEeeEEEeeeeEeeeeeeeEEeE",
        "WETH": "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2",
        "USDC": "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
        "USDT": "0xdAC17F958D2ee523a2206206994597C13D831ec7",
        "DAI": "0x6B175474E89094C44Da98b954EedeAC495271d0F",
        "WBTC": "0x2260FAC5E5542a773Aa44fBCfeDf7C193bc2C599",
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
    },
    "optimism": {
        "ETH": "0xEeeeeEeeeEeEeeEeEeEeeEEEeeeeEeeeeeeeEEeE",
        "WETH": "0x4200000000000000000000000000000000000006",
        "USDC": "0x0b2C639c533813f4Aa9D7837CAf62653d097Ff85",
        "USDC.e": "0x7F5c764cBc14f9669B88837ca1490cCa17c31607",
        "USDT": "0x94b008aA00579c1307B0EF2c499aD98a8ce58e58",
        "DAI": "0xDA10009cBd5D07dd0CeCc66161FC93D7c9000da1",
        "OP": "0x4200000000000000000000000000000000000042",
    },
    "polygon": {
        "MATIC": "0xEeeeeEeeeEeEeeEeEeEeeEEEeeeeEeeeeeeeEEeE",
        "WMATIC": "0x0d500B1d8E8eF31E21C99d1Db9A6444d3ADf1270",
        "WETH": "0x7ceB23fD6bC0adD59E62ac25578270cFf1b9f619",
        "USDC": "0x3c499c542cEF5E3811e1192ce70d8cC03d5c3359",
        "USDC.e": "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174",
        "USDT": "0xc2132D05D31c914a87C6611C10748AEb04B58e8F",
        "DAI": "0x8f3Cf7ad23Cd3CaDbD9735AFf958023239c6A063",
    },
    "base": {
        "ETH": "0xEeeeeEeeeEeEeeEeEeEeeEEEeeeeEeeeeeeeEEeE",
        "WETH": "0x4200000000000000000000000000000000000006",
        "USDC": "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913",
        "DAI": "0x50c5725949A6F0c72E6C4a641F24049A917DB0Cb",
    },
    "avalanche": {
        "AVAX": "0xEeeeeEeeeEeEeeEeEeEeeEEEeeeeEeeeeeeeEEeE",
        "WAVAX": "0xB31f66AA3C1e785363F0875A1B74E27b85FD66c7",
        "WETH.e": "0x49D5c2BdFfac6CE2BFdB6640F4F80f226bc10bAB",
        "USDC": "0xB97EF9Ef8734C71904D8002F8b6Bc66Dd9c48a6E",
        "USDT": "0x9702230A8Ea53601f5cD2dc00fDBc13d4dF4A8c7",
        "DAI.e": "0xd586E7F844cEa2F87f50152665BCbc2C279D8d70",
        "WBTC.e": "0x50b7545627a5162F82A992c33b87aDc75187B218",
    },
    "bsc": {
        "BNB": "0xEeeeeEeeeEeEeeEeEeEeeEEEeeeeEeeeeeeeEEeE",
        "WBNB": "0xbb4CdB9CBd36B01bD1cBaEBF2De08d9173bc095c",
        "WETH": "0x2170Ed0880ac9A755fd29B2688956BD959F933F8",
        "USDC": "0x8AC76a51cc950d9822D68b83fE1Ad97B32Cd580d",
        "USDT": "0x55d398326f99059fF775485246999027B3197955",
        "DAI": "0x1AF3F329e8BE154074D8769D1FFa4eE058B1DBc3",
        "BTCB": "0x7130d2A12B9BCbFAe4f2634d864A1Ee1Ce3Ead9c",
    },
    "linea": {
        "ETH": "0xEeeeeEeeeEeEeeEeEeEeeEEEeeeeEeeeeeeeEEeE",
        "WETH": "0xe5D7C2a44FfDDf6b295A15c148167daaAf5Cf34f",
        "USDC": "0x176211869cA2b568f2A7D4EE941E073a821EE1ff",
        "USDT": "0xA219439258ca9da29E9Cc4cE5596924745e12B93",
        "DAI": "0x4AF15ec2A0BD43Db75dd04E62FAA3B8EF36b00d5",
        "WBTC": "0x3aAB2285ddcDdaD8edf438C1bAB47e1a9D05a9b4",
    },
    "blast": {
        "ETH": "0xEeeeeEeeeEeEeeEeEeEeeEEEeeeeEeeeeeeeEEeE",
        "WETH": "0x4300000000000000000000000000000000000004",
        "USDB": "0x4300000000000000000000000000000000000003",
    },
    "mantle": {
        "MNT": "0xEeeeeEeeeEeEeeEeEeEeeEEEeeeeEeeeeeeeEEeE",
        "WMNT": "0x78c1b0C915c4FAA5FffA6CAbf0219DA63d7f4cb8",
        "WETH": "0xdEAddEaDdeadDEadDEADDEAddEADDEAddead1111",
        "USDC": "0x09Bc4E0D864854c6aFB6eB9A9cdF58aC190D0dF9",
        "USDT": "0x201EBa5CC46D216Ce6DC03F6a759e8E766e956aE",
    },
    "monad": {
        # Provisional — Monad testnet (chain 143) token addresses; verify before mainnet launch
        "MON": "0xEeeeeEeeeEeEeeEeEeEeeEEEeeeeEeeeeeeeEEeE",
        "WMON": "0x3bd359C1119dA7Da1D913D1C4D2B7c461115433A",
        "WETH": "0xEE8c0E9f1BFFb4Eb878d8f15f368A02a35481242",  # WETH on Monad
        "USDC": "0x754704Bc059F8C67012fEd69BC8A327a5aafb603",
        "USDT0": "0xe7cd86e13AC4309349F30B3435a9d337750fC82D",
        "WBTC": "0x0555E30da8f98308EdB960aa94C0Db47230d2B9c",
    },
}

# =============================================================================
# Uniswap V4
# =============================================================================

# Uniswap V4 uses a singleton PoolManager deployed via CREATE2 (same address across chains).
# The Universal Router V2 supports both V3 and V4 swaps.
# V4SwapRouter is the dedicated V4 swap router.
UNISWAP_V4: dict[str, dict[str, str]] = {
    "ethereum": {
        "pool_manager": "0x000000000004444c5dc75cB358380D2e3dE08A90",
        "position_manager": "0xBd216513D74C8cf14cF4747E6AaE6fDf64e83b24",
        "universal_router": "0x66a9893cC07D91D95644AEDD05D03f95e1dBA8Af",
        "v4_swap_router": "0x3A9D48AB9751398BbFa63ad67599Bb04e4BdF98b",
        "quoter": "0x52F0E24D1c21C8A0cB1e5a5dD6198556BD9E1203",
        "state_view": "0x7ffA62d1F57a97A4A4A35c6dDF1f9e36bCBBbE8a",
    },
    "arbitrum": {
        "pool_manager": "0x000000000004444c5dc75cB358380D2e3dE08A90",
        "position_manager": "0xBd216513D74C8cf14cF4747E6AaE6fDf64e83b24",
        "universal_router": "0x66a9893cC07D91D95644AEDD05D03f95e1dBA8Af",
        "v4_swap_router": "0x3A9D48AB9751398BbFa63ad67599Bb04e4BdF98b",
        "quoter": "0x52F0E24D1c21C8A0cB1e5a5dD6198556BD9E1203",
        "state_view": "0x7ffA62d1F57a97A4A4A35c6dDF1f9e36bCBBbE8a",
    },
    "base": {
        "pool_manager": "0x000000000004444c5dc75cB358380D2e3dE08A90",
        "position_manager": "0xBd216513D74C8cf14cF4747E6AaE6fDf64e83b24",
        "universal_router": "0x66a9893cC07D91D95644AEDD05D03f95e1dBA8Af",
        "v4_swap_router": "0x3A9D48AB9751398BbFa63ad67599Bb04e4BdF98b",
        "quoter": "0x52F0E24D1c21C8A0cB1e5a5dD6198556BD9E1203",
        "state_view": "0x7ffA62d1F57a97A4A4A35c6dDF1f9e36bCBBbE8a",
    },
    "optimism": {
        "pool_manager": "0x000000000004444c5dc75cB358380D2e3dE08A90",
        "position_manager": "0xBd216513D74C8cf14cF4747E6AaE6fDf64e83b24",
        "universal_router": "0x66a9893cC07D91D95644AEDD05D03f95e1dBA8Af",
        "v4_swap_router": "0x3A9D48AB9751398BbFa63ad67599Bb04e4BdF98b",
        "quoter": "0x52F0E24D1c21C8A0cB1e5a5dD6198556BD9E1203",
        "state_view": "0x7ffA62d1F57a97A4A4A35c6dDF1f9e36bCBBbE8a",
    },
    "polygon": {
        "pool_manager": "0x000000000004444c5dc75cB358380D2e3dE08A90",
        "position_manager": "0xBd216513D74C8cf14cF4747E6AaE6fDf64e83b24",
        "universal_router": "0x66a9893cC07D91D95644AEDD05D03f95e1dBA8Af",
        "v4_swap_router": "0x3A9D48AB9751398BbFa63ad67599Bb04e4BdF98b",
        "quoter": "0x52F0E24D1c21C8A0cB1e5a5dD6198556BD9E1203",
        "state_view": "0x7ffA62d1F57a97A4A4A35c6dDF1f9e36bCBBbE8a",
    },
    "avalanche": {
        "pool_manager": "0x000000000004444c5dc75cB358380D2e3dE08A90",
        "position_manager": "0xBd216513D74C8cf14cF4747E6AaE6fDf64e83b24",
        "universal_router": "0x66a9893cC07D91D95644AEDD05D03f95e1dBA8Af",
        "v4_swap_router": "0x3A9D48AB9751398BbFa63ad67599Bb04e4BdF98b",
        "quoter": "0x52F0E24D1c21C8A0cB1e5a5dD6198556BD9E1203",
        "state_view": "0x7ffA62d1F57a97A4A4A35c6dDF1f9e36bCBBbE8a",
    },
    "bsc": {
        "pool_manager": "0x000000000004444c5dc75cB358380D2e3dE08A90",
        "position_manager": "0xBd216513D74C8cf14cF4747E6AaE6fDf64e83b24",
        "universal_router": "0x66a9893cC07D91D95644AEDD05D03f95e1dBA8Af",
        "v4_swap_router": "0x3A9D48AB9751398BbFa63ad67599Bb04e4BdF98b",
        "quoter": "0x52F0E24D1c21C8A0cB1e5a5dD6198556BD9E1203",
        "state_view": "0x7ffA62d1F57a97A4A4A35c6dDF1f9e36bCBBbE8a",
    },
}

# =============================================================================
# PancakeSwap V3
# =============================================================================

PANCAKESWAP_V3: dict[str, dict[str, str]] = {
    "bsc": {
        "swap_router": "0x13f4EA83D0bd40E75C8222255bc855a974568Dd4",  # SmartRouter
        "factory": "0x0BFbCF9fa4f9C56B0F40a671Ad40E0805A091865",
        "quoter": "0xB048Bbc1Ee6b733FFfCFb9e9CeF7375518e25997",
    },
    "ethereum": {
        "swap_router": "0x13f4EA83D0bd40E75C8222255bc855a974568Dd4",  # SmartRouter
        "factory": "0x0BFbCF9fa4f9C56B0F40a671Ad40E0805A091865",
        "quoter": "0xB048Bbc1Ee6b733FFfCFb9e9CeF7375518e25997",
    },
    "arbitrum": {
        "swap_router": "0x32226588378236Fd0c7c4053999F88aC0e5cAc77",  # SmartRouter
        "factory": "0x0BFbCF9fa4f9C56B0F40a671Ad40E0805A091865",
        "quoter": "0xB048Bbc1Ee6b733FFfCFb9e9CeF7375518e25997",
    },
    "base": {
        "swap_router": "0x678Aa4bF4E210cf2166753e054d5b7c31cc7fa86",  # SmartRouter
        "factory": "0x0BFbCF9fa4f9C56B0F40a671Ad40E0805A091865",
        "quoter": "0xB048Bbc1Ee6b733FFfCFb9e9CeF7375518e25997",
    },
    "linea": {
        "swap_router": "0x678Aa4bF4E210cf2166753e054d5b7c31cc7fa86",  # SmartRouter
        "factory": "0x0BFbCF9fa4f9C56B0F40a671Ad40E0805A091865",
        "quoter": "0xB048Bbc1Ee6b733FFfCFb9e9CeF7375518e25997",
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

# =============================================================================
# Aave V3
# =============================================================================

AAVE_V3: dict[str, dict[str, str]] = {
    "ethereum": {
        "pool": "0x87870Bca3F3fD6335C3F4ce8392D69350B4fA4E2",
        "pool_data_provider": "0x7B4EB56E7CD4b454BA8ff71E4518426369a138a3",
        "oracle": "0x54586bE62E3c3580375aE3723C145253060Ca0C2",
    },
    "arbitrum": {
        "pool": "0x794a61358D6845594F94dc1DB02A252b5b4814aD",
        "pool_data_provider": "0x69FA688f1Dc47d4B5d8029D5a35FB7a548310654",
        "oracle": "0xb56c2F0B653B2e0b10C9b928C8580Ac5Df02C7C7",
    },
    "optimism": {
        "pool": "0x794a61358D6845594F94dc1DB02A252b5b4814aD",
        "pool_data_provider": "0x69FA688f1Dc47d4B5d8029D5a35FB7a548310654",
        "oracle": "0xD81eb3728a631871a7eBBaD631b5f424909f0c77",
    },
    "polygon": {
        "pool": "0x794a61358D6845594F94dc1DB02A252b5b4814aD",
        "pool_data_provider": "0x69FA688f1Dc47d4B5d8029D5a35FB7a548310654",
        "oracle": "0xb023e699F5a33916Ea823A16485e259257cA8Bd1",
    },
    "base": {
        "pool": "0xA238Dd80C259a72e81d7e4664a9801593F98d1c5",
        "pool_data_provider": "0x2d8A3C5677189723C4cB8873CfC9C8976FDF38Ac",
        "oracle": "0x2Cc0Fc26eD4563A5ce5e8bdcfe1A2878676Ae156",
    },
    "avalanche": {
        "pool": "0x794a61358D6845594F94dc1DB02A252b5b4814aD",
        "pool_data_provider": "0x69FA688f1Dc47d4B5d8029D5a35FB7a548310654",
        "oracle": "0xEBd36016B3eD09D4693Ed4251c67Bd858c3c7C9C",
    },
    "bsc": {
        "pool": "0x6807dc923806fE8Fd134338EABCA509979a7e0cB",
        "pool_data_provider": "0x41585C50524fb8c3899B43D7D797d9486AAc94DB",
        "oracle": "0x39bc1bfDa2130d6Bb6DBEfd366939b4c7aa7C697",
    },
    "linea": {
        "pool": "0xc47b8C00b0f69a36fa203Ffeac0334874574a8Ac",
        "pool_data_provider": "0x2D97F8FA96886Fd923c065F5457F9DDd494e3877",
        "oracle": "0x3c6Cd9Cc7c7a4c2Cf5a82734CD249D7D593354dA",
    },
    "plasma": {
        "pool": "0x925a2A7214Ed92428B5b1B090F80b25700095e12",
        "pool_data_provider": "0x69FA688f1Dc47d4B5d8029D5a35FB7a548310654",
        "oracle": "0xb56c2F0B653B2e0b10C9b928C8580Ac5Df02C7C7",
    },
    "sonic": {
        "pool": "0x5362dBb1e601abF3a4c14c22ffEdA64042E5eAA3",
        "pool_data_provider": "0xc0a344397cfa89dF1e1d3e4fb330834D789cF2CD",
        "oracle": "0xD63f7658C66B2934Bd234D79D06aEF5290734B30",
    },
    "mantle": {
        "pool": "0x458F293454fE0d67EC0655f3672301301DD51422",
        "pool_data_provider": "0x487c5c669D9eee6057C44973207101276cf73b68",
        "oracle": "0x47a063CfDa980532267970d478EC340C0F80E8df",
    },
}

AAVE_V3_TOKENS: dict[str, dict[str, str]] = {
    "ethereum": {
        "WETH": "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2",
        "USDC": "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
        "USDT": "0xdAC17F958D2ee523a2206206994597C13D831ec7",
        "DAI": "0x6B175474E89094C44Da98b954EedeAC495271d0F",
        "WBTC": "0x2260FAC5E5542a773Aa44fBCfeDf7C193bc2C599",
        "LINK": "0x514910771AF9Ca656af840dff83E8264EcF986CA",
        "AAVE": "0x7Fc66500c84A76Ad7e9c93437bFc5Ac33E2DDaE9",
        "wstETH": "0x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0",
        "cbETH": "0xBe9895146f7AF43049ca1c1AE358B0541Ea49704",
        "rETH": "0xae78736Cd615f374D3085123A210448E74Fc6393",
    },
    "arbitrum": {
        "WETH": "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1",
        "USDC": "0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
        "USDC.e": "0xFF970A61A04b1cA14834A43f5dE4533eBDDB5CC8",
        "USDT": "0xFd086bC7CD5C481DCC9C85ebE478A1C0b69FCbb9",
        "DAI": "0xDA10009cBd5D07dd0CeCc66161FC93D7c9000da1",
        "WBTC": "0x2f2a2543B76A4166549F7aaB2e75Bef0aefC5B0f",
        "LINK": "0xf97f4df75117a78c1A5a0DBb814Af92458539FB4",
        "ARB": "0x912CE59144191C1204E64559FE8253a0e49E6548",
        "wstETH": "0x5979D7b546E38E414F7E9822514be443A4800529",
        "rETH": "0xEC70Dcb4A1EFa46b8F2D97C310C9c4790ba5ffA8",
    },
    "optimism": {
        "WETH": "0x4200000000000000000000000000000000000006",
        "USDC": "0x0b2C639c533813f4Aa9D7837CAf62653d097Ff85",
        "USDC.e": "0x7F5c764cBc14f9669B88837ca1490cCa17c31607",
        "USDT": "0x94b008aA00579c1307B0EF2c499aD98a8ce58e58",
        "DAI": "0xDA10009cBd5D07dd0CeCc66161FC93D7c9000da1",
        "wstETH": "0x1F32b1c2345538c0c6f582fCB022739c4A194Ebb",
        "OP": "0x4200000000000000000000000000000000000042",
        "rETH": "0x9Bcef72be871e61ED4fBbc7630889beE758eb81D",
    },
    "polygon": {
        "WMATIC": "0x0d500B1d8E8eF31E21C99d1Db9A6444d3ADf1270",
        "WETH": "0x7ceB23fD6bC0adD59E62ac25578270cFf1b9f619",
        "USDC": "0x3c499c542cEF5E3811e1192ce70d8cC03d5c3359",
        "USDC.e": "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174",
        "USDT": "0xc2132D05D31c914a87C6611C10748AEb04B58e8F",
        "DAI": "0x8f3Cf7ad23Cd3CaDbD9735AFf958023239c6A063",
        "WBTC": "0x1BFD67037B42Cf73acF2047067bd4F2C47D9BfD6",
        "LINK": "0x53E0bca35eC356BD5ddDFebbD1Fc0fD03FaBad39",
        "wstETH": "0x03b54A6e9a984069379fae1a4fC4dBAE93B3bCCD",
    },
    "base": {
        "WETH": "0x4200000000000000000000000000000000000006",
        "USDC": "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913",
        "cbETH": "0x2Ae3F1Ec7F1F5012CFEab0185bfc7aa3cf0DEc22",
        "wstETH": "0xc1CBa3fCea344f92D9239c08C0568f6F2F0ee452",
    },
    "avalanche": {
        "WAVAX": "0xB31f66AA3C1e785363F0875A1B74E27b85FD66c7",
        "WETH.e": "0x49D5c2BdFfac6CE2BFdB6640F4F80f226bc10bAB",
        "USDC": "0xB97EF9Ef8734C71904D8002F8b6Bc66Dd9c48a6E",
        "USDT": "0x9702230A8Ea53601f5cD2dc00fDBc13d4dF4A8c7",
        "DAI.e": "0xd586E7F844cEa2F87f50152665BCbc2C279D8d70",
        "WBTC.e": "0x50b7545627a5162F82A992c33b87aDc75187B218",
        "LINK.e": "0x5947BB275c521040051D82396192181b413227A3",
        "sAVAX": "0x2b2C81e08f1Af8835a78Bb2A90AE924ACE0eA4bE",
    },
    "bsc": {
        "WBNB": "0xbb4CdB9CBd36B01bD1cBaEBF2De08d9173bc095c",
        "WETH": "0x2170Ed0880ac9A755fd29B2688956BD959F933F8",
        "USDC": "0x8AC76a51cc950d9822D68b83fE1Ad97B32Cd580d",
        "USDT": "0x55d398326f99059fF775485246999027B3197955",
        "BTCB": "0x7130d2A12B9BCbFAe4f2634d864A1Ee1Ce3Ead9c",
    },
    "linea": {
        "WETH": "0xe5D7C2a44FfDDf6b295A15c148167daaAf5Cf34f",
        "USDC": "0x176211869cA2b568f2A7D4EE941E073a821EE1ff",
        "USDT": "0xA219439258ca9da29E9Cc4cE5596924745e12B93",
        "DAI": "0x4AF15ec2A0BD43Db75dd04E62FAA3B8EF36b00d5",
        "WBTC": "0x3aAB2285ddcDdaD8edf438C1bAB47e1a9D05a9b4",
    },
    "plasma": {
        "WETH": "0x9895D81bB462A195b4922ED7De0e3ACD007c32CB",
    },
    "sonic": {
        "wS": "0x039e2fB66102314Ce7b64Ce5Ce3E5183bc94aD38",
        "WETH": "0x50c42dEAcD8Fc9773493ED674b675bE577f2634b",
        "USDC": "0x29219dd400f2Bf60E5a23d13Be72B486D4038894",
        "USDT": "0x6047828dc181963ba44974801FF68e538dA5eaF9",
    },
    "mantle": {
        "WMNT": "0x78c1b0C915c4FAA5FffA6CAbf0219DA63d7f4cb8",
        "WETH": "0xdEAddEaDdeadDEadDEADDEAddEADDEAddead1111",
        "USDT0": "0x201EBa5CC46D216Ce6DC03F6a759e8E766e956aE",
    },
}

# =============================================================================
# SushiSwap V3
# =============================================================================

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

# =============================================================================
# GMX V2
# =============================================================================

GMX_V2: dict[str, dict[str, str]] = {
    "arbitrum": {
        "exchange_router": "0x1C3fa76e6E1088bCE750f23a5BFcffa1efEF6A41",
        "router": "0x7452c558d45f8afC8c83dAe62C3f8A5BE19c71f6",
        "data_store": "0xFD70de6b91282D8017aA4E741e9Ae325CAb992d8",
        "order_vault": "0x31eF83a530Fde1B38EE9A18093A333D8Bbbc40D5",
        "reader": "0x470fbC46bcC0f16532691Df360A07d8Bf5ee0789",
        "eth_usd_market": "0x70d95587d40A2caf56bd97485aB3Eec10Bee6336",
        "btc_usd_market": "0x47c031236e19d024b42f8AE6780E44A573170703",
    },
}

GMX_V2_TOKENS: dict[str, dict[str, str]] = {
    "arbitrum": {
        "WETH": "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1",
        "WBTC": "0x2f2a2543B76A4166549F7aaB2e75Bef0aefC5B0f",
        "USDC": "0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
        "USDT": "0xFd086bC7CD5C481DCC9C85ebE478A1C0b69FCbb9",
    },
}

# =============================================================================
# Aerodrome
# =============================================================================

AERODROME: dict[str, dict[str, str]] = {
    "base": {
        "router": "0xcF77a3Ba9A5CA399B7c97c74d54e5b1Beb874E43",
        "factory": "0x420DD381b31aEf6683db6B902084cB0FFECe40Da",
        "voter": "0x16613524e02ad97eDfeF371bC883F2F5d6C480A5",
        "cl_router": "0xBE6D8f0d05cC4be24d5167a3eF062215bE6D18a5",
        "cl_factory": "0x5e7BB104d84c7CB9B682AaC2F3d509f5F406809A",
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

# =============================================================================
# TraderJoe V2
# =============================================================================

TRADERJOE_V2: dict[str, dict[str, str]] = {
    "avalanche": {
        "factory": "0x8e42f2F4101563bF679975178e880FD87d3eFd4e",
        "router": "0xb4315e873dBcf96Ffd0acd8EA43f689D8c20fB30",  # LBRouter v2.1
    },
    "arbitrum": {
        "factory": "0x8e42f2F4101563bF679975178e880FD87d3eFd4e",
        "router": "0xb4315e873dBcf96Ffd0acd8EA43f689D8c20fB30",  # LBRouter v2.1 (CREATE2 — same address)
    },
    "bsc": {
        "factory": "0x8e42f2F4101563bF679975178e880FD87d3eFd4e",
        "router": "0xb4315e873dBcf96Ffd0acd8EA43f689D8c20fB30",  # LBRouter v2.1 (CREATE2 — same address)
    },
    "ethereum": {
        "factory": "0xDC8d77b69155c7E68A95a4fb0f06a71FF90B943a",
        "router": "0x9A93a421b74F1c5755b83dD2C211614dC419C44b",  # LBRouter v2.1
    },
}

TRADERJOE_V2_TOKENS: dict[str, dict[str, str]] = {
    "avalanche": {
        "AVAX": "0xEeeeeEeeeEeEeeEeEeEeeEEEeeeeEeeeeeeeEEeE",  # Native
        "WAVAX": "0xB31f66AA3C1e785363F0875A1B74E27b85FD66c7",
        "USDC": "0xB97EF9Ef8734C71904D8002F8b6Bc66Dd9c48a6E",
        "USDT": "0x9702230A8Ea53601f5cD2dc00fDBc13d4dF4A8c7",
        "JOE": "0x6e84a6216eA6dACC71eE8E6b0a5B7322EEbC0fDd",
        "WETH.e": "0x49D5c2BdFfac6CE2BFdB6640F4F80f226bc10bAB",
        "BTC.b": "0x152b9d0FdC40C096757F570A51E494bd4b943E50",
    },
    "arbitrum": {
        "ETH": "0xEeeeeEeeeEeEeeEeEeEeeEEEeeeeEeeeeeeeEEeE",
        "WETH": "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1",
        "USDC": "0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
        "USDT": "0xFd086bC7CD5C481DCC9C85ebE478A1C0b69FCbb9",
    },
    "bsc": {
        "BNB": "0xEeeeeEeeeEeEeeEeEeEeeEEEeeeeEeeeeeeeEEeE",
        "WBNB": "0xbb4CdB9CBd36B01bD1cBaEBF2De08d9173bc095c",
        "USDC": "0x8AC76a51cc950d9822D68b83fE1Ad97B32Cd580d",
        "USDT": "0x55d398326f99059fF775485246999027B3197955",
    },
    "ethereum": {
        "ETH": "0xEeeeeEeeeEeEeeEeEeEeeEEEeeeeEeeeeeeeEEeE",
        "WETH": "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2",
        "USDC": "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
        "USDT": "0xdAC17F958D2ee523a2206206994597C13D831ec7",
    },
}

# =============================================================================
# Morpho Blue
# =============================================================================

# Morpho Blue singleton contract address (same on all supported chains)
MORPHO_BLUE_ADDRESS = "0xBBBBBbbBBb9cC5e90e3b3Af64bdAF62C37EEFFCb"

MORPHO_BLUE: dict[str, dict[str, str]] = {
    "ethereum": {
        "morpho": MORPHO_BLUE_ADDRESS,
        "bundler": "0x4095F064B8d3c3548A3bebfd0Bbfd04750E30077",
    },
    "base": {
        "morpho": MORPHO_BLUE_ADDRESS,
        "bundler": "0x23055618898e202386e6c13955a58D3C68200BFB",
    },
}

MORPHO_BLUE_TOKENS: dict[str, dict[str, str]] = {
    "ethereum": {
        "WETH": "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2",
        "USDC": "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
        "USDT": "0xdAC17F958D2ee523a2206206994597C13D831ec7",
        "DAI": "0x6B175474E89094C44Da98b954EedeAC495271d0F",
        "WBTC": "0x2260FAC5E5542a773Aa44fBCfeDf7C193bc2C599",
        "wstETH": "0x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0",
        "cbETH": "0xBe9895146f7AF43049ca1c1AE358B0541Ea49704",
        "sDAI": "0x83F20F44975D03b1b09e64809B757c47f942BEeA",
        "MORPHO": "0x9994E35Db50125E0DF82e4c2dde62496CE330999",
        "USDe": "0x4c9EDD5852cd905f086C759E8383e09bff1E68B3",
        "sUSDe": "0x9D39A5DE30e57443BfF2A8307A4256c8797A3497",
        "weETH": "0xCd5fE23C85820F7B72D0926FC9b05b43E359b7ee",
        "ezETH": "0xbf5495Efe5DB9ce00f80364C8B423567e58d2110",
    },
    "base": {
        "WETH": "0x4200000000000000000000000000000000000006",
        "USDC": "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913",
        "USDbC": "0xd9aAEc86B65D86f6A7B5B1b0c42FFA531710b6CA",
        "cbETH": "0x2Ae3F1Ec7F1F5012CFEab0185bfc7aa3cf0DEc22",
        "wstETH": "0xc1CBa3fCea344f92D9239c08C0568f6F2F0ee452",
    },
}

# =============================================================================
# Pendle
# =============================================================================

PENDLE: dict[str, dict[str, str]] = {
    "arbitrum": {
        # Core contracts
        "router": "0x888888888889758F76e7103c6CbF23ABbF58F946",
        "router_static": "0x263833d47eA3fA4a30f269323aba6a107f9eB14C",
        "market_factory": "0x2FCb47B58350cD377f94d3821e7373Df60bD9Ced",
        "yt_factory": "0x28d4cE244fCE6f26C6A4A0447fFe8A4ccf9F1CcC",
        "pt_oracle": "0x1Fd95db7B7C0067De8D45C0cb35D59796adfD187",
        # Popular markets
        "market_wsteth_26dec2024": "0xf769035a247af48bf55BaA82d8b5e14E02E49A25",
        "market_wsteth_26jun2025": "0x08a152834de126d2ef83D612ff36e4523FD0017F",  # Expired
        "market_wsteth_active": "0xf78452e0f5C0B95fc5dC8353B8CD1e06E53fa25B",
        "market_eeth_26dec2024": "0x952083cde7aaa11AB8449057F7de23A970AA8472",
        "market_rseth_26dec2024": "0x6ae79089b2CF4be441480801F9f1CA1a54e3ce9C",
    },
    "ethereum": {
        "router": "0x888888888889758F76e7103c6CbF23ABbF58F946",
        "router_static": "0x263833d47eA3fA4a30f269323aba6a107f9eB14C",
        "market_factory": "0x1A6fCc85557BC4fB7B534ed835a03EF056552D52",
        "yt_factory": "0xeA1CE3Fd2da6C6BD47C227526be5e54e4E12fE00",
        "pt_oracle": "0x66a1096C6366b2529274dF4f5D8247827fe4CEA8",
    },
    "plasma": {
        "router": "0x888888888889758F76e7103c6CbF23ABbF58F946",
        # fUSDT0 market (Fluid) - expires 26 Feb 2026
        "market_fusdt0_26feb2026": "0x0cb289E9df2d0dCFe13732638C89655fb80C2bE2",
        "pt_fusdt0_26feb2026": "0xbE45F6F17b81571fC30253BDaE0A2A6f7b04D60F",
        "yt_fusdt0_26feb2026": "0xC0f6a41a9837C4d824Bc8d346341DB77e634ae69",
        "sy_fusdt0": "0xfF3CCC1245D59B21B6EC4A597557E748f8311E8c",
    },
    "sonic": {
        "router": "0x888888888889758F76e7103c6CbF23ABbF58F946",
        "router_static": "0x0013ACc071f732fd6BF8210AB46A3794a7D8945e",
        "market_factory": "0x0AB3ae25c42a2f3748a018556989355D568Fa6d6",  # V6
    },
    "base": {
        "router": "0x888888888889758F76e7103c6CbF23ABbF58F946",
        "router_static": "0xB4205a645c7e920BD8504181B1D7f2c5C955C3e7",
        "market_factory": "0x81E80A50E56d10C501fF17B5Fe2F662bd9EA4590",  # V6
    },
    "mantle": {
        "router": "0x888888888889758F76e7103c6CbF23ABbF58F946",
        "router_static": "0xCAd502Bb55d1A3F79952F969BFF3f011CF30a94a",
        "market_factory": "0xa35AE21a593CB06959978E20b33Db34163166C79",  # V6
    },
    "bsc": {
        "router": "0x888888888889758F76e7103c6CbF23ABbF58F946",
        "router_static": "0x2700ADB035F82a11899ce1D3f1BF8451c296eABb",
        "market_factory": "0x80cE46449DF1c977f6ba60495125ce282F83DdFB",  # V6
    },
}

PENDLE_TOKENS: dict[str, dict[str, str]] = {
    "arbitrum": {
        "WETH": "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1",
        "WSTETH": "0x5979D7b546E38E414F7E9822514be443A4800529",
        "USDC": "0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
        "USDT": "0xFd086bC7CD5C481DCC9C85ebE478A1C0b69FCbb9",
        "PENDLE": "0x0c880f6761F1af8d9Aa9C466984b80DAb9a8c9e8",
    },
    "ethereum": {
        "WETH": "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2",
        "WSTETH": "0x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0",
        "USDC": "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
        "PENDLE": "0x808507121B80c02388fAd14726482e061B8da827",
    },
    "plasma": {
        "USDT0": "0xB8CE59FC3717ada4C02eaDF9682A9e934F625ebb",
        "FUSDT0": "0x1DD4b13fcAE900C60a350589BE8052959D2Ed27B",
        "PENDLE": "0x17Bac5F906c9A0282aC06a59958D85796c831f24",
        "WXPL": "0x6100E367285b01F48D07953803A2d8dCA5D19873",
    },
    "sonic": {
        "wS": "0x039e2fB66102314Ce7b64Ce5Ce3E5183bc94aD38",
        "WETH": "0x50c42dEAcD8Fc9773493ED674b675bE577f2634b",
        "USDC": "0x29219dd400f2Bf60E5a23d13Be72B486D4038894",
    },
    "base": {
        "WETH": "0x4200000000000000000000000000000000000006",
        "USDC": "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913",
    },
    "mantle": {
        "WMNT": "0x78c1b0C915c4FAA5FffA6CAbf0219DA63d7f4cb8",
        "WETH": "0xdEAddEaDdeadDEadDEADDEAddEADDEAddead1111",
        "USDC": "0x09Bc4E0D864854c6aFB6eB9A9cdF58aC190D0dF9",
    },
    "bsc": {
        "WBNB": "0xbb4CdB9CBd36B01bD1cBaEBF2De08d9173bc095c",
        "USDC": "0x8AC76a51cc950d9822D68b83fE1Ad97B32Cd580d",
        "USDT": "0x55d398326f99059fF775485246999027B3197955",
    },
}


# =============================================================================
# MetaMorpho Vaults
# =============================================================================

# Notable MetaMorpho vault addresses for reference and testing
METAMORPHO_VAULTS: dict[str, dict[str, str]] = {
    "ethereum": {
        "steakhouse_usdc": "0xBEEF01735c132Ada46AA9aA4c54623cAA92A64CB",  # Steakhouse USDC
        "flagship_eth": "0x38989BBA00BDF8181F4082995b3DEAe96163aC5D",  # Flagship ETH (wETH)
        "flagship_usdc": "0x186514400e52270cef3D80e1c6F8d10A75d47344",  # Flagship USDC
        "gauntlet_usdc_core": "0x8eB67A509616cd6A7c1B3c8C21D48FF57df3d458",  # Gauntlet USDC Core
        "re7_weth": "0x78FC2c2ed71dAb0491d268b4E2B4A14CaE2a5c90",  # Re7 WETH
        "gauntlet_weth_prime": "0x4881Ef0BF6d2365D3dd6499ccd7532bcdBCE0658",  # Gauntlet WETH Prime
    },
    "base": {
        "moonwell_flagship_usdc": "0xc1256Ae5FF1cf2719D4937adb3bbCCab2E00A2Ca",  # Moonwell Flagship USDC
        "moonwell_flagship_eth": "0xa0E430870C4604Ccfc7b38ca7845b1F882eb5860",  # Moonwell Flagship ETH
    },
}


# =============================================================================
# Helper
# =============================================================================


def get_address(protocol_addresses: dict[str, dict[str, str]], chain: str, contract_type: str) -> str:
    """Lookup a contract address with clear error messages.

    Args:
        protocol_addresses: Protocol address dict (e.g., UNISWAP_V3, AAVE_V3)
        chain: Chain name (e.g., "arbitrum", "ethereum")
        contract_type: Contract type key (e.g., "swap_router", "pool")

    Returns:
        The contract address string.

    Raises:
        KeyError: If chain or contract_type not found.
    """
    # Normalize chain alias (e.g., "bnb" -> "bsc") via central resolver
    try:
        from almanak.core.constants import resolve_chain_name

        chain_lower = resolve_chain_name(chain)
    except (ValueError, ImportError):
        chain_lower = chain.lower()
    if chain_lower not in protocol_addresses:
        available = ", ".join(sorted(protocol_addresses.keys()))
        raise KeyError(f"Chain '{chain}' not found. Available chains: {available}")
    chain_addresses = protocol_addresses[chain_lower]
    if contract_type not in chain_addresses:
        available = ", ".join(sorted(chain_addresses.keys()))
        raise KeyError(f"Contract type '{contract_type}' not found for chain '{chain}'. Available: {available}")
    return chain_addresses[contract_type]
