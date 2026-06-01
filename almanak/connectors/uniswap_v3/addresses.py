"""Uniswap V3 contract addresses per chain.

Single source of truth for this connector's on-chain addresses. Replaces
the entries previously held in ``almanak.core.contracts`` (W1 / VIB-4853
/ epic VIB-4851). Surfaced to non-connector callers through
:class:`GatewayAddressCapability` on
``UniswapV3GatewayConnector``; strategy-side connector code reads the
dicts directly.

Each per-chain mapping uses the connector's internal contract-kind
vocabulary (``swap_router`` / ``swap_router_02`` / ``factory`` /
``position_manager`` / ``quoter_v2``). The vocabulary is connector-
private — callers outside this folder should consume the registry, not
guess key names.
"""

from __future__ import annotations

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
    "monad": {
        # Verified mainnet (chain 143) — https://docs.uniswap.org/contracts/v3/reference/deployments/monad-deployments
        "swap_router": "0xfE31F71C1b106EAc32F1A19239c9a9A72ddfb900",  # SwapRouter02
        "swap_router_02": "0xfE31F71C1b106EAc32F1A19239c9a9A72ddfb900",
        "factory": "0x204FAca1764B154221e35c0d20aBb3c525710498",
        "position_manager": "0x7197E214c0b767cFB76Fb734ab638E2c192F4E53",
        "quoter_v2": "0x661E93cca42AfacB172121EF892830cA3b70F08d",
    },
    "mantle": {
        # Governance-deployed mainnet (chain 5000) — non-canonical addresses.
        # Source: https://gov.uniswap.org/t/official-uniswap-v3-deployments-list/24323
        # Coexists with Agni Finance (Uniswap V3 fork — see agni_finance/addresses.py).
        "swap_router": "0x738fD6d10bCc05c230388B4027CAd37f82fe2AF2",  # SwapRouter02
        "swap_router_02": "0x738fD6d10bCc05c230388B4027CAd37f82fe2AF2",
        "factory": "0x0d922Fb1Bc191F64970ac40376643808b4B74Df9",
        "position_manager": "0x5911cB3633e764939edc2d92b7e1ad375Bb57649",
        "quoter_v2": "0xdD489C75be1039ec7d843A6aC2Fd658350B067Cf",
    },
    "xlayer": {
        # Non-canonical deployment via Uniswap Governance Proposal 67
        # Source: https://github.com/Uniswap/sdks (XLAYER_ADDRESSES)
        "swap_router": "0x4f0C28f5926AFDA16bf2506D5D9e57Ea190f9bcA",  # SwapRouter02
        "swap_router_02": "0x4f0C28f5926AFDA16bf2506D5D9e57Ea190f9bcA",
        "factory": "0x4B2ab38DBF28D31D467aA8993f6c2585981D6804",
        "position_manager": "0x315e413A11AB0df498eF83873012430ca36638Ae",
        "quoter_v2": "0x976183AC3d09840D243A88c0268BADb3B3e3259f",
    },
    "zerog": {
        # JAINE DEX (Uniswap V3 fork) on 0G Chain.
        # Verified on-chain 2026-04-17: NPM.name() = "Jaine V3 Positions NFT",
        # and Router/NPM/Quoter all return factory() = 0x9bdcA5...7ef4 and WETH9() = W0G.
        # Source: Jaine UI JS bundle (jaine.app) + on-chain verification via 0G RPC.
        "swap_router": "0x8B598A7C136215A95ba0282b4d832B9f9801f2e2",
        "swap_router_02": "0x8B598A7C136215A95ba0282b4d832B9f9801f2e2",
        "factory": "0x9bdcA5798E52e592A08e3b34d3F18EeF76Af7ef4",
        "position_manager": "0x8F67A30Ed186e3E1f6504c6dE3239Ef43A2e0d72",
        "quoter_v2": "0xd00883722cECAD3A1c60bCA611f09e1851a0bE02",
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
        "WETH": "0xdEAddEaDdeadDEadDEADDEAddEADDEAddead1111",  # Canonical Mantle Bridged WETH (deterministic bridge address, not a placeholder)
        "USDC": "0x09Bc4E0D864854c6aFB6eB9A9cdF58aC190D0dF9",
        "USDT": "0x201EBa5CC46D216Ce6DC03F6a759e8E766e956aE",
    },
    "monad": {
        # Verified mainnet (chain 143) — https://docs.monad.xyz/developer-essentials/network-information/tokens-and-bridges
        "MON": "0xEeeeeEeeeEeEeeEeEeEeeEEEeeeeEeeeeeeeEEeE",
        "WMON": "0x3bd359C1119dA7Da1D913D1C4D2B7c461115433A",
        "WETH": "0xEE8c0E9f1BFFb4Eb878d8f15f368A02a35481242",  # WETH on Monad
        "USDC": "0x754704Bc059F8C67012fEd69BC8A327a5aafb603",
        "USDT0": "0xe7cd86e13AC4309349F30B3435a9d337750fC82D",
        "WBTC": "0x0555E30da8f98308EdB960aa94C0Db47230d2B9c",
    },
    "xlayer": {
        # Source: https://web3.okx.com/xlayer/docs/developer/build-on-xlayer/contracts
        "OKB": "0xEeeeeEeeeEeEeeEeEeEeeEEEeeeeEeeeeeeeEEeE",
        "WOKB": "0xe538905cf8410324e03A5A23C1c177a474D59b2b",
        "WETH": "0x5A77f1443D16ee5761d310e38b62f77f726bC71c",
        "USDC": "0x74b7F16337b8972027F6196A17a631aC6dE26d22",
        "USDT": "0x779Ded0c9e1022225f8E0630b35a9b54bE713736",  # USD₮0 (Aave V3.6 reserve)
        "WBTC": "0xEA034fb02eB1808C2cc3adbC15f447B93CbE08e1",
    },
    "zerog": {
        # 0G Chain tokens (verified on-chain)
        "A0GI": "0xEeeeeEeeeEeEeeEeEeEeeEEEeeeeEeeeeeeeEEeE",
        "W0G": "0x1Cd0690fF9a693f5EF2dD976660a8dAFc81A109c",
    },
}


# =============================================================================
# Agni Finance (Uniswap V3 fork on Mantle)
# =============================================================================
#
# Agni Finance is a Uniswap V3 fork that reuses ``UniswapV3ReceiptParser`` and
# rides on top of the Uniswap V3 connector — there is no separate
# ``almanak/connectors/agni_finance/`` folder. The addresses live alongside
# their parent so the receipt-parser and pool-validation code can read both
# from the same connector module.

AGNI_FINANCE: dict[str, dict[str, str]] = {
    "mantle": {
        "swap_router": "0x319B69888b0d11cEC22caA5034e25FfFBDc88421",  # Agni SwapRouter (V1 style, with deadline)
        "swap_router_02": "0x319B69888b0d11cEC22caA5034e25FfFBDc88421",
        "factory": "0x25780dc8Fc3cfBD75F33bFDAB65e969b603b2035",  # Agni Factory
        "position_manager": "0x218bf598D1453383e2F4AA7b14fFB9BfB102D637",  # Agni NonfungiblePositionManager
        "quoter_v2": "0xc4aaDc921E1cdb66c5300Bc158a313292923C0cb",  # Agni QuoterV2
    },
}


__all__ = ["UNISWAP_V3", "UNISWAP_V3_TOKENS", "AGNI_FINANCE"]
