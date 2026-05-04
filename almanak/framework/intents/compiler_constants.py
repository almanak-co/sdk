"""Compiler constants — protocol addresses, gas estimates, and selectors.

These are extracted from compiler.py for file-size management.
All symbols remain importable from ``almanak.framework.intents.compiler``.
"""

# =============================================================================
# Constants
# =============================================================================

# Default gas estimates per operation type (used as fallback for all chains)
# Note: approve is set high (80K) to handle proxy contracts like Avalanche native USDC
DEFAULT_GAS_ESTIMATES: dict[str, int] = {
    "approve": 80000,
    "swap_simple": 200000,  # Increased from 120k - USDC proxy contracts need ~180k+
    "swap_multi_hop": 350000,  # Increased from 200k - Arbitrum swaps use more gas
    "wrap_eth": 30000,
    "unwrap_eth": 30000,
    # LP operations
    "lp_mint": 500000,  # Uniswap V3 mint new position (wide ranges need more gas)
    "lp_increase_liquidity": 200000,  # Add liquidity to existing position
    "lp_decrease_liquidity": 250000,  # Remove liquidity from position (extra buffer for Arbitrum)
    "lp_collect": 200000,  # Collect fees/tokens (buffer for fee growth updates)
    "lp_burn": 100000,  # Burn position NFT (if fully withdrawn)
    # Lending operations (Aave V3 on Arbitrum uses ~220k+ for supply due to hooks/incentives)
    "lending_supply": 300000,  # Supply collateral to lending protocol
    "lending_borrow": 450000,  # Borrow tokens from lending protocol (Aave needs ~310k+)
    "lending_repay": 250000,  # Repay borrowed tokens
    "lending_withdraw": 250000,  # Withdraw supplied collateral
    # Flash loan operations (Aave)
    "flash_loan": 500000,  # Multi-asset flash loan base gas
    "flash_loan_simple": 300000,  # Single-asset flash loan base gas
    # Flash loan operations (Balancer)
    "balancer_flash_loan": 400000,  # Balancer multi-token flash loan base gas
    "balancer_flash_loan_simple": 250000,  # Balancer single-token flash loan base gas
    "bridge_deposit": 800000,  # Cross-chain bridge deposit tx (quote-dependent, Across can exceed 675K)
    # MetaMorpho vault operations (ERC-4626)
    "vault_deposit": 200000,  # MetaMorpho deposit (approve handled separately)
    "vault_redeem": 250000,  # MetaMorpho redeem (multi-market withdrawal)
}

# Chain-specific gas overrides for operations that need different estimates
# Ethereum mainnet has proxy tokens (USDC, USDT) requiring extra delegatecall gas
CHAIN_GAS_OVERRIDES: dict[str, dict[str, int]] = {
    "ethereum": {
        "swap_simple": 180000,  # Proxy tokens like USDC need ~150k+, add buffer
        "swap_multi_hop": 300000,
    },
    "avalanche": {
        "swap_simple": 180000,  # Native USDC is also a proxy
    },
    "bsc": {
        "lp_decrease_liquidity": 400000,  # BNB Uniswap V3 uses more gas for LP ops
        "lp_collect": 300000,
        "lp_burn": 150000,
    },
    "mantle": {
        # Mantle gas units are ~2000x higher than L1 equivalents (a Uniswap V3 swap
        # uses ~150k on L1 but ~340M on Mantle). Gas prices are proportionally lower
        # (~0.02 Gwei), so actual cost in MNT is comparable to other L2s (~$0.006/swap).
        # Fallback values when simulation (Tenderly/Alchemy) is unavailable.
        # Measured via cast estimate: USDC approve ~203M, wrap ~118M, unwrap ~146M.
        "approve": 250_000_000,
        "swap_simple": 500_000_000,
        "swap_multi_hop": 800_000_000,
        "wrap_eth": 200_000_000,
        "unwrap_eth": 200_000_000,
        "lp_mint": 1_000_000_000,
        "lp_increase_liquidity": 400_000_000,
        "lp_decrease_liquidity": 500_000_000,
        "lp_collect": 400_000_000,
        "lp_burn": 200_000_000,
        "lending_supply": 600_000_000,
        "lending_borrow": 900_000_000,
        "vault_deposit": 400_000_000,
    },
}


def get_gas_estimate(chain: str, operation: str) -> int:
    """Get gas estimate for an operation, with chain-specific overrides.

    Args:
        chain: Target blockchain (ethereum, arbitrum, bsc, etc.)
        operation: Operation type (swap_simple, approve, etc.)

    Returns:
        Gas estimate in units
    """
    # Normalize chain name (e.g., "bnb" -> "bsc")
    try:
        from almanak.core.constants import resolve_chain_name

        chain = resolve_chain_name(chain)
    except (ValueError, ImportError):
        pass

    # Check chain-specific override first
    if chain in CHAIN_GAS_OVERRIDES:
        if operation in CHAIN_GAS_OVERRIDES[chain]:
            return CHAIN_GAS_OVERRIDES[chain][operation]

    # Fall back to default
    return DEFAULT_GAS_ESTIMATES.get(operation, 120000)


# Protocol router addresses per chain
# Note: Using SwapRouter02 for Uniswap V3 (7-param struct, no deadline)
PROTOCOL_ROUTERS: dict[str, dict[str, str]] = {
    "ethereum": {
        "uniswap_v3": "0x68b3465833fb72A70ecDF485E0e4C7bD8665Fc45",  # SwapRouter02
        "uniswap_v2": "0x7a250d5630B4cF539739dF2C5dAcb4c659F2488D",
        "sushiswap_v3": "0x2E6cd2d30aa43f40aa81619ff4b6E0a41479B13F",  # SushiSwap V3 SwapRouter
        "pancakeswap_v3": "0x13f4EA83D0bd40E75C8222255bc855a974568Dd4",  # SmartRouter (7-param)
        # traderjoe_v2: uses dedicated _compile_swap_traderjoe_v2() (VIB-1928), not DefaultSwapAdapter
        "1inch": "0x1111111254EEB25477B68fb85Ed929f73A960582",
    },
    "arbitrum": {
        "uniswap_v3": "0x68b3465833fb72A70ecDF485E0e4C7bD8665Fc45",  # SwapRouter02
        "sushiswap_v3": "0x8A21F6768C1f8075791D08546Dadf6daA0bE820c",  # SushiSwap V3 SwapRouter
        "pancakeswap_v3": "0x32226588378236Fd0c7c4053999F88aC0e5cAc77",  # SmartRouter (7-param)
        # traderjoe_v2: uses dedicated _compile_swap_traderjoe_v2() (VIB-1928), not DefaultSwapAdapter
        "sushiswap": "0x1b02dA8Cb0d097eB8D57A175b88c7D8b47997506",
        "camelot": "0x1F721E2E82F6676FCE4eA07A5958cF098D339e18",  # Algebra V3 SwapRouter (VIB-1636)
        "1inch": "0x1111111254EEB25477B68fb85Ed929f73A960582",
    },
    "optimism": {
        "uniswap_v3": "0x68b3465833fb72A70ecDF485E0e4C7bD8665Fc45",  # SwapRouter02
        "sushiswap_v3": "0x8516944E89f296eb6473d79aED1Ba12088016c9e",  # SushiSwap V3 SwapRouter
        "velodrome": "0xa062aE8A9c5e11aaA026fc2670B0D65cCc8B2858",
        "1inch": "0x1111111254EEB25477B68fb85Ed929f73A960582",
    },
    "polygon": {
        "uniswap_v3": "0x68b3465833fb72A70ecDF485E0e4C7bD8665Fc45",  # SwapRouter02
        "sushiswap_v3": "0x0aF89E1620b96170e2a9D0b68fEebb767eD044c3",  # SushiSwap V3 SwapRouter
        "quickswap": "0xa5E0829CaCEd8fFDD4De3c43696c57F7D7A678ff",
        "1inch": "0x1111111254EEB25477B68fb85Ed929f73A960582",
    },
    "base": {
        "uniswap_v3": "0x2626664c2603336E57B271c5C0b26F421741e481",
        "sushiswap_v3": "0xfB7ef66A7e61fF9e400671e4b5BFbaBE2ea025B4",  # SushiSwap V3 SwapRouter
        "aerodrome": "0xcF77a3Ba9A5CA399B7c97c74d54e5b1Beb874E43",
        "pancakeswap_v3": "0x678Aa4bF4E210cf2166753e054d5b7c31cc7fa86",  # SmartRouter (7-param)
    },
    "avalanche": {
        # traderjoe_v2: uses dedicated _compile_swap_traderjoe_v2() (VIB-1928), not DefaultSwapAdapter
        "uniswap_v3": "0xbb00FF08d01D300023C629E8fFfFcb65A5a578cE",  # SwapRouter02
        # sushiswap_v3 removed: ~54% price impact on $100, on-chain reverts on $10 (VIB-2069)
    },
    "bsc": {
        "pancakeswap_v3": "0x13f4EA83D0bd40E75C8222255bc855a974568Dd4",  # SmartRouter (7-param)
        "pancakeswap_v2": "0x10ED43C718714eb63d5aA57B78B54704E256024E",
        "uniswap_v3": "0xB971eF87ede563556b2ED4b1C0b0019111Dd85d2",  # SwapRouter02
        "sushiswap_v3": "0xB45e53277a7e0F1D35f2a77160e91e25507f1763",  # SushiSwap V3 SwapRouter
        # traderjoe_v2: uses dedicated _compile_swap_traderjoe_v2() (VIB-1928), not DefaultSwapAdapter
        "sushiswap": "0x1b02dA8Cb0d097eB8D57A175b88c7D8b47997506",
    },
    "linea": {
        "uniswap_v3": "0x3d4e44Eb1374240CE5F1B871ab261CD16335B76a",  # SwapRouter02
        "pancakeswap_v3": "0x678Aa4bF4E210cf2166753e054d5b7c31cc7fa86",  # SmartRouter
    },
    "mantle": {
        "agni_finance": "0x319B69888b0d11cEC22caA5034e25FfFBDc88421",  # Agni Finance SwapRouter
    },
    "xlayer": {
        "uniswap_v3": "0x4f0C28f5926AFDA16bf2506D5D9e57Ea190f9bcA",  # SwapRouter02 (Governance Proposal 67)
    },
    "monad": {
        "uniswap_v3": "0xfE31F71C1b106EAc32F1A19239c9a9A72ddfb900",  # SwapRouter02 — https://docs.uniswap.org/contracts/v3/reference/deployments/monad-deployments
    },
    "zerog": {
        "uniswap_v3": "0x8B598A7C136215A95ba0282b4d832B9f9801f2e2",  # JAINE DEX SwapRouter02 (Uniswap V3 fork)
    },
}

# Uniswap V3 NonfungiblePositionManager addresses per chain
LP_POSITION_MANAGERS: dict[str, dict[str, str]] = {
    "ethereum": {
        "uniswap_v3": "0xC36442b4a4522E871399CD717aBDD847Ab11FE88",
        "uniswap_v4": "0xBd216513D74C8cf14cF4747E6AaE6fDf64e83b24",  # V4 PositionManager
        "sushiswap_v3": "0x2214A42d8e2A1d20635c2cb0664422c528B6A432",
        "pancakeswap_v3": "0x46A15B0b27311cedF172AB29E4f4766fbE7F4364",
        "traderjoe_v2": "0x9A93a421b74F1c5755b83dD2C211614dC419C44b",  # LBRouter v2.1
    },
    "arbitrum": {
        "uniswap_v3": "0xC36442b4a4522E871399CD717aBDD847Ab11FE88",
        "uniswap_v4": "0xBd216513D74C8cf14cF4747E6AaE6fDf64e83b24",  # V4 PositionManager
        "sushiswap_v3": "0xF0cBce1942A68BEB3d1b73F0dd86C8DCc363eF49",
        "camelot": "0x00c7f3082833e796A5b3e4Bd59f6642FF44DCD15",
        "pancakeswap_v3": "0x46A15B0b27311cedF172AB29E4f4766fbE7F4364",
        "traderjoe_v2": "0xb4315e873dBcf96Ffd0acd8EA43f689D8c20fB30",  # LBRouter v2.1
        "fluid": "0x91716C4EDA1Fb55e84Bf8b4c7085f84285c19085",  # Fluid DexFactory (pools resolved dynamically)
    },
    "optimism": {
        "uniswap_v3": "0xC36442b4a4522E871399CD717aBDD847Ab11FE88",
        "uniswap_v4": "0xBd216513D74C8cf14cF4747E6AaE6fDf64e83b24",  # V4 PositionManager
        "sushiswap_v3": "0x1af415a1EbA07a4986a52B6f2e7dE7003D82231e",
        # Velodrome V2 uses the Router for liquidity operations (fungible LP tokens, same as Aerodrome)
        "aerodrome": "0xa062aE8A9c5e11aaA026fc2670B0D65cCc8B2858",  # Velodrome V2 Router
    },
    "polygon": {
        "uniswap_v3": "0xC36442b4a4522E871399CD717aBDD847Ab11FE88",
        "uniswap_v4": "0xBd216513D74C8cf14cF4747E6AaE6fDf64e83b24",  # V4 PositionManager
        "sushiswap_v3": "0xb7402ee99F0A008e461098AC3A27F4957Df89a40",
    },
    "base": {
        "uniswap_v3": "0x03a520b32C04BF3bEEf7BEb72E919cf822Ed34f1",
        "uniswap_v4": "0xBd216513D74C8cf14cF4747E6AaE6fDf64e83b24",  # V4 PositionManager
        "sushiswap_v3": "0x80C7DD17B01855a6D2347444a0FCC36136a314de",
        # Aerodrome uses the Router for liquidity operations (fungible LP tokens)
        "aerodrome": "0xcF77a3Ba9A5CA399B7c97c74d54e5b1Beb874E43",  # Aerodrome Router
        "aerodrome_slipstream": "0x827922686190790b37229fd06084350E74485b72",  # Slipstream NonfungiblePositionManager
        "pancakeswap_v3": "0x46A15B0b27311cedF172AB29E4f4766fbE7F4364",
    },
    "avalanche": {
        "uniswap_v3": "0x655C406EBFa14EE2006250925e54ec43AD184f8B",
        "uniswap_v4": "0xBd216513D74C8cf14cF4747E6AaE6fDf64e83b24",  # V4 PositionManager
        # sushiswap_v3 removed: zero usable liquidity (VIB-2069)
        # TraderJoe V2 uses the LBRouter for liquidity operations (not NFT-based)
        "traderjoe_v2": "0xb4315e873dBcf96Ffd0acd8EA43f689D8c20fB30",  # LBRouter2
    },
    "bsc": {
        "uniswap_v3": "0x7b8A01B39D58278b5DE7e48c8449c9f4F5170613",
        "uniswap_v4": "0xBd216513D74C8cf14cF4747E6AaE6fDf64e83b24",  # V4 PositionManager
        "sushiswap_v3": "0xF70c086618dcf2b1A461311275e00D6B722ef914",
        "pancakeswap_v3": "0x46A15B0b27311cedF172AB29E4f4766fbE7F4364",
        "traderjoe_v2": "0xb4315e873dBcf96Ffd0acd8EA43f689D8c20fB30",  # LBRouter v2.1
    },
    "linea": {
        "uniswap_v3": "0x4615C383F85D0a2BbED973d83ccecf5CB7121463",
        "pancakeswap_v3": "0x46A15B0b27311cedF172AB29E4f4766fbE7F4364",
    },
    "mantle": {
        "agni_finance": "0x218bf598D1453383e2F4AA7b14fFB9BfB102D637",  # Agni Finance NFT Position Manager
    },
    "xlayer": {
        "uniswap_v3": "0x315e413A11AB0df498eF83873012430ca36638Ae",  # Non-canonical deployment (Governance Proposal 67)
    },
    "monad": {
        "uniswap_v3": "0x7197E214c0b767cFB76Fb734ab638E2c192F4E53",  # NonfungiblePositionManager — https://docs.uniswap.org/contracts/v3/reference/deployments/monad-deployments
    },
    "zerog": {
        "uniswap_v3": "0x8F67A30Ed186e3E1f6504c6dE3239Ef43A2e0d72",  # JAINE DEX NonfungiblePositionManager (Uniswap V3 fork)
    },
}

# Chain-specific token addresses for fee tier selection in swaps
# Used by DefaultSwapAdapter to determine optimal fee tiers for common pairs
CHAIN_TOKENS: dict[str, dict[str, str]] = {
    "ethereum": {
        "usdc": "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
        "usdt": "0xdAC17F958D2ee523a2206206994597C13D831ec7",
        "weth": "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2",
        "wbtc": "0x2260FAC5E5542a773Aa44fBCfeDf7C193bc2C599",
        "dai": "0x6B175474E89094C44Da98b954EedeAC495271d0F",
    },
    "arbitrum": {
        "usdc": "0xaf88d065e77c8cC2239327C5EDb3A432268e5831",  # Native USDC
        "usdc_bridged": "0xFF970A61A04b1cA14834A43f5dE4533eBDDB5CC8",  # USDC.e
        "usdt": "0xFd086bC7CD5C481DCC9C85ebE478A1C0b69FCbb9",
        "weth": "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1",
        "wbtc": "0x2f2a2543B76A4166549F7aaB2e75Bef0aefC5B0f",
    },
    "optimism": {
        "usdc": "0x0b2C639c533813f4Aa9D7837CAf62653d097Ff85",
        "usdt": "0x94b008aA00579c1307B0EF2c499aD98a8ce58e58",
        "weth": "0x4200000000000000000000000000000000000006",
    },
    "polygon": {
        "usdc": "0x3c499c542cEF5E3811e1192ce70d8cC03d5c3359",
        "usdt": "0xc2132D05D31c914a87C6611C10748AEb04B58e8F",
        "weth": "0x7ceB23fD6bC0adD59E62ac25578270cFf1b9f619",
    },
    "base": {
        "usdc": "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913",
        "weth": "0x4200000000000000000000000000000000000006",
    },
    "avalanche": {
        "usdc": "0xB97EF9Ef8734C71904D8002F8b6Bc66Dd9c48a6E",
        "usdt": "0x9702230A8Ea53601f5cD2dc00fDBc13d4dF4A8c7",
        "wavax": "0xB31f66AA3C1e785363F0875A1B74E27b85FD66c7",
    },
    "bsc": {
        "usdc": "0x8AC76a51cc950d9822D68b83fE1Ad97B32Cd580d",
        "usdt": "0x55d398326f99059fF775485246999027B3197955",
        "wbnb": "0xbb4CdB9CBd36B01bD1cBaEBF2De08d9173bc095c",
        "weth": "0x2170Ed0880ac9A755fd29B2688956BD959F933F8",
    },
    "linea": {
        "usdc": "0x176211869cA2b568f2A7D4EE941E073a821EE1ff",
        "usdt": "0xA219439258ca9da29E9Cc4cE5596924745e12B93",
        "weth": "0xe5D7C2a44FfDDf6b295A15c148167daaAf5Cf34f",
    },
    "sonic": {
        "usdc": "0x29219dd400f2Bf60E5a23d13Be72B486D4038894",
        "weth": "0x50c42dEAcD8Fc9773493ED674b675bE577f2634b",
        "ws": "0x039e2fB66102314Ce7b64Ce5Ce3E5183bc94aD38",
    },
    "mantle": {
        "usdc": "0x09Bc4E0D864854c6aFB6eB9A9cdF58aC190D0dF9",
        "usdt": "0x201EBa5CC46D216Ce6DC03F6a759e8E766e956aE",
        "weth": "0xdEAddEaDdeadDEadDEADDEAddEADDEAddead1111",
        "wmnt": "0x78c1b0C915c4FAA5FffA6CAbf0219DA63d7f4cb8",
    },
    "xlayer": {
        "usdc": "0x74b7F16337b8972027F6196A17a631aC6dE26d22",
        "usdt": "0x779Ded0c9e1022225f8E0630b35a9b54bE713736",  # USD₮0 (Aave V3.6 reserve)
        "weth": "0x5A77f1443D16ee5761d310e38b62f77f726bC71c",
        "wokb": "0xe538905cf8410324e03A5A23C1c177a474D59b2b",
        "xeth": "0xE7B000003A45145decf8a28FC755aD5eC5EA025A",
        "xbtc": "0xb7C00000bcDEeF966b20B3D884B98E64d2b06b4f",
        "usdg": "0x4ae46a509F6b1D9056937BA4500cb143933D2dc8",
        "usdt0": "0x779Ded0c9e1022225f8E0630b35a9b54bE713736",
    },
    "monad": {
        "usdc": "0x754704Bc059F8C67012fEd69BC8A327a5aafb603",
        "weth": "0xEE8c0E9f1BFFb4Eb878d8f15f368A02a35481242",
        "wmon": "0x3bd359C1119dA7Da1D913D1C4D2B7c461115433A",
        "wbtc": "0x0555E30da8f98308EdB960aa94C0Db47230d2B9c",
    },
}

# Supported fee tiers by protocol for exactInputSingle-style swaps.
SWAP_FEE_TIERS: dict[str, tuple[int, ...]] = {
    "uniswap_v3": (100, 500, 3000, 10000),
    "sushiswap_v3": (100, 500, 3000, 10000),
    "pancakeswap_v3": (100, 500, 2500, 10000),
    "agni_finance": (100, 500, 2500, 3000, 10000),
}

# Chain-specific fee tier overrides. Uniswap V3 forks on some chains support
# additional fee tiers beyond their base protocol definition.
SWAP_FEE_TIERS_CHAIN: dict[tuple[str, str], tuple[int, ...]] = {}

DEFAULT_SWAP_FEE_TIER: dict[str, int] = {
    "uniswap_v3": 3000,
    "sushiswap_v3": 3000,
    "pancakeswap_v3": 2500,
    "agni_finance": 3000,  # Agni Finance on Mantle: heuristic picks 500 for USDC/WETH pairs, 3000 is safer default for others
}

# Protocols using the original SwapRouter interface (8-param exactInputSingle WITH deadline).
# All other protocols use SwapRouter02 interface (7-param WITHOUT deadline).
SWAP_ROUTER_V1_PROTOCOLS: frozenset[str] = frozenset({"sushiswap_v3"})

# Chain-specific overrides: some chains use V3 forks with V1-style routers (e.g., Agni on Mantle).
# Maps chain -> set of protocols that use the V1 router interface on that chain.
SWAP_ROUTER_V1_CHAIN_OVERRIDES: dict[str, frozenset[str]] = {
    "mantle": frozenset({"agni_finance"}),  # Agni Finance uses original SwapRouter (with deadline)
    "zerog": frozenset({"uniswap_v3"}),  # Jaine DEX SwapRouter accepts only the V1 8-arg form
}

# Protocols using the Algebra V1.9 router interface (VIB-1636).
# exactInputSingle((address,address,address,uint256,uint256,uint256,uint160)) -> 0xbc651188
# Struct: tokenIn, tokenOut, recipient, deadline, amountIn, amountOutMinimum, limitSqrtPrice
# NOTE: Algebra has no `fee` parameter — fees are determined dynamically by the pool.
SWAP_ROUTER_ALGEBRA_PROTOCOLS: frozenset[str] = frozenset({"camelot"})

# Quoter addresses used for AUTO fee tier selection.
SWAP_QUOTER_ADDRESSES: dict[str, dict[str, str]] = {
    "ethereum": {
        "uniswap_v3": "0x61fFE014bA17989E743c5F6cB21bF9697530B21e",
        "sushiswap_v3": "0x64e8802FE490fa7cc61d3463958199161Bb608A7",
        "pancakeswap_v3": "0xB048Bbc1Ee6b733FFfCFb9e9CeF7375518e25997",
    },
    "arbitrum": {
        "uniswap_v3": "0x61fFE014bA17989E743c5F6cB21bF9697530B21e",
        "sushiswap_v3": "0x0524E833cCD057e4d7A296e3aaAb9f7675964Ce1",
        "pancakeswap_v3": "0xB048Bbc1Ee6b733FFfCFb9e9CeF7375518e25997",
        # Camelot V3 (Algebra V1.9) Quoter — VIB-3750.
        # Algebra-style ABI (no fee tier param, no struct):
        #   quoteExactInputSingle(address tokenIn, address tokenOut,
        #                         uint256 amountIn, uint160 limitSqrtPrice)
        #     -> (uint256 amountOut, uint16 fee)
        # Source: https://docs.camelot.exchange/contracts/amm-v3/deployed-contracts
        "camelot": "0x0Fc73040b26E9bC8514fA028D998E73A254Fa76E",
    },
    "optimism": {
        "uniswap_v3": "0x61fFE014bA17989E743c5F6cB21bF9697530B21e",
    },
    "polygon": {
        "uniswap_v3": "0x61fFE014bA17989E743c5F6cB21bF9697530B21e",
        "sushiswap_v3": "0xb1E835Dc2785b52265711e17fCCb0fd018226a6e",
    },
    "base": {
        "uniswap_v3": "0x3d4e44Eb1374240CE5F1B871ab261CD16335B76a",
        "sushiswap_v3": "0xb1E835Dc2785b52265711e17fCCb0fd018226a6e",
        "pancakeswap_v3": "0xB048Bbc1Ee6b733FFfCFb9e9CeF7375518e25997",
    },
    "avalanche": {
        "uniswap_v3": "0xbe0F5544EC67e9B3b2D979aaA43f18Fd87E6257F",
        # sushiswap_v3 removed: zero usable liquidity (VIB-2069)
    },
    "bsc": {
        "uniswap_v3": "0x78D78E420Da98ad378D7799bE8f4AF69033EB077",  # QuoterV2
        "pancakeswap_v3": "0xB048Bbc1Ee6b733FFfCFb9e9CeF7375518e25997",  # PCS V3 Quoter
        "sushiswap_v3": "0xb1E835Dc2785b52265711e17fCCb0fd018226a6e",  # SushiSwap V3 Quoter
    },
    "bnb": {  # Alias for "bsc" (VIB-708 unification)
        "uniswap_v3": "0x78D78E420Da98ad378D7799bE8f4AF69033EB077",
        "pancakeswap_v3": "0xB048Bbc1Ee6b733FFfCFb9e9CeF7375518e25997",
        "sushiswap_v3": "0xb1E835Dc2785b52265711e17fCCb0fd018226a6e",
    },
    "linea": {
        "uniswap_v3": "0x42bE4D6527829FeFA1493e1fb9F3676d2425C3C1",
        "pancakeswap_v3": "0xB048Bbc1Ee6b733FFfCFb9e9CeF7375518e25997",
    },
    "mantle": {
        "agni_finance": "0xc4aaDc921E1cdb66c5300Bc158a313292923C0cb",  # Agni Finance QuoterV2
    },
    "xlayer": {
        "uniswap_v3": "0x976183AC3d09840D243A88c0268BADb3B3e3259f",  # QuoterV2 (Governance Proposal 67)
    },
    "monad": {
        "uniswap_v3": "0x661E93cca42AfacB172121EF892830cA3b70F08d",  # QuoterV2 — https://docs.uniswap.org/contracts/v3/reference/deployments/monad-deployments
    },
    "zerog": {
        "uniswap_v3": "0xd00883722cECAD3A1c60bCA611f09e1851a0bE02",  # JAINE DEX QuoterV2 (Uniswap V3 fork)
    },
}

# Aave V3 Pool addresses per chain
LENDING_POOL_ADDRESSES: dict[str, dict[str, str]] = {
    "ethereum": {
        "aave_v3": "0x87870Bca3F3fD6335C3F4ce8392D69350B4fA4E2",
        "radiant_v2": "0xA950974f64aA33f27F6C5e017eEE93BF7588ED07",
        # Spark is an Aave V3 fork — same pool interface, Spark-specific deployment.
        # Address mirrors SPARK_POOL_ADDRESSES in almanak/framework/connectors/spark/adapter.py.
        "spark": "0xC13e21B648A5Ee794902342038FF3aDAB66BE987",
    },
    "arbitrum": {
        "aave_v3": "0x794a61358D6845594F94dc1DB02A252b5b4814aD",
        # radiant_v2 intentionally absent — pool is a stub on arbitrum.
        # See #1842 / #1847 / #1889 and tests/unit/connectors/test_radiant_v2.py.
    },
    "optimism": {
        "aave_v3": "0x794a61358D6845594F94dc1DB02A252b5b4814aD",
    },
    "polygon": {
        "aave_v3": "0x794a61358D6845594F94dc1DB02A252b5b4814aD",
    },
    "base": {
        "aave_v3": "0xA238Dd80C259a72e81d7e4664a9801593F98d1c5",
    },
    "avalanche": {
        "aave_v3": "0x794a61358D6845594F94dc1DB02A252b5b4814aD",
    },
    "bsc": {
        "aave_v3": "0x6807dc923806fE8Fd134338EABCA509979a7e0cB",
    },
    "sonic": {
        "aave_v3": "0x5362dBb1e601abF3a4c14c22ffEdA64042E5eAA3",
    },
    "linea": {
        "aave_v3": "0xc47b8C00b0f69a36fa203Ffeac0334874574a8Ac",
    },
    "plasma": {
        "aave_v3": "0x925a2A7214Ed92428B5b1B090F80b25700095e12",
    },
    "mantle": {
        "aave_v3": "0x458F293454fE0d67EC0655f3672301301DD51422",
    },
    "xlayer": {
        "aave_v3": "0xE3F3Caefdd7180F884c01E57f65Df979Af84f116",
    },
}

# PoolDataProvider addresses per chain/protocol. Used by the lending pre-flight
# checks to call `getReserveConfigurationData(asset)` and surface frozen/inactive
# reserves as typed compile-time errors (VIB-3701, VIB-3749).
#
# Aave V2 and Aave V3 share the `getReserveConfigurationData(address)` selector
# and ABI-encoded return layout, so the same pre-flight code works for both.
# Radiant V2 is an Aave V2 fork — only Ethereum has a working deployment:
#   - Ethereum Pool 0xA950...ED07, AaveProtocolDataProvider 0x362f...3813.
#     Pool is active; the pre-flight gives us a fast fail when the operator
#     picks an asset whose reserve has been retired.
#   - Arbitrum: deliberately absent. The Radiant V2 LendingPool proxy on
#     Arbitrum was reduced to a stub implementation after the Oct 2024 attack
#     and the framework excludes radiant_v2/arbitrum at every layer. See
#     ``LENDING_POOL_ADDRESSES`` above and issues #1842 / #1847 / #1889.
LENDING_POOL_DATA_PROVIDERS: dict[str, dict[str, str]] = {
    "ethereum": {
        "aave_v3": "0x7B4EB56E7CD4b454BA8ff71E4518426369a138a3",
        "radiant_v2": "0x362f3BB63Cff83bd169aE1793979E9e537993813",
    },
    "arbitrum": {
        "aave_v3": "0x69FA688f1Dc47d4B5d8029D5a35FB7a548310654",
        # radiant_v2 intentionally absent — see comment above and
        # LENDING_POOL_ADDRESSES.
    },
    "optimism": {
        "aave_v3": "0x69FA688f1Dc47d4B5d8029D5a35FB7a548310654",
    },
    "polygon": {
        "aave_v3": "0x69FA688f1Dc47d4B5d8029D5a35FB7a548310654",
    },
    "base": {
        "aave_v3": "0x2d8A3C5677189723C4cB8873CfC9C8976FDF38Ac",
    },
    "avalanche": {
        "aave_v3": "0x69FA688f1Dc47d4B5d8029D5a35FB7a548310654",
    },
    "bsc": {
        "aave_v3": "0xc90Df74A7c16245c5F5C5870327Ceb38Fe5d5328",
    },
    "sonic": {
        "aave_v3": "0xc0a344397cfa89dF1e1d3e4fb330834D789cF2CD",
    },
    "linea": {
        "aave_v3": "0x47cd4b507B81cB831669c71c7077f4daF6762FF4",
    },
    "plasma": {
        "aave_v3": "0x69FA688f1Dc47d4B5d8029D5a35FB7a548310654",
    },
    "mantle": {
        "aave_v3": "0x487c5c669D9eee6057C44973207101276cf73b68",
    },
    "xlayer": {
        "aave_v3": "0x6C505C31714f14e8af2A03633EB2Cdfb4959138F",
    },
}

# Standard ERC20 function selectors
ERC20_APPROVE_SELECTOR = "0x095ea7b3"  # approve(address,uint256)
ERC20_ALLOWANCE_SELECTOR = "0xdd62ed3e"  # allowance(address,address)
ERC20_TRANSFER_SELECTOR = "0xa9059cbb"  # transfer(address,uint256)
ERC20_TRANSFER_FROM_SELECTOR = "0x23b872dd"  # transferFrom(address,address,uint256)

# Tokens that require approve(0) before approving a new amount if allowance > 0
# This is a security feature in USDC/USDT to prevent certain attack vectors
APPROVE_ZERO_FIRST_TOKENS: set[str] = {
    # Avalanche USDC
    "0xB97EF9Ef8734C71904D8002F8b6Bc66Dd9c48a6E".lower(),
    # Avalanche USDC.e (bridged)
    "0xA7D7079b0FEaD91F3e65f86E8915Cb59c1a4C664".lower(),
    # Avalanche USDT
    "0x9702230A8Ea53601f5cD2dc00fDBc13d4dF4A8c7".lower(),
    # Arbitrum USDC
    "0xaf88d065e77c8cC2239327C5EDb3A432268e5831".lower(),
    # Arbitrum USDC.e (bridged)
    "0xFF970A61A04b1cA14834A43f5dE4533eBDDB5CC8".lower(),
    # Arbitrum USDT
    "0xFd086bC7CD5C481DCC9C85ebE478A1C0b69FCbb9".lower(),
    # Ethereum USDC
    "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48".lower(),
    # Ethereum USDT
    "0xdAC17F958D2ee523a2206206994597C13D831ec7".lower(),
    # Base USDC
    "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913".lower(),
    # Optimism USDC
    "0x0b2C639c533813f4Aa9D7837CAf62653d097Ff85".lower(),
    # Optimism USDC.e (bridged)
    "0x7F5c764cBc14f9669B88837ca1490cCa17c31607".lower(),
}

# Uniswap V3 NonfungiblePositionManager function selectors
# mint(MintParams): create new position
NFT_POSITION_MINT_SELECTOR = "0x88316456"
# increaseLiquidity(IncreaseLiquidityParams): add liquidity to existing position
NFT_POSITION_INCREASE_SELECTOR = "0x219f5d17"
# decreaseLiquidity(DecreaseLiquidityParams): remove liquidity from position
NFT_POSITION_DECREASE_SELECTOR = "0x0c49ccbe"
# collect(CollectParams): collect tokens owed (fees + withdrawn liquidity)
NFT_POSITION_COLLECT_SELECTOR = "0xfc6f7865"
# burn(tokenId): burn position NFT (requires position to be empty)
NFT_POSITION_BURN_SELECTOR = "0x42966c68"

# Aave V2 forks (use deposit() instead of supply(), otherwise same ABI).
AAVE_V2_FORKS = {"radiant_v2"}

# Protocols that share the Aave V3 lending pool interface (same ABI, different addresses).
AAVE_COMPATIBLE_PROTOCOLS = {"aave_v3"} | AAVE_V2_FORKS

# Aave V2 Pool function selectors (used by V2 forks: Radiant V2, etc.)
# deposit(address asset, uint256 amount, address onBehalfOf, uint16 referralCode)
AAVE_V2_DEPOSIT_SELECTOR = "0xe8eda9df"

# Aave V3 Pool function selectors
# supply(address asset, uint256 amount, address onBehalfOf, uint16 referralCode)
AAVE_SUPPLY_SELECTOR = "0x617ba037"
# borrow(address asset, uint256 amount, uint256 interestRateMode, uint16 referralCode, address onBehalfOf)
AAVE_BORROW_SELECTOR = "0xa415bcad"
# repay(address asset, uint256 amount, uint256 interestRateMode, address onBehalfOf)
AAVE_REPAY_SELECTOR = "0x573ade81"
# withdraw(address asset, uint256 amount, address to)
AAVE_WITHDRAW_SELECTOR = "0x69328dec"
# setUserUseReserveAsCollateral(address asset, bool useAsCollateral)
AAVE_SET_COLLATERAL_SELECTOR = "0x5a3b74b9"
# flashLoan(address receiverAddress, address[] assets, uint256[] amounts, uint256[] modes, address onBehalfOf, bytes params, uint16 referralCode)
AAVE_FLASH_LOAN_SELECTOR = "0xab9c4b5d"
# flashLoanSimple(address receiverAddress, address asset, uint256 amount, bytes params, uint16 referralCode)
AAVE_FLASH_LOAN_SIMPLE_SELECTOR = "0x42b0b77c"

# Aave interest rate modes
AAVE_VARIABLE_RATE_MODE = 2  # Variable rate (stable rate deprecated on Aave V3)


# Balancer Vault function selectors
# flashLoan(address recipient, address[] tokens, uint256[] amounts, bytes userData)
BALANCER_FLASH_LOAN_SELECTOR = "0x5c38449e"

# Balancer Vault addresses (same on all chains - Balancer uses deterministic deployment)
BALANCER_VAULT_ADDRESSES: dict[str, str] = {
    "ethereum": "0xBA12222222228d8Ba445958a75a0704d566BF2C8",
    "arbitrum": "0xBA12222222228d8Ba445958a75a0704d566BF2C8",
    "optimism": "0xBA12222222228d8Ba445958a75a0704d566BF2C8",
    "polygon": "0xBA12222222228d8Ba445958a75a0704d566BF2C8",
    "base": "0xBA12222222228d8Ba445958a75a0704d566BF2C8",
    "avalanche": "0xBA12222222228d8Ba445958a75a0704d566BF2C8",
}

# Max uint256 for unlimited approvals
MAX_UINT256 = 2**256 - 1
# Max uint128 for collecting all fees/tokens
MAX_UINT128 = 2**128 - 1
