"""Curve Finance Protocol Adapter.

This module provides the CurveAdapter class for executing swaps and
managing liquidity positions on Curve Finance pools.

Curve Pool Types:
- StableSwap: Optimized for stablecoin pairs (low slippage)
- CryptoSwap: For volatile asset pairs (2 coins)
- Tricrypto: For 3-coin volatile pools

Key Contracts:
- Router: CurveRouterNG for multi-hop swaps
- Pools: Individual pool contracts for direct swaps and LP operations
- Factory: Creates new pools

Function Selectors:
- exchange(int128,int128,uint256,uint256): 0x3df02124 (StableSwap)
- exchange(uint256,uint256,uint256,uint256): 0x5b41b908 (CryptoSwap/Tricrypto)
- add_liquidity(uint256[2],uint256): varies by pool size
- remove_liquidity(uint256,uint256[2]): varies by pool size
- remove_liquidity_one_coin(uint256,int128,uint256): 0x1a4d01d2
"""

import logging
from dataclasses import dataclass, field
from decimal import Decimal
from enum import Enum
from typing import TYPE_CHECKING, Any

from almanak.connectors._strategy_base.rpc import eth_call_uint256
from almanak.connectors._strategy_base.swap_oracle_guard import (
    DEFAULT_SWAP_ORACLE_DIVERGENCE_BPS,
    check_swap_oracle_divergence,
)
from almanak.core.chains._helpers import native_symbols_for
from almanak.framework.data.tokens.exceptions import TokenResolutionError

if TYPE_CHECKING:
    from almanak.framework.data.tokens.resolver import TokenResolver as TokenResolverType
    from almanak.framework.gateway_client import GatewayClient

logger = logging.getLogger(__name__)


# =============================================================================
# Constants
# =============================================================================

# Curve contract addresses per chain
CURVE_ADDRESSES: dict[str, dict[str, str]] = {
    "ethereum": {
        "router": "0x16C6521Dff6baB339122a0FE25a9116693265353",
        "address_provider": "0x5ffe7FB82894076ECB99A30D6A32e969e6e35E98",
        "stableswap_factory": "0x6A8cbed756804B16E05E741eDaBd5cB544AE21bf",
        "twocrypto_factory": "0x98EE851a00abeE0d95D08cF4CA2BdCE32aeaAF7F",
        "tricrypto_factory": "0x0c0e5f2fF0ff18a3be9b835635039256dC4B4963",
        "crv_token": "0xD533a949740bb3306d119CC777fa900bA034cd52",
    },
    "arbitrum": {
        "router": "0x2191718CD32d02B8E60BAdFFeA33E4B5DD9A0A0D",
        "address_provider": "0x5ffe7FB82894076ECB99A30D6A32e969e6e35E98",
        "stableswap_factory": "0x9AF14D26075f142eb3F292D5065EB3faa646167b",
        "twocrypto_factory": "0x98EE851a00abeE0d95D08cF4CA2BdCE32aeaAF7F",
        "tricrypto_factory": "0xbC0797015fcFc47d9C1856639CaE50D0e69FbEE8",
    },
    "base": {
        "router": "0xd6681e74eEA20d196c15038C580f721EF2aB6320",  # CurveRouterNG on Base
        "address_provider": "0x5ffe7FB82894076ECB99A30D6A32e969e6e35E98",  # Same across all EVM chains
        "stableswap_factory": "0x3093f9B57A428F3EB6285a589cb35bEA6e78c336",  # StableswapFactory NG
        "twocrypto_factory": "0xc9FE0c63AF9a39402E8a5514F9c21af076813f1b",  # TwocryptoFactory NG
        "tricrypto_factory": "0xa5961898d4539B95e3B8571c74f86D5E5b48DB25",  # TricryptoFactory NG
    },
    "optimism": {
        "router": "0xF0d4c12A5768D806021F80a262B4d39d26C58b8D",  # CurveRouterNG on Optimism
        "address_provider": "0x5ffe7FB82894076ECB99A30D6A32e969e6e35E98",  # Universal
        "stableswap_factory": "0xA9B52d3CfB60073b7cC3D53dD3f25a8C619Afd78",
    },
    "polygon": {
        "address_provider": "0x5ffe7FB82894076ECB99A30D6A32e969e6e35E98",  # Universal across EVM chains
        "stableswap_factory": "0x722272D36ef0Da72FF51c5A65Db7b870E2e8D4ee",  # Polygon StableSwap factory
    },
}

# Popular Curve pools per chain
# TECH_DEBT(VIB-581): virtual_price values are approximate snapshots. Curve virtual_price
# increases monotonically as fees accumulate, so these will drift over time. The safe direction
# is under-estimating (lower min_lp = worse slippage protection but no reverts). A future
# improvement should query virtual_price() from the pool contract at runtime via gateway RPC,
# falling back to these static values if the RPC call fails.
CURVE_POOLS: dict[str, dict[str, dict[str, Any]]] = {
    "ethereum": {
        "3pool": {
            "address": "0xbEbc44782C7dB0a1A60Cb6fe97d0b483032FF1C7",
            "lp_token": "0x6c3F90f043a72FA612cbac8115EE7e52BDe6E490",
            "coins": ["DAI", "USDC", "USDT"],
            "coin_addresses": [
                "0x6B175474E89094C44Da98b954EedeAC495271d0F",
                "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
                "0xdAC17F958D2ee523a2206206994597C13D831ec7",
            ],
            "pool_type": "stableswap",
            "n_coins": 3,
            "virtual_price": Decimal("1.04"),
        },
        "frax_usdc": {
            "address": "0xDcEF968d416a41Cdac0ED8702fAC8128A64241A2",
            "lp_token": "0x3175Df0976dFA876431C2E9eE6Bc45b65d3473CC",
            "coins": ["FRAX", "USDC"],
            "coin_addresses": [
                "0x853d955aCEf822Db058eb8505911ED77F175b99e",
                "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
            ],
            "pool_type": "stableswap",
            "n_coins": 2,
            "virtual_price": Decimal("1.01"),
        },
        "steth": {
            "address": "0xDC24316b9AE028F1497c275EB9192a3Ea0f67022",
            "lp_token": "0x06325440D014e39736583c165C2963BA99fAf14E",
            "coins": ["ETH", "stETH"],
            "coin_addresses": [
                "0xEeeeeEeeeEeEeeEeEeEeeEEEeeeeEeeeeeeeEEeE",
                "0xae7ab96520DE3A18E5e111B5EaAb095312D7fE84",
            ],
            "pool_type": "stableswap",
            "n_coins": 2,
            "virtual_price": Decimal("1.06"),
        },
        "tricrypto2": {
            "address": "0xD51a44d3FaE010294C616388b506AcdA1bfAAE46",
            "lp_token": "0xc4AD29ba4B3c580e6D59105FFf484999997675Ff",
            "coins": ["USDT", "WBTC", "WETH"],
            "coin_addresses": [
                "0xdAC17F958D2ee523a2206206994597C13D831ec7",
                "0x2260FAC5E5542a773Aa44fBCfeDf7C193bc2C599",
                "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2",
            ],
            "pool_type": "tricrypto",
            "n_coins": 3,
            "virtual_price": Decimal("1.0"),
        },
        # FRAX/3CRV factory metapool (VIB-5419).
        # A Curve metapool is NATIVELY a 2-coin StableSwap pool whose coins are
        # [meta coin, base-pool LP token]: here coins(0)=FRAX, coins(1)=3CRV.
        # The metapool IS its own LP token (FRAX3CRV-f), so lp_token == address.
        # Coin order verified on-chain 2026-06-25 via cast call coins(0..1):
        #   coins(0) = FRAX (0x853d..., 18 dec)
        #   coins(1) = 3CRV (0x6c3F..., 18 dec — the 3pool LP token)
        #   get_virtual_price() = 1020475713094786446 -> 1.0205
        #
        # Two interfaces (see PoolInfo.is_metapool):
        #   - Native 2-coin: add_liquidity([fraxAmt, 3crvAmt], min) / exchange(0,1,...)
        #     — handled by the SAME flat-pool code paths as any 2-coin StableSwap
        #     (Tier A). The base LP token (3CRV) is just coins[1].
        #   - Underlying (combined coin space, index 0=FRAX, 1..3=DAI/USDC/USDT):
        #     exchange_underlying(i,j,...) is on the metapool itself; the combined
        #     add_liquidity/remove_liquidity route through the generic 3CRV
        #     DepositZap (zap_address below) whose ABI takes the POOL as the first
        #     arg (Tier B).
        # TECH_DEBT(VIB-581): virtual_price is a snapshot; query get_virtual_price() at runtime.
        # ACCOUNTING NOTE (VIB-5420): coins[1] (3CRV) is a base-LP token, NOT a
        # price-oracle symbol — valuing the native LP's base-LP leg to underlying
        # USD needs a base-pool decomposition / virtual_price mark not yet wired.
        "frax_3crv": {
            "address": "0xd632f22692FaC7611d2AA1C0D552930D43CAEd3B",
            "lp_token": "0xd632f22692FaC7611d2AA1C0D552930D43CAEd3B",  # metapool IS its own LP token
            "coins": ["FRAX", "3CRV"],
            "coin_addresses": [
                "0x853d955aCEf822Db058eb8505911ED77F175b99e",  # FRAX (meta coin)
                "0x6c3F90f043a72FA612cbac8115EE7e52BDe6E490",  # 3CRV (base-pool LP token)
            ],
            "pool_type": "stableswap",
            "n_coins": 2,
            "virtual_price": Decimal("1.0205"),
            "is_metapool": True,
            "base_pool": "0xbEbc44782C7dB0a1A60Cb6fe97d0b483032FF1C7",  # 3pool
            # Underlying coins exposed by the combined interface, in COMBINED index
            # order MINUS the meta coin: i.e. base_pool_coins[k] is combined index
            # k+1 (combined index 0 is always the meta coin). Here 3pool order:
            #   combined 1 = DAI, combined 2 = USDC, combined 3 = USDT.
            "base_pool_coins": ["DAI", "USDC", "USDT"],
            "base_pool_coin_addresses": [
                "0x6B175474E89094C44Da98b954EedeAC495271d0F",  # DAI
                "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",  # USDC
                "0xdAC17F958D2ee523a2206206994597C13D831ec7",  # USDT
            ],
            # Generic 3CRV DepositZap (Curve metapool deposit/withdraw zap).
            # ABI: add_liquidity(address _pool, uint256[4], uint256),
            #      remove_liquidity(address _pool, uint256, uint256[4]),
            #      calc_token_amount(address _pool, uint256[4], bool).
            # The POOL address is the FIRST arg (generic zap, not pool-specific).
            "zap_address": "0xA79828DF1850E8a3A3064576f380D90aECDD3359",
        },
    },
    "arbitrum": {
        "2pool": {
            "address": "0x7f90122BF0700F9E7e1F688fe926940E8839F353",
            "lp_token": "0x7f90122BF0700F9E7e1F688fe926940E8839F353",
            "coins": ["USDC.e", "USDT"],
            "coin_addresses": [
                "0xFF970A61A04b1cA14834A43f5dE4533eBDDB5CC8",  # USDC.e (bridged), NOT native USDC
                "0xFd086bC7CD5C481DCC9C85ebE478A1C0b69FCbb9",
            ],
            "pool_type": "stableswap",
            "n_coins": 2,
            "virtual_price": Decimal("1.022"),
        },
        "tricrypto": {
            "address": "0x960ea3e3C7FB317332d990873d354E18d7645590",
            "lp_token": "0x8e0B8c8BB9db49a46697F3a5Bb8A308e744821D2",
            "coins": ["USDT", "WBTC", "WETH"],
            "coin_addresses": [
                "0xFd086bC7CD5C481DCC9C85ebE478A1C0b69FCbb9",
                "0x2f2a2543B76A4166549F7aaB2e75Bef0aefC5B0f",
                "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1",
            ],
            "pool_type": "tricrypto",
            "n_coins": 3,
            "virtual_price": Decimal("1.0"),
        },
    },
    "base": {
        # WETH/cbETH Twocrypto pool — ETH liquid staking yield, ~3% APY
        # Pool: 0x11C1fBd4b3De66bC0565779b35171a6CF3E71f59 (old Twocrypto, NOT NG)
        # LP token: 0x98244d93D42b42aB3E3A4D12A5dc0B3e7f8F32f9 (SEPARATE from pool — old-style Twocrypto)
        # NOTE: This pool uses an OLD Twocrypto factory (not TwocryptoNG), so the LP token
        # is a separate ERC20 contract, not the pool address itself.
        # Verified on-chain 2026-03-19: pool.token() = 0x98244d93...F32f9
        # TECH_DEBT(VIB-581): virtual_price is a snapshot; query pool.virtual_price() at runtime for accuracy.
        # Queried on-chain 2026-03-19: pool.virtual_price() = 1017035434756947721 -> 1.0170
        # Pool reserves (2026-03-19): 3804.66 WETH + 3223.99 cbETH, LP supply: 3445 LP tokens
        "weth_cbeth": {
            "address": "0x11C1fBd4b3De66bC0565779b35171a6CF3E71f59",
            "lp_token": "0x98244d93D42b42aB3E3A4D12A5dc0B3e7f8F32f9",  # Separate LP token (old-style Twocrypto)
            "coins": ["WETH", "cbETH"],
            "coin_addresses": [
                "0x4200000000000000000000000000000000000006",  # WETH on Base
                "0x2Ae3F1Ec7F1F5012CFEab0185bfc7aa3cf0DEc22",  # cbETH on Base
            ],
            "pool_type": "cryptoswap",
            "n_coins": 2,
            "virtual_price": Decimal("1.017"),
        },
        # StableSwap NG 4pool on Base: USDC / USDbC / axlUSDC / crvUSD
        # First 4-coin pool in the Curve adapter. StableSwap NG: LP token IS the pool address.
        # Coin order verified on-chain 2026-03-23 via cast call coins(0..3):
        #   coins(0) = USDC (0x8335..., 6 dec), coins(1) = USDbC (0xd9aA..., 6 dec)
        #   coins(2) = axlUSDC (0xEB46..., 6 dec), coins(3) = crvUSD (0x417A..., 18 dec)
        # Pool reserves (2026-03-23): ~$50K USDC, ~$50K USDbC, ~$50K axlUSDC, ~$91K crvUSD
        # TECH_DEBT(VIB-581): virtual_price is a snapshot; query pool.virtual_price() at runtime.
        # Queried on-chain 2026-03-23: pool.get_virtual_price() = 1019566780337011070 -> 1.0196
        # NOTE: despite the "StableSwap NG" lp_token comment above, the deployed
        # implementation (0x1621e58d36eb5ef26f9768ebe9db77181b1f5a02, an EIP-1167
        # minimal-proxy target) exposes only the legacy fixed-size selectors
        # (verified 2026-05-26 via bytecode probe). is_ng=False keeps the
        # legacy uint256[4] calldata path. VIB-4836.
        "4pool": {
            "address": "0xf6C5F01C7F3148891ad0e19DF78743D31E390D1f",
            "lp_token": "0xf6C5F01C7F3148891ad0e19DF78743D31E390D1f",
            "coins": ["USDC", "USDbC", "axlUSDC", "crvUSD"],
            "coin_addresses": [
                "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913",  # USDC (native) on Base
                "0xd9aAEc86B65D86f6A7B5B1b0c42FFA531710b6CA",  # USDbC (bridged) on Base
                "0xEB466342C4d449BC9f53A865D5Cb90586f405215",  # axlUSDC (Axelar bridged)
                "0x417Ac0e078398C154EdFadD9Ef675d30Be60Af93",  # crvUSD on Base
            ],
            "pool_type": "stableswap",
            "n_coins": 4,
            "virtual_price": Decimal("1.0196"),
        },
    },
    "optimism": {
        "3pool": {
            # Curve 3pool on Optimism (DAI/USDC.e/USDT)
            # USDC.e = bridged USDC (0x7F5...); native USDC (0x0b2...) is in a separate pool
            "address": "0x1337BedC9D22ecbe766dF105c9623922A27963EC",
            "lp_token": "0x1337BedC9D22ecbe766dF105c9623922A27963EC",
            "coins": ["DAI", "USDC.e", "USDT"],
            "coin_addresses": [
                "0xDA10009cBd5D07dd0CeCc66161FC93D7c9000da1",  # DAI on Optimism
                "0x7F5c764cBc14f9669B88837ca1490cCa17c31607",  # USDC.e (bridged) on Optimism
                "0x94b008aA00579c1307B0EF2c499aD98a8ce58e58",  # USDT on Optimism
            ],
            "pool_type": "stableswap",
            "n_coins": 3,
            "virtual_price": Decimal("1.02"),
        },
        # crvUSD/USDC StableSwap NG pool on Optimism (VIB-1587)
        # Contains NATIVE USDC (0x0b2C639c...) — the key missing piece vs 3pool (which uses USDC.e)
        # StableSwap NG: LP token IS the pool address.
        # Pool verified on Optimism Etherscan: CurveStableSwapNG "crvUSDC Pool"
        # Coin order verified from on-chain contract via cast call (iter 114):
        #   coins(0) = crvUSD (0xC52D...), coins(1) = USDC (0x0b2C...)
        # BUG FIX (iter 114): previous config had coins reversed, causing approve
        # to target wrong token -> "ERC20: insufficient allowance" on every swap.
        # TECH_DEBT(VIB-581): virtual_price is a snapshot; query pool.virtual_price() at runtime for accuracy.
        "crvusd_usdc": {
            "address": "0x03771e24b7C9172d163Bf447490B142a15be3485",
            "lp_token": "0x03771e24b7C9172d163Bf447490B142a15be3485",  # StableSwap NG: LP = pool
            "coins": ["crvUSD", "USDC"],
            "coin_addresses": [
                "0xC52D7F23a2e460248Db6eE192Cb23dD12bDDCbf6",  # crvUSD on Optimism (coins[0])
                "0x0b2C639c533813f4Aa9D7837CAf62653d097Ff85",  # USDC (native) on Optimism (coins[1])
            ],
            "pool_type": "stableswap",
            "n_coins": 2,
            "virtual_price": Decimal("1.0"),
            "is_ng": True,  # StableSwap NG variant (VIB-4836)
        },
    },
    "polygon": {
        # Curve am3pool on Polygon (aave lending pool variant)
        # Pool address: 0x445FE580eF8d70FF569aB36e898ed8631406Db5f
        # LP token (am3CRV): 0xE7a24EF0C5e95Ffb0f6684b813A78F2a3AD7D171
        # This is an aave-type pool: internally holds aDAI/aUSDC/aUSDT (interest-bearing tokens).
        # Users swap UNDERLYING tokens (DAI/USDC.e/USDT) via exchange_underlying().
        # coin_addresses are the UNDERLYING token addresses (not aTokens) since users
        # approve/receive the underlying tokens.
        # Coin order: coins(0)=DAI, coins(1)=USDC.e, coins(2)=USDT
        # TECH_DEBT(VIB-581): virtual_price is a snapshot; query pool.virtual_price() at runtime.
        "3pool": {
            "address": "0x445Fe580Ef8d70Ff569ab36e898ed8631406dB5f",
            "lp_token": "0xE7a24EF0C5e95Ffb0f6684b813A78F2a3AD7D171",  # am3CRV LP token
            "coins": ["DAI", "USDC.e", "USDT"],
            "coin_addresses": [
                "0x8f3Cf7ad23Cd3CaDbD9735AFf958023239c6A063",  # DAI on Polygon
                "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174",  # USDC.e (bridged USDC) on Polygon
                "0xc2132D05D31c914a87C6611C10748AEb04B58e8F",  # USDT on Polygon
            ],
            "pool_type": "stableswap",
            "n_coins": 3,
            "virtual_price": Decimal("1.02"),
            "use_underlying": True,  # exchange_underlying() required for non-aToken swaps
        },
    },
}


# Gas estimates for Curve operations
CURVE_GAS_ESTIMATES: dict[str, int] = {
    "approve": 65000,  # 65K to accommodate proxy tokens (USDC FiatTokenProxy ~56-65K)
    "exchange": 500000,
    "exchange_underlying": 300000,
    # Metapool exchange_underlying routes a leg through the BASE pool (two pools,
    # extra SLOADs/SSTOREs), so it costs more than a flat aave-type
    # exchange_underlying — measured ~284K on FRAX/3CRV. 300K is too tight a
    # limit for the orchestrator's cap; 600K gives safe headroom (VIB-5419).
    "exchange_underlying_metapool": 600000,
    # Metapool zap add/remove deposit into the base pool AND the metapool, so
    # they cost more than a single flat add/remove. 700K headroom.
    "metapool_zap_add_liquidity": 700000,
    "metapool_zap_remove_liquidity": 600000,
    "add_liquidity_2": 250000,
    "add_liquidity_3": 350000,
    "add_liquidity_4": 450000,
    "remove_liquidity": 200000,
    "remove_liquidity_one_coin": 250000,
    "remove_liquidity_imbalance": 300000,
    "router_exchange": 400000,
}

# Function selectors
EXCHANGE_SELECTOR = "0x3df02124"  # exchange(int128,int128,uint256,uint256) - StableSwap
EXCHANGE_UINT256_SELECTOR = "0x5b41b908"  # exchange(uint256,uint256,uint256,uint256) - CryptoSwap/Tricrypto
EXCHANGE_UNDERLYING_SELECTOR = "0xa6417ed6"  # exchange_underlying(int128,int128,uint256,uint256)
ADD_LIQUIDITY_2_SELECTOR = "0x0b4c7e4d"  # add_liquidity(uint256[2],uint256)
ADD_LIQUIDITY_3_SELECTOR = "0x4515cef3"  # add_liquidity(uint256[3],uint256)
ADD_LIQUIDITY_4_SELECTOR = "0x029b2f34"  # add_liquidity(uint256[4],uint256)
ADD_LIQUIDITY_DYN_SELECTOR = "0xb72df5de"  # add_liquidity(uint256[],uint256) — StableSwap NG
REMOVE_LIQUIDITY_2_SELECTOR = "0x5b36389c"  # remove_liquidity(uint256,uint256[2])
REMOVE_LIQUIDITY_3_SELECTOR = "0xecb586a5"  # remove_liquidity(uint256,uint256[3])
REMOVE_LIQUIDITY_4_SELECTOR = "0x7d49d875"  # remove_liquidity(uint256,uint256[4])
REMOVE_LIQUIDITY_DYN_SELECTOR = "0xd40ddb8c"  # remove_liquidity(uint256,uint256[]) — StableSwap NG
REMOVE_LIQUIDITY_ONE_SELECTOR = "0x1a4d01d2"  # remove_liquidity_one_coin(uint256,int128,uint256)
GET_DY_SELECTOR = "0x5e0d443f"  # get_dy(int128,int128,uint256)
GET_DY_UINT256_SELECTOR = "0x556d6e9f"  # get_dy(uint256,uint256,uint256)
GET_DY_UNDERLYING_SELECTOR = "0x07211ef7"  # get_dy_underlying(int128,int128,uint256)
ERC20_APPROVE_SELECTOR = "0x095ea7b3"  # approve(address,uint256)
ERC20_ALLOWANCE_SELECTOR = "0xdd62ed3e"  # allowance(address owner, address spender)

# Metapool generic-zap selectors (VIB-5419 — Tier B underlying routing).
# The generic 3CRV DepositZap takes the POOL address as the FIRST argument, so
# its add/remove/calc selectors differ from the pool-direct ones above. The
# `_deposit_amounts` / `_min_amounts` arrays span the COMBINED coin space
# (index 0 = meta coin, 1..N = base-pool coins). N_COINS+1 is the array length;
# the FRAX/3CRV zap is fixed at uint256[4] (1 meta + 3 base coins).
ZAP_ADD_LIQUIDITY_4_SELECTOR = "0x384e03db"  # add_liquidity(address,uint256[4],uint256)
ZAP_REMOVE_LIQUIDITY_4_SELECTOR = "0xad5cc918"  # remove_liquidity(address,uint256,uint256[4])
ZAP_CALC_TOKEN_AMOUNT_4_SELECTOR = "0x861cdef0"  # calc_token_amount(address,uint256[4],bool)
ZAP_GET_DY_UNDERLYING_SELECTOR = "0x07211ef7"  # exchange_underlying lives on the metapool itself

# CryptoSwap / Tricrypto ``calc_token_amount`` selectors, keyed by coin count.
# Crypto pools split on version: crypto-NG / Tricrypto-2 carry the ``bool deposit``
# flag, while the older twocrypto pools (e.g. Base WETH/cbETH) are deposit-only and
# omit it. The deposit array is a FIXED-size ``uint256[N]`` (encoded inline, no
# offset/length — unlike the StableSwap-NG dynamic ``uint256[]``). We probe
# bool-first then no-bool at call time rather than hard-code a per-pool version
# flag (cf. M0 dynamic resolution, VIB-5424). Selectors + encoding verified
# on-chain 2026-06-27: tricrypto2 (eth) + tricrypto (arb) answer ``[3],bool``;
# weth_cbeth (base) answers ``[2]`` (no bool).
CRYPTO_CALC_TOKEN_AMOUNT_SELECTORS: dict[int, tuple[str, str]] = {
    2: ("0xed8e84f3", "0x8d8ea727"),  # calc_token_amount(uint256[2],bool) | (uint256[2])
    3: ("0x3883e119", "0x5b6f1b5a"),  # calc_token_amount(uint256[3],bool) | (uint256[3])
}

# Max uint256 for unlimited approvals
MAX_UINT256 = 2**256 - 1


# =============================================================================
# Enums
# =============================================================================


class PoolType(Enum):
    """Curve pool type."""

    STABLESWAP = "stableswap"
    CRYPTOSWAP = "cryptoswap"
    TRICRYPTO = "tricrypto"


# =============================================================================
# Data Classes
# =============================================================================


@dataclass
class CurveConfig:
    """Configuration for CurveAdapter.

    Attributes:
        chain: Target blockchain (ethereum, arbitrum)
        wallet_address: Address executing transactions
        default_slippage_bps: Default slippage tolerance in basis points (default 50 = 0.5%)
        deadline_seconds: Transaction deadline in seconds (default 300 = 5 minutes)
        rpc_url: Optional JSON-RPC URL for on-chain state queries (e.g., pool balances
            for accurate remove_liquidity slippage estimates). When provided, the adapter
            queries pool.balances(i) and lp_token.totalSupply() to compute proportional
            min_amounts rather than returning zeros. When absent or on RPC failure,
            min_amounts fall back to [0, 0, ..., 0] with a warning.
    """

    chain: str
    wallet_address: str
    default_slippage_bps: int = 50
    deadline_seconds: int = 300
    rpc_url: str | None = None  # DEPRECATED — use gateway_client
    gateway_client: "GatewayClient | None" = field(default=None, repr=False, compare=False)

    def __post_init__(self) -> None:
        """Validate configuration."""
        if self.chain not in CURVE_ADDRESSES:
            raise ValueError(f"Unsupported chain: {self.chain}. Supported: {list(CURVE_ADDRESSES.keys())}")

        if self.default_slippage_bps < 0 or self.default_slippage_bps > 10000:
            raise ValueError("Slippage must be between 0 and 10000 basis points")

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "chain": self.chain,
            "wallet_address": self.wallet_address,
            "default_slippage_bps": self.default_slippage_bps,
            "deadline_seconds": self.deadline_seconds,
            "rpc_url": self.rpc_url,
        }


@dataclass
class PoolInfo:
    """Information about a Curve pool.

    Attributes:
        address: Pool contract address
        lp_token: LP token address
        coins: List of coin symbols
        coin_addresses: List of coin addresses
        pool_type: Type of pool (stableswap, cryptoswap, tricrypto)
        n_coins: Number of coins in pool
        name: Pool name
        virtual_price: Pool virtual price (LP token value relative to underlying).
            Mature pools accumulate fees so virtual_price > 1.0. Used to adjust
            LP token estimates to prevent over-estimation that causes add_liquidity reverts.
    """

    address: str
    lp_token: str
    coins: list[str]
    coin_addresses: list[str]
    pool_type: PoolType
    n_coins: int
    name: str = ""
    virtual_price: Decimal = field(default_factory=lambda: Decimal("1.0"))
    use_underlying: bool = False  # When True, use exchange_underlying() (aave-type pools)
    # StableSwap NG pools encode add_liquidity / remove_liquidity with a dynamic
    # `uint256[]` array regardless of n_coins, instead of the legacy fixed-size
    # `uint256[N_COINS]` ABI. exchange / remove_liquidity_one_coin selectors are
    # unchanged. See VIB-4836 for the diagnostic; pool bytecode probe of the
    # Optimism crvUSD/USDC pool (0x03771e24…) on 2026-05-26 confirmed only the
    # dynamic-array selectors are present.
    is_ng: bool = False
    # Metapool support (VIB-5419). A Curve metapool is NATIVELY a 2-coin pool
    # `[meta coin, base-pool LP token]` (coins[1] is itself an LP token like
    # 3CRV). All native operations (`add_liquidity([meta, baseLP])`,
    # `exchange(0, 1)`, `remove_liquidity`) reuse the flat 2-coin paths
    # unchanged (Tier A). The fields below describe the UNDERLYING / combined
    # coin space (index 0 = meta coin, 1..N = base-pool coins) reached through
    # the generic deposit zap and the metapool's own `exchange_underlying`
    # (Tier B). All additive — non-meta pools keep `is_metapool=False` and
    # ignore the rest.
    is_metapool: bool = False
    base_pool: str | None = None  # base-pool contract address (e.g. 3pool)
    base_pool_coins: list[str] | None = None  # underlying symbols, combined index 1..N
    base_pool_coin_addresses: list[str] | None = None  # underlying addresses, combined index 1..N
    zap_address: str | None = None  # generic deposit zap (pool is first arg)

    @staticmethod
    def _match_coin(coin: str, symbols: list[str], addresses: list[str]) -> int | None:
        """Index of ``coin`` in the parallel symbol / address lists, or ``None``.

        Matches case-insensitively by symbol first, then by address — so a caller
        may pass either form. ``symbols`` and ``addresses`` are positionally
        aligned (entry ``k`` is the same coin in both); a match in either list
        returns that shared index.
        """
        for k, sym in enumerate(symbols):
            if sym.upper() == coin.upper():
                return k
        for k, addr in enumerate(addresses):
            if addr.lower() == coin.lower():
                return k
        return None

    def underlying_coin_index(self, coin: str) -> int | None:
        """Return the COMBINED-space index of ``coin`` for a metapool, or ``None``.

        Combined index 0 is always the meta coin (``coins[0]``); indices 1..N map
        to ``base_pool_coins`` / ``base_pool_coin_addresses`` in order. ``coin``
        may be a symbol or an address. Returns ``None`` when this is not a
        metapool or ``coin`` is neither the meta coin nor a base-pool coin — the
        caller then falls back to the native 2-coin path.
        """
        if not self.is_metapool:
            return None
        # Combined coin space = [meta coin, *base-pool coins], so combined index 0
        # is coins[0] and base coin k lands at k+1 — preserved by ordering the
        # meta coin first in both parallel lists.
        combined_syms = [self.coins[0], *(self.base_pool_coins or [])]
        combined_addrs = [self.coin_addresses[0], *(self.base_pool_coin_addresses or [])]
        return self._match_coin(coin, combined_syms, combined_addrs)

    def get_coin_index(self, coin: str) -> int:
        """Get the index of a coin in the pool.

        Args:
            coin: Coin symbol or address

        Returns:
            Index of the coin

        Raises:
            ValueError: If coin not found in pool
        """
        # Check by symbol
        for i, c in enumerate(self.coins):
            if c.upper() == coin.upper():
                return i

        # Check by address
        for i, addr in enumerate(self.coin_addresses):
            if addr.lower() == coin.lower():
                return i

        raise ValueError(f"Coin {coin} not found in pool. Available: {self.coins}")

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "address": self.address,
            "lp_token": self.lp_token,
            "coins": self.coins,
            "coin_addresses": self.coin_addresses,
            "pool_type": self.pool_type.value,
            "n_coins": self.n_coins,
            "name": self.name,
            "virtual_price": str(self.virtual_price),
            "use_underlying": self.use_underlying,
            "is_ng": self.is_ng,
            "is_metapool": self.is_metapool,
            "base_pool": self.base_pool,
            "base_pool_coins": self.base_pool_coins,
            "base_pool_coin_addresses": self.base_pool_coin_addresses,
            "zap_address": self.zap_address,
        }


@dataclass
class TransactionData:
    """Transaction data for execution.

    Attributes:
        to: Target contract address
        value: Native token value to send
        data: Encoded calldata
        gas_estimate: Estimated gas
        description: Human-readable description
        tx_type: Type of transaction (approve, swap, add_liquidity, remove_liquidity)
    """

    to: str
    value: int
    data: str
    gas_estimate: int
    description: str
    tx_type: str = "swap"

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "to": self.to,
            "value": str(self.value),
            "data": self.data,
            "gas_estimate": self.gas_estimate,
            "description": self.description,
            "tx_type": self.tx_type,
        }


@dataclass
class SwapResult:
    """Result of a swap operation.

    Attributes:
        success: Whether the swap was built successfully
        transactions: List of transactions to execute
        pool_address: Pool used for swap
        amount_in: Input amount in wei
        amount_out_minimum: Minimum output amount (with slippage)
        token_in: Input token address
        token_out: Output token address
        error: Error message if failed
        gas_estimate: Total gas estimate
    """

    success: bool
    transactions: list[TransactionData] = field(default_factory=list)
    pool_address: str = ""
    amount_in: int = 0
    amount_out_minimum: int = 0
    amount_out_estimate: int = 0  # VIB-3203 Phase B — pre-slippage quote (wei)
    token_out_decimals: int = 18  # decimal-policy-exempt: display fallback only; measured value overwrites on success
    token_in: str = ""
    token_out: str = ""
    error: str | None = None
    gas_estimate: int = 0

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "success": self.success,
            "transactions": [tx.to_dict() for tx in self.transactions],
            "pool_address": self.pool_address,
            "amount_in": str(self.amount_in),
            "amount_out_minimum": str(self.amount_out_minimum),
            "amount_out_estimate": str(self.amount_out_estimate),
            "token_out_decimals": self.token_out_decimals,
            "token_in": self.token_in,
            "token_out": self.token_out,
            "error": self.error,
            "gas_estimate": self.gas_estimate,
        }


@dataclass
class LiquidityResult:
    """Result of a liquidity operation.

    Attributes:
        success: Whether the operation was built successfully
        transactions: List of transactions to execute
        pool_address: Pool address
        operation: Operation type (add_liquidity, remove_liquidity, remove_liquidity_one_coin)
        amounts: Token amounts for the operation
        lp_amount: LP token amount (minted or burned)
        error: Error message if failed
        gas_estimate: Total gas estimate
    """

    success: bool
    transactions: list[TransactionData] = field(default_factory=list)
    pool_address: str = ""
    operation: str = ""
    amounts: list[int] = field(default_factory=list)
    lp_amount: int = 0
    error: str | None = None
    gas_estimate: int = 0

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "success": self.success,
            "transactions": [tx.to_dict() for tx in self.transactions],
            "pool_address": self.pool_address,
            "operation": self.operation,
            "amounts": [str(a) for a in self.amounts],
            "lp_amount": str(self.lp_amount),
            "error": self.error,
            "gas_estimate": self.gas_estimate,
        }


# =============================================================================
# Curve Adapter
# =============================================================================


class CurveAdapter:
    """Adapter for Curve Finance DEX protocol.

    This adapter provides methods for:
    - Executing token swaps via Curve pools
    - Adding liquidity to pools (LP_OPEN)
    - Removing liquidity from pools (LP_CLOSE)
    - Handling ERC-20 approvals
    - Managing slippage protection

    Example:
        config = CurveConfig(
            chain="ethereum",
            wallet_address="0x...",
        )
        adapter = CurveAdapter(config)

        # Execute a swap on 3pool
        result = adapter.swap(
            pool_address="0xbEbc44782C7dB0a1A60Cb6fe97d0b483032FF1C7",
            token_in="USDC",
            token_out="DAI",
            amount_in=Decimal("1000"),
        )
    """

    def __init__(self, config: CurveConfig, token_resolver: "TokenResolverType | None" = None) -> None:
        """Initialize the adapter.

        Args:
            config: Curve adapter configuration
            token_resolver: Optional TokenResolver instance. If None, uses singleton.
        """
        self.config = config
        self.chain = config.chain
        self.wallet_address = config.wallet_address
        self._rpc_url = config.rpc_url
        self._gateway_client = config.gateway_client

        # Load contract addresses
        self.addresses = CURVE_ADDRESSES[self.chain]
        self.pools = CURVE_POOLS.get(self.chain, {})

        # TokenResolver integration
        if token_resolver is not None:
            self._token_resolver = token_resolver
        else:
            from almanak.framework.data.tokens.resolver import get_token_resolver

            self._token_resolver = get_token_resolver()

        # Allowance cache (token -> amount approved)
        self._allowance_cache: dict[str, int] = {}

        logger.info(f"CurveAdapter initialized for chain={self.chain}, wallet={self.wallet_address[:10]}...")

    # =========================================================================
    # Pool Information
    # =========================================================================

    def get_pool_info(self, pool_address: str) -> PoolInfo | None:
        """Get information about a pool.

        Args:
            pool_address: Pool contract address

        Returns:
            PoolInfo if known, None otherwise
        """
        for name, pool_data in self.pools.items():
            if pool_data["address"].lower() == pool_address.lower():
                return PoolInfo(
                    address=pool_data["address"],
                    lp_token=pool_data["lp_token"],
                    coins=pool_data["coins"],
                    coin_addresses=pool_data["coin_addresses"],
                    pool_type=PoolType(pool_data["pool_type"]),
                    n_coins=pool_data["n_coins"],
                    name=name,
                    virtual_price=pool_data.get("virtual_price", Decimal("1.0")),
                    use_underlying=pool_data.get("use_underlying", False),
                    is_ng=pool_data.get("is_ng", False),
                    is_metapool=pool_data.get("is_metapool", False),
                    base_pool=pool_data.get("base_pool"),
                    base_pool_coins=pool_data.get("base_pool_coins"),
                    base_pool_coin_addresses=pool_data.get("base_pool_coin_addresses"),
                    zap_address=pool_data.get("zap_address"),
                )
        return None

    def get_pool_by_name(self, name: str) -> PoolInfo | None:
        """Get pool info by name.

        Args:
            name: Pool name (e.g., "3pool", "frax_usdc")

        Returns:
            PoolInfo if found, None otherwise
        """
        pool_data = self.pools.get(name)
        if pool_data:
            return PoolInfo(
                address=pool_data["address"],
                lp_token=pool_data["lp_token"],
                coins=pool_data["coins"],
                coin_addresses=pool_data["coin_addresses"],
                pool_type=PoolType(pool_data["pool_type"]),
                n_coins=pool_data["n_coins"],
                name=name,
                virtual_price=pool_data.get("virtual_price", Decimal("1.0")),
                use_underlying=pool_data.get("use_underlying", False),
                is_ng=pool_data.get("is_ng", False),
                is_metapool=pool_data.get("is_metapool", False),
                base_pool=pool_data.get("base_pool"),
                base_pool_coins=pool_data.get("base_pool_coins"),
                base_pool_coin_addresses=pool_data.get("base_pool_coin_addresses"),
                zap_address=pool_data.get("zap_address"),
            )
        return None

    # =========================================================================
    # Swap Operations
    # =========================================================================

    def _swap_oracle_guard_error(
        self,
        *,
        pool_info: "PoolInfo",
        token_in_symbol: str,
        token_out_symbol: str,
        amount_in: Decimal,
        amount_out_estimate: int,
        token_out_decimals: int,
        price_ratio: Decimal | None,
        oracle_guard_bps: int | None,
        strict_oracle_guard: bool,
        oracle_prices_real: bool,
    ) -> str | None:
        """Run the P0-8 oracle/MEV min-out guard; return an error to fail with, or
        ``None`` to proceed.

        **Scoped to StableSwap pools.** The check compares the pool's *execution*
        rate (``get_dy``) to the oracle mid. On a StableSwap pool that gap is just
        fee + a few bps of impact, so a material shortfall is a real depeg /
        displacement signal (the audit's P0-8 priority). On a **CryptoSwap /
        Tricrypto** pool the same gap legitimately includes genuine price impact
        that scales with trade size and pool depth without bound — so the
        execution-rate check cannot distinguish a bad fill from a large-but-fair
        one and **false-blocks legitimate swaps** (CI surfaced a 637 bps fill on
        arb-tricrypto). Volatile-pool min-out protection is the slippage floor; an
        impact-immune spot-price-vs-oracle guard is the correct mechanism and is
        tracked separately. So volatile pools skip this check entirely.
        """
        if pool_info.pool_type in (PoolType.CRYPTOSWAP, PoolType.TRICRYPTO):
            logger.debug(
                "Curve swap oracle guard skipped for volatile pool %s "
                "(execution-rate vs oracle conflates real price impact with manipulation).",
                pool_info.name,
            )
            return None
        threshold_bps = oracle_guard_bps if oracle_guard_bps is not None else DEFAULT_SWAP_ORACLE_DIVERGENCE_BPS
        guard = check_swap_oracle_divergence(
            amount_in=amount_in,
            pool_quoted_out=Decimal(amount_out_estimate) / Decimal(10**token_out_decimals),
            price_ratio=price_ratio if oracle_prices_real else None,
            threshold_bps=threshold_bps,
            strict_when_unmeasured=strict_oracle_guard,
        )
        if not guard.ok:
            if guard.reason == "oracle_unmeasured":
                return (
                    f"Curve swap oracle guard (strict): no oracle price for "
                    f"{token_in_symbol}->{token_out_symbol} on {pool_info.name}; "
                    f"refusing to trade without an independent oracle reference"
                )
            return (
                f"Curve swap blocked: {pool_info.name} quote is {guard.shortfall_bps} bps "
                f"below oracle-fair (threshold {threshold_bps} bps) — pre-moved / displaced "
                f"(stale depeg / persistent imbalance) pool; refusing to build a bad-fill swap"
            )
        if guard.reason == "oracle_unmeasured":
            logger.warning(
                "Curve swap oracle guard unmeasured for %s->%s on %s (no oracle price_ratio); "
                "proceeding with pool-self-referential min-out only (degrade-open).",
                token_in_symbol,
                token_out_symbol,
                pool_info.name,
            )
        return None

    def swap(
        self,
        pool_address: str,
        token_in: str,
        token_out: str,
        amount_in: Decimal,
        slippage_bps: int | None = None,
        recipient: str | None = None,
        price_ratio: Decimal | None = None,
        oracle_guard_bps: int | None = None,
        strict_oracle_guard: bool = False,
        oracle_prices_real: bool = True,
    ) -> SwapResult:
        """Build a swap transaction on a Curve pool.

        Args:
            pool_address: Pool contract address
            token_in: Input token symbol or address
            token_out: Output token symbol or address
            amount_in: Amount of input token (in token units, not wei)
            slippage_bps: Slippage tolerance in basis points (default from config)
            recipient: Address to receive output tokens (default: wallet_address)
            price_ratio: Price of input token / price of output token (e.g., if
                swapping USDT at $1 for WETH at $2500, price_ratio = 1/2500 = 0.0004).
                Required for CryptoSwap/Tricrypto pools; StableSwap pools ignore it.
                When None and pool is CryptoSwap, the swap fails (fail-closed) rather
                than executing with inaccurate slippage protection. Also the
                independent oracle reference for the P0-8 min-out guard below.
            oracle_guard_bps: max bps the pool quote may sit below oracle-fair
                before the swap is blocked as pre-moved (VIB-5439). ``None`` uses
                ``DEFAULT_SWAP_ORACLE_DIVERGENCE_BPS``. Separate from
                ``slippage_bps`` (which buffers the floor below the pool quote).
            strict_oracle_guard: when no oracle ``price_ratio`` is available, fail
                closed instead of degrading open to pool-self-referential min-out.
            oracle_prices_real: whether ``price_ratio`` is a real oracle reference.
                ``False`` (placeholder / offline-price mode) makes the guard treat
                the oracle as unmeasured so it never fires on a known-fake price,
                while ``price_ratio`` still feeds the CryptoSwap slippage estimate.

        Returns:
            SwapResult with transaction data
        """
        try:
            if price_ratio is not None and price_ratio <= 0:
                raise ValueError(f"price_ratio must be positive, got {price_ratio}")

            slippage_bps = slippage_bps or self.config.default_slippage_bps
            recipient = recipient or self.wallet_address

            # Get pool info
            pool_info = self.get_pool_info(pool_address)
            if not pool_info:
                return SwapResult(
                    success=False,
                    error=f"Unknown pool: {pool_address}",
                )

            # Get coin indices
            try:
                i = pool_info.get_coin_index(token_in)
                j = pool_info.get_coin_index(token_out)
            except ValueError as e:
                return SwapResult(success=False, error=str(e))

            # Resolve token addresses
            token_in_address = pool_info.coin_addresses[i]
            token_out_address = pool_info.coin_addresses[j]

            # Get token decimals
            token_in_symbol = pool_info.coins[i]
            token_in_decimals = self._get_token_decimals(token_in_symbol)

            # Convert amount to wei
            amount_in_wei = int(amount_in * Decimal(10**token_in_decimals))

            # Estimate output for min_amount_out slippage protection.
            # VIB-3203 Phase B: also surface this pre-slippage estimate on
            # SwapResult so the IntentCompiler can persist it as
            # ``expected_output_human`` for realized slippage tracking.
            if self._gateway_client is not None or self._rpc_url:
                try:
                    amount_out_estimate = self.quote_swap_output(
                        pool_address=pool_address,
                        token_in=token_in,
                        token_out=token_out,
                        amount_in_wei=amount_in_wei,
                    )
                except Exception as exc:
                    logger.warning(
                        "Curve on-chain quote unavailable for %s (%s -> %s): %s. "
                        "Falling back to deterministic pool estimate.",
                        pool_info.name,
                        token_in,
                        token_out,
                        exc,
                    )
                    amount_out_estimate = self._estimate_swap_output(
                        pool_info,
                        i,
                        j,
                        amount_in_wei,
                        price_ratio=price_ratio,
                    )
            else:
                amount_out_estimate = self._estimate_swap_output(
                    pool_info, i, j, amount_in_wei, price_ratio=price_ratio
                )
            amount_out_minimum = max(1, int(amount_out_estimate * (10000 - slippage_bps) // 10000))
            token_out_decimals = self._get_token_decimals(pool_info.coins[j])

            # P0-8 oracle/MEV min-out guard (VIB-5439): on a StableSwap pool,
            # cross-check the quote against the independent oracle and fail closed
            # on a displaced / depegged pool BEFORE building the tx. Volatile pools
            # are skipped inside the helper (their execution rate legitimately
            # diverges from oracle mid by real price impact).
            guard_error = self._swap_oracle_guard_error(
                pool_info=pool_info,
                token_in_symbol=token_in_symbol,
                token_out_symbol=pool_info.coins[j],
                amount_in=amount_in,
                amount_out_estimate=amount_out_estimate,
                token_out_decimals=token_out_decimals,
                price_ratio=price_ratio,
                oracle_guard_bps=oracle_guard_bps,
                strict_oracle_guard=strict_oracle_guard,
                oracle_prices_real=oracle_prices_real,
            )
            if guard_error is not None:
                return SwapResult(success=False, error=guard_error)

            # Build transactions
            transactions: list[TransactionData] = []

            # Check if input is native ETH
            is_native_input = self._is_native_token(token_in_address)

            # Build approve transaction if needed (skip for native token)
            if not is_native_input:
                transactions.extend(self._build_approve_txs(token_in_address, pool_address, amount_in_wei))

            # Build swap transaction
            swap_tx = self._build_exchange_tx(
                pool_address=pool_address,
                i=i,
                j=j,
                amount_in=amount_in_wei,
                min_amount_out=amount_out_minimum,
                value=amount_in_wei if is_native_input else 0,
                token_in_symbol=token_in_symbol,
                token_out_symbol=pool_info.coins[j],
                pool_type=pool_info.pool_type,
                use_underlying=pool_info.use_underlying,
            )
            transactions.append(swap_tx)

            total_gas = sum(tx.gas_estimate for tx in transactions)

            logger.info(
                f"Built Curve swap: {token_in_symbol} -> {pool_info.coins[j]}, "
                f"pool={pool_info.name}, amount_in={amount_in}"
            )

            return SwapResult(
                success=True,
                transactions=transactions,
                pool_address=pool_address,
                amount_in=amount_in_wei,
                amount_out_minimum=amount_out_minimum,
                amount_out_estimate=amount_out_estimate,
                token_out_decimals=token_out_decimals,
                token_in=token_in_address,
                token_out=token_out_address,
                gas_estimate=total_gas,
            )

        except Exception as e:
            logger.exception(f"Failed to build Curve swap: {e}")
            return SwapResult(success=False, error=str(e))

    # =========================================================================
    # Liquidity Operations
    # =========================================================================

    def add_liquidity(
        self,
        pool_address: str,
        amounts: list[Decimal],
        slippage_bps: int | None = None,
        recipient: str | None = None,
    ) -> LiquidityResult:
        """Build an add_liquidity transaction (LP_OPEN).

        Args:
            pool_address: Pool contract address
            amounts: List of token amounts to deposit (in token units)
            slippage_bps: Slippage tolerance for min LP tokens (default from config).
                For CryptoSwap/Tricrypto (volatile) pools the min_lp floor is the
                build-time on-chain quote × (1 − slippage); if the pool price drifts
                between build and execution by more than ``slippage_bps`` the
                add_liquidity reverts with "Slippage". A revert is fail-safe (no
                loss, vs the old min_lp=0 which could be sandwiched), but a volatile
                or large deposit may need a wider ``slippage_bps`` than the stable
                default to avoid a benign revert (VIB-5441).
            recipient: Address to receive LP tokens (default: wallet_address)

        Returns:
            LiquidityResult with transaction data
        """
        try:
            slippage_bps = slippage_bps or self.config.default_slippage_bps
            recipient = recipient or self.wallet_address

            # Get pool info
            pool_info = self.get_pool_info(pool_address)
            if not pool_info:
                return LiquidityResult(
                    success=False,
                    error=f"Unknown pool: {pool_address}",
                )

            if len(amounts) != pool_info.n_coins:
                return LiquidityResult(
                    success=False,
                    error=f"Expected {pool_info.n_coins} amounts, got {len(amounts)}",
                )

            # Convert amounts to wei
            amounts_wei: list[int] = []
            for idx, amt in enumerate(amounts):
                decimals = self._get_token_decimals(pool_info.coins[idx])
                amounts_wei.append(int(amt * Decimal(10**decimals)))

            # Estimate LP tokens (simplified)
            lp_quote = self._estimate_add_liquidity(pool_info, amounts_wei)
            min_lp_tokens = int(lp_quote * (10000 - slippage_bps) // 10000)
            # Fail closed (VIB-5441): a positive on-chain quote that rounds to <=0
            # after integer slippage math (a tiny quote or a very wide slippage_bps)
            # would re-introduce the unprotected min_lp=0 path this PR removes — an
            # MEV/sandwich theft vector on volatile pools. Refuse rather than ship 0.
            if pool_info.pool_type in (PoolType.CRYPTOSWAP, PoolType.TRICRYPTO) and lp_quote > 0 and min_lp_tokens <= 0:
                return LiquidityResult(
                    success=False,
                    error=(
                        f"CryptoSwap/Tricrypto pool {pool_info.name}: slippage-adjusted min_lp "
                        f"rounded to {min_lp_tokens} from quote {lp_quote} (slippage_bps={slippage_bps}); "
                        f"refusing to ship min_lp=0"
                    ),
                )

            # Build transactions
            transactions: list[TransactionData] = []

            # Build approve transactions for each non-zero amount
            native_value: int = 0
            for amount_wei, coin_addr in zip(amounts_wei, pool_info.coin_addresses, strict=False):
                if amount_wei > 0:
                    if self._is_native_token(coin_addr):
                        native_value = amount_wei
                    else:
                        transactions.extend(self._build_approve_txs(coin_addr, pool_address, amount_wei))

            # Build add_liquidity transaction
            add_liq_tx = self._build_add_liquidity_tx(
                pool_address=pool_address,
                amounts=amounts_wei,
                min_lp_tokens=min_lp_tokens,
                n_coins=pool_info.n_coins,
                value=native_value,
                pool_name=pool_info.name,
                is_ng=pool_info.is_ng,
            )
            transactions.append(add_liq_tx)

            total_gas = sum(tx.gas_estimate for tx in transactions)

            logger.info(f"Built Curve add_liquidity: pool={pool_info.name}, amounts={amounts}, min_lp={min_lp_tokens}")

            return LiquidityResult(
                success=True,
                transactions=transactions,
                pool_address=pool_address,
                operation="add_liquidity",
                amounts=amounts_wei,
                lp_amount=min_lp_tokens,
                gas_estimate=total_gas,
            )

        except Exception as e:
            logger.exception(f"Failed to build add_liquidity: {e}")
            return LiquidityResult(success=False, error=str(e))

    def remove_liquidity(
        self,
        pool_address: str,
        lp_amount: Decimal,
        slippage_bps: int | None = None,
        recipient: str | None = None,
    ) -> LiquidityResult:
        """Build a remove_liquidity transaction (LP_CLOSE, proportional).

        Args:
            pool_address: Pool contract address
            lp_amount: Amount of LP tokens to burn
            slippage_bps: Slippage tolerance for min output (default from config)
            recipient: Address to receive tokens (default: wallet_address)

        Returns:
            LiquidityResult with transaction data
        """
        try:
            slippage_bps = slippage_bps or self.config.default_slippage_bps
            recipient = recipient or self.wallet_address

            # Get pool info
            pool_info = self.get_pool_info(pool_address)
            if not pool_info:
                return LiquidityResult(
                    success=False,
                    error=f"Unknown pool: {pool_address}",
                )

            # Convert LP amount to wei (18 decimals)
            lp_amount_wei = int(lp_amount * Decimal(10**18))

            # Estimate output amounts via on-chain query (or fallback to zeros)
            self._last_estimation_error: str | None = None
            min_amounts = self._estimate_remove_liquidity(pool_info, lp_amount_wei)
            min_amounts = [int(a * (10000 - slippage_bps) // 10000) for a in min_amounts]

            # Guard: fail closed when min_amounts are all zero — proceeding without slippage
            # protection would expose the full withdrawal to sandwich attacks.
            # This can happen when: (a) rpc_url is not configured and LP amount is very small
            # (1% floor rounds to 0 via integer division), or (b) on-chain estimation fails
            # and the fallback also rounds to 0. Either way, refusing is safer than proceeding.
            if all(a == 0 for a in min_amounts):
                reason = self._last_estimation_error or "unknown"
                return LiquidityResult(
                    success=False,
                    error=(
                        f"remove_liquidity: cannot compute slippage protection (min_amounts are all zero). "
                        f"Cause: {reason}. "
                        f"Set CurveConfig.rpc_url for on-chain estimation."
                    ),
                )

            # Build transactions
            transactions: list[TransactionData] = []

            # Approve LP token if needed
            transactions.extend(self._build_approve_txs(pool_info.lp_token, pool_address, lp_amount_wei))

            # Build remove_liquidity transaction
            remove_tx = self._build_remove_liquidity_tx(
                pool_address=pool_address,
                lp_amount=lp_amount_wei,
                min_amounts=min_amounts,
                n_coins=pool_info.n_coins,
                pool_name=pool_info.name,
                is_ng=pool_info.is_ng,
            )
            transactions.append(remove_tx)

            total_gas = sum(tx.gas_estimate for tx in transactions)

            logger.info(f"Built Curve remove_liquidity: pool={pool_info.name}, lp_amount={lp_amount}")

            return LiquidityResult(
                success=True,
                transactions=transactions,
                pool_address=pool_address,
                operation="remove_liquidity",
                amounts=min_amounts,
                lp_amount=lp_amount_wei,
                gas_estimate=total_gas,
            )

        except Exception as e:
            logger.exception(f"Failed to build remove_liquidity: {e}")
            return LiquidityResult(success=False, error=str(e))

    def remove_liquidity_one_coin(
        self,
        pool_address: str,
        lp_amount: Decimal,
        coin_index: int,
        slippage_bps: int | None = None,
        recipient: str | None = None,
    ) -> LiquidityResult:
        """Build a remove_liquidity_one_coin transaction (LP_CLOSE, single-sided).

        Args:
            pool_address: Pool contract address
            lp_amount: Amount of LP tokens to burn
            coin_index: Index of the coin to receive
            slippage_bps: Slippage tolerance (default from config)
            recipient: Address to receive tokens (default: wallet_address)

        Returns:
            LiquidityResult with transaction data
        """
        try:
            slippage_bps = slippage_bps or self.config.default_slippage_bps
            recipient = recipient or self.wallet_address

            # Get pool info
            pool_info = self.get_pool_info(pool_address)
            if not pool_info:
                return LiquidityResult(
                    success=False,
                    error=f"Unknown pool: {pool_address}",
                )

            if coin_index < 0 or coin_index >= pool_info.n_coins:
                return LiquidityResult(
                    success=False,
                    error=f"Invalid coin index: {coin_index}. Pool has {pool_info.n_coins} coins.",
                )

            # Convert LP amount to wei
            lp_amount_wei = int(lp_amount * Decimal(10**18))

            # Estimate output (simplified)
            min_amount = self._estimate_remove_liquidity_one(pool_info, lp_amount_wei, coin_index)
            min_amount = int(min_amount * (10000 - slippage_bps) // 10000)

            # Build transactions
            transactions: list[TransactionData] = []

            # Approve LP token if needed
            transactions.extend(self._build_approve_txs(pool_info.lp_token, pool_address, lp_amount_wei))

            # Build remove_liquidity_one_coin transaction
            remove_tx = self._build_remove_liquidity_one_tx(
                pool_address=pool_address,
                lp_amount=lp_amount_wei,
                coin_index=coin_index,
                min_amount=min_amount,
                coin_symbol=pool_info.coins[coin_index],
                pool_name=pool_info.name,
            )
            transactions.append(remove_tx)

            total_gas = sum(tx.gas_estimate for tx in transactions)

            logger.info(
                f"Built Curve remove_liquidity_one_coin: pool={pool_info.name}, "
                f"lp_amount={lp_amount}, coin={pool_info.coins[coin_index]}"
            )

            # Build amounts list with only the withdrawn coin
            amounts = [0] * pool_info.n_coins
            amounts[coin_index] = min_amount

            return LiquidityResult(
                success=True,
                transactions=transactions,
                pool_address=pool_address,
                operation="remove_liquidity_one_coin",
                amounts=amounts,
                lp_amount=lp_amount_wei,
                gas_estimate=total_gas,
            )

        except Exception as e:
            logger.exception(f"Failed to build remove_liquidity_one_coin: {e}")
            return LiquidityResult(success=False, error=str(e))

    # =========================================================================
    # Metapool Underlying (Zap) Operations — Tier B (VIB-5419)
    # =========================================================================

    def swap_underlying(
        self,
        pool_address: str,
        token_in: str,
        token_out: str,
        amount_in: Decimal,
        slippage_bps: int | None = None,
        recipient: str | None = None,
        price_ratio: Decimal | None = None,
        oracle_guard_bps: int | None = None,
        strict_oracle_guard: bool = False,
        oracle_prices_real: bool = True,
    ) -> SwapResult:
        """Build a metapool underlying swap via ``exchange_underlying``.

        Routes a swap across the COMBINED coin space of a metapool (index 0 =
        meta coin, 1..N = base-pool coins) — e.g. FRAX -> USDC through a
        FRAX/3CRV metapool. ``exchange_underlying`` lives on the metapool
        contract itself (NOT the zap); the metapool transparently routes the
        leg through its base pool.

        Stablecoin-only assumption: every coin on a 3CRV/FRAX-style metapool's
        combined space is a USD stable, so the 1:1 decimal-adjusted estimate
        (the same the StableSwap path uses) is the correct slippage floor, and
        the on-chain ``get_dy_underlying`` quote is preferred when a gateway /
        rpc is wired.
        """
        try:
            slippage_bps = slippage_bps or self.config.default_slippage_bps
            recipient = recipient or self.wallet_address

            pool_info = self.get_pool_info(pool_address)
            if not pool_info:
                return SwapResult(success=False, error=f"Unknown pool: {pool_address}")
            if not pool_info.is_metapool:
                return SwapResult(
                    success=False,
                    error=f"swap_underlying requires a metapool; {pool_info.name} is not one",
                )

            i = pool_info.underlying_coin_index(token_in)
            j = pool_info.underlying_coin_index(token_out)
            if i is None or j is None:
                return SwapResult(
                    success=False,
                    error=(
                        f"Underlying swap {token_in}->{token_out} not on metapool "
                        f"{pool_info.name} combined coin space "
                        f"[{pool_info.coins[0]}]+{pool_info.base_pool_coins}"
                    ),
                )
            if i == j:
                return SwapResult(success=False, error="token_in and token_out resolve to the same coin")

            token_in_address = self._underlying_coin_address(pool_info, i)
            token_out_address = self._underlying_coin_address(pool_info, j)
            token_in_symbol = self._underlying_coin_symbol(pool_info, i)
            token_out_symbol = self._underlying_coin_symbol(pool_info, j)

            token_in_decimals = self._get_token_decimals(token_in_symbol)
            amount_in_wei = int(amount_in * Decimal(10**token_in_decimals))

            # Quote on-chain via get_dy_underlying when available; otherwise use
            # the stable 1:1 decimal-adjusted estimate.
            amount_out_estimate = self._estimate_underlying_swap_output(
                pool_info, i, j, amount_in_wei, token_in_symbol, token_out_symbol
            )
            amount_out_minimum = max(1, int(amount_out_estimate * (10000 - slippage_bps) // 10000))
            token_out_decimals = self._get_token_decimals(token_out_symbol)

            # P0-8 oracle/MEV min-out guard (VIB-5439). The combined coin space is
            # all USD stables and the underlying estimate (get_dy_underlying or a
            # 1:1 decimal-adjust) is independent of the oracle either way, so the
            # cross-check always applies — it flags a depegged underlying / moved
            # metapool before the tx is built.
            guard_error = self._swap_oracle_guard_error(
                pool_info=pool_info,
                token_in_symbol=token_in_symbol,
                token_out_symbol=token_out_symbol,
                amount_in=amount_in,
                amount_out_estimate=amount_out_estimate,
                token_out_decimals=token_out_decimals,
                price_ratio=price_ratio,
                oracle_guard_bps=oracle_guard_bps,
                strict_oracle_guard=strict_oracle_guard,
                oracle_prices_real=oracle_prices_real,
            )
            if guard_error is not None:
                return SwapResult(success=False, error=guard_error)

            transactions: list[TransactionData] = []
            transactions.extend(self._build_approve_txs(token_in_address, pool_address, amount_in_wei))

            # exchange_underlying(int128 i, int128 j, uint256 dx, uint256 min_dy)
            calldata = (
                EXCHANGE_UNDERLYING_SELECTOR
                + self._pad_int128(i)
                + self._pad_int128(j)
                + self._pad_uint256(amount_in_wei)
                + self._pad_uint256(amount_out_minimum)
            )
            transactions.append(
                TransactionData(
                    to=pool_address,
                    value=0,
                    data=calldata,
                    gas_estimate=CURVE_GAS_ESTIMATES["exchange_underlying_metapool"],
                    description=f"Curve metapool underlying swap {token_in_symbol} -> {token_out_symbol}",
                    tx_type="swap",
                )
            )

            total_gas = sum(tx.gas_estimate for tx in transactions)
            logger.info(
                "Built Curve metapool underlying swap: %s(%d) -> %s(%d), pool=%s, amount_in=%s",
                token_in_symbol,
                i,
                token_out_symbol,
                j,
                pool_info.name,
                amount_in,
            )
            return SwapResult(
                success=True,
                transactions=transactions,
                pool_address=pool_address,
                amount_in=amount_in_wei,
                amount_out_minimum=amount_out_minimum,
                amount_out_estimate=amount_out_estimate,
                token_out_decimals=token_out_decimals,
                token_in=token_in_address,
                token_out=token_out_address,
                gas_estimate=total_gas,
            )
        except Exception as e:
            logger.exception(f"Failed to build Curve metapool underlying swap: {e}")
            return SwapResult(success=False, error=str(e))

    def add_liquidity_underlying(
        self,
        pool_address: str,
        underlying_amounts: list[Decimal],
        slippage_bps: int | None = None,
        recipient: str | None = None,
    ) -> LiquidityResult:
        """Build a metapool deposit over the COMBINED coin space via the zap.

        ``underlying_amounts`` is indexed in COMBINED order: index 0 = meta coin,
        indices 1..N = base-pool coins (DAI/USDC/USDT). The generic 3CRV
        DepositZap's ABI takes the metapool as the first argument:
        ``add_liquidity(address _pool, uint256[N+1] _deposit, uint256 _min_mint)``.
        It deposits the base coins into the base pool (minting the base-LP), then
        the base-LP plus the meta coin into the metapool — a user only has to
        hold/approve the underlying coins.
        """
        try:
            slippage_bps = slippage_bps or self.config.default_slippage_bps
            recipient = recipient or self.wallet_address

            pool_info = self.get_pool_info(pool_address)
            if not pool_info:
                return LiquidityResult(success=False, error=f"Unknown pool: {pool_address}")
            zap, combined_len = self._require_metapool_zap(pool_info)

            if len(underlying_amounts) != combined_len:
                return LiquidityResult(
                    success=False,
                    error=(
                        f"underlying_amounts has {len(underlying_amounts)} entries but metapool "
                        f"'{pool_info.name}' combined coin space has {combined_len} "
                        f"([{pool_info.coins[0]}]+{pool_info.base_pool_coins})"
                    ),
                )

            amounts_wei: list[int] = []
            for idx, amt in enumerate(underlying_amounts):
                decimals = self._get_token_decimals(self._underlying_coin_symbol(pool_info, idx))
                amounts_wei.append(int(amt * Decimal(10**decimals)))

            min_lp_tokens = self._estimate_add_liquidity_underlying(pool_info, zap, amounts_wei)
            min_lp_tokens = int(min_lp_tokens * (10000 - slippage_bps) // 10000)

            transactions: list[TransactionData] = []
            for idx, amount_wei in enumerate(amounts_wei):
                if amount_wei > 0:
                    coin_addr = self._underlying_coin_address(pool_info, idx)
                    transactions.extend(self._build_approve_txs(coin_addr, zap, amount_wei))

            # add_liquidity(address _pool, uint256[4] _deposit_amounts, uint256 _min_mint_amount)
            calldata = ZAP_ADD_LIQUIDITY_4_SELECTOR + self._pad_address(pool_address)
            for amount in amounts_wei:
                calldata += self._pad_uint256(amount)
            calldata += self._pad_uint256(min_lp_tokens)
            transactions.append(
                TransactionData(
                    to=zap,
                    value=0,
                    data=calldata,
                    gas_estimate=CURVE_GAS_ESTIMATES["metapool_zap_add_liquidity"],
                    description=f"Add underlying liquidity to Curve metapool {pool_info.name} (zap)",
                    tx_type="add_liquidity",
                )
            )

            total_gas = sum(tx.gas_estimate for tx in transactions)
            logger.info(
                "Built Curve metapool zap add_liquidity: pool=%s, underlying_amounts=%s, min_lp=%s",
                pool_info.name,
                underlying_amounts,
                min_lp_tokens,
            )
            return LiquidityResult(
                success=True,
                transactions=transactions,
                pool_address=pool_address,
                operation="add_liquidity_underlying",
                amounts=amounts_wei,
                lp_amount=min_lp_tokens,
                gas_estimate=total_gas,
            )
        except Exception as e:
            logger.exception(f"Failed to build metapool add_liquidity_underlying: {e}")
            return LiquidityResult(success=False, error=str(e))

    def remove_liquidity_underlying(
        self,
        pool_address: str,
        lp_amount: Decimal,
        slippage_bps: int | None = None,
        recipient: str | None = None,
    ) -> LiquidityResult:
        """Build a metapool proportional withdrawal to underlying coins via the zap.

        Burns ``lp_amount`` metapool LP and returns the COMBINED underlying coins
        (meta coin + base-pool coins) using the generic zap's
        ``remove_liquidity(address _pool, uint256 _amount, uint256[N+1] _min_amounts)``.
        The min-amounts vector is derived from the metapool's native proportional
        split (meta coin + base-LP), then the base-LP leg is decomposed across the
        base pool's coins by its on-chain reserves. When the on-chain reads are
        unavailable, fails closed (no slippage floor) — mirrors the native
        ``remove_liquidity`` guard.
        """
        try:
            slippage_bps = slippage_bps or self.config.default_slippage_bps
            recipient = recipient or self.wallet_address

            pool_info = self.get_pool_info(pool_address)
            if not pool_info:
                return LiquidityResult(success=False, error=f"Unknown pool: {pool_address}")
            zap, combined_len = self._require_metapool_zap(pool_info)

            lp_amount_wei = int(lp_amount * Decimal(10**18))

            self._last_estimation_error = None
            min_amounts = self._estimate_remove_liquidity_underlying(pool_info, lp_amount_wei)
            min_amounts = [int(a * (10000 - slippage_bps) // 10000) for a in min_amounts]

            if all(a == 0 for a in min_amounts):
                reason = self._last_estimation_error or "unknown"
                return LiquidityResult(
                    success=False,
                    error=(
                        f"remove_liquidity_underlying: cannot compute slippage protection "
                        f"(min_amounts are all zero). Cause: {reason}. "
                        f"Set CurveConfig.gateway_client for on-chain estimation."
                    ),
                )

            transactions: list[TransactionData] = []
            # The zap pulls the metapool LP from the caller, so approve the LP
            # token (== metapool address) to the zap.
            transactions.extend(self._build_approve_txs(pool_info.lp_token, zap, lp_amount_wei))

            # remove_liquidity(address _pool, uint256 _amount, uint256[4] _min_amounts)
            calldata = (
                ZAP_REMOVE_LIQUIDITY_4_SELECTOR + self._pad_address(pool_address) + self._pad_uint256(lp_amount_wei)
            )
            for min_amount in min_amounts:
                calldata += self._pad_uint256(min_amount)
            transactions.append(
                TransactionData(
                    to=zap,
                    value=0,
                    data=calldata,
                    gas_estimate=CURVE_GAS_ESTIMATES["metapool_zap_remove_liquidity"],
                    description=f"Remove underlying liquidity from Curve metapool {pool_info.name} (zap)",
                    tx_type="remove_liquidity",
                )
            )

            total_gas = sum(tx.gas_estimate for tx in transactions)
            logger.info(
                "Built Curve metapool zap remove_liquidity: pool=%s, lp_amount=%s",
                pool_info.name,
                lp_amount,
            )
            return LiquidityResult(
                success=True,
                transactions=transactions,
                pool_address=pool_address,
                operation="remove_liquidity_underlying",
                amounts=min_amounts,
                lp_amount=lp_amount_wei,
                gas_estimate=total_gas,
            )
        except Exception as e:
            logger.exception(f"Failed to build metapool remove_liquidity_underlying: {e}")
            return LiquidityResult(success=False, error=str(e))

    # ---- Metapool underlying helpers -------------------------------------

    @staticmethod
    def _require_metapool_zap(pool_info: PoolInfo) -> tuple[str, int]:
        """Return ``(zap_address, combined_coin_count)`` or raise for a non-zap metapool."""
        if not pool_info.is_metapool:
            raise ValueError(f"{pool_info.name} is not a metapool")
        if not pool_info.zap_address:
            raise ValueError(f"metapool {pool_info.name} has no zap_address configured")
        combined_len = 1 + len(pool_info.base_pool_coins or [])
        return pool_info.zap_address, combined_len

    def _underlying_coin_symbol(self, pool_info: PoolInfo, combined_index: int) -> str:
        """Symbol for a COMBINED-space index (0 = meta coin, 1..N = base coins)."""
        if combined_index == 0:
            return pool_info.coins[0]
        return (pool_info.base_pool_coins or [])[combined_index - 1]

    def _underlying_coin_address(self, pool_info: PoolInfo, combined_index: int) -> str:
        """Address for a COMBINED-space index (0 = meta coin, 1..N = base coins)."""
        if combined_index == 0:
            return pool_info.coin_addresses[0]
        return (pool_info.base_pool_coin_addresses or [])[combined_index - 1]

    def _estimate_underlying_swap_output(
        self,
        pool_info: PoolInfo,
        i: int,
        j: int,
        amount_in: int,
        token_in_symbol: str,
        token_out_symbol: str,
    ) -> int:
        """Estimate an underlying-swap output (prefer on-chain get_dy_underlying)."""
        if self._gateway_client is not None or self._rpc_url:
            try:
                calldata = (
                    GET_DY_UNDERLYING_SELECTOR
                    + self._pad_int128(i)
                    + self._pad_int128(j)
                    + self._pad_uint256(amount_in)
                )
                amount_out = eth_call_uint256(
                    chain=self.chain,
                    to=pool_info.address,
                    data=calldata,
                    rpc_url=self._rpc_url,
                    gateway_client=self._gateway_client,
                    timeout=10.0,
                )
                if amount_out is not None and amount_out > 0:
                    return amount_out
            except Exception as exc:  # noqa: BLE001 — fall back to the stable 1:1 estimate
                logger.warning(
                    "Curve metapool get_dy_underlying unavailable for %s (%s -> %s): %s; "
                    "falling back to decimal-adjusted stable estimate",
                    pool_info.name,
                    token_in_symbol,
                    token_out_symbol,
                    exc,
                )
        # Combined coin space of a 3CRV/FRAX metapool is all USD stables -> 1:1.
        in_decimals = self._get_token_decimals(token_in_symbol)
        out_decimals = self._get_token_decimals(token_out_symbol)
        decimal_diff = out_decimals - in_decimals
        if decimal_diff > 0:
            return amount_in * (10**decimal_diff)
        if decimal_diff < 0:
            return amount_in // (10 ** abs(decimal_diff))
        return amount_in

    def _estimate_add_liquidity_underlying(self, pool_info: PoolInfo, zap: str, amounts: list[int]) -> int:
        """Estimate metapool LP minted for a combined-space deposit via the zap.

        Prefers the zap's ``calc_token_amount(address,uint256[4],bool)`` on-chain
        quote; falls back to the deposit-sum / virtual_price stable estimate
        (the combined coins are all USD-denominated 1.0 stables).
        """
        if self._gateway_client is not None or self._rpc_url:
            try:
                calldata = ZAP_CALC_TOKEN_AMOUNT_4_SELECTOR + self._pad_address(pool_info.address)
                for amount in amounts:
                    calldata += self._pad_uint256(amount)
                calldata += self._pad_uint256(1)  # is_deposit = True
                minted = eth_call_uint256(
                    chain=self.chain,
                    to=zap,
                    data=calldata,
                    rpc_url=self._rpc_url,
                    gateway_client=self._gateway_client,
                    timeout=10.0,
                )
                if minted is not None and minted > 0:
                    return minted
            except Exception as exc:  # noqa: BLE001 — fall back to naive estimate
                logger.warning(
                    "Curve metapool zap calc_token_amount unavailable for %s (%s); naive estimate",
                    pool_info.name,
                    exc,
                )
        total = 0
        for idx, amount in enumerate(amounts):
            decimals = self._get_token_decimals(self._underlying_coin_symbol(pool_info, idx))
            total += amount * (10 ** (18 - decimals))
        return int(Decimal(total) / pool_info.virtual_price)

    def _estimate_remove_liquidity_underlying(self, pool_info: PoolInfo, lp_amount: int) -> list[int]:
        """Estimate combined-space min amounts for a proportional metapool withdrawal.

        Splits the burned LP across the metapool's NATIVE coins (meta + base-LP)
        by on-chain reserves, then decomposes the base-LP leg across the base
        pool's underlying coins by ITS reserves — yielding the combined vector
        the zap returns: [meta, base_coin_0, base_coin_1, ...]. Returns all-zeros
        (fail closed) when on-chain reads are unavailable.
        """
        combined_len = 1 + len(pool_info.base_pool_coins or [])
        zero = [0] * combined_len
        if self._gateway_client is None and not self._rpc_url:
            self._last_estimation_error = "gateway_client or rpc_url not configured"
            return zero
        try:
            # 1. Native proportional split of the metapool: [meta, base-LP].
            native = self._query_proportional_amounts_onchain(pool_info, lp_amount)
            meta_amount = native[0]
            base_lp_amount = native[1]
            # 2. Decompose the base-LP leg across the base pool's coins by reserves.
            base_amounts = self._query_base_pool_underlying_amounts(pool_info, base_lp_amount)
            return [meta_amount, *base_amounts]
        except Exception as e:  # noqa: BLE001
            self._last_estimation_error = str(e)
            logger.warning(
                "remove_liquidity_underlying: on-chain estimation failed for %s: %s -- "
                "falling back to all-zeros (no slippage protection)",
                pool_info.name,
                e,
            )
            return zero

    def _query_base_pool_underlying_amounts(self, pool_info: PoolInfo, base_lp_amount: int) -> list[int]:
        """Proportional share of base-pool reserves for ``base_lp_amount`` base-LP tokens.

        Reuses the proportional-amounts query against the base pool (3pool) by
        building a transient PoolInfo for it: base_lp / base_pool.totalSupply()
        times each base reserve. The base LP token is the base pool's coins[1]
        (3CRV) on the metapool, i.e. ``coin_addresses[1]``.
        """
        base_pool_addr = pool_info.base_pool or ""
        base_addrs = pool_info.base_pool_coin_addresses or []
        # A metapool's native coins are [meta coin, base-LP token], so the base LP
        # token is coins[1]. Guard the index access before reading it so a
        # misconfigured pool fails loudly with a clear message, not an IndexError.
        if not base_pool_addr or not base_addrs or len(pool_info.coin_addresses) < 2:
            raise ValueError(f"metapool {pool_info.name} missing base-pool metadata")
        base_lp_token = pool_info.coin_addresses[1]  # 3CRV is the metapool's coin 1
        base_info = PoolInfo(
            address=base_pool_addr,
            lp_token=base_lp_token,
            coins=list(pool_info.base_pool_coins or []),
            coin_addresses=list(base_addrs),
            pool_type=PoolType.STABLESWAP,
            n_coins=len(base_addrs),
            name=f"{pool_info.name}:base_pool",
        )
        return self._query_proportional_amounts_onchain(base_info, base_lp_amount)

    # =========================================================================
    # Transaction Building
    # =========================================================================

    def _build_exchange_tx(
        self,
        pool_address: str,
        i: int,
        j: int,
        amount_in: int,
        min_amount_out: int,
        value: int = 0,
        token_in_symbol: str = "",
        token_out_symbol: str = "",
        pool_type: PoolType = PoolType.STABLESWAP,
        use_underlying: bool = False,
    ) -> TransactionData:
        """Build exchange transaction.

        StableSwap:           exchange(int128 i, int128 j, uint256 dx, uint256 min_dy)
        CryptoSwap/Tricrypto: exchange(uint256 i, uint256 j, uint256 dx, uint256 min_dy)
        Aave-type (underlying): exchange_underlying(int128 i, int128 j, uint256 dx, uint256 min_dy)
        """
        if use_underlying:
            # Aave-type pools (e.g. Polygon am3pool): swap underlying tokens via exchange_underlying()
            selector = EXCHANGE_UNDERLYING_SELECTOR
            pad_index = self._pad_int128
        elif pool_type in (PoolType.CRYPTOSWAP, PoolType.TRICRYPTO):
            # CryptoSwap and Tricrypto pools use uint256 indices
            selector = EXCHANGE_UINT256_SELECTOR
            pad_index = self._pad_uint256
        else:
            # StableSwap pools use int128 indices
            selector = EXCHANGE_SELECTOR
            pad_index = self._pad_int128

        calldata = (
            selector + pad_index(i) + pad_index(j) + self._pad_uint256(amount_in) + self._pad_uint256(min_amount_out)
        )

        return TransactionData(
            to=pool_address,
            value=value,
            data=calldata,
            gas_estimate=CURVE_GAS_ESTIMATES["exchange_underlying" if use_underlying else "exchange"],
            description=f"Curve swap {token_in_symbol} -> {token_out_symbol}",
            tx_type="swap",
        )

    def _build_add_liquidity_tx(
        self,
        pool_address: str,
        amounts: list[int],
        min_lp_tokens: int,
        n_coins: int,
        value: int = 0,
        pool_name: str = "",
        is_ng: bool = False,
    ) -> TransactionData:
        """Build add_liquidity transaction.

        Legacy:        add_liquidity(uint256[N_COINS] amounts, uint256 min_mint_amount)
        StableSwap NG: add_liquidity(uint256[] amounts, uint256 min_mint_amount)
        """
        gas_estimate = CURVE_GAS_ESTIMATES.get(f"add_liquidity_{n_coins}", CURVE_GAS_ESTIMATES["add_liquidity_4"])

        if is_ng:
            # Dynamic-array ABI: head = [offset_to_amounts, min_mint]; tail = [length, *amounts]
            # offset = 0x40 (two head slots × 32 bytes).
            calldata = ADD_LIQUIDITY_DYN_SELECTOR
            calldata += self._pad_uint256(0x40)
            calldata += self._pad_uint256(min_lp_tokens)
            calldata += self._pad_uint256(n_coins)
            for amount in amounts:
                calldata += self._pad_uint256(amount)
        else:
            if n_coins == 2:
                selector = ADD_LIQUIDITY_2_SELECTOR
            elif n_coins == 3:
                selector = ADD_LIQUIDITY_3_SELECTOR
            elif n_coins == 4:
                selector = ADD_LIQUIDITY_4_SELECTOR
            else:
                raise ValueError(f"Unsupported n_coins={n_coins} for add_liquidity (expected 2, 3, or 4)")

            calldata = selector
            for amount in amounts:
                calldata += self._pad_uint256(amount)
            calldata += self._pad_uint256(min_lp_tokens)

        return TransactionData(
            to=pool_address,
            value=value,
            data=calldata,
            gas_estimate=gas_estimate,
            description=f"Add liquidity to Curve {pool_name}",
            tx_type="add_liquidity",
        )

    def _build_remove_liquidity_tx(
        self,
        pool_address: str,
        lp_amount: int,
        min_amounts: list[int],
        n_coins: int,
        pool_name: str = "",
        is_ng: bool = False,
    ) -> TransactionData:
        """Build remove_liquidity transaction.

        Legacy:        remove_liquidity(uint256 _amount, uint256[N_COINS] min_amounts)
        StableSwap NG: remove_liquidity(uint256 _amount, uint256[] min_amounts)
        """
        if is_ng:
            # Dynamic-array ABI: head = [_amount, offset_to_min_amounts]; tail = [length, *min_amounts]
            # offset = 0x40 (two head slots × 32 bytes).
            calldata = REMOVE_LIQUIDITY_DYN_SELECTOR
            calldata += self._pad_uint256(lp_amount)
            calldata += self._pad_uint256(0x40)
            calldata += self._pad_uint256(n_coins)
            for min_amount in min_amounts:
                calldata += self._pad_uint256(min_amount)
        else:
            if n_coins == 2:
                selector = REMOVE_LIQUIDITY_2_SELECTOR
            elif n_coins == 3:
                selector = REMOVE_LIQUIDITY_3_SELECTOR
            elif n_coins == 4:
                selector = REMOVE_LIQUIDITY_4_SELECTOR
            else:
                raise ValueError(f"Unsupported n_coins={n_coins} for remove_liquidity (expected 2, 3, or 4)")

            calldata = selector + self._pad_uint256(lp_amount)
            for min_amount in min_amounts:
                calldata += self._pad_uint256(min_amount)

        return TransactionData(
            to=pool_address,
            value=0,
            data=calldata,
            gas_estimate=CURVE_GAS_ESTIMATES["remove_liquidity"],
            description=f"Remove liquidity from Curve {pool_name}",
            tx_type="remove_liquidity",
        )

    def _build_remove_liquidity_one_tx(
        self,
        pool_address: str,
        lp_amount: int,
        coin_index: int,
        min_amount: int,
        coin_symbol: str = "",
        pool_name: str = "",
    ) -> TransactionData:
        """Build remove_liquidity_one_coin transaction.

        remove_liquidity_one_coin(uint256 _token_amount, int128 i, uint256 _min_amount)
        """
        calldata = (
            REMOVE_LIQUIDITY_ONE_SELECTOR
            + self._pad_uint256(lp_amount)
            + self._pad_int128(coin_index)
            + self._pad_uint256(min_amount)
        )

        return TransactionData(
            to=pool_address,
            value=0,
            data=calldata,
            gas_estimate=CURVE_GAS_ESTIMATES["remove_liquidity_one_coin"],
            description=f"Remove {coin_symbol} from Curve {pool_name}",
            tx_type="remove_liquidity",
        )

    def _build_approve_txs(
        self,
        token_address: str,
        spender: str,
        amount: int,
    ) -> list[TransactionData]:
        """Build the ERC-20 approve transaction(s) needed to spend ``amount`` (VIB-5442).

        Returns an empty list when the current allowance already covers ``amount``,
        a single ``approve(MAX)`` when there is no existing allowance, or a
        ``approve(0)`` + ``approve(MAX)`` pair when an existing NON-ZERO allowance
        must be changed — USDT-class tokens revert on a non-zero → non-zero
        ``approve`` (``require(value == 0 || allowance == 0)``), which silently
        kills the whole bundle. The current allowance is **seeded from on-chain
        ``allowance()``** (not assumed 0), so a token already approved in a prior
        run is not needlessly (and, for USDT, revertingly) re-approved.

        Args:
            token_address: Token to approve
            spender: Address to approve
            amount: Amount that must be spendable

        Returns:
            Zero, one, or two ``TransactionData`` (reset + approve) in order.
        """
        cache_key = self._allowance_cache_key(token_address, spender)
        current = self._current_allowance(cache_key, token_address, spender)
        if current is not None and current >= amount:
            logger.debug("Sufficient allowance for %s (%d >= %d)", token_address, current, amount)
            return []

        txs: list[TransactionData] = []
        # Reset-to-zero before changing the allowance unless we have POSITIVELY
        # confirmed it is zero (a successful on-chain read). Emitted when the
        # allowance is non-zero, OR ``None`` (UNKNOWN — read failed, or no transport
        # to read with): ``approve(0)`` never reverts on USDT (the ``value == 0``
        # branch), so failing toward a reset is the always-safe default and costs
        # only one extra tx — rather than a lone ``approve(MAX)`` that would revert
        # on a USDT that turns out to still hold a non-zero allowance.
        if current is None or current > 0:
            txs.append(self._single_approve_tx(token_address, spender, 0))
        txs.append(self._single_approve_tx(token_address, spender, MAX_UINT256))
        self._allowance_cache[cache_key] = MAX_UINT256
        return txs

    def _current_allowance(self, cache_key: str, token_address: str, spender: str) -> int | None:
        """Return the current allowance, or ``None`` when it cannot be confirmed.

        Returns the cached value when present (set by a prior approve in this bundle
        or by ``set_allowance`` in tests). Otherwise queries ``allowance(wallet,
        spender)`` via the gateway / RPC and returns the on-chain value. Returns
        ``None`` whenever the allowance cannot be **positively confirmed** — the
        read failed (RPC error / no result) OR no transport is configured to read
        with — so the caller fails toward a safe reset rather than assuming zero
        and emitting a lone ``approve(MAX)`` that could revert on a USDT-class token.
        """
        if cache_key in self._allowance_cache:
            return self._allowance_cache[cache_key]
        if self._gateway_client is not None or self._rpc_url:
            try:
                calldata = (
                    ERC20_ALLOWANCE_SELECTOR + self._pad_address(self.wallet_address) + self._pad_address(spender)
                )
                onchain = eth_call_uint256(
                    chain=self.chain,
                    to=token_address,
                    data=calldata,
                    rpc_url=self._rpc_url,
                    gateway_client=self._gateway_client,
                    timeout=10.0,
                )
                if onchain is not None:
                    self._allowance_cache[cache_key] = onchain
                    return onchain
            except Exception as exc:  # noqa: BLE001 — unknown allowance on any read failure
                logger.debug("On-chain allowance read failed for %s: %s; treating as unknown", token_address, exc)
        return None  # could not confirm allowance → caller resets to be safe

    @staticmethod
    def _allowance_cache_key(token_address: str, spender: str) -> str:
        """Case-normalized allowance cache key (token:spender), lowercased so a
        checksummed and a lowercase address hit the same cache entry — a casing
        miss would otherwise re-approve a USDT and revert."""
        return f"{token_address.lower()}:{spender.lower()}"

    def _single_approve_tx(self, token_address: str, spender: str, value: int) -> TransactionData:
        """Build one ERC-20 ``approve(spender, value)`` transaction."""
        calldata = ERC20_APPROVE_SELECTOR + self._pad_address(spender) + self._pad_uint256(value)
        # _get_token_symbol falls back to truncated address for unresolved tokens (e.g., 3CRV)
        token_symbol = self._get_token_symbol(token_address)
        action = "Reset approval for" if value == 0 else "Approve"
        return TransactionData(
            to=token_address,
            value=0,
            data=calldata,
            gas_estimate=CURVE_GAS_ESTIMATES["approve"],
            description=f"{action} {token_symbol} for Curve",
            tx_type="approve",
        )

    # =========================================================================
    # Estimation Methods
    # =========================================================================

    def _estimate_swap_output(
        self,
        pool_info: PoolInfo,
        i: int,
        j: int,
        amount_in: int,
        price_ratio: Decimal | None = None,
    ) -> int:
        """Estimate swap output amount for min_amount_out calculation.

        For StableSwap pools: assumes 1:1 price ratio, adjusts for decimals.
        For CryptoSwap/Tricrypto pools: requires price_ratio from the compiler
        (which has access to oracle prices). If price_ratio is not provided,
        raises ValueError (fail closed) — decimal-only adjustment is wrong for
        volatile pairs and would produce astronomically incorrect min_amount_out.

        Args:
            pool_info: Pool metadata
            i: Input coin index
            j: Output coin index
            amount_in: Input amount in wei (input token decimals)
            price_ratio: price_in / price_out ratio. E.g., USDT($1)->WETH($2500)
                gives price_ratio=0.0004. When provided, the estimate is:
                amount_in * price_ratio * (10^(out_decimals - in_decimals))

        Returns:
            Estimated output amount in wei (output token decimals)
        """
        in_decimals = self._get_token_decimals(pool_info.coins[i])
        out_decimals = self._get_token_decimals(pool_info.coins[j])
        decimal_diff = out_decimals - in_decimals

        if pool_info.pool_type == PoolType.STABLESWAP:
            # Stablecoins: assume 1:1, adjust for decimals only
            if decimal_diff > 0:
                return amount_in * (10**decimal_diff)
            elif decimal_diff < 0:
                return amount_in // (10 ** abs(decimal_diff))
            return amount_in

        # CryptoSwap / Tricrypto: volatile pairs need price-based estimation
        if price_ratio is not None:
            # price_ratio = price_in / price_out
            # expected_output_tokens = amount_in_tokens * price_ratio
            # Convert: amount_in is in input wei, output must be in output wei
            # amount_out_wei = amount_in_wei * price_ratio * 10^(out_decimals - in_decimals)
            estimate = Decimal(amount_in) * price_ratio
            if decimal_diff > 0:
                estimate = estimate * Decimal(10**decimal_diff)
            elif decimal_diff < 0:
                estimate = estimate / Decimal(10 ** abs(decimal_diff))
            return int(estimate)

        # Fail closed: CryptoSwap pools swap volatile assets with very different prices.
        # Without price_ratio, decimal-only adjustment produces astronomically wrong
        # min_amount_out values (e.g. 100 USDT -> WETH would set min_amount_out to
        # 100*10^12 wei = ~100 billion WETH, guaranteeing a revert). The compiler
        # always provides price_ratio from oracle prices; reaching this path means
        # the price oracle was unavailable. Fail closed rather than execute unprotected.
        raise ValueError(
            f"CryptoSwap pool {pool_info.name} ({pool_info.coins[i]} -> {pool_info.coins[j]}): "
            "price_ratio is required for accurate slippage protection but was not provided. "
            "Ensure price oracle data is available for both tokens before swapping volatile pairs."
        )

    def quote_swap_output(
        self,
        *,
        pool_address: str,
        token_in: str,
        token_out: str,
        amount_in_wei: int,
    ) -> int:
        """Quote a Curve exact-input swap with the pool's on-chain quote method."""
        if self._gateway_client is None and not self._rpc_url:
            raise ValueError("Curve on-chain swap quote requires either a gateway client or rpc_url")
        if amount_in_wei <= 0:
            raise ValueError(f"amount_in_wei must be positive, got {amount_in_wei}")

        pool_info = self.get_pool_info(pool_address)
        if not pool_info:
            raise ValueError(f"Unknown Curve pool: {pool_address}")
        i = pool_info.get_coin_index(token_in)
        j = pool_info.get_coin_index(token_out)
        return self._query_swap_output_onchain(pool_info, i, j, amount_in_wei)

    def _query_swap_output_onchain(self, pool_info: PoolInfo, i: int, j: int, amount_in: int) -> int:
        """Query Curve get_dy for a swap output quote."""
        if pool_info.use_underlying:
            selector = GET_DY_UNDERLYING_SELECTOR
            pad_index = self._pad_int128
        elif pool_info.pool_type in (PoolType.CRYPTOSWAP, PoolType.TRICRYPTO):
            selector = GET_DY_UINT256_SELECTOR
            pad_index = self._pad_uint256
        else:
            selector = GET_DY_SELECTOR
            pad_index = self._pad_int128

        calldata = selector + pad_index(i) + pad_index(j) + self._pad_uint256(amount_in)
        amount_out = eth_call_uint256(
            chain=self.chain,
            to=pool_info.address,
            data=calldata,
            rpc_url=self._rpc_url,
            gateway_client=self._gateway_client,
            timeout=10.0,
        )
        if amount_out is None:
            raise ValueError(f"Curve get_dy returned no result for {pool_info.name}")
        if amount_out <= 0:
            raise ValueError(f"Curve get_dy returned non-positive amount_out for {pool_info.name}: {amount_out}")
        return amount_out

    def _estimate_add_liquidity(self, pool_info: PoolInfo, amounts: list[int]) -> int:
        """Estimate LP tokens from add_liquidity.

        For StableSwap pools: divides total deposit by virtual_price.
        Mature pools have virtual_price > 1.0 because fees increase LP token value.
        The sum/virtual_price formula works for stablecoin pools because deposit
        value is proportional to LP supply.

        For StableSwap NG pools (``pool_info.is_ng``): if a gateway client or
        RPC URL is configured, query ``calc_token_amount(uint256[], bool)``
        on-chain for an accurate quote. The naive sum/virtual_price estimate
        is too tight for NG pools because their imbalance-fee model means the
        actual minted amount is meaningfully below the deposit value on small
        single-asset-heavy deposits, and the configured ``virtual_price``
        drifts as fees accrue (VIB-4836).

        For CryptoSwap/Tricrypto pools (VIB-5441 / audit P1-7): query
        ``calc_token_amount`` on-chain and return the real LP-mint quote. A
        volatile-asset pool tracks LP tokens as a share of the D-invariant (it
        depends on reserves, A, gamma, and current prices), so there is no safe
        static estimate — the previous behaviour returned ``min_lp=0`` (accept any
        output), an MEV/sandwich theft vector. This method now **fails closed**:
        with no gateway/rpc, or if the on-chain quote cannot be obtained, it
        raises rather than returning 0, so ``add_liquidity`` rejects the deposit
        instead of shipping an unprotected ``min_lp=0``.
        """
        if pool_info.pool_type in (PoolType.CRYPTOSWAP, PoolType.TRICRYPTO):
            if self._gateway_client is None and not self._rpc_url:
                raise ValueError(
                    f"CryptoSwap/Tricrypto pool {pool_info.name}: cannot compute min_lp without a "
                    "gateway client or rpc_url; refusing to ship min_lp=0 (MEV theft vector). "
                    "Configure CurveConfig.gateway_client to enable the on-chain calc_token_amount quote."
                )
            # Propagates on query failure — the caller (add_liquidity) fails closed.
            return self._query_calc_token_amount_crypto_onchain(pool_info, amounts)

        if pool_info.is_ng and (self._gateway_client is not None or self._rpc_url):
            try:
                return self._query_calc_token_amount_ng_onchain(pool_info, amounts)
            except Exception as exc:  # noqa: BLE001 — fall back to naive estimate
                logger.warning(
                    "StableSwap NG calc_token_amount query failed for pool %s (%s); falling back to naive estimate",
                    pool_info.name,
                    exc,
                )

        total = 0
        for i, amount in enumerate(amounts):
            decimals = self._get_token_decimals(pool_info.coins[i])
            # Normalize to 18 decimals
            normalized = amount * (10 ** (18 - decimals))
            total += normalized

        # Adjust for virtual_price: each LP token is worth virtual_price underlying
        total = int(Decimal(total) / pool_info.virtual_price)

        return total

    def _query_calc_token_amount_crypto_onchain(self, pool_info: PoolInfo, amounts: list[int]) -> int:
        """Query ``calc_token_amount`` on a CryptoSwap/Tricrypto pool (VIB-5441).

        Returns the real LP-mint quote so ``_estimate_add_liquidity`` can derive a
        non-zero ``min_lp`` for a volatile deposit. The deposit array is a fixed
        ``uint256[N]`` encoded inline; we probe the bool-carrying selector first
        then the deposit-only one (see ``CRYPTO_CALC_TOKEN_AMOUNT_SELECTORS``),
        using whichever returns a positive quote. Raises when no selector yields a
        quote so the caller fails closed (never ships ``min_lp=0``).
        """
        selectors = CRYPTO_CALC_TOKEN_AMOUNT_SELECTORS.get(pool_info.n_coins)
        if selectors is None:
            raise ValueError(
                f"No CryptoSwap calc_token_amount selector for {pool_info.n_coins}-coin pool {pool_info.name}"
            )
        # Fail fast on a present-but-disconnected gateway: it would otherwise reach
        # eth_call and surface a low-level RPC error instead of the clear fail-closed
        # min_lp message. Drop to rpc_url when available, else refuse (never min_lp=0).
        gateway_client = self._gateway_client
        if gateway_client is not None and not getattr(gateway_client, "is_connected", False):
            if not self._rpc_url:
                raise ValueError(
                    f"CryptoSwap/Tricrypto pool {pool_info.name}: gateway client present but "
                    "disconnected and no rpc_url; cannot compute min_lp, refusing to ship min_lp=0."
                )
            gateway_client = None
        inline_amounts = "".join(self._pad_uint256(a) for a in amounts)
        last_error: Exception | None = None
        for selector, has_deposit_flag in ((selectors[0], True), (selectors[1], False)):
            calldata = selector + inline_amounts + (self._pad_uint256(1) if has_deposit_flag else "")
            try:
                minted = eth_call_uint256(
                    chain=self.chain,
                    to=pool_info.address,
                    data=calldata,
                    rpc_url=self._rpc_url,
                    gateway_client=gateway_client,
                    timeout=10.0,
                )
            except Exception as exc:  # noqa: BLE001 — wrong selector may revert; try the next
                last_error = exc
                continue
            if minted is not None and minted > 0:
                return minted
        raise ValueError(
            f"CryptoSwap calc_token_amount returned no quote for {pool_info.name} "
            f"({pool_info.n_coins}-coin); cannot derive min_lp (last error: {last_error})"
        )

    def _estimate_remove_liquidity(self, pool_info: PoolInfo, lp_amount: int) -> list[int]:
        """Estimate expected per-coin amounts for proportional remove_liquidity.

        When rpc_url is configured, queries on-chain pool.balances(i) and
        lp_token.totalSupply() to compute accurate proportional amounts:
            expected_i = pool.balances(i) * lp_amount / lp_token.totalSupply()

        This is the only correct approach for imbalanced pools (e.g., Curve 3pool
        where DAI is ~7% of pool, not 33%). A slippage tolerance is then applied by
        the caller: min_amount_i = expected_i * (10000 - slippage_bps) / 10000

        When rpc_url is not configured or the RPC call fails, returns [0, ..., 0] and
        logs a warning. Callers that receive all-zeros should log an additional warning
        about absent slippage protection.

        Args:
            pool_info: Pool configuration including address, lp_token, and coin list
            lp_amount: LP token amount to burn (in wei, 18 decimals)

        Returns:
            List of expected token amounts (in native token decimals), one per coin.
            Returns [0, ..., 0] when on-chain estimation is unavailable.
        """
        zero_amounts = [0] * pool_info.n_coins
        if self._gateway_client is None and not self._rpc_url:
            logger.warning(
                f"remove_liquidity: no gateway_client or rpc_url configured for {pool_info.name} -- "
                "min_amounts will be [0, ..., 0] (no slippage protection). "
                "Set CurveConfig.gateway_client to enable on-chain estimation."
            )
            self._last_estimation_error = "gateway_client or rpc_url not configured"
            return zero_amounts

        try:
            return self._query_proportional_amounts_onchain(pool_info, lp_amount)
        except Exception as e:
            logger.warning(
                f"remove_liquidity: on-chain estimation failed for {pool_info.name}: {e} -- "
                "falling back to [0, ..., 0] (no slippage protection)"
            )
            self._last_estimation_error = str(e)
            return zero_amounts

    def _query_proportional_amounts_onchain(self, pool_info: PoolInfo, lp_amount: int) -> list[int]:
        """Query on-chain pool balances and LP totalSupply to compute proportional amounts.

        Makes synchronous JSON-RPC eth_call requests:
        1. lp_token.totalSupply() -> total LP supply
        2. pool.balances(i) for each coin -> current pool reserves

        Proportional amount for coin i:
            expected_i = pool.balances(i) * lp_amount / totalSupply

        This is exact for proportional remove_liquidity because Curve V1 StableSwap
        pools charge no fee on proportional withdrawals (only imbalanced ones do).

        Args:
            pool_info: Pool configuration
            lp_amount: LP token amount in wei

        Returns:
            List of expected token amounts in native decimals

        Raises:
            ValueError: If RPC returns unexpected data
            Exception: On network or parsing errors (caller handles fallback)
        """
        import json as _json

        # ABI selectors
        TOTAL_SUPPLY_SELECTOR = "18160ddd"  # totalSupply() -> uint256
        BALANCES_UINT256_SELECTOR = "4903b0d1"  # balances(uint256) -> uint256 (factory/newer pools)
        BALANCES_INT128_SELECTOR = "065a80d8"  # balances(int128) -> uint256 (old Vyper pools, e.g. 3pool)

        def _encode_uint256_arg(value: int) -> str:
            """Encode a single uint256 argument (32 bytes, no 0x prefix)."""
            return hex(value)[2:].zfill(64)

        def _eth_call(to: str, data: str) -> int:
            """Make a synchronous eth_call and return the result as int.

            Routes through the gateway when gateway_client is configured.
            Falls back to direct httpx POST only for ad-hoc script usage.
            """
            if self._gateway_client is not None:
                from almanak.gateway.proto import gateway_pb2

                rpc_request = gateway_pb2.RpcRequest(
                    chain=self.chain,
                    method="eth_call",
                    params=_json.dumps([{"to": to, "data": data}, "latest"]),
                    id="curve_remove_liquidity",
                )
                response = self._gateway_client.rpc.Call(rpc_request, timeout=10.0)
                if not response.success:
                    raise ValueError(f"eth_call error: {response.error or 'gateway returned failure'}")
                hex_result = _json.loads(response.result) if response.result else "0x"
                if not hex_result or hex_result == "0x":
                    raise ValueError("eth_call returned empty result")
                return self._decode_first_uint256_word(hex_result)

            # Fallback: direct RPC (deprecated, ad-hoc use only)
            import httpx

            assert self._rpc_url is not None
            payload = {
                "jsonrpc": "2.0",
                "method": "eth_call",
                "params": [{"to": to, "data": data}, "latest"],
                "id": 1,
            }
            response = httpx.post(
                self._rpc_url, json=payload, timeout=10.0
            )  # vib-2986-exempt: gateway-internal fallback
            response.raise_for_status()
            result = response.json()
            if "error" in result:
                raise ValueError(f"eth_call error: {result['error'].get('message', result['error'])}")
            hex_result = result.get("result", "0x0")
            if not hex_result or hex_result == "0x":
                raise ValueError("eth_call returned empty result")
            return self._decode_first_uint256_word(hex_result)

        # 1. Query LP totalSupply
        total_supply = _eth_call(
            pool_info.lp_token,
            f"0x{TOTAL_SUPPLY_SELECTOR}",
        )
        if total_supply == 0:
            raise ValueError(f"LP totalSupply is zero for pool {pool_info.name}")

        # 2. Query balances for each coin
        # Try balances(uint256) first (newer/factory pools), fall back to balances(int128)
        # (old Vyper pools like Ethereum 3pool). We detect the correct selector on the
        # first coin query and reuse it for the rest.
        amounts = []
        balances_selector = BALANCES_UINT256_SELECTOR
        for i in range(pool_info.n_coins):
            try:
                balance_raw = _eth_call(
                    pool_info.address,
                    f"0x{balances_selector}{_encode_uint256_arg(i)}",
                )
            except (ValueError, Exception):
                if i == 0 and balances_selector == BALANCES_UINT256_SELECTOR:
                    # First call failed with uint256 selector, retry with int128
                    balances_selector = BALANCES_INT128_SELECTOR
                    balance_raw = _eth_call(
                        pool_info.address,
                        f"0x{balances_selector}{_encode_uint256_arg(i)}",
                    )
                else:
                    raise
            # Proportional share: balance * lp_amount / total_supply
            expected = balance_raw * lp_amount // total_supply
            amounts.append(expected)

        logger.debug(
            f"remove_liquidity on-chain estimate for {pool_info.name}: "
            f"lp={lp_amount}, total_supply={total_supply}, amounts={amounts}"
        )
        return amounts

    def _query_calc_token_amount_ng_onchain(self, pool_info: PoolInfo, amounts: list[int]) -> int:
        """Query ``calc_token_amount(uint256[], bool)`` on a StableSwap NG pool.

        Returns the exact LP-token mint quote the pool would produce for these
        deposit amounts. Used by ``_estimate_add_liquidity`` for NG pools so
        slippage protection is computed against real pool math rather than the
        naive sum/virtual_price estimator (which drifts as fees accrue and
        ignores imbalance fees). VIB-4836.

        Routing mirrors the existing ``_eth_call`` helper used by
        ``_estimate_remove_liquidity_proportional``: gateway-first when a
        ``GatewayRpcClient`` is wired, falling back to ``rpc_url`` for
        intent-test / ad-hoc adapter constructions that don't go through the
        gateway. The httpx fallback carries the same ``vib-2986-exempt`` marker
        as the rest of this connector's gateway-internal RPC paths.
        """
        import json as _json

        # selector: calc_token_amount(uint256[],bool) → 0x3db06dd8
        # head: [offset_to_amounts=0x40, is_deposit=1]
        # tail: [length=n_coins, amounts...]
        calldata = "0x3db06dd8"
        calldata += self._pad_uint256(0x40)
        calldata += self._pad_uint256(1)  # is_deposit
        calldata += self._pad_uint256(pool_info.n_coins)
        for amount in amounts:
            calldata += self._pad_uint256(amount)

        if self._gateway_client is not None:
            from almanak.gateway.proto import gateway_pb2

            rpc_request = gateway_pb2.RpcRequest(
                chain=self.chain,
                method="eth_call",
                params=_json.dumps([{"to": pool_info.address, "data": calldata}, "latest"]),
                id="curve_add_liquidity_ng",
            )
            response = self._gateway_client.rpc.Call(rpc_request, timeout=10.0)
            if not response.success:
                raise ValueError(f"calc_token_amount eth_call failed: {response.error or 'gateway failure'}")
            hex_result = _json.loads(response.result) if response.result else "0x"
        else:
            import httpx

            assert self._rpc_url is not None, "calc_token_amount on-chain requires either a gateway client or rpc_url"
            payload = {
                "jsonrpc": "2.0",
                "method": "eth_call",
                "params": [{"to": pool_info.address, "data": calldata}, "latest"],
                "id": 1,
            }
            response = httpx.post(
                self._rpc_url, json=payload, timeout=10.0
            )  # vib-2986-exempt: gateway-internal fallback
            response.raise_for_status()
            result = response.json()
            if "error" in result:
                raise ValueError(f"calc_token_amount eth_call failed: {result['error']}")
            hex_result = result.get("result", "0x0")

        if not hex_result or hex_result == "0x":
            raise ValueError("calc_token_amount returned empty result")
        return self._decode_first_uint256_word(hex_result)

    @staticmethod
    def _decode_first_uint256_word(hex_result: str) -> int:
        """Decode the FIRST 32-byte word of an ``eth_call`` hex response.

        A single-``uint256`` return is the first 32-byte (64 hex char) word.
        Some Vyper pools — notably factory METAPOOLS — return extra trailing
        words for getters like ``balances(uint256)``, so decoding the whole
        response with ``int(hex_result, 16)`` builds a multi-thousand-digit
        integer that trips Python's ``int``-string-conversion guard (VIB-5419).
        Slicing the first word is correct for every single-value getter and a
        no-op for the legacy flat pools that already return exactly 32 bytes.
        """
        body = hex_result[2:] if hex_result.startswith("0x") else hex_result
        if len(body) < 64:
            # Fewer than one full word — decode whatever is present (preserves
            # the prior behaviour for short/edge responses).
            return int(body or "0", 16)
        return int(body[:64], 16)

    def _estimate_remove_liquidity_one(self, pool_info: PoolInfo, lp_amount: int, coin_index: int) -> int:
        """Estimate tokens from remove_liquidity_one_coin.

        Multiplies by virtual_price because LP tokens are worth virtual_price
        underlying value. This is different from _estimate_remove_liquidity
        (proportional): single-sided withdrawal doesn't have balance-ratio issues,
        so virtual_price adjustment is both safe and necessary.

        Applies 1% penalty for single-sided removal (pool charges a fee for
        imbalanced withdrawals).
        """
        # Adjust LP amount by virtual_price to get underlying value
        adjusted_lp = int(Decimal(lp_amount) * pool_info.virtual_price)
        decimals = self._get_token_decimals(pool_info.coins[coin_index])
        # Convert from 18 decimals, apply small penalty for single-sided
        return (adjusted_lp // (10 ** (18 - decimals))) * 99 // 100

    # =========================================================================
    # Helper Methods
    # =========================================================================

    def _resolve_token(self, token: str) -> str:
        """Resolve token symbol or address to address using TokenResolver."""
        if token.startswith("0x") and len(token) == 42:
            return token
        try:
            resolved = self._token_resolver.resolve(token, self.chain)
            return resolved.address
        except TokenResolutionError as e:
            raise TokenResolutionError(
                token=token,
                chain=str(self.chain),
                reason=f"[CurveAdapter] Cannot resolve token: {e.reason}",
                suggestions=e.suggestions,
            ) from e

    def _get_token_symbol(self, address: str) -> str:
        """Get token symbol from address using TokenResolver.

        Falls back to truncated address if token is not in registry
        (e.g., Curve LP tokens like 3Crv). This is used only for log
        descriptions, not for transaction logic.

        Uses skip_gateway=True to avoid 30-second gateway timeouts for
        LP pool addresses that are valid ERC-20s but not in the static registry.
        """
        if not address.startswith("0x"):
            return address
        try:
            resolved = self._token_resolver.resolve(address, self.chain, skip_gateway=True, log_errors=False)
            return resolved.symbol
        except TokenResolutionError:
            logger.debug(f"Cannot resolve symbol for {address}, using truncated address")
            return f"{address[:10]}..."

    def _get_token_decimals(self, symbol: str) -> int:
        """Get token decimals from symbol using TokenResolver."""
        try:
            resolved = self._token_resolver.resolve(symbol, self.chain)
            return resolved.decimals
        except TokenResolutionError as e:
            raise TokenResolutionError(
                token=symbol,
                chain=str(self.chain),
                reason=f"[CurveAdapter] Cannot determine decimals: {e.reason}",
                suggestions=e.suggestions,
            ) from e

    def _is_native_token(self, token: str) -> bool:
        """Check if ``token`` denotes the CURRENT chain's native coin.

        Callers pass pool coin ADDRESSES, where Curve marks raw native with the
        0xEeee placeholder — that arm does the real work. The symbol arm is
        derived per-chain from ``ChainDescriptor.native`` via
        ``native_symbols_for`` (VIB-4851 A1) instead of the legacy hardcoded
        "ETH", so a symbol caller on polygon gets MATIC/POL right.
        """
        if token.upper() in native_symbols_for(self.chain):
            return True
        native_address = "0xEeeeeEeeeEeEeeEeEeEeeEEEeeeeEeeeeeeeEEeE".lower()
        return token.lower() == native_address

    @staticmethod
    def _pad_address(addr: str) -> str:
        """Pad address to 32 bytes."""
        return addr.lower().replace("0x", "").zfill(64)

    @staticmethod
    def _pad_uint256(value: int) -> str:
        """Pad uint256 to 32 bytes."""
        return hex(value)[2:].zfill(64)

    @staticmethod
    def _pad_int128(value: int) -> str:
        """Pad int128 to 32 bytes (signed)."""
        if value < 0:
            # Two's complement for negative values
            value = (1 << 256) + value
        return hex(value)[2:].zfill(64)

    # =========================================================================
    # State Management
    # =========================================================================

    def set_allowance(self, token: str, spender: str, amount: int) -> None:
        """Set cached allowance (for testing).

        Args:
            token: Token address
            spender: Spender address
            amount: Allowance amount
        """
        self._allowance_cache[self._allowance_cache_key(token, spender)] = amount

    def clear_allowance_cache(self) -> None:
        """Clear the allowance cache."""
        self._allowance_cache.clear()


# =============================================================================
# Exports
# =============================================================================

__all__ = [
    "CurveAdapter",
    "CurveConfig",
    "SwapResult",
    "LiquidityResult",
    "PoolInfo",
    "PoolType",
    "TransactionData",
    "CURVE_ADDRESSES",
    "CURVE_POOLS",
    "CURVE_GAS_ESTIMATES",
]
