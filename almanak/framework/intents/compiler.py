"""Intent to ActionBundle Compiler.

This module provides the IntentCompiler class that converts high-level
trading intents into executable ActionBundles containing transaction data.

The compiler:
1. Takes an Intent (e.g., SwapIntent)
2. Resolves token addresses and amounts
3. Builds necessary approve transactions
4. Builds the primary action transaction (swap, LP, etc.)
5. Estimates gas for all transactions
6. Returns an ActionBundle ready for execution

Example:
    from almanak.framework.intents import Intent
    from almanak.framework.intents.compiler import IntentCompiler

    compiler = IntentCompiler(chain="arbitrum")
    intent = Intent.swap("USDC", "ETH", amount_usd=Decimal("1000"))
    bundle = compiler.compile(intent)
"""

import logging
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from enum import Enum
from typing import TYPE_CHECKING, Any, ClassVar, Literal, Protocol

# Note: FlashLoanSelector import is done lazily in _compile_flash_loan to avoid circular import
# Note: PolymarketAdapter import is done lazily in __init__ to avoid circular import and allow optional usage
# Note: MorphoBlueAdapter is imported lazily in _compile_* methods to avoid circular import
# Note: TokenNotFoundError and get_token_resolver are imported lazily to avoid circular import
# (compiler -> data/__init__ -> prediction_provider -> connectors/__init__ -> ... -> compiler)
from ..models.reproduction_bundle import ActionBundle
from ..utils.log_formatters import (
    _emojis_enabled,
    format_percentage,
    format_slippage_bps,
    format_token_amount,
)
from .vocabulary import (
    AnyIntent,
    BorrowIntent,
    CollectFeesIntent,
    FlashLoanIntent,
    HoldIntent,
    Intent,
    IntentType,
    LPCloseIntent,
    LPOpenIntent,
    PerpCloseIntent,
    PerpOpenIntent,
    PredictionBuyIntent,
    PredictionRedeemIntent,
    PredictionSellIntent,
    RepayIntent,
    StakeIntent,
    SupplyIntent,
    SwapIntent,
    UnstakeIntent,
    VaultDepositIntent,
    VaultRedeemIntent,
    WithdrawIntent,
)

if TYPE_CHECKING:
    from web3 import Web3

    from ..connectors.bridges.selector import BridgeSelector
    from ..connectors.polymarket.adapter import PolymarketAdapter
    from ..connectors.polymarket.models import PolymarketConfig
    from ..data.tokens import TokenResolver as TokenResolverType
    from ..gateway_client import GatewayClient
    from .bridge import BridgeIntent
    from .pool_validation import PoolValidationResult
    from .vocabulary import UnwrapNativeIntent, WrapNativeIntent

logger = logging.getLogger(__name__)


# =============================================================================
# Configuration
# =============================================================================


@dataclass
class IntentCompilerConfig:
    """Configuration for IntentCompiler.

    Attributes:
        allow_placeholder_prices: If False (default), raises ValueError when no
            price_oracle is given. Set to True ONLY for unit tests.
            NEVER set to True in production - placeholder prices will cause
            incorrect slippage calculations and swap reverts.
        polymarket_config: Optional PolymarketConfig for prediction market intents.
            Required when compiling PredictionBuyIntent, PredictionSellIntent,
            or PredictionRedeemIntent on Polygon. If not provided when on Polygon,
            a warning is logged and prediction intents will fail to compile.
        swap_pool_selection_mode: Pool selection mode for V3-style swaps.
            - "auto" (default): Try all supported fee tiers and pick best quote when RPC is available.
            - "fixed": Use fixed_swap_fee_tier for deterministic execution.
        fixed_swap_fee_tier: Optional fixed fee tier used when swap_pool_selection_mode="fixed".
            Must be valid for the selected protocol.
        max_price_impact_pct: Maximum acceptable price impact as a fraction (0.0 to 1.0).
            If the on-chain quoter returns an amount deviating more than this from the oracle
            estimate, compilation fails with a clear error. Default: 0.30 (30%).
            Can be overridden per-intent via SwapIntent.max_price_impact.
    """

    allow_placeholder_prices: bool = False
    polymarket_config: "PolymarketConfig | None" = None
    swap_pool_selection_mode: Literal["auto", "fixed"] = "auto"
    fixed_swap_fee_tier: int | None = None
    max_price_impact_pct: Decimal = Decimal("0.30")

    def __post_init__(self) -> None:
        """Validate swap pool selection settings."""
        if self.swap_pool_selection_mode not in {"auto", "fixed"}:
            raise ValueError("swap_pool_selection_mode must be 'auto' or 'fixed'")
        if self.swap_pool_selection_mode == "fixed" and self.fixed_swap_fee_tier is None:
            raise ValueError("fixed_swap_fee_tier is required when swap_pool_selection_mode='fixed'")
        # Coerce float to Decimal to ensure guard always operates in Decimal space
        if not isinstance(self.max_price_impact_pct, Decimal):
            object.__setattr__(self, "max_price_impact_pct", Decimal(str(self.max_price_impact_pct)))
        if not Decimal("0") < self.max_price_impact_pct <= Decimal("1"):
            raise ValueError("max_price_impact_pct must be between 0 (exclusive) and 1 (inclusive)")


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
        # traderjoe_v2 removed (VIB-1406): LBRouter2 incompatible with DefaultSwapAdapter
        "1inch": "0x1111111254EEB25477B68fb85Ed929f73A960582",
    },
    "arbitrum": {
        "uniswap_v3": "0x68b3465833fb72A70ecDF485E0e4C7bD8665Fc45",  # SwapRouter02
        "sushiswap_v3": "0x8A21F6768C1f8075791D08546Dadf6daA0bE820c",  # SushiSwap V3 SwapRouter
        "pancakeswap_v3": "0x32226588378236Fd0c7c4053999F88aC0e5cAc77",  # SmartRouter (7-param)
        # traderjoe_v2 removed (VIB-1406): LBRouter2 incompatible with DefaultSwapAdapter
        "sushiswap": "0x1b02dA8Cb0d097eB8D57A175b88c7D8b47997506",
        "camelot": "0xc873fEcbd354f5A56E00E710B90EF4201db2448d",
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
        # traderjoe_v2 removed (VIB-1406): LBRouter2 incompatible with DefaultSwapAdapter
        "uniswap_v3": "0xbb00FF08d01D300023C629E8fFfFcb65A5a578cE",  # SwapRouter02
        "sushiswap_v3": "0x717b7948AA264DeCf4D780aa6914482e5F46Da3e",  # SushiSwap V3 SwapRouter
    },
    "bsc": {
        "pancakeswap_v3": "0x13f4EA83D0bd40E75C8222255bc855a974568Dd4",  # SmartRouter (7-param)
        "pancakeswap_v2": "0x10ED43C718714eb63d5aA57B78B54704E256024E",
        "uniswap_v3": "0xB971eF87ede563556b2ED4b1C0b0019111Dd85d2",  # SwapRouter02
        "sushiswap_v3": "0xB45e53277a7e0F1D35f2a77160e91e25507f1763",  # SushiSwap V3 SwapRouter
        # traderjoe_v2 removed (VIB-1406): LBRouter2 incompatible with DefaultSwapAdapter
        "sushiswap": "0x1b02dA8Cb0d097eB8D57A175b88c7D8b47997506",
    },
    "linea": {
        "uniswap_v3": "0x3d4e44Eb1374240CE5F1B871ab261CD16335B76a",  # SwapRouter02
        "pancakeswap_v3": "0x678Aa4bF4E210cf2166753e054d5b7c31cc7fa86",  # SmartRouter
    },
    "mantle": {
        "agni_finance": "0x319B69888b0d11cEC22caA5034e25FfFBDc88421",  # Agni Finance SwapRouter
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
        "pancakeswap_v3": "0x46A15B0b27311cedF172AB29E4f4766fbE7F4364",
    },
    "avalanche": {
        "uniswap_v3": "0x655C406EBFa14EE2006250925e54ec43AD184f8B",
        "uniswap_v4": "0xBd216513D74C8cf14cF4747E6AaE6fDf64e83b24",  # V4 PositionManager
        "sushiswap_v3": "0x18350b048AB366ed601fFDbC669110Ecb36016f3",
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
    "agni_finance": 3000,
}

# Protocols using the original SwapRouter interface (8-param exactInputSingle WITH deadline).
# All other protocols use SwapRouter02 interface (7-param WITHOUT deadline).
SWAP_ROUTER_V1_PROTOCOLS: frozenset[str] = frozenset({"sushiswap_v3"})

# Chain-specific overrides: some chains use V3 forks with V1-style routers (e.g., Agni on Mantle).
# Maps chain -> set of protocols that use the V1 router interface on that chain.
SWAP_ROUTER_V1_CHAIN_OVERRIDES: dict[str, frozenset[str]] = {
    "mantle": frozenset({"agni_finance"}),  # Agni Finance uses original SwapRouter (with deadline)
}

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
        "sushiswap_v3": "0xb1E835Dc2785b52265711e17fCCb0fd018226a6e",
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
}

# Aave V3 Pool addresses per chain
LENDING_POOL_ADDRESSES: dict[str, dict[str, str]] = {
    "ethereum": {
        "aave_v3": "0x87870Bca3F3fD6335C3F4ce8392D69350B4fA4E2",
    },
    "arbitrum": {
        "aave_v3": "0x794a61358D6845594F94dc1DB02A252b5b4814aD",
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


# =============================================================================
# Protocol Adapter Protocol
# =============================================================================


class SwapProtocolAdapter(Protocol):
    """Protocol interface for DEX adapters."""

    def get_swap_calldata(
        self,
        from_token: str,
        to_token: str,
        amount_in: int,
        min_amount_out: int,
        recipient: str,
        deadline: int,
    ) -> bytes:
        """Generate calldata for a swap transaction."""
        ...

    def get_router_address(self) -> str:
        """Get the router address for this protocol."""
        ...

    def estimate_gas(self, from_token: str, to_token: str) -> int:
        """Estimate gas for a swap."""
        ...


class LPProtocolAdapter(Protocol):
    """Protocol interface for LP (liquidity provider) adapters."""

    def get_mint_calldata(
        self,
        token0: str,
        token1: str,
        fee: int,
        tick_lower: int,
        tick_upper: int,
        amount0_desired: int,
        amount1_desired: int,
        amount0_min: int,
        amount1_min: int,
        recipient: str,
        deadline: int,
    ) -> bytes:
        """Generate calldata for minting a new LP position."""
        ...

    def get_decrease_liquidity_calldata(
        self,
        token_id: int,
        liquidity: int,
        amount0_min: int,
        amount1_min: int,
        deadline: int,
    ) -> bytes:
        """Generate calldata for decreasing liquidity in an existing position."""
        ...

    def get_collect_calldata(
        self,
        token_id: int,
        recipient: str,
        amount0_max: int,
        amount1_max: int,
    ) -> bytes:
        """Generate calldata for collecting tokens from a position."""
        ...

    def get_position_manager_address(self) -> str:
        """Get the NFT position manager address for this protocol."""
        ...

    def estimate_mint_gas(self) -> int:
        """Estimate gas for minting a new position."""
        ...

    def estimate_close_gas(self, collect_fees: bool) -> int:
        """Estimate gas for closing a position."""
        ...


# =============================================================================
# Data Classes
# =============================================================================


class CompilationStatus(Enum):
    """Status of intent compilation."""

    SUCCESS = "SUCCESS"
    FAILED = "FAILED"
    PARTIAL = "PARTIAL"  # Some transactions built, some failed


@dataclass
class TransactionData:
    """Represents a single transaction in an ActionBundle.

    Attributes:
        to: Target contract address
        value: ETH value to send (in wei)
        data: Encoded calldata
        gas_estimate: Estimated gas for this transaction
        description: Human-readable description of what this TX does
        tx_type: Type of transaction (approve, swap, etc.)
    """

    to: str
    value: int
    data: str  # Hex-encoded calldata
    gas_estimate: int
    description: str
    tx_type: str

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "to": self.to,
            "value": str(self.value),
            "data": self.data,
            "gas_estimate": self.gas_estimate,
            "description": self.description,
            "tx_type": self.tx_type,
        }


@dataclass
class CompilationResult:
    """Result of compiling an intent to an ActionBundle.

    Attributes:
        status: Compilation status
        action_bundle: The compiled ActionBundle (if successful)
        transactions: List of transaction data
        total_gas_estimate: Sum of all gas estimates
        error: Error message (if failed)
        warnings: List of warnings encountered during compilation
        intent_id: ID of the intent that was compiled
        compiled_at: Timestamp of compilation
    """

    status: CompilationStatus
    action_bundle: ActionBundle | None = None
    transactions: list[TransactionData] = field(default_factory=list)
    total_gas_estimate: int = 0
    error: str | None = None
    warnings: list[str] = field(default_factory=list)
    intent_id: str = ""
    compiled_at: datetime = field(default_factory=lambda: datetime.now(UTC))

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "status": self.status.value,
            "action_bundle": self.action_bundle.to_dict() if self.action_bundle else None,
            "transactions": [t.to_dict() for t in self.transactions],
            "total_gas_estimate": self.total_gas_estimate,
            "error": self.error,
            "warnings": self.warnings,
            "intent_id": self.intent_id,
            "compiled_at": self.compiled_at.isoformat(),
        }


@dataclass
class TokenInfo:
    """Information about a token.

    Attributes:
        symbol: Token symbol (e.g., "USDC")
        address: Token contract address
        decimals: Token decimals
        is_native: Whether this is the native token (ETH, MATIC, etc.)
    """

    symbol: str
    address: str
    decimals: int = 18
    is_native: bool = False

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "symbol": self.symbol,
            "address": self.address,
            "decimals": self.decimals,
            "is_native": self.is_native,
        }


@dataclass
class PriceInfo:
    """Price information for amount calculations.

    Attributes:
        token: Token symbol
        price_usd: Price in USD
        timestamp: When this price was fetched
    """

    token: str
    price_usd: Decimal
    timestamp: datetime = field(default_factory=lambda: datetime.now(UTC))


# =============================================================================
# Default Protocol Adapter
# =============================================================================


class DefaultSwapAdapter:
    """Default swap adapter using Uniswap V3-style interface.

    This adapter generates calldata compatible with Uniswap V3's
    SwapRouter interface (exactInputSingle).

    Note: Instances are single-use per swap compilation. The compiler creates
    a fresh adapter in ``_compile_swap`` for each SwapIntent. Mutable state
    (``_cached_fee``, ``last_quoted_amount_out``) is therefore never carried
    across different token pairs or amounts.
    """

    def __init__(
        self,
        chain: str,
        protocol: str = "uniswap_v3",
        pool_selection_mode: Literal["auto", "fixed"] = "auto",
        fixed_fee_tier: int | None = None,
        rpc_url: str | None = None,
        rpc_timeout: float = 10.0,
    ) -> None:
        """Initialize the adapter.

        Args:
            chain: Target blockchain
            protocol: Protocol name for router lookup
            pool_selection_mode: "auto" to quote all tiers (when possible), "fixed" for deterministic tier
            fixed_fee_tier: Optional fixed fee tier (required when pool_selection_mode="fixed")
            rpc_url: Optional RPC URL for on-chain quote queries in auto mode
            rpc_timeout: HTTP timeout for on-chain quote calls in seconds
        """
        self.chain = chain
        self.protocol = protocol
        self.pool_selection_mode = pool_selection_mode
        self.fixed_fee_tier = fixed_fee_tier
        self.rpc_url = rpc_url
        self.rpc_timeout = rpc_timeout
        self.last_fee_selection: dict[str, Any] = {}
        self.last_quoted_amount_out: int | None = None
        self._cached_fee: int | None = None

        # Get router address
        chain_routers = PROTOCOL_ROUTERS.get(chain, {})
        self.router_address = chain_routers.get(protocol, "0x0000000000000000000000000000000000000000")

    def get_router_address(self) -> str:
        """Get the router address."""
        return self.router_address

    def select_fee_tier(self, from_token: str, to_token: str, amount_in: int) -> int:
        """Pre-select fee tier and cache the result.

        Call this before get_swap_calldata() to make quoter data available
        for slippage adjustments. The selected fee tier is cached and reused
        by get_swap_calldata().

        Returns:
            Selected fee tier (bps).
        """
        # Clear previous state so stale data is never carried across calls.
        self._cached_fee = None
        self.last_quoted_amount_out = None
        fee = self._select_fee_tier(from_token, to_token, amount_in)
        self._cached_fee = fee
        return fee

    def get_quoted_amount_out(self) -> int | None:
        """Return the best quoted amount_out from the last fee tier selection.

        Only available after select_fee_tier() or get_swap_calldata() when
        the quoter was used (auto mode with RPC). Returns None if quoter
        was not used or no valid quotes were returned.
        """
        return self.last_quoted_amount_out

    def get_swap_calldata(
        self,
        from_token: str,
        to_token: str,
        amount_in: int,
        min_amount_out: int,
        recipient: str,
        deadline: int,  # used by SwapRouter V1; ignored by SwapRouter02
    ) -> bytes:
        """Generate calldata for exactInputSingle swap.

        Args:
            from_token: Input token address
            to_token: Output token address
            amount_in: Amount of input tokens (in wei)
            min_amount_out: Minimum output amount (in wei)
            recipient: Address to receive output tokens
            deadline: Transaction deadline (used by SwapRouter V1; ignored by SwapRouter02)

        Returns:
            Encoded calldata for the swap
        """
        # Use cached fee tier if pre-selected via select_fee_tier()
        if self._cached_fee is not None:
            fee = self._cached_fee
        else:
            fee = self._select_fee_tier(from_token, to_token, amount_in)
        sqrt_price_limit = 0

        chain_v1_overrides = SWAP_ROUTER_V1_CHAIN_OVERRIDES.get(self.chain, frozenset())
        if self.protocol in SWAP_ROUTER_V1_PROTOCOLS or self.protocol in chain_v1_overrides:
            # Original SwapRouter (V1) exactInputSingle: 8-param WITH deadline
            # selector: 0x414bf389
            # Struct: tokenIn, tokenOut, fee, recipient, deadline, amountIn, amountOutMinimum, sqrtPriceLimitX96
            selector = "0x414bf389"
            swap_deadline = deadline
            params = (
                self._pad_address(from_token)
                + self._pad_address(to_token)
                + self._pad_uint24(fee)
                + self._pad_address(recipient)
                + self._pad_uint256(swap_deadline)
                + self._pad_uint256(amount_in)
                + self._pad_uint256(min_amount_out)
                + self._pad_uint160(sqrt_price_limit)
            )
        else:
            # SwapRouter02 / IV3SwapRouter exactInputSingle: 7-param WITHOUT deadline
            # selector: 0x04e45aaf
            # Struct: tokenIn, tokenOut, fee, recipient, amountIn, amountOutMinimum, sqrtPriceLimitX96
            selector = "0x04e45aaf"
            params = (
                self._pad_address(from_token)
                + self._pad_address(to_token)
                + self._pad_uint24(fee)
                + self._pad_address(recipient)
                + self._pad_uint256(amount_in)
                + self._pad_uint256(min_amount_out)
                + self._pad_uint160(sqrt_price_limit)
            )

        return bytes.fromhex(selector[2:] + params)

    def _supported_fee_tiers(self) -> tuple[int, ...]:
        """Return supported fee tiers for current protocol, with chain-specific overrides."""
        chain_key = (str(self.chain).lower(), self.protocol)
        return SWAP_FEE_TIERS_CHAIN.get(chain_key, SWAP_FEE_TIERS.get(self.protocol, ()))

    def _select_fee_tier(self, from_token: str, to_token: str, amount_in: int) -> int:
        """Select fee tier using fixed mode, on-chain quotes, or safe heuristic fallback."""
        candidates = self._supported_fee_tiers()
        if self.pool_selection_mode == "fixed":
            if not candidates or self.fixed_fee_tier is None or self.fixed_fee_tier not in candidates:
                raise ValueError(
                    f"Invalid fixed fee tier {self.fixed_fee_tier} for protocol {self.protocol}. "
                    f"Available tiers: {list(candidates)}"
                )
            self.last_fee_selection = {
                "mode": "fixed",
                "source": "fixed_config",
                "selected_fee_tier": self.fixed_fee_tier,
                "candidate_fee_tiers": list(candidates),
            }
            return self.fixed_fee_tier

        if not candidates:
            self.last_fee_selection = {
                "mode": "unsupported",
                "source": "fallback_default",
                "selected_fee_tier": 3000,
                "candidate_fee_tiers": [],
            }
            return 3000

        if self.pool_selection_mode == "auto":
            quoted = self._select_fee_tier_by_quoter(from_token, to_token, amount_in, candidates)
            if quoted is not None:
                self.last_fee_selection = {
                    "mode": "auto",
                    "source": "quoter_best_quote",
                    "selected_fee_tier": quoted["fee_tier"],
                    "candidate_fee_tiers": list(candidates),
                    "quoted_candidates": quoted["quoted_candidates"],
                }
                return quoted["fee_tier"]

        heuristic_fee = self._select_fee_tier_heuristic(from_token, to_token)
        if heuristic_fee not in candidates:
            heuristic_fee = DEFAULT_SWAP_FEE_TIER.get(self.protocol, candidates[0])
        self.last_fee_selection = {
            "mode": self.pool_selection_mode,
            "source": "heuristic_fallback",
            "selected_fee_tier": heuristic_fee,
            "candidate_fee_tiers": list(candidates),
        }
        return heuristic_fee

    def _select_fee_tier_heuristic(self, from_token: str, to_token: str) -> int:
        """Conservative heuristic when no on-chain quoting is available."""
        from_lower = from_token.lower()
        to_lower = to_token.lower()
        from ..data.tokens import get_token_resolver

        resolver = get_token_resolver()

        def resolve_address(symbol: str, probe: bool = False) -> str | None:
            """Resolve a token symbol to its address.

            Args:
                symbol: Token symbol to resolve.
                probe: If True, suppress WARNING-level resolver logs for
                    expected probe failures (e.g. USDC.e on chains with
                    only native USDC).
            """
            try:
                # Use log_errors=False for probe lookups (expected failures should not warn).
                # This is thread-safe -- unlike mutating a shared logger level, the
                # log_errors flag is passed per-call and does not affect other threads.
                token = resolver.resolve(symbol, self.chain, log_errors=not probe)
            except Exception:
                return None
            if token is None:
                return None
            address = getattr(token, "address", None)
            return address.lower() if isinstance(address, str) else None

        usdc_addr = resolve_address("USDC")
        usdc_bridged = resolve_address("USDC.e", probe=True) or resolve_address("USDC_BRIDGED", probe=True)

        # Only resolve the wrapped native token for the current chain (not all chains)
        _wrapped_symbols = {
            "ethereum": "WETH",
            "arbitrum": "WETH",
            "optimism": "WETH",
            "base": "WETH",
            "polygon": "WMATIC",
            "avalanche": "WAVAX",
            "plasma": "WXPL",
            "bsc": "WBNB",
            "mantle": "WMNT",
            "sonic": "WS",
        }
        _wn_symbol = _wrapped_symbols.get(self.chain)
        wrapped_native_addr = resolve_address(_wn_symbol) if _wn_symbol else None

        is_usdc = bool(usdc_addr and usdc_addr in (from_lower, to_lower))
        is_usdc_bridged = bool(usdc_bridged and usdc_bridged in (from_lower, to_lower))
        is_native_wrapped = bool(wrapped_native_addr and wrapped_native_addr in (from_lower, to_lower))
        if (is_usdc or is_usdc_bridged) and is_native_wrapped:
            return 100 if self.protocol == "pancakeswap_v3" else 500
        return DEFAULT_SWAP_FEE_TIER.get(self.protocol, 3000)

    def _select_fee_tier_by_quoter(
        self,
        from_token: str,
        to_token: str,
        amount_in: int,
        candidates: tuple[int, ...],
    ) -> dict[str, Any] | None:
        """Try quoting all candidate tiers via QuoterV2 and return best output tier."""
        if not self.rpc_url:
            return None
        quoter_address = SWAP_QUOTER_ADDRESSES.get(self.chain, {}).get(self.protocol)
        if not quoter_address:
            return None

        try:
            from web3 import Web3
        except ImportError:
            return None

        web3 = Web3(
            Web3.HTTPProvider(
                self.rpc_url,
                request_kwargs={"timeout": self.rpc_timeout},
            )
        )
        if not web3.is_connected():
            return None

        quoter_abi = [
            {
                "inputs": [
                    {
                        "components": [
                            {"internalType": "address", "name": "tokenIn", "type": "address"},
                            {"internalType": "address", "name": "tokenOut", "type": "address"},
                            {"internalType": "uint256", "name": "amountIn", "type": "uint256"},
                            {"internalType": "uint24", "name": "fee", "type": "uint24"},
                            {"internalType": "uint160", "name": "sqrtPriceLimitX96", "type": "uint160"},
                        ],
                        "internalType": "struct IQuoterV2.QuoteExactInputSingleParams",
                        "name": "params",
                        "type": "tuple",
                    }
                ],
                "name": "quoteExactInputSingle",
                "outputs": [
                    {"internalType": "uint256", "name": "amountOut", "type": "uint256"},
                    {"internalType": "uint160", "name": "sqrtPriceX96After", "type": "uint160"},
                    {"internalType": "uint32", "name": "initializedTicksCrossed", "type": "uint32"},
                    {"internalType": "uint256", "name": "gasEstimate", "type": "uint256"},
                ],
                "stateMutability": "nonpayable",
                "type": "function",
            }
        ]

        contract = web3.eth.contract(address=web3.to_checksum_address(quoter_address), abi=quoter_abi)
        from_addr = web3.to_checksum_address(from_token)
        to_addr = web3.to_checksum_address(to_token)

        def _quote_fee_tier(fee_tier: int) -> dict[str, int] | None:
            """Quote a single fee tier. Returns result dict or None on failure."""
            try:
                amount_out, _, _, gas_estimate = contract.functions.quoteExactInputSingle(
                    (from_addr, to_addr, amount_in, fee_tier, 0)
                ).call()
                if amount_out > 0:
                    return {
                        "fee_tier": fee_tier,
                        "amount_out": int(amount_out),
                        "gas_estimate": int(gas_estimate),
                    }
            except Exception as exc:
                logger.debug("Fee-tier quote failed for fee_tier=%s: %s", fee_tier, exc)
            return None

        # Query all fee tiers in parallel to avoid sequential RPC latency
        quoted_candidates: list[dict[str, int]] = []
        with ThreadPoolExecutor(max_workers=len(candidates)) as executor:
            futures = {executor.submit(_quote_fee_tier, ft): ft for ft in candidates}
            for future in as_completed(futures):
                result = future.result()
                if result is not None:
                    quoted_candidates.append(result)

        if not quoted_candidates:
            return None

        best = max(quoted_candidates, key=lambda quote: (quote["amount_out"], -quote["fee_tier"]))
        # Store the best quoted amount for downstream slippage adjustments
        self.last_quoted_amount_out = int(best["amount_out"])
        return {
            "fee_tier": int(best["fee_tier"]),
            "quoted_candidates": quoted_candidates,
        }

    def estimate_gas(self, from_token: str, to_token: str) -> int:
        """Estimate gas for a swap.

        Args:
            from_token: Input token address
            to_token: Output token address

        Returns:
            Estimated gas units (chain-aware for proxy tokens)
        """
        # Check if this is a native token swap (requires wrap/unwrap)
        native_placeholder = "0xEeeeeEeeeEeEeeEeEeEeeEEEeeeeEeeeeeeeEEeE".lower()
        if from_token.lower() == native_placeholder or to_token.lower() == native_placeholder:
            return get_gas_estimate(self.chain, "swap_simple") + get_gas_estimate(self.chain, "wrap_eth")
        return get_gas_estimate(self.chain, "swap_simple")

    @staticmethod
    def _pad_address(addr: str) -> str:
        """Pad address to 32 bytes."""
        addr_clean = addr.lower().replace("0x", "")
        return addr_clean.zfill(64)

    @staticmethod
    def _pad_uint256(value: int) -> str:
        """Pad uint256 to 32 bytes."""
        return hex(value)[2:].zfill(64)

    @staticmethod
    def _pad_uint160(value: int) -> str:
        """Pad uint160 to 32 bytes."""
        return hex(value)[2:].zfill(64)

    @staticmethod
    def _pad_uint24(value: int) -> str:
        """Pad uint24 to 32 bytes."""
        return hex(value)[2:].zfill(64)


class UniswapV3LPAdapter:
    """LP adapter for Uniswap V3 NonfungiblePositionManager.

    This adapter generates calldata for managing concentrated liquidity
    positions on Uniswap V3 and compatible protocols.
    """

    def __init__(self, chain: str, protocol: str = "uniswap_v3") -> None:
        """Initialize the adapter.

        Args:
            chain: Target blockchain
            protocol: Protocol name for position manager lookup
        """
        self.chain = chain
        self.protocol = protocol

        # Get position manager address
        chain_managers = LP_POSITION_MANAGERS.get(chain, {})
        self.position_manager_address = chain_managers.get(protocol, "0x0000000000000000000000000000000000000000")

    def get_position_manager_address(self) -> str:
        """Get the NFT position manager address."""
        return self.position_manager_address

    def get_mint_calldata(
        self,
        token0: str,
        token1: str,
        fee: int,
        tick_lower: int,
        tick_upper: int,
        amount0_desired: int,
        amount1_desired: int,
        amount0_min: int,
        amount1_min: int,
        recipient: str,
        deadline: int,
    ) -> bytes:
        """Generate calldata for minting a new LP position.

        Args:
            token0: Address of token0 (must be sorted, lower address first)
            token1: Address of token1 (must be sorted, higher address second)
            fee: Fee tier (500, 3000, 10000 for 0.05%, 0.3%, 1%)
            tick_lower: Lower tick bound for the position
            tick_upper: Upper tick bound for the position
            amount0_desired: Desired amount of token0 to deposit
            amount1_desired: Desired amount of token1 to deposit
            amount0_min: Minimum amount of token0 to deposit (slippage protection)
            amount1_min: Minimum amount of token1 to deposit (slippage protection)
            recipient: Address to receive the position NFT
            deadline: Transaction deadline (Unix timestamp)

        Returns:
            Encoded calldata for the mint transaction
        """
        # mint(MintParams) selector
        selector = NFT_POSITION_MINT_SELECTOR

        # Encode MintParams struct:
        # struct MintParams {
        #     address token0;
        #     address token1;
        #     uint24 fee;
        #     int24 tickLower;
        #     int24 tickUpper;
        #     uint256 amount0Desired;
        #     uint256 amount1Desired;
        #     uint256 amount0Min;
        #     uint256 amount1Min;
        #     address recipient;
        #     uint256 deadline;
        # }

        params = (
            self._pad_address(token0)
            + self._pad_address(token1)
            + self._pad_uint24(fee)
            + self._pad_int24(tick_lower)
            + self._pad_int24(tick_upper)
            + self._pad_uint256(amount0_desired)
            + self._pad_uint256(amount1_desired)
            + self._pad_uint256(amount0_min)
            + self._pad_uint256(amount1_min)
            + self._pad_address(recipient)
            + self._pad_uint256(deadline)
        )

        return bytes.fromhex(selector[2:] + params)

    def get_decrease_liquidity_calldata(
        self,
        token_id: int,
        liquidity: int,
        amount0_min: int,
        amount1_min: int,
        deadline: int,
    ) -> bytes:
        """Generate calldata for decreasing liquidity in a position.

        Args:
            token_id: NFT token ID of the position
            liquidity: Amount of liquidity to remove
            amount0_min: Minimum amount of token0 to receive
            amount1_min: Minimum amount of token1 to receive
            deadline: Transaction deadline (Unix timestamp)

        Returns:
            Encoded calldata for the decreaseLiquidity transaction
        """
        # decreaseLiquidity(DecreaseLiquidityParams) selector
        selector = NFT_POSITION_DECREASE_SELECTOR

        # Encode DecreaseLiquidityParams struct:
        # struct DecreaseLiquidityParams {
        #     uint256 tokenId;
        #     uint128 liquidity;
        #     uint256 amount0Min;
        #     uint256 amount1Min;
        #     uint256 deadline;
        # }

        params = (
            self._pad_uint256(token_id)
            + self._pad_uint128(liquidity)
            + self._pad_uint256(amount0_min)
            + self._pad_uint256(amount1_min)
            + self._pad_uint256(deadline)
        )

        return bytes.fromhex(selector[2:] + params)

    def get_collect_calldata(
        self,
        token_id: int,
        recipient: str,
        amount0_max: int,
        amount1_max: int,
    ) -> bytes:
        """Generate calldata for collecting tokens from a position.

        This collects both:
        - Tokens from decreased liquidity
        - Accumulated trading fees

        Args:
            token_id: NFT token ID of the position
            recipient: Address to receive the collected tokens
            amount0_max: Maximum amount of token0 to collect
            amount1_max: Maximum amount of token1 to collect

        Returns:
            Encoded calldata for the collect transaction
        """
        # collect(CollectParams) selector
        selector = NFT_POSITION_COLLECT_SELECTOR

        # Encode CollectParams struct:
        # struct CollectParams {
        #     uint256 tokenId;
        #     address recipient;
        #     uint128 amount0Max;
        #     uint128 amount1Max;
        # }

        params = (
            self._pad_uint256(token_id)
            + self._pad_address(recipient)
            + self._pad_uint128(amount0_max)
            + self._pad_uint128(amount1_max)
        )

        return bytes.fromhex(selector[2:] + params)

    def get_burn_calldata(self, token_id: int) -> bytes:
        """Generate calldata for burning a position NFT.

        Note: The position must be empty (all liquidity removed and collected)
        before burning.

        Args:
            token_id: NFT token ID of the position to burn

        Returns:
            Encoded calldata for the burn transaction
        """
        # burn(uint256 tokenId) selector
        selector = NFT_POSITION_BURN_SELECTOR

        params = self._pad_uint256(token_id)

        return bytes.fromhex(selector[2:] + params)

    def estimate_mint_gas(self) -> int:
        """Estimate gas for minting a new position (chain-aware)."""
        return get_gas_estimate(self.chain, "lp_mint")

    def estimate_close_gas(self, collect_fees: bool) -> int:
        """Estimate gas for closing a position (decrease + collect + optional burn).

        Args:
            collect_fees: Whether fees will be collected (always True for close)

        Returns:
            Total estimated gas for the close operation (chain-aware)
        """
        # decreaseLiquidity + collect + burn
        gas = get_gas_estimate(self.chain, "lp_decrease_liquidity")
        gas += get_gas_estimate(self.chain, "lp_collect")
        gas += get_gas_estimate(self.chain, "lp_burn")
        return gas

    @staticmethod
    def _pad_address(addr: str) -> str:
        """Pad address to 32 bytes."""
        addr_clean = addr.lower().replace("0x", "")
        return addr_clean.zfill(64)

    @staticmethod
    def _pad_uint256(value: int) -> str:
        """Pad uint256 to 32 bytes."""
        return hex(value)[2:].zfill(64)

    @staticmethod
    def _pad_uint128(value: int) -> str:
        """Pad uint128 to 32 bytes."""
        return hex(value)[2:].zfill(64)

    @staticmethod
    def _pad_uint24(value: int) -> str:
        """Pad uint24 to 32 bytes."""
        return hex(value)[2:].zfill(64)

    @staticmethod
    def _pad_int24(value: int) -> str:
        """Pad int24 to 32 bytes (signed, two's complement)."""
        if value < 0:
            # Two's complement for negative int24
            # int24 range: -8388608 to 8388607
            value = (1 << 256) + value
        return hex(value)[2:].zfill(64)


class LendingProtocolAdapter(Protocol):
    """Protocol interface for lending adapters."""

    def get_supply_calldata(
        self,
        asset: str,
        amount: int,
        on_behalf_of: str,
    ) -> bytes:
        """Generate calldata for supplying collateral."""
        ...

    def get_borrow_calldata(
        self,
        asset: str,
        amount: int,
        interest_rate_mode: int,
        on_behalf_of: str,
    ) -> bytes:
        """Generate calldata for borrowing tokens."""
        ...

    def get_repay_calldata(
        self,
        asset: str,
        amount: int,
        interest_rate_mode: int,
        on_behalf_of: str,
    ) -> bytes:
        """Generate calldata for repaying borrowed tokens."""
        ...

    def get_pool_address(self) -> str:
        """Get the lending pool address for this protocol."""
        ...

    def estimate_supply_gas(self) -> int:
        """Estimate gas for supply operation."""
        ...

    def estimate_borrow_gas(self) -> int:
        """Estimate gas for borrow operation."""
        ...

    def estimate_repay_gas(self) -> int:
        """Estimate gas for repay operation."""
        ...


class AaveV3Adapter:
    """Lending adapter for Aave V3 protocol.

    This adapter generates calldata for interacting with Aave V3 lending pools,
    supporting supply, borrow, and repay operations.

    Aave V3 features:
    - Efficiency Mode (E-Mode) for higher LTVs between correlated assets
    - Isolation Mode for new assets with limited debt ceiling
    - Variable and stable interest rates (stable being deprecated)
    """

    def __init__(self, chain: str, protocol: str = "aave_v3") -> None:
        """Initialize the adapter.

        Args:
            chain: Target blockchain
            protocol: Protocol name for pool lookup
        """
        self.chain = chain
        self.protocol = protocol

        # Get pool address
        chain_pools = LENDING_POOL_ADDRESSES.get(chain, {})
        self.pool_address = chain_pools.get(protocol, "0x0000000000000000000000000000000000000000")

    def get_pool_address(self) -> str:
        """Get the Aave V3 Pool address."""
        return self.pool_address

    def get_supply_calldata(
        self,
        asset: str,
        amount: int,
        on_behalf_of: str,
    ) -> bytes:
        """Generate calldata for supplying assets to Aave V3.

        Aave V3 supply function:
        supply(address asset, uint256 amount, address onBehalfOf, uint16 referralCode)

        Args:
            asset: Token address to supply
            amount: Amount to supply (in token's smallest units)
            on_behalf_of: Address to credit with the supply

        Returns:
            Encoded calldata for the supply transaction
        """
        # No referral code (0)
        referral_code = 0

        params = (
            self._pad_address(asset)
            + self._pad_uint256(amount)
            + self._pad_address(on_behalf_of)
            + self._pad_uint16(referral_code)
        )

        return bytes.fromhex(AAVE_SUPPLY_SELECTOR[2:] + params)

    def get_borrow_calldata(
        self,
        asset: str,
        amount: int,
        interest_rate_mode: int,
        on_behalf_of: str,
    ) -> bytes:
        """Generate calldata for borrowing from Aave V3.

        Aave V3 borrow function:
        borrow(address asset, uint256 amount, uint256 interestRateMode,
               uint16 referralCode, address onBehalfOf)

        Args:
            asset: Token address to borrow
            amount: Amount to borrow (in token's smallest units)
            interest_rate_mode: 1 for stable (deprecated), 2 for variable
            on_behalf_of: Address to debit with the borrow

        Returns:
            Encoded calldata for the borrow transaction
        """
        # No referral code (0)
        referral_code = 0

        params = (
            self._pad_address(asset)
            + self._pad_uint256(amount)
            + self._pad_uint256(interest_rate_mode)
            + self._pad_uint16(referral_code)
            + self._pad_address(on_behalf_of)
        )

        return bytes.fromhex(AAVE_BORROW_SELECTOR[2:] + params)

    def get_repay_calldata(
        self,
        asset: str,
        amount: int,
        interest_rate_mode: int,
        on_behalf_of: str,
    ) -> bytes:
        """Generate calldata for repaying borrowed tokens to Aave V3.

        Aave V3 repay function:
        repay(address asset, uint256 amount, uint256 interestRateMode, address onBehalfOf)

        To repay the full debt, pass MAX_UINT256 as amount.

        Args:
            asset: Token address to repay
            amount: Amount to repay (in token's smallest units), MAX_UINT256 for full
            interest_rate_mode: 1 for stable (deprecated), 2 for variable
            on_behalf_of: Address that has the debt being repaid

        Returns:
            Encoded calldata for the repay transaction
        """
        params = (
            self._pad_address(asset)
            + self._pad_uint256(amount)
            + self._pad_uint256(interest_rate_mode)
            + self._pad_address(on_behalf_of)
        )

        return bytes.fromhex(AAVE_REPAY_SELECTOR[2:] + params)

    def get_withdraw_calldata(
        self,
        asset: str,
        amount: int,
        to: str,
    ) -> bytes:
        """Generate calldata for withdrawing supplied assets from Aave V3.

        Aave V3 withdraw function:
        withdraw(address asset, uint256 amount, address to)

        To withdraw all supplied assets, pass MAX_UINT256 as amount.

        Args:
            asset: Token address to withdraw
            amount: Amount to withdraw (in token's smallest units), MAX_UINT256 for full
            to: Address to receive the withdrawn tokens

        Returns:
            Encoded calldata for the withdraw transaction
        """
        params = self._pad_address(asset) + self._pad_uint256(amount) + self._pad_address(to)

        return bytes.fromhex(AAVE_WITHDRAW_SELECTOR[2:] + params)

    def get_set_collateral_calldata(
        self,
        asset: str,
        use_as_collateral: bool,
    ) -> bytes:
        """Generate calldata for enabling/disabling an asset as collateral.

        Aave V3 setUserUseReserveAsCollateral function:
        setUserUseReserveAsCollateral(address asset, bool useAsCollateral)

        This must be called after supplying to enable borrowing against the asset.

        Args:
            asset: Token address to enable/disable as collateral
            use_as_collateral: True to enable, False to disable

        Returns:
            Encoded calldata for the setUserUseReserveAsCollateral transaction
        """
        params = self._pad_address(asset) + self._pad_uint256(1 if use_as_collateral else 0)

        return bytes.fromhex(AAVE_SET_COLLATERAL_SELECTOR[2:] + params)

    def estimate_set_collateral_gas(self) -> int:
        """Estimate gas for setUserUseReserveAsCollateral operation."""
        return 150000  # Aave V3 can use more gas with incentives

    def estimate_supply_gas(self) -> int:
        """Estimate gas for supply operation."""
        return DEFAULT_GAS_ESTIMATES["lending_supply"]

    def estimate_borrow_gas(self) -> int:
        """Estimate gas for borrow operation."""
        return DEFAULT_GAS_ESTIMATES["lending_borrow"]

    def estimate_repay_gas(self) -> int:
        """Estimate gas for repay operation."""
        return DEFAULT_GAS_ESTIMATES["lending_repay"]

    def estimate_withdraw_gas(self) -> int:
        """Estimate gas for withdraw operation."""
        return DEFAULT_GAS_ESTIMATES["lending_withdraw"]

    def estimate_flash_loan_gas(self) -> int:
        """Estimate gas for flash loan operation (base only, not including callbacks)."""
        return DEFAULT_GAS_ESTIMATES["flash_loan"]

    def estimate_flash_loan_simple_gas(self) -> int:
        """Estimate gas for simple flash loan operation (base only, not including callbacks)."""
        return DEFAULT_GAS_ESTIMATES["flash_loan_simple"]

    def get_flash_loan_simple_calldata(
        self,
        receiver_address: str,
        asset: str,
        amount: int,
        params: bytes = b"",
    ) -> bytes:
        """Generate calldata for a simple (single-asset) flash loan.

        Aave V3 flashLoanSimple function:
        flashLoanSimple(
            address receiverAddress,
            address asset,
            uint256 amount,
            bytes calldata params,
            uint16 referralCode
        )

        The receiver contract must implement executeOperation() and return the
        borrowed amount plus premium (0.09% on Aave) within the same transaction.

        Args:
            receiver_address: Contract that will receive and handle the flash loan
            asset: Token address to borrow
            amount: Amount to borrow (in token's smallest units)
            params: Extra data to pass to receiver's executeOperation

        Returns:
            Encoded calldata for the flashLoanSimple transaction
        """
        # Calculate params offset (after fixed params: 5 * 32 bytes)
        params_offset = 5 * 32  # receiver(32) + asset(32) + amount(32) + paramsOffset(32) + referralCode(32)

        # Encode params data
        params_hex = params.hex() if params else ""
        params_len = len(params)

        encoded = (
            self._pad_address(receiver_address)
            + self._pad_address(asset)
            + self._pad_uint256(amount)
            + self._pad_uint256(params_offset)
            + self._pad_uint16(0)  # referral code
            + self._pad_uint256(params_len)
        )

        if params_len > 0:
            # Pad params to 32-byte boundary
            padded_params = params_hex + "0" * ((64 - len(params_hex) % 64) % 64)
            encoded += padded_params

        return bytes.fromhex(AAVE_FLASH_LOAN_SIMPLE_SELECTOR[2:] + encoded)

    def get_flash_loan_calldata(
        self,
        receiver_address: str,
        assets: list[str],
        amounts: list[int],
        modes: list[int],
        on_behalf_of: str,
        params: bytes = b"",
    ) -> bytes:
        """Generate calldata for a multi-asset flash loan.

        Aave V3 flashLoan function:
        flashLoan(
            address receiverAddress,
            address[] calldata assets,
            uint256[] calldata amounts,
            uint256[] calldata modes,
            address onBehalfOf,
            bytes calldata params,
            uint16 referralCode
        )

        Modes:
        - 0: No debt opened (must repay within same transaction) - for atomic arb
        - 1: Open stable rate debt
        - 2: Open variable rate debt

        Args:
            receiver_address: Contract that will receive and handle the flash loan
            assets: List of token addresses to borrow
            amounts: List of amounts to borrow (in token's smallest units)
            modes: List of debt modes (0, 1, or 2) for each asset
            on_behalf_of: Address to receive debt if mode != 0
            params: Extra data to pass to receiver's executeOperation

        Returns:
            Encoded calldata for the flashLoan transaction
        """
        n_assets = len(assets)

        # Calculate offsets for dynamic arrays
        # Fixed params before arrays: receiverAddress(32) + 3 array offsets(32*3) + onBehalfOf(32) + params offset(32) + referralCode(32) = 7*32
        assets_offset = 7 * 32
        amounts_offset = assets_offset + 32 + n_assets * 32  # length(32) + data(32*n)
        modes_offset = amounts_offset + 32 + n_assets * 32
        params_offset = modes_offset + 32 + n_assets * 32

        # Build header
        encoded = self._pad_address(receiver_address)
        encoded += self._pad_uint256(assets_offset)
        encoded += self._pad_uint256(amounts_offset)
        encoded += self._pad_uint256(modes_offset)
        encoded += self._pad_address(on_behalf_of)
        encoded += self._pad_uint256(params_offset)
        encoded += self._pad_uint16(0)  # referral code

        # Encode assets array
        encoded += self._pad_uint256(n_assets)
        for addr in assets:
            encoded += self._pad_address(addr)

        # Encode amounts array
        encoded += self._pad_uint256(n_assets)
        for amount_val in amounts:
            encoded += self._pad_uint256(amount_val)

        # Encode modes array
        encoded += self._pad_uint256(n_assets)
        for mode in modes:
            encoded += self._pad_uint256(mode)

        # Encode params
        params_hex = params.hex() if params else ""
        params_len = len(params)
        encoded += self._pad_uint256(params_len)
        if params_len > 0:
            padded_params = params_hex + "0" * ((64 - len(params_hex) % 64) % 64)
            encoded += padded_params

        return bytes.fromhex(AAVE_FLASH_LOAN_SELECTOR[2:] + encoded)

    @staticmethod
    def _pad_address(addr: str) -> str:
        """Pad address to 32 bytes."""
        addr_clean = addr.lower().replace("0x", "")
        return addr_clean.zfill(64)

    @staticmethod
    def _pad_uint256(value: int) -> str:
        """Pad uint256 to 32 bytes."""
        return hex(value)[2:].zfill(64)

    @staticmethod
    def _pad_uint16(value: int) -> str:
        """Pad uint16 to 32 bytes."""
        return hex(value)[2:].zfill(64)


class BalancerAdapter:
    """Flash loan adapter for Balancer Vault.

    Balancer flash loans have zero fees (no premium), making them ideal for
    arbitrage strategies. The Vault contract holds all pool liquidity.

    Balancer Vault flash loan function:
    flashLoan(
        IFlashLoanRecipient recipient,
        IERC20[] memory tokens,
        uint256[] memory amounts,
        bytes memory userData
    )

    Key differences from Aave:
    - Zero fees (no premium to repay)
    - All tokens and amounts in arrays (batch flash loans native)
    - userData is arbitrary bytes passed to receiver
    - Receiver must implement receiveFlashLoan() not executeOperation()
    """

    def __init__(self, chain: str, protocol: str = "balancer") -> None:
        """Initialize the adapter.

        Args:
            chain: Target blockchain
            protocol: Protocol name (always "balancer")
        """
        self.chain = chain
        self.protocol = protocol

        # Get vault address
        self.vault_address = BALANCER_VAULT_ADDRESSES.get(chain, "0x0000000000000000000000000000000000000000")

    def get_vault_address(self) -> str:
        """Get the Balancer Vault address."""
        return self.vault_address

    def get_flash_loan_calldata(
        self,
        recipient: str,
        tokens: list[str],
        amounts: list[int],
        user_data: bytes = b"",
    ) -> bytes:
        """Generate calldata for a Balancer flash loan.

        Balancer flashLoan function:
        flashLoan(
            IFlashLoanRecipient recipient,
            IERC20[] memory tokens,
            uint256[] memory amounts,
            bytes memory userData
        )

        Args:
            recipient: Contract address that will receive and handle the flash loan
            tokens: List of token addresses to borrow
            amounts: List of amounts to borrow (in token's smallest units)
            user_data: Extra data to pass to receiver's receiveFlashLoan

        Returns:
            Encoded calldata for the flashLoan transaction
        """
        n_tokens = len(tokens)
        if n_tokens != len(amounts):
            raise ValueError("tokens and amounts must have same length")

        # ABI encoding for flashLoan(address,address[],uint256[],bytes)
        # Layout:
        # - recipient (32 bytes, padded address)
        # - offset to tokens array (32 bytes)
        # - offset to amounts array (32 bytes)
        # - offset to userData (32 bytes)
        # - tokens array: length (32) + addresses (32 * n)
        # - amounts array: length (32) + amounts (32 * n)
        # - userData: length (32) + data (padded to 32)

        # Calculate offsets
        # Fixed header: recipient(32) + 3 offsets(32*3) = 128 bytes
        tokens_offset = 128
        amounts_offset = tokens_offset + 32 + n_tokens * 32
        user_data_offset = amounts_offset + 32 + n_tokens * 32

        # Build header
        encoded = self._pad_address(recipient)
        encoded += self._pad_uint256(tokens_offset)
        encoded += self._pad_uint256(amounts_offset)
        encoded += self._pad_uint256(user_data_offset)

        # Encode tokens array
        encoded += self._pad_uint256(n_tokens)
        for token in tokens:
            encoded += self._pad_address(token)

        # Encode amounts array
        encoded += self._pad_uint256(n_tokens)
        for amount in amounts:
            encoded += self._pad_uint256(amount)

        # Encode userData
        user_data_hex = user_data.hex() if user_data else ""
        user_data_len = len(user_data)
        encoded += self._pad_uint256(user_data_len)
        if user_data_len > 0:
            # Pad to 32-byte boundary
            padded_data = user_data_hex + "0" * ((64 - len(user_data_hex) % 64) % 64)
            encoded += padded_data

        return bytes.fromhex(BALANCER_FLASH_LOAN_SELECTOR[2:] + encoded)

    def get_flash_loan_simple_calldata(
        self,
        recipient: str,
        token: str,
        amount: int,
        user_data: bytes = b"",
    ) -> bytes:
        """Generate calldata for a single-token flash loan.

        This is a convenience method that wraps get_flash_loan_calldata
        for single-token flash loans.

        Args:
            recipient: Contract address that will receive the flash loan
            token: Token address to borrow
            amount: Amount to borrow (in token's smallest units)
            user_data: Extra data to pass to receiver's receiveFlashLoan

        Returns:
            Encoded calldata for the flashLoan transaction
        """
        return self.get_flash_loan_calldata(
            recipient=recipient,
            tokens=[token],
            amounts=[amount],
            user_data=user_data,
        )

    def estimate_flash_loan_gas(self) -> int:
        """Estimate gas for a multi-token flash loan (base only, not including callbacks)."""
        return DEFAULT_GAS_ESTIMATES["balancer_flash_loan"]

    def estimate_flash_loan_simple_gas(self) -> int:
        """Estimate gas for a single-token flash loan (base only, not including callbacks)."""
        return DEFAULT_GAS_ESTIMATES["balancer_flash_loan_simple"]

    @staticmethod
    def _pad_address(addr: str) -> str:
        """Pad an address to 32 bytes (64 hex chars)."""
        clean_addr = addr.lower().replace("0x", "")
        return clean_addr.zfill(64)

    @staticmethod
    def _pad_uint256(value: int) -> str:
        """Pad a uint256 to 32 bytes (64 hex chars)."""
        return hex(value)[2:].zfill(64)


# =============================================================================
# Intent Compiler
# =============================================================================


class IntentCompiler:
    """Compiles Intents into executable ActionBundles.

    The IntentCompiler takes high-level trading intents and converts them
    into low-level transaction data ready for execution on-chain.

    Example:
        compiler = IntentCompiler(
            chain="arbitrum",
            wallet_address="0x...",
            rpc_url="https://arb1.arbitrum.io/rpc",
        )
        intent = Intent.swap("USDC", "ETH", amount_usd=Decimal("1000"))
        result = compiler.compile(intent)
        if result.status == CompilationStatus.SUCCESS:
            # Execute result.action_bundle
            pass
    """

    def __init__(
        self,
        chain: str = "arbitrum",
        wallet_address: str = "0x0000000000000000000000000000000000000000",
        default_protocol: str = "uniswap_v3",
        price_oracle: dict[str, Decimal] | None = None,
        default_deadline_seconds: int = 300,
        rpc_url: str | None = None,
        rpc_timeout: float = 10.0,
        default_lp_slippage: Decimal = Decimal("0.99"),
        config: IntentCompilerConfig | None = None,
        gateway_client: "GatewayClient | None" = None,
        token_resolver: "TokenResolverType | None" = None,
        chain_wallets: dict[str, str] | None = None,
    ) -> None:
        """Initialize the compiler.

        Args:
            chain: Target blockchain (ethereum, arbitrum, etc.)
            wallet_address: Address that will execute transactions
            default_protocol: Default DEX protocol for swaps
            price_oracle: Price oracle dict (token -> USD price). Required for
                production use to calculate accurate slippage amounts.
            default_deadline_seconds: Default transaction deadline
            rpc_url: RPC URL for on-chain queries (needed for LP close).
                DEPRECATED: Use gateway_client instead for production deployments.
            rpc_timeout: HTTP timeout for direct RPC calls in seconds.
            default_lp_slippage: Default slippage for LP operations (0.99 = 99%).
                This controls the minimum acceptable amounts when adding/removing liquidity.
                LP operations differ from swaps - for concentrated liquidity, the actual
                deposit ratio depends heavily on where the current price is relative to
                your tick range. A price near the range edge means most liquidity is in
                one token. Default 99% allows nearly full flexibility for this behavior.
                Can be lowered for tighter protection if needed.
            config: Optional configuration. If not provided, defaults to
                IntentCompilerConfig() which requires price_oracle.
            gateway_client: Optional gateway client for RPC queries. When provided,
                all on-chain queries (allowance, balance, position liquidity) go through
                the gateway instead of direct RPC. This is the preferred mode for
                production deployments where strategies run in isolated containers.
            token_resolver: Optional TokenResolver instance for token resolution.
                If not provided, uses the singleton instance from get_token_resolver().
                The resolver provides unified token lookup with caching and on-chain
                discovery support.

        Raises:
            ValueError: If no price_oracle is provided and allow_placeholder_prices is False.
        """
        # Use default config if not provided
        self._config = config or IntentCompilerConfig()

        # Validate price_oracle requirement
        self._using_placeholders = price_oracle is None
        if self._using_placeholders and not self._config.allow_placeholder_prices:
            raise ValueError(
                "IntentCompiler requires a price_oracle for production use. "
                "Pass a dict mapping token symbols to USD prices (e.g., {'ETH': Decimal('3400')}) "
                "or set config=IntentCompilerConfig(allow_placeholder_prices=True) for testing only. "
                "Using placeholder prices will cause incorrect slippage calculations and swap reverts."
            )

        # Normalize chain name (e.g., "bnb" -> "bsc") via central resolver
        try:
            from almanak.core.constants import resolve_chain_name

            self.chain = resolve_chain_name(chain)
        except (ValueError, ImportError):
            self.chain = chain
        self.wallet_address = wallet_address
        # Normalize protocol alias (e.g., "agni" -> "agni_finance" on mantle)
        from ..connectors.protocol_aliases import normalize_protocol

        self.default_protocol = normalize_protocol(self.chain, default_protocol)
        self.default_deadline_seconds = default_deadline_seconds
        self.rpc_url = rpc_url
        self.rpc_timeout = rpc_timeout
        self._web3: Web3 | None = None
        self._gateway_client = gateway_client
        self._chain_wallets = chain_wallets

        # LP slippage configuration (0.99 = 99% default, allows concentrated liquidity flexibility)
        self.default_lp_slippage = min(max(default_lp_slippage, Decimal("0")), Decimal("1"))

        # Token resolver - use provided or default singleton (lazy import to avoid circular dependency)
        if token_resolver is None:
            from ..data.tokens import get_token_resolver

            token_resolver = get_token_resolver()
        self._token_resolver = token_resolver

        # Price oracle - use provided or fall back to placeholders (only if allowed)
        self.price_oracle: dict[str, Decimal] | None
        if self._using_placeholders:
            logger.debug(
                "IntentCompiler created without price oracle, will use placeholders if not updated before compilation"
            )
            self.price_oracle = self._get_placeholder_prices()
        else:
            self.price_oracle = price_oracle
        self._placeholder_warning_logged = False

        # Allowance cache (token -> spender -> amount)
        self._allowance_cache: dict[str, dict[str, int]] = {}
        # Log stablecoin price fallbacks once per symbol per compiler instance.
        self._stablecoin_fallback_logged: set[str] = set()

        # Polymarket adapter for prediction market intents (Polygon only)
        self._polymarket_adapter: PolymarketAdapter | None = None
        self._bridge_selector: BridgeSelector | None = None
        self._init_polymarket_adapter()

        # Cached Solana adapter instances (lazily initialized)
        self._cached_jupiter_adapter: Any = None
        self._cached_kamino_adapter: Any = None
        self._cached_kamino_adapter_with_rpc: Any = None
        self._cached_jupiter_lend_adapter: Any = None
        self._cached_raydium_adapter: Any = None
        self._cached_raydium_adapter_with_rpc: Any = None
        self._cached_meteora_adapter: Any = None
        self._cached_meteora_adapter_with_rpc: Any = None
        self._cached_orca_adapter: Any = None
        self._cached_orca_adapter_with_rpc: Any = None
        self._cached_drift_adapter: Any = None

        effective_protocol = "jupiter" if self._is_solana_chain() else default_protocol
        logger.info(
            f"IntentCompiler initialized for chain={chain}, wallet={wallet_address[:10]}..., protocol={effective_protocol}, using_placeholders={self._using_placeholders}"
        )

    def update_prices(self, prices: dict[str, Decimal]) -> None:
        """Update the price oracle with real prices, clearing placeholder state."""
        self.price_oracle = prices
        self._using_placeholders = False

    def restore_prices(self, original_oracle: dict[str, Decimal] | None, original_using_placeholders: bool) -> None:
        """Restore prices to a previous state (used after temporary override)."""
        self.price_oracle = original_oracle
        self._using_placeholders = original_using_placeholders

    def _resolve_protocol(self, intent_protocol: str | None) -> str:
        """Resolve intent protocol to canonical key, falling back to default.

        Normalizes aliases (e.g., "agni" -> "agni_finance" on mantle) and falls
        back to self.default_protocol if intent_protocol is None.
        """
        if intent_protocol is None:
            return self.default_protocol
        from ..connectors.protocol_aliases import normalize_protocol

        return normalize_protocol(self.chain, intent_protocol)

    def _init_polymarket_adapter(self) -> None:
        """Initialize Polymarket adapter if on Polygon and config is available.

        This method lazily initializes the PolymarketAdapter for prediction market
        intents. The adapter is only initialized when:
        1. The chain is 'polygon' (case-insensitive)
        2. A PolymarketConfig is provided in the IntentCompilerConfig

        If on Polygon without a PolymarketConfig, the method silently returns.
        VIB-307: Warning is deferred to compile time so non-prediction Polygon
        strategies don't see noisy Polymarket warnings at startup.

        This lazy initialization ensures:
        - Non-Polygon usage is unaffected (no import overhead)
        - Missing config is handled gracefully
        - Clear error messages when prediction intents are attempted without config
        """
        # Only initialize for Polygon chain
        if self.chain.lower() != "polygon":
            return

        # Check if config is provided -- silently skip if not.
        # VIB-307: Warning deferred to compile time so non-prediction strategies on Polygon
        # don't see noisy Polymarket warnings at startup.
        polymarket_config = self._config.polymarket_config
        if polymarket_config is None:
            return

        # Lazy import to avoid circular imports and allow optional usage
        try:
            from ..connectors.polymarket.adapter import PolymarketAdapter

            # Initialize web3 for redemption intents if rpc_url is available
            web3_instance = None
            if self.rpc_url:
                from web3 import Web3

                if self._web3 is None:
                    self._web3 = Web3(Web3.HTTPProvider(self.rpc_url))
                web3_instance = self._web3
                logger.debug("Web3 instance initialized for PolymarketAdapter (redemption support enabled)")

            self._polymarket_adapter = PolymarketAdapter(polymarket_config, web3=web3_instance)
            logger.info(f"PolymarketAdapter initialized for wallet={polymarket_config.wallet_address[:10]}...")
        except ImportError as e:
            logger.warning(f"Failed to import PolymarketAdapter: {e}. Prediction market intents will not be available.")
        except Exception as e:
            logger.warning(
                f"Failed to initialize PolymarketAdapter: {e}. Prediction market intents will not be available."
            )

    def _get_chain_rpc_url(self) -> str | None:
        """Get RPC URL for the current chain.

        If rpc_url is set on the compiler, use it. Otherwise, check if a managed
        Anvil fork is running (via ANVIL_{CHAIN}_PORT env var set by managed.py),
        and use that. Finally, fall back to the gateway's RPC provider.

        This is needed for protocol adapters (like Aerodrome, TraderJoe, Pendle)
        that need to make direct RPC calls for pool queries when the compiler is
        using gateway mode (rpc_url=None).

        Returns:
            RPC URL string or None if not available.
        """
        if self.rpc_url:
            return self.rpc_url

        # Check if a managed Anvil fork is running for this chain.
        # managed.py sets ANVIL_{CHAIN}_PORT when it starts an Anvil fork.
        # This MUST take priority over mainnet RPC so that protocol adapters
        # (e.g., TraderJoe, Aerodrome) query on-chain state from the fork
        # where LP positions actually exist, not mainnet.
        anvil_port_var = f"ANVIL_{self.chain.upper()}_PORT"
        anvil_port = os.environ.get(anvil_port_var)
        if anvil_port:
            anvil_url = f"http://127.0.0.1:{anvil_port}"
            logger.debug(
                f"Anvil fork detected for {self.chain} ({anvil_port_var}={anvil_port}), using fork URL: {anvil_url}"
            )
            return anvil_url

        try:
            from almanak.gateway.utils import get_rpc_url

            rpc_url = get_rpc_url(self.chain)
        except (ImportError, ValueError) as e:
            logger.debug(f"Failed to fetch mainnet RPC URL for {self.chain}: {e}")
        else:
            logger.debug(f"Fetched RPC URL for {self.chain} from gateway utils")
            return rpc_url

        # Fallback: try Anvil ONLY if no RPC source is configured.
        # If an RPC source is set but resolution failed (bad key, unsupported chain),
        # we should fail fast - not silently switch to localhost.
        from almanak.gateway.utils.rpc_provider import has_api_key_configured

        if has_api_key_configured():
            logger.warning(
                f"RPC source is configured but resolution failed for {self.chain}. Not falling back to Anvil."
            )
            return None

        try:
            from almanak.gateway.utils import get_rpc_url

            rpc_url = get_rpc_url(self.chain, network="anvil")
        except (ImportError, ValueError) as e:
            logger.warning(f"Failed to get RPC URL for {self.chain} (no API key, Anvil also unavailable): {e}")
            return None
        else:
            logger.debug(f"No API key configured, using Anvil RPC for {self.chain}: {rpc_url}")
            return rpc_url

    def _is_wallet_contract(self) -> bool | None:
        """Check if the wallet address is a contract (has bytecode).

        Uses eth_getCode via RPC to check if the wallet has deployed bytecode.
        Flash loans require a contract wallet to handle callbacks.

        Returns:
            True if contract, False if EOA, None if RPC unavailable.
        """
        rpc_url = self._get_chain_rpc_url()
        if not rpc_url:
            return None

        try:
            import httpx

            response = httpx.post(
                rpc_url,
                json={
                    "jsonrpc": "2.0",
                    "method": "eth_getCode",
                    "params": [self.wallet_address, "latest"],
                    "id": 1,
                },
                timeout=self.rpc_timeout,
            )
            response.raise_for_status()
            data = response.json()
            if not isinstance(data, dict) or data.get("error") is not None:
                logger.debug("eth_getCode RPC error: %s", data)
                return None
            code = data.get("result")
            # EOA wallets return "0x" (empty bytecode)
            return code not in ("0x", "0x0", "", None)
        except Exception as e:
            logger.debug(f"Failed to check wallet bytecode via eth_getCode: {e}")
            return None

    def _validate_pool(self, result: "PoolValidationResult", intent_id: str) -> CompilationResult | None:
        """Check pool validation result and return FAILED CompilationResult if pool doesn't exist.

        Args:
            result: Pool validation result from pool_validation module.
            intent_id: Intent ID for error reporting.

        Returns:
            CompilationResult with FAILED status if pool doesn't exist, None if OK to proceed.
        """
        if result.exists is False:
            return CompilationResult(
                status=CompilationStatus.FAILED,
                error=result.error or "Pool does not exist",
                intent_id=intent_id,
            )
        if result.warning:
            logger.warning("Pool validation: %s", result.warning)
        return None

    @property
    def polymarket_adapter(self) -> "PolymarketAdapter | None":
        """Get the Polymarket adapter for prediction market intents.

        Returns:
            PolymarketAdapter if initialized, None otherwise.
        """
        return self._polymarket_adapter

    def compile(self, intent: AnyIntent) -> CompilationResult:
        """Compile an intent into an ActionBundle.

        This is the main entry point for compiling intents. It dispatches
        to the appropriate handler based on intent type.

        Args:
            intent: The intent to compile

        Returns:
            CompilationResult with ActionBundle and metadata
        """
        try:
            intent_type = intent.intent_type

            # Suppress placeholder price warning for intent types that don't use prices.
            # STAKE/UNSTAKE amounts are in native units, not USD, so no price conversion needed.
            # HOLD is a no-op with no transactions.
            _price_irrelevant = intent_type in (
                IntentType.STAKE,
                IntentType.UNSTAKE,
                IntentType.HOLD,
                IntentType.UNWRAP_NATIVE,
            )
            if self._using_placeholders and not self._placeholder_warning_logged and not _price_irrelevant:
                logger.warning(
                    "IntentCompiler using PLACEHOLDER PRICES. Slippage calculations will be INCORRECT. "
                    "This is only acceptable for unit tests."
                )
                self._placeholder_warning_logged = True

            if intent_type == IntentType.SWAP:
                return self._compile_swap(intent)  # type: ignore[arg-type]
            elif intent_type == IntentType.LP_OPEN:
                return self._compile_lp_open(intent)  # type: ignore[arg-type]
            elif intent_type == IntentType.LP_CLOSE:
                return self._compile_lp_close(intent)  # type: ignore[arg-type]
            elif intent_type == IntentType.LP_COLLECT_FEES:
                return self._compile_collect_fees(intent)  # type: ignore[arg-type]
            elif intent_type == IntentType.BORROW:
                return self._compile_borrow(intent)  # type: ignore[arg-type]
            elif intent_type == IntentType.REPAY:
                return self._compile_repay(intent)  # type: ignore[arg-type]
            elif intent_type == IntentType.SUPPLY:
                return self._compile_supply(intent)  # type: ignore[arg-type]
            elif intent_type == IntentType.WITHDRAW:
                return self._compile_withdraw(intent)  # type: ignore[arg-type]
            elif intent_type == IntentType.PERP_OPEN:
                return self._compile_perp_open(intent)  # type: ignore[arg-type]
            elif intent_type == IntentType.PERP_CLOSE:
                return self._compile_perp_close(intent)  # type: ignore[arg-type]
            elif intent_type == IntentType.HOLD:
                return self._compile_hold(intent)  # type: ignore[arg-type]
            elif intent_type == IntentType.FLASH_LOAN:
                return self._compile_flash_loan(intent)  # type: ignore[arg-type]
            elif intent_type == IntentType.STAKE:
                return self._compile_stake_intent(intent)  # type: ignore[arg-type]
            elif intent_type == IntentType.UNSTAKE:
                return self._compile_unstake_intent(intent)  # type: ignore[arg-type]
            elif intent_type == IntentType.PREDICTION_BUY:
                return self._compile_prediction_buy(intent)  # type: ignore[arg-type]
            elif intent_type == IntentType.PREDICTION_SELL:
                return self._compile_prediction_sell(intent)  # type: ignore[arg-type]
            elif intent_type == IntentType.PREDICTION_REDEEM:
                return self._compile_prediction_redeem(intent)  # type: ignore[arg-type]
            elif intent_type == IntentType.BRIDGE:
                return self._compile_bridge(intent)  # type: ignore[arg-type]
            elif intent_type == IntentType.VAULT_DEPOSIT:
                return self._compile_vault_deposit(intent)  # type: ignore[arg-type]
            elif intent_type == IntentType.VAULT_REDEEM:
                return self._compile_vault_redeem(intent)  # type: ignore[arg-type]
            elif intent_type == IntentType.ENSURE_BALANCE:
                return self._compile_ensure_balance(intent)
            elif intent_type == IntentType.WRAP_NATIVE:
                return self._compile_wrap_native(intent)  # type: ignore[arg-type]
            elif intent_type == IntentType.UNWRAP_NATIVE:
                return self._compile_unwrap_native(intent)  # type: ignore[arg-type]
            else:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"Intent type {intent_type.value} is not supported by the compiler",
                    intent_id=intent.intent_id,
                )

        except Exception as e:
            logger.exception(f"Failed to compile intent: {e}")
            return CompilationResult(
                status=CompilationStatus.FAILED,
                error=str(e),
                intent_id=intent.intent_id,
            )

    def _get_bridge_selector(self) -> "BridgeSelector":
        """Get lazily-initialized BridgeSelector with default bridge adapters."""
        if self._bridge_selector is not None:
            return self._bridge_selector

        from ..connectors.bridges.across.adapter import AcrossBridgeAdapter
        from ..connectors.bridges.selector import BridgeSelector
        from ..connectors.bridges.stargate.adapter import StargateBridgeAdapter

        bridges = [
            AcrossBridgeAdapter(token_resolver=self._token_resolver),
            StargateBridgeAdapter(token_resolver=self._token_resolver),
        ]
        self._bridge_selector = BridgeSelector(bridges=bridges)
        return self._bridge_selector

    def _compile_bridge(self, intent: "BridgeIntent") -> CompilationResult:
        """Compile a BRIDGE intent into an ActionBundle."""
        result = CompilationResult(
            status=CompilationStatus.SUCCESS,
            intent_id=intent.intent_id,
        )

        try:
            if intent.amount == "all":
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=(
                        "amount='all' must be resolved before compilation. "
                        "Use Intent.set_resolved_amount() to resolve chained amounts."
                    ),
                    intent_id=intent.intent_id,
                )

            amount_decimal = intent.amount
            if not isinstance(amount_decimal, Decimal):
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"Bridge amount must be Decimal after resolution, got: {type(amount_decimal).__name__}",
                    intent_id=intent.intent_id,
                )
            from_chain = intent.from_chain.lower()
            to_chain = intent.to_chain.lower()
            token_symbol = intent.token

            token_info = self._resolve_token(token_symbol, chain=from_chain)
            if token_info is None:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"Unknown token for bridge on {from_chain}: {token_symbol}",
                    intent_id=intent.intent_id,
                )

            selector = self._get_bridge_selector()

            # If preferred_bridge is set, exclude all other bridges
            preferred = getattr(intent, "preferred_bridge", None)
            excluded = None
            if preferred:
                excluded = [b.name.lower() for b in selector.bridges if b.name.lower() != preferred.lower()]

            if excluded:
                selection = selector.select_bridge_with_fallback(
                    token=token_symbol,
                    amount=amount_decimal,
                    from_chain=from_chain,
                    to_chain=to_chain,
                    max_slippage=intent.max_slippage,
                    excluded_bridges=excluded,
                )
            else:
                selection = selector.select_bridge(
                    token=token_symbol,
                    amount=amount_decimal,
                    from_chain=from_chain,
                    to_chain=to_chain,
                    max_slippage=intent.max_slippage,
                )
            if not selection.is_success or selection.bridge is None or selection.quote is None:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"No bridge available for {token_symbol} from {from_chain} to {to_chain}",
                    intent_id=intent.intent_id,
                )

            quote = selection.quote
            bridge = selection.bridge
            # Use destination_address from intent or resolve from wallet registry
            dest_wallet = getattr(intent, "destination_address", None) or self._resolve_dest_wallet(to_chain)
            bridge_tx = bridge.build_deposit_tx(quote=quote, recipient=dest_wallet)

            amount_in_wei: int | None = None
            if quote.route_data and "amount_wei" in quote.route_data:
                try:
                    amount_in_wei = int(quote.route_data["amount_wei"])
                except (ValueError, TypeError):
                    amount_in_wei = None
            if amount_in_wei is None:
                amount_in_wei = int(amount_decimal * Decimal(10**token_info.decimals))

            transactions: list[TransactionData] = []
            if not token_info.is_native:
                transactions.extend(
                    self._build_approve_tx(
                        token_address=token_info.address,
                        spender=bridge_tx["to"],
                        amount=amount_in_wei,
                    )
                )

            bridge_transaction = TransactionData(
                to=bridge_tx["to"],
                value=int(bridge_tx.get("value", 0)),
                data=bridge_tx["data"],
                gas_estimate=int(bridge_tx.get("gas_estimate", get_gas_estimate(from_chain, "bridge_deposit"))),
                description=f"Bridge {amount_decimal} {token_symbol} from {from_chain} to {to_chain} via {bridge.name}",
                tx_type="bridge_deposit",
            )
            transactions.append(bridge_transaction)

            metadata: dict[str, Any] = {
                "from_chain": from_chain,
                "to_chain": to_chain,
                "token": token_symbol,
                "amount": str(amount_decimal),
                "bridge": bridge.name,
                "estimated_time": int(quote.estimated_time_seconds),
                "fee": str(quote.fee_amount),
                "is_cross_chain": from_chain != to_chain,
                "route": {"from_chain": quote.from_chain, "to_chain": quote.to_chain},
                "quote_id": quote.quote_id,
            }

            action_bundle = ActionBundle(
                intent_type=IntentType.BRIDGE.value,
                transactions=[tx.to_dict() for tx in transactions],
                metadata=metadata,
            )

            result.action_bundle = action_bundle
            result.transactions = transactions
            result.total_gas_estimate = sum(tx.gas_estimate for tx in transactions)

            logger.info(
                f"Compiled BRIDGE intent: {amount_decimal} {token_symbol} {from_chain}->{to_chain} via {bridge.name}, "
                f"{len(transactions)} txs"
            )
        except Exception as e:
            logger.exception("Failed to compile BRIDGE intent")
            result.status = CompilationStatus.FAILED
            result.error = str(e)
        return result

    def _is_solana_chain(self) -> bool:
        """Check if the compiler's target chain is in the Solana family."""
        try:
            from almanak.core.enums import Chain, ChainFamily, get_chain_family

            chain_enum = Chain(self.chain.upper())
            return get_chain_family(chain_enum) == ChainFamily.SOLANA
        except (ValueError, KeyError):
            return False

    # =========================================================================
    # Solana adapter caching helpers
    # =========================================================================

    def _get_jupiter_adapter(self) -> Any:
        """Get or create a cached JupiterAdapter instance."""
        if self._cached_jupiter_adapter is None:
            from almanak.framework.connectors.jupiter import JupiterAdapter, JupiterConfig

            config = JupiterConfig(wallet_address=self.wallet_address)
            self._cached_jupiter_adapter = JupiterAdapter(
                config=config,
                price_provider=self.price_oracle,
                allow_placeholder_prices=self.price_oracle is None,
                token_resolver=self._token_resolver,
            )
        else:
            # Update price provider on cached adapter in case prices have changed
            self._cached_jupiter_adapter.price_provider = self.price_oracle
            self._cached_jupiter_adapter.allow_placeholder_prices = self.price_oracle is None
        return self._cached_jupiter_adapter

    def _get_kamino_adapter(self, *, needs_rpc: bool = False) -> Any:
        """Get or create a cached KaminoAdapter instance."""
        if needs_rpc:
            if self._cached_kamino_adapter_with_rpc is None:
                from almanak.framework.connectors.kamino import KaminoAdapter, KaminoConfig

                config = KaminoConfig(wallet_address=self.wallet_address)
                self._cached_kamino_adapter_with_rpc = KaminoAdapter(config=config, token_resolver=self._token_resolver)
            return self._cached_kamino_adapter_with_rpc
        if self._cached_kamino_adapter is None:
            from almanak.framework.connectors.kamino import KaminoAdapter, KaminoConfig

            config = KaminoConfig(wallet_address=self.wallet_address)
            self._cached_kamino_adapter = KaminoAdapter(config=config, token_resolver=self._token_resolver)
        return self._cached_kamino_adapter

    def _get_raydium_adapter(self, *, needs_rpc: bool = False) -> Any:
        """Get or create a cached RaydiumAdapter instance."""
        if needs_rpc:
            if self._cached_raydium_adapter_with_rpc is None:
                from almanak.framework.connectors.raydium import RaydiumAdapter, RaydiumConfig

                config = RaydiumConfig(wallet_address=self.wallet_address, rpc_url=self.rpc_url or "")
                self._cached_raydium_adapter_with_rpc = RaydiumAdapter(
                    config=config, token_resolver=self._token_resolver
                )
            return self._cached_raydium_adapter_with_rpc
        if self._cached_raydium_adapter is None:
            from almanak.framework.connectors.raydium import RaydiumAdapter, RaydiumConfig

            config = RaydiumConfig(wallet_address=self.wallet_address)
            self._cached_raydium_adapter = RaydiumAdapter(config=config, token_resolver=self._token_resolver)
        return self._cached_raydium_adapter

    def _get_meteora_adapter(self, *, needs_rpc: bool = False) -> Any:
        """Get or create a cached MeteoraAdapter instance."""
        if needs_rpc:
            if self._cached_meteora_adapter_with_rpc is None:
                from almanak.framework.connectors.meteora import MeteoraAdapter, MeteoraConfig

                config = MeteoraConfig(wallet_address=self.wallet_address, rpc_url=self.rpc_url or "")
                self._cached_meteora_adapter_with_rpc = MeteoraAdapter(
                    config=config, token_resolver=self._token_resolver
                )
            return self._cached_meteora_adapter_with_rpc
        if self._cached_meteora_adapter is None:
            from almanak.framework.connectors.meteora import MeteoraAdapter, MeteoraConfig

            config = MeteoraConfig(wallet_address=self.wallet_address)
            self._cached_meteora_adapter = MeteoraAdapter(config=config, token_resolver=self._token_resolver)
        return self._cached_meteora_adapter

    def _get_orca_adapter(self, *, needs_rpc: bool = False) -> Any:
        """Get or create a cached OrcaAdapter instance."""
        if needs_rpc:
            if self._cached_orca_adapter_with_rpc is None:
                from almanak.framework.connectors.orca import OrcaAdapter, OrcaConfig

                config = OrcaConfig(wallet_address=self.wallet_address, rpc_url=self.rpc_url or "")
                self._cached_orca_adapter_with_rpc = OrcaAdapter(config=config, token_resolver=self._token_resolver)
            return self._cached_orca_adapter_with_rpc
        if self._cached_orca_adapter is None:
            from almanak.framework.connectors.orca import OrcaAdapter, OrcaConfig

            config = OrcaConfig(wallet_address=self.wallet_address)
            self._cached_orca_adapter = OrcaAdapter(config=config, token_resolver=self._token_resolver)
        return self._cached_orca_adapter

    def _get_drift_adapter(self) -> Any:
        """Get or create a cached DriftAdapter instance."""
        if self._cached_drift_adapter is None:
            from ..connectors.drift import DriftAdapter, DriftConfig

            config = DriftConfig(wallet_address=self.wallet_address)
            self._cached_drift_adapter = DriftAdapter(config=config, token_resolver=self._token_resolver)
        return self._cached_drift_adapter

    # =========================================================================
    # Solana compilation methods
    # =========================================================================

    def _compile_jupiter_swap(self, intent: SwapIntent) -> CompilationResult:
        """Compile a SWAP intent using Jupiter for Solana chains.

        Args:
            intent: SwapIntent to compile

        Returns:
            CompilationResult with Jupiter ActionBundle
        """
        result = CompilationResult(
            status=CompilationStatus.SUCCESS,
            intent_id=intent.intent_id,
        )
        try:
            adapter = self._get_jupiter_adapter()
            bundle = adapter.compile_swap_intent(intent, price_oracle=self.price_oracle)

            if bundle.metadata.get("error"):
                result.status = CompilationStatus.FAILED
                result.error = bundle.metadata["error"]
            else:
                result.action_bundle = bundle
        except Exception as e:
            logger.exception(f"Jupiter swap compilation failed: {e}")
            result.status = CompilationStatus.FAILED
            result.error = str(e)
        return result

    # ==========================================================================
    # KAMINO LENDING (Solana)
    # ==========================================================================

    def _compile_kamino_supply(self, intent: SupplyIntent) -> CompilationResult:
        """Compile a SUPPLY intent using Kamino for Solana chains."""
        result = CompilationResult(
            status=CompilationStatus.SUCCESS,
            intent_id=intent.intent_id,
        )
        try:
            adapter = self._get_kamino_adapter()
            bundle = adapter.compile_supply_intent(intent)

            if bundle.metadata.get("error"):
                result.status = CompilationStatus.FAILED
                result.error = bundle.metadata["error"]
            else:
                result.action_bundle = bundle
        except Exception as e:
            logger.exception(f"Kamino supply compilation failed: {e}")
            result.status = CompilationStatus.FAILED
            result.error = str(e)
        return result

    def _compile_kamino_borrow(self, intent: BorrowIntent) -> CompilationResult:
        """Compile a BORROW intent using Kamino for Solana chains."""
        result = CompilationResult(
            status=CompilationStatus.SUCCESS,
            intent_id=intent.intent_id,
        )
        try:
            adapter = self._get_kamino_adapter()
            bundle = adapter.compile_borrow_intent(intent)

            if bundle.metadata.get("error"):
                result.status = CompilationStatus.FAILED
                result.error = bundle.metadata["error"]
            else:
                result.action_bundle = bundle
        except Exception as e:
            logger.exception(f"Kamino borrow compilation failed: {e}")
            result.status = CompilationStatus.FAILED
            result.error = str(e)
        return result

    def _compile_kamino_repay(self, intent: RepayIntent) -> CompilationResult:
        """Compile a REPAY intent using Kamino for Solana chains."""
        result = CompilationResult(
            status=CompilationStatus.SUCCESS,
            intent_id=intent.intent_id,
        )
        try:
            adapter = self._get_kamino_adapter()
            bundle = adapter.compile_repay_intent(intent)

            if bundle.metadata.get("error"):
                result.status = CompilationStatus.FAILED
                result.error = bundle.metadata["error"]
            else:
                result.action_bundle = bundle
        except Exception as e:
            logger.exception(f"Kamino repay compilation failed: {e}")
            result.status = CompilationStatus.FAILED
            result.error = str(e)
        return result

    def _compile_kamino_withdraw(self, intent: WithdrawIntent) -> CompilationResult:
        """Compile a WITHDRAW intent using Kamino for Solana chains."""
        result = CompilationResult(
            status=CompilationStatus.SUCCESS,
            intent_id=intent.intent_id,
        )
        try:
            adapter = self._get_kamino_adapter()
            bundle = adapter.compile_withdraw_intent(intent)

            if bundle.metadata.get("error"):
                result.status = CompilationStatus.FAILED
                result.error = bundle.metadata["error"]
            else:
                result.action_bundle = bundle
        except Exception as e:
            logger.exception(f"Kamino withdraw compilation failed: {e}")
            result.status = CompilationStatus.FAILED
            result.error = str(e)
        return result

    # ==========================================================================
    # JUPITER LEND (Solana)
    # ==========================================================================

    def _get_jupiter_lend_adapter(self) -> Any:
        """Get or create a cached JupiterLendAdapter instance."""
        if self._cached_jupiter_lend_adapter is None:
            from almanak.framework.connectors.jupiter_lend import JupiterLendAdapter, JupiterLendConfig

            config = JupiterLendConfig(wallet_address=self.wallet_address)
            self._cached_jupiter_lend_adapter = JupiterLendAdapter(config=config, token_resolver=self._token_resolver)
        return self._cached_jupiter_lend_adapter

    def _compile_jupiter_lend_supply(self, intent: SupplyIntent) -> CompilationResult:
        """Compile a SUPPLY intent using Jupiter Lend for Solana chains."""
        result = CompilationResult(
            status=CompilationStatus.SUCCESS,
            intent_id=intent.intent_id,
        )
        try:
            adapter = self._get_jupiter_lend_adapter()
            bundle = adapter.compile_supply_intent(intent)

            if bundle.metadata.get("error"):
                result.status = CompilationStatus.FAILED
                result.error = bundle.metadata["error"]
            else:
                result.action_bundle = bundle
        except Exception as e:
            logger.exception(f"Jupiter Lend supply compilation failed: {e}")
            result.status = CompilationStatus.FAILED
            result.error = str(e)
        return result

    def _compile_jupiter_lend_borrow(self, intent: BorrowIntent) -> CompilationResult:
        """Compile a BORROW intent using Jupiter Lend for Solana chains."""
        result = CompilationResult(
            status=CompilationStatus.SUCCESS,
            intent_id=intent.intent_id,
        )
        try:
            adapter = self._get_jupiter_lend_adapter()
            bundle = adapter.compile_borrow_intent(intent)

            if bundle.metadata.get("error"):
                result.status = CompilationStatus.FAILED
                result.error = bundle.metadata["error"]
            else:
                result.action_bundle = bundle
        except Exception as e:
            logger.exception(f"Jupiter Lend borrow compilation failed: {e}")
            result.status = CompilationStatus.FAILED
            result.error = str(e)
        return result

    def _compile_jupiter_lend_repay(self, intent: RepayIntent) -> CompilationResult:
        """Compile a REPAY intent using Jupiter Lend for Solana chains."""
        result = CompilationResult(
            status=CompilationStatus.SUCCESS,
            intent_id=intent.intent_id,
        )
        try:
            adapter = self._get_jupiter_lend_adapter()
            bundle = adapter.compile_repay_intent(intent)

            if bundle.metadata.get("error"):
                result.status = CompilationStatus.FAILED
                result.error = bundle.metadata["error"]
            else:
                result.action_bundle = bundle
        except Exception as e:
            logger.exception(f"Jupiter Lend repay compilation failed: {e}")
            result.status = CompilationStatus.FAILED
            result.error = str(e)
        return result

    def _compile_jupiter_lend_withdraw(self, intent: WithdrawIntent) -> CompilationResult:
        """Compile a WITHDRAW intent using Jupiter Lend for Solana chains."""
        result = CompilationResult(
            status=CompilationStatus.SUCCESS,
            intent_id=intent.intent_id,
        )
        try:
            adapter = self._get_jupiter_lend_adapter()
            bundle = adapter.compile_withdraw_intent(intent)

            if bundle.metadata.get("error"):
                result.status = CompilationStatus.FAILED
                result.error = bundle.metadata["error"]
            else:
                result.action_bundle = bundle
        except Exception as e:
            logger.exception(f"Jupiter Lend withdraw compilation failed: {e}")
            result.status = CompilationStatus.FAILED
            result.error = str(e)
        return result

    def _compile_raydium_lp_open(self, intent: LPOpenIntent) -> CompilationResult:
        """Compile an LP_OPEN intent using Raydium CLMM for Solana chains."""
        result = CompilationResult(
            status=CompilationStatus.SUCCESS,
            intent_id=intent.intent_id,
        )
        try:
            adapter = self._get_raydium_adapter()
            bundle = adapter.compile_lp_open_intent(intent)

            if bundle.metadata.get("error"):
                result.status = CompilationStatus.FAILED
                result.error = bundle.metadata["error"]
            else:
                result.action_bundle = bundle
        except Exception as e:
            logger.exception(f"Raydium LP open compilation failed: {e}")
            result.status = CompilationStatus.FAILED
            result.error = str(e)
        return result

    def _compile_raydium_lp_close(self, intent: LPCloseIntent) -> CompilationResult:
        """Compile an LP_CLOSE intent using Raydium CLMM for Solana chains."""
        result = CompilationResult(
            status=CompilationStatus.SUCCESS,
            intent_id=intent.intent_id,
        )
        try:
            adapter = self._get_raydium_adapter(needs_rpc=True)
            bundle = adapter.compile_lp_close_intent(intent)

            if bundle.metadata.get("error"):
                result.status = CompilationStatus.FAILED
                result.error = bundle.metadata["error"]
            else:
                result.action_bundle = bundle
        except Exception as e:
            logger.exception(f"Raydium LP close compilation failed: {e}")
            result.status = CompilationStatus.FAILED
            result.error = str(e)
        return result

    def _compile_meteora_lp_open(self, intent: LPOpenIntent) -> CompilationResult:
        """Compile an LP_OPEN intent using Meteora DLMM for Solana chains."""
        result = CompilationResult(
            status=CompilationStatus.SUCCESS,
            intent_id=intent.intent_id,
        )
        try:
            adapter = self._get_meteora_adapter()
            bundle = adapter.compile_lp_open_intent(intent)

            if bundle.metadata.get("error"):
                result.status = CompilationStatus.FAILED
                result.error = bundle.metadata["error"]
            else:
                result.action_bundle = bundle
        except Exception as e:
            logger.exception(f"Meteora LP open compilation failed: {e}")
            result.status = CompilationStatus.FAILED
            result.error = str(e)
        return result

    def _compile_meteora_lp_close(self, intent: LPCloseIntent) -> CompilationResult:
        """Compile an LP_CLOSE intent using Meteora DLMM for Solana chains."""
        result = CompilationResult(
            status=CompilationStatus.SUCCESS,
            intent_id=intent.intent_id,
        )
        try:
            adapter = self._get_meteora_adapter(needs_rpc=True)
            bundle = adapter.compile_lp_close_intent(intent)

            if bundle.metadata.get("error"):
                result.status = CompilationStatus.FAILED
                result.error = bundle.metadata["error"]
            else:
                result.action_bundle = bundle
        except Exception as e:
            logger.exception(f"Meteora LP close compilation failed: {e}")
            result.status = CompilationStatus.FAILED
            result.error = str(e)
        return result

    def _compile_orca_lp_open(self, intent: LPOpenIntent) -> CompilationResult:
        """Compile an LP_OPEN intent using Orca Whirlpools for Solana chains."""
        result = CompilationResult(
            status=CompilationStatus.SUCCESS,
            intent_id=intent.intent_id,
        )
        try:
            adapter = self._get_orca_adapter()
            bundle = adapter.compile_lp_open_intent(intent)

            if bundle.metadata.get("error"):
                result.status = CompilationStatus.FAILED
                result.error = bundle.metadata["error"]
            else:
                result.action_bundle = bundle
        except Exception as e:
            logger.exception(f"Orca LP open compilation failed: {e}")
            result.status = CompilationStatus.FAILED
            result.error = str(e)
        return result

    def _compile_orca_lp_close(self, intent: LPCloseIntent) -> CompilationResult:
        """Compile an LP_CLOSE intent using Orca Whirlpools for Solana chains."""
        result = CompilationResult(
            status=CompilationStatus.SUCCESS,
            intent_id=intent.intent_id,
        )
        try:
            adapter = self._get_orca_adapter(needs_rpc=True)
            bundle = adapter.compile_lp_close_intent(intent)

            if bundle.metadata.get("error"):
                result.status = CompilationStatus.FAILED
                result.error = bundle.metadata["error"]
            else:
                result.action_bundle = bundle
        except Exception as e:
            logger.exception(f"Orca LP close compilation failed: {e}")
            result.status = CompilationStatus.FAILED
            result.error = str(e)
        return result

    def _compile_wrap_native(self, intent: "WrapNativeIntent") -> CompilationResult:
        """Compile a WRAP_NATIVE intent into an ActionBundle.

        Generates a single transaction calling the wrapped native token's
        ``deposit()`` function with ``msg.value`` to convert native currency
        (ETH, MATIC, AVAX, etc.) to its wrapped ERC-20 equivalent.
        """

        result = CompilationResult(
            status=CompilationStatus.SUCCESS,
            intent_id=intent.intent_id,
        )

        try:
            token_symbol = intent.token

            # Resolve the wrapped native token address
            weth_address = self._get_wrapped_native_address()
            if not weth_address:
                result.status = CompilationStatus.FAILED
                result.error = f"No wrapped native token found for chain {self.chain}"
                return result

            # Resolve token to verify it matches the chain's wrapped native
            resolved = self._resolve_token(token_symbol)
            if not resolved:
                result.status = CompilationStatus.FAILED
                result.error = f"Cannot resolve token {token_symbol} on {self.chain}"
                return result

            if resolved.address.lower() != weth_address.lower():
                result.status = CompilationStatus.FAILED
                result.error = (
                    f"{token_symbol} ({resolved.address}) is not the wrapped native token "
                    f"({weth_address}) on {self.chain}"
                )
                return result

            # Resolve amount
            amount = intent.amount
            decimals = resolved.decimals
            gas_reserve = int(Decimal("0.001") * Decimal(10**decimals))
            if isinstance(amount, str) and amount == "all":
                # Query native balance
                balance = self._query_native_balance(self.wallet_address)
                if balance is None or balance <= 0:
                    result.status = CompilationStatus.FAILED
                    result.error = f"No native balance to wrap on {self.chain}"
                    return result
                # Reserve gas (0.001 native token ~= minimal gas buffer)
                amount_raw = max(balance - gas_reserve, 0)
                if amount_raw <= 0:
                    result.status = CompilationStatus.FAILED
                    result.error = "Native balance too low to wrap after reserving gas"
                    return result
            else:
                amount_raw = int(Decimal(str(amount)) * Decimal(10**decimals))

            if amount_raw <= 0:
                result.status = CompilationStatus.FAILED
                result.error = "Wrap amount must be positive"
                return result

            # Pre-flight balance check (must cover wrap amount + gas for the tx)
            if not (isinstance(amount, str) and amount == "all"):
                balance = self._query_native_balance(self.wallet_address)
                if balance is not None and balance < amount_raw + gas_reserve:
                    have = Decimal(balance) / Decimal(10**decimals)
                    need = Decimal(str(intent.amount))
                    result.status = CompilationStatus.FAILED
                    result.error = f"Insufficient native balance: have {have}, need {need} + gas reserve"
                    return result

            # Build deposit() calldata
            # Function selector: 0xd0e30db0 = keccak256("deposit()")[:4]
            calldata = "0xd0e30db0"

            wrap_tx = TransactionData(
                to=weth_address,
                value=amount_raw,
                data=calldata,
                gas_estimate=get_gas_estimate(self.chain, "unwrap_eth"),  # similar gas cost
                description=f"Wrap {intent.amount} native to {token_symbol}",
                tx_type="wrap",
            )

            transactions = [wrap_tx]

            action_bundle = ActionBundle(
                intent_type=IntentType.WRAP_NATIVE.value,
                transactions=[tx.to_dict() for tx in transactions],
                metadata={
                    "token": token_symbol,
                    "amount": str(intent.amount),
                    "chain": self.chain,
                },
            )

            result.action_bundle = action_bundle
            result.transactions = transactions
            result.total_gas_estimate = wrap_tx.gas_estimate

            logger.info(
                f"Compiled WRAP_NATIVE intent: {intent.amount} native -> {token_symbol} on {self.chain}, "
                f"1 tx, gas={wrap_tx.gas_estimate}"
            )
        except Exception as e:
            logger.exception("Failed to compile WRAP_NATIVE intent")
            result.status = CompilationStatus.FAILED
            result.error = str(e)
        return result

    def _compile_unwrap_native(self, intent: "UnwrapNativeIntent") -> CompilationResult:
        """Compile an UNWRAP_NATIVE intent into an ActionBundle.

        Generates a single ``WETH.withdraw(uint256)`` transaction to convert
        wrapped native tokens (WETH, WMATIC, WAVAX, etc.) back to the chain's
        native currency.
        """

        result = CompilationResult(
            status=CompilationStatus.SUCCESS,
            intent_id=intent.intent_id,
        )

        try:
            token_symbol = intent.token

            # Resolve the wrapped native token address
            weth_address = self._get_wrapped_native_address()
            if not weth_address:
                result.status = CompilationStatus.FAILED
                result.error = f"No wrapped native token found for chain {self.chain}"
                return result

            # Resolve token to verify it matches the chain's wrapped native
            resolved = self._resolve_token(token_symbol)
            if not resolved:
                result.status = CompilationStatus.FAILED
                result.error = f"Cannot resolve token {token_symbol} on {self.chain}"
                return result

            if resolved.address.lower() != weth_address.lower():
                result.status = CompilationStatus.FAILED
                result.error = (
                    f"{token_symbol} ({resolved.address}) is not the wrapped native token "
                    f"({weth_address}) on {self.chain}"
                )
                return result

            # Resolve amount
            amount = intent.amount
            if isinstance(amount, str) and amount == "all":
                # Query balance of wrapped native token
                balance = self._query_erc20_balance(weth_address, self.wallet_address)
                if balance is None or balance <= 0:
                    result.status = CompilationStatus.FAILED
                    result.error = f"No {token_symbol} balance to unwrap"
                    return result
                amount_raw = balance
            else:
                decimals = resolved.decimals
                amount_raw = int(Decimal(str(amount)) * Decimal(10**decimals))

            if amount_raw <= 0:
                result.status = CompilationStatus.FAILED
                result.error = "Unwrap amount must be positive"
                return result

            # Pre-flight balance check: catch insufficient balance before on-chain revert
            if not (isinstance(amount, str) and amount == "all"):
                balance = self._query_erc20_balance(weth_address, self.wallet_address)
                if balance is not None and balance < amount_raw:
                    decimals = resolved.decimals
                    have = Decimal(balance) / Decimal(10**decimals)
                    need = Decimal(str(intent.amount))
                    result.status = CompilationStatus.FAILED
                    result.error = (
                        f"Insufficient {token_symbol} balance: have {have} {token_symbol}, need {need} {token_symbol}"
                    )
                    return result

            # Build withdraw(uint256) calldata
            # Function selector: 0x2e1a7d4d = keccak256("withdraw(uint256)")[:4]
            amount_hex = hex(amount_raw)[2:].zfill(64)
            calldata = f"0x2e1a7d4d{amount_hex}"

            unwrap_tx = TransactionData(
                to=weth_address,
                value=0,
                data=calldata,
                gas_estimate=get_gas_estimate(self.chain, "unwrap_eth"),
                description=f"Unwrap {intent.amount} {token_symbol} to native",
                tx_type="unwrap",
            )

            transactions = [unwrap_tx]

            action_bundle = ActionBundle(
                intent_type=IntentType.UNWRAP_NATIVE.value,
                transactions=[tx.to_dict() for tx in transactions],
                metadata={
                    "token": token_symbol,
                    "amount": str(intent.amount),
                    "chain": self.chain,
                },
            )

            result.action_bundle = action_bundle
            result.transactions = transactions
            result.total_gas_estimate = unwrap_tx.gas_estimate

            logger.info(
                f"Compiled UNWRAP_NATIVE intent: {intent.amount} {token_symbol} on {self.chain}, "
                f"1 tx, gas={unwrap_tx.gas_estimate}"
            )
        except Exception as e:
            logger.exception("Failed to compile UNWRAP_NATIVE intent")
            result.status = CompilationStatus.FAILED
            result.error = str(e)
        return result

    def _compile_swap(self, intent: SwapIntent) -> CompilationResult:
        """Compile a SWAP intent into an ActionBundle.

        This method:
        1. Resolves token addresses
        2. Calculates amounts (USD to token if needed)
        3. Calculates minimum output with slippage
        4. Builds approve TX if needed
        5. Builds swap TX

        For cross-chain swaps (when destination_chain is set), uses Enso
        for routing which handles the bridging automatically.

        For Solana chains, routes to Jupiter aggregator.

        Args:
            intent: SwapIntent to compile

        Returns:
            CompilationResult with swap ActionBundle
        """
        # Route to Jupiter for Solana chains
        if self._is_solana_chain():
            protocol = intent.protocol
            allowed_solana_swap = {None, "jupiter"}
            if protocol and protocol.lower() not in allowed_solana_swap:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    intent_id=intent.intent_id,
                    error=f"Protocol '{protocol}' is not supported for SWAP on Solana. Supported: jupiter",
                )
            return self._compile_jupiter_swap(intent)

        # Check for cross-chain swap - route to appropriate aggregator
        # Preserve historical behavior: protocol=None defaults to Enso for cross-chain swaps.
        if intent.is_cross_chain:
            if intent.protocol is not None:
                from ..connectors.protocol_aliases import normalize_protocol

                protocol = normalize_protocol(self.chain, intent.protocol)
                if protocol == "lifi":
                    return self._compile_lifi_swap(intent)
            return self._compile_cross_chain_swap(intent)

        # Check for aggregator protocols
        protocol = self._resolve_protocol(intent.protocol)
        if protocol == "enso":
            return self._compile_enso_swap(intent)
        if protocol == "lifi":
            return self._compile_lifi_swap(intent)

        # Handle Aerodrome/Velodrome separately (Solidly-fork with different swap interface)
        # protocol is already resolved via _resolve_protocol() above (velodrome -> aerodrome on Optimism)
        if protocol == "aerodrome":
            return self._compile_swap_aerodrome(intent)

        # Handle Pendle separately (yield tokenization protocol with PT/YT tokens)
        if protocol == "pendle":
            return self._compile_pendle_swap(intent)

        # Handle Curve separately (pool-based AMM with direct pool addressing)
        if protocol == "curve":
            return self._compile_swap_curve(intent)

        # Handle Uniswap V4 separately (PoolManager-based singleton with different interface)
        if protocol == "uniswap_v4":
            return self._compile_swap_uniswap_v4(intent)

        # Handle Fluid DEX swaps (direct pool swapIn call)
        if protocol == "fluid":
            return self._compile_swap_fluid(intent)

        # Guard: TraderJoe V2 uses LBRouter2 (bin-based AMM), NOT Uniswap V3 interface.
        # DefaultSwapAdapter generates exactInputSingle calldata which reverts on LBRouter2.
        # LP operations still work via the dedicated TraderJoe V2 connector. (VIB-1406)
        if protocol == "traderjoe_v2":
            return CompilationResult(
                status=CompilationStatus.FAILED,
                intent_id=intent.intent_id,
                error=(
                    "TraderJoe V2 swap is not yet supported (VIB-1406): LBRouter2 uses a "
                    "bin-based AMM interface incompatible with the default swap adapter. "
                    "Use protocol='uniswap_v3' or protocol='enso' for swaps on Avalanche/Arbitrum. "
                    "TraderJoe V2 LP operations (LPOpenIntent/LPCloseIntent) still work."
                ),
            )

        result = CompilationResult(
            status=CompilationStatus.SUCCESS,
            intent_id=intent.intent_id,
        )
        transactions: list[TransactionData] = []
        warnings: list[str] = []

        try:
            # Step 1: Resolve token addresses
            from_token = self._resolve_token(intent.from_token)
            to_token = self._resolve_token(intent.to_token)

            if from_token is None:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"Unknown token: {intent.from_token}",
                    intent_id=intent.intent_id,
                )
            if to_token is None:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"Unknown token: {intent.to_token}",
                    intent_id=intent.intent_id,
                )

            # Step 2: Calculate input amount
            if intent.amount_usd is not None:
                amount_in = self._usd_to_token_amount(
                    intent.amount_usd,
                    from_token,
                )
            elif intent.amount is not None:
                # Check for chained amount - must be resolved before compilation
                if intent.amount == "all":
                    return CompilationResult(
                        status=CompilationStatus.FAILED,
                        error="amount='all' must be resolved before compilation. Use Intent.set_resolved_amount() to resolve chained amounts.",
                        intent_id=intent.intent_id,
                    )
                # Type is validated above to be Decimal (not "all")
                amount_decimal: Decimal = intent.amount  # type: ignore[assignment]
                amount_in = int(amount_decimal * Decimal(10**from_token.decimals))
            else:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error="Either amount_usd or amount must be provided",
                    intent_id=intent.intent_id,
                )

            # Step 3: Calculate minimum output with slippage
            try:
                expected_output = self._calculate_expected_output(amount_in, from_token, to_token)
            except ValueError as e:
                # Price unavailable -- fail-closed to prevent swaps with zero slippage protection.
                # Without a price, min_output would be 0 and the swap would be vulnerable to
                # sandwich attacks / MEV extraction.
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=(
                        f"Cannot calculate slippage protection for {from_token.symbol} -> {to_token.symbol}: {e}. "
                        f"The price oracle does not have a price for one of the tokens. "
                        f"Ensure the token price is available via market.price() before swapping."
                    ),
                    intent_id=intent.intent_id,
                )

            # Step 4: Get protocol adapter
            protocol = self._resolve_protocol(intent.protocol)
            adapter = DefaultSwapAdapter(
                self.chain,
                protocol,
                pool_selection_mode=self._config.swap_pool_selection_mode,
                fixed_fee_tier=self._config.fixed_swap_fee_tier,
                rpc_url=self._get_chain_rpc_url(),
                rpc_timeout=self.rpc_timeout,
            )
            router_address = adapter.get_router_address()

            if router_address == "0x0000000000000000000000000000000000000000":
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"Unknown router for protocol {protocol} on {self.chain}.",
                    intent_id=intent.intent_id,
                )

            # Step 5: Build approve TX if needed (skip for native token)
            if not from_token.is_native:
                approve_txs = self._build_approve_tx(
                    from_token.address,
                    router_address,
                    amount_in,
                )
                transactions.extend(approve_txs)

            # Step 6: Build swap TX
            deadline = int(datetime.now(UTC).timestamp()) + self.default_deadline_seconds

            # Handle native token wrapping if needed
            value = 0
            actual_from_token = from_token.address
            if from_token.is_native:
                # Swapping from native - send ETH value
                value = amount_in
                # Use WETH for the swap
                weth_address = self._get_wrapped_native_address() or from_token.address
                actual_from_token = weth_address
                warnings.append("Native token swap: will wrap to WETH before swapping")

            actual_to_token = to_token.address
            if to_token.is_native:
                # Swapping to native - receive WETH, then unwrap
                weth_address = self._get_wrapped_native_address() or to_token.address
                actual_to_token = weth_address
                warnings.append("Native token output: will receive WETH, unwrap separately")

            # Pre-select fee tier to make quoter data available for slippage adjustment.
            # This also parallelizes fee tier queries for faster compilation.
            # Wrapped in try/except so that RPC failures in the quoter path degrade
            # gracefully to the oracle-only slippage estimate instead of crashing compilation.
            try:
                adapter.select_fee_tier(actual_from_token, actual_to_token, amount_in)
            except Exception as exc:
                logger.warning("Fee tier pre-selection failed, falling back to oracle estimate: %s", exc)

            # Tighten slippage using quoter data when available.
            # The on-chain quoter reflects actual pool liquidity and is more accurate
            # than the price oracle estimate. Use the lower of the two to protect against
            # both quoter overestimates and stale oracle prices.
            oracle_estimate = expected_output
            quoter_amount = adapter.get_quoted_amount_out()
            if quoter_amount is not None and quoter_amount < expected_output:
                logger.info(
                    "Quoter amount (%s) is lower than price oracle estimate (%s) — "
                    "using quoter amount as slippage basis for safer execution",
                    quoter_amount,
                    expected_output,
                )
                expected_output = quoter_amount

            # Price impact guard: fail compilation if quoter deviates too far from oracle.
            # This catches zero/low-liquidity pools where slippage protection is meaningless
            # because the quoter amount itself is catastrophically bad.
            # Skip when using placeholder prices — oracle estimates are unreliable in that mode.
            if quoter_amount is not None and oracle_estimate > 0 and not self._using_placeholders:
                price_impact = Decimal(1) - (Decimal(quoter_amount) / Decimal(oracle_estimate))
                max_impact = (
                    intent.max_price_impact
                    if intent.max_price_impact is not None
                    else self._config.max_price_impact_pct
                )
                if price_impact > max_impact:
                    return CompilationResult(
                        status=CompilationStatus.FAILED,
                        error=(
                            f"Price impact too high: quoter returned amount implying "
                            f"{price_impact:.1%} price impact "
                            f"(oracle estimate: {oracle_estimate}, quoter: {quoter_amount}). "
                            f"Maximum allowed: {max_impact:.0%}. "
                            f"Likely cause: pool has insufficient liquidity for {intent.from_token}->{intent.to_token}."
                        ),
                    )
            elif quoter_amount is None and oracle_estimate > 0 and not self._using_placeholders:
                logger.warning(
                    "Price impact guard skipped: quoter returned None (RPC may be unavailable). "
                    "Proceeding with oracle-only estimate for %s->%s.",
                    intent.from_token,
                    intent.to_token,
                )

            min_output = int(Decimal(str(expected_output)) * (Decimal("1") - intent.max_slippage))

            # Generate swap calldata (uses cached fee tier from select_fee_tier above)
            swap_calldata = adapter.get_swap_calldata(
                from_token=actual_from_token,
                to_token=actual_to_token,
                amount_in=amount_in,
                min_amount_out=min_output,
                recipient=self.wallet_address,
                deadline=deadline,
            )

            # Validate pool existence (best-effort, after fee tier is selected)
            selected_fee = adapter.last_fee_selection.get("selected_fee_tier")
            if selected_fee is not None:
                from .pool_validation import validate_v3_pool

                pool_check = validate_v3_pool(
                    self.chain, protocol, actual_from_token, actual_to_token, selected_fee, self._get_chain_rpc_url()
                )
                failed = self._validate_pool(pool_check, intent.intent_id)
                if failed is not None:
                    return failed

            # Estimate gas
            swap_gas = adapter.estimate_gas(actual_from_token, actual_to_token)

            swap_tx = TransactionData(
                to=router_address,
                value=value,
                data="0x" + swap_calldata.hex(),
                gas_estimate=swap_gas,
                description=(
                    f"Swap {self._format_amount(amount_in, from_token.decimals)} {from_token.symbol} -> {to_token.symbol} (min: {self._format_amount(min_output, to_token.decimals)})"
                ),
                tx_type="swap",
            )
            transactions.append(swap_tx)

            # Step 7: Build ActionBundle
            total_gas = sum(tx.gas_estimate for tx in transactions)

            action_bundle = ActionBundle(
                intent_type=IntentType.SWAP.value,
                transactions=[tx.to_dict() for tx in transactions],
                metadata={
                    "from_token": from_token.to_dict(),
                    "to_token": to_token.to_dict(),
                    "amount_in": str(amount_in),
                    "min_amount_out": str(min_output),
                    "slippage": str(intent.max_slippage),
                    "protocol": protocol,
                    "router": router_address,
                    "pool_selection_mode": self._config.swap_pool_selection_mode,
                    "selected_fee_tier": adapter.last_fee_selection.get("selected_fee_tier"),
                    "fee_tier_candidates": adapter.last_fee_selection.get("candidate_fee_tiers"),
                    "fee_selection_source": adapter.last_fee_selection.get("source"),
                    "deadline": deadline,
                    "chain": self.chain,
                },
            )

            result.action_bundle = action_bundle
            result.transactions = transactions
            result.total_gas_estimate = total_gas
            result.warnings = warnings

            # Format amounts for user-friendly logging
            amount_in_fmt = format_token_amount(amount_in, from_token.symbol, from_token.decimals)
            expected_out_fmt = format_token_amount(expected_output, to_token.symbol, to_token.decimals)
            min_out_fmt = format_token_amount(min_output, to_token.symbol, to_token.decimals)
            slippage_fmt = format_percentage(intent.max_slippage)

            ok = "✅" if _emojis_enabled() else "[OK]"
            logger.info(f"{ok} Compiled SWAP: {amount_in_fmt} → {expected_out_fmt} (min: {min_out_fmt})")
            logger.info(f"   Slippage: {slippage_fmt} | Txs: {len(transactions)} | Gas: {total_gas:,}")

        except Exception as e:
            logger.exception(f"Failed to compile SWAP intent: {e}")
            result.status = CompilationStatus.FAILED
            result.error = str(e)

        return result

    def _compile_enso_swap(self, intent: SwapIntent) -> CompilationResult:
        """Compile a same-chain SWAP intent using Enso DEX aggregator.

        Enso provides DEX aggregation which may find better prices by routing
        through multiple DEXes. This method:
        1. Resolves token addresses
        2. Gets optimal route from Enso API (via gateway gRPC or direct client)
        3. Builds approve TX if needed
        4. Returns the transaction from Enso

        When a gateway_client is available, the route is fetched via the gateway's
        EnsoService gRPC, keeping the API key in the gateway process. Falls back to
        the direct EnsoClient only when no gateway is connected (local dev).

        Args:
            intent: SwapIntent with protocol="enso"

        Returns:
            CompilationResult with Enso swap ActionBundle
        """
        result = CompilationResult(
            status=CompilationStatus.SUCCESS,
            intent_id=intent.intent_id,
        )
        transactions: list[TransactionData] = []
        warnings: list[str] = []

        try:
            # Step 1: Resolve token addresses
            from_token = self._resolve_token(intent.from_token)
            to_token = self._resolve_token(intent.to_token)

            if from_token is None:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"Unknown token: {intent.from_token}",
                    intent_id=intent.intent_id,
                )
            if to_token is None:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"Unknown token: {intent.to_token}",
                    intent_id=intent.intent_id,
                )

            # Step 2: Calculate input amount
            if intent.amount_usd is not None:
                amount_in = self._usd_to_token_amount(intent.amount_usd, from_token)
            elif intent.amount is not None:
                if intent.amount == "all":
                    return CompilationResult(
                        status=CompilationStatus.FAILED,
                        error="amount='all' must be resolved before compilation.",
                        intent_id=intent.intent_id,
                    )
                amount_decimal: Decimal = intent.amount  # type: ignore[assignment]
                amount_in = int(amount_decimal * Decimal(10**from_token.decimals))
            else:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error="Either amount_usd or amount must be provided",
                    intent_id=intent.intent_id,
                )

            # Step 3: Get route from Enso (via gateway gRPC or direct client)
            logger.info(f"Getting Enso route: {from_token.symbol} -> {to_token.symbol}, amount={amount_in}")

            slippage_bps = int(intent.max_slippage * 10000)
            route_data = self._get_enso_route(from_token.address, to_token.address, str(amount_in), slippage_bps)

            # Step 4: Build approve TX if needed (skip for native token)
            router_address = route_data["to"]
            if not from_token.is_native:
                approve_txs = self._build_approve_tx(
                    from_token.address,
                    router_address,
                    amount_in,
                )
                transactions.extend(approve_txs)

            # Step 5: Build swap TX from Enso route
            value = int(route_data["value"]) if route_data["value"] else 0
            swap_tx = TransactionData(
                to=route_data["to"],
                value=value,
                data=route_data["data"],
                gas_estimate=route_data["gas"] if route_data["gas"] else 200000,
                description=(
                    f"Swap via Enso: {self._format_amount(amount_in, from_token.decimals)} {from_token.symbol} -> {to_token.symbol}"
                ),
                tx_type="swap",
            )
            transactions.append(swap_tx)

            # Step 6: Build ActionBundle
            total_gas = sum(tx.gas_estimate for tx in transactions)
            amount_out = int(route_data["amount_out"]) if route_data["amount_out"] else 0

            # Calculate minimum output with slippage
            min_output = int(Decimal(str(amount_out)) * (Decimal("1") - intent.max_slippage))

            action_bundle = ActionBundle(
                intent_type=IntentType.SWAP.value,
                transactions=[tx.to_dict() for tx in transactions],
                metadata={
                    "from_token": from_token.to_dict(),
                    "to_token": to_token.to_dict(),
                    "amount_in": str(amount_in),
                    "amount_out": str(amount_out),
                    "min_amount_out": str(min_output),
                    "slippage": str(intent.max_slippage),
                    "protocol": "enso",
                    "router": router_address,
                    "price_impact_bps": route_data.get("price_impact", 0),
                },
            )

            result.action_bundle = action_bundle
            result.transactions = transactions
            result.total_gas_estimate = total_gas
            result.warnings = warnings

            # Format amounts for user-friendly logging
            amount_in_fmt = format_token_amount(amount_in, from_token.symbol, from_token.decimals)
            amount_out_fmt = format_token_amount(amount_out, to_token.symbol, to_token.decimals)
            min_out_fmt = format_token_amount(min_output, to_token.symbol, to_token.decimals)
            slippage_fmt = format_percentage(intent.max_slippage)
            price_impact_val = route_data.get("price_impact")
            price_impact_fmt = format_slippage_bps(price_impact_val) if price_impact_val is not None else "N/A"

            ok = "✅" if _emojis_enabled() else "[OK]"
            logger.info(f"{ok} Compiled SWAP (Enso): {amount_in_fmt} → {amount_out_fmt} (min: {min_out_fmt})")
            logger.info(
                f"   Slippage: {slippage_fmt} | Impact: {price_impact_fmt} | Txs: {len(transactions)} | Gas: {total_gas:,}"
            )

        except Exception as e:
            logger.exception(f"Failed to compile Enso SWAP intent: {e}")
            result.status = CompilationStatus.FAILED
            result.error = str(e)

        return result

    def _get_enso_route(
        self,
        token_in: str,
        token_out: str,
        amount_in: str,
        slippage_bps: int,
        *,
        chain: str | None = None,
        destination_chain_id: int | None = None,
        receiver: str | None = None,
        refund_receiver: str | None = None,
    ) -> dict[str, Any]:
        """Get Enso route via gateway gRPC or direct client.

        When a gateway_client is connected, routes through the gateway's
        EnsoService gRPC (API key stays in the gateway). Falls back to
        the direct EnsoClient for local development without a gateway.

        In deployed/managed mode (AGENT_ID set), the gateway is mandatory
        and no fallback to direct HTTP is attempted.

        Args:
            token_in: Input token address.
            token_out: Output token address.
            amount_in: Input amount in wei (as string).
            slippage_bps: Slippage tolerance in basis points.
            chain: Source chain override (defaults to self.chain).
            destination_chain_id: Target chain ID for cross-chain routes.
            receiver: Receiver address for cross-chain routes.
            refund_receiver: Refund receiver for cross-chain routes.

        Returns:
            Dict with keys: to, data, value, gas (int|None), amount_out, price_impact,
            and optionally bridge_fee, estimated_time, is_cross_chain for cross-chain routes.
        """
        if self._gateway_client is not None:
            if not self._gateway_client.is_connected:
                raise RuntimeError(
                    "Gateway client is configured but not connected; cannot fetch Enso route. "
                    "Ensure the gateway is running before compiling Enso intents."
                )
            return self._get_enso_route_via_gateway(
                token_in,
                token_out,
                amount_in,
                slippage_bps,
                chain=chain,
                destination_chain_id=destination_chain_id,
                receiver=receiver,
                refund_receiver=refund_receiver,
            )

        # No gateway client configured — only allowed in local dev.
        # In managed deployments the deployer always injects a gateway client;
        # this guard catches misconfiguration.
        if os.environ.get("AGENT_ID"):
            raise RuntimeError(
                "Enso route request failed: no gateway client configured. "
                "In deployed mode, all Enso API calls must go through the gateway."
            )

        return self._get_enso_route_direct(
            token_in,
            token_out,
            int(amount_in),
            slippage_bps,
            chain=chain,
            destination_chain_id=destination_chain_id,
            receiver=receiver,
            refund_receiver=refund_receiver,
        )

    def _get_enso_route_via_gateway(
        self,
        token_in: str,
        token_out: str,
        amount_in: str,
        slippage_bps: int,
        *,
        chain: str | None = None,
        destination_chain_id: int | None = None,
        receiver: str | None = None,
        refund_receiver: str | None = None,
    ) -> dict[str, Any]:
        """Get Enso route via gateway's EnsoService gRPC."""
        from almanak.gateway.proto import gateway_pb2

        request = gateway_pb2.EnsoRouteRequest(
            chain=chain or self.chain,
            token_in=token_in,
            token_out=token_out,
            amount_in=amount_in,
            from_address=self.wallet_address,
            slippage_bps=slippage_bps,
            destination_chain_id=destination_chain_id or 0,
            receiver=receiver or "",
            refund_receiver=refund_receiver or "",
        )
        response = self._gateway_client.enso.GetRoute(request, timeout=30.0)  # type: ignore[union-attr]

        if not response.success:
            raise RuntimeError(f"Gateway Enso GetRoute failed: {response.error}")

        gas_str = response.gas or response.gas_estimate
        result = {
            "to": response.to,
            "data": response.data,
            "value": response.value,
            "gas": int(gas_str) if gas_str and gas_str != "0" else None,
            "amount_out": response.amount_out,
            "price_impact": response.price_impact,
        }

        if response.is_cross_chain:
            result["bridge_fee"] = response.bridge_fee
            result["estimated_time"] = response.estimated_time
            result["is_cross_chain"] = True

        return result

    def _get_enso_route_direct(
        self,
        token_in: str,
        token_out: str,
        amount_in: int,
        slippage_bps: int,
        *,
        chain: str | None = None,
        destination_chain_id: int | None = None,
        receiver: str | None = None,
        refund_receiver: str | None = None,
    ) -> dict[str, Any]:
        """Get Enso route via direct HTTP client (local dev fallback)."""
        from ..connectors.enso import EnsoClient, EnsoConfig

        config = EnsoConfig(
            chain=chain or self.chain,
            wallet_address=self.wallet_address,
        )
        client = EnsoClient(config)
        route = client.get_route(
            token_in=token_in,
            token_out=token_out,
            amount_in=amount_in,
            slippage_bps=slippage_bps,
            destination_chain_id=destination_chain_id,
            refund_receiver=refund_receiver,
        )

        result: dict[str, Any] = {
            "to": route.tx.to,
            "data": route.tx.data,
            "value": str(route.tx.value) if route.tx.value else "0",
            "gas": int(route.gas) if route.gas else None,
            "amount_out": str(route.get_amount_out_wei()),
            "price_impact": route.price_impact,
        }

        if destination_chain_id:
            result["bridge_fee"] = getattr(route, "bridge_fee", None)
            result["estimated_time"] = getattr(route, "estimated_time", None)
            result["is_cross_chain"] = True

        return result

    def _compile_lifi_swap(self, intent: SwapIntent) -> CompilationResult:
        """Compile a SWAP intent using LiFi aggregator.

        LiFi is a cross-chain liquidity meta-aggregator that routes through
        bridges (Across, Stargate, Hop, etc.) and DEXs (1inch, 0x, etc.).
        Supports both same-chain swaps and cross-chain bridge+swap operations.

        This method:
        1. Resolves token addresses for source (and destination) chains
        2. Gets quote from LiFi API with transaction data
        3. Builds approve TX if needed (standard ERC-20, no Permit2)
        4. Returns ActionBundle with deferred swap markers

        Args:
            intent: SwapIntent with protocol="lifi"

        Returns:
            CompilationResult with LiFi swap ActionBundle
        """
        from ..connectors.lifi import CHAIN_MAPPING, LiFiAdapter, LiFiConfig

        result = CompilationResult(
            status=CompilationStatus.SUCCESS,
            intent_id=intent.intent_id,
        )
        transactions: list[TransactionData] = []
        warnings: list[str] = []

        try:
            # Step 1: Determine source and destination chains
            source_chain = intent.chain or self.chain
            dest_chain = intent.destination_chain or source_chain
            is_cross_chain = source_chain != dest_chain

            # Resolve chain IDs
            source_chain_lower = source_chain.lower()
            dest_chain_lower = dest_chain.lower()

            if source_chain_lower not in CHAIN_MAPPING:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"LiFi does not support chain: {source_chain}. Supported: {', '.join(CHAIN_MAPPING.keys())}",
                    intent_id=intent.intent_id,
                )
            if dest_chain_lower not in CHAIN_MAPPING:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"LiFi does not support chain: {dest_chain}. Supported: {', '.join(CHAIN_MAPPING.keys())}",
                    intent_id=intent.intent_id,
                )

            from_chain_id = CHAIN_MAPPING[source_chain_lower]
            to_chain_id = CHAIN_MAPPING[dest_chain_lower]

            # Step 2: Resolve token addresses
            from_token = self._resolve_token(intent.from_token, chain=source_chain)
            if from_token is None:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"Unknown token on {source_chain}: {intent.from_token}",
                    intent_id=intent.intent_id,
                )

            to_token = self._resolve_token(intent.to_token, chain=dest_chain)
            if to_token is None:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"Unknown token on {dest_chain}: {intent.to_token}",
                    intent_id=intent.intent_id,
                )

            # Step 3: Calculate input amount
            if intent.amount_usd is not None:
                amount_in = self._usd_to_token_amount(intent.amount_usd, from_token)
            elif intent.amount is not None:
                if intent.amount == "all":
                    return CompilationResult(
                        status=CompilationStatus.FAILED,
                        error="amount='all' must be resolved before compilation.",
                        intent_id=intent.intent_id,
                    )
                amount_decimal: Decimal = intent.amount  # type: ignore[assignment]
                amount_in = int(amount_decimal * Decimal(10**from_token.decimals))
            else:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error="Either amount_usd or amount must be provided",
                    intent_id=intent.intent_id,
                )

            # Step 4: Translate native token addresses for LiFi API
            # Framework uses 0xEeee... sentinel for native tokens, but LiFi expects 0x0000...0000
            from ..connectors.lifi.client import NATIVE_TOKEN_ADDRESS as LIFI_NATIVE_ADDRESS

            lifi_from_address = LIFI_NATIVE_ADDRESS if from_token.is_native else from_token.address
            lifi_to_address = LIFI_NATIVE_ADDRESS if to_token.is_native else to_token.address

            # Step 5: Get quote from LiFi
            logger.info(
                f"Getting LiFi quote: {from_token.symbol}@{source_chain} -> {to_token.symbol}@{dest_chain}, "
                f"amount={amount_in}"
            )

            config = LiFiConfig(
                chain_id=from_chain_id,
                wallet_address=self.wallet_address,
            )
            adapter = LiFiAdapter(
                config,
                price_provider=self.price_oracle,
                allow_placeholder_prices=self._using_placeholders,
            )

            slippage = float(intent.max_slippage)
            quote = adapter.client.get_quote(
                from_chain_id=from_chain_id,
                to_chain_id=to_chain_id,
                from_token=lifi_from_address,
                to_token=lifi_to_address,
                from_amount=str(amount_in),
                from_address=self.wallet_address,
                slippage=slippage,
            )

            # Step 5: Build approve TX if needed (skip for native token)
            approval_address = quote.estimate.approval_address if quote.estimate else ""
            if approval_address and not from_token.is_native:
                approve_txs = self._build_approve_tx(
                    from_token.address,
                    approval_address,
                    amount_in,
                )
                transactions.extend(approve_txs)

            # Step 6: Build swap/bridge TX from LiFi quote
            tx_request = quote.transaction_request
            if tx_request is None or not tx_request.to or not tx_request.data:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error="LiFi quote missing transaction_request data",
                    intent_id=intent.intent_id,
                )
            tx_type = "bridge_deferred" if is_cross_chain else "swap_deferred"
            description_action = "Bridge" if is_cross_chain else "Swap"

            raw_value = tx_request.value if tx_request else None
            if raw_value:
                raw_str = str(raw_value)
                value = int(raw_str, 16) if raw_str.startswith("0x") else int(raw_str)
            else:
                value = 0
            gas_estimate = 200000
            if quote.estimate and quote.estimate.total_gas_estimate > 0:
                gas_estimate = quote.estimate.total_gas_estimate
            elif tx_request and tx_request.gas_limit:
                try:
                    gl = str(tx_request.gas_limit)
                    gas_estimate = int(gl, 16) if gl.startswith("0x") else int(gl)
                except (ValueError, TypeError):
                    pass

            swap_tx = TransactionData(
                to=tx_request.to if tx_request else "",
                value=value,
                data=tx_request.data if tx_request else "",
                gas_estimate=gas_estimate,
                description=(
                    f"{description_action} via LiFi ({quote.tool}): "
                    f"{self._format_amount(amount_in, from_token.decimals)} {from_token.symbol} -> {to_token.symbol}"
                ),
                tx_type=tx_type,
            )
            transactions.append(swap_tx)

            # Step 7: Build ActionBundle
            total_gas = sum(tx.gas_estimate for tx in transactions)
            amount_out = quote.get_to_amount()
            amount_out_min = quote.get_to_amount_min()

            action_bundle = ActionBundle(
                intent_type=IntentType.SWAP.value,
                transactions=[tx.to_dict() for tx in transactions],
                metadata={
                    "from_token": from_token.to_dict(),
                    "to_token": to_token.to_dict(),
                    "amount_in": str(amount_in),
                    "amount_out": str(amount_out),
                    "min_amount_out": str(amount_out_min),
                    "slippage": str(intent.max_slippage),
                    "protocol": "lifi",
                    "tool": quote.tool,
                    "from_chain_id": from_chain_id,
                    "to_chain_id": to_chain_id,
                    "is_cross_chain": is_cross_chain,
                    "deferred_swap": True,
                    "route_params": {
                        "from_chain_id": from_chain_id,
                        "to_chain_id": to_chain_id,
                        "from_token": lifi_from_address,
                        "to_token": lifi_to_address,
                        "from_amount": str(amount_in),
                        "from_address": self.wallet_address,
                        "to_address": self._resolve_dest_wallet(dest_chain) if is_cross_chain else self.wallet_address,
                        "slippage": slippage,
                    },
                },
            )

            result.action_bundle = action_bundle
            result.transactions = transactions
            result.total_gas_estimate = total_gas
            result.warnings = warnings

            # Format amounts for user-friendly logging
            amount_in_fmt = format_token_amount(amount_in, from_token.symbol, from_token.decimals)
            amount_out_fmt = format_token_amount(amount_out, to_token.symbol, to_token.decimals)
            min_out_fmt = format_token_amount(amount_out_min, to_token.symbol, to_token.decimals)
            slippage_fmt = format_percentage(intent.max_slippage)

            chain_info = f"{source_chain}->{dest_chain}" if is_cross_chain else source_chain
            logger.info(
                f"Compiled SWAP (LiFi/{quote.tool}): {amount_in_fmt} -> {amount_out_fmt} "
                f"(min: {min_out_fmt}) [{chain_info}]"
            )
            logger.info(f"   Slippage: {slippage_fmt} | Txs: {len(transactions)} | Gas: {total_gas:,}")

        except Exception as e:
            logger.exception("Failed to compile LiFi SWAP intent")
            result.status = CompilationStatus.FAILED
            result.error = str(e)

        return result

    def _compile_cross_chain_swap(self, intent: SwapIntent) -> CompilationResult:
        """Compile a cross-chain SWAP intent using Enso.

        Cross-chain swaps use Enso's routing which handles bridging automatically.
        This method:
        1. Resolves token addresses for source and destination chains
        2. Gets cross-chain route from Enso API (via gateway gRPC or direct client)
        3. Builds approve TX if needed
        4. Returns the transaction from Enso

        Args:
            intent: SwapIntent with destination_chain set

        Returns:
            CompilationResult with cross-chain swap ActionBundle
        """
        from ..connectors.enso import CHAIN_MAPPING

        result = CompilationResult(
            status=CompilationStatus.SUCCESS,
            intent_id=intent.intent_id,
        )
        transactions: list[TransactionData] = []
        warnings: list[str] = []

        try:
            source_chain = intent.chain or self.chain
            dest_chain = intent.destination_chain

            if not dest_chain:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error="Cross-chain swap requires destination_chain to be set",
                    intent_id=intent.intent_id,
                )

            # Step 1: Resolve token addresses for source chain
            from_token = self._resolve_token(intent.from_token, chain=source_chain)
            if from_token is None:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"Unknown token on {source_chain}: {intent.from_token}",
                    intent_id=intent.intent_id,
                )

            # Resolve token on destination chain
            to_token = self._resolve_token(intent.to_token, chain=dest_chain)
            if to_token is None:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"Unknown token on {dest_chain}: {intent.to_token}",
                    intent_id=intent.intent_id,
                )

            # Step 2: Calculate input amount
            if intent.amount_usd is not None:
                amount_in = self._usd_to_token_amount(intent.amount_usd, from_token)
            elif intent.amount is not None:
                if intent.amount == "all":
                    return CompilationResult(
                        status=CompilationStatus.FAILED,
                        error="amount='all' must be resolved before compilation for cross-chain swaps.",
                        intent_id=intent.intent_id,
                    )
                amount_decimal: Decimal = intent.amount  # type: ignore[assignment]
                amount_in = int(amount_decimal * Decimal(10**from_token.decimals))
            else:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error="Either amount_usd or amount must be provided",
                    intent_id=intent.intent_id,
                )

            # Step 3: Get cross-chain route from Enso (via gateway gRPC or direct client)
            logger.info(
                f"Getting cross-chain route: {source_chain} {from_token.symbol} -> {dest_chain} {to_token.symbol}, amount={amount_in}"
            )

            slippage_bps = int(intent.max_slippage * 10000)
            dest_chain_id = CHAIN_MAPPING.get(dest_chain.lower())
            if dest_chain_id is None:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"Unsupported destination chain: {dest_chain}",
                    intent_id=intent.intent_id,
                )

            dest_wallet = self._resolve_dest_wallet(dest_chain)
            route_data = self._get_enso_route(
                from_token.address,
                to_token.address,
                str(amount_in),
                slippage_bps,
                chain=source_chain,
                destination_chain_id=dest_chain_id,
                receiver=dest_wallet,
                refund_receiver=dest_wallet,
            )

            # Step 4: Build approve TX if needed (skip for native token)
            router_address = route_data["to"]
            if not from_token.is_native:
                approve_txs = self._build_approve_tx(
                    from_token.address,
                    router_address,
                    amount_in,
                )
                transactions.extend(approve_txs)

            # Step 5: Build swap TX from Enso route
            value = int(route_data["value"]) if route_data["value"] else 0
            swap_tx = TransactionData(
                to=route_data["to"],
                value=value,
                data=route_data["data"],
                gas_estimate=route_data["gas"] if route_data["gas"] else 300000,
                description=(
                    f"Cross-chain swap via Enso: {self._format_amount(amount_in, from_token.decimals)} {from_token.symbol} ({source_chain}) -> {to_token.symbol} ({dest_chain})"
                ),
                tx_type="cross_chain_swap",
            )
            transactions.append(swap_tx)

            # Step 6: Build ActionBundle
            total_gas = sum(tx.gas_estimate for tx in transactions)
            amount_out = int(route_data["amount_out"]) if route_data["amount_out"] else 0
            bridge_fee = route_data.get("bridge_fee")
            estimated_time = route_data.get("estimated_time")

            action_bundle = ActionBundle(
                intent_type=IntentType.SWAP.value,
                transactions=[tx.to_dict() for tx in transactions],
                metadata={
                    "from_token": from_token.to_dict(),
                    "to_token": to_token.to_dict(),
                    "amount_in": str(amount_in),
                    "amount_out": str(amount_out),
                    "slippage": str(intent.max_slippage),
                    "protocol": "enso",
                    "router": router_address,
                    "source_chain": source_chain,
                    "destination_chain": dest_chain,
                    "is_cross_chain": True,
                    "bridge_fee": bridge_fee,
                    "estimated_time": estimated_time,
                },
            )

            result.action_bundle = action_bundle
            result.transactions = transactions
            result.total_gas_estimate = total_gas
            result.warnings = warnings

            logger.info(
                f"Compiled cross-chain SWAP intent: {from_token.symbol} ({source_chain}) -> {to_token.symbol} ({dest_chain}), {len(transactions)} txs, bridge_fee={bridge_fee}, est_time={estimated_time}s"
            )

        except Exception as e:
            logger.exception(f"Failed to compile cross-chain SWAP intent: {e}")
            result.status = CompilationStatus.FAILED
            result.error = str(e)

        return result

    def _compile_lp_open(self, intent: LPOpenIntent) -> CompilationResult:
        """Compile an LP_OPEN intent into an ActionBundle.

        This method:
        1. Resolves pool token addresses
        2. Converts price range to tick range (or bin range for TraderJoe)
        3. Calculates minimum amounts with slippage
        4. Builds approve TXs for both tokens
        5. Builds mint position TX

        Args:
            intent: LPOpenIntent to compile

        Returns:
            CompilationResult with LP mint ActionBundle
        """
        # Route Meteora DLMM to Solana-specific adapter
        if intent.protocol == "meteora_dlmm":
            if not self._is_solana_chain():
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    intent_id=intent.intent_id,
                    error="Meteora DLMM is only supported on Solana",
                )
            return self._compile_meteora_lp_open(intent)

        # Route Orca Whirlpools to Solana-specific adapter
        if intent.protocol == "orca_whirlpools":
            if not self._is_solana_chain():
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    intent_id=intent.intent_id,
                    error="Orca Whirlpools is only supported on Solana",
                )
            return self._compile_orca_lp_open(intent)

        # Route Raydium CLMM to Solana-specific adapter (default LP protocol on Solana)
        if intent.protocol == "raydium_clmm" or (self._is_solana_chain() and intent.protocol is None):
            return self._compile_raydium_lp_open(intent)

        # Fail explicitly for unsupported protocols on Solana
        if self._is_solana_chain():
            allowed_solana_lp = {"raydium_clmm", "meteora_dlmm", "orca_whirlpools"}
            return CompilationResult(
                status=CompilationStatus.FAILED,
                intent_id=intent.intent_id,
                error=f"Protocol '{intent.protocol}' is not supported for LP_OPEN on Solana. Supported: {', '.join(sorted(allowed_solana_lp))}",
            )

        # Handle Uniswap V4 LP separately (flash accounting via PositionManager)
        if self._resolve_protocol(intent.protocol) == "uniswap_v4":
            return self._compile_lp_open_uniswap_v4(intent)

        # Handle TraderJoe V2 separately (different architecture - bins vs ticks)
        if intent.protocol == "traderjoe_v2":
            return self._compile_lp_open_traderjoe_v2(intent)

        # Handle Aerodrome/Velodrome separately (Solidly-fork with fungible LP tokens)
        # Resolve alias so velodrome -> aerodrome on Optimism (LP dispatch doesn't pre-resolve)
        if self._resolve_protocol(intent.protocol) == "aerodrome":
            return self._compile_lp_open_aerodrome(intent)

        # Handle Pendle LP (single-token liquidity provision)
        if intent.protocol == "pendle":
            return self._compile_pendle_lp_open(intent)

        # Handle Curve LP (pool-based AMM with proportional liquidity)
        if intent.protocol == "curve":
            return self._compile_lp_open_curve(intent)

        # Handle Fluid DEX LP (Arbitrum only, unencumbered positions)
        if intent.protocol == "fluid":
            return self._compile_lp_open_fluid(intent)

        result = CompilationResult(
            status=CompilationStatus.SUCCESS,
            intent_id=intent.intent_id,
        )
        transactions: list[TransactionData] = []
        warnings: list[str] = []

        try:
            # Step 1: Get LP adapter (resolve alias e.g. "agni" -> "uniswap_v3")
            protocol = self._resolve_protocol(intent.protocol)
            adapter = UniswapV3LPAdapter(self.chain, protocol)
            position_manager = adapter.get_position_manager_address()

            if position_manager == "0x0000000000000000000000000000000000000000":
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=(f"Unknown position manager for protocol {protocol} on {self.chain}"),
                    intent_id=intent.intent_id,
                )

            # Step 2: Parse pool info to get token addresses
            # Pool format expected: "0xPoolAddress" or "TOKEN0/TOKEN1/FEE"
            pool_info = self._parse_pool_info(intent.pool)
            if pool_info is None:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"Could not parse pool info: {intent.pool}",
                    intent_id=intent.intent_id,
                )

            token0_info, token1_info, fee_tier, tokens_swapped = pool_info

            # When tokens were reordered to match on-chain convention (token0 addr < token1 addr),
            # we must invert the price range and swap the amounts to stay consistent.
            # The user specified prices as "token1-per-token0" in their original ordering.
            # After swapping, that relationship is inverted: new price = 1 / old price.
            range_lower = intent.range_lower
            range_upper = intent.range_upper
            amount0 = intent.amount0
            amount1 = intent.amount1

            if tokens_swapped:
                # Invert price range: if user said 550-670 (WBNB in USDT), after swap
                # token0=USDT, token1=WBNB, so price is now WBNB-per-USDT = 1/550 to 1/670.
                # new_lower = 1/old_upper, new_upper = 1/old_lower (preserves lower < upper).
                range_lower = Decimal(1) / intent.range_upper
                range_upper = Decimal(1) / intent.range_lower
                # Swap amounts to match new token order
                amount0, amount1 = amount1, amount0
                logger.debug(
                    f"Tokens swapped: inverted price range [{intent.range_lower}, {intent.range_upper}] "
                    f"-> [{range_lower:.10f}, {range_upper:.10f}], swapped amounts"
                )

            # Validate pool existence (best-effort)
            from .pool_validation import validate_v3_pool

            pool_check = validate_v3_pool(
                self.chain,
                protocol,
                token0_info.address,
                token1_info.address,
                fee_tier,
                self._get_chain_rpc_url(),
            )
            failed = self._validate_pool(pool_check, intent.intent_id)
            if failed is not None:
                return failed

            # Step 3: Convert amounts to wei
            amount0_desired = int(amount0 * Decimal(10**token0_info.decimals))
            amount1_desired = int(amount1 * Decimal(10**token1_info.decimals))

            # Step 4: Convert price range to ticks
            # Uniswap V3 uses tick-based ranges: price = 1.0001^tick
            # Price must be adjusted for token decimals difference
            tick_lower = self._price_to_tick(
                range_lower,
                token0_decimals=token0_info.decimals,
                token1_decimals=token1_info.decimals,
            )
            tick_upper = self._price_to_tick(
                range_upper,
                token0_decimals=token0_info.decimals,
                token1_decimals=token1_info.decimals,
            )

            # Align ticks to tick spacing (60 for 0.3% fee tier)
            tick_spacing = self._get_tick_spacing(fee_tier)
            tick_lower = (tick_lower // tick_spacing) * tick_spacing
            tick_upper = (tick_upper // tick_spacing) * tick_spacing

            logger.debug(
                f"LP tick calculation: price_range=[{range_lower:.8f}, {range_upper:.8f}]"
                f"{' (inverted)' if tokens_swapped else ''}, "
                f"decimals=({token0_info.decimals}, {token1_info.decimals}), "
                f"ticks=[{tick_lower}, {tick_upper}], spacing={tick_spacing}"
            )

            # Step 5: Calculate minimum amounts using LP slippage
            # LP slippage is different from swap slippage:
            # - In swaps, slippage = receiving fewer tokens (real loss)
            # - In LP, slippage = different deposit ratio (no loss, just different position)
            # Default 20% slippage (80% minimum), configurable to 100% (0 minimum) for volatile pairs
            lp_slippage = getattr(intent, "max_slippage", None) or self.default_lp_slippage
            min_multiplier = Decimal("1") - lp_slippage  # 0.80 for 20% slippage
            amount0_min = int(amount0_desired * min_multiplier)
            amount1_min = int(amount1_desired * min_multiplier)

            logger.debug(
                f"LP mint: slippage={float(lp_slippage) * 100:.1f}%, amount0={amount0_desired} (min={amount0_min}), amount1={amount1_desired} (min={amount1_min})"
            )

            # Step 6: Build approve TXs for both tokens
            if amount0_desired > 0 and not token0_info.is_native:
                approve_txs0 = self._build_approve_tx(
                    token0_info.address,
                    position_manager,
                    amount0_desired,
                )
                transactions.extend(approve_txs0)

            if amount1_desired > 0 and not token1_info.is_native:
                approve_txs1 = self._build_approve_tx(
                    token1_info.address,
                    position_manager,
                    amount1_desired,
                )
                transactions.extend(approve_txs1)

            # Step 7: Build mint TX
            deadline = int(datetime.now(UTC).timestamp()) + self.default_deadline_seconds

            mint_calldata = adapter.get_mint_calldata(
                token0=token0_info.address,
                token1=token1_info.address,
                fee=fee_tier,
                tick_lower=tick_lower,
                tick_upper=tick_upper,
                amount0_desired=amount0_desired,
                amount1_desired=amount1_desired,
                amount0_min=amount0_min,
                amount1_min=amount1_min,
                recipient=self.wallet_address,
                deadline=deadline,
            )

            # Handle native token (ETH) - send value with transaction
            value = 0
            if token0_info.is_native:
                value = amount0_desired
                warnings.append("Token0 is native - sending ETH with transaction")
            elif token1_info.is_native:
                value = amount1_desired
                warnings.append("Token1 is native - sending ETH with transaction")

            mint_tx = TransactionData(
                to=position_manager,
                value=value,
                data="0x" + mint_calldata.hex(),
                gas_estimate=adapter.estimate_mint_gas(),
                description=(
                    f"Mint LP position: "
                    f"{self._format_amount(amount0_desired, token0_info.decimals)} "
                    f"{token0_info.symbol} + "
                    f"{self._format_amount(amount1_desired, token1_info.decimals)} "
                    f"{token1_info.symbol} "
                    f"[{intent.range_lower:.2f} - {intent.range_upper:.2f}]"
                ),
                tx_type="lp_mint",
            )
            transactions.append(mint_tx)

            # Step 8: Build ActionBundle
            total_gas = sum(tx.gas_estimate for tx in transactions)

            action_bundle = ActionBundle(
                intent_type=IntentType.LP_OPEN.value,
                transactions=[tx.to_dict() for tx in transactions],
                metadata={
                    "pool": intent.pool,
                    "token0": token0_info.to_dict(),
                    "token1": token1_info.to_dict(),
                    "fee_tier": fee_tier,
                    "tick_lower": tick_lower,
                    "tick_upper": tick_upper,
                    "range_lower": str(intent.range_lower),
                    "range_upper": str(intent.range_upper),
                    "amount0_desired": str(amount0_desired),
                    "amount1_desired": str(amount1_desired),
                    "amount0_min": str(amount0_min),
                    "amount1_min": str(amount1_min),
                    "protocol": protocol,
                    "position_manager": position_manager,
                    "deadline": deadline,
                    "chain": self.chain,
                },
            )

            result.action_bundle = action_bundle
            result.transactions = transactions
            result.total_gas_estimate = total_gas
            result.warnings = warnings

            tx_types = " + ".join(tx.tx_type for tx in transactions) if transactions else ""
            tx_summary = f" ({tx_types})" if tx_types else ""
            logger.info(
                f"Compiled LP_OPEN intent: {token0_info.symbol}/{token1_info.symbol}, range [{intent.range_lower:.2f}-{intent.range_upper:.2f}], {len(transactions)} txs{tx_summary}, {total_gas} gas"
            )

        except Exception as e:
            logger.exception(f"Failed to compile LP_OPEN intent: {e}")
            result.status = CompilationStatus.FAILED
            result.error = str(e)

        return result

    def _compile_lp_open_fluid(self, intent: "LPOpenIntent") -> "CompilationResult":
        """Compile LP_OPEN intent for Fluid DEX T1 (Arbitrum only).

        Phase 1 limitation: LP deposit is not yet supported on-chain.
        Fluid DEX deposit() reverts due to complex Liquidity-layer routing.
        This method short-circuits with a clear FAILED status.
        """
        return CompilationResult(
            status=CompilationStatus.FAILED,
            error=(
                "Fluid DEX LP_OPEN is not supported in phase 1. "
                "The Liquidity-layer routing causes on-chain reverts on all pools. "
                "LP deposit support is a follow-up. Use swap intents instead."
            ),
            intent_id=intent.intent_id,
        )

    def _compile_lp_close_fluid(self, intent: "LPCloseIntent") -> "CompilationResult":
        """Compile LP_CLOSE intent for Fluid DEX T1 (with encumbrance guard).

        ENCUMBRANCE GUARD: Rejects compilation if the pool has smart-collateral
        or smart-debt enabled, preventing liquidation risk.
        """
        result = CompilationResult(
            status=CompilationStatus.SUCCESS,
            intent_id=intent.intent_id,
        )
        transactions: list[TransactionData] = []

        try:
            from almanak.framework.connectors.fluid import FluidAdapter, FluidConfig

            try:
                nft_id = int(intent.position_id)
            except ValueError:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"Invalid Fluid position ID (must be integer): {intent.position_id}",
                    intent_id=intent.intent_id,
                )

            dex_address = intent.pool
            if not dex_address or not dex_address.startswith("0x"):
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=(f"Fluid LP_CLOSE requires pool address in pool field. Got pool={intent.pool}"),
                    intent_id=intent.intent_id,
                )

            rpc_url = self._get_chain_rpc_url()
            if not rpc_url:
                raise ValueError("RPC URL required for Fluid DEX adapter.")

            config = FluidConfig(
                chain=self.chain,
                wallet_address=self.wallet_address,
                rpc_url=rpc_url,
            )
            fluid_adapter = FluidAdapter(config)

            # COMPILE-TIME ENCUMBRANCE GUARD — raises if pool has smart-debt/collateral
            lp_tx = fluid_adapter.build_remove_liquidity_transaction(
                dex_address=dex_address,
                nft_id=nft_id,
            )

            transactions.append(
                TransactionData(
                    to=lp_tx.to,
                    value=lp_tx.value,
                    data=lp_tx.data,
                    gas_estimate=lp_tx.gas,
                    description=lp_tx.description,
                    tx_type="fluid_operate_close",
                )
            )

            total_gas = sum(tx.gas_estimate for tx in transactions)
            action_bundle = ActionBundle(
                intent_type=IntentType.LP_CLOSE.value,
                transactions=[tx.to_dict() for tx in transactions],
                metadata={
                    "dex_address": dex_address,
                    "nft_id": nft_id,
                    "protocol": "fluid",
                    "chain": self.chain,
                },
            )

            result.action_bundle = action_bundle
            result.transactions = transactions
            result.total_gas_estimate = total_gas

            logger.info(
                f"Compiled Fluid LP_CLOSE intent: nft_id={nft_id}, pool={dex_address}, "
                f"{len(transactions)} txs, {total_gas} gas"
            )

        except Exception as e:
            logger.exception(f"Failed to compile Fluid LP_CLOSE intent: {e}")
            result.status = CompilationStatus.FAILED
            result.error = str(e)

        return result

    def _compile_lp_open_uniswap_v4(self, intent: LPOpenIntent) -> CompilationResult:
        """Compile LP_OPEN intent for Uniswap V4 via PositionManager.

        V4 uses flash accounting (modifyLiquidities + Actions-encoded bytes).
        This delegates to the UniswapV4Adapter which handles the full encoding.
        """
        from almanak.framework.connectors.uniswap_v4.adapter import UniswapV4Adapter, UniswapV4Config

        result = CompilationResult(
            status=CompilationStatus.SUCCESS,
            intent_id=intent.intent_id,
        )

        try:
            config = UniswapV4Config(
                chain=self.chain,
                wallet_address=self.wallet_address,
            )
            adapter = UniswapV4Adapter(config=config, token_resolver=self._token_resolver)
            bundle = adapter.compile_lp_open_intent(intent, self.price_oracle)

            if not bundle.transactions:
                error_msg = bundle.metadata.get("error", "Unknown error during V4 LP_OPEN compilation")
                result.status = CompilationStatus.FAILED
                result.error = error_msg
                return result

            result.action_bundle = bundle
            result.transactions = [
                TransactionData(
                    to=tx["to"],
                    value=int(tx.get("value", 0)),
                    data=tx["data"],
                    gas_estimate=tx.get("gas_estimate", 0),
                    description=tx.get("description", ""),
                    tx_type="approve" if "approve" in tx.get("description", "").lower() else "lp_mint",
                )
                for tx in bundle.transactions
            ]
            result.total_gas_estimate = bundle.metadata.get("gas_estimate", 0)

            # Forward warnings
            if bundle.metadata.get("warnings"):
                result.warnings = bundle.metadata["warnings"]

            logger.info(
                "Compiled V4 LP_OPEN intent: %d txs, %d gas, pool=%s",
                len(bundle.transactions),
                result.total_gas_estimate,
                intent.pool,
            )

        except Exception as e:
            logger.exception("Failed to compile V4 LP_OPEN intent: %s", e)
            result.status = CompilationStatus.FAILED
            result.error = str(e)

        return result

    def _compile_lp_close_uniswap_v4(self, intent: LPCloseIntent) -> CompilationResult:
        """Compile LP_CLOSE intent for Uniswap V4 via PositionManager.

        V4 uses flash accounting (modifyLiquidities + Actions-encoded bytes).
        This delegates to the UniswapV4Adapter which handles the full encoding.

        Note: In production, the caller should provide liquidity and currency addresses
        from an on-chain position query. For offline compilation, we use placeholder
        values that will be updated at execution time.
        """
        from almanak.framework.connectors.uniswap_v4.adapter import UniswapV4Adapter, UniswapV4Config

        result = CompilationResult(
            status=CompilationStatus.SUCCESS,
            intent_id=intent.intent_id,
        )

        try:
            config = UniswapV4Config(
                chain=self.chain,
                wallet_address=self.wallet_address,
            )
            adapter = UniswapV4Adapter(config=config, token_resolver=self._token_resolver)

            # Extract liquidity and currency addresses from protocol_params if available
            # LPCloseIntent may not have protocol_params field
            liquidity = 0
            currency0 = ""
            currency1 = ""
            protocol_params = getattr(intent, "protocol_params", None) or {}
            if protocol_params:
                liquidity = int(protocol_params.get("liquidity", 0))
                currency0 = protocol_params.get("currency0", "")
                currency1 = protocol_params.get("currency1", "")

            # If pool is specified, try to resolve currency addresses
            if (not currency0 or not currency1) and intent.pool:
                try:
                    parts = intent.pool.split("/")
                    if len(parts) >= 2:
                        addr0, _ = adapter._resolve_token(parts[0], for_v4_pool=True)
                        addr1, _ = adapter._resolve_token(parts[1], for_v4_pool=True)
                        # Ensure sorted order
                        if int(addr0, 16) > int(addr1, 16):
                            addr0, addr1 = addr1, addr0
                        currency0 = addr0
                        currency1 = addr1
                except (ValueError, KeyError) as e:
                    logger.debug("Could not resolve currencies from pool '%s': %s", type(e).__name__, e)
                except Exception as e:
                    # TokenNotFoundError/TokenResolutionError or unexpected errors — log, don't swallow
                    logger.warning("Failed to resolve currencies from pool '%s': %s", intent.pool, e)

            # Fail fast: liquidity and currency addresses are required for a valid close
            if liquidity == 0:
                result.status = CompilationStatus.FAILED
                result.error = (
                    "V4 LP_CLOSE requires 'liquidity' in protocol_params (query on-chain position first). "
                    "Example: intent.protocol_params = {'liquidity': <int>, 'currency0': '<addr>', 'currency1': '<addr>'}"
                )
                return result
            if not currency0 or not currency1:
                result.status = CompilationStatus.FAILED
                result.error = (
                    "V4 LP_CLOSE requires 'currency0' and 'currency1' in protocol_params "
                    "or a resolvable 'pool' string (e.g. 'WETH/USDC/3000')."
                )
                return result

            # Enforce canonical V4 ordering: currency0 < currency1
            if int(currency0, 16) > int(currency1, 16):
                currency0, currency1 = currency1, currency0

            bundle = adapter.compile_lp_close_intent(
                intent,
                liquidity=liquidity,
                currency0=currency0,
                currency1=currency1,
            )

            if not bundle.transactions:
                error_msg = bundle.metadata.get("error", "Unknown error during V4 LP_CLOSE compilation")
                result.status = CompilationStatus.FAILED
                result.error = error_msg
                return result

            result.action_bundle = bundle
            result.transactions = [
                TransactionData(
                    to=tx["to"],
                    value=int(tx.get("value", 0)),
                    data=tx["data"],
                    gas_estimate=tx.get("gas_estimate", 0),
                    description=tx.get("description", ""),
                    tx_type="lp_close",
                )
                for tx in bundle.transactions
            ]
            result.total_gas_estimate = bundle.metadata.get("gas_estimate", 0)

            if bundle.metadata.get("warnings"):
                result.warnings = bundle.metadata["warnings"]

            logger.info(
                "Compiled V4 LP_CLOSE intent: position_id=%s, %d txs, %d gas",
                intent.position_id,
                len(bundle.transactions),
                result.total_gas_estimate,
            )

        except Exception as e:
            logger.exception("Failed to compile V4 LP_CLOSE intent: %s", e)
            result.status = CompilationStatus.FAILED
            result.error = str(e)

        return result

    def _compile_lp_open_traderjoe_v2(self, intent: LPOpenIntent) -> CompilationResult:
        """Compile LP_OPEN intent for TraderJoe V2 Liquidity Book.

        TraderJoe V2 uses discrete price bins instead of continuous ticks:
        - Price at bin ID: price = (1 + binStep/10000)^(binId - 8388608)
        - Liquidity is distributed across bins with explicit distributions
        - LP tokens are fungible ERC1155-like tokens per bin (not NFTs)

        Args:
            intent: LPOpenIntent to compile

        Returns:
            CompilationResult with TraderJoe V2 LP ActionBundle
        """
        result = CompilationResult(
            status=CompilationStatus.SUCCESS,
            intent_id=intent.intent_id,
        )
        transactions: list[TransactionData] = []
        warnings: list[str] = []

        try:
            # Import TraderJoe V2 adapter (lazy import to avoid circular deps)
            from almanak.framework.connectors.traderjoe_v2 import TraderJoeV2Adapter, TraderJoeV2Config

            # Parse pool info (format: TOKEN_X/TOKEN_Y/BIN_STEP)
            pool_parts = intent.pool.split("/")
            if len(pool_parts) < 2:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"Invalid pool format for TraderJoe V2: {intent.pool}. Expected format: TOKEN_X/TOKEN_Y/BIN_STEP",
                    intent_id=intent.intent_id,
                )

            token_x_symbol = pool_parts[0]
            token_y_symbol = pool_parts[1]
            bin_step = int(pool_parts[2]) if len(pool_parts) > 2 else 20

            # Resolve token addresses and info via TokenResolver
            token_x_info = self._resolve_token(token_x_symbol)
            token_y_info = self._resolve_token(token_y_symbol)

            if not token_x_info:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"Unknown token {token_x_symbol} for chain {self.chain}",
                    intent_id=intent.intent_id,
                )
            if not token_y_info:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"Unknown token {token_y_symbol} for chain {self.chain}",
                    intent_id=intent.intent_id,
                )

            token_x_addr = token_x_info.address
            token_y_addr = token_y_info.address

            # Validate pool existence (best-effort)
            from .pool_validation import validate_traderjoe_pool

            pool_check = validate_traderjoe_pool(
                self.chain, token_x_addr, token_y_addr, bin_step, self._get_chain_rpc_url()
            )
            failed = self._validate_pool(pool_check, intent.intent_id)
            if failed is not None:
                return failed

            # Convert amounts to wei
            amount_x_wei = int(intent.amount0 * Decimal(10**token_x_info.decimals))
            amount_y_wei = int(intent.amount1 * Decimal(10**token_y_info.decimals))

            # Get router address (position manager for TraderJoe V2)
            router_address = LP_POSITION_MANAGERS.get(self.chain, {}).get(
                "traderjoe_v2", "0x0000000000000000000000000000000000000000"
            )

            if router_address == "0x0000000000000000000000000000000000000000":
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"TraderJoe V2 not configured for chain {self.chain}",
                    intent_id=intent.intent_id,
                )

            # Build approval TXs for both tokens
            if amount_x_wei > 0 and not token_x_info.is_native:
                approve_txs_x = self._build_approve_tx(
                    token_x_info.address,
                    router_address,
                    amount_x_wei,
                )
                transactions.extend(approve_txs_x)

            if amount_y_wei > 0 and not token_y_info.is_native:
                approve_txs_y = self._build_approve_tx(
                    token_y_info.address,
                    router_address,
                    amount_y_wei,
                )
                transactions.extend(approve_txs_y)

            # Get RPC URL
            rpc_url = self._get_chain_rpc_url()
            if not rpc_url:
                raise ValueError(
                    "RPC URL required for TraderJoe V2 adapter. "
                    "Either provide rpc_url to IntentCompiler or use GatewayExecutionOrchestrator."
                )

            # Create TraderJoe V2 adapter to build the liquidity TX
            config = TraderJoeV2Config(
                chain=self.chain,
                wallet_address=self.wallet_address,
                rpc_url=rpc_url,
            )
            tj_adapter = TraderJoeV2Adapter(config)

            # Number of bins on each side of active bin
            # Read from intent's protocol_params if provided, otherwise default to 5
            params = intent.protocol_params or {}
            bin_range = int(params.get("bin_range", 5))
            if bin_range < 1 or bin_range > 100:
                raise ValueError(f"bin_range must be between 1 and 100, got {bin_range}")

            # Build add liquidity transaction
            lp_tx = tj_adapter.build_add_liquidity_transaction(
                token_x=token_x_addr,
                token_y=token_y_addr,
                amount_x=intent.amount0,
                amount_y=intent.amount1,
                bin_step=bin_step,
                bin_range=bin_range,
            )

            # Convert to TransactionData format
            lp_tx_data = TransactionData(
                to=lp_tx.to,
                value=lp_tx.value,
                data=lp_tx.data if isinstance(lp_tx.data, str) else lp_tx.data,
                gas_estimate=lp_tx.gas or 400000,
                description=(
                    f"Add liquidity to TraderJoe V2: {intent.amount0} {token_x_symbol} + {intent.amount1} {token_y_symbol} (bin_step={bin_step})"
                ),
                tx_type="traderjoe_v2_add_liquidity",
            )
            transactions.append(lp_tx_data)

            # Build ActionBundle
            total_gas = sum(tx.gas_estimate for tx in transactions)

            action_bundle = ActionBundle(
                intent_type=IntentType.LP_OPEN.value,
                transactions=[tx.to_dict() for tx in transactions],
                metadata={
                    "pool": intent.pool,
                    "token_x": token_x_info.to_dict(),
                    "token_y": token_y_info.to_dict(),
                    "bin_step": bin_step,
                    "bin_range": bin_range,
                    "range_lower": str(intent.range_lower),
                    "range_upper": str(intent.range_upper),
                    "amount_x": str(amount_x_wei),
                    "amount_y": str(amount_y_wei),
                    "protocol": "traderjoe_v2",
                    "router": router_address,
                    "chain": self.chain,
                },
            )

            result.action_bundle = action_bundle
            result.transactions = transactions
            result.total_gas_estimate = total_gas
            result.warnings = warnings

            tx_types = " + ".join(tx.tx_type for tx in transactions) if transactions else ""
            tx_summary = f" ({tx_types})" if tx_types else ""
            logger.info(
                f"Compiled TraderJoe V2 LP_OPEN intent: {token_x_symbol}/{token_y_symbol}, bin_step={bin_step}, {len(transactions)} txs{tx_summary}, {total_gas} gas"
            )

        except Exception as e:
            logger.exception(f"Failed to compile TraderJoe V2 LP_OPEN intent: {e}")
            result.status = CompilationStatus.FAILED
            result.error = str(e)

        return result

    def _compile_lp_close(self, intent: LPCloseIntent) -> CompilationResult:
        """Compile an LP_CLOSE intent into an ActionBundle.

        This method:
        1. Builds decreaseLiquidity TX to remove all liquidity
        2. Builds collect TX to collect tokens and fees
        3. Optionally builds burn TX (if position is empty)

        Args:
            intent: LPCloseIntent to compile

        Returns:
            CompilationResult with LP close ActionBundle
        """
        # Route Meteora DLMM to Solana-specific adapter
        if intent.protocol == "meteora_dlmm":
            if not self._is_solana_chain():
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    intent_id=intent.intent_id,
                    error="Meteora DLMM is only supported on Solana",
                )
            return self._compile_meteora_lp_close(intent)

        # Route Orca Whirlpools to Solana-specific adapter
        if intent.protocol == "orca_whirlpools":
            if not self._is_solana_chain():
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    intent_id=intent.intent_id,
                    error="Orca Whirlpools is only supported on Solana",
                )
            return self._compile_orca_lp_close(intent)

        # Route Raydium CLMM to Solana-specific adapter (default LP protocol on Solana)
        if intent.protocol == "raydium_clmm" or (self._is_solana_chain() and intent.protocol is None):
            return self._compile_raydium_lp_close(intent)

        # Fail explicitly for unsupported protocols on Solana
        if self._is_solana_chain():
            allowed_solana_lp = {"raydium_clmm", "meteora_dlmm", "orca_whirlpools"}
            return CompilationResult(
                status=CompilationStatus.FAILED,
                intent_id=intent.intent_id,
                error=f"Protocol '{intent.protocol}' is not supported for LP_CLOSE on Solana. Supported: {', '.join(sorted(allowed_solana_lp))}",
            )

        # Handle Uniswap V4 LP close separately (flash accounting via PositionManager)
        if self._resolve_protocol(intent.protocol) == "uniswap_v4":
            return self._compile_lp_close_uniswap_v4(intent)

        # Handle TraderJoe V2 separately
        if intent.protocol == "traderjoe_v2":
            return self._compile_lp_close_traderjoe_v2(intent)

        # Handle Aerodrome/Velodrome separately (Solidly-fork with fungible LP tokens)
        # Resolve alias so velodrome -> aerodrome on Optimism (LP dispatch doesn't pre-resolve)
        if self._resolve_protocol(intent.protocol) == "aerodrome":
            return self._compile_lp_close_aerodrome(intent)

        # Handle Pendle LP close
        if intent.protocol == "pendle":
            return self._compile_pendle_lp_close(intent)

        # Handle Curve LP close (pool-based AMM, proportional removal)
        if intent.protocol == "curve":
            return self._compile_lp_close_curve(intent)

        # Handle Fluid DEX LP close (with encumbrance guard)
        if intent.protocol == "fluid":
            return self._compile_lp_close_fluid(intent)

        result = CompilationResult(
            status=CompilationStatus.SUCCESS,
            intent_id=intent.intent_id,
        )
        transactions: list[TransactionData] = []
        warnings: list[str] = []

        try:
            # Step 1: Get LP adapter (resolve alias e.g. "agni" -> "uniswap_v3")
            protocol = self._resolve_protocol(intent.protocol)
            adapter = UniswapV3LPAdapter(self.chain, protocol)
            position_manager = adapter.get_position_manager_address()

            if position_manager == "0x0000000000000000000000000000000000000000":
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=(f"Unknown position manager for protocol {protocol} on {self.chain}"),
                    intent_id=intent.intent_id,
                )

            # Step 2: Parse position ID to token ID
            try:
                token_id = int(intent.position_id)
            except ValueError:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"Invalid position ID (must be integer): {intent.position_id}",
                    intent_id=intent.intent_id,
                )

            deadline = int(datetime.now(UTC).timestamp()) + self.default_deadline_seconds

            # Step 3: Query position's actual liquidity and tokens owed from on-chain
            liquidity = self._query_position_liquidity(position_manager, token_id)
            if liquidity is None:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"Could not query liquidity for position #{token_id}. Ensure rpc_url is provided to IntentCompiler.",
                    intent_id=intent.intent_id,
                )

            # Query tokens owed (fees + withdrawn liquidity that hasn't been collected)
            tokens_owed0, tokens_owed1 = self._query_position_tokens_owed(position_manager, token_id)
            tokens_owed_unknown = tokens_owed0 is None or tokens_owed1 is None
            if tokens_owed_unknown:
                warnings.append(f"Could not query tokens owed for position #{token_id} - collecting anyway")
            elif tokens_owed0 == 0 and tokens_owed1 == 0:
                warnings.append(
                    f"Position #{token_id} has no tokens owed pre-decrease - will still collect after close"
                )

            # Step 3a: Skip decreaseLiquidity if position has 0 liquidity
            # (position may already be closed or liquidity already removed)
            if liquidity == 0:
                warnings.append(f"Position #{token_id} has 0 liquidity - skipping decreaseLiquidity step")
            else:
                # Use 0 for min amounts to ensure position can be closed
                amount0_min = 0
                amount1_min = 0

                decrease_calldata = adapter.get_decrease_liquidity_calldata(
                    token_id=token_id,
                    liquidity=liquidity,
                    amount0_min=amount0_min,
                    amount1_min=amount1_min,
                    deadline=deadline,
                )

                decrease_tx = TransactionData(
                    to=position_manager,
                    value=0,
                    data="0x" + decrease_calldata.hex(),
                    gas_estimate=get_gas_estimate(self.chain, "lp_decrease_liquidity"),
                    description=f"Decrease liquidity: position #{token_id} (remove all)",
                    tx_type="lp_decrease_liquidity",
                )
                transactions.append(decrease_tx)

            # Determine if position has anything to collect/burn
            # Treat unknown owed as potential activity (collect anyway to avoid leaving fees uncollected)
            position_has_activity = (
                liquidity > 0
                or tokens_owed_unknown
                or (tokens_owed0 is not None and tokens_owed1 is not None and (tokens_owed0 > 0 or tokens_owed1 > 0))
            )

            # Step 4: Build collect TX
            # Collect when requested AND position has activity (liquidity decreased or fees owed)
            # Skip collect on already-closed/burned positions to avoid guaranteed reverts
            if intent.collect_fees and position_has_activity:
                collect_calldata = adapter.get_collect_calldata(
                    token_id=token_id,
                    recipient=self.wallet_address,
                    amount0_max=MAX_UINT128,
                    amount1_max=MAX_UINT128,
                )

                collect_tx = TransactionData(
                    to=position_manager,
                    value=0,
                    data="0x" + collect_calldata.hex(),
                    gas_estimate=get_gas_estimate(self.chain, "lp_collect"),
                    description=f"Collect tokens and fees: position #{token_id}",
                    tx_type="lp_collect",
                )
                transactions.append(collect_tx)
            elif intent.collect_fees:
                warnings.append(f"Skipping collect for position #{token_id} - position appears already closed")
            else:
                warnings.append("Skipping fee collection as collect_fees=False")

            # Step 5: Build burn TX
            # Only burn if position has activity (decreased liquidity or has tokens owed)
            # If position was already closed (0 liquidity, 0 tokens owed), skip burn
            # to avoid reverting on already-burned NFTs
            should_burn = position_has_activity

            if should_burn:
                burn_calldata = adapter.get_burn_calldata(token_id=token_id)

                burn_tx = TransactionData(
                    to=position_manager,
                    value=0,
                    data="0x" + burn_calldata.hex(),
                    gas_estimate=get_gas_estimate(self.chain, "lp_burn"),
                    description=f"Burn position NFT: #{token_id}",
                    tx_type="lp_burn",
                )
                transactions.append(burn_tx)
            else:
                warnings.append(
                    f"Position #{token_id} appears already closed (0 liquidity, 0 tokens owed) - skipping burn"
                )

            # Step 6: Build ActionBundle
            total_gas = sum(tx.gas_estimate for tx in transactions)

            action_bundle = ActionBundle(
                intent_type=IntentType.LP_CLOSE.value,
                transactions=[tx.to_dict() for tx in transactions],
                metadata={
                    "position_id": intent.position_id,
                    "token_id": token_id,
                    "pool": intent.pool,
                    "collect_fees": intent.collect_fees,
                    "protocol": protocol,
                    "position_manager": position_manager,
                    "deadline": deadline,
                    "chain": self.chain,
                },
            )

            result.action_bundle = action_bundle
            result.transactions = transactions
            result.total_gas_estimate = total_gas
            result.warnings = warnings

            tx_types = " + ".join(tx.tx_type for tx in transactions) if transactions else ""
            tx_summary = f" ({tx_types})" if tx_types else ""
            logger.info(
                f"Compiled LP_CLOSE intent: position #{token_id}, collect_fees={intent.collect_fees}, {len(transactions)} txs{tx_summary}, {total_gas} gas"
            )

        except Exception as e:
            logger.exception(f"Failed to compile LP_CLOSE intent: {e}")
            result.status = CompilationStatus.FAILED
            result.error = str(e)

        return result

    def _compile_lp_close_traderjoe_v2(self, intent: LPCloseIntent) -> CompilationResult:
        """Compile LP_CLOSE intent for TraderJoe V2 Liquidity Book.

        TraderJoe V2 LP close differs from Uniswap V3:
        - Need to query LP token balances per bin
        - Call removeLiquidity with bin IDs and amounts
        - No NFT to burn (fungible LP tokens)

        Args:
            intent: LPCloseIntent to compile

        Returns:
            CompilationResult with TraderJoe V2 LP close ActionBundle
        """
        result = CompilationResult(
            status=CompilationStatus.SUCCESS,
            intent_id=intent.intent_id,
        )
        transactions: list[TransactionData] = []
        warnings: list[str] = []

        try:
            # Import TraderJoe V2 adapter
            from almanak.framework.connectors.traderjoe_v2 import TraderJoeV2Adapter, TraderJoeV2Config

            # Parse pool info (format: TOKEN_X/TOKEN_Y/BIN_STEP)
            if intent.pool is None:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error="pool is required for TraderJoe V2 LP close",
                    intent_id=intent.intent_id,
                )
            pool_parts = intent.pool.split("/")
            if len(pool_parts) < 2:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"Invalid pool format for TraderJoe V2: {intent.pool}. Expected format: TOKEN_X/TOKEN_Y/BIN_STEP",
                    intent_id=intent.intent_id,
                )

            token_x_symbol = pool_parts[0]
            token_y_symbol = pool_parts[1]
            bin_step = int(pool_parts[2]) if len(pool_parts) > 2 else 20

            # Resolve token addresses via TokenResolver
            token_x_info = self._resolve_token(token_x_symbol)
            token_y_info = self._resolve_token(token_y_symbol)

            if not token_x_info or not token_y_info:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"Unknown tokens for pool {intent.pool} on {self.chain}",
                    intent_id=intent.intent_id,
                )

            token_x_addr = token_x_info.address
            token_y_addr = token_y_info.address

            # Get RPC URL
            rpc_url = self._get_chain_rpc_url()
            if not rpc_url:
                raise ValueError(
                    "RPC URL required for TraderJoe V2 adapter. "
                    "Either provide rpc_url to IntentCompiler or use GatewayExecutionOrchestrator."
                )

            # Create TraderJoe V2 adapter
            config = TraderJoeV2Config(
                chain=self.chain,
                wallet_address=self.wallet_address,
                rpc_url=rpc_url,
            )
            tj_adapter = TraderJoeV2Adapter(config)

            # Get position to check if we have liquidity
            t0 = time.perf_counter()
            position = tj_adapter.get_position(token_x_addr, token_y_addr, bin_step)
            logger.debug(f"TraderJoe V2 get_position (LP_CLOSE): {time.perf_counter() - t0:.2f}s")

            if not position or not position.bin_ids:
                warnings.append("No LP position found to close")
                action_bundle = ActionBundle(
                    intent_type=IntentType.LP_CLOSE.value,
                    transactions=[],
                    metadata={
                        "pool": intent.pool,
                        "protocol": "traderjoe_v2",
                        "warning": "No position found",
                    },
                )
                result.action_bundle = action_bundle
                result.warnings = warnings
                return result

            # Build approval for LB tokens (ERC1155-like, need approveForAll)
            pool_addr = position.pool_address
            router_addr = tj_adapter.sdk.router_address
            approve_tx, approve_gas = tj_adapter.sdk.build_approve_for_all_transaction(
                pool_address=pool_addr,
                spender_address=router_addr,
                from_address=self.wallet_address,
            )
            approve_tx_data = TransactionData(
                to=approve_tx["to"],
                value=approve_tx.get("value", 0),
                data=approve_tx["data"].hex() if isinstance(approve_tx["data"], bytes) else approve_tx["data"],
                gas_estimate=approve_gas,
                description="Approve LB tokens for router",
                tx_type="approve",
            )
            transactions.append(approve_tx_data)

            # Build remove liquidity transaction - pass pre-fetched position to
            # avoid a redundant get_position() call (saves ~50 serial RPC calls)
            lp_tx = tj_adapter.build_remove_liquidity_transaction(
                token_x=token_x_addr,
                token_y=token_y_addr,
                bin_step=bin_step,
                position=position,
            )

            if lp_tx is None:
                warnings.append("No LP position found to close")
                # Return success with empty transactions
                action_bundle = ActionBundle(
                    intent_type=IntentType.LP_CLOSE.value,
                    transactions=[],
                    metadata={
                        "pool": intent.pool,
                        "protocol": "traderjoe_v2",
                        "warning": "No position found",
                    },
                )
                result.action_bundle = action_bundle
                result.warnings = warnings
                return result

            # Convert to TransactionData format
            lp_tx_data = TransactionData(
                to=lp_tx.to,
                value=lp_tx.value,
                data=lp_tx.data if isinstance(lp_tx.data, str) else lp_tx.data,
                gas_estimate=lp_tx.gas or 300000,
                description=(f"Remove liquidity from TraderJoe V2: {token_x_symbol}/{token_y_symbol}"),
                tx_type="traderjoe_v2_remove_liquidity",
            )
            transactions.append(lp_tx_data)

            # Build ActionBundle
            total_gas = sum(tx.gas_estimate for tx in transactions)

            action_bundle = ActionBundle(
                intent_type=IntentType.LP_CLOSE.value,
                transactions=[tx.to_dict() for tx in transactions],
                metadata={
                    "pool": intent.pool,
                    "position_id": intent.position_id,
                    "collect_fees": intent.collect_fees,
                    "protocol": "traderjoe_v2",
                    "chain": self.chain,
                },
            )

            result.action_bundle = action_bundle
            result.transactions = transactions
            result.total_gas_estimate = total_gas
            result.warnings = warnings

            tx_types = " + ".join(tx.tx_type for tx in transactions) if transactions else ""
            tx_summary = f" ({tx_types})" if tx_types else ""
            logger.info(
                f"Compiled TraderJoe V2 LP_CLOSE intent: {token_x_symbol}/{token_y_symbol}, {len(transactions)} txs{tx_summary}, {total_gas} gas"
            )

        except Exception as e:
            logger.exception(f"Failed to compile TraderJoe V2 LP_CLOSE intent: {e}")
            result.status = CompilationStatus.FAILED
            result.error = str(e)

        return result

    def _compile_collect_fees(self, intent: "CollectFeesIntent") -> CompilationResult:
        """Compile an LP_COLLECT_FEES intent into an ActionBundle.

        Routes to protocol-specific handlers for fee collection.

        Args:
            intent: CollectFeesIntent to compile

        Returns:
            CompilationResult with fee collection ActionBundle
        """
        from .vocabulary import CollectFeesIntent

        if not isinstance(intent, CollectFeesIntent):
            return CompilationResult(
                status=CompilationStatus.FAILED,
                error="Expected CollectFeesIntent",
                intent_id=intent.intent_id,
            )

        protocol = self._resolve_protocol(intent.protocol)
        if protocol == "traderjoe_v2":
            return self._compile_collect_fees_traderjoe_v2(intent)

        if protocol == "uniswap_v4":
            return self._compile_collect_fees_uniswap_v4(intent)

        return CompilationResult(
            status=CompilationStatus.FAILED,
            error=f"Protocol '{intent.protocol}' does not support LP_COLLECT_FEES. Supported: traderjoe_v2, uniswap_v4",
            intent_id=intent.intent_id,
        )

    def _compile_collect_fees_traderjoe_v2(self, intent: "CollectFeesIntent") -> CompilationResult:
        """Compile LP_COLLECT_FEES intent for TraderJoe V2 Liquidity Book.

        Calls LBPair.collectFees(account, binIds) to harvest accumulated fees
        without removing any liquidity from the position.

        Args:
            intent: CollectFeesIntent to compile

        Returns:
            CompilationResult with TraderJoe V2 fee collection ActionBundle
        """
        result = CompilationResult(
            status=CompilationStatus.SUCCESS,
            intent_id=intent.intent_id,
        )
        transactions: list[TransactionData] = []
        warnings: list[str] = []

        try:
            from almanak.framework.connectors.traderjoe_v2 import TraderJoeV2Adapter, TraderJoeV2Config

            # Parse pool info (format: TOKEN_X/TOKEN_Y/BIN_STEP)
            pool_parts = intent.pool.split("/")
            if len(pool_parts) < 2:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"Invalid pool format for TraderJoe V2: {intent.pool}. Expected: TOKEN_X/TOKEN_Y/BIN_STEP",
                    intent_id=intent.intent_id,
                )

            token_x_symbol = pool_parts[0]
            token_y_symbol = pool_parts[1]
            bin_step = int(pool_parts[2]) if len(pool_parts) > 2 else 20

            # Resolve token addresses via TokenResolver
            token_x_info = self._resolve_token(token_x_symbol)
            token_y_info = self._resolve_token(token_y_symbol)

            if not token_x_info or not token_y_info:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"Unknown tokens for pool {intent.pool} on {self.chain}",
                    intent_id=intent.intent_id,
                )

            token_x_addr = token_x_info.address
            token_y_addr = token_y_info.address

            # Get RPC URL
            rpc_url = self._get_chain_rpc_url()
            if not rpc_url:
                raise ValueError(
                    "RPC URL required for TraderJoe V2 adapter. "
                    "Either provide rpc_url to IntentCompiler or use GatewayExecutionOrchestrator."
                )

            # Create TraderJoe V2 adapter
            config = TraderJoeV2Config(
                chain=self.chain,
                wallet_address=self.wallet_address,
                rpc_url=rpc_url,
            )
            tj_adapter = TraderJoeV2Adapter(config)

            # Get position to check if we have liquidity
            position = tj_adapter.get_position(token_x_addr, token_y_addr, bin_step)
            if not position or not position.bin_ids:
                warnings.append("No LP position found for fee collection")
                action_bundle = ActionBundle(
                    intent_type=IntentType.LP_COLLECT_FEES.value,
                    transactions=[],
                    metadata={
                        "pool": intent.pool,
                        "protocol": "traderjoe_v2",
                        "warning": "No position found",
                    },
                )
                result.action_bundle = action_bundle
                result.warnings = warnings
                return result

            # Build collect fees transaction (no approval needed - calling LBPair directly)
            fee_tx = tj_adapter.build_collect_fees_transaction(
                token_x=token_x_addr,
                token_y=token_y_addr,
                bin_step=bin_step,
            )

            if fee_tx is None:
                warnings.append("No LP position found for fee collection")
                action_bundle = ActionBundle(
                    intent_type=IntentType.LP_COLLECT_FEES.value,
                    transactions=[],
                    metadata={
                        "pool": intent.pool,
                        "protocol": "traderjoe_v2",
                        "warning": "No position found",
                    },
                )
                result.action_bundle = action_bundle
                result.warnings = warnings
                return result

            # Convert to TransactionData format
            fee_tx_data = TransactionData(
                to=fee_tx.to,
                value=fee_tx.value,
                data=fee_tx.data if isinstance(fee_tx.data, str) else fee_tx.data,
                gas_estimate=fee_tx.gas or 200000,
                description=f"Collect fees from TraderJoe V2: {token_x_symbol}/{token_y_symbol}",
                tx_type="traderjoe_v2_collect_fees",
            )
            transactions.append(fee_tx_data)

            # Build ActionBundle
            total_gas = sum(tx.gas_estimate for tx in transactions)

            action_bundle = ActionBundle(
                intent_type=IntentType.LP_COLLECT_FEES.value,
                transactions=[tx.to_dict() for tx in transactions],
                metadata={
                    "pool": intent.pool,
                    "protocol": "traderjoe_v2",
                    "chain": self.chain,
                    "bin_ids": position.bin_ids,
                },
            )

            result.action_bundle = action_bundle
            result.transactions = transactions
            result.total_gas_estimate = total_gas
            result.warnings = warnings

            logger.info(
                f"Compiled TraderJoe V2 LP_COLLECT_FEES intent: {token_x_symbol}/{token_y_symbol}, "
                f"{len(position.bin_ids)} bins, {total_gas} gas"
            )

        except Exception as e:
            logger.exception(f"Failed to compile TraderJoe V2 LP_COLLECT_FEES intent: {e}")
            result.status = CompilationStatus.FAILED
            result.error = str(e)

        return result

    def _compile_collect_fees_uniswap_v4(self, intent: "CollectFeesIntent") -> CompilationResult:
        """Compile LP_COLLECT_FEES intent for Uniswap V4 via PositionManager.

        Decreases liquidity by 0 (triggers fee accrual update) then takes the
        accrued fees via TAKE_PAIR.

        Args:
            intent: CollectFeesIntent to compile

        Returns:
            CompilationResult with V4 fee collection ActionBundle
        """
        from almanak.framework.connectors.uniswap_v4.adapter import UniswapV4Adapter, UniswapV4Config

        result = CompilationResult(
            status=CompilationStatus.SUCCESS,
            intent_id=intent.intent_id,
        )

        try:
            config = UniswapV4Config(
                chain=self.chain,
                wallet_address=self.wallet_address,
            )
            adapter = UniswapV4Adapter(config=config, token_resolver=self._token_resolver)

            # Extract required params
            protocol_params = getattr(intent, "protocol_params", None) or {}
            position_id = protocol_params.get("position_id") or getattr(intent, "position_id", None)
            if not position_id:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error="V4 LP_COLLECT_FEES requires 'position_id' in protocol_params.",
                    intent_id=intent.intent_id,
                )

            currency0 = protocol_params.get("currency0", "")
            currency1 = protocol_params.get("currency1", "")

            # Try resolving from pool string if currencies not provided
            if (not currency0 or not currency1) and intent.pool:
                try:
                    parts = intent.pool.split("/")
                    if len(parts) >= 2:
                        addr0, _ = adapter._resolve_token(parts[0], for_v4_pool=True)
                        addr1, _ = adapter._resolve_token(parts[1], for_v4_pool=True)
                        if int(addr0, 16) > int(addr1, 16):
                            addr0, addr1 = addr1, addr0
                        currency0 = currency0 or addr0
                        currency1 = currency1 or addr1
                except Exception as e:
                    logger.warning("Failed to resolve currencies from pool '%s': %s", intent.pool, e)

            if not currency0 or not currency1:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=(
                        "V4 LP_COLLECT_FEES requires 'currency0' and 'currency1' in protocol_params "
                        "or a resolvable 'pool' string (e.g. 'WETH/USDC/3000')."
                    ),
                    intent_id=intent.intent_id,
                )

            # Enforce canonical V4 ordering: currency0 < currency1
            if int(currency0, 16) > int(currency1, 16):
                currency0, currency1 = currency1, currency0

            hook_data = b""
            hook_data_hex = protocol_params.get("hook_data", "")
            if hook_data_hex:
                hook_data = bytes.fromhex(hook_data_hex.replace("0x", ""))

            bundle = adapter.compile_collect_fees_intent(
                position_id=int(position_id),
                currency0=currency0,
                currency1=currency1,
                hook_data=hook_data,
            )

            if not bundle.transactions:
                error_msg = bundle.metadata.get("error", "Unknown error during V4 LP_COLLECT_FEES compilation")
                result.status = CompilationStatus.FAILED
                result.error = error_msg
                return result

            result.action_bundle = bundle
            result.transactions = [
                TransactionData(
                    to=tx["to"],
                    value=int(tx.get("value", 0)),
                    data=tx["data"],
                    gas_estimate=tx.get("gas_estimate", 0),
                    description=tx.get("description", ""),
                    tx_type="lp_collect_fees",
                )
                for tx in bundle.transactions
            ]
            result.total_gas_estimate = bundle.metadata.get("gas_estimate", 0)

            # Forward warnings (e.g. hook warnings)
            if bundle.metadata.get("warnings"):
                result.warnings = bundle.metadata["warnings"]

            logger.info(
                "Compiled V4 LP_COLLECT_FEES intent: position_id=%s, %d txs",
                position_id,
                len(bundle.transactions),
            )

        except Exception as e:
            logger.exception("Failed to compile V4 LP_COLLECT_FEES intent: %s", e)
            result.status = CompilationStatus.FAILED
            result.error = str(e)

        return result

    def _compile_lp_open_aerodrome(self, intent: LPOpenIntent) -> CompilationResult:
        """Compile LP_OPEN intent for Aerodrome Finance (Solidly fork on Base).

        Aerodrome uses a simple xy=k or x³y+y³x AMM with:
        - Fungible LP tokens (not NFTs)
        - Two pool types: volatile (0.3% fee) and stable (0.05% fee)
        - Full range liquidity (no concentrated positions)

        Pool format: "TOKEN0/TOKEN1/volatile" or "TOKEN0/TOKEN1/stable"

        Args:
            intent: LPOpenIntent to compile

        Returns:
            CompilationResult with Aerodrome addLiquidity ActionBundle
        """
        result = CompilationResult(
            status=CompilationStatus.SUCCESS,
            intent_id=intent.intent_id,
        )
        transactions: list[Any] = []
        warnings: list[str] = []

        try:
            # Import Aerodrome adapter (lazy import to avoid circular deps)
            from almanak.framework.connectors.aerodrome import AerodromeAdapter, AerodromeConfig

            # Parse pool info (format: TOKEN0/TOKEN1/pool_type)
            pool_parts = intent.pool.split("/")
            if len(pool_parts) < 2:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"Invalid pool format: {intent.pool}. Expected: TOKEN0/TOKEN1/volatile or TOKEN0/TOKEN1/stable",
                    intent_id=intent.intent_id,
                )

            token0_symbol = pool_parts[0]
            token1_symbol = pool_parts[1]
            # Default to volatile if not specified
            stable = pool_parts[2].lower() == "stable" if len(pool_parts) > 2 else False

            logger.info(
                f"Compiling Aerodrome LP_OPEN: {token0_symbol}/{token1_symbol}, stable={stable}, amounts={intent.amount0}/{intent.amount1}"
            )

            # Resolve token addresses
            token0_info = self._resolve_token(token0_symbol)
            token1_info = self._resolve_token(token1_symbol)

            if token0_info is None:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"Unknown token: {token0_symbol}",
                    intent_id=intent.intent_id,
                )
            if token1_info is None:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"Unknown token: {token1_symbol}",
                    intent_id=intent.intent_id,
                )

            # Validate pool existence (best-effort)
            from .pool_validation import validate_aerodrome_pool

            pool_check = validate_aerodrome_pool(
                self.chain, token0_info.address, token1_info.address, stable, self._get_chain_rpc_url()
            )
            failed = self._validate_pool(pool_check, intent.intent_id)
            if failed is not None:
                return failed

            # Convert amounts to wei
            int(intent.amount0 * Decimal(10**token0_info.decimals))
            int(intent.amount1 * Decimal(10**token1_info.decimals))

            # Get router address
            router_address = LP_POSITION_MANAGERS.get(self.chain, {}).get(
                "aerodrome", "0x0000000000000000000000000000000000000000"
            )

            if router_address == "0x0000000000000000000000000000000000000000":
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"Aerodrome not supported on {self.chain}",
                    intent_id=intent.intent_id,
                )

            # Create Aerodrome adapter to build all transactions
            # The adapter handles approvals and the addLiquidity call
            config = AerodromeConfig(
                chain=self.chain,
                wallet_address=self.wallet_address,
                price_provider=self.price_oracle,
                rpc_url=self._get_chain_rpc_url(),
            )
            adapter = AerodromeAdapter(config)

            # Build addLiquidity transaction using the adapter
            liquidity_result = adapter.add_liquidity(
                token_a=token0_symbol,
                token_b=token1_symbol,
                amount_a=intent.amount0,
                amount_b=intent.amount1,
                stable=stable,
                recipient=self.wallet_address,
            )

            if not liquidity_result.success:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"Failed to build addLiquidity TX: {liquidity_result.error}",
                    intent_id=intent.intent_id,
                )

            # Use transactions from the adapter result (includes approvals + addLiquidity)
            # The adapter already builds all needed transactions
            for tx in liquidity_result.transactions:
                transactions.append(tx)

            # Build ActionBundle
            total_gas = sum(tx.gas_estimate for tx in transactions)

            action_bundle = ActionBundle(
                intent_type=IntentType.LP_OPEN.value,
                transactions=[tx.to_dict() for tx in transactions],
                metadata={
                    "pool": intent.pool,
                    "token0": token0_info.to_dict(),
                    "token1": token1_info.to_dict(),
                    "stable": stable,
                    "amount0": str(intent.amount0),
                    "amount1": str(intent.amount1),
                    "protocol": "aerodrome",
                },
            )

            result.action_bundle = action_bundle
            result.transactions = transactions
            result.total_gas_estimate = total_gas
            result.warnings = warnings

            tx_types = " + ".join(tx.tx_type for tx in transactions) if transactions else ""
            tx_summary = f" ({tx_types})" if tx_types else ""
            logger.info(
                f"Compiled Aerodrome LP_OPEN intent: {token0_symbol}/{token1_symbol}, stable={stable}, {len(transactions)} txs{tx_summary}, {total_gas} gas"
            )

        except Exception as e:
            logger.exception(f"Failed to compile Aerodrome LP_OPEN intent: {e}")
            result.status = CompilationStatus.FAILED
            result.error = str(e)

        return result

    def _compile_lp_close_aerodrome(self, intent: LPCloseIntent) -> CompilationResult:
        """Compile LP_CLOSE intent for Aerodrome Finance.

        Aerodrome LP close:
        1. Approve LP tokens for router (if needed)
        2. Call removeLiquidity to burn LP and receive both tokens

        Pool format: "TOKEN0/TOKEN1/volatile" or "TOKEN0/TOKEN1/stable"

        Args:
            intent: LPCloseIntent to compile

        Returns:
            CompilationResult with Aerodrome removeLiquidity ActionBundle
        """
        result = CompilationResult(
            status=CompilationStatus.SUCCESS,
            intent_id=intent.intent_id,
        )
        transactions: list[Any] = []
        warnings: list[str] = []

        try:
            # Import Aerodrome adapter (lazy import to avoid circular deps)
            from almanak.framework.connectors.aerodrome import AerodromeAdapter, AerodromeConfig

            # Parse pool info from position_id (format: TOKEN0/TOKEN1/pool_type)
            pool_parts = intent.position_id.split("/")
            if len(pool_parts) < 2:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"Invalid position ID: {intent.position_id}. Expected: TOKEN0/TOKEN1/volatile or TOKEN0/TOKEN1/stable",
                    intent_id=intent.intent_id,
                )

            token0_symbol = pool_parts[0]
            token1_symbol = pool_parts[1]
            stable = pool_parts[2].lower() == "stable" if len(pool_parts) > 2 else False

            logger.info(f"Compiling Aerodrome LP_CLOSE: {token0_symbol}/{token1_symbol}, stable={stable}")

            # Resolve token addresses
            token0_info = self._resolve_token(token0_symbol)
            token1_info = self._resolve_token(token1_symbol)

            if token0_info is None:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"Unknown token: {token0_symbol}",
                    intent_id=intent.intent_id,
                )
            if token1_info is None:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"Unknown token: {token1_symbol}",
                    intent_id=intent.intent_id,
                )

            # Get router address
            router_address = LP_POSITION_MANAGERS.get(self.chain, {}).get(
                "aerodrome", "0x0000000000000000000000000000000000000000"
            )

            if router_address == "0x0000000000000000000000000000000000000000":
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"Aerodrome not supported on {self.chain}",
                    intent_id=intent.intent_id,
                )

            # Create Aerodrome adapter
            config = AerodromeConfig(
                chain=self.chain,
                wallet_address=self.wallet_address,
                price_provider=self.price_oracle,
                rpc_url=self._get_chain_rpc_url(),
            )
            adapter = AerodromeAdapter(config)

            # Get LP token address for the pool
            pool_address = adapter.sdk.get_pool_address(
                token0_info.address,
                token1_info.address,
                stable,
            )

            if not pool_address:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"Pool not found for {token0_symbol}/{token1_symbol} (stable={stable})",
                    intent_id=intent.intent_id,
                )

            # Query actual LP token balance from on-chain
            # LP token is the pool contract itself (ERC-20)
            lp_balance_wei = self._query_erc20_balance(pool_address, self.wallet_address)
            if lp_balance_wei is None:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"Could not query LP balance for pool {pool_address}. Ensure rpc_url is provided to IntentCompiler.",
                    intent_id=intent.intent_id,
                )

            if lp_balance_wei == 0:
                warning = (
                    f"No LP tokens found in wallet for {token0_symbol}/{token1_symbol} pool "
                    f"(pool={pool_address}) - treating LP_CLOSE as no-op"
                )
                warnings.append(warning)
                logger.info(warning)

                result.action_bundle = ActionBundle(
                    intent_type=IntentType.LP_CLOSE.value,
                    transactions=[],
                    metadata={
                        "pool": intent.position_id,
                        "pool_address": pool_address,
                        "token0_symbol": token0_symbol,
                        "token1_symbol": token1_symbol,
                        "stable": stable,
                        "protocol": "aerodrome",
                        "collect_fees": intent.collect_fees,
                        "warning": "No LP tokens found; LP_CLOSE no-op",
                    },
                )
                result.transactions = []
                result.total_gas_estimate = 0
                result.warnings = warnings
                return result

            # Convert wei to decimal (LP tokens have 18 decimals)
            lp_balance = Decimal(lp_balance_wei) / Decimal(10**18)
            logger.info(f"Found {lp_balance} LP tokens ({lp_balance_wei} wei) for Aerodrome pool")

            # Build removeLiquidity transaction using the adapter
            liquidity_result = adapter.remove_liquidity(
                token_a=token0_symbol,
                token_b=token1_symbol,
                liquidity=lp_balance,
                stable=stable,
                recipient=self.wallet_address,
            )

            if not liquidity_result.success:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"Failed to build removeLiquidity TX: {liquidity_result.error}",
                    intent_id=intent.intent_id,
                )

            # Use transactions from the adapter result (includes approvals + removeLiquidity)
            for tx in liquidity_result.transactions:
                transactions.append(tx)

            # Build ActionBundle
            total_gas = sum(tx.gas_estimate for tx in transactions)

            action_bundle = ActionBundle(
                intent_type=IntentType.LP_CLOSE.value,
                transactions=[tx.to_dict() for tx in transactions],
                metadata={
                    "pool": intent.position_id,
                    "token0": token0_info.to_dict(),
                    "token1": token1_info.to_dict(),
                    "stable": stable,
                    "protocol": "aerodrome",
                    "collect_fees": intent.collect_fees,
                },
            )

            result.action_bundle = action_bundle
            result.transactions = transactions
            result.total_gas_estimate = total_gas
            result.warnings = warnings

            tx_types = " + ".join(str(getattr(tx, "tx_type", "")) for tx in transactions) if transactions else ""
            tx_summary = f" ({tx_types})" if tx_types else ""
            logger.info(
                f"Compiled Aerodrome LP_CLOSE intent: {token0_symbol}/{token1_symbol}, {len(transactions)} txs{tx_summary}, {total_gas} gas"
            )

        except Exception as e:
            logger.exception(f"Failed to compile Aerodrome LP_CLOSE intent: {e}")
            result.status = CompilationStatus.FAILED
            result.error = str(e)

        return result

    def _compile_swap_aerodrome(self, intent: SwapIntent) -> CompilationResult:
        """Compile SWAP intent for Aerodrome/Velodrome (Solidly forks).

        On Base (Aerodrome): defaults to Slipstream CL pools; classic via swap_params={"classic": True}.
        On Optimism (Velodrome): defaults to classic routing (no CL/Slipstream contracts).

        swap_params options:
        - tick_spacing (int): CL pool tick spacing, default 100
        - classic (bool): If True, use Classic volatile/stable routing
        - stable (bool): Pool type for Classic routing (default False)

        Args:
            intent: SwapIntent with from_token, to_token, and amount

        Returns:
            CompilationResult with Aerodrome swap ActionBundle
        """
        result = CompilationResult(
            status=CompilationStatus.SUCCESS,
            intent_id=intent.intent_id,
        )
        transactions: list[Any] = []

        try:
            # Import Aerodrome adapter (lazy import to avoid circular deps)
            from almanak.framework.connectors.aerodrome import AerodromeAdapter, AerodromeConfig

            # Resolve tokens
            from_token = self._resolve_token(intent.from_token)
            to_token = self._resolve_token(intent.to_token)

            if from_token is None:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"Unknown from_token: {intent.from_token}",
                    intent_id=intent.intent_id,
                )
            if to_token is None:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"Unknown to_token: {intent.to_token}",
                    intent_id=intent.intent_id,
                )

            # Calculate input amount
            amount_decimal: Decimal
            if intent.amount_usd is not None:
                price = self._require_token_price(from_token.symbol)
                amount_decimal = intent.amount_usd / price
            elif intent.amount is not None:
                if intent.amount == "all":
                    return CompilationResult(
                        status=CompilationStatus.FAILED,
                        error="amount='all' must be resolved before compilation. Use Intent.set_resolved_amount() to resolve chained amounts.",
                        intent_id=intent.intent_id,
                    )
                amount_decimal = intent.amount  # type: ignore[assignment]
            else:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error="Either amount_usd or amount must be provided",
                    intent_id=intent.intent_id,
                )

            # Extract routing params from swap_params
            swap_params = intent.swap_params if hasattr(intent, "swap_params") and intent.swap_params else {}
            tick_spacing = swap_params.get("tick_spacing", 100)
            stable = swap_params.get("stable", False)

            # Check chain support dynamically from contract addresses
            from almanak.core.contracts import AERODROME as AERODROME_ADDRESSES

            if self.chain not in AERODROME_ADDRESSES:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"Aerodrome/Velodrome is not supported on {self.chain}. Supported: {list(AERODROME_ADDRESSES.keys())}",
                    intent_id=intent.intent_id,
                )

            # Auto-detect CL (Slipstream) availability from contract addresses.
            # Only Base has cl_router/cl_factory; Optimism (Velodrome) uses classic only.
            chain_addrs = AERODROME_ADDRESSES[self.chain]
            has_cl = bool(chain_addrs.get("cl_router") and chain_addrs.get("cl_factory"))
            requested_classic = swap_params.get("classic")
            if requested_classic is False and not has_cl:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"CL (Slipstream) routing is not available on {self.chain}; use classic routing instead.",
                    intent_id=intent.intent_id,
                )
            use_classic = requested_classic if requested_classic is not None else not has_cl

            routing = "classic" if use_classic else "cl"
            logger.info(
                f"Compiling Aerodrome SWAP ({routing}): {from_token.symbol} -> {to_token.symbol}, amount={amount_decimal}"
            )

            # Validate pool existence
            if use_classic:
                from .pool_validation import validate_aerodrome_pool

                pool_check = validate_aerodrome_pool(
                    self.chain, from_token.address, to_token.address, stable, self._get_chain_rpc_url()
                )
            else:
                from .pool_validation import validate_aerodrome_cl_pool

                pool_check = validate_aerodrome_cl_pool(
                    self.chain, from_token.address, to_token.address, tick_spacing, self._get_chain_rpc_url()
                )
            failed = self._validate_pool(pool_check, intent.intent_id)
            if failed is not None:
                return failed

            # Create Aerodrome adapter
            config = AerodromeConfig(
                chain=self.chain,
                wallet_address=self.wallet_address,
                default_slippage_bps=int(intent.max_slippage * Decimal("10000")),
                price_provider=self.price_oracle,
                rpc_url=self._get_chain_rpc_url(),
            )
            adapter = AerodromeAdapter(config)

            # Build swap using adapter
            swap_result = adapter.swap_exact_input(
                token_in=from_token.symbol,
                token_out=to_token.symbol,
                amount_in=amount_decimal,
                stable=stable,
                tick_spacing=tick_spacing,
                use_classic=use_classic,
            )

            if not swap_result.success:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=swap_result.error or "Aerodrome swap failed",
                    intent_id=intent.intent_id,
                )

            # Convert adapter transactions to compiler format
            for tx_data in swap_result.transactions:
                transactions.append(tx_data)

            total_gas = sum(tx.gas_estimate for tx in transactions)

            action_bundle = ActionBundle(
                intent_type=IntentType.SWAP.value,
                transactions=[tx.to_dict() for tx in transactions],
                metadata={
                    "from_token": from_token.to_dict(),
                    "to_token": to_token.to_dict(),
                    "amount_in": str(amount_decimal),
                    "routing": routing,
                    "protocol": "aerodrome",
                },
            )

            result.action_bundle = action_bundle
            result.transactions = transactions
            result.total_gas_estimate = total_gas

            logger.info(
                f"Compiled Aerodrome SWAP intent ({routing}): {from_token.symbol} -> {to_token.symbol}, {len(transactions)} txs, {total_gas} gas"
            )

        except Exception as e:
            logger.exception(f"Failed to compile Aerodrome SWAP intent: {e}")
            result.status = CompilationStatus.FAILED
            result.error = str(e)

        return result

    def _compile_swap_curve(self, intent: SwapIntent) -> CompilationResult:
        """Compile SWAP intent for Curve Finance.

        Curve uses pool-specific AMMs (StableSwap, CryptoSwap, Tricrypto).
        The pool is selected automatically from the registry by matching the
        token pair, or can be overridden via swap_params={"pool": "0x..."}.

        swap_params options:
        - pool (str): Explicit pool address (overrides auto-lookup)
        - slippage_bps (int): Override slippage in basis points

        Args:
            intent: SwapIntent with from_token, to_token, and amount

        Returns:
            CompilationResult with Curve exchange ActionBundle
        """
        from almanak.framework.connectors.curve.adapter import (
            CURVE_ADDRESSES,
            CURVE_POOLS,
            CurveAdapter,
            CurveConfig,
        )

        result = CompilationResult(
            status=CompilationStatus.SUCCESS,
            intent_id=intent.intent_id,
        )
        transactions: list[Any] = []

        try:
            # Check chain support
            if self.chain not in CURVE_ADDRESSES:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"Curve is not supported on {self.chain}. Supported chains: {list(CURVE_ADDRESSES.keys())}",
                    intent_id=intent.intent_id,
                )

            # Resolve tokens
            from_token = self._resolve_token(intent.from_token)
            to_token = self._resolve_token(intent.to_token)

            if from_token is None:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"Unknown from_token: {intent.from_token}",
                    intent_id=intent.intent_id,
                )
            if to_token is None:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"Unknown to_token: {intent.to_token}",
                    intent_id=intent.intent_id,
                )

            # Calculate input amount (in token units)
            if intent.amount_usd is not None:
                price = self._require_token_price(from_token.symbol)
                amount_decimal = intent.amount_usd / price
            elif intent.amount is not None:
                if intent.amount == "all":
                    return CompilationResult(
                        status=CompilationStatus.FAILED,
                        error="amount='all' must be resolved before compilation. Use Intent.set_resolved_amount() to resolve chained amounts.",
                        intent_id=intent.intent_id,
                    )
                amount_decimal = Decimal(str(intent.amount))
            else:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error="Either amount_usd or amount must be provided",
                    intent_id=intent.intent_id,
                )

            # Resolve pool address: explicit override or auto-lookup by token pair
            swap_params = intent.swap_params if hasattr(intent, "swap_params") and intent.swap_params else {}
            pool_address: str | None = swap_params.get("pool")
            pool_name: str = ""

            if not pool_address:
                chain_pools = CURVE_POOLS.get(self.chain, {})
                for name, pool_data in chain_pools.items():
                    coins_upper = [c.upper() for c in pool_data["coins"]]
                    if from_token.symbol.upper() in coins_upper and to_token.symbol.upper() in coins_upper:
                        pool_address = pool_data["address"]
                        pool_name = name
                        break

            if not pool_address:
                chain_pools = CURVE_POOLS.get(self.chain, {})
                available = {name: d["coins"] for name, d in chain_pools.items()}
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=(
                        f"No Curve pool found for {from_token.symbol}/{to_token.symbol} on {self.chain}. "
                        f"Available pools: {available}. "
                        f'You can specify a pool explicitly via swap_params={{"pool": "0x..."}}.'
                    ),
                    intent_id=intent.intent_id,
                )

            slippage_bps = int(intent.max_slippage * Decimal("10000"))

            logger.info(
                "Compiling Curve SWAP: %s -> %s, pool=%s (%s), amount=%s",
                from_token.symbol,
                to_token.symbol,
                pool_name or pool_address,
                self.chain,
                amount_decimal,
            )

            config = CurveConfig(
                chain=self.chain,
                wallet_address=self.wallet_address,
                default_slippage_bps=slippage_bps,
                rpc_url=self._get_chain_rpc_url(),
            )
            adapter = CurveAdapter(config)

            # Compute price ratio for CryptoSwap/Tricrypto slippage protection.
            # price_ratio = price_in / price_out so that:
            # expected_output_tokens = amount_in_tokens * price_ratio
            price_ratio: Decimal | None = None
            try:
                price_in = self._require_token_price(from_token.symbol)
                price_out = self._require_token_price(to_token.symbol)
                if price_out > 0:
                    price_ratio = price_in / price_out
            except (ValueError, ZeroDivisionError):
                # Price unavailable — adapter will reject CryptoSwap swaps (fail closed)
                # and accept StableSwap swaps (price_ratio not needed for 1:1 pairs)
                logger.warning(
                    "Could not compute price_ratio for Curve swap %s -> %s; "
                    "CryptoSwap pools will fail, StableSwap pools will proceed safely.",
                    from_token.symbol,
                    to_token.symbol,
                )

            swap_result = adapter.swap(
                pool_address=pool_address,
                token_in=from_token.symbol,
                token_out=to_token.symbol,
                amount_in=amount_decimal,
                slippage_bps=slippage_bps,
                price_ratio=price_ratio,
            )

            if not swap_result.success:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=swap_result.error or "Curve swap failed",
                    intent_id=intent.intent_id,
                )

            transactions = swap_result.transactions
            total_gas = sum(tx.gas_estimate for tx in transactions)

            action_bundle = ActionBundle(
                intent_type=IntentType.SWAP.value,
                transactions=[tx.to_dict() for tx in transactions],
                metadata={
                    "from_token": from_token.to_dict(),
                    "to_token": to_token.to_dict(),
                    "amount_in": str(amount_decimal),
                    "pool_address": pool_address,
                    "pool_name": pool_name,
                    "protocol": "curve",
                },
            )

            result.action_bundle = action_bundle
            result.transactions = transactions  # type: ignore[assignment]
            result.total_gas_estimate = total_gas

            logger.info(
                "Compiled Curve SWAP intent: %s -> %s, %d txs, %d gas",
                from_token.symbol,
                to_token.symbol,
                len(transactions),
                total_gas,
            )

        except Exception as e:
            logger.exception("Failed to compile Curve SWAP intent")
            result.status = CompilationStatus.FAILED
            result.error = str(e)

        return result

    def _compile_swap_fluid(self, intent: SwapIntent) -> CompilationResult:
        """Compile SWAP intent for Fluid DEX (Arbitrum only).

        Uses the pool's swapIn() function directly. Automatically discovers
        the Fluid DEX pool for the token pair and determines swap direction.
        """
        result = CompilationResult(
            status=CompilationStatus.SUCCESS,
            intent_id=intent.intent_id,
        )
        transactions: list[TransactionData] = []

        try:
            from almanak.framework.connectors.fluid.sdk import FluidSDK

            # Resolve tokens
            from_token_info = self._resolve_token(intent.from_token)
            to_token_info = self._resolve_token(intent.to_token)

            if not from_token_info:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"Unknown token {intent.from_token} for chain {self.chain}",
                    intent_id=intent.intent_id,
                )
            if not to_token_info:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"Unknown token {intent.to_token} for chain {self.chain}",
                    intent_id=intent.intent_id,
                )

            rpc_url = self._get_chain_rpc_url()
            if not rpc_url:
                raise ValueError("RPC URL required for Fluid DEX adapter.")

            sdk = FluidSDK(chain=self.chain, rpc_url=rpc_url)

            # Find the pool
            pool_addr = sdk.find_dex_by_tokens(from_token_info.address, to_token_info.address)
            if not pool_addr:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"No Fluid DEX pool found for {intent.from_token}/{intent.to_token} on {self.chain}",
                    intent_id=intent.intent_id,
                )

            # Determine swap direction (swap0to1 = from_token is token0)
            pool_data = sdk.get_dex_data(pool_addr)
            from_addr_lower = from_token_info.address.lower()
            swap0to1 = pool_data.token0.lower() == from_addr_lower

            # Calculate input amount (support amount_usd, amount, and "all")
            amount_decimal: Decimal
            if intent.amount_usd is not None:
                price = self._require_token_price(from_token_info.symbol)
                amount_decimal = intent.amount_usd / price
            elif intent.amount is not None:
                if intent.amount == "all":
                    return CompilationResult(
                        status=CompilationStatus.FAILED,
                        error="amount='all' must be resolved before compilation. Use Intent.set_resolved_amount() to resolve chained amounts.",
                        intent_id=intent.intent_id,
                    )
                amount_decimal = intent.amount  # type: ignore[assignment]
            else:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error="Either amount_usd or amount must be provided",
                    intent_id=intent.intent_id,
                )

            # Convert to wei
            amount_in_wei = int(amount_decimal * Decimal(10**from_token_info.decimals))

            # Build approval TX — approve to the pool (Fluid routes through Liquidity layer)
            if not from_token_info.is_native:
                approve_txs = self._build_approve_tx(from_token_info.address, pool_addr, amount_in_wei)
                transactions.extend(approve_txs)

            # Calculate min output with slippage
            # get_swap_quote uses ERC-20 state overrides so approval is not needed for eth_call.
            slippage_bps = int((intent.max_slippage or Decimal("0.005")) * 10000)
            try:
                quote = sdk.get_swap_quote(pool_addr, swap0to1, amount_in_wei, self.wallet_address)
                min_out = quote * (10000 - slippage_bps) // 10000
            except Exception as e:
                from almanak.framework.connectors.fluid.sdk import FluidMinAmountError

                if isinstance(e, FluidMinAmountError):
                    return CompilationResult(
                        status=CompilationStatus.FAILED,
                        error=(
                            f"Fluid swap rejected: {amount_decimal} {intent.from_token} "
                            f"({amount_in_wei} wei) too small for pool. {e}"
                        ),
                        intent_id=intent.intent_id,
                    )
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"Fluid swap quote failed: {e}",
                    intent_id=intent.intent_id,
                )

            # Build swap TX
            swap_tx_data = sdk.build_swap_tx(
                dex_address=pool_addr,
                swap0to1=swap0to1,
                amount_in=amount_in_wei,
                amount_out_min=min_out,
                to=self.wallet_address,
            )

            transactions.append(
                TransactionData(
                    to=swap_tx_data["to"],
                    value=swap_tx_data["value"],
                    data=swap_tx_data["data"],
                    gas_estimate=swap_tx_data["gas"],
                    description=f"Swap {amount_decimal} {intent.from_token} -> {intent.to_token} on Fluid DEX",
                    tx_type="fluid_swap",
                )
            )

            total_gas = sum(tx.gas_estimate for tx in transactions)
            action_bundle = ActionBundle(
                intent_type=IntentType.SWAP.value,
                transactions=[tx.to_dict() for tx in transactions],
                metadata={
                    "from_token": from_token_info.to_dict(),
                    "to_token": to_token_info.to_dict(),
                    "amount_in": str(amount_in_wei),
                    "min_amount_out": str(min_out),
                    "pool": pool_addr,
                    "swap0to1": swap0to1,
                    "protocol": "fluid",
                    "chain": self.chain,
                },
            )

            result.action_bundle = action_bundle
            result.transactions = transactions
            result.total_gas_estimate = total_gas

            logger.info(
                f"Compiled Fluid SWAP: {intent.from_token} -> {intent.to_token}, "
                f"pool={pool_addr[:10]}..., {len(transactions)} txs, {total_gas} gas"
            )

        except Exception as e:
            logger.exception(f"Failed to compile Fluid SWAP intent: {e}")
            result.status = CompilationStatus.FAILED
            result.error = str(e)

        return result

    def _compile_swap_uniswap_v4(self, intent: SwapIntent) -> CompilationResult:
        """Compile SWAP intent for Uniswap V4.

        BLOCKED (VIB-1965): V4 core contracts (PoolManager, PositionManager,
        UniversalRouter, Quoter, StateView) are verified canonical CREATE2
        deployments on all supported chains. However, the V4 adapter currently
        routes swaps through ``v4_swap_router`` which is NOT a canonical Uniswap
        deployment — it may be an empty EOA on some chains. Phase 1 (VIB-1965)
        will rewrite the adapter to use the canonical UniversalRouter
        (``0x66a9893cC07D91D95644AEDD05D03f95e1dBA8Af``).

        Use uniswap_v3 as a drop-in alternative for swap intents until Phase 1.
        """
        logger.warning(
            "Uniswap V4 swap BLOCKED (VIB-1965): adapter uses unverified v4_swap_router. "
            "Use protocol='uniswap_v3' instead. Tokens: %s -> %s",
            intent.from_token,
            intent.to_token,
        )
        return CompilationResult(
            status=CompilationStatus.FAILED,
            intent_id=intent.intent_id,
            error=(
                "Uniswap V4 swaps are blocked pending VIB-1965: the adapter routes through "
                "v4_swap_router (unverified, non-canonical address) instead of the canonical "
                "UniversalRouter. Core V4 contracts (PoolManager, PositionManager) are verified. "
                "Use protocol='uniswap_v3' as a drop-in alternative until the V4 adapter is "
                "rewritten to use the UniversalRouter."
            ),
        )

    def _compile_lp_open_curve(self, intent: LPOpenIntent) -> CompilationResult:
        """Compile LP_OPEN intent for Curve Finance.

        Curve LP positions are fungible (not NFT-based). The pool is specified
        via intent.pool (address or name like "3pool"). Both amount0 and amount1
        are used; for 3-coin pools, only these two coins are deposited (third = 0).

        Pool format: "0xPoolAddress" or pool name like "3pool", "frax_usdc"

        Args:
            intent: LPOpenIntent with pool, amount0, amount1

        Returns:
            CompilationResult with Curve add_liquidity ActionBundle
        """
        from almanak.framework.connectors.curve.adapter import (
            CURVE_ADDRESSES,
            CURVE_POOLS,
            CurveAdapter,
            CurveConfig,
        )

        result = CompilationResult(
            status=CompilationStatus.SUCCESS,
            intent_id=intent.intent_id,
        )
        transactions: list[Any] = []

        try:
            # Check chain support
            if self.chain not in CURVE_ADDRESSES:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"Curve is not supported on {self.chain}. Supported chains: {list(CURVE_ADDRESSES.keys())}",
                    intent_id=intent.intent_id,
                )

            chain_pools = CURVE_POOLS.get(self.chain, {})

            # Resolve pool: by address or by name
            pool_name: str = ""
            pool_address: str = intent.pool
            pool_data: dict[str, Any] | None = None

            # Check by name first (e.g., "3pool", "frax_usdc")
            if intent.pool in chain_pools:
                pool_name = intent.pool
                pool_data = chain_pools[intent.pool]
                pool_address = pool_data["address"]
            else:
                # Check by address
                for name, data in chain_pools.items():
                    if data["address"].lower() == intent.pool.lower():
                        pool_name = name
                        pool_data = data
                        pool_address = data["address"]
                        break

            if pool_data is None:
                available = {name: d["address"] for name, d in chain_pools.items()}
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=(f"Unknown Curve pool: {intent.pool} on {self.chain}. Available pools: {available}"),
                    intent_id=intent.intent_id,
                )

            n_coins = pool_data["n_coins"]

            # Build amounts list padded to n_coins (amount0, amount1, then 0s for remaining)
            amounts: list[Decimal] = [intent.amount0, intent.amount1]
            while len(amounts) < n_coins:
                amounts.append(Decimal("0"))

            slippage_bps = 50  # Default 0.5% for LP

            logger.info(
                "Compiling Curve LP_OPEN: pool=%s (%s), amounts=%s",
                pool_name,
                self.chain,
                amounts,
            )

            config = CurveConfig(
                chain=self.chain,
                wallet_address=self.wallet_address,
                default_slippage_bps=slippage_bps,
                rpc_url=self._get_chain_rpc_url(),
            )
            adapter = CurveAdapter(config)

            liq_result = adapter.add_liquidity(
                pool_address=pool_address,
                amounts=amounts,
                slippage_bps=slippage_bps,
            )

            if not liq_result.success:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=liq_result.error or "Curve add_liquidity failed",
                    intent_id=intent.intent_id,
                )

            transactions = liq_result.transactions
            total_gas = sum(tx.gas_estimate for tx in transactions)

            action_bundle = ActionBundle(
                intent_type=IntentType.LP_OPEN.value,
                transactions=[tx.to_dict() for tx in transactions],
                metadata={
                    "pool_address": pool_address,
                    "pool_name": pool_name,
                    "amounts": [str(a) for a in amounts],
                    "n_coins": n_coins,
                    "lp_token": pool_data["lp_token"],
                    "protocol": "curve",
                },
            )

            result.action_bundle = action_bundle
            result.transactions = transactions  # type: ignore[assignment]
            result.total_gas_estimate = total_gas

            logger.info(
                "Compiled Curve LP_OPEN intent: pool=%s, %d txs, %d gas",
                pool_name,
                len(transactions),
                total_gas,
            )

        except Exception as e:
            logger.exception("Failed to compile Curve LP_OPEN intent")
            result.status = CompilationStatus.FAILED
            result.error = str(e)

        return result

    def _compile_lp_close_curve(self, intent: LPCloseIntent) -> CompilationResult:
        """Compile LP_CLOSE intent for Curve Finance.

        Burns LP tokens in exchange for underlying tokens (proportional removal).
        LP token amount is passed via intent.position_id (as a decimal string).

        intent.pool: Curve pool address or name
        intent.position_id: LP token amount to burn (e.g., "100.5")

        Args:
            intent: LPCloseIntent with pool and position_id (LP amount)

        Returns:
            CompilationResult with Curve remove_liquidity ActionBundle
        """
        from almanak.framework.connectors.curve.adapter import (
            CURVE_ADDRESSES,
            CURVE_POOLS,
            CurveAdapter,
            CurveConfig,
        )

        result = CompilationResult(
            status=CompilationStatus.SUCCESS,
            intent_id=intent.intent_id,
        )
        transactions: list[Any] = []

        try:
            # Check chain support
            if self.chain not in CURVE_ADDRESSES:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"Curve is not supported on {self.chain}. Supported chains: {list(CURVE_ADDRESSES.keys())}",
                    intent_id=intent.intent_id,
                )

            if not intent.pool:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error="intent.pool must be set to the Curve pool address for LP_CLOSE",
                    intent_id=intent.intent_id,
                )

            chain_pools = CURVE_POOLS.get(self.chain, {})

            # Resolve pool: by name or address
            pool_name: str = ""
            pool_address: str = intent.pool
            pool_data: dict[str, Any] | None = None

            if intent.pool in chain_pools:
                pool_name = intent.pool
                pool_data = chain_pools[intent.pool]
                pool_address = pool_data["address"]
            else:
                for name, data in chain_pools.items():
                    if data["address"].lower() == intent.pool.lower():
                        pool_name = name
                        pool_data = data
                        pool_address = data["address"]
                        break

            if pool_data is None:
                available = {name: d["address"] for name, d in chain_pools.items()}
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=(f"Unknown Curve pool: {intent.pool} on {self.chain}. Available pools: {available}"),
                    intent_id=intent.intent_id,
                )

            # Parse LP token amount from position_id.
            # position_id can be:
            #   - An LP token address (0x...) — query on-chain balance and withdraw all
            #   - An LP token amount as decimal string (e.g., "100.5") — legacy, withdraw that amount
            lp_token_for_pool = pool_data.get("lp_token", "")
            if not lp_token_for_pool:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=(
                        f"Pool config for '{pool_name or pool_address}' is missing 'lp_token' field. "
                        f"Cannot compile Curve LP_CLOSE safely."
                    ),
                    intent_id=intent.intent_id,
                )

            position_id_str = str(intent.position_id).strip()
            if position_id_str.startswith("0x") and len(position_id_str) == 42:
                # Position ID is an LP token address — withdraw full balance
                lp_token_address = position_id_str
                if lp_token_address.lower() != lp_token_for_pool.lower():
                    return CompilationResult(
                        status=CompilationStatus.FAILED,
                        error=(
                            f"position_id LP token {lp_token_address} does not match "
                            f"pool '{pool_name}' LP token {lp_token_for_pool}. "
                            f"Refusing to proceed — this would close the wrong position."
                        ),
                        intent_id=intent.intent_id,
                    )
                # Query on-chain LP token balance via shared helper
                raw_balance = self._query_erc20_balance(lp_token_for_pool, self.wallet_address)
                if raw_balance is None:
                    return CompilationResult(
                        status=CompilationStatus.FAILED,
                        error=(
                            f"Failed to query LP token balance for {pool_name or pool_address} "
                            f"({lp_token_for_pool}). Ensure gateway_client or rpc_url is configured."
                        ),
                        intent_id=intent.intent_id,
                    )
                if raw_balance == 0:
                    return CompilationResult(
                        status=CompilationStatus.FAILED,
                        error=f"Wallet has zero LP token balance for {pool_name} ({lp_token_for_pool})",
                        intent_id=intent.intent_id,
                    )
                lp_token_info = self._resolve_token(lp_token_for_pool)
                if not lp_token_info:
                    return CompilationResult(
                        status=CompilationStatus.FAILED,
                        error=(
                            f"Could not resolve decimals for Curve LP token {lp_token_for_pool}. "
                            f"Cannot safely compute withdrawal amount without known decimals."
                        ),
                        intent_id=intent.intent_id,
                    )
                lp_amount = Decimal(raw_balance) / Decimal(10**lp_token_info.decimals)
                logger.info("Queried on-chain LP balance for %s: %s", pool_name, lp_amount)
            else:
                # Legacy: position_id is LP token amount as decimal string
                try:
                    lp_amount = Decimal(position_id_str)
                except (InvalidOperation, TypeError, ValueError):
                    return CompilationResult(
                        status=CompilationStatus.FAILED,
                        error=(
                            f"Invalid position_id for Curve LP_CLOSE: '{intent.position_id}'. "
                            f"Must be an LP token address (0x...) or LP token amount as decimal string (e.g., '100.5')."
                        ),
                        intent_id=intent.intent_id,
                    )

            slippage_bps = 50  # Default 0.5%

            logger.info(
                "Compiling Curve LP_CLOSE: pool=%s (%s), lp_amount=%s",
                pool_name,
                self.chain,
                lp_amount,
            )

            config = CurveConfig(
                chain=self.chain,
                wallet_address=self.wallet_address,
                default_slippage_bps=slippage_bps,
                rpc_url=self._get_chain_rpc_url(),
            )
            adapter = CurveAdapter(config)

            liq_result = adapter.remove_liquidity(
                pool_address=pool_address,
                lp_amount=lp_amount,
                slippage_bps=slippage_bps,
            )

            if not liq_result.success:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=liq_result.error or "Curve remove_liquidity failed",
                    intent_id=intent.intent_id,
                )

            transactions = liq_result.transactions
            total_gas = sum(tx.gas_estimate for tx in transactions)

            action_bundle = ActionBundle(
                intent_type=IntentType.LP_CLOSE.value,
                transactions=[tx.to_dict() for tx in transactions],
                metadata={
                    "pool_address": pool_address,
                    "pool_name": pool_name,
                    "lp_amount": str(lp_amount),
                    "lp_token": pool_data["lp_token"],
                    "protocol": "curve",
                },
            )

            result.action_bundle = action_bundle
            result.transactions = transactions  # type: ignore[assignment]
            result.total_gas_estimate = total_gas

            logger.info(
                "Compiled Curve LP_CLOSE intent: pool=%s, %d txs, %d gas",
                pool_name,
                len(transactions),
                total_gas,
            )

        except Exception as e:
            logger.exception("Failed to compile Curve LP_CLOSE intent")
            result.status = CompilationStatus.FAILED
            result.error = str(e)

        return result

    def _compile_pendle_swap(self, intent: SwapIntent) -> CompilationResult:
        """Compile SWAP intent for Pendle Protocol (yield tokenization).

        Pendle enables swapping tokens to PT (Principal Tokens) and YT (Yield Tokens).
        PT tokens trade at a discount before maturity and can be redeemed 1:1 for the
        underlying at maturity.

        Args:
            intent: SwapIntent with from_token, to_token, and amount.
                    to_token should be a PT token like "PT-wstETH"

        Returns:
            CompilationResult with Pendle swap ActionBundle
        """
        from almanak.framework.connectors.pendle import PendleAdapter, PendleSwapParams
        from almanak.framework.connectors.pendle.sdk import (
            MARKET_BY_PT_TOKEN,
            MARKET_BY_YT_TOKEN,
            MARKET_TOKEN_MINT_SY,
            PT_TOKEN_INFO,
            YT_TOKEN_INFO,
        )

        result = CompilationResult(
            status=CompilationStatus.SUCCESS,
            intent_id=intent.intent_id,
        )
        transactions: list[TransactionData] = []

        try:
            # Check chain support
            if self.chain not in ("arbitrum", "ethereum", "plasma"):
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"Pendle is only available on Arbitrum, Ethereum, and Plasma, not {self.chain}",
                    intent_id=intent.intent_id,
                )

            # Pre-detect PT/YT tokens before resolution
            from_token_name = intent.from_token.upper()
            is_from_pt = from_token_name.startswith("PT-")
            is_from_yt = from_token_name.startswith("YT-")

            # Resolve from token - handle PT/YT tokens specially
            from_token = self._resolve_token(intent.from_token)
            if from_token is None and is_from_pt:
                # Try to resolve PT token from Pendle SDK mappings
                pt_info = PT_TOKEN_INFO.get(self.chain, {})
                pt_data = pt_info.get(from_token_name) or pt_info.get(intent.from_token)
                if pt_data:
                    pt_address, pt_decimals = pt_data
                    from_token = TokenInfo(
                        symbol=intent.from_token,
                        address=pt_address,
                        decimals=pt_decimals,
                        is_native=False,
                    )
            elif from_token is None and is_from_yt:
                # Try to resolve YT token from Pendle SDK mappings
                yt_info = YT_TOKEN_INFO.get(self.chain, {})
                yt_data = yt_info.get(from_token_name) or yt_info.get(intent.from_token)
                if yt_data:
                    yt_address, yt_decimals = yt_data
                    from_token = TokenInfo(
                        symbol=intent.from_token,
                        address=yt_address,
                        decimals=yt_decimals,
                        is_native=False,
                    )

            if from_token is None:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"Unknown from_token: {intent.from_token}",
                    intent_id=intent.intent_id,
                )

            # Calculate input amount
            if intent.amount_usd is not None:
                amount_in = self._usd_to_token_amount(intent.amount_usd, from_token)
            elif intent.amount is not None:
                if intent.amount == "all":
                    return CompilationResult(
                        status=CompilationStatus.FAILED,
                        error="amount='all' must be resolved before compilation. Use Intent.set_resolved_amount() to resolve chained amounts.",
                        intent_id=intent.intent_id,
                    )
                amount_decimal: Decimal = intent.amount  # type: ignore[assignment]
                amount_in = int(amount_decimal * Decimal(10**from_token.decimals))
            else:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error="Either amount_usd or amount must be provided",
                    intent_id=intent.intent_id,
                )

            # Get RPC URL
            rpc_url = self._get_chain_rpc_url()
            if not rpc_url:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"RPC URL not available for {self.chain}. Configure gateway client or provide rpc_url.",
                    intent_id=intent.intent_id,
                )

            # Create Pendle adapter
            adapter = PendleAdapter(
                rpc_url=rpc_url,
                chain=self.chain,
                wallet_address=self.wallet_address,
            )

            # Determine swap type based on token names
            # PT-*/YT-* prefix means buying/selling PT or YT tokens
            to_token_name = intent.to_token.upper()
            from_token_name = intent.from_token.upper()

            is_buying_pt = to_token_name.startswith("PT-")
            is_selling_pt = from_token_name.startswith("PT-")
            is_buying_yt = to_token_name.startswith("YT-")
            is_selling_yt = from_token_name.startswith("YT-")

            # Guard against invalid PT/YT→PT/YT swaps
            pendle_token_count = sum([is_buying_pt, is_selling_pt, is_buying_yt, is_selling_yt])
            if pendle_token_count > 1:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error="Pendle swaps do not support direct PT/YT to PT/YT transfers",
                    intent_id=intent.intent_id,
                )

            if is_buying_pt:
                swap_type = "token_to_pt"
                pt_markets = MARKET_BY_PT_TOKEN.get(self.chain, {})
                market = pt_markets.get(to_token_name) or pt_markets.get(to_token_name.upper())
                if not market:
                    return CompilationResult(
                        status=CompilationStatus.FAILED,
                        error=f"No Pendle market found for {to_token_name} on {self.chain}. "
                        f"Available PT tokens: {', '.join(sorted(pt_markets.keys()))}",
                        intent_id=intent.intent_id,
                    )
            elif is_selling_pt:
                swap_type = "pt_to_token"
                pt_markets = MARKET_BY_PT_TOKEN.get(self.chain, {})
                market = pt_markets.get(from_token_name) or pt_markets.get(from_token_name.upper())
                if not market:
                    return CompilationResult(
                        status=CompilationStatus.FAILED,
                        error=f"No Pendle market found for {from_token_name} on {self.chain}. "
                        f"Available PT tokens: {', '.join(sorted(pt_markets.keys()))}",
                        intent_id=intent.intent_id,
                    )
            elif is_buying_yt:
                swap_type = "token_to_yt"
                yt_markets = MARKET_BY_YT_TOKEN.get(self.chain, {})
                market = yt_markets.get(to_token_name) or yt_markets.get(to_token_name.upper())
                if not market:
                    return CompilationResult(
                        status=CompilationStatus.FAILED,
                        error=f"No Pendle market found for {to_token_name} on {self.chain}. "
                        f"Available YT tokens: {', '.join(sorted(yt_markets.keys()))}",
                        intent_id=intent.intent_id,
                    )
            elif is_selling_yt:
                swap_type = "yt_to_token"
                yt_markets = MARKET_BY_YT_TOKEN.get(self.chain, {})
                market = yt_markets.get(from_token_name) or yt_markets.get(from_token_name.upper())
                if not market:
                    return CompilationResult(
                        status=CompilationStatus.FAILED,
                        error=f"No Pendle market found for {from_token_name} on {self.chain}. "
                        f"Available YT tokens: {', '.join(sorted(yt_markets.keys()))}",
                        intent_id=intent.intent_id,
                    )
            else:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error="Pendle swaps require either from_token or to_token to be a PT or YT token "
                    "(e.g., PT-wstETH, YT-wstETH)",
                    intent_id=intent.intent_id,
                )

            slippage_bps = int(intent.max_slippage * Decimal("10000"))

            # The Pendle SDK methods apply slippage_bps internally on top of min_amount_out.
            # Previously this line also reduced by slippage, causing double-count (VIB-576).
            #
            # For BUY directions (token_to_pt, token_to_yt): PT/YT is cheaper than the
            # underlying, so output >= input. A 1:1 estimate is a safe conservative minimum.
            #
            # For SELL directions (pt_to_token, yt_to_token): PT/YT trades at a DISCOUNT
            # to the underlying (depends on implied yield + time to maturity). Output < input.
            # A 1:1 estimate causes INSUFFICIENT_TOKEN_OUT reverts (VIB-1366).
            #
            # PT holds most of the underlying's value (typically 90-99%), so a 50% haircut
            # is a safe floor even for long-dated maturities.
            # YT represents only the remaining yield and can approach zero near expiry,
            # so we use a 1% floor to avoid reverts on near-maturity YT sells.
            if swap_type == "yt_to_token":
                min_amount_out = amount_in // 100
                estimation_method = "1% floor (YT near-expiry safe)"
            elif swap_type == "pt_to_token":
                min_amount_out = amount_in // 2
                estimation_method = "50% floor (PT discount safe)"
            else:
                min_amount_out = amount_in
                estimation_method = "1:1 estimate (BUY direction)"

            logger.info(
                f"Pendle slippage params: swap_type={swap_type}, amount_in={amount_in}, "
                f"min_amount_out={min_amount_out}, slippage_bps={slippage_bps}, "
                f"estimation={estimation_method}"
            )

            # Look up the token that mints SY for this market
            # For yield-bearing token markets (like fUSDT0), this is the yield-bearing token
            chain_mint_sy_map = MARKET_TOKEN_MINT_SY.get(self.chain, {})
            token_mint_sy = chain_mint_sy_map.get(market.lower())

            # ================================================================
            # Pre-swap routing: when tokenIn != tokenMintSy
            # ================================================================
            # When the input token differs from the token that mints SY
            # (e.g., WETH as input but wstETH mints SY), the Pendle router
            # cannot route internally. We insert a Uniswap V3 pre-swap step
            # to convert tokenIn -> tokenMintSy before calling Pendle.
            if token_mint_sy and (is_buying_pt or is_buying_yt) and from_token.address.lower() != token_mint_sy.lower():
                # Resolve tokenMintSy to get its symbol and decimals
                mint_sy_token = self._resolve_token(token_mint_sy)
                if mint_sy_token is None:
                    return CompilationResult(
                        status=CompilationStatus.FAILED,
                        error=f"Cannot resolve tokenMintSy address {token_mint_sy} for pre-swap routing on {self.chain}. "
                        f"Use the SY-minting token directly as from_token instead.",
                        intent_id=intent.intent_id,
                    )

                # Check if a V3-compatible DEX is available on this chain for the pre-swap.
                # Prefer the chain's default protocol (may be a V3 fork like Agni Finance).
                from almanak.framework.connectors.protocol_aliases import display_protocol, is_uniswap_v3_fork

                chain_routers = PROTOCOL_ROUTERS.get(self.chain, {})
                v3_pre_swap_protocol = None
                v3_pre_swap_router = None
                # Prefer self.default_protocol if it's a V3 fork on this chain
                if self.default_protocol in chain_routers and is_uniswap_v3_fork(self.default_protocol):
                    v3_pre_swap_protocol = self.default_protocol
                    v3_pre_swap_router = chain_routers[self.default_protocol]
                else:
                    for proto_key, router_addr in chain_routers.items():
                        if is_uniswap_v3_fork(proto_key):
                            v3_pre_swap_protocol = proto_key
                            v3_pre_swap_router = router_addr
                            break
                if not v3_pre_swap_router or not v3_pre_swap_protocol:
                    return CompilationResult(
                        status=CompilationStatus.FAILED,
                        error=f"Pre-swap routing from {from_token.symbol} to {mint_sy_token.symbol} requires "
                        f"a V3-compatible DEX, but none is configured for {self.chain}. "
                        f"Use {mint_sy_token.symbol} directly as from_token instead.",
                        intent_id=intent.intent_id,
                    )

                # Estimate the pre-swap output using price oracle
                try:
                    estimated_mint_sy_output = self._calculate_expected_output(amount_in, from_token, mint_sy_token)
                except (ValueError, KeyError, ZeroDivisionError):
                    return CompilationResult(
                        status=CompilationStatus.FAILED,
                        error=f"Cannot estimate pre-swap output for {from_token.symbol} -> {mint_sy_token.symbol}. "
                        f"Price data unavailable. Use {mint_sy_token.symbol} directly as from_token.",
                        intent_id=intent.intent_id,
                    )

                # Apply 2% safety buffer on the estimated output for the Pendle step.
                # This ensures the Pendle transaction doesn't try to spend more
                # tokenMintSy than the pre-swap actually produces.
                pre_swap_buffer = Decimal("0.98")
                buffered_mint_sy_amount = int(Decimal(str(estimated_mint_sy_output)) * pre_swap_buffer)

                # Handle native ETH: the SwapRouter02 accepts msg.value for native swaps
                actual_from_address = from_token.address
                pre_swap_value = 0
                if from_token.is_native:
                    pre_swap_value = amount_in
                    weth_address = self._get_wrapped_native_address()
                    if not weth_address:
                        return CompilationResult(
                            status=CompilationStatus.FAILED,
                            error=f"Cannot resolve wrapped native token address for {self.chain}. "
                            f"Native ETH pre-swap routing requires a configured wrapped native address.",
                            intent_id=intent.intent_id,
                        )
                    actual_from_address = weth_address

                # Build approval for V3 DEX router (skip for native token)
                if not from_token.is_native:
                    approve_txs = self._build_approve_tx(
                        from_token.address,
                        v3_pre_swap_router,
                        amount_in,
                    )
                    transactions.extend(approve_txs)

                # Build pre-swap calldata via V3-compatible DEX
                pre_swap_adapter = DefaultSwapAdapter(
                    chain=self.chain,
                    protocol=v3_pre_swap_protocol,
                    pool_selection_mode=self._config.swap_pool_selection_mode,
                    fixed_fee_tier=self._config.fixed_swap_fee_tier,
                    rpc_url=self._get_chain_rpc_url(),
                    rpc_timeout=self.rpc_timeout,
                )

                pre_swap_min_out = int(Decimal(str(estimated_mint_sy_output)) * (Decimal("1") - intent.max_slippage))
                # Cap Pendle input to the guaranteed pre-swap minimum.
                # When max_slippage > 2%, the V3 DEX swap may legally return
                # less than the 2%-buffered estimate, so the Pendle step must
                # not try to spend more than the swap guarantees.
                buffered_mint_sy_amount = min(buffered_mint_sy_amount, pre_swap_min_out)

                if buffered_mint_sy_amount <= 0:
                    return CompilationResult(
                        status=CompilationStatus.FAILED,
                        error=f"Pre-swap routing failed: computed Pendle input amount is {buffered_mint_sy_amount} "
                        f"(max_slippage={intent.max_slippage} too high for pre-swap path). "
                        f"Use {mint_sy_token.symbol} directly as from_token or reduce max_slippage.",
                        intent_id=intent.intent_id,
                    )

                deadline = int(datetime.now(UTC).timestamp()) + self.default_deadline_seconds

                pre_swap_calldata = pre_swap_adapter.get_swap_calldata(
                    from_token=actual_from_address,
                    to_token=token_mint_sy,
                    amount_in=amount_in,
                    min_amount_out=pre_swap_min_out,
                    recipient=self.wallet_address,
                    deadline=deadline,
                )

                pre_swap_tx = TransactionData(
                    to=v3_pre_swap_router,
                    value=pre_swap_value,
                    data="0x" + pre_swap_calldata.hex(),
                    gas_estimate=200_000,
                    description=f"Pre-swap: {from_token.symbol} -> {mint_sy_token.symbol} via {display_protocol(self.chain, v3_pre_swap_protocol)}",
                    tx_type="swap",
                )
                transactions.append(pre_swap_tx)

                logger.info(
                    f"Pendle pre-swap routing: {from_token.symbol} -> {mint_sy_token.symbol} -> {intent.to_token}, "
                    f"estimated output={estimated_mint_sy_output}, using {buffered_mint_sy_amount} "
                    f"(capped to min of 2% buffer and {intent.max_slippage:.1%} slippage floor)"
                )

                # Override from_token and amount for the Pendle step
                from_token = mint_sy_token
                amount_in = buffered_mint_sy_amount
                token_mint_sy = None  # tokenIn now equals tokenMintSy
                # Don't apply slippage here -- the SDK applies it internally (VIB-576).
                # For sell directions, use discounted estimate (VIB-1366).
                if swap_type == "yt_to_token":
                    min_amount_out = amount_in // 100
                    estimation_method = "1% floor (YT near-expiry safe, post-pre-swap)"
                elif swap_type == "pt_to_token":
                    min_amount_out = amount_in // 2
                    estimation_method = "50% floor (PT discount safe, post-pre-swap)"
                else:
                    min_amount_out = amount_in
                    estimation_method = "1:1 estimate (BUY direction, post-pre-swap)"

                logger.info(
                    f"Pendle slippage params (post-pre-swap): swap_type={swap_type}, amount_in={amount_in}, "
                    f"min_amount_out={min_amount_out}, slippage_bps={slippage_bps}, "
                    f"estimation={estimation_method}"
                )

            # Resolve token_out to an address
            # For buying PT/YT, token_out is the PT/YT (use PT_TOKEN_INFO/YT_TOKEN_INFO)
            # For selling PT/YT, token_out is the underlying token (use _resolve_token)
            to_token_name_upper = intent.to_token.upper()
            if to_token_name_upper.startswith("PT-"):
                # Buying PT - resolve PT address
                pt_info = PT_TOKEN_INFO.get(self.chain, {})
                pt_data = pt_info.get(to_token_name_upper) or pt_info.get(intent.to_token)
                if pt_data:
                    token_out_address = pt_data[0]
                else:
                    return CompilationResult(
                        status=CompilationStatus.FAILED,
                        error=f"Cannot resolve PT token '{intent.to_token}' - not found in PT_TOKEN_INFO for chain {self.chain}",
                        intent_id=intent.intent_id,
                    )
            elif to_token_name_upper.startswith("YT-"):
                # Buying YT - resolve YT address
                yt_info = YT_TOKEN_INFO.get(self.chain, {})
                yt_data = yt_info.get(to_token_name_upper) or yt_info.get(intent.to_token)
                if yt_data:
                    token_out_address = yt_data[0]
                else:
                    return CompilationResult(
                        status=CompilationStatus.FAILED,
                        error=f"Cannot resolve YT token '{intent.to_token}' - not found in YT_TOKEN_INFO for chain {self.chain}",
                        intent_id=intent.intent_id,
                    )
            else:
                # Selling PT/YT - resolve underlying token address
                to_token = self._resolve_token(intent.to_token)
                if to_token is None:
                    return CompilationResult(
                        status=CompilationStatus.FAILED,
                        error=f"Cannot resolve output token '{intent.to_token}' - token not found in registry for chain {self.chain}",
                        intent_id=intent.intent_id,
                    )
                token_out_address = to_token.address

            # Build swap parameters
            params = PendleSwapParams(
                market=market,
                token_in=from_token.address,
                token_out=token_out_address,
                amount_in=amount_in,
                min_amount_out=min_amount_out,
                receiver=self.wallet_address,
                swap_type=swap_type,
                slippage_bps=slippage_bps,
                token_mint_sy=token_mint_sy,
            )

            logger.info(
                f"Compiling Pendle SWAP: {from_token.symbol} -> {intent.to_token}, "
                f"amount={amount_in}, market={market[:10]}..."
            )

            # Build approval transaction if needed
            router_address = adapter.get_router_address()
            if not from_token.is_native:
                approve_txs = self._build_approve_tx(
                    from_token.address,
                    router_address,
                    amount_in,
                )
                transactions.extend(approve_txs)

            # Build swap transaction using adapter
            tx_data = adapter.build_swap(params)

            swap_tx = TransactionData(
                to=tx_data.to,
                value=tx_data.value,
                data=tx_data.data,
                gas_estimate=tx_data.gas_estimate,
                description=tx_data.description,
                tx_type="swap",
            )
            transactions.append(swap_tx)

            total_gas = sum(tx.gas_estimate for tx in transactions)

            action_bundle = ActionBundle(
                intent_type=IntentType.SWAP.value,
                transactions=[tx.to_dict() for tx in transactions],
                metadata={
                    "from_token": from_token.to_dict(),
                    "to_token": intent.to_token,
                    "amount_in": str(amount_in),
                    "min_amount_out": str(min_amount_out),
                    "slippage": str(intent.max_slippage),
                    "protocol": "pendle",
                    "market": market,
                    "swap_type": swap_type,
                },
            )

            result.action_bundle = action_bundle
            result.transactions = transactions
            result.total_gas_estimate = total_gas

            logger.info(
                f"Compiled Pendle SWAP intent: {from_token.symbol} -> {intent.to_token}, "
                f"{len(transactions)} txs, {total_gas} gas"
            )

        except Exception:
            logger.exception("Failed to compile Pendle SWAP intent")
            result.status = CompilationStatus.FAILED
            result.error = "Pendle SWAP compilation failed"

        return result

    def _compile_pendle_lp_open(self, intent: LPOpenIntent) -> CompilationResult:
        """Compile LP_OPEN intent for Pendle Protocol (single-token liquidity).

        Adds liquidity to a Pendle market using a single input token.
        The router handles splitting into SY and PT.

        Args:
            intent: LPOpenIntent with pool (market address), token, and amount

        Returns:
            CompilationResult with Pendle LP open ActionBundle
        """
        from almanak.framework.connectors.pendle import PendleAdapter
        from almanak.framework.connectors.pendle.sdk import MARKET_BY_PT_TOKEN

        result = CompilationResult(
            status=CompilationStatus.SUCCESS,
            intent_id=intent.intent_id,
        )
        transactions: list[TransactionData] = []

        try:
            if self.chain not in ("arbitrum", "ethereum", "plasma"):
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"Pendle LP not available on {self.chain}",
                    intent_id=intent.intent_id,
                )

            # Pool format for Pendle: "TOKEN/0xmarket_address" or "TOKEN/PT-name"
            # Parse token symbol and market from pool field
            pool_str = intent.pool or ""
            if "/" in pool_str:
                parts = pool_str.split("/", 1)
                token_symbol = parts[0].strip()
                market_part = parts[1].strip()
            elif pool_str.startswith("0x"):
                # Bare market address -- no token specified
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"Pendle LP pool must be 'TOKEN/0xmarket_address' format. Got: {pool_str}",
                    intent_id=intent.intent_id,
                )
            else:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"Invalid Pendle pool format: {pool_str}. Expected: TOKEN/0xmarket_address",
                    intent_id=intent.intent_id,
                )

            token = self._resolve_token(token_symbol)
            if token is None:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"Unknown token: {token_symbol}",
                    intent_id=intent.intent_id,
                )

            # Resolve market address
            market = market_part
            if not market.startswith("0x"):
                pt_markets = MARKET_BY_PT_TOKEN.get(self.chain, {})
                found_market = pt_markets.get(market, None)
                if found_market:
                    market = found_market
                else:
                    return CompilationResult(
                        status=CompilationStatus.FAILED,
                        error=f"Invalid Pendle market: {market}. Must be a 0x address or known PT token name.",
                        intent_id=intent.intent_id,
                    )

            # Use amount0 as deposit amount (single-sided LP)
            amount_decimal: Decimal = intent.amount0
            if amount_decimal <= 0:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error="amount0 must be positive for Pendle LP",
                    intent_id=intent.intent_id,
                )
            amount_in = int(amount_decimal * Decimal(10**token.decimals))

            # Default slippage (LPOpenIntent has no max_slippage field)
            slippage_bps = 50
            min_lp_out = 0  # Pendle LP minting: use adapter to estimate proper min

            rpc_url = self._get_chain_rpc_url()
            if not rpc_url:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"RPC URL not available for {self.chain}",
                    intent_id=intent.intent_id,
                )

            adapter = PendleAdapter(rpc_url=rpc_url, chain=self.chain, wallet_address=self.wallet_address)

            # Build approval
            router_address = adapter.get_router_address()
            if not token.is_native:
                approve_txs = self._build_approve_tx(token.address, router_address, amount_in)
                transactions.extend(approve_txs)

            # Build add liquidity TX
            from almanak.framework.connectors.pendle import PendleLPParams

            lp_params = PendleLPParams(
                market=market,
                token=token.address,
                amount=amount_in,
                min_amount=min_lp_out,
                receiver=self.wallet_address,
                operation="add",
                slippage_bps=slippage_bps,
            )
            tx_data = adapter.build_add_liquidity(lp_params)

            lp_tx = TransactionData(
                to=tx_data.to,
                value=tx_data.value,
                data=tx_data.data,
                gas_estimate=tx_data.gas_estimate,
                description=tx_data.description,
                tx_type="lp_open",
            )
            transactions.append(lp_tx)

            total_gas = sum(tx.gas_estimate for tx in transactions)
            action_bundle = ActionBundle(
                intent_type=IntentType.LP_OPEN.value,
                transactions=[tx.to_dict() for tx in transactions],
                metadata={
                    "protocol": "pendle",
                    "market": market,
                    "token": token.to_dict(),
                    "amount_in": str(amount_in),
                    "min_lp_out": str(min_lp_out),
                    "chain": self.chain,
                },
            )

            result.action_bundle = action_bundle
            result.transactions = transactions
            result.total_gas_estimate = total_gas

            logger.info(f"Compiled Pendle LP_OPEN: {token.symbol} -> market {market[:10]}..., {len(transactions)} txs")

        except Exception:
            logger.exception("Failed to compile Pendle LP_OPEN intent")
            result.status = CompilationStatus.FAILED
            result.error = "Pendle LP_OPEN compilation failed"

        return result

    def _compile_pendle_lp_close(self, intent: LPCloseIntent) -> CompilationResult:
        """Compile LP_CLOSE intent for Pendle Protocol.

        Removes liquidity from a Pendle market to a single output token.

        Args:
            intent: LPCloseIntent with pool (market address), position_id (LP amount), token

        Returns:
            CompilationResult with Pendle LP close ActionBundle
        """
        from almanak.framework.connectors.pendle import PendleAdapter, PendleLPParams

        result = CompilationResult(
            status=CompilationStatus.SUCCESS,
            intent_id=intent.intent_id,
        )
        transactions: list[TransactionData] = []

        try:
            if self.chain not in ("arbitrum", "ethereum", "plasma"):
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"Pendle LP not available on {self.chain}",
                    intent_id=intent.intent_id,
                )

            # Resolve output token (LPCloseIntent has no dedicated token field)
            out_token_name: str = getattr(intent, "token_a", None) or getattr(intent, "token", None) or ""
            if not out_token_name:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error="Pendle LP close requires an output token. Specify via intent metadata.",
                    intent_id=intent.intent_id,
                )
            out_token = self._resolve_token(out_token_name)
            if out_token is None:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"Unknown output token: {out_token_name}",
                    intent_id=intent.intent_id,
                )

            market = intent.pool
            if not market or not market.startswith("0x"):
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"Invalid Pendle market address: {intent.pool}",
                    intent_id=intent.intent_id,
                )

            # LP amount comes from position_id (the LP token amount in wei)
            try:
                lp_amount = int(intent.position_id)
            except (ValueError, TypeError):
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"Invalid LP amount (position_id): {intent.position_id}. Must be LP token amount in wei.",
                    intent_id=intent.intent_id,
                )

            # Default slippage (LPCloseIntent has no max_slippage field)
            slippage_bps = 50
            min_token_out = 0  # Pendle LP removal: use adapter to estimate proper min

            rpc_url = self._get_chain_rpc_url()
            if not rpc_url:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"RPC URL not available for {self.chain}",
                    intent_id=intent.intent_id,
                )

            adapter = PendleAdapter(rpc_url=rpc_url, chain=self.chain, wallet_address=self.wallet_address)

            # Build approval for LP token (market address IS the LP token)
            approve_txs = self._build_approve_tx(market, adapter.get_router_address(), lp_amount)
            transactions.extend(approve_txs)

            # Build remove liquidity TX
            lp_params = PendleLPParams(
                market=market,
                token=out_token.address,
                amount=lp_amount,
                min_amount=min_token_out,
                receiver=self.wallet_address,
                operation="remove",
                slippage_bps=slippage_bps,
            )
            tx_data = adapter.build_remove_liquidity(lp_params)

            remove_tx = TransactionData(
                to=tx_data.to,
                value=tx_data.value,
                data=tx_data.data,
                gas_estimate=tx_data.gas_estimate,
                description=tx_data.description,
                tx_type="lp_close",
            )
            transactions.append(remove_tx)

            total_gas = sum(tx.gas_estimate for tx in transactions)
            action_bundle = ActionBundle(
                intent_type=IntentType.LP_CLOSE.value,
                transactions=[tx.to_dict() for tx in transactions],
                metadata={
                    "protocol": "pendle",
                    "market": market,
                    "out_token": out_token.to_dict(),
                    "lp_amount": str(lp_amount),
                    "min_token_out": str(min_token_out),
                    "chain": self.chain,
                },
            )

            result.action_bundle = action_bundle
            result.transactions = transactions
            result.total_gas_estimate = total_gas

            logger.info(f"Compiled Pendle LP_CLOSE: market {market[:10]}..., {len(transactions)} txs")

        except Exception:
            logger.exception("Failed to compile Pendle LP_CLOSE intent")
            result.status = CompilationStatus.FAILED
            result.error = "Pendle LP_CLOSE compilation failed"

        return result

    def _compile_pendle_redeem(self, intent: WithdrawIntent) -> CompilationResult:
        """Compile WITHDRAW intent as Pendle PT+YT redemption.

        Redeems PT+YT to the underlying token via Pendle's redeemPyToToken.

        Args:
            intent: WithdrawIntent with token (underlying), amount, and optionally market_id (YT address)

        Returns:
            CompilationResult with Pendle redeem ActionBundle
        """
        from almanak.framework.connectors.pendle import PendleAdapter, PendleRedeemParams

        result = CompilationResult(
            status=CompilationStatus.SUCCESS,
            intent_id=intent.intent_id,
        )
        transactions: list[TransactionData] = []

        try:
            if self.chain not in ("arbitrum", "ethereum", "plasma"):
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"Pendle redeem not available on {self.chain}",
                    intent_id=intent.intent_id,
                )

            # Resolve output token
            out_token = self._resolve_token(intent.token)
            if out_token is None:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"Unknown token: {intent.token}",
                    intent_id=intent.intent_id,
                )

            # YT address comes from market_id field
            yt_address = intent.market_id
            if not yt_address:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error="market_id (YT address) is required for Pendle redeem. Set intent.market_id to the YT contract address.",
                    intent_id=intent.intent_id,
                )

            # Calculate amount
            if intent.amount == "all":
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error="amount='all' must be resolved before compilation for Pendle redeem",
                    intent_id=intent.intent_id,
                )
            amount_decimal: Decimal = intent.amount  # type: ignore[assignment]
            # PT/YT tokens are always 18 decimals on Pendle
            py_decimals = 18
            py_amount = int(amount_decimal * Decimal(10**py_decimals))

            slippage_bps = 50
            min_token_out = 0  # Pendle redeem: use adapter to estimate proper min

            rpc_url = self._get_chain_rpc_url()
            if not rpc_url:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"RPC URL not available for {self.chain}",
                    intent_id=intent.intent_id,
                )

            adapter = PendleAdapter(rpc_url=rpc_url, chain=self.chain, wallet_address=self.wallet_address)

            # Build redeem TX
            redeem_params = PendleRedeemParams(
                yt_address=yt_address,
                py_amount=py_amount,
                token_out=out_token.address,
                min_token_out=min_token_out,
                receiver=self.wallet_address,
                slippage_bps=slippage_bps,
            )
            tx_data = adapter.build_redeem(redeem_params)

            redeem_tx = TransactionData(
                to=tx_data.to,
                value=tx_data.value,
                data=tx_data.data,
                gas_estimate=tx_data.gas_estimate,
                description=tx_data.description,
                tx_type="redeem",
            )
            transactions.append(redeem_tx)

            total_gas = sum(tx.gas_estimate for tx in transactions)
            action_bundle = ActionBundle(
                intent_type=IntentType.WITHDRAW.value,
                transactions=[tx.to_dict() for tx in transactions],
                metadata={
                    "protocol": "pendle",
                    "yt_address": yt_address,
                    "out_token": out_token.to_dict(),
                    "py_amount": str(py_amount),
                    "min_token_out": str(min_token_out),
                    "chain": self.chain,
                },
            )

            result.action_bundle = action_bundle
            result.transactions = transactions
            result.total_gas_estimate = total_gas

            logger.info(f"Compiled Pendle REDEEM: {out_token.symbol}, {len(transactions)} txs")

        except Exception:
            logger.exception("Failed to compile Pendle REDEEM intent")
            result.status = CompilationStatus.FAILED
            result.error = "Pendle REDEEM compilation failed"

        return result

    def _compile_borrow(self, intent: BorrowIntent) -> CompilationResult:
        """Compile a BORROW intent into an ActionBundle.

        This method:
        1. Resolves collateral and borrow token addresses
        2. Converts amounts to wei
        3. Builds approve TX for collateral
        4. Builds supply TX to deposit collateral
        5. Builds borrow TX to borrow tokens

        Args:
            intent: BorrowIntent to compile

        Returns:
            CompilationResult with borrow ActionBundle
        """
        result = CompilationResult(
            status=CompilationStatus.SUCCESS,
            intent_id=intent.intent_id,
        )
        transactions: list[TransactionData] = []
        warnings: list[str] = []

        try:
            protocol_lower = intent.protocol.lower()

            # =================================================================
            # SOLANA LENDING PATH (Kamino / Jupiter Lend)
            # =================================================================
            if protocol_lower == "jupiter_lend":
                if not self._is_solana_chain():
                    return CompilationResult(
                        status=CompilationStatus.FAILED,
                        intent_id=intent.intent_id,
                        error="Protocol 'jupiter_lend' is only available on Solana chains.",
                    )
                return self._compile_jupiter_lend_borrow(intent)
            if protocol_lower == "kamino" or (
                self._is_solana_chain() and protocol_lower not in ("morpho", "morpho_blue", "jupiter_lend")
            ):
                if self._is_solana_chain() and protocol_lower not in ("kamino", ""):
                    return CompilationResult(
                        status=CompilationStatus.FAILED,
                        intent_id=intent.intent_id,
                        error=f"Protocol '{intent.protocol}' is not supported for BORROW on Solana. Supported: kamino, jupiter_lend",
                    )
                return self._compile_kamino_borrow(intent)

            # Step 1: Resolve token addresses (needed for both protocols)
            collateral_token = self._resolve_token(intent.collateral_token)
            borrow_token = self._resolve_token(intent.borrow_token)

            if collateral_token is None:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"Unknown collateral token: {intent.collateral_token}",
                    intent_id=intent.intent_id,
                )
            if borrow_token is None:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"Unknown borrow token: {intent.borrow_token}",
                    intent_id=intent.intent_id,
                )

            # Step 2: Check for chained amount
            if intent.collateral_amount == "all":
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error="collateral_amount='all' must be resolved before compilation. Use Intent.set_resolved_amount() to resolve chained amounts.",
                    intent_id=intent.intent_id,
                )
            collateral_amount_decimal: Decimal = intent.collateral_amount  # type: ignore[assignment]

            # =================================================================
            # MORPHO BLUE PATH
            # =================================================================
            if protocol_lower in ("morpho", "morpho_blue"):
                # Validate market_id is provided
                if not intent.market_id:
                    return CompilationResult(
                        status=CompilationStatus.FAILED,
                        error="market_id is required for Morpho Blue borrow",
                        intent_id=intent.intent_id,
                    )

                # Lazy import to avoid circular import
                from ..connectors.morpho_blue.adapter import MorphoBlueAdapter, MorphoBlueConfig

                # Create Morpho adapter
                morpho_config = MorphoBlueConfig(
                    chain=self.chain,
                    wallet_address=self.wallet_address,
                )
                morpho_adapter = MorphoBlueAdapter(morpho_config)

                # If collateral > 0, first supply collateral
                if collateral_amount_decimal > 0:
                    # Build approve TX for Morpho Blue contract
                    approve_txs = self._build_approve_tx(
                        collateral_token.address,
                        morpho_adapter.morpho_address,
                        int(collateral_amount_decimal * Decimal(10**collateral_token.decimals)),
                    )
                    transactions.extend(approve_txs)

                    # Build supply collateral TX
                    supply_result: Any = morpho_adapter.supply_collateral(
                        market_id=intent.market_id,
                        amount=collateral_amount_decimal,
                        on_behalf_of=self.wallet_address,
                    )

                    if not supply_result.success:
                        return CompilationResult(
                            status=CompilationStatus.FAILED,
                            error=f"Morpho Blue supply collateral failed: {supply_result.error}",
                            intent_id=intent.intent_id,
                        )

                    assert supply_result.tx_data is not None
                    supply_tx = TransactionData(
                        to=supply_result.tx_data["to"],
                        value=supply_result.tx_data["value"],
                        data=supply_result.tx_data["data"],
                        gas_estimate=supply_result.gas_estimate,
                        description=supply_result.description
                        or f"Supply {collateral_amount_decimal} {collateral_token.symbol} as collateral",
                        tx_type="lending_supply_collateral",
                    )
                    transactions.append(supply_tx)
                else:
                    warnings.append("No collateral supplied - borrowing against existing collateral")

                # Build borrow TX
                borrow_result: Any = morpho_adapter.borrow(
                    market_id=intent.market_id,
                    amount=intent.borrow_amount,
                    on_behalf_of=self.wallet_address,
                    receiver=self.wallet_address,
                )

                if not borrow_result.success:
                    return CompilationResult(
                        status=CompilationStatus.FAILED,
                        error=f"Morpho Blue borrow failed: {borrow_result.error}",
                        intent_id=intent.intent_id,
                    )

                assert borrow_result.tx_data is not None
                borrow_tx = TransactionData(
                    to=borrow_result.tx_data["to"],
                    value=borrow_result.tx_data["value"],
                    data=borrow_result.tx_data["data"],
                    gas_estimate=borrow_result.gas_estimate,
                    description=borrow_result.description or f"Borrow {intent.borrow_amount} {borrow_token.symbol}",
                    tx_type="lending_borrow",
                )
                transactions.append(borrow_tx)

                # Build ActionBundle for Morpho
                total_gas = sum(tx.gas_estimate for tx in transactions)
                action_bundle = ActionBundle(
                    intent_type=IntentType.BORROW.value,
                    transactions=[tx.to_dict() for tx in transactions],
                    metadata={
                        "protocol": intent.protocol,
                        "morpho_address": morpho_adapter.morpho_address,
                        "market_id": intent.market_id,
                        "collateral_token": collateral_token.to_dict(),
                        "borrow_token": borrow_token.to_dict(),
                        "collateral_amount": str(collateral_amount_decimal),
                        "borrow_amount": str(intent.borrow_amount),
                        "chain": self.chain,
                    },
                )

                result.action_bundle = action_bundle
                result.transactions = transactions
                result.total_gas_estimate = total_gas
                result.warnings = warnings

                logger.info(
                    f"Compiled BORROW: {collateral_amount_decimal} {collateral_token.symbol} collateral -> {intent.borrow_amount} {borrow_token.symbol} on Morpho Blue"
                )
                return result

            # =================================================================
            # AAVE V3 PATH
            # =================================================================
            elif protocol_lower.startswith("aave"):
                # Get lending adapter
                adapter = AaveV3Adapter(self.chain, "aave_v3")
                pool_address = adapter.get_pool_address()

                if pool_address == "0x0000000000000000000000000000000000000000":
                    return CompilationResult(
                        status=CompilationStatus.FAILED,
                        error=f"Aave V3 not available on chain: {self.chain}",
                        intent_id=intent.intent_id,
                    )

                collateral_amount = int(collateral_amount_decimal * Decimal(10**collateral_token.decimals))
                borrow_amount = int(intent.borrow_amount * Decimal(10**borrow_token.decimals))

                # Build approve TX and supply TX for collateral (if collateral > 0)
                if collateral_amount > 0:
                    actual_collateral_address = collateral_token.address
                    supply_value = 0

                    if collateral_token.is_native:
                        weth_address = self._get_wrapped_native_address()
                        if weth_address:
                            actual_collateral_address = weth_address
                            warnings.append("Native token collateral: will wrap to WETH before supplying")
                        else:
                            return CompilationResult(
                                status=CompilationStatus.FAILED,
                                error="Cannot use native ETH as collateral - WETH address not found",
                                intent_id=intent.intent_id,
                            )

                    if not collateral_token.is_native:
                        approve_txs = self._build_approve_tx(
                            actual_collateral_address,
                            pool_address,
                            collateral_amount,
                        )
                        transactions.extend(approve_txs)

                    supply_calldata = adapter.get_supply_calldata(
                        asset=actual_collateral_address,
                        amount=collateral_amount,
                        on_behalf_of=self.wallet_address,
                    )

                    supply_tx = TransactionData(
                        to=pool_address,
                        value=supply_value,
                        data="0x" + supply_calldata.hex(),
                        gas_estimate=adapter.estimate_supply_gas(),
                        description=(
                            f"Supply {self._format_amount(collateral_amount, collateral_token.decimals)} {collateral_token.symbol} as collateral"
                        ),
                        tx_type="lending_supply",
                    )
                    transactions.append(supply_tx)
                else:
                    warnings.append("No collateral supplied - borrowing against existing collateral")

                # Resolve interest rate mode: use intent value or default to variable
                # Note: stable rate is deprecated on Aave V3, rejected at intent layer
                aave_borrow_rate_mode = AAVE_VARIABLE_RATE_MODE
                borrow_rate_mode_label = "variable"

                # Build borrow TX
                borrow_calldata = adapter.get_borrow_calldata(
                    asset=borrow_token.address,
                    amount=borrow_amount,
                    interest_rate_mode=aave_borrow_rate_mode,
                    on_behalf_of=self.wallet_address,
                )

                borrow_tx = TransactionData(
                    to=pool_address,
                    value=0,
                    data="0x" + borrow_calldata.hex(),
                    gas_estimate=adapter.estimate_borrow_gas(),
                    description=(
                        f"Borrow {self._format_amount(borrow_amount, borrow_token.decimals)} {borrow_token.symbol} ({borrow_rate_mode_label} rate)"
                    ),
                    tx_type="lending_borrow",
                )
                transactions.append(borrow_tx)

                # Build ActionBundle
                total_gas = sum(tx.gas_estimate for tx in transactions)

                action_bundle = ActionBundle(
                    intent_type=IntentType.BORROW.value,
                    transactions=[tx.to_dict() for tx in transactions],
                    metadata={
                        "protocol": intent.protocol,
                        "pool_address": pool_address,
                        "collateral_token": collateral_token.to_dict(),
                        "borrow_token": borrow_token.to_dict(),
                        "collateral_amount": str(collateral_amount),
                        "borrow_amount": str(borrow_amount),
                        "interest_rate_mode": aave_borrow_rate_mode,
                        "chain": self.chain,
                    },
                )

                result.action_bundle = action_bundle
                result.transactions = transactions
                result.total_gas_estimate = total_gas
                result.warnings = warnings

                collateral_fmt = format_token_amount(
                    collateral_amount, collateral_token.symbol, collateral_token.decimals
                )
                borrow_fmt = format_token_amount(borrow_amount, borrow_token.symbol, borrow_token.decimals)

                logger.info(f"Compiled BORROW: Supply {collateral_fmt} (collateral) -> Borrow {borrow_fmt}")
                logger.info(f"   Protocol: {intent.protocol} | Txs: {len(transactions)} | Gas: {total_gas:,}")

            # =================================================================
            # SPARK PATH (Aave V3 fork with Spark-specific addresses)
            # =================================================================
            elif protocol_lower == "spark":
                from ..connectors.spark import (
                    SPARK_POOL_ADDRESSES,
                    SPARK_VARIABLE_RATE_MODE,
                    SparkAdapter,
                    SparkConfig,
                )

                if self.chain not in SPARK_POOL_ADDRESSES:
                    return CompilationResult(
                        status=CompilationStatus.FAILED,
                        error=f"Spark not available on chain: {self.chain}. Supported: {list(SPARK_POOL_ADDRESSES.keys())}",
                        intent_id=intent.intent_id,
                    )

                spark_config = SparkConfig(
                    chain=self.chain,
                    wallet_address=self.wallet_address,
                )
                spark_adapter = SparkAdapter(spark_config)
                pool_address = spark_adapter.pool_address

                collateral_amount = int(collateral_amount_decimal * Decimal(10**collateral_token.decimals))
                borrow_amount = int(intent.borrow_amount * Decimal(10**borrow_token.decimals))

                # Build approve TX and supply TX for collateral (if collateral > 0)
                if collateral_amount > 0:
                    actual_collateral_address = collateral_token.address
                    supply_value = 0

                    if collateral_token.is_native:
                        weth_address = self._get_wrapped_native_address()
                        if weth_address:
                            actual_collateral_address = weth_address
                            # Wrap native ETH -> WETH
                            wrap_tx = TransactionData(
                                to=weth_address,
                                value=collateral_amount,
                                data="0xd0e30db0",  # WETH.deposit()
                                gas_estimate=get_gas_estimate(self.chain, "wrap_eth"),
                                description=f"Wrap {self._format_amount(collateral_amount, collateral_token.decimals)} {collateral_token.symbol} to WETH",
                                tx_type="wrap",
                            )
                            transactions.append(wrap_tx)
                            # Approve WETH for pool
                            approve_txs = self._build_approve_tx(
                                weth_address,
                                pool_address,
                                collateral_amount,
                            )
                            transactions.extend(approve_txs)
                            warnings.append("Native token collateral: wrapped to WETH before supplying")
                        else:
                            return CompilationResult(
                                status=CompilationStatus.FAILED,
                                error="Cannot use native ETH as collateral - WETH address not found",
                                intent_id=intent.intent_id,
                            )
                    else:
                        approve_txs = self._build_approve_tx(
                            actual_collateral_address,
                            pool_address,
                            collateral_amount,
                        )
                        transactions.extend(approve_txs)

                    # Build supply TX via Spark adapter
                    supply_result = spark_adapter.supply(
                        asset=actual_collateral_address,
                        amount=collateral_amount_decimal,
                        on_behalf_of=self.wallet_address,
                    )

                    if not supply_result.success:
                        return CompilationResult(
                            status=CompilationStatus.FAILED,
                            error=f"Spark supply collateral failed: {supply_result.error}",
                            intent_id=intent.intent_id,
                        )

                    assert supply_result.tx_data is not None
                    supply_data = supply_result.tx_data["data"]
                    if not supply_data.startswith("0x"):
                        supply_data = "0x" + supply_data

                    supply_value = int(supply_result.tx_data.get("value", 0))

                    supply_tx = TransactionData(
                        to=supply_result.tx_data["to"],
                        value=supply_value,
                        data=supply_data,
                        gas_estimate=supply_result.gas_estimate,
                        description=(
                            f"Supply {self._format_amount(collateral_amount, collateral_token.decimals)} {collateral_token.symbol} as collateral to Spark"
                        ),
                        tx_type="lending_supply",
                    )
                    transactions.append(supply_tx)
                else:
                    warnings.append("No collateral supplied - borrowing against existing collateral")

                # Resolve interest rate mode: use intent value or default to variable
                # Note: stable rate is deprecated on Spark, rejected at intent layer
                spark_borrow_rate_mode = SPARK_VARIABLE_RATE_MODE
                spark_borrow_rate_label = "variable"

                # Build borrow TX via Spark adapter
                borrow_result = spark_adapter.borrow(
                    asset=borrow_token.address,
                    amount=intent.borrow_amount,
                    interest_rate_mode=spark_borrow_rate_mode,
                    on_behalf_of=self.wallet_address,
                )

                if not borrow_result.success:
                    return CompilationResult(
                        status=CompilationStatus.FAILED,
                        error=f"Spark borrow failed: {borrow_result.error}",
                        intent_id=intent.intent_id,
                    )

                assert borrow_result.tx_data is not None
                borrow_data = borrow_result.tx_data["data"]
                if not borrow_data.startswith("0x"):
                    borrow_data = "0x" + borrow_data

                borrow_tx = TransactionData(
                    to=borrow_result.tx_data["to"],
                    value=0,
                    data=borrow_data,
                    gas_estimate=borrow_result.gas_estimate,
                    description=(
                        f"Borrow {self._format_amount(borrow_amount, borrow_token.decimals)} {borrow_token.symbol} from Spark ({spark_borrow_rate_label} rate)"
                    ),
                    tx_type="lending_borrow",
                )
                transactions.append(borrow_tx)

                # Build ActionBundle
                total_gas = sum(tx.gas_estimate for tx in transactions)

                action_bundle = ActionBundle(
                    intent_type=IntentType.BORROW.value,
                    transactions=[tx.to_dict() for tx in transactions],
                    metadata={
                        "protocol": intent.protocol,
                        "pool_address": pool_address,
                        "collateral_token": collateral_token.to_dict(),
                        "borrow_token": borrow_token.to_dict(),
                        "collateral_amount": str(collateral_amount),
                        "borrow_amount": str(borrow_amount),
                        "interest_rate_mode": spark_borrow_rate_mode,
                        "chain": self.chain,
                    },
                )

                result.action_bundle = action_bundle
                result.transactions = transactions
                result.total_gas_estimate = total_gas
                result.warnings = warnings

                collateral_fmt = format_token_amount(
                    collateral_amount, collateral_token.symbol, collateral_token.decimals
                )
                borrow_fmt = format_token_amount(borrow_amount, borrow_token.symbol, borrow_token.decimals)

                logger.info(f"Compiled BORROW: Supply {collateral_fmt} (collateral) -> Borrow {borrow_fmt}")
                logger.info(f"   Protocol: Spark | Txs: {len(transactions)} | Gas: {total_gas:,}")

            # =================================================================
            # COMPOUND V3 PATH
            # =================================================================
            elif protocol_lower == "compound_v3":
                from ..connectors.compound_v3.adapter import (
                    COMPOUND_V3_COMET_ADDRESSES,
                    CompoundV3Adapter,
                    CompoundV3Config,
                )

                market = intent.market_id or "usdc"

                if self.chain not in COMPOUND_V3_COMET_ADDRESSES:
                    return CompilationResult(
                        status=CompilationStatus.FAILED,
                        error=f"Compound V3 not available on chain: {self.chain}. Supported: {list(COMPOUND_V3_COMET_ADDRESSES.keys())}",
                        intent_id=intent.intent_id,
                    )

                available_markets = COMPOUND_V3_COMET_ADDRESSES.get(self.chain, {})
                if market not in available_markets:
                    return CompilationResult(
                        status=CompilationStatus.FAILED,
                        error=f"Compound V3 market '{market}' not available on {self.chain}. Available: {list(available_markets.keys())}",
                        intent_id=intent.intent_id,
                    )

                compound_config = CompoundV3Config(
                    chain=self.chain,
                    wallet_address=self.wallet_address,
                    market=market,
                )
                compound_adapter = CompoundV3Adapter(compound_config)

                # If collateral > 0, first supply collateral
                if collateral_amount_decimal > 0:
                    collateral_amount_wei = int(collateral_amount_decimal * Decimal(10**collateral_token.decimals))

                    # Build approve TX for Comet contract (collateral token)
                    approve_txs = self._build_approve_tx(
                        collateral_token.address,
                        compound_adapter.comet_address,
                        collateral_amount_wei,
                    )
                    transactions.extend(approve_txs)

                    # Build supply collateral TX
                    # Determine collateral symbol for adapter
                    collateral_symbol = collateral_token.symbol.upper()
                    supply_result = compound_adapter.supply_collateral(
                        asset=collateral_symbol,
                        amount=collateral_amount_decimal,
                    )

                    if not supply_result.success:
                        return CompilationResult(
                            status=CompilationStatus.FAILED,
                            error=f"Compound V3 supply collateral failed: {supply_result.error}",
                            intent_id=intent.intent_id,
                        )

                    assert supply_result.tx_data is not None
                    supply_data = supply_result.tx_data["data"]
                    if not supply_data.startswith("0x"):
                        supply_data = "0x" + supply_data

                    supply_tx = TransactionData(
                        to=supply_result.tx_data["to"],
                        value=int(supply_result.tx_data.get("value", 0)),
                        data=supply_data,
                        gas_estimate=supply_result.gas_estimate,
                        description=supply_result.description
                        or f"Supply {collateral_amount_decimal} {collateral_token.symbol} as collateral to Compound V3",
                        tx_type="lending_supply_collateral",
                    )
                    transactions.append(supply_tx)
                else:
                    warnings.append("No collateral supplied - borrowing against existing collateral")

                # Build borrow TX
                borrow_result = compound_adapter.borrow(amount=intent.borrow_amount)

                if not borrow_result.success:
                    return CompilationResult(
                        status=CompilationStatus.FAILED,
                        error=f"Compound V3 borrow failed: {borrow_result.error}",
                        intent_id=intent.intent_id,
                    )

                assert borrow_result.tx_data is not None
                borrow_data = borrow_result.tx_data["data"]
                if not borrow_data.startswith("0x"):
                    borrow_data = "0x" + borrow_data

                borrow_tx = TransactionData(
                    to=borrow_result.tx_data["to"],
                    value=int(borrow_result.tx_data.get("value", 0)),
                    data=borrow_data,
                    gas_estimate=borrow_result.gas_estimate,
                    description=borrow_result.description
                    or f"Borrow {intent.borrow_amount} {borrow_token.symbol} from Compound V3",
                    tx_type="lending_borrow",
                )
                transactions.append(borrow_tx)

                # Build ActionBundle
                total_gas = sum(tx.gas_estimate for tx in transactions)
                action_bundle = ActionBundle(
                    intent_type=IntentType.BORROW.value,
                    transactions=[tx.to_dict() for tx in transactions],
                    metadata={
                        "protocol": intent.protocol,
                        "comet_address": compound_adapter.comet_address,
                        "market": market,
                        "collateral_token": collateral_token.to_dict(),
                        "borrow_token": borrow_token.to_dict(),
                        "collateral_amount": str(collateral_amount_decimal),
                        "borrow_amount": str(intent.borrow_amount),
                        "chain": self.chain,
                    },
                )

                result.action_bundle = action_bundle
                result.transactions = transactions
                result.total_gas_estimate = total_gas
                result.warnings = warnings

                collateral_fmt = format_token_amount(
                    int(collateral_amount_decimal * Decimal(10**collateral_token.decimals)),
                    collateral_token.symbol,
                    collateral_token.decimals,
                )
                borrow_fmt = format_token_amount(
                    int(intent.borrow_amount * Decimal(10**borrow_token.decimals)),
                    borrow_token.symbol,
                    borrow_token.decimals,
                )

                logger.info(f"Compiled BORROW: Supply {collateral_fmt} (collateral) -> Borrow {borrow_fmt}")
                logger.info(
                    f"   Protocol: Compound V3 ({market} market) | Txs: {len(transactions)} | Gas: {total_gas:,}"
                )

            # =================================================================
            # BENQI PATH (Compound V2 fork on Avalanche)
            # =================================================================
            elif protocol_lower == "benqi":
                from ..connectors.benqi.adapter import (
                    BENQI_QI_TOKENS,
                    BenqiAdapter,
                    BenqiConfig,
                )

                if self.chain != "avalanche":
                    return CompilationResult(
                        status=CompilationStatus.FAILED,
                        error=f"BENQI is only available on Avalanche, got: {self.chain}",
                        intent_id=intent.intent_id,
                    )

                benqi_config = BenqiConfig(
                    chain=self.chain,
                    wallet_address=self.wallet_address,
                )
                benqi_adapter = BenqiAdapter(benqi_config)

                # If collateral > 0, first supply collateral + enterMarkets
                if collateral_amount_decimal > 0:
                    collateral_symbol = collateral_token.symbol.upper()
                    collateral_market = benqi_adapter.get_market_info(collateral_symbol)

                    if not collateral_market:
                        return CompilationResult(
                            status=CompilationStatus.FAILED,
                            error=f"BENQI does not support collateral asset: {collateral_symbol}. Supported: {list(BENQI_QI_TOKENS.keys())}",
                            intent_id=intent.intent_id,
                        )

                    collateral_amount_wei = int(collateral_amount_decimal * Decimal(10**collateral_token.decimals))

                    # Build approve TX for qiToken (skip for native AVAX)
                    if not collateral_market.is_native:
                        approve_txs = self._build_approve_tx(
                            collateral_token.address,
                            collateral_market.qi_token_address,
                            collateral_amount_wei,
                        )
                        transactions.extend(approve_txs)

                    # Build supply (mint) TX
                    supply_result = benqi_adapter.supply(
                        asset=collateral_symbol,
                        amount=collateral_amount_decimal,
                    )

                    if not supply_result.success:
                        return CompilationResult(
                            status=CompilationStatus.FAILED,
                            error=f"BENQI supply collateral failed: {supply_result.error}",
                            intent_id=intent.intent_id,
                        )

                    assert supply_result.tx_data is not None
                    supply_data = supply_result.tx_data["data"]
                    if not supply_data.startswith("0x"):
                        supply_data = "0x" + supply_data

                    supply_tx = TransactionData(
                        to=supply_result.tx_data["to"],
                        value=int(supply_result.tx_data.get("value", 0)),
                        data=supply_data,
                        gas_estimate=supply_result.gas_estimate,
                        description=supply_result.description,
                        tx_type="lending_supply_collateral",
                    )
                    transactions.append(supply_tx)

                    # Build enterMarkets TX to enable as collateral
                    enter_result = benqi_adapter.enter_markets([collateral_symbol])
                    if not enter_result.success:
                        return CompilationResult(
                            status=CompilationStatus.FAILED,
                            error=f"BENQI enterMarkets failed: {enter_result.error}",
                            intent_id=intent.intent_id,
                        )
                    assert enter_result.tx_data is not None
                    enter_data = enter_result.tx_data["data"]
                    if not enter_data.startswith("0x"):
                        enter_data = "0x" + enter_data
                    enter_tx = TransactionData(
                        to=enter_result.tx_data["to"],
                        value=0,
                        data=enter_data,
                        gas_estimate=enter_result.gas_estimate,
                        description=enter_result.description,
                        tx_type="lending_enter_markets",
                    )
                    transactions.append(enter_tx)
                else:
                    warnings.append("No collateral supplied - borrowing against existing collateral")

                # Build borrow TX
                borrow_symbol = borrow_token.symbol.upper()
                borrow_result = benqi_adapter.borrow(asset=borrow_symbol, amount=intent.borrow_amount)

                if not borrow_result.success:
                    return CompilationResult(
                        status=CompilationStatus.FAILED,
                        error=f"BENQI borrow failed: {borrow_result.error}",
                        intent_id=intent.intent_id,
                    )

                assert borrow_result.tx_data is not None
                borrow_data = borrow_result.tx_data["data"]
                if not borrow_data.startswith("0x"):
                    borrow_data = "0x" + borrow_data

                borrow_tx = TransactionData(
                    to=borrow_result.tx_data["to"],
                    value=int(borrow_result.tx_data.get("value", 0)),
                    data=borrow_data,
                    gas_estimate=borrow_result.gas_estimate,
                    description=borrow_result.description,
                    tx_type="lending_borrow",
                )
                transactions.append(borrow_tx)

                # Build ActionBundle
                total_gas = sum(tx.gas_estimate for tx in transactions)
                action_bundle = ActionBundle(
                    intent_type=IntentType.BORROW.value,
                    transactions=[tx.to_dict() for tx in transactions],
                    metadata={
                        "protocol": intent.protocol,
                        "comptroller_address": benqi_adapter.comptroller_address,
                        "collateral_token": collateral_token.to_dict(),
                        "borrow_token": borrow_token.to_dict(),
                        "collateral_amount": str(collateral_amount_decimal),
                        "borrow_amount": str(intent.borrow_amount),
                        "chain": self.chain,
                    },
                )

                result.action_bundle = action_bundle
                result.transactions = transactions
                result.total_gas_estimate = total_gas
                result.warnings = warnings

                collateral_fmt = format_token_amount(
                    int(collateral_amount_decimal * Decimal(10**collateral_token.decimals)),
                    collateral_token.symbol,
                    collateral_token.decimals,
                )
                borrow_fmt = format_token_amount(
                    int(intent.borrow_amount * Decimal(10**borrow_token.decimals)),
                    borrow_token.symbol,
                    borrow_token.decimals,
                )

                logger.info(f"Compiled BORROW: Supply {collateral_fmt} (collateral) -> Borrow {borrow_fmt}")
                logger.info(f"   Protocol: BENQI | Txs: {len(transactions)} | Gas: {total_gas:,}")

            # =================================================================
            # UNSUPPORTED PROTOCOL
            # =================================================================
            else:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"Unsupported lending protocol: {intent.protocol}. Supported: aave_v3, morpho, morpho_blue, spark, compound_v3, benqi",
                    intent_id=intent.intent_id,
                )

        except Exception as e:
            logger.exception(f"Failed to compile BORROW intent: {e}")
            result.status = CompilationStatus.FAILED
            result.error = str(e)

        return result

    def _compile_repay(self, intent: RepayIntent) -> CompilationResult:
        """Compile a REPAY intent into an ActionBundle.

        This method:
        1. Resolves repay token address
        2. Converts amount to wei (or uses MAX_UINT256 for full repay)
        3. Builds approve TX for repay token
        4. Builds repay TX

        Args:
            intent: RepayIntent to compile

        Returns:
            CompilationResult with repay ActionBundle
        """
        result = CompilationResult(
            status=CompilationStatus.SUCCESS,
            intent_id=intent.intent_id,
        )
        transactions: list[TransactionData] = []
        warnings: list[str] = []

        try:
            protocol_lower = intent.protocol.lower()

            # =================================================================
            # SOLANA LENDING PATH (Kamino / Jupiter Lend)
            # =================================================================
            if protocol_lower == "jupiter_lend":
                if not self._is_solana_chain():
                    return CompilationResult(
                        status=CompilationStatus.FAILED,
                        intent_id=intent.intent_id,
                        error="Protocol 'jupiter_lend' is only available on Solana chains.",
                    )
                return self._compile_jupiter_lend_repay(intent)
            if protocol_lower == "kamino" or (
                self._is_solana_chain() and protocol_lower not in ("morpho", "morpho_blue", "jupiter_lend")
            ):
                if self._is_solana_chain() and protocol_lower not in ("kamino", ""):
                    return CompilationResult(
                        status=CompilationStatus.FAILED,
                        intent_id=intent.intent_id,
                        error=f"Protocol '{intent.protocol}' is not supported for REPAY on Solana. Supported: kamino, jupiter_lend",
                    )
                return self._compile_kamino_repay(intent)

            # Step 1: Resolve token address
            repay_token = self._resolve_token(intent.token)
            if repay_token is None:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"Unknown repay token: {intent.token}",
                    intent_id=intent.intent_id,
                )

            # Step 2: Calculate repay amount
            repay_amount_decimal: Decimal | None
            if intent.repay_full:
                repay_amount_decimal = None  # Will use shares-based repay for Morpho
                amount_description = "full debt"
                warnings.append("Repaying full debt - ensure sufficient balance to cover interest")
            else:
                if intent.amount == "all":
                    return CompilationResult(
                        status=CompilationStatus.FAILED,
                        error="amount='all' must be resolved before compilation. Use Intent.set_resolved_amount() to resolve chained amounts.",
                        intent_id=intent.intent_id,
                    )
                repay_amount_decimal = intent.amount  # type: ignore[assignment]
                amount_description = str(repay_amount_decimal)

            # =================================================================
            # MORPHO BLUE PATH
            # =================================================================
            if protocol_lower in ("morpho", "morpho_blue"):
                if not intent.market_id:
                    return CompilationResult(
                        status=CompilationStatus.FAILED,
                        error="market_id is required for Morpho Blue repay",
                        intent_id=intent.intent_id,
                    )

                # Lazy import to avoid circular import
                from ..connectors.morpho_blue.adapter import MorphoBlueAdapter, MorphoBlueConfig

                # Get RPC URL from compiler config (set by runtime config for the correct network)
                morpho_rpc_url = self.rpc_url

                morpho_config = MorphoBlueConfig(
                    chain=self.chain,
                    wallet_address=self.wallet_address,
                    rpc_url=morpho_rpc_url,  # Pass RPC URL for on-chain queries (e.g., repay_full)
                )
                morpho_adapter = MorphoBlueAdapter(morpho_config)

                # Build approve TX for Morpho Blue contract
                if repay_amount_decimal is not None:
                    approve_amount = int(repay_amount_decimal * Decimal(10**repay_token.decimals))
                else:
                    approve_amount = MAX_UINT256  # Approve max for full repay

                approve_txs = self._build_approve_tx(
                    repay_token.address,
                    morpho_adapter.morpho_address,
                    approve_amount,
                )
                transactions.extend(approve_txs)

                # Build repay TX
                repay_result: Any = morpho_adapter.repay(
                    market_id=intent.market_id,
                    amount=repay_amount_decimal if repay_amount_decimal else Decimal("0"),
                    on_behalf_of=self.wallet_address,
                    repay_all=intent.repay_full,
                )

                if not repay_result.success:
                    return CompilationResult(
                        status=CompilationStatus.FAILED,
                        error=f"Morpho Blue repay failed: {repay_result.error}",
                        intent_id=intent.intent_id,
                    )

                assert repay_result.tx_data is not None
                repay_tx = TransactionData(
                    to=repay_result.tx_data["to"],
                    value=repay_result.tx_data["value"],
                    data=repay_result.tx_data["data"],
                    gas_estimate=repay_result.gas_estimate,
                    description=repay_result.description or f"Repay {amount_description} {repay_token.symbol}",
                    tx_type="lending_repay",
                )
                transactions.append(repay_tx)

                total_gas = sum(tx.gas_estimate for tx in transactions)
                action_bundle = ActionBundle(
                    intent_type=IntentType.REPAY.value,
                    transactions=[tx.to_dict() for tx in transactions],
                    metadata={
                        "protocol": intent.protocol,
                        "morpho_address": morpho_adapter.morpho_address,
                        "market_id": intent.market_id,
                        "repay_token": repay_token.to_dict(),
                        "repay_amount": amount_description,
                        "repay_full": intent.repay_full,
                        "chain": self.chain,
                    },
                )

                result.action_bundle = action_bundle
                result.transactions = transactions
                result.total_gas_estimate = total_gas
                result.warnings = warnings

                logger.info(f"Compiled REPAY: {amount_description} {repay_token.symbol} on Morpho Blue")
                return result

            # =================================================================
            # AAVE V3 PATH
            # =================================================================
            elif protocol_lower.startswith("aave"):
                adapter = AaveV3Adapter(self.chain, "aave_v3")
                pool_address = adapter.get_pool_address()

                if pool_address == "0x0000000000000000000000000000000000000000":
                    return CompilationResult(
                        status=CompilationStatus.FAILED,
                        error=f"Aave V3 not available on chain: {self.chain}",
                        intent_id=intent.intent_id,
                    )

                if intent.repay_full:
                    repay_amount = MAX_UINT256
                else:
                    assert repay_amount_decimal is not None
                    repay_amount = int(repay_amount_decimal * Decimal(10**repay_token.decimals))

                approve_amount = repay_amount if repay_amount != MAX_UINT256 else MAX_UINT256

                if not repay_token.is_native:
                    approve_txs = self._build_approve_tx(
                        repay_token.address,
                        pool_address,
                        approve_amount,
                    )
                    transactions.extend(approve_txs)
                else:
                    weth_address = self._get_wrapped_native_address()
                    if weth_address:
                        approve_txs = self._build_approve_tx(
                            weth_address,
                            pool_address,
                            approve_amount,
                        )
                        transactions.extend(approve_txs)
                        warnings.append("Native token debt: using WETH for repayment")

                actual_repay_address = repay_token.address
                if repay_token.is_native:
                    weth_address = self._get_wrapped_native_address()
                    if weth_address:
                        actual_repay_address = weth_address

                # Resolve interest rate mode: use intent value or default to variable
                # Note: stable rate is deprecated on Aave V3, rejected at intent layer
                aave_rate_mode = AAVE_VARIABLE_RATE_MODE
                rate_mode_label = "variable"

                repay_calldata = adapter.get_repay_calldata(
                    asset=actual_repay_address,
                    amount=repay_amount,
                    interest_rate_mode=aave_rate_mode,
                    on_behalf_of=self.wallet_address,
                )

                repay_tx = TransactionData(
                    to=pool_address,
                    value=0,
                    data="0x" + repay_calldata.hex(),
                    gas_estimate=adapter.estimate_repay_gas(),
                    description=(f"Repay {amount_description} {repay_token.symbol} ({rate_mode_label} rate)"),
                    tx_type="lending_repay",
                )
                transactions.append(repay_tx)

                total_gas = sum(tx.gas_estimate for tx in transactions)

                action_bundle = ActionBundle(
                    intent_type=IntentType.REPAY.value,
                    transactions=[tx.to_dict() for tx in transactions],
                    metadata={
                        "protocol": intent.protocol,
                        "pool_address": pool_address,
                        "repay_token": repay_token.to_dict(),
                        "repay_amount": str(repay_amount),
                        "repay_full": intent.repay_full,
                        "interest_rate_mode": aave_rate_mode,
                        "chain": self.chain,
                    },
                )

                result.action_bundle = action_bundle
                result.transactions = transactions
                result.total_gas_estimate = total_gas
                result.warnings = warnings

                logger.info(
                    f"Compiled REPAY: {repay_token.symbol}, full={intent.repay_full}, {len(transactions)} txs, {total_gas} gas"
                )

            # =================================================================
            # SPARK PATH (Aave V3 fork with Spark-specific addresses)
            # =================================================================
            elif protocol_lower == "spark":
                from ..connectors.spark import (
                    SPARK_POOL_ADDRESSES,
                    SPARK_VARIABLE_RATE_MODE,
                    SparkAdapter,
                    SparkConfig,
                )

                if self.chain not in SPARK_POOL_ADDRESSES:
                    return CompilationResult(
                        status=CompilationStatus.FAILED,
                        error=f"Spark not available on chain: {self.chain}. Supported: {list(SPARK_POOL_ADDRESSES.keys())}",
                        intent_id=intent.intent_id,
                    )

                spark_config = SparkConfig(
                    chain=self.chain,
                    wallet_address=self.wallet_address,
                )
                spark_adapter = SparkAdapter(spark_config)
                pool_address = spark_adapter.pool_address

                if intent.repay_full:
                    repay_amount = MAX_UINT256
                else:
                    assert repay_amount_decimal is not None
                    repay_amount = int(repay_amount_decimal * Decimal(10**repay_token.decimals))

                approve_amount = repay_amount if repay_amount != MAX_UINT256 else MAX_UINT256

                actual_repay_address = repay_token.address
                if not repay_token.is_native:
                    approve_txs = self._build_approve_tx(
                        repay_token.address,
                        pool_address,
                        approve_amount,
                    )
                    transactions.extend(approve_txs)
                else:
                    weth_address = self._get_wrapped_native_address()
                    if weth_address:
                        actual_repay_address = weth_address
                        approve_txs = self._build_approve_tx(
                            weth_address,
                            pool_address,
                            approve_amount,
                        )
                        transactions.extend(approve_txs)
                        warnings.append("Native token debt: using WETH for repayment")

                # Resolve interest rate mode: use intent value or default to variable
                # Note: stable rate is deprecated on Spark, rejected at intent layer
                spark_repay_rate_mode = SPARK_VARIABLE_RATE_MODE
                spark_repay_rate_label = "variable"

                # Build repay TX via Spark adapter
                repay_result = spark_adapter.repay(
                    asset=actual_repay_address,
                    amount=repay_amount_decimal if repay_amount_decimal else Decimal("0"),
                    interest_rate_mode=spark_repay_rate_mode,
                    on_behalf_of=self.wallet_address,
                    repay_all=intent.repay_full,
                )

                if not repay_result.success:
                    return CompilationResult(
                        status=CompilationStatus.FAILED,
                        error=f"Spark repay failed: {repay_result.error}",
                        intent_id=intent.intent_id,
                    )

                assert repay_result.tx_data is not None
                repay_data = repay_result.tx_data["data"]
                if not repay_data.startswith("0x"):
                    repay_data = "0x" + repay_data

                repay_tx = TransactionData(
                    to=repay_result.tx_data["to"],
                    value=0,
                    data=repay_data,
                    gas_estimate=repay_result.gas_estimate,
                    description=repay_result.description
                    or f"Repay {amount_description} {repay_token.symbol} to Spark ({spark_repay_rate_label} rate)",
                    tx_type="lending_repay",
                )
                transactions.append(repay_tx)

                total_gas = sum(tx.gas_estimate for tx in transactions)

                action_bundle = ActionBundle(
                    intent_type=IntentType.REPAY.value,
                    transactions=[tx.to_dict() for tx in transactions],
                    metadata={
                        "protocol": intent.protocol,
                        "pool_address": pool_address,
                        "repay_token": repay_token.to_dict(),
                        "repay_amount": str(repay_amount),
                        "repay_full": intent.repay_full,
                        "interest_rate_mode": spark_repay_rate_mode,
                        "chain": self.chain,
                    },
                )

                result.action_bundle = action_bundle
                result.transactions = transactions
                result.total_gas_estimate = total_gas
                result.warnings = warnings

                logger.info(
                    f"Compiled REPAY: {repay_token.symbol}, full={intent.repay_full}, {len(transactions)} txs, {total_gas} gas (Spark)"
                )

            # =================================================================
            # COMPOUND V3 PATH
            # =================================================================
            elif protocol_lower == "compound_v3":
                from ..connectors.compound_v3.adapter import (
                    COMPOUND_V3_COMET_ADDRESSES,
                    CompoundV3Adapter,
                    CompoundV3Config,
                )

                market = intent.market_id or "usdc"

                if self.chain not in COMPOUND_V3_COMET_ADDRESSES:
                    return CompilationResult(
                        status=CompilationStatus.FAILED,
                        error=f"Compound V3 not available on chain: {self.chain}. Supported: {list(COMPOUND_V3_COMET_ADDRESSES.keys())}",
                        intent_id=intent.intent_id,
                    )

                available_markets = COMPOUND_V3_COMET_ADDRESSES.get(self.chain, {})
                if market not in available_markets:
                    return CompilationResult(
                        status=CompilationStatus.FAILED,
                        error=f"Compound V3 market '{market}' not available on {self.chain}. Available: {list(available_markets.keys())}",
                        intent_id=intent.intent_id,
                    )

                compound_config = CompoundV3Config(
                    chain=self.chain,
                    wallet_address=self.wallet_address,
                    market=market,
                )
                compound_adapter = CompoundV3Adapter(compound_config)

                # Build approve TX for Comet contract (repay token -> Comet)
                if repay_amount_decimal is not None:
                    approve_amount = int(repay_amount_decimal * Decimal(10**repay_token.decimals))
                else:
                    approve_amount = MAX_UINT256  # Approve max for full repay

                approve_txs = self._build_approve_tx(
                    repay_token.address,
                    compound_adapter.comet_address,
                    approve_amount,
                )
                transactions.extend(approve_txs)

                # Build repay TX via Compound V3 adapter
                repay_result = compound_adapter.repay(
                    amount=repay_amount_decimal if repay_amount_decimal else Decimal("0"),
                    on_behalf_of=self.wallet_address,
                    repay_all=intent.repay_full,
                )

                if not repay_result.success:
                    return CompilationResult(
                        status=CompilationStatus.FAILED,
                        error=f"Compound V3 repay failed: {repay_result.error}",
                        intent_id=intent.intent_id,
                    )

                assert repay_result.tx_data is not None
                repay_data = repay_result.tx_data["data"]
                if not repay_data.startswith("0x"):
                    repay_data = "0x" + repay_data

                repay_tx = TransactionData(
                    to=repay_result.tx_data["to"],
                    value=int(repay_result.tx_data.get("value", 0)),
                    data=repay_data,
                    gas_estimate=repay_result.gas_estimate,
                    description=repay_result.description
                    or f"Repay {amount_description} {repay_token.symbol} to Compound V3",
                    tx_type="lending_repay",
                )
                transactions.append(repay_tx)

                total_gas = sum(tx.gas_estimate for tx in transactions)

                action_bundle = ActionBundle(
                    intent_type=IntentType.REPAY.value,
                    transactions=[tx.to_dict() for tx in transactions],
                    metadata={
                        "protocol": intent.protocol,
                        "comet_address": compound_adapter.comet_address,
                        "market": market,
                        "repay_token": repay_token.to_dict(),
                        "repay_amount": amount_description,
                        "repay_full": intent.repay_full,
                        "chain": self.chain,
                    },
                )

                result.action_bundle = action_bundle
                result.transactions = transactions
                result.total_gas_estimate = total_gas
                result.warnings = warnings

                logger.info(
                    f"Compiled REPAY: {amount_description} {repay_token.symbol} to Compound V3 {market}, "
                    f"full={intent.repay_full}, {len(transactions)} txs, {total_gas} gas"
                )

            # =================================================================
            # BENQI PATH (Compound V2 fork on Avalanche)
            # =================================================================
            elif protocol_lower == "benqi":
                from ..connectors.benqi.adapter import (
                    BENQI_QI_TOKENS,
                    BenqiAdapter,
                    BenqiConfig,
                )

                if self.chain != "avalanche":
                    return CompilationResult(
                        status=CompilationStatus.FAILED,
                        error=f"BENQI is only available on Avalanche, got: {self.chain}",
                        intent_id=intent.intent_id,
                    )

                benqi_config = BenqiConfig(
                    chain=self.chain,
                    wallet_address=self.wallet_address,
                )
                benqi_adapter = BenqiAdapter(benqi_config)

                repay_symbol = repay_token.symbol.upper()
                repay_market = benqi_adapter.get_market_info(repay_symbol)

                if not repay_market:
                    return CompilationResult(
                        status=CompilationStatus.FAILED,
                        error=f"BENQI does not support asset: {repay_symbol}. Supported: {list(BENQI_QI_TOKENS.keys())}",
                        intent_id=intent.intent_id,
                    )

                # Build approve TX for qiToken (skip for native AVAX)
                if not repay_market.is_native and not intent.repay_full:
                    if repay_amount_decimal is None:
                        return CompilationResult(
                            status=CompilationStatus.FAILED,
                            error="BENQI repay requires an explicit amount (or use repay_full=True)",
                            intent_id=intent.intent_id,
                        )
                    repay_amount_wei = int(repay_amount_decimal * Decimal(10**repay_token.decimals))
                    approve_txs = self._build_approve_tx(
                        repay_token.address,
                        repay_market.qi_token_address,
                        repay_amount_wei,
                    )
                    transactions.extend(approve_txs)
                elif not repay_market.is_native and intent.repay_full:
                    # For repay_full, approve MAX_UINT256
                    from ..connectors.benqi.adapter import MAX_UINT256 as BENQI_MAX_UINT256

                    approve_txs = self._build_approve_tx(
                        repay_token.address,
                        repay_market.qi_token_address,
                        BENQI_MAX_UINT256,
                    )
                    transactions.extend(approve_txs)

                # Build repay TX
                repay_result = benqi_adapter.repay(
                    asset=repay_symbol,
                    amount=repay_amount_decimal if repay_amount_decimal is not None else Decimal("0"),
                    repay_all=intent.repay_full,
                )

                if not repay_result.success:
                    return CompilationResult(
                        status=CompilationStatus.FAILED,
                        error=f"BENQI repay failed: {repay_result.error}",
                        intent_id=intent.intent_id,
                    )

                assert repay_result.tx_data is not None
                repay_data = repay_result.tx_data["data"]
                if not repay_data.startswith("0x"):
                    repay_data = "0x" + repay_data

                amount_description = "all" if intent.repay_full else str(repay_amount_decimal)

                repay_tx = TransactionData(
                    to=repay_result.tx_data["to"],
                    value=int(repay_result.tx_data.get("value", 0)),
                    data=repay_data,
                    gas_estimate=repay_result.gas_estimate,
                    description=repay_result.description,
                    tx_type="lending_repay",
                )
                transactions.append(repay_tx)

                total_gas = sum(tx.gas_estimate for tx in transactions)
                action_bundle = ActionBundle(
                    intent_type=IntentType.REPAY.value,
                    transactions=[tx.to_dict() for tx in transactions],
                    metadata={
                        "protocol": intent.protocol,
                        "comptroller_address": benqi_adapter.comptroller_address,
                        "repay_token": repay_token.to_dict(),
                        "repay_amount": amount_description,
                        "repay_full": intent.repay_full,
                        "chain": self.chain,
                    },
                )

                result.action_bundle = action_bundle
                result.transactions = transactions
                result.total_gas_estimate = total_gas
                result.warnings = warnings

                logger.info(
                    f"Compiled REPAY: {amount_description} {repay_token.symbol} to BENQI, "
                    f"full={intent.repay_full}, {len(transactions)} txs, {total_gas} gas"
                )

            # =================================================================
            # UNSUPPORTED PROTOCOL
            # =================================================================
            else:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"Unsupported lending protocol: {intent.protocol}. Supported: aave_v3, morpho, morpho_blue, spark, compound_v3, benqi",
                    intent_id=intent.intent_id,
                )

        except Exception as e:
            logger.exception(f"Failed to compile REPAY intent: {e}")
            result.status = CompilationStatus.FAILED
            result.error = str(e)

        return result

    def _compile_supply(self, intent: SupplyIntent) -> CompilationResult:
        """Compile a SUPPLY intent into an ActionBundle.

        This method:
        1. Resolves token address
        2. Converts amount to wei
        3. Builds approve TX for supply token
        4. Builds supply TX to deposit tokens

        Args:
            intent: SupplyIntent to compile

        Returns:
            CompilationResult with supply ActionBundle
        """
        result = CompilationResult(
            status=CompilationStatus.SUCCESS,
            intent_id=intent.intent_id,
        )
        transactions: list[TransactionData] = []
        warnings: list[str] = []

        try:
            protocol_lower = intent.protocol.lower()

            # =================================================================
            # SOLANA LENDING PATH (Kamino / Jupiter Lend)
            # =================================================================
            if protocol_lower == "jupiter_lend":
                if not self._is_solana_chain():
                    return CompilationResult(
                        status=CompilationStatus.FAILED,
                        intent_id=intent.intent_id,
                        error="Protocol 'jupiter_lend' is only available on Solana chains.",
                    )
                return self._compile_jupiter_lend_supply(intent)
            if protocol_lower == "kamino" or (
                self._is_solana_chain() and protocol_lower not in ("morpho", "morpho_blue", "jupiter_lend")
            ):
                if self._is_solana_chain() and protocol_lower not in ("kamino", ""):
                    return CompilationResult(
                        status=CompilationStatus.FAILED,
                        intent_id=intent.intent_id,
                        error=f"Protocol '{intent.protocol}' is not supported for SUPPLY on Solana. Supported: kamino, jupiter_lend",
                    )
                return self._compile_kamino_supply(intent)

            # Step 1: Resolve token address (needed for both protocols)
            supply_token = self._resolve_token(intent.token)
            if supply_token is None:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"Unknown token: {intent.token}",
                    intent_id=intent.intent_id,
                )

            # Step 2: Check for chained amount
            if intent.amount == "all":
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error="amount='all' must be resolved before compilation. Use Intent.set_resolved_amount() to resolve chained amounts.",
                    intent_id=intent.intent_id,
                )
            amount_decimal: Decimal = intent.amount  # type: ignore[assignment]

            # =================================================================
            # MORPHO BLUE PATH
            # =================================================================
            if protocol_lower in ("morpho", "morpho_blue"):
                # Validate market_id is provided
                if not intent.market_id:
                    return CompilationResult(
                        status=CompilationStatus.FAILED,
                        error="market_id is required for Morpho Blue supply",
                        intent_id=intent.intent_id,
                    )

                # Lazy import to avoid circular import
                from ..connectors.morpho_blue.adapter import MorphoBlueAdapter, MorphoBlueConfig

                # Create Morpho adapter
                morpho_config = MorphoBlueConfig(
                    chain=self.chain,
                    wallet_address=self.wallet_address,
                )
                morpho_adapter = MorphoBlueAdapter(morpho_config)

                # Build approve TX for Morpho Blue contract
                approve_txs = self._build_approve_tx(
                    supply_token.address,
                    morpho_adapter.morpho_address,
                    int(amount_decimal * Decimal(10**supply_token.decimals)),
                )
                transactions.extend(approve_txs)

                # Build supply collateral TX via Morpho adapter
                tx_result = morpho_adapter.supply_collateral(
                    market_id=intent.market_id,
                    amount=amount_decimal,
                    on_behalf_of=self.wallet_address,
                )

                if not tx_result.success:
                    return CompilationResult(
                        status=CompilationStatus.FAILED,
                        error=f"Morpho Blue supply failed: {tx_result.error}",
                        intent_id=intent.intent_id,
                    )

                assert tx_result.tx_data is not None
                supply_tx = TransactionData(
                    to=tx_result.tx_data["to"],
                    value=tx_result.tx_data["value"],
                    data=tx_result.tx_data["data"],
                    gas_estimate=tx_result.gas_estimate,
                    description=tx_result.description
                    or f"Supply {amount_decimal} {supply_token.symbol} to Morpho Blue",
                    tx_type="lending_supply_collateral",
                )
                transactions.append(supply_tx)

                # Build ActionBundle for Morpho
                total_gas = sum(tx.gas_estimate for tx in transactions)
                action_bundle = ActionBundle(
                    intent_type=IntentType.SUPPLY.value,
                    transactions=[tx.to_dict() for tx in transactions],
                    metadata={
                        "protocol": intent.protocol,
                        "morpho_address": morpho_adapter.morpho_address,
                        "market_id": intent.market_id,
                        "supply_token": supply_token.to_dict(),
                        "supply_amount": str(amount_decimal),
                        "chain": self.chain,
                    },
                )

                result.action_bundle = action_bundle
                result.transactions = transactions
                result.total_gas_estimate = total_gas
                result.warnings = warnings

                logger.info(
                    f"Compiled SUPPLY: {amount_decimal} {supply_token.symbol} to Morpho Blue market {intent.market_id[:16]}..."
                )
                return result

            # =================================================================
            # AAVE V3 PATH
            # =================================================================
            elif protocol_lower.startswith("aave"):
                # Get lending adapter
                adapter = AaveV3Adapter(self.chain, "aave_v3")
                pool_address = adapter.get_pool_address()

                if pool_address == "0x0000000000000000000000000000000000000000":
                    return CompilationResult(
                        status=CompilationStatus.FAILED,
                        error=f"Aave V3 not available on chain: {self.chain}",
                        intent_id=intent.intent_id,
                    )

                supply_amount = int(amount_decimal * Decimal(10**supply_token.decimals))

                # Handle native token vs ERC20
                actual_supply_address = supply_token.address
                supply_value = 0

                if supply_token.is_native:
                    weth_address = self._get_wrapped_native_address()
                    if weth_address:
                        actual_supply_address = weth_address
                        warnings.append("Native token supply: will wrap to WETH before supplying")
                    else:
                        return CompilationResult(
                            status=CompilationStatus.FAILED,
                            error="Cannot supply native ETH - WETH address not found",
                            intent_id=intent.intent_id,
                        )

                # Build approve TX (skip for native token scenarios)
                if not supply_token.is_native:
                    approve_txs = self._build_approve_tx(
                        actual_supply_address,
                        pool_address,
                        supply_amount,
                    )
                    transactions.extend(approve_txs)

                # Build supply TX
                supply_calldata = adapter.get_supply_calldata(
                    asset=actual_supply_address,
                    amount=supply_amount,
                    on_behalf_of=self.wallet_address,
                )

                supply_tx = TransactionData(
                    to=pool_address,
                    value=supply_value,
                    data="0x" + supply_calldata.hex(),
                    gas_estimate=adapter.estimate_supply_gas(),
                    description=(
                        f"Supply {self._format_amount(supply_amount, supply_token.decimals)} {supply_token.symbol} to Aave V3"
                    ),
                    tx_type="lending_supply",
                )
                transactions.append(supply_tx)

                # Build setUserUseReserveAsCollateral TX if requested
                if intent.use_as_collateral:
                    set_collateral_calldata = adapter.get_set_collateral_calldata(
                        asset=actual_supply_address,
                        use_as_collateral=True,
                    )

                    set_collateral_tx = TransactionData(
                        to=pool_address,
                        value=0,
                        data="0x" + set_collateral_calldata.hex(),
                        gas_estimate=adapter.estimate_set_collateral_gas(),
                        description=(f"Enable {supply_token.symbol} as collateral on Aave V3"),
                        tx_type="lending_set_collateral",
                    )
                    transactions.append(set_collateral_tx)

                # Build ActionBundle
                total_gas = sum(tx.gas_estimate for tx in transactions)

                action_bundle = ActionBundle(
                    intent_type=IntentType.SUPPLY.value,
                    transactions=[tx.to_dict() for tx in transactions],
                    metadata={
                        "protocol": intent.protocol,
                        "pool_address": pool_address,
                        "supply_token": supply_token.to_dict(),
                        "supply_amount": str(supply_amount),
                        "use_as_collateral": intent.use_as_collateral,
                        "chain": self.chain,
                    },
                )

                result.action_bundle = action_bundle
                result.transactions = transactions
                result.total_gas_estimate = total_gas
                result.warnings = warnings

                # Format amounts for user-friendly logging
                supply_fmt = format_token_amount(supply_amount, supply_token.symbol, supply_token.decimals)
                collateral_str = " (as collateral)" if intent.use_as_collateral else ""

                logger.info(f"Compiled SUPPLY: {supply_fmt} to {intent.protocol}{collateral_str}")
                logger.info(f"   Txs: {len(transactions)} | Gas: {total_gas:,}")

            # =================================================================
            # SPARK PATH (Aave V3 fork with Spark-specific addresses)
            # =================================================================
            elif protocol_lower == "spark":
                from ..connectors.spark import (
                    SPARK_POOL_ADDRESSES,
                    SparkAdapter,
                    SparkConfig,
                )

                if self.chain not in SPARK_POOL_ADDRESSES:
                    return CompilationResult(
                        status=CompilationStatus.FAILED,
                        error=f"Spark not available on chain: {self.chain}. Supported: {list(SPARK_POOL_ADDRESSES.keys())}",
                        intent_id=intent.intent_id,
                    )

                spark_config = SparkConfig(
                    chain=self.chain,
                    wallet_address=self.wallet_address,
                )
                spark_adapter = SparkAdapter(spark_config)
                pool_address = spark_adapter.pool_address

                supply_amount = int(amount_decimal * Decimal(10**supply_token.decimals))

                # Handle native token vs ERC20
                actual_supply_address = supply_token.address
                supply_value = 0

                if supply_token.is_native:
                    weth_address = self._get_wrapped_native_address()
                    if weth_address:
                        actual_supply_address = weth_address
                        # Wrap native ETH -> WETH
                        wrap_tx = TransactionData(
                            to=weth_address,
                            value=supply_amount,
                            data="0xd0e30db0",  # WETH.deposit()
                            gas_estimate=get_gas_estimate(self.chain, "wrap_eth"),
                            description=f"Wrap {self._format_amount(supply_amount, supply_token.decimals)} {supply_token.symbol} to WETH",
                            tx_type="wrap",
                        )
                        transactions.append(wrap_tx)
                        # Approve WETH for pool
                        approve_txs = self._build_approve_tx(
                            weth_address,
                            pool_address,
                            supply_amount,
                        )
                        transactions.extend(approve_txs)
                        warnings.append("Native token supply: wrapped to WETH before supplying")
                    else:
                        return CompilationResult(
                            status=CompilationStatus.FAILED,
                            error="Cannot supply native ETH - WETH address not found",
                            intent_id=intent.intent_id,
                        )
                else:
                    approve_txs = self._build_approve_tx(
                        actual_supply_address,
                        pool_address,
                        supply_amount,
                    )
                    transactions.extend(approve_txs)

                # Build supply TX via Spark adapter
                supply_result: Any = spark_adapter.supply(
                    asset=actual_supply_address,
                    amount=amount_decimal,
                    on_behalf_of=self.wallet_address,
                )

                if not supply_result.success:
                    return CompilationResult(
                        status=CompilationStatus.FAILED,
                        error=f"Spark supply failed: {supply_result.error}",
                        intent_id=intent.intent_id,
                    )

                assert supply_result.tx_data is not None
                supply_data = supply_result.tx_data["data"]
                if not supply_data.startswith("0x"):
                    supply_data = "0x" + supply_data

                supply_value = int(supply_result.tx_data.get("value", 0))

                supply_tx = TransactionData(
                    to=supply_result.tx_data["to"],
                    value=supply_value,
                    data=supply_data,
                    gas_estimate=supply_result.gas_estimate,
                    description=supply_result.description
                    or f"Supply {self._format_amount(supply_amount, supply_token.decimals)} {supply_token.symbol} to Spark",
                    tx_type="lending_supply",
                )
                transactions.append(supply_tx)

                # Build ActionBundle
                total_gas = sum(tx.gas_estimate for tx in transactions)

                action_bundle = ActionBundle(
                    intent_type=IntentType.SUPPLY.value,
                    transactions=[tx.to_dict() for tx in transactions],
                    metadata={
                        "protocol": intent.protocol,
                        "pool_address": pool_address,
                        "supply_token": supply_token.to_dict(),
                        "supply_amount": str(supply_amount),
                        "use_as_collateral": intent.use_as_collateral,
                        "chain": self.chain,
                    },
                )

                result.action_bundle = action_bundle
                result.transactions = transactions
                result.total_gas_estimate = total_gas
                result.warnings = warnings

                supply_fmt = format_token_amount(supply_amount, supply_token.symbol, supply_token.decimals)
                collateral_str = " (as collateral)" if intent.use_as_collateral else ""

                logger.info(f"Compiled SUPPLY: {supply_fmt} to Spark{collateral_str}")
                logger.info(f"   Txs: {len(transactions)} | Gas: {total_gas:,}")

            # =================================================================
            # COMPOUND V3 PATH
            # =================================================================
            elif protocol_lower == "compound_v3":
                from ..connectors.compound_v3.adapter import (
                    COMPOUND_V3_COMET_ADDRESSES,
                    CompoundV3Adapter,
                    CompoundV3Config,
                )

                market = intent.market_id or "usdc"

                if self.chain not in COMPOUND_V3_COMET_ADDRESSES:
                    return CompilationResult(
                        status=CompilationStatus.FAILED,
                        error=f"Compound V3 not available on chain: {self.chain}. Supported: {list(COMPOUND_V3_COMET_ADDRESSES.keys())}",
                        intent_id=intent.intent_id,
                    )

                available_markets = COMPOUND_V3_COMET_ADDRESSES.get(self.chain, {})
                if market not in available_markets:
                    return CompilationResult(
                        status=CompilationStatus.FAILED,
                        error=f"Compound V3 market '{market}' not available on {self.chain}. Available: {list(available_markets.keys())}",
                        intent_id=intent.intent_id,
                    )

                compound_config = CompoundV3Config(
                    chain=self.chain,
                    wallet_address=self.wallet_address,
                    market=market,
                )
                compound_adapter = CompoundV3Adapter(compound_config)

                supply_amount_wei = int(amount_decimal * Decimal(10**supply_token.decimals))

                # Build approve TX for Comet contract
                approve_txs = self._build_approve_tx(
                    supply_token.address,
                    compound_adapter.comet_address,
                    supply_amount_wei,
                )
                transactions.extend(approve_txs)

                # Build supply TX via Compound V3 adapter
                supply_result = compound_adapter.supply(amount=amount_decimal)

                if not supply_result.success:
                    return CompilationResult(
                        status=CompilationStatus.FAILED,
                        error=f"Compound V3 supply failed: {supply_result.error}",
                        intent_id=intent.intent_id,
                    )

                assert supply_result.tx_data is not None
                supply_data = supply_result.tx_data["data"]
                if not supply_data.startswith("0x"):
                    supply_data = "0x" + supply_data

                supply_tx = TransactionData(
                    to=supply_result.tx_data["to"],
                    value=int(supply_result.tx_data.get("value", 0)),
                    data=supply_data,
                    gas_estimate=supply_result.gas_estimate,
                    description=supply_result.description
                    or f"Supply {amount_decimal} {supply_token.symbol} to Compound V3",
                    tx_type="lending_supply",
                )
                transactions.append(supply_tx)

                # Build ActionBundle
                total_gas = sum(tx.gas_estimate for tx in transactions)

                action_bundle = ActionBundle(
                    intent_type=IntentType.SUPPLY.value,
                    transactions=[tx.to_dict() for tx in transactions],
                    metadata={
                        "protocol": intent.protocol,
                        "comet_address": compound_adapter.comet_address,
                        "market": market,
                        "supply_token": supply_token.to_dict(),
                        "supply_amount": str(amount_decimal),
                        "chain": self.chain,
                    },
                )

                result.action_bundle = action_bundle
                result.transactions = transactions
                result.total_gas_estimate = total_gas
                result.warnings = warnings

                supply_fmt = format_token_amount(supply_amount_wei, supply_token.symbol, supply_token.decimals)
                logger.info(f"Compiled SUPPLY: {supply_fmt} to Compound V3 ({market} market)")
                logger.info(f"   Txs: {len(transactions)} | Gas: {total_gas:,}")

            # =================================================================
            # BENQI PATH (Compound V2 fork on Avalanche)
            # =================================================================
            elif protocol_lower == "benqi":
                from ..connectors.benqi.adapter import (
                    BENQI_QI_TOKENS,
                    BenqiAdapter,
                    BenqiConfig,
                )

                if self.chain != "avalanche":
                    return CompilationResult(
                        status=CompilationStatus.FAILED,
                        error=f"BENQI is only available on Avalanche, got: {self.chain}",
                        intent_id=intent.intent_id,
                    )

                benqi_config = BenqiConfig(
                    chain=self.chain,
                    wallet_address=self.wallet_address,
                )
                benqi_adapter = BenqiAdapter(benqi_config)

                supply_symbol = supply_token.symbol.upper()
                supply_market = benqi_adapter.get_market_info(supply_symbol)

                if not supply_market:
                    return CompilationResult(
                        status=CompilationStatus.FAILED,
                        error=f"BENQI does not support asset: {supply_symbol}. Supported: {list(BENQI_QI_TOKENS.keys())}",
                        intent_id=intent.intent_id,
                    )

                supply_amount_wei = int(amount_decimal * Decimal(10**supply_token.decimals))

                # Build approve TX for qiToken (skip for native AVAX)
                if not supply_market.is_native:
                    approve_txs = self._build_approve_tx(
                        supply_token.address,
                        supply_market.qi_token_address,
                        supply_amount_wei,
                    )
                    transactions.extend(approve_txs)

                # Build supply (mint) TX
                supply_result = benqi_adapter.supply(
                    asset=supply_symbol,
                    amount=amount_decimal,
                )

                if not supply_result.success:
                    return CompilationResult(
                        status=CompilationStatus.FAILED,
                        error=f"BENQI supply failed: {supply_result.error}",
                        intent_id=intent.intent_id,
                    )

                assert supply_result.tx_data is not None
                supply_data = supply_result.tx_data["data"]
                if not supply_data.startswith("0x"):
                    supply_data = "0x" + supply_data

                supply_tx = TransactionData(
                    to=supply_result.tx_data["to"],
                    value=int(supply_result.tx_data.get("value", 0)),
                    data=supply_data,
                    gas_estimate=supply_result.gas_estimate,
                    description=supply_result.description or f"Supply {amount_decimal} {supply_token.symbol} to BENQI",
                    tx_type="lending_supply",
                )
                transactions.append(supply_tx)

                # Optionally enable as collateral via enterMarkets
                if intent.use_as_collateral:
                    enter_result = benqi_adapter.enter_markets([supply_symbol])
                    if not enter_result.success:
                        return CompilationResult(
                            status=CompilationStatus.FAILED,
                            error=f"BENQI enterMarkets failed: {enter_result.error}",
                            intent_id=intent.intent_id,
                        )
                    assert enter_result.tx_data is not None
                    enter_data = enter_result.tx_data["data"]
                    if not enter_data.startswith("0x"):
                        enter_data = "0x" + enter_data
                    enter_tx = TransactionData(
                        to=enter_result.tx_data["to"],
                        value=0,
                        data=enter_data,
                        gas_estimate=enter_result.gas_estimate,
                        description=enter_result.description,
                        tx_type="lending_enter_markets",
                    )
                    transactions.append(enter_tx)

                # Build ActionBundle
                total_gas = sum(tx.gas_estimate for tx in transactions)

                action_bundle = ActionBundle(
                    intent_type=IntentType.SUPPLY.value,
                    transactions=[tx.to_dict() for tx in transactions],
                    metadata={
                        "protocol": intent.protocol,
                        "comptroller_address": benqi_adapter.comptroller_address,
                        "qi_token_address": supply_market.qi_token_address,
                        "supply_token": supply_token.to_dict(),
                        "supply_amount": str(amount_decimal),
                        "use_as_collateral": intent.use_as_collateral,
                        "chain": self.chain,
                    },
                )

                result.action_bundle = action_bundle
                result.transactions = transactions
                result.total_gas_estimate = total_gas
                result.warnings = warnings

                supply_fmt = format_token_amount(supply_amount_wei, supply_token.symbol, supply_token.decimals)
                collateral_str = " (as collateral)" if intent.use_as_collateral else ""
                logger.info(f"Compiled SUPPLY: {supply_fmt} to BENQI{collateral_str}")
                logger.info(f"   Txs: {len(transactions)} | Gas: {total_gas:,}")

            # =================================================================
            # UNSUPPORTED PROTOCOL
            # =================================================================
            else:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"Unsupported lending protocol: {intent.protocol}. Supported: aave_v3, morpho, morpho_blue, spark, compound_v3, benqi",
                    intent_id=intent.intent_id,
                )

        except Exception as e:
            logger.exception(f"Failed to compile SUPPLY intent: {e}")
            result.status = CompilationStatus.FAILED
            result.error = str(e)

        return result

    def _compile_withdraw(self, intent: WithdrawIntent) -> CompilationResult:
        """Compile a WITHDRAW intent into an ActionBundle.

        This method:
        1. Resolves token address
        2. Converts amount to wei (or uses MAX_UINT256 for withdraw all)
        3. Builds withdraw TX

        Args:
            intent: WithdrawIntent to compile

        Returns:
            CompilationResult with withdraw ActionBundle
        """
        result = CompilationResult(
            status=CompilationStatus.SUCCESS,
            intent_id=intent.intent_id,
        )
        transactions: list[TransactionData] = []
        warnings: list[str] = []

        try:
            protocol_lower = intent.protocol.lower()

            # =================================================================
            # SOLANA LENDING PATH (Kamino / Jupiter Lend)
            # =================================================================
            if protocol_lower == "jupiter_lend":
                if not self._is_solana_chain():
                    return CompilationResult(
                        status=CompilationStatus.FAILED,
                        intent_id=intent.intent_id,
                        error="Protocol 'jupiter_lend' is only available on Solana chains.",
                    )
                return self._compile_jupiter_lend_withdraw(intent)
            if protocol_lower == "kamino" or (
                self._is_solana_chain() and protocol_lower not in ("morpho", "morpho_blue", "jupiter_lend")
            ):
                if self._is_solana_chain() and protocol_lower not in ("kamino", ""):
                    return CompilationResult(
                        status=CompilationStatus.FAILED,
                        intent_id=intent.intent_id,
                        error=f"Protocol '{intent.protocol}' is not supported for WITHDRAW on Solana. Supported: kamino, jupiter_lend",
                    )
                return self._compile_kamino_withdraw(intent)

            # Step 1: Resolve token address
            withdraw_token = self._resolve_token(intent.token)
            if withdraw_token is None:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"Unknown token: {intent.token}",
                    intent_id=intent.intent_id,
                )

            # Step 2: Calculate amount
            withdraw_amount_decimal: Decimal | None
            if intent.withdraw_all:
                withdraw_amount_decimal = None  # Will use withdraw_all flag
                warnings.append("Withdrawing all available balance")
            else:
                if intent.amount == "all":
                    return CompilationResult(
                        status=CompilationStatus.FAILED,
                        error="amount='all' must be resolved before compilation. Use Intent.set_resolved_amount() to resolve chained amounts.",
                        intent_id=intent.intent_id,
                    )
                withdraw_amount_decimal = intent.amount  # type: ignore[assignment]

            # =================================================================
            # MORPHO BLUE PATH
            # =================================================================
            if protocol_lower in ("morpho", "morpho_blue"):
                if not intent.market_id:
                    return CompilationResult(
                        status=CompilationStatus.FAILED,
                        error="market_id is required for Morpho Blue withdraw",
                        intent_id=intent.intent_id,
                    )

                # Lazy import to avoid circular import
                from ..connectors.morpho_blue.adapter import MorphoBlueAdapter, MorphoBlueConfig

                # Resolve RPC URL with compiler's chain-aware fallback logic
                # (explicit rpc_url -> managed Anvil fork -> configured provider)
                morpho_rpc_url = self._get_chain_rpc_url()

                morpho_config = MorphoBlueConfig(
                    chain=self.chain,
                    wallet_address=self.wallet_address,
                    rpc_url=morpho_rpc_url,  # Pass RPC URL for on-chain queries (e.g., withdraw_all)
                )
                morpho_adapter = MorphoBlueAdapter(morpho_config)

                # Build withdraw collateral TX
                withdraw_result: Any = morpho_adapter.withdraw_collateral(
                    market_id=intent.market_id,
                    amount=withdraw_amount_decimal if withdraw_amount_decimal else Decimal("0"),
                    receiver=self.wallet_address,
                    on_behalf_of=self.wallet_address,
                    withdraw_all=intent.withdraw_all,
                )

                if not withdraw_result.success:
                    return CompilationResult(
                        status=CompilationStatus.FAILED,
                        error=f"Morpho Blue withdraw failed: {withdraw_result.error}",
                        intent_id=intent.intent_id,
                    )

                amount_display = "all" if intent.withdraw_all else str(withdraw_amount_decimal)

                assert withdraw_result.tx_data is not None
                withdraw_tx = TransactionData(
                    to=withdraw_result.tx_data["to"],
                    value=withdraw_result.tx_data["value"],
                    data=withdraw_result.tx_data["data"],
                    gas_estimate=withdraw_result.gas_estimate,
                    description=withdraw_result.description
                    or f"Withdraw {amount_display} {withdraw_token.symbol} from Morpho Blue",
                    tx_type="lending_withdraw_collateral",
                )
                transactions.append(withdraw_tx)

                total_gas = sum(tx.gas_estimate for tx in transactions)
                action_bundle = ActionBundle(
                    intent_type=IntentType.WITHDRAW.value,
                    transactions=[tx.to_dict() for tx in transactions],
                    metadata={
                        "protocol": intent.protocol,
                        "morpho_address": morpho_adapter.morpho_address,
                        "market_id": intent.market_id,
                        "withdraw_token": withdraw_token.to_dict(),
                        "withdraw_amount": amount_display,
                        "withdraw_all": intent.withdraw_all,
                        "chain": self.chain,
                    },
                )

                result.action_bundle = action_bundle
                result.transactions = transactions
                result.total_gas_estimate = total_gas
                result.warnings = warnings

                logger.info(f"Compiled WITHDRAW: {amount_display} {withdraw_token.symbol} from Morpho Blue")
                return result

            # =================================================================
            # AAVE V3 PATH
            # =================================================================
            elif protocol_lower.startswith("aave"):
                adapter = AaveV3Adapter(self.chain, "aave_v3")
                pool_address = adapter.get_pool_address()

                if pool_address == "0x0000000000000000000000000000000000000000":
                    return CompilationResult(
                        status=CompilationStatus.FAILED,
                        error=f"Aave V3 not available on chain: {self.chain}",
                        intent_id=intent.intent_id,
                    )

                if intent.withdraw_all:
                    withdraw_amount = MAX_UINT256
                else:
                    assert withdraw_amount_decimal is not None
                    withdraw_amount = int(withdraw_amount_decimal * Decimal(10**withdraw_token.decimals))

                actual_withdraw_address = withdraw_token.address

                if withdraw_token.is_native:
                    weth_address = self._get_wrapped_native_address()
                    if weth_address:
                        actual_withdraw_address = weth_address
                        warnings.append("Native token withdraw: will receive WETH (unwrap separately if needed)")
                    else:
                        return CompilationResult(
                            status=CompilationStatus.FAILED,
                            error="Cannot withdraw native ETH - WETH address not found",
                            intent_id=intent.intent_id,
                        )

                withdraw_calldata = adapter.get_withdraw_calldata(
                    asset=actual_withdraw_address,
                    amount=withdraw_amount,
                    to=self.wallet_address,
                )

                amount_display = (
                    "all" if intent.withdraw_all else self._format_amount(withdraw_amount, withdraw_token.decimals)
                )

                withdraw_tx = TransactionData(
                    to=pool_address,
                    value=0,
                    data="0x" + withdraw_calldata.hex(),
                    gas_estimate=adapter.estimate_withdraw_gas(),
                    description=(f"Withdraw {amount_display} {withdraw_token.symbol} from Aave V3"),
                    tx_type="lending_withdraw",
                )
                transactions.append(withdraw_tx)

                total_gas = sum(tx.gas_estimate for tx in transactions)

                action_bundle = ActionBundle(
                    intent_type=IntentType.WITHDRAW.value,
                    transactions=[tx.to_dict() for tx in transactions],
                    metadata={
                        "protocol": intent.protocol,
                        "pool_address": pool_address,
                        "withdraw_token": withdraw_token.to_dict(),
                        "withdraw_amount": str(withdraw_amount),
                        "withdraw_all": intent.withdraw_all,
                        "chain": self.chain,
                    },
                )

                result.action_bundle = action_bundle
                result.transactions = transactions
                result.total_gas_estimate = total_gas
                result.warnings = warnings

                logger.info(
                    f"Compiled WITHDRAW: {withdraw_token.symbol}, all={intent.withdraw_all}, {len(transactions)} txs, {total_gas} gas"
                )

            # =================================================================
            # SPARK PATH (Aave V3 fork with Spark-specific addresses)
            # =================================================================
            elif protocol_lower == "spark":
                from ..connectors.spark import (
                    SPARK_POOL_ADDRESSES,
                    SparkAdapter,
                    SparkConfig,
                )

                if self.chain not in SPARK_POOL_ADDRESSES:
                    return CompilationResult(
                        status=CompilationStatus.FAILED,
                        error=f"Spark not available on chain: {self.chain}. Supported: {list(SPARK_POOL_ADDRESSES.keys())}",
                        intent_id=intent.intent_id,
                    )

                spark_config = SparkConfig(
                    chain=self.chain,
                    wallet_address=self.wallet_address,
                )
                spark_adapter = SparkAdapter(spark_config)
                pool_address = spark_adapter.pool_address

                if intent.withdraw_all:
                    withdraw_amount = MAX_UINT256
                else:
                    assert withdraw_amount_decimal is not None
                    withdraw_amount = int(withdraw_amount_decimal * Decimal(10**withdraw_token.decimals))

                actual_withdraw_address = withdraw_token.address

                if withdraw_token.is_native:
                    weth_address = self._get_wrapped_native_address()
                    if weth_address:
                        actual_withdraw_address = weth_address
                        warnings.append("Native token withdraw: will receive WETH (unwrap separately if needed)")
                    else:
                        return CompilationResult(
                            status=CompilationStatus.FAILED,
                            error="Cannot withdraw native ETH - WETH address not found",
                            intent_id=intent.intent_id,
                        )

                # Build withdraw TX via Spark adapter
                withdraw_result = spark_adapter.withdraw(
                    asset=actual_withdraw_address,
                    amount=withdraw_amount_decimal if withdraw_amount_decimal else Decimal("0"),
                    to=self.wallet_address,
                    withdraw_all=intent.withdraw_all,
                )

                if not withdraw_result.success:
                    return CompilationResult(
                        status=CompilationStatus.FAILED,
                        error=f"Spark withdraw failed: {withdraw_result.error}",
                        intent_id=intent.intent_id,
                    )

                amount_display = (
                    "all" if intent.withdraw_all else self._format_amount(withdraw_amount, withdraw_token.decimals)
                )

                assert withdraw_result.tx_data is not None
                withdraw_data = withdraw_result.tx_data["data"]
                if not withdraw_data.startswith("0x"):
                    withdraw_data = "0x" + withdraw_data

                withdraw_tx = TransactionData(
                    to=withdraw_result.tx_data["to"],
                    value=0,
                    data=withdraw_data,
                    gas_estimate=withdraw_result.gas_estimate,
                    description=withdraw_result.description
                    or f"Withdraw {amount_display} {withdraw_token.symbol} from Spark",
                    tx_type="lending_withdraw",
                )
                transactions.append(withdraw_tx)

                total_gas = sum(tx.gas_estimate for tx in transactions)

                action_bundle = ActionBundle(
                    intent_type=IntentType.WITHDRAW.value,
                    transactions=[tx.to_dict() for tx in transactions],
                    metadata={
                        "protocol": intent.protocol,
                        "pool_address": pool_address,
                        "withdraw_token": withdraw_token.to_dict(),
                        "withdraw_amount": str(withdraw_amount),
                        "withdraw_all": intent.withdraw_all,
                        "chain": self.chain,
                    },
                )

                result.action_bundle = action_bundle
                result.transactions = transactions
                result.total_gas_estimate = total_gas
                result.warnings = warnings

                logger.info(
                    f"Compiled WITHDRAW: {withdraw_token.symbol}, all={intent.withdraw_all}, {len(transactions)} txs, {total_gas} gas (Spark)"
                )

            # =================================================================
            # PENDLE REDEEM PATH
            # =================================================================
            elif protocol_lower == "pendle":
                return self._compile_pendle_redeem(intent)

            # =================================================================
            # COMPOUND V3 PATH
            # =================================================================
            elif protocol_lower == "compound_v3":
                from ..connectors.compound_v3.adapter import (
                    COMPOUND_V3_COMET_ADDRESSES,
                    CompoundV3Adapter,
                    CompoundV3Config,
                )

                market = intent.market_id or "usdc"

                if self.chain not in COMPOUND_V3_COMET_ADDRESSES:
                    return CompilationResult(
                        status=CompilationStatus.FAILED,
                        error=f"Compound V3 not available on chain: {self.chain}. Supported: {list(COMPOUND_V3_COMET_ADDRESSES.keys())}",
                        intent_id=intent.intent_id,
                    )

                available_markets = COMPOUND_V3_COMET_ADDRESSES.get(self.chain, {})
                if market not in available_markets:
                    return CompilationResult(
                        status=CompilationStatus.FAILED,
                        error=f"Compound V3 market '{market}' not available on {self.chain}. Available: {list(available_markets.keys())}",
                        intent_id=intent.intent_id,
                    )

                compound_config = CompoundV3Config(
                    chain=self.chain,
                    wallet_address=self.wallet_address,
                    market=market,
                )
                compound_adapter = CompoundV3Adapter(compound_config)

                # Build withdraw TX via Compound V3 adapter
                withdraw_result = compound_adapter.withdraw(
                    amount=withdraw_amount_decimal if withdraw_amount_decimal else Decimal("0"),
                    withdraw_all=intent.withdraw_all,
                )

                if not withdraw_result.success:
                    return CompilationResult(
                        status=CompilationStatus.FAILED,
                        error=f"Compound V3 withdraw failed: {withdraw_result.error}",
                        intent_id=intent.intent_id,
                    )

                amount_display = "all" if intent.withdraw_all else str(withdraw_amount_decimal)

                assert withdraw_result.tx_data is not None
                withdraw_data = withdraw_result.tx_data["data"]
                if not withdraw_data.startswith("0x"):
                    withdraw_data = "0x" + withdraw_data

                withdraw_tx = TransactionData(
                    to=withdraw_result.tx_data["to"],
                    value=int(withdraw_result.tx_data.get("value", 0)),
                    data=withdraw_data,
                    gas_estimate=withdraw_result.gas_estimate,
                    description=withdraw_result.description
                    or f"Withdraw {amount_display} {withdraw_token.symbol} from Compound V3",
                    tx_type="lending_withdraw",
                )
                transactions.append(withdraw_tx)

                total_gas = sum(tx.gas_estimate for tx in transactions)

                action_bundle = ActionBundle(
                    intent_type=IntentType.WITHDRAW.value,
                    transactions=[tx.to_dict() for tx in transactions],
                    metadata={
                        "protocol": intent.protocol,
                        "comet_address": compound_adapter.comet_address,
                        "market": market,
                        "withdraw_token": withdraw_token.to_dict(),
                        "withdraw_amount": amount_display,
                        "withdraw_all": intent.withdraw_all,
                        "chain": self.chain,
                    },
                )

                result.action_bundle = action_bundle
                result.transactions = transactions
                result.total_gas_estimate = total_gas
                result.warnings = warnings

                logger.info(
                    f"Compiled WITHDRAW: {withdraw_token.symbol}, all={intent.withdraw_all}, {len(transactions)} txs, {total_gas} gas (Compound V3)"
                )

            # =================================================================
            # BENQI PATH (Compound V2 fork on Avalanche)
            # =================================================================
            elif protocol_lower == "benqi":
                from ..connectors.benqi.adapter import (
                    BENQI_QI_TOKENS,
                    BenqiAdapter,
                    BenqiConfig,
                )

                if self.chain != "avalanche":
                    return CompilationResult(
                        status=CompilationStatus.FAILED,
                        error=f"BENQI is only available on Avalanche, got: {self.chain}",
                        intent_id=intent.intent_id,
                    )

                benqi_config = BenqiConfig(
                    chain=self.chain,
                    wallet_address=self.wallet_address,
                )
                benqi_adapter = BenqiAdapter(benqi_config)

                withdraw_symbol = withdraw_token.symbol.upper()
                withdraw_market = benqi_adapter.get_market_info(withdraw_symbol)

                if not withdraw_market:
                    return CompilationResult(
                        status=CompilationStatus.FAILED,
                        error=f"BENQI does not support asset: {withdraw_symbol}. Supported: {list(BENQI_QI_TOKENS.keys())}",
                        intent_id=intent.intent_id,
                    )

                # Build withdraw (redeem) TX
                withdraw_result = benqi_adapter.withdraw(
                    asset=withdraw_symbol,
                    amount=withdraw_amount_decimal if withdraw_amount_decimal else Decimal("0"),
                    withdraw_all=intent.withdraw_all,
                )

                if not withdraw_result.success:
                    return CompilationResult(
                        status=CompilationStatus.FAILED,
                        error=f"BENQI withdraw failed: {withdraw_result.error}",
                        intent_id=intent.intent_id,
                    )

                amount_display = "all" if intent.withdraw_all else str(withdraw_amount_decimal)

                assert withdraw_result.tx_data is not None
                withdraw_data = withdraw_result.tx_data["data"]
                if not withdraw_data.startswith("0x"):
                    withdraw_data = "0x" + withdraw_data

                withdraw_tx = TransactionData(
                    to=withdraw_result.tx_data["to"],
                    value=int(withdraw_result.tx_data.get("value", 0)),
                    data=withdraw_data,
                    gas_estimate=withdraw_result.gas_estimate,
                    description=withdraw_result.description
                    or f"Withdraw {amount_display} {withdraw_token.symbol} from BENQI",
                    tx_type="lending_withdraw",
                )
                transactions.append(withdraw_tx)

                total_gas = sum(tx.gas_estimate for tx in transactions)

                action_bundle = ActionBundle(
                    intent_type=IntentType.WITHDRAW.value,
                    transactions=[tx.to_dict() for tx in transactions],
                    metadata={
                        "protocol": intent.protocol,
                        "comptroller_address": benqi_adapter.comptroller_address,
                        "qi_token_address": withdraw_market.qi_token_address,
                        "withdraw_token": withdraw_token.to_dict(),
                        "withdraw_amount": amount_display,
                        "withdraw_all": intent.withdraw_all,
                        "chain": self.chain,
                    },
                )

                result.action_bundle = action_bundle
                result.transactions = transactions
                result.total_gas_estimate = total_gas
                result.warnings = warnings

                logger.info(
                    f"Compiled WITHDRAW: {withdraw_token.symbol}, all={intent.withdraw_all}, {len(transactions)} txs, {total_gas} gas (BENQI)"
                )

            # =================================================================
            # UNSUPPORTED PROTOCOL
            # =================================================================
            else:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"Unsupported lending protocol: {intent.protocol}. Supported: aave_v3, morpho, morpho_blue, spark, pendle, compound_v3, benqi",
                    intent_id=intent.intent_id,
                )

        except Exception as e:
            logger.exception(f"Failed to compile WITHDRAW intent: {e}")
            result.status = CompilationStatus.FAILED
            result.error = str(e)

        return result

    def _compile_ensure_balance(self, intent: Any) -> CompilationResult:
        """Compile an ENSURE_BALANCE meta-intent by resolving it first.

        EnsureBalanceIntent is a meta-intent that resolves to either a
        HoldIntent or BridgeIntent depending on current balances. If the
        gateway client is available, the target chain balance is fetched
        automatically. Otherwise, the caller must resolve the intent before
        compilation.

        Args:
            intent: EnsureBalanceIntent to compile

        Returns:
            CompilationResult from compiling the resolved intent
        """
        from .ensure_balance import EnsureBalanceIntent

        if not isinstance(intent, EnsureBalanceIntent):
            return CompilationResult(
                status=CompilationStatus.FAILED,
                error="Expected EnsureBalanceIntent",
                intent_id=getattr(intent, "intent_id", ""),
            )

        # Try to resolve using gateway balances if available
        if self._gateway_client is not None:
            try:
                token_info = self._resolve_token(intent.token, intent.target_chain)
                if token_info is None:
                    return CompilationResult(
                        status=CompilationStatus.FAILED,
                        error=f"Cannot resolve token '{intent.token}' on {intent.target_chain}",
                        intent_id=intent.intent_id,
                    )

                # Native tokens (ETH, MATIC, etc.) cannot be queried via
                # query_erc20_balance — fail fast until a native balance RPC exists
                if token_info.is_native:
                    return CompilationResult(
                        status=CompilationStatus.FAILED,
                        error=(
                            "Gateway auto-resolution does not yet support native-token balances. "
                            "Resolve EnsureBalanceIntent manually or use the wrapped token symbol "
                            f"(e.g., WETH instead of ETH) on {intent.target_chain}."
                        ),
                        intent_id=intent.intent_id,
                    )

                target_balance = Decimal("0")
                # Note: chain_balances is empty because the compiler is single-chain
                # scoped and cannot enumerate other configured chains. This means
                # auto-resolution only succeeds when target chain has sufficient balance
                # (producing HoldIntent). Cross-chain bridging requires the caller to
                # resolve the intent manually with multi-chain balance data.
                chain_balances: dict[str, Decimal] = {}

                raw_balance = self._gateway_client.query_erc20_balance(
                    chain=intent.target_chain,
                    token_address=token_info.address,
                    wallet_address=self.wallet_address,
                )
                if raw_balance is None:
                    raise RuntimeError(f"Gateway balance query failed for {intent.token} on {intent.target_chain}")
                target_balance = Decimal(raw_balance) / Decimal(10**token_info.decimals)

                resolved = intent.resolve(target_balance, chain_balances)
                return self.compile(resolved)  # type: ignore[arg-type]
            except Exception as e:  # noqa: BLE001 - best-effort gateway resolution; falls back to manual resolution
                logger.warning("Failed to auto-resolve EnsureBalanceIntent via gateway: %s", e)

        return CompilationResult(
            status=CompilationStatus.FAILED,
            error=(
                "EnsureBalanceIntent must be resolved before compilation. "
                "Call intent.resolve(target_balance, chain_balances) to convert "
                "to a HoldIntent or BridgeIntent, then compile the result."
            ),
            intent_id=intent.intent_id,
        )

    def _compile_hold(self, intent: HoldIntent) -> CompilationResult:
        """Compile a HOLD intent (no-op).

        A HOLD intent produces an empty ActionBundle with no transactions.

        Args:
            intent: HoldIntent to compile

        Returns:
            CompilationResult with empty ActionBundle
        """
        action_bundle = ActionBundle(
            intent_type=IntentType.HOLD.value,
            transactions=[],
            metadata={
                "reason": intent.reason,
            },
        )

        return CompilationResult(
            status=CompilationStatus.SUCCESS,
            action_bundle=action_bundle,
            transactions=[],
            total_gas_estimate=0,
            intent_id=intent.intent_id,
        )

    def _compile_perp_open(self, intent: PerpOpenIntent) -> CompilationResult:
        """Compile a PERP_OPEN intent into an ActionBundle.

        Routes to protocol-specific adapter based on intent.protocol:
        - "drift": Drift Protocol on Solana (via DriftAdapter)
        - "gmx_v2": GMX V2 on Arbitrum/Avalanche (via GMXv2Adapter)

        Args:
            intent: PerpOpenIntent to compile

        Returns:
            CompilationResult with perp open ActionBundle
        """
        protocol = intent.protocol.lower()
        if protocol == "drift":
            return self._compile_drift_perp_open(intent)

        # Fail explicitly for unsupported perp protocols on Solana
        if self._is_solana_chain():
            return CompilationResult(
                status=CompilationStatus.FAILED,
                intent_id=intent.intent_id,
                error=f"Protocol '{intent.protocol}' is not supported for PERP_OPEN on Solana. Supported: drift",
            )

        from ..connectors import GMXv2Adapter, GMXv2Config

        result = CompilationResult(
            status=CompilationStatus.SUCCESS,
            intent_id=intent.intent_id,
        )
        transactions: list[TransactionData] = []
        warnings: list[str] = []

        try:
            # Step 1: Validate chain (GMX v2 only supports arbitrum and avalanche)
            if self.chain not in ["arbitrum", "avalanche"]:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"GMX v2 not supported on chain: {self.chain}",
                    intent_id=intent.intent_id,
                )

            # Step 2: Create GMX adapter
            slippage_bps = int(intent.max_slippage * 10000)
            gmx_config = GMXv2Config(
                chain=self.chain,
                wallet_address=self.wallet_address,
                default_slippage_bps=slippage_bps,
            )
            adapter = GMXv2Adapter(gmx_config)

            # Step 3: Calculate acceptable price
            # For longs: max price willing to pay (price * (1 + slippage))
            # For shorts: min price willing to accept (price * (1 - slippage))
            # We'll calculate based on current price estimate from intent size_usd
            acceptable_price = None  # Let adapter use default max/min
            if intent.is_long:
                acceptable_price = Decimal(10**30)  # Max uint for long
            else:
                acceptable_price = Decimal("0")  # Min for short

            # Step 3.5: Validate collateral amount is not chained
            if intent.collateral_amount == "all":
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error="collateral_amount='all' must be resolved before compilation. Use Intent.set_resolved_amount() to resolve chained amounts.",
                    intent_id=intent.intent_id,
                )

            # Step 4: Build position open order
            order_result = adapter.open_position(
                market=intent.market,
                collateral_token=intent.collateral_token,
                collateral_amount=intent.collateral_amount,  # type: ignore[arg-type]  # Validated above
                size_delta_usd=intent.size_usd,
                is_long=intent.is_long,
                acceptable_price=acceptable_price,
            )

            if not order_result.success:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=order_result.error or "Failed to create position order",
                    intent_id=intent.intent_id,
                )

            # Step 5: Create transaction data using GMX V2 SDK
            # Use the real SDK to build calldata with proper ABI encoding
            from ..connectors.gmx_v2 import GMX_V2_MARKETS, GMX_V2_TOKENS, GMXV2SDK, GMXV2OrderParams

            # Get RPC URL via centralized resolver
            rpc_url = self._get_chain_rpc_url()
            if not rpc_url:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"No RPC URL available for GMX V2. Set ALMANAK_{self.chain.upper()}_RPC_URL, RPC_URL, or ALCHEMY_API_KEY.",
                    intent_id=intent.intent_id,
                )

            # Initialize SDK
            sdk = GMXV2SDK(rpc_url, chain=self.chain)

            # Resolve market address
            market_address = GMX_V2_MARKETS.get(self.chain, {}).get(intent.market)
            if not market_address:
                try:
                    market_address = sdk.get_market_address(intent.market)
                except ValueError:
                    return CompilationResult(
                        status=CompilationStatus.FAILED,
                        error=f"Unknown market: {intent.market}",
                        intent_id=intent.intent_id,
                    )

            # Resolve collateral token address
            collateral_token_upper = intent.collateral_token.upper()
            collateral_address = GMX_V2_TOKENS.get(self.chain, {}).get(collateral_token_upper)
            if not collateral_address:
                if intent.collateral_token.startswith("0x"):
                    collateral_address = intent.collateral_token
                else:
                    return CompilationResult(
                        status=CompilationStatus.FAILED,
                        error=f"Unknown collateral token: {intent.collateral_token}",
                        intent_id=intent.intent_id,
                    )

            # Calculate collateral in wei
            collateral_decimals = 18 if collateral_token_upper in ["WETH", "ETH"] else 6
            collateral_amount_decimal: Decimal = intent.collateral_amount  # type: ignore[assignment]
            collateral_wei = int(collateral_amount_decimal * Decimal(10**collateral_decimals))

            # Calculate size in USD (GMX uses 30 decimals for USD)
            size_delta_usd = int(intent.size_usd * Decimal(10**30))

            # Calculate acceptable price (GMX uses 30 decimals)
            acceptable_price_wei = int(acceptable_price)

            # Get dynamic execution fee
            execution_fee = sdk.get_execution_fee(order_type="increase")

            # Build order parameters
            order_params = GMXV2OrderParams(
                from_address=self.wallet_address,
                market=market_address,
                initial_collateral_token=collateral_address,
                initial_collateral_delta_amount=collateral_wei,
                size_delta_usd=size_delta_usd,
                is_long=intent.is_long,
                acceptable_price=acceptable_price_wei,
                execution_fee=execution_fee,
            )

            # Build the multicall transaction with real calldata
            tx_data = sdk.build_increase_order_multicall(order_params)

            # Step 5.5: Prepend ERC-20 approval for collateral token.
            # ExchangeRouter.sendTokens() delegates to Router.pluginTransfer(),
            # which calls IERC20.safeTransferFrom() — so the Router is the msg.sender
            # that needs the allowance, NOT the ExchangeRouter.
            # Native tokens (WETH/ETH) are sent as msg.value via sendWnt(), no approval needed.
            is_native_collateral = collateral_token_upper in ("WETH", "ETH", "WAVAX", "AVAX")
            if not is_native_collateral and collateral_wei > 0:
                approve_txs = self._build_approve_tx(
                    token_address=collateral_address,
                    spender=sdk.ROUTER_ADDRESS,
                    amount=collateral_wei,
                )
                transactions.extend(approve_txs)

            open_tx = TransactionData(
                to=tx_data.to,
                value=tx_data.value,
                data=tx_data.data,
                gas_estimate=tx_data.gas_estimate,
                description=(
                    f"Open {'LONG' if intent.is_long else 'SHORT'} {intent.market} position: ${intent.size_usd} size, {intent.collateral_amount} collateral"
                ),
                tx_type="perp_open",
            )
            transactions.append(open_tx)

            # Step 6: Build ActionBundle
            total_gas = sum(tx.gas_estimate for tx in transactions)

            action_bundle = ActionBundle(
                intent_type=IntentType.PERP_OPEN.value,
                transactions=[tx.to_dict() for tx in transactions],
                metadata={
                    "protocol": intent.protocol,
                    "market": intent.market,
                    "collateral_token": intent.collateral_token,
                    "collateral_amount": str(intent.collateral_amount),
                    "size_usd": str(intent.size_usd),
                    "is_long": intent.is_long,
                    "leverage": str(intent.leverage),
                    "max_slippage": str(intent.max_slippage),
                    "order_key": order_result.order_key,
                    "chain": self.chain,
                },
            )

            result.action_bundle = action_bundle
            result.transactions = transactions
            result.total_gas_estimate = total_gas
            result.warnings = warnings

            logger.info(
                f"Compiled PERP_OPEN intent: {'LONG' if intent.is_long else 'SHORT'} {intent.market}, ${intent.size_usd} size, {len(transactions)} txs, {total_gas} gas"
            )

        except Exception as e:
            logger.exception(f"Failed to compile PERP_OPEN intent: {e}")
            result.status = CompilationStatus.FAILED
            result.error = str(e)

        return result

    def _compile_perp_close(self, intent: PerpCloseIntent) -> CompilationResult:
        """Compile a PERP_CLOSE intent into an ActionBundle.

        Routes to protocol-specific adapter based on intent.protocol:
        - "drift": Drift Protocol on Solana (via DriftAdapter)
        - "gmx_v2": GMX V2 on Arbitrum/Avalanche (via GMXv2Adapter)

        Args:
            intent: PerpCloseIntent to compile

        Returns:
            CompilationResult with perp close ActionBundle
        """
        protocol = intent.protocol.lower()
        if protocol == "drift":
            return self._compile_drift_perp_close(intent)

        # Fail explicitly for unsupported perp protocols on Solana
        if self._is_solana_chain():
            return CompilationResult(
                status=CompilationStatus.FAILED,
                intent_id=intent.intent_id,
                error=f"Protocol '{intent.protocol}' is not supported for PERP_CLOSE on Solana. Supported: drift",
            )

        from ..connectors import GMXv2Adapter, GMXv2Config

        result = CompilationResult(
            status=CompilationStatus.SUCCESS,
            intent_id=intent.intent_id,
        )
        transactions: list[TransactionData] = []
        warnings: list[str] = []

        try:
            # Step 1: Validate chain
            if self.chain not in ["arbitrum", "avalanche"]:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"GMX v2 not supported on chain: {self.chain}",
                    intent_id=intent.intent_id,
                )

            # Step 2: Create GMX adapter
            slippage_bps = int(intent.max_slippage * 10000)
            gmx_config = GMXv2Config(
                chain=self.chain,
                wallet_address=self.wallet_address,
                default_slippage_bps=slippage_bps,
            )
            adapter = GMXv2Adapter(gmx_config)

            # Step 3: Calculate acceptable price for closing
            # For closing longs: min price to sell at (price * (1 - slippage))
            # For closing shorts: max price to buy back at (price * (1 + slippage))
            acceptable_price = None
            if intent.is_long:
                acceptable_price = Decimal("0")  # Min price for closing long
            else:
                acceptable_price = Decimal(10**30)  # Max price for closing short

            # Step 4: Initialize SDK and resolve addresses (needed before adapter call
            # so we can query on-chain position size for full closes — VIB-1946)
            from ..connectors.gmx_v2 import GMX_V2_MARKETS, GMX_V2_TOKENS, GMXV2SDK, GMXV2OrderParams

            # Get RPC URL via centralized resolver
            rpc_url = self._get_chain_rpc_url()
            if not rpc_url:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"No RPC URL available for GMX V2. Set ALMANAK_{self.chain.upper()}_RPC_URL, RPC_URL, or ALCHEMY_API_KEY.",
                    intent_id=intent.intent_id,
                )

            # Initialize SDK
            sdk = GMXV2SDK(rpc_url, chain=self.chain)

            # Resolve market address
            market_address = GMX_V2_MARKETS.get(self.chain, {}).get(intent.market)
            if not market_address:
                try:
                    market_address = sdk.get_market_address(intent.market)
                except ValueError:
                    return CompilationResult(
                        status=CompilationStatus.FAILED,
                        error=f"Unknown market: {intent.market}",
                        intent_id=intent.intent_id,
                    )

            # Resolve collateral token address
            collateral_token_upper = intent.collateral_token.upper()
            collateral_address = GMX_V2_TOKENS.get(self.chain, {}).get(collateral_token_upper)
            if not collateral_address:
                if intent.collateral_token.startswith("0x"):
                    collateral_address = intent.collateral_token
                else:
                    return CompilationResult(
                        status=CompilationStatus.FAILED,
                        error=f"Unknown collateral token: {intent.collateral_token}",
                        intent_id=intent.intent_id,
                    )

            # Step 5: Resolve position size in USD (GMX uses 30 decimals)
            # GMX V2 validates sizeDeltaUsd <= position.sizeInUsd — max uint and any
            # overshoot burns keeper fees without closing (VIB-1946).
            resolved_size_usd = intent.size_usd
            if intent.size_usd:
                size_delta_usd = int(intent.size_usd * Decimal(10**30))
            else:
                queried_size = self._get_gmx_position_size_onchain(
                    sdk, market_address, collateral_address, intent.is_long
                )
                if queried_size is None:
                    return CompilationResult(
                        status=CompilationStatus.FAILED,
                        error=(
                            "Cannot close full GMX V2 position: unable to read position size on-chain. "
                            "Either specify size_usd explicitly or ensure RPC/API connectivity. "
                            "Refusing to guess — incorrect sizes burn keeper execution fees."
                        ),
                        intent_id=intent.intent_id,
                    )
                size_delta_usd = queried_size
                # Convert on-chain size (30-decimal int) to Decimal for adapter
                resolved_size_usd = Decimal(size_delta_usd) / Decimal(10**30)

            # Step 6: Build position close order via adapter (with resolved size)
            order_result = adapter.close_position(
                market=intent.market,
                collateral_token=intent.collateral_token,
                is_long=intent.is_long,
                size_delta_usd=resolved_size_usd,
                acceptable_price=acceptable_price,
            )

            if not order_result.success:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=order_result.error or "Failed to create close order",
                    intent_id=intent.intent_id,
                )

            # Calculate acceptable price (GMX uses 30 decimals)
            acceptable_price_wei = int(acceptable_price)

            # Get dynamic execution fee
            execution_fee = sdk.get_execution_fee(order_type="decrease")

            # Build order parameters
            order_params = GMXV2OrderParams(
                from_address=self.wallet_address,
                market=market_address,
                initial_collateral_token=collateral_address,
                initial_collateral_delta_amount=0,  # No additional collateral for decrease
                size_delta_usd=size_delta_usd,
                is_long=intent.is_long,
                acceptable_price=acceptable_price_wei,
                execution_fee=execution_fee,
            )

            # Build the decrease order transaction with real calldata
            tx_data = sdk.build_decrease_order_multicall(order_params)

            size_desc = f"${intent.size_usd}" if intent.size_usd else "full position"
            close_tx = TransactionData(
                to=tx_data.to,
                value=tx_data.value,
                data=tx_data.data,
                gas_estimate=tx_data.gas_estimate,
                description=(f"Close {'LONG' if intent.is_long else 'SHORT'} {intent.market} position: {size_desc}"),
                tx_type="perp_close",
            )
            transactions.append(close_tx)

            # Step 6: Build ActionBundle
            total_gas = sum(tx.gas_estimate for tx in transactions)

            action_bundle = ActionBundle(
                intent_type=IntentType.PERP_CLOSE.value,
                transactions=[tx.to_dict() for tx in transactions],
                metadata={
                    "protocol": intent.protocol,
                    "market": intent.market,
                    "collateral_token": intent.collateral_token,
                    "is_long": intent.is_long,
                    "size_usd": str(intent.size_usd) if intent.size_usd else None,
                    "close_full_position": intent.close_full_position,
                    "max_slippage": str(intent.max_slippage),
                    "order_key": order_result.order_key,
                    "chain": self.chain,
                },
            )

            result.action_bundle = action_bundle
            result.transactions = transactions
            result.total_gas_estimate = total_gas
            result.warnings = warnings

            logger.info(
                f"Compiled PERP_CLOSE intent: {'LONG' if intent.is_long else 'SHORT'} {intent.market}, {size_desc}, {len(transactions)} txs, {total_gas} gas"
            )

        except Exception as e:
            logger.exception(f"Failed to compile PERP_CLOSE intent: {e}")
            result.status = CompilationStatus.FAILED
            result.error = str(e)

        return result

    def _get_gmx_position_size_onchain(
        self,
        sdk: Any,
        market_address: str,
        collateral_address: str,
        is_long: bool,
    ) -> int | None:
        """Read exact GMX V2 position size from on-chain for close-full-position.

        GMX V2 validates sizeDeltaUsd <= position.sizeInUsd strictly.
        Any overshoot burns keeper fees without closing the position (VIB-1946).

        Args:
            sdk: GMXV2SDK instance (already initialized with RPC)
            market_address: Market contract address
            collateral_address: Collateral token address
            is_long: Position direction

        Returns:
            size_in_usd in 30-decimal int format, or None if query failed.
        """
        from ..connectors.gmx_v2.sdk import PositionQueryError

        try:
            positions = sdk.get_account_positions(self.wallet_address)
        except PositionQueryError as e:
            logger.warning("GMX V2 position query failed: %s", e)
            return None
        except Exception as e:
            logger.warning("Unexpected error querying GMX V2 positions: %s", e)
            return None

        if not positions:
            logger.warning("No GMX V2 positions found for %s", self.wallet_address)
            return None

        # Match position by (market, collateral_token, is_long)
        market_lower = market_address.lower()
        collateral_lower = collateral_address.lower()
        for pos in positions:
            if (
                pos.get("market", "").lower() == market_lower
                and pos.get("collateral_token", "").lower() == collateral_lower
                and pos.get("is_long") == is_long
                and pos.get("size_in_usd", 0) > 0
            ):
                size_in_usd = pos["size_in_usd"]  # Already in 30 decimals from chain
                logger.info(
                    "Read on-chain GMX V2 position size: %s (30-decimal) for market=%s is_long=%s",
                    size_in_usd,
                    market_address,
                    is_long,
                )
                return int(size_in_usd)

        logger.warning(
            "No matching GMX V2 position found for market=%s collateral=%s is_long=%s",
            market_address,
            collateral_address,
            is_long,
        )
        return None

    # ==========================================================================
    # DRIFT PERPS (Solana)
    # ==========================================================================

    def _compile_drift_perp_open(self, intent: PerpOpenIntent) -> CompilationResult:
        """Compile a PERP_OPEN intent using Drift for Solana chains."""
        result = CompilationResult(
            status=CompilationStatus.SUCCESS,
            intent_id=intent.intent_id,
        )
        try:
            if not self._is_solana_chain():
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error="Drift is only supported on Solana",
                    intent_id=intent.intent_id,
                )

            # Validate collateral_amount is not chained
            if intent.collateral_amount == "all":
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error="collateral_amount='all' must be resolved before compilation.",
                    intent_id=intent.intent_id,
                )

            adapter = self._get_drift_adapter()
            bundle = adapter.compile_perp_open_intent(intent, price_oracle=self.price_oracle)

            if bundle.metadata.get("error"):
                result.status = CompilationStatus.FAILED
                result.error = bundle.metadata["error"]
            else:
                result.action_bundle = bundle

        except Exception as e:
            logger.exception(f"Drift perp open compilation failed: {e}")
            result.status = CompilationStatus.FAILED
            result.error = str(e)
        return result

    def _compile_drift_perp_close(self, intent: PerpCloseIntent) -> CompilationResult:
        """Compile a PERP_CLOSE intent using Drift for Solana chains."""
        result = CompilationResult(
            status=CompilationStatus.SUCCESS,
            intent_id=intent.intent_id,
        )
        try:
            if not self._is_solana_chain():
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error="Drift is only supported on Solana",
                    intent_id=intent.intent_id,
                )

            adapter = self._get_drift_adapter()
            bundle = adapter.compile_perp_close_intent(intent, price_oracle=self.price_oracle)

            if bundle.metadata.get("error"):
                result.status = CompilationStatus.FAILED
                result.error = bundle.metadata["error"]
            else:
                result.action_bundle = bundle

        except Exception as e:
            logger.exception(f"Drift perp close compilation failed: {e}")
            result.status = CompilationStatus.FAILED
            result.error = str(e)
        return result

    def _compile_flash_loan(self, intent: FlashLoanIntent) -> CompilationResult:
        """Compile a FLASH_LOAN intent into an ActionBundle.

        This method:
        1. Validates the provider (Aave or Balancer)
        2. Resolves the flash loan token
        3. Compiles nested callback intents
        4. Encodes callbacks as flash loan params
        5. Builds the flash loan transaction

        For atomic arbitrage strategies, the flash loan must be repaid within
        the same transaction. The callback_intents should return sufficient
        tokens to repay the loan plus fees (0.09% for Aave, 0% for Balancer).

        Args:
            intent: FlashLoanIntent to compile

        Returns:
            CompilationResult with flash loan ActionBundle
        """
        result = CompilationResult(
            status=CompilationStatus.SUCCESS,
            intent_id=intent.intent_id,
        )
        transactions: list[TransactionData] = []
        warnings: list[str] = []

        try:
            # Step 0: Check wallet can handle flash loan callbacks
            # Flash loans require a contract wallet (e.g., Safe) because the lending
            # protocol calls back into the recipient to execute callback operations.
            # EOA wallets have no bytecode and cannot receive these callbacks.
            # Skip check for zero-address sentinel (used by permission discovery synthetic intents).
            _zero_addr = "0x" + "0" * 40
            is_contract = True if self.wallet_address == _zero_addr else self._is_wallet_contract()
            if is_contract is False:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=(
                        "Flash loans require a receiver contract that implements the provider callback "
                        "(e.g., Balancer's receiveFlashLoan or Aave's executeOperation). "
                        f"Wallet {self.wallet_address} is an EOA (no bytecode). "
                        "Flash-loan providers call back into the recipient during the same transaction, "
                        "which EOAs cannot handle. Deploy a compatible flash-loan receiver contract."
                    ),
                    intent_id=intent.intent_id,
                )
            elif is_contract is None:
                warnings.append(
                    "Could not verify wallet bytecode (no RPC available). "
                    "Flash loans will revert if the wallet is an EOA (not a contract)."
                )

            # Step 1: Validate and resolve provider
            if intent.provider == "auto":
                # Use FlashLoanSelector to find optimal provider
                # Lazy import to avoid circular dependency
                from ..connectors.flash_loan import (
                    FlashLoanSelector,
                    NoProviderAvailableError,
                )

                try:
                    selector = FlashLoanSelector(chain=self.chain)
                    selection_result = selector.select_provider(
                        token=intent.token,
                        amount=intent.amount,
                        priority="fee",  # Prefer lower fees (Balancer is zero)
                    )
                    effective_provider = selection_result.provider
                    if selection_result.selection_reasoning:
                        logger.info(f"Flash loan provider auto-selected: {selection_result.selection_reasoning}")
                except NoProviderAvailableError as e:
                    return CompilationResult(
                        status=CompilationStatus.FAILED,
                        error=f"No flash loan provider available: {e}",
                        intent_id=intent.intent_id,
                    )
            else:
                effective_provider = intent.provider

            if effective_provider not in ("aave", "balancer", "morpho"):
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"Unsupported flash loan provider: {intent.provider}. Supported providers: aave, balancer, morpho.",
                    intent_id=intent.intent_id,
                )

            # Step 2: Resolve flash loan token
            token_info = self._resolve_token(intent.token)
            if token_info is None:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"Unknown flash loan token: {intent.token}",
                    intent_id=intent.intent_id,
                )

            # Step 3: Calculate flash loan amount in wei
            amount_wei = int(intent.amount * Decimal(10**token_info.decimals))

            # Step 4: Compile callback intents to get their transactions
            # For flash loan callbacks, amount='all' means "use the full output from the
            # previous callback." We estimate this at compile time using the price oracle,
            # since the exact amount is only known on-chain at execution time.
            callback_transactions: list[TransactionData] = []
            callback_gas_total = 0
            # Seed with flash loan's own borrow amount/token so callback 1 can use amount='all'
            prev_output_amount: Decimal | None = intent.amount
            prev_output_token: str | None = intent.token

            for i, callback_intent in enumerate(intent.callback_intents):
                # Resolve amount='all' using estimated output from previous callback
                resolved_intent: AnyIntent = callback_intent
                if (
                    hasattr(callback_intent, "amount")
                    and callback_intent.amount == "all"
                    and prev_output_amount is not None
                ):
                    # Validate token compatibility: the callback's input token must match
                    # the previous callback's output token to use amount='all'.
                    # Resolve both tokens to addresses to handle symbol/address/alias equivalence.
                    callback_from = getattr(callback_intent, "from_token", None)
                    if callback_from and prev_output_token:
                        resolved_from = self._resolve_token(callback_from)
                        resolved_prev = self._resolve_token(prev_output_token)
                        if (
                            resolved_from
                            and resolved_prev
                            and resolved_from.address.lower() != resolved_prev.address.lower()
                        ):
                            return CompilationResult(
                                status=CompilationStatus.FAILED,
                                error=(
                                    f"Flash loan callback {i + 1}: amount='all' expects token "
                                    f"'{prev_output_token}' (output of previous callback) but "
                                    f"from_token is '{callback_from}'. Use an explicit amount instead."
                                ),
                                intent_id=intent.intent_id,
                            )
                    resolved_intent = Intent.set_resolved_amount(callback_intent, prev_output_amount)
                    logger.info(
                        f"Flash loan callback {i + 1}: resolved amount='all' to "
                        f"{prev_output_amount} {prev_output_token} (estimated from previous callback)"
                    )

                callback_result = self.compile(resolved_intent)
                if callback_result.status != CompilationStatus.SUCCESS:
                    return CompilationResult(
                        status=CompilationStatus.FAILED,
                        error=f"Failed to compile callback intent {i + 1}: {callback_result.error}",
                        intent_id=intent.intent_id,
                    )
                if callback_result.transactions:
                    callback_transactions.extend(callback_result.transactions)
                    callback_gas_total += callback_result.total_gas_estimate or 0

                # Estimate output for next callback's amount='all' resolution
                prev_output_amount, prev_output_token = self._estimate_callback_output(
                    resolved_intent, prev_output_amount, prev_output_token
                )

            # Step 5: Encode callback transactions as params
            callback_params = self._encode_flash_loan_callbacks(callback_transactions)

            # Step 6: Build flash loan transaction based on provider
            if effective_provider == "balancer":
                # Use Balancer Vault for flash loans (zero fees!)
                flash_loan_result = self._build_balancer_flash_loan(
                    token_info=token_info,
                    amount_wei=amount_wei,
                    callback_params=callback_params,
                    callback_gas_total=callback_gas_total,
                )
            elif effective_provider == "morpho":
                # Use Morpho Blue for flash loans (zero fees!)
                flash_loan_result = self._build_morpho_flash_loan(
                    token_info=token_info,
                    amount_wei=amount_wei,
                    callback_params=callback_params,
                    callback_gas_total=callback_gas_total,
                )
            else:
                # Use Aave V3 for flash loans (0.09% fee)
                flash_loan_result = self._build_aave_flash_loan(
                    token_info=token_info,
                    amount_wei=amount_wei,
                    callback_params=callback_params,
                    callback_gas_total=callback_gas_total,
                )

            if flash_loan_result.get("error"):
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=flash_loan_result["error"],
                    intent_id=intent.intent_id,
                )

            transactions.append(flash_loan_result["transaction"])

            # Step 7: Build ActionBundle
            total_gas = sum(tx.gas_estimate for tx in transactions)

            action_bundle = ActionBundle(
                intent_type=IntentType.FLASH_LOAN.value,
                transactions=[tx.to_dict() for tx in transactions],
                metadata={
                    "provider": effective_provider,
                    "pool_address": flash_loan_result["pool_address"],
                    "token": token_info.to_dict(),
                    "amount": str(amount_wei),
                    "amount_formatted": str(intent.amount),
                    "premium_bps": flash_loan_result["premium_bps"],
                    "premium_amount": str(flash_loan_result["premium_amount"]),
                    "total_repay": str(flash_loan_result["total_repay"]),
                    "callback_count": len(intent.callback_intents),
                    "callback_gas_estimate": callback_gas_total,
                    "chain": self.chain,
                },
            )

            result.action_bundle = action_bundle
            result.transactions = transactions
            result.total_gas_estimate = total_gas
            result.warnings = warnings

            logger.info(
                f"Compiled FLASH_LOAN intent: {intent.amount} {intent.token} via {effective_provider}, {len(intent.callback_intents)} callbacks, {len(transactions)} txs, {total_gas} gas"
            )

        except Exception as e:
            logger.exception(f"Failed to compile FLASH_LOAN intent: {e}")
            result.status = CompilationStatus.FAILED
            result.error = str(e)

        return result

    def _estimate_callback_output(
        self,
        callback_intent: AnyIntent,
        prev_output_amount: Decimal | None,
        prev_output_token: str | None,
    ) -> tuple[Decimal | None, str | None]:
        """Estimate the output token and amount from a compiled callback intent.

        Used by _compile_flash_loan to resolve amount='all' in subsequent callbacks.
        The estimate is based on the price oracle and is approximate -- the actual
        amount is only known on-chain at execution time.

        Args:
            callback_intent: The callback intent (after amount='all' resolution)
            prev_output_amount: Previous callback's estimated output amount
            prev_output_token: Previous callback's output token symbol

        Returns:
            Tuple of (estimated_output_amount, output_token_symbol).
            Returns (None, None) for unsupported intent types.

        Raises:
            ValueError: If price data is unavailable for token resolution.
        """
        if not isinstance(callback_intent, SwapIntent):
            intent_type = getattr(callback_intent, "intent_type", "unknown")
            logger.warning(
                f"Cannot estimate output for non-swap callback intent type {intent_type}. "
                f"Subsequent amount='all' callbacks will fail to resolve."
            )
            return None, None

        from_token_info = self._resolve_token(callback_intent.from_token)
        to_token_info = self._resolve_token(callback_intent.to_token)
        if not from_token_info or not to_token_info:
            raise ValueError(
                f"Cannot resolve tokens for callback output estimate: "
                f"{callback_intent.from_token} -> {callback_intent.to_token}"
            )

        # Determine input amount in wei
        amount_in_wei: int | None = None
        if callback_intent.amount_usd is not None:
            amount_in_wei = self._usd_to_token_amount(callback_intent.amount_usd, from_token_info)
        elif callback_intent.amount is not None and callback_intent.amount != "all":
            amount_decimal = (
                callback_intent.amount
                if isinstance(callback_intent.amount, Decimal)
                else Decimal(str(callback_intent.amount))
            )
            amount_in_wei = int(amount_decimal * Decimal(10**from_token_info.decimals))

        if amount_in_wei is not None:
            expected_out_wei = self._calculate_expected_output(amount_in_wei, from_token_info, to_token_info)
            return (
                Decimal(str(expected_out_wei)) / Decimal(10**to_token_info.decimals),
                callback_intent.to_token,
            )
        return None, None

    def _compile_stake_intent(self, intent: StakeIntent) -> CompilationResult:
        """Compile a STAKE intent into an ActionBundle.

        Routes to the appropriate staking adapter based on protocol:
        - 'lido': Uses LidoAdapter for ETH staking (stETH/wstETH)
        - 'ethena': Uses EthenaAdapter for USDe staking (sUSDe)

        Args:
            intent: StakeIntent to compile

        Returns:
            CompilationResult with stake ActionBundle
        """
        from ..connectors import EthenaAdapter, EthenaConfig, LidoAdapter, LidoConfig

        result = CompilationResult(
            status=CompilationStatus.SUCCESS,
            intent_id=intent.intent_id,
        )
        warnings: list[str] = []

        try:
            protocol = intent.protocol.lower()

            # Validate chained amount
            if intent.amount == "all":
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error="amount='all' must be resolved before compilation. Use Intent.set_resolved_amount() to resolve chained amounts.",
                    intent_id=intent.intent_id,
                )

            # Route to appropriate adapter based on protocol
            action_bundle: ActionBundle
            if protocol == "lido":
                # Validate chain - Lido only on Ethereum mainnet
                if self.chain not in ["ethereum", "mainnet"]:
                    return CompilationResult(
                        status=CompilationStatus.FAILED,
                        error=f"Lido staking only supported on Ethereum mainnet, got: {self.chain}",
                        intent_id=intent.intent_id,
                    )

                lido_config = LidoConfig(
                    chain=self.chain,
                    wallet_address=self.wallet_address,
                )
                lido_adapter = LidoAdapter(lido_config)
                action_bundle = lido_adapter.compile_stake_intent(intent)

            elif protocol == "ethena":
                # Validate chain - Ethena only on Ethereum mainnet
                if self.chain not in ["ethereum", "mainnet"]:
                    return CompilationResult(
                        status=CompilationStatus.FAILED,
                        error=f"Ethena staking only supported on Ethereum mainnet, got: {self.chain}",
                        intent_id=intent.intent_id,
                    )

                ethena_config = EthenaConfig(
                    chain=self.chain,
                    wallet_address=self.wallet_address,
                )
                ethena_adapter = EthenaAdapter(ethena_config)
                action_bundle = ethena_adapter.compile_stake_intent(intent)

            else:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"Unsupported staking protocol: {protocol}. Supported: lido, ethena",
                    intent_id=intent.intent_id,
                )

            # Convert ActionBundle transactions to TransactionData
            transactions: list[TransactionData] = []
            for tx_dict in action_bundle.transactions:
                tx = TransactionData(
                    to=tx_dict.get("to", ""),
                    value=int(tx_dict.get("value", 0)),
                    data=tx_dict.get("data", "0x"),
                    gas_estimate=tx_dict.get("gas_estimate", 0),
                    description=tx_dict.get("description", ""),
                    tx_type=tx_dict.get("tx_type", "stake"),
                )
                transactions.append(tx)

            total_gas = sum(tx.gas_estimate for tx in transactions)

            result.action_bundle = action_bundle
            result.transactions = transactions
            result.total_gas_estimate = total_gas
            result.warnings = warnings

            logger.info(
                f"Compiled STAKE intent: {intent.amount} {intent.token_in} via {protocol}, {len(transactions)} txs, {total_gas} gas"
            )

        except Exception as e:
            logger.exception(f"Failed to compile STAKE intent: {e}")
            result.status = CompilationStatus.FAILED
            result.error = str(e)

        return result

    def _compile_unstake_intent(self, intent: UnstakeIntent) -> CompilationResult:
        """Compile an UNSTAKE intent into an ActionBundle.

        Routes to the appropriate staking adapter based on protocol:
        - 'lido': Uses LidoAdapter for stETH/wstETH unstaking
        - 'ethena': Uses EthenaAdapter for sUSDe unstaking (initiates cooldown)

        Args:
            intent: UnstakeIntent to compile

        Returns:
            CompilationResult with unstake ActionBundle
        """
        from ..connectors import EthenaAdapter, EthenaConfig, LidoAdapter, LidoConfig

        result = CompilationResult(
            status=CompilationStatus.SUCCESS,
            intent_id=intent.intent_id,
        )
        warnings: list[str] = []

        try:
            protocol = intent.protocol.lower()

            # Validate chained amount
            if intent.amount == "all":
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error="amount='all' must be resolved before compilation. Use Intent.set_resolved_amount() to resolve chained amounts.",
                    intent_id=intent.intent_id,
                )

            # Route to appropriate adapter based on protocol
            action_bundle: ActionBundle
            if protocol == "lido":
                # Validate chain - Lido only on Ethereum mainnet
                if self.chain not in ["ethereum", "mainnet"]:
                    return CompilationResult(
                        status=CompilationStatus.FAILED,
                        error=f"Lido unstaking only supported on Ethereum mainnet, got: {self.chain}",
                        intent_id=intent.intent_id,
                    )

                lido_config = LidoConfig(
                    chain=self.chain,
                    wallet_address=self.wallet_address,
                )
                lido_adapter = LidoAdapter(lido_config)
                action_bundle = lido_adapter.compile_unstake_intent(intent)

            elif protocol == "ethena":
                # Validate chain - Ethena only on Ethereum mainnet
                if self.chain not in ["ethereum", "mainnet"]:
                    return CompilationResult(
                        status=CompilationStatus.FAILED,
                        error=f"Ethena unstaking only supported on Ethereum mainnet, got: {self.chain}",
                        intent_id=intent.intent_id,
                    )

                ethena_config = EthenaConfig(
                    chain=self.chain,
                    wallet_address=self.wallet_address,
                )
                ethena_adapter = EthenaAdapter(ethena_config)
                action_bundle = ethena_adapter.compile_unstake_intent(intent)

            else:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"Unsupported unstaking protocol: {protocol}. Supported: lido, ethena",
                    intent_id=intent.intent_id,
                )

            # Convert ActionBundle transactions to TransactionData
            transactions: list[TransactionData] = []
            for tx_dict in action_bundle.transactions:
                tx = TransactionData(
                    to=tx_dict.get("to", ""),
                    value=int(tx_dict.get("value", 0)),
                    data=tx_dict.get("data", "0x"),
                    gas_estimate=tx_dict.get("gas_estimate", 0),
                    description=tx_dict.get("description", ""),
                    tx_type=tx_dict.get("tx_type", "unstake"),
                )
                transactions.append(tx)

            total_gas = sum(tx.gas_estimate for tx in transactions)

            result.action_bundle = action_bundle
            result.transactions = transactions
            result.total_gas_estimate = total_gas
            result.warnings = warnings

            logger.info(
                f"Compiled UNSTAKE intent: {intent.amount} {intent.token_in} via {protocol}, {len(transactions)} txs, {total_gas} gas"
            )

        except Exception as e:
            logger.exception(f"Failed to compile UNSTAKE intent: {e}")
            result.status = CompilationStatus.FAILED
            result.error = str(e)

        return result

    # =========================================================================
    # Prediction Market Intent Compilation
    # =========================================================================

    def _compile_prediction_buy(self, intent: PredictionBuyIntent) -> CompilationResult:
        """Compile a PREDICTION_BUY intent into an ActionBundle.

        This method delegates to the PolymarketAdapter for compilation.
        The resulting ActionBundle contains CLOB order data in metadata,
        not on-chain transactions (buy orders are submitted off-chain).

        Args:
            intent: PredictionBuyIntent to compile

        Returns:
            CompilationResult with prediction buy ActionBundle
        """
        # Check if adapter is available
        if self._polymarket_adapter is None:
            if self.chain.lower() != "polygon":
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"Prediction market intents are only supported on Polygon, not {self.chain}",
                    intent_id=intent.intent_id,
                )
            # VIB-307: Warn at compile time (not at init) so non-prediction Polygon strategies
            # don't see this warning unless they actually attempt a prediction intent.
            logger.warning(
                "PredictionBuyIntent requires polymarket_config in IntentCompilerConfig. "
                "Provide polymarket_config to enable prediction market intents on Polygon."
            )
            return CompilationResult(
                status=CompilationStatus.FAILED,
                error=(
                    "PolymarketAdapter not initialized. "
                    "Provide polymarket_config in IntentCompilerConfig to enable prediction intents."
                ),
                intent_id=intent.intent_id,
            )

        result = CompilationResult(
            status=CompilationStatus.SUCCESS,
            intent_id=intent.intent_id,
        )

        try:
            # Delegate to PolymarketAdapter
            action_bundle = self._polymarket_adapter.compile_intent(intent)

            # Check if compilation failed (error in metadata)
            if "error" in action_bundle.metadata:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=action_bundle.metadata["error"],
                    intent_id=intent.intent_id,
                )

            # CLOB orders have no on-chain transactions (gas = 0)
            result.action_bundle = action_bundle
            result.transactions = []
            result.total_gas_estimate = 0

            logger.info(
                f"Compiled PREDICTION_BUY: market={intent.market_id}, "
                f"outcome={intent.outcome}, "
                f"amount_usd={intent.amount_usd}, shares={intent.shares}"
            )

        except Exception as e:
            logger.exception(f"Failed to compile PREDICTION_BUY intent: {e}")
            result.status = CompilationStatus.FAILED
            result.error = str(e)

        return result

    def _compile_prediction_sell(self, intent: PredictionSellIntent) -> CompilationResult:
        """Compile a PREDICTION_SELL intent into an ActionBundle.

        This method delegates to the PolymarketAdapter for compilation.
        The resulting ActionBundle contains CLOB order data in metadata,
        not on-chain transactions (sell orders are submitted off-chain).

        Args:
            intent: PredictionSellIntent to compile

        Returns:
            CompilationResult with prediction sell ActionBundle
        """
        # Check if adapter is available
        if self._polymarket_adapter is None:
            if self.chain.lower() != "polygon":
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"Prediction market intents are only supported on Polygon, not {self.chain}",
                    intent_id=intent.intent_id,
                )
            # VIB-307: Warn at compile time (not at init) so non-prediction Polygon strategies
            # don't see this warning unless they actually attempt a prediction intent.
            logger.warning(
                "PredictionSellIntent requires polymarket_config in IntentCompilerConfig. "
                "Provide polymarket_config to enable prediction market intents on Polygon."
            )
            return CompilationResult(
                status=CompilationStatus.FAILED,
                error=(
                    "PolymarketAdapter not initialized. "
                    "Provide polymarket_config in IntentCompilerConfig to enable prediction intents."
                ),
                intent_id=intent.intent_id,
            )

        result = CompilationResult(
            status=CompilationStatus.SUCCESS,
            intent_id=intent.intent_id,
        )

        try:
            # Delegate to PolymarketAdapter
            action_bundle = self._polymarket_adapter.compile_intent(intent)

            # Check if compilation failed (error in metadata)
            if "error" in action_bundle.metadata:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=action_bundle.metadata["error"],
                    intent_id=intent.intent_id,
                )

            # CLOB orders have no on-chain transactions (gas = 0)
            result.action_bundle = action_bundle
            result.transactions = []
            result.total_gas_estimate = 0

            logger.info(
                f"Compiled PREDICTION_SELL: market={intent.market_id}, outcome={intent.outcome}, shares={intent.shares}"
            )

        except Exception as e:
            logger.exception(f"Failed to compile PREDICTION_SELL intent: {e}")
            result.status = CompilationStatus.FAILED
            result.error = str(e)

        return result

    def _compile_prediction_redeem(self, intent: PredictionRedeemIntent) -> CompilationResult:
        """Compile a PREDICTION_REDEEM intent into an ActionBundle.

        This method delegates to the PolymarketAdapter for compilation.
        Unlike buy/sell, redemption is an on-chain CTF transaction that
        converts winning outcome tokens into USDC.

        Args:
            intent: PredictionRedeemIntent to compile

        Returns:
            CompilationResult with prediction redeem ActionBundle
        """
        from ..connectors.polymarket.exceptions import PolymarketMarketNotResolvedError

        # Check if adapter is available
        if self._polymarket_adapter is None:
            if self.chain.lower() != "polygon":
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"Prediction market intents are only supported on Polygon, not {self.chain}",
                    intent_id=intent.intent_id,
                )
            # VIB-307: Warn at compile time (not at init) so non-prediction Polygon strategies
            # don't see this warning unless they actually attempt a prediction intent.
            logger.warning(
                "PredictionRedeemIntent requires polymarket_config in IntentCompilerConfig. "
                "Provide polymarket_config to enable prediction market intents on Polygon."
            )
            return CompilationResult(
                status=CompilationStatus.FAILED,
                error=(
                    "PolymarketAdapter not initialized. "
                    "Provide polymarket_config in IntentCompilerConfig to enable prediction intents."
                ),
                intent_id=intent.intent_id,
            )

        result = CompilationResult(
            status=CompilationStatus.SUCCESS,
            intent_id=intent.intent_id,
        )

        try:
            # Delegate to PolymarketAdapter
            action_bundle = self._polymarket_adapter.compile_intent(intent)

            # Check if compilation failed (error in metadata)
            if "error" in action_bundle.metadata:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=action_bundle.metadata["error"],
                    intent_id=intent.intent_id,
                )

            # Convert ActionBundle transactions to TransactionData objects
            transactions: list[TransactionData] = []
            for tx_dict in action_bundle.transactions:
                tx = TransactionData(
                    to=tx_dict.get("to", ""),
                    value=int(tx_dict.get("value", 0)),
                    data=tx_dict.get("data", ""),
                    gas_estimate=tx_dict.get("gas_estimate", 200_000),
                    description=tx_dict.get("description", "Redeem prediction market positions"),
                    tx_type=tx_dict.get("tx_type", "redeem"),
                )
                transactions.append(tx)

            result.action_bundle = action_bundle
            result.transactions = transactions
            result.total_gas_estimate = sum(tx.gas_estimate for tx in transactions)

            logger.info(
                f"Compiled PREDICTION_REDEEM: market={intent.market_id}, "
                f"outcome={intent.outcome}, txs={len(transactions)}"
            )

        except PolymarketMarketNotResolvedError as e:
            # Re-raise with clear message for unresolved markets
            logger.warning(f"Cannot redeem - market not resolved: {e}")
            result.status = CompilationStatus.FAILED
            result.error = str(e)

        except Exception as e:
            logger.exception(f"Failed to compile PREDICTION_REDEEM intent: {e}")
            result.status = CompilationStatus.FAILED
            result.error = str(e)

        return result

    # =================================================================
    # MetaMorpho Vault Operations
    # =================================================================

    def _compile_vault_deposit(self, intent: VaultDepositIntent) -> CompilationResult:
        """Compile a VAULT_DEPOSIT intent into an ActionBundle.

        This method:
        1. Creates MetaMorpho adapter with gateway client
        2. Queries vault asset address
        3. Resolves asset token for decimals
        4. Builds approve TX for the vault
        5. Builds deposit TX

        Args:
            intent: VaultDepositIntent to compile

        Returns:
            CompilationResult with vault deposit ActionBundle
        """
        result = CompilationResult(
            status=CompilationStatus.SUCCESS,
            intent_id=intent.intent_id,
        )
        transactions: list[TransactionData] = []

        try:
            # Check for chained amount
            if intent.amount == "all":
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error="amount='all' must be resolved before compilation. Use Intent.set_resolved_amount() to resolve chained amounts.",
                    intent_id=intent.intent_id,
                )
            amount_decimal: Decimal = intent.amount  # type: ignore[assignment]
            if amount_decimal <= Decimal("0"):
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error="Vault deposit amount must be positive",
                    intent_id=intent.intent_id,
                )

            if self._gateway_client is None:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error="GatewayClient is required for MetaMorpho vault compilation (on-chain reads).",
                    intent_id=intent.intent_id,
                )

            # Lazy import to avoid circular import
            from ..connectors.morpho_vault.adapter import MetaMorphoAdapter, MetaMorphoConfig

            # Create adapter with gateway client
            vault_config = MetaMorphoConfig(
                chain=self.chain,
                wallet_address=self.wallet_address,
            )
            adapter = MetaMorphoAdapter(
                vault_config,
                gateway_client=self._gateway_client,
                token_resolver=self._token_resolver,
            )

            # Query vault asset address
            asset_address = adapter.sdk.get_vault_asset(intent.vault_address)

            # Resolve asset token for decimals
            asset_token = self._resolve_token(asset_address)
            if asset_token is None:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"Cannot resolve vault asset token: {asset_address}",
                    intent_id=intent.intent_id,
                )

            amount_wei = int(amount_decimal * Decimal(10**asset_token.decimals))

            # Build approve TX
            approve_txs = self._build_approve_tx(
                asset_token.address,
                intent.vault_address,
                amount_wei,
            )
            transactions.extend(approve_txs)

            # Build deposit TX via SDK
            deposit_tx_data = adapter.sdk.build_deposit_tx(
                vault_address=intent.vault_address,
                assets=amount_wei,
                receiver=self.wallet_address,
            )

            deposit_tx = TransactionData(
                to=deposit_tx_data["to"],
                value=deposit_tx_data["value"],
                data=deposit_tx_data["data"],
                gas_estimate=deposit_tx_data["gas_estimate"],
                description=f"Deposit {amount_decimal} {asset_token.symbol} into MetaMorpho vault {intent.vault_address[:10]}...",
                tx_type="vault_deposit",
            )
            transactions.append(deposit_tx)

            # Build ActionBundle
            total_gas = sum(tx.gas_estimate for tx in transactions)
            action_bundle = ActionBundle(
                intent_type=IntentType.VAULT_DEPOSIT.value,
                transactions=[tx.to_dict() for tx in transactions],
                metadata={
                    "protocol": intent.protocol,
                    "vault_address": intent.vault_address,
                    "asset_address": asset_token.address,
                    "asset_symbol": asset_token.symbol,
                    "deposit_amount": str(amount_decimal),
                    "deposit_amount_wei": str(amount_wei),
                    "chain": self.chain,
                },
            )

            result.action_bundle = action_bundle
            result.transactions = transactions
            result.total_gas_estimate = total_gas

            logger.info(
                f"Compiled VAULT_DEPOSIT: {amount_decimal} {asset_token.symbol} into vault {intent.vault_address[:10]}..."
            )
            return result

        except Exception as e:
            logger.exception(f"Failed to compile VAULT_DEPOSIT intent: {e}")
            result.status = CompilationStatus.FAILED
            result.error = str(e)
            return result

    def _compile_vault_redeem(self, intent: VaultRedeemIntent) -> CompilationResult:
        """Compile a VAULT_REDEEM intent into an ActionBundle.

        This method:
        1. Creates MetaMorpho adapter with gateway client
        2. If shares="all", queries maxRedeem to get share count
        3. Builds redeem TX (no approve needed)

        Args:
            intent: VaultRedeemIntent to compile

        Returns:
            CompilationResult with vault redeem ActionBundle
        """
        result = CompilationResult(
            status=CompilationStatus.SUCCESS,
            intent_id=intent.intent_id,
        )
        transactions: list[TransactionData] = []

        try:
            if self._gateway_client is None:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error="GatewayClient is required for MetaMorpho vault compilation (on-chain reads).",
                    intent_id=intent.intent_id,
                )

            # Lazy import to avoid circular import
            from ..connectors.morpho_vault.adapter import MetaMorphoAdapter, MetaMorphoConfig

            # Create adapter with gateway client
            vault_config = MetaMorphoConfig(
                chain=self.chain,
                wallet_address=self.wallet_address,
            )
            adapter = MetaMorphoAdapter(
                vault_config,
                gateway_client=self._gateway_client,
                token_resolver=self._token_resolver,
            )

            # Resolve shares amount
            if intent.shares == "all":
                # Query max redeemable shares
                shares_wei = adapter.sdk.get_max_redeem(intent.vault_address, self.wallet_address)
                if shares_wei <= 0:
                    return CompilationResult(
                        status=CompilationStatus.FAILED,
                        error="No shares to redeem",
                        intent_id=intent.intent_id,
                    )
            else:
                shares_decimal: Decimal = intent.shares  # type: ignore[assignment]
                # Resolve share decimals dynamically (vault address IS the share token for ERC-4626)
                share_decimals = adapter.sdk.get_decimals(intent.vault_address)
                shares_wei = int(shares_decimal * Decimal(10**share_decimals))

            if shares_wei <= 0:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error="Redeem shares must be positive",
                    intent_id=intent.intent_id,
                )

            # Build redeem TX via SDK (no approve needed - redeeming own shares)
            redeem_tx_data = adapter.sdk.build_redeem_tx(
                vault_address=intent.vault_address,
                shares=shares_wei,
                receiver=self.wallet_address,
                owner=self.wallet_address,
            )

            redeem_tx = TransactionData(
                to=redeem_tx_data["to"],
                value=redeem_tx_data["value"],
                data=redeem_tx_data["data"],
                gas_estimate=redeem_tx_data["gas_estimate"],
                description=f"Redeem {'all' if intent.shares == 'all' else intent.shares} shares from MetaMorpho vault {intent.vault_address[:10]}...",
                tx_type="vault_redeem",
            )
            transactions.append(redeem_tx)

            # Build ActionBundle
            total_gas = sum(tx.gas_estimate for tx in transactions)
            action_bundle = ActionBundle(
                intent_type=IntentType.VAULT_REDEEM.value,
                transactions=[tx.to_dict() for tx in transactions],
                metadata={
                    "protocol": intent.protocol,
                    "vault_address": intent.vault_address,
                    "shares_wei": str(shares_wei),
                    "redeem_all": intent.shares == "all",
                    "chain": self.chain,
                },
            )

            result.action_bundle = action_bundle
            result.transactions = transactions
            result.total_gas_estimate = total_gas

            logger.info(
                f"Compiled VAULT_REDEEM: {'all' if intent.shares == 'all' else intent.shares} shares from vault {intent.vault_address[:10]}..."
            )
            return result

        except Exception as e:
            logger.exception(f"Failed to compile VAULT_REDEEM intent: {e}")
            result.status = CompilationStatus.FAILED
            result.error = str(e)
            return result

    def _build_aave_flash_loan(
        self,
        token_info: "TokenInfo",
        amount_wei: int,
        callback_params: bytes,
        callback_gas_total: int,
    ) -> dict:
        """Build an Aave V3 flash loan transaction.

        Args:
            token_info: Token information
            amount_wei: Flash loan amount in wei
            callback_params: Encoded callback transaction data
            callback_gas_total: Total gas for callback operations

        Returns:
            Dict with transaction, pool_address, premium_bps, premium_amount, total_repay
        """
        adapter = AaveV3Adapter(self.chain, "aave_v3")
        pool_address = adapter.get_pool_address()

        if pool_address == "0x0000000000000000000000000000000000000000":
            return {"error": f"Aave V3 not available on chain: {self.chain}"}

        flash_loan_calldata = adapter.get_flash_loan_simple_calldata(
            receiver_address=self.wallet_address,
            asset=token_info.address,
            amount=amount_wei,
            params=callback_params,
        )

        # Calculate premium (0.09% for Aave V3)
        premium_bps = 9
        premium_amount = (amount_wei * premium_bps) // 10000
        total_repay = amount_wei + premium_amount

        flash_loan_tx = TransactionData(
            to=pool_address,
            value=0,
            data="0x" + flash_loan_calldata.hex(),
            gas_estimate=adapter.estimate_flash_loan_simple_gas() + callback_gas_total,
            description=(
                f"Flash loan {self._format_amount(amount_wei, token_info.decimals)} {token_info.symbol} via Aave V3 (premium: {self._format_amount(premium_amount, token_info.decimals)} {token_info.symbol})"
            ),
            tx_type="flash_loan",
        )

        return {
            "transaction": flash_loan_tx,
            "pool_address": pool_address,
            "premium_bps": premium_bps,
            "premium_amount": premium_amount,
            "total_repay": total_repay,
        }

    def _build_balancer_flash_loan(
        self,
        token_info: "TokenInfo",
        amount_wei: int,
        callback_params: bytes,
        callback_gas_total: int,
    ) -> dict:
        """Build a Balancer Vault flash loan transaction.

        Balancer flash loans have ZERO fees, making them ideal for arbitrage.

        Args:
            token_info: Token information
            amount_wei: Flash loan amount in wei
            callback_params: Encoded callback transaction data (userData)
            callback_gas_total: Total gas for callback operations

        Returns:
            Dict with transaction, pool_address (vault), premium_bps (0), premium_amount (0), total_repay
        """
        adapter = BalancerAdapter(self.chain, "balancer")
        vault_address = adapter.get_vault_address()

        if vault_address == "0x0000000000000000000000000000000000000000":
            return {"error": f"Balancer Vault not available on chain: {self.chain}"}

        flash_loan_calldata = adapter.get_flash_loan_simple_calldata(
            recipient=self.wallet_address,
            token=token_info.address,
            amount=amount_wei,
            user_data=callback_params,
        )

        # Balancer has ZERO fees!
        premium_bps = 0
        premium_amount = 0
        total_repay = amount_wei

        flash_loan_tx = TransactionData(
            to=vault_address,
            value=0,
            data="0x" + flash_loan_calldata.hex(),
            gas_estimate=adapter.estimate_flash_loan_simple_gas() + callback_gas_total,
            description=(
                f"Flash loan {self._format_amount(amount_wei, token_info.decimals)} {token_info.symbol} via Balancer (zero fee)"
            ),
            tx_type="flash_loan",
        )

        return {
            "transaction": flash_loan_tx,
            "pool_address": vault_address,
            "premium_bps": premium_bps,
            "premium_amount": premium_amount,
            "total_repay": total_repay,
        }

    def _build_morpho_flash_loan(
        self,
        token_info: "TokenInfo",
        amount_wei: int,
        callback_params: bytes,
        callback_gas_total: int,
    ) -> dict:
        """Build a Morpho Blue flash loan transaction.

        Morpho Blue flash loans have ZERO fees, making them ideal for
        PT leverage looping on Morpho Blue markets.

        Args:
            token_info: Token information
            amount_wei: Flash loan amount in wei
            callback_params: Encoded callback transaction data
            callback_gas_total: Total gas for callback operations

        Returns:
            Dict with transaction, pool_address, premium_bps (0), premium_amount (0), total_repay
        """
        from ..connectors.flash_loan.selector import MORPHO_BLUE_ADDRESSES

        morpho_address = MORPHO_BLUE_ADDRESSES.get(self.chain)
        if not morpho_address:
            return {"error": f"Morpho Blue not available on chain: {self.chain}"}

        # Build Morpho flash loan calldata
        # flashLoan(address token, uint256 assets, bytes calldata data)
        from web3 import Web3

        w3 = Web3()
        flash_loan_selector = "0xe0232b42"  # flashLoan(address,uint256,bytes)
        calldata = (
            flash_loan_selector
            + w3.codec.encode(
                ["address", "uint256", "bytes"],
                [w3.to_checksum_address(token_info.address), amount_wei, callback_params],
            ).hex()
        )

        # Morpho has ZERO fees!
        premium_bps = 0
        premium_amount = 0
        total_repay = amount_wei

        flash_loan_tx = TransactionData(
            to=morpho_address,
            value=0,
            data=calldata,
            gas_estimate=200_000 + callback_gas_total,
            description=(
                f"Flash loan {self._format_amount(amount_wei, token_info.decimals)} {token_info.symbol} via Morpho Blue (zero fee)"
            ),
            tx_type="flash_loan",
        )

        return {
            "transaction": flash_loan_tx,
            "pool_address": morpho_address,
            "premium_bps": premium_bps,
            "premium_amount": premium_amount,
            "total_repay": total_repay,
        }

    def _encode_flash_loan_callbacks(
        self,
        callback_transactions: list[TransactionData],
    ) -> bytes:
        """Encode callback transactions for flash loan params.

        The encoded data will be passed to the receiver contract's executeOperation
        function. The receiver contract is responsible for decoding and executing
        these transactions atomically.

        Format: ABI-encoded array of (address to, uint256 value, bytes data) tuples

        Args:
            callback_transactions: List of transactions to encode

        Returns:
            ABI-encoded bytes for the params field
        """
        if not callback_transactions:
            return b""

        # Simple encoding: concatenate transaction data
        # In production, this would use proper ABI encoding
        # Format for each tx: to(20 bytes) + value(32 bytes) + data_length(32 bytes) + data
        encoded_parts: list[bytes] = []

        for tx in callback_transactions:
            # Extract address (remove 0x prefix, pad to 20 bytes)
            to_addr = bytes.fromhex(tx.to.lower().replace("0x", "").zfill(40))

            # Value as 32-byte big-endian
            value_bytes = tx.value.to_bytes(32, "big")

            # Data (remove 0x prefix if present)
            data_hex = tx.data.lower().replace("0x", "") if tx.data else ""
            data_bytes = bytes.fromhex(data_hex) if data_hex else b""

            # Data length as 32-byte big-endian
            data_len_bytes = len(data_bytes).to_bytes(32, "big")

            # Combine: to + value + data_length + data
            encoded_parts.append(to_addr + value_bytes + data_len_bytes + data_bytes)

        # Prepend count of transactions
        count_bytes = len(callback_transactions).to_bytes(32, "big")
        return count_bytes + b"".join(encoded_parts)

    def _resolve_token(self, token: str, chain: str | None = None) -> TokenInfo | None:
        """Resolve a token symbol or address to TokenInfo.

        Uses the TokenResolver for unified token lookup with caching and
        optional on-chain discovery via gateway.

        Args:
            token: Token symbol (e.g., "USDC") or address
            chain: Optional chain to resolve token for (defaults to self.chain)

        Returns:
            TokenInfo or None if not found
        """
        target_chain = chain or self.chain

        try:
            # Use TokenResolver for unified lookup
            resolved = self._token_resolver.resolve(token, target_chain)

            return TokenInfo(
                symbol=resolved.symbol,
                address=resolved.address,
                decimals=resolved.decimals,
                is_native=resolved.is_native,
            )
        except Exception as e:
            # Import lazily to avoid circular import
            from almanak.framework.data.tokens.exceptions import TokenNotFoundError

            if isinstance(e, TokenNotFoundError):
                # Token not found in registry or on-chain - return None for backward compatibility
                logger.debug(f"Token '{token}' not found on {target_chain}")
                return None
            raise

    def _get_token_decimals(self, symbol: str) -> int:
        """Get decimals for a token symbol.

        Uses the TokenResolver for unified lookup. NEVER defaults to 18 decimals -
        raises TokenNotFoundError if decimals are unknown.

        Args:
            symbol: Token symbol (e.g., "USDC")

        Returns:
            Number of decimal places for the token

        Raises:
            TokenNotFoundError: If token cannot be resolved
        """
        return self._token_resolver.get_decimals(self.chain, symbol)

    def _is_native_token(self, symbol: str) -> bool:
        """Check if token is the native token."""
        native_tokens = {"ETH", "MATIC", "AVAX", "XPL"}
        return symbol.upper() in native_tokens

    def _get_wrapped_native_address(self) -> str | None:
        """Get the wrapped native token address for the current chain.

        Uses TokenResolver to resolve WETH/WMATIC/WAVAX/WXPL depending on chain.
        Returns None if the wrapped native token cannot be resolved.
        """
        # Map chains to their wrapped native token symbol
        wrapped_symbols = {
            "ethereum": "WETH",
            "arbitrum": "WETH",
            "optimism": "WETH",
            "base": "WETH",
            "polygon": "WMATIC",
            "avalanche": "WAVAX",
            "plasma": "WXPL",
            "bsc": "WBNB",
            "mantle": "WMNT",
            "sonic": "WS",
        }
        symbol = wrapped_symbols.get(self.chain)
        if not symbol:
            return None
        try:
            return self._token_resolver.get_address(self.chain, symbol)
        except Exception:
            return None

    def _usd_to_token_amount(self, usd_amount: Decimal, token: TokenInfo) -> int:
        """Convert USD amount to token amount in wei.

        Args:
            usd_amount: Amount in USD
            token: Target token info

        Returns:
            Token amount in smallest units (wei)
        """
        price = self._require_token_price(token.symbol)
        token_amount = usd_amount / price
        return int(token_amount * Decimal(10**token.decimals))

    def _calculate_expected_output(
        self,
        amount_in: int,
        from_token: TokenInfo,
        to_token: TokenInfo,
    ) -> int:
        """Calculate expected output amount.

        In production, this would query the DEX for a quote.
        For now, uses price oracle to estimate.

        Args:
            amount_in: Input amount in wei
            from_token: Input token info
            to_token: Output token info

        Returns:
            Expected output amount in wei
        """
        # Get prices
        from_price = self._require_token_price(from_token.symbol)
        to_price = self._require_token_price(to_token.symbol)

        # Convert input to USD
        from_amount_decimal = Decimal(str(amount_in)) / Decimal(10**from_token.decimals)
        usd_value = from_amount_decimal * from_price

        # Convert USD to output tokens
        to_amount_decimal = usd_value / to_price

        # Apply a small fee estimate (0.3%)
        to_amount_decimal = to_amount_decimal * Decimal("0.997")

        return int(to_amount_decimal * Decimal(10**to_token.decimals))

    def _build_approve_tx(
        self,
        token_address: str,
        spender: str,
        amount: int,
    ) -> list[TransactionData]:
        """Build approve transaction(s) if needed.

        For most tokens, returns a single approve TX if allowance is insufficient.
        For tokens like USDC/USDT that require approve(0) first when allowance > 0,
        returns two TXs: approve(0) followed by approve(amount).

        Args:
            token_address: ERC20 token to approve
            spender: Address to approve (router)
            amount: Amount to approve

        Returns:
            List of TransactionData for approval (may be empty, 1, or 2 transactions)
        """
        transactions: list[TransactionData] = []
        token_lower = token_address.lower()
        requires_zero_first = token_lower in APPROVE_ZERO_FIRST_TOKENS
        on_chain_allowance = 0

        # ALWAYS query on-chain allowance to avoid stale cache issues
        # This is critical for safety - never skip approve based on cache alone
        if self._gateway_client is not None or self.rpc_url:
            on_chain_allowance = self._query_allowance(token_address, spender)
            if on_chain_allowance >= amount:
                # Already have sufficient on-chain allowance - update cache and skip
                if token_lower not in self._allowance_cache:
                    self._allowance_cache[token_lower] = {}
                self._allowance_cache[token_lower][spender.lower()] = on_chain_allowance
                logger.debug(
                    f"Sufficient on-chain allowance exists for {token_address} -> {spender}: {on_chain_allowance}"
                )
                return []
        else:
            # No way to query on-chain - check cache as fallback but log warning
            cached = self._allowance_cache.get(token_lower, {}).get(spender.lower(), 0)
            if cached >= amount:
                logger.warning(
                    f"Using cached allowance for {token_address} -> {spender} (no RPC available). "
                    f"This may cause issues if allowance was revoked on-chain."
                )
                return []

        # Build approve calldata helper
        def build_approve_calldata(approve_amount: int) -> str:
            spender_padded = spender.lower().replace("0x", "").zfill(64)
            amount_padded = hex(approve_amount)[2:].zfill(64)
            return ERC20_APPROVE_SELECTOR + spender_padded + amount_padded

        # If token requires approve(0) first AND has existing on-chain allowance > 0
        if requires_zero_first and on_chain_allowance > 0:
            logger.debug(f"Token {token_address} requires approve(0) first (existing allowance: {on_chain_allowance})")
            # Add approve(0) transaction first
            transactions.append(
                TransactionData(
                    to=token_address,
                    value=0,
                    data=build_approve_calldata(0),
                    gas_estimate=get_gas_estimate(self.chain, "approve"),
                    description=f"Reset approval to 0 for {spender[:10]}...",
                    tx_type="approve_reset",
                )
            )

        # Build main approve TX
        # Use actual amount + 10% buffer, but cap at MAX_UINT256
        # to avoid overflow when building calldata (hex would be >64 chars)
        if amount >= MAX_UINT256:
            approval_amount = MAX_UINT256
        else:
            approval_amount = min(int(amount * 1.1), MAX_UINT256)  # 10% buffer, capped

        transactions.append(
            TransactionData(
                to=token_address,
                value=0,
                data=build_approve_calldata(approval_amount),
                gas_estimate=get_gas_estimate(self.chain, "approve"),
                description=f"Approve {spender[:10]}... to spend token",
                tx_type="approve",
            )
        )

        # Update cache
        if token_lower not in self._allowance_cache:
            self._allowance_cache[token_lower] = {}
        self._allowance_cache[token_lower][spender.lower()] = approval_amount

        return transactions

    def _query_allowance(self, token_address: str, spender: str) -> int:
        """Query on-chain allowance for a token/spender pair.

        Uses gateway RPC when gateway_client is configured, otherwise falls back
        to direct Web3 RPC (deprecated for production use).

        Args:
            token_address: ERC20 token address
            spender: Spender address

        Returns:
            Current allowance (0 if query fails)
        """
        # Prefer gateway RPC when available
        if self._gateway_client is not None:
            try:
                result = self._gateway_client.query_allowance(
                    chain=self.chain,
                    token_address=token_address,
                    owner_address=self.wallet_address,
                    spender_address=spender,
                )
                return result if result is not None else 0
            except Exception as e:
                logger.warning(f"Gateway allowance query failed for {token_address}: {e}")
                return 0

        # Fallback to direct Web3 RPC (deprecated)
        if self.rpc_url is None and self._web3 is None:
            return 0

        try:
            from web3 import Web3

            if self._web3 is None:
                logger.debug("Using direct Web3 RPC for allowance query - this is deprecated")
                self._web3 = Web3(Web3.HTTPProvider(self.rpc_url))

            assert self._web3 is not None
            # Build allowance call: allowance(owner, spender)
            owner_padded = self.wallet_address.lower().replace("0x", "").zfill(64)
            spender_padded = spender.lower().replace("0x", "").zfill(64)
            calldata = ERC20_ALLOWANCE_SELECTOR + owner_padded + spender_padded

            raw_result = self._web3.eth.call(
                {
                    "to": self._web3.to_checksum_address(token_address),
                    "data": calldata,  # type: ignore[typeddict-item]
                }
            )

            if raw_result:
                return int(raw_result.hex(), 16)
            return 0
        except Exception as e:
            logger.warning(f"Failed to query allowance for {token_address}: {e}")
            return 0

    # Lazy-loaded from almanak.core.constants to avoid circular import
    # (compiler -> data/__init__ -> prediction_provider -> connectors -> execution -> compiler)
    _KNOWN_STABLECOINS: ClassVar[frozenset[str] | None] = None

    @classmethod
    def _get_known_stablecoins(cls) -> frozenset[str]:
        known = cls._KNOWN_STABLECOINS
        if known is None:
            from almanak.core.constants import STABLECOINS

            known = frozenset(s.upper() for s in STABLECOINS)
            cls._KNOWN_STABLECOINS = known
        return known

    # Wrapped native tokens map to their native counterpart for price lookups.
    # Wrapped natives are 1:1 pegged by the WETH9 contract (deposit/withdraw at par),
    # so ETH price == WETH price. When the oracle only has "ETH", a lookup for
    # "WETH" should resolve to ETH's price rather than failing.
    _WRAPPED_TO_NATIVE: ClassVar[dict[str, str]] = {
        "WETH": "ETH",
        "WMATIC": "MATIC",
        "WAVAX": "AVAX",
        "WBNB": "BNB",
        "WMNT": "MNT",
        "WS": "S",
        "WXPL": "XPL",
        "WPOL": "POL",
    }

    def _require_token_price(self, symbol: str) -> Decimal:
        """Look up a token price, failing fast on missing or zero prices.

        When ``_using_placeholders`` is True (test-only mode) a fallback of
        ``Decimal("1")`` is returned for unknown tokens so that compilation
        can proceed with approximate values.  In production mode (a real
        price oracle is provided) a missing or zero price raises
        ``ValueError`` so the caller surfaces a clear error instead of
        silently using a bogus price.

        For known stablecoins (USDC, USDT, DAI, etc.), falls back to $1.00
        if the price oracle doesn't have them cached. This prevents compilation
        failures when the strategy's decide() didn't explicitly fetch the price.

        For wrapped native tokens (WETH, WMATIC, WAVAX, etc.), falls back to
        the native token price (ETH, MATIC, AVAX) since they are 1:1 pegged
        by the WETH9 contract.

        Args:
            symbol: Token symbol to look up.

        Returns:
            Token price in USD as ``Decimal``.

        Raises:
            ValueError: If the price is missing/zero and we are *not*
                using placeholder prices.
        """
        if self.price_oracle is None:
            if self._using_placeholders:
                return Decimal("1")
            # Fall back for stablecoins even without an oracle
            if symbol.upper() in self._get_known_stablecoins():
                return Decimal("1")
            raise ValueError(
                f"No price oracle available and placeholder prices are disabled. Cannot resolve price for '{symbol}'."
            )

        price = self.price_oracle.get(symbol)
        if price is None or price == 0:
            # Case-insensitive fallback: Token.__post_init__ uppercases symbols
            # (e.g., "cbETH" -> "CBETH") but the price oracle may store them in
            # original case. Try case-insensitive match before giving up.
            symbol_upper = symbol.upper()
            for key, val in self.price_oracle.items():
                if key.upper() == symbol_upper and val is not None and val != 0:
                    price = val
                    logger.debug(f"Resolved '{symbol}' price via case-insensitive match (key='{key}')")
                    break

        if price is None or price == 0:
            # Try wrapped-native alias (WETH -> ETH, WMATIC -> MATIC, etc.)
            native_alias = self._WRAPPED_TO_NATIVE.get(symbol.upper())
            if native_alias:
                alias_price = self.price_oracle.get(native_alias)
                if alias_price is not None and alias_price != 0:
                    logger.debug(f"Resolved '{symbol}' price via native alias '{native_alias}'")
                    return alias_price

            if self._using_placeholders:
                return Decimal("1")
            # Stablecoin fallback: these are always ~$1, safe to assume
            if symbol.upper() in self._get_known_stablecoins():
                if symbol not in self._stablecoin_fallback_logged:
                    logger.info(f"Price for '{symbol}' not in oracle cache, using stablecoin fallback ($1.00)")
                    self._stablecoin_fallback_logged.add(symbol)
                else:
                    logger.debug(f"Reusing stablecoin fallback price for '{symbol}'")
                return Decimal("1")
            raise ValueError(
                f"Price for '{symbol}' is {'zero' if price == 0 else 'missing'} in the price oracle. "
                "Compilation requires a valid price to calculate amounts and slippage."
            )
        return price

    def _resolve_dest_wallet(self, dest_chain: str) -> str:
        """Resolve destination wallet for cross-chain operations.

        If chain_wallets is configured (from wallet registry), returns the
        wallet for the destination chain. Otherwise returns self.wallet_address.

        Args:
            dest_chain: Destination chain name

        Returns:
            Wallet address for the destination chain
        """
        if self._chain_wallets:
            return self._chain_wallets.get(dest_chain.lower(), self.wallet_address)
        return self.wallet_address

    def _get_placeholder_prices(self) -> dict[str, Decimal]:
        """Get placeholder price data for testing only.

        WARNING: These prices are HARDCODED and OUTDATED.
        DO NOT USE IN PRODUCTION - they will cause:
        - Incorrect slippage calculations
        - Swap reverts (amountOutMinimum too high)
        - Position sizing errors
        - Health factor miscalculations

        Real prices as of 2026-01: ETH ~$3400, BTC ~$105,000
        These placeholders show ETH at $2000, BTC at $45,000 - 40-60% wrong!
        """
        logger.debug(
            "PLACEHOLDER PRICES being used - NOT SAFE FOR PRODUCTION. ETH=$2000 (real ~$3400), BTC=$45000 (real ~$105000)"
        )
        return {
            "ETH": Decimal("2000"),
            "WETH": Decimal("2000"),
            "USDC": Decimal("1"),
            "USDC.e": Decimal("1"),
            "USDT": Decimal("1"),
            "DAI": Decimal("1"),
            "WBTC": Decimal("45000"),
            "MATIC": Decimal("0.80"),
            "WMATIC": Decimal("0.80"),
            "ARB": Decimal("1.20"),
            "OP": Decimal("2.50"),
            "AVAX": Decimal("35"),
            "WAVAX": Decimal("35"),
            "BNB": Decimal("600"),
            "WBNB": Decimal("600"),
            "S": Decimal("0.50"),
            "WS": Decimal("0.50"),
            "MNT": Decimal("0.80"),
            "WMNT": Decimal("0.80"),
        }

    @staticmethod
    def _format_amount(amount: int, decimals: int) -> str:
        """Format a wei amount for display."""
        decimal_amount = Decimal(str(amount)) / Decimal(10**decimals)
        return f"{decimal_amount:,.4f}"

    def _parse_pool_info(self, pool: str) -> tuple[TokenInfo, TokenInfo, int, bool] | None:
        """Parse pool identifier to extract token addresses and fee tier.

        Supports formats:
        - "TOKEN0/TOKEN1/FEE" (e.g., "WETH/USDC/3000")
        - "TOKEN0/TOKEN1" (defaults to 3000 fee tier)
        - Pool address starting with "0x" (returns default tokens)

        Args:
            pool: Pool identifier string

        Returns:
            Tuple of (token0_info, token1_info, fee_tier, tokens_swapped) or None if parsing fails.
            tokens_swapped is True when the user-specified token order was reversed to match
            the on-chain convention (token0 address < token1 address). Callers must invert
            price ranges and swap amounts when this flag is True.
        """
        # Default fee tier (0.3%)
        default_fee = 3000

        # Handle single pool address format (e.g., "0xbDbC38652D78AF...")
        # Only match if no "/" present -- a pool string like "0xToken0/0xToken1/3000"
        # should fall through to the TOKEN0/TOKEN1/FEE parsing below.
        if pool.startswith("0x") and "/" not in pool:
            # For pool addresses, we need external lookup in production
            # For now, return placeholder tokens based on common patterns
            # This would query the pool contract for token addresses
            logger.warning(f"Pool address format requires on-chain lookup: {pool}. Using default WETH/USDC pair.")
            token0 = self._resolve_token("WETH")
            token1 = self._resolve_token("USDC")
            if token0 is None or token1 is None:
                return None
            return (token0, token1, default_fee, False)

        # Handle TOKEN0/TOKEN1/FEE or TOKEN0/TOKEN1 format
        parts = pool.split("/")
        if len(parts) < 2:
            return None

        token0_symbol = parts[0].strip()
        token1_symbol = parts[1].strip()

        # Parse fee tier if provided
        fee_tier = default_fee
        if len(parts) >= 3:
            try:
                fee_tier = int(parts[2].strip())
            except ValueError:
                logger.warning(f"Invalid fee tier: {parts[2]}, using default {default_fee}")

        # Resolve token addresses
        token0 = self._resolve_token(token0_symbol)
        token1 = self._resolve_token(token1_symbol)

        if token0 is None:
            logger.error(f"Unknown token: {token0_symbol}")
            return None
        if token1 is None:
            logger.error(f"Unknown token: {token1_symbol}")
            return None

        # Ensure tokens are sorted (token0 < token1 by address)
        tokens_swapped = False
        if token0.address.lower() > token1.address.lower():
            token0, token1 = token1, token0
            tokens_swapped = True
            logger.debug(f"Swapped tokens to maintain sorting: {token0.symbol}/{token1.symbol}")

        return (token0, token1, fee_tier, tokens_swapped)

    # Uniswap V3 tick bounds
    UNISWAP_MIN_TICK = -887272
    UNISWAP_MAX_TICK = 887272

    @staticmethod
    def _price_to_tick(
        price: Decimal,
        token0_decimals: int = 18,
        token1_decimals: int = 18,
    ) -> int:
        """Convert a price to a Uniswap V3 tick.

        Uniswap V3 uses tick-based pricing where:
            price = 1.0001^tick

        But the price must be adjusted for token decimals first:
            adjusted_price = price / 10^(token0_decimals - token1_decimals)
            tick = log(adjusted_price) / log(1.0001)

        For example, with WETH/USDC (18/6 decimals):
            price = 3400 USDC per WETH (nominal)
            adjusted = 3400 / 10^(18-6) = 3400 / 10^12 = 3.4e-9
            tick = log(3.4e-9) / log(1.0001) ≈ -194957

        Args:
            price: The price in nominal units (token1 per token0)
            token0_decimals: Decimals of token0
            token1_decimals: Decimals of token1

        Returns:
            The tick value (rounded down), bounded to valid Uniswap tick range
        """
        import math

        if price <= 0:
            raise ValueError("Price must be positive")

        # Adjust price for decimal difference
        decimal_adjustment = 10 ** (token0_decimals - token1_decimals)
        adjusted_price = float(price) / decimal_adjustment

        # tick = ln(adjusted_price) / ln(1.0001)
        tick = math.floor(math.log(adjusted_price) / math.log(1.0001))

        # Bound to valid tick range
        tick = max(tick, IntentCompiler.UNISWAP_MIN_TICK)
        tick = min(tick, IntentCompiler.UNISWAP_MAX_TICK)

        return tick

    @staticmethod
    def _tick_to_price(tick: int) -> Decimal:
        """Convert a Uniswap V3 tick to a price.

        Args:
            tick: The tick value

        Returns:
            The price (1.0001^tick)
        """
        return Decimal(str(1.0001**tick))

    @staticmethod
    def _get_tick_spacing(fee_tier: int) -> int:
        """Get the tick spacing for a given fee tier.

        Standard tick spacings by fee tier:
        - 100 (0.01%): tick spacing 1
        - 500 (0.05%): tick spacing 10
        - 2500 (0.25%): tick spacing 50  (PancakeSwap V3)
        - 3000 (0.30%): tick spacing 60
        - 10000 (1.00%): tick spacing 200

        Args:
            fee_tier: The fee tier in basis points

        Returns:
            The tick spacing
        """
        tick_spacings = {
            100: 1,
            500: 10,
            2500: 50,
            3000: 60,
            10000: 200,
        }
        if fee_tier not in tick_spacings:
            logger.warning(
                "Unknown fee tier %d -- defaulting to tick_spacing=60. "
                "Known fee tiers: %s. "
                "If this is a protocol-specific fee tier, add it to _get_tick_spacing().",
                fee_tier,
                list(tick_spacings.keys()),
            )
        return tick_spacings.get(fee_tier, 60)

    def set_allowance(self, token_address: str, spender: str, amount: int) -> None:
        """Set cached allowance (for testing or after on-chain approval).

        Args:
            token_address: Token contract address
            spender: Spender address
            amount: Allowance amount
        """
        if token_address not in self._allowance_cache:
            self._allowance_cache[token_address] = {}
        self._allowance_cache[token_address][spender] = amount

    def clear_allowance_cache(self) -> None:
        """Clear the allowance cache."""
        self._allowance_cache.clear()

    def _query_position_liquidity(self, position_manager: str, token_id: int) -> int | None:
        """Query the liquidity of a Uniswap V3 position from on-chain.

        Uses gateway RPC when gateway_client is configured, otherwise falls back
        to direct Web3 RPC (deprecated for production use).

        Args:
            position_manager: NonfungiblePositionManager contract address
            token_id: Position NFT token ID

        Returns:
            Liquidity amount, or None if query fails
        """
        # Prefer gateway RPC when available
        if self._gateway_client is not None:
            try:
                return self._gateway_client.query_position_liquidity(
                    chain=self.chain,
                    position_manager=position_manager,
                    token_id=token_id,
                )
            except Exception as e:
                error_msg = str(e)
                if "invalid token id" in error_msg.lower():
                    logger.info(
                        "Gateway position liquidity query returned invalid token id; treating as closed position",
                        extra={"token_id": token_id, "error": error_msg},
                    )
                    return 0
                logger.error(f"Gateway position liquidity query failed: {e}")
                return None

        # Fallback to direct Web3 RPC (deprecated)
        if self.rpc_url is None and self._web3 is None:
            logger.warning("No RPC URL or gateway client - cannot query position liquidity")
            return None

        try:
            # Lazy import web3
            from web3 import Web3

            if self._web3 is None:
                logger.warning("Using direct Web3 RPC for position query - this is deprecated")
                self._web3 = Web3(Web3.HTTPProvider(self.rpc_url))

            assert self._web3 is not None
            # positions(uint256) returns a tuple with liquidity at index 7
            # Encode the call: positions(tokenId)
            selector = "0x99fbab88"  # positions(uint256)
            data = selector + hex(token_id)[2:].zfill(64)

            result = self._web3.eth.call(
                {
                    "to": self._web3.to_checksum_address(position_manager),
                    "data": data,  # type: ignore[typeddict-item]
                }
            )

            # Decode result - liquidity is at offset 7 * 32 = 224 bytes
            # Position struct: nonce, operator, token0, token1, fee, tickLower, tickUpper, liquidity, ...
            if len(result) >= 256:  # 8 * 32 bytes minimum
                liquidity_offset = 7 * 32
                liquidity = int.from_bytes(result[liquidity_offset : liquidity_offset + 32], byteorder="big")
                logger.debug(f"Position #{token_id} liquidity: {liquidity}")
                return liquidity
            else:
                logger.warning(f"Unexpected result length from positions call: {len(result)}")
                return None

        except Exception as e:
            logger.error(f"Failed to query position liquidity: {e}")
            return None

    def _query_position_tokens_owed(self, position_manager: str, token_id: int) -> tuple[int | None, int | None]:
        """Query tokens owed (fees + withdrawn liquidity) for a Uniswap V3 position.

        Args:
            position_manager: NonfungiblePositionManager contract address
            token_id: Position NFT token ID

        Returns:
            Tuple of (tokensOwed0, tokensOwed1) or (None, None) if query fails
        """
        # Prefer gateway RPC when available
        if self._gateway_client is not None:
            try:
                # Use gateway's dedicated QueryPositionTokensOwed method
                from almanak.gateway.proto import gateway_pb2

                request = gateway_pb2.PositionTokensOwedRequest(
                    chain=str(self.chain),
                    position_manager=position_manager,
                    token_id=token_id,
                )

                response = self._gateway_client.rpc.QueryPositionTokensOwed(request, timeout=10.0)

                if not response.success:
                    error_msg = response.error or ""
                    if "position not found" in error_msg.lower() or "invalid token id" in error_msg.lower():
                        logger.info(
                            "Gateway tokens owed query indicates closed position",
                            extra={"token_id": token_id, "error": error_msg},
                        )
                        return 0, 0
                    logger.error(f"Gateway QueryPositionTokensOwed failed: {error_msg}")
                    return None, None

                # Parse response - tokens are returned as decimal strings
                try:
                    tokens_owed0 = int(response.tokens_owed0) if response.tokens_owed0 else 0
                    tokens_owed1 = int(response.tokens_owed1) if response.tokens_owed1 else 0
                    logger.debug(f"Position #{token_id} tokens owed: {tokens_owed0} token0, {tokens_owed1} token1")
                    return tokens_owed0, tokens_owed1
                except (ValueError, TypeError) as e:
                    logger.error(f"Failed to parse tokens owed from gateway response: {e}")
                    return None, None
            except Exception as e:
                error_msg = str(e)
                if "invalid token id" in error_msg.lower():
                    logger.info(
                        "Gateway tokens owed query returned invalid token id; treating as closed position",
                        extra={"token_id": token_id, "error": error_msg},
                    )
                    return 0, 0
                logger.error(f"Gateway position tokens owed query failed: {e}")
                return None, None

        # Fallback to direct Web3 RPC
        if self.rpc_url is None and self._web3 is None:
            logger.warning("No RPC URL or gateway client - cannot query position tokens owed")
            return None, None

        try:
            # Lazy import web3
            from web3 import Web3

            if self._web3 is None:
                logger.warning("Using direct Web3 RPC for position query - this is deprecated")
                self._web3 = Web3(Web3.HTTPProvider(self.rpc_url))

            assert self._web3 is not None
            # positions(uint256) returns a tuple
            # tokensOwed0 is at index 10, tokensOwed1 is at index 11
            selector = "0x99fbab88"  # positions(uint256)
            data = selector + hex(token_id)[2:].zfill(64)

            result = self._web3.eth.call(
                {
                    "to": self._web3.to_checksum_address(position_manager),
                    "data": data,  # type: ignore[typeddict-item]
                }
            )

            # Decode result - tokensOwed0 is at offset 10 * 32 = 320 bytes, tokensOwed1 at 11 * 32 = 352 bytes
            if len(result) >= 384:  # 12 * 32 bytes minimum
                tokens_owed0_offset = 10 * 32
                tokens_owed1_offset = 11 * 32
                tokens_owed0 = int.from_bytes(result[tokens_owed0_offset : tokens_owed0_offset + 32], byteorder="big")
                tokens_owed1 = int.from_bytes(result[tokens_owed1_offset : tokens_owed1_offset + 32], byteorder="big")
                logger.debug(f"Position #{token_id} tokens owed: {tokens_owed0} token0, {tokens_owed1} token1")
                return tokens_owed0, tokens_owed1
            else:
                logger.warning(f"Unexpected result length from positions call: {len(result)}")
                return None, None

        except Exception as e:
            logger.error(f"Failed to query position tokens owed: {e}")
            return None, None

    def _query_erc20_balance(self, token_address: str, wallet_address: str) -> int | None:
        """Query ERC-20 token balance from on-chain.

        Uses gateway RPC when gateway_client is configured, otherwise falls back
        to direct Web3 RPC (deprecated for production use).

        Args:
            token_address: ERC-20 token contract address
            wallet_address: Wallet address to query balance for

        Returns:
            Token balance in wei, or None if query fails
        """
        # Prefer gateway RPC when available
        if self._gateway_client is not None:
            try:
                return self._gateway_client.query_erc20_balance(
                    chain=self.chain,
                    token_address=token_address,
                    wallet_address=wallet_address,
                )
            except Exception as e:
                logger.error(f"Gateway balance query failed: {e}")
                return None

        # Fallback to direct Web3 RPC (deprecated)
        if self.rpc_url is None and self._web3 is None:
            logger.warning("No RPC URL or gateway client - cannot query ERC-20 balance")
            return None

        try:
            # Lazy import web3
            from web3 import Web3

            if self._web3 is None:
                logger.warning("Using direct Web3 RPC for balance query - this is deprecated")
                self._web3 = Web3(Web3.HTTPProvider(self.rpc_url))

            assert self._web3 is not None
            # balanceOf(address) selector
            selector = "0x70a08231"
            # Pad address to 32 bytes (remove 0x prefix, left-pad with zeros)
            padded_address = wallet_address[2:].lower().zfill(64)
            data = selector + padded_address

            result = self._web3.eth.call(
                {
                    "to": self._web3.to_checksum_address(token_address),
                    "data": data,  # type: ignore[typeddict-item]
                }
            )

            # Decode uint256 balance
            balance = int.from_bytes(result, byteorder="big")
            logger.debug(f"ERC-20 balance for {wallet_address} at {token_address}: {balance}")
            return balance

        except Exception as e:
            logger.error(f"Failed to query ERC-20 balance: {e}")
            return None

    def _query_native_balance(self, wallet_address: str) -> int | None:
        """Query native token balance (ETH, MATIC, AVAX, etc.) from on-chain.

        Uses gateway RPC when available, otherwise falls back to direct Web3 RPC.

        Returns:
            Native balance in wei, or None if query fails
        """
        # Prefer gateway RPC via public API
        if self._gateway_client is not None:
            try:
                return self._gateway_client.query_native_balance(
                    chain=self.chain,
                    wallet_address=wallet_address,
                )
            except Exception as e:
                logger.error(f"Gateway native balance query failed: {e}")
                return None

        # Fallback to direct Web3 RPC (deprecated)
        if self.rpc_url is None and self._web3 is None:
            logger.warning("No RPC URL or gateway client - cannot query native balance")
            return None

        try:
            from web3 import Web3

            if self._web3 is None:
                self._web3 = Web3(Web3.HTTPProvider(self.rpc_url))

            assert self._web3 is not None
            balance = self._web3.eth.get_balance(self._web3.to_checksum_address(wallet_address))
            logger.debug(f"Native balance for {wallet_address}: {balance}")
            return balance
        except Exception as e:
            logger.error(f"Failed to query native balance: {e}")
            return None


# =============================================================================
# Exports
# =============================================================================

__all__ = [
    "IntentCompiler",
    "CompilationResult",
    "CompilationStatus",
    "TransactionData",
    "TokenInfo",
    "PriceInfo",
    "DefaultSwapAdapter",
    "SwapProtocolAdapter",
    "UniswapV3LPAdapter",
    "LPProtocolAdapter",
    "AaveV3Adapter",
    "LendingProtocolAdapter",
    "DEFAULT_GAS_ESTIMATES",
    "CHAIN_GAS_OVERRIDES",
    "get_gas_estimate",
    "PROTOCOL_ROUTERS",
    "LP_POSITION_MANAGERS",
    "LENDING_POOL_ADDRESSES",
    "AAVE_VARIABLE_RATE_MODE",
]
