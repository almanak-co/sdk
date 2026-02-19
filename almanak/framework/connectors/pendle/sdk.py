"""
Pendle Protocol SDK for Yield Tokenization

Pendle is a permissionless yield-trading protocol that enables users to:
1. Tokenize yield-bearing assets into Principal Tokens (PT) and Yield Tokens (YT)
2. Trade PT and YT on Pendle's AMM
3. Provide liquidity to PT/SY pools
4. Redeem PT at maturity for the underlying asset

Key Concepts:
- SY (Standardized Yield): Wrapped yield-bearing tokens (e.g., SY-stETH)
- PT (Principal Token): Represents the principal, redeemable at maturity
- YT (Yield Token): Represents the yield until maturity
- Market: AMM pool containing PT and SY tokens

This SDK provides methods for:
- Swapping tokens to/from PT
- Adding/removing liquidity
- Redeeming PT/YT at maturity
"""

import json
import logging
import os
from dataclasses import dataclass
from decimal import Decimal
from enum import Enum
from typing import TYPE_CHECKING, Any

from web3 import Web3
from web3.contract import Contract

if TYPE_CHECKING:
    from almanak.framework.data.tokens.resolver import TokenResolver as TokenResolverType

from almanak.core.contracts import PENDLE as _PENDLE_REGISTRY
from almanak.core.contracts import PENDLE_TOKENS as _PENDLE_TOKENS

logger = logging.getLogger(__name__)


class PendleActionType(Enum):
    """Pendle action types."""

    APPROVE = "APPROVE"
    SWAP_EXACT_TOKEN_FOR_PT = "SWAP_EXACT_TOKEN_FOR_PT"
    SWAP_EXACT_PT_FOR_TOKEN = "SWAP_EXACT_PT_FOR_TOKEN"
    SWAP_EXACT_TOKEN_FOR_YT = "SWAP_EXACT_TOKEN_FOR_YT"
    SWAP_EXACT_YT_FOR_TOKEN = "SWAP_EXACT_YT_FOR_TOKEN"
    ADD_LIQUIDITY_SINGLE_TOKEN = "ADD_LIQUIDITY_SINGLE_TOKEN"
    REMOVE_LIQUIDITY_SINGLE_TOKEN = "REMOVE_LIQUIDITY_SINGLE_TOKEN"
    ADD_LIQUIDITY_DUAL = "ADD_LIQUIDITY_DUAL"
    MINT_SY_FROM_TOKEN = "MINT_SY_FROM_TOKEN"
    REDEEM_SY_TO_TOKEN = "REDEEM_SY_TO_TOKEN"
    MINT_PY_FROM_TOKEN = "MINT_PY_FROM_TOKEN"
    REDEEM_PY_TO_TOKEN = "REDEEM_PY_TO_TOKEN"


# =============================================================================
# Contract Addresses
# =============================================================================

# Pendle V2 Contract Addresses by chain (merged from centralized registry)
PENDLE_ADDRESSES: dict[str, dict[str, str]] = {}
for _chain, _addrs in _PENDLE_REGISTRY.items():
    PENDLE_ADDRESSES[_chain] = {k.upper(): v for k, v in _addrs.items()}
    if _chain in _PENDLE_TOKENS:
        PENDLE_ADDRESSES[_chain].update(_PENDLE_TOKENS[_chain])

# Mapping of PT token names to market addresses (canonical lookup)
# This provides a direct, unambiguous mapping from PT token names to market contracts
MARKET_BY_PT_TOKEN: dict[str, dict[str, str]] = {
    "plasma": {
        "PT-FUSDT0": "0x0cb289e9df2d0dcfe13732638c89655fb80c2be2",
        "PT-fUSDT0": "0x0cb289e9df2d0dcfe13732638c89655fb80c2be2",  # Case-insensitive support
    },
    "arbitrum": {
        # PT-wstETH-25JUN2026 (active, wstETH PT on Arbitrum)
        "PT-WSTETH-25JUN2026": "0xf78452e0f5c0b95fc5dc8353b8cd1e06e53fa25b",  # Fully uppercase for compiler lookup
        "PT-wstETH-25JUN2026": "0xf78452e0f5c0b95fc5dc8353b8cd1e06e53fa25b",
        "PT-WSTETH": "0xf78452e0f5c0b95fc5dc8353b8cd1e06e53fa25b",
        "PT-wstETH": "0xf78452e0f5c0b95fc5dc8353b8cd1e06e53fa25b",  # Case-insensitive support
    },
    "ethereum": {
        # PT-sUSDe-7MAY2026 (active, 91.5% LLTV on Morpho when market available)
        "PT-SUSDE-7MAY2026": "0x8dae8ece668cf80d348873f23d456448e8694883",  # Fully uppercase for compiler lookup
        "PT-sUSDE-7MAY2026": "0x8dae8ece668cf80d348873f23d456448e8694883",
        "PT-sUSDe-7MAY2026": "0x8dae8ece668cf80d348873f23d456448e8694883",
        "PT-SUSDE": "0x8dae8ece668cf80d348873f23d456448e8694883",
        "PT-sUSDe": "0x8dae8ece668cf80d348873f23d456448e8694883",
        # PT-sUSDe-5FEB2026 (expired, but Morpho market exists for testing)
        "PT-SUSDE-5FEB2026": "0xed81f8ba2941c3979de2265c295748a6b6956567",  # Fully uppercase for compiler lookup
        "PT-sUSDE-5FEB2026": "0xed81f8ba2941c3979de2265c295748a6b6956567",
        "PT-sUSDe-5FEB2026": "0xed81f8ba2941c3979de2265c295748a6b6956567",
    },
}

# Mapping of PT token names to their contract addresses and decimals
# Used for resolving PT tokens when selling (PT -> token)
PT_TOKEN_INFO: dict[str, dict[str, tuple[str, int]]] = {
    "plasma": {
        # PT-fUSDT0: (address, decimals)
        "PT-FUSDT0": ("0xbe45f6f17b81571fc30253bdae0a2a6f7b04d60f", 6),
        "PT-fUSDT0": ("0xbe45f6f17b81571fc30253bdae0a2a6f7b04d60f", 6),
    },
    "arbitrum": {
        # PT-wstETH-25JUN2026: (address, decimals) - wstETH PT uses 18 decimals
        "PT-WSTETH-25JUN2026": ("0x71fbf40651e9d4278a74586afc99f307f369ce9a", 18),  # Fully uppercase for compiler
        "PT-wstETH-25JUN2026": ("0x71fbf40651e9d4278a74586afc99f307f369ce9a", 18),
        "PT-WSTETH": ("0x71fbf40651e9d4278a74586afc99f307f369ce9a", 18),
        "PT-wstETH": ("0x71fbf40651e9d4278a74586afc99f307f369ce9a", 18),
    },
    "ethereum": {
        # PT-sUSDe-7MAY2026: (address, decimals) - active sUSDe PT
        "PT-SUSDE-7MAY2026": ("0x3de0ff76e8b528c092d47b9dac775931cef80f49", 18),  # Fully uppercase for compiler
        "PT-sUSDE-7MAY2026": ("0x3de0ff76e8b528c092d47b9dac775931cef80f49", 18),
        "PT-sUSDe-7MAY2026": ("0x3de0ff76e8b528c092d47b9dac775931cef80f49", 18),
        "PT-SUSDE": ("0x3de0ff76e8b528c092d47b9dac775931cef80f49", 18),
        "PT-sUSDe": ("0x3de0ff76e8b528c092d47b9dac775931cef80f49", 18),
        # PT-sUSDe-5FEB2026: expired but usable for testing
        "PT-SUSDE-5FEB2026": ("0xe8483517077afa11a9b07f849cee2552f040d7b2", 18),  # Fully uppercase for compiler
        "PT-sUSDE-5FEB2026": ("0xe8483517077afa11a9b07f849cee2552f040d7b2", 18),
        "PT-sUSDe-5FEB2026": ("0xe8483517077afa11a9b07f849cee2552f040d7b2", 18),
    },
}

# Mapping of market addresses to the token that mints SY for that market
# This is needed because yield-bearing token markets (like fUSDT0) require
# the yield-bearing token to mint SY, not the underlying token
# Mapping of YT token names to their contract addresses and decimals
# Used for resolving YT tokens when trading (token -> YT, YT -> token)
YT_TOKEN_INFO: dict[str, dict[str, tuple[str, int]]] = {
    "plasma": {
        "YT-FUSDT0": ("0x7b6ad25e30ab1e7f5393e26c3f6bf1f4e8c0138a", 6),
        "YT-fUSDT0": ("0x7b6ad25e30ab1e7f5393e26c3f6bf1f4e8c0138a", 6),
    },
    "ethereum": {
        "YT-sUSDE-7MAY2026": ("0x30775b422b9c7415349855346352faa61fd97e41", 18),
        "YT-sUSDe-7MAY2026": ("0x30775b422b9c7415349855346352faa61fd97e41", 18),
        "YT-sUSDE-5FEB2026": ("0xe36c6c271779c080ba2e68e1e68410291a1b3f7a", 18),
        "YT-sUSDe-5FEB2026": ("0xe36c6c271779c080ba2e68e1e68410291a1b3f7a", 18),
    },
    # NOTE: Arbitrum YT addresses must be verified against Pendle deployments
    # before being added here. Do not use placeholder addresses.
}

# Mapping of YT token names to market addresses (canonical lookup)
# YT tokens share the same market as their corresponding PT tokens
MARKET_BY_YT_TOKEN: dict[str, dict[str, str]] = {
    "plasma": {
        "YT-FUSDT0": "0x0cb289e9df2d0dcfe13732638c89655fb80c2be2",
        "YT-fUSDT0": "0x0cb289e9df2d0dcfe13732638c89655fb80c2be2",
    },
}

MARKET_TOKEN_MINT_SY: dict[str, dict[str, str]] = {
    "plasma": {
        # fUSDT0 market - SY is minted from FUSDT0, not USDT0
        "0x0cb289e9df2d0dcfe13732638c89655fb80c2be2": "0x1dd4b13fcae900c60a350589be8052959d2ed27b",  # FUSDT0
    },
    "arbitrum": {
        # wstETH market - SY is minted from wstETH directly
        "0xf78452e0f5c0b95fc5dc8353b8cd1e06e53fa25b": "0x5979d7b546e38e414f7e9822514be443a4800529",  # wstETH
    },
    "ethereum": {
        # sUSDe-7MAY2026 market - SY is minted from sUSDe
        "0x8dae8ece668cf80d348873f23d456448e8694883": "0x9D39A5DE30e57443BfF2A8307A4256c8797A3497",  # sUSDe
        # sUSDe-5FEB2026 market (expired) - SY is minted from sUSDe
        "0xed81f8ba2941c3979de2265c295748a6b6956567": "0x9D39A5DE30e57443BfF2A8307A4256c8797A3497",  # sUSDe
    },
}


# Gas estimates for Pendle operations
PENDLE_GAS_ESTIMATES: dict[str, int] = {
    "swap_exact_token_for_pt": 400_000,
    "swap_exact_pt_for_token": 400_000,
    "swap_exact_token_for_yt": 450_000,
    "swap_exact_yt_for_token": 450_000,
    "add_liquidity_single": 500_000,
    "remove_liquidity_single": 400_000,
    "add_liquidity_dual": 600_000,
    "mint_sy": 200_000,
    "redeem_sy": 200_000,
    "mint_py": 300_000,
    "redeem_py": 300_000,
    "approve": 50_000,
}

# Function selectors for Pendle Router V4
FUNCTION_SELECTORS: dict[str, str] = {
    # Trading
    "swapExactTokenForPt": "0x1a8631b2",
    "swapExactPtForToken": "0x8f7f3b41",
    "swapExactTokenForYt": "0xc45d2728",
    "swapExactYtForToken": "0xad01bcd8",
    # Liquidity
    "addLiquiditySingleToken": "0x4f2df48f",
    "removeLiquiditySingleToken": "0x79d29f8a",
    "addLiquiditySingleTokenKeepYt": "0x37c8e2e7",
    # SY operations
    "mintSyFromToken": "0x73f9deaa",
    "redeemSyToToken": "0x7b79a4e2",
    # PY operations
    "mintPyFromToken": "0xf9e7fded",
    "redeemPyToToken": "0x7ecc6dbe",
}

# ERC20 approve selector
ERC20_APPROVE_SELECTOR = "0x095ea7b3"
MAX_UINT256 = 2**256 - 1


# =============================================================================
# Data Classes
# =============================================================================


@dataclass
class MarketInfo:
    """Information about a Pendle market."""

    market_address: str
    sy_address: str
    pt_address: str
    yt_address: str
    expiry: int
    underlying_token: str
    underlying_symbol: str

    def is_expired(self, current_timestamp: int) -> bool:
        """Check if the market has expired."""
        return current_timestamp >= self.expiry


@dataclass
class SwapParams:
    """Parameters for a swap operation."""

    receiver: str
    market: str
    min_out: int
    token_in: str
    amount_in: int
    slippage_bps: int = 50  # 0.5% default

    @property
    def amount_out_minimum(self) -> int:
        """Calculate minimum output with slippage."""
        # This would be calculated based on quote
        return int(self.min_out * (10000 - self.slippage_bps) // 10000)


@dataclass
class LiquidityParams:
    """Parameters for liquidity operations."""

    receiver: str
    market: str
    token_in: str
    amount_in: int
    min_lp_out: int
    slippage_bps: int = 50


@dataclass
class PendleTransactionData:
    """Transaction data for Pendle operations."""

    to: str
    value: int
    data: str
    gas_estimate: int
    description: str
    action_type: PendleActionType

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "to": self.to,
            "value": str(self.value),
            "data": self.data,
            "gas_estimate": self.gas_estimate,
            "description": self.description,
            "action_type": self.action_type.value,
        }


@dataclass
class PendleQuote:
    """Quote for a Pendle operation."""

    token_in: str
    token_out: str
    amount_in: int
    amount_out: int
    price_impact_bps: int
    gas_estimate: int
    effective_price: Decimal

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "token_in": self.token_in,
            "token_out": self.token_out,
            "amount_in": str(self.amount_in),
            "amount_out": str(self.amount_out),
            "price_impact_bps": self.price_impact_bps,
            "gas_estimate": self.gas_estimate,
            "effective_price": str(self.effective_price),
        }


# =============================================================================
# Pendle SDK
# =============================================================================


class PendleSDK:
    """
    SDK for interacting with Pendle Protocol.

    Pendle enables yield tokenization and trading through its AMM.
    This SDK builds transactions for:
    - Swapping tokens to/from PT (Principal Token)
    - Swapping tokens to/from YT (Yield Token)
    - Adding/removing liquidity
    - Minting/redeeming SY and PY tokens

    Example:
        sdk = PendleSDK(rpc_url="https://arb1.arbitrum.io/rpc", chain="arbitrum")

        # Build swap transaction (WETH -> PT-wstETH)
        tx = sdk.build_swap_exact_token_for_pt(
            receiver="0x...",
            market="0x...",
            token_in="0x...",  # WETH
            amount_in=10**18,  # 1 WETH
            min_pt_out=10**18,  # Minimum PT to receive
        )
    """

    def __init__(self, rpc_url: str, chain: str = "arbitrum", token_resolver: "TokenResolverType | None" = None):
        """
        Initialize Pendle SDK.

        Args:
            rpc_url: RPC endpoint URL
            chain: Target chain (arbitrum, ethereum)
            token_resolver: Optional TokenResolver instance. If None, uses singleton.
        """
        if chain not in PENDLE_ADDRESSES:
            raise ValueError(f"Unsupported chain: {chain}. Supported: {list(PENDLE_ADDRESSES.keys())}")

        self.web3 = Web3(Web3.HTTPProvider(rpc_url))
        self.chain = chain
        self.addresses = PENDLE_ADDRESSES[chain]
        self.router_address = self.addresses["ROUTER"]

        # Load ABIs
        self.abi_dir = os.path.join(os.path.dirname(__file__), "abis")
        self._router_abi: list[dict] | None = None
        self._erc20_abi: list[dict] | None = None

        # TokenResolver integration
        if token_resolver is not None:
            self._token_resolver = token_resolver
        else:
            from almanak.framework.data.tokens.resolver import get_token_resolver

            self._token_resolver = get_token_resolver()

        logger.info(f"PendleSDK initialized for chain={chain}, router={self.router_address}")

    @property
    def router_abi(self) -> list[dict]:
        """Load router ABI lazily."""
        if self._router_abi is None:
            self._router_abi = self._load_abi("pendle_router")
        return self._router_abi

    @property
    def erc20_abi(self) -> list[dict]:
        """Load ERC20 ABI lazily."""
        if self._erc20_abi is None:
            self._erc20_abi = self._load_abi("erc20")
        return self._erc20_abi

    def _load_abi(self, name: str) -> list[dict]:
        """Load ABI from file."""
        abi_path = os.path.join(self.abi_dir, f"{name}.json")
        if not os.path.exists(abi_path):
            logger.warning(f"ABI file not found: {abi_path}, using minimal ABI")
            return self._get_minimal_abi(name)
        with open(abi_path) as f:
            return json.load(f)

    def _get_minimal_abi(self, name: str) -> list[dict]:
        """Get minimal ABI for contract interaction."""
        if name == "erc20":
            return [
                {
                    "name": "approve",
                    "type": "function",
                    "inputs": [
                        {"name": "spender", "type": "address"},
                        {"name": "amount", "type": "uint256"},
                    ],
                    "outputs": [{"name": "", "type": "bool"}],
                },
                {
                    "name": "balanceOf",
                    "type": "function",
                    "inputs": [{"name": "account", "type": "address"}],
                    "outputs": [{"name": "", "type": "uint256"}],
                },
            ]
        # Return empty list for other ABIs
        return []

    def get_router(self) -> Contract:
        """Get the Pendle Router contract."""
        return self.web3.eth.contract(
            address=self.web3.to_checksum_address(self.router_address),
            abi=self.router_abi,
        )

    # =========================================================================
    # Swap Operations
    # =========================================================================

    def build_swap_exact_token_for_pt(
        self,
        receiver: str,
        market: str,
        token_in: str,
        amount_in: int,
        min_pt_out: int,
        slippage_bps: int = 50,
        token_mint_sy: str | None = None,
    ) -> PendleTransactionData:
        """
        Build a swap transaction from token to PT using swapExactTokenForPtSimple.

        This uses the simplified Pendle V4 function that doesn't require
        ApproxParams or LimitOrderData, making encoding more reliable.

        Args:
            receiver: Address to receive the PT
            market: Market address
            token_in: Input token address
            amount_in: Amount of input token (in wei)
            min_pt_out: Minimum PT to receive
            slippage_bps: Slippage tolerance in basis points
            token_mint_sy: Token that mints SY (defaults to token_in if not specified).
                          For yield-bearing token markets (like fUSDT0), this should be
                          the yield-bearing token address, not the underlying.

        Returns:
            Transaction data for execution
        """
        # Calculate min output with slippage
        min_pt_out_with_slippage = int(min_pt_out * (10000 - slippage_bps) // 10000)

        # Use token_mint_sy if provided, otherwise use token_in
        # For yield-bearing markets, token_mint_sy should be the yield-bearing token
        mint_sy_address = token_mint_sy if token_mint_sy else token_in

        # Build TokenInput struct for Simple function
        # TokenInput: (tokenIn, netTokenIn, tokenMintSy, pendleSwap, swapData)
        # SwapData: (swapType, extRouter, extCalldata, needScale)
        token_input = (
            self.web3.to_checksum_address(token_in),  # tokenIn
            amount_in,  # netTokenIn
            self.web3.to_checksum_address(mint_sy_address),  # tokenMintSy
            "0x0000000000000000000000000000000000000000",  # pendleSwap (no external swap)
            (
                0,
                "0x0000000000000000000000000000000000000000",
                b"",
                False,
            ),  # swapData: (swapType=NONE, extRouter, extCalldata, needScale)
        )

        # Encode using the Simple function (no ApproxParams, no LimitOrderData)
        calldata = self._encode_swap_exact_token_for_pt_simple(
            receiver=receiver,
            market=market,
            min_pt_out=min_pt_out_with_slippage,
            token_input=token_input,
        )

        # Determine if this is a native token swap (ETH)
        # Note: WETH is an ERC-20 requiring approval, only the native ETH address triggers msg.value
        is_native = token_in.lower() == "0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
        value = amount_in if is_native else 0

        return PendleTransactionData(
            to=self.router_address,
            value=value,
            data=calldata,
            gas_estimate=PENDLE_GAS_ESTIMATES["swap_exact_token_for_pt"],
            description=f"Swap {amount_in} token for PT (min: {min_pt_out_with_slippage})",
            action_type=PendleActionType.SWAP_EXACT_TOKEN_FOR_PT,
        )

    def build_swap_exact_pt_for_token(
        self,
        receiver: str,
        market: str,
        pt_amount: int,
        token_out: str,
        min_token_out: int,
        slippage_bps: int = 50,
    ) -> PendleTransactionData:
        """
        Build a swap transaction from PT to token using swapExactPtForTokenSimple.

        This uses the simplified Pendle V4 function that doesn't require
        LimitOrderData, making encoding more reliable.

        Args:
            receiver: Address to receive the token
            market: Market address
            pt_amount: Amount of PT to swap
            token_out: Output token address
            min_token_out: Minimum output token to receive
            slippage_bps: Slippage tolerance in basis points

        Returns:
            Transaction data for execution
        """
        min_token_out_with_slippage = int(min_token_out * (10000 - slippage_bps) // 10000)

        # Build TokenOutput struct for Simple function
        # TokenOutput: (tokenOut, minTokenOut, tokenRedeemSy, pendleSwap, swapData)
        # SwapData: (swapType, extRouter, extCalldata, needScale)
        token_output = (
            self.web3.to_checksum_address(token_out),  # tokenOut
            min_token_out_with_slippage,  # minTokenOut
            self.web3.to_checksum_address(token_out),  # tokenRedeemSy
            "0x0000000000000000000000000000000000000000",  # pendleSwap
            (
                0,
                "0x0000000000000000000000000000000000000000",
                b"",
                False,
            ),  # swapData: (swapType=NONE, extRouter, extCalldata, needScale)
        )

        calldata = self._encode_swap_exact_pt_for_token_simple(
            receiver=receiver,
            market=market,
            pt_amount=pt_amount,
            token_output=token_output,
        )

        return PendleTransactionData(
            to=self.router_address,
            value=0,
            data=calldata,
            gas_estimate=PENDLE_GAS_ESTIMATES["swap_exact_pt_for_token"],
            description=f"Swap {pt_amount} PT for token (min: {min_token_out_with_slippage})",
            action_type=PendleActionType.SWAP_EXACT_PT_FOR_TOKEN,
        )

    def build_swap_exact_token_for_yt(
        self,
        receiver: str,
        market: str,
        token_in: str,
        amount_in: int,
        min_yt_out: int,
        slippage_bps: int = 50,
        token_mint_sy: str | None = None,
    ) -> PendleTransactionData:
        """
        Build a swap transaction from token to YT using swapExactTokenForYt.

        Unlike PT swaps which use the Simple variant, YT swaps require
        ApproxParams for binary search of optimal flash swap size, plus
        LimitOrderData.

        Args:
            receiver: Address to receive the YT
            market: Market address
            token_in: Input token address
            amount_in: Amount of input token (in wei)
            min_yt_out: Minimum YT to receive
            slippage_bps: Slippage tolerance in basis points
            token_mint_sy: Token that mints SY (defaults to token_in)

        Returns:
            Transaction data for execution
        """
        min_yt_out_with_slippage = int(min_yt_out * (10000 - slippage_bps) // 10000)

        mint_sy_address = token_mint_sy if token_mint_sy else token_in

        # Build TokenInput struct
        token_input = (
            self.web3.to_checksum_address(token_in),
            amount_in,
            self.web3.to_checksum_address(mint_sy_address),
            "0x0000000000000000000000000000000000000000",
            (0, "0x0000000000000000000000000000000000000000", b"", False),
        )

        # ApproxParams for YT binary search: (guessMin, guessMax, guessOffchain, maxIteration, eps)
        approx_params = (0, 2**256 - 1, 0, 256, 10**14)

        # LimitOrderData (empty - no limit orders)
        limit_order_data: tuple[Any, ...] = (
            "0x0000000000000000000000000000000000000000",
            0,
            [],
            [],
            b"",
        )

        calldata = self._encode_swap_exact_token_for_yt(
            receiver=receiver,
            market=market,
            min_yt_out=min_yt_out_with_slippage,
            approx_params=approx_params,
            token_input=token_input,
            limit_order_data=limit_order_data,
        )

        is_native = token_in.lower() == "0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
        value = amount_in if is_native else 0

        return PendleTransactionData(
            to=self.router_address,
            value=value,
            data=calldata,
            gas_estimate=PENDLE_GAS_ESTIMATES["swap_exact_token_for_yt"],
            description=f"Swap {amount_in} token for YT (min: {min_yt_out_with_slippage})",
            action_type=PendleActionType.SWAP_EXACT_TOKEN_FOR_YT,
        )

    def build_swap_exact_yt_for_token(
        self,
        receiver: str,
        market: str,
        yt_amount: int,
        token_out: str,
        min_token_out: int,
        slippage_bps: int = 50,
    ) -> PendleTransactionData:
        """
        Build a swap transaction from YT to token using swapExactYtForToken.

        Args:
            receiver: Address to receive the token
            market: Market address
            yt_amount: Amount of YT to swap
            token_out: Output token address
            min_token_out: Minimum output token to receive
            slippage_bps: Slippage tolerance in basis points

        Returns:
            Transaction data for execution
        """
        min_token_out_with_slippage = int(min_token_out * (10000 - slippage_bps) // 10000)

        # Build TokenOutput struct
        token_output = (
            self.web3.to_checksum_address(token_out),
            min_token_out_with_slippage,
            self.web3.to_checksum_address(token_out),
            "0x0000000000000000000000000000000000000000",
            (0, "0x0000000000000000000000000000000000000000", b"", False),
        )

        # LimitOrderData (empty)
        limit_order_data: tuple[Any, ...] = (
            "0x0000000000000000000000000000000000000000",
            0,
            [],
            [],
            b"",
        )

        calldata = self._encode_swap_exact_yt_for_token(
            receiver=receiver,
            market=market,
            yt_amount=yt_amount,
            token_output=token_output,
            limit_order_data=limit_order_data,
        )

        return PendleTransactionData(
            to=self.router_address,
            value=0,
            data=calldata,
            gas_estimate=PENDLE_GAS_ESTIMATES["swap_exact_yt_for_token"],
            description=f"Swap {yt_amount} YT for token (min: {min_token_out_with_slippage})",
            action_type=PendleActionType.SWAP_EXACT_YT_FOR_TOKEN,
        )

    # =========================================================================
    # Liquidity Operations
    # =========================================================================

    def build_add_liquidity_single_token(
        self,
        receiver: str,
        market: str,
        token_in: str,
        amount_in: int,
        min_lp_out: int,
        slippage_bps: int = 50,
    ) -> PendleTransactionData:
        """
        Build a transaction to add liquidity with a single token.

        This adds liquidity to a Pendle market using a single input token.
        The router handles conversion to the proper ratio of SY and PT.

        Args:
            receiver: Address to receive LP tokens
            market: Market address
            token_in: Input token address
            amount_in: Amount of input token
            min_lp_out: Minimum LP tokens to receive
            slippage_bps: Slippage tolerance in basis points

        Returns:
            Transaction data for execution
        """
        min_lp_out_with_slippage = int(min_lp_out * (10000 - slippage_bps) // 10000)

        # Build TokenInput struct
        token_input = (
            self.web3.to_checksum_address(token_in),
            amount_in,
            self.web3.to_checksum_address(token_in),
            "0x0000000000000000000000000000000000000000",
            (0, "0x0000000000000000000000000000000000000000", b"", False),
        )

        # ApproxParams for liquidity calculation
        approx_params = (
            0,
            2**256 - 1,
            0,
            256,
            10**14,
        )

        # LimitOrderData (empty)
        limit_order_data: tuple[Any, ...] = (
            "0x0000000000000000000000000000000000000000",
            0,
            [],
            [],
            b"",
        )

        calldata = self._encode_add_liquidity_single_token(
            receiver=receiver,
            market=market,
            min_lp_out=min_lp_out_with_slippage,
            approx_params=approx_params,
            token_input=token_input,
            limit_order_data=limit_order_data,
        )

        # Check for native token
        # Note: WETH is an ERC-20 requiring approval, only the native ETH address triggers msg.value
        is_native = token_in.lower() == "0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
        value = amount_in if is_native else 0

        return PendleTransactionData(
            to=self.router_address,
            value=value,
            data=calldata,
            gas_estimate=PENDLE_GAS_ESTIMATES["add_liquidity_single"],
            description=f"Add liquidity with {amount_in} token (min LP: {min_lp_out_with_slippage})",
            action_type=PendleActionType.ADD_LIQUIDITY_SINGLE_TOKEN,
        )

    def build_remove_liquidity_single_token(
        self,
        receiver: str,
        market: str,
        lp_amount: int,
        token_out: str,
        min_token_out: int,
        slippage_bps: int = 50,
    ) -> PendleTransactionData:
        """
        Build a transaction to remove liquidity to a single token.

        Args:
            receiver: Address to receive output token
            market: Market address
            lp_amount: Amount of LP tokens to burn
            token_out: Output token address
            min_token_out: Minimum output token to receive
            slippage_bps: Slippage tolerance in basis points

        Returns:
            Transaction data for execution
        """
        min_token_out_with_slippage = int(min_token_out * (10000 - slippage_bps) // 10000)

        # Build TokenOutput struct
        token_output = (
            self.web3.to_checksum_address(token_out),
            min_token_out_with_slippage,
            self.web3.to_checksum_address(token_out),
            "0x0000000000000000000000000000000000000000",
            (0, "0x0000000000000000000000000000000000000000", b"", False),
        )

        # LimitOrderData (empty)
        limit_order_data: tuple[Any, ...] = (
            "0x0000000000000000000000000000000000000000",
            0,
            [],
            [],
            b"",
        )

        calldata = self._encode_remove_liquidity_single_token(
            receiver=receiver,
            market=market,
            lp_amount=lp_amount,
            token_output=token_output,
            limit_order_data=limit_order_data,
        )

        return PendleTransactionData(
            to=self.router_address,
            value=0,
            data=calldata,
            gas_estimate=PENDLE_GAS_ESTIMATES["remove_liquidity_single"],
            description=f"Remove {lp_amount} LP for token (min: {min_token_out_with_slippage})",
            action_type=PendleActionType.REMOVE_LIQUIDITY_SINGLE_TOKEN,
        )

    # =========================================================================
    # PY (PT + YT) Operations
    # =========================================================================

    def build_redeem_py_to_token(
        self,
        receiver: str,
        yt_address: str,
        py_amount: int,
        token_out: str,
        min_token_out: int,
        slippage_bps: int = 50,
    ) -> PendleTransactionData:
        """
        Build a transaction to redeem PT+YT to token.

        After maturity, PT can be redeemed 1:1 for the underlying.
        Before maturity, you need equal amounts of PT and YT to redeem.

        Args:
            receiver: Address to receive output token
            yt_address: YT contract address
            py_amount: Amount of PT+YT to redeem
            token_out: Output token address
            min_token_out: Minimum output token
            slippage_bps: Slippage tolerance

        Returns:
            Transaction data for execution
        """
        min_token_out_with_slippage = int(min_token_out * (10000 - slippage_bps) // 10000)

        # Build TokenOutput struct
        token_output = (
            self.web3.to_checksum_address(token_out),
            min_token_out_with_slippage,
            self.web3.to_checksum_address(token_out),
            "0x0000000000000000000000000000000000000000",
            (0, "0x0000000000000000000000000000000000000000", b"", False),
        )

        calldata = self._encode_redeem_py_to_token(
            receiver=receiver,
            yt_address=yt_address,
            py_amount=py_amount,
            token_output=token_output,
        )

        return PendleTransactionData(
            to=self.router_address,
            value=0,
            data=calldata,
            gas_estimate=PENDLE_GAS_ESTIMATES["redeem_py"],
            description=f"Redeem {py_amount} PY for token (min: {min_token_out_with_slippage})",
            action_type=PendleActionType.REDEEM_PY_TO_TOKEN,
        )

    # =========================================================================
    # Approval Helpers
    # =========================================================================

    def build_approve_tx(
        self,
        token_address: str,
        spender: str | None = None,
        amount: int = MAX_UINT256,
    ) -> PendleTransactionData:
        """
        Build an ERC-20 approval transaction.

        Args:
            token_address: Token to approve
            spender: Spender address (defaults to router)
            amount: Amount to approve (defaults to max)

        Returns:
            Transaction data for execution
        """
        if spender is None:
            spender = self.router_address

        calldata = ERC20_APPROVE_SELECTOR + self._pad_address(spender) + self._pad_uint256(amount)

        return PendleTransactionData(
            to=token_address,
            value=0,
            data=calldata,
            gas_estimate=PENDLE_GAS_ESTIMATES["approve"],
            description="Approve token for Pendle Router",
            action_type=PendleActionType.APPROVE,
        )

    # =========================================================================
    # Encoding Helpers
    # =========================================================================

    def _encode_swap_exact_token_for_pt_simple(
        self,
        receiver: str,
        market: str,
        min_pt_out: int,
        token_input: tuple,
    ) -> str:
        """Encode swapExactTokenForPtSimple calldata using ABI encoding.

        This uses the simplified V4 function that doesn't require ApproxParams
        or LimitOrderData, making it more reliable for standard swaps.
        """
        router = self.get_router()

        receiver = self.web3.to_checksum_address(receiver)
        market = self.web3.to_checksum_address(market)

        calldata = router.encode_abi(
            "swapExactTokenForPtSimple",
            args=[
                receiver,
                market,
                min_pt_out,
                token_input,
            ],
        )

        return calldata

    def _encode_swap_exact_pt_for_token_simple(
        self,
        receiver: str,
        market: str,
        pt_amount: int,
        token_output: tuple,
    ) -> str:
        """Encode swapExactPtForTokenSimple calldata using ABI encoding."""
        router = self.get_router()

        receiver = self.web3.to_checksum_address(receiver)
        market = self.web3.to_checksum_address(market)

        calldata = router.encode_abi(
            "swapExactPtForTokenSimple",
            args=[
                receiver,
                market,
                pt_amount,
                token_output,
            ],
        )

        return calldata

    def _encode_add_liquidity_single_token(
        self,
        receiver: str,
        market: str,
        min_lp_out: int,
        approx_params: tuple,
        token_input: tuple,
        limit_order_data: tuple,
    ) -> str:
        """Encode addLiquiditySingleToken calldata using ABI encoding."""
        router = self.get_router()

        receiver = self.web3.to_checksum_address(receiver)
        market = self.web3.to_checksum_address(market)

        calldata = router.encode_abi(
            "addLiquiditySingleToken",
            args=[
                receiver,
                market,
                min_lp_out,
                approx_params,
                token_input,
                limit_order_data,
            ],
        )

        return calldata

    def _encode_remove_liquidity_single_token(
        self,
        receiver: str,
        market: str,
        lp_amount: int,
        token_output: tuple,
        limit_order_data: tuple,
    ) -> str:
        """Encode removeLiquiditySingleToken calldata using ABI encoding."""
        router = self.get_router()

        receiver = self.web3.to_checksum_address(receiver)
        market = self.web3.to_checksum_address(market)

        calldata = router.encode_abi(
            "removeLiquiditySingleToken",
            args=[
                receiver,
                market,
                lp_amount,
                token_output,
                limit_order_data,
            ],
        )

        return calldata

    def _encode_redeem_py_to_token(
        self,
        receiver: str,
        yt_address: str,
        py_amount: int,
        token_output: tuple,
    ) -> str:
        """Encode redeemPyToToken calldata using ABI encoding."""
        router = self.get_router()

        receiver = self.web3.to_checksum_address(receiver)
        yt_address = self.web3.to_checksum_address(yt_address)

        calldata = router.encode_abi(
            "redeemPyToToken",
            args=[
                receiver,
                yt_address,
                py_amount,
                token_output,
            ],
        )

        return calldata

    def _encode_swap_exact_token_for_yt(
        self,
        receiver: str,
        market: str,
        min_yt_out: int,
        approx_params: tuple,
        token_input: tuple,
        limit_order_data: tuple,
    ) -> str:
        """Encode swapExactTokenForYt calldata using ABI encoding.

        Unlike PT swaps which use the Simple variant, YT swaps require
        ApproxParams (binary search for flash swap size) and LimitOrderData.
        """
        router = self.get_router()

        receiver = self.web3.to_checksum_address(receiver)
        market = self.web3.to_checksum_address(market)

        calldata = router.encode_abi(
            "swapExactTokenForYt",
            args=[
                receiver,
                market,
                min_yt_out,
                approx_params,
                token_input,
                limit_order_data,
            ],
        )

        return calldata

    def _encode_swap_exact_yt_for_token(
        self,
        receiver: str,
        market: str,
        yt_amount: int,
        token_output: tuple,
        limit_order_data: tuple,
    ) -> str:
        """Encode swapExactYtForToken calldata using ABI encoding."""
        router = self.get_router()

        receiver = self.web3.to_checksum_address(receiver)
        market = self.web3.to_checksum_address(market)

        calldata = router.encode_abi(
            "swapExactYtForToken",
            args=[
                receiver,
                market,
                yt_amount,
                token_output,
                limit_order_data,
            ],
        )

        return calldata

    @staticmethod
    def _pad_address(addr: str) -> str:
        """Pad address to 32 bytes."""
        return addr.lower().replace("0x", "").zfill(64)

    @staticmethod
    def _pad_uint256(value: int) -> str:
        """Pad uint256 to 32 bytes."""
        return hex(value)[2:].zfill(64)


# =============================================================================
# Factory Function
# =============================================================================


def get_pendle_sdk(rpc_url: str, chain: str = "arbitrum") -> PendleSDK:
    """Factory function to create a PendleSDK instance."""
    return PendleSDK(rpc_url, chain)


__all__ = [
    "LiquidityParams",
    "MARKET_BY_PT_TOKEN",
    "MARKET_BY_YT_TOKEN",
    "MARKET_TOKEN_MINT_SY",
    "MarketInfo",
    "PENDLE_ADDRESSES",
    "PENDLE_GAS_ESTIMATES",
    "PT_TOKEN_INFO",
    "PendleActionType",
    "PendleQuote",
    "PendleSDK",
    "PendleTransactionData",
    "SwapParams",
    "YT_TOKEN_INFO",
    "get_pendle_sdk",
]
