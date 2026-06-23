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
    from almanak.framework.gateway_client import GatewayClient

from .addresses import PENDLE as _PENDLE_REGISTRY
from .addresses import PENDLE_TOKENS as _PENDLE_TOKENS

logger = logging.getLogger(__name__)

# Bound on each blocking web3 RPC request in the direct/HTTPProvider fallback (the
# local-dev / no-gateway compile path). Without it web3 defaults to no timeout, so a
# slow or unresponsive RPC wedges the caller indefinitely. Mirrors on_chain_reader's
# ``request_timeout_seconds`` default (30s) and the gateway's own eth_call timeout.
_PENDLE_SDK_RPC_TIMEOUT_SECONDS = 30.0


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
        "PT-FUSDT0": "0x0cb289E9df2d0dCFe13732638C89655fb80C2bE2",
        "PT-fUSDT0": "0x0cb289E9df2d0dCFe13732638C89655fb80C2bE2",  # Case-insensitive support
    },
    "arbitrum": {
        # PT-wstETH-25JUN2026 (active, wstETH PT on Arbitrum)
        "PT-WSTETH-25JUN2026": "0xf78452e0f5C0B95fc5dC8353B8CD1e06E53fa25B",  # Fully uppercase for compiler lookup
        "PT-wstETH-25JUN2026": "0xf78452e0f5C0B95fc5dC8353B8CD1e06E53fa25B",
        "PT-WSTETH": "0xf78452e0f5C0B95fc5dC8353B8CD1e06E53fa25B",
        "PT-wstETH": "0xf78452e0f5C0B95fc5dC8353B8CD1e06E53fa25B",  # Case-insensitive support
        # PT-sUSDai-15OCT2026 (Pendle PT dislocation, Exp8)
        "PT-SUSDAI-15OCT2026": "0xcbf629c8d396b1261f81f55175afa010e94787d8",
        "PT-sUSDai-15OCT2026": "0xcbf629c8d396b1261f81f55175afa010e94787d8",
    },
    "ethereum": {
        # NOTE ON ORDER: permission_hints._market_grid() picks the FIRST
        # fully-supported market in this dict's insertion order as the canonical
        # Zodiac synthetic / LP_CLOSE market for ethereum. The sUSDe-13AUG2026
        # market MUST stay first so the on-chain Zodiac LP_CLOSE coverage
        # (tests/intents/ethereum/test_pendle_lp.py::test_lp_close_returns_susde)
        # keeps resolving to it. The VIB-5324 stETH demo-roll market is appended
        # below — it is resolved by symbol for the SWAP demo and does not need to
        # be the canonical synthetic market.
        # PT-sUSDe-13AUG2026 (active sUSDe market — replaced 7MAY2026 after expiry)
        "PT-SUSDE-13AUG2026": "0x177768caf9d0e036725a51d3f60d7e20f2d4d194",
        "PT-sUSDE-13AUG2026": "0x177768caf9d0e036725a51d3f60d7e20f2d4d194",
        "PT-sUSDe-13AUG2026": "0x177768caf9d0e036725a51d3f60d7e20f2d4d194",
        # PT-stETH-30DEC2027 (long-dated wstETH/stETH market — VIB-5324 breadth +
        # durable demo roll target). On-chain-verified via readTokens(): market
        # expiry() = 1830124800 (2027-12-30 UTC). SY-stETH accepts native ETH,
        # WETH, stETH and wstETH as mint inputs (getTokensIn), so funding wstETH
        # mints SY directly without a V3 pre-swap. Both stETH and wstETH aliases
        # are registered because Pendle's UI labels this the "wstETH" market while
        # the on-chain PT symbol is PT-stETH-30DEC2027. Kept AFTER sUSDe so it is
        # NOT the canonical synthetic market (see ORDER note above).
        "PT-STETH-30DEC2027": "0x34280882267ffa6383B363E278B027Be083bBe3b",
        "PT-stETH-30DEC2027": "0x34280882267ffa6383B363E278B027Be083bBe3b",
        "PT-WSTETH-30DEC2027": "0x34280882267ffa6383B363E278B027Be083bBe3b",
        "PT-wstETH-30DEC2027": "0x34280882267ffa6383B363E278B027Be083bBe3b",
        # PT-sUSDe-7MAY2026 (expired 2026-05-07; kept for historical receipt parsing)
        "PT-SUSDE-7MAY2026": "0x8dAe8ECe668cf80d348873F23D456448E8694883",  # Fully uppercase for compiler lookup
        "PT-sUSDE-7MAY2026": "0x8dAe8ECe668cf80d348873F23D456448E8694883",
        "PT-sUSDe-7MAY2026": "0x8dAe8ECe668cf80d348873F23D456448E8694883",
        "PT-SUSDE": "0x8dAe8ECe668cf80d348873F23D456448E8694883",
        "PT-sUSDe": "0x8dAe8ECe668cf80d348873F23D456448E8694883",
        # PT-sUSDe-5FEB2026 (expired, but Morpho market exists for testing)
        "PT-SUSDE-5FEB2026": "0xed81f8bA2941C3979de2265C295748a6b6956567",  # Fully uppercase for compiler lookup
        "PT-sUSDE-5FEB2026": "0xed81f8bA2941C3979de2265C295748a6b6956567",
        "PT-sUSDe-5FEB2026": "0xed81f8bA2941C3979de2265C295748a6b6956567",
        # PT-USDG-28MAY2026 (Pendle PT dislocation, Exp8)
        "PT-USDG-28MAY2026": "0xc5b32dba5f29f8395fb9591e1a15f23a75214f33",
        # PT-aPYUSD-28MAY2026 (Pendle PT dislocation, Exp8)
        "PT-APYUSD-28MAY2026": "0x5d88790d68c45d2cec4b7b1ad842587e1c51188a",
        "PT-aPYUSD-28MAY2026": "0x5d88790d68c45d2cec4b7b1ad842587e1c51188a",
    },
}

# Mapping of PT token names to their contract addresses and decimals
# Used for resolving PT tokens when selling (PT -> token)
PT_TOKEN_INFO: dict[str, dict[str, tuple[str, int]]] = {
    "plasma": {
        # PT-fUSDT0: (address, decimals)
        "PT-FUSDT0": ("0xbE45F6F17b81571fC30253BDaE0A2A6f7b04D60F", 6),
        "PT-fUSDT0": ("0xbE45F6F17b81571fC30253BDaE0A2A6f7b04D60F", 6),
    },
    "arbitrum": {
        # PT-wstETH-25JUN2026: (address, decimals) - wstETH PT uses 18 decimals
        "PT-WSTETH-25JUN2026": ("0x71fBF40651E9D4278a74586AfC99F307f369Ce9A", 18),  # Fully uppercase for compiler
        "PT-wstETH-25JUN2026": ("0x71fBF40651E9D4278a74586AfC99F307f369Ce9A", 18),
        "PT-WSTETH": ("0x71fBF40651E9D4278a74586AfC99F307f369Ce9A", 18),
        "PT-wstETH": ("0x71fBF40651E9D4278a74586AfC99F307f369Ce9A", 18),
        # PT-sUSDai-15OCT2026: (address, decimals) - Pendle PT dislocation, Exp8
        "PT-SUSDAI-15OCT2026": ("0xb459db106f645d698e74027eef6019a26a0675cc", 18),
        "PT-sUSDai-15OCT2026": ("0xb459db106f645d698e74027eef6019a26a0675cc", 18),
    },
    "ethereum": {
        # PT-stETH-30DEC2027: (address, decimals) - long-dated wstETH/stETH PT.
        # On-chain-verified PT address via market.readTokens() (VIB-5324).
        "PT-STETH-30DEC2027": ("0xb253Eff1104802b97aC7E3aC9FdD73AecE295a2c", 18),
        "PT-stETH-30DEC2027": ("0xb253Eff1104802b97aC7E3aC9FdD73AecE295a2c", 18),
        "PT-WSTETH-30DEC2027": ("0xb253Eff1104802b97aC7E3aC9FdD73AecE295a2c", 18),
        "PT-wstETH-30DEC2027": ("0xb253Eff1104802b97aC7E3aC9FdD73AecE295a2c", 18),
        # PT-sUSDe-13AUG2026: (address, decimals) - active sUSDe PT (replaced 7MAY2026)
        "PT-SUSDE-13AUG2026": ("0x5a19fa369f2895dcd8d2cee62e4ceae58ef92bbb", 18),
        "PT-sUSDE-13AUG2026": ("0x5a19fa369f2895dcd8d2cee62e4ceae58ef92bbb", 18),
        "PT-sUSDe-13AUG2026": ("0x5a19fa369f2895dcd8d2cee62e4ceae58ef92bbb", 18),
        # PT-sUSDe-7MAY2026: (address, decimals) - expired 2026-05-07, kept for parsing
        "PT-SUSDE-7MAY2026": ("0x3de0ff76E8b528C092d47b9DaC775931cef80F49", 18),  # Fully uppercase for compiler
        "PT-sUSDE-7MAY2026": ("0x3de0ff76E8b528C092d47b9DaC775931cef80F49", 18),
        "PT-sUSDe-7MAY2026": ("0x3de0ff76E8b528C092d47b9DaC775931cef80F49", 18),
        "PT-SUSDE": ("0x3de0ff76E8b528C092d47b9DaC775931cef80F49", 18),
        "PT-sUSDe": ("0x3de0ff76E8b528C092d47b9DaC775931cef80F49", 18),
        # PT-sUSDe-5FEB2026: expired but usable for testing
        "PT-SUSDE-5FEB2026": ("0xE8483517077afa11A9B07f849cee2552f040d7b2", 18),  # Fully uppercase for compiler
        "PT-sUSDE-5FEB2026": ("0xE8483517077afa11A9B07f849cee2552f040d7b2", 18),
        "PT-sUSDe-5FEB2026": ("0xE8483517077afa11A9B07f849cee2552f040d7b2", 18),
        # PT-USDG-28MAY2026: Pendle PT dislocation, Exp8
        "PT-USDG-28MAY2026": ("0x9db38d74a0d29380899ad354121dfb521adb0548", 18),
        # PT-aPYUSD-28MAY2026: Pendle PT dislocation, Exp8
        "PT-APYUSD-28MAY2026": ("0x790cd0b90a73a506106dc184be478041547fe00f", 18),
        "PT-aPYUSD-28MAY2026": ("0x790cd0b90a73a506106dc184be478041547fe00f", 18),
    },
}

# Mapping of market addresses to the token that mints SY for that market
# This is needed because yield-bearing token markets (like fUSDT0) require
# the yield-bearing token to mint SY, not the underlying token
# Mapping of YT token names to their contract addresses and decimals
# Used for resolving YT tokens when trading (token -> YT, YT -> token)
YT_TOKEN_INFO: dict[str, dict[str, tuple[str, int]]] = {
    "plasma": {
        "YT-FUSDT0": ("0x7B6aD25E30AB1E7F5393E26C3F6bF1f4e8C0138A", 6),
        "YT-fUSDT0": ("0x7B6aD25E30AB1E7F5393E26C3F6bF1f4e8C0138A", 6),
    },
    "ethereum": {
        # YT-stETH-30DEC2027 — long-dated wstETH/stETH YT. On-chain-verified YT
        # address via market.readTokens() (VIB-5324). Shares market 0x342808...
        "YT-STETH-30DEC2027": ("0x04B7Fa1e727d7290D6E24fA9b426d0c940283a95", 18),
        "YT-stETH-30DEC2027": ("0x04B7Fa1e727d7290D6E24fA9b426d0c940283a95", 18),
        "YT-WSTETH-30DEC2027": ("0x04B7Fa1e727d7290D6E24fA9b426d0c940283a95", 18),
        "YT-wstETH-30DEC2027": ("0x04B7Fa1e727d7290D6E24fA9b426d0c940283a95", 18),
        # YT-sUSDe-13AUG2026 — active sUSDe YT (replaced 7MAY2026 after expiry)
        "YT-SUSDE-13AUG2026": ("0x45a699a11a4a17fe0931ef3cea4bfc3235e659f2", 18),
        "YT-sUSDE-13AUG2026": ("0x45a699a11a4a17fe0931ef3cea4bfc3235e659f2", 18),
        "YT-sUSDe-13AUG2026": ("0x45a699a11a4a17fe0931ef3cea4bfc3235e659f2", 18),
        # YT-sUSDe-7MAY2026 — expired 2026-05-07, kept for historical receipt parsing
        "YT-sUSDE-7MAY2026": ("0x30775B422b9c7415349855346352FAA61fD97E41", 18),
        "YT-sUSDe-7MAY2026": ("0x30775B422b9c7415349855346352FAA61fD97E41", 18),
        "YT-sUSDE-5FEB2026": ("0xe36c6c271779C080Ba2e68E1E68410291a1b3F7A", 18),
        "YT-sUSDe-5FEB2026": ("0xe36c6c271779C080Ba2e68E1E68410291a1b3F7A", 18),
    },
    "arbitrum": {
        # YT-wstETH-25JUN2026: verified via readTokens() on market 0xf78452e...
        "YT-WSTETH-25JUN2026": ("0x25bda1edd6af17c61399aa0eb84b93daa3069764", 18),
        "YT-wstETH-25JUN2026": ("0x25bda1edd6af17c61399aa0eb84b93daa3069764", 18),
        "YT-WSTETH": ("0x25bda1edd6af17c61399aa0eb84b93daa3069764", 18),
        "YT-wstETH": ("0x25bda1edd6af17c61399aa0eb84b93daa3069764", 18),
    },
}

# Mapping of YT token names to market addresses (canonical lookup)
# YT tokens share the same market as their corresponding PT tokens
MARKET_BY_YT_TOKEN: dict[str, dict[str, str]] = {
    "plasma": {
        "YT-FUSDT0": "0x0cb289E9df2d0dCFe13732638C89655fb80C2bE2",
        "YT-fUSDT0": "0x0cb289E9df2d0dCFe13732638C89655fb80C2bE2",
    },
    "arbitrum": {
        # YT-wstETH-25JUN2026 shares the same market as PT-wstETH-25JUN2026
        "YT-WSTETH-25JUN2026": "0xf78452e0f5c0b95fc5dc8353b8cd1e06e53fa25b",
        "YT-wstETH-25JUN2026": "0xf78452e0f5c0b95fc5dc8353b8cd1e06e53fa25b",
        "YT-WSTETH": "0xf78452e0f5c0b95fc5dc8353b8cd1e06e53fa25b",
        "YT-wstETH": "0xf78452e0f5c0b95fc5dc8353b8cd1e06e53fa25b",
    },
    "ethereum": {
        # YT-stETH-30DEC2027 shares the market 0x342808... with PT-stETH-30DEC2027
        "YT-STETH-30DEC2027": "0x34280882267ffa6383B363E278B027Be083bBe3b",
        "YT-stETH-30DEC2027": "0x34280882267ffa6383B363E278B027Be083bBe3b",
        "YT-WSTETH-30DEC2027": "0x34280882267ffa6383B363E278B027Be083bBe3b",
        "YT-wstETH-30DEC2027": "0x34280882267ffa6383B363E278B027Be083bBe3b",
        # YT-sUSDe-13AUG2026 shares the market 0x177768... with PT-sUSDe-13AUG2026
        "YT-SUSDE-13AUG2026": "0x177768caf9d0e036725a51d3f60d7e20f2d4d194",
        "YT-sUSDE-13AUG2026": "0x177768caf9d0e036725a51d3f60d7e20f2d4d194",
        "YT-sUSDe-13AUG2026": "0x177768caf9d0e036725a51d3f60d7e20f2d4d194",
        "YT-SUSDE-7MAY2026": "0x8dAe8ECe668cf80d348873F23D456448E8694883",
        "YT-sUSDE-7MAY2026": "0x8dAe8ECe668cf80d348873F23D456448E8694883",
        "YT-sUSDe-7MAY2026": "0x8dAe8ECe668cf80d348873F23D456448E8694883",
        "YT-SUSDE-5FEB2026": "0xed81f8bA2941C3979de2265C295748a6b6956567",
        "YT-sUSDE-5FEB2026": "0xed81f8bA2941C3979de2265C295748a6b6956567",
        "YT-sUSDe-5FEB2026": "0xed81f8bA2941C3979de2265C295748a6b6956567",
    },
}

MARKET_TOKEN_MINT_SY: dict[str, dict[str, str]] = {
    "plasma": {
        # fUSDT0 market - SY is minted from FUSDT0, not USDT0
        "0x0cb289e9df2d0dcfe13732638c89655fb80c2be2": "0x1DD4b13fcAE900C60a350589BE8052959D2Ed27B",  # FUSDT0
    },
    "arbitrum": {
        # wstETH market - SY is minted from wstETH directly
        "0xf78452e0f5c0b95fc5dc8353b8cd1e06e53fa25b": "0x5979D7b546E38E414F7E9822514be443A4800529",  # wstETH
        # sUSDai-15OCT2026 market - SY is minted from sUSDai (VIB-2534, Exp8)
        "0xcbf629c8d396b1261f81f55175afa010e94787d8": "0x0b2b2b2076d95dda7817e785989fe353fe955ef9",  # sUSDai
    },
    "ethereum": {
        # stETH-30DEC2027 market — SY-stETH accepts native ETH / WETH / stETH /
        # wstETH as mint inputs (getTokensIn). We pin wstETH because it is the
        # Anvil-fundable, non-rebasing demo funding token, so from_token=WSTETH
        # equals tokenMintSy and no V3 pre-swap leg is inserted (VIB-5324).
        "0x34280882267ffa6383b363e278b027be083bbe3b": "0x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0",  # wstETH
        # sUSDe-13AUG2026 market - SY is minted from sUSDe (active, replaced 7MAY2026)
        "0x177768caf9d0e036725a51d3f60d7e20f2d4d194": "0x9D39A5DE30e57443BfF2A8307A4256c8797A3497",  # sUSDe
        # sUSDe-7MAY2026 market - SY is minted from sUSDe
        "0x8dae8ece668cf80d348873f23d456448e8694883": "0x9D39A5DE30e57443BfF2A8307A4256c8797A3497",  # sUSDe
        # sUSDe-5FEB2026 market (expired) - SY is minted from sUSDe
        "0xed81f8ba2941c3979de2265c295748a6b6956567": "0x9D39A5DE30e57443BfF2A8307A4256c8797A3497",  # sUSDe
        # USDG-28MAY2026 market - SY is minted from USDG (VIB-2534, Exp8)
        "0xc5b32dba5f29f8395fb9591e1a15f23a75214f33": "0xe343167631d89b6ffc58b88d6b7fb0228795491d",  # USDG
        # aPYUSD-28MAY2026 market - SY is minted from aEthPYUSD (VIB-2534, Exp8)
        "0x5d88790d68c45d2cec4b7b1ad842587e1c51188a": "0x0c0d01abf3e6adfca0989ebba9d6e85dd58eab1e",  # aEthPYUSD
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

    def __init__(
        self,
        rpc_url: str | None = None,
        chain: str = "arbitrum",
        token_resolver: "TokenResolverType | None" = None,
        gateway_client: "GatewayClient | None" = None,
    ):
        """
        Initialize Pendle SDK.

        Args:
            rpc_url: DEPRECATED — direct RPC URL. Prefer gateway_client for
                any code path running in a strategy container.
            chain: Target chain (arbitrum, ethereum)
            token_resolver: Optional TokenResolver instance. If None, uses singleton.
            gateway_client: Gateway client for routing eth_call through the
                gateway's RpcService. Preferred over rpc_url.
        """
        if chain not in PENDLE_ADDRESSES:
            raise ValueError(f"Unsupported chain: {chain}. Supported: {list(PENDLE_ADDRESSES.keys())}")
        if rpc_url is None and gateway_client is None:
            raise ValueError("PendleSDK requires either rpc_url (deprecated) or gateway_client")

        if gateway_client is not None:
            from almanak.framework.web3.gateway_provider import GatewayWeb3Provider

            self.web3 = Web3(GatewayWeb3Provider(gateway_client, chain=chain))
        else:
            # Direct-RPC web3 for the local-dev / no-gateway compile path only. The
            # provider is consumed for ABI encoding + ``to_checksum_address`` and, via
            # the compiler's ``_resolve_pt_from_yt``, a single ``eth_call`` — all of
            # which route through ``GatewayWeb3Provider`` when a gateway is wired.
            #
            # UNREACHABLE from a hosted strategy container: the runner always wires a
            # connected gateway_client, so ``intents/compiler._get_chain_rpc_url`` returns
            # None and ``pendle/compiler._resolve_pendle_adapter_inputs`` forces
            # ``rpc_url=None`` → the gateway branch above is taken. Proven by
            # ``tests/reports/pendle_egress_trace_vib5305.md`` and the
            # ``TestHostedStrategyContainerNoHttpProvider`` regression guards (which
            # also pin the compiler decision that yields ``rpc_url=None``).
            #
            # NOT removable in M0: deleting it regresses the explicitly-supported
            # local-dev-without-gateway path (see ``_get_chain_rpc_url`` docstring).
            # Full removal is tracked by VIB-5348 (debt origin: VIB-2986).
            self.web3 = Web3(
                Web3.HTTPProvider(rpc_url, request_kwargs={"timeout": _PENDLE_SDK_RPC_TIMEOUT_SECONDS})
            )  # vib-2986-exempt: local-dev fallback, removal tracked by VIB-5348
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
        token_redeem_sy: str | None = None,
    ) -> PendleTransactionData:
        """
        Build a swap transaction from PT to token using swapExactPtForToken.

        Uses the full Pendle V4 function with empty LimitOrderData (the
        Simple variant does not exist on the deployed router).

        Args:
            receiver: Address to receive the token
            market: Market address
            pt_amount: Amount of PT to swap
            token_out: Output token address
            min_token_out: Minimum output token to receive
            slippage_bps: Slippage tolerance in basis points
            token_redeem_sy: Token that redeems SY (defaults to token_out if not specified).
                            For yield-bearing token markets, this should be the
                            yield-bearing token address, not the underlying.

        Returns:
            Transaction data for execution
        """
        min_token_out_with_slippage = int(min_token_out * (10000 - slippage_bps) // 10000)

        # Use token_redeem_sy if provided, otherwise default to token_out
        redeem_sy_address = token_redeem_sy if token_redeem_sy else token_out

        # Build TokenOutput struct
        # TokenOutput: (tokenOut, minTokenOut, tokenRedeemSy, pendleSwap, swapData)
        # SwapData: (swapType, extRouter, extCalldata, needScale)
        token_output = (
            self.web3.to_checksum_address(token_out),  # tokenOut
            min_token_out_with_slippage,  # minTokenOut
            self.web3.to_checksum_address(redeem_sy_address),  # tokenRedeemSy
            "0x0000000000000000000000000000000000000000",  # pendleSwap
            (
                0,
                "0x0000000000000000000000000000000000000000",
                b"",
                False,
            ),  # swapData: (swapType=NONE, extRouter, extCalldata, needScale)
        )

        # LimitOrderData (empty - no limit orders)
        limit_order_data: tuple[Any, ...] = (
            "0x0000000000000000000000000000000000000000",
            0,
            [],
            [],
            b"",
        )

        calldata = self._encode_swap_exact_pt_for_token(
            receiver=receiver,
            market=market,
            pt_amount=pt_amount,
            token_output=token_output,
            limit_order_data=limit_order_data,
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

    def _encode_swap_exact_pt_for_token(
        self,
        receiver: str,
        market: str,
        pt_amount: int,
        token_output: tuple,
        limit_order_data: tuple,
    ) -> str:
        """Encode swapExactPtForToken calldata using ABI encoding.

        Uses the full V4 function with LimitOrderData (pass empty tuple
        for no limit orders). The Simple variant does not exist on the
        deployed Pendle Router.
        """
        router = self.get_router()

        receiver = self.web3.to_checksum_address(receiver)
        market = self.web3.to_checksum_address(market)

        calldata = router.encode_abi(
            "swapExactPtForToken",
            args=[
                receiver,
                market,
                pt_amount,
                token_output,
                limit_order_data,
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


def get_pendle_sdk(
    rpc_url: str | None = None,
    chain: str = "arbitrum",
    gateway_client: "GatewayClient | None" = None,
) -> PendleSDK:
    """Factory function to create a PendleSDK instance."""
    return PendleSDK(rpc_url=rpc_url, chain=chain, gateway_client=gateway_client)


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
