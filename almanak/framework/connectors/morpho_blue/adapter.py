"""Morpho Blue Adapter.

This module provides an adapter for interacting with Morpho Blue lending protocol,
supporting supply, borrow, repay, and withdraw operations.

Morpho Blue is a permissionless lending protocol that allows:
- Creating isolated lending markets
- Supplying assets to earn yield
- Borrowing against collateral
- Flexible market parameters (LTV, oracles, interest rate models)

Supported chains:
- Ethereum
- Base

Example:
    from almanak.framework.connectors.morpho_blue import MorphoBlueAdapter, MorphoBlueConfig

    config = MorphoBlueConfig(
        chain="ethereum",
        wallet_address="0x...",
    )
    adapter = MorphoBlueAdapter(config)

    # Supply collateral
    result = adapter.supply_collateral(
        market_id="0x...",
        amount=Decimal("1000"),
    )

    # Borrow against collateral
    result = adapter.borrow(
        market_id="0x...",
        amount=Decimal("500"),
    )
"""

import logging
from collections.abc import Callable
from dataclasses import dataclass
from decimal import Decimal
from enum import IntEnum
from typing import TYPE_CHECKING, Any

from almanak.framework.data.tokens.exceptions import TokenResolutionError

if TYPE_CHECKING:
    from almanak.framework.data.tokens.resolver import TokenResolver as TokenResolverType

from almanak.core.contracts import MORPHO_BLUE as _MORPHO_BLUE_REGISTRY

logger = logging.getLogger(__name__)


# =============================================================================
# Constants
# =============================================================================

# Morpho Blue contract addresses per chain (derived from centralized registry)
MORPHO_BLUE_ADDRESSES: dict[str, str] = {chain: addrs["morpho"] for chain, addrs in _MORPHO_BLUE_REGISTRY.items()}

# Bundler addresses per chain (for batched operations, derived from centralized registry)
MORPHO_BUNDLER_ADDRESSES: dict[str, str] = {chain: addrs["bundler"] for chain, addrs in _MORPHO_BLUE_REGISTRY.items()}

# Morpho Blue function selectors
# Note: MarketParams struct is encoded as tuple (address,address,address,address,uint256)
MORPHO_SUPPLY_SELECTOR = "0xa99aad89"  # supply((address,address,address,address,uint256),uint256,uint256,address,bytes)
MORPHO_WITHDRAW_SELECTOR = "0x5c2bea49"  # withdraw(MarketParams,uint256,uint256,address,address)
MORPHO_BORROW_SELECTOR = "0x50d8cd4b"  # borrow(MarketParams,uint256,uint256,address,address)
MORPHO_REPAY_SELECTOR = "0x20b76e81"  # repay(MarketParams,uint256,uint256,address,bytes)
MORPHO_SUPPLY_COLLATERAL_SELECTOR = "0x238d6579"  # supplyCollateral(MarketParams,uint256,address,bytes)
MORPHO_WITHDRAW_COLLATERAL_SELECTOR = "0x8720316d"  # withdrawCollateral(MarketParams,uint256,address,address)
MORPHO_LIQUIDATE_SELECTOR = "0xd8eabcb8"  # liquidate(MarketParams,address,uint256,uint256,bytes)
MORPHO_FLASH_LOAN_SELECTOR = "0xe0232b42"  # flashLoan(address,uint256,bytes)
MORPHO_SET_AUTHORIZATION_SELECTOR = "0xeecea000"  # setAuthorization(address,bool)
MORPHO_ACCRUE_INTEREST_SELECTOR = "0x151c1ade"  # accrueInterest(MarketParams)

# ERC20 approve selector
ERC20_APPROVE_SELECTOR = "0x095ea7b3"

# Max values
MAX_UINT256 = 2**256 - 1

# Gas estimates for Morpho Blue operations
DEFAULT_GAS_ESTIMATES: dict[str, int] = {
    "supply": 180000,
    "withdraw": 180000,
    "borrow": 250000,
    "repay": 180000,
    "supply_collateral": 180000,
    "withdraw_collateral": 180000,
    "liquidate": 350000,
    "flash_loan": 400000,
    "set_authorization": 50000,
    "accrue_interest": 100000,
    "approve": 46000,
}

# Pre-configured Morpho Blue markets (market_id -> market info)
# Market ID is keccak256(abi.encode(loanToken, collateralToken, oracle, irm, lltv))
MORPHO_MARKETS: dict[str, dict[str, dict[str, Any]]] = {
    "ethereum": {
        # wstETH/USDC market (86% LLTV)
        "0xb323495f7e4148be5643a4ea4a8221eef163e4bccfdedc2a6f4696baacbc86cc": {
            "name": "wstETH/USDC",
            "loan_token": "USDC",
            "loan_token_address": "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
            "collateral_token": "wstETH",
            "collateral_token_address": "0x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0",
            "oracle": "0x48F7E36EB6B826B2dF4B2E630B62Cd25e89E40e2",
            "irm": "0x870aC11D48B15DB9a138Cf899d20F13F79Ba00BC",
            "lltv": 860000000000000000,  # 86%
        },
        # wstETH/WETH market (94.5% LLTV)
        "0xc54d7acf14de29e0e5527cabd7a576506870346a78a11a6762e2cca66322ec41": {
            "name": "wstETH/WETH",
            "loan_token": "WETH",
            "loan_token_address": "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2",
            "collateral_token": "wstETH",
            "collateral_token_address": "0x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0",
            "oracle": "0x2a01EB9496094dA03c4E364Def50f5aD1280AD72",
            "irm": "0x870aC11D48B15DB9a138Cf899d20F13F79Ba00BC",
            "lltv": 945000000000000000,  # 94.5%
        },
        # WBTC/USDC market (86% LLTV)
        "0x3a85e619751152991742810df6ec69ce473daef99e28a64ab2340d7b7ccfee49": {
            "name": "WBTC/USDC",
            "loan_token": "USDC",
            "loan_token_address": "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
            "collateral_token": "WBTC",
            "collateral_token_address": "0x2260FAC5E5542a773Aa44fBCfeDf7C193bc2C599",
            "oracle": "0xDddd770BADd886dF3864029e4B377B5F6a2B6b83",
            "irm": "0x870aC11D48B15DB9a138Cf899d20F13F79Ba00BC",
            "lltv": 860000000000000000,  # 86%
        },
        # sUSDe/DAI market (86% LLTV) - Ethena synthetic dollar
        "0x39d11026eae1c6ec02aa4c0910778664089cdd97c3fd23f68f7cd05e2e95af48": {
            "name": "sUSDe/DAI",
            "loan_token": "DAI",
            "loan_token_address": "0x6B175474E89094C44Da98b954EedeAC495271d0F",
            "collateral_token": "sUSDe",
            "collateral_token_address": "0x9D39A5DE30e57443BfF2A8307A4256c8797A3497",
            "oracle": "0x5D916980D5Ae1737a8330Bf24dF812b2911Aae25",
            "irm": "0x870aC11D48B15DB9a138Cf899d20F13F79Ba00BC",
            "lltv": 860000000000000000,  # 86%
        },
        # sUSDe/USDC market (91.5% LLTV) - Ethena synthetic dollar
        "0x85c7f4374f3a403b36d54cc284983b2b02bbd8581ee0f3c36494447b87d9fcab": {
            "name": "sUSDe/USDC",
            "loan_token": "USDC",
            "loan_token_address": "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
            "collateral_token": "sUSDe",
            "collateral_token_address": "0x9D39A5DE30e57443BfF2A8307A4256c8797A3497",
            "oracle": "0x873CD44b860DEDFe139f93e12A4AcCa0926Ffb87",
            "irm": "0x870aC11D48B15DB9a138Cf899d20F13F79Ba00BC",
            "lltv": 915000000000000000,  # 91.5%
        },
        # weETH/WETH market (90% LLTV) - ether.fi wrapped ETH
        "0x698fe98247a40c5771537b5786b2f3f9d78eb487b4ce4d75533cd0e94d88a115": {
            "name": "weETH/WETH",
            "loan_token": "WETH",
            "loan_token_address": "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2",
            "collateral_token": "weETH",
            "collateral_token_address": "0xCd5fE23C85820F7B72D0926FC9b05b43E359b7ee",
            "oracle": "0x3fa58b74e9a8eA8768eb33c8453e9C2Ed089A40a",
            "irm": "0x870aC11D48B15DB9a138Cf899d20F13F79Ba00BC",
            "lltv": 900000000000000000,  # 90%
        },
        # ezETH/WETH market (86% LLTV) - Renzo restaked ETH
        "0x49bb2d114be9041a787432952927f6f144f05ad3e83196a7d062f374ee11d0ee": {
            "name": "ezETH/WETH",
            "loan_token": "WETH",
            "loan_token_address": "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2",
            "collateral_token": "ezETH",
            "collateral_token_address": "0xbf5495Efe5DB9ce00f80364C8B423567e58d2110",
            "oracle": "0x61025e2B0122ac8bE4e37365A4003d87ad888Cc3",
            "irm": "0x870aC11D48B15DB9a138Cf899d20F13F79Ba00BC",
            "lltv": 860000000000000000,  # 86%
        },
        # =====================================================================
        # Pendle PT Collateral Markets
        # =====================================================================
        # PT-sUSDe-5FEB2026/USDC market (91.5% LLTV) - Pendle PT as collateral (expired, verified on-chain)
        "0xd174bb7b8dd6ef16b116753b56679932ee13382b94f81bf66a2b37962cb41f56": {
            "name": "PT-sUSDe-5FEB2026/USDC",
            "loan_token": "USDC",
            "loan_token_address": "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
            "collateral_token": "PT-sUSDe-5FEB2026",
            "collateral_token_address": "0xE8483517077afa11A9B07f849cee2552f040d7b2",
            "oracle": "0xFAfb71F2fe9a4330c34a192812F36D8d6f07f095",
            "irm": "0x870aC11D48B15DB9a138Cf899d20F13F79Ba00BC",
            "lltv": 915000000000000000,  # 91.5%
            "is_pt_market": True,
        },
        # PT-sUSDe-27MAR2025/USDC market (91.5% LLTV) - expired but verified on-chain
        "0x346afa2b6d528222a2f9721ded6e7e2c40ac94877a598f5dae5013c651d2a462": {
            "name": "PT-sUSDe-27MAR2025/USDC",
            "loan_token": "USDC",
            "loan_token_address": "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
            "collateral_token": "PT-sUSDe-27MAR2025",
            "collateral_token_address": "0xE00bd3Df25fb187d6ABBB620b3dfd19839947b81",
            "oracle": "0x9c0174fE7748F318dcB7300b93B170b6026280B0",
            "irm": "0x870aC11D48B15DB9a138Cf899d20F13F79Ba00BC",
            "lltv": 915000000000000000,  # 91.5%
            "is_pt_market": True,
        },
        # PT-sUSDe-31JUL2025/USDC market (91.5% LLTV) - expired but verified on-chain
        "0xbc552f0b14dd6f8e60b760a534ac1d8613d3539153b4d9675d697e048f2edc7e": {
            "name": "PT-sUSDe-31JUL2025/USDC",
            "loan_token": "USDC",
            "loan_token_address": "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
            "collateral_token": "PT-sUSDe-31JUL2025",
            "collateral_token_address": "0x3b3fB9C57858EF816833dC91565EFcd85D96f634",
            "oracle": "0x1376913337ceC523B4DDEAD8a60eDb1fA43fF1E3",
            "irm": "0x870aC11D48B15DB9a138Cf899d20F13F79Ba00BC",
            "lltv": 915000000000000000,  # 91.5%
            "is_pt_market": True,
        },
        # PT-eUSDe/USDe market (86% LLTV)
        "0xe7a06721ca6dce24fce8c5a57d7bb39688dc0f5700e86be29d1f488acab63876": {
            "name": "PT-eUSDe/USDe",
            "loan_token": "USDe",
            "loan_token_address": "0x4c9EDD5852cd905f086C759E8383e09bff1E68B3",
            "collateral_token": "PT-eUSDe",
            "collateral_token_address": "0x308C36BaF407f543DaC3a6340B7B6B31079e8e0d",
            "oracle": "0x5D916980D5Ae1737a8330Bf24dF812b2911Aae25",
            "irm": "0x870aC11D48B15DB9a138Cf899d20F13F79Ba00BC",
            "lltv": 860000000000000000,  # 86%
            "is_pt_market": True,
        },
    },
    "arbitrum": {
        # PT-USDai/USDC market (86% LLTV) - Pendle PT on Arbitrum
        "0xf4abce39de1e88e6f98e2e5e0960f609caf67db710b3e2a36e8e06a1038ec949": {
            "name": "PT-USDai/USDC",
            "loan_token": "USDC",
            "loan_token_address": "0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
            "collateral_token": "PT-USDai",
            "collateral_token_address": "0x3b0C5ef8D4c8Ae6db1A3E3B9c876a53f3fe8C0b1",
            "oracle": "0x2a01EB9496094dA03c4E364Def50f5aD1280AD72",
            "irm": "0x870aC11D48B15DB9a138Cf899d20F13F79Ba00BC",
            "lltv": 860000000000000000,  # 86%
            "is_pt_market": True,
        },
    },
    "base": {
        # cbETH/USDC market (86% LLTV)
        "0xdba352d93a64b17c71104cbddc6aef85cd432322a1446b5b65163cbbc615cd0c": {
            "name": "cbETH/USDC",
            "loan_token": "USDC",
            "loan_token_address": "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913",
            "collateral_token": "cbETH",
            "collateral_token_address": "0x2Ae3F1Ec7F1F5012CFEab0185bfc7aa3cf0DEc22",
            "oracle": "0xF4030086522a5bEEa4988F8cA5B36dbC97BeE88c",
            "irm": "0x46415998764C29aB2a25CbeA6254146D50D22687",
            "lltv": 860000000000000000,  # 86%
        },
        # wstETH/USDC market (86% LLTV) - https://app.morpho.org/base/market/0x13c42741a359ac4a8aa8287d2be109dcf28344484f91185f9a79bd5a805a55ae/wsteth-usdc
        "0x13c42741a359ac4a8aa8287d2be109dcf28344484f91185f9a79bd5a805a55ae": {
            "name": "wstETH/USDC",
            "loan_token": "USDC",
            "loan_token_address": "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913",
            "collateral_token": "wstETH",
            "collateral_token_address": "0xc1CBa3fCea344f92D9239c08C0568f6F2F0ee452",
            "oracle": "0xD7A1abA119a236Fea5BBC5cAC6836465cbe9289A",
            "irm": "0x46415998764C29aB2a25CbeA6254146D50D22687",
            "lltv": 860000000000000000,  # 86%
        },
    },
}


# =============================================================================
# Enums
# =============================================================================


class MorphoBlueInterestRateMode(IntEnum):
    """Morpho Blue uses a single interest rate model per market.

    Unlike Aave, Morpho Blue markets have a single adaptive interest rate
    determined by the IRM (Interest Rate Model) contract.
    """

    VARIABLE = 0  # All rates are variable in Morpho Blue


# =============================================================================
# Data Classes
# =============================================================================


@dataclass
class MorphoBlueConfig:
    """Configuration for Morpho Blue adapter.

    Attributes:
        chain: Blockchain network (ethereum, base)
        wallet_address: User wallet address
        default_slippage_bps: Default slippage tolerance in basis points
        rpc_url: Optional RPC URL. If not provided, uses ALCHEMY_API_KEY.
        price_provider: Optional dict mapping token symbols to USD prices.
            Required for health factor calculations in production.
        allow_placeholder_prices: If True, allows using placeholder prices
            for testing. DO NOT use in production.
        enable_sdk: If True, initializes the SDK for on-chain reads.
            Requires RPC access.
    """

    chain: str
    wallet_address: str
    default_slippage_bps: int = 50  # 0.5%
    rpc_url: str | None = None
    price_provider: dict[str, Decimal] | None = None
    allow_placeholder_prices: bool = False
    enable_sdk: bool = True  # Enable SDK by default for production use

    def __post_init__(self) -> None:
        """Validate configuration."""
        valid_chains = set(MORPHO_BLUE_ADDRESSES.keys())
        if self.chain not in valid_chains:
            raise ValueError(f"Invalid chain: {self.chain}. Valid chains: {valid_chains}")
        if not self.wallet_address.startswith("0x") or len(self.wallet_address) != 42:
            raise ValueError(f"Invalid wallet address: {self.wallet_address}. Must be 0x-prefixed 40 hex chars.")
        if self.default_slippage_bps < 0 or self.default_slippage_bps > 10000:
            raise ValueError(f"Invalid slippage: {self.default_slippage_bps}. Must be 0-10000 bps.")


@dataclass
class MorphoBlueMarketParams:
    """Market parameters for Morpho Blue.

    In Morpho Blue, each market is uniquely identified by these 5 parameters.
    The market_id is derived as: keccak256(abi.encode(loan_token, collateral_token, oracle, irm, lltv))

    Attributes:
        loan_token: Address of the asset being borrowed
        collateral_token: Address of the collateral asset
        oracle: Address of the price oracle
        irm: Address of the interest rate model
        lltv: Liquidation LTV (in 1e18 scale, e.g., 860000000000000000 = 86%)
    """

    loan_token: str
    collateral_token: str
    oracle: str
    irm: str
    lltv: int

    def to_tuple(self) -> tuple[str, str, str, str, int]:
        """Convert to tuple for ABI encoding."""
        return (self.loan_token, self.collateral_token, self.oracle, self.irm, self.lltv)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "loan_token": self.loan_token,
            "collateral_token": self.collateral_token,
            "oracle": self.oracle,
            "irm": self.irm,
            "lltv": self.lltv,
            "lltv_percent": self.lltv / 1e16,  # Convert to percentage
        }


@dataclass
class MorphoBlueMarketState:
    """State of a Morpho Blue market.

    Attributes:
        market_id: Unique identifier for the market
        total_supply_assets: Total assets supplied to the market
        total_supply_shares: Total supply shares
        total_borrow_assets: Total assets borrowed from the market
        total_borrow_shares: Total borrow shares
        last_update: Timestamp of last interest accrual
        fee: Protocol fee (in 1e18 scale)
    """

    market_id: str
    total_supply_assets: Decimal = Decimal("0")
    total_supply_shares: Decimal = Decimal("0")
    total_borrow_assets: Decimal = Decimal("0")
    total_borrow_shares: Decimal = Decimal("0")
    last_update: int = 0
    fee: Decimal = Decimal("0")

    @property
    def utilization(self) -> Decimal:
        """Calculate market utilization rate."""
        if self.total_supply_assets == 0:
            return Decimal("0")
        return self.total_borrow_assets / self.total_supply_assets

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "market_id": self.market_id,
            "total_supply_assets": str(self.total_supply_assets),
            "total_supply_shares": str(self.total_supply_shares),
            "total_borrow_assets": str(self.total_borrow_assets),
            "total_borrow_shares": str(self.total_borrow_shares),
            "last_update": self.last_update,
            "fee": str(self.fee),
            "utilization": str(self.utilization),
        }


@dataclass
class MorphoBluePosition:
    """User position in a Morpho Blue market.

    Attributes:
        market_id: Market identifier
        supply_shares: User's supply shares
        borrow_shares: User's borrow shares
        collateral: User's collateral amount
    """

    market_id: str
    supply_shares: Decimal = Decimal("0")
    borrow_shares: Decimal = Decimal("0")
    collateral: Decimal = Decimal("0")

    @property
    def has_supply(self) -> bool:
        """Check if user has supply in this market."""
        return self.supply_shares > 0

    @property
    def has_borrow(self) -> bool:
        """Check if user has borrow in this market."""
        return self.borrow_shares > 0

    @property
    def has_collateral(self) -> bool:
        """Check if user has collateral in this market."""
        return self.collateral > 0

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "market_id": self.market_id,
            "supply_shares": str(self.supply_shares),
            "borrow_shares": str(self.borrow_shares),
            "collateral": str(self.collateral),
            "has_supply": self.has_supply,
            "has_borrow": self.has_borrow,
            "has_collateral": self.has_collateral,
        }


@dataclass
class MorphoBlueHealthFactor:
    """Health factor calculation for a Morpho Blue position.

    Attributes:
        collateral_value_usd: Value of collateral in USD
        debt_value_usd: Value of debt in USD
        lltv: Liquidation LTV of the market
        health_factor: Calculated health factor
        max_borrow_usd: Maximum borrowable amount
    """

    collateral_value_usd: Decimal
    debt_value_usd: Decimal
    lltv: Decimal
    health_factor: Decimal
    max_borrow_usd: Decimal = Decimal("0")

    @property
    def is_healthy(self) -> bool:
        """Check if position is healthy (HF >= 1)."""
        return self.health_factor >= Decimal("1.0")

    @property
    def liquidation_threshold_usd(self) -> Decimal:
        """Get the USD debt level at which liquidation would occur."""
        return self.collateral_value_usd * self.lltv

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "collateral_value_usd": str(self.collateral_value_usd),
            "debt_value_usd": str(self.debt_value_usd),
            "lltv": str(self.lltv),
            "health_factor": str(self.health_factor),
            "is_healthy": self.is_healthy,
            "max_borrow_usd": str(self.max_borrow_usd),
            "liquidation_threshold_usd": str(self.liquidation_threshold_usd),
        }


@dataclass
class TransactionResult:
    """Result of a transaction build operation.

    Attributes:
        success: Whether operation succeeded
        tx_data: Transaction data (to, value, data)
        gas_estimate: Estimated gas
        description: Human-readable description
        error: Error message if failed
    """

    success: bool
    tx_data: dict[str, Any] | None = None
    gas_estimate: int = 0
    description: str = ""
    error: str | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "success": self.success,
            "tx_data": self.tx_data,
            "gas_estimate": self.gas_estimate,
            "description": self.description,
            "error": self.error,
        }


# =============================================================================
# Type Aliases
# =============================================================================

PriceOracle = Callable[[str], Decimal]


# =============================================================================
# Adapter
# =============================================================================


class MorphoBlueAdapter:
    """Adapter for Morpho Blue lending protocol.

    This adapter provides methods for interacting with Morpho Blue:
    - Supply/withdraw assets (lending)
    - Supply/withdraw collateral
    - Borrow/repay assets
    - Health factor calculations
    - On-chain position and market state reading (via SDK)

    Example:
        # Production usage with real prices
        config = MorphoBlueConfig(
            chain="ethereum",
            wallet_address="0x...",
            price_provider={"USDC": Decimal("1"), "wstETH": Decimal("3500")},
        )
        adapter = MorphoBlueAdapter(config)

        # Read on-chain position
        position = adapter.get_position_on_chain(market_id)
        print(f"Collateral: {position.collateral}")

        # Supply collateral
        result = adapter.supply_collateral(
            market_id="0x...",
            amount=Decimal("1.0"),
        )

        # For testing only (with placeholder prices)
        config = MorphoBlueConfig(
            chain="ethereum",
            wallet_address="0x...",
            allow_placeholder_prices=True,
            enable_sdk=False,  # Disable SDK for unit tests
        )
        adapter = MorphoBlueAdapter(config)
    """

    def __init__(
        self,
        config: MorphoBlueConfig,
        price_oracle: PriceOracle | None = None,
        token_resolver: "TokenResolverType | None" = None,
    ) -> None:
        """Initialize the adapter.

        Args:
            config: Adapter configuration
            price_oracle: Optional price oracle callback. If not provided,
                uses config.price_provider dict or placeholder prices.
            token_resolver: Optional TokenResolver instance. If None, uses singleton.
        """
        self.config = config
        self.chain = config.chain
        self.wallet_address = config.wallet_address

        # Contract addresses
        self.morpho_address = MORPHO_BLUE_ADDRESSES[config.chain]
        self.bundler_address = MORPHO_BUNDLER_ADDRESSES.get(config.chain)

        # Markets for this chain
        self.markets = MORPHO_MARKETS.get(config.chain, {})

        # TokenResolver integration
        if token_resolver is not None:
            self._token_resolver = token_resolver
        else:
            from almanak.framework.data.tokens.resolver import get_token_resolver

            self._token_resolver = get_token_resolver()

        # SDK for on-chain reads (optional)
        self._sdk: Any = None  # Lazy initialization
        self._sdk_enabled = config.enable_sdk

        # Price oracle setup
        self._using_placeholder_prices = False
        if price_oracle is not None:
            self._price_oracle = price_oracle
        elif config.price_provider is not None:
            # Create oracle from price_provider dict
            self._price_oracle = self._create_price_oracle_from_dict(config.price_provider)
        elif config.allow_placeholder_prices:
            logger.warning(
                "MorphoBlueAdapter using PLACEHOLDER PRICES for chain=%s. "
                "Health factor calculations WILL BE INACCURATE.",
                config.chain,
            )
            self._price_oracle = self._default_price_oracle
            self._using_placeholder_prices = True
        else:
            # Default to placeholder with warning for backwards compatibility
            logger.warning(
                "MorphoBlueAdapter: No price_oracle or price_provider provided. "
                "Using placeholder prices. For production, use create_adapter_with_prices()."
            )
            self._price_oracle = self._default_price_oracle
            self._using_placeholder_prices = True

        logger.info(
            f"MorphoBlueAdapter initialized for chain={config.chain}, "
            f"wallet={config.wallet_address[:10]}..., "
            f"sdk={'enabled' if self._sdk_enabled else 'disabled'}"
        )

    # =========================================================================
    # SDK Property (Lazy Initialization)
    # =========================================================================

    @property
    def sdk(self) -> Any:
        """Get the SDK instance (lazy initialization).

        Returns:
            MorphoBlueSDK instance

        Raises:
            RuntimeError: If SDK is disabled
        """
        if not self._sdk_enabled:
            raise RuntimeError("SDK is disabled. Set enable_sdk=True in config to use on-chain reads.")

        if self._sdk is None:
            # Import here to avoid circular imports
            from .sdk import MorphoBlueSDK

            self._sdk = MorphoBlueSDK(
                chain=self.chain,
                rpc_url=self.config.rpc_url,
            )

        return self._sdk

    # =========================================================================
    # On-Chain Reading Methods
    # =========================================================================

    def get_position_on_chain(
        self,
        market_id: str,
        user: str | None = None,
    ) -> MorphoBluePosition:
        """Get user position from on-chain data.

        Requires SDK to be enabled.

        Args:
            market_id: Market identifier
            user: User address (defaults to wallet_address)

        Returns:
            MorphoBluePosition with supply, borrow, and collateral data
        """
        user_address = user or self.wallet_address
        sdk_position = self.sdk.get_position(market_id, user_address)

        return MorphoBluePosition(
            market_id=sdk_position.market_id,
            supply_shares=Decimal(sdk_position.supply_shares),
            borrow_shares=Decimal(sdk_position.borrow_shares),
            collateral=Decimal(sdk_position.collateral),
        )

    def get_market_state_on_chain(self, market_id: str) -> MorphoBlueMarketState:
        """Get market state from on-chain data.

        Requires SDK to be enabled.

        Args:
            market_id: Market identifier

        Returns:
            MorphoBlueMarketState with current market totals
        """
        sdk_state = self.sdk.get_market_state(market_id)

        return MorphoBlueMarketState(
            market_id=sdk_state.market_id,
            total_supply_assets=Decimal(sdk_state.total_supply_assets),
            total_supply_shares=Decimal(sdk_state.total_supply_shares),
            total_borrow_assets=Decimal(sdk_state.total_borrow_assets),
            total_borrow_shares=Decimal(sdk_state.total_borrow_shares),
            last_update=sdk_state.last_update,
            fee=Decimal(sdk_state.fee),
        )

    def get_market_params_on_chain(self, market_id: str) -> MorphoBlueMarketParams:
        """Get market parameters from on-chain data.

        Requires SDK to be enabled.

        Args:
            market_id: Market identifier

        Returns:
            MorphoBlueMarketParams with loan token, collateral token, oracle, IRM, LLTV
        """
        sdk_params = self.sdk.get_market_params(market_id)

        return MorphoBlueMarketParams(
            loan_token=sdk_params.loan_token,
            collateral_token=sdk_params.collateral_token,
            oracle=sdk_params.oracle,
            irm=sdk_params.irm,
            lltv=sdk_params.lltv,
        )

    def discover_markets_on_chain(self) -> list[str]:
        """Discover all markets from on-chain events.

        Requires SDK to be enabled.

        Returns:
            List of market IDs (bytes32 hex strings)
        """
        return self.sdk.discover_markets()

    def get_supply_assets_on_chain(
        self,
        market_id: str,
        user: str | None = None,
    ) -> Decimal:
        """Get user's supply amount in assets (not shares) from on-chain.

        Args:
            market_id: Market identifier
            user: User address (defaults to wallet_address)

        Returns:
            Supply amount in asset units
        """
        user_address = user or self.wallet_address
        assets = self.sdk.get_supply_assets(market_id, user_address)
        return Decimal(assets)

    def get_borrow_assets_on_chain(
        self,
        market_id: str,
        user: str | None = None,
    ) -> Decimal:
        """Get user's borrow amount in assets (not shares) from on-chain.

        Args:
            market_id: Market identifier
            user: User address (defaults to wallet_address)

        Returns:
            Borrow amount in asset units
        """
        user_address = user or self.wallet_address
        assets = self.sdk.get_borrow_assets(market_id, user_address)
        return Decimal(assets)

    # =========================================================================
    # Price Oracle Helpers
    # =========================================================================

    def _create_price_oracle_from_dict(
        self,
        prices: dict[str, Decimal],
    ) -> PriceOracle:
        """Create price oracle callback from a dictionary.

        Args:
            prices: Dict mapping token symbols to USD prices

        Returns:
            PriceOracle callback function
        """

        def price_oracle(asset: str) -> Decimal:
            # Try exact match first
            if asset in prices:
                return prices[asset]
            # Try uppercase
            if asset.upper() in prices:
                return prices[asset.upper()]
            # Try lowercase
            if asset.lower() in prices:
                return prices[asset.lower()]
            # Log warning and return 0
            logger.warning(f"No price found for asset: {asset}")
            return Decimal("0")

        return price_oracle

    # =========================================================================
    # Supply Operations (Lending)
    # =========================================================================

    def supply(
        self,
        market_id: str,
        amount: Decimal,
        on_behalf_of: str | None = None,
        shares_mode: bool = False,
    ) -> TransactionResult:
        """Build a supply transaction for lending assets.

        Supplies loan tokens to the market to earn interest.

        Args:
            market_id: Market identifier
            amount: Amount to supply (in token units) or shares if shares_mode=True
            on_behalf_of: Address to credit (defaults to wallet_address)
            shares_mode: If True, amount represents shares instead of assets

        Returns:
            TransactionResult with transaction data
        """
        try:
            market_info = self._get_market_info(market_id)
            if market_info is None:
                return TransactionResult(
                    success=False,
                    error=f"Unknown market: {market_id}",
                )

            market_params = self._get_market_params(market_info)
            loan_token = market_info["loan_token"]
            decimals = self._get_decimals(loan_token)
            recipient = on_behalf_of or self.wallet_address

            if shares_mode:
                # amount is shares (always 18 decimals in Morpho Blue)
                assets_wei = 0
                shares_wei = int(amount * Decimal(10**18))
            else:
                # amount is assets
                assets_wei = int(amount * Decimal(10**decimals))
                shares_wei = 0

            # Build calldata: supply(MarketParams,uint256,uint256,address,bytes)
            # MarketParams is a tuple: (loanToken, collateralToken, oracle, irm, lltv)
            calldata = self._build_supply_calldata(market_params, assets_wei, shares_wei, recipient, b"")

            amount_str = f"{amount} shares" if shares_mode else f"{amount} {loan_token}"
            return TransactionResult(
                success=True,
                tx_data={
                    "to": self.morpho_address,
                    "value": 0,
                    "data": calldata,
                },
                gas_estimate=DEFAULT_GAS_ESTIMATES["supply"],
                description=f"Supply {amount_str} to Morpho Blue market {market_info['name']}",
            )

        except Exception as e:
            logger.exception(f"Failed to build supply transaction: {e}")
            return TransactionResult(success=False, error=str(e))

    def withdraw(
        self,
        market_id: str,
        amount: Decimal,
        receiver: str | None = None,
        on_behalf_of: str | None = None,
        shares_mode: bool = False,
        withdraw_all: bool = False,
    ) -> TransactionResult:
        """Build a withdraw transaction for withdrawing supplied assets.

        Withdraws loan tokens from the market.

        Args:
            market_id: Market identifier
            amount: Amount to withdraw (in token units) or shares if shares_mode=True
            receiver: Address to receive tokens (defaults to wallet_address)
            on_behalf_of: Address to debit (defaults to wallet_address)
            shares_mode: If True, amount represents shares instead of assets
            withdraw_all: If True, withdraws all supplied assets

        Returns:
            TransactionResult with transaction data
        """
        try:
            market_info = self._get_market_info(market_id)
            if market_info is None:
                return TransactionResult(
                    success=False,
                    error=f"Unknown market: {market_id}",
                )

            market_params = self._get_market_params(market_info)
            loan_token = market_info["loan_token"]
            decimals = self._get_decimals(loan_token)
            recipient = receiver or self.wallet_address
            owner = on_behalf_of or self.wallet_address

            if withdraw_all:
                # Use max uint256 for shares to withdraw all
                assets_wei = 0
                shares_wei = MAX_UINT256
            elif shares_mode:
                assets_wei = 0
                shares_wei = int(amount * Decimal(10**18))
            else:
                assets_wei = int(amount * Decimal(10**decimals))
                shares_wei = 0

            # Build calldata: withdraw(MarketParams,uint256,uint256,address,address)
            calldata = self._build_withdraw_calldata(market_params, assets_wei, shares_wei, owner, recipient)

            if withdraw_all:
                amount_str = "all"
            elif shares_mode:
                amount_str = f"{amount} shares"
            else:
                amount_str = f"{amount} {loan_token}"

            return TransactionResult(
                success=True,
                tx_data={
                    "to": self.morpho_address,
                    "value": 0,
                    "data": calldata,
                },
                gas_estimate=DEFAULT_GAS_ESTIMATES["withdraw"],
                description=f"Withdraw {amount_str} from Morpho Blue market {market_info['name']}",
            )

        except Exception as e:
            logger.exception(f"Failed to build withdraw transaction: {e}")
            return TransactionResult(success=False, error=str(e))

    # =========================================================================
    # Collateral Operations
    # =========================================================================

    def supply_collateral(
        self,
        market_id: str,
        amount: Decimal,
        on_behalf_of: str | None = None,
    ) -> TransactionResult:
        """Build a supply collateral transaction.

        Supplies collateral tokens to the market for borrowing.

        Args:
            market_id: Market identifier
            amount: Amount of collateral to supply (in token units)
            on_behalf_of: Address to credit (defaults to wallet_address)

        Returns:
            TransactionResult with transaction data
        """
        try:
            market_info = self._get_market_info(market_id)
            if market_info is None:
                return TransactionResult(
                    success=False,
                    error=f"Unknown market: {market_id}",
                )

            market_params = self._get_market_params(market_info)
            collateral_token = market_info["collateral_token"]
            decimals = self._get_decimals(collateral_token)
            recipient = on_behalf_of or self.wallet_address

            amount_wei = int(amount * Decimal(10**decimals))

            # Build calldata: supplyCollateral(MarketParams,uint256,address,bytes)
            calldata = self._build_supply_collateral_calldata(market_params, amount_wei, recipient, b"")

            return TransactionResult(
                success=True,
                tx_data={
                    "to": self.morpho_address,
                    "value": 0,
                    "data": calldata,
                },
                gas_estimate=DEFAULT_GAS_ESTIMATES["supply_collateral"],
                description=f"Supply {amount} {collateral_token} as collateral to Morpho Blue market {market_info['name']}",
            )

        except Exception as e:
            logger.exception(f"Failed to build supply collateral transaction: {e}")
            return TransactionResult(success=False, error=str(e))

    def withdraw_collateral(
        self,
        market_id: str,
        amount: Decimal,
        receiver: str | None = None,
        on_behalf_of: str | None = None,
        withdraw_all: bool = False,
    ) -> TransactionResult:
        """Build a withdraw collateral transaction.

        Withdraws collateral tokens from the market.

        Args:
            market_id: Market identifier
            amount: Amount of collateral to withdraw (in token units)
            receiver: Address to receive tokens (defaults to wallet_address)
            on_behalf_of: Address to debit (defaults to wallet_address)
            withdraw_all: If True, withdraws all collateral

        Returns:
            TransactionResult with transaction data
        """
        try:
            market_info = self._get_market_info(market_id)
            if market_info is None:
                return TransactionResult(
                    success=False,
                    error=f"Unknown market: {market_id}",
                )

            market_params = self._get_market_params(market_info)
            collateral_token = market_info["collateral_token"]
            decimals = self._get_decimals(collateral_token)
            recipient = receiver or self.wallet_address
            owner = on_behalf_of or self.wallet_address

            amount_wei = MAX_UINT256 if withdraw_all else int(amount * Decimal(10**decimals))

            # Build calldata: withdrawCollateral(MarketParams,uint256,address,address)
            calldata = self._build_withdraw_collateral_calldata(market_params, amount_wei, owner, recipient)

            amount_str = "all" if withdraw_all else f"{amount} {collateral_token}"
            return TransactionResult(
                success=True,
                tx_data={
                    "to": self.morpho_address,
                    "value": 0,
                    "data": calldata,
                },
                gas_estimate=DEFAULT_GAS_ESTIMATES["withdraw_collateral"],
                description=f"Withdraw {amount_str} collateral from Morpho Blue market {market_info['name']}",
            )

        except Exception as e:
            logger.exception(f"Failed to build withdraw collateral transaction: {e}")
            return TransactionResult(success=False, error=str(e))

    # =========================================================================
    # Borrow Operations
    # =========================================================================

    def borrow(
        self,
        market_id: str,
        amount: Decimal,
        receiver: str | None = None,
        on_behalf_of: str | None = None,
        shares_mode: bool = False,
    ) -> TransactionResult:
        """Build a borrow transaction.

        Borrows loan tokens from the market against collateral.

        Args:
            market_id: Market identifier
            amount: Amount to borrow (in token units) or shares if shares_mode=True
            receiver: Address to receive tokens (defaults to wallet_address)
            on_behalf_of: Address to debit (defaults to wallet_address)
            shares_mode: If True, amount represents shares instead of assets

        Returns:
            TransactionResult with transaction data
        """
        try:
            market_info = self._get_market_info(market_id)
            if market_info is None:
                return TransactionResult(
                    success=False,
                    error=f"Unknown market: {market_id}",
                )

            market_params = self._get_market_params(market_info)
            loan_token = market_info["loan_token"]
            decimals = self._get_decimals(loan_token)
            recipient = receiver or self.wallet_address
            owner = on_behalf_of or self.wallet_address

            if shares_mode:
                assets_wei = 0
                shares_wei = int(amount * Decimal(10**18))
            else:
                assets_wei = int(amount * Decimal(10**decimals))
                shares_wei = 0

            # Build calldata: borrow(MarketParams,uint256,uint256,address,address)
            calldata = self._build_borrow_calldata(market_params, assets_wei, shares_wei, owner, recipient)

            amount_str = f"{amount} shares" if shares_mode else f"{amount} {loan_token}"
            return TransactionResult(
                success=True,
                tx_data={
                    "to": self.morpho_address,
                    "value": 0,
                    "data": calldata,
                },
                gas_estimate=DEFAULT_GAS_ESTIMATES["borrow"],
                description=f"Borrow {amount_str} from Morpho Blue market {market_info['name']}",
            )

        except Exception as e:
            logger.exception(f"Failed to build borrow transaction: {e}")
            return TransactionResult(success=False, error=str(e))

    def repay(
        self,
        market_id: str,
        amount: Decimal,
        on_behalf_of: str | None = None,
        shares_mode: bool = False,
        repay_all: bool = False,
    ) -> TransactionResult:
        """Build a repay transaction.

        Repays borrowed loan tokens to the market.

        Args:
            market_id: Market identifier
            amount: Amount to repay (in token units) or shares if shares_mode=True
            on_behalf_of: Address with debt (defaults to wallet_address)
            shares_mode: If True, amount represents shares instead of assets
            repay_all: If True, repays full debt

        Returns:
            TransactionResult with transaction data
        """
        try:
            market_info = self._get_market_info(market_id)
            if market_info is None:
                return TransactionResult(
                    success=False,
                    error=f"Unknown market: {market_id}",
                )

            market_params = self._get_market_params(market_info)
            loan_token = market_info["loan_token"]
            decimals = self._get_decimals(loan_token)
            owner = on_behalf_of or self.wallet_address

            if repay_all:
                # Query actual borrow shares from position (MAX_UINT256 causes overflow)
                position = self.get_position_on_chain(market_id, owner)
                if position.borrow_shares <= 0:
                    return TransactionResult(
                        success=False,
                        error="No borrow position to repay",
                    )
                assets_wei = 0
                shares_wei = int(position.borrow_shares)
            elif shares_mode:
                assets_wei = 0
                shares_wei = int(amount * Decimal(10**18))
            else:
                assets_wei = int(amount * Decimal(10**decimals))
                shares_wei = 0

                # Guard: Morpho Blue panics (0x11 underflow) if repay amount > actual debt.
                # Unlike Aave V3/Compound V3, Morpho does NOT cap repay at outstanding debt.
                # Query on-chain debt and cap the amount to prevent revert.
                if self._sdk_enabled:
                    try:
                        actual_debt_wei = self.sdk.get_borrow_assets(market_id, owner)
                        if assets_wei > actual_debt_wei:
                            logger.info(
                                "Morpho repay amount %d exceeds actual debt %d, capping to actual debt",
                                assets_wei,
                                actual_debt_wei,
                            )
                            assets_wei = actual_debt_wei
                    except Exception as e:
                        logger.warning(
                            "Could not query on-chain debt for repay cap, proceeding with requested amount: %s", e
                        )

            # Build calldata: repay(MarketParams,uint256,uint256,address,bytes)
            calldata = self._build_repay_calldata(market_params, assets_wei, shares_wei, owner, b"")

            if repay_all:
                amount_str = "full debt"
            elif shares_mode:
                amount_str = f"{amount} shares"
            else:
                amount_str = f"{amount} {loan_token}"

            return TransactionResult(
                success=True,
                tx_data={
                    "to": self.morpho_address,
                    "value": 0,
                    "data": calldata,
                },
                gas_estimate=DEFAULT_GAS_ESTIMATES["repay"],
                description=f"Repay {amount_str} to Morpho Blue market {market_info['name']}",
            )

        except Exception as e:
            logger.exception(f"Failed to build repay transaction: {e}")
            return TransactionResult(success=False, error=str(e))

    # =========================================================================
    # Flash Loan Operations
    # =========================================================================

    def flash_loan(
        self,
        token: str,
        amount: Decimal,
        callback_data: bytes = b"",
    ) -> TransactionResult:
        """Build a flash loan transaction.

        Borrows assets in a flash loan that must be repaid within the same transaction.

        Note: Flash loans require a callback contract to receive and repay the loan.
        The callback_data is passed to the flash loan receiver.

        Args:
            token: Token symbol or address to borrow
            amount: Amount to borrow
            callback_data: Data passed to flash loan receiver callback

        Returns:
            TransactionResult with transaction data
        """
        try:
            token_address = self._resolve_token(token)
            if token_address is None:
                return TransactionResult(
                    success=False,
                    error=f"Unknown token: {token}",
                )

            decimals = self._get_decimals(token)
            amount_wei = int(amount * Decimal(10**decimals))

            # Build calldata: flashLoan(address token, uint256 assets, bytes calldata data)
            calldata = self._build_flash_loan_calldata(token_address, amount_wei, callback_data)

            return TransactionResult(
                success=True,
                tx_data={
                    "to": self.morpho_address,
                    "value": 0,
                    "data": calldata,
                },
                gas_estimate=DEFAULT_GAS_ESTIMATES["flash_loan"],
                description=f"Flash loan {amount} {token} from Morpho Blue",
            )

        except Exception as e:
            logger.exception(f"Failed to build flash loan transaction: {e}")
            return TransactionResult(success=False, error=str(e))

    # =========================================================================
    # Liquidation Operations
    # =========================================================================

    def liquidate(
        self,
        market_id: str,
        borrower: str,
        seized_assets: Decimal,
        repaid_shares: Decimal | None = None,
        callback_data: bytes = b"",
    ) -> TransactionResult:
        """Build a liquidation transaction.

        Liquidates an unhealthy position by repaying debt and seizing collateral.

        In Morpho Blue, liquidators specify the amount of collateral to seize,
        and the protocol calculates how much debt to repay based on the oracle price
        and liquidation incentive.

        Args:
            market_id: Market identifier
            borrower: Address of the borrower to liquidate
            seized_assets: Amount of collateral to seize (in collateral token units)
            repaid_shares: Optional amount of debt shares to repay (if 0, uses seized_assets)
            callback_data: Data passed to liquidation callback (for flash liquidations)

        Returns:
            TransactionResult with transaction data
        """
        try:
            market_info = self._get_market_info(market_id)
            if market_info is None:
                return TransactionResult(
                    success=False,
                    error=f"Unknown market: {market_id}",
                )

            market_params = self._get_market_params(market_info)
            collateral_token = market_info["collateral_token"]
            decimals = self._get_decimals(collateral_token)

            seized_assets_wei = int(seized_assets * Decimal(10**decimals))
            repaid_shares_wei = int(repaid_shares * Decimal(10**18)) if repaid_shares is not None else 0

            # Build calldata: liquidate(MarketParams, address borrower, uint256 seizedAssets, uint256 repaidShares, bytes data)
            calldata = self._build_liquidate_calldata(
                market_params,
                borrower,
                seized_assets_wei,
                repaid_shares_wei,
                callback_data,
            )

            return TransactionResult(
                success=True,
                tx_data={
                    "to": self.morpho_address,
                    "value": 0,
                    "data": calldata,
                },
                gas_estimate=DEFAULT_GAS_ESTIMATES["liquidate"],
                description=f"Liquidate {borrower[:10]}... in Morpho Blue market {market_info['name']}, seize {seized_assets} {collateral_token}",
            )

        except Exception as e:
            logger.exception(f"Failed to build liquidate transaction: {e}")
            return TransactionResult(success=False, error=str(e))

    # =========================================================================
    # Authorization Operations
    # =========================================================================

    def set_authorization(
        self,
        authorized: str,
        is_authorized: bool,
    ) -> TransactionResult:
        """Build a set authorization transaction.

        Authorizes another address to manage positions on behalf of the caller.

        Args:
            authorized: Address to authorize/deauthorize
            is_authorized: Whether to grant or revoke authorization

        Returns:
            TransactionResult with transaction data
        """
        try:
            # Build calldata: setAuthorization(address authorized, bool newIsAuthorized)
            calldata = (
                MORPHO_SET_AUTHORIZATION_SELECTOR
                + self._pad_address(authorized)
                + self._pad_uint256(1 if is_authorized else 0)
            )

            action = "Authorize" if is_authorized else "Deauthorize"
            return TransactionResult(
                success=True,
                tx_data={
                    "to": self.morpho_address,
                    "value": 0,
                    "data": calldata,
                },
                gas_estimate=DEFAULT_GAS_ESTIMATES["set_authorization"],
                description=f"{action} {authorized[:10]}... for Morpho Blue",
            )

        except Exception as e:
            logger.exception(f"Failed to build set authorization transaction: {e}")
            return TransactionResult(success=False, error=str(e))

    # =========================================================================
    # Market Information
    # =========================================================================

    def get_market_info(self, market_id: str) -> dict[str, Any] | None:
        """Get information about a market.

        Args:
            market_id: Market identifier

        Returns:
            Market info dictionary or None if not found
        """
        return self._get_market_info(market_id)

    def get_markets(self) -> dict[str, dict[str, Any]]:
        """Get all known markets for the current chain.

        Returns:
            Dictionary mapping market_id to market info
        """
        return self.markets.copy()

    def get_market_params(self, market_id: str) -> MorphoBlueMarketParams | None:
        """Get market parameters for a market.

        Args:
            market_id: Market identifier

        Returns:
            MorphoBlueMarketParams or None if not found
        """
        market_info = self._get_market_info(market_id)
        if market_info is None:
            return None
        return self._get_market_params(market_info)

    # =========================================================================
    # Health Factor Calculations
    # =========================================================================

    def calculate_health_factor(
        self,
        collateral_amount: Decimal,
        collateral_price_usd: Decimal,
        debt_amount: Decimal,
        debt_price_usd: Decimal,
        lltv: Decimal,
    ) -> MorphoBlueHealthFactor:
        """Calculate health factor for a position.

        Health Factor = (Collateral Value * LLTV) / Debt Value

        Args:
            collateral_amount: Amount of collateral
            collateral_price_usd: Price of collateral in USD
            debt_amount: Amount of debt
            debt_price_usd: Price of debt token in USD
            lltv: Liquidation LTV (0-1 scale)

        Returns:
            MorphoBlueHealthFactor with calculated values
        """
        collateral_value_usd = collateral_amount * collateral_price_usd
        debt_value_usd = debt_amount * debt_price_usd

        if debt_value_usd == 0:
            health_factor = Decimal("999999")  # Effectively infinite
        else:
            health_factor = (collateral_value_usd * lltv) / debt_value_usd

        max_borrow_usd = collateral_value_usd * lltv - debt_value_usd
        if max_borrow_usd < 0:
            max_borrow_usd = Decimal("0")

        return MorphoBlueHealthFactor(
            collateral_value_usd=collateral_value_usd,
            debt_value_usd=debt_value_usd,
            lltv=lltv,
            health_factor=health_factor,
            max_borrow_usd=max_borrow_usd,
        )

    # =========================================================================
    # Approval Helpers
    # =========================================================================

    def build_approve_transaction(
        self,
        token: str,
        amount: Decimal | None = None,
        spender: str | None = None,
    ) -> TransactionResult:
        """Build an ERC20 approve transaction.

        Args:
            token: Token symbol or address to approve
            amount: Amount to approve (None for max)
            spender: Address to approve (defaults to Morpho Blue contract)

        Returns:
            TransactionResult with transaction data
        """
        try:
            token_address = self._resolve_token(token)
            if token_address is None:
                return TransactionResult(
                    success=False,
                    error=f"Unknown token: {token}",
                )

            target = spender or self.morpho_address

            if amount is None:
                amount_wei = MAX_UINT256
            else:
                decimals = self._get_decimals(token)
                amount_wei = int(amount * Decimal(10**decimals))

            # Build calldata: approve(address spender, uint256 amount)
            calldata = ERC20_APPROVE_SELECTOR + self._pad_address(target) + self._pad_uint256(amount_wei)

            amount_str = "unlimited" if amount is None else str(amount)
            return TransactionResult(
                success=True,
                tx_data={
                    "to": token_address,
                    "value": 0,
                    "data": calldata,
                },
                gas_estimate=DEFAULT_GAS_ESTIMATES["approve"],
                description=f"Approve {amount_str} {token} for Morpho Blue",
            )

        except Exception as e:
            logger.exception(f"Failed to build approve transaction: {e}")
            return TransactionResult(success=False, error=str(e))

    # =========================================================================
    # Private Helper Methods
    # =========================================================================

    def _get_market_info(self, market_id: str) -> dict[str, Any] | None:
        """Get market info by market_id."""
        # Normalize market_id
        if not market_id.startswith("0x"):
            market_id = "0x" + market_id
        market_id = market_id.lower()

        # Check in known markets
        for mid, info in self.markets.items():
            if mid.lower() == market_id:
                return info

        return None

    def _get_market_params(self, market_info: dict[str, Any]) -> MorphoBlueMarketParams:
        """Create MorphoBlueMarketParams from market info."""
        return MorphoBlueMarketParams(
            loan_token=market_info["loan_token_address"],
            collateral_token=market_info["collateral_token_address"],
            oracle=market_info["oracle"],
            irm=market_info["irm"],
            lltv=market_info["lltv"],
        )

    def _resolve_token(self, token: str) -> str:
        """Resolve token symbol or address to address using TokenResolver.

        Args:
            token: Token symbol or address

        Returns:
            Token address

        Raises:
            TokenResolutionError: If the token cannot be resolved
        """
        if token.startswith("0x") and len(token) == 42:
            return token
        try:
            resolved = self._token_resolver.resolve(token, self.chain)
            return resolved.address
        except TokenResolutionError as e:
            raise TokenResolutionError(
                token=token,
                chain=str(self.chain),
                reason=f"[MorphoBlueAdapter] Cannot resolve token: {e.reason}",
                suggestions=e.suggestions,
            ) from e

    def _get_decimals(self, token: str) -> int:
        """Get decimals for a token using TokenResolver.

        Args:
            token: Token symbol or address

        Returns:
            Number of decimals

        Raises:
            TokenResolutionError: If decimals cannot be determined
        """
        try:
            resolved = self._token_resolver.resolve(token, self.chain)
            return resolved.decimals
        except TokenResolutionError as e:
            raise TokenResolutionError(
                token=token,
                chain=str(self.chain),
                reason=f"[MorphoBlueAdapter] Cannot determine decimals: {e.reason}",
                suggestions=e.suggestions,
            ) from e

    def _default_price_oracle(self, token: str) -> Decimal:
        """Default price oracle (returns 1.0)."""
        return Decimal("1.0")

    # =========================================================================
    # Calldata Building Helpers
    # =========================================================================

    def _build_supply_calldata(
        self,
        market_params: MorphoBlueMarketParams,
        assets: int,
        shares: int,
        on_behalf_of: str,
        data: bytes,
    ) -> str:
        """Build calldata for supply function."""
        # supply(MarketParams memory marketParams, uint256 assets, uint256 shares, address onBehalfOf, bytes memory data)
        # Static slots: 5 (MarketParams) + 1 (assets) + 1 (shares) + 1 (onBehalfOf) + 1 (bytes offset) = 9
        offset_part, tail_part = self._encode_bytes(data, static_slots=9)
        return (
            MORPHO_SUPPLY_SELECTOR
            + self._encode_market_params(market_params)
            + self._pad_uint256(assets)
            + self._pad_uint256(shares)
            + self._pad_address(on_behalf_of)
            + offset_part
            + tail_part
        )

    def _build_withdraw_calldata(
        self,
        market_params: MorphoBlueMarketParams,
        assets: int,
        shares: int,
        on_behalf_of: str,
        receiver: str,
    ) -> str:
        """Build calldata for withdraw function."""
        # withdraw(MarketParams memory marketParams, uint256 assets, uint256 shares, address onBehalfOf, address receiver)
        return (
            MORPHO_WITHDRAW_SELECTOR
            + self._encode_market_params(market_params)
            + self._pad_uint256(assets)
            + self._pad_uint256(shares)
            + self._pad_address(on_behalf_of)
            + self._pad_address(receiver)
        )

    def _build_supply_collateral_calldata(
        self,
        market_params: MorphoBlueMarketParams,
        assets: int,
        on_behalf_of: str,
        data: bytes,
    ) -> str:
        """Build calldata for supplyCollateral function."""
        # supplyCollateral(MarketParams memory marketParams, uint256 assets, address onBehalfOf, bytes memory data)
        # Static slots: 5 (MarketParams) + 1 (assets) + 1 (onBehalfOf) + 1 (bytes offset) = 8
        offset_part, tail_part = self._encode_bytes(data, static_slots=8)
        return (
            MORPHO_SUPPLY_COLLATERAL_SELECTOR
            + self._encode_market_params(market_params)
            + self._pad_uint256(assets)
            + self._pad_address(on_behalf_of)
            + offset_part
            + tail_part
        )

    def _build_withdraw_collateral_calldata(
        self,
        market_params: MorphoBlueMarketParams,
        assets: int,
        on_behalf_of: str,
        receiver: str,
    ) -> str:
        """Build calldata for withdrawCollateral function."""
        # withdrawCollateral(MarketParams memory marketParams, uint256 assets, address onBehalfOf, address receiver)
        return (
            MORPHO_WITHDRAW_COLLATERAL_SELECTOR
            + self._encode_market_params(market_params)
            + self._pad_uint256(assets)
            + self._pad_address(on_behalf_of)
            + self._pad_address(receiver)
        )

    def _build_borrow_calldata(
        self,
        market_params: MorphoBlueMarketParams,
        assets: int,
        shares: int,
        on_behalf_of: str,
        receiver: str,
    ) -> str:
        """Build calldata for borrow function."""
        # borrow(MarketParams memory marketParams, uint256 assets, uint256 shares, address onBehalfOf, address receiver)
        return (
            MORPHO_BORROW_SELECTOR
            + self._encode_market_params(market_params)
            + self._pad_uint256(assets)
            + self._pad_uint256(shares)
            + self._pad_address(on_behalf_of)
            + self._pad_address(receiver)
        )

    def _build_repay_calldata(
        self,
        market_params: MorphoBlueMarketParams,
        assets: int,
        shares: int,
        on_behalf_of: str,
        data: bytes,
    ) -> str:
        """Build calldata for repay function."""
        # repay(MarketParams memory marketParams, uint256 assets, uint256 shares, address onBehalfOf, bytes memory data)
        # Static slots: 5 (MarketParams) + 1 (assets) + 1 (shares) + 1 (onBehalfOf) + 1 (bytes offset) = 9
        offset_part, tail_part = self._encode_bytes(data, static_slots=9)
        return (
            MORPHO_REPAY_SELECTOR
            + self._encode_market_params(market_params)
            + self._pad_uint256(assets)
            + self._pad_uint256(shares)
            + self._pad_address(on_behalf_of)
            + offset_part
            + tail_part
        )

    def _build_flash_loan_calldata(
        self,
        token: str,
        assets: int,
        data: bytes,
    ) -> str:
        """Build calldata for flashLoan function."""
        # flashLoan(address token, uint256 assets, bytes calldata data)
        # Static slots: 1 (token) + 1 (assets) + 1 (bytes offset) = 3
        offset_part, tail_part = self._encode_bytes(data, static_slots=3)
        return (
            MORPHO_FLASH_LOAN_SELECTOR + self._pad_address(token) + self._pad_uint256(assets) + offset_part + tail_part
        )

    def _build_liquidate_calldata(
        self,
        market_params: MorphoBlueMarketParams,
        borrower: str,
        seized_assets: int,
        repaid_shares: int,
        data: bytes,
    ) -> str:
        """Build calldata for liquidate function."""
        # liquidate(MarketParams memory marketParams, address borrower, uint256 seizedAssets, uint256 repaidShares, bytes calldata data)
        # Static slots: 5 (MarketParams) + 1 (borrower) + 1 (seizedAssets) + 1 (repaidShares) + 1 (bytes offset) = 9
        offset_part, tail_part = self._encode_bytes(data, static_slots=9)
        return (
            MORPHO_LIQUIDATE_SELECTOR
            + self._encode_market_params(market_params)
            + self._pad_address(borrower)
            + self._pad_uint256(seized_assets)
            + self._pad_uint256(repaid_shares)
            + offset_part
            + tail_part
        )

    def _encode_market_params(self, params: MorphoBlueMarketParams) -> str:
        """Encode MarketParams struct.

        MarketParams is: (address loanToken, address collateralToken, address oracle, address irm, uint256 lltv)
        This is encoded inline as 5 slots (not ABI dynamic struct encoding).
        """
        return (
            self._pad_address(params.loan_token)
            + self._pad_address(params.collateral_token)
            + self._pad_address(params.oracle)
            + self._pad_address(params.irm)
            + self._pad_uint256(params.lltv)
        )

    def _encode_bytes(self, data: bytes, static_slots: int = 8) -> tuple[str, str]:
        """Encode bytes for calldata (dynamic type).

        For ABI encoding of dynamic bytes:
        - Returns (offset_part, tail_part) where:
          - offset_part: 32-byte offset pointing to where the length starts
          - tail_part: length (32 bytes) + padded data

        Args:
            data: The bytes to encode
            static_slots: Number of 32-byte slots before the bytes data starts
                         (used to calculate the offset). Default 8 for supplyCollateral.

        Returns:
            Tuple of (offset_hex, tail_hex) to be placed in appropriate positions
        """
        offset = static_slots * 32  # Offset in bytes
        offset_part = self._pad_uint256(offset)

        if len(data) == 0:
            # Empty bytes: just the length = 0
            tail_part = self._pad_uint256(0)
        else:
            # Non-empty: length + padded data
            length = len(data)
            padded_length = (length + 31) // 32 * 32
            padded_data = data.ljust(padded_length, b"\x00")
            tail_part = self._pad_uint256(length) + padded_data.hex()

        return (offset_part, tail_part)

    def _pad_address(self, address: str) -> str:
        """Pad address to 32 bytes."""
        addr = address.lower().replace("0x", "")
        return addr.zfill(64)

    def _pad_uint256(self, value: int) -> str:
        """Pad uint256 to 32 bytes."""
        return hex(value)[2:].zfill(64)

    def _pad_uint16(self, value: int) -> str:
        """Pad uint16 to 32 bytes (for ABI encoding)."""
        return hex(value)[2:].zfill(64)


# =============================================================================
# Factory Functions
# =============================================================================


def create_adapter_with_prices(
    config: MorphoBlueConfig,
    prices: dict[str, Decimal],
) -> MorphoBlueAdapter:
    """Create an adapter with a dictionary of real prices.

    This is the recommended way to create an adapter for production use.
    It ensures accurate health factor calculations by providing real prices.

    Args:
        config: Adapter configuration
        prices: Dict mapping token symbols to USD prices

    Returns:
        MorphoBlueAdapter configured with real prices

    Example:
        prices = {
            "USDC": Decimal("1.00"),
            "USDT": Decimal("1.00"),
            "wstETH": Decimal("3500.00"),
            "WETH": Decimal("3100.00"),
            "WBTC": Decimal("98000.00"),
        }
        config = MorphoBlueConfig(
            chain="ethereum",
            wallet_address="0x...",
        )
        adapter = create_adapter_with_prices(config, prices)
    """

    def price_oracle(asset: str) -> Decimal:
        """Look up price from the provided dictionary."""
        # Try exact match
        if asset in prices:
            return prices[asset]
        # Try uppercase
        if asset.upper() in prices:
            return prices[asset.upper()]
        # Try lowercase
        if asset.lower() in prices:
            return prices[asset.lower()]
        raise KeyError(f"No price found for '{asset}' in provided prices dict")

    return MorphoBlueAdapter(config, price_oracle=price_oracle)


def create_test_adapter(
    chain: str = "ethereum",
    wallet_address: str = "0x1234567890123456789012345678901234567890",
) -> MorphoBlueAdapter:
    """Create a test adapter with placeholder prices and SDK disabled.

    For unit testing only. DO NOT use in production.

    Args:
        chain: Chain name (default: ethereum)
        wallet_address: Wallet address (default: test address)

    Returns:
        MorphoBlueAdapter configured for testing
    """
    config = MorphoBlueConfig(
        chain=chain,
        wallet_address=wallet_address,
        allow_placeholder_prices=True,
        enable_sdk=False,  # Disable SDK for unit tests
    )
    return MorphoBlueAdapter(config)
