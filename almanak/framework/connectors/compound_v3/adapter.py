"""Compound V3 (Comet) Adapter.

This module provides an adapter for interacting with Compound V3 (Comet) lending protocol,
supporting supply, withdraw, borrow, and repay operations.

Compound V3 (Comet) is a lending protocol that allows:
- Supplying base assets to earn yield
- Supplying collateral assets for borrowing
- Borrowing base assets against collateral
- Variable interest rates per market

Key differences from traditional Compound:
- Single borrowable asset (base) per market (e.g., USDC, WETH)
- Multiple collateral assets per market
- No cTokens for collateral (only for base asset lending)
- Simplified liquidation model

Supported chains:
- Ethereum
- Arbitrum

Example:
    from almanak.framework.connectors.compound_v3 import CompoundV3Adapter, CompoundV3Config

    config = CompoundV3Config(
        chain="ethereum",
        wallet_address="0x...",
        market="usdc",  # or "weth", "usdt"
    )
    adapter = CompoundV3Adapter(config)

    # Supply base asset to earn interest
    result = adapter.supply(
        amount=Decimal("1000"),
    )

    # Supply collateral for borrowing
    result = adapter.supply_collateral(
        asset="WETH",
        amount=Decimal("1.0"),
    )

    # Borrow against collateral
    result = adapter.borrow(
        amount=Decimal("500"),
    )
"""

import logging
from collections.abc import Callable
from dataclasses import dataclass, field
from decimal import Decimal
from typing import TYPE_CHECKING, Any

from almanak.framework.data.tokens.exceptions import TokenResolutionError

if TYPE_CHECKING:
    from almanak.framework.data.tokens.resolver import TokenResolver as TokenResolverType

logger = logging.getLogger(__name__)


# =============================================================================
# Constants
# =============================================================================

# Compound V3 Comet contract addresses per chain and market
COMPOUND_V3_COMET_ADDRESSES: dict[str, dict[str, str]] = {
    "ethereum": {
        "usdc": "0xc3d688B66703497DAA19211EEdff47f25384cdc3",
        "weth": "0xA17581A9E3356d9A858b789D68B4d866e593aE94",
        "usdt": "0x3Afdc9BCA9213A35503b077a6072F3D0d5AB0840",
        "wsteth": "0x3D0bb1ccaB520A66e607822fC55BC921738fAFE3",
        "usds": "0x5D409e56D886231aDAf00c8775665AD0f9897b56",
    },
    "arbitrum": {
        "usdc_bridged": "0xA5EDBDD9646f8dFF606d7448e414884C7d905dCA",
        "usdc": "0x9c4ec768c28520B50860ea7a15bd7213a9fF58bf",
        "weth": "0x6f7D514bbD4aFf3BcD1140B7344b32f063dEe486",
        "usdt": "0xd98Be00b5D27fc98112BdE293e487f8D4cA57d07",
    },
    "base": {
        "usdc": "0xb125E6687d4313864e53df431d5425969c15Eb2F",
        "weth": "0x46e6b214b524310239732D51387075E0e70970bf",
        "aero": "0x784efeB622244d2348d4F2522f8860B96fbEcE89",
    },
}


# Compound V3 function selectors
COMPOUND_V3_SUPPLY_SELECTOR = "0xf2b9fdb8"  # supply(address,uint256)
COMPOUND_V3_SUPPLY_TO_SELECTOR = "0x4232cd63"  # supplyTo(address,address,uint256)
COMPOUND_V3_SUPPLY_FROM_SELECTOR = "0x2a7c6ef0"  # supplyFrom(address,address,address,uint256)
COMPOUND_V3_WITHDRAW_SELECTOR = "0xf3fef3a3"  # withdraw(address,uint256)
COMPOUND_V3_WITHDRAW_TO_SELECTOR = "0x8013f3a7"  # withdrawTo(address,address,uint256)
COMPOUND_V3_WITHDRAW_FROM_SELECTOR = "0x7eb8ff0d"  # withdrawFrom(address,address,address,uint256)
COMPOUND_V3_ABSORB_SELECTOR = "0xf8138c6e"  # absorb(address,address[])
COMPOUND_V3_BUY_COLLATERAL_SELECTOR = "0x3c447ff5"  # buyCollateral(address,uint256,uint256,address)

# ERC20 approve selector
ERC20_APPROVE_SELECTOR = "0x095ea7b3"  # approve(address,uint256)

# Max values
MAX_UINT256 = 2**256 - 1

# Gas estimates for Compound V3 operations
DEFAULT_GAS_ESTIMATES: dict[str, int] = {
    "supply": 150000,
    "supply_collateral": 150000,
    "withdraw": 180000,
    "withdraw_collateral": 180000,
    "borrow": 200000,
    "repay": 150000,
    "absorb": 400000,
    "buy_collateral": 250000,
    "approve": 46000,
}

# Market configurations (base asset and supported collateral)
COMPOUND_V3_MARKETS: dict[str, dict[str, dict[str, Any]]] = {
    "ethereum": {
        "usdc": {
            "name": "USDC Market",
            "base_token": "USDC",
            "base_token_address": "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
            "collaterals": {
                "WETH": {
                    "address": "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2",
                    "borrow_collateral_factor": Decimal("0.825"),
                    "liquidation_collateral_factor": Decimal("0.895"),
                    "liquidation_factor": Decimal("0.95"),
                },
                "WBTC": {
                    "address": "0x2260FAC5E5542a773Aa44fBCfeDf7C193bc2C599",
                    "borrow_collateral_factor": Decimal("0.70"),
                    "liquidation_collateral_factor": Decimal("0.77"),
                    "liquidation_factor": Decimal("0.95"),
                },
                "COMP": {
                    "address": "0xc00e94Cb662C3520282E6f5717214004A7f26888",
                    "borrow_collateral_factor": Decimal("0.65"),
                    "liquidation_collateral_factor": Decimal("0.70"),
                    "liquidation_factor": Decimal("0.93"),
                },
                "UNI": {
                    "address": "0x1f9840a85d5aF5bf1D1762F925BDADdC4201F984",
                    "borrow_collateral_factor": Decimal("0.75"),
                    "liquidation_collateral_factor": Decimal("0.81"),
                    "liquidation_factor": Decimal("0.93"),
                },
                "LINK": {
                    "address": "0x514910771AF9Ca656af840dff83E8264EcF986CA",
                    "borrow_collateral_factor": Decimal("0.79"),
                    "liquidation_collateral_factor": Decimal("0.85"),
                    "liquidation_factor": Decimal("0.93"),
                },
            },
        },
        "weth": {
            "name": "WETH Market",
            "base_token": "WETH",
            "base_token_address": "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2",
            "collaterals": {
                "wstETH": {
                    "address": "0x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0",
                    "borrow_collateral_factor": Decimal("0.90"),
                    "liquidation_collateral_factor": Decimal("0.93"),
                    "liquidation_factor": Decimal("0.975"),
                },
                "cbETH": {
                    "address": "0xBe9895146f7AF43049ca1c1AE358B0541Ea49704",
                    "borrow_collateral_factor": Decimal("0.90"),
                    "liquidation_collateral_factor": Decimal("0.93"),
                    "liquidation_factor": Decimal("0.975"),
                },
                "rETH": {
                    "address": "0xae78736Cd615f374D3085123A210448E74Fc6393",
                    "borrow_collateral_factor": Decimal("0.90"),
                    "liquidation_collateral_factor": Decimal("0.93"),
                    "liquidation_factor": Decimal("0.975"),
                },
            },
        },
        "usdt": {
            "name": "USDT Market",
            "base_token": "USDT",
            "base_token_address": "0xdAC17F958D2ee523a2206206994597C13D831ec7",
            "collaterals": {
                "WETH": {
                    "address": "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2",
                    "borrow_collateral_factor": Decimal("0.825"),
                    "liquidation_collateral_factor": Decimal("0.895"),
                    "liquidation_factor": Decimal("0.95"),
                },
                "WBTC": {
                    "address": "0x2260FAC5E5542a773Aa44fBCfeDf7C193bc2C599",
                    "borrow_collateral_factor": Decimal("0.70"),
                    "liquidation_collateral_factor": Decimal("0.77"),
                    "liquidation_factor": Decimal("0.95"),
                },
            },
        },
    },
    "arbitrum": {
        "usdc": {
            "name": "USDC Market (Native)",
            "base_token": "USDC",
            "base_token_address": "0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
            "collaterals": {
                "WETH": {
                    "address": "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1",
                    "borrow_collateral_factor": Decimal("0.825"),
                    "liquidation_collateral_factor": Decimal("0.895"),
                    "liquidation_factor": Decimal("0.95"),
                },
                "WBTC": {
                    "address": "0x2f2a2543B76A4166549F7aaB2e75Bef0aefC5B0f",
                    "borrow_collateral_factor": Decimal("0.70"),
                    "liquidation_collateral_factor": Decimal("0.77"),
                    "liquidation_factor": Decimal("0.95"),
                },
                "ARB": {
                    "address": "0x912CE59144191C1204E64559FE8253a0e49E6548",
                    "borrow_collateral_factor": Decimal("0.55"),
                    "liquidation_collateral_factor": Decimal("0.60"),
                    "liquidation_factor": Decimal("0.90"),
                },
                "GMX": {
                    "address": "0xfc5A1A6EB076a2C7aD06eD22C90d7E710E35ad0a",
                    "borrow_collateral_factor": Decimal("0.50"),
                    "liquidation_collateral_factor": Decimal("0.55"),
                    "liquidation_factor": Decimal("0.90"),
                },
            },
        },
        "usdc_bridged": {
            "name": "USDC.e Market (Bridged)",
            "base_token": "USDC.e",
            "base_token_address": "0xFF970A61A04b1cA14834A43f5dE4533eBDDB5CC8",
            "collaterals": {
                "WETH": {
                    "address": "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1",
                    "borrow_collateral_factor": Decimal("0.825"),
                    "liquidation_collateral_factor": Decimal("0.895"),
                    "liquidation_factor": Decimal("0.95"),
                },
                "WBTC": {
                    "address": "0x2f2a2543B76A4166549F7aaB2e75Bef0aefC5B0f",
                    "borrow_collateral_factor": Decimal("0.70"),
                    "liquidation_collateral_factor": Decimal("0.77"),
                    "liquidation_factor": Decimal("0.95"),
                },
            },
        },
        "weth": {
            "name": "WETH Market",
            "base_token": "WETH",
            "base_token_address": "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1",
            "collaterals": {
                "wstETH": {
                    "address": "0x5979D7b546E38E414F7E9822514be443A4800529",
                    "borrow_collateral_factor": Decimal("0.90"),
                    "liquidation_collateral_factor": Decimal("0.93"),
                    "liquidation_factor": Decimal("0.975"),
                },
                "rETH": {
                    "address": "0xEC70Dcb4A1EFa46b8F2D97C310C9c4790ba5ffA8",
                    "borrow_collateral_factor": Decimal("0.90"),
                    "liquidation_collateral_factor": Decimal("0.93"),
                    "liquidation_factor": Decimal("0.975"),
                },
            },
        },
        "usdt": {
            "name": "USDT Market",
            "base_token": "USDT",
            "base_token_address": "0xFd086bC7CD5C481DCC9C85ebE478A1C0b69FCbb9",
            "collaterals": {
                "WETH": {
                    "address": "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1",
                    "borrow_collateral_factor": Decimal("0.825"),
                    "liquidation_collateral_factor": Decimal("0.895"),
                    "liquidation_factor": Decimal("0.95"),
                },
                "WBTC": {
                    "address": "0x2f2a2543B76A4166549F7aaB2e75Bef0aefC5B0f",
                    "borrow_collateral_factor": Decimal("0.70"),
                    "liquidation_collateral_factor": Decimal("0.77"),
                    "liquidation_factor": Decimal("0.95"),
                },
            },
        },
    },
    "base": {
        "usdc": {
            "name": "USDC Market",
            "base_token": "USDC",
            "base_token_address": "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913",
            "collaterals": {
                "WETH": {
                    "address": "0x4200000000000000000000000000000000000006",
                    "borrow_collateral_factor": Decimal("0.80"),
                    "liquidation_collateral_factor": Decimal("0.85"),
                    "liquidation_factor": Decimal("0.95"),
                },
                "cbETH": {
                    "address": "0x2Ae3F1Ec7F1F5012CFEab0185bfc7aa3cf0DEc22",
                    "borrow_collateral_factor": Decimal("0.80"),
                    "liquidation_collateral_factor": Decimal("0.85"),
                    "liquidation_factor": Decimal("0.95"),
                },
            },
        },
        "weth": {
            "name": "WETH Market",
            "base_token": "WETH",
            "base_token_address": "0x4200000000000000000000000000000000000006",
            "collaterals": {
                "cbETH": {
                    "address": "0x2Ae3F1Ec7F1F5012CFEab0185bfc7aa3cf0DEc22",
                    "borrow_collateral_factor": Decimal("0.90"),
                    "liquidation_collateral_factor": Decimal("0.93"),
                    "liquidation_factor": Decimal("0.975"),
                },
                "wstETH": {
                    "address": "0xc1CBa3fCea344f92D9239c08C0568f6F2F0ee452",
                    "borrow_collateral_factor": Decimal("0.90"),
                    "liquidation_collateral_factor": Decimal("0.93"),
                    "liquidation_factor": Decimal("0.975"),
                },
            },
        },
        "aero": {
            "name": "AERO Market",
            "base_token": "AERO",
            "base_token_address": "0x940181a94A35A4569E4529A3CDfB74e38FD98631",
            "collaterals": {
                "WETH": {
                    "address": "0x4200000000000000000000000000000000000006",
                    "borrow_collateral_factor": Decimal("0.80"),
                    "liquidation_collateral_factor": Decimal("0.85"),
                    "liquidation_factor": Decimal("0.95"),
                },
            },
        },
    },
}


# =============================================================================
# Data Classes
# =============================================================================


@dataclass
class CompoundV3Config:
    """Configuration for Compound V3 adapter.

    Attributes:
        chain: Blockchain network (ethereum, arbitrum)
        wallet_address: User wallet address
        market: Market identifier (usdc, weth, usdt, etc.)
        default_slippage_bps: Default slippage tolerance in basis points
    """

    chain: str
    wallet_address: str
    market: str = "usdc"
    default_slippage_bps: int = 50  # 0.5%

    def __post_init__(self) -> None:
        """Validate configuration."""
        valid_chains = set(COMPOUND_V3_COMET_ADDRESSES.keys())
        if self.chain not in valid_chains:
            raise ValueError(f"Invalid chain: {self.chain}. Valid chains: {valid_chains}")
        if not self.wallet_address.startswith("0x") or len(self.wallet_address) != 42:
            raise ValueError(f"Invalid wallet address: {self.wallet_address}. Must be 0x-prefixed 40 hex chars.")
        valid_markets = set(COMPOUND_V3_COMET_ADDRESSES.get(self.chain, {}).keys())
        if self.market not in valid_markets:
            raise ValueError(f"Invalid market: {self.market}. Valid markets for {self.chain}: {valid_markets}")
        if self.default_slippage_bps < 0 or self.default_slippage_bps > 10000:
            raise ValueError(f"Invalid slippage: {self.default_slippage_bps}. Must be 0-10000 bps.")


@dataclass
class CompoundV3MarketInfo:
    """Information about a Compound V3 market.

    Attributes:
        market_id: Market identifier (e.g., "usdc")
        name: Human-readable market name
        base_token: Symbol of the base token (borrowable asset)
        base_token_address: Address of the base token
        comet_address: Address of the Comet contract
        collaterals: Dictionary of supported collateral assets
    """

    market_id: str
    name: str
    base_token: str
    base_token_address: str
    comet_address: str
    collaterals: dict[str, dict[str, Any]]

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "market_id": self.market_id,
            "name": self.name,
            "base_token": self.base_token,
            "base_token_address": self.base_token_address,
            "comet_address": self.comet_address,
            "collaterals": {
                k: {
                    "address": v["address"],
                    "borrow_collateral_factor": str(v["borrow_collateral_factor"]),
                    "liquidation_collateral_factor": str(v["liquidation_collateral_factor"]),
                    "liquidation_factor": str(v["liquidation_factor"]),
                }
                for k, v in self.collaterals.items()
            },
        }


@dataclass
class CompoundV3Position:
    """User position in a Compound V3 market.

    Attributes:
        market_id: Market identifier
        base_balance: Balance of base token (positive = supply, negative = borrow)
        collateral_balances: Balances of collateral tokens
    """

    market_id: str
    base_balance: Decimal = Decimal("0")
    collateral_balances: dict[str, Decimal] = field(default_factory=dict)

    @property
    def is_supplier(self) -> bool:
        """Check if user is a net supplier."""
        return self.base_balance > 0

    @property
    def is_borrower(self) -> bool:
        """Check if user is a net borrower."""
        return self.base_balance < 0

    @property
    def borrow_balance(self) -> Decimal:
        """Get the borrow balance (positive value)."""
        return abs(self.base_balance) if self.base_balance < 0 else Decimal("0")

    @property
    def supply_balance(self) -> Decimal:
        """Get the supply balance."""
        return self.base_balance if self.base_balance > 0 else Decimal("0")

    @property
    def has_collateral(self) -> bool:
        """Check if user has any collateral."""
        return any(bal > 0 for bal in self.collateral_balances.values())

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "market_id": self.market_id,
            "base_balance": str(self.base_balance),
            "supply_balance": str(self.supply_balance),
            "borrow_balance": str(self.borrow_balance),
            "is_supplier": self.is_supplier,
            "is_borrower": self.is_borrower,
            "collateral_balances": {k: str(v) for k, v in self.collateral_balances.items()},
            "has_collateral": self.has_collateral,
        }


@dataclass
class CompoundV3HealthFactor:
    """Health factor calculation for a Compound V3 position.

    Attributes:
        collateral_value_usd: Total value of collateral in USD
        borrow_value_usd: Total value of borrowed assets in USD
        borrow_capacity_usd: Maximum borrowable amount based on collateral
        liquidation_threshold_usd: USD debt level at which liquidation occurs
        health_factor: Calculated health factor (liquidation_threshold / borrow)
        is_liquidatable: Whether the position can be liquidated
    """

    collateral_value_usd: Decimal
    borrow_value_usd: Decimal
    borrow_capacity_usd: Decimal
    liquidation_threshold_usd: Decimal
    health_factor: Decimal
    is_liquidatable: bool = False

    @property
    def is_healthy(self) -> bool:
        """Check if position is healthy (HF >= 1)."""
        return self.health_factor >= Decimal("1.0")

    @property
    def available_borrow_usd(self) -> Decimal:
        """Get remaining borrowable amount in USD."""
        return max(Decimal("0"), self.borrow_capacity_usd - self.borrow_value_usd)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "collateral_value_usd": str(self.collateral_value_usd),
            "borrow_value_usd": str(self.borrow_value_usd),
            "borrow_capacity_usd": str(self.borrow_capacity_usd),
            "liquidation_threshold_usd": str(self.liquidation_threshold_usd),
            "health_factor": str(self.health_factor),
            "is_healthy": self.is_healthy,
            "is_liquidatable": self.is_liquidatable,
            "available_borrow_usd": str(self.available_borrow_usd),
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


class CompoundV3Adapter:
    """Adapter for Compound V3 (Comet) lending protocol.

    This adapter provides methods for interacting with Compound V3:
    - Supply/withdraw base assets (lending)
    - Supply/withdraw collateral assets
    - Borrow/repay base assets
    - Health factor calculations

    Compound V3 uses a single borrowable asset (base) per market with multiple
    collateral options. Unlike traditional Compound, collateral does not earn
    interest - only base asset suppliers earn yield.

    Example:
        config = CompoundV3Config(
            chain="ethereum",
            wallet_address="0x...",
            market="usdc",
        )
        adapter = CompoundV3Adapter(config)

        # Supply base asset to earn interest
        result = adapter.supply(amount=Decimal("1000"))

        # Supply collateral for borrowing
        result = adapter.supply_collateral(asset="WETH", amount=Decimal("1.0"))

        # Borrow against collateral
        result = adapter.borrow(amount=Decimal("500"))
    """

    def __init__(
        self,
        config: CompoundV3Config,
        price_oracle: PriceOracle | None = None,
        token_resolver: "TokenResolverType | None" = None,
    ) -> None:
        """Initialize the adapter.

        Args:
            config: Adapter configuration
            price_oracle: Optional price oracle callback
            token_resolver: Optional TokenResolver instance (defaults to singleton)
        """
        self.config = config
        self.chain = config.chain
        self.wallet_address = config.wallet_address
        self.market = config.market

        # Contract addresses
        self.comet_address = COMPOUND_V3_COMET_ADDRESSES[config.chain][config.market]

        # Market configuration
        self.market_config = COMPOUND_V3_MARKETS.get(config.chain, {}).get(config.market, {})

        # Price oracle
        self._price_oracle = price_oracle or self._default_price_oracle

        # Token resolver
        if token_resolver is not None:
            self._token_resolver = token_resolver
        else:
            from almanak.framework.data.tokens.resolver import get_token_resolver

            self._token_resolver = get_token_resolver()

        logger.info(
            f"CompoundV3Adapter initialized for chain={config.chain}, "
            f"market={config.market}, wallet={config.wallet_address[:10]}..."
        )

    # =========================================================================
    # Supply Operations (Base Asset Lending)
    # =========================================================================

    def supply(
        self,
        amount: Decimal,
        on_behalf_of: str | None = None,
    ) -> TransactionResult:
        """Build a supply transaction for the base asset.

        Supplies the base asset (e.g., USDC) to earn interest.

        Args:
            amount: Amount of base token to supply
            on_behalf_of: Address to credit (defaults to wallet_address)

        Returns:
            TransactionResult with transaction data
        """
        try:
            base_token = self.market_config.get("base_token", "USDC")
            base_token_address = self.market_config.get("base_token_address")
            if not base_token_address:
                return TransactionResult(
                    success=False,
                    error=f"Unknown base token for market: {self.market}",
                )

            decimals = self._get_decimals(base_token)
            amount_wei = int(amount * Decimal(10**decimals))
            recipient = on_behalf_of or self.wallet_address

            if recipient == self.wallet_address:
                # Use simple supply(address,uint256)
                calldata = self._build_supply_calldata(base_token_address, amount_wei)
            else:
                # Use supplyTo(address,address,uint256)
                calldata = self._build_supply_to_calldata(recipient, base_token_address, amount_wei)

            return TransactionResult(
                success=True,
                tx_data={
                    "to": self.comet_address,
                    "value": 0,
                    "data": calldata,
                },
                gas_estimate=DEFAULT_GAS_ESTIMATES["supply"],
                description=f"Supply {amount} {base_token} to Compound V3 {self.market_config.get('name', self.market)}",
            )

        except Exception as e:
            logger.exception(f"Failed to build supply transaction: {e}")
            return TransactionResult(success=False, error=str(e))

    def withdraw(
        self,
        amount: Decimal,
        receiver: str | None = None,
        withdraw_all: bool = False,
    ) -> TransactionResult:
        """Build a withdraw transaction for the base asset.

        Withdraws supplied base asset from the market.

        Args:
            amount: Amount of base token to withdraw
            receiver: Address to receive tokens (defaults to wallet_address)
            withdraw_all: If True, withdraws all supplied base asset

        Returns:
            TransactionResult with transaction data
        """
        try:
            base_token = self.market_config.get("base_token", "USDC")
            base_token_address = self.market_config.get("base_token_address")
            if not base_token_address:
                return TransactionResult(
                    success=False,
                    error=f"Unknown base token for market: {self.market}",
                )

            decimals = self._get_decimals(base_token)
            recipient = receiver or self.wallet_address

            if withdraw_all:
                amount_wei = MAX_UINT256
            else:
                amount_wei = int(amount * Decimal(10**decimals))

            if recipient == self.wallet_address:
                # Use simple withdraw(address,uint256)
                calldata = self._build_withdraw_calldata(base_token_address, amount_wei)
            else:
                # Use withdrawTo(address,address,uint256)
                calldata = self._build_withdraw_to_calldata(recipient, base_token_address, amount_wei)

            amount_str = "all" if withdraw_all else f"{amount} {base_token}"
            return TransactionResult(
                success=True,
                tx_data={
                    "to": self.comet_address,
                    "value": 0,
                    "data": calldata,
                },
                gas_estimate=DEFAULT_GAS_ESTIMATES["withdraw"],
                description=f"Withdraw {amount_str} from Compound V3 {self.market_config.get('name', self.market)}",
            )

        except Exception as e:
            logger.exception(f"Failed to build withdraw transaction: {e}")
            return TransactionResult(success=False, error=str(e))

    # =========================================================================
    # Collateral Operations
    # =========================================================================

    def supply_collateral(
        self,
        asset: str,
        amount: Decimal,
        on_behalf_of: str | None = None,
    ) -> TransactionResult:
        """Build a supply collateral transaction.

        Supplies collateral to enable borrowing.

        Args:
            asset: Collateral asset symbol (e.g., "WETH")
            amount: Amount of collateral to supply
            on_behalf_of: Address to credit (defaults to wallet_address)

        Returns:
            TransactionResult with transaction data
        """
        try:
            collaterals = self.market_config.get("collaterals", {})
            if asset not in collaterals:
                return TransactionResult(
                    success=False,
                    error=f"Unsupported collateral: {asset}. Supported: {list(collaterals.keys())}",
                )

            asset_address = collaterals[asset]["address"]
            decimals = self._get_decimals(asset)
            amount_wei = int(amount * Decimal(10**decimals))
            recipient = on_behalf_of or self.wallet_address

            if recipient == self.wallet_address:
                # Use simple supply(address,uint256)
                calldata = self._build_supply_calldata(asset_address, amount_wei)
            else:
                # Use supplyTo(address,address,uint256)
                calldata = self._build_supply_to_calldata(recipient, asset_address, amount_wei)

            return TransactionResult(
                success=True,
                tx_data={
                    "to": self.comet_address,
                    "value": 0,
                    "data": calldata,
                },
                gas_estimate=DEFAULT_GAS_ESTIMATES["supply_collateral"],
                description=f"Supply {amount} {asset} as collateral to Compound V3 {self.market_config.get('name', self.market)}",
            )

        except Exception as e:
            logger.exception(f"Failed to build supply collateral transaction: {e}")
            return TransactionResult(success=False, error=str(e))

    def withdraw_collateral(
        self,
        asset: str,
        amount: Decimal,
        receiver: str | None = None,
        withdraw_all: bool = False,
    ) -> TransactionResult:
        """Build a withdraw collateral transaction.

        Withdraws collateral from the market.

        Args:
            asset: Collateral asset symbol (e.g., "WETH")
            amount: Amount of collateral to withdraw
            receiver: Address to receive tokens (defaults to wallet_address)
            withdraw_all: If True, withdraws all collateral for this asset

        Returns:
            TransactionResult with transaction data
        """
        try:
            collaterals = self.market_config.get("collaterals", {})
            if asset not in collaterals:
                return TransactionResult(
                    success=False,
                    error=f"Unsupported collateral: {asset}. Supported: {list(collaterals.keys())}",
                )

            asset_address = collaterals[asset]["address"]
            decimals = self._get_decimals(asset)
            recipient = receiver or self.wallet_address

            if withdraw_all:
                amount_wei = MAX_UINT256
            else:
                amount_wei = int(amount * Decimal(10**decimals))

            if recipient == self.wallet_address:
                # Use simple withdraw(address,uint256)
                calldata = self._build_withdraw_calldata(asset_address, amount_wei)
            else:
                # Use withdrawTo(address,address,uint256)
                calldata = self._build_withdraw_to_calldata(recipient, asset_address, amount_wei)

            amount_str = "all" if withdraw_all else f"{amount} {asset}"
            return TransactionResult(
                success=True,
                tx_data={
                    "to": self.comet_address,
                    "value": 0,
                    "data": calldata,
                },
                gas_estimate=DEFAULT_GAS_ESTIMATES["withdraw_collateral"],
                description=f"Withdraw {amount_str} collateral from Compound V3 {self.market_config.get('name', self.market)}",
            )

        except Exception as e:
            logger.exception(f"Failed to build withdraw collateral transaction: {e}")
            return TransactionResult(success=False, error=str(e))

    # =========================================================================
    # Borrow Operations
    # =========================================================================

    def borrow(
        self,
        amount: Decimal,
        receiver: str | None = None,
    ) -> TransactionResult:
        """Build a borrow transaction.

        Borrows the base asset against supplied collateral.

        Args:
            amount: Amount of base token to borrow
            receiver: Address to receive borrowed tokens (defaults to wallet_address)

        Returns:
            TransactionResult with transaction data
        """
        try:
            base_token = self.market_config.get("base_token", "USDC")
            base_token_address = self.market_config.get("base_token_address")
            if not base_token_address:
                return TransactionResult(
                    success=False,
                    error=f"Unknown base token for market: {self.market}",
                )

            decimals = self._get_decimals(base_token)
            amount_wei = int(amount * Decimal(10**decimals))
            recipient = receiver or self.wallet_address

            # In Compound V3, borrowing is done via withdraw when you have collateral
            # but no supplied base asset
            if recipient == self.wallet_address:
                calldata = self._build_withdraw_calldata(base_token_address, amount_wei)
            else:
                calldata = self._build_withdraw_to_calldata(recipient, base_token_address, amount_wei)

            return TransactionResult(
                success=True,
                tx_data={
                    "to": self.comet_address,
                    "value": 0,
                    "data": calldata,
                },
                gas_estimate=DEFAULT_GAS_ESTIMATES["borrow"],
                description=f"Borrow {amount} {base_token} from Compound V3 {self.market_config.get('name', self.market)}",
            )

        except Exception as e:
            logger.exception(f"Failed to build borrow transaction: {e}")
            return TransactionResult(success=False, error=str(e))

    def repay(
        self,
        amount: Decimal,
        on_behalf_of: str | None = None,
        repay_all: bool = False,
    ) -> TransactionResult:
        """Build a repay transaction.

        Repays borrowed base asset.

        Args:
            amount: Amount of base token to repay
            on_behalf_of: Address with debt (defaults to wallet_address)
            repay_all: If True, repays full debt

        Returns:
            TransactionResult with transaction data
        """
        try:
            base_token = self.market_config.get("base_token", "USDC")
            base_token_address = self.market_config.get("base_token_address")
            if not base_token_address:
                return TransactionResult(
                    success=False,
                    error=f"Unknown base token for market: {self.market}",
                )

            decimals = self._get_decimals(base_token)
            recipient = on_behalf_of or self.wallet_address

            if repay_all:
                # Use max uint256 to repay all debt
                amount_wei = MAX_UINT256
            else:
                amount_wei = int(amount * Decimal(10**decimals))

            # In Compound V3, repaying is done via supply when you have a borrow position
            if recipient == self.wallet_address:
                calldata = self._build_supply_calldata(base_token_address, amount_wei)
            else:
                calldata = self._build_supply_to_calldata(recipient, base_token_address, amount_wei)

            amount_str = "full debt" if repay_all else f"{amount} {base_token}"
            return TransactionResult(
                success=True,
                tx_data={
                    "to": self.comet_address,
                    "value": 0,
                    "data": calldata,
                },
                gas_estimate=DEFAULT_GAS_ESTIMATES["repay"],
                description=f"Repay {amount_str} to Compound V3 {self.market_config.get('name', self.market)}",
            )

        except Exception as e:
            logger.exception(f"Failed to build repay transaction: {e}")
            return TransactionResult(success=False, error=str(e))

    # =========================================================================
    # Market Information
    # =========================================================================

    def get_market_info(self) -> CompoundV3MarketInfo:
        """Get information about the current market.

        Returns:
            CompoundV3MarketInfo with market details
        """
        return CompoundV3MarketInfo(
            market_id=self.market,
            name=self.market_config.get("name", self.market),
            base_token=self.market_config.get("base_token", "USDC"),
            base_token_address=self.market_config.get("base_token_address", ""),
            comet_address=self.comet_address,
            collaterals=self.market_config.get("collaterals", {}),
        )

    def get_supported_collaterals(self) -> list[str]:
        """Get list of supported collateral assets for the current market.

        Returns:
            List of collateral asset symbols
        """
        return list(self.market_config.get("collaterals", {}).keys())

    def get_collateral_info(self, asset: str) -> dict[str, Any] | None:
        """Get information about a collateral asset.

        Args:
            asset: Collateral asset symbol

        Returns:
            Collateral info dictionary or None if not supported
        """
        collaterals = self.market_config.get("collaterals", {})
        if asset not in collaterals:
            return None
        info = collaterals[asset].copy()
        info["symbol"] = asset
        return info

    # =========================================================================
    # Health Factor Calculations
    # =========================================================================

    def calculate_health_factor(
        self,
        collateral_balances: dict[str, Decimal],
        borrow_balance: Decimal,
    ) -> CompoundV3HealthFactor:
        """Calculate health factor for a position.

        Args:
            collateral_balances: Dictionary of collateral asset balances
            borrow_balance: Amount of borrowed base asset

        Returns:
            CompoundV3HealthFactor with health calculation
        """
        collateral_value_usd = Decimal("0")
        borrow_capacity_usd = Decimal("0")
        liquidation_threshold_usd = Decimal("0")

        collaterals = self.market_config.get("collaterals", {})

        for asset, balance in collateral_balances.items():
            if asset not in collaterals or balance <= 0:
                continue

            price = self._price_oracle(asset)
            value_usd = balance * price
            collateral_value_usd += value_usd

            collateral_info = collaterals[asset]
            borrow_cf = collateral_info.get("borrow_collateral_factor", Decimal("0"))
            liquidation_cf = collateral_info.get("liquidation_collateral_factor", Decimal("0"))

            borrow_capacity_usd += value_usd * borrow_cf
            liquidation_threshold_usd += value_usd * liquidation_cf

        # Get base token price for borrow value
        base_token = self.market_config.get("base_token", "USDC")
        base_price = self._price_oracle(base_token)
        borrow_value_usd = borrow_balance * base_price

        # Calculate health factor
        if borrow_value_usd > 0:
            health_factor = liquidation_threshold_usd / borrow_value_usd
        else:
            health_factor = Decimal("999999")  # No debt = max health

        is_liquidatable = health_factor < Decimal("1.0") if borrow_value_usd > 0 else False

        return CompoundV3HealthFactor(
            collateral_value_usd=collateral_value_usd,
            borrow_value_usd=borrow_value_usd,
            borrow_capacity_usd=borrow_capacity_usd,
            liquidation_threshold_usd=liquidation_threshold_usd,
            health_factor=health_factor,
            is_liquidatable=is_liquidatable,
        )

    # =========================================================================
    # Approval Operations
    # =========================================================================

    def build_approve_transaction(
        self,
        token: str,
        amount: Decimal | None = None,
    ) -> TransactionResult:
        """Build an ERC20 approval transaction for the Comet contract.

        Args:
            token: Token symbol to approve
            amount: Amount to approve (None for max approval)

        Returns:
            TransactionResult with transaction data
        """
        try:
            # Get token address
            if token == self.market_config.get("base_token"):
                token_address = self.market_config.get("base_token_address")
            else:
                collaterals = self.market_config.get("collaterals", {})
                if token in collaterals:
                    token_address = collaterals[token]["address"]
                else:
                    token_address = self._resolve_token_address(token)

            if not token_address:
                return TransactionResult(
                    success=False,
                    error=f"Unknown token: {token}",
                )

            if amount is None:
                amount_wei = MAX_UINT256
            else:
                decimals = self._get_decimals(token)
                amount_wei = int(amount * Decimal(10**decimals))

            calldata = self._build_approve_calldata(self.comet_address, amount_wei)

            amount_str = "unlimited" if amount is None else f"{amount} {token}"
            return TransactionResult(
                success=True,
                tx_data={
                    "to": token_address,
                    "value": 0,
                    "data": calldata,
                },
                gas_estimate=DEFAULT_GAS_ESTIMATES["approve"],
                description=f"Approve {amount_str} for Compound V3 {self.market_config.get('name', self.market)}",
            )

        except Exception as e:
            logger.exception(f"Failed to build approve transaction: {e}")
            return TransactionResult(success=False, error=str(e))

    # =========================================================================
    # Private Helpers
    # =========================================================================

    def _resolve_token_address(self, token: str) -> str:
        """Resolve a token symbol to its address using TokenResolver.

        Args:
            token: Token symbol (e.g., "USDC")

        Returns:
            Token address

        Raises:
            TokenResolutionError: If the token cannot be resolved
        """
        try:
            resolved = self._token_resolver.resolve(token, self.chain)
            return resolved.address
        except TokenResolutionError as e:
            raise TokenResolutionError(
                token=token,
                chain=str(self.chain),
                reason=f"[CompoundV3Adapter] Cannot resolve token: {e.reason}",
                suggestions=e.suggestions,
            ) from e

    def _get_decimals(self, token: str) -> int:
        """Get decimals for a token using TokenResolver.

        Args:
            token: Token symbol

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
                reason=f"[CompoundV3Adapter] Cannot determine decimals: {e.reason}",
                suggestions=e.suggestions,
            ) from e

    def _default_price_oracle(self, token: str) -> Decimal:
        """Default price oracle (returns placeholder prices).

        In production, this should be replaced with a real price oracle.
        """
        # Placeholder prices for common tokens
        prices = {
            "USDC": Decimal("1.0"),
            "USDC.e": Decimal("1.0"),
            "USDT": Decimal("1.0"),
            "WETH": Decimal("2500.0"),
            "WBTC": Decimal("60000.0"),
            "COMP": Decimal("50.0"),
            "UNI": Decimal("10.0"),
            "LINK": Decimal("15.0"),
            "wstETH": Decimal("2800.0"),
            "cbETH": Decimal("2600.0"),
            "rETH": Decimal("2700.0"),
            "ARB": Decimal("1.5"),
            "GMX": Decimal("40.0"),
            "USDS": Decimal("1.0"),
            "sUSDe": Decimal("1.0"),
        }
        return prices.get(token, Decimal("1.0"))

    def _build_supply_calldata(self, asset: str, amount: int) -> str:
        """Build calldata for supply(address,uint256)."""
        # supply(address,uint256) = 0xf2b9fdb8
        asset_padded = asset[2:].lower().zfill(64)
        amount_hex = hex(amount)[2:].zfill(64)
        return f"{COMPOUND_V3_SUPPLY_SELECTOR}{asset_padded}{amount_hex}"

    def _build_supply_to_calldata(self, dst: str, asset: str, amount: int) -> str:
        """Build calldata for supplyTo(address,address,uint256)."""
        # supplyTo(address,address,uint256) = 0x4232cd63
        dst_padded = dst[2:].lower().zfill(64)
        asset_padded = asset[2:].lower().zfill(64)
        amount_hex = hex(amount)[2:].zfill(64)
        return f"{COMPOUND_V3_SUPPLY_TO_SELECTOR}{dst_padded}{asset_padded}{amount_hex}"

    def _build_withdraw_calldata(self, asset: str, amount: int) -> str:
        """Build calldata for withdraw(address,uint256)."""
        # withdraw(address,uint256) = 0xf3fef3a3
        asset_padded = asset[2:].lower().zfill(64)
        amount_hex = hex(amount)[2:].zfill(64)
        return f"{COMPOUND_V3_WITHDRAW_SELECTOR}{asset_padded}{amount_hex}"

    def _build_withdraw_to_calldata(self, to: str, asset: str, amount: int) -> str:
        """Build calldata for withdrawTo(address,address,uint256)."""
        # withdrawTo(address,address,uint256) = 0x8013f3a7
        to_padded = to[2:].lower().zfill(64)
        asset_padded = asset[2:].lower().zfill(64)
        amount_hex = hex(amount)[2:].zfill(64)
        return f"{COMPOUND_V3_WITHDRAW_TO_SELECTOR}{to_padded}{asset_padded}{amount_hex}"

    def _build_approve_calldata(self, spender: str, amount: int) -> str:
        """Build calldata for ERC20 approve(address,uint256)."""
        # approve(address,uint256) = 0x095ea7b3
        spender_padded = spender[2:].lower().zfill(64)
        amount_hex = hex(amount)[2:].zfill(64)
        return f"{ERC20_APPROVE_SELECTOR}{spender_padded}{amount_hex}"
