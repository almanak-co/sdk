"""Aave V3 Adapter.

This module provides an adapter for interacting with Aave V3 lending protocol,
supporting supply, borrow, repay, withdraw, flash loans, E-Mode, and isolation mode.

Aave V3 is a decentralized lending protocol supporting:
- Supply assets to earn yield
- Borrow against collateral
- Flash loans for atomic arbitrage
- Efficiency Mode (E-Mode) for correlated assets
- Isolation Mode for new assets with limited debt ceiling
- Variable and stable interest rates

Supported chains:
- Ethereum
- Arbitrum
- Optimism
- Polygon
- Base
- Avalanche

Example:
    from almanak.framework.connectors.aave_v3 import AaveV3Adapter, AaveV3Config

    config = AaveV3Config(
        chain="arbitrum",
        wallet_address="0x...",
    )
    adapter = AaveV3Adapter(config)

    # Supply collateral
    result = adapter.supply(
        asset="USDC",
        amount=Decimal("1000"),
    )

    # Borrow against collateral
    result = adapter.borrow(
        asset="ETH",
        amount=Decimal("0.5"),
    )
"""

import logging
from collections.abc import Callable
from dataclasses import dataclass, field
from decimal import Decimal
from enum import IntEnum
from typing import TYPE_CHECKING, Any

from almanak.framework.data.tokens.exceptions import TokenResolutionError

if TYPE_CHECKING:
    from almanak.framework.data.tokens.resolver import TokenResolver as TokenResolverType

from almanak.core.contracts import AAVE_V3

logger = logging.getLogger(__name__)


# =============================================================================
# Constants
# =============================================================================

# Derive per-contract-type dicts from centralized registry for backward compatibility
AAVE_V3_POOL_ADDRESSES: dict[str, str] = {chain: addrs["pool"] for chain, addrs in AAVE_V3.items()}
AAVE_V3_POOL_DATA_PROVIDER_ADDRESSES: dict[str, str] = {
    chain: addrs["pool_data_provider"] for chain, addrs in AAVE_V3.items()
}
AAVE_V3_ORACLE_ADDRESSES: dict[str, str] = {chain: addrs["oracle"] for chain, addrs in AAVE_V3.items()}

# Aave V3 function selectors
AAVE_SUPPLY_SELECTOR = "0x617ba037"
AAVE_BORROW_SELECTOR = "0xa415bcad"
AAVE_REPAY_SELECTOR = "0x573ade81"
AAVE_WITHDRAW_SELECTOR = "0x69328dec"
AAVE_SET_USER_USE_RESERVE_AS_COLLATERAL_SELECTOR = "0x5a3b74b9"
AAVE_SET_USER_EMODE_SELECTOR = "0x28530a47"
AAVE_FLASH_LOAN_SELECTOR = "0xab9c4b5d"
AAVE_FLASH_LOAN_SIMPLE_SELECTOR = "0x42b0b77c"
AAVE_LIQUIDATION_CALL_SELECTOR = "0x00a718a9"

# ERC20 approve selector
ERC20_APPROVE_SELECTOR = "0x095ea7b3"

# Max values
MAX_UINT256 = 2**256 - 1

# Interest rate modes
AAVE_STABLE_RATE_MODE = 1  # Being deprecated
AAVE_VARIABLE_RATE_MODE = 2  # Most common

# E-Mode categories (predefined by Aave governance)
EMODE_CATEGORIES: dict[str, int] = {
    "NONE": 0,
    "ETH_CORRELATED": 1,  # ETH, wstETH, cbETH, rETH
    "STABLECOINS": 2,  # USDC, USDT, DAI
}

# Gas estimates for Aave operations
DEFAULT_GAS_ESTIMATES: dict[str, int] = {
    "supply": 150000,
    "borrow": 350000,
    "repay": 150000,
    "withdraw": 150000,
    "set_collateral": 80000,
    "set_emode": 100000,
    "flash_loan": 500000,
    "flash_loan_simple": 300000,
    "liquidation_call": 400000,
    "approve": 46000,
}


# =============================================================================
# Enums
# =============================================================================


class AaveV3InterestRateMode(IntEnum):
    """Aave V3 interest rate modes."""

    STABLE = 1  # Being deprecated
    VARIABLE = 2  # Most common


class AaveV3EModeCategory(IntEnum):
    """Aave V3 E-Mode categories."""

    NONE = 0
    ETH_CORRELATED = 1
    STABLECOINS = 2


# =============================================================================
# Data Classes
# =============================================================================


@dataclass
class AaveV3Config:
    """Configuration for Aave V3 adapter.

    Attributes:
        chain: Blockchain network (ethereum, arbitrum, optimism, polygon, base, avalanche)
        wallet_address: User wallet address
        default_slippage_bps: Default slippage tolerance in basis points
        allow_placeholder_prices: If True, allows fallback to hardcoded placeholder prices.
            WARNING: NEVER set to True in production! Placeholder prices cause incorrect
            health factor calculations which can lead to unexpected liquidations.
            Only use for local testing/development without real price data.
    """

    chain: str
    wallet_address: str
    default_slippage_bps: int = 50  # 0.5%
    allow_placeholder_prices: bool = False

    def __post_init__(self) -> None:
        """Validate configuration."""
        valid_chains = set(AAVE_V3_POOL_ADDRESSES.keys())
        if self.chain not in valid_chains:
            raise ValueError(f"Invalid chain: {self.chain}. Valid chains: {valid_chains}")
        if not self.wallet_address.startswith("0x") or len(self.wallet_address) != 42:
            raise ValueError(f"Invalid wallet address: {self.wallet_address}. Must be 0x-prefixed 40 hex chars.")
        if self.default_slippage_bps < 0 or self.default_slippage_bps > 10000:
            raise ValueError(f"Invalid slippage: {self.default_slippage_bps}. Must be 0-10000 bps.")
        if self.allow_placeholder_prices:
            import warnings

            warnings.warn(
                "AaveV3Config.allow_placeholder_prices=True is UNSAFE for production. "
                "Health factor calculations will use hardcoded prices (e.g., ETH=$2000) "
                "which DO NOT reflect real market prices. This can cause liquidations.",
                UserWarning,
                stacklevel=3,
            )


@dataclass
class AaveV3ReserveData:
    """Reserve data for an Aave V3 asset.

    Attributes:
        asset: Asset symbol
        asset_address: Asset contract address
        atoken_address: aToken contract address
        stable_debt_token_address: Stable debt token address
        variable_debt_token_address: Variable debt token address
        ltv: Loan-to-Value ratio (in basis points, e.g., 8000 = 80%)
        liquidation_threshold: Liquidation threshold (in basis points)
        liquidation_bonus: Liquidation bonus (in basis points, e.g., 10500 = 5% bonus)
        reserve_factor: Reserve factor (in basis points)
        usage_as_collateral_enabled: Whether asset can be used as collateral
        borrowing_enabled: Whether asset can be borrowed
        stable_borrow_rate_enabled: Whether stable borrow rate is enabled
        is_active: Whether reserve is active
        is_frozen: Whether reserve is frozen
        is_paused: Whether reserve is paused
        supply_cap: Maximum supply (0 = unlimited)
        borrow_cap: Maximum borrow (0 = unlimited)
        debt_ceiling: Debt ceiling for isolation mode (0 = not isolated)
        emode_ltv: LTV when in E-Mode
        emode_liquidation_threshold: Liquidation threshold in E-Mode
        emode_liquidation_bonus: Liquidation bonus in E-Mode
        emode_category: E-Mode category ID
    """

    asset: str
    asset_address: str
    atoken_address: str = ""
    stable_debt_token_address: str = ""
    variable_debt_token_address: str = ""
    ltv: int = 0  # basis points
    liquidation_threshold: int = 0  # basis points
    liquidation_bonus: int = 0  # basis points
    reserve_factor: int = 0  # basis points
    usage_as_collateral_enabled: bool = True
    borrowing_enabled: bool = True
    stable_borrow_rate_enabled: bool = False
    is_active: bool = True
    is_frozen: bool = False
    is_paused: bool = False
    supply_cap: Decimal = Decimal("0")
    borrow_cap: Decimal = Decimal("0")
    debt_ceiling: Decimal = Decimal("0")  # 0 = not isolated
    emode_ltv: int = 0
    emode_liquidation_threshold: int = 0
    emode_liquidation_bonus: int = 0
    emode_category: int = 0

    @property
    def is_isolated(self) -> bool:
        """Check if asset is in isolation mode."""
        return self.debt_ceiling > 0

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "asset": self.asset,
            "asset_address": self.asset_address,
            "atoken_address": self.atoken_address,
            "stable_debt_token_address": self.stable_debt_token_address,
            "variable_debt_token_address": self.variable_debt_token_address,
            "ltv": self.ltv,
            "liquidation_threshold": self.liquidation_threshold,
            "liquidation_bonus": self.liquidation_bonus,
            "reserve_factor": self.reserve_factor,
            "usage_as_collateral_enabled": self.usage_as_collateral_enabled,
            "borrowing_enabled": self.borrowing_enabled,
            "stable_borrow_rate_enabled": self.stable_borrow_rate_enabled,
            "is_active": self.is_active,
            "is_frozen": self.is_frozen,
            "is_paused": self.is_paused,
            "supply_cap": str(self.supply_cap),
            "borrow_cap": str(self.borrow_cap),
            "debt_ceiling": str(self.debt_ceiling),
            "is_isolated": self.is_isolated,
            "emode_ltv": self.emode_ltv,
            "emode_liquidation_threshold": self.emode_liquidation_threshold,
            "emode_liquidation_bonus": self.emode_liquidation_bonus,
            "emode_category": self.emode_category,
        }


@dataclass
class AaveV3UserAccountData:
    """User account data from Aave V3.

    Attributes:
        total_collateral_base: Total collateral in base currency (USD)
        total_debt_base: Total debt in base currency (USD)
        available_borrows_base: Available borrows in base currency
        current_liquidation_threshold: Current weighted liquidation threshold
        ltv: Current weighted LTV
        health_factor: Health factor (1e18 = 1.0, < 1.0 = liquidatable)
        emode_category: Current E-Mode category
    """

    total_collateral_base: Decimal
    total_debt_base: Decimal
    available_borrows_base: Decimal
    current_liquidation_threshold: int  # basis points
    ltv: int  # basis points
    health_factor: Decimal  # 1e18 scale
    emode_category: int = 0

    @property
    def health_factor_normalized(self) -> Decimal:
        """Get health factor as a normalized value (1.0 = healthy threshold)."""
        return self.health_factor / Decimal("1000000000000000000")  # 1e18

    @property
    def is_liquidatable(self) -> bool:
        """Check if position is liquidatable."""
        return self.health_factor_normalized < Decimal("1.0")

    @property
    def distance_to_liquidation(self) -> Decimal:
        """Get distance to liquidation (0 = liquidatable, 1 = 2x health factor)."""
        hf = self.health_factor_normalized
        if hf <= 0:
            return Decimal("0")
        return (hf - Decimal("1")) / hf if hf >= 1 else Decimal("0")

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "total_collateral_base": str(self.total_collateral_base),
            "total_debt_base": str(self.total_debt_base),
            "available_borrows_base": str(self.available_borrows_base),
            "current_liquidation_threshold": self.current_liquidation_threshold,
            "ltv": self.ltv,
            "health_factor": str(self.health_factor),
            "health_factor_normalized": str(self.health_factor_normalized),
            "is_liquidatable": self.is_liquidatable,
            "distance_to_liquidation": str(self.distance_to_liquidation),
            "emode_category": self.emode_category,
        }


@dataclass
class AaveV3Position:
    """User position in a specific Aave V3 reserve.

    Attributes:
        asset: Asset symbol
        asset_address: Asset contract address
        current_atoken_balance: Current aToken balance (supplied amount + interest)
        current_stable_debt: Current stable debt
        current_variable_debt: Current variable debt
        principal_stable_debt: Principal stable debt
        scaled_variable_debt: Scaled variable debt
        stable_borrow_rate: Current stable borrow rate
        liquidity_rate: Current supply/liquidity rate
        usage_as_collateral_enabled: Whether using as collateral
        is_collateral: Alias for usage_as_collateral_enabled
    """

    asset: str
    asset_address: str
    current_atoken_balance: Decimal = Decimal("0")
    current_stable_debt: Decimal = Decimal("0")
    current_variable_debt: Decimal = Decimal("0")
    principal_stable_debt: Decimal = Decimal("0")
    scaled_variable_debt: Decimal = Decimal("0")
    stable_borrow_rate: Decimal = Decimal("0")
    liquidity_rate: Decimal = Decimal("0")
    usage_as_collateral_enabled: bool = False

    @property
    def is_collateral(self) -> bool:
        """Check if position is being used as collateral."""
        return self.usage_as_collateral_enabled

    @property
    def total_debt(self) -> Decimal:
        """Get total debt (stable + variable)."""
        return self.current_stable_debt + self.current_variable_debt

    @property
    def has_supply(self) -> bool:
        """Check if user has supply in this reserve."""
        return self.current_atoken_balance > 0

    @property
    def has_debt(self) -> bool:
        """Check if user has debt in this reserve."""
        return self.total_debt > 0

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "asset": self.asset,
            "asset_address": self.asset_address,
            "current_atoken_balance": str(self.current_atoken_balance),
            "current_stable_debt": str(self.current_stable_debt),
            "current_variable_debt": str(self.current_variable_debt),
            "principal_stable_debt": str(self.principal_stable_debt),
            "scaled_variable_debt": str(self.scaled_variable_debt),
            "stable_borrow_rate": str(self.stable_borrow_rate),
            "liquidity_rate": str(self.liquidity_rate),
            "usage_as_collateral_enabled": self.usage_as_collateral_enabled,
            "is_collateral": self.is_collateral,
            "total_debt": str(self.total_debt),
            "has_supply": self.has_supply,
            "has_debt": self.has_debt,
        }


@dataclass
class AaveV3FlashLoanParams:
    """Parameters for an Aave V3 flash loan.

    Attributes:
        assets: List of asset addresses to borrow
        amounts: List of amounts to borrow (in token units)
        modes: Interest rate modes (0 = no debt, 1 = stable, 2 = variable)
        on_behalf_of: Address to receive debt (if modes != 0)
        params: Extra params to pass to receiver contract
        referral_code: Referral code (usually 0)
    """

    assets: list[str]
    amounts: list[Decimal]
    modes: list[int]  # 0 = no debt (must repay), 1 = stable, 2 = variable
    on_behalf_of: str
    params: bytes = field(default_factory=bytes)
    referral_code: int = 0

    def __post_init__(self) -> None:
        """Validate flash loan parameters."""
        if len(self.assets) != len(self.amounts):
            raise ValueError("Assets and amounts must have same length")
        if len(self.assets) != len(self.modes):
            raise ValueError("Assets and modes must have same length")
        for mode in self.modes:
            if mode not in (0, 1, 2):
                raise ValueError(f"Invalid mode: {mode}. Must be 0, 1, or 2.")


@dataclass
class AaveV3HealthFactorCalculation:
    """Health factor calculation details.

    Attributes:
        total_collateral_usd: Total collateral value in USD
        total_debt_usd: Total debt value in USD
        weighted_liquidation_threshold: Weighted average liquidation threshold
        health_factor: Calculated health factor
        liquidation_price: Price at which position becomes liquidatable
        assets_breakdown: Per-asset breakdown
    """

    total_collateral_usd: Decimal
    total_debt_usd: Decimal
    weighted_liquidation_threshold: Decimal
    health_factor: Decimal
    liquidation_price: Decimal | None = None
    assets_breakdown: dict[str, dict[str, Any]] = field(default_factory=dict)

    @property
    def is_healthy(self) -> bool:
        """Check if position is healthy (HF >= 1)."""
        return self.health_factor >= Decimal("1.0")

    @property
    def buffer_to_liquidation(self) -> Decimal:
        """Get buffer to liquidation as percentage."""
        if self.health_factor <= 0:
            return Decimal("0")
        return (self.health_factor - Decimal("1")) * 100

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "total_collateral_usd": str(self.total_collateral_usd),
            "total_debt_usd": str(self.total_debt_usd),
            "weighted_liquidation_threshold": str(self.weighted_liquidation_threshold),
            "health_factor": str(self.health_factor),
            "is_healthy": self.is_healthy,
            "buffer_to_liquidation_percent": str(self.buffer_to_liquidation),
            "liquidation_price": str(self.liquidation_price) if self.liquidation_price else None,
            "assets_breakdown": self.assets_breakdown,
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


class AaveV3Adapter:
    """Adapter for Aave V3 lending protocol.

    This adapter provides methods for interacting with Aave V3:
    - Supply/withdraw collateral
    - Borrow/repay assets
    - Flash loans
    - E-Mode configuration
    - Health factor calculations
    - Liquidation price calculations

    Example:
        config = AaveV3Config(
            chain="arbitrum",
            wallet_address="0x...",
        )
        adapter = AaveV3Adapter(config)

        # Supply USDC as collateral
        result = adapter.supply("USDC", Decimal("1000"))

        # Borrow ETH
        result = adapter.borrow("WETH", Decimal("0.5"))

        # Check health factor
        hf = adapter.calculate_health_factor(positions, prices)
    """

    def __init__(
        self,
        config: AaveV3Config,
        price_oracle: PriceOracle | None = None,
        token_resolver: "TokenResolverType | None" = None,
    ) -> None:
        """Initialize the adapter.

        Args:
            config: Adapter configuration
            price_oracle: Price oracle callback. REQUIRED for production use.
                If not provided and allow_placeholder_prices=False, health factor
                calculations will raise an error. Pass a real price oracle from
                your MarketSnapshot or PriceAggregator.
            token_resolver: Optional TokenResolver instance. If None, uses singleton.

        Raises:
            ValueError: If no price_oracle is provided and allow_placeholder_prices=False
        """
        self.config = config
        self.chain = config.chain
        self.wallet_address = config.wallet_address

        # Contract addresses
        self.pool_address = AAVE_V3_POOL_ADDRESSES[config.chain]
        self.pool_data_provider_address = AAVE_V3_POOL_DATA_PROVIDER_ADDRESSES[config.chain]
        self.oracle_address = AAVE_V3_ORACLE_ADDRESSES[config.chain]

        # TokenResolver integration
        if token_resolver is not None:
            self._token_resolver = token_resolver
        else:
            from almanak.framework.data.tokens.resolver import get_token_resolver

            self._token_resolver = get_token_resolver()

        # Price oracle - track if using real prices
        self._using_placeholder_prices = price_oracle is None
        if price_oracle is not None:
            self._price_oracle = price_oracle
        elif config.allow_placeholder_prices:
            logger.warning(
                "AaveV3Adapter using PLACEHOLDER PRICES for chain=%s. "
                "Health factor calculations WILL BE INACCURATE. "
                "Only use for testing/development!",
                config.chain,
            )
            self._price_oracle = self._default_price_oracle
        else:
            # No oracle provided and placeholders not allowed
            raise ValueError(
                "AaveV3Adapter requires a price_oracle for production use. "
                "Health factor calculations need real market prices to prevent liquidations. "
                "Options:\n"
                "  1. Pass a price_oracle callback: adapter = AaveV3Adapter(config, price_oracle=my_oracle)\n"
                "  2. Use create_adapter_with_prices() helper: adapter = create_adapter_with_prices(config, prices_dict)\n"
                "  3. For testing ONLY, set allow_placeholder_prices=True in config (UNSAFE FOR PRODUCTION)"
            )

        # Reserve data cache
        self._reserve_data_cache: dict[str, AaveV3ReserveData] = {}

        # Log initialization with price source info
        price_source = "real_oracle" if not self._using_placeholder_prices else "PLACEHOLDER"
        logger.info(
            f"AaveV3Adapter initialized for chain={config.chain}, "
            f"wallet={config.wallet_address[:10]}..., price_source={price_source}"
        )

    # =========================================================================
    # Supply Operations
    # =========================================================================

    def supply(
        self,
        asset: str,
        amount: Decimal,
        on_behalf_of: str | None = None,
    ) -> TransactionResult:
        """Build a supply transaction.

        Args:
            asset: Asset symbol or address
            amount: Amount to supply (in token units)
            on_behalf_of: Address to credit (defaults to wallet_address)

        Returns:
            TransactionResult with transaction data
        """
        try:
            asset_address = self._resolve_asset(asset)
            if asset_address is None:
                return TransactionResult(
                    success=False,
                    error=f"Unknown asset: {asset}",
                )

            decimals = self._get_decimals(asset)
            amount_wei = int(amount * Decimal(10**decimals))
            recipient = on_behalf_of or self.wallet_address

            # Build calldata: supply(address asset, uint256 amount, address onBehalfOf, uint16 referralCode)
            calldata = (
                AAVE_SUPPLY_SELECTOR
                + self._pad_address(asset_address)
                + self._pad_uint256(amount_wei)
                + self._pad_address(recipient)
                + self._pad_uint16(0)  # referral code
            )

            return TransactionResult(
                success=True,
                tx_data={
                    "to": self.pool_address,
                    "value": 0,
                    "data": calldata,
                },
                gas_estimate=DEFAULT_GAS_ESTIMATES["supply"],
                description=f"Supply {amount} {asset} to Aave V3",
            )

        except Exception as e:
            logger.exception(f"Failed to build supply transaction: {e}")
            return TransactionResult(success=False, error=str(e))

    def withdraw(
        self,
        asset: str,
        amount: Decimal,
        to: str | None = None,
        withdraw_all: bool = False,
    ) -> TransactionResult:
        """Build a withdraw transaction.

        Args:
            asset: Asset symbol or address
            amount: Amount to withdraw (in token units)
            to: Address to receive tokens (defaults to wallet_address)
            withdraw_all: If True, use MAX_UINT256 to withdraw all

        Returns:
            TransactionResult with transaction data
        """
        try:
            asset_address = self._resolve_asset(asset)
            if asset_address is None:
                return TransactionResult(
                    success=False,
                    error=f"Unknown asset: {asset}",
                )

            decimals = self._get_decimals(asset)
            amount_wei = MAX_UINT256 if withdraw_all else int(amount * Decimal(10**decimals))
            recipient = to or self.wallet_address

            # Build calldata: withdraw(address asset, uint256 amount, address to)
            calldata = (
                AAVE_WITHDRAW_SELECTOR
                + self._pad_address(asset_address)
                + self._pad_uint256(amount_wei)
                + self._pad_address(recipient)
            )

            amount_desc = "all" if withdraw_all else str(amount)
            return TransactionResult(
                success=True,
                tx_data={
                    "to": self.pool_address,
                    "value": 0,
                    "data": calldata,
                },
                gas_estimate=DEFAULT_GAS_ESTIMATES["withdraw"],
                description=f"Withdraw {amount_desc} {asset} from Aave V3",
            )

        except Exception as e:
            logger.exception(f"Failed to build withdraw transaction: {e}")
            return TransactionResult(success=False, error=str(e))

    # =========================================================================
    # Borrow Operations
    # =========================================================================

    def borrow(
        self,
        asset: str,
        amount: Decimal,
        interest_rate_mode: AaveV3InterestRateMode = AaveV3InterestRateMode.VARIABLE,
        on_behalf_of: str | None = None,
    ) -> TransactionResult:
        """Build a borrow transaction.

        Args:
            asset: Asset symbol or address
            amount: Amount to borrow (in token units)
            interest_rate_mode: Interest rate mode (STABLE or VARIABLE)
            on_behalf_of: Address to debit (defaults to wallet_address)

        Returns:
            TransactionResult with transaction data
        """
        try:
            asset_address = self._resolve_asset(asset)
            if asset_address is None:
                return TransactionResult(
                    success=False,
                    error=f"Unknown asset: {asset}",
                )

            decimals = self._get_decimals(asset)
            amount_wei = int(amount * Decimal(10**decimals))
            recipient = on_behalf_of or self.wallet_address

            # Build calldata: borrow(address asset, uint256 amount, uint256 interestRateMode, uint16 referralCode, address onBehalfOf)
            calldata = (
                AAVE_BORROW_SELECTOR
                + self._pad_address(asset_address)
                + self._pad_uint256(amount_wei)
                + self._pad_uint256(int(interest_rate_mode))
                + self._pad_uint16(0)  # referral code
                + self._pad_address(recipient)
            )

            rate_mode_str = "variable" if interest_rate_mode == AaveV3InterestRateMode.VARIABLE else "stable"
            return TransactionResult(
                success=True,
                tx_data={
                    "to": self.pool_address,
                    "value": 0,
                    "data": calldata,
                },
                gas_estimate=DEFAULT_GAS_ESTIMATES["borrow"],
                description=f"Borrow {amount} {asset} from Aave V3 ({rate_mode_str} rate)",
            )

        except Exception as e:
            logger.exception(f"Failed to build borrow transaction: {e}")
            return TransactionResult(success=False, error=str(e))

    def repay(
        self,
        asset: str,
        amount: Decimal,
        interest_rate_mode: AaveV3InterestRateMode = AaveV3InterestRateMode.VARIABLE,
        on_behalf_of: str | None = None,
        repay_all: bool = False,
    ) -> TransactionResult:
        """Build a repay transaction.

        Args:
            asset: Asset symbol or address
            amount: Amount to repay (in token units)
            interest_rate_mode: Interest rate mode of debt to repay
            on_behalf_of: Address with debt (defaults to wallet_address)
            repay_all: If True, use MAX_UINT256 to repay full debt

        Returns:
            TransactionResult with transaction data
        """
        try:
            asset_address = self._resolve_asset(asset)
            if asset_address is None:
                return TransactionResult(
                    success=False,
                    error=f"Unknown asset: {asset}",
                )

            decimals = self._get_decimals(asset)
            amount_wei = MAX_UINT256 if repay_all else int(amount * Decimal(10**decimals))
            recipient = on_behalf_of or self.wallet_address

            # Build calldata: repay(address asset, uint256 amount, uint256 interestRateMode, address onBehalfOf)
            calldata = (
                AAVE_REPAY_SELECTOR
                + self._pad_address(asset_address)
                + self._pad_uint256(amount_wei)
                + self._pad_uint256(int(interest_rate_mode))
                + self._pad_address(recipient)
            )

            amount_desc = "full debt" if repay_all else str(amount)
            return TransactionResult(
                success=True,
                tx_data={
                    "to": self.pool_address,
                    "value": 0,
                    "data": calldata,
                },
                gas_estimate=DEFAULT_GAS_ESTIMATES["repay"],
                description=f"Repay {amount_desc} {asset} to Aave V3",
            )

        except Exception as e:
            logger.exception(f"Failed to build repay transaction: {e}")
            return TransactionResult(success=False, error=str(e))

    # =========================================================================
    # Collateral Operations
    # =========================================================================

    def set_user_use_reserve_as_collateral(
        self,
        asset: str,
        use_as_collateral: bool,
    ) -> TransactionResult:
        """Build a transaction to enable/disable asset as collateral.

        Args:
            asset: Asset symbol or address
            use_as_collateral: Whether to use as collateral

        Returns:
            TransactionResult with transaction data
        """
        try:
            asset_address = self._resolve_asset(asset)
            if asset_address is None:
                return TransactionResult(
                    success=False,
                    error=f"Unknown asset: {asset}",
                )

            # Build calldata: setUserUseReserveAsCollateral(address asset, bool useAsCollateral)
            calldata = (
                AAVE_SET_USER_USE_RESERVE_AS_COLLATERAL_SELECTOR
                + self._pad_address(asset_address)
                + self._pad_uint256(1 if use_as_collateral else 0)
            )

            action = "enable" if use_as_collateral else "disable"
            return TransactionResult(
                success=True,
                tx_data={
                    "to": self.pool_address,
                    "value": 0,
                    "data": calldata,
                },
                gas_estimate=DEFAULT_GAS_ESTIMATES["set_collateral"],
                description=f"{action.capitalize()} {asset} as collateral",
            )

        except Exception as e:
            logger.exception(f"Failed to build set collateral transaction: {e}")
            return TransactionResult(success=False, error=str(e))

    # =========================================================================
    # E-Mode Operations
    # =========================================================================

    def set_user_emode(
        self,
        category_id: int,
    ) -> TransactionResult:
        """Build a transaction to set user E-Mode category.

        E-Mode (Efficiency Mode) allows higher LTV for correlated assets.

        Categories:
        - 0: None (normal mode)
        - 1: ETH correlated (ETH, wstETH, cbETH, rETH)
        - 2: Stablecoins (USDC, USDT, DAI)

        Args:
            category_id: E-Mode category ID

        Returns:
            TransactionResult with transaction data
        """
        try:
            # Build calldata: setUserEMode(uint8 categoryId)
            calldata = AAVE_SET_USER_EMODE_SELECTOR + self._pad_uint256(category_id)

            category_names = {0: "None", 1: "ETH Correlated", 2: "Stablecoins"}
            category_name = category_names.get(category_id, f"Category {category_id}")

            return TransactionResult(
                success=True,
                tx_data={
                    "to": self.pool_address,
                    "value": 0,
                    "data": calldata,
                },
                gas_estimate=DEFAULT_GAS_ESTIMATES["set_emode"],
                description=f"Set E-Mode to {category_name}",
            )

        except Exception as e:
            logger.exception(f"Failed to build set E-Mode transaction: {e}")
            return TransactionResult(success=False, error=str(e))

    def get_emode_category_data(
        self,
        category_id: int,
    ) -> dict[str, Any]:
        """Get E-Mode category configuration.

        Args:
            category_id: E-Mode category ID

        Returns:
            Dictionary with E-Mode category data
        """
        # Typical E-Mode configurations (would be fetched from contract in production)
        emode_configs: dict[int, dict[str, Any]] = {
            0: {
                "id": 0,
                "label": "None",
                "ltv": 0,
                "liquidation_threshold": 0,
                "liquidation_bonus": 0,
                "oracle_address": "0x0000000000000000000000000000000000000000",
            },
            1: {
                "id": 1,
                "label": "ETH correlated",
                "ltv": 9300,  # 93%
                "liquidation_threshold": 9500,  # 95%
                "liquidation_bonus": 10100,  # 1% bonus
                "oracle_address": "0x0000000000000000000000000000000000000000",
            },
            2: {
                "id": 2,
                "label": "Stablecoins",
                "ltv": 9700,  # 97%
                "liquidation_threshold": 9750,  # 97.5%
                "liquidation_bonus": 10100,  # 1% bonus
                "oracle_address": "0x0000000000000000000000000000000000000000",
            },
        }

        return emode_configs.get(category_id, emode_configs[0])

    # =========================================================================
    # Flash Loan Operations
    # =========================================================================

    def flash_loan(
        self,
        receiver_address: str,
        assets: list[str],
        amounts: list[Decimal],
        modes: list[int],
        on_behalf_of: str | None = None,
        params: bytes = b"",
    ) -> TransactionResult:
        """Build a flash loan transaction.

        Flash loans allow borrowing assets without collateral, provided they
        are returned (plus premium) within the same transaction.

        Modes:
        - 0: No open debt (must repay within same transaction)
        - 1: Open stable rate debt
        - 2: Open variable rate debt

        Args:
            receiver_address: Contract that will receive the flash loan
            assets: List of asset symbols or addresses
            amounts: List of amounts to borrow
            modes: List of debt modes (0, 1, or 2)
            on_behalf_of: Address to receive debt if mode != 0
            params: Extra data to pass to receiver

        Returns:
            TransactionResult with transaction data
        """
        try:
            if len(assets) != len(amounts) or len(assets) != len(modes):
                return TransactionResult(
                    success=False,
                    error="Assets, amounts, and modes must have same length",
                )

            # Resolve asset addresses
            asset_addresses: list[str] = []
            amounts_wei: list[int] = []

            for i, asset in enumerate(assets):
                address = self._resolve_asset(asset)
                if address is None:
                    return TransactionResult(
                        success=False,
                        error=f"Unknown asset: {asset}",
                    )
                asset_addresses.append(address)

                decimals = self._get_decimals(asset)
                amounts_wei.append(int(amounts[i] * Decimal(10**decimals)))

            recipient = on_behalf_of or self.wallet_address

            # Build calldata for flashLoan
            # flashLoan(
            #   address receiverAddress,
            #   address[] calldata assets,
            #   uint256[] calldata amounts,
            #   uint256[] calldata modes,
            #   address onBehalfOf,
            #   bytes calldata params,
            #   uint16 referralCode
            # )

            # Encode dynamic arrays
            # ABI encoding for dynamic arrays is complex - simplified here
            # In production, would use proper ABI encoding library

            # Calculate offsets for dynamic data
            # Fixed params: receiverAddress(32) + arrays offset(32*3) + onBehalfOf(32) + params offset(32) + referralCode(32)
            # Arrays: each array needs length(32) + data(32*n)

            n_assets = len(asset_addresses)

            # Build ABI-encoded params manually
            # This is a simplified version - production would use eth-abi

            # Header: receiver address
            encoded = self._pad_address(receiver_address)

            # Offsets for arrays (each 32 bytes, pointing to data location)
            # assets array offset (after fixed params)
            assets_offset = 7 * 32  # 7 fixed-size params before arrays
            encoded += self._pad_uint256(assets_offset)

            # amounts array offset
            amounts_offset = assets_offset + 32 + n_assets * 32  # length + data
            encoded += self._pad_uint256(amounts_offset)

            # modes array offset
            modes_offset = amounts_offset + 32 + n_assets * 32
            encoded += self._pad_uint256(modes_offset)

            # onBehalfOf
            encoded += self._pad_address(recipient)

            # params offset
            params_offset = modes_offset + 32 + n_assets * 32
            encoded += self._pad_uint256(params_offset)

            # referralCode
            encoded += self._pad_uint16(0)

            # Now encode the arrays

            # assets array: length + addresses
            encoded += self._pad_uint256(n_assets)
            for addr in asset_addresses:
                encoded += self._pad_address(addr)

            # amounts array: length + amounts
            encoded += self._pad_uint256(n_assets)
            for amount_val in amounts_wei:
                encoded += self._pad_uint256(amount_val)

            # modes array: length + modes
            encoded += self._pad_uint256(n_assets)
            for mode in modes:
                encoded += self._pad_uint256(mode)

            # params: length + data (padded to 32 bytes)
            params_hex = params.hex() if params else ""
            params_len = len(params)
            encoded += self._pad_uint256(params_len)
            if params_len > 0:
                # Pad params to 32-byte boundary
                padded_params = params_hex + "0" * ((64 - len(params_hex) % 64) % 64)
                encoded += padded_params

            calldata = AAVE_FLASH_LOAN_SELECTOR + encoded

            assets_str = ", ".join(f"{amounts[i]} {assets[i]}" for i in range(len(assets)))
            return TransactionResult(
                success=True,
                tx_data={
                    "to": self.pool_address,
                    "value": 0,
                    "data": calldata,
                },
                gas_estimate=DEFAULT_GAS_ESTIMATES["flash_loan"],
                description=f"Flash loan: {assets_str}",
            )

        except Exception as e:
            logger.exception(f"Failed to build flash loan transaction: {e}")
            return TransactionResult(success=False, error=str(e))

    def flash_loan_simple(
        self,
        receiver_address: str,
        asset: str,
        amount: Decimal,
        params: bytes = b"",
    ) -> TransactionResult:
        """Build a simple flash loan transaction (single asset, no debt).

        This is a simplified flash loan for a single asset that must be
        repaid within the same transaction.

        Args:
            receiver_address: Contract that will receive the flash loan
            asset: Asset symbol or address
            amount: Amount to borrow
            params: Extra data to pass to receiver

        Returns:
            TransactionResult with transaction data
        """
        try:
            asset_address = self._resolve_asset(asset)
            if asset_address is None:
                return TransactionResult(
                    success=False,
                    error=f"Unknown asset: {asset}",
                )

            decimals = self._get_decimals(asset)
            amount_wei = int(amount * Decimal(10**decimals))

            # Build calldata for flashLoanSimple
            # flashLoanSimple(
            #   address receiverAddress,
            #   address asset,
            #   uint256 amount,
            #   bytes calldata params,
            #   uint16 referralCode
            # )

            # Calculate params offset (after fixed params)
            params_offset = 5 * 32  # 5 fixed params before dynamic params

            # Encode params data
            params_hex = params.hex() if params else ""
            params_len = len(params)

            encoded = (
                self._pad_address(receiver_address)
                + self._pad_address(asset_address)
                + self._pad_uint256(amount_wei)
                + self._pad_uint256(params_offset)
                + self._pad_uint16(0)  # referral code
                + self._pad_uint256(params_len)
            )

            if params_len > 0:
                padded_params = params_hex + "0" * ((64 - len(params_hex) % 64) % 64)
                encoded += padded_params

            calldata = AAVE_FLASH_LOAN_SIMPLE_SELECTOR + encoded

            return TransactionResult(
                success=True,
                tx_data={
                    "to": self.pool_address,
                    "value": 0,
                    "data": calldata,
                },
                gas_estimate=DEFAULT_GAS_ESTIMATES["flash_loan_simple"],
                description=f"Simple flash loan: {amount} {asset}",
            )

        except Exception as e:
            logger.exception(f"Failed to build simple flash loan transaction: {e}")
            return TransactionResult(success=False, error=str(e))

    # =========================================================================
    # Liquidation Operations
    # =========================================================================

    def liquidation_call(
        self,
        collateral_asset: str,
        debt_asset: str,
        user: str,
        debt_to_cover: Decimal,
        receive_atoken: bool = False,
    ) -> TransactionResult:
        """Build a liquidation transaction.

        Liquidation allows repaying another user's debt in exchange for
        their collateral at a discount (liquidation bonus).

        Args:
            collateral_asset: Asset to receive as collateral
            debt_asset: Asset to repay
            user: Address of user to liquidate
            debt_to_cover: Amount of debt to repay
            receive_atoken: If True, receive aTokens instead of underlying

        Returns:
            TransactionResult with transaction data
        """
        try:
            collateral_address = self._resolve_asset(collateral_asset)
            debt_address = self._resolve_asset(debt_asset)

            if collateral_address is None:
                return TransactionResult(
                    success=False,
                    error=f"Unknown collateral asset: {collateral_asset}",
                )
            if debt_address is None:
                return TransactionResult(
                    success=False,
                    error=f"Unknown debt asset: {debt_asset}",
                )

            decimals = self._get_decimals(debt_asset)
            debt_to_cover_wei = int(debt_to_cover * Decimal(10**decimals))

            # Build calldata: liquidationCall(
            #   address collateralAsset,
            #   address debtAsset,
            #   address user,
            #   uint256 debtToCover,
            #   bool receiveAToken
            # )
            calldata = (
                AAVE_LIQUIDATION_CALL_SELECTOR
                + self._pad_address(collateral_address)
                + self._pad_address(debt_address)
                + self._pad_address(user)
                + self._pad_uint256(debt_to_cover_wei)
                + self._pad_uint256(1 if receive_atoken else 0)
            )

            return TransactionResult(
                success=True,
                tx_data={
                    "to": self.pool_address,
                    "value": 0,
                    "data": calldata,
                },
                gas_estimate=DEFAULT_GAS_ESTIMATES["liquidation_call"],
                description=f"Liquidate {user[:10]}...: repay {debt_to_cover} {debt_asset}, receive {collateral_asset}",
            )

        except Exception as e:
            logger.exception(f"Failed to build liquidation transaction: {e}")
            return TransactionResult(success=False, error=str(e))

    # =========================================================================
    # Health Factor Calculations
    # =========================================================================

    def calculate_health_factor(
        self,
        positions: list[AaveV3Position],
        reserve_data: dict[str, AaveV3ReserveData],
        prices: dict[str, Decimal] | None = None,
        emode_category: int = 0,
    ) -> AaveV3HealthFactorCalculation:
        """Calculate health factor from positions.

        Health Factor = (Sum of Collateral * Liquidation Threshold) / Total Debt

        A health factor < 1.0 means the position can be liquidated.

        Args:
            positions: List of user positions
            reserve_data: Reserve data for each asset
            prices: Asset prices in USD (uses oracle if not provided)
            emode_category: User's E-Mode category (affects LT calculations)

        Returns:
            AaveV3HealthFactorCalculation with detailed breakdown
        """
        prices = prices or {}
        assets_breakdown: dict[str, dict[str, Any]] = {}

        total_collateral_usd = Decimal("0")
        total_debt_usd = Decimal("0")
        weighted_lt_sum = Decimal("0")

        for position in positions:
            asset = position.asset
            price = prices.get(asset) or self._price_oracle(asset)

            # Skip if no price data
            if price <= 0:
                logger.warning(f"No price data for {asset}, skipping")
                continue

            reserve = reserve_data.get(asset)
            if reserve is None:
                logger.warning(f"No reserve data for {asset}, using defaults")
                reserve = AaveV3ReserveData(asset=asset, asset_address=position.asset_address)

            # Calculate collateral value
            collateral_value = position.current_atoken_balance * price
            debt_value = position.total_debt * price

            # Get liquidation threshold (use E-Mode if applicable)
            lt = reserve.liquidation_threshold
            if emode_category > 0 and reserve.emode_category == emode_category:
                lt = reserve.emode_liquidation_threshold or lt

            lt_decimal = Decimal(lt) / Decimal("10000")  # Convert from bps

            # Only count as collateral if enabled
            if position.usage_as_collateral_enabled and collateral_value > 0:
                total_collateral_usd += collateral_value
                weighted_lt_sum += collateral_value * lt_decimal

            total_debt_usd += debt_value

            assets_breakdown[asset] = {
                "collateral_balance": str(position.current_atoken_balance),
                "collateral_value_usd": str(collateral_value),
                "debt_balance": str(position.total_debt),
                "debt_value_usd": str(debt_value),
                "price_usd": str(price),
                "liquidation_threshold_bps": lt,
                "is_collateral": position.usage_as_collateral_enabled,
            }

        # Calculate weighted liquidation threshold
        weighted_lt = weighted_lt_sum / total_collateral_usd if total_collateral_usd > 0 else Decimal("0")

        # Calculate health factor
        # HF = (Collateral * Weighted LT) / Debt
        if total_debt_usd > 0:
            health_factor = (total_collateral_usd * weighted_lt) / total_debt_usd
        else:
            # No debt = infinite health factor (use very large number)
            health_factor = Decimal("999999")

        # Calculate liquidation price (for single-collateral positions)
        liquidation_price = None
        if len(positions) == 1 and positions[0].has_supply and total_debt_usd > 0:
            position = positions[0]
            if position.current_atoken_balance > 0:
                # Liquidation price = Debt / (Collateral Amount * LT)
                reserve = reserve_data.get(position.asset)
                lt_bps: int = reserve.liquidation_threshold if reserve else 8000
                lt_decimal = Decimal(lt_bps) / Decimal("10000")
                liquidation_price = total_debt_usd / (position.current_atoken_balance * lt_decimal)

        return AaveV3HealthFactorCalculation(
            total_collateral_usd=total_collateral_usd,
            total_debt_usd=total_debt_usd,
            weighted_liquidation_threshold=weighted_lt,
            health_factor=health_factor,
            liquidation_price=liquidation_price,
            assets_breakdown=assets_breakdown,
        )

    def calculate_liquidation_price(
        self,
        collateral_asset: str,
        collateral_amount: Decimal,
        debt_usd: Decimal,
        liquidation_threshold_bps: int,
    ) -> Decimal:
        """Calculate the price at which a position becomes liquidatable.

        Args:
            collateral_asset: Collateral asset symbol
            collateral_amount: Amount of collateral (in token units)
            debt_usd: Total debt in USD
            liquidation_threshold_bps: Liquidation threshold in basis points

        Returns:
            Price at which position becomes liquidatable
        """
        if collateral_amount <= 0 or debt_usd <= 0:
            return Decimal("0")

        lt = Decimal(liquidation_threshold_bps) / Decimal("10000")

        # Liquidation occurs when: Collateral * Price * LT = Debt
        # So: Price = Debt / (Collateral * LT)
        return debt_usd / (collateral_amount * lt)

    def calculate_max_borrow(
        self,
        collateral_value_usd: Decimal,
        current_debt_usd: Decimal,
        ltv_bps: int,
    ) -> Decimal:
        """Calculate maximum additional borrow amount.

        Args:
            collateral_value_usd: Total collateral value in USD
            current_debt_usd: Current debt in USD
            ltv_bps: Loan-to-Value ratio in basis points

        Returns:
            Maximum additional borrow amount in USD
        """
        ltv = Decimal(ltv_bps) / Decimal("10000")
        max_borrow = collateral_value_usd * ltv
        available = max_borrow - current_debt_usd
        return max(Decimal("0"), available)

    def calculate_health_factor_after_borrow(
        self,
        current_hf_calc: AaveV3HealthFactorCalculation,
        borrow_amount_usd: Decimal,
    ) -> Decimal:
        """Calculate health factor after a hypothetical borrow.

        Args:
            current_hf_calc: Current health factor calculation
            borrow_amount_usd: Amount to borrow in USD

        Returns:
            Projected health factor after borrow
        """
        new_debt = current_hf_calc.total_debt_usd + borrow_amount_usd
        if new_debt <= 0:
            return Decimal("999999")

        return (current_hf_calc.total_collateral_usd * current_hf_calc.weighted_liquidation_threshold) / new_debt

    # =========================================================================
    # Isolation Mode Support
    # =========================================================================

    def get_isolation_mode_debt_ceiling(
        self,
        asset: str,
    ) -> Decimal:
        """Get debt ceiling for an isolated asset.

        Assets in isolation mode have a debt ceiling limiting total borrows.

        Args:
            asset: Asset symbol

        Returns:
            Debt ceiling in USD (0 if not isolated)
        """
        reserve = self._reserve_data_cache.get(asset)
        if reserve and reserve.is_isolated:
            return reserve.debt_ceiling
        return Decimal("0")

    def is_asset_isolated(self, asset: str) -> bool:
        """Check if asset is in isolation mode.

        Args:
            asset: Asset symbol

        Returns:
            True if asset is isolated
        """
        reserve = self._reserve_data_cache.get(asset)
        return reserve.is_isolated if reserve else False

    # =========================================================================
    # Utility Methods
    # =========================================================================

    def build_approve_tx(
        self,
        asset: str,
        amount: Decimal | None = None,
    ) -> TransactionResult:
        """Build an ERC20 approve transaction for Aave Pool.

        Args:
            asset: Asset symbol or address
            amount: Amount to approve (defaults to MAX_UINT256)

        Returns:
            TransactionResult with transaction data
        """
        try:
            asset_address = self._resolve_asset(asset)
            if asset_address is None:
                return TransactionResult(
                    success=False,
                    error=f"Unknown asset: {asset}",
                )

            if amount is not None:
                decimals = self._get_decimals(asset)
                amount_wei = int(amount * Decimal(10**decimals))
            else:
                amount_wei = MAX_UINT256

            # Build calldata: approve(address spender, uint256 amount)
            calldata = ERC20_APPROVE_SELECTOR + self._pad_address(self.pool_address) + self._pad_uint256(amount_wei)

            amount_desc = "unlimited" if amount is None else str(amount)
            return TransactionResult(
                success=True,
                tx_data={
                    "to": asset_address,
                    "value": 0,
                    "data": calldata,
                },
                gas_estimate=DEFAULT_GAS_ESTIMATES["approve"],
                description=f"Approve Aave Pool to spend {amount_desc} {asset}",
            )

        except Exception as e:
            logger.exception(f"Failed to build approve transaction: {e}")
            return TransactionResult(success=False, error=str(e))

    def get_reserve_data(self, asset: str) -> AaveV3ReserveData | None:
        """Get reserve data for an asset.

        Args:
            asset: Asset symbol

        Returns:
            Reserve data or None
        """
        return self._reserve_data_cache.get(asset)

    def set_reserve_data(self, asset: str, data: AaveV3ReserveData) -> None:
        """Set reserve data for an asset (for testing/mocking).

        Args:
            asset: Asset symbol
            data: Reserve data
        """
        self._reserve_data_cache[asset] = data

    def _resolve_asset(self, asset: str) -> str:
        """Resolve asset symbol to address using TokenResolver.

        Args:
            asset: Asset symbol or address

        Returns:
            Asset address

        Raises:
            TokenResolutionError: If the asset cannot be resolved
        """
        if asset.startswith("0x") and len(asset) == 42:
            return asset
        try:
            resolved = self._token_resolver.resolve(asset, self.chain)
            return resolved.address
        except TokenResolutionError as e:
            raise TokenResolutionError(
                token=asset,
                chain=str(self.chain),
                reason=f"[AaveV3Adapter] Cannot resolve asset: {e.reason}",
                suggestions=e.suggestions,
            ) from e

    def _get_decimals(self, asset: str) -> int:
        """Get decimals for an asset using TokenResolver.

        Args:
            asset: Asset symbol

        Returns:
            Number of decimals

        Raises:
            TokenResolutionError: If decimals cannot be determined
        """
        try:
            resolved = self._token_resolver.resolve(asset, self.chain)
            return resolved.decimals
        except TokenResolutionError as e:
            raise TokenResolutionError(
                token=asset,
                chain=str(self.chain),
                reason=f"[AaveV3Adapter] Cannot determine decimals: {e.reason}",
                suggestions=e.suggestions,
            ) from e

    def _default_price_oracle(self, asset: str) -> Decimal:
        """Default price oracle (returns placeholder prices).

        WARNING: These prices are HARDCODED and DO NOT reflect real market prices.
        Using these for health factor calculations can cause:
        - Incorrect borrow limits
        - Unexpected liquidations
        - Underestimated/overestimated position values

        This method should ONLY be used for local testing/development.
        For production, always pass a real price_oracle to the adapter.

        Args:
            asset: Asset symbol

        Returns:
            Price in USD (PLACEHOLDER - NOT REAL MARKET PRICE)
        """
        # These prices are from early 2024 and are SEVERELY OUTDATED
        # ETH is ~$3,100+ (not $2,000)
        # BTC is ~$100,000+ (not $45,000)
        # Using these WILL cause incorrect health factor calculations
        default_prices: dict[str, Decimal] = {
            "WETH": Decimal("2000"),  # OUTDATED - Real: ~$3,100+
            "WETH.e": Decimal("2000"),  # OUTDATED
            "WBTC": Decimal("45000"),  # OUTDATED - Real: ~$100,000+
            "WBTC.e": Decimal("45000"),  # OUTDATED
            "BTCB": Decimal("45000"),  # OUTDATED
            "USDC": Decimal("1"),
            "USDC.e": Decimal("1"),
            "USDT": Decimal("1"),
            "DAI": Decimal("1"),
            "DAI.e": Decimal("1"),
            "LINK": Decimal("15"),  # OUTDATED
            "LINK.e": Decimal("15"),  # OUTDATED
            "AAVE": Decimal("100"),  # OUTDATED
            "ARB": Decimal("1.2"),  # OUTDATED
            "OP": Decimal("2.5"),  # OUTDATED
            "WMATIC": Decimal("0.8"),  # OUTDATED
            "WAVAX": Decimal("35"),  # OUTDATED
            "WBNB": Decimal("300"),  # OUTDATED
            "wstETH": Decimal("2300"),  # OUTDATED
            "cbETH": Decimal("2100"),  # OUTDATED
            "rETH": Decimal("2200"),  # OUTDATED
            "sAVAX": Decimal("40"),  # OUTDATED
        }

        price = default_prices.get(asset) or default_prices.get(asset.upper())
        if price is None:
            raise ValueError(
                f"No placeholder price available for '{asset}'. "
                "Pass a real price_oracle to the adapter for production use."
            )

        # Log every time a placeholder price is used
        logger.warning(
            "PLACEHOLDER PRICE used for %s: $%s (NOT REAL MARKET PRICE). "
            "Health factor calculations may be INCORRECT. "
            "Pass a real price_oracle to avoid liquidation risk.",
            asset,
            price,
        )

        return price

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


# =============================================================================
# Factory Functions
# =============================================================================


def create_adapter_with_prices(
    config: AaveV3Config,
    prices: dict[str, Decimal],
) -> AaveV3Adapter:
    """Create an AaveV3Adapter with a dictionary of real prices.

    This is the recommended way to create an adapter for production use.
    Pass in prices from your MarketSnapshot or PriceAggregator.

    Example:
        from almanak.framework.connectors.aave_v3 import create_adapter_with_prices, AaveV3Config

        # Get prices from your data layer
        prices = {
            "WETH": Decimal("3100.50"),
            "WBTC": Decimal("100500.00"),
            "USDC": Decimal("1.00"),
        }

        config = AaveV3Config(chain="arbitrum", wallet_address="0x...")
        adapter = create_adapter_with_prices(config, prices)

        # Now health factor calculations use real prices
        hf = adapter.calculate_health_factor(positions, reserve_data)

    Args:
        config: Adapter configuration
        prices: Dictionary mapping token symbols to USD prices

    Returns:
        AaveV3Adapter configured with real prices
    """

    def price_oracle(asset: str) -> Decimal:
        """Look up price from the provided dictionary."""
        # Try exact match first
        if asset in prices:
            return prices[asset]
        # Try uppercase
        if asset.upper() in prices:
            return prices[asset.upper()]
        # Try common variations
        variations = [asset, asset.upper(), asset.lower()]
        for v in variations:
            if v in prices:
                return prices[v]

        # No price found - this is an error, not a fallback to placeholder
        raise KeyError(
            f"No price found for '{asset}'. Available prices: {list(prices.keys())}. "
            f"Ensure your price source includes all assets used in Aave positions."
        )

    return AaveV3Adapter(config, price_oracle=price_oracle)


def create_adapter_from_price_oracle_dict(
    chain: str,
    wallet_address: str,
    price_oracle_dict: dict[str, Decimal],
) -> AaveV3Adapter:
    """Create an AaveV3Adapter with a price oracle dictionary.

    Convenience method that creates the config automatically.

    Args:
        chain: Target chain (arbitrum, ethereum, etc.)
        wallet_address: User wallet address
        price_oracle_dict: Dictionary mapping token symbols to USD prices

    Returns:
        AaveV3Adapter configured with real prices
    """
    config = AaveV3Config(chain=chain, wallet_address=wallet_address)
    return create_adapter_with_prices(config, price_oracle_dict)


# =============================================================================
# Exports
# =============================================================================

__all__ = [
    # Adapter
    "AaveV3Adapter",
    "AaveV3Config",
    # Factory functions
    "create_adapter_with_prices",
    "create_adapter_from_price_oracle_dict",
    # Data classes
    "AaveV3ReserveData",
    "AaveV3UserAccountData",
    "AaveV3Position",
    "AaveV3FlashLoanParams",
    "AaveV3HealthFactorCalculation",
    "TransactionResult",
    # Enums
    "AaveV3InterestRateMode",
    "AaveV3EModeCategory",
    # Constants
    "AAVE_V3_POOL_ADDRESSES",
    "AAVE_V3_POOL_DATA_PROVIDER_ADDRESSES",
    "AAVE_V3_ORACLE_ADDRESSES",
    "EMODE_CATEGORIES",
    "DEFAULT_GAS_ESTIMATES",
    "AAVE_STABLE_RATE_MODE",
    "AAVE_VARIABLE_RATE_MODE",
]
