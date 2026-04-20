"""Spark Adapter.

This module provides an adapter for interacting with Spark lending protocol,
which is an Aave V3 fork with Spark-specific addresses.

Spark is a decentralized lending protocol supporting:
- Supply assets to earn yield
- Borrow against collateral
- Variable interest rates

Supported chains:
- Ethereum

Example:
    from almanak.framework.connectors.spark import SparkAdapter, SparkConfig

    config = SparkConfig(
        chain="ethereum",
        wallet_address="0x...",
    )
    adapter = SparkAdapter(config)

    # Supply collateral
    result = adapter.supply(
        asset="USDC",
        amount=Decimal("1000"),
    )

    # Borrow against collateral
    result = adapter.borrow(
        asset="DAI",
        amount=Decimal("500"),
    )
"""

import logging
from dataclasses import dataclass
from decimal import Decimal
from typing import TYPE_CHECKING, Any

from almanak.framework.data.tokens.exceptions import TokenResolutionError

if TYPE_CHECKING:
    from almanak.framework.data.tokens.resolver import TokenResolver as TokenResolverType

logger = logging.getLogger(__name__)


# =============================================================================
# Constants
# =============================================================================

# Spark Pool addresses per chain
SPARK_POOL_ADDRESSES: dict[str, str] = {
    "ethereum": "0xC13e21B648A5Ee794902342038FF3aDAB66BE987",
}

# Spark Pool Data Provider addresses per chain
SPARK_POOL_DATA_PROVIDER_ADDRESSES: dict[str, str] = {
    "ethereum": "0xFc21d6d146E6086B8359705C8b28512a983db0cb",
}

# Spark Oracle addresses per chain
SPARK_ORACLE_ADDRESSES: dict[str, str] = {
    "ethereum": "0x8105f69D9C41644c6A0803fDA7D03Aa70996cFD9",
}


# Spark uses same ABI as Aave V3, so same function selectors
SPARK_SUPPLY_SELECTOR = "0x617ba037"
SPARK_BORROW_SELECTOR = "0xa415bcad"
SPARK_REPAY_SELECTOR = "0x573ade81"
SPARK_WITHDRAW_SELECTOR = "0x69328dec"

# Max uint256 for max amount operations
MAX_UINT256 = 2**256 - 1

# Interest rate modes (same as Aave V3)
SPARK_STABLE_RATE_MODE = 1  # Being deprecated
SPARK_VARIABLE_RATE_MODE = 2  # Most common

# Gas estimates for Spark operations
# Spark supply() uses ~200,539 gas on Ethereum (measured on-chain).
# Previous value of 150,000 caused TX reverts in multi-TX bundles.
DEFAULT_GAS_ESTIMATES: dict[str, int] = {
    "supply": 250000,
    "borrow": 350000,
    "repay": 250000,
    "withdraw": 250000,
}


# =============================================================================
# Data Classes
# =============================================================================


@dataclass
class SparkConfig:
    """Configuration for Spark adapter.

    Attributes:
        chain: Blockchain network (ethereum)
        wallet_address: User wallet address
        default_slippage_bps: Default slippage tolerance in basis points
    """

    chain: str
    wallet_address: str
    default_slippage_bps: int = 50  # 0.5%

    def __post_init__(self) -> None:
        """Validate configuration."""
        valid_chains = set(SPARK_POOL_ADDRESSES.keys())
        if self.chain not in valid_chains:
            raise ValueError(f"Invalid chain: {self.chain}. Valid chains: {valid_chains}")
        if not self.wallet_address.startswith("0x") or len(self.wallet_address) != 42:
            raise ValueError(f"Invalid wallet address: {self.wallet_address}. Must be 0x-prefixed 40 hex chars.")
        if self.default_slippage_bps < 0 or self.default_slippage_bps > 10000:
            raise ValueError(f"Invalid slippage: {self.default_slippage_bps}. Must be 0-10000 bps.")


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


# =============================================================================
# Adapter
# =============================================================================


class SparkAdapter:
    """Adapter for Spark lending protocol.

    This adapter provides methods for interacting with Spark:
    - Supply/withdraw collateral
    - Borrow/repay assets

    Spark is an Aave V3 fork with the same ABI but different contract addresses.

    Example:
        config = SparkConfig(
            chain="ethereum",
            wallet_address="0x...",
        )
        adapter = SparkAdapter(config)

        # Supply DAI as collateral
        result = adapter.supply("DAI", Decimal("1000"))

        # Borrow WETH
        result = adapter.borrow("WETH", Decimal("0.5"))
    """

    def __init__(self, config: SparkConfig, token_resolver: "TokenResolverType | None" = None) -> None:
        """Initialize the adapter.

        Args:
            config: Adapter configuration
            token_resolver: Optional TokenResolver instance. If None, uses singleton.
        """
        self.config = config
        self.chain = config.chain
        self.wallet_address = config.wallet_address

        # Contract addresses
        self.pool_address = SPARK_POOL_ADDRESSES[config.chain]
        self.pool_data_provider_address = SPARK_POOL_DATA_PROVIDER_ADDRESSES[config.chain]
        self.oracle_address = SPARK_ORACLE_ADDRESSES[config.chain]

        # TokenResolver integration
        if token_resolver is not None:
            self._token_resolver = token_resolver
        else:
            from almanak.framework.data.tokens.resolver import get_token_resolver

            self._token_resolver = get_token_resolver()

        logger.info(f"SparkAdapter initialized for chain={config.chain}, wallet={config.wallet_address[:10]}...")

    def supply(
        self,
        asset: str,
        amount: Decimal,
        on_behalf_of: str | None = None,
    ) -> TransactionResult:
        """Build a supply transaction.

        Args:
            asset: Asset symbol to supply
            amount: Amount to supply
            on_behalf_of: Address to supply on behalf of (default: wallet_address)

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
                SPARK_SUPPLY_SELECTOR
                + self._pad_address(asset_address)
                + self._pad_uint256(amount_wei)
                + self._pad_address(recipient)
                + self._pad_uint256(0)  # referral code
            )

            return TransactionResult(
                success=True,
                tx_data={
                    "to": self.pool_address,
                    "value": 0,
                    "data": calldata,
                },
                gas_estimate=DEFAULT_GAS_ESTIMATES["supply"],
                description=f"Supply {amount} {asset} to Spark",
            )

        except Exception as e:
            logger.exception(f"Failed to build supply transaction: {e}")
            return TransactionResult(success=False, error=str(e))

    def borrow(
        self,
        asset: str,
        amount: Decimal,
        interest_rate_mode: int = SPARK_VARIABLE_RATE_MODE,
        on_behalf_of: str | None = None,
    ) -> TransactionResult:
        """Build a borrow transaction.

        Args:
            asset: Asset symbol to borrow
            amount: Amount to borrow
            interest_rate_mode: Interest rate mode (2 = variable)
            on_behalf_of: Address to borrow on behalf of (default: wallet_address)

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
                SPARK_BORROW_SELECTOR
                + self._pad_address(asset_address)
                + self._pad_uint256(amount_wei)
                + self._pad_uint256(interest_rate_mode)
                + self._pad_uint256(0)  # referral code
                + self._pad_address(recipient)
            )

            rate_mode_str = "variable" if interest_rate_mode == SPARK_VARIABLE_RATE_MODE else "stable"
            return TransactionResult(
                success=True,
                tx_data={
                    "to": self.pool_address,
                    "value": 0,
                    "data": calldata,
                },
                gas_estimate=DEFAULT_GAS_ESTIMATES["borrow"],
                description=f"Borrow {amount} {asset} from Spark ({rate_mode_str} rate)",
            )

        except Exception as e:
            logger.exception(f"Failed to build borrow transaction: {e}")
            return TransactionResult(success=False, error=str(e))

    def repay(
        self,
        asset: str,
        amount: Decimal,
        interest_rate_mode: int = SPARK_VARIABLE_RATE_MODE,
        on_behalf_of: str | None = None,
        repay_all: bool = False,
    ) -> TransactionResult:
        """Build a repay transaction.

        Args:
            asset: Asset symbol to repay
            amount: Amount to repay
            interest_rate_mode: Interest rate mode (2 = variable)
            on_behalf_of: Address to repay on behalf of (default: wallet_address)
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
                SPARK_REPAY_SELECTOR
                + self._pad_address(asset_address)
                + self._pad_uint256(amount_wei)
                + self._pad_uint256(interest_rate_mode)
                + self._pad_address(recipient)
            )

            amount_desc = "full debt" if repay_all else str(amount)
            rate_mode_str = "variable" if interest_rate_mode == SPARK_VARIABLE_RATE_MODE else "stable"
            return TransactionResult(
                success=True,
                tx_data={
                    "to": self.pool_address,
                    "value": 0,
                    "data": calldata,
                },
                gas_estimate=DEFAULT_GAS_ESTIMATES["repay"],
                description=f"Repay {amount_desc} {asset} to Spark ({rate_mode_str} rate)",
            )

        except Exception as e:
            logger.exception(f"Failed to build repay transaction: {e}")
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
            asset: Asset symbol to withdraw
            amount: Amount to withdraw
            to: Address to send withdrawn assets (default: wallet_address)
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
                SPARK_WITHDRAW_SELECTOR
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
                description=f"Withdraw {amount_desc} {asset} from Spark",
            )

        except Exception as e:
            logger.exception(f"Failed to build withdraw transaction: {e}")
            return TransactionResult(success=False, error=str(e))

    # =========================================================================
    # Utility Methods
    # =========================================================================

    def _resolve_asset(self, asset: str) -> str:
        """Resolve asset symbol or address to address using TokenResolver.

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
                reason=f"[SparkAdapter] Cannot resolve asset: {e.reason}",
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
                reason=f"[SparkAdapter] Cannot determine decimals: {e.reason}",
                suggestions=e.suggestions,
            ) from e

    @staticmethod
    def _pad_address(addr: str) -> str:
        """Pad address to 32 bytes."""
        addr_clean = addr.lower().replace("0x", "")
        return addr_clean.zfill(64)

    @staticmethod
    def _pad_uint256(value: int) -> str:
        """Pad uint256 to 32 bytes."""
        return hex(value)[2:].zfill(64)
