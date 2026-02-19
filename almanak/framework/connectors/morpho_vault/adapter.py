"""MetaMorpho Vault Adapter.

This module provides a high-level adapter for interacting with MetaMorpho vaults,
supporting deposit and redeem operations via ERC-4626.

MetaMorpho vaults aggregate capital across multiple Morpho Blue lending markets,
offering passive yield optimization with curator-managed allocation.

Supported chains:
- Ethereum
- Base

Example:
    from almanak.framework.connectors.morpho_vault import MetaMorphoAdapter, MetaMorphoConfig

    config = MetaMorphoConfig(chain="ethereum", wallet_address="0x...")
    adapter = MetaMorphoAdapter(config, gateway_client=gateway_client)

    # Deposit assets
    result = adapter.deposit(
        vault_address="0xBEEF01735c132Ada46AA9aA4c54623cAA92A64CB",
        amount=Decimal("1000"),
    )
"""

import logging
from dataclasses import dataclass
from decimal import Decimal
from typing import TYPE_CHECKING, Any

from almanak.framework.data.tokens.exceptions import TokenResolutionError

from .sdk import (
    SUPPORTED_CHAINS,
    DepositExceedsCapError,
    InsufficientSharesError,
    MetaMorphoSDK,
    MetaMorphoSDKError,
    VaultInfo,
    VaultPosition,
)

if TYPE_CHECKING:
    from almanak.framework.data.tokens.resolver import TokenResolver as TokenResolverType

logger = logging.getLogger(__name__)


# =============================================================================
# Data Classes
# =============================================================================


@dataclass
class MetaMorphoConfig:
    """Configuration for MetaMorpho adapter.

    Attributes:
        chain: Blockchain network (ethereum, base)
        wallet_address: User wallet address
    """

    chain: str
    wallet_address: str

    def __post_init__(self) -> None:
        """Validate configuration."""
        if self.chain.lower() not in SUPPORTED_CHAINS:
            raise ValueError(f"Invalid chain: {self.chain}. Supported: {sorted(SUPPORTED_CHAINS)}")
        if not self.wallet_address.startswith("0x") or len(self.wallet_address) != 42:
            raise ValueError(f"Invalid wallet address: {self.wallet_address}. Must be 0x-prefixed 40 hex chars.")


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
# Adapter
# =============================================================================


class MetaMorphoAdapter:
    """Adapter for MetaMorpho vault protocol.

    Provides high-level methods for depositing into and redeeming from
    MetaMorpho ERC-4626 vaults, with token resolution and validation.

    Example:
        config = MetaMorphoConfig(chain="ethereum", wallet_address="0x...")
        adapter = MetaMorphoAdapter(config, gateway_client=client)

        # Get vault info
        info = adapter.get_vault_info("0xBEEF...")

        # Deposit
        result = adapter.deposit("0xBEEF...", Decimal("1000"))
    """

    def __init__(
        self,
        config: MetaMorphoConfig,
        gateway_client=None,
        token_resolver: "TokenResolverType | None" = None,
    ) -> None:
        """Initialize the adapter.

        Args:
            config: Adapter configuration
            gateway_client: Gateway client for RPC calls. Required for on-chain operations.
            token_resolver: Optional TokenResolver instance. If None, uses singleton.
        """
        self.config = config
        self.chain = config.chain.lower()
        self.wallet_address = config.wallet_address

        # Gateway client for SDK
        self._gateway_client = gateway_client

        # SDK (lazy init)
        self._sdk: MetaMorphoSDK | None = None

        # Token resolver
        if token_resolver is not None:
            self._token_resolver = token_resolver
        else:
            from almanak.framework.data.tokens.resolver import get_token_resolver

            self._token_resolver = get_token_resolver()

        logger.info(f"MetaMorphoAdapter initialized for chain={self.chain}, wallet={self.wallet_address[:10]}...")

    @property
    def sdk(self) -> MetaMorphoSDK:
        """Get the SDK instance (lazy initialization)."""
        if self._sdk is None:
            if self._gateway_client is None:
                raise RuntimeError("gateway_client is required for on-chain operations")
            self._sdk = MetaMorphoSDK(self._gateway_client, self.chain)
        return self._sdk

    # =========================================================================
    # Vault Information
    # =========================================================================

    def get_vault_info(self, vault_address: str) -> VaultInfo:
        """Get complete vault information.

        Args:
            vault_address: MetaMorpho vault address

        Returns:
            VaultInfo with vault state
        """
        self._validate_address(vault_address)
        return self.sdk.get_vault_info(vault_address)

    def get_position(self, vault_address: str, user: str | None = None) -> VaultPosition:
        """Get user's position in the vault.

        Args:
            vault_address: MetaMorpho vault address
            user: User address (defaults to wallet_address)

        Returns:
            VaultPosition with shares and assets
        """
        self._validate_address(vault_address)
        user_address = user or self.wallet_address
        return self.sdk.get_position(vault_address, user_address)

    # =========================================================================
    # Deposit
    # =========================================================================

    def deposit(
        self,
        vault_address: str,
        amount: Decimal,
    ) -> TransactionResult:
        """Build a deposit transaction for a MetaMorpho vault.

        This builds approve + deposit transactions. The approve TX authorizes
        the vault to pull the exact amount of underlying tokens.

        Args:
            vault_address: MetaMorpho vault address
            amount: Amount of underlying assets to deposit (in token units, e.g. 1000.0 USDC)

        Returns:
            TransactionResult with transaction data for both approve and deposit
        """
        try:
            self._validate_address(vault_address)

            # Get vault asset and resolve decimals
            asset_address = self.sdk.get_vault_asset(vault_address)
            asset_decimals = self._get_decimals_for_address(asset_address)
            amount_wei = int(amount * Decimal(10**asset_decimals))

            if amount_wei <= 0:
                return TransactionResult(success=False, error="Deposit amount must be positive")

            # Check maxDeposit
            max_deposit = self.sdk.get_max_deposit(vault_address, self.wallet_address)
            if amount_wei > max_deposit:
                raise DepositExceedsCapError(
                    f"Deposit amount {amount_wei} exceeds maxDeposit {max_deposit} "
                    f"for vault {vault_address} on {self.chain}"
                )

            # Build approve TX (exact amount, not MAX_UINT256)
            approve_tx = self.sdk.build_approve_tx(
                token_address=asset_address,
                spender=vault_address,
                amount=amount_wei,
                owner=self.wallet_address,
            )

            # Build deposit TX
            deposit_tx = self.sdk.build_deposit_tx(
                vault_address=vault_address,
                assets=amount_wei,
                receiver=self.wallet_address,
            )

            return TransactionResult(
                success=True,
                tx_data={
                    "approve": approve_tx,
                    "deposit": deposit_tx,
                },
                gas_estimate=approve_tx["gas_estimate"] + deposit_tx["gas_estimate"],
                description=f"Deposit {amount} tokens into MetaMorpho vault {vault_address[:10]}...",
            )

        except MetaMorphoSDKError as e:
            logger.error(f"MetaMorpho deposit failed: {e}")
            return TransactionResult(success=False, error=str(e))
        except Exception as e:
            logger.exception(f"Failed to build deposit transaction: {e}")
            return TransactionResult(success=False, error=str(e))

    # =========================================================================
    # Redeem
    # =========================================================================

    def redeem(
        self,
        vault_address: str,
        shares: Decimal | str,
    ) -> TransactionResult:
        """Build a redeem transaction for a MetaMorpho vault.

        Args:
            vault_address: MetaMorpho vault address
            shares: Number of shares to redeem, or "all" to redeem all

        Returns:
            TransactionResult with transaction data
        """
        try:
            self._validate_address(vault_address)

            if shares == "all":
                # Query max redeemable shares (single RPC call)
                shares_wei = self.sdk.get_max_redeem(vault_address, self.wallet_address)
                if shares_wei <= 0:
                    return TransactionResult(success=False, error="No shares to redeem")
                # No need to check maxRedeem again -- we already have the exact value
            else:
                if not isinstance(shares, Decimal):
                    shares = Decimal(str(shares))
                # Resolve share decimals dynamically
                share_decimals = self.sdk.get_decimals(vault_address)
                shares_wei = int(shares * Decimal(10**share_decimals))

                if shares_wei <= 0:
                    return TransactionResult(success=False, error="Redeem shares must be positive")

                # Check maxRedeem
                max_redeem = self.sdk.get_max_redeem(vault_address, self.wallet_address)
                if shares_wei > max_redeem:
                    raise InsufficientSharesError(
                        f"Redeem shares {shares_wei} exceeds maxRedeem {max_redeem} "
                        f"for vault {vault_address} on {self.chain}"
                    )

            # Build redeem TX (no approve needed - redeeming own shares)
            redeem_tx = self.sdk.build_redeem_tx(
                vault_address=vault_address,
                shares=shares_wei,
                receiver=self.wallet_address,
                owner=self.wallet_address,
            )

            return TransactionResult(
                success=True,
                tx_data={"redeem": redeem_tx},
                gas_estimate=redeem_tx["gas_estimate"],
                description=f"Redeem {'all' if shares == 'all' else shares} shares from MetaMorpho vault {vault_address[:10]}...",
            )

        except MetaMorphoSDKError as e:
            logger.error(f"MetaMorpho redeem failed: {e}")
            return TransactionResult(success=False, error=str(e))
        except Exception as e:
            logger.exception(f"Failed to build redeem transaction: {e}")
            return TransactionResult(success=False, error=str(e))

    # =========================================================================
    # Approval
    # =========================================================================

    def build_approve_transaction(
        self,
        token: str,
        amount: Decimal,
        spender: str,
    ) -> TransactionResult:
        """Build an ERC20 approve transaction.

        Args:
            token: Token symbol or address
            amount: Amount to approve
            spender: Address to approve

        Returns:
            TransactionResult with transaction data
        """
        try:
            token_address = self._resolve_token(token)
            decimals = self._get_decimals(token)
            amount_wei = int(amount * Decimal(10**decimals))

            approve_tx = self.sdk.build_approve_tx(
                token_address=token_address,
                spender=spender,
                amount=amount_wei,
                owner=self.wallet_address,
            )

            return TransactionResult(
                success=True,
                tx_data=approve_tx,
                gas_estimate=approve_tx["gas_estimate"],
                description=f"Approve {amount} {token} for {spender[:10]}...",
            )

        except Exception as e:
            logger.exception(f"Failed to build approve transaction: {e}")
            return TransactionResult(success=False, error=str(e))

    # =========================================================================
    # Private Helpers
    # =========================================================================

    def _validate_address(self, address: str) -> None:
        """Validate an Ethereum address."""
        if not address.startswith("0x") or len(address) != 42:
            raise ValueError(f"Invalid address: {address}. Must be 0x-prefixed 40 hex chars.")

    def _resolve_token(self, token: str) -> str:
        """Resolve token symbol or address to address."""
        if token.startswith("0x") and len(token) == 42:
            return token
        try:
            resolved = self._token_resolver.resolve(token, self.chain)
            return resolved.address
        except TokenResolutionError as e:
            raise TokenResolutionError(
                token=token,
                chain=self.chain,
                reason=f"[MetaMorphoAdapter] Cannot resolve token: {e.reason}",
                suggestions=e.suggestions,
            ) from e

    def _get_decimals(self, token: str) -> int:
        """Get decimals for a token using TokenResolver."""
        try:
            resolved = self._token_resolver.resolve(token, self.chain)
            return resolved.decimals
        except TokenResolutionError as e:
            raise TokenResolutionError(
                token=token,
                chain=self.chain,
                reason=f"[MetaMorphoAdapter] Cannot determine decimals: {e.reason}",
                suggestions=e.suggestions,
            ) from e

    def _get_decimals_for_address(self, token_address: str) -> int:
        """Get decimals for a token by address using TokenResolver."""
        try:
            resolved = self._token_resolver.resolve(token_address, self.chain)
            return resolved.decimals
        except TokenResolutionError as e:
            raise TokenResolutionError(
                token=token_address,
                chain=self.chain,
                reason=f"[MetaMorphoAdapter] Cannot determine decimals for {token_address}: {e.reason}",
                suggestions=e.suggestions,
            ) from e


# =============================================================================
# Factory Functions
# =============================================================================


def create_test_adapter(
    chain: str = "ethereum",
    wallet_address: str = "0x1234567890123456789012345678901234567890",
) -> MetaMorphoAdapter:
    """Create a test adapter without gateway client (for unit tests).

    For unit testing only. On-chain operations will raise RuntimeError.

    Args:
        chain: Chain name (default: ethereum)
        wallet_address: Wallet address (default: test address)

    Returns:
        MetaMorphoAdapter configured for testing
    """
    config = MetaMorphoConfig(chain=chain, wallet_address=wallet_address)
    return MetaMorphoAdapter(config)
