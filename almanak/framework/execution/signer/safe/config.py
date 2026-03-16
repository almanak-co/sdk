"""Configuration for Safe wallet signers.

This module provides configuration dataclasses for Safe wallet operations,
including wallet mapping from environment variables and signer configuration.

Key Components:
    - SafeWalletConfig: Configuration for a single Safe wallet
    - SafeSignerConfig: Complete configuration for a Safe signer
    - SafeWalletMapping: Loads and manages Safe wallet mappings from env vars

Environment Variables:
    ALMANAK_PLATFORM_WALLETS: JSON array of Safe wallet configurations
    ALMANAK_SIGNER_SERVICE_ENDPOINT_ROOT: URL for remote signer service (Zodiac mode)
    ALMANAK_SIGNER_SERVICE_JWT: JWT token for signer service authentication

Example:
    from almanak.framework.execution.signer.safe.config import SafeWalletMapping, SafeSignerConfig

    # Load wallet mapping
    mapping = SafeWalletMapping()
    wallet_config = mapping.get_config("0xSafeAddress...")

    # Create signer config
    signer_config = SafeSignerConfig(
        mode="direct",
        wallet_config=wallet_config,
        private_key="0x...",  # Required for direct mode, optional for zodiac mode
    )
"""

import json
import logging
import os
from dataclasses import dataclass, field
from functools import lru_cache

from almanak.framework.execution.signer.safe.constants import (
    DEFAULT_GAS_BUFFER_MULTIPLIER,
    DEFAULT_ROLE_KEY,
)

logger = logging.getLogger(__name__)


# =============================================================================
# Exceptions
# =============================================================================


class SafeConfigError(Exception):
    """Raised when Safe configuration is invalid or missing."""

    pass


# =============================================================================
# Configuration Dataclasses
# =============================================================================


@dataclass
class SafeWalletConfig:
    """Configuration for a Gnosis Safe wallet.

    Attributes:
        safe_address: The Gnosis Safe multisig address
        eoa_address: The EOA that signs transactions for this Safe
        zodiac_roles_address: Zodiac Roles module address (for production mode)
        role_key: The role key for Zodiac authorization (default: "AlmanakAgentRole")

    Example:
        config = SafeWalletConfig(
            safe_address="0xSafe...",
            eoa_address="0xEOA...",
            zodiac_roles_address="0xZodiac...",
        )
    """

    safe_address: str
    eoa_address: str
    zodiac_roles_address: str | None = None
    role_key: str = DEFAULT_ROLE_KEY

    def __post_init__(self) -> None:
        """Validate and normalize addresses."""
        # Normalize addresses to checksummed format
        from web3 import Web3

        try:
            self.safe_address = Web3.to_checksum_address(self.safe_address)
        except Exception as e:
            raise SafeConfigError(f"Invalid safe_address: {self.safe_address}") from e

        try:
            self.eoa_address = Web3.to_checksum_address(self.eoa_address)
        except Exception as e:
            raise SafeConfigError(f"Invalid eoa_address: {self.eoa_address}") from e

        if self.zodiac_roles_address:
            try:
                self.zodiac_roles_address = Web3.to_checksum_address(self.zodiac_roles_address)
            except Exception as e:
                raise SafeConfigError(f"Invalid zodiac_roles_address: {self.zodiac_roles_address}") from e


@dataclass
class SafeSignerConfig:
    """Complete configuration for a Safe signer.

    Attributes:
        mode: Signing mode - "zodiac" for production, "direct" for testing
        wallet_config: Safe wallet configuration
        private_key: EOA private key (never logged or included in repr).
            Required for "direct" mode, optional for "zodiac" mode (which uses
            a remote signer service instead).
        signer_service_url: URL for remote signer service (Zodiac mode only)
        signer_service_jwt: JWT token for signer service (Zodiac mode only)
        gas_buffer_multiplier: Multiplier for gas estimates (default: 2.0)

    Example:
        config = SafeSignerConfig(
            mode="direct",
            wallet_config=wallet_config,
            private_key="0x...",
            gas_buffer_multiplier=2.0,
        )
    """

    mode: str
    wallet_config: SafeWalletConfig
    private_key: str | None = field(default=None, repr=False)  # Never include in repr
    signer_service_url: str | None = None
    signer_service_jwt: str | None = field(default=None, repr=False)
    gas_buffer_multiplier: float = DEFAULT_GAS_BUFFER_MULTIPLIER

    def __post_init__(self) -> None:
        """Validate configuration based on mode."""
        valid_modes = ("zodiac", "direct")
        if self.mode not in valid_modes:
            raise SafeConfigError(f"Invalid mode '{self.mode}'. Must be one of: {valid_modes}")

        if self.mode == "zodiac":
            if not self.wallet_config.zodiac_roles_address:
                raise SafeConfigError("zodiac_roles_address is required for Zodiac mode")
            # Zodiac mode requires either a private_key (local signing) or
            # signer_service_url + signer_service_jwt (remote signing via plugin)
            has_local_key = bool(self.private_key)
            has_remote_signer = bool(self.signer_service_url) and bool(self.signer_service_jwt)
            if not has_local_key and not has_remote_signer:
                raise SafeConfigError(
                    "Zodiac mode requires either private_key (local signing) or "
                    "signer_service_url + signer_service_jwt (remote signing via plugin)"
                )
        else:
            if not self.private_key:
                raise SafeConfigError("private_key is required for direct mode")

        if self.gas_buffer_multiplier <= 0:
            raise SafeConfigError(f"gas_buffer_multiplier must be positive, got {self.gas_buffer_multiplier}")


# =============================================================================
# Safe Wallet Mapping
# =============================================================================


class SafeWalletMapping:
    """Manages Safe wallet mappings loaded from environment variables.

    This class loads Safe wallet configurations from the ALMANAK_PLATFORM_WALLETS
    environment variable and provides lookup methods to retrieve configurations
    by Safe address.

    The environment variable should contain a JSON array of objects:
    ```json
    [
        {
            "SAFE_ACCOUNT_ADDRESS": "0x...",
            "EOA_ADDRESS": "0x...",
            "ZODIAC_ROLES_ADDRESS": "0x..."
        }
    ]
    ```

    Example:
        mapping = SafeWalletMapping()
        config = mapping.get_config("0xSafeAddress...")
        eoa = mapping.get_eoa_address("0xSafeAddress...")
    """

    def __init__(self, env_var: str = "ALMANAK_PLATFORM_WALLETS") -> None:
        """Initialize the wallet mapping.

        Args:
            env_var: Name of environment variable containing wallet JSON

        Raises:
            SafeConfigError: If environment variable is missing or invalid
        """
        self._env_var = env_var
        self._mapping: dict[str, dict] = {}
        self._load_mapping()

    def _load_mapping(self) -> None:
        """Load wallet mapping from environment variable."""
        from web3 import Web3

        env_value = os.environ.get(self._env_var)
        if not env_value:
            raise SafeConfigError(f"Environment variable {self._env_var} is not set")

        try:
            wallets_data = json.loads(env_value)
        except json.JSONDecodeError as e:
            raise SafeConfigError(f"Failed to parse {self._env_var} as JSON: {e}") from e

        if not isinstance(wallets_data, list):
            raise SafeConfigError(f"{self._env_var} must be a JSON array")

        for wallet in wallets_data:
            if not isinstance(wallet, dict):
                raise SafeConfigError(f"Each wallet in {self._env_var} must be an object")

            safe_address = wallet.get("SAFE_ACCOUNT_ADDRESS")
            if not safe_address:
                raise SafeConfigError(f"Missing SAFE_ACCOUNT_ADDRESS in wallet: {wallet}")

            try:
                safe_address = Web3.to_checksum_address(safe_address)
            except Exception as e:
                raise SafeConfigError(f"Invalid SAFE_ACCOUNT_ADDRESS: {safe_address}") from e

            self._mapping[safe_address] = wallet

        logger.debug(f"Loaded {len(self._mapping)} Safe wallet mappings")

    def get_config(self, safe_address: str) -> SafeWalletConfig:
        """Get SafeWalletConfig for a Safe address.

        Args:
            safe_address: The Safe address to look up

        Returns:
            SafeWalletConfig for the Safe

        Raises:
            SafeConfigError: If Safe address is not found
        """
        from web3 import Web3

        safe_address = Web3.to_checksum_address(safe_address)

        if safe_address not in self._mapping:
            raise SafeConfigError(f"No wallet mapping found for Safe: {safe_address}")

        wallet_data = self._mapping[safe_address]

        return SafeWalletConfig(
            safe_address=safe_address,
            eoa_address=wallet_data["EOA_ADDRESS"],
            zodiac_roles_address=wallet_data.get("ZODIAC_ROLES_ADDRESS"),
        )

    def get_eoa_address(self, safe_address: str) -> str:
        """Get the EOA address for a Safe.

        Args:
            safe_address: The Safe address to look up

        Returns:
            The EOA address that signs for this Safe

        Raises:
            SafeConfigError: If Safe address is not found
        """
        config = self.get_config(safe_address)
        return config.eoa_address

    def get_zodiac_address(self, safe_address: str) -> str:
        """Get the Zodiac Roles address for a Safe.

        Args:
            safe_address: The Safe address to look up

        Returns:
            The Zodiac Roles module address

        Raises:
            SafeConfigError: If Safe address not found or no Zodiac address
        """
        config = self.get_config(safe_address)
        if not config.zodiac_roles_address:
            raise SafeConfigError(f"No Zodiac Roles address for Safe: {safe_address}")
        return config.zodiac_roles_address

    def list_safes(self) -> list[str]:
        """List all configured Safe addresses.

        Returns:
            List of Safe addresses
        """
        return list(self._mapping.keys())

    def __contains__(self, safe_address: str) -> bool:
        """Check if a Safe address is in the mapping."""
        from web3 import Web3

        try:
            safe_address = Web3.to_checksum_address(safe_address)
            return safe_address in self._mapping
        except Exception:
            return False

    def __len__(self) -> int:
        """Return number of configured Safe wallets."""
        return len(self._mapping)


# =============================================================================
# Factory Functions
# =============================================================================


@lru_cache(maxsize=1)
def get_wallet_mapping() -> SafeWalletMapping:
    """Get or create the global SafeWalletMapping instance.

    Returns a cached instance of SafeWalletMapping loaded from
    ALMANAK_PLATFORM_WALLETS environment variable.

    Returns:
        Cached SafeWalletMapping instance

    Raises:
        SafeConfigError: If environment variable is missing or invalid
    """
    return SafeWalletMapping()


def create_signer_config_from_env(
    safe_address: str,
    private_key: str,
    mode: str = "direct",
) -> SafeSignerConfig:
    """Create a SafeSignerConfig from environment variables.

    This is a convenience function that loads wallet configuration from
    ALMANAK_PLATFORM_WALLETS and signer service config from environment
    variables.

    Args:
        safe_address: The Safe address to configure
        private_key: The EOA private key for signing
        mode: Signing mode - "zodiac" or "direct"

    Returns:
        SafeSignerConfig ready for use

    Raises:
        SafeConfigError: If configuration is invalid or missing

    Environment Variables:
        ALMANAK_PLATFORM_WALLETS: Required - wallet mappings
        ALMANAK_SIGNER_SERVICE_ENDPOINT_ROOT: Required for Zodiac mode
        ALMANAK_SIGNER_SERVICE_JWT: Required for Zodiac mode
    """
    mapping = get_wallet_mapping()
    wallet_config = mapping.get_config(safe_address)

    signer_service_url = None
    signer_service_jwt = None

    if mode == "zodiac":
        signer_service_url = os.environ.get("ALMANAK_SIGNER_SERVICE_ENDPOINT_ROOT")
        signer_service_jwt = os.environ.get("ALMANAK_SIGNER_SERVICE_JWT")

        if not signer_service_url:
            raise SafeConfigError("ALMANAK_SIGNER_SERVICE_ENDPOINT_ROOT required for Zodiac mode")
        if not signer_service_jwt:
            raise SafeConfigError("ALMANAK_SIGNER_SERVICE_JWT required for Zodiac mode")

    return SafeSignerConfig(
        mode=mode,
        wallet_config=wallet_config,
        private_key=private_key,
        signer_service_url=signer_service_url,
        signer_service_jwt=signer_service_jwt,
    )


# =============================================================================
# Exports
# =============================================================================


__all__ = [
    # Exceptions
    "SafeConfigError",
    # Configuration classes
    "SafeWalletConfig",
    "SafeSignerConfig",
    "SafeWalletMapping",
    # Factory functions
    "get_wallet_mapping",
    "create_signer_config_from_env",
]
