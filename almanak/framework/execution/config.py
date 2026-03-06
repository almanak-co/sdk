"""Local Runtime Configuration for Execution Environment.

This module provides typed, validated configuration for local execution
environments. It supports loading from environment variables and provides
secure handling of sensitive data like private keys.

Key Features:
    - Required fields: chain, rpc_url, private_key (validated at instantiation)
    - Derived fields: wallet_address (from private_key)
    - Optional fields with defaults: max_gas_price_gwei, tx_timeout_seconds, simulation_enabled
    - Environment variable loading via python-dotenv
    - Private key never exposed in __repr__ or string representations

Example:
    # Load from environment variables
    config = LocalRuntimeConfig.from_env()

    # Or create directly
    config = LocalRuntimeConfig(
        chain="arbitrum",
        rpc_url="https://arb1.arbitrum.io/rpc",
        private_key="0x...",
    )

    print(f"Wallet: {config.wallet_address}")
    print(f"Chain: {config.chain}")
"""

import logging
import os
import re
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any, Literal, Optional

from dotenv import load_dotenv
from eth_account import Account

# =============================================================================
# Type Aliases
# =============================================================================


# Data freshness policy for multi-chain strategies
# - fail_closed: If ANY chain's data is stale (>30s) or unavailable, decide() receives error
# - fail_open: Stale/unavailable chains excluded from snapshot, decide() proceeds with available data
DataFreshnessPolicy = Literal["fail_closed", "fail_open"]

# Import Safe signer types for type hints (avoid circular imports)
from typing import TYPE_CHECKING

from almanak.framework.execution.gas.constants import (
    ANVIL_GAS_PRICE_CAP_GWEI,
    CHAIN_GAS_PRICE_CAPS_GWEI,
    DEFAULT_GAS_PRICE_CAP_GWEI,
)
from almanak.framework.execution.interfaces import Chain

if TYPE_CHECKING:
    from almanak.framework.execution.signer.safe import SafeSigner

logger = logging.getLogger(__name__)

# VIB-308: Env vars that are silently ignored when set without the ALMANAK_ prefix.
_UNPREFIXED_WARN_VARS: list[str] = [
    "MAX_GAS_PRICE_GWEI",
    "TX_TIMEOUT_SECONDS",
    "SIMULATION_ENABLED",
    "MAX_SLIPPAGE_BPS",
]


def _warn_unprefixed_env_vars(prefix: str) -> None:
    """Warn when env vars are set without the required prefix.

    Silently-ignored env vars are dangerous when real money is at stake.
    This helper emits a warning so operators notice the misconfiguration.
    """
    for var in _UNPREFIXED_WARN_VARS:
        if os.environ.get(var) and not os.environ.get(f"{prefix}{var}"):
            logger.warning(
                f"Environment variable '{var}' is set but will be ignored -- "
                f"use '{prefix}{var}' instead (the {prefix} prefix is required)"
            )


# =============================================================================
# Exceptions
# =============================================================================


class ConfigurationError(Exception):
    """Raised when configuration is invalid or missing.

    Attributes:
        field: The configuration field that caused the error
        reason: Human-readable explanation of the error
    """

    def __init__(self, field: str, reason: str) -> None:
        self.field = field
        self.reason = reason
        super().__init__(f"Configuration error for '{field}': {reason}")


class MissingEnvironmentVariableError(ConfigurationError):
    """Raised when a required environment variable is missing."""

    def __init__(self, var_name: str) -> None:
        self.var_name = var_name
        super().__init__(field=var_name, reason="Required environment variable not set")


# =============================================================================
# Constants
# =============================================================================


# Chain ID mapping for supported chains
CHAIN_IDS: dict[str, int] = {
    "ethereum": 1,
    "arbitrum": 42161,
    "optimism": 10,
    "polygon": 137,
    "base": 8453,
    "avalanche": 43114,
    "bnb": 56,
    "linea": 59144,
    "plasma": 9745,
    "blast": 81457,
    "mantle": 5000,
    "berachain": 80094,
    "sonic": 146,
}

# Supported protocols and which chains they are available on
SUPPORTED_PROTOCOLS: dict[str, set[str]] = {
    "aave_v3": {"ethereum", "arbitrum", "optimism", "polygon", "base", "avalanche", "bnb", "linea", "plasma", "blast"},
    "uniswap_v3": {
        "ethereum",
        "arbitrum",
        "optimism",
        "polygon",
        "base",
        "avalanche",
        "bnb",
        "linea",
        "blast",
        "mantle",
    },
    "gmx_v2": {"arbitrum", "avalanche"},
    "hyperliquid": {"arbitrum"},  # Hyperliquid is on its own L1 but accessed via Arbitrum
    "enso": {
        "ethereum",
        "arbitrum",
        "optimism",
        "polygon",
        "base",
        "avalanche",
        "bnb",
        "linea",
        "plasma",
        "blast",
        "mantle",
        "berachain",
        "sonic",
    },  # Aggregator
    "traderjoe_v2": {"avalanche"},  # TraderJoe Liquidity Book V2 on Avalanche
    "spark": {"ethereum"},  # Spark is an Aave V3 fork on Ethereum
    "pancakeswap_v3": {"bnb", "ethereum", "arbitrum"},  # PancakeSwap V3 DEX
    "lido": {"ethereum", "arbitrum", "optimism", "polygon"},  # Lido liquid staking
    "ethena": {"ethereum"},  # Ethena synthetic dollar (USDe/sUSDe)
}


# =============================================================================
# Execution Mode
# =============================================================================


class ExecutionMode(StrEnum):
    """Execution mode for transaction signing.

    This enum controls how transactions are signed and executed:

    EOA:
        Direct EOA (Externally Owned Account) signing. Transactions are signed
        directly with the private key and sent to the network. Works on both
        mainnet/testnet and local Anvil forks.

        Required env vars:
            - ALMANAK_PRIVATE_KEY

    SAFE_DIRECT:
        Safe wallet with direct local signing. Transactions are wrapped in
        Safe.execTransaction() calls and signed locally. Used for testing
        Safe flows on Anvil forks without needing the signer service.

        Required env vars:
            - ALMANAK_PRIVATE_KEY
            - ALMANAK_SAFE_ADDRESS

    SAFE_ZODIAC:
        Safe wallet with Zodiac Roles module. Transactions are executed via
        execTransactionWithRole() through a remote signer service. Used in
        production where the private key is held by a secure signer service.

        Required env vars:
            - ALMANAK_PRIVATE_KEY (for EOA that has the role)
            - ALMANAK_SAFE_ADDRESS
            - ALMANAK_ZODIAC_ADDRESS
            - ALMANAK_SIGNER_SERVICE_URL
            - ALMANAK_SIGNER_SERVICE_JWT

    Example:
        # Set via environment variable
        export ALMANAK_EXECUTION_MODE=safe_direct

        # Or use in code
        config = MultiChainRuntimeConfig.from_env(
            chains=["arbitrum"],
            protocols={"arbitrum": ["enso"]},
        )
        # Mode is auto-detected from ALMANAK_EXECUTION_MODE
    """

    EOA = "eoa"
    SAFE_DIRECT = "safe_direct"
    SAFE_ZODIAC = "safe_zodiac"

    @classmethod
    def from_string(cls, value: str) -> "ExecutionMode":
        """Parse execution mode from string.

        Args:
            value: Mode string (case-insensitive)

        Returns:
            ExecutionMode enum value

        Raises:
            ConfigurationError: If value is not a valid mode
        """
        try:
            return cls(value.lower())
        except ValueError as e:
            valid_modes = ", ".join(m.value for m in cls)
            raise ConfigurationError(
                field="execution_mode",
                reason=f"Invalid execution mode '{value}'. Valid modes: {valid_modes}",
            ) from e


# =============================================================================
# Configuration Class
# =============================================================================


@dataclass
class LocalRuntimeConfig:
    """Typed configuration for local execution environment.

    This configuration class provides all settings needed for local
    transaction execution. It validates all fields at instantiation
    and derives the wallet address from the private key.

    Required Fields:
        chain: Blockchain network to execute on (e.g., "arbitrum", "ethereum")
        rpc_url: RPC endpoint URL for the chain
        private_key: Hex-encoded private key for signing transactions

    Derived Fields:
        wallet_address: Ethereum address derived from private_key
        chain_id: Numeric chain ID derived from chain name

    Optional Fields:
        max_gas_price_gwei: Maximum gas price in gwei (default 100)
        tx_timeout_seconds: Transaction confirmation timeout (default 120)
        simulation_enabled: Whether to simulate transactions before submission (default True)
        max_tx_value_eth: Maximum value per transaction in ETH (default 10.0)
        base_retry_delay: Base delay for retries in seconds (default 1.0)
        max_retry_delay: Maximum delay for retries in seconds (default 32.0)
        max_retries: Maximum number of retry attempts (default 3)

    SECURITY CONTRACT:
        - private_key is NEVER included in __repr__, __str__, or to_dict()
        - private_key is NEVER logged
        - Wallet address is derived once at initialization and cached

    Example:
        # From environment variables
        config = LocalRuntimeConfig.from_env()

        # Direct instantiation
        config = LocalRuntimeConfig(
            chain="arbitrum",
            rpc_url="https://arb1.arbitrum.io/rpc",
            private_key="0x...",
            max_gas_price_gwei=50,
        )

        # Access derived fields
        print(config.wallet_address)  # 0x71C7...
        print(config.chain_id)  # 42161
    """

    # Required fields
    chain: str
    rpc_url: str
    private_key: str = field(repr=False)  # Never include in repr

    # Derived fields (set in __post_init__)
    wallet_address: str = field(default="", init=False)
    chain_id: int = field(default=0, init=False)

    # Optional fields with defaults
    max_gas_price_gwei: int = 100
    max_gas_cost_native: float = 0.0  # Max gas cost per tx in native token (0 = no limit)
    max_gas_cost_usd: float = 0.0  # Max gas cost per tx in USD (0 = no limit)
    max_slippage_bps: int = 0  # Max acceptable swap slippage in bps (0 = no limit)
    tx_timeout_seconds: int = 120
    simulation_enabled: bool = True
    max_tx_value_eth: float = 10.0
    base_retry_delay: float = 1.0
    max_retry_delay: float = 32.0
    max_retries: int = 3

    # Safe wallet signer (optional - mirrors MultiChainRuntimeConfig)
    safe_signer: Optional["SafeSigner"] = None

    # Metadata
    created_at: datetime = field(default_factory=lambda: datetime.now(UTC))

    def __post_init__(self) -> None:
        """Validate configuration and derive fields after initialization."""
        # Validate chain
        self._validate_chain()

        # Validate RPC URL
        self._validate_rpc_url()

        # Validate and derive wallet address from private key
        self._validate_and_derive_wallet()

        # Validate optional fields
        self._validate_optional_fields()

        # Set derived chain_id
        self.chain_id = CHAIN_IDS.get(self.chain.lower(), 0)

        logger.debug(
            "LocalRuntimeConfig initialized: chain=%s, wallet=%s",
            self.chain,
            self.wallet_address,
        )

    def _validate_chain(self) -> None:
        """Validate the chain field."""
        if not self.chain:
            raise ConfigurationError(field="chain", reason="Chain cannot be empty")

        chain_lower = self.chain.lower()
        if chain_lower not in CHAIN_IDS:
            valid_chains = ", ".join(sorted(CHAIN_IDS.keys()))
            raise ConfigurationError(
                field="chain",
                reason=f"Unsupported chain '{self.chain}'. Valid chains: {valid_chains}",
            )

        # Normalize to lowercase
        self.chain = chain_lower

    def _validate_rpc_url(self) -> None:
        """Validate the RPC URL field."""
        if not self.rpc_url:
            raise ConfigurationError(field="rpc_url", reason="RPC URL cannot be empty")

        # Basic URL format validation
        url_pattern = re.compile(
            r"^https?://"  # http:// or https://
            r"[a-zA-Z0-9.-]+"  # domain
            r"(:\d+)?"  # optional port
            r"(/.*)?$"  # optional path
        )
        if not url_pattern.match(self.rpc_url):
            raise ConfigurationError(
                field="rpc_url",
                reason=f"Invalid RPC URL format: {self._mask_url(self.rpc_url)}",
            )

    def _validate_and_derive_wallet(self) -> None:
        """Validate private key and derive wallet address."""
        if not self.private_key:
            raise ConfigurationError(field="private_key", reason="Private key cannot be empty")

        # Normalize private key format
        key = self.private_key
        if not key.startswith("0x"):
            key = "0x" + key

        # Validate hex format
        if len(key) != 66:  # 0x + 64 hex chars
            raise ConfigurationError(
                field="private_key",
                reason="Private key must be 32 bytes (64 hex characters)",
            )

        try:
            # Derive wallet address from private key
            account = Account.from_key(key)
            self.wallet_address = account.address
        except Exception:
            # Never expose key material in error messages
            raise ConfigurationError(field="private_key", reason="Invalid private key format") from None

    def _validate_optional_fields(self) -> None:
        """Validate optional fields with defaults."""
        if self.max_gas_price_gwei <= 0:
            raise ConfigurationError(
                field="max_gas_price_gwei",
                reason=f"Must be positive, got {self.max_gas_price_gwei}",
            )

        if self.max_gas_price_gwei > 10000:
            raise ConfigurationError(
                field="max_gas_price_gwei",
                reason=f"Exceeds maximum (10000 gwei), got {self.max_gas_price_gwei}",
            )

        if self.tx_timeout_seconds <= 0:
            raise ConfigurationError(
                field="tx_timeout_seconds",
                reason=f"Must be positive, got {self.tx_timeout_seconds}",
            )

        if self.tx_timeout_seconds > 600:
            raise ConfigurationError(
                field="tx_timeout_seconds",
                reason=f"Exceeds maximum (600 seconds), got {self.tx_timeout_seconds}",
            )

        if self.max_tx_value_eth < 0:
            raise ConfigurationError(
                field="max_tx_value_eth",
                reason=f"Cannot be negative, got {self.max_tx_value_eth}",
            )

        if self.max_gas_cost_native < 0:
            raise ConfigurationError(
                field="max_gas_cost_native",
                reason=f"Cannot be negative, got {self.max_gas_cost_native}",
            )

        if self.max_gas_cost_usd < 0:
            raise ConfigurationError(
                field="max_gas_cost_usd",
                reason=f"Cannot be negative, got {self.max_gas_cost_usd}",
            )

        if self.max_slippage_bps < 0:
            raise ConfigurationError(
                field="max_slippage_bps",
                reason=f"Cannot be negative, got {self.max_slippage_bps}",
            )

        if self.max_slippage_bps > 10000:
            raise ConfigurationError(
                field="max_slippage_bps",
                reason=f"Exceeds maximum (10000 bps = 100%), got {self.max_slippage_bps}",
            )

        if self.base_retry_delay <= 0:
            raise ConfigurationError(
                field="base_retry_delay",
                reason=f"Must be positive, got {self.base_retry_delay}",
            )

        if self.max_retry_delay < self.base_retry_delay:
            raise ConfigurationError(
                field="max_retry_delay",
                reason=f"Must be >= base_retry_delay ({self.base_retry_delay}), got {self.max_retry_delay}",
            )

        if self.max_retries < 0:
            raise ConfigurationError(
                field="max_retries",
                reason=f"Cannot be negative, got {self.max_retries}",
            )

    @staticmethod
    def _mask_url(url: str) -> str:
        """Mask sensitive parts of URL for logging.

        Hides API keys and credentials in URLs.

        Args:
            url: URL to mask

        Returns:
            Masked URL safe for logging
        """
        if not url:
            return url

        # Mask API keys in query parameters
        masked = re.sub(
            r"(api[_-]?key|apikey|key|token)=([^&]+)",
            r"\1=***",
            url,
            flags=re.IGNORECASE,
        )

        # Mask credentials in URL (user:pass@host)
        masked = re.sub(
            r"://([^:]+):([^@]+)@",
            r"://\1:***@",
            masked,
        )

        return masked

    @classmethod
    def from_env(
        cls,
        chain: str | None = None,
        network: str = "mainnet",
        dotenv_path: str | None = None,
        prefix: str = "ALMANAK_",
    ) -> "LocalRuntimeConfig":
        """Create configuration from environment variables.

        Loads configuration from environment variables, optionally loading
        from a .env file first. Environment variable names are prefixed
        with the given prefix (default: ALMANAK_).

        This method supports two modes:
        1. **Explicit RPC URL**: Set {prefix}RPC_URL directly
        2. **Dynamic URL** (recommended): Set ALCHEMY_API_KEY and the URL is built automatically

        Environment Variables:
            {prefix}PRIVATE_KEY: Hex-encoded private key (required)
            {prefix}CHAIN: Blockchain network (e.g., "arbitrum") - optional if chain param provided
            {prefix}RPC_URL: RPC endpoint URL - optional if ALCHEMY_API_KEY is set
            ALCHEMY_API_KEY: Alchemy API key for dynamic URL building (recommended)
            {prefix}MAX_GAS_PRICE_GWEI: Optional, max gas price (default 100)
            {prefix}MAX_GAS_COST_NATIVE: Optional, max gas cost per tx in native token (default 0 = no limit)
            {prefix}MAX_GAS_COST_USD: Optional, max gas cost per tx in USD (default 0 = no limit)
            {prefix}MAX_SLIPPAGE_BPS: Optional, max acceptable swap slippage in basis points (default 0 = no limit)
            {prefix}TX_TIMEOUT_SECONDS: Optional, timeout (default 120)
            {prefix}SIMULATION_ENABLED: Optional, "true"/"false" (default true)
            {prefix}MAX_TX_VALUE_ETH: Optional, max tx value (default 10.0)
            {prefix}BASE_RETRY_DELAY: Optional, base delay (default 1.0)
            {prefix}MAX_RETRY_DELAY: Optional, max delay (default 32.0)
            {prefix}MAX_RETRIES: Optional, max retries (default 3)

        Args:
            chain: Optional chain name. If provided, overrides {prefix}CHAIN env var.
                   Useful when chain comes from strategy config.
            network: Network environment ("mainnet", "sepolia", "anvil"). Default: "mainnet"
            dotenv_path: Optional path to .env file to load
            prefix: Environment variable prefix (default "ALMANAK_")

        Returns:
            LocalRuntimeConfig instance

        Raises:
            MissingEnvironmentVariableError: If required env var is missing
            ConfigurationError: If env var value is invalid

        Example:
            # Minimal setup with ALCHEMY_API_KEY (recommended)
            # .env: ALCHEMY_API_KEY=xxx, ALMANAK_PRIVATE_KEY=0x...
            config = LocalRuntimeConfig.from_env(chain="arbitrum")

            # Legacy mode with explicit RPC URL
            # .env: ALMANAK_CHAIN=arbitrum, ALMANAK_RPC_URL=https://..., ALMANAK_PRIVATE_KEY=0x...
            config = LocalRuntimeConfig.from_env()

            # Local development with Anvil
            config = LocalRuntimeConfig.from_env(chain="arbitrum", network="anvil")
        """
        from almanak.gateway.utils.rpc_provider import get_rpc_url

        # Load .env file if specified or found in default locations
        if dotenv_path:
            load_dotenv(dotenv_path)
        else:
            load_dotenv()

        def get_required(name: str) -> str:
            """Get required environment variable (empty string treated as missing)."""
            full_name = f"{prefix}{name}"
            value = os.environ.get(full_name)
            if not value:
                raise MissingEnvironmentVariableError(full_name)
            return value

        def get_optional(name: str, default: str | None = None) -> str | None:
            """Get optional environment variable."""
            full_name = f"{prefix}{name}"
            return os.environ.get(full_name, default)

        def get_optional_int(name: str, default: int) -> int:
            """Get optional integer environment variable."""
            full_name = f"{prefix}{name}"
            value = os.environ.get(full_name)
            if value is None:
                return default
            try:
                return int(value)
            except ValueError:
                raise ConfigurationError(
                    field=full_name,
                    reason=f"Invalid integer value: {value}",
                ) from None

        def get_optional_float(name: str, default: float) -> float:
            """Get optional float environment variable."""
            full_name = f"{prefix}{name}"
            value = os.environ.get(full_name)
            if value is None:
                return default
            try:
                return float(value)
            except ValueError:
                raise ConfigurationError(
                    field=full_name,
                    reason=f"Invalid float value: {value}",
                ) from None

        def get_optional_bool(name: str, default: bool) -> bool:
            """Get optional boolean environment variable."""
            full_name = f"{prefix}{name}"
            value = os.environ.get(full_name)
            if value is None:
                return default
            return value.lower() in ("true", "1", "yes", "y")

        # VIB-308: Warn when unprefixed env vars are set -- silently ignored without prefix.
        _warn_unprefixed_env_vars(prefix)

        # Determine chain: parameter > env var (normalize to lowercase for lookups)
        resolved_chain = (chain or get_optional("CHAIN") or "").lower() or None
        if not resolved_chain:
            raise ConfigurationError(
                field="chain",
                reason="Chain must be provided via 'chain' parameter or ALMANAK_CHAIN env var",
            )

        # Determine RPC URL based on network setting
        # Priority when network="anvil": use local Anvil directly (skip env vars)
        # Priority when network="mainnet": chain-specific env var > generic env var > dynamic build
        rpc_url: str | None = None

        if network.lower() == "anvil":
            # Anvil mode: always use local fork URL, ignore env vars
            rpc_url = get_rpc_url(resolved_chain, network="anvil")
            logger.debug(f"Using Anvil RPC URL for {resolved_chain}: {rpc_url}")
        else:
            # Mainnet/testnet: check env vars first, then dynamic build
            # First try chain-specific (e.g., ALMANAK_ARBITRUM_RPC_URL for arbitrum)
            chain_specific_rpc_var = f"{prefix}{resolved_chain.upper()}_RPC_URL"
            rpc_url = os.environ.get(chain_specific_rpc_var)
            if rpc_url:
                logger.debug(f"Using chain-specific RPC URL from {chain_specific_rpc_var}")
            else:
                # Fall back to generic RPC_URL
                rpc_url = get_optional("RPC_URL")

            if not rpc_url:
                # Build dynamically via get_rpc_url (handles API keys, Tenderly, and free public RPCs)
                try:
                    rpc_url = get_rpc_url(resolved_chain, network=network)
                    logger.debug(f"Built RPC URL dynamically for {resolved_chain}")
                except ValueError as e:
                    raise ConfigurationError(
                        field="rpc_url",
                        reason=f"Could not build RPC URL: {e}. Set {prefix}RPC_URL, {prefix}{resolved_chain.upper()}_RPC_URL, ALCHEMY_API_KEY, or TENDERLY_API_KEY_{resolved_chain.upper()}.",
                    ) from None

        # VIB-303 + VIB-304: Use chain-specific gas price cap as default (not 100 gwei hardcoded).
        # In Anvil mode, gas costs no real money -- use ANVIL_GAS_PRICE_CAP_GWEI to prevent cap errors during dev.
        # On mainnet, use the chain-specific cap (Polygon=500, Ethereum=300, Arbitrum=10, etc.).
        if network.lower() == "anvil":
            default_gas_cap = ANVIL_GAS_PRICE_CAP_GWEI
        else:
            default_gas_cap = CHAIN_GAS_PRICE_CAPS_GWEI.get(resolved_chain, DEFAULT_GAS_PRICE_CAP_GWEI)

        # Get execution mode and create Safe signer if needed
        private_key = get_required("PRIVATE_KEY")
        mode_str = get_optional("EXECUTION_MODE", "eoa") or "eoa"
        execution_mode = ExecutionMode.from_string(mode_str)
        safe_signer = None
        if execution_mode in (ExecutionMode.SAFE_DIRECT, ExecutionMode.SAFE_ZODIAC):
            safe_signer = _create_safe_signer_from_env(
                execution_mode=execution_mode,
                private_key=private_key,
                prefix=prefix,
            )

        return cls(
            chain=resolved_chain,
            rpc_url=rpc_url,
            private_key=private_key,
            max_gas_price_gwei=get_optional_int("MAX_GAS_PRICE_GWEI", default_gas_cap),
            max_gas_cost_native=get_optional_float("MAX_GAS_COST_NATIVE", 0.0),
            max_gas_cost_usd=get_optional_float("MAX_GAS_COST_USD", 0.0),
            max_slippage_bps=get_optional_int("MAX_SLIPPAGE_BPS", 0),
            tx_timeout_seconds=get_optional_int("TX_TIMEOUT_SECONDS", 120),
            simulation_enabled=get_optional_bool("SIMULATION_ENABLED", True),
            max_tx_value_eth=get_optional_float("MAX_TX_VALUE_ETH", 10.0),
            base_retry_delay=get_optional_float("BASE_RETRY_DELAY", 1.0),
            max_retry_delay=get_optional_float("MAX_RETRY_DELAY", 32.0),
            max_retries=get_optional_int("MAX_RETRIES", 3),
            safe_signer=safe_signer,
        )

    def to_dict(self) -> dict[str, Any]:
        """Convert configuration to dictionary for serialization.

        SECURITY: Private key is NEVER included in the output.

        Returns:
            Dictionary representation safe for logging/serialization
        """
        return {
            "chain": self.chain,
            "chain_id": self.chain_id,
            "rpc_url": self._mask_url(self.rpc_url),
            "wallet_address": self.wallet_address,
            "max_gas_price_gwei": self.max_gas_price_gwei,
            "max_gas_cost_native": self.max_gas_cost_native,
            "max_gas_cost_usd": self.max_gas_cost_usd,
            "max_slippage_bps": self.max_slippage_bps,
            "tx_timeout_seconds": self.tx_timeout_seconds,
            "simulation_enabled": self.simulation_enabled,
            "max_tx_value_eth": self.max_tx_value_eth,
            "base_retry_delay": self.base_retry_delay,
            "max_retry_delay": self.max_retry_delay,
            "max_retries": self.max_retries,
            "created_at": self.created_at.isoformat(),
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "LocalRuntimeConfig":
        """Create configuration from dictionary.

        Note: private_key must be provided in the data dictionary.

        Args:
            data: Dictionary with configuration values

        Returns:
            LocalRuntimeConfig instance

        Raises:
            ConfigurationError: If required fields are missing
        """
        if "private_key" not in data:
            raise ConfigurationError(
                field="private_key",
                reason="private_key must be provided in data dictionary",
            )

        return cls(
            chain=data.get("chain", ""),
            rpc_url=data.get("rpc_url", ""),
            private_key=data["private_key"],
            max_gas_price_gwei=data.get("max_gas_price_gwei", 100),
            max_gas_cost_native=data.get("max_gas_cost_native", 0.0),
            max_gas_cost_usd=data.get("max_gas_cost_usd", 0.0),
            max_slippage_bps=data.get("max_slippage_bps", 0),
            tx_timeout_seconds=data.get("tx_timeout_seconds", 120),
            simulation_enabled=data.get("simulation_enabled", True),
            max_tx_value_eth=data.get("max_tx_value_eth", 10.0),
            base_retry_delay=data.get("base_retry_delay", 1.0),
            max_retry_delay=data.get("max_retry_delay", 32.0),
            max_retries=data.get("max_retries", 3),
        )

    def get_chain_enum(self) -> Chain:
        """Get the Chain enum value for this configuration.

        Returns:
            Chain enum value

        Example:
            config = LocalRuntimeConfig(chain="arbitrum", ...)
            chain_enum = config.get_chain_enum()  # Chain.ARBITRUM
        """
        return Chain(self.chain)

    @property
    def max_gas_price_wei(self) -> int:
        """Get max gas price in wei.

        Returns:
            Max gas price converted to wei
        """
        return self.max_gas_price_gwei * 10**9

    @property
    def max_tx_value_wei(self) -> int:
        """Get max transaction value in wei.

        Returns:
            Max transaction value converted to wei
        """
        return int(self.max_tx_value_eth * 10**18)

    @property
    def is_safe_mode(self) -> bool:
        """Check if Safe wallet mode is enabled.

        Returns:
            True if a Safe signer is configured
        """
        return self.safe_signer is not None

    @property
    def execution_address(self) -> str:
        """Get the address that will execute transactions.

        In Safe mode, this returns the Safe wallet address.
        Otherwise, returns the EOA wallet address.

        Returns:
            Address that will execute transactions
        """
        if self.safe_signer is not None:
            return self.safe_signer.address
        return self.wallet_address

    def __repr__(self) -> str:
        """Return string representation without exposing private key."""
        return (
            f"LocalRuntimeConfig("
            f"chain={self.chain!r}, "
            f"wallet_address={self.wallet_address!r}, "
            f"safe_mode={self.is_safe_mode}, "
            f"simulation_enabled={self.simulation_enabled})"
        )

    def __str__(self) -> str:
        """Return string representation without exposing private key."""
        return f"LocalRuntimeConfig(chain={self.chain}, wallet={self.wallet_address})"


# =============================================================================
# Multi-Chain Configuration
# =============================================================================


@dataclass
class MultiChainRuntimeConfig:
    """Configuration for multi-chain execution environments.

    This configuration class extends the single-chain model to support strategies
    that operate across multiple blockchain networks. It manages per-chain RPC URLs,
    protocol mappings, and provides a unified interface for multi-chain execution.

    Required Fields:
        chains: List of chain names to operate on (e.g., ['arbitrum', 'optimism', 'base'])
        protocols: Mapping of chain name to list of allowed protocols on that chain
        private_key: Hex-encoded private key for signing transactions (same key across all chains)

    Derived Fields:
        wallet_address: Ethereum address derived from private_key (same across all EVM chains)
        chain_ids: Mapping of chain name to numeric chain ID
        rpc_urls: Mapping of chain name to RPC URL (loaded from environment)
        primary_chain: The first chain in the chains list (used as default)

    Optional Fields:
        max_gas_price_gwei: Maximum gas price in gwei per chain (default 100)
        tx_timeout_seconds: Transaction confirmation timeout (default 120)
        simulation_enabled: Whether to simulate transactions before submission (default True)
        max_tx_value_eth: Maximum value per transaction in ETH (default 10.0)

    Environment Variables:
        Per-chain RPC URLs are loaded from ALMANAK_{CHAIN}_RPC_URL format:
            - ALMANAK_ARBITRUM_RPC_URL
            - ALMANAK_OPTIMISM_RPC_URL
            - ALMANAK_BASE_RPC_URL

    SECURITY CONTRACT:
        - private_key is NEVER included in __repr__, __str__, or to_dict()
        - private_key is NEVER logged
        - Wallet address is derived once at initialization and cached

    Example:
        # Create multi-chain config
        config = MultiChainRuntimeConfig(
            chains=['arbitrum', 'optimism', 'base'],
            protocols={
                'arbitrum': ['aave_v3', 'gmx_v2'],
                'optimism': ['aave_v3', 'uniswap_v3'],
                'base': ['uniswap_v3'],
            },
            private_key="0x...",
        )

        # Access per-chain settings
        print(config.rpc_urls['arbitrum'])  # https://arb1.arbitrum.io/rpc
        print(config.chain_ids['optimism'])  # 10

        # Backward compatibility: single chain usage
        config = MultiChainRuntimeConfig.from_single_chain(
            chain="arbitrum",
            protocols=["aave_v3", "gmx_v2"],
            private_key="0x...",
        )
    """

    # Required fields
    chains: list[str]
    protocols: dict[str, list[str]]
    private_key: str = field(repr=False)  # Never include in repr

    # Derived fields (set in __post_init__)
    wallet_address: str = field(default="", init=False)
    chain_ids: dict[str, int] = field(default_factory=dict, init=False)
    rpc_urls: dict[str, str] = field(default_factory=dict, init=False)
    primary_chain: str = field(default="", init=False)

    # Optional fields with defaults
    network: str = "mainnet"  # Network environment: "mainnet", "sepolia", or "anvil"
    max_gas_price_gwei: int = 100
    max_gas_cost_native: float = 0.0  # Max gas cost per tx in native token (0 = no limit)
    max_gas_cost_usd: float = 0.0  # Max gas cost per tx in USD (0 = no limit)
    max_slippage_bps: int = 0  # Max acceptable swap slippage in bps (0 = no limit)
    tx_timeout_seconds: int = 120
    simulation_enabled: bool = True
    max_tx_value_eth: float = 10.0
    base_retry_delay: float = 1.0
    max_retry_delay: float = 32.0
    max_retries: int = 3

    # Data freshness settings
    data_freshness_policy: DataFreshnessPolicy = "fail_closed"
    stale_data_threshold_seconds: float = 30.0  # Data older than this is considered stale

    # Safe wallet signer (optional - for executing via Safe multisig)
    # When set, ChainExecutors will use this signer for Safe-based execution
    # and multi-tx bundles (like approve + swap) will be atomic via MultiSend
    safe_signer: Optional["SafeSigner"] = None

    # Metadata
    created_at: datetime = field(default_factory=lambda: datetime.now(UTC))

    def __post_init__(self) -> None:
        """Validate configuration and derive fields after initialization."""
        # Validate chains list
        self._validate_chains()

        # Validate protocols mapping
        self._validate_protocols()

        # Validate and derive wallet address from private key
        self._validate_and_derive_wallet()

        # Load RPC URLs from environment (uses network setting for dynamic URL building)
        self._load_rpc_urls(network=self.network)

        # Validate optional fields
        self._validate_optional_fields()

        # Set primary chain (first in list)
        self.primary_chain = self.chains[0]

        logger.debug(
            "MultiChainRuntimeConfig initialized: chains=%s, wallet=%s",
            self.chains,
            self.wallet_address,
        )

    def _validate_chains(self) -> None:
        """Validate the chains list."""
        if not self.chains:
            raise ConfigurationError(
                field="chains",
                reason="At least one chain must be specified",
            )

        # Normalize and validate each chain
        normalized_chains: list[str] = []
        for chain in self.chains:
            if not chain:
                raise ConfigurationError(
                    field="chains",
                    reason="Chain name cannot be empty",
                )

            chain_lower = chain.lower()
            if chain_lower not in CHAIN_IDS:
                valid_chains = ", ".join(sorted(CHAIN_IDS.keys()))
                raise ConfigurationError(
                    field="chains",
                    reason=f"Unsupported chain '{chain}'. Valid chains: {valid_chains}",
                )

            if chain_lower in normalized_chains:
                raise ConfigurationError(
                    field="chains",
                    reason=f"Duplicate chain '{chain_lower}' in chains list",
                )

            normalized_chains.append(chain_lower)

        # Update with normalized chains
        self.chains = normalized_chains

        # Populate chain_ids
        self.chain_ids = {chain: CHAIN_IDS[chain] for chain in self.chains}

    def _validate_protocols(self) -> None:
        """Validate protocols mapping against chains and supported protocols."""
        if not self.protocols:
            raise ConfigurationError(
                field="protocols",
                reason="Protocols mapping cannot be empty",
            )

        # Normalize protocol keys to lowercase
        normalized_protocols: dict[str, list[str]] = {}

        for chain, protocol_list in self.protocols.items():
            chain_lower = chain.lower()

            # Ensure chain is in configured chains
            if chain_lower not in self.chains:
                raise ConfigurationError(
                    field="protocols",
                    reason=f"Protocol mapping for chain '{chain}' but chain not in configured chains: {self.chains}",
                )

            if not protocol_list:
                raise ConfigurationError(
                    field="protocols",
                    reason=f"Protocol list for chain '{chain}' cannot be empty",
                )

            # Validate each protocol
            validated_protocols: list[str] = []
            for protocol in protocol_list:
                protocol_lower = protocol.lower()

                # Check if protocol is known
                if protocol_lower not in SUPPORTED_PROTOCOLS:
                    valid_protocols = ", ".join(sorted(SUPPORTED_PROTOCOLS.keys()))
                    raise ConfigurationError(
                        field="protocols",
                        reason=f"Unknown protocol '{protocol}'. Valid protocols: {valid_protocols}",
                    )

                # Check if protocol is available on this chain
                if chain_lower not in SUPPORTED_PROTOCOLS[protocol_lower]:
                    available_chains = ", ".join(sorted(SUPPORTED_PROTOCOLS[protocol_lower]))
                    raise ConfigurationError(
                        field="protocols",
                        reason=f"Protocol '{protocol}' is not available on chain '{chain}'. "
                        f"Available on: {available_chains}",
                    )

                if protocol_lower in validated_protocols:
                    raise ConfigurationError(
                        field="protocols",
                        reason=f"Duplicate protocol '{protocol}' for chain '{chain}'",
                    )

                validated_protocols.append(protocol_lower)

            normalized_protocols[chain_lower] = validated_protocols

        # Ensure all configured chains have protocol mappings
        for chain in self.chains:
            if chain not in normalized_protocols:
                raise ConfigurationError(
                    field="protocols",
                    reason=f"No protocols configured for chain '{chain}'",
                )

        self.protocols = normalized_protocols

    def _validate_and_derive_wallet(self) -> None:
        """Validate private key and derive wallet address."""
        if not self.private_key:
            raise ConfigurationError(
                field="private_key",
                reason="Private key cannot be empty",
            )

        # Normalize private key format
        key = self.private_key
        if not key.startswith("0x"):
            key = "0x" + key

        # Validate hex format
        if len(key) != 66:  # 0x + 64 hex chars
            raise ConfigurationError(
                field="private_key",
                reason="Private key must be 32 bytes (64 hex characters)",
            )

        try:
            # Derive wallet address from private key
            account = Account.from_key(key)
            self.wallet_address = account.address
        except Exception:
            # Never expose key material in error messages
            raise ConfigurationError(
                field="private_key",
                reason="Invalid private key format",
            ) from None

    def _load_rpc_urls(self, network: str = "mainnet") -> None:
        """Load RPC URLs for each configured chain.

        This method supports two modes:
        1. **Anvil mode** (network="anvil"): Use local Anvil URLs directly, skip env vars
        2. **Mainnet mode**: Explicit per-chain URLs > dynamic URL building

        Args:
            network: Network environment ("mainnet", "sepolia", "anvil"). Default: "mainnet"
        """
        from almanak.gateway.utils.rpc_provider import get_rpc_url

        load_dotenv()

        is_anvil = network.lower() == "anvil"

        for chain in self.chains:
            env_var = f"ALMANAK_{chain.upper()}_RPC_URL"

            if is_anvil:
                # Anvil mode: always use local fork URL, skip env vars
                rpc_url = get_rpc_url(chain, network="anvil")
                self.rpc_urls[chain] = rpc_url
                logger.debug(f"Using Anvil RPC URL for {chain}: {rpc_url}")

            else:
                # Mainnet/testnet: try explicit per-chain URL first
                rpc_url_env = os.environ.get(env_var)

                if rpc_url_env:
                    # Validate URL format
                    url_pattern = re.compile(
                        r"^https?://"  # http:// or https://
                        r"[a-zA-Z0-9.-]+"  # domain
                        r"(:\d+)?"  # optional port
                        r"(/.*)?$"  # optional path
                    )
                    if not url_pattern.match(rpc_url_env):
                        raise ConfigurationError(
                            field=env_var,
                            reason=f"Invalid RPC URL format: {self._mask_url(rpc_url_env)}",
                        )
                    self.rpc_urls[chain] = rpc_url_env
                    logger.debug(f"Using explicit RPC URL for {chain}")

                else:
                    # Build dynamically via get_rpc_url (handles API keys, Tenderly, and free public RPCs)
                    try:
                        rpc_url = get_rpc_url(chain, network=network)
                        self.rpc_urls[chain] = rpc_url
                        logger.debug(f"Built RPC URL dynamically for {chain}")
                    except ValueError as e:
                        raise ConfigurationError(
                            field=env_var,
                            reason=f"Could not build RPC URL for {chain}: {e}. Set {env_var}, RPC_URL, ALCHEMY_API_KEY, or TENDERLY_API_KEY_{chain.upper()}.",
                        ) from None

    def _validate_optional_fields(self) -> None:
        """Validate optional fields with defaults."""
        if self.max_gas_price_gwei <= 0:
            raise ConfigurationError(
                field="max_gas_price_gwei",
                reason=f"Must be positive, got {self.max_gas_price_gwei}",
            )

        if self.max_gas_price_gwei > 10000:
            raise ConfigurationError(
                field="max_gas_price_gwei",
                reason=f"Exceeds maximum (10000 gwei), got {self.max_gas_price_gwei}",
            )

        if self.tx_timeout_seconds <= 0:
            raise ConfigurationError(
                field="tx_timeout_seconds",
                reason=f"Must be positive, got {self.tx_timeout_seconds}",
            )

        if self.tx_timeout_seconds > 600:
            raise ConfigurationError(
                field="tx_timeout_seconds",
                reason=f"Exceeds maximum (600 seconds), got {self.tx_timeout_seconds}",
            )

        if self.max_tx_value_eth < 0:
            raise ConfigurationError(
                field="max_tx_value_eth",
                reason=f"Cannot be negative, got {self.max_tx_value_eth}",
            )

        if self.max_gas_cost_native < 0:
            raise ConfigurationError(
                field="max_gas_cost_native",
                reason=f"Cannot be negative, got {self.max_gas_cost_native}",
            )

        if self.max_gas_cost_usd < 0:
            raise ConfigurationError(
                field="max_gas_cost_usd",
                reason=f"Cannot be negative, got {self.max_gas_cost_usd}",
            )

        if self.max_slippage_bps < 0:
            raise ConfigurationError(
                field="max_slippage_bps",
                reason=f"Cannot be negative, got {self.max_slippage_bps}",
            )

        if self.max_slippage_bps > 10000:
            raise ConfigurationError(
                field="max_slippage_bps",
                reason=f"Exceeds maximum (10000 bps = 100%), got {self.max_slippage_bps}",
            )

        if self.base_retry_delay <= 0:
            raise ConfigurationError(
                field="base_retry_delay",
                reason=f"Must be positive, got {self.base_retry_delay}",
            )

        if self.max_retry_delay < self.base_retry_delay:
            raise ConfigurationError(
                field="max_retry_delay",
                reason=f"Must be >= base_retry_delay ({self.base_retry_delay}), got {self.max_retry_delay}",
            )

        if self.max_retries < 0:
            raise ConfigurationError(
                field="max_retries",
                reason=f"Cannot be negative, got {self.max_retries}",
            )

        # Validate data freshness settings
        if self.data_freshness_policy not in ("fail_closed", "fail_open"):
            raise ConfigurationError(
                field="data_freshness_policy",
                reason=f"Must be 'fail_closed' or 'fail_open', got '{self.data_freshness_policy}'",
            )

        if self.stale_data_threshold_seconds <= 0:
            raise ConfigurationError(
                field="stale_data_threshold_seconds",
                reason=f"Must be positive, got {self.stale_data_threshold_seconds}",
            )

    @staticmethod
    def _mask_url(url: str) -> str:
        """Mask sensitive parts of URL for logging."""
        if not url:
            return url

        # Mask API keys in query parameters
        masked = re.sub(
            r"(api[_-]?key|apikey|key|token)=([^&]+)",
            r"\1=***",
            url,
            flags=re.IGNORECASE,
        )

        # Mask credentials in URL (user:pass@host)
        masked = re.sub(
            r"://([^:]+):([^@]+)@",
            r"://\1:***@",
            masked,
        )

        return masked

    @classmethod
    def from_single_chain(
        cls,
        chain: str,
        protocols: list[str],
        private_key: str,
        **kwargs: Any,
    ) -> "MultiChainRuntimeConfig":
        """Create a multi-chain config from a single chain for backward compatibility.

        This factory method allows existing single-chain configurations to be
        easily migrated to the multi-chain format.

        Args:
            chain: Single chain name (e.g., "arbitrum")
            protocols: List of protocols for this chain
            private_key: Hex-encoded private key
            **kwargs: Additional optional parameters (max_gas_price_gwei, etc.)

        Returns:
            MultiChainRuntimeConfig instance configured for a single chain

        Example:
            # Convert from old single-chain pattern
            config = MultiChainRuntimeConfig.from_single_chain(
                chain="arbitrum",
                protocols=["aave_v3", "gmx_v2"],
                private_key="0x...",
            )
        """
        return cls(
            chains=[chain],
            protocols={chain: protocols},
            private_key=private_key,
            **kwargs,
        )

    @classmethod
    def create(
        cls,
        chains: list[str],
        protocols: dict[str, list[str]],
        private_key: str,
        execution_mode: str = "eoa",
        safe_address: str | None = None,
        zodiac_address: str | None = None,
        signer_service_url: str | None = None,
        signer_service_jwt: str | None = None,
        **kwargs: Any,
    ) -> "MultiChainRuntimeConfig":
        """Create configuration with explicit execution mode.

        This factory method provides a simple way to create a configuration
        with the desired execution mode without using environment variables.

        Args:
            chains: List of chain names to operate on
            protocols: Mapping of chain name to list of protocols
            private_key: Hex-encoded private key
            execution_mode: "eoa", "safe_direct", or "safe_zodiac"
            safe_address: Safe wallet address (required for safe_* modes)
            zodiac_address: Zodiac Roles address (required for safe_zodiac)
            signer_service_url: Remote signer URL (required for safe_zodiac)
            signer_service_jwt: JWT for signer (required for safe_zodiac)
            **kwargs: Additional optional parameters

        Returns:
            MultiChainRuntimeConfig instance

        Raises:
            ConfigurationError: If required parameters are missing for the mode

        Example:
            # EOA mode
            config = MultiChainRuntimeConfig.create(
                chains=["arbitrum"],
                protocols={"arbitrum": ["enso"]},
                private_key="0x...",
                execution_mode="eoa",
            )

            # Safe direct mode (for Anvil testing)
            config = MultiChainRuntimeConfig.create(
                chains=["arbitrum"],
                protocols={"arbitrum": ["enso"]},
                private_key="0x...",
                execution_mode="safe_direct",
                safe_address="0x...",
            )

            # Safe Zodiac mode (production)
            config = MultiChainRuntimeConfig.create(
                chains=["arbitrum"],
                protocols={"arbitrum": ["enso"]},
                private_key="0x...",
                execution_mode="safe_zodiac",
                safe_address="0x...",
                zodiac_address="0x...",
                signer_service_url="https://...",
                signer_service_jwt="...",
            )
        """
        from almanak.framework.execution.signer.safe import (
            DirectSafeSigner,
            SafeSignerConfig,
            SafeWalletConfig,
            ZodiacRolesSigner,
        )

        mode = ExecutionMode.from_string(execution_mode)

        # Create Safe signer if needed
        safe_signer: DirectSafeSigner | ZodiacRolesSigner | None = None

        if mode == ExecutionMode.SAFE_DIRECT:
            if not safe_address:
                raise ConfigurationError(
                    field="safe_address",
                    reason="safe_address is required for safe_direct mode",
                )

            account = Account.from_key(private_key)
            wallet_config = SafeWalletConfig(
                safe_address=safe_address,
                eoa_address=account.address,
            )
            signer_config = SafeSignerConfig(
                mode="direct",
                wallet_config=wallet_config,
                private_key=private_key,
            )
            safe_signer = DirectSafeSigner(signer_config)

        elif mode == ExecutionMode.SAFE_ZODIAC:
            if not safe_address:
                raise ConfigurationError(
                    field="safe_address",
                    reason="safe_address is required for safe_zodiac mode",
                )
            if not zodiac_address:
                raise ConfigurationError(
                    field="zodiac_address",
                    reason="zodiac_address is required for safe_zodiac mode",
                )
            if not signer_service_url:
                raise ConfigurationError(
                    field="signer_service_url",
                    reason="signer_service_url is required for safe_zodiac mode",
                )
            if not signer_service_jwt:
                raise ConfigurationError(
                    field="signer_service_jwt",
                    reason="signer_service_jwt is required for safe_zodiac mode",
                )

            account = Account.from_key(private_key)
            wallet_config = SafeWalletConfig(
                safe_address=safe_address,
                eoa_address=account.address,
                zodiac_roles_address=zodiac_address,
            )
            signer_config = SafeSignerConfig(
                mode="zodiac",
                wallet_config=wallet_config,
                private_key=private_key,
                signer_service_url=signer_service_url,
                signer_service_jwt=signer_service_jwt,
            )
            safe_signer = ZodiacRolesSigner(signer_config)

        return cls(
            chains=chains,
            protocols=protocols,
            private_key=private_key,
            safe_signer=safe_signer,
            **kwargs,
        )

    @classmethod
    def from_env(
        cls,
        chains: list[str],
        protocols: dict[str, list[str]],
        network: str = "mainnet",
        dotenv_path: str | None = None,
        prefix: str = "ALMANAK_",
    ) -> "MultiChainRuntimeConfig":
        """Create configuration from environment variables.

        Loads private key, execution mode, and optional settings from environment
        variables. RPC URLs are loaded per-chain from ALMANAK_{CHAIN}_RPC_URL format.

        Environment Variables:
            {prefix}PRIVATE_KEY: Hex-encoded private key (required)
            {prefix}EXECUTION_MODE: Execution mode - "eoa", "safe_direct", "safe_zodiac"
                                    (default: "eoa")
            {prefix}SAFE_ADDRESS: Safe wallet address (required for safe_* modes)
            {prefix}ZODIAC_ADDRESS: Zodiac Roles module address (required for safe_zodiac)
            {prefix}SIGNER_SERVICE_URL: Remote signer URL (required for safe_zodiac)
            {prefix}SIGNER_SERVICE_JWT: JWT for signer service (required for safe_zodiac)
            {prefix}MAX_GAS_PRICE_GWEI: Optional, max gas price (default 100)
            {prefix}MAX_GAS_COST_NATIVE: Optional, max gas cost per tx in native token (default 0 = no limit)
            {prefix}MAX_GAS_COST_USD: Optional, max gas cost per tx in USD (default 0 = no limit)
            {prefix}MAX_SLIPPAGE_BPS: Optional, max acceptable swap slippage in basis points (default 0 = no limit)
            {prefix}TX_TIMEOUT_SECONDS: Optional, timeout (default 120)
            {prefix}SIMULATION_ENABLED: Optional, "true"/"false" (default true)
            {prefix}MAX_TX_VALUE_ETH: Optional, max tx value (default 10.0)
            ALMANAK_{CHAIN}_RPC_URL: RPC URL for each chain (required per chain)

        Args:
            chains: List of chain names to configure
            protocols: Protocol mapping per chain
            network: Network environment ("mainnet", "sepolia", "anvil"). Default: "mainnet"
            dotenv_path: Optional path to .env file to load
            prefix: Environment variable prefix (default "ALMANAK_")

        Returns:
            MultiChainRuntimeConfig instance

        Example:
            # EOA mode (default)
            export ALMANAK_EXECUTION_MODE=eoa
            config = MultiChainRuntimeConfig.from_env(...)

            # Safe direct mode (for Anvil testing)
            export ALMANAK_EXECUTION_MODE=safe_direct
            export ALMANAK_SAFE_ADDRESS=0x...
            config = MultiChainRuntimeConfig.from_env(...)

            # Safe Zodiac mode (production)
            export ALMANAK_EXECUTION_MODE=safe_zodiac
            export ALMANAK_SAFE_ADDRESS=0x...
            export ALMANAK_ZODIAC_ADDRESS=0x...
            export ALMANAK_SIGNER_SERVICE_URL=https://...
            export ALMANAK_SIGNER_SERVICE_JWT=...
            config = MultiChainRuntimeConfig.from_env(...)
        """
        # Load .env file if specified or found in default locations
        if dotenv_path:
            load_dotenv(dotenv_path)
        else:
            load_dotenv()

        def get_required(name: str) -> str:
            """Get required environment variable (empty string treated as missing)."""
            full_name = f"{prefix}{name}"
            value = os.environ.get(full_name)
            if not value:
                raise MissingEnvironmentVariableError(full_name)
            return value

        def get_optional(name: str, default: str | None = None) -> str | None:
            """Get optional environment variable."""
            full_name = f"{prefix}{name}"
            return os.environ.get(full_name, default)

        def get_optional_int(name: str, default: int) -> int:
            """Get optional integer environment variable."""
            full_name = f"{prefix}{name}"
            value = os.environ.get(full_name)
            if value is None:
                return default
            try:
                return int(value)
            except ValueError:
                raise ConfigurationError(
                    field=full_name,
                    reason=f"Invalid integer value: {value}",
                ) from None

        def get_optional_float(name: str, default: float) -> float:
            """Get optional float environment variable."""
            full_name = f"{prefix}{name}"
            value = os.environ.get(full_name)
            if value is None:
                return default
            try:
                return float(value)
            except ValueError:
                raise ConfigurationError(
                    field=full_name,
                    reason=f"Invalid float value: {value}",
                ) from None

        def get_optional_bool(name: str, default: bool) -> bool:
            """Get optional boolean environment variable."""
            full_name = f"{prefix}{name}"
            value = os.environ.get(full_name)
            if value is None:
                return default
            return value.lower() in ("true", "1", "yes", "y")

        # VIB-308: Warn when unprefixed env vars are set -- silently ignored without prefix.
        _warn_unprefixed_env_vars(prefix)

        # Get private key (always required)
        private_key = get_required("PRIVATE_KEY")

        # Get execution mode (default: EOA)
        mode_str = get_optional("EXECUTION_MODE", "eoa") or "eoa"
        execution_mode = ExecutionMode.from_string(mode_str)

        # Create Safe signer if needed
        safe_signer = None
        if execution_mode in (ExecutionMode.SAFE_DIRECT, ExecutionMode.SAFE_ZODIAC):
            safe_signer = _create_safe_signer_from_env(
                execution_mode=execution_mode,
                private_key=private_key,
                prefix=prefix,
            )

        # VIB-303 + VIB-304: Use chain-aware gas price cap as default.
        # In Anvil mode, gas costs no real money -- use ANVIL_GAS_PRICE_CAP_GWEI to prevent cap errors during dev.
        # For multi-chain mainnet, use DEFAULT_GAS_PRICE_CAP_GWEI (conservative but correct for all chains).
        if network.lower() == "anvil":
            multi_default_gas_cap = ANVIL_GAS_PRICE_CAP_GWEI
        else:
            multi_default_gas_cap = DEFAULT_GAS_PRICE_CAP_GWEI

        return cls(
            chains=chains,
            protocols=protocols,
            private_key=private_key,
            safe_signer=safe_signer,
            network=network,
            max_gas_price_gwei=get_optional_int("MAX_GAS_PRICE_GWEI", multi_default_gas_cap),
            max_gas_cost_native=get_optional_float("MAX_GAS_COST_NATIVE", 0.0),
            max_gas_cost_usd=get_optional_float("MAX_GAS_COST_USD", 0.0),
            max_slippage_bps=get_optional_int("MAX_SLIPPAGE_BPS", 0),
            tx_timeout_seconds=get_optional_int("TX_TIMEOUT_SECONDS", 120),
            simulation_enabled=get_optional_bool("SIMULATION_ENABLED", True),
            max_tx_value_eth=get_optional_float("MAX_TX_VALUE_ETH", 10.0),
            base_retry_delay=get_optional_float("BASE_RETRY_DELAY", 1.0),
            max_retry_delay=get_optional_float("MAX_RETRY_DELAY", 32.0),
            max_retries=get_optional_int("MAX_RETRIES", 3),
        )

    def to_dict(self) -> dict[str, Any]:
        """Convert configuration to dictionary for serialization.

        SECURITY: Private key is NEVER included in the output.

        Returns:
            Dictionary representation safe for logging/serialization
        """
        return {
            "chains": self.chains,
            "protocols": self.protocols,
            "chain_ids": self.chain_ids,
            "rpc_urls": {chain: self._mask_url(url) for chain, url in self.rpc_urls.items()},
            "wallet_address": self.wallet_address,
            "execution_address": self.execution_address,
            "primary_chain": self.primary_chain,
            "max_gas_price_gwei": self.max_gas_price_gwei,
            "max_gas_cost_native": self.max_gas_cost_native,
            "max_gas_cost_usd": self.max_gas_cost_usd,
            "max_slippage_bps": self.max_slippage_bps,
            "tx_timeout_seconds": self.tx_timeout_seconds,
            "simulation_enabled": self.simulation_enabled,
            "max_tx_value_eth": self.max_tx_value_eth,
            "base_retry_delay": self.base_retry_delay,
            "max_retry_delay": self.max_retry_delay,
            "max_retries": self.max_retries,
            "data_freshness_policy": self.data_freshness_policy,
            "stale_data_threshold_seconds": self.stale_data_threshold_seconds,
            "safe_mode": self.is_safe_mode,
            "created_at": self.created_at.isoformat(),
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "MultiChainRuntimeConfig":
        """Create configuration from dictionary.

        Note: private_key must be provided in the data dictionary.

        Args:
            data: Dictionary with configuration values

        Returns:
            MultiChainRuntimeConfig instance

        Raises:
            ConfigurationError: If required fields are missing
        """
        if "private_key" not in data:
            raise ConfigurationError(
                field="private_key",
                reason="private_key must be provided in data dictionary",
            )

        return cls(
            chains=data.get("chains", []),
            protocols=data.get("protocols", {}),
            private_key=data["private_key"],
            max_gas_price_gwei=data.get("max_gas_price_gwei", 100),
            max_gas_cost_native=data.get("max_gas_cost_native", 0.0),
            max_gas_cost_usd=data.get("max_gas_cost_usd", 0.0),
            max_slippage_bps=data.get("max_slippage_bps", 0),
            tx_timeout_seconds=data.get("tx_timeout_seconds", 120),
            simulation_enabled=data.get("simulation_enabled", True),
            max_tx_value_eth=data.get("max_tx_value_eth", 10.0),
            base_retry_delay=data.get("base_retry_delay", 1.0),
            max_retry_delay=data.get("max_retry_delay", 32.0),
            max_retries=data.get("max_retries", 3),
            data_freshness_policy=data.get("data_freshness_policy", "fail_closed"),
            stale_data_threshold_seconds=data.get("stale_data_threshold_seconds", 30.0),
        )

    def get_rpc_url(self, chain: str) -> str:
        """Get RPC URL for a specific chain.

        Args:
            chain: Chain name

        Returns:
            RPC URL for the chain

        Raises:
            ConfigurationError: If chain is not configured
        """
        chain_lower = chain.lower()
        if chain_lower not in self.rpc_urls:
            raise ConfigurationError(
                field="chain",
                reason=f"Chain '{chain}' is not configured. Configured chains: {self.chains}",
            )
        return self.rpc_urls[chain_lower]

    def get_chain_id(self, chain: str) -> int:
        """Get chain ID for a specific chain.

        Args:
            chain: Chain name

        Returns:
            Numeric chain ID

        Raises:
            ConfigurationError: If chain is not configured
        """
        chain_lower = chain.lower()
        if chain_lower not in self.chain_ids:
            raise ConfigurationError(
                field="chain",
                reason=f"Chain '{chain}' is not configured. Configured chains: {self.chains}",
            )
        return self.chain_ids[chain_lower]

    def get_protocols_for_chain(self, chain: str) -> list[str]:
        """Get list of protocols configured for a specific chain.

        Args:
            chain: Chain name

        Returns:
            List of protocol names

        Raises:
            ConfigurationError: If chain is not configured
        """
        chain_lower = chain.lower()
        if chain_lower not in self.protocols:
            raise ConfigurationError(
                field="chain",
                reason=f"Chain '{chain}' is not configured. Configured chains: {self.chains}",
            )
        return self.protocols[chain_lower]

    def is_protocol_available(self, protocol: str, chain: str) -> bool:
        """Check if a protocol is available on a specific chain.

        Args:
            protocol: Protocol name
            chain: Chain name

        Returns:
            True if protocol is configured for the chain
        """
        chain_lower = chain.lower()
        protocol_lower = protocol.lower()

        if chain_lower not in self.protocols:
            return False

        return protocol_lower in self.protocols[chain_lower]

    def to_local_config(self, chain: str) -> LocalRuntimeConfig:
        """Create a LocalRuntimeConfig for a specific chain.

        Useful for interacting with single-chain components that expect
        a LocalRuntimeConfig.

        Args:
            chain: Chain to create config for

        Returns:
            LocalRuntimeConfig for the specified chain

        Raises:
            ConfigurationError: If chain is not configured
        """
        chain_lower = chain.lower()
        if chain_lower not in self.chains:
            raise ConfigurationError(
                field="chain",
                reason=f"Chain '{chain}' is not configured. Configured chains: {self.chains}",
            )

        return LocalRuntimeConfig(
            chain=chain_lower,
            rpc_url=self.rpc_urls[chain_lower],
            private_key=self.private_key,
            max_gas_price_gwei=self.max_gas_price_gwei,
            max_gas_cost_native=self.max_gas_cost_native,
            max_gas_cost_usd=self.max_gas_cost_usd,
            max_slippage_bps=self.max_slippage_bps,
            tx_timeout_seconds=self.tx_timeout_seconds,
            simulation_enabled=self.simulation_enabled,
            max_tx_value_eth=self.max_tx_value_eth,
            base_retry_delay=self.base_retry_delay,
            max_retry_delay=self.max_retry_delay,
            max_retries=self.max_retries,
            safe_signer=self.safe_signer,
        )

    @property
    def max_gas_price_wei(self) -> int:
        """Get max gas price in wei.

        Returns:
            Max gas price converted to wei
        """
        return self.max_gas_price_gwei * 10**9

    @property
    def max_tx_value_wei(self) -> int:
        """Get max transaction value in wei.

        Returns:
            Max transaction value converted to wei
        """
        return int(self.max_tx_value_eth * 10**18)

    @property
    def is_safe_mode(self) -> bool:
        """Check if Safe wallet mode is enabled.

        Returns:
            True if a Safe signer is configured
        """
        return self.safe_signer is not None

    @property
    def execution_address(self) -> str:
        """Get the address that will execute transactions.

        In Safe mode, this returns the Safe wallet address.
        Otherwise, returns the EOA wallet address.

        Returns:
            Address that will execute transactions
        """
        if self.safe_signer is not None:
            return self.safe_signer.address
        return self.wallet_address

    def __repr__(self) -> str:
        """Return string representation without exposing private key."""
        return (
            f"MultiChainRuntimeConfig("
            f"chains={self.chains!r}, "
            f"protocols={list(self.protocols.keys())!r}, "
            f"wallet_address={self.wallet_address!r}, "
            f"safe_mode={self.is_safe_mode}, "
            f"simulation_enabled={self.simulation_enabled})"
        )

    def __str__(self) -> str:
        """Return string representation without exposing private key."""
        return f"MultiChainRuntimeConfig(chains={self.chains}, wallet={self.wallet_address})"


# =============================================================================
# Safe Signer Factory
# =============================================================================


def _create_safe_signer_from_env(
    execution_mode: ExecutionMode,
    private_key: str,
    prefix: str = "ALMANAK_",
) -> "SafeSigner":
    """Create a Safe signer from environment variables.

    This is a helper function used by MultiChainRuntimeConfig.from_env() to
    create the appropriate Safe signer based on the execution mode.

    Args:
        execution_mode: The execution mode (SAFE_DIRECT or SAFE_ZODIAC)
        private_key: The private key for signing
        prefix: Environment variable prefix

    Returns:
        SafeSigner instance (DirectSafeSigner or ZodiacRolesSigner)

    Raises:
        MissingEnvironmentVariableError: If required env vars are missing
        ConfigurationError: If configuration is invalid
    """
    from almanak.framework.execution.signer.safe import (
        DirectSafeSigner,
        SafeSignerConfig,
        SafeWalletConfig,
        ZodiacRolesSigner,
    )

    def get_required(name: str) -> str:
        """Get required environment variable (empty string treated as missing)."""
        full_name = f"{prefix}{name}"
        value = os.environ.get(full_name)
        if not value:
            raise MissingEnvironmentVariableError(full_name)
        return value

    def get_optional(name: str) -> str | None:
        """Get optional environment variable."""
        full_name = f"{prefix}{name}"
        return os.environ.get(full_name)

    # Get Safe address (required for all Safe modes)
    safe_address = get_required("SAFE_ADDRESS")

    # Derive EOA address from private key
    account = Account.from_key(private_key)
    eoa_address = account.address

    if execution_mode == ExecutionMode.SAFE_DIRECT:
        # Direct mode - local signing for Anvil testing
        wallet_config = SafeWalletConfig(
            safe_address=safe_address,
            eoa_address=eoa_address,
        )
        signer_config = SafeSignerConfig(
            mode="direct",
            wallet_config=wallet_config,
            private_key=private_key,
        )
        logger.info(f"Creating DirectSafeSigner: safe={safe_address[:10]}..., eoa={eoa_address[:10]}...")
        return DirectSafeSigner(signer_config)

    elif execution_mode == ExecutionMode.SAFE_ZODIAC:
        # Zodiac mode - remote signing for production
        zodiac_address = get_required("ZODIAC_ADDRESS")
        signer_service_url = get_required("SIGNER_SERVICE_URL")
        signer_service_jwt = get_required("SIGNER_SERVICE_JWT")

        wallet_config = SafeWalletConfig(
            safe_address=safe_address,
            eoa_address=eoa_address,
            zodiac_roles_address=zodiac_address,
        )
        signer_config = SafeSignerConfig(
            mode="zodiac",
            wallet_config=wallet_config,
            private_key=private_key,
            signer_service_url=signer_service_url,
            signer_service_jwt=signer_service_jwt,
        )
        logger.info(f"Creating ZodiacRolesSigner: safe={safe_address[:10]}..., zodiac={zodiac_address[:10]}...")
        return ZodiacRolesSigner(signer_config)

    else:
        raise ConfigurationError(
            field="execution_mode",
            reason=f"Cannot create Safe signer for mode: {execution_mode}",
        )


# =============================================================================
# Exports
# =============================================================================


__all__ = [
    "LocalRuntimeConfig",
    "MultiChainRuntimeConfig",
    "ConfigurationError",
    "MissingEnvironmentVariableError",
    "ExecutionMode",
    "CHAIN_IDS",
    "SUPPORTED_PROTOCOLS",
    "DataFreshnessPolicy",
]
