"""Local Runtime Configuration for Execution Environment.

This module hosts the runtime-config dataclass containers consumed by the
runner / executor / multi-chain orchestrator. Phase 5a-2 of the
config-service migration moved the env-reading entry points to
:mod:`almanak.config.runtime`; this module no longer reads ``os.environ`` /
``load_dotenv`` from its public API. The ``from_env`` classmethods are
gone — consumers now call :func:`almanak.config.runtime.runtime_config_from_env`
and convert via :meth:`LocalRuntimeConfig.from_runtime_config` /
:meth:`MultiChainRuntimeConfig.from_runtime_config`.

What stays here:
    * The dataclass containers themselves (``LocalRuntimeConfig``,
      ``MultiChainRuntimeConfig``, ``GatewayRuntimeConfig``).
    * Direct-construction validation in ``__post_init__``.
    * Direct-construction helpers: ``from_dict``, ``from_single_chain``,
      ``create``, ``to_dict``, ``to_local_config``, properties, etc.

What moved to :mod:`almanak.config.runtime`:
    * ``runtime_config_from_env`` (the env-reading factory).
    * ``_resolve_private_key_from_env``, ``_create_safe_signer_from_env``.
    * ``CHAIN_IDS``, ``ConfigurationError``, ``MissingEnvironmentVariableError``,
      ``DataFreshnessPolicy``, ``ExecutionMode`` are RE-EXPORTED here for
      back-compat — they have a single canonical home in ``almanak.config.runtime``.

Example:
    # Build a runtime config from env (Phase 5a-2 entry point):
    from almanak.config.runtime import runtime_config_from_env
    rc = runtime_config_from_env(chain="arbitrum", network="anvil")

    # Convert to the dataclass shape consumed by the runner / executor:
    config = LocalRuntimeConfig.from_runtime_config(rc)

    # Or construct the dataclass directly for tests:
    config = LocalRuntimeConfig(
        chain="arbitrum",
        rpc_url="https://arb1.arbitrum.io/rpc",
        private_key="0x...",
    )
"""

import logging
import re
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any, Optional

from eth_account import Account

# The Phase-5a-1 hack of importing the legacy dataclasses from
# almanak.config.runtime is reversed: this module now imports the canonical
# types FROM the config service. ``CHAIN_IDS`` / ``ConfigurationError`` /
# ``MissingEnvironmentVariableError`` / ``DataFreshnessPolicy`` /
# ``ExecutionMode`` live in almanak.config.runtime and are re-exported here.
from almanak.config.runtime import (
    CHAIN_IDS,
    ConfigurationError,
    DataFreshnessPolicy,
    ExecutionMode,
    MissingEnvironmentVariableError,
    RuntimeConfig,
    gateway_wallets_configured,
    multi_chain_rpc_urls_from_env,
)
from almanak.framework.execution.interfaces import Chain


# Imported here so callers can reference without touching chain_executor directly.
# Populated after chain_executor module is available (lazy import to avoid circular deps).
def _default_receipt_timeout(chain: str) -> int:
    """Return the default receipt timeout in seconds for a given chain.

    Slow chains (BSC, Avalanche) need longer timeouts on Anvil forks.
    Users can still override per-strategy with TX_TIMEOUT_SECONDS.
    """
    from almanak.framework.execution.chain_executor import CHAIN_RECEIPT_TIMEOUTS, DEFAULT_RECEIPT_TIMEOUT

    return CHAIN_RECEIPT_TIMEOUTS.get(chain.lower(), DEFAULT_RECEIPT_TIMEOUT)


if TYPE_CHECKING:
    from almanak.framework.execution.signer.safe import SafeSigner

logger = logging.getLogger(__name__)

# Supported protocols and which chains they are available on
SUPPORTED_PROTOCOLS: dict[str, set[str]] = {
    "aave_v3": {"ethereum", "arbitrum", "optimism", "polygon", "base", "avalanche", "bsc", "linea", "plasma", "xlayer"},
    "uniswap_v3": {
        "ethereum",
        "arbitrum",
        "optimism",
        "polygon",
        "base",
        "avalanche",
        "bsc",
        "linea",
        "blast",
        "monad",
        "xlayer",
        "zerog",  # JAINE DEX (Uniswap V3 fork on 0G Chain)
    },
    "agni_finance": {"mantle"},  # Agni Finance (Uniswap V3 fork, primary DEX on Mantle)
    "gmx_v2": {"arbitrum", "avalanche"},
    "hyperliquid": {"arbitrum"},  # Hyperliquid is on its own L1 but accessed via Arbitrum
    "enso": {
        "ethereum",
        "arbitrum",
        "optimism",
        "polygon",
        "base",
        "avalanche",
        "bsc",
        "linea",
        "plasma",
        "blast",
        "berachain",
        "sonic",
    },  # Aggregator (Mantle excluded: Enso client CHAIN_MAPPING does not support it)
    "traderjoe_v2": {"avalanche"},  # TraderJoe Liquidity Book V2 on Avalanche
    "spark": {"ethereum"},  # Spark is an Aave V3 fork on Ethereum
    "pancakeswap_v3": {"bsc", "ethereum", "arbitrum"},  # PancakeSwap V3 DEX
    "lido": {"ethereum", "arbitrum", "optimism", "polygon"},  # Lido liquid staking
    "ethena": {"ethereum"},  # Ethena synthetic dollar (USDe/sUSDe)
    "radiant_v2": {"ethereum"},  # Radiant V2 (Aave V2 fork) — Arbitrum pool frozen post-hack
    "sushiswap_v3": {
        "ethereum",
        "arbitrum",
        "optimism",
        "polygon",
        "base",
        "bsc",
    },  # SushiSwap V3 DEX — avalanche excluded: zero usable liquidity (VIB-2069)
    "benqi": {"avalanche"},  # BENQI (Compound V2 fork) on Avalanche
    # joelend removed — Joe Lend (Banker Joe) was wound down by governance; VIB-3960
    "euler_v2": {"avalanche", "ethereum"},  # Euler V2 (ERC-4626 vaults + EVC)
    "silo_v2": {"avalanche"},  # Silo V2 isolated lending on Avalanche
    "gimo": {"zerog"},  # Gimo Finance liquid staking on 0G Chain
}


# =============================================================================
# Execution Mode (canonical home: ``almanak.config.runtime.ExecutionMode``;
# re-exported above for back-compat).
# =============================================================================


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
        max_gas_price_gwei: Maximum gas price in gwei (default: 100 when constructed directly;
            from_env() uses chain-specific defaults, and Anvil mode always uses 9999)
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
        # From environment variables (Phase 5a-2 entry point):
        from almanak.config.runtime import runtime_config_from_env
        rc = runtime_config_from_env(chain="arbitrum")
        config = LocalRuntimeConfig.from_runtime_config(rc)

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

        # Normalize chain alias (e.g., "bnb" -> "bsc") via central resolver
        try:
            from almanak.core.constants import resolve_chain_name

            chain_lower = resolve_chain_name(self.chain)
        except (ValueError, ImportError):
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
        # Zodiac mode: private key is held by remote signer, wallet_address
        # comes from the Safe signer's EOA address instead.
        if not self.private_key and self.safe_signer is not None and self.safe_signer.mode == "zodiac":
            self.wallet_address = self.safe_signer.eoa_address
            return

        # Gateway wallets mode: wallet addresses come from the gateway's WalletRegistry
        # at RegisterChains time, not from local env vars.
        if not self.private_key and gateway_wallets_configured():
            self.wallet_address = ""  # Resolved later by register_chains()
            return

        if not self.private_key:
            raise ConfigurationError(field="private_key", reason="Private key cannot be empty")

        if self._is_solana():
            return self._validate_solana_wallet()

        # EVM: normalize private key format
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

    def _validate_solana_wallet(self) -> None:
        """Validate Solana base58 private key and derive wallet address."""
        try:
            from almanak.framework.execution.solana.signer import SolanaSigner

            signer = SolanaSigner.from_base58(self.private_key)
            self.wallet_address = signer.wallet_address
        except Exception:
            raise ConfigurationError(
                field="private_key",
                reason="Invalid Solana private key (expected base58 Ed25519 keypair)",
            ) from None

    def _is_solana(self) -> bool:
        """Check if this config is for a Solana chain.

        VIB-4803: routes through the :class:`ChainFamily` adapter — the
        single seam for "is this chain SVM?". Falls back to ``False`` for
        unknown / unregistered chains, matching the legacy ``.lower() ==
        "solana"`` contract.
        """
        # Import here to avoid pulling the framework chain_family module into
        # every importer of execution.config — this method is only on the
        # validation path.
        from almanak.framework.chain_family import SvmFamily, family_for

        return isinstance(family_for(self.chain), SvmFamily)

    def _validate_optional_fields(self) -> None:
        """Validate optional fields with defaults."""
        # Gas price checks are EVM-only (Solana uses lamports, not gwei)
        if not self._is_solana():
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
    def from_runtime_config(cls, rc: RuntimeConfig) -> "LocalRuntimeConfig":
        """Build a :class:`LocalRuntimeConfig` from a typed :class:`RuntimeConfig`.

        Phase 5a-2: replaces the deleted :meth:`from_env` classmethod. The
        env-reading factory :func:`almanak.config.runtime.runtime_config_from_env`
        produces a :class:`RuntimeConfig`; this classmethod adapts it to the
        dataclass shape consumed downstream.

        For single-chain ``RuntimeConfig`` (``rc.single_chain == True``),
        the singular view (``chain``, ``rpc_url``, ``chain_id``) is the
        primary source. Multi-chain rows are not valid input here — call
        :meth:`MultiChainRuntimeConfig.from_runtime_config` for those.

        Args:
            rc: Typed runtime config from
                :func:`almanak.config.runtime.runtime_config_from_env`.

        Returns:
            ``LocalRuntimeConfig`` instance.

        Raises:
            ConfigurationError: If ``rc`` is multi-chain (use
                :class:`MultiChainRuntimeConfig` instead).
        """
        if not rc.single_chain:
            raise ConfigurationError(
                field="single_chain",
                reason=(
                    "LocalRuntimeConfig.from_runtime_config requires a "
                    "single-chain RuntimeConfig — use "
                    "MultiChainRuntimeConfig.from_runtime_config for multi-chain rows"
                ),
            )
        # ``rc.chain`` / ``rc.rpc_url`` are non-None on single-chain rows
        # (validated by the model's ``_check_single_vs_multi_consistency``).
        assert rc.chain is not None and rc.rpc_url is not None  # for type narrowing
        return cls(
            chain=rc.chain,
            rpc_url=rc.rpc_url,
            private_key=rc.private_key,
            max_gas_price_gwei=rc.max_gas_price_gwei,
            max_gas_cost_native=rc.max_gas_cost_native,
            max_gas_cost_usd=rc.max_gas_cost_usd,
            max_slippage_bps=rc.max_slippage_bps,
            tx_timeout_seconds=rc.tx_timeout_seconds,
            simulation_enabled=rc.simulation_enabled,
            max_tx_value_eth=rc.max_tx_value_eth,
            base_retry_delay=rc.base_retry_delay,
            max_retry_delay=rc.max_retry_delay,
            max_retries=rc.max_retries,
            safe_signer=rc.safe_signer,
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

        chain_str = data.get("chain", "").lower()
        return cls(
            chain=chain_str,
            rpc_url=data.get("rpc_url", ""),
            private_key=data["private_key"],
            max_gas_price_gwei=data.get("max_gas_price_gwei", 100),
            max_gas_cost_native=data.get("max_gas_cost_native", 0.0),
            max_gas_cost_usd=data.get("max_gas_cost_usd", 0.0),
            max_slippage_bps=data.get("max_slippage_bps", 0),
            tx_timeout_seconds=data.get("tx_timeout_seconds", _default_receipt_timeout(chain_str)),
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
# Gateway (Sidecar) Configuration
# =============================================================================


@dataclass
class GatewayRuntimeConfig:
    """Lightweight runtime config for sidecar deployments.

    Used when --no-gateway is set and ALMANAK_PRIVATE_KEY is absent.
    The gateway container handles all signing and RPC access; the strategy
    only needs chain info and the wallet address for balance queries.
    """

    chain: str
    wallet_address: str
    is_safe: bool = False
    max_gas_price_gwei: int = 100

    @property
    def execution_address(self) -> str:
        """Wallet address that executes transactions (via gateway)."""
        return self.wallet_address

    @property
    def is_safe_mode(self) -> bool:
        """Whether the wallet is a Safe."""
        return self.is_safe


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

            # Normalize chain alias (e.g., "bnb" -> "bsc") via central resolver
            try:
                from almanak.core.constants import resolve_chain_name

                chain_lower = resolve_chain_name(chain)
            except (ValueError, ImportError):
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
        # Zodiac mode: private key is held by remote signer, wallet_address
        # comes from the Safe signer's EOA address instead.
        if not self.private_key and self.safe_signer is not None and self.safe_signer.mode == "zodiac":
            self.wallet_address = self.safe_signer.eoa_address
            return

        # Gateway wallets mode: wallet addresses come from the gateway's WalletRegistry
        # at RegisterChains time, not from local env vars. Set a placeholder that will
        # be overridden after register_chains() returns per-chain wallets.
        if not self.private_key and gateway_wallets_configured():
            self.wallet_address = ""  # Resolved later by register_chains()
            logger.info(
                "Gateway wallets configured — wallet address will be resolved "
                "from WalletRegistry at RegisterChains time"
            )
            return

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
        self.rpc_urls = multi_chain_rpc_urls_from_env(
            chains=self.chains,
            network=network,
            private_key=self.private_key,
        )

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
        eoa_address: str | None = None,
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
            eoa_address: EOA address (required for safe_zodiac, derived from private_key for safe_direct)
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
                eoa_address="0x...",
                zodiac_address="0x...",
                signer_service_url="https://...",
                signer_service_jwt="...",
            )
        """
        from almanak.framework.execution.signer.safe import (
            SafeSigner,
            SafeSignerConfig,
            SafeWalletConfig,
            create_safe_signer,
        )

        mode = ExecutionMode.from_string(execution_mode)

        # Create Safe signer if needed
        safe_signer: SafeSigner | None = None

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
            safe_signer = create_safe_signer(signer_config)

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
            # Prefer explicit eoa_address; derive from private key only as fallback
            if not eoa_address and private_key:
                try:
                    eoa_address = Account.from_key(private_key).address
                except Exception:
                    raise ConfigurationError(
                        field="private_key",
                        reason="Invalid private key format for safe_zodiac mode",
                    ) from None
            if not eoa_address:
                raise ConfigurationError(
                    field="eoa_address",
                    reason="eoa_address is required for safe_zodiac mode when no private_key is provided",
                )

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
            safe_signer = create_safe_signer(signer_config)

        return cls(
            chains=chains,
            protocols=protocols,
            private_key=private_key,
            safe_signer=safe_signer,
            **kwargs,
        )

    @classmethod
    def from_runtime_config(cls, rc: RuntimeConfig) -> "MultiChainRuntimeConfig":
        """Build a :class:`MultiChainRuntimeConfig` from a typed :class:`RuntimeConfig`.

        Phase 5a-2: replaces the deleted :meth:`from_env` classmethod. The
        env-reading factory :func:`almanak.config.runtime.runtime_config_from_env`
        produces a :class:`RuntimeConfig`; this classmethod adapts it to the
        dataclass shape consumed by :class:`MultiChainOrchestrator`.

        Args:
            rc: Multi-chain ``RuntimeConfig`` from
                :func:`almanak.config.runtime.runtime_config_from_env`.

        Returns:
            ``MultiChainRuntimeConfig`` instance with RPC URLs already
            resolved (the dataclass's ``__post_init__`` re-loads them from
            env, and we override with the canonical resolved view post-hoc).

        Raises:
            ConfigurationError: If ``rc`` is single-chain (use
                :class:`LocalRuntimeConfig` instead).
        """
        if rc.single_chain:
            raise ConfigurationError(
                field="single_chain",
                reason=(
                    "MultiChainRuntimeConfig.from_runtime_config requires a "
                    "multi-chain RuntimeConfig — use "
                    "LocalRuntimeConfig.from_runtime_config for single-chain rows"
                ),
            )

        chains = list(rc.chains)
        protocols = {chain: list(plist) for chain, plist in rc.protocols.items()}

        # The dataclass __post_init__ re-runs validation and re-loads RPC
        # URLs from env if ``rpc_urls`` is empty at construction. To preserve
        # the resolved URLs from ``rc``, we set them on the instance after
        # construction (the dataclass treats ``rpc_urls`` as init=False).
        instance = cls(
            chains=chains,
            protocols=protocols,
            private_key=rc.private_key,
            network=rc.network,
            max_gas_price_gwei=rc.max_gas_price_gwei,
            max_gas_cost_native=rc.max_gas_cost_native,
            max_gas_cost_usd=rc.max_gas_cost_usd,
            max_slippage_bps=rc.max_slippage_bps,
            tx_timeout_seconds=rc.tx_timeout_seconds,
            simulation_enabled=rc.simulation_enabled,
            max_tx_value_eth=rc.max_tx_value_eth,
            base_retry_delay=rc.base_retry_delay,
            max_retry_delay=rc.max_retry_delay,
            max_retries=rc.max_retries,
            data_freshness_policy=rc.data_freshness_policy,
            stale_data_threshold_seconds=rc.stale_data_threshold_seconds,
            safe_signer=rc.safe_signer,
        )
        # Override RPC URLs with the resolved values from ``rc`` (the
        # dataclass loaded them from env in __post_init__; we replace with
        # the canonical resolved view from the config service).
        if rc.rpc_urls:
            instance.rpc_urls = dict(rc.rpc_urls)
        return instance

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
# Safe Signer Factory (canonical home: ``almanak.config.runtime``;
# re-exported here for back-compat).
# =============================================================================


# ``_resolve_private_key_from_env`` and ``_create_safe_signer_from_env``
# moved to ``almanak.config.runtime`` in Phase 5a-2. Importing them here
# preserves the back-compat surface for code that still imports them via
# ``from almanak.framework.execution.config import _resolve_private_key_from_env``.
from almanak.config.runtime import (  # noqa: E402  (intentional: re-export at module bottom)
    _create_safe_signer_from_env,
    _resolve_private_key_from_env,
)

# =============================================================================
# Exports
# =============================================================================


__all__ = [
    "LocalRuntimeConfig",
    "MultiChainRuntimeConfig",
    "GatewayRuntimeConfig",
    "ConfigurationError",
    "MissingEnvironmentVariableError",
    "ExecutionMode",
    "CHAIN_IDS",
    "SUPPORTED_PROTOCOLS",
    "DataFreshnessPolicy",
    "RuntimeConfig",
    "_resolve_private_key_from_env",
    "_create_safe_signer_from_env",
]
