"""Typed runtime configuration submodel.

Phase 5a-2 of the config-service migration (see
``docs/internal/config-service-plan.md``). This module is the **single
env-reading entry point** for runtime configuration. Phase 5a-1 introduced
:class:`RuntimeConfig` as a parity shim that delegated to the legacy
``LocalRuntimeConfig.from_env`` / ``MultiChainRuntimeConfig.from_env``
classmethods; Phase 5a-2 inverts that direction. The env-reading logic now
lives here, the legacy classmethods are gone, and the dataclass containers
in :mod:`almanak.framework.execution.config` are reduced to in-memory
shapes constructed via :meth:`LocalRuntimeConfig.from_runtime_config` /
:meth:`MultiChainRuntimeConfig.from_runtime_config`.

Lane selection
--------------

* ``chain=`` (or ``ALMANAK_CHAIN`` env): single-chain lane.
* ``chains=`` + ``protocols=``: multi-chain lane.

The two lanes are mutually exclusive and the factory raises
:class:`ConfigurationError` for ambiguous arguments.

Shape choice
------------
A single Pydantic v2 ``BaseModel`` with a ``single_chain: bool``
discriminator carries BOTH the singular and the plural views of every
chain-shaped field. Single-chain rows therefore expose
``rpc_urls == {chain: rpc_url}`` and ``chain_ids == {chain: chain_id}``;
multi-chain rows leave ``chain``, ``rpc_url``, ``chain_id`` as ``None`` /
``0`` so callers always reach for the dict view. The discriminator keeps
the union flat (no ``Annotated[Union[...], Discriminator]`` ceremony) and
preserves every field name from the legacy dataclasses.

Helper home
-----------
The private-key resolver, Safe-signer factory, RPC URL loader, gas-cap
default lookup, gateway-wallets check, and unprefixed-env warning helpers
all live in this module. They were lifted from
:mod:`almanak.framework.execution.config` in Phase 5a-2 and are private
(underscore-prefixed); ``runtime_config_from_env`` is the only public
entry point.

Import direction
----------------
Strict: ``almanak.config.*`` MUST NOT import from
``almanak.framework.execution.*``. The Phase 5a-1 hack of importing the
legacy dataclasses is reversed in 5a-2. The dataclass containers in
``framework/execution/config.py`` are now constructed FROM the
:class:`RuntimeConfig` produced here (see ``LocalRuntimeConfig.from_runtime_config``).
"""

from __future__ import annotations

import logging
import os
import re
from collections.abc import Callable
from datetime import UTC, datetime
from enum import StrEnum
from typing import TYPE_CHECKING, Any, Literal

from eth_account import Account
from pydantic import BaseModel, ConfigDict, Field, model_validator

from almanak.config.env import _load_dotenv_once

if TYPE_CHECKING:
    from almanak.framework.execution.signer.safe import SafeSigner

logger = logging.getLogger(__name__)


# =============================================================================
# Type Aliases
# =============================================================================


# Data freshness policy for multi-chain strategies.
# - fail_closed: any chain stale (>30s) or unavailable → decide() receives error
# - fail_open: stale chains excluded; decide() proceeds with available data
DataFreshnessPolicy = Literal["fail_closed", "fail_open"]


# =============================================================================
# Exceptions (canonical home — re-exported from framework/execution/config.py)
# =============================================================================


class ConfigurationError(Exception):
    """Raised when configuration is invalid or missing.

    Attributes:
        field: The configuration field that caused the error.
        reason: Human-readable explanation of the error.
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
# Constants — chain id / supported protocol mapping
# =============================================================================


# Chain ID mapping for supported chains. Canonical home; the legacy module
# in ``framework/execution/config.py`` re-exports this for back-compat.
CHAIN_IDS: dict[str, int] = {
    "ethereum": 1,
    "arbitrum": 42161,
    "optimism": 10,
    "polygon": 137,
    "base": 8453,
    "avalanche": 43114,
    "bsc": 56,
    "linea": 59144,
    "plasma": 9745,
    "blast": 81457,
    "mantle": 5000,
    "berachain": 80094,
    "solana": 0,  # Non-EVM chain, no EVM chain ID
    "sonic": 146,
    "monad": 143,
    "xlayer": 196,
    "zerog": 16661,
}


# =============================================================================
# Execution mode (canonical home)
# =============================================================================


class ExecutionMode(StrEnum):
    """Execution mode for transaction signing.

    See ``framework/execution/config.py`` for the full docstring (the symbol
    is re-exported there for back-compat). Three modes:

    * ``EOA`` — direct EOA signing.
    * ``SAFE_DIRECT`` — Safe wallet, locally signed.
    * ``SAFE_ZODIAC`` — Safe wallet via Zodiac roles + remote signer.
    """

    EOA = "eoa"
    SAFE_DIRECT = "safe_direct"
    SAFE_ZODIAC = "safe_zodiac"

    @classmethod
    def from_string(cls, value: str) -> ExecutionMode:
        """Parse execution mode from string (case-insensitive)."""
        try:
            return cls(value.lower())
        except ValueError as e:
            valid_modes = ", ".join(m.value for m in cls)
            raise ConfigurationError(
                field="execution_mode",
                reason=f"Invalid execution mode '{value}'. Valid modes: {valid_modes}",
            ) from e


# =============================================================================
# RuntimeConfig — typed, validated runtime config (single + multi)
# =============================================================================


class RuntimeConfig(BaseModel):
    """Typed runtime config — single-chain and multi-chain in one shape.

    The ``single_chain`` discriminator selects which view is primary:

    * Single-chain (``single_chain=True``): ``chain``, ``rpc_url``, and
      ``chain_id`` are populated. ``chains`` / ``rpc_urls`` / ``chain_ids``
      mirror as length-1 / single-key dicts so consumers can read either view.
    * Multi-chain (``single_chain=False``): ``chains``, ``rpc_urls``,
      ``chain_ids``, ``protocols``, ``primary_chain``, and the data-freshness
      knobs are populated. ``chain`` / ``rpc_url`` / ``chain_id`` are
      ``None`` / ``0``.

    Numeric guards (``max_gas_price_gwei`` range, ``tx_timeout_seconds``,
    etc.) are enforced by the dataclass containers' ``__post_init__``
    today; they will move into Pydantic validators on this model in a
    follow-up phase. ``runtime_config_from_env`` already produces values
    that pass those guards.
    """

    # Discriminator — never read from env; determined by the factory's lane.
    single_chain: bool

    # Chain identity (single-chain view).
    chain: str | None = None
    rpc_url: str | None = None
    chain_id: int = 0

    # Chain identity (multi-chain view; also populated for single-chain).
    chains: list[str] = Field(default_factory=list)
    rpc_urls: dict[str, str] = Field(default_factory=dict)
    chain_ids: dict[str, int] = Field(default_factory=dict)
    primary_chain: str = ""

    # Multi-chain only — protocol mapping per chain.
    protocols: dict[str, list[str]] = Field(default_factory=dict)

    # Network environment ("mainnet" | "sepolia" | "anvil"). Captured for
    # both lanes so downstream code has a single accessor.
    network: str = "mainnet"

    # Wallet — derived from ``private_key`` (or remote signer). Empty when
    # gateway-wallets mode resolves the address later via RegisterChains.
    wallet_address: str = ""
    private_key: str = Field(default="", repr=False)

    # Gas / tx / retry knobs — identical defaults across both legacy dataclasses.
    max_gas_price_gwei: int = 100
    max_gas_cost_native: float = 0.0
    max_gas_cost_usd: float = 0.0
    max_slippage_bps: int = 0
    tx_timeout_seconds: int = 120
    simulation_enabled: bool = True
    max_tx_value_eth: float = 10.0
    base_retry_delay: float = 1.0
    max_retry_delay: float = 32.0
    max_retries: int = 3

    # Multi-chain only — data freshness policy. Default values match the
    # multi-chain dataclass; single-chain rows carry the same defaults so
    # consumers can read the field unconditionally.
    data_freshness_policy: DataFreshnessPolicy = "fail_closed"
    stale_data_threshold_seconds: float = 30.0

    # Optional Safe wallet signer. ``SafeSigner`` is not Pydantic-validated;
    # we pass the runtime instance through unchanged.
    safe_signer: Any | None = None

    # Metadata.
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))

    # ``extra="forbid"`` matches every other Phase 5 submodel — a misspelled
    # adapter field on ``RuntimeConfig(...)`` should fail at the config
    # boundary, not slip through silently as an attribute that nothing reads
    # (PR #2152 review).
    model_config = ConfigDict(arbitrary_types_allowed=True, extra="forbid")

    @model_validator(mode="after")
    def _check_single_vs_multi_consistency(self) -> RuntimeConfig:
        """Enforce the invariant the discriminator promises."""
        if self.single_chain:
            if not self.chain:
                raise ValueError(
                    "RuntimeConfig(single_chain=True) requires `chain` to be set",
                )
        else:
            if not self.chains:
                raise ValueError(
                    "RuntimeConfig(single_chain=False) requires `chains` non-empty",
                )
        return self


# =============================================================================
# Internal helpers — env reads, RPC URL loading, Safe signer factory
# =============================================================================


# Env vars that are silently ignored when set without the ALMANAK_ prefix.
# Documented operator-facing footgun; we warn so misconfiguration is loud.
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


def _make_env_getters(
    prefix: str,
) -> tuple[
    Callable[[str], str],
    Callable[..., str | None],
    Callable[[str, int], int],
    Callable[[str, float], float],
    Callable[[str, bool], bool],
]:
    """Build the typed env-getter closures used by ``runtime_config_from_env``.

    Returns ``(get_required, get_optional, get_optional_int, get_optional_float,
    get_optional_bool)``. All five inject the ``prefix`` once and raise the
    canonical exceptions on parse failure.
    """

    def get_required(name: str) -> str:
        full_name = f"{prefix}{name}"
        value = os.environ.get(full_name)
        if not value:
            raise MissingEnvironmentVariableError(full_name)
        return value

    def get_optional(name: str, default: str | None = None) -> str | None:
        full_name = f"{prefix}{name}"
        return os.environ.get(full_name, default)

    def get_optional_int(name: str, default: int) -> int:
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
        full_name = f"{prefix}{name}"
        value = os.environ.get(full_name)
        if value is None:
            return default
        return value.lower() in ("true", "1", "yes", "y")

    return get_required, get_optional, get_optional_int, get_optional_float, get_optional_bool


def _resolve_private_key_from_env(
    *,
    private_key: str | None,
    chain: str | None,
    execution_mode: ExecutionMode,
    gateway_wallets_configured: bool,
    get_required: Callable[[str], str],
    get_optional: Callable[..., str | None],
) -> str:
    """Resolve the wallet signing key with kwarg-over-env precedence (#2100).

    Encodes the precedence ladder once:
    explicit ``private_key`` kwarg (incl. ``""``) > ``{prefix}PRIVATE_KEY``
    env > unprefixed env (only the Solana branch reads ``SOLANA_PRIVATE_KEY``
    directly because that name is the canonical bare env var).

    ``private_key is not None`` honours an explicit empty-string override:
    callers can pass ``private_key=""`` to force the no-local-key path even
    when an ambient ``ALMANAK_PRIVATE_KEY`` is set in env (e.g. SAFE_ZODIAC
    or gateway-wallets flows running on a developer box).
    """
    if private_key is not None:
        return private_key
    if chain == "solana":
        # Solana uses base58 Ed25519 instead of hex secp256k1 — separate env var.
        return os.environ.get("SOLANA_PRIVATE_KEY") or get_required("PRIVATE_KEY")
    if execution_mode == ExecutionMode.SAFE_ZODIAC:
        # Zodiac: private key held by the remote signer service, not needed locally.
        return get_optional("PRIVATE_KEY", "") or ""
    if gateway_wallets_configured:
        # Gateway wallets mode: signing handled by the gateway, key optional locally.
        return get_optional("PRIVATE_KEY", "") or ""
    return get_required("PRIVATE_KEY")


def _create_safe_signer_from_env(
    execution_mode: ExecutionMode,
    private_key: str,
    prefix: str = "ALMANAK_",
) -> SafeSigner:
    """Create a Safe signer from environment variables.

    Mirrors the legacy helper that lived in
    :mod:`almanak.framework.execution.config`. Re-exported from there for
    back-compat; the canonical home is here.
    """
    # Imported lazily so this module's import cost stays low and the gateway
    # signer package isn't pulled into ``almanak.config.*`` at module-load.
    from almanak.framework.execution.signer.safe import (
        SafeSignerConfig,
        SafeWalletConfig,
        create_safe_signer,
    )

    def get_required(name: str) -> str:
        full_name = f"{prefix}{name}"
        value = os.environ.get(full_name)
        if not value:
            raise MissingEnvironmentVariableError(full_name)
        return value

    def get_optional(name: str) -> str | None:
        full_name = f"{prefix}{name}"
        return os.environ.get(full_name)

    safe_address = get_required("SAFE_ADDRESS")

    if execution_mode == ExecutionMode.SAFE_ZODIAC:
        # Zodiac mode: prefer explicit EOA_ADDRESS (platform deployments use
        # remote signer with no local key). Fall back to deriving from private
        # key only when EOA_ADDRESS is not set.
        explicit_eoa = get_optional("EOA_ADDRESS")
        if explicit_eoa:
            eoa_address = explicit_eoa
        elif private_key:
            try:
                account = Account.from_key(private_key)
                eoa_address = account.address
            except Exception:
                raise ConfigurationError(
                    field="private_key",
                    reason="Invalid private key format for safe_zodiac mode",
                ) from None
        else:
            raise MissingEnvironmentVariableError(f"{prefix}EOA_ADDRESS")
    else:
        # Direct mode: derive EOA from private key.
        account = Account.from_key(private_key)
        eoa_address = account.address

    if execution_mode == ExecutionMode.SAFE_DIRECT:
        wallet_config = SafeWalletConfig(
            safe_address=safe_address,
            eoa_address=eoa_address,
        )
        signer_config = SafeSignerConfig(
            mode="direct",
            wallet_config=wallet_config,
            private_key=private_key,
        )
        logger.info(f"Creating Safe signer (direct): safe={safe_address[:10]}..., eoa={eoa_address[:10]}...")
        return create_safe_signer(signer_config)

    if execution_mode == ExecutionMode.SAFE_ZODIAC:
        zodiac_address = get_required("ZODIAC_ADDRESS")
        # Service URL/JWT are optional — zodiac mode also works with just a private_key.
        signer_service_url = get_optional("SIGNER_SERVICE_URL")
        signer_service_jwt = get_optional("SIGNER_SERVICE_JWT")

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
        logger.info(f"Creating Safe signer (zodiac): safe={safe_address[:10]}..., zodiac={zodiac_address[:10]}...")
        return create_safe_signer(signer_config)

    raise ConfigurationError(
        field="execution_mode",
        reason=f"Cannot create Safe signer for mode: {execution_mode}",
    )


# crap-allowlist: bit-for-bit mirror of LocalRuntimeConfig._validate_and_derive_wallet
# / MultiChainRuntimeConfig._validate_and_derive_wallet — splitting the gateway-wallets,
# Zodiac, Solana, and EVM branches just to lower CC would diverge from the dataclass
# methods the dataclass __post_init__ still calls when constructed directly. The legacy
# branches are covered via the dataclass tests in tests/unit/cli/test_local_runtime_config.py
# and tests/unit/execution/test_multichain_sidecar.py. (#2097, Phase 5a-2)
def _derive_wallet_address(
    *,
    chain: str,
    private_key: str,
    safe_signer: SafeSigner | None,
    gateway_wallets_configured: bool,
) -> str:
    """Derive the wallet address from the resolved private key.

    Mirrors the legacy ``LocalRuntimeConfig._validate_and_derive_wallet`` and
    ``MultiChainRuntimeConfig._validate_and_derive_wallet`` flows; the order
    of branches is identical so wallet identity stays bit-for-bit.

    Returns ``""`` when the gateway resolves the wallet at RegisterChains
    time (gateway-wallets mode) or when Zodiac mode holds the key remotely.
    """
    if not private_key and safe_signer is not None and getattr(safe_signer, "mode", None) == "zodiac":
        return safe_signer.eoa_address
    if not private_key and gateway_wallets_configured:
        # Gateway wallets mode: address resolved later by register_chains().
        return ""
    if not private_key:
        raise ConfigurationError(field="private_key", reason="Private key cannot be empty")
    if chain.lower() == "solana":
        try:
            from almanak.framework.execution.solana.signer import SolanaSigner

            signer = SolanaSigner.from_base58(private_key)
            return signer.wallet_address
        except Exception:
            raise ConfigurationError(
                field="private_key",
                reason="Invalid Solana private key (expected base58 Ed25519 keypair)",
            ) from None

    # EVM: normalise hex format and derive via eth_account.
    key = private_key
    if not key.startswith("0x"):
        key = "0x" + key
    if len(key) != 66:  # 0x + 64 hex chars
        raise ConfigurationError(
            field="private_key",
            reason="Private key must be 32 bytes (64 hex characters)",
        )
    try:
        return Account.from_key(key).address
    except Exception:
        # Never expose key material in error messages.
        raise ConfigurationError(field="private_key", reason="Invalid private key format") from None


_RPC_URL_PATTERN = re.compile(
    r"^https?://"  # http:// or https://
    r"[a-zA-Z0-9.-]+"  # domain
    r"(:\d+)?"  # optional port
    r"(/.*)?$"  # optional path
)


def _mask_url(url: str | None) -> str | None:
    """Mask sensitive parts of URL for logging."""
    if not url:
        return url
    masked = re.sub(
        r"(api[_-]?key|apikey|key|token)=([^&]+)",
        r"\1=***",
        url,
        flags=re.IGNORECASE,
    )
    masked = re.sub(
        r"://([^:]+):([^@]+)@",
        r"://\1:***@",
        masked,
    )
    return masked


def _gas_cap_for_chain(
    *, chain: str | None, network: str, prefix: str, get_optional_int: Callable[[str, int], int]
) -> int:
    """Return the resolved ``max_gas_price_gwei`` for this chain + network.

    VIB-303 + VIB-304 + VIB-1719: Anvil mode always uses
    ``ANVIL_GAS_PRICE_CAP_GWEI`` (gas costs no real money, low caps cause
    false-positive errors on high-gas chains like Polygon). Mainnet uses
    chain-specific defaults from ``CHAIN_GAS_PRICE_CAPS_GWEI`` with
    ``DEFAULT_GAS_PRICE_CAP_GWEI`` as the fallback.

    ``chain`` is ``None`` for the multi-chain lane (no per-chain cap is
    expressible there); the multi-chain mainnet default is the global
    ``DEFAULT_GAS_PRICE_CAP_GWEI``.
    """
    # Imported lazily so this module's startup cost stays low.
    from almanak.framework.execution.gas.constants import (
        ANVIL_GAS_PRICE_CAP_GWEI,
        CHAIN_GAS_PRICE_CAPS_GWEI,
        DEFAULT_GAS_PRICE_CAP_GWEI,
    )

    if network.lower() == "anvil":
        default_gas_cap = ANVIL_GAS_PRICE_CAP_GWEI
        user_gas_cap = get_optional_int("MAX_GAS_PRICE_GWEI", default_gas_cap)
        if user_gas_cap < ANVIL_GAS_PRICE_CAP_GWEI:
            logger.warning(
                "%sMAX_GAS_PRICE_GWEI=%d is too low for Anvil mode "
                "(gas costs no real money). Overriding to %d gwei to prevent "
                "false-positive gas cap errors on high-gas chains (e.g. Polygon).",
                prefix,
                user_gas_cap,
                ANVIL_GAS_PRICE_CAP_GWEI,
            )
        return ANVIL_GAS_PRICE_CAP_GWEI

    if chain is not None:
        default_gas_cap = CHAIN_GAS_PRICE_CAPS_GWEI.get(chain, DEFAULT_GAS_PRICE_CAP_GWEI)
    else:
        default_gas_cap = DEFAULT_GAS_PRICE_CAP_GWEI
    return get_optional_int("MAX_GAS_PRICE_GWEI", default_gas_cap)


def _resolve_single_chain_rpc_url(
    *,
    chain: str,
    network: str,
    prefix: str,
    get_optional: Callable[..., str | None],
) -> str:
    """Resolve the single-chain RPC URL with the legacy precedence ladder.

    Anvil mode always uses the local fork URL (skips env). Mainnet/sepolia
    follow: per-chain env (``ALMANAK_{CHAIN}_RPC_URL``) > generic env
    (``ALMANAK_RPC_URL``) > dynamic build via ``get_rpc_url`` (handles API
    keys, Tenderly, free public RPCs).
    """
    # Imported lazily — Phase 4 territory; rpc_provider has its own backlog
    # of env reads (per the boundary lint allowlist).
    from almanak.gateway.utils.rpc_provider import get_rpc_url

    if network.lower() == "anvil":
        anvil_url = get_rpc_url(chain, network="anvil")
        logger.debug(f"Using Anvil RPC URL for {chain}: {anvil_url}")
        return anvil_url

    chain_specific_var = f"{prefix}{chain.upper()}_RPC_URL"
    chain_specific = os.environ.get(chain_specific_var)
    if chain_specific:
        logger.debug(f"Using chain-specific RPC URL from {chain_specific_var}")
        return chain_specific

    generic = get_optional("RPC_URL")
    if generic:
        return generic

    try:
        built = get_rpc_url(chain, network=network)
        logger.debug(f"Built RPC URL dynamically for {chain}")
        return built
    except ValueError as e:
        raise ConfigurationError(
            field="rpc_url",
            reason=(
                f"Could not build RPC URL: {e}. Set {prefix}RPC_URL, "
                f"{prefix}{chain.upper()}_RPC_URL, ALCHEMY_API_KEY, "
                f"or TENDERLY_API_KEY_{chain.upper()}."
            ),
        ) from None


def _resolve_multi_chain_rpc_urls(
    *,
    chains: list[str],
    network: str,
    private_key: str,
    prefix: str,
) -> dict[str, str]:
    """Resolve per-chain RPC URLs for the multi-chain lane.

    Mirrors :meth:`MultiChainRuntimeConfig._load_rpc_urls`. Gateway wallets
    mode (no local private key + ``{prefix}GATEWAY_WALLETS`` set) returns
    an empty dict — the gateway handles all RPC access. Anvil mode returns
    local fork URLs per chain. Mainnet/sepolia follow the per-chain env
    var → dynamic build ladder. ``prefix`` is threaded through so callers
    using a non-default prefix (e.g. ``"MYAPP_"``) get the same lookup
    behaviour the single-chain lane already provides (PR #2152 review).
    """
    if not private_key and os.environ.get(f"{prefix}GATEWAY_WALLETS"):
        logger.info("Gateway wallets mode — skipping local RPC URL loading (gateway handles RPC)")
        return {}

    from almanak.gateway.utils.rpc_provider import get_rpc_url

    is_anvil = network.lower() == "anvil"
    rpc_urls: dict[str, str] = {}

    for chain in chains:
        env_var = f"{prefix}{chain.upper()}_RPC_URL"

        if is_anvil:
            rpc_urls[chain] = get_rpc_url(chain, network="anvil")
            logger.debug(f"Using Anvil RPC URL for {chain}: {rpc_urls[chain]}")
            continue

        rpc_url_env = os.environ.get(env_var)
        if rpc_url_env:
            if not _RPC_URL_PATTERN.match(rpc_url_env):
                raise ConfigurationError(
                    field=env_var,
                    reason=f"Invalid RPC URL format: {_mask_url(rpc_url_env)}",
                )
            rpc_urls[chain] = rpc_url_env
            logger.debug(f"Using explicit RPC URL for {chain}")
            continue

        try:
            rpc_urls[chain] = get_rpc_url(chain, network=network)
            logger.debug(f"Built RPC URL dynamically for {chain}")
        except ValueError as e:
            raise ConfigurationError(
                field=env_var,
                reason=(
                    f"Could not build RPC URL for {chain}: {e}. Set {env_var}, "
                    f"RPC_URL, ALCHEMY_API_KEY, or TENDERLY_API_KEY_{chain.upper()}."
                ),
            ) from None

    return rpc_urls


def _default_receipt_timeout(chain: str) -> int:
    """Return the default receipt timeout for ``chain``.

    Slow chains (BSC, Avalanche) need longer timeouts on Anvil forks. Users
    can still override per-strategy with ``ALMANAK_TX_TIMEOUT_SECONDS``.
    """
    # Imported lazily so this module's startup cost stays low; chain_executor
    # pulls in execution machinery we don't otherwise need at config-load time.
    from almanak.framework.execution.chain_executor import (
        CHAIN_RECEIPT_TIMEOUTS,
        DEFAULT_RECEIPT_TIMEOUT,
    )

    return CHAIN_RECEIPT_TIMEOUTS.get(chain.lower(), DEFAULT_RECEIPT_TIMEOUT)


# =============================================================================
# Public factory — single env-reading entry point for runtime config
# =============================================================================


def runtime_config_from_env(  # noqa: C901
    *,
    chain: str | None = None,
    chains: list[str] | None = None,
    protocols: dict[str, list[str]] | None = None,
    network: Literal["mainnet", "sepolia", "anvil"] | str = "mainnet",
    dotenv_path: str | None = None,
    prefix: str = "ALMANAK_",
    private_key: str | None = None,
) -> RuntimeConfig:
    """Construct a :class:`RuntimeConfig` from environment variables.

    Phase 5a-2: this is the single env-reading entry point for runtime
    configuration. The legacy ``LocalRuntimeConfig.from_env`` /
    ``MultiChainRuntimeConfig.from_env`` classmethods are gone; consumers
    that need the dataclass shapes call ``LocalRuntimeConfig.from_runtime_config(rc)``
    / ``MultiChainRuntimeConfig.from_runtime_config(rc)`` after this factory.

    Lane selection:

    * ``chains`` (and ``protocols``) given → multi-chain. ``chain`` must be
      ``None``.
    * ``chain`` given (or ``ALMANAK_CHAIN`` env var fills it) → single-chain.
      ``chains`` / ``protocols`` must be ``None``.

    Args:
        chain: Single-chain name. Mutually exclusive with ``chains``.
        chains: Multi-chain list. Requires ``protocols``.
        protocols: Per-chain protocol mapping (multi-chain only).
        network: ``"mainnet"`` | ``"sepolia"`` | ``"anvil"``. Default ``"mainnet"``.
        dotenv_path: Optional ``.env`` path; routed through
            :func:`almanak.config.env._load_dotenv_once`.
        prefix: Env-var prefix. Default ``"ALMANAK_"``.
        private_key: Optional explicit signing key — kwarg-over-env
            precedence, identical to the legacy classmethods (#2100). Pass
            ``""`` to force the no-local-key path even when an ambient
            ``ALMANAK_PRIVATE_KEY`` is set.

    Returns:
        :class:`RuntimeConfig` with all fields fully resolved.

    Raises:
        :class:`MissingEnvironmentVariableError`: missing required env var
            (e.g. ``ALMANAK_PRIVATE_KEY`` for non-Anvil non-Zodiac
            non-gateway-wallets execution).
        :class:`ConfigurationError`: invalid env values, ambiguous lane
            selection, or unbuildable RPC URLs.
    """
    # Reject ambiguous lane selection up front.
    if chains is not None and chain is not None:
        raise ConfigurationError(
            field="chain",
            reason="`chain` and `chains` are mutually exclusive in runtime_config_from_env",
        )
    if chains is not None and protocols is None:
        raise ConfigurationError(
            field="protocols",
            reason="`protocols` is required when `chains` is provided",
        )
    if chains is None and protocols is not None:
        raise ConfigurationError(
            field="protocols",
            reason="`protocols` is only valid alongside `chains` (multi-chain lane)",
        )

    # Single dotenv ingest at the service boundary. ``_load_dotenv_once`` is
    # process-wide; subsequent calls are no-ops regardless of arg shape.
    _load_dotenv_once(dotenv_path)

    get_required, get_optional, get_optional_int, get_optional_float, get_optional_bool = _make_env_getters(prefix)

    # VIB-308: warn when unprefixed env vars are set — silently ignored without prefix.
    _warn_unprefixed_env_vars(prefix)

    # Common: execution mode + gateway-wallets check + safe signer.
    mode_str = get_optional("EXECUTION_MODE", "eoa") or "eoa"
    execution_mode = ExecutionMode.from_string(mode_str)
    gateway_wallets_configured = bool(os.environ.get(f"{prefix}GATEWAY_WALLETS"))

    if chains is not None:
        return _build_multi_chain(
            chains=chains,
            protocols=protocols or {},  # validated above; never None at this point
            network=network,
            private_key=private_key,
            execution_mode=execution_mode,
            gateway_wallets_configured=gateway_wallets_configured,
            prefix=prefix,
            get_required=get_required,
            get_optional=get_optional,
            get_optional_int=get_optional_int,
            get_optional_float=get_optional_float,
            get_optional_bool=get_optional_bool,
        )

    return _build_single_chain(
        chain=chain,
        network=network,
        private_key=private_key,
        execution_mode=execution_mode,
        gateway_wallets_configured=gateway_wallets_configured,
        prefix=prefix,
        get_required=get_required,
        get_optional=get_optional,
        get_optional_int=get_optional_int,
        get_optional_float=get_optional_float,
        get_optional_bool=get_optional_bool,
    )


def _build_single_chain(  # noqa: PLR0913 (intentional: explicit getter injection)
    *,
    chain: str | None,
    network: str,
    private_key: str | None,
    execution_mode: ExecutionMode,
    gateway_wallets_configured: bool,
    prefix: str,
    get_required: Callable[[str], str],
    get_optional: Callable[..., str | None],
    get_optional_int: Callable[[str, int], int],
    get_optional_float: Callable[[str, float], float],
    get_optional_bool: Callable[[str, bool], bool],
) -> RuntimeConfig:
    """Single-chain lane — mirrors the legacy ``LocalRuntimeConfig.from_env``."""
    # Determine chain: parameter > env var (normalised to lowercase).
    resolved_chain = (chain or get_optional("CHAIN") or "").lower() or None
    if not resolved_chain:
        raise ConfigurationError(
            field="chain",
            reason="Chain must be provided via 'chain' parameter or ALMANAK_CHAIN env var",
        )

    # Normalise chain alias (e.g. "bnb" -> "bsc") via central resolver.
    try:
        from almanak.core.constants import resolve_chain_name

        resolved_chain = resolve_chain_name(resolved_chain)
    except (ValueError, ImportError):
        # Fall back to the lowercased name as-is; CHAIN_IDS lookup below
        # will raise if the chain is genuinely unsupported.
        pass
    if resolved_chain not in CHAIN_IDS:
        valid_chains = ", ".join(sorted(CHAIN_IDS.keys()))
        raise ConfigurationError(
            field="chain",
            reason=f"Unsupported chain '{resolved_chain}'. Valid chains: {valid_chains}",
        )

    rpc_url = _resolve_single_chain_rpc_url(
        chain=resolved_chain,
        network=network,
        prefix=prefix,
        get_optional=get_optional,
    )

    resolved_private_key = _resolve_private_key_from_env(
        private_key=private_key,
        chain=resolved_chain,
        execution_mode=execution_mode,
        gateway_wallets_configured=gateway_wallets_configured,
        get_required=get_required,
        get_optional=get_optional,
    )

    safe_signer: SafeSigner | None = None
    if execution_mode in (ExecutionMode.SAFE_DIRECT, ExecutionMode.SAFE_ZODIAC) and not gateway_wallets_configured:
        safe_signer = _create_safe_signer_from_env(
            execution_mode=execution_mode,
            private_key=resolved_private_key,
            prefix=prefix,
        )

    wallet_address = _derive_wallet_address(
        chain=resolved_chain,
        private_key=resolved_private_key,
        safe_signer=safe_signer,
        gateway_wallets_configured=gateway_wallets_configured,
    )

    max_gas_price_gwei = _gas_cap_for_chain(
        chain=resolved_chain,
        network=network,
        prefix=prefix,
        get_optional_int=get_optional_int,
    )

    chain_id = CHAIN_IDS.get(resolved_chain, 0)

    return RuntimeConfig(
        single_chain=True,
        chain=resolved_chain,
        rpc_url=rpc_url,
        chain_id=chain_id,
        chains=[resolved_chain],
        rpc_urls={resolved_chain: rpc_url},
        chain_ids={resolved_chain: chain_id},
        primary_chain=resolved_chain,
        protocols={},
        network=network,
        wallet_address=wallet_address,
        private_key=resolved_private_key,
        max_gas_price_gwei=max_gas_price_gwei,
        max_gas_cost_native=get_optional_float("MAX_GAS_COST_NATIVE", 0.0),
        max_gas_cost_usd=get_optional_float("MAX_GAS_COST_USD", 0.0),
        max_slippage_bps=get_optional_int("MAX_SLIPPAGE_BPS", 0),
        tx_timeout_seconds=get_optional_int("TX_TIMEOUT_SECONDS", _default_receipt_timeout(resolved_chain)),
        simulation_enabled=get_optional_bool("SIMULATION_ENABLED", True),
        max_tx_value_eth=get_optional_float("MAX_TX_VALUE_ETH", 10.0),
        base_retry_delay=get_optional_float("BASE_RETRY_DELAY", 1.0),
        max_retry_delay=get_optional_float("MAX_RETRY_DELAY", 32.0),
        max_retries=get_optional_int("MAX_RETRIES", 3),
        # Single-chain has no data-freshness fields; defaults match the
        # multi-chain dataclass so downstream contracts are uniform.
        data_freshness_policy="fail_closed",
        stale_data_threshold_seconds=30.0,
        safe_signer=safe_signer,
    )


def _build_multi_chain(  # noqa: PLR0913
    *,
    chains: list[str],
    protocols: dict[str, list[str]],
    network: str,
    private_key: str | None,
    execution_mode: ExecutionMode,
    gateway_wallets_configured: bool,
    prefix: str,
    get_required: Callable[[str], str],
    get_optional: Callable[..., str | None],
    get_optional_int: Callable[[str, int], int],
    get_optional_float: Callable[[str, float], float],
    get_optional_bool: Callable[[str, bool], bool],
) -> RuntimeConfig:
    """Multi-chain lane — mirrors the legacy ``MultiChainRuntimeConfig.from_env``."""
    # Resolve the signing key. ``chain=None`` because MultiChainRuntimeConfig
    # is multi-chain by construction — the Solana-specific env-var fallback
    # only fires for the single-chain lane.
    resolved_private_key = _resolve_private_key_from_env(
        private_key=private_key,
        chain=None,
        execution_mode=execution_mode,
        gateway_wallets_configured=gateway_wallets_configured,
        get_required=get_required,
        get_optional=get_optional,
    )

    safe_signer: SafeSigner | None = None
    if execution_mode in (ExecutionMode.SAFE_DIRECT, ExecutionMode.SAFE_ZODIAC) and not gateway_wallets_configured:
        safe_signer = _create_safe_signer_from_env(
            execution_mode=execution_mode,
            private_key=resolved_private_key,
            prefix=prefix,
        )

    # The multi-chain dataclass does the chain validation in __post_init__;
    # we replicate that here so the factory raises before the dataclass would.
    normalised_chains, chain_ids = _normalise_multi_chains(chains)
    normalised_protocols = _normalise_multi_protocols(normalised_chains, protocols)

    rpc_urls = _resolve_multi_chain_rpc_urls(
        chains=normalised_chains,
        network=network,
        private_key=resolved_private_key,
        prefix=prefix,
    )

    wallet_address = _derive_wallet_address(
        chain="",  # multi-chain has no canonical "the chain" for wallet derivation
        private_key=resolved_private_key,
        safe_signer=safe_signer,
        gateway_wallets_configured=gateway_wallets_configured,
    )

    max_gas_price_gwei = _gas_cap_for_chain(
        chain=None,
        network=network,
        prefix=prefix,
        get_optional_int=get_optional_int,
    )

    return RuntimeConfig(
        single_chain=False,
        chain=None,
        rpc_url=None,
        chain_id=0,
        chains=normalised_chains,
        rpc_urls=rpc_urls,
        chain_ids=chain_ids,
        primary_chain=normalised_chains[0],
        protocols=normalised_protocols,
        network=network,
        wallet_address=wallet_address,
        private_key=resolved_private_key,
        max_gas_price_gwei=max_gas_price_gwei,
        max_gas_cost_native=get_optional_float("MAX_GAS_COST_NATIVE", 0.0),
        max_gas_cost_usd=get_optional_float("MAX_GAS_COST_USD", 0.0),
        max_slippage_bps=get_optional_int("MAX_SLIPPAGE_BPS", 0),
        tx_timeout_seconds=get_optional_int("TX_TIMEOUT_SECONDS", 120),
        simulation_enabled=get_optional_bool("SIMULATION_ENABLED", True),
        max_tx_value_eth=get_optional_float("MAX_TX_VALUE_ETH", 10.0),
        base_retry_delay=get_optional_float("BASE_RETRY_DELAY", 1.0),
        max_retry_delay=get_optional_float("MAX_RETRY_DELAY", 32.0),
        max_retries=get_optional_int("MAX_RETRIES", 3),
        data_freshness_policy="fail_closed",
        stale_data_threshold_seconds=30.0,
        safe_signer=safe_signer,
    )


# crap-allowlist: mirrors MultiChainRuntimeConfig._validate_chains — same chain
# normalization/duplicate-detection branches the dataclass __post_init__ runs.
# Covered by the dataclass tests in tests/unit/execution/test_multichain_sidecar.py.
# (#2097, Phase 5a-2)
def _normalise_multi_chains(chains: list[str]) -> tuple[list[str], dict[str, int]]:
    """Normalise + validate a multi-chain list; build the chain-id map.

    Mirrors :meth:`MultiChainRuntimeConfig._validate_chains` so the factory
    raises before the dataclass would.
    """
    if not chains:
        raise ConfigurationError(
            field="chains",
            reason="At least one chain must be specified",
        )

    normalised: list[str] = []
    for c in chains:
        if not c:
            raise ConfigurationError(field="chains", reason="Chain name cannot be empty")
        try:
            from almanak.core.constants import resolve_chain_name

            cl = resolve_chain_name(c)
        except (ValueError, ImportError):
            cl = c.lower()
        if cl not in CHAIN_IDS:
            valid_chains = ", ".join(sorted(CHAIN_IDS.keys()))
            raise ConfigurationError(
                field="chains",
                reason=f"Unsupported chain '{c}'. Valid chains: {valid_chains}",
            )
        if cl == "solana":
            # ``_build_multi_chain`` resolves wallets and RPC URLs through the
            # EVM path (``chain=None``-keyed env reads, ``_derive_wallet_address``
            # via private key). Letting Solana through here would surface as a
            # confusing validation failure further down — reject explicitly so
            # the operator gets a precise remediation hint (PR #2152 review).
            raise ConfigurationError(
                field="chains",
                reason=(
                    "Solana is not supported in the multi-chain lane "
                    "(``_build_multi_chain`` is EVM-only). Use the single-chain "
                    "lane (``chain='solana'``) instead, or open a ticket if "
                    "Solana belongs in a multi-chain mix you're building."
                ),
            )
        if cl in normalised:
            raise ConfigurationError(
                field="chains",
                reason=f"Duplicate chain '{cl}' in chains list",
            )
        normalised.append(cl)

    chain_ids = {ch: CHAIN_IDS[ch] for ch in normalised}
    return normalised, chain_ids


# crap-allowlist: mirrors MultiChainRuntimeConfig._validate_protocols which is
# covered by the dataclass __post_init__ tests in
# tests/unit/cli/test_local_runtime_config.py and
# tests/unit/execution/test_multichain_sidecar.py — splitting the validation
# into branches just to lower CC would diverge from the legacy validator the
# dataclass still calls when constructed directly. (#2097, Phase 5a-2)
def _normalise_multi_protocols(chains: list[str], protocols: dict[str, list[str]]) -> dict[str, list[str]]:
    """Normalise + validate the multi-chain protocols mapping.

    Mirrors :meth:`MultiChainRuntimeConfig._validate_protocols` so the
    factory raises before the dataclass would.
    """
    # Imported lazily — protocol validation matrix lives in the legacy module.
    from almanak.framework.execution.config import SUPPORTED_PROTOCOLS

    if not protocols:
        raise ConfigurationError(field="protocols", reason="Protocols mapping cannot be empty")

    normalised: dict[str, list[str]] = {}
    for chain, protocol_list in protocols.items():
        cl = chain.lower()
        if cl not in chains:
            raise ConfigurationError(
                field="protocols",
                reason=(f"Protocol mapping for chain '{chain}' but chain not in configured chains: {chains}"),
            )
        if not protocol_list:
            raise ConfigurationError(
                field="protocols",
                reason=f"Protocol list for chain '{chain}' cannot be empty",
            )

        validated: list[str] = []
        for protocol in protocol_list:
            pl = protocol.lower()
            if pl not in SUPPORTED_PROTOCOLS:
                valid = ", ".join(sorted(SUPPORTED_PROTOCOLS.keys()))
                raise ConfigurationError(
                    field="protocols",
                    reason=f"Unknown protocol '{protocol}'. Valid protocols: {valid}",
                )
            if cl not in SUPPORTED_PROTOCOLS[pl]:
                avail = ", ".join(sorted(SUPPORTED_PROTOCOLS[pl]))
                raise ConfigurationError(
                    field="protocols",
                    reason=(f"Protocol '{protocol}' is not available on chain '{chain}'. Available on: {avail}"),
                )
            if pl in validated:
                raise ConfigurationError(
                    field="protocols",
                    reason=f"Duplicate protocol '{protocol}' for chain '{chain}'",
                )
            validated.append(pl)
        normalised[cl] = validated

    for chain in chains:
        if chain not in normalised:
            raise ConfigurationError(
                field="protocols",
                reason=f"No protocols configured for chain '{chain}'",
            )
    return normalised


def private_key_from_env(*, prefix: str = "ALMANAK_") -> str:
    """Read the bare ``{prefix}PRIVATE_KEY`` env var; ``""`` if unset.

    Side-effect-free reader for callers that want the resolved private
    key but cannot pay the cost of building a full :class:`RuntimeConfig`
    (which probes RPC URLs and gas caps as part of construction). The
    typical caller is the ``almanak ax`` CLI bootstrap, which derives a
    wallet address from the key but degrades cleanly to the no-wallet
    read-only mode when the env var is unset.

    Mirrors the no-chain branch of
    :func:`_resolve_private_key_from_env` — no Solana, Zodiac, or
    gateway-wallets fallbacks; just the prefixed env. Callers that need
    those fallbacks should construct a full :class:`RuntimeConfig` via
    :func:`runtime_config_from_env`.
    """
    _load_dotenv_once()
    return os.environ.get(f"{prefix}PRIVATE_KEY", "") or ""


def gateway_wallets_configured(*, prefix: str = "ALMANAK_") -> bool:
    """Return True when ``{prefix}GATEWAY_WALLETS`` is present and non-empty."""
    return bool(os.environ.get(f"{prefix}GATEWAY_WALLETS"))


def multi_chain_rpc_urls_from_env(
    *,
    chains: list[str],
    network: str,
    private_key: str | None,
    prefix: str = "ALMANAK_",
    dotenv_path: str | None = None,
) -> dict[str, str]:
    """Resolve per-chain RPC URLs for the legacy multi-chain dataclass path."""
    _load_dotenv_once(dotenv_path)
    return _resolve_multi_chain_rpc_urls(
        chains=chains,
        network=network,
        private_key=private_key or "",
        prefix=prefix,
    )


__all__ = [
    "CHAIN_IDS",
    "ConfigurationError",
    "DataFreshnessPolicy",
    "ExecutionMode",
    "MissingEnvironmentVariableError",
    "RuntimeConfig",
    "gateway_wallets_configured",
    "multi_chain_rpc_urls_from_env",
    "private_key_from_env",
    "runtime_config_from_env",
]
