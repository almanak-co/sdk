"""Dynamic gateway-runtime env helpers.

``load_config().gateway`` owns boot-time, typed gateway settings. This module
owns the remaining gateway env contracts that intentionally stay dynamic or
would require invasive API threading to inject everywhere today.

VIB-4424 uses this file as the single env-reading boundary for the gateway
backlog slice's runtime helpers:

* RPC URL ladders / Tenderly keys
* ``ANVIL_<CHAIN>_PORT`` and ``ANVIL_FORK_BLOCK_<CHAIN>``
* wallet-registry JSON discovery
* manual price override discovery
* per-provider portfolio chain filters / cache TTLs
* small process-env mutation helpers used by managed Anvil bootstrap
"""

from __future__ import annotations

import json
import logging
import os
from collections.abc import Iterable
from typing import Any

from almanak.config.cli_runtime import (
    DEFAULT_ANVIL_PORT,
)
from almanak.config.cli_runtime import (
    anvil_port_for_chain as _cli_anvil_port_for_chain,
)

logger = logging.getLogger(__name__)


def gateway_prefixed_or_bare(name: str) -> str | None:
    """Return ``ALMANAK_GATEWAY_{name}`` or the bare-name fallback."""
    return os.environ.get(f"ALMANAK_GATEWAY_{name}") or os.environ.get(name)


def _rpc_variants(chain: str) -> tuple[str, ...]:
    """Return the env-var chain variants that preserve the legacy BSC/BNB ladder."""
    chain_upper = chain.upper()
    if chain_upper == "BSC":
        return ("BSC", "BNB")
    if chain_upper == "BNB":
        return ("BNB", "BSC")
    return (chain_upper,)


def chain_specific_rpc_url(chain: str) -> str | None:
    """Return the first chain-specific RPC URL configured for ``chain``."""
    for variant in _rpc_variants(chain):
        for env_var in (f"ALMANAK_{variant}_RPC_URL", f"{variant}_RPC_URL"):
            value = os.environ.get(env_var)
            if value:
                return value
    return None


def has_chain_specific_rpc_url(chain: str) -> bool:
    """Whether ``chain`` has any chain-specific RPC URL configured."""
    return chain_specific_rpc_url(chain) is not None


def generic_rpc_url() -> str | None:
    """Return the generic cross-chain RPC URL, if configured."""
    return os.environ.get("ALMANAK_RPC_URL") or os.environ.get("RPC_URL")


def generic_rpc_url_env_name() -> str | None:
    """Return the generic RPC env var that won the precedence ladder."""
    if os.environ.get("ALMANAK_RPC_URL"):
        return "ALMANAK_RPC_URL"
    if os.environ.get("RPC_URL"):
        return "RPC_URL"
    return None


def has_generic_rpc_url() -> bool:
    """Whether a generic cross-chain RPC URL is configured."""
    return generic_rpc_url() is not None


def tenderly_api_key_for_chain(chain: str) -> str | None:
    """Return ``TENDERLY_API_KEY_<CHAIN>`` for ``chain`` if set."""
    return os.environ.get(f"TENDERLY_API_KEY_{chain.upper()}")


def any_tenderly_api_key_configured(chains: Iterable[str]) -> bool:
    """Whether any chain in ``chains`` has a Tenderly key configured."""
    return any(tenderly_api_key_for_chain(chain) for chain in chains)


def anvil_port_for_chain(chain: str) -> int | None:
    """Return ``ANVIL_<CHAIN>_PORT`` via the shared CLI-runtime helper."""
    return _cli_anvil_port_for_chain(chain)


def anvil_generic_port_string() -> str:
    """Return the raw generic ``ANVIL_PORT`` value, preserving legacy semantics."""
    raw = os.environ.get("ANVIL_PORT")
    if raw is None:
        return str(DEFAULT_ANVIL_PORT)
    return raw


def anvil_fork_block_for_chain(chain: str) -> int | None:
    """Return ``ANVIL_FORK_BLOCK_<CHAIN>`` as ``int``, or ``None`` when unset.

    A malformed value (typo, hex string, ``"latest"``) logs a warning and
    returns ``None`` rather than crashing managed-Anvil bootstrap with a
    ``ValueError``. ``None`` is the same signal the caller uses when the
    env var is missing, so the fork falls back to the chain head.
    """
    raw = os.environ.get(f"ANVIL_FORK_BLOCK_{chain.upper()}")
    if raw is None or not raw.strip():
        return None
    try:
        return int(raw.strip())
    except ValueError:
        logger.warning(
            "Invalid integer in ANVIL_FORK_BLOCK_%s=%r; falling back to chain head",
            chain.upper(),
            raw,
        )
        return None


def env_value(name: str) -> str | None:
    """Return the raw process env value for ``name``."""
    return os.environ.get(name)


def set_env_value(name: str, value: str) -> None:
    """Set ``name`` in the live process environment."""
    os.environ[name] = value


def restore_env_value(name: str, original: str | None) -> None:
    """Restore ``name`` to ``original`` (or delete it when ``None``)."""
    if original is None:
        os.environ.pop(name, None)
    else:
        os.environ[name] = original


def gateway_wallets_json() -> str | None:
    """Return the raw ``ALMANAK_GATEWAY_WALLETS`` JSON string, if configured."""
    return os.environ.get("ALMANAK_GATEWAY_WALLETS") or None


def gateway_wallets_configured() -> bool:
    """Whether ``ALMANAK_GATEWAY_WALLETS`` is present and non-empty."""
    return bool(gateway_wallets_json())


def parse_gateway_wallets_json() -> dict[str, Any] | None:
    """Parse ``ALMANAK_GATEWAY_WALLETS`` and validate the top-level shape."""
    raw = gateway_wallets_json()
    if not raw:
        return None
    try:
        wallets = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ValueError(f"ALMANAK_GATEWAY_WALLETS is not valid JSON: {exc}") from exc
    if not isinstance(wallets, dict):
        raise ValueError(f"ALMANAK_GATEWAY_WALLETS must be a JSON object keyed by chain, got {type(wallets).__name__}")
    return wallets


def legacy_safe_wallet_address() -> str | None:
    """Return the legacy ``SAFE_WALLET_ADDRESS`` env var if set."""
    return os.environ.get("SAFE_WALLET_ADDRESS") or None


def manual_price_override_raw(env_var: str) -> str | None:
    """Return the raw manual-price override value for ``env_var``."""
    return os.environ.get(env_var)


def manual_price_override_keys(*, prefix: str = "ALMANAK_PRICE_OVERRIDE_") -> tuple[str, ...]:
    """Return every currently-set manual-price override env var."""
    return tuple(key for key in os.environ if key.startswith(prefix))


def portfolio_provider_chain_filter(name: str) -> list[str]:
    """Return the parsed ``<NAME>_CHAIN_FILTER`` CSV for ``name``."""
    raw = os.environ.get(f"{name.upper()}_CHAIN_FILTER", "")
    return [chain.strip() for chain in raw.split(",") if chain.strip()] if raw else []


def portfolio_provider_cache_ttl(name: str, default: int) -> int:
    """Return ``<NAME>_CACHE_TTL`` or ``default`` when unset / malformed.

    A non-numeric value (typo, empty string) logs a warning and returns
    ``default`` instead of raising — losing the override is preferable to
    crashing portfolio-provider construction at gateway boot.
    """
    raw = os.environ.get(f"{name.upper()}_CACHE_TTL")
    if raw is None or not raw.strip():
        return default
    try:
        return int(raw)
    except ValueError:
        logger.warning(
            "Invalid integer in %s_CACHE_TTL=%r; using default %s",
            name.upper(),
            raw,
            default,
        )
        return default


__all__ = [
    "anvil_fork_block_for_chain",
    "anvil_generic_port_string",
    "anvil_port_for_chain",
    "any_tenderly_api_key_configured",
    "chain_specific_rpc_url",
    "env_value",
    "gateway_prefixed_or_bare",
    "gateway_wallets_configured",
    "gateway_wallets_json",
    "generic_rpc_url",
    "generic_rpc_url_env_name",
    "has_chain_specific_rpc_url",
    "has_generic_rpc_url",
    "legacy_safe_wallet_address",
    "manual_price_override_keys",
    "manual_price_override_raw",
    "parse_gateway_wallets_json",
    "portfolio_provider_cache_ttl",
    "portfolio_provider_chain_filter",
    "restore_env_value",
    "set_env_value",
    "tenderly_api_key_for_chain",
]
