"""Typed connectors configuration submodel.

Phase 5b of the config-service migration (see
``docs/internal/config-service-plan.md``). Owns every env read for
connector-side API keys and base-URL overrides that previously lived
inside individual connector modules under
``almanak/framework/connectors/*``.

Two surfaces consolidated here:

* **API keys / secrets** (``enso_api_key``, ``jupiter_api_key``,
  ``lifi_api_key``, ``kraken_api_key``, ``kraken_api_secret``,
  ``polymarket_*``, ``solana_rpc_url``). Stored ``repr=False`` so
  ``logger.info(repr(cfg))`` cannot leak credentials. Defaults to
  ``None`` — the connector decides whether to hard-fail or degrade
  to a public endpoint when the key is missing (see each connector's
  call site for the exact policy preserved on cutover).

* **Base-URL overrides** (``drift_data_api_base_url``,
  ``meteora_api_base_url``, ``orca_api_base_url``,
  ``raydium_api_base_url``). Defaults populated to the public
  production endpoints — bit-for-bit identical to the legacy
  module-level ``... = os.environ.get(...) or "https://..."``
  constants. Pulling the lookup off module-load fixes a real bug:
  the legacy form froze the env value at import time, so a test
  that monkeypatched ``METEORA_API_BASE_URL`` after the constants
  module was first imported saw no effect. Pydantic-model fields
  are read at construction time, which is the right moment.

Import direction
----------------
Strict (mirrors :mod:`almanak.config.runtime`): this module MUST NOT
import from ``almanak.connectors.*``. The connectors import
:class:`ConnectorsConfig` from here at construction time; reverse
imports would create a cycle and make the typed-config service depend
on the connector layer it is meant to feed.
"""

from __future__ import annotations

import os
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from almanak.config.env import _load_dotenv_once

# =============================================================================
# Default base URLs — bit-for-bit mirrors of the legacy module-level constants
# =============================================================================

# Drift data API (read-only, no auth). Legacy:
#     DRIFT_DATA_API_BASE_URL = os.environ.get("DRIFT_DATA_API_BASE_URL") or "https://data.api.drift.trade"
DEFAULT_DRIFT_DATA_API_BASE_URL: str = "https://data.api.drift.trade"

# Meteora DLMM API (read-only). Legacy:
#     METEORA_API_BASE_URL = os.environ.get("METEORA_API_BASE_URL") or "https://dlmm.datapi.meteora.ag"
DEFAULT_METEORA_API_BASE_URL: str = "https://dlmm.datapi.meteora.ag"

# Orca Whirlpools API (read-only). Legacy:
#     ORCA_API_BASE_URL = os.environ.get("ORCA_API_BASE_URL") or "https://api.orca.so/v2/solana"
DEFAULT_ORCA_API_BASE_URL: str = "https://api.orca.so/v2/solana"

# Raydium CLMM API (read-only). Legacy:
#     RAYDIUM_API_BASE_URL = os.environ.get("RAYDIUM_API_BASE_URL") or "https://api-v3.raydium.io"
DEFAULT_RAYDIUM_API_BASE_URL: str = "https://api-v3.raydium.io"

# Solana mainnet RPC fallback used by Jupiter when no URL is configured.
# Drift uses an empty fallback (the SDK requires explicit RPC for the
# direct path; gateway-routed callers never read it). Preserved verbatim.
DEFAULT_SOLANA_RPC_URL_JUPITER: str = "https://api.mainnet-beta.solana.com"


# =============================================================================
# ConnectorsConfig — typed, validated, secret-safe
# =============================================================================


class ConnectorsConfig(BaseModel):
    """Typed configuration for every connector under ``framework/connectors/*``.

    Every field is optional from the connector's standpoint — when a value
    is ``None``, the connector's existing missing-env-var behaviour fires
    (Enso hard-fails without a gateway, LiFi degrades to anonymous public
    endpoint, etc). The field-by-field policy is preserved bit-for-bit on
    cutover; this model is the *single env reader*, not a behavioural
    rewrite.

    Secret fields (``*_api_key``, ``*_api_secret``, ``*_secret``,
    ``*_passphrase``, ``*_private_key``) carry ``Field(repr=False)`` so
    pydantic ``__repr__()`` never leaks credentials into logs.
    ``model_dump()`` is *not* covered by ``repr=False`` and still returns
    raw values — callers must treat its output as sensitive (or pass
    ``exclude=`` / migrate to ``SecretStr`` if they need redaction).
    Plaintext is intentional — connectors today consume the raw string
    (no ``SecretStr`` round-trip) and changing the on-the-wire type on
    cutover would be a behavioural change. Wrapping into ``SecretStr``
    is a follow-up phase.
    """

    # -------------------------------------------------------------------------
    # API keys / secrets — repr suppressed.
    # -------------------------------------------------------------------------

    enso_api_key: str | None = Field(default=None, repr=False)
    """Enso Finance API key (``ENSO_API_KEY``).

    Required for direct-HTTP mode; gateway-routed callers fetch the key
    from the gateway and pass ``gateway_client`` to ``EnsoConfig`` which
    drops the requirement entirely.
    """

    jupiter_api_key: str | None = Field(default=None, repr=False)
    """Jupiter Swap API key (``JUPITER_API_KEY``).

    Optional — when set, ``JupiterConfig`` selects the paid endpoint
    ``https://api.jup.ag``; when unset, falls back to the rate-limited
    free endpoint ``https://lite-api.jup.ag``.
    """

    lifi_api_key: str | None = Field(default=None, repr=False)
    """LiFi cross-chain routing API key (``LIFI_API_KEY``).

    Optional — LiFi exposes a public anonymous tier. Setting this raises
    rate limits.
    """

    kraken_api_key: str | None = Field(default=None, repr=False)
    """Kraken exchange API key (``KRAKEN_API_KEY``).

    Required when ``KrakenConfig`` is used without a ``credentials=``
    constructor kwarg. ``KrakenCredentials.from_env()`` raises
    ``ValueError`` when missing — that hard-fail policy is preserved.
    """

    kraken_api_secret: str | None = Field(default=None, repr=False)
    """Kraken exchange API secret (``KRAKEN_API_SECRET``)."""

    # Polymarket — the gateway carries its own copy of these (see
    # ``GatewayConfig.polymarket_*`` and ``almanak.config.env._resolve_polymarket_credentials``).
    # The fields below are the connector-side mirror; the legacy
    # ``PolymarketConfig.from_env`` / ``ApiCredentials.from_env`` /
    # ``signer_from_env`` classmethods are the only readers.
    polymarket_wallet_address: str | None = Field(default=None, repr=False)
    """Polymarket wallet address (``POLYMARKET_WALLET_ADDRESS``)."""

    polymarket_private_key: str | None = Field(default=None, repr=False)
    """Polymarket signing key (``POLYMARKET_PRIVATE_KEY``)."""

    polymarket_api_key: str | None = Field(default=None, repr=False)
    """Polymarket L2 API key (``POLYMARKET_API_KEY``)."""

    polymarket_secret: str | None = Field(default=None, repr=False)
    """Polymarket L2 HMAC secret (``POLYMARKET_SECRET``)."""

    polymarket_passphrase: str | None = Field(default=None, repr=False)
    """Polymarket L2 passphrase (``POLYMARKET_PASSPHRASE``)."""

    # Polymarket signer service (platform mode). These are read by
    # ``signer_from_env`` only — never by ``PolymarketConfig`` itself.
    polymarket_signer_service_url: str | None = Field(default=None, repr=False)
    """Almanak signer service base URL (``ALMANAK_SIGNER_SERVICE_URL``).

    Same env var as the gateway-tier signer; mirrored here so the
    Polymarket signer factory has a typed reader.
    """

    polymarket_signer_service_jwt: str | None = Field(default=None, repr=False)
    """Almanak signer service JWT (``ALMANAK_SIGNER_SERVICE_JWT``)."""

    polygon_rpc_url: str | None = Field(default=None, repr=False)
    """Polygon RPC URL for on-chain Polymarket operations (``POLYGON_RPC_URL``)."""

    polymarket_clob_url: str | None = Field(default=None, repr=False)
    """Override for the Polymarket CLOB API base URL (``POLYMARKET_CLOB_URL``)."""

    polymarket_gamma_url: str | None = Field(default=None, repr=False)
    """Override for the Polymarket Gamma Markets API URL (``POLYMARKET_GAMMA_URL``)."""

    polymarket_data_api_url: str | None = Field(default=None, repr=False)
    """Override for the Polymarket Data API URL (``POLYMARKET_DATA_API_URL``)."""

    # -------------------------------------------------------------------------
    # RPC URL — borderline secret (some providers carry an API key in the
    # path); ``repr=False`` preserves the legacy redaction semantics.
    # -------------------------------------------------------------------------

    solana_rpc_url: str | None = Field(default=None, repr=False)
    """Solana RPC URL (``SOLANA_RPC_URL``).

    ``None`` means "no override" — the consumer applies its own default:

    * ``DriftAdapter`` resolves to the empty string (direct path requires
      an explicit URL or a gateway client).
    * ``JupiterAdapter`` resolves to ``https://api.mainnet-beta.solana.com``
      (the public mainnet endpoint).

    The two policies are preserved verbatim; the typed config does not
    impose a uniform default because Drift and Jupiter disagree on what
    the right miss behaviour is.
    """

    # -------------------------------------------------------------------------
    # Base URLs — non-secret, defaulted to public production endpoints.
    # -------------------------------------------------------------------------

    drift_data_api_base_url: str = DEFAULT_DRIFT_DATA_API_BASE_URL
    """Drift data API base URL (``DRIFT_DATA_API_BASE_URL``)."""

    meteora_api_base_url: str = DEFAULT_METEORA_API_BASE_URL
    """Meteora DLMM API base URL (``METEORA_API_BASE_URL``)."""

    orca_api_base_url: str = DEFAULT_ORCA_API_BASE_URL
    """Orca Whirlpools API base URL (``ORCA_API_BASE_URL``)."""

    raydium_api_base_url: str = DEFAULT_RAYDIUM_API_BASE_URL
    """Raydium CLMM API base URL (``RAYDIUM_API_BASE_URL``)."""

    model_config = ConfigDict(
        # Reject typos at the service boundary — a misspelt kwarg here
        # would silently flow into the config without populating any
        # connector field.
        extra="forbid",
    )


# =============================================================================
# Public factory — single env-reading entry point for connector config
# =============================================================================


def connectors_config_from_env(
    *,
    dotenv_path: str | None = None,
) -> ConnectorsConfig:
    """Construct a :class:`ConnectorsConfig` from environment variables.

    Single env-reading entry point for every connector under
    ``framework/connectors/*``. Mirrors the legacy per-connector lookups
    bit-for-bit:

    * ``ENSO_API_KEY`` → ``enso_api_key``
    * ``JUPITER_API_KEY`` → ``jupiter_api_key``
    * ``LIFI_API_KEY`` → ``lifi_api_key``
    * ``KRAKEN_API_KEY`` / ``KRAKEN_API_SECRET`` → ``kraken_api_*``
    * ``SOLANA_RPC_URL`` → ``solana_rpc_url``
    * ``POLYMARKET_*`` → ``polymarket_*`` (mirrors the gateway-tier
      fallback ladder used by ``almanak.config.env``)
    * ``ALMANAK_SIGNER_SERVICE_URL`` / ``ALMANAK_SIGNER_SERVICE_JWT``
      → ``polymarket_signer_service_*``
    * ``POLYGON_RPC_URL`` → ``polygon_rpc_url``
    * ``DRIFT_DATA_API_BASE_URL`` / ``METEORA_API_BASE_URL`` /
      ``ORCA_API_BASE_URL`` / ``RAYDIUM_API_BASE_URL`` → ``*_base_url``
      (only when the env var is set; field defaults to the public
      production endpoint otherwise).
    """
    _load_dotenv_once(dotenv_path)

    # Bare-name primary, ALMANAK_-prefixed alias secondary — mirrors the
    # gateway-tier ladder ``_resolve_polymarket_*`` applies (see
    # almanak/config/env.py).
    polymarket_wallet = os.environ.get("POLYMARKET_WALLET_ADDRESS") or os.environ.get(
        "ALMANAK_POLYMARKET_WALLET_ADDRESS"
    )
    polymarket_private_key = os.environ.get("POLYMARKET_PRIVATE_KEY") or os.environ.get(
        "ALMANAK_POLYMARKET_PRIVATE_KEY"
    )
    polymarket_api_key = os.environ.get("POLYMARKET_API_KEY") or os.environ.get("ALMANAK_POLYMARKET_API_KEY")
    polymarket_secret = os.environ.get("POLYMARKET_SECRET") or os.environ.get("ALMANAK_POLYMARKET_SECRET")
    polymarket_passphrase = os.environ.get("POLYMARKET_PASSPHRASE") or os.environ.get("ALMANAK_POLYMARKET_PASSPHRASE")

    kwargs: dict[str, Any] = {
        "enso_api_key": os.environ.get("ENSO_API_KEY"),
        "jupiter_api_key": os.environ.get("JUPITER_API_KEY"),
        "lifi_api_key": os.environ.get("LIFI_API_KEY"),
        "kraken_api_key": os.environ.get("KRAKEN_API_KEY"),
        "kraken_api_secret": os.environ.get("KRAKEN_API_SECRET"),
        "polymarket_wallet_address": polymarket_wallet,
        "polymarket_private_key": polymarket_private_key,
        "polymarket_api_key": polymarket_api_key,
        "polymarket_secret": polymarket_secret,
        "polymarket_passphrase": polymarket_passphrase,
        "polymarket_signer_service_url": os.environ.get("ALMANAK_SIGNER_SERVICE_URL"),
        "polymarket_signer_service_jwt": os.environ.get("ALMANAK_SIGNER_SERVICE_JWT"),
        "polygon_rpc_url": os.environ.get("POLYGON_RPC_URL"),
        "polymarket_clob_url": os.environ.get("POLYMARKET_CLOB_URL"),
        "polymarket_gamma_url": os.environ.get("POLYMARKET_GAMMA_URL"),
        "polymarket_data_api_url": os.environ.get("POLYMARKET_DATA_API_URL"),
        "solana_rpc_url": os.environ.get("SOLANA_RPC_URL"),
    }

    # Base URLs — only set the field when the env var is provided so the
    # model default (the public production endpoint) survives.
    base_url_overrides = {
        "drift_data_api_base_url": os.environ.get("DRIFT_DATA_API_BASE_URL"),
        "meteora_api_base_url": os.environ.get("METEORA_API_BASE_URL"),
        "orca_api_base_url": os.environ.get("ORCA_API_BASE_URL"),
        "raydium_api_base_url": os.environ.get("RAYDIUM_API_BASE_URL"),
    }
    for key, value in base_url_overrides.items():
        if value:
            kwargs[key] = value

    return ConnectorsConfig(**kwargs)


__all__ = [
    "DEFAULT_DRIFT_DATA_API_BASE_URL",
    "DEFAULT_METEORA_API_BASE_URL",
    "DEFAULT_ORCA_API_BASE_URL",
    "DEFAULT_RAYDIUM_API_BASE_URL",
    "DEFAULT_SOLANA_RPC_URL_JUPITER",
    "ConnectorsConfig",
    "connectors_config_from_env",
]
