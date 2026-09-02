"""Typed connector secrets, RPC URLs, and API endpoint configuration.

This module must not import from ``almanak.connectors.*``; connectors depend on
this configuration layer, and reversing that dependency creates import cycles.
"""

from __future__ import annotations

import os
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from almanak.config.env import _load_dotenv_once

DEFAULT_DRIFT_DATA_API_BASE_URL: str = "https://data.api.drift.trade"

DEFAULT_METEORA_API_BASE_URL: str = "https://dlmm.datapi.meteora.ag"

DEFAULT_ORCA_API_BASE_URL: str = "https://api.orca.so/v2/solana"

DEFAULT_RAYDIUM_API_BASE_URL: str = "https://api-v3.raydium.io"

# Solana mainnet RPC fallback used by Jupiter when no URL is configured.
# Drift uses an empty fallback (the SDK requires explicit RPC for the
# direct path; gateway-routed callers never read it). Preserved verbatim.
DEFAULT_SOLANA_RPC_URL_JUPITER: str = "https://api.mainnet-beta.solana.com"


class ConnectorsConfig(BaseModel):
    """Typed configuration for every connector under ``almanak/connectors/*``.

    Secret fields (``*_api_key``, ``*_api_secret``, ``*_secret``,
    ``*_passphrase``, ``*_private_key``) carry ``Field(repr=False)`` so
    pydantic ``__repr__()`` does not leak credentials. ``model_dump()`` still
    returns raw values and its output must be treated as sensitive. Values stay
    as plain strings because connector APIs consume the raw type.
    """

    enso_api_key: str | None = Field(default=None, repr=False)

    jupiter_api_key: str | None = Field(default=None, repr=False)

    lifi_api_key: str | None = Field(default=None, repr=False)

    kraken_api_key: str | None = Field(default=None, repr=False)

    kraken_api_secret: str | None = Field(default=None, repr=False)

    polymarket_wallet_address: str | None = Field(default=None, repr=False)

    polymarket_private_key: str | None = Field(default=None, repr=False)

    polymarket_api_key: str | None = Field(default=None, repr=False)

    polymarket_secret: str | None = Field(default=None, repr=False)

    polymarket_passphrase: str | None = Field(default=None, repr=False)

    polymarket_signer_service_url: str | None = Field(default=None, repr=False)

    polymarket_signer_service_jwt: str | None = Field(default=None, repr=False)

    polygon_rpc_url: str | None = Field(default=None, repr=False)

    polymarket_clob_url: str | None = Field(default=None, repr=False)

    polymarket_gamma_url: str | None = Field(default=None, repr=False)

    polymarket_data_api_url: str | None = Field(default=None, repr=False)

    # RPC URLs can embed API keys, so suppress them from repr as well.
    solana_rpc_url: str | None = Field(default=None, repr=False)
    """Solana RPC URL (``SOLANA_RPC_URL``).

    ``None`` means "no override" — the consumer applies its own default:

    * ``DriftAdapter`` resolves to the empty string (direct path requires
      an explicit URL or a gateway client).
    * ``JupiterAdapter`` resolves to ``https://api.mainnet-beta.solana.com``
      (the public mainnet endpoint).

    The model does not impose a uniform default because Drift and Jupiter
    require different missing-value behavior.
    """

    gmx_anvil_trace_dir: str | None = Field(default=None)
    """Directory for GMX managed-Anvil callTracer artifacts (``ALMANAK_GMX_ANVIL_TRACE_DIR``).

    ``None`` means capture is off (the default). When set, the anvil keeper
    persists raw callTracer JSON for every keeper ``executeOrder`` — filling
    and reverting — plus any reverting harness (oracle setup/cleanup)
    transaction whose diagnosis fetched a trace. The fill artifacts are the
    control arm of revert differentials (VIB-6437 R16). Documented in
    ``docs/environment-variables.md`` §Anvil & Fork Health.
    """

    drift_data_api_base_url: str = DEFAULT_DRIFT_DATA_API_BASE_URL

    meteora_api_base_url: str = DEFAULT_METEORA_API_BASE_URL

    orca_api_base_url: str = DEFAULT_ORCA_API_BASE_URL

    raydium_api_base_url: str = DEFAULT_RAYDIUM_API_BASE_URL

    model_config = ConfigDict(
        # Reject typos at the service boundary — a misspelt kwarg here
        # would silently flow into the config without populating any
        # connector field.
        extra="forbid",
    )


def connectors_config_from_env(
    *,
    dotenv_path: str | None = None,
) -> ConnectorsConfig:
    """Construct connector configuration from environment variables."""
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
        "gmx_anvil_trace_dir": os.environ.get("ALMANAK_GMX_ANVIL_TRACE_DIR"),
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
