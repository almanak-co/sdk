"""Typed Safe signer configuration submodel."""

from __future__ import annotations

import os

from pydantic import BaseModel, ConfigDict, Field

from almanak.config.env import _load_dotenv_once


class SafeSignerServiceConfig(BaseModel):
    """Typed Safe signer config for wallet mapping and Zodiac mode."""

    platform_wallets_json: str | None = Field(default=None, repr=False)
    """Raw JSON payload from ``ALMANAK_PLATFORM_WALLETS``."""

    endpoint_root: str | None = None
    jwt: str | None = Field(default=None, repr=False)

    model_config = ConfigDict(extra="forbid")


def safe_signer_service_config_from_env(
    *,
    dotenv_path: str | None = None,
    wallet_env_var: str = "ALMANAK_PLATFORM_WALLETS",
) -> SafeSignerServiceConfig:
    """Return the typed Safe signer config from env."""
    _load_dotenv_once(dotenv_path)
    return SafeSignerServiceConfig(
        platform_wallets_json=os.environ.get(wallet_env_var) or None,
        endpoint_root=os.environ.get("ALMANAK_SIGNER_SERVICE_ENDPOINT_ROOT") or None,
        jwt=os.environ.get("ALMANAK_SIGNER_SERVICE_JWT") or None,
    )


__all__ = [
    "SafeSignerServiceConfig",
    "safe_signer_service_config_from_env",
]
