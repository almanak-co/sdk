"""Typed simulation configuration submodel."""

from __future__ import annotations

import logging
import os

from pydantic import BaseModel, ConfigDict, Field

from almanak.config.env import _load_dotenv_once

logger = logging.getLogger(__name__)

DEFAULT_SIMULATION_ENABLED: bool = True
DEFAULT_SIMULATION_TIMEOUT_SECONDS: float = 10.0
DEFAULT_PREFER_ALCHEMY: bool = False
_TRUTHY_VALUES: frozenset[str] = frozenset({"true", "1", "yes", "y"})


class SimulationConfig(BaseModel):
    """Typed simulation configuration."""

    enabled: bool = DEFAULT_SIMULATION_ENABLED
    tenderly_account: str | None = None
    tenderly_project: str | None = None
    tenderly_access_key: str | None = Field(default=None, repr=False)
    alchemy_api_key: str | None = Field(default=None, repr=False)
    timeout_seconds: float = DEFAULT_SIMULATION_TIMEOUT_SECONDS
    prefer_alchemy: bool = DEFAULT_PREFER_ALCHEMY

    model_config = ConfigDict(extra="forbid")


def _parse_bool(value: str | None, default: bool) -> bool:
    if value is None:
        return default
    return value.lower() in _TRUTHY_VALUES


def _parse_float(name: str, value: str | None, default: float) -> float:
    if value is None:
        return default
    try:
        return float(value)
    except ValueError:
        logger.warning("Invalid float for %s: %s, using default %s", name, value, default)
        return default


def simulation_config_from_env(
    *,
    prefix: str = "ALMANAK_",
    dotenv_path: str | None = None,
) -> SimulationConfig:
    """Construct a typed simulation config from environment variables."""
    _load_dotenv_once(dotenv_path)

    return SimulationConfig(
        enabled=_parse_bool(os.environ.get(f"{prefix}SIMULATION_ENABLED"), DEFAULT_SIMULATION_ENABLED),
        tenderly_account=os.environ.get("TENDERLY_ACCOUNT_SLUG"),
        tenderly_project=os.environ.get("TENDERLY_PROJECT_SLUG"),
        tenderly_access_key=os.environ.get("TENDERLY_ACCESS_KEY"),
        alchemy_api_key=os.environ.get("ALCHEMY_API_KEY"),
        timeout_seconds=_parse_float(
            f"{prefix}SIMULATION_TIMEOUT",
            os.environ.get(f"{prefix}SIMULATION_TIMEOUT"),
            DEFAULT_SIMULATION_TIMEOUT_SECONDS,
        ),
        prefer_alchemy=_parse_bool(os.environ.get(f"{prefix}SIMULATION_PREFER_ALCHEMY"), DEFAULT_PREFER_ALCHEMY),
    )


__all__ = [
    "DEFAULT_PREFER_ALCHEMY",
    "DEFAULT_SIMULATION_ENABLED",
    "DEFAULT_SIMULATION_TIMEOUT_SECONDS",
    "SimulationConfig",
    "simulation_config_from_env",
]
