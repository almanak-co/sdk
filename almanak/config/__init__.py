"""Typed configuration service for the Almanak SDK.

The package owns every read of ``os.environ`` and every call to
``load_dotenv()`` outside a small, explicitly allowlisted set of files. The
plan and migration sequence live in
``docs/internal/config-service-plan.md``.

Public surface:

* :func:`load_config` — construct the typed config once (called by the Click
  main group).
* :class:`BaseConfig`, :class:`LocalConfig`, :class:`HostedConfig` — the
  shared base and the two deployment-mode siblings.
* :class:`GatewayConfig` — the gateway submodel embedded in every mode.
* :class:`StrategyConfig` — the Pydantic base for per-strategy ``config.json``
  schemas (filled in during Phase 3).
"""

from almanak.config.base import BaseConfig, GatewayConfig
from almanak.config.hosted import HostedConfig
from almanak.config.local import LocalConfig
from almanak.config.service import load_config
from almanak.config.strategy import StrategyConfig

__all__ = [
    "BaseConfig",
    "GatewayConfig",
    "HostedConfig",
    "LocalConfig",
    "StrategyConfig",
    "load_config",
]
